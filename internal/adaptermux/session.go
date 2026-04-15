package adaptermux

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

const (
	// defaultSessionSendBuffer is the default send channel capacity
	// for external sessions. If the buffer fills, the session is
	// forcibly closed (backpressure protection, from proxy convention).
	defaultSessionSendBuffer = 8192

	// maxSessions is the hard ceiling on concurrent external sessions.
	// Prevents unbounded memory growth from runaway connections (AM25/AM50).
	maxSessions = 1000
)

// session represents an external ENH client connected via the proxy
// endpoint (e.g., ebusd). Each session has its own echo tracker and
// send buffer.
type session struct {
	id          uint64
	conn        net.Conn
	mux         *Mux
	echoTracker *echoTracker

	// sendCh buffers outgoing frames (ENHResReceived bytes) for the
	// session's writer goroutine. Overflow causes session close.
	sendCh chan sessionFrame

	// done is closed by session.close() to signal writeLoop to exit.
	// Unlike closing sendCh (which panics on concurrent sends), this
	// is safe because only close() writes to it.
	done chan struct{}

	// closed tracks whether the session has been shut down.
	closed atomic.Bool

	// parserResetNeeded signals readLoop to reset its ENH parser.
	// Set by handleStart goroutine after arbitration resolves (AM23:
	// enh.md:128 — reset parser after arbitration complete).
	parserResetNeeded atomic.Bool

	// wg waits for reader, writer, and handleStart goroutines to finish.
	wg sync.WaitGroup
}

// sessionFrame is a frame to deliver to an external session.
type sessionFrame struct {
	kind    sessionFrameKind
	payload byte
}

type sessionFrameKind uint8

const (
	sessionFrameReceived  sessionFrameKind = iota // ENHResReceived(byte)
	sessionFrameStarted                           // ENHResStarted
	sessionFrameFailed                            // ENHResFailed
	sessionFrameResetted                          // ENHResResetted
	sessionFrameErrorEBUS                         // ENHResErrorEBUS
	sessionFrameErrorHost                         // ENHResErrorHost
	sessionFrameInfo                              // ENHResInfo(byte)
)

// AddSession registers an external TCP connection as an ENH session.
// Returns the session ID (>0). The session starts reader and writer
// goroutines. Returns 0 if the mux is shutting down (context cancelled);
// the connection is closed and no goroutines are leaked.
func (m *Mux) AddSession(conn net.Conn) uint64 {
	if m.ctx == nil || m.ctx.Err() != nil {
		_ = conn.Close()
		m.logger.Printf("adaptermux: rejecting session — mux not started or shutting down (AM13)")
		return 0
	}

	id := m.nextSessionID()
	sess := &session{
		id:          id,
		conn:        conn,
		mux:         m,
		echoTracker: newEchoTracker(),
		sendCh:      make(chan sessionFrame, defaultSessionSendBuffer),
		done:        make(chan struct{}),
	}

	// AM25/AM50: check + insert under a single lock to prevent TOCTOU.
	m.sessionsMu.Lock()
	if len(m.sessions) >= maxSessions {
		m.sessionsMu.Unlock()
		m.logger.Printf("adaptermux: rejecting session — max sessions (%d) reached (AM50)", maxSessions)
		_ = conn.Close()
		return 0
	}
	m.sessions[id] = sess
	m.sessionsMu.Unlock()

	sess.wg.Add(2)
	go sess.readLoop()  // goroutine: reads ENH commands from client
	go sess.writeLoop() // goroutine: writes ENH responses to client

	m.logger.Printf("adaptermux: session %d connected from %s", id, conn.RemoteAddr())
	return id
}

// RemoveSession disconnects and cleans up an external session.
func (m *Mux) RemoveSession(id uint64) {
	m.sessionsMu.Lock()
	sess, ok := m.sessions[id]
	if ok {
		delete(m.sessions, id)
	}
	m.sessionsMu.Unlock()

	if ok {
		m.arb.removeSession(id)
		m.cancelPendingStart(id)
		m.tryGrantAndStart()
		sess.close()
		m.logger.Printf("adaptermux: session %d disconnected", id)
	}
}

// close shuts down a session's goroutines and connection.
// Signals writeLoop via done channel (not close(sendCh) which would
// panic on concurrent sends).
func (s *session) close() {
	if s.closed.Swap(true) {
		return // already closed
	}
	close(s.done) // signal writeLoop and handleStart goroutines to exit
	if err := s.conn.Close(); err != nil {
		s.mux.logger.Printf("adaptermux: session %d conn.Close: %v", s.id, err)
	}

	// Wait with timeout to avoid hanging on stuck goroutines.
	waitDone := make(chan struct{})
	go func() { s.wg.Wait(); close(waitDone) }() // goroutine: session close wait
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		s.mux.logger.Printf("adaptermux: session %d close timed out — wg.Wait pending, will self-resolve on I/O unblock (AM14)", s.id)
	}
}

// deliverReceived enqueues an ENHResReceived byte for the session.
// If the send buffer is full, the session is forcibly closed.
func (s *session) deliverReceived(symbol byte) {
	if s.closed.Load() {
		return
	}
	select {
	case s.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: symbol}:
	default:
		// Buffer overflow — close session (backpressure protection).
		s.mux.logger.Printf("adaptermux: session %d send buffer overflow, closing", s.id)
		go s.mux.RemoveSession(s.id) // goroutine: async removal to avoid lock contention
	}
}

// deliverReset enqueues an ENHResResetted for the session.
// payload carries the features byte for INIT negotiation fidelity.
func (s *session) deliverReset(payload byte) {
	if s.closed.Load() {
		return
	}
	// Reset boundaries are non-droppable — losing a reset merges
	// pre/post-reset streams and corrupts client frame reconstruction.
	// Block until delivered (matching passive_transport reset delivery).
	// s.done unblocks on session close/shutdown.
	select {
	case s.sendCh <- sessionFrame{kind: sessionFrameResetted, payload: payload}:
	case <-s.done:
	}
}

// deliverStarted notifies the session that its START was granted.
// payload carries the initiator byte for ENH protocol fidelity.
func (s *session) deliverStarted(payload byte) {
	if s.closed.Load() {
		return
	}
	select {
	case s.sendCh <- sessionFrame{kind: sessionFrameStarted, payload: payload}:
	default:
		go s.mux.RemoveSession(s.id) // goroutine: overflow removal
	}
}

// deliverFailed notifies the session that its START failed.
// payload carries the initiator byte for ENH protocol fidelity.
func (s *session) deliverFailed(payload byte) {
	if s.closed.Load() {
		return
	}
	select {
	case s.sendCh <- sessionFrame{kind: sessionFrameFailed, payload: payload}:
	default:
		go s.mux.RemoveSession(s.id) // goroutine: overflow removal
	}
}

// readLoop reads ENH commands from the client connection and processes
// them through the multiplexer.
func (s *session) readLoop() {
	defer s.mux.RemoveSession(s.id) // runs second: cleanup after wg.Done
	defer s.wg.Done()               // runs first: unblock close() before RemoveSession

	reader := bufio.NewReader(s.conn)
	var parser transport.ENHParser
	buf := make([]byte, 256)

	for {
		if s.closed.Load() {
			return
		}

		n, err := reader.Read(buf)
		if err != nil {
			if !s.closed.Load() {
				s.mux.logger.Printf("adaptermux: session %d read error: %v", s.id, err)
			}
			return
		}

		// AM23: reset parser if arbitration completed (enh.md:128).
		if s.parserResetNeeded.CompareAndSwap(true, false) {
			parser.Reset()
		}

		messages, parseErr := parser.Parse(buf[:n])
		if parseErr != nil {
			s.mux.logger.Printf("adaptermux: session %d parse error: %v", s.id, parseErr)
			s.deliverErrorHost() // AM10: notify client of parse error
			parser.Reset()       // recover parser state (MEDIUM-4 fix)
			continue
		}

		for _, msg := range messages {
			s.handleMessage(msg)
		}
	}
}

// handleMessage processes a parsed ENH message from the client.
// ENHReqSend and ENHResReceived share the same opcode (0x01) — the parser
// returns ENHResReceived for short-form bytes (< 0x80) and ENHReqSend for
// long-form SEND frames. Both map to handleSend.
func (s *session) handleMessage(msg transport.ENHMessage) {
	switch msg.Command {
	case transport.ENHReqSend: // also matches ENHResReceived (same opcode 0x01)
		s.handleSend(msg.Data)
	case transport.ENHReqStart:
		s.handleStart(msg.Data)
	case transport.ENHReqInit:
		s.handleInit(msg.Data)
	case transport.ENHReqInfo:
		s.handleInfo(msg.Data)
	}
}

// handleSend processes a SEND command from the client.
func (s *session) handleSend(data byte) {
	if !s.mux.arb.isOwner(s.id) {
		// Session is not bus owner — host-side error, not bus error.
		s.deliverErrorHost()
		return
	}

	result := make(chan error, 1)
	select {
	case s.mux.activeSendCh <- sendRequest{
		sessionID: s.id,
		data:      data,
		result:    result,
	}:
	case <-s.done:
		return // AM14: session closing, don't block on activeSendCh
	case <-s.mux.ctx.Done():
		return
	}

	select {
	case err := <-result:
		if err != nil {
			s.mux.logger.Printf("adaptermux: session %d SEND error: %v", s.id, err)
			if errors.Is(err, errNotBusOwner) || errors.Is(err, errNotConnected) || errors.Is(err, errAdapterWrite) {
				s.deliverErrorHost()
			} else {
				s.deliverError()
			}
		}
	case <-s.done:
		return // AM14: session closing, don't block waiting for send result
	case <-s.mux.ctx.Done():
	}
}

// handleStart processes a START command from the client.
func (s *session) handleStart(initiator byte) {
	// START with SYN (0xAA) is a cancel request — the client is
	// withdrawing its pending START without acquiring the bus.
	// Also release ownership in case the session already owns the bus
	// (proxy does both cancel+release on SYN cancel).
	// P1 fix: also cancel any in-flight pending START at the adapter
	// level if it belongs to this session.
	if initiator == protocol.SymbolSyn {
		s.mux.arb.cancelStart(s.id)
		s.mux.arb.releaseOwnership(s.id)
		s.mux.cancelPendingStart(s.id)
		return
	}

	ch := s.mux.arb.requestStart(s.id, initiator)

	// Wait for arbitration result in a tracked goroutine.
	s.wg.Add(1)
	go func() { // goroutine: wait for START arbitration result
		defer s.wg.Done()
		select {
		case result := <-ch:
			// AM23: signal readLoop to reset ENH parser after arbitration
			// completes (enh.md:128). Set unconditionally — parser state
			// may be stale regardless of grant/fail outcome.
			s.parserResetNeeded.Store(true)
			if s.closed.Load() {
				return
			}
			if result.granted {
				s.deliverStarted(result.initiator)
			} else if result.cancelled {
				// AM55 fix: client-initiated SYN cancel — the client
				// already knows it cancelled, don't deliver spurious FAILED.
				return
			} else if result.err != nil && isResetOrDisconnectError(result.err) {
				// Reset/disconnect caused the START failure — deliver
				// RESETTED so the client sees the correct boundary event
				// instead of a spurious collision (P1 fix).
				s.deliverReset(byte(s.mux.upstreamFeatures.Load()))
			} else {
				s.deliverFailed(result.initiator)
			}
		case <-s.done:
			return
		case <-s.mux.ctx.Done():
			if s.closed.Load() {
				return
			}
			// AM43: mux shutdown — deliver RESETTED (boundary event), not FAILED.
			s.deliverReset(byte(s.mux.upstreamFeatures.Load()))
		}
	}()
}

// handleInit processes an INIT command from the client.
func (s *session) handleInit(features byte) {
	// Reply with the features negotiated with the upstream adapter.
	// If upstream features are unknown (e.g. ENS transport without
	// INIT), echo back what the client requested.
	stored := byte(s.mux.upstreamFeatures.Load())
	if stored == 0 {
		stored = features
	}
	s.deliverReset(stored)
}

// handleInfo processes an INFO request from the client.
// Reads from the mux-level INFO cache (populated at connect time)
// instead of querying the upstream transport directly, avoiding
// readMu contention with the readLoop.
func (s *session) handleInfo(id byte) {
	data, err := s.mux.CachedInfo(transport.AdapterInfoID(id))
	if err != nil {
		s.mux.logger.Printf("adaptermux: session %d INFO id=0x%02X: %v", s.id, id, err)
		s.deliverErrorHost()
		return
	}
	// AM39: guard against payload >255 which would silently truncate
	// via byte(len(data)) wrap-around.
	if len(data) > 255 {
		s.mux.logger.Printf("adaptermux: session %d INFO id=0x%02X payload too large (%d bytes, max 255) (AM39)", s.id, id, len(data))
		s.deliverErrorHost()
		return
	}
	// AM35: zero-length INFO payload is valid per ENH spec (N=0).
	// Deliver just the length prefix (0x00) with no data bytes.
	s.deliverInfo(byte(len(data)))
	for _, b := range data {
		s.deliverInfo(b)
	}
}

// deliverError sends an ENHResErrorEBUS to the client.
func (s *session) deliverError() {
	if s.closed.Load() {
		return
	}
	select {
	case s.sendCh <- sessionFrame{kind: sessionFrameErrorEBUS}:
	default:
		s.mux.logger.Printf("adaptermux: session %d send buffer full, unable to deliver error", s.id)
		go s.mux.RemoveSession(s.id) // goroutine: overflow removal on error delivery
	}
}

// deliverErrorHost sends an ENHResErrorHost to the client.
// Used for host-side errors (e.g. not bus owner, no transport).
func (s *session) deliverErrorHost() {
	if s.closed.Load() {
		return
	}
	select {
	case s.sendCh <- sessionFrame{kind: sessionFrameErrorHost}:
	default:
		s.mux.logger.Printf("adaptermux: session %d send buffer full, unable to deliver host error", s.id)
		go s.mux.RemoveSession(s.id) // goroutine: overflow removal on error delivery
	}
}

// deliverInfo enqueues an ENHResInfo byte for the session.
func (s *session) deliverInfo(b byte) {
	if s.closed.Load() {
		return
	}
	select {
	case s.sendCh <- sessionFrame{kind: sessionFrameInfo, payload: b}:
	default:
		go s.mux.RemoveSession(s.id) // goroutine: overflow removal
	}
}

// writeLoop writes ENH frames to the client connection.
func (s *session) writeLoop() {
	defer s.wg.Done()

	for {
		select {
		case frame := <-s.sendCh:
			if err := s.writeFrame(frame); err != nil {
				if !s.closed.Load() && !errors.Is(err, net.ErrClosed) {
					s.mux.logger.Printf("adaptermux: session %d write error: %v", s.id, err)
				}
				// AM46: drain remaining frames to prevent goroutine leaks
				// from blocked senders (deliverReceived, deliverReset, etc.).
			drainLoop:
				for {
					select {
					case <-s.sendCh:
					default:
						break drainLoop
					}
				}
				go s.mux.RemoveSession(s.id) // goroutine: cleanup on write failure
				return
			}
		case <-s.done:
			return
		case <-s.mux.ctx.Done():
			return
		}
	}
}

// writeFrame encodes and writes an ENH frame to the client connection.
func (s *session) writeFrame(frame sessionFrame) error {
	var buf []byte

	switch frame.kind {
	case sessionFrameReceived:
		// ENHResReceived: if payload < 0x80, send as single byte (short form).
		if frame.payload < 0x80 {
			buf = []byte{frame.payload}
		} else {
			encoded := transport.EncodeENH(transport.ENHResReceived, frame.payload)
			buf = encoded[:]
		}

	case sessionFrameStarted:
		encoded := transport.EncodeENH(transport.ENHResStarted, frame.payload)
		buf = encoded[:]

	case sessionFrameFailed:
		encoded := transport.EncodeENH(transport.ENHResFailed, frame.payload)
		buf = encoded[:]

	case sessionFrameResetted:
		encoded := transport.EncodeENH(transport.ENHResResetted, frame.payload)
		buf = encoded[:]

	case sessionFrameErrorEBUS:
		encoded := transport.EncodeENH(transport.ENHResErrorEBUS, 0x00)
		buf = encoded[:]

	case sessionFrameErrorHost:
		encoded := transport.EncodeENH(transport.ENHResErrorHost, 0x00)
		buf = encoded[:]

	case sessionFrameInfo:
		encoded := transport.EncodeENH(transport.ENHResInfo, frame.payload)
		buf = encoded[:]
	}

	if len(buf) == 0 {
		return fmt.Errorf("adaptermux: unknown session frame kind %d", frame.kind)
	}

	_, err := s.conn.Write(buf)
	return err
}

// isResetOrDisconnectError reports whether err represents a bus reset
// or adapter disconnect, as opposed to an arbitration collision. When
// a pending START fails due to reset/disconnect, the session should
// see RESETTED (not FAILED) so the client can distinguish boundary
// events from normal collision backoff.
func isResetOrDisconnectError(err error) bool {
	if err == nil {
		return false
	}
	// AM18/AM47: prefer typed error matching over broad string matching.
	// The old code matched any error containing "reset" (false-positive
	// on unrelated messages like "budget reset").
	if errors.Is(err, ebuserrors.ErrAdapterReset) {
		return true
	}
	// Fall back to specific substrings for wrapped/untyped errors.
	msg := err.Error()
	return strings.Contains(msg, "adapter disconnected") ||
		strings.Contains(msg, "adapter reset") ||
		strings.Contains(msg, "adaptermux: closed")
}
