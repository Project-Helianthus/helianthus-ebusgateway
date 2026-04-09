package adaptermux

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

const (
	// defaultSessionSendBuffer is the default send channel capacity
	// for external sessions. If the buffer fills, the session is
	// forcibly closed (backpressure protection, from proxy convention).
	defaultSessionSendBuffer = 8192
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
	sessionFrameInfo                              // ENHResInfo(byte)
)

// AddSession registers an external TCP connection as an ENH session.
// Returns the session ID. The session starts reader and writer goroutines.
func (m *Mux) AddSession(conn net.Conn) uint64 {
	id := m.nextSessionID()
	sess := &session{
		id:          id,
		conn:        conn,
		mux:         m,
		echoTracker: newEchoTracker(),
		sendCh:      make(chan sessionFrame, defaultSessionSendBuffer),
		done:        make(chan struct{}),
	}

	m.sessionsMu.Lock()
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
		s.mux.logger.Printf("adaptermux: session %d close timed out — goroutine leak", s.id)
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
func (s *session) deliverReset() {
	if s.closed.Load() {
		return
	}
	select {
	case s.sendCh <- sessionFrame{kind: sessionFrameResetted}:
	default:
		go s.mux.RemoveSession(s.id) // goroutine: overflow removal
	}
}

// deliverStarted notifies the session that its START was granted.
func (s *session) deliverStarted() {
	if s.closed.Load() {
		return
	}
	select {
	case s.sendCh <- sessionFrame{kind: sessionFrameStarted}:
	default:
		go s.mux.RemoveSession(s.id) // goroutine: overflow removal
	}
}

// deliverFailed notifies the session that its START failed.
func (s *session) deliverFailed() {
	if s.closed.Load() {
		return
	}
	select {
	case s.sendCh <- sessionFrame{kind: sessionFrameFailed}:
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

		messages, parseErr := parser.Parse(buf[:n])
		if parseErr != nil {
			s.mux.logger.Printf("adaptermux: session %d parse error: %v", s.id, parseErr)
			parser.Reset() // recover parser state (MEDIUM-4 fix)
			continue
		}

		for _, msg := range messages {
			s.handleMessage(msg)
		}
	}
}

// handleMessage processes a parsed ENH message from the client.
func (s *session) handleMessage(msg transport.ENHMessage) {
	switch msg.Kind {
	case transport.ENHMessageData:
		// Short-form SEND: raw byte < 0x80.
		s.handleSend(msg.Byte)

	case transport.ENHMessageFrame:
		switch msg.Command {
		case transport.ENHReqSend:
			s.handleSend(msg.Data)
		case transport.ENHReqStart:
			s.handleStart(msg.Data)
		case transport.ENHReqInit:
			s.handleInit(msg.Data)
		case transport.ENHReqInfo:
			s.handleInfo(msg.Data)
		}
	}
}

// handleSend processes a SEND command from the client.
func (s *session) handleSend(data byte) {
	if !s.mux.arb.isOwner(s.id) {
		// Session is not bus owner — reject.
		s.deliverError()
		return
	}

	result := make(chan error, 1)
	select {
	case s.mux.activeSendCh <- sendRequest{
		sessionID: s.id,
		data:      data,
		result:    result,
	}:
	case <-s.mux.ctx.Done():
		return
	}

	select {
	case err := <-result:
		if err != nil {
			s.mux.logger.Printf("adaptermux: session %d SEND error: %v", s.id, err)
			s.deliverError()
		}
	case <-s.mux.ctx.Done():
	}
}

// handleStart processes a START command from the client.
func (s *session) handleStart(initiator byte) {
	ch := s.mux.arb.requestStart(s.id, initiator)

	// Wait for arbitration result in a tracked goroutine.
	s.wg.Add(1)
	go func() { // goroutine: wait for START arbitration result
		defer s.wg.Done()
		select {
		case result := <-ch:
			if s.closed.Load() {
				return
			}
			if result.granted {
				s.deliverStarted()
			} else {
				s.deliverFailed()
			}
		case <-s.done:
			return
		case <-s.mux.ctx.Done():
			if s.closed.Load() {
				return
			}
			s.deliverFailed()
		}
	}()
}

// handleInit processes an INIT command from the client.
func (s *session) handleInit(features byte) {
	// Respond with RESETTED to complete the handshake.
	s.deliverReset()
}

// handleInfo processes an INFO request from the client.
func (s *session) handleInfo(id byte) {
	s.mux.connMu.Lock()
	tr := s.mux.upstream
	s.mux.connMu.Unlock()

	if tr == nil {
		s.deliverError()
		return
	}

	infoReq, ok := tr.(transport.InfoRequester)
	if !ok {
		s.mux.logger.Printf("adaptermux: session %d INFO requested but transport does not support it", s.id)
		s.deliverError()
		return
	}

	data, err := infoReq.RequestInfo(transport.AdapterInfoID(id))
	if err != nil {
		s.mux.logger.Printf("adaptermux: session %d INFO request failed: %v", s.id, err)
		s.deliverError()
		return
	}
	if len(data) == 0 {
		s.mux.logger.Printf("adaptermux: session %d INFO id=0x%02X returned empty response", s.id, id)
		s.deliverError()
		return
	}

	// Deliver INFO response: length prefix + data bytes, each as
	// sessionFrameInfo so writeLoop encodes them as ENHResInfo frames.
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
		encoded := transport.EncodeENH(transport.ENHResStarted, 0x00)
		buf = encoded[:]

	case sessionFrameFailed:
		encoded := transport.EncodeENH(transport.ENHResFailed, 0x00)
		buf = encoded[:]

	case sessionFrameResetted:
		encoded := transport.EncodeENH(transport.ENHResResetted, 0x00)
		buf = encoded[:]

	case sessionFrameErrorEBUS:
		encoded := transport.EncodeENH(transport.ENHResErrorEBUS, 0x00)
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
