package adaptermux

import (
	"bufio"
	"errors"
	"expvar"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
)

// F-10 diagnostic instrumentation (EBUSD-VERIFICATION-2026-05-10.md):
// per-frame enqueue→TCP-write latency bucketed at /debug/vars. ebusd's
// default --receivetimeout is 25 ms; bytes that take longer than that
// to traverse our pipeline are the suspected root cause of "send to fe:
// ERR: read timeout" events.
//
// Prometheus-style cumulative histogram: each `le_X` counter holds the
// total number of samples with elapsed_us ≤ X. A sample at 500 µs
// increments le_1000, le_5000, le_25000, and le_100000. The `gt_100000`
// counter is the overflow bin for samples > 100 ms; it is NOT
// cumulative with the le_* counters. To compute "frames over the
// 25 ms ebusd budget", read `samples_total - le_25000` where
// samples_total = le_100000 + gt_100000.
//
// Codex P2 round 3 on PR #619 — single-bucket increments under `le_X`
// labels were misleading; le_25000 must include faster frames.
var adaptermuxSessionFrameLatencyBucketTotal = expvar.NewMap("adaptermux_session_frame_latency_us_bucket_total")

// adaptermuxSessionFrameLatencySlowThresholdMicros is the per-frame
// latency above which we emit a structured log line so operators can
// see concrete slow samples without parsing the bucket histogram. 25 ms
// matches ebusd's default --receivetimeout (the budget that, when
// exceeded, produces "read timeout" errors on ebusd's side).
const adaptermuxSessionFrameLatencySlowThresholdMicros int64 = 25_000

// monoAnchor is a process-start time.Time captured WITH its monotonic
// clock reading. Subtractions via time.Since(monoAnchor) preserve the
// monotonic component (per the time package docs: "later time-measuring
// operations, specifically comparisons and subtractions, use the
// monotonic clock reading"), which guarantees that wall-clock steps
// from NTP/chrony or manual `date -s` do not produce negative or
// inflated latency samples.
//
// We store frame timestamps as int64 nanoseconds *relative to this
// anchor* rather than as time.Time (24 B → 8 B per sessionFrame).
// Codex P2 round 2 on PR #619.
var monoAnchor = time.Now()

// recordLatencyBuckets increments every cumulative `le_X` bucket whose
// upper bound is ≥ elapsedUs, plus the `gt_100000` overflow bin when
// elapsedUs exceeds the highest le_* boundary.
//
// Cumulative semantics: a single sample at e.g. 500 µs increments
// le_1000, le_5000, le_25000, and le_100000 — so reading le_25000 at
// any point yields the exact count of frames that met the 25 ms ebusd
// budget. (Codex P2 round 3 on PR #619.)
func recordLatencyBuckets(elapsedUs int64) {
	if elapsedUs <= 1_000 {
		adaptermuxSessionFrameLatencyBucketTotal.Add("le_1000", 1)
	}
	if elapsedUs <= 5_000 {
		adaptermuxSessionFrameLatencyBucketTotal.Add("le_5000", 1)
	}
	if elapsedUs <= 25_000 {
		adaptermuxSessionFrameLatencyBucketTotal.Add("le_25000", 1)
	}
	if elapsedUs <= 100_000 {
		adaptermuxSessionFrameLatencyBucketTotal.Add("le_100000", 1)
	} else {
		// Overflow bin: outside every cumulative le_* boundary.
		adaptermuxSessionFrameLatencyBucketTotal.Add("gt_100000", 1)
	}
}

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
// endpoint (e.g., ebusd). Each session has its own send buffer.
//
// F-18 (_work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-12-batch13.md):
// the per-session echoTracker field was removed. External ENH sessions
// must receive their own post-arbitration echoes per
// john30/ebusd's enhanced_proto.md, which means there is no echo to
// "suppress" — the per-session echo tracker was load-bearing for the
// wrong behavior. Gateway-side echo accounting still uses
// m.gatewayEcho (the same echoTracker struct is retained), but it is
// strictly gateway-scoped.
type session struct {
	id   uint64
	conn net.Conn
	mux  *Mux

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

	// sawHandshake records whether the client has ever sent an INIT,
	// INFO, or START frame. Clients that have NOT performed any
	// handshake yet (sawHandshake.Load() == false) but flood the
	// listener with SEND bytes are almost certainly speaking raw TCP
	// rather than ENH — most commonly an ebusd configured with
	// `network_device: "HOST:PORT"` (no `enh:` scheme prefix).
	// Tracked atomically because handleMessage runs on the readLoop
	// goroutine while the diagnostic check below reads the value
	// without holding any session-wide lock.
	// (EBUSD-VERIFICATION-2026-05-10.md F-7 diagnostic.)
	sawHandshake atomic.Bool

	// rejectedSendsWithoutHandshake counts handleSend calls that hit
	// the not-bus-owner rejection path while sawHandshake is still
	// false. Once this exceeds rawTCPDiagnosticThreshold, the session
	// is closed with a clear "did you forget enh:HOST:PORT?" log line.
	rejectedSendsWithoutHandshake atomic.Uint32

	// F-34 (batch-30, iter11, 2026-05-15): "first SEND deadline"
	// watchdog support. Bumped on every accepted handleSend call.
	// The arbitrator's completeArbitrationGrant resets this to 0
	// when granting ownership to this session and starts a timer; if
	// the timer fires while this counter is still 0, the grant is
	// declared abandoned (ebusd silently dropped it due to bs_skip
	// state) and ownership is force-released.
	//
	// Live evidence (iter10): 23 `arbitration won in invalid state
	// skip` events / 60-scan tight burst, each costing ~250ms F-27
	// grace period before release. Reducing this to ~25ms watchdog
	// is the surgical fix.
	bytesSentSinceGrant atomic.Uint32

	// grantGeneration increments on every grant of this session via
	// completeArbitrationGrant. The F-34/F-35 abandon watchdog captures
	// the generation at arm time and only fires if the captured
	// generation still matches at deadline; otherwise the timer is for
	// an OLDER grant that has already been superseded by a fresh one,
	// and firing would force-release the new grant's ownership before
	// it can write any bytes (PR #633 P1 review on line 3196,
	// 2026-05-15).
	grantGeneration atomic.Uint64

	// wg waits for reader, writer, and handleStart goroutines to finish.
	wg sync.WaitGroup
}

// rawTCPDiagnosticThreshold is the number of consecutive SEND-rejections
// (with no preceding ENH handshake) after which the listener emits the
// F-7 raw-TCP diagnostic and closes the session. 16 chosen so a short
// burst of legitimate-but-misordered traffic doesn't trip it — the
// observed-in-the-wild pattern shows tens of rejections per second from
// a misconfigured client, so 16 fires within ~1 s of connect.
const rawTCPDiagnosticThreshold uint32 = 16

// sessionFrame is a frame to deliver to an external session.
type sessionFrame struct {
	kind    sessionFrameKind
	payload byte
	// enqueuedAtNano is the elapsed nanoseconds from the package-level
	// monoAnchor at the moment the frame was enqueued on sendCh. The
	// writeLoop computes latency as
	// `time.Since(monoAnchor).Nanoseconds() - frame.enqueuedAtNano`,
	// which is a pure monotonic-clock subtraction and is therefore
	// immune to wall-clock steps (NTP/chrony, manual `date -s`).
	//
	// We store an int64 (8 bytes) rather than a time.Time (24 bytes,
	// because of the wall/ext/Location triple) because every external
	// session preallocates an 8192-entry ring buffer (sendCh) and the
	// gateway may host up to maxSessions=1000 sessions; a 24-byte
	// field would inflate baseline channel capacity by ~128 MiB across
	// the session pool versus the 8-byte form (Codex P2 PR #619).
	//
	// Zero value indicates "not measured" — paths that bypass the
	// latency instrumentation may construct frames without setting
	// this field. The first frame enqueued in the program's first
	// nanosecond would also read zero, but that race is irrelevant
	// for a diagnostic histogram.
	//
	// (F-10 diagnostic instrumentation per EBUSD-VERIFICATION-2026-05-10.md.)
	enqueuedAtNano int64
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
		id:     id,
		conn:   conn,
		mux:    m,
		sendCh: make(chan sessionFrame, defaultSessionSendBuffer),
		done:   make(chan struct{}),
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

	// Phase 3 Step B3.6c: pre-create the per-session pacer when
	// V8ClassifierMode != Off. SessionPacer is load-only (per
	// Codex round-1 MEDIUM on PR #645 — load-only eliminates the
	// RemoveSession+lazy-create resurrection race). The
	// ensureSessionPacer helper is the LIFECYCLE-ONLY
	// constructor invoked here and in Mux.New.
	m.ensureSessionPacer(id)

	// Populate the lock-free RemoteAddr side index so diagnostic logs
	// taken from stateMu-holding code paths (onSYNLocked, etc.) can
	// label themselves with the client endpoint without sessionsMu.
	if remote := conn.RemoteAddr(); remote != nil {
		m.sessionRemoteAddrs.Store(id, remote.String())
	}

	sess.wg.Add(2)
	go sess.readLoop()  // goroutine: reads ENH commands from client
	go sess.writeLoop() // goroutine: writes ENH responses to client

	m.logger.Printf("adaptermux: session %d connected from %s", id, conn.RemoteAddr())
	return id
}

// sessionRemoteAddrOrUnknown returns the cached RemoteAddr string for
// the given session ID, or "unknown" if the session has never been
// registered or has already been removed. Safe to call from any
// goroutine and from any lock-holding context — the underlying
// sync.Map performs no ordering with other mutexes.
func (m *Mux) sessionRemoteAddrOrUnknown(id uint64) string {
	if v, ok := m.sessionRemoteAddrs.Load(id); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return "unknown"
}

// RemoveSession disconnects and cleans up an external session.
func (m *Mux) RemoveSession(id uint64) {
	m.sessionsMu.Lock()
	sess, ok := m.sessions[id]
	if ok {
		delete(m.sessions, id)
	}
	m.sessionsMu.Unlock()
	// Drop the lock-free RemoteAddr side index entry too. Done after
	// sessionsMu is released so this never blocks the critical section.
	m.sessionRemoteAddrs.Delete(id)

	// Phase 3 Step B3.6c: drop the per-session v8 pacer. No-op
	// when V8ClassifierMode == Off (the entry was never created).
	//
	// Phase 3 Step B3.6e: cancel any pending echo-watchdog timers
	// BEFORE the Delete so a delayed soft/hard timer fire after
	// the session is gone cannot emit a stale ClassifierAdminEvent
	// for a sessionID that no longer exists. The cancel runs
	// idempotent if the watchdog was never armed.
	if v, ok := m.sessionPacers.Load(id); ok {
		if pacer, _ := v.(*v8classifier.Pacer); pacer != nil {
			pacer.CancelEchoWatchdog()
		}
	}
	m.sessionPacers.Delete(id)

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
	case s.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: symbol, enqueuedAtNano: time.Since(monoAnchor).Nanoseconds()}:
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
	case s.sendCh <- sessionFrame{kind: sessionFrameResetted, payload: payload, enqueuedAtNano: time.Since(monoAnchor).Nanoseconds()}:
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
	case s.sendCh <- sessionFrame{kind: sessionFrameStarted, payload: payload, enqueuedAtNano: time.Since(monoAnchor).Nanoseconds()}:
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
	case s.sendCh <- sessionFrame{kind: sessionFrameFailed, payload: payload, enqueuedAtNano: time.Since(monoAnchor).Nanoseconds()}:
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
		// F-6 observability: log so operators can correlate orphan
		// SEND attempts with missing/lost STARTs
		// (EBUSD-VERIFICATION-2026-05-10.md).
		s.mux.logger.Printf("adaptermux: session %d SEND 0x%02X rejected — session does not own bus", s.id, data)
		// F-7 diagnostic: a client that has never sent an ENH
		// INIT/INFO/START but floods the listener with SEND
		// rejections is almost certainly speaking raw TCP, not ENH.
		// The most common cause (observed in the wild) is an ebusd
		// configured with `network_device: "HOST:PORT"` instead of
		// `network_device: "enh:HOST:PORT"` — ebusd then transmits
		// raw eBUS bytes, our parser decodes each one as a short-
		// form ENHResReceived (== SEND), and every byte fails the
		// owner check. After rawTCPDiagnosticThreshold consecutive
		// rejections without a handshake, emit a clear diagnostic
		// and close the session so the misconfiguration is obvious
		// instead of an ever-spinning log spam.
		// (EBUSD-VERIFICATION-2026-05-10.md F-7.)
		if !s.sawHandshake.Load() {
			n := s.rejectedSendsWithoutHandshake.Add(1)
			if n == rawTCPDiagnosticThreshold {
				remote := "unknown"
				if s.conn != nil && s.conn.RemoteAddr() != nil {
					remote = s.conn.RemoteAddr().String()
				}
				s.mux.logger.Printf("adaptermux: session %d (%s) sent %d SEND frames with no preceding ENH INIT/INFO/START — closing as suspected raw-TCP client (did you forget the `enh:` scheme prefix? e.g. ebusd `network_device: enh:HOST:PORT`)",
					s.id, remote, rawTCPDiagnosticThreshold)
				go s.mux.RemoveSession(s.id) // goroutine: close out-of-band so handleMessage can return
			}
		}
		s.deliverErrorHost()
		return
	}
	// F-34 (iter11): bump the bytes-since-grant counter so the
	// abandon watchdog (started in completeArbitrationGrant) can
	// distinguish a live transaction from a silently-dropped grant.
	s.bytesSentSinceGrant.Add(1)

	// F-6 observability: log accepted SEND bytes too. Without this, the
	// happy-path forwarding to activeSendCh is silent and session logs
	// can show START/INIT/INFO with no per-session SEND traffic even
	// though bytes are flowing to the bus
	// (EBUSD-VERIFICATION-2026-05-10.md, Codex P2 follow-up on PR #617).
	s.mux.logger.Printf("adaptermux: session %d SEND 0x%02X forwarded", s.id, data)

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
		s.mux.logger.Printf("adaptermux: session %d START 0xAA (SYN cancel)", s.id)
		s.sawHandshake.Store(true) // F-7: SYN-cancel is still a START
		s.mux.arb.cancelStart(s.id)
		s.mux.arb.releaseOwnership(s.id)
		s.mux.cancelPendingStart(s.id)
		return
	}

	// F-6 observability: log every START dispatch so operators can
	// distinguish "no client traffic" from "client traffic but no
	// grants" when debugging arbitration starvation
	// (EBUSD-VERIFICATION-2026-05-10.md).
	s.mux.logger.Printf("adaptermux: session %d START 0x%02X requested (RequestStart(0x%02X) sent for session %d)", s.id, initiator, initiator, s.id)
	s.sawHandshake.Store(true) // F-7 diagnostic: legitimate ENH client
	// Use the Mux wrapper so the proxy-bug C4 in-flight cancel and
	// C1 enqueue-kick fire on every external START submission.
	ch := s.mux.requestStartForSession(s.id, initiator)

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
	// F-6 observability: log INIT handshake so operators can confirm
	// ENH framing reached the mux (EBUSD-VERIFICATION-2026-05-10.md).
	s.mux.logger.Printf("adaptermux: session %d INIT features=0x%02X (stored=0x%02X)", s.id, features, stored)
	s.sawHandshake.Store(true) // F-7 diagnostic: legitimate ENH client
	s.deliverReset(stored)
}

// handleInfo processes an INFO request from the client.
// Reads from the mux-level INFO cache (populated at connect time)
// instead of querying the upstream transport directly, avoiding
// readMu contention with the readLoop.
func (s *session) handleInfo(id byte) {
	// F-6 observability: log INFO dispatch (EBUSD-VERIFICATION-2026-05-10.md).
	s.mux.logger.Printf("adaptermux: session %d INFO id=0x%02X", s.id, id)
	s.sawHandshake.Store(true) // F-7 diagnostic: legitimate ENH client
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
	case s.sendCh <- sessionFrame{kind: sessionFrameErrorEBUS, enqueuedAtNano: time.Since(monoAnchor).Nanoseconds()}:
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
	case s.sendCh <- sessionFrame{kind: sessionFrameErrorHost, enqueuedAtNano: time.Since(monoAnchor).Nanoseconds()}:
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
	case s.sendCh <- sessionFrame{kind: sessionFrameInfo, payload: b, enqueuedAtNano: time.Since(monoAnchor).Nanoseconds()}:
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
			err := s.writeFrame(frame)
			// F-10 instrumentation: measure enqueue→TCP-write latency
			// for every frame so operators can correlate "send to fe:
			// ERR: read timeout" events on ebusd's side with concrete
			// pipeline-latency samples. ebusd's 25 ms per-byte budget
			// is the threshold: frames slower than that get a log line
			// in addition to bucket counts. (Measure on every frame,
			// not just slow ones, so the histogram surface is complete.)
			if frame.enqueuedAtNano != 0 {
				// Monotonic subtraction relative to monoAnchor; immune
				// to wall-clock steps (Codex P2 round 2 on PR #619).
				elapsedUs := (time.Since(monoAnchor).Nanoseconds() - frame.enqueuedAtNano) / 1_000
				recordLatencyBuckets(elapsedUs)
				if elapsedUs > adaptermuxSessionFrameLatencySlowThresholdMicros && !s.closed.Load() {
					s.mux.logger.Printf("adaptermux: session %d frame delivery slow: kind=%d latency=%dus (threshold=%dus — exceeds ebusd's default --receivetimeout)",
						s.id, frame.kind, elapsedUs, adaptermuxSessionFrameLatencySlowThresholdMicros)
				}
			}
			if err != nil {
				if !s.closed.Load() && !errors.Is(err, net.ErrClosed) {
					s.mux.logger.Printf("adaptermux: session %d write error: %v", s.id, err)
				}
				// AM46+Codex: mark closed and close resources directly.
				// We must close done+conn HERE because RemoveSession's
				// sess.close() checks closed.Swap(true) and would no-op
				// if we already set closed=true. Without closing conn,
				// readLoop stays blocked on Read() indefinitely.
				if !s.closed.Swap(true) {
					close(s.done)
					_ = s.conn.Close()
				}
				// Drain remaining frames so blocked senders are unblocked.
			drainLoop:
				for {
					select {
					case <-s.sendCh:
					default:
						break drainLoop
					}
				}
				go s.mux.RemoveSession(s.id)
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

	// B3.6 INTEGRATION HOOK POINT (Phase 3, frame-atomic-visibility
	// v8 §4.5): this is the TCP-egress choke point where the
	// v8classifier.Pacer's Schedule must be consulted when
	// V8ClassifierMode != Off and the session is a cross-proxy
	// observer. See internal/adaptermux/v8classifier/pacer.go for
	// the API; the pacer is a per-session struct that lands in
	// B3.6 (not yet instantiated in B3.5). Sequence per the
	// pacer's design:
	//   emitAt := pacer.Schedule(time.Now())
	//   if emitAt.After(time.Now()) { sleep until emitAt }
	//   s.conn.Write(buf)                          <-- here
	// Do NOT conflate this hook point with mux.doSend's tr.Write
	// (that's the L_rtt-EMA / BeforeActiveWrite side per v8 §1.4,
	// which uses BeforeActiveWrite + EchoDeadlines + RecordEcho
	// against the adapter, NOT the client's TCP egress).
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
