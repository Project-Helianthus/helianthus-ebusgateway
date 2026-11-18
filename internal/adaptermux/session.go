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

var adaptermuxSessionBacklogFlushesTotal = expvar.NewInt("adaptermux_session_backlog_flushes_total")

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
	// defaultSessionSendBuffer bounds retained frames, including writer
	// lookahead, for external sessions. If the channel fills, the session is
	// forcibly closed (backpressure protection, from proxy convention).
	defaultSessionSendBuffer = 8192

	maxSessionPacerBacklog = 2
	sessionWriterLookahead = maxSessionPacerBacklog + 1

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
	grantDelivered atomic.Bool

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

	// Writer-owned state; backlogFlushes is atomic for diagnostics.
	flushBacklog   bool
	backlogFlushes atomic.Uint64

	// wg waits for reader, writer, and handleStart goroutines to finish.
	wg sync.WaitGroup

	// infoHold guards the queued external bid and the INFO answer held
	// until the grant step that included that bid returns from RequestStart.
	infoHold   sync.Mutex
	heldReq    *startRequest
	heldEpoch  uint64
	parkedInfo []sessionFrame
	// boundReq is the bid the last START of this session bound, read by
	// handleStart so the answer can follow that bid's result.
	boundReq *startRequest

	// writeMu serializes one frame write. Removal flushes a held answer
	// only when the writer is not already inside a write.
	writeMu sync.Mutex
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
	// grantGen is set on a STARTED frame queued by the mux at grant time
	// (issue #1003): the low 32 bits of the session's grantGeneration for
	// that grant. The writer arms the F-34/F-35 abandon watchdog for this
	// grant once the frame reached the client. Zero means "no watchdog".
	// A uint32 keeps the frame at 16 bytes next to enqueuedAtNano.
	grantGen uint32
	// enqueuedAtNano is the elapsed nanoseconds from the package-level
	// monoAnchor at the moment the frame was enqueued on sendCh. The
	// writeLoop computes latency as
	// `time.Since(monoAnchor).Nanoseconds() - frame.enqueuedAtNano`,
	// which is a pure monotonic-clock subtraction and is therefore
	// immune to wall-clock steps (NTP/chrony, manual `date -s`).
	//
	// We store an int64 (8 bytes) rather than a time.Time (24 bytes,
	// because of the wall/ext/Location triple) because every external
	// session reserves an 8192-frame budget (sendCh plus lookahead) and the
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
// goroutines. Returns 0 if the mux is shutting down (context cancelled) or a
// managed connection loss has delegated replacement to the lifecycle owner;
// the connection is closed and no goroutines are leaked.
func (m *Mux) AddSession(conn net.Conn) uint64 {
	managedGateHeld, admitted := m.beginManagedConnectionUse()
	if !admitted {
		_ = conn.Close()
		m.logger.Printf("adaptermux: rejecting session — managed generation retired")
		return 0
	}
	defer m.endManagedConnectionUse(managedGateHeld)
	if m.ctx == nil || m.ctx.Err() != nil {
		_ = conn.Close()
		m.logger.Printf("adaptermux: rejecting session — mux not available")
		return 0
	}

	id := m.nextSessionID()
	sess := &session{
		id:     id,
		conn:   conn,
		mux:    m,
		sendCh: make(chan sessionFrame, defaultSessionSendBuffer-sessionWriterLookahead),
		done:   make(chan struct{}),
	}

	// AM25/AM50: check + insert under a single lock to prevent TOCTOU.
	m.sessionsMu.Lock()
	// Fatal listener retirement does not take connectionUseMu's write side:
	// an already-admitted START/SEND may be blocked in the upstream transport.
	// Recheck the atomic fence under the session-map lock instead, so an
	// AddSession that acquired its managed read lease before withdrawal cannot
	// insert after RetireManagedProxySessions detached the generation's map.
	if m.connectionLossManaged.Load() && m.connectionLossDelegated.Load() {
		m.sessionsMu.Unlock()
		m.logger.Printf("adaptermux: rejecting session — managed generation retired")
		_ = conn.Close()
		return 0
	}
	if len(m.sessions) >= maxSessions {
		m.sessionsMu.Unlock()
		m.logger.Printf("adaptermux: rejecting session — max sessions (%d) reached", maxSessions)
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
	// Installed before the session goroutines run, so a bid that fails
	// before its grant can release or discard the hold.
	m.arb.setInfoHoldHooks(
		func(req *startRequest) { m.bindSessionInfoHold(req.sessionID, req) },
		m.releaseHandedInfoHold,
		releaseBoundInfoHold,
		dropBoundInfoHold,
	)

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
		// The session leaves the map before cancelStart. Deliver the held
		// answer first, while the writer can still push it to the client.
		sess.flushInfoHoldOnRemoval()
		m.releaseSessionGrant(sess)
		m.arb.cancelStart(id)
		m.cancelPendingStart(id)
		sess.close()
		m.logger.Printf("adaptermux: session %d disconnected", id)
	}
}

// releaseSessionGrant retires an unused win behind the next SYN boundary.
func (m *Mux) releaseSessionGrant(sess *session) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.arb.isOwner(sess.id) {
		m.fenceArbitrationUntilSYNLocked()
		if !sess.grantDelivered.Load() {
			m.undeliverableStarted.Add(1)
		}
	}
	m.arb.releaseOwnership(sess.id)
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
		s.mux.logger.Printf("adaptermux: session %d close timed out — wg.Wait pending, will self-resolve on I/O unblock", s.id)
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

// deliverStartedForGrant queues the STARTED of a grant the mux just
// confirmed, tagged with that grant's generation so the writer can arm the
// abandon watchdog when the frame becomes visible to the client.
func (s *session) deliverStartedForGrant(payload byte, grantGen uint64) bool {
	if s.closed.Load() {
		return false
	}
	// AM23: the parser reset that follows arbitration must be armed
	// before the client can see STARTED and start sending.
	s.parserResetNeeded.Store(true)
	select {
	case s.sendCh <- sessionFrame{kind: sessionFrameStarted, payload: payload, grantGen: uint32(grantGen), enqueuedAtNano: time.Since(monoAnchor).Nanoseconds()}:
		return true
	default:
		go s.mux.RemoveSession(s.id) // goroutine: overflow removal
		return false
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
	// STARTED can already be visible while completeArbitrationGrant holds
	// stateMu. Wait for that commit before checking ownership, and release
	// the lock before any potentially blocking send or transport operation.
	//
	// The bytes-since-grant counter lets the abandon watchdog tell a live
	// transaction from a silently dropped grant. It is bumped in the same
	// critical section as the owner check because the watchdog decides
	// under stateMu: it either sees this SEND or releases the grant before it.
	s.mux.stateMu.Lock()
	isOwner := s.mux.arb.isOwner(s.id)
	if isOwner {
		s.mux.phase.markOwnerWrite()
		s.bytesSentSinceGrant.Add(1)
	}
	s.mux.stateMu.Unlock()
	if !isOwner {
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
		s.releaseAnyInfoHold()
		s.mux.releaseSessionGrant(s)
		s.mux.cancelPendingStart(s.id)
		return
	}

	// F-6 observability: log every START dispatch so operators can
	// distinguish "no client traffic" from "client traffic but no
	// grants" when debugging arbitration starvation
	// (EBUSD-VERIFICATION-2026-05-10.md).
	s.mux.logger.Printf("adaptermux: session %d START 0x%02X requested (RequestStart(0x%02X) sent for session %d)", s.id, initiator, initiator, s.id)
	s.sawHandshake.Store(true) // F-7 diagnostic: legitimate ENH client
	// Submit this session's bid to the next idle SYN window.
	s.takeBoundReq()
	ch := s.mux.requestStartForSession(s.id, initiator)
	bid := s.takeBoundReq()

	// Wait for arbitration result in a tracked goroutine.
	s.wg.Add(1)
	go func() { // goroutine: wait for START arbitration result
		defer s.wg.Done()
		select {
		case result := <-ch:
			// AM23: signal readLoop to reset ENH parser after arbitration
			// completes (enh.md:128). Set for every outcome — parser
			// state may be stale regardless of grant/fail outcome —
			// except an outcome the mux already queued: those paths
			// armed the reset before publishing STARTED or FAILED.
			// Re-arming here could discard a split SEND or re-bid
			// that the client began after seeing the control frame.
			if !result.startedQueued && !result.failedQueued {
				s.parserResetNeeded.Store(true)
			}
			if s.closed.Load() {
				return
			}
			if result.granted {
				// Issue #1003: the mux queues the grant's STARTED at its
				// stream position; deliver here only for a grant that
				// was not queued by the mux.
				if !result.startedQueued {
					s.deliverStarted(result.initiator)
				}
			} else if result.cancelled {
				// AM55 fix: client-initiated SYN cancel — the client
				// already knows it cancelled, don't deliver spurious FAILED.
				// A same-address re-bid keeps this request, so its held
				// answer stays with that re-bid.
				return
			} else if result.err != nil && isResetOrDisconnectError(result.err) {
				// Reset/disconnect caused the START failure — deliver
				// RESETTED so the client sees the correct boundary event
				// instead of a spurious collision (P1 fix).
				s.deliverReset(byte(s.mux.upstreamFeatures.Load()))
			} else if !result.failedQueued {
				s.deliverFailed(result.initiator)
			}
			// A START that never reached the adapter keeps its INFO
			// answer parked until here, so the answer follows the result
			// and cannot show that bid in flight.
			s.releaseInfoHold(bid)
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
	frames := s.infoAnswer(id)
	if s.parkInfo(frames) {
		return
	}
	s.deliverInfoFrames(frames)
}

// infoAnswer is the cached INFO body, or ERROR_HOST when the cache cannot
// answer. The bytes are the whole response, in order.
func (s *session) infoAnswer(id byte) []sessionFrame {
	now := time.Since(monoAnchor).Nanoseconds()
	data, err := s.mux.CachedInfo(transport.AdapterInfoID(id))
	if err != nil {
		s.mux.logger.Printf("adaptermux: session %d INFO id=0x%02X: %v", s.id, id, err)
		return []sessionFrame{{kind: sessionFrameErrorHost, enqueuedAtNano: now}}
	}
	// AM39: guard against payload >255 which would silently truncate
	// via byte(len(data)) wrap-around.
	if len(data) > 255 {
		s.mux.logger.Printf("adaptermux: session %d INFO id=0x%02X payload too large (%d bytes, max 255)", s.id, id, len(data))
		return []sessionFrame{{kind: sessionFrameErrorHost, enqueuedAtNano: now}}
	}
	// AM35: zero-length INFO payload is valid per ENH spec (N=0).
	// Deliver just the length prefix (0x00) with no data bytes.
	frames := make([]sessionFrame, 0, 1+len(data))
	frames = append(frames, sessionFrame{kind: sessionFrameInfo, payload: byte(len(data)), enqueuedAtNano: now})
	for _, b := range data {
		frames = append(frames, sessionFrame{kind: sessionFrameInfo, payload: b, enqueuedAtNano: now})
	}
	return frames
}

// parkInfo holds frames while this session has a queued bid that the
// including grant step has not yet returned from RequestStart. The parked
// queue uses the same frame budget as the session send buffer. Overflow
// rejects the session instead of growing without that bound.
func (s *session) parkInfo(frames []sessionFrame) bool {
	s.infoHold.Lock()
	if s.heldReq == nil {
		s.infoHold.Unlock()
		return false
	}
	if len(s.parkedInfo)+len(frames) > defaultSessionSendBuffer {
		s.heldReq = nil
		s.parkedInfo = nil
		s.heldEpoch = 0
		s.infoHold.Unlock()
		go s.mux.RemoveSession(s.id) // goroutine: parked-answer overflow
		return true
	}
	s.parkedInfo = append(s.parkedInfo, frames...)
	s.infoHold.Unlock()
	return true
}

// bindInfoHold remembers the session's current queued bid. A replacement
// keeps answers already parked so they are released for the new bid.
// A bid that already left the queue does not keep the hold: failure can
// run as soon as the bid is visible and before this bind, and that answer
// is delivered rather than dropped.
func (s *session) bindInfoHold(req *startRequest) {
	if req == nil {
		return
	}
	req.infoSession.Store(s)
	s.infoHold.Lock()
	s.boundReq = req
	s.heldReq = req
	s.heldEpoch = s.mux.arb.lifecycleEpoch.Load()
	s.infoHold.Unlock()
	if s.mux.arb.externalBid(s.id) != req {
		s.releaseInfoHold(req)
	}
}

// takeBoundReq returns the bid the last START bound, and clears it.
func (s *session) takeBoundReq() *startRequest {
	s.infoHold.Lock()
	defer s.infoHold.Unlock()
	req := s.boundReq
	s.boundReq = nil
	return req
}

// releaseAnyInfoHold delivers whatever answer this session is holding.
func (s *session) releaseAnyInfoHold() {
	s.infoHold.Lock()
	req := s.heldReq
	s.infoHold.Unlock()
	if req != nil {
		s.releaseInfoHold(req)
	}
}

// releaseInfoHold delivers the parked answer when req is the bid the hold
// is for. The bid stays held until every parked frame has been queued, so
// an INFO that arrives during delivery parks behind those frames instead of
// passing them. A lifecycle fence after the bind discards the answer so a
// late RequestStart cannot deliver it.
func (s *session) releaseInfoHold(req *startRequest) {
	if req == nil {
		return
	}
	for {
		s.infoHold.Lock()
		if s.heldReq != req {
			s.infoHold.Unlock()
			return
		}
		if s.heldEpoch != s.mux.arb.lifecycleEpoch.Load() {
			s.heldReq = nil
			s.parkedInfo = nil
			s.heldEpoch = 0
			s.infoHold.Unlock()
			return
		}
		frames := s.parkedInfo
		s.parkedInfo = nil
		if len(frames) == 0 {
			s.heldReq = nil
			s.heldEpoch = 0
			s.infoHold.Unlock()
			return
		}
		s.infoHold.Unlock()
		s.deliverInfoFrames(frames)
	}
}

// dropInfoHold discards a parked answer. A nil req clears whatever is held;
// a non-nil req clears only that bid, so a replacement's answer stays.
func (s *session) dropInfoHold(req *startRequest) {
	s.infoHold.Lock()
	defer s.infoHold.Unlock()
	if req != nil && s.heldReq != req {
		return
	}
	s.heldReq = nil
	s.parkedInfo = nil
	s.heldEpoch = 0
}

// releaseBoundInfoHold delivers the answer held for req. The session pointer
// is taken from the bid, so this still runs after the session leaves the map.
func releaseBoundInfoHold(req *startRequest) {
	if req == nil {
		return
	}
	if sess := req.infoSession.Load(); sess != nil {
		sess.releaseInfoHold(req)
	}
}

// dropBoundInfoHold discards the answer held for req. The session pointer
// is taken from the bid, so this still runs after the session leaves the map.
func dropBoundInfoHold(req *startRequest) {
	if req == nil {
		return
	}
	if sess := req.infoSession.Load(); sess != nil {
		sess.dropInfoHold(req)
	}
}

// flushInfoHoldOnRemoval delivers the held answer before the session's
// writer is stopped. A direct write is used when the writer is idle, so
// the bytes reach the client before the connection closes.
func (s *session) flushInfoHoldOnRemoval() {
	s.infoHold.Lock()
	req := s.heldReq
	epochOK := req != nil && s.heldEpoch == s.mux.arb.lifecycleEpoch.Load()
	frames := s.parkedInfo
	if req != nil {
		s.heldReq = nil
		s.parkedInfo = nil
		s.heldEpoch = 0
	}
	s.infoHold.Unlock()
	if req == nil || !epochOK || len(frames) == 0 {
		return
	}
	if len(s.sendCh) == 0 && s.writeMu.TryLock() {
		for _, frame := range frames {
			_ = s.writeFrame(frame)
		}
		s.writeMu.Unlock()
		return
	}
	s.deliverInfoFrames(frames)
}

// releaseHandedInfoHold delivers the answers of the external bids that lost
// the selection: their FAILED is already queued. The handed bid's answer is
// delivered only when its START reached the adapter. A failed START keeps
// it parked until the session has queued that bid's result, and a pending
// RequestStart outcome is resolved by releaseAcceptedInfoHold.
func (m *Mux) releaseHandedInfoHold(outcome grantReturn) {
	req, losers := m.arb.takeHandedGrant()
	for _, loser := range losers {
		if loser != req {
			releaseBoundInfoHold(loser)
		}
	}
	if req == nil {
		return
	}
	switch outcome {
	case grantReturnAccepted:
		releaseBoundInfoHold(req)
	case grantReturnPending:
		m.arb.noteReturnedGrant(req)
	}
}

// releaseAcceptedInfoHold runs after the grant step has returned. It
// delivers the answer of the bid whose RequestStart returned when that bid
// is still the START awaiting the adapter's answer. A failed write has
// already withdrawn that START, so its answer waits for the result.
func (m *Mux) releaseAcceptedInfoHold() {
	req := m.arb.takeReturnedGrant()
	if req == nil {
		return
	}
	m.stateMu.Lock()
	accepted := m.pendingStart != nil && m.pendingStart.req == req && !m.pendingStart.responseReceived
	m.stateMu.Unlock()
	if accepted {
		releaseBoundInfoHold(req)
	}
}

// dropAllInfoHolds discards every session's held answer. Used at an adapter
// reset, disconnect, or close, including a bid already handed to RequestStart.
func (m *Mux) dropAllInfoHolds() {
	m.sessionsMu.Lock()
	sessions := make([]*session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		sessions = append(sessions, sess)
	}
	m.sessionsMu.Unlock()
	for _, sess := range sessions {
		sess.dropInfoHold(nil)
	}
}

func (s *session) deliverInfoFrames(frames []sessionFrame) {
	for _, frame := range frames {
		switch frame.kind {
		case sessionFrameErrorHost:
			s.deliverErrorHost()
		case sessionFrameInfo:
			s.deliverInfo(frame.payload)
		default:
			s.mux.logger.Printf("adaptermux: session %d dropped parked frame kind %d", s.id, frame.kind)
		}
	}
}

func (m *Mux) sessionByID(sessionID uint64) *session {
	if sessionID == gatewaySessionID {
		return nil
	}
	m.sessionsMu.Lock()
	sess := m.sessions[sessionID]
	m.sessionsMu.Unlock()
	return sess
}

func (m *Mux) bindSessionInfoHold(sessionID uint64, req *startRequest) {
	if sess := m.sessionByID(sessionID); sess != nil {
		sess.bindInfoHold(req)
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

	var lookahead [sessionWriterLookahead]sessionFrame
	pending := lookahead[:0]
	flushRemaining := 0
	for {
		if len(pending) == 0 {
			select {
			case frame := <-s.sendCh:
				pending = append(pending, frame)
			case <-s.done:
				return
			case <-s.mux.ctx.Done():
				return
			}
		}
		select {
		case <-s.done:
			return
		case <-s.mux.ctx.Done():
			return
		default:
		}
		// ModeOff retains its FIFO, zero-pacer path. Snapshot only frames
		// already available, so a producer cannot keep the writer draining.
		if pacer := s.mux.SessionPacer(s.id); pacer != nil {
			// Look ahead only far enough to detect a symbol backlog. Never
			// drain a full channel into a second buffer while TCP is stalled.
			for n := len(s.sendCh); n > 0 && len(pending) <= maxSessionPacerBacklog; n-- {
				pending = append(pending, <-s.sendCh)
			}
			queuedSymbols := 0
			for _, frame := range pending {
				if frame.kind == sessionFrameReceived {
					queuedSymbols++
					if queuedSymbols > maxSessionPacerBacklog {
						break
					}
				}
			}
			if !s.flushBacklog && queuedSymbols > maxSessionPacerBacklog {
				s.flushBacklog = true
				flushRemaining = len(pending) + len(s.sendCh)
				total := s.backlogFlushes.Add(1)
				adaptermuxSessionBacklogFlushesTotal.Add(1)
				s.mux.logger.Printf("adaptermux: session %d frame latency backlog flush: backlog_flushes=%d", s.id, total)
			}
			// Outside a backlog flush, drain the older RECEIVED prefix without
			// pacing through its arbitration notification. Keep strict FIFO so
			// pre-grant traffic cannot be mistaken for a post-grant SEND echo.
			// Other controls fence this scan; the backlog counter is unchanged.
			if !s.flushBacklog {
				for i, frame := range pending {
					if frame.kind == sessionFrameReceived {
						continue
					}
					if i > 0 && (frame.kind == sessionFrameStarted || frame.kind == sessionFrameFailed) {
						s.flushBacklog = true
						flushRemaining = i + 1
					}
					break
				}
			}
			if !s.flushBacklog && pending[0].kind == sessionFrameReceived {
				if delay := time.Until(pacer.NextOutput(time.Now())); delay > 0 {
					timer := time.NewTimer(delay)
					select {
					case <-timer.C:
					case frame := <-s.pacerArrival(pending):
						timer.Stop()
						pending = append(pending, frame)
						continue
					case <-s.done:
						timer.Stop()
						return
					case <-s.mux.ctx.Done():
						timer.Stop()
						return
					}
				}
			}
		}
		frame := pending[0]
		copy(pending, pending[1:])
		pending = pending[:len(pending)-1]
		s.writeMu.Lock()
		err := s.writeFrame(frame)
		s.writeMu.Unlock()
		if err == nil && frame.kind == sessionFrameStarted && frame.grantGen != 0 && s.mux != nil {
			s.grantDelivered.Store(true)
			// Issue #1003: the grant became visible to the client
			// only now; start its abandon deadline here, not at the
			// upstream grant.
			s.mux.armExternalAbandonWatchdog(s, frame.grantGen)
		}
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
				s.mux.logger.Printf("adaptermux: session %d frame delivery slow: kind=%d latency=%dus backlog_flushes=%d (threshold=%dus — exceeds ebusd's default --receivetimeout)",
					s.id, frame.kind, elapsedUs, s.backlogFlushes.Load(), adaptermuxSessionFrameLatencySlowThresholdMicros)
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
		if s.flushBacklog {
			flushRemaining--
			if flushRemaining == 0 {
				s.flushBacklog = false
			}
		}
	}
}

// pacerArrival bounds writer lookahead even when controls occupy the window.
func (s *session) pacerArrival(pending []sessionFrame) <-chan sessionFrame {
	if len(pending) > maxSessionPacerBacklog {
		return nil
	}
	return s.sendCh
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

	// Keep ordinary mirrored symbols at wire cadence. A queued burst is
	// drained without sleeps and rebases the cadence on actual egress.
	// STARTED/FAILED bypass scheduling; every ENH frame remains one Write.
	var flushPacer *v8classifier.Pacer
	if s.mux != nil && s.mux.v8 != nil {
		if pacer := s.mux.SessionPacer(s.id); pacer != nil {
			if s.flushBacklog {
				flushPacer = pacer
			} else if frame.kind != sessionFrameStarted && frame.kind != sessionFrameFailed {
				if emitAt, paced := reserveSymbolSlots(pacer, time.Now(), sessionFrameSymbols(frame.kind)); paced {
					if delay := time.Until(emitAt); delay > 0 {
						timer := time.NewTimer(delay)
						select {
						case <-timer.C:
						case <-s.done:
							timer.Stop()
							return net.ErrClosed
						case <-s.mux.ctx.Done():
							timer.Stop()
							return s.mux.ctx.Err()
						}
					}
				}
			}
		}
	}
	_, err := s.conn.Write(buf)
	if err == nil && flushPacer != nil {
		// A blocked Write must not leave the next ordinary slot in the past.
		flushPacer.ResumeOutput(time.Now())
	}
	return err
}

// sessionFrameSymbols reports how many eBUS wire symbols a session frame
// represents for output pacing (issue #1003). RECEIVED carries one observed
// wire symbol regardless of its ENH encoding length; STARTED and FAILED
// stand for the single arbitration symbol the adapter consumed; control
// frames (RESETTED, ERROR_EBUS, ERROR_HOST, INFO) carry none.
func sessionFrameSymbols(kind sessionFrameKind) int {
	switch kind {
	case sessionFrameReceived, sessionFrameStarted, sessionFrameFailed:
		return 1
	default:
		return 0
	}
}

// reserveSymbolSlots reserves one pacer slot per wire symbol and returns the
// emit time of the first one. paced is false for a frame without wire
// symbols, which is written without consulting the pacer.
func reserveSymbolSlots(pacer *v8classifier.Pacer, now time.Time, symbols int) (emitAt time.Time, paced bool) {
	if symbols <= 0 {
		return time.Time{}, false
	}
	emitAt = pacer.Schedule(now)
	for i := 1; i < symbols; i++ {
		_ = pacer.Schedule(now)
	}
	return emitAt, true
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
