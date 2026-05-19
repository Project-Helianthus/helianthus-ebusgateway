package adaptermux

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol/telegram_fsm"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
)

// Phase 3 Step B3.2: integration tests for the v8 classifier
// wiring at the adaptermux.Mux boundary. These pin:
//
//   - cfg.V8ClassifierMode = ModeOff yields a nil Mux.V8Classifier()
//     (no allocation, no observation overhead).
//   - cfg.V8ClassifierMode = ModeShadow / ModeEnforce instantiates a
//     classifier whose ObservedBytesTotal increments as the readLoop
//     dispatches stream events.
//   - The classifier's presence does NOT alter the byte stream
//     forwarded to sessions (legacy adaptermux tests in the same
//     package already pin that — they all run with the default
//     ModeOff). The shadow-mode test below confirms the BYTES counter
//     bumps without exercising any session-level assertion (which is
//     the same null behavior as ModeOff).

// newClassifiedTestMux is a variant of newP3TestMux that lets a test
// override the V8ClassifierMode. The rest of the configuration
// matches newP3TestMux exactly so legacy invariants stay aligned.
func newClassifiedTestMux(t *testing.T, mode v8classifier.Mode) (*Mux, *p3MockTransport, context.CancelFunc, func()) {
	t.Helper()

	mock := newP3MockTransport()

	mux := New(Config{
		Protocol:         "enh",
		Network:          "tcp",
		Address:          "127.0.0.1:0",
		ReadTimeout:      200 * time.Millisecond,
		PendingStartTTL:  24 * time.Hour,
		SYNInterval:      time.Hour,
		V8ClassifierMode: mode,
	})

	ctx, cancel := context.WithCancel(context.Background())
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	mux.stateMu.Lock()
	mux.lastWireActivity = time.Now()
	mux.stateMu.Unlock()

	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()

	cleanup := func() {
		cancel()
		closeOrLog(t, mock, "p3 mock transport")
		mux.wg.Wait()
	}

	return mux, mock, cancel, cleanup
}

// TestV8Classifier_OffMode_NilClassifier pins the production-default
// invariant: ModeOff (the zero value) yields a nil classifier
// instance on the Mux. This is the production-default behavior
// through Phase 3 until B3.7 live-bus validation passes.
func TestV8Classifier_OffMode_NilClassifier(t *testing.T) {
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeOff)
	defer cleanup()

	if got := mux.V8Classifier(); got != nil {
		t.Errorf("V8Classifier() = %v; want nil in ModeOff (zero-allocation default)", got)
	}
}

// TestV8Classifier_ShadowMode_InstantiatedAndObserves pins the
// shadow-mode wiring: configuring ModeShadow instantiates a real
// classifier whose ObservedBytesTotal increments as bytes flow
// through the readLoop. The byte stream itself is NOT altered
// (B3.2 scaffold; real filtering lands in B3.3+).
func TestV8Classifier_ShadowMode_InstantiatedAndObserves(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	c := mux.V8Classifier()
	if c == nil {
		t.Fatal("V8Classifier() = nil in ModeShadow; want non-nil")
	}
	if got := c.Mode(); got != v8classifier.ModeShadow {
		t.Errorf("classifier mode = %v; want ModeShadow", got)
	}

	// Push 5 byte events through the mock; readLoop dispatches
	// each to onReceived AND through the classifier Observe hook.
	const n = 5
	for i := 0; i < n; i++ {
		mock.eventCh <- transport.StreamEvent{
			Kind: transport.StreamEventByte,
			Byte: 0xAA, // SYN — won't disturb mux state (idle marker)
		}
	}

	// Poll until the classifier has observed all 5 events or the
	// deadline expires. The readLoop processes events
	// asynchronously, so we need to wait for the dispatch.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.ObservedBytesTotal() >= n {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.ObservedBytesTotal(); got != n {
		t.Errorf("ObservedBytesTotal() = %d; want %d (readLoop must call Observe for every dispatched StreamEvent)", got, n)
	}
}

// TestV8Classifier_ShadowMode_ProvenanceCountersThroughMux pins the
// B3.3 provenance taxonomy at the mux boundary: bytes with
// different (Byte, WasEscaped) tuples flowing through the
// readLoop must land in the correct provenance buckets.
//
// Three event shapes:
//   - (0xAA, WasEscaped=true)  → escapedPayloadAaTotal
//   - (0xAA, WasEscaped=false) → wireAutoSynTotal
//   - (0x55, WasEscaped=false) → plainByteTotal
func TestV8Classifier_ShadowMode_ProvenanceCountersThroughMux(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	c := mux.V8Classifier()
	if c == nil {
		t.Fatal("V8Classifier() = nil in ModeShadow; want non-nil")
	}

	// Push 3 of each shape (9 StreamEventByte events).
	for i := 0; i < 3; i++ {
		mock.eventCh <- transport.StreamEvent{
			Kind:       transport.StreamEventByte,
			Byte:       0xAA,
			WasEscaped: true,
		}
		mock.eventCh <- transport.StreamEvent{
			Kind:       transport.StreamEventByte,
			Byte:       0xAA,
			WasEscaped: false,
		}
		mock.eventCh <- transport.StreamEvent{
			Kind:       transport.StreamEventByte,
			Byte:       0x55,
			WasEscaped: false,
		}
	}

	// Poll until all 9 events are observed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.ObservedBytesTotal() >= 9 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.ObservedBytesTotal(); got != 9 {
		t.Fatalf("ObservedBytesTotal()=%d; want 9 (readLoop must dispatch every StreamEvent)", got)
	}

	if got := c.EscapedPayloadAaTotal(); got != 3 {
		t.Errorf("EscapedPayloadAaTotal()=%d; want 3 (3 payload-AA events)", got)
	}
	if got := c.WireAutoSynTotal(); got != 3 {
		t.Errorf("WireAutoSynTotal()=%d; want 3 (3 wire-SYN events)", got)
	}
	if got := c.PlainByteTotal(); got != 3 {
		t.Errorf("PlainByteTotal()=%d; want 3 (3 plain-byte events)", got)
	}
	// EscapedPayloadEsc should stay 0 — we didn't send any.
	if got := c.EscapedPayloadEscTotal(); got != 0 {
		t.Errorf("EscapedPayloadEscTotal()=%d; want 0 (no payload-ESC events sent)", got)
	}

	// Cross-counter invariant from the unit test, re-verified at
	// the mux level: sum of provenance buckets == count of
	// StreamEventByte events == 9.
	sum := c.EscapedPayloadAaTotal() + c.EscapedPayloadEscTotal() +
		c.WireAutoSynTotal() + c.PlainByteTotal()
	if sum != 9 {
		t.Errorf("sum of provenance counters = %d; want 9", sum)
	}
}

// TestV8Classifier_ShadowMode_FSMDrivenThroughMux pins the B3.4
// integration: a StreamEventStarted dispatched by readLoop must
// cause the classifier's FSM to enter passive-tracking, and
// subsequent StreamEventByte events must increment the FSM
// decision counter at the mux boundary.
func TestV8Classifier_ShadowMode_FSMDrivenThroughMux(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	c := mux.V8Classifier()
	if c == nil {
		t.Fatal("V8Classifier() = nil in ModeShadow; want non-nil")
	}

	// Push StreamEventStarted (winner = 0x71). FSM should enter
	// passive tracking after readLoop dispatches the event.
	mock.eventCh <- transport.StreamEvent{
		Kind: transport.StreamEventStarted,
		Data: 0x71,
	}
	// Then push 3 plain payload bytes — each should yield a
	// Forward decision.
	for _, b := range []byte{0x10, 0xB5, 0x09} {
		mock.eventCh <- transport.StreamEvent{
			Kind: transport.StreamEventByte,
			Byte: b,
		}
	}

	// Poll until the classifier has observed all 4 events.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.ObservedBytesTotal() >= 4 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.ObservedBytesTotal(); got != 4 {
		t.Fatalf("ObservedBytesTotal()=%d; want 4", got)
	}

	// FSM should be in passive tracking (or its internal
	// MASTER_HEADER sub-phase, collapsed via FSMState).
	if got := c.FSMState(); got != telegram_fsm.StatePassiveTracking {
		t.Errorf("FSMState()=%v; want StatePassiveTracking through mux", got)
	}
	if !c.FSMIsPassive() {
		t.Error("FSMIsPassive()=false; want true through mux")
	}

	// Lifecycle counters: 1 enter passive, 0 resets.
	if got := c.FsmEnterPassiveTotal(); got != 1 {
		t.Errorf("FsmEnterPassiveTotal()=%d; want 1 through mux", got)
	}
	if got := c.FsmResetTotal(); got != 0 {
		t.Errorf("FsmResetTotal()=%d; want 0 (no reset events sent)", got)
	}

	// Decision counters: 3 forwards (one per plain payload byte),
	// 0 drops, 0 faults.
	if got := c.FsmForwardTotal(); got != 3 {
		t.Errorf("FsmForwardTotal()=%d; want 3 (three plain bytes)", got)
	}
	if got := c.FsmDropAaInjectionTotal(); got != 0 {
		t.Errorf("FsmDropAaInjectionTotal()=%d; want 0", got)
	}
	if got := c.FsmProtocolFaultTotal(); got != 0 {
		t.Errorf("FsmProtocolFaultTotal()=%d; want 0", got)
	}
}

// classifiedTestMuxWithSession returns a mux + a real attached
// session whose client-side pipe end can be read by the test to
// observe what bytes actually flow out (or get filtered).
//
// Per Codex round-1 MEDIUM on PR #644: the previous version of
// the mux-boundary tests only asserted classifier counters; a
// buggy readLoop that incremented EnforceDropsAppliedTotal AND
// still dispatched the byte would have passed those tests.
// Reading from a real session pipe is the load-bearing assertion
// that the drop actually skipped onReceived.
func classifiedTestMuxWithSession(t *testing.T, mode v8classifier.Mode) (
	mux *Mux,
	mock *p3MockTransport,
	clientConn net.Conn,
	sid uint64,
	cleanup func(),
) {
	t.Helper()
	mux, mock, _, baseCleanup := newClassifiedTestMux(t, mode)

	clientConn, serverConn := net.Pipe()
	sid = mux.AddSession(serverConn)
	if sid == 0 {
		t.Fatal("AddSession returned 0")
	}

	// Drain whatever ENH framing the writeLoop emits during
	// INIT. minBytes=0: we don't pre-assume any specific frame
	// arrives during INIT — just wait for idle.
	drainSessionUntilIdle(t, clientConn, 0, 50*time.Millisecond, 500*time.Millisecond)

	cleanup = func() {
		_ = clientConn.Close()
		mux.RemoveSession(sid)
		baseCleanup()
	}
	return mux, mock, clientConn, sid, cleanup
}

// drainSessionUntilIdle reads from clientConn enforcing a
// "wait-for-frame then wait-for-idle" contract — both halves
// must succeed within hardDeadline, otherwise t.Fatalf.
//
// minBytes: if > 0, the helper MUST observe at least this many
// bytes before the idle window starts counting. Used for
// post-injection drains where a specific frame is expected to
// arrive (e.g. Started's session-mirror frame). minBytes == 0
// is the "just drain whatever is queued, then wait for idle"
// pattern used for INIT drains where the precondition is
// unknown.
//
// idleWindow: the duration the pipe must be silent BEFORE the
// helper returns. Confirms no late-arriving frame will
// contaminate the subsequent per-test probe.
//
// hardDeadline: total budget for both phases combined. If
// minBytes isn't reached OR an idle window isn't observed
// within this time, the helper FATALS — the test cannot
// proceed under unproven preconditions.
//
// Per Codex round-3 review on PR #644:
//   - Round 2's "wait for idle only" let delayed Started
//     bytes leak past the helper when they arrived AFTER the
//     first 50ms quiet period.
//   - Round 3's minBytes>0 contract requires observing the
//     expected frame BEFORE the idle window starts, closing
//     the race.
//   - Hard-deadline expiry and non-timeout read errors now
//     surface as test failures rather than silent degradation.
//
// Returns total bytes drained.
func drainSessionUntilIdle(t *testing.T, conn net.Conn, minBytes int, idleWindow, hardDeadline time.Duration) int {
	t.Helper()
	buf := make([]byte, 256)
	total := 0
	hard := time.Now().Add(hardDeadline)

	// Phase 1: wait for at least minBytes (if specified). Skipped
	// when minBytes == 0.
	for total < minBytes {
		if time.Now().After(hard) {
			t.Fatalf("drainSessionUntilIdle: hard deadline %v elapsed before observing minBytes=%d (got %d) — expected frame did not arrive", hardDeadline, minBytes, total)
		}
		_ = conn.SetReadDeadline(time.Now().Add(idleWindow))
		n, err := conn.Read(buf)
		total += n
		if err != nil && !isPipeTimeoutErr(err) {
			t.Fatalf("drainSessionUntilIdle phase-1 read err: %v (n=%d total=%d)", err, n, total)
		}
		// Timeout with n==0 is fine; loop continues waiting for
		// minBytes within the hard budget.
	}

	// Phase 2: wait for idleWindow of silence.
	for {
		if time.Now().After(hard) {
			t.Fatalf("drainSessionUntilIdle: hard deadline %v elapsed before observing %v idle window (drained %d bytes total)", hardDeadline, idleWindow, total)
		}
		_ = conn.SetReadDeadline(time.Now().Add(idleWindow))
		n, err := conn.Read(buf)
		total += n
		if n > 0 {
			// More bytes — keep waiting for silence.
			continue
		}
		if err != nil && !isPipeTimeoutErr(err) {
			t.Fatalf("drainSessionUntilIdle phase-2 read err: %v (total=%d)", err, total)
		}
		// Timeout with n==0 — idle window satisfied.
		return total
	}
}

// isPipeTimeoutErr reports whether the read error is the
// "deadline exceeded" kind that we treat as a successful idle
// signal. EOF / closed pipe / other errors are real failures
// and surface via t.Fatalf in drainSessionUntilIdle.
func isPipeTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	var nerr net.Error
	if errors.As(err, &nerr) {
		return nerr.Timeout()
	}
	return false
}

// TestV8Classifier_EnforceMode_AaInjectionDoesNotReachSession is
// the LOAD-BEARING B3.6b behavioral test (Codex round-1 MEDIUM
// fix). It proves that under ModeEnforce, an AA-injection wire
// byte is filtered BEFORE the session sees it — by reading from
// a real session's pipe end and asserting NO bytes arrive in the
// post-injection window.
//
// Counter assertions remain as secondary confirmation but the
// PRIMARY pin is the session-pipe silence.
//
// Test setup uses StreamEventFailed (not Started) to drive the
// FSM into PASSIVE_TRACKING because Failed reliably fans out
// the winner byte to external sessions (per the phantom-filter
// test pattern), giving us a concrete frame to wait for before
// the AA probe. Started would be logged as "stale arbitration
// response" without a pending bid in this minimal test setup
// and produce nothing on the session pipe. From the v8
// classifier's perspective, Started and Failed are semantically
// equivalent — both trigger fsm.EnterPassiveTracking — so
// either is fine for the FSM-state precondition.
func TestV8Classifier_EnforceMode_AaInjectionDoesNotReachSession(t *testing.T) {
	mux, mock, clientConn, _, cleanup := classifiedTestMuxWithSession(t, v8classifier.ModeEnforce)
	defer cleanup()

	c := mux.V8Classifier()
	if c == nil {
		t.Fatal("V8Classifier() = nil in ModeEnforce; want non-nil")
	}

	// Drive the FSM into mid-frame via Failed (winner byte 0x71
	// fans out to all external sessions per the mux's Failed
	// handler, giving us a concrete frame to wait for).
	mock.eventCh <- transport.StreamEvent{
		Kind: transport.StreamEventFailed, Data: 0x71,
	}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if c.FsmEnterPassiveTotal() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.FsmEnterPassiveTotal(); got != 1 {
		t.Fatalf("FsmEnterPassiveTotal()=%d; want 1 (Failed not processed)", got)
	}

	// Drain Failed's session-mirror frame, then wait for idle.
	// minBytes=1: the mirror produces at least 1 byte (exact
	// count varies — sometimes 1, sometimes 2 of the ENH-
	// framed pair depending on read batching). After the first
	// byte arrives, the 50ms idle-window phase guarantees any
	// remaining mirror bytes are also consumed before the AA
	// probe starts. Per Codex round-3 MEDIUM on PR #644: this
	// closes the race where a late-arriving mirror byte could
	// have contaminated the probe under the round-2
	// "idle-only" pattern.
	_ = drainSessionUntilIdle(t, clientConn, 1, 50*time.Millisecond, 1*time.Second)

	// LOAD-BEARING INJECTION: mid-frame wire AUTO-SYN → FSM emits
	// DropAaInjection → ModeEnforce returns drop=true → readLoop
	// `continue`s past onReceived → session pipe sees NOTHING.
	mock.eventCh <- transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0xAA, WasEscaped: false,
	}

	// Wait for the classifier to confirm it processed the byte.
	deadline = time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if c.EnforceDropsAppliedTotal() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.EnforceDropsAppliedTotal(); got != 1 {
		t.Fatalf("EnforceDropsAppliedTotal()=%d; want 1 (classifier should have applied the drop)", got)
	}

	// PRIMARY ASSERTION: zero bytes flow on the session pipe in
	// the post-injection window. A regression that called Observe
	// + incremented the counter AND dispatched the byte would
	// produce >= 2 bytes here (the ENH-encoded ENHResReceived
	// frame).
	bytesAfterInject := 0
	buf := make([]byte, 64)
	probeUntil := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(probeUntil) {
		_ = clientConn.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
		n, _ := clientConn.Read(buf)
		bytesAfterInject += n
	}
	if bytesAfterInject != 0 {
		t.Errorf("Enforce dropped AA-injection but session pipe got %d bytes; want 0 (filter must skip onReceived)", bytesAfterInject)
	}

	// Secondary: FsmDropAaInjectionTotal should also be 1.
	if got := c.FsmDropAaInjectionTotal(); got != 1 {
		t.Errorf("FsmDropAaInjectionTotal()=%d; want 1", got)
	}
}

// TestV8Classifier_ShadowMode_AaInjectionReachesSession is the
// inverse: under ModeShadow, the FSM emits DropAaInjection
// (FsmDropAaInjectionTotal increments) but the byte STILL reaches
// the session pipe. EnforceDropsAppliedTotal stays at 0.
//
// This is the pre-promotion validation mode — operators count
// would-have-dropped vs in-mode dropped before promoting to
// Enforce.
//
// Same test-setup choice as the Enforce sibling: Failed (not
// Started) drives the FSM to passive tracking AND fans out a
// concrete mirror frame for the drain helper to wait for.
func TestV8Classifier_ShadowMode_AaInjectionReachesSession(t *testing.T) {
	mux, mock, clientConn, _, cleanup := classifiedTestMuxWithSession(t, v8classifier.ModeShadow)
	defer cleanup()

	c := mux.V8Classifier()
	if c == nil {
		t.Fatal("V8Classifier() = nil in ModeShadow; want non-nil")
	}

	mock.eventCh <- transport.StreamEvent{
		Kind: transport.StreamEventFailed, Data: 0x71,
	}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if c.FsmEnterPassiveTotal() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Drain Failed's session-mirror frame, then wait for idle.
	_ = drainSessionUntilIdle(t, clientConn, 1, 50*time.Millisecond, 1*time.Second)

	mock.eventCh <- transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0xAA, WasEscaped: false,
	}

	// Wait for classifier to register the FSM verdict.
	deadline = time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if c.FsmDropAaInjectionTotal() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.FsmDropAaInjectionTotal(); got != 1 {
		t.Fatalf("FsmDropAaInjectionTotal()=%d; want 1 (FSM verdict counted in Shadow)", got)
	}

	// PRIMARY ASSERTION: under Shadow, despite the FSM
	// recommending a drop, the byte SHOULD reach the session.
	// We expect >= 2 bytes (ENHResReceived frame).
	bytesAfterInject := 0
	buf := make([]byte, 64)
	probeUntil := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(probeUntil) && bytesAfterInject == 0 {
		_ = clientConn.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
		n, _ := clientConn.Read(buf)
		bytesAfterInject += n
	}
	if bytesAfterInject == 0 {
		t.Error("Shadow mode dropped the AA byte from session — Shadow MUST forward (observe-only contract)")
	}

	// Secondary: EnforceDropsAppliedTotal MUST stay at 0.
	if got := c.EnforceDropsAppliedTotal(); got != 0 {
		t.Errorf("EnforceDropsAppliedTotal()=%d; want 0 (Shadow must never apply drops)", got)
	}
}
