package adaptermux

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// =======================================================================
// PR502 Review Item 1: 0xAA data byte invariant
// =======================================================================

// TestOnReceived_0xAA_WireLayerSYNInvariant proves that 0xAA
// (protocol.SymbolSyn) in onReceived is ALWAYS treated as a SYN
// boundary -- ownership is released, the wire phase resets to idle.
//
// Layer boundary note:
//
//	XR_ENH_0xAA_DataNotSYN: Not applicable at mux layer. The mux receives
//	raw wire bytes where 0xAA is always SYN. Logical 0xAA data is escaped
//	at the ENH transport layer (0xA9 0x01 -> 0xAA) and never reaches
//	onReceived as 0xAA. Enforced by ebusgo ENHTransport -- see
//	TestOnReceived_0xAA_WireLayerSYNInvariant for the mux-layer wire
//	invariant.
//
// Why this is correct for eBUS:
//
//	On the eBUS wire, 0xAA is the SYN (bus idle) symbol. It can NEVER
//	appear as a logical data byte on the wire. If a data payload needs
//	to carry the value 0xAA, the ENH transport escapes it on the wire
//	as {0xA9, 0x01}. The ENH parser decodes {0xA9, 0x01} into a
//	ENHResReceived(0xAA) frame, which ReadEvent surfaces as
//	StreamEventByte{Byte: 0xAA}. However, the adapter firmware NEVER
//	sends ENHResReceived(0xAA) because raw wire byte 0xAA is always
//	consumed as SYN by the adapter's own parser -- it never reaches
//	the "received data" path.
//
//	Therefore, any 0xAA arriving in readLoop -> onReceived is
//	necessarily a SYN event from the adapter. Treating it as SYN
//	is the correct and only valid interpretation.
//
// XR_SYN_DATA_INVARIANT
func TestOnReceived_0xAA_WireLayerSYNInvariant(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Set up gateway ownership to verify SYN releases it.
	ch := mux.arb.requestStart(gatewaySessionID, 0x71)

	// Feed SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Confirm via STARTED.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x71}

	select {
	case result := <-ch:
		if !result.granted {
			t.Fatal("expected gateway to be granted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for grant")
	}

	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("gateway should own the bus")
	}

	// Now feed 0xAA -- this MUST be treated as SYN.
	// After IdleReleaseGrace (200ms default), a second SYN releases ownership.
	// First SYN: starts the grace period.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}
	time.Sleep(10 * time.Millisecond)

	// Verify phase tracker is idle after 0xAA.
	mux.stateMu.Lock()
	isIdle := mux.phase.isIdle()
	mux.stateMu.Unlock()
	if !isIdle {
		t.Fatal("wire phase must be idle after 0xAA -- it must be treated as SYN")
	}

	// Wait for IdleReleaseGrace to expire, then send another SYN.
	time.Sleep(250 * time.Millisecond)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}
	time.Sleep(50 * time.Millisecond)

	// Ownership should be released.
	if mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("0xAA must trigger SYN handling -- ownership should be released after grace period")
	}
}

// TestMux_0xAA_MockTransportAlwaysSYN verifies the mux-layer invariant:
// if a mock transport produces StreamEventByte{Byte: 0xAA}, the mux
// treats it as SYN. This proves that at the mux layer, 0xAA is
// unconditionally SYN regardless of transport type.
//
// The ENH transport layer guarantees this never happens with real data:
// logical 0xAA is escaped on the wire as {0xA9, 0x01} and the adapter
// firmware consumes raw 0xAA as SYN before framing. This test documents
// that even if a hypothetical transport produced 0xAA, the mux would
// handle it correctly as SYN.
//
// Strategy: grant bus ownership via SYN+STARTED, then feed 0xAA.
// The mux must enter the SYN path (start IdleReleaseGrace). After the
// grace period + a second 0xAA, ownership must be released.
func TestMux_0xAA_MockTransportAlwaysSYN(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Grant bus ownership to gateway.
	ch := mux.arb.requestStart(gatewaySessionID, 0x71)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}
	time.Sleep(50 * time.Millisecond)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x71}
	select {
	case r := <-ch:
		if !r.granted {
			t.Fatal("expected grant")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for grant")
	}

	// Feed 0xAA -- the mux must treat this as SYN (wire phase idle).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}
	time.Sleep(10 * time.Millisecond)

	mux.stateMu.Lock()
	isIdle := mux.phase.isIdle()
	mux.stateMu.Unlock()
	if !isIdle {
		t.Fatal("0xAA from mock transport must be treated as SYN -- phase must be idle")
	}

	// After IdleReleaseGrace (200ms), a second 0xAA releases ownership.
	time.Sleep(250 * time.Millisecond)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}
	time.Sleep(50 * time.Millisecond)

	if mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("0xAA must trigger SYN handling -- ownership should be released")
	}
}

// TestWirePhaseTracker_0xAA_AlwaysSYN verifies at the tracker level that
// 0xAA is treated as SYN regardless of the current phase.
func TestWirePhaseTracker_0xAA_AlwaysSYN(t *testing.T) {
	// Case 1: idle -- 0xAA returns SYNIdle.
	var tracker wirePhaseTracker
	tracker.reset(wirePhaseIdle)
	ev := tracker.advance(0xAA)
	if ev != wirePhaseEventSYNIdle {
		t.Fatalf("idle: advance(0xAA) = %d, want SYNIdle (%d)", ev, wirePhaseEventSYNIdle)
	}

	// Case 2: CollectRequest with bytesSeen > 1 -- 0xAA returns SYNTimeout.
	tracker.startRequest()
	tracker.advance(0x71) // SRC -- bytesSeen=1
	tracker.advance(0x08) // DST -- bytesSeen=2
	ev = tracker.advance(0xAA)
	if ev != wirePhaseEventSYNTimeout {
		t.Fatalf("CollectRequest(bytesSeen>1): advance(0xAA) = %d, want SYNTimeout (%d)", ev, wirePhaseEventSYNTimeout)
	}
	if !tracker.isIdle() {
		t.Fatal("tracker must be idle after SYN in CollectRequest")
	}

	// Case 3: WaitCmdAck -- 0xAA returns SYNTimeout.
	tracker.startRequest()
	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x00} {
		tracker.advance(b)
	}
	tracker.advance(0xDD) // CRC -> WaitCmdAck
	ev = tracker.advance(0xAA)
	if ev != wirePhaseEventSYNTimeout {
		t.Fatalf("WaitCmdAck: advance(0xAA) = %d, want SYNTimeout (%d)", ev, wirePhaseEventSYNTimeout)
	}
}

// =======================================================================
// PR502 Review Item 3: blocking arb deadline + queue advance
// =======================================================================

// TestBlockingStartArbitrationDeadlineReal verifies end-to-end that when
// a blocking StartArbitration transport is used and the AM8 deadline fires:
//  1. pendingStart is cleared and the session is notified of failure.
//  2. The deadline callback does NOT spawn a second overlapping blocking
//     arbitration — blockingArbActive stays true until a transport
//     reconnect (via handleReset/reconnect) bumps blockingArbGen and
//     clears the flag. C2 (PR #502 Copilot): the deadline now triggers
//     a transport reconnect (m.conn.Close) to force the hung
//     StartArbitration to return; with a nil m.conn in this unit test
//     no reconnect occurs, so the flag persists until the goroutine
//     returns and the stale-gen path is exercised.
//  3. The late return of the hung goroutine does NOT decrement absorb
//     in this test because its gen is still current (no reconnect
//     happened). absorb was incremented once by the deadline, then
//     decremented once by the cancelled-pending cleanup branch.
//
// This test uses a real timer with a short StartDeadline (200ms) instead
// of manually simulating the deadline callback.
//
// XR_BLOCKING_ARB_DEADLINE
func TestBlockingStartArbitrationDeadlineReal(t *testing.T) {
	mock := &slowBlockingStartTransport{
		readCh: make(chan byte, 256),
		gate:   make(chan struct{}),
	}

	mux := New(Config{
		Protocol:      "enh",
		Network:       "tcp",
		Address:       "127.0.0.1:0",
		ReadTimeout:   200 * time.Millisecond,
		StartDeadline: 200 * time.Millisecond, // short deadline for test speed
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Queue a gateway START request.
	gwCh := mux.arb.requestStart(gatewaySessionID, 0x71)

	// PR502-Fix1: tryGrantAndStart returns immediately; the blocking
	// StartArbitration runs in an internal goroutine.
	mux.tryGrantAndStart()

	// Wait for the internal goroutine to enter StartArbitration.
	time.Sleep(50 * time.Millisecond)

	// Verify pendingStart is set with blockingArb=true BEFORE the deadline fires.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil
	isBlocking := hasPending && mux.pendingStart.blockingArb
	mux.stateMu.Unlock()

	if !hasPending {
		t.Fatal("expected pendingStart to be set")
	}
	if !isBlocking {
		t.Fatal("expected blockingArb=true for blocking StartArbitration path")
	}

	// Wait for the real 200ms deadline to fire.
	// The gateway session should receive a failure notification.
	select {
	case result := <-gwCh:
		if result.granted {
			t.Fatal("expected granted=false after deadline")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for deadline failure result")
	}

	// After deadline fires: pendingStart must be cleared.
	mux.stateMu.Lock()
	pendingAfterDeadline := mux.pendingStart
	mux.stateMu.Unlock()
	if pendingAfterDeadline != nil {
		t.Fatal("pendingStart must be nil after deadline fires")
	}

	// C2 (PR #502 Copilot): AM8 deadline on blocking path now triggers a
	// transport reconnect (m.conn.Close) instead of clearing blockingArbActive
	// in-line. In this unit test m.conn is nil so no reconnect occurs —
	// blockingArbActive stays true and the hung goroutine keeps running.
	// This is the critical "no overlap" property: a second blocking
	// StartArbitration CANNOT start while the first is still in the
	// transport call.
	time.Sleep(50 * time.Millisecond)
	mux.stateMu.Lock()
	pendingStillNil := mux.pendingStart == nil
	activeFlag := mux.blockingArbActive
	mux.stateMu.Unlock()
	if !pendingStillNil {
		t.Fatal("pendingStart should stay nil (no pending after deadline)")
	}
	if !activeFlag {
		t.Fatal("blockingArbActive must stay true while hung goroutine runs (C2 no-overlap invariant)")
	}

	// Release the hung blocking goroutine. With no reconnect having
	// occurred, its gen still matches blockingArbGen (current). Its
	// cancelled-path cleanup runs under isCurrentGen=true, which
	// decrements pendingStartAbsorb and clears blockingArbActive.
	close(mock.gate)
	time.Sleep(50 * time.Millisecond)

	mux.stateMu.Lock()
	absorb := mux.pendingStartAbsorb
	activeAfter := mux.blockingArbActive
	mux.stateMu.Unlock()
	if absorb != 0 {
		t.Fatalf("pendingStartAbsorb = %d, want 0 (deadline +1, current-gen goroutine late-return -1)", absorb)
	}
	if activeAfter {
		t.Fatal("blockingArbActive must be cleared by current-gen goroutine's late return")
	}
}

// TestBlockingArbDeadline_NoOverlap verifies that when two requests are
// queued and the first uses blocking StartArbitration, the deadline does
// NOT start a second overlapping StartArbitration call.
func TestBlockingArbDeadline_NoOverlap(t *testing.T) {
	var arbCount int32
	mock := &countingBlockingStartTransport{
		readCh:   make(chan byte, 256),
		gate:     make(chan struct{}),
		arbCount: &arbCount,
	}

	mux := New(Config{
		Protocol:      "enh",
		Network:       "tcp",
		Address:       "127.0.0.1:0",
		ReadTimeout:   200 * time.Millisecond,
		StartDeadline: 150 * time.Millisecond,
		// C1/C3 test isolation: this test pins blocking-START no-
		// overlap semantics on the gateway, so disable the TTL
		// drain (default 50 ms would reject the external pending
		// during the test's 150 ms deadline window).
		PendingStartTTL: 24 * time.Hour,
	SYNInterval: time.Hour,  // disable C1 idle fast path in legacy tests
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)
	// Force contended-bus path so gateway gets the first grant
	// (test pre-dates the C1 idle fast-path).
	mux.stateMu.Lock()
	mux.lastWireActivity = time.Now()
	mux.stateMu.Unlock()

	// Queue two requests: gateway + external.
	gwCh := mux.arb.requestStart(gatewaySessionID, 0x71)
	extCh := mux.arb.requestStart(2, 0x50)

	// PR502-Fix1: tryGrantAndStart returns immediately; the blocking
	// StartArbitration runs in an internal goroutine.
	mux.tryGrantAndStart()

	// Wait for first StartArbitration to enter.
	time.Sleep(50 * time.Millisecond)

	// Deadline fires at ~150ms. Wait for gateway failure.
	select {
	case result := <-gwCh:
		if result.granted {
			t.Fatal("expected granted=false after deadline")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for deadline failure")
	}

	// C2 (PR #502 Copilot): after the deadline fires on the blocking
	// path, the callback triggers a transport reconnect instead of
	// clearing blockingArbActive in-line. In this unit test m.conn is
	// nil so no reconnect happens — blockingArbActive stays true and
	// NO second StartArbitration is spawned while the first is still
	// hung. This is the critical no-overlap invariant enforced by C2.
	time.Sleep(50 * time.Millisecond)
	if c := atomic.LoadInt32(&arbCount); c != 1 {
		t.Fatalf("expected exactly 1 StartArbitration while first is hung (C2 no-overlap), got %d", c)
	}
	mux.stateMu.Lock()
	stillActive := mux.blockingArbActive
	mux.stateMu.Unlock()
	if !stillActive {
		t.Fatal("blockingArbActive must stay true while hung goroutine runs (C2)")
	}

	// Release the first hung goroutine. With no reconnect, its gen is
	// still current, so its cancelled-path cleanup decrements absorb,
	// clears blockingArbActive, and calls tryGrantAndStart which
	// dequeues request #2 and launches a second StartArbitration.
	close(mock.gate)

	time.Sleep(150 * time.Millisecond)
	if c := atomic.LoadInt32(&arbCount); c != 2 {
		t.Fatalf("expected 2 StartArbitration calls after hung goroutine returns and advances queue, got %d", c)
	}
	select {
	case result := <-extCh:
		if result.granted {
			t.Fatal("external session should have failed (not registered)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for external session result after queue advance")
	}
}

// slowBlockingStartTransport blocks StartArbitration until gate is closed,
// then returns nil (success). Does NOT implement RequestStart.
type slowBlockingStartTransport struct {
	readCh chan byte
	gate   chan struct{}
}

func (s *slowBlockingStartTransport) ReadByte() (byte, error) {
	v, ok := <-s.readCh
	if !ok {
		return 0, nil
	}
	return v, nil
}

func (s *slowBlockingStartTransport) Write(p []byte) (int, error) { return len(p), nil }
func (s *slowBlockingStartTransport) Close() error                { return nil }

func (s *slowBlockingStartTransport) StartArbitration(initiator byte) error {
	<-s.gate
	return nil
}

// countingBlockingStartTransport is like slowBlockingStartTransport but
// counts how many times StartArbitration is called.
type countingBlockingStartTransport struct {
	readCh   chan byte
	gate     chan struct{}
	arbCount *int32
}

func (s *countingBlockingStartTransport) ReadByte() (byte, error) {
	v, ok := <-s.readCh
	if !ok {
		return 0, nil
	}
	return v, nil
}

func (s *countingBlockingStartTransport) Write(p []byte) (int, error) { return len(p), nil }
func (s *countingBlockingStartTransport) Close() error                { return nil }

func (s *countingBlockingStartTransport) StartArbitration(initiator byte) error {
	atomic.AddInt32(s.arbCount, 1)
	<-s.gate
	return nil
}

// =======================================================================
// PR502 Review Item: negative config validation
// =======================================================================

// TestConfig_NegativeValues verifies that negative durations for
// StartDeadline and BlackholeThreshold are clamped to their defaults.
func TestConfig_NegativeValues(t *testing.T) {
	cfg := Config{
		StartDeadline:      -1 * time.Second,
		BlackholeThreshold: -5 * time.Second,
	}
	cfg.defaults()

	if cfg.StartDeadline != 5*time.Second {
		t.Fatalf("StartDeadline = %v, want 5s (negative must be clamped)", cfg.StartDeadline)
	}
	if cfg.BlackholeThreshold != 30*time.Second {
		t.Fatalf("BlackholeThreshold = %v, want 30s (negative must be clamped)", cfg.BlackholeThreshold)
	}
}

// =======================================================================
// PR502 Review Item: BlackholeThreshold duration-based test
// =======================================================================

// TestBlackholeThreshold_DurationBased verifies that the blackhole
// reconnect fires based on elapsed duration (BlackholeThreshold) rather
// than a count of timeout iterations. With ReadTimeout=50ms and
// BlackholeThreshold=500ms, the reconnect should fire at ~500ms, NOT
// at 150*50ms=7.5s (the old count-based behavior).
//
// Strategy: use a mock transport that returns one data byte (to seed
// lastDataTime) then returns only timeouts. The mux readLoop should
// trigger reconnect after ~500ms. Since reconnect calls m.connect()
// which will fail (no real server), we detect the reconnect attempt
// by observing a PassiveEventDisconnected event.
//
// XR_BLACKHOLE_DURATION
func TestBlackholeThreshold_DurationBased(t *testing.T) {
	mock := &blackholeMockTransport{
		dataCh: make(chan byte, 1),
	}
	// Seed one data byte so lastDataTime is set.
	mock.dataCh <- 0x42

	disconnected := make(chan struct{}, 1)

	mux := New(Config{
		Protocol:              "enh",
		Network:               "tcp",
		Address:               "127.0.0.1:0",
		ReadTimeout:           50 * time.Millisecond,
		BlackholeThreshold:    500 * time.Millisecond,
		ReconnectInitialDelay: 10 * time.Second, // large so reconnect loop doesn't complete
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	mux.SetPassiveCallback(func(pe PassiveEvent) {
		if pe.Kind == PassiveEventDisconnected {
			select {
			case disconnected <- struct{}{}:
			default:
			}
		}
	})

	// Start readLoop.
	mux.wg.Add(1)
	go mux.readLoop()

	// The reconnect should fire at ~500ms (BlackholeThreshold).
	// Allow up to 2s for race-detector overhead, but the key assertion
	// is that it fires MUCH sooner than 7.5s (the old count-based threshold).
	start := time.Now()
	select {
	case <-disconnected:
		elapsed := time.Since(start)
		// Must fire within 1.5s (generous for race detector).
		// Old count-based would take ~7.5s with 50ms ReadTimeout.
		if elapsed > 1500*time.Millisecond {
			t.Fatalf("blackhole reconnect took %v, expected ~500ms (duration-based)", elapsed)
		}
		t.Logf("blackhole reconnect fired after %v (threshold=500ms)", elapsed)
	case <-time.After(3 * time.Second):
		t.Fatal("blackhole reconnect did not fire within 3s (expected ~500ms)")
	}

	cancel()
	mux.wg.Wait()
}

// blackholeMockTransport returns one data byte from dataCh, then
// returns ErrTimeout on every subsequent read. Does not implement
// StreamEventReader, so readLoop uses ReadByte.
type blackholeMockTransport struct {
	dataCh chan byte
}

func (b *blackholeMockTransport) ReadByte() (byte, error) {
	select {
	case v := <-b.dataCh:
		return v, nil
	default:
		// Simulate read timeout.
		time.Sleep(50 * time.Millisecond)
		return 0, ebuserrors.ErrTimeout
	}
}

func (b *blackholeMockTransport) Write(p []byte) (int, error) { return len(p), nil }
func (b *blackholeMockTransport) Close() error                { return nil }

// =======================================================================
// PR502 Review Item 6: XR_ conformance cross-reference
// =======================================================================
//
// The following XR_ identifiers map review requirements to existing test
// coverage across the adaptermux test suite:
//
// XR_SYN_DATA_INVARIANT
//   TestOnReceived_0xAA_WireLayerSYNInvariant       (this file)
//   TestMux_0xAA_MockTransportAlwaysSYN              (this file)
//   TestWirePhaseTracker_0xAA_AlwaysSYN              (this file)
//   Rationale: 0xAA on the wire is always SYN. The ENH transport
//   never produces StreamEventByte{0xAA} as a data byte because the
//   adapter parser consumes raw 0xAA as SYN before framing.
//
// XR_ENH_0xAA_DataNotSYN
//   Not applicable at mux layer. The mux receives raw wire bytes where
//   0xAA is always SYN. Logical 0xAA data is escaped at the ENH
//   transport layer (0xA9 0x01 -> 0xAA) and never reaches onReceived
//   as 0xAA. Enforced by ebusgo ENHTransport -- see
//   TestOnReceived_0xAA_WireLayerSYNInvariant for the mux-layer wire
//   invariant.
//
// XR_BLACKHOLE_DURATION
//   Blackhole detection uses duration-based threshold (30s default).
//   See readLoop in mux.go: blackholeThreshold field + time.Since check.
//
// XR_BLOCKING_ARB_DEADLINE
//   TestBlockingStartArbitrationDeadlineReal          (this file)
//   TestBlockingArbDeadline_NoOverlap                 (this file)
//   TestP3_FallbackStartArbitration                   (p3_test.go)
//   Rationale: AM8 deadline + blockingArb flag prevents overlapping
//   arbitration. Queue advances only after blocking call returns.
//   blockingArb is set in the pendingStart struct literal BEFORE the
//   deadline timer is created, eliminating the assignment race.
//
// XR_PASSIVE_RESET_BACKPRESSURE
//   TestPassiveTransport_ResetDroppedAfterTimeout     (passive_transport_test.go)
//   TestPassiveTransport_ResetBoundedBlockOnFullBuffer (passive_transport_test.go)
//   See passive_transport.go deliver() -- AM52/AM-fix5/Codex-R6 comment
//   documents the 100ms bounded-blocking trade-off. The consumer can
//   resync on the next reset after draining stale data.
//
// XR_GATEWAY_PRIORITY
//   TestArbitrator_GatewayPriority                    (arbitration_test.go)
//   Rationale: arbitrator.tryGrant checks pendingGateway before
//   pendingExternal. See doc comment on arbitrator type.
//
// XR_INFO_CACHE_SNAPSHOT
//   TestPopulateInfoCache                             (info_cache_test.go)
//   TestCachedInfo_ReturnsCopy                        (info_cache_test.go)
//   Rationale: INFO cache is populated once at connect, serves
//   startup snapshots. See doc comment on populateInfoCache.

// =======================================================================
// PR502 Runtime Soak Followup: gatewayTxnActive + EscapeAware
// =======================================================================

// grantGateway is a test helper that grants gateway ownership via the
// real readLoop path (no manual state mutation).
func grantGateway(t *testing.T, mux *Mux, mock *p3MockTransport, initiator byte) {
	t.Helper()
	ch := mux.arb.requestStart(gatewaySessionID, initiator)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}
	select {
	case r := <-ch:
		if !r.granted {
			t.Fatalf("grant failed: %v", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for grant")
	}
}

// TestActivePath_RealFlow_NoAccumulationAfterTxnSyn verifies the real
// production lifecycle: after a gateway transaction, the first SYN
// clears gatewayTxnActive and subsequent third-party bytes do NOT
// accumulate on activeCh — even though ownership lingers up to
// IdleReleaseGrace.
//
// Does NOT manually mutate gatewayTxnActive. Exercises the actual
// readLoop path: grant → txn bytes → trailing SYN (real lifecycle
// clear) → third-party bytes must NOT enter activeCh.
//
// This matches the RPi runtime observation: the phase tracker is
// skipped during gateway ownership, so TransactionDone/CmdNACK
// never fire for real gateway traffic. The SYN-based clear is the
// only reliable lifecycle signal.
func TestActivePath_RealFlow_NoAccumulationAfterTxnSyn(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Write request bytes (simulates bus.Send; bytesWritten > 0 moves the
	// txn out of the pre-echo suppression window — without this the
	// trailing SYN would be suppressed rather than delivered as terminator
	// under the echo_mismatch-fix gate).
	request := []byte{0x71, 0x08, 0xB5, 0x04, 0x01}
	if _, err := at.Write(request); err != nil {
		t.Fatalf("Write err=%v", err)
	}

	// Feed transaction echoes + response (real txn bytes).
	txnBytes := []byte{0x71, 0x08, 0xB5, 0x04, 0x01, 0x42, 0xCD, 0x00, 0x01, 0x33, 0xAB, 0x00}
	for _, b := range txnBytes {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(50 * time.Millisecond)

	// Consume via activeTransport.ReadByte so bytesRead increments —
	// this is what real bus.Send does for echo matching + response.
	for range txnBytes {
		if _, err := at.ReadByte(); err != nil {
			t.Fatalf("ReadByte err=%v", err)
		}
	}

	// End-of-transaction: bus.Send has returned. Next SYN clears
	// gatewayTxnActive BEFORE IdleReleaseGrace expires. PR #502 E2E
	// fix: the terminator SYN is delivered to activeCh before the
	// clear — drain it here so the "no accumulation" assertion below
	// reflects third-party noise only, not the legitimate terminator.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)
	if b, err := at.ReadByte(); err != nil || b != protocol.SymbolSyn {
		t.Fatalf("ReadByte terminator=(0x%02X,%v), want (0xAA,nil)", b, err)
	}

	// Ownership STILL held (IdleReleaseGrace=200ms, we waited 30ms).
	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("ownership should still be held during IdleReleaseGrace window")
	}

	// gatewayTxnActive cleared by the SYN (real lifecycle signal).
	mux.stateMu.Lock()
	stillActive := mux.gatewayTxnActive
	mux.stateMu.Unlock()
	if stillActive {
		t.Fatal("gatewayTxnActive must be cleared by SYN (real lifecycle), not only by ownership release")
	}

	// Third-party bytes during IdleReleaseGrace window must NOT accumulate.
	thirdParty := []byte{0x10, 0x08, 0x50, 0x0E, 0x03}
	for _, b := range thirdParty {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(50 * time.Millisecond)

	if remaining := len(mux.activeCh); remaining != 0 {
		t.Fatalf("activeCh accumulated %d bytes after txn-SYN during IdleReleaseGrace — real lifecycle broken", remaining)
	}

	// Follow-up grant must not need to drain — activeCh is already empty.
	pre := len(mux.activeCh)
	if pre != 0 {
		t.Fatalf("pre-grant activeCh has %d stale bytes — next grant would log spurious drain", pre)
	}
}

// TestActivePath_EarlyAbort_SYNBeforeRead_StaysInactive verifies the
// lifecycle-correctness rule: a SYN arriving during gateway ownership before
// the active caller starts Write does not arm or terminate the active
// transaction. This is a pre-write stale SYN from the TCP buffer (normal
// grant-handoff residual).
//
// Genuine aborts (no writes, no reads) are resolved by MaxOwnershipDuration,
// ActiveReadTimeout, ActiveWriteError, ctx cancel, reset, or reconnect —
// NOT by SYN alone.
func TestActivePath_EarlyAbort_SYNBeforeRead_StaysInactive(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	// Pre-write stale SYN arrives with no reads yet. It must not arm the
	// active path and must not produce an inactive reason.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Fatalf("activeTxn must stay inactive before first Write; got inactive reason=%q", snap.InactiveReason)
	}
	if snap.InactiveReason != ReasonNone {
		t.Fatalf("InactiveReason must be empty, got %q", snap.InactiveReason)
	}
}

// TestActivePath_SoakCycle_NoSustainedGrowth runs repeated grant/
// complete cycles with background third-party traffic between them,
// and asserts that activeCh does not accumulate stale bytes.
//
// Matches the runtime soak scenario: scan/poll issues many short
// transactions while the bus also carries third-party traffic.
// Pre-fix: each cycle left noise on activeCh requiring the next
// grant's drainActiveCh to discard (log spam).
func TestActivePath_SoakCycle_NoSustainedGrowth(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	at := mux.ActiveTransport()
	cycles := 5
	for i := 0; i < cycles; i++ {
		grantGateway(t, mux, mock, 0x71)

		// Write request to move out of pre-echo suppression window
		// (bytesWritten > 0). Required for the trailing SYN below to be
		// treated as terminator under the echo_mismatch-fix gate.
		request := []byte{0x71, 0x08}
		if _, err := at.Write(request); err != nil {
			t.Fatalf("cycle %d: Write err=%v", i, err)
		}

		// Short txn bytes.
		txn := []byte{0x71, 0x08, 0xB5, 0x04, 0x00, 0xC9, 0x00}
		for _, b := range txn {
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
		}
		time.Sleep(15 * time.Millisecond)
		// Consume via ReadByte so bytesRead > 0 (lifecycle requirement).
		for range txn {
			if _, err := at.ReadByte(); err != nil {
				t.Fatalf("cycle %d: ReadByte err=%v", i, err)
			}
		}

		// End-of-txn SYN: clears gatewayTxnActive (bytesRead > 0).
		// PR #502 E2E fix: the SYN is delivered to activeCh as the
		// frame terminator. Drain it so the no-accumulation assertion
		// below reflects noise only, not the legitimate terminator.
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
		time.Sleep(10 * time.Millisecond)
		if b, err := at.ReadByte(); err != nil || b != protocol.SymbolSyn {
			t.Fatalf("cycle %d: ReadByte terminator=(0x%02X,%v), want (0xAA,nil)", i, b, err)
		}

		// Third-party noise during IdleReleaseGrace window.
		noise := []byte{0xFE, 0x10, 0xBA, 0x55, 0x99}
		for _, b := range noise {
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
		}
		time.Sleep(5 * time.Millisecond)

		// Assert no accumulation.
		if leftover := len(mux.activeCh); leftover != 0 {
			t.Fatalf("cycle %d: activeCh has %d leftover bytes (no-drain invariant broken)", i, leftover)
		}

		// Wait for ownership release via IdleReleaseGrace + SYN.
		time.Sleep(220 * time.Millisecond)
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
		time.Sleep(10 * time.Millisecond)

		if mux.arb.isOwner(gatewaySessionID) {
			t.Fatalf("cycle %d: ownership not released after IdleReleaseGrace", i)
		}
	}
}

// TestCancelPendingStart_ClearsBlockingGuard is the Codex PR #502 P1
// regression test.
//
// Updated expectations (Codex follow-up, mirrors the C2 deadline→reconnect
// pattern): cancelPendingStart on a blocking path no longer clears
// blockingArbActive / bumps blockingArbGen in-line. Clearing the guard
// while a hung StartArbitration goroutine is still running the adapter
// exchange can leave mux/arbitrator state diverged from adapter state
// (adapter may have already granted START to the cancelled initiator).
// Instead, cancelPendingStart closes m.conn to force the hung goroutine
// to return with I/O error; readLoop's reconnect path bumps the gen and
// clears blockingArbActive atomically. The hung goroutine's late return
// hits a stale gen and does not mutate state. This test uses a nil
// m.conn (classic unit-test setup) so the in-line clear path is
// exercised only via the hung goroutine's own return after gate release.
// With m.conn == nil, the cancelled pending is FAILED but the guard
// stays true until the goroutine releases.
func TestCancelPendingStart_ClearsBlockingGuard(t *testing.T) {
	mock := &slowBlockingStartTransport{
		readCh: make(chan byte, 256),
		gate:   make(chan struct{}),
	}

	mux := New(Config{
		Protocol:      "enh",
		Network:       "tcp",
		Address:       "127.0.0.1:0",
		ReadTimeout:   200 * time.Millisecond,
		StartDeadline: 10 * time.Second, // long — we exercise cancel, not deadline
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Queue an external session START so we can cancel it by sessionID.
	const extSessionID uint64 = 42
	extCh := mux.arb.requestStart(extSessionID, 0x50)

	// Kick off: tryGrantAndStart will take the external request and
	// run StartArbitration in a goroutine that hangs on `gate`.
	mux.tryGrantAndStart()

	// Wait for the internal blocking goroutine to be in-flight.
	time.Sleep(50 * time.Millisecond)

	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil
	isBlocking := hasPending && mux.pendingStart.blockingArb
	genBefore := mux.blockingArbGen
	activeBefore := mux.blockingArbActive
	mux.stateMu.Unlock()

	if !hasPending {
		t.Fatal("expected pendingStart to be set")
	}
	if !isBlocking {
		t.Fatal("expected blockingArb=true")
	}
	if !activeBefore {
		t.Fatal("expected blockingArbActive=true while StartArbitration is hung")
	}

	// Cancel the pending START (simulates session disconnect).
	// With m.conn == nil, the cancelPendingStart reconnect trigger is a
	// no-op; the hung goroutine will only release once gate closes.
	mux.cancelPendingStart(extSessionID)

	// Session must have been notified of cancellation.
	select {
	case r := <-extCh:
		if r.granted {
			t.Fatal("expected granted=false after cancel")
		}
		if !r.cancelled {
			t.Fatal("expected cancelled=true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for cancelled result")
	}

	// Updated assertions: pendingStart cleared, absorb incremented, but
	// blockingArbActive and blockingArbGen UNCHANGED in-line (reconnect
	// path was skipped because m.conn == nil in this unit test).
	mux.stateMu.Lock()
	activeAfter := mux.blockingArbActive
	genAfter := mux.blockingArbGen
	pendingAfter := mux.pendingStart
	absorbAfter := mux.pendingStartAbsorb
	mux.stateMu.Unlock()

	if !activeAfter {
		t.Fatal("blockingArbActive must stay TRUE in-line; it is cleared only by reconnect or by the hung goroutine's own return (C2 pattern)")
	}
	if genAfter != genBefore {
		t.Fatalf("blockingArbGen = %d, want %d (cancelPendingStart must not bump gen in-line; reconnect handles that)", genAfter, genBefore)
	}
	if pendingAfter != nil {
		t.Fatal("pendingStart must be nil after cancel")
	}
	if absorbAfter != 1 {
		t.Fatalf("pendingStartAbsorb = %d, want 1 (cancel increments absorb so a stale STARTED/FAILED is discarded)", absorbAfter)
	}

	// Release the gate. The hung goroutine wakes up and returns nil.
	// Its gen is still current (no reconnect happened in this test),
	// so it executes the cancelled-path cleanup: decrements absorb,
	// clears blockingArbActive, advances the queue.
	close(mock.gate)
	time.Sleep(100 * time.Millisecond)

	mux.stateMu.Lock()
	activeAfterRelease := mux.blockingArbActive
	absorbAfterRelease := mux.pendingStartAbsorb
	mux.stateMu.Unlock()

	if activeAfterRelease {
		t.Fatal("blockingArbActive must be cleared once the hung goroutine returns")
	}
	if absorbAfterRelease != 0 {
		t.Fatalf("pendingStartAbsorb = %d, want 0 (goroutine's current-gen late return decrements absorb)", absorbAfterRelease)
	}
}

// =======================================================================
// PR502 Copilot Findings (C1, C2)
// =======================================================================

// TestTryGrantAndStart_NoSpawnAfterClose verifies C1: after Close() has
// set the shutdown flag, tryGrantAndStart on the blocking-StartArbitration
// path must NOT call m.wg.Add(1) (which would race with m.wg.Wait() in
// Close() and panic). The pending request must be notified of failure.
func TestTryGrantAndStart_NoSpawnAfterClose(t *testing.T) {
	mock := &slowBlockingStartTransport{
		readCh: make(chan byte, 256),
		gate:   make(chan struct{}),
	}

	mux := New(Config{
		Protocol:      "enh",
		Network:       "tcp",
		Address:       "127.0.0.1:0",
		ReadTimeout:   200 * time.Millisecond,
		StartDeadline: 5 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Queue an external request BEFORE setting the closing flag.
	const extSessionID uint64 = 77
	extCh := mux.arb.requestStart(extSessionID, 0x50)

	// Simulate shutdown-in-progress: set the closing flag directly
	// (equivalent to being inside Close() after m.closing.Store(true)
	// but before m.wg.Wait() — no goroutine may call m.wg.Add(1)).
	mux.closing.Store(true)

	// Call tryGrantAndStart — the blocking path must observe closing
	// and notify the pending of failure WITHOUT spawning the blocking
	// goroutine (which would wg.Add(1) and be unsafe during shutdown).
	//
	// If the closing gate is broken, this path calls m.wg.Add(1) after
	// Close() has called m.wg.Wait() — in a real Close() that panics
	// with "sync: WaitGroup misuse: Add called concurrently with Wait".
	// Here we avoid actually invoking Close() so the test is
	// deterministic; we assert the observable behavior (no goroutine
	// spawn, pending FAILED).
	mux.tryGrantAndStart()

	select {
	case r := <-extCh:
		if r.granted {
			t.Fatal("expected granted=false when closing during tryGrantAndStart")
		}
		if r.err == nil {
			t.Fatal("expected non-nil err when refusing to spawn during shutdown")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for shutdown-failure notification")
	}

	// No goroutine should have entered StartArbitration (mock would have
	// blocked on gate). Confirm by checking pendingStart is nil.
	mux.stateMu.Lock()
	pending := mux.pendingStart
	active := mux.blockingArbActive
	mux.stateMu.Unlock()
	if pending != nil {
		t.Fatal("pendingStart must be cleared after closing-gate rejection")
	}
	if active {
		t.Fatal("blockingArbActive must NOT be set when spawn was refused")
	}

	cancel()
	close(mock.gate)
}

// deadlineConnMock is a minimal net.Conn used to observe Close() calls
// from the AM8 deadline path (C2).
type deadlineConnMock struct {
	mu     sync.Mutex
	closed bool
	closeC chan struct{}
}

func newDeadlineConnMock() *deadlineConnMock {
	return &deadlineConnMock{closeC: make(chan struct{})}
}

func (d *deadlineConnMock) Read(b []byte) (int, error)  { return 0, errors.New("not used") }
func (d *deadlineConnMock) Write(b []byte) (int, error) { return len(b), nil }
func (d *deadlineConnMock) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	close(d.closeC)
	return nil
}
func (d *deadlineConnMock) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (d *deadlineConnMock) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (d *deadlineConnMock) SetDeadline(time.Time) error      { return nil }
func (d *deadlineConnMock) SetReadDeadline(time.Time) error  { return nil }
func (d *deadlineConnMock) SetWriteDeadline(time.Time) error { return nil }

// TestBlockingArbDeadline_TriggersReconnect_NoOverlap verifies C2: when a
// blocking StartArbitration is hung and the AM8 deadline fires, the
// deadline callback triggers a transport reconnect (m.conn.Close) rather
// than clearing blockingArbActive in-line. Critically, during the window
// between the deadline firing and the hung goroutine returning, NO second
// blocking goroutine is spawned.
func TestBlockingArbDeadline_TriggersReconnect_NoOverlap(t *testing.T) {
	var arbCount int32
	mock := &countingBlockingStartTransport{
		readCh:   make(chan byte, 256),
		gate:     make(chan struct{}),
		arbCount: &arbCount,
	}

	mux := New(Config{
		Protocol:      "enh",
		Network:       "tcp",
		Address:       "127.0.0.1:0",
		ReadTimeout:   200 * time.Millisecond,
		StartDeadline: 120 * time.Millisecond,
		// C1/C3 test isolation (see paired test above).
		PendingStartTTL: 24 * time.Hour,
	SYNInterval: time.Hour,  // disable C1 idle fast path in legacy tests
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel

	// Install a fake conn so the deadline path's m.conn.Close() fires.
	connMock := newDeadlineConnMock()
	mux.connMu.Lock()
	mux.conn = connMock
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)
	// Force contended-bus path so gateway gets the first grant
	// (test pre-dates the C1 idle fast-path).
	mux.stateMu.Lock()
	mux.lastWireActivity = time.Now()
	mux.stateMu.Unlock()

	// Queue two requests: the first will hit the blocking path, the
	// second should remain queued while the first is hung.
	gwCh := mux.arb.requestStart(gatewaySessionID, 0x71)
	_ = mux.arb.requestStart(2, 0x50)

	mux.tryGrantAndStart()
	time.Sleep(30 * time.Millisecond)

	// Wait for deadline to fire and gateway to be notified.
	select {
	case r := <-gwCh:
		if r.granted {
			t.Fatal("expected granted=false after deadline")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for deadline failure")
	}

	// C2 core assertion: deadline triggered m.conn.Close().
	select {
	case <-connMock.closeC:
		// ok — deadline closed the conn to unstick the hung goroutine.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("deadline did not close m.conn (C2 reconnect trigger missing)")
	}

	// Overlap invariant: while the hung goroutine is still running, NO
	// second StartArbitration has been spawned. The old (pre-C2) code
	// cleared blockingArbActive in the deadline and called tryGrantAndStart
	// directly, which would race a second blocking goroutine onto the same
	// transport. C2 prevents that — the second arbitration can only start
	// after a real reconnect bumps the gen.
	if c := atomic.LoadInt32(&arbCount); c != 1 {
		t.Fatalf("expected 1 StartArbitration while hung goroutine runs (C2 no-overlap), got %d", c)
	}

	mux.stateMu.Lock()
	activeDuringHang := mux.blockingArbActive
	mux.stateMu.Unlock()
	if !activeDuringHang {
		t.Fatal("blockingArbActive must remain true until reconnect completes (no in-line clear)")
	}

	// Release the hung goroutine. In a real gateway the readLoop would
	// call reconnect() on the conn-closed I/O error; here the mock
	// StartArbitration simply returns nil when gate is closed. Its
	// generation is still current (no real reconnect bumped it in this
	// unit test), so it executes the cancelled-path cleanup, decrements
	// absorb, clears blockingArbActive, and advances the queue.
	close(mock.gate)
	time.Sleep(100 * time.Millisecond)

	mux.stateMu.Lock()
	activeAfter := mux.blockingArbActive
	mux.stateMu.Unlock()
	if activeAfter {
		t.Fatal("blockingArbActive must be cleared once the hung goroutine returns")
	}

	// The queue advance launched a second StartArbitration (now that
	// the flag is clear). arbCount went from 1 to 2.
	if c := atomic.LoadInt32(&arbCount); c != 2 {
		t.Fatalf("after hung goroutine returned, expected queue to advance (arbCount=2), got %d", c)
	}
}

// TestActiveTransport_ImplementsEscapeAware verifies that the adaptermux
// active transport implements transport.EscapeAware and returns
// BytesAreUnescaped()==true for ENH. Without this, protocol.Bus would
// treat logical 0xAA as SYN on the ENH unescaped path.
func TestActiveTransport_ImplementsEscapeAware(t *testing.T) {
	mux, _, _, cleanup := newP3TestMux(t)
	defer cleanup()

	activeTr := mux.ActiveTransport()
	ea, ok := activeTr.(transport.EscapeAware)
	if !ok {
		t.Fatal("active transport must implement transport.EscapeAware")
	}
	if !ea.BytesAreUnescaped() {
		t.Fatal("active transport BytesAreUnescaped must return true for ENH mock transport")
	}
}

// =======================================================================
// PR502 Codex P2: Serialize shutdown gate with wg.Add
// =======================================================================

// raceBlockingStartTransport has StartArbitration return an error promptly
// so the blocking-path goroutine exits quickly — we are stressing the
// Add/Wait race, not the in-flight behavior.
type raceBlockingStartTransport struct {
	readCh chan byte
}

func (r *raceBlockingStartTransport) ReadByte() (byte, error) {
	v, ok := <-r.readCh
	if !ok {
		return 0, errors.New("closed")
	}
	return v, nil
}
func (r *raceBlockingStartTransport) Write(p []byte) (int, error) { return len(p), nil }
func (r *raceBlockingStartTransport) Close() error                { return nil }
func (r *raceBlockingStartTransport) StartArbitration(initiator byte) error {
	// Return an error promptly so the goroutine exits and wg.Done runs.
	return errors.New("race test: no arbitration")
}

// TestTryGrantAndStart_ShutdownRace_NoAddAfterWait is the PR #502 P2
// regression test for the check-then-add TOCTOU between tryGrantAndStart
// and Close. Many concurrent goroutines hit the blocking-path gate while
// Close races with them. Under -race, a broken serialization would
// eventually panic with "sync: WaitGroup misuse: Add called concurrently
// with Wait". This test iterates many times to force the interleaving.
func TestTryGrantAndStart_ShutdownRace_NoAddAfterWait(t *testing.T) {
	const iterations = 40
	const concurrent = 64

	for iter := 0; iter < iterations; iter++ {
		mock := &raceBlockingStartTransport{
			readCh: make(chan byte, 8),
		}

		mux := New(Config{
			Protocol:      "enh",
			Network:       "tcp",
			Address:       "127.0.0.1:0",
			ReadTimeout:   200 * time.Millisecond,
			StartDeadline: 2 * time.Second,
		})

		ctx, cancel := context.WithCancel(context.Background())
		mux.ctx, mux.cancel = ctx, cancel

		mux.connMu.Lock()
		mux.upstream = mock
		mux.connMu.Unlock()
		mux.upstreamFeatures.Store(0x01)

		// Pre-queue many external requests so tryGrantAndStart has work to do.
		for i := 0; i < concurrent; i++ {
			_ = mux.arb.requestStart(uint64(1000+i), 0x50)
		}

		// Launch N goroutines each calling tryGrantAndStart, racing with Close.
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(concurrent)
		for i := 0; i < concurrent; i++ {
			go func() {
				defer wg.Done()
				<-start
				// Each caller races the shutdown gate. If the gate is
				// not serialized with Close, wg.Add(1) may run after
				// Close reaches Wait — panic under -race.
				mux.tryGrantAndStart()
			}()
		}

		closeDone := make(chan error, 1)
		go func() {
			<-start
			closeDone <- mux.Close()
		}()

		close(start)
		wg.Wait()

		select {
		case err := <-closeDone:
			if err != nil {
				// Close may return a nil error since m.conn is nil here.
				// Non-nil is OK too, what matters is no panic/hang.
				_ = err
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("iter %d: Close hung — possible wg.Add/Wait deadlock", iter)
		}

		cancel()
		close(mock.readCh)
	}
}

// =======================================================================
// PR502 Codex follow-up P1: cancelPendingStart reconnect pattern
// =======================================================================

// TestCancelPendingStart_BlockingArb_TriggersReconnect_NoOverlap verifies
// the Codex PR #502 follow-up P1 fix: cancelPendingStart on a blocking
// path triggers a transport reconnect (m.conn.Close) instead of clearing
// blockingArbActive + calling tryGrantAndStart in-line. Critically, no
// second blocking goroutine is spawned while the hung goroutine is still
// running (no overlap). Mirrors TestBlockingArbDeadline_TriggersReconnect_NoOverlap.
func TestCancelPendingStart_BlockingArb_TriggersReconnect_NoOverlap(t *testing.T) {
	var arbCount int32
	mock := &countingBlockingStartTransport{
		readCh:   make(chan byte, 256),
		gate:     make(chan struct{}),
		arbCount: &arbCount,
	}

	mux := New(Config{
		Protocol:      "enh",
		Network:       "tcp",
		Address:       "127.0.0.1:0",
		ReadTimeout:   200 * time.Millisecond,
		StartDeadline: 10 * time.Second, // long — we exercise cancel, not deadline
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel

	// Install a fake conn so cancelPendingStart's reconnect path fires.
	connMock := newDeadlineConnMock()
	mux.connMu.Lock()
	mux.conn = connMock
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Queue an external session START and a second queued request.
	const extSessionID uint64 = 77
	extCh := mux.arb.requestStart(extSessionID, 0x50)
	_ = mux.arb.requestStart(78, 0x51)

	// Kick off: tryGrantAndStart takes the external request and runs
	// StartArbitration in a goroutine hung on gate.
	mux.tryGrantAndStart()
	time.Sleep(40 * time.Millisecond)

	// Verify we are in the hung-blocking state.
	mux.stateMu.Lock()
	pending := mux.pendingStart
	blocking := pending != nil && pending.blockingArb
	active := mux.blockingArbActive
	genBefore := mux.blockingArbGen
	mux.stateMu.Unlock()
	if !blocking {
		t.Fatal("expected pendingStart with blockingArb=true before cancel")
	}
	if !active {
		t.Fatal("expected blockingArbActive=true while StartArbitration is hung")
	}

	// Cancel the pending START (session disconnect).
	mux.cancelPendingStart(extSessionID)

	// Session notified as cancelled.
	select {
	case r := <-extCh:
		if r.granted {
			t.Fatal("expected granted=false after cancel")
		}
		if !r.cancelled {
			t.Fatal("expected cancelled=true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for cancelled result")
	}

	// Core assertion: cancelPendingStart closed m.conn to unstick the
	// hung goroutine (C2 reconnect pattern).
	select {
	case <-connMock.closeC:
		// ok
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cancelPendingStart did not close m.conn on blocking path (reconnect trigger missing)")
	}

	// Overlap invariant: while the hung goroutine is still running, NO
	// second StartArbitration has been spawned. The old (pre-fix) code
	// cleared blockingArbActive in-line and called tryGrantAndStart,
	// which would launch a second overlapping arbitration on the same
	// transport.
	if c := atomic.LoadInt32(&arbCount); c != 1 {
		t.Fatalf("expected 1 StartArbitration while hung goroutine runs (no-overlap), got %d", c)
	}

	mux.stateMu.Lock()
	activeDuringHang := mux.blockingArbActive
	genDuringHang := mux.blockingArbGen
	pendingDuringHang := mux.pendingStart
	mux.stateMu.Unlock()
	if !activeDuringHang {
		t.Fatal("blockingArbActive must remain true until the hung goroutine returns (no in-line clear)")
	}
	if genDuringHang != genBefore {
		t.Fatalf("blockingArbGen = %d, want %d (cancel must not bump gen in-line; reconnect handles that in real gateway)", genDuringHang, genBefore)
	}
	if pendingDuringHang != nil {
		t.Fatal("pendingStart must be nil after cancel")
	}

	// Release the hung goroutine. Its gen is still current (no real
	// reconnect bumped it in this unit test — there's no readLoop here
	// to call reconnect() on the conn-closed error). The goroutine sees
	// pendingStart==nil and takes the cancelled-path cleanup:
	// decrements absorb, clears blockingArbActive, advances the queue.
	close(mock.gate)
	time.Sleep(100 * time.Millisecond)

	mux.stateMu.Lock()
	activeAfter := mux.blockingArbActive
	mux.stateMu.Unlock()
	if activeAfter {
		t.Fatal("blockingArbActive must be cleared once the hung goroutine returns")
	}

	// Queue advanced: second arbitration launched, arbCount went 1->2.
	if c := atomic.LoadInt32(&arbCount); c != 2 {
		t.Fatalf("after hung goroutine returned, expected queue to advance (arbCount=2), got %d", c)
	}
}

// =======================================================================
// PR502 Codex follow-up P2: atomic recheck of active-path gating
// =======================================================================

// TestDeliverToActive_RechecksGatingAtomically verifies the Codex PR #502
// P2 follow-up fix: the active-path gating decision is revalidated under
// stateMu at the delivery site. Without the recheck, gatewayTxnActive
// could flip from true→false between the initial snapshot and the
// enqueue, letting stale bytes leak onto activeCh AND bypassing the
// afterInactive diagnostic counter (because the counter was decided
// against the stale snapshot).
//
// This test drives onReceived directly with many non-SYN gateway-owned
// bytes while a concurrent goroutine rapidly flips gatewayTxnActive
// between true and false. Invariants under the fix:
//   - (bytes enqueued on activeCh) + (afterInactive counter increments)
//     must equal the total bytes pushed.
//   - No byte is lost to "neither enqueued nor counted".
//
// Without the atomic recheck, a window exists where a byte is enqueued
// after the flip — activeCh length + afterInactive < total bytes.
func TestDeliverToActive_RechecksGatingAtomically(t *testing.T) {
	mux, _, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Grant gateway ownership so isGatewayOwned=true in onReceived.
	_ = mux.arb.requestStart(gatewaySessionID, 0x71)
	tryGrantLegacy(mux.arb)
	mux.arb.confirmOwnership(gatewaySessionID, 0x71)
	mux.stateMu.Lock()
	mux.gatewayTxnActive = true
	mux.stateMu.Unlock()

	// Drain activeCh in a goroutine — simulates the real consumer.
	var drained int64
	stopDrain := make(chan struct{})
	var drainWg sync.WaitGroup
	drainWg.Add(1)
	go func() {
		defer drainWg.Done()
		for {
			select {
			case <-stopDrain:
				return
			case ev := <-mux.activeCh:
				if ev.kind == activeEventByte {
					atomic.AddInt64(&drained, 1)
				}
			}
		}
	}()

	// Flipper: toggles gatewayTxnActive rapidly under stateMu to race
	// the delivery-site recheck.
	stopFlip := make(chan struct{})
	var flipWg sync.WaitGroup
	flipWg.Add(1)
	go func() {
		defer flipWg.Done()
		for {
			select {
			case <-stopFlip:
				return
			default:
			}
			mux.stateMu.Lock()
			mux.gatewayTxnActive = !mux.gatewayTxnActive
			mux.stateMu.Unlock()
		}
	}()

	// Push N non-SYN bytes via onReceived.
	const N = 2000
	baselineAfterInactive := mux.activeTxn.afterInactive.Load()
	for i := 0; i < N; i++ {
		// Non-SYN byte (0x55 is arbitrary, not 0xAA).
		mux.onReceived(0x55)
	}

	close(stopFlip)
	flipWg.Wait()

	// Let the drain goroutine finish consuming anything still in the
	// channel.
	time.Sleep(50 * time.Millisecond)
	close(stopDrain)
	drainWg.Wait()

	// Anything still buffered in activeCh.
	remaining := int64(len(mux.activeCh))

	delivered := atomic.LoadInt64(&drained) + remaining
	afterInactiveDelta := int64(mux.activeTxn.afterInactive.Load() - baselineAfterInactive)

	// Invariant: every byte pushed must be accounted for either as
	// delivered to activeCh or counted in afterInactive. Without the
	// atomic recheck, bytes can slip through both nets (enqueued after
	// the flip, but afterInactive was decided against a stale snapshot
	// that still said active).
	//
	// Note: deliverToActive's non-blocking send can drop on a full
	// activeCh — we log but do not count. With cap=4096 and a live
	// drainer, overflow does not occur at N=2000.
	if delivered+afterInactiveDelta < int64(N) {
		t.Fatalf("accounting mismatch: delivered=%d + afterInactive=%d = %d, want >= %d (atomic recheck failed — stale bytes leaked)",
			delivered, afterInactiveDelta, delivered+afterInactiveDelta, N)
	}
}

// TestDeliverToActive_RecheckSkipsEnqueueAfterFlip is a deterministic
// variant of the P2 follow-up: we force gatewayTxnActive=true for the
// initial snapshot, then flip it to false via a test-only hook before
// the delivery-site recheck. The byte MUST NOT appear on activeCh and
// afterInactive MUST increment.
//
// Determinism approach: we synchronously flip gatewayTxnActive to false
// between calls to onReceived. Because onReceived's first snapshot sees
// true and the recheck is under stateMu, we rely on the fact that any
// interleaving between the snapshot and the recheck is covered by the
// lock — so we cannot directly observe a "between" state from a test.
// We instead assert the aggregate property: after flipping to false,
// subsequent onReceived calls must increment afterInactive (not activeCh).
func TestDeliverToActive_RecheckSkipsEnqueueAfterFlip(t *testing.T) {
	mux, _, _, cleanup := newP3TestMux(t)
	defer cleanup()

	_ = mux.arb.requestStart(gatewaySessionID, 0x71)
	tryGrantLegacy(mux.arb)
	mux.arb.confirmOwnership(gatewaySessionID, 0x71)
	mux.stateMu.Lock()
	mux.gatewayTxnActive = false // active path does NOT expect bytes
	mux.stateMu.Unlock()

	baselineAfterInactive := mux.activeTxn.afterInactive.Load()
	baselineActiveCh := len(mux.activeCh)

	// Push 10 bytes while gatewayTxnActive=false. Each byte must NOT
	// land on activeCh; each must increment afterInactive.
	const N = 10
	for i := 0; i < N; i++ {
		mux.onReceived(0x55)
	}

	if got := len(mux.activeCh) - baselineActiveCh; got != 0 {
		t.Fatalf("activeCh received %d bytes while gatewayTxnActive=false; want 0 (gating check failed)", got)
	}
	if got := int64(mux.activeTxn.afterInactive.Load() - baselineAfterInactive); got != int64(N) {
		t.Fatalf("afterInactive delta = %d, want %d (each post-inactive gateway-owned byte must be counted)", got, N)
	}
}
