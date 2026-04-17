package adaptermux

import (
	"context"
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
//  2. The deadline callback does NOT call tryGrantAndStart (blockingArb flag
//     prevents overlapping arbitration on the same transport).
//  3. After the blocking call returns, the absorb counter is decremented
//     (the cancelled request is accounted for).
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

	// Verify no overlapping tryGrantAndStart was called by the deadline
	// callback (blockingArb prevents it). Since we have no second request
	// queued, we just verify pendingStart stays nil.
	time.Sleep(50 * time.Millisecond)
	mux.stateMu.Lock()
	queueAdvanced := mux.pendingStart != nil
	mux.stateMu.Unlock()
	if queueAdvanced {
		t.Fatal("deadline callback must NOT call tryGrantAndStart when blockingArb=true")
	}

	// Release the blocking StartArbitration goroutine.
	close(mock.gate)

	// Wait for the internal goroutine to process the cancelled-pending
	// path and update pendingStartAbsorb.
	time.Sleep(50 * time.Millisecond)

	// After the blocking call returns, the code path detects that
	// pendingStart was already cancelled (notify mismatch). The absorb
	// counter should be decremented back to 0.
	mux.stateMu.Lock()
	absorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if absorb != 0 {
		t.Fatalf("pendingStartAbsorb = %d after blocking call returned, want 0", absorb)
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
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

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

	// At this point, deadline has fired. blockingArb=true should
	// prevent tryGrantAndStart from being called for the second request.
	// The second request should still be pending in the queue.
	time.Sleep(50 * time.Millisecond)
	if c := atomic.LoadInt32(&arbCount); c != 1 {
		t.Fatalf("expected exactly 1 StartArbitration call (no overlap), got %d", c)
	}

	// Release the blocked goroutine.
	close(mock.gate)

	// After the blocking call returns, the cancelled-pending path now
	// calls tryGrantAndStart to advance the queue. The second request
	// (session 2) is dequeued and attempted. Since session 2 is not
	// registered in the sessions map, completeArbitrationGrant discards
	// it as "disconnected during START". Verify queue advanced (arbCount=2).
	time.Sleep(150 * time.Millisecond)
	if c := atomic.LoadInt32(&arbCount); c != 2 {
		t.Fatalf("expected 2 StartArbitration calls after queue advance, got %d", c)
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

	// Feed transaction echoes + response (real txn bytes).
	txnBytes := []byte{0x71, 0x08, 0xB5, 0x04, 0x01, 0x42, 0xCD, 0x00, 0x01, 0x33, 0xAB, 0x00}
	for _, b := range txnBytes {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(50 * time.Millisecond)

	// Consume via activeTransport.ReadByte so bytesRead increments —
	// this is what real bus.Send does for echo matching + response.
	at := mux.ActiveTransport()
	for range txnBytes {
		if _, err := at.ReadByte(); err != nil {
			t.Fatalf("ReadByte err=%v", err)
		}
	}

	// End-of-transaction: bus.Send has returned. Next SYN clears
	// gatewayTxnActive BEFORE IdleReleaseGrace expires.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

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

// TestActivePath_EarlyAbort_SYNBeforeRead_StaysActive verifies the
// lifecycle-correctness rule: a SYN arriving during gateway ownership
// BEFORE any response byte has been read does NOT terminate the
// transaction as syn_idle. This is a pre-grant stale SYN from the TCP
// buffer (normal grant-handoff residual).
//
// Genuine aborts (no writes, no reads) are resolved by MaxOwnershipDuration,
// ActiveReadTimeout, ActiveWriteError, ctx cancel, reset, or reconnect —
// NOT by SYN alone.
func TestActivePath_EarlyAbort_SYNBeforeRead_StaysActive(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	// Pre-grant stale SYN arrives with no reads yet. Must NOT clear.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if !snap.Active {
		t.Fatalf("activeTxn must stay active on SYN-before-read; got inactive reason=%q", snap.InactiveReason)
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
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
		time.Sleep(10 * time.Millisecond)

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
