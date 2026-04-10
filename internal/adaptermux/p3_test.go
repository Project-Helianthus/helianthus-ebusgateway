package adaptermux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// --- P3 test infrastructure ---

// p3MockTransport simulates an ENH transport that supports non-blocking
// RequestStart and surfaces STARTED/FAILED via ReadEvent (StreamEvent).
// It does NOT implement StartArbitration, so the blocking fallback is
// never used.
type p3MockTransport struct {
	mu     sync.Mutex
	closed bool

	// eventCh is the stream of events read by readLoop.
	// Tests push StreamEvents here to simulate adapter responses.
	eventCh chan transport.StreamEvent

	// startRequests records RequestStart calls.
	startRequests []byte

	// writtenBytes records Write calls.
	writtenBytes []byte
}

func newP3MockTransport() *p3MockTransport {
	return &p3MockTransport{
		eventCh: make(chan transport.StreamEvent, 256),
	}
}

func (t *p3MockTransport) RequestStart(initiator byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.startRequests = append(t.startRequests, initiator)
	return nil
}

func (t *p3MockTransport) ReadEvent() (transport.StreamEvent, error) {
	ev, ok := <-t.eventCh
	if !ok {
		return transport.StreamEvent{}, io.EOF
	}
	return ev, nil
}

func (t *p3MockTransport) ReadByte() (byte, error) {
	for {
		ev, err := t.ReadEvent()
		if err != nil {
			return 0, err
		}
		if ev.Kind == transport.StreamEventByte {
			return ev.Byte, nil
		}
		// Skip non-byte events (STARTED, FAILED, RESET)
	}
}

func (t *p3MockTransport) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writtenBytes = append(t.writtenBytes, p...)
	return len(p), nil
}

func (t *p3MockTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.closed = true
		close(t.eventCh)
	}
	return nil
}

func (t *p3MockTransport) Init(features byte) (byte, error) {
	return features, nil
}

func (t *p3MockTransport) getStartRequests() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]byte, len(t.startRequests))
	copy(result, t.startRequests)
	return result
}

// newP3TestMux creates a Mux with a p3MockTransport injected, fully
// started with readLoop and sendLoop goroutines running.
func newP3TestMux(t *testing.T) (*Mux, *p3MockTransport, context.CancelFunc, func()) {
	t.Helper()

	mock := newP3MockTransport()

	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	mux.ctx, mux.cancel = ctx, cancel

	// Inject mock transport.
	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Start readLoop and sendLoop.
	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()

	cleanup := func() {
		cancel()
		mock.Close()
		mux.wg.Wait()
	}

	return mux, mock, cancel, cleanup
}

// --- P3 tests ---

// TestObserverContinuityDuringArbitration verifies that bus bytes
// continue to flow to passive observers while a START request is
// pending at the adapter level. This is the core P3 fix: readLoop
// must NOT block during arbitration.
func TestObserverContinuityDuringArbitration(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Collect passive events.
	var passiveMu sync.Mutex
	var passiveSymbols []byte
	mux.SetPassiveCallback(func(pe PassiveEvent) {
		if pe.Kind == PassiveEventSymbol {
			passiveMu.Lock()
			passiveSymbols = append(passiveSymbols, pe.Symbol)
			passiveMu.Unlock()
		}
	})

	// Request a START for the gateway. This will call RequestStart
	// on the mock transport (non-blocking) and register a pendingStart.
	mux.arb.requestStart(gatewaySessionID, 0x31)

	// Feed a SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}

	// Wait for the START to be processed.
	time.Sleep(50 * time.Millisecond)

	// Verify RequestStart was called.
	starts := mock.getStartRequests()
	if len(starts) == 0 {
		t.Fatal("expected RequestStart to be called")
	}
	if starts[0] != 0x31 {
		t.Fatalf("RequestStart initiator = 0x%02X, want 0x31", starts[0])
	}

	// Verify pendingStart is set.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("expected pendingStart to be set after RequestStart")
	}

	// NOW: while START is pending, feed bus bytes. These MUST flow
	// to the passive observer without blocking.
	busBytes := []byte{0xAA, 0x10, 0x08, 0x07, 0x04, 0x00, 0x55, 0x66, 0x77, 0x88, 0xCC}
	for _, b := range busBytes {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}

	// Wait for delivery.
	time.Sleep(100 * time.Millisecond)

	// Verify passive observer received all bytes.
	passiveMu.Lock()
	received := make([]byte, len(passiveSymbols))
	copy(received, passiveSymbols)
	passiveMu.Unlock()

	// The first SYN that triggered tryGrantAndStart should also be in
	// the passive stream, plus all the bus bytes.
	expectedLen := 1 + len(busBytes) // initial SYN + bus bytes
	if len(received) < expectedLen {
		t.Fatalf("passive received %d symbols, want at least %d (observer continuity broken)", len(received), expectedLen)
	}

	// Now resolve the pending START with STARTED.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31}

	time.Sleep(50 * time.Millisecond)

	// Verify ownership was confirmed.
	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("gateway should own the bus after STARTED")
	}
}

// TestArbitrationResponse_Started verifies that a STARTED event from
// the adapter confirms ownership and notifies the requester.
func TestArbitrationResponse_Started(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Request START for the gateway.
	ch := mux.arb.requestStart(gatewaySessionID, 0x42)

	// Feed SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}

	time.Sleep(50 * time.Millisecond)

	// Verify pendingStart exists.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("expected pendingStart after SYN")
	}

	// Inject STARTED from adapter.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x42}

	// Wait for result.
	select {
	case result := <-ch:
		if !result.granted {
			t.Fatal("expected granted=true after STARTED")
		}
		if result.initiator != 0x42 {
			t.Fatalf("result.initiator = 0x%02X, want 0x42", result.initiator)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for START result")
	}

	// Verify ownership set.
	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("gateway should own bus after STARTED")
	}

	// Verify pendingStart cleared.
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	mux.stateMu.Unlock()
	if hasPending {
		t.Fatal("pendingStart should be nil after STARTED")
	}
}

// TestArbitrationResponse_Failed verifies that a FAILED event from
// the adapter notifies the requester of failure without setting ownership.
func TestArbitrationResponse_Failed(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	ch := mux.arb.requestStart(gatewaySessionID, 0x42)

	// Feed SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Inject FAILED from adapter (winner byte = 0x10).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x10}

	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected granted=false after FAILED")
		}
		// initiator carries the winner address from the FAILED event.
		if result.initiator != 0x10 {
			t.Fatalf("result.initiator = 0x%02X, want 0x10 (winner)", result.initiator)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for START result")
	}

	// Verify NO ownership.
	if mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("gateway should NOT own bus after FAILED")
	}
}

// TestArbitrationResponse_SessionDisconnected verifies that if a
// session disconnects while a START is pending at the adapter, the
// STARTED response does not confirm ownership for a dead session.
func TestArbitrationResponse_SessionDisconnected(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Add an external session.
	client, server := net.Pipe()
	defer client.Close()
	id := mux.AddSession(server)

	// Request START for the external session.
	ch := mux.arb.requestStart(id, 0x55)

	// Feed SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Verify pendingStart is for this session.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil && mux.pendingStart.sessionID == id
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("expected pendingStart for external session")
	}

	// Disconnect the session BEFORE adapter responds.
	mux.RemoveSession(id)

	// Inject STARTED from adapter.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x55}

	// The result should be failure (session disconnected).
	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected granted=false for disconnected session")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for START result")
	}

	// Verify NO ownership set.
	if mux.arb.isOwner(id) {
		t.Fatal("dead session should NOT own the bus")
	}
}

// TestCancelPendingStart verifies that cancelPendingStart clears an
// in-flight pending START for the given session and notifies failure.
func TestCancelPendingStart(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Add an external session.
	client, server := net.Pipe()
	defer client.Close()
	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Request START.
	ch := mux.arb.requestStart(id, 0x33)

	// Feed SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Verify pending.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil && mux.pendingStart.sessionID == id
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("expected pendingStart for session")
	}

	// Cancel the pending START via SYN cancel path.
	// This simulates session.handleStart receiving START with SYN.
	mux.arb.cancelStart(id)
	mux.arb.releaseOwnership(id)
	mux.cancelPendingStart(id)

	// The arbitration result should be failure.
	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected granted=false after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for cancel result")
	}

	// Verify pendingStart is cleared.
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	mux.stateMu.Unlock()
	if hasPending {
		t.Fatal("pendingStart should be nil after cancel")
	}
}

// TestCancelPendingStart_WrongSession verifies that cancelPendingStart
// is a no-op when the pending START belongs to a different session.
func TestCancelPendingStart_WrongSession(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Request START for the gateway.
	mux.arb.requestStart(gatewaySessionID, 0x31)

	// Feed SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Verify pending is for gateway.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil && mux.pendingStart.sessionID == gatewaySessionID
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("expected pendingStart for gateway")
	}

	// Try to cancel with a different session ID — should be no-op.
	mux.cancelPendingStart(42)

	// pendingStart should still be set.
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("pendingStart should NOT be cleared by wrong session cancel")
	}
}

// TestCancelPendingStart_DuringRequestStart verifies the P1 fix:
// pendingStart is registered BEFORE RequestStart so that a concurrent
// cancel during RequestStart finds and clears the pending entry.
// Without the fix, neither arb.cancelStart (already dequeued) nor
// cancelPendingStart (not yet stored) could see the in-flight request.
func TestCancelPendingStart_DuringRequestStart(t *testing.T) {
	// Use a delayed mock transport that blocks inside RequestStart
	// long enough for us to issue a cancel.
	mock := &delayedStartTransport{
		p3MockTransport: newP3MockTransport(),
		startGate:       make(chan struct{}),
	}

	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Add an external session.
	client, server := net.Pipe()
	defer client.Close()
	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Request START for the external session.
	ch := mux.arb.requestStart(id, 0x55)

	// Call tryGrantAndStart in a goroutine — it will block inside
	// RequestStart until we release startGate.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mux.tryGrantAndStart()
	}()

	// Wait for RequestStart to be entered (the goroutine blocks on startGate).
	time.Sleep(30 * time.Millisecond)

	// P1 fix validation: pendingStart MUST be set even though
	// RequestStart has not returned yet.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil && mux.pendingStart.sessionID == id
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("P1 regression: pendingStart must be set BEFORE RequestStart returns")
	}

	// Cancel the pending START — simulates a client sending START
	// cancel (0xAA) while RequestStart is in flight.
	mux.cancelPendingStart(id)

	// Now release RequestStart so tryGrantAndStart can return.
	close(mock.startGate)
	wg.Wait()

	// The result should be failure (cancelled).
	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected granted=false — START was cancelled during RequestStart")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for cancel result")
	}

	// Verify NO ownership set — the cancelled request must not
	// grant the bus, even if a STARTED arrives later.
	if mux.arb.isOwner(id) {
		t.Fatal("cancelled session should NOT own the bus")
	}
}

// delayedStartTransport wraps p3MockTransport with a blocking gate
// in RequestStart, allowing tests to exercise the cancellation race.
type delayedStartTransport struct {
	*p3MockTransport
	startGate chan struct{} // blocks RequestStart until closed
}

func (d *delayedStartTransport) RequestStart(initiator byte) error {
	// Block until the test releases the gate.
	<-d.startGate
	return d.p3MockTransport.RequestStart(initiator)
}

// TestOnlyOnePendingStart verifies that tryGrantAndStart is a no-op
// when there is already a pending START in flight.
func TestOnlyOnePendingStart(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Request two STARTs.
	ch1 := mux.arb.requestStart(gatewaySessionID, 0x31)

	// Add an external session and request a second START.
	client, server := net.Pipe()
	defer client.Close()
	id := mux.AddSession(server)
	defer mux.RemoveSession(id)
	ch2 := mux.arb.requestStart(id, 0x42)

	// Feed SYN to trigger tryGrantAndStart — should grant first (gateway priority).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Verify exactly one RequestStart call (gateway has priority).
	starts := mock.getStartRequests()
	if len(starts) != 1 {
		t.Fatalf("expected 1 RequestStart call, got %d", len(starts))
	}
	if starts[0] != 0x31 {
		t.Fatalf("first RequestStart initiator = 0x%02X, want 0x31 (gateway)", starts[0])
	}

	// Feed another SYN — tryGrantAndStart should be no-op (pendingStart set).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Still only 1 RequestStart call.
	starts = mock.getStartRequests()
	if len(starts) != 1 {
		t.Fatalf("expected still 1 RequestStart call after second SYN, got %d", len(starts))
	}

	// Resolve first pending with STARTED.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31}

	select {
	case result := <-ch1:
		if !result.granted {
			t.Fatal("expected gateway START to be granted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for gateway START result")
	}

	// handleArbitrationResponse should have called tryGrantAndStart
	// for the remaining external request. Feed a SYN to release
	// ownership first (bus is now owned by gateway).
	mux.arb.releaseOwnership(gatewaySessionID)

	// Feed SYN to trigger the next grant attempt.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Now the external session's START should have been requested.
	starts = mock.getStartRequests()
	if len(starts) != 2 {
		t.Fatalf("expected 2 RequestStart calls after resolving first, got %d", len(starts))
	}
	if starts[1] != 0x42 {
		t.Fatalf("second RequestStart initiator = 0x%02X, want 0x42 (external)", starts[1])
	}

	// Resolve second with STARTED.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x42}

	select {
	case result := <-ch2:
		if !result.granted {
			t.Fatal("expected external START to be granted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for external START result")
	}
}

// TestArbitrationResponse_StaleIsHarmless verifies that receiving a
// STARTED/FAILED when no pending START exists logs but does not panic.
func TestArbitrationResponse_StaleIsHarmless(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Inject STARTED without any pending START.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x42}

	// Wait for readLoop to process it.
	time.Sleep(50 * time.Millisecond)

	// No crash, no panic. Verify mux is still functional.
	if mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("no ownership should be set from stale response")
	}

	// Inject FAILED without pending START.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x10}
	time.Sleep(50 * time.Millisecond)

	// Still no crash.
}

// TestHandleReset_ClearsPendingStart verifies that handleReset clears
// any in-flight pending START and notifies the requester of failure.
func TestHandleReset_ClearsPendingStart(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	ch := mux.arb.requestStart(gatewaySessionID, 0x31)

	// Feed SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Verify pending.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("expected pendingStart")
	}

	// Trigger RESETTED.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventReset}
	time.Sleep(50 * time.Millisecond)

	// The pending START should have been cancelled.
	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected failure after reset")
		}
		if result.err == nil {
			t.Fatal("expected error on reset-cancelled START")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for cancelled START result")
	}

	// Verify pendingStart cleared.
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	mux.stateMu.Unlock()
	if hasPending {
		t.Fatal("pendingStart should be nil after reset")
	}
}

// TestP3_ActivePathReceivesDuringPending verifies that the active
// path (activeCh) also receives bytes while a START is pending.
func TestP3_ActivePathReceivesDuringPending(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Request START.
	mux.arb.requestStart(gatewaySessionID, 0x31)

	// Feed SYN + data bytes.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	// Feed data bytes while START is pending.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08}
	time.Sleep(50 * time.Millisecond)

	// Drain active channel and count received bytes.
	var activeCount int
	drainTimeout := time.After(200 * time.Millisecond)
drain:
	for {
		select {
		case <-mux.activeCh:
			activeCount++
		case <-drainTimeout:
			break drain
		}
	}

	// Should have received: SYN + 0x10 + 0x08 = 3 bytes minimum.
	if activeCount < 3 {
		t.Fatalf("active path received %d bytes during pending START, want >= 3", activeCount)
	}
}

// TestP3_ExternalSessionReceivesDuringPending verifies that external
// sessions receive bus bytes while a gateway START is pending.
func TestP3_ExternalSessionReceivesDuringPending(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Add external session.
	client, server := net.Pipe()
	defer client.Close()
	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Read session output in background.
	var receivedCount atomic.Int32
	go func() {
		buf := make([]byte, 1)
		for {
			_, err := client.Read(buf)
			if err != nil {
				return
			}
			receivedCount.Add(1)
		}
	}()

	// Request gateway START.
	mux.arb.requestStart(gatewaySessionID, 0x31)

	// Feed SYN + data bytes.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	// Feed 5 data bytes while START is pending.
	for i := byte(0); i < 5; i++ {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10 + i}
	}
	time.Sleep(100 * time.Millisecond)

	// External session should have received bytes (SYN + 5 data = 6).
	count := receivedCount.Load()
	if count < 5 {
		t.Fatalf("external session received %d bytes during pending gateway START, want >= 5", count)
	}
}

// TestP3_Close_ClearsPendingStart verifies that Close() cancels any
// in-flight pending START.
func TestP3_Close_ClearsPendingStart(t *testing.T) {
	mock := newP3MockTransport()

	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Manually set a pending START.
	ch := make(chan startResult, 1)
	mux.stateMu.Lock()
	mux.pendingStart = &pendingStartState{
		sessionID: gatewaySessionID,
		initiator: 0x31,
		notify:    ch,
	}
	mux.stateMu.Unlock()

	// Close the mux.
	cancel()
	mock.Close()
	mux.Close()

	// The pending START should have been cancelled.
	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected failure after close")
		}
		if result.err == nil {
			t.Fatal("expected error on close-cancelled START")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for close-cancelled START result")
	}
}

// TestP3_FallbackStartArbitration verifies that transports implementing
// only StartArbitration (not RequestStart) still work via the blocking
// fallback path.
func TestP3_FallbackStartArbitration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use mockInitTransport which has StartArbitration via Init but
	// NOT RequestStart. We add StartArbitration to it.
	mock := &blockingStartTransport{
		readCh: make(chan byte, 256),
	}

	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
	})
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Request START.
	ch := mux.arb.requestStart(gatewaySessionID, 0x31)

	// Call tryGrantAndStart directly (simulating SYN handler).
	mux.tryGrantAndStart()

	// Should have used the blocking fallback.
	calls := mock.getStartCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 StartArbitration call, got %d", len(calls))
	}
	if calls[0] != 0x31 {
		t.Fatalf("StartArbitration initiator = 0x%02X, want 0x31", calls[0])
	}

	// Result should be immediately available (blocking path).
	select {
	case result := <-ch:
		if !result.granted {
			t.Fatal("expected granted=true from blocking fallback")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocking fallback result")
	}

	// Ownership should be set.
	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("gateway should own bus after blocking fallback START")
	}
}

// blockingStartTransport implements StartArbitration but NOT RequestStart.
type blockingStartTransport struct {
	mu         sync.Mutex
	readCh     chan byte
	startCalls []byte
}

func (b *blockingStartTransport) ReadByte() (byte, error) {
	v, ok := <-b.readCh
	if !ok {
		return 0, io.EOF
	}
	return v, nil
}

func (b *blockingStartTransport) Write(p []byte) (int, error) {
	return len(p), nil
}

func (b *blockingStartTransport) Close() error {
	return nil
}

func (b *blockingStartTransport) StartArbitration(initiator byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.startCalls = append(b.startCalls, initiator)
	return nil
}

func (b *blockingStartTransport) getStartCalls() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]byte, len(b.startCalls))
	copy(result, b.startCalls)
	return result
}

// --- P1 #3062452008: RequestStart failure after cancelPendingStart must not double-send ---

// failingStartTransport wraps p3MockTransport with a blocking gate
// that, when released, makes RequestStart return an error. This exercises
// the race where cancelPendingStart sends on notify BEFORE RequestStart
// returns an error — the error path must not send a second time.
type failingStartTransport struct {
	*p3MockTransport
	startGate chan struct{} // blocks RequestStart until closed
	startErr  error        // error to return after gate opens
}

func (f *failingStartTransport) RequestStart(initiator byte) error {
	<-f.startGate
	return f.startErr
}

// TestRequestStartFailAfterCancel_NoDoubleSend verifies the P1 fix:
// when cancelPendingStart already cleared pendingStart and sent on
// notify, a subsequent RequestStart error must NOT send a second result
// on the cap-1 channel (which would block forever, pinning readLoop).
func TestRequestStartFailAfterCancel_NoDoubleSend(t *testing.T) {
	mock := &failingStartTransport{
		p3MockTransport: newP3MockTransport(),
		startGate:       make(chan struct{}),
		startErr:        errors.New("adapter disconnected"),
	}

	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Add an external session.
	client, server := net.Pipe()
	defer client.Close()
	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Request START for the external session.
	ch := mux.arb.requestStart(id, 0x55)

	// Call tryGrantAndStart in a goroutine — blocks inside RequestStart.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mux.tryGrantAndStart()
	}()

	// Wait for RequestStart to be entered.
	time.Sleep(30 * time.Millisecond)

	// Verify pendingStart is set.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil && mux.pendingStart.sessionID == id
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("pendingStart must be set before RequestStart returns")
	}

	// Simulate session disconnect: cancelPendingStart clears pendingStart
	// and sends failure on notify.
	mux.cancelPendingStart(id)

	// Release RequestStart — it will return startErr.
	// Without the fix, tryGrantAndStart would send a second result on
	// the cap-1 channel, blocking forever.
	close(mock.startGate)

	// tryGrantAndStart must return promptly (not block on double-send).
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Good — tryGrantAndStart returned without blocking.
	case <-time.After(3 * time.Second):
		t.Fatal("tryGrantAndStart blocked — likely double-send on notify channel")
	}

	// Drain the result — should be the cancel failure from cancelPendingStart.
	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected granted=false from cancel path")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for cancel result on notify channel")
	}

	// Channel must be empty — no second result from the error path.
	select {
	case extra := <-ch:
		t.Fatalf("unexpected second result on notify channel: %+v", extra)
	default:
		// Good — no double-send.
	}
}

// TestHandleArbitrationResponse_StaleStartedIgnored verifies that a STARTED
// event whose confirmed initiator does not match the pending request is
// treated as stale: ownership must NOT be granted, and pendingStart must
// remain set so the real response can still be processed.
func TestHandleArbitrationResponse_StaleStartedIgnored(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Request START with initiator 0x71 for the gateway session.
	ch := mux.arb.requestStart(gatewaySessionID, 0x71)

	// Feed SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}

	time.Sleep(50 * time.Millisecond)

	// Verify pendingStart exists with initiator 0x71.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil
	var pendingInit byte
	if hasPending {
		pendingInit = mux.pendingStart.initiator
	}
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("expected pendingStart after SYN")
	}
	if pendingInit != 0x71 {
		t.Fatalf("pendingStart.initiator = 0x%02X, want 0x71", pendingInit)
	}

	// Inject a STARTED with wrong initiator 0x31 (stale from a
	// cancelled request). This must NOT grant ownership.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31}

	time.Sleep(50 * time.Millisecond)

	// Ownership must NOT be set.
	if mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("stale STARTED should not grant ownership")
	}

	// pendingStart must still be set (restored after stale rejection).
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("pendingStart should be restored after stale STARTED")
	}

	// No result should have been sent on the notify channel.
	select {
	case result := <-ch:
		t.Fatalf("unexpected result on notify channel: %+v", result)
	default:
		// Good — stale STARTED was ignored.
	}

	// Now inject the correct STARTED for 0x71. This must succeed.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x71}

	select {
	case result := <-ch:
		if !result.granted {
			t.Fatal("expected granted=true after correct STARTED")
		}
		if result.initiator != 0x71 {
			t.Fatalf("result.initiator = 0x%02X, want 0x71", result.initiator)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for START result after correct STARTED")
	}

	// Verify ownership now confirmed.
	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("gateway should own bus after correct STARTED")
	}

	// pendingStart must be cleared.
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	mux.stateMu.Unlock()
	if hasPending {
		t.Fatal("pendingStart should be nil after correct STARTED")
	}
}
