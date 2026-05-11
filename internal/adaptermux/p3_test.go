package adaptermux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
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

	// readTimeout, if > 0, causes ReadEvent to return ErrTimeout
	// after this duration instead of blocking indefinitely (AM33).
	readTimeout time.Duration
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
	// AM33: optional read timeout for more realistic mock behavior.
	if t.readTimeout > 0 {
		select {
		case ev, ok := <-t.eventCh:
			if !ok {
				return transport.StreamEvent{}, io.EOF
			}
			return ev, nil
		case <-time.After(t.readTimeout):
			return transport.StreamEvent{}, ebuserrors.ErrTimeout
		}
	}
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

// BytesAreUnescaped reports that this mock delivers ENH-style
// pre-unescaped logical bytes (matching real ENHTransport semantics).
func (t *p3MockTransport) BytesAreUnescaped() bool { return true }

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
		closeOrLog(t, mock, "p3 mock transport")
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
// pending START is cancelled immediately (not deferred to adapter
// response), the absorb counter is set, and the stale adapter
// response is absorbed without affecting arbitration.
func TestArbitrationResponse_SessionDisconnected(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Add an external session.
	client, server := net.Pipe()
	defer closeOrLog(t, client, "client")
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
	// RemoveSession must call cancelPendingStart to immediately clear
	// pendingStart and unblock arbitration (P3 fix: #3062875632).
	mux.RemoveSession(id)

	// pendingStart must be nil immediately after RemoveSession.
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	absorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if hasPending {
		t.Fatal("pendingStart should be nil immediately after RemoveSession")
	}
	if absorb != 1 {
		t.Fatalf("pendingStartAbsorb = %d after RemoveSession, want 1", absorb)
	}

	// The result should be failure, delivered immediately by
	// cancelPendingStart (not deferred to adapter response).
	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected granted=false for disconnected session")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for START result")
	}

	// Inject STARTED from adapter — must be absorbed as stale.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x55}
	time.Sleep(50 * time.Millisecond)

	// absorb counter should be decremented back to 0.
	mux.stateMu.Lock()
	absorb = mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if absorb != 0 {
		t.Fatalf("pendingStartAbsorb = %d after absorbing stale STARTED, want 0", absorb)
	}

	// Verify NO ownership set.
	if mux.arb.isOwner(id) {
		t.Fatal("dead session should NOT own the bus")
	}
}

// TestRemoveSession_UnblocksNextRequest verifies that RemoveSession
// clears the pending START and advances the next queued session after
// absorbing the cancelled request's stale adapter response.
func TestRemoveSession_UnblocksNextRequest(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Session A — will be disconnected.
	clientA, serverA := net.Pipe()
	defer closeOrLog(t, clientA, "clientA")
	idA := mux.AddSession(serverA)

	// Session B — should be serviced after A disconnects.
	clientB, serverB := net.Pipe()
	defer closeOrLog(t, clientB, "clientB")
	idB := mux.AddSession(serverB)
	defer mux.RemoveSession(idB)

	// Queue START requests for both sessions.
	mux.arb.requestStart(idA, 0x55)
	mux.arb.requestStart(idB, 0x66)

	// Feed SYN to trigger tryGrantAndStart — grants A first.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Verify pendingStart is for session A.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil && mux.pendingStart.sessionID == idA
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("expected pendingStart for session A")
	}

	// Disconnect A while its START is pending at the adapter.
	mux.RemoveSession(idA)
	time.Sleep(50 * time.Millisecond)

	// The cancelled START is still in flight at the adapter. The mux must
	// wait for and absorb its stale response before issuing B's START,
	// otherwise the stale STARTED/FAILED cannot be distinguished from B.
	mux.stateMu.Lock()
	hasPendingAfterCancel := mux.pendingStart != nil
	absorbAfterCancel := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if hasPendingAfterCancel {
		t.Fatal("pendingStart should be cleared after RemoveSession(A)")
	}
	if absorbAfterCancel != 1 {
		t.Fatalf("pendingStartAbsorb = %d after RemoveSession(A), want 1", absorbAfterCancel)
	}

	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x10}
	time.Sleep(50 * time.Millisecond)

	// pendingStart should now be for session B — the stale response was
	// absorbed and tryGrantAndStart picked up B's queued request.
	mux.stateMu.Lock()
	hasPendingB := mux.pendingStart != nil && mux.pendingStart.sessionID == idB
	absorbAfterStale := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if absorbAfterStale != 0 {
		t.Fatalf("pendingStartAbsorb = %d after stale FAILED, want 0", absorbAfterStale)
	}
	if !hasPendingB {
		t.Fatal("expected pendingStart to advance to session B after absorbing stale FAILED")
	}
}

func TestPendingStartAbsorbTimeoutUnblocksQueue(t *testing.T) {
	mux, _, _, cleanup := newP3TestMux(t)
	defer cleanup()
	mux.cfg.StartDeadline = 50 * time.Millisecond

	client, server := net.Pipe()
	defer closeOrLog(t, client, "client")
	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	mux.arb.requestStart(id, 0x66)

	mux.stateMu.Lock()
	mux.armPendingStartAbsorbLocked("test")
	mux.stateMu.Unlock()

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		mux.stateMu.Lock()
		absorb := mux.pendingStartAbsorb
		hasPending := mux.pendingStart != nil && mux.pendingStart.sessionID == id
		mux.stateMu.Unlock()
		if absorb == 0 && hasPending {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	mux.stateMu.Lock()
	absorb := mux.pendingStartAbsorb
	hasPending := mux.pendingStart != nil && mux.pendingStart.sessionID == id
	mux.stateMu.Unlock()
	if absorb != 0 {
		t.Fatalf("pendingStartAbsorb = %d after absorb timeout, want 0", absorb)
	}
	if !hasPending {
		t.Fatal("expected queued START to advance after absorb timeout")
	}
}

// TestCancelPendingStart verifies that cancelPendingStart clears an
// in-flight pending START for the given session and notifies failure.
func TestCancelPendingStart(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Add an external session.
	client, server := net.Pipe()
	defer closeOrLog(t, client, "client")
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
	defer closeOrLog(t, client, "client")
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
	defer closeOrLog(t, client, "client")
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

// TestP3_ActivePathDoesNotAccumulateDuringPending verifies that bytes
// arriving while gateway START is PENDING (not yet confirmed) do NOT
// accumulate on activeCh. bus.Send is blocked inside StartArbitration
// waiting for the grant — it is not reading activeCh. Any bytes that
// arrived before STARTED are third-party traffic belonging to the
// passive path, not to the gateway's transaction.
//
// Policy (runtime soak fix): deliver to activeCh ONLY when gateway
// owns the bus. Idle SYN bursts and third-party traffic during
// pending START must go through the passive path, not activeCh.
// This prevents the 4096-slot activeCh from overflowing with
// unconsumed traffic (runtime incident: 667k dropped bytes/4min).
func TestP3_ActivePathDoesNotAccumulateDuringPending(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Request START.
	mux.arb.requestStart(gatewaySessionID, 0x31)

	// Feed SYN + data bytes before STARTED is delivered.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08}
	time.Sleep(50 * time.Millisecond)

	// Verify pendingStart is set (we're in the pending window).
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("expected pendingStart during this window")
	}

	// Drain active channel — should be empty or nearly empty.
	// No bytes should accumulate because gateway does not yet own the bus.
	var activeCount int
	drainTimeout := time.After(100 * time.Millisecond)
drain:
	for {
		select {
		case <-mux.activeCh:
			activeCount++
		case <-drainTimeout:
			break drain
		}
	}

	// Policy: bytes during pending START go to passive, not active.
	// activeCh should remain empty (no owner → no consumer).
	if activeCount != 0 {
		t.Fatalf("activeCh accumulated %d bytes during pending START, want 0 (runtime soak policy)", activeCount)
	}
}

// TestP3_ExternalSessionReceivesDuringPending verifies that external
// sessions receive bus bytes while a gateway START is pending.
func TestP3_ExternalSessionReceivesDuringPending(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Add external session.
	client, server := net.Pipe()
	defer closeOrLog(t, client, "client")
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
	closeOrLog(t, mock, "mock")
	closeOrLog(t, mux, "mux")

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
	// PR502-Fix1: the blocking StartArbitration now runs in a goroutine,
	// so tryGrantAndStart returns immediately. Wait for the result on ch.
	mux.tryGrantAndStart()

	// Result arrives asynchronously from the blocking goroutine.
	select {
	case result := <-ch:
		if !result.granted {
			t.Fatal("expected granted=true from blocking fallback")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocking fallback result")
	}

	// Should have used the blocking fallback.
	calls := mock.getStartCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 StartArbitration call, got %d", len(calls))
	}
	if calls[0] != 0x31 {
		t.Fatalf("StartArbitration initiator = 0x%02X, want 0x31", calls[0])
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
	startErr  error         // error to return after gate opens
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
	defer closeOrLog(t, client, "client")
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

// TestAbsorbDecrementOnRequestStartFailAfterCancel verifies P1 #3062745920:
// when cancelPendingStart increments pendingStartAbsorb and then RequestStart
// fails (adapter never received the START), the absorb counter must be
// decremented. Otherwise the next real arbitration response for a newer
// request would be incorrectly consumed as stale, leaving that request
// unresolved.
func TestAbsorbDecrementOnRequestStartFailAfterCancel(t *testing.T) {
	mock := &failingStartTransport{
		p3MockTransport: newP3MockTransport(),
		startGate:       make(chan struct{}),
		startErr:        errors.New("adapter write error"),
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
	defer closeOrLog(t, client, "client")
	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// --- Phase 1: Request A — will be cancelled then fail ---
	chA := mux.arb.requestStart(id, 0x55)

	// Start tryGrantAndStart — blocks inside RequestStart.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mux.tryGrantAndStart()
	}()

	// Wait for RequestStart to be entered and pendingStart to be set.
	time.Sleep(30 * time.Millisecond)

	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil && mux.pendingStart.sessionID == id
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("pendingStart must be set before RequestStart returns")
	}

	// Cancel the pending request — this increments pendingStartAbsorb.
	mux.cancelPendingStart(id)

	// Verify absorb counter was incremented.
	mux.stateMu.Lock()
	absorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if absorb != 1 {
		t.Fatalf("pendingStartAbsorb = %d after cancel, want 1", absorb)
	}

	// Release RequestStart — it will return an error.
	// The error path detects the pending was already cancelled and
	// must decrement the absorb counter.
	close(mock.startGate)

	// Wait for tryGrantAndStart to finish.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("tryGrantAndStart blocked")
	}

	// Drain cancel result from A.
	select {
	case result := <-chA:
		if result.granted {
			t.Fatal("expected granted=false from cancel path")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for cancel result")
	}

	// KEY ASSERTION: absorb counter must be 0 — the adapter never
	// received the START, so no stale response will arrive.
	mux.stateMu.Lock()
	absorb = mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if absorb != 0 {
		t.Fatalf("pendingStartAbsorb = %d after RequestStart fail, want 0 "+
			"(adapter never received START, no stale response expected)", absorb)
	}

	// --- Phase 2: Request B — must NOT be consumed as stale ---
	// Re-add session since arb state may have been cleared.
	client2, server2 := net.Pipe()
	defer closeOrLog(t, client2, "client2")
	id2 := mux.AddSession(server2)
	defer mux.RemoveSession(id2)

	// Use a normal transport for B so RequestStart succeeds.
	normalMock := newP3MockTransport()
	mux.connMu.Lock()
	mux.upstream = normalMock
	mux.connMu.Unlock()

	chB := mux.arb.requestStart(id2, 0x71)

	// Grant and start B.
	mux.tryGrantAndStart()

	// Verify pendingStart is set for B.
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil && mux.pendingStart.sessionID == id2
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("pendingStart must be set for request B")
	}

	// Deliver STARTED for B — must be processed normally, NOT absorbed.
	mux.handleArbitrationResponse(true, 0x71)

	select {
	case result := <-chB:
		if !result.granted {
			t.Fatal("expected granted=true for request B after STARTED")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for STARTED result on request B — " +
			"response was likely consumed as stale due to non-zero absorb counter")
	}
}

// TestHandleArbitrationResponse_StaleStartedFailsPending verifies that a
// STARTED event whose confirmed initiator does not match the pending request
// clears pendingStart and delivers FAILED to the pending session (AM56 fix).
// This prevents permanent bus deadlock from a stale STARTED leaving
// pendingStart set indefinitely.
func TestHandleArbitrationResponse_StaleStartedFailsPending(t *testing.T) {
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

	// AM56: pendingStart must be CLEARED (not restored).
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	mux.stateMu.Unlock()
	if hasPending {
		t.Fatal("pendingStart should be cleared after STARTED mismatch (AM56)")
	}

	// AM56: FAILED must have been delivered on the notify channel.
	// Codex P2 round 7 on PR #620: result.initiator carries the
	// THIRD-PARTY winner byte (the value observed on the wire), not the
	// pending bidder's own initiator. This matches normal FAILED
	// semantics so the bidder's handleStart goroutine can deliver
	// ENHResFailed(winner_byte), giving the external session enough
	// information to keep its bus reconstructor in sync.
	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected granted=false on STARTED mismatch")
		}
		if result.err == nil {
			t.Fatal("expected error on STARTED mismatch")
		}
		if result.initiator != 0x31 {
			t.Fatalf("result.initiator = 0x%02X, want 0x31 (third-party winner observed on wire, Codex P2 round 7 PR #620)", result.initiator)
		}
	default:
		t.Fatal("expected result on notify channel after STARTED mismatch")
	}
}

// TestHandleArbitrationResponse_StaleFailedIgnored verifies that a FAILED
// event from a cancelled request does not incorrectly fail a newer pending
// request. On ENH, FAILED carries the WINNER's address (not the loser's),
// so initiator matching cannot be used. The epoch counter detects staleness.
//
// Race scenario:
//  1. Request A → pendingStart (epoch=1), START sent to adapter
//  2. cancelPendingStart clears A, sends failure to A
//  3. Request B → pendingStart (epoch=2), START sent to adapter
//  4. Stale FAILED for A arrives — must NOT fail B
func TestHandleArbitrationResponse_StaleFailedIgnored(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// --- Phase 1: Request A (gateway, initiator 0x71) ---
	chA := mux.arb.requestStart(gatewaySessionID, 0x71)

	// Feed SYN to trigger tryGrantAndStart for A.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Verify pendingStart exists.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("expected pendingStart after SYN for request A")
	}

	// --- Phase 2: Cancel A via cancelPendingStart ---
	mux.cancelPendingStart(gatewaySessionID)

	// A's channel must receive a failure.
	select {
	case resultA := <-chA:
		if resultA.granted {
			t.Fatal("cancelled request A should not be granted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for cancel result on request A")
	}

	// pendingStart must be nil after cancel; absorb counter must be 1.
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	absorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if hasPending {
		t.Fatal("pendingStart should be nil after cancelPendingStart")
	}
	if absorb != 1 {
		t.Fatalf("pendingStartAbsorb = %d after cancel, want 1", absorb)
	}

	// --- Phase 3: Request B (gateway, initiator 0x71) ---
	chB := mux.arb.requestStart(gatewaySessionID, 0x71)

	// Feed SYN to trigger tryGrantAndStart for B.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// New invariant: B must NOT start until A's stale response has been
	// absorbed. The absorb counter is now a real regrant barrier.
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	starts := mock.getStartRequests()
	mux.stateMu.Unlock()
	if hasPending {
		t.Fatal("pendingStart for B must stay nil until stale FAILED is absorbed")
	}
	if len(starts) != 1 {
		t.Fatalf("RequestStart calls before stale FAILED = %d, want 1", len(starts))
	}

	// --- Phase 4: Inject stale FAILED for A ---
	// FAILED data=0x10 (the winner of that old arbitration round — irrelevant).
	// This must be absorbed by the counter, NOT fail request B. After absorb,
	// tryGrantAndStart should automatically launch B.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x10}
	time.Sleep(50 * time.Millisecond)

	// B's channel must NOT have received anything.
	select {
	case result := <-chB:
		t.Fatalf("stale FAILED incorrectly resolved request B: %+v", result)
	default:
		// Good — stale FAILED was absorbed.
	}

	// B should now have been started.
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	absorb = mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("pendingStart for B should be set after stale FAILED is absorbed")
	}
	if absorb != 0 {
		t.Fatalf("pendingStartAbsorb = %d after absorbing stale FAILED, want 0", absorb)
	}
	starts = mock.getStartRequests()
	if len(starts) != 2 {
		t.Fatalf("RequestStart calls after stale FAILED = %d, want 2", len(starts))
	}

	// --- Phase 5: Correct STARTED for B ---
	// The real response for B arrives. This must succeed.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x71}

	select {
	case result := <-chB:
		if !result.granted {
			t.Fatal("expected granted=true after correct STARTED for B")
		}
		if result.initiator != 0x71 {
			t.Fatalf("result.initiator = 0x%02X, want 0x71", result.initiator)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for START result after correct STARTED for B")
	}

	// Verify ownership confirmed for B.
	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("gateway should own bus after correct STARTED for B")
	}

	// pendingStart must be cleared.
	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	mux.stateMu.Unlock()
	if hasPending {
		t.Fatal("pendingStart should be nil after correct STARTED for B")
	}
}

// TestTryGrantAndStart_WaitsForStaleStartedAbsorb verifies the key safety
// invariant for gateway regrant: after a pending START is cancelled, the mux
// must NOT issue the next RequestStart until the stale STARTED/FAILED from the
// cancelled request has been handled. Gateway requests frequently reuse the
// same initiator (0x71), so launching B before A's stale STARTED arrives makes
// the response ambiguous and can consume B's valid STARTED as stale.
//
// A stale FAILED is safe to absorb and advance past. A stale STARTED is not:
// on the adapter it means the old request really won arbitration, so the mux
// must force a reconnect/resync rather than launch the next request inline.
func TestTryGrantAndStart_WaitsForStaleStartedAbsorb(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Request A for the gateway and let it reach the adapter.
	chA := mux.arb.requestStart(gatewaySessionID, 0x71)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	starts := mock.getStartRequests()
	if len(starts) != 1 || starts[0] != 0x71 {
		t.Fatalf("RequestStart calls after A = %v, want [0x71]", starts)
	}

	// Cancel A. This sets the absorb barrier because the adapter can still
	// emit STARTED/FAILED for that old RequestStart.
	mux.cancelPendingStart(gatewaySessionID)

	select {
	case result := <-chA:
		if result.granted {
			t.Fatal("cancelled request A should not be granted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for cancel result on A")
	}

	mux.stateMu.Lock()
	absorb := mux.pendingStartAbsorb
	hasPending := mux.pendingStart != nil
	mux.stateMu.Unlock()
	if absorb != 1 {
		t.Fatalf("pendingStartAbsorb after cancel = %d, want 1", absorb)
	}
	if hasPending {
		t.Fatal("pendingStart should be nil after cancel")
	}

	// Queue request B with the same initiator and feed another SYN.
	// The mux must NOT issue RequestStart(B) yet because A's stale response
	// has not been absorbed.
	chB := mux.arb.requestStart(gatewaySessionID, 0x71)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	starts = mock.getStartRequests()
	if len(starts) != 1 {
		t.Fatalf("RequestStart should be blocked by absorb barrier; got %d calls, want 1", len(starts))
	}

	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	absorb = mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if hasPending {
		t.Fatal("pendingStart for B must not be created before stale response is absorbed")
	}
	if absorb != 1 {
		t.Fatalf("pendingStartAbsorb before stale STARTED = %d, want 1", absorb)
	}

	// Deliver stale STARTED for A. The mux must absorb it and keep B blocked:
	// stale STARTED means the adapter really granted the cancelled request, so
	// the mux cannot safely launch B inline. Production code forces a reconnect
	// boundary here; in this unit test there is no real conn, so we just assert
	// that B is NOT launched spuriously.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x71}
	time.Sleep(50 * time.Millisecond)

	starts = mock.getStartRequests()
	if len(starts) != 1 {
		t.Fatalf("RequestStart for B must stay blocked after stale STARTED; got %d calls, want 1", len(starts))
	}

	mux.stateMu.Lock()
	hasPending = mux.pendingStart != nil
	absorb = mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if hasPending {
		t.Fatal("pendingStart for B must remain nil after stale STARTED; reconnect/resync is required")
	}
	if absorb != 0 {
		t.Fatalf("pendingStartAbsorb after stale STARTED = %d, want 0", absorb)
	}
	select {
	case result := <-chB:
		t.Fatalf("request B must not resolve inline after stale STARTED: %+v", result)
	default:
	}
}

// TestConcurrentTryGrantAndStart exercises the race fixed by P1
// #3062924968: two goroutines calling tryGrantAndStart concurrently
// must not both dequeue a request from the arbiter. Before the fix,
// both could pass the pendingStart nil-guard, each call tryGrant(),
// and one would overwrite the other's pendingStart — leaving the
// first waiter without a terminal result.
//
// The test uses a delayedStartTransport to block inside RequestStart
// so we can observe exactly one grant despite concurrent callers.
func TestConcurrentTryGrantAndStart(t *testing.T) {
	delayed := &delayedStartTransport{
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

	// Inject delayed transport (NOT started with readLoop — we drive
	// tryGrantAndStart manually to control concurrency).
	mux.connMu.Lock()
	mux.upstream = delayed
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Create two external sessions with pending START requests.
	clientA, serverA := net.Pipe()
	defer closeOrLog(t, clientA, "clientA")
	idA := mux.AddSession(serverA)
	defer mux.RemoveSession(idA)
	chA := mux.arb.requestStart(idA, 0xA1)

	clientB, serverB := net.Pipe()
	defer closeOrLog(t, clientB, "clientB")
	idB := mux.AddSession(serverB)
	defer mux.RemoveSession(idB)
	chB := mux.arb.requestStart(idB, 0xB2)

	// Launch two concurrent tryGrantAndStart calls.  The gate is
	// closed, so the first one to reach RequestStart will block.
	var wg sync.WaitGroup
	var callCount atomic.Int32
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			mux.tryGrantAndStart()
			callCount.Add(1)
		}()
	}

	// Give both goroutines time to race into tryGrantAndStart.
	time.Sleep(50 * time.Millisecond)

	// Open the gate so RequestStart completes.
	close(delayed.startGate)

	// Wait for both goroutines.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for concurrent tryGrantAndStart calls")
	}

	// Exactly one RequestStart must have been issued.
	starts := delayed.getStartRequests()
	if len(starts) != 1 {
		t.Fatalf("expected exactly 1 RequestStart, got %d: %v", len(starts), starts)
	}

	// pendingStart must be set for exactly one session.
	mux.stateMu.Lock()
	pending := mux.pendingStart
	mux.stateMu.Unlock()
	if pending == nil {
		t.Fatal("pendingStart must be set after tryGrantAndStart")
	}

	// The other request must still be in the arbiter queue (not lost).
	if !mux.arb.hasPending() {
		t.Fatal("second request must still be pending in arbiter")
	}

	// Verify the pending session got dequeued correctly.
	if pending.sessionID != idA && pending.sessionID != idB {
		t.Fatalf("pendingStart.sessionID = %d, want %d or %d", pending.sessionID, idA, idB)
	}

	// Verify the non-dequeued request's channel has not received yet.
	otherCh := chB
	if pending.sessionID == idB {
		otherCh = chA
	}
	select {
	case r := <-otherCh:
		t.Fatalf("other request's channel should not have received yet, got %+v", r)
	default:
		// Good — still waiting.
	}
}

// --- P1 #3063005909: blocking StartArbitration fallback must verify pending ownership ---

// gatedBlockingStartTransport implements StartArbitration with a gate
// that blocks until closed, plus an optional error to return.
type gatedBlockingStartTransport struct {
	readCh    chan byte
	startGate chan struct{} // blocks StartArbitration until closed
	startErr  error         // error to return after gate opens
	mu        sync.Mutex
	calls     []byte
}

func (g *gatedBlockingStartTransport) ReadByte() (byte, error) {
	v, ok := <-g.readCh
	if !ok {
		return 0, io.EOF
	}
	return v, nil
}

func (g *gatedBlockingStartTransport) Write(p []byte) (int, error) { return len(p), nil }
func (g *gatedBlockingStartTransport) Close() error                { return nil }

func (g *gatedBlockingStartTransport) StartArbitration(initiator byte) error {
	<-g.startGate
	g.mu.Lock()
	g.calls = append(g.calls, initiator)
	g.mu.Unlock()
	return g.startErr
}

// TestBlockingFallbackSuccessAfterCancel_NoDoubleSend verifies P1
// #3063005909: when cancelPendingStart clears pendingStart while
// the blocking StartArbitration is in flight, the success path must
// not complete the grant or send on the cap-1 notify channel.
func TestBlockingFallbackSuccessAfterCancel_NoDoubleSend(t *testing.T) {
	mock := &gatedBlockingStartTransport{
		readCh:    make(chan byte, 256),
		startGate: make(chan struct{}),
		startErr:  nil, // success
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
	defer closeOrLog(t, client, "client")
	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Request START.
	ch := mux.arb.requestStart(id, 0x55)

	// PR502-Fix1: tryGrantAndStart returns immediately; the blocking
	// StartArbitration runs in an internal goroutine.
	mux.tryGrantAndStart()

	// Wait for the internal goroutine to enter StartArbitration.
	time.Sleep(30 * time.Millisecond)

	// Cancel the pending request while StartArbitration blocks.
	mux.cancelPendingStart(id)

	// Release StartArbitration — returns success.
	// Without the fix, tryGrantAndStart would call
	// completeArbitrationGrant and double-send on notify.
	close(mock.startGate)

	// Drain the cancel result.
	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected granted=false from cancel path")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for cancel result")
	}

	// Wait for the internal goroutine to complete after gate release.
	time.Sleep(50 * time.Millisecond)

	// No second result must appear.
	select {
	case extra := <-ch:
		t.Fatalf("unexpected second result on notify channel: %+v", extra)
	default:
		// Good — no double-send.
	}

	// Bus ownership must NOT be granted to the cancelled session.
	if mux.arb.isOwner(id) {
		t.Fatal("cancelled session should not own the bus")
	}

	// Codex PR #502 follow-up (C2 pattern): cancelPendingStart on the
	// blocking path NO LONGER bumps blockingArbGen / clears
	// blockingArbActive in-line. Instead it closes m.conn to force the
	// hung goroutine to return; the real gateway reconnect path bumps
	// the gen. In this unit test m.conn == nil, so no gen bump happens
	// and the hung goroutine's late return runs with current gen —
	// which decrements pendingStartAbsorb back to 0. That is the
	// correct end-state once the goroutine has actually released: no
	// stale adapter response can still be in-flight after the hung
	// StartArbitration has returned.
	mux.stateMu.Lock()
	absorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if absorb != 0 {
		t.Fatalf("pendingStartAbsorb = %d, want 0 (current-gen late return decrements absorb — C2 pattern)", absorb)
	}
}

// TestBlockingFallbackErrorAfterCancel_NoDoubleSend verifies P1
// #3063005909: when cancelPendingStart clears pendingStart while
// StartArbitration is blocking, the error path must not send a second
// failure on the cap-1 notify channel.
func TestBlockingFallbackErrorAfterCancel_NoDoubleSend(t *testing.T) {
	mock := &gatedBlockingStartTransport{
		readCh:    make(chan byte, 256),
		startGate: make(chan struct{}),
		startErr:  errors.New("adapter bus fault"),
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
	defer closeOrLog(t, client, "client")
	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Request START.
	ch := mux.arb.requestStart(id, 0x55)

	// PR502-Fix1: tryGrantAndStart now returns immediately; the blocking
	// StartArbitration runs in an internal goroutine.
	mux.tryGrantAndStart()

	time.Sleep(30 * time.Millisecond)

	// Cancel while the internal goroutine is blocking on StartArbitration.
	mux.cancelPendingStart(id)

	// Release — StartArbitration returns error.
	close(mock.startGate)

	// Drain the cancel result.
	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected granted=false from cancel path")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for cancel result")
	}

	// Wait for the internal goroutine to process the error path and
	// update pendingStartAbsorb.
	time.Sleep(50 * time.Millisecond)

	// No second result (the error path must not double-send).
	select {
	case extra := <-ch:
		t.Fatalf("unexpected second result on notify channel: %+v", extra)
	default:
		// Good.
	}

	// Codex PR #502 follow-up (C2 pattern): cancelPendingStart no
	// longer bumps blockingArbGen in-line. With m.conn == nil, no real
	// reconnect happens either, so the error-path cleanup runs at
	// current gen and DOES decrement pendingStartAbsorb. End state 0
	// is correct: the hung goroutine has returned, so no stale adapter
	// response can still be in-flight for the cancelled request.
	mux.stateMu.Lock()
	absorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if absorb != 0 {
		t.Fatalf("pendingStartAbsorb = %d, want 0 (current-gen late return decrements absorb — C2 pattern)", absorb)
	}
}

// TestDrainActiveChOnGrant verifies that stale bytes in activeCh are
// drained when the gateway is granted bus ownership. Without this,
// gateway.Bus reads pre-arbitration bus traffic instead of its own echo
// after StartArbitration returns, causing echo mismatches and scan
// failures in adapter-direct mode.
func TestDrainActiveChOnGrant(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Inject stale bytes into activeCh to simulate pre-arbitration bus
	// traffic that accumulated while the gateway was waiting for SYN.
	staleBytes := []byte{0xAA, 0x10, 0x08, 0x07, 0x04}
	for _, b := range staleBytes {
		select {
		case mux.activeCh <- activeEvent{kind: activeEventByte, b: b}:
		default:
			t.Fatalf("activeCh unexpectedly full injecting stale byte 0x%02X", b)
		}
	}

	// Verify stale bytes are present.
	if len(mux.activeCh) != len(staleBytes) {
		t.Fatalf("activeCh has %d events, want %d before arbitration", len(mux.activeCh), len(staleBytes))
	}

	// Request START for the gateway.
	ch := mux.arb.requestStart(gatewaySessionID, 0x71)

	// Feed SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Verify pendingStart is set.
	mux.stateMu.Lock()
	hasPending := mux.pendingStart != nil
	mux.stateMu.Unlock()
	if !hasPending {
		t.Fatal("expected pendingStart after SYN")
	}

	// Resolve with STARTED — this should drain activeCh before notifying.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x71}
	time.Sleep(50 * time.Millisecond)

	// Verify the grant was received.
	select {
	case result := <-ch:
		if !result.granted {
			t.Fatalf("expected granted=true, got false (err=%v)", result.err)
		}
	default:
		t.Fatal("no result on notify channel after STARTED")
	}

	// Core assertion: activeCh must be empty after the grant.
	// The drain in completeArbitrationGrant should have discarded all
	// stale bytes before the notify was sent.
	remaining := len(mux.activeCh)
	if remaining != 0 {
		t.Fatalf("activeCh has %d stale events after grant, want 0 (drain failed)", remaining)
	}

	// Verify ownership was granted correctly.
	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("gateway should own the bus after STARTED")
	}
}

// TestDrainActiveChOnGrant_ExternalSession verifies that activeCh is
// NOT drained when an external session (not gateway) is granted ownership.
// External sessions use their own net.Conn, not activeCh.
func TestDrainActiveChOnGrant_ExternalSession(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Add an external session.
	client, server := net.Pipe()
	defer closeOrLog(t, client, "client")
	id := mux.AddSession(server)

	// Inject bytes into activeCh.
	staleBytes := []byte{0xAA, 0xBB, 0xCC}
	for _, b := range staleBytes {
		select {
		case mux.activeCh <- activeEvent{kind: activeEventByte, b: b}:
		default:
			t.Fatalf("activeCh full injecting 0x%02X", b)
		}
	}

	// Request START for the external session.
	ch := mux.arb.requestStart(id, 0x50)

	// Feed SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Resolve with STARTED.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x50}
	time.Sleep(50 * time.Millisecond)

	// Verify the grant was received.
	select {
	case result := <-ch:
		if !result.granted {
			t.Fatalf("expected granted=true, got false (err=%v)", result.err)
		}
	default:
		t.Fatal("no result on notify channel after STARTED")
	}

	// activeCh must still contain the staleBytes injected above.
	// Soak policy (runtime-soak directive): activeCh is fed ONLY when
	// gateway owns the bus. The triggering SYN does NOT land in activeCh
	// because at that moment ownership was not yet confirmed for the
	// gateway (the grant is for an external session). Additionally,
	// external-session grants must NOT drain activeCh — external sessions
	// use their own net.Conn path.
	remaining := len(mux.activeCh)
	if remaining != len(staleBytes) {
		t.Fatalf("activeCh has %d events after external grant, want exactly %d (no drain, no idle-SYN accumulation)", remaining, len(staleBytes))
	}
}

// TestPassive_NoGarbageDuringGatewayTransaction verifies that when the
// gateway owns the bus, ALL received bytes are suppressed from the
// passive path — not just echo-matched request bytes. Response bytes
// (target ACK, response LEN, DATA, CRC, final ACK) must also be
// suppressed. Without this, the reconstructor parses orphaned response
// bytes as garbage frames with Source=0x00 and fake protocol IDs.
func TestPassive_NoGarbageDuringGatewayTransaction(t *testing.T) {
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

	// Step 1: Request START for gateway and grant ownership.
	ch := mux.arb.requestStart(gatewaySessionID, 0x71)

	// Feed SYN to trigger tryGrantAndStart.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// Inject STARTED from adapter.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x71}

	select {
	case result := <-ch:
		if !result.granted {
			t.Fatalf("expected granted=true, got false (err=%v)", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for START grant")
	}

	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("gateway should own bus after STARTED")
	}

	// Record how many passive symbols we have so far (the initial SYN).
	passiveMu.Lock()
	preCount := len(passiveSymbols)
	passiveMu.Unlock()

	// Step 2: Simulate gateway's request echoes being received.
	// These are the echoed bytes of a B524 register read request:
	// SRC=0x71, DST=0x08, PB=0xB5, SB=0x24, NN=0x03, DATA..., CRC
	requestEchoes := []byte{0x71, 0x08, 0xB5, 0x24, 0x03, 0x00, 0x09, 0x00, 0x42}
	for _, b := range requestEchoes {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}

	// Step 3: Simulate target's response bytes (these are NOT gateway echoes).
	// ACK from target, then response: LEN=0x02, DATA=0x55 0x66, CRC=0xAB,
	// then final SYN-ACK from gateway.
	responseBytes := []byte{
		protocol.SymbolAck, // target ACK
		0x02,               // response LEN
		0x55,               // response DATA[0]
		0x66,               // response DATA[1]
		0xAB,               // response CRC
		protocol.SymbolAck, // gateway final ACK
	}
	for _, b := range responseBytes {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}

	// Wait for all events to be processed.
	time.Sleep(100 * time.Millisecond)

	// Step 4: Verify that NO bytes leaked to passive during gateway ownership.
	passiveMu.Lock()
	postCount := len(passiveSymbols)
	leaked := passiveSymbols[preCount:]
	leakedCopy := make([]byte, len(leaked))
	copy(leakedCopy, leaked)
	passiveMu.Unlock()

	if postCount != preCount {
		t.Fatalf("passive received %d bytes during gateway ownership, want 0; leaked: %v",
			postCount-preCount, leakedCopy)
	}

	// Step 5: Release ownership (SYN boundary) and verify passive resumes.
	// Wire phase advance is skipped during gateway ownership, so SYN
	// during ownership is always SYNIdle. IdleReleaseGrace (default
	// 200ms) must elapse since busOwned for the idle SYN to release.
	time.Sleep(200 * time.Millisecond)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// After SYN, ownership should be released and passive should get the SYN.
	passiveMu.Lock()
	afterRelease := len(passiveSymbols)
	passiveMu.Unlock()

	if afterRelease <= postCount {
		t.Fatal("passive should resume receiving after ownership release (SYN not delivered)")
	}

	// Feed a third-party byte after release — must appear on passive.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10}
	time.Sleep(50 * time.Millisecond)

	passiveMu.Lock()
	finalCount := len(passiveSymbols)
	lastByte := passiveSymbols[finalCount-1]
	passiveMu.Unlock()

	if lastByte != 0x10 {
		t.Fatalf("passive last byte = 0x%02X, want 0x10 (third-party byte after release)", lastByte)
	}
}

// TestResetErrorPriorityOverByteFlood verifies that reset/error events
// are not lost on activeCh when the channel is saturated with byte
// traffic. Policy (runtime-soak directive): reset/error events must
// take priority over stale byte traffic. handleReset/reconnect drain
// activeCh before enqueuing the error marker, so the consumer always
// sees the reset boundary even under byte flood.
//
// Scenario:
//  1. Grant gateway ownership so activeCh receives bytes.
//  2. Flood activeCh to near-capacity with legitimate byte traffic.
//  3. Trigger handleReset() (simulates in-band RESETTED).
//  4. Assert that after handleReset, the consumer sees the reset
//     error event — NOT lost behind stale bytes.
func TestResetErrorPriorityOverByteFlood(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Grant gateway ownership so activeCh is fed by readLoop.
	ch := mux.arb.requestStart(gatewaySessionID, 0x71)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x71}
	select {
	case r := <-ch:
		if !r.granted {
			t.Fatal("expected grant")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for grant")
	}

	// Flood activeCh directly to near-capacity (avoid goroutine races).
	// This simulates a burst of transaction bytes queued up but not yet
	// consumed by bus.Send. Once the channel fills we exit the outer
	// for-loop; the prior bare `break` only exited the inner select and
	// let the loop spin through the remaining iterations hitting the
	// default branch each time (staticcheck SA4011). Labeled break
	// preserves the author's stated intent ("Channel full — good
	// enough, we have backlog.") without changing test semantics.
	flood := 2000
floodLoop:
	for i := 0; i < flood; i++ {
		select {
		case mux.activeCh <- activeEvent{kind: activeEventByte, b: byte(i % 256)}:
		default:
			// Channel full — good enough, we have backlog.
			break floodLoop
		}
	}

	preLen := len(mux.activeCh)
	if preLen < 100 {
		t.Fatalf("activeCh should have backlog before reset, got %d", preLen)
	}

	// Trigger reset — this should drain activeCh and enqueue the error.
	mux.handleReset()

	// Consume events from activeCh and assert we see the reset error.
	// The drain in handleReset ensures the error is at the front (FIFO).
	foundReset := false
	eventsAfterReset := 0
	deadline := time.After(500 * time.Millisecond)
scan:
	for {
		select {
		case ev := <-mux.activeCh:
			eventsAfterReset++
			if ev.kind == activeEventError {
				if ev.err == nil {
					t.Fatal("reset event has nil error")
				}
				foundReset = true
				break scan
			}
			// Should not see more than a handful of bytes before the reset.
			if eventsAfterReset > 10 {
				t.Fatalf("reset error not found within first 10 events — drained-before-error invariant broken (%d events so far, all bytes)", eventsAfterReset)
			}
		case <-deadline:
			break scan
		}
	}

	if !foundReset {
		t.Fatalf("reset error event lost under byte flood (preLen=%d, scanned=%d)", preLen, eventsAfterReset)
	}
}

// TestSYNTimeoutRelease_BranchesOnOwnerIdentity pins the F-10v2 fix
// (EBUSD-VERIFICATION-2026-05-11-batch3.md): a wirePhaseEventSYNTimeout
// must release ownership IMMEDIATELY for the gateway, but only after
// ExternalSessionSYNGrace for an external session.
//
// Background: the wire-phase machine emits SYN-timeout when SYN appears
// during WaitCmdAck/CollectRequest. For the gateway's tight B524
// protocol that signal is reliable ("txn died"). For external clients
// like ebusd running broadcast scans, the protocol legitimately
// produces multi-second gaps that look identical to SYN-timeout but
// are not transaction death. Without this branch the mux yanked
// ownership from ebusd mid-frame (80 false-positive releases vs 0 for
// the gateway in a 5000-line capture).
func TestSYNTimeoutRelease_BranchesOnOwnerIdentity(t *testing.T) {
	t.Run("gateway_owner_releases_immediately", func(t *testing.T) {
		mux, _, _, cleanup := newP3TestMux(t)
		defer cleanup()
		mux.cfg.ExternalSessionSYNGrace = 2 * time.Second

		// Grant the bus to the gateway and pretend a request was just
		// dispatched (busOwned = now).
		mux.stateMu.Lock()
		mux.arb.confirmOwnership(gatewaySessionID, 0x31)
		mux.busOwned = time.Now()
		mux.lastWireActivity = mux.busOwned
		mux.gatewayTxnActive = true
		_, _, _ = mux.onSYNLocked(wirePhaseEventSYNTimeout, gatewaySessionID, true, time.Now())
		_, _, owned := mux.arb.owner()
		mux.stateMu.Unlock()

		if owned {
			t.Fatalf("gateway-owned SYN-timeout: ownership must release immediately, still owned")
		}
	})

	t.Run("external_owner_held_below_grace", func(t *testing.T) {
		mux, _, _, cleanup := newP3TestMux(t)
		defer cleanup()
		mux.cfg.ExternalSessionSYNGrace = 2 * time.Second

		const extSessionID = uint64(42)
		mux.stateMu.Lock()
		mux.arb.confirmOwnership(extSessionID, 0x31)
		mux.busOwned = time.Now() // just acquired → elapsed ~0
		mux.lastWireActivity = mux.busOwned
		_, _, _ = mux.onSYNLocked(wirePhaseEventSYNTimeout, extSessionID, true, time.Now())
		ownerID, _, owned := mux.arb.owner()
		mux.stateMu.Unlock()

		if !owned {
			t.Fatalf("external owner SYN-timeout: ownership must be held until grace expires (got owned=false at elapsed≈0)")
		}
		if ownerID != extSessionID {
			t.Fatalf("external owner SYN-timeout: ownerID changed to %d (want %d)", ownerID, extSessionID)
		}
	})

	t.Run("external_owner_released_after_grace", func(t *testing.T) {
		mux, _, _, cleanup := newP3TestMux(t)
		defer cleanup()
		// Use a small grace so the test isn't slow.
		mux.cfg.ExternalSessionSYNGrace = 30 * time.Millisecond

		const extSessionID = uint64(43)
		mux.stateMu.Lock()
		mux.arb.confirmOwnership(extSessionID, 0x31)
		// Pretend the bus was acquired 100ms ago — well past the
		// 30ms grace.
		mux.busOwned = time.Now().Add(-100 * time.Millisecond)
		mux.lastWireActivity = mux.busOwned // no wire activity since grant
		_, _, _ = mux.onSYNLocked(wirePhaseEventSYNTimeout, extSessionID, true, time.Now())
		_, _, owned := mux.arb.owner()
		mux.stateMu.Unlock()

		if owned {
			t.Fatalf("external owner SYN-timeout past grace: ownership must release, still owned")
		}
	})
}

// TestSessionRemoteAddrSideIndex verifies the lock-free RemoteAddr
// index stays in sync with AddSession/RemoveSession and that
// sessionRemoteAddrOrUnknown returns the recorded value (or
// "unknown" after removal).
func TestSessionRemoteAddrSideIndex(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	clientA, serverA := net.Pipe()
	defer closeOrLog(t, clientA, "clientA")
	idA := mux.AddSession(serverA)
	if idA == 0 {
		t.Fatalf("AddSession returned 0")
	}
	addrA := mux.sessionRemoteAddrOrUnknown(idA)
	if addrA == "unknown" || addrA == "" {
		t.Fatalf("expected non-unknown RemoteAddr for active session, got %q", addrA)
	}

	mux.RemoveSession(idA)
	if got := mux.sessionRemoteAddrOrUnknown(idA); got != "unknown" {
		t.Errorf("after RemoveSession: sessionRemoteAddrOrUnknown(%d) = %q, want %q", idA, got, "unknown")
	}
	if got := mux.sessionRemoteAddrOrUnknown(99999); got != "unknown" {
		t.Errorf("never-seen session: sessionRemoteAddrOrUnknown(99999) = %q, want %q", got, "unknown")
	}
}

// TestLatencyHistogramReportLoop_EmitsLine verifies the periodic
// histogram log loop emits the expected log line shape. We don't
// inspect content beyond the marker prefix — the bucket-counter
// rendering is covered by TestRecordLatencyBuckets_CumulativeSemantics
// in session_test.go.
func TestLatencyHistogramReportLoop_EmitsLine(t *testing.T) {
	mux, _, _, cleanup := newP3TestMux(t)
	defer cleanup()

	var logBuf bytes.Buffer
	origLogger := mux.logger
	mux.logger = log.New(&logBuf, "", 0)
	defer func() { mux.logger = origLogger }()

	mux.wg.Add(1)
	mux.cfg.LatencyHistogramReportInterval = 25 * time.Millisecond
	go mux.latencyHistogramReportLoop()

	// Pump a sample so the histogram has non-trivial content.
	recordLatencyBuckets(500)
	recordLatencyBuckets(50_000)

	// Give the ticker at least 2 cycles to fire.
	time.Sleep(80 * time.Millisecond)

	if !strings.Contains(logBuf.String(), "session frame latency histogram") {
		t.Fatalf("expected histogram report log line, got:\n%s", logBuf.String())
	}
}

// TestExternalSessionRelease_MeasuresGapNotGrant covers Codex P1 + P2
// on PR #621: long-running external transactions must not be torn
// down based on grant age alone. The release decision must measure
// the inter-byte idle gap (m.lastWireActivity), not the time since
// the original grant (m.busOwned).
func TestExternalSessionRelease_MeasuresGapNotGrant(t *testing.T) {
	t.Run("SYNTimeout_long_grant_recent_activity_holds", func(t *testing.T) {
		mux, _, _, cleanup := newP3TestMux(t)
		defer cleanup()
		mux.cfg.ExternalSessionSYNGrace = 50 * time.Millisecond

		const extSessionID = uint64(7)
		mux.stateMu.Lock()
		mux.arb.confirmOwnership(extSessionID, 0x31)
		// Grant is 5s old (well past grace) but wire activity is fresh
		// (10ms ago) — the protocol is alive, must NOT release.
		now := time.Now()
		mux.busOwned = now.Add(-5 * time.Second)
		mux.lastWireActivity = now.Add(-10 * time.Millisecond)
		_, _, _ = mux.onSYNLocked(wirePhaseEventSYNTimeout, extSessionID, true, now)
		_, _, owned := mux.arb.owner()
		mux.stateMu.Unlock()

		if !owned {
			t.Fatalf("SYNTimeout with fresh wire activity (10ms ago) must hold ownership despite old grant (5s)")
		}
	})

	t.Run("SYNTimeout_long_grant_stale_activity_releases", func(t *testing.T) {
		mux, _, _, cleanup := newP3TestMux(t)
		defer cleanup()
		mux.cfg.ExternalSessionSYNGrace = 50 * time.Millisecond

		const extSessionID = uint64(8)
		mux.stateMu.Lock()
		mux.arb.confirmOwnership(extSessionID, 0x31)
		// Both grant AND activity are old → idle gap exceeds grace → release.
		now := time.Now()
		mux.busOwned = now.Add(-5 * time.Second)
		mux.lastWireActivity = now.Add(-200 * time.Millisecond)
		_, _, _ = mux.onSYNLocked(wirePhaseEventSYNTimeout, extSessionID, true, now)
		_, _, owned := mux.arb.owner()
		mux.stateMu.Unlock()

		if owned {
			t.Fatalf("SYNTimeout with stale wire activity (200ms ≥ 50ms grace) must release")
		}
	})

	t.Run("SYNIdle_external_owner_uses_gap_grace_not_idle_release", func(t *testing.T) {
		mux, _, _, cleanup := newP3TestMux(t)
		defer cleanup()
		mux.cfg.ExternalSessionSYNGrace = 500 * time.Millisecond
		mux.cfg.IdleReleaseGrace = 50 * time.Millisecond // would have fired

		const extSessionID = uint64(9)
		mux.stateMu.Lock()
		mux.arb.confirmOwnership(extSessionID, 0x31)
		now := time.Now()
		// Grant is 200ms old (past IdleReleaseGrace 50ms) but wire
		// activity is fresh (10ms ago). The legacy code would have
		// released here; the fix must hold because the external
		// gap-based grace (500ms) hasn't elapsed.
		mux.busOwned = now.Add(-200 * time.Millisecond)
		mux.lastWireActivity = now.Add(-10 * time.Millisecond)
		_, _, _ = mux.onSYNLocked(wirePhaseEventSYNIdle, extSessionID, true, now)
		_, _, owned := mux.arb.owner()
		mux.stateMu.Unlock()

		if !owned {
			t.Fatalf("SYNIdle on external owner with fresh activity must hold (legacy IdleReleaseGrace bypass closed; Codex P1 PR #621)")
		}
	})

	t.Run("SYNIdle_gateway_owner_still_uses_busOwned", func(t *testing.T) {
		mux, _, _, cleanup := newP3TestMux(t)
		defer cleanup()
		mux.cfg.IdleReleaseGrace = 50 * time.Millisecond

		mux.stateMu.Lock()
		mux.arb.confirmOwnership(gatewaySessionID, 0x31)
		now := time.Now()
		// Gateway uses grant-age, not gap. Grant is 200ms old → release.
		mux.busOwned = now.Add(-200 * time.Millisecond)
		mux.lastWireActivity = now // even with fresh "activity"
		_, _, _ = mux.onSYNLocked(wirePhaseEventSYNIdle, gatewaySessionID, true, now)
		_, _, owned := mux.arb.owner()
		mux.stateMu.Unlock()

		if owned {
			t.Fatalf("SYNIdle on gateway owner must still use busOwned-based IdleReleaseGrace; legacy behavior preserved")
		}
	})
}
