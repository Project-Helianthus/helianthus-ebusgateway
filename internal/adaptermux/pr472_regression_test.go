package adaptermux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// --- Fix 1 regression: handleReset does NOT re-INIT (reset loop prevention) ---

// mockInitTransport is a RawTransport that records Init calls.
// It satisfies both RawTransport and the Init(byte)(byte,error) interface.
type mockInitTransport struct {
	mu             sync.Mutex
	initCalls      []byte // features bytes passed to Init
	returnFeatures byte   // features byte to return from Init (0 = echo request)
	readCh         chan byte
	closed         bool
}

func newMockInitTransport() *mockInitTransport {
	return &mockInitTransport{
		readCh: make(chan byte, 256),
	}
}

func (m *mockInitTransport) Init(features byte) (byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initCalls = append(m.initCalls, features)
	ret := m.returnFeatures
	if ret == 0 {
		ret = features
	}
	return ret, nil
}

func (m *mockInitTransport) ReadByte() (byte, error) {
	b, ok := <-m.readCh
	if !ok {
		return 0, io.EOF
	}
	return b, nil
}

func (m *mockInitTransport) Write(p []byte) (int, error) {
	return len(p), nil
}

func (m *mockInitTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.readCh)
	}
	return nil
}

func (m *mockInitTransport) getInitCalls() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]byte, len(m.initCalls))
	copy(result, m.initCalls)
	return result
}

// TestHandleReset_NoReINIT verifies that handleReset() does NOT call
// Init on the upstream transport. Re-INIT after in-band RESETTED
// causes an infinite reset loop on ENS adapters: Init sends ENHReqInit,
// adapter responds with RESETTED, which triggers another handleReset,
// ad infinitum. INIT is only performed at TCP connection time in
// connect(). upstreamFeatures must be preserved across in-band resets.
func TestHandleReset_NoReINIT(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock := newMockInitTransport()

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

	// Trigger handleReset multiple times to simulate repeated in-band RESETTED.
	mux.handleReset()
	mux.handleReset()
	mux.handleReset()

	// Wait well past the old 200ms re-INIT delay.
	time.Sleep(400 * time.Millisecond)

	// No Init calls should have occurred — re-INIT was removed.
	if calls := mock.getInitCalls(); len(calls) != 0 {
		t.Fatalf("expected 0 Init calls after handleReset, got %d (reset loop not fixed)", len(calls))
	}

	// upstreamFeatures must be preserved (not cleared).
	if f := mux.upstreamFeatures.Load(); f != 0x01 {
		t.Fatalf("upstreamFeatures = 0x%02X after handleReset, want 0x01 (should be preserved)", f)
	}

	cancel()
	mux.wg.Wait()
}

// --- Fix 2 regression: START cancel releases ownership ---

// TestSession_StartCancelReleasesOwnership verifies that sending
// START with SYN (0xAA) not only cancels the pending request but
// also releases bus ownership if the session already owns the bus.
// Without the fix, a session that owns the bus and sends START 0xAA
// would retain ownership, blocking other sessions indefinitely.
func TestSession_StartCancelReleasesOwnership(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Step 1: Request START and grant ownership to this session.
	startReq := transport.EncodeENH(transport.ENHReqStart, 0x31)
	if _, err := client.Write(startReq[:]); err != nil {
		t.Fatalf("write START: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Grant ownership via arbitrator.
	sessionID, initiator, notify, granted := mux.arb.tryGrant()
	if !granted {
		t.Fatal("expected grant from arbitrator")
	}
	if sessionID != id {
		t.Fatalf("granted to session %d, want %d", sessionID, id)
	}

	// Confirm ownership after successful adapter START.
	mux.arb.confirmOwnership(sessionID, initiator)

	// Simulate successful adapter START.
	notify <- startResult{granted: true, initiator: initiator}

	// Drain the STARTED response.
	_ = readENHFrame(t, client, 2*time.Second)

	// Verify session now owns the bus.
	if !mux.arb.isOwner(id) {
		t.Fatal("session should own the bus after grant")
	}

	// Step 2: Send START with SYN to cancel+release.
	cancelReq := transport.EncodeENH(transport.ENHReqStart, protocol.SymbolSyn)
	if _, err := client.Write(cancelReq[:]); err != nil {
		t.Fatalf("write START cancel: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Step 3: Verify ownership is released.
	if mux.arb.isOwner(id) {
		t.Fatal("session should NOT own the bus after START cancel (SYN) — ownership was not released")
	}
}

// --- Fix 4 regression: broadcastResetToSessions carries features ---

// TestBroadcastReset_CarriesUpstreamFeatures verifies that
// broadcastResetToSessions() sends the actual upstream features byte
// to external sessions, not 0x00. Without the fix, sessions receive
// 0x00 on reset boundaries regardless of the negotiated features.
func TestBroadcastReset_CarriesUpstreamFeatures(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	// Set upstream features to 0x03 (non-default value).
	mux.upstreamFeatures.Store(0x03)

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Trigger reset broadcast.
	mux.broadcastResetToSessions()

	// Read the RESETTED frame from the session.
	frame := readENHFrame(t, client, 2*time.Second)

	// Should carry 0x03, not 0x00.
	expected := transport.EncodeENH(transport.ENHResResetted, 0x03)
	if frame != expected {
		t.Fatalf("RESETTED frame = %x, want %x (upstream features 0x03)", frame, expected)
	}

	// Verify it is NOT the old 0x00 behavior.
	old := transport.EncodeENH(transport.ENHResResetted, 0x00)
	if frame == old {
		t.Fatal("RESETTED broadcast still sends 0x00 — fix not applied")
	}
}

// TestBroadcastReset_ZeroFeaturesPassesThrough verifies that when
// upstreamFeatures is 0 (e.g. ENS transport), the broadcast sends
// 0x00 (which is correct — no features negotiated).
func TestBroadcastReset_ZeroFeaturesPassesThrough(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	// Force upstream features to 0.
	mux.upstreamFeatures.Store(0)

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	mux.broadcastResetToSessions()

	frame := readENHFrame(t, client, 2*time.Second)
	expected := transport.EncodeENH(transport.ENHResResetted, 0x00)
	if frame != expected {
		t.Fatalf("RESETTED frame = %x, want %x (zero features)", frame, expected)
	}
}

// TestBroadcastReset_MultipleSessionsAllGetFeatures verifies that
// all connected sessions receive the upstream features on reset.
func TestBroadcastReset_MultipleSessionsAllGetFeatures(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	mux.upstreamFeatures.Store(0x05)

	const numSessions = 3
	type sessConn struct {
		client net.Conn
		id     uint64
	}
	sessions := make([]sessConn, numSessions)

	for i := 0; i < numSessions; i++ {
		client, server := net.Pipe()
		id := mux.AddSession(server)
		sessions[i] = sessConn{client: client, id: id}
	}
	defer func() {
		for _, sc := range sessions {
			mux.RemoveSession(sc.id)
			sc.client.Close()
		}
	}()

	mux.broadcastResetToSessions()

	expected := transport.EncodeENH(transport.ENHResResetted, 0x05)
	for i, sc := range sessions {
		frame := readENHFrame(t, sc.client, 2*time.Second)
		if frame != expected {
			t.Fatalf("session %d: RESETTED frame = %x, want %x", i, frame, expected)
		}
	}
}

// --- Fix 1+4 combined: handleReset triggers broadcast with features ---

// TestHandleReset_BroadcastCarriesFeatures verifies the end-to-end
// path: handleReset() calls broadcastResetToSessions() which sends
// upstreamFeatures. This catches regressions in both the handleReset
// path and the broadcast features fix working together.
func TestHandleReset_BroadcastCarriesFeatures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock := newMockInitTransport()

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
	mux.upstreamFeatures.Store(0x07)

	// Add a session.
	client, server := net.Pipe()
	defer client.Close()
	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Trigger handleReset.
	mux.handleReset()

	// Session should receive RESETTED with features 0x07.
	frame := readENHFrame(t, client, 2*time.Second)
	expected := transport.EncodeENH(transport.ENHResResetted, 0x07)
	if frame != expected {
		t.Fatalf("RESETTED after handleReset = %x, want %x (features 0x07)", frame, expected)
	}

	// No re-INIT goroutine to wait for — handleReset no longer spawns one.
	cancel()
	mux.wg.Wait()
}

// --- Fix 2: START cancel without prior ownership is harmless ---

// TestSession_StartCancelWithoutOwnershipIsNoOp verifies that sending
// START cancel (SYN) when the session does NOT own the bus is harmless
// (no panic, no state corruption).
func TestSession_StartCancelWithoutOwnershipIsNoOp(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Session does not own the bus. Send START cancel — should be a no-op.
	cancelReq := transport.EncodeENH(transport.ENHReqStart, protocol.SymbolSyn)
	if _, err := client.Write(cancelReq[:]); err != nil {
		t.Fatalf("write START cancel: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// No ownership should exist.
	if mux.arb.isOwner(id) {
		t.Fatal("session should not own bus after cancel-only (no prior ownership)")
	}

	// Mux should still be functional — verify by requesting a new START.
	startReq := transport.EncodeENH(transport.ENHReqStart, 0x42)
	if _, err := client.Write(startReq[:]); err != nil {
		t.Fatalf("write START after cancel: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if !mux.arb.hasPending() {
		t.Fatal("expected pending START after post-cancel request")
	}
}

// --- Fix 4: concurrent safety of broadcastResetToSessions ---

// TestBroadcastReset_ConcurrentFeatureUpdate verifies that reading
// upstreamFeatures in broadcastResetToSessions is safe under
// concurrent atomic updates (race detector validation).
func TestBroadcastReset_ConcurrentFeatureUpdate(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	mux.upstreamFeatures.Store(0x01)

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Read responses in background to prevent send buffer overflow.
	var received atomic.Int32
	go func() {
		buf := make([]byte, 2)
		for {
			_, err := io.ReadFull(client, buf)
			if err != nil {
				return
			}
			received.Add(1)
		}
	}()

	// Concurrently update features while broadcasting.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			mux.upstreamFeatures.Store(uint32(i % 256))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			mux.broadcastResetToSessions()
		}
	}()
	wg.Wait()

	// Give reader time to drain.
	time.Sleep(100 * time.Millisecond)

	if received.Load() == 0 {
		t.Fatal("expected at least one broadcast to be received")
	}
}

// --- Codex P2 #3060135587: SEND error classification (host vs bus) ---

// TestSession_SendErrorFromDoSend_HostErrors verifies that when doSend
// returns a host-side error (ownership lost between pre-check and
// sendLoop, or adapter disconnected), handleSend delivers ErrorHost
// instead of ErrorEBUS.
func TestSession_SendErrorFromDoSend_NotConnected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build a mux with injected nil transport to trigger errNotConnected.
	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
	})
	mux.ctx, mux.cancel = ctx, cancel

	// Start the sendLoop goroutine (normally done by Start).
	mux.wg.Add(1)
	go mux.sendLoop()

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	if id == 0 {
		t.Fatal("AddSession returned 0 unexpectedly")
	}
	defer mux.RemoveSession(id)

	// Grant ownership to this session so it passes the pre-check.
	ch := mux.arb.requestStart(id, 0x31)
	_, initiator, notify, granted := mux.arb.tryGrant()
	if !granted {
		t.Fatal("expected grant")
	}
	mux.arb.confirmOwnership(id, initiator)
	notify <- startResult{granted: true, initiator: 0x31}
	<-ch // drain the result

	// upstream is nil (never connected) -- doSend will return errNotConnected.
	// Send a SEND command.
	sendReq := transport.EncodeENH(transport.ENHReqSend, 0x42)
	if _, err := client.Write(sendReq[:]); err != nil {
		t.Fatalf("write SEND: %v", err)
	}

	// Read the error response.
	frame := readENHFrame(t, client, 2*time.Second)

	// Must be ErrorHost, not ErrorEBUS.
	expectedHost := transport.EncodeENH(transport.ENHResErrorHost, 0x00)
	expectedBus := transport.EncodeENH(transport.ENHResErrorEBUS, 0x00)

	if frame == expectedBus {
		t.Fatal("SEND with nil transport returned ErrorEBUS — should be ErrorHost")
	}
	if frame != expectedHost {
		t.Fatalf("SEND error frame = %x, want %x (ErrorHost)", frame, expectedHost)
	}
}

// --- Codex P2 #3060135596: AddSession rejects after mux shutdown ---

// TestAddSession_RejectsAfterShutdown verifies that AddSession returns 0
// and closes the connection when the mux context is cancelled.
func TestAddSession_RejectsAfterShutdown(t *testing.T) {
	mux, cancel, cleanup := newTestMux(t)
	defer cleanup()

	// Cancel the mux context to simulate shutdown.
	cancel()
	time.Sleep(50 * time.Millisecond)

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	if id != 0 {
		t.Fatalf("AddSession returned %d after shutdown, want 0", id)
	}

	// The server-side conn should be closed by AddSession.
	// Verify by attempting to write — should fail.
	_, err := server.Write([]byte{0x42})
	if err == nil {
		t.Fatal("expected write to closed conn to fail, but it succeeded")
	}

	// No sessions should be registered.
	mux.sessionsMu.Lock()
	count := len(mux.sessions)
	mux.sessionsMu.Unlock()
	if count != 0 {
		t.Fatalf("expected 0 sessions after shutdown rejection, got %d", count)
	}
}

// TestAddSession_AcceptsBeforeShutdown verifies normal operation —
// AddSession returns a non-zero ID when the mux is running.
func TestAddSession_AcceptsBeforeShutdown(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	if id == 0 {
		t.Fatal("AddSession returned 0 on a running mux")
	}
	defer mux.RemoveSession(id)

	// Session should be registered.
	mux.sessionsMu.Lock()
	_, ok := mux.sessions[id]
	mux.sessionsMu.Unlock()
	if !ok {
		t.Fatalf("session %d not found in sessions map", id)
	}
}

// --- Codex P1 #3060199707: ownership not set until START succeeds ---

// TestOwnership_NotSetUntilStartSucceeds verifies that tryGrant does
// NOT set hasOwner/currentOwner. Ownership is only confirmed after
// confirmOwnership is called (simulating adapter START success).
// Without the fix, a client could SEND before the adapter confirms.
func TestOwnership_NotSetUntilStartSucceeds(t *testing.T) {
	arb := newArbitrator()

	arb.requestStart(1, 0x31)

	// tryGrant selects the winner but must NOT set ownership.
	sessionID, initiator, _, granted := arb.tryGrant()
	if !granted {
		t.Fatal("expected grant")
	}
	if sessionID != 1 {
		t.Fatalf("sessionID = %d, want 1", sessionID)
	}

	// Ownership must NOT be set yet.
	if arb.isOwner(1) {
		t.Fatal("isOwner returned true after tryGrant — ownership must not be set until confirmOwnership")
	}
	_, _, owned := arb.owner()
	if owned {
		t.Fatal("owner() reports owned=true after tryGrant — must be false until confirmOwnership")
	}

	// Confirm ownership (simulating adapter START success).
	arb.confirmOwnership(sessionID, initiator)

	// Now ownership must be set.
	if !arb.isOwner(1) {
		t.Fatal("isOwner returned false after confirmOwnership")
	}
	ownerID, ownerInit, owned := arb.owner()
	if !owned {
		t.Fatal("owner() reports owned=false after confirmOwnership")
	}
	if ownerID != 1 {
		t.Fatalf("owner sessionID = %d, want 1", ownerID)
	}
	if ownerInit != 0x31 {
		t.Fatalf("owner initiator = 0x%02x, want 0x31", ownerInit)
	}
}

// TestOwnership_FailurePathNeverSetsOwnership verifies that when
// adapter START fails, ownership is never set — no need for
// releaseOwnership cleanup.
func TestOwnership_FailurePathNeverSetsOwnership(t *testing.T) {
	arb := newArbitrator()

	ch := arb.requestStart(1, 0x31)

	_, _, notify, granted := arb.tryGrant()
	if !granted {
		t.Fatal("expected grant")
	}

	// Simulate failure — do NOT call confirmOwnership.
	notify <- startResult{granted: false, initiator: 0x31, err: errors.New("adapter refused")}

	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("should not be granted after failure")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// Bus must be free for a new grant.
	if arb.isOwner(1) {
		t.Fatal("ownership should never have been set")
	}

	// A second request should be immediately grantable.
	arb.requestStart(2, 0x42)
	sid, _, _, g := arb.tryGrant()
	if !g {
		t.Fatal("bus should be free for a new grant after failure")
	}
	if sid != 2 {
		t.Fatalf("second grant sessionID = %d, want 2", sid)
	}
}

// --- Codex P1 #3060199712: reset not dropped under buffer pressure ---

// TestPassiveTransport_ResetNotDroppedUnderPressure verifies that when
// the passive transport buffer is full, a reset event is still delivered
// (blocks until consumed). Without the fix, the reset boundary could be
// dropped and pre/post-reset streams would merge.
func TestPassiveTransport_ResetNotDroppedUnderPressure(t *testing.T) {
	// Tiny buffer to force pressure.
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 2),
		done:   make(chan struct{}),
	}
	defer pt.Close()

	// Fill buffer completely with symbols.
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x01})
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x02})

	// Buffer is now full. Deliver a reset — it must NOT be dropped.
	// The reset uses a blocking send, so we need a consumer goroutine.
	resetDelivered := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(resetDelivered)
	}()

	// Consume events. The reset must appear in the stream.
	var gotReset bool
	timeout := time.After(2 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case ev := <-pt.events:
			if ev.Kind == transport.StreamEventReset {
				gotReset = true
			}
		case <-timeout:
			t.Fatalf("timed out waiting for event %d", i)
		}
	}

	// Wait for deliver goroutine to finish.
	select {
	case <-resetDelivered:
	case <-time.After(2 * time.Second):
		t.Fatal("reset deliver goroutine did not finish")
	}

	if !gotReset {
		t.Fatal("reset event was dropped under buffer pressure — stream boundary lost")
	}
}

// TestPassiveTransport_ResetNonBlockingOnFullBuffer_PR472 verifies AM52:
// delivering a reset to a full buffer does NOT block -- the reset is
// dropped to prevent readLoop stalls. This supersedes the original
// blocking invariant from Codex P1 #3060199712.
func TestPassiveTransport_ResetNonBlockingOnFullBuffer_PR472(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 1),
		done:   make(chan struct{}),
	}
	defer pt.Close()

	// Fill the single-slot buffer.
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0xFF})

	// Deliver reset in background -- must not block (AM52).
	delivered := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(delivered)
	}()

	select {
	case <-delivered:
		// Good -- non-blocking (AM52).
	case <-time.After(500 * time.Millisecond):
		t.Fatal("deliver(Reset) blocked on full buffer -- AM52 violation")
	}

	// The original symbol is still in the buffer.
	ev := <-pt.events
	if ev.Kind != transport.StreamEventByte || ev.Byte != 0xFF {
		t.Fatalf("expected symbol 0xFF, got %+v", ev)
	}
}

// TestPassiveTransport_ConnectedDisconnectedNonBlocking verifies AM52:
// PassiveEventConnected and PassiveEventDisconnected (which map to
// StreamEventReset) use non-blocking delivery when the buffer is full.
func TestPassiveTransport_ConnectedDisconnectedNonBlocking(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 1),
		done:   make(chan struct{}),
	}
	defer pt.Close()

	// Fill buffer.
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0xAA})

	// Deliver Connected (maps to reset) in background -- must not block.
	delivered := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventConnected})
		close(delivered)
	}()

	select {
	case <-delivered:
		// Good -- non-blocking (AM52).
	case <-time.After(500 * time.Millisecond):
		t.Fatal("connected event blocked on full buffer -- AM52 violation")
	}

	// Original symbol is still in buffer.
	ev := <-pt.events
	if ev.Kind != transport.StreamEventByte || ev.Byte != 0xAA {
		t.Fatalf("expected symbol 0xAA, got %+v", ev)
	}
}

// --- Codex P2 #3060199716: adapter write failures classified as host errors ---

// TestDoSend_AdapterWriteFailure_IsHostError verifies that when the
// adapter write fails, the error wraps errAdapterWrite (a sentinel) and
// handleSend classifies it as ErrorHost, not ErrorEBUS.
func TestDoSend_AdapterWriteFailure_IsHostError(t *testing.T) {
	// Verify sentinel wrapping.
	innerErr := fmt.Errorf("connection reset by peer")
	wrapped := fmt.Errorf("%w: %v", errAdapterWrite, innerErr)
	if !errors.Is(wrapped, errAdapterWrite) {
		t.Fatal("wrapped error does not match errAdapterWrite sentinel")
	}
}

// TestSession_AdapterWriteFailure_ReturnsErrorHost is an end-to-end
// test verifying that when the adapter transport's Write fails, the
// session receives ENHResErrorHost (not ENHResErrorEBUS).
func TestSession_AdapterWriteFailure_ReturnsErrorHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a failing transport: reads block, writes always fail.
	failTr := &failWriteTransport{
		readCh: make(chan byte, 256),
	}

	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
	})
	mux.ctx, mux.cancel = ctx, cancel

	// Inject the failing transport.
	mux.connMu.Lock()
	mux.upstream = failTr
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Start sendLoop.
	mux.wg.Add(1)
	go mux.sendLoop()

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	if id == 0 {
		t.Fatal("AddSession returned 0")
	}
	defer mux.RemoveSession(id)

	// Grant ownership to the session.
	ch := mux.arb.requestStart(id, 0x31)
	_, initiator, notify, granted := mux.arb.tryGrant()
	if !granted {
		t.Fatal("expected grant")
	}
	mux.arb.confirmOwnership(id, initiator)
	notify <- startResult{granted: true, initiator: 0x31}
	<-ch

	// Send a SEND command — Write will fail.
	sendReq := transport.EncodeENH(transport.ENHReqSend, 0x42)
	if _, err := client.Write(sendReq[:]); err != nil {
		t.Fatalf("write SEND: %v", err)
	}

	// Read the error response.
	frame := readENHFrame(t, client, 2*time.Second)

	expectedHost := transport.EncodeENH(transport.ENHResErrorHost, 0x00)
	expectedBus := transport.EncodeENH(transport.ENHResErrorEBUS, 0x00)

	if frame == expectedBus {
		t.Fatal("adapter write failure returned ErrorEBUS — should be ErrorHost (Codex P2 #3060199716)")
	}
	if frame != expectedHost {
		t.Fatalf("error frame = %x, want %x (ErrorHost)", frame, expectedHost)
	}
}

// failWriteTransport is a RawTransport where Write always fails.
type failWriteTransport struct {
	readCh chan byte
}

func (f *failWriteTransport) ReadByte() (byte, error) {
	b, ok := <-f.readCh
	if !ok {
		return 0, io.EOF
	}
	return b, nil
}

func (f *failWriteTransport) Write(p []byte) (int, error) {
	return 0, errors.New("simulated adapter write failure")
}

func (f *failWriteTransport) Close() error {
	return nil
}

// --- Regression fix 5: handleReset preserves INFO cache ---

// TestHandleReset_PreservesInfoCache verifies that in-band RESETTED does
// NOT clear the INFO cache. Stable INFO entries (version, hardware ID)
// do not change on soft reset. The cache is only cleared on TCP
// disconnect (in reconnect), not on in-band RESETTED.
func TestHandleReset_PreservesInfoCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock := newMockInitTransport()

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

	// Populate INFO cache with test data (simulating connect-time population).
	mux.infoCacheMu.Lock()
	mux.infoCache[transport.AdapterInfoVersion] = []byte{0x23, 0x01}
	mux.infoCache[transport.AdapterInfoHardwareID] = []byte{0x10, 0x20, 0x30}
	mux.infoCache[transport.AdapterInfoHardwareConf] = []byte{0x05}
	mux.infoCache[transport.AdapterInfoTemperature] = []byte{0x1C}
	mux.infoCache[transport.AdapterInfoWiFiRSSI] = []byte{0xE0}
	mux.infoCacheMu.Unlock()

	// Trigger handleReset (simulating in-band RESETTED).
	mux.handleReset()

	// Verify CachedInfo still returns data for all IDs.
	ids := []transport.AdapterInfoID{
		transport.AdapterInfoVersion,
		transport.AdapterInfoHardwareID,
		transport.AdapterInfoHardwareConf,
		transport.AdapterInfoTemperature,
		transport.AdapterInfoWiFiRSSI,
	}
	for _, id := range ids {
		data, err := mux.CachedInfo(id)
		if err != nil {
			t.Fatalf("CachedInfo(0x%02X) after handleReset: %v — cache should be preserved", byte(id), err)
		}
		if len(data) == 0 {
			t.Fatalf("CachedInfo(0x%02X) returned empty data after handleReset", byte(id))
		}
	}

	// Verify specific values are intact.
	ver, _ := mux.CachedInfo(transport.AdapterInfoVersion)
	if len(ver) != 2 || ver[0] != 0x23 || ver[1] != 0x01 {
		t.Fatalf("version cache corrupted after handleReset: %v, want [0x23 0x01]", ver)
	}

	hwid, _ := mux.CachedInfo(transport.AdapterInfoHardwareID)
	if len(hwid) != 3 || hwid[0] != 0x10 {
		t.Fatalf("hardware ID cache corrupted after handleReset: %v", hwid)
	}

	cancel()
	mux.wg.Wait()
}

// TestHandleReset_DoesNotReINIT verifies that handleReset does NOT send
// an INIT to the adapter. Re-INIT after in-band RESETTED causes an
// infinite reset loop on ENS adapters (INIT -> RESETTED -> INIT -> ...).
// This is the proxy divergence: the standalone proxy did re-INIT, but
// the mux intentionally skips it.
func TestHandleReset_DoesNotReINIT(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mock := newMockInitTransport()

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

	// Trigger handleReset.
	mux.handleReset()

	// Wait well past the old 200ms re-INIT delay.
	time.Sleep(300 * time.Millisecond)

	// Verify NO Init was called.
	if calls := mock.getInitCalls(); len(calls) != 0 {
		t.Fatalf("handleReset sent %d Init calls — should send none (proxy divergence: no re-INIT)", len(calls))
	}

	// Verify the mux continues operating normally: upstreamFeatures preserved,
	// and we can still broadcast resets to sessions.
	if f := mux.upstreamFeatures.Load(); f != 0x01 {
		t.Fatalf("upstreamFeatures = 0x%02X after handleReset, want 0x01", f)
	}

	// Add a session and verify broadcast still works after handleReset.
	client, server := net.Pipe()
	defer client.Close()
	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	mux.broadcastResetToSessions()
	frame := readENHFrame(t, client, 2*time.Second)
	expected := transport.EncodeENH(transport.ENHResResetted, 0x01)
	if frame != expected {
		t.Fatalf("broadcast after handleReset = %x, want %x — mux not operating normally", frame, expected)
	}

	cancel()
	mux.wg.Wait()
}
