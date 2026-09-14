package main

// M6_DISPATCHER_BRIDGE — capability-signal 8-state truth table per AD18 +
// helianthus-docs-ebus B503.md §12.5. Each row is its own test.
//
// The capability signal is computed by mcp/vaillant_b503.go's
// VaillantB503AvailabilityCtx (probe-based: it issues a 00 01
// errors.current via the dispatcher and classifies the outcome). These
// tests exercise that probe under controlled mock-bus state and assert
// the resulting B503Availability matches the truth-table row.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
)

// --- Row 1: cold-boot, no successful dispatch yet → UNKNOWN ---
//
// On cold-boot the Manager is Idle, the dispatcher has not been invoked,
// and the probe (errors.current) has not run. The probe inside
// VaillantB503AvailabilityCtx triggers a dispatch — if the bus is
// configured to return an error on cold-boot, capability surfaces as
// UNKNOWN (per §11 / §12.5 row 1). We model "no successful dispatch yet"
// by making the bus refuse the probe with a generic error.
func TestM6TruthTable_Row1_ColdBoot_Unknown(t *testing.T) {
	srv, mgr, bus := newTruthTableHarness(t)
	bus.setErr([2]byte{0x00, 0x01}, errors.New("cold-boot: no canned response"))

	got := srv.VaillantB503AvailabilityCtx(context.Background())
	if got != mcp.AvailabilityUnknown {
		t.Fatalf("row 1 cold-boot: capability = %s; want UNKNOWN", got)
	}
	if mgr.IsOwned() {
		t.Fatalf("row 1: session was unexpectedly claimed during probe")
	}
}

// --- Row 2: post-first-success → AVAILABLE ---

func TestM6TruthTable_Row2_PostFirstSuccess_Available(t *testing.T) {
	srv, _, bus := newTruthTableHarness(t)
	bus.setResp([2]byte{0x00, 0x01}, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	got := srv.VaillantB503AvailabilityCtx(context.Background())
	if got != mcp.AvailabilityAvailable {
		t.Fatalf("row 2: capability = %s; want AVAILABLE", got)
	}
}

// --- Row 3: disconnect during ACTIVE → in-flight TRANSPORT_DOWN, capability UNKNOWN ---

func TestM6TruthTable_Row3_DisconnectDuringActive_UnknownWithCleanup(t *testing.T) {
	srv, mgr, bus := newTruthTableHarness(t)
	if _, err := mgr.EnableOperation(context.Background(), defaultVaillantTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: mgr.TransportKey()}
	}); err != nil {
		t.Fatalf("Enable = %v", err)
	}
	bus.setErr([2]byte{0x00, 0x03}, errors.New("ebus: transport closed"))
	dispatcher := newRawFrameDispatcher(bus, gatewaySource, &sync.Mutex{}, mgr, time.Second)
	dispatcher.disconnectIfCurrent = func(b503session.TransportKey) { mgr.OnTransportDisconnect() }
	_, err := mgr.ReadOperation(context.Background(), defaultVaillantTarget, func(ctx context.Context, target byte) b503session.DispatchOutcome {
		return mcp.InvokeB503Operation(ctx, dispatcher, target, []byte{0x00, 0x03})
	})
	if err == nil || !errors.Is(err, errRawFrameTransportDown) {
		t.Fatalf("row 3 in-flight error = %v, want TRANSPORT_DOWN", err)
	}
	if mgr.IsOwned() {
		t.Fatal("row 3 disconnect retained caller ownership")
	}
	beforeCleanup, ok := mgr.CleanupObligation()
	if !ok || beforeCleanup.Target != defaultVaillantTarget {
		t.Fatalf("row 3 cleanup = %+v, present=%v", beforeCleanup, ok)
	}
	if got := srv.VaillantB503AvailabilityCtx(context.Background()); got != mcp.AvailabilityUnknown {
		t.Fatalf("row 3 capability = %s, want UNKNOWN while cleanup remains", got)
	}
	beforeBusCalls := bus.callCount()
	var recoveryWrites int
	mgr.SetCleanupDispatcher(func(context.Context, byte) b503session.DispatchOutcome {
		recoveryWrites++
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK}
	})
	mgr.OnEpochAdvance(context.Background(), 99)
	mgr.OnEpochAdvance(context.Background(), 99)
	mgr.OnEpochAdvance(context.Background(), 100)
	if recoveryWrites != 0 {
		t.Fatalf("row 3 reconnect recovery writes = %d, want zero", recoveryWrites)
	}
	afterCleanup, ok := mgr.CleanupObligation()
	if !ok || afterCleanup.GatewayCleanupAttemptID != beforeCleanup.GatewayCleanupAttemptID || afterCleanup.Target != beforeCleanup.Target || afterCleanup.TransportEpoch != beforeCleanup.TransportEpoch || afterCleanup.OriginEmitted != beforeCleanup.OriginEmitted || afterCleanup.OriginNative != beforeCleanup.OriginNative || afterCleanup.OriginErr != beforeCleanup.OriginErr || afterCleanup.LastAttemptEpoch != beforeCleanup.LastAttemptEpoch || afterCleanup.LastEmitted != beforeCleanup.LastEmitted || afterCleanup.LastNative != beforeCleanup.LastNative || afterCleanup.LastErr != beforeCleanup.LastErr {
		t.Fatalf("row 3 reconnect mutated cleanup: before=%+v after=%+v present=%v", beforeCleanup, afterCleanup, ok)
	}
	if got := mgr.TransportKey().TransportEpoch; got != 100 {
		t.Fatalf("row 3 transport epoch = %d, want 100", got)
	}
	if got := srv.VaillantB503AvailabilityCtx(context.Background()); got != mcp.AvailabilityUnknown {
		t.Fatalf("row 3 post-reconnect capability = %s, want UNKNOWN", got)
	}
	if got := bus.callCount(); got != beforeBusCalls {
		t.Fatalf("row 3 post-reconnect probe bus calls = %d, want unchanged %d", got, beforeBusCalls)
	}
}

// --- Row 4: reconnect, before first post-reconnect dispatch → UNKNOWN ---

func TestM6TruthTable_Row4_PostReconnectBeforeDispatch_Unknown(t *testing.T) {
	srv, mgr, bus := newTruthTableHarness(t)
	// Successful dispatch first to prove sticky-AVAILABLE would be a bug.
	bus.setResp([2]byte{0x00, 0x01}, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	got := srv.VaillantB503AvailabilityCtx(context.Background())
	if got != mcp.AvailabilityAvailable {
		t.Fatalf("setup row 4: pre-disconnect cap = %s; want AVAILABLE", got)
	}

	// Trigger reconnect with epoch advance. After OnEpochAdvance with no
	// owner, the Manager updates the transport epoch but does NOT publish
	// any cached "available" state — the resolver must re-probe to know.
	mgr.OnEpochAdvance(context.Background(), 99)

	// On the post-reconnect probe, configure the bus to return a generic
	// error (meaning "we haven't successfully dispatched yet"). Capability
	// must NOT stick on AVAILABLE. Clear the resp first so the err takes
	// effect (mock prefers err over resp when both are set).
	bus.mu.Lock()
	delete(bus.respByPrefix, string([]byte{0x00, 0x01}))
	bus.mu.Unlock()
	bus.setErr([2]byte{0x00, 0x01}, errors.New("post-reconnect: no canned response yet"))

	got = srv.VaillantB503AvailabilityCtx(context.Background())
	if got == mcp.AvailabilityAvailable {
		t.Fatalf("row 4: capability sticky-AVAILABLE after reconnect; want UNKNOWN (or TRANSPORT_DOWN if surfaced)")
	}
	if got != mcp.AvailabilityUnknown && got != mcp.AvailabilityTransportDown {
		t.Fatalf("row 4: capability = %s; want UNKNOWN or TRANSPORT_DOWN", got)
	}
}

// --- Row 5: reconnect, post-first-success-after-reconnect → AVAILABLE ---

func TestM6TruthTable_Row5_PostReconnectFirstSuccess_Available(t *testing.T) {
	srv, mgr, bus := newTruthTableHarness(t)
	mgr.OnEpochAdvance(context.Background(), 99)
	bus.setResp([2]byte{0x00, 0x01}, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	got := srv.VaillantB503AvailabilityCtx(context.Background())
	if got != mcp.AvailabilityAvailable {
		t.Fatalf("row 5: capability = %s; want AVAILABLE", got)
	}
}

// --- Row 6: timeout/NAK/CRC during dispatch → UPSTREAM_RPC_FAILED to caller; capability stays last-known ---

func TestM6TruthTable_Row6_DispatchError_CapabilityStaysLastKnown(t *testing.T) {
	srv, _, bus := newTruthTableHarness(t)

	// Establish AVAILABLE first.
	bus.setResp([2]byte{0x00, 0x01}, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	got := srv.VaillantB503AvailabilityCtx(context.Background())
	if got != mcp.AvailabilityAvailable {
		t.Fatalf("setup row 6: pre-error cap = %s; want AVAILABLE", got)
	}

	// Now make the bus NAK on a different selector that the caller would
	// invoke (e.g. errors.history). The capability probe re-runs against
	// the 00 01 slot and still sees a successful response — capability
	// MUST NOT regress merely because some unrelated dispatch NAK'd.
	bus.setErr([2]byte{0x01, 0x01}, ebuserrors.ErrNACK)

	got = srv.VaillantB503AvailabilityCtx(context.Background())
	if got != mcp.AvailabilityAvailable {
		t.Fatalf("row 6: capability = %s; want AVAILABLE (last-known); a NAK on an unrelated selector must not poison the probe", got)
	}
}

func TestM6TruthTable_Row6_DisableFailurePublishesUnknownWithCleanup(t *testing.T) {
	srv, mgr, bus := newTruthTableHarness(t)
	key, err := mgr.EnableOperation(context.Background(), defaultVaillantTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: mgr.TransportKey()}
	})
	if err != nil {
		t.Fatalf("enable = %v", err)
	}
	bus.setErr([2]byte{0x00, 0x03}, ebuserrors.ErrNACK)
	dispatcher := newRawFrameDispatcher(bus, gatewaySource, &sync.Mutex{}, mgr, time.Second)
	err = mgr.DisableOperation(context.Background(), key, defaultVaillantTarget, func(ctx context.Context, target byte) b503session.DispatchOutcome {
		return mcp.InvokeB503Operation(ctx, dispatcher, target, []byte{0x00, 0x03})
	})
	if err == nil || !errors.Is(err, errRawFrameUpstreamRPCFailed) {
		t.Fatalf("disable outcome = %v, want UPSTREAM_RPC_FAILED", err)
	}
	if _, ok := mgr.CleanupObligation(); !ok {
		t.Fatal("disable NAK did not retain cleanup")
	}
	if got := srv.VaillantB503AvailabilityCtx(context.Background()); got != mcp.AvailabilityUnknown {
		t.Fatalf("cleanup-bearing row 6 capability = %s, want UNKNOWN", got)
	}
}

// --- Row 7: held-session epoch refresh → UNKNOWN, one triggering dispatch ---

func TestM6TruthTable_Row7_RefreshingUnknownAndTriggerDispatchedOnce(t *testing.T) {
	srv, _, bus := newTruthTableHarness(t)
	refreshStarted := make(chan struct{})
	allowRefresh := make(chan struct{})
	freshTK := b503session.TransportKey{AdapterInstanceID: "test", TransportEpoch: 99}
	mgr2 := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "test", TransportEpoch: 1},
		30*time.Second,
		func(ctx context.Context) (b503session.TransportKey, error) {
			close(refreshStarted)
			<-allowRefresh
			return freshTK, nil
		},
	)
	dispatcher := newRawFrameDispatcher(bus, gatewaySource, &sync.Mutex{}, mgr2, time.Second)
	mcp.RegisterVaillantB503Tools(srv, mcp.VaillantB503Options{
		Dispatcher:     dispatcher,
		SessionManager: mgr2,
		DefaultTarget:  defaultVaillantTarget,
	})
	if _, err := mgr2.EnableOperation(context.Background(), defaultVaillantTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: mgr2.TransportKey()}
	}); err != nil {
		t.Fatalf("Enable = %v", err)
	}
	mgr2.OnEpochAdvance(context.Background(), 99)
	bus.setResp([2]byte{0x00, 0x03}, []byte{0x01, 0x00})
	before := bus.callCount()
	done := make(chan error, 1)
	go func() {
		_, err := mgr2.ReadOperation(context.Background(), defaultVaillantTarget, func(ctx context.Context, target byte) b503session.DispatchOutcome {
			return mcp.InvokeB503Operation(ctx, dispatcher, target, []byte{0x00, 0x03})
		})
		done <- err
	}()
	<-refreshStarted
	if got := srv.VaillantB503AvailabilityCtx(context.Background()); got != mcp.AvailabilityUnknown {
		t.Fatalf("row 7 capability during refresh = %s, want UNKNOWN", got)
	}
	if got := mgr2.StatusSnapshot(); got.State != b503session.Refreshing || !got.Owned {
		t.Fatalf("row 7 session = %+v, want Refreshing owned", got)
	}
	close(allowRefresh)
	if err := <-done; err != nil {
		t.Fatalf("row 7 triggering read = %v", err)
	}
	if calls := bus.callCount() - before; calls != 1 {
		t.Fatalf("row 7 triggering dispatch count = %d, want 1", calls)
	}
}

// --- Row 8: stale-epoch in-flight completion → discarded; capability stays last-known ---

func TestM6TruthTable_Row8_StaleEpochCompletion_Discarded(t *testing.T) {
	disp, bus, mgr := newTestDispatcher(t)

	// Block bus.Send so we can advance the epoch mid-flight.
	gate := make(chan struct{})
	bus.blockUntil = gate
	bus.setResp([2]byte{0x00, 0x01}, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	respCh := make(chan struct {
		out []byte
		err error
	}, 1)
	go func() {
		out, err := disp.Invoke(context.Background(), 0x08, []byte{0x00, 0x01})
		respCh <- struct {
			out []byte
			err error
		}{out, err}
	}()
	time.Sleep(20 * time.Millisecond)

	// Roll the epoch via OnEpochAdvance (no owner held → just bumps
	// transport.TransportEpoch).
	mgr.OnEpochAdvance(context.Background(), 999)
	close(gate)

	select {
	case r := <-respCh:
		if r.err == nil {
			t.Fatalf("row 8: late epoch-N reply succeeded; want errRawFrameStaleEpoch (caller waiter must NOT be satisfied)")
		}
		if !errors.Is(r.err, errRawFrameStaleEpoch) {
			t.Fatalf("row 8: err = %v; want errRawFrameStaleEpoch", r.err)
		}
		if len(r.out) != 0 {
			t.Fatalf("row 8: stale-epoch completion returned non-empty data %x; want nil/empty", r.out)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("row 8: dispatch did not return; deadlock or stale-epoch handling missing")
	}
}

// --- Forbidden states (assertion targets per §12.5) ---

func TestM6TruthTable_NoStickyAvailableAfterTransportLoss(t *testing.T) {
	srv, mgr, bus := newTruthTableHarness(t)
	bus.setResp([2]byte{0x00, 0x01}, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	if got := srv.VaillantB503AvailabilityCtx(context.Background()); got != mcp.AvailabilityAvailable {
		t.Fatalf("setup: %s; want AVAILABLE", got)
	}
	bus.mu.Lock()
	delete(bus.respByPrefix, string([]byte{0x00, 0x01}))
	bus.mu.Unlock()
	bus.setErr([2]byte{0x00, 0x01}, b503session.ErrTransportDown)
	mgr.OnTransportDisconnect()

	got := srv.VaillantB503AvailabilityCtx(context.Background())
	if got == mcp.AvailabilityAvailable {
		t.Fatalf("forbidden: sticky AVAILABLE after transport loss; got %s", got)
	}
}

// --- helper: harness producing (server, manager, bus) wired through prod dispatcher ---

func newTruthTableHarness(t *testing.T) (*mcp.Server, *b503session.Manager, *b503DispatcherMockBus) {
	t.Helper()
	srv, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("mcp.NewServer = %v", err)
	}
	bus := newB503DispatcherMockBus()
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "test", TransportEpoch: 1},
		30*time.Second,
		func(ctx context.Context) (b503session.TransportKey, error) {
			return b503session.TransportKey{}, b503session.ErrTransportDown
		},
	)
	disp := newRawFrameDispatcher(bus, gatewaySource, &sync.Mutex{}, mgr, time.Second)
	mcp.RegisterVaillantB503Tools(srv, mcp.VaillantB503Options{
		Dispatcher:     disp,
		SessionManager: mgr,
		DefaultTarget:  defaultVaillantTarget,
	})
	return srv, mgr, bus
}
