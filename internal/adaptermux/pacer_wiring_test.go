package adaptermux

import (
	"net"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
)

// Phase 3 Step B3.6c: tests for the pacer wiring at the
// adapter-write hot path (mux.doSend's BeforeActiveWrite hook).
// Scope contract: pacer storage lifecycle + BeforeActiveWrite
// call on every gateway-internal and external-session active
// write. NO RecordEcho yet (B3.6d), NO watchdog (B3.6e), NO
// output pacer (B3.6f).

// TestSessionPacer_OffMode_AlwaysNil pins the production-default
// invariant: V8ClassifierMode == Off → SessionPacer returns nil
// regardless of sessionID. No per-session pacer allocation, no
// adapter-write hot path overhead beyond the nil-check.
func TestSessionPacer_OffMode_AlwaysNil(t *testing.T) {
	t.Parallel()
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeOff)
	defer cleanup()

	for _, sid := range []uint64{gatewaySessionID, 1, 42, 12345} {
		if got := mux.SessionPacer(sid); got != nil {
			t.Errorf("SessionPacer(%d) = %v in ModeOff; want nil", sid, got)
		}
	}
}

// TestSessionPacer_ShadowMode_GatewayPreCreated pins that the
// gateway-internal session pacer (sessionID = gatewaySessionID = 0)
// is pre-created in Mux.New when V8ClassifierMode != Off, so the
// first active write doesn't race on lazy-create.
func TestSessionPacer_ShadowMode_GatewayPreCreated(t *testing.T) {
	t.Parallel()
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	pacer := mux.SessionPacer(gatewaySessionID)
	if pacer == nil {
		t.Fatal("SessionPacer(gatewaySessionID) = nil in ModeShadow; want pre-created Pacer")
	}
	// Verify it's a fresh Pacer (default bootstrap state).
	if got := pacer.Lrtt(); got != v8classifier.LrttBootstrapInitial {
		t.Errorf("gateway Pacer.Lrtt() = %v; want LrttBootstrapInitial %v (fresh state)",
			got, v8classifier.LrttBootstrapInitial)
	}
	if pacer.InGraceBootstrap() {
		t.Error("gateway Pacer.InGraceBootstrap() = true; want false (no prior history)")
	}
}

// TestSessionPacer_AddSession_CreatesPacer pins that adding an
// external session in ModeShadow / ModeEnforce pre-creates its
// pacer entry.
func TestSessionPacer_AddSession_CreatesPacer(t *testing.T) {
	t.Parallel()
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	sid := mux.AddSession(serverConn)
	if sid == 0 {
		t.Fatal("AddSession returned 0")
	}
	defer mux.RemoveSession(sid)

	pacer := mux.SessionPacer(sid)
	if pacer == nil {
		t.Fatalf("SessionPacer(%d) = nil after AddSession in ModeShadow; want non-nil", sid)
	}
}

// TestSessionPacer_RemoveSession_DropsEntry pins that RemoveSession
// cleans up the per-session pacer entry AND that the load-only
// SessionPacer accessor returns nil afterward (NO lazy-recreate).
//
// Per Codex round-1 MEDIUM on PR #645: the prior version of this
// test couldn't actually prove deletion — it observed
// SessionPacer's lazy-create returning a fresh Pacer, which is
// indistinguishable from the deletion never having fired. The
// round-1 fix removed lazy-create from SessionPacer; this test
// is the load-only assertion that proves deletion.
func TestSessionPacer_RemoveSession_DropsEntry(t *testing.T) {
	t.Parallel()
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	sid := mux.AddSession(serverConn)
	if sid == 0 {
		t.Fatal("AddSession returned 0")
	}

	pacerBefore := mux.SessionPacer(sid)
	if pacerBefore == nil {
		t.Fatalf("precondition: SessionPacer(%d) nil before Remove", sid)
	}
	// Bump the counter on the pre-Remove pacer so the load-only
	// post-Remove probe would clearly distinguish the OLD pacer
	// from a hypothetical lazy-recreate (if lazy-create ever
	// resurges as a regression). With load-only, the second
	// SessionPacer call MUST return nil — no Pacer to inspect.
	pacerBefore.BeforeActiveWrite(time.Now())
	if got := pacerBefore.BeforeActiveWriteTotal(); got != 1 {
		t.Fatalf("precondition: pacerBefore counter = %d; want 1", got)
	}

	mux.RemoveSession(sid)

	// PRIMARY ASSERTION: SessionPacer is load-only post-Remove.
	// Returns nil — proves the Delete actually fired AND proves
	// no resurrection-on-hot-path race.
	if got := mux.SessionPacer(sid); got != nil {
		t.Errorf("SessionPacer(%d) = %v after Remove; want nil (load-only contract — no lazy-recreate)", sid, got)
	}
}

// TestSessionPacer_DoSend_GatewayInvokesBeforeActiveWrite is the
// LOAD-BEARING test for B3.6c: a gateway-internal active write
// MUST invoke BeforeActiveWrite on the gateway session's pacer.
// We exercise this via grantGateway → active path so doSend fires.
func TestSessionPacer_DoSend_GatewayInvokesBeforeActiveWrite(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	pacer := mux.SessionPacer(gatewaySessionID)
	if pacer == nil {
		t.Fatal("gateway Pacer nil in ModeShadow")
	}
	if got := pacer.BeforeActiveWriteTotal(); got != 0 {
		t.Fatalf("precondition: BeforeActiveWriteTotal = %d; want 0", got)
	}

	// Grant the gateway and do an active write.
	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	req := []byte{0x71, 0x08, 0x07, 0x04, 0x00}
	n, err := at.Write(req)
	if err != nil || n != len(req) {
		t.Fatalf("ActiveTransport.Write returned (%d, %v); want (%d, nil)", n, err, len(req))
	}

	// Poll BeforeActiveWriteTotal until it reaches the byte count
	// (each byte triggers one BeforeActiveWrite via doSend).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pacer.BeforeActiveWriteTotal() >= uint64(len(req)) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := pacer.BeforeActiveWriteTotal(); got != uint64(len(req)) {
		t.Errorf("BeforeActiveWriteTotal() = %d; want %d (one per byte written)", got, len(req))
	}
}

// TestSessionPacer_OffMode_DoSendDoesNotCallPacer pins the
// performance contract: ModeOff means SessionPacer returns nil
// at doSend's hook site, so no pacer overhead per byte.
//
// This is asserted indirectly: in ModeOff, no Pacer is allocated
// for any sessionID, so even if doSend tried to call
// BeforeActiveWrite, the nil-guard prevents the call. We verify
// by attempting a gateway write and confirming no pacer was
// ever created.
func TestSessionPacer_OffMode_DoSendDoesNotCallPacer(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeOff)
	defer cleanup()

	// Sanity: no gateway pacer pre-created in Off.
	if got := mux.SessionPacer(gatewaySessionID); got != nil {
		t.Fatalf("SessionPacer(gateway) = %v in ModeOff; want nil", got)
	}

	// Do the active write.
	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x71, 0x08}); err != nil {
		t.Fatalf("Write err: %v", err)
	}

	// After the write, SessionPacer(gateway) should STILL be nil
	// — no lazy-creation in Off mode.
	if got := mux.SessionPacer(gatewaySessionID); got != nil {
		t.Errorf("SessionPacer(gateway) = %v after write in ModeOff; want nil (no lazy-create)", got)
	}
}
