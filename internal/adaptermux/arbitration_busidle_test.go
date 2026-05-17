package adaptermux

import (
	"errors"
	"testing"
	"time"
)

// TestTryGrant_BusIdle_ExternalPreemptsFairnessWindow pins proxy-bug
// C1 (R1): when both gateway and external are pending AND the wire
// has been idle for at least one SYN interval, tryGrant must grant
// external FIRST without advancing the fairness counter — on an
// uncontended bus, the fairness rotation is wasted wall-clock that
// ebusd's local arbitration deadline (~50 ms) counts against the
// gateway, even when no contention exists.
func TestTryGrant_BusIdle_ExternalPreemptsFairnessWindow(t *testing.T) {
	arb := newArbitrator()
	arb.setPolicy(24 * time.Hour, 0, 0) // disable C3 TTL for this test

	// Both classes pending — same shape as the legacy fairness test.
	_ = arb.requestStart(gatewaySessionID, 0x71)
	_ = arb.requestStart(1, 0x31)

	priorCounter := arb.fairnessCounter

	req, granted := arb.tryGrant(true /* busIdle */)
	if !granted {
		t.Fatal("expected grant on idle bus with external pending")
	}
	if req.sessionID != 1 {
		t.Fatalf("idle-bus fast path: granted session %d, want 1 (external preempts fairness)", req.sessionID)
	}
	if req.initiator != 0x31 {
		t.Fatalf("idle-bus fast path: granted initiator 0x%02X, want 0x31", req.initiator)
	}
	if got := arb.fairnessCounter; got != priorCounter {
		t.Fatalf("fairness counter advanced (%d → %d) on idle-bus fast path; must NOT — fast path is not a contention rotation",
			priorCounter, got)
	}
}

// TestTryGrant_BusIdle_GatewayOnly_StillGranted ensures the bus-idle
// fast path doesn't break the standalone case: when only gateway is
// pending, the fast path is a no-op and the regular gateway-priority
// branch grants normally.
func TestTryGrant_BusIdle_GatewayOnly_StillGranted(t *testing.T) {
	arb := newArbitrator()
	arb.setPolicy(24 * time.Hour, 0, 0)

	_ = arb.requestStart(gatewaySessionID, 0x71)

	req, granted := arb.tryGrant(true)
	if !granted {
		t.Fatal("expected gateway grant on idle bus when only gateway is pending")
	}
	if req.sessionID != gatewaySessionID {
		t.Fatalf("granted session %d, want gateway", req.sessionID)
	}
}

// TestTryGrant_NotIdle_GatewayPriorityPreserved ensures the C1 fast
// path is GATED on busIdle — when the bus is actively contended
// (busIdle=false), gateway-priority + DefaultFairnessRatio rotation apply
// unchanged. This is the regression guard for the legacy semantic.
func TestTryGrant_NotIdle_GatewayPriorityPreserved(t *testing.T) {
	arb := newArbitrator()
	arb.setPolicy(24 * time.Hour, 0, 0)

	_ = arb.requestStart(gatewaySessionID, 0x71)
	_ = arb.requestStart(1, 0x31)

	req, granted := arb.tryGrant(false)
	if !granted {
		t.Fatal("expected grant")
	}
	if req.sessionID != gatewaySessionID {
		t.Fatalf("contended-bus path: granted session %d, want gateway (priority preserved when bus is not idle)",
			req.sessionID)
	}
}

// TestPendingExternalTTL_StaleEntryRejected pins proxy-bug C3 (R3):
// an external pending START whose enqueuedAt has aged past
// PendingStartTTL is dropped from the queue head and its notify
// channel receives granted=false with errStaleStartRequest, so the
// client can retry cleanly instead of receiving a stale grant.
func TestPendingExternalTTL_StaleEntryRejected(t *testing.T) {
	arb := newArbitrator()
	// Use a fixed clock and a 50 ms TTL.
	base := time.Now()
	clock := base
	arb.nowFn = func() time.Time { return clock }
	arb.setPolicy(50 * time.Millisecond, 0, 0)

	// Stale entry first.
	staleCh := arb.requestStart(1, 0x31)

	// Fresh entry enqueued at clock=60ms so it stays under the 50 ms
	// TTL when we eventually call tryGrant at clock=80ms.
	clock = base.Add(60 * time.Millisecond)
	freshCh := arb.requestStart(2, 0x32)

	clock = base.Add(80 * time.Millisecond) // session 1 now 80ms old (>50ms TTL)
	// session 2 was enqueued at 60ms; at clock=80ms it is 20ms old — fresh.

	req, granted := arb.tryGrant(false)
	if !granted {
		t.Fatal("expected fresh entry to be granted after stale drain")
	}
	if req.sessionID != 2 {
		t.Fatalf("granted session %d, want 2 (fresh) — stale head should have been drained", req.sessionID)
	}

	// session 1's notify must have received a stale rejection.
	select {
	case result := <-staleCh:
		if result.granted {
			t.Fatal("stale entry must not be granted")
		}
		if !errors.Is(result.err, errStaleStartRequest) {
			t.Fatalf("stale rejection error = %v, want errStaleStartRequest", result.err)
		}
	default:
		t.Fatal("stale entry's notify channel did not receive a rejection")
	}

	// session 2's notify must NOT have fired yet — its result fires
	// only after the caller delivers via the grant outcome.
	select {
	case r := <-freshCh:
		t.Fatalf("fresh entry's notify received an unexpected early result: %+v", r)
	default:
	}
}

// TestRequestStart_ReplaceMarksCancelledFlag pins proxy-bug C4 (R4):
// a session that re-submits a START while a previous request is
// still in the arbitrator's queue replaces the prior entry AND sets
// its cancelled flag, so the mux's delivery path can convert a late
// in-flight STARTED into a FAILED instead of granting bus to an
// abandoned request.
func TestRequestStart_ReplaceMarksCancelledFlag(t *testing.T) {
	arb := newArbitrator()
	arb.setPolicy(24 * time.Hour, 0, 0)

	// Reach into pendingExternal to grab the pointer before replace.
	_ = arb.requestStart(1, 0x31)
	arb.mu.Lock()
	if len(arb.pendingExternal) != 1 {
		arb.mu.Unlock()
		t.Fatalf("pendingExternal len = %d, want 1", len(arb.pendingExternal))
	}
	oldReq := arb.pendingExternal[0]
	arb.mu.Unlock()

	if oldReq.cancelled.Load() {
		t.Fatal("cancelled flag set prematurely on first requestStart")
	}

	// Re-submit for the same session — replaces the prior entry AND
	// sets cancelled=true on it.
	_ = arb.requestStart(1, 0x32)

	if !oldReq.cancelled.Load() {
		t.Fatal("cancelled flag was NOT set on the replaced startRequest (proxy-bug C4 regression)")
	}
}

// TestMarkInFlightCancelled exercises arbitrator.markInFlightCancelled,
// the API the mux uses to flag a previously-popped startRequest as
// cancelled when a new requestStart arrives for the same session
// while the prior grant is still in flight at the adapter.
func TestMarkInFlightCancelled(t *testing.T) {
	arb := newArbitrator()

	req := &startRequest{sessionID: 1, initiator: 0x31}
	if prev := arb.markInFlightCancelled(req); prev {
		t.Fatal("first markInFlightCancelled returned prev=true")
	}
	if !req.cancelled.Load() {
		t.Fatal("cancelled flag not set after markInFlightCancelled")
	}
	if prev := arb.markInFlightCancelled(req); !prev {
		t.Fatal("second markInFlightCancelled returned prev=false")
	}
	if prev := arb.markInFlightCancelled(nil); prev {
		t.Fatal("markInFlightCancelled(nil) returned prev=true; want false")
	}
}
