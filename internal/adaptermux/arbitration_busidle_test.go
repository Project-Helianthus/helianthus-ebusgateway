package adaptermux

import (
	"testing"
)

// TestRequestStart_ReplaceMarksCancelledFlag pins proxy-bug C4 (R4):
// a session that re-submits a START while a previous request is
// still in the arbitrator's queue replaces the prior entry AND sets
// its cancelled flag, so the mux's delivery path can convert a late
// in-flight STARTED into a FAILED instead of granting bus to an
// abandoned request.
func TestRequestStart_ReplaceMarksCancelledFlag(t *testing.T) {
	arb := newArbitrator()

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
