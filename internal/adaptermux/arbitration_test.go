package adaptermux

import (
	"testing"
	"time"
)

// tryGrantLegacy simulates one idle SYN for ownership lifecycle tests.
func tryGrantLegacy(arb *arbitrator) (uint64, byte, chan startResult, bool) {
	req, ok := arb.tryGrant(true)
	if !ok {
		return 0, 0, nil, false
	}
	return req.sessionID, req.initiator, req.notify, true
}

func TestArbitrator_CancelStart(t *testing.T) {
	arb := newArbitrator()

	ch := arb.requestStart(1, 0x31)
	cancelled := arb.cancelStart(1)
	if !cancelled {
		t.Fatal("expected cancel to succeed")
	}

	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("cancelled request should not be granted")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// Nothing to grant now.
	_, _, _, granted := tryGrantLegacy(arb)
	if granted {
		t.Fatal("should not grant after cancel")
	}
}

func TestArbitrator_RemoveSession(t *testing.T) {
	arb := newArbitrator()

	arb.requestStart(1, 0x31)

	// Grant and confirm ownership.
	_, initiator, notify, granted := tryGrantLegacy(arb)
	if !granted {
		t.Fatal("expected grant")
	}
	arb.confirmOwnership(1, initiator)
	notify <- startResult{granted: true}

	// Remove session while owning bus.
	arb.removeSession(1)

	if arb.isOwner(1) {
		t.Fatal("session should no longer own bus")
	}
}

func TestArbitrator_FailAllPending(t *testing.T) {
	arb := newArbitrator()

	gwCh := arb.requestStart(gatewaySessionID, 0x71)
	extCh := arb.requestStart(1, 0x31)

	arb.failAllPending(nil)

	select {
	case result := <-gwCh:
		if result.granted {
			t.Fatal("gateway should not be granted after failAll")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	select {
	case result := <-extCh:
		if result.granted {
			t.Fatal("external should not be granted after failAll")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestArbitrator_ForceRelease(t *testing.T) {
	arb := newArbitrator()

	arb.requestStart(1, 0x31)
	_, initiator, notify, _ := tryGrantLegacy(arb)
	arb.confirmOwnership(1, initiator)
	notify <- startResult{granted: true}

	if !arb.isOwner(1) {
		t.Fatal("session 1 should own bus")
	}

	arb.forceRelease()

	if arb.isOwner(1) {
		t.Fatal("bus should be released after force")
	}
}

func TestArbitrator_DuplicateGatewayRequest(t *testing.T) {
	arb := newArbitrator()

	// First gateway request.
	ch1 := arb.requestStart(gatewaySessionID, 0x71)

	// Second gateway request replaces the first.
	ch2 := arb.requestStart(gatewaySessionID, 0x72)

	// First should be cancelled.
	select {
	case result := <-ch1:
		if result.granted {
			t.Fatal("first gateway request should be cancelled")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// Second should be grantable.
	_, initiator, notify, granted := tryGrantLegacy(arb)
	if !granted {
		t.Fatal("expected grant")
	}
	if initiator != 0x72 {
		t.Fatalf("initiator = 0x%02x, want 0x72", initiator)
	}

	notify <- startResult{granted: true}

	select {
	case result := <-ch2:
		if !result.granted {
			t.Fatal("second gateway request should be granted")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestArbitrator_NoPendingNothingToGrant(t *testing.T) {
	arb := newArbitrator()

	_, _, _, granted := tryGrantLegacy(arb)
	if granted {
		t.Fatal("should not grant with no pending requests")
	}

	if arb.hasPending() {
		t.Fatal("should have no pending requests")
	}
}

func TestArbitrator_GrantFailureReleasesOwnership(t *testing.T) {
	arb := newArbitrator()

	ch := arb.requestStart(1, 0x31)

	_, _, notify, granted := tryGrantLegacy(arb)
	if !granted {
		t.Fatal("expected grant")
	}

	// Simulate adapter START failure — caller notifies with granted=false.
	notify <- startResult{granted: false, initiator: 0x31}

	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("should not be granted after START failure")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

// --- Fix 1: startResult carries initiator ---

func TestArbitrator_StartResultCarriesInitiator(t *testing.T) {
	arb := newArbitrator()

	ch := arb.requestStart(1, 0x31)

	sessionID, initiator, notify, granted := tryGrantLegacy(arb)
	if !granted {
		t.Fatal("expected grant")
	}
	if initiator != 0x31 {
		t.Fatalf("tryGrant initiator = 0x%02x, want 0x31", initiator)
	}

	arb.confirmOwnership(sessionID, initiator)
	notify <- startResult{granted: true, initiator: initiator}

	select {
	case result := <-ch:
		if !result.granted {
			t.Fatal("expected granted=true")
		}
		if result.initiator != 0x31 {
			t.Fatalf("result.initiator = 0x%02x, want 0x31", result.initiator)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestArbitrator_CancelCarriesInitiator(t *testing.T) {
	arb := newArbitrator()

	ch := arb.requestStart(1, 0x31)
	arb.cancelStart(1)

	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("cancelled request should not be granted")
		}
		if result.initiator != 0x31 {
			t.Fatalf("cancel result.initiator = 0x%02x, want 0x31", result.initiator)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestArbitrator_FailAllPendingCarriesInitiator(t *testing.T) {
	arb := newArbitrator()

	gwCh := arb.requestStart(gatewaySessionID, 0x71)
	extCh := arb.requestStart(1, 0x31)

	arb.failAllPending(nil)

	select {
	case result := <-gwCh:
		if result.initiator != 0x71 {
			t.Fatalf("gateway result.initiator = 0x%02x, want 0x71", result.initiator)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	select {
	case result := <-extCh:
		if result.initiator != 0x31 {
			t.Fatalf("external result.initiator = 0x%02x, want 0x31", result.initiator)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestArbitrator_DuplicateReplaceCarriesInitiator(t *testing.T) {
	arb := newArbitrator()

	// First gateway request.
	ch1 := arb.requestStart(gatewaySessionID, 0x71)

	// Second gateway request replaces the first.
	_ = arb.requestStart(gatewaySessionID, 0x72)

	// First should be cancelled with its own initiator.
	select {
	case result := <-ch1:
		if result.granted {
			t.Fatal("first request should be cancelled")
		}
		if result.initiator != 0x71 {
			t.Fatalf("replaced result.initiator = 0x%02x, want 0x71", result.initiator)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}
