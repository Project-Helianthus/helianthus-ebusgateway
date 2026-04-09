package adaptermux

import (
	"testing"
	"time"
)

func TestArbitrator_GatewayPriority(t *testing.T) {
	arb := newArbitrator()

	// Both gateway and external request START.
	gwCh := arb.requestStart(gatewaySessionID, 0x71)
	extCh := arb.requestStart(1, 0x31)

	// Grant: gateway should win.
	sessionID, initiator, granted := arb.tryGrant()
	if !granted {
		t.Fatal("expected grant")
	}
	if sessionID != gatewaySessionID {
		t.Fatalf("sessionID = %d, want gateway (0)", sessionID)
	}
	if initiator != 0x71 {
		t.Fatalf("initiator = 0x%02x, want 0x71", initiator)
	}

	// Gateway should receive granted.
	select {
	case result := <-gwCh:
		if !result.granted {
			t.Fatal("gateway should be granted")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for gateway grant")
	}

	// External should still be pending (bus is owned by gateway).
	_, _, granted2 := arb.tryGrant()
	if granted2 {
		t.Fatal("should not grant while bus is owned")
	}

	// Release gateway ownership.
	arb.releaseOwnership(gatewaySessionID)

	// Now external should win.
	sessionID, initiator, granted3 := arb.tryGrant()
	if !granted3 {
		t.Fatal("expected grant for external")
	}
	if sessionID != 1 {
		t.Fatalf("sessionID = %d, want 1", sessionID)
	}
	if initiator != 0x31 {
		t.Fatalf("initiator = 0x%02x, want 0x31", initiator)
	}

	select {
	case result := <-extCh:
		if !result.granted {
			t.Fatal("external should be granted")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for external grant")
	}
}

func TestArbitrator_ExternalFIFO(t *testing.T) {
	arb := newArbitrator()

	// Two external sessions request START.
	ch1 := arb.requestStart(1, 0x31)
	ch2 := arb.requestStart(2, 0x32)

	// First external (FIFO) should win.
	sessionID, _, granted := arb.tryGrant()
	if !granted {
		t.Fatal("expected grant")
	}
	if sessionID != 1 {
		t.Fatalf("first grant: sessionID = %d, want 1", sessionID)
	}

	select {
	case result := <-ch1:
		if !result.granted {
			t.Fatal("session 1 should be granted")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	arb.releaseOwnership(1)

	// Second external should be next.
	sessionID, _, granted = arb.tryGrant()
	if !granted {
		t.Fatal("expected second grant")
	}
	if sessionID != 2 {
		t.Fatalf("second grant: sessionID = %d, want 2", sessionID)
	}

	select {
	case result := <-ch2:
		if !result.granted {
			t.Fatal("session 2 should be granted")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
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
	_, _, granted := arb.tryGrant()
	if granted {
		t.Fatal("should not grant after cancel")
	}
}

func TestArbitrator_RemoveSession(t *testing.T) {
	arb := newArbitrator()

	arb.requestStart(1, 0x31)

	// Grant ownership.
	_, _, granted := arb.tryGrant()
	if !granted {
		t.Fatal("expected grant")
	}

	// Remove session while owning bus.
	arb.removeSession(1)

	// Bus should be released.
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
	arb.tryGrant()

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

	// Second should be granted.
	_, initiator, granted := arb.tryGrant()
	if !granted {
		t.Fatal("expected grant")
	}
	if initiator != 0x72 {
		t.Fatalf("initiator = 0x%02x, want 0x72", initiator)
	}

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

	_, _, granted := arb.tryGrant()
	if granted {
		t.Fatal("should not grant with no pending requests")
	}

	if arb.hasPending() {
		t.Fatal("should have no pending requests")
	}
}
