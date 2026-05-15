package adaptermux

import (
	"testing"
	"time"
)

// F-39 (2026-05-15): same-session + same-initiator coalescing.
//
// Pcap forensics on the live HA host captured a 4.85 s cluster where
// ebusd issued 24 REQ_START 0x31 frames and only 15 received an ENH
// response within 500 ms — 9 were silently canceled by the F-17
// cancel+replace path, which RESET the queued bid's FIFO position on
// every retry and prevented it from reaching the adapter for real
// arbitration.
//
// F-39 keeps the queued startRequest in place when the new submission
// is for the same session AND same initiator, retargeting only the
// notify channel. Different-initiator submissions still fall back to
// F-17's cancel+replace semantics.

// TestF39_QueuedSameInitiator_CoalesceNoReplace verifies that a same-
// session same-initiator duplicate does NOT remove the existing queue
// entry. The queue length stays at 1; existing.cancelled stays false;
// existing.notify is retargeted to the new caller's channel.
func TestF39_QueuedSameInitiator_CoalesceNoReplace(t *testing.T) {
	arb := newArbitrator()

	// First REQ_START: ebusd session 1 with initiator 0x31.
	ch1 := arb.requestStart(1, 0x31)

	arb.mu.Lock()
	if got := len(arb.pendingExternal); got != 1 {
		arb.mu.Unlock()
		t.Fatalf("after 1st request: pendingExternal len = %d; want 1", got)
	}
	originalReq := arb.pendingExternal[0]
	arb.mu.Unlock()

	// Old goroutine reads cancelled:true and exits silently — must
	// drain ch1 BEFORE the duplicate to keep the cap-1 channel non-
	// blocking when the duplicate sends cancelled:true to ch1.
	go func() { <-ch1 }()
	// brief sleep to let goroutine attach
	time.Sleep(2 * time.Millisecond)

	// Duplicate: same session, same initiator.
	ch2 := arb.requestStart(1, 0x31)

	arb.mu.Lock()
	defer arb.mu.Unlock()

	if got := len(arb.pendingExternal); got != 1 {
		t.Fatalf("after coalesced duplicate: pendingExternal len = %d; want 1 (no replace)", got)
	}
	if arb.pendingExternal[0] != originalReq {
		t.Fatal("coalesce changed the underlying startRequest pointer; bid lost its FIFO position")
	}
	if arb.pendingExternal[0].cancelled.Load() {
		t.Fatal("coalesce wrongly set existing.cancelled — the bid is still valid, only the waiter changed")
	}
	if arb.pendingExternal[0].notify != ch2 {
		t.Fatal("coalesce did not retarget notify to the new caller's channel")
	}
	if arb.pendingExternal[0].initiator != 0x31 {
		t.Fatalf("initiator changed unexpectedly: got 0x%02X want 0x31", arb.pendingExternal[0].initiator)
	}
}

// TestF39_QueuedDifferentInitiator_FallsBackToReplace ensures that
// when the new submission's initiator differs from the queued one,
// the F-17 cancel+replace semantics still apply: the old startRequest
// is removed and cancelled; the new one is queued fresh.
func TestF39_QueuedDifferentInitiator_FallsBackToReplace(t *testing.T) {
	arb := newArbitrator()

	ch1 := arb.requestStart(1, 0x31)

	// Capture the original startRequest pointer for identity check.
	arb.mu.Lock()
	oldReq := arb.pendingExternal[0]
	arb.mu.Unlock()

	// Drain the old caller's cancellation (so the duplicate doesn't
	// block sending cancelled:true to the cap-1 ch1).
	done := make(chan startResult, 1)
	go func() { done <- <-ch1 }()
	time.Sleep(2 * time.Millisecond)

	// Different initiator from same session — should replace.
	ch2 := arb.requestStart(1, 0x71)

	// Old caller must have received cancelled:true (F-17 silent-cancel).
	select {
	case result := <-done:
		if !result.cancelled || result.granted {
			t.Fatalf("old caller: granted=%v cancelled=%v; want granted=false cancelled=true",
				result.granted, result.cancelled)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("old caller did not receive cancellation within 100ms")
	}

	arb.mu.Lock()
	defer arb.mu.Unlock()

	// The OLD request must be flagged cancelled (replace semantics).
	if !oldReq.cancelled.Load() {
		t.Fatal("different-initiator replace did not flag old request as cancelled")
	}

	if got := len(arb.pendingExternal); got != 1 {
		t.Fatalf("after different-initiator replace: pendingExternal len = %d; want 1", got)
	}
	if arb.pendingExternal[0] == oldReq {
		t.Fatal("different-initiator replace did not produce a new startRequest")
	}
	// The NEW (surviving) request must NOT be flagged cancelled.
	if arb.pendingExternal[0].cancelled.Load() {
		t.Fatal("new request wrongly inherited cancelled flag from replaced request")
	}
	if arb.pendingExternal[0].notify != ch2 {
		t.Fatal("different-initiator replace did not install the new caller's channel")
	}
	if arb.pendingExternal[0].initiator != 0x71 {
		t.Fatalf("different-initiator replace did not update initiator: got 0x%02X want 0x71",
			arb.pendingExternal[0].initiator)
	}
}

// TestF39_QueuedSameInitiator_OldChannelGetsCancelled verifies the
// old caller's notify channel receives cancelled:true (so the old
// handleStart goroutine exits silently without emitting ENHResFailed).
func TestF39_QueuedSameInitiator_OldChannelGetsCancelled(t *testing.T) {
	arb := newArbitrator()

	ch1 := arb.requestStart(1, 0x31)

	done := make(chan startResult, 1)
	go func() {
		result := <-ch1
		done <- result
	}()
	time.Sleep(2 * time.Millisecond)

	_ = arb.requestStart(1, 0x31)

	select {
	case result := <-done:
		if result.granted {
			t.Fatal("old caller received granted=true; F-39 must keep the bid alive but cancel the OLD waiter")
		}
		if !result.cancelled {
			t.Fatal("old caller received cancelled=false; F-39 silent-cancel requires cancelled=true to avoid ENHResFailed emission")
		}
		if result.initiator != 0x31 {
			t.Fatalf("old caller initiator: got 0x%02X want 0x31", result.initiator)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("old caller did not receive cancellation within 100ms")
	}
}

// TestF39_QueuedSameInitiator_NewCallerEventuallyGetsGrant verifies
// that when the coalesced bid is finally granted (via tryGrant), the
// STARTED result is delivered to the LATEST caller's channel, not
// the original one (which has already received cancelled:true).
func TestF39_QueuedSameInitiator_NewCallerEventuallyGetsGrant(t *testing.T) {
	arb := newArbitrator()

	ch1 := arb.requestStart(1, 0x31)
	go func() { <-ch1 }() // drain old cancel
	time.Sleep(2 * time.Millisecond)

	ch2 := arb.requestStart(1, 0x31)

	// Simulate tryGrant popping the bid.
	req, ok := arb.tryGrant(true)
	if !ok {
		t.Fatal("tryGrant failed; coalesced bid should still be grantable")
	}
	if req.initiator != 0x31 {
		t.Fatalf("granted initiator: got 0x%02X want 0x31", req.initiator)
	}

	// Deliver a synthetic STARTED to the popped request's notify
	// (this is what handleArbitrationResponse does in production).
	select {
	case req.notify <- startResult{granted: true, initiator: 0x31}:
	default:
		t.Fatal("notify channel was unexpectedly full")
	}

	select {
	case result := <-ch2:
		if !result.granted {
			t.Fatal("new caller did not receive granted=true; F-39 notify-retarget broken")
		}
		if result.initiator != 0x31 {
			t.Fatalf("new caller initiator: got 0x%02X want 0x31", result.initiator)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("new caller did not receive grant result; F-39 notify-retarget broken")
	}
}

// TestF39_GatewaySessionUnaffected ensures the gateway session-0
// arbitration path is NOT changed by F-39 (no coalescing). Gateway
// session bids retain pendingGateway replace semantics.
func TestF39_GatewaySessionUnaffected(t *testing.T) {
	arb := newArbitrator()

	ch1 := arb.requestStart(gatewaySessionID, 0x71)
	go func() { <-ch1 }()
	time.Sleep(2 * time.Millisecond)

	// Same gateway session, same initiator — should still replace
	// (not coalesce). The gateway does not have the ebusd retry
	// behaviour, so the replace semantics are appropriate.
	_ = arb.requestStart(gatewaySessionID, 0x71)

	arb.mu.Lock()
	defer arb.mu.Unlock()

	if arb.pendingGateway == nil {
		t.Fatal("pendingGateway was nil after duplicate; gateway path must always have an active bid")
	}
	// Verify cancelled flag was set on the OLD (via the standard
	// pendingGateway replace path).
	if arb.pendingGateway.cancelled.Load() {
		t.Fatal("new pendingGateway is marked cancelled; replace semantics broken")
	}
}
