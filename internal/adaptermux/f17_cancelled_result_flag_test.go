package adaptermux

import (
	"testing"
	"time"
)

// TestArbitrator_SameSessionReplace_StartResultCarriesCancelledFlag
// pins F-17 (operator hand-off, batch-9): when a session re-submits a
// START while it still has a pending entry in the arbitrator queue,
// the previous request's notify channel MUST receive a startResult
// with `cancelled = true`. The handleStart goroutine in session.go
// uses that flag to silently suppress the FAILED delivery instead of
// firing `deliverFailed(initiator)` on the wire.
//
// Without `cancelled: true` on the result, the bidder reads a spurious
// `ENHResFailed(initiator)` within ~0.3 ms of its own ReqStart (faster
// than the bus can possibly arbitrate). ebusd interprets that as "you
// lost arbitration to your own initiator byte" and retries within ~50 ms,
// which cancels the just-queued entry, producing another spurious
// FAILED, and so on — positive-feedback loop where no bid ever reaches
// the adapter. This is the pcap-confirmed root cause of "ebusd never
// lands a frame on the bus through the proxy."
//
// Note: this flag is on the startResult VALUE (the channel message),
// distinct from the `cancelled` atomic.Bool on the startRequest STRUCT.
// Both must be set on a same-session cancellation:
//   - struct flag (atomic.Bool): checked by mux's C4/R4 late-STARTED
//     suppression path when the request has been popped to pendingStart
//   - result flag (channel value): checked by session.go's handleStart
//     when the request is still in pendingExternal
func TestArbitrator_SameSessionReplace_StartResultCarriesCancelledFlag(t *testing.T) {
	t.Run("external_session_replace", func(t *testing.T) {
		arb := newArbitrator()
		arb.setPolicy(24 * time.Hour) // disable C3 TTL for this test

		chOld := arb.requestStart(1, 0x31)
		_ = arb.requestStart(1, 0x32) // replaces chOld in pendingExternal

		select {
		case result := <-chOld:
			if result.granted {
				t.Fatal("replaced request must not be granted")
			}
			if !result.cancelled {
				t.Fatalf("startResult.cancelled = %v on same-session-replace; want true (F-17 regression — would produce spurious ENHResFailed and trigger the same-session retry feedback loop)", result.cancelled)
			}
			if result.initiator != 0x31 {
				t.Fatalf("startResult.initiator = 0x%02X, want 0x31 (old request's bid byte)", result.initiator)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("replaced request's notify channel did not fire")
		}
	})

	t.Run("gateway_session_replace", func(t *testing.T) {
		arb := newArbitrator()
		arb.setPolicy(24 * time.Hour)

		chOld := arb.requestStart(gatewaySessionID, 0x71)
		_ = arb.requestStart(gatewaySessionID, 0x72) // replaces pendingGateway

		select {
		case result := <-chOld:
			if result.granted {
				t.Fatal("replaced gateway request must not be granted")
			}
			if !result.cancelled {
				t.Fatalf("startResult.cancelled = %v on gateway same-session-replace; want true (F-17 regression, symmetric path)", result.cancelled)
			}
			if result.initiator != 0x71 {
				t.Fatalf("startResult.initiator = 0x%02X, want 0x71", result.initiator)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("replaced gateway request's notify channel did not fire")
		}
	})
}

// TestArbitrator_SameSessionReplace_StructFlagAlsoSet pins that the
// startRequest.cancelled atomic.Bool flag (the one the C4/R4 mux path
// reads) is also set when the same-session replace happens. F-17's
// fix is to set BOTH flags; verify the previously-correct struct flag
// is not regressed.
func TestArbitrator_SameSessionReplace_StructFlagAlsoSet(t *testing.T) {
	arb := newArbitrator()
	arb.setPolicy(24 * time.Hour)

	_ = arb.requestStart(1, 0x31)
	arb.mu.Lock()
	oldReq := arb.pendingExternal[0]
	arb.mu.Unlock()

	if oldReq.cancelled.Load() {
		t.Fatal("setup: cancelled flag set prematurely")
	}

	// Drain the channel so the replace's send doesn't block on a full
	// buffer.
	go func() { <-oldReq.notify }()

	_ = arb.requestStart(1, 0x32)

	if !oldReq.cancelled.Load() {
		t.Fatal("startRequest.cancelled (struct flag) not set on same-session-replace (would break the C4/R4 in-flight-cancel suppression)")
	}
}
