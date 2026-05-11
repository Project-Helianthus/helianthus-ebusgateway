package adaptermux

import (
	"log"
	"strings"
	"testing"
	"time"
)

// PR #626 review round-1 — angry-tester findings F-1 and F-2.
//
// F-1 (HIGH): the original PR fixed `arbitration.requestStart`'s
// same-session-replace path but missed the in-flight branch.
// `requestStartForSession` calls `markInFlightCancelled` (sets the
// startRequest.cancelled atomic.Bool) when the prior request has
// already been popped into m.pendingStart by tryGrant. The deferred
// channel-value send was supposed to happen in
// handleArbitrationResponse when the STARTED or FAILED arrives — but
// the original code sent `startResult{granted: false, …}` WITHOUT
// `cancelled: true`. Result: session.go's handleStart goroutine fell
// through to deliverFailed(initiator) and put ENHResFailed(0x31) on
// the wire — exactly the failure mode F-17 was filed to fix, just
// on the in-flight branch.
//
// F-2 (MEDIUM): pre-existing latent. arbitrator.cancelStart resolves
// pending SYN-cancel by sending `startResult{granted: false, …}`
// without `cancelled: true`. Race window is short (SYN-cancel arriving
// before tryGrant pops the bid into m.pendingStart) but real on idle
// buses. F-17 makes the inconsistency obvious; AM55 was supposed to
// cover this exact case.

// TestHandleArbitrationResponse_InFlightCancelled_STARTED_SuppressedSilently
// pins F-1's STARTED-half: after markInFlightCancelled flips the
// struct flag, a STARTED arriving for the old (now-cancelled) request
// MUST resolve the old notify channel with `cancelled: true` so
// session.go's handleStart silent-returns instead of emitting
// ENHResFailed on the wire.
func TestHandleArbitrationResponse_InFlightCancelled_STARTED_SuppressedSilently(t *testing.T) {
	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		SYNInterval:     time.Hour,
		PendingStartTTL: 24 * time.Hour,
	})

	// Stage 1: simulate request popped into m.pendingStart.
	chA := mux.arb.requestStart(1, 0x31)
	mux.stateMu.Lock()
	if len(mux.arb.pendingExternal) == 0 {
		mux.stateMu.Unlock()
		t.Fatal("setup: pendingExternal empty after requestStart")
	}
	reqA := mux.arb.pendingExternal[0]
	mux.arb.pendingExternal = mux.arb.pendingExternal[:0]
	mux.pendingStart = &pendingStartState{
		sessionID: 1,
		initiator: 0x31,
		notify:    reqA.notify,
		req:       reqA,
	}
	mux.stateMu.Unlock()

	// Stage 2: mark the in-flight request cancelled (what
	// requestStartForSession does on a same-session re-submit).
	reqA.cancelled.Store(true)

	// Stage 3: feed STARTED for the cancelled request.
	mux.handleArbitrationResponse(true, 0x31)

	// F-1 ASSERTION: the channel-value cancelled flag MUST be true.
	select {
	case result := <-chA:
		if result.granted {
			t.Fatal("cancelled in-flight STARTED must not be granted")
		}
		if !result.cancelled {
			t.Fatalf("startResult.cancelled = %v on in-flight-cancel STARTED-half; want true (F-1 regression — session.go handleStart would fall through to deliverFailed and emit ENHResFailed on the wire, regressing F-17 fix for the C4/R4 path)", result.cancelled)
		}
		if result.initiator != 0x31 {
			t.Fatalf("startResult.initiator = 0x%02X, want 0x31", result.initiator)
		}
	case <-time.After(time.Second):
		t.Fatal("notify channel did not fire after handleArbitrationResponse(STARTED)")
	}
}

// TestHandleArbitrationResponse_InFlightCancelled_FAILED_SuppressedSilently
// pins F-1's FAILED-half: when the bus genuinely arbitrates and someone
// else wins (FAILED arrives with the winner's byte), the result for a
// cancelled in-flight bid MUST also carry `cancelled: true`. Otherwise
// session.go emits ENHResFailed(winner_byte) on the wire for a session
// that has already moved on — same retry-loop class as F-17.
func TestHandleArbitrationResponse_InFlightCancelled_FAILED_SuppressedSilently(t *testing.T) {
	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		SYNInterval:     time.Hour,
		PendingStartTTL: 24 * time.Hour,
	})

	// Setup identical to STARTED-half.
	chA := mux.arb.requestStart(1, 0x31)
	mux.stateMu.Lock()
	reqA := mux.arb.pendingExternal[0]
	mux.arb.pendingExternal = mux.arb.pendingExternal[:0]
	mux.pendingStart = &pendingStartState{
		sessionID: 1,
		initiator: 0x31,
		notify:    reqA.notify,
		req:       reqA,
	}
	mux.stateMu.Unlock()
	reqA.cancelled.Store(true)

	// Feed FAILED (someone else won the bus with byte 0x10).
	mux.handleArbitrationResponse(false, 0x10)

	select {
	case result := <-chA:
		if result.granted {
			t.Fatal("cancelled in-flight FAILED must not be granted")
		}
		if !result.cancelled {
			t.Fatalf("startResult.cancelled = %v on in-flight-cancel FAILED-half; want true (F-1 regression — session.go handleStart would emit ENHResFailed(0x10) on the wire to a session that has moved on, triggering the same retry feedback loop F-17 fixed)", result.cancelled)
		}
		// initiator field carries the bidder's byte (so any stray
		// downstream consumer sees the bidder's own bid, not the
		// third-party winner). Either value is acceptable for the
		// silent-suppression branch since session.go returns
		// silently — but pin the design choice so it doesn't drift.
		if result.initiator != 0x31 {
			t.Fatalf("startResult.initiator = 0x%02X on suppressed FAILED-half; want 0x31 (the bidder's byte, matching STARTED-half symmetry)", result.initiator)
		}
	case <-time.After(time.Second):
		t.Fatal("notify channel did not fire after handleArbitrationResponse(FAILED)")
	}
}

// TestHandleArbitrationResponse_InFlightCancelled_FAILED_LogMarksSuppression
// is a log-level guard: the FAILED-half suppression should emit a
// distinct log line so the diagnostic trail clearly shows F-1's
// FAILED-half firing in production.
func TestHandleArbitrationResponse_InFlightCancelled_FAILED_LogMarksSuppression(t *testing.T) {
	var buf cancelledStartedLogBuffer
	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		SYNInterval:     time.Hour,
		PendingStartTTL: 24 * time.Hour,
		Logger:          log.New(&buf, "", 0),
	})

	_ = mux.arb.requestStart(1, 0x31)
	mux.stateMu.Lock()
	reqA := mux.arb.pendingExternal[0]
	mux.arb.pendingExternal = mux.arb.pendingExternal[:0]
	mux.pendingStart = &pendingStartState{
		sessionID: 1, initiator: 0x31, notify: reqA.notify, req: reqA,
	}
	mux.stateMu.Unlock()
	reqA.cancelled.Store(true)

	mux.handleArbitrationResponse(false, 0x10)

	got := buf.String()
	if !strings.Contains(got, "FAILED-half") {
		t.Errorf("expected FAILED-half suppression log line, got:\n%s", got)
	}
}

// TestArbitrator_CancelStart_ExternalSession_SetsCancelledFlag pins
// F-2 for the external-session path: SYN-cancel before tryGrant pops
// the bid into m.pendingStart MUST resolve the notify channel with
// cancelled=true so handleStart silent-returns.
func TestArbitrator_CancelStart_ExternalSession_SetsCancelledFlag(t *testing.T) {
	arb := newArbitrator()
	arb.setPolicy(24 * time.Hour)

	ch := arb.requestStart(1, 0x31)
	if !arb.cancelStart(1) {
		t.Fatal("expected cancelStart to find the pending request")
	}

	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("cancelled request must not be granted")
		}
		if !result.cancelled {
			t.Fatalf("startResult.cancelled = %v on external cancelStart; want true (F-2 regression — session.go handleStart would fall through to deliverFailed and emit ENHResFailed on the wire for a session that already initiated SYN-cancel)", result.cancelled)
		}
		if result.initiator != 0x31 {
			t.Fatalf("startResult.initiator = 0x%02X, want 0x31", result.initiator)
		}
	case <-time.After(time.Second):
		t.Fatal("notify channel did not fire on cancelStart")
	}
}

// TestArbitrator_CancelStart_GatewaySession_SetsCancelledFlag pins F-2
// for the gateway-session path. Same contract as the external path.
func TestArbitrator_CancelStart_GatewaySession_SetsCancelledFlag(t *testing.T) {
	arb := newArbitrator()
	arb.setPolicy(24 * time.Hour)

	ch := arb.requestStart(gatewaySessionID, 0x71)
	if !arb.cancelStart(gatewaySessionID) {
		t.Fatal("expected cancelStart to find the gateway pending request")
	}

	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("cancelled gateway request must not be granted")
		}
		if !result.cancelled {
			t.Fatalf("startResult.cancelled = %v on gateway cancelStart; want true (F-2 regression, symmetric path)", result.cancelled)
		}
		if result.initiator != 0x71 {
			t.Fatalf("startResult.initiator = 0x%02X, want 0x71", result.initiator)
		}
	case <-time.After(time.Second):
		t.Fatal("notify channel did not fire on gateway cancelStart")
	}
}

// TestArbitrator_CancelStart_AlsoSetsStructFlag is the symmetric
// guard to F-17's TestArbitrator_SameSessionReplace_StructFlagAlsoSet:
// callers downstream may still read the struct flag (e.g., the C4/R4
// late-STARTED suppression path), so cancelStart must set both flags.
func TestArbitrator_CancelStart_AlsoSetsStructFlag(t *testing.T) {
	arb := newArbitrator()
	arb.setPolicy(24 * time.Hour)

	_ = arb.requestStart(1, 0x31)
	arb.mu.Lock()
	req := arb.pendingExternal[0]
	arb.mu.Unlock()

	if req.cancelled.Load() {
		t.Fatal("setup: cancelled flag set prematurely")
	}

	// Drain the channel asynchronously so the send doesn't block.
	go func() { <-req.notify }()

	if !arb.cancelStart(1) {
		t.Fatal("expected cancelStart to find the pending request")
	}

	if !req.cancelled.Load() {
		t.Fatal("startRequest.cancelled (struct flag) not set by cancelStart (F-2 regression — would leave any downstream observer of the struct flag in the wrong state)")
	}
}
