package adaptermux

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func newCtxRequestStartTest() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// TestRequestStartForSession_MarksInFlightCancelledOnResubmit pins the
// Codex P1 round 1 finding on PR #623: when a session re-submits a
// START while a previous request has already been popped from the
// arbitrator into mux.pendingStart, the previous request's cancelled
// flag must be set so handleArbitrationResponse can convert a late
// STARTED into a FAILED. arb.requestStart's replace loop alone cannot
// catch this — it only scans pendingExternal — so the Mux wrapper
// has to thread the in-flight cancel itself.
func TestRequestStartForSession_MarksInFlightCancelledOnResubmit(t *testing.T) {
	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
		// Long SYNInterval so the wrapper's idle-kick does NOT
		// auto-grant the second request — we want to observe the
		// in-flight cancel flag set on the FIRST request via the
		// wrapper, independent of subsequent grants.
		SYNInterval:     time.Hour,
		PendingStartTTL: 24 * time.Hour,
	})

	// Stage 1: enqueue request A, then simulate it having been popped
	// by tryGrant by manually constructing the in-flight pendingStart
	// state. We don't drive tryGrantAndStart end-to-end here — that
	// requires a transport mock and is covered by integration tests.
	chA := mux.arb.requestStart(1, 0x31)
	_ = chA

	mux.stateMu.Lock()
	if len(mux.arb.pendingExternal) == 0 {
		mux.stateMu.Unlock()
		t.Fatal("setup: expected enqueued request to be present in pendingExternal")
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
	if reqA.cancelled.Load() {
		t.Fatal("setup: cancelled flag set prematurely")
	}

	// Stage 2: re-submit for the same session via the Mux wrapper.
	// arb.requestStart's replace loop won't find anything in
	// pendingExternal — so without the wrapper, reqA's cancelled
	// flag would stay false. With the wrapper, the in-flight check
	// fires markInFlightCancelled.
	_ = mux.requestStartForSession(1, 0x32)

	if !reqA.cancelled.Load() {
		t.Fatal("in-flight startRequest's cancelled flag was NOT set by the Mux wrapper (Codex P1 round 1 regression)")
	}
}

// TestRequestStartForSession_IdleKickDispatchesImmediately pins the
// Codex P1 round 1 (third finding) on PR #623: when the wire has
// been quiet for at least one SYN interval, requestStartForSession
// must kick tryGrantAndStart on enqueue so an external request does
// not sit in pendingExternal until the next ReadTimeout-driven SYN
// poll wakes up — long past the C3 PendingStartTTL window.
//
// We assert by observing the kick effect: under no other pending
// state and busIdle=true, the enqueue must clear pendingExternal
// (because tryGrant pops it on the kick) and set m.pendingStart.
func TestRequestStartForSession_IdleKickDispatchesImmediately(t *testing.T) {
	mock := newP3MockTransport()

	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
		// Short SYNInterval so the idle check yields true with
		// lastWireActivity sufficiently old.
		SYNInterval:     time.Microsecond,
		PendingStartTTL: 24 * time.Hour,
	})

	ctx, mockCancel := newCtxRequestStartTest()
	defer mockCancel()
	mux.ctx, mux.cancel = ctx, mockCancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)
	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()
	defer func() {
		mockCancel()
		_ = mock.Close()
		mux.wg.Wait()
	}()

	// Leave lastWireActivity at zero so the idle check fires.
	_ = mux.requestStartForSession(1, 0x31)

	// Allow the kick goroutine path inside tryGrantAndStart to run.
	// The blocking adapter mock's RequestStart records but doesn't
	// reply; we only need to observe that the kick reached the
	// adapter.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(getStartReqCount(mock)) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(getStartReqCount(mock)) == 0 {
		t.Fatal("idle-kick did NOT dispatch RequestStart to the adapter (Codex P1 round 1 regression)")
	}
}

// TestHandleArbitrationResponse_CancelledStartedDoesNotArmAbsorb pins
// Codex P1 round 2 on PR #623: when handleArbitrationResponse consumes
// a STARTED whose request was marked cancelled via the C4 in-flight
// path, it MUST NOT call armPendingStartAbsorbLocked. The STARTED
// being consumed is the only adapter response for the cancelled
// request — there's no further stale response in flight. Arming
// absorb would gate the next tryGrantAndStart on the absorb barrier
// for up to StartDeadline (5 s) and starve the replacement request
// for the duration of the client's retry budget.
func TestHandleArbitrationResponse_CancelledStartedDoesNotArmAbsorb(t *testing.T) {
	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		SYNInterval:     time.Hour,
		PendingStartTTL: 24 * time.Hour,
	})

	// Stage 1: simulate an in-flight cancelled grant.
	chA := mux.arb.requestStart(1, 0x31)
	_ = chA
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
	reqA.cancelled.Store(true)
	mux.stateMu.Unlock()

	// Stage 2: feed the STARTED response for the cancelled request.
	mux.handleArbitrationResponse(true, 0x31)

	// pendingStartAbsorb MUST be zero — there's no further stale
	// response to swallow.
	mux.stateMu.Lock()
	absorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if absorb != 0 {
		t.Fatalf("pendingStartAbsorb = %d after consuming cancelled STARTED; want 0 (Codex P1 round 2 regression — would gate next tryGrantAndStart for up to StartDeadline)", absorb)
	}
}

// getStartReqCount peeks into p3MockTransport's startRequests slice
// for the test above. p3_test.go exposes getStartRequests(); we count
// via that.
func getStartReqCount(m *p3MockTransport) *int32 {
	// p3MockTransport tracks via getStartRequests() but doesn't
	// expose a counter directly. Build a tiny shim via the existing
	// method.
	count := int32(len(m.getStartRequests()))
	return &count
}
