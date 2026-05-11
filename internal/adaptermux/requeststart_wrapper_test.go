package adaptermux

import (
	"context"
	"net"
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

// TestHandleArbitrationResponse_CancelledStartedResyncsAndDoesNotArmAbsorb
// pins two Codex P1 rounds on PR #623:
//
//   - Round 2: handleArbitrationResponse MUST NOT arm
//     pendingStartAbsorb when consuming a cancelled STARTED — there's
//     no further stale response in flight; arming would gate the next
//     tryGrantAndStart on the absorb barrier for up to StartDeadline.
//
//   - Round 3: the adapter has already granted the abandoned bid, so
//     before any replacement START can launch, the mux must force a
//     transport resync (close the upstream conn so readLoop's error
//     handler invokes reconnect). Otherwise the adapter is one
//     arbitration ahead of the mux's view and the replacement would
//     collide with the in-progress (but abandoned) transaction.
func TestHandleArbitrationResponse_CancelledStartedResyncsAndDoesNotArmAbsorb(t *testing.T) {
	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		SYNInterval:     time.Hour,
		PendingStartTTL: 24 * time.Hour,
	})

	// Install a fake conn so we can observe the reconnect-via-close.
	connMock := newCancelledStartedConnMock()
	mux.connMu.Lock()
	mux.conn = connMock
	mux.connMu.Unlock()

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

	// Round-2 assertion: pendingStartAbsorb MUST be zero.
	mux.stateMu.Lock()
	absorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if absorb != 0 {
		t.Errorf("pendingStartAbsorb = %d after consuming cancelled STARTED; want 0 (Codex P1 round 2 regression)", absorb)
	}

	// Round-3 assertion: the upstream conn MUST have been closed to
	// force a reconnect/resync with the adapter.
	if !connMock.closed.Load() {
		t.Error("upstream conn was NOT closed after cancelled STARTED — adapter resync skipped (Codex P1 round 3 regression)")
	}
}

// cancelledStartedConnMock is a minimal net.Conn-compatible stub used
// to observe whether handleArbitrationResponse's cancelled-STARTED
// branch closes the upstream conn for reconnect. Only Close is
// inspected; other methods are no-ops sufficient for the test path.
type cancelledStartedConnMock struct {
	closed atomic.Bool
}

func newCancelledStartedConnMock() *cancelledStartedConnMock {
	return &cancelledStartedConnMock{}
}

func (c *cancelledStartedConnMock) Read(_ []byte) (int, error)         { return 0, nil }
func (c *cancelledStartedConnMock) Write(p []byte) (int, error)        { return len(p), nil }
func (c *cancelledStartedConnMock) Close() error                       { c.closed.Store(true); return nil }
func (c *cancelledStartedConnMock) LocalAddr() net.Addr                { return nil }
func (c *cancelledStartedConnMock) RemoteAddr() net.Addr               { return nil }
func (c *cancelledStartedConnMock) SetDeadline(_ time.Time) error      { return nil }
func (c *cancelledStartedConnMock) SetReadDeadline(_ time.Time) error  { return nil }
func (c *cancelledStartedConnMock) SetWriteDeadline(_ time.Time) error { return nil }

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
