package adaptermux

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// PR #626 review round-3 — angry-tester findings M1 and M2.
//
// M1 (MEDIUM): the AM8 deadline path in mux.go sends a startResult
// for the pending request without honoring `pending.req.cancelled`.
// This path fires PRECISELY when the adapter is slow — the exact
// production condition that motivated this PR cycle. A same-session
// re-submit that flipped `pending.req.cancelled` via
// markInFlightCancelled, followed by the AM8 deadline firing before
// the slow STARTED/FAILED arrives, makes session.go's handleStart
// fall through to deliverFailed → ENHResFailed on the wire. Same
// retry-loop class as F-17 / F-NEW-1.
//
// M2 (MEDIUM): transport-call err paths (non-blocking RequestStart
// + blocking StartArbitration) also send a startResult with the
// transport error without honoring the cancelled flag. Lower
// probability than M1 but same class.

// TestAM8Deadline_NonBlockingPath_InFlightCancelled_SuppressedSilently
// pins M1: after markInFlightCancelled, the AM8 deadline firing
// while the adapter is silent MUST resolve the old notify channel
// with `cancelled: true` so session.go silent-returns.
func TestAM8Deadline_NonBlockingPath_InFlightCancelled_SuppressedSilently(t *testing.T) {
	mock := newP3MockTransport()

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		StartDeadline:   150 * time.Millisecond,
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.conn = newCancelledStartedConnMock()
	connMock := mux.conn.(*cancelledStartedConnMock)
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()
	defer func() {
		cancel()
		_ = mock.Close()
		done := make(chan struct{})
		go func() { mux.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("mux goroutine drain exceeded 2s")
		}
	}()

	// Dispatch a START on the non-blocking path; adapter never
	// responds (eventCh stays empty except for the priming SYN).
	gwCh := mux.arb.requestStart(gatewaySessionID, 0x71)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}

	// Wait for pendingStart to be armed.
	armDeadline := time.Now().Add(500 * time.Millisecond)
	armed := false
	for time.Now().Before(armDeadline) {
		mux.stateMu.Lock()
		if mux.pendingStart != nil {
			armed = true
			mux.stateMu.Unlock()
			break
		}
		mux.stateMu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	if !armed {
		t.Fatal("setup: pendingStart not armed within 500ms")
	}

	// Flip the in-flight cancelled flag (what markInFlightCancelled
	// does when a same-session re-submit comes in while pendingStart
	// is still in flight).
	mux.stateMu.Lock()
	if mux.pendingStart.req == nil {
		mux.stateMu.Unlock()
		t.Fatal("setup: pendingStart.req unexpectedly nil — round-3 invariant relies on this being set by production code")
	}
	mux.pendingStart.req.cancelled.Store(true)
	mux.stateMu.Unlock()

	// Wait for the AM8 deadline to fire.
	select {
	case result := <-gwCh:
		if result.granted {
			t.Fatal("expected granted=false after AM8 deadline")
		}
		if !result.cancelled {
			t.Fatalf("startResult.cancelled = %v on AM8 deadline + in-flight-cancelled; want true (M1 regression — session.go handleStart would emit ENHResFailed(0x71) on the wire to a session that has moved on, regressing F-17 closure on the deadline path that fires whenever the adapter is slow)", result.cancelled)
		}
		if result.initiator != 0x71 {
			t.Fatalf("startResult.initiator = 0x%02X, want 0x71 (bidder's byte)", result.initiator)
		}
		if result.err == nil {
			t.Fatal("expected non-nil err on deadline-expired result")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for AM8 deadline failure result")
	}

	// Non-blocking path must NOT reconnect on AM8 deadline.
	time.Sleep(50 * time.Millisecond)
	if connMock.closed.Load() {
		t.Fatal("upstream conn was closed by AM8 deadline on non-blocking path — F-15 regression")
	}
}

// TestRequestStartErr_InFlightCancelled_SuppressedSilently pins M2's
// non-blocking-half: if the transport's RequestStart returns an error
// AND a same-session re-submit has flipped pending.req.cancelled,
// the old wait goroutine MUST silent-return rather than emit
// ENHResFailed.
//
// We construct this by injecting a transport that fails RequestStart
// after pre-cancellation: arm pendingStart with cancelled=true, then
// invoke tryGrantAndStart so the RequestStart err path runs.
func TestRequestStartErr_InFlightCancelled_SuppressedSilently(t *testing.T) {
	mock := &requestStartErrTransport{
		eventCh: make(chan transport.StreamEvent, 16),
		err:     errors.New("adaptermux-test: simulated RequestStart failure"),
	}

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		StartDeadline:   2 * time.Second, // long; we want the err path, not the deadline
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Enqueue a request whose struct flag is pre-flipped, simulating
	// the markInFlightCancelled outcome from a same-session re-submit
	// happening just before tryGrantAndStart pops it. We need to
	// flip the flag BEFORE tryGrant pops so the err path observes
	// pending.req.cancelled == true.
	ch := mux.arb.requestStart(1, 0x31)
	mux.arb.mu.Lock()
	req := mux.arb.pendingExternal[0]
	mux.arb.mu.Unlock()
	req.cancelled.Store(true)

	// Force-grant via tryGrantAndStart; the RequestStart-err branch
	// runs synchronously after popping.
	mux.tryGrantAndStart()

	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected granted=false after RequestStart error")
		}
		if !result.cancelled {
			t.Fatalf("startResult.cancelled = %v on RequestStart-err + in-flight-cancelled; want true (M2 regression — session.go handleStart would emit ENHResFailed on the wire on transport-call failure for a cancelled session)", result.cancelled)
		}
		if result.err == nil {
			t.Fatal("expected non-nil err on transport-failure result (M2 must preserve err for diagnostic trail)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for RequestStart-err result")
	}
}

// TestRequestStartErr_InFlightCancelled_ResetErr_DeliversReset pins the
// Codex bot P2 finding on PR #626 (2026-05-11): when the M2 RequestStart
// err path observes BOTH `cancelledInFlight=true` AND a transport err
// matching `isResetOrDisconnectError` (e.g. "adapter disconnected" /
// "adapter reset" / "adaptermux: closed" / ErrAdapterReset), the result
// MUST NOT carry `cancelled: true`. session.go's branch order is
// `granted → cancelled → err(reset) → deliverFailed`, so setting both
// flags would silent-return on the cancelled branch and the session
// would miss the RESETTED boundary event the client needs.
//
// The contract is documented in arbitration.failAllPending's block
// comment. The M2 fix originally violated it for the narrow case where
// a same-session re-submit raced with an adapter-reset-triggered
// RequestStart failure. Codex bot caught the gap; this test pins it.
func TestRequestStartErr_InFlightCancelled_ResetErr_DeliversReset(t *testing.T) {
	mock := &requestStartErrTransport{
		eventCh: make(chan transport.StreamEvent, 16),
		err:     errors.New("adapter disconnected during RequestStart"),
	}

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		StartDeadline:   2 * time.Second,
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// Same setup as TestRequestStartErr_InFlightCancelled_SuppressedSilently:
	// pre-flip the struct flag so the err-path observes cancelledInFlight.
	ch := mux.arb.requestStart(1, 0x31)
	mux.arb.mu.Lock()
	req := mux.arb.pendingExternal[0]
	mux.arb.mu.Unlock()
	req.cancelled.Store(true)

	mux.tryGrantAndStart()

	select {
	case result := <-ch:
		if result.granted {
			t.Fatal("expected granted=false after RequestStart error")
		}
		// Codex P2 ASSERTION: the result MUST NOT carry cancelled=true
		// when the err matches isResetOrDisconnectError, even though
		// the in-flight request was cancelled. Setting both would
		// suppress the RESETTED delivery the client needs.
		if result.cancelled {
			t.Fatalf("startResult.cancelled = true on RequestStart-err + cancelledInFlight + reset-err; want false so session.go's err(reset) → deliverReset branch fires (Codex P2 regression — the precedence rule in failAllPending's contract block applies here too)")
		}
		if result.err == nil {
			t.Fatal("expected non-nil err on transport-failure result")
		}
		if !isResetOrDisconnectError(result.err) {
			t.Fatalf("result.err must still match isResetOrDisconnectError so session.go routes to deliverReset; got: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for RequestStart-err result")
	}
}

// requestStartErrTransport implements arbitrationRequester returning
// a fixed error from RequestStart. Used to exercise M2's
// non-blocking-half err branch deterministically.
type requestStartErrTransport struct {
	eventCh    chan transport.StreamEvent
	err        error
	calls      atomic.Int32
	closed     atomic.Bool
}

func (t *requestStartErrTransport) RequestStart(initiator byte) error {
	t.calls.Add(1)
	return t.err
}

func (t *requestStartErrTransport) ReadEvent() (transport.StreamEvent, error) {
	ev, ok := <-t.eventCh
	if !ok {
		return transport.StreamEvent{}, errors.New("transport closed")
	}
	return ev, nil
}

func (t *requestStartErrTransport) ReadByte() (byte, error) {
	for {
		ev, err := t.ReadEvent()
		if err != nil {
			return 0, err
		}
		if ev.Kind == transport.StreamEventByte {
			return ev.Byte, nil
		}
	}
}

func (t *requestStartErrTransport) Write(p []byte) (int, error) { return len(p), nil }

func (t *requestStartErrTransport) Close() error {
	if t.closed.CompareAndSwap(false, true) {
		close(t.eventCh)
	}
	return nil
}

func (t *requestStartErrTransport) Init(features byte) (byte, error) { return features, nil }
func (t *requestStartErrTransport) BytesAreUnescaped() bool          { return true }
