package adaptermux

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// TestAM8Deadline_NonBlockingPath_NoReconnect pins F-15 (operator
// hand-off, batch-9/11): when the AM8 pendingStart deadline fires on
// the non-blocking ENH RequestStart path, the upstream conn MUST NOT
// be closed. The absorb safety-net inside
// `armPendingStartAbsorbLocked` already handles the rare truly-hung
// case via its own deadline timer (covered by
// `TestAM8Deadline_NonBlockingPath_AbsorbTimerReconnectsOnTrulyHungAdapter`)
// — the unconditional reconnect that used to live in the AM8 callback
// was an asymmetric design defect.
//
// Background: prior to v0.6.7 the deadline callback hard-coded
// `needReconnect := true`, so every slow-but-not-hung adapter
// response on the non-blocking path tore down the TCP transport.
// Under F-17's retry storm this amplified into a feedback loop —
// adapter backlog → slow STARTED → AM8 deadline trips → forced
// reconnect → next ReqStart aborted → another retry. Live evidence
// across 30k log lines: zero absorb-timeout reconnects fired in
// production, so the safety-net was sufficient and the AM8 reconnect
// was load-bearing only on the legacy blocking transport.
//
// Symmetry with `cancelPendingStart` is the design invariant: both
// code paths drop the pending and gate the reconnect on
// `pending.blockingArb`.
func TestAM8Deadline_NonBlockingPath_NoReconnect(t *testing.T) {
	mock := newP3MockTransport()

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		StartDeadline:   150 * time.Millisecond, // short for test speed
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour, // keep idle path quiet
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		mux.wg.Wait()
	}()

	// Queue a gateway START and feed a SYN so tryGrantAndStart pops
	// it into pendingStart via the non-blocking RequestStart path.
	gwCh := mux.arb.requestStart(gatewaySessionID, 0x71)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}

	// Wait for RequestStart to be called (confirms pendingStart is
	// armed on the non-blocking path with blockingArb=false).
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(mock.getStartRequests()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := mock.getStartRequests(); len(got) == 0 {
		t.Fatal("setup: RequestStart was never dispatched to the non-blocking mock")
	}

	mux.stateMu.Lock()
	if mux.pendingStart == nil {
		mux.stateMu.Unlock()
		t.Fatal("setup: pendingStart not armed")
	}
	if mux.pendingStart.blockingArb {
		mux.stateMu.Unlock()
		t.Fatal("setup: blockingArb=true on non-blocking RequestStart path (test invariant violated)")
	}
	mux.stateMu.Unlock()

	// Wait for the AM8 deadline (150ms) to fire and propagate.
	select {
	case result := <-gwCh:
		if result.granted {
			t.Fatal("expected granted=false after AM8 deadline")
		}
		if result.err == nil {
			t.Fatal("expected non-nil err on deadline-expired result")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for AM8 deadline failure result")
	}

	// Give the deadline goroutine a moment to complete its
	// post-notify housekeeping (no reconnect should happen, but the
	// goroutine still runs to completion).
	time.Sleep(50 * time.Millisecond)

	// F-15 ASSERTION: upstream conn MUST NOT have been closed. Prior
	// to the fix this would fail — `needReconnect := true` forced a
	// close on every deadline expiry regardless of transport type.
	if connMock.closed.Load() {
		t.Fatal("upstream conn was closed by AM8 deadline on the non-blocking path — F-15 regression: AM8 must mirror cancelPendingStart and gate the reconnect on pending.blockingArb")
	}

	// Sanity: pendingStart must be cleared after the deadline.
	mux.stateMu.Lock()
	pendingAfter := mux.pendingStart
	mux.stateMu.Unlock()
	if pendingAfter != nil {
		t.Fatal("pendingStart must be nil after AM8 deadline")
	}
}

// TestAM8Deadline_BlockingPath_StillReconnects pins the other half of
// F-15: the blocking StartArbitration path (legacy transport) MUST
// still trigger an upstream reconnect when the AM8 deadline fires,
// because the goroutine may still be hung in the transport call and
// the reconnect is the only safe way to unstick it. This is the
// same behavior `cancelPendingStart` implements.
//
// We use the same slowBlockingStartTransport mock from
// pr502_review_test.go (TestBlockingStartArbitrationDeadlineReal),
// but also install an inspectable upstream conn so we can assert
// Close was actually called on it.
func TestAM8Deadline_BlockingPath_StillReconnects(t *testing.T) {
	mock := &slowBlockingStartTransport{
		readCh: make(chan byte, 256),
		gate:   make(chan struct{}),
	}

	mux := New(Config{
		Protocol:      "enh",
		Network:       "tcp",
		Address:       "127.0.0.1:0",
		ReadTimeout:   200 * time.Millisecond,
		StartDeadline: 150 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	mux.ctx, mux.cancel = ctx, cancel

	connMock := newCancelledStartedConnMock()
	mux.connMu.Lock()
	mux.upstream = mock
	mux.conn = connMock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	// F-4 (review round-1): drain the spawned blocking goroutine
	// before the test function returns so we do not leak post-test
	// state mutations into the next test in the same package. The
	// gated goroutine inside StartArbitration is wg.Add'd by the
	// dispatch path; close(mock.gate) wakes it, mux.wg.Wait drains it.
	defer func() {
		// Close the gate first so any goroutine still parked inside
		// StartArbitration returns. Then cancel the context to
		// release readLoop/sendLoop, and wait on the WaitGroup with
		// a bounded timeout so a stray goroutine surfaces as a test
		// failure instead of a deadlock.
		close(mock.gate)
		cancel()
		done := make(chan struct{})
		go func() { mux.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("mux goroutine drain exceeded 2s — possible leak from blocking-path teardown")
		}
	}()

	// Queue a gateway request and dispatch on the blocking path.
	gwCh := mux.arb.requestStart(gatewaySessionID, 0x71)
	mux.tryGrantAndStart()

	// Confirm the blocking goroutine is in flight before the
	// deadline fires.
	time.Sleep(50 * time.Millisecond)
	mux.stateMu.Lock()
	if mux.pendingStart == nil || !mux.pendingStart.blockingArb {
		mux.stateMu.Unlock()
		t.Fatal("setup: expected pendingStart with blockingArb=true on blocking path")
	}
	mux.stateMu.Unlock()

	// Wait for the AM8 deadline.
	select {
	case result := <-gwCh:
		if result.granted {
			t.Fatal("expected granted=false after AM8 deadline")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for AM8 deadline failure result")
	}

	// Give the reconnect path a moment to run.
	time.Sleep(50 * time.Millisecond)

	// F-15 ASSERTION (blocking-half): conn MUST have been closed
	// on the blocking path. Removing or weakening this branch would
	// regress unstick-on-hung behavior for the legacy transport.
	if !connMock.closed.Load() {
		t.Fatal("upstream conn was NOT closed by AM8 deadline on the blocking path — F-15 regression: blocking path must still trigger reconnect to unstick the hung StartArbitration goroutine")
	}
}

// TestAM8Deadline_NonBlockingPath_AbsorbTimerReconnectsOnTrulyHungAdapter
// pins the F-15 fallback contract surfaced by review round-1 finding
// F-3: when the non-blocking adapter is TRULY hung (no STARTED or
// FAILED ever arrives), the AM8 deadline must NOT itself reconnect
// (proven by `TestAM8Deadline_NonBlockingPath_NoReconnect` above), but
// the absorb safety-net (armPendingStartAbsorbLocked's own
// time.AfterFunc) MUST drive a reconnect after another StartDeadline
// elapses. Otherwise the mux is permanently stuck behind the
// pendingStartAbsorb>0 guard at tryGrantAndStart and no new arbitration
// can proceed.
//
// This test waits past 2×StartDeadline against a silent non-blocking
// adapter and asserts conn.Close was eventually invoked by the absorb
// timer path.
func TestAM8Deadline_NonBlockingPath_AbsorbTimerReconnectsOnTrulyHungAdapter(t *testing.T) {
	mock := newP3MockTransport()

	const startDeadline = 120 * time.Millisecond

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		StartDeadline:   startDeadline,
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

	// Dispatch a START on the non-blocking path. Adapter never
	// responds (mock.eventCh stays empty for STARTED/FAILED, only
	// the priming SYN below is fed).
	_ = mux.arb.requestStart(gatewaySessionID, 0x71)
	// Prime tryGrantAndStart via a SYN byte (0xAA).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}

	// Wait for the first AM8 deadline (≈ startDeadline). Asserting
	// no reconnect AT this point is already done by the sibling
	// test — here we just need the absorb counter to be armed so
	// the safety-net timer is ticking.
	time.Sleep(startDeadline + 30*time.Millisecond)

	if connMock.closed.Load() {
		t.Fatal("F-15 broke: non-blocking AM8 deadline reconnected directly — see sibling TestAM8Deadline_NonBlockingPath_NoReconnect")
	}

	// Wait for the absorb timer's own StartDeadline to fire and
	// close the conn. Total elapsed ≈ 2×StartDeadline + slack.
	deadline := time.Now().Add(2*startDeadline + 500*time.Millisecond)
	for time.Now().Before(deadline) {
		if connMock.closed.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// F-3 ASSERTION: absorb-safety-net must have closed the conn.
	if !connMock.closed.Load() {
		t.Fatal("absorb safety-net did NOT close the conn after AM8 deadline + absorb timer window — F-15's claim that armPendingStartAbsorbLocked covers the truly-hung case is unsubstantiated; either the absorb timer never fired, or its reconnect path was removed. Recovery from a permanently silent non-blocking adapter requires this fallback to fire reliably.")
	}
}

