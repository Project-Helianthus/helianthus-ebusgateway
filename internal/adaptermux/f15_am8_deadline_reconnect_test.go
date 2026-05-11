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
// be closed. The absorb safety-net at armPendingStartAbsorbLocked
// already handles the rare truly-hung case via its own deadline timer
// (mux.go:3056) — the unconditional reconnect that used to live in
// the AM8 callback was an asymmetric design defect.
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
// Symmetry with cancelPendingStart (mux.go:3013) is the design
// invariant: both code paths drop the pending and gate the
// reconnect on `pending.blockingArb`.
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
// same behavior cancelPendingStart implements at mux.go:3013.
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
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel

	connMock := newCancelledStartedConnMock()
	mux.connMu.Lock()
	mux.upstream = mock
	mux.conn = connMock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

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

	// Allow the gated blocking transport goroutine to complete so
	// the test does not leak a hanging goroutine into the test
	// runner.
	close(mock.gate)
}
