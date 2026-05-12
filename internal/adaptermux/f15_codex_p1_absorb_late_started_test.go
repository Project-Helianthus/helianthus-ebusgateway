package adaptermux

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// TestAbsorbLateStarted_NonBlockingPath_DoesNotReconnect pins the
// Codex bot P1 finding on PR #626 (reviewing commits b3b7f13 and
// e6b96ee, 2026-05-11): the F-15 fix prevented the IMMEDIATE AM8
// deadline reconnect on the non-blocking path, but
// `handleArbitrationResponse`'s absorb-consume branch still had
// `needReconnect := started`, so the LATE STARTED arriving after
// the deadline armed pendingStartAbsorb would tear down the TCP
// transport via the absorb path. That preserves the F-17 / F-15
// retry-feedback-loop on a delayed path.
//
// Reproducer:
//  1. Dispatch a START on the non-blocking ENH path; adapter does
//     not respond before StartDeadline.
//  2. AM8 deadline fires → armPendingStartAbsorbLocked("deadline")
//     arms pendingStartAbsorb=1. F-15 fix: no immediate conn.Close.
//  3. Adapter eventually responds with STARTED (the late
//     "slow-not-hung" response that F-15 was filed for).
//  4. handleArbitrationResponse enters the absorb branch
//     (pendingStartAbsorb>0), decrements counter, and PREVIOUSLY
//     would conn.Close because `needReconnect := started`.
//  5. Codex bot P1 fix (mirror F-15 transport-type gate): on the
//     non-blocking RequestStart path, the absorb-consume must NOT
//     close the conn — eBUS is per-SYN stateless, the counter has
//     already done its bookkeeping job.
//
// Invariant: end-to-end on non-blocking transport, a slow-not-hung
// STARTED arriving after AM8 deadline must NOT close the upstream.
func TestAbsorbLateStarted_NonBlockingPath_DoesNotReconnect(t *testing.T) {
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

	// Step 1: dispatch a START on the non-blocking path; adapter
	// will respond LATE (after AM8 deadline).
	gwCh := mux.arb.requestStart(gatewaySessionID, 0x71)
	// Prime tryGrantAndStart via a SYN byte.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}

	// Wait for AM8 deadline to fire (the first failure result).
	select {
	case result := <-gwCh:
		if result.granted {
			t.Fatal("expected granted=false after AM8 deadline")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for AM8 deadline failure result")
	}

	// F-15 invariant: AM8 deadline itself must NOT have reconnected
	// the non-blocking path.
	if connMock.closed.Load() {
		t.Fatal("F-15 regression: AM8 deadline reconnected the non-blocking path")
	}

	// Confirm absorb counter is armed (the AM8 deadline path armed
	// it via armPendingStartAbsorbLocked("deadline")).
	mux.stateMu.Lock()
	absorbArmed := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if absorbArmed == 0 {
		t.Fatal("setup: expected pendingStartAbsorb > 0 after AM8 deadline")
	}

	// Step 3: now feed the LATE STARTED (the slow-but-not-hung
	// response that finally arrived). This drives
	// handleArbitrationResponse's absorb-consume branch.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Byte: 0x71}

	// Give readLoop a moment to consume the event and run the
	// absorb branch.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mux.stateMu.Lock()
		consumed := mux.pendingStartAbsorb < absorbArmed
		mux.stateMu.Unlock()
		if consumed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mux.stateMu.Lock()
	finalAbsorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if finalAbsorb >= absorbArmed {
		t.Fatalf("late STARTED was not absorbed: pendingStartAbsorb stayed at %d (was %d)", finalAbsorb, absorbArmed)
	}

	// CODEX P1 ASSERTION: the absorb-consume of the late STARTED
	// must NOT close the upstream conn on the non-blocking path.
	// Prior to this fix `needReconnect := started` was true in the
	// absorb branch and the conn was torn down.
	time.Sleep(50 * time.Millisecond) // let any deferred close fire
	if connMock.closed.Load() {
		t.Fatal("upstream conn was closed by absorb-consume of late STARTED on non-blocking path — Codex P1 regression: the F-15 retry-feedback-loop is preserved on a delayed path. The absorb branch must gate reconnect on isBlockingPath, mirroring F-15.")
	}
}
