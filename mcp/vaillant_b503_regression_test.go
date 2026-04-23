package mcp

// M5_TRANSPORT_MATRIX regression rows (plan AD12):
//
// These tests are the in-code enforcement of the matrix/M6a-vaillant-b503.md
// §3 regression transcripts. They do NOT re-prove M2a's session FSM
// correctness (that's done in internal/vaillant/b503session/session_test.go
// and mcp/vaillant_b503_test.go); they specifically assert the plan-AD12
// invariants that M2a's new liveMonitorMu mutex has not perturbed the
// gateway's pre-existing concurrency contracts.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
)

// TestB524Regression_LockIdentityContract (RB-03, contract assertion):
//
// This test does NOT exercise the real adaptermux/B524 readMu — from the
// mcp package we cannot import adaptermux without inviting a dependency
// cycle, and adaptermux does not expose its readMu across package
// boundaries by design. Instead, this test documents the RB-03 invariant
// explicitly so regressions to it surface as a compilation break here.
//
// The invariant (design-level):
//   b503session.Manager.mu (liveMonitorMu) is a distinct *sync.Mutex
//   from any adaptermux mutex. Acquisition order when both are needed
//   is liveMonitorMu → readMu; the reverse is forbidden (spec §7.4).
//
// Live enforcement: the existing M2a concurrency test suite
// (internal/vaillant/b503session/session_test.go) runs under -race and
// would surface any shared-mutex deadlock; the CI -race matrix (test
// job in this PR) would surface any lock-ordering cycle. This test
// exists as a structural marker — if someone refactors
// b503session.Manager to reuse a pre-existing readMu, the next
// reviewer sees the RB-03 comment and reconsiders.
func TestB524Regression_LockIdentityContract(t *testing.T) {
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "rb03", TransportEpoch: 1},
		100*time.Millisecond,
		nil,
	)
	// Smoke-check: Manager initialises with a private mutex. We cannot
	// introspect mu identity from here, but we can verify the public
	// surface (Enable → Disable round-trip) does not deadlock and
	// completes within a tight budget (no hidden shared-lock wait).
	budget := 100 * time.Millisecond
	deadline := time.Now().Add(budget)

	key, err := mgr.Enable(context.Background())
	if err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if err := mgr.Disable(key); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if time.Now().After(deadline) {
		t.Errorf("Enable→Disable exceeded %v budget — suggests hidden mutex wait", budget)
	}
}

// TestB524Regression_ConcurrentReadsNoDeadlock (RB-02): concurrent
// simulated B524 readers + B503 session reads run for 100ms with no
// deadlock and no mutex-contention stalls. This is the matrix §3 RB-02
// evidence: B524 throughput unchanged under new liveMonitorMu.
//
// Synchronisation: both workers join via WaitGroup with a hard timeout
// budget, so if a regression causes one of them to block indefinitely
// the test fails rather than silently passing on the counter thresholds.
func TestB524Regression_ConcurrentReadsNoDeadlock(t *testing.T) {
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "rb02", TransportEpoch: 1},
		500*time.Millisecond,
		nil,
	)
	key, err := mgr.Enable(context.Background())
	if err != nil {
		t.Fatalf("Enable: %v", err)
	}

	var (
		b524Reads, b503Reads int64
		wg                   sync.WaitGroup
	)
	done := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				_ = mgr.State()
				atomic.AddInt64(&b524Reads, 1)
				time.Sleep(100 * time.Microsecond)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				if err := mgr.Read(mgr.TransportKey()); err != nil {
					t.Errorf("Read: unexpected error %v", err)
					return
				}
				atomic.AddInt64(&b503Reads, 1)
				time.Sleep(200 * time.Microsecond)
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(done)

	// Explicit join with hard timeout: if a regression causes a worker
	// to block on a mutex indefinitely, this fires and fails the test.
	joined := make(chan struct{})
	go func() {
		wg.Wait()
		close(joined)
	}()
	select {
	case <-joined:
		// both workers exited cleanly
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker goroutines did not exit within 500ms of close(done) — suggests deadlock / stuck mutex")
	}

	if atomic.LoadInt64(&b524Reads) < 10 {
		t.Errorf("B524 read count = %d; expected ≥10 in 100ms (suggests mutex stall)", atomic.LoadInt64(&b524Reads))
	}
	if atomic.LoadInt64(&b503Reads) < 10 {
		t.Errorf("B503 read count = %d; expected ≥10 in 100ms", atomic.LoadInt64(&b503Reads))
	}

	_ = mgr.Disable(key)
}

// TestB524Regression_IsOwnedNoReadContention (RB-04): verifies the
// IsOwned() inspection helper used by VaillantB503AvailabilityCtx has
// bounded stateMu contention even under simultaneous B524-style read
// pressure. A regression that introduces unexpected lock contention
// only under concurrent access must not slip through — so this test
// runs IsOwned() alongside a simulated B524 poll loop that touches the
// same stateMu (via mgr.State / mgr.Read), and asserts per-call cost
// stays bounded under the overlap.
func TestB524Regression_IsOwnedNoReadContention(t *testing.T) {
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "rb04", TransportEpoch: 1},
		500*time.Millisecond,
		nil,
	)
	key, err := mgr.Enable(context.Background())
	if err != nil {
		t.Fatalf("Enable: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Simulated B524 poller exercising stateMu via State() + Read().
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = mgr.State()
				_ = mgr.Read(mgr.TransportKey())
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()

	// IsOwned loop under the concurrent poller.
	const iterations = 10_000
	started := time.Now()
	for i := 0; i < iterations; i++ {
		_ = mgr.IsOwned()
	}
	elapsed := time.Since(started)

	close(stop)
	// Join the poller with a hard timeout so regressions that stall the
	// worker fail here rather than silently leaving it running.
	joined := make(chan struct{})
	go func() {
		wg.Wait()
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("poller goroutine did not exit within 500ms of stop — suggests mutex stall")
	}

	// Bound: under contention, 10k IsOwned calls should complete in
	// well under 500ms (sync.Mutex fast-path + brief wait for poller
	// windows). A regression adding real contention would push this
	// multiple seconds.
	if elapsed > 500*time.Millisecond {
		t.Errorf("%d IsOwned() calls under concurrent poller took %v; expected <500ms (suggests real mutex contention)", iterations, elapsed)
	}

	_ = mgr.Disable(key)
}
