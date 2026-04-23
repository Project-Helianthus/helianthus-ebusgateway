package mcp

// M5_TRANSPORT_MATRIX regression rows (plan AD12):
//
// These tests are the in-code enforcement of the matrix/M6a-vaillant-b503.md
// §3 regression transcripts. They do NOT re-prove M2a's session FSM
// correctness (that's done in internal/vaillant/b503session/session_test.go
// and mcp/vaillant_b503_test.go); they specifically assert the plan-AD12
// invariants that M2a's new liveMonitorMu mutex has not perturbed the
// gateway's pre-existing concurrency contracts.
//
// RB-03 (lock identity / ordering) is NOT testable from this package.
// b503session.Manager.mu is intentionally unexported, and adaptermux's
// readMu is package-private behind-a-stable-surface — importing
// adaptermux here would risk a dependency cycle. The RB-03 invariant
// (liveMonitorMu distinct from adaptermux readMu; acquisition order
// liveMonitorMu → readMu; reverse forbidden per spec §7.4) is enforced
// EXTERNALLY by:
//
//  1. -race CI on the whole repo: any shared-mutex deadlock or
//     lock-order cycle would surface as a -race runtime abort.
//  2. internal/vaillant/b503session/session_test.go concurrency suite:
//     directly exercises the Manager FSM under -race.
//  3. Code review: a refactor that reuses a pre-existing mutex would
//     have to rewrite b503session.Manager's struct — the
//     "liveMonitorMu — ownership gate, distinct from B524 readMu"
//     comment on that field is the reviewer trip-wire.
//
// No in-file test pretends to enforce RB-03; doing so with the surface
// available here would be either a false-positive smoke check (prior
// revision) or a mock-mutex stand-in (even earlier revision) — both
// struck by Codex review. Honesty over theatre.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
)

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

	var (
		wg            sync.WaitGroup
		pollerIter    int64
	)
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
				atomic.AddInt64(&pollerIter, 1)
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()

	// Barrier: wait until the poller has actually begun touching stateMu
	// at least a few times. Without this, on single-core or busy CI
	// runners the IsOwned loop could complete before the poller ever
	// runs, giving a fast uncontended path that misses the regression
	// this test is meant to catch.
	barrierDeadline := time.Now().Add(500 * time.Millisecond)
	for atomic.LoadInt64(&pollerIter) < 3 {
		if time.Now().After(barrierDeadline) {
			t.Fatal("poller did not reach 3 iterations within 500ms — scheduler starvation or regression")
		}
		time.Sleep(100 * time.Microsecond)
	}

	// IsOwned loop under the concurrent poller (now guaranteed running).
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
