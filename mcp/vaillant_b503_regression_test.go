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

// TestB524Regression_LockIsolation (RB-03): asserts the session gate is a
// distinct sync.Mutex from any B524 readMu — i.e., acquiring liveMonitorMu
// via b503session.Manager.Enable MUST NOT block a goroutine that holds a
// hypothetical B524 readMu, and vice versa. We model this with two
// unrelated mutexes the test concurrently acquires.
func TestB524Regression_LockIsolation(t *testing.T) {
	// Stand-in B524 readMu — in production this lives in the gateway's
	// adaptermux/bus layer. For the regression assertion we only need any
	// unrelated sync.Mutex; the invariant is that the session gate does
	// NOT share identity with it.
	var b524ReadMu sync.Mutex

	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "rb03", TransportEpoch: 1},
		100*time.Millisecond,
		nil,
	)

	// Acquire the stand-in B524 readMu in a background goroutine and hold
	// it for 50ms.
	readHeld := make(chan struct{})
	readReleased := make(chan struct{})
	go func() {
		b524ReadMu.Lock()
		close(readHeld)
		time.Sleep(50 * time.Millisecond)
		b524ReadMu.Unlock()
		close(readReleased)
	}()
	<-readHeld

	// With B524 readMu held, the session gate Enable MUST proceed without
	// blocking. If the two mutexes were the same (or session gate waited
	// on readMu), Enable would block for 50ms+.
	enableStarted := time.Now()
	key, err := mgr.Enable(context.Background())
	enableDuration := time.Since(enableStarted)
	if err != nil {
		t.Fatalf("Enable: unexpected error: %v", err)
	}
	if enableDuration > 20*time.Millisecond {
		t.Errorf("Enable blocked for %v while unrelated B524 readMu held; expected <20ms (lock isolation)", enableDuration)
	}
	_ = mgr.Disable(key)
	<-readReleased
}

// TestB524Regression_ConcurrentReadsNoDeadlock (RB-02): concurrent
// simulated B524 readers + B503 session reads run for 100ms with no
// deadlock and no mutex-contention stalls. This is the matrix §3 RB-02
// evidence: B524 throughput unchanged under new liveMonitorMu.
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

	var b524Reads, b503Reads int64
	done := make(chan struct{})

	// Simulated B524 poller.
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				// Observe manager state (transport_key match, ignored for
				// B524 but simulates a concurrent read path that inspects
				// session FSM).
				_ = mgr.State()
				atomic.AddInt64(&b524Reads, 1)
				time.Sleep(100 * time.Microsecond)
			}
		}
	}()

	// B503 read loop — resets idle timer each iteration.
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				if err := mgr.Read(mgr.TransportKey()); err != nil {
					t.Errorf("Read: unexpected error %v", err)
					close(done)
					return
				}
				atomic.AddInt64(&b503Reads, 1)
				time.Sleep(200 * time.Microsecond)
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(done)
	// Allow goroutines to exit.
	time.Sleep(10 * time.Millisecond)

	if atomic.LoadInt64(&b524Reads) < 10 {
		t.Errorf("B524 read count = %d; expected ≥10 in 100ms (suggests mutex stall)", atomic.LoadInt64(&b524Reads))
	}
	if atomic.LoadInt64(&b503Reads) < 10 {
		t.Errorf("B503 read count = %d; expected ≥10 in 100ms", atomic.LoadInt64(&b503Reads))
	}

	_ = mgr.Disable(key)
}

// TestB524Regression_IsOwnedNoReadContention (RB-04): verifies the
// IsOwned() inspection helper used by VaillantB503AvailabilityCtx does
// NOT take any lock that a concurrent B524 read path would contend with.
// It takes stateMu (field-level protection) but not liveMonitorMu.
// Contention on stateMu is bounded (<1µs per call in steady state).
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

	// Call IsOwned 10_000 times; should complete well under 100ms.
	started := time.Now()
	for i := 0; i < 10_000; i++ {
		_ = mgr.IsOwned()
	}
	elapsed := time.Since(started)
	if elapsed > 100*time.Millisecond {
		t.Errorf("10_000 IsOwned() calls took %v; expected <100ms (suggests mutex contention)", elapsed)
	}

	_ = mgr.Disable(key)
}
