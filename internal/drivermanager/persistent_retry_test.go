package drivermanager

// Issue #1369: a persistent retry policy keeps a retryable failure series in
// BACKOFF after the fast budget is spent, with capped exponential delays and
// bounded jitter, while the driver is desired RUNNING. A non-retryable
// failure, an explicit Stop and an explicit Start still end or reset the
// series. The clock, the jitter source and the retry wait are injected, so
// every delay is asserted exactly.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var (
	errPersistentDial   = errors.New("dial refused")
	errPersistentConfig = errors.New("configuration rejected")
)

// persistentRuntime admits a first generation on Start; every later
// construction consults outcome with its 1-based replacement number.
type persistentRuntime struct {
	mu         sync.Mutex
	generation uint64
	revision   uint64
	starts     int
	replaces   int
	stops      int
	outcome    func(replace int) error
}

func (runtime *persistentRuntime) construct(replace bool) (uint64, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.revision++
	var err error
	if replace {
		runtime.replaces++
		if runtime.outcome != nil {
			err = runtime.outcome(runtime.replaces)
		}
	} else {
		runtime.starts++
	}
	if err != nil {
		return runtime.generation, err
	}
	runtime.generation++
	return runtime.generation, nil
}

func (runtime *persistentRuntime) Start(context.Context) (uint64, error) {
	return runtime.construct(false)
}

func (runtime *persistentRuntime) Replace(context.Context) (uint64, error) {
	return runtime.construct(true)
}

func (runtime *persistentRuntime) Stop(context.Context) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.stops++
	runtime.revision++
	return nil
}

func (runtime *persistentRuntime) Generation() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.generation
}

func (runtime *persistentRuntime) Revision() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.revision
}

func (*persistentRuntime) SafetyQuarantined() bool { return false }

func (runtime *persistentRuntime) counts() (starts, replaces int) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.starts, runtime.replaces
}

// persistentClock is a manual clock, an alternating extreme jitter source
// (+limit, -limit, ...) and a retry wait that records each scheduled retry
// and advances the clock by its delay instead of sleeping.
type persistentClock struct {
	mu      sync.Mutex
	now     time.Time
	jitters int
	retries []persistentRetry
	// block, when it reports true for a retry ordinal, holds that wait until
	// its context ends (a retry that is still waiting when it is canceled).
	block   func(attempt int) bool
	waiting chan int
	manager *Manager
}

type persistentRetry struct {
	delay    time.Duration
	clock    time.Time
	snapshot Snapshot
}

func newPersistentClock() *persistentClock {
	return &persistentClock{now: time.Date(2026, time.October, 25, 18, 27, 25, 0, time.UTC), waiting: make(chan int, 64)}
}

func (clock *persistentClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *persistentClock) Jitter(limit time.Duration) time.Duration {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.jitters++
	if clock.jitters%2 == 1 {
		return limit
	}
	return -limit
}

func (clock *persistentClock) Wait(ctx context.Context, delay time.Duration) bool {
	snapshot, _ := clock.manager.Snapshot("ebus.primary")
	clock.mu.Lock()
	clock.retries = append(clock.retries, persistentRetry{delay: delay, clock: clock.now, snapshot: snapshot})
	block := clock.block
	clock.mu.Unlock()
	attempt := 0
	if snapshot.Retry != nil {
		attempt = snapshot.Retry.Attempt
	}
	if block != nil && block(attempt) {
		clock.waiting <- attempt
		<-ctx.Done()
		return false
	}
	clock.mu.Lock()
	clock.now = clock.now.Add(delay)
	clock.mu.Unlock()
	return true
}

func (clock *persistentClock) recorded() []persistentRetry {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return append([]persistentRetry(nil), clock.retries...)
}

// persistentPolicy is the ebus.primary production shape: five fast attempts
// from one second doubling to thirty seconds, then one second doubling to
// five minutes, 20% jitter.
func persistentPolicy() RetryPolicy {
	return RetryPolicy{
		Budget: 5, InitialDelay: time.Second, MaxDelay: 30 * time.Second, JitterRatio: 0.2,
		Persistent: PersistentRetry{Enabled: true, InitialDelay: time.Second, MaxDelay: 5 * time.Minute},
	}
}

func newPersistentManager(t *testing.T, runtime Runtime, clock *persistentClock, policy RetryPolicy) *Manager {
	t.Helper()
	manager, err := New(Config{
		Now:         clock.Now,
		RetryJitter: clock.Jitter,
		RetryWait:   clock.Wait,
		Drivers: []DriverConfig{{
			ID: "ebus.primary", Enabled: true, Runtime: runtime,
			Capabilities: []Capability{CapabilityRead},
			ClassifyError: func(err error) Failure {
				if errors.Is(err, errPersistentConfig) {
					return Failure{Reason: Reason{Code: ReasonConfigInvalid}}
				}
				return Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}
			},
			Retry: policy,
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	clock.manager = manager
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	return manager
}

// expectedPersistentDelay is the delay of the n-th retry of a series (from
// zero) under persistentPolicy, given the sign of its jitter. It is written
// out independently of retryDelay: nominal = initial * 2^index capped, the
// jitter is a fifth of the nominal, and the result is clamped to the phase.
func expectedPersistentDelay(n int, positive bool) time.Duration {
	initial, ceiling, index := time.Second, 30*time.Second, n
	if n >= 5 {
		ceiling, index = 5*time.Minute, n-5
	}
	nominal := initial
	for i := 0; i < index && nominal < ceiling; i++ {
		nominal *= 2
	}
	if nominal > ceiling {
		nominal = ceiling
	}
	delay := nominal - nominal/5
	if positive {
		delay = nominal + nominal/5
	}
	if delay < initial {
		delay = initial
	}
	if delay > ceiling {
		delay = ceiling
	}
	return delay
}

func reportPersistentLoss(t *testing.T, manager *Manager) {
	t.Helper()
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if !manager.ReportFailure("ebus.primary", Correlation{Generation: running.Generation}, Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}) {
		t.Fatal("ReportFailure refused the stable RUNNING correlation")
	}
}

func TestPersistentRetryContinuesPastFastBudgetWithCappedJitteredDelays(t *testing.T) {
	const failures = 19 // five fast attempts and fourteen persistent ones
	clock := newPersistentClock()
	runtime := &persistentRuntime{outcome: func(replace int) error {
		if replace <= failures {
			return errPersistentDial
		}
		return nil
	}}
	manager := newPersistentManager(t, runtime, clock, persistentPolicy())
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	reportPersistentLoss(t, manager)

	recovered := requireObservedState(t, manager, "ebus.primary", ObservedRunning, 5*time.Second)
	if recovered.Generation != 2 || recovered.Retry != nil {
		t.Fatalf("recovered snapshot = %#v; want generation 2 without a retry", recovered)
	}
	retries := clock.recorded()
	if len(retries) != failures+1 {
		t.Fatalf("scheduled retries = %d; want %d (every failure retried, none terminal)", len(retries), failures+1)
	}
	for n, retry := range retries {
		want := expectedPersistentDelay(n, n%2 == 0)
		if retry.delay != want {
			t.Fatalf("retry %d delay = %s; want %s", n+1, retry.delay, want)
		}
		if retry.delay < time.Second || retry.delay > 5*time.Minute {
			t.Fatalf("retry %d delay %s outside [1s, 5m]", n+1, retry.delay)
		}
		snapshot := retry.snapshot
		if snapshot.ObservedState != ObservedBackoff || snapshot.Reason.Code != ReasonRetryScheduled || snapshot.DesiredState != DesiredRunning || snapshot.Retry == nil {
			t.Fatalf("retry %d snapshot = %#v; want BACKOFF/RETRY_SCHEDULED with a schedule", n+1, snapshot)
		}
		wantRemaining := 0
		if n < 5 {
			wantRemaining = 5 - n
		}
		if snapshot.Retry.Attempt != n+1 || snapshot.Retry.BudgetRemaining != wantRemaining || !snapshot.Retry.Eligible {
			t.Fatalf("retry %d schedule = %#v; want attempt %d with %d fast attempts remaining", n+1, snapshot.Retry, n+1, wantRemaining)
		}
		if !snapshot.Retry.NotBeforeUTC.Equal(retry.clock.Add(retry.delay)) {
			t.Fatalf("retry %d not-before = %s; want %s", n+1, snapshot.Retry.NotBeforeUTC, retry.clock.Add(retry.delay))
		}
	}
	// Anchors stated literally: the persistent phase restarts at one second
	// (a negative jitter is floored there), the cap is five minutes, and at
	// the cap only the negative jitter moves the delay.
	for n, want := range map[int]time.Duration{4: 19200 * time.Millisecond, 5: time.Second, 6: 2400 * time.Millisecond, 14: 5 * time.Minute, 15: 4 * time.Minute, 18: 5 * time.Minute} {
		if retries[n].delay != want {
			t.Fatalf("retry %d delay = %s; want %s", n+1, retries[n].delay, want)
		}
	}

	// A success closes the series: the next loss starts again at the first
	// fast attempt.
	reportPersistentLoss(t, manager)
	requireObservedState(t, manager, "ebus.primary", ObservedRunning, 5*time.Second)
	retries = clock.recorded()
	next := retries[failures+1].snapshot.Retry
	if next == nil || next.Attempt != 1 || next.BudgetRemaining != 5 {
		t.Fatalf("schedule after recovery = %#v; want a new series at attempt 1 with the full fast budget", next)
	}
}

func TestPersistentRetryEndsOnNonRetryableFailureWithItsReason(t *testing.T) {
	clock := newPersistentClock()
	runtime := &persistentRuntime{outcome: func(replace int) error {
		if replace == 7 {
			return errPersistentConfig
		}
		return errPersistentDial
	}}
	manager := newPersistentManager(t, runtime, clock, persistentPolicy())
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	reportPersistentLoss(t, manager)
	failed := requireObservedState(t, manager, "ebus.primary", ObservedFailed, 5*time.Second)
	if failed.Reason.Code != ReasonConfigInvalid || failed.Retry != nil || failed.DesiredState != DesiredRunning {
		t.Fatalf("terminal snapshot = %#v; want FAILED/CONFIG_INVALID, desired RUNNING", failed)
	}
	time.Sleep(20 * time.Millisecond)
	if _, replaces := runtime.counts(); replaces != 7 {
		t.Fatalf("replacements = %d; want 7 (no retry after the non-retryable failure)", replaces)
	}
}

func TestBoundedRetryPolicyStillExhaustsWithoutPersistentPhase(t *testing.T) {
	clock := newPersistentClock()
	runtime := &persistentRuntime{outcome: func(int) error { return errPersistentDial }}
	policy := persistentPolicy()
	policy.Persistent = PersistentRetry{}
	manager := newPersistentManager(t, runtime, clock, policy)
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	reportPersistentLoss(t, manager)
	failed := requireObservedState(t, manager, "ebus.primary", ObservedFailed, 5*time.Second)
	if failed.Reason.Code != ReasonRetryExhausted {
		t.Fatalf("bounded policy ended %s; want RETRY_EXHAUSTED", failed.Reason.Code)
	}
	if _, replaces := runtime.counts(); replaces != 5 {
		t.Fatalf("replacements = %d; want the 5 fast attempts", replaces)
	}
}

func TestPersistentRetryExplicitStartResetsScheduleAndStopEndsIt(t *testing.T) {
	clock := newPersistentClock()
	clock.block = func(attempt int) bool { return attempt == 7 }
	runtime := &persistentRuntime{outcome: func(int) error { return errPersistentDial }}
	manager := newPersistentManager(t, runtime, clock, persistentPolicy())
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	reportPersistentLoss(t, manager)
	select {
	case <-clock.waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("the series never reached its seventh retry")
	}
	before := manager.mustSnapshot(t)
	if before.Retry == nil || before.Retry.Attempt != 7 || before.Retry.BudgetRemaining != 0 {
		t.Fatalf("waiting snapshot = %#v; want persistent attempt 7", before.Retry)
	}

	// An explicit Start during the persistent wait cancels that wait and
	// opens a new series: attempt 1, full fast budget, first fast delay.
	clock.mu.Lock()
	clock.block = func(int) bool { return true }
	clock.mu.Unlock()
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("explicit Start() error = %v", err)
	}
	select {
	case <-clock.waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("the explicit start scheduled no retry")
	}
	retries := clock.recorded()
	last := retries[len(retries)-1]
	if last.snapshot.Retry == nil || last.snapshot.Retry.Attempt != 1 || last.snapshot.Retry.BudgetRemaining != 5 {
		t.Fatalf("schedule after explicit Start = %#v; want attempt 1 with the full fast budget", last.snapshot.Retry)
	}
	if last.delay != expectedPersistentDelay(0, true) && last.delay != expectedPersistentDelay(0, false) {
		t.Fatalf("delay after explicit Start = %s; want the first fast delay (1s, jittered)", last.delay)
	}

	// An explicit Stop ends the series: STOPPED, and no construction after it.
	if err := manager.Stop(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	stopped := requireObservedState(t, manager, "ebus.primary", ObservedStopped, time.Second)
	if stopped.DesiredState != DesiredStopped || stopped.Retry != nil {
		t.Fatalf("stopped snapshot = %#v", stopped)
	}
	_, replaces := runtime.counts()
	time.Sleep(30 * time.Millisecond)
	if _, after := runtime.counts(); after != replaces {
		t.Fatalf("constructions after Stop: %d -> %d", replaces, after)
	}
}

func (manager *Manager) mustSnapshot(t *testing.T) Snapshot {
	t.Helper()
	snapshot, ok := manager.Snapshot("ebus.primary")
	if !ok {
		t.Fatal("driver snapshot missing")
	}
	return snapshot
}
