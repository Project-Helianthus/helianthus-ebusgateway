package drivermanager

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type scriptedRuntime struct {
	mu           sync.Mutex
	startResults []error
	stopResults  []error
	startCalls   int
	stopCalls    int
	generation   uint64
	revision     uint64
	quarantined  bool
}

type cancelableConstructionRuntime struct {
	mu              sync.Mutex
	generation      uint64
	revision        uint64
	replaceCalls    int
	stopCalls       int
	replaceStarted  chan struct{}
	replaceCanceled chan struct{}
	startOnce       sync.Once
	cancelOnce      sync.Once
}

type shutdownBlockingRuntime struct {
	*scriptedRuntime
	stopEntered chan struct{}
	stopRelease chan struct{}
	stopOnce    sync.Once
}

type repeatedStartRuntime struct {
	mu           sync.Mutex
	startCalls   int
	stopCalls    int
	generation   uint64
	revision     uint64
	startEntered chan struct{}
	startRelease chan struct{}
	startOnce    sync.Once
}

type lateAdmissionRuntime struct {
	mu           sync.Mutex
	generation   uint64
	revision     uint64
	stopCalls    int
	running      bool
	startEntered chan struct{}
	startRelease chan struct{}
}

type lateCleanupRuntime struct {
	mu            sync.Mutex
	generation    uint64
	revision      uint64
	stopCalls     int
	running       bool
	startEntered  chan struct{}
	startRelease  chan struct{}
	lateStopError error
	quarantined   bool
}

type withdrawalFenceRuntime struct {
	mu              sync.Mutex
	generation      uint64
	revision        uint64
	fenced          bool
	stopSawFence    bool
	replaceSawFence bool
}

func (runtime *withdrawalFenceRuntime) FenceWithdrawal() {
	runtime.mu.Lock()
	runtime.fenced = true
	runtime.mu.Unlock()
}

func (runtime *withdrawalFenceRuntime) Start(context.Context) (uint64, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.generation++
	runtime.revision++
	runtime.fenced = false
	return runtime.generation, nil
}

func (runtime *withdrawalFenceRuntime) Replace(context.Context) (uint64, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.replaceSawFence = runtime.fenced
	runtime.generation++
	runtime.revision++
	runtime.fenced = false
	return runtime.generation, nil
}

func (runtime *withdrawalFenceRuntime) Stop(context.Context) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.stopSawFence = runtime.fenced
	runtime.revision++
	return nil
}

func (runtime *withdrawalFenceRuntime) Generation() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.generation
}

func (runtime *withdrawalFenceRuntime) Revision() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.revision
}

func (*withdrawalFenceRuntime) SafetyQuarantined() bool { return false }

func (runtime *lateCleanupRuntime) Start(context.Context) (uint64, error) {
	close(runtime.startEntered)
	<-runtime.startRelease
	runtime.mu.Lock()
	runtime.generation++
	runtime.revision++
	runtime.running = true
	generation := runtime.generation
	runtime.mu.Unlock()
	return generation, nil
}

func (runtime *lateCleanupRuntime) Replace(ctx context.Context) (uint64, error) {
	return runtime.Start(ctx)
}

func (runtime *lateCleanupRuntime) Stop(ctx context.Context) error {
	runtime.mu.Lock()
	runtime.stopCalls++
	call := runtime.stopCalls
	runtime.revision++
	runtime.running = false
	err := runtime.lateStopError
	runtime.mu.Unlock()
	if call == 1 {
		return nil
	}
	if err != nil {
		if errors.Is(err, ErrSafetyQuarantined) {
			runtime.mu.Lock()
			runtime.quarantined = true
			runtime.mu.Unlock()
		}
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func (runtime *lateCleanupRuntime) Generation() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.generation
}

func (runtime *lateCleanupRuntime) Revision() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.revision
}

func (runtime *lateCleanupRuntime) SafetyQuarantined() bool {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.quarantined
}

func (runtime *lateAdmissionRuntime) Start(context.Context) (uint64, error) {
	close(runtime.startEntered)
	<-runtime.startRelease // deliberately ignores cancellation and Stop
	runtime.mu.Lock()
	runtime.generation++
	runtime.revision++
	runtime.running = true
	generation := runtime.generation
	runtime.mu.Unlock()
	return generation, nil
}

func (runtime *lateAdmissionRuntime) Replace(ctx context.Context) (uint64, error) {
	return runtime.Start(ctx)
}

func (runtime *lateAdmissionRuntime) Stop(context.Context) error {
	runtime.mu.Lock()
	runtime.stopCalls++
	runtime.revision++
	runtime.running = false
	runtime.mu.Unlock()
	return nil
}

func (runtime *lateAdmissionRuntime) Generation() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.generation
}

func (runtime *lateAdmissionRuntime) Revision() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.revision
}

func (*lateAdmissionRuntime) SafetyQuarantined() bool { return false }

func (runtime *repeatedStartRuntime) Start(context.Context) (uint64, error) {
	runtime.mu.Lock()
	runtime.startCalls++
	runtime.revision++
	runtime.mu.Unlock()
	runtime.startOnce.Do(func() { close(runtime.startEntered) })
	<-runtime.startRelease
	runtime.mu.Lock()
	runtime.generation++
	generation := runtime.generation
	runtime.mu.Unlock()
	return generation, nil
}

func (runtime *repeatedStartRuntime) Replace(ctx context.Context) (uint64, error) {
	return runtime.Start(ctx)
}

func (runtime *repeatedStartRuntime) Stop(context.Context) error {
	runtime.mu.Lock()
	runtime.stopCalls++
	runtime.revision++
	runtime.mu.Unlock()
	return nil
}

func (runtime *repeatedStartRuntime) Generation() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.generation
}

func (runtime *repeatedStartRuntime) Revision() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.revision
}

func (*repeatedStartRuntime) SafetyQuarantined() bool { return false }

func (runtime *shutdownBlockingRuntime) Stop(ctx context.Context) error {
	runtime.stopOnce.Do(func() { close(runtime.stopEntered) })
	select {
	case <-runtime.stopRelease:
		return runtime.scriptedRuntime.Stop(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newCancelableConstructionRuntime() *cancelableConstructionRuntime {
	return &cancelableConstructionRuntime{
		replaceStarted:  make(chan struct{}),
		replaceCanceled: make(chan struct{}),
	}
}

func (runtime *cancelableConstructionRuntime) Start(context.Context) (uint64, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.generation++
	runtime.revision++
	return runtime.generation, nil
}

func (runtime *cancelableConstructionRuntime) Replace(ctx context.Context) (uint64, error) {
	runtime.mu.Lock()
	runtime.replaceCalls++
	generation := runtime.generation
	runtime.mu.Unlock()
	runtime.startOnce.Do(func() { close(runtime.replaceStarted) })
	<-ctx.Done()
	runtime.cancelOnce.Do(func() { close(runtime.replaceCanceled) })
	return generation, ctx.Err()
}

func (runtime *cancelableConstructionRuntime) Stop(context.Context) error {
	runtime.mu.Lock()
	runtime.stopCalls++
	runtime.revision++
	runtime.mu.Unlock()
	return nil
}

func (runtime *cancelableConstructionRuntime) Generation() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.generation
}

func (runtime *cancelableConstructionRuntime) Revision() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.revision
}

func (*cancelableConstructionRuntime) SafetyQuarantined() bool { return false }

func (runtime *scriptedRuntime) Start(context.Context) (uint64, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.startCalls++
	runtime.revision++
	var err error
	if len(runtime.startResults) != 0 {
		err = runtime.startResults[0]
		runtime.startResults = runtime.startResults[1:]
	}
	if err != nil {
		return runtime.generation, err
	}
	runtime.generation++
	return runtime.generation, nil
}

func (runtime *scriptedRuntime) Replace(ctx context.Context) (uint64, error) {
	if err := runtime.Stop(ctx); err != nil {
		return runtime.Generation(), err
	}
	return runtime.Start(ctx)
}

func (runtime *scriptedRuntime) Stop(context.Context) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.stopCalls++
	runtime.revision++
	var err error
	if len(runtime.stopResults) != 0 {
		err = runtime.stopResults[0]
		runtime.stopResults = runtime.stopResults[1:]
	}
	if errors.Is(err, ErrSafetyQuarantined) {
		runtime.quarantined = true
	}
	return err
}

func (runtime *scriptedRuntime) Generation() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.generation
}

func (runtime *scriptedRuntime) Revision() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.revision
}

func (runtime *scriptedRuntime) SafetyQuarantined() bool {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.quarantined
}

func (runtime *scriptedRuntime) calls() (start, stop int) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.startCalls, runtime.stopCalls
}

func TestRetryDelayUsesBoundedInjectableJitter(t *testing.T) {
	policy := normalizeRetryPolicy(RetryPolicy{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     80 * time.Millisecond,
		JitterRatio:  0.25,
	})
	var observedLimit time.Duration
	positive := retryDelay(policy, 1, func(limit time.Duration) time.Duration {
		observedLimit = limit
		return 10 * limit // manager must clamp an out-of-range source
	})
	if observedLimit != 5*time.Millisecond || positive != 25*time.Millisecond {
		t.Fatalf("positive jitter limit/delay = %v/%v, want 5ms/25ms", observedLimit, positive)
	}
	negative := retryDelay(policy, 1, func(limit time.Duration) time.Duration { return -10 * limit })
	if negative != 15*time.Millisecond {
		t.Fatalf("negative jitter delay = %v, want 15ms", negative)
	}
	minimum := retryDelay(policy, 0, func(limit time.Duration) time.Duration { return -limit })
	if minimum != policy.InitialDelay {
		t.Fatalf("minimum jitter delay = %v, want floor %v", minimum, policy.InitialDelay)
	}
	maximum := retryDelay(policy, 3, func(limit time.Duration) time.Duration { return limit })
	if maximum != policy.MaxDelay {
		t.Fatalf("maximum jitter delay = %v, want ceiling %v", maximum, policy.MaxDelay)
	}
}

func TestManagerRetrySnapshotUsesInjectedJitter(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	runtime := &scriptedRuntime{startResults: []error{errors.New("offline")}}
	manager, err := New(Config{
		Now: func() time.Time { return now },
		RetryJitter: func(limit time.Duration) time.Duration {
			return limit
		},
		Drivers: []DriverConfig{{
			ID:      "ebus.primary",
			Enabled: true,
			Runtime: runtime,
			ClassifyError: func(error) Failure {
				return Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}
			},
			Retry: RetryPolicy{Budget: 1, InitialDelay: 100 * time.Millisecond, MaxDelay: time.Second, JitterRatio: 0.25},
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	backoff := requireObservedState(t, manager, "ebus.primary", ObservedBackoff, time.Second)
	if backoff.Retry == nil || !backoff.Retry.NotBeforeUTC.Equal(now.Add(125*time.Millisecond)) {
		t.Fatalf("retry snapshot = %#v, want not-before %s", backoff.Retry, now.Add(125*time.Millisecond))
	}
	_ = manager.Stop(context.Background(), "ebus.primary")
}

func TestManagerInitialFailureBacksOffAndRecoversCategorically(t *testing.T) {
	rawFailure := errors.New("dial tcp://user:secret@example.invalid:9999: refused")
	runtime := &scriptedRuntime{startResults: []error{rawFailure, nil}}
	manager, err := New(Config{
		Drivers: []DriverConfig{{
			ID:           "ebus.primary",
			Enabled:      true,
			Runtime:      runtime,
			Capabilities: []Capability{CapabilityWrite, CapabilityRead, CapabilityDiscovery, CapabilityRead},
			ClassifyError: func(error) Failure {
				return Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}
			},
			Retry: RetryPolicy{Budget: 2, InitialDelay: 5 * time.Millisecond, MaxDelay: 5 * time.Millisecond},
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()

	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() promoted a managed driver failure to caller error: %v", err)
	}
	backoff := requireObservedState(t, manager, "ebus.primary", ObservedBackoff, time.Second)
	if backoff.DesiredState != DesiredRunning || backoff.Generation != 0 || backoff.Revision <= 1 {
		t.Fatalf("BACKOFF snapshot = %#v", backoff)
	}
	if backoff.Reason.Code != ReasonRetryScheduled || !backoff.Reason.Retryable || backoff.Retry == nil {
		t.Fatalf("BACKOFF reason/retry = %#v / %#v", backoff.Reason, backoff.Retry)
	}
	if rendered := backoff.Reason.String(); rendered == rawFailure.Error() || containsSensitiveDriverText(rendered) {
		t.Fatalf("snapshot reason leaked raw construction detail: %q", rendered)
	}

	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if running.Generation != 1 || running.Revision <= backoff.Revision || running.Reason.Code != ReasonNone {
		t.Fatalf("RUNNING snapshot = %#v", running)
	}
	wantCapabilities := []Capability{CapabilityDiscovery, CapabilityRead, CapabilityWrite}
	if !reflect.DeepEqual(running.Capabilities, wantCapabilities) || !reflect.DeepEqual(running.EffectiveCapabilities, wantCapabilities) {
		t.Fatalf("capabilities static=%v effective=%v, want %v", running.Capabilities, running.EffectiveCapabilities, wantCapabilities)
	}
}

func TestManagerStopCancelsBackoffWithoutResurrection(t *testing.T) {
	runtime := &scriptedRuntime{startResults: []error{errors.New("offline"), nil}}
	manager, err := New(Config{Drivers: []DriverConfig{{
		ID:      "ebus.primary",
		Enabled: true,
		Runtime: runtime,
		ClassifyError: func(error) Failure {
			return Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}
		},
		Retry: RetryPolicy{Budget: 1, InitialDelay: 150 * time.Millisecond, MaxDelay: 150 * time.Millisecond},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	requireObservedState(t, manager, "ebus.primary", ObservedBackoff, time.Second)
	if err := manager.Stop(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	stopped := requireObservedState(t, manager, "ebus.primary", ObservedStopped, time.Second)
	if stopped.DesiredState != DesiredStopped || len(stopped.EffectiveCapabilities) != 0 || stopped.Reason.Code != ReasonOperatorStopped {
		t.Fatalf("STOPPED snapshot = %#v", stopped)
	}
	time.Sleep(220 * time.Millisecond)
	starts, _ := runtime.calls()
	if starts != 1 {
		t.Fatalf("runtime Start calls after stopped backoff = %d, want 1", starts)
	}
}

func TestManagerRejectsStaleGenerationHealthCallbackAfterReplace(t *testing.T) {
	runtime := &scriptedRuntime{}
	manager, err := New(Config{Drivers: []DriverConfig{{
		ID:           "ebus.primary",
		Enabled:      true,
		Runtime:      runtime,
		Capabilities: []Capability{CapabilityDiscovery, CapabilityRead, CapabilityWrite},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()

	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	first := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if accepted := manager.ReportDegraded("ebus.primary", first.Generation, []Capability{CapabilityRead}, Reason{Code: ReasonCapabilityDegraded}); !accepted {
		t.Fatal("current generation degraded callback was rejected")
	}
	degraded := requireObservedState(t, manager, "ebus.primary", ObservedDegraded, time.Second)
	if !reflect.DeepEqual(degraded.EffectiveCapabilities, []Capability{CapabilityRead}) {
		t.Fatalf("degraded effective capabilities = %v", degraded.EffectiveCapabilities)
	}

	if err := manager.Replace(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	second := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if second.Generation != first.Generation+1 {
		t.Fatalf("replacement generation = %d, want %d", second.Generation, first.Generation+1)
	}
	if accepted := manager.ReportDegraded("ebus.primary", first.Generation, []Capability{CapabilityRead}, Reason{Code: ReasonCapabilityDegraded}); accepted {
		t.Fatal("stale generation callback mutated current state")
	}
	afterStale, _ := manager.Snapshot("ebus.primary")
	if afterStale.ObservedState != ObservedRunning || afterStale.Generation != second.Generation || !reflect.DeepEqual(afterStale.EffectiveCapabilities, second.Capabilities) {
		t.Fatalf("snapshot after stale callback = %#v", afterStale)
	}
}

func TestManagerSameDesiredStartPreservesDegradedGenerationByteForByte(t *testing.T) {
	runtime := &scriptedRuntime{}
	manager, err := New(Config{Drivers: []DriverConfig{{
		ID:           "ebus.primary",
		Enabled:      true,
		Runtime:      runtime,
		Capabilities: []Capability{CapabilityDiscovery, CapabilityRead, CapabilityWrite},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()

	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("initial Start() error = %v", err)
	}
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if accepted := manager.ReportDegraded(
		"ebus.primary",
		running.Generation,
		[]Capability{CapabilityRead},
		Reason{Code: ReasonCapabilityDegraded},
	); !accepted {
		t.Fatal("ReportDegraded() rejected current generation")
	}
	before := requireObservedState(t, manager, "ebus.primary", ObservedDegraded, time.Second)
	startsBefore, stopsBefore := runtime.calls()

	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("same-desired Start() error = %v", err)
	}
	after, ok := manager.Snapshot("ebus.primary")
	if !ok {
		t.Fatal("Snapshot() missing driver")
	}
	startsAfter, stopsAfter := runtime.calls()
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("same-desired Start changed degraded snapshot\nbefore: %#v\nafter:  %#v", before, after)
	}
	if startsAfter != startsBefore || stopsAfter != stopsBefore {
		t.Fatalf("same-desired Start runtime calls = start %d->%d stop %d->%d", startsBefore, startsAfter, stopsBefore, stopsAfter)
	}
}

func TestManagerSafetyQuarantineMirrorsRuntimeAndBlocksRecovery(t *testing.T) {
	runtime := &scriptedRuntime{stopResults: []error{ErrSafetyQuarantined}}
	manager, err := New(Config{Drivers: []DriverConfig{{
		ID:           "ebus.primary",
		Enabled:      true,
		Runtime:      runtime,
		Capabilities: []Capability{CapabilityRead, CapabilityWrite},
		Retry:        RetryPolicy{Budget: 2, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if err := manager.Stop(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	failed := requireObservedState(t, manager, "ebus.primary", ObservedFailed, time.Second)
	if !failed.SafetyQuarantined || failed.Reason.Code != ReasonCloseUnconfirmed || len(failed.EffectiveCapabilities) != 0 {
		t.Fatalf("quarantined snapshot = %#v", failed)
	}
	if failed.Generation != running.Generation {
		t.Fatalf("quarantine generation = %d, want %d", failed.Generation, running.Generation)
	}
	if err := manager.Start(context.Background(), "ebus.primary"); !errors.Is(err, ErrSafetyQuarantined) {
		t.Fatalf("Start() after quarantine error = %v, want ErrSafetyQuarantined", err)
	}
	if err := manager.Replace(context.Background(), "ebus.primary"); !errors.Is(err, ErrSafetyQuarantined) {
		t.Fatalf("Replace() after quarantine error = %v, want ErrSafetyQuarantined", err)
	}
	if accepted := manager.ReportDegradedCorrelated("ebus.primary", Correlation{Generation: running.Generation}, []Capability{CapabilityRead}, Reason{Code: ReasonCapabilityDegraded}); accepted {
		t.Fatal("late callback cleared process-epoch quarantine")
	}
	starts, stops := runtime.calls()
	if starts != 1 || stops != 1 {
		t.Fatalf("runtime calls after recovery attempts = start:%d stop:%d, want 1/1", starts, stops)
	}
}

func TestManagerRejectsCallbackFromSupersededOperation(t *testing.T) {
	runtime := &scriptedRuntime{}
	manager, err := New(Config{Drivers: []DriverConfig{{
		ID:           "ebus.primary",
		Enabled:      true,
		Runtime:      runtime,
		Capabilities: []Capability{CapabilityRead, CapabilityWrite},
		Retry:        RetryPolicy{Budget: 1, InitialDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	oldCorrelation := Correlation{Generation: running.Generation}
	if accepted := manager.ReportFailure("ebus.primary", oldCorrelation, Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}); !accepted {
		t.Fatal("first current-operation failure was rejected")
	}
	backoff := requireObservedState(t, manager, "ebus.primary", ObservedBackoff, time.Second)
	if backoff.ActiveOperation == 0 {
		t.Fatalf("BACKOFF snapshot has no active operation: %#v", backoff)
	}
	if accepted := manager.ReportFailure("ebus.primary", oldCorrelation, Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}); accepted {
		t.Fatal("callback from superseded operation mutated BACKOFF state")
	}
	after, _ := manager.Snapshot("ebus.primary")
	if after.Revision != backoff.Revision || after.ActiveOperation != backoff.ActiveOperation {
		t.Fatalf("superseded callback changed snapshot: before=%#v after=%#v", backoff, after)
	}
}

func TestManagerReportFailureWithZeroRetryBudgetPublishesRetryExhausted(t *testing.T) {
	runtime := &scriptedRuntime{}
	manager, err := New(Config{Drivers: []DriverConfig{{
		ID:           "ebus.primary",
		Enabled:      true,
		Runtime:      runtime,
		Capabilities: []Capability{CapabilityRead, CapabilityWrite},
		Retry:        RetryPolicy{Budget: 0},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()

	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if accepted := manager.ReportFailure(
		"ebus.primary",
		Correlation{Generation: running.Generation},
		Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}},
	); !accepted {
		t.Fatal("ReportFailure() rejected current generation")
	}

	failed := requireObservedState(t, manager, "ebus.primary", ObservedFailed, time.Second)
	if failed.DesiredState != DesiredRunning || failed.Reason != (Reason{Code: ReasonRetryExhausted}) {
		t.Fatalf("zero-budget failure snapshot = %#v", failed)
	}
	if failed.Retry != nil || len(failed.EffectiveCapabilities) != 0 || failed.ActiveOperation != 0 {
		t.Fatalf("zero-budget failure retained retry/admission = %#v", failed)
	}
	starts, stops := runtime.calls()
	if starts != 1 || stops != 0 {
		t.Fatalf("zero-budget failure runtime calls = start:%d stop:%d, want 1/0", starts, stops)
	}
}

func TestIssue851ManagerReportFailureFencesRuntimeBeforeBackoff(t *testing.T) {
	runtime := &withdrawalFenceRuntime{}
	manager, err := New(Config{Drivers: []DriverConfig{{
		ID:           "ebus.primary",
		Enabled:      true,
		Runtime:      runtime,
		Capabilities: []Capability{CapabilityRead, CapabilityWrite},
		Retry:        RetryPolicy{Budget: 1, InitialDelay: time.Hour, MaxDelay: time.Hour},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if accepted := manager.ReportFailure(
		"ebus.primary",
		Correlation{Generation: running.Generation},
		Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}},
	); !accepted {
		t.Fatal("ReportFailure() rejected current generation")
	}
	backoff := requireObservedState(t, manager, "ebus.primary", ObservedBackoff, time.Second)
	runtime.mu.Lock()
	fenced := runtime.fenced
	runtime.mu.Unlock()
	if !fenced {
		t.Fatalf("BACKOFF published before runtime withdrawal fence: %#v", backoff)
	}
}

func TestManagerStopCancelsInFlightRetryConstruction(t *testing.T) {
	runtime := newCancelableConstructionRuntime()
	manager, err := New(Config{Drivers: []DriverConfig{{
		ID:           "ebus.primary",
		Enabled:      true,
		Runtime:      runtime,
		Capabilities: []Capability{CapabilityRead, CapabilityWrite},
		Retry:        RetryPolicy{Budget: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if accepted := manager.ReportFailure("ebus.primary", Correlation{Generation: running.Generation}, Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}); !accepted {
		t.Fatal("ReportFailure() rejected current generation")
	}
	select {
	case <-runtime.replaceStarted:
	case <-time.After(time.Second):
		t.Fatal("automatic retry construction did not start")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- manager.Stop(context.Background(), "ebus.primary") }()
	select {
	case <-runtime.replaceCanceled:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Stop did not cancel the in-flight retry construction")
	}
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not complete after construction cancellation")
	}
	stopped := requireObservedState(t, manager, "ebus.primary", ObservedStopped, time.Second)
	if stopped.DesiredState != DesiredStopped || len(stopped.EffectiveCapabilities) != 0 {
		t.Fatalf("STOPPED snapshot = %#v", stopped)
	}
}

func TestManagerShutdownFencePreventsEarlierDriverResurrection(t *testing.T) {
	firstRuntime := &scriptedRuntime{}
	secondRuntime := &shutdownBlockingRuntime{
		scriptedRuntime: &scriptedRuntime{},
		stopEntered:     make(chan struct{}),
		stopRelease:     make(chan struct{}),
	}
	manager, err := New(Config{Drivers: []DriverConfig{
		{ID: "driver.a", Enabled: true, Runtime: firstRuntime, Capabilities: []Capability{CapabilityRead}},
		{ID: "driver.b", Enabled: true, Runtime: secondRuntime, Capabilities: []Capability{CapabilityRead}},
	}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := manager.Start(context.Background(), "driver.a"); err != nil {
		t.Fatalf("Start(driver.a) error = %v", err)
	}
	if err := manager.Start(context.Background(), "driver.b"); err != nil {
		t.Fatalf("Start(driver.b) error = %v", err)
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- manager.Shutdown(context.Background()) }()
	select {
	case <-secondRuntime.stopEntered:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not reach the second driver")
	}
	if err := manager.Start(context.Background(), "driver.a"); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Start(driver.a) during Shutdown error = %v, want ErrManagerClosed", err)
	}
	if err := manager.Replace(context.Background(), "driver.a"); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Replace(driver.a) during Shutdown error = %v, want ErrManagerClosed", err)
	}
	close(secondRuntime.stopRelease)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish")
	}
	first, _ := manager.Snapshot("driver.a")
	if first.ObservedState != ObservedStopped || first.DesiredState != DesiredStopped {
		t.Fatalf("driver.a resurrected after Shutdown: %#v", first)
	}
	starts, stops := firstRuntime.calls()
	if starts != 1 || stops != 1 {
		t.Fatalf("driver.a runtime calls = start:%d stop:%d, want 1/1", starts, stops)
	}
}

func TestManagerRepeatedStartJoinsStartingIntent(t *testing.T) {
	runtime := &repeatedStartRuntime{startEntered: make(chan struct{}), startRelease: make(chan struct{})}
	manager, err := New(Config{Drivers: []DriverConfig{{
		ID: "ebus.primary", Enabled: true, Runtime: runtime, Capabilities: []Capability{CapabilityRead},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- manager.Start(context.Background(), "ebus.primary") }()
	select {
	case <-runtime.startEntered:
	case <-time.After(time.Second):
		t.Fatal("first Start did not reach runtime")
	}
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("repeated Start() error = %v", err)
	}
	snapshot, _ := manager.Snapshot("ebus.primary")
	if snapshot.ObservedState != ObservedStarting || snapshot.Reason.Code != ReasonStartRequested {
		t.Fatalf("repeated Start changed in-progress state: %#v", snapshot)
	}
	runtime.mu.Lock()
	calls := runtime.startCalls
	runtime.mu.Unlock()
	if calls != 1 {
		t.Fatalf("runtime Start calls = %d, want 1", calls)
	}
	close(runtime.startRelease)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	_ = manager.Shutdown(context.Background())
}

func TestManagerRepeatedStartDuringRetryPublicationKeepsRecoveryLive(t *testing.T) {
	runtime := &scriptedRuntime{startResults: []error{errors.New("offline"), nil}}
	classification := make(chan bool, 1)
	releaseClassification := make(chan struct{})
	var manager *Manager
	var err error
	manager, err = New(Config{Drivers: []DriverConfig{{
		ID:      "ebus.primary",
		Enabled: true,
		Runtime: runtime,
		ClassifyError: func(error) Failure {
			// finishAttempt holds driver.mu while classifying. The completed
			// attempt must already be retired before BACKOFF and its sole retry
			// can be published under that same lock.
			classification <- manager.drivers["ebus.primary"].activeAttempt == nil
			<-releaseClassification
			return Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}
		},
		Retry: RetryPolicy{Budget: 1, InitialDelay: time.Second, MaxDelay: time.Second},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()

	firstDone := make(chan error, 1)
	go func() { firstDone <- manager.Start(context.Background(), "ebus.primary") }()
	attemptRetired := <-classification

	repeatedStarted := make(chan struct{})
	repeatedDone := make(chan error, 1)
	go func() {
		close(repeatedStarted)
		repeatedDone <- manager.Start(context.Background(), "ebus.primary")
	}()
	<-repeatedStarted
	close(releaseClassification)

	if err := <-firstDone; err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	if err := <-repeatedDone; err != nil {
		t.Fatalf("repeated Start() error = %v", err)
	}
	if !attemptRetired {
		t.Fatal("retry publication retained its completed active attempt")
	}
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if running.Generation != 1 || running.Reason.Code != ReasonNone || running.Retry != nil {
		t.Fatalf("repeated Start stranded recovery: %#v", running)
	}
	starts, _ := runtime.calls()
	if starts != 2 {
		t.Fatalf("runtime Start calls = %d, want initial failure plus one recovery", starts)
	}
}

func TestManagerRetiresLateAdmissionAfterStopIntent(t *testing.T) {
	runtime := &lateAdmissionRuntime{startEntered: make(chan struct{}), startRelease: make(chan struct{})}
	manager, err := New(Config{Drivers: []DriverConfig{{
		ID: "ebus.primary", Enabled: true, Runtime: runtime, Capabilities: []Capability{CapabilityRead},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := manager.StartAsync(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("StartAsync() error = %v", err)
	}
	select {
	case <-runtime.startEntered:
	case <-time.After(time.Second):
		t.Fatal("StartAsync did not enter runtime")
	}
	if err := manager.Stop(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	stopped := requireObservedState(t, manager, "ebus.primary", ObservedStopped, time.Second)
	if stopped.Generation != 0 {
		t.Fatalf("STOPPED generation = %d, want 0", stopped.Generation)
	}
	if err := manager.Start(context.Background(), "ebus.primary"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Start() while stale admission retires error = %v, want ErrUnavailable", err)
	}
	close(runtime.startRelease)
	deadline := time.Now().Add(time.Second)
	for {
		runtime.mu.Lock()
		stopCalls, running := runtime.stopCalls, runtime.running
		runtime.mu.Unlock()
		if stopCalls >= 2 && !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("late admission was not retired: stopCalls=%d running=%v", stopCalls, running)
		}
		time.Sleep(time.Millisecond)
	}
	after, _ := manager.Snapshot("ebus.primary")
	if after.ObservedState != ObservedStopped || after.Generation != 0 || len(after.EffectiveCapabilities) != 0 {
		t.Fatalf("late admission changed manager snapshot: %#v", after)
	}
	_ = manager.Shutdown(context.Background())
}

func TestManagerWithdrawalFencePrecedesStopAndReplace(t *testing.T) {
	for _, action := range []string{"stop", "replace"} {
		t.Run(action, func(t *testing.T) {
			runtime := &withdrawalFenceRuntime{}
			manager, err := New(Config{Drivers: []DriverConfig{{
				ID: "ebus.primary", Enabled: true, Runtime: runtime, Capabilities: []Capability{CapabilityRead},
			}}})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			switch action {
			case "stop":
				if err := manager.Stop(context.Background(), "ebus.primary"); err != nil {
					t.Fatalf("Stop() error = %v", err)
				}
			case "replace":
				if err := manager.Replace(context.Background(), "ebus.primary"); err != nil {
					t.Fatalf("Replace() error = %v", err)
				}
			}
			runtime.mu.Lock()
			stopSawFence := runtime.stopSawFence
			replaceSawFence := runtime.replaceSawFence
			runtime.mu.Unlock()
			if action == "stop" && !stopSawFence {
				t.Fatal("Runtime.Stop observed an unfenced external surface")
			}
			if action == "replace" && !replaceSawFence {
				t.Fatal("Runtime.Replace observed an unfenced external surface")
			}
			_ = manager.Shutdown(context.Background())
		})
	}
}

func TestManagerLateAdmissionCleanupFailureQuarantinesProcessEpoch(t *testing.T) {
	for _, test := range []struct {
		name            string
		lateStopError   error
		cleanupDeadline time.Duration
	}{
		{name: "deadline-dependent cleanup", cleanupDeadline: 10 * time.Millisecond},
		{name: "runtime safety quarantine", lateStopError: ErrSafetyQuarantined, cleanupDeadline: time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := &lateCleanupRuntime{
				startEntered:  make(chan struct{}),
				startRelease:  make(chan struct{}),
				lateStopError: test.lateStopError,
			}
			manager, err := New(Config{
				LateRetireTimeout: test.cleanupDeadline,
				Drivers: []DriverConfig{{
					ID: "ebus.primary", Enabled: true, Runtime: runtime, Capabilities: []Capability{CapabilityRead},
				}},
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if err := manager.StartAsync(context.Background(), "ebus.primary"); err != nil {
				t.Fatalf("StartAsync() error = %v", err)
			}
			select {
			case <-runtime.startEntered:
			case <-time.After(time.Second):
				t.Fatal("late runtime did not enter Start")
			}
			if err := manager.Stop(context.Background(), "ebus.primary"); err != nil {
				t.Fatalf("Stop() error = %v", err)
			}
			close(runtime.startRelease)
			failed := requireObservedState(t, manager, "ebus.primary", ObservedFailed, time.Second)
			if failed.DesiredState != DesiredStopped || failed.Reason.Code != ReasonCloseUnconfirmed || !failed.SafetyQuarantined || len(failed.EffectiveCapabilities) != 0 {
				t.Fatalf("late cleanup failure snapshot = %#v", failed)
			}
			if err := manager.Start(context.Background(), "ebus.primary"); !errors.Is(err, ErrSafetyQuarantined) {
				t.Fatalf("Start() after late cleanup failure error = %v, want ErrSafetyQuarantined", err)
			}
			runtime.mu.Lock()
			stopCalls := runtime.stopCalls
			runtime.mu.Unlock()
			if stopCalls != 2 {
				t.Fatalf("Runtime.Stop calls = %d, want initial stop plus bounded late retirement", stopCalls)
			}
		})
	}
}

func requireObservedState(t *testing.T, manager *Manager, id string, want ObservedState, timeout time.Duration) Snapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if snapshot, ok := manager.Snapshot(id); ok && snapshot.ObservedState == want {
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
	snapshot, _ := manager.Snapshot(id)
	t.Fatalf("driver %s state = %s, want %s; snapshot=%#v", id, snapshot.ObservedState, want, snapshot)
	return Snapshot{}
}

// --- S4 lifecycle hooks: admission hold and classified close ---

type hookRuntime struct {
	mu          sync.Mutex
	generation  uint64
	revision    uint64
	open        bool
	startErrs   []error
	stopErrs    []error
	quarantined bool
	starts      int
	replaces    int
	stops       int
}

func (runtime *hookRuntime) construct() (uint64, error) {
	if len(runtime.startErrs) > 0 {
		err := runtime.startErrs[0]
		runtime.startErrs = runtime.startErrs[1:]
		if err != nil {
			return 0, err
		}
	}
	runtime.generation++
	runtime.revision++
	runtime.open = true
	return runtime.generation, nil
}

func (runtime *hookRuntime) Start(context.Context) (uint64, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.starts++
	return runtime.construct()
}

func (runtime *hookRuntime) Replace(context.Context) (uint64, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.replaces++
	runtime.open = false
	return runtime.construct()
}

func (runtime *hookRuntime) Stop(context.Context) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.stops++
	var err error
	if len(runtime.stopErrs) > 0 {
		err = runtime.stopErrs[0]
		runtime.stopErrs = runtime.stopErrs[1:]
	}
	if errors.Is(err, ErrSafetyQuarantined) {
		runtime.quarantined = true
		return err
	}
	runtime.open = false
	runtime.revision++
	return err
}

func (runtime *hookRuntime) Generation() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.generation
}

func (runtime *hookRuntime) Revision() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.revision
}

func (runtime *hookRuntime) SafetyQuarantined() bool {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.quarantined
}

func (runtime *hookRuntime) Admit(context.Context) (*Admission, error) {
	runtime.mu.Lock()
	generation := runtime.generation
	runtime.mu.Unlock()
	return &Admission{Correlation: Correlation{Generation: generation}, Invoke: func(fn func(any) error) error { return fn(runtime) }, Release: func() error { return nil }}, nil
}

func (runtime *hookRuntime) counts() (starts, replaces, stops int) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.starts, runtime.replaces, runtime.stops
}

type closeLog struct {
	mu      sync.Mutex
	reports []CloseReport
}

func (log *closeLog) add(report CloseReport) {
	log.mu.Lock()
	log.reports = append(log.reports, report)
	log.mu.Unlock()
}

func (log *closeLog) list() []CloseReport {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]CloseReport(nil), log.reports...)
}

func newHookManager(t *testing.T, runtime Runtime, budget int, hooks LifecycleHooks) *Manager {
	t.Helper()
	manager, err := New(Config{Drivers: []DriverConfig{{
		ID: "ebus.primary", Enabled: true, Runtime: runtime,
		Capabilities: []Capability{CapabilityRead, CapabilityWrite},
		ClassifyError: func(error) Failure {
			return Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}
		},
		Retry: RetryPolicy{Budget: budget, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, JitterRatio: -1},
		Hooks: hooks,
	}}, LateRetireTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	return manager
}

// The hold keeps admission closed while the hook runs with no manager lock:
// STARTING, no effective capability and Invoke refused; snapshot and watch
// reads stay responsive. RUNNING and the capabilities then publish in one
// transition.
func TestLifecycleHookHoldAdmissionPublishesRunningAndCapabilitiesTogether(t *testing.T) {
	runtime := &hookRuntime{}
	entered := make(chan AdmissionHold, 1)
	release := make(chan struct{})
	manager := newHookManager(t, runtime, 1, LifecycleHooks{HoldAdmission: func(ctx context.Context, hold AdmissionHold) error {
		entered <- hold
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}})
	if err := manager.StartAsync(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("StartAsync() error = %v", err)
	}
	var hold AdmissionHold
	select {
	case hold = <-entered:
	case <-time.After(time.Second):
		t.Fatal("admission hold did not run")
	}
	if hold.DriverID != "ebus.primary" || hold.Correlation.Generation != 1 || hold.Correlation.Operation == 0 || hold.Previous != 0 || hold.Replacement || hold.Retry {
		t.Fatalf("hold = %#v", hold)
	}
	readDone := make(chan Snapshot, 1)
	go func() {
		snapshot, _, _ := manager.Watch("ebus.primary")
		readDone <- snapshot
	}()
	var held Snapshot
	select {
	case held = <-readDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Watch blocked while the admission hold ran: a manager lock is held")
	}
	if held.ObservedState != ObservedStarting || len(held.EffectiveCapabilities) != 0 || held.Generation != 0 {
		t.Fatalf("snapshot during hold = %#v, want STARTING without capability or generation", held)
	}
	if _, err := manager.Invoke(context.Background(), "ebus.primary", CapabilityRead, func(any) error { return nil }); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Invoke during hold error = %v, want ErrUnavailable", err)
	}
	close(release)
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if running.Revision != held.Revision+1 || running.Generation != 1 || !reflect.DeepEqual(running.EffectiveCapabilities, []Capability{CapabilityRead, CapabilityWrite}) {
		t.Fatalf("RUNNING = %#v, want generation and capabilities in the single transition after %d", running, held.Revision)
	}
	if _, err := manager.Invoke(context.Background(), "ebus.primary", CapabilityRead, func(any) error { return nil }); err != nil {
		t.Fatalf("Invoke after hold error = %v", err)
	}
}

// A failed hold retires the never-published generation on the independent
// deadline; a proven close fails the attempt through the manager's own
// retry policy, which constructs afresh and holds again.
func TestLifecycleHookHoldFailureRetiresGenerationAndKeepsManagerRetry(t *testing.T) {
	runtime := &hookRuntime{}
	closes := &closeLog{}
	var holds []AdmissionHold
	var mu sync.Mutex
	manager := newHookManager(t, runtime, 1, LifecycleHooks{
		HoldAdmission: func(_ context.Context, hold AdmissionHold) error {
			mu.Lock()
			defer mu.Unlock()
			holds = append(holds, hold)
			if len(holds) == 1 {
				return errors.New("hub obligation not met")
			}
			return nil
		},
		Closed: closes.add,
	})
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if running.Generation != 2 {
		t.Fatalf("admitted generation = %d, want the retry's generation 2", running.Generation)
	}
	starts, replaces, stops := runtime.counts()
	if starts != 2 || replaces != 0 || stops != 1 {
		t.Fatalf("runtime calls start=%d replace=%d stop=%d, want 2/0/1", starts, replaces, stops)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(holds) != 2 || holds[0].Correlation.Generation != 1 || holds[1].Correlation.Generation != 2 || holds[0].Retry || !holds[1].Retry {
		t.Fatalf("holds = %#v", holds)
	}
	if holds[0].Correlation.Operation != holds[1].Correlation.Operation {
		t.Fatalf("manager retry changed the operation: %#v", holds)
	}
	reports := closes.list()
	if len(reports) != 1 || reports[0].Class != CloseProven || reports[0].Generation != 1 {
		t.Fatalf("close reports = %#v, want one PROVEN retirement of generation 1", reports)
	}
}

func TestLifecycleHookHoldFailureWithUnprovenCloseQuarantines(t *testing.T) {
	runtime := &hookRuntime{stopErrs: []error{errors.New("close not confirmed")}}
	closes := &closeLog{}
	manager := newHookManager(t, runtime, 3, LifecycleHooks{
		HoldAdmission: func(context.Context, AdmissionHold) error { return errors.New("hold failed") },
		Closed:        closes.add,
	})
	_ = manager.Start(context.Background(), "ebus.primary")
	failed := requireObservedState(t, manager, "ebus.primary", ObservedFailed, time.Second)
	if failed.Reason.Code != ReasonCloseUnconfirmed || !failed.SafetyQuarantined || len(failed.EffectiveCapabilities) != 0 {
		t.Fatalf("snapshot = %#v, want FAILED/CLOSE_UNCONFIRMED quarantined", failed)
	}
	reports := closes.list()
	if len(reports) != 1 || reports[0].Class != CloseInternalError || reports[0].Class.Proven() {
		t.Fatalf("close reports = %#v, want one unproven INTERNAL_ERROR", reports)
	}
	if starts, _, _ := runtime.counts(); starts != 1 {
		t.Fatalf("starts = %d, want no retry after an unproven close", starts)
	}
}

// A Stop during the hold supersedes the operation: the held generation is
// never published and is retired on the independent deadline.
func TestLifecycleHookStopDuringHoldNeverPublishes(t *testing.T) {
	runtime := &hookRuntime{}
	entered := make(chan struct{})
	holdCtxDone := make(chan struct{})
	manager := newHookManager(t, runtime, 1, LifecycleHooks{HoldAdmission: func(ctx context.Context, _ AdmissionHold) error {
		close(entered)
		<-ctx.Done()
		close(holdCtxDone)
		return nil
	}})
	if err := manager.StartAsync(context.Background(), "ebus.primary"); err != nil {
		t.Fatal(err)
	}
	<-entered
	report, err := manager.StopWithReport(context.Background(), "ebus.primary")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-holdCtxDone:
	case <-time.After(time.Second):
		t.Fatal("hold context was not canceled by Stop")
	}
	if report.Class != CloseProven {
		t.Fatalf("stop report = %#v", report)
	}
	waitForCondition(t, func() bool {
		_, _, stops := runtime.counts()
		return stops == 2
	})
	snapshot, _ := manager.Snapshot("ebus.primary")
	if snapshot.ObservedState != ObservedStopped || snapshot.Generation != 0 || len(snapshot.EffectiveCapabilities) != 0 {
		t.Fatalf("snapshot = %#v, want STOPPED with the held generation never published", snapshot)
	}
}

func TestLifecycleHookManagerRetryHoldIsMarkedRetryAndReplacement(t *testing.T) {
	runtime := &hookRuntime{}
	holds := make(chan AdmissionHold, 4)
	manager := newHookManager(t, runtime, 2, LifecycleHooks{HoldAdmission: func(_ context.Context, hold AdmissionHold) error {
		holds <- hold
		return nil
	}})
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatal(err)
	}
	first := <-holds
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if !manager.ReportFailure("ebus.primary", Correlation{Generation: running.Generation}, Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}) {
		t.Fatal("failure report rejected")
	}
	var second AdmissionHold
	select {
	case second = <-holds:
	case <-time.After(time.Second):
		t.Fatal("manager retry did not hold admission")
	}
	if first.Retry || first.Replacement || !second.Retry || !second.Replacement || second.Previous != first.Correlation.Generation || second.Correlation.Generation != first.Correlation.Generation+1 {
		t.Fatalf("holds first=%#v second=%#v", first, second)
	}
	requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
}

func TestDriverStopWithReportCloseClasses(t *testing.T) {
	cases := []struct {
		name          string
		stopErr       error
		quarantineNil bool
		wantClass     CloseClass
		wantObserved  ObservedState
		wantReason    ReasonCode
	}{
		{name: "nil", wantClass: CloseProven, wantObserved: ObservedStopped, wantReason: ReasonOperatorStopped},
		{name: "drain timeout with confirmed close", stopErr: ErrStopTimeout, wantClass: CloseProvenAfterDrainTimeout, wantObserved: ObservedStopped, wantReason: ReasonStopTimeout},
		{name: "quarantine", stopErr: ErrSafetyQuarantined, wantClass: CloseUnconfirmed, wantObserved: ObservedFailed, wantReason: ReasonCloseUnconfirmed},
		{name: "internal error", stopErr: errors.New("boom"), wantClass: CloseInternalError, wantObserved: ObservedFailed, wantReason: ReasonInternalError},
		// The observed transition keeps its characterized precedence; the
		// class never trusts the nil result over runtime quarantine.
		{name: "nil with runtime quarantine", quarantineNil: true, wantClass: CloseUnconfirmed, wantObserved: ObservedStopped, wantReason: ReasonOperatorStopped},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runtime := &hookRuntime{stopErrs: []error{tc.stopErr}}
			closes := &closeLog{}
			manager := newHookManager(t, runtime, 0, LifecycleHooks{Closed: closes.add})
			if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
				t.Fatal(err)
			}
			requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
			if tc.quarantineNil {
				runtime.mu.Lock()
				runtime.quarantined = true
				runtime.mu.Unlock()
			}
			report, err := manager.StopWithReport(context.Background(), "ebus.primary")
			if err != nil {
				t.Fatal(err)
			}
			if report.Class != tc.wantClass || report.Observed != tc.wantObserved || report.Reason.Code != tc.wantReason || report.Generation != 1 || report.Operation == 0 {
				t.Fatalf("report = %#v", report)
			}
			if report.Class.Proven() != (tc.wantClass == CloseProven || tc.wantClass == CloseProvenAfterDrainTimeout) {
				t.Fatalf("Proven() = %v for %s", report.Class.Proven(), report.Class)
			}
			if got := closes.list(); len(got) != 1 || got[0] != report {
				t.Fatalf("Closed hook reports = %#v, want the returned report", got)
			}
		})
	}
	t.Run("already stopped", func(t *testing.T) {
		manager := newHookManager(t, &hookRuntime{}, 0, LifecycleHooks{})
		report, _ := manager.StopWithReport(context.Background(), "ebus.primary")
		if report.Class != CloseNotRequired || !report.Class.Proven() {
			t.Fatalf("report = %#v", report)
		}
	})
	t.Run("quarantined manager", func(t *testing.T) {
		runtime := &hookRuntime{stopErrs: []error{ErrSafetyQuarantined}}
		manager := newHookManager(t, runtime, 0, LifecycleHooks{})
		_ = manager.Start(context.Background(), "ebus.primary")
		_, _ = manager.StopWithReport(context.Background(), "ebus.primary")
		report, _ := manager.StopWithReport(context.Background(), "ebus.primary")
		if report.Class != CloseUnconfirmed {
			t.Fatalf("repeated stop on quarantine = %#v", report)
		}
	})
	t.Run("internal error is retried by a later stop", func(t *testing.T) {
		runtime := &hookRuntime{stopErrs: []error{errors.New("boom"), nil}}
		manager := newHookManager(t, runtime, 0, LifecycleHooks{})
		_ = manager.Start(context.Background(), "ebus.primary")
		first, _ := manager.StopWithReport(context.Background(), "ebus.primary")
		second, _ := manager.StopWithReport(context.Background(), "ebus.primary")
		if first.Class != CloseInternalError || second.Class != CloseProven {
			t.Fatalf("first=%#v second=%#v", first, second)
		}
	})
	t.Run("stopped with construction in flight", func(t *testing.T) {
		manager := newHookManager(t, &hookRuntime{}, 0, LifecycleHooks{})
		// A canceled construction that ignored its context still owns the
		// attempt slot while the driver already reads STOPPED.
		driver := manager.drivers["ebus.primary"]
		driver.mu.Lock()
		driver.activeAttempt = &attemptControl{cancel: func() {}}
		driver.mu.Unlock()
		report, _ := manager.StopWithReport(context.Background(), "ebus.primary")
		if report.Class != CloseInternalError || report.Class.Proven() {
			t.Fatalf("report = %#v, want no proof while a construction is in flight", report)
		}
	})
}

func TestDriverClassifyCloseTable(t *testing.T) {
	for _, tc := range []struct {
		err         error
		quarantined bool
		want        CloseClass
	}{
		{nil, false, CloseProven},
		{ErrStopTimeout, false, CloseProvenAfterDrainTimeout},
		{errors.Join(ErrStopTimeout, errors.New("native")), false, CloseProvenAfterDrainTimeout},
		{ErrSafetyQuarantined, false, CloseUnconfirmed},
		{nil, true, CloseUnconfirmed},
		{ErrStopTimeout, true, CloseUnconfirmed},
		{errors.New("other"), false, CloseInternalError},
	} {
		if got := ClassifyClose(tc.err, tc.quarantined); got != tc.want {
			t.Fatalf("ClassifyClose(%v, %v) = %s, want %s", tc.err, tc.quarantined, got, tc.want)
		}
	}
	if CloseInternalError.Proven() || CloseUnconfirmed.Proven() || !CloseNotRequired.Proven() {
		t.Fatal("Proven() classification is wrong")
	}
}

func TestDriverBindHooksOnceAndWatch(t *testing.T) {
	manager := newHookManager(t, &hookRuntime{}, 0, LifecycleHooks{})
	if err := manager.BindHooks("missing", LifecycleHooks{}); !errors.Is(err, ErrDriverNotFound) {
		t.Fatalf("BindHooks(missing) = %v", err)
	}
	if err := manager.BindHooks("ebus.primary", LifecycleHooks{Closed: func(CloseReport) {}}); err != nil {
		t.Fatalf("BindHooks() = %v", err)
	}
	if err := manager.BindHooks("ebus.primary", LifecycleHooks{}); !errors.Is(err, ErrHooksBound) {
		t.Fatalf("second BindHooks() = %v, want ErrHooksBound", err)
	}
	snapshot, changed, ok := manager.Watch("ebus.primary")
	if !ok || snapshot.ObservedState != ObservedStopped {
		t.Fatalf("Watch() = %#v %v", snapshot, ok)
	}
	select {
	case <-changed:
		t.Fatal("watch channel closed without a transition")
	default:
	}
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	case <-time.After(time.Second):
		t.Fatal("watch channel not closed by a transition")
	}
	if _, _, ok := manager.Watch("missing"); ok {
		t.Fatal("Watch(missing) reported a driver")
	}
}

func waitForCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLifecycleHookRepeatedStopUnderRuntimeQuarantineStaysUnconfirmed(t *testing.T) {
	runtime := &hookRuntime{}
	manager := newHookManager(t, runtime, 0, LifecycleHooks{})
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatal(err)
	}
	requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	runtime.mu.Lock()
	runtime.quarantined = true
	runtime.mu.Unlock()
	for i := 0; i < 3; i++ {
		report, err := manager.StopWithReport(context.Background(), "ebus.primary")
		if err != nil {
			t.Fatal(err)
		}
		if report.Class != CloseUnconfirmed || report.Class.Proven() {
			t.Fatalf("stop %d report = %#v, want UNCONFIRMED while runtime quarantine remains", i, report)
		}
	}
}

func TestLifecycleHookShutdownDeliversClosedReport(t *testing.T) {
	runtime := &hookRuntime{}
	closes := &closeLog{}
	manager := newHookManager(t, runtime, 0, LifecycleHooks{Closed: closes.add})
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatal(err)
	}
	requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	reports := closes.list()
	if len(reports) != 1 || reports[0].Class != CloseProven || reports[0].Generation != 1 {
		t.Fatalf("shutdown close reports = %#v, want one PROVEN report for generation 1", reports)
	}
}

func TestLifecycleHookLateAdmissionCloseReportCarriesCorrelation(t *testing.T) {
	runtime := &lateCleanupRuntime{startEntered: make(chan struct{}), startRelease: make(chan struct{})}
	closes := &closeLog{}
	manager, err := New(Config{
		LateRetireTimeout: time.Second,
		Drivers: []DriverConfig{{
			ID: "ebus.primary", Enabled: true, Runtime: runtime, Capabilities: []Capability{CapabilityRead},
			Hooks: LifecycleHooks{Closed: closes.add},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.StartAsync(context.Background(), "ebus.primary"); err != nil {
		t.Fatal(err)
	}
	<-runtime.startEntered
	if err := manager.Stop(context.Background(), "ebus.primary"); err != nil {
		t.Fatal(err)
	}
	close(runtime.startRelease)
	deadline := time.Now().Add(5 * time.Second)
	for len(closes.list()) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	reports := closes.list()
	if len(reports) != 2 {
		t.Fatalf("close reports = %#v, want the stop and the late retirement", reports)
	}
	late := reports[1]
	if late.Generation != 1 || late.Operation == 0 {
		t.Fatalf("late retirement report = %#v, want the late generation and its originating operation", late)
	}
}
