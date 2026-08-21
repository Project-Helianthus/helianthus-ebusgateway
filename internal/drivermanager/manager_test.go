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
