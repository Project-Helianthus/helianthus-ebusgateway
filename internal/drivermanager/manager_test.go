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
	startCalls   int
	stopCalls    int
	generation   uint64
	revision     uint64
	quarantined  bool
}

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
	return nil
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
