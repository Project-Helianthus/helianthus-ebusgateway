package main

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

// issue1432Runtime is a fake native driver whose first failures starts fail,
// as an endpoint that is not up yet. A negative count fails every start.
type issue1432Runtime struct {
	failures   atomic.Int32
	starts     atomic.Int32
	generation atomic.Uint64
	running    atomic.Bool
}

func (r *issue1432Runtime) Start(context.Context) (uint64, error) {
	r.starts.Add(1)
	if n := r.failures.Load(); n != 0 {
		if n > 0 {
			r.failures.Add(-1)
		}
		return 0, errors.New("synthetic endpoint offline")
	}
	r.running.Store(true)
	return r.generation.Add(1), nil
}
func (r *issue1432Runtime) Stop(context.Context) error { r.running.Store(false); return nil }
func (r *issue1432Runtime) Replace(ctx context.Context) (uint64, error) {
	_ = r.Stop(ctx)
	return r.Start(ctx)
}
func (r *issue1432Runtime) Generation() uint64    { return r.generation.Load() }
func (r *issue1432Runtime) Revision() uint64      { return r.generation.Load() }
func (*issue1432Runtime) SafetyQuarantined() bool { return false }

// issue1432Compose replaces every sidecar adapter with a fake runtime, all
// enabled; failures[id] sets that driver's initial failed starts.
func issue1432Compose(t *testing.T, policy modbusStartRetryPolicy, failures map[runtimecontrol.DriverID]int32) map[runtimecontrol.DriverID]*issue1432Runtime {
	t.Helper()
	originalDescriptors, originalPolicy := gatewayManagedSidecarDescriptorsFor, gatewayModbusStartRetryPolicy
	t.Cleanup(func() {
		gatewayManagedSidecarDescriptorsFor, gatewayModbusStartRetryPolicy = originalDescriptors, originalPolicy
	})
	gatewayModbusStartRetryPolicy = policy
	runtimes := map[runtimecontrol.DriverID]*issue1432Runtime{}
	for _, id := range []runtimecontrol.DriverID{managedSunSpecID, managedGrowattID, managedEEBusID} {
		runtimes[id] = &issue1432Runtime{}
		runtimes[id].failures.Store(failures[id])
	}
	gatewayManagedSidecarDescriptorsFor = func(descriptors []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
		for i := range descriptors {
			id := descriptors[i].DriverID
			descriptors[i].Disposition = runtimecontrol.DispositionEnabled
			descriptors[i].Adapter = runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*issue1432Runtime, error) {
				return runtimes[id], nil
			})
		}
		return descriptors
	}
	return runtimes
}

// issue1432Await waits for a status of id that satisfies want.
func issue1432Await(t *testing.T, service *runtimecontrol.Service, id runtimecontrol.DriverID, want func(runtimecontrol.DriverStatus) bool) runtimecontrol.DriverStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
	defer cancel()
	for {
		changed := service.EventNotify()
		status, err := service.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if want(status) {
			return status
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("%s: condition not reached; last %+v", id, status)
		}
	}
}

// A Modbus driver whose endpoint comes up after the gateway recovers without
// an operator: each failed START is re-issued after the bounded backoff, the
// retries advance the desired revision and are logged, and the sibling that
// started at once is never restarted.
func TestIssue1432FailedModbusStartRecoversAfterBackoff(t *testing.T) {
	logs := captureIssue1367Logs(t)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(logs.text())
		}
	})
	natives := issue1432Compose(t, modbusStartRetryPolicy{Initial: 20 * time.Millisecond, Max: 40 * time.Millisecond}, map[runtimecontrol.DriverID]int32{managedSunSpecID: 3})
	process := newRCtlEnv(t).start(rctlProcessOptions{})
	service := process.control.Service()
	running := issue1432Await(t, service, managedSunSpecID, func(s runtimecontrol.DriverStatus) bool {
		return s.ObservedState == drivermanager.ObservedRunning
	})
	if got := natives[managedSunSpecID].starts.Load(); got != 4 || !natives[managedSunSpecID].running.Load() {
		t.Fatalf("starts=%d running=%v, want 4 starts and running", got, natives[managedSunSpecID].running.Load())
	}
	// The boot START is revision 1 at most; three accepted RESTARTs follow.
	if running.DesiredRevision < 4 || running.DesiredState != runtimecontrol.DesiredRunning {
		t.Fatalf("retries not visible in the control view: %+v", running)
	}
	text := logs.text()
	for attempt, delay := range []string{"20ms", "40ms", "40ms"} {
		line := "Modbus driver start failed; retrying driver=modbus.sunspec reason=PROVIDER_UNAVAILABLE attempt=" + string(rune('1'+attempt)) + " delay=" + delay
		if !strings.Contains(text, line) {
			t.Fatalf("missing log line %q in:\n%s", line, text)
		}
	}
	if strings.Contains(text, "attempt=4") {
		t.Fatal("a retry was scheduled after RUNNING")
	}
	for _, id := range []runtimecontrol.DriverID{managedGrowattID, managedEEBusID} {
		if natives[id].starts.Load() != 1 {
			t.Fatalf("sibling %s restarted: starts=%d", id, natives[id].starts.Load())
		}
	}
}

// An operator STOP during the backoff cancels the scheduled retry: the
// driver stays STOPPED and no further start reaches the native driver.
func TestIssue1432StopCancelsScheduledRetry(t *testing.T) {
	natives := issue1432Compose(t, modbusStartRetryPolicy{Initial: 300 * time.Millisecond, Max: 300 * time.Millisecond}, map[runtimecontrol.DriverID]int32{managedGrowattID: -1})
	process := newRCtlEnv(t).start(rctlProcessOptions{})
	service := process.control.Service()
	failed := issue1432Await(t, service, managedGrowattID, func(s runtimecontrol.DriverStatus) bool {
		return s.ObservedState == drivermanager.ObservedFailed
	})
	ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
	defer cancel()
	if _, err := service.Decide(ctx, runtimecontrol.MethodStop, runtimecontrol.Request{DriverID: managedGrowattID, ExpectedRevision: failed.DesiredRevision}); err != nil {
		t.Fatal(err)
	}
	stopped := issue1432Await(t, service, managedGrowattID, func(s runtimecontrol.DriverStatus) bool {
		return s.DesiredState == runtimecontrol.DesiredStopped && s.ObservedState == drivermanager.ObservedStopped
	})
	starts := natives[managedGrowattID].starts.Load()
	time.Sleep(3 * 300 * time.Millisecond)
	after, err := service.Get(managedGrowattID)
	if err != nil {
		t.Fatal(err)
	}
	if got := natives[managedGrowattID].starts.Load(); got != starts || after.DesiredRevision != stopped.DesiredRevision || after.DesiredState != runtimecontrol.DesiredStopped {
		t.Fatalf("retry ran after STOP: starts %d -> %d, view %+v", starts, got, after)
	}
}

// Closing admission joins the supervisor; no retry is decided afterwards.
func TestIssue1432CloseAdmissionEndsRetries(t *testing.T) {
	natives := issue1432Compose(t, modbusStartRetryPolicy{Initial: 50 * time.Millisecond, Max: 50 * time.Millisecond}, map[runtimecontrol.DriverID]int32{managedSunSpecID: -1})
	process := newRCtlEnv(t).start(rctlProcessOptions{})
	issue1432Await(t, process.control.Service(), managedSunSpecID, func(s runtimecontrol.DriverStatus) bool {
		return s.ObservedState == drivermanager.ObservedFailed
	})
	ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
	defer cancel()
	if err := process.control.CloseAdmission(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-process.control.modbusRetry.done:
	default:
		t.Fatal("supervisor still running after CloseAdmission")
	}
	starts := natives[managedSunSpecID].starts.Load()
	time.Sleep(4 * 50 * time.Millisecond)
	if got := natives[managedSunSpecID].starts.Load(); got != starts {
		t.Fatalf("start after CloseAdmission: %d -> %d", starts, got)
	}
}

func TestIssue1432BackoffDoublesToBound(t *testing.T) {
	policy := modbusStartRetryPolicy{Initial: 5 * time.Second, Max: 5 * time.Minute}
	want := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 160 * time.Second, 5 * time.Minute, 5 * time.Minute}
	for i, w := range want {
		if got := policy.delay(i + 1); got != w {
			t.Fatalf("delay(%d)=%s, want %s", i+1, got, w)
		}
	}
	if got := policy.delay(1 << 20); got != 5*time.Minute {
		t.Fatalf("delay is unbounded: %s", got)
	}
	if gatewayModbusStartRetryPolicy != policy {
		t.Fatalf("production policy changed: %+v", gatewayModbusStartRetryPolicy)
	}
}

// Every field of the status that the retry decision reads is varied alone
// from a retryable status; each variation must stop the retry.
func TestIssue1432RetryableStatusVariesEveryField(t *testing.T) {
	base := runtimecontrol.DriverStatus{Enabled: true, DesiredState: runtimecontrol.DesiredRunning, ObservedState: drivermanager.ObservedFailed, Reason: drivermanager.ReasonProviderUnavailable}
	if !modbusStartRetryable(base) {
		t.Fatal("base status not retryable")
	}
	for _, reason := range []drivermanager.ReasonCode{drivermanager.ReasonDependencyUnavailable, drivermanager.ReasonRuntimeNotReady, drivermanager.ReasonRetryExhausted} {
		s := base
		s.Reason = reason
		if !modbusStartRetryable(s) {
			t.Fatalf("reason %s not retried", reason)
		}
	}
	refused := map[string]func(*runtimecontrol.DriverStatus){
		"disabled":         func(s *runtimecontrol.DriverStatus) { s.Enabled = false },
		"desired stopped":  func(s *runtimecontrol.DriverStatus) { s.DesiredState = runtimecontrol.DesiredStopped },
		"observed running": func(s *runtimecontrol.DriverStatus) { s.ObservedState = drivermanager.ObservedRunning },
		"observed backoff": func(s *runtimecontrol.DriverStatus) { s.ObservedState = drivermanager.ObservedBackoff },
		"quarantined":      func(s *runtimecontrol.DriverStatus) { s.SafetyQuarantined = true },
		"config invalid":   func(s *runtimecontrol.DriverStatus) { s.Reason = drivermanager.ReasonConfigInvalid },
		"internal error":   func(s *runtimecontrol.DriverStatus) { s.Reason = drivermanager.ReasonInternalError },
		"close unconfirmed": func(s *runtimecontrol.DriverStatus) {
			s.Reason = drivermanager.ReasonCloseUnconfirmed
		},
	}
	for name, mutate := range refused {
		s := base
		mutate(&s)
		if modbusStartRetryable(s) {
			t.Fatalf("%s retried", name)
		}
	}
}

// The supervised set is the composed Modbus drivers only: eeBUS keeps its
// own policy, and a withheld Modbus driver is never supervised.
func TestIssue1432SupervisesComposedModbusDriversOnly(t *testing.T) {
	issue1432Compose(t, gatewayModbusStartRetryPolicy, nil)
	cfg := ebusgateway.DefaultConfig()
	cfg.ModbusTCPConfig = ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: "tcp://fixture.invalid:502", DialTimeout: time.Second}
	sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), rctlGUID, nil, nil)
	ids := sidecars.modbusStartRetryIDs()
	slices.Sort(ids)
	if want := []runtimecontrol.DriverID{managedGrowattID, managedSunSpecID}; !slices.Equal(ids, want) {
		t.Fatalf("supervised %v, want %v", ids, want)
	}
	for _, s := range sidecars.entries {
		if s.descriptor.DriverID == managedSunSpecID {
			s.withheld = errModbusTCPFamilyNotAdmitted
		}
	}
	if ids := sidecars.modbusStartRetryIDs(); !slices.Equal(ids, []runtimecontrol.DriverID{managedGrowattID}) {
		t.Fatalf("withheld driver supervised: %v", ids)
	}
}

// A failed START is superseded by a later generation of its epoch or by the
// epoch of a later decision; every other variation of the correlation, and a
// START that has not completed or did not fail, is refused.
func TestIssue1432SupersedesFailedVariesEveryField(t *testing.T) {
	failed := runtimecontrol.LifecycleCorrelation{DriverID: managedSunSpecID, SourceID: "modbus.sunspec:fixture", Epoch: "fixture.modbus.sunspec.1", Generation: 2, Sequence: 3}
	sidecar := func(done, failedStart bool) *managedSidecar {
		s := &managedSidecar{key: failed, started: make(chan struct{})}
		if done {
			close(s.started)
		}
		if failedStart {
			s.startErr = errors.New("synthetic failed start")
		}
		return s
	}
	cases := []struct {
		name   string
		mutate func(*runtimecontrol.LifecycleCorrelation)
		want   bool
	}{
		{"later generation, same epoch", func(c *runtimecontrol.LifecycleCorrelation) { c.Generation = 3 }, true},
		{"same generation, same epoch", func(*runtimecontrol.LifecycleCorrelation) {}, false},
		{"earlier generation, same epoch", func(c *runtimecontrol.LifecycleCorrelation) { c.Generation = 1 }, false},
		{"new epoch, later sequence", func(c *runtimecontrol.LifecycleCorrelation) {
			c.Epoch, c.Generation, c.Sequence = "fixture.modbus.sunspec.2", 1, 4
		}, true},
		{"new epoch, same sequence", func(c *runtimecontrol.LifecycleCorrelation) { c.Epoch, c.Generation = "fixture.modbus.sunspec.2", 1 }, false},
		{"new epoch, earlier sequence", func(c *runtimecontrol.LifecycleCorrelation) {
			c.Epoch, c.Generation, c.Sequence = "fixture.modbus.sunspec.2", 1, 2
		}, false},
		{"other driver", func(c *runtimecontrol.LifecycleCorrelation) { c.DriverID, c.Generation = managedGrowattID, 3 }, false},
		{"other source", func(c *runtimecontrol.LifecycleCorrelation) { c.SourceID, c.Generation = "modbus.sunspec:other", 3 }, false},
	}
	for _, tc := range cases {
		c := failed
		tc.mutate(&c)
		if got := sidecar(true, true).supersedesFailedLocked(c); got != tc.want {
			t.Fatalf("%s: supersedes=%v, want %v", tc.name, got, tc.want)
		}
		if tc.want {
			if sidecar(false, true).supersedesFailedLocked(c) {
				t.Fatalf("%s: superseded a START still in flight", tc.name)
			}
			if sidecar(true, false).supersedesFailedLocked(c) {
				t.Fatalf("%s: superseded a START that succeeded", tc.name)
			}
		}
	}
}

// A resync after event-log overflow replaces a scheduled retry that belongs to
// an older desired revision, so the failure standing in the catalog is retried
// instead of the obsolete one being discarded when it fires.
func TestIssue1432ResyncReconcilesStaleScheduledRevision(t *testing.T) {
	issue1432Compose(t, modbusStartRetryPolicy{Initial: time.Hour, Max: time.Hour}, map[runtimecontrol.DriverID]int32{managedSunSpecID: -1})
	process := newRCtlEnv(t).start(rctlProcessOptions{})
	service := process.control.Service()
	failed := issue1432Await(t, service, managedSunSpecID, func(s runtimecontrol.DriverStatus) bool {
		return s.ObservedState == drivermanager.ObservedFailed
	})
	stale := failed.DesiredRevision + 7
	retry := &modbusStartRetry{
		service: service,
		policy:  modbusStartRetryPolicy{Initial: time.Hour, Max: time.Hour},
		now:     time.Now,
		drivers: map[runtimecontrol.DriverID]*modbusStartRetryDriver{
			managedSunSpecID: {attempts: 1, revision: stale, due: time.Now().Add(time.Hour)},
		},
	}
	retry.resync()
	state := retry.drivers[managedSunSpecID]
	if state.revision != failed.DesiredRevision || state.due.IsZero() || state.attempts != 2 {
		t.Fatalf("stale scheduled revision kept: %+v, want revision %d rescheduled", *state, failed.DesiredRevision)
	}
}

func TestIssue1432GrowattInvalidConfigIsNotRetried(t *testing.T) {
	sidecar := &managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: managedGrowattID}}
	config := ebusgateway.GrowattBMSRS485Config{Enabled: true}
	_, err := startGrowattBMSRS485RuntimeWithEndpoint(config, nil, nil, nil)
	if err == nil {
		t.Fatal("invalid enabled configuration started")
	}
	failure := sidecar.classifyStartFailure(err)
	if failure.Reason.Code != drivermanager.ReasonConfigInvalid {
		t.Fatalf("reason = %q, want %q", failure.Reason.Code, drivermanager.ReasonConfigInvalid)
	}
	if modbusStartRetryable(runtimecontrol.DriverStatus{Enabled: true, DesiredState: runtimecontrol.DesiredRunning, ObservedState: drivermanager.ObservedFailed, Reason: failure.Reason.Code}) {
		t.Fatal("configuration failure is retryable")
	}
	if got := sidecar.classifyStartFailure(errors.New("dial failed")).Reason.Code; got != drivermanager.ReasonProviderUnavailable {
		t.Fatalf("transport failure reason = %q", got)
	}
}

// Exercise lifecycle binding and failed-attempt retirement with the native
// driver; replacing the driver itself with a fake hides retained ownership.
func TestIssue1432NativeModbusDialFailureRecovers(t *testing.T) {
	originalDescriptors, originalPolicy := gatewayManagedSidecarDescriptorsFor, gatewayModbusStartRetryPolicy
	t.Cleanup(func() {
		gatewayManagedSidecarDescriptorsFor, gatewayModbusStartRetryPolicy = originalDescriptors, originalPolicy
	})
	gatewayModbusStartRetryPolicy = modbusStartRetryPolicy{Initial: 20 * time.Millisecond, Max: 40 * time.Millisecond}
	var dials atomic.Int32
	endpoint := &m03Endpoint{}
	hub := newTestSemanticHub(t)
	cfg := ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: "tcp://fixture.invalid:502", DialTimeout: time.Second}
	gatewayManagedSidecarDescriptorsFor = func(descriptors []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
		for i := range descriptors {
			if descriptors[i].DriverID != managedSunSpecID {
				continue
			}
			descriptors[i].Disposition = runtimecontrol.DispositionEnabled
			descriptors[i].Adapter = newModbusManagedDriverAdapter(cfg, hub,
				func(context.Context, string, string) (net.Conn, error) {
					if dials.Add(1) <= 3 {
						return nil, errors.New("synthetic endpoint offline")
					}
					client, peer := net.Pipe()
					_ = peer.Close()
					return client, nil
				}, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) { return endpoint, nil }, nil)
		}
		return descriptors
	}
	process := newRCtlEnv(t).start(rctlProcessOptions{})
	running := issue1432Await(t, process.control.Service(), managedSunSpecID, func(s runtimecontrol.DriverStatus) bool {
		if s.ObservedState == drivermanager.ObservedFailed && s.Reason != drivermanager.ReasonProviderUnavailable {
			t.Fatalf("native retry failed permanently: %+v", s)
		}
		return s.ObservedState == drivermanager.ObservedRunning
	})
	if dials.Load() != 4 || endpoint.opens.Load() != 1 || running.DesiredRevision < 4 {
		t.Fatalf("dials=%d opens=%d status=%+v, want four dials and a running successor", dials.Load(), endpoint.opens.Load(), running)
	}
}

type issue1432RetirementRuntime struct {
	issue1432Runtime
	stopErr     error
	quarantined bool
	binds       int
}

func (r *issue1432RetirementRuntime) Stop(context.Context) error { return r.stopErr }
func (r *issue1432RetirementRuntime) SafetyQuarantined() bool    { return r.quarantined }
func (r *issue1432RetirementRuntime) BindLifecycle(runtimecontrol.LifecycleCorrelation) error {
	r.binds++
	return nil
}

func TestIssue1432FailedRetirementRefusesSuccessor(t *testing.T) {
	for _, quarantined := range []bool{false, true} {
		name := "stop error"
		if quarantined {
			name = "quarantined"
		}
		t.Run(name, func(t *testing.T) {
			native := &issue1432RetirementRuntime{quarantined: quarantined}
			if !quarantined {
				native.stopErr = errors.New("synthetic close failure")
			}
			runtime := &managedSidecarRuntime{Runtime: native}
			manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: string(managedSunSpecID), Enabled: true, Runtime: runtime}}})
			if err != nil {
				t.Fatal(err)
			}
			predecessor := runtimecontrol.LifecycleCorrelation{DriverID: managedSunSpecID, SourceID: "modbus.sunspec:fixture", Epoch: "epoch:failed", Generation: 1, Sequence: 1}
			done := make(chan struct{})
			close(done)
			sidecar := &managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: managedSunSpecID, Sources: []runtimecontrol.SourceID{predecessor.SourceID}}, runtime: runtime, manager: manager, key: predecessor, started: done, startErr: errors.New("synthetic start failure")}
			successor := predecessor
			successor.Epoch, successor.Sequence = "epoch:retry", 2
			err = sidecar.Start(context.Background(), successor)
			var failure *managedSidecarStartError
			if !errors.As(err, &failure) || failure.ReasonCode() != drivermanager.ReasonCloseUnconfirmed || failure.Retryable() {
				t.Fatalf("unproven retirement error = %v, want non-retryable close failure", err)
			}
			if native.binds != 0 || native.starts.Load() != 0 || sidecar.key != predecessor || sidecar.started != done || sidecar.retired[sidecarGenerationKey(predecessor)] {
				t.Fatal("unproven predecessor was retired or successor was admitted")
			}
		})
	}
}

// A failed boot followed by a RESTART can prove native close while semantic
// cleanup remains stuck. That is not an exhausted start and must not loop.
type issue1432StalledCleanupExecutor struct{}

func (issue1432StalledCleanupExecutor) Start(context.Context, runtimecontrol.LifecycleCorrelation) error {
	return errors.New("synthetic start failure")
}

func (issue1432StalledCleanupExecutor) Stop(context.Context, runtimecontrol.LifecycleCorrelation) runtimecontrol.StopOutcome {
	return runtimecontrol.StopOutcome{NativeClosed: true}
}

func TestIssue1432CleanupStallIsNotRetried(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
	t.Cleanup(cancel)
	store := runtimestate.New(runtimestate.Options{Path: filepath.Join(t.TempDir(), "runtime_state.json")})
	if _, err := store.Load(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Stop(ctx) })
	if err := store.EagerPersistInstanceGUID(ctx, rctlGUID, runtimestate.IdentitySourceRuntimeState); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BootstrapControl(ctx, []runtimestate.ControlDriverConfig{{DriverID: string(managedSunSpecID), Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	service, err := runtimecontrol.New(runtimecontrol.Options{
		Store: &runtimeControlStore{m: store}, Recovery: rctlNoRecovery{}, Incarnation: "fixture-cleanup",
		Drivers:     []runtimecontrol.DriverConfig{{DriverID: managedSunSpecID, Executor: issue1432StalledCleanupExecutor{}}},
		MaxAttempts: 2, RetryDelay: func(int) time.Duration { return time.Millisecond },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)
	failed := issue1432Await(t, service, managedSunSpecID, func(s runtimecontrol.DriverStatus) bool {
		return s.ObservedState == drivermanager.ObservedFailed
	})
	if _, err := service.Decide(ctx, runtimecontrol.MethodRestart, runtimecontrol.Request{DriverID: managedSunSpecID, ExpectedRevision: failed.DesiredRevision}); err != nil {
		t.Fatal(err)
	}
	newRCtlEnv(t).waitFor("cleanup stall diagnostic", func() bool {
		for _, diagnostic := range service.Diagnostics(managedSunSpecID) {
			if diagnostic.Outcome == "CLEANUP_STALLED" {
				return true
			}
		}
		return false
	})
	stalled, err := service.Get(managedSunSpecID)
	if err != nil {
		t.Fatal(err)
	}
	if stalled.Reason != drivermanager.ReasonRetryExhausted || stalled.SafetyQuarantined || !modbusStartRetryable(stalled) {
		t.Fatalf("fixture must expose non-quarantined start-like exhaustion: %+v", stalled)
	}
	now := time.Now()
	state := &modbusStartRetryDriver{}
	retry := &modbusStartRetry{service: service, policy: modbusStartRetryPolicy{Initial: time.Millisecond, Max: time.Millisecond}, now: func() time.Time { return now },
		drivers: map[runtimecontrol.DriverID]*modbusStartRetryDriver{managedSunSpecID: state}}
	retry.resync()
	if !state.due.IsZero() {
		t.Fatal("resync scheduled cleanup-stalled driver")
	}
	retry.observe(runtimecontrol.Event{Kind: runtimecontrol.EventObservedChanged, DriverID: managedSunSpecID,
		DesiredState: stalled.DesiredState, DesiredRevision: stalled.DesiredRevision, ObservedState: stalled.ObservedState, Reason: stalled.Reason})
	if !state.due.IsZero() {
		t.Fatal("event scheduled cleanup-stalled driver")
	}
	// A retry scheduled before cleanup stalled is discarded when it fires.
	state.revision, state.due = stalled.DesiredRevision, now
	if next := retry.fire(ctx); !next.IsZero() || !state.due.IsZero() {
		t.Fatal("due retry rescheduled cleanup-stalled driver")
	}
}

// A transient admission refusal retries the same failed START. It must not
// advance the failure series or its backoff before another START actually fails.
type issue1432BusyStore struct {
	runtimecontrol.Store
	busy atomic.Int32
}

func (s *issue1432BusyStore) Decide(ctx context.Context, method runtimecontrol.Method, req runtimecontrol.Request, planner runtimecontrol.Planner) (runtimecontrol.DesiredView, error) {
	if s.busy.Load() > 0 {
		s.busy.Add(-1)
		return runtimecontrol.DesiredView{}, &runtimecontrol.Refusal{Code: runtimecontrol.RefusalTransitionBusy, Row: 8, Reason: "terminal work slot is being pruned"}
	}
	return s.Store.Decide(ctx, method, req, planner)
}

type issue1432RecoveringExecutor struct{ rctlCountingExecutor }

func (e *issue1432RecoveringExecutor) Start(context.Context, runtimecontrol.LifecycleCorrelation) error {
	if e.starts.Add(1) == 1 {
		return &managedSidecarStartError{reason: drivermanager.ReasonProviderUnavailable}
	}
	return nil
}

func TestIssue1432BusyDecisionKeepsFailedStartCount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
	t.Cleanup(cancel)
	store := runtimestate.New(runtimestate.Options{Path: filepath.Join(t.TempDir(), "runtime_state.json")})
	if _, err := store.Load(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Stop(ctx) })
	if err := store.EagerPersistInstanceGUID(ctx, rctlGUID, runtimestate.IdentitySourceRuntimeState); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BootstrapControl(ctx, []runtimestate.ControlDriverConfig{{DriverID: string(managedSunSpecID), Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	busy := &issue1432BusyStore{Store: &runtimeControlStore{m: store}}
	executor := &issue1432RecoveringExecutor{}
	service, err := runtimecontrol.New(runtimecontrol.Options{
		Store: busy, Recovery: rctlNoRecovery{}, Incarnation: "fixture-busy",
		Drivers: []runtimecontrol.DriverConfig{{DriverID: managedSunSpecID, Executor: executor}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)
	failed := issue1432Await(t, service, managedSunSpecID, func(s runtimecontrol.DriverStatus) bool {
		return s.ObservedState == drivermanager.ObservedFailed
	})
	now := time.Now()
	state := &modbusStartRetryDriver{}
	retry := &modbusStartRetry{service: service, policy: modbusStartRetryPolicy{Initial: time.Second, Max: 8 * time.Second}, now: func() time.Time { return now },
		drivers: map[runtimecontrol.DriverID]*modbusStartRetryDriver{managedSunSpecID: state}}
	retry.resync()
	busy.busy.Store(2)
	for i := 0; i < 2; i++ {
		now = state.due
		next := retry.fire(ctx)
		if state.attempts != 1 || state.revision != failed.DesiredRevision || !next.Equal(now.Add(time.Second)) || !state.due.Equal(next) {
			t.Fatalf("busy decision counted as another failed START: %+v, next=%v", *state, next)
		}
		if executor.starts.Load() != 1 {
			t.Fatal("busy decision reached the native START")
		}
	}
	now = state.due
	if next := retry.fire(ctx); !next.IsZero() {
		t.Fatalf("accepted decision left a scheduled retry: %v", next)
	}
	running := issue1432Await(t, service, managedSunSpecID, func(s runtimecontrol.DriverStatus) bool {
		return s.ObservedState == drivermanager.ObservedRunning
	})
	if executor.starts.Load() != 2 || running.DesiredRevision != failed.DesiredRevision+1 {
		t.Fatalf("recovery after busy decisions: starts=%d status=%+v", executor.starts.Load(), running)
	}
	retry.resync()
	if next := retry.fire(ctx); !next.IsZero() || !state.due.IsZero() || state.attempts != 0 || executor.starts.Load() != 2 {
		t.Fatalf("retry continued after RUNNING: %+v, next=%v", *state, next)
	}
}
