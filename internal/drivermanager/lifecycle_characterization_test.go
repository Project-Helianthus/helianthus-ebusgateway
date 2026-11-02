package drivermanager

// G5 PR 1a.1 characterization baseline (issue #1147).
//
// These tests pin the CURRENT protocol-neutral DriverManager lifecycle as
// observed before the G5 production edits (1a.2 registry seam, 1b durable
// control, 1c bridge). They are not a specification of desired behaviour.
// Every pinned line carries a one-line "Changes when:" note naming the later
// production change that may legitimately alter it; a later PR that changes a
// pinned value must update the expectation here and justify the difference in
// its PR body.
//
// Regeneration: the expectations are inline. On mismatch each test prints the
// complete observed block (between "--- observed ---" markers) so it can be
// pasted back after the intended change has been reviewed. Run:
//
//	go test ./internal/drivermanager -run Characterization -count=1 -v
//
// Determinism: the manager clock is a fake tick counter, retry jitter is
// zero, retry delays are one hour so no timer fires inside a test, and the
// fake runtime completes synchronously. No test sleeps or reads wall-clock.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// charClock is a deterministic manager clock: every call advances one tick.
type charClock struct {
	mu   sync.Mutex
	base time.Time
	tick int
}

func newCharClock() *charClock {
	return &charClock{base: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (clock *charClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.tick++
	return clock.base.Add(time.Duration(clock.tick) * time.Second)
}

func (clock *charClock) offset(at time.Time) string {
	if at.IsZero() {
		return "zero"
	}
	return fmt.Sprintf("t+%d", int(at.Sub(clock.base)/time.Second))
}

// charTrace is shared by every fake runtime of one manager so the relative
// order of runtime calls and optional lifecycle hooks is observable.
type charTrace struct {
	mu     sync.Mutex
	events []string
}

func (trace *charTrace) add(format string, args ...any) {
	trace.mu.Lock()
	trace.events = append(trace.events, fmt.Sprintf(format, args...))
	trace.mu.Unlock()
}

func (trace *charTrace) take() []string {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	events := trace.events
	trace.events = nil
	return events
}

// charRuntime implements Runtime plus every optional lifecycle hook the
// manager probes for (withdrawalFencer, correlationActivator,
// correlationBinder, AdmittingRuntime). Scripted results are consumed in
// call order; an exhausted script means success.
type charRuntime struct {
	mu          sync.Mutex
	id          string
	trace       *charTrace
	generation  uint64
	revision    uint64
	startErrs   []error
	replaceErrs []error
	stopErrs    []error
	quarantined bool
	startCalls  int
}

func (runtime *charRuntime) pop(script *[]error) error {
	if len(*script) == 0 {
		return nil
	}
	err := (*script)[0]
	*script = (*script)[1:]
	return err
}

func (runtime *charRuntime) construct(kind string, script *[]error) (uint64, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if kind == "start" {
		runtime.startCalls++
	}
	err := runtime.pop(script)
	if err != nil {
		runtime.trace.add("%s:%s err=%s", runtime.id, kind, charErrorClass(err))
		return 0, err
	}
	runtime.generation++
	runtime.revision++
	runtime.trace.add("%s:%s ok g%d", runtime.id, kind, runtime.generation)
	return runtime.generation, nil
}

func (runtime *charRuntime) Start(context.Context) (uint64, error) {
	return runtime.construct("start", &runtime.startErrs)
}

func (runtime *charRuntime) Replace(context.Context) (uint64, error) {
	return runtime.construct("replace", &runtime.replaceErrs)
}

func (runtime *charRuntime) Stop(context.Context) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	err := runtime.pop(&runtime.stopErrs)
	runtime.revision++
	if err != nil {
		runtime.trace.add("%s:stop err=%s", runtime.id, charErrorClass(err))
		return err
	}
	runtime.trace.add("%s:stop ok", runtime.id)
	return nil
}

func (runtime *charRuntime) Generation() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.generation
}

func (runtime *charRuntime) Revision() uint64 {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.revision
}

func (runtime *charRuntime) SafetyQuarantined() bool {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.quarantined
}

func (runtime *charRuntime) FenceWithdrawal() {
	runtime.trace.add("%s:fence", runtime.id)
}

func (runtime *charRuntime) ActivateCorrelation(correlation Correlation) {
	runtime.trace.add("%s:activate g%d op%d", runtime.id, correlation.Generation, correlation.Operation)
}

func (runtime *charRuntime) BindCorrelation(correlation Correlation) {
	runtime.trace.add("%s:bind g%d op%d", runtime.id, correlation.Generation, correlation.Operation)
}

func (runtime *charRuntime) Admit(context.Context) (*Admission, error) {
	generation := runtime.Generation()
	return &Admission{
		Correlation: Correlation{Generation: generation},
		Invoke:      func(fn func(any) error) error { return fn(nil) },
		Release:     func() error { return nil },
	}, nil
}

func (runtime *charRuntime) starts() int {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.startCalls
}

func charErrorClass(err error) string {
	switch {
	case err == nil:
		return "nil"
	case errors.Is(err, ErrSafetyQuarantined):
		return "safety_quarantined"
	case errors.Is(err, ErrStopTimeout):
		return "stop_timeout"
	case errors.Is(err, ErrUnavailable):
		return "unavailable"
	case errors.Is(err, ErrManagerClosed):
		return "manager_closed"
	case errors.Is(err, ErrDriverNotFound):
		return "not_found"
	default:
		return "other"
	}
}

var (
	errCharRetryable    = errors.New("characterization: dependency offline")
	errCharConfig       = errors.New("characterization: configuration rejected")
	errCharCloseUnknown = errors.New("characterization: close failed")
)

// charClassify mirrors the shape of the gateway eBUS classifier without
// importing it: a configuration error is terminal, everything else is a
// retryable dependency outage.
func charClassify(err error) Failure {
	if errors.Is(err, errCharConfig) {
		return Failure{Reason: Reason{Code: ReasonConfigInvalid}}
	}
	return Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}
}

func charRetryPolicy(budget int) RetryPolicy {
	// One-hour delays guarantee the retry timer never fires during a test;
	// negative jitter disables jitter so NotBeforeUTC is exact.
	return RetryPolicy{Budget: budget, InitialDelay: time.Hour, MaxDelay: 4 * time.Hour, JitterRatio: -1}
}

type charHarness struct {
	clock   *charClock
	trace   *charTrace
	manager *Manager
	runtime map[string]*charRuntime
}

func newCharHarness(t *testing.T, budget int, runtimes ...*charRuntime) *charHarness {
	t.Helper()
	harness := &charHarness{clock: newCharClock(), trace: &charTrace{}, runtime: map[string]*charRuntime{}}
	drivers := make([]DriverConfig, 0, len(runtimes))
	for _, runtime := range runtimes {
		runtime.trace = harness.trace
		harness.runtime[runtime.id] = runtime
		drivers = append(drivers, DriverConfig{
			ID:            runtime.id,
			Enabled:       true,
			Runtime:       runtime,
			Capabilities:  []Capability{CapabilityWrite, CapabilityRead, CapabilityDiscovery, Capability("NOT_A_CAPABILITY")},
			ClassifyError: charClassify,
			Retry:         charRetryPolicy(budget),
		})
	}
	manager, err := New(Config{Drivers: drivers, Now: harness.clock.Now, RetryJitter: func(time.Duration) time.Duration { return 0 }})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	harness.manager = manager
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	return harness
}

func (harness *charHarness) snapshot(t *testing.T, id string) string {
	t.Helper()
	snapshot, ok := harness.manager.Snapshot(id)
	if !ok {
		t.Fatalf("Snapshot(%q) missing", id)
	}
	retry := "none"
	if snapshot.Retry != nil {
		retry = fmt.Sprintf("{eligible=%v budget=%d not_before=%s}", snapshot.Retry.Eligible, snapshot.Retry.BudgetRemaining, harness.clock.offset(snapshot.Retry.NotBeforeUTC))
	}
	return fmt.Sprintf("desired=%s observed=%s reason=%s retryable=%v gen=%d rev=%d attempt=%d op=%d caps=%v eff=%v quarantined=%v retry=%s changed=%s",
		snapshot.DesiredState, snapshot.ObservedState, snapshot.Reason.Code, snapshot.Reason.Retryable,
		snapshot.Generation, snapshot.Revision, snapshot.Attempt, snapshot.ActiveOperation,
		snapshot.Capabilities, snapshot.EffectiveCapabilities, snapshot.SafetyQuarantined, retry,
		harness.clock.offset(snapshot.ChangedAtUTC))
}

func (harness *charHarness) invoke(id string, capability Capability) string {
	correlation, err := harness.manager.Invoke(context.Background(), id, capability, func(any) error { return nil })
	return fmt.Sprintf("invoke %s -> g%d op%d err=%s", capability, correlation.Generation, correlation.Operation, charErrorClass(err))
}

// charStep is one pinned observation plus the production change that would
// legitimately alter it.
type charStep struct {
	got       string
	want      string
	changesBy string
}

func requireCharSteps(t *testing.T, steps []charStep) {
	t.Helper()
	failed := false
	for index, step := range steps {
		if step.got != step.want {
			failed = true
			t.Errorf("step %d drifted (changes when: %s)\n want: %s\n  got: %s", index, step.changesBy, step.want, step.got)
		}
	}
	if failed {
		var observed strings.Builder
		for _, step := range steps {
			fmt.Fprintf(&observed, "%q,\n", step.got)
		}
		t.Logf("--- observed ---\n%s--- observed ---", observed.String())
	}
}

func joinTrace(events []string) string {
	if len(events) == 0 {
		return "trace=[]"
	}
	return "trace=[" + strings.Join(events, "; ") + "]"
}

func charErr(err error) string { return "err=" + charErrorClass(err) }

// TestLifecycleCharacterizationStartStopReplaceSnapshots pins the snapshot
// sequence and the runtime/hook call order for a healthy Start, Replace,
// Stop and restart through the protocol-neutral manager.
func TestLifecycleCharacterizationStartStopReplaceSnapshots(t *testing.T) {
	harness := newCharHarness(t, 2, &charRuntime{id: "driver.alpha"})
	const id = "driver.alpha"
	steps := []charStep{}
	record := func(got, want, changesBy string) {
		steps = append(steps, charStep{got: got, want: want, changesBy: changesBy})
	}

	record(harness.snapshot(t, id),
		"desired=RUNNING observed=STOPPED reason=NONE retryable=false gen=0 rev=1 attempt=0 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+1",
		"1a.2 registry seam changes initial enabled-driver state or capability normalization (unknown capability dropped, sorted)")
	record(harness.invoke(id, CapabilityWrite),
		"invoke WRITE -> g0 op0 err=unavailable",
		"1b durable control admits work before the first RUNNING generation")

	record(charErr(harness.manager.Start(context.Background(), id)), "err=nil", "1b durable control makes Start asynchronous-only or returns an operation handle")
	record(joinTrace(harness.trace.take()),
		"trace=[driver.alpha:start ok g1; driver.alpha:activate g1 op0; driver.alpha:bind g1 op0]",
		"1a.2/1c move pre-admission activation (B503 issuance) after RUNNING or drop the post-publication bind")
	record(harness.snapshot(t, id),
		"desired=RUNNING observed=RUNNING reason=NONE retryable=false gen=1 rev=3 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[DISCOVERY READ WRITE] quarantined=false retry=none changed=t+3",
		"1b durable control adds intermediate observable states or changes revision accounting (STARTING then RUNNING)")
	record(harness.invoke(id, CapabilityWrite),
		"invoke WRITE -> g1 op0 err=nil",
		"1a.2 registry seam moves admission off Manager.Invoke")

	record(charErr(harness.manager.Replace(context.Background(), id)), "err=nil", "1b durable control changes Replace into an asynchronous control operation")
	record(joinTrace(harness.trace.take()),
		"trace=[driver.alpha:fence; driver.alpha:replace ok g2; driver.alpha:activate g2 op0; driver.alpha:bind g2 op0]",
		"1c bridge stops fencing withdrawal (B503 withdrawal) before Replace")
	record(harness.snapshot(t, id),
		"desired=RUNNING observed=RUNNING reason=NONE retryable=false gen=2 rev=5 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[DISCOVERY READ WRITE] quarantined=false retry=none changed=t+5",
		"1b durable control publishes Replace progress differently (Replace publishes STOPPING, not STARTING, before construction)")

	record(charErr(harness.manager.Stop(context.Background(), id)), "err=nil", "1b durable control returns Stop outcomes as errors instead of snapshot reasons")
	record(joinTrace(harness.trace.take()),
		"trace=[driver.alpha:fence; driver.alpha:stop ok]",
		"1c bridge drops the withdrawal fence before Runtime.Stop")
	record(harness.snapshot(t, id),
		"desired=STOPPED observed=STOPPED reason=OPERATOR_STOPPED retryable=false gen=2 rev=7 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+7",
		"1b durable control persists desired STOPPED or resets generation on stop (generation is retained after Stop today)")
	record(harness.invoke(id, CapabilityRead),
		"invoke READ -> g2 op0 err=unavailable",
		"1a.2 registry seam changes the correlation returned for a withdrawn driver")

	record(charErr(harness.manager.Start(context.Background(), id)), "err=nil", "1b durable control changes restart semantics")
	record(joinTrace(harness.trace.take()),
		"trace=[driver.alpha:start ok g3; driver.alpha:activate g3 op0; driver.alpha:bind g3 op0]",
		"1b durable control restarts a clean stop through Replace instead of Start")
	record(harness.snapshot(t, id),
		"desired=RUNNING observed=RUNNING reason=NONE retryable=false gen=3 rev=9 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[DISCOVERY READ WRITE] quarantined=false retry=none changed=t+9",
		"1b durable control changes restart revision accounting")
	requireCharSteps(t, steps)
}

// TestLifecycleCharacterizationStopCloseClasses pins how Stop classifies the
// runtime close result into observed state, reason and quarantine, and
// whether a later Start is admitted.
func TestLifecycleCharacterizationStopCloseClasses(t *testing.T) {
	cases := []struct {
		name        string
		stopErr     error
		quarantine  bool
		wantStop    string
		wantRestart string
		changesBy   string
	}{
		{
			name:        "clean_close",
			wantStop:    "desired=STOPPED observed=STOPPED reason=OPERATOR_STOPPED retryable=false gen=1 rev=5 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+5",
			wantRestart: "err=nil observed=RUNNING trace=[driver.alpha:start ok g2; driver.alpha:activate g2 op0; driver.alpha:bind g2 op0]",
			changesBy:   "1b durable control renames the clean-stop reason or restarts through Replace",
		},
		{
			name:        "stop_timeout",
			stopErr:     fmt.Errorf("drain: %w", ErrStopTimeout),
			wantStop:    "desired=STOPPED observed=STOPPED reason=STOP_TIMEOUT retryable=false gen=1 rev=5 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+5",
			wantRestart: "err=nil observed=RUNNING trace=[driver.alpha:start ok g2; driver.alpha:activate g2 op0; driver.alpha:bind g2 op0]",
			changesBy:   "1b durable control treats a drain timeout as unproven close (FAILED) instead of STOPPED",
		},
		{
			name:        "safety_quarantined_error",
			stopErr:     fmt.Errorf("close unproven: %w", ErrSafetyQuarantined),
			wantStop:    "desired=STOPPED observed=FAILED reason=CLOSE_UNCONFIRMED retryable=false gen=1 rev=5 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=true retry=none changed=t+5",
			wantRestart: "err=safety_quarantined observed=FAILED trace=[]",
			changesBy:   "1b durable control adds an operator path out of process-epoch quarantine",
		},
		{
			name:        "other_error",
			stopErr:     errCharCloseUnknown,
			wantStop:    "desired=STOPPED observed=FAILED reason=INTERNAL_ERROR retryable=false gen=1 rev=5 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+5",
			wantRestart: "err=nil observed=RUNNING trace=[driver.alpha:start ok g2; driver.alpha:activate g2 op0; driver.alpha:bind g2 op0]",
			changesBy:   "1b durable control quarantines or retries after an unclassified close error",
		},
		{
			name:        "nil_error_runtime_reports_quarantine",
			quarantine:  true,
			wantStop:    "desired=STOPPED observed=STOPPED reason=OPERATOR_STOPPED retryable=false gen=1 rev=5 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+5",
			wantRestart: "err=nil observed=RUNNING trace=[driver.alpha:start ok g2; driver.alpha:activate g2 op0; driver.alpha:bind g2 op0]",
			changesBy:   "1b durable control consults Runtime.SafetyQuarantined on a nil close or successful construction (today a nil error wins on both paths)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runtime := &charRuntime{id: "driver.alpha"}
			if tc.stopErr != nil {
				runtime.stopErrs = []error{tc.stopErr}
			}
			harness := newCharHarness(t, 0, runtime)
			if err := harness.manager.Start(context.Background(), runtime.id); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			harness.trace.take()
			runtime.mu.Lock()
			runtime.quarantined = tc.quarantine
			runtime.mu.Unlock()
			stopErr := harness.manager.Stop(context.Background(), runtime.id)
			stopTrace := joinTrace(harness.trace.take())
			stopSnapshot := harness.snapshot(t, runtime.id)
			restartErr := harness.manager.Start(context.Background(), runtime.id)
			restartSnapshot, _ := harness.manager.Snapshot(runtime.id)
			restart := fmt.Sprintf("%s observed=%s %s", charErr(restartErr), restartSnapshot.ObservedState, joinTrace(harness.trace.take()))
			wantStopTrace := "trace=[driver.alpha:fence; driver.alpha:stop ok]"
			if tc.stopErr != nil {
				wantStopTrace = fmt.Sprintf("trace=[driver.alpha:fence; driver.alpha:stop err=%s]", charErrorClass(tc.stopErr))
			}
			requireCharSteps(t, []charStep{
				{got: charErr(stopErr), want: "err=nil", changesBy: "1b durable control surfaces close classes as Stop errors"},
				{got: stopTrace, want: wantStopTrace, changesBy: "1c bridge drops the withdrawal fence before Runtime.Stop"},
				{got: stopSnapshot, want: tc.wantStop, changesBy: tc.changesBy},
				{got: restart, want: tc.wantRestart, changesBy: tc.changesBy},
			})
		})
	}
}

// TestLifecycleCharacterizationStartFailureClassesAndRetryOwnership pins the
// snapshot published for each construction failure class and that the
// scheduled retry is manager-owned: an explicit Start or Stop supersedes it
// and Shutdown reaps it without a further construction.
func TestLifecycleCharacterizationStartFailureClassesAndRetryOwnership(t *testing.T) {
	cases := []struct {
		name      string
		budget    int
		startErr  error
		wantFail  string
		then      string // "start" or "stop"
		wantThen  string
		wantCalls int
		changesBy string
	}{
		{
			name:      "retryable_with_budget_then_explicit_start",
			budget:    2,
			startErr:  errCharRetryable,
			wantFail:  "desired=RUNNING observed=BACKOFF reason=RETRY_SCHEDULED retryable=true gen=0 rev=3 attempt=1 op=1 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry={eligible=true budget=2 not_before=t+3603} changed=t+4",
			then:      "start",
			wantThen:  "observed=RUNNING trace=[driver.alpha:start ok g1; driver.alpha:activate g1 op0; driver.alpha:bind g1 op0]",
			wantCalls: 2,
			changesBy: "1b durable control moves retry ownership out of the manager goroutine or changes backoff accounting",
		},
		{
			name:      "retryable_with_budget_then_stop",
			budget:    2,
			startErr:  errCharRetryable,
			wantFail:  "desired=RUNNING observed=BACKOFF reason=RETRY_SCHEDULED retryable=true gen=0 rev=3 attempt=1 op=1 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry={eligible=true budget=2 not_before=t+3603} changed=t+4",
			then:      "stop",
			wantThen:  "observed=STOPPED trace=[driver.alpha:fence; driver.alpha:stop ok]",
			wantCalls: 1,
			changesBy: "1b durable control keeps retrying after an explicit stop",
		},
		{
			name:      "retryable_without_budget",
			budget:    0,
			startErr:  errCharRetryable,
			wantFail:  "desired=RUNNING observed=FAILED reason=RETRY_EXHAUSTED retryable=false gen=0 rev=3 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+3",
			then:      "stop",
			wantThen:  "observed=STOPPED trace=[driver.alpha:fence; driver.alpha:stop ok]",
			wantCalls: 1,
			changesBy: "1b durable control changes exhausted-budget reporting",
		},
		{
			name:      "terminal_configuration_error",
			budget:    2,
			startErr:  errCharConfig,
			wantFail:  "desired=RUNNING observed=FAILED reason=CONFIG_INVALID retryable=false gen=0 rev=3 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+3",
			then:      "start",
			wantThen:  "observed=RUNNING trace=[driver.alpha:start ok g1; driver.alpha:activate g1 op0; driver.alpha:bind g1 op0]",
			wantCalls: 2,
			changesBy: "1b durable control blocks restart after a terminal configuration failure",
		},
		{
			name:      "provider_unavailable",
			budget:    2,
			startErr:  fmt.Errorf("factory: %w", ErrUnavailable),
			wantFail:  "desired=RUNNING observed=FAILED reason=PROVIDER_UNAVAILABLE retryable=false gen=0 rev=3 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+3",
			then:      "stop",
			wantThen:  "observed=STOPPED trace=[driver.alpha:fence; driver.alpha:stop ok]",
			wantCalls: 1,
			changesBy: "1a.2 registry seam retries an unavailable provider instead of failing terminally",
		},
		{
			name:      "safety_quarantined_construction",
			budget:    2,
			startErr:  fmt.Errorf("construct: %w", ErrSafetyQuarantined),
			wantFail:  "desired=RUNNING observed=FAILED reason=CLOSE_UNCONFIRMED retryable=false gen=0 rev=3 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=true retry=none changed=t+3",
			then:      "start",
			wantThen:  "observed=FAILED trace=[]",
			wantCalls: 1,
			changesBy: "1b durable control adds a quarantine release path",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runtime := &charRuntime{id: "driver.alpha", startErrs: []error{tc.startErr}}
			harness := newCharHarness(t, tc.budget, runtime)
			startErr := harness.manager.Start(context.Background(), runtime.id)
			failTrace := joinTrace(harness.trace.take())
			failSnapshot := harness.snapshot(t, runtime.id)
			var thenErr error
			if tc.then == "start" {
				thenErr = harness.manager.Start(context.Background(), runtime.id)
			} else {
				thenErr = harness.manager.Stop(context.Background(), runtime.id)
			}
			thenSnapshot, _ := harness.manager.Snapshot(runtime.id)
			then := fmt.Sprintf("observed=%s %s", thenSnapshot.ObservedState, joinTrace(harness.trace.take()))
			if err := harness.manager.Shutdown(context.Background()); err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}
			harness.trace.take()
			wantThenErr := "err=nil"
			if tc.name == "safety_quarantined_construction" {
				wantThenErr = "err=safety_quarantined"
			}
			requireCharSteps(t, []charStep{
				{got: charErr(startErr), want: "err=nil", changesBy: "1b durable control returns construction failures from Start"},
				{got: failTrace, want: fmt.Sprintf("trace=[driver.alpha:start err=%s]", charErrorClass(tc.startErr)), changesBy: "1a.2 registry seam adds construction hooks"},
				{got: failSnapshot, want: tc.wantFail, changesBy: tc.changesBy},
				{got: charErr(thenErr), want: wantThenErr, changesBy: tc.changesBy},
				{got: then, want: tc.wantThen, changesBy: tc.changesBy},
				{got: fmt.Sprintf("start_calls=%d", runtime.starts()), want: fmt.Sprintf("start_calls=%d", tc.wantCalls), changesBy: "retry ownership: the manager-owned backoff timer is canceled by explicit control and Shutdown (1b moves ownership)"},
			})
		})
	}
}

// TestLifecycleCharacterizationReportFailureWithdrawalAndRecovery pins the
// generation-correlated failure callback: withdrawal is fenced before
// BACKOFF, stale correlations are rejected, recovery uses Replace, and
// degradation withdraws a capability subset.
func TestLifecycleCharacterizationReportFailureWithdrawalAndRecovery(t *testing.T) {
	harness := newCharHarness(t, 1, &charRuntime{id: "driver.alpha"})
	const id = "driver.alpha"
	steps := []charStep{}
	record := func(got, want, changesBy string) {
		steps = append(steps, charStep{got: got, want: want, changesBy: changesBy})
	}

	if err := harness.manager.Start(context.Background(), id); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	harness.trace.take()

	record(fmt.Sprintf("degraded=%v", harness.manager.ReportDegraded(id, 1, []Capability{CapabilityRead}, Reason{Code: ReasonCapabilityDegraded})),
		"degraded=true", "1a.2 registry seam changes degradation admission")
	record(harness.snapshot(t, id),
		"desired=RUNNING observed=DEGRADED reason=CAPABILITY_DEGRADED retryable=false gen=1 rev=4 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[READ] quarantined=false retry=none changed=t+4",
		"1b durable control changes degraded-state projection")
	record(harness.invoke(id, CapabilityWrite), "invoke WRITE -> g1 op0 err=unavailable", "1a.2 registry seam stops gating admission on effective capability")
	record(charErr(harness.manager.Start(context.Background(), id))+" "+joinTrace(harness.trace.take()),
		"err=nil trace=[]", "1b durable control lets a repeated Start recover a degraded generation")

	record(fmt.Sprintf("stale_accepted=%v", harness.manager.ReportFailure(id, Correlation{Generation: 0}, Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}})),
		"stale_accepted=false", "1a.2 registry seam changes callback correlation fencing")
	record(fmt.Sprintf("accepted=%v", harness.manager.ReportFailure(id, Correlation{Generation: 1}, Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}})),
		"accepted=true", "1a.2 registry seam changes failure callback admission")
	record(joinTrace(harness.trace.take()),
		"trace=[driver.alpha:fence]", "1c bridge drops the withdrawal fence (B503 withdrawal) on reported failure")
	record(harness.snapshot(t, id),
		"desired=RUNNING observed=BACKOFF reason=RETRY_SCHEDULED retryable=true gen=1 rev=5 attempt=1 op=2 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry={eligible=true budget=1 not_before=t+3605} changed=t+6",
		"1b durable control changes failure backoff accounting")

	record(charErr(harness.manager.Start(context.Background(), id))+" "+joinTrace(harness.trace.take()),
		"err=nil trace=[driver.alpha:replace ok g2; driver.alpha:activate g2 op0; driver.alpha:bind g2 op0]",
		"1b durable control recovers a failed generation through Start instead of Replace")
	record(harness.snapshot(t, id),
		"desired=RUNNING observed=RUNNING reason=NONE retryable=false gen=2 rev=7 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[DISCOVERY READ WRITE] quarantined=false retry=none changed=t+8",
		"1b durable control changes recovery projection")

	record(fmt.Sprintf("accepted=%v", harness.manager.ReportFailure(id, Correlation{Generation: 2}, Failure{Reason: Reason{Code: ReasonConfigInvalid}})),
		"accepted=true", "1a.2 registry seam changes failure callback admission")
	record(joinTrace(harness.trace.take())+" "+harness.snapshot(t, id),
		"trace=[driver.alpha:fence] desired=RUNNING observed=FAILED reason=CONFIG_INVALID retryable=false gen=2 rev=8 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+9",
		"1b durable control changes terminal failure projection")
	record(charErr(harness.manager.Stop(context.Background(), id))+" "+joinTrace(harness.trace.take())+" "+harness.snapshot(t, id),
		"err=nil trace=[driver.alpha:fence; driver.alpha:stop ok] desired=STOPPED observed=STOPPED reason=OPERATOR_STOPPED retryable=false gen=2 rev=10 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+11",
		"1b durable control skips Runtime.Stop for an already failed generation")
	requireCharSteps(t, steps)
}

// TestLifecycleCharacterizationShutdownOrderAndClosedFence pins that
// Shutdown stops drivers in sorted ID order and permanently fences new
// lifecycle requests.
func TestLifecycleCharacterizationShutdownOrderAndClosedFence(t *testing.T) {
	harness := newCharHarness(t, 1, &charRuntime{id: "driver.zulu"}, &charRuntime{id: "driver.alpha"})
	for _, id := range []string{"driver.zulu", "driver.alpha"} {
		if err := harness.manager.Start(context.Background(), id); err != nil {
			t.Fatalf("Start(%s) error = %v", id, err)
		}
	}
	harness.trace.take()
	shutdownErr := harness.manager.Shutdown(context.Background())
	requireCharSteps(t, []charStep{
		{got: charErr(shutdownErr), want: "err=nil", changesBy: "1b durable control returns per-driver shutdown outcomes"},
		{got: joinTrace(harness.trace.take()),
			want:      "trace=[driver.alpha:fence; driver.alpha:stop ok; driver.zulu:fence; driver.zulu:stop ok]",
			changesBy: "1a.2 registry seam orders shutdown by registration or dependency instead of sorted ID"},
		{got: harness.snapshot(t, "driver.zulu"),
			want:      "desired=STOPPED observed=STOPPED reason=OPERATOR_STOPPED retryable=false gen=1 rev=5 attempt=1 op=0 caps=[DISCOVERY READ WRITE] eff=[] quarantined=false retry=none changed=t+10",
			changesBy: "1b durable control distinguishes process shutdown from operator stop"},
		{got: charErr(harness.manager.Start(context.Background(), "driver.alpha")), want: "err=manager_closed", changesBy: "1b durable control allows restart after a service STOP (J15 acceptance)"},
		{got: charErr(harness.manager.Replace(context.Background(), "driver.alpha")), want: "err=manager_closed", changesBy: "1b durable control allows replace after a service STOP"},
		{got: charErr(harness.manager.Start(context.Background(), "driver.missing")), want: "err=not_found", changesBy: "1a.2 registry seam registers drivers dynamically"},
	})
}
