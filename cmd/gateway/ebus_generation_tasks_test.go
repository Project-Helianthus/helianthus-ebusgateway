package main

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

// --- fakes: admission predicate, process-scoped effects, watchdogs ---

// genAdmission is a fake lifecycle owner: exactly one key is admitted at a
// time, or none.
type genAdmission struct {
	mu      sync.Mutex
	current ebusGenerationKey
	ok      bool
	calls   atomic.Int64
}

func (a *genAdmission) set(key ebusGenerationKey) {
	a.mu.Lock()
	a.current, a.ok = key, true
	a.mu.Unlock()
}

func (a *genAdmission) withdraw() {
	a.mu.Lock()
	a.ok = false
	a.mu.Unlock()
}

func (a *genAdmission) admitted(key ebusGenerationKey) bool {
	a.calls.Add(1)
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.ok && a.current == key
}

// genEffects is the process-scoped state that generation tasks may mutate
// only through scope.Commit: the active bus source, publications, poll
// results and B503/proxy work. Its records carry provenance and age.
type genEffects struct {
	mu sync.Mutex
	genEffectsView
}

type genEffectsView struct {
	activeSource ebusGenerationKey
	published    []ebusGenerationKey
	polls        int
	b503         int
	proxy        int
}

func (e *genEffects) snapshot() genEffectsView {
	e.mu.Lock()
	defer e.mu.Unlock()
	view := e.genEffectsView
	view.published = append([]ebusGenerationKey(nil), e.published...)
	return view
}

// mutateAll tries every effect kind through the scope and reports how many
// landed.
func (e *genEffects) mutateAll(scope *ebusGenerationScope) int {
	landed := 0
	for _, fn := range []func(){
		func() { e.activeSource = scope.Key() },
		func() { e.published = append(e.published, scope.Key()) },
		func() { e.polls++ },
		func() { e.b503++ },
		func() { e.proxy++ },
	} {
		if scope.Commit(func() { e.mu.Lock(); fn(); e.mu.Unlock() }) {
			landed++
		}
	}
	return landed
}

// genRecord is a retained evidence record with native provenance and age.
type genRecord struct {
	Provenance ebusGenerationKey
	ObservedAt time.Time
	Value      string
}

type genEvidence struct {
	mu      sync.Mutex
	records []genRecord
}

func (s *genEvidence) all() []genRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]genRecord(nil), s.records...)
}

func genKey(epoch string, generation, sequence uint64, native uint64) ebusGenerationKey {
	return ebusGenerationKey{Control: ebusControlKey{DriverID: primaryEBusDriverID, SourceID: ctlSource,
		Epoch: runtimecontrol.SourceEpoch(epoch), Generation: runtimecontrol.DriverGeneration(generation),
		Sequence: runtimecontrol.LifecycleSequence(sequence)}, Native: runtimecontrol.NativeGeneration(native)}
}

// genWatch is a bounded watchdog on a barrier channel.
func genWatch(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("watchdog: timed out waiting for %s", what)
	}
}

func genRunner(t *testing.T, cfg ebusGenerationTaskConfig) *ebusGenerationTaskRunner {
	t.Helper()
	runner, err := newEBusGenerationTaskRunner(cfg)
	if err != nil {
		t.Fatalf("newEBusGenerationTaskRunner: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = runner.Close(ctx)
	})
	return runner
}

func genTaskState(status ebusGenerationStatus, name string) ebusGenerationTaskState {
	for _, task := range status.Tasks {
		if task.Name == name {
			return task.State
		}
	}
	return ""
}

func genNop(context.Context, *ebusGenerationScope) error { return nil }

// --- construction ---

func TestGenerationTasksConfigValidationRunsNothing(t *testing.T) {
	var calls atomic.Int64
	run := func(context.Context, *ebusGenerationScope) error { calls.Add(1); return nil }
	admitted := func(ebusGenerationKey) bool { calls.Add(1); return true }
	many := make([]ebusGenerationTask, maxEBusGenerationTasks+1)
	for i := range many {
		many[i] = ebusGenerationTask{Name: "task" + strings.Repeat("a", i+1), Run: run}
	}
	cases := map[string]ebusGenerationTaskConfig{
		"no predicate":     {Tasks: []ebusGenerationTask{{Name: "scan", Run: run}}},
		"no tasks":         {Admitted: admitted},
		"too many tasks":   {Admitted: admitted, Tasks: many},
		"empty name":       {Admitted: admitted, Tasks: []ebusGenerationTask{{Run: run}}},
		"noncanonical":     {Admitted: admitted, Tasks: []ebusGenerationTask{{Name: "Scan", Run: run}}},
		"long name":        {Admitted: admitted, Tasks: []ebusGenerationTask{{Name: strings.Repeat("a", maxEBusGenerationTaskName+1), Run: run}}},
		"duplicate":        {Admitted: admitted, Tasks: []ebusGenerationTask{{Name: "scan", Run: run}, {Name: "scan", Run: run}}},
		"no function":      {Admitted: admitted, Tasks: []ebusGenerationTask{{Name: "scan"}}},
		"unknown requires": {Admitted: admitted, Tasks: []ebusGenerationTask{{Name: "scan", Requires: []string{"validate"}, Run: run}}},
		"later requires":   {Admitted: admitted, Tasks: []ebusGenerationTask{{Name: "scan", Requires: []string{"validate"}, Run: run}, {Name: "validate", Run: run}}},
		"self requires":    {Admitted: admitted, Tasks: []ebusGenerationTask{{Name: "scan", Requires: []string{"scan"}, Run: run}}},
		"repeated require": {Admitted: admitted, Tasks: []ebusGenerationTask{{Name: "validate", Run: run}, {Name: "scan", Requires: []string{"validate", "validate"}, Run: run}}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := newEBusGenerationTaskRunner(cfg); !errors.Is(err, errGenerationTasksInvalid) {
				t.Fatalf("err = %v, want invalid configuration", err)
			}
		})
	}
	// Caller-owned slices are copied at construction.
	requires := []string{"validate"}
	tasks := []ebusGenerationTask{{Name: "validate", Run: run}, {Name: "scan", Requires: requires, Run: run}}
	runner, err := newEBusGenerationTaskRunner(ebusGenerationTaskConfig{Admitted: admitted, Tasks: tasks})
	if err != nil {
		t.Fatal(err)
	}
	requires[0] = "scan"
	tasks[1].Name = "mutated"
	if runner.tasks[1].name != "scan" || !reflect.DeepEqual(runner.tasks[1].requires, []int{0}) {
		t.Fatalf("frozen task set changed with the caller's slices: %#v", runner.tasks[1])
	}
	if calls.Load() != 0 {
		t.Fatalf("construction invoked %d task or predicate calls", calls.Load())
	}
	if status := runner.Status(); status.Phase != ebusGenerationIdle || len(status.Tasks) != 0 {
		t.Fatalf("status = %#v", status)
	}
}

// --- acceptance 1: one admitted key owns every task; readiness first ---

func TestGenerationTasksNoWorkBeforeReadinessOrForIncompleteKeys(t *testing.T) {
	adm := &genAdmission{}
	var calls atomic.Int64
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "validate", Run: func(context.Context, *ebusGenerationScope) error { calls.Add(1); return nil }},
	}})
	key := genKey("inc.e1", 1, 1, 1)
	if err := runner.Admit(key); !errors.Is(err, errGenerationTasksNotReady) {
		t.Fatalf("Admit before readiness = %v", err)
	}
	// Every field of the key is required.
	incomplete := map[string]func(*ebusGenerationKey){
		"DriverID":   func(k *ebusGenerationKey) { k.Control.DriverID = "" },
		"SourceID":   func(k *ebusGenerationKey) { k.Control.SourceID = "" },
		"Epoch":      func(k *ebusGenerationKey) { k.Control.Epoch = "" },
		"Generation": func(k *ebusGenerationKey) { k.Control.Generation = 0 },
		"Native":     func(k *ebusGenerationKey) { k.Native = 0 },
	}
	// Sequence 0 is legitimate: it names the untyped process-start attempt
	// of durable desired RUNNING, which has no typed work slot.
	for field, clear := range incomplete {
		variant := key
		clear(&variant)
		adm.set(variant)
		if err := runner.Admit(variant); !errors.Is(err, errGenerationTasksKey) {
			t.Fatalf("Admit without %s = %v", field, err)
		}
	}
	if calls.Load() != 0 || runner.Status().Phase != ebusGenerationIdle {
		t.Fatalf("work ran before readiness: calls=%d status=%#v", calls.Load(), runner.Status())
	}

	// Admission withdrawn between Admit and task start: the task never runs.
	var withdrawn atomic.Bool
	gate := make(chan struct{})
	started := make(chan struct{})
	runner2 := genRunner(t, ebusGenerationTaskConfig{
		Admitted: func(k ebusGenerationKey) bool { return k == key && !withdrawn.Load() },
		Tasks: []ebusGenerationTask{
			{Name: "validate", Run: func(context.Context, *ebusGenerationScope) error { close(started); <-gate; return nil }},
			{Name: "scan", Requires: []string{"validate"}, Run: func(context.Context, *ebusGenerationScope) error { calls.Add(1); return nil }},
		},
	})
	if err := runner2.Admit(key); err != nil {
		t.Fatal(err)
	}
	genWatch(t, "validate start", started)
	withdrawn.Store(true)
	close(gate)
	ctlWait(t, "scan withheld", func() bool { return genTaskState(runner2.Status(), "scan") == ebusGenerationTaskCancelled })
	if calls.Load() != 0 {
		t.Fatal("dependent task ran after admission withdrawal")
	}
}

func TestGenerationTasksOncePerGenerationUnderRepeatedWakes(t *testing.T) {
	adm := &genAdmission{}
	key := genKey("inc.e1", 1, 1, 7)
	adm.set(key)
	var validate, revalidate, pollStarts, polls atomic.Int64
	validated := make(chan struct{})
	tick := make(chan struct{})
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "source-validation", Run: func(ctx context.Context, scope *ebusGenerationScope) error {
			if scope.Key() != key {
				t.Errorf("task key = %#v", scope.Key())
			}
			validate.Add(1)
			<-validated
			return nil
		}},
		{Name: "revalidate", Requires: []string{"source-validation"}, Run: func(context.Context, *ebusGenerationScope) error {
			revalidate.Add(1)
			return nil
		}},
		{Name: "semantic-poll", Run: func(ctx context.Context, scope *ebusGenerationScope) error {
			pollStarts.Add(1)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-tick:
					scope.Commit(func() { polls.Add(1) })
				}
			}
		}},
	}})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runner.Admit(key); err != nil {
				t.Errorf("repeated Admit: %v", err)
			}
			if _, err := runner.Sync(context.Background(), key, true); err != nil {
				t.Errorf("repeated Sync: %v", err)
			}
		}()
	}
	wg.Wait()
	if revalidate.Load() != 0 {
		t.Fatal("dependent ran before its prerequisite returned")
	}
	close(validated)
	ctlWait(t, "revalidate", func() bool { return revalidate.Load() == 1 })
	// The polling loop iterates inside its one generation-owned task.
	for i := 0; i < 3; i++ {
		tick <- struct{}{}
	}
	ctlWait(t, "polls", func() bool { return polls.Load() == 3 })
	if validate.Load() != 1 || revalidate.Load() != 1 || pollStarts.Load() != 1 {
		t.Fatalf("starts: validate=%d revalidate=%d poll=%d, want 1 each", validate.Load(), revalidate.Load(), pollStarts.Load())
	}
	status := runner.Status()
	if status.Phase != ebusGenerationRunning || status.Key != key || genTaskState(status, "semantic-poll") != ebusGenerationTaskRunning ||
		genTaskState(status, "source-validation") != ebusGenerationTaskSucceeded || genTaskState(status, "revalidate") != ebusGenerationTaskSucceeded {
		t.Fatalf("status = %#v", status)
	}
	if join := runner.Withdraw(ctlCtx(t), key); !join.Joined {
		t.Fatalf("Withdraw = %#v", join)
	}
	if genTaskState(runner.Status(), "semantic-poll") != ebusGenerationTaskCancelled {
		t.Fatalf("poll state = %s", genTaskState(runner.Status(), "semantic-poll"))
	}
}

func TestGenerationTasksSourceValidationRerunsOnLaterStart(t *testing.T) {
	adm := &genAdmission{}
	var mu sync.Mutex
	var validated []ebusGenerationKey
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "source-validation", Run: func(_ context.Context, scope *ebusGenerationScope) error {
			mu.Lock()
			validated = append(validated, scope.Key())
			mu.Unlock()
			return nil
		}},
	}})
	first := genKey("inc.e1", 1, 1, 3)
	adm.set(first)
	if err := runner.Admit(first); err != nil {
		t.Fatal(err)
	}
	ctlWait(t, "first validation", func() bool { mu.Lock(); defer mu.Unlock(); return len(validated) == 1 })
	// STOP: the owner withdraws admission; Sync fences and joins.
	adm.withdraw()
	if join, err := runner.Sync(ctlCtx(t), ebusGenerationKey{}, false); err != nil || !join.Joined || join.Key != first {
		t.Fatalf("Sync(stopped) = %#v, %v", join, err)
	}
	if runner.Status().Phase != ebusGenerationIdle {
		t.Fatalf("stopped status = %#v", runner.Status())
	}
	// An ended generation is never re-run, and an older native mapping is
	// refused even after the stopped interval.
	adm.set(first)
	if err := runner.Admit(first); !errors.Is(err, errGenerationTasksStale) {
		t.Fatalf("re-Admit of ended generation = %v", err)
	}
	older := genKey("inc.e0", 1, 1, 2)
	adm.set(older)
	if err := runner.Admit(older); !errors.Is(err, errGenerationTasksStale) {
		t.Fatalf("Admit of older native generation = %v", err)
	}
	// Later START: a new epoch at generation 1 on a later native generation.
	second := genKey("inc.e2", 1, 2, 4)
	adm.set(second)
	if _, err := runner.Sync(ctlCtx(t), second, true); err != nil {
		t.Fatal(err)
	}
	ctlWait(t, "second validation", func() bool { mu.Lock(); defer mu.Unlock(); return len(validated) == 2 })
	mu.Lock()
	defer mu.Unlock()
	if validated[0] != first || validated[1] != second {
		t.Fatalf("validated = %#v", validated)
	}
}

// --- acceptance 2: STOP/RESTART cancels and joins; old generation fenced ---

func TestGenerationTasksWithdrawJoinsBlockedErrorAndLateReturn(t *testing.T) {
	adm := &genAdmission{}
	effects := &genEffects{}
	var failures atomic.Int64
	release := make(chan struct{})
	lateStarted := make(chan struct{})
	lateReturned := make(chan struct{})
	helperStarted := make(chan struct{})
	ctxStarted := make(chan struct{})
	var lateLanded atomic.Int64
	first := genKey("inc.e1", 1, 1, 5)
	runner := genRunner(t, ebusGenerationTaskConfig{
		Admitted:    adm.admitted,
		JoinTimeout: 50 * time.Millisecond,
		Failure:     func(ebusGenerationKey, string, error) { failures.Add(1) },
		Tasks: []ebusGenerationTask{
			// Ignores cancellation until released: the late-return case.
			{Name: "startup-scan", Run: func(_ context.Context, scope *ebusGenerationScope) error {
				if scope.Key() != first {
					return nil
				}
				close(lateStarted)
				<-release
				lateLanded.Store(int64(effects.mutateAll(scope)))
				close(lateReturned)
				return errors.New("scan interrupted")
			}},
			// Honors cancellation and returns an error.
			{Name: "semantic-poll", Run: func(ctx context.Context, scope *ebusGenerationScope) error {
				if scope.Key() != first {
					return nil
				}
				if !scope.Go(func(ctx context.Context) { close(helperStarted); <-ctx.Done() }) {
					t.Error("helper refused while live")
				}
				close(ctxStarted)
				<-ctx.Done()
				return ctx.Err()
			}},
			{Name: "promote", Run: func(context.Context, *ebusGenerationScope) error { return nil }},
		},
	})
	key := first
	adm.set(key)
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	genWatch(t, "late task start", lateStarted)
	genWatch(t, "poll start", ctxStarted)
	genWatch(t, "helper start", helperStarted)
	before := effects.snapshot()

	// Fence, then wait (bounded) until only the task that ignores
	// cancellation is still running, so the join outcome does not depend on
	// how fast the cancellable work unwinds.
	if !runner.Fence(key) {
		t.Fatal("Fence of the owning key reported false")
	}
	ctlWait(t, "cancellable work joined", func() bool {
		return reflect.DeepEqual(runner.Status().Pending, []string{"startup-scan"})
	})
	join := runner.Withdraw(ctlCtx(t), key)
	if join.Joined || !reflect.DeepEqual(join.Pending, []string{"startup-scan"}) {
		t.Fatalf("Withdraw with a blocked task = %#v, want not joined with startup-scan pending", join)
	}
	status := runner.Status()
	if status.Phase != ebusGenerationDraining || genTaskState(status, "semantic-poll") != ebusGenerationTaskCancelled {
		t.Fatalf("draining status = %#v", status)
	}
	// RESTART: the successor is admitted by its owner, but no successor task
	// may start while the predecessor has not joined.
	next := genKey("inc.e1", 2, 1, 6)
	adm.set(next)
	if err := runner.Admit(next); !errors.Is(err, errGenerationTasksNotJoined) {
		t.Fatalf("Admit during drain = %v", err)
	}
	if join, err := runner.Sync(ctlCtx(t), next, true); !errors.Is(err, errGenerationTasksNotJoined) || join.Joined {
		t.Fatalf("Sync during drain = %#v, %v", join, err)
	}
	close(release)
	genWatch(t, "late return", lateReturned)
	if lateLanded.Load() != 0 || !reflect.DeepEqual(effects.snapshot(), before) {
		t.Fatalf("late return of the fenced generation mutated state: landed=%d", lateLanded.Load())
	}
	if join := runner.Withdraw(ctlCtx(t), key); !join.Joined || len(join.Pending) != 0 {
		t.Fatalf("Withdraw after late return = %#v", join)
	}
	if failures.Load() != 0 {
		t.Fatalf("cancelled or late errors were reported %d times", failures.Load())
	}
	if got := genTaskState(runner.Status(), "startup-scan"); got != ebusGenerationTaskCancelled {
		t.Fatalf("late task state = %s", got)
	}
	if _, err := runner.Sync(ctlCtx(t), next, true); err != nil {
		t.Fatalf("successor after join: %v", err)
	}
	if status := runner.Status(); status.Key != next || status.Phase != ebusGenerationRunning {
		t.Fatalf("successor status = %#v", status)
	}
}

func TestGenerationTasksFencedGenerationCannotMutate(t *testing.T) {
	adm := &genAdmission{}
	effects := &genEffects{}
	scopes := make(chan *ebusGenerationScope, 2)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "promote", Run: func(_ context.Context, scope *ebusGenerationScope) error { scopes <- scope; return nil }},
	}})
	old := genKey("inc.e1", 1, 1, 1)
	adm.set(old)
	if err := runner.Admit(old); err != nil {
		t.Fatal(err)
	}
	oldScope := <-scopes
	oldCallback := oldScope.Callback(func() { effects.mu.Lock(); effects.b503++; effects.mu.Unlock() })
	if !oldCallback() || effects.mutateAll(oldScope) != 5 {
		t.Fatal("live generation could not take effect")
	}
	if !runner.Fence(old) {
		t.Fatal("Fence of the owning key reported false")
	}
	before := effects.snapshot()
	if oldScope.Live() || oldCallback() || effects.mutateAll(oldScope) != 0 || oldScope.Go(func(context.Context) { t.Error("helper ran after fence") }) {
		t.Fatal("fenced generation kept authority")
	}
	if oldScope.Context().Err() == nil {
		t.Fatal("fenced generation context not cancelled")
	}
	if join := runner.Withdraw(ctlCtx(t), old); !join.Joined {
		t.Fatalf("Withdraw = %#v", join)
	}
	next := genKey("inc.e2", 1, 2, 2)
	adm.set(next)
	if err := runner.Admit(next); err != nil {
		t.Fatal(err)
	}
	nextScope := <-scopes
	// The old callback stays dead while the successor's works.
	if oldCallback() || effects.mutateAll(oldScope) != 0 {
		t.Fatal("old generation mutated state after the successor was admitted")
	}
	if !reflect.DeepEqual(effects.snapshot(), before) {
		t.Fatal("effects changed between fence and successor")
	}
	if effects.mutateAll(nextScope) != 5 || effects.snapshot().activeSource != next {
		t.Fatal("successor could not take effect")
	}
}

func TestGenerationTasksAdmissionWithdrawalFencesBeforeRunnerFence(t *testing.T) {
	adm := &genAdmission{}
	effects := &genEffects{}
	scopes := make(chan *ebusGenerationScope, 1)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "promote", Run: func(_ context.Context, scope *ebusGenerationScope) error { scopes <- scope; return nil }},
	}})
	key := genKey("inc.e1", 1, 1, 1)
	adm.set(key)
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	scope := <-scopes
	adm.withdraw() // the STOP fence of the lifecycle owner, before any Sync
	if scope.Live() || effects.mutateAll(scope) != 0 || scope.Go(func(context.Context) {}) {
		t.Fatal("generation took effect after its admission was withdrawn")
	}
	if !reflect.DeepEqual(effects.snapshot(), genEffectsView{}) {
		t.Fatal("effects changed")
	}
}

func TestGenerationTasksFenceExcludesInFlightCommit(t *testing.T) {
	adm := &genAdmission{}
	scopes := make(chan *ebusGenerationScope, 1)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "promote", Run: func(_ context.Context, scope *ebusGenerationScope) error { scopes <- scope; return nil }},
	}})
	key := genKey("inc.e1", 1, 1, 1)
	adm.set(key)
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	scope := <-scopes
	inCommit := make(chan struct{})
	release := make(chan struct{})
	var installed atomic.Bool
	go scope.Commit(func() { close(inCommit); <-release; installed.Store(true) })
	genWatch(t, "commit entry", inCommit)
	fenced := make(chan struct{})
	go func() { runner.Fence(key); close(fenced) }()
	select {
	case <-fenced:
		t.Fatal("Fence returned while a commit was installing")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	genWatch(t, "fence", fenced)
	if !installed.Load() {
		t.Fatal("Fence returned before the in-flight install completed")
	}
	if scope.Commit(func() { t.Error("commit after Fence") }) {
		t.Fatal("commit accepted after Fence returned")
	}
}

func TestGenerationTasksFenceReportsFalseWhenGateTimesOut(t *testing.T) {
	adm := &genAdmission{}
	effects := &genEffects{}
	scopes := make(chan *ebusGenerationScope, 1)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, JoinTimeout: 50 * time.Millisecond, Tasks: []ebusGenerationTask{
		{Name: "promote", Run: func(_ context.Context, scope *ebusGenerationScope) error { scopes <- scope; return nil }},
	}})
	key := genKey("inc.e1", 1, 1, 1)
	adm.set(key)
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	scope := <-scopes
	inCommit := make(chan struct{})
	release := make(chan struct{})
	var installed atomic.Bool
	go scope.Commit(func() { close(inCommit); <-release; installed.Store(true) })
	genWatch(t, "commit entry", inCommit)
	if runner.Fence(key) {
		t.Fatal("Fence reported success before serialization completed")
	}
	if runner.Fence(key) {
		t.Fatal("repeated Fence reported success before serialization completed")
	}
	if status := runner.Status(); status.Phase != ebusGenerationDraining {
		t.Fatalf("phase = %s, want DRAINING", status.Phase)
	}
	before := effects.snapshot()
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for !runner.Fence(key) {
		if time.Now().After(deadline) {
			t.Fatal("Fence never completed after the in-flight effect returned")
		}
		time.Sleep(time.Millisecond)
	}
	if !installed.Load() {
		t.Fatal("in-flight install did not complete")
	}
	if effects.mutateAll(scope) != 0 || !reflect.DeepEqual(effects.snapshot(), before) {
		t.Fatal("an effect of the fenced generation landed")
	}
}

func TestGenerationTasksRepeatedSyncSharesOneFenceWaiter(t *testing.T) {
	adm := &genAdmission{}
	scopes := make(chan *ebusGenerationScope, 1)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, JoinTimeout: 5 * time.Millisecond, Tasks: []ebusGenerationTask{
		{Name: "promote", Run: func(_ context.Context, scope *ebusGenerationScope) error { scopes <- scope; return nil }},
	}})
	key := genKey("inc.e1", 1, 1, 1)
	adm.set(key)
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	scope := <-scopes
	inCommit := make(chan struct{})
	release := make(chan struct{})
	go scope.Commit(func() { close(inCommit); <-release })
	genWatch(t, "commit entry", inCommit)
	adm.withdraw()
	base := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		if _, err := runner.Sync(context.Background(), ebusGenerationKey{}, false); err == nil {
			t.Fatal("Sync joined a generation with a stuck effect")
		}
	}
	if grown := runtime.NumGoroutine() - base; grown > 3 {
		t.Fatalf("goroutines grew by %d across repeated Sync calls", grown)
	}
	close(release)
}

// --- acceptance 3: failure never spawns a generation or a retry owner ---

func TestGenerationTasksFailureReportsOnceAndNeverRestarts(t *testing.T) {
	adm := &genAdmission{}
	key := genKey("inc.e1", 1, 1, 1)
	adm.set(key)
	type report struct {
		key  ebusGenerationKey
		task string
		err  error
	}
	reports := make(chan report, 8)
	var validate, dependent atomic.Int64
	boom := errors.New("validation refused")
	runner := genRunner(t, ebusGenerationTaskConfig{
		Admitted: adm.admitted,
		Failure:  func(k ebusGenerationKey, task string, err error) { reports <- report{k, task, err} },
		Tasks: []ebusGenerationTask{
			{Name: "source-validation", Run: func(context.Context, *ebusGenerationScope) error { validate.Add(1); return boom }},
			{Name: "revalidate", Requires: []string{"source-validation"}, Run: func(context.Context, *ebusGenerationScope) error { dependent.Add(1); return nil }},
			{Name: "promote", Requires: []string{"revalidate"}, Run: func(context.Context, *ebusGenerationScope) error { dependent.Add(1); return nil }},
			{Name: "idle", Run: genNop},
		},
	})
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	var got report
	select {
	case got = <-reports:
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog: no failure report")
	}
	if got.key != key || got.task != "source-validation" || !errors.Is(got.err, boom) {
		t.Fatalf("report = %#v", got)
	}
	ctlWait(t, "dependents skipped", func() bool {
		status := runner.Status()
		return genTaskState(status, "revalidate") == ebusGenerationTaskSkipped && genTaskState(status, "promote") == ebusGenerationTaskSkipped &&
			genTaskState(status, "idle") == ebusGenerationTaskSucceeded
	})
	// Completion and failure leave the generation as it was: nothing
	// restarts, nothing new is admitted, and repeated wakes do not rerun.
	for i := 0; i < 4; i++ {
		if err := runner.Admit(key); err != nil {
			t.Fatal(err)
		}
	}
	status := runner.Status()
	if status.Key != key || status.Phase != ebusGenerationRunning || len(status.Pending) != 0 {
		t.Fatalf("status = %#v", status)
	}
	if validate.Load() != 1 || dependent.Load() != 0 || len(reports) != 0 {
		t.Fatalf("validate=%d dependent=%d extra reports=%d", validate.Load(), dependent.Load(), len(reports))
	}
	if genTaskState(status, "source-validation") != ebusGenerationTaskFailed {
		t.Fatalf("failed task state = %s", genTaskState(status, "source-validation"))
	}
}

func TestGenerationTasksFailureAfterWithdrawalIsNotReported(t *testing.T) {
	adm := &genAdmission{}
	key := genKey("inc.e1", 1, 1, 1)
	adm.set(key)
	var reports atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})
	runner := genRunner(t, ebusGenerationTaskConfig{
		Admitted: adm.admitted,
		Failure:  func(ebusGenerationKey, string, error) { reports.Add(1) },
		Tasks: []ebusGenerationTask{{Name: "scan", Run: func(context.Context, *ebusGenerationScope) error {
			close(started)
			<-release
			return errors.New("bus error")
		}}},
	})
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	genWatch(t, "scan start", started)
	adm.withdraw()
	close(release)
	ctlWait(t, "scan settled", func() bool { return genTaskState(runner.Status(), "scan") == ebusGenerationTaskFailed })
	if reports.Load() != 0 {
		t.Fatal("failure of an unadmitted generation reached the lifecycle owner")
	}
}

// --- key ownership: every field is part of the owning key ---

func TestGenerationTasksEveryKeyFieldIsOwned(t *testing.T) {
	adm := &genAdmission{}
	key := genKey("inc.e1", 2, 3, 9)
	adm.set(key)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "poll", Run: func(ctx context.Context, _ *ebusGenerationScope) error { <-ctx.Done(); return nil }},
	}})
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	variants := map[string]func(*ebusGenerationKey){
		"DriverID":   func(k *ebusGenerationKey) { k.Control.DriverID = "ebus.secondary" },
		"SourceID":   func(k *ebusGenerationKey) { k.Control.SourceID = "source.other" },
		"Epoch":      func(k *ebusGenerationKey) { k.Control.Epoch = "inc.e0" },
		"Generation": func(k *ebusGenerationKey) { k.Control.Generation = 1 },
		"Sequence":   func(k *ebusGenerationKey) { k.Control.Sequence = 4 },
		"Native":     func(k *ebusGenerationKey) { k.Native = 10 },
	}
	for field, mutate := range variants {
		variant := key
		mutate(&variant)
		if runner.Fence(variant) {
			t.Fatalf("%s variant fenced the owning generation", field)
		}
		if join := runner.Withdraw(ctlCtx(t), variant); !join.Joined || join.Key != variant {
			t.Fatalf("%s variant Withdraw = %#v", field, join)
		}
		adm.set(variant)
		if err := runner.Admit(variant); !errors.Is(err, errGenerationTasksBusy) {
			t.Fatalf("%s variant Admit = %v, want busy", field, err)
		}
		adm.set(key)
		if status := runner.Status(); status.Phase != ebusGenerationRunning || status.Key != key {
			t.Fatalf("%s variant disturbed the owner: %#v", field, status)
		}
	}
}

// --- acceptance 4: process-stable handles and retained evidence ---

func TestGenerationTasksRetainedEvidenceSurvivesStoppedInterval(t *testing.T) {
	adm := &genAdmission{}
	store := &genEvidence{} // process-scoped: never owned by the runner
	clock := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var mu sync.Mutex
	now := func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	advance := func(d time.Duration) { mu.Lock(); clock = clock.Add(d); mu.Unlock() }
	scopes := make(chan *ebusGenerationScope, 2)
	handles := map[*genEvidence]int{}
	var handlesMu sync.Mutex
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "capture", Run: func(_ context.Context, scope *ebusGenerationScope) error {
			handlesMu.Lock()
			handles[store]++
			handlesMu.Unlock()
			scope.Commit(func() {
				store.mu.Lock()
				store.records = append(store.records, genRecord{Provenance: scope.Key(), ObservedAt: now(), Value: "identity"})
				store.mu.Unlock()
			})
			scopes <- scope
			return nil
		}},
	}})
	first := genKey("inc.e1", 1, 1, 1)
	adm.set(first)
	if err := runner.Admit(first); err != nil {
		t.Fatal(err)
	}
	oldScope := <-scopes
	retained := store.all()
	if len(retained) != 1 || retained[0].Provenance != first {
		t.Fatalf("records = %#v", retained)
	}
	adm.withdraw()
	if _, err := runner.Sync(ctlCtx(t), ebusGenerationKey{}, false); err != nil {
		t.Fatal(err)
	}
	// Stopped interval: time passes, capture is paused, and a late write of
	// the stopped generation cannot relabel or refresh retained records.
	advance(time.Hour)
	if oldScope.Commit(func() { store.records[0].ObservedAt = now() }) {
		t.Fatal("stopped generation refreshed a retained record")
	}
	if !reflect.DeepEqual(store.all(), retained) {
		t.Fatalf("retained records changed while stopped: %#v", store.all())
	}
	second := genKey("inc.e2", 1, 2, 2)
	adm.set(second)
	if _, err := runner.Sync(ctlCtx(t), second, true); err != nil {
		t.Fatal(err)
	}
	<-scopes
	records := store.all()
	if len(records) != 2 || !reflect.DeepEqual(records[0], retained[0]) {
		t.Fatalf("retained record not preserved with provenance and age: %#v", records)
	}
	if records[1].Provenance != second || !records[1].ObservedAt.Equal(retained[0].ObservedAt.Add(time.Hour)) {
		t.Fatalf("new capture = %#v", records[1])
	}
	handlesMu.Lock()
	defer handlesMu.Unlock()
	if len(handles) != 1 || handles[store] != 2 {
		t.Fatalf("process handle not stable across generations: %#v", handles)
	}
}

func TestGenerationTasksCloseRefusesLaterAdmission(t *testing.T) {
	adm := &genAdmission{}
	key := genKey("inc.e1", 1, 1, 1)
	adm.set(key)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "poll", Run: func(ctx context.Context, _ *ebusGenerationScope) error { <-ctx.Done(); return ctx.Err() }},
	}})
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(ctlCtx(t)); err != nil {
		t.Fatal(err)
	}
	if status := runner.Status(); status.Phase != ebusGenerationClosed || len(status.Pending) != 0 {
		t.Fatalf("status = %#v", status)
	}
	next := genKey("inc.e2", 1, 2, 2)
	adm.set(next)
	if err := runner.Admit(next); !errors.Is(err, errGenerationTasksClosed) {
		t.Fatalf("Admit after Close = %v", err)
	}
}

// --- composition with the lifecycle-only ebus.primary bridge ---

func TestGenerationTasksBridgeStartStopRestartAndManagerReplacement(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 2})
	effects := &genEffects{}
	var mu sync.Mutex
	validated := []ebusGenerationKey{}
	scopes := map[ebusGenerationKey]*ebusGenerationScope{}
	var failOnce atomic.Bool
	failOnce.Store(true)
	failNow := make(chan struct{})
	runner := genRunner(t, ebusGenerationTaskConfig{
		Admitted: ebusControlGenerationAdmitted(plant.bridge),
		Failure: func(key ebusGenerationKey, _ string, _ error) {
			// The only path to a native retry: the manager decides.
			plant.bridge.ReportFailure(key.Control, drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}})
		},
		Tasks: []ebusGenerationTask{
			{Name: "source-validation", Run: func(_ context.Context, scope *ebusGenerationScope) error {
				mu.Lock()
				validated = append(validated, scope.Key())
				scopes[scope.Key()] = scope
				mu.Unlock()
				return nil
			}},
			{Name: "startup-scan", Requires: []string{"source-validation"}, Run: func(ctx context.Context, _ *ebusGenerationScope) error {
				if failOnce.CompareAndSwap(true, false) {
					select {
					case <-failNow:
					case <-ctx.Done():
						return ctx.Err()
					}
					return errors.New("scan lost the bus")
				}
				return nil
			}},
			{Name: "semantic-poll", Requires: []string{"source-validation"}, Run: func(ctx context.Context, _ *ebusGenerationScope) error {
				<-ctx.Done()
				return ctx.Err()
			}},
		},
	})
	validations := func() []ebusGenerationKey {
		mu.Lock()
		defer mu.Unlock()
		return append([]ebusGenerationKey(nil), validated...)
	}
	scopeOf := func(key ebusGenerationKey) *ebusGenerationScope { mu.Lock(); defer mu.Unlock(); return scopes[key] }

	// Prepared but not started: no work before readiness.
	if _, err := runner.SyncEBusControl(ctlCtx(t), plant.bridge); err != nil {
		t.Fatal(err)
	}
	if len(validations()) != 0 || runner.Status().Phase != ebusGenerationIdle {
		t.Fatal("work ran before the bridge admitted a generation")
	}

	corr := ctlCorr("inc.e1", 1, 3)
	if err := plant.bridge.Start(ctlCtx(t), corr); err != nil {
		t.Fatal(err)
	}
	first, ok := ebusControlGeneration(plant.bridge)
	if !ok || first.Control != ctlKey(corr) {
		t.Fatalf("admitted generation = %#v, %v", first, ok)
	}
	for i := 0; i < 3; i++ {
		if _, err := runner.SyncEBusControl(ctlCtx(t), plant.bridge); err != nil {
			t.Fatal(err)
		}
	}
	ctlWait(t, "first validation", func() bool { return len(validations()) == 1 })
	close(failNow)
	// startup-scan fails once: the report goes to the bridge, DriverManager
	// performs exactly one same-epoch replacement, and the runner itself
	// starts nothing.
	ctlWait(t, "manager replacement", func() bool {
		key, ok := ebusControlGeneration(plant.bridge)
		return ok && key.Native > first.Native
	})
	second, _ := ebusControlGeneration(plant.bridge)
	if second.Control.Epoch != first.Control.Epoch || second.Control.Generation != 2 {
		t.Fatalf("replacement key = %#v", second)
	}
	if plant.constructed() != 2 || plant.hub.count("AdmitSuccessor") != 0 || plant.overlapped() {
		t.Fatalf("constructed=%d successions=%d overlap=%v", plant.constructed(), plant.hub.count("AdmitSuccessor"), plant.overlapped())
	}
	if got := validations(); len(got) != 1 || got[0] != first {
		t.Fatalf("runner ran tasks for an unsynced generation: %#v", got)
	}
	// The predecessor lost authority at the bridge before any Sync.
	if old := scopeOf(first); old.Live() || effects.mutateAll(old) != 0 {
		t.Fatal("replaced generation kept authority")
	}
	if join, err := runner.SyncEBusControl(ctlCtx(t), plant.bridge); err != nil || !join.Joined || join.Key != first {
		t.Fatalf("Sync(replacement) = %#v, %v", join, err)
	}
	ctlWait(t, "replacement validation", func() bool { return len(validations()) == 2 })
	if got := validations(); got[1] != second {
		t.Fatalf("validations = %#v", got)
	}
	if effects.mutateAll(scopeOf(second)) != 5 {
		t.Fatal("current generation could not take effect")
	}
	before := effects.snapshot()

	// STOP: the bridge fence withdraws authority immediately; Sync joins.
	if out := plant.bridge.Stop(ctlCtx(t), ctlCorr("inc.e1", 1, 4)); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("Stop = %#v", out)
	}
	if effects.mutateAll(scopeOf(second)) != 0 || !reflect.DeepEqual(effects.snapshot(), before) {
		t.Fatal("stopped generation mutated state")
	}
	if join, err := runner.SyncEBusControl(ctlCtx(t), plant.bridge); err != nil || !join.Joined || join.Key != second {
		t.Fatalf("Sync(stop) = %#v, %v", join, err)
	}
	if status := runner.Status(); status.Phase != ebusGenerationIdle || len(status.Pending) != 0 {
		t.Fatalf("stopped status = %#v", status)
	}

	// Later START: source validation reruns under the successor epoch.
	if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e2", 1, 5)); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.SyncEBusControl(ctlCtx(t), plant.bridge); err != nil {
		t.Fatal(err)
	}
	ctlWait(t, "later START validation", func() bool { return len(validations()) == 3 })
	third := validations()[2]
	if third.Control.Epoch != "inc.e2" || third.Control.Generation != 1 || third.Native <= second.Native {
		t.Fatalf("later START key = %#v", third)
	}
	if plant.constructed() != 3 {
		t.Fatalf("constructed = %d, want 3 (one per admitted generation)", plant.constructed())
	}
}

func TestGenerationTasksBridgePredicateOwnsEveryKeyField(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e1", 1, 3)); err != nil {
		t.Fatal(err)
	}
	key, ok := ebusControlGeneration(plant.bridge)
	admitted := ebusControlGenerationAdmitted(plant.bridge)
	if !ok || !admitted(key) {
		t.Fatalf("current key not admitted: %#v", key)
	}
	// Each field caught: a predicate ignoring it would admit another
	// driver, a foreign source, a prior epoch, a fenced driver generation,
	// a superseded operation or a stale native mapping.
	variants := map[string]func(*ebusGenerationKey){
		"DriverID":   func(k *ebusGenerationKey) { k.Control.DriverID = "ebus.secondary" },
		"SourceID":   func(k *ebusGenerationKey) { k.Control.SourceID = "source.other" },
		"Epoch":      func(k *ebusGenerationKey) { k.Control.Epoch = "inc.e0" },
		"Generation": func(k *ebusGenerationKey) { k.Control.Generation++ },
		"Sequence":   func(k *ebusGenerationKey) { k.Control.Sequence++ },
		"Native":     func(k *ebusGenerationKey) { k.Native++ },
	}
	for field, mutate := range variants {
		variant := key
		mutate(&variant)
		if admitted(variant) {
			t.Fatalf("bridge predicate admitted a key with a different %s", field)
		}
	}
	if out := plant.bridge.Stop(ctlCtx(t), ctlCorr("inc.e1", 1, 4)); !out.NativeClosed {
		t.Fatalf("Stop = %#v", out)
	}
	if admitted(key) {
		t.Fatal("stopped generation still admitted")
	}
	if _, ok := ebusControlGeneration(plant.bridge); ok {
		t.Fatal("stopped bridge reports an admitted generation")
	}
}

// A stale reconciliation snapshot must not fence a generation that the
// lifecycle owner still admits.
func TestGenerationTasksStaleSyncDoesNotFenceAdmittedGeneration(t *testing.T) {
	adm := &genAdmission{}
	scopes := make(chan *ebusGenerationScope, 1)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "promote", Run: func(_ context.Context, scope *ebusGenerationScope) error { scopes <- scope; return nil }},
	}})
	older, current := genKey("inc.e1", 1, 1, 1), genKey("inc.e1", 1, 2, 2)
	adm.set(current)
	if err := runner.Admit(current); err != nil {
		t.Fatal(err)
	}
	scope := <-scopes
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A delayed snapshot taken while stopped, then one carrying an older key.
	_, _ = runner.Sync(ctx, ebusGenerationKey{}, false)
	_, _ = runner.Sync(ctx, older, true)
	if !scope.Live() || !scope.Commit(func() {}) {
		t.Fatal("stale snapshot fenced a still-admitted generation")
	}
	if _, err := runner.Sync(ctx, current, true); err != nil {
		t.Fatalf("current wake after stale snapshots: %v", err)
	}
	if st := runner.Status(); st.Phase != ebusGenerationRunning && st.Phase != ebusGenerationIdle {
		t.Fatalf("phase = %v", st.Phase)
	}
	adm.withdraw()
	if join, err := runner.Sync(ctx, ebusGenerationKey{}, false); err != nil || !join.Joined {
		t.Fatalf("withdrawn generation not joined: %+v %v", join, err)
	}
}

// A helper queued by Go must re-check admission when it starts executing:
// the lifecycle owner withdraws admission between launch and start.
func TestGenerationTasksDelayedHelperDoesNotRunAfterWithdrawal(t *testing.T) {
	var (
		mu    sync.Mutex
		ok    bool
		cur   ebusGenerationKey
		armed bool
		calls int
	)
	admitted := func(key ebusGenerationKey) bool {
		mu.Lock()
		defer mu.Unlock()
		if armed {
			if calls++; calls == 2 { // the helper's own start check
				ok = false
			}
		}
		return ok && cur == key
	}
	scopes := make(chan *ebusGenerationScope, 1)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: admitted, Tasks: []ebusGenerationTask{
		{Name: "poll", Run: func(_ context.Context, scope *ebusGenerationScope) error { scopes <- scope; return nil }},
	}})
	key := genKey("inc.e1", 1, 1, 1)
	mu.Lock()
	cur, ok = key, true
	mu.Unlock()
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	scope := <-scopes
	var ran atomic.Bool
	mu.Lock()
	armed = true
	mu.Unlock()
	if !scope.Go(func(context.Context) { ran.Store(true) }) {
		t.Fatal("Go refused while admitted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if join := runner.Withdraw(ctx, key); !join.Joined {
		t.Fatalf("not joined: %+v", join)
	}
	if ran.Load() {
		t.Fatal("helper ran after admission was withdrawn")
	}
}

func TestGenerationTasksBlockedFailureHookDoesNotStallWithdrawal(t *testing.T) {
	adm := &genAdmission{}
	inFailure := make(chan struct{})
	release := make(chan struct{})
	var reports atomic.Int64
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted,
		JoinTimeout: 50 * time.Millisecond,
		Failure:     func(ebusGenerationKey, string, error) { reports.Add(1); close(inFailure); <-release },
		Tasks: []ebusGenerationTask{
			{Name: "scan", Run: func(context.Context, *ebusGenerationScope) error { return errors.New("boom") }},
		}})
	key := genKey("inc.e1", 1, 1, 1)
	adm.set(key)
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	genWatch(t, "failure report", inFailure)

	type result struct {
		join ebusGenerationJoin
		sync error
		clos error
	}
	done := make(chan result, 1)
	go func() {
		var r result
		r.join = runner.Withdraw(context.Background(), key)
		adm.withdraw()
		_, r.sync = runner.Sync(context.Background(), key, false)
		r.clos = runner.Close(context.Background())
		done <- r
	}()
	var r result
	select {
	case r = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog: withdrawal stalled behind a blocked failure hook")
	}
	if r.join.Joined || len(r.join.Pending) == 0 {
		t.Fatalf("blocked hook reported as joined: %+v", r.join)
	}
	if !errors.Is(r.sync, errGenerationTasksNotJoined) || !errors.Is(r.clos, errGenerationTasksNotJoined) {
		t.Fatalf("sync=%v close=%v, want not-joined", r.sync, r.clos)
	}
	if st := runner.Status(); st.Phase != ebusGenerationDraining {
		t.Fatalf("phase = %v, want draining", st.Phase)
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if join := runner.Withdraw(ctx, key); !join.Joined {
		t.Fatalf("late return not joined: %+v", join)
	}
	if got := reports.Load(); got != 1 {
		t.Fatalf("reports = %d, want 1", got)
	}
}

func TestGenerationTasksTaskInvokedAfterFenceHasNoAuthority(t *testing.T) {
	adm := &genAdmission{}
	paused := make(chan struct{})
	release := make(chan struct{})
	type seen struct {
		cancelled bool
		landed    bool
	}
	got := make(chan seen, 1)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "scan", Run: func(ctx context.Context, scope *ebusGenerationScope) error {
			got <- seen{cancelled: ctx.Err() != nil, landed: scope.Commit(func() {}) || scope.Go(func(context.Context) {})}
			return nil
		}},
	}})
	runner.beforeRun = func() { close(paused); <-release }
	key := genKey("inc.e1", 1, 1, 1)
	adm.set(key)
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	genWatch(t, "startup pause", paused)
	if !runner.Fence(key) {
		t.Fatal("Fence did not own the key")
	}
	close(release)
	select {
	case s := <-got:
		if !s.cancelled || s.landed {
			t.Fatalf("task after Fence: %+v, want cancelled context and no effect", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog: task never returned")
	}
}

func TestGenerationTasksAdmitValidatesUnderRunnerLock(t *testing.T) {
	var (
		mu  sync.Mutex
		ok  = true
		cur = genKey("inc.e1", 1, 1, 1)
	)
	inPredicate := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	admitted := func(key ebusGenerationKey) bool {
		mu.Lock()
		stale := ok && cur == key
		mu.Unlock()
		once.Do(func() { close(inPredicate); <-release })
		return stale
	}
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: admitted, Tasks: []ebusGenerationTask{
		{Name: "scan", Run: genNop},
	}})
	done := make(chan error, 1)
	go func() { done <- runner.Admit(cur) }()
	genWatch(t, "predicate", inPredicate)
	mu.Lock()
	ok = false
	mu.Unlock()
	synced := make(chan struct{})
	go func() {
		_, _ = runner.Sync(context.Background(), ebusGenerationKey{}, false)
		close(synced)
	}()
	select {
	case <-synced:
		t.Fatal("Sync reconciled while an admission was mid-validation")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	genWatch(t, "sync", synced)
	<-done
	// Whatever Admit decided, Sync ran after it: no stale run may remain.
	if phase := runner.Status().Phase; phase == ebusGenerationRunning {
		t.Fatalf("stale admission left the runner %s", phase)
	}
}

func TestGenerationTasksCallbackOutlivingTaskIsJoinedBeforeSuccessor(t *testing.T) {
	adm := &genAdmission{}
	callbacks := make(chan func() bool, 1)
	inCallback := make(chan struct{})
	release := make(chan struct{})
	var landed atomic.Bool
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted,
		JoinTimeout: 50 * time.Millisecond,
		Tasks: []ebusGenerationTask{
			{Name: "promote", Run: func(_ context.Context, scope *ebusGenerationScope) error {
				callbacks <- scope.Callback(func() { close(inCallback); <-release; landed.Store(true) })
				return nil
			}},
		}})
	key := genKey("inc.e1", 1, 1, 1)
	adm.set(key)
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	cb := <-callbacks
	// The registering task has returned; the callback now runs on its own.
	go cb()
	genWatch(t, "callback entry", inCallback)

	join := runner.Withdraw(context.Background(), key)
	if join.Joined || len(join.Pending) == 0 {
		t.Fatalf("in-flight callback not awaited: %+v", join)
	}
	adm.withdraw()
	next := genKey("inc.e2", 2, 2, 2)
	adm.set(next)
	if err := runner.Admit(next); !errors.Is(err, errGenerationTasksNotJoined) {
		t.Fatalf("successor admitted over a running callback: %v", err)
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if join := runner.Withdraw(ctx, key); !join.Joined {
		t.Fatalf("late callback not joined: %+v", join)
	}
	if !landed.Load() {
		t.Fatal("joined before the callback finished")
	}
}

func TestGenerationTasksWithdrawUsesOneBoundAcrossFenceAndJoin(t *testing.T) {
	adm := &genAdmission{}
	scopes := make(chan *ebusGenerationScope, 1)
	const bound = 300 * time.Millisecond
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, JoinTimeout: bound,
		Tasks: []ebusGenerationTask{
			{Name: "promote", Run: func(_ context.Context, scope *ebusGenerationScope) error { scopes <- scope; return nil }},
		}})
	key := genKey("inc.e1", 1, 1, 1)
	adm.set(key)
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	scope := <-scopes
	inCommit := make(chan struct{})
	release := make(chan struct{})
	go scope.Commit(func() { close(inCommit); <-release })
	genWatch(t, "commit entry", inCommit)
	defer close(release)

	start := time.Now()
	join := runner.Withdraw(context.Background(), key)
	elapsed := time.Since(start)
	if join.Joined {
		t.Fatalf("joined while a commit holds the gate: %+v", join)
	}
	if elapsed >= bound+bound/2 {
		t.Fatalf("withdraw took %v, want one bound of %v", elapsed, bound)
	}
}

// A join obligation registered through GoJoin must run even when the
// generation is fenced between acceptance and execution, so the join
// awaits what the obligation awaits.
func TestGenerationTasksJoinObligationRunsWhenFencedBeforeStart(t *testing.T) {
	var (
		mu    sync.Mutex
		ok    bool
		cur   ebusGenerationKey
		armed bool
		calls int
	)
	admitted := func(key ebusGenerationKey) bool {
		mu.Lock()
		defer mu.Unlock()
		if armed {
			if calls++; calls == 2 { // the helper's own start check
				ok = false
			}
		}
		return ok && cur == key
	}
	scopes := make(chan *ebusGenerationScope, 1)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: admitted, Tasks: []ebusGenerationTask{
		{Name: "poll", Run: func(_ context.Context, scope *ebusGenerationScope) error { scopes <- scope; return nil }},
	}})
	key := genKey("inc.e1", 1, 1, 1)
	mu.Lock()
	cur, ok = key, true
	mu.Unlock()
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	scope := <-scopes
	var ran atomic.Bool
	mu.Lock()
	armed = true
	mu.Unlock()
	if !scope.GoJoin(func(context.Context) { ran.Store(true) }) {
		t.Fatal("GoJoin refused while admitted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if join := runner.Withdraw(ctx, key); !join.Joined {
		t.Fatalf("not joined: %+v", join)
	}
	if !ran.Load() {
		t.Fatal("join obligation was skipped after the fence; the join would not await it")
	}
}

// p13ReturnedScope observes task completion under the runner lock; receiving
// the scope alone would not establish that its registering task has returned.
func p13ReturnedScope(t *testing.T, adm *genAdmission, key ebusGenerationKey) (*ebusGenerationTaskRunner, *ebusGenerationScope) {
	t.Helper()
	scopes := make(chan *ebusGenerationScope, 1)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, Tasks: []ebusGenerationTask{
		{Name: "promote", Run: func(_ context.Context, scope *ebusGenerationScope) error { scopes <- scope; return nil }},
	}})
	adm.set(key)
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	var scope *ebusGenerationScope
	watchdog := time.NewTimer(5 * time.Second)
	defer watchdog.Stop()
	for {
		select {
		case scope = <-scopes:
		case <-watchdog.C:
			t.Fatal("watchdog: registering task did not return")
		default:
		}
		runner.mu.Lock()
		returned := runner.current.states[0] == ebusGenerationTaskSucceeded
		runner.mu.Unlock()
		if scope != nil && returned {
			return runner, scope
		}
		runtime.Gosched()
	}
}

func TestP13ExpiredWithdrawalTracksInvocationsAfterTaskReturn(t *testing.T) {
	for _, kind := range []string{"commit", "callback"} {
		t.Run(kind, func(t *testing.T) {
			adm := &genAdmission{}
			key := genKey("inc.e1", 1, 1, 1)
			runner, scope := p13ReturnedScope(t, adm, key)
			expected := []string{"promote/" + kind}
			invoke := scope.Commit
			if kind == "callback" {
				invoke = func(fn func()) bool { return scope.Callback(fn)() }
			}
			var releases [2]func()
			var finished [2]chan struct{}
			for i := range releases {
				release, done := make(chan struct{}), make(chan struct{})
				finished[i] = done
				var once sync.Once
				releases[i] = func() { once.Do(func() { close(release) }) }
				unblock := releases[i]
				t.Cleanup(func() {
					unblock()
					genWatch(t, "invocation cleanup", done)
				})
				entered := make(chan struct{})
				go func() {
					defer close(done)
					if !invoke(func() { close(entered); <-release }) {
						t.Error("admitted invocation refused")
					}
				}()
				genWatch(t, "invocation entry", entered)
			}
			// Both invocations own the same diagnostic unit independently.
			runner.mu.Lock()
			active, units := scope.run.active, scope.run.units[expected[0]]
			runner.mu.Unlock()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			join := runner.Withdraw(ctx, key)
			if join.Joined || join.Key != key || !reflect.DeepEqual(join.Pending, expected) {
				t.Fatalf("expired withdrawal = %+v, want unjoined with %v", join, expected)
			}
			if active != 2 || units != 2 {
				t.Fatalf("active=%d units=%d, want two retained invocations", active, units)
			}
			next := genKey("inc.e2", 2, 2, 2)
			adm.set(next)
			if err := runner.Admit(next); !errors.Is(err, errGenerationTasksNotJoined) {
				t.Fatalf("successor overlapped predecessor: %v", err)
			}
			for i := range releases {
				releases[i]()
				genWatch(t, "invocation return", finished[i])
				if i == 0 {
					if got := runner.Status().Pending; !reflect.DeepEqual(got, expected) {
						t.Fatalf("remaining invocation lost: %v", got)
					}
				}
			}
			if join := runner.Withdraw(ctlCtx(t), key); !join.Joined || len(join.Pending) != 0 {
				t.Fatalf("completed invocations not joined: %+v", join)
			}
			if err := runner.Admit(next); err != nil {
				t.Fatalf("successor refused after join: %v", err)
			}
		})
	}
}

func TestP13InvocationPanicAndRefusalDoNotLeakWork(t *testing.T) {
	for _, kind := range []string{"commit", "callback"} {
		t.Run(kind, func(t *testing.T) {
			adm := &genAdmission{}
			key := genKey("inc.e1", 1, 1, 1)
			runner, scope := p13ReturnedScope(t, adm, key)
			invoke := scope.Commit
			if kind == "callback" {
				invoke = func(fn func()) bool { return scope.Callback(fn)() }
			}
			func() {
				defer func() {
					if got := recover(); got != "synthetic panic" {
						t.Errorf("panic = %v", got)
					}
				}()
				invoke(func() { panic("synthetic panic") })
			}()
			variants := map[string]func(*ebusGenerationKey){
				"DriverID":   func(k *ebusGenerationKey) { k.Control.DriverID = "ebus.secondary" },
				"SourceID":   func(k *ebusGenerationKey) { k.Control.SourceID = "source.other" },
				"Epoch":      func(k *ebusGenerationKey) { k.Control.Epoch = "inc.e2" },
				"Generation": func(k *ebusGenerationKey) { k.Control.Generation = 2 },
				"Sequence":   func(k *ebusGenerationKey) { k.Control.Sequence = 2 },
				"Native":     func(k *ebusGenerationKey) { k.Native = 2 },
			}
			for field, change := range variants {
				other := key
				change(&other)
				adm.set(other)
				if invoke(func() { t.Errorf("effect admitted with mismatched %s", field) }) {
					t.Errorf("invocation admitted with mismatched %s", field)
				}
				adm.set(key)
				if !invoke(func() {}) {
					t.Errorf("positive control refused after %s", field)
				}
			}
			if join := runner.Withdraw(ctlCtx(t), key); !join.Joined || len(join.Pending) != 0 {
				t.Fatalf("panic/refusal leaked work: %+v", join)
			}
			if invoke(func() { t.Error("fenced invocation ran") }) {
				t.Error("fenced invocation accepted")
			}
			runner.mu.Lock()
			active, units := scope.run.active, len(scope.run.units)
			runner.mu.Unlock()
			if active != 0 || units != 0 {
				t.Fatalf("cleanup active=%d units=%d", active, units)
			}
		})
	}
}

// These direct-helper oracles distinguish both necessary join guards even
// though an executing invocation normally holds the gate as well as a unit.
func TestP13SettleRequiresActiveDrainAndExclusiveGate(t *testing.T) {
	for _, barrier := range []string{"active", "exclusive-gate"} {
		t.Run(barrier, func(t *testing.T) {
			key := genKey("inc.e1", 1, 1, 1)
			next := genKey("inc.e2", 2, 2, 2)
			run := &ebusGenerationRun{key: key, drained: make(chan struct{}), units: map[string]int{}}
			run.fenced.Store(true)
			if barrier == "active" {
				run.serialized = true
				run.enterLocked("promote/callback")
			}
			runner := &ebusGenerationTaskRunner{current: run, admitted: func(ebusGenerationKey) bool { return true }}
			run.settleLocked()
			if err := runner.Admit(next); !errors.Is(err, errGenerationTasksNotJoined) {
				t.Fatalf("successor bypassed %s: %v", barrier, err)
			}
			if run.joined {
				t.Fatal("joined before required barrier")
			}
			if barrier == "active" {
				run.exitLocked("promote/callback")
			} else {
				run.serialized = true
				run.settleLocked()
			}
			if !run.joined {
				t.Fatal("completed barrier did not join")
			}
			genWatch(t, "drained barrier", run.drained)
		})
	}
}

// A real in-flight invocation also holds the shared gate. Supplying the
// serialized condition directly isolates active accounting from that redundant
// guard, so removing callback accounting cannot hide behind gate exclusion.
func TestP13ActiveInvocationRequiresJoinAfterParentReturn(t *testing.T) {
	for _, kind := range []string{"commit", "callback"} {
		t.Run(kind, func(t *testing.T) {
			adm := &genAdmission{}
			key := genKey("inc.e1", 1, 1, 1)
			runner, scope := p13ReturnedScope(t, adm, key)
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(func() { unblock(); genWatch(t, "invocation cleanup", done) })
			invoke := scope.Commit
			if kind == "callback" {
				invoke = func(fn func()) bool { return scope.Callback(fn)() }
			}
			go func() {
				defer close(done)
				if !invoke(func() { close(entered); <-release }) {
					t.Error("admitted invocation refused")
				}
			}()
			genWatch(t, "invocation entry", entered)
			runner.mu.Lock()
			scope.run.fenced.Store(true)
			scope.run.serialized = true
			scope.run.settleLocked()
			joined := scope.run.joined
			scope.run.serialized = false
			runner.mu.Unlock()
			if joined {
				t.Fatal("active invocation reported Joined after its parent returned")
			}
			unblock()
			genWatch(t, "invocation return", done)
			if join := runner.Withdraw(ctlCtx(t), key); !join.Joined {
				t.Fatalf("finished invocation not joined: %+v", join)
			}
		})
	}
}
