package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

// This file is the generation-owned post-start task runner of the
// lifecycle-only ebus.primary driver. It is callable composition only:
// nothing in process startup constructs it, and the existing post-start
// scan, polling, revalidation, promotion and source-validation code paths
// are unchanged. Shell integration supplies the tasks, which wrap those
// existing native functions.
//
// Ownership split:
//   - The runner owns task startup, cancellation, completion and join for
//     exactly one admitted generation at a time. It owns no protocol
//     semantics and no process-scoped store or handle.
//   - DriverManager stays the only native retry owner. A task never starts,
//     restarts or replaces a driver generation; a task failure is only
//     reported, gated by the generation's admission, through Failure.
//   - Process-stable handles, retained evidence and caches are owned by the
//     process. Tasks close over them; the runner never resets, moves or
//     relabels them, so retained records keep their provenance and age
//     across stopped intervals.
//
// Serialization point: every effect a task, helper or callback installs
// (active source, publication, poll result, B503 or proxy work) goes
// through scope.Commit, which re-checks the runner fence and the external
// admission predicate under the runner gate. Fence takes that gate
// exclusively, so once Fence returns no effect of the fenced generation can
// land. Fence and Withdraw must not be called while holding a lock that
// the Admitted predicate takes.

const (
	defaultEBusGenerationJoinTimeout = 5 * time.Second
	maxEBusGenerationTasks           = 32
	maxEBusGenerationTaskName        = 64
)

var (
	errGenerationTasksInvalid   = errors.New("ebus generation tasks: invalid configuration")
	errGenerationTasksKey       = errors.New("ebus generation tasks: incomplete generation key")
	errGenerationTasksNotReady  = errors.New("ebus generation tasks: generation not admitted")
	errGenerationTasksStale     = errors.New("ebus generation tasks: generation ended or older than one already admitted")
	errGenerationTasksBusy      = errors.New("ebus generation tasks: another generation still owns tasks")
	errGenerationTasksNotJoined = errors.New("ebus generation tasks: predecessor tasks not joined")
	errGenerationTasksClosed    = errors.New("ebus generation tasks: runner closed")

	ebusGenerationTaskNamePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`)
)

// ebusGenerationKey is the full admitted generation correlation that owns a
// task set: the lifecycle key (driver, source, epoch, driver generation and
// lifecycle sequence) plus its explicit native-generation mapping. Native
// generation is process monotonic, so a key never repeats in a process.
type ebusGenerationKey struct {
	Control ebusControlKey
	Native  runtimecontrol.NativeGeneration
}

func (key ebusGenerationKey) complete() bool {
	c := key.Control
	return c.DriverID != "" && c.SourceID != "" && c.Epoch != "" && c.Generation != 0 && c.Sequence != 0 && key.Native != 0
}

// ebusGenerationTask is one post-start action. Run is invoked at most once
// per admitted generation, after every task named in Requires returned nil
// in that same generation. A polling loop iterates inside Run until ctx is
// cancelled; "once per generation" describes task startup, not one poll.
// A task that returns (with or without error) is never restarted by the
// runner.
type ebusGenerationTask struct {
	Name     string
	Requires []string
	Run      func(ctx context.Context, scope *ebusGenerationScope) error
}

// ebusGenerationTaskConfig is the composition input of the runner.
//
// Admitted is the external admission predicate (for ebus.primary, the
// bridge's current admitted generation). It is required, consulted before
// any task starts and on every Commit, and must be cheap and non-blocking.
// Failure, when set, receives task failures of a generation that is still
// admitted; it is the only path from a task to the lifecycle owner.
type ebusGenerationTaskConfig struct {
	Tasks       []ebusGenerationTask
	Admitted    func(ebusGenerationKey) bool
	Failure     func(key ebusGenerationKey, task string, err error)
	JoinTimeout time.Duration
}

type ebusGenerationTaskState string

const (
	ebusGenerationTaskPending   ebusGenerationTaskState = "PENDING"
	ebusGenerationTaskRunning   ebusGenerationTaskState = "RUNNING"
	ebusGenerationTaskSucceeded ebusGenerationTaskState = "SUCCEEDED"
	ebusGenerationTaskFailed    ebusGenerationTaskState = "FAILED"
	ebusGenerationTaskCancelled ebusGenerationTaskState = "CANCELLED"
	ebusGenerationTaskSkipped   ebusGenerationTaskState = "SKIPPED"
)

type ebusGenerationPhase string

const (
	// ebusGenerationIdle: no generation owns tasks (none admitted yet, or
	// the last one is fenced and fully joined).
	ebusGenerationIdle ebusGenerationPhase = "IDLE"
	// ebusGenerationRunning: an admitted, unfenced generation owns tasks.
	ebusGenerationRunning ebusGenerationPhase = "RUNNING"
	// ebusGenerationDraining: the generation is fenced but some of its work
	// has not returned; no successor may start tasks.
	ebusGenerationDraining ebusGenerationPhase = "DRAINING"
	// ebusGenerationClosed: the runner was closed and its last generation
	// fully joined.
	ebusGenerationClosed ebusGenerationPhase = "CLOSED"
)

type ebusGenerationTaskStatus struct {
	Name  string
	State ebusGenerationTaskState
}

// ebusGenerationStatus is a cached view; it performs no task or native call.
type ebusGenerationStatus struct {
	Key     ebusGenerationKey
	Phase   ebusGenerationPhase
	Tasks   []ebusGenerationTaskStatus
	Pending []string
}

// ebusGenerationJoin reports the drain of one fenced generation. Joined is
// true only when every task, helper and callback of that generation
// returned; Pending names the work units still running otherwise.
type ebusGenerationJoin struct {
	Key     ebusGenerationKey
	Joined  bool
	Pending []string
}

type ebusGenerationTaskSpec struct {
	name     string
	requires []int
	run      func(context.Context, *ebusGenerationScope) error
}

// ebusGenerationTaskRunner runs the frozen task set once per admitted
// generation and cancels and joins it when the generation ends.
type ebusGenerationTaskRunner struct {
	tasks       []ebusGenerationTaskSpec
	admitted    func(ebusGenerationKey) bool
	failure     func(ebusGenerationKey, string, error)
	joinTimeout time.Duration

	// beforeRun, when set by a test before the first Admit, runs between a
	// task's startup liveness check and its invocation.
	beforeRun func()

	// gate is the effect serialization point. Commit, Go and task startup
	// hold it shared; Fence holds it exclusively. Lock order: gate, then mu.
	gate sync.RWMutex

	mu         sync.Mutex
	current    *ebusGenerationRun
	lastNative runtimecontrol.NativeGeneration
	closed     bool
}

type ebusGenerationRun struct {
	runner *ebusGenerationTaskRunner
	key    ebusGenerationKey
	ctx    context.Context
	cancel context.CancelFunc

	// fenced is written under gate held exclusively and read under gate or
	// lock-free for status.
	fenced atomic.Bool

	// Guarded by runner.mu.
	states  []ebusGenerationTaskState
	active  int
	units   map[string]int
	drained chan struct{}
	joined  bool
}

// ebusGenerationScope is the handle a task receives. Every method is fenced
// by the owning generation.
type ebusGenerationScope struct {
	run  *ebusGenerationRun
	task string
}

// newEBusGenerationTaskRunner validates and freezes the task set. It runs
// nothing; tasks start only on Admit of an admitted generation.
func newEBusGenerationTaskRunner(cfg ebusGenerationTaskConfig) (*ebusGenerationTaskRunner, error) {
	if cfg.Admitted == nil {
		return nil, fmt.Errorf("%w: an admission predicate is required", errGenerationTasksInvalid)
	}
	if len(cfg.Tasks) == 0 || len(cfg.Tasks) > maxEBusGenerationTasks {
		return nil, fmt.Errorf("%w: between 1 and %d tasks are required", errGenerationTasksInvalid, maxEBusGenerationTasks)
	}
	index := make(map[string]int, len(cfg.Tasks))
	specs := make([]ebusGenerationTaskSpec, 0, len(cfg.Tasks))
	for i, task := range cfg.Tasks {
		if len(task.Name) > maxEBusGenerationTaskName || !ebusGenerationTaskNamePattern.MatchString(task.Name) {
			return nil, fmt.Errorf("%w: task %d has a noncanonical name", errGenerationTasksInvalid, i)
		}
		if _, dup := index[task.Name]; dup {
			return nil, fmt.Errorf("%w: duplicate task %q", errGenerationTasksInvalid, task.Name)
		}
		if task.Run == nil {
			return nil, fmt.Errorf("%w: task %q has no function", errGenerationTasksInvalid, task.Name)
		}
		spec := ebusGenerationTaskSpec{name: task.Name, run: task.Run}
		seen := map[int]bool{}
		for _, name := range task.Requires {
			// Prerequisites must be declared earlier, so the task graph is
			// acyclic by construction.
			at, ok := index[name]
			if !ok || seen[at] {
				return nil, fmt.Errorf("%w: task %q requires unknown, later or repeated task %q", errGenerationTasksInvalid, task.Name, name)
			}
			seen[at] = true
			spec.requires = append(spec.requires, at)
		}
		index[task.Name] = i
		specs = append(specs, spec)
	}
	join := cfg.JoinTimeout
	if join <= 0 {
		join = defaultEBusGenerationJoinTimeout
	}
	return &ebusGenerationTaskRunner{tasks: specs, admitted: cfg.Admitted, failure: cfg.Failure, joinTimeout: join}, nil
}

// Admit starts the task set for key. It is idempotent for the generation
// that already owns tasks, so repeated wakes never duplicate a task. It
// refuses an incomplete or unadmitted key, a key that ended or is older than
// one already admitted, and any key while a predecessor is unfenced or not
// fully joined.
func (runner *ebusGenerationTaskRunner) Admit(key ebusGenerationKey) error {
	if !key.complete() {
		return errGenerationTasksKey
	}
	// Admission is validated under the runner lock, so a concurrent Sync
	// either sees the installed run or runs after the predicate observed
	// the withdrawal. The predicate must not call back into the runner.
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if !runner.admitted(key) {
		return errGenerationTasksNotReady
	}
	if runner.closed {
		return errGenerationTasksClosed
	}
	if cur := runner.current; cur != nil {
		if cur.key == key {
			if cur.fenced.Load() {
				return errGenerationTasksStale
			}
			return nil
		}
		if !cur.fenced.Load() {
			return errGenerationTasksBusy
		}
		if !cur.joined {
			return errGenerationTasksNotJoined
		}
	}
	if key.Native <= runner.lastNative {
		return errGenerationTasksStale
	}
	ctx, cancel := context.WithCancel(context.Background())
	run := &ebusGenerationRun{runner: runner, key: key, ctx: ctx, cancel: cancel,
		states: make([]ebusGenerationTaskState, len(runner.tasks)), units: map[string]int{}, drained: make(chan struct{})}
	for i := range run.states {
		run.states[i] = ebusGenerationTaskPending
	}
	runner.current, runner.lastNative = run, key.Native
	for i, spec := range runner.tasks {
		if len(spec.requires) == 0 {
			run.launchLocked(i)
		}
	}
	return nil
}

// Fence immediately withdraws the authority of key's generation: its
// context is cancelled and, once Fence returns, no Commit, callback, helper
// or task start of that generation succeeds. It does not wait for tasks to
// return; Withdraw also joins them. It reports whether key owned tasks.
func (runner *ebusGenerationTaskRunner) Fence(key ebusGenerationKey) bool {
	runner.mu.Lock()
	run := runner.current
	runner.mu.Unlock()
	if run == nil || run.key != key {
		return false
	}
	run.fence(context.Background())
	return true
}

// Withdraw fences key's generation and joins all of its work within the
// join timeout or ctx, whichever ends first. A generation that does not
// join stays DRAINING and keeps every successor's tasks blocked; a later
// Withdraw (or Sync) observes the late return.
func (runner *ebusGenerationTaskRunner) Withdraw(ctx context.Context, key ebusGenerationKey) ebusGenerationJoin {
	runner.mu.Lock()
	run := runner.current
	runner.mu.Unlock()
	if run == nil || run.key != key {
		// Only the newest generation can own tasks: an older key was fully
		// joined before its successor was admitted.
		return ebusGenerationJoin{Key: key, Joined: true}
	}
	run.fence(ctx)
	return run.join(ctx)
}

// Sync reconciles the runner with the lifecycle owner's current view: key
// is the currently admitted generation when admitted is true. A generation
// that is no longer current is fenced and joined (bounded); the current one
// is admitted once its predecessor joined. The returned join describes the
// predecessor drain, if one was pending.
func (runner *ebusGenerationTaskRunner) Sync(ctx context.Context, key ebusGenerationKey, admitted bool) (ebusGenerationJoin, error) {
	runner.mu.Lock()
	run := runner.current
	runner.mu.Unlock()
	var join ebusGenerationJoin
	// A snapshot taken before this call may be stale: only withdraw a run
	// whose own key the lifecycle owner no longer admits right now.
	if run != nil && (!admitted || run.key != key) && !runner.admitted(run.key) {
		run.fence(ctx)
		if join = run.join(ctx); !join.Joined {
			return join, errGenerationTasksNotJoined
		}
	}
	if !admitted {
		return join, nil
	}
	if run != nil && run.key != key && runner.admitted(run.key) {
		return join, errGenerationTasksBusy
	}
	return join, runner.Admit(key)
}

// Close refuses later admissions, fences the current generation and joins
// it within the join timeout or ctx.
func (runner *ebusGenerationTaskRunner) Close(ctx context.Context) error {
	runner.mu.Lock()
	runner.closed = true
	run := runner.current
	runner.mu.Unlock()
	if run == nil {
		return nil
	}
	run.fence(ctx)
	if join := run.join(ctx); !join.Joined {
		return fmt.Errorf("%w: %v", errGenerationTasksNotJoined, join.Pending)
	}
	return nil
}

// Status returns the cached runner view.
func (runner *ebusGenerationTaskRunner) Status() ebusGenerationStatus {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	run := runner.current
	if run == nil {
		phase := ebusGenerationIdle
		if runner.closed {
			phase = ebusGenerationClosed
		}
		return ebusGenerationStatus{Phase: phase}
	}
	status := ebusGenerationStatus{Key: run.key, Pending: run.pendingLocked()}
	switch {
	case !run.fenced.Load():
		status.Phase = ebusGenerationRunning
	case !run.joined:
		status.Phase = ebusGenerationDraining
	case runner.closed:
		status.Phase = ebusGenerationClosed
	default:
		status.Phase = ebusGenerationIdle
	}
	for i, spec := range runner.tasks {
		status.Tasks = append(status.Tasks, ebusGenerationTaskStatus{Name: spec.name, State: run.states[i]})
	}
	return status
}

// live reports, under the gate, whether the run may still take effect.
func (run *ebusGenerationRun) liveLocked() bool {
	return !run.fenced.Load() && run.ctx.Err() == nil && run.runner.admitted(run.key)
}

// fence withdraws the generation's authority. The fenced flag is raised
// before the gate is taken, so no new effect of the generation lands while
// the exclusive acquisition waits for in-flight effects. A hook or effect
// that blocks while holding the shared gate cannot stall the caller past the
// join bound or ctx: the generation then stays unjoined (DRAINING) and the
// blocked unit is reported as pending.
func (run *ebusGenerationRun) fence(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	run.fenced.Store(true)
	run.cancel()
	runner := run.runner
	locked := make(chan struct{})
	go func() {
		runner.gate.Lock()
		defer runner.gate.Unlock()
		close(locked)
	}()
	timer := time.NewTimer(runner.joinTimeout)
	defer timer.Stop()
	select {
	case <-locked:
	case <-timer.C:
	case <-ctx.Done():
	}
	runner.mu.Lock()
	run.settleLocked()
	runner.mu.Unlock()
}

func (run *ebusGenerationRun) join(ctx context.Context) ebusGenerationJoin {
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(run.runner.joinTimeout)
	defer timer.Stop()
	select {
	case <-run.drained:
	case <-timer.C:
	case <-ctx.Done():
	}
	runner := run.runner
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return ebusGenerationJoin{Key: run.key, Joined: run.joined, Pending: run.pendingLocked()}
}

// settleLocked marks the run joined once it is fenced and nothing runs.
func (run *ebusGenerationRun) settleLocked() {
	if run.fenced.Load() && run.active == 0 && !run.joined {
		run.joined = true
		close(run.drained)
	}
}

func (run *ebusGenerationRun) pendingLocked() []string {
	names := make([]string, 0, len(run.units))
	for name := range run.units {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (run *ebusGenerationRun) enterLocked(unit string) {
	run.active++
	run.units[unit]++
}

func (run *ebusGenerationRun) exitLocked(unit string) {
	run.active--
	if run.units[unit]--; run.units[unit] <= 0 {
		delete(run.units, unit)
	}
	run.settleLocked()
}

func (run *ebusGenerationRun) launchLocked(i int) {
	spec := run.runner.tasks[i]
	run.states[i] = ebusGenerationTaskRunning
	run.enterLocked(spec.name)
	go run.execute(i)
}

func (run *ebusGenerationRun) execute(i int) {
	runner := run.runner
	spec := runner.tasks[i]
	scope := &ebusGenerationScope{run: run, task: spec.name}

	// No work starts before readiness or after withdrawal.
	runner.gate.RLock()
	live := run.liveLocked()
	runner.gate.RUnlock()
	var err error
	if live {
		// A fence can land between the check and the invocation. The task
		// then starts with a cancelled context and every scope effect
		// refuses, so nothing of a fenced generation lands.
		if runner.beforeRun != nil {
			runner.beforeRun()
		}
		err = spec.run(run.ctx, scope)
	}

	state := ebusGenerationTaskSucceeded
	switch {
	case !live:
		state = ebusGenerationTaskCancelled
	case err != nil && (run.fenced.Load() || run.ctx.Err() != nil):
		state = ebusGenerationTaskCancelled
	case err != nil:
		state = ebusGenerationTaskFailed
	}
	if state == ebusGenerationTaskFailed && runner.failure != nil {
		// The report is delivered under the shared gate, so Fence cannot
		// complete between the liveness check and the hook. The hook must
		// not call back into the runner. The lifecycle owner re-checks the
		// key at its own serialization point.
		runner.gate.RLock()
		if run.liveLocked() {
			runner.failure(run.key, spec.name, err)
		}
		runner.gate.RUnlock()
	}

	runner.mu.Lock()
	defer runner.mu.Unlock()
	run.states[i] = state
	run.advanceLocked()
	run.exitLocked(spec.name)
}

// advanceLocked starts every pending task whose prerequisites all
// succeeded, and skips every pending task with a prerequisite that ended
// otherwise. Prerequisites precede their dependents, so one forward pass
// reaches a fixed point.
func (run *ebusGenerationRun) advanceLocked() {
	for i, spec := range run.runner.tasks {
		if run.states[i] != ebusGenerationTaskPending {
			continue
		}
		blocked, ready := false, true
		for _, at := range spec.requires {
			switch run.states[at] {
			case ebusGenerationTaskSucceeded:
			case ebusGenerationTaskPending, ebusGenerationTaskRunning:
				ready = false
			default:
				blocked = true
			}
		}
		switch {
		case blocked || (ready && run.fenced.Load()):
			run.states[i] = ebusGenerationTaskSkipped
			continue
		case !ready:
			continue
		}
		run.launchLocked(i)
	}
}

// Key returns the generation that owns the scope.
func (scope *ebusGenerationScope) Key() ebusGenerationKey { return scope.run.key }

// Context is cancelled when the generation is fenced.
func (scope *ebusGenerationScope) Context() context.Context { return scope.run.ctx }

// Live reports whether the generation is unfenced and still admitted.
func (scope *ebusGenerationScope) Live() bool {
	runner := scope.run.runner
	runner.gate.RLock()
	defer runner.gate.RUnlock()
	return scope.run.liveLocked()
}

// Commit runs fn only while the generation is unfenced and admitted, and
// excludes Fence while fn runs. fn must be a short, non-blocking install of
// an already computed result and must not call back into the runner.
func (scope *ebusGenerationScope) Commit(fn func()) bool {
	runner := scope.run.runner
	runner.gate.RLock()
	defer runner.gate.RUnlock()
	if !scope.run.liveLocked() {
		return false
	}
	fn()
	return true
}

// Callback wraps fn for registration with a native subscription or
// callback seam. Each invocation runs through Commit, so invocations after
// the generation is fenced are dropped and report false.
func (scope *ebusGenerationScope) Callback(fn func()) func() bool {
	return func() bool { return scope.Commit(fn) }
}

// Go runs fn as additional work of the generation, joined with its tasks.
// It refuses (false) once the generation is fenced or unadmitted. fn
// receives the generation context.
func (scope *ebusGenerationScope) Go(fn func(context.Context)) bool {
	run := scope.run
	runner := run.runner
	unit := scope.task + "/go"
	runner.gate.RLock()
	if !run.liveLocked() {
		runner.gate.RUnlock()
		return false
	}
	runner.mu.Lock()
	run.enterLocked(unit)
	runner.mu.Unlock()
	runner.gate.RUnlock()
	go func() {
		defer func() {
			runner.mu.Lock()
			run.exitLocked(unit)
			runner.mu.Unlock()
		}()
		// The generation may have been fenced between the launch and now.
		runner.gate.RLock()
		live := run.liveLocked()
		runner.gate.RUnlock()
		if live {
			fn(run.ctx)
		}
	}()
	return true
}

// ebusControlGeneration derives the currently admitted generation key of
// the ebus.primary bridge from one cached view.
func ebusControlGeneration(bridge *ebusControlRuntime) (ebusGenerationKey, bool) {
	view := bridge.View()
	if !view.Admitted || view.Native == 0 {
		return ebusGenerationKey{}, false
	}
	return ebusGenerationKey{Control: view.Key, Native: view.Native}, true
}

// ebusControlGenerationAdmitted is the runner admission predicate over the
// bridge: key is admitted only while it is exactly the bridge's admitted
// generation, including its native mapping.
func ebusControlGenerationAdmitted(bridge *ebusControlRuntime) func(ebusGenerationKey) bool {
	return func(key ebusGenerationKey) bool {
		current, ok := ebusControlGeneration(bridge)
		return ok && current == key
	}
}

// SyncEBusControl reconciles the runner with the bridge's admitted
// generation. It performs no native start, retry or hub call.
func (runner *ebusGenerationTaskRunner) SyncEBusControl(ctx context.Context, bridge *ebusControlRuntime) (ebusGenerationJoin, error) {
	key, ok := ebusControlGeneration(bridge)
	return runner.Sync(ctx, key, ok)
}
