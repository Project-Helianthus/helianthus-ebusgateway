// Package drivermanager owns protocol-neutral in-process driver lifecycle state.
package drivermanager

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrDriverNotFound    = errors.New("driver not found")
	ErrUnavailable       = errors.New("driver unavailable")
	ErrStopTimeout       = errors.New("driver stop timed out")
	ErrSafetyQuarantined = errors.New("driver safety quarantined")
	ErrManagerClosed     = errors.New("driver manager is shutting down")
	ErrHooksBound        = errors.New("driver lifecycle hooks already bound")
)

const defaultLateRetireTimeout = 2 * time.Second

type DesiredState string

const (
	DesiredRunning DesiredState = "RUNNING"
	DesiredStopped DesiredState = "STOPPED"
)

type ObservedState string

const (
	ObservedDisabled ObservedState = "DISABLED"
	ObservedStopped  ObservedState = "STOPPED"
	ObservedStarting ObservedState = "STARTING"
	ObservedRunning  ObservedState = "RUNNING"
	ObservedDegraded ObservedState = "DEGRADED"
	ObservedBackoff  ObservedState = "BACKOFF"
	ObservedStopping ObservedState = "STOPPING"
	ObservedFailed   ObservedState = "FAILED"
)

type ReasonCode string

const (
	ReasonNone                  ReasonCode = "NONE"
	ReasonConfigDisabled        ReasonCode = "CONFIG_DISABLED"
	ReasonOperatorStopped       ReasonCode = "OPERATOR_STOPPED"
	ReasonStartRequested        ReasonCode = "START_REQUESTED"
	ReasonStopRequested         ReasonCode = "STOP_REQUESTED"
	ReasonConfigInvalid         ReasonCode = "CONFIG_INVALID"
	ReasonProviderUnavailable   ReasonCode = "PROVIDER_UNAVAILABLE"
	ReasonDependencyUnavailable ReasonCode = "DEPENDENCY_UNAVAILABLE"
	ReasonRuntimeNotReady       ReasonCode = "RUNTIME_NOT_READY"
	ReasonCapabilityDegraded    ReasonCode = "CAPABILITY_DEGRADED"
	ReasonRetryScheduled        ReasonCode = "RETRY_SCHEDULED"
	ReasonRetryExhausted        ReasonCode = "RETRY_EXHAUSTED"
	ReasonStopTimeout           ReasonCode = "STOP_TIMEOUT"
	ReasonCloseUnconfirmed      ReasonCode = "CLOSE_UNCONFIRMED"
	ReasonInternalError         ReasonCode = "INTERNAL_ERROR"
)

type Capability string

const (
	CapabilityDiscovery          Capability = "DISCOVERY"
	CapabilityRead               Capability = "READ"
	CapabilityWrite              Capability = "WRITE"
	CapabilityPairing            Capability = "PAIRING"
	CapabilityTopology           Capability = "TOPOLOGY"
	CapabilityRawEvidence        Capability = "RAW_EVIDENCE"
	CapabilitySemanticProjection Capability = "SEMANTIC_PROJECTION"
)

type Reason struct {
	Code      ReasonCode
	Retryable bool
}

func (reason Reason) String() string { return string(reason.Code) }

type RetrySnapshot struct {
	Eligible bool
	// BudgetRemaining counts the fast attempts left, the scheduled one
	// included. It is zero in the persistent phase.
	BudgetRemaining int
	NotBeforeUTC    time.Time
	// Attempt is the ordinal, from one, of the scheduled retry within the
	// current failure series; every earlier retry of the series failed. A
	// success, an explicit Start or Replace and a new reported failure open
	// a new series.
	Attempt int
	// Persistent reports that the driver's policy continues this series
	// past the fast budget while the driver is desired RUNNING: only a
	// non-retryable failure, a safety quarantine or an explicit Stop ends it.
	Persistent bool
}

// Correlation fences callbacks from both stale generations and superseded
// lifecycle operations. Operation is zero only during stable RUNNING/DEGRADED.
type Correlation struct {
	Generation uint64
	Operation  uint64
}

type Snapshot struct {
	DriverID              string
	DesiredState          DesiredState
	ObservedState         ObservedState
	Reason                Reason
	Retry                 *RetrySnapshot
	Generation            uint64
	Revision              uint64
	Attempt               uint64
	Capabilities          []Capability
	EffectiveCapabilities []Capability
	SafetyQuarantined     bool
	ActiveOperation       uint64
	ChangedAtUTC          time.Time
}

type Failure struct {
	Reason Reason
}

type RetryPolicy struct {
	Budget       int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	// JitterRatio is the symmetric jitter bound around each exponential
	// delay. Zero selects the default 20%; a negative value disables jitter.
	JitterRatio float64
	// Persistent continues a retryable failure series once the Budget fast
	// attempts are spent. The zero value keeps the bounded behavior: the
	// series ends FAILED/RETRY_EXHAUSTED.
	Persistent PersistentRetry
}

// PersistentRetry is the phase that follows the fast attempts of a retry
// series while the driver is desired RUNNING. Its delays restart at
// InitialDelay and double up to MaxDelay, with the policy's JitterRatio; a
// delay is always clamped to [InitialDelay, MaxDelay]. Only a retryable
// failure continues the series, so a non-retryable failure (configuration
// rejected, a stale or foreign identity), a safety quarantine and an
// explicit Stop still end it. Start and Replace reset the schedule.
type PersistentRetry struct {
	Enabled      bool
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// Runtime is the protocol-neutral lifecycle boundary implemented by adapters.
// Stop must honor ctx cancellation; Manager supplies an independent deadline
// when retiring a provider admitted after its original operation was canceled.
type Runtime interface {
	Start(context.Context) (uint64, error)
	Replace(context.Context) (uint64, error)
	Stop(context.Context) error
	Generation() uint64
	Revision() uint64
	SafetyQuarantined() bool
}

// withdrawalFencer is an optional internal hook for runtimes that expose
// generation-local work outside Manager.Invoke (for example, an adapter proxy
// listener). The hook runs while the manager state lock is held immediately
// before STOPPING is published, so it MUST be non-blocking and MUST NOT call
// back into Manager. Resource drain and close proof remain Runtime.Stop's job.
type withdrawalFencer interface {
	FenceWithdrawal()
}

// Admission keeps a provider handle private until its guarded callback runs.
type Admission struct {
	Correlation Correlation
	Invoke      func(func(any) error) error
	Release     func() error
}

// AdmittingRuntime is optional for runtimes that expose provider invocations.
type AdmittingRuntime interface {
	Runtime
	Admit(context.Context) (*Admission, error)
}

// correlationBinder is an optional internal lifecycle hook. It lets an
// adapter bind generation-owned asynchronous health signals only after the
// manager has published the corresponding stable RUNNING correlation.
type correlationBinder interface {
	BindCorrelation(Correlation)
}

// correlationActivator is an optional pre-admission lifecycle hook. Manager
// invokes it under the driver state lock after a provider generation has been
// constructed but immediately before RUNNING publishes effective capability.
// Implementations MUST be bounded and MUST NOT call back into Manager. Health
// reporters that may re-enter Manager belong in correlationBinder instead.
type correlationActivator interface {
	ActivateCorrelation(Correlation)
}

// CloseClass is the native close evidence of one manager withdrawal. Only
// the proven classes are close proof; a nil Stop result is classified, never
// trusted on its own.
type CloseClass string

const (
	// CloseNotRequired means nothing was open: the driver was already stopped
	// or disabled and no construction was still in flight.
	CloseNotRequired CloseClass = "NOT_REQUIRED"
	// CloseProven means the runtime drained and proved the close.
	CloseProven CloseClass = "PROVEN"
	// CloseProvenAfterDrainTimeout means the bounded drain expired but the
	// close was subsequently confirmed. It is restartable.
	CloseProvenAfterDrainTimeout CloseClass = "PROVEN_AFTER_DRAIN_TIMEOUT"
	// CloseUnconfirmed means the close could not be proven. The driver is
	// safety quarantined and replacement is blocked.
	CloseUnconfirmed CloseClass = "UNCONFIRMED"
	// CloseInternalError means the runtime failed without a classified close.
	// It is never close proof.
	CloseInternalError CloseClass = "INTERNAL_ERROR"
)

// Proven reports whether the class is native close proof.
func (class CloseClass) Proven() bool {
	switch class {
	case CloseNotRequired, CloseProven, CloseProvenAfterDrainTimeout:
		return true
	default:
		return false
	}
}

// ClassifyClose maps one Runtime.Stop result to its close class. Quarantine
// dominates: a runtime that reports safety quarantine never proves a close,
// whatever Stop returned.
func ClassifyClose(err error, quarantined bool) CloseClass {
	switch {
	case quarantined || errors.Is(err, ErrSafetyQuarantined):
		return CloseUnconfirmed
	case err == nil:
		return CloseProven
	case errors.Is(err, ErrStopTimeout):
		return CloseProvenAfterDrainTimeout
	default:
		return CloseInternalError
	}
}

// CloseReport is the classified result of one manager withdrawal of a
// driver. Generation is the last admitted native generation the withdrawal
// covered (zero when none was admitted) and Operation is the manager
// operation that performed it.
type CloseReport struct {
	DriverID   string
	Class      CloseClass
	Generation uint64
	Operation  uint64
	Observed   ObservedState
	Reason     Reason
}

// AdmissionHold describes one constructed native generation whose admission
// is held closed: observed state is still STARTING and no effective
// capability is published while the hold runs.
type AdmissionHold struct {
	DriverID string
	// Correlation is the held native generation and the manager operation
	// that constructed it.
	Correlation Correlation
	// Previous is the last native generation the manager admitted, or zero.
	Previous uint64
	// Replacement reports a Runtime.Replace construction.
	Replacement bool
	// Retry reports a manager-owned retry attempt (scheduled by the manager
	// after a failed attempt or a reported failure), not an explicit call.
	Retry bool
}

// LifecycleHooks are optional manager hooks bound by a lifecycle bridge.
// Both run with every manager lock released, so they may perform hub work,
// persistence or other bounded blocking work. They must not call Start,
// Replace, Stop, StopWithReport or Shutdown of the same manager; snapshot and
// watch reads are allowed.
type LifecycleHooks struct {
	// HoldAdmission runs after a provider generation was constructed and
	// before RUNNING and its effective capabilities publish. A nil result lets
	// the manager publish both together in one state transition, provided
	// the operation is still current. An error retires the held generation
	// on an independent bounded deadline and fails the attempt through the
	// driver's ordinary classification and retry policy. The context ends
	// when the attempt is canceled.
	HoldAdmission func(context.Context, AdmissionHold) error
	// Closed receives the classified close of every manager withdrawal that
	// called Runtime.Stop or found nothing to close: explicit stops, a
	// superseded late admission and a failed admission hold.
	Closed func(CloseReport)
}

type DriverConfig struct {
	ID            string
	Enabled       bool
	Runtime       Runtime
	Capabilities  []Capability
	ClassifyError func(error) Failure
	Retry         RetryPolicy
	Hooks         LifecycleHooks
}

type Config struct {
	Drivers     []DriverConfig
	Now         func() time.Time
	RetryJitter func(time.Duration) time.Duration
	// RetryWait waits out the delay of one scheduled retry and reports
	// whether the retry is due; it returns false once ctx ends. Nil selects a
	// timer. Tests inject it to drive a retry schedule without real time.
	RetryWait         func(ctx context.Context, delay time.Duration) bool
	LateRetireTimeout time.Duration
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc
	now    func() time.Time
	jitter func(time.Duration) time.Duration
	wait   func(context.Context, time.Duration) bool
	// lateRetireTimeout bounds cleanup of a provider that completed admission
	// after its manager operation had already been superseded.
	lateRetireTimeout time.Duration

	drivers map[string]*managedDriver
	wg      sync.WaitGroup
	once    sync.Once
	taskMu  sync.Mutex
	closed  atomic.Bool
}

type managedDriver struct {
	cfg DriverConfig

	opMu sync.Mutex
	mu   sync.RWMutex

	desired         DesiredState
	observed        ObservedState
	reason          Reason
	retry           *RetrySnapshot
	generation      uint64
	revision        uint64
	attempt         uint64
	effective       []Capability
	quarantined     bool
	changedAt       time.Time
	operationSeq    uint64
	activeOperation uint64
	retryRemaining  int
	// retrySeries counts the retries scheduled in the current failure
	// series; it resets together with retryRemaining.
	retrySeries       int
	retryToken        uint64
	retryCancel       context.CancelFunc
	retryNeedsReplace bool
	needsReplace      bool
	activeAttempt     *attemptControl
	hooks             LifecycleHooks
	hooksBound        bool
	changed           chan struct{}
}

type attemptControl struct {
	cancel context.CancelFunc
}

func New(cfg Config) (*Manager, error) {
	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	jitter := cfg.RetryJitter
	if jitter == nil {
		jitter = defaultRetryJitter
	}
	wait := cfg.RetryWait
	if wait == nil {
		wait = defaultRetryWait
	}
	ctx, cancel := context.WithCancel(context.Background())
	lateRetireTimeout := cfg.LateRetireTimeout
	if lateRetireTimeout <= 0 {
		lateRetireTimeout = defaultLateRetireTimeout
	}
	manager := &Manager{
		ctx:               ctx,
		cancel:            cancel,
		now:               now,
		jitter:            jitter,
		wait:              wait,
		lateRetireTimeout: lateRetireTimeout,
		drivers:           make(map[string]*managedDriver, len(cfg.Drivers)),
	}
	for _, driverCfg := range cfg.Drivers {
		id := strings.TrimSpace(driverCfg.ID)
		if id == "" || id != driverCfg.ID {
			cancel()
			return nil, errors.New("driver id is empty or non-canonical")
		}
		if _, exists := manager.drivers[id]; exists {
			cancel()
			return nil, fmt.Errorf("duplicate driver id %q", id)
		}
		driverCfg.Capabilities = normalizeCapabilities(driverCfg.Capabilities)
		driverCfg.Retry = normalizeRetryPolicy(driverCfg.Retry)
		driver := &managedDriver{
			cfg:            driverCfg,
			revision:       1,
			changedAt:      now(),
			retryRemaining: driverCfg.Retry.Budget,
			hooks:          driverCfg.Hooks,
			hooksBound:     driverCfg.Hooks.HoldAdmission != nil || driverCfg.Hooks.Closed != nil,
			changed:        make(chan struct{}),
		}
		if driverCfg.Enabled {
			driver.desired = DesiredRunning
			driver.observed = ObservedStopped
			driver.reason = Reason{Code: ReasonNone}
		} else {
			driver.desired = DesiredStopped
			driver.observed = ObservedDisabled
			driver.reason = Reason{Code: ReasonConfigDisabled}
		}
		manager.drivers[id] = driver
	}
	return manager, nil
}

func (manager *Manager) Snapshot(id string) (Snapshot, bool) {
	driver := manager.drivers[id]
	if driver == nil {
		return Snapshot{}, false
	}
	driver.mu.RLock()
	defer driver.mu.RUnlock()
	return driver.snapshotLocked(), true
}

// BindHooks binds the lifecycle hooks of one driver once. It lets a
// lifecycle bridge attach to a manager composed elsewhere; a second binding,
// or a binding over hooks given in DriverConfig, is refused.
func (manager *Manager) BindHooks(id string, hooks LifecycleHooks) error {
	driver := manager.drivers[id]
	if driver == nil {
		return ErrDriverNotFound
	}
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if driver.hooksBound {
		return ErrHooksBound
	}
	driver.hooks = hooks
	driver.hooksBound = true
	return nil
}

// Watch returns the current snapshot of one driver together with a channel
// that is closed at the driver's next observed transition. It never blocks
// on a lifecycle operation.
func (manager *Manager) Watch(id string) (Snapshot, <-chan struct{}, bool) {
	driver := manager.drivers[id]
	if driver == nil {
		return Snapshot{}, nil, false
	}
	driver.mu.RLock()
	defer driver.mu.RUnlock()
	return driver.snapshotLocked(), driver.changed, true
}

func (driver *managedDriver) currentHooks() LifecycleHooks {
	driver.mu.RLock()
	defer driver.mu.RUnlock()
	return driver.hooks
}

func (driver *managedDriver) snapshotLocked() Snapshot {
	snapshot := Snapshot{
		DriverID:              driver.cfg.ID,
		DesiredState:          driver.desired,
		ObservedState:         driver.observed,
		Reason:                driver.reason,
		Generation:            driver.generation,
		Revision:              driver.revision,
		Attempt:               driver.attempt,
		Capabilities:          append([]Capability(nil), driver.cfg.Capabilities...),
		EffectiveCapabilities: append([]Capability(nil), driver.effective...),
		SafetyQuarantined:     driver.quarantined,
		ActiveOperation:       driver.activeOperation,
		ChangedAtUTC:          driver.changedAt,
	}
	if driver.retry != nil {
		retry := *driver.retry
		snapshot.Retry = &retry
	}
	return snapshot
}

func (manager *Manager) Start(ctx context.Context, id string) error {
	return manager.start(ctx, id, false)
}

// StartAsync reserves STARTING synchronously, then runs construction as a
// manager-owned task. Shutdown fences new tasks, cancels the attempt, and
// accounts for it in the manager wait group.
func (manager *Manager) StartAsync(ctx context.Context, id string) error {
	return manager.start(ctx, id, true)
}

func (manager *Manager) start(ctx context.Context, id string, async bool) error {
	driver := manager.drivers[id]
	if driver == nil {
		return ErrDriverNotFound
	}
	// Cancel a pending backoff before reserving explicit start intent. An
	// already STARTING driver remains idempotent and keeps its one construction.
	manager.cancelRetry(driver)
	driver.opMu.Lock()
	manager.cancelRetry(driver)

	driver.mu.Lock()
	if manager.closed.Load() {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return ErrManagerClosed
	}
	if driver.quarantined {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return ErrSafetyQuarantined
	}
	if driver.observed == ObservedRunning {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return nil
	}
	// Repeating desired RUNNING intent is also idempotent while the current
	// generation is DEGRADED. Degradation deliberately withdraws a capability
	// subset; only an explicit recovery/replacement operation may restore it.
	if driver.desired == DesiredRunning && driver.observed == ObservedDegraded {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return nil
	}
	if driver.desired == DesiredRunning && driver.observed == ObservedStarting {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return nil
	}
	// A canceled construction that ignored its context still owns the runtime
	// retirement fence until startAttempt observes and retires its completion.
	// Do not let a new Start race the stale cleanup's coarse Runtime.Stop.
	if driver.activeAttempt != nil {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return ErrUnavailable
	}
	if driver.observed == ObservedStopping {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return ErrUnavailable
	}
	driver.desired = DesiredRunning
	driver.resetRetryLocked()
	operation := driver.nextOperationLocked()
	replace := driver.needsReplace
	manager.transitionLocked(driver, ObservedStarting, Reason{Code: ReasonStartRequested}, nil, nil)
	driver.attempt++
	driver.mu.Unlock()
	driver.opMu.Unlock()

	if async {
		if !manager.beginTask() {
			return ErrManagerClosed
		}
		go func() {
			defer manager.wg.Done()
			manager.startAttempt(manager.ctx, driver, operation, replace, false)
		}()
		return nil
	}
	manager.startAttempt(ctx, driver, operation, replace, false)
	return nil
}

func (manager *Manager) Replace(ctx context.Context, id string) error {
	driver := manager.drivers[id]
	if driver == nil {
		return ErrDriverNotFound
	}
	manager.cancelRetry(driver)
	driver.opMu.Lock()
	manager.cancelRetry(driver)

	driver.mu.Lock()
	if manager.closed.Load() {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return ErrManagerClosed
	}
	if driver.quarantined {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return ErrSafetyQuarantined
	}
	if driver.observed == ObservedStarting || driver.observed == ObservedStopping {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return ErrUnavailable
	}
	if driver.activeAttempt != nil {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return ErrUnavailable
	}
	driver.desired = DesiredRunning
	driver.resetRetryLocked()
	operation := driver.nextOperationLocked()
	manager.fenceWithdrawalLocked(driver)
	manager.transitionLocked(driver, ObservedStopping, Reason{Code: ReasonStopRequested}, nil, nil)
	driver.attempt++
	driver.mu.Unlock()
	driver.opMu.Unlock()
	manager.startAttempt(ctx, driver, operation, true, false)
	return nil
}

func (manager *Manager) Stop(ctx context.Context, id string) error {
	_, err := manager.StopWithReport(ctx, id)
	return err
}

// StopWithReport is Stop that also returns the classified native close. The
// manager state transition is identical to Stop; the report is the evidence
// a lifecycle bridge uses instead of trusting a nil Stop result. The Closed
// hook receives the same report after every manager lock is released.
func (manager *Manager) StopWithReport(ctx context.Context, id string) (CloseReport, error) {
	driver := manager.drivers[id]
	if driver == nil {
		return CloseReport{DriverID: id, Class: CloseInternalError}, ErrDriverNotFound
	}
	manager.cancelRetry(driver)
	manager.cancelAttempt(driver)
	driver.opMu.Lock()
	report := manager.stopLocked(ctx, driver)
	driver.opMu.Unlock()
	if closed := driver.currentHooks().Closed; closed != nil {
		closed(report)
	}
	return report, nil
}

func (manager *Manager) stopLocked(ctx context.Context, driver *managedDriver) CloseReport {
	manager.cancelRetry(driver)
	driver.mu.Lock()
	attempt := driver.activeAttempt
	report := CloseReport{DriverID: driver.cfg.ID, Generation: driver.generation}
	finish := func(class CloseClass) CloseReport {
		report.Class = class
		report.Observed = driver.observed
		report.Reason = driver.reason
		return report
	}
	if driver.quarantined {
		if driver.desired != DesiredStopped {
			driver.desired = DesiredStopped
			manager.transitionLocked(driver, ObservedFailed, Reason{Code: ReasonCloseUnconfirmed}, nil, nil)
		}
		report = finish(CloseUnconfirmed)
		driver.mu.Unlock()
		cancelAttemptControl(attempt)
		return report
	}
	driver.desired = DesiredStopped
	if driver.observed == ObservedStopped || driver.observed == ObservedDisabled {
		// Nothing is admitted. A construction that is still in flight may yet
		// admit late; until it retires (or quarantines) nothing is proven.
		class := CloseNotRequired
		if driver.runtimeQuarantined() {
			class = CloseUnconfirmed
		} else if attempt != nil {
			class = CloseInternalError
		}
		report = finish(class)
		driver.mu.Unlock()
		cancelAttemptControl(attempt)
		return report
	}
	report.Operation = driver.nextOperationLocked()
	manager.fenceWithdrawalLocked(driver)
	manager.transitionLocked(driver, ObservedStopping, Reason{Code: ReasonStopRequested}, nil, nil)
	driver.mu.Unlock()
	// activeAttempt is captured under the same lock that publishes STOPPED
	// intent. Either construction installed its cancel handle first and is
	// canceled here, or it observes desired STOPPED and never calls Runtime.
	cancelAttemptControl(attempt)

	var err error
	if driver.cfg.Runtime != nil {
		err = driver.cfg.Runtime.Stop(ctx)
	}
	driver.mu.Lock()
	defer driver.mu.Unlock()
	driver.activeOperation = 0
	// The observed transition keeps its characterized precedence (a nil
	// error is STOPPED). The report class is computed independently and lets
	// runtime quarantine dominate, so a nil Stop result alone is never taken
	// as proof by a caller of StopWithReport.
	class := ClassifyClose(err, driver.runtimeQuarantined())
	switch {
	case err == nil:
		driver.needsReplace = false
		manager.transitionLocked(driver, ObservedStopped, Reason{Code: ReasonOperatorStopped}, nil, nil)
	case errors.Is(err, ErrStopTimeout):
		driver.needsReplace = false
		manager.transitionLocked(driver, ObservedStopped, Reason{Code: ReasonStopTimeout}, nil, nil)
	case errors.Is(err, ErrSafetyQuarantined) || driver.runtimeQuarantined():
		driver.quarantined = true
		manager.transitionLocked(driver, ObservedFailed, Reason{Code: ReasonCloseUnconfirmed}, nil, nil)
	default:
		manager.transitionLocked(driver, ObservedFailed, Reason{Code: ReasonInternalError}, nil, nil)
	}
	return finish(class)
}

func cancelAttemptControl(attempt *attemptControl) {
	if attempt != nil {
		attempt.cancel()
	}
}

func (manager *Manager) startAttempt(ctx context.Context, driver *managedDriver, operation uint64, replace, retry bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	control := &attemptControl{cancel: cancel}
	driver.mu.Lock()
	if driver.activeOperation != operation || driver.desired != DesiredRunning || driver.quarantined {
		driver.mu.Unlock()
		cancel()
		return
	}
	driver.activeAttempt = control
	driver.mu.Unlock()
	defer func() {
		cancel()
		driver.mu.Lock()
		if driver.activeAttempt == control {
			driver.activeAttempt = nil
		}
		driver.mu.Unlock()
	}()

	if driver.cfg.Runtime == nil {
		manager.finishAttempt(driver, control, operation, 0, errors.New("provider unavailable"), replace)
		return
	}
	var generation uint64
	var err error
	if replace {
		generation, err = driver.cfg.Runtime.Replace(attemptCtx)
	} else {
		generation, err = driver.cfg.Runtime.Start(attemptCtx)
	}
	driver.mu.RLock()
	superseded := manager.closed.Load() || driver.activeOperation != operation || driver.desired != DesiredRunning
	currentGeneration := driver.generation
	driver.mu.RUnlock()
	if superseded && (err == nil || generation > currentGeneration) {
		// A runtime that ignored cancellation may have admitted after STOPPED
		// intent was published. Retire that stale completion immediately; it is
		// never published as an effective manager generation.
		manager.retireSupersededAdmission(driver, generation, operation)
		return
	}
	if err == nil && !superseded {
		if hold := driver.currentHooks().HoldAdmission; hold != nil {
			// The constructed generation stays unadmitted: observed is STARTING
			// and no effective capability is published, so Invoke refuses it.
			// The hook runs with no manager lock held.
			holdErr := hold(attemptCtx, AdmissionHold{
				DriverID:    driver.cfg.ID,
				Correlation: Correlation{Generation: generation, Operation: operation},
				Previous:    currentGeneration,
				Replacement: replace,
				Retry:       retry,
			})
			driver.mu.RLock()
			superseded = manager.closed.Load() || driver.activeOperation != operation || driver.desired != DesiredRunning
			driver.mu.RUnlock()
			if superseded {
				manager.retireSupersededAdmission(driver, generation, operation)
				return
			}
			if holdErr != nil {
				manager.retireHeldAdmission(driver, control, operation, generation, holdErr)
				return
			}
		}
	}
	manager.finishAttempt(driver, control, operation, generation, err, replace)
}

// retireHeldAdmission retires a constructed generation whose admission hold
// failed. The generation was never published. Its close runs on the
// independent late-retirement deadline; an unproven close quarantines the
// driver. A proven close fails the attempt with the hold error through the
// ordinary classification, so any retry stays manager-owned and constructs
// afresh (nothing remains open to replace).
func (manager *Manager) retireHeldAdmission(driver *managedDriver, control *attemptControl, operation, generation uint64, holdErr error) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), manager.lateRetireTimeout)
	err := driver.cfg.Runtime.Stop(cleanupCtx)
	cancel()
	class := ClassifyClose(err, driver.runtimeQuarantined())
	report := CloseReport{DriverID: driver.cfg.ID, Class: class, Generation: generation, Operation: operation}
	if !class.Proven() {
		driver.mu.Lock()
		if driver.activeAttempt == control {
			driver.activeAttempt = nil
		}
		driver.quarantined = true
		driver.activeOperation = 0
		manager.transitionLocked(driver, ObservedFailed, Reason{Code: ReasonCloseUnconfirmed}, nil, nil)
		report.Observed, report.Reason = driver.observed, driver.reason
		driver.mu.Unlock()
	} else {
		manager.finishAttempt(driver, control, operation, 0, holdErr, false)
		driver.mu.RLock()
		report.Observed, report.Reason = driver.observed, driver.reason
		driver.mu.RUnlock()
	}
	if closed := driver.currentHooks().Closed; closed != nil {
		closed(report)
	}
}

func (manager *Manager) fenceWithdrawalLocked(driver *managedDriver) {
	if driver == nil || driver.cfg.Runtime == nil {
		return
	}
	if fencer, ok := driver.cfg.Runtime.(withdrawalFencer); ok {
		fencer.FenceWithdrawal()
	}
}

// retireSupersededAdmission handles the only path where a runtime can finish
// admission after its manager operation was canceled. The cleanup deadline is
// independent of the canceled caller and every unproven close is promoted to
// process-epoch quarantine; a stale provider is never silently abandoned.
func (manager *Manager) retireSupersededAdmission(driver *managedDriver, generation, operation uint64) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), manager.lateRetireTimeout)
	err := driver.cfg.Runtime.Stop(cleanupCtx)
	cancel()
	quarantined := driver.runtimeQuarantined()
	report := CloseReport{DriverID: driver.cfg.ID, Class: CloseProven, Generation: generation, Operation: operation}
	if err != nil || quarantined {
		// Every unproven close of a stale provider, a drain timeout included,
		// is promoted to quarantine here: nothing else observes it later.
		report.Class = CloseUnconfirmed
		driver.mu.Lock()
		driver.quarantined = true
		driver.activeOperation = 0
		manager.transitionLocked(driver, ObservedFailed, Reason{Code: ReasonCloseUnconfirmed}, nil, nil)
		driver.mu.Unlock()
	}
	driver.mu.RLock()
	report.Observed, report.Reason = driver.observed, driver.reason
	driver.mu.RUnlock()
	if closed := driver.currentHooks().Closed; closed != nil {
		closed(report)
	}
}

func (manager *Manager) finishAttempt(driver *managedDriver, control *attemptControl, operation, generation uint64, err error, replace bool) {
	driver.mu.Lock()
	var bindCorrelation Correlation
	defer func() {
		driver.mu.Unlock()
		if bindCorrelation.Generation == 0 {
			return
		}
		if binder, ok := driver.cfg.Runtime.(correlationBinder); ok {
			binder.BindCorrelation(bindCorrelation)
		}
	}()
	// Retire the completed construction before publishing any terminal or
	// retry state. A repeated Start that observes BACKOFF must therefore see no
	// stale in-flight attempt: it may atomically replace the scheduled retry,
	// rather than canceling the only recovery path and then returning because
	// this already-completed attempt still appeared active.
	if driver.activeAttempt == control {
		driver.activeAttempt = nil
	}
	if manager.closed.Load() || driver.activeOperation != operation || driver.desired != DesiredRunning {
		return
	}
	if err == nil {
		driver.generation = generation
		driver.attempt = 1
		driver.quarantined = false
		driver.needsReplace = false
		driver.resetRetryLocked()
		driver.activeOperation = 0
		bindCorrelation = Correlation{Generation: generation}
		if activator, ok := driver.cfg.Runtime.(correlationActivator); ok {
			activator.ActivateCorrelation(bindCorrelation)
		}
		manager.transitionLocked(driver, ObservedRunning, Reason{Code: ReasonNone}, nil, driver.cfg.Capabilities)
		return
	}
	if errors.Is(err, ErrSafetyQuarantined) || driver.runtimeQuarantined() {
		driver.quarantined = true
		driver.activeOperation = 0
		manager.transitionLocked(driver, ObservedFailed, Reason{Code: ReasonCloseUnconfirmed}, nil, nil)
		return
	}
	failure := manager.classify(driver, err)
	if errors.Is(err, ErrUnavailable) {
		failure = Failure{Reason: Reason{Code: ReasonProviderUnavailable}}
	}
	if replace {
		driver.needsReplace = true
	}
	if failure.Reason.Retryable && driver.retryContinuesLocked() {
		manager.scheduleRetryLocked(driver, operation, replace)
		return
	}
	if failure.Reason.Retryable {
		failure.Reason = Reason{Code: ReasonRetryExhausted}
	}
	driver.activeOperation = 0
	manager.transitionLocked(driver, ObservedFailed, failure.Reason, nil, nil)
}

// resetRetryLocked opens a new failure series: the full fast budget and the
// persistent schedule from its first delay.
func (driver *managedDriver) resetRetryLocked() {
	driver.retryRemaining = driver.cfg.Retry.Budget
	driver.retrySeries = 0
}

// retryContinuesLocked reports whether a retryable failure schedules another
// retry: while fast attempts remain, and afterwards only under a persistent
// policy.
func (driver *managedDriver) retryContinuesLocked() bool {
	return driver.retryRemaining > 0 || driver.cfg.Retry.Persistent.Enabled
}

// nextRetryDelay picks the delay of the next retry of the series: the fast
// schedule while budget remains, then the persistent schedule, which
// restarts at its own initial delay. It consumes one fast attempt.
func (manager *Manager) nextRetryDelay(driver *managedDriver) time.Duration {
	policy := driver.cfg.Retry
	if driver.retryRemaining > 0 {
		index := policy.Budget - driver.retryRemaining
		driver.retryRemaining--
		return retryDelay(policy, index, manager.jitter)
	}
	index := driver.retrySeries - policy.Budget
	if index < 0 {
		index = 0
	}
	persistent := RetryPolicy{InitialDelay: policy.Persistent.InitialDelay, MaxDelay: policy.Persistent.MaxDelay, JitterRatio: policy.JitterRatio}
	return retryDelay(persistent, index, manager.jitter)
}

func (manager *Manager) scheduleRetryLocked(driver *managedDriver, operation uint64, replace bool) {
	remaining := driver.retryRemaining
	delay := manager.nextRetryDelay(driver)
	driver.retrySeries++
	retry := &RetrySnapshot{Eligible: true, BudgetRemaining: remaining, NotBeforeUTC: manager.now().Add(delay), Attempt: driver.retrySeries,
		Persistent: driver.cfg.Retry.Persistent.Enabled}
	manager.transitionLocked(driver, ObservedBackoff, Reason{Code: ReasonRetryScheduled, Retryable: true}, retry, nil)
	driver.retryToken++
	token := driver.retryToken
	driver.retryNeedsReplace = replace
	retryCtx, cancel := context.WithCancel(manager.ctx)
	driver.retryCancel = cancel
	if !manager.beginTask() {
		cancel()
		driver.retryCancel = nil
		return
	}
	go manager.runRetry(retryCtx, driver, operation, token, delay)
}

func (manager *Manager) runRetry(ctx context.Context, driver *managedDriver, operation, token uint64, delay time.Duration) {
	defer manager.wg.Done()
	if !manager.wait(ctx, delay) {
		return
	}

	driver.opMu.Lock()
	driver.mu.Lock()
	if manager.closed.Load() || driver.retryToken != token || driver.activeOperation != operation || driver.desired != DesiredRunning || driver.observed != ObservedBackoff || driver.quarantined {
		driver.mu.Unlock()
		driver.opMu.Unlock()
		return
	}
	driver.retryCancel = nil
	replace := driver.retryNeedsReplace
	manager.transitionLocked(driver, ObservedStarting, Reason{Code: ReasonStartRequested}, nil, nil)
	driver.attempt++
	driver.mu.Unlock()
	driver.opMu.Unlock()
	manager.startAttempt(ctx, driver, operation, replace, true)
}

func (manager *Manager) beginTask() bool {
	manager.taskMu.Lock()
	defer manager.taskMu.Unlock()
	if manager.closed.Load() {
		return false
	}
	manager.wg.Add(1)
	return true
}

func (manager *Manager) cancelRetry(driver *managedDriver) {
	driver.mu.Lock()
	driver.retryToken++
	cancel := driver.retryCancel
	driver.retryCancel = nil
	driver.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (manager *Manager) cancelAttempt(driver *managedDriver) {
	driver.mu.Lock()
	attempt := driver.activeAttempt
	driver.mu.Unlock()
	if attempt != nil {
		attempt.cancel()
	}
}

func (driver *managedDriver) nextOperationLocked() uint64 {
	driver.operationSeq++
	driver.activeOperation = driver.operationSeq
	return driver.activeOperation
}

func (manager *Manager) transitionLocked(driver *managedDriver, observed ObservedState, reason Reason, retry *RetrySnapshot, effective []Capability) {
	driver.observed = observed
	driver.reason = normalizeReason(reason)
	driver.retry = retry
	driver.effective = normalizeCapabilities(effective)
	driver.revision++
	driver.changedAt = manager.now()
	close(driver.changed)
	driver.changed = make(chan struct{})
}

func (manager *Manager) classify(driver *managedDriver, err error) Failure {
	failure := Failure{Reason: Reason{Code: ReasonInternalError}}
	if driver.cfg.ClassifyError != nil {
		failure = driver.cfg.ClassifyError(err)
	}
	failure.Reason = normalizeReason(failure.Reason)
	return failure
}

func (driver *managedDriver) runtimeQuarantined() bool {
	return driver.cfg.Runtime != nil && driver.cfg.Runtime.SafetyQuarantined()
}

// ReportDegraded is the stable-runtime convenience path. Lifecycle callbacks
// with a non-zero operation use ReportDegradedCorrelated instead.
func (manager *Manager) ReportDegraded(id string, generation uint64, effective []Capability, reason Reason) bool {
	return manager.ReportDegradedCorrelated(id, Correlation{Generation: generation}, effective, reason)
}

func (manager *Manager) ReportDegradedCorrelated(id string, correlation Correlation, effective []Capability, reason Reason) bool {
	driver := manager.drivers[id]
	if driver == nil || manager.closed.Load() || reason.Code != ReasonCapabilityDegraded {
		return false
	}
	effective = normalizeCapabilities(effective)
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if manager.closed.Load() || !driver.acceptsCallbackLocked(correlation) || (driver.observed != ObservedRunning && driver.observed != ObservedDegraded) || len(effective) == 0 || len(effective) >= len(driver.cfg.Capabilities) || !capabilitySubset(effective, driver.cfg.Capabilities) {
		return false
	}
	manager.transitionLocked(driver, ObservedDegraded, reason, nil, effective)
	return true
}

// ReportFailure atomically withdraws effective capability, then schedules a
// replacement after the current callback releases its DriverRuntime admission.
func (manager *Manager) ReportFailure(id string, correlation Correlation, failure Failure) bool {
	driver := manager.drivers[id]
	if driver == nil || manager.closed.Load() || !validReasonCode(failure.Reason.Code) {
		return false
	}
	failure.Reason = normalizeReason(failure.Reason)
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if manager.closed.Load() || !driver.acceptsCallbackLocked(correlation) || (driver.observed != ObservedRunning && driver.observed != ObservedDegraded) {
		return false
	}
	operation := driver.nextOperationLocked()
	// Failure withdrawal has the same generation-local admission boundary as
	// explicit Stop/Replace. The hook is non-blocking by contract and runs
	// before BACKOFF/FAILED becomes observable; resource drain remains the
	// later Runtime.Replace/Stop owner's responsibility.
	manager.fenceWithdrawalLocked(driver)
	driver.needsReplace = true
	driver.resetRetryLocked()
	if failure.Reason.Retryable {
		if driver.retryContinuesLocked() {
			manager.scheduleRetryLocked(driver, operation, true)
			return true
		}
		failure.Reason = Reason{Code: ReasonRetryExhausted}
	}
	driver.activeOperation = 0
	manager.transitionLocked(driver, ObservedFailed, failure.Reason, nil, nil)
	return true
}

func (driver *managedDriver) acceptsCallbackLocked(correlation Correlation) bool {
	return !driver.quarantined && driver.desired == DesiredRunning && driver.generation == correlation.Generation && driver.activeOperation == correlation.Operation
}

// Invoke checks effective capability and admits provider work while holding the
// same state lock used to withdraw capabilities during Stop/Replace/failure.
func (manager *Manager) Invoke(ctx context.Context, id string, capability Capability, fn func(any) error) (Correlation, error) {
	driver := manager.drivers[id]
	if driver == nil || manager.closed.Load() {
		return Correlation{}, ErrUnavailable
	}
	driver.mu.Lock()
	if manager.closed.Load() || !containsCapability(driver.effective, capability) || driver.quarantined {
		correlation := Correlation{Generation: driver.generation, Operation: driver.activeOperation}
		driver.mu.Unlock()
		return correlation, ErrUnavailable
	}
	runtime, ok := driver.cfg.Runtime.(AdmittingRuntime)
	if !ok {
		correlation := Correlation{Generation: driver.generation, Operation: driver.activeOperation}
		driver.mu.Unlock()
		return correlation, ErrUnavailable
	}
	admission, err := runtime.Admit(ctx)
	driver.mu.Unlock()
	if err != nil {
		return Correlation{}, err
	}
	defer func() { _ = admission.Release() }()
	return admission.Correlation, admission.Invoke(fn)
}

func (manager *Manager) Shutdown(ctx context.Context) error {
	manager.taskMu.Lock()
	manager.closed.Store(true)
	manager.once.Do(manager.cancel)
	manager.taskMu.Unlock()
	ids := make([]string, 0, len(manager.drivers))
	for id := range manager.drivers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		driver := manager.drivers[id]
		manager.cancelRetry(driver)
		manager.cancelAttempt(driver)
		driver.opMu.Lock()
		report := manager.stopLocked(ctx, driver)
		driver.opMu.Unlock()
		if closed := driver.currentHooks().Closed; closed != nil {
			closed(report)
		}
	}
	done := make(chan struct{})
	go func() {
		manager.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func normalizeRetryPolicy(policy RetryPolicy) RetryPolicy {
	if policy.Budget < 0 {
		policy.Budget = 0
	}
	if policy.InitialDelay <= 0 {
		policy.InitialDelay = time.Second
	}
	if policy.MaxDelay < policy.InitialDelay {
		policy.MaxDelay = policy.InitialDelay
	}
	if policy.JitterRatio == 0 {
		policy.JitterRatio = 0.2
	} else if policy.JitterRatio < 0 {
		policy.JitterRatio = 0
	} else if policy.JitterRatio > 0.5 {
		policy.JitterRatio = 0.5
	}
	if policy.Persistent.Enabled {
		if policy.Persistent.InitialDelay <= 0 {
			policy.Persistent.InitialDelay = time.Second
		}
		if policy.Persistent.MaxDelay < policy.Persistent.InitialDelay {
			policy.Persistent.MaxDelay = policy.Persistent.InitialDelay
		}
	}
	return policy
}

func retryDelay(policy RetryPolicy, index int, jitter func(time.Duration) time.Duration) time.Duration {
	delay := policy.InitialDelay
	for n := 0; n < index && delay < policy.MaxDelay; n++ {
		if delay > policy.MaxDelay/2 {
			delay = policy.MaxDelay
			break
		}
		delay *= 2
	}
	if delay > policy.MaxDelay {
		delay = policy.MaxDelay
	}
	limit := time.Duration(float64(delay) * policy.JitterRatio)
	if limit <= 0 || jitter == nil {
		return delay
	}
	adjustment := jitter(limit)
	if adjustment > limit {
		adjustment = limit
	} else if adjustment < -limit {
		adjustment = -limit
	}
	delay += adjustment
	if delay < policy.InitialDelay {
		return policy.InitialDelay
	}
	if delay > policy.MaxDelay {
		return policy.MaxDelay
	}
	return delay
}

func defaultRetryWait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func defaultRetryJitter(limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	return time.Duration((rand.Float64()*2 - 1) * float64(limit))
}

func normalizeCapabilities(capabilities []Capability) []Capability {
	seen := make(map[Capability]struct{}, len(capabilities))
	for _, capability := range capabilities {
		if validCapability(capability) {
			seen[capability] = struct{}{}
		}
	}
	result := make([]Capability, 0, len(seen))
	for capability := range seen {
		result = append(result, capability)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func validCapability(capability Capability) bool {
	switch capability {
	case CapabilityDiscovery, CapabilityRead, CapabilityWrite, CapabilityPairing, CapabilityTopology, CapabilityRawEvidence, CapabilitySemanticProjection:
		return true
	default:
		return false
	}
}

func validReasonCode(code ReasonCode) bool {
	switch code {
	case ReasonNone, ReasonConfigDisabled, ReasonOperatorStopped, ReasonStartRequested, ReasonStopRequested, ReasonConfigInvalid, ReasonProviderUnavailable, ReasonDependencyUnavailable, ReasonRuntimeNotReady, ReasonCapabilityDegraded, ReasonRetryScheduled, ReasonRetryExhausted, ReasonStopTimeout, ReasonCloseUnconfirmed, ReasonInternalError:
		return true
	default:
		return false
	}
}

func normalizeReason(reason Reason) Reason {
	if !validReasonCode(reason.Code) {
		return Reason{Code: ReasonInternalError}
	}
	switch reason.Code {
	case ReasonDependencyUnavailable, ReasonRuntimeNotReady, ReasonRetryScheduled:
		return reason
	default:
		reason.Retryable = false
		return reason
	}
}

func containsCapability(capabilities []Capability, want Capability) bool {
	index := sort.Search(len(capabilities), func(index int) bool { return capabilities[index] >= want })
	return index < len(capabilities) && capabilities[index] == want
}

func capabilitySubset(candidate, allowed []Capability) bool {
	for _, capability := range candidate {
		if !containsCapability(allowed, capability) {
			return false
		}
	}
	return true
}

func containsSensitiveDriverText(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(lower, "://") || strings.Contains(lower, "secret") || strings.Contains(lower, "password") || strings.Contains(lower, "@")
}
