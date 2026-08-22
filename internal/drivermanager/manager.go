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
	Eligible        bool
	BudgetRemaining int
	NotBeforeUTC    time.Time
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

type DriverConfig struct {
	ID            string
	Enabled       bool
	Runtime       Runtime
	Capabilities  []Capability
	ClassifyError func(error) Failure
	Retry         RetryPolicy
}

type Config struct {
	Drivers           []DriverConfig
	Now               func() time.Time
	RetryJitter       func(time.Duration) time.Duration
	LateRetireTimeout time.Duration
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc
	now    func() time.Time
	jitter func(time.Duration) time.Duration
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

	desired           DesiredState
	observed          ObservedState
	reason            Reason
	retry             *RetrySnapshot
	generation        uint64
	revision          uint64
	attempt           uint64
	effective         []Capability
	quarantined       bool
	changedAt         time.Time
	operationSeq      uint64
	activeOperation   uint64
	retryRemaining    int
	retryToken        uint64
	retryCancel       context.CancelFunc
	retryNeedsReplace bool
	needsReplace      bool
	activeAttempt     *attemptControl
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
	driver.retryRemaining = driver.cfg.Retry.Budget
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
			manager.startAttempt(manager.ctx, driver, operation, replace)
		}()
		return nil
	}
	manager.startAttempt(ctx, driver, operation, replace)
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
	driver.retryRemaining = driver.cfg.Retry.Budget
	operation := driver.nextOperationLocked()
	manager.fenceWithdrawalLocked(driver)
	manager.transitionLocked(driver, ObservedStopping, Reason{Code: ReasonStopRequested}, nil, nil)
	driver.attempt++
	driver.mu.Unlock()
	driver.opMu.Unlock()
	manager.startAttempt(ctx, driver, operation, true)
	return nil
}

func (manager *Manager) Stop(ctx context.Context, id string) error {
	driver := manager.drivers[id]
	if driver == nil {
		return ErrDriverNotFound
	}
	manager.cancelRetry(driver)
	manager.cancelAttempt(driver)
	driver.opMu.Lock()
	defer driver.opMu.Unlock()
	return manager.stopLocked(ctx, driver)
}

func (manager *Manager) stopLocked(ctx context.Context, driver *managedDriver) error {
	manager.cancelRetry(driver)
	driver.mu.Lock()
	attempt := driver.activeAttempt
	if driver.quarantined {
		if driver.desired != DesiredStopped {
			driver.desired = DesiredStopped
			manager.transitionLocked(driver, ObservedFailed, Reason{Code: ReasonCloseUnconfirmed}, nil, nil)
		}
		driver.mu.Unlock()
		cancelAttemptControl(attempt)
		return nil
	}
	driver.desired = DesiredStopped
	if driver.observed == ObservedStopped || driver.observed == ObservedDisabled {
		driver.mu.Unlock()
		cancelAttemptControl(attempt)
		return nil
	}
	driver.nextOperationLocked()
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
	return nil
}

func cancelAttemptControl(attempt *attemptControl) {
	if attempt != nil {
		attempt.cancel()
	}
}

func (manager *Manager) startAttempt(ctx context.Context, driver *managedDriver, operation uint64, replace bool) {
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
		manager.retireSupersededAdmission(driver)
		return
	}
	manager.finishAttempt(driver, control, operation, generation, err, replace)
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
func (manager *Manager) retireSupersededAdmission(driver *managedDriver) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), manager.lateRetireTimeout)
	err := driver.cfg.Runtime.Stop(cleanupCtx)
	cancel()
	if err == nil && !driver.runtimeQuarantined() {
		return
	}

	driver.mu.Lock()
	driver.quarantined = true
	driver.activeOperation = 0
	manager.transitionLocked(driver, ObservedFailed, Reason{Code: ReasonCloseUnconfirmed}, nil, nil)
	driver.mu.Unlock()
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
		driver.retryRemaining = driver.cfg.Retry.Budget
		driver.activeOperation = 0
		manager.transitionLocked(driver, ObservedRunning, Reason{Code: ReasonNone}, nil, driver.cfg.Capabilities)
		bindCorrelation = Correlation{Generation: generation}
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
	if failure.Reason.Retryable && driver.retryRemaining > 0 {
		manager.scheduleRetryLocked(driver, operation, replace)
		return
	}
	if failure.Reason.Retryable {
		failure.Reason = Reason{Code: ReasonRetryExhausted}
	}
	driver.activeOperation = 0
	manager.transitionLocked(driver, ObservedFailed, failure.Reason, nil, nil)
}

func (manager *Manager) scheduleRetryLocked(driver *managedDriver, operation uint64, replace bool) {
	delay := retryDelay(driver.cfg.Retry, driver.cfg.Retry.Budget-driver.retryRemaining, manager.jitter)
	retry := &RetrySnapshot{Eligible: true, BudgetRemaining: driver.retryRemaining, NotBeforeUTC: manager.now().Add(delay)}
	manager.transitionLocked(driver, ObservedBackoff, Reason{Code: ReasonRetryScheduled, Retryable: true}, retry, nil)
	driver.retryRemaining--
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
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
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
	manager.startAttempt(ctx, driver, operation, replace)
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
	driver.needsReplace = true
	driver.retryRemaining = driver.cfg.Retry.Budget
	if failure.Reason.Retryable {
		if driver.retryRemaining > 0 {
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
		_ = manager.stopLocked(ctx, driver)
		driver.opMu.Unlock()
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
