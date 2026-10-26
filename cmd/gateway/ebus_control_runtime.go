package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// This file is the S4 manager-to-hub bridge for the lifecycle-only
// ebus.primary driver. Process startup (gateway_run_setup.go) binds it to
// the eBUS DriverManager before anything starts the manager and registers
// it as the control service's executor; it carries no authorization.
//
// Ownership split:
//   - DriverManager alone owns native attempts, retry/backoff, drain and
//     close. The bridge calls it once per accepted lifecycle operation and
//     never retries natively itself.
//   - The control service owns decisions, durable work slots and the
//     lifecycle correlation (DriverID, SourceID, epoch, generation, sequence).
//   - The bridge maps each correlation onto the hub-native source epoch of
//     the one configured source and onto the process-monotonic native
//     generation, holds admission while that hub work runs outside manager
//     locks, and reports the classified close instead of a nil Stop result.
//
// The source is asset-free: no asset, binding, capability instance or
// activation proof is declared, and the epoch barrier is legitimately empty.

const (
	defaultEBusControlCleanupTimeout = 5 * time.Second

	ebusControlEvidenceOwner = "helianthus.gateway.runtime"
	ebusControlFenceKind     = "lifecycle.ebus.generation_fence"
	ebusControlRetireKind    = "lifecycle.ebus.epoch_retirement"
	ebusControlFenceReason   = "lifecycle.driver_generation_replaced"
	ebusControlRetireReason  = "lifecycle.source_stopped"
)

var (
	errEBusControlNotPrepared   = errors.New("ebus control: source not prepared")
	errEBusControlStale         = errors.New("ebus control: stale lifecycle correlation")
	errEBusControlForeign       = errors.New("ebus control: correlation names another driver or source")
	errEBusControlUnconfirmed   = errors.New("ebus control: predecessor close unconfirmed")
	errEBusControlPredecessor   = errors.New("ebus control: predecessor epoch still owns the source")
	errEBusControlHubNotLive    = errors.New("ebus control: hub source generation not live")
	errEBusControlForeignAdmit  = errors.New("ebus control: manager admitted a generation outside the bridge")
	errEBusControlStartFailed   = errors.New("ebus control: native start failed")
	errEBusControlInvalidConfig = errors.New("ebus control: invalid configuration")
)

// ebusLifecycleHub is the hub-native subset the bridge uses. *semhub.Hub
// implements it; there is no Declare, Begin or publication method.
type ebusLifecycleHub interface {
	AdmitSource(semhub.Admission) (*semhub.Handle, error)
	AdmitSuccessor(semhub.Succession) (*semhub.Handle, error)
	Fence(semhub.FenceRequest) (semhub.ControlResult, error)
	Activate(*semhub.Handle, uint64) (*semhub.Handle, error)
	RetireEpoch(semhub.RetireRequest) (semhub.ControlResult, error)
	Source(semreg.SourceID) (semhub.SourceInfo, bool)
}

var _ ebusLifecycleHub = (*semhub.Hub)(nil)

// ebusControlConfig is the composition input of the bridge. Source and
// ClockEpoch are caller supplied: the bridge never invents a source identity
// or derives one from an endpoint.
type ebusControlConfig struct {
	Manager        *drivermanager.Manager
	Hub            ebusLifecycleHub
	Source         runtimecontrol.SourceID
	ClockEpoch     semreg.ClockEpochID
	Now            func() time.Time
	CleanupTimeout time.Duration
}

// ebusControlKey is the full lifecycle correlation of one ebus.primary
// generation: driver, source, source epoch, driver generation and the
// service operation (lifecycle sequence) that opened the epoch.
type ebusControlKey struct {
	DriverID   runtimecontrol.DriverID
	SourceID   runtimecontrol.SourceID
	Epoch      runtimecontrol.SourceEpoch
	Generation runtimecontrol.DriverGeneration
	Sequence   runtimecontrol.LifecycleSequence
}

// ebusGenerationMapping records the explicit mapping of one driver
// generation onto the native generation admitted for it. Driver generation
// restarts at 1 in every epoch; native generation is process monotonic. A
// mapping is process-local and historical after process loss. Automatic
// reports that a manager-owned retry or failure replacement constructed the
// native generation; Operation is the DriverManager operation.
type ebusGenerationMapping struct {
	Key       ebusControlKey
	Native    runtimecontrol.NativeGeneration
	Operation uint64
	Automatic bool
}

// ebusControlView is the cached lifecycle view of the bridge. Semantic
// capabilities stay empty for the lifecycle-only source; native capabilities
// are the currently admitted native invocation gates only.
type ebusControlView struct {
	Key                  ebusControlKey
	Native               runtimecontrol.NativeGeneration
	Admitted             bool
	Observed             drivermanager.ObservedState
	Reason               drivermanager.ReasonCode
	NativeCapabilities   []drivermanager.Capability
	SemanticCapabilities []string
	Quarantined          bool
	CloseClass           drivermanager.CloseClass
}

// ebusControlError is a closed, non-retryable executor failure. The service
// maps ReasonCode to its categorical reason; the bridge never asks the
// service to retry, so DriverManager stays the only native retry owner.
type ebusControlError struct {
	reason drivermanager.ReasonCode
	err    error
}

func (e *ebusControlError) Error() string                        { return e.err.Error() }
func (e *ebusControlError) Unwrap() error                        { return e.err }
func (e *ebusControlError) ReasonCode() drivermanager.ReasonCode { return e.reason }

func ebusControlFailure(reason drivermanager.ReasonCode, err error) error {
	return &ebusControlError{reason: reason, err: err}
}

type ebusControlEpoch struct {
	key        ebusControlKey
	handle     *semhub.Handle
	generation runtimecontrol.DriverGeneration
	native     runtimecontrol.NativeGeneration
	mappings   []ebusGenerationMapping

	fenced    bool
	fenceCh   chan struct{}
	launched  bool
	startDone chan struct{}
	startErr  error

	retired    bool
	retiring   chan struct{}
	closeClass drivermanager.CloseClass
	closed     bool
}

type ebusNativeStop struct {
	epoch *ebusControlEpoch
	done  chan struct{}
}

// ebusControlRuntime implements runtimecontrol.LifecycleExecutor for
// ebus.primary over DriverManager and the hub.
type ebusControlRuntime struct {
	id             runtimecontrol.DriverID
	source         runtimecontrol.SourceID
	manager        *drivermanager.Manager
	hub            ebusLifecycleHub
	clockEpoch     semreg.ClockEpochID
	now            func() time.Time
	monoBase       time.Time
	cleanupTimeout time.Duration

	// opMu serializes epoch binding and succession (hub calls included). It
	// is never held while waiting for a native start and Stop never takes
	// it, so fencing stays immediate.
	opMu sync.Mutex

	// dispatchMu serializes the current-epoch check with the native start
	// dispatch, and Stop waits on it after fencing, so a fenced operation can
	// never dispatch into a successor's retry or reset its native budget.
	dispatchMu sync.Mutex
	// beforeDispatch is a test seam run before a start dispatch.
	beforeDispatch func()
	// admitGate, when set before the manager starts, runs in the admission
	// hold of a manager-driven same-epoch replacement, outside every bridge
	// and manager lock and before any hub work. An error keeps the
	// replacement unadmitted (the generation-task drain of the predecessor
	// has not joined); the manager retries it as a dependency failure.
	admitGate func(context.Context) error

	mu          sync.Mutex
	prepared    bool
	initial     *semhub.Handle
	current     *ebusControlEpoch
	history     map[runtimecontrol.SourceEpoch]*ebusControlEpoch
	tombstones  map[runtimecontrol.SourceEpoch]bool
	quarantined bool
	nativeStop  *ebusNativeStop
	lastClose   drivermanager.CloseClass

	wg sync.WaitGroup
}

var _ runtimecontrol.LifecycleExecutor = (*ebusControlRuntime)(nil)

// newEBusControlRuntime binds the bridge to the ebus.primary driver of an
// existing DriverManager. It performs no hub call, endpoint construction or
// native start; Prepare admits the inert source later.
func newEBusControlRuntime(cfg ebusControlConfig) (*ebusControlRuntime, error) {
	if cfg.Manager == nil || cfg.Hub == nil {
		return nil, fmt.Errorf("%w: manager and hub are required", errEBusControlInvalidConfig)
	}
	if cfg.Source == "" || semhub.ValidateOpaque(string(cfg.Source)) != nil {
		return nil, fmt.Errorf("%w: a configured source identity is required", errEBusControlInvalidConfig)
	}
	if semhub.ValidateOpaque(string(cfg.ClockEpoch)) != nil {
		return nil, fmt.Errorf("%w: a clock epoch is required", errEBusControlInvalidConfig)
	}
	if _, ok := cfg.Manager.Snapshot(primaryEBusDriverID); !ok {
		return nil, fmt.Errorf("%w: manager has no %s driver", errEBusControlInvalidConfig, primaryEBusDriverID)
	}
	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	cleanup := cfg.CleanupTimeout
	if cleanup <= 0 {
		cleanup = defaultEBusControlCleanupTimeout
	}
	bridge := &ebusControlRuntime{
		id:             primaryEBusDriverID,
		source:         cfg.Source,
		manager:        cfg.Manager,
		hub:            cfg.Hub,
		clockEpoch:     cfg.ClockEpoch,
		now:            now,
		monoBase:       time.Now(),
		cleanupTimeout: cleanup,
		history:        map[runtimecontrol.SourceEpoch]*ebusControlEpoch{},
		tombstones:     map[runtimecontrol.SourceEpoch]bool{},
	}
	if err := cfg.Manager.BindHooks(primaryEBusDriverID, drivermanager.LifecycleHooks{
		HoldAdmission: bridge.holdAdmission,
		Closed:        bridge.observeClose,
	}); err != nil {
		return nil, err
	}
	return bridge, nil
}

// SetAdmitGate installs the replacement admission gate. It must be called
// before anything starts the manager.
func (bridge *ebusControlRuntime) SetAdmitGate(gate func(context.Context) error) {
	bridge.admitGate = gate
}

// Prepare admits the configured source's inert first epoch (generation 1)
// in the hub before any endpoint construction, also when desired state is
// STOPPED. It declares no asset: the epoch barrier is empty and complete.
// It is idempotent.
func (bridge *ebusControlRuntime) Prepare(context.Context) error {
	bridge.opMu.Lock()
	defer bridge.opMu.Unlock()
	bridge.mu.Lock()
	prepared := bridge.prepared
	bridge.mu.Unlock()
	if prepared {
		return nil
	}
	handle, err := bridge.hub.AdmitSource(semhub.Admission{Source: semreg.SourceID(bridge.source), Driver: string(bridge.id)})
	if err != nil {
		return fmt.Errorf("ebus control: admit inert source: %w", err)
	}
	bridge.mu.Lock()
	bridge.prepared, bridge.initial = true, handle
	bridge.mu.Unlock()
	return nil
}

func (bridge *ebusControlRuntime) key(corr runtimecontrol.LifecycleCorrelation) (ebusControlKey, error) {
	if corr.DriverID != bridge.id || (corr.SourceID != "" && corr.SourceID != bridge.source) || corr.Epoch == "" {
		return ebusControlKey{}, errEBusControlForeign
	}
	// The service correlation carries no SourceID yet; the bridge binds it
	// to the one configured source so every recorded key is complete.
	return ebusControlKey{DriverID: corr.DriverID, SourceID: bridge.source, Epoch: corr.Epoch, Generation: corr.Generation, Sequence: corr.Sequence}, nil
}

// successionOperation is the opaque hub correlation of the accepted
// lifecycle operation that owns a successor epoch.
func successionOperation(key ebusControlKey) string {
	sum := sha256.Sum256([]byte(string(key.DriverID) + "\x00" + string(key.SourceID) + "\x00" + string(key.Epoch) + "\x00" + strconv.FormatUint(uint64(key.Sequence), 10)))
	return "lifecycle-op:" + hex.EncodeToString(sum[:16])
}

// Start opens the generation of corr. It returns nil only when DriverManager
// published RUNNING for a native generation the bridge mapped to this exact
// epoch after the hub obligations held. A failed Start leaves nothing open.
func (bridge *ebusControlRuntime) Start(ctx context.Context, corr runtimecontrol.LifecycleCorrelation) error {
	if ctx == nil {
		ctx = context.Background()
	}
	key, err := bridge.key(corr)
	if err != nil {
		return ebusControlFailure(drivermanager.ReasonConfigInvalid, err)
	}
	if key.Generation == 0 {
		return ebusControlFailure(drivermanager.ReasonConfigInvalid, errEBusControlStale)
	}
	epoch, join, err := bridge.bind(key)
	if err != nil {
		return err
	}
	if join {
		return bridge.awaitStart(ctx, epoch)
	}
	return bridge.runStart(ctx, epoch)
}

// bind resolves the epoch of key: the inert first epoch, the epoch an
// earlier call of the same operation already bound (join), or exactly one
// hub-native successor after the predecessor's retirement and proven close.
func (bridge *ebusControlRuntime) bind(key ebusControlKey) (*ebusControlEpoch, bool, error) {
	bridge.opMu.Lock()
	defer bridge.opMu.Unlock()
	bridge.mu.Lock()
	if !bridge.prepared {
		bridge.mu.Unlock()
		return nil, false, ebusControlFailure(drivermanager.ReasonRuntimeNotReady, errEBusControlNotPrepared)
	}
	if bridge.tombstones[key.Epoch] || bridge.history[key.Epoch] != nil {
		bridge.mu.Unlock()
		return nil, false, ebusControlFailure(drivermanager.ReasonInternalError, errEBusControlStale)
	}
	pred := bridge.current
	if pred != nil && pred.key.Epoch == key.Epoch {
		defer bridge.mu.Unlock()
		if pred.key != key {
			return nil, false, ebusControlFailure(drivermanager.ReasonInternalError, errEBusControlStale)
		}
		// The same operation already bound and launched this epoch: join it,
		// or return its recorded result. Nothing is started again, so no
		// native budget is renewed; a stopped epoch stays stale.
		return pred, true, nil
	}
	if bridge.quarantined {
		bridge.mu.Unlock()
		return nil, false, ebusControlFailure(drivermanager.ReasonInternalError, errEBusControlUnconfirmed)
	}
	if key.Generation != 1 {
		bridge.mu.Unlock()
		return nil, false, ebusControlFailure(drivermanager.ReasonInternalError, errEBusControlStale)
	}
	if pred == nil {
		// First START of this incarnation: claim the inert generation-1
		// reservation admitted by Prepare; no second epoch is allocated.
		epoch := newEBusControlEpoch(key, bridge.initial)
		epoch.launched = true
		bridge.initial = nil
		bridge.current = epoch
		bridge.mu.Unlock()
		return epoch, false, nil
	}
	switch {
	case !pred.fenced:
		bridge.mu.Unlock()
		return nil, false, ebusControlFailure(drivermanager.ReasonInternalError, errEBusControlPredecessor)
	case !pred.closed:
		bridge.mu.Unlock()
		return nil, false, ebusControlFailure(drivermanager.ReasonInternalError, errEBusControlUnconfirmed)
	}
	bridge.mu.Unlock()
	// The predecessor's native close is proven; settle its retirement before
	// succession (a failed start may have closed natively without a STOP).
	if err := bridge.awaitRetire(context.Background(), pred); err != nil {
		return nil, false, ebusControlFailure(drivermanager.ReasonDependencyUnavailable, err)
	}
	handle, err := bridge.hub.AdmitSuccessor(semhub.Succession{Previous: pred.handle, Operation: successionOperation(key)})
	if err != nil {
		return nil, false, ebusControlFailure(drivermanager.ReasonDependencyUnavailable, fmt.Errorf("ebus control: admit successor: %w", err))
	}
	epoch := newEBusControlEpoch(key, handle)
	epoch.launched = true
	bridge.mu.Lock()
	bridge.history[pred.key.Epoch] = pred
	bridge.current = epoch
	if bridge.tombstones[key.Epoch] {
		// A STOP for this correlation arrived while the successor was being
		// journalled: the successor is never started, opened nothing natively
		// and is retired like any stopped epoch.
		bridge.fenceLocked(epoch)
		epoch.closed, epoch.closeClass = true, drivermanager.CloseNotRequired
		bridge.mu.Unlock()
		_ = bridge.awaitRetire(context.Background(), epoch)
		return nil, false, ebusControlFailure(drivermanager.ReasonInternalError, errEBusControlStale)
	}
	bridge.mu.Unlock()
	return epoch, false, nil
}

func newEBusControlEpoch(key ebusControlKey, handle *semhub.Handle) *ebusControlEpoch {
	return &ebusControlEpoch{key: key, handle: handle, generation: key.Generation, fenceCh: make(chan struct{}), startDone: make(chan struct{})}
}

func (bridge *ebusControlRuntime) fenceLocked(epoch *ebusControlEpoch) {
	if !epoch.fenced {
		epoch.fenced = true
		close(epoch.fenceCh)
	}
}

// runStart asks DriverManager for exactly one native start of this
// operation and waits for its terminal result. Retries, backoff and drain
// stay inside DriverManager.
func (bridge *ebusControlRuntime) runStart(ctx context.Context, epoch *ebusControlEpoch) error {
	if err := bridge.startManager(ctx, epoch); err != nil {
		reason := drivermanager.ReasonProviderUnavailable
		if errors.Is(err, drivermanager.ErrSafetyQuarantined) {
			reason = drivermanager.ReasonInternalError
		}
		return bridge.failStart(epoch, ebusControlFailure(reason, fmt.Errorf("%w: %w", errEBusControlStartFailed, err)))
	}
	for {
		snapshot, changed, _ := bridge.manager.Watch(string(bridge.id))
		bridge.mu.Lock()
		fenced, current, native := epoch.fenced, bridge.current == epoch, epoch.native
		bridge.mu.Unlock()
		if fenced || !current {
			return bridge.failStart(epoch, ebusControlFailure(drivermanager.ReasonInternalError, errEBusControlStale))
		}
		switch snapshot.ObservedState {
		case drivermanager.ObservedRunning, drivermanager.ObservedDegraded:
			if native == 0 || uint64(native) != snapshot.Generation {
				if native == 0 {
					return bridge.failStart(epoch, ebusControlFailure(drivermanager.ReasonInternalError, errEBusControlForeignAdmit))
				}
				break // a newer mapping is being held; wait for it
			}
			bridge.mu.Lock()
			if epoch.fenced {
				bridge.mu.Unlock()
				continue
			}
			close(epoch.startDone)
			bridge.mu.Unlock()
			return nil
		case drivermanager.ObservedFailed:
			if snapshot.ActiveOperation == 0 {
				reason := snapshot.Reason.Code
				if reason == drivermanager.ReasonRetryExhausted || reason == drivermanager.ReasonCloseUnconfirmed {
					reason = drivermanager.ReasonInternalError
				}
				return bridge.failStart(epoch, ebusControlFailure(reason, fmt.Errorf("%w: %s", errEBusControlStartFailed, snapshot.Reason.Code)))
			}
		case drivermanager.ObservedStopped, drivermanager.ObservedDisabled:
			if snapshot.DesiredState == drivermanager.DesiredStopped {
				return bridge.failStart(epoch, ebusControlFailure(drivermanager.ReasonInternalError, errEBusControlStale))
			}
		}
		select {
		case <-changed:
		case <-epoch.fenceCh:
		case <-ctx.Done():
			return bridge.failStart(epoch, ebusControlFailure(drivermanager.ReasonInternalError, ctx.Err()))
		}
	}
}

// startManager requests the native start. A predecessor's construction may
// still own its attempt (for example a hold blocked in hub work after STOP
// already published STOPPED); DriverManager reports ErrUnavailable until
// that attempt retires. The successor waits for the retirement, bounded by
// the cleanup deadline, instead of failing the accepted operation.
func (bridge *ebusControlRuntime) startManager(ctx context.Context, epoch *ebusControlEpoch) error {
	deadline := time.NewTimer(bridge.cleanupTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		err := bridge.dispatchStart(epoch)
		if !errors.Is(err, drivermanager.ErrUnavailable) {
			return err
		}
		select {
		case <-tick.C:
		case <-epoch.fenceCh:
			return err
		case <-deadline.C:
			return err
		case <-ctx.Done():
			return err
		}
	}
}

// dispatchStart calls StartAsync only while the epoch is still current and
// unfenced, holding dispatchMu so Stop's fence cannot interleave with it.
func (bridge *ebusControlRuntime) dispatchStart(epoch *ebusControlEpoch) error {
	if bridge.beforeDispatch != nil {
		bridge.beforeDispatch()
	}
	bridge.dispatchMu.Lock()
	defer bridge.dispatchMu.Unlock()
	bridge.mu.Lock()
	stale := epoch.fenced || bridge.current != epoch
	bridge.mu.Unlock()
	if stale {
		return errEBusControlStale
	}
	return bridge.manager.StartAsync(context.Background(), string(bridge.id))
}

// awaitDispatchIdle returns once no start dispatch is in flight.
func (bridge *ebusControlRuntime) awaitDispatchIdle() {
	bridge.dispatchMu.Lock()
	defer bridge.dispatchMu.Unlock()
}

// awaitStart joins the start of an operation already in flight, or returns
// its recorded result. It never starts the manager again, so a repeated call
// renews no native budget.
func (bridge *ebusControlRuntime) awaitStart(ctx context.Context, epoch *ebusControlEpoch) error {
	select {
	case <-epoch.startDone:
	case <-ctx.Done():
		return ebusControlFailure(drivermanager.ReasonInternalError, ctx.Err())
	}
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if epoch.startErr != nil {
		return epoch.startErr
	}
	if epoch.fenced {
		return ebusControlFailure(drivermanager.ReasonInternalError, errEBusControlStale)
	}
	return nil
}

// failStart fences the epoch and makes sure nothing stays open natively: it
// joins (or requests) the manager-owned close on the independent cleanup
// deadline and records its class.
func (bridge *ebusControlRuntime) failStart(epoch *ebusControlEpoch, cause error) error {
	bridge.mu.Lock()
	bridge.fenceLocked(epoch)
	select {
	case <-epoch.startDone:
	default:
		epoch.startErr = cause
		close(epoch.startDone)
	}
	bridge.mu.Unlock()
	done := bridge.ensureNativeStop(epoch)
	timer := time.NewTimer(bridge.cleanupTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
	return cause
}

// Stop fences the correlation's epoch immediately, then runs the two
// independent obligations: manager-owned native cancellation/drain/close
// with its classified result, and hub retirement of the epoch. Neither
// waits for the other's success; STOPPED needs both.
func (bridge *ebusControlRuntime) Stop(ctx context.Context, corr runtimecontrol.LifecycleCorrelation) runtimecontrol.StopOutcome {
	if ctx == nil {
		ctx = context.Background()
	}
	key, err := bridge.key(corr)
	if err != nil {
		return runtimecontrol.StopOutcome{}
	}
	bridge.mu.Lock()
	epoch := bridge.current
	if epoch == nil || epoch.key.Epoch != key.Epoch {
		epoch = bridge.history[key.Epoch]
	}
	if epoch == nil {
		// The correlation never bound an epoch: it opened nothing natively
		// and owns no hub epoch. Tombstone it so a late Start is refused.
		bridge.tombstones[key.Epoch] = true
		bridge.mu.Unlock()
		return runtimecontrol.StopOutcome{NativeClosed: true, SemanticCleaned: true}
	}
	bridge.fenceLocked(epoch)
	bridge.mu.Unlock()

	// A dispatch that passed its epoch check finishes before the native stop
	// is requested, so the stop also cancels what that dispatch started.
	bridge.awaitDispatchIdle()

	nativeDone := bridge.ensureNativeStop(epoch)
	retireDone := make(chan struct{})
	go func(done chan struct{}) {
		defer close(done)
		_ = bridge.awaitRetire(ctx, epoch)
	}(retireDone)
	for nativeDone != nil || retireDone != nil {
		select {
		case <-nativeDone:
			nativeDone = nil
		case <-retireDone:
			retireDone = nil
		case <-ctx.Done():
			nativeDone, retireDone = nil, nil
		}
	}
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	return runtimecontrol.StopOutcome{NativeClosed: epoch.closed, SemanticCleaned: epoch.retired}
}

// ensureNativeStop returns a channel closed when the native close of the
// epoch is classified. At most one manager stop runs at a time; a repeated
// call joins it. A proven close is never repeated.
func (bridge *ebusControlRuntime) ensureNativeStop(epoch *ebusControlEpoch) <-chan struct{} {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if epoch.closed {
		done := make(chan struct{})
		close(done)
		return done
	}
	if bridge.nativeStop != nil {
		return bridge.nativeStop.done
	}
	stop := &ebusNativeStop{epoch: epoch, done: make(chan struct{})}
	bridge.nativeStop = stop
	bridge.wg.Add(1)
	go func() {
		defer bridge.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), bridge.cleanupTimeout)
		report, err := bridge.manager.StopWithReport(ctx, string(bridge.id))
		cancel()
		if err != nil {
			report.Class = drivermanager.CloseInternalError
		}
		bridge.mu.Lock()
		stop.epoch.closeClass = report.Class
		bridge.lastClose = report.Class
		if report.Class.Proven() {
			stop.epoch.closed = true
		}
		if report.Class == drivermanager.CloseUnconfirmed {
			bridge.quarantined = true
		}
		bridge.nativeStop = nil
		close(stop.done)
		bridge.mu.Unlock()
	}()
	return stop.done
}

// awaitRetire retires the epoch in the hub once the native withdrawal fence
// is visible (or the native close already finished). It is idempotent and
// joins a retirement in flight. A failed retirement leaves the epoch
// unretired for the next attempt and never blocks native close.
func (bridge *ebusControlRuntime) awaitRetire(ctx context.Context, epoch *ebusControlEpoch) error {
	bridge.mu.Lock()
	if epoch.retired {
		bridge.mu.Unlock()
		return nil
	}
	if epoch.retiring != nil {
		wait := epoch.retiring
		bridge.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
		bridge.mu.Lock()
		defer bridge.mu.Unlock()
		if !epoch.retired {
			return errors.New("ebus control: epoch retirement failed")
		}
		return nil
	}
	retiring := make(chan struct{})
	epoch.retiring = retiring
	handle := epoch.handle
	bridge.mu.Unlock()
	defer func() {
		bridge.mu.Lock()
		epoch.retiring = nil
		close(retiring)
		bridge.mu.Unlock()
	}()
	if err := bridge.awaitWithdrawalFence(ctx, epoch); err != nil {
		return err
	}
	_, err := bridge.hub.RetireEpoch(bridge.retireRequest(handle))
	if err != nil && bridge.alreadyRetired(err, handle) {
		err = nil
	}
	if err != nil {
		return fmt.Errorf("ebus control: retire epoch: %w", err)
	}
	bridge.mu.Lock()
	epoch.retired = true
	bridge.mu.Unlock()
	return nil
}

// awaitWithdrawalFence waits until DriverManager no longer exposes the
// epoch's generation: native admission is withdrawn (STOPPING or later) or
// the native close already finished.
func (bridge *ebusControlRuntime) awaitWithdrawalFence(ctx context.Context, epoch *ebusControlEpoch) error {
	for {
		snapshot, changed, _ := bridge.manager.Watch(string(bridge.id))
		bridge.mu.Lock()
		closed := epoch.closed
		bridge.mu.Unlock()
		switch snapshot.ObservedState {
		case drivermanager.ObservedStopping, drivermanager.ObservedStopped, drivermanager.ObservedDisabled, drivermanager.ObservedFailed:
			return nil
		}
		if closed {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (bridge *ebusControlRuntime) alreadyRetired(err error, handle *semhub.Handle) bool {
	var semErr *semreg.Error
	if !errors.As(err, &semErr) || semErr.ID != semreg.StaleSourceEpoch || handle == nil {
		return false
	}
	// A retirement of the source's current epoch is refused as stale only
	// when that epoch is already retired; the bridge is its sole retirer.
	info, ok := bridge.hub.Source(semreg.SourceID(bridge.source))
	return ok && info.Epoch == handle.Epoch()
}

// holdAdmission is the DriverManager admission hold. It runs with every
// manager lock released while the constructed generation is unadmitted. It
// maps the native generation to the current epoch's driver generation and,
// for a manager-driven replacement, fences the old and activates the next
// driver generation in the hub inside the same epoch.
func (bridge *ebusControlRuntime) holdAdmission(ctx context.Context, hold drivermanager.AdmissionHold) error {
	native := runtimecontrol.NativeGeneration(hold.Correlation.Generation)
	bridge.mu.Lock()
	epoch := bridge.current
	if epoch == nil || epoch.fenced || bridge.quarantined || !epoch.launched || native == 0 {
		bridge.mu.Unlock()
		return fmt.Errorf("%w: %w", drivermanager.ErrUnavailable, errEBusControlStale)
	}
	if epoch.native != 0 && native <= epoch.native {
		bridge.mu.Unlock()
		return fmt.Errorf("%w: %w", drivermanager.ErrUnavailable, errEBusControlStale)
	}
	handle, generation, replacement := epoch.handle, epoch.generation, epoch.native != 0
	bridge.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	if replacement && bridge.admitGate != nil {
		if err := bridge.admitGate(ctx); err != nil {
			// Classified by the driver as a retryable dependency failure, so
			// DriverManager retries the replacement under its retry policy.
			return fmt.Errorf("ebus control: replacement admission gate: %w", err)
		}
	}
	if replacement {
		// Same-epoch manager retry or failure replacement: the epoch is kept
		// and the driver generation advances through the hub's Fence/Activate
		// path. No epoch succession happens here.
		next := generation + 1
		info, ok := bridge.hub.Source(semreg.SourceID(bridge.source))
		if !ok || info.Epoch != handle.Epoch() || info.Generation != uint64(generation) {
			return errEBusControlHubNotLive
		}
		if info.Live {
			if _, err := bridge.hub.Fence(bridge.fenceRequest(handle, generation)); err != nil {
				return fmt.Errorf("ebus control: fence generation: %w", err)
			}
		}
		activated, err := bridge.hub.Activate(handle, uint64(next))
		if err != nil {
			return fmt.Errorf("ebus control: activate generation: %w", err)
		}
		bridge.mu.Lock()
		defer bridge.mu.Unlock()
		if bridge.current == epoch {
			epoch.handle = activated
			epoch.generation = next
		}
		if bridge.current != epoch || epoch.fenced {
			return fmt.Errorf("%w: %w", drivermanager.ErrUnavailable, errEBusControlStale)
		}
		key := epoch.key
		key.Generation = next
		epoch.native = native
		epoch.mappings = append(epoch.mappings, ebusGenerationMapping{Key: key, Native: native, Operation: hold.Correlation.Operation, Automatic: true})
		return nil
	}

	// First admission of the epoch's opening operation: the claimed
	// generation-1 hub admission must be live; nothing is declared.
	info, ok := bridge.hub.Source(semreg.SourceID(bridge.source))
	if !ok || !info.Live || info.Epoch != handle.Epoch() || info.Generation != handle.Generation() || handle.Generation() != uint64(generation) {
		return errEBusControlHubNotLive
	}
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if bridge.current != epoch || epoch.fenced || epoch.native != 0 {
		return fmt.Errorf("%w: %w", drivermanager.ErrUnavailable, errEBusControlStale)
	}
	key := epoch.key
	key.Generation = generation
	epoch.native = native
	epoch.mappings = append(epoch.mappings, ebusGenerationMapping{Key: key, Native: native, Operation: hold.Correlation.Operation, Automatic: hold.Retry})
	return nil
}

// observeClose records the classified close of manager-initiated
// retirements (a superseded late admission or a failed hold). An unproven
// close quarantines the bridge for this process epoch.
func (bridge *ebusControlRuntime) observeClose(report drivermanager.CloseReport) {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	if report.Class == drivermanager.CloseUnconfirmed {
		bridge.quarantined = true
	}
}

func (bridge *ebusControlRuntime) evidence(kind semreg.DefinitionID, parts ...string) []semreg.EvidenceRef {
	material := string(bridge.source)
	for _, part := range parts {
		material += "\x00" + part
	}
	sum := sha256.Sum256([]byte(material))
	return []semreg.EvidenceRef{{Owner: ebusControlEvidenceOwner, Kind: kind, Digest: semreg.Digest("sha256:" + hex.EncodeToString(sum[:])),
		Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}}
}

func (bridge *ebusControlRuntime) instants() (semreg.TimePoint, semreg.MonotonicPoint) {
	wall := bridge.now().UTC()
	mono := time.Since(bridge.monoBase)
	if mono < 0 {
		mono = 0
	}
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(wall.UnixNano(), 10)), ClockID: "wall.utc", UncertaintyNS: "0"},
		semreg.MonotonicPoint{ClockEpochID: bridge.clockEpoch, Nanoseconds: semreg.Uint64(strconv.FormatInt(mono.Nanoseconds(), 10))}
}

func (bridge *ebusControlRuntime) fenceRequest(handle *semhub.Handle, generation runtimecontrol.DriverGeneration) semhub.FenceRequest {
	wall, mono := bridge.instants()
	return semhub.FenceRequest{Source: semreg.SourceID(bridge.source), Epoch: handle.Epoch(), Generation: uint64(generation),
		Reason: ebusControlFenceReason, Evidence: bridge.evidence(ebusControlFenceKind, string(handle.Epoch()), strconv.FormatUint(uint64(generation), 10)),
		ObservedAt: wall, Monotonic: mono}
}

func (bridge *ebusControlRuntime) retireRequest(handle *semhub.Handle) semhub.RetireRequest {
	wall, mono := bridge.instants()
	return semhub.RetireRequest{Source: semreg.SourceID(bridge.source), Epoch: handle.Epoch(), Reason: ebusControlRetireReason,
		Evidence: bridge.evidence(ebusControlRetireKind, string(handle.Epoch())), ObservedAt: wall, Monotonic: mono}
}

// admittedLocked returns the epoch and full key of the currently admitted
// generation, derived from one manager snapshot.
func (bridge *ebusControlRuntime) admittedLocked(snapshot drivermanager.Snapshot) (*ebusControlEpoch, ebusControlKey, bool) {
	epoch := bridge.current
	if epoch == nil || epoch.fenced || epoch.native == 0 || bridge.quarantined {
		return nil, ebusControlKey{}, false
	}
	if snapshot.ObservedState != drivermanager.ObservedRunning && snapshot.ObservedState != drivermanager.ObservedDegraded {
		return nil, ebusControlKey{}, false
	}
	if snapshot.Generation != uint64(epoch.native) {
		return nil, ebusControlKey{}, false
	}
	key := epoch.key
	key.Generation = epoch.generation
	return epoch, key, true
}

// View returns the cached lifecycle view. It performs no hub or endpoint
// call. Admission and observed RUNNING come from the same manager snapshot,
// so they are visible together.
func (bridge *ebusControlRuntime) View() ebusControlView {
	snapshot, _ := bridge.manager.Snapshot(string(bridge.id))
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	view := ebusControlView{Observed: snapshot.ObservedState, Reason: snapshot.Reason.Code,
		Quarantined: bridge.quarantined || snapshot.SafetyQuarantined, CloseClass: bridge.lastClose, SemanticCapabilities: []string{}}
	if epoch := bridge.current; epoch != nil {
		view.Key = epoch.key
		view.Key.Generation = epoch.generation
		view.Native = epoch.native
	}
	if _, _, ok := bridge.admittedLocked(snapshot); ok {
		view.Admitted = true
		view.NativeCapabilities = append([]drivermanager.Capability(nil), snapshot.EffectiveCapabilities...)
	}
	return view
}

// NativeObservation derives, from one manager snapshot, the executor
// observation of the current epoch once its opening START succeeded. It
// reports DriverManager's own failure replacement after RUNNING: BACKOFF with
// the scheduled attempt number and due time, a retry STARTING, the recovered
// RUNNING or DEGRADED generation, and the terminal FAILED of a failure that
// retrying cannot heal (CLOSE_UNCONFIRMED and quarantine for an unproven
// close). The correlation is the one the
// service opened the epoch with; same-epoch replacements keep it. STOPPING,
// STOPPED and DISABLED belong to the service's STOP path and a fenced epoch
// reports nothing, so a service transition is never overwritten.
func (bridge *ebusControlRuntime) NativeObservation(snapshot drivermanager.Snapshot) (runtimecontrol.LifecycleCorrelation, runtimecontrol.NativeObservation, bool) {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	epoch := bridge.current
	if epoch == nil || epoch.fenced || epoch.native == 0 {
		return runtimecontrol.LifecycleCorrelation{}, runtimecontrol.NativeObservation{}, false
	}
	select {
	case <-epoch.startDone:
		if epoch.startErr != nil {
			return runtimecontrol.LifecycleCorrelation{}, runtimecontrol.NativeObservation{}, false
		}
	default:
		// The opening START is still the service's own operation.
		return runtimecontrol.LifecycleCorrelation{}, runtimecontrol.NativeObservation{}, false
	}
	corr := runtimecontrol.LifecycleCorrelation{DriverID: epoch.key.DriverID, SourceID: epoch.key.SourceID,
		Epoch: epoch.key.Epoch, Generation: epoch.key.Generation, Sequence: epoch.key.Sequence}
	obs := runtimecontrol.NativeObservation{State: snapshot.ObservedState, Reason: snapshot.Reason.Code}
	switch {
	case bridge.quarantined || snapshot.SafetyQuarantined:
		obs = runtimecontrol.NativeObservation{State: drivermanager.ObservedFailed, Reason: drivermanager.ReasonCloseUnconfirmed, Quarantined: true}
	case snapshot.ObservedState == drivermanager.ObservedRunning || snapshot.ObservedState == drivermanager.ObservedDegraded:
		if snapshot.Generation != uint64(epoch.native) {
			// A replacement mapping is still being held; wait for it.
			return runtimecontrol.LifecycleCorrelation{}, runtimecontrol.NativeObservation{}, false
		}
	case snapshot.ObservedState == drivermanager.ObservedBackoff:
		// The manager's schedule: the next attempt of the failure series and
		// when it is due.
		if retry := snapshot.Retry; retry != nil && retry.Attempt > 0 && !retry.NotBeforeUTC.IsZero() {
			obs.NextAttempt, obs.NextAttemptUTC = retry.Attempt, retry.NotBeforeUTC.UTC()
		}
	case snapshot.ObservedState == drivermanager.ObservedStarting:
	case snapshot.ObservedState == drivermanager.ObservedFailed:
		if snapshot.ActiveOperation != 0 {
			return runtimecontrol.LifecycleCorrelation{}, runtimecontrol.NativeObservation{}, false
		}
	default:
		return runtimecontrol.LifecycleCorrelation{}, runtimecontrol.NativeObservation{}, false
	}
	return corr, obs, true
}

// Mappings returns the driver-to-native generation mappings recorded in
// this process, in admission order.
func (bridge *ebusControlRuntime) Mappings() []ebusGenerationMapping {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	var out []ebusGenerationMapping
	for _, epoch := range bridge.orderedEpochsLocked() {
		out = append(out, epoch.mappings...)
	}
	return out
}

func (bridge *ebusControlRuntime) orderedEpochsLocked() []*ebusControlEpoch {
	epochs := make([]*ebusControlEpoch, 0, len(bridge.history)+1)
	for _, epoch := range bridge.history {
		epochs = append(epochs, epoch)
	}
	// History is small (one entry per succession); order by first native
	// mapping, epochs without one last.
	for i := 1; i < len(epochs); i++ {
		for j := i; j > 0 && firstNative(epochs[j]) < firstNative(epochs[j-1]); j-- {
			epochs[j], epochs[j-1] = epochs[j-1], epochs[j]
		}
	}
	if bridge.current != nil {
		epochs = append(epochs, bridge.current)
	}
	return epochs
}

func firstNative(epoch *ebusControlEpoch) runtimecontrol.NativeGeneration {
	if len(epoch.mappings) == 0 {
		return ^runtimecontrol.NativeGeneration(0)
	}
	return epoch.mappings[0].Native
}

// Admitted reports whether key is the full correlation of the currently
// admitted generation. Late health, callbacks and publications use it.
func (bridge *ebusControlRuntime) Admitted(key ebusControlKey) bool {
	snapshot, _ := bridge.manager.Snapshot(string(bridge.id))
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	_, current, ok := bridge.admittedLocked(snapshot)
	return ok && current == key
}

// PublicationHandle returns the hub handle of the admitted generation for an
// exact key. The lifecycle-only source publishes nothing; the gate exists so
// that a late or stale correlation can never obtain a live handle.
func (bridge *ebusControlRuntime) PublicationHandle(key ebusControlKey) (*semhub.Handle, error) {
	snapshot, _ := bridge.manager.Snapshot(string(bridge.id))
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	epoch, current, ok := bridge.admittedLocked(snapshot)
	if !ok || current != key {
		return nil, errEBusControlStale
	}
	return epoch.handle, nil
}

// ReportFailure forwards a health failure of an exact key to DriverManager
// under its mapped native correlation. A late or stale key is rejected with
// no manager or hub mutation.
func (bridge *ebusControlRuntime) ReportFailure(key ebusControlKey, failure drivermanager.Failure) bool {
	snapshot, _ := bridge.manager.Snapshot(string(bridge.id))
	bridge.mu.Lock()
	epoch, current, ok := bridge.admittedLocked(snapshot)
	if !ok || current != key {
		bridge.mu.Unlock()
		return false
	}
	native := epoch.native
	bridge.mu.Unlock()
	return bridge.manager.ReportFailure(string(bridge.id), drivermanager.Correlation{Generation: uint64(native)}, failure)
}

// Close waits for bridge-owned native close tasks within ctx.
func (bridge *ebusControlRuntime) Close(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		bridge.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// newEBusPrimaryDescriptor builds the typed registration of the
// lifecycle-only ebus.primary driver. The native contract reference and the
// configured source are caller supplied and required: no placeholder is
// installed and no identity is derived from an endpoint. It declares no hook.
func newEBusPrimaryDescriptor(native runtimecontrol.NativeContractRef, source runtimecontrol.SourceID, adapter runtimecontrol.LifecycleAdapter) (runtimecontrol.Descriptor, error) {
	if native.Owner == "" || native.Contract == "" || native.Version == "" {
		return runtimecontrol.Descriptor{}, fmt.Errorf("%w: native contract owner, contract and version are required", errEBusControlInvalidConfig)
	}
	if source == "" {
		return runtimecontrol.Descriptor{}, fmt.Errorf("%w: a configured source identity is required", errEBusControlInvalidConfig)
	}
	if adapter == nil {
		return runtimecontrol.Descriptor{}, fmt.Errorf("%w: a lifecycle adapter is required", errEBusControlInvalidConfig)
	}
	return runtimecontrol.Descriptor{
		Format:         runtimecontrol.DescriptorFormatV1,
		DriverID:       primaryEBusDriverID,
		Direction:      runtimecontrol.DirectionSouthbound,
		Disposition:    runtimecontrol.DispositionEnabled,
		NativeContract: native,
		Adapter:        adapter,
		Sources:        []runtimecontrol.SourceID{source},
	}, nil
}

// newEBusPrimaryLifecycleAdapter is the typed factory of the ebus.primary
// DriverManager runtime. Registration stores it without invoking it; when a
// later composition step constructs it, it builds the runtime without
// opening any endpoint.
func newEBusPrimaryLifecycleAdapter(cfg ebusgateway.Config) runtimecontrol.LifecycleAdapter {
	return runtimecontrol.NewAdapter(func(_ context.Context, construction runtimecontrol.Construction) (*ebusDriverRuntime, error) {
		if construction.DriverID != primaryEBusDriverID {
			return nil, errEBusControlForeign
		}
		protocol := configuredEBusDriverProtocol(cfg)
		needsPassive := cfg.BroadcastListen && (protocol == ebusgateway.TransportAdapterDirect || shouldStartPassiveObserveFirst(cfg))
		return newEBusDriverRuntime(cfg, protocol, needsPassive, &ebusV8CumulativeCounter{}, nil), nil
	})
}
