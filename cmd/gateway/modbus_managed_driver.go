package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type modbusManagedWork interface {
	Start()
	Close() error
}

// Factories prepare inert work; Start installs generation-owned effects.
type modbusManagedWorkFactory func(context.Context, *modbusadapter.Adapter) modbusManagedWork

const (
	modbusWorkPending uint32 = iota
	modbusWorkInstalling
	modbusWorkWithdrawn
)

var (
	errModbusStartWithdrawn = errors.New("modbus managed start withdrawn")
	// A stop deadline that expires while the publication fence is pending
	// reports this: the generation is not fenced and remains DRAINING.
	errModbusPublicationFenceTimeout = errors.New("modbus managed publication fence timeout")
)

// modbusManagedAttempt owns one native generation. Startup has a single
// commit point; failed startup and Stop share one retirement shape with two
// independent cleanup obligations:
//   - semantic: publication admission is revoked first, then the hub fence
//     runs as soon as native open has returned, never waiting for a blocked
//     installation, worker join or endpoint close;
//   - native: installed work is joined before the endpoint closes once, and
//     each failure is latched before the next step can block. A latched
//     native failure is never cleared and quarantines the attempt.
type modbusManagedAttempt struct {
	nativeGeneration uint64
	scope            *modbusadapter.PublicationScope
	cancel           context.CancelFunc

	admission atomic.Uint32 // installation claim, revoked by withdrawal
	// Worker close failure is latched here. Provisional and endpoint close
	// failures are latched on scope.
	nativeCleanupFailed atomic.Bool

	// startupMu orders caller cancellation against the startup commit.
	startupMu        sync.Mutex
	startupFinalized bool

	opened   chan struct{} // native open returned; adapter and openErr are final
	started  chan struct{} // startup decided and, if it failed, retired
	stopped  chan struct{}
	stopOnce sync.Once

	adapter *modbusadapter.Adapter
	openErr error
	// work is owned by open until started closes, then by the stop routine.
	work          modbusManagedWork
	workErr       error
	startErr      error
	startupClosed bool
	stopErr       error
}

// modbusManagedDriver is inert until the shared manager starts it. M06 binds
// the service correlation before startup and owns registration and consumers.
type modbusManagedDriver struct {
	mu           sync.Mutex
	config       ebusgateway.ModbusTCPConfig
	hub          *semhub.Hub
	dial         modbusadapter.Dialer
	factory      modbusadapter.Factory
	workFactory  modbusManagedWorkFactory
	construction runtimecontrol.Construction
	correlation  runtimecontrol.LifecycleCorrelation
	previous     *modbusadapter.PublicationScope
	generation   atomic.Uint64
	revision     atomic.Uint64
	active       atomic.Pointer[modbusManagedAttempt]
}

var _ drivermanager.Runtime = (*modbusManagedDriver)(nil)

// newModbusManagedDriverAdapter exposes compiled construction without adding
// registration or starting a native endpoint. Work remains injectable for M06.
func newModbusManagedDriverAdapter(config ebusgateway.ModbusTCPConfig, hub *semhub.Hub, dial modbusadapter.Dialer, factory modbusadapter.Factory, work modbusManagedWorkFactory) runtimecontrol.LifecycleAdapter {
	return runtimecontrol.NewAdapter(func(ctx context.Context, construction runtimecontrol.Construction) (*modbusManagedDriver, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := mapModbusRuntimeConfig(config); err != nil {
			return nil, err
		}
		if construction.DriverID == "" || len(construction.Sources) != 1 || construction.Sources[0] == "" {
			return nil, errors.New("modbus managed construction identity incomplete")
		}
		construction.Sources = append([]runtimecontrol.SourceID(nil), construction.Sources...)
		return &modbusManagedDriver{config: config, hub: hub, dial: dial, factory: factory, workFactory: work, construction: construction}, nil
	})
}

// BindLifecycle binds service-owned identity, never transport authority. The
// configured source fills the service's currently omitted SourceID field.
func (driver *modbusManagedDriver) BindLifecycle(c runtimecontrol.LifecycleCorrelation) error {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if c.SourceID == "" {
		c.SourceID = driver.construction.Sources[0]
	}
	if c.DriverID != driver.construction.DriverID || c.SourceID != driver.construction.Sources[0] || semhub.ValidateOpaque(string(c.Epoch)) != nil || c.Generation == 0 {
		return errors.New("modbus managed lifecycle identity mismatch")
	}
	if a := driver.active.Load(); a != nil {
		if c == driver.correlation {
			return nil
		}
		return errors.New("modbus managed generation still owns resources")
	}
	driver.correlation = c
	return nil
}

func (driver *modbusManagedDriver) Start(ctx context.Context) (uint64, error) {
retry:
	driver.mu.Lock()
	if err := ctx.Err(); err != nil {
		driver.mu.Unlock()
		return 0, err
	}
	a := driver.active.Load()
	if a != nil {
		select {
		case <-a.stopped:
			if a.stopErr != nil {
				driver.mu.Unlock()
				return 0, a.stopErr
			}
			driver.releaseLocked(a)
			a = nil
		default:
			select {
			case <-a.started:
				if a.retryableStartup() {
					a.cancel()
					driver.releaseLocked(a)
					a = nil
				}
			default:
			}
		}
	}
	joined := a != nil
	if a == nil {
		if driver.config.Enabled && driver.correlation.Epoch == "" {
			driver.mu.Unlock()
			return 0, errors.New("modbus managed lifecycle not bound")
		}
		generation := driver.generation.Add(1)
		runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		a = &modbusManagedAttempt{nativeGeneration: generation, scope: &modbusadapter.PublicationScope{Epoch: semreg.SourceEpochID(driver.correlation.Epoch), Generation: uint64(driver.correlation.Generation), Previous: driver.previous}, cancel: cancel, opened: make(chan struct{}), started: make(chan struct{}), stopped: make(chan struct{})}
		driver.active.Store(a)
		driver.revision.Add(1)
		forwardCancel := context.AfterFunc(ctx, a.cancelStartup)
		go func() { defer close(a.started); defer forwardCancel(); driver.open(runCtx, a) }()
	}
	driver.mu.Unlock()
	select {
	case <-ctx.Done():
		if !joined {
			a.cancelStartup()
		}
		return 0, ctx.Err()
	case <-a.started:
		if a.startErr != nil {
			// Joining a canceled predecessor does not consume this invocation
			// once its cleanup is proven. Only retry an inherited attempt.
			if joined && a.retryableStartup() {
				goto retry
			}
			return 0, a.startErr
		}
		if !driver.current(a) {
			return 0, errModbusStartWithdrawn
		}
		return a.nativeGeneration, nil
	}
}

// releaseLocked drops a retired attempt. Its scope precedes the next
// generation only when an adapter consumed it.
func (driver *modbusManagedDriver) releaseLocked(a *modbusManagedAttempt) {
	if a.adapter != nil {
		driver.previous = a.scope
	}
	driver.active.Store(nil)
}

// cancelStartup and the startup commit share a lock, so caller cancellation
// either fails the attempt at its commit or leaves the admitted worker alive.
func (a *modbusManagedAttempt) cancelStartup() {
	a.startupMu.Lock()
	defer a.startupMu.Unlock()
	if !a.startupFinalized {
		a.cancel()
	}
}

// retryableStartup is read only after started closes. Explicit withdrawal and
// unproven cleanup keep successor admission closed.
func (a *modbusManagedAttempt) retryableStartup() bool {
	return a.startErr != nil && (a.adapter == nil || a.startupClosed) && !a.withdrawn() &&
		!errors.Is(a.startErr, modbusadapter.ErrStartupCloseUnproven) && !a.nativeCloseUnconfirmed()
}

func (driver *modbusManagedDriver) current(a *modbusManagedAttempt) bool {
	return driver.active.Load() == a && !a.withdrawn()
}

func (a *modbusManagedAttempt) withdrawn() bool {
	select {
	case <-a.stopped:
		return true
	default:
	}
	return a.admission.Load() == modbusWorkWithdrawn
}

func (a *modbusManagedAttempt) admissible(ctx context.Context) bool {
	return ctx.Err() == nil && !a.withdrawn()
}

func (a *modbusManagedAttempt) nativeCloseUnconfirmed() bool {
	return a.nativeCleanupFailed.Load() || a.scope.NativeCloseUnconfirmed()
}

// open runs startup to its single commit point. A failed native open has
// already settled its provisional cleanup, with any close failure latched on
// the scope; every later failure retires the attempt before started closes.
func (driver *modbusManagedDriver) open(ctx context.Context, a *modbusManagedAttempt) {
	a.adapter, a.openErr = startModbusRuntimeWithPublication(ctx, driver.config, driver.hub, driver.dial, driver.factory, a.scope)
	a.startErr = a.openErr
	close(a.opened)
	if a.openErr != nil {
		return
	}
	if a.adapter != nil && driver.workFactory != nil && a.admissible(ctx) {
		a.work = driver.workFactory(ctx, a.adapter)
		// Claim installation after inert preparation. Withdrawal revokes the
		// claim without waiting for external Start code; retirement joins it.
		if a.work != nil && a.admissible(ctx) && a.admission.CompareAndSwap(modbusWorkPending, modbusWorkInstalling) {
			a.work.Start()
		}
	}
	if err := a.commitStartup(ctx); err != nil {
		nativeErr, semanticErr := a.retire()
		a.startupClosed = nativeErr == nil && semanticErr == nil
		a.startErr = errors.Join(err, nativeErr, semanticErr)
	}
}

// commitStartup is the startup decision. Withdrawal or caller cancellation
// observed here fails the attempt; cancellation arriving later is ignored.
func (a *modbusManagedAttempt) commitStartup(ctx context.Context) error {
	a.startupMu.Lock()
	defer a.startupMu.Unlock()
	if a.withdrawn() {
		return errModbusStartWithdrawn
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.startupFinalized = true
	return nil
}

// retire settles a failed startup. Publication admission is revoked before
// the fence starts, and the fence runs concurrently with native cleanup.
func (a *modbusManagedAttempt) retire() (nativeErr, semanticErr error) {
	a.scope.Withdraw()
	semantic := a.fenceAsync()
	nativeErr = a.retireNative()
	return nativeErr, <-semantic
}

// retireNative joins installed work before the endpoint closes exactly once.
// Each failure is latched before the next step can block. Only the owner of
// work calls it with work installed; later calls reread cached results. A
// failed provisional close of a startup without adapter is immutable.
func (a *modbusManagedAttempt) retireNative() error {
	if a.work != nil {
		a.workErr = a.work.Close()
		a.work = nil
		if a.workErr != nil {
			a.nativeCleanupFailed.Store(true)
		}
	}
	var provisional error
	if a.adapter == nil && errors.Is(a.openErr, modbusadapter.ErrStartupNativeCloseUnproven) {
		provisional = a.openErr
	}
	return errors.Join(a.workErr, a.adapter.CloseNative(), provisional)
}

// fenceSemantic runs one semantic withdrawal pass: the adapter's publication
// fence, or the retained fence owner of a failed epoch admission. Passes
// serialize in the publication core, and the scope records their progress.
func (a *modbusManagedAttempt) fenceSemantic() error {
	if a.adapter != nil {
		return a.adapter.WithdrawSemantic()
	}
	if err, owned := a.scope.RetryStartupCleanup(); owned {
		return err
	}
	return nil
}

func (a *modbusManagedAttempt) fenceAsync() <-chan error {
	result := make(chan error, 1)
	go func() { result <- a.fenceSemantic() }()
	return result
}

// withdraw revokes new installation without joining an admitted Start. Factories
// may finish late, but cannot claim installation after this gate closes.
func (a *modbusManagedAttempt) withdraw() {
	a.admission.Store(modbusWorkWithdrawn)
	a.scope.Withdraw()
	a.cancel()
}

// FenceWithdrawal only revokes admission and signals cancellation. Stop owns
// the join, native close, and semantic retirement proof.
func (driver *modbusManagedDriver) FenceWithdrawal() {
	if a := driver.active.Load(); a != nil {
		a.withdraw()
	}
}

func (driver *modbusManagedDriver) Stop(ctx context.Context) error {
	driver.mu.Lock()
	a := driver.active.Load()
	if a == nil {
		driver.mu.Unlock()
		return nil
	}
	a.withdraw()
	a.stopOnce.Do(func() { go driver.stopAttempt(a) })
	driver.mu.Unlock()
	select {
	case <-ctx.Done():
		return a.deadlineErr(ctx.Err())
	case <-a.stopped:
	}
	stopErr := a.stopErr
	if stopErr != nil {
		// Native results are immutable once observed. Only the semantic fence
		// and a retained startup cleanup owner are retried; fail closed.
		retried := make(chan error, 1)
		go func() { retried <- errors.Join(a.retireNative(), a.fenceSemantic()) }()
		select {
		case <-ctx.Done():
			return a.deadlineErr(ctx.Err())
		case stopErr = <-retried:
		}
	}
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if stopErr == nil && driver.active.Load() == a {
		driver.releaseLocked(a)
	}
	return stopErr
}

// stopAttempt is the attempt's single stop routine. The publication fence
// starts once native open has returned, before the installation join; native
// cleanup then joins startup and its worker before the endpoint closes.
func (driver *modbusManagedDriver) stopAttempt(a *modbusManagedAttempt) {
	defer close(a.stopped)
	<-a.opened
	semantic := a.fenceAsync()
	<-a.started
	nativeErr := a.retireNative()
	a.stopErr = errors.Join(nativeErr, <-semantic)
	driver.revision.Add(1)
}

// deadlineErr reports a stop deadline truthfully: it proves neither close nor
// fence. A pending publication fence, including the compensating fence of a
// failed epoch admission, is a fence timeout; cleanup keeps ownership and
// successor admission stays closed until it completes.
func (a *modbusManagedAttempt) deadlineErr(err error) error {
	if a.scope.FencePending() {
		return errors.Join(errModbusPublicationFenceTimeout, err)
	}
	return err
}

func (driver *modbusManagedDriver) Replace(ctx context.Context) (uint64, error) {
	if err := driver.Stop(ctx); err != nil {
		return 0, err
	}
	return driver.Start(ctx)
}
func (driver *modbusManagedDriver) Generation() uint64 { return driver.generation.Load() }
func (driver *modbusManagedDriver) Revision() uint64   { return driver.revision.Load() }

// SafetyQuarantined reports a latched native close failure of the current
// attempt, whether or not its semantic cleanup has completed.
func (driver *modbusManagedDriver) SafetyQuarantined() bool {
	a := driver.active.Load()
	return a != nil && a.nativeCloseUnconfirmed()
}
