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

type modbusManagedAttempt struct {
	admissionMu      sync.Mutex
	nativeGeneration uint64
	startupClosed    bool
	scopeWithdrawn   atomic.Bool
	scope            *modbusadapter.PublicationScope
	cancel           context.CancelFunc
	started          chan struct{}
	stopped          chan struct{}
	stopOnce         sync.Once
	adapter          *modbusadapter.Adapter
	work             modbusManagedWork
	startErr         error
	stopErr          error
	workErr          error
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
			if a.adapter != nil {
				driver.previous = a.scope
			}
			driver.active.Store(nil)
			a = nil
		default:
			select {
			case <-a.started:
				if a.startErr != nil && (a.adapter == nil || a.startupClosed) && !a.scopeWithdrawn.Load() && !errors.Is(a.startErr, modbusadapter.ErrStartupCloseUnproven) {
					a.cancel()
					if a.adapter != nil {
						driver.previous = a.scope
					}
					driver.active.Store(nil)
					a = nil
				}
			default:
			}
		}
	}
	if a == nil {
		if driver.config.Enabled && driver.correlation.Epoch == "" {
			driver.mu.Unlock()
			return 0, errors.New("modbus managed lifecycle not bound")
		}
		generation := driver.generation.Add(1)
		runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		a = &modbusManagedAttempt{nativeGeneration: generation, scope: &modbusadapter.PublicationScope{Epoch: semreg.SourceEpochID(driver.correlation.Epoch), Generation: uint64(driver.correlation.Generation), Previous: driver.previous}, cancel: cancel, started: make(chan struct{}), stopped: make(chan struct{})}
		driver.active.Store(a)
		driver.revision.Add(1)
		forwardCancel := context.AfterFunc(ctx, cancel)
		go func() { defer close(a.started); defer forwardCancel(); driver.open(runCtx, a) }()
	}
	driver.mu.Unlock()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-a.started:
		if a.startErr != nil {
			return 0, a.startErr
		}
		if !driver.current(a) {
			return 0, errors.New("modbus managed start withdrawn")
		}
		return a.nativeGeneration, nil
	}
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
	return a.scopeWithdrawn.Load()
}

func (driver *modbusManagedDriver) open(ctx context.Context, a *modbusManagedAttempt) {
	a.adapter, a.startErr = startModbusRuntimeWithPublication(ctx, driver.config, driver.hub, driver.dial, driver.factory, a.scope)
	if a.startErr != nil {
		return
	}
	if ctx.Err() != nil || a.scopeWithdrawn.Load() {
		closeErr := a.adapter.Close()
		a.startupClosed = closeErr == nil
		a.startErr = errors.Join(errors.New("modbus managed start withdrawn"), closeErr)
		return
	}
	if a.adapter != nil && driver.workFactory != nil {
		a.work = driver.workFactory(ctx, a.adapter)
		// Serialize effect installation with withdrawal after inert preparation.
		a.admissionMu.Lock()
		if ctx.Err() != nil || a.withdrawn() {
			a.admissionMu.Unlock()
			var workErr error
			if a.work != nil {
				workErr = a.work.Close()
				a.work = nil
			}
			closeErr := a.adapter.Close()
			a.startupClosed = workErr == nil && closeErr == nil
			a.workErr = workErr
			a.startErr = errors.Join(errors.New("modbus managed start withdrawn"), workErr, closeErr)
			return
		}
		if a.work != nil {
			a.work.Start()
		}
		a.admissionMu.Unlock()
	}
}

// withdraw serializes revocation with effect installation. Factories may finish
// late, but their inert work cannot Start after this gate closes.
func (a *modbusManagedAttempt) withdraw() {
	a.admissionMu.Lock()
	defer a.admissionMu.Unlock()
	a.scopeWithdrawn.Store(true)
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
	a.stopOnce.Do(func() {
		go func() {
			defer close(a.stopped)
			<-a.started
			semanticDone := make(chan error, 1)
			go func() { semanticDone <- a.adapter.WithdrawSemantic() }()
			workErr := a.workErr
			if a.work != nil {
				workErr = a.work.Close()
			}
			closeErr := a.adapter.Close()
			if errors.Is(a.startErr, modbusadapter.ErrStartupCloseUnproven) {
				closeErr = errors.Join(closeErr, a.startErr)
			}
			a.workErr = workErr
			a.stopErr = errors.Join(closeErr, workErr, <-semanticDone)
			driver.revision.Add(1)
		}()
	})
	driver.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.stopped:
		stopErr := a.stopErr
		if stopErr != nil {
			// Close caches native shutdown but retries semantic withdrawal.
			// Keep the joined work/startup failures immutable and fail closed.
			retried := make(chan error, 1)
			go func() {
				err := errors.Join(a.workErr, a.adapter.Close())
				if errors.Is(a.startErr, modbusadapter.ErrStartupCloseUnproven) {
					err = errors.Join(err, a.startErr)
				}
				retried <- err
			}()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case stopErr = <-retried:
			}
		}
		driver.mu.Lock()
		defer driver.mu.Unlock()
		if stopErr == nil && driver.active.Load() == a {
			if a.adapter != nil {
				driver.previous = a.scope
			}
			driver.active.Store(nil)
		}
		return stopErr
	}
}

func (driver *modbusManagedDriver) Replace(ctx context.Context) (uint64, error) {
	if err := driver.Stop(ctx); err != nil {
		return 0, err
	}
	return driver.Start(ctx)
}
func (driver *modbusManagedDriver) Generation() uint64 { return driver.generation.Load() }
func (driver *modbusManagedDriver) Revision() uint64   { return driver.revision.Load() }
func (*modbusManagedDriver) SafetyQuarantined() bool   { return false }
