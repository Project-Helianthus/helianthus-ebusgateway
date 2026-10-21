package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

type eebusManagedDriverOptions struct {
	Config          ebusgateway.EEBusConfig
	Resolve         eebusInterfaceAddressResolver
	Factory         eebusRuntimeFactory
	OperatorFactory eebusOperatorRuntimeFactory
}

// newEEBusManagedDriverDescriptor is inert: native construction occurs only
// when the constructed lifecycle runtime is started. Native safeguards remain
// in the existing configuration and startup adapters.
func newEEBusManagedDriverDescriptor(id runtimecontrol.DriverID, sources []runtimecontrol.SourceID, options eebusManagedDriverOptions) runtimecontrol.Descriptor {
	options.Config.Interfaces = append([]string(nil), options.Config.Interfaces...)
	options.Config.Subnets = append([]string(nil), options.Config.Subnets...)
	if options.Config.RemoteSKIAllowlist != nil {
		options.Config.RemoteSKIAllowlist = append([]string{}, options.Config.RemoteSKIAllowlist...)
	}
	disposition := runtimecontrol.DispositionDisabled
	if options.Config.Enabled {
		disposition = runtimecontrol.DispositionEnabled
	}
	return runtimecontrol.Descriptor{
		Format: runtimecontrol.DescriptorFormatV1, DriverID: id,
		Direction: runtimecontrol.DirectionSouthbound, Disposition: disposition,
		NativeContract: runtimecontrol.NativeContractRef{Owner: "github.com/Project-Helianthus/helianthus-eebusreg", Contract: "Runtime", Version: "v0.1.36"},
		Sources:        append([]runtimecontrol.SourceID(nil), sources...),
		Adapter: runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*eebusManagedDriver, error) {
			return newEEBusManagedDriver(options), nil
		}),
	}
}

type eebusManagedGeneration struct {
	adapter   *eebusRuntimeAdapter
	admin     eebusruntime.AdminV1
	number    uint64
	fenced    atomic.Bool
	callbacks sync.RWMutex
	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error
}

func (generation *eebusManagedGeneration) retire() {
	generation.fenced.Store(true)
	generation.closeOnce.Do(func() {
		go func() {
			generation.callbacks.Lock()
			defer generation.callbacks.Unlock()
			generation.closeErr = generation.adapter.Shutdown()
			close(generation.closed)
		}()
	})
}

type eebusManagedDriver struct {
	options     eebusManagedDriverOptions
	operation   chan struct{}
	mu          sync.Mutex
	current     *eebusManagedGeneration
	cancel      context.CancelFunc
	generation  atomic.Uint64
	revision    atomic.Uint64
	quarantined atomic.Bool
}

var _ drivermanager.Runtime = (*eebusManagedDriver)(nil)

func newEEBusManagedDriver(options eebusManagedDriverOptions) *eebusManagedDriver {
	return &eebusManagedDriver{options: options, operation: make(chan struct{}, 1)}
}

func (driver *eebusManagedDriver) lock(ctx context.Context) error {
	select {
	case driver.operation <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-driver.operation
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (driver *eebusManagedDriver) Start(ctx context.Context) (uint64, error) {
	if err := driver.lock(ctx); err != nil {
		return 0, err
	}
	defer func() { <-driver.operation }()
	return driver.start(ctx)
}

func (driver *eebusManagedDriver) start(ctx context.Context) (uint64, error) {
	return driver.startRuntime(ctx, driver.options.OperatorFactory != nil, nil)
}

func (driver *eebusManagedDriver) startRuntime(ctx context.Context, operator bool, degraded error) (uint64, error) {
	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()
	driver.mu.Lock()
	if err := attemptCtx.Err(); err != nil {
		driver.mu.Unlock()
		return 0, err
	}
	if driver.quarantined.Load() {
		driver.mu.Unlock()
		return 0, drivermanager.ErrUnavailable
	}
	if driver.current != nil {
		current := driver.current
		driver.mu.Unlock()
		if !current.fenced.Load() {
			return current.number, nil
		}
		return 0, drivermanager.ErrUnavailable
	}
	// Native workers belong to this generation, beyond the manager attempt.
	startCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopForwarding := context.AfterFunc(attemptCtx, cancel)
	defer stopForwarding()
	driver.cancel = func() { cancelAttempt(); cancel() }
	driver.mu.Unlock()
	var adapter *eebusRuntimeAdapter
	var admin eebusruntime.AdminV1
	var err error
	if operator {
		adapter, admin, err = startEEBusOperatorRuntimeRetained(startCtx, driver.options.Config, driver.options.Resolve, driver.options.OperatorFactory)
	} else {
		adapter, err = startEEBusRuntimeRetained(startCtx, driver.options.Config, driver.options.Resolve, driver.options.Factory)
	}
	stopForwarding()
	driver.mu.Lock()
	if err == nil {
		err = attemptCtx.Err()
	}
	if err == nil {
		err = startCtx.Err()
	}
	if err == nil && adapter == nil {
		err = runtimecontrol.ErrNilRuntime
	}
	if err != nil {
		fallback := operator && attemptCtx.Err() == nil && startCtx.Err() == nil
		cancel()
		driver.mu.Unlock()
		if adapter != nil {
			if closeErr := driver.retireFailedStart(ctx, adapter); closeErr != nil {
				return 0, errors.Join(err, closeErr)
			}
		}
		if fallback && attemptCtx.Err() == nil {
			// Only proven cleanup permits the established read-only fallback.
			number, fallbackErr := driver.startRuntime(attemptCtx, false, err)
			if fallbackErr != nil {
				return 0, errors.Join(err, fallbackErr)
			}
			return number, nil
		}
		return 0, err
	}
	if degraded != nil {
		adapter.startupDegradedReason = eebusStartupFailureReason(degraded)
	}
	number := driver.generation.Add(1)
	driver.current = &eebusManagedGeneration{adapter: adapter, admin: admin, number: number, closed: make(chan struct{})}
	driver.revision.Add(1)
	driver.mu.Unlock()
	return number, nil
}

// Failed native starts remain owned and fenced until closure is proven.
func (driver *eebusManagedDriver) retireFailedStart(ctx context.Context, adapter *eebusRuntimeAdapter) error {
	failed := &eebusManagedGeneration{adapter: adapter, closed: make(chan struct{})}
	failed.fenced.Store(true)
	driver.mu.Lock()
	driver.current = failed
	driver.mu.Unlock()
	failed.retire()
	select {
	case <-failed.closed:
		if failed.closeErr != nil {
			driver.quarantined.Store(true)
			return failed.closeErr
		}
	case <-ctx.Done():
		driver.quarantined.Store(true)
		return ctx.Err()
	}
	driver.mu.Lock()
	driver.current = nil
	driver.mu.Unlock()
	return nil
}

func (driver *eebusManagedDriver) Replace(ctx context.Context) (uint64, error) {
	driver.FenceWithdrawal()
	if err := driver.lock(ctx); err != nil {
		return 0, err
	}
	defer func() { <-driver.operation }()
	if err := driver.stop(ctx); err != nil {
		return 0, err
	}
	return driver.start(ctx)
}

// FenceWithdrawal never calls native code or waits for a callback.
func (driver *eebusManagedDriver) FenceWithdrawal() {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	if driver.cancel != nil {
		driver.cancel()
	}
	if driver.current != nil {
		driver.current.fenced.Store(true)
	}
}

func (driver *eebusManagedDriver) Stop(ctx context.Context) error {
	driver.FenceWithdrawal()
	if err := driver.lock(ctx); err != nil {
		return err
	}
	defer func() { <-driver.operation }()
	return driver.stop(ctx)
}

func (driver *eebusManagedDriver) stop(ctx context.Context) error {
	driver.mu.Lock()
	current := driver.current
	driver.mu.Unlock()
	if current == nil {
		return nil
	}
	current.retire()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-current.closed:
	}
	if current.closeErr != nil {
		driver.quarantined.Store(true)
		return current.closeErr
	}
	if driver.quarantined.Load() {
		return drivermanager.ErrSafetyQuarantined
	}
	driver.mu.Lock()
	driver.current = nil
	driver.cancel = nil
	driver.mu.Unlock()
	driver.revision.Add(1)
	return nil
}

// WithGeneration guards the complete publication callback, including its final
// effect. Stop fences new callbacks and drains those already admitted before
// reporting successful closure. Callbacks must not reenter lifecycle methods.
func (driver *eebusManagedDriver) WithGeneration(number uint64, publish func(*eebusRuntimeAdapter, eebusruntime.AdminV1) error) error {
	driver.mu.Lock()
	current := driver.current
	driver.mu.Unlock()
	if current == nil || current.number != number || current.fenced.Load() || publish == nil {
		return drivermanager.ErrUnavailable
	}
	current.callbacks.RLock()
	defer current.callbacks.RUnlock()
	if current.fenced.Load() {
		return drivermanager.ErrUnavailable
	}
	return publish(current.adapter, current.admin)
}

func (driver *eebusManagedDriver) Generation() uint64      { return driver.generation.Load() }
func (driver *eebusManagedDriver) Revision() uint64        { return driver.revision.Load() }
func (driver *eebusManagedDriver) SafetyQuarantined() bool { return driver.quarantined.Load() }
