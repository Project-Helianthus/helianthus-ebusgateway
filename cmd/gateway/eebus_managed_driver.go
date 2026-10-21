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
		NativeContract: runtimecontrol.NativeContractRef{Owner: "helianthus-eebusreg", Contract: "Runtime", Version: "v0.1.36"},
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
	driver.mu.Lock()
	if driver.current != nil {
		current := driver.current
		driver.mu.Unlock()
		if !current.fenced.Load() {
			return current.number, nil
		}
		return 0, drivermanager.ErrUnavailable
	}
	startCtx, cancel := context.WithCancel(ctx)
	driver.cancel = cancel
	driver.mu.Unlock()
	var adapter *eebusRuntimeAdapter
	var admin eebusruntime.AdminV1
	var err error
	if driver.options.OperatorFactory != nil {
		adapter, admin, err = startEEBusOperatorRuntime(startCtx, driver.options.Config, driver.options.Resolve, driver.options.OperatorFactory)
	} else {
		adapter, err = startEEBusRuntime(startCtx, driver.options.Config, driver.options.Resolve, driver.options.Factory)
	}
	driver.mu.Lock()
	if err == nil {
		err = startCtx.Err()
	}
	if err == nil && adapter == nil {
		err = runtimecontrol.ErrNilRuntime
	}
	if err != nil {
		cancel()
		driver.cancel = nil
		driver.mu.Unlock()
		if adapter != nil {
			err = errors.Join(err, adapter.Shutdown())
		}
		return 0, err
	}
	number := driver.generation.Add(1)
	driver.current = &eebusManagedGeneration{adapter: adapter, admin: admin, number: number, closed: make(chan struct{})}
	driver.revision.Add(1)
	driver.mu.Unlock()
	return number, nil
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
