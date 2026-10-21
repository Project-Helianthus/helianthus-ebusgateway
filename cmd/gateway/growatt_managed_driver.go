package main

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// The factory is inert until G5 constructs and starts this compiled driver.
// The profile is the existing native read-only path, never Protocol II.
func newGrowattManagedDriverFactory(config ebusgateway.GrowattBMSRS485Config, profile modbusProviderProfileID, hub *semhub.Hub, open func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error)) runtimecontrol.LifecycleAdapter {
	if open == nil {
		open = openGrowattBMSRTUEndpoint
	}
	return runtimecontrol.NewAdapter(func(ctx context.Context, _ runtimecontrol.Construction) (*growattManagedDriver, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if profile != modbusProviderGrowattBMS {
			return nil, errModbusProviderUnknown
		}
		return &growattManagedDriver{config: config, hub: hub, open: open}, nil
	})
}

type growattManagedDriver struct {
	mu          sync.Mutex
	config      ebusgateway.GrowattBMSRS485Config
	hub         *semhub.Hub
	open        func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error)
	provider    *growattBMSRS485ProductionProvider
	current     atomic.Pointer[growattBMSRS485ProductionProvider]
	handle      *semhub.Handle
	generation  uint64
	revision    uint64
	draining    chan struct{}
	drainErr    error
	starting    chan struct{}
	startCancel context.CancelFunc
}

var _ drivermanager.Runtime = (*growattManagedDriver)(nil)

func (driver *growattManagedDriver) Start(ctx context.Context) (uint64, error) {
	driver.mu.Lock()
	if err := ctx.Err(); err != nil {
		driver.mu.Unlock()
		return 0, err
	}
	if driver.starting != nil {
		driver.mu.Unlock()
		return 0, drivermanager.ErrUnavailable
	}
	if driver.draining != nil {
		select {
		case <-driver.draining:
			if driver.drainErr != nil {
				err := driver.drainErr
				driver.mu.Unlock()
				return 0, err
			}
			driver.draining = nil
		default:
			driver.mu.Unlock()
			return 0, drivermanager.ErrUnavailable
		}
	}
	if driver.provider != nil {
		generation := driver.generation
		driver.mu.Unlock()
		return generation, nil
	}
	if !driver.config.Enabled {
		driver.mu.Unlock()
		return 0, drivermanager.ErrUnavailable
	}
	config, previous := driver.config, driver.handle
	if previous != nil {
		config.DriverGeneration = driver.generation + 1
	}
	ctx, cancel := context.WithCancel(ctx)
	driver.startCancel = cancel
	done := make(chan struct{})
	driver.starting = done
	driver.mu.Unlock()
	defer cancel()
	var admitted *semhub.Handle
	provider, err := startGrowattBMSRS485RuntimeWithEndpoint(config, driver.hub, func(config modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return driver.open(config)
	}, func(request semhub.Admission) (*semhub.Handle, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var admissionErr error
		if previous == nil {
			admitted, admissionErr = driver.hub.AdmitSource(request)
		} else {
			admitted, admissionErr = driver.hub.Activate(previous, config.DriverGeneration)
		}
		return admitted, admissionErr
	})
	if err == nil {
		err = ctx.Err()
	}
	var cleanupErr error
	if err != nil && admitted != nil {
		if provider != nil {
			cleanupErr = provider.Close()
		}
		if withdrawErr := driver.hub.Withdraw(admitted); withdrawErr == nil {
			admitted = nil
		} else {
			cleanupErr = errors.Join(cleanupErr, driver.fence(admitted, provider))
		}
	}
	driver.mu.Lock()
	defer driver.mu.Unlock()
	driver.starting, driver.startCancel = nil, nil
	defer close(done)
	driver.revision++
	if admitted != nil {
		driver.handle = admitted
		driver.generation = config.DriverGeneration
	}
	if err != nil {
		if cleanupErr != nil {
			driver.drainErr = cleanupErr
			driver.draining = make(chan struct{})
			close(driver.draining)
		}
		return 0, errors.Join(err, cleanupErr)
	}
	driver.provider = provider
	driver.current.Store(provider)
	return driver.generation, nil
}

func (driver *growattManagedDriver) Replace(ctx context.Context) (uint64, error) {
	if err := driver.Stop(ctx); err != nil {
		return 0, err
	}
	return driver.Start(ctx)
}

// Withdrawal cancels acquisition immediately; Stop supplies the join proof.
func (driver *growattManagedDriver) FenceWithdrawal() {
	if provider := driver.current.Load(); provider != nil {
		provider.cancel()
	}
}

func (driver *growattManagedDriver) Stop(ctx context.Context) error {
	driver.mu.Lock()
	if driver.starting != nil {
		driver.startCancel()
		done := driver.starting
		driver.mu.Unlock()
		select {
		case <-done:
			return driver.Stop(ctx)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if driver.draining == nil && driver.provider != nil {
		provider, handle := driver.provider, driver.handle
		provider.cancel()
		driver.provider = nil
		driver.current.Store(nil)
		driver.draining = make(chan struct{})
		done := driver.draining
		go func() {
			// Fence before native work, outside mu so callers can time out
			// and inspect status even if the hub's drain stalls.
			fenceErr := driver.fence(handle, provider)
			_ = provider.acquire(context.Background())
			provider.release()
			closeErr := provider.Close()
			driver.mu.Lock()
			driver.drainErr = errors.Join(closeErr, fenceErr)
			driver.revision++
			close(done)
			driver.mu.Unlock()
		}()
	}
	done := driver.draining
	driver.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		driver.mu.Lock()
		defer driver.mu.Unlock()
		return driver.drainErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (driver *growattManagedDriver) fence(handle *semhub.Handle, provider *growattBMSRS485ProductionProvider) error {
	if handle == nil {
		return nil
	}
	mono := time.Duration(1)
	if provider != nil {
		if evidence, ok := provider.LastObservationEvidence(); ok {
			mono = evidence.ReceiptMonotonic + 1
		}
	}
	_, err := driver.hub.Fence(semhub.FenceRequest{
		Source: semreg.SourceID(driver.config.SourceID), Epoch: semreg.SourceEpochID(driver.config.SourceEpoch), Generation: handle.Generation(),
		Reason:     "lifecycle.generation_replaced",
		Evidence:   []semreg.EvidenceRef{growattStorageDigestEvidence("native.growatt.bms.rs485.v202.generation_fence", "managed lifecycle withdrawal")},
		ObservedAt: growattWall(time.Now()), Monotonic: semreg.MonotonicPoint{ClockEpochID: growattBMSRS485ClockEpoch, Nanoseconds: semreg.Uint64(strconv.FormatInt(int64(mono), 10))},
	})
	return err
}

func (driver *growattManagedDriver) Generation() uint64 {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	return driver.generation
}
func (driver *growattManagedDriver) Revision() uint64 {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	return driver.revision
}
func (driver *growattManagedDriver) SafetyQuarantined() bool {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	return driver.drainErr != nil
}

func (driver *growattManagedDriver) Provider() (*growattBMSRS485ProductionProvider, bool) {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	return driver.provider, driver.provider != nil && driver.provider.lifetime.Err() == nil
}
