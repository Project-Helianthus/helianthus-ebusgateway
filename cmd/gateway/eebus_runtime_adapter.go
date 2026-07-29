package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebuscommand"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

type eebusRuntimeFactory func(eebusruntime.Config) (eebusruntime.Runtime, error)

var (
	resolveEEBusInterfaceAddressesFn eebusInterfaceAddressResolver = resolveEEBusInterfaceAddresses
	newEEBusRuntimeFn                eebusRuntimeFactory           = eebusruntime.New
)

type eebusRuntimeAdapter struct {
	runtime eebusruntime.Runtime

	shutdownOnce sync.Once
	shutdownErr  error
}

func eebusMCPProvider(adapter *eebusRuntimeAdapter) mcp.EEBusV1Provider {
	if adapter == nil {
		return nil
	}
	return adapter
}

func eebusMCPCommandRouter(adapter *eebusRuntimeAdapter) mcp.EEBusV1CommandRouter {
	if adapter == nil || adapter.runtime == nil {
		return nil
	}
	return eebuscommand.New(adapter.runtime)
}

func startEEBusRuntime(
	ctx context.Context,
	config ebusgateway.EEBusConfig,
	resolve eebusInterfaceAddressResolver,
	factory eebusRuntimeFactory,
) (*eebusRuntimeAdapter, error) {
	runtimeConfig, err := mapEEBusRuntimeConfig(config, resolve)
	if err != nil {
		return nil, fmt.Errorf("map eeBUS runtime configuration: %w", err)
	}
	if !config.Enabled {
		return nil, nil
	}
	if factory == nil {
		return nil, errors.New("enabled eeBUS configuration requires a runtime factory")
	}
	profile, err := loadEEBusMutationLabProfile(runtimeConfig.StateRoot)
	if err != nil {
		return nil, fmt.Errorf("load eeBUS mutation lab profile: %w", err)
	}
	if profile != nil {
		runtimeConfig.MutationLabProfiles = []eebusraw.MutationLabProfileV1{
			profile.Clone(),
		}
	}

	runtime, factoryErr := factory(runtimeConfig)
	if runtime == nil {
		if factoryErr != nil {
			return nil, fmt.Errorf("construct eeBUS runtime: %w", factoryErr)
		}
		return nil, errors.New("construct eeBUS runtime: factory returned nil")
	}
	adapter := &eebusRuntimeAdapter{runtime: runtime}
	if factoryErr != nil {
		return nil, fmt.Errorf("construct eeBUS runtime: %w", errors.Join(factoryErr, adapter.Shutdown()))
	}
	if err := runtime.Start(ctx); err != nil {
		return nil, fmt.Errorf("start eeBUS runtime: %w", errors.Join(err, adapter.Shutdown()))
	}
	return adapter, nil
}

func (adapter *eebusRuntimeAdapter) Shutdown() error {
	if adapter == nil || adapter.runtime == nil {
		return nil
	}
	adapter.shutdownOnce.Do(func() {
		adapter.shutdownErr = adapter.runtime.Shutdown()
	})
	return adapter.shutdownErr
}

func (adapter *eebusRuntimeAdapter) Snapshot() (eebusruntime.SnapshotV1, error) {
	if adapter == nil || adapter.runtime == nil {
		return eebusruntime.SnapshotV1{}, errors.New("eeBUS runtime unavailable")
	}
	return adapter.runtime.Snapshot()
}
