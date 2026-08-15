package main

import (
	"context"
	"log"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

// startEEBusAdminAwareRuntime always attempts the typed operator-capable
// runtime when eeBUS is enabled. If that private capability cannot be built,
// the public read-only runtime remains available and only the HTTP operator
// boundary degrades.
func startEEBusAdminAwareRuntime(ctx context.Context, config ebusgateway.Config) (*eebusRuntimeAdapter, eebusruntime.AdminV1, bool, error) {
	if config.EEBusConfig.Enabled {
		adapter, admin, runtimeErr := startEEBusOperatorRuntime(ctx, config.EEBusConfig, resolveEEBusInterfaceAddressesFn, newEEBusOperatorRuntimeFn)
		if runtimeErr == nil {
			return adapter, admin, true, nil
		}
		log.Printf("eeBUS operator boundary unavailable reason=operator_runtime")
	}
	adapter, err := startEEBusRuntime(ctx, config.EEBusConfig, resolveEEBusInterfaceAddressesFn, newEEBusRuntimeFn)
	if err != nil {
		return nil, nil, false, err
	}
	return adapter, nil, false, nil
}
