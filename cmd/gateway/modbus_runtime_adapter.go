package main

import (
	"context"
	"fmt"
	"net"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

var (
	newModbusEndpointFn modbusadapter.Factory = func(config modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return modbus.NewTCPEndpoint(config)
	}
	dialModbusEndpointFn modbusadapter.Dialer = (&net.Dialer{}).DialContext
)

func startModbusRuntime(
	ctx context.Context,
	config ebusgateway.ModbusTCPConfig,
	dial modbusadapter.Dialer,
	factory modbusadapter.Factory,
) (*modbusadapter.Adapter, error) {
	runtimeConfig, err := mapModbusRuntimeConfig(config)
	if err != nil {
		return nil, fmt.Errorf("map Modbus TCP runtime configuration: %w", err)
	}
	adapter, err := modbusadapter.Start(ctx, runtimeConfig, dial, factory)
	if err != nil {
		return nil, fmt.Errorf("start Modbus TCP runtime: %w", err)
	}
	return adapter, nil
}

// startGatewayPortalPVContribution binds the existing Modbus adapter lifecycle
// to its Portal contribution. The adapter has no Gateway-owned methods, so the
// caller closes this lease before closing the adapter.
func startGatewayPortalPVContribution(adapter *modbusadapter.Adapter, contributions *gatewayPortalCatalogContributions) (*gatewayPortalContributionLifecycle, error) {
	if adapter == nil {
		return nil, nil
	}
	return startGatewayPortalContributionLifecycle(contributions, contributionv1.DriverGeneration{DriverID: "pv.primary", Generation: 1}, []contributionv1.Manifest{gatewayPortalPVDescriptor()})
}
