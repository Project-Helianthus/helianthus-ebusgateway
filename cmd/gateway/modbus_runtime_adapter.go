package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

var (
	newModbusEndpointFn modbusadapter.Factory = func(config modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return modbus.NewTCPEndpoint(config)
	}
	dialModbusEndpointFn modbusadapter.Dialer = (&net.Dialer{}).DialContext
)

func startModbusRuntimeWithPublication(ctx context.Context, config ebusgateway.ModbusTCPConfig, hub *semhub.Hub, dial modbusadapter.Dialer, factory modbusadapter.Factory, scope *modbusadapter.PublicationScope) (*modbusadapter.Adapter, error) {
	runtimeConfig, err := mapModbusRuntimeConfig(config)
	if err != nil {
		return nil, fmt.Errorf("map Modbus TCP runtime configuration: %w", err)
	}
	if runtimeConfig.Enabled {
		// Dynamically discovered PV assets are declared per asset through the
		// PV class of the process's semantic hub.
		if hub == nil {
			return nil, fmt.Errorf("start Modbus TCP runtime: semantic hub unavailable")
		}
		if runtimeConfig.SemanticPV, err = modbusadapter.NewSemanticPVClass(hub); err != nil {
			return nil, fmt.Errorf("start Modbus TCP runtime: %w", err)
		}
	}
	runtimeConfig.Publication = scope
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

type managedSunSpecWork struct {
	ctx           context.Context
	adapter       *modbusadapter.Adapter
	contributions *gatewayPortalCatalogContributions
	generation    uint64
	worker        *sunSpecLiveSmokeWorker
	lease         *gatewayPortalContributionLifecycle
	err           error
}

func newManagedSunSpecWork(ctx context.Context, adapter *modbusadapter.Adapter, contributions *gatewayPortalCatalogContributions, generation uint64) *managedSunSpecWork {
	return &managedSunSpecWork{ctx: ctx, adapter: adapter, contributions: contributions, generation: generation}
}
func (work *managedSunSpecWork) Start() {
	work.lease, work.err = startGatewayPortalContributionLifecycle(work.contributions, contributionv1.DriverGeneration{DriverID: "pv.primary", Generation: work.generation}, []contributionv1.Manifest{gatewayPortalPVDescriptor()})
	if work.err != nil {
		return
	}
	work.worker = newGatewaySunSpecLiveSmokeWorker(work.ctx, work.adapter, log.Printf)
	work.worker.Start()
}
func (work *managedSunSpecWork) Close() error {
	if work.lease != nil {
		work.lease.Stop()
	}
	return errors.Join(work.err, work.worker.Close())
}
