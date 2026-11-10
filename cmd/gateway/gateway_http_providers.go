package main

import (
	"fmt"
	"net/http"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// httpControlPlaneProviders retains the shared semantic provider and the
// handlers that consume it. The caller owns validation, routes and listeners.
type httpControlPlaneProviders struct {
	sharedSemanticProvider *hubSemanticSnapshotProvider
	queryHandler           http.Handler
	snapshotHandler        http.Handler
	subscriptionHandler    http.Handler
	mcpServer              *mcp.Server
	semanticMetrics        *gatewaySemanticMetrics
}

// newHTTPControlPlaneProviders composes registrations in startup order without
// mounting routes or opening listeners. Optional native providers remain optional.
func newHTTPControlPlaneProviders(
	cfg ebusgateway.Config,
	gateway *ebusgateway.Gateway,
	builder *graphql.Builder,
	hub *graphql.BroadcastHub,
	semanticProvider graphql.SemanticProvider,
	eebusProvider mcp.EEBusV1Provider,
	eebusCommandRouter mcp.EEBusV1CommandRouter,
	modbusProvider mcp.ModbusV1Provider,
	busObservability *ebusgateway.BusObservabilityStore,
	watchSummaryProvider mcp.WatchSummaryProvider,
	ebusSourceProvider func() (byte, bool),
	buildInfo gatewayBuildInfo,
	daemonUpdatesAvailable func() bool,
	ebusDriver *ebusDriverController,
	runtimeControl *gatewayRuntimeControl,
) (*httpControlPlaneProviders, error) {
	sharedSemanticProvider := newGatewaySemanticSnapshotProvider(runtimeControl)
	if err := installGatewaySemanticOperationRoute(builder, sharedSemanticProvider); err != nil {
		return nil, err
	}
	queryHandler, snapshotHandler, subscriptionHandler, mcpServer, err := newHTTPControlPlaneSemanticHandlers(gateway, builder, hub, sharedSemanticProvider)
	if err != nil {
		return nil, err
	}
	// The scrape route reads the same provider instance as GraphQL, MCP and
	// Portal; its transport exposition stays owned by the bus store.
	semanticMetrics, err := newGatewaySemanticMetrics(sharedSemanticProvider, gatewaySemanticMetricRegistry())
	if err != nil {
		return nil, err
	}
	if err := mcpServer.SetServerVersion(buildInfo.ReleaseVersion); err != nil {
		return nil, fmt.Errorf("configure MCP build identity: %w", err)
	}
	if err := registerRuntimeControlMCPTools(mcpServer, runtimeControl); err != nil {
		return nil, fmt.Errorf("register runtime control MCP tools: %w", err)
	}
	if eebusProvider != nil {
		if err := mcpServer.RegisterEEBusV1Provider(eebusProvider); err != nil {
			return nil, fmt.Errorf("register eeBUS MCP provider: %w", err)
		}
	}
	if eebusCommandRouter != nil {
		if err := mcpServer.RegisterEEBusV1CommandRouter(eebusCommandRouter); err != nil {
			return nil, fmt.Errorf("register eeBUS MCP command router: %w", err)
		}
	}
	mcp.RegisterModbusV1Tools(mcpServer, modbusProvider)
	if source, ok := eebusProvider.(mcp.LeafPromotionCaptureSource); ok {
		if capture := source.LeafPromotionCapture(); capture != nil {
			if err := mcpServer.RegisterLeafPromotionCapture(capture); err != nil {
				return nil, fmt.Errorf("register leaf promotion capture: %w", err)
			}
		}
	}
	oneShotRuntime, err := newSynchronizedEvidenceOneShotRuntime(
		cfg.EvidenceOneShotEnabled,
		eebusProvider,
		eebusCommandRouter,
		buildInfo,
	)
	if err != nil {
		return nil, err
	}
	if oneShotRuntime != nil {
		if err := mcpServer.RegisterSynchronizedEvidenceCapture(oneShotRuntime); err != nil {
			return nil, fmt.Errorf("register synchronized evidence capture: %w", err)
		}
	}
	mcpServer.SetAdmittedRPCSourceProvider(ebusSourceProvider)
	mcpServer.SetStatusProvider(newMCPRuntimeStatusProviderForControl(semanticProvider, ebusSourceProvider, buildInfo, daemonUpdatesAvailable, ebusAdapterLifecycleStatus(ebusDriver), ebusAdapterConditionFor(ebusDriver)))
	if busObservability != nil {
		mcpServer.SetBusObservabilityProvider(newMCPBusObservabilityProvider(busObservability))
	}
	if watchSummaryProvider != nil {
		mcpServer.SetWatchSummaryProvider(watchSummaryProvider)
	}
	mcpServer.SetSemanticProvider(newMCPSemanticProvider(semanticProvider))

	return &httpControlPlaneProviders{
		sharedSemanticProvider: sharedSemanticProvider,
		queryHandler:           queryHandler,
		snapshotHandler:        snapshotHandler,
		subscriptionHandler:    subscriptionHandler,
		mcpServer:              mcpServer,
		semanticMetrics:        semanticMetrics,
	}, nil
}
