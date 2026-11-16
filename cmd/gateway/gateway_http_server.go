package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func startHTTPServer(
	ctx context.Context,
	cfg ebusgateway.Config,
	gateway *ebusgateway.Gateway,
	builder *graphql.Builder,
	hub *graphql.BroadcastHub,
	semanticProvider graphql.SemanticProvider,
	eebusProvider mcp.EEBusV1Provider,
	eebusCommandRouter mcp.EEBusV1CommandRouter,
	modbusProvider mcp.ModbusV1Provider,
	portalCatalogSource catalogv1.SourceCapture,
	scheduleWriter mcp.ScheduleWriter,
	configWriter mcp.ConfigWriter,
	busObservability *ebusgateway.BusObservabilityStore,
	watchSummaryProvider mcp.WatchSummaryProvider,
	eebusAdminHandler http.Handler,
	eebusLifecycle *eebusRuntimeLifecycle,
	ebusProxyReadiness func() string,
	ebusSourceProvider func() (byte, bool),
	buildInfo gatewayBuildInfo,
	daemonUpdatesAvailable func() bool,
	ebusDriver *ebusDriverController,
	runtimeControl *gatewayRuntimeControl,
) (*http.Server, mdns.Advertiser, error) {
	if cfg.HTTPAddr == "" {
		return nil, nil, nil
	}
	if err := validatePortalCatalogRoute(cfg); err != nil {
		return nil, nil, err
	}
	if gateway == nil {
		return nil, nil, fmt.Errorf("gateway missing for http server")
	}
	if builder == nil {
		return nil, nil, fmt.Errorf("graphql builder missing for http server")
	}
	if hub == nil {
		return nil, nil, fmt.Errorf("graphql broadcast hub missing for http server")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cfg.ValidatePortalPV(); err != nil {
		return nil, nil, fmt.Errorf("validate Portal PV configuration: %w", err)
	}
	if err := cfg.ValidatePortalStorage(); err != nil {
		return nil, nil, fmt.Errorf("validate Portal storage configuration: %w", err)
	}
	portalPVClient, err := newPortalPVClient(cfg.PortalPV)
	if err != nil {
		return nil, nil, fmt.Errorf("portal PV configuration: %w", err)
	}
	portalStorageClient, err := newPortalStorageClient(cfg.PortalStorage)
	if err != nil {
		return nil, nil, fmt.Errorf("portal storage configuration: %w", err)
	}
	storageAvailable := growattStoragePortalAvailable(modbusProvider)
	if !storageAvailable {
		portalStorageClient = nil
	}

	providers, err := newHTTPControlPlaneProviders(
		cfg, gateway, builder, hub, semanticProvider, eebusProvider, eebusCommandRouter,
		modbusProvider, busObservability, watchSummaryProvider, ebusSourceProvider,
		buildInfo, daemonUpdatesAvailable, ebusDriver, runtimeControl,
	)
	if err != nil {
		return nil, nil, err
	}
	sharedSemanticProvider := providers.sharedSemanticProvider
	queryHandler, snapshotHandler, subscriptionHandler := providers.queryHandler, providers.snapshotHandler, providers.subscriptionHandler
	mcpServer, semanticMetrics := providers.mcpServer, providers.semanticMetrics

	if err := installGatewayHTTPMCPSurface(gatewayHTTPMCPSurface{
		server: mcpServer, gateway: gateway, builder: builder, cfg: &cfg, sourceProvider: ebusSourceProvider,
		ebusDriver: ebusDriver, scheduleWriter: scheduleWriter, configWriter: configWriter,
	}); err != nil {
		return nil, nil, err
	}

	mux := http.NewServeMux()
	routePlan := newHTTPControlPlaneRoutePlan(cfg, busObservability != nil)
	registerHTTPControlPlaneCoreRoutes(
		mux, routePlan, cfg.DumpOutputDir, newGatewayMetricsHandler(busObservability, semanticMetrics), queryHandler, snapshotHandler, subscriptionHandler, mcpServer, eebusAdminHandler,
	)
	var portalOptions portal.Options
	var portalConfig *gatewayPortalConfigRuntime
	if routePlan.portalPath != "" {
		// Startup loaded the record before driver registration and applied
		// its driver selections. A composition without that step mounts the
		// same store and reports nothing as applied.
		portalConfig = runtimeControl.PortalConfig()
		if portalConfig == nil {
			portalConfig = openGatewayPortalConfigRuntime(cfg)
		}
		portalOptions, err = newGatewayHTTPPortalOptions(gatewayHTTPPortalSurface{
			cfg: cfg, gateway: gateway, builder: builder, mcpServer: mcpServer, semanticProvider: semanticProvider,
			eebusProvider: eebusProvider, eebusLifecycle: eebusLifecycle, ebusProxyReadiness: ebusProxyReadiness,
			ebusSourceProvider: ebusSourceProvider, modbusProvider: modbusProvider, busObservability: busObservability,
			buildInfo: buildInfo, runtimeControl: runtimeControl, portalConfig: portalConfig,
			semanticPV: portalPVClient, semanticStorage: portalStorageClient, storageAvailable: storageAvailable,
		})
		if err != nil {
			// The listener would have released the writer; release it here.
			_ = portalConfig.Close()
			return nil, nil, err
		}
	}
	configureGatewayPortalSemanticRead(&portalOptions, sharedSemanticProvider)
	registerHTTPPortalRoutes(mux, routePlan.portalPath, portalOptions, portalCatalogSource)

	cfg.HTTPAddr, _ = gatewayPortalHTTPAddress(cfg.HTTPAddr)
	if portalConfig != nil {
		// The listener releases the writer on exit or startup failure.
		return startHTTPControlPlaneListener(ctx, cfg, mux, gateway, mcpServer, eebusProvider, portalConfig)
	}
	return startHTTPControlPlaneListener(ctx, cfg, mux, gateway, mcpServer, eebusProvider)
}

func gatewayPortalReadiness(eebusProvider mcp.EEBusV1Provider, eebusLifecycle *eebusRuntimeLifecycle, ebusProxyReadiness func() string) func() portal.RuntimeReadiness {
	return func() portal.RuntimeReadiness {
		snapshot := eebusLifecycle.LifecycleSnapshot()
		if adapter, ok := eebusProvider.(*eebusRuntimeAdapter); ok && adapter.managed != nil {
			snapshot = adapter.managed.eebusLifecycleSnapshot()
		}
		return projectGatewayReadiness(ebusProxyReadiness, snapshot)
	}
}

// registerRuntimeControlMCPTools registers the helianthus.runtime tools on the
// ordinary MCP server as thin delegates of the composed control service. They
// stay registered for the shell's lifetime whatever eBUS or eeBUS do (G5 D4:
// tool disappearance is not an unavailable-state signal); a control path
// without a service still lists them and answers UNAVAILABLE.
func registerRuntimeControlMCPTools(server *mcp.Server, control *gatewayRuntimeControl) error {
	var service mcp.RuntimeControlService
	if composed := control.Service(); composed != nil {
		service = composed
	}
	return mcp.RegisterRuntimeControlTools(server, service)
}

// registerHTTPPortalRoutes declares catalog availability at the same boundary
// that mounts the real route, independently of Portal's in-process adapter.
func registerHTTPPortalRoutes(mux *http.ServeMux, portalPath string, opts portal.Options, source catalogv1.SourceCapture) {
	const catalogPath = "/graphql/portal/v1"
	mux.Handle(catalogPath, newPortalCatalogV1Handler(source))
	if portalPath == "" {
		return
	}
	opts.PortalCatalogPath = catalogPath
	opts.PortalCatalogAvailable = true
	opts.BasePath = portalPath + "/"
	portalHandler := portal.NewHandler(opts)
	mux.Handle(portalPath+"/", http.StripPrefix(portalPath, portalHandler))
	// The bare mount reaches the same handler unstripped, so its redirect to the
	// Portal directory carries the served prefix (portal.IngressPathHeader) and
	// the query instead of a root-absolute target that Ingress cannot route.
	mux.Handle(portalPath, portalHandler)
}

func validatePortalCatalogRoute(cfg ebusgateway.Config) error {
	plan := newHTTPControlPlaneRoutePlan(cfg, true)
	for _, path := range []string{plan.metricsPath, plan.graphqlPath, plan.snapshotPath, plan.subscriptionPath, plan.mcpPath, plan.dumpUploadPath, plan.uiPath, plan.portalPath} {
		if strings.TrimRight(path, "/") == "/graphql/portal/v1" {
			return fmt.Errorf("configured route %q collides with reserved Portal catalog route", path)
		}
	}
	return nil
}

func growattStoragePortalAvailable(provider mcp.ModbusV1Provider) bool {
	if availability, ok := provider.(interface{ GrowattStoragePortalAvailable() bool }); ok {
		return availability.GrowattStoragePortalAvailable()
	}
	return false
}

// portalTCPModbusProvider prevents a composed native-only provider from
// advertising Portal's TCP raw-read surface. Native Growatt MCP composition is
// independent of this Portal diagnostic route.
func portalTCPModbusProvider(provider mcp.ModbusV1Provider) mcp.ModbusV1Provider {
	if provider == nil {
		return nil
	}
	if availability, ok := provider.(interface{ ModbusV1CoreAvailable() bool }); ok && !availability.ModbusV1CoreAvailable() {
		return nil
	}
	return provider
}

// installGatewaySemanticOperationRoute composes the one GraphQL
// semantic_operation route over the shared current-generation capture and the
// SemReg operation kernel, and injects its executor lookup. The default lookup
// holds no send-capable executor; a qualified driver composition supplies its
// own. No runtime intent owner is composed yet, so the route answers
// UNAVAILABLE without capture or send until that integration exists.
func installGatewaySemanticOperationRoute(builder *graphql.Builder, snapshots semanticoperations.SnapshotCapture) error {
	return composeGatewaySemanticOperationRoute(builder, snapshots, nil, semanticoperations.NoExecutors())
}

func composeGatewaySemanticOperationRoute(builder *graphql.Builder, snapshots semanticoperations.SnapshotCapture, intents semanticoperations.IntentOwner, executors semanticoperations.ExecutorLookup) error {
	if builder == nil {
		return fmt.Errorf("graphql builder missing for semantic operation route")
	}
	kernel, err := semanticoperations.NewBuiltinKernel()
	if err != nil {
		return fmt.Errorf("compose semantic operation kernel: %w", err)
	}
	builder.SetSemanticOperationRouter(semanticoperations.NewRouter(gatewaySemanticOperationConfigFor(semanticoperations.Config{
		Snapshots: snapshots, Kernel: kernel, Intents: intents, Executors: executors,
	})))
	return nil
}

// gatewaySemanticOperationConfigFor is a test seam around the production route
// composition; production leaves the configuration unchanged.
var gatewaySemanticOperationConfigFor = func(config semanticoperations.Config) semanticoperations.Config { return config }

// configureGatewayPortalSemanticRead binds Portal to the HTTP shared provider.
func configureGatewayPortalSemanticRead(opts *portal.Options, provider *hubSemanticSnapshotProvider) {
	opts.SemanticSnapshotAvailable = true
	opts.SharedSemanticView = func(ctx context.Context) (semanticview.View, error) {
		if provider == nil || provider.hub == nil {
			return semanticview.View{}, semhub.ErrCaptureUnavailable
		}
		selection, ok := provider.hub.(interface{ AssetIDs() []semreg.AssetID })
		if !ok {
			return semanticview.View{}, semhub.ErrCaptureUnavailable
		}
		metadata, inputs, err := provider.Capture(ctx, selection.AssetIDs())
		if err != nil {
			return semanticview.View{}, err
		}
		return semanticview.Build(metadata, inputs)
	}
}
