package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
)

// gatewayAPIComposition owns the API composition of the gateway: the startup
// source selection and the observability providers that the API reads, the
// gateway itself, the static seed, the GraphQL builder, the address-table
// inserter and its runtime-state observer, the hub and router plane, the
// semantic runtime, the late-bound eBUS writers, the HTTP control plane and the
// registration of the consumer shutdown.
//
// Contract:
//   - Required collaborators: the process context, the process shell after
//     acquireResources and acquireEEBusSurfaces, the run configuration that
//     the shell prepared and the Portal catalog source.
//     newGatewayAPIComposition rejects a missing collaborator before any side
//     effect.
//   - Startup: compose builds, in this order, the startup source selection
//     with its delayed artifact emission, the observe-first observers with the
//     transport watch and the semantic metrics and adapter multiplexer
//     providers, the gateway (created, then started) with its v8 rollout
//     observability, the static seed, the GraphQL builder with the live
//     admitted source, the address-table inserter, the hub and router plane,
//     the semantic runtime, and the late-bound writers, then starts the
//     builder. serve, called after the caller has created the retained eBUS
//     consumers, starts the HTTP control plane through startHTTPServerFn on
//     the control-plane context and registers the consumer shutdown. Neither
//     waits for an eBUS generation.
//   - Defaults: without bus observability no transport watch is registered
//     and the observability providers stay unset; the static seed is applied
//     only when it is enabled; the mutable eBUS surfaces are late-bound and
//     fail closed until a generation admits a source; an empty HTTP address
//     starts no listener and registers a consumer shutdown without an HTTP
//     drain.
//   - Failure isolation: the first failing step returns its error unchanged
//     and the releases registered until then still run. A gateway close
//     failure is logged and leaves the process result unchanged.
//   - Shutdown: the releases join the process-shell shutdown sequence in this
//     order: final source-selection artifact, transport watch, gateway,
//     control-plane context, consumers. They run in reverse after the
//     generation tasks have joined, so the consumers are closed first and the
//     final artifact is written last of them.
type gatewayAPIComposition struct {
	ctx                 context.Context
	cfg                 *ebusgateway.Config
	shell               *gatewayProcessShell
	portalCatalogSource catalogv1.SourceCapture

	startupSelection   startupSourceSelection
	busObservability   *ebusgateway.BusObservabilityStore
	deduplicator       *ebusgateway.ActivePassiveDeduplicator
	stopTransportWatch func()

	gateway                *ebusgateway.Gateway
	builder                *graphql.Builder
	liveAdmittedEBusSource func() (byte, bool)
	addressTableInserter   *ebusgateway.AddressTableInserter
	hub                    *graphql.BroadcastHub
	semanticRuntime        *graphql.SemanticRuntime
	portalSemanticProvider graphql.SemanticProvider

	lateScheduleWriter       *lateEBusScheduleWriter
	lateConfigWriter         *lateEBusConfigWriter
	lateWatchProvider        *lateEBusWatchSummaryProvider
	lateGraphQLWriter        *lateEBusGraphQLWriter
	lateGraphQLWatchProvider *lateEBusGraphQLWatchSummaryProvider
}

// newGatewayAPIComposition validates the collaborators. It acquires nothing.
func newGatewayAPIComposition(ctx context.Context, shell *gatewayProcessShell, cfg *ebusgateway.Config, portalCatalogSource catalogv1.SourceCapture) (*gatewayAPIComposition, error) {
	required := []struct {
		name    string
		missing func() bool
	}{
		{"process context", func() bool { return ctx == nil }},
		{"process shell", func() bool { return shell == nil }},
		{"run configuration", func() bool { return cfg == nil }},
		{"Portal catalog source", func() bool { return portalCatalogSource == nil }},
		{"process shell run configuration", func() bool { return shell.cfg != cfg }},
		{"runtime state", func() bool { return shell.runtimeStateMgr == nil }},
		{"semantic publication hub", func() bool { return shell.semanticHub == nil }},
		{"release checker", func() bool { return shell.releaseChecker == nil }},
		{"managed sidecar bindings", func() bool { return shell.bindings == nil }},
		{"eBUS driver", func() bool { return shell.ebusDriver == nil }},
		{"runtime control", func() bool { return shell.control == nil }},
		{"eeBUS adapter", func() bool { return shell.eebusAdapter == nil }},
		{"eeBUS admin handler", func() bool { return shell.eebusAdminHandler == nil }},
	}
	for _, requirement := range required {
		if requirement.missing() {
			return nil, fmt.Errorf("gateway API composition: %s is required", requirement.name)
		}
	}
	return &gatewayAPIComposition{ctx: ctx, cfg: cfg, shell: shell, portalCatalogSource: portalCatalogSource}, nil
}

// compose builds the API in startup order. It starts no listener.
func (api *gatewayAPIComposition) compose() error {
	if err := api.composeStartupObservability(); err != nil {
		return err
	}
	if err := api.composeGateway(); err != nil {
		return err
	}
	api.composeGraphQLBuilder()
	api.composeSemanticRuntime()
	return api.composeLateBoundWriters()
}

// composeStartupObservability prepares the startup source selection and the
// observe-first observers and installs the providers that read them.
func (api *gatewayAPIComposition) composeStartupObservability() error {
	ctx, cfg, shell := api.ctx, api.cfg, api.shell
	// Warn if --proxy-listen is set but adapter-direct transport was not
	// activated (the proxy endpoint requires the adapter multiplexer).
	if cfg.ProxyListenAddr != "" && !adapterDirectProxyEnabled(cfg.TransportConfig) {
		log.Printf("warning: --proxy-listen requires adapter-direct transport; proxy endpoint not started")
	}
	shell.acquire("startup source selection")
	startupSelection, err := newStartupSourceSelection(*cfg)
	if err != nil {
		return err
	}
	// The generation owners hold the remaining source-selection values.
	api.startupSelection = startupSelection
	startupSelection.startArtifactEmission(ctx)
	shell.onShutdownFunc("final source-selection artifact", startupSelection.emitFinalArtifact)

	shell.acquire("observe-first observers")
	busObservability, deduplicator, err := wireObserveFirstObserversFn(cfg)
	if err != nil {
		return err
	}
	api.busObservability, api.deduplicator = busObservability, deduplicator
	api.stopTransportWatch = func() {}
	if busObservability != nil {
		api.stopTransportWatch = shell.bindings.observeTransports(ctx, *cfg, busObservability)
		shell.onShutdownFunc("transport watch", api.stopTransportWatch)
	}
	wireSemanticMetricsProvider(busObservability, *cfg, shell.bindings, shell.teslaHSCRetained)

	wireAdaptermuxDiagnosticProviders(busObservability, shell.adapterClassifier)
	return nil
}

// composeGateway creates and starts the gateway, registers its close and
// applies the static seed when it is enabled.
func (api *gatewayAPIComposition) composeGateway() error {
	ctx, shell := api.ctx, api.shell
	shell.acquire("gateway")
	gateway, err := ebusgateway.New(ctx, *api.cfg)
	if err != nil {
		return err
	}
	api.gateway = gateway

	shell.onShutdown("gateway", func(result error) error {
		if err := gateway.Close(); err != nil {
			log.Printf("gateway close: %v", err)
		}
		return result
	})

	gateway.Start(ctx)

	wireV8RolloutObservability(api.busObservability, gateway.Bus, shell.adapterClassifier)

	// When enabled, plant the static seed entries into the registry at
	// startup. They make known addresses that do not answer an active scan
	// visible from MCP, GraphQL and Portal before passive observation produces
	// corroborated evidence. Identity (manufacturer and device id) comes from
	// the seed table; serial and software or hardware versions stay empty
	// until enrichment populates them.
	if api.cfg.EnableStaticSeedTable {
		shell.acquire("static seed")
		applyStaticSeedTable(gateway.Registry)
	}
	return nil
}

// composeGraphQLBuilder creates the GraphQL builder with the live admitted
// source, the address-table inserter and its runtime-state observer, and the
// broadcast hub on the gateway router plane.
func (api *gatewayAPIComposition) composeGraphQLBuilder() {
	cfg, shell, gateway := api.cfg, api.shell, api.gateway
	shell.acquire("graphql builder")
	builder := graphql.NewBuilder(gateway.Registry, nil)
	liveAdmittedEBusSource := newManagedEBusSourceProvider(shell.ebusDriver, builder.AdmittedMutationSource)
	builder.SetAdmittedMutationSourceProvider(liveAdmittedEBusSource)
	api.builder, api.liveAdmittedEBusSource = builder, liveAdmittedEBusSource

	// The address table and its inserter consume the passive reconstructor's
	// classified events to insert passively observed addresses into the
	// registry as passive_observed or corroborated. The inserter is idle until
	// the retained consumers subscribe it to the reconstructor.
	//
	// The inserter reads the admitted source live from the builder: source
	// selection may change the admitted source after startup, and a snapshot
	// taken here would let the inserter mistake the gateway's own admitted
	// source for a third-party initiator.
	shell.acquire("address-table inserter")
	addressTableCfg := *cfg
	addressTableCfg.AdmittedSource = func() byte {
		source, ok := builder.AdmittedMutationSource()
		if !ok {
			return 0
		}
		return source
	}
	addressTable := ebusgateway.NewAddressTable(gateway.Registry)
	addressTableInserter := ebusgateway.NewAddressTableInserter(addressTable, addressTableCfg)
	// Every passive observation that flows through the inserter refreshes the
	// presence of a known bus member in the runtime state. The refresh keeps
	// identity, companion address and confidence of an existing entry: only
	// the directed-probe responder path may raise confidence to verified, and
	// only identity probes write identity and companion address. Without it
	// the known bus members would stay empty after a fresh install and the
	// revalidator would have nothing to revalidate.
	runtimeStateMgr := shell.runtimeStateMgr
	addressTableInserter.SetRuntimeStateObserver(func(addr byte, observedAt time.Time, reportedSource string) {
		// Fresh passive evidence clears the eviction blocklist entry of the
		// address: an address observed on the bus again is not a stale ghost.
		globalEvictionBlocklist.clear(addr)
		runtimeStateMgr.RefreshKnownBusMemberPresence(addr, observedAt, runtimestate.LastSource(reportedSource))
	})
	api.addressTableInserter = addressTableInserter
	if api.busObservability != nil {
		builder.SetBusObservabilityProvider(newGraphQLBusObservabilityProvider(api.busObservability))
	}
	wireSourceSelectionBaselineEvidence(api.startupSelection.artifactBuilder, api.busObservability)
	builder.SetGatewayIdentityProvider(newRuntimeGatewayIdentityProvider(*cfg))
	shell.acquire("router plane")
	hub := graphql.NewBroadcastHub(nil)
	gateway.AddRouterPlane(hub)
	gateway.RefreshRouterPlanes()
	api.hub = hub
}

// composeSemanticRuntime wires and starts the semantic runtime and the
// status provider over it.
func (api *gatewayAPIComposition) composeSemanticRuntime() {
	ctx, shell, builder := api.ctx, api.shell, api.builder
	shell.acquire("semantic runtime")
	semanticRuntime := graphql.WireSemantic(builder, api.gateway.Router, api.hub)
	api.semanticRuntime = semanticRuntime
	api.portalSemanticProvider = wireEEBusPromotedSemanticGraphQL(ctx, builder, semanticRuntime.Provider(), shell.eebusAdapter)
	builder.SetStatusProvider(newRuntimeStatusProviderForControl(semanticRuntime.Provider(), api.liveAdmittedEBusSource, shell.buildInfo, shell.releaseChecker.UpdatesAvailable, ebusAdapterLifecycleStatus(shell.ebusDriver)))
	semanticRuntime.SetBootLiveTimeout(api.cfg.BootLiveTimeout)
	semanticRuntime.Start(ctx)
	wireSemanticStartupObservability(api.busObservability, semanticRuntime)
}

// composeLateBoundWriters installs the late-bound eBUS writers and watch
// providers on the builder and starts the builder.
func (api *gatewayAPIComposition) composeLateBoundWriters() error {
	// The control plane is process-owned, not eBUS-owned. It starts before
	// passive source-selection warmup so a slow or unavailable provider cannot
	// delay the stable HTTP/API shell. Mutable eBUS surfaces use late bindings:
	// they fail closed before source admission and delegate without rebuilding
	// the server once the healthy semantic poller is ready.
	api.shell.acquire("late-bound writers")
	api.lateScheduleWriter = &lateEBusScheduleWriter{}
	api.lateConfigWriter = &lateEBusConfigWriter{}
	api.lateWatchProvider = &lateEBusWatchSummaryProvider{}
	api.lateGraphQLWriter = &lateEBusGraphQLWriter{}
	api.lateGraphQLWatchProvider = &lateEBusGraphQLWatchSummaryProvider{}
	builder := api.builder
	builder.SetBoilerConfigWriter(api.lateGraphQLWriter)
	builder.SetSystemConfigWriter(api.lateGraphQLWriter)
	builder.SetScheduleWriter(api.lateGraphQLWriter)
	builder.SetWatchSummaryProvider(api.lateGraphQLWatchProvider)
	api.shell.acquire("graphql builder start")
	return builder.Start(api.ctx)
}

// retainedConsumerOptions returns the collaborators of the retained eBUS
// consumers that the composition holds. The caller supplies the leaf capture
// state root.
func (api *gatewayAPIComposition) retainedConsumerOptions() retainedEBusConsumerOptions {
	selection := api.startupSelection
	return retainedEBusConsumerOptions{
		Config:               *api.cfg,
		SourceSelectionAdmit: selection.admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && !selection.overrideSet,
		Gateway:              api.gateway,
		Builder:              api.builder,
		SemanticRuntime:      api.semanticRuntime,
		SemanticHub:          api.shell.semanticHub,
		Hub:                  api.hub,
		Control:              api.shell.control,
		AddressTableInserter: api.addressTableInserter,
		EEBusAdapter:         api.shell.eebusAdapter,
		AdmittedSource:       api.liveAdmittedEBusSource,
		ScheduleWriter:       api.lateScheduleWriter,
		ConfigWriter:         api.lateConfigWriter,
		WatchProvider:        api.lateWatchProvider,
		GraphQLWriter:        api.lateGraphQLWriter,
		GraphQLWatchSource:   api.lateGraphQLWatchProvider,
		BusObservability:     api.busObservability,
		Deduplicator:         api.deduplicator,
	}
}

// serve starts the HTTP control plane and registers the consumer shutdown
// over the retained eBUS consumers.
func (api *gatewayAPIComposition) serve(retained *retainedEBusConsumers) error {
	if retained == nil {
		return errors.New("gateway API composition: retained eBUS consumers are required")
	}
	server, advertiser, err := api.startControlPlane()
	if err != nil {
		return err
	}
	cfg, busObservability, deduplicator := api.cfg, api.busObservability, api.deduplicator
	stopTransportWatch := api.stopTransportWatch
	consumerShutdown := gatewayConsumerShutdown{
		stopTransportWatch: stopTransportWatch,
		publishRetirement: func() {
			publishManagedTransportRetirement(busObservability, *cfg)
		},
		drainHTTP: func() error {
			if server != nil {
				return shutdownHTTPControlPlane(server)
			}
			return nil
		},
		surface: retained.surface,
	}
	if deduplicator != nil {
		consumerShutdown.deduplicator = deduplicator
	}
	if busObservability != nil {
		consumerShutdown.busObservability = busObservability
	}
	if advertiser != nil {
		consumerShutdown.advertiser = advertiser
	}
	api.shell.onShutdownFunc("consumers", consumerShutdown.shutdown)
	return nil
}

// startControlPlane registers the cancellation of the control-plane context
// and starts the HTTP control plane through startHTTPServerFn.
func (api *gatewayAPIComposition) startControlPlane() (*http.Server, mdns.Advertiser, error) {
	ctx, cfg, shell := api.ctx, api.cfg, api.shell
	gateway, builder, hub, busObservability := api.gateway, api.builder, api.hub, api.busObservability
	ebusDriver, control, bindings := shell.ebusDriver, shell.control, shell.bindings
	eebusAdapter, eebusAdminHandler := shell.eebusAdapter, shell.eebusAdminHandler
	resolvedBuildInfo, daemonReleaseChecker := shell.buildInfo, shell.releaseChecker
	lateWatchProvider, liveAdmittedEBusSource := api.lateWatchProvider, api.liveAdmittedEBusSource
	startHTTPServerOverride := startHTTPServerFn
	startHTTPServerFn := func(
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
		shadowCache *ebusgateway.ShadowCache,
	) (*http.Server, mdns.Advertiser, error) {
		if startHTTPServerOverride != nil {
			return startHTTPServerOverride(
				ctx, cfg, gateway, builder, hub, semanticProvider, scheduleWriter,
				configWriter, busObservability, shadowCache,
			)
		}
		return startHTTPServer(
			ctx, cfg, gateway, builder, hub, semanticProvider, eebusProvider, eebusCommandRouter,
			modbusProvider, portalCatalogSource, scheduleWriter, configWriter, busObservability, lateWatchProvider, eebusAdminHandler, nil, ebusDriver.ProxyReadiness, liveAdmittedEBusSource, resolvedBuildInfo, daemonReleaseChecker.UpdatesAvailable, ebusDriver, control,
		)
	}
	// Keep the control plane alive through final transport retirement. The main
	// context still cancels work immediately; this lifecycle-owned context is
	// canceled by teardown after retirement and the explicit close sequence.
	controlPlaneCtx, controlPlaneCancel := newGatewayControlPlaneContext(ctx)
	shell.onShutdownFunc("control-plane context", controlPlaneCancel)
	shell.acquire("HTTP control plane")
	return startHTTPServerFn(
		controlPlaneCtx,
		*cfg,
		gateway,
		builder,
		hub,
		api.portalSemanticProvider,
		eebusMCPProvider(eebusAdapter),
		eebusMCPCommandRouter(eebusAdapter),
		newManagedModbusMCPProvider(bindings, cfg.ModbusTCPConfig, shell.teslaHSCRetained),
		api.portalCatalogSource,
		api.lateScheduleWriter,
		api.lateConfigWriter,
		busObservability,
		nil,
	)
}
