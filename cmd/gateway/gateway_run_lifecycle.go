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
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/releasecheck"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
)

// startGatewayReleaseChecker starts the cache-only release discovery task.
var startGatewayReleaseChecker = func(checker *releasecheck.Checker, ctx context.Context) {
	checker.Start(ctx)
}

func runGatewayLifecycle(ctx context.Context, cfg ebusgateway.Config) (result error) {
	shell, err := newGatewayProcessShell(ctx, &cfg)
	if err != nil {
		return err
	}
	// Every release of this function joins the shell's shutdown sequence in
	// acquisition order; the sequence runs them last in, first out and applies
	// the endpoint redaction last.
	defer func() {
		result = shell.runShutdown(result)
	}()
	if err := shell.acquireResources(); err != nil {
		return err
	}
	resolvedBuildInfo, semanticHub, daemonReleaseChecker := shell.buildInfo, shell.semanticHub, shell.releaseChecker
	runtimeStateMgr, runtimeState := shell.runtimeStateMgr, shell.runtimeState
	portalContributions, bindings := shell.portalContributions, shell.bindings
	ebusDriver, adapterClassifier, control := shell.ebusDriver, shell.adapterClassifier, shell.control
	teslaHSCRetained := shell.teslaHSCRetained
	portalCatalogSource := newGatewayPortalCatalogSource(nil, nil, teslaHSCRetained, portalContributions, bindings)
	m2mRuntime, err := newProfileM2MRuntime(cfg, teslaHSCRetained, portalCatalogSource, bindings)
	if err != nil {
		return fmt.Errorf("M2M GraphQL sidecar: %w", err)
	}
	if m2mRuntime != nil {
		shell.onShutdown("M2M sidecar", func(result error) error {
			if err := m2mRuntime.Close(); err != nil {
				result = errors.Join(result, fmt.Errorf("shutdown M2M GraphQL sidecar: %w", err))
			}
			return result
		})
	}

	if err := shell.acquireEEBusSurfaces(); err != nil {
		return err
	}
	eebusAdapter, eebusAdminHandler := shell.eebusAdapter, shell.eebusAdminHandler

	// Warn if --proxy-listen is set but adapter-direct transport was not
	// activated (the proxy endpoint requires the adapter multiplexer).
	if cfg.ProxyListenAddr != "" && !adapterDirectProxyEnabled(cfg.TransportConfig) {
		log.Printf("warning: --proxy-listen requires adapter-direct transport; proxy endpoint not started")
	}

	startupSelection, err := newStartupSourceSelection(cfg)
	if err != nil {
		return err
	}
	// The generation owners below hold the remaining source-selection values.
	admissionPath, artifactBuilder := startupSelection.admissionPath, startupSelection.artifactBuilder
	overrideSet := startupSelection.overrideSet
	startupSelection.startArtifactEmission(ctx)
	shell.onShutdownFunc("final source-selection artifact", startupSelection.emitFinalArtifact)

	busObservability, deduplicator, err := wireObserveFirstObserversFn(&cfg)
	if err != nil {
		return err
	}
	stopTransportWatch := func() {}
	if busObservability != nil {
		stopTransportWatch = bindings.observeTransports(ctx, cfg, busObservability)
		shell.onShutdownFunc("transport watch", stopTransportWatch)
	}
	wireSemanticMetricsProvider(busObservability, cfg, bindings, teslaHSCRetained)

	wireAdaptermuxDiagnosticProviders(busObservability, adapterClassifier)

	gateway, err := ebusgateway.New(ctx, cfg)
	if err != nil {
		return err
	}

	shell.onShutdown("gateway", func(result error) error {
		if err := gateway.Close(); err != nil {
			log.Printf("gateway close: %v", err)
		}
		return result
	})

	gateway.Start(ctx)

	wireV8RolloutObservability(busObservability, gateway.Bus, adapterClassifier)

	// P3 (post-Phase-C live validation 2026-05-08): when enabled,
	// plant the productids static seed entries into the registry at
	// startup. Surfaces Vaillant addresses that don't respond to
	// active scan (NETX3 0x04, SOL00 0xEC) so they're visible from
	// MCP/GraphQL/portal even before passive observation produces
	// corroborated evidence. Identity (Manufacturer, DeviceID) is
	// authoritative from the seed table; serial/sw/hw versions
	// remain empty until enrichment populates them.
	if cfg.EnableStaticSeedTable {
		applyStaticSeedTable(gateway.Registry)
	}

	builder := graphql.NewBuilder(gateway.Registry, nil)
	liveAdmittedEBusSource := newManagedEBusSourceProvider(ebusDriver, builder.AdmittedMutationSource)
	builder.SetAdmittedMutationSourceProvider(liveAdmittedEBusSource)

	// Phase A.5 runtime wire-up: AddressTable + AddressTableInserter consume
	// the PassiveTransactionReconstructor's classified events to insert
	// passively-observed addresses (e.g. NETX3 0xF6/0x04, SOL00 0xEC) into
	// the registry as passive_observed/corroborated. The inserter is
	// idle until subscribeAddressTableInserter binds it to the reconstructor.
	//
	// Inject a live AdmittedSource closure tied to builder.AdmittedMutationSource
	// — the same pattern PassiveDiscoveryPromoter uses below. This is critical
	// because cfg.ScanSource may be mutated by source-selection later in run();
	// a static cfg.ScanSource snapshot here would let the inserter mistake the
	// gateway's own admitted source for a third-party initiator after auto-
	// selection, corrupting passive_observed metadata. (Codex P2 from PR #565
	// review.)
	addressTableCfg := cfg
	addressTableCfg.AdmittedSource = func() byte {
		source, ok := builder.AdmittedMutationSource()
		if !ok {
			return 0
		}
		return source
	}
	addressTable := ebusgateway.NewAddressTable(gateway.Registry)
	addressTableInserter := ebusgateway.NewAddressTableInserter(addressTable, addressTableCfg)
	// M3 + M5 wiring: every passive observation flowing through the
	// inserter feeds the runtime-state Manager via
	// RefreshKnownBusMemberPresence — a presence-only refresh that
	// preserves Identity / CompanionAddr / Confidence on existing
	// entries (the directed-probe responder path is the only writer
	// that may upgrade Confidence to verified, and identity probes
	// are the only writer to Identity / CompanionAddr). Without this
	// wiring known_bus_members[] would stay empty after a fresh
	// install and M5 would have nothing to revalidate. (Codex P2
	// follow-up on PR #615.)
	addressTableInserter.SetRuntimeStateObserver(func(addr byte, observedAt time.Time, reportedSource string) {
		// Fresh passive evidence clears any M5 eviction blocklist
		// entry — the AD23 stale-ghost premise no longer holds when
		// the address is observed on the bus again. (Codex P2
		// follow-up on PR #615.)
		globalEvictionBlocklist.clear(addr)
		runtimeStateMgr.RefreshKnownBusMemberPresence(addr, observedAt, runtimestate.LastSource(reportedSource))
	})
	if busObservability != nil {
		builder.SetBusObservabilityProvider(newGraphQLBusObservabilityProvider(busObservability))
	}
	wireSourceSelectionBaselineEvidence(artifactBuilder, busObservability)
	builder.SetGatewayIdentityProvider(newRuntimeGatewayIdentityProvider(cfg))
	hub := graphql.NewBroadcastHub(nil)
	gateway.AddRouterPlane(hub)
	gateway.RefreshRouterPlanes()

	semanticRuntime := graphql.WireSemantic(builder, gateway.Router, hub)
	portalSemanticProvider := wireEEBusPromotedSemanticGraphQL(ctx, builder, semanticRuntime.Provider(), eebusAdapter)
	builder.SetStatusProvider(newRuntimeStatusProviderForControl(semanticRuntime.Provider(), liveAdmittedEBusSource, resolvedBuildInfo, daemonReleaseChecker.UpdatesAvailable, ebusAdapterLifecycleStatus(ebusDriver)))
	semanticRuntime.SetBootLiveTimeout(cfg.BootLiveTimeout)
	semanticRuntime.Start(ctx)
	wireSemanticStartupObservability(busObservability, semanticRuntime)
	// The control plane is process-owned, not eBUS-owned. Start it before
	// passive source-selection warmup so a slow or unavailable provider cannot
	// delay the stable HTTP/API shell. Mutable eBUS surfaces use late bindings:
	// they fail closed before source admission and delegate without rebuilding
	// the server once the healthy semantic poller is ready.
	lateScheduleWriter := &lateEBusScheduleWriter{}
	lateConfigWriter := &lateEBusConfigWriter{}
	lateWatchProvider := &lateEBusWatchSummaryProvider{}
	lateGraphQLWriter := &lateEBusGraphQLWriter{}
	lateGraphQLWatchProvider := &lateEBusGraphQLWatchSummaryProvider{}
	builder.SetBoilerConfigWriter(lateGraphQLWriter)
	builder.SetSystemConfigWriter(lateGraphQLWriter)
	builder.SetScheduleWriter(lateGraphQLWriter)
	builder.SetWatchSummaryProvider(lateGraphQLWatchProvider)
	if err := builder.Start(ctx); err != nil {
		return err
	}
	sourceSelectionAdmit := admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && !overrideSet
	// The consumers that live across eBUS generations have one owner; the
	// generation tasks below reach them only through its methods.
	retained, err := newRetainedEBusConsumers(ctx, retainedEBusConsumerOptions{
		Config:               cfg,
		SourceSelectionAdmit: sourceSelectionAdmit,
		LeafCaptureStateRoot: cfg.EEBusConfig.StateRoot,
		Gateway:              gateway,
		Builder:              builder,
		SemanticRuntime:      semanticRuntime,
		SemanticHub:          semanticHub,
		Hub:                  hub,
		Control:              control,
		AddressTableInserter: addressTableInserter,
		EEBusAdapter:         eebusAdapter,
		AdmittedSource:       liveAdmittedEBusSource,
		ScheduleWriter:       lateScheduleWriter,
		ConfigWriter:         lateConfigWriter,
		WatchProvider:        lateWatchProvider,
		GraphQLWriter:        lateGraphQLWriter,
		GraphQLWatchSource:   lateGraphQLWatchProvider,
		BusObservability:     busObservability,
		Deduplicator:         deduplicator,
	})
	if err != nil {
		return err
	}
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
	server, advertiser, err := startHTTPServerFn(
		controlPlaneCtx,
		cfg,
		gateway,
		builder,
		hub,
		portalSemanticProvider,
		eebusMCPProvider(eebusAdapter),
		eebusMCPCommandRouter(eebusAdapter),
		newManagedModbusMCPProvider(bindings, cfg.ModbusTCPConfig, teslaHSCRetained),
		portalCatalogSource,
		lateScheduleWriter,
		lateConfigWriter,
		busObservability,
		nil,
	)
	if err != nil {
		return err
	}
	consumerShutdown := gatewayConsumerShutdown{
		stopTransportWatch: stopTransportWatch,
		publishRetirement: func() {
			publishManagedTransportRetirement(busObservability, cfg)
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
	shell.onShutdownFunc("consumers", consumerShutdown.shutdown)

	// The control plane is already serving through stable fail-closed
	// bindings. Every post-start eBUS step (current-session source
	// validation and admission, the startup discovery scan, revalidation,
	// background scanning and passive promotion) runs as a task owned by the
	// full admitted generation correlation: once per admitted generation,
	// including a later START from STOPPED and a manager-driven replacement,
	// cancelled and joined before any successor. Process-scoped consumers
	// (semantic poller, writers, passive reconstructor, listener, address
	// inserter) are created once, by the first admitted generation through
	// their retained owner, and survive stopped periods with truthful
	// unavailability.
	//
	// The process-level barrier opens on process exit even when no
	// generation ever reported an admission outcome.
	retained.openSemanticBarrierOnProcessExit()

	// The eBUS generation owners hold the generation-owned post-start tasks.
	// They reuse the existing native selection, scan, revalidation, background
	// scan and promotion functions unchanged; every effect they install goes
	// through the generation scope, and every goroutine they start is joined
	// with the generation.
	generationOwners, err := newEBusGenerationOwners(ctx, ebusGenerationOwnerOptions{
		Config:              cfg,
		Selection:           startupSelection,
		RuntimeStateManager: runtimeStateMgr,
		RuntimeState:        runtimeState,
		Gateway:             gateway,
		Builder:             builder,
		AdapterClassifier:   adapterClassifier,
		Control:             control,
		Retained:            retained,
		BusObservability:    busObservability,
	})
	if err != nil {
		return err
	}
	generationTasks, err := newEBusGenerationTaskRunner(generationOwners.taskConfig())
	if err != nil {
		return fmt.Errorf("ebus generation tasks: %w", err)
	}
	// The generation tasks are fenced and joined before the shell and the
	// process consumers are torn down (this release runs first).
	shell.onShutdown("generation tasks", func(result error) error {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultEBusGenerationJoinTimeout+time.Second)
		defer cancel()
		if err := control.CloseAdmission(closeCtx); err != nil {
			log.Printf("runtime control close: %v", err)
		}
		return result
	})
	control.BindTasks(ctx, generationTasks, retained.withdrawAdmittedSource)
	<-ctx.Done()
	return nil
}

// runRuntimeStateRevalidatorLoop is the generation-owned form of the M5
// revalidator: the same native revalidation cycle, run in the caller's
// goroutine so the generation task runner joins it. The first cycle runs
// immediately; later cycles follow the interval until ctx ends.
func runRuntimeStateRevalidatorLoop(
	ctx context.Context,
	mgr *runtimestate.Manager,
	gw *ebusgateway.Gateway,
	cfg ebusgateway.Config,
	admittedSource byte,
	interval time.Duration,
) {
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	rotation := newRevalidationRotation()
	blocklist := globalEvictionBlocklist
	revalidateRuntimeStateMembers(ctx, mgr, gw, cfg, admittedSource, rotation, blocklist)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			revalidateRuntimeStateMembers(ctx, mgr, gw, cfg, admittedSource, rotation, blocklist)
		}
	}
}

// publishManagedTransportRetirement records final statuses after the managed
// transport observers have joined, before HTTP admission and observability close.
func publishManagedTransportRetirement(store *ebusgateway.BusObservabilityStore, cfg ebusgateway.Config) {
	if store == nil {
		return
	}
	if cfg.ModbusTCPConfig.Enabled {
		store.SetTransportRuntimeStatus(retiredModbusRuntimeTransportStatus())
	}
	if cfg.EEBusConfig.Enabled {
		// Observers have joined and this store is about to close. A terminal
		// revision supersedes every status published during its lifetime.
		store.SetTransportRuntimeStatus(eebusRuntimeTransportStatus(eebusRuntimeLifecycleSnapshot{State: eebusLifecycleStopped, Revision: ^uint64(0)}))
	}
}
