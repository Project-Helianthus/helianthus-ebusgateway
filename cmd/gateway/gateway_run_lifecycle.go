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
	// Registered first, so it runs last: no error leaving the process carries
	// an endpoint of the configuration v2 document.
	defer func() {
		result = redactFileSourcedModbusEndpoints(result, modbusTCPDocumentEndpoints(cfg.ModbusTCPConfig)...)
	}()
	resolvedBuildInfo, err := prepareGatewayRunConfig(&cfg)
	if err != nil {
		return err
	}
	// Every static semantic identity is declared before any runtime starts; a
	// cross-family collision stops the process here, before any side effect.
	semanticHub, err := preflightSemanticHub(cfg)
	if err != nil {
		return err
	}
	// Release discovery is a public, read-only background task. Both status
	// APIs receive the same cache-only reader below; their request handlers do
	// not trigger or wait for this network operation.
	daemonReleaseChecker := releasecheck.New(releasecheck.Options{CurrentVersion: resolvedBuildInfo.ReleaseVersion})
	if cfg.HTTPAddr != "" {
		startGatewayReleaseChecker(daemonReleaseChecker, ctx)
	}

	evidenceRuntime, err := openSynchronizedEvidenceRuntime(cfg.EvidenceRecorderConfig)
	if err != nil {
		return fmt.Errorf("synchronized evidence sidecar: %w", err)
	}
	if evidenceRuntime != nil {
		defer func() {
			if err := evidenceRuntime.Close(); err != nil {
				result = errors.Join(result, fmt.Errorf("shutdown synchronized evidence sidecar: %w", err))
			}
		}()
	}
	portalContributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		return fmt.Errorf("create portal contribution registry: %w", err)
	}
	// Initialize the runtime-state Manager early so the cached
	// ebus.self.last_admitted_source can be passed as a hint to the
	// SourceAddressSelector below, and the Manager is available for
	// post-admission UpdateSelf and address-table-revalidate write-back.
	// Errors during Load are tolerated — Manager.Load returns an empty
	// state on missing/corrupt and the gateway continues without a hint.
	// (runtime-state-w19-26.locked M2_GATEWAY_LOADER + M4_SOURCE_SELECTION_HINT)
	runtimeStateMgr, runtimeState := initRuntimeStateManager(ctx, cfg, resolvedBuildInfo)
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtimeStateMgr.Stop(stopCtx); err != nil {
			log.Printf("runtime_state stop: %v", err)
		}
	}()

	// Load the Portal desired record before any driver is registered, so its
	// accepted driver selections become configured eligibility. A record that
	// does not load keeps the shell up and withholds record-controlled drivers.
	portalConfig := openGatewayPortalConfigRuntime(cfg)
	defer func() {
		if err := portalConfig.Close(); err != nil {
			log.Printf("portal configuration close: %v", err)
		}
	}()
	// Earlier builds created the eeBUS operator MCP socket inside the eeBUS
	// state root, whose store rejects any entry it does not own. Clear that
	// one stale socket before any eeBUS resource opens.
	clearLegacyEEBusOperatorSocket(eebusEnabledStateRoot(cfg))
	// Construct the protocol-neutral manager and stable eBUS provider before
	// opening any eBUS resource. Driver-local construction failure is retained
	// as categorical state and never promoted to process failure.
	sidecars := prepareManagedSidecars(ctx, cfg, semanticHub, runtimeStateMgr.State().Meta.InstanceGUID, portalContributions, portalConfig)
	bindings := &managedSidecarBindings{sidecars: sidecars}
	ebusDriver, err := prepareGatewayEBusDriver(&cfg, sidecars.driverConfigs()...)
	if err != nil {
		return err
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*defaultEBusDriverDrainTimeout+time.Second)
		defer cancel()
		if err := errors.Join(ebusDriver.Shutdown(stopCtx), sidecars.shutdownError()); err != nil {
			result = errors.Join(result, fmt.Errorf("shutdown eBUS driver: %w", err))
		}
	}()
	adapterClassifier := ebusDriver.classifier
	// Compose the frozen registration and the durable control service
	// before eBUS reconciliation. The service is the single owner of eBUS
	// startup: it restores D5 material, prepares the inert source and then
	// reconciles durable desired state through the manager-to-hub bridge.
	// Nothing else starts the DriverManager, and no failure here is a
	// process error.
	control := startGatewayRuntimeControl(ctx, gatewayRuntimeControlOptionsFor(gatewayRuntimeControlOptions{
		Sidecars:     sidecars,
		Driver:       ebusDriver,
		Hub:          semanticHub,
		Store:        runtimeStateMgr,
		InstanceGUID: runtimeStateMgr.State().Meta.InstanceGUID,
		PortalConfig: portalConfig,
	}))
	gatewayRuntimeControlStarted(control)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultEBusGenerationJoinTimeout+defaultEBusControlCleanupTimeout)
		defer cancel()
		if err := control.CloseAdmission(closeCtx); err != nil {
			log.Printf("runtime control close: %v", err)
		}
		if err := control.CloseNative(closeCtx); err != nil {
			log.Printf("runtime control native close: %v", err)
		}
	}()
	if service := control.Service(); service == nil {
		log.Printf("eBUS control service unavailable; continuing without eBUS")
	} else if view, err := service.Get(primaryEBusDriverID); err == nil && view.ObservedState != "RUNNING" {
		log.Printf("eBUS driver not running at startup; continuing desired=%s state=%s reason=%s", view.DesiredState, view.ObservedState, view.Reason)
	}

	teslaHSCRetained, err := startTeslaHSCRetainedOwner(cfg.ModbusTCPConfig.TeslaGen3HSC, semanticHub)
	if err != nil {
		return fmt.Errorf("tesla HSC retained owner: %w", err)
	}
	if teslaHSCRetained != nil {
		if err := attachGatewayPortalEVSEContribution(teslaHSCRetained, portalContributions); err != nil {
			_ = teslaHSCRetained.Close()
			return fmt.Errorf("publish EVSE portal contribution: %w", err)
		}
		defer func() {
			if err := teslaHSCRetained.Close(); err != nil {
				result = errors.Join(result, fmt.Errorf("shutdown Tesla HSC retained owner: %w", err))
			}
		}()
	}
	portalCatalogSource := newGatewayPortalCatalogSource(nil, nil, teslaHSCRetained, portalContributions, bindings)
	m2mRuntime, err := newProfileM2MRuntime(cfg, teslaHSCRetained, portalCatalogSource, bindings)
	if err != nil {
		return fmt.Errorf("M2M GraphQL sidecar: %w", err)
	}
	if m2mRuntime != nil {
		defer func() {
			if err := m2mRuntime.Close(); err != nil {
				result = errors.Join(result, fmt.Errorf("shutdown M2M GraphQL sidecar: %w", err))
			}
		}()
	}

	eebusAdapter := &eebusRuntimeAdapter{managed: bindings}
	eebusAdminHandler := http.HandlerFunc(bindings.serveEEBusAdmin)
	if evidenceRuntime != nil {
		if err := evidenceRuntime.Configure(bindings.eebusEvidenceCapture(), resolvedBuildInfo.EvidenceVersion(), newSynchronizedEvidenceClock(), synchronizedEvidenceEntropy); err != nil {
			return fmt.Errorf("configure synchronized evidence sidecar: %w", err)
		}
	}

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
	defer startupSelection.emitFinalArtifact()

	busObservability, deduplicator, err := wireObserveFirstObserversFn(&cfg)
	if err != nil {
		return err
	}
	stopTransportWatch := func() {}
	if busObservability != nil {
		stopTransportWatch = bindings.observeTransports(ctx, cfg, busObservability)
		defer stopTransportWatch()
	}
	wireSemanticMetricsProvider(busObservability, cfg, bindings, teslaHSCRetained)

	wireAdaptermuxDiagnosticProviders(busObservability, adapterClassifier)

	gateway, err := ebusgateway.New(ctx, cfg)
	if err != nil {
		return err
	}

	defer func() {
		if err := gateway.Close(); err != nil {
			log.Printf("gateway close: %v", err)
		}
	}()

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
	defer controlPlaneCancel()
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
	defer consumerShutdown.shutdown()

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
	// process consumers are torn down (this defer runs first).
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultEBusGenerationJoinTimeout+time.Second)
		defer cancel()
		if err := control.CloseAdmission(closeCtx); err != nil {
			log.Printf("runtime control close: %v", err)
		}
	}()
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
