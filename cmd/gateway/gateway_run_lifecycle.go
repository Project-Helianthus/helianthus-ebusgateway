package main

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/releasecheck"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// startGatewayReleaseChecker starts the cache-only release discovery task.
var startGatewayReleaseChecker = func(checker *releasecheck.Checker, ctx context.Context) {
	checker.Start(ctx)
}

func runGatewayLifecycle(ctx context.Context, cfg ebusgateway.Config) (result error) {
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

	// Construct the protocol-neutral manager and stable eBUS provider before
	// opening any eBUS resource. Driver-local construction failure is retained
	// as categorical state and never promoted to process failure.
	sidecars := prepareManagedSidecars(ctx, cfg, semanticHub, runtimeStateMgr.State().Meta.InstanceGUID, portalContributions)
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
	matterFeedProvider, err := newGatewayMatterBindingFeedProvider(portalCatalogSource, cfg.M2MGraphQL.AllowedAssets)
	if err != nil {
		return fmt.Errorf("matter binding feed provider: %w", err)
	}
	m2mRuntime, err := newM2MGraphQLRuntimeWithMatter(cfg, nil, nil, teslaHSCRetained, matterFeedProvider, bindings)
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

	// ResolveAdmissionPath centralises the adapter-direct -> source-selection
	// special case so all classifier call sites (main.go run(),
	// startup_scan.go startDiscoveryScanLoop, runBackgroundFullScan) agree
	// on the dispatch. Adapter-direct multiplexer mode always wraps a
	// source-selection-capable underlying transport (ENH/ENS), so source-address selector runs through
	// the shared PassiveTransactionReconstructor regardless of multiplexer
	// presence. Resolves cruise-run #20 M7 follow-up #1 + validation
	// finding (source_selection.mode was incorrectly degraded_no_events
	// on adapter-direct deployments).
	admissionPath, adapterDirectSpecialCased := ebusgateway.ResolveAdmissionPath(cfg.TransportConfig.Protocol)
	if adapterDirectSpecialCased {
		log.Printf("startup source selection: adapter-direct multiplexer detected; treating as source-selection-capable (underlying transport is always ENH/ENS)")
	} else if admissionPath == ebusgateway.TransportAdmissionStaticFallback && cfg.TransportConfig.Protocol != ebusgateway.TransportEbusdTCP {
		log.Printf("startup source selection: classifier produced static-fallback for transport protocol=%q (unknown/empty)", cfg.TransportConfig.Protocol)
	}
	metrics := ebusgateway.GetOrInitStartupSourceSelectionMetrics()
	artifactBuilder := ebusgateway.NewSourceSelectionArtifactBuilder(string(cfg.TransportConfig.Protocol))
	if err := artifactBuilder.SetSourceSelectionMode("degraded_no_events"); err != nil {
		return fmt.Errorf("set startup source-selection mode: %w", err)
	}
	overrideSet := admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && cfg.StartupSource.Source != nil
	overrideSource := byte(0x00)
	if overrideSet {
		overrideSource = *cfg.StartupSource.Source
		log.Print(ebusgateway.FormatStartupSourceSelectionExplicitLog(overrideSource))
		metrics.SetExplicitSourceActive(true)
		metrics.RecordExplicitValidateOnly()
		artifactBuilder.SetExplicitSource(overrideSource)
		if err := artifactBuilder.SetSourceSelectionMode("explicit_validate_only"); err != nil {
			return fmt.Errorf("set explicit source-selection mode: %w", err)
		}
		// Exact source is configured: state immediately becomes "active" because
		// the explicit source is in use from the first active frame. The selector
		// may still run advisory-only under Validate=true but does not gate.
		artifactBuilder.SetActiveExplicitSource(overrideSource)
	} else {
		metrics.SetExplicitSourceActive(false)
	}
	const artifactPath = "/tmp/helianthus-source-selection-artifact.json"
	go func() {
		timer := time.NewTimer(60 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := artifactBuilder.EmitToFile(artifactPath); err != nil {
				log.Printf("startup source selection artifact emit: %v", err)
			}
		}
	}()
	defer func() {
		if err := artifactBuilder.EmitToFile(artifactPath); err != nil {
			log.Printf("startup source selection artifact emit: %v", err)
		}
	}()

	busObservability, deduplicator, err := wireObserveFirstObserversFn(&cfg)
	if err != nil {
		return err
	}
	stopTransportWatch := func() {}
	if busObservability != nil {
		stopTransportWatch = bindings.observeTransports(ctx, cfg, busObservability)
		defer stopTransportWatch()
	}
	if busObservability != nil && (cfg.ModbusTCPConfig.Enabled || cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled || cfg.PrometheusEVSEEnabled) {
		// This is intentionally wired before the HTTP control plane starts. The
		// callback reaches only immutable SemReg current views; it cannot invoke
		// the Growatt observe/publish path or any native transport operation.
		busObservability.SetSemanticMetricsProvider(func(at time.Time) []ebusgateway.SemanticMetricsDomain {
			// The Tesla owner supplies only its detached retained publication. It
			// cannot acquire, publish, or execute a native operation from a scrape.
			return bindings.semanticMetrics(cfg, teslaHSCRetained, at)
		})
	}

	// batch-21 forensic counters — wire the adaptermux diagnostic
	// snapshot through the BusObservabilityStore Prometheus surface.
	// Type-assert through the optional adaptermuxDiagSnapshotter seam
	// (satisfied by *adaptermux.Mux). When the active transport isn't
	// adapter-direct (no mux), classifier is nil and the provider
	// stays unset — Prometheus surface degrades cleanly (the
	// ebus_adaptermux_syn_seen_* metrics are simply absent).
	if snap, ok := adapterClassifier.(adaptermuxDiagSnapshotter); ok {
		busObservability.SetAdaptermuxDiagProvider(func() ebusgateway.AdaptermuxDiagSnapshot {
			s := snap.ActiveTxnSnapshot()
			return ebusgateway.AdaptermuxDiagSnapshot{
				SynSuppressedPreEcho:               s.SynSuppressedPreEcho,
				SynSeenDuringGrantWindow:           s.SynSeenDuringGrantWindow,
				SynSeenWhileInterWriteEmpty:        s.SynSeenWhileInterWriteEmpty,
				SynSeenAfterTransportWindowExpired: s.SynSeenAfterTransportWindowExpired,
				SynSuppressedBetweenWrites:         s.SynSuppressedBetweenWrites,
			}
		})
	}

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

	// v8 frame-atomic-visibility rollout counters — wire the four
	// protocol.Bus counters (round-9 + payload_aa_*) and the v8
	// classifier shadow-mode counter into the BusObservabilityStore
	// Prometheus surface. When the active transport isn't adapter-
	// direct (no mux, no classifier) we still publish the four bus
	// counters so HelianthusRound9FiredUnderProxy can fire on any
	// transport. The v8 classifier counter degrades to 0 when the
	// classifier is nil (Classifier.ShadowWouldHaveDroppedTotal
	// has nil-receiver handling — see
	// internal/adaptermux/v8classifier/classifier.go:848).
	if busObservability != nil && gateway.Bus != nil {
		bus := gateway.Bus
		withClassifier := func(func(*v8classifier.Classifier)) {}
		if provider, ok := adapterClassifier.(interface {
			WithV8Classifier(func(*v8classifier.Classifier))
		}); ok {
			withClassifier = provider.WithV8Classifier
		}
		shadowDropCountFn := func() uint64 {
			var total uint64
			withClassifier(func(classifier *v8classifier.Classifier) {
				total = classifier.ShadowWouldHaveDroppedTotal()
			})
			return total
		}
		if provider, ok := adapterClassifier.(interface {
			V8ShadowWouldHaveDroppedTotal() uint64
		}); ok {
			shadowDropCountFn = provider.V8ShadowWouldHaveDroppedTotal
		}
		busObservability.SetV8RolloutProvider(func() ebusgateway.V8RolloutSnapshot {
			return ebusgateway.V8RolloutSnapshot{
				Round9AbsorbEntered:            bus.Round9AbsorbEntered(),
				PayloadAaAutoSynAbsorbed:       bus.PayloadAaAutoSynAbsorbed(),
				PayloadAaAutoSynRecovered:      bus.PayloadAaAutoSynRecovered(),
				PayloadAaAutoSynDrainExhausted: bus.PayloadAaAutoSynDrainExhausted(),
				V8ShadowWouldHaveDroppedTotal:  shadowDropCountFn(),
			}
		})

		// Mirror the same counters on /debug/vars (expvar) so
		// operators can read them with curl + jq without
		// scraping the Prometheus surface. expvar.Func re-reads
		// the underlying atomic on every scrape — keeps the two
		// surfaces in lock-step. Names match the Prometheus
		// metric names 1:1 so dashboards built on either
		// transport produce identical numbers.
		//
		// Stale-closure defense (PR #655 round-1, Codex MAJOR):
		// the expvar.Publish calls themselves run ONCE per process
		// (publish panics on duplicate names), but the closures
		// dereference an atomic.Pointer that run() updates on each
		// invocation. The test harness re-entering run() with a
		// fresh gateway therefore swaps the pointer and /debug/vars
		// immediately starts reading the new bus — no surface
		// inconsistency between /metrics and /debug/vars.
		v8RolloutExpvarCurrent.Store(&v8RolloutExpvarSource{
			bus:               bus,
			shadowDropCountFn: shadowDropCountFn,
		})
		// F-NEW-26: install the current-generation classifier callback for
		// /debug/v8/admin-events. Production operations execute inside stable
		// driver admission, so a classifier pointer cannot escape across
		// replacement. A missing classifier (non-adapter-direct transport)
		// leaves the response empty while preserving the HTTP contract.
		v8AdminEventsCurrentClassifier.Store(nil)
		v8AdminEventsCurrentProvider.Store(&v8AdminClassifierProvider{withClassifier: withClassifier})
		v8RolloutExpvarPublishOnce.Do(func() {
			expvar.Publish("helianthus_round9_absorb_entered_total",
				expvar.Func(func() any {
					if src := v8RolloutExpvarCurrent.Load(); src != nil {
						return src.bus.Round9AbsorbEntered()
					}
					return uint64(0)
				}))
			expvar.Publish("helianthus_payload_aa_auto_syn_absorbed_total",
				expvar.Func(func() any {
					if src := v8RolloutExpvarCurrent.Load(); src != nil {
						return src.bus.PayloadAaAutoSynAbsorbed()
					}
					return uint64(0)
				}))
			expvar.Publish("helianthus_payload_aa_auto_syn_recovered_total",
				expvar.Func(func() any {
					if src := v8RolloutExpvarCurrent.Load(); src != nil {
						return src.bus.PayloadAaAutoSynRecovered()
					}
					return uint64(0)
				}))
			expvar.Publish("helianthus_payload_aa_auto_syn_drain_exhausted_total",
				expvar.Func(func() any {
					if src := v8RolloutExpvarCurrent.Load(); src != nil {
						return src.bus.PayloadAaAutoSynDrainExhausted()
					}
					return uint64(0)
				}))
			expvar.Publish("helianthus_v8_shadow_would_have_dropped_total",
				expvar.Func(func() any {
					if src := v8RolloutExpvarCurrent.Load(); src != nil {
						// Non-adapter-direct transports install no counter callback.
						if src.shadowDropCountFn == nil {
							return uint64(0)
						}
						return src.shadowDropCountFn()
					}
					return uint64(0)
				}))
		})
	}

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
	// Wire baseline evidence provider so the artifact emitter populates
	// per_baseline_address_evidence_counts at emit time. Reads the bus
	// observability periodicity snapshot, filters to baseline addresses
	// (Vaillant default), and counts cumulative observations per address.
	// Resolves cruise-run #20 validation finding: artifact's per-baseline
	// map was always empty even when the registry observed traffic to
	// baseline addresses.
	if busObservability != nil {
		artifactBuilder.SetBaselineEvidenceProvider(func() map[string]int {
			counts := make(map[string]int)
			for _, addr := range ebusgateway.VaillantBaselineTopologySeed {
				counts[fmt.Sprintf("%02X", addr)] = 0
			}
			// RecentMessages returns most recent observations; iterate
			// and count per baseline address (as either source or
			// target). 1024 is the default observe-first capacity, large
			// enough to capture passive traffic to all baselines on a
			// healthy bus.
			for _, msg := range busObservability.RecentMessages(1024) {
				srcKey := fmt.Sprintf("%02X", msg.Source)
				if _, ok := counts[srcKey]; ok {
					counts[srcKey]++
				}
				tgtKey := fmt.Sprintf("%02X", msg.Target)
				if _, ok := counts[tgtKey]; ok {
					counts[tgtKey]++
				}
			}
			return counts
		})
	}
	builder.SetGatewayIdentityProvider(newRuntimeGatewayIdentityProvider(cfg))
	hub := graphql.NewBroadcastHub(nil)
	gateway.AddRouterPlane(hub)
	gateway.RefreshRouterPlanes()

	semanticRuntime := graphql.WireSemantic(builder, gateway.Router, hub)
	portalSemanticProvider := wireEEBusPromotedSemanticGraphQL(ctx, builder, semanticRuntime.Provider(), eebusAdapter)
	builder.SetStatusProvider(newRuntimeStatusProviderForControl(semanticRuntime.Provider(), liveAdmittedEBusSource, resolvedBuildInfo, daemonReleaseChecker.UpdatesAvailable, ebusAdapterLifecycleStatus(ebusDriver)))
	semanticRuntime.SetBootLiveTimeout(cfg.BootLiveTimeout)
	semanticRuntime.Start(ctx)
	if busObservability != nil && semanticRuntime.Provider() != nil {
		busObservability.SetStartupSurfaceProvider(func() *ebusgateway.BusObservabilityStartup {
			startupUpdatedAt := semanticRuntime.Provider().StartupUpdatedAt()
			cacheEpoch, liveEpoch := semanticRuntime.Provider().StartupEpochs()
			return &ebusgateway.BusObservabilityStartup{
				LastUpdatedAt: &startupUpdatedAt,
				Phase:         string(semanticRuntime.Provider().StartupPhase()),
				CacheEpoch:    cacheEpoch,
				LiveEpoch:     liveEpoch,
			}
		})
		semanticRuntime.Provider().SetEnergyPassiveStateProvider(func() string {
			snapshot := busObservability.Snapshot()
			return snapshot.Summary.Status.Capability.PassiveState
		})
		busObservability.SetEnergyFreshnessMetricsRefresher(func(now time.Time, passiveState string) {
			semanticRuntime.Provider().RefreshEnergyFreshnessMetrics(now, passiveState)
		})
	}
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
	var (
		// surfaceMu guards the process-scoped passive consumers, which the
		// first admitted generation's task creates and teardown closes.
		surfaceMu     sync.Mutex
		listener      *ebusgateway.BroadcastListener
		reconstructor *ebusgateway.PassiveTransactionReconstructor
	)
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
	defer func() {
		// After HTTP admission has stopped and active requests have drained,
		// preserve the historical native-consumer cleanup order.
		surfaceMu.Lock()
		listener, reconstructor := listener, reconstructor
		surfaceMu.Unlock()
		if listener != nil {
			if err := listener.Close(); err != nil {
				log.Printf("broadcast listener close: %v", err)
			}
		}
		if deduplicator != nil {
			if err := deduplicator.Close(); err != nil {
				log.Printf("deduplicator close: %v", err)
			}
		}
		if reconstructor != nil {
			if err := reconstructor.Close(); err != nil {
				log.Printf("reconstructor close: %v", err)
			}
		}
		if busObservability != nil {
			if err := busObservability.Close(); err != nil {
				log.Printf("bus observability close: %v", err)
			}
		}
		if advertiser != nil {
			if err := advertiser.Close(); err != nil {
				log.Printf("mdns close: %v", err)
			}
		}
	}()
	defer func() {
		// This defer is registered after control-plane teardown, so LIFO makes
		// the final bounded runtime statuses observable before HTTP immediately
		// stops admitting new work and drains already-admitted requests.
		stopTransportWatch()
		publishManagedTransportRetirement(busObservability, cfg)
		if server != nil {
			if err := shutdownHTTPControlPlane(server); err != nil {
				log.Printf("http server shutdown: %v", err)
			}
		}
	}()

	// The control plane is already serving through stable fail-closed
	// bindings. Every post-start eBUS step (current-session source
	// validation and admission, the startup discovery scan, revalidation,
	// background scanning and passive promotion) runs as a task owned by the
	// full admitted generation correlation: once per admitted generation,
	// including a later START from STOPPED and a manager-driven replacement,
	// cancelled and joined before any successor. Process-scoped consumers
	// (semantic poller, writers, passive reconstructor, listener, address
	// inserter) are created once, by the first admitted generation, and
	// survive stopped periods with truthful unavailability.
	var (
		surfaceReady         bool
		surfacePollerStarted bool
		surfaceCaptureDone   bool
		surfaceShadowDone    bool
		semanticPoller       *vaillantSemanticPoller
		promoter             *ebusgateway.PassiveDiscoveryPromoter
		probeReady           atomic.Bool
		semanticBarrier      chan struct{}
		semanticBarrierOnce  sync.Once
		passiveAttached      bool
		insertSubscribed     atomic.Bool
		admittedSourceMu     sync.Mutex
		observeFirstFlags    = ebusgateway.NormalizeObserveFirstFeatureFlags(cfg.ObserveFirstEnabled, cfg.PassiveStateDirectApply, cfg.PassiveConfigDirectApply, cfg.ExternalWritePolicy)
		sourceSelectionAdmit = admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && !overrideSet
	)
	var insertSubscribeOnce sync.Once
	if cfg.ScanOnStart && (shouldStartPassiveObserveFirst(cfg) || sourceSelectionAdmit) {
		semanticBarrier = make(chan struct{})
	}
	closeSemanticBarrier := func() {
		if semanticBarrier != nil {
			semanticBarrierOnce.Do(func() { close(semanticBarrier) })
		}
	}
	// The process-level barrier opens on process exit even when no
	// generation ever reported an admission outcome.
	go func() {
		<-ctx.Done()
		closeSemanticBarrier()
	}()
	currentReconstructor := func() *ebusgateway.PassiveTransactionReconstructor {
		surfaceMu.Lock()
		defer surfaceMu.Unlock()
		return reconstructor
	}

	// subscribeAddressTableInserter binds the AddressTableInserter to the
	// active reconstructor at most once per process. Subscription is gated
	// on the admitted source being finalized (builder.AdmittedMutationSource
	// returns ok=true) so the inserter's self-source filter sees the real
	// admitted address — never 0 — and cannot mistakenly insert the
	// gateway's own active-probe targets/companions as passive_observed.
	// Subscription failure is non-fatal; the inserter is a non-critical
	// observer.
	subscribeAddressTableInserter := func() {
		active := currentReconstructor()
		if insertSubscribed.Load() || active == nil {
			return
		}
		if _, ok := builder.AdmittedMutationSource(); !ok {
			return
		}
		insertSubscribeOnce.Do(func() {
			sub, err := active.Subscribe(
				"address_table_inserter",
				ebusgateway.PassiveSubscriberNonCritical,
				0,
			)
			if err != nil {
				log.Printf("address_table_inserter subscribe failed: %v", err)
				return
			}
			insertSubscribed.Store(true)
			go func() {
				for ev := range sub.Events() {
					addressTableInserter.OnPassiveClassifiedEvent(ev)
				}
			}()
		})
	}
	attachPassiveObserveFirst := func() error {
		active := currentReconstructor()
		if active == nil {
			return nil
		}
		surfaceMu.Lock()
		attached := passiveAttached
		surfaceMu.Unlock()
		if !attached {
			if busObservability != nil {
				if err := busObservability.AttachReconstructor(ctx, active); err != nil {
					return err
				}
			}
			if deduplicator != nil {
				if err := deduplicator.AttachReconstructor(ctx, active); err != nil {
					return err
				}
			}
			surfaceMu.Lock()
			passiveAttached = true
			surfaceMu.Unlock()
		}
		surfaceMu.Lock()
		needListener := listener == nil
		surfaceMu.Unlock()
		if needListener {
			started, err := startBroadcastListenerWithReconstructorFn(ctx, gateway.Router, active)
			if err != nil {
				return err
			}
			surfaceMu.Lock()
			listener = started
			surfaceMu.Unlock()
		}
		// Best-effort early bind. If admission isn't finalized yet
		// (sourceSelection path: waits on activeProbePassed), this
		// returns silently and the wire-up below catches the signal.
		subscribeAddressTableInserter()
		return nil
	}
	// installAdmittedSource and clearAdmittedSource serialize the
	// process-level admitted source with generation admission: a fenced
	// generation can never install it (scope.Commit), and a clear after the
	// lifecycle withdrew admission rechecks that no successor installed one.
	installAdmittedSource := func(scope *ebusGenerationScope, source byte) bool {
		return scope.Commit(func() {
			admittedSourceMu.Lock()
			builder.SetAdmittedMutationSource(source)
			admittedSourceMu.Unlock()
		})
	}
	clearAdmittedSource := func(scope *ebusGenerationScope) bool {
		return scope.Commit(func() {
			admittedSourceMu.Lock()
			builder.ClearAdmittedMutationSource()
			admittedSourceMu.Unlock()
		})
	}

	// ensureProcessSurface creates the process-scoped eBUS consumers once,
	// from the first admitted generation's validated configuration. They use
	// the process context, so a stopped interval never tears them down.
	ensureProcessSurface := func(genCfg ebusgateway.Config) error {
		surfaceMu.Lock()
		ready, pollerStarted, poller := surfaceReady, surfacePollerStarted, semanticPoller
		surfaceMu.Unlock()
		// Every admitted generation rebinds the retained poller to its own
		// initiator and companion.
		bindPollerSource := func(poller *vaillantSemanticPoller) {
			if poller == nil {
				return
			}
			poller.attachBoilerThermal(semanticHub, func() (*semhub.Handle, error) {
				if control.bridge == nil {
					return nil, errEBusControlNotPrepared
				}
				view := control.bridge.View()
				return control.bridge.PublicationHandle(view.Key)
			})
			resolved := resolveStartupScanSourceConfig(genCfg)
			poller.setSourceAndCompanion(resolved.ScanSource, resolved.StartupCompanionTarget)
		}
		bindPollerSource(poller)
		if ready {
			return nil
		}
		// Readiness is recorded only after every fallible step succeeded; a
		// failed step is retried by the next generation, and the stages that
		// already completed are not repeated.
		if !pollerStarted {
			semanticCfg := resolveStartupScanSourceConfig(genCfg)
			poller = startVaillantSemanticPollingFn(ctx, semanticCfg, gateway, semanticRuntime.Provider(), hub, semanticBarrier)
			surfaceMu.Lock()
			semanticPoller, surfacePollerStarted = poller, true
			surfaceMu.Unlock()
			bindPollerSource(poller)
		}
		if !pollerStarted && poller != nil {
			gatedGraphQLWriter := admittedGraphQLSemanticWriter{
				boiler:   poller,
				system:   poller,
				schedule: poller,
				admitted: liveAdmittedEBusSource,
			}
			lateGraphQLWriter.Bind(gatedGraphQLWriter)
			lateGraphQLWatchProvider.Bind(newGraphQLWatchSummaryProvider(poller.shadow))
		}
		// Bound before any fallible step so a retried generation cannot
		// leave the deduplicator as its own observer.
		if !pollerStarted && poller != nil && poller.shadow != nil && deduplicator != nil {
			// Use the semantic shadow directly here. The broader runtime
			// observer can fall back to the deduplicator itself, which would
			// re-enter dedup locks while passive fingerprints are being built.
			deduplicator.SetWatchObserver(poller.shadow)
		}
		// Install the shell capture once; its binding checks admission per call.
		if poller != nil && !surfaceCaptureDone {
			captureSource := newLeafPromotionLiveSource(
				poller,
				eebusAdapter,
				eebusMCPCommandRouter(eebusAdapter),
				liveAdmittedEBusSource,
			)
			eebusAdapter.SetLeafPromotionCapture(&lazyLeafPromotionCapture{
				construct: func() (mcp.LeafPromotionCapture, error) {
					return newLeafPromotionCaptureRuntime(cfg.EEBusConfig.StateRoot, captureSource)
				},
			})
			surfaceCaptureDone = true
		}
		passiveShadowLaneEnabled := observeFirstFlags.PassiveStateDirectApply() ||
			observeFirstFlags.PassiveConfigDirectApply() ||
			observeFirstFlags.ExternalWritePolicy() != ebusgateway.ObserveFirstExternalWritePolicyRecordOnly
		if poller != nil && deduplicator != nil && observeFirstFlags.ObserveFirstEnabled() && passiveShadowLaneEnabled && !surfaceShadowDone {
			if err := attachPassiveShadowProducerFn(poller, ctx, deduplicator); err != nil {
				return err
			}
			surfaceShadowDone = true
		}
		// Runtime passive-promotion pipeline: late-arriving devices (e.g.
		// the regulator boots after the gateway) accumulate passive evidence
		// in busObservability's EvidenceBuffer; the promoter confirms
		// candidates with B524 coherency, registers them in the registry,
		// refreshes router planes, and signals semantic discovery refresh.
		// The promoter is process-scoped; its loop runs per generation.
		if busObservability != nil && busObservability.EvidenceBuffer() != nil && poller != nil {
			busObservability.SetAdmittedSourceProvider(func() byte {
				source, ok := builder.AdmittedMutationSource()
				if !ok {
					return 0
				}
				return source
			})
			created, err := ebusgateway.NewPassiveDiscoveryPromoter(ebusgateway.PassiveDiscoveryPromoterOptions{
				Registry:          gateway.Registry,
				EvidenceBuffer:    busObservability.EvidenceBuffer(),
				ConfirmFn:         poller.ConfirmB524Coherent,
				SemanticRefreshFn: poller.EnqueueDiscoveryRefresh,
				RouterRefreshFn:   func() { _ = gateway.RefreshRouterPlanes() },
				AdmittedSourceFn: func() byte {
					source, ok := builder.AdmittedMutationSource()
					if !ok {
						return 0
					}
					return source
				},
			})
			if err != nil {
				log.Printf("passive_discovery_promoter init failed: %v", err)
			} else {
				surfaceMu.Lock()
				promoter = created
				surfaceMu.Unlock()
			}
		}
		// M6 enrichment trigger and P5 per-address identity probe, gated on
		// the startup admission barrier so no ScanDirected traffic races the
		// startup directed scan; the post-barrier backfill catches up.
		if poller != nil {
			addressTableInserter.SetEnrichmentRefreshFn(poller.EnqueueDiscoveryRefresh)
			if semanticBarrier == nil {
				probeReady.Store(true)
			}
			addressTableInserter.SetEnrichmentIdentityProbeFn(func(addr byte) {
				if !probeReady.Load() {
					return
				}
				poller.EnqueueAddressIdentityProbe(addr)
			})
			if semanticBarrier != nil {
				go func() {
					select {
					case <-ctx.Done():
						return
					case <-semanticBarrier:
						probeReady.Store(true)
						addressTableInserter.BackfillUnidentifiedAddresses()
					}
				}()
			} else {
				addressTableInserter.BackfillUnidentifiedAddresses()
			}
			lateScheduleWriter.Bind(admittedMCPScheduleWriter{
				writer:   poller,
				admitted: liveAdmittedEBusSource,
			})
			lateConfigWriter.Bind(admittedMCPConfigWriter{
				writer:   &mcpConfigWriterAdapter{poller: poller},
				admitted: liveAdmittedEBusSource,
			})
			lateWatchProvider.Bind(newMCPWatchSummaryProvider(poller.shadow))
		}
		if cfg.BroadcastListen && !shouldStartPassiveObserveFirst(cfg) {
			log.Printf("passive observe-first unavailable on transport=%s; continuing degraded", cfg.TransportConfig.Protocol)
		}
		surfaceMu.Lock()
		surfaceReady = true
		surfaceMu.Unlock()
		return nil
	}

	// runEBusGenerationStartup is the generation-owned post-start task. It
	// reuses the existing native selection, scan, revalidation, background
	// scan and promotion functions unchanged; every effect it installs goes
	// through the generation scope, and every goroutine it starts is joined
	// with the generation.
	// Per-generation hand-off between the ordered startup tasks. Only the
	// newest admitted generation keeps an entry.
	type ebusStartupState struct {
		cfg               ebusgateway.Config
		selection         *protocol.SourceAddressSelection
		staticSource      byte
		static            bool
		signals           startupScanSignals
		admitted, scanned bool
	}
	var startupStateMu sync.Mutex
	startupStates := map[runtimecontrol.NativeGeneration]*ebusStartupState{}
	startupStateFor := func(scope *ebusGenerationScope) *ebusStartupState {
		startupStateMu.Lock()
		defer startupStateMu.Unlock()
		native := scope.Key().Native
		state := startupStates[native]
		if state == nil {
			for other := range startupStates {
				delete(startupStates, other)
			}
			state = &ebusStartupState{}
			startupStates[native] = state
		}
		return state
	}

	// runEBusSourceAdmission: current-session source validation and
	// admission of the generation (start-critical).
	runEBusSourceAdmission := func(genCtx context.Context, scope *ebusGenerationScope) error {
		genCfg := cfg
		var sourceSelection *protocol.SourceAddressSelection
		if admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && !overrideSet && genCfg.ScanSourceAuto && genCfg.ScanSource == 0x00 && !shouldStartPassiveObserveFirst(genCfg) {
			result, err := ebusgateway.SelectDefaultStartupSourceAddress(genCtx)
			if err != nil {
				log.Printf("startup source selection degraded reason=source_selection_default_policy_failed err=%v", err)
				metrics.MarkDegraded(time.Now())
				artifactBuilder.SetDegraded("source_selection_default_policy_failed")
			} else {
				sourceSelection = &result
				genCfg.ScanSource = result.Source
				genCfg.ScanSourceAuto = false
				genCfg.StartupCompanionTarget = result.Companion
				genCfg.StartupProbeTargets = append(append([]byte(nil), genCfg.StartupProbeTargets...), startupProbeTargetsForSelection(result)...)
				artifactBuilder.SetPromotedSuspects(len(startupProbeTargets(genCfg)))
				artifactBuilder.SetSourceSelection(result.Source, result.Companion, result.Metrics.WarmupDurationActual)
				if perr := artifactBuilder.SetSourceSelectionMode("source_selection"); perr != nil {
					return fmt.Errorf("set default-policy source-selection mode: %w", perr)
				}
				log.Printf("startup source selection candidate source=0x%02X companion_target=0x%02X provenance=source_selection_default_policy", result.Source, result.Companion)
				recordBusAdmissionTransitionWithStabilityRefresh(genCtx, busObservability, "pending", result.Source, result.Companion, "active_probe_pending")
			}
		}
		if sourceSelection == nil {
			result, ok := configuredStartupSourceAdmissionCandidate(genCfg, admissionPath, overrideSet)
			if ok {
				sourceSelection = &result
				genCfg.ScanSource = result.Source
				genCfg.ScanSourceAuto = false
				genCfg.StartupCompanionTarget = result.Companion
				genCfg.StartupProbeTargets = append(append([]byte(nil), genCfg.StartupProbeTargets...), startupProbeTargetsForSelection(result)...)
				artifactBuilder.SetPromotedSuspects(len(startupProbeTargets(genCfg)))
				artifactBuilder.SetSourceSelection(result.Source, result.Companion, 0)
				if perr := artifactBuilder.SetSourceSelectionMode("source_selection"); perr != nil {
					return fmt.Errorf("set configured source-selection mode: %w", perr)
				}
				log.Printf("startup source selection candidate source=0x%02X companion_target=0x%02X provenance=configured_source", result.Source, result.Companion)
				recordBusAdmissionTransitionWithStabilityRefresh(genCtx, busObservability, "pending", result.Source, result.Companion, "active_probe_pending")
			}
		}

		runAdvisorySourceSelector := admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && shouldStartPassiveObserveFirst(genCfg) && (!overrideSet || genCfg.StartupSource.Validate)
		if runAdvisorySourceSelector {
			// The passive reconstructor is a process handle: it is started
			// once and reused by every later generation's warmup.
			active := currentReconstructor()
			if active == nil {
				started, err := startPassiveTransactionReconstructor(ctx, genCfg)
				if err != nil {
					return err
				}
				surfaceMu.Lock()
				reconstructor, active = started, started
				surfaceMu.Unlock()
				log.Printf("passive reconstructor started")
			}
			// The inserter is not subscribed here: warmup runs before the
			// admitted source is installed, so its self-source filter would
			// see 0 (Codex P2 round 2). attachPassiveObserveFirst binds it
			// after admission is finalized.
			selectionBus, err := ebusgateway.NewSourceSelectionBusAdapter(active, "startup_source_selection_bus", false)
			if err != nil {
				return fmt.Errorf("m3: new source address selection bus: %w", err)
			}
			// M4_SOURCE_SELECTION_HINT: the cached admitted source orders
			// candidates; validation still runs in full.
			hintCandidate, hintSet := runtimestate.HintFromState(runtimeState)
			if hintSet {
				log.Printf("source address selection hint=0x%02X (cached from prior admission cycle; warmup validation still required)", hintCandidate)
			}
			selector := protocol.NewSourceAddressSelector(selectionBus, ebusgateway.StartupAdmissionConfigWithHint(hintCandidate, hintSet))
			if busObservability != nil {
				busObservability.SetAdmissionStabilityWindow(
					ebusgateway.NewAdmissionStabilityWindow(ebusgateway.StartupAdmissionStateMinStabilitySecondsDefault),
				)
				recordBusAdmissionTransitionWithStabilityRefresh(genCtx, busObservability, "pending", 0, 0, "source_selection_warmup_in_progress")
			}
			metrics.MarkPending()
			metrics.StartWarmupCycle()

			log.Printf("source address selection warmup begin")
			warmupCtx, cancel := context.WithTimeout(genCtx, 6*time.Second)
			warmupStartedAt := time.Now()
			result, err := selector.Select(warmupCtx)
			cancel()
			if err != nil {
				log.Printf("startup source selection degraded reason=source_selection_failed err=%v", err)
				recordBusAdmissionTransitionWithStabilityRefresh(genCtx, busObservability, "degraded", 0, 0, "source_selection_failed")
				metrics.MarkDegraded(time.Now())
				artifactBuilder.SetDegraded("source_selection_failed")
				if perr := artifactBuilder.SetSourceSelectionMode("degraded_no_events"); perr != nil {
					return fmt.Errorf("set degraded source-selection mode: %w", perr)
				}
			} else if overrideSet {
				_ = ebusgateway.CheckExplicitSourceCompanionConflict(overrideSource, &result, metrics)
			} else {
				sourceSelection = &result
				genCfg.ScanSource = result.Source
				genCfg.ScanSourceAuto = false
				genCfg.StartupCompanionTarget = result.Companion
				genCfg.StartupProbeTargets = append(append([]byte(nil), genCfg.StartupProbeTargets...), startupProbeTargetsForSelection(result)...)
				artifactBuilder.SetPromotedSuspects(len(startupProbeTargets(genCfg)))
				artifactBuilder.SetSourceSelection(result.Source, result.Companion, time.Since(warmupStartedAt))
				if perr := artifactBuilder.SetSourceSelectionMode("source_selection"); perr != nil {
					return fmt.Errorf("set selected source-selection mode: %w", perr)
				}
				log.Printf("startup source selection candidate source=0x%02X companion_target=0x%02X", result.Source, result.Companion)
				recordBusAdmissionTransitionWithStabilityRefresh(genCtx, busObservability, "pending", result.Source, result.Companion, "active_probe_pending")
			}
		}

		if admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && overrideSet {
			genCfg.ScanSource = overrideSource
			genCfg.ScanSourceAuto = false
		}
		syncStaticAdmittedSource, syncStaticAdmitted := admittedMutationSourceForGateway(genCfg, admissionPath, overrideSet)
		if syncStaticAdmitted {
			if !installAdmittedSource(scope, syncStaticAdmittedSource) {
				return nil
			}
			// Admission resolved synchronously (override / static
			// fallback): bind the inserter and write ebus.self now. The M5
			// revalidator waits for activeProbePassed below.
			subscribeAddressTableInserter()
			selectionMethod := runtimestate.SelectionMethodExplicitValidateOnly
			if isEbusdTransportProtocol(genCfg.TransportConfig.Protocol) {
				selectionMethod = runtimestate.SelectionMethodEbusdTCPFallback
			}
			recordAdmittedSourceInRuntimeState(runtimeStateMgr, syncStaticAdmittedSource, 0, selectionMethod, false)
		} else if !clearAdmittedSource(scope) {
			return nil
		}
		state := startupStateFor(scope)
		startupStateMu.Lock()
		state.cfg, state.selection = genCfg, sourceSelection
		state.staticSource, state.static, state.admitted = syncStaticAdmittedSource, syncStaticAdmitted, true
		startupStateMu.Unlock()
		return nil
	}

	// runEBusSemanticSurface creates the process-scoped consumers from the
	// first admitted generation's validated configuration (start-critical).
	runEBusSemanticSurface := func(_ context.Context, scope *ebusGenerationScope) error {
		state := startupStateFor(scope)
		startupStateMu.Lock()
		genCfg, ready := state.cfg, state.admitted
		startupStateMu.Unlock()
		if !ready {
			return nil
		}
		// The install runs through the generation fence: a generation that
		// was withdrawn before this point creates no process-scoped consumer.
		return installProcessSurface(scope, func() error { return ensureProcessSurface(genCfg) })
	}

	// runEBusStartupScan starts the startup discovery scan of the generation
	// and its admission follow-ups; revalidation, background scanning and
	// passive promotion run as joined, best-effort generation work.
	runEBusStartupScan := func(genCtx context.Context, scope *ebusGenerationScope) error {
		state := startupStateFor(scope)
		startupStateMu.Lock()
		genCfg, sourceSelection, ready := state.cfg, state.selection, state.admitted
		syncStaticAdmittedSource, syncStaticAdmitted := state.staticSource, state.static
		startupStateMu.Unlock()
		if !ready {
			return nil
		}
		surfaceMu.Lock()
		poller, passivePromoter := semanticPoller, promoter
		surfaceMu.Unlock()

		startupCfg := resolveStartupScanSourceConfig(genCfg)
		startupSourceProvenance := startupScanSourceMode(genCfg, startupCfg)
		if admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && overrideSet {
			startupCfg.ScanSource = overrideSource
			startupCfg.ScanSourceAuto = false
			startupSourceProvenance = "explicit_validate_only"
		} else if admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && sourceSelection != nil {
			startupCfg.ScanSource = sourceSelection.Source
			startupCfg.ScanSourceAuto = false
			startupSourceProvenance = "source_selection"
		}

		log.Printf("startup scan pass")
		log.Printf("startup_directed_probe_phase begin source=0x%02X provenance=%s", startupCfg.ScanSource, startupSourceProvenance)
		if overrideSet {
			artifactBuilder.SetExplicitSource(startupCfg.ScanSource)
		}
		startupScanSignals := startDiscoveryScanLoopFn(genCtx, startupCfg, gateway, builder, adapterClassifier)
		// The scan loop goroutine is joined with the generation through its
		// completion signal: it runs on the generation context, and the
		// generation is not joined before the loop returned.
		if loopDone := startupScanSignals.done; loopDone != nil {
			joinLoop := func() {
				<-loopDone
				// The loop has returned, so no further delayed identity
				// enrichment can be scheduled; wait for the ones it started.
				postStartupIdentityRetryWG.Wait()
			}
			if !scope.GoJoin(func(context.Context) { joinLoop() }) {
				joinLoop()
			}
		}
		startupStateMu.Lock()
		state.signals, state.scanned = startupScanSignals, true
		startupStateMu.Unlock()

		// Static / override admission: the M5 revalidator starts only after
		// activeProbePassed, so directed 07 04 probes stay outside the
		// startup admission scan window (Codex P2 follow-up on PR #615).
		if syncStaticAdmitted && !sourceSelectionAdmit {
			source := syncStaticAdmittedSource
			scope.Go(func(runCtx context.Context) {
				select {
				case <-runCtx.Done():
				case <-startupScanSignals.activeProbePassed:
					runRuntimeStateRevalidatorLoop(runCtx, runtimeStateMgr, gateway, genCfg, source, 0)
				case <-startupScanSignals.admissionFailed:
				}
			})
		}

		if semanticBarrier != nil || sourceSelection != nil {
			selection := sourceSelection
			scope.Go(func(runCtx context.Context) {
				select {
				case <-runCtx.Done():
					return
				case <-startupScanSignals.activeProbePassed:
					if selection != nil {
						if !installAdmittedSource(scope, selection.Source) {
							return
						}
						artifactBuilder.SetSourceSelectionActive()
						metrics.MarkActive()
						recordBusAdmissionTransitionWithStabilityRefresh(runCtx, busObservability, "active", selection.Source, selection.Companion, "active_probe_passed")
						// Admission finalized asynchronously: bind the
						// inserter now that AdmittedSource() returns the
						// selected source (Codex P2 round 3).
						subscribeAddressTableInserter()
						selectionMethod := runtimestate.SelectionMethodWarmup
						if overrideSet {
							selectionMethod = runtimestate.SelectionMethodExplicitValidateOnly
						}
						recordAdmittedSourceInRuntimeState(runtimeStateMgr, selection.Source, selection.Companion, selectionMethod, true)
						scope.Go(func(revalidateCtx context.Context) {
							runRuntimeStateRevalidatorLoop(revalidateCtx, runtimeStateMgr, gateway, genCfg, selection.Source, 0)
						})
					}
				case <-startupScanSignals.admissionFailed:
					if selection != nil {
						if !clearAdmittedSource(scope) {
							return
						}
						artifactBuilder.SetDegraded("active_probe_failed")
						metrics.MarkDegraded(time.Now())
						recordBusAdmissionTransitionWithStabilityRefresh(runCtx, busObservability, "degraded", selection.Source, selection.Companion, "active_probe_failed")
					}
					if shouldCloseSemanticBarrier(admissionPath, overrideSet, selection != nil) {
						closeSemanticBarrier()
					}
					return
				}
				if shouldCloseSemanticBarrier(admissionPath, overrideSet, selection != nil) {
					closeSemanticBarrier()
				}
			})
		}

		scope.Go(func(runCtx context.Context) {
			startBackgroundFullScanFn(runCtx, startupCfg, gateway, builder, startupScanSignals.semanticBootstrapReady)
		})
		if passivePromoter != nil && poller != nil {
			scope.Go(passivePromoter.Run)
		}
		return nil
	}

	// runEBusPassiveObserveFirst starts the process-scoped passive
	// reconstructor after the generation's first scan pass and attaches the
	// observe-first consumers (start-critical).
	runEBusPassiveObserveFirst := func(genCtx context.Context, scope *ebusGenerationScope) error {
		state := startupStateFor(scope)
		startupStateMu.Lock()
		genCfg, startupScanSignals, ready := state.cfg, state.signals, state.scanned
		startupStateMu.Unlock()
		if !ready {
			return nil
		}
		if shouldStartPassiveObserveFirst(genCfg) {
			if currentReconstructor() == nil {
				waitForStartupScanFirstPass(genCtx, genCfg, startupScanSignals.firstPassDone)
				if genCtx.Err() != nil {
					return nil
				}
				started, err := startPassiveTransactionReconstructor(ctx, genCfg)
				if err != nil {
					return err
				}
				surfaceMu.Lock()
				reconstructor = started
				surfaceMu.Unlock()
			}
			if err := attachPassiveObserveFirst(); err != nil {
				return err
			}
		}
		return nil
	}

	generationTasks, err := newEBusGenerationTaskRunner(ebusGenerationTaskConfig{
		Tasks: []ebusGenerationTask{
			{Name: ebusTaskSourceAdmission, Run: runEBusSourceAdmission},
			{Name: ebusTaskSemanticSurface, Requires: []string{ebusTaskSourceAdmission}, Run: runEBusSemanticSurface},
			{Name: ebusTaskStartupScan, Requires: []string{ebusTaskSemanticSurface}, Run: runEBusStartupScan},
			{Name: ebusTaskPassiveObserveFirst, Requires: []string{ebusTaskStartupScan}, Run: runEBusPassiveObserveFirst},
		},
		Admitted: control.GenerationAdmitted(),
		Failure: func(key ebusGenerationKey, task string, err error) {
			log.Printf("ebus generation task failed task=%s native_generation=%d err=%v", task, key.Native, err)
		},
	})
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
	control.BindTasks(ctx, generationTasks, func(force bool) {
		admittedSourceMu.Lock()
		defer admittedSourceMu.Unlock()
		if force || !control.EBusAdmitted() {
			builder.ClearAdmittedMutationSource()
		}
	})
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
		store.SetTransportRuntimeStatus(eebusRuntimeTransportStatus(eebusRuntimeLifecycleSnapshot{State: eebusLifecycleStopped}))
	}
}
