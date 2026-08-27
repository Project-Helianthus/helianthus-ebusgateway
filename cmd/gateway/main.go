package main

import (
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusadmin"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	vaillantproviders "github.com/Project-Helianthus/helianthus-ebusreg/providers/vaillant"
)

func main() {
	cfg := ebusgateway.DefaultConfig()
	inputs := bindFlags(flag.CommandLine, &cfg)
	flag.Parse()
	if err := resolveModbusEndpointFile(&cfg.ModbusTCPConfig, inputs.modbusEndpointFile); err != nil {
		log.Fatalf("gateway: %v", err)
	}
	applyTransportSourcePolicy(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		if inputs.modbusEndpointFile != "" {
			err = redactFileSourcedModbusError(err, cfg.ModbusTCPConfig.Endpoint)
		}
		log.Fatalf("gateway: %v", err)
	}
}

func run(ctx context.Context, cfg ebusgateway.Config) (result error) {
	resolvedBuildInfo, err := resolveGatewayBuildInfo(buildVersion, buildID)
	if err != nil {
		return fmt.Errorf("gateway build identity: %w", err)
	}

	applyTransportSourcePolicy(&cfg)
	if err := cfg.ValidatePortalPV(); err != nil {
		return fmt.Errorf("validate Portal PV configuration: %w", err)
	}
	if err := ebusgateway.ValidateSynchronizedEvidenceConfig(cfg); err != nil {
		return fmt.Errorf("validate synchronized evidence config: %w", err)
	}
	if len(cfg.Providers) == 0 {
		cfg.Providers = vaillantproviders.Default()
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

	modbusAdapter, err := startModbusRuntime(
		ctx,
		cfg.ModbusTCPConfig,
		dialModbusEndpointFn,
		newModbusEndpointFn,
	)
	if err != nil {
		log.Printf("Modbus TCP unavailable; continuing without Modbus")
		modbusAdapter = nil
	}
	if modbusAdapter != nil {
		defer func() {
			if err := modbusAdapter.Close(); err != nil {
				result = errors.Join(result, fmt.Errorf("shutdown Modbus TCP sidecar: %w", err))
			}
		}()
		sunSpecWorker := newGatewaySunSpecLiveSmokeWorker(ctx, modbusAdapter, log.Printf)
		if sunSpecWorker != nil {
			sunSpecWorker.Start()
			defer func() { _ = sunSpecWorker.Close() }()
		}
	}
	m2mRuntime, err := newM2MGraphQLRuntime(cfg, modbusAdapter)
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

	eebusAdapter, _, eebusLifecycle, _, err := startEEBusAdminAwareRuntime(ctx, cfg)
	if err != nil {
		log.Printf("eeBUS runtime unavailable; continuing without eeBUS reason=startup")
	}
	if eebusLifecycle != nil {
		defer func() {
			if err := eebusLifecycle.Shutdown(); err != nil {
				result = errors.Join(result, fmt.Errorf("shutdown eeBUS sidecar: %w", err))
			}
		}()
	}
	eebusAdminHandler := eebusadmin.NewUnavailableHandler()
	if eebusLifecycle != nil {
		eebusAdminHandler = eebusLifecycle
	}
	if evidenceRuntime != nil {
		var captureEEBus eebusEvidenceCapture
		if eebusAdapter != nil {
			captureEEBus = func(pseudonymKey []byte) (json.RawMessage, time.Time, error) {
				return mcp.CaptureEEBusV1ServicesEvidence(eebusAdapter, pseudonymKey)
			}
		}
		if err := evidenceRuntime.Configure(captureEEBus, resolvedBuildInfo.EvidenceVersion(), newSynchronizedEvidenceClock(), synchronizedEvidenceEntropy); err != nil {
			return fmt.Errorf("configure synchronized evidence sidecar: %w", err)
		}
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
	ebusDriver, err := newEBusDriverController(cfg)
	if err != nil {
		return fmt.Errorf("construct eBUS DriverManager: %w", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*defaultEBusDriverDrainTimeout+time.Second)
		defer cancel()
		if err := ebusDriver.Shutdown(stopCtx); err != nil {
			result = errors.Join(result, fmt.Errorf("shutdown eBUS driver: %w", err))
		}
	}()
	cfg.Transport = ebusDriver.active
	if ebusDriver.passive != nil {
		cfg.PassiveTransport = ebusDriver.passive
		if configuredEBusDriverProtocol(cfg) == ebusgateway.TransportAdapterDirect {
			cfg.ObserveFirstWarmupCompletedTransactions = 0
			cfg.ObserveFirstWarmupConnectedWindow = 0
			cfg.ObserveFirstWarmupPostResetTransactions = 0
			cfg.ObserveFirstWarmupPostResetWindow = 0
		}
	}
	adapterClassifier := ebusDriver.classifier
	if err := ebusDriver.Start(ctx); err != nil {
		return fmt.Errorf("start eBUS DriverManager: %w", err)
	}
	ebusDriverSnapshot := ebusDriver.Snapshot()
	if ebusDriverSnapshot.ObservedState != "RUNNING" {
		log.Printf("eBUS driver unavailable; continuing state=%s reason=%s", ebusDriverSnapshot.ObservedState, ebusDriverSnapshot.Reason.Code)
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
	// the registry as passive_observed/corroborated_pending. The inserter is
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
	builder.SetStatusProvider(newRuntimeStatusProvider(semanticRuntime.Provider(), liveAdmittedEBusSource))
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
			modbusProvider, scheduleWriter, configWriter, busObservability, lateWatchProvider, eebusAdminHandler, eebusLifecycle, ebusDriver.ProxyReadiness, liveAdmittedEBusSource, resolvedBuildInfo, ebusDriver,
		)
	}
	server, advertiser, err := startHTTPServerFn(
		ctx,
		cfg,
		gateway,
		builder,
		hub,
		portalSemanticProvider,
		eebusMCPProvider(eebusAdapter),
		eebusMCPCommandRouter(eebusAdapter),
		newGatewayModbusMCPProvider(modbusAdapter),
		lateScheduleWriter,
		lateConfigWriter,
		busObservability,
		nil,
	)
	if err != nil {
		return err
	}
	defer func() {
		// Preserve the historical teardown order: stop eBUS consumers before
		// closing observability, then retire mDNS and HTTP before Gateway/driver.
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
		if server != nil {
			if err := server.Close(); err != nil {
				log.Printf("http server close: %v", err)
			}
		}
	}()

	// The control plane is already serving through stable fail-closed
	// bindings. For startup-scan deployments, wait for the first correlated
	// RUNNING generation before source admission and the one initial scan pass.
	// This turns a slow healthy constructor into one immediate scan instead of
	// an unavailable pass followed by the default 30-second retry interval.
	if cfg.ScanOnStart && !ebusDriver.WaitRunning(ctx) {
		return nil
	}

	var sourceSelection *protocol.SourceAddressSelection
	if admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && !overrideSet && cfg.ScanSourceAuto && cfg.ScanSource == 0x00 && !shouldStartPassiveObserveFirst(cfg) {
		result, err := ebusgateway.SelectDefaultStartupSourceAddress(ctx)
		if err != nil {
			log.Printf("startup source selection degraded reason=source_selection_default_policy_failed err=%v", err)
			metrics.MarkDegraded(time.Now())
			artifactBuilder.SetDegraded("source_selection_default_policy_failed")
		} else {
			sourceSelection = &result
			cfg.ScanSource = result.Source
			cfg.ScanSourceAuto = false
			cfg.StartupCompanionTarget = result.Companion
			cfg.StartupProbeTargets = append(cfg.StartupProbeTargets, startupProbeTargetsForSelection(result)...)
			artifactBuilder.SetPromotedSuspects(len(startupProbeTargets(cfg)))
			artifactBuilder.SetSourceSelection(result.Source, result.Companion, result.Metrics.WarmupDurationActual)
			if perr := artifactBuilder.SetSourceSelectionMode("source_selection"); perr != nil {
				return fmt.Errorf("set default-policy source-selection mode: %w", perr)
			}
			log.Printf("startup source selection candidate source=0x%02X companion_target=0x%02X provenance=source_selection_default_policy", result.Source, result.Companion)
			recordBusAdmissionTransitionWithStabilityRefresh(ctx, busObservability, "pending", result.Source, result.Companion, "active_probe_pending")
		}
	}
	if sourceSelection == nil {
		result, ok := configuredStartupSourceAdmissionCandidate(cfg, admissionPath, overrideSet)
		if ok {
			sourceSelection = &result
			cfg.ScanSource = result.Source
			cfg.ScanSourceAuto = false
			cfg.StartupCompanionTarget = result.Companion
			cfg.StartupProbeTargets = append(cfg.StartupProbeTargets, startupProbeTargetsForSelection(result)...)
			artifactBuilder.SetPromotedSuspects(len(startupProbeTargets(cfg)))
			artifactBuilder.SetSourceSelection(result.Source, result.Companion, 0)
			if perr := artifactBuilder.SetSourceSelectionMode("source_selection"); perr != nil {
				return fmt.Errorf("set configured source-selection mode: %w", perr)
			}
			log.Printf("startup source selection candidate source=0x%02X companion_target=0x%02X provenance=configured_source", result.Source, result.Companion)
			recordBusAdmissionTransitionWithStabilityRefresh(ctx, busObservability, "pending", result.Source, result.Companion, "active_probe_pending")
		}
	}

	var (
		insertSubscribeOnce sync.Once
		insertSubscribed    atomic.Bool
	)
	// subscribeAddressTableInserter binds the AddressTableInserter to the
	// active reconstructor at most once. Subscription is gated on the
	// admitted source being finalized (builder.AdmittedMutationSource
	// returns ok=true) so the inserter's self-source filter sees the real
	// admitted address — never 0 — and cannot mistakenly insert the
	// gateway's own active-probe targets/companions as passive_observed.
	// sync.Once + atomic.Bool make the helper safe to call from both the
	// main run() goroutine and the async activeProbePassed goroutine.
	// Subscription failure is non-fatal; the inserter is a non-critical
	// observer.
	subscribeAddressTableInserter := func() {
		if insertSubscribed.Load() || reconstructor == nil {
			return
		}
		if _, ok := builder.AdmittedMutationSource(); !ok {
			return
		}
		insertSubscribeOnce.Do(func() {
			sub, err := reconstructor.Subscribe(
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
		if reconstructor == nil {
			return nil
		}
		if busObservability != nil {
			if err := busObservability.AttachReconstructor(ctx, reconstructor); err != nil {
				return err
			}
		}
		if deduplicator != nil {
			if err := deduplicator.AttachReconstructor(ctx, reconstructor); err != nil {
				return err
			}
		}
		if listener == nil {
			var err error
			listener, err = startBroadcastListenerWithReconstructorFn(ctx, gateway.Router, reconstructor)
			if err != nil {
				return err
			}
		}
		// Best-effort early bind. If admission isn't finalized yet
		// (sourceSelection path: waits on activeProbePassed), this
		// returns silently and the wire-up below catches the signal.
		subscribeAddressTableInserter()
		return nil
	}

	runAdvisorySourceSelector := admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && shouldStartPassiveObserveFirst(cfg) && (!overrideSet || cfg.StartupSource.Validate)
	if runAdvisorySourceSelector {
		reconstructor, err = startPassiveTransactionReconstructor(ctx, cfg)
		if err != nil {
			return err
		}
		log.Printf("passive reconstructor started")

		// Phase A.5 (Codex P2 round 2): do NOT subscribe the inserter
		// here. The source-selection warmup runs before
		// builder.SetAdmittedMutationSource is called, so the
		// inserter's AdmittedSource() closure would return 0 during
		// startup_directed_probe_phase. On non-adapter-direct
		// transports the passive tap can see the gateway's own
		// active probes; with admitted=0 the inserter's self-source
		// filter would NOT skip them and could insert the gateway's
		// own companion/targets as passive_observed.
		//
		// Subscription is deferred to attachPassiveObserveFirst (late
		// path), which runs after builder.SetAdmittedMutationSource is
		// called and admission is finalized.

		selectionBus, err := ebusgateway.NewSourceSelectionBusAdapter(reconstructor, "startup_source_selection_bus", false)
		if err != nil {
			return fmt.Errorf("m3: new source address selection bus: %w", err)
		}
		// M4_SOURCE_SELECTION_HINT: bias candidate ordering toward the
		// cached ebus.self.last_admitted_source from the prior admission
		// cycle. Validation still runs in full — the hint is a candidate-
		// ordering aid, NEVER a bypass of warmup or evidence checks.
		// AD24: HintFromState is the only exported surface that emits
		// the cached byte; the Manager.State() field exposes it as a
		// plain struct member but every consumer routes through this
		// helper for clarity.
		hintCandidate, hintSet := runtimestate.HintFromState(runtimeState)
		if hintSet {
			log.Printf("source address selection hint=0x%02X (cached from prior admission cycle; warmup validation still required)", hintCandidate)
		}
		selector := protocol.NewSourceAddressSelector(selectionBus, ebusgateway.StartupAdmissionConfigWithHint(hintCandidate, hintSet))

		// Install AD08/AD22 stability window on the bus_observability store
		// before the first source-selection state observation. Window is sized
		// from cfg.StateMinStabilitySeconds (default 30, AD22 invariant
		// enforced at config-load).
		if busObservability != nil {
			busObservability.SetAdmissionStabilityWindow(
				ebusgateway.NewAdmissionStabilityWindow(ebusgateway.StartupAdmissionStateMinStabilitySecondsDefault),
			)
			recordBusAdmissionTransitionWithStabilityRefresh(ctx, busObservability, "pending", 0, 0, "source_selection_warmup_in_progress")
		}
		// Wire M5 expvar surfaces: state pending -> warmup cycle starts.
		// Resolves cruise-run #20 validation finding that the 11
		// startup_source_selection_* expvars were defined and Publish()'d via
		// GetOrInitStartupSourceSelectionMetrics but never updated by the
		// runtime — they all stayed at 0 even after source-address selector success.
		metrics.MarkPending()
		metrics.StartWarmupCycle()

		log.Printf("source address selection warmup begin")
		warmupCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
		warmupStartedAt := time.Now()
		result, err := selector.Select(warmupCtx)
		cancel()
		if err != nil {
			log.Printf("startup source selection degraded reason=source_selection_failed err=%v", err)
			recordBusAdmissionTransitionWithStabilityRefresh(ctx, busObservability, "degraded", 0, 0, "source_selection_failed")
			metrics.MarkDegraded(time.Now())
			artifactBuilder.SetDegraded("source_selection_failed")
			if perr := artifactBuilder.SetSourceSelectionMode("degraded_no_events"); perr != nil {
				return fmt.Errorf("set degraded source-selection mode: %w", perr)
			}
		} else {
			if overrideSet {
				_ = ebusgateway.CheckExplicitSourceCompanionConflict(overrideSource, &result, metrics)
			} else {
				sourceSelection = &result
				cfg.ScanSource = result.Source
				cfg.ScanSourceAuto = false
				cfg.StartupCompanionTarget = result.Companion
				cfg.StartupProbeTargets = append(cfg.StartupProbeTargets, startupProbeTargetsForSelection(result)...)
				artifactBuilder.SetPromotedSuspects(len(startupProbeTargets(cfg)))
				artifactBuilder.SetSourceSelection(result.Source, result.Companion, time.Since(warmupStartedAt))
				if perr := artifactBuilder.SetSourceSelectionMode("source_selection"); perr != nil {
					return fmt.Errorf("set selected source-selection mode: %w", perr)
				}
				log.Printf("startup source selection candidate source=0x%02X companion_target=0x%02X", result.Source, result.Companion)
				recordBusAdmissionTransitionWithStabilityRefresh(ctx, busObservability, "pending", result.Source, result.Companion, "active_probe_pending")
				// Reflect selector-observed event count if exposed by ebusgo.
				// SourceAddressSelection does not currently surface a count; the
				// warmup_events_seen stays at the per-cycle reset value
				// of 0 unless the selection bus adapter is extended to call
				// metrics.RecordWarmupEvent on each forwarded frame.
				// Tracked as cruise-run #20 follow-up.
			}
		}
	}

	if admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && overrideSet {
		cfg.ScanSource = overrideSource
		cfg.ScanSourceAuto = false
	}
	builder.SetStatusProvider(newRuntimeStatusProvider(semanticRuntime.Provider(), liveAdmittedEBusSource))
	syncStaticAdmittedSource, syncStaticAdmitted := admittedMutationSourceForGateway(cfg, admissionPath, overrideSet)
	if syncStaticAdmitted {
		builder.SetAdmittedMutationSource(syncStaticAdmittedSource)
		// Phase A.5 (Codex P2 round 3): admission resolved synchronously
		// (override / static fallback). Bind the inserter now that
		// AdmittedSource() returns the real source.
		subscribeAddressTableInserter()
		// M4: write the validated source to runtime_state.ebus.self
		// synchronously — this is just a cache update, not bus
		// traffic, so it's safe to run before the startup directed-
		// probe phase. M5 revalidator start is DEFERRED to
		// post-validation (after startupScanSignals.activeProbePassed)
		// so directed 07 04 probes don't race the startup admission
		// scan's bus traffic. Codex P2 follow-up on PR #615.
		selectionMethod := runtimestate.SelectionMethodExplicitValidateOnly
		if isEbusdTransportProtocol(cfg.TransportConfig.Protocol) {
			selectionMethod = runtimestate.SelectionMethodEbusdTCPFallback
		}
		recordAdmittedSourceInRuntimeState(runtimeStateMgr, syncStaticAdmittedSource, 0, selectionMethod, false)
	} else {
		builder.ClearAdmittedMutationSource()
	}

	sourceSelectionAdmission := admissionPath == ebusgateway.TransportAdmissionSourceSelectionCapable && !overrideSet
	var semanticBarrier chan struct{}
	if cfg.ScanOnStart && (shouldStartPassiveObserveFirst(cfg) || sourceSelectionAdmission) {
		semanticBarrier = make(chan struct{})
	}
	semanticCfg := resolveStartupScanSourceConfig(cfg)
	semanticPoller := startVaillantSemanticPollingFn(ctx, semanticCfg, gateway, semanticRuntime.Provider(), hub, semanticBarrier)
	if semanticPoller != nil {
		gatedGraphQLWriter := admittedGraphQLSemanticWriter{
			boiler:   semanticPoller,
			system:   semanticPoller,
			schedule: semanticPoller,
			admitted: liveAdmittedEBusSource,
		}
		lateGraphQLWriter.Bind(gatedGraphQLWriter)
		lateGraphQLWatchProvider.Bind(newGraphQLWatchSummaryProvider(semanticPoller.shadow))
	}
	if semanticPoller != nil && eebusAdapter != nil {
		captureSource := newLeafPromotionLiveSource(
			semanticPoller,
			eebusAdapter,
			eebusMCPCommandRouter(eebusAdapter),
			liveAdmittedEBusSource,
		)
		captureRuntime, captureErr := newLeafPromotionCaptureRuntime(cfg.EEBusConfig.StateRoot, captureSource)
		if captureErr != nil {
			return fmt.Errorf("leaf promotion capture runtime: %w", captureErr)
		}
		if captureRuntime != nil {
			eebusAdapter.SetLeafPromotionCapture(captureRuntime)
		}
	}
	if semanticPoller != nil && semanticPoller.shadow != nil && deduplicator != nil {
		// Use the semantic shadow directly here. The broader runtime observer can
		// fall back to the deduplicator itself, which would re-enter dedup locks
		// while passive fingerprints are being built.
		deduplicator.SetWatchObserver(semanticPoller.shadow)
	}
	observeFirstFlags := ebusgateway.NormalizeObserveFirstFeatureFlags(
		cfg.ObserveFirstEnabled,
		cfg.PassiveStateDirectApply,
		cfg.PassiveConfigDirectApply,
		cfg.ExternalWritePolicy,
	)
	passiveShadowLaneEnabled := observeFirstFlags.PassiveStateDirectApply() ||
		observeFirstFlags.PassiveConfigDirectApply() ||
		observeFirstFlags.ExternalWritePolicy() != ebusgateway.ObserveFirstExternalWritePolicyRecordOnly
	if semanticPoller != nil && deduplicator != nil && observeFirstFlags.ObserveFirstEnabled() && passiveShadowLaneEnabled {
		if err := attachPassiveShadowProducerFn(semanticPoller, ctx, deduplicator); err != nil {
			return err
		}
	}

	startupCfg := resolveStartupScanSourceConfig(cfg)
	startupSourceProvenance := startupScanSourceMode(cfg, startupCfg)
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
	startupScanSignals := startDiscoveryScanLoopFn(ctx, startupCfg, gateway, builder, adapterClassifier)

	// Synchronous static / override admission path: defer the M5
	// revalidator start until activeProbePassed (or ctx cancel) to keep
	// directed 07 04 probes outside the startup admission scan window.
	// (Codex P2 follow-up on PR #615 — without this defer, M5 would
	// emit bus traffic concurrent with startup directed-probe phase.)
	if syncStaticAdmitted && !sourceSelectionAdmission {
		go func(source byte) {
			select {
			case <-ctx.Done():
				return
			case <-startupScanSignals.activeProbePassed:
				startRuntimeStateRevalidator(ctx, runtimeStateMgr, gateway, cfg, source, 0)
			case <-startupScanSignals.admissionFailed:
				// Admission failed mid-validation; skip M5 start —
				// no point probing cached members when we don't have
				// a healthy admitted source on the bus.
				return
			}
		}(syncStaticAdmittedSource)
	}

	if semanticBarrier != nil || sourceSelection != nil {
		go func() {
			select {
			case <-ctx.Done():
				if semanticBarrier != nil {
					close(semanticBarrier)
				}
				return
			case <-startupScanSignals.activeProbePassed:
				if sourceSelection != nil {
					builder.SetAdmittedMutationSource(sourceSelection.Source)
					artifactBuilder.SetSourceSelectionActive()
					metrics.MarkActive()
					recordBusAdmissionTransitionWithStabilityRefresh(ctx, busObservability, "active", sourceSelection.Source, sourceSelection.Companion, "active_probe_passed")
					// Phase A.5 (Codex P2 round 3): admission finalized
					// asynchronously via activeProbePassed. Bind the
					// inserter now that AdmittedSource() returns the
					// selected source — closes the directed-probe window
					// where admitted=0 could leak gateway-own probes.
					subscribeAddressTableInserter()

					// M4 write-back + M5 revalidator wiring delegate to
					// finalizeRuntimeStateForAdmittedSource so override
					// + static-fallback admissions get the same
					// treatment (Codex P2 follow-up on PR #615).
					selectionMethod := runtimestate.SelectionMethodWarmup
					if overrideSet {
						selectionMethod = runtimestate.SelectionMethodExplicitValidateOnly
					}
					finalizeRuntimeStateForAdmittedSource(ctx, runtimeStateMgr, gateway, cfg, sourceSelection.Source, sourceSelection.Companion, selectionMethod, true)
				}
			case <-startupScanSignals.admissionFailed:
				if sourceSelection != nil {
					builder.ClearAdmittedMutationSource()
					artifactBuilder.SetDegraded("active_probe_failed")
					metrics.MarkDegraded(time.Now())
					recordBusAdmissionTransitionWithStabilityRefresh(ctx, busObservability, "degraded", sourceSelection.Source, sourceSelection.Companion, "active_probe_failed")
				}
				if shouldCloseSemanticBarrier(admissionPath, overrideSet, sourceSelection != nil) {
					if semanticBarrier != nil {
						close(semanticBarrier)
					}
				}
				return
			}
			if shouldCloseSemanticBarrier(admissionPath, overrideSet, sourceSelection != nil) {
				if semanticBarrier != nil {
					close(semanticBarrier)
				}
			}
		}()
	}

	go startBackgroundFullScanFn(ctx, startupCfg, gateway, builder, startupScanSignals.semanticBootstrapReady)

	// Runtime passive-promotion pipeline: late-arriving devices (e.g.
	// the regulator boots after the gateway) accumulate passive
	// evidence in busObservability's EvidenceBuffer; the promoter
	// loop confirms candidates with B524 coherency, registers them
	// in the registry, refreshes router planes, and signals semantic
	// discovery refresh.
	if busObservability != nil && busObservability.EvidenceBuffer() != nil && semanticPoller != nil {
		busObservability.SetAdmittedSourceProvider(func() byte {
			source, ok := builder.AdmittedMutationSource()
			if !ok {
				return 0
			}
			return source
		})
		promoter, err := ebusgateway.NewPassiveDiscoveryPromoter(ebusgateway.PassiveDiscoveryPromoterOptions{
			Registry:          gateway.Registry,
			EvidenceBuffer:    busObservability.EvidenceBuffer(),
			ConfirmFn:         semanticPoller.ConfirmB524Coherent,
			SemanticRefreshFn: semanticPoller.EnqueueDiscoveryRefresh,
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
			go promoter.Run(ctx)
		}
	}

	// M6 enrichment trigger — once the AddressTableInserter inserts a
	// new passive slot, schedule a semantic-poller discovery refresh so
	// the new address gets probed for identity. Once SerialNumber +
	// Manufacturer land via the post-probe Register, the registry's
	// M6 identity-merge collapses canonical pairs that share identity
	// into a single DeviceEntry.
	if semanticPoller != nil {
		addressTableInserter.SetEnrichmentRefreshFn(semanticPoller.EnqueueDiscoveryRefresh)
		// P5 (post-Phase-C live validation 2026-05-08): per-address
		// identity probe wired so passive-observed slots (e.g.
		// NETX3 0xF1↔0xF6, BASV2 0x10) get a 0x07/0x04 + B5.09
		// ScanID probe and re-Register with full identity. Bounded
		// + idempotent (probes each address at most once per
		// gateway lifetime).
		//
		// P5 round-5 (Codex P2 follow-up 2026-05-08): gate per-
		// insert probes on the startup admission barrier. Without
		// the gate, a passive slot inserted before semanticBarrier
		// closes would call EnqueueAddressIdentityProbe immediately
		// and the task scheduler (already running) would emit
		// ScanDirected bus traffic during the admission validation
		// window — racing the startup directed scan. When the
		// gate drops a probe call during admission, the post-
		// barrier BackfillUnidentifiedAddresses catches up: it
		// iterates the registry's existing entries and re-fires
		// the probe for any unidentified address.
		var probeReady atomic.Bool
		if semanticBarrier == nil {
			probeReady.Store(true)
		}
		addressTableInserter.SetEnrichmentIdentityProbeFn(func(addr byte) {
			if !probeReady.Load() {
				return
			}
			semanticPoller.EnqueueAddressIdentityProbe(addr)
		})

		// Backfill identity probes for any addresses that were
		// inserted before the hook was wired (early subscription
		// in subscribeAddressTableInserter can fire during the
		// activeProbePassed window) OR while the barrier was open.
		// Defer until semanticBarrier closes to avoid racing the
		// startup directed scan.
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
	}

	if semanticPoller != nil {
		lateScheduleWriter.Bind(admittedMCPScheduleWriter{
			writer:   semanticPoller,
			admitted: liveAdmittedEBusSource,
		})
		lateConfigWriter.Bind(admittedMCPConfigWriter{
			writer:   &mcpConfigWriterAdapter{poller: semanticPoller},
			admitted: liveAdmittedEBusSource,
		})
		lateWatchProvider.Bind(newMCPWatchSummaryProvider(semanticPoller.shadow))
	}
	if shouldStartPassiveObserveFirst(cfg) {
		if reconstructor == nil {
			waitForStartupScanFirstPass(ctx, cfg, startupScanSignals.firstPassDone)

			reconstructor, err = startPassiveTransactionReconstructor(ctx, cfg)
			if err != nil {
				return err
			}
		}
		if err := attachPassiveObserveFirst(); err != nil {
			_ = reconstructor.Close()
			return err
		}
	}
	if cfg.BroadcastListen && !shouldStartPassiveObserveFirst(cfg) {
		log.Printf("passive observe-first unavailable on transport=%s; continuing degraded", cfg.TransportConfig.Protocol)
	}
	<-ctx.Done()
	return nil
}
