package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	"github.com/Project-Helianthus/helianthus-ebusgateway/ui"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	ebusgoTransport "github.com/Project-Helianthus/helianthus-ebusgo/transport"
	vaillantproviders "github.com/Project-Helianthus/helianthus-ebusreg/providers/vaillant"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

var (
	buildVersion                              = "0.4.0"
	buildID                                   = "unknown"
	wireObserveFirstObserversFn               = wireObserveFirstObservers
	startDiscoveryScanLoopFn                  = startDiscoveryScanLoopWithClassifier
	startVaillantSemanticPollingFn            = startVaillantSemanticPolling
	attachPassiveShadowProducerFn             = (*vaillantSemanticPoller).AttachPassiveShadowProducer
	startPassiveTransactionReconstructor      = ebusgateway.StartPassiveTransactionReconstructor
	startBroadcastListenerWithReconstructorFn = ebusgateway.StartBroadcastListenerWithReconstructor
	startHTTPServerFn                         = startHTTPServer
	admissionStabilityRefreshDelay            = time.Duration(ebusgateway.StartupAdmissionStateMinStabilitySecondsDefault)*time.Second + 200*time.Millisecond
	instanceGUIDPattern                       = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

type runtimeWatchObserver struct {
	primary  ebusgateway.WatchObserver
	fallback ebusgateway.WatchObserver
}

func (observer *runtimeWatchObserver) Observe(key ebusgateway.WatchKey) ebusgateway.WatchObservation {
	if key == nil {
		return ebusgateway.WatchObservation{State: ebusgateway.WatchObservationStateCatalogMiss}
	}
	if observer != nil && observer.primary != nil {
		observation := observer.primary.Observe(key)
		if observation.State != ebusgateway.WatchObservationStateCatalogMiss {
			return observation
		}
	}
	if observer != nil && observer.fallback != nil {
		return observer.fallback.Observe(key)
	}
	return ebusgateway.WatchObservation{State: ebusgateway.WatchObservationStateCatalogMiss}
}

func main() {
	cfg := ebusgateway.DefaultConfig()
	bindFlags(flag.CommandLine, &cfg)
	flag.Parse()
	applyTransportSourcePolicy(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		log.Fatalf("gateway: %v", err)
	}
}

func recordBusAdmissionTransitionWithStabilityRefresh(ctx context.Context, store *ebusgateway.BusObservabilityStore, state string, source, companionTarget byte, reason string) {
	if store == nil {
		return
	}
	if store.RecordBusAdmissionTransition(state, source, companionTarget, reason) {
		return
	}
	if state != "active" && state != "degraded" {
		return
	}
	go func() {
		timer := time.NewTimer(admissionStabilityRefreshDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			store.RecordBusAdmissionTransition(state, source, companionTarget, reason)
		}
	}()
}

func run(ctx context.Context, cfg ebusgateway.Config) error {
	applyTransportSourcePolicy(&cfg)

	if len(cfg.Providers) == 0 {
		cfg.Providers = vaillantproviders.Default()
	}

	// Initialize the runtime-state Manager early so the cached
	// ebus.self.last_admitted_source can be passed as a hint to the
	// SourceAddressSelector below, and the Manager is available for
	// post-admission UpdateSelf and address-table-revalidate write-back.
	// Errors during Load are tolerated — Manager.Load returns an empty
	// state on missing/corrupt and the gateway continues without a hint.
	// (runtime-state-w19-26.locked M2_GATEWAY_LOADER + M4_SOURCE_SELECTION_HINT)
	runtimeStateMgr, runtimeState := initRuntimeStateManager(ctx, cfg)
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtimeStateMgr.Stop(stopCtx); err != nil {
			log.Printf("runtime_state stop: %v", err)
		}
	}()

	// Wire adapter-direct mode: create multiplexer, configure active
	// and passive transports before gateway construction.
	adapterMuxCloser, adapterClassifier, err := wireAdapterDirect(ctx, &cfg)
	if err != nil {
		return fmt.Errorf("adapter-direct: %w", err)
	}
	if adapterMuxCloser != nil {
		defer func() {
			if err := adapterMuxCloser(); err != nil {
				log.Printf("adapter-direct close: %v", err)
			}
		}()
	}

	// Warn if --proxy-listen is set but adapter-direct transport was not
	// activated (the proxy endpoint requires the adapter multiplexer).
	if cfg.ProxyListenAddr != "" && cfg.Transport == nil {
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
		log.Fatalf("FATAL: AD23 enum violation at startup: %v", err)
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
			log.Fatalf("FATAL: SAS M4 enum violation on explicit source path: %v", err)
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
	builder.SetStatusProvider(newRuntimeStatusProvider(cfg, semanticRuntime.Provider()))
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
				log.Fatalf("FATAL: SAS M4 enum violation on source-selection default-policy path: %v", perr)
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
				log.Fatalf("FATAL: SAS M4 enum violation on configured source validation path: %v", perr)
			}
			log.Printf("startup source selection candidate source=0x%02X companion_target=0x%02X provenance=configured_source", result.Source, result.Companion)
			recordBusAdmissionTransitionWithStabilityRefresh(ctx, busObservability, "pending", result.Source, result.Companion, "active_probe_pending")
		}
	}

	var (
		listener            *ebusgateway.BroadcastListener
		reconstructor       *ebusgateway.PassiveTransactionReconstructor
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
				log.Fatalf("FATAL: AD23 enum violation on source-address selector-fail path: %v", perr)
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
					log.Fatalf("FATAL: AD23 enum violation on source-selection path: %v", perr)
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
	builder.SetStatusProvider(newRuntimeStatusProvider(cfg, semanticRuntime.Provider()))
	if source, admitted := admittedMutationSourceForGateway(cfg, admissionPath, overrideSet); admitted {
		builder.SetAdmittedMutationSource(source)
		// Phase A.5 (Codex P2 round 3): admission resolved synchronously
		// (override / static fallback). Bind the inserter now that
		// AdmittedSource() returns the real source.
		subscribeAddressTableInserter()
		// M4/M5: also finalize runtime-state for override + static-
		// fallback admissions. Without this, cached known_bus_members[]
		// would only ever be revalidated on warmup-based admissions and
		// stale entries would persist indefinitely on configured
		// transports (Codex P2 follow-up on PR #615). The selection
		// method varies by path: ebusd-tcp → ebusd-tcp-fallback,
		// override / explicit static → explicit_validate_only.
		selectionMethod := runtimestate.SelectionMethodExplicitValidateOnly
		if isEbusdTransportProtocol(cfg.TransportConfig.Protocol) {
			selectionMethod = runtimestate.SelectionMethodEbusdTCPFallback
		}
		finalizeRuntimeStateForAdmittedSource(ctx, runtimeStateMgr, gateway, cfg, source, 0, selectionMethod, false)
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
			admitted: builder.AdmittedMutationSource,
		}
		builder.SetSystemConfigWriter(gatedGraphQLWriter)
		builder.SetBoilerConfigWriter(gatedGraphQLWriter)
		builder.SetScheduleWriter(gatedGraphQLWriter)
		builder.SetWatchSummaryProvider(newGraphQLWatchSummaryProvider(semanticPoller.shadow))
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

	if err := builder.Start(ctx); err != nil {
		return err
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

	var scheduleWriter mcp.ScheduleWriter
	if semanticPoller != nil {
		scheduleWriter = admittedMCPScheduleWriter{
			writer:   semanticPoller,
			admitted: builder.AdmittedMutationSource,
		}
	}
	var configWriter mcp.ConfigWriter
	if semanticPoller != nil {
		configWriter = admittedMCPConfigWriter{
			writer:   &mcpConfigWriterAdapter{poller: semanticPoller},
			admitted: builder.AdmittedMutationSource,
		}
	}
	var shadowCache *ebusgateway.ShadowCache
	if semanticPoller != nil {
		shadowCache = semanticPoller.shadow
	}

	server, advertiser, err := startHTTPServerFn(
		ctx,
		cfg,
		gateway,
		builder,
		hub,
		semanticRuntime.Provider(),
		scheduleWriter,
		configWriter,
		busObservability,
		shadowCache,
	)
	if err != nil {
		return err
	}
	if shouldStartPassiveObserveFirst(cfg) {
		if reconstructor == nil {
			waitForStartupScanFirstPass(ctx, cfg, startupScanSignals.firstPassDone)

			reconstructor, err = startPassiveTransactionReconstructor(ctx, cfg)
			if err != nil {
				if advertiser != nil {
					_ = advertiser.Close()
				}
				if server != nil {
					_ = server.Close()
				}
				return err
			}
		}
		if err := attachPassiveObserveFirst(); err != nil {
			_ = reconstructor.Close()
			if advertiser != nil {
				_ = advertiser.Close()
			}
			if server != nil {
				_ = server.Close()
			}
			return err
		}
	}
	if cfg.BroadcastListen && !shouldStartPassiveObserveFirst(cfg) {
		log.Printf("passive observe-first unavailable on transport=%s; continuing degraded", cfg.TransportConfig.Protocol)
	}
	defer func() {
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

	<-ctx.Done()
	return nil
}

func shouldStartPassiveObserveFirst(cfg ebusgateway.Config) bool {
	return cfg.BroadcastListen && ebusgateway.PassiveTransportSupported(cfg)
}

func waitForStartupScanFirstPass(ctx context.Context, cfg ebusgateway.Config, firstPassDone <-chan struct{}) {
	if !cfg.ScanOnStart || !shouldStartPassiveObserveFirst(cfg) || firstPassDone == nil {
		return
	}
	select {
	case <-ctx.Done():
	case <-firstPassDone:
	}
}

func wireObserveFirstObservers(cfg *ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
	if cfg == nil {
		return nil, nil, nil
	}

	observer := &runtimeWatchObserver{primary: cfg.WatchObserver}

	dedupCfg := *cfg
	dedupCfg.WatchObserver = cfg.WatchObserver

	var deduplicator *ebusgateway.ActivePassiveDeduplicator
	if cfg.BroadcastListen {
		dedup, err := ebusgateway.NewActivePassiveDeduplicator(dedupCfg)
		if err != nil {
			return nil, nil, err
		}
		deduplicator = dedup
		observer.fallback = deduplicator
	}

	observerCfg := *cfg
	observerCfg.WatchObserver = observer
	if deduplicator != nil {
		observerCfg.LocalAddressSnapshotter = deduplicator
	}

	busObservability := ebusgateway.NewBusObservabilityStore(observerCfg)
	cfg.BusConfig.Observer = ebusgateway.ChainBusObservers(cfg.BusConfig.Observer, busObservability)
	cfg.WatchObserver = observer
	cfg.WatchEfficiencyObserver = busObservability
	if deduplicator != nil {
		cfg.BusConfig.Observer = ebusgateway.ChainBusObservers(cfg.BusConfig.Observer, deduplicator)
	}

	return busObservability, deduplicator, nil
}

func applyTransportSourcePolicy(cfg *ebusgateway.Config) {
	if cfg == nil {
		return
	}

	protocol := strings.TrimSpace(strings.ToLower(string(cfg.TransportConfig.Protocol)))
	switch protocol {
	case "ebusd", "ebusd-tcp":
		return
	default:
		if cfg.ScanSourceAuto {
			cfg.ScanSource = 0x00
		}
	}
}

func admittedMutationSourceForGateway(cfg ebusgateway.Config, admissionPath ebusgateway.TransportAdmissionPath, overrideSet bool) (byte, bool) {
	if cfg.ScanSource != 0 && !cfg.ScanSourceAuto &&
		(admissionPath != ebusgateway.TransportAdmissionSourceSelectionCapable || overrideSet) {
		return cfg.ScanSource, true
	}
	if admissionPath == ebusgateway.TransportAdmissionStaticFallback && isEbusdTransportProtocol(cfg.TransportConfig.Protocol) && cfg.ScanSourceAuto && cfg.ScanSource == 0 {
		defaultSource := ebusgateway.DefaultConfig().ScanSource
		return defaultSource, defaultSource != 0
	}
	return 0, false
}

func configuredStartupSourceAdmissionCandidate(cfg ebusgateway.Config, admissionPath ebusgateway.TransportAdmissionPath, overrideSet bool) (protocol.SourceAddressSelection, bool) {
	if admissionPath != ebusgateway.TransportAdmissionSourceSelectionCapable || overrideSet || shouldStartPassiveObserveFirst(cfg) {
		return protocol.SourceAddressSelection{}, false
	}
	if cfg.ScanSourceAuto || cfg.ScanSource == 0x00 {
		return protocol.SourceAddressSelection{}, false
	}
	companion := cfg.StartupCompanionTarget
	if companion == 0x00 {
		companion = protocol.CompanionAddressForSource(cfg.ScanSource)
	}
	return protocol.SourceAddressSelection{
		Source:    cfg.ScanSource,
		Companion: companion,
		Metrics: protocol.SourceAddressSelectionMetrics{
			ObservedProbableTargets: append([]byte(nil), cfg.StartupProbeTargets...),
		},
	}, true
}

func isEbusdTransportProtocol(protocol ebusgateway.TransportProtocol) bool {
	switch strings.TrimSpace(strings.ToLower(string(protocol))) {
	case "ebusd", string(ebusgateway.TransportEbusdTCP):
		return true
	default:
		return false
	}
}

func normalizeInstanceGUID(value string) (string, error) {
	normalized := strings.TrimSpace(strings.ToLower(value))
	if normalized == "" {
		return "", nil
	}
	if !instanceGUIDPattern.MatchString(normalized) {
		return "", fmt.Errorf("invalid instance-guid %q", value)
	}
	return normalized, nil
}

func gatewayMDNSText(cfg ebusgateway.Config) []string {
	text := []string{
		"path=" + cfg.GraphQLPath,
		"transport=http",
		"version=1",
	}
	if cfg.InstanceGUID != "" {
		text = append(text, "instance_guid="+cfg.InstanceGUID)
	}
	return text
}

func bindFlags(fs *flag.FlagSet, cfg *ebusgateway.Config) {
	if fs == nil || cfg == nil {
		return
	}

	fs.StringVar((*string)(&cfg.TransportConfig.Protocol), "transport", string(cfg.TransportConfig.Protocol), "transport protocol: enh, ens, udp-plain, tcp-plain, or ebusd-tcp")
	fs.StringVar(&cfg.TransportConfig.Network, "network", cfg.TransportConfig.Network, "transport network: unix, tcp, or udp")
	fs.StringVar(&cfg.TransportConfig.Address, "address", cfg.TransportConfig.Address, "transport address (unix socket path or host:port)")
	fs.DurationVar(&cfg.TransportConfig.ReadTimeout, "read-timeout", cfg.TransportConfig.ReadTimeout, "transport read timeout")
	fs.DurationVar(&cfg.TransportConfig.WriteTimeout, "write-timeout", cfg.TransportConfig.WriteTimeout, "transport write timeout")
	fs.DurationVar(&cfg.TransportConfig.DialTimeout, "dial-timeout", cfg.TransportConfig.DialTimeout, "transport dial timeout")
	fs.IntVar(&cfg.QueueCapacity, "queue-capacity", cfg.QueueCapacity, "bus queue capacity (0 uses protocol default)")
	fs.BoolVar(&cfg.ScanOnStart, "scan", cfg.ScanOnStart, "scan bus on startup")
	fs.DurationVar(&cfg.ScanTimeout, "scan-timeout", cfg.ScanTimeout, "startup scan timeout")
	fs.DurationVar(&cfg.ScanRequestTimeout, "scan-request-timeout", cfg.ScanRequestTimeout, "startup scan per-request timeout")
	fs.DurationVar(&cfg.ScanInterval, "scan-interval", cfg.ScanInterval, "startup scan retry interval (when scan finds 0 devices)")
	fs.BoolVar(&cfg.DiagnosticFullRangeRetry, "diagnostic-full-range-retry", cfg.DiagnosticFullRangeRetry, "allow full-range retry on non-ebusd-tcp transports after a Vaillant root candidate is observed")
	fs.DurationVar(&cfg.BootLiveTimeout, "boot-live-timeout", cfg.BootLiveTimeout, "semantic startup timeout before entering degraded mode")
	fs.DurationVar(&cfg.SemanticDiscoveryInterval, "semantic-discovery-interval", cfg.SemanticDiscoveryInterval, "semantic discovery polling interval")
	fs.DurationVar(&cfg.SemanticConfigInterval, "semantic-config-interval", cfg.SemanticConfigInterval, "semantic config polling interval")
	fs.DurationVar(&cfg.SemanticStateInterval, "semantic-state-interval", cfg.SemanticStateInterval, "semantic state polling interval")
	fs.DurationVar(&cfg.SemanticEnergyInterval, "semantic-energy-interval", cfg.SemanticEnergyInterval, "semantic energy polling interval")
	fs.DurationVar(&cfg.SemanticRequestTimeout, "semantic-request-timeout", cfg.SemanticRequestTimeout, "semantic per-request timeout")
	fs.Func("semantic-read-breaker-failure-budget", "semantic read breaker consecutive failure budget (<=0 disables)", func(value string) error {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("invalid semantic-read-breaker-failure-budget %q", value)
		}
		cfg.SemanticReadBreakerFailureBudget = parsed
		cfg.SemanticReadBreakerFailureBudgetSet = true
		return nil
	})
	fs.DurationVar(&cfg.SemanticReadBreakerOpenCooldown, "semantic-read-breaker-open-cooldown", cfg.SemanticReadBreakerOpenCooldown, "semantic read breaker open-state cooldown before probe")
	fs.IntVar(&cfg.SemanticReadBreakerHalfOpenProbeLimit, "semantic-read-breaker-half-open-probe-limit", cfg.SemanticReadBreakerHalfOpenProbeLimit, "semantic read breaker half-open probes per cooldown window")
	fs.IntVar(&cfg.SemanticZonePresenceMissThreshold, "semantic-zone-presence-miss-threshold", cfg.SemanticZonePresenceMissThreshold, "consecutive discovery misses required before a zone is marked absent")
	fs.IntVar(&cfg.SemanticZonePresenceHitThreshold, "semantic-zone-presence-hit-threshold", cfg.SemanticZonePresenceHitThreshold, "consecutive discovery hits required before an absent zone is marked present")
	fs.DurationVar(&cfg.SemanticDHWStaleTTL, "semantic-dhw-stale-ttl", cfg.SemanticDHWStaleTTL, "maximum age to keep DHW last-known state during cache-sourced/transient failures")
	fs.DurationVar(&cfg.SemanticRegulatorRecheckInterval, "semantic-regulator-recheck-interval", cfg.SemanticRegulatorRecheckInterval, "regulator capability re-evaluation interval")
	fs.DurationVar(&cfg.SemanticRegulatorAbsenceGrace, "semantic-regulator-absence-grace", cfg.SemanticRegulatorAbsenceGrace, "grace window before WARN_NO_REGULATOR after regulator disappears")
	fs.StringVar(&cfg.SemanticCachePath, "semantic-cache-path", cfg.SemanticCachePath, "semantic cache file path for startup preload and live persistence")
	fs.Func("semantic-interval", "DEPRECATED: semantic state polling interval", func(value string) error {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid semantic-interval %q", value)
		}
		cfg.SemanticInterval = duration
		cfg.SemanticStateInterval = duration
		return nil
	})
	fs.BoolVar(&cfg.BroadcastListen, "broadcast", cfg.BroadcastListen, "enable broadcast listener (separate connection)")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "http listen address (empty disables)")
	fs.StringVar(&cfg.GraphQLPath, "graphql-path", cfg.GraphQLPath, "graphql endpoint path")
	fs.StringVar(&cfg.SnapshotPath, "snapshot-path", cfg.SnapshotPath, "projection snapshot endpoint path")
	fs.StringVar(&cfg.SubscriptionPath, "subscription-path", cfg.SubscriptionPath, "graphql subscriptions path")
	fs.StringVar(&cfg.MCPPath, "mcp-path", cfg.MCPPath, "mcp endpoint path")
	fs.StringVar(&cfg.UIPath, "ui-path", cfg.UIPath, "portal ui path")
	fs.StringVar(&cfg.PortalPath, "portal-path", cfg.PortalPath, "dynamic portal path")
	fs.StringVar(&cfg.DumpUploadPath, "dump-upload-path", cfg.DumpUploadPath, "register dump upload endpoint path")
	fs.BoolVar(&cfg.MDNSAdvertise, "mdns", cfg.MDNSAdvertise, "advertise graphql endpoint via mdns")
	fs.StringVar(&cfg.MDNSInstance, "mdns-instance", cfg.MDNSInstance, "mdns instance name")
	fs.Func("instance-guid", "stable gateway instance UUIDv4 (lowercase)", func(value string) error {
		normalized, err := normalizeInstanceGUID(value)
		if err != nil {
			return err
		}
		cfg.InstanceGUID = normalized
		return nil
	})
	fs.StringVar(&cfg.InstanceGUIDSource, "instance-guid-source",
		cfg.InstanceGUIDSource,
		"AD27 provenance tag for -instance-guid (runtime_state | legacy_migrated | generated | cli-override); "+
			"empty defaults to cli-override with a deprecation log when -instance-guid is provided")
	fs.StringVar(&cfg.RuntimeStatePath, "runtime-state-path",
		cfg.RuntimeStatePath,
		"override /data/runtime_state.json path (empty uses runtimestate package default)")
	fs.StringVar(&cfg.DumpOutputDir, "dump-output-dir", cfg.DumpOutputDir, "unknown device dump output dir")
	fs.StringVar(&cfg.DumpUploadURL, "dump-upload-url", cfg.DumpUploadURL, "unknown device dump upload url (internal)")
	fs.BoolVar(&cfg.DumpIncludePII, "dump-include-pii", cfg.DumpIncludePII, "include identifiers in unknown device dumps")
	fs.BoolVar(&cfg.EnableStaticSeedTable, "enable-static-seed-table", cfg.EnableStaticSeedTable, "plant productids static seed entries (NETX3 0xF1 / 0xF6 / 0x04 / 0xFF, BASV2 0x15 / 0xEC) into registry at startup; default false")
	fs.BoolVar(&cfg.ObserveFirstEnabled, "observe-first-enabled", cfg.ObserveFirstEnabled, "enable observe-first runtime behavior gates")
	fs.BoolVar(&cfg.PassiveStateDirectApply, "passive-state-direct-apply", cfg.PassiveStateDirectApply, "allow passive state direct-apply when observe-first is enabled")
	fs.BoolVar(&cfg.PassiveConfigDirectApply, "passive-config-direct-apply", cfg.PassiveConfigDirectApply, "allow passive config direct-apply when state direct-apply is enabled")
	fs.Func("external-write-policy", "externally observed write policy: invalidate_only, record_only, or record_and_invalidate", func(value string) error {
		policy, err := ebusgateway.ParseObserveFirstExternalWritePolicy(value)
		if err != nil {
			return err
		}
		cfg.ExternalWritePolicy = policy
		return nil
	})

	fs.StringVar(&cfg.ProxyListenAddr, "proxy-listen", cfg.ProxyListenAddr, "TCP listen address for ENH proxy clients (e.g. :19001, empty disables)")

	fs.Func("source-addr", "source address for scans/semantic reads (e.g. 0xf0, 0x00, or auto)", func(value string) error {
		value = strings.TrimSpace(strings.ToLower(value))
		if value == "" {
			return nil
		}
		if value == "auto" {
			cfg.ScanSource = 0x00
			cfg.ScanSourceAuto = true
			return nil
		}
		parsed, err := strconv.ParseUint(value, 0, 8)
		if err != nil {
			return fmt.Errorf("invalid source-addr %q", value)
		}
		cfg.ScanSource = byte(parsed)
		cfg.ScanSourceAuto = cfg.ScanSource == 0x00
		return nil
	})
	fs.Func("startup-probe-targets", "comma-separated explicit startup directed-probe targets (e.g. 0x08,0x15)", func(value string) error {
		targets, err := parseStartupProbeTargets(value)
		if err != nil {
			return err
		}
		cfg.StartupProbeTargets = targets
		return nil
	})
	fs.Func("startup-source-override", "override source address for source-selection-capable direct transports (e.g. 0xf0)", func(value string) error {
		value = strings.TrimSpace(strings.ToLower(value))
		if value == "" {
			cfg.StartupSource.Source = nil
			return nil
		}
		parsed, err := strconv.ParseUint(value, 0, 8)
		if err != nil {
			return fmt.Errorf("invalid startup-source-override %q", value)
		}
		source := uint8(parsed)
		if source == 0x00 {
			return fmt.Errorf("invalid startup-source-override %q: source 0x00 is not a valid active initiator", value)
		}
		cfg.StartupSource.Source = &source
		return nil
	})
	fs.BoolVar(&cfg.StartupSource.Validate, "startup-source-override-validate", cfg.StartupSource.Validate, "run source-address selector in advisory-only mode alongside startup-source-override")
}

func parseStartupProbeTargets(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	targets := make([]byte, 0, len(parts))
	seen := make(map[byte]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(strings.ToLower(part))
		if part == "" {
			continue
		}
		parsed, err := strconv.ParseUint(part, 0, 8)
		if err != nil {
			return nil, fmt.Errorf("invalid startup-probe-targets address %q", part)
		}
		target := byte(parsed)
		if target < 0x03 || target >= 0xFE || target == 0xAA || target == 0xA9 || isInitiatorCapableAddress(target) {
			return nil, fmt.Errorf("startup-probe-targets address 0x%02X outside target-capable range", target)
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		targets = append(targets, target)
	}
	return targets, nil
}

// wireAdapterDirect creates and starts the adapter multiplexer if the
// transport protocol is adapter-direct. It configures both active and
// passive transports in cfg before gateway construction.
//
// Returns a closer function for the multiplexer, the instance-scoped
// activeTxnClassifier (the mux), or nils if not in adapter-direct mode.
// The classifier is threaded explicitly into startDiscoveryScanLoopFn
// at the call site in run() — never captured in a package-level closure —
// so classifier state is strictly instance-local (Codex PR #502 P2).
func wireAdapterDirect(ctx context.Context, cfg *ebusgateway.Config) (func() error, activeTxnClassifier, error) {
	network := cfg.TransportConfig.Network
	address := cfg.TransportConfig.Address

	// Detect adapter-direct:// or adapter-direct-ens:// URI scheme in
	// the address field. When the user passes
	// --address=adapter-direct://host:port the protocol flag is still
	// the default ("enh") because parseTransportEndpoint runs later
	// inside ebusgateway.New.
	const (
		schemePrefix    = "adapter-direct://"
		schemePrefixENS = "adapter-direct-ens://"
	)
	addrLower := strings.ToLower(address)
	uriIsENS := strings.HasPrefix(addrLower, schemePrefixENS)
	uriIsStd := strings.HasPrefix(addrLower, schemePrefix)
	if !strings.EqualFold(string(cfg.TransportConfig.Protocol), string(ebusgateway.TransportAdapterDirect)) {
		if !uriIsStd && !uriIsENS {
			return nil, nil, nil
		}
	}

	// Always strip the scheme prefix if present, regardless of which
	// detection branch was taken. Without this, net.Dial receives
	// "adapter-direct://host:port" as the address and fails.
	if uriIsENS {
		address = address[len(schemePrefixENS):]
		network = "tcp"
	} else if uriIsStd {
		address = address[len(schemePrefix):]
		// The URI form implies TCP — force it unconditionally since
		// the default TransportConfig.Network is "unix" and would
		// cause net.Dial("unix", "host:port") to fail.
		network = "tcp"
	} else if network == "" || (network == "unix" && strings.Contains(address, ":")) {
		// Explicit --transport adapter-direct path: if network is
		// still the default "unix" but address looks like host:port,
		// force TCP to avoid net.Dial("unix", "host:port") failures.
		network = "tcp"
	}
	if address == "" {
		return nil, nil, fmt.Errorf("adapter-direct requires an address (e.g. adapter-direct://boiler.local:9999)")
	}

	// Determine ENH vs ENS sub-protocol. ENH is the default.
	// The adapter-direct-ens:// URI scheme selects ENS explicitly.
	adapterProtocol := "enh"
	if uriIsENS {
		adapterProtocol = "ens"
	}

	muxCfg := adaptermux.Config{
		Protocol:     adapterProtocol,
		Network:      network,
		Address:      address,
		DialTimeout:  cfg.TransportConfig.DialTimeout,
		ReadTimeout:  cfg.TransportConfig.ReadTimeout,
		WriteTimeout: cfg.TransportConfig.WriteTimeout,
	}
	if muxCfg.DialTimeout == 0 {
		muxCfg.DialTimeout = 5 * time.Second
	}
	// Mux read timeout controls how often the idle-timeout branch runs
	// (tryGrantAndStart) AND how quickly activeCh receives bytes from
	// the upstream ENH transport. 50ms matches the eBUS SYN interval so
	// waitForSyn (which needs 2 SYNs) completes within ~100ms instead
	// of ~400ms at 200ms, preventing scan-abort after collision retry.
	muxCfg.ReadTimeout = 50 * time.Millisecond
	if muxCfg.WriteTimeout == 0 {
		muxCfg.WriteTimeout = 5 * time.Second
	}

	mux := adaptermux.New(muxCfg)
	// Codex PR #502 P2: the instance-scoped classifier is returned to
	// run() and threaded explicitly as the 5th argument to
	// startDiscoveryScanLoopFn. No package-level closure captures this
	// mux — so a second wireAdapterDirect call (e.g. in the same process
	// across tests) sees its own classifier at the call site.

	// Create passive transport BEFORE Start() so the callback is wired.
	// Only create it when BroadcastListen is enabled — otherwise no
	// consumer reads from the passive channel and reset delivery blocks
	// readLoop (P0 deadlock fix).
	if cfg.BroadcastListen {
		cfg.PassiveTransport = mux.PassiveTransport()

		// Skip passive warmup in adapter-direct mode. The passive stream
		// only sees third-party traffic (gateway bytes are suppressed by
		// the mux), so warmup thresholds are never met organically. Zero
		// thresholds tell the store to promote immediately.
		cfg.ObserveFirstWarmupCompletedTransactions = 0
		cfg.ObserveFirstWarmupConnectedWindow = 0
		cfg.ObserveFirstWarmupPostResetTransactions = 0
		cfg.ObserveFirstWarmupPostResetWindow = 0
	}

	if err := mux.Start(ctx); err != nil {
		return nil, nil, fmt.Errorf("start multiplexer: %w", err)
	}

	log.Printf("adapter-direct: connected to %s/%s", network, address)

	// Start proxy listener if configured (exposes ENH endpoint for
	// external clients like ebusd).
	var proxyListener *adaptermux.ProxyListener
	if cfg.ProxyListenAddr != "" {
		pl, err := adaptermux.NewProxyListener(ctx, mux, cfg.ProxyListenAddr, log.Default())
		if err != nil {
			if cerr := mux.Close(); cerr != nil {
				log.Printf("adapter-direct: mux.Close after proxy-listener error: %v", cerr)
			}
			return nil, nil, fmt.Errorf("proxy listener: %w", err)
		}
		proxyListener = pl
		log.Printf("adapter-direct: proxy listener on %s", pl.Addr())
	}

	// Configure gateway transports.
	cfg.Transport = mux.ActiveTransport()

	// Return a closer that cleans up both the proxy listener (if any)
	// and the mux itself. This covers early run() failures where
	// gateway.Close() never runs (and thus activeTransport.Close
	// never calls mux.Close). On normal shutdown the gateway's
	// transport.Close calls mux.Close first — that is safe because
	// mux.Close is idempotent (sync.Once guarded).
	closer := func() error {
		if proxyListener != nil {
			if err := proxyListener.Close(); err != nil {
				log.Printf("adapter-direct: proxyListener.Close: %v", err)
			}
		}
		return mux.Close()
	}
	return closer, activeTxnClassifier(mux), nil
}

func startHTTPServer(
	ctx context.Context,
	cfg ebusgateway.Config,
	gateway *ebusgateway.Gateway,
	builder *graphql.Builder,
	hub *graphql.BroadcastHub,
	semanticProvider graphql.SemanticProvider,
	scheduleWriter mcp.ScheduleWriter,
	configWriter mcp.ConfigWriter,
	busObservability *ebusgateway.BusObservabilityStore,
	shadowCache *ebusgateway.ShadowCache,
) (*http.Server, mdns.Advertiser, error) {
	if cfg.HTTPAddr == "" {
		return nil, nil, nil
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

	queryHandler, err := graphql.NewInvokeHandler(builder, gateway.Registry, gateway.Router)
	if err != nil {
		return nil, nil, err
	}
	snapshotHandler, err := graphql.NewProjectionSnapshotHandler(builder)
	if err != nil {
		return nil, nil, err
	}
	subscriptionHandler, err := graphql.NewSubscriptionHandler(builder, gateway.Registry, gateway.Router, hub)
	if err != nil {
		return nil, nil, err
	}
	mcpServer, err := mcp.NewServer(gateway.Registry, gateway.Router)
	if err != nil {
		return nil, nil, err
	}
	mcpServer.SetAdmittedRPCSourceProvider(builder.AdmittedMutationSource)
	mcpServer.SetStatusProvider(newMCPRuntimeStatusProvider(cfg, semanticProvider))
	if busObservability != nil {
		mcpServer.SetBusObservabilityProvider(newMCPBusObservabilityProvider(busObservability))
	}
	if shadowCache != nil {
		mcpServer.SetWatchSummaryProvider(newMCPWatchSummaryProvider(shadowCache))
	}
	mcpServer.SetSemanticProvider(newMCPSemanticProvider(semanticProvider))

	// M2a_GATEWAY_MCP (execution-plans#19): install Vaillant B503 MCP tool
	// surface. Uses a deferred dispatcher stub — production B524-style raw
	// RPC wiring for the 2-byte (family, selector) frame is scheduled as a
	// follow-up under the M2b / M3 rollout (the MCP substrate currently
	// models dispatch as (plane, method, params) through the catalog, and
	// B503 has not yet been added to the catalog — intentional, per plan
	// AD01: Vaillant protocol knowledge stays out of ebusreg).
	//
	// Registering here ensures:
	//   - `ebus.v1.vaillant.*` tools are listed by tools/list (P1 lint from
	//     Codex review of M2a — tools MUST be reachable by clients, even
	//     if production dispatch is not yet wired);
	//   - capability signal is emitted;
	//   - forbidden-surface guards (TestNoVaillantInstallWriteTools) apply
	//     to the production build, not just the test harness.
	//
	// With the stub dispatcher, read tools surface `UPSTREAM_RPC_FAILED`
	// with the "production wiring pending" message; live-monitor action
	// paths use the real session FSM so EXPIRED normalization, session
	// epochs, and owner-conditional release are all exercised — only the
	// raw bus dispatch is stubbed.
	b503rt := installVaillantB503(mcpServer, gateway, &cfg, builder.AdmittedMutationSource)
	// M2b_GATEWAY_GRAPHQL (execution-plans#19): wire the GraphQL B503
	// provider to the same Manager + Dispatcher the MCP surface uses. A
	// single Manager across both surfaces is mandatory — GraphQL
	// Enable/Read/Disable operating on a separate Manager would break
	// the single-owner session invariant.
	if b503rt != nil {
		builder.SetVaillantB503Provider(newB503GraphQLProvider(b503rt))
	}

	// M4c2: populate the package-level responder capability provider
	// based on the active transport protocol. Consumers apply fail-closed
	// semantics on absence, so this MUST be called before any MCP
	// surface serves its first envelope. The provider closure is
	// evaluated per-envelope; hot-path cost is a single pointer load +
	// struct copy. See decision doc @ 567a6798 §4.2 + §5.
	//
	// A nil return from buildResponderCapabilityProvider means the raw
	// transport protocol does not canonicalize to any of the three
	// locked rows at v1.1 (ENH / ENS / ebusd-tcp). In that case we omit
	// the capability entirely so consumers fall back to §4.3 rule 1
	// (absence → scope=none, fail-closed). This preserves invariant I1
	// (exactly three rows at v1.1) and I2 (active.transport MUST appear
	// in transports[]) without widening the schema.
	// Pass the live transport instance (gateway.Transport) so the provider
	// can type-assert against ebusgoTransport.ResponderTransport. The
	// adapter-direct mux returns a RawTransport that does NOT satisfy
	// ResponderTransport, so config-string "enh" + mux active path
	// correctly downgrades to scope=none (see Codex P1 on PR #509).
	if provider := buildResponderCapabilityProvider(cfg, gateway.Transport); provider != nil {
		ebus_standard.SetResponderCapabilityProvider(provider)
	} else {
		log.Printf("warning: meta.capabilities.responder omitted: transport protocol %q does not canonicalize to ENH/ENS/ebusd-tcp", cfg.TransportConfig.Protocol)
	}
	if scheduleWriter != nil {
		mcpServer.SetScheduleWriter(scheduleWriter)
	}
	if configWriter != nil {
		mcpServer.SetConfigWriter(configWriter)
	}

	mux := http.NewServeMux()
	if busObservability != nil {
		mux.Handle(normalizeMountPath(cfg.MetricsPath, ebusgateway.DefaultMetricsPath), busObservability.MetricsHandler())
	}
	// Expose expvar surfaces (including the 11 startup_source_selection_* counters
	// from M5) via /debug/vars. The expvar package's init registers the
	// handler on http.DefaultServeMux, but the gateway uses its own mux so
	// the handler must be wired explicitly. Resolves cruise-run #20
	// validation finding: M5 expvars were defined + Publish()'d but not
	// reachable over HTTP.
	mux.Handle("/debug/vars", expvar.Handler())
	mux.Handle(cfg.GraphQLPath, queryHandler)
	mux.Handle(cfg.SnapshotPath, snapshotHandler)
	mux.Handle(cfg.SubscriptionPath, subscriptionHandler)
	mux.Handle(cfg.MCPPath, mcpServer.Handler())
	if cfg.DumpUploadPath != "" {
		uploadPath := cfg.DumpUploadPath
		if !strings.HasPrefix(uploadPath, "/") {
			uploadPath = "/" + uploadPath
		}
		mux.Handle(uploadPath, ebusgateway.NewRegisterDumpUploadHandler(cfg.DumpOutputDir))
	}
	if cfg.UIPath != "" {
		uiPath := normalizeMountPath(cfg.UIPath, "/ui")
		uiHandler := ui.NewHandler(cfg.GraphQLPath)
		mux.Handle(uiPath+"/", http.StripPrefix(uiPath, uiHandler))
		mux.HandleFunc(uiPath, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, uiPath+"/", http.StatusMovedPermanently)
		})
	}
	if cfg.PortalPath != "" {
		portalPath := normalizeMountPath(cfg.PortalPath, "/portal")
		var getPortalBusObservability func() any
		if busObservability != nil {
			getPortalBusObservability = func() any {
				return busObservability.Snapshot().Summary
			}
		}
		portalHandler := portal.NewHandler(portal.Options{
			GraphQLPath:      cfg.GraphQLPath,
			SnapshotPath:     cfg.SnapshotPath,
			SubscriptionPath: cfg.SubscriptionPath,
			MCPPath:          cfg.MCPPath,
			GatewayVersion:   buildVersion,
			BuildID:          buildID,
			ListRegistry: func() []portal.RegistryDevice {
				schemaSnapshot := builder.FreshSchema()
				schemaByAddr := make(map[byte]graphql.Device, len(schemaSnapshot.Devices))
				for _, sd := range schemaSnapshot.Devices {
					schemaByAddr[sd.Address] = sd
				}
				items := make([]portal.RegistryDevice, 0)
				gateway.Registry.Iterate(func(entry registry.DeviceEntry) bool {
					if entry == nil {
						return true
					}
					rawAddrs := entry.Addresses()
					intAddrs := make([]int, len(rawAddrs))
					for i, a := range rawAddrs {
						intAddrs[i] = int(a)
					}
					device := portal.RegistryDevice{
						Address:      int(entry.PrimaryDisplayAddress()),
						Addresses:    intAddrs,
						Manufacturer: entry.Manufacturer(),
						DeviceID:     entry.DeviceID(),
						SerialNumber: entry.SerialNumber(),
						Software:     entry.SoftwareVersion(),
						Hardware:     entry.HardwareVersion(),
						Planes:       make([]portal.RegistryPlane, 0),
					}
					if sd, ok := schemaByAddr[entry.PrimaryDisplayAddress()]; ok {
						device.DisplayName = sd.DisplayName
						device.Role = sd.Role
					}
					for _, plane := range entry.Planes() {
						if plane == nil {
							continue
						}
						methods := plane.Methods()
						methodNames := make([]string, 0, len(methods))
						for _, method := range methods {
							if method == nil {
								continue
							}
							methodNames = append(methodNames, method.Name())
						}
						device.Planes = append(device.Planes, portal.RegistryPlane{
							Name:    plane.Name(),
							Methods: methodNames,
						})
					}
					items = append(items, device)
					return true
				})
				return items
			},
			ListSemantic: func() portal.SemanticSnapshot {
				if semanticProvider == nil {
					return portal.SemanticSnapshot{}
				}
				return portal.SemanticSnapshot{
					Zones:        mapPortalZones(semanticProvider.Zones()),
					DHW:          mapPortalDHW(semanticProvider.DHW()),
					Energy:       mapPortalEnergyTotals(semanticProvider.EnergyTotals()),
					BoilerStatus: mapPortalBoilerStatus(semanticProvider.BoilerStatus()),
					System:       mapPortalSystemStatus(semanticProvider.System()),
					Circuits:     mapPortalCircuits(semanticProvider.Circuits()),
					RadioDevices: mapPortalRadioDevices(semanticProvider.RadioDevices()),
					FM5Mode:      string(semanticProvider.FM5SemanticMode()),
					Solar:        mapPortalSolarStatus(semanticProvider.Solar()),
					Cylinders:    mapPortalCylinders(semanticProvider.Cylinders()),
					AdapterInfo:  mapPortalAdapterInfo(semanticProvider.AdapterHardwareInfo()),
					CapturedUTC:  time.Now().UTC().Format(time.RFC3339),
				}
			},
			GetBusObservability: getPortalBusObservability,
			ListProjections: func() []portal.ProjectionDevice {
				snapshot := builder.FreshSchema()
				items := make([]portal.ProjectionDevice, 0, len(snapshot.Devices))
				for _, device := range snapshot.Devices {
					summaries := make([]portal.ProjectionSummary, 0, len(device.Projections))
					for _, projection := range device.Projections {
						summaries = append(summaries, portal.ProjectionSummary{
							Plane:     projection.Plane,
							NodeCount: len(projection.Nodes),
							EdgeCount: len(projection.Edges),
						})
					}
					items = append(items, portal.ProjectionDevice{
						Address:      device.Address,
						DeviceID:     device.DeviceID,
						DisplayName:  device.DisplayName,
						Manufacturer: device.Manufacturer,
						Projections:  summaries,
					})
				}
				return items
			},
			GetProjection: func(address byte, plane string) (portal.ProjectionGraph, bool) {
				snapshot := builder.FreshSchema()
				for _, device := range snapshot.Devices {
					if device.Address != address {
						continue
					}
					for _, projection := range device.Projections {
						if !strings.EqualFold(projection.Plane, plane) {
							continue
						}
						nodes := make([]portal.ProjectionNode, 0, len(projection.Nodes))
						for _, node := range projection.Nodes {
							nodes = append(nodes, portal.ProjectionNode{
								ID:            node.ID,
								Path:          node.Path,
								CanonicalPath: node.CanonicalPath,
							})
						}
						edges := make([]portal.ProjectionEdge, 0, len(projection.Edges))
						for _, edge := range projection.Edges {
							edges = append(edges, portal.ProjectionEdge{
								ID:   edge.ID,
								From: edge.From,
								To:   edge.To,
							})
						}
						return portal.ProjectionGraph{
							Address: address,
							Plane:   projection.Plane,
							Nodes:   nodes,
							Edges:   edges,
						}, true
					}
				}
				return portal.ProjectionGraph{}, false
			},
			ExplorerBus:    gateway.Bus,
			ExplorerSource: cfg.ScanSource,
			// Wire the in-process L7 catalog sub-server (M5_PORTAL).
			// mcpServer.EbusStandardServer() returns the same instance
			// RegisterEbusStandardTools installed inside mcp.NewServer;
			// sharing it between MCP + portal surfaces guarantees both
			// reach the identical SHA256-pinned embedded catalog. Nil
			// here would make /api/v1/ebus-standard/* routes 404 in
			// production (handler.go nil-guard) — see PR #507.
			EbusStandardServer: mcpServer.EbusStandardServer(),
		})
		mux.Handle(portalPath+"/", http.StripPrefix(portalPath, portalHandler))
		mux.HandleFunc(portalPath, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, portalPath+"/", http.StatusMovedPermanently)
		})
	}

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return nil, nil, err
	}

	server := &http.Server{
		Handler: mux,
	}

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("http server error: %v", err)
		}
	}()

	var advertiser mdns.Advertiser
	if cfg.MDNSAdvertise {
		port := listener.Addr().(*net.TCPAddr).Port
		advertiser, err = mdns.Advertise(ctx, mdns.Service{
			Instance: cfg.MDNSInstance,
			Service:  mdns.ServiceTypeGateway,
			Port:     port,
			Text:     gatewayMDNSText(cfg),
		})
		if err != nil {
			_ = server.Close()
			return nil, nil, err
		}
	}

	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	return server, advertiser, nil
}

func normalizeMountPath(path string, fallback string) string {
	normalized := strings.TrimSpace(path)
	if normalized == "" {
		normalized = fallback
	}
	if !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	if normalized != "/" {
		normalized = strings.TrimRight(normalized, "/")
	}
	if normalized == "/" {
		return fallback
	}
	return normalized
}

func mapPortalZones(zones []graphql.Zone) []portal.SemanticZone {
	if len(zones) == 0 {
		return nil
	}
	items := make([]portal.SemanticZone, 0, len(zones))
	for _, zone := range zones {
		items = append(items, portal.SemanticZone{
			ID:   zone.ID,
			Name: zone.Name,
			State: portal.SemanticZoneState{
				CurrentTempC:       cloneFloatPtr(zone.State.CurrentTempC),
				CurrentHumidityPct: cloneFloatPtr(zone.State.CurrentHumidityPct),
				HvacAction:         zone.State.HvacAction,
				SpecialFunction:    zone.State.SpecialFunction,
				HeatingDemandPct:   cloneFloatPtr(zone.State.HeatingDemandPct),
				ValvePositionPct:   cloneFloatPtr(zone.State.ValvePositionPct),
			},
			Config: portal.SemanticZoneConfig{
				OperatingMode:              zone.Config.OperatingMode,
				Preset:                     zone.Config.Preset,
				TargetTempC:                cloneFloatPtr(zone.Config.TargetTempC),
				AllowedModes:               append([]string(nil), zone.Config.AllowedModes...),
				CircuitType:                zone.Config.CircuitType,
				AssociatedCircuit:          cloneIntPtr(zone.Config.AssociatedCircuit),
				RoomTemperatureZoneMapping: cloneIntPtr(zone.Config.RoomTemperatureZoneMapping),
			},
		})
	}
	return items
}

func mapPortalDHW(status *graphql.DhwStatus) *portal.SemanticDHW {
	if status == nil {
		return nil
	}
	return &portal.SemanticDHW{
		State: portal.SemanticDhwState{
			CurrentTempC:     cloneFloatPtr(status.State.CurrentTempC),
			SpecialFunction:  status.State.SpecialFunction,
			HeatingDemandPct: cloneFloatPtr(status.State.HeatingDemandPct),
		},
		Config: portal.SemanticDhwConfig{
			OperatingMode: status.Config.OperatingMode,
			Preset:        status.Config.Preset,
			TargetTempC:   cloneFloatPtr(status.Config.TargetTempC),
		},
	}
}

func mapPortalEnergyTotals(value *graphql.EnergyTotals) *portal.SemanticEnergyTotals {
	if value == nil {
		return nil
	}
	return &portal.SemanticEnergyTotals{
		Gas:      mapPortalEnergyChannel(value.Gas),
		Electric: mapPortalEnergyChannel(value.Electric),
		Solar:    mapPortalEnergyChannel(value.Solar),
	}
}

func mapPortalEnergyChannel(channel graphql.EnergyChannel) portal.SemanticEnergyChannel {
	return portal.SemanticEnergyChannel{
		DHW:     mapPortalEnergySeries(channel.DHW),
		Climate: mapPortalEnergySeries(channel.Climate),
	}
}

func mapPortalEnergySeries(series graphql.EnergySeries) portal.SemanticEnergySeries {
	out := portal.SemanticEnergySeries{
		Today:     series.Today,
		TodayMeta: mapPortalEnergyPointMeta(series.TodayMeta),
	}
	if len(series.Yearly) > 0 {
		out.Yearly = append([]float64(nil), series.Yearly...)
	}
	if len(series.Monthly) > 0 {
		out.Monthly = append([]float64(nil), series.Monthly...)
	}
	if len(series.YearlyMeta) > 0 {
		out.YearlyMeta = make([]portal.SemanticEnergyPointMeta, len(series.YearlyMeta))
		for i, meta := range series.YearlyMeta {
			out.YearlyMeta[i] = mapPortalEnergyPointMeta(meta)
		}
	}
	if len(series.MonthlyMeta) > 0 {
		out.MonthlyMeta = make([]portal.SemanticEnergyPointMeta, len(series.MonthlyMeta))
		for i, meta := range series.MonthlyMeta {
			out.MonthlyMeta[i] = mapPortalEnergyPointMeta(meta)
		}
	}
	return out
}

func mapPortalEnergyPointMeta(meta graphql.EnergyPointMeta) portal.SemanticEnergyPointMeta {
	return portal.SemanticEnergyPointMeta{
		FreshnessState:  string(meta.FreshnessState),
		Provenance:      string(meta.Provenance),
		LastObservedUTC: meta.LastObservedUTC,
		AgeSeconds:      meta.AgeSeconds,
		Stale:           meta.Stale,
	}
}

func mapPortalBoilerStatus(status *graphql.BoilerStatus) *portal.SemanticBoilerStatus {
	if status == nil {
		return nil
	}
	return &portal.SemanticBoilerStatus{
		State: portal.SemanticBoilerState{
			FlowTemperatureC:         cloneFloatPtr(status.State.FlowTemperatureC),
			ReturnTemperatureC:       cloneFloatPtr(status.State.ReturnTemperatureC),
			CentralHeatingPumpActive: cloneBoolPtr(status.State.CentralHeatingPumpActive),
			WaterPressureBar:         cloneFloatPtr(status.State.WaterPressureBar),
			ExternalPumpActive:       cloneBoolPtr(status.State.ExternalPumpActive),
			CirculationPumpActive:    cloneBoolPtr(status.State.CirculationPumpActive),
			GasValveActive:           cloneBoolPtr(status.State.GasValveActive),
			FlameActive:              cloneBoolPtr(status.State.FlameActive),
			DiverterValvePositionPct: cloneFloatPtr(status.State.DiverterValvePositionPct),
			FanSpeedRpm:              cloneIntPtr(status.State.FanSpeedRpm),
			TargetFanSpeedRpm:        cloneIntPtr(status.State.TargetFanSpeedRpm),
			IonisationVoltageUa:      cloneFloatPtr(status.State.IonisationVoltageUa),
			DhwWaterFlowLpm:          cloneFloatPtr(status.State.DhwWaterFlowLpm),
			DhwDemandActive:          cloneBoolPtr(status.State.DhwDemandActive),
			HeatingSwitchActive:      cloneBoolPtr(status.State.HeatingSwitchActive),
			StorageLoadPumpPct:       cloneFloatPtr(status.State.StorageLoadPumpPct),
			ModulationPct:            cloneFloatPtr(status.State.ModulationPct),
			PrimaryCircuitFlowLpm:    cloneFloatPtr(status.State.PrimaryCircuitFlowLpm),
			FlowTempDesiredC:         cloneFloatPtr(status.State.FlowTempDesiredC),
			DhwTempDesiredC:          cloneFloatPtr(status.State.DhwTempDesiredC),
			StateNumber:              cloneIntPtr(status.State.StateNumber),
			DhwTemperatureC:          cloneFloatPtr(status.State.DhwTemperatureC),
			DhwTargetTemperatureC:    cloneFloatPtr(status.State.DhwTargetTemperatureC),
		},
		Config: portal.SemanticBoilerConfig{
			DhwOperatingMode: cloneStringPtr(status.Config.DhwOperatingMode),
			FlowsetHcMaxC:    cloneFloatPtr(status.Config.FlowsetHcMaxC),
			FlowsetHwcMaxC:   cloneFloatPtr(status.Config.FlowsetHwcMaxC),
			PartloadHcKW:     cloneFloatPtr(status.Config.PartloadHcKW),
			PartloadHwcKW:    cloneFloatPtr(status.Config.PartloadHwcKW),
		},
		Diagnostics: portal.SemanticBoilerDiagnostics{
			HeatingStatusRaw:         cloneIntPtr(status.Diagnostics.HeatingStatusRaw),
			DhwStatusRaw:             cloneIntPtr(status.Diagnostics.DhwStatusRaw),
			CentralHeatingHours:      cloneFloatPtr(status.Diagnostics.CentralHeatingHours),
			DhwHours:                 cloneFloatPtr(status.Diagnostics.DhwHours),
			CentralHeatingStarts:     cloneIntPtr(status.Diagnostics.CentralHeatingStarts),
			DhwStarts:                cloneIntPtr(status.Diagnostics.DhwStarts),
			PumpHours:                cloneFloatPtr(status.Diagnostics.PumpHours),
			FanHours:                 cloneFloatPtr(status.Diagnostics.FanHours),
			DeactivationsIFC:         cloneIntPtr(status.Diagnostics.DeactivationsIFC),
			DeactivationsTemplimiter: cloneIntPtr(status.Diagnostics.DeactivationsTemplimiter),
		},
	}
}

func mapPortalSystemStatus(status *graphql.SystemStatus) *portal.SemanticSystemStatus {
	if status == nil {
		return nil
	}
	return &portal.SemanticSystemStatus{
		State: portal.SemanticSystemState{
			SystemOff:                    cloneBoolPtr(status.State.SystemOff),
			SystemWaterPressure:          cloneFloatPtr(status.State.SystemWaterPressure),
			SystemFlowTemperature:        cloneFloatPtr(status.State.SystemFlowTemperature),
			OutdoorTemperature:           cloneFloatPtr(status.State.OutdoorTemperature),
			OutdoorTemperatureAvg24h:     cloneFloatPtr(status.State.OutdoorTemperatureAvg24h),
			MaintenanceDue:               cloneBoolPtr(status.State.MaintenanceDue),
			HwcCylinderTemperatureTop:    cloneFloatPtr(status.State.HwcCylinderTemperatureTop),
			HwcCylinderTemperatureBottom: cloneFloatPtr(status.State.HwcCylinderTemperatureBottom),
		},
		Config: portal.SemanticSystemConfig{
			AdaptiveHeatingCurve:         cloneBoolPtr(status.Config.AdaptiveHeatingCurve),
			AlternativePoint:             cloneFloatPtr(status.Config.AlternativePoint),
			HeatingCircuitBivalencePoint: cloneFloatPtr(status.Config.HeatingCircuitBivalencePoint),
			DhwBivalencePoint:            cloneFloatPtr(status.Config.DhwBivalencePoint),
			HcEmergencyTemperature:       cloneFloatPtr(status.Config.HcEmergencyTemperature),
			HwcMaxFlowTempDesired:        cloneFloatPtr(status.Config.HwcMaxFlowTempDesired),
			MaxRoomHumidity:              cloneIntPtr(status.Config.MaxRoomHumidity),
		},
		Properties: portal.SemanticSystemProperties{
			SystemScheme:            cloneIntPtr(status.Properties.SystemScheme),
			ModuleConfigurationVR71: cloneIntPtr(status.Properties.ModuleConfigurationVR71),
		},
	}
}

func mapPortalCircuits(circuits []graphql.CircuitStatus) []portal.SemanticCircuit {
	if len(circuits) == 0 {
		return nil
	}
	items := make([]portal.SemanticCircuit, 0, len(circuits))
	for _, circuit := range circuits {
		items = append(items, portal.SemanticCircuit{
			Index:       circuit.Index,
			CircuitType: circuit.CircuitType,
			HasMixer:    circuit.HasMixer,
			State: portal.SemanticCircuitState{
				PumpActive:       cloneBoolPtr(circuit.State.PumpActive),
				MixerPositionPct: cloneFloatPtr(circuit.State.MixerPositionPct),
				FlowTemperatureC: cloneFloatPtr(circuit.State.FlowTemperatureC),
				FlowSetpointC:    cloneFloatPtr(circuit.State.FlowSetpointC),
				CalcFlowTempC:    cloneFloatPtr(circuit.State.CalcFlowTempC),
				CircuitState:     circuit.State.CircuitState,
				Humidity:         cloneFloatPtr(circuit.State.Humidity),
				DewPoint:         cloneFloatPtr(circuit.State.DewPoint),
				PumpHours:        cloneFloatPtr(circuit.State.PumpHours),
				PumpStarts:       cloneIntPtr(circuit.State.PumpStarts),
			},
			Config: portal.SemanticCircuitConfig{
				HeatingCurve:    cloneFloatPtr(circuit.Config.HeatingCurve),
				FlowTempMaxC:    cloneFloatPtr(circuit.Config.FlowTempMaxC),
				FlowTempMinC:    cloneFloatPtr(circuit.Config.FlowTempMinC),
				SummerLimitC:    cloneFloatPtr(circuit.Config.SummerLimitC),
				FrostProtC:      cloneFloatPtr(circuit.Config.FrostProtC),
				RoomTempControl: circuit.Config.RoomTempControl,
				CoolingEnabled:  cloneBoolPtr(circuit.Config.CoolingEnabled),
			},
			ManagingDevice: portal.SemanticManagingDevice{
				Role:     string(circuit.ManagingDevice.Role),
				DeviceID: cloneStringPtr(circuit.ManagingDevice.DeviceID),
				Address:  cloneIntPtr(circuit.ManagingDevice.Address),
			},
		})
	}
	return items
}

func mapPortalRadioDevices(devices []graphql.RadioDevice) []portal.SemanticRadioDevice {
	if len(devices) == 0 {
		return nil
	}
	items := make([]portal.SemanticRadioDevice, 0, len(devices))
	for _, device := range devices {
		items = append(items, portal.SemanticRadioDevice{
			Group:                device.Group,
			Instance:             device.Instance,
			SlotMode:             device.SlotMode,
			DeviceConnected:      cloneBoolPtr(device.DeviceConnected),
			DeviceClassAddress:   cloneIntPtr(device.DeviceClassAddress),
			DeviceModel:          device.DeviceModel,
			FirmwareVersion:      cloneStringPtr(device.FirmwareVersion),
			HardwareIdentifier:   cloneIntPtr(device.HardwareIdentifier),
			RemoteControlAddress: cloneIntPtr(device.RemoteControlAddress),
			DevicePaired:         cloneBoolPtr(device.DevicePaired),
			ReceptionStrength:    cloneIntPtr(device.ReceptionStrength),
			ZoneAssignment:       cloneIntPtr(device.ZoneAssignment),
			RoomTemperatureC:     cloneFloatPtr(device.RoomTemperatureC),
			RoomHumidityPct:      cloneFloatPtr(device.RoomHumidityPct),
		})
	}
	return items
}

func mapPortalSolarStatus(status *graphql.SolarStatus) *portal.SemanticSolarStatus {
	if status == nil {
		return nil
	}
	return &portal.SemanticSolarStatus{
		CollectorTemperatureC: cloneFloatPtr(status.CollectorTemperatureC),
		ReturnTemperatureC:    cloneFloatPtr(status.ReturnTemperatureC),
		PumpActive:            cloneBoolPtr(status.PumpActive),
		CurrentYield:          cloneFloatPtr(status.CurrentYield),
		PumpHours:             cloneFloatPtr(status.PumpHours),
		SolarEnabled:          cloneBoolPtr(status.SolarEnabled),
		FunctionMode:          cloneBoolPtr(status.FunctionMode),
	}
}

func mapPortalCylinders(cylinders []graphql.CylinderStatus) []portal.SemanticCylinder {
	if len(cylinders) == 0 {
		return nil
	}
	items := make([]portal.SemanticCylinder, 0, len(cylinders))
	for _, cylinder := range cylinders {
		items = append(items, portal.SemanticCylinder{
			Index:             cylinder.Index,
			TemperatureC:      cloneFloatPtr(cylinder.TemperatureC),
			MaxSetpointC:      cloneFloatPtr(cylinder.MaxSetpointC),
			ChargeHysteresisC: cloneFloatPtr(cylinder.ChargeHysteresisC),
			ChargeOffsetC:     cloneFloatPtr(cylinder.ChargeOffsetC),
		})
	}
	return items
}

func mapPortalAdapterInfo(info *graphql.AdapterHardwareInfo) *portal.SemanticAdapterInfo {
	if info == nil {
		return nil
	}
	result := &portal.SemanticAdapterInfo{
		FirmwareVersion:    info.FirmwareVersion,
		FirmwareChecksum:   info.FirmwareChecksum,
		BootloaderVersion:  info.BootloaderVersion,
		BootloaderChecksum: info.BootloaderChecksum,
		HardwareID:         info.HardwareID,
		HardwareConfig:     info.HardwareConfig,
		Features:           info.Features,
		Jumpers:            info.Jumpers,
		IsWiFi:             info.IsWiFi,
		IsEthernet:         info.IsEthernet,
		VersionResponseLen: info.VersionResponseLen,
		InfoSupported:      info.InfoSupported,
		TemperatureC:       cloneFloatPtr(info.TemperatureC),
		SupplyVoltageMV:    cloneIntPtr(info.SupplyVoltageMV),
		BusVoltageMaxDV:    cloneIntPtr(info.BusVoltageMaxDV),
		BusVoltageMinDV:    cloneIntPtr(info.BusVoltageMinDV),
		ResetCause:         cloneStringPtr(info.ResetCause),
		WiFiRSSIDBm:        cloneIntPtr(info.WiFiRSSIDBm),
	}
	if info.JumperFlags != nil {
		result.JumperFlags = make([]string, len(info.JumperFlags))
		copy(result.JumperFlags, info.JumperFlags)
	}
	if info.ResetCauseCode != nil {
		code := int(*info.ResetCauseCode)
		result.ResetCauseCode = &code
	}
	if info.RestartCount != nil {
		count := int(*info.RestartCount)
		result.RestartCount = &count
	}
	if info.LastIdentityQuery != nil {
		result.LastIdentityQuery = info.LastIdentityQuery.Format(time.RFC3339)
	}
	if info.LastTelemetryQuery != nil {
		result.LastTelemetryQuery = info.LastTelemetryQuery.Format(time.RFC3339)
	}
	return result
}

// buildResponderCapabilityProvider composes the `meta.capabilities.responder`
// signal for the active transport per decision doc @ 567a6798 §4.2 + §5.
//
// Mapping (v1.1, locked):
//   - ENH / ENS → active.scope = "partial", surfaces = FF_03..FF_06,
//     refusal = nil. transports[] reports the same row as "supported"
//     and the ebusd-tcp row as perpetually "blocked".
//   - ebusd-tcp → active.scope = "none", surfaces = [],
//     refusal.code = "command_bridge_no_companion_listen". Consumers
//     apply fail-closed per §4.3 rule 4.
//   - Any other transport (udp-plain, tcp-plain, adapter-direct, empty,
//     or unrecognised string) → returns nil. The caller omits the
//     capability entirely and consumers fall back to §4.3 rule 1
//     (absence → scope=none, fail-closed). This is the only way to
//     preserve invariant I1 (exactly three rows at v1.1) AND I2
//     (active.transport MUST appear in transports[]) simultaneously,
//     because emitting a non-canonical active.transport literal would
//     violate I2 and widening transports[] with a fourth "unknown" row
//     would violate I1.
//
// The raw cfg.TransportConfig.Protocol is first canonicalised via
// ebusgateway.canonicalTransportProtocol (which handles the "ebusd"
// alias → "ebusd-tcp" and lowercases/trims whitespace). This ensures
// downstream envelope assertions see the canonical enum literal in
// every surface, matching the three fixed transports[] rows byte-for-
// byte.
//
// Invariants I1/I7 are enforced by always emitting exactly three
// transport rows (ENH, ENS, ebusd-tcp) in a fixed order. I2/I3 are
// enforced by deriving active.transport from the same canonical enum
// literals the rows use.
//
// Runtime-transport authority (Codex P1 on PR #509): the canonical
// protocol enum alone is NOT sufficient to decide active.scope. The
// adapter-direct URI mode (--address=adapter-direct://...) keeps
// TransportConfig.Protocol as the default "enh" string while the live
// transport instance is actually the adapter-direct mux's active path.
// That path implements transport.RawTransport but does NOT implement
// transport.ResponderTransport — it has no SendResponderBytes primitive.
// Reporting active.scope="partial" purely from the config string would
// over-advertise responder support for FF_03..FF_06 surfaces the gateway
// cannot actually emit. Accordingly the provider type-asserts the live
// transport instance against ebusgoTransport.ResponderTransport; when
// the canonical protocol is ENH/ENS but the instance does NOT satisfy
// the interface, we downgrade to scope="none" with
// refusal.code="transport_mux_bypass" AND rewrite both ENH and ENS
// rows in transports[] to state="blocked", scope="none",
// reason="transport_mux_bypass" so invariant I3 (active.scope ==
// matching row.scope) holds — the mux is a shared runtime wrapper
// above either upstream, so switching the canonical protocol would
// not restore responder emission. ebusd-tcp path is unchanged —
// it is forbidden from responder emission per M4b2 §3 regardless of
// what underlying transport type ebusd is wrapped by.
//
// actualTransport is the live instance returned by the bootstrap
// transport factory. A nil value means the caller has not yet wired a
// transport (legacy callers, some unit-test paths); in that case the
// provider falls back to protocol-only inference to preserve previous
// behaviour on paths that never exercise the adapter-direct mux.
func buildResponderCapabilityProvider(cfg ebusgateway.Config, actualTransport ebusgoTransport.RawTransport) ebus_standard.ResponderCapabilityProvider {
	surfacesFF := []string{"FF_03", "FF_04", "FF_05", "FF_06"}
	// transports[] is static at v1.1 — same three rows on every gateway
	// regardless of deployment (I1 locks the count at exactly three).
	transports := []ebus_standard.TransportRow{
		{Transport: "ENH", State: "supported", Scope: "partial", Surfaces: surfacesFF, Reason: ""},
		{Transport: "ENS", State: "supported", Scope: "partial", Surfaces: surfacesFF, Reason: ""},
		{Transport: "ebusd-tcp", State: "blocked", Scope: "none", Surfaces: []string{}, Reason: "command_bridge_no_companion_listen"},
	}

	// Runtime transport authority: a non-nil actualTransport that does
	// NOT satisfy ResponderTransport means the live bus path cannot
	// actually emit responder bytes (the adapter-direct mux is the
	// concrete case today). A nil actualTransport means bootstrap is
	// still pre-wiring (e.g. unit tests that only construct Config); in
	// that case fall back to protocol-only inference so legacy callers
	// keep their behaviour.
	transportKnown := actualTransport != nil
	_, responderCapable := actualTransport.(ebusgoTransport.ResponderTransport)
	muxBypass := transportKnown && !responderCapable
	muxBypassRefusal := &ebus_standard.ActiveRefusal{
		Code:   "transport_mux_bypass",
		Reason: "runtime transport does not satisfy ResponderTransport (e.g. adapter-direct mux)",
	}

	// Invariant I3 (decision doc §4.4): active.scope MUST equal
	// transports[x].scope where x.transport == active.transport. When the
	// runtime mux bypass is in effect we therefore also rewrite the
	// matching transports[] row(s) — otherwise a consumer joining
	// active.transport → transports[] would see contradictory metadata
	// (active.scope=none but row.scope=partial).
	//
	// Interpretation A (shared-runtime downgrade): the adapter-direct mux
	// is a single wrapper that sits above whichever ENH/ENS upstream the
	// operator configured. The mux instance itself does not satisfy
	// ResponderTransport, so switching the canonical protocol from ENH to
	// ENS (or vice versa) under the same mux would not restore responder
	// emission — the bypass is shared. Accordingly, BOTH the ENH and ENS
	// rows downgrade to state=blocked, scope=none, reason="transport_mux_bypass".
	// The ebusd-tcp row is left untouched (it is always blocked with its
	// own reason "command_bridge_no_companion_listen" per §4.2/§3).
	// Invariants preserved: I1 (still exactly 3 rows), I2 (active.transport
	// still appears verbatim), I3 (active.scope == matching row.scope),
	// I5 (state=blocked ⇒ reason != ""), I7 (row order ENH,ENS,ebusd-tcp
	// unchanged).
	if muxBypass {
		for i := range transports {
			row := &transports[i]
			if row.Transport == "ENH" || row.Transport == "ENS" {
				row.State = "blocked"
				row.Scope = "none"
				row.Surfaces = []string{}
				row.Reason = "transport_mux_bypass"
			}
		}
	}

	var active ebus_standard.ActiveResponder
	// Canonicalise raw TransportProtocol (handles "ebusd" alias, case,
	// whitespace) into one of the enum constants so active.transport
	// literally equals the transports[] row label (invariant I2).
	switch ebusgateway.CanonicalTransportProtocol(cfg.TransportConfig.Protocol) {
	case ebusgateway.TransportENH:
		if muxBypass {
			active = ebus_standard.ActiveResponder{Transport: "ENH", Scope: "none", Surfaces: []string{}, Refusal: muxBypassRefusal}
		} else {
			active = ebus_standard.ActiveResponder{Transport: "ENH", Scope: "partial", Surfaces: surfacesFF}
		}
	case ebusgateway.TransportENS:
		if muxBypass {
			active = ebus_standard.ActiveResponder{Transport: "ENS", Scope: "none", Surfaces: []string{}, Refusal: muxBypassRefusal}
		} else {
			active = ebus_standard.ActiveResponder{Transport: "ENS", Scope: "partial", Surfaces: surfacesFF}
		}
	case ebusgateway.TransportEbusdTCP:
		active = ebus_standard.ActiveResponder{
			Transport: "ebusd-tcp",
			Scope:     "none",
			Surfaces:  []string{},
			Refusal:   &ebus_standard.ActiveRefusal{Code: "command_bridge_no_companion_listen", Reason: "ebusd command bridge does not expose a responder-role emission primitive"},
		}
	default:
		// Non-enumerated transports (udp-plain, tcp-plain,
		// adapter-direct, empty, or entirely unknown). Fail-closed:
		// return nil so the envelope omits meta.capabilities.responder
		// entirely (§4.3 rule 1). Emitting the raw string would
		// violate I2; adding a fourth transports[] row would violate
		// I1. Absence is the only invariant-preserving outcome.
		return nil
	}

	cap := ebus_standard.ResponderCapability{
		Version:    "v1",
		Active:     active,
		Transports: transports,
	}
	return func() ebus_standard.ResponderCapability { return cap }
}

// applyStaticSeedTable plants the productids static seed entries into
// the registry. Each seed entry contributes one DeviceInfo per address
// with full Vaillant identity (Manufacturer + DeviceID), allowing the
// registry's identity-merge contract to collapse canonical-pair faces
// into a single entry. SerialNumber and version fields are
// intentionally empty — they will be populated by subsequent active
// enrichment (P5 follow-up) or remain empty for seed-only addresses
// (e.g. NETX3 broadcast face 0x04 which does not respond to active
// probes).
//
// Phase post-C P3 (live validation 2026-05-08): NETX3's 0x04 face was
// absent from the registry entirely because broadcast-source frames
// never carry an ACKCorrelation that would feed the inserter. Static
// seed bypasses that gate at startup.
//
// P3.5 (Codex P2 follow-up, ebusreg PR #137): switches the per-address
// call from registry.Register (which stamps the AddressSlot with
// DiscoverySourceActiveConfirmed/VerificationStateIdentityConfirmed —
// wrong observability label for a pre-known seed entry) to
// registry.RegisterStaticSeed (which stamps DiscoverySourceStaticSeed
// /VerificationStateCandidate). Operators reading
// `ebus.v1.registry.devices.list` or the address-table snapshot via
// MCP/JSON now correctly see seeded addresses labelled
// `discovery_source: "static_seed"`, `verification: "candidate"`
// instead of pretending the gateway actively confirmed them at boot.
//
// Role mapping happens at this seam (gateway), not in productids or
// registry: productids.SeedAddressEntry.Role is a free-form string
// (`"initiator"` / `"target"`); registry.SlotRole is a typed enum
// (SlotRoleMaster / SlotRoleSlave / SlotRoleUnknown). Unknown role
// strings fall through to SlotRoleUnknown — registry's monotonic Role
// guard then leaves Role empty until passive observation or active
// scan fills it in.
func applyStaticSeedTable(reg *registry.DeviceRegistry) {
	seeds := productids.LoadSeedTable(true)
	if len(seeds) == 0 {
		return
	}
	now := time.Now()
	count := 0
	for _, seed := range seeds {
		for _, addr := range seed.Addresses {
			role := registry.SlotRoleUnknown
			switch addr.Role {
			case "initiator":
				role = registry.SlotRoleMaster
			case "target":
				role = registry.SlotRoleSlave
			}
			info := registry.DeviceInfo{
				Address:      addr.Addr,
				Manufacturer: seed.Manufacturer,
				DeviceID:     seed.DeviceID,
			}
			reg.RegisterStaticSeed(info, role, now)
			count++
		}
	}
	log.Printf("static seed table: planted %d address(es) across %d device(s) at startup (source=productids.LoadSeedTable, label=static_seed/candidate)", count, len(seeds))
}

// initRuntimeStateManager constructs and starts the runtime-state Manager,
// wiring AD08 eager-persist for the instance-guid CLI flag (when present)
// and exposing the loaded state for hint extraction.
//
// On Manager.Load failure (missing / corrupt file), Manager.Load returns an
// empty state and the gateway continues without a hint — the cache is a
// best-effort optimisation, not a startup requirement (AD11 + M2 spec).
func initRuntimeStateManager(ctx context.Context, cfg ebusgateway.Config) (*runtimestate.Manager, *runtimestate.State) {
	mgr := runtimestate.New(runtimestate.Options{
		Path:         cfg.RuntimeStatePath,
		GatewayBuild: fmt.Sprintf("%s+%s", buildVersion, buildID),
		AddonVersion: "", // populated by add-on via future flag if needed
	})
	state, err := mgr.Load(ctx)
	if err != nil {
		log.Printf("runtime_state: load returned error (continuing with empty state): %v", err)
		state = &runtimestate.State{}
	}

	// AD08 eager persist: when -instance-guid is supplied, durably write
	// meta.{schema_version, instance_guid, written_at} within ~1 s so the
	// crash-before-first-periodic-persist window is closed. Provenance per
	// AD27.
	if cfg.InstanceGUID != "" {
		source := identitySourceFromCfg(cfg.InstanceGUIDSource)
		if perr := mgr.EagerPersistInstanceGUID(ctx, cfg.InstanceGUID, source); perr != nil {
			log.Printf("runtime_state: eager-persist instance_guid failed (continuing): %v", perr)
		}
	}

	if serr := mgr.Start(ctx); serr != nil {
		log.Printf("runtime_state: start failed (continuing without periodic persister): %v", serr)
	}

	return mgr, state
}

// identitySourceFromCfg maps the CLI -instance-guid-source flag value to the
// runtimestate.IdentitySource enum, with AD27 deprecation log when the flag
// is absent. The well-formed value set is enforced here; unknown values fall
// back to "cli-override" with a warning rather than failing startup.
func identitySourceFromCfg(raw string) runtimestate.IdentitySource {
	switch raw {
	case string(runtimestate.IdentitySourceRuntimeState):
		return runtimestate.IdentitySourceRuntimeState
	case string(runtimestate.IdentitySourceLegacyMigrated):
		return runtimestate.IdentitySourceLegacyMigrated
	case string(runtimestate.IdentitySourceGenerated):
		return runtimestate.IdentitySourceGenerated
	case string(runtimestate.IdentitySourceCLIOverride):
		return runtimestate.IdentitySourceCLIOverride
	case "":
		log.Print("runtime_state: -instance-guid-source absent; defaulting to cli-override (deprecated; pass -instance-guid-source explicitly)")
		return runtimestate.IdentitySourceCLIOverride
	default:
		log.Printf("runtime_state: -instance-guid-source=%q is not a recognised AD27 value; treating as cli-override", raw)
		return runtimestate.IdentitySourceCLIOverride
	}
}
