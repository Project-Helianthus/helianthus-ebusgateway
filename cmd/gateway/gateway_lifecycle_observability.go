package main

import (
	"context"
	"expvar"
	"fmt"
	"log"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// The lifecycle composition owns the startup source-selection setup and the
// bus-observability providers through the functions below. runGatewayLifecycle
// calls each of them at the point where its statements are required, so the
// order of logs, metrics, provider installation and artifact emission is the
// order of those calls.

// startupSourceSelectionArtifactPath is the file the startup source-selection
// artifact is written to, once after the startup delay and once at shutdown.
const startupSourceSelectionArtifactPath = "/tmp/helianthus-source-selection-artifact.json"

// startupSourceSelection carries the startup source-selection values that the
// lifecycle and its admitted generations read.
type startupSourceSelection struct {
	admissionPath   ebusgateway.TransportAdmissionPath
	metrics         *ebusgateway.StartupSourceSelectionMetrics
	artifactBuilder *ebusgateway.SourceSelectionArtifactBuilder
	overrideSet     bool
	overrideSource  byte
}

// newStartupSourceSelection resolves the admission path, records the explicit
// source override and prepares the source-selection artifact builder.
//
// ResolveAdmissionPath centralises the adapter-direct to source-selection
// special case so every classifier call site agrees on the dispatch.
// Adapter-direct multiplexer mode always wraps a source-selection-capable
// underlying transport (ENH/ENS), so the source-address selector runs through
// the shared PassiveTransactionReconstructor regardless of multiplexer
// presence.
func newStartupSourceSelection(cfg ebusgateway.Config) (startupSourceSelection, error) {
	var selection startupSourceSelection
	admissionPath, adapterDirectSpecialCased := ebusgateway.ResolveAdmissionPath(cfg.TransportConfig.Protocol)
	if adapterDirectSpecialCased {
		log.Printf("startup source selection: adapter-direct multiplexer detected; treating as source-selection-capable (underlying transport is always ENH/ENS)")
	} else if admissionPath == ebusgateway.TransportAdmissionStaticFallback && cfg.TransportConfig.Protocol != ebusgateway.TransportEbusdTCP {
		log.Printf("startup source selection: classifier produced static-fallback for transport protocol=%q (unknown/empty)", cfg.TransportConfig.Protocol)
	}
	metrics := ebusgateway.GetOrInitStartupSourceSelectionMetrics()
	artifactBuilder := ebusgateway.NewSourceSelectionArtifactBuilder(string(cfg.TransportConfig.Protocol))
	if err := artifactBuilder.SetSourceSelectionMode("degraded_no_events"); err != nil {
		return selection, fmt.Errorf("set startup source-selection mode: %w", err)
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
			return selection, fmt.Errorf("set explicit source-selection mode: %w", err)
		}
		// Exact source is configured: state immediately becomes "active" because
		// the explicit source is in use from the first active frame. The selector
		// may still run advisory-only under Validate=true but does not gate.
		artifactBuilder.SetActiveExplicitSource(overrideSource)
	} else {
		metrics.SetExplicitSourceActive(false)
	}
	selection = startupSourceSelection{
		admissionPath:   admissionPath,
		metrics:         metrics,
		artifactBuilder: artifactBuilder,
		overrideSet:     overrideSet,
		overrideSource:  overrideSource,
	}
	return selection, nil
}

// startArtifactEmission writes the artifact once, 60 seconds after startup,
// unless the process context ends first.
func (selection startupSourceSelection) startArtifactEmission(ctx context.Context) {
	artifactBuilder := selection.artifactBuilder
	go func() {
		timer := time.NewTimer(60 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := artifactBuilder.EmitToFile(startupSourceSelectionArtifactPath); err != nil {
				log.Printf("startup source selection artifact emit: %v", err)
			}
		}
	}()
}

// emitFinalArtifact writes the artifact at shutdown.
func (selection startupSourceSelection) emitFinalArtifact() {
	if err := selection.artifactBuilder.EmitToFile(startupSourceSelectionArtifactPath); err != nil {
		log.Printf("startup source selection artifact emit: %v", err)
	}
}

// wireSemanticMetricsProvider installs the SemReg metrics provider before the
// HTTP control plane starts when a Modbus, Growatt or EVSE domain is
// configured. The callback reaches only immutable SemReg current views; it
// cannot invoke the Growatt observe/publish path or any native transport
// operation.
func wireSemanticMetricsProvider(busObservability *ebusgateway.BusObservabilityStore, cfg ebusgateway.Config, bindings *managedSidecarBindings, teslaHSCRetained *teslaHSCRetainedOwner) {
	if busObservability != nil && (cfg.ModbusTCPConfig.Enabled || cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled || cfg.PrometheusEVSEEnabled) {
		busObservability.SetSemanticMetricsProvider(func(at time.Time) []ebusgateway.SemanticMetricsDomain {
			// The Tesla owner supplies only its detached retained publication. It
			// cannot acquire, publish, or execute a native operation from a scrape.
			return bindings.semanticMetrics(cfg, teslaHSCRetained, at)
		})
	}
}

// wireAdaptermuxDiagnosticProviders exposes the adapter multiplexer
// diagnostic snapshot, the arbitration hold and the escape-pair failure
// counters through the bus observability Prometheus surface. A classifier
// without the optional seams (no multiplexer) leaves the providers unset, and
// the ebus_adaptermux_syn_seen_* metrics are absent.
func wireAdaptermuxDiagnosticProviders(busObservability *ebusgateway.BusObservabilityStore, adapterClassifier activeTxnClassifier) {
	if holder, ok := adapterClassifier.(activeArbitrationHolder); ok {
		busObservability.SetActiveArbitrationHoldProvider(holder.ActiveArbitrationHoldReason)
	}
	if snap, ok := adapterClassifier.(adaptermuxDiagSnapshotter); ok {
		busObservability.SetAdaptermuxDiagProvider(func() ebusgateway.AdaptermuxDiagSnapshot {
			var s adaptermux.ActiveTxnSnapshot
			if cumulative, ok := adapterClassifier.(interface {
				CumulativeActiveTxnSnapshot() adaptermux.ActiveTxnSnapshot
			}); ok {
				s = cumulative.CumulativeActiveTxnSnapshot()
			} else {
				s = snap.ActiveTxnSnapshot()
			}
			return ebusgateway.AdaptermuxDiagSnapshot{
				SynSuppressedPreEcho:               s.SynSuppressedPreEcho,
				SynSeenDuringGrantWindow:           s.SynSeenDuringGrantWindow,
				SynSeenWhileInterWriteEmpty:        s.SynSeenWhileInterWriteEmpty,
				SynSeenAfterTransportWindowExpired: s.SynSeenAfterTransportWindowExpired,
				SynSuppressedBetweenWrites:         s.SynSuppressedBetweenWrites,
				InactiveReasonCounts:               inactiveReasonCounts(s.InactiveReasonCounts),
				StaleActiveBytesDiscarded:          s.StaleActiveBytesDiscarded,
				InterWriteDrainTotal:               s.InterWriteDrainTotal,
				ForeignInitiatorReleases:           s.ForeignInitiatorReleases,
				EscapePairLeadFailures:             s.EscapePairLeadFailures,
				EscapePairSuffixFailures:           s.EscapePairSuffixFailures,
				ActiveArbitrationHeld:              activeArbitrationHoldReason(adapterClassifier) != "",
			}
		})
		busObservability.SetEscapePairFailureGenerationProvider(func() (uint64, uint64, uint64) {
			if live, ok := adapterClassifier.(interface {
				LiveEscapePairFailures() (lead, suffix, generation uint64)
			}); ok {
				return live.LiveEscapePairFailures()
			}
			s := snap.ActiveTxnSnapshot()
			return s.EscapePairLeadFailures, s.EscapePairSuffixFailures, 0
		})
	}
}

// v8ClassifierAccessors returns the current-generation classifier callback and
// the shadow-drop counter. The counter reads 0 when the classifier is absent;
// the classifier counter has nil-receiver handling.
func v8ClassifierAccessors(adapterClassifier activeTxnClassifier) (func(func(*v8classifier.Classifier)), func() uint64) {
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
	return withClassifier, shadowDropCountFn
}

// wireV8RolloutObservability publishes the four protocol.Bus rollout counters
// and the classifier shadow-mode counter on the Prometheus surface and on
// /debug/vars, and installs the classifier callback for
// /debug/v8/admin-events. The bus counters are published on every transport;
// without an adapter-direct classifier the shadow counter reads 0 and the
// admin-events response stays empty while the HTTP contract is preserved.
func wireV8RolloutObservability(busObservability *ebusgateway.BusObservabilityStore, bus *protocol.Bus, adapterClassifier activeTxnClassifier) {
	if busObservability == nil || bus == nil {
		return
	}
	withClassifier, shadowDropCountFn := v8ClassifierAccessors(adapterClassifier)
	busObservability.SetV8RolloutProvider(func() ebusgateway.V8RolloutSnapshot {
		return ebusgateway.V8RolloutSnapshot{
			Round9AbsorbEntered:            bus.Round9AbsorbEntered(),
			PayloadAaAutoSynAbsorbed:       bus.PayloadAaAutoSynAbsorbed(),
			PayloadAaAutoSynRecovered:      bus.PayloadAaAutoSynRecovered(),
			PayloadAaAutoSynDrainExhausted: bus.PayloadAaAutoSynDrainExhausted(),
			V8ShadowWouldHaveDroppedTotal:  shadowDropCountFn(),
		}
	})
	// The /debug/vars names match the Prometheus metric names one to one.
	// expvar.Publish runs once per process (it panics on duplicate names);
	// its callbacks read the current source, which every lifecycle run
	// replaces, so both surfaces read the same bus.
	v8RolloutExpvarCurrent.Store(&v8RolloutExpvarSource{
		bus:               bus,
		shadowDropCountFn: shadowDropCountFn,
	})
	// Production operations execute inside stable driver admission, so a
	// classifier pointer cannot escape across replacement.
	v8AdminEventsCurrentClassifier.Store(nil)
	v8AdminEventsCurrentProvider.Store(&v8AdminClassifierProvider{withClassifier: withClassifier})
	publishV8RolloutExpvarsOnce()
}

// publishV8RolloutExpvarsOnce registers the /debug/vars rollout counters on
// the first call of the process.
func publishV8RolloutExpvarsOnce() {
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

// wireSourceSelectionBaselineEvidence lets the artifact emitter fill
// per_baseline_address_evidence_counts at emit time from the bus
// observability recent messages, counting each baseline address as source or
// target.
func wireSourceSelectionBaselineEvidence(artifactBuilder *ebusgateway.SourceSelectionArtifactBuilder, busObservability *ebusgateway.BusObservabilityStore) {
	if busObservability != nil {
		artifactBuilder.SetBaselineEvidenceProvider(func() map[string]int {
			counts := make(map[string]int)
			for _, addr := range ebusgateway.VaillantBaselineTopologySeed {
				counts[fmt.Sprintf("%02X", addr)] = 0
			}
			// 1024 is the default observe-first capacity, large enough to
			// capture passive traffic to every baseline address on a healthy
			// bus.
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
}

// wireSemanticStartupObservability connects the semantic provider and the bus
// observability store: the startup surface, the energy passive state and the
// energy freshness metrics.
func wireSemanticStartupObservability(busObservability *ebusgateway.BusObservabilityStore, semanticRuntime *graphql.SemanticRuntime) {
	if busObservability != nil && semanticRuntime.Provider() != nil {
		busObservability.SetStartupSurfaceProvider(func() *ebusgateway.BusObservabilityStartup {
			return semanticStartupSurface(semanticRuntime.Provider())
		})
		semanticRuntime.Provider().SetEnergyPassiveStateProvider(func() string {
			snapshot := busObservability.Snapshot()
			return snapshot.Summary.Status.Capability.PassiveState
		})
		busObservability.SetEnergyFreshnessMetricsRefresher(func(now time.Time, passiveState string) {
			semanticRuntime.Provider().RefreshEnergyFreshnessMetrics(now, passiveState)
		})
	}
}

func inactiveReasonCounts(counts map[adaptermux.ActiveTxnInactiveReason]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(counts))
	for reason, count := range counts {
		out[string(reason)] = count
	}
	return out
}

// activeArbitrationHolder is implemented by the adapter-direct classifiers
// (the stable driver classifier and *adaptermux.Mux).
type activeArbitrationHolder interface {
	ActiveArbitrationHoldReason() string
}

// activeArbitrationHoldReason returns the classifier's active arbitration hold
// reason, "" when it has none.
func activeArbitrationHoldReason(classifier activeTxnClassifier) string {
	if holder, ok := classifier.(activeArbitrationHolder); ok {
		return holder.ActiveArbitrationHoldReason()
	}
	return ""
}
