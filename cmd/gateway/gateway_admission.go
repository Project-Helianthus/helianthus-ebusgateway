package main

import (
	"context"
	"strings"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

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
	// Resolve a valid URI override once at the process integration boundary so
	// every downstream admission/source/evidence decision sees the same
	// canonical transport tuple as the driver factory. Invalid descriptors stay
	// untouched and are classified driver-locally by DriverManager.
	if normalized, err := ebusgateway.NormalizeEBusDriverTransportConfig(cfg.TransportConfig); err == nil {
		cfg.TransportConfig = normalized
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
