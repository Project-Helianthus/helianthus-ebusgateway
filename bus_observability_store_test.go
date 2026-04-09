package ebusgateway

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

func TestBusObservabilityStoreRecentRingEvictsOldestButCountersAdvance(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ObserveFirstRecentMessageCapacity = 2
	store := NewBusObservabilityStore(cfg)
	base := time.Now().UTC()

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x01},
	}
	response := protocol.Frame{
		Source:    0x08,
		Target:    0x10,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x02},
	}

	for index, observedAt := range []time.Time{
		base.Add(10 * time.Second),
		base.Add(20 * time.Second),
		base.Add(30 * time.Second),
	} {
		now := observedAt
		store.now = func() time.Time { return now }
		if err := store.OnBusEvent(protocol.BusEvent{
			Kind:        protocol.BusEventAttemptComplete,
			Request:     request,
			HasRequest:  true,
			Response:    response,
			HasResponse: true,
		}); err != nil {
			t.Fatalf("OnBusEvent[%d] error = %v", index, err)
		}
	}

	messages := store.RecentMessages(10)
	if len(messages) != 2 {
		t.Fatalf("RecentMessages length = %d; want 2", len(messages))
	}
	if !messages[0].ObservedAt.Equal(base.Add(20 * time.Second)) {
		t.Fatalf("RecentMessages[0].ObservedAt = %s; want 20s entry", messages[0].ObservedAt)
	}
	if !messages[1].ObservedAt.Equal(base.Add(30 * time.Second)) {
		t.Fatalf("RecentMessages[1].ObservedAt = %s; want 30s entry", messages[1].ObservedAt)
	}

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `ebus_frames_observed_total{dst="0x08",family="B524",frame_type="initiator_target",scope="active",src="0x10"} 3`) {
		t.Fatalf("RenderPrometheus missing cumulative frame counter:\n%s", metrics)
	}
	if !strings.Contains(metrics, "ebus_observability_recent_messages 2") {
		t.Fatalf("RenderPrometheus missing recent-message occupancy:\n%s", metrics)
	}
}

func TestBusObservabilityStorePeriodicityBudgetEvictsLRU(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"
	cfg.ObserveFirstPeriodicityCapacity = 1
	store := NewBusObservabilityStore(cfg)
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(30 * time.Second) }

	store.mu.Lock()
	store.passive.state = "available"
	store.passive.startupWindowClosed = true
	store.mu.Unlock()

	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(10*time.Second), 0x10, 0x08, 0xB5, 0x24))
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(20*time.Second), 0x10, 0x08, 0xB5, 0x24))
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(30*time.Second), 0x15, 0x26, 0xB5, 0x16))

	items := store.PeriodicitySnapshot()
	if len(items) != 1 {
		t.Fatalf("PeriodicitySnapshot length = %d; want 1", len(items))
	}
	if items[0].SourceBucket != "0x15" || items[0].TargetBucket != "0x26" {
		t.Fatalf("PeriodicitySnapshot[0] = %+v; want surviving tuple 0x15->0x26", items[0])
	}

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, "ebus_periodicity_tuple_budget_overflow_total 1") {
		t.Fatalf("RenderPrometheus missing periodicity overflow counter:\n%s", metrics)
	}
}

func TestBusObservabilityStorePeriodicitySnapshotOrdersSameTimestampByTuple(t *testing.T) {
	cfg := DefaultConfig()
	store := NewBusObservabilityStore(cfg)
	base := time.Now().UTC()
	store.now = func() time.Time { return base }

	store.mu.Lock()
	store.periodicity = map[periodicityKey]*BusPeriodicityEntry{
		{SourceBucket: "0x08", TargetBucket: "0x15", Primary: 0xB5, Secondary: 0x24}: {
			SourceBucket: "0x08",
			TargetBucket: "0x15",
			Primary:      0xB5,
			Secondary:    0x24,
			Family:       "B524",
			State:        "available",
			LastSeen:     base,
		},
		{SourceBucket: "0x08", TargetBucket: "0x15", Primary: 0xB5, Secondary: 0x09}: {
			SourceBucket: "0x08",
			TargetBucket: "0x15",
			Primary:      0xB5,
			Secondary:    0x09,
			Family:       "B509",
			State:        "available",
			LastSeen:     base,
		},
	}
	store.mu.Unlock()

	items := store.PeriodicitySnapshot()
	if len(items) != 2 {
		t.Fatalf("PeriodicitySnapshot length = %d; want 2", len(items))
	}
	if items[0].Secondary != 0x09 || items[1].Secondary != 0x24 {
		t.Fatalf("PeriodicitySnapshot ordering = %#v; want secondary 0x09 before 0x24", items)
	}
}

func TestBusObservabilityStoreSuppressesBusyMetricsOnEbusdTCP(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TransportConfig.Protocol = TransportEbusdTCP
	store := NewBusObservabilityStore(cfg)

	metrics := store.RenderPrometheus()
	if strings.Contains(metrics, "ebus_bus_busy_seconds_total") {
		t.Fatalf("RenderPrometheus unexpectedly exposed busy metrics for ebusd-tcp:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_observability_transport_info{scope="passive",timing_quality="unavailable",transport_class="ebusd-tcp"} 1`) {
		t.Fatalf("RenderPrometheus missing passive transport metadata:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_passive_warmup_state{state="unavailable"} 1`) {
		t.Fatalf("RenderPrometheus missing unavailable passive warmup state:\n%s", metrics)
	}
}

func TestBusObservabilityStoreMarksPassiveDisabledAsCapabilityWithdrawn(t *testing.T) {
	cfg := DefaultConfig()
	base := time.Now().UTC()
	store := NewBusObservabilityStore(cfg)
	store.now = func() time.Time {
		return base.Add(cfg.ObserveFirstWarmupOuterWindow + time.Second)
	}

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="capability_withdrawn"} 1`) {
		t.Fatalf("RenderPrometheus missing capability_withdrawn reason for disabled passive mode:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0`) {
		t.Fatalf("RenderPrometheus unexpectedly counted timeout probes for disabled passive mode:\n%s", metrics)
	}
	if strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="startup_timeout"} 1`) {
		t.Fatalf("RenderPrometheus reported startup_timeout for disabled passive mode:\n%s", metrics)
	}
}

func TestBusObservabilityStoreSnapshotDoesNotAdvanceTimestampOnRepeatedCapabilityWithdrawnReads(t *testing.T) {
	cfg := DefaultConfig()
	base := time.Now().UTC().Truncate(time.Second)
	now := base
	store := NewBusObservabilityStore(cfg)
	store.now = func() time.Time { return now }

	first := store.Snapshot()
	if first.Summary.LastUpdatedAt == nil {
		t.Fatal("first Summary.LastUpdatedAt = nil; want unavailable mutation timestamp")
	}
	if first.Summary.Status.LastUpdatedAt == nil {
		t.Fatal("first Status.LastUpdatedAt = nil; want unavailable mutation timestamp")
	}
	firstSummary := *first.Summary.LastUpdatedAt
	firstStatus := *first.Summary.Status.LastUpdatedAt

	now = base.Add(2 * time.Minute)
	second := store.Snapshot()
	if second.Summary.LastUpdatedAt == nil {
		t.Fatal("second Summary.LastUpdatedAt = nil; want stable unavailable timestamp")
	}
	if second.Summary.Status.LastUpdatedAt == nil {
		t.Fatal("second Status.LastUpdatedAt = nil; want stable unavailable timestamp")
	}
	if !second.Summary.LastUpdatedAt.Equal(firstSummary) {
		t.Fatalf("second Summary.LastUpdatedAt = %s; want stable %s", second.Summary.LastUpdatedAt, firstSummary)
	}
	if !second.Summary.Status.LastUpdatedAt.Equal(firstStatus) {
		t.Fatalf("second Status.LastUpdatedAt = %s; want stable %s", second.Summary.Status.LastUpdatedAt, firstStatus)
	}
}

func TestBusObservabilityStoreExportsNormalizedFeatureFlags(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ObserveFirstEnabled = true
	cfg.PassiveStateDirectApply = false
	cfg.PassiveConfigDirectApply = true
	cfg.ExternalWritePolicy = ObserveFirstExternalWritePolicyInvalidateOnly

	store := NewBusObservabilityStore(cfg)
	snapshot := store.Snapshot()
	state := snapshot.Summary.Status.FeatureFlags

	if !state.ObserveFirstEnabled {
		t.Fatal("FeatureFlags.ObserveFirstEnabled = false; want true")
	}
	if state.PassiveStateDirectApply {
		t.Fatal("FeatureFlags.PassiveStateDirectApply = true; want false")
	}
	if state.PassiveConfigDirectApply {
		t.Fatal("FeatureFlags.PassiveConfigDirectApply = true; want false")
	}
	if state.ExternalWritePolicy != ObserveFirstExternalWritePolicyInvalidateOnly {
		t.Fatalf("FeatureFlags.ExternalWritePolicy = %q; want invalidate_only", state.ExternalWritePolicy)
	}
	if len(state.Normalizations) != 1 {
		t.Fatalf("FeatureFlags.Normalizations = %v; want 1 entry", state.Normalizations)
	}

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `feature_flag_enabled{flag="observe_first_enabled"} 1`) {
		t.Fatalf("RenderPrometheus missing observe_first_enabled gauge:\n%s", metrics)
	}
	if !strings.Contains(metrics, `feature_flag_enabled{flag="passive_state_direct_apply"} 0`) {
		t.Fatalf("RenderPrometheus missing passive_state_direct_apply gauge:\n%s", metrics)
	}
	if !strings.Contains(metrics, `external_write_policy_state{policy="invalidate_only"} 1`) {
		t.Fatalf("RenderPrometheus missing normalized invalidate_only policy:\n%s", metrics)
	}
	if !strings.Contains(metrics, `feature_flag_normalizations_total{reason="config_requires_state"} 1`) {
		t.Fatalf("RenderPrometheus missing config_requires_state normalization:\n%s", metrics)
	}
}

func TestBusObservabilityStoreKeepsFeatureFlagTimestampProcessLifetimeImmutableAcrossBusMutations(t *testing.T) {
	cfg := DefaultConfig()
	store := NewBusObservabilityStore(cfg)

	base := time.Now().UTC().Truncate(time.Second)
	mutated := base.Add(2 * time.Minute)
	store.now = func() time.Time { return mutated }

	store.mu.Lock()
	store.lastUpdatedAt = base
	store.featureFlagsUpdatedAt = base
	store.mu.Unlock()

	if err := store.OnBusEvent(protocol.BusEvent{
		Kind: protocol.BusEventAttemptComplete,
		Request: protocol.Frame{
			Source:    0x08,
			Target:    0x15,
			Primary:   0xB5,
			Secondary: 0x09,
			Data:      []byte{0x03},
		},
		HasRequest: true,
	}); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	snapshot := store.Snapshot()
	if snapshot.Summary.LastUpdatedAt == nil {
		t.Fatal("Summary.LastUpdatedAt = nil; want mutation timestamp")
	}
	if !snapshot.Summary.LastUpdatedAt.Equal(mutated) {
		t.Fatalf("Summary.LastUpdatedAt = %s; want %s", snapshot.Summary.LastUpdatedAt, mutated)
	}
	if snapshot.Summary.Status.LastUpdatedAt == nil {
		t.Fatal("Status.LastUpdatedAt = nil; want mutation timestamp")
	}
	if !snapshot.Summary.Status.LastUpdatedAt.Equal(mutated) {
		t.Fatalf("Status.LastUpdatedAt = %s; want %s", snapshot.Summary.Status.LastUpdatedAt, mutated)
	}
	if snapshot.Summary.Status.FeatureFlags.LastUpdatedAt == nil {
		t.Fatal("FeatureFlags.LastUpdatedAt = nil; want provider-owned timestamp")
	}
	if !snapshot.Summary.Status.FeatureFlags.LastUpdatedAt.Equal(base) {
		t.Fatalf("FeatureFlags.LastUpdatedAt = %s; want stable %s", snapshot.Summary.Status.FeatureFlags.LastUpdatedAt, base)
	}
}

func TestBusObservabilityStoreExposesPublisherCadenceFromSemanticStateInterval(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SemanticStateInterval = 7 * time.Minute
	store := NewBusObservabilityStore(cfg)

	snapshot := store.Snapshot()
	if snapshot.Summary.Status.LastUpdatedAt == nil {
		t.Fatal("Snapshot() missing status timestamp; want populated snapshot")
	}
	if got, want := snapshot.Summary.Status.PublisherCadenceSec, cfg.SemanticStateInterval.Seconds(); got != want {
		t.Fatalf("PublisherCadenceSec = %v; want %v", got, want)
	}
	if got, want := snapshot.Summary.Status.PublisherCadenceSource, busObservabilityPublisherCadenceSource; got != want {
		t.Fatalf("PublisherCadenceSource = %q; want %q", got, want)
	}
}

func TestBusObservabilityStoreIncludesStartupSurfaceInSnapshot(t *testing.T) {
	cfg := DefaultConfig()
	store := NewBusObservabilityStore(cfg)
	startupUpdatedAt := time.Now().UTC().Truncate(time.Second)
	store.SetStartupSurfaceProvider(func() *BusObservabilityStartup {
		return &BusObservabilityStartup{
			LastUpdatedAt: &startupUpdatedAt,
			Phase:         string(graphql.SemanticStartupPhaseLiveReady),
			CacheEpoch:    2,
			LiveEpoch:     5,
		}
	})

	snapshot := store.Snapshot()
	startup := snapshot.Summary.Status.Startup
	if startup == nil {
		t.Fatal("Snapshot().Summary.Status.Startup = nil; want startup surface")
	}
	if startup.Phase != string(graphql.SemanticStartupPhaseLiveReady) {
		t.Fatalf("Startup.Phase = %q; want LIVE_READY", startup.Phase)
	}
	if startup.CacheEpoch != 2 || startup.LiveEpoch != 5 {
		t.Fatalf("Startup epochs = (%d,%d); want (2,5)", startup.CacheEpoch, startup.LiveEpoch)
	}
	if startup.LastUpdatedAt == nil {
		t.Fatal("Startup.LastUpdatedAt = nil; want provider-owned timestamp")
	}
	if !startup.LastUpdatedAt.Equal(startupUpdatedAt) {
		t.Fatalf("Startup.LastUpdatedAt = %s; want %s", startup.LastUpdatedAt, startupUpdatedAt)
	}
}

func TestBusObservabilityStoreKeepsUnsupportedPassiveTransportOutOfStartupTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportEbusdTCP

	base := time.Now().UTC()
	store := NewBusObservabilityStore(cfg)
	store.now = func() time.Time {
		return base.Add(cfg.ObserveFirstWarmupOuterWindow + time.Second)
	}

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="unsupported_or_misconfigured"} 1`) {
		t.Fatalf("RenderPrometheus missing unsupported_or_misconfigured reason for ebusd-tcp passive mode:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0`) {
		t.Fatalf("RenderPrometheus unexpectedly counted timeout probes for ebusd-tcp passive mode:\n%s", metrics)
	}
	if strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="startup_timeout"} 1`) {
		t.Fatalf("RenderPrometheus reported startup_timeout for ebusd-tcp passive mode:\n%s", metrics)
	}
}

func TestBusObservabilityStoreSnapshotDoesNotAdvanceTimestampOnRepeatedUnsupportedReads(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportEbusdTCP

	base := time.Now().UTC().Truncate(time.Second)
	now := base
	store := NewBusObservabilityStore(cfg)
	store.now = func() time.Time { return now }

	first := store.Snapshot()
	if first.Summary.LastUpdatedAt == nil {
		t.Fatal("first Summary.LastUpdatedAt = nil; want unavailable mutation timestamp")
	}
	if first.Summary.Status.LastUpdatedAt == nil {
		t.Fatal("first Status.LastUpdatedAt = nil; want unavailable mutation timestamp")
	}
	firstSummary := *first.Summary.LastUpdatedAt
	firstStatus := *first.Summary.Status.LastUpdatedAt

	now = base.Add(2 * time.Minute)
	second := store.Snapshot()
	if second.Summary.LastUpdatedAt == nil {
		t.Fatal("second Summary.LastUpdatedAt = nil; want stable unavailable timestamp")
	}
	if second.Summary.Status.LastUpdatedAt == nil {
		t.Fatal("second Status.LastUpdatedAt = nil; want stable unavailable timestamp")
	}
	if !second.Summary.LastUpdatedAt.Equal(firstSummary) {
		t.Fatalf("second Summary.LastUpdatedAt = %s; want stable %s", second.Summary.LastUpdatedAt, firstSummary)
	}
	if !second.Summary.Status.LastUpdatedAt.Equal(firstStatus) {
		t.Fatalf("second Status.LastUpdatedAt = %s; want stable %s", second.Summary.Status.LastUpdatedAt, firstStatus)
	}
}

func TestBusObservabilityStoreKeepsDirectRemotePassiveOutOfStartupTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "192.168.100.2:9999"

	base := time.Now().UTC()
	store := NewBusObservabilityStore(cfg)
	store.now = func() time.Time {
		return base.Add(cfg.ObserveFirstWarmupOuterWindow + time.Second)
	}

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="unsupported_or_misconfigured"} 1`) {
		t.Fatalf("RenderPrometheus missing unsupported_or_misconfigured reason for direct remote passive mode:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0`) {
		t.Fatalf("RenderPrometheus unexpectedly counted timeout probes for direct remote passive mode:\n%s", metrics)
	}
	if strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="startup_timeout"} 1`) {
		t.Fatalf("RenderPrometheus reported startup_timeout for direct remote passive mode:\n%s", metrics)
	}
}

func TestBusObservabilityStoreKeepsDirectRemoteHostnamePassiveOutOfStartupTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportENS
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "adapter.local:9999"

	base := time.Now().UTC()
	store := NewBusObservabilityStore(cfg)
	store.now = func() time.Time {
		return base.Add(cfg.ObserveFirstWarmupOuterWindow + time.Second)
	}

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="unsupported_or_misconfigured"} 1`) {
		t.Fatalf("RenderPrometheus missing unsupported_or_misconfigured reason for hostname direct remote passive mode:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0`) {
		t.Fatalf("RenderPrometheus unexpectedly counted timeout probes for hostname direct remote passive mode:\n%s", metrics)
	}
	if strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="startup_timeout"} 1`) {
		t.Fatalf("RenderPrometheus reported startup_timeout for hostname direct remote passive mode:\n%s", metrics)
	}
}

func TestBusObservabilityStoreDoesNotDowngradeRemoteProxyLikeEndpoint(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportENS
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "192.168.100.4:19001"

	base := time.Now().UTC()
	store := NewBusObservabilityStore(cfg)
	store.now = func() time.Time {
		return base.Add(cfg.ObserveFirstWarmupOuterWindow + time.Second)
	}

	metrics := store.RenderPrometheus()
	if strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="unsupported_or_misconfigured"} 1`) {
		t.Fatalf("RenderPrometheus incorrectly marked remote proxy-like endpoint unsupported:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="startup_timeout"} 1`) {
		t.Fatalf("RenderPrometheus missing startup_timeout for remote proxy-like endpoint without passive ingress:\n%s", metrics)
	}
}

func TestBusObservabilityStoreDoesNotDowngradeRemoteProxyLikeHostnameEndpoint(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportENS
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "proxy.local:19001"

	base := time.Now().UTC()
	store := NewBusObservabilityStore(cfg)
	store.now = func() time.Time {
		return base.Add(cfg.ObserveFirstWarmupOuterWindow + time.Second)
	}

	metrics := store.RenderPrometheus()
	if strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="unsupported_or_misconfigured"} 1`) {
		t.Fatalf("RenderPrometheus incorrectly marked hostname remote proxy-like endpoint unsupported:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_passive_capability_unavailable_reason{reason="startup_timeout"} 1`) {
		t.Fatalf("RenderPrometheus missing startup_timeout for hostname remote proxy-like endpoint without passive ingress:\n%s", metrics)
	}
}

func TestBusObservabilityStoreBootstrapsWarmupFromConnectedSnapshotAfterAttach(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"

	reconstructor := newPassiveTransactionReconstructorCore(cfg)
	reconstructor.tap = &PassiveBusTap{
		status: PassiveTapStatus{
			Connected:     true,
			EndpointState: PassiveEndpointStateConnected,
			ConnectCount:  1,
			LastConnectAt: time.Unix(1700000000, 0).UTC(),
		},
	}

	store := NewBusObservabilityStore(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("store.Close error = %v", err)
		}
	}()

	if err := store.AttachReconstructor(ctx, reconstructor); err != nil {
		t.Fatalf("AttachReconstructor error = %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		store.mu.RLock()
		state := store.passive.state
		probeAttempts := store.passive.probeAttemptsTotal
		sessionStartedAt := store.passive.sessionStartedAt
		store.mu.RUnlock()
		if state == "warming_up" && probeAttempts == 1 && sessionStartedAt.Equal(reconstructor.tap.status.LastConnectAt) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	store.mu.RLock()
	defer store.mu.RUnlock()
	t.Fatalf("passive warmup state = %q, probeAttempts=%d, sessionStartedAt=%s; want warming_up bootstrap from connected snapshot",
		store.passive.state, store.passive.probeAttemptsTotal, store.passive.sessionStartedAt)
}

func TestBusObservabilityStoreExportsBusyMetricsWhenPassiveAvailable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"
	store := NewBusObservabilityStore(cfg)
	base := time.Now().UTC()
	store.now = func() time.Time { return base }

	store.mu.Lock()
	store.passive.state = "available"
	store.passive.startupWindowClosed = true
	store.mu.Unlock()

	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base, 0x10, 0x08, 0xB5, 0x24))

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, "ebus_bus_busy_seconds_total 0.05") {
		t.Fatalf("RenderPrometheus missing busy total for available passive state:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_bus_busy_ratio{window="1m"} 0.0008333333333333334`) {
		t.Fatalf("RenderPrometheus missing 1m busy ratio:\n%s", metrics)
	}
	if !strings.Contains(metrics, "ebus_passive_completed_transactions_total 1") {
		t.Fatalf("RenderPrometheus missing cumulative passive completed transaction counter:\n%s", metrics)
	}
}

func TestBusObservabilityStoreCompletedTransactionsCounterTracksOnlyTransactions(t *testing.T) {
	cfg := DefaultConfig()
	store := NewBusObservabilityStore(cfg)
	base := time.Now().UTC()

	store.OnPassiveClassifiedEvent(observabilityPassiveBroadcastEvent(base, 0x15, 0xB5, 0x16))
	store.OnPassiveClassifiedEvent(observabilityPassiveMasterFrameEvent(base.Add(time.Second), 0x15, 0x26, 0xB5, 0x09))

	store.mu.RLock()
	completed := store.passive.completedTransactionsTotal
	store.mu.RUnlock()
	if completed != 0 {
		t.Fatalf("completedTransactionsTotal after broadcast/frame = %d; want 0", completed)
	}

	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(2*time.Second), 0x10, 0x08, 0xB5, 0x24))
	store.mu.RLock()
	completed = store.passive.completedTransactionsTotal
	store.mu.RUnlock()
	if completed != 1 {
		t.Fatalf("completedTransactionsTotal after transaction = %d; want 1", completed)
	}
}

func TestBusObservabilityStoreRebootsWarmupFromConnectedSnapshotAfterSocketLoss(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"

	store := NewBusObservabilityStore(cfg)
	reconstructor := newPassiveTransactionReconstructorCore(cfg)
	connectedAt := time.Unix(1700000100, 0).UTC()
	reconstructor.tap = &PassiveBusTap{
		status: PassiveTapStatus{
			Connected:     true,
			EndpointState: PassiveEndpointStateConnected,
			ConnectCount:  2,
			LastConnectAt: connectedAt,
		},
	}

	store.mu.Lock()
	store.reconstructor = reconstructor
	store.passive.state = "unavailable"
	store.passive.unavailableReason = "socket_loss"
	store.passive.startupWindowClosed = true
	store.passive.probeAttemptsTotal = 1
	store.bootstrapPassiveWarmupFromSnapshotLocked(connectedAt, reconstructor.Snapshot())
	state := store.passive.state
	probeAttempts := store.passive.probeAttemptsTotal
	sessionStartedAt := store.passive.sessionStartedAt
	store.mu.Unlock()

	if state != "warming_up" {
		t.Fatalf("passive warmup state = %q; want warming_up", state)
	}
	if probeAttempts != 2 {
		t.Fatalf("probeAttemptsTotal = %d; want 2", probeAttempts)
	}
	if !sessionStartedAt.Equal(connectedAt) {
		t.Fatalf("sessionStartedAt = %s; want %s", sessionStartedAt, connectedAt)
	}
}

func TestBusObservabilityStoreRestartsWarmupOnTrafficAfterStartupTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"

	store := NewBusObservabilityStore(cfg)
	base := time.Unix(1700000200, 0).UTC()
	store.now = func() time.Time { return base }

	reconstructor := newPassiveTransactionReconstructorCore(cfg)
	reconstructor.tap = &PassiveBusTap{
		status: PassiveTapStatus{
			Connected:     true,
			EndpointState: PassiveEndpointStateConnected,
			ConnectCount:  1,
			LastConnectAt: base.Add(-time.Minute),
		},
	}

	store.mu.Lock()
	store.reconstructor = reconstructor
	store.passive.state = "unavailable"
	store.passive.unavailableReason = "startup_timeout"
	store.passive.startupWindowClosed = true
	store.passive.probeAttemptsTotal = 1
	store.mu.Unlock()

	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base, 0x10, 0x08, 0xB5, 0x24))

	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.passive.state != "warming_up" {
		t.Fatalf("passive warmup state = %q; want warming_up after passive traffic resumes", store.passive.state)
	}
	if store.passive.probeAttemptsTotal != 2 {
		t.Fatalf("probeAttemptsTotal = %d; want 2", store.passive.probeAttemptsTotal)
	}
	if store.passive.completedTransactions != 1 {
		t.Fatalf("completedTransactions = %d; want 1", store.passive.completedTransactions)
	}
	if store.passive.completedTransactionsTotal != 1 {
		t.Fatalf("completedTransactionsTotal = %d; want 1", store.passive.completedTransactionsTotal)
	}
	if store.passive.unavailableReason != "" {
		t.Fatalf("unavailableReason = %q; want cleared after traffic restart", store.passive.unavailableReason)
	}
}

func TestBusObservabilityStoreExportsWatchEfficiencyMetrics(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ObserveFirstEnabled = true
	cfg.PassiveStateDirectApply = true
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportEbusdTCP

	store := NewBusObservabilityStore(cfg)
	base := time.Unix(1700000300, 0).UTC()
	store.now = func() time.Time { return base.Add(4 * time.Second) }

	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	descriptor := watchEfficiencyStateFastDescriptor(key)
	for index := 0; index < 5; index++ {
		store.ObserveWatchRead(WatchEfficiencyReadEvent{
			Key:           key,
			Descriptor:    descriptor,
			HasDescriptor: true,
			MaxAge:        10 * time.Second,
			Stats: SemanticReadExecutionStats{
				ActiveFetchAttempted: true,
				ActiveFetchSucceeded: true,
				ActiveFetchDuration:  time.Second,
			},
			ObservedAt: base.Add(time.Duration(index) * time.Second),
		})
	}

	store.ObserveWatchRead(WatchEfficiencyReadEvent{
		Key:           key,
		Descriptor:    descriptor,
		HasDescriptor: true,
		MaxAge:        10 * time.Second,
		Stats: SemanticReadExecutionStats{
			ServedFromShadow:        true,
			ServedFromPassiveShadow: true,
		},
		ObservedAt: base.Add(6 * time.Second),
	})

	store.ObserveWatchDirectApply(WatchEfficiencyDirectApplyEvent{
		Key:                key,
		Descriptor:         descriptor,
		HasDescriptor:      true,
		ObservedAt:         base.Add(7 * time.Second),
		CandidateEvaluated: true,
		Accepted:           true,
	})
	store.ObserveWatchDirectApply(WatchEfficiencyDirectApplyEvent{
		Key:                key,
		Descriptor:         descriptor,
		HasDescriptor:      true,
		ObservedAt:         base.Add(8 * time.Second),
		CandidateEvaluated: true,
		Accepted:           false,
	})

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `passive_hits_total{family="B524",freshness_profile="state_fast"} 1`) {
		t.Fatalf("RenderPrometheus missing passive_hits_total bucket sample:\n%s", metrics)
	}
	if !strings.Contains(metrics, `direct_apply_total{family="B524",freshness_profile="state_fast"} 1`) {
		t.Fatalf("RenderPrometheus missing direct_apply_total bucket sample:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_passive_direct_apply_candidates_evaluated_total 2`) {
		t.Fatalf("RenderPrometheus missing direct-apply candidate counter:\n%s", metrics)
	}
	if !strings.Contains(metrics, `active_reads_avoided_total{family="B524",freshness_profile="state_fast"} 2`) {
		t.Fatalf("RenderPrometheus missing active_reads_avoided_total bucket sample:\n%s", metrics)
	}
	if !strings.Contains(metrics, `active_read_saved_seconds{family="B524",freshness_profile="state_fast"} 1`) {
		t.Fatalf("RenderPrometheus missing active_read_saved_seconds estimate:\n%s", metrics)
	}
	if !strings.Contains(metrics, `missed_due_to_transport_limitations_total{family="B524",freshness_profile="state_fast",limitation="transport_unavailable"} 5`) {
		t.Fatalf("RenderPrometheus missing transport limitation miss counter:\n%s", metrics)
	}
	if !strings.Contains(metrics, "Bucket-level estimate") {
		t.Fatalf("RenderPrometheus missing bucket-level estimate help text for saved duration:\n%s", metrics)
	}
}

func TestBusObservabilityStoreExcludesMaxAgeZeroReadsFromSavedDurationSamples(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ObserveFirstEnabled = true
	cfg.PassiveStateDirectApply = true
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportEbusdTCP

	store := NewBusObservabilityStore(cfg)
	base := time.Unix(1700000350, 0).UTC()
	store.now = func() time.Time { return base.Add(4 * time.Second) }

	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	descriptor := watchEfficiencyStateFastDescriptor(key)
	for index := 0; index < 5; index++ {
		store.ObserveWatchRead(WatchEfficiencyReadEvent{
			Key:           key,
			Descriptor:    descriptor,
			HasDescriptor: true,
			MaxAge:        0,
			Stats: SemanticReadExecutionStats{
				ActiveFetchAttempted: true,
				ActiveFetchSucceeded: true,
				ActiveFetchDuration:  time.Second,
			},
			ObservedAt: base.Add(time.Duration(index) * time.Second),
		})
	}

	metrics := store.RenderPrometheus()
	if strings.Contains(metrics, `active_read_saved_seconds{family="B524",freshness_profile="state_fast"}`) {
		t.Fatalf("RenderPrometheus unexpectedly included maxAge=0 reads in active_read_saved_seconds:\n%s", metrics)
	}
}

func TestBusObservabilityStoreOmitsStaleActiveReadSavedSeconds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ObserveFirstEnabled = true
	cfg.PassiveStateDirectApply = true
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportEbusdTCP

	base := time.Unix(1700000400, 0).UTC()
	store := NewBusObservabilityStore(cfg)
	store.now = func() time.Time { return base }

	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	descriptor := watchEfficiencyStateFastDescriptor(key)
	for index := 0; index < 5; index++ {
		store.ObserveWatchRead(WatchEfficiencyReadEvent{
			Key:           key,
			Descriptor:    descriptor,
			HasDescriptor: true,
			MaxAge:        10 * time.Second,
			Stats: SemanticReadExecutionStats{
				ActiveFetchAttempted: true,
				ActiveFetchSucceeded: true,
				ActiveFetchDuration:  900 * time.Millisecond,
			},
			ObservedAt: base.Add(time.Duration(index) * time.Second),
		})
	}

	store.now = func() time.Time { return base.Add(16 * time.Minute) }
	metrics := store.RenderPrometheus()
	if strings.Contains(metrics, `active_read_saved_seconds{family="B524",freshness_profile="state_fast"}`) {
		t.Fatalf("RenderPrometheus unexpectedly kept stale active_read_saved_seconds sample:\n%s", metrics)
	}
}

func TestBusObservabilityStoreStaleSavedWindowMustReearnThreshold(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ObserveFirstEnabled = true
	cfg.PassiveStateDirectApply = true
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportEbusdTCP

	base := time.Unix(1700000450, 0).UTC()
	store := NewBusObservabilityStore(cfg)
	store.now = func() time.Time { return base.Add(4 * time.Second) }

	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	descriptor := watchEfficiencyStateFastDescriptor(key)
	for index := 0; index < 5; index++ {
		store.ObserveWatchRead(WatchEfficiencyReadEvent{
			Key:           key,
			Descriptor:    descriptor,
			HasDescriptor: true,
			MaxAge:        10 * time.Second,
			Stats: SemanticReadExecutionStats{
				ActiveFetchAttempted: true,
				ActiveFetchSucceeded: true,
				ActiveFetchDuration:  time.Second,
			},
			ObservedAt: base.Add(time.Duration(index) * time.Second),
		})
	}

	store.now = func() time.Time { return base.Add(16 * time.Minute) }
	metrics := store.RenderPrometheus()
	if strings.Contains(metrics, `active_read_saved_seconds{family="B524",freshness_profile="state_fast"}`) {
		t.Fatalf("RenderPrometheus unexpectedly kept stale active_read_saved_seconds sample:\n%s", metrics)
	}

	freshStart := base.Add(16*time.Minute + time.Second)
	store.ObserveWatchRead(WatchEfficiencyReadEvent{
		Key:           key,
		Descriptor:    descriptor,
		HasDescriptor: true,
		MaxAge:        10 * time.Second,
		Stats: SemanticReadExecutionStats{
			ActiveFetchAttempted: true,
			ActiveFetchSucceeded: true,
			ActiveFetchDuration:  2 * time.Second,
		},
		ObservedAt: freshStart,
	})

	store.now = func() time.Time { return freshStart }
	metrics = store.RenderPrometheus()
	if strings.Contains(metrics, `active_read_saved_seconds{family="B524",freshness_profile="state_fast"}`) {
		t.Fatalf("RenderPrometheus should require fresh re-earning threshold after staleness:\n%s", metrics)
	}

	for index := 1; index < 5; index++ {
		store.ObserveWatchRead(WatchEfficiencyReadEvent{
			Key:           key,
			Descriptor:    descriptor,
			HasDescriptor: true,
			MaxAge:        10 * time.Second,
			Stats: SemanticReadExecutionStats{
				ActiveFetchAttempted: true,
				ActiveFetchSucceeded: true,
				ActiveFetchDuration:  2 * time.Second,
			},
			ObservedAt: freshStart.Add(time.Duration(index) * time.Second),
		})
	}

	store.now = func() time.Time { return freshStart.Add(4 * time.Second) }
	metrics = store.RenderPrometheus()
	if !strings.Contains(metrics, `active_read_saved_seconds{family="B524",freshness_profile="state_fast"} 2`) {
		t.Fatalf("RenderPrometheus missing fresh active_read_saved_seconds estimate after re-earning:\n%s", metrics)
	}
}

func TestBusObservabilityStoreTreatsActiveConfirmedShadowHitsAsNonPassive(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ObserveFirstEnabled = true
	cfg.PassiveStateDirectApply = true
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportEbusdTCP

	store := NewBusObservabilityStore(cfg)
	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	descriptor := watchEfficiencyStateFastDescriptor(key)
	store.ObserveWatchRead(WatchEfficiencyReadEvent{
		Key:           key,
		Descriptor:    descriptor,
		HasDescriptor: true,
		MaxAge:        10 * time.Second,
		Stats: SemanticReadExecutionStats{
			ServedFromShadow: true,
		},
		ObservedAt: time.Unix(1700000475, 0).UTC(),
	})

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `passive_hits_total{family="B524",freshness_profile="state_fast"} 0`) {
		t.Fatalf("RenderPrometheus should not count active-confirmed shadow hit as passive:\n%s", metrics)
	}
	if !strings.Contains(metrics, `active_reads_avoided_total{family="B524",freshness_profile="state_fast"} 0`) {
		t.Fatalf("RenderPrometheus should not count active-confirmed shadow hit as read avoidance:\n%s", metrics)
	}
}

func TestBusObservabilityStoreExportsWatchEfficiencyAmbiguousReason(t *testing.T) {
	cfg := DefaultConfig()
	store := NewBusObservabilityStore(cfg)
	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)

	store.ObserveWatchRead(WatchEfficiencyReadEvent{
		Key: key,
		Stats: SemanticReadExecutionStats{
			ActiveFetchAttempted: true,
		},
		ObservedAt: time.Unix(1700000500, 0).UTC(),
	})

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `ambiguous_total{family="B524",reason="missing_runtime_descriptor"} 1`) {
		t.Fatalf("RenderPrometheus missing ambiguous_total sample for missing descriptor:\n%s", metrics)
	}
}

func TestBusObservabilityStoreTransportLimitationPrefersTransportUnavailableOverBroadcast(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ObserveFirstEnabled = true
	cfg.PassiveStateDirectApply = true
	cfg.BroadcastListen = false
	cfg.TransportConfig.Protocol = TransportEbusdTCP

	store := NewBusObservabilityStore(cfg)
	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	descriptor := watchEfficiencyStateFastDescriptor(key)
	store.ObserveWatchRead(WatchEfficiencyReadEvent{
		Key:           key,
		Descriptor:    descriptor,
		HasDescriptor: true,
		MaxAge:        10 * time.Second,
		Stats: SemanticReadExecutionStats{
			ActiveFetchAttempted: true,
			ActiveFetchSucceeded: true,
			ActiveFetchDuration:  time.Second,
		},
		ObservedAt: time.Unix(1700000525, 0).UTC(),
	})

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `missed_due_to_transport_limitations_total{family="B524",freshness_profile="state_fast",limitation="transport_unavailable"} 1`) {
		t.Fatalf("RenderPrometheus missing transport_unavailable miss classification on dual-failure config:\n%s", metrics)
	}
	if strings.Contains(metrics, `missed_due_to_transport_limitations_total{family="B524",freshness_profile="state_fast",limitation="broadcast_unavailable"} 1`) {
		t.Fatalf("RenderPrometheus incorrectly preferred broadcast_unavailable over transport_unavailable on dual-failure config:\n%s", metrics)
	}
}

func TestBusObservabilityStoreTransportLimitationClassifiesBroadcastUnavailable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ObserveFirstEnabled = true
	cfg.PassiveStateDirectApply = true
	cfg.BroadcastListen = false
	cfg.TransportConfig.Protocol = TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"

	store := NewBusObservabilityStore(cfg)
	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	descriptor := watchEfficiencyStateFastDescriptor(key)
	store.ObserveWatchRead(WatchEfficiencyReadEvent{
		Key:           key,
		Descriptor:    descriptor,
		HasDescriptor: true,
		MaxAge:        10 * time.Second,
		Stats: SemanticReadExecutionStats{
			ActiveFetchAttempted: true,
			ActiveFetchSucceeded: true,
			ActiveFetchDuration:  time.Second,
		},
		ObservedAt: time.Unix(1700000550, 0).UTC(),
	})

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `missed_due_to_transport_limitations_total{family="B524",freshness_profile="state_fast",limitation="broadcast_unavailable"} 1`) {
		t.Fatalf("RenderPrometheus missing broadcast_unavailable miss classification:\n%s", metrics)
	}
	if strings.Contains(metrics, `missed_due_to_transport_limitations_total{family="B524",freshness_profile="state_fast",limitation="transport_unavailable"} 1`) {
		t.Fatalf("RenderPrometheus unexpectedly classified ENH broadcast-off miss as transport_unavailable:\n%s", metrics)
	}
}

func TestBusObservabilityStoreRenderPrometheusOmitsEnergyBroadcastMetricsWithoutData(t *testing.T) {
	store := NewBusObservabilityStore(DefaultConfig())

	metrics := store.RenderPrometheus()
	if strings.Contains(metrics, `energy_broadcast_selectors`) {
		t.Fatalf("RenderPrometheus emits energy_broadcast_selectors without data:\n%s", metrics)
	}
	if strings.Contains(metrics, `energy_broadcast_freshness_transitions_total`) {
		t.Fatalf("RenderPrometheus emits energy_broadcast_freshness_transitions_total without data:\n%s", metrics)
	}
}

func TestBusObservabilityStoreEnergyFreshnessMetricsTrackPassiveAndAgingWithoutNewReads(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"

	store := NewBusObservabilityStore(cfg)
	base := time.Now().UTC()
	now := base
	store.now = func() time.Time { return now }
	store.mu.Lock()
	store.passive.startupWindowClosed = true
	store.passive.state = "warming_up"
	store.mu.Unlock()

	semantic := graphql.NewLiveSemanticProvider()
	store.SetEnergyFreshnessMetricsRefresher(func(observedAt time.Time, passiveState string) {
		semantic.RefreshEnergyFreshnessMetrics(observedAt, passiveState)
	})

	_, updated := semantic.ApplyBroadcast(router.BroadcastEvent{
		Values: map[string]types.Value{
			"wh":     {Valid: true, Value: float64(1000)},
			"source": {Valid: true, Value: "gas"},
			"usage":  {Valid: true, Value: "heating"},
			"period": {Valid: true, Value: "day"},
		},
	})
	if !updated {
		t.Fatal("ApplyBroadcast() updated = false; want true")
	}

	now = now.Add(1 * time.Minute)
	staleAfterWarmBefore := readExpvarNamedMapInt("energy_broadcast_selectors", "stale")
	metricsWarm := store.RenderPrometheus()
	staleAfterWarm := readExpvarNamedMapInt("energy_broadcast_selectors", "stale")
	if staleAfterWarm < staleAfterWarmBefore {
		t.Fatalf("energy_broadcast_selectors[stale] = %d; want >= %d", staleAfterWarm, staleAfterWarmBefore)
	}

	warmToStaleBefore := readExpvarNamedMapInt("energy_broadcast_freshness_transitions_total", "warming_up->stale")

	now = now.Add(9 * time.Minute)
	store.mu.Lock()
	store.passive.state = "available"
	store.mu.Unlock()
	metricsAged := store.RenderPrometheus()
	staleAfterAged := readExpvarNamedMapInt("energy_broadcast_selectors", "stale")
	warmToStaleAfter := readExpvarNamedMapInt("energy_broadcast_freshness_transitions_total", "warming_up->stale")

	if staleAfterAged <= staleAfterWarm {
		t.Fatalf("energy_broadcast_selectors[stale] after aging = %d; want > %d", staleAfterAged, staleAfterWarm)
	}
	if warmToStaleAfter <= warmToStaleBefore {
		t.Fatalf("energy_broadcast_freshness_transitions_total[warming_up->stale] = %d; want > %d", warmToStaleAfter, warmToStaleBefore)
	}
	if !strings.Contains(metricsWarm, "energy_broadcast_selectors") || !strings.Contains(metricsAged, "energy_broadcast_selectors") {
		t.Fatalf("RenderPrometheus missing energy freshness selectors metric:\nwarm:\n%s\naged:\n%s", metricsWarm, metricsAged)
	}
	if !strings.Contains(metricsAged, "recomputed at scrape time") {
		t.Fatalf("RenderPrometheus help text does not describe scrape-time recomputation:\n%s", metricsAged)
	}
}

func watchEfficiencyStateFastDescriptor(key WatchKey) WatchDescriptor {
	return WatchDescriptor{
		Key:               key,
		SemanticClass:     WatchSemanticClassState,
		FreshnessProfile:  WatchFreshnessProfileStateFast,
		DecoderID:         "test.watch.efficiency",
		CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
		DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
	}
}

func observabilityPassiveTransactionEvent(observedAt time.Time, source, target, primary, secondary byte) PassiveClassifiedEvent {
	return PassiveClassifiedEvent{
		Kind:      PassiveClassifiedEventTransaction,
		FrameType: protocol.FrameTypeInitiatorTarget,
		Request: protocol.Frame{
			Source:    source,
			Target:    target,
			Primary:   primary,
			Secondary: secondary,
			Data:      []byte{0x01},
		},
		Response: protocol.Frame{
			Source:    target,
			Target:    source,
			Primary:   primary,
			Secondary: secondary,
			Data:      []byte{0x02},
		},
		HasRequest:  true,
		HasResponse: true,
		Timing: PassiveTimingMarkers{
			RequestStart:  observedAt.Add(-50 * time.Millisecond),
			RequestEnd:    observedAt.Add(-25 * time.Millisecond),
			ResponseStart: observedAt.Add(-20 * time.Millisecond),
			ResponseEnd:   observedAt.Add(-5 * time.Millisecond),
			Terminal:      observedAt,
		},
		ObservedAt: observedAt,
	}
}

func observabilityPassiveBroadcastEvent(observedAt time.Time, source, primary, secondary byte) PassiveClassifiedEvent {
	return PassiveClassifiedEvent{
		Kind:      PassiveClassifiedEventBroadcastFrame,
		FrameType: protocol.FrameTypeBroadcast,
		Request: protocol.Frame{
			Source:    source,
			Target:    0xFF,
			Primary:   primary,
			Secondary: secondary,
			Data:      []byte{0x01},
		},
		HasRequest: true,
		ObservedAt: observedAt,
	}
}

func observabilityPassiveMasterFrameEvent(observedAt time.Time, source, target, primary, secondary byte) PassiveClassifiedEvent {
	return PassiveClassifiedEvent{
		Kind:      PassiveClassifiedEventMasterFrame,
		FrameType: protocol.FrameTypeInitiatorInitiator,
		Request: protocol.Frame{
			Source:    source,
			Target:    target,
			Primary:   primary,
			Secondary: secondary,
			Data:      []byte{0x01},
		},
		HasRequest: true,
		ObservedAt: observedAt,
	}
}

func specimenPassiveStore() *BusObservabilityStore {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"
	store := NewBusObservabilityStore(cfg)
	store.mu.Lock()
	store.passive.state = "available"
	store.passive.startupWindowClosed = true
	store.mu.Unlock()
	return store
}

func TestSpecimenCaptureFromPassiveFrame(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(10 * time.Second) }

	// Primary=0x07, Secondary=0x04 => family "0704" (not implemented).
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(10*time.Second), 0x10, 0x08, 0x07, 0x04))

	items := store.ProtocolSpecimens("")
	if len(items) != 1 {
		t.Fatalf("ProtocolSpecimens length = %d; want 1", len(items))
	}
	if items[0].Family != "0704" {
		t.Fatalf("ProtocolSpecimens[0].Family = %q; want %q", items[0].Family, "0704")
	}
	if items[0].Source != 0x10 {
		t.Fatalf("ProtocolSpecimens[0].Source = 0x%02x; want 0x10", items[0].Source)
	}
	if items[0].Target != 0x08 {
		t.Fatalf("ProtocolSpecimens[0].Target = 0x%02x; want 0x08", items[0].Target)
	}
	if items[0].Count != 1 {
		t.Fatalf("ProtocolSpecimens[0].Count = %d; want 1", items[0].Count)
	}
	if items[0].Outcome != "success" {
		t.Fatalf("ProtocolSpecimens[0].Outcome = %q; want %q", items[0].Outcome, "success")
	}
	if items[0].RequestHex != "01" {
		t.Fatalf("ProtocolSpecimens[0].RequestHex = %q; want %q", items[0].RequestHex, "01")
	}
	if items[0].ResponseHex != "02" {
		t.Fatalf("ProtocolSpecimens[0].ResponseHex = %q; want %q", items[0].ResponseHex, "02")
	}
}

func TestSpecimenDedupIncrements(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(30 * time.Second) }

	// Same frame twice.
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(10*time.Second), 0x10, 0x08, 0x07, 0x04))
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(20*time.Second), 0x10, 0x08, 0x07, 0x04))

	items := store.ProtocolSpecimens("")
	if len(items) != 1 {
		t.Fatalf("ProtocolSpecimens length = %d; want 1 (dedup)", len(items))
	}
	if items[0].Count != 2 {
		t.Fatalf("ProtocolSpecimens[0].Count = %d; want 2", items[0].Count)
	}
	if !items[0].LastSeenAt.Equal(base.Add(20 * time.Second)) {
		t.Fatalf("ProtocolSpecimens[0].LastSeenAt = %v; want %v", items[0].LastSeenAt, base.Add(20*time.Second))
	}
}

func TestSpecimenRejectsImplementedFamily(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(10 * time.Second) }

	// B509 (0xB5, 0x09) is an implemented family.
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(10*time.Second), 0x10, 0x08, 0xB5, 0x09))
	// B524 (0xB5, 0x24) is an implemented family.
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(11*time.Second), 0x10, 0x08, 0xB5, 0x24))
	// B516 (0xB5, 0x16) is an implemented family.
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(12*time.Second), 0x15, 0x26, 0xB5, 0x16))
	// B555 (0xB5, 0x55) is an implemented family.
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(13*time.Second), 0x15, 0x26, 0xB5, 0x55))

	items := store.ProtocolSpecimens("")
	if len(items) != 0 {
		t.Fatalf("ProtocolSpecimens length = %d; want 0 (all implemented families rejected)", len(items))
	}
}

func TestSpecimenFamilyFilter(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(30 * time.Second) }

	// Two different non-implemented families.
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(10*time.Second), 0x10, 0x08, 0x07, 0x04))
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(20*time.Second), 0x15, 0x26, 0x08, 0x00))

	all := store.ProtocolSpecimens("")
	if len(all) != 2 {
		t.Fatalf("ProtocolSpecimens (all) length = %d; want 2", len(all))
	}

	filtered := store.ProtocolSpecimens("0704")
	if len(filtered) != 1 {
		t.Fatalf("ProtocolSpecimens (filtered 0704) length = %d; want 1", len(filtered))
	}
	if filtered[0].Family != "0704" {
		t.Fatalf("ProtocolSpecimens (filtered)[0].Family = %q; want %q", filtered[0].Family, "0704")
	}

	empty := store.ProtocolSpecimens("ZZZZ")
	if len(empty) != 0 {
		t.Fatalf("ProtocolSpecimens (no match) length = %d; want 0", len(empty))
	}
}

func TestSpecimenRingBufferEviction(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()

	// Push specimenMaxPerFamily + 1 unique entries into one family.
	for i := 0; i <= specimenMaxPerFamily; i++ {
		now := base.Add(time.Duration(i) * time.Second)
		store.now = func() time.Time { return now }
		event := PassiveClassifiedEvent{
			Kind:      PassiveClassifiedEventTransaction,
			FrameType: protocol.FrameTypeInitiatorTarget,
			Request: protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   0x07,
				Secondary: 0x04,
				Data:      []byte{byte(i)}, // unique data per entry
			},
			Response: protocol.Frame{
				Source:    0x08,
				Target:    0x10,
				Primary:   0x07,
				Secondary: 0x04,
				Data:      []byte{0x02},
			},
			HasRequest:  true,
			HasResponse: true,
			ObservedAt:  now,
			Timing: PassiveTimingMarkers{
				RequestStart:  now.Add(-50 * time.Millisecond),
				RequestEnd:    now.Add(-25 * time.Millisecond),
				ResponseStart: now.Add(-20 * time.Millisecond),
				ResponseEnd:   now.Add(-5 * time.Millisecond),
				Terminal:      now,
			},
		}
		store.OnPassiveClassifiedEvent(event)
	}

	items := store.ProtocolSpecimens("0704")
	if len(items) != specimenMaxPerFamily {
		t.Fatalf("ProtocolSpecimens length = %d; want %d (ring buffer cap)", len(items), specimenMaxPerFamily)
	}

	// The oldest entry (Data=0x00) should have been evicted.
	for _, item := range items {
		if item.RequestHex == "00" {
			t.Fatalf("ProtocolSpecimens contains evicted entry with RequestHex=00")
		}
	}
}

func TestSpecimenSummaryFields(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(10 * time.Second) }

	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(10*time.Second), 0x10, 0x08, 0x07, 0x04))
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(11*time.Second), 0x15, 0x26, 0x08, 0x00))

	snapshot := store.Snapshot()
	if snapshot.Summary.SpecimenFamilies != 2 {
		t.Fatalf("Summary.SpecimenFamilies = %d; want 2", snapshot.Summary.SpecimenFamilies)
	}
	if snapshot.Summary.SpecimenCount != 2 {
		t.Fatalf("Summary.SpecimenCount = %d; want 2", snapshot.Summary.SpecimenCount)
	}
}

func TestSpecimenMaxFamiliesCapDropsSilently(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(10 * time.Second) }

	// Fill to specimenMaxFamilies (64) by varying both primary and secondary.
	for i := 0; i < specimenMaxFamilies; i++ {
		primary := byte(0xA0 + i/256)
		secondary := byte(i % 256)
		store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(time.Duration(i)*time.Second), 0x10, 0x08, primary, secondary))
	}
	if got := len(store.ProtocolSpecimens("")); got != specimenMaxFamilies {
		t.Fatalf("specimens after filling %d families = %d; want %d", specimenMaxFamilies, got, specimenMaxFamilies)
	}

	// 65th family should be silently dropped.
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(100*time.Second), 0x10, 0x08, 0xFF, 0xFF))
	if got := len(store.ProtocolSpecimens("")); got != specimenMaxFamilies {
		t.Fatalf("specimens after 65th family = %d; want %d (cap enforced)", got, specimenMaxFamilies)
	}
}

func TestScanCollisionClassifiedThroughReconstructor(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(10 * time.Second) }

	// Simulate a scan collision: a 0704 probe whose raw bytes fail parseFrame.
	// OnPassiveClassifiedEvent receives already-classified events, so we inject
	// an abandoned event with ScanCollision reason directly.
	store.OnPassiveClassifiedEvent(PassiveClassifiedEvent{
		Kind:      PassiveClassifiedEventAbandonedTransaction,
		FrameType: protocol.FrameTypeInitiatorTarget,
		Request: protocol.Frame{
			Source:    0x71,
			Target:    0x42,
			Primary:   0x07,
			Secondary: 0x04,
			Data:      []byte{0xDE, 0xAD}, // garbled payload
		},
		HasRequest:    true,
		AbandonReason: PassiveAbandonReasonScanCollision,
		ObservedAt:    base.Add(10 * time.Second),
	})

	metrics := store.RenderPrometheus()
	// scan_collision is a non-error — should NOT appear in ebus_errors_total.
	if strings.Contains(metrics, `class="corrupted_request"`) {
		t.Fatalf("scan_collision misclassified as corrupted_request:\n%s", metrics)
	}
	if strings.Contains(metrics, `class="scan_collision"`) {
		t.Fatalf("scan_collision should not appear as error class:\n%s", metrics)
	}
}

func TestScanRelatedAbandonUsesAggregateTargetBucket(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(10 * time.Second) }

	// Inject scan_timeout abandoned events to different targets.
	for _, target := range []byte{0x05, 0x06, 0x42} {
		store.OnPassiveClassifiedEvent(PassiveClassifiedEvent{
			Kind:      PassiveClassifiedEventAbandonedTransaction,
			FrameType: protocol.FrameTypeInitiatorTarget,
			Request: protocol.Frame{
				Source:    0x71,
				Target:    target,
				Primary:   0x07,
				Secondary: 0x04,
			},
			HasRequest:    true,
			AbandonReason: PassiveAbandonReasonScanTimeout,
			ObservedAt:    base.Add(10 * time.Second),
		})
	}

	metrics := store.RenderPrometheus()
	// All three should aggregate under dst="scan", not per-address.
	if !strings.Contains(metrics, `dst="scan"`) {
		t.Fatalf("scan-related abandoned frames not aggregated under dst=\"scan\":\n%s", metrics)
	}
	// Individual addresses should NOT appear for scan events.
	if strings.Contains(metrics, `dst="0x05"`) || strings.Contains(metrics, `dst="0x42"`) {
		t.Fatalf("scan-related abandoned frames leaked per-address labels:\n%s", metrics)
	}
}

func TestSpecimenConcurrentReadWrite(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(10 * time.Second) }

	done := make(chan struct{})
	// Writer goroutine.
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(
				base.Add(time.Duration(i)*time.Millisecond), 0x10, 0x08, 0x07, 0x04,
			))
		}
	}()
	// Concurrent reader.
	for i := 0; i < 200; i++ {
		_ = store.ProtocolSpecimens("")
	}
	<-done
	items := store.ProtocolSpecimens("")
	if len(items) == 0 {
		t.Fatal("ProtocolSpecimens empty after concurrent writes")
	}
}

func TestSelfEchoClassifiedAsNonError(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(10 * time.Second) }

	store.OnPassiveClassifiedEvent(PassiveClassifiedEvent{
		Kind:          PassiveClassifiedEventAbandonedTransaction,
		AbandonReason: PassiveAbandonReasonSelfEcho,
		ObservedAt:    base.Add(10 * time.Second),
	})

	metrics := store.RenderPrometheus()
	if strings.Contains(metrics, `class="corrupted_request"`) {
		t.Fatalf("self_echo misclassified as corrupted_request:\n%s", metrics)
	}
	if strings.Contains(metrics, `class="self_echo"`) {
		t.Fatalf("self_echo should not appear as error class:\n%s", metrics)
	}
}

func TestPassiveCorruptedRequestIsNonError(t *testing.T) {
	store := specimenPassiveStore()
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(10 * time.Second) }

	store.OnPassiveClassifiedEvent(PassiveClassifiedEvent{
		Kind:          PassiveClassifiedEventAbandonedTransaction,
		AbandonReason: PassiveAbandonReasonCorruptedRequest,
		ObservedAt:    base.Add(10 * time.Second),
	})

	metrics := store.RenderPrometheus()
	// CRC failure on passive observation is normal bus contention, not a
	// software error.  The error counter must NOT be incremented.
	if strings.Contains(metrics, `class="corrupted_request"`) {
		t.Fatalf("passive corrupted_request should not be counted as error:\n%s", metrics)
	}
}
