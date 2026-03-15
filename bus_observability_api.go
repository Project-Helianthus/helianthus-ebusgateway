package ebusgateway

import (
	"sort"
	"time"
)

type BusObservabilityCapability struct {
	ActiveSupported    bool   `json:"active_supported"`
	PassiveSupported   bool   `json:"passive_supported"`
	BroadcastSupported bool   `json:"broadcast_supported"`
	PassiveAvailable   bool   `json:"passive_available"`
	PassiveState       string `json:"passive_state"`
	PassiveReason      string `json:"passive_reason,omitempty"`
	EndpointState      string `json:"endpoint_state"`
	TapConnected       bool   `json:"tap_connected"`
}

type BusObservabilityWarmup struct {
	State                 string  `json:"state"`
	Blocker               string  `json:"blocker,omitempty"`
	ElapsedSeconds        float64 `json:"elapsed_seconds,omitempty"`
	CompletedTransactions int     `json:"completed_transactions"`
	RequiredTransactions  int     `json:"required_transactions"`
	CompletionMode        string  `json:"completion_mode,omitempty"`
}

type BusObservabilityTimingQuality struct {
	Active      string `json:"active"`
	Passive     string `json:"passive"`
	Busy        string `json:"busy"`
	Periodicity string `json:"periodicity"`
}

type BusObservabilityDegraded struct {
	Active  bool     `json:"active"`
	Reasons []string `json:"reasons,omitempty"`
}

type BusObservabilityStartup struct {
	Phase      string `json:"phase"`
	CacheEpoch uint64 `json:"cache_epoch"`
	LiveEpoch  uint64 `json:"live_epoch"`
}

type BusObservabilityStatus struct {
	TransportClass string                        `json:"transport_class"`
	Capability     BusObservabilityCapability    `json:"capability"`
	Warmup         BusObservabilityWarmup        `json:"warmup"`
	TimingQuality  BusObservabilityTimingQuality `json:"timing_quality"`
	Degraded       BusObservabilityDegraded      `json:"degraded"`
	Startup        *BusObservabilityStartup      `json:"startup,omitempty"`
	FeatureFlags   ObserveFirstFeatureFlagState  `json:"feature_flags"`
}

type BusObservabilityBoundedList struct {
	Count    int `json:"count"`
	Capacity int `json:"capacity"`
}

type BusObservabilityCounters struct {
	SeriesBudgetOverflowTotal      uint64 `json:"series_budget_overflow_total"`
	PeriodicityBudgetOverflowTotal uint64 `json:"periodicity_budget_overflow_total"`
}

type BusObservabilitySummary struct {
	Status      BusObservabilityStatus      `json:"status"`
	Messages    BusObservabilityBoundedList `json:"messages"`
	Periodicity BusObservabilityBoundedList `json:"periodicity"`
	Counters    BusObservabilityCounters    `json:"counters"`
}

type BusObservabilitySnapshot struct {
	Summary     BusObservabilitySummary `json:"summary"`
	Messages    []BusMessageRecord      `json:"messages,omitempty"`
	Periodicity []BusPeriodicityEntry   `json:"periodicity,omitempty"`
}

func (store *BusObservabilityStore) Snapshot() BusObservabilitySnapshot {
	if store == nil {
		return BusObservabilitySnapshot{}
	}

	startup := store.startupSurface()

	store.mu.Lock()
	defer store.mu.Unlock()

	now := store.now()
	reconstructor := store.reconstructorSnapshotLocked()
	store.refreshPassiveStateLocked(now, reconstructor.TapStatus)
	store.evictStalePeriodicityLocked(now)

	return BusObservabilitySnapshot{
		Summary:     store.summaryLocked(now, reconstructor.TapStatus, startup),
		Messages:    store.recentMessagesLocked(store.recentLen),
		Periodicity: store.periodicitySnapshotLocked(),
	}
}

func (store *BusObservabilityStore) recentMessagesLocked(limit int) []BusMessageRecord {
	if store.recentLen == 0 {
		return nil
	}
	if limit <= 0 || limit > store.recentLen {
		limit = store.recentLen
	}

	items := make([]BusMessageRecord, 0, limit)
	start := (store.recentStart + store.recentLen - limit + len(store.recent)) % len(store.recent)
	for i := 0; i < limit; i++ {
		idx := (start + i) % len(store.recent)
		items = append(items, store.recent[idx])
	}
	return items
}

func (store *BusObservabilityStore) periodicitySnapshotLocked() []BusPeriodicityEntry {
	items := make([]BusPeriodicityEntry, 0, len(store.periodicity))
	for _, entry := range store.periodicity {
		items = append(items, store.exportedPeriodicityEntryLocked(entry))
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].LastSeen.Equal(items[j].LastSeen) {
			if items[i].SourceBucket == items[j].SourceBucket {
				if items[i].TargetBucket == items[j].TargetBucket {
					if items[i].Primary == items[j].Primary {
						return items[i].Secondary < items[j].Secondary
					}
					return items[i].Primary < items[j].Primary
				}
				return items[i].TargetBucket < items[j].TargetBucket
			}
			return items[i].SourceBucket < items[j].SourceBucket
		}
		return items[i].LastSeen.Before(items[j].LastSeen)
	})
	return items
}

func (store *BusObservabilityStore) summaryLocked(now time.Time, tapStatus PassiveTapStatus, startup *BusObservabilityStartup) BusObservabilitySummary {
	passiveSupported := store.cfg.BroadcastListen && PassiveTransportSupported(store.cfg)
	reasons := make([]string, 0, 2)
	if store.passive.state == "unavailable" && store.passive.unavailableReason != "" {
		reasons = append(reasons, store.passive.unavailableReason)
	}
	if readDedupDegradedState() > 0 {
		reasons = append(reasons, "dedup_degraded")
	}

	endpointState := string(tapStatus.EndpointState)
	if endpointState == "" {
		endpointState = string(PassiveEndpointStateUnknown)
	}

	return BusObservabilitySummary{
		Status: BusObservabilityStatus{
			TransportClass: store.transportClass,
			Capability: BusObservabilityCapability{
				ActiveSupported:    true,
				PassiveSupported:   passiveSupported,
				BroadcastSupported: passiveSupported,
				PassiveAvailable:   store.passive.state == "available",
				PassiveState:       store.passive.state,
				PassiveReason:      store.passive.unavailableReason,
				EndpointState:      endpointState,
				TapConnected:       tapStatus.Connected,
			},
			Warmup: BusObservabilityWarmup{
				State:                 store.passive.state,
				Blocker:               passiveBlockerReason(store.passive, now),
				ElapsedSeconds:        passiveElapsedSeconds(store.passive, now),
				CompletedTransactions: store.passive.completedTransactions,
				RequiredTransactions:  store.passive.requiredTransactions,
				CompletionMode:        store.passive.lastCompletionMode,
			},
			TimingQuality: BusObservabilityTimingQuality{
				Active:      store.activeTimingQuality,
				Passive:     store.passiveTimingQuality,
				Busy:        store.passiveTimingQuality,
				Periodicity: store.passiveTimingQuality,
			},
			Degraded: BusObservabilityDegraded{
				Active:  len(reasons) > 0,
				Reasons: reasons,
			},
			Startup:      cloneBusObservabilityStartup(startup),
			FeatureFlags: store.cfg.ObserveFirstFlags.State(),
		},
		Messages: BusObservabilityBoundedList{
			Count:    store.recentLen,
			Capacity: len(store.recent),
		},
		Periodicity: BusObservabilityBoundedList{
			Count:    len(store.periodicity),
			Capacity: store.cfg.ObserveFirstPeriodicityCapacity,
		},
		Counters: BusObservabilityCounters{
			SeriesBudgetOverflowTotal:      store.seriesBudgetOverflowTotal,
			PeriodicityBudgetOverflowTotal: store.periodicityOverflowTotal,
		},
	}
}

func cloneBusObservabilityStartup(source *BusObservabilityStartup) *BusObservabilityStartup {
	if source == nil {
		return nil
	}
	out := *source
	return &out
}
