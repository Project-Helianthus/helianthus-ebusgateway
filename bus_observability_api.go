package ebusgateway

import (
	"encoding/hex"
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
	LastUpdatedAt *time.Time `json:"last_updated_at,omitempty"`
	Phase         string     `json:"phase"`
	CacheEpoch    uint64     `json:"cache_epoch"`
	LiveEpoch     uint64     `json:"live_epoch"`
}

const busObservabilityPublisherCadenceSource = "config.semantic_state_interval"

type BusObservabilityStatus struct {
	LastUpdatedAt          *time.Time                    `json:"last_updated_at,omitempty"`
	TransportClass         string                        `json:"transport_class"`
	PublisherCadenceSec    float64                       `json:"publisher_cadence_sec"`
	PublisherCadenceSource string                        `json:"publisher_cadence_source"`
	Capability             BusObservabilityCapability    `json:"capability"`
	Warmup                 BusObservabilityWarmup        `json:"warmup"`
	TimingQuality          BusObservabilityTimingQuality `json:"timing_quality"`
	Degraded               BusObservabilityDegraded      `json:"degraded"`
	Startup                *BusObservabilityStartup      `json:"startup,omitempty"`
	FeatureFlags           ObserveFirstFeatureFlagState  `json:"feature_flags"`
}

type BusObservabilityBoundedList struct {
	Count    int `json:"count"`
	Capacity int `json:"capacity"`
}

type BusObservabilityCounters struct {
	SeriesBudgetOverflowTotal      uint64 `json:"series_budget_overflow_total"`
	PeriodicityBudgetOverflowTotal uint64 `json:"periodicity_budget_overflow_total"`
}

type BusErrorAggregate struct {
	Scope string `json:"scope"`
	Class string `json:"class"`
	Phase string `json:"phase"`
	Count uint64 `json:"count"`
}

type BusFrameAggregate struct {
	Scope     string `json:"scope"`
	Source    string `json:"source"`
	Target    string `json:"target"`
	Family    string `json:"family"`
	FrameType string `json:"frame_type"`
	Count     uint64 `json:"count"`
}

type BusBusyWindow struct {
	Window string  `json:"window"`
	Ratio  float64 `json:"ratio"`
}

type BusBusyAggregate struct {
	TotalSeconds float64         `json:"total_seconds"`
	Windows      []BusBusyWindow `json:"windows"`
}

type BusReconstructorRecovery struct {
	Reason string `json:"reason"`
	Count  uint64 `json:"count"`
}

type BusReconstructorAggregate struct {
	Recoveries []BusReconstructorRecovery `json:"recoveries"`
}

type BusObservabilitySummary struct {
	LastUpdatedAt    *time.Time                  `json:"last_updated_at,omitempty"`
	Status           BusObservabilityStatus      `json:"status"`
	Messages         BusObservabilityBoundedList `json:"messages"`
	Periodicity      BusObservabilityBoundedList `json:"periodicity"`
	Counters         BusObservabilityCounters    `json:"counters"`
	Errors           []BusErrorAggregate         `json:"errors,omitempty"`
	Frames           []BusFrameAggregate         `json:"frames,omitempty"`
	Busy             *BusBusyAggregate           `json:"busy,omitempty"`
	Reconstructor    *BusReconstructorAggregate  `json:"reconstructor,omitempty"`
	SpecimenFamilies int                         `json:"specimen_families"`
	SpecimenCount    int                         `json:"specimen_count"`
}

type ProtocolSpecimenExport struct {
	Family      string    `json:"family"`
	Source      byte      `json:"source"`
	Target      byte      `json:"target"`
	FrameType   string    `json:"frame_type"`
	RequestHex  string    `json:"request_hex"`
	ResponseHex string    `json:"response_hex,omitempty"`
	RequestLen  int       `json:"request_len"`
	ResponseLen int       `json:"response_len"`
	Outcome     string    `json:"outcome"`
	FirstSeenAt time.Time `json:"first_seen_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
	Count       uint64    `json:"count"`
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
		Summary:     store.summaryLocked(now, reconstructor.TapStatus, startup, reconstructor),
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

func (store *BusObservabilityStore) summaryLocked(now time.Time, tapStatus PassiveTapStatus, startup *BusObservabilityStartup, reconstructor PassiveReconstructorSnapshot) BusObservabilitySummary {
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
		LastUpdatedAt: cloneTimePtr(store.lastUpdatedAtPtrLocked()),
		Status: BusObservabilityStatus{
			LastUpdatedAt:          store.lastUpdatedAtPtrLocked(),
			TransportClass:         store.transportClass,
			PublisherCadenceSec:    store.cfg.SemanticStateInterval.Seconds(),
			PublisherCadenceSource: busObservabilityPublisherCadenceSource,
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
			FeatureFlags: store.cfg.ObserveFirstFlags.StateAt(store.featureFlagsUpdatedAt),
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
		Errors:           store.errorsSnapshotLocked(),
		Frames:           store.framesSnapshotLocked(),
		Busy:             store.busySnapshotLocked(now),
		Reconstructor:    reconstructorAggregateFromSnapshot(reconstructor),
		SpecimenFamilies: store.specimenFamilyCountLocked(),
		SpecimenCount:    store.specimenTotalCountLocked(),
	}
}

func (store *BusObservabilityStore) errorsSnapshotLocked() []BusErrorAggregate {
	if len(store.errors) == 0 {
		return nil
	}
	items := make([]BusErrorAggregate, 0, len(store.errors))
	for key, count := range store.errors {
		items = append(items, BusErrorAggregate{
			Scope: key.Scope,
			Class: key.Class,
			Phase: key.Phase,
			Count: count,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Scope != items[j].Scope {
			return items[i].Scope < items[j].Scope
		}
		if items[i].Class != items[j].Class {
			return items[i].Class < items[j].Class
		}
		return items[i].Phase < items[j].Phase
	})
	return items
}

func (store *BusObservabilityStore) framesSnapshotLocked() []BusFrameAggregate {
	if len(store.frames) == 0 {
		return nil
	}
	items := make([]BusFrameAggregate, 0, len(store.frames))
	for key, count := range store.frames {
		items = append(items, BusFrameAggregate{
			Scope:     key.Scope,
			Source:    key.Source,
			Target:    key.Target,
			Family:    key.Family,
			FrameType: key.FrameType,
			Count:     count,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Scope != items[j].Scope {
			return items[i].Scope < items[j].Scope
		}
		if items[i].Source != items[j].Source {
			return items[i].Source < items[j].Source
		}
		if items[i].Target != items[j].Target {
			return items[i].Target < items[j].Target
		}
		if items[i].Family != items[j].Family {
			return items[i].Family < items[j].Family
		}
		return items[i].FrameType < items[j].FrameType
	})
	return items
}

func (store *BusObservabilityStore) busySnapshotLocked(now time.Time) *BusBusyAggregate {
	windows := make([]BusBusyWindow, 0, len(busyWindows))
	for _, windowName := range busyWindows {
		window := parseBusyWindow(windowName)
		if window <= 0 {
			continue
		}
		busy := windowBusyDuration(store.busySegments, now, window)
		windows = append(windows, BusBusyWindow{
			Window: windowName,
			Ratio:  clippedRatio(busy, window),
		})
	}
	return &BusBusyAggregate{
		TotalSeconds: store.totalBusy.Seconds(),
		Windows:      windows,
	}
}

func reconstructorAggregateFromSnapshot(snapshot PassiveReconstructorSnapshot) *BusReconstructorAggregate {
	recoveries := make([]BusReconstructorRecovery, 0, len(reconstructorRecoveryKeys))
	for _, reason := range reconstructorRecoveryKeys {
		recoveries = append(recoveries, BusReconstructorRecovery{
			Reason: reason,
			Count:  snapshot.RecoveryTotal[reason],
		})
	}
	return &BusReconstructorAggregate{
		Recoveries: recoveries,
	}
}

func cloneBusObservabilityStartup(source *BusObservabilityStartup) *BusObservabilityStartup {
	if source == nil {
		return nil
	}
	out := *source
	out.LastUpdatedAt = cloneTimePtr(source.LastUpdatedAt)
	return &out
}

func (store *BusObservabilityStore) specimenFamilyCountLocked() int {
	return len(store.specimens)
}

func (store *BusObservabilityStore) specimenTotalCountLocked() int {
	total := 0
	for _, bucket := range store.specimens {
		total += bucket.length
	}
	return total
}

func (store *BusObservabilityStore) ProtocolSpecimens(family string) []ProtocolSpecimenExport {
	if store == nil {
		return nil
	}
	store.mu.RLock()
	defer store.mu.RUnlock()

	var items []ProtocolSpecimenExport

	for fam, bucket := range store.specimens {
		if family != "" && fam != family {
			continue
		}
		for i := 0; i < bucket.length; i++ {
			idx := (bucket.start + i) % specimenMaxPerFamily
			entry := &bucket.entries[idx]
			items = append(items, ProtocolSpecimenExport{
				Family:      entry.Family,
				Source:      entry.Source,
				Target:      entry.Target,
				FrameType:   entry.FrameType,
				RequestHex:  hex.EncodeToString(entry.RequestData),
				ResponseHex: hex.EncodeToString(entry.ResponseData),
				RequestLen:  entry.RequestLen,
				ResponseLen: entry.ResponseLen,
				Outcome:     entry.Outcome,
				FirstSeenAt: entry.FirstSeenAt,
				LastSeenAt:  entry.LastSeenAt,
				Count:       entry.Count,
			})
		}
	}

	// Sort by LastSeenAt descending, then deterministic tiebreakers.
	sort.Slice(items, func(i, j int) bool {
		if !items[i].LastSeenAt.Equal(items[j].LastSeenAt) {
			return items[i].LastSeenAt.After(items[j].LastSeenAt)
		}
		if items[i].Family != items[j].Family {
			return items[i].Family < items[j].Family
		}
		if items[i].Source != items[j].Source {
			return items[i].Source < items[j].Source
		}
		if items[i].Target != items[j].Target {
			return items[i].Target < items[j].Target
		}
		if items[i].FrameType != items[j].FrameType {
			return items[i].FrameType < items[j].FrameType
		}
		if items[i].Outcome != items[j].Outcome {
			return items[i].Outcome < items[j].Outcome
		}
		if items[i].RequestHex != items[j].RequestHex {
			return items[i].RequestHex < items[j].RequestHex
		}
		return items[i].ResponseHex < items[j].ResponseHex
	})

	return items
}

func (store *BusObservabilityStore) lastUpdatedAtPtrLocked() *time.Time {
	if store == nil || store.lastUpdatedAt.IsZero() {
		return nil
	}
	updatedAt := store.lastUpdatedAt.UTC()
	return &updatedAt
}

func cloneTimePtr(source *time.Time) *time.Time {
	if source == nil {
		return nil
	}
	updatedAt := source.UTC()
	return &updatedAt
}
