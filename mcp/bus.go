package mcp

import "time"

type BusObservabilityCapability struct {
	ActiveSupported    bool   `json:"active_supported"`
	PassiveSupported   bool   `json:"passive_supported"`
	BroadcastSupported bool   `json:"broadcast_supported"`
	PassiveAvailable   bool   `json:"passive_available"`
	PassiveState       string `json:"passive_state"`
	PassiveReason      string `json:"passive_reason,omitempty"`
	EndpointState      string `json:"endpoint_state,omitempty"`
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

type ObserveFirstFeatureFlagState struct {
	ObserveFirstEnabled      bool     `json:"observe_first_enabled"`
	PassiveStateDirectApply  bool     `json:"passive_state_direct_apply"`
	PassiveConfigDirectApply bool     `json:"passive_config_direct_apply"`
	ExternalWritePolicy      string   `json:"external_write_policy"`
	Normalizations           []string `json:"normalizations,omitempty"`
}

type BusBoundedListSummary struct {
	Count    int `json:"count"`
	Capacity int `json:"capacity"`
}

type BusObservabilityCounters struct {
	SeriesBudgetOverflowTotal      uint64 `json:"series_budget_overflow_total"`
	PeriodicityBudgetOverflowTotal uint64 `json:"periodicity_budget_overflow_total"`
}

type BusSummary struct {
	Status      *BusObservabilityStatus  `json:"status,omitempty"`
	Messages    BusBoundedListSummary    `json:"messages"`
	Periodicity BusBoundedListSummary    `json:"periodicity"`
	Counters    BusObservabilityCounters `json:"counters"`
}

type BusMessage struct {
	Scope         string    `json:"scope"`
	Family        string    `json:"family"`
	FrameType     string    `json:"frame_type"`
	Outcome       string    `json:"outcome"`
	ObservedAt    time.Time `json:"observed_at"`
	SourceAddress int       `json:"source_address"`
	TargetAddress int       `json:"target_address"`
	RequestLen    int       `json:"request_len"`
	ResponseLen   int       `json:"response_len"`
}

type BusPeriodicityEntry struct {
	SourceBucket string    `json:"source_bucket"`
	TargetBucket string    `json:"target_bucket"`
	Primary      int       `json:"primary"`
	Secondary    int       `json:"secondary"`
	Family       string    `json:"family"`
	State        string    `json:"state"`
	LastSeen     time.Time `json:"last_seen"`
	SampleCount  int       `json:"sample_count"`
	LastInterval string    `json:"last_interval,omitempty"`
	MeanInterval string    `json:"mean_interval,omitempty"`
	MinInterval  string    `json:"min_interval,omitempty"`
	MaxInterval  string    `json:"max_interval,omitempty"`
}

type BusObservabilitySnapshot struct {
	Summary     *BusSummary           `json:"summary,omitempty"`
	Messages    []BusMessage          `json:"messages,omitempty"`
	Periodicity []BusPeriodicityEntry `json:"periodicity,omitempty"`
}

type BusMessagesList struct {
	Status   *BusObservabilityStatus `json:"status,omitempty"`
	Count    int                     `json:"count"`
	Capacity int                     `json:"capacity"`
	Items    []BusMessage            `json:"items,omitempty"`
}

type BusPeriodicityList struct {
	Status   *BusObservabilityStatus `json:"status,omitempty"`
	Count    int                     `json:"count"`
	Capacity int                     `json:"capacity"`
	Items    []BusPeriodicityEntry   `json:"items,omitempty"`
}

type BusObservabilityProvider interface {
	Snapshot() BusObservabilitySnapshot
}

func cloneBusObservabilitySnapshot(source BusObservabilitySnapshot) BusObservabilitySnapshot {
	return BusObservabilitySnapshot{
		Summary:     cloneBusSummary(source.Summary),
		Messages:    cloneBusMessages(source.Messages),
		Periodicity: cloneBusPeriodicity(source.Periodicity),
	}
}

func cloneBusSummary(source *BusSummary) *BusSummary {
	if source == nil {
		return nil
	}
	out := *source
	out.Status = cloneBusObservabilityStatus(source.Status)
	return &out
}

func cloneBusObservabilityStatus(source *BusObservabilityStatus) *BusObservabilityStatus {
	if source == nil {
		return nil
	}
	out := *source
	out.Startup = cloneBusObservabilityStartup(source.Startup)
	if len(source.Degraded.Reasons) > 0 {
		out.Degraded.Reasons = append([]string(nil), source.Degraded.Reasons...)
	}
	if len(source.FeatureFlags.Normalizations) > 0 {
		out.FeatureFlags.Normalizations = append([]string(nil), source.FeatureFlags.Normalizations...)
	}
	return &out
}

func cloneBusObservabilityStartup(source *BusObservabilityStartup) *BusObservabilityStartup {
	if source == nil {
		return nil
	}
	out := *source
	return &out
}

func cloneBusMessages(source []BusMessage) []BusMessage {
	if len(source) == 0 {
		return nil
	}
	out := make([]BusMessage, len(source))
	copy(out, source)
	return out
}

func cloneBusPeriodicity(source []BusPeriodicityEntry) []BusPeriodicityEntry {
	if len(source) == 0 {
		return nil
	}
	out := make([]BusPeriodicityEntry, len(source))
	copy(out, source)
	return out
}

func trimBusMessages(items []BusMessage, limit int) []BusMessage {
	if len(items) == 0 {
		return nil
	}
	if limit <= 0 || limit >= len(items) {
		return cloneBusMessages(items)
	}
	start := len(items) - limit
	out := make([]BusMessage, limit)
	copy(out, items[start:])
	return out
}

func trimBusPeriodicity(items []BusPeriodicityEntry, limit int) []BusPeriodicityEntry {
	if len(items) == 0 {
		return nil
	}
	if limit <= 0 || limit >= len(items) {
		return cloneBusPeriodicity(items)
	}
	start := len(items) - limit
	out := make([]BusPeriodicityEntry, limit)
	copy(out, items[start:])
	return out
}
