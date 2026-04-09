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
	LastUpdatedAt *time.Time `json:"last_updated_at,omitempty"`
	Phase         string     `json:"phase"`
	CacheEpoch    uint64     `json:"cache_epoch"`
	LiveEpoch     uint64     `json:"live_epoch"`
}

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

type ObserveFirstFeatureFlagState struct {
	ObserveFirstEnabled      bool       `json:"observe_first_enabled"`
	PassiveStateDirectApply  bool       `json:"passive_state_direct_apply"`
	PassiveConfigDirectApply bool       `json:"passive_config_direct_apply"`
	ExternalWritePolicy      string     `json:"external_write_policy"`
	LastUpdatedAt            *time.Time `json:"last_updated_at,omitempty"`
	Normalizations           []string   `json:"normalizations,omitempty"`
}

type BusBoundedListSummary struct {
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

type BusSummary struct {
	LastUpdatedAt    *time.Time                 `json:"last_updated_at,omitempty"`
	Status           *BusObservabilityStatus    `json:"status,omitempty"`
	Messages         BusBoundedListSummary      `json:"messages"`
	Periodicity      BusBoundedListSummary      `json:"periodicity"`
	Counters         BusObservabilityCounters   `json:"counters"`
	Errors           []BusErrorAggregate        `json:"errors,omitempty"`
	Frames           []BusFrameAggregate        `json:"frames,omitempty"`
	Busy             *BusBusyAggregate          `json:"busy,omitempty"`
	Reconstructor    *BusReconstructorAggregate `json:"reconstructor,omitempty"`
	SpecimenFamilies int                        `json:"specimen_families"`
	SpecimenCount    int                        `json:"specimen_count"`
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

type BusProtocolSpecimen struct {
	Family      string `json:"family"`
	Source      string `json:"source"`
	Target      string `json:"target"`
	FrameType   string `json:"frame_type"`
	RequestHex  string `json:"request_hex"`
	ResponseHex string `json:"response_hex,omitempty"`
	RequestLen  int    `json:"request_len"`
	ResponseLen int    `json:"response_len"`
	Outcome     string `json:"outcome"`
	FirstSeenAt string `json:"first_seen_at"`
	LastSeenAt  string `json:"last_seen_at"`
	Count       uint64 `json:"count"`
}

type BusProtocolSpecimenList struct {
	Items []BusProtocolSpecimen `json:"items,omitempty"`
	Count int                   `json:"count"`
}

type BusObservabilityProvider interface {
	Snapshot() BusObservabilitySnapshot
	ProtocolSpecimens(family string) []BusProtocolSpecimen
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
	out.LastUpdatedAt = cloneTimePtr(source.LastUpdatedAt)
	out.Status = cloneBusObservabilityStatus(source.Status)
	if len(source.Errors) > 0 {
		out.Errors = make([]BusErrorAggregate, len(source.Errors))
		copy(out.Errors, source.Errors)
	}
	if len(source.Frames) > 0 {
		out.Frames = make([]BusFrameAggregate, len(source.Frames))
		copy(out.Frames, source.Frames)
	}
	if source.Busy != nil {
		busy := *source.Busy
		if len(source.Busy.Windows) > 0 {
			busy.Windows = make([]BusBusyWindow, len(source.Busy.Windows))
			copy(busy.Windows, source.Busy.Windows)
		}
		out.Busy = &busy
	}
	if source.Reconstructor != nil {
		recon := *source.Reconstructor
		if len(source.Reconstructor.Recoveries) > 0 {
			recon.Recoveries = make([]BusReconstructorRecovery, len(source.Reconstructor.Recoveries))
			copy(recon.Recoveries, source.Reconstructor.Recoveries)
		}
		out.Reconstructor = &recon
	}
	return &out
}

func cloneBusObservabilityStatus(source *BusObservabilityStatus) *BusObservabilityStatus {
	if source == nil {
		return nil
	}
	out := *source
	out.LastUpdatedAt = cloneTimePtr(source.LastUpdatedAt)
	out.Startup = cloneBusObservabilityStartup(source.Startup)
	if len(source.Degraded.Reasons) > 0 {
		out.Degraded.Reasons = append([]string(nil), source.Degraded.Reasons...)
	}
	out.FeatureFlags = cloneObserveFirstFeatureFlagState(source.FeatureFlags)
	return &out
}

func cloneObserveFirstFeatureFlagState(source ObserveFirstFeatureFlagState) ObserveFirstFeatureFlagState {
	out := source
	out.LastUpdatedAt = cloneTimePtr(source.LastUpdatedAt)
	if len(source.Normalizations) > 0 {
		out.Normalizations = append([]string(nil), source.Normalizations...)
	}
	return out
}

func cloneTimePtr(source *time.Time) *time.Time {
	if source == nil {
		return nil
	}
	updatedAt := source.UTC()
	return &updatedAt
}

func cloneBusObservabilityStartup(source *BusObservabilityStartup) *BusObservabilityStartup {
	if source == nil {
		return nil
	}
	out := *source
	out.LastUpdatedAt = cloneTimePtr(source.LastUpdatedAt)
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
