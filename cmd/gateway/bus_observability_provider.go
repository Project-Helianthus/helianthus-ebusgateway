package main

import (
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

type graphqlBusObservabilityProviderAdapter struct {
	store *ebusgateway.BusObservabilityStore
}

type mcpBusObservabilityProviderAdapter struct {
	store *ebusgateway.BusObservabilityStore
}

func newGraphQLBusObservabilityProvider(store *ebusgateway.BusObservabilityStore) graphql.BusObservabilityProvider {
	return graphqlBusObservabilityProviderAdapter{store: store}
}

func newMCPBusObservabilityProvider(store *ebusgateway.BusObservabilityStore) mcp.BusObservabilityProvider {
	return mcpBusObservabilityProviderAdapter{store: store}
}

func (adapter graphqlBusObservabilityProviderAdapter) Snapshot() graphql.BusObservabilitySnapshot {
	if adapter.store == nil {
		return graphql.BusObservabilitySnapshot{}
	}

	snapshot := adapter.store.Snapshot()
	return graphql.BusObservabilitySnapshot{
		Summary:     mapGraphQLBusSummary(snapshot.Summary),
		Messages:    mapGraphQLBusMessages(snapshot.Messages),
		Periodicity: mapGraphQLBusPeriodicity(snapshot.Periodicity),
	}
}

func (adapter mcpBusObservabilityProviderAdapter) Snapshot() mcp.BusObservabilitySnapshot {
	if adapter.store == nil {
		return mcp.BusObservabilitySnapshot{}
	}

	snapshot := adapter.store.Snapshot()
	return mcp.BusObservabilitySnapshot{
		Summary:     mapMCPBusSummary(snapshot.Summary),
		Messages:    mapMCPBusMessages(snapshot.Messages),
		Periodicity: mapMCPBusPeriodicity(snapshot.Periodicity),
	}
}

func mapGraphQLBusSummary(summary ebusgateway.BusObservabilitySummary) *graphql.BusSummary {
	return &graphql.BusSummary{
		LastUpdatedAt: cloneTimePtr(summary.LastUpdatedAt),
		Status:        mapGraphQLBusStatus(summary.Status),
		Messages: graphql.BusBoundedListSummary{
			Count:    summary.Messages.Count,
			Capacity: summary.Messages.Capacity,
		},
		Periodicity: graphql.BusBoundedListSummary{
			Count:    summary.Periodicity.Count,
			Capacity: summary.Periodicity.Capacity,
		},
		Counters: graphql.BusObservabilityCounters{
			SeriesBudgetOverflowTotal:      summary.Counters.SeriesBudgetOverflowTotal,
			PeriodicityBudgetOverflowTotal: summary.Counters.PeriodicityBudgetOverflowTotal,
		},
	}
}

func mapMCPBusSummary(summary ebusgateway.BusObservabilitySummary) *mcp.BusSummary {
	return &mcp.BusSummary{
		LastUpdatedAt: cloneTimePtr(summary.LastUpdatedAt),
		Status:        mapMCPBusStatus(summary.Status),
		Messages: mcp.BusBoundedListSummary{
			Count:    summary.Messages.Count,
			Capacity: summary.Messages.Capacity,
		},
		Periodicity: mcp.BusBoundedListSummary{
			Count:    summary.Periodicity.Count,
			Capacity: summary.Periodicity.Capacity,
		},
		Counters: mcp.BusObservabilityCounters{
			SeriesBudgetOverflowTotal:      summary.Counters.SeriesBudgetOverflowTotal,
			PeriodicityBudgetOverflowTotal: summary.Counters.PeriodicityBudgetOverflowTotal,
		},
	}
}

func mapGraphQLBusStatus(status ebusgateway.BusObservabilityStatus) *graphql.BusObservabilityStatus {
	return &graphql.BusObservabilityStatus{
		LastUpdatedAt:  cloneTimePtr(status.LastUpdatedAt),
		TransportClass: status.TransportClass,
		Capability: graphql.BusObservabilityCapability{
			ActiveSupported:    status.Capability.ActiveSupported,
			PassiveSupported:   status.Capability.PassiveSupported,
			BroadcastSupported: status.Capability.BroadcastSupported,
			PassiveAvailable:   status.Capability.PassiveAvailable,
			PassiveState:       status.Capability.PassiveState,
			PassiveReason:      status.Capability.PassiveReason,
			EndpointState:      status.Capability.EndpointState,
			TapConnected:       status.Capability.TapConnected,
		},
		Warmup: graphql.BusObservabilityWarmup{
			State:                 status.Warmup.State,
			Blocker:               status.Warmup.Blocker,
			ElapsedSeconds:        status.Warmup.ElapsedSeconds,
			CompletedTransactions: status.Warmup.CompletedTransactions,
			RequiredTransactions:  status.Warmup.RequiredTransactions,
			CompletionMode:        status.Warmup.CompletionMode,
		},
		TimingQuality: graphql.BusObservabilityTimingQuality{
			Active:      status.TimingQuality.Active,
			Passive:     status.TimingQuality.Passive,
			Busy:        status.TimingQuality.Busy,
			Periodicity: status.TimingQuality.Periodicity,
		},
		Degraded: graphql.BusObservabilityDegraded{
			Active:  status.Degraded.Active,
			Reasons: append([]string(nil), status.Degraded.Reasons...),
		},
		Startup: mapGraphQLBusStartup(status.Startup),
		FeatureFlags: graphql.ObserveFirstFeatureFlagState{
			ObserveFirstEnabled:      status.FeatureFlags.ObserveFirstEnabled,
			PassiveStateDirectApply:  status.FeatureFlags.PassiveStateDirectApply,
			PassiveConfigDirectApply: status.FeatureFlags.PassiveConfigDirectApply,
			ExternalWritePolicy:      string(status.FeatureFlags.ExternalWritePolicy),
			LastUpdatedAt:            cloneTimePtr(status.FeatureFlags.LastUpdatedAt),
			Normalizations:           append([]string(nil), status.FeatureFlags.Normalizations...),
		},
	}
}

func mapMCPBusStatus(status ebusgateway.BusObservabilityStatus) *mcp.BusObservabilityStatus {
	return &mcp.BusObservabilityStatus{
		LastUpdatedAt:  cloneTimePtr(status.LastUpdatedAt),
		TransportClass: status.TransportClass,
		Capability: mcp.BusObservabilityCapability{
			ActiveSupported:    status.Capability.ActiveSupported,
			PassiveSupported:   status.Capability.PassiveSupported,
			BroadcastSupported: status.Capability.BroadcastSupported,
			PassiveAvailable:   status.Capability.PassiveAvailable,
			PassiveState:       status.Capability.PassiveState,
			PassiveReason:      status.Capability.PassiveReason,
			EndpointState:      status.Capability.EndpointState,
			TapConnected:       status.Capability.TapConnected,
		},
		Warmup: mcp.BusObservabilityWarmup{
			State:                 status.Warmup.State,
			Blocker:               status.Warmup.Blocker,
			ElapsedSeconds:        status.Warmup.ElapsedSeconds,
			CompletedTransactions: status.Warmup.CompletedTransactions,
			RequiredTransactions:  status.Warmup.RequiredTransactions,
			CompletionMode:        status.Warmup.CompletionMode,
		},
		TimingQuality: mcp.BusObservabilityTimingQuality{
			Active:      status.TimingQuality.Active,
			Passive:     status.TimingQuality.Passive,
			Busy:        status.TimingQuality.Busy,
			Periodicity: status.TimingQuality.Periodicity,
		},
		Degraded: mcp.BusObservabilityDegraded{
			Active:  status.Degraded.Active,
			Reasons: append([]string(nil), status.Degraded.Reasons...),
		},
		Startup: mapMCPBusStartup(status.Startup),
		FeatureFlags: mcp.ObserveFirstFeatureFlagState{
			ObserveFirstEnabled:      status.FeatureFlags.ObserveFirstEnabled,
			PassiveStateDirectApply:  status.FeatureFlags.PassiveStateDirectApply,
			PassiveConfigDirectApply: status.FeatureFlags.PassiveConfigDirectApply,
			ExternalWritePolicy:      string(status.FeatureFlags.ExternalWritePolicy),
			LastUpdatedAt:            cloneTimePtr(status.FeatureFlags.LastUpdatedAt),
			Normalizations:           append([]string(nil), status.FeatureFlags.Normalizations...),
		},
	}
}

func mapGraphQLBusStartup(startup *ebusgateway.BusObservabilityStartup) *graphql.BusObservabilityStartup {
	if startup == nil {
		return nil
	}
	return &graphql.BusObservabilityStartup{
		LastUpdatedAt: cloneTimePtr(startup.LastUpdatedAt),
		Phase:         startup.Phase,
		CacheEpoch:    startup.CacheEpoch,
		LiveEpoch:     startup.LiveEpoch,
	}
}

func mapMCPBusStartup(startup *ebusgateway.BusObservabilityStartup) *mcp.BusObservabilityStartup {
	if startup == nil {
		return nil
	}
	return &mcp.BusObservabilityStartup{
		LastUpdatedAt: cloneTimePtr(startup.LastUpdatedAt),
		Phase:         startup.Phase,
		CacheEpoch:    startup.CacheEpoch,
		LiveEpoch:     startup.LiveEpoch,
	}
}

func mapGraphQLBusMessages(records []ebusgateway.BusMessageRecord) []graphql.BusMessage {
	if len(records) == 0 {
		return nil
	}

	items := make([]graphql.BusMessage, len(records))
	for i, record := range records {
		items[i] = graphql.BusMessage{
			Scope:         record.Scope,
			Family:        record.Family,
			FrameType:     record.FrameType,
			Outcome:       record.Outcome,
			ObservedAt:    timestampString(record.ObservedAt),
			SourceAddress: int(record.Source),
			TargetAddress: int(record.Target),
			RequestLen:    record.RequestLen,
			ResponseLen:   record.ResponseLen,
		}
	}
	return items
}

func mapMCPBusMessages(records []ebusgateway.BusMessageRecord) []mcp.BusMessage {
	if len(records) == 0 {
		return nil
	}

	items := make([]mcp.BusMessage, len(records))
	for i, record := range records {
		items[i] = mcp.BusMessage{
			Scope:         record.Scope,
			Family:        record.Family,
			FrameType:     record.FrameType,
			Outcome:       record.Outcome,
			ObservedAt:    record.ObservedAt,
			SourceAddress: int(record.Source),
			TargetAddress: int(record.Target),
			RequestLen:    record.RequestLen,
			ResponseLen:   record.ResponseLen,
		}
	}
	return items
}

func mapGraphQLBusPeriodicity(entries []ebusgateway.BusPeriodicityEntry) []graphql.BusPeriodicityEntry {
	if len(entries) == 0 {
		return nil
	}

	items := make([]graphql.BusPeriodicityEntry, len(entries))
	for i, entry := range entries {
		items[i] = graphql.BusPeriodicityEntry{
			SourceBucket: entry.SourceBucket,
			TargetBucket: entry.TargetBucket,
			Primary:      int(entry.Primary),
			Secondary:    int(entry.Secondary),
			Family:       entry.Family,
			State:        entry.State,
			LastSeen:     timestampString(entry.LastSeen),
			SampleCount:  entry.SampleCount,
			LastInterval: durationString(entry.LastInterval),
			MeanInterval: durationString(entry.MeanInterval),
			MinInterval:  durationString(entry.MinInterval),
			MaxInterval:  durationString(entry.MaxInterval),
		}
	}
	return items
}

func mapMCPBusPeriodicity(entries []ebusgateway.BusPeriodicityEntry) []mcp.BusPeriodicityEntry {
	if len(entries) == 0 {
		return nil
	}

	items := make([]mcp.BusPeriodicityEntry, len(entries))
	for i, entry := range entries {
		items[i] = mcp.BusPeriodicityEntry{
			SourceBucket: entry.SourceBucket,
			TargetBucket: entry.TargetBucket,
			Primary:      int(entry.Primary),
			Secondary:    int(entry.Secondary),
			Family:       entry.Family,
			State:        entry.State,
			LastSeen:     entry.LastSeen,
			SampleCount:  entry.SampleCount,
			LastInterval: durationString(entry.LastInterval),
			MeanInterval: durationString(entry.MeanInterval),
			MinInterval:  durationString(entry.MinInterval),
			MaxInterval:  durationString(entry.MaxInterval),
		}
	}
	return items
}

func timestampString(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func cloneTimePtr(source *time.Time) *time.Time {
	if source == nil {
		return nil
	}
	updatedAt := source.UTC()
	return &updatedAt
}

func durationString(value time.Duration) string {
	if value <= 0 {
		return ""
	}
	return value.String()
}
