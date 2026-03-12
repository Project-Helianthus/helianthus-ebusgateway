package main

import (
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

type mcpBusObservabilityProviderAdapter struct {
	store *ebusgateway.BusObservabilityStore
}

func newMCPBusObservabilityProvider(store *ebusgateway.BusObservabilityStore) mcp.BusObservabilityProvider {
	return mcpBusObservabilityProviderAdapter{store: store}
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

func mapMCPBusSummary(summary ebusgateway.BusObservabilitySummary) *mcp.BusSummary {
	return &mcp.BusSummary{
		Status: mapMCPBusStatus(summary.Status),
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

func mapMCPBusStatus(status ebusgateway.BusObservabilityStatus) *mcp.BusObservabilityStatus {
	return &mcp.BusObservabilityStatus{
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
	}
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

func durationString(value time.Duration) string {
	if value <= 0 {
		return ""
	}
	return value.String()
}
