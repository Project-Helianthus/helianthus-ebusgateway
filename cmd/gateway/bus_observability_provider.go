package main

import (
	"fmt"
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

func (adapter mcpBusObservabilityProviderAdapter) ProtocolSpecimens(family string) []mcp.BusProtocolSpecimen {
	if adapter.store == nil {
		return nil
	}
	exports := adapter.store.ProtocolSpecimens(family)
	return mapMCPProtocolSpecimens(exports)
}

func mapGraphQLBusSummary(summary ebusgateway.BusObservabilitySummary) *graphql.BusSummary {
	out := &graphql.BusSummary{
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
	out.Errors = mapGraphQLBusErrors(summary.Errors)
	out.Frames = mapGraphQLBusFrames(summary.Frames)
	out.Busy = mapGraphQLBusBusy(summary.Busy)
	out.Reconstructor = mapGraphQLBusReconstructor(summary.Reconstructor)
	out.SpecimenFamilies = summary.SpecimenFamilies
	out.SpecimenCount = summary.SpecimenCount
	return out
}

func mapGraphQLBusErrors(errors []ebusgateway.BusErrorAggregate) []graphql.BusErrorAggregate {
	if len(errors) == 0 {
		return nil
	}
	items := make([]graphql.BusErrorAggregate, len(errors))
	for i, e := range errors {
		items[i] = graphql.BusErrorAggregate{
			Scope: e.Scope,
			Class: e.Class,
			Phase: e.Phase,
			Count: e.Count,
		}
	}
	return items
}

func mapGraphQLBusFrames(frames []ebusgateway.BusFrameAggregate) []graphql.BusFrameAggregate {
	if len(frames) == 0 {
		return nil
	}
	items := make([]graphql.BusFrameAggregate, len(frames))
	for i, f := range frames {
		items[i] = graphql.BusFrameAggregate{
			Scope:     f.Scope,
			Source:    f.Source,
			Target:    f.Target,
			Family:    f.Family,
			FrameType: f.FrameType,
			Count:     f.Count,
		}
	}
	return items
}

func mapGraphQLBusBusy(busy *ebusgateway.BusBusyAggregate) *graphql.BusBusyAggregate {
	if busy == nil {
		return nil
	}
	out := &graphql.BusBusyAggregate{
		TotalSeconds: busy.TotalSeconds,
	}
	if len(busy.Windows) > 0 {
		out.Windows = make([]graphql.BusBusyWindow, len(busy.Windows))
		for i, w := range busy.Windows {
			out.Windows[i] = graphql.BusBusyWindow{
				Window: w.Window,
				Ratio:  w.Ratio,
			}
		}
	}
	return out
}

func mapGraphQLBusReconstructor(recon *ebusgateway.BusReconstructorAggregate) *graphql.BusReconstructorAggregate {
	if recon == nil {
		return nil
	}
	out := &graphql.BusReconstructorAggregate{}
	if len(recon.Recoveries) > 0 {
		out.Recoveries = make([]graphql.BusReconstructorRecovery, len(recon.Recoveries))
		for i, r := range recon.Recoveries {
			out.Recoveries[i] = graphql.BusReconstructorRecovery{
				Reason: r.Reason,
				Count:  r.Count,
			}
		}
	}
	return out
}

func mapMCPBusSummary(summary ebusgateway.BusObservabilitySummary) *mcp.BusSummary {
	out := &mcp.BusSummary{
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
	out.Errors = mapMCPBusErrors(summary.Errors)
	out.Frames = mapMCPBusFrames(summary.Frames)
	out.Busy = mapMCPBusBusy(summary.Busy)
	out.Reconstructor = mapMCPBusReconstructor(summary.Reconstructor)
	out.SpecimenFamilies = summary.SpecimenFamilies
	out.SpecimenCount = summary.SpecimenCount
	return out
}

func mapMCPProtocolSpecimens(exports []ebusgateway.ProtocolSpecimenExport) []mcp.BusProtocolSpecimen {
	if len(exports) == 0 {
		return nil
	}
	items := make([]mcp.BusProtocolSpecimen, len(exports))
	for i, e := range exports {
		items[i] = mcp.BusProtocolSpecimen{
			Family:      e.Family,
			Source:      fmt.Sprintf("0x%02x", e.Source),
			Target:      fmt.Sprintf("0x%02x", e.Target),
			FrameType:   e.FrameType,
			RequestHex:  e.RequestHex,
			ResponseHex: e.ResponseHex,
			RequestLen:  e.RequestLen,
			ResponseLen: e.ResponseLen,
			Outcome:     e.Outcome,
			FirstSeenAt: timestampString(e.FirstSeenAt),
			LastSeenAt:  timestampString(e.LastSeenAt),
			Count:       e.Count,
		}
	}
	return items
}

func mapMCPBusErrors(errors []ebusgateway.BusErrorAggregate) []mcp.BusErrorAggregate {
	if len(errors) == 0 {
		return nil
	}
	items := make([]mcp.BusErrorAggregate, len(errors))
	for i, e := range errors {
		items[i] = mcp.BusErrorAggregate{
			Scope: e.Scope,
			Class: e.Class,
			Phase: e.Phase,
			Count: e.Count,
		}
	}
	return items
}

func mapMCPBusFrames(frames []ebusgateway.BusFrameAggregate) []mcp.BusFrameAggregate {
	if len(frames) == 0 {
		return nil
	}
	items := make([]mcp.BusFrameAggregate, len(frames))
	for i, f := range frames {
		items[i] = mcp.BusFrameAggregate{
			Scope:     f.Scope,
			Source:    f.Source,
			Target:    f.Target,
			Family:    f.Family,
			FrameType: f.FrameType,
			Count:     f.Count,
		}
	}
	return items
}

func mapMCPBusBusy(busy *ebusgateway.BusBusyAggregate) *mcp.BusBusyAggregate {
	if busy == nil {
		return nil
	}
	out := &mcp.BusBusyAggregate{
		TotalSeconds: busy.TotalSeconds,
	}
	if len(busy.Windows) > 0 {
		out.Windows = make([]mcp.BusBusyWindow, len(busy.Windows))
		for i, w := range busy.Windows {
			out.Windows[i] = mcp.BusBusyWindow{
				Window: w.Window,
				Ratio:  w.Ratio,
			}
		}
	}
	return out
}

func mapMCPBusReconstructor(recon *ebusgateway.BusReconstructorAggregate) *mcp.BusReconstructorAggregate {
	if recon == nil {
		return nil
	}
	out := &mcp.BusReconstructorAggregate{}
	if len(recon.Recoveries) > 0 {
		out.Recoveries = make([]mcp.BusReconstructorRecovery, len(recon.Recoveries))
		for i, r := range recon.Recoveries {
			out.Recoveries[i] = mcp.BusReconstructorRecovery{
				Reason: r.Reason,
				Count:  r.Count,
			}
		}
	}
	return out
}

func mapGraphQLBusStatus(status ebusgateway.BusObservabilityStatus) *graphql.BusObservabilityStatus {
	return &graphql.BusObservabilityStatus{
		LastUpdatedAt:          cloneTimePtr(status.LastUpdatedAt),
		TransportClass:         status.TransportClass,
		PublisherCadenceSec:    status.PublisherCadenceSec,
		PublisherCadenceSource: status.PublisherCadenceSource,
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
		BusAdmission: mapGraphQLBusAdmission(status.BusAdmission),
		Startup:      mapGraphQLBusStartup(status.Startup),
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
		LastUpdatedAt:          cloneTimePtr(status.LastUpdatedAt),
		TransportClass:         status.TransportClass,
		PublisherCadenceSec:    status.PublisherCadenceSec,
		PublisherCadenceSource: status.PublisherCadenceSource,
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
		BusAdmission: mapMCPBusAdmission(status.BusAdmission),
		Startup:      mapMCPBusStartup(status.Startup),
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

func mapGraphQLBusAdmission(admission *ebusgateway.BusAdmission) *graphql.BusAdmission {
	if admission == nil {
		return nil
	}
	return &graphql.BusAdmission{
		State:           admission.State,
		Source:          admission.Source,
		CompanionTarget: admission.CompanionTarget,
		Reason:          admission.Reason,
		SourceSelection: mapGraphQLBusAdmissionSourceSelection(admission.SourceSelection),
	}
}

func mapMCPBusAdmission(admission *ebusgateway.BusAdmission) *mcp.BusAdmission {
	if admission == nil {
		return nil
	}
	return &mcp.BusAdmission{
		State:           admission.State,
		Source:          admission.Source,
		CompanionTarget: admission.CompanionTarget,
		Reason:          admission.Reason,
	}
}

func mapGraphQLBusAdmissionSourceSelection(selection *ebusgateway.BusAdmissionSourceSelection) *graphql.BusAdmissionSourceSelection {
	if selection == nil {
		return nil
	}
	out := &graphql.BusAdmissionSourceSelection{
		State:                   selection.State,
		Mode:                    selection.Mode,
		Outcome:                 selection.Outcome,
		Reason:                  selection.Reason,
		SelectedSource:          cloneAdmissionUint8Ptr(selection.SelectedSource),
		FailedSource:            cloneAdmissionUint8Ptr(selection.FailedSource),
		CompanionTarget:         cloneAdmissionUint8Ptr(selection.CompanionTarget),
		Retryable:               selection.Retryable,
		NextAction:              selection.NextAction,
		LastSuccessfulSource:    cloneAdmissionUint8Ptr(selection.LastSuccessfulSource),
		AutomaticRetryScheduled: selection.AutomaticRetryScheduled,
	}
	if selection.ActiveProbe != nil {
		out.ActiveProbe = &graphql.BusAdmissionActiveProbe{
			Target: cloneAdmissionUint8Ptr(selection.ActiveProbe.Target),
			Opcode: selection.ActiveProbe.Opcode,
			Status: selection.ActiveProbe.Status,
		}
	}
	if len(selection.RejectedCandidates) > 0 {
		out.RejectedCandidates = make([]graphql.BusAdmissionRejectedCandidate, 0, len(selection.RejectedCandidates))
		for _, candidate := range selection.RejectedCandidates {
			out.RejectedCandidates = append(out.RejectedCandidates, graphql.BusAdmissionRejectedCandidate{
				Source:             candidate.Source,
				Reason:             candidate.Reason,
				OccupancyState:     candidate.OccupancyState,
				EvidenceProvenance: candidate.EvidenceProvenance,
			})
		}
	}
	return out
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

func cloneAdmissionUint8Ptr(source *uint8) *uint8 {
	if source == nil {
		return nil
	}
	value := *source
	return &value
}

func durationString(value time.Duration) string {
	if value <= 0 {
		return ""
	}
	return value.String()
}
