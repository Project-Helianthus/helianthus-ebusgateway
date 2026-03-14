package main

import (
	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

type graphqlWatchSummaryProviderAdapter struct {
	shadow *ebusgateway.ShadowCache
}

type mcpWatchSummaryProviderAdapter struct {
	shadow *ebusgateway.ShadowCache
}

func newGraphQLWatchSummaryProvider(shadow *ebusgateway.ShadowCache) graphql.WatchSummaryProvider {
	return graphqlWatchSummaryProviderAdapter{shadow: shadow}
}

func newMCPWatchSummaryProvider(shadow *ebusgateway.ShadowCache) mcp.WatchSummaryProvider {
	return mcpWatchSummaryProviderAdapter{shadow: shadow}
}

func (adapter graphqlWatchSummaryProviderAdapter) Snapshot() graphql.WatchSummary {
	if adapter.shadow == nil {
		return graphql.WatchSummary{}
	}
	return mapGraphQLWatchSummary(adapter.shadow.WatchSummary())
}

func (adapter mcpWatchSummaryProviderAdapter) Snapshot() mcp.WatchSummary {
	if adapter.shadow == nil {
		return mcp.WatchSummary{}
	}
	return mapMCPWatchSummary(adapter.shadow.WatchSummary())
}

func mapGraphQLWatchSummary(summary ebusgateway.WatchSummary) graphql.WatchSummary {
	return graphql.WatchSummary{
		Inventory: graphql.WatchSummaryInventory{
			TotalEntries:             summary.Inventory.TotalEntries,
			PinnedEntries:            summary.Inventory.PinnedEntries,
			EvictableEntries:         summary.Inventory.EvictableEntries,
			StaticPinnedFootprint:    summary.Inventory.StaticPinnedFootprint,
			WriteConfirmPinnedActive: summary.Inventory.WriteConfirmPinnedActive,
			StateClasses:             mapGraphQLWatchSummaryClassCounts(summary.Inventory.StateClasses),
			PinClasses:               mapGraphQLWatchSummaryClassCounts(summary.Inventory.PinClasses),
		},
		ActivationCounts: graphql.WatchSummaryActivationCounts{
			CatalogDescriptors: summary.ActivationCounts.CatalogDescriptors,
			ActiveKeys:         summary.ActivationCounts.ActiveKeys,
			SourceClasses:      mapGraphQLWatchSummaryClassCounts(summary.ActivationCounts.SourceClasses),
		},
		FreshnessClasses:              mapGraphQLWatchSummaryClassCounts(summary.FreshnessClasses),
		DirectApplyEligibilityClasses: mapGraphQLWatchSummaryClassCounts(summary.DirectApplyEligibilityClasses),
		Degraded: graphql.WatchSummaryDegraded{
			Active:               summary.Degraded.Active,
			ShadowingEnabled:     summary.Degraded.ShadowingEnabled,
			PinnedBudgetDegraded: summary.Degraded.PinnedBudgetDegraded,
			CompactorDegraded:    summary.Degraded.CompactorDegraded,
			Reasons:              append([]string(nil), summary.Degraded.Reasons...),
		},
	}
}

func mapMCPWatchSummary(summary ebusgateway.WatchSummary) mcp.WatchSummary {
	return mcp.WatchSummary{
		Inventory: mcp.WatchSummaryInventory{
			TotalEntries:             summary.Inventory.TotalEntries,
			PinnedEntries:            summary.Inventory.PinnedEntries,
			EvictableEntries:         summary.Inventory.EvictableEntries,
			StaticPinnedFootprint:    summary.Inventory.StaticPinnedFootprint,
			WriteConfirmPinnedActive: summary.Inventory.WriteConfirmPinnedActive,
			StateClasses:             mapMCPWatchSummaryClassCounts(summary.Inventory.StateClasses),
			PinClasses:               mapMCPWatchSummaryClassCounts(summary.Inventory.PinClasses),
		},
		ActivationCounts: mcp.WatchSummaryActivationCounts{
			CatalogDescriptors: summary.ActivationCounts.CatalogDescriptors,
			ActiveKeys:         summary.ActivationCounts.ActiveKeys,
			SourceClasses:      mapMCPWatchSummaryClassCounts(summary.ActivationCounts.SourceClasses),
		},
		FreshnessClasses:              mapMCPWatchSummaryClassCounts(summary.FreshnessClasses),
		DirectApplyEligibilityClasses: mapMCPWatchSummaryClassCounts(summary.DirectApplyEligibilityClasses),
		Degraded: mcp.WatchSummaryDegraded{
			Active:               summary.Degraded.Active,
			ShadowingEnabled:     summary.Degraded.ShadowingEnabled,
			PinnedBudgetDegraded: summary.Degraded.PinnedBudgetDegraded,
			CompactorDegraded:    summary.Degraded.CompactorDegraded,
			Reasons:              append([]string(nil), summary.Degraded.Reasons...),
		},
	}
}

func mapGraphQLWatchSummaryClassCounts(items []ebusgateway.WatchSummaryClassCount) []graphql.WatchSummaryClassCount {
	if len(items) == 0 {
		return nil
	}
	out := make([]graphql.WatchSummaryClassCount, len(items))
	for i, item := range items {
		out[i] = graphql.WatchSummaryClassCount{
			Class: item.Class,
			Count: item.Count,
		}
	}
	return out
}

func mapMCPWatchSummaryClassCounts(items []ebusgateway.WatchSummaryClassCount) []mcp.WatchSummaryClassCount {
	if len(items) == 0 {
		return nil
	}
	out := make([]mcp.WatchSummaryClassCount, len(items))
	for i, item := range items {
		out[i] = mcp.WatchSummaryClassCount{
			Class: item.Class,
			Count: item.Count,
		}
	}
	return out
}
