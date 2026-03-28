package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

func TestGraphQLWatchSummaryProviderAdapter_ParityWithMCPAdapter(t *testing.T) {
	now := time.Date(2026, time.March, 13, 9, 0, 0, 0, time.UTC)
	stateKey := ebusgateway.NewB509WatchKey(0x15, 0x1001)
	configKey := ebusgateway.NewB524WatchKey(0x15, 0x02, 0x09, 0x00, 0x2345)

	catalog, err := ebusgateway.NewWatchCatalog([]ebusgateway.WatchDescriptor{
		{
			Key:               stateKey,
			SemanticClass:     ebusgateway.WatchSemanticClassState,
			FreshnessProfile:  ebusgateway.WatchFreshnessProfileStateFast,
			DecoderID:         "test.watch.state",
			CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: ebusgateway.WatchDirectApplyPolicyStateDefault,
		},
		{
			Key:               configKey,
			SemanticClass:     ebusgateway.WatchSemanticClassConfig,
			FreshnessProfile:  ebusgateway.WatchFreshnessProfileConfig,
			DecoderID:         "test.watch.config",
			CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: ebusgateway.WatchDirectApplyPolicyConfigOptIn,
		},
	})
	if err != nil {
		t.Fatalf("NewWatchCatalog error = %v", err)
	}
	activations := ebusgateway.NewWatchActivationSet(catalog)
	if err := activations.Activate(ebusgateway.WatchActivationSourcePoller, stateKey, configKey); err != nil {
		t.Fatalf("Activate poller error = %v", err)
	}

	shadow := ebusgateway.NewShadowCache(ebusgateway.ShadowCacheOptions{
		Catalog:      catalog,
		Activations:  activations,
		FeatureFlags: ebusgateway.NormalizeObserveFirstFeatureFlags(true, true, true, ebusgateway.ObserveFirstExternalWritePolicyRecordAndInvalidate),
		Now:          func() time.Time { return now },
	})
	for _, key := range []ebusgateway.WatchKey{stateKey, configKey} {
		write := shadow.Write(ebusgateway.ShadowWrite{
			Key:        key,
			Source:     ebusgateway.ShadowWriteSourcePassive,
			Confidence: ebusgateway.ShadowConfidenceHigh,
			Value:      []byte{0x01},
			ObservedAt: now,
		})
		if !write.Accepted {
			t.Fatalf("Shadow write for %q rejected: %s", key.Canonical(), write.Reason)
		}
	}

	got := graphQLWatchSummaryToMCPShape(newGraphQLWatchSummaryProvider(shadow).Snapshot())
	want := newMCPWatchSummaryProvider(shadow).Snapshot()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GraphQL/MCP watch summary parity mismatch\nwant: %#v\ngot:  %#v", want, got)
	}
}

func graphQLWatchSummaryToMCPShape(summary graphql.WatchSummary) mcp.WatchSummary {
	return mcp.WatchSummary{
		LastUpdatedAt: cloneTimePtr(summary.LastUpdatedAt),
		Inventory: mcp.WatchSummaryInventory{
			TotalEntries:             summary.Inventory.TotalEntries,
			PinnedEntries:            summary.Inventory.PinnedEntries,
			EvictableEntries:         summary.Inventory.EvictableEntries,
			StaticPinnedFootprint:    summary.Inventory.StaticPinnedFootprint,
			WriteConfirmPinnedActive: summary.Inventory.WriteConfirmPinnedActive,
			StateClasses:             graphQLWatchClassCountsToMCP(summary.Inventory.StateClasses),
			PinClasses:               graphQLWatchClassCountsToMCP(summary.Inventory.PinClasses),
		},
		ActivationCounts: mcp.WatchSummaryActivationCounts{
			CatalogDescriptors: summary.ActivationCounts.CatalogDescriptors,
			ActiveKeys:         summary.ActivationCounts.ActiveKeys,
			SourceClasses:      graphQLWatchClassCountsToMCP(summary.ActivationCounts.SourceClasses),
		},
		FreshnessClasses:              graphQLWatchClassCountsToMCP(summary.FreshnessClasses),
		DirectApplyEligibilityClasses: graphQLWatchClassCountsToMCP(summary.DirectApplyEligibilityClasses),
		Degraded: mcp.WatchSummaryDegraded{
			Active:               summary.Degraded.Active,
			ShadowingEnabled:     summary.Degraded.ShadowingEnabled,
			PinnedBudgetDegraded: summary.Degraded.PinnedBudgetDegraded,
			CompactorDegraded:    summary.Degraded.CompactorDegraded,
			Reasons:              append([]string(nil), summary.Degraded.Reasons...),
		},
	}
}

func graphQLWatchClassCountsToMCP(items []graphql.WatchSummaryClassCount) []mcp.WatchSummaryClassCount {
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
