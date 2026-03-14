package ebusgateway

import (
	"testing"
	"time"
)

func TestShadowCacheWatchSummary_ComputesInventoryActivationAndEligibilityClasses(t *testing.T) {
	base := time.Date(2026, time.March, 13, 10, 0, 0, 0, time.UTC)

	stateKey := NewB509WatchKey(0x15, 0x1001)
	writeConfirmKey := NewB509WatchKey(0x15, 0x1002)
	configKey := NewB524WatchKey(0x15, 0x02, 0x08, 0x00, 0x2001)
	debugKey := NewB524WatchKey(0x15, 0x06, 0x08, 0x00, 0x2002)

	catalog, err := NewWatchCatalog([]WatchDescriptor{
		{
			Key:               stateKey,
			SemanticClass:     WatchSemanticClassState,
			FreshnessProfile:  WatchFreshnessProfileStateFast,
			DecoderID:         "test.watch.state",
			CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
		},
		{
			Key:               writeConfirmKey,
			SemanticClass:     WatchSemanticClassState,
			FreshnessProfile:  WatchFreshnessProfileStateFast,
			DecoderID:         "test.watch.write_confirm",
			CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
		},
		{
			Key:               configKey,
			SemanticClass:     WatchSemanticClassConfig,
			FreshnessProfile:  WatchFreshnessProfileConfig,
			DecoderID:         "test.watch.config",
			CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: WatchDirectApplyPolicyConfigOptIn,
		},
		{
			Key:               debugKey,
			SemanticClass:     WatchSemanticClassDebug,
			FreshnessProfile:  WatchFreshnessProfileDebug,
			DecoderID:         "test.watch.debug",
			CorrelationPolicy: WatchCorrelationPolicyRecordOnly,
			DirectApplyPolicy: WatchDirectApplyPolicyNever,
		},
	})
	if err != nil {
		t.Fatalf("NewWatchCatalog error = %v", err)
	}

	activations := NewWatchActivationSet(catalog)
	if err := activations.Activate(WatchActivationSourcePoller, stateKey, configKey); err != nil {
		t.Fatalf("Activate poller error = %v", err)
	}
	if err := activations.Activate(WatchActivationSourceWriteConfirm, writeConfirmKey); err != nil {
		t.Fatalf("Activate write_confirm error = %v", err)
	}
	if err := activations.Activate(WatchActivationSourceTooling, stateKey, debugKey); err != nil {
		t.Fatalf("Activate tooling error = %v", err)
	}

	cache := NewShadowCache(ShadowCacheOptions{
		Catalog:               catalog,
		Activations:           activations,
		WriteConfirmPinnedCap: 16,
		FeatureFlags:          NormalizeObserveFirstFeatureFlags(true, true, false, ObserveFirstExternalWritePolicyRecordOnly),
		Now:                   func() time.Time { return base },
	})

	writeShadow(t, cache, stateKey, ShadowWriteSourcePassive, base, []byte{0x10})

	startGeneration := cache.CaptureGeneration(writeConfirmKey)
	writeShadowWithStart(t, cache, writeConfirmKey, ShadowWriteSourceActiveConfirmed, base.Add(time.Second), []byte{0x20}, startGeneration)

	writeShadow(t, cache, configKey, ShadowWriteSourcePassive, base.Add(2*time.Second), []byte{0x30})
	cache.Invalidate(ShadowInvalidation{
		Key:           configKey,
		Reason:        ShadowInvalidationReasonExternalWrite,
		Source:        ShadowInvalidationSourceSystem,
		InvalidatedAt: base.Add(3 * time.Second),
	})
	cache.Invalidate(ShadowInvalidation{
		Key:           debugKey,
		Reason:        ShadowInvalidationReasonManual,
		Source:        ShadowInvalidationSourceOperator,
		InvalidatedAt: base.Add(4 * time.Second),
	})

	summary := cache.WatchSummary()

	if summary.Inventory.TotalEntries != 4 {
		t.Fatalf("inventory.total_entries = %d; want 4", summary.Inventory.TotalEntries)
	}
	if summary.Inventory.PinnedEntries != 3 || summary.Inventory.EvictableEntries != 1 {
		t.Fatalf("inventory pinned/evictable = (%d,%d); want (3,1)", summary.Inventory.PinnedEntries, summary.Inventory.EvictableEntries)
	}
	if summary.Inventory.WriteConfirmPinnedActive != 1 {
		t.Fatalf("inventory.write_confirm_pinned_active = %d; want 1", summary.Inventory.WriteConfirmPinnedActive)
	}

	stateClasses := classCountsToMap(summary.Inventory.StateClasses)
	if stateClasses[string(ShadowEntryStatePresent)] != 2 ||
		stateClasses[string(ShadowEntryStateInvalidated)] != 1 ||
		stateClasses[string(ShadowEntryStateTombstone)] != 1 {
		t.Fatalf("inventory.state_classes = %+v; want present=2 invalidated=1 tombstone=1", stateClasses)
	}

	pinClasses := classCountsToMap(summary.Inventory.PinClasses)
	if pinClasses[watchSummaryPinClassStatic] != 2 ||
		pinClasses[watchSummaryPinClassWriteConfirm] != 1 ||
		pinClasses[watchSummaryPinClassEvictable] != 1 {
		t.Fatalf("inventory.pin_classes = %+v; want static=2 write_confirm=1 evictable=1", pinClasses)
	}

	if summary.ActivationCounts.CatalogDescriptors != 4 || summary.ActivationCounts.ActiveKeys != 4 {
		t.Fatalf("activation_counts = %+v; want catalog_descriptors=4 active_keys=4", summary.ActivationCounts)
	}

	sourceClasses := classCountsToMap(summary.ActivationCounts.SourceClasses)
	if sourceClasses[string(WatchActivationSourcePoller)] != 2 ||
		sourceClasses[string(WatchActivationSourceWriteConfirm)] != 1 ||
		sourceClasses[string(WatchActivationSourceTooling)] != 2 ||
		sourceClasses[string(WatchActivationSourceOperator)] != 0 {
		t.Fatalf("activation source_classes = %+v; want poller=2 write_confirm=1 tooling=2 operator=0", sourceClasses)
	}

	freshnessClasses := classCountsToMap(summary.FreshnessClasses)
	if freshnessClasses[string(WatchFreshnessProfileStateFast)] != 2 ||
		freshnessClasses[string(WatchFreshnessProfileConfig)] != 1 ||
		freshnessClasses[string(WatchFreshnessProfileDebug)] != 1 {
		t.Fatalf("freshness_classes = %+v; want state_fast=2 config=1 debug=1", freshnessClasses)
	}

	directApplyClasses := classCountsToMap(summary.DirectApplyEligibilityClasses)
	if directApplyClasses[watchSummaryDirectApplyClassStateEligible] != 2 ||
		directApplyClasses[watchSummaryDirectApplyClassConfigIneligible] != 1 ||
		directApplyClasses[watchSummaryDirectApplyClassNotApplicable] != 1 {
		t.Fatalf("direct_apply_eligibility_classes = %+v; want state_eligible=2 config_ineligible=1 not_applicable=1", directApplyClasses)
	}

	if summary.Degraded.Active {
		t.Fatalf("degraded.active = true; want false")
	}
	if !summary.Degraded.ShadowingEnabled {
		t.Fatalf("degraded.shadowing_enabled = false; want true")
	}
	if summary.Degraded.PinnedBudgetDegraded || summary.Degraded.CompactorDegraded {
		t.Fatalf("degraded markers = %+v; want all false", summary.Degraded)
	}
	if len(summary.Degraded.Reasons) != 0 {
		t.Fatalf("degraded.reasons = %v; want empty", summary.Degraded.Reasons)
	}
}

func TestShadowCacheWatchSummary_ReportsPinnedBudgetDegradedMarker(t *testing.T) {
	keyA := NewB509WatchKey(0x15, 0x1101)
	keyB := NewB509WatchKey(0x15, 0x1102)
	catalog, err := NewWatchCatalog([]WatchDescriptor{
		{
			Key:               keyA,
			SemanticClass:     WatchSemanticClassState,
			FreshnessProfile:  WatchFreshnessProfileStateFast,
			DecoderID:         "test.watch.degraded.a",
			CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
		},
		{
			Key:               keyB,
			SemanticClass:     WatchSemanticClassState,
			FreshnessProfile:  WatchFreshnessProfileStateFast,
			DecoderID:         "test.watch.degraded.b",
			CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
		},
	})
	if err != nil {
		t.Fatalf("NewWatchCatalog error = %v", err)
	}
	activations := NewWatchActivationSet(catalog)
	if err := activations.Activate(WatchActivationSourcePoller, keyA, keyB); err != nil {
		t.Fatalf("Activate poller error = %v", err)
	}

	cache := NewShadowCache(ShadowCacheOptions{
		Catalog:               catalog,
		Activations:           activations,
		Capacity:              4,
		PinnedCapacity:        1,
		WriteConfirmPinnedCap: 0,
		FeatureFlags:          NormalizeObserveFirstFeatureFlags(true, true, false, ObserveFirstExternalWritePolicyRecordOnly),
	})

	summary := cache.WatchSummary()
	if !summary.Degraded.Active {
		t.Fatal("degraded.active = false; want true while pinned budget degraded")
	}
	if summary.Degraded.ShadowingEnabled {
		t.Fatal("degraded.shadowing_enabled = true; want false while pinned budget degraded")
	}
	if !summary.Degraded.PinnedBudgetDegraded {
		t.Fatal("degraded.pinned_budget_degraded = false; want true")
	}
	reasons := make(map[string]struct{}, len(summary.Degraded.Reasons))
	for _, reason := range summary.Degraded.Reasons {
		reasons[reason] = struct{}{}
	}
	if _, ok := reasons[watchSummaryDegradedReasonPinnedBudget]; !ok {
		t.Fatalf("degraded.reasons = %v; want %q", summary.Degraded.Reasons, watchSummaryDegradedReasonPinnedBudget)
	}
}

func classCountsToMap(items []WatchSummaryClassCount) map[string]int {
	out := make(map[string]int, len(items))
	for _, item := range items {
		out[item.Class] = item.Count
	}
	return out
}
