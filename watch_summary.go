package ebusgateway

import "sort"

const (
	watchSummaryDirectApplyClassStateEligible    = "state_eligible"
	watchSummaryDirectApplyClassStateIneligible  = "state_ineligible"
	watchSummaryDirectApplyClassStateMasterOff   = "state_master_off"
	watchSummaryDirectApplyClassConfigEligible   = "config_eligible"
	watchSummaryDirectApplyClassConfigIneligible = "config_ineligible"
	watchSummaryDirectApplyClassConfigMasterOff  = "config_master_off"
	watchSummaryDirectApplyClassNotApplicable    = "not_applicable"
	watchSummaryPinClassStatic                   = "static"
	watchSummaryPinClassWriteConfirm             = "write_confirm"
	watchSummaryPinClassEvictable                = "evictable"
	watchSummaryDegradedReasonPinnedBudget       = "shadow_pinned_budget_degraded"
	watchSummaryDegradedReasonCompactor          = "shadow_compactor_degraded"
	watchSummaryUnknownClass                     = "unknown"
)

var watchSummaryActivationSourceOrder = []string{
	string(WatchActivationSourcePoller),
	string(WatchActivationSourceWriteConfirm),
	string(WatchActivationSourceTooling),
	string(WatchActivationSourceOperator),
}

var watchSummaryFreshnessClassOrder = []string{
	string(WatchFreshnessProfileStateFast),
	string(WatchFreshnessProfileStateSlow),
	string(WatchFreshnessProfileConfig),
	string(WatchFreshnessProfileDiscovery),
	string(WatchFreshnessProfileDebug),
}

var watchSummaryDirectApplyClassOrder = []string{
	watchSummaryDirectApplyClassStateEligible,
	watchSummaryDirectApplyClassStateIneligible,
	watchSummaryDirectApplyClassStateMasterOff,
	watchSummaryDirectApplyClassConfigEligible,
	watchSummaryDirectApplyClassConfigIneligible,
	watchSummaryDirectApplyClassConfigMasterOff,
	watchSummaryDirectApplyClassNotApplicable,
}

var watchSummaryStateClassOrder = []string{
	string(ShadowEntryStatePresent),
	string(ShadowEntryStateInvalidated),
	string(ShadowEntryStateTombstone),
}

var watchSummaryPinClassOrder = []string{
	watchSummaryPinClassStatic,
	watchSummaryPinClassWriteConfirm,
	watchSummaryPinClassEvictable,
}

type WatchSummaryClassCount struct {
	Class string `json:"class"`
	Count int    `json:"count"`
}

type WatchSummaryInventory struct {
	TotalEntries             int                      `json:"total_entries"`
	PinnedEntries            int                      `json:"pinned_entries"`
	EvictableEntries         int                      `json:"evictable_entries"`
	StaticPinnedFootprint    int                      `json:"static_pinned_footprint"`
	WriteConfirmPinnedActive int                      `json:"write_confirm_pinned_active"`
	StateClasses             []WatchSummaryClassCount `json:"state_classes"`
	PinClasses               []WatchSummaryClassCount `json:"pin_classes"`
}

type WatchSummaryActivationCounts struct {
	CatalogDescriptors int                      `json:"catalog_descriptors"`
	ActiveKeys         int                      `json:"active_keys"`
	SourceClasses      []WatchSummaryClassCount `json:"source_classes"`
}

type WatchSummaryDegraded struct {
	Active               bool     `json:"active"`
	ShadowingEnabled     bool     `json:"shadowing_enabled"`
	PinnedBudgetDegraded bool     `json:"pinned_budget_degraded"`
	CompactorDegraded    bool     `json:"compactor_degraded"`
	Reasons              []string `json:"reasons,omitempty"`
}

type WatchSummary struct {
	Inventory                     WatchSummaryInventory        `json:"inventory"`
	ActivationCounts              WatchSummaryActivationCounts `json:"activation_counts"`
	FreshnessClasses              []WatchSummaryClassCount     `json:"freshness_classes"`
	DirectApplyEligibilityClasses []WatchSummaryClassCount     `json:"direct_apply_eligibility_classes"`
	Degraded                      WatchSummaryDegraded         `json:"degraded"`
}

func (cache *ShadowCache) WatchSummary() WatchSummary {
	if cache == nil {
		return WatchSummary{}
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	sourceCounts := make(map[string]int, len(watchSummaryActivationSourceOrder))
	freshnessCounts := make(map[string]int, len(watchSummaryFreshnessClassOrder))
	directApplyCounts := make(map[string]int, len(watchSummaryDirectApplyClassOrder))
	stateCounts := make(map[string]int, len(watchSummaryStateClassOrder))
	pinCounts := make(map[string]int, len(watchSummaryPinClassOrder))

	inventory := WatchSummaryInventory{
		TotalEntries:          len(cache.entries),
		StaticPinnedFootprint: cache.staticPinnedFootprint(),
	}
	activation := WatchSummaryActivationCounts{
		CatalogDescriptors: cache.catalog.Len(),
	}

	for _, descriptor := range cache.catalog.Descriptors() {
		if descriptor.Key == nil {
			continue
		}
		activeSources := cache.activations.ActiveSources(descriptor.Key)
		if len(activeSources) == 0 {
			continue
		}
		activation.ActiveKeys++

		freshnessClass := string(descriptor.FreshnessProfile)
		if freshnessClass == "" {
			freshnessClass = watchSummaryUnknownClass
		}
		freshnessCounts[freshnessClass]++

		directApplyClass := watchSummaryDirectApplyClass(descriptor.SemanticClass, cache.featureFlags)
		directApplyCounts[directApplyClass]++

		for _, source := range activeSources {
			sourceClass := string(source)
			if sourceClass == "" {
				sourceClass = watchSummaryUnknownClass
			}
			sourceCounts[sourceClass]++
		}
	}

	for _, entry := range cache.entries {
		cache.syncEntryPinLocked(entry)

		stateClass := string(entry.state)
		if stateClass == "" {
			stateClass = watchSummaryUnknownClass
		}
		stateCounts[stateClass]++

		pinClass := watchSummaryPinClass(entry.pinClass)
		pinCounts[pinClass]++
		switch entry.pinClass {
		case shadowPinClassNone:
			inventory.EvictableEntries++
		case shadowPinClassWriteConfirm:
			inventory.PinnedEntries++
			inventory.WriteConfirmPinnedActive++
		default:
			inventory.PinnedEntries++
		}
	}

	reasons := make([]string, 0, 2)
	pinnedBudgetDegraded := cache.pinnedBudgetDegraded.Load()
	compactorDegraded := cache.compactorDegraded.Load()
	if pinnedBudgetDegraded {
		reasons = append(reasons, watchSummaryDegradedReasonPinnedBudget)
	}
	if compactorDegraded {
		reasons = append(reasons, watchSummaryDegradedReasonCompactor)
	}
	sort.Strings(reasons)

	return WatchSummary{
		Inventory:        inventory.withClasses(stateCounts, pinCounts),
		ActivationCounts: activation.withClasses(sourceCounts),
		FreshnessClasses: watchSummaryOrderedClassCounts(watchSummaryFreshnessClassOrder, freshnessCounts, true),
		DirectApplyEligibilityClasses: watchSummaryOrderedClassCounts(
			watchSummaryDirectApplyClassOrder,
			directApplyCounts,
			true,
		),
		Degraded: WatchSummaryDegraded{
			Active:               len(reasons) > 0,
			ShadowingEnabled:     cache.shadowingEnabled(),
			PinnedBudgetDegraded: pinnedBudgetDegraded,
			CompactorDegraded:    compactorDegraded,
			Reasons:              reasons,
		},
	}
}

func (inventory WatchSummaryInventory) withClasses(stateCounts map[string]int, pinCounts map[string]int) WatchSummaryInventory {
	inventory.StateClasses = watchSummaryOrderedClassCounts(watchSummaryStateClassOrder, stateCounts, true)
	inventory.PinClasses = watchSummaryOrderedClassCounts(watchSummaryPinClassOrder, pinCounts, true)
	return inventory
}

func (activation WatchSummaryActivationCounts) withClasses(sourceCounts map[string]int) WatchSummaryActivationCounts {
	activation.SourceClasses = watchSummaryOrderedClassCounts(watchSummaryActivationSourceOrder, sourceCounts, true)
	return activation
}

func watchSummaryDirectApplyClass(class WatchSemanticClass, featureFlags ObserveFirstFeatureFlags) string {
	switch class {
	case WatchSemanticClassState:
		if !featureFlags.ObserveFirstEnabled() {
			return watchSummaryDirectApplyClassStateMasterOff
		}
		if featureFlags.PassiveStateDirectApply() {
			return watchSummaryDirectApplyClassStateEligible
		}
		return watchSummaryDirectApplyClassStateIneligible
	case WatchSemanticClassConfig:
		if !featureFlags.ObserveFirstEnabled() {
			return watchSummaryDirectApplyClassConfigMasterOff
		}
		if featureFlags.PassiveConfigDirectApply() {
			return watchSummaryDirectApplyClassConfigEligible
		}
		return watchSummaryDirectApplyClassConfigIneligible
	default:
		return watchSummaryDirectApplyClassNotApplicable
	}
}

func watchSummaryPinClass(pinClass shadowPinClass) string {
	switch pinClass {
	case shadowPinClassStatic:
		return watchSummaryPinClassStatic
	case shadowPinClassWriteConfirm:
		return watchSummaryPinClassWriteConfirm
	default:
		return watchSummaryPinClassEvictable
	}
}

func watchSummaryOrderedClassCounts(ordered []string, counts map[string]int, includeZero bool) []WatchSummaryClassCount {
	seen := make(map[string]struct{}, len(ordered))
	out := make([]WatchSummaryClassCount, 0, len(ordered)+len(counts))

	for _, class := range ordered {
		seen[class] = struct{}{}
		count := counts[class]
		if includeZero || count > 0 {
			out = append(out, WatchSummaryClassCount{
				Class: class,
				Count: count,
			})
		}
	}

	extra := make([]string, 0, len(counts))
	for class := range counts {
		if _, ok := seen[class]; ok {
			continue
		}
		extra = append(extra, class)
	}
	sort.Strings(extra)
	for _, class := range extra {
		count := counts[class]
		if includeZero || count > 0 {
			out = append(out, WatchSummaryClassCount{
				Class: class,
				Count: count,
			})
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
