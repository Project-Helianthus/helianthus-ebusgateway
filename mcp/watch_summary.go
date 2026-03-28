package mcp

import "time"

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
	StateClasses             []WatchSummaryClassCount `json:"state_classes,omitempty"`
	PinClasses               []WatchSummaryClassCount `json:"pin_classes,omitempty"`
}

type WatchSummaryActivationCounts struct {
	CatalogDescriptors int                      `json:"catalog_descriptors"`
	ActiveKeys         int                      `json:"active_keys"`
	SourceClasses      []WatchSummaryClassCount `json:"source_classes,omitempty"`
}

type WatchSummaryDegraded struct {
	Active               bool     `json:"active"`
	ShadowingEnabled     bool     `json:"shadowing_enabled"`
	PinnedBudgetDegraded bool     `json:"pinned_budget_degraded"`
	CompactorDegraded    bool     `json:"compactor_degraded"`
	Reasons              []string `json:"reasons,omitempty"`
}

type WatchSummary struct {
	LastUpdatedAt                 *time.Time                   `json:"last_updated_at,omitempty"`
	Inventory                     WatchSummaryInventory        `json:"inventory"`
	ActivationCounts              WatchSummaryActivationCounts `json:"activation_counts"`
	FreshnessClasses              []WatchSummaryClassCount     `json:"freshness_classes,omitempty"`
	DirectApplyEligibilityClasses []WatchSummaryClassCount     `json:"direct_apply_eligibility_classes,omitempty"`
	Degraded                      WatchSummaryDegraded         `json:"degraded"`
}

type WatchSummaryProvider interface {
	Snapshot() WatchSummary
}

func cloneWatchSummary(source *WatchSummary) *WatchSummary {
	if source == nil {
		return nil
	}
	out := *source
	out.LastUpdatedAt = cloneTimePtr(source.LastUpdatedAt)
	out.Inventory = cloneWatchSummaryInventory(source.Inventory)
	out.ActivationCounts = cloneWatchSummaryActivationCounts(source.ActivationCounts)
	out.FreshnessClasses = cloneWatchSummaryClassCounts(source.FreshnessClasses)
	out.DirectApplyEligibilityClasses = cloneWatchSummaryClassCounts(source.DirectApplyEligibilityClasses)
	out.Degraded = cloneWatchSummaryDegraded(source.Degraded)
	return &out
}

func cloneWatchSummaryInventory(source WatchSummaryInventory) WatchSummaryInventory {
	out := source
	out.StateClasses = cloneWatchSummaryClassCounts(source.StateClasses)
	out.PinClasses = cloneWatchSummaryClassCounts(source.PinClasses)
	return out
}

func cloneWatchSummaryActivationCounts(source WatchSummaryActivationCounts) WatchSummaryActivationCounts {
	out := source
	out.SourceClasses = cloneWatchSummaryClassCounts(source.SourceClasses)
	return out
}

func cloneWatchSummaryDegraded(source WatchSummaryDegraded) WatchSummaryDegraded {
	out := source
	if len(source.Reasons) > 0 {
		out.Reasons = append([]string(nil), source.Reasons...)
	}
	return out
}

func cloneWatchSummaryClassCounts(source []WatchSummaryClassCount) []WatchSummaryClassCount {
	if len(source) == 0 {
		return nil
	}
	out := make([]WatchSummaryClassCount, len(source))
	copy(out, source)
	return out
}
