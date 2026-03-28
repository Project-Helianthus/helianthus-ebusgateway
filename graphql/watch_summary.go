package graphql

import (
	"sync"
	"time"

	graphqlgo "github.com/graphql-go/graphql"
)

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

type staticWatchSummaryProvider struct{}

func (staticWatchSummaryProvider) Snapshot() WatchSummary {
	return WatchSummary{}
}

const watchSummarySnapshotCacheRootKey = "_watchSummarySnapshotCache"

type watchSummarySnapshotCache struct {
	provider WatchSummaryProvider

	once     sync.Once
	snapshot *WatchSummary
}

func newWatchSummarySnapshotCache(builder *Builder) *watchSummarySnapshotCache {
	cache := &watchSummarySnapshotCache{
		provider: staticWatchSummaryProvider{},
	}
	if builder != nil {
		cache.provider = builder.watchSummaryProvider()
	}
	return cache
}

func watchSummarySnapshotCacheFromRoot(rootValue any) *watchSummarySnapshotCache {
	root, ok := rootValue.(map[string]any)
	if !ok {
		return nil
	}
	cache, ok := root[watchSummarySnapshotCacheRootKey].(*watchSummarySnapshotCache)
	if !ok {
		return nil
	}
	return cache
}

func (cache *watchSummarySnapshotCache) Snapshot() *WatchSummary {
	if cache == nil {
		return nil
	}

	cache.once.Do(func() {
		snapshot := cache.provider.Snapshot()
		cache.snapshot = cloneWatchSummary(&snapshot)
	})

	return cloneWatchSummary(cache.snapshot)
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

func graphqlWatchTimeString(source *time.Time) any {
	if source == nil || source.IsZero() {
		return nil
	}
	return source.UTC().Format(time.RFC3339Nano)
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

func buildWatchSummaryType() *graphqlgo.Object {
	classCountType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "WatchSummaryClassCount",
		Fields: graphqlgo.Fields{
			"class": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryClassCount)
					if !ok {
						return "", nil
					}
					return value.Class, nil
				},
			},
			"count": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryClassCount)
					if !ok {
						return 0, nil
					}
					return value.Count, nil
				},
			},
		},
	})

	inventoryType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "WatchSummaryInventory",
		Fields: graphqlgo.Fields{
			"totalEntries": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryInventory)
					if !ok {
						return 0, nil
					}
					return value.TotalEntries, nil
				},
			},
			"pinnedEntries": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryInventory)
					if !ok {
						return 0, nil
					}
					return value.PinnedEntries, nil
				},
			},
			"evictableEntries": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryInventory)
					if !ok {
						return 0, nil
					}
					return value.EvictableEntries, nil
				},
			},
			"staticPinnedFootprint": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryInventory)
					if !ok {
						return 0, nil
					}
					return value.StaticPinnedFootprint, nil
				},
			},
			"writeConfirmPinnedActive": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryInventory)
					if !ok {
						return 0, nil
					}
					return value.WriteConfirmPinnedActive, nil
				},
			},
			"stateClasses": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(classCountType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryInventory)
					if !ok || len(value.StateClasses) == 0 {
						return []WatchSummaryClassCount{}, nil
					}
					return value.StateClasses, nil
				},
			},
			"pinClasses": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(classCountType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryInventory)
					if !ok || len(value.PinClasses) == 0 {
						return []WatchSummaryClassCount{}, nil
					}
					return value.PinClasses, nil
				},
			},
		},
	})

	activationCountsType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "WatchSummaryActivationCounts",
		Fields: graphqlgo.Fields{
			"catalogDescriptors": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryActivationCounts)
					if !ok {
						return 0, nil
					}
					return value.CatalogDescriptors, nil
				},
			},
			"activeKeys": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryActivationCounts)
					if !ok {
						return 0, nil
					}
					return value.ActiveKeys, nil
				},
			},
			"sourceClasses": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(classCountType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryActivationCounts)
					if !ok || len(value.SourceClasses) == 0 {
						return []WatchSummaryClassCount{}, nil
					}
					return value.SourceClasses, nil
				},
			},
		},
	})

	degradedType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "WatchSummaryDegraded",
		Fields: graphqlgo.Fields{
			"active": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryDegraded)
					if !ok {
						return false, nil
					}
					return value.Active, nil
				},
			},
			"shadowingEnabled": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryDegraded)
					if !ok {
						return false, nil
					}
					return value.ShadowingEnabled, nil
				},
			},
			"pinnedBudgetDegraded": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryDegraded)
					if !ok {
						return false, nil
					}
					return value.PinnedBudgetDegraded, nil
				},
			},
			"compactorDegraded": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryDegraded)
					if !ok {
						return false, nil
					}
					return value.CompactorDegraded, nil
				},
			},
			"reasons": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.String))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					value, ok := params.Source.(WatchSummaryDegraded)
					if !ok || len(value.Reasons) == 0 {
						return []string{}, nil
					}
					return append([]string(nil), value.Reasons...), nil
				},
			},
		},
	})

	return graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "WatchSummary",
		Fields: graphqlgo.Fields{
			"lastUpdatedAt": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(*WatchSummary)
					if ok && summary != nil {
						return graphqlWatchTimeString(summary.LastUpdatedAt), nil
					}
					value, ok := params.Source.(WatchSummary)
					if !ok {
						return nil, nil
					}
					return graphqlWatchTimeString(value.LastUpdatedAt), nil
				},
			},
			"inventory": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(inventoryType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(*WatchSummary)
					if ok && summary != nil {
						return summary.Inventory, nil
					}
					value, ok := params.Source.(WatchSummary)
					if !ok {
						return WatchSummaryInventory{}, nil
					}
					return value.Inventory, nil
				},
			},
			"activationCounts": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(activationCountsType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(*WatchSummary)
					if ok && summary != nil {
						return summary.ActivationCounts, nil
					}
					value, ok := params.Source.(WatchSummary)
					if !ok {
						return WatchSummaryActivationCounts{}, nil
					}
					return value.ActivationCounts, nil
				},
			},
			"freshnessClasses": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(classCountType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(*WatchSummary)
					if ok && summary != nil {
						if len(summary.FreshnessClasses) == 0 {
							return []WatchSummaryClassCount{}, nil
						}
						return summary.FreshnessClasses, nil
					}
					value, ok := params.Source.(WatchSummary)
					if !ok || len(value.FreshnessClasses) == 0 {
						return []WatchSummaryClassCount{}, nil
					}
					return value.FreshnessClasses, nil
				},
			},
			"directApplyEligibilityClasses": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(classCountType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(*WatchSummary)
					if ok && summary != nil {
						if len(summary.DirectApplyEligibilityClasses) == 0 {
							return []WatchSummaryClassCount{}, nil
						}
						return summary.DirectApplyEligibilityClasses, nil
					}
					value, ok := params.Source.(WatchSummary)
					if !ok || len(value.DirectApplyEligibilityClasses) == 0 {
						return []WatchSummaryClassCount{}, nil
					}
					return value.DirectApplyEligibilityClasses, nil
				},
			},
			"degraded": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(degradedType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(*WatchSummary)
					if ok && summary != nil {
						return summary.Degraded, nil
					}
					value, ok := params.Source.(WatchSummary)
					if !ok {
						return WatchSummaryDegraded{}, nil
					}
					return value.Degraded, nil
				},
			},
		},
	})
}

func snapshotWatchSummary(builder *Builder, rootValue any) *WatchSummary {
	if cache := watchSummarySnapshotCacheFromRoot(rootValue); cache != nil {
		return cache.Snapshot()
	}
	if builder == nil {
		return nil
	}
	snapshot := builder.watchSummaryProvider().Snapshot()
	return cloneWatchSummary(&snapshot)
}

func resolveWatchSummary(builder *Builder, rootValue any) *WatchSummary {
	snapshot := snapshotWatchSummary(builder, rootValue)
	if snapshot == nil {
		return &WatchSummary{}
	}
	return cloneWatchSummary(snapshot)
}
