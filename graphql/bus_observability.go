package graphql

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	graphqlgo "github.com/graphql-go/graphql"
)

type BusObservabilityCapability struct {
	ActiveSupported    bool
	PassiveSupported   bool
	BroadcastSupported bool
	PassiveAvailable   bool
	PassiveState       string
	PassiveReason      string
	EndpointState      string
	TapConnected       bool
}

type BusObservabilityWarmup struct {
	State                 string
	Blocker               string
	ElapsedSeconds        float64
	CompletedTransactions int
	RequiredTransactions  int
	CompletionMode        string
}

type BusObservabilityTimingQuality struct {
	Active      string
	Passive     string
	Busy        string
	Periodicity string
}

type BusObservabilityDegraded struct {
	Active  bool
	Reasons []string
}

type BusObservabilityStartup struct {
	LastUpdatedAt *time.Time
	Phase         string
	CacheEpoch    uint64
	LiveEpoch     uint64
}

type BusObservabilityStatus struct {
	LastUpdatedAt          *time.Time
	TransportClass         string
	PublisherCadenceSec    float64
	PublisherCadenceSource string
	Capability             BusObservabilityCapability
	Warmup                 BusObservabilityWarmup
	TimingQuality          BusObservabilityTimingQuality
	Degraded               BusObservabilityDegraded
	Startup                *BusObservabilityStartup
	FeatureFlags           ObserveFirstFeatureFlagState
}

type ObserveFirstFeatureFlagState struct {
	ObserveFirstEnabled      bool
	PassiveStateDirectApply  bool
	PassiveConfigDirectApply bool
	ExternalWritePolicy      string
	LastUpdatedAt            *time.Time
	Normalizations           []string
}

type BusBoundedListSummary struct {
	Count    int
	Capacity int
}

type BusObservabilityCounters struct {
	SeriesBudgetOverflowTotal      uint64
	PeriodicityBudgetOverflowTotal uint64
}

type BusSummary struct {
	LastUpdatedAt *time.Time
	Status        *BusObservabilityStatus
	Messages      BusBoundedListSummary
	Periodicity   BusBoundedListSummary
	Counters      BusObservabilityCounters
}

type BusMessage struct {
	Scope         string
	Family        string
	FrameType     string
	Outcome       string
	ObservedAt    string
	SourceAddress int
	TargetAddress int
	RequestLen    int
	ResponseLen   int
}

type BusPeriodicityEntry struct {
	SourceBucket string
	TargetBucket string
	Primary      int
	Secondary    int
	Family       string
	State        string
	LastSeen     string
	SampleCount  int
	LastInterval string
	MeanInterval string
	MinInterval  string
	MaxInterval  string
}

type BusObservabilitySnapshot struct {
	Summary     *BusSummary
	Messages    []BusMessage
	Periodicity []BusPeriodicityEntry
}

type BusMessagesList struct {
	Status   *BusObservabilityStatus
	Count    int
	Capacity int
	Items    []BusMessage
}

type BusPeriodicityList struct {
	Status   *BusObservabilityStatus
	Count    int
	Capacity int
	Items    []BusPeriodicityEntry
}

type BusObservabilityProvider interface {
	Snapshot() BusObservabilitySnapshot
}

type staticBusObservabilityProvider struct{}

func (staticBusObservabilityProvider) Snapshot() BusObservabilitySnapshot {
	return BusObservabilitySnapshot{}
}

const busObservabilitySnapshotCacheRootKey = "_busObservabilitySnapshotCache"

type busObservabilitySnapshotCache struct {
	provider BusObservabilityProvider

	once     sync.Once
	snapshot *BusObservabilitySnapshot
}

func newGraphQLRootObject(builder *Builder) map[string]any {
	return map[string]any{
		busObservabilitySnapshotCacheRootKey: newBusObservabilitySnapshotCache(builder),
		watchSummarySnapshotCacheRootKey:     newWatchSummarySnapshotCache(builder),
	}
}

func newBusObservabilitySnapshotCache(builder *Builder) *busObservabilitySnapshotCache {
	cache := &busObservabilitySnapshotCache{
		provider: staticBusObservabilityProvider{},
	}
	if builder != nil {
		cache.provider = builder.busObservabilityProvider()
	}
	return cache
}

func busObservabilitySnapshotCacheFromRoot(rootValue any) *busObservabilitySnapshotCache {
	root, ok := rootValue.(map[string]any)
	if !ok {
		return nil
	}
	cache, ok := root[busObservabilitySnapshotCacheRootKey].(*busObservabilitySnapshotCache)
	if !ok {
		return nil
	}
	return cache
}

func (cache *busObservabilitySnapshotCache) Snapshot() *BusObservabilitySnapshot {
	if cache == nil {
		return nil
	}

	cache.once.Do(func() {
		snapshot := cloneBusObservabilitySnapshot(cache.provider.Snapshot())
		cache.snapshot = &snapshot
	})

	return cache.snapshot
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

func cloneBusObservabilityStartup(source *BusObservabilityStartup) *BusObservabilityStartup {
	if source == nil {
		return nil
	}
	out := *source
	out.LastUpdatedAt = cloneTimePtr(source.LastUpdatedAt)
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

func graphqlTimeString(source *time.Time) any {
	if source == nil || source.IsZero() {
		return nil
	}
	return source.UTC().Format(time.RFC3339Nano)
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

func buildBusObservabilityTypes() (*graphqlgo.Object, *graphqlgo.Object, *graphqlgo.Object) {
	capabilityType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusObservabilityCapability",
		Fields: graphqlgo.Fields{
			"activeSupported": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					capability, ok := params.Source.(BusObservabilityCapability)
					if !ok {
						return false, nil
					}
					return capability.ActiveSupported, nil
				},
			},
			"passiveSupported": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					capability, ok := params.Source.(BusObservabilityCapability)
					if !ok {
						return false, nil
					}
					return capability.PassiveSupported, nil
				},
			},
			"broadcastSupported": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					capability, ok := params.Source.(BusObservabilityCapability)
					if !ok {
						return false, nil
					}
					return capability.BroadcastSupported, nil
				},
			},
			"passiveAvailable": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					capability, ok := params.Source.(BusObservabilityCapability)
					if !ok {
						return false, nil
					}
					return capability.PassiveAvailable, nil
				},
			},
			"passiveState": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					capability, ok := params.Source.(BusObservabilityCapability)
					if !ok {
						return "", nil
					}
					return capability.PassiveState, nil
				},
			},
			"passiveReason": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					capability, ok := params.Source.(BusObservabilityCapability)
					if !ok || capability.PassiveReason == "" {
						return nil, nil
					}
					return capability.PassiveReason, nil
				},
			},
			"endpointState": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					capability, ok := params.Source.(BusObservabilityCapability)
					if !ok {
						return "", nil
					}
					return capability.EndpointState, nil
				},
			},
			"tapConnected": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					capability, ok := params.Source.(BusObservabilityCapability)
					if !ok {
						return false, nil
					}
					return capability.TapConnected, nil
				},
			},
		},
	})

	warmupType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusObservabilityWarmup",
		Fields: graphqlgo.Fields{
			"state": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					warmup, ok := params.Source.(BusObservabilityWarmup)
					if !ok {
						return "", nil
					}
					return warmup.State, nil
				},
			},
			"blocker": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					warmup, ok := params.Source.(BusObservabilityWarmup)
					if !ok || warmup.Blocker == "" {
						return nil, nil
					}
					return warmup.Blocker, nil
				},
			},
			"elapsedSeconds": &graphqlgo.Field{
				Type: graphqlgo.Float,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					warmup, ok := params.Source.(BusObservabilityWarmup)
					if !ok || warmup.ElapsedSeconds <= 0 {
						return nil, nil
					}
					return warmup.ElapsedSeconds, nil
				},
			},
			"completedTransactions": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					warmup, ok := params.Source.(BusObservabilityWarmup)
					if !ok {
						return 0, nil
					}
					return warmup.CompletedTransactions, nil
				},
			},
			"requiredTransactions": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					warmup, ok := params.Source.(BusObservabilityWarmup)
					if !ok {
						return 0, nil
					}
					return warmup.RequiredTransactions, nil
				},
			},
			"completionMode": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					warmup, ok := params.Source.(BusObservabilityWarmup)
					if !ok || warmup.CompletionMode == "" {
						return nil, nil
					}
					return warmup.CompletionMode, nil
				},
			},
		},
	})

	timingQualityType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusObservabilityTimingQuality",
		Fields: graphqlgo.Fields{
			"active": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					timing, ok := params.Source.(BusObservabilityTimingQuality)
					if !ok {
						return "", nil
					}
					return timing.Active, nil
				},
			},
			"passive": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					timing, ok := params.Source.(BusObservabilityTimingQuality)
					if !ok {
						return "", nil
					}
					return timing.Passive, nil
				},
			},
			"busy": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					timing, ok := params.Source.(BusObservabilityTimingQuality)
					if !ok {
						return "", nil
					}
					return timing.Busy, nil
				},
			},
			"periodicity": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					timing, ok := params.Source.(BusObservabilityTimingQuality)
					if !ok {
						return "", nil
					}
					return timing.Periodicity, nil
				},
			},
		},
	})

	degradedType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusObservabilityDegraded",
		Fields: graphqlgo.Fields{
			"active": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					degraded, ok := params.Source.(BusObservabilityDegraded)
					if !ok {
						return false, nil
					}
					return degraded.Active, nil
				},
			},
			"reasons": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.String))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					degraded, ok := params.Source.(BusObservabilityDegraded)
					if !ok || len(degraded.Reasons) == 0 {
						return []string{}, nil
					}
					return append([]string(nil), degraded.Reasons...), nil
				},
			},
		},
	})

	startupType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusObservabilityStartup",
		Fields: graphqlgo.Fields{
			"phase": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					startup, ok := params.Source.(*BusObservabilityStartup)
					if ok && startup != nil {
						return startup.Phase, nil
					}
					value, ok := params.Source.(BusObservabilityStartup)
					if !ok {
						return "", nil
					}
					return value.Phase, nil
				},
			},
			"lastUpdatedAt": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					startup, ok := params.Source.(*BusObservabilityStartup)
					if ok && startup != nil {
						return graphqlTimeString(startup.LastUpdatedAt), nil
					}
					value, ok := params.Source.(BusObservabilityStartup)
					if !ok {
						return nil, nil
					}
					return graphqlTimeString(value.LastUpdatedAt), nil
				},
			},
			"cacheEpoch": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					startup, ok := params.Source.(*BusObservabilityStartup)
					if ok && startup != nil {
						return strconv.FormatUint(startup.CacheEpoch, 10), nil
					}
					value, ok := params.Source.(BusObservabilityStartup)
					if !ok {
						return "0", nil
					}
					return strconv.FormatUint(value.CacheEpoch, 10), nil
				},
			},
			"liveEpoch": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					startup, ok := params.Source.(*BusObservabilityStartup)
					if ok && startup != nil {
						return strconv.FormatUint(startup.LiveEpoch, 10), nil
					}
					value, ok := params.Source.(BusObservabilityStartup)
					if !ok {
						return "0", nil
					}
					return strconv.FormatUint(value.LiveEpoch, 10), nil
				},
			},
		},
	})

	featureFlagStateType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "ObserveFirstFeatureFlagState",
		Fields: graphqlgo.Fields{
			"observeFirstEnabled": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ObserveFirstFeatureFlagState)
					if !ok {
						return false, nil
					}
					return state.ObserveFirstEnabled, nil
				},
			},
			"passiveStateDirectApply": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ObserveFirstFeatureFlagState)
					if !ok {
						return false, nil
					}
					return state.PassiveStateDirectApply, nil
				},
			},
			"passiveConfigDirectApply": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Boolean),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ObserveFirstFeatureFlagState)
					if !ok {
						return false, nil
					}
					return state.PassiveConfigDirectApply, nil
				},
			},
			"externalWritePolicy": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ObserveFirstFeatureFlagState)
					if !ok {
						return "", nil
					}
					return state.ExternalWritePolicy, nil
				},
			},
			"normalizations": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(graphqlgo.String))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ObserveFirstFeatureFlagState)
					if !ok || len(state.Normalizations) == 0 {
						return []string{}, nil
					}
					return append([]string(nil), state.Normalizations...), nil
				},
			},
			"lastUpdatedAt": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					state, ok := params.Source.(ObserveFirstFeatureFlagState)
					if !ok {
						return nil, nil
					}
					return graphqlTimeString(state.LastUpdatedAt), nil
				},
			},
		},
	})

	statusType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusObservabilityStatus",
		Fields: graphqlgo.Fields{
			"transportClass": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BusObservabilityStatus)
					if ok && status != nil {
						return status.TransportClass, nil
					}
					value, ok := params.Source.(BusObservabilityStatus)
					if !ok {
						return "", nil
					}
					return value.TransportClass, nil
				},
			},
			"publisherCadenceSec": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Float),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BusObservabilityStatus)
					if ok && status != nil {
						return status.PublisherCadenceSec, nil
					}
					value, ok := params.Source.(BusObservabilityStatus)
					if !ok {
						return 0.0, nil
					}
					return value.PublisherCadenceSec, nil
				},
			},
			"publisherCadenceSource": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BusObservabilityStatus)
					if ok && status != nil {
						return status.PublisherCadenceSource, nil
					}
					value, ok := params.Source.(BusObservabilityStatus)
					if !ok {
						return "", nil
					}
					return value.PublisherCadenceSource, nil
				},
			},
			"capability": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(capabilityType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BusObservabilityStatus)
					if ok && status != nil {
						return status.Capability, nil
					}
					value, ok := params.Source.(BusObservabilityStatus)
					if !ok {
						return BusObservabilityCapability{}, nil
					}
					return value.Capability, nil
				},
			},
			"warmup": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(warmupType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BusObservabilityStatus)
					if ok && status != nil {
						return status.Warmup, nil
					}
					value, ok := params.Source.(BusObservabilityStatus)
					if !ok {
						return BusObservabilityWarmup{}, nil
					}
					return value.Warmup, nil
				},
			},
			"timingQuality": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(timingQualityType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BusObservabilityStatus)
					if ok && status != nil {
						return status.TimingQuality, nil
					}
					value, ok := params.Source.(BusObservabilityStatus)
					if !ok {
						return BusObservabilityTimingQuality{}, nil
					}
					return value.TimingQuality, nil
				},
			},
			"degraded": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(degradedType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BusObservabilityStatus)
					if ok && status != nil {
						return status.Degraded, nil
					}
					value, ok := params.Source.(BusObservabilityStatus)
					if !ok {
						return BusObservabilityDegraded{}, nil
					}
					return value.Degraded, nil
				},
			},
			"startup": &graphqlgo.Field{
				Type: startupType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BusObservabilityStatus)
					if ok && status != nil {
						return status.Startup, nil
					}
					value, ok := params.Source.(BusObservabilityStatus)
					if !ok {
						return nil, nil
					}
					return value.Startup, nil
				},
			},
			"featureFlags": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(featureFlagStateType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BusObservabilityStatus)
					if ok && status != nil {
						return status.FeatureFlags, nil
					}
					value, ok := params.Source.(BusObservabilityStatus)
					if !ok {
						return ObserveFirstFeatureFlagState{}, nil
					}
					return value.FeatureFlags, nil
				},
			},
			"lastUpdatedAt": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					status, ok := params.Source.(*BusObservabilityStatus)
					if ok && status != nil {
						return graphqlTimeString(status.LastUpdatedAt), nil
					}
					value, ok := params.Source.(BusObservabilityStatus)
					if !ok {
						return nil, nil
					}
					return graphqlTimeString(value.LastUpdatedAt), nil
				},
			},
		},
	})

	boundedListSummaryType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusBoundedListSummary",
		Fields: graphqlgo.Fields{
			"count": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(BusBoundedListSummary)
					if !ok {
						return 0, nil
					}
					return summary.Count, nil
				},
			},
			"capacity": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(BusBoundedListSummary)
					if !ok {
						return 0, nil
					}
					return summary.Capacity, nil
				},
			},
		},
	})

	countersType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusObservabilityCounters",
		Fields: graphqlgo.Fields{
			"seriesBudgetOverflowTotal": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					counters, ok := params.Source.(BusObservabilityCounters)
					if !ok {
						return "0", nil
					}
					return strconv.FormatUint(counters.SeriesBudgetOverflowTotal, 10), nil
				},
			},
			"periodicityBudgetOverflowTotal": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					counters, ok := params.Source.(BusObservabilityCounters)
					if !ok {
						return "0", nil
					}
					return strconv.FormatUint(counters.PeriodicityBudgetOverflowTotal, 10), nil
				},
			},
		},
	})

	busSummaryType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusSummary",
		Fields: graphqlgo.Fields{
			"lastUpdatedAt": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(*BusSummary)
					if ok && summary != nil {
						return graphqlTimeString(summary.LastUpdatedAt), nil
					}
					value, ok := params.Source.(BusSummary)
					if !ok {
						return nil, nil
					}
					return graphqlTimeString(value.LastUpdatedAt), nil
				},
			},
			"status": &graphqlgo.Field{
				Type: statusType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(*BusSummary)
					if ok && summary != nil {
						return summary.Status, nil
					}
					value, ok := params.Source.(BusSummary)
					if !ok {
						return nil, nil
					}
					return value.Status, nil
				},
			},
			"messages": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(boundedListSummaryType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(*BusSummary)
					if ok && summary != nil {
						return summary.Messages, nil
					}
					value, ok := params.Source.(BusSummary)
					if !ok {
						return BusBoundedListSummary{}, nil
					}
					return value.Messages, nil
				},
			},
			"periodicity": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(boundedListSummaryType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(*BusSummary)
					if ok && summary != nil {
						return summary.Periodicity, nil
					}
					value, ok := params.Source.(BusSummary)
					if !ok {
						return BusBoundedListSummary{}, nil
					}
					return value.Periodicity, nil
				},
			},
			"counters": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(countersType),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					summary, ok := params.Source.(*BusSummary)
					if ok && summary != nil {
						return summary.Counters, nil
					}
					value, ok := params.Source.(BusSummary)
					if !ok {
						return BusObservabilityCounters{}, nil
					}
					return value.Counters, nil
				},
			},
		},
	})

	messageType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusMessage",
		Fields: graphqlgo.Fields{
			"scope": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					message, ok := params.Source.(BusMessage)
					if !ok {
						return "", nil
					}
					return message.Scope, nil
				},
			},
			"family": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					message, ok := params.Source.(BusMessage)
					if !ok {
						return "", nil
					}
					return message.Family, nil
				},
			},
			"frameType": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					message, ok := params.Source.(BusMessage)
					if !ok {
						return "", nil
					}
					return message.FrameType, nil
				},
			},
			"outcome": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					message, ok := params.Source.(BusMessage)
					if !ok {
						return "", nil
					}
					return message.Outcome, nil
				},
			},
			"observedAt": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					message, ok := params.Source.(BusMessage)
					if !ok || message.ObservedAt == "" {
						return nil, nil
					}
					return message.ObservedAt, nil
				},
			},
			"sourceAddress": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					message, ok := params.Source.(BusMessage)
					if !ok {
						return 0, nil
					}
					return message.SourceAddress, nil
				},
			},
			"targetAddress": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					message, ok := params.Source.(BusMessage)
					if !ok {
						return 0, nil
					}
					return message.TargetAddress, nil
				},
			},
			"requestLen": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					message, ok := params.Source.(BusMessage)
					if !ok {
						return 0, nil
					}
					return message.RequestLen, nil
				},
			},
			"responseLen": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					message, ok := params.Source.(BusMessage)
					if !ok {
						return 0, nil
					}
					return message.ResponseLen, nil
				},
			},
		},
	})

	busMessagesListType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusMessagesList",
		Fields: graphqlgo.Fields{
			"status": &graphqlgo.Field{
				Type: statusType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					list, ok := params.Source.(*BusMessagesList)
					if ok && list != nil {
						return list.Status, nil
					}
					value, ok := params.Source.(BusMessagesList)
					if !ok {
						return nil, nil
					}
					return value.Status, nil
				},
			},
			"count": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					list, ok := params.Source.(*BusMessagesList)
					if ok && list != nil {
						return list.Count, nil
					}
					value, ok := params.Source.(BusMessagesList)
					if !ok {
						return 0, nil
					}
					return value.Count, nil
				},
			},
			"capacity": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					list, ok := params.Source.(*BusMessagesList)
					if ok && list != nil {
						return list.Capacity, nil
					}
					value, ok := params.Source.(BusMessagesList)
					if !ok {
						return 0, nil
					}
					return value.Capacity, nil
				},
			},
			"items": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(messageType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					list, ok := params.Source.(*BusMessagesList)
					if ok && list != nil {
						if len(list.Items) == 0 {
							return []BusMessage{}, nil
						}
						return list.Items, nil
					}
					value, ok := params.Source.(BusMessagesList)
					if !ok || len(value.Items) == 0 {
						return []BusMessage{}, nil
					}
					return value.Items, nil
				},
			},
		},
	})

	periodicityEntryType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusPeriodicityEntry",
		Fields: graphqlgo.Fields{
			"sourceBucket": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok {
						return "", nil
					}
					return entry.SourceBucket, nil
				},
			},
			"targetBucket": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok {
						return "", nil
					}
					return entry.TargetBucket, nil
				},
			},
			"primary": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok {
						return 0, nil
					}
					return entry.Primary, nil
				},
			},
			"secondary": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok {
						return 0, nil
					}
					return entry.Secondary, nil
				},
			},
			"family": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok {
						return "", nil
					}
					return entry.Family, nil
				},
			},
			"state": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.String),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok {
						return "", nil
					}
					return entry.State, nil
				},
			},
			"lastSeen": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok || entry.LastSeen == "" {
						return nil, nil
					}
					return entry.LastSeen, nil
				},
			},
			"sampleCount": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok {
						return 0, nil
					}
					return entry.SampleCount, nil
				},
			},
			"lastInterval": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok || entry.LastInterval == "" {
						return nil, nil
					}
					return entry.LastInterval, nil
				},
			},
			"meanInterval": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok || entry.MeanInterval == "" {
						return nil, nil
					}
					return entry.MeanInterval, nil
				},
			},
			"minInterval": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok || entry.MinInterval == "" {
						return nil, nil
					}
					return entry.MinInterval, nil
				},
			},
			"maxInterval": &graphqlgo.Field{
				Type: graphqlgo.String,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					entry, ok := params.Source.(BusPeriodicityEntry)
					if !ok || entry.MaxInterval == "" {
						return nil, nil
					}
					return entry.MaxInterval, nil
				},
			},
		},
	})

	busPeriodicityListType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "BusPeriodicityList",
		Fields: graphqlgo.Fields{
			"status": &graphqlgo.Field{
				Type: statusType,
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					list, ok := params.Source.(*BusPeriodicityList)
					if ok && list != nil {
						return list.Status, nil
					}
					value, ok := params.Source.(BusPeriodicityList)
					if !ok {
						return nil, nil
					}
					return value.Status, nil
				},
			},
			"count": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					list, ok := params.Source.(*BusPeriodicityList)
					if ok && list != nil {
						return list.Count, nil
					}
					value, ok := params.Source.(BusPeriodicityList)
					if !ok {
						return 0, nil
					}
					return value.Count, nil
				},
			},
			"capacity": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.Int),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					list, ok := params.Source.(*BusPeriodicityList)
					if ok && list != nil {
						return list.Capacity, nil
					}
					value, ok := params.Source.(BusPeriodicityList)
					if !ok {
						return 0, nil
					}
					return value.Capacity, nil
				},
			},
			"items": &graphqlgo.Field{
				Type: graphqlgo.NewNonNull(graphqlgo.NewList(graphqlgo.NewNonNull(periodicityEntryType))),
				Resolve: func(params graphqlgo.ResolveParams) (any, error) {
					list, ok := params.Source.(*BusPeriodicityList)
					if ok && list != nil {
						if len(list.Items) == 0 {
							return []BusPeriodicityEntry{}, nil
						}
						return list.Items, nil
					}
					value, ok := params.Source.(BusPeriodicityList)
					if !ok || len(value.Items) == 0 {
						return []BusPeriodicityEntry{}, nil
					}
					return value.Items, nil
				},
			},
		},
	})

	return busSummaryType, busMessagesListType, busPeriodicityListType
}

func parseBusObservabilityLimit(args map[string]any) (int, error) {
	if args == nil {
		return 0, nil
	}
	raw, ok := args["limit"]
	if !ok || raw == nil {
		return 0, nil
	}
	switch value := raw.(type) {
	case int:
		if value <= 0 {
			return 0, fmt.Errorf("graphql query invalid limit: %w", ebuserrors.ErrInvalidPayload)
		}
		return value, nil
	case int64:
		if value <= 0 {
			return 0, fmt.Errorf("graphql query invalid limit: %w", ebuserrors.ErrInvalidPayload)
		}
		return int(value), nil
	case float64:
		limit := int(value)
		if value != float64(limit) || limit <= 0 {
			return 0, fmt.Errorf("graphql query invalid limit: %w", ebuserrors.ErrInvalidPayload)
		}
		return limit, nil
	default:
		return 0, fmt.Errorf("graphql query invalid limit: %w", ebuserrors.ErrInvalidPayload)
	}
}

func snapshotBusObservability(builder *Builder, rootValue any) *BusObservabilitySnapshot {
	if cache := busObservabilitySnapshotCacheFromRoot(rootValue); cache != nil {
		return cache.Snapshot()
	}
	if builder == nil {
		return nil
	}
	snapshot := cloneBusObservabilitySnapshot(builder.busObservabilityProvider().Snapshot())
	return &snapshot
}

func resolveBusSummary(builder *Builder, rootValue any) *BusSummary {
	snapshot := snapshotBusObservability(builder, rootValue)
	if snapshot == nil {
		return nil
	}
	if snapshot.Summary == nil {
		if len(snapshot.Messages) == 0 && len(snapshot.Periodicity) == 0 {
			return &BusSummary{}
		}
		return nil
	}
	return cloneBusSummary(snapshot.Summary)
}

func resolveBusMessages(builder *Builder, rootValue any, limit int) *BusMessagesList {
	snapshot := snapshotBusObservability(builder, rootValue)
	if snapshot == nil {
		return nil
	}

	result := &BusMessagesList{
		Items: trimBusMessages(snapshot.Messages, limit),
	}
	if snapshot.Summary != nil {
		result.Status = cloneBusObservabilityStatus(snapshot.Summary.Status)
		result.Count = snapshot.Summary.Messages.Count
		result.Capacity = snapshot.Summary.Messages.Capacity
		return result
	}
	result.Count = len(snapshot.Messages)
	result.Capacity = len(snapshot.Messages)
	return result
}

func resolveBusPeriodicity(builder *Builder, rootValue any, limit int) *BusPeriodicityList {
	snapshot := snapshotBusObservability(builder, rootValue)
	if snapshot == nil {
		return nil
	}

	result := &BusPeriodicityList{
		Items: trimBusPeriodicity(snapshot.Periodicity, limit),
	}
	if snapshot.Summary != nil {
		result.Status = cloneBusObservabilityStatus(snapshot.Summary.Status)
		result.Count = snapshot.Summary.Periodicity.Count
		result.Capacity = snapshot.Summary.Periodicity.Capacity
		return result
	}
	result.Count = len(snapshot.Periodicity)
	result.Capacity = len(snapshot.Periodicity)
	return result
}
