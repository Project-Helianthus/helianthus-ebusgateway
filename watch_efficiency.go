package ebusgateway

import "time"

type SemanticReadExecutionStats struct {
	ServedFromShadow        bool
	ServedFromPassiveShadow bool
	ActiveFetchAttempted    bool
	ActiveFetchSucceeded    bool
	ActiveFetchDuration     time.Duration
}

type WatchEfficiencyReadEvent struct {
	Key           WatchKey
	Descriptor    WatchDescriptor
	HasDescriptor bool
	MaxAge        time.Duration
	Stats         SemanticReadExecutionStats
	ObservedAt    time.Time
}

type WatchEfficiencyDirectApplyEvent struct {
	Key           WatchKey
	Descriptor    WatchDescriptor
	HasDescriptor bool
	ObservedAt    time.Time
}

type WatchEfficiencyObserver interface {
	ObserveWatchRead(event WatchEfficiencyReadEvent)
	ObserveWatchDirectApply(event WatchEfficiencyDirectApplyEvent)
}
