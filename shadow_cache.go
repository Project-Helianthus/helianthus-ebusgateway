package ebusgateway

import (
	"bytes"
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultShadowCacheCapacity                 = 4096
	DefaultShadowCachePinnedCapacity           = 2048
	DefaultShadowCacheWriteConfirmPinnedCap    = 256
	DefaultShadowCacheTombstoneRetainWindow    = 15 * time.Minute
	DefaultShadowCacheTombstoneHardLifespan    = 24 * time.Hour
	DefaultShadowCacheCompactorCadence         = time.Minute
	DefaultShadowCacheCompactorBatchSize       = 64
	DefaultShadowCacheShutdownCompactorTimeout = time.Second
)

type ShadowWriteSource string

const (
	ShadowWriteSourcePassive         ShadowWriteSource = "passive"
	ShadowWriteSourceActiveConfirmed ShadowWriteSource = "active_confirmed"
)

type ShadowConfidenceTier string

const (
	ShadowConfidenceHigh    ShadowConfidenceTier = "high_confidence"
	ShadowConfidenceLimited ShadowConfidenceTier = "limited_confidence"
	ShadowConfidenceNone    ShadowConfidenceTier = "no_confidence"
)

type ShadowEntryState string

const (
	ShadowEntryStatePresent     ShadowEntryState = "present"
	ShadowEntryStateInvalidated ShadowEntryState = "invalidated"
	ShadowEntryStateTombstone   ShadowEntryState = "tombstone"
)

type ShadowInvalidationReason string

const (
	ShadowInvalidationReasonExternalWrite ShadowInvalidationReason = "external_write"
	ShadowInvalidationReasonRollback      ShadowInvalidationReason = "rollback"
	ShadowInvalidationReasonManual        ShadowInvalidationReason = "manual"
	ShadowInvalidationReasonPolicyReject  ShadowInvalidationReason = "policy_reject"
)

type ShadowInvalidationSource string

const (
	ShadowInvalidationSourcePassive  ShadowInvalidationSource = "passive"
	ShadowInvalidationSourceActive   ShadowInvalidationSource = "active"
	ShadowInvalidationSourceOperator ShadowInvalidationSource = "operator"
	ShadowInvalidationSourceSystem   ShadowInvalidationSource = "system"
)

type ShadowWriteRejectionReason string

const (
	ShadowWriteRejectionReasonStaleTimestamp        ShadowWriteRejectionReason = "stale_timestamp"
	ShadowWriteRejectionReasonSameTimestampConflict ShadowWriteRejectionReason = "same_timestamp_conflict"
	ShadowWriteRejectionReasonGenerationAdvanced    ShadowWriteRejectionReason = "generation_advanced"
	ShadowWriteRejectionReasonPolicyReject          ShadowWriteRejectionReason = "policy_reject"
	ShadowWriteRejectionReasonCapacity              ShadowWriteRejectionReason = "capacity"
)

type ShadowCacheOptions struct {
	Catalog                  *WatchCatalog
	Activations              *WatchActivationSet
	Capacity                 int
	PinnedCapacity           int
	WriteConfirmPinnedCap    int
	TombstoneRetainWindow    time.Duration
	TombstoneHardLifespan    time.Duration
	CompactorCadence         time.Duration
	CompactorBatchSize       int
	ShutdownCompactorTimeout time.Duration
	Now                      func() time.Time
}

type ShadowEligibilitySnapshot struct {
	Present    bool
	Eligible   bool
	Generation uint64
	State      ShadowEntryState
	ObservedAt time.Time
	ExpiresAt  time.Time
}

type ShadowCacheSummary struct {
	Enabled                  bool
	PinnedBudgetDegraded     bool
	CompactorDegraded        bool
	TotalEntries             int
	PinnedEntries            int
	EvictableEntries         int
	StaticPinnedFootprint    int
	WriteConfirmPinnedActive int
}

type ShadowEntryView struct {
	CanonicalKey           string
	Descriptor             WatchDescriptor
	Source                 ShadowWriteSource
	Confidence             ShadowConfidenceTier
	State                  ShadowEntryState
	Value                  []byte
	ObservedAt             time.Time
	ExpiresAt              time.Time
	Generation             uint64
	LastWriteGeneration    uint64
	InvalidationGeneration uint64
	InvalidationReason     ShadowInvalidationReason
	InvalidationSource     ShadowInvalidationSource
	InvalidatedAt          time.Time
	Pinned                 bool
}

type ShadowLookupResult struct {
	Entry         ShadowEntryView
	Found         bool
	Eligible      bool
	Descriptor    WatchDescriptor
	HasDescriptor bool
}

type ShadowWrite struct {
	Key             WatchKey
	Source          ShadowWriteSource
	Confidence      ShadowConfidenceTier
	Value           []byte
	ObservedAt      time.Time
	StartGeneration uint64
}

type ShadowWriteResult struct {
	Accepted            bool
	Generation          uint64
	LastWriteGeneration uint64
	Reason              ShadowWriteRejectionReason
}

type ShadowInvalidation struct {
	Key           WatchKey
	Reason        ShadowInvalidationReason
	Source        ShadowInvalidationSource
	InvalidatedAt time.Time
}

type ShadowInvalidationResult struct {
	Generation uint64
	State      ShadowEntryState
}

type ShadowCache struct {
	catalog                  *WatchCatalog
	activations              *WatchActivationSet
	capacity                 int
	pinnedCapacity           int
	writeConfirmPinnedCap    int
	tombstoneRetainWindow    time.Duration
	tombstoneHardLifespan    time.Duration
	compactorCadence         time.Duration
	compactorBatchSize       int
	shutdownCompactorTimeout time.Duration
	now                      func() time.Time

	mu              sync.Mutex
	entries         map[string]*shadowEntry
	keyState        sync.Map
	evictableLRU    *list.List
	compactorList   *list.List
	compactorCursor *list.Element
	compactorStop   chan struct{}
	compactorDone   chan struct{}
	compactorOnce   sync.Once

	pinnedBudgetDegraded atomic.Bool
	compactorDegraded    atomic.Bool
	compactorStarted     atomic.Bool
}

type shadowKeyState struct {
	generation          uint64
	lastWriteGeneration uint64
	snapshot            atomic.Pointer[shadowEligibilitySnapshotRecord]
}

type shadowEligibilitySnapshotRecord struct {
	present    bool
	state      ShadowEntryState
	generation uint64
	observedAt time.Time
	expiresAt  time.Time
}

type shadowEntry struct {
	canonicalKey string
	descriptor   WatchDescriptor
	source       ShadowWriteSource
	confidence   ShadowConfidenceTier
	state        ShadowEntryState
	value        []byte
	observedAt   time.Time
	expiresAt    time.Time

	generation             uint64
	lastWriteGeneration    uint64
	invalidationGeneration uint64
	invalidationReason     ShadowInvalidationReason
	invalidationSource     ShadowInvalidationSource
	invalidatedAt          time.Time

	pinClass      shadowPinClass
	evictableElem *list.Element
	compactorElem *list.Element
}

type shadowPinClass int

const (
	shadowPinClassNone shadowPinClass = iota
	shadowPinClassWriteConfirm
	shadowPinClassStatic
)

func NewShadowCache(options ShadowCacheOptions) *ShadowCache {
	options = normalizeShadowCacheOptions(options)
	cache := &ShadowCache{
		catalog:                  options.Catalog,
		activations:              options.Activations,
		capacity:                 options.Capacity,
		pinnedCapacity:           options.PinnedCapacity,
		writeConfirmPinnedCap:    options.WriteConfirmPinnedCap,
		tombstoneRetainWindow:    options.TombstoneRetainWindow,
		tombstoneHardLifespan:    options.TombstoneHardLifespan,
		compactorCadence:         options.CompactorCadence,
		compactorBatchSize:       options.CompactorBatchSize,
		shutdownCompactorTimeout: options.ShutdownCompactorTimeout,
		now:                      options.Now,
		entries:                  make(map[string]*shadowEntry),
		evictableLRU:             list.New(),
		compactorList:            list.New(),
		compactorStop:            make(chan struct{}),
		compactorDone:            make(chan struct{}),
	}
	cache.RevalidatePinnedBudget()
	return cache
}

func normalizeShadowCacheOptions(options ShadowCacheOptions) ShadowCacheOptions {
	if options.Catalog == nil {
		options.Catalog = &WatchCatalog{}
	}
	if options.Activations == nil {
		options.Activations = NewWatchActivationSet(options.Catalog)
	}
	if options.Capacity <= 0 {
		options.Capacity = DefaultShadowCacheCapacity
	}
	if options.PinnedCapacity <= 0 || options.PinnedCapacity > options.Capacity {
		options.PinnedCapacity = DefaultShadowCachePinnedCapacity
		if options.PinnedCapacity > options.Capacity {
			options.PinnedCapacity = options.Capacity
		}
	}
	if options.WriteConfirmPinnedCap < 0 || options.WriteConfirmPinnedCap > options.PinnedCapacity {
		options.WriteConfirmPinnedCap = DefaultShadowCacheWriteConfirmPinnedCap
		if options.WriteConfirmPinnedCap > options.PinnedCapacity {
			options.WriteConfirmPinnedCap = options.PinnedCapacity
		}
	}
	if options.TombstoneRetainWindow <= 0 {
		options.TombstoneRetainWindow = DefaultShadowCacheTombstoneRetainWindow
	}
	if options.TombstoneHardLifespan <= 0 {
		options.TombstoneHardLifespan = DefaultShadowCacheTombstoneHardLifespan
	}
	if options.TombstoneHardLifespan < options.TombstoneRetainWindow {
		options.TombstoneHardLifespan = options.TombstoneRetainWindow
	}
	if options.CompactorCadence <= 0 {
		options.CompactorCadence = DefaultShadowCacheCompactorCadence
	}
	if options.CompactorBatchSize <= 0 {
		options.CompactorBatchSize = DefaultShadowCacheCompactorBatchSize
	}
	if options.ShutdownCompactorTimeout <= 0 {
		options.ShutdownCompactorTimeout = DefaultShadowCacheShutdownCompactorTimeout
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return options
}

func (cache *ShadowCache) StartCompactor() {
	if cache == nil || cache.compactorCadence <= 0 {
		return
	}
	cache.compactorOnce.Do(func() {
		cache.compactorStarted.Store(true)
		go cache.runCompactor()
	})
}

func (cache *ShadowCache) runCompactor() {
	ticker := time.NewTicker(cache.compactorCadence)
	defer ticker.Stop()
	defer close(cache.compactorDone)

	for {
		select {
		case <-cache.compactorStop:
			return
		case <-ticker.C:
			func() {
				defer func() {
					if recover() != nil {
						cache.compactorDegraded.Store(true)
					}
				}()
				cache.CompactOnce()
			}()
		}
	}
}

func (cache *ShadowCache) Close(ctx context.Context) error {
	if cache == nil {
		return nil
	}
	if !cache.compactorStarted.Load() {
		return nil
	}
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), cache.shutdownCompactorTimeout)
		defer cancel()
	}
	select {
	case <-cache.compactorDone:
		return nil
	default:
	}
	close(cache.compactorStop)
	select {
	case <-cache.compactorDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (cache *ShadowCache) CaptureGeneration(key WatchKey) uint64 {
	if cache == nil || key == nil {
		return 0
	}
	record := cache.loadSnapshotRecord(key.Canonical())
	return record.generation
}

func (cache *ShadowCache) SnapshotEligibility(key WatchKey) ShadowEligibilitySnapshot {
	if cache == nil || key == nil {
		return ShadowEligibilitySnapshot{}
	}
	record := cache.loadSnapshotRecord(key.Canonical())
	if !record.present {
		return ShadowEligibilitySnapshot{
			Generation: record.generation,
		}
	}
	eligible := cache.shadowingEnabled() && record.state == ShadowEntryStatePresent
	if eligible {
		now := cache.now()
		if record.expiresAt.IsZero() || now.After(record.expiresAt) {
			eligible = false
		}
	}
	return ShadowEligibilitySnapshot{
		Present:    record.present,
		Eligible:   eligible,
		Generation: record.generation,
		State:      record.state,
		ObservedAt: record.observedAt,
		ExpiresAt:  record.expiresAt,
	}
}

func (cache *ShadowCache) Lookup(key WatchKey, maxAge time.Duration) ShadowLookupResult {
	if cache == nil || key == nil {
		return ShadowLookupResult{}
	}
	canonical := key.Canonical()
	cache.mu.Lock()
	defer cache.mu.Unlock()

	entry := cache.entries[canonical]
	if entry == nil {
		return ShadowLookupResult{}
	}
	cache.syncEntryPinLocked(entry)
	view := cloneShadowEntryView(entry)
	descriptor, hasDescriptor := cache.catalog.DescriptorByCanonical(canonical)
	eligible := cache.lookupEligibleLocked(entry, maxAge)
	if eligible && entry.pinClass == shadowPinClassNone {
		cache.touchEvictableLocked(entry)
	}
	return ShadowLookupResult{
		Entry:         view,
		Found:         true,
		Eligible:      eligible,
		Descriptor:    descriptor,
		HasDescriptor: hasDescriptor,
	}
}

func (cache *ShadowCache) Entry(key WatchKey) (ShadowEntryView, bool) {
	if cache == nil || key == nil {
		return ShadowEntryView{}, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry := cache.entries[key.Canonical()]
	if entry == nil {
		return ShadowEntryView{}, false
	}
	cache.syncEntryPinLocked(entry)
	return cloneShadowEntryView(entry), true
}

func (cache *ShadowCache) Write(write ShadowWrite) ShadowWriteResult {
	if cache == nil {
		return ShadowWriteResult{Reason: ShadowWriteRejectionReasonPolicyReject}
	}
	if write.Key == nil || write.ObservedAt.IsZero() || write.Source == "" || write.Confidence == "" {
		return ShadowWriteResult{Reason: ShadowWriteRejectionReasonPolicyReject}
	}

	canonical := write.Key.Canonical()
	descriptor, ok := cache.catalog.DescriptorByCanonical(canonical)
	if !ok || descriptor.FreshnessProfile == WatchFreshnessProfileDebug || write.Confidence == ShadowConfidenceNone {
		return ShadowWriteResult{Reason: ShadowWriteRejectionReasonPolicyReject}
	}

	if !cache.shadowingEnabled() {
		return ShadowWriteResult{Reason: ShadowWriteRejectionReasonPolicyReject}
	}

	sources := cache.activations.ActiveSources(write.Key)
	if len(sources) == 0 {
		return ShadowWriteResult{Reason: ShadowWriteRejectionReasonPolicyReject}
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	state := cache.ensureKeyStateLocked(canonical)
	entry := cache.entries[canonical]
	desiredPin := cache.desiredPinClass(write.Key, descriptor, sources)
	if desiredPin == shadowPinClassStatic && cache.pinnedBudgetDegraded.Load() {
		return ShadowWriteResult{Reason: ShadowWriteRejectionReasonPolicyReject}
	}
	if desiredPin == shadowPinClassWriteConfirm && !cache.canAdmitWriteConfirmPinLocked(canonical, entry) {
		return ShadowWriteResult{Reason: ShadowWriteRejectionReasonCapacity}
	}
	if entry != nil {
		cache.syncEntryPinLocked(entry)
	}

	if write.Source == ShadowWriteSourceActiveConfirmed && state.generation != write.StartGeneration {
		return ShadowWriteResult{
			Generation: state.generation,
			Reason:     ShadowWriteRejectionReasonGenerationAdvanced,
		}
	}

	rejectReason := cache.rejectWriteByPrecedence(entry, write)
	if rejectReason != "" {
		return ShadowWriteResult{
			Generation: state.generation,
			Reason:     rejectReason,
		}
	}

	ttl, err := descriptor.EffectiveFreshnessTTL()
	if err != nil || ttl <= 0 {
		return ShadowWriteResult{Reason: ShadowWriteRejectionReasonPolicyReject}
	}

	if entry == nil {
		if !cache.ensureCapacityLocked(desiredPin) {
			return ShadowWriteResult{Reason: ShadowWriteRejectionReasonCapacity}
		}
		entry = &shadowEntry{
			canonicalKey:  canonical,
			descriptor:    descriptor,
			compactorElem: cache.compactorList.PushBack(canonical),
		}
		cache.entries[canonical] = entry
		cache.advanceGenerationLocked(entry, state)
	}

	entry.descriptor = descriptor
	entry.source = write.Source
	entry.confidence = write.Confidence
	entry.state = ShadowEntryStatePresent
	entry.value = append(entry.value[:0], write.Value...)
	entry.observedAt = write.ObservedAt
	entry.expiresAt = write.ObservedAt.Add(ttl)
	entry.invalidationReason = ""
	entry.invalidationSource = ""
	entry.invalidatedAt = time.Time{}
	entry.pinClass = desiredPin
	if entry.generation == 0 {
		cache.advanceGenerationLocked(entry, state)
	}
	cache.bumpLastWriteGenerationLocked(entry, state)
	cache.applyPinClassLocked(entry, desiredPin)
	cache.storeSnapshotLocked(entry, state)

	return ShadowWriteResult{
		Accepted:            true,
		Generation:          entry.generation,
		LastWriteGeneration: entry.lastWriteGeneration,
	}
}

func (cache *ShadowCache) Invalidate(invalidation ShadowInvalidation) ShadowInvalidationResult {
	if cache == nil || invalidation.Key == nil {
		return ShadowInvalidationResult{}
	}
	if invalidation.Reason == "" {
		invalidation.Reason = ShadowInvalidationReasonManual
	}
	if invalidation.Source == "" {
		invalidation.Source = ShadowInvalidationSourceSystem
	}
	if invalidation.InvalidatedAt.IsZero() {
		invalidation.InvalidatedAt = cache.now()
	}

	canonical := invalidation.Key.Canonical()
	descriptor, _ := cache.catalog.DescriptorByCanonical(canonical)
	sources := cache.activations.ActiveSources(invalidation.Key)

	cache.mu.Lock()
	defer cache.mu.Unlock()

	state := cache.ensureKeyStateLocked(canonical)
	entry := cache.entries[canonical]
	desiredPin := cache.desiredPinClass(invalidation.Key, descriptor, sources)

	if entry == nil {
		if !cache.ensureCapacityLocked(desiredPin) {
			return ShadowInvalidationResult{
				Generation: state.generation,
				State:      ShadowEntryStateTombstone,
			}
		}
		entry = &shadowEntry{
			canonicalKey:  canonical,
			descriptor:    descriptor,
			compactorElem: cache.compactorList.PushBack(canonical),
		}
		cache.entries[canonical] = entry
	}

	cache.advanceGenerationLocked(entry, state)
	entry.descriptor = descriptor
	entry.state = ShadowEntryStateTombstone
	if len(entry.value) > 0 {
		entry.state = ShadowEntryStateInvalidated
	}
	entry.invalidationGeneration = entry.generation
	entry.invalidationReason = invalidation.Reason
	entry.invalidationSource = invalidation.Source
	entry.invalidatedAt = invalidation.InvalidatedAt
	entry.pinClass = desiredPin
	cache.bumpLastWriteGenerationLocked(entry, state)
	cache.applyPinClassLocked(entry, desiredPin)
	cache.storeSnapshotLocked(entry, state)

	return ShadowInvalidationResult{
		Generation: entry.generation,
		State:      entry.state,
	}
}

func (cache *ShadowCache) RevalidatePinnedBudget() ShadowCacheSummary {
	if cache == nil {
		return ShadowCacheSummary{}
	}
	footprint := cache.staticPinnedFootprint()
	cache.pinnedBudgetDegraded.Store(footprint > cache.staticPinnedBudget())
	cache.mu.Lock()
	cache.syncAllPinsLocked()
	cache.mu.Unlock()
	return cache.Summary()
}

func (cache *ShadowCache) RefreshActivations() {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.syncAllPinsLocked()
}

func (cache *ShadowCache) CompactOnce() time.Duration {
	if cache == nil {
		return 0
	}
	start := cache.now()
	remaining := cache.compactorLength()
	for remaining > 0 {
		batch := cache.nextCompactorBatch()
		if len(batch) == 0 {
			break
		}
		cache.compactBatch(batch, cache.now())
		remaining -= len(batch)
	}
	return cache.now().Sub(start)
}

func (cache *ShadowCache) Summary() ShadowCacheSummary {
	if cache == nil {
		return ShadowCacheSummary{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	pinned := 0
	evictable := 0
	writeConfirm := 0
	for _, entry := range cache.entries {
		cache.syncEntryPinLocked(entry)
		if entry.pinClass == shadowPinClassNone {
			evictable++
			continue
		}
		pinned++
		if entry.pinClass == shadowPinClassWriteConfirm {
			writeConfirm++
		}
	}
	return ShadowCacheSummary{
		Enabled:                  cache.shadowingEnabled(),
		PinnedBudgetDegraded:     cache.pinnedBudgetDegraded.Load(),
		CompactorDegraded:        cache.compactorDegraded.Load(),
		TotalEntries:             len(cache.entries),
		PinnedEntries:            pinned,
		EvictableEntries:         evictable,
		StaticPinnedFootprint:    cache.staticPinnedFootprint(),
		WriteConfirmPinnedActive: writeConfirm,
	}
}

func (cache *ShadowCache) shadowingEnabled() bool {
	return cache != nil && !cache.pinnedBudgetDegraded.Load()
}

func (cache *ShadowCache) staticPinnedBudget() int {
	budget := cache.pinnedCapacity - cache.writeConfirmPinnedCap
	if budget < 0 {
		return 0
	}
	return budget
}

func (cache *ShadowCache) staticPinnedFootprint() int {
	if cache == nil || cache.catalog == nil {
		return 0
	}
	total := 0
	for _, descriptor := range cache.catalog.Descriptors() {
		if descriptor.Key == nil || descriptor.FreshnessProfile == WatchFreshnessProfileDebug {
			continue
		}
		for _, source := range cache.activations.ActiveSources(descriptor.Key) {
			if source == WatchActivationSourcePoller {
				total++
				break
			}
		}
	}
	return total
}

func (cache *ShadowCache) desiredPinClass(key WatchKey, descriptor WatchDescriptor, sources []WatchActivationSource) shadowPinClass {
	if key == nil || descriptor.FreshnessProfile == WatchFreshnessProfileDebug {
		return shadowPinClassNone
	}
	for _, source := range sources {
		if source == WatchActivationSourcePoller {
			return shadowPinClassStatic
		}
	}
	for _, source := range sources {
		if source == WatchActivationSourceWriteConfirm {
			return shadowPinClassWriteConfirm
		}
	}
	return shadowPinClassNone
}

func (cache *ShadowCache) rejectWriteByPrecedence(entry *shadowEntry, write ShadowWrite) ShadowWriteRejectionReason {
	if entry == nil {
		return ""
	}
	if write.ObservedAt.Before(entry.observedAt) {
		return ShadowWriteRejectionReasonStaleTimestamp
	}
	if write.ObservedAt.After(entry.observedAt) {
		return ""
	}
	if len(entry.value) == 0 || bytes.Equal(entry.value, write.Value) {
		if write.Source == entry.source || shadowSourcePrecedence(write.Source) >= shadowSourcePrecedence(entry.source) {
			return ""
		}
		return ShadowWriteRejectionReasonSameTimestampConflict
	}
	if write.Source == entry.source {
		return ShadowWriteRejectionReasonSameTimestampConflict
	}
	if shadowSourcePrecedence(write.Source) > shadowSourcePrecedence(entry.source) {
		return ""
	}
	return ShadowWriteRejectionReasonSameTimestampConflict
}

func shadowSourcePrecedence(source ShadowWriteSource) int {
	switch source {
	case ShadowWriteSourceActiveConfirmed:
		return 1
	default:
		return 0
	}
}

func (cache *ShadowCache) lookupEligibleLocked(entry *shadowEntry, maxAge time.Duration) bool {
	if entry == nil || !cache.shadowingEnabled() || entry.state != ShadowEntryStatePresent {
		return false
	}
	limit := maxAge
	if ttl, err := entry.descriptor.EffectiveFreshnessTTL(); err == nil {
		if limit <= 0 || (ttl > 0 && ttl < limit) {
			limit = ttl
		}
	}
	if limit <= 0 {
		return false
	}
	age := cache.now().Sub(entry.observedAt)
	return age >= 0 && age <= limit
}

func (cache *ShadowCache) syncAllPinsLocked() {
	for _, entry := range cache.entries {
		cache.syncEntryPinLocked(entry)
	}
}

func (cache *ShadowCache) syncEntryPinLocked(entry *shadowEntry) {
	if entry == nil || entry.descriptor.Key == nil {
		return
	}
	sources := cache.activations.ActiveSources(entry.descriptor.Key)
	desired := cache.desiredPinClass(entry.descriptor.Key, entry.descriptor, sources)
	if desired == shadowPinClassStatic && cache.pinnedBudgetDegraded.Load() {
		desired = shadowPinClassNone
	}
	if desired == shadowPinClassWriteConfirm && !cache.canAdmitWriteConfirmPinLocked(entry.canonicalKey, entry) {
		desired = shadowPinClassNone
	}
	cache.applyPinClassLocked(entry, desired)
}

func (cache *ShadowCache) applyPinClassLocked(entry *shadowEntry, desired shadowPinClass) {
	if entry == nil {
		return
	}
	if entry.pinClass == desired {
		if desired == shadowPinClassNone && entry.evictableElem == nil {
			entry.evictableElem = cache.evictableLRU.PushBack(entry.canonicalKey)
		}
		return
	}
	if entry.evictableElem != nil {
		cache.evictableLRU.Remove(entry.evictableElem)
		entry.evictableElem = nil
	}
	entry.pinClass = desired
	if desired == shadowPinClassNone {
		entry.evictableElem = cache.evictableLRU.PushBack(entry.canonicalKey)
	}
}

func (cache *ShadowCache) touchEvictableLocked(entry *shadowEntry) {
	if entry == nil || entry.pinClass != shadowPinClassNone {
		return
	}
	if entry.evictableElem == nil {
		entry.evictableElem = cache.evictableLRU.PushBack(entry.canonicalKey)
		return
	}
	cache.evictableLRU.MoveToBack(entry.evictableElem)
}

func (cache *ShadowCache) ensureCapacityLocked(desiredPin shadowPinClass) bool {
	if len(cache.entries) < cache.capacity {
		return true
	}
	for len(cache.entries) >= cache.capacity {
		front := cache.evictableLRU.Front()
		if front == nil {
			return false
		}
		canonical, _ := front.Value.(string)
		cache.removeEntryLocked(canonical, false)
	}
	if desiredPin == shadowPinClassWriteConfirm && cache.writeConfirmPinnedCountLocked() >= cache.writeConfirmPinnedCap {
		return false
	}
	return true
}

func (cache *ShadowCache) canAdmitWriteConfirmPinLocked(canonical string, entry *shadowEntry) bool {
	if entry != nil && (entry.pinClass == shadowPinClassStatic || entry.pinClass == shadowPinClassWriteConfirm) {
		return true
	}
	return cache.writeConfirmPinnedCountLocked() < cache.writeConfirmPinnedCap
}

func (cache *ShadowCache) writeConfirmPinnedCountLocked() int {
	total := 0
	for key, entry := range cache.entries {
		if key == "" || entry == nil {
			continue
		}
		if entry.pinClass == shadowPinClassWriteConfirm {
			total++
		}
	}
	return total
}

func (cache *ShadowCache) compactBatch(batch []string, now time.Time) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	for _, canonical := range batch {
		entry := cache.entries[canonical]
		if entry == nil {
			continue
		}
		cache.syncEntryPinLocked(entry)
		state := cache.ensureKeyStateLocked(canonical)
		switch entry.state {
		case ShadowEntryStateInvalidated:
			if entry.invalidatedAt.IsZero() || now.Sub(entry.invalidatedAt) < cache.tombstoneRetainWindow {
				continue
			}
			cache.advanceGenerationLocked(entry, state)
			entry.state = ShadowEntryStateTombstone
			entry.value = nil
			cache.bumpLastWriteGenerationLocked(entry, state)
			cache.storeSnapshotLocked(entry, state)
		case ShadowEntryStateTombstone:
			if entry.invalidatedAt.IsZero() || now.Sub(entry.invalidatedAt) < cache.tombstoneHardLifespan {
				continue
			}
			cache.advanceGenerationLocked(entry, state)
			cache.storeAbsentSnapshotLocked(state)
			cache.removeEntryLocked(canonical, true)
		}
	}
}

func (cache *ShadowCache) compactorLength() int {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return cache.compactorList.Len()
}

func (cache *ShadowCache) nextCompactorBatch() []string {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.compactorList.Len() == 0 {
		cache.compactorCursor = nil
		return nil
	}
	if cache.compactorCursor == nil {
		cache.compactorCursor = cache.compactorList.Front()
	}

	batch := make([]string, 0, cache.compactorBatchSize)
	for len(batch) < cache.compactorBatchSize && cache.compactorCursor != nil {
		canonical, _ := cache.compactorCursor.Value.(string)
		batch = append(batch, canonical)
		cache.compactorCursor = cache.compactorCursor.Next()
		if cache.compactorCursor == nil {
			break
		}
	}
	return batch
}

func (cache *ShadowCache) advanceGenerationLocked(entry *shadowEntry, state *shadowKeyState) {
	state.generation++
	entry.generation = state.generation
}

func (cache *ShadowCache) bumpLastWriteGenerationLocked(entry *shadowEntry, state *shadowKeyState) {
	state.lastWriteGeneration++
	entry.lastWriteGeneration = state.lastWriteGeneration
}

func (cache *ShadowCache) ensureKeyStateLocked(canonical string) *shadowKeyState {
	if loaded, ok := cache.keyState.Load(canonical); ok {
		if state, _ := loaded.(*shadowKeyState); state != nil {
			return state
		}
	}
	state := &shadowKeyState{}
	state.snapshot.Store(&shadowEligibilitySnapshotRecord{})
	actual, _ := cache.keyState.LoadOrStore(canonical, state)
	state, _ = actual.(*shadowKeyState)
	return state
}

func (cache *ShadowCache) storeSnapshotLocked(entry *shadowEntry, state *shadowKeyState) {
	if state == nil {
		state = cache.ensureKeyStateLocked(entry.canonicalKey)
	}
	state.snapshot.Store(&shadowEligibilitySnapshotRecord{
		present:    true,
		state:      entry.state,
		generation: entry.generation,
		observedAt: entry.observedAt,
		expiresAt:  entry.expiresAt,
	})
}

func (cache *ShadowCache) storeAbsentSnapshotLocked(state *shadowKeyState) {
	if state == nil {
		return
	}
	state.snapshot.Store(&shadowEligibilitySnapshotRecord{
		generation: state.generation,
	})
}

func (cache *ShadowCache) loadSnapshotRecord(canonical string) shadowEligibilitySnapshotRecord {
	loaded, ok := cache.keyState.Load(canonical)
	if !ok {
		return shadowEligibilitySnapshotRecord{}
	}
	state, _ := loaded.(*shadowKeyState)
	if state == nil {
		return shadowEligibilitySnapshotRecord{}
	}
	record := state.snapshot.Load()
	if record == nil {
		return shadowEligibilitySnapshotRecord{generation: state.generation}
	}
	return *record
}

func (cache *ShadowCache) removeEntryLocked(canonical string, removeSnapshot bool) {
	entry := cache.entries[canonical]
	if entry == nil {
		return
	}
	if entry.evictableElem != nil {
		cache.evictableLRU.Remove(entry.evictableElem)
	}
	if entry.compactorElem != nil {
		next := entry.compactorElem.Next()
		if cache.compactorCursor == entry.compactorElem {
			cache.compactorCursor = next
		}
		cache.compactorList.Remove(entry.compactorElem)
	}
	delete(cache.entries, canonical)
	if removeSnapshot {
		loaded, _ := cache.keyState.Load(canonical)
		state, _ := loaded.(*shadowKeyState)
		cache.storeAbsentSnapshotLocked(state)
	}
}

func cloneShadowEntryView(entry *shadowEntry) ShadowEntryView {
	if entry == nil {
		return ShadowEntryView{}
	}
	return ShadowEntryView{
		CanonicalKey:           entry.canonicalKey,
		Descriptor:             cloneWatchDescriptor(entry.descriptor),
		Source:                 entry.source,
		Confidence:             entry.confidence,
		State:                  entry.state,
		Value:                  append([]byte(nil), entry.value...),
		ObservedAt:             entry.observedAt,
		ExpiresAt:              entry.expiresAt,
		Generation:             entry.generation,
		LastWriteGeneration:    entry.lastWriteGeneration,
		InvalidationGeneration: entry.invalidationGeneration,
		InvalidationReason:     entry.invalidationReason,
		InvalidationSource:     entry.invalidationSource,
		InvalidatedAt:          entry.invalidatedAt,
		Pinned:                 entry.pinClass != shadowPinClassNone,
	}
}
