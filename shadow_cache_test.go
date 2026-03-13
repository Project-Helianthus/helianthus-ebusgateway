package ebusgateway

import (
	"context"
	"testing"
	"time"
)

func TestShadowCacheDegradesWhenStaticPinnedFootprintExceedsBudget(t *testing.T) {
	t.Parallel()

	keys := []WatchKey{
		NewB509WatchKey(0x08, 0x0200),
		NewB509WatchKey(0x08, 0x0201),
		NewB509WatchKey(0x08, 0x0202),
	}
	catalog, activations := testShadowCatalogAndActivations(t, keys, WatchActivationSourcePoller)

	cache := NewShadowCache(ShadowCacheOptions{
		Catalog:               catalog,
		Activations:           activations,
		Capacity:              8,
		PinnedCapacity:        3,
		WriteConfirmPinnedCap: 1,
		Now:                   func() time.Time { return time.Unix(100, 0) },
	})

	summary := cache.Summary()
	if !summary.PinnedBudgetDegraded {
		t.Fatal("PinnedBudgetDegraded = false; want true")
	}
	if summary.Enabled {
		t.Fatal("Enabled = true; want false while pinned budget degraded")
	}

	result := cache.Write(ShadowWrite{
		Key:        keys[0],
		Source:     ShadowWriteSourcePassive,
		Confidence: ShadowConfidenceHigh,
		Value:      []byte{0x01},
		ObservedAt: time.Unix(100, 0),
	})
	if result.Accepted {
		t.Fatal("Write() succeeded while pinned budget degraded")
	}
	if result.Reason != ShadowWriteRejectionReasonPolicyReject {
		t.Fatalf("Write() rejection = %s; want %s", result.Reason, ShadowWriteRejectionReasonPolicyReject)
	}
}

func TestShadowCacheEvictsOldestEvictableEntryFirst(t *testing.T) {
	t.Parallel()

	pinnedKey := NewB509WatchKey(0x08, 0x0200)
	evictableA := NewB509WatchKey(0x08, 0x0201)
	evictableB := NewB509WatchKey(0x08, 0x0202)

	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{pinnedKey, evictableA, evictableB}, WatchActivationSourceTooling)
	if err := activations.Activate(WatchActivationSourcePoller, pinnedKey); err != nil {
		t.Fatalf("Activate(poller) error = %v", err)
	}

	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{
		Capacity:              2,
		PinnedCapacity:        2,
		WriteConfirmPinnedCap: 1,
	})

	writeShadow(t, cache, pinnedKey, ShadowWriteSourcePassive, time.Unix(100, 0), []byte{0x10})
	writeShadow(t, cache, evictableA, ShadowWriteSourcePassive, time.Unix(101, 0), []byte{0x11})
	writeShadow(t, cache, evictableB, ShadowWriteSourcePassive, time.Unix(102, 0), []byte{0x12})

	if _, ok := cache.Entry(evictableA); ok {
		t.Fatal("Entry(evictableA) = ok; want oldest evictable entry evicted first")
	}
	if _, ok := cache.Entry(pinnedKey); !ok {
		t.Fatal("Entry(pinnedKey) missing; pinned entry must not be evicted")
	}
	if _, ok := cache.Entry(evictableB); !ok {
		t.Fatal("Entry(evictableB) missing; newest evictable entry should remain")
	}
}

func TestShadowCachePrecedenceRejectsOlderActiveConfirmedWrite(t *testing.T) {
	t.Parallel()

	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{})

	writeShadow(t, cache, key, ShadowWriteSourcePassive, time.Unix(120, 0), []byte{0x20})
	startGeneration := cache.CaptureGeneration(key)

	result := cache.Write(ShadowWrite{
		Key:             key,
		Source:          ShadowWriteSourceActiveConfirmed,
		Confidence:      ShadowConfidenceHigh,
		Value:           []byte{0x21},
		ObservedAt:      time.Unix(119, 0),
		StartGeneration: startGeneration,
	})
	if result.Accepted {
		t.Fatal("older active_confirmed write accepted over newer passive evidence")
	}
	if result.Reason != ShadowWriteRejectionReasonStaleTimestamp {
		t.Fatalf("rejection reason = %s; want %s", result.Reason, ShadowWriteRejectionReasonStaleTimestamp)
	}

	entry, ok := cache.Entry(key)
	if !ok {
		t.Fatal("Entry(key) missing after rejected stale write")
	}
	if got := entry.Value; len(got) != 1 || got[0] != 0x20 {
		t.Fatalf("Entry(key).Value = %v; want [0x20]", got)
	}
}

func TestShadowCacheEqualTimestampActiveConfirmedOutranksPassive(t *testing.T) {
	t.Parallel()

	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{})

	writeShadow(t, cache, key, ShadowWriteSourcePassive, time.Unix(120, 0), []byte{0x20})
	startGeneration := cache.CaptureGeneration(key)
	writeShadowWithStart(t, cache, key, ShadowWriteSourceActiveConfirmed, time.Unix(120, 0), []byte{0x21}, startGeneration)

	entry, ok := cache.Entry(key)
	if !ok {
		t.Fatal("Entry(key) missing")
	}
	if got := entry.Value; len(got) != 1 || got[0] != 0x21 {
		t.Fatalf("Entry(key).Value = %v; want [0x21]", got)
	}
	if entry.Source != ShadowWriteSourceActiveConfirmed {
		t.Fatalf("Entry(key).Source = %s; want %s", entry.Source, ShadowWriteSourceActiveConfirmed)
	}
}

func TestShadowCacheRejectsSameTimestampConflictForSameSource(t *testing.T) {
	t.Parallel()

	key := NewB509WatchKey(0x08, 0x0200)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourceTooling)
	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{})

	writeShadow(t, cache, key, ShadowWriteSourcePassive, time.Unix(120, 0), []byte{0x20})
	result := cache.Write(ShadowWrite{
		Key:        key,
		Source:     ShadowWriteSourcePassive,
		Confidence: ShadowConfidenceHigh,
		Value:      []byte{0x21},
		ObservedAt: time.Unix(120, 0),
	})
	if result.Accepted {
		t.Fatal("same-timestamp passive conflict accepted")
	}
	if result.Reason != ShadowWriteRejectionReasonSameTimestampConflict {
		t.Fatalf("rejection reason = %s; want %s", result.Reason, ShadowWriteRejectionReasonSameTimestampConflict)
	}
}

func TestShadowCacheInvalidationGenerationRejectsStaleActiveWrite(t *testing.T) {
	t.Parallel()

	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{})

	writeShadow(t, cache, key, ShadowWriteSourcePassive, time.Unix(120, 0), []byte{0x20})
	startGeneration := cache.CaptureGeneration(key)

	invalidation := cache.Invalidate(ShadowInvalidation{
		Key:           key,
		Reason:        ShadowInvalidationReasonExternalWrite,
		Source:        ShadowInvalidationSourcePassive,
		InvalidatedAt: time.Unix(121, 0),
	})
	if invalidation.Generation == startGeneration {
		t.Fatalf("Invalidate() generation = %d; want generation advancement", invalidation.Generation)
	}
	if invalidation.State != ShadowEntryStateInvalidated {
		t.Fatalf("Invalidate() state = %s; want %s", invalidation.State, ShadowEntryStateInvalidated)
	}

	result := cache.Write(ShadowWrite{
		Key:             key,
		Source:          ShadowWriteSourceActiveConfirmed,
		Confidence:      ShadowConfidenceHigh,
		Value:           []byte{0x22},
		ObservedAt:      time.Unix(122, 0),
		StartGeneration: startGeneration,
	})
	if result.Accepted {
		t.Fatal("stale active_confirmed write accepted after invalidation generation advanced")
	}
	if result.Reason != ShadowWriteRejectionReasonGenerationAdvanced {
		t.Fatalf("rejection reason = %s; want %s", result.Reason, ShadowWriteRejectionReasonGenerationAdvanced)
	}

	entry, ok := cache.Entry(key)
	if !ok {
		t.Fatal("Entry(key) missing after invalidation")
	}
	if entry.State != ShadowEntryStateInvalidated {
		t.Fatalf("Entry(key).State = %s; want %s", entry.State, ShadowEntryStateInvalidated)
	}
	if len(entry.Value) == 0 {
		t.Fatal("invalidated entry lost retained diagnostic payload before compaction")
	}
}

func TestShadowCacheCapacityBlockedInvalidationStillRejectsStaleActiveWrite(t *testing.T) {
	t.Parallel()

	pinnedKey := NewB509WatchKey(0x08, 0x0200)
	invalidatedKey := NewB509WatchKey(0x08, 0x0201)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{pinnedKey, invalidatedKey}, WatchActivationSourceWriteConfirm)
	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{
		Capacity:              1,
		PinnedCapacity:        1,
		WriteConfirmPinnedCap: 1,
	})

	writeShadow(t, cache, pinnedKey, ShadowWriteSourcePassive, time.Unix(100, 0), []byte{0x20})
	startGeneration := cache.CaptureGeneration(invalidatedKey)

	invalidation := cache.Invalidate(ShadowInvalidation{
		Key:           invalidatedKey,
		Reason:        ShadowInvalidationReasonExternalWrite,
		Source:        ShadowInvalidationSourcePassive,
		InvalidatedAt: time.Unix(101, 0),
	})
	if invalidation.Generation == startGeneration {
		t.Fatalf("Invalidate() generation = %d; want generation advancement even when tombstone admission fails", invalidation.Generation)
	}
	if invalidation.State != ShadowEntryStateTombstone {
		t.Fatalf("Invalidate() state = %s; want %s when tombstone could not be admitted", invalidation.State, ShadowEntryStateTombstone)
	}

	snapshot := cache.SnapshotEligibility(invalidatedKey)
	if snapshot.Present {
		t.Fatalf("SnapshotEligibility() = %+v; want no cached entry when tombstone admission fails", snapshot)
	}
	if snapshot.Generation != invalidation.Generation {
		t.Fatalf("SnapshotEligibility().Generation = %d; want %d", snapshot.Generation, invalidation.Generation)
	}

	activations.Deactivate(WatchActivationSourceWriteConfirm, pinnedKey)
	cache.RefreshActivations()

	result := cache.Write(ShadowWrite{
		Key:             invalidatedKey,
		Source:          ShadowWriteSourceActiveConfirmed,
		Confidence:      ShadowConfidenceHigh,
		Value:           []byte{0x21},
		ObservedAt:      time.Unix(102, 0),
		StartGeneration: startGeneration,
	})
	if result.Accepted {
		t.Fatal("stale active_confirmed write accepted after capacity-blocked invalidation")
	}
	if result.Reason != ShadowWriteRejectionReasonGenerationAdvanced {
		t.Fatalf("rejection reason = %s; want %s", result.Reason, ShadowWriteRejectionReasonGenerationAdvanced)
	}

	if _, ok := cache.Entry(invalidatedKey); ok {
		t.Fatal("Entry(invalidatedKey) present; want no cached tombstone after admission failure")
	}
}

func TestShadowCacheInvalidateWriteConfirmPinCapFailsClosed(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		capacity       int
		seedEvictable  bool
		wantTotalEntry int
	}{
		{
			name:           "with_free_capacity",
			capacity:       3,
			wantTotalEntry: 1,
		},
		{
			name:           "without_evicting_existing_evictable_entry",
			capacity:       2,
			seedEvictable:  true,
			wantTotalEntry: 2,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			pinnedKey := NewB509WatchKey(0x08, 0x0200)
			evictableKey := NewB509WatchKey(0x08, 0x0201)
			invalidatedKey := NewB509WatchKey(0x08, 0x0202)

			catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{pinnedKey, evictableKey, invalidatedKey}, WatchActivationSourceTooling)
			if err := activations.Activate(WatchActivationSourceWriteConfirm, pinnedKey, invalidatedKey); err != nil {
				t.Fatalf("Activate(write_confirm) error = %v", err)
			}

			cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{
				Capacity:              tc.capacity,
				PinnedCapacity:        2,
				WriteConfirmPinnedCap: 1,
			})

			writeShadow(t, cache, pinnedKey, ShadowWriteSourcePassive, time.Unix(100, 0), []byte{0x20})
			if tc.seedEvictable {
				writeShadow(t, cache, evictableKey, ShadowWriteSourcePassive, time.Unix(101, 0), []byte{0x21})
			}

			startGeneration := cache.CaptureGeneration(invalidatedKey)
			invalidation := cache.Invalidate(ShadowInvalidation{
				Key:           invalidatedKey,
				Reason:        ShadowInvalidationReasonExternalWrite,
				Source:        ShadowInvalidationSourcePassive,
				InvalidatedAt: time.Unix(102, 0),
			})
			if invalidation.Generation == startGeneration {
				t.Fatalf("Invalidate() generation = %d; want generation advancement when write-confirm pin cap is full", invalidation.Generation)
			}
			if invalidation.State != ShadowEntryStateTombstone {
				t.Fatalf("Invalidate() state = %s; want %s when write-confirm tombstone admission fails", invalidation.State, ShadowEntryStateTombstone)
			}

			snapshot := cache.SnapshotEligibility(invalidatedKey)
			if snapshot.Present {
				t.Fatalf("SnapshotEligibility() = %+v; want no cached tombstone when write-confirm pin cap is full", snapshot)
			}
			if snapshot.Generation != invalidation.Generation {
				t.Fatalf("SnapshotEligibility().Generation = %d; want %d", snapshot.Generation, invalidation.Generation)
			}

			if _, ok := cache.Entry(invalidatedKey); ok {
				t.Fatal("Entry(invalidatedKey) present; want no cached tombstone when write-confirm pin cap is full")
			}
			if _, ok := cache.Entry(pinnedKey); !ok {
				t.Fatal("Entry(pinnedKey) missing; existing write-confirm pinned entry must remain")
			}
			if tc.seedEvictable {
				if _, ok := cache.Entry(evictableKey); !ok {
					t.Fatal("Entry(evictableKey) missing; invalidation must not evict unrelated evictable entries before failing the write-confirm pin cap")
				}
			}

			summary := cache.Summary()
			if summary.TotalEntries != tc.wantTotalEntry {
				t.Fatalf("Summary().TotalEntries = %d; want %d", summary.TotalEntries, tc.wantTotalEntry)
			}
			if summary.WriteConfirmPinnedActive != 1 {
				t.Fatalf("Summary().WriteConfirmPinnedActive = %d; want 1", summary.WriteConfirmPinnedActive)
			}
		})
	}
}

func TestShadowCacheInvalidateCachedEntryDoesNotExceedWriteConfirmPinCap(t *testing.T) {
	t.Parallel()

	pinnedKey := NewB509WatchKey(0x08, 0x0200)
	cachedKey := NewB509WatchKey(0x08, 0x0201)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{pinnedKey, cachedKey}, WatchActivationSourceTooling)
	if err := activations.Activate(WatchActivationSourceWriteConfirm, pinnedKey); err != nil {
		t.Fatalf("Activate(write_confirm pinnedKey) error = %v", err)
	}

	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{
		Capacity:              2,
		PinnedCapacity:        2,
		WriteConfirmPinnedCap: 1,
	})

	writeShadow(t, cache, pinnedKey, ShadowWriteSourcePassive, time.Unix(100, 0), []byte{0x20})
	writeShadow(t, cache, cachedKey, ShadowWriteSourcePassive, time.Unix(101, 0), []byte{0x21})

	entry, ok := cache.Entry(cachedKey)
	if !ok {
		t.Fatal("Entry(cachedKey) missing before invalidation")
	}
	if entry.Pinned {
		t.Fatal("Entry(cachedKey).Pinned = true before invalidation; want existing cached entry to start evictable")
	}

	if err := activations.Activate(WatchActivationSourceWriteConfirm, cachedKey); err != nil {
		t.Fatalf("Activate(write_confirm cachedKey) error = %v", err)
	}
	cache.RefreshActivations()

	startGeneration := cache.CaptureGeneration(cachedKey)
	invalidation := cache.Invalidate(ShadowInvalidation{
		Key:           cachedKey,
		Reason:        ShadowInvalidationReasonExternalWrite,
		Source:        ShadowInvalidationSourcePassive,
		InvalidatedAt: time.Unix(102, 0),
	})
	if invalidation.Generation == startGeneration {
		t.Fatalf("Invalidate() generation = %d; want generation advancement", invalidation.Generation)
	}
	if invalidation.State != ShadowEntryStateInvalidated {
		t.Fatalf("Invalidate() state = %s; want %s for cached entry retaining diagnostic payload", invalidation.State, ShadowEntryStateInvalidated)
	}

	entry, ok = cache.Entry(cachedKey)
	if !ok {
		t.Fatal("Entry(cachedKey) missing after invalidation")
	}
	if entry.State != ShadowEntryStateInvalidated {
		t.Fatalf("Entry(cachedKey).State = %s; want %s", entry.State, ShadowEntryStateInvalidated)
	}
	if entry.Pinned {
		t.Fatal("Entry(cachedKey).Pinned = true; want cached invalidation to stay unpinned when write-confirm cap is already full")
	}

	summary := cache.Summary()
	if summary.WriteConfirmPinnedActive != 1 {
		t.Fatalf("Summary().WriteConfirmPinnedActive = %d; want 1", summary.WriteConfirmPinnedActive)
	}
}

func TestShadowCacheCompactsAndDepinsPinnedTombstone(t *testing.T) {
	t.Parallel()

	base := time.Unix(100, 0)
	current := base
	key := NewB509WatchKey(0x08, 0x0200)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := newTestShadowCache(t, catalog, activations, base, ShadowCacheOptions{
		TombstoneRetainWindow: 15 * time.Minute,
		TombstoneHardLifespan: 24 * time.Hour,
	})
	cache.now = func() time.Time { return current }

	writeShadow(t, cache, key, ShadowWriteSourcePassive, base, []byte{0x20})
	cache.Invalidate(ShadowInvalidation{
		Key:           key,
		Reason:        ShadowInvalidationReasonExternalWrite,
		Source:        ShadowInvalidationSourcePassive,
		InvalidatedAt: base.Add(time.Minute),
	})

	current = base.Add(16 * time.Minute)
	cache.CompactOnce()

	entry, ok := cache.Entry(key)
	if !ok {
		t.Fatal("Entry(key) missing after tombstone compaction")
	}
	if entry.State != ShadowEntryStateTombstone {
		t.Fatalf("Entry(key).State = %s; want %s", entry.State, ShadowEntryStateTombstone)
	}
	if len(entry.Value) != 0 {
		t.Fatalf("Entry(key).Value = %v; want compacted metadata-only tombstone", entry.Value)
	}
	if !entry.Pinned {
		t.Fatal("Entry(key).Pinned = false; want tombstone to stay pinned before hard lifespan expires")
	}

	current = base.Add(25 * time.Hour)
	cache.CompactOnce()

	if _, ok := cache.Entry(key); ok {
		t.Fatal("Entry(key) still present after hard tombstone lifespan elapsed")
	}
	snapshot := cache.SnapshotEligibility(key)
	if snapshot.Present {
		t.Fatal("SnapshotEligibility(key).Present = true; want false after tombstone de-pin eviction")
	}
	if snapshot.Generation == 0 {
		t.Fatal("SnapshotEligibility(key).Generation = 0; want persisted generation after depin")
	}
}

func TestShadowCacheRefreshActivationsDepinsEntryWhenLastSourceRemoved(t *testing.T) {
	t.Parallel()

	key := NewB509WatchKey(0x08, 0x0200)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{})

	writeShadow(t, cache, key, ShadowWriteSourcePassive, time.Unix(100, 0), []byte{0x20})
	entry, ok := cache.Entry(key)
	if !ok || !entry.Pinned {
		t.Fatal("initial entry not pinned")
	}

	activations.Deactivate(WatchActivationSourcePoller, key)
	cache.RefreshActivations()

	entry, ok = cache.Entry(key)
	if !ok {
		t.Fatal("Entry(key) missing after deactivation")
	}
	if entry.Pinned {
		t.Fatal("Entry(key).Pinned = true; want immediate de-pin after last activation source removed")
	}
}

func TestShadowCacheWriteConfirmPinCapFailsClosedPerKey(t *testing.T) {
	t.Parallel()

	keyA := NewB509WatchKey(0x08, 0x0200)
	keyB := NewB509WatchKey(0x08, 0x0201)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{keyA, keyB}, WatchActivationSourceWriteConfirm)
	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{
		PinnedCapacity:        2,
		WriteConfirmPinnedCap: 1,
	})

	writeShadow(t, cache, keyA, ShadowWriteSourcePassive, time.Unix(100, 0), []byte{0x20})
	result := cache.Write(ShadowWrite{
		Key:        keyB,
		Source:     ShadowWriteSourcePassive,
		Confidence: ShadowConfidenceHigh,
		Value:      []byte{0x21},
		ObservedAt: time.Unix(101, 0),
	})
	if result.Accepted {
		t.Fatal("second write-confirm pinned key accepted past reserved cap")
	}
	if result.Reason != ShadowWriteRejectionReasonCapacity {
		t.Fatalf("rejection reason = %s; want %s", result.Reason, ShadowWriteRejectionReasonCapacity)
	}
}

func TestShadowCacheSnapshotEligibilityAndLookupFollowFreshness(t *testing.T) {
	t.Parallel()

	base := time.Unix(100, 0)
	current := base
	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := newTestShadowCache(t, catalog, activations, base, ShadowCacheOptions{})
	cache.now = func() time.Time { return current }

	writeShadow(t, cache, key, ShadowWriteSourcePassive, base, []byte{0x20})

	snapshot := cache.SnapshotEligibility(key)
	if !snapshot.Present || !snapshot.Eligible {
		t.Fatalf("SnapshotEligibility() = %+v; want present and eligible", snapshot)
	}

	current = base.Add(11 * time.Second)
	snapshot = cache.SnapshotEligibility(key)
	if snapshot.Eligible {
		t.Fatalf("SnapshotEligibility() = %+v; want ineligible after descriptor ttl expiry", snapshot)
	}

	lookup := cache.Lookup(key, 20*time.Second)
	if !lookup.Found {
		t.Fatal("Lookup() did not return retained entry")
	}
	if lookup.Eligible {
		t.Fatal("Lookup().Eligible = true; want false after descriptor ttl expiry")
	}
}

func TestShadowCacheInvalidateRepinnedEntryLeavesEvictionLRU(t *testing.T) {
	t.Parallel()

	repinnedKey := NewB509WatchKey(0x08, 0x0200)
	evictableKey := NewB509WatchKey(0x08, 0x0201)
	newcomerKey := NewB509WatchKey(0x08, 0x0202)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{repinnedKey, evictableKey, newcomerKey}, WatchActivationSourceTooling)
	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{
		Capacity:              2,
		PinnedCapacity:        2,
		WriteConfirmPinnedCap: 1,
	})

	writeShadow(t, cache, repinnedKey, ShadowWriteSourcePassive, time.Unix(100, 0), []byte{0x20})
	writeShadow(t, cache, evictableKey, ShadowWriteSourcePassive, time.Unix(101, 0), []byte{0x21})

	if err := activations.Activate(WatchActivationSourceWriteConfirm, repinnedKey); err != nil {
		t.Fatalf("Activate(write_confirm repinnedKey) error = %v", err)
	}

	invalidation := cache.Invalidate(ShadowInvalidation{
		Key:           repinnedKey,
		Reason:        ShadowInvalidationReasonExternalWrite,
		Source:        ShadowInvalidationSourcePassive,
		InvalidatedAt: time.Unix(102, 0),
	})
	if invalidation.State != ShadowEntryStateInvalidated {
		t.Fatalf("Invalidate() state = %s; want %s for cached entry with retained payload", invalidation.State, ShadowEntryStateInvalidated)
	}

	entry, ok := cache.entries[repinnedKey.Canonical()]
	if !ok {
		t.Fatal("repinned entry missing after invalidation")
	}
	if entry.pinClass != shadowPinClassWriteConfirm {
		t.Fatalf("repinned entry pinClass = %v; want %v", entry.pinClass, shadowPinClassWriteConfirm)
	}
	if entry.evictableElem != nil {
		t.Fatal("repinned entry still linked in eviction LRU after invalidation")
	}

	writeShadow(t, cache, newcomerKey, ShadowWriteSourcePassive, time.Unix(103, 0), []byte{0x22})

	if _, ok := cache.Entry(repinnedKey); !ok {
		t.Fatal("repinned invalidated entry was evicted under capacity pressure")
	}
	if _, ok := cache.Entry(evictableKey); ok {
		t.Fatal("unrelated evictable entry remained; want it evicted before the repinned tombstone")
	}

	summary := cache.Summary()
	if summary.WriteConfirmPinnedActive != 1 {
		t.Fatalf("Summary().WriteConfirmPinnedActive = %d; want 1", summary.WriteConfirmPinnedActive)
	}
}

func TestShadowCacheEvictionStoresAbsentSnapshot(t *testing.T) {
	t.Parallel()

	evictedKey := NewB509WatchKey(0x08, 0x0200)
	survivorKey := NewB509WatchKey(0x08, 0x0201)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{evictedKey, survivorKey}, WatchActivationSourceTooling)
	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{
		Capacity:       1,
		PinnedCapacity: 1,
	})

	writeShadow(t, cache, evictedKey, ShadowWriteSourcePassive, time.Unix(100, 0), []byte{0x20})
	generation := cache.CaptureGeneration(evictedKey)
	if generation == 0 {
		t.Fatal("CaptureGeneration(evictedKey) = 0; want persisted generation before eviction")
	}

	writeShadow(t, cache, survivorKey, ShadowWriteSourcePassive, time.Unix(101, 0), []byte{0x21})

	if _, ok := cache.Entry(evictedKey); ok {
		t.Fatal("Entry(evictedKey) still present after capacity eviction")
	}
	snapshot := cache.SnapshotEligibility(evictedKey)
	if snapshot.Present {
		t.Fatalf("SnapshotEligibility(evictedKey) = %+v; want absent after eviction", snapshot)
	}
	if snapshot.Generation != generation {
		t.Fatalf("SnapshotEligibility(evictedKey).Generation = %d; want %d", snapshot.Generation, generation)
	}
}

func TestShadowCacheStartAndCloseCompactor(t *testing.T) {
	t.Parallel()

	key := NewB509WatchKey(0x08, 0x0200)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{
		CompactorCadence:         5 * time.Millisecond,
		ShutdownCompactorTimeout: 100 * time.Millisecond,
	})
	cache.StartCompactor()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := cache.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestShadowCacheCloseIsIdempotentAcrossConcurrentCalls(t *testing.T) {
	t.Parallel()

	key := NewB509WatchKey(0x08, 0x0200)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := newTestShadowCache(t, catalog, activations, time.Unix(100, 0), ShadowCacheOptions{
		ShutdownCompactorTimeout: 100 * time.Millisecond,
	})
	cache.compactorStarted.Store(true)
	cache.compactorStop = make(chan struct{})
	cache.compactorDone = make(chan struct{})

	stopObserved := make(chan struct{})
	releaseDone := make(chan struct{})
	go func() {
		<-cache.compactorStop
		close(stopObserved)
		<-releaseDone
		close(cache.compactorDone)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	errCh := make(chan error, 2)
	go func() {
		errCh <- cache.Close(ctx)
	}()

	<-stopObserved

	go func() {
		errCh <- cache.Close(ctx)
	}()

	close(releaseDone)

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("Close() error = %v; want nil across repeated/concurrent calls", err)
		}
	}
}

func newTestShadowCache(t *testing.T, catalog *WatchCatalog, activations *WatchActivationSet, base time.Time, overrides ShadowCacheOptions) *ShadowCache {
	t.Helper()
	options := overrides
	options.Catalog = catalog
	options.Activations = activations
	if options.Now == nil {
		options.Now = func() time.Time { return base }
	}
	return NewShadowCache(options)
}

func writeShadow(t *testing.T, cache *ShadowCache, key WatchKey, source ShadowWriteSource, observedAt time.Time, value []byte) {
	t.Helper()
	startGeneration := uint64(0)
	if source == ShadowWriteSourceActiveConfirmed {
		startGeneration = cache.CaptureGeneration(key)
	}
	writeShadowWithStart(t, cache, key, source, observedAt, value, startGeneration)
}

func writeShadowWithStart(t *testing.T, cache *ShadowCache, key WatchKey, source ShadowWriteSource, observedAt time.Time, value []byte, startGeneration uint64) {
	t.Helper()
	result := cache.Write(ShadowWrite{
		Key:             key,
		Source:          source,
		Confidence:      ShadowConfidenceHigh,
		Value:           value,
		ObservedAt:      observedAt,
		StartGeneration: startGeneration,
	})
	if !result.Accepted {
		t.Fatalf("Write() rejected with reason %s", result.Reason)
	}
}

func testShadowCatalogAndActivations(t *testing.T, keys []WatchKey, source WatchActivationSource) (*WatchCatalog, *WatchActivationSet) {
	t.Helper()
	descriptors := make([]WatchDescriptor, 0, len(keys))
	for _, key := range keys {
		descriptor := WatchDescriptor{
			Key:               key,
			SemanticClass:     WatchSemanticClassState,
			FreshnessProfile:  WatchFreshnessProfileStateFast,
			DecoderID:         "test.decoder",
			CorrelationPolicy: WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: WatchDirectApplyPolicyStateDefault,
		}
		descriptors = append(descriptors, descriptor)
	}
	catalog, err := NewWatchCatalog(descriptors)
	if err != nil {
		t.Fatalf("NewWatchCatalog() error = %v", err)
	}
	activations := NewWatchActivationSet(catalog)
	if err := activations.Activate(source, keys...); err != nil {
		t.Fatalf("Activate(%s) error = %v", source, err)
	}
	return catalog, activations
}
