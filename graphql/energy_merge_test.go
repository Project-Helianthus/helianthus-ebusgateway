package graphql

import (
	"expvar"
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

var (
	t0 = time.Date(2026, 2, 26, 12, 0, 0, 0, time.UTC)
	t1 = t0.Add(1 * time.Second)
	t2 = t0.Add(2 * time.Second)
)

func TestEnergyMerge_EmptyAcceptsAny(t *testing.T) {
	store := newEnergyMergeStore()
	key := wkey("gas", "heating", "day", "")

	if !store.Apply(key, 1.5, EnergySourceBroadcast, t0) {
		t.Fatal("first broadcast into empty store should be accepted")
	}

	store2 := newEnergyMergeStore()
	if !store2.Apply(key, 2.5, EnergySourceRegister, t0) {
		t.Fatal("first register into empty store should be accepted")
	}
}

func TestEnergyMerge_RegisterOverwritesBroadcast(t *testing.T) {
	store := newEnergyMergeStore()
	key := wkey("gas", "heating", "day", "")

	store.Apply(key, 1.0, EnergySourceBroadcast, t0)

	if !store.Apply(key, 2.0, EnergySourceRegister, t1) {
		t.Fatal("register should overwrite broadcast")
	}

	// Key is canonicalized: "heating" → "climate"
	canonKey := wkey("gas", "climate", "day", "")
	store.mu.RLock()
	point := store.points[canonKey]
	store.mu.RUnlock()

	if point.Value != 2.0 {
		t.Fatalf("value = %f; want 2.0", point.Value)
	}
	if point.Source != EnergySourceRegister {
		t.Fatalf("source = %d; want EnergySourceRegister", point.Source)
	}
}

func TestEnergyMerge_BroadcastNeverOverwritesRegister(t *testing.T) {
	store := newEnergyMergeStore()
	key := wkey("gas", "heating", "day", "")

	store.Apply(key, 1.0, EnergySourceRegister, t0)

	// Broadcast with newer timestamp should still be rejected.
	if store.Apply(key, 99.0, EnergySourceBroadcast, t2) {
		t.Fatal("broadcast should never overwrite register")
	}

	canonKey := wkey("gas", "climate", "day", "")
	store.mu.RLock()
	point := store.points[canonKey]
	store.mu.RUnlock()

	if point.Value != 1.0 {
		t.Fatalf("value = %f; want 1.0 (original register)", point.Value)
	}
}

func TestEnergyMerge_MonotonicBroadcast(t *testing.T) {
	store := newEnergyMergeStore()
	key := wkey("electricity", "hot_water", "year", "current")

	store.Apply(key, 10.0, EnergySourceBroadcast, t1)

	// Newer broadcast overwrites.
	if !store.Apply(key, 20.0, EnergySourceBroadcast, t2) {
		t.Fatal("newer broadcast should overwrite older broadcast")
	}

	// Older broadcast rejected.
	if store.Apply(key, 30.0, EnergySourceBroadcast, t0) {
		t.Fatal("older broadcast should be rejected (monotonic)")
	}

	// Same timestamp rejected (not strictly newer).
	if store.Apply(key, 40.0, EnergySourceBroadcast, t2) {
		t.Fatal("same-timestamp broadcast should be rejected (monotonic: must be strictly newer)")
	}

	canonKey := wkey("electricity", "hot_water", "year", "current")
	store.mu.RLock()
	point := store.points[canonKey]
	store.mu.RUnlock()

	if point.Value != 20.0 {
		t.Fatalf("value = %f; want 20.0", point.Value)
	}
}

func TestEnergyMerge_MonotonicRegister(t *testing.T) {
	store := newEnergyMergeStore()
	key := wkey("solar", "heating", "day", "")

	store.Apply(key, 100.0, EnergySourceRegister, t1)

	// Newer register overwrites.
	if !store.Apply(key, 200.0, EnergySourceRegister, t2) {
		t.Fatal("newer register should overwrite older register")
	}

	// Older register rejected.
	if store.Apply(key, 300.0, EnergySourceRegister, t0) {
		t.Fatal("older register should be rejected (monotonic)")
	}

	canonKey := wkey("solar", "climate", "day", "")
	store.mu.RLock()
	point := store.points[canonKey]
	store.mu.RUnlock()

	if point.Value != 200.0 {
		t.Fatalf("value = %f; want 200.0", point.Value)
	}
}

func TestEnergyMerge_RegisterOverwritesBroadcastRegardlessOfTime(t *testing.T) {
	store := newEnergyMergeStore()
	key := wkey("gas", "hot_water", "year", "previous")

	// Broadcast at t2 (later timestamp).
	store.Apply(key, 5.0, EnergySourceBroadcast, t2)

	// Register at t0 (earlier timestamp) should still win.
	if !store.Apply(key, 10.0, EnergySourceRegister, t0) {
		t.Fatal("register should overwrite broadcast even with older timestamp")
	}

	canonKey := wkey("gas", "hot_water", "year", "previous")
	store.mu.RLock()
	point := store.points[canonKey]
	store.mu.RUnlock()

	if point.Value != 10.0 {
		t.Fatalf("value = %f; want 10.0", point.Value)
	}
	if point.Source != EnergySourceRegister {
		t.Fatalf("source = %d; want EnergySourceRegister", point.Source)
	}
}

func TestEnergyMerge_SnapshotBuildsCorrectTotals(t *testing.T) {
	store := newEnergyMergeStore()

	// Gas heating day.
	store.Apply(wkey("gas", "heating", "day", ""), 1.5, EnergySourceBroadcast, t0)

	// Gas hot_water year current.
	store.Apply(wkey("gas", "hot_water", "year", "current"), 100.0, EnergySourceBroadcast, t0)

	// Electricity hot_water year previous.
	store.Apply(wkey("electricity", "hot_water", "year", "previous"), 200.0, EnergySourceRegister, t0)

	// Solar heating day.
	store.Apply(wkey("solar", "heating", "day", ""), 50.0, EnergySourceBroadcast, t0)

	totals := store.SnapshotWithContext(t0, "")
	if totals == nil {
		t.Fatal("Snapshot() = nil; want non-nil")
	}

	// Gas.Climate.Today = 1.5.
	if totals.Gas.Climate.Today != 1.5 {
		t.Fatalf("Gas.Climate.Today = %f; want 1.5", totals.Gas.Climate.Today)
	}

	// Gas.DHW.Yearly[1] = 100.0.
	if len(totals.Gas.DHW.Yearly) < 2 {
		t.Fatalf("Gas.DHW.Yearly len = %d; want >= 2", len(totals.Gas.DHW.Yearly))
	}
	if totals.Gas.DHW.Yearly[1] != 100.0 {
		t.Fatalf("Gas.DHW.Yearly[1] = %f; want 100.0", totals.Gas.DHW.Yearly[1])
	}

	// Electric.DHW.Yearly[0] = 200.0.
	if len(totals.Electric.DHW.Yearly) < 2 {
		t.Fatalf("Electric.DHW.Yearly len = %d; want >= 2", len(totals.Electric.DHW.Yearly))
	}
	if totals.Electric.DHW.Yearly[0] != 200.0 {
		t.Fatalf("Electric.DHW.Yearly[0] = %f; want 200.0", totals.Electric.DHW.Yearly[0])
	}

	// Solar.Climate.Today = 50.0.
	if totals.Solar.Climate.Today != 50.0 {
		t.Fatalf("Solar.Climate.Today = %f; want 50.0", totals.Solar.Climate.Today)
	}
}

func TestEnergyMerge_SnapshotBuildsMonthlySeries(t *testing.T) {
	store := newEnergyMergeStore()

	// Gas climate: this month = 284, last month = 266.
	store.Apply(wkey("gas", "climate", "month", "current"), 284.0, EnergySourceRegister, t0)
	store.Apply(wkey("gas", "climate", "month", "previous"), 266.0, EnergySourceRegister, t0)

	// Electricity hot_water: this month = 5, last month = 3.
	store.Apply(wkey("electricity", "hot_water", "month", "current"), 5.0, EnergySourceRegister, t0)
	store.Apply(wkey("electricity", "hot_water", "month", "previous"), 3.0, EnergySourceRegister, t0)

	totals := store.SnapshotWithContext(t0, "")
	if totals == nil {
		t.Fatal("Snapshot() = nil; want non-nil")
	}

	if len(totals.Gas.Climate.Monthly) < 2 {
		t.Fatalf("Gas.Climate.Monthly len = %d; want >= 2", len(totals.Gas.Climate.Monthly))
	}
	if totals.Gas.Climate.Monthly[0] != 266.0 {
		t.Fatalf("Gas.Climate.Monthly[0] = %f; want 266.0 (previous)", totals.Gas.Climate.Monthly[0])
	}
	if totals.Gas.Climate.Monthly[1] != 284.0 {
		t.Fatalf("Gas.Climate.Monthly[1] = %f; want 284.0 (current)", totals.Gas.Climate.Monthly[1])
	}

	if len(totals.Electric.DHW.Monthly) < 2 {
		t.Fatalf("Electric.DHW.Monthly len = %d; want >= 2", len(totals.Electric.DHW.Monthly))
	}
	if totals.Electric.DHW.Monthly[0] != 3.0 {
		t.Fatalf("Electric.DHW.Monthly[0] = %f; want 3.0 (previous)", totals.Electric.DHW.Monthly[0])
	}
	if totals.Electric.DHW.Monthly[1] != 5.0 {
		t.Fatalf("Electric.DHW.Monthly[1] = %f; want 5.0 (current)", totals.Electric.DHW.Monthly[1])
	}
}

func TestEnergyMerge_SnapshotMonthlyMetadataBackfillsPartialSelectors(t *testing.T) {
	cases := []struct {
		name         string
		presentKind  string
		presentIndex int
		missingIndex int
	}{
		{name: "current_only", presentKind: "current", presentIndex: 1, missingIndex: 0},
		{name: "previous_only", presentKind: "previous", presentIndex: 0, missingIndex: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newEnergyMergeStore()
			if !store.Apply(wkey("gas", "climate", "month", tc.presentKind), 9.5, EnergySourceRegister, t0) {
				t.Fatal("Apply() = false; want true")
			}

			snap := store.SnapshotWithContext(t0.Add(time.Minute), "")
			if snap == nil {
				t.Fatal("SnapshotWithContext() = nil; want non-nil")
			}
			if len(snap.Gas.Climate.MonthlyMeta) != 2 {
				t.Fatalf("Gas.Climate.MonthlyMeta len = %d; want 2", len(snap.Gas.Climate.MonthlyMeta))
			}

			presentMeta := snap.Gas.Climate.MonthlyMeta[tc.presentIndex]
			if presentMeta.FreshnessState != EnergyFreshnessStateFresh {
				t.Fatalf("present selector freshness = %q; want fresh", presentMeta.FreshnessState)
			}
			if presentMeta.Provenance != EnergyProvenanceRegister {
				t.Fatalf("present selector provenance = %q; want register", presentMeta.Provenance)
			}

			missingMeta := snap.Gas.Climate.MonthlyMeta[tc.missingIndex]
			if missingMeta.FreshnessState != EnergyFreshnessStateNeverSeen {
				t.Fatalf("missing selector freshness = %q; want never_seen", missingMeta.FreshnessState)
			}
			if missingMeta.Provenance != EnergyProvenanceNone {
				t.Fatalf("missing selector provenance = %q; want none", missingMeta.Provenance)
			}

			for i, meta := range snap.Gas.Climate.MonthlyMeta {
				if meta.FreshnessState == "" {
					t.Fatalf("MonthlyMeta[%d].FreshnessState is empty; want non-empty", i)
				}
				if meta.Provenance == "" {
					t.Fatalf("MonthlyMeta[%d].Provenance is empty; want non-empty", i)
				}
			}
		})
	}
}

// A store that has never accepted a point projects nothing, whatever the
// passive state; a rejected apply is not an accepted point. From the first
// accepted point on, every slot without an observation reads 0/none.
func TestEnergyMerge_SnapshotIsNilUntilFirstAcceptedPoint(t *testing.T) {
	store := newEnergyMergeStore()
	for _, passive := range []string{"", "unavailable"} {
		if snap := store.SnapshotWithContext(t0, passive); snap != nil {
			t.Fatalf("SnapshotWithContext(%q) before any point = %+v; want nil", passive, snap)
		}
	}
	if snap := store.Snapshot(); snap != nil {
		t.Fatalf("Snapshot() before any point = %+v; want nil", snap)
	}

	obsolete := wkey("gas", "hot_water", "year", "previous")
	obsolete.Year--
	if store.Apply(obsolete, 1, EnergySourceRegister, t0) {
		t.Fatal("Apply(obsolete year) accepted; want rejected")
	}
	if snap := store.SnapshotWithContext(t0, ""); snap != nil {
		t.Fatalf("SnapshotWithContext() after a rejected apply = %+v; want nil", snap)
	}

	if !store.Apply(wkey("electricity", "climate", "day", ""), 1.25, EnergySourceRegister, t0) {
		t.Fatal("Apply(electricity climate day) rejected; want accepted")
	}
	snap := store.SnapshotWithContext(t0, "")
	if snap == nil {
		t.Fatal("SnapshotWithContext() after the first accepted point = nil; want projection")
	}
	if got := snap.Electric.Climate.Today; got != 1.25 {
		t.Fatalf("Electric.Climate.Today = %v; want 1.25", got)
	}
	if got := snap.Gas.DHW.Today; got != 0 {
		t.Fatalf("Gas.DHW.Today = %v; want 0", got)
	}
	if got := snap.Gas.DHW.TodayMeta.FreshnessState; got != EnergyFreshnessStateNeverSeen {
		t.Fatalf("Gas.DHW.TodayMeta.FreshnessState = %q; want never_seen", got)
	}
	if got := snap.Gas.DHW.TodayMeta.Provenance; got != EnergyProvenanceNone {
		t.Fatalf("Gas.DHW.TodayMeta.Provenance = %q; want none", got)
	}
	if len(snap.Gas.DHW.YearlyMeta) != 2 {
		t.Fatalf("Gas.DHW.YearlyMeta len = %d; want 2 for previous/current selectors", len(snap.Gas.DHW.YearlyMeta))
	}

	unavailable := store.SnapshotWithContext(t0, "unavailable")
	if unavailable == nil {
		t.Fatal("SnapshotWithContext(unavailable) after the first accepted point = nil; want projection")
	}
	if got := unavailable.Gas.DHW.TodayMeta.FreshnessState; got != EnergyFreshnessStateUnavailable {
		t.Fatalf("Gas.DHW.TodayMeta.FreshnessState = %q; want unavailable", got)
	}
}

// Once a point was accepted the projection stays visible: after pruning, after
// every retained point has left the projection, and even with no retained
// point at all. Pruning runs only inside an accepted apply and keeps the point
// just written, so the store cannot empty itself today; the last case removes
// the points directly so that a retained-point count can never stand in for the
// accepted-point fact.
func TestEnergyMerge_SnapshotStaysVisibleAfterPruning(t *testing.T) {
	store := newEnergyMergeStore()
	november := time.Date(2026, time.November, 15, 12, 0, 0, 0, time.UTC)
	novemberMonth := energyWindowKey("gas", "climate", "month", "current", november)
	novemberDay := energyWindowKey("gas", "climate", "day", "", november)
	if !store.Apply(novemberMonth, 40, EnergySourceRegister, november) || !store.Apply(novemberDay, 2, EnergySourceRegister, november) {
		t.Fatal("November month/day applies rejected; want accepted")
	}

	january := time.Date(2027, time.January, 15, 12, 0, 0, 0, time.UTC)
	if !store.Apply(energyWindowKey("gas", "climate", "month", "current", january), 30, EnergySourceRegister, january) ||
		!store.Apply(energyWindowKey("gas", "climate", "day", "", january), 1, EnergySourceRegister, january) {
		t.Fatal("January month/day applies rejected; want accepted")
	}
	if _, kept := store.points[novemberMonth]; kept {
		t.Fatal("November month window retained; want pruned by the January write")
	}
	if _, kept := store.points[novemberDay]; kept {
		t.Fatal("November day retained; want pruned by the January day write")
	}

	// A month later nothing retained projects onto the reference date.
	february := time.Date(2027, time.February, 16, 12, 0, 0, 0, time.UTC)
	snap := store.SnapshotWithContext(february, "")
	if snap == nil {
		t.Fatal("SnapshotWithContext() after pruning = nil; want projection")
	}
	if got := snap.Gas.Climate.Today; got != 0 {
		t.Fatalf("Gas.Climate.Today = %v; want 0 once the retained day has passed", got)
	}
	if got := snap.Gas.Climate.TodayMeta.Provenance; got != EnergyProvenanceNone {
		t.Fatalf("Gas.Climate.TodayMeta.Provenance = %q; want none", got)
	}

	store.mu.Lock()
	for key := range store.points {
		delete(store.points, key)
	}
	store.mu.Unlock()
	if snap := store.SnapshotWithContext(february, ""); snap == nil {
		t.Fatal("SnapshotWithContext() with no retained point after accepted points = nil; want projection")
	}
}

func TestEnergyMerge_AllChannelUsagePeriodCombinations(t *testing.T) {
	// Canonical usages after canonicalization: "hot_water", "climate".
	// 3 channels x 2 canonical usages x 5 period variants = 30 canonical keys.
	channels := []string{"gas", "electricity", "solar"}
	usages := []string{"hot_water", "climate"}
	type periodSpec struct {
		period   string
		yearKind string
	}
	periods := []periodSpec{
		{period: "day", yearKind: ""},
		{period: "year", yearKind: "previous"},
		{period: "year", yearKind: "current"},
		{period: "month", yearKind: "current"},
		{period: "month", yearKind: "previous"},
	}

	store := newEnergyMergeStore()
	expectedCount := 0

	for _, ch := range channels {
		for _, us := range usages {
			for _, ps := range periods {
				key := wkey(ch, us, ps.period, ps.yearKind)
				if !store.Apply(key, float64(expectedCount+1), EnergySourceBroadcast, t0) {
					t.Fatalf("Apply(%v) = false; want true for first write", key)
				}
				expectedCount++
			}
		}
	}

	if expectedCount != 30 {
		t.Fatalf("expected 30 combinations; got %d", expectedCount)
	}

	store.mu.RLock()
	gotCount := len(store.points)
	store.mu.RUnlock()

	if gotCount != 30 {
		t.Fatalf("store has %d points; want 30", gotCount)
	}

	totals := store.SnapshotWithContext(t0, "")
	if totals == nil {
		t.Fatal("Snapshot() = nil after 30 inserts")
	}

	checkChannel := func(name string, ch EnergyChannel) {
		t.Helper()
		if ch.DHW.Today == 0 && ch.Climate.Today == 0 &&
			len(ch.DHW.Yearly) == 0 && len(ch.Climate.Yearly) == 0 {
			t.Fatalf("channel %s has no data in snapshot", name)
		}
		if len(ch.DHW.Monthly) < 2 {
			t.Fatalf("channel %s DHW.Monthly len = %d; want >= 2", name, len(ch.DHW.Monthly))
		}
		if len(ch.Climate.Monthly) < 2 {
			t.Fatalf("channel %s Climate.Monthly len = %d; want >= 2", name, len(ch.Climate.Monthly))
		}
	}
	checkChannel("Gas", totals.Gas)
	checkChannel("Electric", totals.Electric)
	checkChannel("Solar", totals.Solar)
}

func TestEnergyMerge_HeatingCoolingCanonicalizedToClimate(t *testing.T) {
	store := newEnergyMergeStore()

	// "heating" and "cooling" both map to "climate" — they share the same merge slot.
	keyHeating := wkey("gas", "heating", "day", "")
	keyCooling := wkey("gas", "cooling", "day", "")

	if !store.Apply(keyHeating, 10.0, EnergySourceBroadcast, t0) {
		t.Fatal("first heating broadcast should be accepted")
	}

	// Cooling with a newer timestamp overwrites the same slot (same canonical key).
	if !store.Apply(keyCooling, 20.0, EnergySourceBroadcast, t1) {
		t.Fatal("cooling broadcast with newer timestamp should overwrite heating (same canonical key)")
	}

	canonKey := wkey("gas", "climate", "day", "")
	store.mu.RLock()
	point := store.points[canonKey]
	count := len(store.points)
	store.mu.RUnlock()

	if count != 1 {
		t.Fatalf("store has %d points; want 1 (heating and cooling share canonical key)", count)
	}
	if point.Value != 20.0 {
		t.Fatalf("value = %f; want 20.0 (latest write)", point.Value)
	}
}

func TestApplyEnergyFromRegister(t *testing.T) {
	provider := newClockedProvider()
	key := provider.EnergyWindowKey("gas", "climate", "day", "")

	if !provider.ApplyEnergyFromRegister(key, 1.25) {
		t.Fatal("ApplyEnergyFromRegister() = false; want true")
	}

	totals := provider.EnergyTotals()
	if totals == nil {
		t.Fatal("energy_totals() = nil; want non-nil")
	}
	if totals.Gas.Climate.Today != 1.25 {
		t.Fatalf("Gas.Climate.Today = %f; want 1.25", totals.Gas.Climate.Today)
	}
}

func TestApplyEnergyFromRegister_OverwritesBroadcast(t *testing.T) {
	provider := newClockedProvider()
	_, updated := provider.ApplyBroadcast(router.BroadcastEvent{
		Values: map[string]types.Value{
			"wh":     {Valid: true, Value: float64(1000)},
			"source": {Valid: true, Value: "gas"},
			"usage":  {Valid: true, Value: "heating"},
			"period": {Valid: true, Value: "day"},
			"year":   {Valid: true, Value: uint16(2026)},
			"month":  {Valid: true, Value: uint8(2)},
			"day":    {Valid: true, Value: uint8(26)},
		},
	})
	if !updated {
		t.Fatal("ApplyBroadcast() updated = false; want true")
	}

	key := provider.EnergyWindowKey("gas", "climate", "day", "")
	if !provider.ApplyEnergyFromRegister(key, 2.5) {
		t.Fatal("ApplyEnergyFromRegister() = false; want true")
	}

	totals := provider.EnergyTotals()
	if totals == nil {
		t.Fatal("energy_totals() = nil; want non-nil")
	}
	if totals.Gas.Climate.Today != 2.5 {
		t.Fatalf("Gas.Climate.Today = %f; want 2.5", totals.Gas.Climate.Today)
	}
}

func TestEnergyMerge_BroadcastMetadataStatesAndStaleness(t *testing.T) {
	store := newEnergyMergeStore()
	key := wkey("gas", "heating", "day", "")
	if !store.Apply(key, 12.5, EnergySourceBroadcast, t0) {
		t.Fatal("Apply() = false; want true")
	}

	warm := store.SnapshotWithContext(t0.Add(time.Minute), "warming_up")
	if warm == nil {
		t.Fatal("SnapshotWithContext() = nil; want non-nil")
	}
	if warm.Gas.Climate.TodayMeta.FreshnessState != EnergyFreshnessStateWarmingUp {
		t.Fatalf("warm state = %q; want warming_up", warm.Gas.Climate.TodayMeta.FreshnessState)
	}
	if warm.Gas.Climate.TodayMeta.Provenance != EnergyProvenanceBroadcast {
		t.Fatalf("warm provenance = %q; want broadcast", warm.Gas.Climate.TodayMeta.Provenance)
	}

	stale := store.SnapshotWithContext(t0.Add(10*time.Minute), "")
	if stale.Gas.Climate.TodayMeta.FreshnessState != EnergyFreshnessStateStale {
		t.Fatalf("stale state = %q; want stale", stale.Gas.Climate.TodayMeta.FreshnessState)
	}
	if stale.Gas.Climate.Today != 12.5 {
		t.Fatalf("stale value = %v; want 12.5 (queryable stale value)", stale.Gas.Climate.Today)
	}
	if !stale.Gas.Climate.TodayMeta.Stale {
		t.Fatal("stale flag = false; want true")
	}

	unavailable := store.SnapshotWithContext(t0.Add(time.Hour), "unavailable")
	if unavailable.Gas.Climate.TodayMeta.FreshnessState != EnergyFreshnessStateUnavailable {
		t.Fatalf("unavailable state = %q; want unavailable", unavailable.Gas.Climate.TodayMeta.FreshnessState)
	}
	if unavailable.Gas.Climate.Today != 12.5 {
		t.Fatalf("unavailable value = %v; want stale value retained", unavailable.Gas.Climate.Today)
	}
}

func TestEnergyMerge_MissingSelectorStatesNeverSeenAndUnavailable(t *testing.T) {
	store := newEnergyMergeStore()
	if !store.Apply(wkey("gas", "climate", "day", ""), 1, EnergySourceBroadcast, t0) {
		t.Fatal("Apply() = false; want true")
	}

	seen := store.SnapshotWithContext(t0.Add(time.Minute), "")
	if got := seen.Electric.DHW.TodayMeta.FreshnessState; got != EnergyFreshnessStateNeverSeen {
		t.Fatalf("missing selector state = %q; want never_seen", got)
	}

	unavailable := store.SnapshotWithContext(t0.Add(time.Minute), "unavailable")
	if got := unavailable.Electric.DHW.TodayMeta.FreshnessState; got != EnergyFreshnessStateUnavailable {
		t.Fatalf("missing selector unavailable state = %q; want unavailable", got)
	}
}

func TestEnergyMerge_BroadcastFreshnessTransitionsMetricIncrements(t *testing.T) {
	before := readExpvarMapValue(energyBroadcastTransitions, "warming_up->stale")

	store := newEnergyMergeStore()
	if !store.Apply(wkey("gas", "climate", "day", ""), 2, EnergySourceBroadcast, t0) {
		t.Fatal("Apply() = false; want true")
	}
	store.RefreshFreshnessMetricsWithContext(t0.Add(time.Minute), "warming_up")
	store.RefreshFreshnessMetricsWithContext(t0.Add(10*time.Minute), "")

	after := readExpvarMapValue(energyBroadcastTransitions, "warming_up->stale")
	if after <= before {
		t.Fatalf("energy_broadcast_freshness_transitions_total[warming_up->stale] = %d; want > %d", after, before)
	}
}

func TestEnergyMerge_SnapshotWithContextDoesNotMutateBroadcastMetrics(t *testing.T) {
	store := newEnergyMergeStore()
	if !store.Apply(wkey("gas", "climate", "day", ""), 2, EnergySourceBroadcast, t0) {
		t.Fatal("Apply() = false; want true")
	}
	store.RefreshFreshnessMetricsWithContext(t0.Add(time.Minute), "warming_up")

	beforeGauge := make(map[EnergyFreshnessState]int64)
	for _, state := range energyFreshnessStates() {
		beforeGauge[state] = readExpvarMapValue(energyBroadcastSelectors, string(state))
	}
	beforeTransition := readExpvarMapValue(energyBroadcastTransitions, "warming_up->stale")

	_ = store.SnapshotWithContext(t0.Add(10*time.Minute), "")

	afterTransition := readExpvarMapValue(energyBroadcastTransitions, "warming_up->stale")
	if afterTransition != beforeTransition {
		t.Fatalf("SnapshotWithContext mutated transitions: warming_up->stale = %d; want %d", afterTransition, beforeTransition)
	}
	for _, state := range energyFreshnessStates() {
		afterGauge := readExpvarMapValue(energyBroadcastSelectors, string(state))
		if afterGauge != beforeGauge[state] {
			t.Fatalf("SnapshotWithContext mutated gauge state=%s: got %d want %d", state, afterGauge, beforeGauge[state])
		}
	}
}

func TestEnergyMerge_BroadcastFreshnessIgnoresRegisterSelectors(t *testing.T) {
	store := newEnergyMergeStore()
	if !store.Apply(wkey("gas", "climate", "day", ""), 7, EnergySourceRegister, t0) {
		t.Fatal("Apply() = false; want true")
	}
	if !store.Apply(wkey("gas", "climate", "year", "previous"), 70, EnergySourceRegister, t0) {
		t.Fatal("Apply() year previous = false; want true")
	}
	if !store.Apply(wkey("gas", "climate", "year", "current"), 71, EnergySourceRegister, t0) {
		t.Fatal("Apply() year current = false; want true")
	}

	beforeFresh := readExpvarMapValue(energyBroadcastSelectors, string(EnergyFreshnessStateFresh))
	beforeNeverSeen := readExpvarMapValue(energyBroadcastSelectors, string(EnergyFreshnessStateNeverSeen))
	beforeRegisterTransition := readExpvarMapValue(energyBroadcastTransitions, "never_seen->fresh")

	store.RefreshFreshnessMetricsWithContext(t0.Add(time.Minute), "")

	afterFresh := readExpvarMapValue(energyBroadcastSelectors, string(EnergyFreshnessStateFresh))
	afterNeverSeen := readExpvarMapValue(energyBroadcastSelectors, string(EnergyFreshnessStateNeverSeen))
	afterRegisterTransition := readExpvarMapValue(energyBroadcastTransitions, "never_seen->fresh")

	if afterFresh != beforeFresh {
		t.Fatalf("energy_broadcast_selectors[fresh] = %d; want unchanged %d for register-only selectors", afterFresh, beforeFresh)
	}
	wantNeverSeenDelta := int64(len(energyBroadcastSelectorCatalog(t0)))
	if delta := afterNeverSeen - beforeNeverSeen; delta != wantNeverSeenDelta {
		t.Fatalf("energy_broadcast_selectors[never_seen] delta = %d; want %d", delta, wantNeverSeenDelta)
	}
	if afterRegisterTransition != beforeRegisterTransition {
		t.Fatalf("energy_broadcast_freshness_transitions_total[never_seen->fresh] = %d; want unchanged %d", afterRegisterTransition, beforeRegisterTransition)
	}
}

func TestEnergyMerge_ConstructorsDoNotMutateBroadcastSelectorMetrics(t *testing.T) {
	before := make(map[EnergyFreshnessState]int64)
	for _, state := range energyFreshnessStates() {
		before[state] = readExpvarMapValue(energyBroadcastSelectors, string(state))
	}

	_ = newEnergyMergeStore()
	_ = newClockedProvider()

	for _, state := range energyFreshnessStates() {
		after := readExpvarMapValue(energyBroadcastSelectors, string(state))
		if after != before[state] {
			t.Fatalf("energy_broadcast_selectors[%s] = %d; want %d (constructor must not mutate process-global gauge)", state, after, before[state])
		}
	}
}

func TestApplyEnergyFromRegister_MetadataFresh(t *testing.T) {
	provider := newClockedProvider()
	if !provider.ApplyEnergyFromRegister(provider.EnergyWindowKey("gas", "climate", "day", ""), 8.75) {
		t.Fatal("ApplyEnergyFromRegister() = false; want true")
	}

	totals := provider.EnergyTotals()
	if totals == nil {
		t.Fatal("energy_totals() = nil; want non-nil")
	}
	if totals.Gas.Climate.TodayMeta.FreshnessState != EnergyFreshnessStateFresh {
		t.Fatalf("register freshness = %q; want fresh", totals.Gas.Climate.TodayMeta.FreshnessState)
	}
	if totals.Gas.Climate.TodayMeta.Provenance != EnergyProvenanceRegister {
		t.Fatalf("register provenance = %q; want register", totals.Gas.Climate.TodayMeta.Provenance)
	}
	if totals.Gas.Climate.TodayMeta.Stale {
		t.Fatal("register stale flag = true; want false")
	}
}

func TestEnergyMerge_RegisterFreshnessAgesOut(t *testing.T) {
	store := newEnergyMergeStore()
	key := wkey("gas", "climate", "day", "")
	if !store.Apply(key, 8.75, EnergySourceRegister, t0) {
		t.Fatal("Apply() = false; want true")
	}

	fresh := store.SnapshotWithContext(t0.Add(time.Minute), "")
	if fresh == nil {
		t.Fatal("SnapshotWithContext() fresh = nil; want non-nil")
	}
	if got := fresh.Gas.Climate.TodayMeta.FreshnessState; got != EnergyFreshnessStateFresh {
		t.Fatalf("fresh register freshness = %q; want fresh", got)
	}

	staleAt := t0.Add(energyBroadcastUnavailableTTL + time.Second)
	stale := store.SnapshotWithContext(staleAt, "")
	if stale == nil {
		t.Fatal("SnapshotWithContext() stale = nil; want non-nil")
	}
	if got := stale.Gas.Climate.TodayMeta.FreshnessState; got != EnergyFreshnessStateStale {
		t.Fatalf("aged register freshness = %q; want stale", got)
	}
	if got := stale.Gas.Climate.TodayMeta.Provenance; got != EnergyProvenanceRegister {
		t.Fatalf("aged register provenance = %q; want register", got)
	}
	if !stale.Gas.Climate.TodayMeta.Stale {
		t.Fatal("aged register stale flag = false; want true")
	}

	staleUnavailableCtx := store.SnapshotWithContext(staleAt, "unavailable")
	if staleUnavailableCtx == nil {
		t.Fatal("SnapshotWithContext() stale unavailable = nil; want non-nil")
	}
	if got := staleUnavailableCtx.Gas.Climate.TodayMeta.FreshnessState; got != EnergyFreshnessStateStale {
		t.Fatalf("aged register freshness with passive unavailable = %q; want stale", got)
	}
}

// The provider's energy plane is null until its merge store accepts a point,
// then carries the per-slot 0/none projection, also after pruning.
func TestLiveSemanticProvider_EnergyTotalsNullUntilFirstAcceptedPoint(t *testing.T) {
	clock := time.Date(2026, time.November, 15, 12, 0, 0, 0, time.UTC)
	provider := NewLiveSemanticProvider()
	provider.SetEnergyClock(func() time.Time { return clock })

	if totals := provider.EnergyTotals(); totals != nil {
		t.Fatalf("energy_totals() before any point = %+v; want nil", totals)
	}
	provider.SetEnergyPassiveState("unavailable")
	if totals := provider.EnergyTotals(); totals != nil {
		t.Fatalf("energy_totals() before any point with passive unavailable = %+v; want nil", totals)
	}
	provider.SetEnergyPassiveState("")

	november := provider.EnergyWindowKey("gas", "climate", "month", "current")
	if !provider.ApplyEnergyFromRegister(november, 40) {
		t.Fatal("ApplyEnergyFromRegister(November month) rejected; want accepted")
	}
	totals := provider.EnergyTotals()
	if totals == nil {
		t.Fatal("energy_totals() after the first accepted point = nil; want projection")
	}
	if got := totals.Gas.Climate.Monthly; !reflect.DeepEqual(got, []float64{0, 40}) {
		t.Fatalf("Gas.Climate.Monthly = %v; want [0 40]", got)
	}
	if got := totals.Gas.DHW.TodayMeta.FreshnessState; got != EnergyFreshnessStateNeverSeen {
		t.Fatalf("Gas.DHW.TodayMeta.FreshnessState = %q; want never_seen", got)
	}
	if got := totals.Gas.DHW.TodayMeta.Provenance; got != EnergyProvenanceNone {
		t.Fatalf("Gas.DHW.TodayMeta.Provenance = %q; want none", got)
	}

	provider.SetEnergyPassiveState("unavailable")
	unavailable := provider.EnergyTotals()
	if unavailable == nil {
		t.Fatal("energy_totals() with passive unavailable after the first point = nil; want projection")
	}
	if got := unavailable.Gas.DHW.TodayMeta.FreshnessState; got != EnergyFreshnessStateUnavailable {
		t.Fatalf("Gas.DHW.TodayMeta.FreshnessState = %q; want unavailable", got)
	}
	provider.SetEnergyPassiveState("")

	// Two months later a write prunes the November window, which has left the
	// projection; the plane keeps the 0/none projection.
	clock = time.Date(2027, time.January, 15, 12, 0, 0, 0, time.UTC)
	if !provider.ApplyEnergyFromRegister(provider.EnergyWindowKey("gas", "climate", "month", "current"), 30) {
		t.Fatal("ApplyEnergyFromRegister(January month) rejected; want accepted")
	}
	clock = time.Date(2027, time.March, 15, 12, 0, 0, 0, time.UTC)
	pruned := provider.EnergyTotals()
	if pruned == nil {
		t.Fatal("energy_totals() after pruning = nil; want projection")
	}
	if got := pruned.Gas.Climate.Monthly; len(got) != 0 {
		t.Fatalf("Gas.Climate.Monthly in March = %v; want empty, no retained month projects", got)
	}
	if got := pruned.Gas.Climate.MonthlyMeta; len(got) != 2 || got[0].Provenance != EnergyProvenanceNone || got[1].Provenance != EnergyProvenanceNone {
		t.Fatalf("Gas.Climate.MonthlyMeta in March = %+v; want two none slots", got)
	}
	if got := pruned.Gas.Climate.TodayMeta.Provenance; got != EnergyProvenanceNone {
		t.Fatalf("Gas.Climate.TodayMeta.Provenance in March = %q; want none", got)
	}
}

func readExpvarMapValue(value *expvar.Map, key string) int64 {
	if value == nil {
		return 0
	}
	var out int64
	value.Do(func(kv expvar.KeyValue) {
		if kv.Key != key {
			return
		}
		if parsed, ok := kv.Value.(*expvar.Int); ok {
			out = parsed.Value()
		}
	})
	return out
}

// Exercise the same store across two month boundaries and New Year.
func TestEnergyMerge_ObsoleteWindowsRollover(t *testing.T) {
	store := newEnergyMergeStore()
	for _, ref := range []time.Time{
		time.Date(2026, 11, 30, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 12, 31, 12, 0, 0, 0, time.UTC),
		time.Date(2027, 1, 31, 12, 0, 0, 0, time.UTC),
	} {
		for _, channel := range []string{"gas", "electricity", "solar"} {
			for _, usage := range []string{"hot_water", "climate"} {
				for _, period := range []string{"month", "year"} {
					for i, label := range []string{"previous", "current"} {
						key := energyWindowKey(channel, usage, period, label, ref)
						source := EnergySourceBroadcast
						if period == "month" {
							source = EnergySourceRegister
						}
						if !store.Apply(key, float64(i+10), source, ref) {
							t.Fatalf("Apply(%+v) rejected", key)
						}
					}
				}
			}
		}
		if got := len(store.points); got != 24 {
			t.Fatalf("at %v: retained %d points, want 24", ref, got)
		}
		for key := range store.points {
			if _, ok := energySlotFor(key, ref); !ok {
				t.Fatalf("obsolete point retained at %v: %+v", ref, key)
			}
		}
		snapshot := store.SnapshotWithContext(ref, "")
		for _, channel := range []EnergyChannel{snapshot.Gas, snapshot.Electric, snapshot.Solar} {
			for _, series := range []EnergySeries{channel.DHW, channel.Climate} {
				if !reflect.DeepEqual(series.Monthly, []float64{10, 11}) || !reflect.DeepEqual(series.Yearly, []float64{10, 11}) {
					t.Fatalf("projection changed at %v: %+v", ref, series)
				}
			}
		}
		revision := store.Revision()
		for range 3 {
			if got := store.SnapshotWithContext(ref, ""); !reflect.DeepEqual(got, snapshot) {
				t.Fatal("repeated snapshot changed projection")
			}
		}
		if store.Revision() != revision || len(store.points) != 24 {
			t.Fatal("snapshot mutated retained state")
		}
	}
}

func TestEnergyMerge_ObsoleteWindowsLateArrivalsAndPrecedence(t *testing.T) {
	ref := time.Date(2027, 1, 15, 12, 0, 0, 0, time.UTC)
	for _, period := range []string{"month", "year"} {
		t.Run(period, func(t *testing.T) {
			store := newEnergyMergeStore()
			current := energyWindowKey("gas", "heating", period, "current", ref)
			previous := energyWindowKey("gas", "climate", period, "previous", ref)
			if !store.Apply(current, 20, EnergySourceRegister, ref) || !store.Apply(previous, 10, EnergySourceBroadcast, ref) {
				t.Fatal("current and late previous windows must be accepted independently")
			}
			if !store.Apply(previous, 12, EnergySourceBroadcast, ref.Add(time.Second)) {
				t.Fatal("late previous update must update its own key")
			}
			if !store.Apply(previous, 15, EnergySourceRegister, ref.Add(-time.Second)) {
				t.Fatal("older register must override previous broadcast")
			}
			if store.Apply(previous, 99, EnergySourceBroadcast, ref.Add(2*time.Second)) {
				t.Fatal("broadcast must not override retained register")
			}
			obsolete := previous
			if period == "month" {
				obsolete.Month-- // November of the preceding year.
			} else {
				obsolete.Year--
			}
			revision := store.Revision()
			for _, source := range []EnergyDataSource{EnergySourceBroadcast, EnergySourceRegister} {
				if store.Apply(obsolete, 99, source, ref.Add(3*time.Second)) {
					t.Fatal("obsolete late arrival must not resurrect a pruned window")
				}
			}
			current.Usage = "climate"
			if store.Revision() != revision || len(store.points) != 2 || store.points[current].Value != 20 || store.points[previous].Value != 15 {
				t.Fatal("late arrival changed retained windows or revision")
			}
		})
	}
}

func TestEnergyMerge_ObsoleteWindowsClockRollback(t *testing.T) {
	future := time.Date(2027, 1, 15, 12, 0, 0, 0, time.UTC)
	rollback := time.Date(2026, 12, 15, 12, 0, 0, 0, time.UTC)
	for _, period := range []string{"month", "year"} {
		t.Run(period, func(t *testing.T) {
			store := newEnergyMergeStore()
			futureKey := energyWindowKey("gas", "climate", period, "current", future)
			if !store.Apply(futureKey, 20, EnergySourceRegister, future) {
				t.Fatal("initial write rejected")
			}
			for i, label := range []string{"previous", "current"} {
				key := energyWindowKey("gas", "climate", period, label, rollback)
				if !store.Apply(key, float64(i+10), EnergySourceBroadcast, rollback) {
					t.Fatal("rollback window rejected")
				}
			}
			if len(store.points) != 3 || store.points[futureKey].Value != 20 {
				t.Fatal("rollback deleted a future projectable point")
			}
			before := store.SnapshotWithContext(rollback, "")
			after := store.SnapshotWithContext(future, "")
			if period == "month" {
				if !reflect.DeepEqual(before.Gas.Climate.Monthly, []float64{10, 11}) || !reflect.DeepEqual(after.Gas.Climate.Monthly, []float64{11, 20}) {
					t.Fatal("rollback/catch-up month projection lost retained data")
				}
			} else if !reflect.DeepEqual(before.Gas.Climate.Yearly, []float64{10, 11}) || !reflect.DeepEqual(after.Gas.Climate.Yearly, []float64{11, 20}) {
				t.Fatal("rollback/catch-up year projection lost retained data")
			}
			// A rejected source downgrade must not prune unrelated stored history.
			if store.Apply(futureKey, 99, EnergySourceBroadcast, future.Add(time.Second)) || len(store.points) != 3 {
				t.Fatal("rejected write changed retention")
			}
			if !store.Apply(futureKey, 21, EnergySourceRegister, future.Add(time.Second)) || len(store.points) != 2 {
				t.Fatal("accepted catch-up write must prune the obsolete rollback window")
			}
		})
	}
}

func TestEnergyMerge_ObsoleteWindowsSelectorIsolationAndDayBehavior(t *testing.T) {
	for _, period := range []string{"month", "year"} {
		t.Run(period, func(t *testing.T) {
			otherPeriod := "year"
			if period == "year" {
				otherPeriod = "month"
			}
			oldRef := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
			ref := oldRef.AddDate(1, 2, 0)
			store := newEnergyMergeStore()
			var preserved []energyMergeKey
			for _, key := range []energyMergeKey{
				energyWindowKey("electricity", "climate", period, "current", oldRef),
				energyWindowKey("gas", "hot_water", period, "current", oldRef),
				energyWindowKey("gas", "climate", otherPeriod, "previous", oldRef),
				energyWindowKey("gas", "climate", "day", "", oldRef),
				energyLifetimeKey("gas", "climate"),
			} {
				if !store.Apply(key, 7, EnergySourceRegister, oldRef) {
					t.Fatal("setup write rejected")
				}
				preserved = append(preserved, key)
			}
			obsolete := energyWindowKey("gas", "climate", period, "current", oldRef)
			store.Apply(obsolete, 8, EnergySourceRegister, oldRef)
			current := energyWindowKey("gas", "heating", period, "current", ref)
			if !store.Apply(current, 9, EnergySourceRegister, ref) {
				t.Fatal("rollover write rejected")
			}
			if _, exists := store.points[obsolete]; exists {
				t.Fatal("canonicalized same-selector obsolete window retained")
			}
			for _, key := range preserved {
				if store.points[key].Value != 7 {
					t.Fatalf("other selector/day/lifetime changed: %+v", key)
				}
			}
			newDay := energyWindowKey("gas", "climate", "day", "", ref)
			if !store.Apply(newDay, 10, EnergySourceRegister, ref) {
				t.Fatal("new day rejected")
			}
			oldDay := energyWindowKey("gas", "climate", "day", "", oldRef)
			if _, exists := store.points[oldDay]; exists {
				t.Fatal("existing older-day pruning regressed")
			}
			if got := store.SnapshotWithContext(ref, "").Gas.Climate.Today; got != 10 {
				t.Fatalf("today = %v, want 10", got)
			}
		})
	}
}
