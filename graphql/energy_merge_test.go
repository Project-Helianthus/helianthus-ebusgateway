package graphql

import (
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
	key := energyMergeKey{Channel: "gas", Usage: "heating", Period: "day"}

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
	key := energyMergeKey{Channel: "gas", Usage: "heating", Period: "day"}

	store.Apply(key, 1.0, EnergySourceBroadcast, t0)

	if !store.Apply(key, 2.0, EnergySourceRegister, t1) {
		t.Fatal("register should overwrite broadcast")
	}

	// Key is canonicalized: "heating" → "climate"
	canonKey := energyMergeKey{Channel: "gas", Usage: "climate", Period: "day"}
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
	key := energyMergeKey{Channel: "gas", Usage: "heating", Period: "day"}

	store.Apply(key, 1.0, EnergySourceRegister, t0)

	// Broadcast with newer timestamp should still be rejected.
	if store.Apply(key, 99.0, EnergySourceBroadcast, t2) {
		t.Fatal("broadcast should never overwrite register")
	}

	canonKey := energyMergeKey{Channel: "gas", Usage: "climate", Period: "day"}
	store.mu.RLock()
	point := store.points[canonKey]
	store.mu.RUnlock()

	if point.Value != 1.0 {
		t.Fatalf("value = %f; want 1.0 (original register)", point.Value)
	}
}

func TestEnergyMerge_MonotonicBroadcast(t *testing.T) {
	store := newEnergyMergeStore()
	key := energyMergeKey{Channel: "electricity", Usage: "hot_water", Period: "year", YearKind: "current"}

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

	canonKey := energyMergeKey{Channel: "electricity", Usage: "hot_water", Period: "year", YearKind: "current"}
	store.mu.RLock()
	point := store.points[canonKey]
	store.mu.RUnlock()

	if point.Value != 20.0 {
		t.Fatalf("value = %f; want 20.0", point.Value)
	}
}

func TestEnergyMerge_MonotonicRegister(t *testing.T) {
	store := newEnergyMergeStore()
	key := energyMergeKey{Channel: "solar", Usage: "heating", Period: "day"}

	store.Apply(key, 100.0, EnergySourceRegister, t1)

	// Newer register overwrites.
	if !store.Apply(key, 200.0, EnergySourceRegister, t2) {
		t.Fatal("newer register should overwrite older register")
	}

	// Older register rejected.
	if store.Apply(key, 300.0, EnergySourceRegister, t0) {
		t.Fatal("older register should be rejected (monotonic)")
	}

	canonKey := energyMergeKey{Channel: "solar", Usage: "climate", Period: "day"}
	store.mu.RLock()
	point := store.points[canonKey]
	store.mu.RUnlock()

	if point.Value != 200.0 {
		t.Fatalf("value = %f; want 200.0", point.Value)
	}
}

func TestEnergyMerge_RegisterOverwritesBroadcastRegardlessOfTime(t *testing.T) {
	store := newEnergyMergeStore()
	key := energyMergeKey{Channel: "gas", Usage: "hot_water", Period: "year", YearKind: "previous"}

	// Broadcast at t2 (later timestamp).
	store.Apply(key, 5.0, EnergySourceBroadcast, t2)

	// Register at t0 (earlier timestamp) should still win.
	if !store.Apply(key, 10.0, EnergySourceRegister, t0) {
		t.Fatal("register should overwrite broadcast even with older timestamp")
	}

	canonKey := energyMergeKey{Channel: "gas", Usage: "hot_water", Period: "year", YearKind: "previous"}
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
	store.Apply(energyMergeKey{Channel: "gas", Usage: "heating", Period: "day"}, 1.5, EnergySourceBroadcast, t0)

	// Gas hot_water year current.
	store.Apply(energyMergeKey{Channel: "gas", Usage: "hot_water", Period: "year", YearKind: "current"}, 100.0, EnergySourceBroadcast, t0)

	// Electricity hot_water year previous.
	store.Apply(energyMergeKey{Channel: "electricity", Usage: "hot_water", Period: "year", YearKind: "previous"}, 200.0, EnergySourceRegister, t0)

	// Solar heating day.
	store.Apply(energyMergeKey{Channel: "solar", Usage: "heating", Period: "day"}, 50.0, EnergySourceBroadcast, t0)

	totals := store.Snapshot()
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
	store.Apply(energyMergeKey{Channel: "gas", Usage: "climate", Period: "month", YearKind: "current"}, 284.0, EnergySourceRegister, t0)
	store.Apply(energyMergeKey{Channel: "gas", Usage: "climate", Period: "month", YearKind: "previous"}, 266.0, EnergySourceRegister, t0)

	// Electricity hot_water: this month = 5, last month = 3.
	store.Apply(energyMergeKey{Channel: "electricity", Usage: "hot_water", Period: "month", YearKind: "current"}, 5.0, EnergySourceRegister, t0)
	store.Apply(energyMergeKey{Channel: "electricity", Usage: "hot_water", Period: "month", YearKind: "previous"}, 3.0, EnergySourceRegister, t0)

	totals := store.Snapshot()
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

func TestEnergyMerge_SnapshotReturnsNilWhenEmpty(t *testing.T) {
	store := newEnergyMergeStore()
	if snap := store.Snapshot(); snap != nil {
		t.Fatalf("Snapshot() = %v; want nil for empty store", snap)
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
				key := energyMergeKey{
					Channel:  ch,
					Usage:    us,
					Period:   ps.period,
					YearKind: ps.yearKind,
				}
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

	totals := store.Snapshot()
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
	keyHeating := energyMergeKey{Channel: "gas", Usage: "heating", Period: "day"}
	keyCooling := energyMergeKey{Channel: "gas", Usage: "cooling", Period: "day"}

	if !store.Apply(keyHeating, 10.0, EnergySourceBroadcast, t0) {
		t.Fatal("first heating broadcast should be accepted")
	}

	// Cooling with a newer timestamp overwrites the same slot (same canonical key).
	if !store.Apply(keyCooling, 20.0, EnergySourceBroadcast, t1) {
		t.Fatal("cooling broadcast with newer timestamp should overwrite heating (same canonical key)")
	}

	canonKey := energyMergeKey{Channel: "gas", Usage: "climate", Period: "day"}
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
	provider := NewLiveSemanticProvider()
	key := EnergyMergeKey{
		Channel: "gas",
		Usage:   "climate",
		Period:  "day",
	}

	if !provider.ApplyEnergyFromRegister(key, 1.25) {
		t.Fatal("ApplyEnergyFromRegister() = false; want true")
	}

	totals := provider.EnergyTotals()
	if totals == nil {
		t.Fatal("EnergyTotals() = nil; want non-nil")
	}
	if totals.Gas.Climate.Today != 1.25 {
		t.Fatalf("Gas.Climate.Today = %f; want 1.25", totals.Gas.Climate.Today)
	}
}

func TestApplyEnergyFromRegister_OverwritesBroadcast(t *testing.T) {
	provider := NewLiveSemanticProvider()
	_, updated := provider.ApplyBroadcast(router.BroadcastEvent{
		Values: map[string]types.Value{
			"wh":     {Valid: true, Value: float64(1000)},
			"source": {Valid: true, Value: "gas"},
			"usage":  {Valid: true, Value: "heating"},
			"period": {Valid: true, Value: "day"},
		},
	})
	if !updated {
		t.Fatal("ApplyBroadcast() updated = false; want true")
	}

	key := EnergyMergeKey{
		Channel: "gas",
		Usage:   "climate",
		Period:  "day",
	}
	if !provider.ApplyEnergyFromRegister(key, 2.5) {
		t.Fatal("ApplyEnergyFromRegister() = false; want true")
	}

	totals := provider.EnergyTotals()
	if totals == nil {
		t.Fatal("EnergyTotals() = nil; want non-nil")
	}
	if totals.Gas.Climate.Today != 2.5 {
		t.Fatalf("Gas.Climate.Today = %f; want 2.5", totals.Gas.Climate.Today)
	}
}
