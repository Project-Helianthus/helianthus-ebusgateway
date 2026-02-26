package graphql

import (
	"testing"
	"time"
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

	store.mu.RLock()
	point := store.points[key]
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

	store.mu.RLock()
	point := store.points[key]
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

	store.mu.RLock()
	point := store.points[key]
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

	store.mu.RLock()
	point := store.points[key]
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

	store.mu.RLock()
	point := store.points[key]
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

func TestEnergyMerge_SnapshotReturnsNilWhenEmpty(t *testing.T) {
	store := newEnergyMergeStore()
	if snap := store.Snapshot(); snap != nil {
		t.Fatalf("Snapshot() = %v; want nil for empty store", snap)
	}
}

func TestEnergyMerge_AllChannelUsagePeriodCombinations(t *testing.T) {
	channels := []string{"gas", "electricity", "solar"}
	usages := []string{"hot_water", "heating"}
	type periodSpec struct {
		period   string
		yearKind string
	}
	periods := []periodSpec{
		{period: "day", yearKind: ""},
		{period: "year", yearKind: "previous"},
		{period: "year", yearKind: "current"},
	}

	// 3 channels x 2 usages x 3 periods = 18 combinations.
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

	if expectedCount != 18 {
		t.Fatalf("expected 18 combinations; got %d", expectedCount)
	}

	store.mu.RLock()
	gotCount := len(store.points)
	store.mu.RUnlock()

	if gotCount != 18 {
		t.Fatalf("store has %d points; want 18", gotCount)
	}

	// Verify snapshot produces non-nil totals with data in all channels.
	totals := store.Snapshot()
	if totals == nil {
		t.Fatal("Snapshot() = nil after 18 inserts")
	}

	// Verify each channel has non-zero data.
	checkChannel := func(name string, ch EnergyChannel) {
		t.Helper()
		if ch.DHW.Today == 0 && ch.Climate.Today == 0 &&
			len(ch.DHW.Yearly) == 0 && len(ch.Climate.Yearly) == 0 {
			t.Fatalf("channel %s has no data in snapshot", name)
		}
	}
	checkChannel("Gas", totals.Gas)
	checkChannel("Electric", totals.Electric)
	checkChannel("Solar", totals.Solar)
}
