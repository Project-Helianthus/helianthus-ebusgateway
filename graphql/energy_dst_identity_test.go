package graphql

import (
	"reflect"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

// Characterize the injected reference calendar; choosing a production calendar
// remains outside this test. Inputs are synthetic decoded values and store seeds.
func TestEnergyDSTIdentity(t *testing.T) {
	bucharest, err := time.LoadLocation("Europe/Bucharest")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                    string
		month, day              int
		before, after           time.Time
		localBefore, localAfter string
	}{
		{
			name: "spring skipped hour boundary", month: 3, day: 29,
			before:      time.Date(2026, 3, 29, 0, 59, 59, 0, time.UTC),
			after:       time.Date(2026, 3, 29, 1, 0, 0, 0, time.UTC),
			localBefore: "2026-03-29 02:59:59 +0200", localAfter: "2026-03-29 04:00:00 +0300",
		},
		{
			name: "autumn repeated hour distinct instants", month: 10, day: 25,
			before:      time.Date(2026, 10, 25, 0, 30, 0, 0, time.UTC),
			after:       time.Date(2026, 10, 25, 1, 30, 0, 0, time.UTC),
			localBefore: "2026-10-25 03:30:00 +0300", localAfter: "2026-10-25 03:30:00 +0200",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.after.After(tc.before) {
				t.Fatal("fixture must contain distinct, increasing UTC instants")
			}
			for i, instant := range []time.Time{tc.before, tc.after} {
				want := []string{tc.localBefore, tc.localAfter}[i]
				if got := instant.In(bucharest).Format("2006-01-02 15:04:05 -0700"); got != want {
					t.Fatalf("local fixture = %s; want %s", got, want)
				}
			}
			clock := tc.before.In(bucharest)
			provider := NewLiveSemanticProvider()
			provider.SetEnergyClock(func() time.Time { return clock })
			dayKey := EnergyMergeKey{Channel: "gas", Usage: "climate", Period: "day", Year: 2026, Month: tc.month, Day: tc.day}
			seeds := []struct {
				key   EnergyMergeKey
				value float64
			}{
				{EnergyMergeKey{Channel: "gas", Usage: "climate", Period: "month", Year: 2026, Month: tc.month - 1}, 7},
				{EnergyMergeKey{Channel: "gas", Usage: "climate", Period: "month", Year: 2026, Month: tc.month}, 11},
				{EnergyMergeKey{Channel: "gas", Usage: "climate", Period: "year", Year: 2025}, 71},
				{EnergyMergeKey{Channel: "gas", Usage: "climate", Period: "year", Year: 2026}, 101},
			}
			for _, seed := range seeds {
				if !provider.ApplyEnergyFromRegister(seed.key, seed.value) {
					t.Fatalf("register seed rejected: %+v", seed.key)
				}
			}
			broadcast := func(wh float64) bool {
				_, accepted := provider.ApplyBroadcast(router.BroadcastEvent{Values: map[string]types.Value{
					"source": {Valid: true, Value: "gas"},
					"usage":  {Valid: true, Value: "heating"},
					"period": {Valid: true, Value: "day"},
					"year":   {Valid: true, Value: uint16(2026)},
					"month":  {Valid: true, Value: uint8(tc.month)},
					"day":    {Valid: true, Value: uint8(tc.day)},
					"wh":     {Valid: true, Value: wh},
				}})
				return accepted
			}
			check := func(want float64, provenance EnergyProvenance, observed time.Time) {
				t.Helper()
				if key := provider.EnergyWindowKey("gas", "climate", "day", ""); key != dayKey {
					t.Fatalf("daily identity = %+v; want %+v", key, dayKey)
				}
				totals := provider.EnergyTotals()
				if totals == nil {
					t.Fatal("missing energy totals")
				}
				got := totals.Gas.Climate
				if got.Today != want || got.TodayMeta.Provenance != provenance || got.TodayMeta.LastObservedUTC != observed.UTC().Format(time.RFC3339) {
					t.Fatalf("daily projection = %v / %+v; want %v / %s observed at %v", got.Today, got.TodayMeta, want, provenance, observed)
				}
				if !reflect.DeepEqual(got.Monthly, []float64{7, 11}) || !reflect.DeepEqual(got.Yearly, []float64{71, 101}) {
					t.Fatalf("month/year projection = %v / %v; want [7 11] / [71 101]", got.Monthly, got.Yearly)
				}
				if len(got.MonthlyMeta) != 2 || len(got.YearlyMeta) != 2 {
					t.Fatal("expected two month/year metadata slots")
				}
				for _, meta := range append(got.MonthlyMeta, got.YearlyMeta...) {
					if meta.Provenance != EnergyProvenanceRegister {
						t.Fatalf("month/year provenance = %s; want register", meta.Provenance)
					}
				}
				store := provider.energyMerge
				store.mu.RLock()
				defer store.mu.RUnlock()
				if len(store.points) != 5 {
					t.Fatalf("stored buckets = %d; want one day plus four month/year buckets", len(store.points))
				}
				if point, ok := store.points[dayKey]; !ok || point.Value != want {
					t.Fatalf("absolute day bucket = %+v, present=%v; want %v", point, ok, want)
				}
				for _, seed := range seeds {
					if point, ok := store.points[seed.key]; !ok || point.Value != seed.value || point.Source != EnergySourceRegister {
						t.Fatalf("absolute month/year bucket %+v = %+v, present=%v", seed.key, point, ok)
					}
				}
			}
			if !broadcast(5000) {
				t.Fatal("initial same-day broadcast rejected")
			}
			check(5, EnergyProvenanceBroadcast, tc.before)
			clock = tc.after.In(bucharest)
			// Crossing the offset change alone keeps the value and observation.
			check(5, EnergyProvenanceBroadcast, tc.before)
			if !broadcast(6000) {
				t.Fatal("later same-day broadcast rejected across offset change")
			}
			check(6, EnergyProvenanceBroadcast, tc.after)
			if !provider.ApplyEnergyFromRegister(provider.EnergyWindowKey("gas", "climate", "day", ""), 9) {
				t.Fatal("register did not replace same-day broadcast")
			}
			check(9, EnergyProvenanceRegister, tc.after)
			clock = clock.Add(time.Second)
			if broadcast(7000) {
				t.Fatal("newer broadcast displaced register across DST transition")
			}
			check(9, EnergyProvenanceRegister, tc.after)
		})
	}
}
