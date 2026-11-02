package graphql

import (
	"reflect"
	"testing"
	"time"
	_ "time/tzdata" // Keep the named-zone fixture independent of host zone files.
)

// These expectations characterize the current process-reference contract:
// SetEnergyClock supplies the calendar used by EnergyTotals on every read.
// See semantic_live.go (SetEnergyClock/EnergyTotals) and energy_merge.go
// (absolute window identity), plus the parent issue's energy-merge companion.
// Equal instants can therefore select different windows. This is evidence of
// the unresolved policy gap in #1022, not a choice of the controller calendar.
// Register inputs below are synthetic store seeds, not native acquisition.
func TestEnergyCalendarCharacterization(t *testing.T) {
	bucharest, err := time.LoadLocation("Europe/Bucharest")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name              string
		utc               time.Time
		bucharestDate     string
		previousMonthYear int
		previousMonth     int
		wantToday         float64
		wantMonthly       []float64
		wantYearly        []float64
	}{
		{
			name:          "ordinary winter same calendar",
			utc:           time.Date(2030, time.January, 15, 12, 0, 0, 0, time.UTC),
			bucharestDate: "2030-01-15", previousMonthYear: 2029, previousMonth: 12,
			wantToday: 2, wantMonthly: []float64{7, 11}, wantYearly: []float64{71, 101},
		},
		{
			name:          "ordinary summer same calendar",
			utc:           time.Date(2030, time.July, 15, 12, 0, 0, 0, time.UTC),
			bucharestDate: "2030-07-15", previousMonthYear: 2030, previousMonth: 6,
			wantToday: 2, wantMonthly: []float64{7, 11}, wantYearly: []float64{71, 101},
		},
		{
			name:          "midnight different day within month",
			utc:           time.Date(2030, time.July, 15, 21, 30, 0, 0, time.UTC),
			bucharestDate: "2030-07-16", previousMonthYear: 2030, previousMonth: 6,
			wantToday: 0, wantMonthly: []float64{7, 11}, wantYearly: []float64{71, 101},
		},
		{
			name:          "month rollover before UTC midnight",
			utc:           time.Date(2030, time.April, 30, 21, 30, 0, 0, time.UTC),
			bucharestDate: "2030-05-01", previousMonthYear: 2030, previousMonth: 3,
			wantToday: 0, wantMonthly: []float64{11, 0}, wantYearly: []float64{71, 101},
		},
		{
			name:          "year rollover before UTC midnight",
			utc:           time.Date(2030, time.December, 31, 22, 30, 0, 0, time.UTC),
			bucharestDate: "2031-01-01", previousMonthYear: 2030, previousMonth: 11,
			wantToday: 0, wantMonthly: []float64{11, 0}, wantYearly: []float64{101, 0},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			local := tc.utc.In(bucharest)
			if !local.Equal(tc.utc) || local.Format("2006-01-02") != tc.bucharestDate {
				t.Fatalf("named-zone fixture = %v; want equal instant on %s", local, tc.bucharestDate)
			}
			ref := tc.utc
			provider := NewLiveSemanticProvider()
			provider.SetEnergyClock(func() time.Time { return ref })
			// Seed absolute identities directly so expected projection is not
			// derived from the relative-window helper being characterized.
			seeds := []struct {
				key   EnergyMergeKey
				value float64
			}{
				{EnergyMergeKey{Channel: "gas", Usage: "climate", Period: "day", Year: 2030, Month: int(tc.utc.Month()), Day: tc.utc.Day()}, 2},
				{EnergyMergeKey{Channel: "gas", Usage: "climate", Period: "month", Year: tc.previousMonthYear, Month: tc.previousMonth}, 7},
				{EnergyMergeKey{Channel: "gas", Usage: "climate", Period: "month", Year: 2030, Month: int(tc.utc.Month())}, 11},
				{EnergyMergeKey{Channel: "gas", Usage: "climate", Period: "year", Year: 2029}, 71},
				{EnergyMergeKey{Channel: "gas", Usage: "climate", Period: "year", Year: 2030}, 101},
			}
			for _, seed := range seeds {
				if !provider.ApplyEnergyFromRegister(seed.key, seed.value) {
					t.Fatalf("synthetic store input rejected: %+v", seed.key)
				}
			}
			check := func(wantToday float64, wantMonthly, wantYearly []float64) {
				t.Helper()
				totals := provider.EnergyTotals()
				if totals == nil {
					t.Fatal("missing energy totals")
				}
				got := totals.Gas.Climate
				if len(got.MonthlyMeta) != 2 || len(got.YearlyMeta) != 2 {
					t.Fatalf("metadata slots = %d monthly / %d yearly; want two each", len(got.MonthlyMeta), len(got.YearlyMeta))
				}
				if got.Today != wantToday || !reflect.DeepEqual(got.Monthly, wantMonthly) || !reflect.DeepEqual(got.Yearly, wantYearly) {
					t.Fatalf("reference %v: today/monthly/yearly = %v/%v/%v; want %v/%v/%v", ref, got.Today, got.Monthly, got.Yearly, wantToday, wantMonthly, wantYearly)
				}
				checkMeta := func(meta EnergyPointMeta, value float64) {
					t.Helper()
					want := EnergyProvenanceRegister
					if value == 0 {
						want = EnergyProvenanceNone
					}
					if meta.Provenance != want {
						t.Fatalf("reference %v: provenance = %s; want %s", ref, meta.Provenance, want)
					}
				}
				checkMeta(got.TodayMeta, wantToday)
				for i := range wantMonthly {
					checkMeta(got.MonthlyMeta[i], wantMonthly[i])
					checkMeta(got.YearlyMeta[i], wantYearly[i])
				}
			}
			check(2, []float64{7, 11}, []float64{71, 101})
			before := provider.EnergyTotals()
			// Only the representation of the reference instant changes; no
			// new input arrives between the UTC and Bucharest projections.
			ref = local
			check(tc.wantToday, tc.wantMonthly, tc.wantYearly)
			ref = tc.utc
			check(2, []float64{7, 11}, []float64{71, 101})
			if !reflect.DeepEqual(provider.EnergyTotals(), before) {
				t.Fatal("returning to the UTC reference did not restore the original snapshot")
			}
		})
	}
}
