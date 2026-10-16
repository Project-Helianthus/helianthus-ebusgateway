package main

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

// These synthetic replies characterize assignment by the process calendar;
// they do not select a calendar policy or qualify native controller behavior.
func TestEnergyAcquisitionCalendar(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		start, end time.Time
		changeRead int
		failRead   int
		wantReads  int
		accepted   int
	}{
		{
			name:       "controller month rolls over before process month",
			start:      time.Date(2030, time.April, 30, 21, 59, 59, 0, time.UTC),
			end:        time.Date(2030, time.April, 30, 22, 0, 1, 0, time.UTC),
			changeRead: 5, wantReads: 12, accepted: 12,
		},
		{
			name:       "failed read crosses process month then next success stops acquisition",
			start:      time.Date(2030, time.April, 30, 23, 59, 59, 0, time.UTC),
			end:        time.Date(2030, time.May, 1, 0, 0, 1, 0, time.UTC),
			changeRead: 6, failRead: 6, wantReads: 7, accepted: 5,
		},
		{
			name:       "same month process day crossing retains every reply",
			start:      time.Date(2030, time.April, 15, 23, 59, 59, 0, time.UTC),
			end:        time.Date(2030, time.April, 16, 0, 0, 1, 0, time.UTC),
			changeRead: 6, wantReads: 12, accepted: 12,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := tc.start.Add(-time.Minute)
			provider := graphql.NewLiveSemanticProvider()
			provider.SetEnergyClock(func() time.Time { return clock })
			poller := &vaillantSemanticPoller{provider: provider}

			selectors := []struct{ channel, usage string }{
				{"gas", "climate"}, {"gas", "hot_water"},
				{"electricity", "climate"}, {"electricity", "hot_water"},
			}
			type point struct {
				value float64
				at    time.Time
			}
			want := make(map[graphql.EnergyMergeKey]point)
			// Absolute identities are explicit, independent of EnergyWindowKeyAt.
			// Include May so a discarded reply cannot silently overwrite a
			// previously accepted point in the newly current month either.
			for i, selector := range selectors {
				for _, month := range []int{0, 3, 4, 5} {
					key := graphql.EnergyMergeKey{Channel: selector.channel, Usage: selector.usage, Period: "month", Year: 2030, Month: month}
					if month == 0 {
						key.Period, key.Year = "all", 0
					}
					value := float64(900 + 10*i + month)
					if !provider.ApplyEnergyFromRegister(key, value) {
						t.Fatalf("seed rejected: %+v", key)
					}
					want[key] = point{value, clock}
				}
			}
			clock = tc.start
			// In the mismatch fixture the controller's synthetic relative
			// windows are May (current) and April (previous), while the process
			// still assigns those replies to April and March respectively.
			if tc.changeRead == 5 {
				zone := time.FixedZone("synthetic controller", 2*60*60)
				before, after := tc.start.In(zone), tc.end.In(zone)
				if before.Month() != time.April || before.Day() != 30 || after.Month() != time.May || after.Day() != 1 || tc.end.Month() != time.April {
					t.Fatalf("fixture does not cross controller month before process: controller=%v -> %v process=%v", before, after, tc.end)
				}
			}
			// Explicit synthetic relative-window replies: four lifetime, four
			// current-month, then four previous-month readings.
			replies := []uint32{101, 102, 103, 104, 11, 12, 13, 14, 21, 22, 23, 24}
			calls := 0
			poller.acquireEnergy(func(q b524EnergyQuery, ref time.Time) (uint32, bool) {
				calls++
				if calls > tc.wantReads {
					t.Fatalf("unexpected read %d after expected stop at %d", calls, tc.wantReads)
				}
				if ref != tc.start {
					t.Fatalf("read %d reference = %v; want single captured reference %v", calls, ref, tc.start)
				}
				selector := selectors[(calls-1)%4]
				period, label, month := "all", "", 0
				switch {
				case calls > 8:
					period, label, month = "month", "previous", 3
				case calls > 4:
					period, label, month = "month", "current", 4
				}
				if q.channel != selector.channel || q.usage != selector.usage || q.period != period || q.yearKind != label {
					t.Fatalf("read %d query = %+v; want %s/%s/%s/%s", calls, q, selector.channel, selector.usage, period, label)
				}
				if calls == tc.changeRead {
					clock = tc.end
				}
				value := replies[calls-1]
				if calls <= tc.accepted {
					key := graphql.EnergyMergeKey{Channel: selector.channel, Usage: selector.usage, Period: period, Month: month}
					if month != 0 {
						key.Year = 2030
					}
					want[key] = point{float64(value), clock}
				}
				return value, calls != tc.failRead
			})
			if calls != tc.wantReads || clock != tc.end {
				t.Fatalf("reads/clock = %d/%v; want %d/%v", calls, clock, tc.wantReads, tc.end)
			}

			check := func(currentMonth, previousMonth int) {
				t.Helper()
				totals := provider.EnergyTotals()
				if totals == nil {
					t.Fatal("missing totals")
				}
				series := []graphql.EnergySeries{totals.Gas.Climate, totals.Gas.DHW, totals.Electric.Climate, totals.Electric.DHW}
				for i, got := range series {
					selector := selectors[i]
					if len(got.Monthly) != 2 || len(got.MonthlyMeta) != 2 || len(got.Yearly) != 2 || len(got.YearlyMeta) != 2 {
						t.Fatalf("missing two-slot values/metadata: %+v", got)
					}
					checkPoint := func(value float64, meta graphql.EnergyPointMeta, key graphql.EnergyMergeKey) {
						t.Helper()
						expected := want[key]
						if value != expected.value || meta.Provenance != graphql.EnergyProvenanceRegister || meta.LastObservedUTC != expected.at.UTC().Format(time.RFC3339) {
							t.Fatalf("%+v: value/meta = %v/%+v; want %v, register, observed %v", key, value, meta, expected.value, expected.at)
						}
					}
					for slot, month := range []int{previousMonth, currentMonth} {
						checkPoint(got.Monthly[slot], got.MonthlyMeta[slot], graphql.EnergyMergeKey{Channel: selector.channel, Usage: selector.usage, Period: "month", Year: 2030, Month: month})
					}
					checkPoint(got.Yearly[1], got.YearlyMeta[1], graphql.EnergyMergeKey{Channel: selector.channel, Usage: selector.usage, Period: "all"})
					// The lifetime compatibility overlay shares register metadata
					// across both yearly slots, including the zero-valued slot.
					if got.Yearly[0] != 0 || got.YearlyMeta[0] != got.YearlyMeta[1] {
						t.Fatalf("unexpected previous yearly value/provenance: %+v", got)
					}
				}
			}
			if tc.end.Month() == time.May {
				check(5, 4)
			} else {
				check(4, 3)
			}
			// Project both sets of absolute identities without another read:
			// retained March/April points and untouched May seeds must survive.
			clock = tc.start
			check(4, 3)
			clock = time.Date(2030, time.May, 1, 0, 0, 1, 0, time.UTC)
			check(5, 4)
		})
	}
}
