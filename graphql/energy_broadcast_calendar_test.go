package graphql

import (
	"maps"
	"reflect"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

// Characterize admission under the existing process-reference calendar, using
// the supported decoded B516 shape from energy_date_contract_test.go. These
// synthetic events do not qualify native acquisition or resolve #1022 policy.
func TestEnergyBroadcastCalendar(t *testing.T) {
	bucharest, err := time.LoadLocation("Europe/Bucharest")
	if err != nil {
		t.Fatal(err)
	}
	key := func(period string, year, month, day int) EnergyMergeKey {
		return EnergyMergeKey{Channel: "gas", Usage: "climate", Period: period, Year: year, Month: month, Day: day}
	}
	cases := []struct {
		name                   string
		instant                string
		localDate              string
		period                 string
		year, month, day       int
		utcKey, localKey       EnergyMergeKey
		utcAccept, localAccept bool
	}{
		{"same date winter", "2030-01-15T12:00:00Z", "2030-01-15", "day", 2030, 1, 15,
			key("day", 2030, 1, 15), key("day", 2030, 1, 15), true, true},
		{"before summer local midnight", "2030-07-15T20:59:00Z", "2030-07-15", "day", 2030, 7, 15,
			key("day", 2030, 7, 15), key("day", 2030, 7, 15), true, true},
		{"after summer local midnight UTC date", "2030-07-15T21:01:00Z", "2030-07-16", "day", 2030, 7, 15,
			key("day", 2030, 7, 15), key("day", 2030, 7, 16), true, false},
		{"after summer local midnight local date", "2030-07-15T21:01:00Z", "2030-07-16", "day", 2030, 7, 16,
			key("day", 2030, 7, 15), key("day", 2030, 7, 16), false, true},
		{"after winter local midnight UTC date", "2030-12-31T22:01:00Z", "2031-01-01", "day", 2030, 12, 31,
			key("day", 2030, 12, 31), key("day", 2031, 1, 1), true, false},
		{"after winter local midnight local date", "2030-12-31T22:01:00Z", "2031-01-01", "day", 2031, 1, 1,
			key("day", 2030, 12, 31), key("day", 2031, 1, 1), false, true},
		// Monthly decoded events are not consumed, even when their absolute
		// month is current in one calendar and previous in the other.
		{"month rollover old month", "2030-04-30T21:01:00Z", "2030-05-01", "month", 2030, 4, 0,
			key("month", 2030, 4, 0), key("month", 2030, 5, 0), false, false},
		{"month rollover new month", "2030-04-30T21:01:00Z", "2030-05-01", "month", 2030, 5, 0,
			key("month", 2030, 4, 0), key("month", 2030, 5, 0), false, false},
		{"year rollover monthly window", "2030-12-31T22:01:00Z", "2031-01-01", "month", 2030, 12, 0,
			key("month", 2030, 12, 0), key("month", 2031, 1, 0), false, false},
		{"year rollover oldest year", "2030-12-31T22:01:00Z", "2031-01-01", "year", 2029, 0, 0,
			key("year", 2029, 0, 0), key("year", 2031, 0, 0), true, false},
		{"year rollover shared year", "2030-12-31T22:01:00Z", "2031-01-01", "year", 2030, 0, 0,
			key("year", 2030, 0, 0), key("year", 2030, 0, 0), true, true},
		{"year rollover new year", "2030-12-31T22:01:00Z", "2031-01-01", "year", 2031, 0, 0,
			key("year", 2030, 0, 0), key("year", 2031, 0, 0), false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			utc, err := time.Parse(time.RFC3339, tc.instant)
			if err != nil {
				t.Fatal(err)
			}
			local := utc.In(bucharest)
			if !utc.Equal(local) || local.Format("2006-01-02") != tc.localDate {
				t.Fatalf("invalid equal-instant fixture: %v / %v", utc, local)
			}
			for _, calendar := range []struct {
				name   string
				ref    time.Time
				key    EnergyMergeKey
				accept bool
			}{
				{"UTC", utc, tc.utcKey, tc.utcAccept},
				{"Europe Bucharest", local, tc.localKey, tc.localAccept},
			} {
				t.Run(calendar.name, func(t *testing.T) {
					ref := calendar.ref
					provider := NewLiveSemanticProvider()
					provider.SetEnergyClock(func() time.Time { return ref })
					var month, day any
					if tc.period == "day" || tc.period == "month" {
						month = uint8(tc.month)
					}
					if tc.period == "day" {
						day = uint8(tc.day)
					}
					event := router.BroadcastEvent{Values: energyDateValues(tc.period, uint16(tc.year), month, day)}
					totals, updated := provider.ApplyBroadcast(event)
					if updated != calendar.accept || (totals != nil) != calendar.accept {
						t.Fatalf("ApplyBroadcast = (%v, %v); want accepted=%v", totals, updated, calendar.accept)
					}
					if calendar.accept {
						want := map[energyMergeKey]energyDataPoint{calendar.key: {Value: 5, Source: EnergySourceBroadcast, IngestAt: ref}}
						if !reflect.DeepEqual(provider.energyMerge.points, want) {
							t.Fatalf("admitted absolute points = %+v; want %+v", provider.energyMerge.points, want)
						}
					} else if len(provider.energyMerge.points) != 0 || provider.energyMerge.Revision() != 0 {
						t.Fatal("rejected event changed empty merge store")
					}

					// Seed an independently specified register identity. For accepted
					// events this replaces their point; for rejected events it retains
					// a valid reference-calendar point across another rejected input.
					if !provider.ApplyEnergyFromRegister(calendar.key, 17) {
						t.Fatal("register input rejected")
					}
					wantRegister := map[energyMergeKey]energyDataPoint{calendar.key: {Value: 17, Source: EnergySourceRegister, IngestAt: ref}}
					if !reflect.DeepEqual(provider.energyMerge.points, wantRegister) {
						t.Fatalf("register points = %+v; want %+v", provider.energyMerge.points, wantRegister)
					}
					climate := provider.EnergyTotals().Gas.Climate
					retained := 0
					checkRetained := func(value float64, meta EnergyPointMeta) {
						if value == 17 && meta.Provenance == EnergyProvenanceRegister {
							retained++
						}
					}
					checkRetained(climate.Today, climate.TodayMeta)
					for i, value := range climate.Monthly {
						checkRetained(value, climate.MonthlyMeta[i])
					}
					for i, value := range climate.Yearly {
						checkRetained(value, climate.YearlyMeta[i])
					}
					if retained != 1 {
						t.Fatalf("public register points = %d; want one value 17 with register provenance", retained)
					}
					beforePoints := maps.Clone(provider.energyMerge.points)
					beforeRevision := provider.energyMerge.Revision()
					// Advance ingest time without changing date: precedence must hold
					// even for a newer broadcast, independently of monotonicity.
					ref = ref.Add(time.Second)
					before := provider.EnergyTotals()
					if totals, updated := provider.ApplyBroadcast(event); updated || totals != nil {
						t.Fatalf("broadcast over retained register = (%v, %v); want (nil, false)", totals, updated)
					}
					if !reflect.DeepEqual(provider.energyMerge.points, beforePoints) || provider.energyMerge.Revision() != beforeRevision {
						t.Fatal("rejected broadcast changed retained register data, source, ingest time or revision")
					}
					if !reflect.DeepEqual(provider.EnergyTotals(), before) {
						t.Fatal("rejected broadcast changed retained public values or provenance")
					}
				})
			}
		})
	}
}
