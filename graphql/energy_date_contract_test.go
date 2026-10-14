package graphql

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
)

// energyDateValues builds the decoded B516 value map published by ebusreg
// (absolute `year`, plus month/day where the period carries them).
func energyDateValues(period string, year, month, day any) map[string]types.Value {
	v := map[string]types.Value{
		"wh":     {Valid: true, Value: float64(5000)},
		"source": {Valid: true, Value: "gas"},
		"usage":  {Valid: true, Value: "heating"},
		"period": {Valid: true, Value: period},
		"year":   {Valid: false},
		"month":  {Valid: false},
		"day":    {Valid: false},
	}
	if year != nil {
		v["year"] = types.Value{Valid: true, Value: year}
	}
	if month != nil {
		v["month"] = types.Value{Valid: true, Value: month}
	}
	if day != nil {
		v["day"] = types.Value{Valid: true, Value: day}
	}
	return v
}

func TestMatchesToday_RequiresValidYearMonthDay(t *testing.T) {
	now := time.Date(2026, time.April, 7, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		vals map[string]types.Value
		want bool
	}{
		{"exact date", energyDateValues("day", uint16(2026), uint8(4), uint8(7)), true},
		{"wrong year", energyDateValues("day", uint16(2025), uint8(4), uint8(7)), false},
		{"wrong month", energyDateValues("day", uint16(2026), uint8(5), uint8(7)), false},
		{"wrong day", energyDateValues("day", uint16(2026), uint8(4), uint8(8)), false},
		{"missing year", energyDateValues("day", nil, uint8(4), uint8(7)), false},
		{"missing month", energyDateValues("day", uint16(2026), nil, uint8(7)), false},
		{"missing day", energyDateValues("day", uint16(2026), uint8(4), nil), false},
		{"no date at all", energyDateValues("day", nil, nil, nil), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchesToday(tc.vals, now); got != tc.want {
				t.Fatalf("matchesToday() = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestApplyEnergy_YearlyUsesAbsoluteYearAgainstReferenceDate(t *testing.T) {
	now := time.Date(2026, time.April, 7, 12, 0, 0, 0, time.UTC)

	provider := providerAt(now)
	if !provider.applyEnergy(energyDateValues("year", uint16(2026), nil, nil), now) {
		t.Fatal("applyEnergy(year=2026) = false; want accepted as current year")
	}
	totals := provider.EnergyTotals()
	if totals == nil || len(totals.Gas.Climate.Yearly) != 2 || totals.Gas.Climate.Yearly[1] != 5.0 || totals.Gas.Climate.Yearly[0] != 0 {
		t.Fatalf("Yearly after 2026 = %#v; want [0 5]", totals)
	}

	provider = providerAt(now)
	if !provider.applyEnergy(energyDateValues("year", uint16(2025), nil, nil), now) {
		t.Fatal("applyEnergy(year=2025) = false; want accepted as previous year")
	}
	totals = provider.EnergyTotals()
	if totals == nil || len(totals.Gas.Climate.Yearly) != 2 || totals.Gas.Climate.Yearly[0] != 5.0 || totals.Gas.Climate.Yearly[1] != 0 {
		t.Fatalf("Yearly after 2025 = %#v; want [5 0]", totals)
	}
}

func TestApplyEnergy_YearlyRejectsUnknownOrOutOfWindowYear(t *testing.T) {
	now := time.Date(2026, time.April, 7, 12, 0, 0, 0, time.UTC)
	cases := map[string]map[string]types.Value{
		"year invalid (unknown kind)": energyDateValues("year", nil, nil, nil),
		"two years back":              energyDateValues("year", uint16(2024), nil, nil),
		"future year":                 energyDateValues("year", uint16(2027), nil, nil),
		"year 2032 not a window":      energyDateValues("year", uint16(2032), nil, nil),
	}
	for name, vals := range cases {
		t.Run(name, func(t *testing.T) {
			provider := providerAt(now)
			if provider.applyEnergy(vals, now) {
				t.Fatal("applyEnergy() = true; want rejected")
			}
		})
	}
}

func providerAt(now time.Time) *LiveSemanticProvider {
	provider := NewLiveSemanticProvider()
	provider.SetEnergyClock(func() time.Time { return now })
	return provider
}

// Same-provider New Year rollover: values stored on Dec 31 keep their absolute
// year identity and are re-projected on Jan 1; re-receiving the old year
// updates its own window instead of the new current slot.
func TestEnergy_YearRolloverKeepsAbsoluteWindowIdentity(t *testing.T) {
	clock := time.Date(2026, time.December, 31, 23, 59, 0, 0, time.UTC)
	provider := NewLiveSemanticProvider()
	provider.SetEnergyClock(func() time.Time { return clock })

	vals := func(year uint16, wh float64) map[string]types.Value {
		v := energyDateValues("year", year, nil, nil)
		v["wh"] = types.Value{Valid: true, Value: wh}
		return v
	}
	if !provider.applyEnergy(vals(2025, 7000), clock) || !provider.applyEnergy(vals(2026, 5000), clock.Add(time.Second)) {
		t.Fatal("applyEnergy on 2026-12-31 rejected")
	}
	if got := provider.EnergyTotals().Gas.Climate.Yearly; got[0] != 7 || got[1] != 5 {
		t.Fatalf("Yearly on 2026-12-31 = %v; want [7 5]", got)
	}

	clock = time.Date(2027, time.January, 1, 0, 1, 0, 0, time.UTC)
	got := provider.EnergyTotals().Gas.Climate
	if got.Yearly[0] != 5 || got.Yearly[1] != 0 {
		t.Fatalf("Yearly on 2027-01-01 = %v; want [5 0] (2026 previous, 2027 current)", got.Yearly)
	}
	if got.YearlyMeta[1].Provenance != EnergyProvenanceNone {
		t.Fatalf("current-year meta = %+v; want no provenance for unseen 2027", got.YearlyMeta[1])
	}

	// Re-receive 2026 after rollover with a new value: it lands in previous only.
	if !provider.applyEnergy(vals(2026, 5500), clock) {
		t.Fatal("re-receive of 2026 after rollover rejected")
	}
	got = provider.EnergyTotals().Gas.Climate
	if got.Yearly[0] != 5.5 || got.Yearly[1] != 0 {
		t.Fatalf("Yearly after re-receive = %v; want [5.5 0] (no duplicate year)", got.Yearly)
	}
	// 2025 is now out of the public window and must not be ingested.
	if provider.applyEnergy(vals(2025, 9000), clock.Add(time.Second)) {
		t.Fatal("applyEnergy(2025) after rollover = true; want rejected")
	}
}

// Register reads resolve relative labels against the same reference clock, so
// they map onto the same dated identity as broadcasts.
func TestEnergy_RegisterWindowKeysShareBroadcastIdentity(t *testing.T) {
	clock := time.Date(2027, time.January, 1, 8, 0, 0, 0, time.UTC)
	provider := providerAt(clock)
	if !provider.applyEnergy(energyDateValues("year", uint16(2026), nil, nil), clock) {
		t.Fatal("broadcast rejected")
	}
	// A register read of "previous" year on 2027-01-01 is the same 2026 window.
	key := provider.EnergyWindowKey("gas", "climate", "year", "previous")
	if key != (energyMergeKey{Channel: "gas", Usage: "climate", Period: "year", Year: 2026}) {
		t.Fatalf("previous-year key = %+v", key)
	}
	if !provider.ApplyEnergyFromRegister(key, 6.0) {
		t.Fatal("register apply rejected (register must win over broadcast)")
	}
	if got := provider.EnergyTotals().Gas.Climate.Yearly; got[0] != 6 || got[1] != 0 {
		t.Fatalf("Yearly = %v; want [6 0]", got)
	}
	// Previous month across the year boundary is December 2026.
	m := provider.EnergyWindowKey("gas", "climate", "month", "previous")
	if m.Year != 2026 || m.Month != 12 {
		t.Fatalf("previous-month key = %+v; want 2026-12", m)
	}
}

func TestApplyEnergy_DayRejectsTotalAndUndatedSelectors(t *testing.T) {
	now := time.Date(2026, time.April, 7, 12, 0, 0, 0, time.UTC)
	provider := providerAt(now)
	// "87 34" total reply echoes 2026-04-07 but the period is "all": not a window.
	if provider.applyEnergy(energyDateValues("all", uint16(2026), nil, nil), now) {
		t.Fatal("applyEnergy(period=all) = true; want rejected")
	}
	if provider.applyEnergy(energyDateValues("day", nil, nil, nil), now) {
		t.Fatal("applyEnergy(day without date) = true; want rejected")
	}
	if !provider.applyEnergy(energyDateValues("day", uint16(2026), uint8(4), uint8(7)), now) {
		t.Fatal("applyEnergy(day 2026-04-07) = false; want accepted")
	}
}

func TestApplyEnergy_MonthlyBroadcastIsNotConsumed(t *testing.T) {
	now := time.Date(2026, time.April, 7, 12, 0, 0, 0, time.UTC)
	provider := providerAt(now)
	if provider.applyEnergy(energyDateValues("month", uint16(2026), uint8(4), nil), now) {
		t.Fatal("applyEnergy(month) = true; want unchanged (monthly broadcast not consumed)")
	}
}

// decodeB516 runs a raw B5 16 payload through the real ebusreg decoder so the
// tests pin the published value map rather than a hand-built one.
func decodeB516(t *testing.T, data []byte) map[string]types.Value {
	t.Helper()
	planes := vaillantsystem.NewProvider().CreatePlanes(registry.DeviceInfo{Manufacturer: "Vaillant", Address: 0x08})
	decoder, ok := planes[0].(router.BroadcastDecoder)
	if !ok {
		t.Fatalf("system plane %T does not implement router.BroadcastDecoder", planes[0])
	}
	values, handled, err := decoder.DecodeBroadcast(protocol.Frame{Primary: 0xB5, Secondary: 0x16, Data: data})
	if err != nil || !handled {
		t.Fatalf("DecodeBroadcast handled=%v err=%v", handled, err)
	}
	return values
}

func TestApplyEnergy_RealDecoderYearlySelectorAndTotalReply(t *testing.T) {
	now := time.Date(2026, time.April, 7, 12, 0, 0, 0, time.UTC)

	// Yearly reply for 2025 (date word 0x3200 -> year 2025): previous slot.
	yearly := decodeB516(t, []byte{0x03, 0x00, 0x00, 0x04, 0x04, 0x00, 0x32, 0x00, 0x00, 0x7A, 0x44})
	if got := yearly["year"]; !got.Valid || got.Value != uint16(2025) {
		t.Fatalf("decoded year = %+v; want 2025", got)
	}
	provider := providerAt(now)
	if !provider.applyEnergy(yearly, now) {
		t.Fatal("applyEnergy(yearly 2025) = false; want accepted")
	}
	if got := provider.EnergyTotals().Gas.DHW.Yearly; len(got) != 2 || got[0] == 0 || got[1] != 0 {
		t.Fatalf("Gas.DHW.Yearly = %v; want previous slot only", got)
	}

	// Documented `87 34` total reply (2026-04-07 echo, period=all) is not a window.
	total := decodeB516(t, []byte{0x00, 0x00, 0x00, 0x03, 0x03, 0x87, 0x34, 0x00, 0x54, 0x5D, 0x46})
	provider = providerAt(now)
	if provider.applyEnergy(total, now) {
		t.Fatal("applyEnergy(total reply 87 34) = true; want rejected")
	}
}

// energyTestRef is the deterministic reference date shared by fixtures and
// ingestion (matches t0 in energy_merge_test.go: 2026-02-26 12:00 UTC).
var energyTestRef = time.Date(2026, time.February, 26, 12, 0, 0, 0, time.UTC)

// newClockedProvider returns a provider whose energy clock is pinned to
// energyTestRef.
func newClockedProvider() *LiveSemanticProvider {
	provider := NewLiveSemanticProvider()
	provider.SetEnergyClock(func() time.Time { return energyTestRef })
	return provider
}

// wkey builds a dated key against energyTestRef.
func wkey(channel, usage, period, label string) energyMergeKey {
	return energyWindowKey(channel, usage, period, label, energyTestRef)
}
