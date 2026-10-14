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

	provider := NewLiveSemanticProvider()
	if !provider.applyEnergy(energyDateValues("year", uint16(2026), nil, nil), now) {
		t.Fatal("applyEnergy(year=2026) = false; want accepted as current year")
	}
	totals := provider.EnergyTotals()
	if totals == nil || len(totals.Gas.Climate.Yearly) != 2 || totals.Gas.Climate.Yearly[1] != 5.0 || totals.Gas.Climate.Yearly[0] != 0 {
		t.Fatalf("Yearly after 2026 = %#v; want [0 5]", totals)
	}

	provider = NewLiveSemanticProvider()
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
			provider := NewLiveSemanticProvider()
			if provider.applyEnergy(vals, now) {
				t.Fatal("applyEnergy() = true; want rejected")
			}
		})
	}
}

func TestApplyEnergy_YearlyFollowsReferenceDateAcrossYearBoundary(t *testing.T) {
	// The same absolute year 2026 is "current" in 2026 and "previous" in 2027.
	now := time.Date(2027, time.January, 2, 0, 5, 0, 0, time.UTC)
	provider := NewLiveSemanticProvider()
	if !provider.applyEnergy(energyDateValues("year", uint16(2026), nil, nil), now) {
		t.Fatal("applyEnergy(year=2026, ref=2027) = false; want accepted as previous")
	}
	totals := provider.EnergyTotals()
	if totals.Gas.Climate.Yearly[0] != 5.0 || totals.Gas.Climate.Yearly[1] != 0 {
		t.Fatalf("Yearly = %v; want [5 0]", totals.Gas.Climate.Yearly)
	}
}

func TestApplyEnergy_DayRejectsTotalAndUndatedSelectors(t *testing.T) {
	now := time.Date(2026, time.April, 7, 12, 0, 0, 0, time.UTC)
	provider := NewLiveSemanticProvider()
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
	provider := NewLiveSemanticProvider()
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
	provider := NewLiveSemanticProvider()
	if !provider.applyEnergy(yearly, now) {
		t.Fatal("applyEnergy(yearly 2025) = false; want accepted")
	}
	if got := provider.EnergyTotals().Gas.DHW.Yearly; len(got) != 2 || got[0] == 0 || got[1] != 0 {
		t.Fatalf("Gas.DHW.Yearly = %v; want previous slot only", got)
	}

	// Documented `87 34` total reply (2026-04-07 echo, period=all) is not a window.
	total := decodeB516(t, []byte{0x00, 0x00, 0x00, 0x03, 0x03, 0x87, 0x34, 0x00, 0x54, 0x5D, 0x46})
	provider = NewLiveSemanticProvider()
	if provider.applyEnergy(total, now) {
		t.Fatal("applyEnergy(total reply 87 34) = true; want rejected")
	}
}
