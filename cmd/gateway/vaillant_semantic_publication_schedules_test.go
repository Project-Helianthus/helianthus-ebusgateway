package main

import (
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

func TestVaillantScheduleAcquisitionImportsNoPublicationPackages(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "semantic_vaillant_schedules.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		switch path {
		case "github.com/Project-Helianthus/helianthus-ebusgateway/graphql",
			"github.com/Project-Helianthus/helianthus-ebusgateway/mcp":
			t.Fatalf("semantic_vaillant_schedules.go imports %s; the schedule acquisition must stay native", path)
		}
	}
}

// Every field of a native program, configuration, day and timer slot carries
// a distinct value, so a mapping that drops or swaps any field fails.
func TestVaillantPublicationOwnerMapsNativeSchedulePrograms(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	owner := newVaillantSemanticPublication(provider, nil, nil)

	minTemp, maxTemp := 11.0, 29.0
	tempC, tempRaw := 21.5, 215
	native := []vaillantScheduleProgram{
		{
			Zone: 2,
			HC:   "heating",
			Config: &vaillantScheduleConfig{
				MaxSlots: 3, TimeResolution: 10, MinDuration: 20, HasTemperature: true, TempSlots: 4,
				MinTempC: &minTemp, MaxTempC: &maxTemp,
			},
			SlotsUsed: []int{1, 2, 0, 0, 0, 0, 3},
			Days: []vaillantScheduleDay{
				{Weekday: "Monday", Slots: []vaillantScheduleTimerSlot{
					{StartHour: 5, StartMinute: 30, EndHour: 21, EndMinute: 45, TemperatureC: &tempC, TemperatureRaw: &tempRaw},
				}},
				{Weekday: "Tuesday"},
			},
		},
		{Zone: 255, HC: "dhw"},
	}
	owner.publishSchedulePrograms(native)

	want := &graphql.ScheduleStatus{Programs: []graphql.ScheduleProgram{
		{
			Zone: 2,
			HC:   "heating",
			Config: &graphql.ScheduleConfig{
				MaxSlots: 3, TimeResolution: 10, MinDuration: 20, HasTemperature: true, TempSlots: 4,
				MinTempC: &minTemp, MaxTempC: &maxTemp,
			},
			SlotsUsed: []int{1, 2, 0, 0, 0, 0, 3},
			Days: []graphql.ScheduleDayProgram{
				{Weekday: "Monday", Slots: []graphql.ScheduleTimerSlot{
					{StartHour: 5, StartMinute: 30, EndHour: 21, EndMinute: 45, TemperatureC: &tempC, TemperatureRaw: &tempRaw},
				}},
				{Weekday: "Tuesday"},
			},
		},
		{Zone: 255, HC: "dhw"},
	}}
	got := provider.Schedules()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("published schedules = %#v; want %#v", got, want)
	}
	if got.Programs[0].Days[1].Slots != nil || got.Programs[1].Days != nil || got.Programs[1].SlotsUsed != nil || got.Programs[1].Config != nil {
		t.Fatalf("nil native slices or configuration were published non-nil: %#v", got.Programs)
	}
	if got.Programs[0].Days[0].Slots[0].TemperatureC == &tempC || got.Programs[0].Config.MinTempC == &minTemp {
		t.Fatal("published schedule shares temperature pointers with the native program")
	}
}

// The program key is (zone, hc) and the day is chosen by weekday: each test
// program differs from the target in one key field only.
func TestVaillantPublicationOwnerPublishesScheduleDayForMatchingProgram(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	owner := newVaillantSemanticPublication(provider, nil, nil)

	owner.publishScheduleDay(0, "heating", 0, vaillantScheduleDay{Weekday: "Monday"}, []int{1})
	if provider.Schedules() != nil {
		t.Fatalf("schedule day published without schedules: %#v", provider.Schedules())
	}

	days := func() []vaillantScheduleDay {
		return []vaillantScheduleDay{{Weekday: "Monday"}, {Weekday: "Tuesday"}, {Weekday: "Wednesday"}}
	}
	owner.publishSchedulePrograms([]vaillantScheduleProgram{
		{Zone: 1, HC: "heating", SlotsUsed: []int{0, 0, 0}, Days: days()},
		{Zone: 0, HC: "cooling", SlotsUsed: []int{0, 0, 0}, Days: days()},
		{Zone: 0, HC: "heating", SlotsUsed: []int{0, 0, 0}, Days: days()},
	})

	tempC, tempRaw := 19.5, 195
	written := vaillantScheduleTimerSlot{StartHour: 6, StartMinute: 15, EndHour: 22, EndMinute: 50, TemperatureC: &tempC, TemperatureRaw: &tempRaw}
	owner.publishScheduleDay(0, "heating", 2, vaillantScheduleDay{Weekday: "Wednesday", Slots: []vaillantScheduleTimerSlot{written}}, []int{0, 0, 1})

	got := provider.Schedules()
	for i, prog := range got.Programs[:2] {
		if prog.Days[2].Slots != nil || !reflect.DeepEqual(prog.SlotsUsed, []int{0, 0, 0}) {
			t.Fatalf("program %d (zone %d, %s) changed: %#v", i, prog.Zone, prog.HC, prog)
		}
	}
	target := got.Programs[2]
	wantDay := graphql.ScheduleDayProgram{Weekday: "Wednesday", Slots: []graphql.ScheduleTimerSlot{
		{StartHour: 6, StartMinute: 15, EndHour: 22, EndMinute: 50, TemperatureC: &tempC, TemperatureRaw: &tempRaw},
	}}
	if !reflect.DeepEqual(target.Days[2], wantDay) {
		t.Fatalf("target day = %#v; want %#v", target.Days[2], wantDay)
	}
	if target.Days[0].Slots != nil || target.Days[1].Slots != nil {
		t.Fatalf("other weekdays of the target changed: %#v", target.Days)
	}
	if !reflect.DeepEqual(target.SlotsUsed, []int{0, 0, 1}) {
		t.Fatalf("target slots used = %v; want [0 0 1]", target.SlotsUsed)
	}

	owner.publishScheduleDay(0, "heating", 0, vaillantScheduleDay{Weekday: "Monday"}, nil)
	if got := provider.Schedules().Programs[2].SlotsUsed; !reflect.DeepEqual(got, []int{0, 0, 1}) {
		t.Fatalf("slots used after an unread slot count = %v; want kept [0 0 1]", got)
	}
	owner.publishScheduleDay(0, "heating", 5, vaillantScheduleDay{Weekday: "Saturday"}, []int{4})
	if got := provider.Schedules().Programs[2]; len(got.Days) != 3 || !reflect.DeepEqual(got.SlotsUsed, []int{4}) {
		t.Fatalf("out-of-range weekday = %#v; want days kept and slots used replaced", got)
	}
}
