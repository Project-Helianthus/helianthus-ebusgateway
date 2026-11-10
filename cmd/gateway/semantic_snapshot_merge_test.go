package main

import (
	"fmt"
	"reflect"
	"testing"
)

type snapshotMergeField struct {
	family string
	name   string
	key    semanticFieldKey
}

func snapshotMergeFields() []snapshotMergeField {
	return []snapshotMergeField{
		{"zone", "Name", zoneFieldName},
		{"zone", "OperatingMode", zoneFieldOperatingMode},
		{"zone", "Preset", zoneFieldPreset},
		{"zone", "HvacAction", zoneFieldHvacAction},
		{"zone", "AllowedModes", zoneFieldAllowedModes},
		{"zone", "CurrentTempC", zoneFieldCurrentTempC},
		{"zone", "TargetTempC", zoneFieldTargetTempC},
		{"zone", "HumidityPct", zoneFieldCurrentHumidityPct},
		{"zone", "ConfigurationHeatingOperationMode", zoneFieldZoneOperationModeRaw},
		{"zone", "StateSpecialFunction", zoneFieldSpecialFunctionRaw},
		{"zone", "ConfigurationRoomTemperatureZoneMappingRaw", zoneFieldRoomTemperatureZoneMappingRaw},
		{"zone", "ConfigurationAssociatedCircuitRaw", zoneFieldZoneCircuitIndexRaw},
		{"zone", "ConfigurationCircuitTypeRaw", zoneFieldCircuitTypeRaw},
		{"zone", "StateValveStatusRaw", zoneFieldZoneValveStatusRaw},
		{"zone", "QuickVetoTempC", zoneFieldQuickVetoTempC},
		{"zone", "QuickVetoDurationH", zoneFieldQuickVetoDurationH},
		{"zone", "QuickVetoEndTime", zoneFieldQuickVetoEndTime},
		{"zone", "QuickVetoEndDate", zoneFieldQuickVetoEndDate},
		{"zone", "HolidayStartDate", zoneFieldHolidayStartDate},
		{"zone", "HolidayEndDate", zoneFieldHolidayEndDate},
		{"zone", "HolidaySetpointC", zoneFieldHolidaySetpointC},
		{"zone", "HolidayStartTime", zoneFieldHolidayStartTime},
		{"zone", "HolidayEndTime", zoneFieldHolidayEndTime},
		{"dhw", "OperatingMode", dhwFieldOperatingMode},
		{"dhw", "Preset", dhwFieldPreset},
		{"dhw", "CurrentTempC", dhwFieldCurrentTempC},
		{"dhw", "TargetTempC", dhwFieldTargetTempC},
		{"dhw", "ConfigurationDHWOperationMode", dhwFieldDhwOperationModeRaw},
		{"dhw", "StateSpecialFunction", dhwFieldSpecialFunctionRaw},
		{"dhw", "HolidayStartDate", dhwFieldHolidayStartDate},
		{"dhw", "HolidayEndDate", dhwFieldHolidayEndDate},
	}
}

// Every field has an independent expected key; distinct values expose crossed wiring.
func TestSemanticSnapshotMergeFields(t *testing.T) {
	for index, field := range snapshotMergeFields() {
		t.Run(field.family+"/"+field.name, func(t *testing.T) {
			for _, source := range []semanticSnapshotSource{semanticSnapshotSourceCache, semanticSnapshotSourceLive} {
				for _, oldSource := range []semanticSnapshotSource{semanticSnapshotSourceCache, semanticSnapshotSourceLive} {
					for _, stale := range []bool{false, true} {
						for _, known := range []bool{false, true} {
							for _, attempted := range []bool{false, true} {
								for _, sample := range []string{"absent", "empty", "blank", "zero", "value"} {
									name := fmt.Sprintf("source=%d/old=%d/stale=%t/known=%t/attempted=%t/%s", source, oldSource, stale, known, attempted, sample)
									t.Run(name, func(t *testing.T) {
										checkSnapshotMergeField(t, index, field, source, oldSource, stale, known, attempted, sample)
									})
								}
							}
						}
					}
				}
			}
		})
	}
}

func newSnapshotMergeEntry(family string) any {
	if family == "zone" {
		return &vaillantZoneSnapshot{Instance: 7, Present: true}
	}
	return &vaillantDhwSnapshot{}
}

func applySnapshotMerge(entry, incoming any, source semanticSnapshotSource, attempted semanticFieldSet) {
	switch entry := entry.(type) {
	case *vaillantZoneSnapshot:
		mergeZoneSnapshotFields(entry, incoming.(*vaillantZoneSnapshot), source, attempted)
	case *vaillantDhwSnapshot:
		mergeDhwSnapshotFields(entry, incoming.(*vaillantDhwSnapshot), source, attempted)
	}
}

func snapshotMergeValue(typ reflect.Type, sample string, index int) (reflect.Value, bool) {
	value := reflect.New(typ).Elem()
	switch typ.Kind() {
	case reflect.String:
		if sample == "blank" {
			value.SetString(" \t ")
		}
		if sample == "zero" {
			value.SetString("0")
		}
		if sample == "value" {
			value.SetString(fmt.Sprintf(" value-%d ", index))
		}
		return value, sample == "zero" || sample == "value"
	case reflect.Slice:
		if sample != "absent" {
			value.Set(reflect.ValueOf([]string{}))
		}
		if sample == "blank" {
			value.Set(reflect.ValueOf([]string{" "}))
		}
		if sample == "value" || sample == "zero" {
			value.Set(reflect.ValueOf([]string{fmt.Sprintf("mode-%d", index), "off", "auto"}))
		}
		return value, value.Len() > 0
	case reflect.Ptr:
		if sample == "absent" || sample == "empty" || sample == "blank" {
			return value, false
		}
		value.Set(reflect.New(typ.Elem()))
		if sample == "value" {
			if typ.Elem().Kind() == reflect.Float64 {
				value.Elem().SetFloat(float64(index) + 12.5)
			} else {
				value.Elem().SetUint(uint64(index) + 9)
			}
		}
		return value, true
	default:
		panic("unexpected merge field type")
	}
}

func populatedSnapshotMergeEntry(family string) any {
	entry := newSnapshotMergeEntry(family)
	object := reflect.ValueOf(entry).Elem()
	for index, field := range snapshotMergeFields() {
		if field.family != family {
			continue
		}
		target := object.FieldByName(field.name)
		value, _ := snapshotMergeValue(target.Type(), "value", index+100)
		target.Set(value)
	}
	return entry
}

func checkSnapshotMergeField(t *testing.T, index int, field snapshotMergeField, source, oldSource semanticSnapshotSource, stale, known, attempted bool, sample string) {
	t.Helper()
	entry := populatedSnapshotMergeEntry(field.family)
	expected := populatedSnapshotMergeEntry(field.family)
	incoming := newSnapshotMergeEntry(field.family)
	target := reflect.ValueOf(incoming).Elem().FieldByName(field.name)
	value, present := snapshotMergeValue(target.Type(), sample, index)
	target.Set(value)
	old := semanticFieldFreshness{Source: oldSource, Stale: stale}
	actualFields := map[semanticFieldKey]semanticFieldFreshness{"unrelated": {Source: oldSource, Stale: stale}}
	wantFields := map[semanticFieldKey]semanticFieldFreshness{"unrelated": {Source: oldSource, Stale: stale}}
	if known {
		actualFields[field.key] = old
		wantFields[field.key] = old
	}
	reflect.ValueOf(entry).Elem().FieldByName("FieldFreshness").Set(reflect.ValueOf(actualFields))
	reflect.ValueOf(expected).Elem().FieldByName("FieldFreshness").Set(reflect.ValueOf(wantFields))
	fields := newSemanticFieldSet()
	if attempted {
		fields[field.key] = struct{}{}
		if present {
			independent, _ := snapshotMergeValue(target.Type(), sample, index)
			reflect.ValueOf(expected).Elem().FieldByName(field.name).Set(independent)
			wantFields[field.key] = semanticFieldFreshness{Source: source, Stale: false}
		} else if known {
			wantFields[field.key] = semanticFieldFreshness{Source: oldSource, Stale: true}
		}
	}
	applySnapshotMerge(entry, incoming, source, fields)
	if !reflect.DeepEqual(entry, expected) {
		t.Fatalf("value or provenance mismatch: got %#v; want %#v", entry, expected)
	}
	// Mutating accepted input must never change the stored value or ordering.
	if present && target.Kind() == reflect.Ptr {
		if target.Type().Elem().Kind() == reflect.Float64 {
			target.Elem().SetFloat(999)
		} else {
			target.Elem().SetUint(999)
		}
	}
	if present && target.Kind() == reflect.Slice {
		target.Index(0).SetString("mutated")
	}
	if !reflect.DeepEqual(entry, expected) {
		t.Fatal("incoming mutation changed merged snapshot")
	}
}

func TestSemanticSnapshotMergeIndependentFailures(t *testing.T) {
	for _, family := range []string{"zone", "dhw"} {
		entry := populatedSnapshotMergeEntry(family)
		incoming := populatedSnapshotMergeEntry(family)
		object := reflect.ValueOf(incoming).Elem()
		fields := newSemanticFieldSet()
		freshness := map[semanticFieldKey]semanticFieldFreshness{}
		for index, field := range snapshotMergeFields() {
			if field.family != family {
				continue
			}
			fields[field.key] = struct{}{}
			freshness[field.key] = semanticFieldFreshness{Source: semanticSnapshotSourceCache}
			target := object.FieldByName(field.name)
			if index%2 == 0 {
				target.Set(reflect.Zero(target.Type()))
			}
		}
		reflect.ValueOf(entry).Elem().FieldByName("FieldFreshness").Set(reflect.ValueOf(freshness))
		applySnapshotMerge(entry, incoming, semanticSnapshotSourceLive, fields)
		for index, field := range snapshotMergeFields() {
			if field.family != family {
				continue
			}
			want := semanticFieldFreshness{Source: semanticSnapshotSourceLive}
			if index%2 == 0 {
				want = semanticFieldFreshness{Source: semanticSnapshotSourceCache, Stale: true}
			}
			if freshness[field.key] != want {
				t.Fatalf("%s: got %+v want %+v", field.name, freshness[field.key], want)
			}
			expected := populatedSnapshotMergeEntry(family)
			got := reflect.ValueOf(entry).Elem().FieldByName(field.name).Interface()
			if !reflect.DeepEqual(got, reflect.ValueOf(expected).Elem().FieldByName(field.name).Interface()) {
				t.Fatalf("%s: partial failure lost last-known value", field.name)
			}
		}
	}
}

func TestSemanticSnapshotMergeNilAndFreshness(t *testing.T) {
	mergeZoneSnapshotFields(nil, &vaillantZoneSnapshot{}, semanticSnapshotSourceLive, nil)
	mergeDhwSnapshotFields(nil, &vaillantDhwSnapshot{}, semanticSnapshotSourceLive, nil)
	zone := &vaillantZoneSnapshot{Name: "retained"}
	dhw := &vaillantDhwSnapshot{Preset: "retained"}
	mergeZoneSnapshotFields(zone, nil, semanticSnapshotSourceLive, nil)
	mergeDhwSnapshotFields(dhw, nil, semanticSnapshotSourceLive, nil)
	if zone.FieldFreshness != nil || dhw.FieldFreshness != nil || zone.Name != "retained" || dhw.Preset != "retained" {
		t.Fatal("nil input changed snapshot")
	}
	mergeZoneSnapshotFields(zone, &vaillantZoneSnapshot{}, semanticSnapshotSourceLive, nil)
	mergeDhwSnapshotFields(dhw, &vaillantDhwSnapshot{}, semanticSnapshotSourceLive, nil)
	if zone.FieldFreshness == nil || dhw.FieldFreshness == nil {
		t.Fatal("missing freshness map initialization")
	}
	setFieldFreshness(nil, zoneFieldName, semanticSnapshotSourceLive, false)
	for _, source := range []semanticSnapshotSource{semanticSnapshotSourceCache, semanticSnapshotSourceLive} {
		for _, stale := range []bool{false, true} {
			fields := map[semanticFieldKey]semanticFieldFreshness{}
			setFieldFreshness(fields, zoneFieldName, source, stale)
			if fields[zoneFieldName] != (semanticFieldFreshness{Source: source, Stale: stale}) {
				t.Fatal("freshness assignment mismatch")
			}
		}
	}
}
