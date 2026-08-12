package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

func TestIssue790ResolveStaticNativeEntitySlots(t *testing.T) {
	snapshot := eebusruntime.SnapshotV1{Entities: []eebusruntime.EntityV1{
		{DeviceAddress: "device", EntityAddress: "device:[0]:", Type: "DeviceInformation"},
		{DeviceAddress: "device", EntityAddress: "device:[1]:", Type: "DHWCircuit"},
		{DeviceAddress: "device", EntityAddress: "device:[2]:", Type: "HeatingZone"},
		{DeviceAddress: "device", EntityAddress: "device:[3]:", Type: "HeatingZone"},
		{DeviceAddress: "device", EntityAddress: "device:[4]:", Type: "HVACRoom"},
		{DeviceAddress: "device", EntityAddress: "device:[5]:", Type: "HVACRoom"},
		{DeviceAddress: "device", EntityAddress: "device:[6]:", Type: "TemperatureSensor"},
	}}
	slots, err := leafPromotionResolveSlots(snapshot)
	if err != nil {
		t.Fatalf("leafPromotionResolveSlots: %v", err)
	}
	want := map[string]string{
		"device_information": "DeviceInformation",
		"zone_1":             "HeatingZone",
		"zone_2":             "HeatingZone",
	}
	for slot, entityType := range want {
		if slots.byName[slot].Type != entityType {
			t.Fatalf("slot %s = %#v, want %s", slot, slots.byName[slot], entityType)
		}
	}
}

func TestIssue790SelectDirectMetadataFields(t *testing.T) {
	for field, want := range map[string]string{
		"brandName":  "Vaillant",
		"vendorName": "Vaillant Group",
		"userLabel":  "Living Room",
	} {
		got, err := leafPromotionSelectField(map[string]any{field: want}, field)
		if err != nil {
			t.Fatalf("leafPromotionSelectField(%s): %v", field, err)
		}
		if got != want {
			t.Fatalf("leafPromotionSelectField(%s) = %#v, want %q", field, got, want)
		}
	}
}

func TestIssue790DecodeNativeMetadataAsString(t *testing.T) {
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		t.Fatalf("DefaultRegistry: %v", err)
	}
	candidate, ok := registry.Candidate("m7-candidate-0019")
	if !ok {
		t.Fatal("metadata candidate missing")
	}
	raw, value, unit, err := leafPromotionDecodeEEBus(candidate, map[string]any{"brandName": "Vaillant"})
	if err != nil {
		t.Fatalf("leafPromotionDecodeEEBus: %v", err)
	}
	if raw.Kind != promotioncapture.ValueString || raw.String == nil || *raw.String != "Vaillant" ||
		value.String == nil || *value.String != "Vaillant" || unit != nil {
		t.Fatalf("decoded metadata = raw %#v value %#v unit %#v", raw, value, unit)
	}
}

func TestIssue790DecodeB555FallbackWithoutB524Relabel(t *testing.T) {
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		t.Fatalf("DefaultRegistry: %v", err)
	}
	candidate, ok := registry.Candidate("m7-candidate-0006")
	if !ok || candidate.EBusFallback == nil {
		t.Fatal("candidate 0006 fallback missing")
	}
	identity, err := promotioncapture.NewB555Identity(*candidate.EBusFallback, 0x15, 0xfd)
	if err != nil {
		t.Fatalf("NewB555Identity: %v", err)
	}
	raw, value, unit, err := leafPromotionDecodeEBus(candidate, identity, []byte{0x26, 0x02})
	if err != nil {
		t.Fatalf("leafPromotionDecodeEBus: %v", err)
	}
	want := promotioncapture.Decimal{Number: 55, Scale: 0}
	if identity.Family != "B555" || raw.Decimal == nil || *raw.Decimal != want || value.Decimal == nil || *value.Decimal != want ||
		unit == nil || *unit != "degC" {
		t.Fatalf("B555 decoded identity=%+v raw=%#v value=%#v unit=%#v", identity, raw, value, unit)
	}
}
