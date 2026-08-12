package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

func TestIssue784LiveNumericDecodersPreserveProtocolGranularity(t *testing.T) {
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		t.Fatalf("DefaultRegistry: %v", err)
	}
	candidate, ok := registry.Candidate("m7-" + "candidate-0005")
	if !ok {
		t.Fatal("numeric candidate missing")
	}

	identity, err := promotioncapture.NewB524Identity(*candidate.EBusSelector, 0xfd)
	if err != nil {
		t.Fatalf("NewB524Identity: %v", err)
	}
	_, ebusValue, ebusUnit, err := leafPromotionDecodeEBus(candidate, identity, []byte{0x00, 0x40, 0x1c, 0x42})
	if err != nil {
		t.Fatalf("leafPromotionDecodeEBus: %v", err)
	}
	if ebusValue.Decimal == nil || *ebusValue.Decimal != (promotioncapture.Decimal{Number: 390625, Scale: -4}) ||
		ebusUnit == nil || *ebusUnit != "degC" {
		t.Fatalf("eBUS decoded value/unit = %+v/%v", ebusValue, ebusUnit)
	}

	spine := map[string]any{
		"measurementData": []any{map[string]any{
			"measurementId": int64(0),
			"value":         map[string]any{"number": int64(39), "scale": int64(0)},
		}},
	}
	_, eebusValue, eebusUnit, err := leafPromotionDecodeEEBus(candidate, spine)
	if err != nil {
		t.Fatalf("leafPromotionDecodeEEBus: %v", err)
	}
	if eebusValue.Decimal == nil || *eebusValue.Decimal != (promotioncapture.Decimal{Number: 39, Scale: 0}) ||
		eebusUnit == nil || *eebusUnit != "degC" {
		t.Fatalf("eeBUS decoded value/unit = %+v/%v", eebusValue, eebusUnit)
	}

	comparison, err := promotioncapture.CompareNumeric(
		*ebusValue.Decimal,
		*eebusValue.Decimal,
		*candidate.EEBusSource.DeclaredConstraints,
		*candidate.Conversion,
	)
	if err != nil {
		t.Fatalf("CompareNumeric: %v", err)
	}
	if !comparison.Match || comparison.Delta != (promotioncapture.Decimal{Number: 625, Scale: -4}) {
		t.Fatalf("comparison = %+v; want inclusive declared-step MATCH", comparison)
	}
}

func TestIssue784LiveLocatorComparisonIncludesEntityPath(t *testing.T) {
	base := eebusraw.FeatureLocatorV1{
		RemoteSKI: "remote", SHIPID: "ship", DeviceAddress: "device",
		EntityAddress: []uint64{4, 1, 1}, FeatureAddress: 11,
		FeatureType: "Measurement", FeatureRole: eebusraw.FeatureRoleV1Server,
	}
	clone := base.Clone()
	if !leafPromotionLocatorEqual(base, clone) {
		t.Fatal("equal locators rejected")
	}
	clone.EntityAddress[2] = 2
	if leafPromotionLocatorEqual(base, clone) {
		t.Fatal("different entity path accepted")
	}
}

func TestIssue784B524ReaderRejectsDifferentDiscoveredController(t *testing.T) {
	poller := &vaillantSemanticPoller{controller: 0x26}
	reader := leafPromotionSemanticB524Reader{poller: poller}
	if payload, _, ok := reader.ReadB524(context.Background(), promotioncapture.EBusSelector{TargetAddress: 0x15}); ok || payload != nil {
		t.Fatalf("different-controller read = %x/%t; want fail closed", payload, ok)
	}
}

func TestIssue784ScaledIntegerRejectsFractionalJSONNumber(t *testing.T) {
	if _, ok := leafPromotionSignedInteger(json.Number("1.5")); ok {
		t.Fatal("fractional SPINE integer accepted")
	}
}
