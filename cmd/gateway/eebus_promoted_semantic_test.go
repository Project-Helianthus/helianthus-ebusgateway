package main

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

func TestApplyPromotedValue_MapsEveryPromotedRegistryPath(t *testing.T) {
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	overlay := graphql.PromotedSemanticOverlay{}
	seen := map[string]bool{}
	for _, candidate := range registry.Candidates() {
		if candidate.SemanticPath == nil || candidate.EEBusSource == nil || (candidate.ProtocolEligibility != promotioncapture.ProtocolCrossProtocol && candidate.ProtocolEligibility != promotioncapture.ProtocolEEBusNative) {
			continue
		}
		path := *candidate.SemanticPath
		applyPromotedValue(&overlay, path, promotedValueFor(candidate.ComparatorClass, path))
		seen[path] = true
	}
	if len(seen) != 18 {
		t.Fatalf("promoted registry paths = %d; want 18", len(seen))
	}
	assertPromotedRegistryOverlay(t, overlay)
}

func TestApplyPromotedValue_DecimalScaleIsNumberTimesTenToScale(t *testing.T) {
	overlay := graphql.PromotedSemanticOverlay{}
	applyPromotedValue(&overlay, "/dhw/temperature_c", promotioncapture.NumericValue(promotioncapture.Decimal{Number: 205, Scale: -1}))
	if overlay.DHW.CurrentTempC == nil || *overlay.DHW.CurrentTempC != 20.5 {
		t.Fatalf("negative-scale decimal = %#v; want 20.5", overlay.DHW.CurrentTempC)
	}
	applyPromotedValue(&overlay, "/dhw/target_temperature_c", promotioncapture.NumericValue(promotioncapture.Decimal{Number: 205, Scale: 1}))
	if overlay.DHW.TargetTempC == nil || *overlay.DHW.TargetTempC != 2050 {
		t.Fatalf("positive-scale decimal = %#v; want 2050", overlay.DHW.TargetTempC)
	}
}

func TestEEBusPromotedSemanticOverlayIsDeepCloned(t *testing.T) {
	temp, changeable, label := 20.5, true, "Zone 1"
	brand := "Vaillant"
	provider := &eebusPromotedSemanticProvider{overlay: graphql.PromotedSemanticOverlay{
		Zones:  map[string]graphql.PromotedZoneOverlay{"zone-1": {CurrentTempC: &temp, OperationModeChangeable: &changeable, SourceLabel: &label}},
		DHW:    graphql.PromotedDHWOverlay{CurrentTempC: &temp, OperationModeChangeable: &changeable},
		System: graphql.PromotedSystemOverlay{OutdoorTemperature: &temp, GatewayBrand: &brand},
	}}
	copy := provider.Overlay()
	*copy.Zones["zone-1"].CurrentTempC = 99
	copy.Zones["zone-2"] = graphql.PromotedZoneOverlay{}
	*copy.DHW.CurrentTempC = 98
	*copy.System.OutdoorTemperature = 97
	*copy.System.GatewayBrand = "changed"
	again := provider.Overlay()
	if len(again.Zones) != 1 || *again.Zones["zone-1"].CurrentTempC != 20.5 || *again.DHW.CurrentTempC != 20.5 || *again.System.OutdoorTemperature != 20.5 || *again.System.GatewayBrand != "Vaillant" {
		t.Fatalf("overlay aliases internal state: %#v", again)
	}
}

func TestEEBusPromotedSemanticStartDoesNotBlockInitialRefresh(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	provider := &eebusPromotedSemanticProvider{runtime: promotedRuntimeStub{snapshot: func() (eebusruntime.SnapshotV1, error) {
		close(entered)
		<-release
		return eebusruntime.SnapshotV1{}, nil
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { provider.Start(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Start blocked on initial refresh")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("initial refresh was not started")
	}
	close(release)
}

func promotedValueFor(class promotioncapture.ComparatorClass, path string) promotioncapture.TypedValue {
	switch class {
	case promotioncapture.ComparatorNumeric:
		return promotioncapture.NumericValue(promotioncapture.Decimal{Number: 205, Scale: -1})
	case promotioncapture.ComparatorEnum:
		return promotioncapture.EnumValue("auto")
	case promotioncapture.ComparatorBoolean:
		return promotioncapture.BooleanValue(true)
	case promotioncapture.ComparatorString:
		if path == "/zones/zone_1/name" || path == "/zones/zone_2/name" {
			return promotioncapture.StringValue("private household label")
		}
		return promotioncapture.StringValue("Vaillant")
	default:
		panic("unexpected comparator")
	}
}

func assertPromotedRegistryOverlay(t *testing.T, overlay graphql.PromotedSemanticOverlay) {
	t.Helper()
	if overlay.DHW.CurrentTempC == nil || *overlay.DHW.CurrentTempC != 20.5 || overlay.DHW.TargetTempC == nil || *overlay.DHW.TargetTempC != 20.5 || overlay.DHW.OperatingMode == nil || *overlay.DHW.OperatingMode != "auto" || overlay.DHW.OperationModeChangeable == nil || !*overlay.DHW.OperationModeChangeable || overlay.DHW.OverrunActive == nil || !*overlay.DHW.OverrunActive {
		t.Fatalf("dhw mapping = %#v", overlay.DHW)
	}
	if overlay.System.OutdoorTemperature == nil || *overlay.System.OutdoorTemperature != 20.5 || overlay.System.GatewayBrand == nil || *overlay.System.GatewayBrand != "Vaillant" || overlay.System.GatewayVendor == nil || *overlay.System.GatewayVendor != "Vaillant" {
		t.Fatalf("system mapping = %#v", overlay.System)
	}
	for _, want := range []struct{ id, label string }{{"zone-1", "Zone 1"}, {"zone-2", "Zone 2"}} {
		zone, ok := overlay.Zones[want.id]
		if !ok || zone.CurrentTempC == nil || *zone.CurrentTempC != 20.5 || zone.TargetTempC == nil || *zone.TargetTempC != 20.5 || zone.OperatingMode == nil || *zone.OperatingMode != "auto" || zone.OperationModeChangeable == nil || !*zone.OperationModeChangeable || zone.SourceLabel == nil || *zone.SourceLabel != want.label {
			t.Fatalf("%s mapping = %#v", want.id, zone)
		}
	}
}

type promotedRuntimeStub struct {
	snapshot func() (eebusruntime.SnapshotV1, error)
}

func (stub promotedRuntimeStub) Snapshot() (eebusruntime.SnapshotV1, error) { return stub.snapshot() }
func (promotedRuntimeStub) FeaturesGet(context.Context, eebusraw.ReadAuthorizationV1, eebusraw.FeaturesGetRequestV1) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	return eebusraw.FeaturesGetDataV1{}, nil
}
func (promotedRuntimeStub) FeaturesDataGet(context.Context, eebusraw.ReadAuthorizationV1, eebusraw.FeatureDataGetRequestV1) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	return eebusraw.FeatureDataGetDataV1{}, nil
}
