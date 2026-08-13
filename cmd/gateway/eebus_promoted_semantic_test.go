package main

import (
	"context"
	"strings"
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

func TestEEBusPromotedSemanticCanonicalProfileAdmissionRejectsDrift(t *testing.T) {
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	measurement, ok := registry.Candidate("m7-" + "candidate-0005")
	if !ok {
		t.Fatal("measurement candidate missing")
	}
	enumCandidate, ok := registry.Candidate("m7-" + "candidate-0007")
	if !ok {
		t.Fatal("enum candidate missing")
	}
	overrun, ok := registry.Candidate("m7-" + "candidate-0009")
	if !ok {
		t.Fatal("overrun candidate missing")
	}

	tests := []struct {
		name      string
		candidate promotioncapture.CandidateDefinition
		values    map[string]any
		omit      string
	}{
		{
			name:      "secondary value function missing",
			candidate: overrun,
			omit:      "hvacOverrunListData",
		},
		{
			name:      "descriptor drift",
			candidate: measurement,
			values: map[string]any{
				"measurementDescriptionListData": map[string]any{"measurementDescriptionData": []any{map[string]any{
					"measurementId": int64(0), "commodityType": "air", "measurementType": "temperature", "scopeType": "wrong", "unit": "degC",
				}}},
			},
		},
		{
			name:      "constraints drift",
			candidate: measurement,
			values: map[string]any{
				"measurementDescriptionListData": promotedMeasurementDescription("domesticHotWater", "dhwTemperature"),
				"measurementConstraintsListData": promotedMeasurementConstraints(-1),
			},
		},
		{
			name:      "enum description drift",
			candidate: enumCandidate,
			values: map[string]any{
				"hvacSystemFunctionDescriptionListData": map[string]any{"hvacSystemFunctionDescriptionData": []any{map[string]any{"systemFunctionId": int64(0), "systemFunctionType": "dhw"}}},
				"hvacOperationModeDescriptionListData": map[string]any{"hvacOperationModeDescriptionData": []any{
					map[string]any{"operationModeId": int64(0), "operationModeType": "auto"},
					map[string]any{"operationModeId": int64(1), "operationModeType": "on"},
					map[string]any{"operationModeId": int64(2), "operationModeType": "changed"},
				}},
				"hvacSystemFunctionOperationModeRelationListData": map[string]any{"hvacSystemFunctionOperationModeRelationData": []any{map[string]any{
					"systemFunctionId": int64(0), "operationModeId": []any{int64(0), int64(1), int64(2)},
				}}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			locator := promotedProfileLocator(test.candidate)
			inventory := promotedProfileInventory(test.candidate, locator, test.omit)
			runtime := &promotedProfileRuntime{binding: inventory.Runtime, values: test.values}
			reader := &leafPromotionLiveSource{eebus: runtime}
			if err := reader.verifySourceProfile(context.Background(), test.candidate, locator, inventory, map[string]eebusraw.ReadObservationV1{}); err == nil {
				t.Fatal("drifted source profile was admitted")
			}
		})
	}
}

func TestEEBusPromotedSemanticCanonicalProfileAdmissionAcceptsExactProfile(t *testing.T) {
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	candidate, ok := registry.Candidate("m7-" + "candidate-0005")
	if !ok {
		t.Fatal("measurement candidate missing")
	}
	locator := promotedProfileLocator(candidate)
	inventory := promotedProfileInventory(candidate, locator, "")
	runtime := &promotedProfileRuntime{binding: inventory.Runtime, values: map[string]any{
		"measurementDescriptionListData": promotedMeasurementDescription("domesticHotWater", "dhwTemperature"),
		"measurementConstraintsListData": promotedMeasurementConstraints(0),
	}}
	reader := &leafPromotionLiveSource{eebus: runtime}
	if err := reader.verifySourceProfile(context.Background(), candidate, locator, inventory, map[string]eebusraw.ReadObservationV1{}); err != nil {
		t.Fatalf("exact source profile rejected: %v", err)
	}
}

func promotedProfileLocator(candidate promotioncapture.CandidateDefinition) eebusraw.FeatureLocatorV1 {
	return eebusraw.FeatureLocatorV1{
		RemoteSKI: strings.Repeat("a", 40), SHIPID: "m9-profile-ship", DeviceAddress: "m9-profile-device",
		EntityAddress: []uint64{1}, FeatureAddress: 2, FeatureType: candidate.EEBusSource.FeatureType,
		FeatureRole: eebusraw.FeatureRoleV1Server,
	}
}

func promotedProfileInventory(candidate promotioncapture.CandidateDefinition, locator eebusraw.FeatureLocatorV1, omit string) eebusraw.FeaturesGetDataV1 {
	profile := candidate.EEBusSource
	functions := append([]string(nil), profile.DescriptionFunctions...)
	if profile.ConstraintsFunction != nil {
		functions = append(functions, *profile.ConstraintsFunction)
	}
	functions = append(functions, profile.ValueFunctions...)
	descriptors := make([]eebusraw.FunctionDescriptorV1, 0, len(functions))
	for _, function := range functions {
		if function != omit {
			descriptors = append(descriptors, eebusraw.FunctionDescriptorV1{Function: function, PossibleOperations: eebusraw.FullOperationsV1{Read: true}})
		}
	}
	return eebusraw.FeaturesGetDataV1{Feature: locator, Functions: descriptors, Runtime: eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 3}}
}

func promotedMeasurementDescription(commodity, scope string) map[string]any {
	return map[string]any{"measurementDescriptionData": []any{map[string]any{
		"measurementId": int64(0), "commodityType": commodity, "measurementType": "temperature", "scopeType": scope, "unit": "degC",
	}}}
}

func promotedMeasurementConstraints(minimum int64) map[string]any {
	return map[string]any{"measurementConstraintsData": []any{map[string]any{
		"measurementId": int64(0),
		"valueRangeMin": map[string]any{"number": minimum, "scale": int64(-6)},
		"valueRangeMax": map[string]any{"number": int64(99), "scale": int64(0)},
		"valueStepSize": map[string]any{"number": int64(1), "scale": int64(0)},
	}}}
}

type promotedProfileRuntime struct {
	binding eebusraw.RuntimeBindingV1
	values  map[string]any
}

func (*promotedProfileRuntime) Snapshot() (eebusruntime.SnapshotV1, error) {
	return eebusruntime.SnapshotV1{}, nil
}

func (*promotedProfileRuntime) FeaturesGet(context.Context, eebusraw.ReadAuthorizationV1, eebusraw.FeaturesGetRequestV1) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	return eebusraw.FeaturesGetDataV1{}, nil
}

func (runtime *promotedProfileRuntime) FeaturesDataGet(_ context.Context, _ eebusraw.ReadAuthorizationV1, request eebusraw.FeatureDataGetRequestV1) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	target := request.Targets[0]
	value, err := eebusraw.NewTypedValueV1(runtime.values[target.Function])
	if err != nil {
		panic(err)
	}
	now := time.Date(2026, 8, 13, 8, 0, 0, 0, time.UTC)
	observation := eebusraw.ReadObservationV1{
		Target: target, Runtime: runtime.binding,
		RawRequest:  eebusraw.ProtocolMessageV1{Classifier: "READ", CorrelationKey: 1, Function: target.Function},
		RawResponse: eebusraw.ProtocolMessageV1{Classifier: "REPLY", CorrelationKey: 1, Function: target.Function, Data: &value},
		Value:       value, RequestedAt: now, ReceivedAt: now.Add(time.Millisecond), DataTimestamp: now.Add(time.Millisecond), Source: eebusraw.ObservationSourceV1Live,
		ReadToken: eebusraw.ReadTokenV1{ReadToken: strings.Repeat("E", 43), ExpiresAt: now.Add(time.Minute), BindingHash: eebusraw.HashV1("sha256:" + strings.Repeat("1", 64))},
	}
	observation.DataHash, err = observation.ComputeDataHash()
	if err != nil {
		panic(err)
	}
	return eebusraw.FeatureDataGetDataV1{Results: []eebusraw.ReadObservationV1{observation}, Complete: true}, nil
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
