package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

// The synthetic node: device information at entity 0, the hot-water circuit at
// 1, heating zones at 2, 3 and 12, rooms at 5, 6 and 13, and the outdoor
// sensor at 8. Entities 12 and 13 sort after 3 and 6 only in numeric
// entity-address order, so a text-ordered pairing would number the zones
// differently.
const (
	eebusInboundTestDevice = "synthetic-device"
	eebusInboundTestSHIP   = "synthetic-ship"
)

var (
	eebusInboundTestZones = []uint64{2, 3, 12}
	eebusInboundTestRooms = []uint64{5, 6, 13}
)

type eebusInboundTestRuntime struct {
	msp05bRuntime
	mu           sync.Mutex
	snapshot     eebusruntime.SnapshotV1
	binding      eebusraw.RuntimeBindingV1
	values       map[string]any
	failValue    map[string]bool
	afterReads   int
	afterBinding eebusraw.RuntimeBindingV1
	reads        int
	featureReads int
}

func newEEBusInboundTestRuntime(t *testing.T) *eebusInboundTestRuntime {
	t.Helper()
	return &eebusInboundTestRuntime{
		snapshot: eebusInboundTestSnapshot(t, eebusruntime.ObservedRuntimeStateV1Ready),
		binding:  eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 3},
		values:   eebusInboundTestValues(),
	}
}

func (runtime *eebusInboundTestRuntime) Snapshot() (eebusruntime.SnapshotV1, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.snapshot.Clone(), nil
}

func (runtime *eebusInboundTestRuntime) setSnapshot(snapshot eebusruntime.SnapshotV1) {
	runtime.mu.Lock()
	runtime.snapshot = snapshot
	runtime.mu.Unlock()
}

func (runtime *eebusInboundTestRuntime) setBinding(binding eebusraw.RuntimeBindingV1) {
	runtime.mu.Lock()
	runtime.binding = binding
	runtime.mu.Unlock()
}

// changeBindingAfter switches the binding once the given number of function
// reads of the current cycle have been answered.
func (runtime *eebusInboundTestRuntime) changeBindingAfter(reads int, binding eebusraw.RuntimeBindingV1) {
	runtime.mu.Lock()
	runtime.afterReads, runtime.afterBinding, runtime.reads = reads, binding, 0
	runtime.mu.Unlock()
}

func (runtime *eebusInboundTestRuntime) failValueRead(entity uint64, featureType, function string) {
	runtime.mu.Lock()
	if runtime.failValue == nil {
		runtime.failValue = map[string]bool{}
	}
	runtime.failValue[eebusInboundTestKey(entity, featureType, function)] = true
	runtime.mu.Unlock()
}

func (runtime *eebusInboundTestRuntime) setValue(entity uint64, featureType, function string, value any) {
	runtime.mu.Lock()
	runtime.values[eebusInboundTestKey(entity, featureType, function)] = value
	runtime.mu.Unlock()
}

func (runtime *eebusInboundTestRuntime) FeaturesGet(_ context.Context, _ eebusraw.ReadAuthorizationV1, request eebusraw.FeaturesGetRequestV1) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.featureReads++
	locator := request.Target
	if len(locator.EntityAddress) != 1 {
		return eebusraw.FeaturesGetDataV1{}, eebusRuntimeUnavailableError()
	}
	var functions []eebusraw.FunctionDescriptorV1
	prefix := eebusInboundTestKey(locator.EntityAddress[0], locator.FeatureType, "")
	for key := range runtime.values {
		if strings.HasPrefix(key, prefix) {
			functions = append(functions, eebusraw.FunctionDescriptorV1{
				Function: strings.TrimPrefix(key, prefix), PossibleOperations: eebusraw.FullOperationsV1{Read: true},
				Changeable: eebusraw.ChangeabilityV1Unknown, Constraints: eebusraw.ConstraintSetV1{Status: eebusraw.ConstraintStatusV1Unknown},
			})
		}
	}
	sort.Slice(functions, func(left, right int) bool { return functions[left].Function < functions[right].Function })
	data := eebusraw.FeaturesGetDataV1{
		Feature: locator.Clone(), Functions: functions, Runtime: runtime.binding,
		DataTimestamp: eebusInboundTestTime(), Source: eebusraw.ObservationSourceV1Live,
	}
	var err error
	if data.DataHash, err = data.ComputeDataHash(); err != nil {
		panic(err)
	}
	return data, nil
}

func (runtime *eebusInboundTestRuntime) FeaturesDataGet(_ context.Context, _ eebusraw.ReadAuthorizationV1, request eebusraw.FeatureDataGetRequestV1) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	target := request.Targets[0]
	key := eebusInboundTestKey(target.EntityAddress[0], target.FeatureType, target.Function)
	raw, ok := runtime.values[key]
	if !ok || runtime.failValue[key] {
		return eebusraw.FeatureDataGetDataV1{}, eebusRuntimeUnavailableError()
	}
	runtime.reads++
	binding := runtime.binding
	if runtime.afterReads != 0 && runtime.reads > runtime.afterReads {
		binding = runtime.afterBinding
	}
	value, err := eebusraw.NewTypedValueV1(raw)
	if err != nil {
		panic(err)
	}
	now := eebusInboundTestTime()
	sum := sha256.Sum256([]byte("synthetic-inbound-binding"))
	observation := eebusraw.ReadObservationV1{
		Target: target, Runtime: binding,
		RawRequest:  eebusraw.ProtocolMessageV1{Classifier: "READ", CorrelationKey: 1, Function: target.Function},
		RawResponse: eebusraw.ProtocolMessageV1{Classifier: "REPLY", CorrelationKey: 1, Function: target.Function, Data: &value},
		Value:       value, RequestedAt: now, ReceivedAt: now.Add(time.Millisecond), DataTimestamp: now.Add(time.Millisecond),
		Source:    eebusraw.ObservationSourceV1Live,
		ReadToken: eebusraw.ReadTokenV1{ReadToken: strings.Repeat("E", 43), ExpiresAt: now.Add(time.Minute), BindingHash: eebusraw.HashV1(fmt.Sprintf("sha256:%x", sum))},
	}
	if observation.DataHash, err = observation.ComputeDataHash(); err != nil {
		panic(err)
	}
	return eebusraw.FeatureDataGetDataV1{Results: []eebusraw.ReadObservationV1{observation}, Complete: true}, nil
}

func eebusInboundTestTime() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

func eebusInboundTestKey(entity uint64, featureType, function string) string {
	return strconv.FormatUint(entity, 10) + "|" + featureType + "|" + function
}

func eebusInboundTestEntity(entity uint64) string {
	return eebusInboundTestDevice + ":[" + strconv.FormatUint(entity, 10) + "]:"
}

func eebusInboundTestSnapshot(t *testing.T, state eebusruntime.ObservedRuntimeStateV1) eebusruntime.SnapshotV1 {
	t.Helper()
	identity, err := eebusraw.RedactID(eebusraw.IDKindPeer, "synthetic-inbound-runtime")
	if err != nil {
		t.Fatal(err)
	}
	ship := eebusInboundTestSHIP
	snapshot := eebusruntime.SnapshotV1{
		Meta: eebusruntime.SnapshotMetaV1{Contract: eebusruntime.SnapshotContractV1, Runtime: identity,
			LocalSKI: strings.Repeat("b", 40), MaskTier: eebusraw.MaskTierRaw,
			CapturedAt: eebusInboundTestTime(), DataTimestamp: eebusInboundTestTime()},
		Status:  eebusruntime.RuntimeObservationV1{State: state},
		Devices: []eebusruntime.DeviceV1{{SKI: strings.Repeat("a", 40), SHIPID: &ship, Address: eebusInboundTestDevice, Type: "Generic"}},
	}
	add := func(entity uint64, entityType string, features ...string) {
		address := eebusInboundTestEntity(entity)
		snapshot.Entities = append(snapshot.Entities, eebusruntime.EntityV1{DeviceAddress: eebusInboundTestDevice, EntityAddress: address, Type: entityType})
		for index, featureType := range features {
			snapshot.Features = append(snapshot.Features, eebusruntime.FeatureV1{
				DeviceAddress: eebusInboundTestDevice, EntityAddress: address,
				FeatureAddress: address + strconv.Itoa(index+1), Type: featureType, Role: "server",
			})
		}
	}
	add(0, "DeviceInformation", "DeviceClassification")
	add(1, "DHWCircuit", "Measurement", "Setpoint", "HVAC")
	for _, zone := range eebusInboundTestZones {
		add(zone, "HeatingZone", "DeviceClassification")
	}
	for _, room := range eebusInboundTestRooms {
		add(room, "HVACRoom", "Measurement", "Setpoint", "HVAC")
	}
	add(8, "TemperatureSensor", "Measurement")
	if state == eebusruntime.ObservedRuntimeStateV1Degraded {
		snapshot.Status.Degradation = &eebusruntime.DegradationV1{Reason: eebusruntime.DegradationReasonV1RemoteDisconnect, Since: eebusInboundTestTime()}
	}
	snapshot, err = eebusruntime.NewSnapshotV1(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func eebusInboundScaled(number, scale int64) map[string]any {
	return map[string]any{"number": number, "scale": scale}
}

func eebusInboundTestValues() map[string]any {
	values := map[string]any{}
	set := func(entity uint64, featureType, function string, value any) {
		values[eebusInboundTestKey(entity, featureType, function)] = value
	}
	measurement := func(entity uint64, commodity, scope string, minimum, maximum, step map[string]any, number int64) {
		set(entity, "Measurement", "measurementDescriptionListData", map[string]any{"measurementDescriptionData": []any{map[string]any{
			"measurementId": int64(0), "commodityType": commodity, "measurementType": "temperature", "scopeType": scope, "unit": "degC",
		}}})
		set(entity, "Measurement", "measurementConstraintsListData", map[string]any{"measurementConstraintsData": []any{map[string]any{
			"measurementId": int64(0), "valueRangeMin": minimum, "valueRangeMax": maximum, "valueStepSize": step,
		}}})
		set(entity, "Measurement", "measurementListData", map[string]any{"measurementData": []any{map[string]any{
			"measurementId": int64(0), "value": eebusInboundScaled(number, -1),
		}}})
	}
	setpoint := func(entity uint64, scope string, minimum, maximum, step map[string]any, number int64) {
		set(entity, "Setpoint", "setpointDescriptionListData", map[string]any{"setpointDescriptionData": []any{map[string]any{
			"measurementId": int64(0), "setpointId": int64(1), "setpointType": "valueAbsolute", "scopeType": scope, "unit": "degC",
		}}})
		set(entity, "Setpoint", "setpointConstraintsListData", map[string]any{"setpointConstraintsData": []any{map[string]any{
			"setpointId": int64(1), "setpointRangeMin": minimum, "setpointRangeMax": maximum, "setpointStepSize": step,
		}}})
		set(entity, "Setpoint", "setpointListData", map[string]any{"setpointData": []any{map[string]any{
			"setpointId": int64(1), "value": eebusInboundScaled(number, -1),
		}}})
	}
	hvac := func(entity uint64, functionType string, mode int64, changeable, overrun bool) {
		set(entity, "HVAC", "hvacSystemFunctionDescriptionListData", map[string]any{"hvacSystemFunctionDescriptionData": []any{map[string]any{
			"systemFunctionId": int64(0), "systemFunctionType": functionType,
		}}})
		set(entity, "HVAC", "hvacOperationModeDescriptionListData", map[string]any{"hvacOperationModeDescriptionData": []any{
			map[string]any{"operationModeId": int64(0), "operationModeType": "auto"},
			map[string]any{"operationModeId": int64(1), "operationModeType": "on"},
			map[string]any{"operationModeId": int64(2), "operationModeType": "off"},
		}})
		set(entity, "HVAC", "hvacSystemFunctionOperationModeRelationListData", map[string]any{"hvacSystemFunctionOperationModeRelationData": []any{map[string]any{
			"systemFunctionId": int64(0), "operationModeId": []any{int64(0), int64(1), int64(2)},
		}}})
		set(entity, "HVAC", "hvacSystemFunctionListData", map[string]any{"hvacSystemFunctionData": []any{map[string]any{
			"systemFunctionId": int64(0), "currentOperationModeId": mode, "isOperationModeIdChangeable": changeable, "isOverrunActive": overrun,
		}}})
		if functionType == "dhw" {
			set(entity, "HVAC", "hvacOverrunDescriptionListData", map[string]any{"hvacOverrunDescriptionData": []any{map[string]any{
				"overrunId": int64(0), "overrunType": "oneTimeDhw", "affectedSystemFunctionId": []any{int64(0)},
			}}})
			set(entity, "HVAC", "hvacOverrunListData", map[string]any{"hvacOverrunData": []any{map[string]any{"overrunId": int64(0), "overrunStatus": "active"}}})
		}
	}
	set(0, "DeviceClassification", "deviceClassificationManufacturerData", map[string]any{"brandName": "SyntheticBrand", "vendorName": "SyntheticVendor"})
	measurement(1, "domesticHotWater", "dhwTemperature", eebusInboundScaled(0, -6), eebusInboundScaled(99, 0), eebusInboundScaled(1, 0), 485)
	setpoint(1, "dhwTemperature", eebusInboundScaled(35, 0), eebusInboundScaled(7, 1), eebusInboundScaled(1, 0), 500)
	hvac(1, "dhw", 1, true, true)
	for index, zone := range eebusInboundTestZones {
		set(zone, "DeviceClassification", "deviceClassificationUserData", map[string]any{"userLabel": "synthetic household label " + strconv.Itoa(index+1)})
	}
	for index, room := range eebusInboundTestRooms {
		number := int64(index + 1)
		measurement(room, "air", "roomAirTemperature", eebusInboundScaled(0, -6), eebusInboundScaled(6, 1), eebusInboundScaled(5, -1), 200+number)
		setpoint(room, "roomAirTemperature", eebusInboundScaled(5, 0), eebusInboundScaled(3, 1), eebusInboundScaled(5, -1), 210+number)
		hvac(room, "heating", number-1, number != 2, false)
	}
	measurement(8, "air", "outsideAirTemperature", eebusInboundScaled(-6, 1), eebusInboundScaled(8, 1), eebusInboundScaled(5, -1), 75)
	return values
}

// eebusInboundTestBase is an eBUS base provider with three zones and no
// values of its own.
func eebusInboundTestBase() *graphql.LiveSemanticProvider {
	base := graphql.NewLiveSemanticProvider()
	base.SetZones([]graphql.Zone{{ID: "zone-1", Name: "One"}, {ID: "zone-2", Name: "Two"}, {ID: "zone-3", Name: "Three"}})
	return base
}

func eebusInboundWait(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func eebusInboundFilled(provider graphql.SemanticProvider) bool {
	dhw, system, zones := provider.DHW(), provider.System(), provider.Zones()
	return dhw != nil && dhw.State.CurrentTempC != nil && system != nil && system.GatewayVendor != nil &&
		len(zones) == 3 && zones[2].Config.SourceLabel != nil
}

func eebusInboundEmpty(provider graphql.SemanticProvider) bool {
	dhw, system := provider.DHW(), provider.System()
	if (dhw != nil && dhw.State.CurrentTempC != nil) || (system != nil && system.GatewayBrand != nil) {
		return false
	}
	for _, zone := range provider.Zones() {
		if zone.State.CurrentTempC != nil || zone.Config.SourceLabel != nil {
			return false
		}
	}
	return true
}

func assertEEBusInboundGraphQL(t *testing.T, provider graphql.SemanticProvider) {
	t.Helper()
	dhw := provider.DHW()
	if dhw == nil || !eebusInboundFloat(dhw.State.CurrentTempC, 48.5) || !eebusInboundFloat(dhw.Config.TargetTempC, 50) ||
		dhw.Config.OperatingMode != "on" || !eebusInboundBool(dhw.Config.OperationModeChangeable, true) || !eebusInboundBool(dhw.State.OverrunActive, true) {
		t.Fatalf("hot-water values = %#v", dhw)
	}
	system := provider.System()
	if system == nil || !eebusInboundFloat(system.State.OutdoorTemperature, 7.5) ||
		system.GatewayBrand == nil || *system.GatewayBrand != "SyntheticBrand" || system.GatewayVendor == nil || *system.GatewayVendor != "SyntheticVendor" {
		t.Fatalf("system values = %#v", system)
	}
	zones := provider.Zones()
	if len(zones) != 3 {
		t.Fatalf("zones = %#v", zones)
	}
	for index, zone := range zones {
		number := index + 1
		mode := []string{"auto", "on", "off"}[index]
		if zone.ID != "zone-"+strconv.Itoa(number) || !eebusInboundFloat(zone.State.CurrentTempC, float64(200+number)/10) ||
			!eebusInboundFloat(zone.Config.TargetTempC, float64(210+number)/10) || zone.Config.OperatingMode != mode ||
			!eebusInboundBool(zone.Config.OperationModeChangeable, number != 2) ||
			zone.Config.SourceLabel == nil || *zone.Config.SourceLabel != "Zone "+strconv.Itoa(number) {
			t.Fatalf("zone %d values = %#v", number, zone)
		}
	}
}

func eebusInboundFloat(value *float64, want float64) bool { return value != nil && *value == want }
func eebusInboundBool(value *bool, want bool) bool        { return value != nil && *value == want }

func TestEEBusInboundGraphQLReturnsHotWaterSystemAndThreeZones(t *testing.T) {
	runtime := newEEBusInboundTestRuntime(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	builder := graphql.NewBuilder(nil, nil)
	composed := wireEEBusPromotedSemanticGraphQL(ctx, builder, eebusInboundTestBase(), &eebusRuntimeAdapter{runtime: runtime})
	eebusInboundWait(t, "the first inbound cycle", func() bool { return eebusInboundFilled(composed) })
	assertEEBusInboundGraphQL(t, composed)
}

func TestEEBusInboundNonNullEBusValueWins(t *testing.T) {
	runtime := newEEBusInboundTestRuntime(t)
	provider := newEEBusPromotedSemanticProvider(&eebusRuntimeAdapter{runtime: runtime})
	provider.Refresh(context.Background())
	base := eebusInboundTestBase()
	ebusTemp, ebusMode, ebusTarget := 19.25, "heat", 55.0
	base.SetZones([]graphql.Zone{{ID: "zone-1", State: graphql.ZoneState{CurrentTempC: &ebusTemp}, Config: graphql.ZoneConfig{OperatingMode: ebusMode}}, {ID: "zone-2"}, {ID: "zone-3"}})
	base.SetDHW(&graphql.DhwStatus{Config: graphql.DhwConfig{TargetTempC: &ebusTarget}})
	composed := graphql.NewPromotedSemanticProvider(base, provider.Overlay)
	zones := composed.Zones()
	if !eebusInboundFloat(zones[0].State.CurrentTempC, ebusTemp) || zones[0].Config.OperatingMode != ebusMode {
		t.Fatalf("eBUS zone value lost: %#v", zones[0])
	}
	if !eebusInboundFloat(zones[0].Config.TargetTempC, 21.1) || !eebusInboundFloat(zones[2].State.CurrentTempC, 20.3) {
		t.Fatalf("eeBUS value did not fill the missing fields: %#v", zones)
	}
	dhw := composed.DHW()
	if !eebusInboundFloat(dhw.Config.TargetTempC, ebusTarget) || !eebusInboundFloat(dhw.State.CurrentTempC, 48.5) {
		t.Fatalf("hot-water precedence = %#v", dhw)
	}
}

func TestEEBusInboundNotReadyRuntimeEmptiesOverlay(t *testing.T) {
	for _, state := range []eebusruntime.ObservedRuntimeStateV1{eebusruntime.ObservedRuntimeStateV1Starting, eebusruntime.ObservedRuntimeStateV1Degraded, eebusruntime.ObservedRuntimeStateV1Stopped} {
		t.Run(string(state), func(t *testing.T) {
			runtime := newEEBusInboundTestRuntime(t)
			provider := newEEBusPromotedSemanticProvider(&eebusRuntimeAdapter{runtime: runtime})
			composed := graphql.NewPromotedSemanticProvider(eebusInboundTestBase(), provider.Overlay)
			provider.Refresh(context.Background())
			assertEEBusInboundGraphQL(t, composed)
			runtime.setSnapshot(eebusInboundTestSnapshot(t, state))
			provider.Refresh(context.Background())
			if !reflect.DeepEqual(provider.Overlay(), graphql.PromotedSemanticOverlay{}) || !eebusInboundEmpty(composed) {
				t.Fatalf("not-ready cycle kept values: %#v", provider.Overlay())
			}
		})
	}
}

// Each field of the remote binding is varied on its own. A comparison that
// ignored RuntimeEpoch or ConnectionGeneration would keep the values.
func TestEEBusInboundBindingChangeDuringCycleEmptiesOverlay(t *testing.T) {
	base := eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 3}
	for _, test := range []struct {
		name    string
		changed eebusraw.RuntimeBindingV1
	}{
		{"runtime epoch", eebusraw.RuntimeBindingV1{RuntimeEpoch: 8, ConnectionGeneration: 3}},
		{"connection generation", eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := newEEBusInboundTestRuntime(t)
			provider := newEEBusPromotedSemanticProvider(&eebusRuntimeAdapter{runtime: runtime})
			provider.Refresh(context.Background())
			if provider.Overlay().DHW.CurrentTempC == nil {
				t.Fatal("first cycle filled no values")
			}
			if _, err := readEEBusInboundValues(context.Background(), runtime, runtime.snapshot); err != nil {
				t.Fatalf("stable binding rejected: %v", err)
			}
			runtime.setBinding(base)
			runtime.changeBindingAfter(4, test.changed)
			if _, err := readEEBusInboundValues(context.Background(), runtime, runtime.snapshot); err != errEEBusInboundBindingChanged {
				t.Fatalf("binding change error = %v", err)
			}
			runtime.changeBindingAfter(4, test.changed)
			provider.Refresh(context.Background())
			if !reflect.DeepEqual(provider.Overlay(), graphql.PromotedSemanticOverlay{}) {
				t.Fatalf("binding change kept values: %#v", provider.Overlay())
			}
		})
	}
}

func TestEEBusInboundReadReturnsObservationPerValue(t *testing.T) {
	runtime := newEEBusInboundTestRuntime(t)
	read, err := readEEBusInboundValues(context.Background(), runtime, runtime.snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if read.Binding != runtime.binding || len(read.Unread) != 0 {
		t.Fatalf("binding/unread = %#v/%v", read.Binding, read.Unread)
	}
	// Eight hot-water and system values and five per zone.
	if len(read.Values) != 8+5*3 {
		t.Fatalf("values = %d", len(read.Values))
	}
	paths := map[string]bool{}
	for _, value := range read.Values {
		if value.Observation.Runtime != runtime.binding || !value.Observation.DataTimestamp.Equal(eebusInboundTestTime().Add(time.Millisecond)) {
			t.Fatalf("%s observation = %#v", value.SemanticPath, value.Observation)
		}
		if strings.HasPrefix(value.SemanticPath, "/zones/") != (value.Zone != 0) {
			t.Fatalf("%s zone = %d", value.SemanticPath, value.Zone)
		}
		paths[value.SemanticPath] = true
	}
	for _, path := range []string{"/zones/zone_3/room_temperature_c", "/zones/zone_3/name", "/dhw/overrun_active", "/system/gateway_vendor"} {
		if !paths[path] {
			t.Fatalf("missing %s in %v", path, paths)
		}
	}
	// The third zone is the entity pair 12/13 in numeric address order.
	for _, value := range read.Values {
		if value.SemanticPath == "/zones/zone_3/room_temperature_c" && value.Observation.Target.EntityAddress[0] != 13 {
			t.Fatalf("zone 3 read entity %v", value.Observation.Target.EntityAddress)
		}
	}
}

func TestEEBusInboundAmbiguousPairingResolvesNoZone(t *testing.T) {
	runtime := newEEBusInboundTestRuntime(t)
	snapshot := runtime.snapshot.Clone()
	snapshot.Meta.DataHash = ""
	var entities []eebusruntime.EntityV1
	for _, entity := range snapshot.Entities {
		if entity.EntityAddress != eebusInboundTestEntity(13) {
			entities = append(entities, entity)
		}
	}
	var features []eebusruntime.FeatureV1
	for _, feature := range snapshot.Features {
		if feature.EntityAddress != eebusInboundTestEntity(13) {
			features = append(features, feature)
		}
	}
	snapshot.Entities, snapshot.Features = entities, features
	snapshot, err := eebusruntime.NewSnapshotV1(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	read, err := readEEBusInboundValues(context.Background(), runtime, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range read.Values {
		if value.Zone != 0 {
			t.Fatalf("ambiguous pairing produced %s", value.SemanticPath)
		}
	}
	if len(read.Values) != 8 {
		t.Fatalf("hot-water and system values = %d", len(read.Values))
	}
}

// The last-known value is kept only under the same remote binding. Each field
// of the binding is varied on its own; a comparison that ignored either field
// would carry a value across a reconnect.
func TestEEBusInboundFailedReadKeepsLastKnownValueOfSameBinding(t *testing.T) {
	for _, test := range []struct {
		name    string
		changed eebusraw.RuntimeBindingV1
	}{
		{"runtime epoch", eebusraw.RuntimeBindingV1{RuntimeEpoch: 9, ConnectionGeneration: 3}},
		{"connection generation", eebusraw.RuntimeBindingV1{RuntimeEpoch: 7, ConnectionGeneration: 9}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := newEEBusInboundTestRuntime(t)
			provider := newEEBusPromotedSemanticProvider(&eebusRuntimeAdapter{runtime: runtime})
			provider.Refresh(context.Background())
			runtime.failValueRead(8, "Measurement", "measurementListData")
			provider.Refresh(context.Background())
			if overlay := provider.Overlay(); !eebusInboundFloat(overlay.System.OutdoorTemperature, 7.5) || !eebusInboundFloat(overlay.DHW.CurrentTempC, 48.5) {
				t.Fatalf("failed read dropped the last-known value: %#v", overlay.System)
			}
			runtime.setBinding(test.changed)
			provider.Refresh(context.Background())
			if overlay := provider.Overlay(); overlay.System.OutdoorTemperature != nil || !eebusInboundFloat(overlay.DHW.CurrentTempC, 48.5) {
				t.Fatalf("value of a previous binding survived: %#v", overlay)
			}
		})
	}
}

func TestEEBusInboundProfileDriftWithdrawsValue(t *testing.T) {
	runtime := newEEBusInboundTestRuntime(t)
	provider := newEEBusPromotedSemanticProvider(&eebusRuntimeAdapter{runtime: runtime})
	provider.Refresh(context.Background())
	runtime.setValue(8, "Measurement", "measurementDescriptionListData", map[string]any{"measurementDescriptionData": []any{map[string]any{
		"measurementId": int64(0), "commodityType": "air", "measurementType": "temperature", "scopeType": "drifted", "unit": "degC",
	}}})
	provider.Refresh(context.Background())
	if overlay := provider.Overlay(); overlay.System.OutdoorTemperature != nil || !eebusInboundFloat(overlay.DHW.CurrentTempC, 48.5) {
		t.Fatalf("drifted profile kept its value: %#v", overlay)
	}
}
