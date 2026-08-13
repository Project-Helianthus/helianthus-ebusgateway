package graphql

import (
	"testing"

	graphqlgo "github.com/graphql-go/graphql"
)

func TestPromotedSemanticGraphQL_AllEighteenValuesFillOnlyMissing(t *testing.T) {
	base := NewLiveSemanticProvider()
	ebusTemp := 19.5
	base.SetZones([]Zone{{ID: "zone-1", Name: "Kitchen", State: ZoneState{CurrentTempC: &ebusTemp}}, {ID: "zone-2", Name: "Office"}})
	base.SetDHW(&DhwStatus{})
	base.SetSystem(&SystemStatus{})
	o := PromotedSemanticOverlay{Zones: map[string]PromotedZoneOverlay{
		"zone-1": {CurrentTempC: f(99), TargetTempC: f(21), OperatingMode: s("auto"), OperationModeChangeable: b(true), SourceLabel: s("Zone 1")},
		"zone-2": {CurrentTempC: f(20), TargetTempC: f(22), OperatingMode: s("heat"), OperationModeChangeable: b(false), SourceLabel: s("Zone 2")},
	}, DHW: PromotedDHWOverlay{CurrentTempC: f(48), TargetTempC: f(52), OperatingMode: s("auto"), OperationModeChangeable: b(true), OverrunActive: b(false)}, System: PromotedSystemOverlay{OutdoorTemperature: f(7), GatewayBrand: s("Vaillant"), GatewayVendor: s("Vaillant Group")}}
	builder := NewBuilder(nil, nil)
	builder.SetSemanticProvider(NewPromotedSemanticProvider(base, func() PromotedSemanticOverlay { return o }))
	schema, err := NewQuerySchema(builder)
	if err != nil {
		t.Fatal(err)
	}
	result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: `{ zones { id name state { current_temp_c } config { target_temp_c operating_mode operation_mode_changeable source_label } } dhw { state { current_temp_c overrun_active } config { target_temp_c operating_mode operation_mode_changeable } } system { gateway_brand gateway_vendor state { outdoor_temperature } } }`})
	if len(result.Errors) != 0 {
		t.Fatalf("query errors: %v", result.Errors)
	}
	data := result.Data.(map[string]any)
	zones := data["zones"].([]any)
	zone1 := zones[0].(map[string]any)
	zone2 := zones[1].(map[string]any)
	if zone1["id"] != "zone-1" || zone2["id"] != "zone-2" {
		t.Fatalf("zone identities drifted: %#v", zones)
	}
	if zone1["name"] != "Kitchen" || zone1["state"].(map[string]any)["current_temp_c"] != 19.5 {
		t.Fatalf("eBUS values overwritten: %#v", zone1)
	}
	assertPromotedFields(t, zone1, 21, "auto", true, "Zone 1")
	assertPromotedFields(t, zone2, 22, "heat", false, "Zone 2")
	dhw := data["dhw"].(map[string]any)
	ds := dhw["state"].(map[string]any)
	dc := dhw["config"].(map[string]any)
	if ds["current_temp_c"] != 48.0 || ds["overrun_active"] != false || dc["target_temp_c"] != 52.0 || dc["operating_mode"] != "auto" || dc["operation_mode_changeable"] != true {
		t.Fatalf("dhw = %#v", dhw)
	}
	system := data["system"].(map[string]any)
	if system["gateway_brand"] != "Vaillant" || system["gateway_vendor"] != "Vaillant Group" || system["state"].(map[string]any)["outdoor_temperature"] != 7.0 {
		t.Fatalf("system = %#v", system)
	}
}

func TestPromotedSemanticOverlayDoesNotCreateZonesOrSurviveClear(t *testing.T) {
	base := NewLiveSemanticProvider()
	base.SetZones([]Zone{{ID: "zone-1"}})
	o := PromotedSemanticOverlay{Zones: map[string]PromotedZoneOverlay{"zone-2": {SourceLabel: s("Zone 2")}}}
	provider := NewPromotedSemanticProvider(base, func() PromotedSemanticOverlay { return o })
	if zones := provider.Zones(); len(zones) != 1 || zones[0].ID != "zone-1" {
		t.Fatalf("overlay created or renamed zone: %#v", zones)
	}
	o = PromotedSemanticOverlay{}
	if zones := provider.Zones(); zones[0].Config.SourceLabel != nil {
		t.Fatalf("cleared overlay retained value: %#v", zones[0])
	}
}

func assertPromotedFields(t *testing.T, zone map[string]any, target float64, mode string, changeable bool, label string) {
	t.Helper()
	config := zone["config"].(map[string]any)
	if config["target_temp_c"] != target || config["operating_mode"] != mode || config["operation_mode_changeable"] != changeable || config["source_label"] != label {
		t.Fatalf("zone config = %#v", config)
	}
}
func f(v float64) *float64 { return &v }
func s(v string) *string   { return &v }
func b(v bool) *bool       { return &v }
