package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func TestMapPortalPromotedSemantic_AllLeavesFillOnlyMissingAndClear(t *testing.T) {
	base := graphql.NewLiveSemanticProvider()
	base.SetZones([]graphql.Zone{{ID: "zone-1", Name: "Kitchen"}, {ID: "zone-2", Name: "Office"}})
	base.SetDHW(&graphql.DhwStatus{})
	base.SetSystem(&graphql.SystemStatus{})
	overlay := promotedPortalOverlay()
	provider := graphql.NewPromotedSemanticProvider(base, func() graphql.PromotedSemanticOverlay { return overlay })

	zones := mapPortalZones(provider.Zones())
	if len(zones) != 2 || zones[0].ID != "zone-1" || zones[0].Name != "Kitchen" || *zones[0].State.CurrentTempC != 99 {
		t.Fatalf("portal zone promotion/identity=%#v", zones)
	}
	assertPortalPromotedZone(t, zones[0], 21, "auto", true, "Zone 1")
	assertPortalPromotedZone(t, zones[1], 22, "heat", false, "Zone 2")
	dhw := mapPortalDHW(provider.DHW())
	if dhw == nil || *dhw.State.CurrentTempC != 48 || *dhw.Config.TargetTempC != 52 || dhw.Config.OperatingMode != "auto" || *dhw.Config.OperationModeChangeable != true || *dhw.State.OverrunActive != false {
		t.Fatalf("portal dhw promoted fields=%#v", dhw)
	}
	system := mapPortalSystemStatus(provider.System())
	if system == nil || *system.State.OutdoorTemperature != 7 || *system.GatewayBrand != "Vaillant" || *system.GatewayVendor != "Vaillant Group" {
		t.Fatalf("portal system promoted fields=%#v", system)
	}

	baseTemp := 19.5
	base.SetZones([]graphql.Zone{{ID: "zone-1", Name: "Kitchen", State: graphql.ZoneState{CurrentTempC: &baseTemp}}, {ID: "zone-2", Name: "Office"}})
	zones = mapPortalZones(provider.Zones())
	if *zones[0].State.CurrentTempC != baseTemp || zones[0].ID != "zone-1" || zones[0].Name != "Kitchen" {
		t.Fatalf("base zone value/identity overwritten: %#v", zones[0])
	}

	overlay = graphql.PromotedSemanticOverlay{}
	zones = mapPortalZones(provider.Zones())
	if zones[0].Config.TargetTempC != nil || zones[0].Config.OperationModeChangeable != nil || zones[0].Config.SourceLabel != nil || zones[1].Config.TargetTempC != nil {
		t.Fatalf("disconnect retained overlay values: %#v", zones)
	}
	if *zones[0].State.CurrentTempC != baseTemp || zones[0].ID != "zone-1" || zones[0].Name != "Kitchen" {
		t.Fatalf("disconnect changed base zone: %#v", zones[0])
	}
}

func promotedPortalOverlay() graphql.PromotedSemanticOverlay {
	return graphql.PromotedSemanticOverlay{Zones: map[string]graphql.PromotedZoneOverlay{
		"zone-1": {CurrentTempC: promotedPortalFloat(99), TargetTempC: promotedPortalFloat(21), OperatingMode: promotedPortalString("auto"), OperationModeChangeable: promotedPortalBool(true), SourceLabel: promotedPortalString("Zone 1")},
		"zone-2": {CurrentTempC: promotedPortalFloat(20), TargetTempC: promotedPortalFloat(22), OperatingMode: promotedPortalString("heat"), OperationModeChangeable: promotedPortalBool(false), SourceLabel: promotedPortalString("Zone 2")},
	}, DHW: graphql.PromotedDHWOverlay{CurrentTempC: promotedPortalFloat(48), TargetTempC: promotedPortalFloat(52), OperatingMode: promotedPortalString("auto"), OperationModeChangeable: promotedPortalBool(true), OverrunActive: promotedPortalBool(false)}, System: graphql.PromotedSystemOverlay{OutdoorTemperature: promotedPortalFloat(7), GatewayBrand: promotedPortalString("Vaillant"), GatewayVendor: promotedPortalString("Vaillant Group")}}
}

func promotedPortalFloat(value float64) *float64 { return &value }
func promotedPortalBool(value bool) *bool        { return &value }
func promotedPortalString(value string) *string  { return &value }

func assertPortalPromotedZone(t *testing.T, zone portal.SemanticZone, target float64, mode string, changeable bool, label string) {
	t.Helper()
	if zone.Config.TargetTempC == nil || *zone.Config.TargetTempC != target || zone.Config.OperatingMode != mode || zone.Config.OperationModeChangeable == nil || *zone.Config.OperationModeChangeable != changeable || zone.Config.SourceLabel == nil || *zone.Config.SourceLabel != label {
		t.Fatalf("portal zone promoted fields=%#v", zone)
	}
}
