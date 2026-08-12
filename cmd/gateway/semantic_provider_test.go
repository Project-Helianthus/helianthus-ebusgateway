package main

import (
	"context"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/m8sourcestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

func TestSemanticEnergyAdaptersExposeNeverSeenShapeWithoutPoints(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()

	graphTotals := provider.EnergyTotals()
	if graphTotals == nil {
		t.Fatal("provider.EnergyTotals() = nil; want visible no-data energy shape")
	}
	if got := graphTotals.Gas.DHW.TodayMeta.FreshnessState; got != graphql.EnergyFreshnessStateNeverSeen {
		t.Fatalf("provider gas.dhw.todayMeta.freshnessState = %q; want never_seen", got)
	}
	if got := graphTotals.Gas.DHW.TodayMeta.Provenance; got != graphql.EnergyProvenanceNone {
		t.Fatalf("provider gas.dhw.todayMeta.provenance = %q; want none", got)
	}

	mcpTotals := newMCPSemanticProvider(provider).EnergyTotals()
	if mcpTotals == nil {
		t.Fatal("mcp adapter EnergyTotals() = nil; want visible no-data energy shape")
	}
	if got := mcpTotals.Gas.DHW.TodayMeta.FreshnessState; got != "never_seen" {
		t.Fatalf("mcp gas.dhw.today_meta.freshness_state = %q; want never_seen", got)
	}
	if got := mcpTotals.Gas.DHW.TodayMeta.Provenance; got != "none" {
		t.Fatalf("mcp gas.dhw.today_meta.provenance = %q; want none", got)
	}

	portalTotals := mapPortalEnergyTotals(provider.EnergyTotals())
	if portalTotals == nil {
		t.Fatal("portal mapper returned nil; want visible no-data energy shape")
	}
	if got := portalTotals.Gas.DHW.TodayMeta.FreshnessState; got != "never_seen" {
		t.Fatalf("portal gas.dhw.today_meta.freshness_state = %q; want never_seen", got)
	}
	if got := portalTotals.Gas.DHW.TodayMeta.Provenance; got != "none" {
		t.Fatalf("portal gas.dhw.today_meta.provenance = %q; want none", got)
	}
}

func TestMCPSemanticProviderAdapterPreservesEmptyCylinders(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	adapter := mcpSemanticProviderAdapter{provider: provider}

	if got := adapter.Cylinders(); got != nil {
		t.Fatalf("Cylinders() before publish = %#v; want nil", got)
	}

	provider.SetCylinders([]graphql.CylinderStatus{})
	if got := adapter.Cylinders(); got == nil || len(got) != 0 {
		t.Fatalf("Cylinders() after empty publish = %#v; want empty non-nil slice", got)
	}

	provider.SetCylinders(nil)
	if got := adapter.Cylinders(); got != nil {
		t.Fatalf("Cylinders() after nil publish = %#v; want nil", got)
	}
}

func TestMCPSemanticProviderAdapterPreservesEmptyZones(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	adapter := mcpSemanticProviderAdapter{provider: provider}

	if got := adapter.Zones(); got != nil {
		t.Fatalf("Zones() before publish = %#v; want nil", got)
	}

	provider.SetZones([]graphql.Zone{})
	if got := adapter.Zones(); got == nil || len(got) != 0 {
		t.Fatalf("Zones() after empty publish = %#v; want empty non-nil slice", got)
	}
}

func TestMCPSemanticProviderAdapterM8InventoryTracksMaterializedOwnerState(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	adapter := mcpSemanticProviderAdapter{provider: provider}

	before, err := adapter.M8SemanticRegistryState()
	if err != nil {
		t.Fatal(err)
	}
	for _, leaf := range before.Leaves {
		if leaf.Path == "/zones/0/id" {
			t.Fatalf("unpublished zone appeared in M8 inventory: %#v", before.Leaves)
		}
	}

	provider.SetZones([]graphql.Zone{{ID: "owner-zone"}})
	after, err := adapter.M8SemanticRegistryState()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, leaf := range after.Leaves {
		if leaf.Path == "/zones/0/id" && leaf.Source == "ebus" && leaf.PromotionState == "PROMOTED" {
			found = true
		}
	}
	if !found {
		t.Fatalf("materialized zone missing from M8 inventory: %#v", after.Leaves)
	}
}

func TestMCPSemanticProviderAdapterPreservesEmptyCircuits(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	adapter := mcpSemanticProviderAdapter{provider: provider}

	if got := adapter.Circuits(); got != nil {
		t.Fatalf("Circuits() before publish = %#v; want nil", got)
	}

	provider.SetCircuits([]graphql.CircuitStatus{})
	if got := adapter.Circuits(); got == nil || len(got) != 0 {
		t.Fatalf("Circuits() after empty publish = %#v; want empty non-nil slice", got)
	}
}

func TestMCPSemanticProviderAdapterPreservesEmptyRadioDevices(t *testing.T) {
	provider := graphql.NewLiveSemanticProvider()
	adapter := mcpSemanticProviderAdapter{provider: provider}

	if got := adapter.RadioDevices(); got != nil {
		t.Fatalf("RadioDevices() before publish = %#v; want nil", got)
	}

	provider.SetRadioDevices([]graphql.RadioDevice{})
	if got := adapter.RadioDevices(); got == nil || len(got) != 0 {
		t.Fatalf("RadioDevices() after empty publish = %#v; want empty non-nil slice", got)
	}

	provider.SetRadioDevices(nil)
	if got := adapter.RadioDevices(); got != nil {
		t.Fatalf("RadioDevices() after nil publish = %#v; want nil", got)
	}
}

type recordingSemanticWriter struct {
	calls int
}

func (*recordingSemanticWriter) M8CommandRoutingState() (m8sourcestate.CommandRoutingFragment, error) {
	return m8sourcestate.CommandRoutingFragment{Routes: []m8sourcestate.CommandRoute{
		{SemanticPath: "/mcp/ebus.v1.semantic.schedules.set_dhw_time_program", Source: "ebus", Available: true},
		{SemanticPath: "/mcp/ebus.v1.semantic.schedules.set_zone_time_program", Source: "ebus", Available: true},
	}}, nil
}

func (writer *recordingSemanticWriter) SetBoilerConfig(context.Context, string, string) graphql.BoilerConfigMutationResult {
	writer.calls++
	return graphql.BoilerConfigMutationResult{Success: true}
}

func (writer *recordingSemanticWriter) SetSystemConfig(context.Context, string, string) graphql.ConfigMutationResult {
	writer.calls++
	return graphql.ConfigMutationResult{Success: true}
}

func (writer *recordingSemanticWriter) SetZoneTimeProgram(context.Context, int, int, []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	writer.calls++
	return &mcp.TimeProgramWriteResult{Success: true}, nil
}

func (writer *recordingSemanticWriter) SetDhwTimeProgram(context.Context, int, []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	writer.calls++
	return &mcp.TimeProgramWriteResult{Success: true}, nil
}

func TestAdmittedSemanticWritersBlockUntilSourceAdmitted(t *testing.T) {
	admitted := false
	provider := func() (byte, bool) {
		if !admitted {
			return 0, false
		}
		return 0x7F, true
	}
	underlying := &recordingSemanticWriter{}

	graphWriter := admittedGraphQLSemanticWriter{
		boiler:   underlying,
		system:   underlying,
		schedule: underlying,
		admitted: provider,
	}
	mcpConfigWriter := admittedMCPConfigWriter{
		writer:   admittedMCPConfigAdapter{writer: underlying},
		admitted: provider,
	}
	mcpScheduleWriter := admittedMCPScheduleWriter{
		writer:   underlying,
		admitted: provider,
	}

	if result := graphWriter.SetSystemConfig(context.Background(), "x", "1"); result.Success || result.Error != semanticWriterSourceNotAdmittedError {
		t.Fatalf("graph system pre-admission = %+v; want source-not-admitted error", result)
	}
	if result := graphWriter.SetBoilerConfig(context.Background(), "x", "1"); result.Success || result.Error != semanticWriterSourceNotAdmittedError {
		t.Fatalf("graph boiler pre-admission = %+v; want source-not-admitted error", result)
	}
	if result, err := graphWriter.SetZoneTimeProgram(context.Background(), 0, 0, []mcp.TimeProgramSlot{{StartHour: 6, EndHour: 7}}); err != nil || result.Success || result.Error != semanticWriterSourceNotAdmittedError {
		t.Fatalf("graph schedule pre-admission = %+v err=%v; want source-not-admitted error", result, err)
	}
	if result := mcpConfigWriter.SetSystemConfig(context.Background(), "x", "1"); result.Success || result.Error != semanticWriterSourceNotAdmittedError {
		t.Fatalf("mcp config pre-admission = %+v; want source-not-admitted error", result)
	}
	if result, err := mcpScheduleWriter.SetDhwTimeProgram(context.Background(), 0, []mcp.TimeProgramSlot{{StartHour: 6, EndHour: 7}}); err != nil || result.Success || result.Error != semanticWriterSourceNotAdmittedError {
		t.Fatalf("mcp schedule pre-admission = %+v err=%v; want source-not-admitted error", result, err)
	}
	if underlying.calls != 0 {
		t.Fatalf("underlying calls before admission = %d; want 0", underlying.calls)
	}
	configRoutes, err := mcpConfigWriter.M8CommandRoutingState()
	if err != nil {
		t.Fatal(err)
	}
	if routes := configRoutes.Routes; len(routes) != 2 || routes[0].Available || routes[1].Available {
		t.Fatalf("pre-admission config routes = %#v; want unavailable owner routes", routes)
	}
	scheduleRoutes, err := mcpScheduleWriter.M8CommandRoutingState()
	if err != nil {
		t.Fatal(err)
	}
	if routes := scheduleRoutes.Routes; len(routes) != 2 || routes[0].Available || routes[1].Available {
		t.Fatalf("pre-admission schedule routes = %#v; want unavailable owner routes", routes)
	}

	admitted = true
	configRoutes, err = mcpConfigWriter.M8CommandRoutingState()
	if err != nil {
		t.Fatal(err)
	}
	if routes := configRoutes.Routes; len(routes) != 2 || !routes[0].Available || !routes[1].Available {
		t.Fatalf("admitted config routes = %#v; want available owner routes", routes)
	}
	scheduleRoutes, err = mcpScheduleWriter.M8CommandRoutingState()
	if err != nil {
		t.Fatal(err)
	}
	if routes := scheduleRoutes.Routes; len(routes) != 2 || !routes[0].Available || !routes[1].Available {
		t.Fatalf("admitted schedule routes = %#v; want available owner routes", routes)
	}
	if result := graphWriter.SetSystemConfig(context.Background(), "x", "1"); !result.Success {
		t.Fatalf("graph system after admission = %+v; want success", result)
	}
	if result := mcpConfigWriter.SetBoilerConfig(context.Background(), "x", "1"); !result.Success {
		t.Fatalf("mcp boiler after admission = %+v; want success", result)
	}
	if result, err := mcpScheduleWriter.SetZoneTimeProgram(context.Background(), 0, 0, []mcp.TimeProgramSlot{{StartHour: 6, EndHour: 7}}); err != nil || !result.Success {
		t.Fatalf("mcp schedule after admission = %+v err=%v; want success", result, err)
	}
	if underlying.calls != 3 {
		t.Fatalf("underlying calls after admission = %d; want 3", underlying.calls)
	}
}

type admittedMCPConfigAdapter struct {
	writer *recordingSemanticWriter
}

func (adapter admittedMCPConfigAdapter) M8CommandRoutingState() (m8sourcestate.CommandRoutingFragment, error) {
	return m8sourcestate.CommandRoutingFragment{Routes: []m8sourcestate.CommandRoute{
		{SemanticPath: "/mcp/ebus.v1.semantic.boiler_status.set_config", Source: "ebus", Available: true},
		{SemanticPath: "/mcp/ebus.v1.semantic.system.set_config", Source: "ebus", Available: true},
	}}, nil
}

func (adapter admittedMCPConfigAdapter) SetSystemConfig(ctx context.Context, field string, value string) mcp.ConfigSetResult {
	result := adapter.writer.SetSystemConfig(ctx, field, value)
	return mcp.ConfigSetResult{Success: result.Success, Error: result.Error}
}

func (adapter admittedMCPConfigAdapter) SetBoilerConfig(ctx context.Context, field string, value string) mcp.ConfigSetResult {
	result := adapter.writer.SetBoilerConfig(ctx, field, value)
	return mcp.ConfigSetResult{Success: result.Success, Error: result.Error}
}
