package main

import (
	"context"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
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

type recordingSemanticWriter struct {
	calls int
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

	admitted = true
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

func (adapter admittedMCPConfigAdapter) SetSystemConfig(ctx context.Context, field string, value string) mcp.ConfigSetResult {
	result := adapter.writer.SetSystemConfig(ctx, field, value)
	return mcp.ConfigSetResult{Success: result.Success, Error: result.Error}
}

func (adapter admittedMCPConfigAdapter) SetBoilerConfig(ctx context.Context, field string, value string) mcp.ConfigSetResult {
	result := adapter.writer.SetBoilerConfig(ctx, field, value)
	return mcp.ConfigSetResult{Success: result.Success, Error: result.Error}
}
