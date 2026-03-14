package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
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
