package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// TestSemanticZonesAndDHWGoldenEnvelope pins the ebus_v1_semantic_zones_get and
// ebus_v1_semantic_dhw_get outputs (including data_hash) with every published
// zone and hot-water field populated, so a renamed or dropped key shows up.
// Regenerate with UPDATE=1 go test ./mcp -run TestSemanticZonesAndDHWGoldenEnvelope.
func TestSemanticZonesAndDHWGoldenEnvelope(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	n := func(v int) *int { return &v }
	zones := []Zone{{
		ID:   "zone-1",
		Name: "Zone 1",
		State: ZoneState{
			CurrentTempC: f(21.5), CurrentHumidityPct: f(45), HvacAction: "heating",
			SpecialFunction: "quick_veto", HeatingDemandPct: f(60), ValvePositionPct: f(55),
		},
		Config: ZoneConfig{
			OperatingMode: "auto", Preset: "schedule", TargetTempC: f(21),
			AllowedModes: []string{"auto", "manual"}, CircuitType: "heating",
			AssociatedCircuit: n(1), RoomTemperatureZoneMapping: n(2), QuickVeto: true,
			QuickVetoSetpointC: f(22), QuickVetoDurationH: f(3), QuickVetoExpiry: "2026-10-14T12:00:00Z",
			HolidayStartDate: "2026-12-20", HolidayEndDate: "2026-12-27", HolidaySetpointC: f(15),
			HolidayStartTime: "08:00", HolidayEndTime: "18:30",
		},
	}}
	dhw := &DhwStatus{
		State: DhwState{CurrentTempC: f(48), SpecialFunction: "load", HeatingDemandPct: f(100)},
		Config: DhwConfig{
			OperatingMode: "auto", Preset: "schedule", TargetTempC: f(55),
			HolidayStartDate: "2026-12-20", HolidayEndDate: "2026-12-27",
		},
	}

	out := map[string]any{}
	for _, tc := range []struct{ name, tool string }{
		{"zones", "ebus_v1_semantic_zones_get"},
		{"dhw", "ebus_v1_semantic_dhw_get"},
	} {
		server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		server.SetSemanticProvider(testSemanticProvider{zones: zones, zonesPublished: true, dhw: dhw})
		call := func() map[string]any {
			return envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
				JSONRPC: "2.0",
				ID:      1,
				Method:  "tools/call",
				Params:  json.RawMessage(`{"name":"` + tc.tool + `","arguments":{}}`),
			}))
		}
		first, second := call(), call()
		firstMeta := first["meta"].(map[string]any)
		if firstMeta["data_hash"] != second["meta"].(map[string]any)["data_hash"] {
			t.Fatalf("%s: data_hash differs across identical calls", tc.name)
		}
		firstMeta["data_timestamp"] = "2026-10-14T09:00:00Z"
		out[tc.name] = first
	}
	actual, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal envelopes: %v", err)
	}
	path := filepath.Join("testdata", "semantic_zones_dhw.golden.json")
	if os.Getenv("UPDATE") == "1" {
		if err := os.WriteFile(path, append(actual, '\n'), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v (run with UPDATE=1 to create)", err)
	}
	if string(actual) != strings.TrimSpace(string(want)) {
		t.Fatalf("zones and dhw envelope golden mismatch\nwant: %s\ngot:  %s", strings.TrimSpace(string(want)), actual)
	}
}
