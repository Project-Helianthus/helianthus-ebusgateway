package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// TestSemanticPopulatedPlanesGoldenEnvelope pins the circuits tool and the
// aggregate snapshot tool with every published zone, circuit and hot-water
// field populated, so each renamed key and the resulting data_hash is reviewed.
// Regenerate with UPDATE=1 go test ./mcp -run TestSemanticPopulatedPlanesGoldenEnvelope.
func TestSemanticPopulatedPlanesGoldenEnvelope(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	n := func(v int) *int { return &v }
	b := func(v bool) *bool { return &v }
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
	circuits := []CircuitStatus{{
		Index:       0,
		CircuitType: "heating",
		HasMixer:    true,
		State: CircuitState{
			PumpActive: b(true), MixerPositionPct: f(40), FlowTemperatureC: f(35.5),
			FlowSetpointC: f(36), CalcFlowTempC: f(36.5), CircuitState: "heating",
			Humidity: f(50), DewPoint: f(12), PumpHours: f(1234), PumpStarts: n(321),
		},
		Config: CircuitConfig{
			HeatingCurve: f(1.2), FlowTempMaxC: f(55), FlowTempMinC: f(20), SummerLimitC: f(18),
			SpecialFunctionMode: n(1), RoomTempControl: "modulating", CoolingEnabled: b(true),
		},
	}}

	out := map[string]any{}
	for _, tc := range []struct{ name, tool, args string }{
		{"circuits", "ebus_v1_semantic_circuits_get", `{}`},
		{"snapshot", "ebus_v1_semantic_snapshot_get", `{"planes":["zones","dhw","circuits"]}`},
	} {
		server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		server.SetSemanticProvider(testSemanticProvider{
			zones: zones, zonesPublished: true, dhw: dhw,
			circuits: circuits, circuitsPublished: true,
		})
		call := func() map[string]any {
			return envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
				JSONRPC: "2.0",
				ID:      1,
				Method:  "tools/call",
				Params:  json.RawMessage(`{"name":"` + tc.tool + `","arguments":` + tc.args + `}`),
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
	path := filepath.Join("testdata", "semantic_populated_planes.golden.json")
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
		t.Fatalf("populated planes envelope golden mismatch\nwant: %s\ngot:  %s", strings.TrimSpace(string(want)), actual)
	}
}
