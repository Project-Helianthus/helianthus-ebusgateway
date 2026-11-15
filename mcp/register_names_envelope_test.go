package mcp

import (
	"encoding/json"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// TestSystemEnvelopesPublishCanonicalRegisterNames drives the real MCP
// envelope of ebus_v1_semantic_system_get and ebus_v1_semantic_snapshot_get
// with every renamed value populated, and requires the canonical names under
// state, config and properties and none of the previous names.
func TestSystemEnvelopesPublishCanonicalRegisterNames(t *testing.T) {
	f, i, b, s := 1.5, 2, true, "2026-01-01"
	system := &SystemStatus{
		State: &SystemState{SystemWaterPressure: &f, SystemFlowTemperature: &f},
		Config: &SystemConfig{
			AdaptiveHeatingCurve: &b, HeatingCircuitBivalencePoint: &f,
			MaxRoomHumidity: &i, InstallerMenuCode: &i, MaintenanceDate: &s,
		},
		Properties: &SystemProperties{SystemScheme: &i, ModuleConfigurationVR71: &i},
	}
	want := map[string][]string{
		"state":      {"water_pressure", "flow_temperature"},
		"config":     {"adaptive_heating_curve_enabled", "heating_outdoor_bivalence_temperature_limit", "room_humidity_limit_maximum", "installer_access_code", "maintenance_date"},
		"properties": {"hydraulic_scheme_code", "functional_module_configuration_code"},
	}
	old := []string{"system_water_pressure", "system_flow_temperature", "adaptive_heating_curve", "heating_circuit_bivalence_point", "max_room_humidity", "installer_menu_code", "system_scheme", "module_configuration_vr71"}
	calls := map[string]string{
		"system":   `{"name":"ebus_v1_semantic_system_get","arguments":{}}`,
		"snapshot": `{"name":"ebus_v1_semantic_snapshot_get","arguments":{"planes":["system"]}}`,
	}
	for name, params := range calls {
		server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		server.SetSemanticProvider(testSemanticProvider{system: system})
		env := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: json.RawMessage(params),
		}))
		data, _ := env["data"].(map[string]any)
		if name == "snapshot" {
			planes, _ := data["planes"].(map[string]any)
			data, _ = planes["system"].(map[string]any)
		}
		if data == nil {
			t.Fatalf("%s: no system data in envelope: %v", name, env)
		}
		for obj, keys := range want {
			got, _ := data[obj].(map[string]any)
			if got == nil {
				t.Fatalf("%s: object %q missing", name, obj)
			}
			for _, k := range keys {
				if _, ok := got[k]; !ok {
					t.Errorf("%s: %s.%s missing from envelope", name, obj, k)
				}
			}
			for _, k := range old {
				if _, ok := got[k]; ok {
					t.Errorf("%s: previous name %s.%s published", name, obj, k)
				}
			}
		}
	}
}
