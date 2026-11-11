package mcp

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type toolSchemaSignature struct {
	Name       string `json:"name"`
	SchemaHash string `json:"schema_hash"`
}

var canonicalSemanticSnapshotPlanes = []string{
	"runtime_status",
	"zones",
	"dhw",
	"energy_totals",
	"boiler_status",
	"system",
	"circuits",
	"radio_devices",
	"fm5_mode",
	"solar",
	"cylinders",
	"schedules",
	"adapter_info",
}

func TestToolInventoryGoldenSignatures(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	res := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list", Params: nil})
	if res.Error != nil {
		t.Fatalf("tools/list error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list result type = %T; want map", res.Result)
	}
	tools, ok := resultMap["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list tools type = %T; want []any", resultMap["tools"])
	}

	actual := make([]toolSchemaSignature, 0, len(tools))
	for _, raw := range tools {
		toolMap, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("tool entry type = %T; want map", raw)
		}
		name, _ := toolMap["name"].(string)
		schema := toolMap["inputSchema"]
		actual = append(actual, toolSchemaSignature{
			Name:       name,
			SchemaHash: hashData(schema),
		})
	}
	sort.Slice(actual, func(i, j int) bool { return actual[i].Name < actual[j].Name })

	// Pin schemas to the separately published descriptor contract, computing hashes from its exact schemas.
	var documented []map[string]any
	if err := json.Unmarshal(namingDocumentedTools(t, "## Current default registration contract"), &documented); err != nil {
		t.Fatal(err)
	}
	expected := make([]toolSchemaSignature, 0, len(documented))
	for _, tool := range documented {
		blob, err := json.Marshal(tool["inputSchema"])
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, toolSchemaSignature{Name: tool["name"].(string), SchemaHash: fmt.Sprintf("%x", sha256.Sum256(blob))})
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i].Name < expected[j].Name })

	if len(expected) == 0 {
		blob, _ := json.MarshalIndent(actual, "", "  ")
		t.Fatalf("golden signatures not initialized:\n%s", string(blob))
	}
	if !reflect.DeepEqual(actual, expected) {
		wantBlob, _ := json.MarshalIndent(expected, "", "  ")
		gotBlob, _ := json.MarshalIndent(actual, "", "  ")
		t.Fatalf("tool signature drift\nwant:\n%s\ngot:\n%s", string(wantBlob), string(gotBlob))
	}
}

func TestParityMatrixReadAndInvoke(t *testing.T) {
	boilerMode := "AUTO"
	plane := &testPlane{
		name: "heating",
		methods: []registry.Method{
			testMethod{name: "get_status", readOnly: true, template: testTemplate{primary: 0xB5, secondary: 0x04}},
		},
	}
	entry := testEntry{
		info: registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "vaillant",
			DeviceID:        "device-a",
			SoftwareVersion: "1.0",
			HardwareVersion: "7603",
		},
		planes: []registry.Plane{plane},
	}
	reg := &testRegistry{entries: map[byte]registry.DeviceEntry{0x08: entry}, order: []byte{0x08}}
	invoker := &testInvoker{}
	server, err := NewServer(reg, invoker)
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetAdmittedRPCSource(0x7F)
	server.SetStatusProvider(testStatusProvider{daemon: ServiceStatus{Status: "running"}, adapter: ServiceStatus{Status: "connected"}})
	server.SetSemanticProvider(testSemanticProvider{
		zones:    []Zone{{ID: "zone-a", Name: "Living", Config: ZoneConfig{OperatingMode: "AUTO", Preset: "COMFORT"}}},
		circuits: []CircuitStatus{{Index: 0, CircuitType: "heating", ManagingDevice: ManagingDevice{Role: "FUNCTION_MODULE", DeviceID: stringPtr("VR_71"), Address: intPtr(0x26)}}},
		radio:    []RadioDevice{{Group: 0x09, Instance: 1, DeviceModel: "VRC720"}},
		fm5Mode:  Fm5SemanticModeInterpreted,
		solar:    &SolarStatus{CollectorTemperatureC: floatPtr(70)},
		cylinders: []CylinderStatus{
			{Index: 0, TemperatureC: floatPtr(48)},
		},
		dhw:    &DhwStatus{Config: DhwConfig{OperatingMode: "AUTO", Preset: "ECO"}},
		energy: &EnergyTotals{Gas: EnergyChannel{DHW: EnergySeries{Today: 1.25}}},
		boiler: &BoilerStatus{Config: &BoilerConfig{DhwOperatingMode: &boilerMode}},
		system: &SystemStatus{Properties: &SystemProperties{SystemScheme: intPtr(8), ModuleConfigurationVR71: intPtr(2)}},
	})

	cases := []struct {
		name string
		tool string
		args string
	}{
		{name: "runtime", tool: toolRuntimeStatusGetName, args: `{}`},
		{name: "devices", tool: toolDevicesV1Name, args: `{}`},
		{name: "device", tool: toolDeviceGetV1Name, args: `{"address":8}`},
		{name: "planes", tool: toolPlanesListV1Name, args: `{"address":8}`},
		{name: "methods", tool: toolMethodsListV1Name, args: `{"address":8,"plane":"heating"}`},
		{name: "zones", tool: toolSemanticZonesGetName, args: `{}`},
		{name: "circuits", tool: toolSemanticCircuitsGetName, args: `{}`},
		{name: "radio", tool: toolSemanticRadioGetName, args: `{}`},
		{name: "fm5_mode", tool: toolSemanticFM5ModeGetName, args: `{}`},
		{name: "solar", tool: toolSemanticSolarGetName, args: `{}`},
		{name: "cylinders", tool: toolSemanticCylindersGetName, args: `{}`},
		{name: "dhw", tool: toolSemanticDHWGetName, args: `{}`},
		{name: "energy", tool: toolSemanticEnergyGetName, args: `{}`},
		{name: "boiler", tool: toolSemanticBoilerGetName, args: `{}`},
		{name: "system", tool: toolSemanticSystemGetName, args: `{}`},
		{name: "semantic_snapshot", tool: toolSemanticSnapshotName, args: `{}`},
		{name: "invoke", tool: toolInvokeV1Name, args: `{"address":8,"plane":"heating","method":"get_status","params":{},"intent":"READ_ONLY","allow_dangerous":false}`},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := doRPC(t, server.Handler(), rpcRequest{
				JSONRPC: "2.0",
				ID:      i + 1,
				Method:  "tools/call",
				Params:  json.RawMessage(`{"name":"` + tc.tool + `","arguments":` + tc.args + `}`),
			})
			envelope := envelopeFromResult(t, res)
			if envelope["error"] != nil {
				t.Fatalf("tool %s envelope.error = %#v; want nil", tc.tool, envelope["error"])
			}
			if envelope["data"] == nil {
				t.Fatalf("tool %s data is nil", tc.tool)
			}
			meta, ok := envelope["meta"].(map[string]any)
			if !ok {
				t.Fatalf("tool %s meta type = %T; want map", tc.tool, envelope["meta"])
			}
			if hash, _ := meta["data_hash"].(string); hash == "" {
				t.Fatalf("tool %s data_hash empty", tc.tool)
			}
			if tc.tool == toolSemanticSnapshotName {
				data, ok := envelope["data"].(map[string]any)
				if !ok {
					t.Fatalf("semantic snapshot data type = %T; want map", envelope["data"])
				}
				planes, ok := data["planes"].(map[string]any)
				if !ok {
					t.Fatalf("semantic snapshot planes type = %T; want map", data["planes"])
				}
				for _, plane := range canonicalSemanticSnapshotPlanes {
					if _, ok := planes[plane]; !ok {
						t.Fatalf("semantic snapshot missing plane %q: %#v", plane, planes)
					}
				}
			}
		})
	}
}
