package mcp

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type toolSchemaSignature struct {
	Name       string `json:"name"`
	SchemaHash string `json:"schema_hash"`
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

	expected := []toolSchemaSignature{
		{Name: "ebus.devices", SchemaHash: "c88e6ce24faaf24f31d9f934af950368a2dd0561e547df2089c7c44195d76389"},
		{Name: "ebus.invoke", SchemaHash: "2e6b78e869aed69fa5d7fb3722c476a3d2b04767a1f2d9e0e64e8bd456d072f7"},
		{Name: "ebus.v1.registry.devices.get", SchemaHash: "da78278884d60ad8b0f1d272acde9ea9aa0d407993039a2b39dbf584d95f0757"},
		{Name: "ebus.v1.registry.devices.list", SchemaHash: "c88e6ce24faaf24f31d9f934af950368a2dd0561e547df2089c7c44195d76389"},
		{Name: "ebus.v1.registry.methods.list", SchemaHash: "97aa07b78405c1f77e2c3845a083b088db13019ca0a3ea83dba4206088d3bcf8"},
		{Name: "ebus.v1.registry.planes.list", SchemaHash: "da78278884d60ad8b0f1d272acde9ea9aa0d407993039a2b39dbf584d95f0757"},
		{Name: "ebus.v1.rpc.invoke", SchemaHash: "163d33b397ffcb2e1374d6d7dc388352d7b565726577c3d0e710b4c2dc78cdb9"},
		{Name: "ebus.v1.runtime.status.get", SchemaHash: "c88e6ce24faaf24f31d9f934af950368a2dd0561e547df2089c7c44195d76389"},
		{Name: "ebus.v1.semantic.boiler_status.get", SchemaHash: "c88e6ce24faaf24f31d9f934af950368a2dd0561e547df2089c7c44195d76389"},
		{Name: "ebus.v1.semantic.circuits.get", SchemaHash: "c88e6ce24faaf24f31d9f934af950368a2dd0561e547df2089c7c44195d76389"},
		{Name: "ebus.v1.semantic.dhw.get", SchemaHash: "c88e6ce24faaf24f31d9f934af950368a2dd0561e547df2089c7c44195d76389"},
		{Name: "ebus.v1.semantic.energy_totals.get", SchemaHash: "c88e6ce24faaf24f31d9f934af950368a2dd0561e547df2089c7c44195d76389"},
		{Name: "ebus.v1.semantic.snapshot.get", SchemaHash: "f40827c9c88a3e7e1e5588a409e0282e27fefa5df9a29eb74a911c8ad75aeb4c"},
		{Name: "ebus.v1.semantic.zones.get", SchemaHash: "c88e6ce24faaf24f31d9f934af950368a2dd0561e547df2089c7c44195d76389"},
		{Name: "ebus.v1.snapshot.capture", SchemaHash: "cd1a463c46d6264134447db17a8c3c7abe5b9a2488c6d759fea66da1f96b133e"},
		{Name: "ebus.v1.snapshot.drop", SchemaHash: "3f2e89bf3346421daa820ec5cd5bf9b4a06815aebd32c0bd8758b90043d69f88"},
	}

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
	server.SetStatusProvider(testStatusProvider{daemon: ServiceStatus{Status: "running"}, adapter: ServiceStatus{Status: "connected"}})
	server.SetSemanticProvider(testSemanticProvider{
		zones:    []Zone{{ID: "zone-a", Name: "Living", Config: ZoneConfig{OperatingMode: "AUTO", Preset: "COMFORT"}}},
		circuits: []CircuitStatus{{Index: 0, CircuitType: "heating"}},
		dhw:      &DhwStatus{Config: DhwConfig{OperatingMode: "AUTO", Preset: "ECO"}},
		energy:   &EnergyTotals{Gas: EnergyChannel{DHW: EnergySeries{Today: 1.25}}},
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
		{name: "dhw", tool: toolSemanticDHWGetName, args: `{}`},
		{name: "energy", tool: toolSemanticEnergyGetName, args: `{}`},
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
		})
	}
}
