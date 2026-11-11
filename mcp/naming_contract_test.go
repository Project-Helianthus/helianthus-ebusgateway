package mcp

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestNamingContractTools(t *testing.T) {
	seen := map[string]bool{}
	valid := regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	for _, tool := range defaultServerTools() {
		if !valid.MatchString(tool.Name) {
			t.Errorf("invalid tool %s", tool.Name)
		}
		if seen[tool.Name] {
			t.Errorf("duplicate %s", tool.Name)
		}
		seen[tool.Name] = true
	}
	for _, old := range []string{"ebus.invoke", "ebus.devices", "ebus.v1.rpc.invoke"} {
		if seen[old] {
			t.Errorf("old alias registered %s", old)
		}
	}
	if !seen["ebus_v1_rpc_invoke"] {
		t.Fatal("versioned invoke missing")
	}
}

func TestNamingContractRejectedToolsNoDispatch(t *testing.T) {
	invoker := &testInvoker{}
	server, err := NewServer(&testRegistry{}, invoker)
	if err != nil {
		t.Fatal(err)
	}
	for _, old := range []string{"ebus.invoke", "ebus.devices", "ebus.v1.rpc.invoke"} {
		response := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: json.RawMessage(`{"name":"` + old + `","arguments":{}}`)})
		if response.Error == nil {
			t.Errorf("accepted %s", old)
		}
	}
	if len(invoker.calls) != 0 {
		t.Fatal("old tool performed native dispatch")
	}
	if out := os.Getenv("NAMING_TOOLS_OUTPUT"); out != "" {
		raw, err := json.Marshal(server.tools)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(out, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNamingContractRawCommandIdentityBoundary(t *testing.T) {
	for _, spec := range eebusV1CommandSpecs {
		public := strings.ReplaceAll(string(spec.tool), ".", "_")
		got, ok := eebusV1CommandSpecForName(public)
		if !ok || got.tool != spec.tool || got.scope != spec.scope {
			t.Fatal("native authorization identity changed")
		}
		if _, ok := eebusV1CommandSpecForName(string(spec.tool)); ok {
			t.Fatal("old raw command alias accepted")
		}
	}
	for _, tool := range eebusV1CommandTools() {
		if strings.Contains(tool.Name, ".") {
			t.Fatal("dotted raw command registration")
		}
	}
}
func TestNamingContractEEBusDescriptorProof(t *testing.T) {
	s, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "synthetic")})
	if out := os.Getenv("NAMING_EEBUS_OUTPUT"); out != "" {
		b, e := json.Marshal(issue749EEBusTools(t, s.Handler()))
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(out, b, 0600); e != nil {
			t.Fatal(e)
		}
	}
}
func TestNamingContractZonePayload(t *testing.T) {
	zero, humidity := 0.0, 42.5
	b, e := json.Marshal(ZoneState{CurrentTempC: &zero, CurrentHumidityPct: &humidity})
	if e != nil {
		t.Fatal(e)
	}
	var data map[string]any
	if e = json.Unmarshal(b, &data); e != nil {
		t.Fatal(e)
	}
	if data["zone_room_temperature"] != float64(0) || data["zone_room_humidity"] != humidity {
		t.Fatal("zone quantities changed")
	}
	for _, old := range []string{"current_temp_c", "current_humidity_pct"} {
		if _, ok := data[old]; ok {
			t.Fatal("old zone alias emitted")
		}
	}
	b, e = json.Marshal(ZoneState{})
	if e != nil || string(b) != "{}" {
		t.Fatal("unknown zone quantities fabricated")
	}
}
