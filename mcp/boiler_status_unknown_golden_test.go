package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// TestSemanticBoilerStatusUnknownGoldenEnvelope pins the
// ebus.v1.semantic.boiler_status.get output (including data_hash) for the
// unknown-status case: the direct boiler read is unavailable, so the boiler
// central-heating pump and the heating-status diagnostic have no source and
// both keys are omitted. Circuit 0 is not a boiler source, so its pump status
// (RR=0x001B) and circuit state (RR=0x001E) never fill either key; before the
// change the diagnostic carried the circuit 0 state and the data_hash differed.
// The provider value is the exact payload the gateway adapter publishes in this
// case: present but empty state, config and diagnostics objects.
// Regenerate with UPDATE=1 go test ./mcp -run TestSemanticBoilerStatusUnknownGoldenEnvelope.
func TestSemanticBoilerStatusUnknownGoldenEnvelope(t *testing.T) {
	server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetSemanticProvider(testSemanticProvider{
		boiler: &BoilerStatus{
			State:       &BoilerState{},
			Config:      &BoilerConfig{},
			Diagnostics: &BoilerDiagnostics{},
		},
	})

	call := func() map[string]any {
		return envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      1,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.boiler_status.get","arguments":{}}`),
		}))
	}
	first, second := call(), call()
	firstMeta := first["meta"].(map[string]any)
	hash, _ := firstMeta["data_hash"].(string)
	if hash == "" {
		t.Fatalf("data_hash missing from envelope meta: %#v", firstMeta)
	}
	if hash != second["meta"].(map[string]any)["data_hash"] {
		t.Fatalf("data_hash differs across identical calls")
	}

	data, ok := first["data"].(map[string]any)
	if !ok {
		t.Fatalf("boiler data type = %T; want map", first["data"])
	}
	state, ok := data["state"].(map[string]any)
	if !ok {
		t.Fatalf("boiler state type = %T; want map", data["state"])
	}
	if _, present := state["central_heating_pump_active"]; present {
		t.Fatalf("boiler state.central_heating_pump_active = %v; want absent without a boiler source", state["central_heating_pump_active"])
	}
	diagnostics, ok := data["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("boiler diagnostics type = %T; want map", data["diagnostics"])
	}
	if _, present := diagnostics["heating_status_raw"]; present {
		t.Fatalf("boiler diagnostics.heating_status_raw = %v; want absent without a boiler source", diagnostics["heating_status_raw"])
	}

	firstMeta["data_timestamp"] = "2026-10-24T09:00:00Z"
	actual, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	path := filepath.Join("testdata", "semantic_boiler_status_unknown.golden.json")
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
		t.Fatalf("boiler status unknown envelope golden mismatch\nwant: %s\ngot:  %s", strings.TrimSpace(string(want)), actual)
	}
}
