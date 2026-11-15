package mcp

import (
	"encoding/json"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// TestSemanticCircuitsSpecialFunctionModePublished pins the circuit
// configuration field circuit_special_function_mode on the MCP circuits tool:
// the raw one-byte value is published under that name only, and it changes the
// deterministic data_hash against the same circuit without it.
func TestSemanticCircuitsSpecialFunctionModePublished(t *testing.T) {
	call := func(config CircuitConfig) map[string]any {
		server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		server.SetSemanticProvider(testSemanticProvider{
			circuits:          []CircuitStatus{{Index: 0, CircuitType: "heating", Config: config}},
			circuitsPublished: true,
		})
		return envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      1,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus_v1_semantic_circuits_get","arguments":{}}`),
		}))
	}

	mode := 3
	with := call(CircuitConfig{SpecialFunctionMode: &mode})
	without := call(CircuitConfig{})

	data := with["data"].([]any)
	cfg := data[0].(map[string]any)["config"].(map[string]any)
	if got, ok := cfg["circuit_special_function_mode"].(float64); !ok || got != 3 {
		t.Fatalf("config = %#v; want circuit_special_function_mode=3", cfg)
	}
	if len(cfg) != 1 {
		t.Fatalf("config = %#v; want only circuit_special_function_mode", cfg)
	}
	if _, ok := cfg["frost_prot_c"]; ok {
		t.Fatalf("config = %#v; old field must be gone", cfg)
	}
	if with["meta"].(map[string]any)["data_hash"] == without["meta"].(map[string]any)["data_hash"] {
		t.Fatalf("data_hash does not reflect circuit_special_function_mode")
	}
}
