package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// TestSemanticCircuitsStateGoldenEnvelope pins the ebus.v1.semantic.circuits.get
// output (including data_hash) for every circuit_state token the gateway can
// publish from B524 OP02 GG02 RR=0x001E: standby, heating, cooling, dhw (raw 3,
// previously published as unknown_3) and a raw unmapped code kept as unknown_N.
// Regenerate with UPDATE=1 go test ./mcp -run TestSemanticCircuitsStateGoldenEnvelope.
func TestSemanticCircuitsStateGoldenEnvelope(t *testing.T) {
	states := []string{"standby", "heating", "cooling", "dhw", "unknown_7"}
	circuits := make([]CircuitStatus, 0, len(states))
	for i, state := range states {
		pump := state == "heating" || state == "dhw"
		circuits = append(circuits, CircuitStatus{
			Index:       i,
			CircuitType: "heating",
			State:       CircuitState{PumpActive: &pump, CircuitState: state},
		})
	}

	server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetSemanticProvider(testSemanticProvider{circuits: circuits, circuitsPublished: true})

	call := func() map[string]any {
		return envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      1,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.circuits.get","arguments":{}}`),
		}))
	}
	first, second := call(), call()
	firstMeta := first["meta"].(map[string]any)
	if firstMeta["data_hash"] != second["meta"].(map[string]any)["data_hash"] {
		t.Fatalf("data_hash differs across identical calls")
	}
	firstMeta["data_timestamp"] = "2026-10-14T09:00:00Z"

	actual, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	path := filepath.Join("testdata", "semantic_circuits_state.golden.json")
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
		t.Fatalf("circuits state envelope golden mismatch\nwant: %s\ngot:  %s", strings.TrimSpace(string(want)), actual)
	}
}
