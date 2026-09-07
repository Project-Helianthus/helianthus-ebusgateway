package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type regulatorCapabilityStatusProvider struct {
	capability string
}

func (provider regulatorCapabilityStatusProvider) DaemonStatus() ServiceStatus {
	return ServiceStatus{Status: "running"}
}

func (provider regulatorCapabilityStatusProvider) AdapterStatus() ServiceStatus {
	return ServiceStatus{Status: "unknown"}
}

func (provider regulatorCapabilityStatusProvider) VaillantRegulatorCapability() string {
	return provider.capability
}

func TestRuntimeStatusRegulatorCapabilityGoldenAndFailClosed(t *testing.T) {
	server, err := NewServer(&testRegistry{}, nil)
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}
	server.SetStatusProvider(regulatorCapabilityStatusProvider{capability: "PRESENT"})

	envelope := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.runtime.status.get","arguments":{}}`),
	}))
	meta, ok := envelope["meta"].(map[string]any)
	if !ok {
		t.Fatalf("envelope meta = %T; want object", envelope["meta"])
	}
	if timestamp, _ := meta["data_timestamp"].(string); timestamp == "" {
		t.Fatal("runtime status envelope data_timestamp is empty")
	}
	meta["data_timestamp"] = "<runtime>"

	got, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		t.Fatalf("Marshal runtime status: %v", err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "runtime_status_regulator_capability.golden.json"))
	if err != nil {
		t.Fatalf("ReadFile golden: %v", err)
	}
	if string(got)+"\n" != string(want) {
		t.Fatalf("runtime status golden mismatch\nwant:\n%s\ngot:\n%s", want, got)
	}

	server.SetStatusProvider(regulatorCapabilityStatusProvider{capability: "invalid"})
	invalid := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.runtime.status.get","arguments":{}}`),
	}))
	if got := invalid["data"].(map[string]any)["vaillant_regulator_capability"]; got != "UNKNOWN" {
		t.Fatalf("invalid capability = %#v; want UNKNOWN", got)
	}
}
