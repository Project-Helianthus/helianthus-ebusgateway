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

func (provider regulatorCapabilityStatusProvider) RegulatorCapability() string {
	return provider.capability
}

func TestRuntimeStatusRegulatorCapabilityGoldenAndFailClosed(t *testing.T) {
	server, err := NewServer(&testRegistry{}, nil)
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}
	server.SetStatusProvider(regulatorCapabilityStatusProvider{capability: "PRESENT"})

	got, err := json.MarshalIndent(server.runtimeStatus(nil), "", "  ")
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
	if got := server.runtimeStatus(nil)["regulator_capability"]; got != "UNKNOWN" {
		t.Fatalf("invalid capability = %#v; want UNKNOWN", got)
	}
}
