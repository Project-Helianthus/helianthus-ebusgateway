package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type semanticPVFixtureProvider struct{ *modbusV1FixtureProvider }

func (*semanticPVFixtureProvider) SemanticPVCurrent(context.Context, string, string) (SemanticPVCurrentResult, error) {
	return SemanticPVCurrentResult{Evaluated: "2026-09-09T10:00:00.123456789Z", Data: map[string]any{"snapshot": map[string]any{"snapshot_id": "snapshot:semantic-pv"}, "projection": map[string]any{"manifest": map[string]any{"target_id": "target:gateway-semantic-pv"}}}}, nil
}

func TestSemanticPVToolReplacesLegacyCanonicalPVTool(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, &semanticPVFixtureProvider{&modbusV1FixtureProvider{}})
	if server.hasToolNamed("modbus.v1.semantic.pv.get") || !server.hasToolNamed(SemanticV1PVCurrentGetTool) {
		t.Fatal("SemReg PV tool replacement is incomplete")
	}
	result := msp06Call(t, server.Handler(), SemanticV1PVCurrentGetTool, map[string]any{"profile_id": "sunspec.inverter.three_phase.monitoring@1.0.0", "sample_id": "sample:1"})
	if result.isError {
		t.Fatalf("semantic PV call=%#v", result)
	}
	meta := msp06Map(t, result.envelope["meta"], "meta")
	if timestamp, ok := meta["data_timestamp"].(string); !ok || timestamp != "2026-09-09T10:00:00.123456789Z" {
		t.Fatalf("semantic PV timestamp=%#v", meta["data_timestamp"])
	} else if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		t.Fatalf("semantic PV timestamp parse: %v", err)
	}
}
