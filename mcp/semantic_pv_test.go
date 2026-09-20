package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type semanticPVFixtureProvider struct{ *modbusV1FixtureProvider }

func (*semanticPVFixtureProvider) SemanticPVCurrent(context.Context, string, string) (SemanticPVCurrentResult, error) {
	return SemanticPVCurrentResult{Evaluated: "2026-09-09T10:00:00.123456789Z", Data: public05SemanticPVData()}, nil
}

func public05SemanticPVData() map[string]any {
	powerKey := map[string]any{"fact_id": "pv.ac.aggregate_active_power"}
	frequencyKey := map[string]any{"fact_id": "pv.ac.frequency"}
	powerCandidate := map[string]any{
		"candidate_id": "candidate:power",
		"value": map[string]any{"quantity": map[string]any{
			"number": map[string]any{"coefficient": "43215", "exponent10": -1},
			"unit":   "unit.watt",
		}},
	}
	return map[string]any{
		"snapshot": map[string]any{"snapshot_id": "snapshot:semantic-pv", "facts": []any{
			map[string]any{"key": powerKey, "candidates": []any{powerCandidate}},
			map[string]any{"key": frequencyKey, "candidates": []any{map[string]any{"candidate_id": "candidate:frequency-retained"}}},
		}},
		"evaluation": map[string]any{"facts": []any{
			map[string]any{"candidate_id": "candidate:power", "freshness": "fresh", "effective_availability": "available"},
			map[string]any{"candidate_id": "candidate:frequency-retained", "freshness": "stale", "effective_availability": "unavailable"},
		}},
		"selections": []any{map[string]any{"key": powerKey, "selected_candidate_id": "candidate:power"}},
		"projection": map[string]any{"manifest": map[string]any{"target_id": "target:gateway-semantic-pv"}, "dispositions": []any{
			map[string]any{"item_id": "projection.gateway.pv.inverter.ac.power.active", "outcome": "exact"},
			map[string]any{"item_id": "projection.gateway.pv.inverter.ac.frequency", "outcome": "withheld", "reason": "mapping.field_invalid"},
		}},
	}
}

type missingSemanticPVFixtureProvider struct{}

func (*missingSemanticPVFixtureProvider) RawRead(context.Context, ModbusRawReadRequest) (ModbusRawReadResult, error) {
	return ModbusRawReadResult{}, nil
}

func (*missingSemanticPVFixtureProvider) ProfileObservation(context.Context, string, string) (ModbusProfileObservationResult, error) {
	return ModbusProfileObservationResult{}, nil
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
	data := msp06Map(t, result.envelope["data"], "data")
	selections := msp06Slice(t, data["selections"], "data.selections")
	if len(selections) != 1 || msp06Map(t, msp06Map(t, selections[0], "selection")["key"], "selection.key")["fact_id"] != "pv.ac.aggregate_active_power" {
		t.Fatalf("semantic PV selections=%#v; invalid frequency must not be selected", selections)
	}
	dispositions := msp06Slice(t, msp06Map(t, data["projection"], "projection")["dispositions"], "projection.dispositions")
	if len(dispositions) != 2 || msp06Map(t, dispositions[1], "frequency disposition")["reason"] != "mapping.field_invalid" {
		t.Fatalf("semantic PV dispositions=%#v", dispositions)
	}
}

func TestModbusV1CompositionRequiresCallableSemanticPVProvider(t *testing.T) {
	missing := &missingSemanticPVFixtureProvider{}
	if _, ok := any(missing).(ModbusV1Provider); ok {
		t.Fatal("provider without SemanticPVCurrent satisfies the migrated Modbus V1 contract")
	}

	var provider ModbusV1Provider = &semanticPVFixtureProvider{&modbusV1FixtureProvider{}}
	server, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, provider)
	if !server.hasToolNamed(SemanticV1PVCurrentGetTool) {
		t.Fatal("composed provider did not advertise mandatory semantic PV tool")
	}
	result := msp06Call(t, server.Handler(), SemanticV1PVCurrentGetTool, map[string]any{"profile_id": "sunspec.inverter.three_phase.monitoring@1.0.0", "sample_id": "sample:1"})
	if result.isError {
		t.Fatalf("advertised semantic PV tool was not callable: %#v", result)
	}
}
