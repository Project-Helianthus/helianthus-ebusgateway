package mcp

import (
	"context"
	"fmt"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type teslaWCVitalsV1FixtureProvider struct {
	*modbusV1FixtureProvider
	result TeslaWCVitalsV1Result
	err    error
	calls  int
}

func (provider *teslaWCVitalsV1FixtureProvider) TeslaWCVitalsV1(context.Context) (TeslaWCVitalsV1Result, error) {
	provider.calls++
	return provider.result, provider.err
}

func TestTeslaWCVitalsV1MCPProjectsOnlyQualifiedRedactedReplay(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	provider := &teslaWCVitalsV1FixtureProvider{
		modbusV1FixtureProvider: &modbusV1FixtureProvider{},
		result: TeslaWCVitalsV1Result{
			Operation:        TeslaWCVitalsV1Operation,
			ProfileVersion:   TeslaWCVitalsV1ProfileVersion,
			OperationVersion: TeslaWCVitalsV1OperationVersion,
			Qualification:    TeslaWCVitalsV1QualificationQualified,
			ReplayKind:       TeslaWCVitalsV1ReplayTerminal,
			SnapshotLength:   2,
			SnapshotDigest:   "fb8da7eb5b1b399e7321179dac9e9f65773d7331e1e30554e3911e4325e1ef19",
			OutboundAllowed:  true,
		},
	}
	RegisterModbusV1Tools(server, provider)
	if !server.hasToolNamed(TeslaWCVitalsV1GetTool) {
		t.Fatal("Tesla WC vitals tool was not registered")
	}
	result := msp06Call(t, server.Handler(), TeslaWCVitalsV1GetTool, map[string]any{})
	if result.isError || provider.calls != 1 {
		t.Fatalf("result = %#v, provider calls = %d", result, provider.calls)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	if data["operation"] != TeslaWCVitalsV1Operation || data["profile_version"] != TeslaWCVitalsV1ProfileVersion ||
		data["operation_version"] != TeslaWCVitalsV1OperationVersion || data["qualification"] != TeslaWCVitalsV1QualificationQualified ||
		data["replay_kind"] != TeslaWCVitalsV1ReplayTerminal || fmt.Sprint(data["snapshot_length"]) != "2" ||
		data["outbound_allowed"] != false {
		t.Fatalf("data = %#v", data)
	}
	for _, forbidden := range []string{"raw", "bytes", "value", "vitals", "function", "request", "fc101", "fc102"} {
		if _, exists := data[forbidden]; exists {
			t.Fatalf("MCP data exposed %q: %#v", forbidden, data)
		}
	}
}

func TestTeslaWCVitalsV1MCPFailsClosedForWrongOperationOrVersion(t *testing.T) {
	for _, mutate := range []func(*TeslaWCVitalsV1Result){
		func(result *TeslaWCVitalsV1Result) { result.Operation = "unknown" },
		func(result *TeslaWCVitalsV1Result) { result.OperationVersion = "unknown" },
		func(result *TeslaWCVitalsV1Result) { result.Qualification = "framing_only" },
		func(result *TeslaWCVitalsV1Result) { result.ReplayKind = "fc101" },
	} {
		server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
		if err != nil {
			t.Fatal(err)
		}
		provider := &teslaWCVitalsV1FixtureProvider{
			modbusV1FixtureProvider: &modbusV1FixtureProvider{},
			result: TeslaWCVitalsV1Result{
				Operation: TeslaWCVitalsV1Operation, ProfileVersion: TeslaWCVitalsV1ProfileVersion,
				OperationVersion: TeslaWCVitalsV1OperationVersion, Qualification: TeslaWCVitalsV1QualificationQualified,
				ReplayKind: TeslaWCVitalsV1ReplayIntermediate, OutboundAllowed: false,
			},
		}
		mutate(&provider.result)
		RegisterModbusV1Tools(server, provider)
		result := msp06Call(t, server.Handler(), TeslaWCVitalsV1GetTool, map[string]any{})
		if !result.isError || result.envelope["data"] != nil || provider.calls != 1 {
			t.Fatalf("invalid provider result = %#v, calls = %d", result, provider.calls)
		}
	}
}
