package mcp

import (
	"context"
	"fmt"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type teslaFC100SummaryV1FixtureProvider struct {
	*modbusV1FixtureProvider
	result TeslaFC100SummaryV1Result
	err    error
}

func (provider *teslaFC100SummaryV1FixtureProvider) TeslaFC100SummaryV1(context.Context) (TeslaFC100SummaryV1Result, error) {
	return provider.result, provider.err
}

func TestTeslaFC100SummaryV1MCPProjectsOnlyBoundedStructuralProvenance(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	provider := &teslaFC100SummaryV1FixtureProvider{
		modbusV1FixtureProvider: &modbusV1FixtureProvider{},
		result: TeslaFC100SummaryV1Result{
			Qualification:  TeslaFC100SummaryV1QualificationFramingOnly,
			EnvelopeLength: 8,
			MessageLength:  7,
			EntryCount:     2,
			Entries: []TeslaFC100SummaryV1WireEntry{
				{FieldNumber: 1, WireType: 0},
				{FieldNumber: 2, WireType: 2},
			},
			PayloadDigest:   "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			OutboundAllowed: true,
		},
	}
	RegisterModbusV1Tools(server, provider)
	if !server.hasToolNamed(TeslaFC100SummaryV1GetTool) {
		t.Fatal("Tesla FC100 summary tool was not registered")
	}
	result := msp06Call(t, server.Handler(), TeslaFC100SummaryV1GetTool, map[string]any{})
	if result.isError {
		t.Fatalf("result = %#v", result)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	if data["qualification"] != TeslaFC100SummaryV1QualificationFramingOnly ||
		fmt.Sprint(data["envelope_length"]) != "8" || fmt.Sprint(data["message_length"]) != "7" ||
		fmt.Sprint(data["entry_count"]) != "2" || data["outbound_allowed"] != false {
		t.Fatalf("summary data = %#v", data)
	}
	for _, forbidden := range []string{"raw", "value", "operation", "capability", "request"} {
		if _, exists := data[forbidden]; exists {
			t.Fatalf("summary exposed %q: %#v", forbidden, data)
		}
	}
}

func TestTeslaFC100SummaryV1MCPFailsClosedForInvalidProviderSummary(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	provider := &teslaFC100SummaryV1FixtureProvider{
		modbusV1FixtureProvider: &modbusV1FixtureProvider{},
		result: TeslaFC100SummaryV1Result{
			Qualification:  TeslaFC100SummaryV1QualificationFramingOnly,
			EnvelopeLength: 8, MessageLength: 7, EntryCount: 2,
			Entries:       []TeslaFC100SummaryV1WireEntry{{FieldNumber: 1, WireType: 0}},
			PayloadDigest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
	}
	RegisterModbusV1Tools(server, provider)
	result := msp06Call(t, server.Handler(), TeslaFC100SummaryV1GetTool, map[string]any{})
	if !result.isError || result.envelope["data"] != nil {
		t.Fatalf("invalid summary did not fail closed: %#v", result)
	}
}
