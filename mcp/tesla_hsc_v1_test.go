package mcp

import (
	"context"
	"fmt"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type teslaHSCV1FixtureProvider struct{ *modbusV1FixtureProvider }

func (*teslaHSCV1FixtureProvider) TeslaHSCV1(context.Context) (TeslaHSCV1Result, error) {
	return TeslaHSCV1Result{Disposition: "disabled", Compatibility: "unknown", OutboundAllowed: false}, nil
}

func TestTeslaHSCV1StatusToolRetainsNativeNamedRecords(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, &teslaHSCV1FixtureProvider{&modbusV1FixtureProvider{}})
	result := msp06Call(t, server.Handler(), TeslaHSCV1StatusGetTool, map[string]any{})
	if result.isError {
		t.Fatalf("result = %#v", result)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	if data["disposition"] != "disabled" || data["outbound_allowed"] != false {
		t.Fatalf("data = %#v", data)
	}
}

type teslaHSCV1UnsafeFixtureProvider struct{ *modbusV1FixtureProvider }

func (*teslaHSCV1UnsafeFixtureProvider) TeslaHSCV1(context.Context) (TeslaHSCV1Result, error) {
	return TeslaHSCV1Result{
		Disposition:     "qualified_read_only",
		Compatibility:   "fixture",
		OutboundAllowed: true,
		NativeRecords: []TeslaHSCV1NativeRecord{{
			Function:      100,
			Payload:       []byte{0x32, 0x02, 0x2a, 0x00},
			Compatibility: "wc3_24_44_3",
			Provenance:    "synthetic-replay",
			Family:        6,
			RequestTag:    5,
			ResponseTag:   6,
			RequestName:   "GetConfig",
			ResponseName:  "WCConfig",
			FieldNames:    []string{"settings", "wifi_config"},
		}},
	}, nil
}

func TestTeslaHSCV1StatusToolPreservesNativePayloadAndContext(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, &teslaHSCV1UnsafeFixtureProvider{&modbusV1FixtureProvider{}})
	result := msp06Call(t, server.Handler(), TeslaHSCV1StatusGetTool, map[string]any{})
	if result.isError {
		t.Fatalf("result = %#v", result)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	if data["outbound_allowed"] != true {
		t.Fatalf("outbound_allowed = %#v", data["outbound_allowed"])
	}
	records, ok := data["native_records"].([]any)
	if !ok || len(records) != 1 {
		t.Fatalf("native_records = %#v", data["native_records"])
	}
	record, ok := records[0].(map[string]any)
	if !ok || fmt.Sprint(record["function"]) != "100" || record["payload"] != "MgIqAA==" ||
		record["compatibility"] != "wc3_24_44_3" || record["provenance"] != "synthetic-replay" ||
		record["request_name"] != "GetConfig" || record["response_name"] != "WCConfig" {
		t.Fatalf("native record = %#v", records[0])
	}
}

type teslaHSCV1InvalidNativeFixtureProvider struct{ *modbusV1FixtureProvider }

func (*teslaHSCV1InvalidNativeFixtureProvider) TeslaHSCV1(context.Context) (TeslaHSCV1Result, error) {
	return TeslaHSCV1Result{Compatibility: "fixture", NativeRecords: []TeslaHSCV1NativeRecord{{
		Function: 101, Payload: make([]byte, 253), Compatibility: "wc3_24_44_3", Provenance: "synthetic-replay",
	}}}, nil
}

func TestTeslaHSCV1StatusToolRejectsUnboundedNativeProviderRecord(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, &teslaHSCV1InvalidNativeFixtureProvider{&modbusV1FixtureProvider{}})
	result := msp06Call(t, server.Handler(), TeslaHSCV1StatusGetTool, map[string]any{})
	if !result.isError {
		t.Fatalf("unbounded native record accepted: %#v", result)
	}
}

type teslaHSCV1EmptyResponseProvider struct{}

func (teslaHSCV1EmptyResponseProvider) TeslaHSCV1Responses(context.Context) ([]TeslaHSCV1CorrelatedResponse, error) {
	return nil, nil
}

func TestTeslaHSCV1RuntimeRejectsEmptyCorrelatedResponseBatch(t *testing.T) {
	runtime, err := NewTeslaHSCV1Runtime(teslaHSCV1EmptyResponseProvider{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.TeslaHSCV1(context.Background())
	if err == nil {
		t.Fatalf("empty correlated response batch accepted as %#v", result)
	}
	if result.Disposition != "" || result.Compatibility != "" || result.OutboundAllowed || len(result.NativeRecords) != 0 {
		t.Fatalf("empty correlated response batch retained result %#v", result)
	}
}
