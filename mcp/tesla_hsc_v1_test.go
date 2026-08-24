package mcp

import (
	"context"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type teslaHSCV1FixtureProvider struct{ *modbusV1FixtureProvider }

func (*teslaHSCV1FixtureProvider) TeslaHSCV1(context.Context) (TeslaHSCV1Result, error) {
	return TeslaHSCV1Result{Disposition: "disabled", Compatibility: "unknown", OutboundAllowed: false}, nil
}

func TestTeslaHSCV1StatusToolIsReadOnlyAndRedacted(t *testing.T) {
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
		RetainedLength:  42,
		RetainedDigest:  "sensitive-fixture-payload",
	}, nil
}

func TestTeslaHSCV1StatusToolFailsClosedForProviderOutput(t *testing.T) {
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
	if data["outbound_allowed"] != false {
		t.Fatalf("outbound_allowed = %#v", data["outbound_allowed"])
	}
	if data["retained_length"] != float64(0) || data["retained_digest"] != "" {
		t.Fatalf("retention metadata = %#v", data)
	}
}
