package mcp

import (
	"context"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type growattProtocolIIV1FixtureProvider struct{ *modbusV1FixtureProvider }

func (*growattProtocolIIV1FixtureProvider) GrowattProtocolIIV1(context.Context) (GrowattProtocolIIV1Result, error) {
	return GrowattProtocolIIV1Result{
		Profile:           GrowattProtocolIIV1Profile,
		Disposition:       "OFFLINE_IDENTITY_ADMITTED",
		Family:            "MAX",
		IdentityQualified: true,
		OutboundAllowed:   false,
	}, nil
}

func TestGrowattProtocolIIV1IdentityToolPreservesNativeIdentity(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, &growattProtocolIIV1FixtureProvider{&modbusV1FixtureProvider{}})
	result := msp06Call(t, server.Handler(), GrowattProtocolIIV1IdentityGetTool, map[string]any{})
	if result.isError {
		t.Fatalf("result = %#v", result)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	if data["profile"] != GrowattProtocolIIV1Profile || data["disposition"] != "OFFLINE_IDENTITY_ADMITTED" ||
		data["family"] != "MAX" || data["identity_qualified"] != true {
		t.Fatalf("data = %#v", data)
	}
	if _, found := data["identity_redacted"]; found {
		t.Fatalf("identity redaction survived: %#v", data)
	}
	if _, found := data["outbound_allowed"]; found {
		t.Fatalf("observation invented operation authority: %#v", data)
	}
	native := msp06Map(t, data["native_identity"], "native_identity")
	if native["family"] != "MAX" || len(msp06Slice(t, native["words"], "native words")) == 0 {
		t.Fatalf("native identity = %#v", native)
	}
}

type growattProtocolIIV1UnsafeProvider struct{ *modbusV1FixtureProvider }

func (*growattProtocolIIV1UnsafeProvider) GrowattProtocolIIV1(context.Context) (GrowattProtocolIIV1Result, error) {
	return GrowattProtocolIIV1Result{
		Profile:           GrowattProtocolIIV1Profile,
		Disposition:       "OFFLINE_IDENTITY_ADMITTED",
		Family:            "MID",
		IdentityQualified: true,
		IdentityRedacted:  false,
		OutboundAllowed:   true,
	}, nil
}

func TestGrowattProtocolIIV1IdentityToolFailsClosedForProviderOutput(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, &growattProtocolIIV1UnsafeProvider{&modbusV1FixtureProvider{}})
	result := msp06Call(t, server.Handler(), GrowattProtocolIIV1IdentityGetTool, map[string]any{})
	if result.isError {
		t.Fatalf("result = %#v", result)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	if data["outbound_allowed"] != false || data["identity_redacted"] != true {
		t.Fatalf("unsafe provider output crossed MCP boundary: %#v", data)
	}
}
