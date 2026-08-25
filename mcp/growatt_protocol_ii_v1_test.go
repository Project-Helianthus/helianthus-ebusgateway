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

func TestGrowattProtocolIIV1IdentityToolIsReadOnlyAndRedacted(t *testing.T) {
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
		data["family"] != "MAX" || data["identity_qualified"] != true || data["identity_redacted"] != true ||
		data["outbound_allowed"] != false {
		t.Fatalf("data = %#v", data)
	}
	for _, forbidden := range []string{"words", "device_type", "model_build", "protocol_version", "firmware"} {
		if _, found := data[forbidden]; found {
			t.Fatalf("raw identity field %q leaked: %#v", forbidden, data)
		}
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
