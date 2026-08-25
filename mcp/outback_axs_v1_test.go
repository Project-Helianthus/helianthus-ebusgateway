package mcp

import (
	"context"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type outBackAXSV1FixtureProvider struct{ *modbusV1FixtureProvider }

func (*outBackAXSV1FixtureProvider) OutBackAXSV1(context.Context) (OutBackAXSV1Result, error) {
	return OutBackAXSV1Result{Profile: OutBackAXSV1Profile, Qualified: true, FirmwareMajor: 1, FirmwareMid: 2, FirmwareMinor: 3}, nil
}

func TestOutBackAXSV1ToolProjectsOnlyReadOnlyFacts(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, &outBackAXSV1FixtureProvider{&modbusV1FixtureProvider{}})
	result := msp06Call(t, server.Handler(), OutBackAXSV1StatusGetTool, map[string]any{})
	if result.isError {
		t.Fatalf("result=%#v", result)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	if data["profile"] != OutBackAXSV1Profile || data["qualified"] != true || data["outbound_allowed"] != false || data["raw_redacted"] != true {
		t.Fatalf("data=%#v", data)
	}
	for _, forbidden := range []string{"words", "config", "network", "mac", "credential", "control"} {
		if _, ok := data[forbidden]; ok {
			t.Fatalf("leaked %q: %#v", forbidden, data)
		}
	}
}
