package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type outBackAXSV1FixtureProvider struct{ *modbusV1FixtureProvider }

func (*outBackAXSV1FixtureProvider) OutBackAXSV1(context.Context) (OutBackAXSV1Result, error) {
	return OutBackAXSV1Result{
		Profile:         OutBackAXSV1Profile,
		Qualified:       true,
		FirmwareMajor:   1,
		FirmwareMid:     2,
		FirmwareMinor:   3,
		RawWords:        []uint16{64110, 282},
		OutboundAllowed: true,
	}, nil
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
	if data["profile"] != OutBackAXSV1Profile || data["qualified"] != true || data["outbound_allowed"] != true {
		t.Fatalf("data=%#v", data)
	}
	rawWords := msp06Slice(t, data["raw_words"], "raw_words")
	if len(rawWords) != 2 || rawWords[0] != json.Number("64110") || rawWords[1] != json.Number("282") {
		t.Fatalf("raw_words=%#v", rawWords)
	}
}
