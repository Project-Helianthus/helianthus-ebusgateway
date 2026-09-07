package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

type growattProtocolIIV1FixtureProvider struct {
	*modbusV1FixtureProvider
	result GrowattProtocolIIV1Result
	err    error
}

func (fixture *growattProtocolIIV1FixtureProvider) GrowattProtocolIIV1(context.Context) (GrowattProtocolIIV1Result, error) {
	if fixture.result.Profile == "" {
		return growattProtocolIIV1ResultFromInput(fixtureInput(tinyGrowattIdentityInput())), fixture.err
	}
	return fixture.result, fixture.err
}

func TestGrowattProtocolIIV1IdentityToolPreservesNativeIdentity(t *testing.T) {
	server := growattProtocolIIV1TestServer(t, &growattProtocolIIV1FixtureProvider{modbusV1FixtureProvider: &modbusV1FixtureProvider{}})
	result := msp06Call(t, server.Handler(), GrowattProtocolIIV1IdentityGetTool, map[string]any{})
	if result.isError {
		t.Fatalf("result = %#v", result)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	if data["profile"] != GrowattProtocolIIV1Profile || data["disposition"] != "OFFLINE_IDENTITY_ADMITTED" ||
		data["family"] != "MAX" || data["identity_qualified"] != true {
		t.Fatalf("data = %#v", data)
	}
	native := msp06Map(t, data["native_identity"], "native_identity")
	if native["family"] != "MAX" || native["unit_id"] != json.Number("1") || native["firmware"] != "FW-1" ||
		native["serial"] != "SN-0001" || native["device_type"] != json.Number("4660") ||
		native["protocol_version"] != json.Number("292") {
		t.Fatalf("native identity = %#v", native)
	}
	modelBuild := msp06Slice(t, native["model_build"], "model build")
	if len(modelBuild) != 2 || modelBuild[0] != json.Number("19777") || modelBuild[1] != json.Number("22577") {
		t.Fatalf("model build = %#v", modelBuild)
	}
	slices := msp06Slice(t, native["slices"], "native slices")
	if len(slices) != 5 {
		t.Fatalf("slice count = %d; want 5", len(slices))
	}
	for index, offset := range []json.Number{"9", "23", "43", "82", "88"} {
		if slice := msp06Map(t, slices[index], "native slice"); slice["offset"] != offset {
			t.Fatalf("slice %d = %#v; want offset %s", index, slice, offset)
		}
	}
}

func TestGrowattProtocolIIV1IdentityToolRejectsUntrustedProviderResults(t *testing.T) {
	for name, mutate := range map[string]func(*GrowattProtocolIIV1Result){
		"malformed unit":   func(result *GrowattProtocolIIV1Result) { result.NativeIdentity.UnitID = 0 },
		"malformed family": func(result *GrowattProtocolIIV1Result) { result.NativeIdentity.Family = "MIX" },
		"malformed offset": func(result *GrowattProtocolIIV1Result) { result.NativeIdentity.Slices[1].Offset = 24 },
		"malformed extent": func(result *GrowattProtocolIIV1Result) {
			result.NativeIdentity.Slices[1].Words = result.NativeIdentity.Slices[1].Words[:4]
		},
		"malformed ASCII":     func(result *GrowattProtocolIIV1Result) { result.NativeIdentity.Slices[0].Words[0] = 0x4600 },
		"contradictory tuple": func(result *GrowattProtocolIIV1Result) { result.NativeIdentity.DeviceType++ },
	} {
		t.Run(name, func(t *testing.T) {
			result := growattProtocolIIV1ResultFromInput(fixtureInput(tinyGrowattIdentityInput()))
			mutate(&result)
			server := growattProtocolIIV1TestServer(t, &growattProtocolIIV1FixtureProvider{
				modbusV1FixtureProvider: &modbusV1FixtureProvider{}, result: result,
			})
			call := msp06Call(t, server.Handler(), GrowattProtocolIIV1IdentityGetTool, map[string]any{})
			if !call.isError || call.envelope["data"] != nil {
				t.Fatalf("malformed provider result = %#v", call)
			}
		})
	}
}

func TestGrowattProtocolIIV1IdentityToolDropsProviderDataOnError(t *testing.T) {
	server := growattProtocolIIV1TestServer(t, &growattProtocolIIV1FixtureProvider{
		modbusV1FixtureProvider: &modbusV1FixtureProvider{},
		result:                  growattProtocolIIV1ResultFromInput(fixtureInput(tinyGrowattIdentityInput())),
		err:                     errors.New("partial identity must not escape"),
	})
	result := msp06Call(t, server.Handler(), GrowattProtocolIIV1IdentityGetTool, map[string]any{})
	if !result.isError || result.envelope["data"] != nil {
		t.Fatalf("provider error result = %#v", result)
	}
}

func TestGrowattProtocolIIV1GoldenToolsListAndCallEnvelope(t *testing.T) {
	server := growattProtocolIIV1TestServer(t, &growattProtocolIIV1FixtureProvider{modbusV1FixtureProvider: &modbusV1FixtureProvider{}})
	list := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	if list.Error != nil {
		t.Fatalf("tools/list error = %+v", list.Error)
	}
	listJSON, err := json.Marshal(list.Result)
	if err != nil {
		t.Fatalf("marshal tools/list: %v", err)
	}
	compareGrowattProtocolIIV1Golden(t, "growatt_protocol_ii_tools_list.golden.json", string(listJSON))

	call := msp06Call(t, server.Handler(), GrowattProtocolIIV1IdentityGetTool, map[string]any{})
	if call.isError {
		t.Fatalf("call result = %#v", call)
	}
	compareGrowattProtocolIIV1Golden(t, "growatt_protocol_ii_call_envelope.golden.json", call.raw)
}

func growattProtocolIIV1TestServer(t *testing.T, provider *growattProtocolIIV1FixtureProvider) *Server {
	t.Helper()
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, provider)
	return server
}

func compareGrowattProtocolIIV1Golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if os.Getenv("UPDATE") == "1" {
		if err := os.WriteFile(path, []byte(got+"\n"), 0o644); err != nil {
			t.Fatalf("write golden %s: %v", name, err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	if got != strings.TrimSpace(string(want)) {
		t.Fatalf("golden mismatch for %s:\nwant: %s\ngot:  %s", name, strings.TrimSpace(string(want)), got)
	}
}

func tinyGrowattIdentityInput() modbusreg.GrowattProtocolIIIdentityInput {
	return modbusreg.GrowattProtocolIIIdentityInput{
		UnitID:   1,
		Function: modbusreg.FunctionReadHoldingRegisters,
		Profile: modbusreg.GrowattProtocolIIIdentityProfile{
			Schema:          "Protocol II v1.24 TL3-X",
			Family:          "MAX",
			DeviceType:      0x1234,
			ModelBuild:      [2]uint16{0x4d41, 0x5831},
			ProtocolVersion: 0x0124,
		},
		Slices: []modbusreg.GrowattProtocolIIIdentitySlice{
			{Offset: 9, Words: []uint16{0x4657, 0x2d31, 0, 0, 0, 0}},
			{Offset: 23, Words: []uint16{0x534e, 0x2d30, 0x3030, 0x3100, 0}},
			{Offset: 43, Words: []uint16{0x1234}},
			{Offset: 82, Words: []uint16{0x4d41, 0x5831}},
			{Offset: 88, Words: []uint16{0x0124}},
		},
	}
}

func fixtureInput(input modbusreg.GrowattProtocolIIIdentityInput) modbusreg.GrowattProtocolIIIdentityInput {
	input.Slices = append([]modbusreg.GrowattProtocolIIIdentitySlice(nil), input.Slices...)
	for index := range input.Slices {
		input.Slices[index].Words = append([]uint16(nil), input.Slices[index].Words...)
	}
	return input
}

func growattProtocolIIV1ResultFromInput(input modbusreg.GrowattProtocolIIIdentityInput) GrowattProtocolIIV1Result {
	observation, err := modbusreg.DecodeGrowattProtocolIIIdentity(input)
	if err != nil {
		panic(err)
	}
	return growattProtocolIIV1ResultFromObservation(observation)
}
