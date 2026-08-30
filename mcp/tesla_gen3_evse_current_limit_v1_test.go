package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

type teslaGen3EVSECurrentLimitV1Fixture struct {
	*modbusV1FixtureProvider
	source TeslaGen3EVSECurrentLimitV1Source
	err    error
}

func (fixture teslaGen3EVSECurrentLimitV1Fixture) TeslaGen3EVSECurrentLimitV1(context.Context) (TeslaGen3EVSECurrentLimitV1Source, error) {
	return fixture.source, fixture.err
}

func TestTeslaGen3EVSECurrentLimitV1ProjectsInjectedRecordsReadOnly(t *testing.T) {
	source := teslaGen3EVSECurrentLimitV1FixtureSource(t)
	server, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, teslaGen3EVSECurrentLimitV1Fixture{modbusV1FixtureProvider: &modbusV1FixtureProvider{}, source: source})
	result := msp06Call(t, server.Handler(), TeslaGen3EVSECurrentLimitV1GetTool, map[string]any{})
	if result.isError {
		t.Fatalf("result = %#v", result)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	if data["operation_version"] != modbusreg.TeslaGen3CurrentLimitOperationVersion24443 || data["outbound_allowed"] != false {
		t.Fatalf("data = %#v", data)
	}
	persistent := msp06Map(t, data["persistent"], "persistent")
	if persistent["max_output_current_amps"] != json.Number("16") || persistent["request_payload"] != base64.StdEncoding.EncodeToString(source.Persistent.RequestPayload()) || persistent["terminal_payload"] != base64.StdEncoding.EncodeToString(source.Persistent.TerminalPayload()) {
		t.Fatalf("persistent = %#v", persistent)
	}
	provisional := msp06Map(t, data["provisional"], "provisional")
	if provisional["limit_current_max_amps"] != json.Number("16") || provisional["limit_timeout_s"] != json.Number("600") || provisional["inhibit_charging"] != false || provisional["set_request_payload"] != base64.StdEncoding.EncodeToString(source.Provisional.SetRequestPayload()) || provisional["ack_payload"] != base64.StdEncoding.EncodeToString(source.Provisional.AckPayload()) || provisional["readback_request_payload"] != base64.StdEncoding.EncodeToString(source.Provisional.ReadbackRequestPayload()) || provisional["readback_terminal_payload"] != base64.StdEncoding.EncodeToString(source.Provisional.ReadbackTerminalPayload()) {
		t.Fatalf("provisional = %#v", provisional)
	}
	if invalid := msp06Call(t, server.Handler(), TeslaGen3EVSECurrentLimitV1GetTool, map[string]any{"unexpected": true}); !invalid.isError {
		t.Fatalf("arguments accepted: %#v", invalid)
	}
}

func TestTeslaGen3EVSECurrentLimitV1FailsClosedWithoutARecord(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, teslaGen3EVSECurrentLimitV1Fixture{modbusV1FixtureProvider: &modbusV1FixtureProvider{}})
	result := msp06Call(t, server.Handler(), TeslaGen3EVSECurrentLimitV1GetTool, map[string]any{})
	if !result.isError || result.envelope["data"] != nil {
		t.Fatalf("empty source accepted: %#v", result)
	}
}

func TestTeslaGen3EVSECurrentLimitV1FailsClosedForAnInvalidRecord(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, teslaGen3EVSECurrentLimitV1Fixture{
		modbusV1FixtureProvider: &modbusV1FixtureProvider{},
		source: TeslaGen3EVSECurrentLimitV1Source{
			Persistent: &modbusreg.TeslaGen3PersistentCurrentLimit{},
		},
	})
	result := msp06Call(t, server.Handler(), TeslaGen3EVSECurrentLimitV1GetTool, map[string]any{})
	if !result.isError || result.envelope["data"] != nil {
		t.Fatalf("invalid source accepted: %#v", result)
	}
}

func TestTeslaGen3EVSECurrentLimitV1ToolListSchemaAndOrder(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	baselineCount := len(server.tools)
	RegisterModbusV1Tools(server, teslaGen3EVSECurrentLimitV1Fixture{modbusV1FixtureProvider: &modbusV1FixtureProvider{}})
	newTools := server.tools[baselineCount:]
	if len(newTools) != 4 || newTools[0].Name != ModbusV1RawReadTool || newTools[1].Name != ModbusV1ProfileObservationGetTool || newTools[2].Name != ModbusV1CanonicalPVGetTool || newTools[3].Name != TeslaGen3EVSECurrentLimitV1GetTool {
		t.Fatalf("new tool order = %#v", newTools)
	}
	response := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	result, ok := response.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list result = %#v", response)
	}
	tools, ok := result["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list tools = %#v", result)
	}
	tool := findToolByName(tools, TeslaGen3EVSECurrentLimitV1GetTool)
	if tool == nil {
		t.Fatal("current-limit tool missing from tools/list")
	}
	schema := msp06Map(t, tool["inputSchema"], "input schema")
	properties := msp06Map(t, schema["properties"], "input properties")
	if schema["type"] != "object" || len(properties) != 0 || schema["additionalProperties"] != false {
		t.Fatalf("input schema = %#v", schema)
	}
}

func teslaGen3EVSECurrentLimitV1FixtureSource(t *testing.T) TeslaGen3EVSECurrentLimitV1Source {
	t.Helper()
	persistentRequest := teslaGen3EVSECurrentLimitV1Request(t, modbusreg.TeslaFC100OperationWCConfigureSettings, []byte{0x08, 0x10})
	persistent, err := modbusreg.NewTeslaGen3PersistentCurrentLimit(modbusreg.TeslaGen3PersistentCurrentLimitSpec{
		OperationVersion:     modbusreg.TeslaGen3CurrentLimitOperationVersion24443,
		MaxOutputCurrentAmps: 16,
		RequestPayload:       persistentRequest,
		TerminalPayload:      teslaGen3EVSECurrentLimitV1Terminal(8, []byte{0x08, 0x10}),
	})
	if err != nil {
		t.Fatal(err)
	}
	setRequest := teslaGen3EVSECurrentLimitV1Request(t, modbusreg.TeslaFC100OperationWCSetProvisional, []byte{0x08, 0x10})
	readbackRequest := teslaGen3EVSECurrentLimitV1Request(t, modbusreg.TeslaFC100OperationWCGetProvisional, nil)
	provisional, err := modbusreg.NewTeslaGen3ProvisionalCurrentLimit(modbusreg.TeslaGen3ProvisionalCurrentLimitSpec{
		OperationVersion:        modbusreg.TeslaGen3CurrentLimitOperationVersion24443,
		LimitCurrentMaxAmps:     16,
		LimitTimeoutSeconds:     600,
		SetRequestPayload:       setRequest,
		AckPayload:              teslaGen3EVSECurrentLimitV1Terminal(26, nil),
		ReadbackRequestPayload:  readbackRequest,
		ReadbackTerminalPayload: teslaGen3EVSECurrentLimitV1Terminal(28, []byte{0x08, 0x10}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return TeslaGen3EVSECurrentLimitV1Source{Persistent: &persistent, Provisional: &provisional}
}

func teslaGen3EVSECurrentLimitV1Request(t *testing.T, operation modbusreg.TeslaFC100Operation, body []byte) []byte {
	t.Helper()
	request, err := modbusreg.BuildTeslaFC100OperationRequest(modbusreg.TeslaGen3CurrentLimitOperationVersion24443, operation, body)
	if err != nil {
		t.Fatal(err)
	}
	return request.Payload()
}

func teslaGen3EVSECurrentLimitV1Terminal(responseTag uint64, body []byte) []byte {
	inner := teslaGen3EVSECurrentLimitV1Varint(nil, responseTag<<3|2)
	inner = teslaGen3EVSECurrentLimitV1Varint(inner, uint64(len(body)))
	inner = append(inner, body...)
	message := teslaGen3EVSECurrentLimitV1Varint(nil, 6<<3|2)
	message = teslaGen3EVSECurrentLimitV1Varint(message, uint64(len(inner)))
	message = append(message, inner...)
	return append([]byte{byte(len(message))}, message...)
}

func teslaGen3EVSECurrentLimitV1Varint(out []byte, value uint64) []byte {
	for value >= 0x80 {
		out = append(out, byte(value)|0x80)
		value >>= 7
	}
	return append(out, byte(value))
}
