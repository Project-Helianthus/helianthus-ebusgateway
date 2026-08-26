package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

func TestGrowattBMSRS485V202RuntimeFeedsRedactedMCPStatus(t *testing.T) {
	session := &growattBMSRS485RuntimeSessionFake{wordsByOffset: growattBMSRS485RuntimeWords(), failAt: -1, mismatchAt: -1}
	runtime, err := NewGrowattBMSRS485V202Runtime(growattBMSRS485RuntimeRevision(), 7, session)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(s, growattBMSRS485RuntimeProvider{modbusV1FixtureProvider: &modbusV1FixtureProvider{}, runtime: runtime})

	r := msp06Call(t, s.Handler(), GrowattBMSRS485V202StatusGetTool, map[string]any{})
	if r.isError {
		t.Fatal(r)
	}
	d := msp06Map(t, r.envelope["data"], "data")
	native := msp06Map(t, d["native_observation"], "native_observation")
	if native["unit_id"] != json.Number("7") || len(msp06Slice(t, native["slices"], "native slices")) != 4 {
		t.Fatalf("data=%#v", d)
	}
	status := msp06Map(t, d["status"], "status")
	if status["operating_state"] != "charging" || status["soc_percent"] != json.Number("75") {
		t.Fatalf("status=%#v", status)
	}
	for _, key := range []string{"raw_redacted", "outbound_allowed"} {
		if _, ok := d[key]; ok {
			t.Fatal(key)
		}
	}
	if got, want := session.calls, [][2]uint16{{0x0001, 7}, {0x000d, 29}, {0x0100, 12}, {0x010d, 2}}; len(got) != len(want) {
		t.Fatalf("calls=%#v", got)
	} else {
		for index := range want {
			if got[index] != want[index] || session.functions[index] != modbus.FunctionReadHoldingRegisters {
				t.Fatalf("calls/functions=%#v/%#v", got, session.functions)
			}
		}
	}
	if session.unitID != 7 {
		t.Fatalf("unit_id=%d", session.unitID)
	}
}

func TestGrowattBMSRS485V202RuntimeFailsClosedAtMCPBoundary(t *testing.T) {
	session := &growattBMSRS485RuntimeSessionFake{wordsByOffset: growattBMSRS485RuntimeWords(), failAt: 1, mismatchAt: -1}
	runtime, err := NewGrowattBMSRS485V202Runtime(growattBMSRS485RuntimeRevision(), 7, session)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(s, growattBMSRS485RuntimeProvider{modbusV1FixtureProvider: &modbusV1FixtureProvider{}, runtime: runtime})

	r := msp06Call(t, s.Handler(), GrowattBMSRS485V202StatusGetTool, map[string]any{})
	if !r.isError || r.envelope["data"] != nil || len(session.calls) != 2 {
		t.Fatalf("result/calls=%#v/%#v", r, session.calls)
	}
}

func TestGrowattBMSRS485V202RuntimeRejectsMismatchedResponseAtMCPBoundary(t *testing.T) {
	session := &growattBMSRS485RuntimeSessionFake{wordsByOffset: growattBMSRS485RuntimeWords(), failAt: -1, mismatchAt: 2}
	runtime, err := NewGrowattBMSRS485V202Runtime(growattBMSRS485RuntimeRevision(), 7, session)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(s, growattBMSRS485RuntimeProvider{modbusV1FixtureProvider: &modbusV1FixtureProvider{}, runtime: runtime})

	r := msp06Call(t, s.Handler(), GrowattBMSRS485V202StatusGetTool, map[string]any{})
	if !r.isError || r.envelope["data"] != nil || len(session.calls) != 3 {
		t.Fatalf("result/calls=%#v/%#v", r, session.calls)
	}
}

type growattBMSRS485RuntimeProvider struct {
	*modbusV1FixtureProvider
	runtime *GrowattBMSRS485V202Runtime
}

func (provider growattBMSRS485RuntimeProvider) GrowattBMSRS485V202(ctx context.Context) (modbusreg.GrowattBMSTypedReadOnlyStatus, error) {
	return provider.runtime.GrowattBMSRS485V202(ctx)
}

type growattBMSRS485RuntimeSessionFake struct {
	wordsByOffset map[uint16][]uint16
	calls         [][2]uint16
	functions     []modbus.FunctionCode
	unitID        byte
	failAt        int
	mismatchAt    int
}

func (session *growattBMSRS485RuntimeSessionFake) ReadHolding(
	_ context.Context,
	unitID byte,
	request modbus.ReadRegistersRequest,
) (modbus.ReadRegistersResponse, error) {
	session.unitID = unitID
	session.calls = append(session.calls, [2]uint16{request.Offset(), request.Quantity()})
	session.functions = append(session.functions, request.Function())
	index := len(session.calls) - 1
	if index == session.failAt {
		return modbus.ReadRegistersResponse{}, errors.New("correlated read failed")
	}
	response, err := modbus.DecodeReadRegistersResponse(request, growattBMSRS485RuntimeReadPDU(session.wordsByOffset[request.Offset()]))
	if err != nil {
		return modbus.ReadRegistersResponse{}, err
	}
	if index == session.mismatchAt {
		response.Provenance.Offset++
	}
	return response, nil
}

func growattBMSRS485RuntimeRevision() modbusreg.GrowattBMSRevisionTuple {
	return modbusreg.GrowattBMSRevisionTuple{Family: "1xSxxP ESS", FileRevision: "Rev2.01", HeaderVersion: "V2.0", CumulativeRevision: "2.02"}
}

func growattBMSRS485RuntimeReadPDU(words []uint16) []byte {
	pdu := make([]byte, 2+len(words)*2)
	pdu[0] = byte(modbus.FunctionReadHoldingRegisters)
	pdu[1] = byte(len(words) * 2)
	for index, word := range words {
		pdu[2+index*2], pdu[3+index*2] = byte(word>>8), byte(word)
	}
	return pdu
}

func growattBMSRS485RuntimeWords() map[uint16][]uint16 {
	identity := make([]uint16, 7)
	identity[0], identity[1] = 0x0102, 0x0304
	status := make([]uint16, 29)
	status[0], status[1] = 0x0204, 0x0301
	status[6], status[8], status[9], status[10], status[11] = 2, 75, 5200, 0xff9c, 25
	status[13], status[14], status[17] = 3200, 5000, 110
	extension := make([]uint16, 12)
	extension[0], extension[1], extension[2], extension[4], extension[5], extension[6] = 100, 123, 3300, 512, 5, 6
	return map[uint16][]uint16{0x0001: identity, 0x000d: status, 0x0100: extension, 0x010d: {0, 0}}
}
