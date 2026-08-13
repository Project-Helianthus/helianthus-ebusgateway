package mcp

import (
	"context"
	"reflect"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type modbusV1FixtureProvider struct {
	rawRequest ModbusRawReadRequest
}

func (provider *modbusV1FixtureProvider) RawRead(_ context.Context, request ModbusRawReadRequest) (ModbusRawReadResult, error) {
	provider.rawRequest = request
	return ModbusRawReadResult{
		EndpointRef:         "sha256:endpoint",
		UnitID:              request.UnitID,
		Function:            request.Function,
		Offset:              request.Offset,
		Quantity:            request.Quantity,
		Words:               []uint16{0x5375, 0x6e53},
		WireResponseID:      91,
		LogicalViewID:       92,
		PhysicalRequestID:   93,
		ConnectionID:        94,
		TransportGeneration: 95,
		PollGenerationID:    96,
		DeadlineIdentity:    97,
	}, nil
}

func (*modbusV1FixtureProvider) ProfileObservation(_ context.Context, profileID, sampleID string) (ModbusProfileObservationResult, error) {
	return ModbusProfileObservationResult{
		ProfileID:          profileID,
		SampleID:           sampleID,
		SourceValidity:     "valid",
		DetectionEvidence:  []string{"detector:standard-only"},
		ActivationEvidence: []string{"activation:fixture"},
		Replay: []ModbusReplayView{{
			LogicalViewID:  92,
			WireResponseID: 91,
			Offset:         40000,
			Words:          []uint16{0x5375, 0x6e53},
		}},
	}, nil
}

func TestModbusV1RegistrationAndBoundedRead(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	provider := &modbusV1FixtureProvider{}
	RegisterModbusV1Tools(server, provider)

	for _, name := range []string{ModbusV1RawReadTool, ModbusV1ProfileObservationGetTool} {
		if !server.hasToolNamed(name) {
			t.Fatalf("tools/list missing %q", name)
		}
	}
	result := msp06Call(t, server.Handler(), ModbusV1RawReadTool, map[string]any{
		"unit_id": 1, "function": 3, "offset": 40000, "quantity": 2,
	})
	if result.isError {
		t.Fatalf("raw read failed: %s", result.raw)
	}
	if provider.rawRequest != (ModbusRawReadRequest{UnitID: 1, Function: 3, Offset: 40000, Quantity: 2}) {
		t.Fatalf("provider request = %+v", provider.rawRequest)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	if data["endpoint_ref"] != "sha256:endpoint" || data["endpoint"] != nil {
		t.Fatalf("endpoint redaction failed: %+v", data)
	}
}

func TestModbusV1ReadRejectsWritesUnboundedRangesAndUnknownArguments(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, &modbusV1FixtureProvider{})

	for _, arguments := range []map[string]any{
		{"unit_id": 1, "function": 6, "offset": 1, "quantity": 1},
		{"unit_id": 1, "function": 3, "offset": 1, "quantity": 126},
		{"unit_id": 1, "function": 3, "offset": 1, "quantity": 1, "payload": "write"},
	} {
		result := msp06Call(t, server.Handler(), ModbusV1RawReadTool, arguments)
		if !result.isError {
			t.Fatalf("unsafe arguments accepted: %+v", arguments)
		}
	}
}

func TestModbusV1ProfileObservationRetainsEvidenceAndReplay(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, &modbusV1FixtureProvider{})
	result := msp06Call(t, server.Handler(), ModbusV1ProfileObservationGetTool, map[string]any{
		"profile_id": "sunspec.phase1", "sample_id": "sample-91",
	})
	if result.isError {
		t.Fatalf("profile observation failed: %s", result.raw)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	if !reflect.DeepEqual(data["detection_evidence"], []any{"detector:standard-only"}) {
		t.Fatalf("detection evidence = %#v", data["detection_evidence"])
	}
	replay := msp06Slice(t, data["replay"], "replay")
	if len(replay) != 1 || msp06Map(t, replay[0], "replay[0]")["wire_response_id"] == nil {
		t.Fatalf("replay provenance missing: %#v", replay)
	}
}

func TestModbusV1NilProviderIsInert(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, nil)
	if server.hasToolNamed(ModbusV1RawReadTool) || server.hasToolNamed(ModbusV1ProfileObservationGetTool) {
		t.Fatal("nil provider exposed Modbus tools")
	}
}
