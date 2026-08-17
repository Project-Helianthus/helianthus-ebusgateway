package mcp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type modbusV1FixtureProvider struct {
	rawRequest ModbusRawReadRequest
	rawErr     error
}

func (*modbusV1FixtureProvider) CanonicalPV(context.Context, string, string) (ModbusCanonicalPVResult, error) {
	return ModbusCanonicalPVResult{ProducedAt: "2026-08-17T15:00:00Z", Snapshot: pv.Snapshot{
		ContractID: pv.ContractV1, AssetRef: "pv-asset-fixture", Generation: 1,
		SourceTimeState: pv.SourceTimeUnavailable,
		Capability:      pv.Capability{ID: pv.CapabilityThreePhaseTelemetryV1, Outcome: pv.CapabilityNotSatisfied},
	}}, nil
}

func (provider *modbusV1FixtureProvider) RawRead(_ context.Context, request ModbusRawReadRequest) (ModbusRawReadResult, error) {
	provider.rawRequest = request
	if provider.rawErr != nil {
		return ModbusRawReadResult{}, provider.rawErr
	}
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

func TestModbusV1ProviderErrorIsStaticAndEndpointFree(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	leaked := "read tcp 192.0.2.10:49152->192.0.2.20:502: connection reset"
	RegisterModbusV1Tools(server, &modbusV1FixtureProvider{rawErr: errors.New(leaked)})

	result := msp06Call(t, server.Handler(), ModbusV1RawReadTool, map[string]any{
		"unit_id": 1, "function": 3, "offset": 40000, "quantity": 1,
	})
	if !result.isError {
		t.Fatal("provider failure returned success")
	}
	envelopeError := msp06Map(t, result.envelope["error"], "error")
	if envelopeError["code"] != "UNAVAILABLE" || envelopeError["retriable"] != true ||
		envelopeError["message"] != "modbus provider unavailable" {
		t.Fatalf("provider error envelope = %#v", envelopeError)
	}
	message, ok := envelopeError["message"].(string)
	if !ok {
		t.Fatalf("provider error message = %#v", envelopeError["message"])
	}
	for _, forbidden := range []string{leaked, "192.0.2.10", "192.0.2.20", "49152", "502"} {
		if strings.Contains(message, forbidden) {
			t.Fatalf("provider error leaked %q: %s", forbidden, message)
		}
	}
}

func (*modbusV1FixtureProvider) ProfileObservation(_ context.Context, profileID, sampleID string) (ModbusProfileObservationResult, error) {
	return ModbusProfileObservationResult{
		ProfileID:          profileID,
		SampleID:           sampleID,
		SourceValidity:     "invalid",
		SourceTime:         "2026-08-13T10:00:00Z",
		LocalReceiptTime:   "2026-08-13T10:00:01Z",
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

	for _, name := range []string{ModbusV1RawReadTool, ModbusV1ProfileObservationGetTool, ModbusV1CanonicalPVGetTool} {
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

func TestModbusV1CanonicalPVUsesClosedSemanticPayload(t *testing.T) {
	server, err := NewServer(&testRegistry{entries: make(map[byte]registry.DeviceEntry)}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	RegisterModbusV1Tools(server, &modbusV1FixtureProvider{})
	result := msp06Call(t, server.Handler(), ModbusV1CanonicalPVGetTool, map[string]any{
		"profile_id": "sunspec.inverter.three_phase.monitoring@1.0.0", "sample_id": "sunspec-71-81",
	})
	if result.isError {
		t.Fatalf("canonical PV failed: %s", result.raw)
	}
	data := msp06Map(t, result.envelope["data"], "data")
	meta := msp06Map(t, result.envelope["meta"], "meta")
	if data["contract_id"] != pv.ContractV1 || data["asset_ref"] != "pv-asset-fixture" || data["produced_at"] != "2026-08-17T15:00:00Z" ||
		meta["data_timestamp"] != data["produced_at"] || data["ContractID"] != nil {
		t.Fatalf("canonical data = %#v", data)
	}
	for _, forbidden := range []string{"endpoint", "raw_words", "wire_bytes"} {
		if strings.Contains(result.raw, forbidden) {
			t.Fatalf("semantic payload leaked %q: %s", forbidden, result.raw)
		}
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
		if result.envelope["data"] != nil {
			t.Fatalf("error data = %#v; want null", result.envelope["data"])
		}
		envelopeError := msp06Map(t, result.envelope["error"], "error")
		if len(envelopeError) != 4 || envelopeError["code"] != "INVALID_ARGUMENT" || envelopeError["retriable"] != false {
			t.Fatalf("error envelope = %#v", envelopeError)
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
	meta := msp06Map(t, result.envelope["meta"], "meta")
	consistency := msp06Map(t, meta["consistency"], "consistency")
	if data["source_validity"] != "invalid" || consistency["mode"] != "RETAINED_SOURCE_OBSERVATION" ||
		meta["data_timestamp"] != "2026-08-13T10:00:01Z" {
		t.Fatalf("retained source facts changed: data=%+v meta=%+v", data, meta)
	}
	if !reflect.DeepEqual(data["detection_evidence"], []any{"detector:standard-only"}) {
		t.Fatalf("detection evidence = %#v", data["detection_evidence"])
	}
	if _, ok := data["observation_json_base64"].(string); !ok || data["observation"] != nil {
		t.Fatalf("observation boundary = %#v", data)
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
	if server.hasToolNamed(ModbusV1RawReadTool) || server.hasToolNamed(ModbusV1ProfileObservationGetTool) || server.hasToolNamed(ModbusV1CanonicalPVGetTool) {
		t.Fatal("nil provider exposed Modbus tools")
	}
}

func TestModbusV1GoldenEnvelopes(t *testing.T) {
	fixtures := map[string]any{
		"modbus_v1_raw_read": ModbusRawReadResult{
			EndpointRef: "sha256:endpoint", UnitID: 1, Function: 3, Offset: 40000, Quantity: 2,
			Words: []uint16{0x5375, 0x6e53}, WireBytesHex: "53756e53", WireResponseID: 91,
			LogicalViewID: 92, PhysicalRequestID: 93, ConnectionID: 94,
			TransportGeneration: 95, PollGenerationID: 96, DeadlineIdentity: 97,
		},
		"modbus_v1_profile_observation": ModbusProfileObservationResult{
			ProfileID: "sunspec.phase1", ProfileVersion: "1.0.0", CodecVersion: "1.0.0",
			SampleID: "sample-91", PollGenerationID: 96, SourceValidity: "invalid",
			SourceTime: "2026-08-13T10:00:00Z", LocalReceiptTime: "2026-08-13T10:00:01Z",
			DetectionEvidence:  []string{"detector:standard-only"},
			ActivationEvidence: []string{"activation:fixture"},
			ObservationJSONB64: "eyJlbmRwb2ludCI6InNoYTI1NjplbmRwb2ludCIsIm5vcm1hbGl6YXRpb25fdmVyc2lvbiI6IjEuMC4wIn0=",
			Replay:             []ModbusReplayView{{LogicalViewID: 92, WireResponseID: 91, Offset: 40000, Words: []uint16{0x5375, 0x6e53}}},
		},
		"modbus_v1_canonical_pv": canonicalPVData(modbusV1CanonicalPVFixture(), "2026-08-17T15:00:00Z"),
	}
	for name, data := range fixtures {
		t.Run(name, func(t *testing.T) {
			mode := "LIVE"
			timestamp := "2026-08-13T10:00:02Z"
			switch name {
			case "modbus_v1_profile_observation":
				mode = "RETAINED_SOURCE_OBSERVATION"
			case "modbus_v1_canonical_pv":
				mode = "RETAINED_CANONICAL_OBSERVATION"
				timestamp = "2026-08-17T15:00:00Z"
			}
			envelope := newModbusV1Envelope(data, nil, true, mode, timestamp)
			got := mustJSON(envelope)
			path := filepath.Join("testdata", name+".golden.json")
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden %s: %v", path, err)
			}
			if got != strings.TrimSpace(string(want)) {
				t.Fatalf("golden mismatch for %s:\nwant: %s\ngot:  %s", name, strings.TrimSpace(string(want)), got)
			}
			second := newModbusV1Envelope(data, nil, true, mode, "2026-08-13T10:00:03Z")
			if envelope["meta"].(map[string]any)["data_hash"] != second["meta"].(map[string]any)["data_hash"] {
				t.Fatal("data_hash changed for identical data")
			}
		})
	}
}

func modbusV1CanonicalPVFixture() pv.Snapshot {
	registryRef := pv.Digest("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	observationRef := pv.Digest("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	requestedRef := pv.Digest("sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	dimensions := pv.Dimensions{Scope: pv.ScopeTotal}
	provenance := pv.Provenance{
		SourceIdentity:    pv.SourceIdentity{Protocol: "sunspec_modbus", ProfileID: "sunspec.inverter.three_phase.monitoring@1.0.0", ProfileVersion: "1.0.0", Validity: pv.SourceTerminalVerified},
		SourceRegistryRef: registryRef, SourceObservationRef: observationRef,
		SourceShadowRef: pv.Digest("sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
		EvidenceRef:     pv.Digest("sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
	}
	key := pv.NewFactKey(pv.FactACActivePower, dimensions)
	return pv.Snapshot{
		ContractID: pv.ContractV1, AssetRef: "pv-asset-fixture", Generation: 1, Evaluated: 100,
		Source: provenance, Origins: map[pv.Digest]pv.Provenance{observationRef: provenance},
		Facts: map[pv.FactKey]pv.Fact{key: {
			ID: pv.FactACActivePower, Dimensions: dimensions, Value: pv.DecimalFactValue(pv.MustDecimal("7310", 0)), Unit: pv.UnitWatt,
			Quality: pv.QualityGood, Availability: pv.AvailabilityAvailable, Freshness: pv.FreshnessFresh,
			Temporal: pv.Temporal{Receipt: 100, FreshUntil: 30_000_000_100, RetainUntil: 300_000_000_100, Policy: pv.PolicyTelemetryFastV1}, OriginRef: observationRef,
		}},
		SourceTimeState:  pv.SourceTimeUnavailable,
		Capability:       pv.Capability{ID: pv.CapabilityThreePhaseTelemetryV1, Outcome: pv.CapabilityNotSatisfied},
		RequestedOutputs: []pv.RequestedOutput{{SourceRef: observationRef, RequestedOutputRef: requestedRef}},
		ProjectionReport: []pv.Projection{{SourceRef: observationRef, RequestedOutputRef: requestedRef, FactID: pv.FactACActivePower, Dimensions: &dimensions, Outcome: pv.ProjectionMapped}},
	}
}
