package mcp

import (
	"context"
	"errors"
)

// Shared native-profile fixture; semantic PV tests use the SemReg provider.
type modbusV1FixtureProvider struct {
	rawRequest ModbusRawReadRequest
	rawErr     error
}

func (provider *modbusV1FixtureProvider) RawRead(_ context.Context, request ModbusRawReadRequest) (ModbusRawReadResult, error) {
	provider.rawRequest = request
	if provider.rawErr != nil {
		return ModbusRawReadResult{}, provider.rawErr
	}
	return ModbusRawReadResult{EndpointRef: "sha256:endpoint", UnitID: request.UnitID, Function: request.Function, Offset: request.Offset, Quantity: request.Quantity, Words: []uint16{0x5375}, WireResponseID: 1, LogicalViewID: 2, PhysicalRequestID: 3, ConnectionID: 4, TransportGeneration: 5, PollGenerationID: 6, DeadlineIdentity: 7}, nil
}

func (*modbusV1FixtureProvider) ProfileObservation(_ context.Context, profileID, sampleID string) (ModbusProfileObservationResult, error) {
	return ModbusProfileObservationResult{ProfileID: profileID, SampleID: sampleID, SourceValidity: "fixture", DetectionEvidence: []string{}, ActivationEvidence: []string{}, Replay: []ModbusReplayView{}}, nil
}

func (*modbusV1FixtureProvider) SemanticPVCurrent(context.Context, string, string) (SemanticPVCurrentResult, error) {
	return SemanticPVCurrentResult{}, errors.New("semantic PV observation unavailable")
}
