package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

type gatewayModbusMCPProvider struct {
	adapter *modbusadapter.Adapter
	nextID  atomic.Uint64
}

func newGatewayModbusMCPProvider(adapter *modbusadapter.Adapter) mcp.ModbusV1Provider {
	if adapter == nil {
		return nil
	}
	return &gatewayModbusMCPProvider{adapter: adapter}
}

func (provider *gatewayModbusMCPProvider) RawRead(ctx context.Context, request mcp.ModbusRawReadRequest) (mcp.ModbusRawReadResult, error) {
	if provider == nil || provider.adapter == nil {
		return mcp.ModbusRawReadResult{}, errors.New("modbus provider unavailable")
	}
	function := modbus.FunctionCode(request.Function)
	read, err := modbus.NewReadRegistersRequest(function, request.Offset, request.Quantity)
	if err != nil {
		return mcp.ModbusRawReadResult{}, err
	}
	id := provider.nextID.Add(1)
	batch, err := provider.adapter.ExecuteRead(ctx, modbusadapter.ReadPlan{
		UnitID:             request.UnitID,
		AuthorizationScope: "mcp:modbus.raw.read",
		PollGeneration:     id,
		DeadlineIdentity:   id,
		Timeout:            3 * time.Second,
		Reads: []modbus.TCPLogicalRead{{
			LogicalViewID: id,
			Request:       read,
		}},
	})
	if err != nil {
		return mcp.ModbusRawReadResult{}, err
	}
	for _, view := range batch.Views {
		if view.LogicalViewID() != id {
			continue
		}
		provenance := view.Provenance()
		result := mcp.ModbusRawReadResult{
			EndpointRef:         endpointReference(provenance.Wire.Endpoint),
			UnitID:              provenance.Wire.UnitID,
			Function:            byte(provenance.Wire.RequestedFunction),
			Offset:              view.LogicalOffset(),
			Quantity:            view.LogicalWordCount(),
			Words:               view.Words(),
			WireResponseID:      view.WireResponseID(),
			LogicalViewID:       view.LogicalViewID(),
			PhysicalRequestID:   provenance.PhysicalRequestID,
			ConnectionID:        provenance.Wire.ConnectionID,
			TransportGeneration: provenance.Wire.TransportGeneration,
			PollGenerationID:    provenance.PollGeneration,
			DeadlineIdentity:    provenance.DeadlineIdentity,
		}
		for _, response := range batch.Responses {
			if response.WireResponseID() == view.WireResponseID() {
				result.WireBytesHex = hex.EncodeToString(response.Bytes())
				break
			}
		}
		return result, nil
	}
	return mcp.ModbusRawReadResult{}, errors.New("modbus runtime returned no matching logical view")
}

func (provider *gatewayModbusMCPProvider) ProfileObservation(_ context.Context, profileID, sampleID string) (mcp.ModbusProfileObservationResult, error) {
	record, ok := provider.adapter.ProfileObservation(profileID, sampleID)
	if !ok {
		return mcp.ModbusProfileObservationResult{}, errors.New("profile observation not found")
	}
	spec := record.Observation.Spec()
	encoded, err := json.Marshal(record.Observation)
	if err != nil {
		return mcp.ModbusProfileObservationResult{}, fmt.Errorf("encode profile observation: %w", err)
	}
	var observation any
	if err := json.Unmarshal(encoded, &observation); err != nil {
		return mcp.ModbusProfileObservationResult{}, fmt.Errorf("decode profile observation envelope: %w", err)
	}
	redactModbusEndpoints(observation)
	replay := record.Observation.Replay()
	views := make([]mcp.ModbusReplayView, 0, len(replay))
	for _, dependency := range replay {
		views = append(views, mcp.ModbusReplayView{
			LogicalViewID:  dependency.LogicalViewID(),
			WireResponseID: dependency.WireResponseID(),
			Offset:         dependency.LogicalOffset(),
			Words:          dependency.RawWords(),
		})
	}
	sourceTime := ""
	if !spec.SourceTime.Time.IsZero() {
		sourceTime = spec.SourceTime.Time.UTC().Format(time.RFC3339Nano)
	}
	return mcp.ModbusProfileObservationResult{
		ProfileID:          spec.ProfileID,
		ProfileVersion:     fmt.Sprint(spec.ProfileVersion),
		CodecVersion:       fmt.Sprint(spec.CodecContractVersion),
		SampleID:           spec.SampleID,
		PollGenerationID:   spec.PollGenerationID,
		SourceValidity:     string(spec.SourceValidity),
		SourceTime:         sourceTime,
		LocalReceiptTime:   spec.LocalReceiptTime.UTC().Format(time.RFC3339Nano),
		DetectionEvidence:  record.DetectionEvidence,
		ActivationEvidence: record.ActivationEvidence,
		Observation:        observation,
		Replay:             views,
	}, nil
}

func endpointReference(endpoint string) string {
	sum := sha256.Sum256([]byte(endpoint))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func redactModbusEndpoints(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "endpoint" {
				if endpoint, ok := child.(string); ok {
					typed[key] = endpointReference(endpoint)
				}
				continue
			}
			redactModbusEndpoints(child)
		}
	case []any:
		for _, child := range typed {
			redactModbusEndpoints(child)
		}
	}
}
