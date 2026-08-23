package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

type gatewayModbusMCPProvider struct {
	adapter modbusMCPAdapter
	nextID  atomic.Uint64
	rateMu  sync.Mutex
	rateAt  time.Time
	rateN   int
	now     func() time.Time
}

type modbusMCPAdapter interface {
	ExecuteReadWithReconnect(context.Context, modbusadapter.ReadPlan) (modbus.TCPReadBatch, error)
	ProfileObservation(string, string) (modbusadapter.ProfileObservationRecord, bool)
	SunSpecQualificationObservation(string, string) (modbusreg.SunSpecQualificationObservation, []byte, bool)
	CanonicalPVSnapshot(string, string) (pv.Snapshot, time.Time, bool)
}

func newGatewayModbusMCPProvider(adapter *modbusadapter.Adapter) mcp.ModbusV1Provider {
	if adapter == nil {
		return nil
	}
	return &gatewayModbusMCPProvider{adapter: adapter, now: time.Now}
}

func (provider *gatewayModbusMCPProvider) RawRead(ctx context.Context, request mcp.ModbusRawReadRequest) (mcp.ModbusRawReadResult, error) {
	if provider == nil || provider.adapter == nil {
		return mcp.ModbusRawReadResult{}, errors.New("modbus provider unavailable")
	}
	if !provider.admitRawRead() {
		return mcp.ModbusRawReadResult{}, mcp.ErrModbusV1ResourceExhausted
	}
	function := modbus.FunctionCode(request.Function)
	read, err := modbus.NewReadRegistersRequest(function, request.Offset, request.Quantity)
	if err != nil {
		return mcp.ModbusRawReadResult{}, err
	}
	id := provider.nextID.Add(1)
	plan := modbusadapter.ReadPlan{
		UnitID:             request.UnitID,
		AuthorizationScope: "mcp:modbus.raw.read",
		PollGeneration:     id,
		DeadlineIdentity:   id,
		Timeout:            3 * time.Second,
		Reads: []modbus.TCPLogicalRead{{
			LogicalViewID: id,
			Request:       read,
		}},
	}
	operationCtx, cancel := context.WithTimeout(ctx, plan.Timeout)
	defer cancel()
	batch, err := provider.adapter.ExecuteReadWithReconnect(operationCtx, plan)
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

func (provider *gatewayModbusMCPProvider) admitRawRead() bool {
	provider.rateMu.Lock()
	defer provider.rateMu.Unlock()
	now := provider.now()
	if provider.rateAt.IsZero() || now.Before(provider.rateAt) || now.Sub(provider.rateAt) >= mcp.ModbusV1RawReadWindow {
		provider.rateAt = now
		provider.rateN = 0
	}
	if provider.rateN >= mcp.ModbusV1MaxRawReadsPerWindow {
		return false
	}
	provider.rateN++
	return true
}

func (provider *gatewayModbusMCPProvider) ProfileObservation(_ context.Context, profileID, sampleID string) (mcp.ModbusProfileObservationResult, error) {
	record, ok := provider.adapter.ProfileObservation(profileID, sampleID)
	if ok {
		return profileObservationResult(record)
	}
	qualification, encoded, ok := provider.adapter.SunSpecQualificationObservation(profileID, sampleID)
	if !ok {
		return mcp.ModbusProfileObservationResult{}, errors.New("profile observation not found")
	}
	return sunSpecQualificationObservationResult(qualification, encoded)
}

func (provider *gatewayModbusMCPProvider) CanonicalPV(_ context.Context, profileID, sampleID string) (mcp.ModbusCanonicalPVResult, error) {
	if provider == nil || provider.adapter == nil || profileID != modbusreg.SunSpecThreePhaseMonitoringCapabilityID {
		return mcp.ModbusCanonicalPVResult{}, errors.New("canonical PV observation unavailable")
	}
	snapshot, producedAt, ok := provider.adapter.CanonicalPVSnapshot(profileID, sampleID)
	if !ok {
		return mcp.ModbusCanonicalPVResult{}, errors.New("canonical PV observation unavailable")
	}
	return mcp.ModbusCanonicalPVResult{Snapshot: snapshot, ProducedAt: producedAt.Format(time.RFC3339Nano)}, nil
}

// TeslaHSCV1 exposes only the disabled-by-default profile state. It does not
// open a serial endpoint, schedule acquisition, or transmit vendor frames.
func (provider *gatewayModbusMCPProvider) TeslaHSCV1(_ context.Context) (mcp.TeslaHSCV1Result, error) {
	profile, err := modbusreg.NewTeslaHSCProfile(modbusreg.TeslaHSCProfileConfig{
		Node:                 0x10,
		CompatibilityVersion: "unknown",
	})
	if err != nil {
		return mcp.TeslaHSCV1Result{}, err
	}
	return mcp.TeslaHSCV1Result{
		Disposition:     string(profile.Disposition()),
		Compatibility:   "unknown",
		OutboundAllowed: profile.OutboundAllowed(),
	}, nil
}

func profileObservationResult(record modbusadapter.ProfileObservationRecord) (mcp.ModbusProfileObservationResult, error) {
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
	sanitizedObservation, err := json.Marshal(observation)
	if err != nil {
		return mcp.ModbusProfileObservationResult{}, fmt.Errorf("encode sanitized profile observation: %w", err)
	}
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
		DetectionEvidence:  append([]string{}, record.DetectionEvidence...),
		ActivationEvidence: append([]string{}, record.ActivationEvidence...),
		ObservationJSONB64: base64.StdEncoding.EncodeToString(sanitizedObservation),
		Replay:             views,
	}, nil
}

func sunSpecQualificationObservationResult(observation modbusreg.SunSpecQualificationObservation, encoded []byte) (mcp.ModbusProfileObservationResult, error) {
	if len(encoded) == 0 {
		return mcp.ModbusProfileObservationResult{}, errors.New("SunSpec qualification observation is not serializable")
	}
	var envelope any
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		return mcp.ModbusProfileObservationResult{}, fmt.Errorf("decode SunSpec qualification observation envelope: %w", err)
	}
	redactModbusEndpoints(envelope)
	sanitized, err := json.Marshal(envelope)
	if err != nil {
		return mcp.ModbusProfileObservationResult{}, fmt.Errorf("encode sanitized SunSpec qualification observation: %w", err)
	}
	replay, err := observation.Replay()
	if err != nil {
		return mcp.ModbusProfileObservationResult{}, fmt.Errorf("replay SunSpec qualification observation: %w", err)
	}
	views := make([]mcp.ModbusReplayView, 0, len(replay.SourceViews()))
	for _, source := range replay.SourceViews() {
		view := source.Record()
		views = append(views, mcp.ModbusReplayView{
			LogicalViewID: view.LogicalViewID, WireResponseID: view.WireResponseID,
			Offset: view.LogicalOffset, Words: append([]uint16(nil), view.Words...),
		})
	}
	identity := observation.SampleIdentity()
	return mcp.ModbusProfileObservationResult{
		ProfileID:          observation.Capability().ProfileID(),
		SampleID:           observation.SampleID(),
		PollGenerationID:   identity.PollGeneration(),
		SourceValidity:     "terminal_verified",
		ObservationJSONB64: base64.StdEncoding.EncodeToString(sanitized),
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
			if strings.Contains(strings.ToLower(key), "endpoint") {
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
