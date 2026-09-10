package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
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
}

type semanticPVAdapter interface {
	SemanticPVCurrent(string, string) (modbusadapter.SemanticPVCurrent, bool)
}

func newGatewayModbusMCPProvider(adapter *modbusadapter.Adapter) mcp.ModbusV1Provider {
	return newGatewayModbusMCPProviderWithGrowatt(adapter, nil)
}

// newGatewayModbusMCPProviderWithGrowatt accepts the concrete lifecycle result
// so a disabled (*growattBMSRS485ProductionProvider)(nil) is checked before it
// can become a non-nil optional-provider interface.
func newGatewayModbusMCPProviderWithGrowatt(adapter *modbusadapter.Adapter, growatt *growattBMSRS485ProductionProvider) mcp.ModbusV1Provider {
	if adapter == nil && growatt == nil {
		return nil
	}
	core := &gatewayModbusMCPProvider{now: time.Now}
	if adapter != nil {
		core.adapter = adapter
	}
	if growatt == nil {
		return core
	}
	return gatewayGrowattBMSMCPProvider{gatewayModbusMCPProvider: core, growatt: growatt, storage: growatt}
}

// gatewayModbusRuntimeProvider composes independent native owners without
// widening the core Modbus provider or adding a transport path to either
// optional read-only surface.
type gatewayModbusRuntimeProvider struct {
	*gatewayModbusMCPProvider
	growatt *growattBMSRS485ProductionProvider
	tesla   *teslaHSCRetainedOwner
}

func newGatewayModbusMCPProviderWithRuntimes(adapter *modbusadapter.Adapter, growatt *growattBMSRS485ProductionProvider, tesla *teslaHSCRetainedOwner) mcp.ModbusV1Provider {
	if tesla == nil {
		return newGatewayModbusMCPProviderWithGrowatt(adapter, growatt)
	}
	core := &gatewayModbusMCPProvider{now: time.Now}
	if adapter != nil {
		core.adapter = adapter
	}
	return &gatewayModbusRuntimeProvider{gatewayModbusMCPProvider: core, growatt: growatt, tesla: tesla}
}

func (provider *gatewayModbusRuntimeProvider) GrowattBMSRS485V202(ctx context.Context) (mcp.GrowattBMSRS485V202Observation, error) {
	if provider == nil || provider.growatt == nil {
		return mcp.GrowattBMSRS485V202Observation{}, mcp.ErrGrowattBMSRS485V202ProviderUnavailable
	}
	return provider.growatt.GrowattBMSRS485V202(ctx)
}

func (provider *gatewayModbusRuntimeProvider) GrowattStorageSemanticCurrent(ctx context.Context) (any, error) {
	if provider == nil || provider.growatt == nil {
		return nil, mcp.ErrGrowattStorageSemanticProviderUnavailable
	}
	return provider.growatt.GrowattStorageSemanticCurrent(ctx)
}

func (provider *gatewayModbusRuntimeProvider) TeslaGen3EVSECurrentLimitV1(ctx context.Context) (mcp.TeslaGen3EVSECurrentLimitV1Source, error) {
	if provider == nil || provider.tesla == nil {
		return mcp.TeslaGen3EVSECurrentLimitV1Source{}, mcp.ErrTeslaGen3EVSECurrentLimitV1ProviderUnavailable
	}
	return provider.tesla.TeslaGen3EVSECurrentLimitV1(ctx)
}

func (provider *gatewayModbusRuntimeProvider) TeslaGen3EVSESemanticCurrent(ctx context.Context) (any, error) {
	if provider == nil || provider.tesla == nil {
		return nil, mcp.ErrTeslaGen3EVSESemanticUnavailable
	}
	return provider.tesla.TeslaGen3EVSESemanticCurrent(ctx)
}

type gatewayGrowattBMSMCPProvider struct {
	*gatewayModbusMCPProvider
	growatt mcp.GrowattBMSRS485V202Provider
	storage mcp.GrowattStorageSemanticProvider
}

func (provider gatewayGrowattBMSMCPProvider) ModbusV1CoreAvailable() bool {
	return provider.gatewayModbusMCPProvider != nil && provider.gatewayModbusMCPProvider.ModbusV1CoreAvailable()
}

// ModbusV1CoreAvailable keeps the independent RTU observer from advertising
// TCP raw/profile/SemReg tools when the TCP sidecar is not composed.
func (provider *gatewayModbusMCPProvider) ModbusV1CoreAvailable() bool {
	return provider != nil && provider.adapter != nil
}

func (provider gatewayGrowattBMSMCPProvider) GrowattBMSRS485V202(ctx context.Context) (mcp.GrowattBMSRS485V202Observation, error) {
	if provider.growatt == nil {
		return mcp.GrowattBMSRS485V202Observation{}, mcp.ErrGrowattBMSRS485V202ProviderUnavailable
	}
	return provider.growatt.GrowattBMSRS485V202(ctx)
}

func (provider gatewayGrowattBMSMCPProvider) GrowattStorageSemanticCurrent(ctx context.Context) (any, error) {
	if provider.storage == nil {
		return nil, mcp.ErrGrowattStorageSemanticProviderUnavailable
	}
	return provider.storage.GrowattStorageSemanticCurrent(ctx)
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

func (provider *gatewayModbusMCPProvider) SemanticPVCurrent(_ context.Context, profileID, sampleID string) (mcp.SemanticPVCurrentResult, error) {
	if provider == nil || provider.adapter == nil || profileID != modbusreg.SunSpecThreePhaseMonitoringCapabilityID {
		return mcp.SemanticPVCurrentResult{}, errors.New("semantic PV observation unavailable")
	}
	adapter, supported := provider.adapter.(semanticPVAdapter)
	if !supported {
		return mcp.SemanticPVCurrentResult{}, errors.New("semantic PV adapter unavailable")
	}
	current, ok := adapter.SemanticPVCurrent(profileID, sampleID)
	if !ok {
		return mcp.SemanticPVCurrentResult{}, errors.New("semantic PV observation unavailable")
	}
	timestamp, err := semanticPVDataTimestamp(string(current.Evaluation.Context.EvaluatedAt.UnixNanoseconds))
	if err != nil {
		return mcp.SemanticPVCurrentResult{}, errors.New("semantic PV evaluation time unavailable")
	}
	return mcp.SemanticPVCurrentResult{Data: map[string]any{"snapshot": current.Snapshot, "evaluation": current.Evaluation, "selections": current.Selections, "projection": current.Projection}, Evaluated: timestamp}, nil
}

func semanticPVDataTimestamp(nanoseconds string) (string, error) {
	value, err := strconv.ParseInt(nanoseconds, 10, 64)
	if err != nil {
		return "", err
	}
	return time.Unix(0, value).UTC().Format(time.RFC3339Nano), nil
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
