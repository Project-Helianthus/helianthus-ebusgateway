package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

const growattBMSRS485ClockEpoch = "system-monotonic-v1"

var growattBMSRS485Revision = modbusreg.GrowattBMSRevisionTuple{
	Family: "1xSxxP ESS", FileRevision: "Rev2.01", HeaderVersion: "V2.0", CumulativeRevision: "2.02",
}

// GrowattBMSRS485ObservationEvidence is one completed qualified native sample.
// It is intentionally protocol-native and has no SemReg or consumer projection.
type GrowattBMSRS485ObservationEvidence struct {
	ObservationID    string
	Revision         uint64
	SourceID         string
	SourceEpoch      string
	DriverGeneration uint64
	ClockEpoch       string
	ReceiptWall      time.Time
	ReceiptMonotonic time.Duration
	Qualification    string
	OutboundAllowed  bool
	Slices           []GrowattBMSRS485SliceEvidence
}

type GrowattBMSRS485SliceEvidence struct {
	UnitID              byte
	Function            modbus.FunctionCode
	Offset, Quantity    uint16
	RequestID           uint64
	TransportGeneration uint64
	RequestADUHex       string
	ResponseADUHex      string
	Words               []uint16
}

func (e GrowattBMSRS485ObservationEvidence) clone() GrowattBMSRS485ObservationEvidence {
	slices := e.Slices
	e.Slices = make([]GrowattBMSRS485SliceEvidence, len(slices))
	for i, slice := range slices {
		e.Slices[i] = slice
		e.Slices[i].Words = append([]uint16(nil), slice.Words...)
	}
	return e
}

type growattBMSRTUEndpoint interface {
	Read(context.Context, byte, modbus.ReadRegistersRequest) (modbus.ReadRegistersResponse, modbus.RTUReadEvidence, error)
	Recover(context.Context) error
	Close() error
}

type growattBMSRTUSession struct {
	endpoint         growattBMSRTUEndpoint
	sourceID         string
	sourceEpoch      string
	driverGeneration uint64
	unitID           byte
	mu               sync.Mutex
	slices           []GrowattBMSRS485SliceEvidence
	lastReceipt      time.Time
	lastMonotonic    time.Duration
	clockEpoch       string
	recoveryNeeded   bool
}

func (session *growattBMSRTUSession) begin() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.slices = nil
	session.lastReceipt = time.Time{}
	session.lastMonotonic = 0
	session.clockEpoch = ""
}

// ReadHolding delegates all framing, correlation, and wire error handling to
// the upstream RTU endpoint. This adapter only binds its immutable receipt to
// the admitted exact Growatt tuple and preserves it for a completed sample.
func (session *growattBMSRTUSession) ReadHolding(ctx context.Context, unitID byte, request modbus.ReadRegistersRequest) (modbus.ReadRegistersResponse, error) {
	if session == nil || session.endpoint == nil || unitID != session.unitID ||
		request.Function() != modbus.FunctionReadHoldingRegisters {
		return modbus.ReadRegistersResponse{}, errors.New("growatt BMS RTU session admission rejected")
	}
	response, receipt, err := session.endpoint.Read(ctx, unitID, request)
	if err != nil {
		if receipt.TerminalOutcome == "write_fault" || receipt.TerminalOutcome == "transport_fault" {
			session.mu.Lock()
			session.recoveryNeeded = true
			session.mu.Unlock()
		}
		return modbus.ReadRegistersResponse{}, err
	}
	if !receipt.Current || !receipt.IntegrityValid || receipt.TerminalOutcome != "success" ||
		receipt.UnitID != unitID || receipt.Function != request.Function() ||
		receipt.Offset != request.Offset() || receipt.Quantity != request.Quantity() ||
		receipt.ClockEpoch != growattBMSRS485ClockEpoch || receipt.Generation == 0 ||
		response.Provenance != (modbus.ReadProvenance{Function: request.Function(), Table: modbus.HoldingRegisters, Offset: request.Offset(), Quantity: request.Quantity()}) {
		return modbus.ReadRegistersResponse{}, errors.New("growatt BMS RTU response binding rejected")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.clockEpoch != "" && session.clockEpoch != receipt.ClockEpoch {
		return modbus.ReadRegistersResponse{}, errors.New("growatt BMS RTU clock epoch changed during sample")
	}
	if len(session.slices) != 0 && session.slices[0].TransportGeneration != receipt.Generation {
		return modbus.ReadRegistersResponse{}, errors.New("growatt BMS RTU transport generation changed during sample")
	}
	session.clockEpoch = receipt.ClockEpoch
	session.lastReceipt = receipt.ReceiptWall
	session.lastMonotonic = receipt.ReceivedAt
	session.slices = append(session.slices, GrowattBMSRS485SliceEvidence{
		UnitID: unitID, Function: request.Function(), Offset: request.Offset(), Quantity: request.Quantity(),
		RequestID: receipt.RequestID, TransportGeneration: receipt.Generation,
		RequestADUHex: hex.EncodeToString(receipt.RequestADU), ResponseADUHex: hex.EncodeToString(receipt.ResponseADU),
		Words: append([]uint16(nil), receipt.Words...),
	})
	return response, nil
}

func (session *growattBMSRTUSession) recover(ctx context.Context) error {
	if session == nil || session.endpoint == nil {
		return errors.New("growatt BMS RTU session unavailable")
	}
	session.mu.Lock()
	needed := session.recoveryNeeded
	session.mu.Unlock()
	if !needed {
		return nil
	}
	if err := session.endpoint.Recover(ctx); err != nil {
		return err
	}
	session.mu.Lock()
	session.recoveryNeeded = false
	session.mu.Unlock()
	return nil
}

func (session *growattBMSRTUSession) complete(observationID string, revision uint64) (GrowattBMSRS485ObservationEvidence, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.sourceID == "" || session.sourceEpoch == "" || session.driverGeneration == 0 ||
		observationID == "" || revision == 0 || len(session.slices) != 4 ||
		session.lastReceipt.IsZero() || session.clockEpoch != growattBMSRS485ClockEpoch {
		return GrowattBMSRS485ObservationEvidence{}, errors.New("growatt BMS RTU observation evidence is incomplete")
	}
	return GrowattBMSRS485ObservationEvidence{
		ObservationID: observationID, Revision: revision, SourceID: session.sourceID, SourceEpoch: session.sourceEpoch,
		DriverGeneration: session.driverGeneration, ClockEpoch: session.clockEpoch, ReceiptWall: session.lastReceipt,
		ReceiptMonotonic: session.lastMonotonic, Qualification: "qualified", OutboundAllowed: false,
		Slices: append([]GrowattBMSRS485SliceEvidence(nil), session.slices...),
	}.clone(), nil
}

type growattBMSRS485ProductionProvider struct {
	mu      sync.Mutex
	gate    chan struct{}
	runtime *mcp.GrowattBMSRS485V202Runtime
	session *growattBMSRTUSession
	storage *growattStoragePublication
	next    uint64
	last    GrowattBMSRS485ObservationEvidence
	have    bool
}

func (provider *growattBMSRS485ProductionProvider) GrowattBMSRS485V202(ctx context.Context) (mcp.GrowattBMSRS485V202Observation, error) {
	if provider == nil || provider.runtime == nil || provider.session == nil {
		return mcp.GrowattBMSRS485V202Observation{}, errors.New("growatt BMS RTU production provider unavailable")
	}
	if err := provider.acquire(ctx); err != nil {
		return mcp.GrowattBMSRS485V202Observation{}, err
	}
	defer provider.release()
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.observeLocked(ctx)
}

func (provider *growattBMSRS485ProductionProvider) acquire(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case provider.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			provider.release()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (provider *growattBMSRS485ProductionProvider) release() { <-provider.gate }

// observeLocked is the single native observation transaction. Both the native
// MCP tool and the semantic path use it, so a typed status can never be paired
// with evidence from a different observation generation.
func (provider *growattBMSRS485ProductionProvider) observeLocked(ctx context.Context) (mcp.GrowattBMSRS485V202Observation, error) {
	if err := provider.session.recover(ctx); err != nil {
		return mcp.GrowattBMSRS485V202Observation{}, err
	}
	provider.session.begin()
	status, err := provider.runtime.GrowattBMSRS485V202(ctx)
	if err != nil {
		return mcp.GrowattBMSRS485V202Observation{}, err
	}
	provider.next++
	evidence, err := provider.session.complete(fmt.Sprintf("%s:%s:%d:%d", provider.session.sourceID, provider.session.sourceEpoch, provider.session.driverGeneration, provider.next), provider.next)
	if err != nil {
		return mcp.GrowattBMSRS485V202Observation{}, err
	}
	provider.last, provider.have = evidence, true
	return mcp.GrowattBMSRS485V202Observation{Status: status, ReceiptWall: evidence.ReceiptWall}, nil
}

// SemanticStorageCurrent performs the one SemReg-only storage publication from
// a completed native read. It contains no control, retry, or fallback path.
func (provider *growattBMSRS485ProductionProvider) SemanticStorageCurrent(ctx context.Context) (any, error) {
	if provider == nil || provider.storage == nil {
		return nil, errors.New("growatt BMS storage semantic provider unavailable")
	}
	if err := provider.acquire(ctx); err != nil {
		return nil, err
	}
	defer provider.release()
	provider.mu.Lock()
	defer provider.mu.Unlock()
	observation, err := provider.observeLocked(ctx)
	if err != nil {
		return nil, err
	}
	evidence, ok := provider.LastObservationEvidenceLocked()
	if !ok {
		return nil, errors.New("growatt BMS storage evidence unavailable")
	}
	return provider.storage.Publish(observation.Status, evidence)
}

func (provider *growattBMSRS485ProductionProvider) GrowattStorageSemanticCurrent(ctx context.Context) (any, error) {
	return provider.SemanticStorageCurrent(ctx)
}

func (provider *growattBMSRS485ProductionProvider) LastObservationEvidence() (GrowattBMSRS485ObservationEvidence, bool) {
	if provider == nil {
		return GrowattBMSRS485ObservationEvidence{}, false
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.last.clone(), provider.have
}

func (provider *growattBMSRS485ProductionProvider) LastObservationEvidenceLocked() (GrowattBMSRS485ObservationEvidence, bool) {
	if provider == nil {
		return GrowattBMSRS485ObservationEvidence{}, false
	}
	return provider.last.clone(), provider.have
}

func (provider *growattBMSRS485ProductionProvider) Close() error {
	if provider == nil || provider.session == nil || provider.session.endpoint == nil {
		return nil
	}
	if err := provider.acquire(context.Background()); err != nil {
		return err
	}
	defer provider.release()
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.session.endpoint.Close()
}

var openGrowattBMSRTUEndpoint = func(config modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) {
	return modbus.OpenRTUProductionEndpoint(config)
}

func startGrowattBMSRS485Runtime(config ebusgateway.GrowattBMSRS485Config) (*growattBMSRS485ProductionProvider, error) {
	if !config.Enabled {
		if config != (ebusgateway.GrowattBMSRS485Config{}) {
			return nil, errors.New("disabled Growatt BMS RS-485 configuration contains active fields")
		}
		return nil, nil
	}
	if !validGrowattAssetID(config.AssetID) || !validGrowattSourceID(config.SourceID) || config.AssetID == config.SourceID || semreg.SourceEpochID(config.SourceEpoch).Validate() != nil || config.DriverGeneration == 0 || config.UnitID == 0 || config.UnitID > 247 ||
		config.SerialPath == "" || config.ResponseTimeout <= 0 || config.MaxResponseDelay <= 0 || config.MaxQuiescence <= config.MaxResponseDelay {
		return nil, errors.New("enabled Growatt BMS RS-485 configuration is incomplete")
	}
	parity := modbus.RTUParity(config.Parity)
	timing, err := modbus.NewRTUTiming(modbus.RTUTimingConfig{
		Baud: config.Baud, DataBits: 8, Parity: parity, StopBits: config.StopBits,
		MaxResponseLatency: config.MaxResponseDelay, MaxQuiescence: config.MaxQuiescence,
	})
	if err != nil {
		return nil, fmt.Errorf("growatt BMS RS-485 timing: %w", err)
	}
	admission := growattBMSRTUAdmission{unitID: config.UnitID}
	endpoint, err := openGrowattBMSRTUEndpoint(modbus.RTUProductionConfig{
		Endpoint: config.SourceID,
		Serial:   modbus.RTUSerialConfig{Path: config.SerialPath, Baud: config.Baud, DataBits: 8, Parity: parity, StopBits: config.StopBits},
		Timing:   timing, ResponseTimeout: config.ResponseTimeout, Enabled: true, Admission: admission,
	})
	if err != nil {
		return nil, err
	}
	session := &growattBMSRTUSession{endpoint: endpoint, sourceID: config.SourceID, sourceEpoch: config.SourceEpoch, driverGeneration: config.DriverGeneration, unitID: config.UnitID}
	runtime, err := mcp.NewGrowattBMSRS485V202Runtime(growattBMSRS485Revision, config.UnitID, session)
	if err != nil {
		_ = endpoint.Close()
		return nil, err
	}
	storage, err := newGrowattStoragePublication(config.AssetID, config.SourceID)
	if err != nil {
		_ = endpoint.Close()
		return nil, err
	}
	return &growattBMSRS485ProductionProvider{runtime: runtime, session: session, storage: storage, gate: make(chan struct{}, 1)}, nil
}

type growattBMSRTUAdmission struct{ unitID byte }

func (admission growattBMSRTUAdmission) AdmitRTURead(request modbus.RTUReadAdmissionRequest) bool {
	return request.UnitID == admission.unitID && request.UnitID != 0 && request.UnitID <= 247 &&
		request.Function == modbus.FunctionReadHoldingRegisters &&
		(request.Offset == 0x0001 && request.Quantity == 7 ||
			request.Offset == 0x000d && request.Quantity == 29 ||
			request.Offset == 0x0100 && request.Quantity == 12 ||
			request.Offset == 0x010d && request.Quantity == 2)
}
