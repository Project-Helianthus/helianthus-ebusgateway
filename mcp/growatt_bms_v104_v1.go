package mcp

import (
	"context"
	"errors"
)

const GrowattBMSV104Profile = "growatt.bms.low_voltage.can.v1_04"

var (
	ErrGrowattBMSV104ProviderUnavailable = errors.New("growatt BMS V1.04 provider is unavailable")
	ErrGrowattBMSV104NotAdmitted         = errors.New("growatt BMS V1.04 snapshot is not admitted")
)

// GrowattBMSV104RawEvidence is retained by the injected provider only.
// It is deliberately absent from the MCP-facing result.
type GrowattBMSV104RawEvidence struct {
	Identifier uint32
	Data       [8]byte
}

type GrowattBMSV104Limits struct {
	ChargeVoltageDecivolts   uint16 `json:"charge_voltage_decivolts"`
	ChargeCurrentDeciamps    uint16 `json:"charge_current_deciamps"`
	DischargeCurrentDeciamps uint16 `json:"discharge_current_deciamps"`
	RawStatus                uint16 `json:"raw_status"`
}

type GrowattBMSV104Status struct {
	Protection     [2]byte `json:"protection"`
	Warning        [2]byte `json:"warning"`
	PackCount      uint8   `json:"pack_count"`
	Manufacturer   [2]byte `json:"manufacturer"`
	TotalCellCount uint8   `json:"total_cell_count"`
}

type GrowattBMSV104Measurements struct {
	VoltageCentivolts           int16 `json:"voltage_centivolts"`
	CurrentDeciamps             int16 `json:"current_deciamps"`
	MaximumCellTemperatureDeciC int16 `json:"maximum_cell_temperature_decic"`
	SOCPercent                  uint8 `json:"soc_percent"`
	SOHValue                    uint8 `json:"soh_value"`
	SOHValid                    bool  `json:"soh_valid"`
}

// GrowattBMSV104ProviderSnapshot is the internal injected input boundary.
type GrowattBMSV104ProviderSnapshot struct {
	Profile         string                      `json:"profile"`
	Admitted        bool                        `json:"admitted"`
	OutboundAllowed bool                        `json:"outbound_allowed"`
	Limits          GrowattBMSV104Limits        `json:"limits"`
	Status          GrowattBMSV104Status        `json:"status"`
	Measurements    GrowattBMSV104Measurements  `json:"measurements"`
	RawEvidence     []GrowattBMSV104RawEvidence `json:"-"`
}

// GrowattBMSV104SnapshotProvider supplies an already-admitted snapshot without transport I/O.
type GrowattBMSV104SnapshotProvider interface {
	GrowattBMSV104Snapshot(context.Context) (GrowattBMSV104ProviderSnapshot, error)
}

// GrowattBMSV104Result is the safe, read-only MCP-facing projection.
type GrowattBMSV104Result struct {
	Profile             string                     `json:"profile"`
	Admitted            bool                       `json:"admitted"`
	OutboundAllowed     bool                       `json:"outbound_allowed"`
	RawEvidenceRedacted bool                       `json:"raw_evidence_redacted"`
	Limits              GrowattBMSV104Limits       `json:"limits"`
	Status              GrowattBMSV104Status       `json:"status"`
	Measurements        GrowattBMSV104Measurements `json:"measurements"`
}

// GrowattBMSV104Runtime owns the injected read-only snapshot boundary.
type GrowattBMSV104Runtime struct {
	provider GrowattBMSV104SnapshotProvider
}

func NewGrowattBMSV104Runtime(provider GrowattBMSV104SnapshotProvider) (*GrowattBMSV104Runtime, error) {
	if provider == nil {
		return nil, ErrGrowattBMSV104ProviderUnavailable
	}
	return &GrowattBMSV104Runtime{provider: provider}, nil
}

func (runtime *GrowattBMSV104Runtime) SnapshotGet(ctx context.Context) (GrowattBMSV104Result, error) {
	if runtime == nil || runtime.provider == nil {
		return GrowattBMSV104Result{}, ErrGrowattBMSV104ProviderUnavailable
	}

	snapshot, err := runtime.provider.GrowattBMSV104Snapshot(ctx)
	if err != nil {
		return GrowattBMSV104Result{}, err
	}
	if !snapshot.Admitted || snapshot.Profile != GrowattBMSV104Profile {
		return GrowattBMSV104Result{}, ErrGrowattBMSV104NotAdmitted
	}

	return GrowattBMSV104Result{
		Profile:             GrowattBMSV104Profile,
		Admitted:            true,
		OutboundAllowed:     false,
		RawEvidenceRedacted: true,
		Limits:              snapshot.Limits,
		Status:              snapshot.Status,
		Measurements:        snapshot.Measurements,
	}, nil
}
