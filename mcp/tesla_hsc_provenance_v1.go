package mcp

import (
	"context"
	"errors"
)

const maxTeslaHSCV1ProvenancePayload = 252

// TeslaHSCProvenanceV1Record is one already-correlated native FC101 or FC102
// terminal response with its firmware-scoped compatibility and provenance.
type TeslaHSCProvenanceV1Record struct {
	Function        byte   `json:"function"`
	Payload         []byte `json:"payload"`
	Compatibility   string `json:"compatibility"`
	Provenance      string `json:"provenance"`
	OutboundAllowed bool   `json:"outbound_allowed"`
}

// TeslaHSCProvenanceV1Provider supplies only selected FC101/FC102 terminal
// provenance. It has no request-construction or transport-I/O capability.
type TeslaHSCProvenanceV1Provider interface {
	TeslaHSCProvenanceV1(context.Context) ([]TeslaHSCProvenanceV1Record, error)
}

// TeslaHSCProvenanceV1Runtime projects injected native terminal records without
// constructing a request or transmitting anything.
type TeslaHSCProvenanceV1Runtime struct {
	provider TeslaHSCProvenanceV1Provider
}

// NewTeslaHSCProvenanceV1Runtime constructs an injected read-only provenance
// runtime. A nil provider is never an implicit transport fallback.
func NewTeslaHSCProvenanceV1Runtime(provider TeslaHSCProvenanceV1Provider) (*TeslaHSCProvenanceV1Runtime, error) {
	if provider == nil {
		return nil, errors.New("tesla HSC provenance provider is required")
	}
	return &TeslaHSCProvenanceV1Runtime{provider: provider}, nil
}

// TeslaHSCProvenanceV1 validates and projects selected native terminal records.
// Every returned record is independently copied while retaining its context.
func (runtime *TeslaHSCProvenanceV1Runtime) TeslaHSCProvenanceV1(ctx context.Context) ([]TeslaHSCProvenanceV1Record, error) {
	if runtime == nil || runtime.provider == nil || ctx == nil {
		return nil, errors.New("tesla HSC provenance runtime is unavailable")
	}
	records, err := runtime.provider.TeslaHSCProvenanceV1(ctx)
	if err != nil {
		return nil, err
	}
	projected := make([]TeslaHSCProvenanceV1Record, 0, len(records))
	for _, record := range records {
		if !validTeslaHSCProvenanceV1Record(record) {
			return nil, errors.New("tesla HSC provenance record is invalid")
		}
		projected = append(projected, copyTeslaHSCProvenanceV1Record(record))
	}
	return projected, nil
}

func validTeslaHSCProvenanceV1Record(record TeslaHSCProvenanceV1Record) bool {
	if (record.Function != 101 && record.Function != 102) ||
		len(record.Payload) > maxTeslaHSCV1ProvenancePayload ||
		record.Compatibility == "" || record.Provenance == "" ||
		len(record.Compatibility) > maxTeslaHSCV1NativeMetadataBytes ||
		len(record.Provenance) > maxTeslaHSCV1NativeMetadataBytes {
		return false
	}
	return true
}

func copyTeslaHSCProvenanceV1Record(record TeslaHSCProvenanceV1Record) TeslaHSCProvenanceV1Record {
	if record.Payload != nil {
		payload := make([]byte, len(record.Payload))
		copy(payload, record.Payload)
		record.Payload = payload
	}
	return record
}
