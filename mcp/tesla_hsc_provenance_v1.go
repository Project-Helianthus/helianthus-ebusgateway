package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

const maxTeslaHSCV1ProvenancePayload = 252

// TeslaHSCProvenanceV1Record is redacted, already-correlated terminal-response
// provenance. It deliberately carries no raw response bytes or send authority.
type TeslaHSCProvenanceV1Record struct {
	Function        byte   `json:"function"`
	PayloadLength   int    `json:"payload_length"`
	PayloadDigest   string `json:"payload_digest"`
	OutboundAllowed bool   `json:"outbound_allowed"`
}

// TeslaHSCProvenanceV1Provider supplies only selected FC101/FC102 terminal
// provenance. It has no request-construction or transport-I/O capability.
type TeslaHSCProvenanceV1Provider interface {
	TeslaHSCProvenanceV1(context.Context) ([]TeslaHSCProvenanceV1Record, error)
}

// TeslaHSCProvenanceV1Runtime projects injected redacted terminal provenance
// for an MCP caller without constructing a request or transmitting anything.
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

// TeslaHSCProvenanceV1 validates and projects selected terminal provenance.
// Every returned record is independently copied and permanently outbound-denied.
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
		record.OutboundAllowed = false
		projected = append(projected, record)
	}
	return projected, nil
}

func validTeslaHSCProvenanceV1Record(record TeslaHSCProvenanceV1Record) bool {
	if (record.Function != 101 && record.Function != 102) ||
		record.PayloadLength < 0 || record.PayloadLength > maxTeslaHSCV1ProvenancePayload {
		return false
	}
	digest, err := hex.DecodeString(record.PayloadDigest)
	return err == nil && len(digest) == sha256.Size
}
