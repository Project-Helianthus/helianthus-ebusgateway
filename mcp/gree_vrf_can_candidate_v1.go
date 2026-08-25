package mcp

import (
	"context"
	"errors"
	"sort"
)

const GreeVRFCANCandidateV1Profile = "gree.vrf.canbus.candidate.v1"

var (
	ErrGreeVRFCANCandidateV1ProviderUnavailable = errors.New("gree VRF CAN candidate provider is unavailable")
	ErrGreeVRFCANCandidateV1NotAdmitted         = errors.New("gree VRF CAN candidate snapshot is not admitted")
)

// GreeVRFCANCandidateV1RawEvidence is retained by the injected provider only.
// It is deliberately absent from the MCP-facing result.
type GreeVRFCANCandidateV1RawEvidence struct {
	Identifier uint32
	Data       [8]byte
}

// GreeVRFCANCandidateV1OpaqueCell is provider-only candidate cell content.
type GreeVRFCANCandidateV1OpaqueCell struct {
	Cell  uint8
	Value uint8
}

// GreeVRFCANCandidateV1OpaqueCellMetadata identifies an updated opaque cell
// without exposing its value.
type GreeVRFCANCandidateV1OpaqueCellMetadata struct {
	Cell uint8 `json:"cell"`
}

// GreeVRFCANCandidateV1ProviderSnapshot is the internal injected input boundary.
type GreeVRFCANCandidateV1ProviderSnapshot struct {
	Profile         string                             `json:"profile"`
	Admitted        bool                               `json:"admitted"`
	OutboundAllowed bool                               `json:"outbound_allowed"`
	Class8          uint8                              `json:"class8"`
	Opaque7         uint8                              `json:"opaque7"`
	Unit7           uint8                              `json:"unit7"`
	Opcode7         uint8                              `json:"opcode7"`
	OpaqueCells     []GreeVRFCANCandidateV1OpaqueCell  `json:"-"`
	RawEvidence     []GreeVRFCANCandidateV1RawEvidence `json:"-"`
}

// GreeVRFCANCandidateV1SnapshotProvider supplies an already-admitted snapshot without transport I/O.
type GreeVRFCANCandidateV1SnapshotProvider interface {
	GreeVRFCANCandidateV1Snapshot(context.Context) (GreeVRFCANCandidateV1ProviderSnapshot, error)
}

// GreeVRFCANCandidateV1Result is the safe, read-only MCP-facing candidate status.
type GreeVRFCANCandidateV1Result struct {
	Profile             string                                    `json:"profile"`
	Admitted            bool                                      `json:"admitted"`
	OutboundAllowed     bool                                      `json:"outbound_allowed"`
	RawEvidenceRedacted bool                                      `json:"raw_evidence_redacted"`
	Class8              uint8                                     `json:"class8"`
	Opaque7             uint8                                     `json:"opaque7"`
	Unit7               uint8                                     `json:"unit7"`
	Opcode7             uint8                                     `json:"opcode7"`
	OpaqueCells         []GreeVRFCANCandidateV1OpaqueCellMetadata `json:"opaque_cells"`
}

// GreeVRFCANCandidateV1Runtime owns the injected read-only candidate boundary.
type GreeVRFCANCandidateV1Runtime struct {
	provider GreeVRFCANCandidateV1SnapshotProvider
}

func NewGreeVRFCANCandidateV1Runtime(provider GreeVRFCANCandidateV1SnapshotProvider) (*GreeVRFCANCandidateV1Runtime, error) {
	if provider == nil {
		return nil, ErrGreeVRFCANCandidateV1ProviderUnavailable
	}
	return &GreeVRFCANCandidateV1Runtime{provider: provider}, nil
}

func (runtime *GreeVRFCANCandidateV1Runtime) SnapshotGet(ctx context.Context) (GreeVRFCANCandidateV1Result, error) {
	if runtime == nil || runtime.provider == nil {
		return GreeVRFCANCandidateV1Result{}, ErrGreeVRFCANCandidateV1ProviderUnavailable
	}

	snapshot, err := runtime.provider.GreeVRFCANCandidateV1Snapshot(ctx)
	if err != nil {
		return GreeVRFCANCandidateV1Result{}, err
	}
	if !greeVRFCANCandidateV1Admitted(snapshot) {
		return GreeVRFCANCandidateV1Result{}, ErrGreeVRFCANCandidateV1NotAdmitted
	}

	cells := make([]GreeVRFCANCandidateV1OpaqueCellMetadata, len(snapshot.OpaqueCells))
	for index, cell := range snapshot.OpaqueCells {
		cells[index] = GreeVRFCANCandidateV1OpaqueCellMetadata{Cell: cell.Cell}
	}
	sort.Slice(cells, func(left, right int) bool {
		return cells[left].Cell < cells[right].Cell
	})

	return GreeVRFCANCandidateV1Result{
		Profile:             GreeVRFCANCandidateV1Profile,
		Admitted:            true,
		OutboundAllowed:     false,
		RawEvidenceRedacted: true,
		Class8:              snapshot.Class8,
		Opaque7:             snapshot.Opaque7,
		Unit7:               snapshot.Unit7,
		Opcode7:             snapshot.Opcode7,
		OpaqueCells:         cells,
	}, nil
}

func greeVRFCANCandidateV1Admitted(snapshot GreeVRFCANCandidateV1ProviderSnapshot) bool {
	if snapshot.Profile != GreeVRFCANCandidateV1Profile || !snapshot.Admitted || snapshot.Class8 != 0xf7 || snapshot.Opaque7 > 0x7f || snapshot.Unit7 != 8 || !greeVRFCANCandidateV1Opcode(snapshot.Opcode7) || len(snapshot.OpaqueCells) == 0 {
		return false
	}
	for _, cell := range snapshot.OpaqueCells {
		if cell.Cell < 0x0f || cell.Cell > 0x1b {
			return false
		}
	}
	return true
}

func greeVRFCANCandidateV1Opcode(opcode uint8) bool {
	switch opcode {
	case 0x10, 0x11, 0x52, 0x58:
		return true
	default:
		return false
	}
}
