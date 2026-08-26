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

// GreeVRFCANCandidateV1RawEvidence preserves the available native observation
// context without assigning HVAC semantics to its bytes.
type GreeVRFCANCandidateV1RawEvidence struct {
	Interface      string  `json:"interface"`
	Sequence       uint64  `json:"sequence"`
	MonotonicNanos int64   `json:"monotonic_nanos"`
	Identifier     uint32  `json:"identifier"`
	Extended       bool    `json:"extended"`
	DLC            uint8   `json:"dlc"`
	RawDLC         uint8   `json:"raw_dlc"`
	Data           [8]byte `json:"data"`
}

// GreeVRFCANCandidateV1OpaqueCell is a bounded opaque candidate cell value.
// It has no unit, range, direction, or HVAC semantic projection.
type GreeVRFCANCandidateV1OpaqueCell struct {
	Cell  uint8 `json:"cell"`
	Value uint8 `json:"value"`
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
	Profile         string                             `json:"profile"`
	Admitted        bool                               `json:"admitted"`
	OutboundAllowed bool                               `json:"outbound_allowed"`
	Class8          uint8                              `json:"class8"`
	Opaque7         uint8                              `json:"opaque7"`
	Unit7           uint8                              `json:"unit7"`
	Opcode7         uint8                              `json:"opcode7"`
	OpaqueCells     []GreeVRFCANCandidateV1OpaqueCell  `json:"opaque_cells"`
	RawEvidence     []GreeVRFCANCandidateV1RawEvidence `json:"raw_evidence"`
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

	cells := append([]GreeVRFCANCandidateV1OpaqueCell(nil), snapshot.OpaqueCells...)
	rawEvidence := make([]GreeVRFCANCandidateV1RawEvidence, len(snapshot.RawEvidence))
	for index, evidence := range snapshot.RawEvidence {
		rawEvidence[index] = greeVRFCANCandidateV1BoundRawEvidence(evidence)
	}
	sort.Slice(cells, func(left, right int) bool {
		if cells[left].Cell != cells[right].Cell {
			return cells[left].Cell < cells[right].Cell
		}
		return cells[left].Value < cells[right].Value
	})

	return GreeVRFCANCandidateV1Result{
		Profile:         GreeVRFCANCandidateV1Profile,
		Admitted:        true,
		OutboundAllowed: snapshot.OutboundAllowed,
		Class8:          snapshot.Class8,
		Opaque7:         snapshot.Opaque7,
		Unit7:           snapshot.Unit7,
		Opcode7:         snapshot.Opcode7,
		OpaqueCells:     cells,
		RawEvidence:     rawEvidence,
	}, nil
}

func greeVRFCANCandidateV1Admitted(snapshot GreeVRFCANCandidateV1ProviderSnapshot) bool {
	if snapshot.Profile != GreeVRFCANCandidateV1Profile || !snapshot.Admitted || snapshot.Class8 != 0xf7 || snapshot.Opaque7 > 0x7f || snapshot.Unit7 != 8 || !greeVRFCANCandidateV1Opcode(snapshot.Opcode7) || len(snapshot.OpaqueCells) == 0 {
		return false
	}
	seen := [256]bool{}
	for _, cell := range snapshot.OpaqueCells {
		if cell.Cell < 0x0f || cell.Cell > 0x1b {
			return false
		}
		if seen[cell.Cell] {
			return false
		}
		seen[cell.Cell] = true
	}
	for _, evidence := range snapshot.RawEvidence {
		if !greeVRFCANCandidateV1RawEvidenceValid(evidence) {
			return false
		}
	}
	return true
}

func greeVRFCANCandidateV1RawEvidenceValid(evidence GreeVRFCANCandidateV1RawEvidence) bool {
	if evidence.DLC > uint8(len(evidence.Data)) {
		return false
	}
	if evidence.DLC < uint8(len(evidence.Data)) {
		return evidence.RawDLC == evidence.DLC
	}
	return evidence.RawDLC >= uint8(len(evidence.Data)) && evidence.RawDLC <= 15
}

func greeVRFCANCandidateV1BoundRawEvidence(evidence GreeVRFCANCandidateV1RawEvidence) GreeVRFCANCandidateV1RawEvidence {
	// Storage beyond DLC is not part of the native CAN payload.
	for index := int(evidence.DLC); index < len(evidence.Data); index++ {
		evidence.Data[index] = 0
	}
	return evidence
}

func greeVRFCANCandidateV1Opcode(opcode uint8) bool {
	switch opcode {
	case 0x10, 0x11, 0x52, 0x58:
		return true
	default:
		return false
	}
}
