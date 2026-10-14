package semhub

import (
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// EventKind names one hub event. Events carry identifiers and digests only.
type EventKind string

const (
	EventEpochAdmitted       EventKind = "epoch_admitted"
	EventGenerationActivated EventKind = "generation_activated"
	EventPublished           EventKind = "published"
	EventGenerationFenced    EventKind = "generation_fenced"
	EventSourceRetired       EventKind = "source_retired"
	EventControlFailed       EventKind = "control_failed"
	EventGenerationRevoked   EventKind = "generation_revoked"
	EventDeclareRefused      EventKind = "declare_refused"
	EventOverflow            EventKind = "overflow"
)

// Event is one ordered hub event. HubSeq is assigned at the commit point and
// starts at 1 for every hub incarnation; an Overflow event consumes none.
type Event struct {
	HubSeq     uint64                 `json:"hub_seq,omitempty"`
	Kind       EventKind              `json:"kind"`
	Asset      semreg.AssetID         `json:"asset,omitempty"`
	Source     semreg.SourceID        `json:"source,omitempty"`
	Epoch      semreg.SourceEpochID   `json:"epoch,omitempty"`
	Generation uint64                 `json:"generation,omitempty"`
	Sequence   semreg.Uint64          `json:"sequence,omitempty"`
	SnapshotID semreg.SnapshotID      `json:"snapshot_id,omitempty"`
	Revisions  *semreg.RevisionVector `json:"revisions,omitempty"`
	Digest     semreg.Digest          `json:"digest,omitempty"`
	Control    *ControlID             `json:"control,omitempty"`
	// ErrorID is the verbatim SemReg error identifier of a failed control.
	ErrorID semreg.ErrorID `json:"error_id,omitempty"`
	// Reason is diagnostic metadata of a fence or retirement.
	Reason semreg.DefinitionID `json:"reason,omitempty"`
	// NoPublication marks a hub-level revocation without a SemReg batch.
	NoPublication bool `json:"no_publication,omitempty"`
	// Drivers names the refusing and owning driver labels of a refused
	// declaration; it never carries identity material.
	Drivers []string `json:"drivers,omitempty"`
	// Dropped and ResumeSeq describe an Overflow.
	Dropped   uint64 `json:"dropped,omitempty"`
	ResumeSeq uint64 `json:"resume_seq,omitempty"`
}
