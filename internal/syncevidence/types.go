package syncevidence

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"
)

const (
	BundleContractV1 = "helianthus.platform.synchronized-evidence-bundle.v1"
	ReplayContractV1 = "helianthus.platform.synchronized-evidence-replay.v1"
	SchemaVersionV1  = 1

	artifactHashDomain = "HELIANTHUS:SYNCHRONIZED-EVIDENCE-ARTIFACT:V1"
	bundleHashDomain   = "HELIANTHUS:SYNCHRONIZED-EVIDENCE-BUNDLE:V1"
	contentHashDomain  = "HELIANTHUS:EVIDENCE-CONTENT:V1"
	gitBlobHashDomain  = "HELIANTHUS:EVIDENCE-GIT-BLOB:V1"

	MaxSafeIntegerV1 = uint64(9007199254740991)
	MaximumClockSkew = uint64(1_000_000_000)
)

var (
	ErrInvalidArgument    = errors.New("invalid_argument")
	ErrContractViolation  = errors.New("contract_violation")
	ErrLimitsExceeded     = errors.New("limits.exceeded")
	ErrBundleExists       = errors.New("bundle_exists")
	ErrQuotaExceeded      = errors.New("quota_exceeded")
	ErrUnsafeStore        = errors.New("unsafe_store")
	ErrStoreLocked        = errors.New("store_locked")
	ErrStoreClosed        = errors.New("store_closed")
	ErrDurability         = errors.New("durability_failure")
	ErrCapturePending     = errors.New("capture.source_pending")
	ErrBackendUnavailable = errors.New("backend_unavailable")
)

type RuntimeKind string

const (
	RuntimeEBus     RuntimeKind = "EBUS"
	RuntimeEEBus    RuntimeKind = "EEBUS"
	RuntimeCloudApp RuntimeKind = "CLOUD_APP"
)

type SourceKind string

const (
	SourceEBusB509 SourceKind = "EBUS_B509"
	SourceEBusB524 SourceKind = "EBUS_B524"
	SourceEBusB555 SourceKind = "EBUS_B555"
	SourceEEBus    SourceKind = "EEBUS"
	SourceCloudApp SourceKind = "CLOUD_APP"
)

const (
	HistoricalEEBusContractV1 = "helianthus-eebus-mcp"
	M625EEBusContractV1       = "helianthus.eebus.m625.public-redacted-evidence.v1"
)

type Phase string

const (
	PhasePre    Phase = "pre"
	PhaseAction Phase = "action"
	PhasePost   Phase = "post"
)

type SnapshotMode string

const (
	SnapshotFrozen      SnapshotMode = "SNAPSHOT"
	SnapshotLiveRead    SnapshotMode = "LIVE_READ"
	SnapshotPrecaptured SnapshotMode = "PRECAPTURED"
)

type SourceState string

const (
	StatePresent     SourceState = "PRESENT"
	StateWithheld    SourceState = "WITHHELD"
	StateNotTested   SourceState = "NOT_TESTED"
	StateUnavailable SourceState = "UNAVAILABLE"
)

type SelectionDecision string

const (
	SelectionIncluded SelectionDecision = "INCLUDED"
	SelectionExcluded SelectionDecision = "EXCLUDED"
)

type PolicyDecision string

const (
	PolicyAllowed  PolicyDecision = "ALLOWED"
	PolicyWithheld PolicyDecision = "WITHHELD"
)

type BackendDecision string

const (
	BackendUnknown     BackendDecision = "UNKNOWN"
	BackendUnreachable BackendDecision = "UNREACHABLE"
)

type ErrorCategory string

const (
	ErrorPolicyWithheld       ErrorCategory = "POLICY_WITHHELD"
	ErrorAuthorizationDenied  ErrorCategory = "AUTHORIZATION_DENIED"
	ErrorRedactionFailed      ErrorCategory = "REDACTION_FAILED"
	ErrorExactIdentityMissing ErrorCategory = "EXACT_IDENTITY_MISSING"
	ErrorNotSelected          ErrorCategory = "NOT_SELECTED"
	ErrorBudgetExhausted      ErrorCategory = "BUDGET_EXHAUSTED"
	ErrorBackendUnavailable   ErrorCategory = "BACKEND_UNAVAILABLE"
	ErrorTimeout              ErrorCategory = "TIMEOUT"
	ErrorAcquisitionFailed    ErrorCategory = "ACQUISITION_FAILED"
)

type EvidenceKind string

const (
	EvidenceKindContent EvidenceKind = "CONTENT"
	EvidenceKindGitBlob EvidenceKind = "GIT_BLOB"
)

type DigestAlgorithm string

const (
	DigestAlgorithmContentBytes DigestAlgorithm = "SHA256_CONTENT_BYTES"
	DigestAlgorithmGitBlobV1    DigestAlgorithm = "SHA256_GIT_BLOB_V1"
)

type DropStatus string

const (
	DropStatusDropped     DropStatus = "DROPPED"
	DropStatusAlreadyGone DropStatus = "ALREADY_GONE"
)

type SourceTupleV1 struct {
	SourceKind SourceKind
	Contract   string
	Version    uint64
}

type OneShotLookupStatus string

const (
	OneShotLookupNone     OneShotLookupStatus = "NONE"
	OneShotLookupExisting OneShotLookupStatus = "EXISTING"
	OneShotLookupConflict OneShotLookupStatus = "CONFLICT"
)

type OneShotLookupResult struct {
	Status OneShotLookupStatus
	Bundle []byte
	Replay []byte
}

type EBusFamily string

const (
	EBusFamilyB509 EBusFamily = "B509"
	EBusFamilyB524 EBusFamily = "B524"
	EBusFamilyB555 EBusFamily = "B555"
)

type ClockObservation struct {
	Wall          time.Time
	OffsetNS      uint64
	UncertaintyNS uint64
}

type Clock interface {
	Observe() ClockObservation
}

type EvidenceRefV1 struct {
	Kind            EvidenceKind    `json:"kind"`
	DigestAlgorithm DigestAlgorithm `json:"digest_algorithm"`
	Digest          string          `json:"digest"`
	Repository      *string         `json:"repository"`
	Commit          *string         `json:"commit"`
	Path            *string         `json:"path"`
}

type AuthScopeV1 struct {
	Authority   string   `json:"authority"`
	Permissions []string `json:"permissions"`
}

type CaptureLimitsV1 struct {
	MaxSources           uint64 `json:"max_sources"`
	MaxItemsPerSource    uint64 `json:"max_items_per_source"`
	MaxArtifactBytes     uint64 `json:"max_artifact_bytes"`
	MaxBundleBytes       uint64 `json:"max_bundle_bytes"`
	MaxDepth             uint64 `json:"max_depth"`
	MaxStringBytes       uint64 `json:"max_string_bytes"`
	MaxCaptureDurationNS uint64 `json:"max_capture_duration_ns"`
	MaxSourceDurationNS  uint64 `json:"max_source_duration_ns"`
}

func DefaultLimitsV1() CaptureLimitsV1 {
	return CaptureLimitsV1{
		MaxSources:           64,
		MaxItemsPerSource:    4096,
		MaxArtifactBytes:     1_048_576,
		MaxBundleBytes:       67_108_864,
		MaxDepth:             32,
		MaxStringBytes:       65_536,
		MaxCaptureDurationNS: 900_000_000_000,
		MaxSourceDurationNS:  60_000_000_000,
	}
}

type WindowSegmentV1 struct {
	StartOffsetNS uint64 `json:"start_offset_ns"`
	EndOffsetNS   uint64 `json:"end_offset_ns"`
}

type ActionWindowV1 struct {
	StartOffsetNS    uint64        `json:"start_offset_ns"`
	MarkerOffsetNS   uint64        `json:"marker_offset_ns"`
	MarkerCapturedAt time.Time     `json:"marker_captured_at"`
	MarkerID         string        `json:"marker_id"`
	EvidenceRef      EvidenceRefV1 `json:"evidence_ref"`
	EndOffsetNS      uint64        `json:"end_offset_ns"`
}

type CaptureWindowV1 struct {
	Pre    WindowSegmentV1 `json:"pre"`
	Action ActionWindowV1  `json:"action"`
	Post   WindowSegmentV1 `json:"post"`
}

type ClockObservationV1 struct {
	ObservedAt    time.Time `json:"observed_at"`
	OffsetNS      uint64    `json:"offset_ns"`
	UncertaintyNS uint64    `json:"uncertainty_ns"`
}

type CaptureClockV1 struct {
	ClockID           string               `json:"clock_id"`
	WallAnchor        time.Time            `json:"wall_anchor"`
	MonotonicAnchorNS uint64               `json:"monotonic_anchor_ns"`
	CapturedOffsetNS  uint64               `json:"captured_offset_ns"`
	ResolutionNS      uint64               `json:"resolution_ns"`
	MaximumSkewNS     uint64               `json:"maximum_skew_ns"`
	Observations      []ClockObservationV1 `json:"observations"`
}

type CaptureScopeV1 struct {
	Purpose     string        `json:"purpose"`
	SourceKinds []RuntimeKind `json:"source_kinds"`
	Phases      []Phase       `json:"phases"`
}

type RequestScopeV1 struct {
	Phase          Phase       `json:"phase"`
	SourceKind     RuntimeKind `json:"source_kind"`
	OperationScope string      `json:"operation_scope"`
}

type SnapshotScopeV1 struct {
	Mode     SnapshotMode `json:"mode"`
	Selector string       `json:"selector"`
}

type EBusSourceIdentityV1 struct {
	Family               EBusFamily `json:"family"`
	TargetPseudonym      string     `json:"target_pseudonym"`
	TargetAddress        uint8      `json:"target_address"`
	TargetProduct        string     `json:"target_product"`
	RegisterFamily       string     `json:"register_family"`
	RegisterID           uint16     `json:"register_id"`
	UnitScaleSource      string     `json:"unit_scale_source"`
	EvidenceRole         string     `json:"evidence_role"`
	Opcode               uint8      `json:"opcode"`
	GG                   uint8      `json:"GG"`
	II                   uint8      `json:"II"`
	RR                   uint16     `json:"RR"`
	SourceAddress        uint8      `json:"source_address"`
	GroupMeaning         string     `json:"group_meaning"`
	InstanceGate         string     `json:"instance_gate"`
	RegisterCategory     string     `json:"register_category"`
	DeviceFamily         string     `json:"device_family"`
	ScheduleProgram      string     `json:"schedule_program"`
	SlotIndex            uint8      `json:"slot_index"`
	DayOfWeek            string     `json:"day_of_week"`
	TimeIdentity         string     `json:"time_identity"`
	OperationModeContext string     `json:"operation_mode_context"`
}

type SourceBindingV1 struct {
	RuntimeKind         RuntimeKind           `json:"runtime_kind"`
	RuntimePseudonym    string                `json:"runtime_pseudonym"`
	OperationID         string                `json:"operation_id"`
	OperationVersion    string                `json:"operation_version"`
	RequestScope        RequestScopeV1        `json:"request_scope"`
	SnapshotScope       SnapshotScopeV1       `json:"snapshot_scope"`
	SourceKind          SourceKind            `json:"source_kind"`
	SourceContract      string                `json:"source_contract"`
	SourceSchemaVersion uint64                `json:"source_schema_version"`
	OwnerRepository     string                `json:"owner_repository"`
	OwnerPath           string                `json:"owner_path"`
	OwnerCommit         string                `json:"owner_commit"`
	SchemaSHA256        string                `json:"schema_sha256"`
	CaptureWindow       CaptureWindowV1       `json:"capture_window"`
	MaskTier            string                `json:"mask_tier"`
	AuthScope           AuthScopeV1           `json:"auth_scope"`
	EBusIdentity        *EBusSourceIdentityV1 `json:"ebus_identity"`
}

type RemaskedPseudonymV1 struct {
	Path      string `json:"path"`
	Pseudonym string `json:"pseudonym"`
}

type RemaskingV1 struct {
	Method  string                `json:"method"`
	ScopeID string                `json:"scope_id"`
	Entries []RemaskedPseudonymV1 `json:"entries"`
}

type SourceRecordV1 struct {
	Contract                 string                `json:"contract"`
	SchemaVersion            uint64                `json:"schema_version"`
	SourceID                 string                `json:"source_id"`
	SourceKind               RuntimeKind           `json:"source_kind"`
	Phase                    Phase                 `json:"phase"`
	State                    SourceState           `json:"state"`
	ErrorCategory            *ErrorCategory        `json:"error_category"`
	SourceContract           string                `json:"source_contract"`
	SourceSchemaVersion      uint64                `json:"source_schema_version"`
	SourceBinding            SourceBindingV1       `json:"source_binding"`
	CaptureWindow            CaptureWindowV1       `json:"capture_window"`
	Clock                    CaptureClockV1        `json:"clock"`
	Scope                    CaptureScopeV1        `json:"scope"`
	MaskTier                 string                `json:"mask_tier"`
	AuthScope                AuthScopeV1           `json:"auth_scope"`
	EvidenceRefs             []EvidenceRefV1       `json:"evidence_refs"`
	RecorderVersion          string                `json:"recorder_version"`
	ReplayVersion            string                `json:"replay_version"`
	AcquisitionStartedAt     *time.Time            `json:"acquisition_started_at"`
	AcquisitionEndedAt       *time.Time            `json:"acquisition_ended_at"`
	AcquisitionStartOffsetNS *uint64               `json:"acquisition_start_offset_ns"`
	AcquisitionEndOffsetNS   *uint64               `json:"acquisition_end_offset_ns"`
	MeasuredLatencyNS        *uint64               `json:"measured_latency_ns"`
	MaximumSkewNS            uint64                `json:"maximum_skew_ns"`
	EBusIdentity             *EBusSourceIdentityV1 `json:"ebus_identity"`
	ArtifactIDs              []string              `json:"artifact_ids"`
}

type SourceArtifactV1 struct {
	Contract                 string                `json:"contract"`
	SchemaVersion            uint64                `json:"schema_version"`
	ArtifactID               string                `json:"artifact_id"`
	SourceID                 string                `json:"source_id"`
	SourceKind               RuntimeKind           `json:"source_kind"`
	Phase                    Phase                 `json:"phase"`
	SourceContract           string                `json:"source_contract"`
	SourceSchemaVersion      uint64                `json:"source_schema_version"`
	SourceBinding            SourceBindingV1       `json:"source_binding"`
	EBusIdentity             *EBusSourceIdentityV1 `json:"ebus_identity"`
	SourceObservedAt         time.Time             `json:"source_observed_at"`
	RecorderIngestedAt       time.Time             `json:"recorder_ingested_at"`
	RecorderIngestedOffsetNS uint64                `json:"recorder_ingested_offset_ns"`
	CaptureWindow            CaptureWindowV1       `json:"capture_window"`
	Clock                    CaptureClockV1        `json:"clock"`
	Scope                    CaptureScopeV1        `json:"scope"`
	MaskTier                 string                `json:"mask_tier"`
	AuthScope                AuthScopeV1           `json:"auth_scope"`
	EvidenceRefs             []EvidenceRefV1       `json:"evidence_refs"`
	RecorderVersion          string                `json:"recorder_version"`
	ReplayVersion            string                `json:"replay_version"`
	Remasking                RemaskingV1           `json:"remasking"`
	ItemCount                uint64                `json:"item_count"`
	ByteCount                uint64                `json:"byte_count"`
	NormalizedEvidence       json.RawMessage       `json:"normalized_evidence"`
	RedactedHash             string                `json:"redacted_hash"`
}

type SynchronizedEvidenceBundleV1 struct {
	Contract        string             `json:"contract"`
	SchemaVersion   uint64             `json:"schema_version"`
	BundleID        string             `json:"bundle_id"`
	CapturedAt      time.Time          `json:"captured_at"`
	CaptureWindow   CaptureWindowV1    `json:"capture_window"`
	Clock           CaptureClockV1     `json:"clock"`
	Scope           CaptureScopeV1     `json:"scope"`
	MaskTier        string             `json:"mask_tier"`
	AuthScope       AuthScopeV1        `json:"auth_scope"`
	Limits          CaptureLimitsV1    `json:"limits"`
	EvidenceRefs    []EvidenceRefV1    `json:"evidence_refs"`
	Sources         []SourceRecordV1   `json:"sources"`
	Artifacts       []SourceArtifactV1 `json:"artifacts"`
	RecorderVersion string             `json:"recorder_version"`
	ReplayVersion   string             `json:"replay_version"`
	BundleHash      string             `json:"bundle_hash"`
}

type RawNormalizedEvidenceRowV1 struct {
	ArtifactID         string          `json:"artifact_id"`
	SourceBinding      SourceBindingV1 `json:"source_binding"`
	SourceObservedAt   time.Time       `json:"source_observed_at"`
	NormalizedEvidence json.RawMessage `json:"normalized_evidence"`
}

type CapturedTimestampRowV1 struct {
	EventKind          string     `json:"event_kind"`
	SourceID           *string    `json:"source_id"`
	ArtifactID         *string    `json:"artifact_id"`
	SourceObservedAt   *time.Time `json:"source_observed_at"`
	RecorderObservedAt time.Time  `json:"recorder_observed_at"`
	RecorderOffsetNS   uint64     `json:"recorder_offset_ns"`
}

type TerminalStateRowV1 struct {
	SourceID      string         `json:"source_id"`
	Phase         Phase          `json:"phase"`
	State         SourceState    `json:"state"`
	ErrorCategory *ErrorCategory `json:"error_category"`
}

type RedactedHashRowV1 struct {
	Kind       string  `json:"kind"`
	ArtifactID *string `json:"artifact_id"`
	Digest     string  `json:"digest"`
}

type FutureCandidateInputRowV1 struct {
	ArtifactID    string          `json:"artifact_id"`
	SourceID      string          `json:"source_id"`
	SourceBinding SourceBindingV1 `json:"source_binding"`
	EvidenceRefs  []EvidenceRefV1 `json:"evidence_refs"`
	RedactedHash  string          `json:"redacted_hash"`
}

type ReplayResultV1 struct {
	Contract              string                       `json:"contract"`
	SchemaVersion         uint64                       `json:"schema_version"`
	BundleID              string                       `json:"bundle_id"`
	RawNormalizedEvidence []RawNormalizedEvidenceRowV1 `json:"raw_normalized_evidence"`
	CapturedTimestamps    []CapturedTimestampRowV1     `json:"captured_timestamps"`
	TerminalStates        []TerminalStateRowV1         `json:"terminal_states"`
	RedactedHashes        []RedactedHashRowV1          `json:"redacted_hashes"`
	FutureCandidateInputs []FutureCandidateInputRowV1  `json:"future_candidate_inputs"`
}

type ActionMarker struct {
	ID          string
	EvidenceRef EvidenceRefV1
}

type SourceRequest struct {
	Phase          Phase
	Limits         CaptureLimitsV1
	OperationID    string
	OperationScope string
	MaskTier       string
	AuthScope      AuthScopeV1
}

type SourceAdmission struct {
	Selection            SelectionDecision
	Policy               PolicyDecision
	Backend              BackendDecision
	EffectivePermissions []string
	RequiredPermissions  []string
}

type AcquiredEvidence struct {
	SourceObservedAt   time.Time
	NormalizedEvidence json.RawMessage
}

// EBusSnapshotReader exposes only the existing read-only snapshot operation.
// It cannot choose policy, authorization, identity, or terminal-state fields.
type EBusSnapshotReader interface {
	ReadSnapshot(context.Context, SourceRequest) (AcquiredEvidence, error)
}

// EEBusServicesReader exposes only the frozen read-only services-list operation.
type EEBusServicesReader interface {
	ListServices(context.Context, SourceRequest) (AcquiredEvidence, error)
}

// EEBusM625Reader exposes only the bounded public-redacted feature-data read.
type EEBusM625Reader interface {
	ReadFeatureData(context.Context, SourceRequest) (AcquiredEvidence, error)
}

// PrecapturedCloudInput is a value-only local seam. It has no callback, client,
// endpoint, credential, retry, or refresh capability.
type PrecapturedCloudInput struct {
	SourceObservedAt   time.Time
	NormalizedEvidence json.RawMessage
	EvidenceRef        EvidenceRefV1
}

type RegisteredSource struct {
	Phase            Phase
	SourceKind       SourceKind
	SourceContract   string
	SourceVersion    uint64
	RuntimeInstance  string
	SourceID         string // Deprecated compatibility alias for RuntimeInstance; never serialized.
	OperationVersion string
	OperationScope   string
	Admission        SourceAdmission
	EvidenceRefs     []EvidenceRefV1
	EBusIdentity     *EBusSourceIdentityV1
	EBusReader       EBusSnapshotReader
	EEBusReader      EEBusServicesReader
	EEBusM625Reader  EEBusM625Reader
	PrecapturedCloud *PrecapturedCloudInput
	cloudBound       bool
}

type StoppableTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type RecorderOptions struct {
	Clock           Clock
	NewTimer        func(time.Duration) StoppableTimer
	Entropy         io.Reader
	Limits          CaptureLimitsV1
	Sources         []RegisteredSource
	RecorderVersion string
	ReplayVersion   string
}
