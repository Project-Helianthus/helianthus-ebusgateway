// Package promotioncapture implements the deterministic, internal-only core of
// the captured multi-leaf promotion contract.
package promotioncapture

import (
	"encoding/json"
	"errors"
)

const (
	RegistrySHA256       = "sha256:00ceefc05439e9aec5830b640661cdc6be2b503f9365eed437e3dbffdf6d0678"
	DocsContractCommit   = "c4cea33a3f6262e31801cad35d663e08317de4dd"
	DocsEEBusCommit      = "ed5354421ddf0a2005f496e3fd65675990032b5e"
	CheckpointContractV1 = "helianthus.platform.leaf-promotion-window-checkpoint.v1"

	RetirementTerminalNotALeaf = "RETIRED_TERMINAL_NOT_A_LEAF"
)

var (
	ErrInvalidDecimal   = errors.New("promotioncapture: invalid decimal")
	ErrInvalidStep      = errors.New("promotioncapture: declared step must be finite and positive")
	ErrOutOfRange       = errors.New("promotioncapture: numeric value outside declared range")
	ErrUnknownCandidate = errors.New("promotioncapture: unknown candidate")
	ErrInvalidEvidence  = errors.New("promotioncapture: invalid assessment evidence")
)

type ComparatorClass string

const (
	ComparatorNone    ComparatorClass = "NONE"
	ComparatorNumeric ComparatorClass = "NUMERIC_DECLARED_GRANULARITY"
	ComparatorEnum    ComparatorClass = "ENUM_EXACT_MAPPING"
	ComparatorBoolean ComparatorClass = "BOOLEAN_EXACT_MAPPING"
	ComparatorString  ComparatorClass = "STRING_EXACT_STABILITY"
)

type ProtocolEligibility string

const (
	ProtocolTerminal      ProtocolEligibility = "TERMINAL"
	ProtocolCrossProtocol ProtocolEligibility = "CROSS_PROTOCOL_EQUIVALENCE"
	ProtocolEEBusNative   ProtocolEligibility = "EEBUS_NATIVE"
)

type ValidationMode string

const (
	ValidationCrossProtocol         ValidationMode = "CROSS_PROTOCOL_EQUIVALENCE"
	ValidationEEBusNativeCapability ValidationMode = "EEBUS_NATIVE_CAPABILITY"
	ValidationEEBusNativeMetadata   ValidationMode = "EEBUS_NATIVE_METADATA"
)

type Outcome string

const (
	OutcomeMatch             Outcome = "MATCH"
	OutcomeMismatch          Outcome = "MISMATCH"
	OutcomeMissing           Outcome = "MISSING"
	OutcomeNotComparable     Outcome = "NOT_COMPARABLE"
	OutcomeIdentityMismatch  Outcome = "IDENTITY_MISMATCH"
	OutcomeGenerationChanged Outcome = "GENERATION_CHANGED"
	OutcomeInvalid           Outcome = "INVALID"
	OutcomeStale             Outcome = "STALE"
	OutcomeConflict          Outcome = "CONFLICT"
	OutcomeCloudOnly         Outcome = "CLOUD_ONLY"
	OutcomeNotTested         Outcome = "NOT_TESTED"
	OutcomeNotEvaluated      Outcome = "NOT_EVALUATED"
	OutcomeNativeValid       Outcome = "NATIVE_VALID"
	OutcomeNativeDrift       Outcome = "NATIVE_DRIFT"
)

type Source string

const (
	SourceEBus  Source = "EBUS"
	SourceEEBus Source = "EEBUS"
)

type WindowPhase string

const (
	PhasePreRestart  WindowPhase = "PRE_RESTART"
	PhasePostRestart WindowPhase = "POST_RESTART"
)

type ValueKind string

const (
	ValueNumeric ValueKind = "NUMERIC"
	ValueEnum    ValueKind = "ENUM"
	ValueBoolean ValueKind = "BOOLEAN"
	ValueString  ValueKind = "STRING"
)

type ConversionMode string

const (
	ConversionIdentity ConversionMode = "IDENTITY"
	ConversionAffine   ConversionMode = "AFFINE"
)

// Decimal is the contract's exact number * 10^scale representation.
type Decimal struct {
	Number int64 `json:"number"`
	Scale  int   `json:"scale"`
}

type TypedValue struct {
	Kind    ValueKind `json:"kind"`
	Decimal *Decimal  `json:"decimal"`
	Enum    *string   `json:"enum"`
	Boolean *bool     `json:"boolean"`
	String  *string   `json:"string"`
}

func NumericValue(value Decimal) TypedValue {
	return TypedValue{Kind: ValueNumeric, Decimal: &value}
}

func EnumValue(value string) TypedValue {
	return TypedValue{Kind: ValueEnum, Enum: &value}
}

func BooleanValue(value bool) TypedValue {
	return TypedValue{Kind: ValueBoolean, Boolean: &value}
}

func StringValue(value string) TypedValue {
	return TypedValue{Kind: ValueString, String: &value}
}

type DeclaredConstraints struct {
	Minimum Decimal `json:"minimum"`
	Maximum Decimal `json:"maximum"`
	Step    Decimal `json:"step"`
}

type Conversion struct {
	Mode       ConversionMode `json:"mode"`
	SourceUnit string         `json:"source_unit"`
	TargetUnit string         `json:"target_unit"`
	Scale      Decimal        `json:"scale"`
	Offset     Decimal        `json:"offset"`
}

type EBusSelector struct {
	Family           string `json:"family"`
	TargetAddress    int    `json:"target_address"`
	Opcode           int    `json:"opcode"`
	GG               int    `json:"GG"`
	II               int    `json:"II"`
	RR               int    `json:"RR"`
	GroupMeaning     string `json:"group_meaning"`
	InstanceGate     string `json:"instance_gate"`
	RegisterCategory string `json:"register_category"`
	UnitScaleSource  string `json:"unit_scale_source"`
}

type B555Selector struct {
	Family               string `json:"family"`
	Operation            string `json:"operation"`
	TargetPseudonymRule  string `json:"target_pseudonym_rule"`
	DeviceFamily         string `json:"device_family"`
	ScheduleProgram      string `json:"schedule_program"`
	SlotIndex            int    `json:"slot_index"`
	DayOfWeek            string `json:"day_of_week"`
	TimeIdentity         string `json:"time_identity"`
	OperationModeContext string `json:"operation_mode_context"`
	UnitScaleSource      string `json:"unit_scale_source"`
	FieldPath            string `json:"field_path"`
	Unit                 string `json:"unit"`
	CouplingRule         string `json:"coupling_rule"`
}

type ProtocolMapping struct {
	Pairs []ProtocolMappingPair `json:"pairs"`
}

type ProtocolMappingPair struct {
	Raw        json.RawMessage `json:"raw"`
	Normalized json.RawMessage `json:"normalized"`
}

type MappingProfile struct {
	Pairs []MappingPair `json:"pairs"`
}

type MappingPair struct {
	EBusRaw    Decimal         `json:"ebus_raw"`
	EEBusRaw   json.RawMessage `json:"eebus_raw"`
	Normalized json.RawMessage `json:"normalized"`
}

type EEBusSource struct {
	EntitySlot           string               `json:"entity_slot"`
	EntityType           string               `json:"entity_type"`
	FeatureType          string               `json:"feature_type"`
	FeatureRole          string               `json:"feature_role"`
	DescriptionFunctions []string             `json:"description_functions"`
	ConstraintsFunction  *string              `json:"constraints_function"`
	ValueFunctions       []string             `json:"value_functions"`
	FieldPath            string               `json:"field_path"`
	Descriptor           json.RawMessage      `json:"descriptor"`
	Unit                 *string              `json:"unit"`
	DeclaredConstraints  *DeclaredConstraints `json:"declared_constraints"`
	ExactMapping         *ProtocolMapping     `json:"exact_mapping"`
}

type CandidateDefinition struct {
	CandidateID         string              `json:"candidate_id"`
	FactHash            string              `json:"fact_hash"`
	SourceStatus        string              `json:"source_status"`
	TerminalState       *Outcome            `json:"terminal_state"`
	RetirementState     *string             `json:"retirement_state"`
	ValidationMode      *ValidationMode     `json:"validation_mode"`
	ComparatorClass     ComparatorClass     `json:"comparator_class"`
	SemanticPath        *string             `json:"semantic_path"`
	ProtocolEligibility ProtocolEligibility `json:"protocol_eligibility"`
	EBusSelector        *EBusSelector       `json:"ebus_selector"`
	EBusFallback        *B555Selector       `json:"ebus_fallback"`
	EEBusSource         *EEBusSource        `json:"eebus_source"`
	Conversion          *Conversion         `json:"conversion"`
	MappingProfile      *MappingProfile     `json:"mapping_profile"`
}

func (candidate CandidateDefinition) FixedOutcome() (Outcome, bool) {
	if candidate.ProtocolEligibility == ProtocolTerminal && candidate.TerminalState != nil {
		return *candidate.TerminalState, true
	}
	return "", false
}

// Window and Sample intentionally mirror the PRIVATE_OPERATOR contract.
type Window struct {
	WindowID             string      `json:"window_id"`
	Phase                WindowPhase `json:"phase"`
	StartedAt            string      `json:"started_at"`
	EndedAt              string      `json:"ended_at"`
	CaptureGeneration    string      `json:"capture_generation"`
	ProcessInstanceHash  string      `json:"process_instance_hash"`
	LocalIdentityHash    string      `json:"local_identity_hash"`
	TrustStateHash       string      `json:"trust_state_hash"`
	PeerBindingHash      string      `json:"peer_binding_hash"`
	AdmittedSource       int         `json:"admitted_source"`
	EEBusRuntimeEpoch    int64       `json:"eebus_runtime_epoch"`
	ConnectionGeneration int64       `json:"connection_generation"`
	EBusPollGeneration   string      `json:"ebus_poll_generation"`
	M8NoDrift            bool        `json:"m8_no_drift"`
	RollbackExact        bool        `json:"rollback_exact"`
}

type Sample struct {
	Source               Source     `json:"source"`
	ObservedAt           string     `json:"observed_at"`
	Valid                bool       `json:"valid"`
	CaptureGeneration    string     `json:"capture_generation"`
	PollID               *string    `json:"poll_id"`
	PollGeneration       *string    `json:"poll_generation"`
	RuntimeEpoch         *int64     `json:"runtime_epoch"`
	ConnectionGeneration *int64     `json:"connection_generation"`
	RawHash              string     `json:"raw_hash"`
	RawValue             TypedValue `json:"raw_value"`
	Value                TypedValue `json:"value"`
	Unit                 *string    `json:"unit"`
}

type Comparator struct {
	Class             ComparatorClass `json:"class"`
	DeclaredSpineStep *Decimal        `json:"declared_spine_step"`
	Delta             *Decimal        `json:"delta"`
	Conversion        *Conversion     `json:"conversion"`
	MappingHash       *string         `json:"mapping_hash"`
	Outcome           Outcome         `json:"outcome"`
}

type Assessment struct {
	WindowID                  string     `json:"window_id"`
	EBusSample                *Sample    `json:"ebus_sample"`
	EEBusSample               *Sample    `json:"eebus_sample"`
	ObservedEBusIdentityHash  *string    `json:"observed_ebus_identity_hash"`
	ObservedEEBusIdentityHash *string    `json:"observed_eebus_identity_hash"`
	ConflictSamples           []Sample   `json:"conflict_samples"`
	SkewNS                    *int64     `json:"skew_ns"`
	MaxSkewNS                 int64      `json:"max_skew_ns"`
	AgeNS                     *int64     `json:"age_ns"`
	MaxAgeNS                  int64      `json:"max_age_ns"`
	Comparator                Comparator `json:"comparator"`
}

type WindowAssessmentInput struct {
	Window                    Window
	ExpectedEBusIdentityHash  string
	ExpectedEEBusIdentityHash string
	ObservedEBusIdentityHash  *string
	ObservedEEBusIdentityHash *string
	EBusSample                *Sample
	EEBusSample               *Sample
	ConflictSamples           []Sample
	PreviousNativeValue       *TypedValue
}

type WindowEvaluation struct {
	CandidateID string      `json:"candidate_id"`
	Outcome     Outcome     `json:"outcome"`
	Fixed       bool        `json:"fixed"`
	Assessment  *Assessment `json:"assessment"`
}

type NumericComparison struct {
	ConvertedEBus Decimal
	EEBus         Decimal
	Delta         Decimal
	Match         bool
}

type CaptureLimits struct {
	MaxSkewNS int64 `json:"max_skew_ns"`
	MaxAgeNS  int64 `json:"max_age_ns"`
}

// EBusIdentity is the exact private/operator B524 or B555 identity. Custom
// JSON encoding keeps the two protocol-family shapes disjoint.
type EBusIdentity struct {
	Family               string `json:"-"`
	TargetPseudonym      string `json:"-"`
	TargetAddress        int    `json:"-"`
	SourceAddress        int    `json:"-"`
	SelectorHash         string `json:"-"`
	Opcode               int    `json:"-"`
	GG                   int    `json:"-"`
	II                   int    `json:"-"`
	RR                   int    `json:"-"`
	GroupMeaning         string `json:"-"`
	InstanceGate         string `json:"-"`
	RegisterCategory     string `json:"-"`
	Operation            string `json:"-"`
	TargetPseudonymRule  string `json:"-"`
	DeviceFamily         string `json:"-"`
	ScheduleProgram      string `json:"-"`
	SlotIndex            int    `json:"-"`
	DayOfWeek            string `json:"-"`
	TimeIdentity         string `json:"-"`
	OperationModeContext string `json:"-"`
	UnitScaleSource      string `json:"-"`
	FieldPath            string `json:"-"`
	Unit                 string `json:"-"`
	CouplingRule         string `json:"-"`
}

type B524Identity = EBusIdentity

// EEBusIdentity binds the complete catalog source profile to the observed
// native SHIP/SPINE selectors. Operational protocol identity is private
// evidence, not cryptographic secret material.
type EEBusIdentity struct {
	ServiceID            string               `json:"service_id"`
	DeviceAddress        string               `json:"device_address"`
	EntityAddress        []uint64             `json:"entity_address"`
	FeatureAddress       uint64               `json:"feature_address"`
	EntitySlot           string               `json:"entity_slot"`
	EntityType           string               `json:"entity_type"`
	FeatureType          string               `json:"feature_type"`
	FeatureRole          string               `json:"feature_role"`
	DescriptionFunctions []string             `json:"description_functions"`
	ConstraintsFunction  *string              `json:"constraints_function"`
	ValueFunctions       []string             `json:"value_functions"`
	FieldPath            string               `json:"field_path"`
	Descriptor           json.RawMessage      `json:"descriptor"`
	Unit                 *string              `json:"unit"`
	DeclaredConstraints  *DeclaredConstraints `json:"declared_constraints"`
	ExactMapping         *ProtocolMapping     `json:"exact_mapping"`
	SourceProfileHash    string               `json:"source_profile_hash"`
	IdentityHash         string               `json:"identity_hash"`
}

type CapturedCandidateWindow struct {
	CandidateID         string              `json:"candidate_id"`
	FactHash            string              `json:"fact_hash"`
	SourceStatus        string              `json:"source_status"`
	RetirementState     *string             `json:"retirement_state"`
	SemanticPath        *string             `json:"semantic_path"`
	ValidationMode      *ValidationMode     `json:"validation_mode"`
	ComparatorClass     ComparatorClass     `json:"comparator_class"`
	ProtocolEligibility ProtocolEligibility `json:"protocol_eligibility"`
	EBusIdentity        *EBusIdentity       `json:"ebus_identity"`
	EEBusIdentity       *EEBusIdentity      `json:"eebus_identity"`
	Evaluation          WindowEvaluation    `json:"evaluation"`
}

type WindowCheckpoint struct {
	Contract          string                    `json:"contract"`
	SchemaVersion     int                       `json:"schema_version"`
	CampaignID        string                    `json:"capture_campaign_id"`
	ProcessInstanceID string                    `json:"process_instance_id"`
	TrustStateID      string                    `json:"trust_state_id"`
	PeerBindingID     string                    `json:"peer_binding_id"`
	Window            Window                    `json:"window"`
	Candidates        []CapturedCandidateWindow `json:"candidates"`
	CapturedAt        string                    `json:"captured_at"`
	CheckpointHash    string                    `json:"checkpoint_hash"`
}

type EvidenceMode string

const (
	EvidenceModeLiveCapture EvidenceMode = "LIVE_CAPTURE"
)

type Decision string

const (
	DecisionPromoted Decision = "PROMOTED"
	DecisionWithheld Decision = "WITHHELD"
)

type Visibility string

const (
	VisibilityLockedNotExposed Visibility = "LOCKED_NOT_EXPOSED"
	VisibilityRawDebugOnly     Visibility = "RAW_DEBUG_ONLY"
)

type CampaignProvenance struct {
	Class                  EvidenceMode `json:"class"`
	FixtureID              *string      `json:"fixture_id"`
	Generator              *string      `json:"generator"`
	CaptureCampaignID      *string      `json:"capture_campaign_id"`
	CaptureReceipts        []string     `json:"capture_receipts"`
	DeploymentSourceCommit *string      `json:"deployment_source_commit"`
	DeploymentSourceHash   *string      `json:"deployment_source_hash"`
	DeploymentBinaryHash   *string      `json:"deployment_binary_hash"`
}

type CampaignSourceBindings struct {
	RegistrySHA256      string `json:"registry_sha256"`
	DocsEEBusCommit     string `json:"docs_eebus_commit"`
	M7GraphID           string `json:"m7_graph_id"`
	M7GraphHash         string `json:"m7_graph_hash"`
	M7GraphBytesHash    string `json:"m7_graph_bytes_hash"`
	M7ReplayID          string `json:"m7_replay_id"`
	M7ReplayHash        string `json:"m7_replay_hash"`
	M7ReplayBytesHash   string `json:"m7_replay_bytes_hash"`
	M7StatusID          string `json:"m7_status_id"`
	M7StatusHash        string `json:"m7_status_hash"`
	M7StatusBytesHash   string `json:"m7_status_bytes_hash"`
	M8EvidenceID        string `json:"m8_evidence_id"`
	M8EvidenceHash      string `json:"m8_evidence_hash"`
	M8EvidenceBytesHash string `json:"m8_evidence_bytes_hash"`
	M8ReportID          string `json:"m8_report_id"`
	M8ReportHash        string `json:"m8_report_hash"`
	M8ReportBytesHash   string `json:"m8_report_bytes_hash"`
	ReplayHash          string `json:"replay_hash"`
}

type CampaignAssemblyManifest struct {
	EvidenceMode   EvidenceMode           `json:"evidence_mode"`
	Provenance     CampaignProvenance     `json:"provenance"`
	SourceBindings CampaignSourceBindings `json:"source_bindings"`
}

type CampaignCandidate struct {
	CandidateID     string          `json:"candidate_id"`
	FactHash        string          `json:"fact_hash"`
	SourceStatus    string          `json:"source_status"`
	RetirementState *string         `json:"retirement_state"`
	SemanticPath    *string         `json:"semantic_path"`
	ValidationMode  *ValidationMode `json:"validation_mode"`
	ComparatorClass ComparatorClass `json:"comparator_class"`
	EBusIdentity    *EBusIdentity   `json:"ebus_identity"`
	EEBusIdentity   *EEBusIdentity  `json:"eebus_identity"`
	Assessments     []Assessment    `json:"assessments"`
	Decision        Decision        `json:"decision"`
	TerminalState   *Outcome        `json:"terminal_state"`
	Visibility      Visibility      `json:"visibility"`
	DossierHash     *string         `json:"dossier_hash"`
}

type Campaign struct {
	Contract       string                 `json:"contract"`
	SchemaVersion  int                    `json:"schema_version"`
	Profile        string                 `json:"profile"`
	EvidenceMode   EvidenceMode           `json:"evidence_mode"`
	ExportTier     string                 `json:"export_tier"`
	Provenance     CampaignProvenance     `json:"provenance"`
	SourceBindings CampaignSourceBindings `json:"source_bindings"`
	Windows        []Window               `json:"windows"`
	Candidates     []CampaignCandidate    `json:"candidates"`
	CampaignHash   string                 `json:"campaign_hash"`
}
