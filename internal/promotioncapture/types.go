// Package promotioncapture implements the deterministic, internal-only core of
// the captured multi-leaf promotion contract.
package promotioncapture

import (
	"encoding/json"
	"errors"
)

const (
	RegistrySHA256     = "sha256:d17a66da1919796f57ecd2a515fa4e538c6be8d00a24c8c7e5d38bce7f36e3cd"
	DocsContractCommit = "47a1cd088fa60c071162e4d2aec742de103fb9f9"
	DocsEEBusCommit    = "657a36d07e52570326384b757a5382a6789f641b"
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
)

type ProtocolEligibility string

const (
	ProtocolTerminal                 ProtocolEligibility = "TERMINAL"
	ProtocolEligible                 ProtocolEligibility = "ELIGIBLE"
	ProtocolWithholdNoEBusCapability ProtocolEligibility = "WITHHOLD_NO_EBUS_CAPABILITY_SOURCE"
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
	Conversion           *Conversion          `json:"conversion"`
	ExactMapping         *ProtocolMapping     `json:"exact_mapping"`
	MappingProfile       *MappingProfile      `json:"mapping_profile"`
}

type CandidateDefinition struct {
	CandidateID         string              `json:"candidate_id"`
	FactHash            string              `json:"fact_hash"`
	SourceStatus        string              `json:"source_status"`
	TerminalState       *Outcome            `json:"terminal_state"`
	ComparatorClass     ComparatorClass     `json:"comparator_class"`
	SemanticPath        *string             `json:"semantic_path"`
	ProtocolEligibility ProtocolEligibility `json:"protocol_eligibility"`
	EBusSelector        *EBusSelector       `json:"ebus_selector"`
	EEBusSource         *EEBusSource        `json:"eebus_source"`
}

func (candidate CandidateDefinition) FixedOutcome() (Outcome, bool) {
	if candidate.ProtocolEligibility == ProtocolTerminal && candidate.TerminalState != nil {
		return *candidate.TerminalState, true
	}
	if candidate.ProtocolEligibility == ProtocolWithholdNoEBusCapability {
		return OutcomeNotComparable, true
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

// B524Identity is the exact private/operator eBUS selector bound to one
// candidate. It intentionally stays inside the gateway implementation.
type B524Identity struct {
	Family           string `json:"family"`
	TargetPseudonym  string `json:"target_pseudonym"`
	TargetAddress    int    `json:"target_address"`
	SourceAddress    int    `json:"source_address"`
	Opcode           int    `json:"opcode"`
	GG               int    `json:"GG"`
	II               int    `json:"II"`
	RR               int    `json:"RR"`
	GroupMeaning     string `json:"group_meaning"`
	InstanceGate     string `json:"instance_gate"`
	RegisterCategory string `json:"register_category"`
	UnitScaleSource  string `json:"unit_scale_source"`
	SelectorHash     string `json:"selector_hash"`
}

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
	Conversion           *Conversion          `json:"conversion"`
	ExactMapping         *ProtocolMapping     `json:"exact_mapping"`
	MappingProfile       *MappingProfile      `json:"mapping_profile"`
	SourceProfileHash    string               `json:"source_profile_hash"`
	IdentityHash         string               `json:"identity_hash"`
}

type CapturedCandidateWindow struct {
	CandidateID         string              `json:"candidate_id"`
	FactHash            string              `json:"fact_hash"`
	SourceStatus        string              `json:"source_status"`
	SemanticPath        *string             `json:"semantic_path"`
	ComparatorClass     ComparatorClass     `json:"comparator_class"`
	ProtocolEligibility ProtocolEligibility `json:"protocol_eligibility"`
	EBusIdentity        *B524Identity       `json:"ebus_identity"`
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
