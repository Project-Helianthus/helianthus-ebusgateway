package candidatefacts

const (
	ContractV1         = "helianthus.platform.draft-candidate-fact-graph.v1"
	ReplayContractV1   = "helianthus.platform.draft-candidate-fact-replay.v1"
	RegistryContractV1 = "helianthus.platform.draft-candidate-fact-registry.v1"
	SchemaVersionV1    = uint64(1)
)

type ContractBindingV1 struct {
	OwnerRepository      string
	OwnerCommit          string
	GraphSchemaPath      string
	GraphSchemaSHA256    string
	ReplaySchemaPath     string
	ReplaySchemaSHA256   string
	RegistryPath         string
	RegistrySHA256       string
	SourceContract       string
	SourceSchemaVersion  uint64
	SourceOwnerCommit    string
	SourceSchemaSHA256   string
	SourceRegistrySHA256 string
}

type artifactsV1 struct {
	GraphSchema    []byte
	ReplaySchema   []byte
	Registry       []byte
	PositiveGraph  []byte
	PositiveReplay []byte
	SourceBundle   []byte
	SourceReplay   []byte
}

type GraphV1 struct {
	Contract         string                `json:"contract"`
	SchemaVersion    uint64                `json:"schema_version"`
	GraphID          string                `json:"graph_id"`
	GraphHash        string                `json:"graph_hash"`
	Registry         RegistryBindingV1     `json:"registry"`
	SourceBundle     SourceBundleBindingV1 `json:"source_bundle"`
	Visibility       VisibilityV1          `json:"visibility"`
	Limits           LimitsV1              `json:"limits"`
	ComparatorDrafts []ComparatorDraftV1   `json:"comparator_drafts"`
	Facts            []FactV1              `json:"facts"`
}

type RegistryBindingV1 struct {
	Contract string `json:"contract"`
	Version  uint64 `json:"version"`
	Digest   string `json:"digest"`
}

type SourceBundleBindingV1 struct {
	Contract      string          `json:"contract"`
	SchemaVersion uint64          `json:"schema_version"`
	BundleID      string          `json:"bundle_id"`
	BundleHash    string          `json:"bundle_hash"`
	ReplayHash    string          `json:"replay_hash"`
	EvidenceRefs  []EvidenceRefV1 `json:"evidence_refs"`
}

type EvidenceRefV1 struct {
	Kind            string  `json:"kind"`
	DigestAlgorithm string  `json:"digest_algorithm"`
	Digest          string  `json:"digest"`
	Repository      *string `json:"repository"`
	Commit          *string `json:"commit"`
	Path            *string `json:"path"`
}

type VisibilityV1 struct {
	Channel             string `json:"channel"`
	PromotionState      string `json:"promotion_state"`
	StableExposure      bool   `json:"stable_exposure"`
	CommandCapable      bool   `json:"command_capable"`
	ProtocolTranslation bool   `json:"protocol_translation"`
}

type LimitsV1 struct {
	MaxGraphBytes           uint64 `json:"max_graph_bytes"`
	MaxDepth                uint64 `json:"max_depth"`
	MaxFacts                uint64 `json:"max_facts"`
	MaxEvidenceRefsPerFact  uint64 `json:"max_evidence_refs_per_fact"`
	MaxSamplesPerComparator uint64 `json:"max_samples_per_comparator"`
	MaxStringBytes          uint64 `json:"max_string_bytes"`
	MaxPathSegments         uint64 `json:"max_path_segments"`
	MaxTotalMembers         uint64 `json:"max_total_members"`
	MaxTotalListItems       uint64 `json:"max_total_list_items"`
}

type ComparatorDraftV1 struct {
	DraftID    string                 `json:"draft_id"`
	Type       string                 `json:"type"`
	Parameters ComparatorParametersV1 `json:"parameters"`
}

type ComparatorParametersV1 struct {
	Window                ComparatorWindowV1            `json:"window"`
	Tolerance             ComparatorToleranceV1         `json:"tolerance"`
	UnitConversion        ComparatorUnitConversionV1    `json:"unit_conversion"`
	Rounding              ComparatorRoundingV1          `json:"rounding"`
	MinimumSamples        uint64                        `json:"minimum_samples"`
	MaximumMissingSamples uint64                        `json:"maximum_missing_samples"`
	StaleCutoffNS         uint64                        `json:"stale_cutoff_ns"`
	ConflictThreshold     ComparatorConflictThresholdV1 `json:"conflict_threshold"`
}

type ComparatorWindowV1 struct {
	StartOffsetNS uint64 `json:"start_offset_ns"`
	EndOffsetNS   uint64 `json:"end_offset_ns"`
}

type ComparatorToleranceV1 struct {
	AbsoluteDecimal string `json:"absolute_decimal"`
	RelativePPM     uint64 `json:"relative_ppm"`
}

type ComparatorUnitConversionV1 struct {
	Mode          string `json:"mode"`
	SourceUnit    string `json:"source_unit"`
	TargetUnit    string `json:"target_unit"`
	ScaleDecimal  string `json:"scale_decimal"`
	OffsetDecimal string `json:"offset_decimal"`
}

type ComparatorRoundingV1 struct {
	Mode          string  `json:"mode"`
	DecimalPlaces *uint64 `json:"decimal_places"`
}

type ComparatorConflictThresholdV1 struct {
	AbsoluteDecimal    string `json:"absolute_decimal"`
	ConsecutiveSamples uint64 `json:"consecutive_samples"`
}

type FactV1 struct {
	CandidateID           string                 `json:"candidate_id"`
	ProposedPath          string                 `json:"proposed_path"`
	DraftValue            *string                `json:"draft_value"`
	DraftUnit             *string                `json:"draft_unit"`
	Status                string                 `json:"status"`
	TerminalNegativeState *string                `json:"terminal_negative_state"`
	Confidence            ConfidenceV1           `json:"confidence"`
	Provenance            ProvenanceV1           `json:"provenance"`
	Comparator            ComparatorEvaluationV1 `json:"comparator"`
	Falsifier             FalsifierV1            `json:"falsifier"`
	RetestTrigger         RetestTriggerV1        `json:"retest_trigger"`
	DebugOnly             bool                   `json:"debug_only"`
	FactHash              string                 `json:"fact_hash"`
}

type ConfidenceV1 struct {
	Level      string `json:"level"`
	Basis      string `json:"basis"`
	ScoreMilli uint64 `json:"score_milli"`
}

type ProvenanceV1 struct {
	SourceBundleID     string             `json:"source_bundle_id"`
	NativeEvidenceRefs []EvidenceRefV1    `json:"native_evidence_refs"`
	SourceTerminal     *SourceTerminalV1  `json:"source_terminal,omitempty"`
	EBusSourceID       *string            `json:"ebus_source_id"`
	EBusArtifactID     *string            `json:"ebus_artifact_id"`
	EBus               *EBusIdentityV1    `json:"ebus"`
	EEBusSourceID      *string            `json:"eebus_source_id"`
	EEBusArtifactID    *string            `json:"eebus_artifact_id"`
	EEBusService       *string            `json:"eebus_service"`
	EEBus              *EEBusIdentityV1   `json:"eebus"`
	Cloud              *CloudProvenanceV1 `json:"cloud"`
}

type SourceTerminalV1 struct {
	SourceID            string          `json:"source_id"`
	SourceKind          string          `json:"source_kind"`
	BindingSourceKind   string          `json:"binding_source_kind"`
	SourceContract      string          `json:"source_contract"`
	SourceSchemaVersion uint64          `json:"source_schema_version"`
	Phase               string          `json:"phase"`
	State               string          `json:"state"`
	ErrorCategory       string          `json:"error_category"`
	EBusIdentity        EBusIdentityV1  `json:"ebus_identity"`
	EvidenceRefs        []EvidenceRefV1 `json:"evidence_refs"`
}

type EBusIdentityV1 struct {
	Family               string  `json:"family"`
	TargetPseudonym      string  `json:"target_pseudonym"`
	TargetAddress        *uint64 `json:"target_address,omitempty"`
	TargetProduct        *string `json:"target_product,omitempty"`
	RegisterFamily       *string `json:"register_family,omitempty"`
	RegisterID           *uint64 `json:"register_id,omitempty"`
	UnitScaleSource      string  `json:"unit_scale_source"`
	EvidenceRole         *string `json:"evidence_role,omitempty"`
	Opcode               *uint64 `json:"opcode,omitempty"`
	GG                   *uint64 `json:"GG,omitempty"`
	II                   *uint64 `json:"II,omitempty"`
	RR                   *uint64 `json:"RR,omitempty"`
	SourceAddress        *uint64 `json:"source_address,omitempty"`
	GroupMeaning         *string `json:"group_meaning,omitempty"`
	InstanceGate         *string `json:"instance_gate,omitempty"`
	RegisterCategory     *string `json:"register_category,omitempty"`
	DeviceFamily         *string `json:"device_family,omitempty"`
	ScheduleProgram      *string `json:"schedule_program,omitempty"`
	SlotIndex            *uint64 `json:"slot_index,omitempty"`
	DayOfWeek            *string `json:"day_of_week,omitempty"`
	TimeIdentity         *string `json:"time_identity,omitempty"`
	OperationModeContext *string `json:"operation_mode_context,omitempty"`
}

type EEBusIdentityV1 struct {
	Service     string               `json:"service"`
	Entity      string               `json:"entity"`
	Feature     string               `json:"feature"`
	FeaturePath []EEBusPathSegmentV1 `json:"feature_path"`
}

type EEBusPathSegmentV1 struct {
	Kind     string `json:"kind"`
	Selector string `json:"selector"`
}

type CloudProvenanceV1 struct {
	SourceID   string `json:"source_id"`
	ArtifactID string `json:"artifact_id"`
	EvidenceID string `json:"evidence_id"`
}

type ComparatorEvaluationV1 struct {
	DraftID string               `json:"draft_id"`
	Samples []ComparatorSampleV1 `json:"samples"`
	Outcome string               `json:"outcome"`
}

type ComparatorSampleV1 struct {
	OffsetNS uint64               `json:"offset_ns"`
	Left     ObservationBindingV1 `json:"left"`
	Right    ObservationBindingV1 `json:"right"`
	State    string               `json:"state"`
}

type ObservationBindingV1 struct {
	SourceKind       string        `json:"source_kind"`
	SourceID         string        `json:"source_id"`
	ArtifactID       string        `json:"artifact_id"`
	EvidenceRef      EvidenceRefV1 `json:"evidence_ref"`
	ObservedOffsetNS uint64        `json:"observed_offset_ns"`
	ValuePointer     string        `json:"value_pointer"`
	UnitPointer      string        `json:"unit_pointer"`
	NativeDecimal    *string       `json:"native_decimal"`
	NativeUnit       *string       `json:"native_unit"`
}

type FalsifierV1 struct {
	ConditionCode         string `json:"condition_code"`
	ExpectedTerminalState string `json:"expected_terminal_state"`
	Description           string `json:"description"`
}

type RetestTriggerV1 struct {
	TriggerCode         string   `json:"trigger_code"`
	RequiredSourceKinds []string `json:"required_source_kinds"`
	MinimumNewSamples   uint64   `json:"minimum_new_samples"`
}

type BuildInputV1 struct {
	SourceBundle     []byte
	SourceReplay     []byte
	ComparatorDrafts []ComparatorDraftV1
	Facts            []FactV1
}
