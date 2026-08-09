package promotionlock

const (
	ContractV1                   = "helianthus.platform.leaf-promotion-lock-result.v1"
	capturedAssessmentContractV1 = "helianthus.platform.leaf-promotion-captured-assessment.v1"
	SchemaVersionV1              = uint64(1)

	ProfileSyntheticConformanceV1  = "SYNTHETIC_CONFORMANCE"
	ProfileCapturedZeroPromotionV1 = "CAPTURED_RUNTIME_ZERO_PROMOTION"
	ExportTierPublicRedactedV1     = "PUBLIC_REDACTED"
	exportTierPrivateOperatorV1    = "PRIVATE_OPERATOR"
	M9BlockedZeroPromotedLeavesV1  = "BLOCKED_ZERO_PROMOTED_LEAVES"
	VerdictValidZeroPromotionV1    = "VALID_ZERO_PROMOTION"
	replayToolV1                   = "leaf-promotion-replay"
)

// InputsV1 is the closed V1 boundary for synthetic conformance or captured
// runtime zero-promotion derivation. Captured inputs mirror coexistence.InputsV1
// and add the exact generated M8 report.
type InputsV1 struct {
	Profile  string
	Registry []byte
	Dossier  []byte

	M7Graph                []byte
	M7Replay               []byte
	M7Registry             []byte
	M7SourceBundle         []byte
	M7SourceReplay         []byte
	M7LiveStatus           []byte
	M7TerminalGraph        []byte
	M7TerminalReplay       []byte
	M7TerminalSourceBundle []byte
	M7TerminalSourceReplay []byte
	M8Evidence             []byte
	M8Report               []byte
	M8Registry             []byte
}

type ResultV1 struct {
	Contract       string                  `json:"contract"`
	SchemaVersion  uint64                  `json:"schema_version"`
	Profile        string                  `json:"profile"`
	ExportTier     string                  `json:"export_tier"`
	DossierID      string                  `json:"dossier_id,omitempty"`
	DossierHash    string                  `json:"dossier_hash,omitempty"`
	SourceBindings *PublicSourceBindingsV1 `json:"source_bindings,omitempty"`
	ReplayTool     string                  `json:"replay_tool"`
	ReplayVersion  uint64                  `json:"replay_version"`
	Counts         CountsV1                `json:"counts"`
	DossierCount   uint64                  `json:"dossier_count"`
	Leaves         []SyntheticLeafV1       `json:"leaves,omitempty"`
	Assessments    []PublicAssessmentV1    `json:"assessments,omitempty"`
	M9ConsumerGate string                  `json:"m9_consumer_gate"`
	Verdict        string                  `json:"verdict"`
	ResultHash     string                  `json:"result_hash"`
}

type PublicSourceBindingsV1 struct {
	M7GatewaySourceCommit  string `json:"m7_gateway_source_commit"`
	M7DocsSourceCommit     string `json:"m7_docs_source_commit"`
	M7GraphID              string `json:"m7_graph_id"`
	M7GraphHash            string `json:"m7_graph_hash"`
	M7ReplayID             string `json:"m7_replay_id"`
	M7ReplayHash           string `json:"m7_replay_hash"`
	M7StatusProjectionID   string `json:"m7_status_projection_id"`
	M7StatusProjectionHash string `json:"m7_status_projection_hash"`
	M8GatewaySourceCommit  string `json:"m8_gateway_source_commit"`
	M8DocsSourceCommit     string `json:"m8_docs_source_commit"`
	M8EvidenceID           string `json:"m8_evidence_id"`
	M8EvidenceHash         string `json:"m8_evidence_hash"`
	M8ReportID             string `json:"m8_report_id"`
	M8ReportHash           string `json:"m8_report_hash"`
	CoexistenceVerdict     string `json:"coexistence_verdict"`
}

type CountsV1 struct {
	Total    uint64 `json:"total"`
	Promoted uint64 `json:"promoted"`
	Withheld uint64 `json:"withheld"`
}

type SyntheticLeafV1 struct {
	LeafID        string  `json:"leaf_id"`
	SemanticPath  string  `json:"semantic_path"`
	Decision      string  `json:"decision"`
	TerminalState *string `json:"terminal_state"`
	Visibility    string  `json:"visibility"`
}

type PublicAssessmentV1 struct {
	CandidateID        string   `json:"candidate_id"`
	FactHash           string   `json:"fact_hash"`
	SourceStatus       string   `json:"source_status"`
	TerminalState      *string  `json:"terminal_state"`
	Decision           string   `json:"decision"`
	WithholdingReasons []string `json:"withholding_reasons"`
	RetestTrigger      RetestV1 `json:"retest_trigger"`
}

type RetestV1 struct {
	Trigger             string   `json:"trigger"`
	RequiredSourceKinds []string `json:"required_source_kinds"`
	MinimumNewSamples   uint64   `json:"minimum_new_samples"`
}

type capturedAssessmentV1 struct {
	Contract       string                 `json:"contract"`
	SchemaVersion  uint64                 `json:"schema_version"`
	Profile        string                 `json:"profile"`
	ExportTier     string                 `json:"export_tier"`
	SourceBindings PublicSourceBindingsV1 `json:"source_bindings"`
	Assessments    []privateAssessmentV1  `json:"assessments"`
	Dossiers       []string               `json:"dossiers"`
	M9ConsumerGate string                 `json:"m9_consumer_gate"`
	AssessmentHash string                 `json:"assessment_hash"`
}

type privateAssessmentV1 struct {
	CandidateID        string        `json:"candidate_id"`
	SemanticPath       string        `json:"semantic_path"`
	FactHash           string        `json:"fact_hash"`
	SourceStatus       string        `json:"source_status"`
	TerminalState      *string       `json:"terminal_state"`
	Eligibility        eligibilityV1 `json:"eligibility"`
	Decision           string        `json:"decision"`
	WithholdingReasons []string      `json:"withholding_reasons"`
	RetestTrigger      RetestV1      `json:"retest_trigger"`
}

type eligibilityV1 struct {
	ExactEBusIdentity        bool `json:"exact_ebus_identity"`
	ExactEEBusPath           bool `json:"exact_eebus_path"`
	ComparatorMatch          bool `json:"comparator_match"`
	CapturedEvidenceEligible bool `json:"captured_evidence_eligible"`
	CoexistenceNoDrift       bool `json:"coexistence_no_drift"`
}

type categoryError string

func (err categoryError) Error() string { return string(err) }

func fail(category string) error { return categoryError(category) }
