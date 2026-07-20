package promotionlock

const (
	ContractV1      = "helianthus.gateway.leaf-promotion-lock-manifest.v1"
	SchemaVersionV1 = uint64(1)
)

// InputsV1 is the closed M7/M8 evidence boundary consumed by the M8.5 lock.
type InputsV1 struct {
	M7Graph        []byte
	M7Replay       []byte
	M7Registry     []byte
	M7SourceBundle []byte
	M7SourceReplay []byte
	M8Evidence     []byte
	M8Report       []byte
	M8Registry     []byte
}

type ContractBindingV1 struct {
	OwnerRepository          string            `json:"owner_repository"`
	OwnerCommit              string            `json:"owner_commit"`
	OwnerTree                string            `json:"owner_tree"`
	OwnerExactHeadActionsRun uint64            `json:"owner_exact_head_actions_run"`
	OwnerPostMainActionsRun  uint64            `json:"owner_post_main_actions_run"`
	ArtifactSHA256           map[string]string `json:"artifact_sha256"`
}

type ManifestV1 struct {
	Contract             string                  `json:"contract"`
	SchemaVersion        uint64                  `json:"schema_version"`
	ManifestID           string                  `json:"manifest_id"`
	ManifestHash         string                  `json:"manifest_hash"`
	ContractBinding      ContractReferenceV1     `json:"contract_binding"`
	SourceBindings       SourceBindingsV1        `json:"source_bindings"`
	PromotionState       string                  `json:"promotion_state"`
	Counts               CountsV1                `json:"counts"`
	Assessments          []CandidateAssessmentV1 `json:"assessments"`
	PromotedPaths        []string                `json:"promoted_paths"`
	LockedDossierIDs     []string                `json:"locked_dossier_ids"`
	M9ConsumerGate       string                  `json:"m9_consumer_gate"`
	Verdict              string                  `json:"verdict"`
	StableSurfaceChanges bool                    `json:"stable_surface_changes"`
}

type ContractReferenceV1 struct {
	OwnerRepository string `json:"owner_repository"`
	OwnerCommit     string `json:"owner_commit"`
	OwnerTree       string `json:"owner_tree"`
}

type SourceBindingsV1 struct {
	M7GraphID       string `json:"m7_graph_id"`
	M7GraphHash     string `json:"m7_graph_hash"`
	M7ReplayID      string `json:"m7_replay_id"`
	M7ReplayHash    string `json:"m7_replay_hash"`
	M8EvidenceID    string `json:"m8_evidence_id"`
	M8EvidenceHash  string `json:"m8_evidence_hash"`
	M8ReportID      string `json:"m8_report_id"`
	M8ReportHash    string `json:"m8_report_hash"`
	EvidenceClass   string `json:"evidence_class"`
	LiveVR940Claim  bool   `json:"live_vr940_claim"`
	CoexistencePass bool   `json:"coexistence_pass"`
}

type CountsV1 struct {
	Candidates uint64 `json:"candidates"`
	Dossiers   uint64 `json:"dossiers"`
	Promoted   uint64 `json:"promoted"`
	Withheld   uint64 `json:"withheld"`
}

type CandidateAssessmentV1 struct {
	CandidateID        string  `json:"candidate_id"`
	SemanticPath       string  `json:"semantic_path"`
	CandidateStatus    string  `json:"candidate_status"`
	CandidateHash      string  `json:"candidate_hash"`
	TerminalState      *string `json:"terminal_state"`
	ExactEBusIdentity  bool    `json:"exact_ebus_identity"`
	ExactEEBusIdentity bool    `json:"exact_eebus_identity"`
	ComparatorPassed   bool    `json:"comparator_passed"`
	Decision           string  `json:"decision"`
	Visibility         string  `json:"visibility"`
	DossierState       string  `json:"dossier_state"`
	ReasonCode         string  `json:"reason_code"`
	RetestTrigger      string  `json:"retest_trigger"`
	MinimumNewSamples  uint64  `json:"minimum_new_samples"`
}

type categoryError string

func (err categoryError) Error() string { return string(err) }

func fail(category string) error { return categoryError(category) }
