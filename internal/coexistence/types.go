package coexistence

// InputsV1 is the closed verification boundary for MSP-08 evidence.
type InputsV1 struct {
	Evidence       []byte
	Registry       []byte
	M7Graph        []byte
	M7Replay       []byte
	M7Registry     []byte
	M7SourceBundle []byte
	M7SourceReplay []byte
}

// GenerateInputsV1 is deliberately distinct from InputsV1. Generation cannot
// be selected by omitting evidence from the verification boundary.
type GenerateInputsV1 struct {
	Registry          []byte
	M7Graph           []byte
	M7Replay          []byte
	M7Registry        []byte
	M7SourceBundle    []byte
	M7SourceReplay    []byte
	BaselineRuntime   []byte
	ComparedRuntime   []byte
	CaptureClock      []byte
	CaptureTimestamps []byte
	MaskedSubjects    []byte
}

type ArtifactsV1 struct {
	Evidence []byte
	Report   []byte
}

type BindingV1 struct {
	OwnerRepository          string            `json:"owner_repository"`
	OwnerCommit              string            `json:"owner_commit"`
	OwnerTree                string            `json:"owner_tree"`
	OwnerExactHeadActionsRun uint64            `json:"owner_exact_head_actions_run"`
	OwnerPostMainActionsRun  uint64            `json:"owner_post_main_actions_run"`
	BaselineGatewayCommit    string            `json:"baseline_gateway_commit"`
	M7CompletionToken        string            `json:"m7_completion_token"`
	ArtifactSHA256           map[string]string `json:"artifact_sha256"`
}

type categoryError string

func (err categoryError) Error() string { return string(err) }

func fail(category string) error { return categoryError(category) }

const (
	coexEvidenceContractV1 = "helianthus.platform.multi-runtime-coexistence-evidence.v1"
	coexReportContractV1   = "helianthus.platform.multi-runtime-coexistence-report.v1"
	coexRegistryContractV1 = "helianthus.platform.multi-runtime-coexistence-registry.v1"

	ownerRepository           = "Project-Helianthus/helianthus-docs-ebus"
	ownerCommit               = "fa335b6f66c97f5f82519ae71f3078687d919800"
	ownerTree                 = "6afa212c29b9270edea031b78de39411505c27b6"
	ownerExactHeadActionsRun  = uint64(29707590892)
	ownerPostMainActionsRun   = uint64(29707652181)
	coexBaselineGatewayCommit = "ff511b035b85aef6123fb0853bb3d2f3af6fc01e"
	coexM7CompletionToken     = "MSP-07@ff511b035b85aef6123fb0853bb3d2f3af6fc01e"
	coexM7DocsSourceCommit    = "ea88fef23ecb154b08f70e7f94b36e1738ed08bf"

	coexRawPayloadDomainV1       = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-RAW-PAYLOAD:V1"
	coexShapeDomainV1            = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-PAYLOAD-SHAPE:V1"
	coexCanonicalPayloadDomainV1 = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-CANONICAL-PAYLOAD:V1"
	coexNormalizationDomainV1    = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-NORMALIZATION:V1"
	coexClockDomainV1            = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-CLOCK:V1"
	coexBuildDomainV1            = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-BUILD:V1"
	coexConfigDomainV1           = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-CONFIG:V1"
	coexAuthDomainV1             = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-AUTH:V1"
	coexEvidenceDomainV1         = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-EVIDENCE:V1"
	coexReportDomainV1           = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-REPORT:V1"

	maxSafeIntegerV1 = int64(9_007_199_254_740_991)
)

var hardLimitsV1 = map[string]int64{
	"max_evidence_bytes":         2_097_152,
	"max_depth":                  32,
	"max_runs":                   8,
	"max_views_per_run":          16,
	"max_inputs_per_run":         16,
	"max_internal_facts_per_run": 64,
	"max_payload_bytes":          262_144,
	"max_string_bytes":           4_096,
	"max_total_members":          65_536,
	"max_total_list_items":       32_768,
}

var coexScenarioOrderV1 = []string{
	"EEBUS_DISABLED_BASELINE",
	"EEBUS_DISABLED_CONFIRMED",
	"EEBUS_ENABLED_NO_SERVICES",
	"EEBUS_CONNECTED_CANDIDATE_ONLY",
	"EEBUS_CONFLICTED_WITHHELD",
	"EEBUS_DISABLED_ROLLBACK",
}
