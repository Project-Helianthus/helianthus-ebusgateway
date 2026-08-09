package coexistence

// InputsV1 is the closed verification boundary for MSP-08 evidence.
type InputsV1 struct {
	Evidence               []byte
	Registry               []byte
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

type categoryError string

func (err categoryError) Error() string { return string(err) }

func fail(category string) error { return categoryError(category) }

const (
	coexEvidenceContractV1 = "helianthus.platform.multi-runtime-coexistence-evidence.v1"
	coexReportContractV1   = "helianthus.platform.multi-runtime-coexistence-report.v1"
	coexRegistryContractV1 = "helianthus.platform.multi-runtime-coexistence-registry.v1"

	coexBaselineGatewayCommit = "ff511b035b85aef6123fb0853bb3d2f3af6fc01e"
	coexSyntheticDocsCommit   = "ea88fef23ecb154b08f70e7f94b36e1738ed08bf"
	coexLiveGatewayCommit     = "8bcba2107d10b149f984ac9546ea6427a9cda8a1"
	coexLiveDocsCommit        = "35d2eba256a77b6575a2b45c07e73f054ff74ced"

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
	coexM7StatusDomainV1         = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-M7-PUBLIC-STATUS:V1"
	coexRestartProcessDomainV1   = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-RESTART-PROCESS-EVENT:V1"
	coexRestartSnapshotDomainV1  = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-RESTART-SNAPSHOT:V1"
	coexRestartSessionDomainV1   = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-RESTART-SESSION-EVENT:V1"
	coexRestartTrustDomainV1     = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-RESTART-TRUST:V1"
	coexRestartPeerDomainV1      = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-RESTART-PEER:V1"

	maxSafeIntegerV1 = int64(9_007_199_254_740_991)
)

var hardLimitsV1 = map[string]int64{
	"max_evidence_bytes":         2_097_152,
	"max_depth":                  32,
	"max_runs":                   8,
	"max_views_per_run":          16,
	"max_inputs_per_run":         27,
	"max_internal_facts_per_run": 64,
	"max_payload_bytes":          262_144,
	"max_string_bytes":           4_096,
	"max_total_members":          65_536,
	"max_total_list_items":       32_768,
}

var coexScenarioProfilesV1 = map[string][]string{
	"SYNTHETIC_OFFLINE_FIXTURE": {
		"EEBUS_DISABLED_BASELINE",
		"EEBUS_DISABLED_CONFIRMED",
		"EEBUS_ENABLED_NO_SERVICES",
		"EEBUS_CONNECTED_CANDIDATE_ONLY",
		"EEBUS_CONFLICTED_WITHHELD",
		"EEBUS_DISABLED_ROLLBACK",
	},
	"CAPTURED_RUNTIME_EVIDENCE": {
		"EEBUS_CONNECTED_BASELINE",
		"EEBUS_CONNECTED_RAW_WITHHELD",
		"EEBUS_RESTART_PERSISTED",
		"EEBUS_CONNECTED_ROLLBACK",
	},
}
