package promotionlock

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/candidatefacts"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/coexistence"
)

const (
	m7GatewaySourceCommitV1 = "8bcba2107d10b149f984ac9546ea6427a9cda8a1"
	m7DocsSourceCommitV1    = "35d2eba256a77b6575a2b45c07e73f054ff74ced"
	m8GatewaySourceCommitV1 = "89cf8876a9cd8aa4e6aab9ad21cc05cac523426a"
	m8DocsSourceCommitV1    = "9cede4c61a4f73019142b7418cf6f87537cf645c"

	m7LiveStatusSHA256V1     = "63ecafd94d507cedadc2cba4bb9ac108488d1020d3b43b0ad034409460826428"
	m7StatusProjectionIDV1   = "dcfpsv1:sha256:856cc167c33cad57c6b761fb82fe1e8872966bcc813a0f64c6b54b7b2dd7cfa8"
	m7StatusProjectionHashV1 = "sha256:856cc167c33cad57c6b761fb82fe1e8872966bcc813a0f64c6b54b7b2dd7cfa8"
)

type verifiedSourcesV1 struct {
	graph    candidatefacts.GraphV1
	replay   m7ReplayHeaderV1
	evidence m8EvidenceHeaderV1
	report   m8ReportHeaderV1
}

type m7ReplayHeaderV1 struct {
	Contract   string `json:"contract"`
	GraphID    string `json:"graph_id"`
	GraphHash  string `json:"graph_hash"`
	ReplayID   string `json:"replay_id"`
	ReplayHash string `json:"replay_hash"`
}

type m7StatusHeaderV1 struct {
	Contract         string `json:"contract"`
	SchemaVersion    uint64 `json:"schema_version"`
	ExportTier       string `json:"export_tier"`
	ProjectionID     string `json:"projection_id"`
	ProjectionHash   string `json:"projection_hash"`
	SourceCommit     string `json:"source_commit"`
	DocsSourceCommit string `json:"docs_source_commit"`
	SourceGraphID    string `json:"source_graph_id"`
	SourceGraphHash  string `json:"source_graph_hash"`
	SourceReplayID   string `json:"source_replay_id"`
	SourceReplayHash string `json:"source_replay_hash"`
	FactCount        uint64 `json:"fact_count"`
	StatusCounts     struct {
		RawOnly  uint64 `json:"RAW_ONLY"`
		Withheld uint64 `json:"WITHHELD"`
	} `json:"status_counts"`
}

type m8EvidenceHeaderV1 struct {
	Contract      string                  `json:"contract"`
	EvidenceID    string                  `json:"evidence_id"`
	EvidenceHash  string                  `json:"evidence_hash"`
	EvidenceClass string                  `json:"evidence_class"`
	Runs          []m8EvidenceRunHeaderV1 `json:"runs"`
}

type m8EvidenceRunHeaderV1 struct {
	Provenance struct {
		Runtime struct {
			SourceCommit string `json:"source_commit"`
		} `json:"runtime"`
	} `json:"provenance"`
}

type m8ReportHeaderV1 struct {
	Contract      string `json:"contract"`
	EvidenceClass string `json:"evidence_class"`
	EvidenceID    string `json:"evidence_id"`
	EvidenceHash  string `json:"evidence_hash"`
	ReportID      string `json:"report_id"`
	ReportHash    string `json:"report_hash"`
	Verdict       string `json:"verdict"`
}

func validateRegistryV1(raw []byte) error {
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != promotionRegistrySHA256V1 {
		return fail("registry.binding")
	}
	return nil
}

func validateSyntheticInputs(inputs InputsV1) error {
	if inputs.Profile != ProfileSyntheticConformanceV1 || len(inputs.Registry) == 0 ||
		!bytes.Equal(inputs.Dossier, mustContractArtifactV1("docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json")) ||
		hasCapturedInputs(inputs) {
		return fail("synthetic.input")
	}
	return nil
}

func validateCapturedSources(inputs InputsV1) (verifiedSourcesV1, error) {
	if inputs.Profile != ProfileCapturedZeroPromotionV1 || len(inputs.Dossier) != 0 || !hasCompleteCapturedInputs(inputs) {
		return verifiedSourcesV1{}, fail("captured.input")
	}
	if err := candidatefacts.Verify(inputs.M7Graph, inputs.M7SourceBundle, inputs.M7SourceReplay); err != nil {
		return verifiedSourcesV1{}, err
	}
	replayed, err := candidatefacts.Replay(inputs.M7Graph, inputs.M7SourceBundle, inputs.M7SourceReplay)
	if err != nil {
		return verifiedSourcesV1{}, err
	}
	if !bytes.Equal(replayed, inputs.M7Replay) {
		return verifiedSourcesV1{}, fail("projection.replay")
	}

	var sources verifiedSourcesV1
	if err := json.Unmarshal(inputs.M7Graph, &sources.graph); err != nil ||
		json.Unmarshal(inputs.M7Replay, &sources.replay) != nil {
		return verifiedSourcesV1{}, fail("captured.input")
	}
	if err := validateM7Status(inputs.M7LiveStatus, sources.graph, sources.replay); err != nil {
		return verifiedSourcesV1{}, err
	}

	coexistenceInputs := coexistence.InputsV1{
		Evidence:               inputs.M8Evidence,
		Registry:               inputs.M8Registry,
		M7Graph:                inputs.M7Graph,
		M7Replay:               inputs.M7Replay,
		M7Registry:             inputs.M7Registry,
		M7SourceBundle:         inputs.M7SourceBundle,
		M7SourceReplay:         inputs.M7SourceReplay,
		M7LiveStatus:           inputs.M7LiveStatus,
		M7TerminalGraph:        inputs.M7TerminalGraph,
		M7TerminalReplay:       inputs.M7TerminalReplay,
		M7TerminalSourceBundle: inputs.M7TerminalSourceBundle,
		M7TerminalSourceReplay: inputs.M7TerminalSourceReplay,
	}
	if err := coexistence.Verify(coexistenceInputs); err != nil {
		return verifiedSourcesV1{}, err
	}
	report, err := coexistence.Report(coexistenceInputs)
	if err != nil {
		return verifiedSourcesV1{}, err
	}
	if !bytes.Equal(report, inputs.M8Report) {
		return verifiedSourcesV1{}, fail("captured.coexistence")
	}
	if err := json.Unmarshal(inputs.M8Evidence, &sources.evidence); err != nil ||
		json.Unmarshal(inputs.M8Report, &sources.report) != nil {
		return verifiedSourcesV1{}, fail("captured.input")
	}
	if err := validateM8RuntimeSourceCommits(sources.evidence); err != nil {
		return verifiedSourcesV1{}, err
	}
	if sources.evidence.EvidenceClass != "CAPTURED_RUNTIME_EVIDENCE" ||
		sources.report.EvidenceClass != "CAPTURED_RUNTIME_EVIDENCE" || sources.report.Verdict != "PASS" ||
		sources.report.EvidenceID != sources.evidence.EvidenceID || sources.report.EvidenceHash != sources.evidence.EvidenceHash {
		return verifiedSourcesV1{}, fail("captured.predecessor")
	}
	return sources, nil
}

func validateM7Status(raw []byte, graph candidatefacts.GraphV1, replay m7ReplayHeaderV1) error {
	var status m7StatusHeaderV1
	if err := json.Unmarshal(raw, &status); err != nil {
		return fail("captured.status")
	}
	if status.SourceCommit != m7GatewaySourceCommitV1 || status.DocsSourceCommit != m7DocsSourceCommitV1 {
		return fail("captured.predecessor")
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != m7LiveStatusSHA256V1 ||
		status.Contract != "helianthus.platform.draft-candidate-fact-public-status.v1" ||
		status.SchemaVersion != SchemaVersionV1 || status.ExportTier != ExportTierPublicRedactedV1 ||
		status.ProjectionID != m7StatusProjectionIDV1 || status.ProjectionHash != m7StatusProjectionHashV1 ||
		status.SourceGraphID != graph.GraphID || status.SourceGraphHash != graph.GraphHash ||
		status.SourceReplayID != replay.ReplayID || status.SourceReplayHash != replay.ReplayHash ||
		status.FactCount != 18 || status.StatusCounts.RawOnly != 14 || status.StatusCounts.Withheld != 4 {
		return fail("captured.status")
	}
	return nil
}

func validateM8RuntimeSourceCommits(evidence m8EvidenceHeaderV1) error {
	for _, run := range evidence.Runs {
		if run.Provenance.Runtime.SourceCommit != m8GatewaySourceCommitV1 {
			return fail("captured.predecessor")
		}
	}
	return nil
}

func hasCompleteCapturedInputs(inputs InputsV1) bool {
	for _, raw := range [][]byte{
		inputs.Registry, inputs.M7Graph, inputs.M7Replay, inputs.M7Registry, inputs.M7SourceBundle, inputs.M7SourceReplay,
		inputs.M7LiveStatus, inputs.M7TerminalGraph, inputs.M7TerminalReplay, inputs.M7TerminalSourceBundle,
		inputs.M7TerminalSourceReplay, inputs.M8Evidence, inputs.M8Report, inputs.M8Registry,
	} {
		if len(raw) == 0 {
			return false
		}
	}
	return true
}

func hasCapturedInputs(inputs InputsV1) bool {
	for _, raw := range [][]byte{
		inputs.M7Graph, inputs.M7Replay, inputs.M7Registry, inputs.M7SourceBundle, inputs.M7SourceReplay,
		inputs.M7LiveStatus, inputs.M7TerminalGraph, inputs.M7TerminalReplay, inputs.M7TerminalSourceBundle,
		inputs.M7TerminalSourceReplay, inputs.M8Evidence, inputs.M8Report, inputs.M8Registry,
	} {
		if len(raw) != 0 {
			return true
		}
	}
	return false
}

func cloneInputsV1(inputs InputsV1) InputsV1 {
	return InputsV1{
		Profile:                inputs.Profile,
		Registry:               bytes.Clone(inputs.Registry),
		Dossier:                bytes.Clone(inputs.Dossier),
		M7Graph:                bytes.Clone(inputs.M7Graph),
		M7Replay:               bytes.Clone(inputs.M7Replay),
		M7Registry:             bytes.Clone(inputs.M7Registry),
		M7SourceBundle:         bytes.Clone(inputs.M7SourceBundle),
		M7SourceReplay:         bytes.Clone(inputs.M7SourceReplay),
		M7LiveStatus:           bytes.Clone(inputs.M7LiveStatus),
		M7TerminalGraph:        bytes.Clone(inputs.M7TerminalGraph),
		M7TerminalReplay:       bytes.Clone(inputs.M7TerminalReplay),
		M7TerminalSourceBundle: bytes.Clone(inputs.M7TerminalSourceBundle),
		M7TerminalSourceReplay: bytes.Clone(inputs.M7TerminalSourceReplay),
		M8Evidence:             bytes.Clone(inputs.M8Evidence),
		M8Report:               bytes.Clone(inputs.M8Report),
		M8Registry:             bytes.Clone(inputs.M8Registry),
	}
}
