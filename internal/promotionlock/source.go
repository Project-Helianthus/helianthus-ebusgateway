package promotionlock

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/candidatefacts"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/coexistence"
)

var sourceSHA256V1 = map[string]string{
	"m7_graph":         "b5c5d79e540a1691ee60c6db3e9405a92d9d544d871c74b26800fe449a318b0e",
	"m7_replay":        "8280f6278ffe8598dfd767bb5bf9e60dce3c145b4612174b7c5a32fbff282f5c",
	"m7_registry":      "e6895b8d7406b58ed97599d8da7e9bd3b252e6e7ca3b0578ec6385bfe6dfe1c0",
	"m7_source_bundle": "e6db2862f9001148deb6f40e286ee5f1eef2907812685a9b48128ddbfca5ce5a",
	"m7_source_replay": "3061c507677f1f41861c20096ff7581ccb6e35c2e01bf66a568e2277df285539",
	"m8_evidence":      "7fe0eced765b2599ee6341985d60edf86936485c75c56cb8c51892775f8d352b",
	"m8_report":        "687662756a2c833548bbc15285c895442c70346bf3832bf21c3a8309a9da0007",
	"m8_registry":      "82cf854d335da31dcda65ff45a024cbb4a1ce515965cb8165122c0b4ef7d8505",
}

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

type m8EvidenceHeaderV1 struct {
	Contract      string `json:"contract"`
	EvidenceID    string `json:"evidence_id"`
	EvidenceHash  string `json:"evidence_hash"`
	EvidenceClass string `json:"evidence_class"`
	Scope         struct {
		LiveVR940Claim bool `json:"live_vr940_claim"`
	} `json:"scope"`
}

type m8ReportHeaderV1 struct {
	Contract     string `json:"contract"`
	EvidenceID   string `json:"evidence_id"`
	EvidenceHash string `json:"evidence_hash"`
	ReportID     string `json:"report_id"`
	ReportHash   string `json:"report_hash"`
	Verdict      string `json:"verdict"`
}

func exactSource(name string, raw []byte) bool {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]) == sourceSHA256V1[name]
}

func validateSources(inputs InputsV1) (verifiedSourcesV1, error) {
	checks := []struct {
		name     string
		raw      []byte
		category string
	}{
		{"m7_graph", inputs.M7Graph, "source.m7_graph"},
		{"m7_replay", inputs.M7Replay, "source.m7_replay"},
		{"m7_registry", inputs.M7Registry, "source.m7_registry"},
		{"m7_source_bundle", inputs.M7SourceBundle, "source.m7_bundle"},
		{"m7_source_replay", inputs.M7SourceReplay, "source.m7_bundle"},
		{"m8_evidence", inputs.M8Evidence, "source.m8_evidence"},
		{"m8_report", inputs.M8Report, "source.m8_report"},
		{"m8_registry", inputs.M8Registry, "source.m8_registry"},
	}
	for _, check := range checks {
		if !exactSource(check.name, check.raw) {
			return verifiedSourcesV1{}, fail(check.category)
		}
	}

	if err := candidatefacts.Verify(inputs.M7Graph, inputs.M7SourceBundle, inputs.M7SourceReplay); err != nil {
		return verifiedSourcesV1{}, fail("source.m7_graph")
	}
	replayed, err := candidatefacts.Replay(inputs.M7Graph, inputs.M7SourceBundle, inputs.M7SourceReplay)
	if err != nil || !bytes.Equal(replayed, inputs.M7Replay) {
		return verifiedSourcesV1{}, fail("source.m7_replay")
	}
	coexistenceInputs := coexistence.InputsV1{
		Evidence:       inputs.M8Evidence,
		Registry:       inputs.M8Registry,
		M7Graph:        inputs.M7Graph,
		M7Replay:       inputs.M7Replay,
		M7Registry:     inputs.M7Registry,
		M7SourceBundle: inputs.M7SourceBundle,
		M7SourceReplay: inputs.M7SourceReplay,
	}
	if err := coexistence.Verify(coexistenceInputs); err != nil {
		return verifiedSourcesV1{}, fail("source.m8_evidence")
	}
	report, err := coexistence.Report(coexistenceInputs)
	if err != nil || !bytes.Equal(report, inputs.M8Report) {
		return verifiedSourcesV1{}, fail("source.m8_report")
	}

	var sources verifiedSourcesV1
	if err := json.Unmarshal(inputs.M7Graph, &sources.graph); err != nil ||
		json.Unmarshal(inputs.M7Replay, &sources.replay) != nil ||
		json.Unmarshal(inputs.M8Evidence, &sources.evidence) != nil ||
		json.Unmarshal(inputs.M8Report, &sources.report) != nil {
		return verifiedSourcesV1{}, fail("source.decode")
	}
	if sources.replay.GraphID != sources.graph.GraphID || sources.replay.GraphHash != sources.graph.GraphHash ||
		sources.report.EvidenceID != sources.evidence.EvidenceID ||
		sources.report.EvidenceHash != sources.evidence.EvidenceHash || sources.report.Verdict != "PASS" {
		return verifiedSourcesV1{}, fail("source.binding")
	}
	return sources, nil
}
