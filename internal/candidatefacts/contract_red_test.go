package candidatefacts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const (
	docsMergeV1         = "ea88fef23ecb154b08f70e7f94b36e1738ed08bf"
	sourceOwnerCommitV1 = "4d7747730be023acb251b20a22d796545a9f3688"
)

var canonicalArtifactDigestsV1 = map[string]string{
	"graph_schema":    "ab5f65a527633c3c5f60119cf0071a6ffdd3ee66d6e210f78bf17a03e438069a",
	"replay_schema":   "d691cfc970e99faae7f5a1f4e30ed0cb6a4c3cc6e11959dffba5f82a8fc6d232",
	"registry":        "e6895b8d7406b58ed97599d8da7e9bd3b252e6e7ca3b0578ec6385bfe6dfe1c0",
	"positive_graph":  "b5c5d79e540a1691ee60c6db3e9405a92d9d544d871c74b26800fe449a318b0e",
	"positive_replay": "8280f6278ffe8598dfd767bb5bf9e60dce3c145b4612174b7c5a32fbff282f5c",
	"source_bundle":   "e6db2862f9001148deb6f40e286ee5f1eef2907812685a9b48128ddbfca5ce5a",
	"source_replay":   "3061c507677f1f41861c20096ff7581ccb6e35c2e01bf66a568e2277df285539",
}

var negativeCategoriesV1 = map[string]string{
	"anti-leak-stable-surface.json":     "anti_leak.consumer",
	"comparator-parameter-invalid.json": "comparator.invalid",
	"evidence-ref-not-in-bundle.json":   "provenance.binding",
	"forged-artifact-id.json":           "provenance.binding",
	"forged-b524-opcode.json":           "identity.native",
	"forged-eebus-entity-feature.json":  "identity.native",
	"forged-source-id.json":             "provenance.binding",
	"graph-hash-mismatch.json":          "hash.graph",
	"incomplete-b524-identity.json":     "schema.graph",
	"invalid-eebus-feature-path.json":   "identity.native",
	"limit-exceeded.json":               "limits.exceeded",
	"ordering-invalid.json":             "ordering.invalid",
	"registry-mismatch.json":            "registry.binding",
	"terminal-state-not-withheld.json":  "state.terminal",
	"unknown-field.json":                "schema.graph",
	"wrong-source-bundle.json":          "provenance.binding",
	"wrong-source-replay.json":          "provenance.binding",
}

func TestMSP07PinnedContractAndSourceAuthorityV1(t *testing.T) {
	if ContractV1 != "helianthus.platform.draft-candidate-fact-graph.v1" ||
		ReplayContractV1 != "helianthus.platform.draft-candidate-fact-replay.v1" ||
		RegistryContractV1 != "helianthus.platform.draft-candidate-fact-registry.v1" ||
		SchemaVersionV1 != 1 {
		t.Fatalf("candidate contract drift: graph=%q replay=%q registry=%q version=%d",
			ContractV1, ReplayContractV1, RegistryContractV1, SchemaVersionV1)
	}

	got := PinnedContractV1()
	want := ContractBindingV1{
		OwnerRepository:      "Project-Helianthus/helianthus-docs-ebus",
		OwnerCommit:          docsMergeV1,
		GraphSchemaPath:      "docs/platform/schemas/draft-candidate-fact-graph-v1.schema.json",
		GraphSchemaSHA256:    canonicalArtifactDigestsV1["graph_schema"],
		ReplaySchemaPath:     "docs/platform/schemas/draft-candidate-fact-replay-v1.schema.json",
		ReplaySchemaSHA256:   canonicalArtifactDigestsV1["replay_schema"],
		RegistryPath:         "docs/platform/schemas/draft-candidate-fact-registry-v1.json",
		RegistrySHA256:       canonicalArtifactDigestsV1["registry"],
		SourceContract:       "helianthus.platform.synchronized-evidence-bundle.v1",
		SourceSchemaVersion:  1,
		SourceOwnerCommit:    sourceOwnerCommitV1,
		SourceSchemaSHA256:   "ed574071fdb11e10d5696c62e873a38c6c6dde64c6069bf616476cea8e8bf737",
		SourceRegistrySHA256: "a91b2106076c3ef0f70578e9fc1c85925dd085af323c5889f809b5b2ef1a2488",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PinnedContractV1() = %#v; want %#v", got, want)
	}
}

func TestMSP07PinnedArtifactsAreExactCanonicalMergeBytes(t *testing.T) {
	artifacts := pinnedTestArtifactsV1()
	assertSHA256(t, "graph_schema", artifacts.GraphSchema, canonicalArtifactDigestsV1["graph_schema"])
	assertSHA256(t, "replay_schema", artifacts.ReplaySchema, canonicalArtifactDigestsV1["replay_schema"])
	assertSHA256(t, "registry", artifacts.Registry, canonicalArtifactDigestsV1["registry"])
	assertSHA256(t, "positive_graph", artifacts.PositiveGraph, canonicalArtifactDigestsV1["positive_graph"])
	assertSHA256(t, "positive_replay", artifacts.PositiveReplay, canonicalArtifactDigestsV1["positive_replay"])
	assertSHA256(t, "source_bundle", artifacts.SourceBundle, canonicalArtifactDigestsV1["source_bundle"])
	assertSHA256(t, "source_replay", artifacts.SourceReplay, canonicalArtifactDigestsV1["source_replay"])

	gotNames := make([]string, 0, len(artifacts.NegativeGraphs))
	for name, raw := range artifacts.NegativeGraphs {
		if len(raw) == 0 {
			t.Fatalf("negative fixture %q is empty", name)
		}
		gotNames = append(gotNames, name)
	}
	wantNames := make([]string, 0, len(negativeCategoriesV1))
	for name := range negativeCategoriesV1 {
		wantNames = append(wantNames, name)
	}
	sort.Strings(gotNames)
	sort.Strings(wantNames)
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("negative fixture inventory = %v; want %v", gotNames, wantNames)
	}
}

func TestMSP07RegistryClosesVocabularyLimitsAndErrorPrecedence(t *testing.T) {
	var registry struct {
		Contract               string            `json:"contract"`
		Version                uint64            `json:"version"`
		Statuses               []string          `json:"statuses"`
		TerminalNegativeStates []string          `json:"terminal_negative_states"`
		CandidateChannel       string            `json:"candidate_channel"`
		ForbiddenSurfaces      []string          `json:"forbidden_surfaces"`
		ValidationPrecedence   []string          `json:"validation_precedence"`
		Limits                 map[string]uint64 `json:"limits"`
	}
	if err := json.Unmarshal(pinnedTestArtifactsV1().Registry, &registry); err != nil {
		t.Fatal(err)
	}
	if registry.Contract != RegistryContractV1 || registry.Version != 1 {
		t.Fatalf("registry identity = %q/v%d", registry.Contract, registry.Version)
	}
	assertStrings(t, "statuses", registry.Statuses, []string{"RAW_ONLY", "CANDIDATE", "CONFLICTED", "WITHHELD"})
	assertStrings(t, "terminal states", registry.TerminalNegativeStates, []string{"NO_SIGNAL", "CLOUD_ONLY", "CONFLICT", "NOT_TESTED"})
	assertStrings(t, "validation precedence", registry.ValidationPrecedence, []string{
		"json.syntax", "schema.graph", "limits.exceeded", "registry.binding", "provenance.binding", "identity.native",
		"ordering.invalid", "state.terminal", "comparator.invalid", "anti_leak.consumer", "hash.fact", "hash.graph",
	})
	if registry.CandidateChannel != "CANDIDATE_DEBUG_REPLAY" {
		t.Fatalf("candidate channel = %q", registry.CandidateChannel)
	}
	wantLimits := map[string]uint64{
		"max_graph_bytes": 1048576, "max_depth": 32, "max_facts": 64,
		"max_evidence_refs_per_fact": 16, "max_samples_per_comparator": 1024,
		"max_string_bytes": 4096, "max_path_segments": 32,
		"max_total_members": 16384, "max_total_list_items": 8192,
	}
	if !reflect.DeepEqual(registry.Limits, wantLimits) {
		t.Fatalf("registry limits = %#v; want %#v", registry.Limits, wantLimits)
	}
	joined := strings.Join(registry.ForbiddenSurfaces, "\n")
	for _, surface := range []string{"EBUS_V1_MCP", "GRAPHQL", "PORTAL", "HOME_ASSISTANT", "COMMAND_ROUTING", "PROMOTED_SEMANTICS", "STABLE_SEMANTIC_REGISTRY"} {
		if !strings.Contains(joined, surface) {
			t.Errorf("forbidden surface %q is not pinned", surface)
		}
	}
}

func assertSHA256(t *testing.T, name string, raw []byte, want string) {
	t.Helper()
	digest := sha256.Sum256(raw)
	if got := hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("%s SHA-256 = %s; want %s", name, got, want)
	}
}

func assertStrings(t *testing.T, name string, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %v; want %v", name, got, want)
	}
}
