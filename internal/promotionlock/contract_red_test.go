package promotionlock

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const (
	docsOwnerCommit = "e8614eed91b424b81c414c3cfad596b7c1e8402f"
	docsOwnerTree   = "24794312f89defcbed5cb9654e8539f37c1aa1df"
)

func readTestFile(t *testing.T, parts ...string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(parts...))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func canonicalInputs(t *testing.T) InputsV1 {
	t.Helper()
	return InputsV1{
		M7Graph:        readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "positive", "graph.json"),
		M7Replay:       readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "positive", "replay-result.json"),
		M7Registry:     readTestFile(t, "..", "candidatefacts", "contracts", "draft-candidate-fact-registry-v1.json"),
		M7SourceBundle: readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "source", "bundle.json"),
		M7SourceReplay: readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "source", "replay-result.json"),
		M8Evidence:     readTestFile(t, "..", "coexistence", "testdata", "canonical", "positive", "evidence.json"),
		M8Report:       readTestFile(t, "..", "coexistence", "testdata", "canonical", "positive", "report.json"),
		M8Registry:     readTestFile(t, "..", "coexistence", "contracts", "multi-runtime-coexistence-registry-v1.json"),
	}
}

func buildCanonical(t *testing.T) []byte {
	t.Helper()
	raw, err := Build(canonicalInputs(t))
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return raw
}

func decodeManifest(t *testing.T, raw []byte) ManifestV1 {
	t.Helper()
	var manifest ManifestV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestMSP085PinsExactDocsOwnerAndExecutableArtifacts(t *testing.T) {
	binding := PinnedContractV1()
	if binding.OwnerRepository != "Project-Helianthus/helianthus-docs-ebus" ||
		binding.OwnerCommit != docsOwnerCommit || binding.OwnerTree != docsOwnerTree ||
		binding.OwnerExactHeadActionsRun != 30135202717 ||
		binding.OwnerPostMainActionsRun != 30135494435 {
		t.Fatalf("unexpected owner binding: %#v", binding)
	}
	want := map[string]string{
		"docs/platform/schemas/leaf-promotion-dossier-v1.schema.json":            "",
		"docs/platform/schemas/leaf-promotion-lock-result-v1.schema.json":        "",
		"docs/platform/schemas/leaf-promotion-registry-v1.json":                  "",
		"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json": "",
		"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/result.json":  "",
	}
	if !reflect.DeepEqual(sortedKeys(binding.ArtifactSHA256), sortedKeys(want)) {
		t.Fatalf("artifact inventory = %v", sortedKeys(binding.ArtifactSHA256))
	}
	for path, digest := range binding.ArtifactSHA256 {
		if len(digest) != 64 {
			t.Fatalf("%s digest = %q", path, digest)
		}
		raw, ok := ContractArtifactV1(path)
		if !ok || len(raw) == 0 {
			t.Fatalf("missing embedded owner artifact %s", path)
		}
		raw[0] ^= 0xff
		fresh, _ := ContractArtifactV1(path)
		if bytes.Equal(raw, fresh) {
			t.Fatalf("artifact %s aliases internal storage", path)
		}
	}
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestMSP085BuildsDeterministicZeroPromotionClosure(t *testing.T) {
	first := buildCanonical(t)
	second := buildCanonical(t)
	if !bytes.Equal(first, second) {
		t.Fatal("Build() is not byte deterministic")
	}
	if err := Verify(first, canonicalInputs(t)); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	manifest := decodeManifest(t, first)
	if manifest.Contract != ContractV1 || manifest.SchemaVersion != 1 ||
		manifest.Verdict != "VALID_ZERO_PROMOTION" ||
		manifest.PromotionState != "LOCKED_ZERO_PROMOTION" ||
		manifest.M9ConsumerGate != "BLOCKED_ZERO_PROMOTED_LEAVES" ||
		manifest.StableSurfaceChanges {
		t.Fatalf("unexpected closure: %#v", manifest)
	}
	if manifest.Counts != (CountsV1{Candidates: 7, Dossiers: 0, Promoted: 0, Withheld: 7}) {
		t.Fatalf("counts = %#v", manifest.Counts)
	}
	if len(manifest.Assessments) != 7 || manifest.ManifestHash == "" ||
		manifest.ManifestID != "lplmv1:"+manifest.ManifestHash {
		t.Fatalf("invalid manifest identity: %#v", manifest)
	}
}

func TestMSP085AssessmentsWithholdEveryCandidateWithoutInventingIdentity(t *testing.T) {
	manifest := decodeManifest(t, buildCanonical(t))
	wantReasons := map[string]string{
		"m7-candidate-0001": "TERMINAL_NEGATIVE_STATE",
		"m7-candidate-0002": "MISSING_EEBUS_ENTITY_FEATURE_PATH",
		"m7-candidate-0003": "MISSING_EEBUS_ENTITY_FEATURE_PATH",
		"m7-candidate-0004": "TERMINAL_NEGATIVE_STATE",
		"m7-candidate-0005": "TERMINAL_NEGATIVE_STATE",
		"m7-candidate-0006": "TERMINAL_NEGATIVE_STATE",
		"m7-candidate-0007": "MISSING_EEBUS_ENTITY_FEATURE_PATH",
	}
	paths := make([]string, 0, len(manifest.Assessments))
	for _, assessment := range manifest.Assessments {
		paths = append(paths, assessment.SemanticPath)
		if assessment.Decision != "WITHHELD" || assessment.Visibility != "RAW_DEBUG_ONLY" ||
			assessment.DossierState != "NOT_CREATED" || assessment.ExactEEBusIdentity {
			t.Fatalf("candidate escaped withheld boundary: %#v", assessment)
		}
		if assessment.ReasonCode != wantReasons[assessment.CandidateID] {
			t.Fatalf("%s reason = %s", assessment.CandidateID, assessment.ReasonCode)
		}
	}
	if !sort.StringsAreSorted(paths) {
		t.Fatalf("assessment paths not stable: %v", paths)
	}
}

func TestMSP085BindsExactM7AndM8ArtifactsAndSyntheticBoundary(t *testing.T) {
	manifest := decodeManifest(t, buildCanonical(t))
	source := manifest.SourceBindings
	if source.M7GraphID != "dcfgv1:sha256:00f2b3c48959605d311d0d3895ec924b475d8fa25ee4e236d32d6facbd32c4ac" ||
		source.M7ReplayID != "dcfrv1:sha256:0d3d6c1b4d23e1a8dfe6137fd7956f2c0c3fa51009c1ebb9129807c9fd49850b" ||
		source.M8EvidenceID != "mrcv1:sha256:9055d83a83042c70131769e7d6f33f6eabe1665532634299d0fbdc65c58b6218" ||
		source.M8ReportID != "mrcrv1:sha256:e87f8e135041b6894be4c0e3ccca9d16a34923ee9ec78cbf3fb614974e05b38b" ||
		source.EvidenceClass != "SYNTHETIC_OFFLINE_FIXTURE" || source.LiveVR940Claim {
		t.Fatalf("source binding = %#v", source)
	}
	if manifest.PromotedPaths == nil || len(manifest.PromotedPaths) != 0 ||
		manifest.LockedDossierIDs == nil || len(manifest.LockedDossierIDs) != 0 {
		t.Fatalf("zero lists must be explicit empty arrays: %#v", manifest)
	}
}

func TestMSP085RejectsEveryTamperedSourceAncestor(t *testing.T) {
	cases := []struct {
		name string
		want string
		edit func(*InputsV1)
	}{
		{"m7_graph", "source.m7_graph", func(in *InputsV1) { in.M7Graph = append(in.M7Graph, ' ') }},
		{"m7_replay", "source.m7_replay", func(in *InputsV1) { in.M7Replay = append(in.M7Replay, ' ') }},
		{"m7_registry", "source.m7_registry", func(in *InputsV1) { in.M7Registry = append(in.M7Registry, ' ') }},
		{"m7_source_bundle", "source.m7_bundle", func(in *InputsV1) { in.M7SourceBundle = append(in.M7SourceBundle, ' ') }},
		{"m7_source_replay", "source.m7_bundle", func(in *InputsV1) { in.M7SourceReplay = append(in.M7SourceReplay, ' ') }},
		{"m8_evidence", "source.m8_evidence", func(in *InputsV1) { in.M8Evidence = append(in.M8Evidence, ' ') }},
		{"m8_report", "source.m8_report", func(in *InputsV1) { in.M8Report = append(in.M8Report, ' ') }},
		{"m8_registry", "source.m8_registry", func(in *InputsV1) { in.M8Registry = append(in.M8Registry, ' ') }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			inputs := canonicalInputs(t)
			test.edit(&inputs)
			if _, err := Build(inputs); err == nil || err.Error() != test.want {
				t.Fatalf("Build() error = %v, want %s", err, test.want)
			}
		})
	}
}

func TestMSP085ManifestValidationIsFailClosedAndPrecedenceStable(t *testing.T) {
	valid := buildCanonical(t)
	cases := []struct {
		name string
		want string
		edit func(map[string]any)
	}{
		{"unknown", "schema.manifest", func(v map[string]any) { v["future"] = true }},
		{"m9_open", "consumer.block", func(v map[string]any) { v["m9_consumer_gate"] = "READY_FOR_M9" }},
		{"promoted_count", "promotion.forbidden", func(v map[string]any) { v["counts"].(map[string]any)["promoted"] = float64(1) }},
		{"stable_surface", "anti_leak.stable_surface", func(v map[string]any) { v["stable_surface_changes"] = true }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var value map[string]any
			if err := json.Unmarshal(valid, &value); err != nil {
				t.Fatal(err)
			}
			test.edit(value)
			raw, _ := json.Marshal(value)
			if err := Verify(raw, canonicalInputs(t)); err == nil || err.Error() != test.want {
				t.Fatalf("Verify() error = %v, want %s", err, test.want)
			}
		})
	}
}

func TestMSP085RejectsHashMismatchDuplicateKeysAndTrailingData(t *testing.T) {
	valid := buildCanonical(t)
	manifest := decodeManifest(t, valid)
	manifest.ManifestHash = "sha256:" + strings.Repeat("f", 64)
	raw, _ := json.Marshal(manifest)
	if err := Verify(raw, canonicalInputs(t)); err == nil || err.Error() != "hash.manifest" {
		t.Fatalf("hash error = %v", err)
	}
	for name, malformed := range map[string][]byte{
		"duplicate": []byte(`{"contract":"a","contract":"b"}`),
		"trailing":  append(append([]byte(nil), valid...), []byte(`{}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			if err := Verify(malformed, canonicalInputs(t)); err == nil || err.Error() != "json.syntax" {
				t.Fatalf("Verify() error = %v", err)
			}
		})
	}
}

func TestMSP085DoesNotLeakToExistingStableSurfaces(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, path := range []string{
		"cmd/gateway",
		"graphql",
		"mcp",
		"portal",
	} {
		raw, err := readPathOrTree(root, path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(ContractV1)) || bytes.Contains(raw, []byte("LOCKED_ZERO_PROMOTION")) {
			t.Fatalf("promotion lock leaked into stable surface %s", path)
		}
	}
}

func readPathOrTree(root, relative string) ([]byte, error) {
	path := filepath.Join(root, relative)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if !info.IsDir() {
		return os.ReadFile(path)
	}
	var combined bytes.Buffer
	err = filepath.WalkDir(path, func(name string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(name, ".go") {
			return nil
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		combined.Write(raw)
		return nil
	})
	return combined.Bytes(), err
}
