package promotionlock

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/candidatefacts"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
)

const (
	syntheticProfileV1 = "SYNTHETIC_CONFORMANCE"
	declaredProfileV1  = "DECLARED_FROM_SPECIFICATION"
	resultContractV1   = "helianthus.platform.leaf-promotion-lock-result.v1"
)

var executableArtifactsV1 = []string{
	"docs/platform/schemas/leaf-promotion-dossier-v1.schema.json",
	"docs/platform/schemas/leaf-promotion-captured-assessment-v1.schema.json",
	"docs/platform/schemas/leaf-promotion-lock-result-v1.schema.json",
	"docs/platform/schemas/leaf-promotion-registry-v1.json",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/result.json",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/captured-runtime-zero-profile.json",
}

func readTestFile(t *testing.T, parts ...string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(parts...))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func contractArtifact(t *testing.T, path string) []byte {
	t.Helper()
	raw, ok := ContractArtifactV1(path)
	if !ok {
		t.Fatalf("missing embedded contract artifact %s", path)
	}
	return raw
}

func setInputString(t *testing.T, inputs *InputsV1, name, value string) bool {
	t.Helper()
	field := reflect.ValueOf(inputs).Elem().FieldByName(name)
	if !field.IsValid() {
		return false
	}
	if field.Kind() != reflect.String || !field.CanSet() {
		t.Fatalf("InputsV1.%s is not a settable string", name)
	}
	field.SetString(value)
	return true
}

func setInputBytes(t *testing.T, inputs *InputsV1, name string, value []byte) bool {
	t.Helper()
	field := reflect.ValueOf(inputs).Elem().FieldByName(name)
	if !field.IsValid() {
		return false
	}
	if field.Type() != reflect.TypeOf([]byte(nil)) || !field.CanSet() {
		t.Fatalf("InputsV1.%s is not a settable byte slice", name)
	}
	field.SetBytes(bytes.Clone(value))
	return true
}

func syntheticInputs(t *testing.T) InputsV1 {
	t.Helper()
	var inputs InputsV1
	if setInputString(t, &inputs, "Profile", syntheticProfileV1) {
		setInputBytes(t, &inputs, "Registry", contractArtifact(t, "docs/platform/schemas/leaf-promotion-registry-v1.json"))
		setInputBytes(t, &inputs, "Dossier", contractArtifact(t, "docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json"))
		return inputs
	}

	// The pre-M8.5 implementation has no profile selector. Populate its source
	// boundary so the RED assertion observes the obsolete manifest behavior.
	setInputBytes(t, &inputs, "M7Graph", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "positive", "graph.json"))
	setInputBytes(t, &inputs, "M7Replay", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "positive", "replay-result.json"))
	setInputBytes(t, &inputs, "M7Registry", readTestFile(t, "..", "candidatefacts", "contracts", "draft-candidate-fact-registry-v1.json"))
	setInputBytes(t, &inputs, "M7SourceBundle", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "source", "bundle.json"))
	setInputBytes(t, &inputs, "M7SourceReplay", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "source", "replay-result.json"))
	setInputBytes(t, &inputs, "M8Evidence", readTestFile(t, "..", "coexistence", "testdata", "canonical", "positive", "evidence.json"))
	setInputBytes(t, &inputs, "M8Report", readTestFile(t, "..", "coexistence", "testdata", "canonical", "positive", "report.json"))
	setInputBytes(t, &inputs, "M8Registry", readTestFile(t, "..", "coexistence", "contracts", "multi-runtime-coexistence-registry-v1.json"))
	return inputs
}

func capturedInputsWithSyntheticSources(t *testing.T) InputsV1 {
	t.Helper()
	inputs := syntheticInputs(t)
	setInputString(t, &inputs, "Profile", declaredProfileV1)
	setInputBytes(t, &inputs, "Dossier", nil)
	setInputBytes(t, &inputs, "M7Graph", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "positive", "graph.json"))
	setInputBytes(t, &inputs, "M7Replay", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "positive", "replay-result.json"))
	setInputBytes(t, &inputs, "M7Registry", readTestFile(t, "..", "candidatefacts", "contracts", "draft-candidate-fact-registry-v1.json"))
	setInputBytes(t, &inputs, "M7SourceBundle", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "source", "bundle.json"))
	setInputBytes(t, &inputs, "M7SourceReplay", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "source", "replay-result.json"))
	setInputBytes(t, &inputs, "M7LiveStatus", readTestFile(t, "..", "coexistence", "testdata", "canonical", "positive", "live-public-status.json"))
	setInputBytes(t, &inputs, "M7TerminalGraph", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "positive", "source-terminal-graph.json"))
	setInputBytes(t, &inputs, "M7TerminalReplay", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "positive", "source-terminal-replay-result.json"))
	setInputBytes(t, &inputs, "M7TerminalSourceBundle", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "source", "source-terminal-bundle.json"))
	setInputBytes(t, &inputs, "M7TerminalSourceReplay", readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "source", "source-terminal-replay-result.json"))
	setInputBytes(t, &inputs, "M8Evidence", readTestFile(t, "..", "coexistence", "testdata", "canonical", "positive", "evidence.json"))
	setInputBytes(t, &inputs, "M8Report", readTestFile(t, "..", "coexistence", "testdata", "canonical", "positive", "report.json"))
	setInputBytes(t, &inputs, "M8Registry", readTestFile(t, "..", "coexistence", "contracts", "multi-runtime-coexistence-registry-v1.json"))
	return inputs
}

func decodeObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func intField(t *testing.T, object map[string]any, key string) int64 {
	t.Helper()
	number, ok := object[key].(json.Number)
	if !ok {
		t.Fatalf("%s is %T; want integer", key, object[key])
	}
	value, err := number.Int64()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestMSP085EmbedsExactExecutableArtifacts(t *testing.T) {
	if canonicalDocsCommitV1 != "bc3f4f949634bfcbe660984e13a97b11bd135eaf" {
		t.Fatalf("canonical docs commit = %s", canonicalDocsCommitV1)
	}
	if len(artifactSHA256V1) != len(executableArtifactsV1) || len(embeddedPathV1) != len(executableArtifactsV1) {
		t.Fatalf("pinned artifacts = %d, embedded paths = %d; want %d", len(artifactSHA256V1), len(embeddedPathV1), len(executableArtifactsV1))
	}
	for _, path := range executableArtifactsV1 {
		raw := contractArtifact(t, path)
		onDisk := readTestFile(t, filepath.FromSlash(embeddedPathV1[path]))
		digest := sha256.Sum256(onDisk)
		if got := hex.EncodeToString(digest[:]); got != artifactSHA256V1[path] || !bytes.Equal(raw, onDisk) {
			t.Fatalf("%s source SHA-256 = %s; pinned %s", path, got, artifactSHA256V1[path])
		}
		raw[0] ^= 0xff
		fresh := contractArtifact(t, path)
		if bytes.Equal(raw, fresh) {
			t.Fatalf("artifact %s aliases embedded storage", path)
		}
	}
}

func TestMSP085DeclaredAssessmentRecordsSpecificationIdentity(t *testing.T) {
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	sources, _ := capturedAssessmentSources(t)
	assessment, err := buildCapturedAssessment(sources)
	if err != nil {
		t.Fatalf("buildCapturedAssessment: %v", err)
	}
	crossProtocol := 0
	for _, item := range assessment.Assessments {
		candidate, ok := registry.Candidate(item.CandidateID)
		if !ok || candidate.SemanticPath == nil || item.SemanticPath != *candidate.SemanticPath {
			t.Fatalf("assessment %s does not mirror the capture registry", item.CandidateID)
		}
		wantKinds := []string{"EEBUS"}
		if candidate.ProtocolEligibility == promotioncapture.ProtocolCrossProtocol {
			crossProtocol++
			wantKinds = []string{"EBUS", "EEBUS"}
		}
		want := eligibilityV1{
			ExactEBusIdentity: candidate.EBusSelector != nil, ExactEEBusPath: true, CoexistenceNoDrift: true,
		}
		if item.Eligibility != want || item.RetestTrigger.Trigger != "NEW_SYNCHRONIZED_BUNDLE" ||
			!reflect.DeepEqual(item.RetestTrigger.RequiredSourceKinds, wantKinds) || item.RetestTrigger.MinimumNewSamples != 2 {
			t.Fatalf("assessment %s = %#v", item.CandidateID, item)
		}
	}
	if crossProtocol != 11 {
		t.Fatalf("cross-protocol declared leaves = %d, want 11", crossProtocol)
	}

	sources.report.Verdict = "FAIL"
	recorded, err := buildCapturedAssessment(sources)
	if err != nil {
		t.Fatalf("a recorded coexistence finding withdrew the declared leaves: %v", err)
	}
	for _, item := range recorded.Assessments {
		if item.Decision != "PROMOTED" || item.Eligibility.CoexistenceNoDrift {
			t.Fatalf("coexistence finding was not recorded as a level: %#v", item)
		}
	}
}

func TestMSP085DeclaredAssessmentBindsSourceFactHashesAndOrdering(t *testing.T) {
	sources, _ := capturedAssessmentSources(t)
	index := -1
	for position, fact := range sources.graph.Facts {
		if fact.CandidateID == "m7-candidate-0010" {
			index = position
		}
	}
	if index < 0 {
		t.Fatal("source fact m7-candidate-0010 missing")
	}
	mismatched, _ := capturedAssessmentSources(t)
	mismatched.graph.Facts[index].FactHash = "sha256:" + strings.Repeat("0", 64)
	if _, err := buildCapturedAssessment(mismatched); err == nil || err.Error() != "assessment.derivation" {
		t.Fatalf("fact hash substitution error = %v; want assessment.derivation", err)
	}
	duplicated, _ := capturedAssessmentSources(t)
	duplicated.graph.Facts = append(duplicated.graph.Facts, duplicated.graph.Facts[index])
	if _, err := buildCapturedAssessment(duplicated); err == nil || err.Error() != "assessment.ordering" {
		t.Fatalf("duplicate source fact error = %v; want assessment.ordering", err)
	}
	retired, _ := capturedAssessmentSources(t)
	retired.graph.Facts[0].FactHash = "sha256:" + strings.Repeat("0", 64)
	if _, err := buildCapturedAssessment(retired); err != nil {
		t.Fatalf("retired source fact conditioned the declared leaves: %v", err)
	}
}

func TestMSP085DeclaredCountsAndConsumerGateAreClosed(t *testing.T) {
	sources, _ := capturedAssessmentSources(t)
	result, err := buildManifest(sources)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateDeclaredCountsV1(result); err != nil {
		t.Fatalf("declared counts rejected: %v", err)
	}
	if consumerGateV1(result.Counts) != M9ReadyV1 || consumerGateV1(CountsV1{Total: 4, Withheld: 4}) != M9BlockedZeroPromotedLeavesV1 {
		t.Fatal("consumer gate does not follow the promoted count")
	}
	for name, mutate := range map[string]func(*ResultV1){
		"total":    func(value *ResultV1) { value.Counts.Total, value.Counts.Promoted = 17, 17 },
		"promoted": func(value *ResultV1) { value.Counts.Promoted, value.Counts.Withheld = 17, 1 },
		"withheld": func(value *ResultV1) { value.Counts.Withheld = 1 },
		"dossiers": func(value *ResultV1) { value.DossierCount = 1 },
		"verdict":  func(value *ResultV1) { value.Verdict = VerdictValidZeroPromotionV1 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := result
			mutate(&changed)
			if err := validateDeclaredCountsV1(changed); err == nil || err.Error() != "promotion.declared" {
				t.Fatalf("validateDeclaredCountsV1 error = %v; want promotion.declared", err)
			}
		})
	}
}

func TestMSP085CapturedProfileBuildVerifyEndToEnd(t *testing.T) {
	privateDir := os.Getenv("HELIANTHUS_PROMOTIONLOCK_CAPTURED_INPUT_DIR")
	if privateDir == "" {
		t.Skip("process-private captured inputs are not available")
	}
	sourceBundle := readTestFile(t, privateDir, "source-bundle.json")
	sourceReplay := readTestFile(t, privateDir, "source-replay.json")
	graph, replay, err := candidatefacts.BuildRawFirstV1(sourceBundle, sourceReplay)
	if err != nil {
		t.Fatalf("candidatefacts.BuildRawFirstV1: %v", err)
	}
	inputs := InputsV1{
		Profile:                declaredProfileV1,
		Registry:               contractArtifact(t, "docs/platform/schemas/leaf-promotion-registry-v1.json"),
		M7Graph:                graph,
		M7Replay:               replay,
		M7Registry:             readTestFile(t, "..", "candidatefacts", "contracts", "draft-candidate-fact-registry-v1.json"),
		M7SourceBundle:         sourceBundle,
		M7SourceReplay:         sourceReplay,
		M7LiveStatus:           readTestFile(t, "..", "coexistence", "testdata", "canonical", "positive", "live-public-status.json"),
		M7TerminalGraph:        readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "positive", "source-terminal-graph.json"),
		M7TerminalReplay:       readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "positive", "source-terminal-replay-result.json"),
		M7TerminalSourceBundle: readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "source", "source-terminal-bundle.json"),
		M7TerminalSourceReplay: readTestFile(t, "..", "candidatefacts", "testdata", "canonical", "source", "source-terminal-replay-result.json"),
		M8Evidence:             readTestFile(t, privateDir, "m8-evidence.json"),
		M8Report:               readTestFile(t, privateDir, "m8-report.json"),
		M8Registry:             readTestFile(t, "..", "coexistence", "contracts", "multi-runtime-coexistence-registry-v1.json"),
	}
	first, err := Build(inputs)
	if err != nil {
		t.Fatalf("Build(captured): %v", err)
	}
	second, err := Build(inputs)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("captured Build is not byte deterministic: %v", err)
	}
	if err := Verify(first, inputs); err != nil {
		t.Fatalf("Verify(captured): %v", err)
	}
}

func TestMSP085CapturedRuntimeSourcePathsResolveCanonicalArtifacts(t *testing.T) {
	registry := decodeObject(t, contractArtifact(t, "docs/platform/schemas/leaf-promotion-registry-v1.json"))
	sources, ok := registry["captured_runtime_sources"].(map[string]any)
	if !ok {
		t.Fatalf("captured_runtime_sources = %T", registry["captured_runtime_sources"])
	}
	for _, expected := range []struct {
		key, path, digest string
	}{
		{
			key: "m7_terminal_source_bundle", path: "internal/candidatefacts/testdata/canonical/source/source-terminal-bundle.json",
			digest: "b7269e608dfc004f6737fd80b34a1c6018483942632e0c3c8bfd33cadbaa8134",
		},
		{
			key: "m7_terminal_source_replay", path: "internal/candidatefacts/testdata/canonical/source/source-terminal-replay-result.json",
			digest: "82ed1c66222348746abd9f4291e10b968314363160ba4cfc568044390aced9d2",
		},
	} {
		got, ok := sources[expected.key].(string)
		if !ok || got != expected.path {
			t.Errorf("captured_runtime_sources.%s = %q; want %q", expected.key, got, expected.path)
			continue
		}
		raw := readTestFile(t, "..", "..", filepath.FromSlash(got))
		digest := sha256.Sum256(raw)
		if gotDigest := hex.EncodeToString(digest[:]); gotDigest != expected.digest {
			t.Errorf("%s SHA-256 = %s; want %s", got, gotDigest, expected.digest)
		}
	}
}

func TestMSP085EmbeddedRegistryDossierAndResultStayHashAligned(t *testing.T) {
	registryRaw := contractArtifact(t, "docs/platform/schemas/leaf-promotion-registry-v1.json")
	registryDigest := sha256.Sum256(registryRaw)
	wantRegistryDigest := "sha256:" + hex.EncodeToString(registryDigest[:])

	dossier := decodeObject(t, contractArtifact(t, "docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json"))
	dossierRegistry := dossier["registry"].(map[string]any)
	if got := dossierRegistry["digest"]; got != wantRegistryDigest {
		t.Fatalf("dossier registry digest = %v; want %s", got, wantRegistryDigest)
	}
	wantDossierHash := hashObjectWithoutFieldV1("HELIANTHUS:LEAF-PROMOTION-DOSSIER:V1", dossier, "dossier_hash")
	if got := dossier["dossier_hash"]; got != wantDossierHash {
		t.Fatalf("dossier hash = %v; want %s", got, wantDossierHash)
	}

	result := decodeObject(t, contractArtifact(t, "docs/platform/fixtures/leaf-promotion-dossier/v1/positive/result.json"))
	if got := result["dossier_hash"]; got != wantDossierHash {
		t.Fatalf("result dossier hash = %v; want %s", got, wantDossierHash)
	}
	wantResultHash := hashObjectWithoutFieldV1(resultHashDomainV1, result, "result_hash")
	if got := result["result_hash"]; got != wantResultHash {
		t.Fatalf("result hash = %v; want %s", got, wantResultHash)
	}
}

func TestMSP085InputsExposeOnlyTheTwoClosedProfileBoundaries(t *testing.T) {
	typeOfInputs := reflect.TypeOf(InputsV1{})
	got := make([]string, typeOfInputs.NumField())
	for index := range got {
		got[index] = typeOfInputs.Field(index).Name
	}
	want := []string{
		"Profile", "Registry", "Dossier",
		"M7Graph", "M7Replay", "M7Registry", "M7SourceBundle", "M7SourceReplay", "M7LiveStatus",
		"M7TerminalGraph", "M7TerminalReplay", "M7TerminalSourceBundle", "M7TerminalSourceReplay",
		"M8Evidence", "M8Report", "M8Registry",
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("InputsV1 fields = %v; want %v", got, want)
	}
}

func TestMSP085SyntheticConformanceRemainsCanonicalAndNonLive(t *testing.T) {
	inputs := syntheticInputs(t)
	first, err := Build(inputs)
	if err != nil {
		t.Fatalf("Build(synthetic): %v", err)
	}
	second, err := Build(inputs)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("synthetic Build is not byte deterministic: %v", err)
	}
	want := contractArtifact(t, "docs/platform/fixtures/leaf-promotion-dossier/v1/positive/result.json")
	if !bytes.Equal(first, want) {
		t.Fatalf("synthetic result differs from merged fixture\ngot:  %s\nwant: %s", first, want)
	}
	if err := Verify(first, inputs); err != nil {
		t.Fatalf("Verify(synthetic): %v", err)
	}
	result := decodeObject(t, first)
	if result["contract"] != resultContractV1 || result["profile"] != syntheticProfileV1 ||
		result["export_tier"] != "PUBLIC_REDACTED" || result["verdict"] != "VALID_ZERO_PROMOTION" ||
		result["m9_consumer_gate"] != "BLOCKED_ZERO_PROMOTED_LEAVES" {
		t.Fatalf("unexpected synthetic closure: %#v", result)
	}
	counts := result["counts"].(map[string]any)
	if intField(t, counts, "total") != 4 || intField(t, counts, "promoted") != 0 || intField(t, counts, "withheld") != 4 {
		t.Fatalf("synthetic counts = %#v", counts)
	}
	if _, exists := result["source_bindings"]; exists {
		t.Fatal("synthetic conformance result made a live source claim")
	}
}

func TestMSP085CapturedProfileRejectsSyntheticAsLive(t *testing.T) {
	if raw, err := Build(capturedInputsWithSyntheticSources(t)); err == nil || err.Error() != "captured.status" || raw != nil {
		t.Fatalf("Build(synthetic as live) = (%q, %v); want nil, captured.status", raw, err)
	}
}

func TestMSP085DeclaredResultDerivesAllEighteenPublicAssessments(t *testing.T) {
	sources, _ := capturedAssessmentSources(t)
	result, err := buildManifest(sources)
	if err != nil {
		t.Fatalf("build declared result: %v", err)
	}
	value := decodeObject(t, mustJSON(t, result))
	if value["contract"] != resultContractV1 || value["profile"] != declaredProfileV1 || value["export_tier"] != "PUBLIC_REDACTED" {
		t.Fatalf("unexpected declared closure: %#v", value)
	}
	assessments, ok := value["assessments"].([]any)
	if !ok || len(assessments) != 18 {
		t.Fatalf("assessments = %T/%d; want 18", value["assessments"], len(assessments))
	}
	for _, rawAssessment := range assessments {
		assessment := rawAssessment.(map[string]any)
		wantKeys := []string{"candidate_id", "decision", "fact_hash", "retest_trigger", "source_status", "terminal_state", "withholding_reasons"}
		if got := sortedMapKeys(assessment); !reflect.DeepEqual(got, wantKeys) {
			t.Fatalf("public assessment keys = %v; want %v", got, wantKeys)
		}
	}
	if _, exists := value["dossier_id"]; exists {
		t.Fatal("declared result fabricated a dossier")
	}
	if _, exists := value["leaves"]; exists {
		t.Fatal("declared result reused the synthetic leaf surface")
	}
}

func TestMSP085CapturedPublicResultRedactsPrivateIdentity(t *testing.T) {
	sources, _ := capturedAssessmentSources(t)
	result, err := buildManifest(sources)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(raw))
	for _, forbidden := range []string{
		"semantic_path", "proposed_path", "source_address", "target_address", "feature_path",
		"ship_id", "candidate_ref", "private_key", "trust_store", "secret", "completion_token",
	} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("public result leaks %q", forbidden)
		}
	}
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range registry.Candidates() {
		if candidate.SemanticPath != nil && strings.Contains(lower, strings.ToLower(*candidate.SemanticPath)) {
			t.Fatalf("public result leaks semantic path of %s", candidate.CandidateID)
		}
	}
	if !leaksPrivateIdentityV1([]byte(`{"candidate_ref":"private"}`)) {
		t.Fatal("public redaction no longer detects private candidate references")
	}
}

func TestMSP085PrivateAssessmentIsDeterministicAndPromotesDeclaredLeaves(t *testing.T) {
	sources, _ := capturedAssessmentSources(t)
	first, err := buildCapturedAssessment(sources)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildCapturedAssessment(sources)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("private assessment is nondeterministic: %v", err)
	}
	if first.Contract != capturedAssessmentContractV1 || first.ExportTier != exportTierPrivateOperatorV1 ||
		first.Profile != declaredProfileV1 || len(first.Assessments) != 18 || len(first.Dossiers) != 0 ||
		first.M9ConsumerGate != M9ReadyV1 ||
		first.AssessmentHash != hashWithoutFieldV1(capturedAssessmentHashDomainV1, first, "assessment_hash") {
		t.Fatalf("invalid private assessment closure: %#v", first)
	}
	for index, assessment := range first.Assessments {
		if assessment.Decision != "PROMOTED" || assessment.SourceStatus != promotioncapture.SourceStatusSpecificationDeclared ||
			assessment.TerminalState != nil || assessment.WithholdingReasons == nil || len(assessment.WithholdingReasons) != 0 {
			t.Fatalf("declared leaf was not promoted: %#v", assessment)
		}
		if index > 0 && first.Assessments[index-1].SemanticPath >= assessment.SemanticPath {
			t.Fatalf("private assessments are not in semantic-path order at %d", index)
		}
	}
}

func TestMSP085ProfilesRejectCrossBoundaryAndRegistrySubstitution(t *testing.T) {
	synthetic := syntheticInputs(t)
	synthetic.M7Graph = []byte(`{}`)
	if _, err := Build(synthetic); err == nil || err.Error() != "synthetic.input" {
		t.Fatalf("synthetic profile accepted captured input: %v", err)
	}

	for _, profile := range []string{"CAPTURED_RUNTIME_PROMOTION", "CAPTURED_RUNTIME_ZERO_PROMOTION"} {
		unknown := syntheticInputs(t)
		unknown.Profile = profile
		if _, err := Build(unknown); err == nil || err.Error() != "captured.input" {
			t.Fatalf("unknown profile %s error = %v", profile, err)
		}
	}

	tampered := capturedInputsWithSyntheticSources(t)
	tampered.Registry = append(bytes.Clone(tampered.Registry), ' ')
	if _, err := Build(tampered); err == nil || err.Error() != "registry.binding" {
		t.Fatalf("registry substitution error = %v", err)
	}

	replayMismatch := capturedInputsWithSyntheticSources(t)
	replayMismatch.M7Replay = append(bytes.Clone(replayMismatch.M7Replay), ' ')
	if _, err := Build(replayMismatch); err == nil || err.Error() != "projection.replay" {
		t.Fatalf("M7 replay substitution error = %v", err)
	}
}

func TestMSP085CapturedAssessmentOrderingUsesPrivateDerivedOrder(t *testing.T) {
	sources, _ := capturedAssessmentSources(t)
	result, err := buildManifest(sources)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]PublicAssessmentV1(nil), result.Assessments...)
	result.Assessments[0], result.Assessments[1] = result.Assessments[1], result.Assessments[0]
	if !assessmentsOutOfOrder(result.Assessments, want) {
		t.Fatal("permuted public assessments did not preserve private semantic-path ordering")
	}
}

func TestMSP085ResultValidationIsClosedAndPrecedenceStable(t *testing.T) {
	inputs := syntheticInputs(t)
	valid, err := Build(inputs)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		want string
		raw  func() []byte
	}{
		{"duplicate", "json.syntax", func() []byte { return []byte(`{"contract":"a","contract":"b"}`) }},
		{"unknown", "captured.result", func() []byte {
			value := decodeObject(t, valid)
			value["future"] = true
			raw, _ := json.Marshal(value)
			return raw
		}},
		{"m9_open", "consumer.gate", func() []byte {
			value := decodeObject(t, valid)
			value["m9_consumer_gate"] = "READY_FOR_M9"
			raw, _ := json.Marshal(value)
			return raw
		}},
		{"hash", "hash.result", func() []byte {
			value := decodeObject(t, valid)
			value["result_hash"] = "sha256:" + strings.Repeat("f", 64)
			raw, _ := json.Marshal(value)
			return raw
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := Verify(test.raw(), inputs); err == nil || err.Error() != test.want {
				t.Fatalf("Verify() error = %v; want %s", err, test.want)
			}
		})
	}
}

func TestMSP085ResultJSONRejectsAliasesOmittedZeroesAndCaseConflicts(t *testing.T) {
	inputs := syntheticInputs(t)
	valid, err := Build(inputs)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		edit func(map[string]any)
	}{
		{
			name: "top-level alias",
			edit: func(value map[string]any) {
				delete(value, "contract")
				value["Contract"] = resultContractV1
			},
		},
		{
			name: "required zero omitted",
			edit: func(value map[string]any) {
				delete(value["counts"].(map[string]any), "promoted")
			},
		},
		{
			name: "nested alias",
			edit: func(value map[string]any) {
				counts := value["counts"].(map[string]any)
				delete(counts, "promoted")
				counts["Promoted"] = json.Number("0")
			},
		},
		{
			name: "conflicting case keys",
			edit: func(value map[string]any) {
				value["Contract"] = resultContractV1
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			value := decodeObject(t, valid)
			test.edit(value)
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := Verify(raw, inputs); err == nil || err.Error() != "captured.result" {
				t.Fatalf("Verify() error = %v; want captured.result", err)
			}
		})
	}
}

func TestMSP085CapturedJSONRequiresExactNestedShapesAndForbidsFieldsByPresence(t *testing.T) {
	sources, _ := capturedAssessmentSources(t)
	result, err := buildManifest(sources)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		edit func(map[string]any)
	}{
		{
			name: "required top-level zero omitted",
			edit: func(value map[string]any) {
				delete(value, "dossier_count")
			},
		},
		{
			name: "required null omitted",
			edit: func(value map[string]any) {
				for _, rawAssessment := range value["assessments"].([]any) {
					assessment := rawAssessment.(map[string]any)
					if assessment["terminal_state"] == nil {
						delete(assessment, "terminal_state")
						return
					}
				}
				t.Fatal("fixture has no null assessment terminal_state")
			},
		},
		{
			name: "source binding alias",
			edit: func(value map[string]any) {
				bindings := value["source_bindings"].(map[string]any)
				bindings["M8_GATEWAY_SOURCE_COMMIT"] = bindings["m8_gateway_source_commit"]
				delete(bindings, "m8_gateway_source_commit")
			},
		},
		{
			name: "assessment alias",
			edit: func(value map[string]any) {
				assessment := value["assessments"].([]any)[0].(map[string]any)
				assessment["Candidate_ID"] = assessment["candidate_id"]
				delete(assessment, "candidate_id")
			},
		},
		{
			name: "retest alias",
			edit: func(value map[string]any) {
				assessment := value["assessments"].([]any)[0].(map[string]any)
				retest := assessment["retest_trigger"].(map[string]any)
				retest["Minimum_New_Samples"] = retest["minimum_new_samples"]
				delete(retest, "minimum_new_samples")
			},
		},
		{
			name: "empty dossier id forbidden",
			edit: func(value map[string]any) { value["dossier_id"] = "" },
		},
		{
			name: "empty dossier hash forbidden",
			edit: func(value map[string]any) { value["dossier_hash"] = "" },
		},
		{
			name: "null leaves forbidden",
			edit: func(value map[string]any) { value["leaves"] = nil },
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			value := decodeObject(t, raw)
			test.edit(value)
			mutated, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeResultV1(mutated)
			if err == nil {
				err = validateResultShape(decoded, declaredProfileV1)
			}
			if err == nil || err.Error() != "captured.result" {
				t.Fatalf("captured shape error = %v; want captured.result", err)
			}
		})
	}
}

func TestMSP085ResultJSONRequiresIntegerTypesForEveryNumericField(t *testing.T) {
	syntheticRaw, err := Build(syntheticInputs(t))
	if err != nil {
		t.Fatal(err)
	}
	sources, _ := capturedAssessmentSources(t)
	captured, err := buildManifest(sources)
	if err != nil {
		t.Fatal(err)
	}
	capturedRaw := mustJSON(t, captured)
	cases := []struct {
		name string
		raw  []byte
		edit func(map[string]any)
	}{
		{"schema version", syntheticRaw, func(value map[string]any) { value["schema_version"] = nil }},
		{"replay version", syntheticRaw, func(value map[string]any) { value["replay_version"] = nil }},
		{"synthetic dossier count", syntheticRaw, func(value map[string]any) { value["dossier_count"] = nil }},
		{"captured dossier count", capturedRaw, func(value map[string]any) { value["dossier_count"] = nil }},
		{"counts total", syntheticRaw, func(value map[string]any) { value["counts"].(map[string]any)["total"] = nil }},
		{"counts promoted", syntheticRaw, func(value map[string]any) { value["counts"].(map[string]any)["promoted"] = nil }},
		{"counts withheld", syntheticRaw, func(value map[string]any) { value["counts"].(map[string]any)["withheld"] = nil }},
		{"minimum new samples", capturedRaw, func(value map[string]any) {
			assessment := value["assessments"].([]any)[0].(map[string]any)
			assessment["retest_trigger"].(map[string]any)["minimum_new_samples"] = nil
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			value := decodeObject(t, test.raw)
			test.edit(value)
			value["result_hash"] = hashObjectWithoutFieldV1(resultHashDomainV1, value, "result_hash")
			_, err := decodeResultV1(mustJSON(t, value))
			if err == nil || err.Error() != "captured.result" {
				t.Fatalf("numeric null error = %v; want captured.result", err)
			}
		})
	}
}

func TestMSP085ResultJSONRejectsMalformedUTF8BeforeDecode(t *testing.T) {
	raw, err := Build(syntheticInputs(t))
	if err != nil {
		t.Fatal(err)
	}
	invalid := bytes.Clone(raw)
	index := bytes.Index(invalid, []byte("helianthus.platform"))
	if index < 0 {
		t.Fatal("synthetic result has no contract value")
	}
	invalid[index] = 0xff
	if _, err := decodeResultV1(invalid); err == nil || err.Error() != "json.syntax" {
		t.Fatalf("malformed UTF-8 error = %v; want json.syntax", err)
	}
}

func TestMSP085CapturedEvidenceBindsEveryRuntimeToM8SourceCommit(t *testing.T) {
	sources, _ := capturedAssessmentSources(t)
	evidence := decodeObject(t, mustJSON(t, sources.evidence))
	runtime := func(sourceCommit string) map[string]any {
		return map[string]any{"provenance": map[string]any{"runtime": map[string]any{"source_commit": sourceCommit}}}
	}
	evidence["runs"] = []any{runtime(m8GatewaySourceCommitV1), runtime(m8GatewaySourceCommitV1)}
	if err := json.Unmarshal(mustJSON(t, evidence), &sources.evidence); err != nil {
		t.Fatal(err)
	}
	if err := validateM8RuntimeSourceCommits(sources.evidence); err != nil {
		t.Fatalf("exact M8 runtime commits rejected: %v", err)
	}

	// This boundary receives evidence whose coexistence identities were already
	// verified, so only the sibling source commit differs here.
	evidence["runs"].([]any)[1] = runtime("89cf8876a9cd8aa4e6aab9ad21cc05cac523426b")
	sources.evidence = m8EvidenceHeaderV1{}
	if err := json.Unmarshal(mustJSON(t, evidence), &sources.evidence); err != nil {
		t.Fatal(err)
	}
	if err := validateM8RuntimeSourceCommits(sources.evidence); err == nil || err.Error() != "captured.predecessor" {
		t.Fatalf("sibling runtime commit error = %v; want captured.predecessor", err)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestMSP085ProductionPackageHasNoProceduralAuthority(t *testing.T) {
	raw := productionGoSource(t, ".")
	for _, forbidden := range []string{
		"PinnedContractV1", "OwnerTree", "owner_tree", "ActionsRun", "actions_run",
		"completion_token", "CompletionToken", "hosted_run", "HostedRun",
	} {
		if bytes.Contains(raw, []byte(forbidden)) {
			t.Fatalf("production promotionlock retains procedural authority token %q", forbidden)
		}
	}
}

func TestMSP085DependencyClosureRemainsCandidateFactsCoexistenceAndPromotionCapture(t *testing.T) {
	allowedInternal := map[string]bool{
		"github.com/Project-Helianthus/helianthus-ebusgateway/internal/candidatefacts":   true,
		"github.com/Project-Helianthus/helianthus-ebusgateway/internal/coexistence":      true,
		"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture": true,
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range parsed.Imports {
			path, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(path, "/internal/") && !allowedInternal[path] {
				t.Fatalf("%s imports forbidden internal dependency %s", name, path)
			}
		}
	}
}

func TestMSP085DoesNotLeakIntoStableConsumerSurfaces(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, path := range []string{"cmd", "graphql", "mcp", "portal"} {
		raw, err := readPathOrTree(root, path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(resultContractV1)) || bytes.Contains(raw, []byte("internal/promotionlock")) ||
			bytes.Contains(raw, []byte(declaredProfileV1)) {
			t.Fatalf("promotion lock leaked into stable surface %s", path)
		}
	}
}

func TestIssue1694DeclaredProfileCountsEighteenPromotedLeaves(t *testing.T) {
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		t.Fatalf("promotioncapture.DefaultRegistry: %v", err)
	}
	declaredHashes := map[string]string{}
	for _, candidate := range registry.Candidates() {
		if candidate.ProtocolEligibility != promotioncapture.ProtocolTerminal {
			declaredHashes[candidate.CandidateID] = candidate.FactHash
		}
	}
	if len(declaredHashes) != 18 {
		t.Fatalf("declared leaves in capture registry = %d, want 18", len(declaredHashes))
	}

	sources, _ := capturedAssessmentSources(t)
	result, err := buildManifest(sources)
	if err != nil {
		t.Fatalf("buildManifest: %v", err)
	}
	raw := mustJSON(t, result)
	value := decodeObject(t, raw)
	if value["profile"] != "DECLARED_FROM_SPECIFICATION" || value["m9_consumer_gate"] != "READY_FOR_M9" ||
		value["verdict"] != "VALID_PROMOTION_LOCK" || intField(t, value, "dossier_count") != 0 {
		t.Fatalf("declared closure = profile %v gate %v verdict %v dossiers %v",
			value["profile"], value["m9_consumer_gate"], value["verdict"], value["dossier_count"])
	}
	counts := value["counts"].(map[string]any)
	if intField(t, counts, "total") != 18 || intField(t, counts, "promoted") != 18 || intField(t, counts, "withheld") != 0 {
		t.Fatalf("declared counts = %#v", counts)
	}
	assessments, _ := value["assessments"].([]any)
	seen := map[string]bool{}
	for _, rawAssessment := range assessments {
		assessment := rawAssessment.(map[string]any)
		id := assessment["candidate_id"].(string)
		reasons, _ := assessment["withholding_reasons"].([]any)
		if assessment["decision"] != "PROMOTED" || assessment["source_status"] != "SPECIFICATION_DECLARED_PUBLISHED" ||
			assessment["terminal_state"] != nil || reasons == nil || len(reasons) != 0 ||
			assessment["fact_hash"] != declaredHashes[id] || seen[id] {
			t.Fatalf("declared assessment %s = %#v", id, assessment)
		}
		seen[id] = true
	}
	if len(seen) != len(declaredHashes) {
		t.Fatalf("declared assessments = %d, want %d", len(seen), len(declaredHashes))
	}
	decoded, err := decodeResultV1(raw)
	if err != nil {
		t.Fatalf("decodeResultV1: %v", err)
	}
	if err := validateResultShape(decoded, "DECLARED_FROM_SPECIFICATION"); err != nil {
		t.Fatalf("validateResultShape(declared): %v", err)
	}

	lockRegistry := decodeObject(t, contractArtifact(t, "docs/platform/schemas/leaf-promotion-registry-v1.json"))
	profiles := lockRegistry["profiles"].(map[string]any)
	if _, ok := profiles["DECLARED_FROM_SPECIFICATION"]; !ok || len(profiles) != 2 {
		t.Fatalf("lock registry profiles = %#v", profiles)
	}
	if lockRegistry["promoted_visibility"] != "PUBLISHED" || lockRegistry["declared_consumer_gate"] != "READY_FOR_M9" ||
		lockRegistry["zero_promotion_consumer_gate"] != nil ||
		lockRegistry["current_evidence_policy"] != "SPECIFICATION_DECLARES_SUPPORT_CAPTURE_RECORDS_VERIFICATION_LEVEL" {
		t.Fatalf("lock registry declared policy = visibility %v gate %v policy %v",
			lockRegistry["promoted_visibility"], lockRegistry["declared_consumer_gate"], lockRegistry["current_evidence_policy"])
	}
	fixture := decodeObject(t, contractArtifact(t, "docs/platform/fixtures/leaf-promotion-dossier/v1/positive/captured-runtime-zero-profile.json"))
	expected := fixture["expected_counts"].(map[string]any)
	if fixture["profile"] != "DECLARED_FROM_SPECIFICATION" || fixture["m9_consumer_gate"] != "READY_FOR_M9" ||
		intField(t, expected, "total") != 18 || intField(t, expected, "promoted") != 18 || intField(t, expected, "withheld") != 0 {
		t.Fatalf("declared profile fixture = %#v", fixture)
	}
	if fixture["profile"] != value["profile"] || fixture["m9_consumer_gate"] != value["m9_consumer_gate"] ||
		!reflect.DeepEqual(expected, counts) {
		t.Fatalf("declared profile fixture %#v differs from the built result counts %#v", fixture, counts)
	}
}

type m7PublicStatusV1 struct {
	SourceGraphID    string                 `json:"source_graph_id"`
	SourceGraphHash  string                 `json:"source_graph_hash"`
	SourceReplayID   string                 `json:"source_replay_id"`
	SourceReplayHash string                 `json:"source_replay_hash"`
	ProjectionID     string                 `json:"projection_id"`
	ProjectionHash   string                 `json:"projection_hash"`
	Facts            []m7PublicStatusFactV1 `json:"facts"`
}

type m7PublicStatusFactV1 struct {
	CandidateID   string  `json:"candidate_id"`
	Status        string  `json:"status"`
	TerminalState *string `json:"terminal_negative_state"`
	FactHash      string  `json:"fact_hash"`
}

func capturedAssessmentSources(t *testing.T) (verifiedSourcesV1, m7PublicStatusV1) {
	t.Helper()
	statusRaw := readTestFile(t, "..", "coexistence", "testdata", "canonical", "positive", "live-public-status.json")
	var status m7PublicStatusV1
	if err := json.Unmarshal(statusRaw, &status); err != nil {
		t.Fatal(err)
	}
	facts := make([]candidatefacts.FactV1, len(status.Facts))
	for index, public := range status.Facts {
		facts[index] = candidatefacts.FactV1{
			CandidateID:           public.CandidateID,
			ProposedPath:          fmt.Sprintf("/private/candidate_%04d", index+1),
			Status:                public.Status,
			TerminalNegativeState: public.TerminalState,
			Comparator:            candidatefacts.ComparatorEvaluationV1{Outcome: "NOT_TESTED"},
			RetestTrigger: candidatefacts.RetestTriggerV1{
				TriggerCode: "SOURCE_RECOVERED", RequiredSourceKinds: []string{"EBUS", "EEBUS"}, MinimumNewSamples: 1,
			},
			DebugOnly: true,
			FactHash:  public.FactHash,
		}
	}
	sources := verifiedSourcesV1{
		graph: candidatefacts.GraphV1{
			Contract: candidatefacts.ContractV1, SchemaVersion: 1,
			GraphID: status.SourceGraphID, GraphHash: status.SourceGraphHash, Facts: facts,
		},
		replay: m7ReplayHeaderV1{
			Contract: candidatefacts.ReplayContractV1, GraphID: status.SourceGraphID, GraphHash: status.SourceGraphHash,
			ReplayID: status.SourceReplayID, ReplayHash: status.SourceReplayHash,
		},
		evidence: m8EvidenceHeaderV1{
			Contract:   "helianthus.platform.multi-runtime-coexistence-evidence.v1",
			EvidenceID: "mrcv1:sha256:" + strings.Repeat("a", 64), EvidenceHash: "sha256:" + strings.Repeat("a", 64),
			EvidenceClass: "CAPTURED_RUNTIME_EVIDENCE",
		},
		report: m8ReportHeaderV1{
			Contract:   "helianthus.platform.multi-runtime-coexistence-report.v1",
			EvidenceID: "mrcv1:sha256:" + strings.Repeat("a", 64), EvidenceHash: "sha256:" + strings.Repeat("a", 64),
			ReportID: "mrcrv1:sha256:" + strings.Repeat("b", 64), ReportHash: "sha256:" + strings.Repeat("b", 64), Verdict: "PASS",
		},
	}
	return sources, status
}

func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func productionGoSource(t *testing.T, root string) []byte {
	t.Helper()
	var combined bytes.Buffer
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		combined.Write(raw)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return combined.Bytes()
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

// The source check is the gate Build applies to the declared profile; the
// default run pins it directly, without the process-private captured inputs.
func TestIssue1694SourceCheckAdmitsOnlyTheDeclaredProfile(t *testing.T) {
	declared := capturedInputsWithSyntheticSources(t)
	if _, err := validateCapturedSources(declared); err == nil || err.Error() != "captured.status" {
		t.Fatalf("validateCapturedSources(declared) error = %v; want the profile admitted and captured.status from the synthetic sources", err)
	}
	if _, err := Build(declared); err == nil || err.Error() != "captured.status" {
		t.Fatalf("Build(declared) error = %v; want the profile admitted and captured.status from the synthetic sources", err)
	}

	for _, profile := range []string{syntheticProfileV1, "CAPTURED_RUNTIME_ZERO_PROMOTION", "CAPTURED_RUNTIME_PROMOTION", ""} {
		other := capturedInputsWithSyntheticSources(t)
		other.Profile = profile
		if _, err := validateCapturedSources(other); err == nil || err.Error() != "captured.input" {
			t.Fatalf("validateCapturedSources(profile %q) error = %v; want captured.input", profile, err)
		}
	}

	withDossier := capturedInputsWithSyntheticSources(t)
	withDossier.Dossier = contractArtifact(t, "docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json")
	if _, err := validateCapturedSources(withDossier); err == nil || err.Error() != "captured.input" {
		t.Fatalf("validateCapturedSources(declared with dossier) error = %v; want captured.input", err)
	}

	incomplete := capturedInputsWithSyntheticSources(t)
	incomplete.M8Report = nil
	if _, err := validateCapturedSources(incomplete); err == nil || err.Error() != "captured.input" {
		t.Fatalf("validateCapturedSources(declared without M8 report) error = %v; want captured.input", err)
	}
}
