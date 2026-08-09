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
)

const (
	syntheticProfileV1 = "SYNTHETIC_CONFORMANCE"
	capturedProfileV1  = "CAPTURED_RUNTIME_ZERO_PROMOTION"
	resultContractV1   = "helianthus.platform.leaf-promotion-lock-result.v1"
)

var executableArtifactsV1 = map[string]string{
	"docs/platform/schemas/leaf-promotion-dossier-v1.schema.json":                                  "ee206ea23d595169d7dec2dd305250a1fd7320a630f89b3b9826b5098e3e1f74",
	"docs/platform/schemas/leaf-promotion-captured-assessment-v1.schema.json":                      "dc2ef02d81d5791ed363f1b18b87874400ab195fcc5463217bef3d165ca19731",
	"docs/platform/schemas/leaf-promotion-lock-result-v1.schema.json":                              "f0da41bc87618bebc2a44b2192e7c7f3b41f75e94108d87e92122a16f5e19a54",
	"docs/platform/schemas/leaf-promotion-registry-v1.json":                                        "a694a897160f3f56cc0221fae7b7999e8dcf0009eeec0d7bbe764d12871c4273",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json":                       "81edb9901737e724370d755de3582d032f0ced9895b0a0d556ea86036095876f",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/result.json":                        "05b63d7d0df412e2376b61b3ec8395a8541c1f21946232d5efc2ec2aa025c850",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/captured-runtime-zero-profile.json": "9b3f2643cb46e45b9b7c890f1ecca27b29942cd91419afb96b30c82912f68cc7",
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
	setInputString(t, &inputs, "Profile", capturedProfileV1)
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

func TestMSP085EmbedsExactMergedExecutableArtifacts(t *testing.T) {
	for path, want := range executableArtifactsV1 {
		raw := contractArtifact(t, path)
		digest := sha256.Sum256(raw)
		if got := hex.EncodeToString(digest[:]); got != want {
			t.Fatalf("%s SHA-256 = %s; want %s", path, got, want)
		}
		raw[0] ^= 0xff
		fresh := contractArtifact(t, path)
		if bytes.Equal(raw, fresh) {
			t.Fatalf("artifact %s aliases embedded storage", path)
		}
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

func TestMSP085CapturedResultDerivesAllEighteenPublicAssessments(t *testing.T) {
	sources, status := capturedAssessmentSources(t)
	result, err := buildManifest(sources)
	if err != nil {
		t.Fatalf("build captured result: %v", err)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	value := decodeObject(t, raw)
	if value["contract"] != resultContractV1 || value["profile"] != capturedProfileV1 ||
		value["export_tier"] != "PUBLIC_REDACTED" || value["verdict"] != "VALID_ZERO_PROMOTION" ||
		value["m9_consumer_gate"] != "BLOCKED_ZERO_PROMOTED_LEAVES" {
		t.Fatalf("unexpected captured closure: %#v", value)
	}
	counts, ok := value["counts"].(map[string]any)
	if !ok || intField(t, counts, "total") != 18 || intField(t, counts, "promoted") != 0 ||
		intField(t, counts, "withheld") != 18 || intField(t, value, "dossier_count") != 0 {
		t.Fatalf("captured counts = %#v, dossier_count = %#v", value["counts"], value["dossier_count"])
	}
	assessments, ok := value["assessments"].([]any)
	if !ok || len(assessments) != 18 {
		t.Fatalf("assessments = %T/%d; want 18", value["assessments"], len(assessments))
	}
	statusByID := make(map[string]m7PublicStatusFactV1, len(status.Facts))
	for _, fact := range status.Facts {
		statusByID[fact.CandidateID] = fact
	}
	statusCounts := map[string]int{}
	for _, rawAssessment := range assessments {
		assessment := rawAssessment.(map[string]any)
		candidateID := assessment["candidate_id"].(string)
		public := statusByID[candidateID]
		if assessment["fact_hash"] != public.FactHash || assessment["source_status"] != public.Status ||
			assessment["terminal_state"] != pointerValue(public.TerminalState) || assessment["decision"] != "WITHHELD" {
			t.Fatalf("assessment %s does not match M7 public status: %#v", candidateID, assessment)
		}
		statusCounts[public.Status]++
		wantKeys := []string{"candidate_id", "decision", "fact_hash", "retest_trigger", "source_status", "terminal_state", "withholding_reasons"}
		if got := sortedMapKeys(assessment); !reflect.DeepEqual(got, wantKeys) {
			t.Fatalf("public assessment keys = %v; want %v", got, wantKeys)
		}
	}
	if statusCounts["RAW_ONLY"] != 14 || statusCounts["WITHHELD"] != 4 {
		t.Fatalf("source statuses = %#v", statusCounts)
	}
	if _, exists := value["dossier_id"]; exists {
		t.Fatal("captured zero-promotion result fabricated a dossier")
	}
	if _, exists := value["leaves"]; exists {
		t.Fatal("captured result reused the synthetic leaf surface")
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
	for index := 1; index <= 18; index++ {
		if strings.Contains(lower, fmt.Sprintf("/private/candidate_%04d", index)) {
			t.Fatalf("public result leaks private semantic path %d", index)
		}
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
		{"m9_open", "consumer.block", func() []byte {
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

func TestMSP085DependencyClosureRemainsCandidateFactsAndCoexistenceOnly(t *testing.T) {
	allowedInternal := map[string]bool{
		"github.com/Project-Helianthus/helianthus-ebusgateway/internal/candidatefacts": true,
		"github.com/Project-Helianthus/helianthus-ebusgateway/internal/coexistence":    true,
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
			bytes.Contains(raw, []byte(capturedProfileV1)) {
			t.Fatalf("promotion lock leaked into stable surface %s", path)
		}
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

func pointerValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
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
