package coexistence

import (
	"bytes"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestMSP08VerificationAndGenerationAreDeterministicAndDoNotMutateInputs(t *testing.T) {
	inputs := canonicalVerificationInputs(t)
	snapshot := cloneInputsV1(inputs)
	if err := Verify(inputs); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	first, err := Report(inputs)
	if err != nil {
		t.Fatalf("Report() error = %v", err)
	}
	second, err := Report(inputs)
	if err != nil {
		t.Fatalf("Report() second error = %v", err)
	}
	if !bytes.Equal(first, second) || !reflect.DeepEqual(inputs, snapshot) {
		t.Fatal("verification/report is nondeterministic or mutated caller input")
	}

	generation := canonicalGenerationInputs(t)
	generationSnapshot := cloneGenerateInputsV1(generation)
	generated, err := Generate(generation)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	again, err := Generate(generation)
	if err != nil {
		t.Fatalf("Generate() second error = %v", err)
	}
	if !bytes.Equal(generated.Evidence, again.Evidence) || !bytes.Equal(generated.Report, again.Report) || !reflect.DeepEqual(generation, generationSnapshot) {
		t.Fatal("generation is nondeterministic or mutated caller input")
	}
}

func TestMSP08M7InputsAreValidatedNotCallerAttributed(t *testing.T) {
	for _, field := range []string{"graph", "replay", "registry", "source bundle", "source replay"} {
		field := field
		t.Run(field, func(t *testing.T) {
			inputs := canonicalVerificationInputs(t)
			var target *[]byte
			switch field {
			case "graph":
				target = &inputs.M7Graph
			case "replay":
				target = &inputs.M7Replay
			case "registry":
				target = &inputs.M7Registry
			case "source bundle":
				target = &inputs.M7SourceBundle
			case "source replay":
				target = &inputs.M7SourceReplay
			}
			altered := bytes.Clone(*target)
			altered[len(altered)/2] ^= 1
			*target = altered
			assertCategory(t, Verify(inputs), "provenance.m7")
		})
	}
}

func TestMSP08ClosedJSONOrderingAndLimitsFailClosed(t *testing.T) {
	canonical := canonicalVerificationInputs(t)
	for _, test := range []struct {
		name string
		raw  []byte
		want string
	}{
		{name: "malformed UTF-8", raw: []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}, want: "json.syntax"},
		{name: "duplicate root key", raw: append([]byte("{\"contract\":\""+evidenceContractV1+"\","), canonical.Evidence[1:]...), want: "json.syntax"},
		{name: "fraction", raw: bytes.Replace(canonical.Evidence, []byte("\"schema_version\": 1"), []byte("\"schema_version\": 1.5"), 1), want: "json.syntax"},
		{name: "unsafe integer", raw: bytes.Replace(canonical.Evidence, []byte("\"schema_version\": 1"), []byte("\"schema_version\": 9007199254740992"), 1), want: "json.syntax"},
		{name: "byte ceiling", raw: bytes.Repeat([]byte(" "), int(limitsV1["max_evidence_bytes"])+1), want: "limits.exceeded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			inputs := canonical
			inputs.Evidence = test.raw
			assertCategory(t, Verify(inputs), test.want)
		})
	}

	for _, mutation := range []struct {
		name string
		edit func(map[string]any)
	}{
		{name: "unknown root field", edit: func(e map[string]any) { e["unknown"] = true }},
		{name: "reordered states", edit: func(e map[string]any) { runs := e["runs"].([]any); runs[1], runs[2] = runs[2], runs[1] }},
		{name: "reordered views", edit: func(e map[string]any) {
			views := e["runs"].([]any)[0].(map[string]any)["protected_views"].([]any)
			views[0], views[1] = views[1], views[0]
		}},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			evidence := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
			mutation.edit(evidence)
			inputs := canonical
			inputs.Evidence = canonicalJSON(t, evidence)
			want := "ordering.duplicate"
			if strings.Contains(mutation.name, "unknown") {
				want = "schema.evidence"
			}
			assertCategory(t, Verify(inputs), want)
		})
	}
}

func TestMSP08EveryProtectedViewRejectsCandidateLeakAndConsumerDrift(t *testing.T) {
	for _, viewID := range protectedViewsV1 {
		viewID := viewID
		t.Run(viewID, func(t *testing.T) {
			for _, leak := range []struct {
				candidate bool
				want      string
			}{
				{candidate: true, want: "anti_leak.candidate"},
				{want: "drift.consumer"},
			} {
				evidence := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
				run := findRun(t, evidence, "EEBUS_CONNECTED_CANDIDATE_ONLY")
				view := findView(t, run, viewID)
				data := objectValue(t, objectValue(t, view, "payload"), "data")
				if leak.candidate {
					data["candidate_status"] = "CANDIDATE"
				} else if viewID == "mcp.eebus.v1.contract" {
					data["schema_digest"] = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
				} else {
					data["neutral_observation"] = "unchanged-shape-forbidden"
				}
				refreshViewHashes(t, evidence, run, view)
				inputs := canonicalVerificationInputs(t)
				inputs.Evidence = canonicalJSON(t, evidence)
				assertCategory(t, Verify(inputs), leak.want)
			}
		})
	}
}

func TestMSP08PublicStatusProjectionIsDeterministicAndMetadataClosed(t *testing.T) {
	want := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/live-public-status.json"))
	graph := map[string]any{
		"graph_id":   want["source_graph_id"],
		"graph_hash": want["source_graph_hash"],
		"facts":      cloneValue(t, want["facts"]),
	}
	replay := map[string]any{
		"replay_id":   want["source_replay_id"],
		"replay_hash": want["source_replay_hash"],
	}
	first, err := projectM7PublicStatus(graph, replay, coexLiveGatewayCommit, coexLiveDocsCommit)
	if err != nil {
		t.Fatalf("projectM7PublicStatus() error = %v", err)
	}
	second, err := projectM7PublicStatus(graph, replay, coexLiveGatewayCommit, coexLiveDocsCommit)
	if err != nil || !reflect.DeepEqual(first, second) || !reflect.DeepEqual(first, want) {
		t.Fatal("public status projection is nondeterministic or differs from canonical bytes")
	}
	for _, rawFact := range first["facts"].([]any) {
		fact := rawFact.(map[string]any)
		if !exactKeys(fact, "candidate_id", "status", "terminal_negative_state", "fact_hash") {
			t.Fatalf("public status fact leaks metadata: %v", sortedKeys(fact))
		}
	}
}

func canonicalVerificationInputs(t *testing.T) InputsV1 {
	t.Helper()
	repo := repoDir(t)
	return InputsV1{
		Evidence:       readFile(t, filepath.Join(repo, "internal/coexistence/testdata/canonical/positive/evidence.json")),
		Registry:       readFile(t, filepath.Join(repo, "internal/coexistence/contracts/multi-runtime-coexistence-registry-v1.json")),
		M7Graph:        readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/graph.json")),
		M7Replay:       readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/replay-result.json")),
		M7Registry:     readFile(t, filepath.Join(repo, "internal/candidatefacts/contracts/draft-candidate-fact-registry-v1.json")),
		M7SourceBundle: readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/source/bundle.json")),
		M7SourceReplay: readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/source/replay-result.json")),
	}
}

func canonicalGenerationInputs(t *testing.T) GenerateInputsV1 {
	t.Helper()
	evidence := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
	runs := arrayValue(t, evidence, "runs")
	var timestamps, subjects []any
	for _, rawRun := range runs {
		view := asObject(t, arrayValue(t, asObject(t, rawRun, "run"), "protected_views")[0], "view")
		meta := objectValue(t, objectValue(t, view, "payload"), "meta")
		timestamps = append(timestamps, stringValue(t, meta, "captured_at"))
		subjects = append(subjects, stringValue(t, meta, "auth_subject"))
	}
	inputs := canonicalVerificationInputs(t)
	return GenerateInputsV1{
		Registry: inputs.Registry, M7Graph: inputs.M7Graph, M7Replay: inputs.M7Replay,
		M7Registry: inputs.M7Registry, M7SourceBundle: inputs.M7SourceBundle, M7SourceReplay: inputs.M7SourceReplay,
		BaselineRuntime:   canonicalJSON(t, objectValue(t, objectValue(t, asObject(t, runs[0], "run"), "provenance"), "runtime")),
		ComparedRuntime:   canonicalJSON(t, objectValue(t, objectValue(t, asObject(t, runs[1], "run"), "provenance"), "runtime")),
		CaptureClock:      canonicalJSON(t, objectValue(t, evidence, "capture_clock")),
		CaptureTimestamps: canonicalJSON(t, timestamps), MaskedSubjects: canonicalJSON(t, subjects),
	}
}

func cloneInputsV1(input InputsV1) InputsV1 {
	return InputsV1{
		Evidence: bytes.Clone(input.Evidence), Registry: bytes.Clone(input.Registry),
		M7Graph: bytes.Clone(input.M7Graph), M7Replay: bytes.Clone(input.M7Replay), M7Registry: bytes.Clone(input.M7Registry),
		M7SourceBundle: bytes.Clone(input.M7SourceBundle), M7SourceReplay: bytes.Clone(input.M7SourceReplay),
		M7LiveStatus: bytes.Clone(input.M7LiveStatus), M7TerminalGraph: bytes.Clone(input.M7TerminalGraph), M7TerminalReplay: bytes.Clone(input.M7TerminalReplay),
		M7TerminalSourceBundle: bytes.Clone(input.M7TerminalSourceBundle), M7TerminalSourceReplay: bytes.Clone(input.M7TerminalSourceReplay),
	}
}

func cloneGenerateInputsV1(input GenerateInputsV1) GenerateInputsV1 {
	return GenerateInputsV1{
		Registry: bytes.Clone(input.Registry), M7Graph: bytes.Clone(input.M7Graph), M7Replay: bytes.Clone(input.M7Replay),
		M7Registry: bytes.Clone(input.M7Registry), M7SourceBundle: bytes.Clone(input.M7SourceBundle), M7SourceReplay: bytes.Clone(input.M7SourceReplay),
		BaselineRuntime: bytes.Clone(input.BaselineRuntime), ComparedRuntime: bytes.Clone(input.ComparedRuntime), CaptureClock: bytes.Clone(input.CaptureClock),
		CaptureTimestamps: bytes.Clone(input.CaptureTimestamps), MaskedSubjects: bytes.Clone(input.MaskedSubjects),
	}
}

func assertCategory(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v; want %s", err, want)
	}
}
