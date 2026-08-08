package candidatefacts

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"strings"
	"testing"
)

func TestCorrectiveContractPinAndCanonicalGraphAreCurrent(t *testing.T) {
	if got := PinnedContractV1().OwnerCommit; got != "35d2eba256a77b6575a2b45c07e73f054ff74ced" {
		t.Fatalf("OwnerCommit = %q; want corrective docs merge", got)
	}
	artifacts := pinnedTestArtifactsV1()
	graph, err := decodeGraphV1(artifacts.PositiveGraph)
	if err != nil {
		t.Fatalf("decodeGraphV1(positive): %v", err)
	}
	for _, fact := range graph.Facts {
		if len(fact.Comparator.Samples) != 0 || fact.Comparator.Outcome != "NOT_EVALUATED" {
			t.Fatalf("canonical MSP-065 fact %q is evaluated", fact.CandidateID)
		}
		if fact.Status == "CANDIDATE" || fact.Status == "CONFLICTED" {
			t.Fatalf("canonical MSP-065 fact %q has unsupported status %q", fact.CandidateID, fact.Status)
		}
	}
}

func TestCorrectiveEvaluatorIsExactAndRejectsCallerControl(t *testing.T) {
	parameters := correctiveParameters()
	left := correctiveArtifact("EBUS", "left", "1", "10", "degC", 2_000_000_000)
	right := correctiveArtifact("EEBUS", "right", "2", "10.3", "degC", 2_000_000_000)
	samples := []map[string]any{correctiveSample(left, right, 3_000_000_000, "PRESENT")}
	parameters["minimum_samples"] = number(1)
	parameters["tolerance"] = map[string]any{"absolute_decimal": "0.197", "relative_ppm": number(10000)}
	parameters["conflict_threshold"] = map[string]any{"absolute_decimal": "10", "consecutive_samples": number(2)}

	outcome, final, err := evaluateNumericWindow(parameters, samples, correctiveArtifactIndex(left, right), nil)
	if err != nil {
		t.Fatalf("evaluate exact boundary: %v", err)
	}
	if outcome != "MATCH" || final != "10.3" {
		t.Fatalf("evaluate exact boundary = (%q, %q); want MATCH, 10.3", outcome, final)
	}

	duplicate := append(samples, correctiveSample(left, right, 3_000_000_000, "PRESENT"))
	if _, _, err := evaluateNumericWindow(parameters, duplicate, correctiveArtifactIndex(left, right), nil); err == nil || err.Error() != "ordering.invalid" {
		t.Fatalf("duplicate sample error = %v; want ordering.invalid", err)
	}
}

func TestCorrectiveEvaluatorAffineHalfEvenConflictAndAvailability(t *testing.T) {
	left := correctiveArtifact("EBUS", "left", "1", "2.25", "source", 2_000_000_000)
	right := correctiveArtifact("EEBUS", "right", "2", "5.4", "target", 2_000_000_000)
	parameters := correctiveParameters()
	parameters["minimum_samples"] = number(1)
	parameters["tolerance"] = map[string]any{"absolute_decimal": "0", "relative_ppm": number(0)}
	parameters["unit_conversion"] = map[string]any{
		"mode": "AFFINE", "source_unit": "source", "target_unit": "target",
		"scale_decimal": "2", "offset_decimal": "0.9",
	}
	parameters["rounding"] = map[string]any{"mode": "HALF_EVEN", "decimal_places": number(0)}
	parameters["conflict_threshold"] = map[string]any{"absolute_decimal": "10", "consecutive_samples": number(2)}
	outcome, final, err := evaluateNumericWindow(parameters, []map[string]any{
		correctiveSample(left, right, 3_000_000_000, "PRESENT"),
	}, correctiveArtifactIndex(left, right), nil)
	if err != nil || outcome != "MATCH" || final != "5" {
		t.Fatalf("affine half-even = (%q, %q, %v); want MATCH, 5", outcome, final, err)
	}

	conflictRight := correctiveArtifact("EEBUS", "conflict", "3", "6.4", "target", 2_000_000_000)
	parameters["minimum_samples"] = number(2)
	parameters["conflict_threshold"] = map[string]any{"absolute_decimal": "1", "consecutive_samples": number(2)}
	outcome, _, err = evaluateNumericWindow(parameters, []map[string]any{
		correctiveSample(left, conflictRight, 3_000_000_000, "PRESENT"),
		correctiveSample(left, conflictRight, 4_000_000_000, "PRESENT"),
	}, correctiveArtifactIndex(left, conflictRight), nil)
	if err != nil || outcome != "CONFLICT" {
		t.Fatalf("inclusive consecutive conflict = (%q, %v); want CONFLICT", outcome, err)
	}

	stale := correctiveSample(left, right, 4_000_000_001, "STALE")
	parameters["minimum_samples"] = number(1)
	parameters["maximum_missing_samples"] = number(0)
	outcome, _, err = evaluateNumericWindow(parameters, []map[string]any{stale}, correctiveArtifactIndex(left, right), nil)
	if err != nil || outcome != "INDETERMINATE" {
		t.Fatalf("stale availability = (%q, %v); want INDETERMINATE", outcome, err)
	}
}

func TestCorrectiveHalfEvenNormalizesRoundedNegativeZero(t *testing.T) {
	left := correctiveArtifact("EBUS", "left-zero", "7", "-0.04", "degC", 2_000_000_000)
	right := correctiveArtifact("EEBUS", "right-zero", "8", "-0.04", "degC", 2_000_000_000)
	parameters := correctiveParameters()
	parameters["tolerance"] = map[string]any{"absolute_decimal": "0", "relative_ppm": number(0)}
	parameters["rounding"] = map[string]any{"mode": "HALF_EVEN", "decimal_places": number(1)}
	outcome, final, err := evaluateNumericWindow(parameters, []map[string]any{
		correctiveSample(left, right, 3_000_000_000, "PRESENT"),
	}, correctiveArtifactIndex(left, right), nil)
	if err != nil || outcome != "MATCH" || final != "0.0" {
		t.Fatalf("negative value rounded to zero = (%q, %q, %v); want MATCH, 0.0", outcome, final, err)
	}
}

func TestCorrectiveObservationPointersBindExactNativeValues(t *testing.T) {
	left := correctiveArtifact("EBUS", "left", "1", "10", "degC", 2_000_000_000)
	right := correctiveArtifact("EEBUS", "right", "2", "10", "degC", 2_000_000_000)
	sample := correctiveSample(left, right, 3_000_000_000, "PRESENT")
	sample["right"].(map[string]any)["native_decimal"] = "99"
	if _, _, err := evaluateNumericWindow(correctiveParameters(), []map[string]any{sample}, correctiveArtifactIndex(left, right), nil); err == nil || err.Error() != "comparator.invalid" {
		t.Fatalf("forged native value error = %v; want comparator.invalid", err)
	}
}

func TestCorrectiveEvaluatorDerivesStateMissingAndConflictReset(t *testing.T) {
	left := correctiveArtifact("EBUS", "left", "1", "10", "degC", 2_000_000_000)
	right := correctiveArtifact("EEBUS", "right", "2", "11", "degC", 2_000_000_000)
	parameters := correctiveParameters()
	parameters["minimum_samples"] = number(3)
	parameters["conflict_threshold"] = map[string]any{"absolute_decimal": "1", "consecutive_samples": number(2)}
	matchingRight := correctiveArtifact("EEBUS", "matching", "3", "10", "degC", 2_000_000_000)
	outcome, _, err := evaluateNumericWindow(parameters, []map[string]any{
		correctiveSample(left, right, 3_000_000_000, "PRESENT"),
		correctiveSample(left, matchingRight, 4_000_000_000, "PRESENT"),
		correctiveSample(left, right, 5_000_000_000, "STALE"),
	}, correctiveArtifactIndex(left, right, matchingRight), nil)
	if err != nil || outcome != "INDETERMINATE" {
		t.Fatalf("conflict reset and stale accounting = (%q, %v); want INDETERMINATE", outcome, err)
	}

	forgedState := correctiveSample(left, right, 5_000_000_000, "PRESENT")
	if _, _, err := evaluateNumericWindow(parameters, []map[string]any{forgedState}, correctiveArtifactIndex(left, right), nil); err == nil || err.Error() != "comparator.invalid" {
		t.Fatalf("caller-controlled state error = %v; want comparator.invalid", err)
	}

	missingLeft := correctiveArtifact("EBUS", "missing", "4", "10", "degC", 2_000_000_000)
	observation := missingLeft["normalized_evidence"].(map[string]any)["observation"].(map[string]any)
	observation["value"] = nil
	observation["unit"] = nil
	parameters["minimum_samples"] = number(1)
	missing := correctiveSample(missingLeft, matchingRight, 3_000_000_000, "MISSING")
	outcome, _, err = evaluateNumericWindow(parameters, []map[string]any{missing}, correctiveArtifactIndex(missingLeft, matchingRight), nil)
	if err != nil || outcome != "INDETERMINATE" {
		t.Fatalf("missing accounting = (%q, %v); want INDETERMINATE", outcome, err)
	}
}

func TestCorrectiveBuildOverwritesStoredOutcomeWithoutMutatingInput(t *testing.T) {
	artifacts := pinnedTestArtifactsV1()
	graph, err := decodeGraphV1(artifacts.PositiveGraph)
	if err != nil {
		t.Fatal(err)
	}
	facts := append([]FactV1(nil), graph.Facts...)
	for index := range facts {
		facts[index].Comparator.Outcome = "MATCH"
		facts[index].FactHash = "sha256:" + strings.Repeat("f", 64)
	}
	input := BuildInputV1{
		SourceBundle: artifacts.SourceBundle, SourceReplay: artifacts.SourceReplay,
		ComparatorDrafts: graph.ComparatorDrafts, Facts: facts,
	}
	built, err := Build(input)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	expectedValue, err := parseJSON(artifacts.PositiveGraph)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := canonicalJSON(expectedValue)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(built, expected) {
		t.Fatalf("Build did not derive canonical outcomes\ngot:  %s\nwant: %s", built, expected)
	}
	for _, fact := range input.Facts {
		if fact.Comparator.Outcome != "MATCH" || fact.FactHash != "sha256:"+strings.Repeat("f", 64) {
			t.Fatal("Build mutated caller-owned input")
		}
	}
}

func TestCorrectiveSchemaIsExhaustiveAndPrecedesSourceVerification(t *testing.T) {
	artifacts := pinnedTestArtifactsV1()
	mutations := []func(map[string]any){
		func(graph map[string]any) { graph["visibility"].(map[string]any)["channel"] = true },
		func(graph map[string]any) {
			graph["facts"].([]any)[0].(map[string]any)["draft_unit"] = strings.Repeat("x", 257)
		},
		func(graph map[string]any) { graph["facts"].([]any)[0].(map[string]any)["draft_unit"] = "degr\u00b0" },
		func(graph map[string]any) {
			graph["facts"].([]any)[0].(map[string]any)["proposed_path"] = "/" + strings.Repeat("a", 512)
		},
		func(graph map[string]any) {
			graph["facts"].([]any)[0].(map[string]any)["confidence"].(map[string]any)["score_milli"] = true
		},
	}
	for index, mutate := range mutations {
		var graph map[string]any
		if err := json.Unmarshal(artifacts.PositiveGraph, &graph); err != nil {
			t.Fatal(err)
		}
		mutate(graph)
		raw, err := json.Marshal(graph)
		if err != nil {
			t.Fatal(err)
		}
		assertCategory(t, Verify(raw, []byte("{"), []byte("{")), "schema.graph")
		_ = index
	}
}

func TestCorrectiveBoundedPreflightRunsBeforeRecursiveDecode(t *testing.T) {
	artifacts := pinnedTestArtifactsV1()
	inputs := [][]byte{
		[]byte(strings.Repeat(" ", 1_048_577)),
		[]byte(strings.Repeat("[", 34) + strings.Repeat("]", 34)),
		[]byte("{\"value\":\"" + strings.Repeat("a", 4097) + "\"}"),
		[]byte("[" + strings.Repeat("0,", 1024) + "0]"),
		correctiveMemberLimitInput(),
		correctiveTotalListLimitInput(),
	}
	for _, raw := range inputs {
		assertCategory(t, Verify(raw, artifacts.SourceBundle, artifacts.SourceReplay), "limits.exceeded")
	}
}

func TestCorrectiveBuildAndSourceInputsAreBounded(t *testing.T) {
	artifacts := pinnedTestArtifactsV1()
	graph, err := decodeGraphV1(artifacts.PositiveGraph)
	if err != nil {
		t.Fatal(err)
	}
	facts := append([]FactV1(nil), graph.Facts...)
	facts[0].Falsifier.Description = strings.Repeat("x", 4097)
	_, err = Build(BuildInputV1{
		SourceBundle: []byte("{"), SourceReplay: []byte("{"),
		ComparatorDrafts: graph.ComparatorDrafts, Facts: facts,
	})
	assertCategory(t, err, "limits.exceeded")
	assertCategory(t, Verify(artifacts.PositiveGraph, []byte(strings.Repeat(" ", 1_048_577)), artifacts.SourceReplay), "provenance.binding")
	_, err = Replay(artifacts.PositiveGraph, []byte(strings.Repeat(" ", 1_048_577)), artifacts.SourceReplay)
	assertCategory(t, err, "provenance.binding")
}

func TestCorrectiveProductionEmbeddingExcludesNegativeFixtures(t *testing.T) {
	err := fs.WalkDir(artifactFiles, ".", func(path string, entry fs.DirEntry, err error) error {
		if err == nil && strings.Contains(path, "/negative/") {
			t.Fatalf("production embed contains negative fixture %q", path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func correctiveParameters() map[string]any {
	return map[string]any{
		"window":          map[string]any{"start_offset_ns": number(1_000_000_000), "end_offset_ns": number(9_000_000_000)},
		"tolerance":       map[string]any{"absolute_decimal": "0.2", "relative_ppm": number(10000)},
		"unit_conversion": map[string]any{"mode": "IDENTITY", "source_unit": "degC", "target_unit": "degC", "scale_decimal": "1", "offset_decimal": "0"},
		"rounding":        map[string]any{"mode": "NONE", "decimal_places": nil},
		"minimum_samples": number(1), "maximum_missing_samples": number(1), "stale_cutoff_ns": number(2_000_000_000),
		"conflict_threshold": map[string]any{"absolute_decimal": "1", "consecutive_samples": number(2)},
	}
}

func correctiveArtifact(kind, sourceSuffix, artifactSuffix, value, unit string, offset int64) map[string]any {
	ref := map[string]any{
		"kind": "CONTENT", "digest_algorithm": "SHA256_CONTENT_BYTES",
		"digest": "sha256:" + strings.Repeat(artifactSuffix, 64), "repository": nil, "commit": nil, "path": nil,
	}
	return map[string]any{
		"source_kind": kind, "source_id": strings.ToLower(kind) + "-" + sourceSuffix,
		"artifact_id":   "seav1:sha256:" + strings.Repeat(artifactSuffix, 64),
		"evidence_refs": []any{ref}, "recorder_ingested_offset_ns": number(offset),
		"normalized_evidence": map[string]any{"observation": map[string]any{"value": value, "unit": unit}},
	}
}

func correctiveSample(left, right map[string]any, offset int64, state string) map[string]any {
	return map[string]any{
		"offset_ns": number(offset), "left": correctiveSide(left), "right": correctiveSide(right), "state": state,
	}
}

func correctiveSide(artifact map[string]any) map[string]any {
	observation := artifact["normalized_evidence"].(map[string]any)["observation"].(map[string]any)
	return map[string]any{
		"source_kind": artifact["source_kind"], "source_id": artifact["source_id"], "artifact_id": artifact["artifact_id"],
		"evidence_ref": artifact["evidence_refs"].([]any)[0], "observed_offset_ns": artifact["recorder_ingested_offset_ns"],
		"value_pointer": "/observation/value", "unit_pointer": "/observation/unit",
		"native_decimal": observation["value"], "native_unit": observation["unit"],
	}
}

func correctiveArtifactIndex(artifacts ...map[string]any) map[string]map[string]any {
	result := make(map[string]map[string]any, len(artifacts))
	for _, artifact := range artifacts {
		key, _ := pairKey(artifact["source_id"], artifact["artifact_id"])
		result[key] = artifact
	}
	return result
}

func correctiveMemberLimitInput() []byte {
	var builder strings.Builder
	builder.WriteByte('{')
	for index := 0; index < 16_385; index++ {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString("\"k")
		builder.WriteString(json.Number(number(int64(index)).(json.Number)).String())
		builder.WriteString("\":0")
	}
	builder.WriteByte('}')
	return []byte(builder.String())
}

func correctiveTotalListLimitInput() []byte {
	row := "[" + strings.Repeat("0,", 1023) + "0]"
	return []byte("[" + strings.TrimSuffix(strings.Repeat(row+",", 9), ",") + "]")
}
