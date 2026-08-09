package coexistence

import (
	"path/filepath"
	"testing"
)

func TestMSP08ReviewRejectsUnrecognizedCandidateLeakValues(t *testing.T) {
	evidence := reviewEvidence(t)
	for _, rawRun := range arrayValue(t, evidence, "runs") {
		run := asObject(t, rawRun, "run")
		view := findView(t, run, "graphql.ebus.values")
		objectValue(t, objectValue(t, view, "payload"), "data")["conflict"] = true
		refreshViewHashes(t, evidence, run, view)
	}
	refreshEvidenceIdentity(t, evidence)

	assertReviewCategory(t, evidence, "anti_leak.candidate")
}

func TestMSP08ReviewRejectsAlternatePublicV2Spellings(t *testing.T) {
	evidence := reviewEvidence(t)
	for _, rawRun := range arrayValue(t, evidence, "runs") {
		run := asObject(t, rawRun, "run")
		view := findView(t, run, "mcp.tool.inventory")
		data := objectValue(t, objectValue(t, view, "payload"), "data")
		data["tools"] = append(arrayValue(t, data, "tools"), "eebus_v2.runtime.status")
		refreshViewHashes(t, evidence, run, view)
	}
	refreshEvidenceIdentity(t, evidence)

	assertReviewCategory(t, evidence, "gate.scope")
}

func TestMSP08ReviewRejectsGraphQLV3SurfaceWithRecomputedEvidence(t *testing.T) {
	evidence := reviewEvidence(t)
	for _, rawRun := range arrayValue(t, evidence, "runs") {
		run := asObject(t, rawRun, "run")
		view := findView(t, run, "graphql.schema")
		data := objectValue(t, objectValue(t, view, "payload"), "data")
		data["query_fields"] = append(arrayValue(t, data, "query_fields"), "eebus.v3.runtime.status.get")
		refreshViewHashes(t, evidence, run, view)
	}
	refreshEvidenceIdentity(t, evidence)

	assertReviewCategory(t, evidence, "gate.scope")
}

func TestMSP08ReviewRejectsUnapprovedV1AliasInDebugWithRecomputedEvidence(t *testing.T) {
	evidence := reviewEvidence(t)
	for _, rawRun := range arrayValue(t, evidence, "runs") {
		run := asObject(t, rawRun, "run")
		view := findView(t, run, "debug.ebus")
		data := objectValue(t, objectValue(t, view, "payload"), "data")
		data["observed_tool"] = "eebus.v1.runtime.status"
		refreshViewHashes(t, evidence, run, view)
	}
	refreshEvidenceIdentity(t, evidence)

	assertReviewCategory(t, evidence, "gate.scope")
}

func TestMSP08ScopeAllowsBenignEEBusProseMetadata(t *testing.T) {
	payload := map[string]any{
		"data": map[string]any{"status": "ready"},
		"meta": map[string]any{"note": "eeBUS raw runtime visibility remains operator-only"},
	}
	if containsEEBusPublicIdentifierOutsideV1Context("debug.ebus", payload) {
		t.Fatal("benign eeBUS prose metadata must not be treated as a public identifier")
	}
}

func TestMSP08ReviewRejectsImpossibleUTCTimestamps(t *testing.T) {
	t.Run("capture clock anchor", func(t *testing.T) {
		evidence := reviewEvidence(t)
		clock := objectValue(t, evidence, "capture_clock")
		clock["wall_anchor_utc"] = "2026-99-99T99:99:99Z"
		clock["clock_hash"] = domainDigest(t, clockDomainV1, withoutKeys(t, clock, "clock_hash"))
		refreshEvidenceIdentity(t, evidence)

		assertReviewCategory(t, evidence, "provenance.clock")
	})

	t.Run("protected view capture", func(t *testing.T) {
		evidence := reviewEvidence(t)
		for _, rawRun := range arrayValue(t, evidence, "runs") {
			run := asObject(t, rawRun, "run")
			for _, rawView := range arrayValue(t, run, "protected_views") {
				view := asObject(t, rawView, "protected view")
				objectValue(t, objectValue(t, view, "payload"), "meta")["captured_at"] = "2026-99-99T99:99:99Z"
				refreshViewHashes(t, evidence, run, view)
			}
		}
		refreshEvidenceIdentity(t, evidence)

		assertReviewCategory(t, evidence, "canonicalization.invalid")
	})
}

func reviewEvidence(t *testing.T) map[string]any {
	t.Helper()
	return loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
}

func assertReviewCategory(t *testing.T, evidence map[string]any, want string) {
	t.Helper()
	inputs := reviewInputs(t)
	inputs.Evidence = canonicalJSON(t, evidence)
	err := Verify(inputs)
	if err == nil || err.Error() != want {
		t.Fatalf("Verify() error = %v; want %s", err, want)
	}
}

func reviewInputs(t *testing.T) InputsV1 {
	t.Helper()
	repo := repoDir(t)
	return InputsV1{
		Registry:       readFile(t, filepath.Join(repo, "internal/coexistence/contracts/multi-runtime-coexistence-registry-v1.json")),
		M7Graph:        readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/graph.json")),
		M7Replay:       readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/replay-result.json")),
		M7Registry:     readFile(t, filepath.Join(repo, "internal/candidatefacts/contracts/draft-candidate-fact-registry-v1.json")),
		M7SourceBundle: readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/source/bundle.json")),
		M7SourceReplay: readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/source/replay-result.json")),
	}
}
