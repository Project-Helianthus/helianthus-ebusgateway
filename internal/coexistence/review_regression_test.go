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

func TestMSP08ReviewRejectsStructuredCandidateLeakKeyVariantsWithRecomputedEvidence(t *testing.T) {
	for _, fixture := range []struct {
		name   string
		viewID string
		key    string
		value  any
	}{
		{name: "GraphQL candidate status suffix", viewID: "graphql.ebus.values", key: "candidate_status_v1", value: map[string]any{"state": true}},
		{name: "HA conflict state", viewID: "ha.graphql.values", key: "conflict_state", value: []any{true}},
		{name: "debug raw only reason", viewID: "debug.ebus", key: "raw_only_reason", value: map[string]any{"code": 1}},
		{name: "debug withheld count suffix", viewID: "debug.ebus", key: "withheld_count_v1", value: 1},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			evidence := reviewEvidence(t)
			for _, rawRun := range arrayValue(t, evidence, "runs") {
				run := asObject(t, rawRun, "run")
				view := findView(t, run, fixture.viewID)
				data := objectValue(t, objectValue(t, view, "payload"), "data")
				data[fixture.key] = fixture.value
				refreshViewHashes(t, evidence, run, view)
			}
			refreshEvidenceIdentity(t, evidence)

			assertReviewCategory(t, evidence, "anti_leak.candidate")
		})
	}
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

func TestMSP08ReviewRejectsStableToolInventoryDrift(t *testing.T) {
	for _, mutation := range []string{"missing", "stale", "reordered", "write"} {
		t.Run(mutation, func(t *testing.T) {
			evidence := reviewEvidence(t)
			for _, rawRun := range arrayValue(t, evidence, "runs")[:4] {
				run := asObject(t, rawRun, "run")
				view := findView(t, run, "mcp.tool.inventory")
				data := objectValue(t, objectValue(t, view, "payload"), "data")
				tools := arrayValue(t, data, "tools")
				switch mutation {
				case "missing":
					data["tools"] = append(tools[:6:6], tools[7:]...)
				case "stale":
					tools[0] = "ebus.v1.devices.list"
				case "reordered":
					tools[len(tools)-1], tools[len(tools)-2] = tools[len(tools)-2], tools[len(tools)-1]
				case "write":
					data["tools"] = append(tools, "eebus.v1.features.data.set")
				}
				refreshViewHashes(t, evidence, run, view)
			}
			refreshEvidenceIdentity(t, evidence)

			assertReviewCategory(t, evidence, "gate.scope")
		})
	}
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

func TestMSP08ReviewRejectsPermutedImmutableInputsWithRecomputedEvidence(t *testing.T) {
	evidence := reviewEvidence(t)
	for _, rawRun := range arrayValue(t, evidence, "runs") {
		run := asObject(t, rawRun, "run")
		inputs := arrayValue(t, objectValue(t, run, "provenance"), "immutable_inputs")
		inputs[0], inputs[1] = inputs[1], inputs[0]
	}
	refreshEvidenceIdentity(t, evidence)

	assertReviewCategory(t, evidence, "ordering.duplicate")
}

func TestMSP08ReviewRejectsUnversionedEEBusPublicIdentifiersWithRecomputedEvidence(t *testing.T) {
	for _, fixture := range []struct {
		name   string
		viewID string
	}{
		{name: "GraphQL", viewID: "graphql.schema"},
		{name: "HA", viewID: "ha.graphql.values"},
		{name: "debug", viewID: "debug.ebus"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			evidence := reviewEvidence(t)
			for _, rawRun := range arrayValue(t, evidence, "runs") {
				run := asObject(t, rawRun, "run")
				view := findView(t, run, fixture.viewID)
				data := objectValue(t, objectValue(t, view, "payload"), "data")
				data["eebusRuntimeStatus"] = "present"
				refreshViewHashes(t, evidence, run, view)
			}
			refreshEvidenceIdentity(t, evidence)

			assertReviewCategory(t, evidence, "gate.scope")
		})
	}
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
