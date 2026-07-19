package candidatefacts

import (
	"reflect"
	"sort"
	"testing"
)

func TestOutcomeMatrixPinsCorrectiveDocsMerge(t *testing.T) {
	const want = "ea88fef23ecb154b08f70e7f94b36e1738ed08bf"
	if got := PinnedContractV1().OwnerCommit; got != want {
		t.Fatalf("OwnerCommit = %q; want %q", got, want)
	}
}

func TestOutcomeMatrixAcceptsIntegratedSampledMappings(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     string
		terminal   any
		outcome    string
		leftValue  any
		rightValue any
		state      string
	}{
		{
			name:   "mismatch is an active conflict",
			status: "CONFLICTED", outcome: "MISMATCH",
			leftValue: "10", rightValue: "10.5", state: "PRESENT",
		},
		{
			name:   "indeterminate is not tested for this bundle",
			status: "WITHHELD", terminal: "NOT_TESTED", outcome: "INDETERMINATE",
			leftValue: nil, rightValue: "10", state: "MISSING",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			graph, source, sourceReplay := outcomeMatrixVector(t, test.status, test.terminal, test.outcome, test.leftValue, test.rightValue, test.state)
			if err := verifyOutcomeMatrixVector(graph, source, sourceReplay); err != nil {
				t.Fatalf("integrated %s mapping: %v", test.outcome, err)
			}
		})
	}
}

func TestOutcomeMatrixRejectsSwappedSampledMappings(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     string
		terminal   any
		outcome    string
		leftValue  any
		rightValue any
		state      string
	}{
		{
			name:   "mismatch is not a terminal bundle conflict",
			status: "WITHHELD", terminal: "CONFLICT", outcome: "MISMATCH",
			leftValue: "10", rightValue: "10.5", state: "PRESENT",
		},
		{
			name:   "indeterminate is not an active conflict",
			status: "CONFLICTED", outcome: "INDETERMINATE",
			leftValue: nil, rightValue: "10", state: "MISSING",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			graph, source, sourceReplay := outcomeMatrixVector(t, test.status, test.terminal, test.outcome, test.leftValue, test.rightValue, test.state)
			assertCategory(t, verifyOutcomeMatrixVector(graph, source, sourceReplay), "state.terminal")
		})
	}
}

func TestOutcomeMatrixAvailabilityFailurePrecedesConflict(t *testing.T) {
	left := correctiveArtifact("EBUS", "availability-left", "7", "10", "degC", 2_000_000_000)
	missingLeft := correctiveArtifact("EBUS", "availability-missing", "8", "10", "degC", 2_000_000_000)
	missingObservation := missingLeft["normalized_evidence"].(map[string]any)["observation"].(map[string]any)
	missingObservation["value"] = nil
	missingObservation["unit"] = nil
	right := correctiveArtifact("EEBUS", "availability-right", "9", "11", "degC", 2_000_000_000)
	parameters := correctiveParameters()
	parameters["minimum_samples"] = number(1)
	parameters["maximum_missing_samples"] = number(0)
	parameters["conflict_threshold"] = map[string]any{
		"absolute_decimal": "1", "consecutive_samples": number(1),
	}
	outcome, _, err := evaluateNumericWindow(parameters, []map[string]any{
		correctiveSample(left, right, 3_000_000_000, "PRESENT"),
		correctiveSample(missingLeft, right, 4_000_000_000, "MISSING"),
	}, correctiveArtifactIndex(left, missingLeft, right), nil)
	if err != nil || outcome != "INDETERMINATE" {
		t.Fatalf("availability plus conflict = (%q, %v); want INDETERMINATE", outcome, err)
	}
}

func outcomeMatrixVector(t *testing.T, status string, terminal any, outcome string, leftValue, rightValue any, state string) (map[string]any, map[string]any, map[string]any) {
	t.Helper()
	artifacts := pinnedTestArtifactsV1()
	graph := decodeObject(t, artifacts.PositiveGraph)
	source := decodeObject(t, artifacts.SourceBundle)
	sourceReplay := decodeObject(t, artifacts.SourceReplay)

	var target map[string]any
	for _, raw := range arrayAt(t, graph, "facts") {
		fact := raw.(map[string]any)
		provenance := objectAt(t, fact, "provenance")
		identity, _ := provenance["ebus"].(map[string]any)
		if identity != nil && identity["family"] == "B524" && fact["status"] == "RAW_ONLY" {
			target = fact
			break
		}
	}
	if target == nil {
		t.Fatal("canonical graph has no B524 RAW_ONLY fact")
	}

	left := correctiveArtifact("EBUS", "outcome-left", "7", "10", "degC", 2_000_000_000)
	right := correctiveArtifact("EEBUS", "outcome-right", "8", "10", "degC", 2_000_000_000)
	setOutcomeObservation(left, leftValue)
	setOutcomeObservation(right, rightValue)

	provenance := objectAt(t, target, "provenance")
	left["ebus_identity"] = cloneObject(provenance["ebus"].(map[string]any))
	eebusPath := map[string]any{
		"service": "service-" + repeatByte('a', 32),
		"entity":  "entity-" + repeatByte('b', 32),
		"feature": "feature-" + repeatByte('c', 32),
		"feature_path": []any{
			map[string]any{"kind": "SERVICE", "selector": "service-" + repeatByte('a', 32)},
			map[string]any{"kind": "ENTITY", "selector": "entity-" + repeatByte('b', 32)},
			map[string]any{"kind": "FEATURE", "selector": "feature-" + repeatByte('c', 32)},
		},
	}
	rightNormalized := right["normalized_evidence"].(map[string]any)
	rightNormalized["data"] = map[string]any{
		"services":      []any{map[string]any{"id": map[string]any{"digest": eebusPath["service"]}}},
		"feature_paths": []any{cloneObject(eebusPath)},
	}

	leftRef := left["evidence_refs"].([]any)[0]
	rightRef := right["evidence_refs"].([]any)[0]
	refs := []any{leftRef, rightRef}
	sort.Slice(refs, func(i, j int) bool { return evidenceRefLess(refs[i], refs[j]) })
	provenance["native_evidence_refs"] = refs
	provenance["ebus_source_id"] = left["source_id"]
	provenance["ebus_artifact_id"] = left["artifact_id"]
	provenance["eebus_source_id"] = right["source_id"]
	provenance["eebus_artifact_id"] = right["artifact_id"]
	provenance["eebus_service"] = eebusPath["service"]
	provenance["eebus"] = eebusPath

	target["status"] = status
	target["terminal_negative_state"] = terminal
	target["draft_value"] = nil
	target["draft_unit"] = nil
	target["comparator"] = map[string]any{
		"draft_id": "NUMERIC_WINDOW_V1_DRAFT",
		"samples": []any{
			correctiveSample(left, right, 3_000_000_000, state),
			correctiveSample(left, right, 4_000_000_000, state),
		},
		"outcome": outcome,
	}

	source["sources"] = append(arrayAt(t, source, "sources"),
		map[string]any{"source_id": left["source_id"], "source_kind": "EBUS", "artifact_ids": []any{left["artifact_id"]}},
		map[string]any{"source_id": right["source_id"], "source_kind": "EEBUS", "artifact_ids": []any{right["artifact_id"]}},
	)
	source["artifacts"] = append(arrayAt(t, source, "artifacts"), left, right)
	rootRefs := append(arrayAt(t, source, "evidence_refs"), leftRef, rightRef)
	sort.Slice(rootRefs, func(i, j int) bool { return evidenceRefLess(rootRefs[i], rootRefs[j]) })
	source["evidence_refs"] = rootRefs
	objectAt(t, graph, "source_bundle")["evidence_refs"] = cloneArray(rootRefs)
	normalizeBuildOrdering(graph)
	rehashOutcomeMatrixGraph(t, graph)
	return graph, source, sourceReplay
}

func setOutcomeObservation(artifact map[string]any, value any) {
	observation := artifact["normalized_evidence"].(map[string]any)["observation"].(map[string]any)
	observation["value"] = value
	if value == nil {
		observation["unit"] = nil
	}
}

func rehashOutcomeMatrixGraph(t *testing.T, graph map[string]any) {
	t.Helper()
	for _, raw := range arrayAt(t, graph, "facts") {
		fact := raw.(map[string]any)
		view := cloneObject(fact)
		delete(view, "fact_hash")
		canonical, err := canonicalJSON(view)
		if err != nil {
			t.Fatal(err)
		}
		fact["fact_hash"] = "sha256:" + domainHex(factDomainV1, canonical)
	}
	view := cloneObject(graph)
	delete(view, "graph_id")
	delete(view, "graph_hash")
	canonical, err := canonicalJSON(view)
	if err != nil {
		t.Fatal(err)
	}
	digest := domainHex(graphDomainV1, canonical)
	graph["graph_id"] = "dcfgv1:sha256:" + digest
	graph["graph_hash"] = "sha256:" + digest
}

func verifyOutcomeMatrixVector(graph, source, sourceReplay map[string]any) error {
	registry, registryRaw, err := loadRegistryV1()
	if err != nil {
		return err
	}
	if err := schemaCheck(graph); err != nil {
		return err
	}
	encoded, err := canonicalJSON(graph)
	if err != nil {
		return err
	}
	if err := checkLimits(graph, len(encoded)); err != nil {
		return err
	}
	if err := checkRegistryBinding(graph, registry, registryRaw); err != nil {
		return err
	}
	return verifyGraphAfterRegistry(graph, registry, source, sourceReplay)
}

func cloneArray(values []any) []any {
	result := make([]any, len(values))
	for index, value := range values {
		switch typed := value.(type) {
		case map[string]any:
			result[index] = cloneObject(typed)
		default:
			result[index] = typed
		}
	}
	return result
}

func TestOutcomeMatrixPreservesFourTerminalStatesAndV1(t *testing.T) {
	registry, _, err := loadRegistryV1()
	if err != nil {
		t.Fatal(err)
	}
	terminals, ok := stringArray(registry["terminal_negative_states"])
	if !ok || !reflect.DeepEqual(terminals, []string{"NO_SIGNAL", "CLOUD_ONLY", "CONFLICT", "NOT_TESTED"}) {
		t.Fatalf("terminal states = %v; want exact V1 set", terminals)
	}
	if PinnedContractV1().GraphSchemaPath != "docs/platform/schemas/draft-candidate-fact-graph-v1.schema.json" {
		t.Fatal("outcome correction introduced a non-V1 graph schema")
	}
}
