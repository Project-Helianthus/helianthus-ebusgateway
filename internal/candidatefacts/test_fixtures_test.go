package candidatefacts

import (
	"embed"
	"strings"
)

//go:embed testdata/canonical/negative/*.json
var negativeFixtureFiles embed.FS

type testArtifactsV1 struct {
	artifactsV1
	NegativeGraphs             map[string][]byte
	SourceTerminalGraph        []byte
	SourceTerminalReplay       []byte
	SourceTerminalBundle       []byte
	SourceTerminalSourceReplay []byte
}

func pinnedTestArtifactsV1() testArtifactsV1 {
	artifacts := testArtifactsV1{
		artifactsV1:                pinnedArtifactsV1(),
		SourceTerminalGraph:        readPinned("testdata/canonical/positive/source-terminal-graph.json", "2fd356d9d3262281bcf830154d8507bbb237f3f0d091b737365e3812cdeaafb3"),
		SourceTerminalReplay:       readPinned("testdata/canonical/positive/source-terminal-replay-result.json", "ba478963171fc238e76ee19036119e0f2543d98f73c35c124abef4028b2fae22"),
		SourceTerminalBundle:       readPinned("testdata/canonical/source/source-terminal-bundle.json", "b7269e608dfc004f6737fd80b34a1c6018483942632e0c3c8bfd33cadbaa8134"),
		SourceTerminalSourceReplay: readPinned("testdata/canonical/source/source-terminal-replay-result.json", "82ed1c66222348746abd9f4291e10b968314363160ba4cfc568044390aced9d2"),
	}
	negative := make(map[string][]byte, len(negativeCategoriesV1))
	for name := range negativeCategoriesV1 {
		raw, err := negativeFixtureFiles.ReadFile("testdata/canonical/negative/" + name)
		if err != nil {
			panic("candidatefacts test: missing negative fixture: " + name)
		}
		value, err := parseJSON(raw)
		if err != nil {
			panic(err)
		}
		fixture, ok := objectValue(value)
		if !ok {
			panic("candidatefacts test: invalid negative fixture: " + name)
		}
		expanded, err := expandNegativeFixtureForTest(fixture, artifacts)
		if err != nil {
			panic(err)
		}
		negative[name], err = canonicalJSON(expanded)
		if err != nil {
			panic(err)
		}
	}
	artifacts.NegativeGraphs = negative
	return artifacts
}

func expandNegativeFixtureForTest(value map[string]any, artifacts testArtifactsV1) (map[string]any, error) {
	if value["contract"] != "helianthus.platform.draft-candidate-fact-negative-fixture.v1" {
		return nil, fail("json.syntax")
	}
	var baseRaw []byte
	switch value["base"] {
	case "../positive/graph.json":
		baseRaw = artifacts.PositiveGraph
	case "../positive/source-terminal-graph.json":
		baseRaw = artifacts.SourceTerminalGraph
	default:
		return nil, fail("json.syntax")
	}
	base, err := parseJSON(baseRaw)
	if err != nil {
		return nil, err
	}
	graph, _ := objectValue(base)
	mutation, ok := stringValue(value["mutation"])
	if !ok {
		return nil, fail("json.syntax")
	}
	facts, _ := arrayValue(graph["facts"])
	sourceBundle, _ := objectValue(graph["source_bundle"])
	rootRefs, _ := arrayValue(sourceBundle["evidence_refs"])
	switch mutation {
	case "ANTI_LEAK_STABLE_SURFACE":
		objectAtUnsafe(graph, "visibility")["stable_exposure"] = true
	case "COMPARATOR_PARAMETER_INVALID":
		drafts, _ := arrayValue(graph["comparator_drafts"])
		draft, _ := objectValue(drafts[0])
		parameters := objectAtUnsafe(draft, "parameters")
		window := objectAtUnsafe(parameters, "window")
		window["start_offset_ns"] = window["end_offset_ns"]
	case "EVIDENCE_REF_NOT_IN_BUNDLE":
		ref := firstFactRef(facts)
		ref["digest"] = "sha256:" + strings.Repeat("f", 64)
	case "FORGED_ARTIFACT_ID":
		cloud := firstCloud(facts)
		cloud["artifact_id"] = "seav1:sha256:" + strings.Repeat("f", 64)
	case "FORGED_SOURCE_ID":
		cloud := firstCloud(facts)
		cloud["source_id"] = "cloud-" + strings.Repeat("f", 32)
	case "FORGED_B524_OPCODE":
		firstEBusIdentityForTest(facts, "B524")["opcode"] = number(6)
	case "INCOMPLETE_B524_IDENTITY":
		delete(firstEBusIdentityForTest(facts, "B524"), "RR")
	case "FORGED_EEBUS_ENTITY_FEATURE", "INVALID_EEBUS_FEATURE_PATH":
		provenance := firstEEBusServiceProvenance(facts)
		service := provenance["eebus_service"]
		path := []any{
			map[string]any{"kind": "SERVICE", "selector": service},
			map[string]any{"kind": "ENTITY", "selector": "entity-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"},
			map[string]any{"kind": "FEATURE", "selector": "feature-ffffffffffffffffffffffffffffffff"},
		}
		provenance["eebus"] = map[string]any{
			"service": service, "entity": "entity-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			"feature": "feature-ffffffffffffffffffffffffffffffff", "feature_path": path,
		}
		if mutation == "INVALID_EEBUS_FEATURE_PATH" {
			path[0].(map[string]any)["kind"] = "FEATURE"
		}
	case "GRAPH_HASH_MISMATCH":
		graph["graph_hash"] = "sha256:" + strings.Repeat("0", 64)
	case "LIMIT_EXCEEDED":
		objectAtUnsafe(graph, "limits")["max_facts"] = number(65)
	case "ORDERING_INVALID":
		for left, right := 0, len(facts)-1; left < right; left, right = left+1, right-1 {
			facts[left], facts[right] = facts[right], facts[left]
		}
	case "REGISTRY_MISMATCH":
		objectAtUnsafe(graph, "registry")["digest"] = "sha256:" + strings.Repeat("0", 64)
	case "SOURCE_TERMINAL_CANDIDATE":
		fact := firstSourceTerminalFactForTest(facts)
		fact["status"] = "CANDIDATE"
		fact["terminal_negative_state"] = nil
	case "SOURCE_TERMINAL_CONFLICTED":
		fact := firstSourceTerminalFactForTest(facts)
		fact["status"] = "CONFLICTED"
		fact["terminal_negative_state"] = nil
	case "SOURCE_TERMINAL_EVALUATED_SAMPLES":
		fact := firstSourceTerminalFactForTest(facts)
		ref := cloneObject(firstFactRef([]any{fact}))
		fact["comparator"] = map[string]any{
			"draft_id": "NUMERIC_WINDOW_V1_DRAFT",
			"samples": []any{map[string]any{
				"offset_ns": number(1),
				"left": map[string]any{
					"source_kind": "EBUS", "source_id": "ebus-" + strings.Repeat("a", 32),
					"artifact_id": "seav1:sha256:" + strings.Repeat("a", 64), "evidence_ref": ref,
					"observed_offset_ns": number(1), "value_pointer": "/value", "unit_pointer": "/unit",
					"native_decimal": "1", "native_unit": "degC",
				},
				"right": map[string]any{
					"source_kind": "EEBUS", "source_id": "eebus-" + strings.Repeat("b", 32),
					"artifact_id": "seav1:sha256:" + strings.Repeat("b", 64), "evidence_ref": ref,
					"observed_offset_ns": number(1), "value_pointer": "/value", "unit_pointer": "/unit",
					"native_decimal": "1", "native_unit": "degC",
				},
				"state": "PRESENT",
			}},
			"outcome": "INDETERMINATE",
		}
	case "SOURCE_TERMINAL_PROMOTED_EXPOSURE":
		objectAtUnsafe(graph, "visibility")["stable_exposure"] = true
	case "SOURCE_TERMINAL_FORGED_SOURCE_ID":
		firstSourceTerminalForTest(facts)["source_id"] = "ebus-" + strings.Repeat("f", 32)
	case "SOURCE_TERMINAL_FORGED_SOURCE_KIND":
		firstSourceTerminalForTest(facts)["source_kind"] = "EEBUS"
	case "SOURCE_TERMINAL_FORGED_BINDING_KIND":
		firstSourceTerminalForTest(facts)["binding_source_kind"] = "EBUS_B524"
	case "SOURCE_TERMINAL_FORGED_CONTRACT":
		firstSourceTerminalForTest(facts)["source_contract"] = "helianthus.ebus.forged.evidence.v1"
	case "SOURCE_TERMINAL_FORGED_VERSION":
		firstSourceTerminalForTest(facts)["source_schema_version"] = number(2)
	case "SOURCE_TERMINAL_FORGED_PHASE":
		firstSourceTerminalForTest(facts)["phase"] = "action"
	case "SOURCE_TERMINAL_FORGED_STATE":
		firstSourceTerminalForTest(facts)["state"] = "NOT_TESTED"
	case "SOURCE_TERMINAL_FORGED_ERROR":
		firstSourceTerminalForTest(facts)["error_category"] = "TIMEOUT"
	case "SOURCE_TERMINAL_FORGED_IDENTITY":
		identity := objectAtUnsafe(firstSourceTerminalForTest(facts), "ebus_identity")
		identity["target_address"] = number(9)
	case "SOURCE_TERMINAL_FORGED_EVIDENCE_REFS":
		refs, _ := arrayValue(firstSourceTerminalForTest(facts)["evidence_refs"])
		ref, _ := objectValue(refs[0])
		ref["digest"] = "sha256:" + strings.Repeat("f", 64)
	case "SOURCE_TERMINAL_CROSS_RUNTIME_PAIRING":
		provenance := factProvenanceForTest(firstSourceTerminalFactForTest(facts))
		provenance["eebus_source_id"] = "eebus-" + strings.Repeat("e", 32)
		provenance["eebus_artifact_id"] = "seav1:sha256:" + strings.Repeat("e", 64)
		provenance["eebus_service"] = "service-" + strings.Repeat("e", 32)
	case "SOURCE_TERMINAL_NO_SIGNAL":
		fact := firstSourceTerminalFactForTest(facts)
		fact["terminal_negative_state"] = "NO_SIGNAL"
		objectAtUnsafe(fact, "falsifier")["expected_terminal_state"] = "NO_SIGNAL"
	case "SOURCE_TERMINAL_NULL":
		factProvenanceForTest(firstSourceTerminalFactForTest(facts))["source_terminal"] = nil
	case "SOURCE_TERMINAL_OMITTED":
		delete(factProvenanceForTest(firstSourceTerminalFactForTest(facts)), "source_terminal")
	case "TERMINAL_STATE_NOT_WITHHELD":
		for _, raw := range facts {
			fact, _ := objectValue(raw)
			provenance, _ := objectValue(fact["provenance"])
			if provenance["cloud"] != nil {
				fact["status"] = "RAW_ONLY"
				fact["terminal_negative_state"] = nil
				break
			}
		}
	case "UNKNOWN_FIELD":
		graph["unknown"] = true
	case "WRONG_SOURCE_BUNDLE":
		sourceBundle["bundle_hash"] = "sha256:" + strings.Repeat("f", 64)
	case "WRONG_SOURCE_REPLAY":
		sourceBundle["replay_hash"] = "sha256:" + strings.Repeat("f", 64)
	default:
		return nil, fail("json.syntax")
	}
	_ = rootRefs
	return graph, nil
}

func firstFactRef(facts []any) map[string]any {
	provenance := factProvenanceForTest(facts[0])
	refs, _ := arrayValue(provenance["native_evidence_refs"])
	ref, _ := objectValue(refs[0])
	return ref
}

func firstCloud(facts []any) map[string]any {
	for _, raw := range facts {
		provenance := factProvenanceForTest(raw)
		if cloud, ok := objectValue(provenance["cloud"]); ok {
			return cloud
		}
	}
	return nil
}

func firstEEBusServiceProvenance(facts []any) map[string]any {
	for _, raw := range facts {
		provenance := factProvenanceForTest(raw)
		if provenance["eebus_service"] != nil {
			return provenance
		}
	}
	return nil
}

func factProvenanceForTest(raw any) map[string]any {
	fact, _ := objectValue(raw)
	provenance, _ := objectValue(fact["provenance"])
	return provenance
}

func firstEBusIdentityForTest(facts []any, family string) map[string]any {
	for _, raw := range facts {
		identity, ok := objectValue(factProvenanceForTest(raw)["ebus"])
		if ok && identity["family"] == family {
			return identity
		}
	}
	return nil
}

func firstSourceTerminalFactForTest(facts []any) map[string]any {
	for _, raw := range facts {
		fact, ok := objectValue(raw)
		if ok {
			if _, terminalOK := objectValue(factProvenanceForTest(fact)["source_terminal"]); terminalOK {
				return fact
			}
		}
	}
	return nil
}

func firstSourceTerminalForTest(facts []any) map[string]any {
	terminal, _ := objectValue(factProvenanceForTest(firstSourceTerminalFactForTest(facts))["source_terminal"])
	return terminal
}
