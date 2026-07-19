package candidatefacts

import (
	"embed"
	"strings"
)

//go:embed testdata/canonical/negative/*.json
var negativeFixtureFiles embed.FS

type testArtifactsV1 struct {
	artifactsV1
	NegativeGraphs map[string][]byte
}

func pinnedTestArtifactsV1() testArtifactsV1 {
	artifacts := pinnedArtifactsV1()
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
		expanded, err := expandNegativeFixtureForTest(fixture)
		if err != nil {
			panic(err)
		}
		negative[name], err = canonicalJSON(expanded)
		if err != nil {
			panic(err)
		}
	}
	return testArtifactsV1{artifactsV1: artifacts, NegativeGraphs: negative}
}

func expandNegativeFixtureForTest(value map[string]any) (map[string]any, error) {
	if value["contract"] != "helianthus.platform.draft-candidate-fact-negative-fixture.v1" ||
		value["base"] != "../positive/graph.json" {
		return nil, fail("json.syntax")
	}
	base, err := parseJSON(pinnedArtifactsV1().PositiveGraph)
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
