package candidatefacts

import (
	"encoding/json"
	"strconv"
	"strings"
)

func expandNegativeFixture(value map[string]any) (map[string]any, error) {
	if value["contract"] != negativeContractV1 {
		return value, nil
	}
	if !exactKeys(value, "contract", "base", "mutation") || value["base"] != "../positive/graph.json" {
		return nil, fail("json.syntax")
	}
	positive, err := parseJSON(PinnedArtifactsV1().PositiveGraph)
	if err != nil {
		return nil, fail("json.syntax")
	}
	graph, ok := objectValue(positive)
	if !ok {
		return nil, fail("json.syntax")
	}
	mutation, ok := stringValue(value["mutation"])
	if !ok {
		return nil, fail("json.syntax")
	}
	facts, _ := arrayValue(graph["facts"])
	sourceBundle, _ := objectValue(graph["source_bundle"])
	rootRefs, _ := arrayValue(sourceBundle["evidence_refs"])
	switch mutation {
	case "ANTI_LEAK_STABLE_SURFACE":
		visibility, _ := objectValue(graph["visibility"])
		visibility["stable_exposure"] = true
	case "COMPARATOR_PARAMETER_INVALID":
		drafts, _ := arrayValue(graph["comparator_drafts"])
		draft, _ := objectValue(drafts[0])
		parameters, _ := objectValue(draft["parameters"])
		parameters["minimum_samples"] = number(0)
	case "EVIDENCE_REF_NOT_IN_BUNDLE":
		provenance := factProvenance(facts[0])
		refs, _ := arrayValue(provenance["native_evidence_refs"])
		ref, _ := objectValue(refs[0])
		ref["digest"] = "sha256:" + strings.Repeat("f", 64)
	case "GRAPH_HASH_MISMATCH":
		graph["graph_hash"] = "sha256:" + strings.Repeat("0", 64)
	case "FORGED_ARTIFACT_ID":
		cloud, _ := objectValue(factProvenance(facts[0])["cloud"])
		cloud["artifact_id"] = "seav1:sha256:" + strings.Repeat("f", 64)
	case "FORGED_SOURCE_ID":
		cloud, _ := objectValue(factProvenance(facts[0])["cloud"])
		cloud["source_id"] = "cloud-" + strings.Repeat("f", 32)
	case "FORGED_B524_OPCODE":
		identity := firstEBusIdentity(facts, "B524")
		identity["opcode"] = number(6)
	case "INCOMPLETE_B524_IDENTITY":
		identity := firstEBusIdentity(facts, "B524")
		delete(identity, "RR")
	case "INVALID_EEBUS_FEATURE_PATH", "FORGED_EEBUS_ENTITY_FEATURE":
		provenance := factProvenance(facts[2])
		provenance["eebus_source_id"] = "eebus-44444444444444444444444444444444"
		provenance["eebus_artifact_id"] = "seav1:sha256:841786ac24dc98b6384aaa8fa3930c50404140bf0a38d97cad1aba717bac3ac8"
		path := []any{
			map[string]any{"kind": "SERVICE", "selector": "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"},
			map[string]any{"kind": "ENTITY", "selector": "entity-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"},
			map[string]any{"kind": "FEATURE", "selector": "feature-ffffffffffffffffffffffffffffffff"},
		}
		provenance["eebus"] = map[string]any{
			"service": "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC", "entity": "entity-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			"feature": "feature-ffffffffffffffffffffffffffffffff", "feature_path": path,
		}
		refs, _ := arrayValue(provenance["native_evidence_refs"])
		provenance["native_evidence_refs"] = append(refs, rootRefs[4])
		if mutation == "INVALID_EEBUS_FEATURE_PATH" {
			first, _ := objectValue(path[0])
			first["kind"] = "FEATURE"
		}
	case "LIMIT_EXCEEDED":
		limits, _ := objectValue(graph["limits"])
		limits["max_facts"] = number(65)
	case "ORDERING_INVALID":
		for left, right := 0, len(facts)-1; left < right; left, right = left+1, right-1 {
			facts[left], facts[right] = facts[right], facts[left]
		}
	case "REGISTRY_MISMATCH":
		registry, _ := objectValue(graph["registry"])
		registry["digest"] = "sha256:" + strings.Repeat("0", 64)
	case "WRONG_SOURCE_BUNDLE":
		sourceBundle["bundle_hash"] = "sha256:" + strings.Repeat("f", 64)
	case "WRONG_SOURCE_REPLAY":
		sourceBundle["replay_hash"] = "sha256:" + strings.Repeat("f", 64)
	case "TERMINAL_STATE_NOT_WITHHELD":
		for _, raw := range facts {
			fact, _ := objectValue(raw)
			if fact["terminal_negative_state"] != nil {
				fact["status"] = "CANDIDATE"
				break
			}
		}
	case "UNKNOWN_FIELD":
		graph["unknown"] = true
	default:
		return nil, fail("json.syntax")
	}
	return graph, nil
}

func factProvenance(raw any) map[string]any {
	fact, _ := objectValue(raw)
	provenance, _ := objectValue(fact["provenance"])
	return provenance
}

func firstEBusIdentity(facts []any, family string) map[string]any {
	for _, raw := range facts {
		identity, ok := objectValue(factProvenance(raw)["ebus"])
		if ok && identity["family"] == family {
			return identity
		}
	}
	return nil
}

func number(value int64) any {
	return json.Number(strconv.FormatInt(value, 10))
}
