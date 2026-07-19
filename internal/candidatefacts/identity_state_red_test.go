package candidatefacts

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

func TestMSP07ExactNativeIdentityAndEvidenceBinding(t *testing.T) {
	artifacts := pinnedTestArtifactsV1()
	source := decodeObject(t, artifacts.SourceBundle)
	graph := decodeObject(t, artifacts.PositiveGraph)

	sourceArtifacts := make(map[string]map[string]any)
	identities := make(map[string]map[string]any)
	for _, raw := range arrayAt(t, source, "artifacts") {
		artifact := raw.(map[string]any)
		key := artifact["source_id"].(string) + "\x00" + artifact["artifact_id"].(string)
		sourceArtifacts[key] = artifact
		if identity, ok := artifact["ebus_identity"].(map[string]any); ok {
			identities[identity["family"].(string)] = identity
		}
	}

	wantIdentities := map[string]map[string]any{
		"B509": {
			"family": "B509", "target_pseudonym": "target-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"target_address": json.Number("8"), "target_product": "BAI00", "register_family": "system",
			"register_id": json.Number("512"), "unit_scale_source": "gateway-catalog-v1", "evidence_role": "AUTHORITATIVE",
		},
		"B524": {
			"family": "B524", "target_pseudonym": "target-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			"opcode": json.Number("2"), "GG": json.Number("3"), "II": json.Number("0"), "RR": json.Number("28"),
			"target_address": json.Number("21"), "source_address": json.Number("247"), "group_meaning": "zones",
			"instance_gate": "index-not-ff", "register_category": "STATE", "unit_scale_source": "vrc-explorer-v1",
		},
		"B555": {
			"family": "B555", "target_pseudonym": "target-cccccccccccccccccccccccccccccccc",
			"device_family": "VRC", "schedule_program": "heating-program-1", "slot_index": json.Number("0"),
			"day_of_week": "MONDAY", "time_identity": "06:00:00", "operation_mode_context": "AUTO",
			"unit_scale_source": "source-native",
		},
	}
	if !reflect.DeepEqual(identities, wantIdentities) {
		t.Fatalf("source native identities = %#v; want %#v", identities, wantIdentities)
	}

	rootRefs := make(map[string]bool)
	for _, ref := range arrayAt(t, objectAt(t, graph, "source_bundle"), "evidence_refs") {
		rootRefs[jsonKey(t, ref)] = true
	}
	seenFamilies := make(map[string]bool)
	for _, raw := range arrayAt(t, graph, "facts") {
		fact := raw.(map[string]any)
		provenance := objectAt(t, fact, "provenance")
		for _, ref := range arrayAt(t, provenance, "native_evidence_refs") {
			if !rootRefs[jsonKey(t, ref)] {
				t.Fatalf("fact %v has evidence ref outside the source bundle: %#v", fact["candidate_id"], ref)
			}
		}
		if identity, ok := provenance["ebus"].(map[string]any); ok {
			key := provenance["ebus_source_id"].(string) + "\x00" + provenance["ebus_artifact_id"].(string)
			artifact, exists := sourceArtifacts[key]
			if !exists || !reflect.DeepEqual(identity, artifact["ebus_identity"]) {
				t.Fatalf("fact %v eBUS identity is not deep-equal to its verified source artifact", fact["candidate_id"])
			}
			seenFamilies[identity["family"].(string)] = true
		}
		if cloud, ok := provenance["cloud"].(map[string]any); ok {
			key := cloud["source_id"].(string) + "\x00" + cloud["artifact_id"].(string)
			if _, exists := sourceArtifacts[key]; !exists {
				t.Fatalf("fact %v cloud provenance is not in verified source", fact["candidate_id"])
			}
		}
	}
	if !reflect.DeepEqual(seenFamilies, map[string]bool{"B509": true, "B524": true, "B555": true}) {
		t.Fatalf("graph native identity families = %v", seenFamilies)
	}
	assertCategory(t, Verify(artifacts.NegativeGraphs["forged-b524-opcode.json"], artifacts.SourceBundle, artifacts.SourceReplay), "identity.native")
	assertCategory(t, Verify(artifacts.NegativeGraphs["incomplete-b524-identity.json"], artifacts.SourceBundle, artifacts.SourceReplay), "schema.graph")
}

func TestMSP07EEBusServiceOnlyEvidenceCannotInventEntityOrFeature(t *testing.T) {
	artifacts := pinnedTestArtifactsV1()
	source := decodeObject(t, artifacts.SourceBundle)
	var eebusArtifact map[string]any
	for _, raw := range arrayAt(t, source, "artifacts") {
		artifact := raw.(map[string]any)
		if artifact["source_kind"] == "EEBUS" {
			eebusArtifact = artifact
			break
		}
	}
	if eebusArtifact == nil {
		t.Fatal("canonical MSP-065 source bundle has no eeBUS artifact")
	}
	normalized := objectAt(t, eebusArtifact, "normalized_evidence")
	data := objectAt(t, normalized, "data")
	services := arrayAt(t, data, "services")
	if len(services) != 1 {
		t.Fatalf("eeBUS service count = %d; want 1", len(services))
	}
	service := services[0].(map[string]any)
	keys := make([]string, 0, len(service))
	for key := range service {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	assertStrings(t, "service-only keys", keys, []string{"id", "kind", "paired", "visible"})

	graph := decodeObject(t, artifacts.PositiveGraph)
	for _, raw := range arrayAt(t, graph, "facts") {
		fact := raw.(map[string]any)
		provenance := objectAt(t, fact, "provenance")
		if provenance["eebus"] != nil {
			t.Fatalf("service-only MSP-065 evidence grew an invented entity/feature path on %v", fact["candidate_id"])
		}
	}
	assertCategory(t, Verify(artifacts.NegativeGraphs["forged-eebus-entity-feature.json"], artifacts.SourceBundle, artifacts.SourceReplay), "identity.native")
	assertCategory(t, Verify(artifacts.NegativeGraphs["invalid-eebus-feature-path.json"], artifacts.SourceBundle, artifacts.SourceReplay), "identity.native")
}

func TestMSP07TerminalNegativeAndComparatorStateMatrices(t *testing.T) {
	artifacts := pinnedTestArtifactsV1()
	graph := decodeObject(t, artifacts.PositiveGraph)
	terminalSeen := make(map[string]bool)
	statusSeen := make(map[string]bool)
	for _, raw := range arrayAt(t, graph, "facts") {
		fact := raw.(map[string]any)
		status := fact["status"].(string)
		statusSeen[status] = true
		terminal, _ := fact["terminal_negative_state"].(string)
		comparator := objectAt(t, fact, "comparator")
		outcome := comparator["outcome"].(string)
		if terminal != "" {
			terminalSeen[terminal] = true
			if status != "WITHHELD" || fact["draft_value"] != nil || fact["draft_unit"] != nil || fact["debug_only"] != true {
				t.Fatalf("terminal fact %v is not withheld/null/debug-only", fact["candidate_id"])
			}
			if len(objectAt(t, fact, "retest_trigger")) == 0 {
				t.Fatalf("fact %v has no bounded retest trigger", fact["candidate_id"])
			}
		}
		switch status {
		case "CANDIDATE":
			if outcome != "MATCH" || terminal != "" {
				t.Fatalf("candidate %v outcome=%s terminal=%s", fact["candidate_id"], outcome, terminal)
			}
		case "CONFLICTED":
			if outcome != "CONFLICT" || terminal != "" {
				t.Fatalf("conflicted %v outcome=%s terminal=%s", fact["candidate_id"], outcome, terminal)
			}
		case "RAW_ONLY":
			if outcome != "NOT_EVALUATED" || terminal != "" {
				t.Fatalf("raw-only %v outcome=%s terminal=%s", fact["candidate_id"], outcome, terminal)
			}
		case "WITHHELD":
			if terminal == "CONFLICT" && outcome != "CONFLICT" {
				t.Fatalf("terminal conflict %v outcome=%s", fact["candidate_id"], outcome)
			}
		}
	}
	if !reflect.DeepEqual(statusSeen, map[string]bool{"RAW_ONLY": true, "WITHHELD": true}) {
		t.Fatalf("status coverage = %v", statusSeen)
	}
	if !reflect.DeepEqual(terminalSeen, map[string]bool{"NO_SIGNAL": true, "CLOUD_ONLY": true, "NOT_TESTED": true}) {
		t.Fatalf("terminal coverage = %v", terminalSeen)
	}
	assertCategory(t, Verify(artifacts.NegativeGraphs["terminal-state-not-withheld.json"], artifacts.SourceBundle, artifacts.SourceReplay), "state.terminal")
	assertCategory(t, Verify(artifacts.NegativeGraphs["comparator-parameter-invalid.json"], artifacts.SourceBundle, artifacts.SourceReplay), "comparator.invalid")
}

func jsonKey(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
