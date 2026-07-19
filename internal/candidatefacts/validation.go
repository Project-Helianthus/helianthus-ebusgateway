package candidatefacts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"regexp"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
)

const (
	factDomainV1         = "HELIANTHUS:DRAFT-CANDIDATE-FACT:V1"
	graphDomainV1        = "HELIANTHUS:DRAFT-CANDIDATE-FACT-GRAPH:V1"
	replayDomainV1       = "HELIANTHUS:DRAFT-CANDIDATE-FACT-REPLAY:V1"
	sourceReplayDomainV1 = "HELIANTHUS:SYNCHRONIZED-EVIDENCE-REPLAY:V1"
	negativeContractV1   = "helianthus.platform.draft-candidate-fact-negative-fixture.v1"
)

var (
	digestPattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	graphIDPattern     = regexp.MustCompile(`^dcfgv1:sha256:[0-9a-f]{64}$`)
	bundleIDPattern    = regexp.MustCompile(`^sebv1:sha256:[0-9a-f]{64}$`)
	candidateIDPattern = regexp.MustCompile(`^m7-candidate-[0-9]{4}$`)
	pathPattern        = regexp.MustCompile(`^/[a-z0-9_]+(?:/[a-z0-9_]+)*$`)
	commitPattern      = regexp.MustCompile(`^[0-9a-f]{40}$`)
	timePattern        = regexp.MustCompile(`^(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$`)
)

var rootKeysV1 = []string{
	"contract", "schema_version", "graph_id", "graph_hash", "registry", "source_bundle",
	"visibility", "limits", "comparator_drafts", "facts",
}

func Verify(graphRaw, sourceBundleRaw, sourceReplayRaw []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = fail("schema.graph")
		}
	}()
	value, parseErr := parseJSON(graphRaw)
	if parseErr != nil {
		return parseErr
	}
	graph, ok := objectValue(value)
	if !ok {
		return fail("schema.graph")
	}
	graph, err = expandNegativeFixture(graph)
	if err != nil {
		return err
	}
	registry, registryRaw, err := loadRegistryV1()
	if err != nil {
		return err
	}
	sourceBundle, sourceReplay, err := verifySourceInputs(sourceBundleRaw, sourceReplayRaw)
	if err != nil {
		return err
	}
	return verifyGraph(graph, registry, registryRaw, len(graphRaw), sourceBundle, sourceReplay)
}

func DecodeGraphV1(raw []byte) (GraphV1, error) {
	value, err := parseJSON(raw)
	if err != nil {
		return GraphV1{}, err
	}
	graph, ok := objectValue(value)
	if !ok {
		return GraphV1{}, fail("schema.graph")
	}
	if err := schemaCheck(graph); err != nil {
		return GraphV1{}, err
	}
	return decodeTypedGraph(graph)
}

func verifyGraph(graph, registry map[string]any, registryRaw []byte, rawSize int, sourceBundle, sourceReplay map[string]any) error {
	checks := []func() error{
		func() error { return schemaCheck(graph) },
		func() error { return checkLimits(graph, registry, rawSize) },
		func() error { return checkRegistryBinding(graph, registry, registryRaw) },
		func() error { return checkProvenance(graph, registry, sourceBundle, sourceReplay) },
		func() error { return checkIdentities(graph, sourceBundle) },
		func() error { return checkOrdering(graph) },
		func() error { return checkStates(graph, registry) },
		func() error { return checkComparators(graph, registry) },
		func() error { return checkAntiLeak(graph, registry) },
		func() error { return checkHashes(graph) },
	}
	for _, check := range checks {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func loadRegistryV1() (map[string]any, []byte, error) {
	raw := PinnedArtifactsV1().Registry
	value, err := parseJSON(raw)
	if err != nil {
		return nil, nil, fail("registry.binding")
	}
	registry, ok := objectValue(value)
	if !ok {
		return nil, nil, fail("registry.binding")
	}
	return registry, raw, nil
}

func verifySourceInputs(bundleRaw, replayRaw []byte) (map[string]any, map[string]any, error) {
	generated, err := syncevidence.Replay(bundleRaw)
	if err != nil {
		return nil, nil, fail("provenance.binding")
	}
	suppliedValue, err := parseJSON(replayRaw)
	if err != nil {
		return nil, nil, fail("provenance.binding")
	}
	supplied, ok := objectValue(suppliedValue)
	if !ok {
		return nil, nil, fail("provenance.binding")
	}
	suppliedCanonical, err := canonicalJSON(supplied)
	if err != nil || !bytes.Equal(generated, suppliedCanonical) {
		return nil, nil, fail("provenance.binding")
	}
	bundleValue, err := parseJSON(bundleRaw)
	if err != nil {
		return nil, nil, fail("provenance.binding")
	}
	bundle, ok := objectValue(bundleValue)
	if !ok {
		return nil, nil, fail("provenance.binding")
	}
	return bundle, supplied, nil
}

func schemaCheck(graph map[string]any) error {
	if !exactKeys(graph, rootKeysV1...) || graph["contract"] != ContractV1 || !numberEquals(graph["schema_version"], 1) {
		return fail("schema.graph")
	}
	graphID, graphIDOK := stringValue(graph["graph_id"])
	graphHash, graphHashOK := stringValue(graph["graph_hash"])
	if !graphIDOK || !graphIDPattern.MatchString(graphID) || !graphHashOK || !digestPattern.MatchString(graphHash) {
		return fail("schema.graph")
	}
	if !exactKeys(graph["registry"], "contract", "version", "digest") ||
		!exactKeys(graph["source_bundle"], "contract", "schema_version", "bundle_id", "bundle_hash", "replay_hash", "evidence_refs") ||
		!exactKeys(graph["visibility"], "channel", "promotion_state", "stable_exposure", "command_capable", "protocol_translation") ||
		!exactKeys(graph["limits"], "max_graph_bytes", "max_depth", "max_facts", "max_evidence_refs_per_fact", "max_samples_per_comparator", "max_string_bytes", "max_path_segments") {
		return fail("schema.graph")
	}
	limits, _ := objectValue(graph["limits"])
	for _, key := range []string{"max_graph_bytes", "max_depth", "max_facts", "max_evidence_refs_per_fact", "max_samples_per_comparator", "max_string_bytes", "max_path_segments"} {
		if _, ok := unsigned(limits[key]); !ok {
			return fail("schema.graph")
		}
	}
	drafts, draftsOK := arrayValue(graph["comparator_drafts"])
	facts, factsOK := arrayValue(graph["facts"])
	if !draftsOK || len(drafts) == 0 || !factsOK || len(facts) == 0 {
		return fail("schema.graph")
	}
	for _, raw := range drafts {
		if !exactKeys(raw, "draft_id", "type", "parameters") {
			return fail("schema.graph")
		}
		draft, _ := objectValue(raw)
		if !exactKeys(draft["parameters"], "window", "tolerance", "unit_conversion", "rounding", "minimum_samples", "maximum_missing_samples", "stale_cutoff_ns", "conflict_threshold") {
			return fail("schema.graph")
		}
	}
	for _, raw := range facts {
		if !exactKeys(raw, "candidate_id", "proposed_path", "draft_value", "draft_unit", "status", "terminal_negative_state", "confidence", "provenance", "comparator", "falsifier", "retest_trigger", "debug_only", "fact_hash") {
			return fail("schema.graph")
		}
		fact, _ := objectValue(raw)
		if !exactKeys(fact["confidence"], "level", "basis", "score_milli") ||
			!exactKeys(fact["provenance"], "source_bundle_id", "native_evidence_refs", "ebus_source_id", "ebus_artifact_id", "ebus", "eebus_source_id", "eebus_artifact_id", "eebus", "cloud") ||
			!exactKeys(fact["comparator"], "draft_id", "samples", "outcome") ||
			!exactKeys(fact["falsifier"], "condition_code", "expected_terminal_state", "description") ||
			!exactKeys(fact["retest_trigger"], "trigger_code", "required_source_kinds", "minimum_new_samples") {
			return fail("schema.graph")
		}
		comparator, _ := objectValue(fact["comparator"])
		samples, ok := arrayValue(comparator["samples"])
		if !ok {
			return fail("schema.graph")
		}
		for _, sample := range samples {
			if !exactKeys(sample, "offset_ns", "left_decimal", "right_decimal", "state") {
				return fail("schema.graph")
			}
		}
	}
	return nil
}

func checkLimits(graph, registry map[string]any, rawSize int) error {
	limits, _ := objectValue(graph["limits"])
	registryLimits, ok := objectValue(registry["limits"])
	if !ok || !reflect.DeepEqual(limits, registryLimits) {
		return fail("limits.exceeded")
	}
	parsed := make(map[string]uint64, len(limits))
	for key, value := range limits {
		number, ok := unsigned(value)
		if !ok {
			return fail("limits.exceeded")
		}
		parsed[key] = number
	}
	facts, _ := arrayValue(graph["facts"])
	if uint64(rawSize) > parsed["max_graph_bytes"] || uint64(len(facts)) > parsed["max_facts"] {
		return fail("limits.exceeded")
	}
	if err := checkPortable(graph, 0, parsed); err != nil {
		return err
	}
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		provenance, _ := objectValue(fact["provenance"])
		refs, _ := arrayValue(provenance["native_evidence_refs"])
		comparator, _ := objectValue(fact["comparator"])
		samples, _ := arrayValue(comparator["samples"])
		path, ok := stringValue(fact["proposed_path"])
		if !ok || uint64(len(refs)) > parsed["max_evidence_refs_per_fact"] || uint64(len(samples)) > parsed["max_samples_per_comparator"] || uint64(countPathSegments(path)) > parsed["max_path_segments"] {
			return fail("limits.exceeded")
		}
	}
	return nil
}

func checkRegistryBinding(graph, registry map[string]any, registryRaw []byte) error {
	binding, _ := objectValue(graph["registry"])
	digest := sha256.Sum256(registryRaw)
	if binding["contract"] != RegistryContractV1 || !numberEquals(binding["version"], 1) || binding["digest"] != "sha256:"+hex.EncodeToString(digest[:]) {
		return fail("registry.binding")
	}
	if !exactKeys(registry, "contract", "version", "candidate_contract", "source_contract", "statuses", "terminal_negative_states", "comparators", "candidate_channel", "forbidden_surfaces", "validation_precedence", "limits") ||
		registry["contract"] != RegistryContractV1 || !numberEquals(registry["version"], 1) {
		return fail("registry.binding")
	}
	candidate, okCandidate := objectValue(registry["candidate_contract"])
	source, okSource := objectValue(registry["source_contract"])
	if !okCandidate || !okSource || candidate["contract"] != ContractV1 || source["contract"] != syncevidence.BundleContractV1 ||
		source["replay_digest_algorithm"] != "SHA256_JCS_DOMAIN_V1" || source["replay_digest_domain"] != sourceReplayDomainV1 {
		return fail("registry.binding")
	}
	return nil
}

func checkProvenance(graph, registry, sourceBundle, sourceReplay map[string]any) error {
	bundle, _ := objectValue(graph["source_bundle"])
	sourceContract, _ := objectValue(registry["source_contract"])
	bundleID, bundleIDOK := stringValue(bundle["bundle_id"])
	bundleHash, bundleHashOK := stringValue(bundle["bundle_hash"])
	replayHash, replayHashOK := stringValue(bundle["replay_hash"])
	refs, refsOK := arrayValue(bundle["evidence_refs"])
	if bundle["contract"] != sourceContract["contract"] || !reflect.DeepEqual(bundle["schema_version"], sourceContract["schema_version"]) ||
		!bundleIDOK || !bundleIDPattern.MatchString(bundleID) || !bundleHashOK || !digestPattern.MatchString(bundleHash) ||
		strings.TrimPrefix(bundleID, "sebv1:") != bundleHash || !replayHashOK || !digestPattern.MatchString(replayHash) || !refsOK || len(refs) == 0 {
		return fail("provenance.binding")
	}
	canonicalReplay, err := canonicalJSON(sourceReplay)
	if err != nil {
		return fail("provenance.binding")
	}
	expectedReplayHash := "sha256:" + domainHex(sourceReplayDomainV1, canonicalReplay)
	if bundle["contract"] != sourceBundle["contract"] || !reflect.DeepEqual(bundle["schema_version"], sourceBundle["schema_version"]) ||
		bundleID != sourceBundle["bundle_id"] || bundleHash != sourceBundle["bundle_hash"] || replayHash != expectedReplayHash ||
		!reflect.DeepEqual(refs, sourceBundle["evidence_refs"]) {
		return fail("provenance.binding")
	}
	rootRefs := make(map[string]bool, len(refs))
	for _, ref := range refs {
		if err := validateEvidenceRef(ref); err != nil {
			return err
		}
		key, err := canonicalKey(ref)
		if err != nil || rootRefs[key] {
			return fail("provenance.binding")
		}
		rootRefs[key] = true
	}
	sources := indexSources(sourceBundle)
	artifacts := indexArtifacts(sourceBundle)
	facts, _ := arrayValue(graph["facts"])
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		provenance, _ := objectValue(fact["provenance"])
		if provenance["source_bundle_id"] != bundleID {
			return fail("provenance.binding")
		}
		factRefs, ok := arrayValue(provenance["native_evidence_refs"])
		if !ok || len(factRefs) == 0 {
			return fail("provenance.binding")
		}
		seen := make(map[string]bool, len(factRefs))
		for _, ref := range factRefs {
			if err := validateEvidenceRef(ref); err != nil {
				return err
			}
			key, err := canonicalKey(ref)
			if err != nil || !rootRefs[key] || seen[key] {
				return fail("provenance.binding")
			}
			seen[key] = true
		}
		referenced := make([]map[string]any, 0, 3)
		for _, fields := range [][3]string{{"ebus_source_id", "ebus_artifact_id", "EBUS"}, {"eebus_source_id", "eebus_artifact_id", "EEBUS"}} {
			sourceID, artifactID := provenance[fields[0]], provenance[fields[1]]
			if sourceID == nil && artifactID == nil {
				continue
			}
			key, ok := pairKey(sourceID, artifactID)
			source, sourceOK := sources[asString(sourceID)]
			artifact, artifactOK := artifacts[key]
			if !ok || !sourceOK || !artifactOK || source["source_kind"] != fields[2] || artifact["source_kind"] != fields[2] || !containsString(source["artifact_ids"], asString(artifactID)) {
				return fail("provenance.binding")
			}
			referenced = append(referenced, artifact)
		}
		if cloud := provenance["cloud"]; cloud != nil {
			cloudObject, ok := objectValue(cloud)
			if !ok {
				return fail("provenance.binding")
			}
			key, keyOK := pairKey(cloudObject["source_id"], cloudObject["artifact_id"])
			source, sourceOK := sources[asString(cloudObject["source_id"])]
			artifact, artifactOK := artifacts[key]
			if !keyOK || !sourceOK || !artifactOK || source["source_kind"] != "CLOUD_APP" || artifact["source_kind"] != "CLOUD_APP" || !containsString(source["artifact_ids"], asString(cloudObject["artifact_id"])) {
				return fail("provenance.binding")
			}
			referenced = append(referenced, artifact)
		}
		for _, artifact := range referenced {
			artifactRefs, ok := arrayValue(artifact["evidence_refs"])
			if !ok {
				return fail("provenance.binding")
			}
			for _, ref := range artifactRefs {
				key, err := canonicalKey(ref)
				if err != nil || !seen[key] {
					return fail("provenance.binding")
				}
			}
		}
	}
	return nil
}
