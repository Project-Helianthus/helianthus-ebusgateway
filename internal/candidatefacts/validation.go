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
)

var (
	digestPattern         = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	bundleIDPattern       = regexp.MustCompile(`^sebv1:sha256:[0-9a-f]{64}$`)
	candidateIDPattern    = regexp.MustCompile(`^m7-candidate-[0-9]{4}$`)
	pathPattern           = regexp.MustCompile(`^/[a-z0-9_]+(?:/[a-z0-9_]+)*$`)
	commitPattern         = regexp.MustCompile(`^[0-9a-f]{40}$`)
	timePattern           = regexp.MustCompile(`^(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$`)
	publicEvidencePattern = regexp.MustCompile(`^public-evidence:sha256:[0-9a-f]{64}$`)
)

func Verify(graphRaw, sourceBundleRaw, sourceReplayRaw []byte) error {
	value, err := parseJSON(graphRaw)
	if err != nil {
		return err
	}
	graph, ok := objectValue(value)
	if !ok {
		return fail("schema.graph")
	}
	if err := schemaCheck(graph); err != nil {
		return err
	}
	registry, registryRaw, err := loadRegistryV1()
	if err != nil {
		return err
	}
	if err := checkLimits(graph, len(graphRaw)); err != nil {
		return err
	}
	if err := checkRegistryBinding(graph, registry, registryRaw); err != nil {
		return err
	}
	sourceBundle, sourceReplay, err := verifySourceInputs(sourceBundleRaw, sourceReplayRaw)
	if err != nil {
		return err
	}
	return verifyGraphAfterRegistry(graph, registry, sourceBundle, sourceReplay)
}

func decodeGraphV1(raw []byte) (GraphV1, error) {
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
	if err := checkLimits(graph, len(raw)); err != nil {
		return GraphV1{}, err
	}
	return decodeTypedGraph(graph)
}

func verifyGraphAfterRegistry(graph, registry, sourceBundle, sourceReplay map[string]any) error {
	checks := []func() error{
		func() error { return checkProvenance(graph, registry, sourceBundle, sourceReplay) },
		func() error { return checkIdentities(graph, sourceBundle) },
		func() error { return checkOrdering(graph) },
		func() error { return checkStates(graph, registry) },
		func() error { return checkComparators(graph, registry, sourceBundle) },
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
	raw := pinnedArtifactsV1().Registry
	value, err := parseJSONBounded(raw, "registry.binding", "registry.binding")
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
	suppliedValue, err := parseJSONBounded(replayRaw, "provenance.binding", "provenance.binding")
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
	bundleValue, err := parseJSONBounded(bundleRaw, "provenance.binding", "provenance.binding")
	if err != nil {
		return nil, nil, fail("provenance.binding")
	}
	bundle, ok := objectValue(bundleValue)
	if !ok {
		return nil, nil, fail("provenance.binding")
	}
	return bundle, supplied, nil
}

func checkLimits(graph map[string]any, rawSize int) error {
	limits, ok := objectValue(graph["limits"])
	if !ok || len(limits) != len(hardLimitsV1) {
		return fail("limits.exceeded")
	}
	for key, expected := range hardLimitsV1 {
		got, valid := unsigned(limits[key])
		if !valid || got != expected {
			return fail("limits.exceeded")
		}
	}
	if uint64(rawSize) > hardLimitsV1["max_graph_bytes"] {
		return fail("limits.exceeded")
	}
	if err := checkPortable(graph, hardLimitsV1); err != nil {
		return fail("limits.exceeded")
	}
	facts, _ := arrayValue(graph["facts"])
	if uint64(len(facts)) > hardLimitsV1["max_facts"] {
		return fail("limits.exceeded")
	}
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		provenance, _ := objectValue(fact["provenance"])
		refs, _ := arrayValue(provenance["native_evidence_refs"])
		comparator, _ := objectValue(fact["comparator"])
		samples, _ := arrayValue(comparator["samples"])
		path, _ := stringValue(fact["proposed_path"])
		if uint64(len(refs)) > hardLimitsV1["max_evidence_refs_per_fact"] ||
			uint64(len(samples)) > hardLimitsV1["max_samples_per_comparator"] ||
			uint64(countPathSegments(path)) > hardLimitsV1["max_path_segments"] {
			return fail("limits.exceeded")
		}
	}
	return nil
}

func checkRegistryBinding(graph, registry map[string]any, registryRaw []byte) error {
	binding, ok := objectValue(graph["registry"])
	if !ok {
		return fail("registry.binding")
	}
	digest := sha256.Sum256(registryRaw)
	if !reflect.DeepEqual(binding, map[string]any{
		"contract": RegistryContractV1,
		"version":  number(1),
		"digest":   "sha256:" + hex.EncodeToString(digest[:]),
	}) {
		return fail("registry.binding")
	}
	if registry["contract"] != RegistryContractV1 || !numberEquals(registry["version"], 1) {
		return fail("registry.binding")
	}
	candidate, candidateOK := objectValue(registry["candidate_contract"])
	source, sourceOK := objectValue(registry["source_contract"])
	if !candidateOK || !sourceOK ||
		candidate["contract"] != ContractV1 || !numberEquals(candidate["schema_version"], 1) ||
		source["contract"] != syncevidence.BundleContractV1 || !numberEquals(source["schema_version"], 1) ||
		source["owner_commit"] != PinnedContractV1().SourceOwnerCommit ||
		source["schema_sha256"] != PinnedContractV1().SourceSchemaSHA256 ||
		source["source_registry_sha256"] != PinnedContractV1().SourceRegistrySHA256 ||
		source["replay_digest_algorithm"] != "SHA256_JCS_DOMAIN_V1" ||
		source["replay_digest_domain"] != sourceReplayDomainV1 {
		return fail("registry.binding")
	}
	limits, limitsOK := objectValue(registry["limits"])
	if !limitsOK || len(limits) != len(hardLimitsV1) {
		return fail("registry.binding")
	}
	for key, expected := range hardLimitsV1 {
		if got, ok := unsigned(limits[key]); !ok || got != expected {
			return fail("registry.binding")
		}
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
	if bundle["contract"] != sourceContract["contract"] ||
		!reflect.DeepEqual(bundle["schema_version"], sourceContract["schema_version"]) ||
		!bundleIDOK || !bundleIDPattern.MatchString(bundleID) ||
		!bundleHashOK || !digestPattern.MatchString(bundleHash) ||
		strings.TrimPrefix(bundleID, "sebv1:") != bundleHash ||
		!replayHashOK || !digestPattern.MatchString(replayHash) || !refsOK || len(refs) == 0 {
		return fail("provenance.binding")
	}
	canonicalReplay, err := canonicalJSON(sourceReplay)
	if err != nil {
		return fail("provenance.binding")
	}
	expectedReplayHash := "sha256:" + domainHex(sourceReplayDomainV1, canonicalReplay)
	if bundle["contract"] != sourceBundle["contract"] ||
		!reflect.DeepEqual(bundle["schema_version"], sourceBundle["schema_version"]) ||
		bundleID != sourceBundle["bundle_id"] || bundleHash != sourceBundle["bundle_hash"] ||
		replayHash != expectedReplayHash || !reflect.DeepEqual(refs, sourceBundle["evidence_refs"]) {
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
		factRefs, _ := arrayValue(provenance["native_evidence_refs"])
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

		selected := make([]map[string]any, 0, 3)
		for _, fields := range [][3]string{
			{"ebus_source_id", "ebus_artifact_id", "EBUS"},
			{"eebus_source_id", "eebus_artifact_id", "EEBUS"},
		} {
			sourceID, artifactID := provenance[fields[0]], provenance[fields[1]]
			if sourceID == nil && artifactID == nil {
				continue
			}
			key, pairOK := pairKey(sourceID, artifactID)
			source, sourceOK := sources[asString(sourceID)]
			artifact, artifactOK := artifacts[key]
			if !pairOK || !sourceOK || !artifactOK ||
				source["source_kind"] != fields[2] || artifact["source_kind"] != fields[2] ||
				!containsString(source["artifact_ids"], asString(artifactID)) {
				return fail("provenance.binding")
			}
			selected = append(selected, artifact)
		}
		if cloudRaw := provenance["cloud"]; cloudRaw != nil {
			cloud, _ := objectValue(cloudRaw)
			key, pairOK := pairKey(cloud["source_id"], cloud["artifact_id"])
			source, sourceOK := sources[asString(cloud["source_id"])]
			artifact, artifactOK := artifacts[key]
			if !pairOK || !sourceOK || !artifactOK ||
				source["source_kind"] != "CLOUD_APP" || artifact["source_kind"] != "CLOUD_APP" ||
				!containsString(source["artifact_ids"], asString(cloud["artifact_id"])) {
				return fail("provenance.binding")
			}
			evidenceID, ok := stringValue(cloud["evidence_id"])
			if !ok || !publicEvidencePattern.MatchString(evidenceID) {
				return fail("provenance.binding")
			}
			wantDigest := strings.TrimPrefix(evidenceID, "public-evidence:")
			if !artifactHasEvidenceDigest(artifact, wantDigest) {
				return fail("provenance.binding")
			}
			selected = append(selected, artifact)
		}
		for _, artifact := range selected {
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
		if err := checkSampleProvenance(fact, artifacts, seen); err != nil {
			return err
		}
		status := asString(fact["status"])
		terminal, _ := optionalString(fact["terminal_negative_state"])
		comparator, _ := objectValue(fact["comparator"])
		samples, _ := arrayValue(comparator["samples"])
		if status == "CANDIDATE" || status == "CONFLICTED" || terminal == "CONFLICT" {
			if provenance["ebus"] == nil || provenance["eebus"] == nil ||
				provenance["ebus_source_id"] == nil || provenance["eebus_source_id"] == nil ||
				len(samples) == 0 {
				return fail("provenance.binding")
			}
		}
	}
	return nil
}

func checkSampleProvenance(fact map[string]any, artifacts map[string]map[string]any, factRefs map[string]bool) error {
	provenance, _ := objectValue(fact["provenance"])
	comparator, _ := objectValue(fact["comparator"])
	samples, _ := arrayValue(comparator["samples"])
	for _, sampleRaw := range samples {
		sample, _ := objectValue(sampleRaw)
		for _, sideSpec := range []struct {
			field, kind, sourceField, artifactField string
		}{
			{"left", "EBUS", "ebus_source_id", "ebus_artifact_id"},
			{"right", "EEBUS", "eebus_source_id", "eebus_artifact_id"},
		} {
			side, ok := objectValue(sample[sideSpec.field])
			if !ok || side["source_kind"] != sideSpec.kind ||
				side["source_id"] != provenance[sideSpec.sourceField] ||
				side["artifact_id"] != provenance[sideSpec.artifactField] {
				return fail("provenance.binding")
			}
			key, pairOK := pairKey(side["source_id"], side["artifact_id"])
			artifact, artifactOK := artifacts[key]
			if !pairOK || !artifactOK {
				return fail("provenance.binding")
			}
			refKey, err := canonicalKey(side["evidence_ref"])
			if err != nil || !factRefs[refKey] || !artifactHasEvidenceRef(artifact, refKey) {
				return fail("provenance.binding")
			}
		}
	}
	return nil
}

func artifactHasEvidenceRef(artifact map[string]any, target string) bool {
	refs, ok := arrayValue(artifact["evidence_refs"])
	if !ok {
		return false
	}
	for _, ref := range refs {
		key, err := canonicalKey(ref)
		if err == nil && key == target {
			return true
		}
	}
	return false
}

func artifactHasEvidenceDigest(artifact map[string]any, target string) bool {
	refs, ok := arrayValue(artifact["evidence_refs"])
	if !ok {
		return false
	}
	for _, raw := range refs {
		ref, ok := objectValue(raw)
		if ok && ref["digest"] == target {
			return true
		}
	}
	return false
}
