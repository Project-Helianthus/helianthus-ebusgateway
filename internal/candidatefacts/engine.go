package candidatefacts

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

func Build(input BuildInputV1) ([]byte, error) {
	sourceBundle, sourceReplay, err := verifySourceInputs(input.SourceBundle, input.SourceReplay)
	if err != nil {
		return nil, err
	}
	registry, registryRaw, err := loadRegistryV1()
	if err != nil {
		return nil, err
	}
	draftsValue, err := typedValue(input.ComparatorDrafts)
	if err != nil {
		return nil, fail("schema.graph")
	}
	drafts, ok := arrayValue(draftsValue)
	if !ok {
		return nil, fail("schema.graph")
	}
	factsValue, err := typedValue(input.Facts)
	if err != nil {
		return nil, fail("schema.graph")
	}
	facts, ok := arrayValue(factsValue)
	if !ok {
		return nil, fail("schema.graph")
	}

	sourceRefs, ok := arrayValue(sourceBundle["evidence_refs"])
	if !ok {
		return nil, fail("provenance.binding")
	}
	sourceRefs = append([]any(nil), sourceRefs...)
	sort.SliceStable(sourceRefs, func(i, j int) bool { return evidenceRefLess(sourceRefs[i], sourceRefs[j]) })
	canonicalSourceReplay, err := canonicalJSON(sourceReplay)
	if err != nil {
		return nil, fail("provenance.binding")
	}
	registryDigest := sha256.Sum256(registryRaw)
	graph := map[string]any{
		"contract": ContractV1, "schema_version": number(1),
		"graph_id": "dcfgv1:sha256:" + zeroDigest(), "graph_hash": "sha256:" + zeroDigest(),
		"registry": map[string]any{
			"contract": RegistryContractV1, "version": number(1), "digest": "sha256:" + hex.EncodeToString(registryDigest[:]),
		},
		"source_bundle": map[string]any{
			"contract": sourceBundle["contract"], "schema_version": sourceBundle["schema_version"],
			"bundle_id": sourceBundle["bundle_id"], "bundle_hash": sourceBundle["bundle_hash"],
			"replay_hash": "sha256:" + domainHex(sourceReplayDomainV1, canonicalSourceReplay), "evidence_refs": sourceRefs,
		},
		"visibility": map[string]any{
			"channel": registry["candidate_channel"], "promotion_state": "NOT_PROMOTED", "stable_exposure": false,
			"command_capable": false, "protocol_translation": false,
		},
		"limits": registry["limits"], "comparator_drafts": drafts, "facts": facts,
	}
	normalizeBuildOrdering(graph)
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		view := cloneObject(fact)
		delete(view, "fact_hash")
		canonical, err := canonicalJSON(view)
		if err != nil {
			return nil, err
		}
		fact["fact_hash"] = "sha256:" + domainHex(factDomainV1, canonical)
	}
	view := cloneObject(graph)
	delete(view, "graph_id")
	delete(view, "graph_hash")
	canonicalView, err := canonicalJSON(view)
	if err != nil {
		return nil, err
	}
	hexdigest := domainHex(graphDomainV1, canonicalView)
	graph["graph_id"] = "dcfgv1:sha256:" + hexdigest
	graph["graph_hash"] = "sha256:" + hexdigest
	encoded, err := canonicalJSON(graph)
	if err != nil {
		return nil, err
	}
	if err := verifyGraph(graph, registry, registryRaw, len(encoded), sourceBundle, sourceReplay); err != nil {
		return nil, err
	}
	return encoded, nil
}

func Replay(graphRaw, sourceBundleRaw, sourceReplayRaw []byte) ([]byte, error) {
	if err := Verify(graphRaw, sourceBundleRaw, sourceReplayRaw); err != nil {
		return nil, err
	}
	value, err := parseJSON(graphRaw)
	if err != nil {
		return nil, err
	}
	graph, _ := objectValue(value)
	facts, _ := arrayValue(graph["facts"])
	results := make([]any, 0, len(facts))
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		provenance, _ := objectValue(fact["provenance"])
		refs, _ := arrayValue(provenance["native_evidence_refs"])
		digests := make([]string, 0, len(refs))
		seen := make(map[string]bool, len(refs))
		for _, refRaw := range refs {
			ref, _ := objectValue(refRaw)
			digest := asString(ref["digest"])
			if !seen[digest] {
				seen[digest] = true
				digests = append(digests, digest)
			}
		}
		sort.Strings(digests)
		comparator, _ := objectValue(fact["comparator"])
		results = append(results, map[string]any{
			"candidate_id": fact["candidate_id"], "proposed_path": fact["proposed_path"], "status": fact["status"],
			"terminal_negative_state": fact["terminal_negative_state"], "confidence": fact["confidence"],
			"comparator_outcome": comparator["outcome"], "fact_hash": fact["fact_hash"], "native_evidence_digests": digests,
		})
	}
	registry, _ := objectValue(graph["registry"])
	bundle, _ := objectValue(graph["source_bundle"])
	replay := map[string]any{
		"contract": ReplayContractV1, "schema_version": number(1),
		"replay_id": "dcfrv1:sha256:" + zeroDigest(), "replay_hash": "sha256:" + zeroDigest(),
		"graph_id": graph["graph_id"], "graph_hash": graph["graph_hash"], "registry_digest": registry["digest"],
		"source_bundle": map[string]any{"bundle_id": bundle["bundle_id"], "bundle_hash": bundle["bundle_hash"], "replay_hash": bundle["replay_hash"]},
		"results":       results,
	}
	view := cloneObject(replay)
	delete(view, "replay_id")
	delete(view, "replay_hash")
	canonicalView, err := canonicalJSON(view)
	if err != nil {
		return nil, err
	}
	hexdigest := domainHex(replayDomainV1, canonicalView)
	replay["replay_id"] = "dcfrv1:sha256:" + hexdigest
	replay["replay_hash"] = "sha256:" + hexdigest
	encoded, err := canonicalJSON(replay)
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func normalizeBuildOrdering(graph map[string]any) {
	bundle, _ := objectValue(graph["source_bundle"])
	refs, _ := arrayValue(bundle["evidence_refs"])
	sort.SliceStable(refs, func(i, j int) bool { return evidenceRefLess(refs[i], refs[j]) })
	facts, _ := arrayValue(graph["facts"])
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		provenance, _ := objectValue(fact["provenance"])
		factRefs, _ := arrayValue(provenance["native_evidence_refs"])
		sort.SliceStable(factRefs, func(i, j int) bool { return evidenceRefLess(factRefs[i], factRefs[j]) })
		comparator, _ := objectValue(fact["comparator"])
		samples, _ := arrayValue(comparator["samples"])
		sort.SliceStable(samples, func(i, j int) bool { return sampleLess(samples[i], samples[j]) })
		trigger, _ := objectValue(fact["retest_trigger"])
		required, ok := arrayValue(trigger["required_source_kinds"])
		if ok {
			sort.SliceStable(required, func(i, j int) bool { return asString(required[i]) < asString(required[j]) })
		}
	}
	sort.SliceStable(facts, func(i, j int) bool { return factLess(facts[i], facts[j]) })
}

func typedValue(value any) (any, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return parseJSON(encoded.Bytes())
}

func zeroDigest() string { return string(bytes.Repeat([]byte{'0'}, 64)) }
