package coexistence

import (
	"bytes"
	"reflect"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/candidatefacts"
)

const (
	m7GraphInputSHA        = "b5c5d79e540a1691ee60c6db3e9405a92d9d544d871c74b26800fe449a318b0e"
	m7ReplayInputSHA       = "8280f6278ffe8598dfd767bb5bf9e60dce3c145b4612174b7c5a32fbff282f5c"
	m7RegistryInputSHA     = "e6895b8d7406b58ed97599d8da7e9bd3b252e6e7ca3b0578ec6385bfe6dfe1c0"
	m7SourceBundleInputSHA = "e6db2862f9001148deb6f40e286ee5f1eef2907812685a9b48128ddbfca5ce5a"
	m7SourceReplayInputSHA = "3061c507677f1f41861c20096ff7581ccb6e35c2e01bf66a568e2277df285539"
)

type m7AuthorityV1 struct {
	graph       map[string]any
	replay      map[string]any
	graphBytes  int64
	replayBytes int64
}

type validationContextV1 struct {
	evidence map[string]any
	registry map[string]any
	m7       m7AuthorityV1
}

func Verify(inputs InputsV1) error {
	_, err := validateInputs(inputs)
	return err
}

func validateInputs(inputs InputsV1) (*validationContextV1, error) {
	value, err := parseEvidenceJSON(inputs.Evidence)
	if err != nil {
		return nil, err
	}
	if err := checkParsedResourceLimits(value, len(inputs.Evidence)); err != nil {
		return nil, err
	}
	if err := schemaCheckEvidence(value); err != nil {
		return nil, err
	}
	evidence := value.(map[string]any)
	registry, err := verifyRegistry(inputs.Registry, evidence)
	if err != nil {
		return nil, err
	}
	m7, err := verifyM7(inputs, registry, evidence)
	if err != nil {
		return nil, err
	}
	checks := []func() error{
		func() error { return checkRuntime(evidence, m7) },
		func() error { return checkConfig(evidence) },
		func() error { return checkAuthMask(evidence) },
		func() error { return checkClock(evidence) },
		func() error { return checkOrdering(evidence, registry) },
		func() error { return checkStates(evidence) },
		func() error { return checkViewCoverage(evidence, registry) },
		func() error { return checkNormalization(evidence, registry) },
		func() error { return checkPayloadHashes(evidence, registry) },
		func() error { return checkAntiLeak(evidence) },
		func() error { return checkAuthority(evidence) },
		func() error { return checkScope(evidence, registry) },
		func() error { return checkDrift(evidence, registry) },
		func() error { return checkRollback(evidence, registry) },
		func() error { return checkEvidenceHash(evidence) },
	}
	for _, check := range checks {
		if err := check(); err != nil {
			return nil, err
		}
	}
	return &validationContextV1{evidence: evidence, registry: registry, m7: m7}, nil
}

func verifyRegistry(raw []byte, evidence map[string]any) (map[string]any, error) {
	if rawSHA256V1(raw) != registrySHA256 {
		return nil, fail("registry.binding")
	}
	value, err := parseCategorizedJSON(raw, "registry.binding")
	if err != nil {
		return nil, err
	}
	registry, ok := objectValueV1(value)
	if !ok || !exactKeys(registry,
		"contract", "version", "evidence_contract", "report_contract", "gate", "excluded_gates",
		"m7_completion_token", "m7_docs_source_commit", "m7_binding", "scenario_order", "protected_views",
		"view_rules", "required_acceptance_checks", "validation_precedence", "limits", "fixture_ids",
	) {
		return nil, fail("registry.binding")
	}
	expectedBinding := map[string]any{
		"contract": coexRegistryContractV1,
		"version":  number(1),
		"digest":   "sha256:" + registrySHA256,
	}
	if !reflect.DeepEqual(evidence["registry"], expectedBinding) {
		return nil, fail("registry.binding")
	}
	return registry, nil
}

func verifyRegistryForGeneration(raw []byte) (map[string]any, error) {
	if rawSHA256V1(raw) != registrySHA256 {
		return nil, fail("registry.binding")
	}
	value, err := parseCategorizedJSON(raw, "registry.binding")
	if err != nil {
		return nil, err
	}
	registry, ok := objectValueV1(value)
	if !ok {
		return nil, fail("registry.binding")
	}
	return registry, nil
}

func verifyM7(inputs InputsV1, registry, evidence map[string]any) (m7AuthorityV1, error) {
	if rawSHA256V1(inputs.M7Graph) != m7GraphInputSHA ||
		rawSHA256V1(inputs.M7Replay) != m7ReplayInputSHA ||
		rawSHA256V1(inputs.M7Registry) != m7RegistryInputSHA ||
		rawSHA256V1(inputs.M7SourceBundle) != m7SourceBundleInputSHA ||
		rawSHA256V1(inputs.M7SourceReplay) != m7SourceReplayInputSHA {
		return m7AuthorityV1{}, fail("provenance.m7")
	}
	if err := candidatefacts.Verify(inputs.M7Graph, inputs.M7SourceBundle, inputs.M7SourceReplay); err != nil {
		return m7AuthorityV1{}, fail("provenance.m7")
	}
	generatedReplay, err := candidatefacts.Replay(inputs.M7Graph, inputs.M7SourceBundle, inputs.M7SourceReplay)
	if err != nil || !bytes.Equal(generatedReplay, inputs.M7Replay) {
		return m7AuthorityV1{}, fail("provenance.m7")
	}
	graphValue, err := parseCategorizedJSON(inputs.M7Graph, "provenance.m7")
	if err != nil {
		return m7AuthorityV1{}, fail("provenance.m7")
	}
	replayValue, err := parseCategorizedJSON(inputs.M7Replay, "provenance.m7")
	if err != nil {
		return m7AuthorityV1{}, fail("provenance.m7")
	}
	graph, graphOK := objectValueV1(graphValue)
	replay, replayOK := objectValueV1(replayValue)
	if !graphOK || !replayOK {
		return m7AuthorityV1{}, fail("provenance.m7")
	}
	registryBinding, ok := objectValueV1(registry["m7_binding"])
	if !ok {
		return m7AuthorityV1{}, fail("provenance.m7")
	}
	expected := map[string]any{
		"completion_token":   registry["m7_completion_token"],
		"docs_source_commit": registry["m7_docs_source_commit"],
		"graph_contract":     registryBinding["graph_contract"],
		"graph_id":           registryBinding["graph_id"],
		"graph_hash":         registryBinding["graph_hash"],
		"replay_contract":    registryBinding["replay_contract"],
		"replay_id":          registryBinding["replay_id"],
		"replay_hash":        registryBinding["replay_hash"],
	}
	actual := map[string]any{
		"completion_token":   registry["m7_completion_token"],
		"docs_source_commit": registry["m7_docs_source_commit"],
		"graph_contract":     graph["contract"],
		"graph_id":           graph["graph_id"],
		"graph_hash":         graph["graph_hash"],
		"replay_contract":    replay["contract"],
		"replay_id":          replay["replay_id"],
		"replay_hash":        replay["replay_hash"],
	}
	if !reflect.DeepEqual(actual, expected) || evidence != nil && !reflect.DeepEqual(evidence["m7_binding"], expected) {
		return m7AuthorityV1{}, fail("provenance.m7")
	}
	graphCanonical, graphErr := marshalCanonicalV1(graph)
	replayCanonical, replayErr := marshalCanonicalV1(replay)
	if graphErr != nil || replayErr != nil {
		return m7AuthorityV1{}, fail("provenance.m7")
	}
	return m7AuthorityV1{graph: graph, replay: replay, graphBytes: int64(len(graphCanonical)), replayBytes: int64(len(replayCanonical))}, nil
}

func checkRuntime(evidence map[string]any, m7 m7AuthorityV1) error {
	runs, _ := arrayValueV1(evidence["runs"])
	if len(runs) < 2 {
		return fail("provenance.runtime")
	}
	baseline := runs[0].(map[string]any)["provenance"].(map[string]any)["runtime"].(map[string]any)
	if baseline["source_commit"] != coexBaselineGatewayCommit || baseline["source_parent_commit"] != nil {
		return fail("provenance.runtime")
	}
	compared := runs[1].(map[string]any)["provenance"].(map[string]any)["runtime"].(map[string]any)
	if compared["source_parent_commit"] != coexBaselineGatewayCommit {
		return fail("provenance.runtime")
	}
	for index, rawRun := range runs {
		run := rawRun.(map[string]any)
		provenance := run["provenance"].(map[string]any)
		runtime := provenance["runtime"].(map[string]any)
		if !validRuntimeIdentity(runtime) || index > 0 && !reflect.DeepEqual(runtime, compared) {
			return fail("provenance.runtime")
		}
		views := run["protected_views"].([]any)
		inputs := provenance["immutable_inputs"].([]any)
		viewIDs := make([]string, len(views))
		expected := make(map[string]inputBindingV1, len(views)+2)
		for viewIndex, rawView := range views {
			view := rawView.(map[string]any)
			viewID := view["view_id"].(string)
			viewIDs[viewIndex] = viewID
			payloadBytes, err := marshalCanonicalV1(view["payload"])
			if err != nil {
				return fail("provenance.runtime")
			}
			expected["view:"+viewID] = inputBindingV1{digest: view["raw_payload_hash"].(string), bytes: int64(len(payloadBytes))}
		}
		expected["m7:graph"] = inputBindingV1{digest: m7.graph["graph_hash"].(string), bytes: m7.graphBytes}
		expected["m7:replay"] = inputBindingV1{digest: m7.replay["replay_hash"].(string), bytes: m7.replayBytes}
		inputIDs := make([]string, len(inputs))
		actual := make(map[string]inputBindingV1, len(inputs))
		for inputIndex, rawInput := range inputs {
			input := rawInput.(map[string]any)
			inputID := input["input_id"].(string)
			inputIDs[inputIndex] = inputID
			length, _ := integerValue(input["byte_length"])
			actual[inputID] = inputBindingV1{digest: input["digest"].(string), bytes: length}
		}
		expectedIDs := make([]string, 0, len(viewIDs)+2)
		for _, viewID := range viewIDs {
			expectedIDs = append(expectedIDs, "view:"+viewID)
		}
		expectedIDs = append(expectedIDs, "m7:graph", "m7:replay")
		if uniqueStrings(viewIDs) && uniqueStrings(inputIDs) && sameStringSet(inputIDs, expectedIDs) && !reflect.DeepEqual(actual, expected) {
			return fail("provenance.runtime")
		}
	}
	return nil
}

type inputBindingV1 struct {
	digest string
	bytes  int64
}

func validRuntimeIdentity(runtime map[string]any) bool {
	source, sourceOK := stringValueV1(runtime["source_commit"])
	digest, digestOK := stringValueV1(runtime["artifact_digest"])
	artifactID, artifactOK := stringValueV1(runtime["artifact_id"])
	size, sizeOK := integerValue(runtime["artifact_size_bytes"])
	manifest, manifestOK := objectValueV1(runtime["build_manifest"])
	manifestHash, hashOK := stringValueV1(runtime["build_manifest_hash"])
	computed, err := domainDigestV1(coexBuildDomainV1, manifest)
	return sourceOK && shaPatternV1.MatchString(source) && digestOK && digestPatternV1.MatchString(digest) &&
		artifactOK && artifactID == "gateway:"+digest && sizeOK && size > 0 && manifestOK && hashOK && err == nil &&
		manifestHash == computed && runtime["repository"] == "github.com/Project-Helianthus/helianthus-ebusgateway"
}

func checkConfig(evidence map[string]any) error {
	runs, _ := arrayValueV1(evidence["runs"])
	for _, rawRun := range runs {
		run := rawRun.(map[string]any)
		config := run["provenance"].(map[string]any)["config"].(map[string]any)
		payload := config["payload"].(map[string]any)
		computed, err := domainDigestV1(coexConfigDomainV1, payload)
		outbound, _ := boolValueV1(payload["outbound_enabled"])
		publicV2, _ := boolValueV1(payload["public_v2_enabled"])
		if err != nil || config["config_hash"] != computed || outbound || publicV2 {
			return fail("provenance.config")
		}
	}
	return nil
}

func checkAuthMask(evidence map[string]any) error {
	profile := evidence["normalization"].(map[string]any)
	profileDigest, err := domainDigestV1(coexNormalizationDomainV1, withoutJSONKeysV1(profile, "profile_digest"))
	if err != nil || profile["profile_digest"] != profileDigest {
		return fail("provenance.auth_mask")
	}
	runs, _ := arrayValueV1(evidence["runs"])
	firstAuth := runs[0].(map[string]any)["provenance"].(map[string]any)["auth_scope"].(map[string]any)
	for _, rawRun := range runs {
		provenance := rawRun.(map[string]any)["provenance"].(map[string]any)
		auth := provenance["auth_scope"].(map[string]any)
		authDigest, digestErr := domainDigestV1(coexAuthDomainV1, withoutJSONKeysV1(auth, "scope_hash"))
		if digestErr != nil || !reflect.DeepEqual(auth, firstAuth) || auth["principal_class"] != "READ_ONLY_TEST" ||
			auth["scope_hash"] != authDigest || provenance["mask_scope_digest"] != profileDigest {
			return fail("provenance.auth_mask")
		}
	}
	return nil
}

func checkClock(evidence map[string]any) error {
	clock := evidence["capture_clock"].(map[string]any)
	computed, err := domainDigestV1(coexClockDomainV1, withoutJSONKeysV1(clock, "clock_hash"))
	wall, _ := stringValueV1(clock["wall_anchor_utc"])
	verification, verificationOK := integerValue(clock["verification_offset_ns"])
	maximumAge, ageOK := integerValue(clock["max_capture_age_ns"])
	if err != nil || clock["basis"] != "MONOTONIC_CAPTURE_OFFSETS" || !validRFC3339UTC(wall) ||
		clock["clock_hash"] != computed || !verificationOK || verification < 0 || !ageOK || maximumAge < 1 {
		return fail("provenance.clock")
	}
	runs, _ := arrayValueV1(evidence["runs"])
	for _, rawRun := range runs {
		run := rawRun.(map[string]any)
		offset, _ := integerValue(run["capture_offset_ns"])
		provenance := run["provenance"].(map[string]any)
		if provenance["capture_clock_id"] != clock["clock_id"] || offset > verification || verification-offset > maximumAge {
			return fail("provenance.clock")
		}
	}
	return nil
}

func checkOrdering(evidence, registry map[string]any) error {
	runs, _ := arrayValueV1(evidence["runs"])
	states := make([]string, len(runs))
	runIDs := make([]string, len(runs))
	offsets := make([]int64, len(runs))
	for index, rawRun := range runs {
		run := rawRun.(map[string]any)
		states[index] = run["state"].(string)
		runIDs[index] = run["run_id"].(string)
		offsets[index], _ = integerValue(run["capture_offset_ns"])
	}
	wantStates, _ := stringsFromArray(registry["scenario_order"])
	if !reflect.DeepEqual(states, wantStates) || !uniqueStrings(runIDs) || !strictlyIncreasing(offsets) {
		return fail("ordering.duplicate")
	}
	wantViews, _ := stringsFromArray(registry["protected_views"])
	for _, rawRun := range runs {
		run := rawRun.(map[string]any)
		views := run["protected_views"].([]any)
		viewIDs := make([]string, len(views))
		for index, rawView := range views {
			viewIDs[index] = rawView.(map[string]any)["view_id"].(string)
		}
		inputs := run["provenance"].(map[string]any)["immutable_inputs"].([]any)
		inputIDs := make([]string, len(inputs))
		for index, rawInput := range inputs {
			inputIDs[index] = rawInput.(map[string]any)["input_id"].(string)
		}
		expectedInputs := make([]string, 0, len(viewIDs)+2)
		for _, viewID := range viewIDs {
			expectedInputs = append(expectedInputs, "view:"+viewID)
		}
		expectedInputs = append(expectedInputs, "m7:graph", "m7:replay")
		if !uniqueStrings(viewIDs) || !uniqueStrings(inputIDs) || !reflect.DeepEqual(inputIDs, expectedInputs) ||
			len(viewIDs) == len(wantViews) && !reflect.DeepEqual(viewIDs, wantViews) {
			return fail("ordering.duplicate")
		}
	}
	return nil
}

func checkStates(evidence map[string]any) error {
	runs, _ := arrayValueV1(evidence["runs"])
	for _, rawRun := range runs {
		run := rawRun.(map[string]any)
		stateName := run["state"].(string)
		expected, ok := expectedStateEvidence(stateName)
		if !ok || !reflect.DeepEqual(run["state_evidence"], expected) {
			return fail("state.evidence")
		}
		state := run["state_evidence"].(map[string]any)
		config := run["provenance"].(map[string]any)["config"].(map[string]any)["payload"].(map[string]any)
		if config["eebus_runtime_enabled"] != state["eebus_runtime_enabled"] ||
			config["candidate_graph_enabled"] != state["candidate_graph_enabled"] {
			return fail("state.evidence")
		}
	}
	return nil
}

func expectedStateEvidence(state string) (map[string]any, bool) {
	values := map[string]map[string]any{
		"EEBUS_DISABLED_BASELINE":   stateEvidence("BASELINE_CAPTURED", false, false, 0, 0, 0, false, []any{}),
		"EEBUS_DISABLED_CONFIRMED":  stateEvidence("DISABLED_CONFIRMED", false, false, 0, 0, 0, false, []any{}),
		"EEBUS_ENABLED_NO_SERVICES": stateEvidence("NO_SERVICES_OBSERVED", true, true, 0, 0, 0, true, []any{}),
		"EEBUS_CONNECTED_CANDIDATE_ONLY": stateEvidence("CANDIDATE_ONLY_OBSERVED", true, true, 1, 1, 0, false, []any{
			map[string]any{"candidate_id": "m7-candidate-synthetic-0001", "status": "CANDIDATE", "terminal_negative_state": nil, "visibility_channel": "CANDIDATE_DEBUG_REPLAY"},
		}),
		"EEBUS_CONFLICTED_WITHHELD": stateEvidence("CONFLICT_WITHHELD_OBSERVED", true, true, 1, 0, 1, true, []any{
			map[string]any{"candidate_id": "m7-candidate-synthetic-conflict-0001", "status": "WITHHELD", "terminal_negative_state": "CONFLICT", "visibility_channel": "CANDIDATE_DEBUG_REPLAY"},
		}),
		"EEBUS_DISABLED_ROLLBACK": stateEvidence("ROLLBACK_BASELINE_RESTORED", false, false, 0, 0, 0, false, []any{}),
	}
	value, ok := values[state]
	return value, ok
}

func stateEvidence(outcome string, runtime, graph bool, services, candidates, conflicts int64, degraded bool, facts []any) map[string]any {
	return map[string]any{
		"outcome": outcome, "eebus_runtime_enabled": runtime, "candidate_graph_enabled": graph,
		"service_count": number(services), "candidate_count": number(candidates), "conflict_count": number(conflicts),
		"degraded": degraded, "empty_success": false, "facts": facts,
	}
}

func checkViewCoverage(evidence, registry map[string]any) error {
	want, _ := stringsFromArray(registry["protected_views"])
	runs, _ := arrayValueV1(evidence["runs"])
	for _, rawRun := range runs {
		views := rawRun.(map[string]any)["protected_views"].([]any)
		got := make([]string, len(views))
		for index, rawView := range views {
			got[index] = rawView.(map[string]any)["view_id"].(string)
		}
		if !reflect.DeepEqual(got, want) {
			return fail("view.coverage")
		}
	}
	return nil
}

func checkNormalization(evidence, registry map[string]any) error {
	profile := evidence["normalization"].(map[string]any)
	if profile["profile_id"] != "multi-runtime-coexistence-no-drift-v1" ||
		profile["canonicalization"] != "RFC8785_JCS_INTEGER_SUBSET" || profile["timestamp_replacement"] != "<TIMESTAMP>" ||
		profile["mask_replacement"] != "<MASKED>" || !reflect.DeepEqual(profile["view_rules"], registry["view_rules"]) {
		return fail("canonicalization.invalid")
	}
	rules := rulesByViewIDV1(registry)
	runs, _ := arrayValueV1(evidence["runs"])
	for _, rawRun := range runs {
		views := rawRun.(map[string]any)["protected_views"].([]any)
		for _, rawView := range views {
			view := rawView.(map[string]any)
			if _, err := normalizedPayload(view["payload"], rules[view["view_id"].(string)], profile); err != nil {
				return fail("canonicalization.invalid")
			}
		}
	}
	return nil
}

func checkPayloadHashes(evidence, registry map[string]any) error {
	profile := evidence["normalization"].(map[string]any)
	rules := rulesByViewIDV1(registry)
	runs, _ := arrayValueV1(evidence["runs"])
	for _, rawRun := range runs {
		views := rawRun.(map[string]any)["protected_views"].([]any)
		for _, rawView := range views {
			view := rawView.(map[string]any)
			rule := rules[view["view_id"].(string)]
			normalized, err := normalizedPayload(view["payload"], rule, profile)
			rawHash, rawErr := domainDigestV1(coexRawPayloadDomainV1, view["payload"])
			shapeHash, shapeErr := domainDigestV1(coexShapeDomainV1, payloadShapeV1(view["payload"]))
			canonicalHash, canonicalErr := domainDigestV1(coexCanonicalPayloadDomainV1, normalized)
			if err != nil || rawErr != nil || shapeErr != nil || canonicalErr != nil || view["capture_path"] != rule["capture_path"] ||
				view["media_type"] != "application/json" || view["raw_payload_hash"] != rawHash || view["shape_hash"] != shapeHash ||
				view["canonical_payload_hash"] != canonicalHash {
				return fail("hash.payload")
			}
		}
	}
	return nil
}

func checkAntiLeak(evidence map[string]any) error {
	runs, _ := arrayValueV1(evidence["runs"])
	for _, rawRun := range runs {
		run := rawRun.(map[string]any)
		for _, rawView := range run["protected_views"].([]any) {
			if containsCandidateLeakV1(rawView.(map[string]any)["payload"]) {
				return fail("anti_leak.candidate")
			}
		}
	}
	return nil
}

func containsCandidateLeakV1(value any) bool {
	switch current := value.(type) {
	case map[string]any:
		for key, item := range current {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "candidate") || strings.Contains(lower, "conflict") {
				return true
			}
			if containsCandidateLeakV1(item) {
				return true
			}
		}
	case []any:
		for _, item := range current {
			if containsCandidateLeakV1(item) {
				return true
			}
		}
	case string:
		lower := strings.ToLower(current)
		switch lower {
		case "candidate", "withheld", "conflict", "withheld/conflict":
			return true
		default:
			return false
		}
	}
	return false
}

func checkAuthority(evidence map[string]any) error {
	runs, _ := arrayValueV1(evidence["runs"])
	for _, rawRun := range runs[:len(runs)-1] {
		run := rawRun.(map[string]any)
		registryView, registryOK := findViewV1(run, "semantic.registry")
		routesView, routesOK := findViewV1(run, "command.routing")
		registryData, registryDataOK := payloadData(registryView)
		routesData, routesDataOK := payloadData(routesView)
		if !registryOK || !routesOK || !registryDataOK || !routesDataOK || registryData["authority"] != "ebus.promoted" {
			return fail("authority.ebus")
		}
		routes, ok := arrayValueV1(routesData["routes"])
		if !ok {
			return fail("authority.ebus")
		}
		for _, rawRoute := range routes {
			route, valid := objectValueV1(rawRoute)
			if !valid || route["source"] != "ebus" {
				return fail("authority.ebus")
			}
		}
	}
	return nil
}

func checkScope(evidence, registry map[string]any) error {
	scope := evidence["scope"].(map[string]any)
	if scope["gate"] != registry["gate"] || !reflect.DeepEqual(scope["claims"], []any{"EEBUS-G18"}) ||
		!reflect.DeepEqual(scope["excluded_gates"], registry["excluded_gates"]) || scope["live_vr940_claim"] != false ||
		scope["public_version_policy"] != "V1_ONLY_NO_PUBLIC_V2" {
		return fail("gate.scope")
	}
	runs, _ := arrayValueV1(evidence["runs"])
	for _, rawRun := range runs {
		run := rawRun.(map[string]any)
		inventory, inventoryOK := findViewV1(run, "mcp.tool.inventory")
		contractView, contractOK := findViewV1(run, "mcp.eebus.v1.contract")
		graphqlView, graphqlOK := findViewV1(run, "graphql.schema")
		semanticView, semanticOK := findViewV1(run, "semantic.registry")
		inventoryData, inventoryDataOK := payloadData(inventory)
		contractData, contractDataOK := payloadData(contractView)
		graphqlData, graphqlDataOK := payloadData(graphqlView)
		semanticData, semanticDataOK := payloadData(semanticView)
		if !inventoryOK || !contractOK || !graphqlOK || !semanticOK || !inventoryDataOK || !contractDataOK || !graphqlDataOK || !semanticDataOK {
			return fail("gate.scope")
		}
		tools, toolsOK := stringsFromArray(inventoryData["tools"])
		if !toolsOK {
			return fail("gate.scope")
		}
		for _, tool := range tools {
			if containsV2Marker(tool) {
				return fail("gate.scope")
			}
		}
		version, versionOK := integerValue(contractData["version"])
		publicV2, publicOK := boolValueV1(contractData["public_v2"])
		if !versionOK || version != 1 || !publicOK || publicV2 || contractData["namespace"] != "eebus.v1" {
			return fail("gate.scope")
		}
		queryFields, queryOK := stringsFromArray(graphqlData["query_fields"])
		if !queryOK {
			return fail("gate.scope")
		}
		for _, field := range queryFields {
			lower := strings.ToLower(field)
			if strings.Contains(lower, "eebus") || containsV2Marker(field) {
				return fail("gate.scope")
			}
		}
		if containsPublicV2(semanticData) {
			return fail("gate.scope")
		}
		for _, rawView := range run["protected_views"].([]any) {
			if containsV2String(rawView.(map[string]any)["payload"]) {
				return fail("gate.scope")
			}
		}
	}
	return nil
}

func checkDrift(evidence, registry map[string]any) error {
	runs, _ := arrayValueV1(evidence["runs"])
	baseline := runs[0].(map[string]any)
	baselineViews := make(map[string]map[string]any)
	for _, rawView := range baseline["protected_views"].([]any) {
		view := rawView.(map[string]any)
		baselineViews[view["view_id"].(string)] = view
	}
	profile := evidence["normalization"].(map[string]any)
	rules := rulesByViewIDV1(registry)
	for _, rawRun := range runs[1 : len(runs)-1] {
		run := rawRun.(map[string]any)
		for _, rawView := range run["protected_views"].([]any) {
			view := rawView.(map[string]any)
			viewID := view["view_id"].(string)
			original := baselineViews[viewID]
			originalNormalized, _ := normalizedPayload(original["payload"], rules[viewID], profile)
			comparedNormalized, _ := normalizedPayload(view["payload"], rules[viewID], profile)
			originalBytes, _ := marshalCanonicalV1(originalNormalized)
			comparedBytes, _ := marshalCanonicalV1(comparedNormalized)
			if view["shape_hash"] != original["shape_hash"] || view["canonical_payload_hash"] != original["canonical_payload_hash"] ||
				!bytes.Equal(comparedBytes, originalBytes) {
				return fail("drift.consumer")
			}
		}
	}
	return nil
}

func checkRollback(evidence, registry map[string]any) error {
	runs, _ := arrayValueV1(evidence["runs"])
	baseline := runs[0].(map[string]any)
	rollback := runs[len(runs)-1].(map[string]any)
	profile := evidence["normalization"].(map[string]any)
	rules := rulesByViewIDV1(registry)
	baselineViews := baseline["protected_views"].([]any)
	rollbackViews := rollback["protected_views"].([]any)
	if len(baselineViews) != len(rollbackViews) {
		return fail("rollback.drift")
	}
	for index := range baselineViews {
		left := baselineViews[index].(map[string]any)
		right := rollbackViews[index].(map[string]any)
		viewID := left["view_id"].(string)
		leftNormalized, _ := normalizedPayload(left["payload"], rules[viewID], profile)
		rightNormalized, _ := normalizedPayload(right["payload"], rules[viewID], profile)
		leftBytes, _ := marshalCanonicalV1(leftNormalized)
		rightBytes, _ := marshalCanonicalV1(rightNormalized)
		if left["view_id"] != right["view_id"] || left["shape_hash"] != right["shape_hash"] ||
			left["canonical_payload_hash"] != right["canonical_payload_hash"] || !bytes.Equal(leftBytes, rightBytes) {
			return fail("rollback.drift")
		}
	}
	config := rollback["provenance"].(map[string]any)["config"].(map[string]any)["payload"].(map[string]any)
	runtimeEnabled, _ := boolValueV1(config["eebus_runtime_enabled"])
	graphEnabled, _ := boolValueV1(config["candidate_graph_enabled"])
	if rollback["state"] != "EEBUS_DISABLED_ROLLBACK" || runtimeEnabled || graphEnabled {
		return fail("rollback.drift")
	}
	return nil
}

func checkEvidenceHash(evidence map[string]any) error {
	expected, err := domainDigestV1(coexEvidenceDomainV1, withoutJSONKeysV1(evidence, "evidence_id", "evidence_hash"))
	if err != nil || evidence["evidence_hash"] != expected || evidence["evidence_id"] != "mrcv1:"+expected {
		return fail("hash.evidence")
	}
	return nil
}

func payloadData(view map[string]any) (map[string]any, bool) {
	if view == nil {
		return nil, false
	}
	payload, ok := objectValueV1(view["payload"])
	if !ok {
		return nil, false
	}
	return objectValueV1(payload["data"])
}

func uniqueStrings(values []string) bool {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	values := make(map[string]bool, len(left))
	for _, value := range left {
		values[value] = true
	}
	for _, value := range right {
		if !values[value] {
			return false
		}
	}
	return true
}

func strictlyIncreasing(values []int64) bool {
	for index := 1; index < len(values); index++ {
		if values[index] <= values[index-1] {
			return false
		}
	}
	return true
}

func containsPublicV2(value any) bool {
	switch current := value.(type) {
	case map[string]any:
		for key, item := range current {
			if mapEntryClaimsV2(key, item) {
				return true
			}
			if containsPublicV2(item) {
				return true
			}
		}
	case []any:
		for _, item := range current {
			if containsPublicV2(item) {
				return true
			}
		}
	case string:
		return containsV2Marker(current)
	}
	return false
}

func containsV2String(value any) bool {
	switch current := value.(type) {
	case map[string]any:
		for key, item := range current {
			if mapEntryClaimsV2(key, item) {
				return true
			}
			if containsV2String(item) {
				return true
			}
		}
	case []any:
		for _, item := range current {
			if containsV2String(item) {
				return true
			}
		}
	case string:
		return containsV2Marker(current)
	}
	return false
}

func mapEntryClaimsV2(key string, value any) bool {
	if !containsV2Marker(key) {
		return false
	}
	enabled, isBoolean := boolValueV1(value)
	return !isBoolean || enabled
}
