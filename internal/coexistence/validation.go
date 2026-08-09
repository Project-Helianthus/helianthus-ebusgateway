package coexistence

import (
	"bytes"
	"reflect"
	"sort"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/candidatefacts"
)

const m7RegistryInputSHA = "e6895b8d7406b58ed97599d8da7e9bd3b252e6e7ca3b0578ec6385bfe6dfe1c0"

type inputBindingV1 struct {
	id     string
	kind   string
	digest string
	bytes  int64
}

type m7SetV1 struct {
	graph           map[string]any
	replay          map[string]any
	graphCanonical  []byte
	replayCanonical []byte
	registryRaw     []byte
	sourceBundleRaw []byte
	sourceReplayRaw []byte
}

type m7AuthorityV1 struct {
	graph       map[string]any
	sourceGraph map[string]any
	inputs      []inputBindingV1
}

type validationContextV1 struct {
	evidence map[string]any
	registry map[string]any
	m7       m7AuthorityV1
}

func Verify(inputs InputsV1) error {
	_, err := validateInputs(inputs, true)
	return err
}

// VerifyPublic validates a public-redacted captured-runtime artifact without
// accepting the redacted status as a substitute for private source evidence.
func VerifyPublic(inputs InputsV1) error {
	_, err := validateInputs(inputs, false)
	return err
}

func validateInputs(inputs InputsV1, requirePrivate bool) (*validationContextV1, error) {
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
	m7, err := verifyM7(inputs, registry, evidence, requirePrivate)
	if err != nil {
		return nil, err
	}
	checks := []func() error{
		func() error { return checkRuntime(evidence, m7) },
		func() error { return checkConfig(evidence) },
		func() error { return checkAuthMask(evidence) },
		func() error { return checkClock(evidence) },
		func() error { return checkOrdering(evidence, registry, m7) },
		func() error { return checkStates(evidence, m7.graph) },
		func() error { return checkRestart(evidence) },
		func() error { return checkViewCoverage(evidence, registry) },
		func() error { return checkNormalization(evidence, registry) },
		func() error { return checkPayloadHashes(evidence, registry) },
		func() error { return checkAntiLeak(evidence, m7.graph, m7.sourceGraph) },
		func() error { return checkPublicRedaction(evidence) },
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
		"m7_synthetic_predecessor", "m7_live_predecessor", "m7_synthetic_binding", "m7_live_binding",
		"m7_live_terminal_binding", "m7_live_private_inputs", "m7_live_status_binding", "scenario_profiles",
		"protected_views", "view_rules", "required_acceptance_checks", "validation_precedence", "limits", "fixture_ids",
	) {
		return nil, fail("registry.binding")
	}
	expectedBinding := map[string]any{
		"contract": coexRegistryContractV1,
		"version":  number(1),
		"digest":   "sha256:" + registrySHA256,
	}
	if registry["contract"] != coexRegistryContractV1 || registry["version"] != number(1) ||
		!reflect.DeepEqual(registry["limits"], integerMapV1(hardLimitsV1)) ||
		!reflect.DeepEqual(evidence["registry"], expectedBinding) {
		return nil, fail("registry.binding")
	}
	profiles, ok := objectValueV1(registry["scenario_profiles"])
	if !ok || len(profiles) != len(coexScenarioProfilesV1) {
		return nil, fail("registry.binding")
	}
	for profile, expected := range coexScenarioProfilesV1 {
		actual, valid := stringsFromArray(profiles[profile])
		if !valid || !reflect.DeepEqual(actual, expected) {
			return nil, fail("registry.binding")
		}
	}
	for key, mode := range map[string]string{
		"m7_synthetic_predecessor": "EXACT_SYNTHETIC_FIXTURE",
		"m7_live_predecessor":      "VALIDATED_INPUTS_AND_REGENERATED_REPLAY",
	} {
		predecessor, valid := objectValueV1(registry[key])
		if !valid || !exactKeys(predecessor, "repository", "source_commit", "docs_source_commit", "binding_mode") ||
			predecessor["repository"] != "github.com/Project-Helianthus/helianthus-ebusgateway" ||
			predecessor["binding_mode"] != mode || !shaPatternV1.MatchString(stringOrEmpty(predecessor["source_commit"])) ||
			!shaPatternV1.MatchString(stringOrEmpty(predecessor["docs_source_commit"])) {
			return nil, fail("registry.binding")
		}
	}
	return registry, nil
}

func verifyRegistryForGeneration(raw []byte) (map[string]any, error) {
	dummy := map[string]any{"registry": map[string]any{
		"contract": coexRegistryContractV1,
		"version":  number(1),
		"digest":   "sha256:" + registrySHA256,
	}}
	return verifyRegistry(raw, dummy)
}

func verifyM7(inputs InputsV1, registry, evidence map[string]any, requirePrivate bool) (m7AuthorityV1, error) {
	class := stringOrEmpty(evidence["evidence_class"])
	if class == "SYNTHETIC_OFFLINE_FIXTURE" {
		if evidence["m7_live_status"] != nil {
			return m7AuthorityV1{}, fail("provenance.m7")
		}
		set, err := loadVerifiedM7Set(inputs.M7Graph, inputs.M7Replay, inputs.M7Registry, inputs.M7SourceBundle, inputs.M7SourceReplay)
		if err != nil {
			return m7AuthorityV1{}, err
		}
		binding := m7ContentBinding(set)
		predecessor := registry["m7_synthetic_predecessor"].(map[string]any)
		expected := mergeObjectsV1(map[string]any{
			"source_commit":      predecessor["source_commit"],
			"docs_source_commit": predecessor["docs_source_commit"],
		}, registry["m7_synthetic_binding"].(map[string]any))
		if !reflect.DeepEqual(binding, registry["m7_synthetic_binding"]) || !reflect.DeepEqual(evidence["m7_binding"], expected) {
			return m7AuthorityV1{}, fail("provenance.m7")
		}
		return m7AuthorityV1{
			graph:       set.graph,
			sourceGraph: set.graph,
			inputs: []inputBindingV1{
				{id: "m7:graph", kind: "M7_GRAPH", digest: stringOrEmpty(set.graph["graph_hash"]), bytes: int64(len(set.graphCanonical))},
				{id: "m7:replay", kind: "M7_REPLAY", digest: stringOrEmpty(set.replay["replay_hash"]), bytes: int64(len(set.replayCanonical))},
				{id: "m7:registry", kind: "M7_REGISTRY", digest: binding["registry_content_hash"].(string), bytes: int64(len(set.registryRaw))},
				{id: "m7:source-bundle", kind: "M7_SOURCE_BUNDLE", digest: binding["source_bundle_content_hash"].(string), bytes: int64(len(set.sourceBundleRaw))},
				{id: "m7:source-replay", kind: "M7_SOURCE_REPLAY", digest: binding["source_replay_content_hash"].(string), bytes: int64(len(set.sourceReplayRaw))},
			},
		}, nil
	}
	if class != "CAPTURED_RUNTIME_EVIDENCE" {
		return m7AuthorityV1{}, fail("provenance.m7")
	}

	statusGraph, statusRaw, err := verifyM7LiveStatus(inputs.M7LiveStatus, registry, evidence)
	if err != nil {
		return m7AuthorityV1{}, err
	}
	terminal, err := loadVerifiedM7Set(
		inputs.M7TerminalGraph,
		inputs.M7TerminalReplay,
		inputs.M7Registry,
		inputs.M7TerminalSourceBundle,
		inputs.M7TerminalSourceReplay,
	)
	if err != nil || !reflect.DeepEqual(m7ContentBinding(terminal), registry["m7_live_terminal_binding"]) {
		return m7AuthorityV1{}, fail("provenance.m7")
	}
	predecessor := registry["m7_live_predecessor"].(map[string]any)
	expectedBinding := mergeObjectsV1(map[string]any{
		"source_commit":      predecessor["source_commit"],
		"docs_source_commit": predecessor["docs_source_commit"],
	}, registry["m7_live_binding"].(map[string]any))
	if !reflect.DeepEqual(evidence["m7_binding"], expectedBinding) {
		return m7AuthorityV1{}, fail("provenance.m7")
	}

	privateInputs := registry["m7_live_private_inputs"].(map[string]any)
	if requirePrivate {
		live, liveErr := loadVerifiedM7Set(inputs.M7Graph, inputs.M7Replay, inputs.M7Registry, inputs.M7SourceBundle, inputs.M7SourceReplay)
		if liveErr != nil || !reflect.DeepEqual(m7ContentBinding(live), registry["m7_live_binding"]) ||
			!matchesPrivateInputV1(privateInputs, "graph", inputs.M7Graph) ||
			!matchesPrivateInputV1(privateInputs, "replay", inputs.M7Replay) ||
			!matchesPrivateInputV1(privateInputs, "source_bundle", inputs.M7SourceBundle) ||
			!matchesPrivateInputV1(privateInputs, "source_replay", inputs.M7SourceReplay) {
			return m7AuthorityV1{}, fail("provenance.m7")
		}
		projected, projectErr := projectM7PublicStatus(live.graph, live.replay, coexLiveGatewayCommit, coexLiveDocsCommit)
		statusValue, parseErr := parseCategorizedJSON(statusRaw, "provenance.m7")
		if projectErr != nil || parseErr != nil || !reflect.DeepEqual(projected, statusValue) {
			return m7AuthorityV1{}, fail("provenance.m7")
		}
	}

	terminalBinding := m7ContentBinding(terminal)
	statusBinding := registry["m7_live_status_binding"].(map[string]any)
	inputsV1 := []inputBindingV1{
		{id: "m7:terminal-graph", kind: "M7_TERMINAL_GRAPH", digest: stringOrEmpty(terminal.graph["graph_hash"]), bytes: int64(len(terminal.graphCanonical))},
		{id: "m7:terminal-replay", kind: "M7_TERMINAL_REPLAY", digest: stringOrEmpty(terminal.replay["replay_hash"]), bytes: int64(len(terminal.replayCanonical))},
		{id: "m7:registry", kind: "M7_REGISTRY", digest: terminalBinding["registry_content_hash"].(string), bytes: int64(len(terminal.registryRaw))},
		{id: "m7:terminal-source-bundle", kind: "M7_TERMINAL_SOURCE_BUNDLE", digest: terminalBinding["source_bundle_content_hash"].(string), bytes: int64(len(terminal.sourceBundleRaw))},
		{id: "m7:terminal-source-replay", kind: "M7_TERMINAL_SOURCE_REPLAY", digest: terminalBinding["source_replay_content_hash"].(string), bytes: int64(len(terminal.sourceReplayRaw))},
		privateInputBindingV1(privateInputs, "graph", "m7:private-graph", "M7_PRIVATE_GRAPH"),
		privateInputBindingV1(privateInputs, "replay", "m7:private-replay", "M7_PRIVATE_REPLAY"),
		privateInputBindingV1(privateInputs, "source_bundle", "m7:private-source-bundle", "M7_PRIVATE_SOURCE_BUNDLE"),
		privateInputBindingV1(privateInputs, "source_replay", "m7:private-source-replay", "M7_PRIVATE_SOURCE_REPLAY"),
		{id: "m7:status-projection", kind: "M7_PUBLIC_STATUS", digest: statusBinding["content_hash"].(string), bytes: int64(len(statusRaw))},
	}
	return m7AuthorityV1{graph: statusGraph, sourceGraph: terminal.graph, inputs: inputsV1}, nil
}

func loadVerifiedM7Set(graphRaw, replayRaw, registryRaw, sourceBundleRaw, sourceReplayRaw []byte) (m7SetV1, error) {
	if rawSHA256V1(registryRaw) != m7RegistryInputSHA || len(graphRaw) == 0 || len(replayRaw) == 0 ||
		len(sourceBundleRaw) == 0 || len(sourceReplayRaw) == 0 {
		return m7SetV1{}, fail("provenance.m7")
	}
	if err := candidatefacts.Verify(graphRaw, sourceBundleRaw, sourceReplayRaw); err != nil {
		return m7SetV1{}, fail("provenance.m7")
	}
	replayed, err := candidatefacts.Replay(graphRaw, sourceBundleRaw, sourceReplayRaw)
	if err != nil || !bytes.Equal(replayed, replayRaw) {
		return m7SetV1{}, fail("provenance.m7")
	}
	graphValue, graphErr := parseCategorizedJSON(graphRaw, "provenance.m7")
	replayValue, replayErr := parseCategorizedJSON(replayRaw, "provenance.m7")
	graph, graphOK := objectValueV1(graphValue)
	replay, replayOK := objectValueV1(replayValue)
	if graphErr != nil || replayErr != nil || !graphOK || !replayOK {
		return m7SetV1{}, fail("provenance.m7")
	}
	graphCanonical, graphErr := marshalCanonicalV1(graph)
	replayCanonical, replayErr := marshalCanonicalV1(replay)
	if graphErr != nil || replayErr != nil {
		return m7SetV1{}, fail("provenance.m7")
	}
	return m7SetV1{
		graph: graph, replay: replay, graphCanonical: graphCanonical, replayCanonical: replayCanonical,
		registryRaw: registryRaw, sourceBundleRaw: sourceBundleRaw, sourceReplayRaw: sourceReplayRaw,
	}, nil
}

func m7ContentBinding(set m7SetV1) map[string]any {
	return map[string]any{
		"graph_contract":             set.graph["contract"],
		"graph_id":                   set.graph["graph_id"],
		"graph_hash":                 set.graph["graph_hash"],
		"replay_contract":            set.replay["contract"],
		"replay_id":                  set.replay["replay_id"],
		"replay_hash":                set.replay["replay_hash"],
		"registry_content_hash":      "sha256:" + rawSHA256V1(set.registryRaw),
		"source_bundle_content_hash": "sha256:" + rawSHA256V1(set.sourceBundleRaw),
		"source_replay_content_hash": "sha256:" + rawSHA256V1(set.sourceReplayRaw),
	}
}

func verifyM7LiveStatus(raw []byte, registry, evidence map[string]any) (map[string]any, []byte, error) {
	value, err := parseCategorizedJSON(raw, "provenance.m7")
	if err != nil || schemaCheckStatus(value) != nil {
		return nil, nil, fail("provenance.m7")
	}
	status := value.(map[string]any)
	view := withoutJSONKeysV1(status, "projection_id", "projection_hash")
	projectionHash, err := domainDigestV1(coexM7StatusDomainV1, view)
	if err != nil {
		return nil, nil, fail("provenance.m7")
	}
	facts := status["facts"].([]any)
	counts := map[string]any{"RAW_ONLY": number(0), "WITHHELD": number(0)}
	ids := make([]string, len(facts))
	for index, rawFact := range facts {
		fact := rawFact.(map[string]any)
		ids[index] = fact["candidate_id"].(string)
		statusName := fact["status"].(string)
		count, _ := integerValue(counts[statusName])
		counts[statusName] = number(count + 1)
		if (statusName == "RAW_ONLY") != (fact["terminal_negative_state"] == nil) {
			return nil, nil, fail("provenance.m7")
		}
	}
	binding := map[string]any{
		"contract":           status["contract"],
		"projection_id":      status["projection_id"],
		"projection_hash":    status["projection_hash"],
		"content_hash":       "sha256:" + rawSHA256V1(raw),
		"source_graph_id":    status["source_graph_id"],
		"source_graph_hash":  status["source_graph_hash"],
		"source_replay_id":   status["source_replay_id"],
		"source_replay_hash": status["source_replay_hash"],
	}
	rawOnly, _ := integerValue(counts["RAW_ONLY"])
	withheld, _ := integerValue(counts["WITHHELD"])
	if status["projection_hash"] != projectionHash || status["projection_id"] != "dcfpsv1:"+projectionHash ||
		status["source_commit"] != coexLiveGatewayCommit || status["docs_source_commit"] != coexLiveDocsCommit ||
		!reflect.DeepEqual(binding, registry["m7_live_status_binding"]) || !reflect.DeepEqual(evidence["m7_live_status"], binding) ||
		status["fact_count"] != number(int64(len(facts))) || !reflect.DeepEqual(status["status_counts"], counts) ||
		rawOnly < 1 || withheld < 1 || !sort.StringsAreSorted(ids) || !uniqueStrings(ids) {
		return nil, nil, fail("provenance.m7")
	}
	return map[string]any{"facts": facts}, raw, nil
}

func projectM7PublicStatus(graph, replay map[string]any, sourceCommit, docsCommit string) (map[string]any, error) {
	if !shaPatternV1.MatchString(sourceCommit) || !shaPatternV1.MatchString(docsCommit) {
		return nil, fail("provenance.m7")
	}
	rawFacts, ok := arrayValueV1(graph["facts"])
	if !ok {
		return nil, fail("provenance.m7")
	}
	facts := make([]any, len(rawFacts))
	for index, rawFact := range rawFacts {
		fact, valid := objectValueV1(rawFact)
		if !valid {
			return nil, fail("provenance.m7")
		}
		facts[index] = map[string]any{
			"candidate_id": fact["candidate_id"], "status": fact["status"],
			"terminal_negative_state": fact["terminal_negative_state"], "fact_hash": fact["fact_hash"],
		}
	}
	sort.Slice(facts, func(left, right int) bool {
		return facts[left].(map[string]any)["candidate_id"].(string) < facts[right].(map[string]any)["candidate_id"].(string)
	})
	counts := map[string]any{"RAW_ONLY": number(0), "WITHHELD": number(0)}
	for _, rawFact := range facts {
		fact := rawFact.(map[string]any)
		statusName := stringOrEmpty(fact["status"])
		count, valid := integerValue(counts[statusName])
		if !valid {
			return nil, fail("provenance.m7")
		}
		counts[statusName] = number(count + 1)
	}
	result := map[string]any{
		"contract": "helianthus.platform.draft-candidate-fact-public-status.v1", "schema_version": number(1),
		"export_tier": "PUBLIC_REDACTED", "projection_id": "", "projection_hash": "",
		"source_commit": sourceCommit, "docs_source_commit": docsCommit,
		"source_graph_id": graph["graph_id"], "source_graph_hash": graph["graph_hash"],
		"source_replay_id": replay["replay_id"], "source_replay_hash": replay["replay_hash"],
		"fact_count": number(int64(len(facts))), "status_counts": counts, "facts": facts,
	}
	hash, err := domainDigestV1(coexM7StatusDomainV1, withoutJSONKeysV1(result, "projection_id", "projection_hash"))
	if err != nil {
		return nil, fail("provenance.m7")
	}
	result["projection_id"] = "dcfpsv1:" + hash
	result["projection_hash"] = hash
	return result, nil
}

func matchesPrivateInputV1(private map[string]any, name string, raw []byte) bool {
	binding, ok := objectValueV1(private[name])
	length, lengthOK := integerValue(binding["byte_length"])
	return ok && lengthOK && binding["digest"] == "sha256:"+rawSHA256V1(raw) && length == int64(len(raw))
}

func privateInputBindingV1(private map[string]any, name, id, kind string) inputBindingV1 {
	binding := private[name].(map[string]any)
	length, _ := integerValue(binding["byte_length"])
	return inputBindingV1{id: id, kind: kind, digest: binding["digest"].(string), bytes: length}
}

func checkRuntime(evidence map[string]any, m7 m7AuthorityV1) error {
	runs := evidence["runs"].([]any)
	class := evidence["evidence_class"].(string)
	baselineRuntime := runs[0].(map[string]any)["provenance"].(map[string]any)["runtime"].(map[string]any)
	comparedRuntime := baselineRuntime
	if class == "SYNTHETIC_OFFLINE_FIXTURE" {
		if baselineRuntime["source_commit"] != coexBaselineGatewayCommit || baselineRuntime["source_parent_commit"] != nil {
			return fail("provenance.runtime")
		}
		comparedRuntime = runs[1].(map[string]any)["provenance"].(map[string]any)["runtime"].(map[string]any)
		if comparedRuntime["source_parent_commit"] != coexBaselineGatewayCommit {
			return fail("provenance.runtime")
		}
	} else if baselineRuntime["source_parent_commit"] != coexLiveGatewayCommit {
		return fail("provenance.runtime")
	}
	for index, rawRun := range runs {
		run := rawRun.(map[string]any)
		provenance := run["provenance"].(map[string]any)
		runtime := provenance["runtime"].(map[string]any)
		if !validRuntimeIdentity(runtime) || class == "SYNTHETIC_OFFLINE_FIXTURE" && index > 0 && !reflect.DeepEqual(runtime, comparedRuntime) ||
			class == "CAPTURED_RUNTIME_EVIDENCE" && !reflect.DeepEqual(runtime, comparedRuntime) {
			return fail("provenance.runtime")
		}
		expected := make([]inputBindingV1, 0, len(run["protected_views"].([]any))+len(m7.inputs)+4)
		for _, rawView := range run["protected_views"].([]any) {
			view := rawView.(map[string]any)
			payload, err := marshalCanonicalV1(view["payload"])
			if err != nil {
				return fail("provenance.runtime")
			}
			expected = append(expected, inputBindingV1{
				id: "view:" + view["view_id"].(string), kind: "PROTECTED_VIEW_PAYLOAD",
				digest: view["raw_payload_hash"].(string), bytes: int64(len(payload)),
			})
		}
		expected = append(expected, m7.inputs...)
		transition := run["state_evidence"].(map[string]any)["restart_transition"]
		if transition != nil {
			item := transition.(map[string]any)
			for _, definition := range []struct {
				id, kind, field, domain string
			}{
				{"restart:process-event", "RESTART_PROCESS_EVENT", "process_event", coexRestartProcessDomainV1},
				{"restart:before-snapshot", "RESTART_STATE_SNAPSHOT", "before_snapshot", coexRestartSnapshotDomainV1},
				{"restart:after-snapshot", "RESTART_STATE_SNAPSHOT", "after_snapshot", coexRestartSnapshotDomainV1},
				{"restart:session-event", "RESTART_SESSION_EVENT", "session_event", coexRestartSessionDomainV1},
			} {
				value := item[definition.field]
				digest, err := domainDigestV1(definition.domain, value)
				encoded, encodeErr := marshalCanonicalV1(value)
				if err != nil || encodeErr != nil {
					return fail("provenance.runtime")
				}
				expected = append(expected, inputBindingV1{id: definition.id, kind: definition.kind, digest: digest, bytes: int64(len(encoded))})
			}
		}
		actualRaw := provenance["immutable_inputs"].([]any)
		actualByID := make(map[string][]inputBindingV1, len(actualRaw))
		for _, rawInput := range actualRaw {
			input := rawInput.(map[string]any)
			length, _ := integerValue(input["byte_length"])
			actual := inputBindingV1{id: stringOrEmpty(input["input_id"]), kind: stringOrEmpty(input["kind"]), digest: stringOrEmpty(input["digest"]), bytes: length}
			actualByID[actual.id] = append(actualByID[actual.id], actual)
		}
		expectedIDs := make(map[string]bool, len(expected))
		for _, want := range expected {
			expectedIDs[want.id] = true
			matches := actualByID[want.id]
			matched := false
			for _, actual := range matches {
				if actual == want {
					matched = true
					break
				}
			}
			if !matched {
				return fail("provenance.runtime")
			}
		}
		for id := range actualByID {
			if !expectedIDs[id] {
				return fail("provenance.runtime")
			}
		}
	}
	return nil
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
	live := evidence["evidence_class"] == "CAPTURED_RUNTIME_EVIDENCE"
	for _, rawRun := range evidence["runs"].([]any) {
		config := rawRun.(map[string]any)["provenance"].(map[string]any)["config"].(map[string]any)
		payload := config["payload"].(map[string]any)
		computed, err := domainDigestV1(coexConfigDomainV1, payload)
		outbound, _ := boolValueV1(payload["outbound_enabled"])
		publicV2, _ := boolValueV1(payload["public_v2_enabled"])
		if err != nil || config["config_hash"] != computed || outbound != live || publicV2 {
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
	runs := evidence["runs"].([]any)
	first := runs[0].(map[string]any)["provenance"].(map[string]any)["auth_scope"].(map[string]any)
	expectedPermissions := []any{"read:ebus", "read:eebus-v1-contract", "read:graphql", "read:portal-bootstrap", "read:debug"}
	for _, rawRun := range runs {
		provenance := rawRun.(map[string]any)["provenance"].(map[string]any)
		auth := provenance["auth_scope"].(map[string]any)
		digest, digestErr := domainDigestV1(coexAuthDomainV1, withoutJSONKeysV1(auth, "scope_hash"))
		if digestErr != nil || !reflect.DeepEqual(auth, first) || auth["principal_class"] != "READ_ONLY_TEST" ||
			!reflect.DeepEqual(auth["permissions"], expectedPermissions) || auth["scope_hash"] != digest || provenance["mask_scope_digest"] != profileDigest {
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
	for _, rawRun := range evidence["runs"].([]any) {
		run := rawRun.(map[string]any)
		offset, _ := integerValue(run["capture_offset_ns"])
		if run["provenance"].(map[string]any)["capture_clock_id"] != clock["clock_id"] ||
			offset > verification || verification-offset > maximumAge {
			return fail("provenance.clock")
		}
	}
	return nil
}

func checkOrdering(evidence, registry map[string]any, m7 m7AuthorityV1) error {
	runs := evidence["runs"].([]any)
	profiles := registry["scenario_profiles"].(map[string]any)
	wantStates, _ := stringsFromArray(profiles[evidence["evidence_class"].(string)])
	states := make([]string, len(runs))
	ids := make([]string, len(runs))
	offsets := make([]int64, len(runs))
	for index, rawRun := range runs {
		run := rawRun.(map[string]any)
		states[index] = run["state"].(string)
		ids[index] = run["run_id"].(string)
		offsets[index], _ = integerValue(run["capture_offset_ns"])
	}
	if !reflect.DeepEqual(states, wantStates) || !uniqueStrings(ids) || !strictlyIncreasing(offsets) {
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
		if !uniqueStrings(viewIDs) || len(viewIDs) == len(wantViews) && !reflect.DeepEqual(viewIDs, wantViews) {
			return fail("ordering.duplicate")
		}
		inputs := run["provenance"].(map[string]any)["immutable_inputs"].([]any)
		inputIDs := make([]string, len(inputs))
		for index, rawInput := range inputs {
			inputIDs[index] = rawInput.(map[string]any)["input_id"].(string)
		}
		if !uniqueStrings(inputIDs) {
			return fail("ordering.duplicate")
		}
		if len(viewIDs) == len(wantViews) {
			wantInputIDs := make([]string, 0, len(wantViews)+len(m7.inputs)+4)
			for _, viewID := range wantViews {
				wantInputIDs = append(wantInputIDs, "view:"+viewID)
			}
			for _, input := range m7.inputs {
				wantInputIDs = append(wantInputIDs, input.id)
			}
			if run["state_evidence"].(map[string]any)["restart_transition"] != nil {
				wantInputIDs = append(wantInputIDs,
					"restart:process-event",
					"restart:before-snapshot",
					"restart:after-snapshot",
					"restart:session-event",
				)
			}
			if !reflect.DeepEqual(inputIDs, wantInputIDs) {
				return fail("ordering.duplicate")
			}
		}
	}
	return nil
}

func checkStates(evidence, graph map[string]any) error {
	class := evidence["evidence_class"].(string)
	expected := syntheticStateEvidenceV1()
	if class == "CAPTURED_RUNTIME_EVIDENCE" {
		facts := factSummariesV1(graph)
		counts := factStatusCountsV1(facts)
		if counts["RAW_ONLY"] < 1 || counts["WITHHELD"] < 1 {
			return fail("state.evidence")
		}
		runs := evidence["runs"].([]any)
		services, _ := integerValue(runs[0].(map[string]any)["state_evidence"].(map[string]any)["service_count"])
		if services < 1 {
			return fail("state.evidence")
		}
		expected = map[string]map[string]any{
			"EEBUS_CONNECTED_BASELINE": stateEvidenceV1("CONNECTED_BASELINE_CAPTURED", true, false, services, 0, 0, 0, 0, false, []any{}),
			"EEBUS_CONNECTED_RAW_WITHHELD": stateEvidenceV1("RAW_WITHHELD_OBSERVED", true, true, services,
				counts["RAW_ONLY"], counts["CANDIDATE"], counts["CONFLICTED"], counts["WITHHELD"], false, facts),
			"EEBUS_RESTART_PERSISTED": stateEvidenceV1("RESTART_PERSISTED", true, true, services,
				counts["RAW_ONLY"], counts["CANDIDATE"], counts["CONFLICTED"], counts["WITHHELD"], false, facts),
			"EEBUS_CONNECTED_ROLLBACK": stateEvidenceV1("GRAPH_EVIDENCE_DROPPED", true, false, services, 0, 0, 0, 0, false, []any{}),
		}
	}
	for _, rawRun := range evidence["runs"].([]any) {
		run := rawRun.(map[string]any)
		state := run["state_evidence"].(map[string]any)
		actual := withoutJSONKeysV1(state, "restart_transition")
		want, ok := expected[run["state"].(string)]
		if !ok || !reflect.DeepEqual(actual, want) {
			return fail("state.evidence")
		}
		config := run["provenance"].(map[string]any)["config"].(map[string]any)["payload"].(map[string]any)
		if config["eebus_runtime_enabled"] != state["eebus_runtime_enabled"] || config["candidate_graph_enabled"] != state["candidate_graph_enabled"] {
			return fail("state.evidence")
		}
	}
	return nil
}

func syntheticStateEvidenceV1() map[string]map[string]any {
	return map[string]map[string]any{
		"EEBUS_DISABLED_BASELINE":   stateEvidenceV1("BASELINE_CAPTURED", false, false, 0, 0, 0, 0, 0, false, []any{}),
		"EEBUS_DISABLED_CONFIRMED":  stateEvidenceV1("DISABLED_CONFIRMED", false, false, 0, 0, 0, 0, 0, false, []any{}),
		"EEBUS_ENABLED_NO_SERVICES": stateEvidenceV1("NO_SERVICES_OBSERVED", true, true, 0, 0, 0, 0, 0, true, []any{}),
		"EEBUS_CONNECTED_CANDIDATE_ONLY": stateEvidenceV1("CANDIDATE_ONLY_OBSERVED", true, true, 1, 0, 1, 0, 0, false, []any{
			map[string]any{"candidate_id": "m7-candidate-synthetic-0001", "status": "CANDIDATE", "terminal_negative_state": nil, "visibility_channel": "CANDIDATE_DEBUG_REPLAY"},
		}),
		"EEBUS_CONFLICTED_WITHHELD": stateEvidenceV1("CONFLICT_WITHHELD_OBSERVED", true, true, 1, 0, 0, 1, 1, true, []any{
			map[string]any{"candidate_id": "m7-candidate-synthetic-conflict-0001", "status": "WITHHELD", "terminal_negative_state": "CONFLICT", "visibility_channel": "CANDIDATE_DEBUG_REPLAY"},
		}),
		"EEBUS_DISABLED_ROLLBACK": stateEvidenceV1("ROLLBACK_BASELINE_RESTORED", false, false, 0, 0, 0, 0, 0, false, []any{}),
	}
}

func stateEvidenceV1(outcome string, runtime, graph bool, services, rawOnly, candidates, conflicts, withheld int64, degraded bool, facts []any) map[string]any {
	return map[string]any{
		"outcome": outcome, "eebus_runtime_enabled": runtime, "candidate_graph_enabled": graph,
		"service_count": number(services), "raw_only_count": number(rawOnly), "candidate_count": number(candidates),
		"conflict_count": number(conflicts), "withheld_count": number(withheld), "degraded": degraded,
		"empty_success": false, "facts": facts,
	}
}

func factSummariesV1(graph map[string]any) []any {
	rawFacts, _ := arrayValueV1(graph["facts"])
	result := make([]any, len(rawFacts))
	for index, rawFact := range rawFacts {
		fact := rawFact.(map[string]any)
		result[index] = map[string]any{
			"candidate_id": fact["candidate_id"], "status": fact["status"],
			"terminal_negative_state": fact["terminal_negative_state"], "visibility_channel": "CANDIDATE_DEBUG_REPLAY",
		}
	}
	return result
}

func factStatusCountsV1(facts []any) map[string]int64 {
	counts := map[string]int64{"RAW_ONLY": 0, "CANDIDATE": 0, "CONFLICTED": 0, "WITHHELD": 0}
	for _, rawFact := range facts {
		counts[stringOrEmpty(rawFact.(map[string]any)["status"])]++
	}
	return counts
}

func checkRestart(evidence map[string]any) error {
	runs := evidence["runs"].([]any)
	transitions := make([]any, len(runs))
	processIDs := make([]string, len(runs))
	for index, rawRun := range runs {
		run := rawRun.(map[string]any)
		transitions[index] = run["state_evidence"].(map[string]any)["restart_transition"]
		processIDs[index] = stringOrEmpty(run["provenance"].(map[string]any)["process_instance_id"])
	}
	if evidence["evidence_class"] == "SYNTHETIC_OFFLINE_FIXTURE" {
		for _, transition := range transitions {
			if transition != nil {
				return fail("state.evidence")
			}
		}
		return nil
	}
	if len(runs) != 4 || transitions[0] != nil || transitions[1] != nil || transitions[2] == nil || transitions[3] != nil ||
		processIDs[0] != processIDs[1] || processIDs[2] != processIDs[3] || processIDs[0] == processIDs[2] {
		return fail("state.evidence")
	}
	transition := transitions[2].(map[string]any)
	processEvent := transition["process_event"].(map[string]any)
	before := transition["before_snapshot"].(map[string]any)
	after := transition["after_snapshot"].(map[string]any)
	sessionEvent := transition["session_event"].(map[string]any)
	thirdOffset, _ := integerValue(runs[2].(map[string]any)["capture_offset_ns"])
	secondOffset, _ := integerValue(runs[1].(map[string]any)["capture_offset_ns"])
	beforeTrust, _ := domainDigestV1(coexRestartTrustDomainV1, map[string]any{"trust_state_id": before["trust_state_id"]})
	afterTrust, _ := domainDigestV1(coexRestartTrustDomainV1, map[string]any{"trust_state_id": after["trust_state_id"]})
	beforePeer, _ := domainDigestV1(coexRestartPeerDomainV1, map[string]any{"peer_binding_id": before["peer_binding_id"]})
	afterPeer, _ := domainDigestV1(coexRestartPeerDomainV1, map[string]any{"peer_binding_id": after["peer_binding_id"]})
	if transition["before_process_instance_id"] != processIDs[1] || transition["after_process_instance_id"] != processIDs[2] ||
		transition["before_trust_state_hash"] != transition["after_trust_state_hash"] || transition["before_peer_binding_hash"] != transition["after_peer_binding_hash"] ||
		transition["session_reconnected"] != true || processEvent["event_id"] != transition["event_id"] ||
		processEvent["event_type"] != "PROCESS_RESTART_OBSERVED" || processEvent["before_process_instance_id"] != processIDs[1] ||
		processEvent["after_process_instance_id"] != processIDs[2] || processEvent["observed_at_offset_ns"] != number(thirdOffset) ||
		before["process_instance_id"] != processIDs[1] || before["capture_offset_ns"] != number(secondOffset) ||
		after["process_instance_id"] != processIDs[2] || after["capture_offset_ns"] != number(thirdOffset) ||
		before["trust_state_id"] != after["trust_state_id"] || before["peer_binding_id"] != after["peer_binding_id"] ||
		before["session_id"] == after["session_id"] || before["session_state"] != "CONNECTED" || after["session_state"] != "CONNECTED" ||
		transition["before_trust_state_hash"] != beforeTrust || transition["after_trust_state_hash"] != afterTrust ||
		transition["before_peer_binding_hash"] != beforePeer || transition["after_peer_binding_hash"] != afterPeer ||
		sessionEvent["event_type"] != "SESSION_RECONNECTED_OBSERVED" || sessionEvent["process_instance_id"] != processIDs[2] ||
		sessionEvent["session_id"] != after["session_id"] || sessionEvent["observed_at_offset_ns"] != number(thirdOffset) || sessionEvent["state"] != "CONNECTED" {
		return fail("state.evidence")
	}
	return nil
}

func checkViewCoverage(evidence, registry map[string]any) error {
	want, _ := stringsFromArray(registry["protected_views"])
	for _, rawRun := range evidence["runs"].([]any) {
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
	if profile["profile_id"] != "multi-runtime-coexistence-no-drift-v1" || profile["canonicalization"] != "RFC8785_JCS_INTEGER_SUBSET" ||
		profile["timestamp_replacement"] != "<TIMESTAMP>" || profile["mask_replacement"] != "<MASKED>" ||
		!reflect.DeepEqual(profile["view_rules"], registry["view_rules"]) {
		return fail("canonicalization.invalid")
	}
	rules := rulesByViewIDV1(registry)
	for _, rawRun := range evidence["runs"].([]any) {
		for _, rawView := range rawRun.(map[string]any)["protected_views"].([]any) {
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
	for _, rawRun := range evidence["runs"].([]any) {
		for _, rawView := range rawRun.(map[string]any)["protected_views"].([]any) {
			view := rawView.(map[string]any)
			rule := rules[view["view_id"].(string)]
			normalized, err := normalizedPayload(view["payload"], rule, profile)
			rawHash, rawErr := domainDigestV1(coexRawPayloadDomainV1, view["payload"])
			shapeHash, shapeErr := domainDigestV1(coexShapeDomainV1, payloadShapeV1(view["payload"]))
			canonicalHash, canonicalErr := domainDigestV1(coexCanonicalPayloadDomainV1, normalized)
			if err != nil || rawErr != nil || shapeErr != nil || canonicalErr != nil || view["capture_path"] != rule["capture_path"] ||
				view["media_type"] != "application/json" || view["raw_payload_hash"] != rawHash || view["shape_hash"] != shapeHash || view["canonical_payload_hash"] != canonicalHash {
				return fail("hash.payload")
			}
		}
	}
	return nil
}

func checkDrift(evidence, registry map[string]any) error {
	runs := evidence["runs"].([]any)
	baseline := runs[0].(map[string]any)
	baselineViews := make(map[string]map[string]any)
	for _, rawView := range baseline["protected_views"].([]any) {
		view := rawView.(map[string]any)
		baselineViews[view["view_id"].(string)] = view
	}
	profile := evidence["normalization"].(map[string]any)
	rules := rulesByViewIDV1(registry)
	for _, rawRun := range runs[1 : len(runs)-1] {
		for _, rawView := range rawRun.(map[string]any)["protected_views"].([]any) {
			view := rawView.(map[string]any)
			viewID := view["view_id"].(string)
			original := baselineViews[viewID]
			originalNormalized, _ := normalizedPayload(original["payload"], rules[viewID], profile)
			comparedNormalized, _ := normalizedPayload(view["payload"], rules[viewID], profile)
			originalBytes, _ := marshalCanonicalV1(originalNormalized)
			comparedBytes, _ := marshalCanonicalV1(comparedNormalized)
			if view["shape_hash"] != original["shape_hash"] || view["canonical_payload_hash"] != original["canonical_payload_hash"] || !bytes.Equal(comparedBytes, originalBytes) {
				return fail("drift.consumer")
			}
		}
	}
	return nil
}

func checkRollback(evidence, registry map[string]any) error {
	runs := evidence["runs"].([]any)
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
		if left["view_id"] != right["view_id"] || left["shape_hash"] != right["shape_hash"] || left["canonical_payload_hash"] != right["canonical_payload_hash"] || !bytes.Equal(leftBytes, rightBytes) {
			return fail("rollback.drift")
		}
	}
	live := evidence["evidence_class"] == "CAPTURED_RUNTIME_EVIDENCE"
	expectedState := "EEBUS_DISABLED_ROLLBACK"
	if live {
		expectedState = "EEBUS_CONNECTED_ROLLBACK"
	}
	config := rollback["provenance"].(map[string]any)["config"].(map[string]any)["payload"].(map[string]any)
	if rollback["state"] != expectedState || config["eebus_runtime_enabled"] != live || config["candidate_graph_enabled"] != false {
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

func mergeObjectsV1(left, right map[string]any) map[string]any {
	result := make(map[string]any, len(left)+len(right))
	for key, value := range left {
		result[key] = value
	}
	for key, value := range right {
		result[key] = value
	}
	return result
}

func integerMapV1(values map[string]int64) map[string]any {
	result := make(map[string]any, len(values))
	for key, value := range values {
		result[key] = number(value)
	}
	return result
}

func stringOrEmpty(value any) string {
	result, _ := stringValueV1(value)
	return result
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

func strictlyIncreasing(values []int64) bool {
	for index := 1; index < len(values); index++ {
		if values[index] <= values[index-1] {
			return false
		}
	}
	return true
}

func containsText(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(value, want) {
			return true
		}
	}
	return false
}
