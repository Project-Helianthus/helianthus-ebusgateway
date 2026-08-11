package coexistence

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type expectedState struct {
	outcome        string
	runtimeEnabled bool
	graphEnabled   bool
	serviceCount   int64
	candidateCount int64
	conflictCount  int64
	degraded       bool
}

var expectedStatesV1 = map[string]expectedState{
	"EEBUS_DISABLED_BASELINE": {
		outcome: "BASELINE_CAPTURED",
	},
	"EEBUS_DISABLED_CONFIRMED": {
		outcome: "DISABLED_CONFIRMED",
	},
	"EEBUS_ENABLED_NO_SERVICES": {
		outcome: "NO_SERVICES_OBSERVED", runtimeEnabled: true, graphEnabled: true, degraded: true,
	},
	"EEBUS_CONNECTED_CANDIDATE_ONLY": {
		outcome: "CANDIDATE_ONLY_OBSERVED", runtimeEnabled: true, graphEnabled: true, serviceCount: 1, candidateCount: 1,
	},
	"EEBUS_CONFLICTED_WITHHELD": {
		outcome: "CONFLICT_WITHHELD_OBSERVED", runtimeEnabled: true, graphEnabled: true, serviceCount: 1, conflictCount: 1, degraded: true,
	},
	"EEBUS_DISABLED_ROLLBACK": {
		outcome: "ROLLBACK_BASELINE_RESTORED",
	},
}

var acceptanceChecksV1 = []string{
	"PROVENANCE_BOUND",
	"STATE_EVIDENCE_EXPLICIT",
	"PROTECTED_VIEW_SET_COMPLETE",
	"PAYLOAD_HASHES_VERIFIED",
	"SHAPE_IDENTICAL",
	"CANONICAL_BYTES_IDENTICAL",
	"EBUS_AUTHORITY_PRESERVED",
	"CANDIDATE_CONFINED",
	"PUBLIC_REDACTION_ENFORCED",
	"V1_SURFACES_PRESERVED",
	"G18_SCOPE_ONLY",
}

func TestMSP08PositiveFixtureExercisesAllStatesViewsAndNoDriftRules(t *testing.T) {
	root := packageDir(t)
	evidence := loadObject(t, filepath.Join(root, "testdata/canonical/positive/evidence.json"))
	registry := loadObject(t, filepath.Join(root, "contracts/multi-runtime-coexistence-registry-v1.json"))

	assertStringValue(t, evidence, "contract", evidenceContractV1)
	assertIntValue(t, evidence, "schema_version", 1)
	assertStringValue(t, evidence, "fixture_id", "MSP08-G18-SYNTHETIC-POSITIVE-001")
	assertStringValue(t, evidence, "evidence_class", "SYNTHETIC_OFFLINE_FIXTURE")

	scope := objectValue(t, evidence, "scope")
	assertStringValue(t, scope, "gate", "EEBUS-G18")
	assertStringSlice(t, scope["claims"], []string{"EEBUS-G18"})
	assertStringSlice(t, scope["excluded_gates"], []string{"EEBUS-G17", "EEBUS-G19"})
	if boolValue(t, scope, "live_vr940_claim") {
		t.Fatal("synthetic G18 fixture must not claim positive live VR940 evidence")
	}
	assertStringValue(t, scope, "public_version_policy", "V1_ONLY_NO_PUBLIC_V2")

	runs := arrayValue(t, evidence, "runs")
	if len(runs) != len(scenarioOrderV1) {
		t.Fatalf("runs = %d; want %d", len(runs), len(scenarioOrderV1))
	}
	rules := rulesByViewID(t, registry)
	baselineCanonical := make(map[string][]byte, len(protectedViewsV1))
	baselineShape := make(map[string][]byte, len(protectedViewsV1))

	for runIndex, rawRun := range runs {
		run := asObject(t, rawRun, "run")
		state := stringValue(t, run, "state")
		if state != scenarioOrderV1[runIndex] {
			t.Fatalf("run %d state = %s; want %s", runIndex, state, scenarioOrderV1[runIndex])
		}
		assertStateEvidence(t, state, objectValue(t, run, "state_evidence"))

		views := arrayValue(t, run, "protected_views")
		if len(views) != len(protectedViewsV1) {
			t.Fatalf("%s views = %d; want %d", state, len(views), len(protectedViewsV1))
		}
		for viewIndex, rawView := range views {
			view := asObject(t, rawView, "protected view")
			viewID := stringValue(t, view, "view_id")
			if viewID != protectedViewsV1[viewIndex] {
				t.Fatalf("%s view %d = %s; want %s", state, viewIndex, viewID, protectedViewsV1[viewIndex])
			}
			rule := rules[viewID]
			assertStringValue(t, view, "capture_path", stringValue(t, rule, "capture_path"))
			assertStringValue(t, view, "media_type", "application/json")

			payload, ok := view["payload"]
			if !ok {
				t.Fatalf("%s/%s has no unmodified payload", state, viewID)
			}
			normalized := normalizePayload(t, payload, rule)
			shape := payloadShape(t, payload)
			if got, want := domainDigest(t, rawPayloadDomainV1, payload), stringValue(t, view, "raw_payload_hash"); got != want {
				t.Fatalf("%s/%s raw hash = %s; want %s", state, viewID, got, want)
			}
			if got, want := domainDigest(t, shapeDomainV1, shape), stringValue(t, view, "shape_hash"); got != want {
				t.Fatalf("%s/%s shape hash = %s; want %s", state, viewID, got, want)
			}
			if got, want := domainDigest(t, canonicalPayloadDomainV1, normalized), stringValue(t, view, "canonical_payload_hash"); got != want {
				t.Fatalf("%s/%s canonical hash = %s; want %s", state, viewID, got, want)
			}
			if containsCandidateLeak(payload) {
				t.Fatalf("%s/%s leaks candidate or conflict material", state, viewID)
			}
			assertViewImmutableInput(t, run, viewID, payload, stringValue(t, view, "raw_payload_hash"))
			assertProtectedAuthority(t, viewID, payload)

			canonicalBytes := canonicalJSON(t, normalized)
			shapeBytes := canonicalJSON(t, shape)
			if runIndex == 0 {
				baselineCanonical[viewID] = canonicalBytes
				baselineShape[viewID] = shapeBytes
				continue
			}
			if !reflect.DeepEqual(canonicalBytes, baselineCanonical[viewID]) {
				t.Fatalf("%s/%s canonical payload drifted from baseline", state, viewID)
			}
			if !reflect.DeepEqual(shapeBytes, baselineShape[viewID]) {
				t.Fatalf("%s/%s shape drifted from baseline", state, viewID)
			}
		}
	}

	baseline := asObject(t, runs[0], "baseline")
	rollback := asObject(t, runs[len(runs)-1], "rollback")
	for _, viewID := range protectedViewsV1 {
		baselineView := findView(t, baseline, viewID)
		rollbackView := findView(t, rollback, viewID)
		for _, field := range []string{"shape_hash", "canonical_payload_hash"} {
			if stringValue(t, baselineView, field) != stringValue(t, rollbackView, field) {
				t.Fatalf("rollback %s/%s differs from exact baseline", viewID, field)
			}
		}
	}
}

func TestMSP08ProvenanceBindsRuntimeConfigM7ReplayAuthMaskAndClock(t *testing.T) {
	root := packageDir(t)
	repo := repoDir(t)
	evidencePath := filepath.Join(root, "testdata/canonical/positive/evidence.json")
	registryPath := filepath.Join(root, "contracts/multi-runtime-coexistence-registry-v1.json")
	evidence := loadObject(t, evidencePath)
	registry := loadObject(t, registryPath)

	registryBinding := objectValue(t, evidence, "registry")
	assertStringValue(t, registryBinding, "contract", registryContractV1)
	assertIntValue(t, registryBinding, "version", 1)
	assertStringValue(t, registryBinding, "digest", "sha256:"+rawSHA256(readFile(t, registryPath)))

	m7 := objectValue(t, evidence, "m7_binding")
	assertStringValue(t, m7, "source_commit", coexBaselineGatewayCommit)
	assertStringValue(t, m7, "docs_source_commit", coexSyntheticDocsCommit)
	syntheticBinding := objectValue(t, registry, "m7_synthetic_binding")
	for _, field := range []string{"graph_contract", "graph_id", "graph_hash", "replay_contract", "replay_id", "replay_hash"} {
		if !reflect.DeepEqual(m7[field], syntheticBinding[field]) {
			t.Fatalf("M7 %s does not match registry", field)
		}
	}
	graphPath := filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/graph.json")
	replayPath := filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/replay-result.json")
	graph := loadObject(t, graphPath)
	replay := loadObject(t, replayPath)
	assertStringValue(t, graph, "graph_id", stringValue(t, m7, "graph_id"))
	assertStringValue(t, graph, "graph_hash", stringValue(t, m7, "graph_hash"))
	assertStringValue(t, replay, "replay_id", stringValue(t, m7, "replay_id"))
	assertStringValue(t, replay, "replay_hash", stringValue(t, m7, "replay_hash"))

	normalization := objectValue(t, evidence, "normalization")
	assertStringValue(t, normalization, "profile_id", "multi-runtime-coexistence-no-drift-v1")
	assertStringValue(t, normalization, "canonicalization", "RFC8785_JCS_INTEGER_SUBSET")
	assertStringValue(t, normalization, "timestamp_replacement", "<TIMESTAMP>")
	assertStringValue(t, normalization, "mask_replacement", "<MASKED>")
	if !reflect.DeepEqual(normalization["view_rules"], registry["view_rules"]) {
		t.Fatal("normalization rules do not exactly match registry")
	}
	profileDigest := domainDigest(t, normalizationDomainV1, withoutKeys(t, normalization, "profile_digest"))
	assertStringValue(t, normalization, "profile_digest", profileDigest)

	clock := objectValue(t, evidence, "capture_clock")
	assertStringValue(t, clock, "basis", "MONOTONIC_CAPTURE_OFFSETS")
	assertStringValue(t, clock, "clock_id", "clock-0123456789abcdef0123456789abcdef")
	assertStringValue(t, clock, "wall_anchor_utc", "2026-07-20T00:00:00Z")
	assertStringValue(t, clock, "monotonic_epoch_id", "msp08-synthetic-clock-epoch")
	assertIntValue(t, clock, "max_clock_error_ns", 1000000)
	assertIntValue(t, clock, "max_capture_age_ns", 10000000000)
	assertIntValue(t, clock, "verification_offset_ns", 6000000000)
	assertStringValue(t, clock, "clock_hash", domainDigest(t, clockDomainV1, withoutKeys(t, clock, "clock_hash")))

	var comparedRuntime map[string]any
	for runIndex, rawRun := range arrayValue(t, evidence, "runs") {
		run := asObject(t, rawRun, "run")
		state := stringValue(t, run, "state")
		if offset := intValue(t, run, "capture_offset_ns"); offset != int64(runIndex)*1000000000 {
			t.Fatalf("%s capture offset = %d", state, offset)
		}
		provenance := objectValue(t, run, "provenance")
		assertStringValue(t, provenance, "capture_clock_id", stringValue(t, clock, "clock_id"))
		assertStringValue(t, provenance, "mask_scope_digest", profileDigest)

		runtime := objectValue(t, provenance, "runtime")
		assertStringValue(t, runtime, "repository", "github.com/Project-Helianthus/helianthus-ebusgateway")
		assertIntValue(t, runtime, "artifact_size_bytes", 16777216)
		artifactDigest := stringValue(t, runtime, "artifact_digest")
		assertStringValue(t, runtime, "artifact_id", "gateway:"+artifactDigest)
		manifest := objectValue(t, runtime, "build_manifest")
		assertStringValue(t, runtime, "build_manifest_hash", domainDigest(t, buildDomainV1, manifest))
		if runIndex == 0 {
			assertStringValue(t, runtime, "source_commit", coexBaselineGatewayCommit)
			if runtime["source_parent_commit"] != nil {
				t.Fatal("baseline source parent must be null")
			}
		} else {
			assertStringValue(t, runtime, "source_parent_commit", coexBaselineGatewayCommit)
			if len(stringValue(t, runtime, "source_commit")) != 40 {
				t.Fatal("compared runtime source commit is not a full SHA")
			}
			if comparedRuntime == nil {
				comparedRuntime = cloneObject(t, runtime)
			} else if !reflect.DeepEqual(runtime, comparedRuntime) {
				t.Fatalf("%s uses a different compared runtime identity", state)
			}
		}

		config := objectValue(t, provenance, "config")
		payload := objectValue(t, config, "payload")
		assertStringValue(t, config, "config_hash", domainDigest(t, configDomainV1, payload))
		wantState := expectedStatesV1[state]
		if boolValue(t, payload, "eebus_runtime_enabled") != wantState.runtimeEnabled ||
			boolValue(t, payload, "candidate_graph_enabled") != wantState.graphEnabled {
			t.Fatalf("%s config does not match state", state)
		}
		if boolValue(t, payload, "outbound_enabled") || boolValue(t, payload, "public_v2_enabled") {
			t.Fatalf("%s enables outbound or public V2", state)
		}

		auth := objectValue(t, provenance, "auth_scope")
		assertStringValue(t, auth, "scope_id", "msp08-read-only-contract-capture")
		assertStringValue(t, auth, "principal_class", "READ_ONLY_TEST")
		assertStringSlice(t, auth["permissions"], []string{"read:ebus", "read:eebus-v1-contract", "read:graphql", "read:portal-bootstrap", "read:debug"})
		assertStringValue(t, auth, "scope_hash", domainDigest(t, authDomainV1, withoutKeys(t, auth, "scope_hash")))

		inputs := arrayValue(t, provenance, "immutable_inputs")
		if len(inputs) != len(protectedViewsV1)+5 {
			t.Fatalf("%s immutable inputs = %d; want %d", state, len(inputs), len(protectedViewsV1)+5)
		}
		for index, viewID := range protectedViewsV1 {
			input := asObject(t, inputs[index], "protected input")
			assertStringValue(t, input, "input_id", "view:"+viewID)
			assertStringValue(t, input, "kind", "PROTECTED_VIEW_PAYLOAD")
		}
		graphInput := asObject(t, inputs[len(protectedViewsV1)], "M7 graph input")
		assertStringValue(t, graphInput, "input_id", "m7:graph")
		assertStringValue(t, graphInput, "kind", "M7_GRAPH")
		assertStringValue(t, graphInput, "digest", stringValue(t, graph, "graph_hash"))
		assertIntValue(t, graphInput, "byte_length", int64(len(canonicalJSON(t, graph))))
		replayInput := asObject(t, inputs[len(protectedViewsV1)+1], "M7 replay input")
		assertStringValue(t, replayInput, "input_id", "m7:replay")
		assertStringValue(t, replayInput, "kind", "M7_REPLAY")
		assertStringValue(t, replayInput, "digest", stringValue(t, replay, "replay_hash"))
		assertIntValue(t, replayInput, "byte_length", int64(len(canonicalJSON(t, replay))))
	}

	assertStringValue(t, evidence, "evidence_hash", domainDigest(t, evidenceDomainV1, withoutKeys(t, evidence, "evidence_id", "evidence_hash")))
	assertStringValue(t, evidence, "evidence_id", "mrcv1:"+stringValue(t, evidence, "evidence_hash"))
}

func TestMSP08GoldenReportPinsDerivedAcceptanceAndExactRollback(t *testing.T) {
	report := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/report.json"))
	assertStringValue(t, report, "contract", reportContractV1)
	assertIntValue(t, report, "schema_version", 1)
	assertStringValue(t, report, "fixture_id", "MSP08-G18-SYNTHETIC-REPORT-001")
	assertStringValue(t, report, "gate", "EEBUS-G18")
	assertStringValue(t, report, "verdict", "PASS")
	assertStringValue(t, report, "report_hash", domainDigest(t, reportDomainV1, withoutKeys(t, report, "report_id", "report_hash")))
	assertStringValue(t, report, "report_id", "mrcrv1:"+stringValue(t, report, "report_hash"))

	baseline := objectValue(t, report, "baseline")
	assertStringValue(t, baseline, "run_id", "msp08-run-01")
	assertStringValue(t, baseline, "state", "EEBUS_DISABLED_BASELINE")
	assertStringValue(t, baseline, "source_commit", coexBaselineGatewayCommit)
	assertViewHashInventory(t, arrayValue(t, baseline, "view_hashes"))

	wantResults := []string{
		"NO_DRIFT",
		"EXPECTED_NO_SERVICES_NO_DRIFT",
		"CANDIDATE_CONFINED_NO_DRIFT",
		"CONFLICT_WITHHELD_NO_DRIFT",
		"ROLLBACK_EXACT_BASELINE",
	}
	scenarios := arrayValue(t, report, "scenarios")
	if len(scenarios) != len(scenarioOrderV1)-1 {
		t.Fatalf("report scenarios = %d; want %d", len(scenarios), len(scenarioOrderV1)-1)
	}
	for index, raw := range scenarios {
		scenario := asObject(t, raw, "report scenario")
		assertStringValue(t, scenario, "state", scenarioOrderV1[index+1])
		assertStringValue(t, scenario, "result", wantResults[index])
		assertStringSlice(t, scenario["checks"], acceptanceChecksV1)
		assertViewHashInventory(t, arrayValue(t, scenario, "view_hashes"))
	}

	matrix := arrayValue(t, report, "acceptance_matrix")
	if len(matrix) != len(scenarioOrderV1) {
		t.Fatalf("acceptance matrix rows = %d; want %d", len(matrix), len(scenarioOrderV1))
	}
	for index, raw := range matrix {
		row := asObject(t, raw, "acceptance row")
		assertStringValue(t, row, "state", scenarioOrderV1[index])
		if !boolValue(t, row, "passed") {
			t.Fatalf("acceptance row %s is not passed", scenarioOrderV1[index])
		}
		assertStringSlice(t, row["required_checks"], acceptanceChecksV1)
	}

	rollback := objectValue(t, report, "rollback")
	assertStringValue(t, rollback, "run_id", "msp08-run-06")
	if boolValue(t, rollback, "runtime_enabled") {
		t.Fatal("synthetic rollback runtime remains enabled")
	}
	for _, field := range []string{"candidate_graph_disabled", "exact_baseline_restored"} {
		if !boolValue(t, rollback, field) {
			t.Fatalf("rollback %s is false", field)
		}
	}
}

func assertStateEvidence(t *testing.T, state string, evidence map[string]any) {
	t.Helper()
	want, ok := expectedStatesV1[state]
	if !ok {
		t.Fatalf("unknown state %s", state)
	}
	assertStringValue(t, evidence, "outcome", want.outcome)
	if boolValue(t, evidence, "eebus_runtime_enabled") != want.runtimeEnabled ||
		boolValue(t, evidence, "candidate_graph_enabled") != want.graphEnabled ||
		intValue(t, evidence, "service_count") != want.serviceCount ||
		intValue(t, evidence, "candidate_count") != want.candidateCount ||
		intValue(t, evidence, "conflict_count") != want.conflictCount ||
		boolValue(t, evidence, "degraded") != want.degraded {
		t.Fatalf("%s state evidence = %s", state, explainValue(evidence))
	}
	if boolValue(t, evidence, "empty_success") {
		t.Fatalf("%s uses forbidden empty success", state)
	}
	facts := arrayValue(t, evidence, "facts")
	switch state {
	case "EEBUS_CONNECTED_CANDIDATE_ONLY":
		if len(facts) != 1 {
			t.Fatalf("candidate state facts = %d; want 1", len(facts))
		}
		fact := asObject(t, facts[0], "candidate fact")
		assertStringValue(t, fact, "status", "CANDIDATE")
		if fact["terminal_negative_state"] != nil {
			t.Fatal("candidate fact has terminal state")
		}
		assertStringValue(t, fact, "visibility_channel", "CANDIDATE_DEBUG_REPLAY")
	case "EEBUS_CONFLICTED_WITHHELD":
		if len(facts) != 1 {
			t.Fatalf("conflicted state facts = %d; want 1", len(facts))
		}
		fact := asObject(t, facts[0], "conflict fact")
		assertStringValue(t, fact, "status", "WITHHELD")
		assertStringValue(t, fact, "terminal_negative_state", "CONFLICT")
		assertStringValue(t, fact, "visibility_channel", "CANDIDATE_DEBUG_REPLAY")
	default:
		if len(facts) != 0 {
			t.Fatalf("%s exposes %d internal facts", state, len(facts))
		}
	}
}

func rulesByViewID(t *testing.T, registry map[string]any) map[string]map[string]any {
	t.Helper()
	result := make(map[string]map[string]any, len(protectedViewsV1))
	for _, raw := range arrayValue(t, registry, "view_rules") {
		rule := asObject(t, raw, "view rule")
		result[stringValue(t, rule, "view_id")] = rule
	}
	return result
}

func assertViewImmutableInput(t *testing.T, run map[string]any, viewID string, payload any, digest string) {
	t.Helper()
	inputs := arrayValue(t, objectValue(t, run, "provenance"), "immutable_inputs")
	inputID := "view:" + viewID
	for _, raw := range inputs {
		input := asObject(t, raw, "immutable input")
		if stringValue(t, input, "input_id") != inputID {
			continue
		}
		assertStringValue(t, input, "kind", "PROTECTED_VIEW_PAYLOAD")
		assertStringValue(t, input, "digest", digest)
		assertIntValue(t, input, "byte_length", int64(len(canonicalJSON(t, payload))))
		return
	}
	t.Fatalf("missing immutable input %s", inputID)
}

func assertProtectedAuthority(t *testing.T, viewID string, payload any) {
	t.Helper()
	data := objectValue(t, asObject(t, payload, viewID+" payload"), "data")
	switch viewID {
	case "mcp.ebus.v1.responses":
		assertStringValue(t, data, "contract", "ebus.v1")
	case "mcp.tool.inventory":
		assertStringSlice(t, data["tools"], []string{
			"ebus.v1.registry.devices.list",
			"ebus.v1.semantic.snapshot.get",
			"eebus.v1.runtime.status.get",
			"eebus.v1.services.list",
			"eebus.v1.services.get",
			"eebus.v1.sessions.list",
			"eebus.v1.sessions.get",
			"eebus.v1.topology.get",
			"eebus.v1.snapshot.capture",
			"eebus.v1.snapshot.drop",
			"eebus.v1.pairing.status.get",
		})
		for _, tool := range stringSlice(t, data["tools"]) {
			if strings.Contains(tool, ".v2.") {
				t.Fatalf("public V2 tool leaked into inventory: %s", tool)
			}
		}
	case "graphql.schema":
		assertIntValue(t, data, "schema_version", 1)
		assertStringSlice(t, data["query_fields"], []string{"devices", "dhw", "energyTotals", "zones"})
	case "graphql.ebus.values":
		zone := asObject(t, arrayValue(t, data, "zones")[0], "GraphQL zone")
		assertStringValue(t, zone, "source", "ebus")
	case "command.routing":
		route := asObject(t, arrayValue(t, data, "routes")[0], "command route")
		assertStringValue(t, route, "source", "ebus")
		if data["fallback"] != nil {
			t.Fatal("command routing gained a fallback")
		}
	case "semantic.registry":
		assertStringValue(t, data, "authority", "ebus.promoted")
		leaf := asObject(t, arrayValue(t, data, "leaves")[0], "semantic leaf")
		assertStringValue(t, leaf, "source", "ebus")
		assertStringValue(t, leaf, "promotion_state", "PROMOTED")
	case "mcp.eebus.v1.contract":
		assertStringValue(t, data, "namespace", "eebus.v1")
		assertIntValue(t, data, "version", 1)
		if boolValue(t, data, "public_v2") {
			t.Fatal("stable eeBUS contract advertises V2")
		}
	}
}

func assertViewHashInventory(t *testing.T, values []any) {
	t.Helper()
	if len(values) != len(protectedViewsV1) {
		t.Fatalf("view hash inventory = %d; want %d", len(values), len(protectedViewsV1))
	}
	for index, raw := range values {
		item := asObject(t, raw, "view hash")
		assertStringValue(t, item, "view_id", protectedViewsV1[index])
		for _, field := range []string{"shape_hash", "canonical_payload_hash"} {
			if value := stringValue(t, item, field); !strings.HasPrefix(value, "sha256:") || len(value) != 71 {
				t.Fatalf("%s %s = %q", protectedViewsV1[index], field, value)
			}
		}
	}
}

func TestMSP08JSONNumbersRemainPortableIntegers(t *testing.T) {
	evidence := loadValue(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
	assertPortableIntegerSubset(t, evidence)
}

func assertPortableIntegerSubset(t *testing.T, value any) {
	t.Helper()
	switch typed := value.(type) {
	case map[string]any:
		for _, item := range typed {
			assertPortableIntegerSubset(t, item)
		}
	case []any:
		for _, item := range typed {
			assertPortableIntegerSubset(t, item)
		}
	case json.Number:
		if strings.ContainsAny(typed.String(), ".eE") || strings.HasPrefix(typed.String(), "-0") {
			t.Fatalf("non-portable JSON number %q", typed.String())
		}
		integer, err := typed.Int64()
		if err != nil || integer < -9007199254740991 || integer > 9007199254740991 {
			t.Fatalf("JSON number %q exceeds portable integer subset", typed.String())
		}
	}
}
