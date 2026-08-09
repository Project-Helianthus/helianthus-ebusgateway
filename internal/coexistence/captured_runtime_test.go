package coexistence

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestMSP08CapturedRuntimePublicReplayProvesFourConnectedStates(t *testing.T) {
	inputs := capturedPublicInputs(t)
	if err := VerifyPublic(inputs); err != nil {
		t.Fatalf("VerifyPublic() error = %v", err)
	}
	assertCategory(t, Verify(inputs), "provenance.m7")

	evidence := asObject(t, decodeJSON(t, inputs.Evidence), "captured evidence")
	assertStringValue(t, evidence, "evidence_class", "CAPTURED_RUNTIME_EVIDENCE")
	if !boolValue(t, objectValue(t, evidence, "scope"), "live_vr940_claim") {
		t.Fatal("captured evidence does not declare its live claim")
	}
	runs := arrayValue(t, evidence, "runs")
	if len(runs) != len(capturedScenarioOrderV1) {
		t.Fatalf("captured runs = %d; want %d", len(runs), len(capturedScenarioOrderV1))
	}
	artifact := objectValue(t, objectValue(t, asObject(t, runs[0], "run"), "provenance"), "runtime")
	for index, rawRun := range runs {
		run := asObject(t, rawRun, "run")
		assertStringValue(t, run, "state", capturedScenarioOrderV1[index])
		if !reflect.DeepEqual(artifact, objectValue(t, objectValue(t, run, "provenance"), "runtime")) {
			t.Fatalf("run %d did not use the same exact artifact", index)
		}
		if !boolValue(t, objectValue(t, run, "state_evidence"), "eebus_runtime_enabled") {
			t.Fatalf("run %d disconnected the eeBUS runtime", index)
		}
		if got := len(arrayValue(t, run, "protected_views")); got != len(protectedViewsV1) {
			t.Fatalf("run %d protected views = %d", index, got)
		}
	}

	restart := objectValue(t, objectValue(t, asObject(t, runs[2], "restart run"), "state_evidence"), "restart_transition")
	if stringValue(t, restart, "before_process_instance_id") == stringValue(t, restart, "after_process_instance_id") || !boolValue(t, restart, "session_reconnected") {
		t.Fatal("restart does not prove a distinct process and reconnection")
	}

	evidencePath := filepath.Join(t.TempDir(), "captured-public-evidence.json")
	writeRaw(t, evidencePath, inputs.Evidence)
	harness := buildProductionHarness(t)
	result := runHarness(t, harness, nil, capturedPublicHarnessArgs(t, evidencePath)...)
	assertHarnessSuccess(t, result)
	if string(result.stdout) != "public-only-ok\n" {
		t.Fatalf("verify-public stdout = %q", result.stdout)
	}
}

func TestMSP08CapturedRuntimeRejectsArtifactRestartRollbackAndSecretFaults(t *testing.T) {
	for _, test := range []struct {
		name string
		want string
		edit func(*testing.T, map[string]any)
	}{
		{
			name: "artifact differs", want: "provenance.runtime",
			edit: func(t *testing.T, evidence map[string]any) {
				run := asObject(t, arrayValue(t, evidence, "runs")[1], "run")
				runtime := objectValue(t, objectValue(t, run, "provenance"), "runtime")
				runtime["artifact_digest"] = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
				runtime["artifact_id"] = "gateway:" + runtime["artifact_digest"].(string)
			},
		},
		{
			name: "restart reuses process", want: "state.evidence",
			edit: func(t *testing.T, evidence map[string]any) {
				runs := arrayValue(t, evidence, "runs")
				before := stringValue(t, objectValue(t, asObject(t, runs[1], "run"), "provenance"), "process_instance_id")
				objectValue(t, asObject(t, runs[2], "run"), "provenance")["process_instance_id"] = before
				objectValue(t, asObject(t, runs[3], "run"), "provenance")["process_instance_id"] = before
			},
		},
		{
			name: "rollback disconnects", want: "provenance.config",
			edit: func(t *testing.T, evidence map[string]any) {
				run := asObject(t, arrayValue(t, evidence, "runs")[3], "run")
				config := objectValue(t, objectValue(t, run, "provenance"), "config")
				payload := objectValue(t, config, "payload")
				payload["outbound_enabled"] = false
				config["config_hash"] = domainDigest(t, configDomainV1, payload)
			},
		},
		{
			name: "private key in protected output", want: "redaction.public",
			edit: func(t *testing.T, evidence map[string]any) {
				run := asObject(t, arrayValue(t, evidence, "runs")[1], "run")
				view := findView(t, run, "debug.ebus")
				objectValue(t, objectValue(t, view, "payload"), "data")["opaque"] = "-----BEGIN PRIVATE KEY-----"
				refreshViewHashes(t, evidence, run, view)
			},
		},
		{
			name: "candidate ref in public output", want: "anti_leak.candidate",
			edit: func(t *testing.T, evidence map[string]any) {
				run := asObject(t, arrayValue(t, evidence, "runs")[1], "run")
				view := findView(t, run, "mcp.eebus.v1.contract")
				objectValue(t, objectValue(t, view, "payload"), "data")["candidate_ref"] = "redacted:sha256:123456789abc"
				refreshViewHashes(t, evidence, run, view)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			inputs := capturedPublicInputs(t)
			evidence := asObject(t, decodeJSON(t, inputs.Evidence), "captured evidence")
			test.edit(t, evidence)
			refreshEvidenceIdentity(t, evidence)
			inputs.Evidence = canonicalJSON(t, evidence)
			assertCategory(t, VerifyPublic(inputs), test.want)
		})
	}
}

func TestMSP08CapturedRuntimeStatusCannotBeFabricated(t *testing.T) {
	inputs := capturedPublicInputs(t)
	status := asObject(t, decodeJSON(t, inputs.M7LiveStatus), "live status")
	status["fact_count"] = number(17)
	inputs.M7LiveStatus = canonicalJSON(t, status)
	assertCategory(t, VerifyPublic(inputs), "provenance.m7")
}

func TestMSP08PublicRedactionRejectsSecretsAndStableProtocolIdentity(t *testing.T) {
	for _, test := range []struct {
		name, key, value string
	}{
		{"private key field", "private_key", "opaque"},
		{"credential token", "token", "secret-value"},
		{"trust store", "trust_store", "opaque"},
		{"stable device ID", "device_id", "vr940-raw-id"},
		{"private IP", "opaque", "192.168.100.4"},
		{"MAC address", "opaque", "aa:bb:cc:dd:ee:ff"},
		{"raw SKI", "opaque", "b1b7197b064084e4cfef2365105d8d36ff185e5b"},
		{"private key PEM", "opaque", "-----BEGIN ENCRYPTED PRIVATE KEY-----"},
	} {
		t.Run(test.name, func(t *testing.T) {
			inputs := capturedPublicInputs(t)
			evidence := asObject(t, decodeJSON(t, inputs.Evidence), "captured evidence")
			run := asObject(t, arrayValue(t, evidence, "runs")[1], "run")
			view := findView(t, run, "debug.ebus")
			objectValue(t, objectValue(t, view, "payload"), "data")[test.key] = test.value
			refreshViewHashes(t, evidence, run, view)
			refreshEvidenceIdentity(t, evidence)
			inputs.Evidence = canonicalJSON(t, evidence)
			assertCategory(t, VerifyPublic(inputs), "redaction.public")
		})
	}
}

func capturedPublicInputs(t *testing.T) InputsV1 {
	t.Helper()
	repo := repoDir(t)
	inputs := InputsV1{
		Registry:               readFile(t, filepath.Join(repo, "internal/coexistence/contracts/multi-runtime-coexistence-registry-v1.json")),
		M7Registry:             readFile(t, filepath.Join(repo, "internal/candidatefacts/contracts/draft-candidate-fact-registry-v1.json")),
		M7LiveStatus:           readFile(t, filepath.Join(repo, "internal/coexistence/testdata/canonical/positive/live-public-status.json")),
		M7TerminalGraph:        readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/source-terminal-graph.json")),
		M7TerminalReplay:       readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/source-terminal-replay-result.json")),
		M7TerminalSourceBundle: readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/source/source-terminal-bundle.json")),
		M7TerminalSourceReplay: readFile(t, filepath.Join(repo, "internal/candidatefacts/testdata/canonical/source/source-terminal-replay-result.json")),
	}
	inputs.Evidence = buildCapturedPublicEvidence(t, inputs)
	return inputs
}

func capturedPublicHarnessArgs(t *testing.T, evidence string) []string {
	t.Helper()
	repo := repoDir(t)
	return []string{
		"verify-public", "--evidence", evidence,
		"--registry", filepath.Join(repo, "internal/coexistence/contracts/multi-runtime-coexistence-registry-v1.json"),
		"--m7-registry", filepath.Join(repo, "internal/candidatefacts/contracts/draft-candidate-fact-registry-v1.json"),
		"--m7-live-status", filepath.Join(repo, "internal/coexistence/testdata/canonical/positive/live-public-status.json"),
		"--m7-terminal-graph", filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/source-terminal-graph.json"),
		"--m7-terminal-replay", filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/source-terminal-replay-result.json"),
		"--m7-terminal-source-bundle", filepath.Join(repo, "internal/candidatefacts/testdata/canonical/source/source-terminal-bundle.json"),
		"--m7-terminal-source-replay", filepath.Join(repo, "internal/candidatefacts/testdata/canonical/source/source-terminal-replay-result.json"),
	}
}

func buildCapturedPublicEvidence(t *testing.T, inputs InputsV1) []byte {
	t.Helper()
	root := packageDir(t)
	evidence := loadObject(t, filepath.Join(root, "testdata/canonical/positive/evidence.json"))
	registry := asObject(t, decodeJSON(t, inputs.Registry), "registry")
	status := asObject(t, decodeJSON(t, inputs.M7LiveStatus), "live status")
	evidence["fixture_id"] = "MSP08-G18-LIVE-EVIDENCE-OFFLINE-001"
	evidence["evidence_class"] = "CAPTURED_RUNTIME_EVIDENCE"
	evidence["m7_binding"] = mergeTestObjects(
		map[string]any{"source_commit": coexLiveGatewayCommit, "docs_source_commit": coexLiveDocsCommit},
		objectValue(t, registry, "m7_live_binding"),
	)
	evidence["m7_live_status"] = cloneValue(t, objectValue(t, registry, "m7_live_status_binding"))
	objectValue(t, evidence, "scope")["live_vr940_claim"] = true

	clock := objectValue(t, evidence, "capture_clock")
	clock["verification_offset_ns"] = number(4_000_000_000)
	clock["clock_hash"] = domainDigest(t, clockDomainV1, withoutKeys(t, clock, "clock_hash"))

	template := asObject(t, arrayValue(t, evidence, "runs")[0], "template run")
	runtime := cloneObject(t, objectValue(t, objectValue(t, asObject(t, arrayValue(t, evidence, "runs")[1], "runtime source"), "provenance"), "runtime"))
	runtime["source_commit"] = "dddddddddddddddddddddddddddddddddddddddd"
	runtime["source_parent_commit"] = coexLiveGatewayCommit

	facts := make([]any, 0, len(status["facts"].([]any)))
	counts := map[string]int64{"RAW_ONLY": 0, "CANDIDATE": 0, "CONFLICTED": 0, "WITHHELD": 0}
	for _, rawFact := range status["facts"].([]any) {
		fact := rawFact.(map[string]any)
		facts = append(facts, map[string]any{
			"candidate_id": fact["candidate_id"], "status": fact["status"],
			"terminal_negative_state": fact["terminal_negative_state"], "visibility_channel": "CANDIDATE_DEBUG_REPLAY",
		})
		counts[fact["status"].(string)]++
	}

	processBefore := "process-11111111111111111111111111111111"
	processAfter := "process-22222222222222222222222222222222"
	states := []struct {
		name, outcome, process string
		graph                  bool
	}{
		{"EEBUS_CONNECTED_BASELINE", "CONNECTED_BASELINE_CAPTURED", processBefore, false},
		{"EEBUS_CONNECTED_RAW_WITHHELD", "RAW_WITHHELD_OBSERVED", processBefore, true},
		{"EEBUS_RESTART_PERSISTED", "RESTART_PERSISTED", processAfter, true},
		{"EEBUS_CONNECTED_ROLLBACK", "GRAPH_EVIDENCE_DROPPED", processAfter, false},
	}
	runs := make([]any, len(states))
	for index, definition := range states {
		run := cloneObject(t, template)
		run["run_id"] = "msp08-run-0" + string(rune('1'+index))
		run["state"] = definition.name
		run["capture_offset_ns"] = number(int64(index) * 1_000_000_000)
		provenance := objectValue(t, run, "provenance")
		provenance["process_instance_id"] = definition.process
		provenance["runtime"] = cloneValue(t, runtime)
		config := objectValue(t, provenance, "config")
		config["config_id"] = "msp08-live-0" + string(rune('1'+index))
		payload := objectValue(t, config, "payload")
		payload["eebus_runtime_enabled"] = true
		payload["candidate_graph_enabled"] = definition.graph
		payload["outbound_enabled"] = true
		payload["public_v2_enabled"] = false
		config["config_hash"] = domainDigest(t, configDomainV1, payload)

		stateFacts := []any{}
		if definition.graph {
			stateFacts = cloneValue(t, facts).([]any)
		}
		state := stateEvidenceV1(definition.outcome, true, definition.graph, 1,
			map[bool]int64{true: counts["RAW_ONLY"]}[definition.graph], 0, 0,
			map[bool]int64{true: counts["WITHHELD"]}[definition.graph], false, stateFacts)
		state["restart_transition"] = nil
		run["state_evidence"] = state

		for _, rawView := range arrayValue(t, run, "protected_views") {
			view := asObject(t, rawView, "protected view")
			meta := objectValue(t, objectValue(t, view, "payload"), "meta")
			meta["captured_at"] = "2026-07-20T00:00:0" + string(rune('0'+index)) + "Z"
			refreshViewHashes(t, evidence, run, view)
		}
		runs[index] = run
	}

	restart := restartTransitionFixture(t)
	objectValue(t, asObject(t, runs[2], "restart run"), "state_evidence")["restart_transition"] = restart
	for _, rawRun := range runs {
		run := rawRun.(map[string]any)
		provenance := objectValue(t, run, "provenance")
		provenance["immutable_inputs"] = capturedImmutableInputs(t, run, registry, inputs, restartIfPresent(run))
	}
	evidence["runs"] = runs
	refreshEvidenceIdentity(t, evidence)
	return canonicalJSON(t, evidence)
}

func capturedImmutableInputs(t *testing.T, run, registry map[string]any, inputs InputsV1, restart map[string]any) []any {
	t.Helper()
	result := make([]any, 0, 25)
	for _, rawView := range arrayValue(t, run, "protected_views") {
		view := rawView.(map[string]any)
		result = append(result, map[string]any{
			"input_id": "view:" + view["view_id"].(string), "kind": "PROTECTED_VIEW_PAYLOAD",
			"digest": view["raw_payload_hash"], "byte_length": number(int64(len(canonicalJSON(t, view["payload"])))),
		})
	}
	terminalGraph := asObject(t, decodeJSON(t, inputs.M7TerminalGraph), "terminal graph")
	terminalReplay := asObject(t, decodeJSON(t, inputs.M7TerminalReplay), "terminal replay")
	result = append(result,
		immutableInput("m7:terminal-graph", "M7_TERMINAL_GRAPH", terminalGraph["graph_hash"].(string), int64(len(canonicalJSON(t, terminalGraph)))),
		immutableInput("m7:terminal-replay", "M7_TERMINAL_REPLAY", terminalReplay["replay_hash"].(string), int64(len(canonicalJSON(t, terminalReplay)))),
		immutableInput("m7:registry", "M7_REGISTRY", "sha256:"+rawSHA256(inputs.M7Registry), int64(len(inputs.M7Registry))),
		immutableInput("m7:terminal-source-bundle", "M7_TERMINAL_SOURCE_BUNDLE", "sha256:"+rawSHA256(inputs.M7TerminalSourceBundle), int64(len(inputs.M7TerminalSourceBundle))),
		immutableInput("m7:terminal-source-replay", "M7_TERMINAL_SOURCE_REPLAY", "sha256:"+rawSHA256(inputs.M7TerminalSourceReplay), int64(len(inputs.M7TerminalSourceReplay))),
	)
	private := objectValue(t, registry, "m7_live_private_inputs")
	for _, definition := range []struct{ name, id, kind string }{
		{"graph", "m7:private-graph", "M7_PRIVATE_GRAPH"},
		{"replay", "m7:private-replay", "M7_PRIVATE_REPLAY"},
		{"source_bundle", "m7:private-source-bundle", "M7_PRIVATE_SOURCE_BUNDLE"},
		{"source_replay", "m7:private-source-replay", "M7_PRIVATE_SOURCE_REPLAY"},
	} {
		binding := objectValue(t, private, definition.name)
		result = append(result, immutableInput(definition.id, definition.kind, stringValue(t, binding, "digest"), intValue(t, binding, "byte_length")))
	}
	status := objectValue(t, registry, "m7_live_status_binding")
	result = append(result, immutableInput("m7:status-projection", "M7_PUBLIC_STATUS", stringValue(t, status, "content_hash"), int64(len(inputs.M7LiveStatus))))
	if restart != nil {
		for _, definition := range []struct{ id, kind, field, domain string }{
			{"restart:process-event", "RESTART_PROCESS_EVENT", "process_event", coexRestartProcessDomainV1},
			{"restart:before-snapshot", "RESTART_STATE_SNAPSHOT", "before_snapshot", coexRestartSnapshotDomainV1},
			{"restart:after-snapshot", "RESTART_STATE_SNAPSHOT", "after_snapshot", coexRestartSnapshotDomainV1},
			{"restart:session-event", "RESTART_SESSION_EVENT", "session_event", coexRestartSessionDomainV1},
		} {
			value := restart[definition.field]
			result = append(result, immutableInput(definition.id, definition.kind, domainDigest(t, definition.domain, value), int64(len(canonicalJSON(t, value)))))
		}
	}
	return result
}

func restartTransitionFixture(t *testing.T) map[string]any {
	t.Helper()
	beforeProcess := "process-11111111111111111111111111111111"
	afterProcess := "process-22222222222222222222222222222222"
	trust := "redacted:sha256:111111111111"
	peer := "redacted:sha256:222222222222"
	before := map[string]any{
		"process_instance_id": beforeProcess, "capture_offset_ns": number(1_000_000_000),
		"trust_state_id": trust, "peer_binding_id": peer, "session_id": "redacted:sha256:333333333333", "session_state": "CONNECTED",
	}
	after := map[string]any{
		"process_instance_id": afterProcess, "capture_offset_ns": number(2_000_000_000),
		"trust_state_id": trust, "peer_binding_id": peer, "session_id": "redacted:sha256:444444444444", "session_state": "CONNECTED",
	}
	eventID := "msp08-restart-event-01"
	return map[string]any{
		"event_id": eventID, "before_process_instance_id": beforeProcess, "after_process_instance_id": afterProcess,
		"before_trust_state_hash":  domainDigest(t, coexRestartTrustDomainV1, map[string]any{"trust_state_id": trust}),
		"after_trust_state_hash":   domainDigest(t, coexRestartTrustDomainV1, map[string]any{"trust_state_id": trust}),
		"before_peer_binding_hash": domainDigest(t, coexRestartPeerDomainV1, map[string]any{"peer_binding_id": peer}),
		"after_peer_binding_hash":  domainDigest(t, coexRestartPeerDomainV1, map[string]any{"peer_binding_id": peer}),
		"session_reconnected":      true,
		"process_event": map[string]any{
			"event_id": eventID, "event_type": "PROCESS_RESTART_OBSERVED", "before_process_instance_id": beforeProcess,
			"after_process_instance_id": afterProcess, "observed_at_offset_ns": number(2_000_000_000),
		},
		"before_snapshot": before,
		"after_snapshot":  after,
		"session_event": map[string]any{
			"event_id": "msp08-session-event-01", "event_type": "SESSION_RECONNECTED_OBSERVED", "process_instance_id": afterProcess,
			"session_id": after["session_id"], "observed_at_offset_ns": number(2_000_000_000), "state": "CONNECTED",
		},
	}
}

func restartIfPresent(run map[string]any) map[string]any {
	value := run["state_evidence"].(map[string]any)["restart_transition"]
	if value == nil {
		return nil
	}
	return value.(map[string]any)
}

func immutableInput(id, kind, digest string, size int64) map[string]any {
	return map[string]any{"input_id": id, "kind": kind, "digest": digest, "byte_length": number(size)}
}

func mergeTestObjects(left, right map[string]any) map[string]any {
	result := cloneMap(left)
	for key, value := range right {
		result[key] = value
	}
	return result
}

func cloneMap(input map[string]any) map[string]any {
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
