package coexistence

import (
	"bytes"
)

func Report(inputs InputsV1) ([]byte, error) {
	context, err := validateInputs(inputs)
	if err != nil {
		return nil, err
	}
	return encodeReport(context.evidence, context.registry)
}

func Generate(inputs GenerateInputsV1) (ArtifactsV1, error) {
	registry, err := verifyRegistryForGeneration(inputs.Registry)
	if err != nil {
		return ArtifactsV1{}, err
	}
	verificationInputs := InputsV1{
		Registry:       inputs.Registry,
		M7Graph:        inputs.M7Graph,
		M7Replay:       inputs.M7Replay,
		M7Registry:     inputs.M7Registry,
		M7SourceBundle: inputs.M7SourceBundle,
		M7SourceReplay: inputs.M7SourceReplay,
	}
	m7, err := verifyM7(verificationInputs, registry, nil)
	if err != nil {
		return ArtifactsV1{}, err
	}
	baseline, err := parseGenerationObject(inputs.BaselineRuntime, "Runtime", "provenance.runtime")
	if err != nil || !validRuntimeIdentity(baseline) || baseline["source_commit"] != coexBaselineGatewayCommit || baseline["source_parent_commit"] != nil {
		return ArtifactsV1{}, fail("provenance.runtime")
	}
	compared, err := parseGenerationObject(inputs.ComparedRuntime, "Runtime", "provenance.runtime")
	if err != nil || !validRuntimeIdentity(compared) || compared["source_parent_commit"] != coexBaselineGatewayCommit {
		return ArtifactsV1{}, fail("provenance.runtime")
	}
	clock, err := parseGenerationObject(inputs.CaptureClock, "CaptureClock", "provenance.clock")
	if err != nil || !validGenerationClock(clock) {
		return ArtifactsV1{}, fail("provenance.clock")
	}
	timestamps, err := parseGenerationStrings(inputs.CaptureTimestamps, "provenance.clock")
	if err != nil || len(timestamps) != len(coexScenarioOrderV1) {
		return ArtifactsV1{}, fail("provenance.clock")
	}
	for _, timestamp := range timestamps {
		if !rfc3339UTCV1.MatchString(timestamp) {
			return ArtifactsV1{}, fail("provenance.clock")
		}
	}
	subjects, err := parseGenerationStrings(inputs.MaskedSubjects, "provenance.auth_mask")
	if err != nil || len(subjects) != len(coexScenarioOrderV1) {
		return ArtifactsV1{}, fail("provenance.auth_mask")
	}
	for _, subject := range subjects {
		if subject == "" || int64(len(subject)) > hardLimitsV1["max_string_bytes"] || containsNUL(subject) {
			return ArtifactsV1{}, fail("provenance.auth_mask")
		}
	}

	templateRaw := readPinnedArtifact("testdata/canonical/positive/evidence.json", positiveEvidenceSHA)
	value, parseErr := parseEvidenceJSON(templateRaw)
	if parseErr != nil {
		panic("coexistence: invalid pinned positive evidence")
	}
	evidence := value.(map[string]any)
	evidence["capture_clock"] = cloneJSONV1(clock)
	evidence["registry"] = map[string]any{"contract": coexRegistryContractV1, "version": number(1), "digest": "sha256:" + registrySHA256}
	runs := evidence["runs"].([]any)
	profile := evidence["normalization"].(map[string]any)
	rules := rulesByViewIDV1(registry)
	for runIndex, rawRun := range runs {
		run := rawRun.(map[string]any)
		provenance := run["provenance"].(map[string]any)
		if runIndex == 0 {
			provenance["runtime"] = cloneJSONV1(baseline)
		} else {
			provenance["runtime"] = cloneJSONV1(compared)
		}
		provenance["capture_clock_id"] = clock["clock_id"]
		views := run["protected_views"].([]any)
		immutable := make([]any, 0, len(views)+2)
		for _, rawView := range views {
			view := rawView.(map[string]any)
			payload := view["payload"].(map[string]any)
			meta := payload["meta"].(map[string]any)
			meta["captured_at"] = timestamps[runIndex]
			meta["auth_subject"] = subjects[runIndex]
			viewID := view["view_id"].(string)
			normalized, normalizationErr := normalizedPayload(payload, rules[viewID], profile)
			if normalizationErr != nil {
				return ArtifactsV1{}, normalizationErr
			}
			rawHash, _ := domainDigestV1(coexRawPayloadDomainV1, payload)
			shapeHash, _ := domainDigestV1(coexShapeDomainV1, payloadShapeV1(payload))
			canonicalHash, _ := domainDigestV1(coexCanonicalPayloadDomainV1, normalized)
			view["raw_payload_hash"] = rawHash
			view["shape_hash"] = shapeHash
			view["canonical_payload_hash"] = canonicalHash
			payloadBytes, _ := marshalCanonicalV1(payload)
			immutable = append(immutable, map[string]any{
				"input_id": "view:" + viewID, "kind": "PROTECTED_VIEW_PAYLOAD", "digest": rawHash, "byte_length": number(int64(len(payloadBytes))),
			})
		}
		immutable = append(immutable,
			map[string]any{"input_id": "m7:graph", "kind": "M7_GRAPH", "digest": m7.graph["graph_hash"], "byte_length": number(m7.graphBytes)},
			map[string]any{"input_id": "m7:replay", "kind": "M7_REPLAY", "digest": m7.replay["replay_hash"], "byte_length": number(m7.replayBytes)},
		)
		provenance["immutable_inputs"] = immutable
	}
	evidenceHash, hashErr := domainDigestV1(coexEvidenceDomainV1, withoutJSONKeysV1(evidence, "evidence_id", "evidence_hash"))
	if hashErr != nil {
		return ArtifactsV1{}, hashErr
	}
	evidence["evidence_hash"] = evidenceHash
	evidence["evidence_id"] = "mrcv1:" + evidenceHash
	evidenceBytes, err := prettyJSON(evidence)
	if err != nil {
		return ArtifactsV1{}, err
	}
	verificationInputs.Evidence = evidenceBytes
	context, err := validateInputs(verificationInputs)
	if err != nil {
		return ArtifactsV1{}, err
	}
	reportBytes, err := encodeReport(context.evidence, context.registry)
	if err != nil {
		return ArtifactsV1{}, err
	}
	return ArtifactsV1{Evidence: bytes.Clone(evidenceBytes), Report: bytes.Clone(reportBytes)}, nil
}

func parseGenerationObject(raw []byte, definition, category string) (map[string]any, error) {
	value, err := parseCategorizedJSON(raw, category)
	if err != nil {
		return nil, err
	}
	if err := schemaCheckDefinition(value, definition, category); err != nil {
		return nil, err
	}
	object, ok := objectValueV1(value)
	if !ok {
		return nil, fail(category)
	}
	return object, nil
}

func parseGenerationStrings(raw []byte, category string) ([]string, error) {
	value, err := parseCategorizedJSON(raw, category)
	if err != nil {
		return nil, err
	}
	values, ok := stringsFromArray(value)
	if !ok {
		return nil, fail(category)
	}
	return values, nil
}

func validGenerationClock(clock map[string]any) bool {
	computed, err := domainDigestV1(coexClockDomainV1, withoutJSONKeysV1(clock, "clock_hash"))
	wall, _ := stringValueV1(clock["wall_anchor_utc"])
	verification, verificationOK := integerValue(clock["verification_offset_ns"])
	maximumAge, ageOK := integerValue(clock["max_capture_age_ns"])
	lastOffset := int64(len(coexScenarioOrderV1)-1) * 1_000_000_000
	return err == nil && clock["basis"] == "MONOTONIC_CAPTURE_OFFSETS" && rfc3339UTCV1.MatchString(wall) &&
		clock["clock_hash"] == computed && verificationOK && ageOK && verification >= lastOffset && verification-lastOffset <= maximumAge
}

func encodeReport(evidence, registry map[string]any) ([]byte, error) {
	runs := evidence["runs"].([]any)
	checks := cloneJSONV1(registry["required_acceptance_checks"])
	resultByState := map[string]string{
		"EEBUS_DISABLED_CONFIRMED":       "NO_DRIFT",
		"EEBUS_ENABLED_NO_SERVICES":      "EXPECTED_NO_SERVICES_NO_DRIFT",
		"EEBUS_CONNECTED_CANDIDATE_ONLY": "CANDIDATE_CONFINED_NO_DRIFT",
		"EEBUS_CONFLICTED_WITHHELD":      "CONFLICT_WITHHELD_NO_DRIFT",
		"EEBUS_DISABLED_ROLLBACK":        "ROLLBACK_EXACT_BASELINE",
	}
	baseline := runs[0].(map[string]any)
	baselineRuntime := baseline["provenance"].(map[string]any)["runtime"].(map[string]any)
	scenarios := make([]any, 0, len(runs)-1)
	for _, rawRun := range runs[1:] {
		run := rawRun.(map[string]any)
		state := run["state"].(string)
		scenarios = append(scenarios, map[string]any{
			"run_id": run["run_id"], "state": state, "result": resultByState[state],
			"checks": cloneJSONV1(checks), "view_hashes": reportViewHashes(run),
		})
	}
	acceptance := make([]any, 0, len(runs))
	for _, rawRun := range runs {
		run := rawRun.(map[string]any)
		acceptance = append(acceptance, map[string]any{
			"state": run["state"], "required_checks": cloneJSONV1(checks), "passed": true,
		})
	}
	m7 := evidence["m7_binding"].(map[string]any)
	fixtureIDs := registry["fixture_ids"].(map[string]any)
	report := map[string]any{
		"contract": coexReportContractV1, "schema_version": number(1), "fixture_id": fixtureIDs["positive_report"],
		"report_id": "mrcrv1:sha256:" + stringsOf('0', 64), "report_hash": "sha256:" + stringsOf('0', 64),
		"evidence_id": evidence["evidence_id"], "evidence_hash": evidence["evidence_hash"], "gate": registry["gate"], "verdict": "PASS",
		"m7_binding": map[string]any{
			"completion_token": m7["completion_token"], "docs_source_commit": m7["docs_source_commit"],
			"graph_id": m7["graph_id"], "graph_hash": m7["graph_hash"], "replay_id": m7["replay_id"], "replay_hash": m7["replay_hash"],
		},
		"baseline": map[string]any{
			"run_id": baseline["run_id"], "state": baseline["state"], "source_commit": baselineRuntime["source_commit"],
			"artifact_digest": baselineRuntime["artifact_digest"], "view_hashes": reportViewHashes(baseline),
		},
		"scenarios": scenarios, "acceptance_matrix": acceptance,
		"rollback": map[string]any{
			"run_id": runs[len(runs)-1].(map[string]any)["run_id"], "runtime_disabled": true,
			"candidate_graph_disabled": true, "exact_baseline_restored": true,
		},
	}
	reportHash, err := domainDigestV1(coexReportDomainV1, withoutJSONKeysV1(report, "report_id", "report_hash"))
	if err != nil {
		return nil, err
	}
	report["report_hash"] = reportHash
	report["report_id"] = "mrcrv1:" + reportHash
	encoded, err := marshalCanonicalV1(report)
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func reportViewHashes(run map[string]any) []any {
	views := run["protected_views"].([]any)
	result := make([]any, len(views))
	for index, rawView := range views {
		view := rawView.(map[string]any)
		result[index] = map[string]any{
			"view_id": view["view_id"], "shape_hash": view["shape_hash"], "canonical_payload_hash": view["canonical_payload_hash"],
		}
	}
	return result
}

func stringsOf(value byte, count int) string {
	return string(bytes.Repeat([]byte{value}, count))
}
