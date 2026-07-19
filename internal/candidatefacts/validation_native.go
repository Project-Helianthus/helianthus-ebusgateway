package candidatefacts

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"sort"
	"strings"
)

func validateEvidenceRef(value any) error {
	if !exactKeys(value, "kind", "digest_algorithm", "digest", "repository", "commit", "path") {
		return fail("provenance.binding")
	}
	ref, _ := objectValue(value)
	digest, ok := stringValue(ref["digest"])
	if !ok || !digestPattern.MatchString(digest) {
		return fail("provenance.binding")
	}
	switch ref["kind"] {
	case "CONTENT":
		if ref["digest_algorithm"] != "SHA256_CONTENT_BYTES" || ref["repository"] != nil || ref["commit"] != nil || ref["path"] != nil {
			return fail("provenance.binding")
		}
	case "GIT_BLOB":
		repository, repositoryOK := stringValue(ref["repository"])
		commit, commitOK := stringValue(ref["commit"])
		path, pathOK := stringValue(ref["path"])
		if ref["digest_algorithm"] != "SHA256_GIT_BLOB_V1" || !repositoryOK || repository == "" || !commitOK || !commitPattern.MatchString(commit) || !pathOK || path == "" {
			return fail("provenance.binding")
		}
	default:
		return fail("provenance.binding")
	}
	return nil
}

func checkIdentities(graph, sourceBundle map[string]any) error {
	artifacts := indexArtifacts(sourceBundle)
	facts, _ := arrayValue(graph["facts"])
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		provenance, _ := objectValue(fact["provenance"])
		if provenance["ebus"] == nil {
			if provenance["ebus_source_id"] != nil || provenance["ebus_artifact_id"] != nil {
				return fail("identity.native")
			}
		} else {
			if !token(provenance["ebus_source_id"]) || !token(provenance["ebus_artifact_id"]) {
				return fail("identity.native")
			}
			identity, ok := objectValue(provenance["ebus"])
			if !ok || validateEBusIdentity(identity) != nil {
				return fail("identity.native")
			}
			key, _ := pairKey(provenance["ebus_source_id"], provenance["ebus_artifact_id"])
			artifact, ok := artifacts[key]
			if !ok || !reflect.DeepEqual(identity, artifact["ebus_identity"]) {
				return fail("identity.native")
			}
		}
		if provenance["eebus"] == nil {
			if provenance["eebus_source_id"] != nil || provenance["eebus_artifact_id"] != nil {
				return fail("identity.native")
			}
		} else {
			if !token(provenance["eebus_source_id"]) || !token(provenance["eebus_artifact_id"]) {
				return fail("identity.native")
			}
			identity, ok := objectValue(provenance["eebus"])
			if !ok || validateEEBusPath(identity) != nil {
				return fail("identity.native")
			}
			key, _ := pairKey(provenance["eebus_source_id"], provenance["eebus_artifact_id"])
			artifact, ok := artifacts[key]
			if !ok || !artifactProvesEEBusPath(artifact, identity) {
				return fail("identity.native")
			}
		}
		if cloud := provenance["cloud"]; cloud != nil {
			if !exactKeys(cloud, "source_id", "artifact_id", "evidence_id") {
				return fail("identity.native")
			}
			cloudObject, _ := objectValue(cloud)
			if !token(cloudObject["source_id"]) || !token(cloudObject["artifact_id"]) || !token(cloudObject["evidence_id"]) {
				return fail("identity.native")
			}
		}
	}
	return nil
}

func validateEBusIdentity(identity map[string]any) error {
	family, _ := stringValue(identity["family"])
	if !token(identity["target_pseudonym"]) || !token(identity["unit_scale_source"]) {
		return fail("identity.native")
	}
	switch family {
	case "B509":
		if !exactKeys(identity, "family", "target_pseudonym", "target_address", "target_product", "register_family", "register_id", "unit_scale_source", "evidence_role") ||
			!boundedInteger(identity["target_address"], 0, 255) || !token(identity["target_product"]) || !token(identity["register_family"]) ||
			!boundedInteger(identity["register_id"], 0, 65535) || !member(identity["evidence_role"], "AUTHORITATIVE", "MIRROR", "FALLBACK") {
			return fail("identity.native")
		}
	case "B524":
		if !exactKeys(identity, "family", "target_pseudonym", "opcode", "GG", "II", "RR", "target_address", "source_address", "group_meaning", "instance_gate", "register_category", "unit_scale_source") ||
			!boundedInteger(identity["opcode"], 0, 255) || !boundedInteger(identity["GG"], 0, 255) || !boundedInteger(identity["II"], 0, 255) ||
			!boundedInteger(identity["RR"], 0, 65535) || !boundedInteger(identity["target_address"], 0, 255) || !boundedInteger(identity["source_address"], 0, 255) ||
			!token(identity["group_meaning"]) || !token(identity["instance_gate"]) || !member(identity["register_category"], "STATE", "CONFIG", "PARAMS") {
			return fail("identity.native")
		}
	case "B555":
		timeIdentity, timeOK := stringValue(identity["time_identity"])
		if !exactKeys(identity, "family", "target_pseudonym", "device_family", "schedule_program", "slot_index", "day_of_week", "time_identity", "operation_mode_context", "unit_scale_source") ||
			!token(identity["device_family"]) || !token(identity["schedule_program"]) || !boundedInteger(identity["slot_index"], 0, 255) ||
			!member(identity["day_of_week"], "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY") ||
			!timeOK || !timePattern.MatchString(timeIdentity) || !token(identity["operation_mode_context"]) {
			return fail("identity.native")
		}
	default:
		return fail("identity.native")
	}
	return nil
}

func validateEEBusPath(identity map[string]any) error {
	if !exactKeys(identity, "service", "entity", "feature", "feature_path") || !token(identity["service"]) || !token(identity["entity"]) || !token(identity["feature"]) {
		return fail("identity.native")
	}
	path, ok := arrayValue(identity["feature_path"])
	if !ok || len(path) < 3 || len(path) > 32 {
		return fail("identity.native")
	}
	for index, raw := range path {
		if !exactKeys(raw, "kind", "selector") {
			return fail("identity.native")
		}
		segment, _ := objectValue(raw)
		if !member(segment["kind"], "SERVICE", "ENTITY", "FEATURE", "FIELD") || !token(segment["selector"]) {
			return fail("identity.native")
		}
		if index >= 3 && segment["kind"] != "FIELD" {
			return fail("identity.native")
		}
	}
	wantKinds := []string{"SERVICE", "ENTITY", "FEATURE"}
	wantSelectors := []any{identity["service"], identity["entity"], identity["feature"]}
	for index := range wantKinds {
		segment, _ := objectValue(path[index])
		if segment["kind"] != wantKinds[index] || segment["selector"] != wantSelectors[index] {
			return fail("identity.native")
		}
	}
	return nil
}

func artifactProvesEEBusPath(artifact, identity map[string]any) bool {
	normalized, ok := objectValue(artifact["normalized_evidence"])
	if !ok {
		return false
	}
	data, ok := objectValue(normalized["data"])
	if !ok {
		return false
	}
	services, ok := arrayValue(data["services"])
	if !ok {
		return false
	}
	serviceFound := false
	for _, raw := range services {
		service, ok := objectValue(raw)
		if !ok {
			continue
		}
		id, ok := objectValue(service["id"])
		if ok && id["digest"] == identity["service"] {
			serviceFound = true
		}
	}
	paths, ok := arrayValue(data["feature_paths"])
	if !serviceFound || !ok {
		return false
	}
	for _, path := range paths {
		if reflect.DeepEqual(path, identity) {
			return true
		}
	}
	return false
}

func checkOrdering(graph map[string]any) error {
	bundle, _ := objectValue(graph["source_bundle"])
	refs, _ := arrayValue(bundle["evidence_refs"])
	if !ordered(refs, evidenceRefLess) {
		return fail("ordering.invalid")
	}
	facts, _ := arrayValue(graph["facts"])
	if !ordered(facts, factLess) {
		return fail("ordering.invalid")
	}
	ids, paths := map[string]bool{}, map[string]bool{}
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		id, _ := stringValue(fact["candidate_id"])
		path, _ := stringValue(fact["proposed_path"])
		if ids[id] || paths[path] {
			return fail("ordering.invalid")
		}
		ids[id], paths[path] = true, true
		provenance, _ := objectValue(fact["provenance"])
		factRefs, _ := arrayValue(provenance["native_evidence_refs"])
		if !ordered(factRefs, evidenceRefLess) {
			return fail("ordering.invalid")
		}
		comparator, _ := objectValue(fact["comparator"])
		samples, _ := arrayValue(comparator["samples"])
		if !ordered(samples, sampleLess) {
			return fail("ordering.invalid")
		}
		trigger, _ := objectValue(fact["retest_trigger"])
		required, ok := stringArray(trigger["required_source_kinds"])
		if !ok || !sort.StringsAreSorted(required) || hasDuplicateStrings(required) {
			return fail("ordering.invalid")
		}
	}
	return nil
}

func checkStates(graph, registry map[string]any) error {
	statuses, _ := stringSet(registry["statuses"])
	terminals, _ := stringSet(registry["terminal_negative_states"])
	facts, _ := arrayValue(graph["facts"])
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		id, idOK := stringValue(fact["candidate_id"])
		path, pathOK := stringValue(fact["proposed_path"])
		status, statusOK := stringValue(fact["status"])
		terminal, terminalOK := optionalString(fact["terminal_negative_state"])
		if !idOK || !candidateIDPattern.MatchString(id) || !pathOK || !pathPattern.MatchString(path) || !statusOK || !statuses[status] || !terminalOK || terminal != "" && !terminals[terminal] || fact["debug_only"] != true {
			return fail("state.terminal")
		}
		if terminal != "" {
			if status != "WITHHELD" || fact["draft_value"] != nil || fact["draft_unit"] != nil {
				return fail("state.terminal")
			}
		} else if status == "WITHHELD" {
			return fail("state.terminal")
		}
		confidence, _ := objectValue(fact["confidence"])
		if !member(confidence["level"], "LOW", "MEDIUM", "HIGH") || !member(confidence["basis"], "OBSERVED", "INFERRED", "INSUFFICIENT") || !boundedInteger(confidence["score_milli"], 0, 1000) {
			return fail("state.terminal")
		}
		falsifier, _ := objectValue(fact["falsifier"])
		if !member(falsifier["condition_code"], "VALUE_DIVERGES", "IDENTITY_CHANGES", "SIGNAL_DISAPPEARS", "ORDER_CHANGES", "PROVENANCE_BREAKS") || !terminals[asString(falsifier["expected_terminal_state"])] || !token(falsifier["description"]) {
			return fail("state.terminal")
		}
		trigger, _ := objectValue(fact["retest_trigger"])
		required, requiredOK := stringArray(trigger["required_source_kinds"])
		if !member(trigger["trigger_code"], "NEW_SYNCHRONIZED_BUNDLE", "SOURCE_RECOVERED", "IDENTITY_CONFIRMED", "COMPARATOR_REVISED") || !boundedInteger(trigger["minimum_new_samples"], 1, 1024) || !requiredOK || len(required) == 0 {
			return fail("state.terminal")
		}
		for _, kind := range required {
			if kind != "CLOUD_APP" && kind != "EBUS" && kind != "EEBUS" {
				return fail("state.terminal")
			}
		}
	}
	return nil
}

func checkComparators(graph, registry map[string]any) error {
	drafts, _ := arrayValue(graph["comparator_drafts"])
	if len(drafts) != 1 {
		return fail("comparator.invalid")
	}
	draft, _ := objectValue(drafts[0])
	if draft["draft_id"] != "NUMERIC_WINDOW_V1_DRAFT" || draft["type"] != "NUMERIC_WINDOW" {
		return fail("comparator.invalid")
	}
	registryComparators, ok := arrayValue(registry["comparators"])
	if !ok || len(registryComparators) == 0 {
		return fail("comparator.invalid")
	}
	registryDraft, ok := objectValue(registryComparators[0])
	if !ok || registryDraft["draft_id"] != draft["draft_id"] {
		return fail("comparator.invalid")
	}
	parameters, _ := objectValue(draft["parameters"])
	window, windowOK := exactObject(parameters["window"], "start_offset_ns", "end_offset_ns")
	tolerance, toleranceOK := exactObject(parameters["tolerance"], "absolute_decimal", "relative_ppm")
	conversion, conversionOK := exactObject(parameters["unit_conversion"], "mode", "source_unit", "target_unit", "scale_decimal", "offset_decimal")
	rounding, roundingOK := exactObject(parameters["rounding"], "mode", "decimal_places")
	threshold, thresholdOK := exactObject(parameters["conflict_threshold"], "absolute_decimal", "consecutive_samples")
	start, startOK := integer(window["start_offset_ns"])
	end, endOK := integer(window["end_offset_ns"])
	if !windowOK || !toleranceOK || !conversionOK || !roundingOK || !thresholdOK || !startOK || !endOK || start < 0 || start >= end ||
		!boundedInteger(tolerance["relative_ppm"], 0, maxSafeIntegerV1) || !member(conversion["mode"], "IDENTITY", "AFFINE") || !token(conversion["source_unit"]) || !token(conversion["target_unit"]) ||
		!member(rounding["mode"], "NONE", "HALF_EVEN") || rounding["decimal_places"] != nil && !boundedInteger(rounding["decimal_places"], 0, 9) ||
		!boundedInteger(parameters["minimum_samples"], 1, maxSafeIntegerV1) || !boundedInteger(parameters["maximum_missing_samples"], 0, maxSafeIntegerV1) ||
		!boundedInteger(parameters["stale_cutoff_ns"], 1, maxSafeIntegerV1) || !boundedInteger(threshold["consecutive_samples"], 1, maxSafeIntegerV1) ||
		!decimalString(tolerance["absolute_decimal"], true) || !decimalString(conversion["scale_decimal"], false) || !decimalString(conversion["offset_decimal"], false) || !decimalString(threshold["absolute_decimal"], true) {
		return fail("comparator.invalid")
	}
	minimumSamples, _ := integer(parameters["minimum_samples"])
	maximumMissing, _ := integer(parameters["maximum_missing_samples"])
	facts, _ := arrayValue(graph["facts"])
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		evaluation, _ := objectValue(fact["comparator"])
		if evaluation["draft_id"] != draft["draft_id"] {
			return fail("comparator.invalid")
		}
		samples, _ := arrayValue(evaluation["samples"])
		present, missing := int64(0), int64(0)
		for _, sampleRaw := range samples {
			sample, _ := objectValue(sampleRaw)
			offset, offsetOK := integer(sample["offset_ns"])
			state, stateOK := stringValue(sample["state"])
			if !offsetOK || offset < start || offset > end || !stateOK || state != "PRESENT" && state != "MISSING" && state != "STALE" {
				return fail("comparator.invalid")
			}
			if state == "MISSING" {
				missing++
				if sample["left_decimal"] != nil || sample["right_decimal"] != nil {
					return fail("comparator.invalid")
				}
			} else {
				if !decimalString(sample["left_decimal"], false) || !decimalString(sample["right_decimal"], false) {
					return fail("comparator.invalid")
				}
				if state == "PRESENT" {
					present++
				}
			}
		}
		if missing > maximumMissing {
			return fail("comparator.invalid")
		}
		status := asString(fact["status"])
		outcome := asString(evaluation["outcome"])
		allowed := map[string]map[string]bool{
			"RAW_ONLY": {"NOT_EVALUATED": true}, "CANDIDATE": {"MATCH": true}, "CONFLICTED": {"CONFLICT": true},
			"WITHHELD": {"NOT_EVALUATED": true, "CONFLICT": true},
		}
		if !allowed[status][outcome] || (status == "CANDIDATE" || status == "CONFLICTED") && present < minimumSamples {
			return fail("comparator.invalid")
		}
		terminal, _ := optionalString(fact["terminal_negative_state"])
		if terminal == "CONFLICT" && outcome != "CONFLICT" || (terminal == "NO_SIGNAL" || terminal == "CLOUD_ONLY" || terminal == "NOT_TESTED") && outcome != "NOT_EVALUATED" {
			return fail("comparator.invalid")
		}
	}
	return nil
}

func checkAntiLeak(graph, registry map[string]any) error {
	visibility, _ := objectValue(graph["visibility"])
	want := map[string]any{
		"channel": registry["candidate_channel"], "promotion_state": "NOT_PROMOTED", "stable_exposure": false,
		"command_capable": false, "protocol_translation": false,
	}
	if !reflect.DeepEqual(visibility, want) {
		return fail("anti_leak.consumer")
	}
	return nil
}

func checkHashes(graph map[string]any) error {
	facts, _ := arrayValue(graph["facts"])
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		view := cloneObject(fact)
		delete(view, "fact_hash")
		canonical, err := canonicalJSON(view)
		if err != nil || fact["fact_hash"] != "sha256:"+domainHex(factDomainV1, canonical) {
			return fail("hash.fact")
		}
	}
	view := cloneObject(graph)
	delete(view, "graph_id")
	delete(view, "graph_hash")
	canonical, err := canonicalJSON(view)
	if err != nil {
		return fail("hash.graph")
	}
	hexdigest := domainHex(graphDomainV1, canonical)
	if graph["graph_hash"] != "sha256:"+hexdigest || graph["graph_id"] != "dcfgv1:sha256:"+hexdigest {
		return fail("hash.graph")
	}
	return nil
}

func numberEquals(value any, expected int64) bool {
	parsed, ok := integer(value)
	return ok && parsed == expected
}

func member(value any, allowed ...string) bool {
	text, ok := stringValue(value)
	if !ok {
		return false
	}
	for _, candidate := range allowed {
		if text == candidate {
			return true
		}
	}
	return false
}

func optionalString(value any) (string, bool) {
	if value == nil {
		return "", true
	}
	return stringValue(value)
}

func exactObject(value any, keys ...string) (map[string]any, bool) {
	object, ok := objectValue(value)
	return object, ok && exactKeys(object, keys...)
}

func indexSources(bundle map[string]any) map[string]map[string]any {
	result := map[string]map[string]any{}
	rows, _ := arrayValue(bundle["sources"])
	for _, raw := range rows {
		row, ok := objectValue(raw)
		if ok {
			result[asString(row["source_id"])] = row
		}
	}
	return result
}

func indexArtifacts(bundle map[string]any) map[string]map[string]any {
	result := map[string]map[string]any{}
	rows, _ := arrayValue(bundle["artifacts"])
	for _, raw := range rows {
		row, ok := objectValue(raw)
		key, keyOK := pairKey(row["source_id"], row["artifact_id"])
		if ok && keyOK {
			result[key] = row
		}
	}
	return result
}

func pairKey(source, artifact any) (string, bool) {
	sourceID, sourceOK := stringValue(source)
	artifactID, artifactOK := stringValue(artifact)
	return sourceID + "\x00" + artifactID, sourceOK && artifactOK
}

func asString(value any) string {
	text, _ := stringValue(value)
	return text
}

func containsString(value any, target string) bool {
	values, ok := stringArray(value)
	if !ok {
		return false
	}
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func stringArray(value any) ([]string, bool) {
	rows, ok := arrayValue(value)
	if !ok {
		return nil, false
	}
	result := make([]string, len(rows))
	for index, raw := range rows {
		text, ok := stringValue(raw)
		if !ok {
			return nil, false
		}
		result[index] = text
	}
	return result, true
}

func stringSet(value any) (map[string]bool, bool) {
	values, ok := stringArray(value)
	if !ok {
		return nil, false
	}
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result, true
}

func hasDuplicateStrings(values []string) bool {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if seen[value] {
			return true
		}
		seen[value] = true
	}
	return false
}

func ordered(values []any, less func(any, any) bool) bool {
	if len(values) < 2 {
		return true
	}
	sorted := append([]any(nil), values...)
	sort.SliceStable(sorted, func(i, j int) bool { return less(sorted[i], sorted[j]) })
	return reflect.DeepEqual(values, sorted)
}

func evidenceRefLess(leftRaw, rightRaw any) bool {
	left, _ := objectValue(leftRaw)
	right, _ := objectValue(rightRaw)
	for _, field := range []string{"kind", "digest_algorithm", "digest", "repository", "commit", "path"} {
		leftValue, rightValue := nullableSortValue(left[field]), nullableSortValue(right[field])
		if leftValue != rightValue {
			return leftValue < rightValue
		}
	}
	return false
}

func nullableSortValue(value any) string {
	if value == nil {
		return "0"
	}
	return "1" + asString(value)
}

func factLess(leftRaw, rightRaw any) bool {
	left, _ := objectValue(leftRaw)
	right, _ := objectValue(rightRaw)
	leftPath, rightPath := asString(left["proposed_path"]), asString(right["proposed_path"])
	if leftPath != rightPath {
		return leftPath < rightPath
	}
	return asString(left["candidate_id"]) < asString(right["candidate_id"])
}

func sampleLess(leftRaw, rightRaw any) bool {
	left, _ := objectValue(leftRaw)
	right, _ := objectValue(rightRaw)
	leftOffset, _ := integer(left["offset_ns"])
	rightOffset, _ := integer(right["offset_ns"])
	if leftOffset != rightOffset {
		return leftOffset < rightOffset
	}
	leftCanonical, _ := canonicalJSON(left)
	rightCanonical, _ := canonicalJSON(right)
	return strings.Compare(string(leftCanonical), string(rightCanonical)) < 0
}

func canonicalKey(value any) (string, error) {
	canonical, err := canonicalJSON(value)
	return string(canonical), err
}

func cloneObject(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func domainHex(domain string, canonical []byte) string {
	digest := sha256Bytes(append(append([]byte(domain), 0), canonical...))
	return digest
}

func sha256Bytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func countPathSegments(path string) int {
	count := 0
	for _, segment := range strings.Split(path, "/") {
		if segment != "" {
			count++
		}
	}
	return count
}
