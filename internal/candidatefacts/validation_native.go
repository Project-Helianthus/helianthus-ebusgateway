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
		if ref["digest_algorithm"] != "SHA256_CONTENT_BYTES" ||
			ref["repository"] != nil || ref["commit"] != nil || ref["path"] != nil {
			return fail("provenance.binding")
		}
	case "GIT_BLOB":
		repository, repositoryOK := stringValue(ref["repository"])
		commit, commitOK := stringValue(ref["commit"])
		path, pathOK := stringValue(ref["path"])
		if ref["digest_algorithm"] != "SHA256_GIT_BLOB_V1" ||
			!repositoryOK || !token(repository) ||
			!commitOK || !commitPattern.MatchString(commit) ||
			!pathOK || len(path) == 0 || len(path) > 512 || !printableASCII(path) {
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

		eebusSource, eebusArtifact := provenance["eebus_source_id"], provenance["eebus_artifact_id"]
		if eebusSource == nil && eebusArtifact == nil {
			if provenance["eebus_service"] != nil || provenance["eebus"] != nil {
				return fail("identity.native")
			}
		} else {
			service, ok := stringValue(provenance["eebus_service"])
			if !ok || !token(service) {
				return fail("identity.native")
			}
			key, _ := pairKey(eebusSource, eebusArtifact)
			artifact, ok := artifacts[key]
			if !ok || !artifactProvesEEBusService(artifact, service) {
				return fail("identity.native")
			}
			if provenance["eebus"] != nil {
				identity, ok := objectValue(provenance["eebus"])
				if !ok || validateEEBusPath(identity) != nil ||
					identity["service"] != service || !artifactProvesEEBusPath(artifact, identity) {
					return fail("identity.native")
				}
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
		if !exactKeys(identity,
			"family", "target_pseudonym", "target_address", "target_product",
			"register_family", "register_id", "unit_scale_source", "evidence_role",
		) ||
			!boundedInteger(identity["target_address"], 0, 255) ||
			!token(identity["target_product"]) || !token(identity["register_family"]) ||
			!boundedInteger(identity["register_id"], 0, 65535) ||
			!member(identity["evidence_role"], "AUTHORITATIVE", "MIRROR", "FALLBACK") {
			return fail("identity.native")
		}
	case "B524":
		if !exactKeys(identity,
			"family", "target_pseudonym", "opcode", "GG", "II", "RR",
			"target_address", "source_address", "group_meaning", "instance_gate",
			"register_category", "unit_scale_source",
		) ||
			!boundedInteger(identity["opcode"], 0, 255) ||
			!boundedInteger(identity["GG"], 0, 255) ||
			!boundedInteger(identity["II"], 0, 255) ||
			!boundedInteger(identity["RR"], 0, 65535) ||
			!boundedInteger(identity["target_address"], 0, 255) ||
			!boundedInteger(identity["source_address"], 0, 255) ||
			!token(identity["group_meaning"]) || !token(identity["instance_gate"]) ||
			!member(identity["register_category"], "STATE", "CONFIG", "PARAMS") {
			return fail("identity.native")
		}
	case "B555":
		timeIdentity, timeOK := stringValue(identity["time_identity"])
		if !exactKeys(identity,
			"family", "target_pseudonym", "device_family", "schedule_program",
			"slot_index", "day_of_week", "time_identity", "operation_mode_context",
			"unit_scale_source",
		) ||
			!token(identity["device_family"]) || !token(identity["schedule_program"]) ||
			!boundedInteger(identity["slot_index"], 0, 255) ||
			!member(identity["day_of_week"],
				"MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY",
			) ||
			!timeOK || !timePattern.MatchString(timeIdentity) ||
			!token(identity["operation_mode_context"]) {
			return fail("identity.native")
		}
	default:
		return fail("identity.native")
	}
	return nil
}

func validateEEBusPath(identity map[string]any) error {
	if !exactKeys(identity, "service", "entity", "feature", "feature_path") ||
		!token(identity["service"]) || !token(identity["entity"]) || !token(identity["feature"]) {
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
		if !member(segment["kind"], "SERVICE", "ENTITY", "FEATURE", "FIELD") ||
			!token(segment["selector"]) || index >= 3 && segment["kind"] != "FIELD" {
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

func artifactProvesEEBusService(artifact map[string]any, target string) bool {
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
	for _, raw := range services {
		service, ok := objectValue(raw)
		if !ok {
			continue
		}
		id, ok := objectValue(service["id"])
		if ok && id["digest"] == target {
			return true
		}
	}
	return false
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
	paths, ok := arrayValue(data["feature_paths"])
	if !ok {
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
		seenSamples := make(map[string]bool, len(samples))
		for _, sample := range samples {
			key, err := canonicalKey(sample)
			if err != nil || seenSamples[key] {
				return fail("ordering.invalid")
			}
			seenSamples[key] = true
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
		id, _ := stringValue(fact["candidate_id"])
		path, _ := stringValue(fact["proposed_path"])
		status, _ := stringValue(fact["status"])
		terminal, terminalOK := optionalString(fact["terminal_negative_state"])
		if !candidateIDPattern.MatchString(id) || !pathPattern.MatchString(path) ||
			!statuses[status] || !terminalOK || terminal != "" && !terminals[terminal] ||
			fact["debug_only"] != true {
			return fail("state.terminal")
		}
		provenance, _ := objectValue(fact["provenance"])
		nativeKinds := map[string]bool{}
		if provenance["ebus_source_id"] != nil {
			nativeKinds["EBUS"] = true
		}
		if provenance["eebus_source_id"] != nil {
			nativeKinds["EEBUS"] = true
		}
		cloudOnly := provenance["cloud"] != nil && len(nativeKinds) == 0
		comparator, _ := objectValue(fact["comparator"])
		samples, _ := arrayValue(comparator["samples"])
		outcome := asString(comparator["outcome"])

		if cloudOnly && (status != "WITHHELD" || terminal != "CLOUD_ONLY") {
			return fail("state.terminal")
		}
		switch status {
		case "RAW_ONLY":
			if terminal != "" || fact["draft_value"] != nil || fact["draft_unit"] != nil ||
				len(samples) != 0 || outcome != "NOT_EVALUATED" {
				return fail("state.terminal")
			}
		case "CANDIDATE":
			if terminal != "" || fact["draft_value"] == nil || fact["draft_unit"] == nil ||
				outcome != "MATCH" || !nativeKinds["EBUS"] || !nativeKinds["EEBUS"] {
				return fail("state.terminal")
			}
		case "CONFLICTED":
			if terminal != "" || fact["draft_value"] != nil || fact["draft_unit"] != nil ||
				outcome != "CONFLICT" || !nativeKinds["EBUS"] || !nativeKinds["EEBUS"] {
				return fail("state.terminal")
			}
		case "WITHHELD":
			if terminal == "" || fact["draft_value"] != nil || fact["draft_unit"] != nil {
				return fail("state.terminal")
			}
			if (terminal == "CLOUD_ONLY" || terminal == "NO_SIGNAL" || terminal == "NOT_TESTED") &&
				(len(samples) != 0 || outcome != "NOT_EVALUATED") {
				return fail("state.terminal")
			}
			if terminal == "CLOUD_ONLY" && !cloudOnly {
				return fail("state.terminal")
			}
			if terminal == "NO_SIGNAL" && len(nativeKinds) == 0 {
				return fail("state.terminal")
			}
			if terminal == "CONFLICT" &&
				(outcome != "CONFLICT" || !nativeKinds["EBUS"] || !nativeKinds["EEBUS"] || len(samples) == 0) {
				return fail("state.terminal")
			}
		default:
			return fail("state.terminal")
		}

		confidence, _ := objectValue(fact["confidence"])
		if !member(confidence["level"], "LOW", "MEDIUM", "HIGH") ||
			!member(confidence["basis"], "OBSERVED", "INFERRED", "INSUFFICIENT") ||
			!boundedInteger(confidence["score_milli"], 0, 1000) {
			return fail("state.terminal")
		}
		falsifier, _ := objectValue(fact["falsifier"])
		if !member(falsifier["condition_code"],
			"VALUE_DIVERGES", "IDENTITY_CHANGES", "SIGNAL_DISAPPEARS", "ORDER_CHANGES", "PROVENANCE_BREAKS",
		) ||
			!terminals[asString(falsifier["expected_terminal_state"])] ||
			!token(falsifier["description"]) {
			return fail("state.terminal")
		}
		trigger, _ := objectValue(fact["retest_trigger"])
		required, requiredOK := stringArray(trigger["required_source_kinds"])
		if !member(trigger["trigger_code"],
			"NEW_SYNCHRONIZED_BUNDLE", "SOURCE_RECOVERED", "IDENTITY_CONFIRMED", "COMPARATOR_REVISED",
		) ||
			!boundedInteger(trigger["minimum_new_samples"], 1, 1024) ||
			!requiredOK || len(required) == 0 {
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

func checkComparators(graph, registry, sourceBundle map[string]any) error {
	drafts, _ := arrayValue(graph["comparator_drafts"])
	if len(drafts) != 1 {
		return fail("comparator.invalid")
	}
	draft, _ := objectValue(drafts[0])
	registryComparators, _ := arrayValue(registry["comparators"])
	registryDraft, _ := objectValue(registryComparators[0])
	if draft["draft_id"] != "NUMERIC_WINDOW_V1_DRAFT" ||
		draft["type"] != "NUMERIC_WINDOW" ||
		registryDraft["draft_id"] != draft["draft_id"] {
		return fail("comparator.invalid")
	}
	parameters, _ := objectValue(draft["parameters"])
	if err := validateComparatorParameters(parameters); err != nil {
		return err
	}
	artifacts := indexArtifacts(sourceBundle)
	facts, _ := arrayValue(graph["facts"])
	for _, raw := range facts {
		fact, _ := objectValue(raw)
		evaluation, _ := objectValue(fact["comparator"])
		if evaluation["draft_id"] != draft["draft_id"] {
			return fail("comparator.invalid")
		}
		rawSamples, _ := arrayValue(evaluation["samples"])
		samples := make([]map[string]any, len(rawSamples))
		for index, rawSample := range rawSamples {
			samples[index], _ = objectValue(rawSample)
		}
		provenance, _ := objectValue(fact["provenance"])
		rawRefs, _ := arrayValue(provenance["native_evidence_refs"])
		allowedRefs := make(map[string]bool, len(rawRefs))
		for _, ref := range rawRefs {
			key, err := canonicalKey(ref)
			if err != nil {
				return fail("comparator.invalid")
			}
			allowedRefs[key] = true
		}
		computed, finalRight, err := evaluateNumericWindow(parameters, samples, artifacts, allowedRefs)
		if err != nil {
			return err
		}
		if evaluation["outcome"] != computed {
			return fail("comparator.invalid")
		}
		status := asString(fact["status"])
		allowedOutcomes := map[string]map[string]bool{
			"RAW_ONLY":   {"NOT_EVALUATED": true},
			"CANDIDATE":  {"MATCH": true},
			"CONFLICTED": {"CONFLICT": true},
			"WITHHELD":   {"NOT_EVALUATED": true, "CONFLICT": true, "INDETERMINATE": true},
		}
		if !allowedOutcomes[status][computed] {
			return fail("comparator.invalid")
		}
		if status == "CANDIDATE" {
			if finalRight == "" ||
				fact["draft_unit"] != objectAtUnsafe(parameters, "unit_conversion")["target_unit"] ||
				fact["draft_value"] != finalRight {
				return fail("comparator.invalid")
			}
		}
		terminal, _ := optionalString(fact["terminal_negative_state"])
		if terminal == "CONFLICT" && computed != "CONFLICT" ||
			(terminal == "NO_SIGNAL" || terminal == "CLOUD_ONLY" || terminal == "NOT_TESTED") &&
				computed != "NOT_EVALUATED" {
			return fail("comparator.invalid")
		}
	}
	return nil
}

func checkAntiLeak(graph, registry map[string]any) error {
	visibility, _ := objectValue(graph["visibility"])
	want := map[string]any{
		"channel": registry["candidate_channel"], "promotion_state": "NOT_PROMOTED",
		"stable_exposure": false, "command_capable": false, "protocol_translation": false,
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
	if graph["graph_hash"] != "sha256:"+hexdigest ||
		graph["graph_id"] != "dcfgv1:sha256:"+hexdigest {
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

func objectAtUnsafe(parent map[string]any, key string) map[string]any {
	value, _ := objectValue(parent[key])
	return value
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
	leftKey := []string{
		asString(left["kind"]), asString(left["digest_algorithm"]), asString(left["digest"]),
		nullableSortValue(left["repository"]), nullableSortValue(left["commit"]), nullableSortValue(left["path"]),
	}
	rightKey := []string{
		asString(right["kind"]), asString(right["digest_algorithm"]), asString(right["digest"]),
		nullableSortValue(right["repository"]), nullableSortValue(right["commit"]), nullableSortValue(right["path"]),
	}
	return strings.Join(leftKey, "\x00") < strings.Join(rightKey, "\x00")
}

func nullableSortValue(value any) string {
	if value == nil {
		return "\x00"
	}
	return "\x01" + asString(value)
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
	return string(leftCanonical) < string(rightCanonical)
}

func canonicalKey(value any) (string, error) {
	encoded, err := canonicalJSON(value)
	return string(encoded), err
}

func cloneObject(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func domainHex(domain string, canonical []byte) string {
	digest := sha256.Sum256(append(append([]byte(domain), 0), canonical...))
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

func printableASCII(value string) bool {
	if len(value) == 0 {
		return false
	}
	for _, current := range []byte(value) {
		if current < 0x20 || current > 0x7e {
			return false
		}
	}
	return true
}
