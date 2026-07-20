package coexistence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func packageDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve coexistence test package directory")
	}
	return filepath.Dir(file)
}

func repoDir(t *testing.T) string {
	t.Helper()
	return filepath.Clean(filepath.Join(packageDir(t), "../.."))
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func loadValue(t *testing.T, path string) any {
	t.Helper()
	return decodeJSON(t, readFile(t, path))
}

func loadObject(t *testing.T, path string) map[string]any {
	t.Helper()
	return asObject(t, loadValue(t, path), path)
}

func decodeJSON(t *testing.T, raw []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			t.Fatal("JSON contains multiple top-level values")
		}
		t.Fatal(err)
	}
	return value
}

func canonicalJSON(t *testing.T, value any) []byte {
	t.Helper()
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))
}

func cloneValue(t *testing.T, value any) any {
	t.Helper()
	return decodeJSON(t, canonicalJSON(t, value))
}

func cloneObject(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	return asObject(t, cloneValue(t, value), "cloned object")
}

func assertFileSHA256(t *testing.T, path, want string) {
	t.Helper()
	digest := sha256.Sum256(readFile(t, path))
	if got := hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("%s SHA-256 = %s; want %s", path, got, want)
	}
}

func rawSHA256(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func domainDigest(t *testing.T, domain string, value any) string {
	t.Helper()
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(canonicalJSON(t, value))
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func asObject(t *testing.T, value any, name string) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s is %T; want object", name, value)
	}
	return object
}

func asArray(t *testing.T, value any, name string) []any {
	t.Helper()
	array, ok := value.([]any)
	if !ok {
		t.Fatalf("%s is %T; want array", name, value)
	}
	return array
}

func objectValue(t *testing.T, object map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := object[key]
	if !ok {
		t.Fatalf("missing object field %q", key)
	}
	return asObject(t, value, key)
}

func arrayValue(t *testing.T, object map[string]any, key string) []any {
	t.Helper()
	value, ok := object[key]
	if !ok {
		t.Fatalf("missing array field %q", key)
	}
	return asArray(t, value, key)
}

func stringValue(t *testing.T, object map[string]any, key string) string {
	t.Helper()
	value, ok := object[key]
	if !ok {
		t.Fatalf("missing string field %q", key)
	}
	result, ok := value.(string)
	if !ok {
		t.Fatalf("field %q is %T; want string", key, value)
	}
	return result
}

func boolValue(t *testing.T, object map[string]any, key string) bool {
	t.Helper()
	value, ok := object[key]
	if !ok {
		t.Fatalf("missing boolean field %q", key)
	}
	result, ok := value.(bool)
	if !ok {
		t.Fatalf("field %q is %T; want boolean", key, value)
	}
	return result
}

func intValue(t *testing.T, object map[string]any, key string) int64 {
	t.Helper()
	value, ok := object[key]
	if !ok {
		t.Fatalf("missing integer field %q", key)
	}
	switch typed := value.(type) {
	case json.Number:
		result, err := typed.Int64()
		if err != nil {
			t.Fatalf("field %q is not an integer: %v", key, err)
		}
		return result
	case int:
		return int64(typed)
	case int64:
		return typed
	default:
		t.Fatalf("field %q is %T; want integer", key, value)
		return 0
	}
}

func assertStringValue(t *testing.T, object map[string]any, key, want string) {
	t.Helper()
	if got := stringValue(t, object, key); got != want {
		t.Fatalf("%s = %q; want %q", key, got, want)
	}
}

func assertIntValue(t *testing.T, object map[string]any, key string, want int64) {
	t.Helper()
	if got := intValue(t, object, key); got != want {
		t.Fatalf("%s = %d; want %d", key, got, want)
	}
}

func assertStringSlice(t *testing.T, value any, want []string) {
	t.Helper()
	got := stringSlice(t, value)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("string list = %v; want %v", got, want)
	}
}

func stringSlice(t *testing.T, value any) []string {
	t.Helper()
	raw := asArray(t, value, "string list")
	result := make([]string, len(raw))
	for index, item := range raw {
		text, ok := item.(string)
		if !ok {
			t.Fatalf("string list item %d is %T", index, item)
		}
		result[index] = text
	}
	return result
}

func intMap(t *testing.T, object map[string]any) map[string]int64 {
	t.Helper()
	result := make(map[string]int64, len(object))
	for key := range object {
		result[key] = intValue(t, object, key)
	}
	return result
}

func sortedKeys[V any](values map[string]V) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func withoutKeys(t *testing.T, object map[string]any, keys ...string) map[string]any {
	t.Helper()
	result := cloneObject(t, object)
	for _, key := range keys {
		delete(result, key)
	}
	return result
}

func payloadShape(t *testing.T, value any) any {
	t.Helper()
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = payloadShape(t, item)
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = payloadShape(t, item)
		}
		return result
	case nil:
		return "null"
	case bool:
		return "boolean"
	case json.Number, int, int64:
		return "integer"
	case string:
		return "string"
	default:
		t.Fatalf("unsupported payload scalar %T", value)
		return nil
	}
}

func replacePointer(t *testing.T, value any, pointer string, replacement string) {
	t.Helper()
	if pointer == "" || pointer[0] != '/' {
		t.Fatalf("invalid JSON pointer %q", pointer)
	}
	parts := strings.Split(pointer[1:], "/")
	current := value
	for _, rawPart := range parts[:len(parts)-1] {
		part := strings.ReplaceAll(strings.ReplaceAll(rawPart, "~1", "/"), "~0", "~")
		switch typed := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = typed[part]
			if !ok {
				t.Fatalf("pointer %q does not resolve", pointer)
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				t.Fatalf("pointer %q has invalid index %q", pointer, part)
			}
			current = typed[index]
		default:
			t.Fatalf("pointer %q traverses scalar %T", pointer, current)
		}
	}
	leaf := strings.ReplaceAll(strings.ReplaceAll(parts[len(parts)-1], "~1", "/"), "~0", "~")
	switch typed := current.(type) {
	case map[string]any:
		if _, ok := typed[leaf].(string); !ok {
			t.Fatalf("pointer %q must resolve to a string", pointer)
		}
		typed[leaf] = replacement
	case []any:
		index, err := strconv.Atoi(leaf)
		if err != nil || index < 0 || index >= len(typed) {
			t.Fatalf("pointer %q has invalid leaf index %q", pointer, leaf)
		}
		if _, ok := typed[index].(string); !ok {
			t.Fatalf("pointer %q must resolve to a string", pointer)
		}
		typed[index] = replacement
	default:
		t.Fatalf("pointer %q resolves through scalar %T", pointer, current)
	}
}

func normalizePayload(t *testing.T, payload any, rule map[string]any) any {
	t.Helper()
	result := cloneValue(t, payload)
	for _, pointer := range stringSlice(t, rule["timestamp_pointers"]) {
		replacePointer(t, result, pointer, "<TIMESTAMP>")
	}
	for _, pointer := range stringSlice(t, rule["mask_pointers"]) {
		replacePointer(t, result, pointer, "<MASKED>")
	}
	return result
}

func findRun(t *testing.T, evidence map[string]any, state string) map[string]any {
	t.Helper()
	for _, raw := range arrayValue(t, evidence, "runs") {
		run := asObject(t, raw, "run")
		if stringValue(t, run, "state") == state {
			return run
		}
	}
	t.Fatalf("missing run state %s", state)
	return nil
}

func findView(t *testing.T, run map[string]any, viewID string) map[string]any {
	t.Helper()
	for _, raw := range arrayValue(t, run, "protected_views") {
		view := asObject(t, raw, "protected view")
		if stringValue(t, view, "view_id") == viewID {
			return view
		}
	}
	t.Fatalf("missing protected view %s", viewID)
	return nil
}

func ruleByID(t *testing.T, evidence map[string]any, viewID string) map[string]any {
	t.Helper()
	normalization := objectValue(t, evidence, "normalization")
	for _, raw := range arrayValue(t, normalization, "view_rules") {
		rule := asObject(t, raw, "normalization rule")
		if stringValue(t, rule, "view_id") == viewID {
			return rule
		}
	}
	t.Fatalf("missing normalization rule %s", viewID)
	return nil
}

func containsCandidateLeak(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "candidate") || strings.Contains(lower, "conflict") || containsCandidateLeak(item) {
				return true
			}
		}
	case []any:
		for _, item := range typed {
			if containsCandidateLeak(item) {
				return true
			}
		}
	case string:
		lower := strings.ToLower(typed)
		return strings.Contains(lower, "candidate") || strings.Contains(lower, "conflict")
	}
	return false
}

func refreshViewHashes(t *testing.T, evidence, run, view map[string]any) {
	t.Helper()
	payload := view["payload"]
	rule := ruleByID(t, evidence, stringValue(t, view, "view_id"))
	view["raw_payload_hash"] = domainDigest(t, rawPayloadDomainV1, payload)
	view["shape_hash"] = domainDigest(t, shapeDomainV1, payloadShape(t, payload))
	view["canonical_payload_hash"] = domainDigest(t, canonicalPayloadDomainV1, normalizePayload(t, payload, rule))

	inputID := "view:" + stringValue(t, view, "view_id")
	provenance := objectValue(t, run, "provenance")
	for _, raw := range arrayValue(t, provenance, "immutable_inputs") {
		input := asObject(t, raw, "immutable input")
		if stringValue(t, input, "input_id") == inputID {
			input["digest"] = view["raw_payload_hash"]
			input["byte_length"] = int64(len(canonicalJSON(t, payload)))
			return
		}
	}
	t.Fatalf("missing immutable input %s", inputID)
}

func applyMutation(t *testing.T, evidence map[string]any, mutation string) {
	t.Helper()
	disabled := findRun(t, evidence, "EEBUS_DISABLED_CONFIRMED")
	noServices := findRun(t, evidence, "EEBUS_ENABLED_NO_SERVICES")
	candidate := findRun(t, evidence, "EEBUS_CONNECTED_CANDIDATE_ONLY")
	conflicted := findRun(t, evidence, "EEBUS_CONFLICTED_WITHHELD")
	rollback := findRun(t, evidence, "EEBUS_DISABLED_ROLLBACK")

	switch mutation {
	case "CANDIDATE_LEAK_EBUS_MCP":
		view := findView(t, candidate, "mcp.ebus.v1.responses")
		objectValue(t, objectValue(t, view, "payload"), "data")["candidate_status"] = "CANDIDATE"
		refreshViewHashes(t, evidence, candidate, view)
	case "CANONICAL_HASH_MISMATCH":
		view := asObject(t, arrayValue(t, disabled, "protected_views")[0], "protected view")
		view["canonical_payload_hash"] = "sha256:" + strings.Repeat("f", 64)
	case "CLOCK_MISMATCH":
		objectValue(t, disabled, "provenance")["capture_clock_id"] = "clock-" + strings.Repeat("f", 32)
	case "CONFIG_HASH_MISMATCH":
		objectValue(t, objectValue(t, disabled, "provenance"), "config")["config_hash"] = "sha256:" + strings.Repeat("f", 64)
	case "CONFLICT_LEAK_GRAPHQL":
		view := findView(t, conflicted, "graphql.ebus.values")
		objectValue(t, objectValue(t, view, "payload"), "data")["conflict_status"] = "WITHHELD/CONFLICT"
		refreshViewHashes(t, evidence, conflicted, view)
	case "DROPPED_PAYLOAD_FIELD":
		view := findView(t, disabled, "ha.identity")
		devices := arrayValue(t, objectValue(t, objectValue(t, view, "payload"), "data"), "devices")
		delete(asObject(t, devices[0], "HA device"), "manufacturer")
		refreshViewHashes(t, evidence, disabled, view)
	case "DUPLICATE_PROVENANCE":
		provenance := objectValue(t, disabled, "provenance")
		inputs := arrayValue(t, provenance, "immutable_inputs")
		provenance["immutable_inputs"] = append(inputs, cloneValue(t, inputs[0]))
	case "G17_CLAIM":
		scope := objectValue(t, evidence, "scope")
		scope["claims"] = append(arrayValue(t, scope, "claims"), "EEBUS-G17")
	case "G19_CLAIM":
		scope := objectValue(t, evidence, "scope")
		scope["claims"] = append(arrayValue(t, scope, "claims"), "EEBUS-G19")
	case "INPUT_HASH_MISMATCH":
		inputs := arrayValue(t, objectValue(t, disabled, "provenance"), "immutable_inputs")
		asObject(t, inputs[0], "immutable input")["digest"] = "sha256:" + strings.Repeat("f", 64)
	case "M7_GRAPH_MISMATCH":
		objectValue(t, evidence, "m7_binding")["graph_hash"] = "sha256:" + strings.Repeat("f", 64)
	case "MASK_SCOPE_MISMATCH":
		objectValue(t, disabled, "provenance")["mask_scope_digest"] = "sha256:" + strings.Repeat("f", 64)
	case "MISSING_PROVENANCE":
		delete(disabled, "provenance")
	case "MISSING_REQUIRED_VIEW":
		views := arrayValue(t, disabled, "protected_views")
		removed := asObject(t, views[len(views)-1], "removed view")
		disabled["protected_views"] = views[:len(views)-1]
		inputID := "view:" + stringValue(t, removed, "view_id")
		provenance := objectValue(t, disabled, "provenance")
		inputs := arrayValue(t, provenance, "immutable_inputs")
		filtered := make([]any, 0, len(inputs)-1)
		for _, raw := range inputs {
			input := asObject(t, raw, "immutable input")
			if stringValue(t, input, "input_id") != inputID {
				filtered = append(filtered, input)
			}
		}
		provenance["immutable_inputs"] = filtered
	case "NO_SERVICES_EMPTY_SUCCESS":
		objectValue(t, noServices, "state_evidence")["empty_success"] = true
	case "PUBLIC_V2_SURFACE":
		view := findView(t, disabled, "mcp.tool.inventory")
		data := objectValue(t, objectValue(t, view, "payload"), "data")
		data["tools"] = append(arrayValue(t, data, "tools"), "eebus.v2.runtime.status")
		refreshViewHashes(t, evidence, disabled, view)
	case "RESOURCE_LIMIT_EXCEEDED":
		limits := objectValue(t, evidence, "limits")
		limits["max_runs"] = intValue(t, limits, "max_runs") + 1
	case "ROLLBACK_DRIFT":
		view := findView(t, rollback, "semantic.registry")
		objectValue(t, objectValue(t, view, "payload"), "data")["authority"] = "eebus-candidate"
		refreshViewHashes(t, evidence, rollback, view)
	case "RUNTIME_ARTIFACT_MISMATCH":
		runtime := objectValue(t, objectValue(t, disabled, "provenance"), "runtime")
		runtime["artifact_digest"] = "sha256:" + strings.Repeat("f", 64)
	case "STALE_CAPTURE":
		clock := objectValue(t, evidence, "capture_clock")
		baseline := findRun(t, evidence, "EEBUS_DISABLED_BASELINE")
		clock["verification_offset_ns"] = intValue(t, clock, "max_capture_age_ns") + intValue(t, baseline, "capture_offset_ns") + 1
	case "TIMESTAMP_EXCLUSION_MISMATCH":
		normalization := objectValue(t, evidence, "normalization")
		firstRule := asObject(t, arrayValue(t, normalization, "view_rules")[0], "first normalization rule")
		firstRule["timestamp_pointers"] = []any{}
		digest := domainDigest(t, normalizationDomainV1, withoutKeys(t, normalization, "profile_digest"))
		normalization["profile_digest"] = digest
		for _, raw := range arrayValue(t, evidence, "runs") {
			objectValue(t, asObject(t, raw, "run"), "provenance")["mask_scope_digest"] = digest
		}
	case "UNKNOWN_FIELD":
		evidence["verdict"] = "PASS"
	default:
		t.Fatalf("unhandled MSP-08 mutation %q", mutation)
	}
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func explainValue(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%#v", value)
	}
	return string(raw)
}
