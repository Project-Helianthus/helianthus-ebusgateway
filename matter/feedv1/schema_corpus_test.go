package feedv1

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSchemaCorpusMatchesFeedWireValues(t *testing.T) {
	schema := readFeedSchema(t)
	snapshot := validSnapshot(t, ResourceCurrent, true)
	intent := validIntent(snapshot)
	execution := Execution{Contract: Contract, ACK: "ACKNOWLEDGED", Readback: json.RawMessage(`{"value":1}`), Outcome: json.RawMessage(`{"state":"completed"}`)}
	cursor := Cursor{Instance: "feed-schema", Sequence: 1}
	withdrawn := validSnapshot(t, ResourceWithdrawn, false)
	withdrawn.Resources[0].LastFence = &Fence{
		SnapshotID:       snapshot.Resources[0].SnapshotID,
		Revisions:        snapshot.Resources[0].Revisions,
		BindingID:        snapshot.Resources[0].BindingID,
		SourceEpoch:      snapshot.Resources[0].SourceEpoch,
		DriverGeneration: snapshot.Resources[0].DriverGeneration,
	}
	if err := withdrawn.Validate(); err != nil {
		t.Fatalf("withdrawn fixture invalid: %v", err)
	}
	quarantined := validSnapshot(t, ResourceQuarantined, false)
	unavailable := validSnapshot(t, ResourceUnavailable, false)

	valid := map[string]any{
		"snapshot":    SnapshotResponse{Contract: Contract, Cursor: cursor, Snapshot: snapshot},
		"changes":     ChangesResponse{Contract: Contract, Cursor: cursor, Changes: []SnapshotResponse{{Contract: Contract, Cursor: cursor, Snapshot: snapshot}}},
		"resync":      ChangesResponse{Contract: Contract, Cursor: cursor, ResyncRequired: true, Changes: []SnapshotResponse{}},
		"withdrawn":   SnapshotResponse{Contract: Contract, Cursor: cursor, Snapshot: withdrawn},
		"quarantined": SnapshotResponse{Contract: Contract, Cursor: cursor, Snapshot: quarantined},
		"unavailable": SnapshotResponse{Contract: Contract, Cursor: cursor, Snapshot: unavailable},
		"intent":      intent,
		"execution":   execution,
		"error":       map[string]any{"contract": Contract, "error": "invalid_intent"},
	}
	for name, value := range valid {
		t.Run(name, func(t *testing.T) {
			if err := validateFeedSchema(schema, wireValue(t, value)); err != nil {
				t.Fatalf("valid %s rejected: %v", name, err)
			}
		})
	}

	invalid := map[string]func(map[string]any){
		"unknown_member":   func(value map[string]any) { value["route"] = "must-not-cross-the-boundary" },
		"wrong_constant":   func(value map[string]any) { value["contract"] = "other/v1" },
		"missing_required": func(value map[string]any) { delete(value["cursor"].(map[string]any), "instance") },
		"malformed_report": func(value map[string]any) {
			delete(value["snapshot"].(map[string]any)["resources"].([]any)[0].(map[string]any)["projected"].(map[string]any)["report"].(map[string]any), "snapshot_id")
		},
		"malformed_action": func(value map[string]any) {
			value["snapshot"].(map[string]any)["operations"].([]any)[0].(map[string]any)["action"].(map[string]any)["discoverable"] = "true"
		},
		"malformed_causal": func(value map[string]any) {
			value["causal"].(map[string]any)["origin"].(map[string]any)["kind"] = "native_route"
		},
		"malformed_revision": func(value map[string]any) {
			value["snapshot"].(map[string]any)["resources"].([]any)[0].(map[string]any)["revisions"].(map[string]any)["semantic"] = float64(1)
		},
		"current_missing_projected": func(value map[string]any) {
			delete(value["snapshot"].(map[string]any)["resources"].([]any)[0].(map[string]any), "projected")
		},
		"current_zero_revisions": func(value map[string]any) {
			revisions := value["snapshot"].(map[string]any)["resources"].([]any)[0].(map[string]any)["revisions"].(map[string]any)
			for key := range revisions {
				revisions[key] = ""
			}
		},
		"noncurrent_retains_projected": func(value map[string]any) {
			value["snapshot"].(map[string]any)["resources"].([]any)[0].(map[string]any)["projected"] = wireValue(t, snapshot).(map[string]any)["resources"].([]any)[0].(map[string]any)["projected"]
		},
		"noncurrent_retains_current_fence": func(value map[string]any) {
			resource := value["snapshot"].(map[string]any)["resources"].([]any)[0].(map[string]any)
			current := wireValue(t, snapshot).(map[string]any)["resources"].([]any)[0].(map[string]any)
			resource["snapshot_id"] = current["snapshot_id"]
			resource["revisions"] = current["revisions"]
			resource["binding_id"] = current["binding_id"]
			resource["source_epoch"] = current["source_epoch"]
			resource["driver_generation"] = current["driver_generation"]
		},
	}
	for name, mutate := range invalid {
		t.Run(name, func(t *testing.T) {
			var value map[string]any
			if name == "malformed_causal" {
				value = wireValue(t, intent).(map[string]any)
			} else if strings.HasPrefix(name, "noncurrent_") {
				value = wireValue(t, valid["withdrawn"]).(map[string]any)
			} else {
				value = wireValue(t, valid["snapshot"]).(map[string]any)
			}
			mutate(value)
			if err := validateFeedSchema(schema, value); err == nil {
				t.Fatal("invalid wire value accepted")
			}
		})
	}
}

func readFeedSchema(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("../../docs/schemas/matter-binding-feed-v1.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	return schema
}

func wireValue(t *testing.T, value any) any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

// validateFeedSchema implements exactly the Draft 2020-12 keywords used by
// the checked-in schema. Keeping it local avoids making this detached contract
// depend on a JSON-Schema runtime only for corpus verification.
func validateFeedSchema(root map[string]any, value any) error {
	return validateFeedSubschema(root, root, value, "$")
}

func validateFeedSubschema(root, schema map[string]any, value any, path string) error {
	if reference, ok := schema["$ref"].(string); ok {
		target, err := feedSchemaPointer(root, reference)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		return validateFeedSubschema(root, target, value, path)
	}
	if choices, ok := schema["oneOf"].([]any); ok {
		matches := 0
		for _, choice := range choices {
			candidate, ok := choice.(map[string]any)
			if ok && validateFeedSubschema(root, candidate, value, path) == nil {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("%s: oneOf matched %d branches", path, matches)
		}
	}
	if forbidden, ok := schema["not"].(map[string]any); ok {
		if err := validateFeedSubschema(root, forbidden, value, path); err == nil {
			return fmt.Errorf("%s: prohibited schema matched", path)
		}
	}
	if constant, ok := schema["const"]; ok && !reflect.DeepEqual(value, constant) {
		return fmt.Errorf("%s: const mismatch", path)
	}
	if values, ok := schema["enum"].([]any); ok {
		for _, candidate := range values {
			if reflect.DeepEqual(value, candidate) {
				goto enumOK
			}
		}
		return fmt.Errorf("%s: enum mismatch", path)
	}
enumOK:
	if kind, ok := schema["type"].(string); ok && !feedSchemaTypeIs(kind, value) {
		return fmt.Errorf("%s: expected %s", path, kind)
	}
	if minimum, ok := schema["minimum"].(float64); ok {
		if number, ok := value.(float64); ok && number < minimum {
			return fmt.Errorf("%s: below minimum", path)
		}
	}
	if maximum, ok := schema["maximum"].(float64); ok {
		if number, ok := value.(float64); ok && number > maximum {
			return fmt.Errorf("%s: above maximum", path)
		}
	}
	if text, ok := value.(string); ok {
		if min, ok := schema["minLength"].(float64); ok && len(text) < int(min) {
			return fmt.Errorf("%s: too short", path)
		}
		if max, ok := schema["maxLength"].(float64); ok && len(text) > int(max) {
			return fmt.Errorf("%s: too long", path)
		}
		if format, ok := schema["format"].(string); ok && format == "date-time" {
			if _, err := time.Parse(time.RFC3339, text); err != nil {
				return fmt.Errorf("%s: invalid date-time", path)
			}
		}
	}
	if object, ok := value.(map[string]any); ok {
		properties, _ := schema["properties"].(map[string]any)
		if required, ok := schema["required"].([]any); ok {
			for _, item := range required {
				key, _ := item.(string)
				if _, present := object[key]; !present {
					return fmt.Errorf("%s: missing %s", path, key)
				}
			}
		}
		if closed, present := schema["additionalProperties"].(bool); present && !closed {
			for key := range object {
				if _, allowed := properties[key]; !allowed {
					return fmt.Errorf("%s: unexpected %s", path, key)
				}
			}
		}
		for key, childValue := range object {
			child, ok := properties[key].(map[string]any)
			if ok {
				if err := validateFeedSubschema(root, child, childValue, path+"."+key); err != nil {
					return err
				}
			}
		}
	}
	if list, ok := value.([]any); ok {
		if max, ok := schema["maxItems"].(float64); ok && len(list) > int(max) {
			return fmt.Errorf("%s: too many items", path)
		}
		if itemSchema, ok := schema["items"].(map[string]any); ok {
			for index, item := range list {
				if err := validateFeedSubschema(root, itemSchema, item, fmt.Sprintf("%s[%d]", path, index)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func feedSchemaTypeIs(kind string, value any) bool {
	switch kind {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "integer":
		number, ok := value.(float64)
		return ok && math.Trunc(number) == number
	default:
		return false
	}
}

func feedSchemaPointer(root map[string]any, reference string) (map[string]any, error) {
	if !strings.HasPrefix(reference, "#/") {
		return nil, fmt.Errorf("unsupported reference %q", reference)
	}
	var current any = root
	for _, token := range strings.Split(strings.TrimPrefix(reference, "#/"), "/") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("reference %q is not an object", reference)
		}
		current, ok = object[strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")]
		if !ok {
			return nil, fmt.Errorf("reference %q is missing", reference)
		}
	}
	result, ok := current.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("reference %q is not a schema", reference)
	}
	return result, nil
}
