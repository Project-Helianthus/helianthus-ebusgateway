package catalogv1

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestCatalogSchemaClosesEveryPopulatedWireItem(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "schemas", "portal-catalog-v1.schema.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	defs := schema["$defs"].(map[string]any)
	for _, name := range []string{"pack_ref", "domain", "contribution", "source", "resource", "field", "action", "quarantine"} {
		entry, ok := defs[name].(map[string]any)
		if !ok || entry["additionalProperties"] != false {
			t.Fatalf("%s is not closed", name)
		}
	}
	properties := schema["properties"].(map[string]any)
	for _, name := range []string{"domains", "contributions", "resources", "fields", "actions", "quarantines"} {
		collection := properties[name].(map[string]any)
		if name == "domains" {
			if prefix, ok := collection["prefixItems"].([]any); !ok || len(prefix) != len(FivePacks) {
				t.Fatalf("domains has no fixed item schemas")
			}
			continue
		}
		item, ok := collection["items"].(map[string]any)
		if !ok || item["$ref"] == nil {
			t.Fatalf("%s has no item schema", name)
		}
	}
	var fixture map[string]any
	fixtureRaw, err := os.ReadFile(filepath.Join("testdata", "five-domain-catalog.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fixtureRaw, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, domain := range fixture["domains"].([]any) {
		id := domain.(map[string]any)["pack"].(map[string]any)["id"].(string)
		if !strings.HasPrefix(id, "helianthus.pack.") {
			t.Fatalf("unreachable fixture pack ID %q", id)
		}
	}
	if err := validateCatalogSchema(schema, fixture); err != nil {
		t.Fatalf("fixture rejected: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"sixth", func(v map[string]any) { v["domains"] = append(v["domains"].([]any), v["domains"].([]any)[0]) }},
		{"duplicate", func(v map[string]any) { v["domains"].([]any)[1] = v["domains"].([]any)[0] }},
		{"arbitrary-id", func(v map[string]any) {
			v["domains"].([]any)[2].(map[string]any)["pack"].(map[string]any)["id"] = "helianthus.pack.arbitrary"
		}},
		{"wrong-version", func(v map[string]any) {
			v["domains"].([]any)[3].(map[string]any)["pack"].(map[string]any)["version"] = "9.9.9"
		}},
	} {
		t.Run("fixed-domains-"+tc.name, func(t *testing.T) {
			var candidate map[string]any
			encoded, _ := json.Marshal(fixture)
			_ = json.Unmarshal(encoded, &candidate)
			tc.mutate(candidate)
			if err := validateCatalogSchema(schema, candidate); err == nil {
				t.Fatalf("invalid fixed domain catalog accepted")
			}
		})
	}
	var populated map[string]any
	encoded, _ := json.Marshal(fixture)
	_ = json.Unmarshal(encoded, &populated)
	source := map[string]any{"asset_id": "asset", "snapshot_id": "snapshot", "revision": "revision", "evaluation_digest": "digest", "binding_id": "binding", "source_epoch": "epoch", "driver_generation": float64(1), "snapshot": map[string]any{}, "evaluation": map[string]any{}, "selections": []any{}, "projection": map[string]any{}}
	populated["contributions"] = []any{map[string]any{"driver_id": "driver", "manifest_id": "manifest", "manifest_version": "1", "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}
	populated["resources"] = []any{map[string]any{"id": "asset", "domain": "helianthus.pack.pv", "service_id": "service", "capability_id": "capability", "contribution_driver_id": "driver", "contribution_manifest_id": "manifest", "contribution_manifest_version": "1", "source": source, "state": "CURRENT"}}
	populated["fields"] = []any{map[string]any{"id": "field", "resource_id": "asset", "definition_id": "definition", "unit_id": "unit", "contribution_driver_id": "driver", "contribution_manifest_id": "manifest", "contribution_manifest_version": "1", "value": float64(1), "quality": "PRESENT", "projection": "projection"}}
	populated["actions"] = []any{map[string]any{"id": "action", "resource_id": "asset", "service_id": "service", "capability_id": "capability", "operation_id": "operation", "contribution_driver_id": "driver", "contribution_manifest_id": "manifest", "contribution_manifest_version": "1", "source": source, "discoverable": true, "enabled": false}}
	populated["quarantines"] = []any{map[string]any{"driver_id": "driver", "manifest_id": "manifest", "manifest_version": "1", "reason": "conflict"}}
	if err := validateCatalogSchema(schema, populated); err != nil {
		t.Fatalf("populated catalog rejected: %v", err)
	}
	populated["contributions"].([]any)[0].(map[string]any)["digest"] = "invalid"
	if err := validateCatalogSchema(schema, populated); err == nil {
		t.Fatal("noncanonical contribution digest accepted")
	}
	populated["contributions"].([]any)[0].(map[string]any)["digest"] = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	populated["resources"].([]any)[0].(map[string]any)["unknown"] = true
	if err := validateCatalogSchema(schema, populated); err == nil {
		t.Fatal("malformed populated resource accepted")
	}
}

func validateCatalogSchema(root map[string]any, value any) error {
	var validate func(map[string]any, any) error
	resolve := func(schema map[string]any) map[string]any { return schema }
	validate = func(schema map[string]any, value any) error {
		if ref, ok := schema["$ref"].(string); ok {
			name := strings.TrimPrefix(ref, "#/$defs/")
			return validate(root["$defs"].(map[string]any)[name].(map[string]any), value)
		}
		if expected, ok := schema["const"]; ok && !reflect.DeepEqual(expected, value) {
			return fmt.Errorf("const mismatch")
		}
		if all, ok := schema["allOf"].([]any); ok {
			for _, item := range all {
				if err := validate(item.(map[string]any), value); err != nil {
					return err
				}
			}
		}
		typeName := schema["type"]
		if typeName == nil && schema["properties"] != nil {
			typeName = "object"
		}
		switch typeName {
		case "object":
			object, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("object required")
			}
			properties, _ := schema["properties"].(map[string]any)
			if required, ok := schema["required"].([]any); ok {
				for _, member := range required {
					if _, ok := object[member.(string)]; !ok {
						return fmt.Errorf("missing %s", member)
					}
				}
			}
			if schema["additionalProperties"] == false {
				for key := range object {
					if _, ok := properties[key]; !ok {
						return fmt.Errorf("unknown %s", key)
					}
				}
			}
			for key, child := range object {
				if childSchema, ok := properties[key].(map[string]any); ok {
					if err := validate(resolve(childSchema), child); err != nil {
						return fmt.Errorf("%s: %w", key, err)
					}
				}
			}
		case "array":
			items, ok := value.([]any)
			if !ok {
				return fmt.Errorf("array required")
			}
			if min, ok := schema["minItems"].(float64); ok && len(items) < int(min) {
				return fmt.Errorf("too few items")
			}
			if max, ok := schema["maxItems"].(float64); ok && len(items) > int(max) {
				return fmt.Errorf("too many items")
			}
			if prefix, ok := schema["prefixItems"].([]any); ok {
				if len(items) < len(prefix) {
					return fmt.Errorf("missing prefix item")
				}
				for i, child := range prefix {
					if err := validate(child.(map[string]any), items[i]); err != nil {
						return fmt.Errorf("item %d: %w", i, err)
					}
				}
			}
			if child, ok := schema["items"].(map[string]any); ok {
				for _, item := range items {
					if err := validate(resolve(child), item); err != nil {
						return err
					}
				}
			}
		case "string":
			text, ok := value.(string)
			if !ok {
				return fmt.Errorf("string required")
			}
			if pattern, ok := schema["pattern"].(string); ok {
				matched, err := regexp.MatchString(pattern, text)
				if err != nil || !matched {
					return fmt.Errorf("pattern mismatch")
				}
			}
		case "boolean":
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("boolean required")
			}
		case "integer":
			if _, ok := value.(float64); !ok {
				return fmt.Errorf("integer required")
			}
		}
		return nil
	}
	return validate(root, value)
}
