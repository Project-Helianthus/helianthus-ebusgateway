package coexistence

import (
	"encoding/json"
	"reflect"
	"regexp"
	"sync"
	"unicode/utf8"
)

var (
	evidenceSchemaOnce sync.Once
	evidenceSchemaV1   map[string]any
	statusSchemaOnce   sync.Once
	statusSchemaV1     map[string]any
)

func loadEvidenceSchema() map[string]any {
	evidenceSchemaOnce.Do(func() {
		raw := readPinnedArtifact("contracts/multi-runtime-coexistence-evidence-v1.schema.json", evidenceSchemaSHA256)
		value, err := parseJSON(raw, "schema.evidence")
		if err != nil {
			panic("coexistence: invalid pinned evidence schema")
		}
		evidenceSchemaV1 = value.(map[string]any)
	})
	return evidenceSchemaV1
}

func loadStatusSchema() map[string]any {
	statusSchemaOnce.Do(func() {
		raw := readPinnedArtifact("contracts/draft-candidate-fact-public-status-v1.schema.json", statusSchemaSHA256)
		value, err := parseCategorizedJSON(raw, "provenance.m7")
		if err != nil {
			panic("coexistence: invalid embedded M7 status schema")
		}
		statusSchemaV1 = value.(map[string]any)
	})
	return statusSchemaV1
}

func schemaCheckEvidence(value any) error {
	schema := loadEvidenceSchema()
	if !schemaValidate(value, schema, schema) {
		return fail("schema.evidence")
	}
	if err := checkPortableKeys(value); err != nil {
		return err
	}
	return nil
}

func schemaCheckStatus(value any) error {
	schema := loadStatusSchema()
	if !schemaValidate(value, schema, schema) {
		return fail("provenance.m7")
	}
	return nil
}

func schemaCheckDefinition(value any, name, category string) error {
	schema := loadEvidenceSchema()
	definitions, ok := objectValueV1(schema["$defs"])
	if !ok {
		return fail(category)
	}
	rule, ok := objectValueV1(definitions[name])
	if !ok || !schemaValidate(value, rule, schema) {
		return fail(category)
	}
	return nil
}

func schemaValidate(value any, rule, root map[string]any) bool {
	if reference, ok := stringValueV1(rule["$ref"]); ok {
		const prefix = "#/$defs/"
		if len(reference) <= len(prefix) || reference[:len(prefix)] != prefix {
			return false
		}
		definitions, valid := objectValueV1(root["$defs"])
		if !valid {
			return false
		}
		target, valid := objectValueV1(definitions[reference[len(prefix):]])
		return valid && schemaValidate(value, target, root)
	}
	if alternatives, ok := arrayValueV1(rule["oneOf"]); ok {
		matched := 0
		for _, rawAlternative := range alternatives {
			alternative, valid := objectValueV1(rawAlternative)
			if valid && schemaValidate(value, alternative, root) {
				matched++
			}
		}
		return matched == 1
	}
	if expected, ok := stringValueV1(rule["type"]); ok && !schemaType(value, expected) {
		return false
	}
	if expected, exists := rule["const"]; exists && !reflect.DeepEqual(value, expected) {
		return false
	}
	if values, ok := arrayValueV1(rule["enum"]); ok {
		matched := false
		for _, candidate := range values {
			if reflect.DeepEqual(value, candidate) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	if text, ok := stringValueV1(value); ok {
		length := int64(utf8.RuneCountInString(text))
		if minimum, exists := integerValue(rule["minLength"]); exists && length < minimum {
			return false
		}
		if maximum, exists := integerValue(rule["maxLength"]); exists && length > maximum {
			return false
		}
		if pattern, exists := stringValueV1(rule["pattern"]); exists {
			matched, err := regexp.MatchString(pattern, text)
			if err != nil || !matched {
				return false
			}
		}
	}
	if numeric, ok := integerValue(value); ok {
		if minimum, exists := integerValue(rule["minimum"]); exists && numeric < minimum {
			return false
		}
		if maximum, exists := integerValue(rule["maximum"]); exists && numeric > maximum {
			return false
		}
	}
	if items, ok := arrayValueV1(value); ok {
		if minimum, exists := integerValue(rule["minItems"]); exists && int64(len(items)) < minimum {
			return false
		}
		if maximum, exists := integerValue(rule["maxItems"]); exists && int64(len(items)) > maximum {
			return false
		}
		if itemRule, exists := objectValueV1(rule["items"]); exists {
			for _, item := range items {
				if !schemaValidate(item, itemRule, root) {
					return false
				}
			}
		}
	}
	if object, ok := objectValueV1(value); ok {
		if required, exists := stringsFromArray(rule["required"]); exists {
			for _, key := range required {
				if _, present := object[key]; !present {
					return false
				}
			}
		}
		properties, _ := objectValueV1(rule["properties"])
		additional, additionalExists := rule["additionalProperties"]
		for key, item := range object {
			if childRule, exists := objectValueV1(properties[key]); exists {
				if !schemaValidate(item, childRule, root) {
					return false
				}
				continue
			}
			if closed, isBool := additional.(bool); additionalExists && isBool && !closed {
				return false
			}
			if childRule, isRule := objectValueV1(additional); additionalExists && isRule && !schemaValidate(item, childRule, root) {
				return false
			}
		}
	}
	return true
}

func schemaType(value any, expected string) bool {
	switch expected {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "integer":
		_, ok := value.(json.Number)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	default:
		return false
	}
}

type portableValue struct {
	value any
	depth int64
}

func checkParsedResourceLimits(value any, rawSize int) error {
	if int64(rawSize) > hardLimitsV1["max_evidence_bytes"] {
		return fail("limits.exceeded")
	}
	stack := []portableValue{{value: value, depth: 1}}
	var members, listItems int64
	for len(stack) > 0 {
		last := len(stack) - 1
		item := stack[last]
		stack = stack[:last]
		if item.depth > hardLimitsV1["max_depth"] {
			return fail("limits.exceeded")
		}
		switch current := item.value.(type) {
		case map[string]any:
			members += int64(len(current))
			if members > hardLimitsV1["max_total_members"] {
				return fail("limits.exceeded")
			}
			for _, child := range current {
				stack = append(stack, portableValue{value: child, depth: item.depth + 1})
			}
		case []any:
			listItems += int64(len(current))
			if listItems > hardLimitsV1["max_total_list_items"] {
				return fail("limits.exceeded")
			}
			for _, child := range current {
				stack = append(stack, portableValue{value: child, depth: item.depth + 1})
			}
		case string:
			if int64(len(current)) > hardLimitsV1["max_string_bytes"] || containsNUL(current) {
				return fail("limits.exceeded")
			}
		}
	}

	root, ok := objectValueV1(value)
	if !ok {
		return nil
	}
	if limits, valid := objectValueV1(root["limits"]); valid {
		allPresent := true
		for key := range hardLimitsV1 {
			if _, exists := limits[key]; !exists {
				allPresent = false
				break
			}
		}
		if allPresent {
			for key, expected := range hardLimitsV1 {
				actual, validInteger := integerValue(limits[key])
				if !validInteger || actual != expected {
					return fail("limits.exceeded")
				}
			}
		}
	}
	runs, ok := arrayValueV1(root["runs"])
	if !ok {
		return nil
	}
	if int64(len(runs)) > hardLimitsV1["max_runs"] {
		return fail("limits.exceeded")
	}
	for _, rawRun := range runs {
		run, valid := objectValueV1(rawRun)
		if !valid {
			continue
		}
		if views, validViews := arrayValueV1(run["protected_views"]); validViews {
			if int64(len(views)) > hardLimitsV1["max_views_per_run"] {
				return fail("limits.exceeded")
			}
			for _, rawView := range views {
				view, validView := objectValueV1(rawView)
				if !validView {
					continue
				}
				payload, exists := view["payload"]
				if !exists {
					continue
				}
				encoded, err := marshalCanonicalV1(payload)
				if err == nil && int64(len(encoded)) > hardLimitsV1["max_payload_bytes"] {
					return fail("limits.exceeded")
				}
			}
		}
		provenance, _ := objectValueV1(run["provenance"])
		if inputs, validInputs := arrayValueV1(provenance["immutable_inputs"]); validInputs && int64(len(inputs)) > hardLimitsV1["max_inputs_per_run"] {
			return fail("limits.exceeded")
		}
		state, _ := objectValueV1(run["state_evidence"])
		if facts, validFacts := arrayValueV1(state["facts"]); validFacts && int64(len(facts)) > hardLimitsV1["max_internal_facts_per_run"] {
			return fail("limits.exceeded")
		}
	}
	return nil
}

func checkPortableKeys(value any) error {
	stack := []any{value}
	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		switch typed := current.(type) {
		case map[string]any:
			for key, child := range typed {
				if !asciiToken(key, hardLimitsV1["max_string_bytes"]) {
					return fail("schema.evidence")
				}
				stack = append(stack, child)
			}
		case []any:
			stack = append(stack, typed...)
		}
	}
	return nil
}

func asciiToken(value string, maximum int64) bool {
	if value == "" || int64(len(value)) > maximum {
		return false
	}
	for _, current := range []byte(value) {
		if current < 0x20 || current > 0x7e {
			return false
		}
	}
	return true
}

func containsNUL(value string) bool {
	for _, current := range value {
		if current == 0 {
			return true
		}
	}
	return false
}
