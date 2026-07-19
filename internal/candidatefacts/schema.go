package candidatefacts

import (
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"unicode/utf8"
)

func schemaCheck(graph map[string]any) error {
	return validateEmbeddedSchema(graph, "contracts/draft-candidate-fact-graph-v1.schema.json", PinnedContractV1().GraphSchemaSHA256, "schema.graph")
}

func replaySchemaCheck(replay map[string]any) error {
	raw, err := canonicalJSON(replay)
	if err != nil {
		return fail("schema.graph")
	}
	value, err := parseJSON(raw)
	if err != nil {
		return fail("schema.graph")
	}
	normalized, ok := objectValue(value)
	if !ok {
		return fail("schema.graph")
	}
	return validateEmbeddedSchema(normalized, "contracts/draft-candidate-fact-replay-v1.schema.json", PinnedContractV1().ReplaySchemaSHA256, "schema.graph")
}

func validateEmbeddedSchema(value map[string]any, path, digest, category string) error {
	schemaValue, err := parseJSONBounded(
		readPinned(path, digest),
		category,
		category,
	)
	if err != nil {
		return fail(category)
	}
	schema, ok := objectValue(schemaValue)
	if !ok || !schemaValidate(value, schema, schema) {
		return fail(category)
	}
	return nil
}

func schemaValidate(value any, rule, root map[string]any) bool {
	if refValue, exists := rule["$ref"]; exists {
		ref, ok := stringValue(refValue)
		const prefix = "#/$defs/"
		if !ok || !strings.HasPrefix(ref, prefix) {
			return false
		}
		defs, ok := objectValue(root["$defs"])
		if !ok {
			return false
		}
		target, ok := objectValue(defs[strings.TrimPrefix(ref, prefix)])
		return ok && schemaValidate(value, target, root)
	}
	if options, exists := rule["oneOf"]; exists {
		rows, ok := arrayValue(options)
		if !ok {
			return false
		}
		matches := 0
		for _, raw := range rows {
			option, ok := objectValue(raw)
			if ok && schemaValidate(value, option, root) {
				matches++
			}
		}
		return matches == 1
	}
	if options, exists := rule["anyOf"]; exists {
		rows, ok := arrayValue(options)
		if !ok {
			return false
		}
		matched := false
		for _, raw := range rows {
			option, ok := objectValue(raw)
			if ok && schemaValidate(value, option, root) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if options, exists := rule["allOf"]; exists {
		rows, ok := arrayValue(options)
		if !ok {
			return false
		}
		for _, raw := range rows {
			option, ok := objectValue(raw)
			if !ok || !schemaValidate(value, option, root) {
				return false
			}
		}
	}
	if expected, exists := rule["type"]; exists && !schemaTypeMatches(value, expected) {
		return false
	}
	if expected, exists := rule["const"]; exists && !reflect.DeepEqual(value, expected) {
		return false
	}
	if allowed, exists := rule["enum"]; exists {
		rows, ok := arrayValue(allowed)
		if !ok {
			return false
		}
		found := false
		for _, candidate := range rows {
			if reflect.DeepEqual(value, candidate) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	switch current := value.(type) {
	case string:
		length := int64(utf8.RuneCountInString(current))
		if minimum, exists := schemaInt(rule["minLength"]); exists && length < minimum {
			return false
		}
		if maximum, exists := schemaInt(rule["maxLength"]); exists && length > maximum {
			return false
		}
		if rawPattern, exists := rule["pattern"]; exists {
			pattern, ok := stringValue(rawPattern)
			if !ok {
				return false
			}
			pattern = strings.ReplaceAll(pattern, "(?:", "(")
			compiled, err := regexp.Compile(pattern)
			if err != nil || !compiled.MatchString(current) {
				return false
			}
		}
	case json.Number:
		number, ok := integer(current)
		if !ok {
			return false
		}
		if minimum, exists := schemaInt(rule["minimum"]); exists && number < minimum {
			return false
		}
		if maximum, exists := schemaInt(rule["maximum"]); exists && number > maximum {
			return false
		}
	case []any:
		if minimum, exists := schemaInt(rule["minItems"]); exists && int64(len(current)) < minimum {
			return false
		}
		if maximum, exists := schemaInt(rule["maxItems"]); exists && int64(len(current)) > maximum {
			return false
		}
		if unique, _ := rule["uniqueItems"].(bool); unique {
			seen := make(map[string]struct{}, len(current))
			for _, item := range current {
				key, err := canonicalKey(item)
				if err != nil {
					return false
				}
				if _, exists := seen[key]; exists {
					return false
				}
				seen[key] = struct{}{}
			}
		}
		if itemRuleRaw, exists := rule["items"]; exists {
			itemRule, ok := objectValue(itemRuleRaw)
			if !ok {
				return false
			}
			for _, item := range current {
				if !schemaValidate(item, itemRule, root) {
					return false
				}
			}
		}
	case map[string]any:
		if requiredRaw, exists := rule["required"]; exists {
			required, ok := arrayValue(requiredRaw)
			if !ok {
				return false
			}
			for _, raw := range required {
				key, ok := stringValue(raw)
				if !ok {
					return false
				}
				if _, exists := current[key]; !exists {
					return false
				}
			}
		}
		properties, _ := objectValue(rule["properties"])
		if closed, exists := rule["additionalProperties"].(bool); exists && !closed {
			for key := range current {
				if _, exists := properties[key]; !exists {
					return false
				}
			}
		}
		for key, item := range current {
			if propertyRaw, exists := properties[key]; exists {
				property, ok := objectValue(propertyRaw)
				if !ok || !schemaValidate(item, property, root) {
					return false
				}
			}
		}
	}
	return true
}

func schemaTypeMatches(value, expected any) bool {
	if rows, ok := arrayValue(expected); ok {
		for _, row := range rows {
			if schemaTypeMatches(value, row) {
				return true
			}
		}
		return false
	}
	name, ok := stringValue(expected)
	if !ok {
		return false
	}
	switch name {
	case "object":
		_, ok = value.(map[string]any)
	case "array":
		_, ok = value.([]any)
	case "string":
		_, ok = value.(string)
	case "integer":
		_, ok = integer(value)
	case "boolean":
		_, ok = value.(bool)
	case "null":
		ok = value == nil
	default:
		ok = false
	}
	return ok
}

func schemaInt(value any) (int64, bool) {
	if value == nil {
		return 0, false
	}
	return integer(value)
}
