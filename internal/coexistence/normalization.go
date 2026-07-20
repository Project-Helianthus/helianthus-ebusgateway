package coexistence

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	digestPatternV1 = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	shaPatternV1    = regexp.MustCompile(`^[0-9a-f]{40}$`)
	rfc3339UTCV1    = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z$`)
)

func normalizedPayload(payload any, rule, profile map[string]any) (any, error) {
	result := cloneJSONV1(payload)
	timestamps, ok := stringsFromArray(rule["timestamp_pointers"])
	if !ok {
		return nil, fail("canonicalization.invalid")
	}
	masks, ok := stringsFromArray(rule["mask_pointers"])
	if !ok {
		return nil, fail("canonicalization.invalid")
	}
	seen := make(map[string]bool, len(timestamps)+len(masks))
	for _, pointer := range append(append([]string(nil), timestamps...), masks...) {
		if seen[pointer] {
			return nil, fail("canonicalization.invalid")
		}
		seen[pointer] = true
	}
	timestampReplacement, ok := stringValueV1(profile["timestamp_replacement"])
	if !ok {
		return nil, fail("canonicalization.invalid")
	}
	maskReplacement, ok := stringValueV1(profile["mask_replacement"])
	if !ok {
		return nil, fail("canonicalization.invalid")
	}
	for _, pointer := range timestamps {
		value, set, err := pointerTarget(result, pointer)
		if err != nil || !validRFC3339UTC(value) {
			return nil, fail("canonicalization.invalid")
		}
		set(timestampReplacement)
	}
	for _, pointer := range masks {
		_, set, err := pointerTarget(result, pointer)
		if err != nil {
			return nil, fail("canonicalization.invalid")
		}
		set(maskReplacement)
	}
	return result, nil
}

func validRFC3339UTC(value string) bool {
	if !rfc3339UTCV1.MatchString(value) {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	return err == nil && parsed.Location() == time.UTC
}

func containsV2Marker(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(lower, ".v2") || strings.Contains(lower, "_v2") ||
		strings.Contains(lower, "/v2") || strings.Contains(lower, "-v2")
}

func pointerTarget(value any, pointer string) (string, func(string), error) {
	if pointer == "" || pointer[0] != '/' {
		return "", nil, fail("canonicalization.invalid")
	}
	parts := strings.Split(pointer[1:], "/")
	for index := range parts {
		parts[index] = strings.ReplaceAll(strings.ReplaceAll(parts[index], "~1", "/"), "~0", "~")
	}
	current := value
	for _, part := range parts[:len(parts)-1] {
		switch typed := current.(type) {
		case map[string]any:
			child, exists := typed[part]
			if !exists {
				return "", nil, fail("canonicalization.invalid")
			}
			current = child
		case []any:
			index, ok := canonicalArrayIndex(part, len(typed))
			if !ok {
				return "", nil, fail("canonicalization.invalid")
			}
			current = typed[index]
		default:
			return "", nil, fail("canonicalization.invalid")
		}
	}
	leaf := parts[len(parts)-1]
	switch typed := current.(type) {
	case map[string]any:
		target, exists := typed[leaf]
		text, valid := stringValueV1(target)
		if !exists || !valid {
			return "", nil, fail("canonicalization.invalid")
		}
		return text, func(replacement string) { typed[leaf] = replacement }, nil
	case []any:
		index, ok := canonicalArrayIndex(leaf, len(typed))
		if !ok {
			return "", nil, fail("canonicalization.invalid")
		}
		text, valid := stringValueV1(typed[index])
		if !valid {
			return "", nil, fail("canonicalization.invalid")
		}
		return text, func(replacement string) { typed[index] = replacement }, nil
	default:
		return "", nil, fail("canonicalization.invalid")
	}
}

func canonicalArrayIndex(value string, length int) (int, bool) {
	if value == "" || len(value) > 1 && value[0] == '0' {
		return 0, false
	}
	index, err := strconv.Atoi(value)
	return index, err == nil && index >= 0 && index < length
}

func rulesByViewIDV1(registry map[string]any) map[string]map[string]any {
	rules := make(map[string]map[string]any)
	items, _ := arrayValueV1(registry["view_rules"])
	for _, raw := range items {
		rule, _ := objectValueV1(raw)
		viewID, _ := stringValueV1(rule["view_id"])
		rules[viewID] = rule
	}
	return rules
}

func findViewV1(run map[string]any, viewID string) (map[string]any, bool) {
	views, ok := arrayValueV1(run["protected_views"])
	if !ok {
		return nil, false
	}
	for _, raw := range views {
		view, valid := objectValueV1(raw)
		id, _ := stringValueV1(view["view_id"])
		if valid && id == viewID {
			return view, true
		}
	}
	return nil, false
}
