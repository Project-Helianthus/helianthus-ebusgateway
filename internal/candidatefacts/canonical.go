package candidatefacts

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
)

const maxSafeIntegerV1 = int64(9007199254740991)

type categoryError string

func (err categoryError) Error() string { return string(err) }

func fail(category string) error { return categoryError(category) }

func parseJSON(raw []byte) (any, error) {
	if !utf8.Valid(raw) {
		return nil, fail("json.syntax")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeJSONValue(decoder)
	if err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return nil, fail("json.syntax")
	}
	return value, nil
}

func decodeJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, fail("json.syntax")
	}
	switch value := token.(type) {
	case nil, bool, string:
		return value, nil
	case json.Number:
		if !validSafeInteger(string(value)) {
			return nil, fail("json.syntax")
		}
		return value, nil
	case json.Delim:
		switch value {
		case '{':
			object := make(map[string]any)
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, fail("json.syntax")
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, fail("json.syntax")
				}
				if _, duplicate := object[key]; duplicate {
					return nil, fail("json.syntax")
				}
				child, err := decodeJSONValue(decoder)
				if err != nil {
					return nil, err
				}
				object[key] = child
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return nil, fail("json.syntax")
			}
			return object, nil
		case '[':
			array := make([]any, 0)
			for decoder.More() {
				child, err := decodeJSONValue(decoder)
				if err != nil {
					return nil, err
				}
				array = append(array, child)
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
				return nil, fail("json.syntax")
			}
			return array, nil
		default:
			return nil, fail("json.syntax")
		}
	default:
		return nil, fail("json.syntax")
	}
}

func validSafeInteger(value string) bool {
	if value == "-0" || value == "" || strings.ContainsAny(value, ".eE+") {
		return false
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < -maxSafeIntegerV1 || parsed > maxSafeIntegerV1 {
		return false
	}
	if value == "0" {
		return true
	}
	if value[0] == '-' {
		value = value[1:]
	}
	return value != "" && value[0] >= '1' && value[0] <= '9'
}

func canonicalJSON(value any) ([]byte, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, fail("json.syntax")
	}
	raw := bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'})
	canonical, err := syncevidence.CanonicalizeJSON(raw)
	if err != nil {
		return nil, fail("json.syntax")
	}
	return canonical, nil
}

func decodeTypedGraph(value map[string]any) (GraphV1, error) {
	raw, err := canonicalJSON(value)
	if err != nil {
		return GraphV1{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var graph GraphV1
	if err := decoder.Decode(&graph); err != nil {
		return GraphV1{}, fail("schema.graph")
	}
	return graph, nil
}

func integer(value any) (int64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseInt(string(number), 10, 64)
	return parsed, err == nil
}

func unsigned(value any) (uint64, bool) {
	parsed, ok := integer(value)
	return uint64(parsed), ok && parsed >= 0
}

func boundedInteger(value any, minimum, maximum int64) bool {
	parsed, ok := integer(value)
	return ok && parsed >= minimum && parsed <= maximum
}

func stringValue(value any) (string, bool) {
	result, ok := value.(string)
	return result, ok
}

func objectValue(value any) (map[string]any, bool) {
	result, ok := value.(map[string]any)
	return result, ok
}

func arrayValue(value any) ([]any, bool) {
	result, ok := value.([]any)
	return result, ok
}

func exactKeys(value any, expected ...string) bool {
	object, ok := objectValue(value)
	if !ok || len(object) != len(expected) {
		return false
	}
	for _, key := range expected {
		if _, exists := object[key]; !exists {
			return false
		}
	}
	return true
}

func token(value any) bool {
	text, ok := stringValue(value)
	return ok && len(text) > 0 && len(text) <= 256 && !strings.ContainsRune(text, 0)
}

func checkPortable(value any, depth uint64, limits map[string]uint64) error {
	if depth > limits["max_depth"] {
		return fail("limits.exceeded")
	}
	switch current := value.(type) {
	case nil, bool, json.Number:
		return nil
	case string:
		if uint64(len(current)) > limits["max_string_bytes"] || strings.ContainsRune(current, 0) {
			return fail("limits.exceeded")
		}
		return nil
	case []any:
		for _, item := range current {
			if err := checkPortable(item, depth+1, limits); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		for key, item := range current {
			if err := checkPortable(key, depth+1, limits); err != nil {
				return err
			}
			if err := checkPortable(item, depth+1, limits); err != nil {
				return err
			}
		}
		return nil
	default:
		return fail("json.syntax")
	}
}

func decimalString(value any, nonnegative bool) bool {
	text, ok := stringValue(value)
	if !ok || text == "" || strings.ContainsAny(text, "eE+") {
		return false
	}
	if text == "-0" || text == "-0.0" {
		return !nonnegative
	}
	if nonnegative && text[0] == '-' {
		return false
	}
	canonical := text
	if canonical[0] == '-' {
		canonical = canonical[1:]
	}
	parts := strings.Split(canonical, ".")
	if len(parts) > 2 || parts[0] == "" || len(parts) == 2 && parts[1] == "" {
		return false
	}
	if len(parts[0]) > 1 && parts[0][0] == '0' {
		return false
	}
	for _, part := range parts {
		for _, char := range part {
			if char < '0' || char > '9' {
				return false
			}
		}
	}
	return true
}
