package coexistence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

func parseEvidenceJSON(raw []byte) (any, error) {
	if len(raw) == 0 {
		return nil, fail("schema.evidence")
	}
	if preflightExceeded(raw) {
		return nil, fail("limits.exceeded")
	}
	return parseJSON(raw, "json.syntax")
}

func parseCategorizedJSON(raw []byte, category string) (any, error) {
	if len(raw) == 0 || int64(len(raw)) > hardLimitsV1["max_evidence_bytes"] {
		return nil, fail(category)
	}
	return parseJSON(raw, category)
}

func parseJSON(raw []byte, category string) (any, error) {
	if !utf8.Valid(raw) {
		return nil, fail(category)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeJSONValue(decoder, category)
	if err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return nil, fail(category)
	}
	return value, nil
}

func decodeJSONValue(decoder *json.Decoder, category string) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, fail(category)
	}
	switch value := token.(type) {
	case nil, bool, string:
		return value, nil
	case json.Number:
		if !validSafeInteger(string(value)) {
			return nil, fail(category)
		}
		return value, nil
	case json.Delim:
		switch value {
		case '{':
			object := make(map[string]any)
			for decoder.More() {
				keyToken, keyErr := decoder.Token()
				if keyErr != nil {
					return nil, fail(category)
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, fail(category)
				}
				if _, duplicate := object[key]; duplicate {
					return nil, fail(category)
				}
				child, childErr := decodeJSONValue(decoder, category)
				if childErr != nil {
					return nil, childErr
				}
				object[key] = child
			}
			if end, endErr := decoder.Token(); endErr != nil || end != json.Delim('}') {
				return nil, fail(category)
			}
			return object, nil
		case '[':
			array := make([]any, 0)
			for decoder.More() {
				child, childErr := decodeJSONValue(decoder, category)
				if childErr != nil {
					return nil, childErr
				}
				array = append(array, child)
			}
			if end, endErr := decoder.Token(); endErr != nil || end != json.Delim(']') {
				return nil, fail(category)
			}
			return array, nil
		default:
			return nil, fail(category)
		}
	default:
		return nil, fail(category)
	}
}

type preflightContainer struct {
	list      bool
	expecting bool
}

func preflightExceeded(raw []byte) bool {
	if int64(len(raw)) > hardLimitsV1["max_evidence_bytes"] {
		return true
	}
	var depth, members, listItems int64
	var inString, escaped bool
	var stringBytes int64
	stack := make([]preflightContainer, 0, hardLimitsV1["max_depth"])

	markListValue := func() bool {
		if len(stack) == 0 || !stack[len(stack)-1].list || !stack[len(stack)-1].expecting {
			return false
		}
		stack[len(stack)-1].expecting = false
		listItems++
		return listItems > hardLimitsV1["max_total_list_items"]
	}

	for _, current := range raw {
		if inString {
			switch {
			case escaped:
				escaped = false
				stringBytes++
			case current == '\\':
				escaped = true
				stringBytes++
			case current == '"':
				inString = false
			default:
				stringBytes++
			}
			if stringBytes > hardLimitsV1["max_string_bytes"] {
				return true
			}
			continue
		}
		switch current {
		case ' ', '\t', '\r', '\n':
			continue
		case '"':
			if markListValue() {
				return true
			}
			inString = true
			stringBytes = 0
		case '{', '[':
			if markListValue() {
				return true
			}
			depth++
			if depth > hardLimitsV1["max_depth"] {
				return true
			}
			stack = append(stack, preflightContainer{list: current == '[', expecting: current == '['})
		case '}', ']':
			if depth > 0 && len(stack) > 0 {
				depth--
				stack = stack[:len(stack)-1]
			}
		case ':':
			members++
			if members > hardLimitsV1["max_total_members"] {
				return true
			}
		case ',':
			if len(stack) > 0 && stack[len(stack)-1].list {
				stack[len(stack)-1].expecting = true
			}
		default:
			if markListValue() {
				return true
			}
		}
	}
	return false
}

func validSafeInteger(value string) bool {
	if value == "" || value == "-0" || strings.ContainsAny(value, ".eE+") {
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

func marshalCanonicalV1(value any) ([]byte, error) {
	var output bytes.Buffer
	if err := appendCanonical(&output, value); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func appendCanonical(output *bytes.Buffer, value any) error {
	switch current := value.(type) {
	case nil:
		output.WriteString("null")
	case bool:
		if current {
			output.WriteString("true")
		} else {
			output.WriteString("false")
		}
	case string:
		appendJSONString(output, current)
	case json.Number:
		if !validSafeInteger(string(current)) {
			return fail("json.syntax")
		}
		output.WriteString(string(current))
	case int:
		return appendCanonical(output, json.Number(strconv.Itoa(current)))
	case int64:
		return appendCanonical(output, json.Number(strconv.FormatInt(current, 10)))
	case uint64:
		if current > uint64(maxSafeIntegerV1) {
			return fail("json.syntax")
		}
		return appendCanonical(output, json.Number(strconv.FormatUint(current, 10)))
	case []any:
		output.WriteByte('[')
		for index, item := range current {
			if index > 0 {
				output.WriteByte(',')
			}
			if err := appendCanonical(output, item); err != nil {
				return err
			}
		}
		output.WriteByte(']')
	case []string:
		items := make([]any, len(current))
		for index := range current {
			items[index] = current[index]
		}
		return appendCanonical(output, items)
	case map[string]any:
		keys := make([]string, 0, len(current))
		for key := range current {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		output.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				output.WriteByte(',')
			}
			appendJSONString(output, key)
			output.WriteByte(':')
			if err := appendCanonical(output, current[key]); err != nil {
				return err
			}
		}
		output.WriteByte('}')
	default:
		return fmt.Errorf("coexistence: unsupported canonical value %T", value)
	}
	return nil
}

func appendJSONString(output *bytes.Buffer, value string) {
	output.WriteByte('"')
	for _, current := range value {
		switch current {
		case '"', '\\':
			output.WriteByte('\\')
			output.WriteRune(current)
		case '\b':
			output.WriteString(`\b`)
		case '\t':
			output.WriteString(`\t`)
		case '\n':
			output.WriteString(`\n`)
		case '\f':
			output.WriteString(`\f`)
		case '\r':
			output.WriteString(`\r`)
		default:
			if current < 0x20 {
				fmt.Fprintf(output, `\u%04x`, current)
			} else {
				output.WriteRune(current)
			}
		}
	}
	output.WriteByte('"')
}

func prettyJSON(value any) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func domainDigestV1(domain string, value any) (string, error) {
	canonical, err := marshalCanonicalV1(value)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	_, _ = io.WriteString(digest, domain)
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(canonical)
	return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
}

func rawSHA256V1(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func cloneJSONV1(value any) any {
	switch current := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(current))
		for key, item := range current {
			result[key] = cloneJSONV1(item)
		}
		return result
	case []any:
		result := make([]any, len(current))
		for index, item := range current {
			result[index] = cloneJSONV1(item)
		}
		return result
	default:
		return current
	}
}

func withoutJSONKeysV1(object map[string]any, keys ...string) map[string]any {
	result := cloneJSONV1(object).(map[string]any)
	for _, key := range keys {
		delete(result, key)
	}
	return result
}

func payloadShapeV1(value any) any {
	switch current := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(current))
		for key, item := range current {
			result[key] = payloadShapeV1(item)
		}
		return result
	case []any:
		result := make([]any, len(current))
		for index, item := range current {
			result[index] = payloadShapeV1(item)
		}
		return result
	case nil:
		return "null"
	case bool:
		return "boolean"
	case json.Number, int, int64, uint64:
		return "integer"
	default:
		return "string"
	}
}

func number(value int64) json.Number {
	return json.Number(strconv.FormatInt(value, 10))
}

func objectValueV1(value any) (map[string]any, bool) {
	result, ok := value.(map[string]any)
	return result, ok
}

func arrayValueV1(value any) ([]any, bool) {
	result, ok := value.([]any)
	return result, ok
}

func stringValueV1(value any) (string, bool) {
	result, ok := value.(string)
	return result, ok
}

func integerValue(value any) (int64, bool) {
	numeric, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseInt(string(numeric), 10, 64)
	return parsed, err == nil
}

func boolValueV1(value any) (bool, bool) {
	result, ok := value.(bool)
	return result, ok
}

func stringsFromArray(value any) ([]string, bool) {
	items, ok := arrayValueV1(value)
	if !ok {
		return nil, false
	}
	result := make([]string, len(items))
	for index, item := range items {
		text, valid := stringValueV1(item)
		if !valid {
			return nil, false
		}
		result[index] = text
	}
	return result, true
}

func exactKeys(value any, keys ...string) bool {
	object, ok := objectValueV1(value)
	if !ok || len(object) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, exists := object[key]; !exists {
			return false
		}
	}
	return true
}
