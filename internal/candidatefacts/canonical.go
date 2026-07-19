package candidatefacts

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
)

const maxSafeIntegerV1 = int64(9007199254740991)

var hardLimitsV1 = map[string]uint64{
	"max_graph_bytes":            1_048_576,
	"max_depth":                  32,
	"max_facts":                  64,
	"max_evidence_refs_per_fact": 16,
	"max_samples_per_comparator": 1024,
	"max_string_bytes":           4096,
	"max_path_segments":          32,
	"max_total_members":          16_384,
	"max_total_list_items":       8192,
}

type categoryError string

func (err categoryError) Error() string { return string(err) }

func fail(category string) error { return categoryError(category) }

func parseJSON(raw []byte) (any, error) {
	return parseJSONBounded(raw, "json.syntax", "limits.exceeded")
}

func parseJSONBounded(raw []byte, syntaxCategory, limitCategory string) (any, error) {
	if err := preflightJSON(raw, hardLimitsV1); err != nil {
		return nil, fail(limitCategory)
	}
	if !utf8.Valid(raw) {
		return nil, fail(syntaxCategory)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeJSONValue(decoder, syntaxCategory)
	if err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return nil, fail(syntaxCategory)
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
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, fail(category)
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, fail(category)
				}
				if _, duplicate := object[key]; duplicate {
					return nil, fail(category)
				}
				child, err := decodeJSONValue(decoder, category)
				if err != nil {
					return nil, err
				}
				object[key] = child
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return nil, fail(category)
			}
			return object, nil
		case '[':
			array := make([]any, 0)
			for decoder.More() {
				child, err := decodeJSONValue(decoder, category)
				if err != nil {
					return nil, err
				}
				array = append(array, child)
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
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
	items     uint64
}

func preflightJSON(raw []byte, limits map[string]uint64) error {
	if uint64(len(raw)) > limits["max_graph_bytes"] {
		return fail("limits.exceeded")
	}
	var depth, members, listItems uint64
	var inString, escaped bool
	var stringBytes uint64
	stack := make([]preflightContainer, 0, limits["max_depth"])

	markListValue := func() error {
		if len(stack) == 0 || !stack[len(stack)-1].list || !stack[len(stack)-1].expecting {
			return nil
		}
		index := len(stack) - 1
		stack[index].expecting = false
		stack[index].items++
		listItems++
		if stack[index].items > limits["max_samples_per_comparator"] || listItems > limits["max_total_list_items"] {
			return fail("limits.exceeded")
		}
		return nil
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
			if stringBytes > limits["max_string_bytes"] {
				return fail("limits.exceeded")
			}
			continue
		}
		switch current {
		case ' ', '\t', '\r', '\n':
			continue
		case '"':
			if err := markListValue(); err != nil {
				return err
			}
			inString = true
			stringBytes = 0
		case '{', '[':
			if err := markListValue(); err != nil {
				return err
			}
			depth++
			if depth > limits["max_depth"] {
				return fail("limits.exceeded")
			}
			stack = append(stack, preflightContainer{list: current == '[', expecting: current == '['})
		case '}', ']':
			if depth == 0 || len(stack) == 0 {
				continue
			}
			depth--
			stack = stack[:len(stack)-1]
		case ':':
			members++
			if members > limits["max_total_members"] {
				return fail("limits.exceeded")
			}
		case ',':
			if len(stack) > 0 && stack[len(stack)-1].list {
				stack[len(stack)-1].expecting = true
			}
		default:
			if err := markListValue(); err != nil {
				return err
			}
		}
	}
	return nil
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
	if err := checkTypedPortable(value); err != nil {
		return nil, err
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, fail("json.syntax")
	}
	raw := bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'})
	if uint64(len(raw)) > hardLimitsV1["max_graph_bytes"] {
		return nil, fail("limits.exceeded")
	}
	if err := preflightJSON(raw, hardLimitsV1); err != nil {
		return nil, err
	}
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

func number(value int64) any {
	return json.Number(strconv.FormatInt(value, 10))
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
	if !ok || len(text) == 0 || len(text) > 256 {
		return false
	}
	for _, current := range []byte(text) {
		if current < 0x20 || current > 0x7e {
			return false
		}
	}
	return true
}

type portableItem struct {
	value any
	depth uint64
}

func checkPortable(value any, limits map[string]uint64) error {
	stack := []portableItem{{value: value, depth: 1}}
	var members, listItems uint64
	for len(stack) > 0 {
		index := len(stack) - 1
		item := stack[index]
		stack = stack[:index]
		switch current := item.value.(type) {
		case nil, bool:
		case json.Number:
			if !validSafeInteger(string(current)) {
				return fail("json.syntax")
			}
		case string:
			if uint64(len(current)) > limits["max_string_bytes"] || strings.ContainsRune(current, 0) {
				return fail("limits.exceeded")
			}
		case []any:
			if item.depth > limits["max_depth"] || uint64(len(current)) > limits["max_samples_per_comparator"] {
				return fail("limits.exceeded")
			}
			listItems += uint64(len(current))
			if listItems > limits["max_total_list_items"] {
				return fail("limits.exceeded")
			}
			for child := len(current) - 1; child >= 0; child-- {
				stack = append(stack, portableItem{value: current[child], depth: item.depth + 1})
			}
		case map[string]any:
			if item.depth > limits["max_depth"] {
				return fail("limits.exceeded")
			}
			members += uint64(len(current))
			if members > limits["max_total_members"] {
				return fail("limits.exceeded")
			}
			for key, child := range current {
				stack = append(stack,
					portableItem{value: child, depth: item.depth + 1},
					portableItem{value: key, depth: item.depth + 1},
				)
			}
		default:
			return fail("json.syntax")
		}
	}
	return nil
}

func checkTypedPortable(value any) error {
	type reflected struct {
		value reflect.Value
		depth uint64
	}
	stack := []reflected{{value: reflect.ValueOf(value), depth: 1}}
	var members, listItems uint64
	for len(stack) > 0 {
		index := len(stack) - 1
		item := stack[index]
		stack = stack[:index]
		if !item.value.IsValid() {
			continue
		}
		for item.value.Kind() == reflect.Interface || item.value.Kind() == reflect.Pointer {
			if item.value.IsNil() {
				item.value = reflect.Value{}
				break
			}
			item.value = item.value.Elem()
		}
		if !item.value.IsValid() {
			continue
		}
		switch item.value.Kind() {
		case reflect.String:
			if uint64(item.value.Len()) > hardLimitsV1["max_string_bytes"] || strings.ContainsRune(item.value.String(), 0) {
				return fail("limits.exceeded")
			}
		case reflect.Slice, reflect.Array:
			length := item.value.Len()
			if item.depth > hardLimitsV1["max_depth"] || uint64(length) > hardLimitsV1["max_samples_per_comparator"] {
				return fail("limits.exceeded")
			}
			listItems += uint64(length)
			if listItems > hardLimitsV1["max_total_list_items"] {
				return fail("limits.exceeded")
			}
			for child := length - 1; child >= 0; child-- {
				stack = append(stack, reflected{value: item.value.Index(child), depth: item.depth + 1})
			}
		case reflect.Map:
			if item.depth > hardLimitsV1["max_depth"] {
				return fail("limits.exceeded")
			}
			members += uint64(item.value.Len())
			if members > hardLimitsV1["max_total_members"] {
				return fail("limits.exceeded")
			}
			iter := item.value.MapRange()
			for iter.Next() {
				stack = append(stack,
					reflected{value: iter.Key(), depth: item.depth + 1},
					reflected{value: iter.Value(), depth: item.depth + 1},
				)
			}
		case reflect.Struct:
			if item.depth > hardLimitsV1["max_depth"] {
				return fail("limits.exceeded")
			}
			members += uint64(item.value.NumField())
			if members > hardLimitsV1["max_total_members"] {
				return fail("limits.exceeded")
			}
			for field := item.value.NumField() - 1; field >= 0; field-- {
				stack = append(stack, reflected{value: item.value.Field(field), depth: item.depth + 1})
			}
		}
	}
	return nil
}

func decimalString(value any, nonnegative bool) bool {
	text, ok := stringValue(value)
	if !ok || len(text) == 0 || len(text) > 64 || strings.ContainsAny(text, "eE+") {
		return false
	}
	if text[0] == '-' {
		if nonnegative {
			return false
		}
		text = text[1:]
	}
	parts := strings.Split(text, ".")
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
	allZero := true
	for _, char := range strings.ReplaceAll(text, ".", "") {
		if char != '0' {
			allZero = false
			break
		}
	}
	return !allZero || !strings.HasPrefix(value.(string), "-")
}
