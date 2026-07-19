package mcp

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

const eebusV1MaxSafeInteger = int64(9007199254740991)

// eebusV1CanonicalHashJSON implements the RFC 8785 subset used by the
// eeBUS v1 contract: I-JSON values, UTF-16 object-key ordering, ECMAScript
// number rendering, and exact JSON-pointer exclusions from the hash view.
func eebusV1CanonicalHashJSON(source []byte, exclusions ...string) ([]byte, string, error) {
	if !utf8.Valid(source) {
		return nil, "", errors.New("canonical JSON input is not valid UTF-8")
	}
	if err := eebusV1ValidateJSONSurrogates(source); err != nil {
		return nil, "", err
	}

	decoder := json.NewDecoder(bytes.NewReader(source))
	decoder.UseNumber()
	value, err := eebusV1DecodeJSONValue(decoder)
	if err != nil {
		return nil, "", fmt.Errorf("decode canonical JSON input: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return nil, "", fmt.Errorf("decode canonical JSON input: %w", err)
	}
	if err := eebusV1ValidateJCSValue(value); err != nil {
		return nil, "", err
	}
	for _, pointer := range exclusions {
		if err := eebusV1DeleteJSONPointer(value, pointer); err != nil {
			return nil, "", err
		}
	}

	var canonical bytes.Buffer
	if err := eebusV1WriteJCS(&canonical, value); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(canonical.Bytes())
	return canonical.Bytes(), "sha256:" + hex.EncodeToString(sum[:]), nil
}

func eebusV1ValidateJSONSurrogates(source []byte) error {
	inString := false
	for index := 0; index < len(source); index++ {
		switch source[index] {
		case '"':
			inString = !inString
		case '\\':
			if !inString || index+1 >= len(source) {
				continue
			}
			index++
			if source[index] != 'u' {
				continue
			}
			codeUnit, ok := eebusV1ParseJSONCodeUnit(source, index+1)
			if !ok {
				return errors.New("canonical JSON contains an invalid Unicode escape")
			}
			index += 4
			switch {
			case codeUnit >= 0xd800 && codeUnit <= 0xdbff:
				if index+6 >= len(source) || source[index+1] != '\\' || source[index+2] != 'u' {
					return errors.New("canonical JSON contains an unpaired Unicode surrogate")
				}
				low, valid := eebusV1ParseJSONCodeUnit(source, index+3)
				if !valid || low < 0xdc00 || low > 0xdfff {
					return errors.New("canonical JSON contains an unpaired Unicode surrogate")
				}
				index += 6
			case codeUnit >= 0xdc00 && codeUnit <= 0xdfff:
				return errors.New("canonical JSON contains an unpaired Unicode surrogate")
			}
		}
	}
	return nil
}

func eebusV1ParseJSONCodeUnit(source []byte, start int) (uint16, bool) {
	if start < 0 || start+4 > len(source) {
		return 0, false
	}
	var value uint16
	for _, character := range source[start : start+4] {
		value <<= 4
		switch {
		case character >= '0' && character <= '9':
			value |= uint16(character - '0')
		case character >= 'a' && character <= 'f':
			value |= uint16(character-'a') + 10
		case character >= 'A' && character <= 'F':
			value |= uint16(character-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func eebusV1DecodeJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		switch token.(type) {
		case nil, bool, string, json.Number:
			return token, nil
		default:
			return nil, fmt.Errorf("unsupported JSON token %T", token)
		}
	}

	switch delimiter {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			rawKey, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := rawKey.(string)
			if !ok {
				return nil, errors.New("JSON object key is not a string")
			}
			if _, exists := object[key]; exists {
				return nil, fmt.Errorf("duplicate JSON object key %q", key)
			}
			value, err := eebusV1DecodeJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return nil, errors.New("unterminated JSON object")
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, err := eebusV1DecodeJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return nil, errors.New("unterminated JSON array")
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func eebusV1ValidateJCSValue(value any) error {
	switch typed := value.(type) {
	case nil, bool:
		return nil
	case string:
		if !utf8.ValidString(typed) {
			return errors.New("canonical JSON string is not valid UTF-8")
		}
		return nil
	case json.Number:
		_, err := eebusV1FormatJCSNumber(typed)
		return err
	case []any:
		for _, item := range typed {
			if err := eebusV1ValidateJCSValue(item); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		for key, item := range typed {
			if !utf8.ValidString(key) {
				return errors.New("canonical JSON object key is not valid UTF-8")
			}
			if err := eebusV1ValidateJCSValue(item); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported canonical JSON value %T", value)
	}
}

func eebusV1DeleteJSONPointer(root any, pointer string) error {
	if pointer == "" {
		return errors.New("canonical JSON exclusion cannot remove the root")
	}
	if !strings.HasPrefix(pointer, "/") {
		return fmt.Errorf("invalid JSON pointer %q", pointer)
	}
	parts := strings.Split(pointer[1:], "/")
	for index := range parts {
		decoded, err := eebusV1DecodeJSONPointerToken(parts[index])
		if err != nil {
			return fmt.Errorf("invalid JSON pointer %q: %w", pointer, err)
		}
		parts[index] = decoded
	}

	current := root
	for _, part := range parts[:len(parts)-1] {
		switch typed := current.(type) {
		case map[string]any:
			next, exists := typed[part]
			if !exists {
				return nil
			}
			current = next
		case []any:
			position, err := strconv.Atoi(part)
			if err != nil || position < 0 || position >= len(typed) {
				return nil
			}
			current = typed[position]
		default:
			return nil
		}
	}

	leaf := parts[len(parts)-1]
	switch typed := current.(type) {
	case map[string]any:
		delete(typed, leaf)
	case []any:
		position, err := strconv.Atoi(leaf)
		if err == nil && position >= 0 && position < len(typed) {
			typed[position] = nil
		}
	}
	return nil
}

func eebusV1DecodeJSONPointerToken(token string) (string, error) {
	var decoded strings.Builder
	for index := 0; index < len(token); index++ {
		if token[index] != '~' {
			decoded.WriteByte(token[index])
			continue
		}
		if index+1 >= len(token) {
			return "", errors.New("truncated escape")
		}
		index++
		switch token[index] {
		case '0':
			decoded.WriteByte('~')
		case '1':
			decoded.WriteByte('/')
		default:
			return "", errors.New("invalid escape")
		}
	}
	return decoded.String(), nil
}

func eebusV1WriteJCS(target *bytes.Buffer, value any) error {
	switch typed := value.(type) {
	case nil:
		target.WriteString("null")
	case bool:
		if typed {
			target.WriteString("true")
		} else {
			target.WriteString("false")
		}
	case string:
		eebusV1WriteJCSString(target, typed)
	case json.Number:
		number, err := eebusV1FormatJCSNumber(typed)
		if err != nil {
			return err
		}
		target.WriteString(number)
	case []any:
		target.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				target.WriteByte(',')
			}
			if err := eebusV1WriteJCS(target, item); err != nil {
				return err
			}
		}
		target.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return eebusV1UTF16Less(keys[i], keys[j])
		})
		target.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				target.WriteByte(',')
			}
			eebusV1WriteJCSString(target, key)
			target.WriteByte(':')
			if err := eebusV1WriteJCS(target, typed[key]); err != nil {
				return err
			}
		}
		target.WriteByte('}')
	default:
		return fmt.Errorf("unsupported canonical JSON value %T", value)
	}
	return nil
}

func eebusV1UTF16Less(left, right string) bool {
	leftUnits := utf16.Encode([]rune(left))
	rightUnits := utf16.Encode([]rune(right))
	for index := 0; index < len(leftUnits) && index < len(rightUnits); index++ {
		if leftUnits[index] != rightUnits[index] {
			return leftUnits[index] < rightUnits[index]
		}
	}
	return len(leftUnits) < len(rightUnits)
}

func eebusV1WriteJCSString(target *bytes.Buffer, value string) {
	const hexDigits = "0123456789abcdef"
	target.WriteByte('"')
	for _, character := range value {
		switch character {
		case '\b':
			target.WriteString(`\b`)
		case '\t':
			target.WriteString(`\t`)
		case '\n':
			target.WriteString(`\n`)
		case '\f':
			target.WriteString(`\f`)
		case '\r':
			target.WriteString(`\r`)
		case '"':
			target.WriteString(`\"`)
		case '\\':
			target.WriteString(`\\`)
		default:
			if character >= 0 && character <= 0x1f {
				target.WriteString(`\u00`)
				target.WriteByte(hexDigits[byte(character)>>4])
				target.WriteByte(hexDigits[byte(character)&0x0f])
			} else {
				target.WriteRune(character)
			}
		}
	}
	target.WriteByte('"')
}

func eebusV1FormatJCSNumber(number json.Number) (string, error) {
	text := number.String()
	value, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return "", errors.New("canonical JSON number is not finite IEEE-754")
	}
	if value == 0 && strings.HasPrefix(text, "-") {
		return "", errors.New("canonical JSON number is negative zero")
	}

	exact, _, err := big.ParseFloat(text, 10, 512, big.ToNearestEven)
	if err != nil {
		return "", errors.New("canonical JSON number is invalid")
	}
	absExact := new(big.Float).SetPrec(512).Abs(exact)
	limit := new(big.Float).SetPrec(512).SetInt64(eebusV1MaxSafeInteger)
	if absExact.Cmp(limit) > 0 {
		return "", errors.New("canonical JSON number exceeds the portable safe-integer range")
	}
	if value == 0 {
		return "0", nil
	}

	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}
	scientific := strconv.FormatFloat(value, 'e', -1, 64)
	exponentMarker := strings.IndexByte(scientific, 'e')
	if exponentMarker < 0 {
		return "", errors.New("canonical JSON number formatting failed")
	}
	mantissa := scientific[:exponentMarker]
	exponent, err := strconv.Atoi(scientific[exponentMarker+1:])
	if err != nil {
		return "", errors.New("canonical JSON exponent formatting failed")
	}
	digits := strings.ReplaceAll(mantissa, ".", "")
	decimalPosition := exponent + 1

	switch {
	case decimalPosition > 0 && decimalPosition <= 21:
		if len(digits) <= decimalPosition {
			return sign + digits + strings.Repeat("0", decimalPosition-len(digits)), nil
		}
		return sign + digits[:decimalPosition] + "." + digits[decimalPosition:], nil
	case decimalPosition <= 0 && decimalPosition > -6:
		return sign + "0." + strings.Repeat("0", -decimalPosition) + digits, nil
	default:
		coefficient := digits[:1]
		if len(digits) > 1 {
			coefficient += "." + digits[1:]
		}
		exponentText := strconv.Itoa(decimalPosition - 1)
		if decimalPosition-1 >= 0 {
			exponentText = "+" + exponentText
		}
		return sign + coefficient + "e" + exponentText, nil
	}
}
