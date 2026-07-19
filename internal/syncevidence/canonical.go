package syncevidence

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
	"unicode/utf16"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

type categoryError struct {
	category string
	cause    error
}

func (err *categoryError) Error() string { return err.category }
func (err *categoryError) Unwrap() error { return err.cause }

func contractError(category string) error {
	return &categoryError{category: category, cause: ErrContractViolation}
}

type jsonStats struct {
	maxDepth    uint64
	maxString   uint64
	arrayItems  uint64
	objectItems uint64
	totalValues uint64
}

const maxStructuralValuesV1 = uint64(8_388_608)

func CanonicalizeJSON(raw []byte) ([]byte, error) {
	value, _, err := parseJSON(raw, DefaultLimitsV1(), true)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := appendCanonical(&out, value); err != nil {
		return nil, contractError("schema.bundle")
	}
	return out.Bytes(), nil
}

func parseJSON(raw []byte, limits CaptureLimitsV1, bundle bool) (any, jsonStats, error) {
	maximum := limits.MaxArtifactBytes
	if bundle {
		maximum = limits.MaxBundleBytes
	}
	if maximum == 0 || uint64(len(raw)) > maximum {
		return nil, jsonStats{}, contractError("limits.exceeded")
	}
	if !utf8.Valid(raw) {
		return nil, jsonStats{}, contractError("schema.bundle")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	stats := jsonStats{}
	structuralBudget := uint64(len(raw)) + 1
	if structuralBudget > maxStructuralValuesV1 {
		structuralBudget = maxStructuralValuesV1
	}
	value, err := decodeJSONValue(decoder, 1, limits, structuralBudget, &stats)
	if err != nil {
		return nil, jsonStats{}, err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return nil, jsonStats{}, contractError("schema.bundle")
	}
	return value, stats, nil
}

func decodeJSONValue(decoder *json.Decoder, depth uint64, limits CaptureLimitsV1, structuralBudget uint64, stats *jsonStats) (any, error) {
	if depth > limits.MaxDepth {
		return nil, contractError("limits.exceeded")
	}
	if depth > stats.maxDepth {
		stats.maxDepth = depth
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, contractError("schema.bundle")
	}
	stats.totalValues++
	if stats.totalValues > structuralBudget {
		return nil, contractError("limits.exceeded")
	}
	switch value := token.(type) {
	case nil, bool:
		return value, nil
	case string:
		if err := validateJSONString(value, limits.MaxStringBytes); err != nil {
			return nil, err
		}
		if uint64(len(value)) > stats.maxString {
			stats.maxString = uint64(len(value))
		}
		return value, nil
	case json.Number:
		if !isSafeInteger(string(value)) {
			return nil, contractError("schema.bundle")
		}
		return value, nil
	case json.Delim:
		switch value {
		case '{':
			object := make(map[string]any)
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, contractError("schema.bundle")
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, contractError("schema.bundle")
				}
				if err := validateJSONKey(key, limits.MaxStringBytes); err != nil {
					return nil, err
				}
				stats.objectItems++
				stats.totalValues++
				if stats.totalValues > structuralBudget {
					return nil, contractError("limits.exceeded")
				}
				if _, duplicate := object[key]; duplicate {
					return nil, contractError("schema.bundle")
				}
				child, err := decodeJSONValue(decoder, depth+1, limits, structuralBudget, stats)
				if err != nil {
					return nil, err
				}
				object[key] = child
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
				return nil, contractError("schema.bundle")
			}
			return object, nil
		case '[':
			array := make([]any, 0)
			for decoder.More() {
				child, err := decodeJSONValue(decoder, depth+1, limits, structuralBudget, stats)
				if err != nil {
					return nil, err
				}
				array = append(array, child)
				stats.arrayItems++
			}
			if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
				return nil, contractError("schema.bundle")
			}
			return array, nil
		default:
			return nil, contractError("schema.bundle")
		}
	default:
		return nil, contractError("schema.bundle")
	}
}

func validateJSONKey(value string, maximum uint64) error {
	if err := validateJSONString(value, maximum); err != nil {
		return err
	}
	for index := range value {
		if value[index] < 0x20 || value[index] > 0x7e {
			return contractError("schema.bundle")
		}
	}
	return nil
}

func validateJSONString(value string, maximum uint64) error {
	if maximum == 0 || uint64(len(value)) > maximum {
		return contractError("limits.exceeded")
	}
	if !utf8.ValidString(value) || !norm.NFC.IsNormalString(value) || strings.ContainsRune(value, utf8.RuneError) {
		return contractError("schema.bundle")
	}
	for _, char := range value {
		if char < 0x20 {
			return contractError("schema.bundle")
		}
	}
	return nil
}

func isSafeInteger(value string) bool {
	if value == "0" {
		return true
	}
	if value == "" || value[0] < '1' || value[0] > '9' {
		return false
	}
	for _, char := range value[1:] {
		if char < '0' || char > '9' {
			return false
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && parsed <= MaxSafeIntegerV1
}

func appendCanonical(out *bytes.Buffer, value any) error {
	switch current := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		if current {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case string:
		appendJSONString(out, current)
	case json.Number:
		if !isSafeInteger(string(current)) {
			return ErrContractViolation
		}
		out.WriteString(string(current))
	case []any:
		out.WriteByte('[')
		for index, item := range current {
			if index > 0 {
				out.WriteByte(',')
			}
			if err := appendCanonical(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(current))
		for key := range current {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(left, right int) bool { return utf16Less(keys[left], keys[right]) })
		out.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				out.WriteByte(',')
			}
			appendJSONString(out, key)
			out.WriteByte(':')
			if err := appendCanonical(out, current[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("unsupported canonical value: %T", value)
	}
	return nil
}

func appendJSONString(out *bytes.Buffer, value string) {
	out.WriteByte('"')
	for _, char := range value {
		switch char {
		case '"', '\\':
			out.WriteByte('\\')
			out.WriteRune(char)
		case '\b':
			out.WriteString(`\b`)
		case '\t':
			out.WriteString(`\t`)
		case '\n':
			out.WriteString(`\n`)
		case '\f':
			out.WriteString(`\f`)
		case '\r':
			out.WriteString(`\r`)
		default:
			if char < 0x20 {
				fmt.Fprintf(out, `\u%04x`, char)
			} else {
				out.WriteRune(char)
			}
		}
	}
	out.WriteByte('"')
}

func utf16Less(left, right string) bool {
	leftUnits := utf16.Encode([]rune(left))
	rightUnits := utf16.Encode([]rune(right))
	for index := 0; index < len(leftUnits) && index < len(rightUnits); index++ {
		if leftUnits[index] != rightUnits[index] {
			return leftUnits[index] < rightUnits[index]
		}
	}
	return len(leftUnits) < len(rightUnits)
}

func canonicalMarshal(value any, limits CaptureLimitsV1, bundle bool) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, contractError("schema.bundle")
	}
	parsed, _, err := parseJSON(raw, limits, bundle)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := appendCanonical(&out, parsed); err != nil {
		return nil, contractError("schema.bundle")
	}
	return out.Bytes(), nil
}

func domainDigest(domain string, payload []byte) string {
	hash := sha256.New()
	_, _ = io.WriteString(hash, domain)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(payload)
	return hex.EncodeToString(hash.Sum(nil))
}

func HashContentBytes(content []byte) string {
	return "sha256:" + domainDigest(contentHashDomain, content)
}

func HashGitBlob(repository, commit, path string, blob []byte) (string, error) {
	if !validRepository(repository) || !gitCommitPattern.MatchString(commit) || !validRepositoryPath(path) {
		return "", ErrInvalidArgument
	}
	payload := make([]byte, 0, len(repository)+len(commit)+len(path)+len(blob)+3)
	payload = append(payload, repository...)
	payload = append(payload, 0)
	payload = append(payload, commit...)
	payload = append(payload, 0)
	payload = append(payload, path...)
	payload = append(payload, 0)
	payload = append(payload, blob...)
	return "sha256:" + domainDigest(gitBlobHashDomain, payload), nil
}
