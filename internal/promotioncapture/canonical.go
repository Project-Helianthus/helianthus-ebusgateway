package promotioncapture

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

const (
	RawValueDomain       = "HELIANTHUS:LEAF-PROMOTION:RAW-VALUE:V1\x00"
	MappingDomain        = "HELIANTHUS:LEAF-PROMOTION:MAPPING:V1\x00"
	WindowEvidenceDomain = "HELIANTHUS:LEAF-PROMOTION:CAPTURE-WINDOW:V1\x00"
)

func CanonicalJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("canonical JSON: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("canonical JSON: %w", err)
	}
	if err := requireJSONEnd(decoder); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err := appendCanonical(&output, decoded); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func CanonicalDigest(domain string, value any) (string, error) {
	canonical, err := CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte(domain))
	_, _ = digest.Write(canonical)
	return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
}

func HashRawValue(value TypedValue) (string, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	return CanonicalDigest(RawValueDomain, value)
}

func HashWindow(window Window) (string, error) {
	return CanonicalDigest(WindowEvidenceDomain, window)
}

func HashMapping(mapping MappingProfile) (string, error) {
	return CanonicalDigest(MappingDomain, mapping)
}

func (sample *Sample) BindRawHash() error {
	if sample == nil {
		return fmt.Errorf("%w: nil sample", ErrInvalidEvidence)
	}
	digest, err := HashRawValue(sample.RawValue)
	if err != nil {
		return err
	}
	sample.RawHash = digest
	return nil
}

func requireJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("canonical JSON: trailing value")
		}
		return fmt.Errorf("canonical JSON: %w", err)
	}
	return nil
}

func appendCanonical(output *bytes.Buffer, value any) error {
	switch typed := value.(type) {
	case nil:
		output.WriteString("null")
	case bool:
		if typed {
			output.WriteString("true")
		} else {
			output.WriteString("false")
		}
	case string:
		encoded, err := jsonString(typed)
		if err != nil {
			return err
		}
		output.Write(encoded)
	case json.Number:
		raw := typed.String()
		if strings.ContainsAny(raw, ".eE") {
			return fmt.Errorf("canonical JSON: non-integer number %q", raw)
		}
		integer, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || integer < -maximumSafeInteger || integer > maximumSafeInteger {
			return fmt.Errorf("canonical JSON: unsafe integer %q", raw)
		}
		output.WriteString(raw)
	case []any:
		output.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				output.WriteByte(',')
			}
			if err := appendCanonical(output, item); err != nil {
				return err
			}
		}
		output.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		output.WriteByte('{')
		for index, key := range keys {
			if index > 0 {
				output.WriteByte(',')
			}
			encoded, err := jsonString(key)
			if err != nil {
				return err
			}
			output.Write(encoded)
			output.WriteByte(':')
			if err := appendCanonical(output, typed[key]); err != nil {
				return err
			}
		}
		output.WriteByte('}')
	default:
		return fmt.Errorf("canonical JSON: unsupported type %T", value)
	}
	return nil
}

func jsonString(value string) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, fmt.Errorf("canonical JSON string: %w", err)
	}
	return bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), nil
}
