package systemconfig

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
)

const OpcodeLocal = byte(0x02)
const Instance = byte(0x00)

type ValueType int

const (
	ValueFloat32 ValueType = iota
	ValueUint16
	ValueBoolU8
	ValueEnumU16
	ValueCString
	ValueDateHDA3
)

// ConfigFieldSpec describes a B524 register for config mutations.
type ConfigFieldSpec struct {
	RegisterGroup byte
	RegisterAddr  uint16
	ValueType     ValueType
	Min           float64
	Max           float64
	MaxLen        int
	Enum          map[string]uint16
}

var systemConfigFieldSpecs = map[string]ConfigFieldSpec{
	"dhwBivalencePointC":   {RegisterGroup: 0x00, RegisterAddr: 0x0001, ValueType: ValueFloat32, Min: unmatchedDHWBivalenceMinC, Max: unmatchedDHWBivalenceMaxC},
	"maxRoomHumidityPct":   {RegisterGroup: 0x00, RegisterAddr: 0x000E, ValueType: ValueUint16, Min: 30, Max: 80},
	"adaptiveHeatingCurve": {RegisterGroup: 0x00, RegisterAddr: 0x0014, ValueType: ValueBoolU8},
	"hcBivalencePointC":    {RegisterGroup: 0x00, RegisterAddr: 0x0023, ValueType: ValueFloat32, Min: -20.0, Max: 30.0},
	"hcEmergencyTempC":     {RegisterGroup: 0x00, RegisterAddr: 0x0026, ValueType: ValueFloat32, Min: 20.0, Max: 80.0},
	"hwcMaxFlowTempC":      {RegisterGroup: 0x00, RegisterAddr: 0x0046, ValueType: ValueFloat32, Min: 15.0, Max: 80.0},
	"maintenanceDate":      {RegisterGroup: 0x00, RegisterAddr: 0x002C, ValueType: ValueDateHDA3},
	"installerName":        {RegisterGroup: 0x00, RegisterAddr: 0x006C, ValueType: ValueCString, MaxLen: 12},
	"installerPhone":       {RegisterGroup: 0x00, RegisterAddr: 0x006F, ValueType: ValueCString, MaxLen: 12},
	"installerMenuCode":    {RegisterGroup: 0x00, RegisterAddr: 0x0076, ValueType: ValueUint16, Min: 0, Max: 999},
}

func EncodeSystemConfigValue(fieldName, rawValue string) ([]byte, ConfigFieldSpec, error) {
	spec, err := resolveConfigFieldSpec("system", fieldName, systemConfigFieldSpecs)
	if err != nil {
		return nil, ConfigFieldSpec{}, err
	}
	payload, err := EncodeValue(spec, rawValue)
	if err != nil {
		return nil, ConfigFieldSpec{}, err
	}
	return payload, spec, nil
}

func (s ConfigFieldSpec) Group() byte  { return s.RegisterGroup }
func (s ConfigFieldSpec) Addr() uint16 { return s.RegisterAddr }

func SystemConfigFieldSpecs() map[string]ConfigFieldSpec {
	out := make(map[string]ConfigFieldSpec, len(systemConfigFieldSpecs))
	for name, spec := range systemConfigFieldSpecs {
		out[name] = spec
	}
	return out
}
func resolveConfigFieldSpec(scope, fieldName string, specs map[string]ConfigFieldSpec) (ConfigFieldSpec, error) {
	if spec, ok := specs[fieldName]; ok {
		return spec, nil
	}
	allowed := make([]string, 0, len(specs))
	for key := range specs {
		allowed = append(allowed, key)
	}
	sort.Strings(allowed)
	return ConfigFieldSpec{}, fmt.Errorf("unknown %s field %q (allowed: %s): %w", scope, fieldName, strings.Join(allowed, ", "), ebuserrors.ErrInvalidPayload)
}

func EncodeValue(spec ConfigFieldSpec, raw string) ([]byte, error) {
	// Reject non-finite numeric input before range comparisons or conversion:
	// NaN makes both comparisons false and must never become a bus payload.
	if spec.ValueType == ValueFloat32 || spec.ValueType == ValueUint16 {
		if value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil && (math.IsNaN(value) || math.IsInf(value, 0)) {
			return nil, fmt.Errorf("numeric value %q must be finite: %w", raw, ebuserrors.ErrInvalidPayload)
		}
	}
	switch spec.ValueType {
	case ValueFloat32:
		value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float value %q: %w", raw, ebuserrors.ErrInvalidPayload)
		}
		if value < spec.Min || value > spec.Max {
			return nil, fmt.Errorf("value %.4g out of range [%.4g, %.4g]: %w", value, spec.Min, spec.Max, ebuserrors.ErrInvalidPayload)
		}
		payload := make([]byte, 4)
		binary.LittleEndian.PutUint32(payload, math.Float32bits(float32(value)))
		return payload, nil
	case ValueUint16:
		value, err := parseIntegerString(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("invalid integer value %q: %w", raw, ebuserrors.ErrInvalidPayload)
		}
		if float64(value) < spec.Min || float64(value) > spec.Max {
			return nil, fmt.Errorf("value %d out of range [%.0f, %.0f]: %w", value, spec.Min, spec.Max, ebuserrors.ErrInvalidPayload)
		}
		payload := make([]byte, 2)
		binary.LittleEndian.PutUint16(payload, uint16(value))
		return payload, nil
	case ValueBoolU8:
		parsed, ok := parseBoolString(raw)
		if !ok {
			return nil, fmt.Errorf("invalid boolean value %q (expected true/false): %w", raw, ebuserrors.ErrInvalidPayload)
		}
		if parsed {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case ValueEnumU16:
		key := strings.ToLower(strings.TrimSpace(raw))
		if spec.Enum == nil {
			return nil, fmt.Errorf("missing enum mapping: %w", ebuserrors.ErrInvalidPayload)
		}
		mapped, ok := spec.Enum[key]
		if !ok {
			return nil, fmt.Errorf("invalid enum value %q (allowed: %s): %w", raw, strings.Join(sortedEnumKeys(spec.Enum), ", "), ebuserrors.ErrInvalidPayload)
		}
		payload := make([]byte, 2)
		binary.LittleEndian.PutUint16(payload, mapped)
		return payload, nil
	case ValueCString:
		return encodeCStringValue(raw, spec.MaxLen)
	case ValueDateHDA3:
		return encodeDateHDA3Value(raw)
	default:
		return nil, fmt.Errorf("unsupported value encoding: %w", ebuserrors.ErrInvalidPayload)
	}
}

func encodeCStringValue(raw string, maxLen int) ([]byte, error) {
	trimmed := strings.TrimSpace(raw)
	for i := 0; i < len(trimmed); i++ {
		b := trimmed[i]
		if b < 0x20 || b > 0x7E {
			return nil, fmt.Errorf("invalid byte 0x%02X at position %d (only printable ASCII 0x20-0x7E allowed): %w", b, i, ebuserrors.ErrInvalidPayload)
		}
	}
	if maxLen > 0 && len(trimmed) > maxLen {
		return nil, fmt.Errorf("string length %d exceeds maxLen %d: %w", len(trimmed), maxLen, ebuserrors.ErrInvalidPayload)
	}
	padLen := len(trimmed) + 1
	if maxLen > 0 && padLen < maxLen+1 {
		padLen = maxLen + 1
	}
	payload := make([]byte, padLen)
	copy(payload, trimmed)
	return payload, nil
}

func encodeDateHDA3Value(raw string) ([]byte, error) {
	trimmed := strings.TrimSpace(raw)
	t, err := time.Parse("2006-01-02", trimmed)
	if err != nil {
		return nil, fmt.Errorf("invalid date %q (expected YYYY-MM-DD): %w", raw, ebuserrors.ErrInvalidPayload)
	}
	year := t.Year()
	if year < 2000 || year > 2099 {
		return nil, fmt.Errorf("year %d out of range [2000, 2099]: %w", year, ebuserrors.ErrInvalidPayload)
	}
	if trimmed == "2015-01-01" {
		return nil, fmt.Errorf("sentinel date 2015-01-01 rejected: %w", ebuserrors.ErrInvalidPayload)
	}
	return []byte{byte(t.Day()), byte(t.Month()), byte(year - 2000)}, nil
}

func parseBoolString(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "true", "1", "on", "yes":
		return true, true
	case "false", "0", "off", "no":
		return false, true
	default:
		return false, false
	}
}

func parseIntegerString(raw string) (int, error) {
	if raw == "" {
		return 0, fmt.Errorf("empty integer")
	}
	if strings.ContainsAny(raw, ".eE") {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return 0, err
		}
		if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value {
			return 0, fmt.Errorf("not an integer")
		}
		return int(value), nil
	}
	return strconv.Atoi(raw)
}

func sortedEnumKeys(values map[string]uint16) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func ConfirmDecodableReadback(spec ConfigFieldSpec, payload []byte) error {
	switch spec.ValueType {
	case ValueFloat32:
		if len(payload) < 4 {
			return fmt.Errorf("float32 payload too short: %w", ebuserrors.ErrInvalidPayload)
		}
		raw := binary.LittleEndian.Uint32(payload[:4])
		value := float64(math.Float32frombits(raw))
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("float32 payload invalid: %w", ebuserrors.ErrInvalidPayload)
		}
		return nil
	case ValueUint16:
		_, ok := decodePayloadUint16(payload)
		if !ok {
			return fmt.Errorf("uint16 payload invalid: %w", ebuserrors.ErrInvalidPayload)
		}
		return nil
	case ValueBoolU8:
		if len(payload) < 1 {
			return fmt.Errorf("bool payload too short: %w", ebuserrors.ErrInvalidPayload)
		}
		if payload[0] > 1 {
			return fmt.Errorf("bool payload out of range: %w", ebuserrors.ErrInvalidPayload)
		}
		return nil
	case ValueCString:
		return nil
	case ValueDateHDA3:
		if len(payload) < 3 {
			return fmt.Errorf("date payload too short: %w", ebuserrors.ErrInvalidPayload)
		}
		return nil
	case ValueEnumU16:
		value, ok := decodePayloadUint16(payload)
		if !ok {
			return fmt.Errorf("enum payload invalid: %w", ebuserrors.ErrInvalidPayload)
		}
		for _, candidate := range spec.Enum {
			if value == candidate {
				return nil
			}
		}
		return fmt.Errorf("enum payload unknown value %d: %w", value, ebuserrors.ErrInvalidPayload)
	default:
		return fmt.Errorf("unsupported readback decode: %w", ebuserrors.ErrInvalidPayload)
	}
}

func ConfigReadbackMatchesWrite(spec ConfigFieldSpec, written, readback []byte) bool {
	switch spec.ValueType {
	case ValueFloat32:
		if len(written) < 4 || len(readback) < 4 {
			return false
		}
		return binary.LittleEndian.Uint32(readback[:4]) == binary.LittleEndian.Uint32(written[:4])
	case ValueUint16, ValueEnumU16:
		want, okWant := decodePayloadUint16(written)
		got, okGot := decodePayloadUint16(readback)
		return okWant && okGot && want == got
	case ValueBoolU8:
		return len(written) > 0 && len(readback) > 0 && (readback[0] == 0 || readback[0] == 1) && readback[0] == written[0]
	case ValueCString:
		wTrimmed := bytes.TrimRight(written, "\x00")
		rTrimmed := bytes.TrimRight(readback, "\x00")
		return bytes.Equal(wTrimmed, rTrimmed)
	case ValueDateHDA3:
		if len(written) < 3 || len(readback) < 3 {
			return false
		}
		return bytes.Equal(written[:3], readback[:3])
	default:
		return false
	}
}

func decodePayloadUint16(payload []byte) (uint16, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	if len(payload) == 1 {
		return uint16(payload[0]), true
	}
	return binary.LittleEndian.Uint16(payload[:2]), true
}
