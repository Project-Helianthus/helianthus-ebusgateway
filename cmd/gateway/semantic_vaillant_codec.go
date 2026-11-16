package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
)

func parseUint16Token(token string) (*uint16, bool) {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return nil, false
	}
	value, err := strconv.ParseUint(trimmed, 10, 16)
	if err != nil {
		return nil, false
	}
	parsed := uint16(value)
	return &parsed, true
}

func decodeUint16Bool(raw *uint16) *bool {
	if raw == nil {
		return nil
	}
	parsed := *raw != 0
	return &parsed
}

func decodeUint32Float64(raw *uint32) *float64 {
	if raw == nil {
		return nil
	}
	parsed := float64(*raw)
	return &parsed
}

func decodeUint32Int(raw *uint32) *int {
	if raw == nil {
		return nil
	}
	parsed := int(*raw)
	return &parsed
}

func decodeUint8Int(raw *uint8) *int {
	if raw == nil {
		return nil
	}
	parsed := int(*raw)
	return &parsed
}

func decodeExactB524Bool(raw []byte) (bool, bool) {
	if len(raw) == 0 {
		return false, false
	}
	switch raw[0] {
	case 0:
		return false, true
	case 1:
		return true, true
	default:
		return false, false
	}
}

func decodeB524BoolFromRaw(raw []byte) *bool {
	if len(raw) == 0 || raw[0] == 0xFF || raw[0] > 1 {
		return nil
	}
	value := raw[0] == 1
	return &value
}

func decodeB524Float32FromRaw(raw []byte) *float64 {
	if len(raw) < 4 {
		return nil
	}
	bits := binary.LittleEndian.Uint32(raw[:4])
	value := float64(math.Float32frombits(bits))
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	return &value
}

func decodeB524Uint32FromRaw(raw []byte) *uint32 {
	if len(raw) < 4 {
		return nil
	}
	value := binary.LittleEndian.Uint32(raw[:4])
	if value == 0xFFFFFFFF {
		return nil
	}
	return &value
}

func boolPtrEquals(a, b *bool) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func stringPtrEquals(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func intPtrEquals(a, b *int) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func decodeB524DateSuppressSentinel(raw []byte) string {
	if len(raw) < 3 {
		return ""
	}
	day, month, year := int(raw[0]), int(raw[1]), 2000+int(raw[2])
	if year == 2015 && month == 1 && day == 1 {
		return ""
	}
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func floatPtrEquals(a, b *float64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func decodeB524FirmwareVersion(raw []byte) *string {
	if len(raw) < 3 {
		return nil
	}
	major, minor, patch := raw[0], raw[1], raw[2]
	if major == 0xFF && minor == 0xFF && patch == 0xFF {
		return nil
	}
	v := fmt.Sprintf("%02d.%02d.%02d", major, minor, patch)
	return &v
}

func hasMeaningfulSpecialFunction(specialFunction *uint16) bool {
	return normalizeSpecialFunctionToken(specialFunction) != ""
}

func derivePresetFromSpecialFunctionAndOpMode(specialFunction, opMode *uint16) string {
	if token := normalizeSpecialFunctionToken(specialFunction); token != "" {
		switch token {
		case "quickveto":
			return "quickveto"
		case "away":
			return "away"
		}
	}
	switch uint16Value(opMode, 1) {
	case 1:
		return "schedule"
	case 2:
		return "manual"
	}
	return ""
}

func normalizeSpecialFunctionToken(specialFunction *uint16) string {
	if specialFunction == nil {
		return ""
	}
	switch *specialFunction {
	case 2:
		return "quickveto"
	case 3, 4:
		return "away"
	default:
		return ""
	}
}

func deriveManualOperatingMode(circuitType *uint16, hasCircuitType bool) string {
	if !hasCircuitType || circuitType == nil {
		return "heat"
	}
	switch *circuitType {
	case 2:
		return "cool"
	default:
		return "heat"
	}
}

func uint16Value(value *uint16, fallback uint16) uint16 {
	if value == nil {
		return fallback
	}
	return *value
}

func formatUintToken(value uint16) string {
	return fmt.Sprintf("%d", value)
}

func decodeB524Uint16(payload []byte) (uint16, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	if len(payload) == 1 {
		return uint16(payload[0]), true
	}
	return binary.LittleEndian.Uint16(payload[:2]), true
}
