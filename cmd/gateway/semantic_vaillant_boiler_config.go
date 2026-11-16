package main

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
)

// vaillantConfigWriteResult is the native outcome of a boiler or system
// configuration write: Success reports a confirmed write and Error carries
// the reason of a refused or failed one. The writer adapters map it to the
// GraphQL and MCP results with the same field names.
type vaillantConfigWriteResult struct {
	Success bool
	Error   string
}

// vaillantBoilerConfigWriter writes one boiler configuration field and
// returns the native result.
type vaillantBoilerConfigWriter interface {
	SetBoilerConfig(ctx context.Context, fieldName string, rawValue string) vaillantConfigWriteResult
}

// vaillantSystemConfigWriter writes one system configuration field and
// returns the native result.
type vaillantSystemConfigWriter interface {
	SetSystemConfig(ctx context.Context, fieldName string, rawValue string) vaillantConfigWriteResult
}

type boilerConfigFieldSpec struct {
	addrs []uint16
	min   float64
	max   float64
	codec boilerConfigCodec
}

type boilerConfigCodec uint8

const (
	boilerConfigCodecTempDATA2c boilerConfigCodec = iota
	boilerConfigCodecUCH
)

var boilerConfigFieldSpecs = map[string]boilerConfigFieldSpec{
	"maximum_heating_flow_temperature_setpoint": {addrs: []uint16{boiler_b509_flowset_hc_max_c, boiler_b509_flowset_hc_max_c_fallback}, min: 20, max: 80, codec: boilerConfigCodecTempDATA2c},
	"maximum_dhw_flow_temperature_setpoint":     {addrs: []uint16{boiler_b509_flowset_hwc_max_c}, min: 30, max: 65, codec: boilerConfigCodecTempDATA2c},
	"heating_partial_load_power":                {addrs: []uint16{boiler_b509_partload_hc_kw}, min: 0, max: 40, codec: boilerConfigCodecUCH},
	"dhw_partial_load_power":                    {addrs: []uint16{boiler_b509_partload_hwc_kw}, min: 0, max: 40, codec: boilerConfigCodecUCH},
}

// Boiler setter keys with a writer of their own codec. The 0x4904 key carries
// the documentation name of its register, which the documentation marks
// provisional; its one-byte integer write is unchanged.
const (
	boilerConfigKeyInstallerPhoneNumber   = "installer_phone_number"
	boilerConfigKeyCentralHeatingPumpMode = "central_heating_pump_mode"
)

func (p *vaillantSemanticPoller) SetBoilerConfig(ctx context.Context, fieldName string, rawValue string) vaillantConfigWriteResult {
	if p == nil {
		return vaillantConfigWriteResult{Success: false, Error: "boiler config writer unavailable"}
	}

	// Handle BCD phone number specially (not float64-based).
	if fieldName == boilerConfigKeyInstallerPhoneNumber {
		return p.writeBoilerPhoneBCD(ctx, rawValue)
	}
	// Handle the 0x4904 register as a one-byte integer (not float64-based).
	if fieldName == boilerConfigKeyCentralHeatingPumpMode {
		return p.writeBoilerInstallerMenuCode(ctx, rawValue)
	}

	spec, ok := boilerConfigFieldSpecs[fieldName]
	if !ok {
		allKeys := make([]string, 0, len(boilerConfigFieldSpecs)+2)
		for key := range boilerConfigFieldSpecs {
			allKeys = append(allKeys, key)
		}
		allKeys = append(allKeys, boilerConfigKeyInstallerPhoneNumber, boilerConfigKeyCentralHeatingPumpMode)
		slices.Sort(allKeys)
		return vaillantConfigWriteResult{
			Success: false,
			Error:   fmt.Sprintf("unknown boiler field %q (allowed: %s)", fieldName, strings.Join(allKeys, ", ")),
		}
	}

	value, err := parseBoilerConfigValue(rawValue, spec)
	if err != nil {
		return vaillantConfigWriteResult{Success: false, Error: err.Error()}
	}

	p.mu.Lock()
	boilerAddress := p.boilerAddress
	p.mu.Unlock()
	if boilerAddress == 0 {
		return vaillantConfigWriteResult{Success: false, Error: "unsupported source: boiler B509 address unavailable"}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	payload, normalizedValue, err := encodeBoilerConfigPayload(fieldName, spec, value)
	if err != nil {
		return vaillantConfigWriteResult{Success: false, Error: err.Error()}
	}
	addr := p.resolveB509BoilerConfigAddr(ctx, boilerAddress, spec)
	if err := p.writeB509Value(ctx, boilerAddress, addr, payload); err != nil {
		return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("b509 write failed: %v", err)}
	}
	p.fenceB509WriteConfirmShadow(boilerAddress, addr)
	readback, ok := p.readB509ValueLive(ctx, boilerAddress, addr)
	if !ok || len(readback) < len(payload) {
		return vaillantConfigWriteResult{Success: false, Error: "b509 write confirm failed: read-back unavailable"}
	}
	if !slices.Equal(readback[:len(payload)], payload) {
		return vaillantConfigWriteResult{Success: false, Error: "b509 write confirm failed: read-back mismatch"}
	}

	p.mu.Lock()
	p.boiler = boilerSnapshotWithConfigValue(p.boiler, fieldName, normalizedValue)
	p.mu.Unlock()

	p.publishBoilerStatus(semanticSnapshotSourceLive)
	return vaillantConfigWriteResult{Success: true}
}

func (p *vaillantSemanticPoller) fenceB509WriteConfirmShadow(target byte, addr uint16) {
	if p == nil || p.shadow == nil || target == 0 {
		return
	}

	key := ebusgateway.NewB509WatchKey(target, addr)
	p.shadow.Invalidate(ebusgateway.ShadowInvalidation{
		Key:           key,
		Reason:        ebusgateway.ShadowInvalidationReasonExternalWrite,
		Source:        ebusgateway.ShadowInvalidationSourceActive,
		InvalidatedAt: p.now(),
	})
}

func (p *vaillantSemanticPoller) writeBoilerPhoneBCD(ctx context.Context, rawValue string) vaillantConfigWriteResult {
	// Strip formatting characters, keep only digits.
	trimmed := strings.TrimSpace(rawValue)
	var digits []byte
	for _, b := range []byte(trimmed) {
		if b >= '0' && b <= '9' {
			digits = append(digits, b)
		}
	}
	if len(digits) > 16 {
		return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("phone number has %d digits (max 16)", len(digits))}
	}

	// Encode digits to BCD (2 digits per byte, pad with 0xF).
	payload := make([]byte, 8)
	for i := range payload {
		payload[i] = 0xFF
	}
	for i := 0; i < len(digits); i += 2 {
		hi := digits[i] - '0'
		var lo byte = 0x0F
		if i+1 < len(digits) {
			lo = digits[i+1] - '0'
		}
		payload[i/2] = (hi << 4) | lo
	}

	p.mu.Lock()
	boilerAddress := p.boilerAddress
	p.mu.Unlock()
	if boilerAddress == 0 {
		return vaillantConfigWriteResult{Success: false, Error: "boiler B509 address unavailable"}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if err := p.writeB509Value(ctx, boilerAddress, boiler_b509_phone_number, payload); err != nil {
		return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("b509 write failed: %v", err)}
	}

	decoded := string(digits)
	p.mu.Lock()
	p.boiler = boilerSnapshotWithStringConfigValue(p.boiler, "installer_phone_number", decoded)
	p.mu.Unlock()

	p.publishBoilerStatus(semanticSnapshotSourceLive)
	return vaillantConfigWriteResult{Success: true}
}

func (p *vaillantSemanticPoller) writeBoilerInstallerMenuCode(ctx context.Context, rawValue string) vaillantConfigWriteResult {
	trimmed := strings.TrimSpace(rawValue)
	value, err := strconv.Atoi(trimmed)
	if err != nil {
		return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("invalid integer %q: %v", rawValue, err)}
	}
	if value < 0 || value > 255 {
		return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("value %d out of range [0, 255]", value)}
	}

	p.mu.Lock()
	boilerAddress := p.boilerAddress
	p.mu.Unlock()
	if boilerAddress == 0 {
		return vaillantConfigWriteResult{Success: false, Error: "boiler B509 address unavailable"}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	payload := []byte{byte(value)}
	if err := p.writeB509Value(ctx, boilerAddress, boiler_b509_installer_menu_code, payload); err != nil {
		return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("b509 write failed: %v", err)}
	}

	p.mu.Lock()
	p.boiler = boilerSnapshotWithIntConfigValue(p.boiler, boilerConfigKeyCentralHeatingPumpMode, value)
	p.mu.Unlock()

	p.publishBoilerStatus(semanticSnapshotSourceLive)
	return vaillantConfigWriteResult{Success: true}
}

func boilerSnapshotWithStringConfigValue(existing *vaillantBoilerSnapshot, fieldName, value string) *vaillantBoilerSnapshot {
	snapshot := cloneBoilerSnapshot(existing)
	if snapshot == nil {
		snapshot = &vaillantBoilerSnapshot{}
	}
	switch fieldName {
	case "installer_phone_number":
		snapshot.PhoneNumber = &value
	}
	return snapshot
}

func boilerSnapshotWithIntConfigValue(existing *vaillantBoilerSnapshot, fieldName string, value int) *vaillantBoilerSnapshot {
	snapshot := cloneBoilerSnapshot(existing)
	if snapshot == nil {
		snapshot = &vaillantBoilerSnapshot{}
	}
	switch fieldName {
	case boilerConfigKeyCentralHeatingPumpMode:
		snapshot.InstallerMenuCode = &value
	}
	return snapshot
}

func parseBoilerConfigValue(rawValue string, spec boilerConfigFieldSpec) (float64, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(rawValue), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid boiler value %q: %v", rawValue, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("invalid boiler value %q: finite number required", rawValue)
	}
	if value < spec.min || value > spec.max {
		return 0, fmt.Errorf("value %.4g out of range [%.4g, %.4g]", value, spec.min, spec.max)
	}
	return value, nil
}

func boilerSnapshotWithConfigValue(existing *vaillantBoilerSnapshot, fieldName string, value float64) *vaillantBoilerSnapshot {
	snapshot := cloneBoilerSnapshot(existing)
	if snapshot == nil {
		snapshot = &vaillantBoilerSnapshot{}
	}

	switch fieldName {
	case "maximum_heating_flow_temperature_setpoint":
		snapshot.FlowsetHcMaxC = cloneFloat64Ptr(&value)
	case "maximum_dhw_flow_temperature_setpoint":
		snapshot.FlowsetHwcMaxC = cloneFloat64Ptr(&value)
	case "heating_partial_load_power":
		snapshot.PartloadHcKW = cloneFloat64Ptr(&value)
	case "dhw_partial_load_power":
		snapshot.PartloadHwcKW = cloneFloat64Ptr(&value)
	}

	return snapshot
}

func encodeBoilerConfigPayload(fieldName string, spec boilerConfigFieldSpec, value float64) ([]byte, float64, error) {
	switch spec.codec {
	case boilerConfigCodecTempDATA2c:
		payload := encodeTempDATA2c(value)
		normalizedValue, ok := decodeDATA2c(payload)
		if !ok {
			return nil, 0, fmt.Errorf("invalid boiler value %.4g for %s: data2c normalization failed", value, fieldName)
		}
		return payload, normalizedValue, nil
	case boilerConfigCodecUCH:
		payload, ok := encodeUCH(value)
		if !ok {
			return nil, 0, fmt.Errorf("invalid boiler value %.4g for %s: whole-number kW required", value, fieldName)
		}
		return payload, float64(payload[0]), nil
	default:
		return nil, 0, fmt.Errorf("unsupported boiler field codec for %s", fieldName)
	}
}

func (p *vaillantSemanticPoller) readB509BoilerConfigFloat(ctx context.Context, target byte, fieldName string) *float64 {
	spec, ok := boilerConfigFieldSpecs[fieldName]
	if !ok {
		return nil
	}
	for _, addr := range spec.addrs {
		raw, ok := p.readB509Value(ctx, target, addr)
		if !ok {
			continue
		}
		value, ok := decodeBoilerConfigRaw(spec, raw)
		if !ok {
			continue
		}
		return &value
	}
	return nil
}

func (p *vaillantSemanticPoller) resolveB509BoilerConfigAddr(ctx context.Context, target byte, spec boilerConfigFieldSpec) uint16 {
	for _, addr := range spec.addrs {
		raw, ok := p.readB509Value(ctx, target, addr)
		if !ok {
			continue
		}
		if _, ok := decodeBoilerConfigRaw(spec, raw); ok {
			return addr
		}
	}
	if len(spec.addrs) == 0 {
		return 0
	}
	return spec.addrs[0]
}

func decodeBoilerConfigRaw(spec boilerConfigFieldSpec, raw []byte) (float64, bool) {
	switch spec.codec {
	case boilerConfigCodecTempDATA2c:
		return decodeDATA2c(raw)
	case boilerConfigCodecUCH:
		value, ok := decodeUCH(raw)
		return float64(value), ok
	default:
		return 0, false
	}
}
