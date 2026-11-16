package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
)

type vaillantSystemSnapshot struct {
	Controller byte

	// State
	SystemOff                    *bool
	SystemWaterPressure          *float64
	SystemFlowTemperature        *float64
	SystemFlowTemperatureLiveAt  time.Time
	OutdoorTemperature           *float64
	OutdoorTemperatureAvg24h     *float64
	MaintenanceDue               *bool
	HwcCylinderTemperatureTop    *float64
	HwcCylinderTemperatureBottom *float64

	// Config
	AdaptiveHeatingCurve         *bool
	AlternativePoint             *float64
	HeatingCircuitBivalencePoint *float64
	DhwBivalencePoint            *float64
	HcEmergencyTemperature       *float64
	HwcMaxFlowTempDesired        *float64
	MaxRoomHumidity              *uint16
	MaintenanceDate              *string
	InstallerName                *string
	InstallerPhone               *string
	InstallerMenuCode            *uint16

	// Properties
	SystemScheme            *uint16
	ModuleConfigurationVR71 *uint16
}

func (p *vaillantSemanticPoller) refreshSystem(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	snapshot := &vaillantSystemSnapshot{Controller: controller}
	updated := false
	configurationAcquired := false

	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_off); ok && raw != nil {
		value := *raw != 0
		snapshot.SystemOff = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_water_pressure); ok {
		snapshot.SystemWaterPressure = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_flow_temperature); ok {
		snapshot.SystemFlowTemperature = &value
		snapshot.SystemFlowTemperatureLiveAt = p.now()
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_outdoor_temperature); ok {
		snapshot.OutdoorTemperature = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_outdoor_temperature_avg_24h); ok {
		snapshot.OutdoorTemperatureAvg24h = &value
		updated = true
	}
	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_maintenance_due); ok && raw != nil {
		value := *raw != 0
		snapshot.MaintenanceDue = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_hwc_cylinder_temperature_top); ok {
		snapshot.HwcCylinderTemperatureTop = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_hwc_cylinder_temperature_bottom); ok {
		snapshot.HwcCylinderTemperatureBottom = &value
		updated = true
	}

	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_adaptive_heating_curve); ok && raw != nil {
		value := *raw != 0
		snapshot.AdaptiveHeatingCurve = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_alternative_point); ok {
		snapshot.AlternativePoint = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_heating_circuit_bivalence_point); ok {
		snapshot.HeatingCircuitBivalencePoint = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_dhw_bivalence_point); ok {
		snapshot.DhwBivalencePoint = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_hc_emergency_temperature); ok {
		snapshot.HcEmergencyTemperature = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_hwc_max_flow_temp_desired); ok {
		snapshot.HwcMaxFlowTempDesired = &value
		updated = true
	}
	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_max_room_humidity); ok && raw != nil {
		snapshot.MaxRoomHumidity = cloneUint16Ptr(raw)
		updated = true
	}

	// Installer/maintenance config (slow-config reads).
	if value, ok := p.readB524DateHDA3(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_maintenance_date); ok {
		snapshot.MaintenanceDate = &value
		updated = true
	}
	{
		// Combined installer name from 2 registers × 6 chars.
		name1, ok1 := p.readB524CStringSanitized(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_installer_name_1)
		name2, ok2 := p.readB524CStringSanitized(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_installer_name_2)
		if ok1 || ok2 {
			var p1, p2 string
			if ok1 {
				p1 = name1
			}
			if ok2 {
				p2 = name2
			}
			combined := strings.TrimRight(p1+p2, " \x00")
			snapshot.InstallerName = &combined
			updated = true
		}
	}
	{
		// Combined installer phone from 2 registers × 6 chars.
		phone1, ok1 := p.readB524CStringSanitized(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_installer_phone_1)
		phone2, ok2 := p.readB524CStringSanitized(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_installer_phone_2)
		if ok1 || ok2 {
			var p1, p2 string
			if ok1 {
				p1 = phone1
			}
			if ok2 {
				p2 = phone2
			}
			combined := strings.TrimRight(p1+p2, " \x00")
			snapshot.InstallerPhone = &combined
			updated = true
		}
	}
	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_installer_menu_code); ok && raw != nil {
		snapshot.InstallerMenuCode = cloneUint16Ptr(raw)
		updated = true
	}

	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_scheme); ok && raw != nil {
		snapshot.SystemScheme = cloneUint16Ptr(raw)
		updated = true
	}
	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_module_configuration_vr71); ok && raw != nil {
		snapshot.ModuleConfigurationVR71 = cloneUint16Ptr(raw)
		updated = true
		configurationAcquired = true
	}

	p.mu.Lock()
	p.updateFM5ConfigurationAcquisitionLocked(configurationAcquired)
	if updated {
		p.updateSystemSnapshotLocked(mergeSystemSnapshotNonDestructive(p.system, snapshot))
	}
	p.mu.Unlock()
	if !updated {
		p.refreshFM5Semantic(ctx)
		return
	}

	p.publishSystem(semanticSnapshotSourceLive)
	p.refreshFM5Semantic(ctx)
}

func (p *vaillantSemanticPoller) updateSystemSnapshotLocked(snapshot *vaillantSystemSnapshot) {
	var previousConfig *uint16
	if p.system != nil {
		previousConfig = p.system.ModuleConfigurationVR71
	}
	var nextConfig *uint16
	if snapshot != nil {
		nextConfig = snapshot.ModuleConfigurationVR71
	}
	p.system = snapshot
	if !uint16PointersEqual(previousConfig, nextConfig) {
		p.fm5EvidenceGeneration++
	}
}

func mergeSystemSnapshotNonDestructive(existing, incoming *vaillantSystemSnapshot) *vaillantSystemSnapshot {
	merged := cloneSystemSnapshot(existing)
	if merged == nil {
		merged = &vaillantSystemSnapshot{}
	}
	if incoming == nil {
		return merged
	}

	controllerChanged := incoming.Controller != 0 && incoming.Controller != merged.Controller
	if controllerChanged && incoming.SystemFlowTemperature == nil {
		merged.SystemFlowTemperature = nil
		merged.SystemFlowTemperatureLiveAt = time.Time{}
	}
	if incoming.Controller != 0 {
		merged.Controller = incoming.Controller
	}
	if incoming.SystemOff != nil {
		merged.SystemOff = cloneBoilerBoolPtr(incoming.SystemOff)
	}
	if incoming.SystemWaterPressure != nil {
		merged.SystemWaterPressure = cloneFloat64Ptr(incoming.SystemWaterPressure)
	}
	if incoming.SystemFlowTemperature != nil {
		merged.SystemFlowTemperature = cloneFloat64Ptr(incoming.SystemFlowTemperature)
		merged.SystemFlowTemperatureLiveAt = incoming.SystemFlowTemperatureLiveAt
	}
	if incoming.OutdoorTemperature != nil {
		merged.OutdoorTemperature = cloneFloat64Ptr(incoming.OutdoorTemperature)
	}
	if incoming.OutdoorTemperatureAvg24h != nil {
		merged.OutdoorTemperatureAvg24h = cloneFloat64Ptr(incoming.OutdoorTemperatureAvg24h)
	}
	if incoming.MaintenanceDue != nil {
		merged.MaintenanceDue = cloneBoilerBoolPtr(incoming.MaintenanceDue)
	}
	if incoming.HwcCylinderTemperatureTop != nil {
		merged.HwcCylinderTemperatureTop = cloneFloat64Ptr(incoming.HwcCylinderTemperatureTop)
	}
	if incoming.HwcCylinderTemperatureBottom != nil {
		merged.HwcCylinderTemperatureBottom = cloneFloat64Ptr(incoming.HwcCylinderTemperatureBottom)
	}
	if incoming.AdaptiveHeatingCurve != nil {
		merged.AdaptiveHeatingCurve = cloneBoilerBoolPtr(incoming.AdaptiveHeatingCurve)
	}
	if incoming.AlternativePoint != nil {
		merged.AlternativePoint = cloneFloat64Ptr(incoming.AlternativePoint)
	}
	if incoming.HeatingCircuitBivalencePoint != nil {
		merged.HeatingCircuitBivalencePoint = cloneFloat64Ptr(incoming.HeatingCircuitBivalencePoint)
	}
	if incoming.DhwBivalencePoint != nil {
		merged.DhwBivalencePoint = cloneFloat64Ptr(incoming.DhwBivalencePoint)
	}
	if incoming.HcEmergencyTemperature != nil {
		merged.HcEmergencyTemperature = cloneFloat64Ptr(incoming.HcEmergencyTemperature)
	}
	if incoming.HwcMaxFlowTempDesired != nil {
		merged.HwcMaxFlowTempDesired = cloneFloat64Ptr(incoming.HwcMaxFlowTempDesired)
	}
	if incoming.MaxRoomHumidity != nil {
		merged.MaxRoomHumidity = cloneUint16Ptr(incoming.MaxRoomHumidity)
	}
	if incoming.MaintenanceDate != nil {
		v := *incoming.MaintenanceDate
		merged.MaintenanceDate = &v
	}
	if incoming.InstallerName != nil {
		v := *incoming.InstallerName
		merged.InstallerName = &v
	}
	if incoming.InstallerPhone != nil {
		v := *incoming.InstallerPhone
		merged.InstallerPhone = &v
	}
	if incoming.InstallerMenuCode != nil {
		merged.InstallerMenuCode = cloneUint16Ptr(incoming.InstallerMenuCode)
	}
	if incoming.SystemScheme != nil {
		merged.SystemScheme = cloneUint16Ptr(incoming.SystemScheme)
	}
	if incoming.ModuleConfigurationVR71 != nil {
		merged.ModuleConfigurationVR71 = cloneUint16Ptr(incoming.ModuleConfigurationVR71)
	}
	return merged
}

func cloneSystemSnapshot(snapshot *vaillantSystemSnapshot) *vaillantSystemSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantSystemSnapshot{
		Controller:                   snapshot.Controller,
		SystemOff:                    cloneBoilerBoolPtr(snapshot.SystemOff),
		SystemWaterPressure:          cloneFloat64Ptr(snapshot.SystemWaterPressure),
		SystemFlowTemperature:        cloneFloat64Ptr(snapshot.SystemFlowTemperature),
		SystemFlowTemperatureLiveAt:  snapshot.SystemFlowTemperatureLiveAt,
		OutdoorTemperature:           cloneFloat64Ptr(snapshot.OutdoorTemperature),
		OutdoorTemperatureAvg24h:     cloneFloat64Ptr(snapshot.OutdoorTemperatureAvg24h),
		MaintenanceDue:               cloneBoilerBoolPtr(snapshot.MaintenanceDue),
		HwcCylinderTemperatureTop:    cloneFloat64Ptr(snapshot.HwcCylinderTemperatureTop),
		HwcCylinderTemperatureBottom: cloneFloat64Ptr(snapshot.HwcCylinderTemperatureBottom),
		AdaptiveHeatingCurve:         cloneBoilerBoolPtr(snapshot.AdaptiveHeatingCurve),
		AlternativePoint:             cloneFloat64Ptr(snapshot.AlternativePoint),
		HeatingCircuitBivalencePoint: cloneFloat64Ptr(snapshot.HeatingCircuitBivalencePoint),
		DhwBivalencePoint:            cloneFloat64Ptr(snapshot.DhwBivalencePoint),
		HcEmergencyTemperature:       cloneFloat64Ptr(snapshot.HcEmergencyTemperature),
		HwcMaxFlowTempDesired:        cloneFloat64Ptr(snapshot.HwcMaxFlowTempDesired),
		MaxRoomHumidity:              cloneUint16Ptr(snapshot.MaxRoomHumidity),
		MaintenanceDate:              cloneStringPtr(snapshot.MaintenanceDate),
		InstallerName:                cloneStringPtr(snapshot.InstallerName),
		InstallerPhone:               cloneStringPtr(snapshot.InstallerPhone),
		InstallerMenuCode:            cloneUint16Ptr(snapshot.InstallerMenuCode),
		SystemScheme:                 cloneUint16Ptr(snapshot.SystemScheme),
		ModuleConfigurationVR71:      cloneUint16Ptr(snapshot.ModuleConfigurationVR71),
	}
}

func uint16ToIntPtr(value *uint16) *int {
	if value == nil {
		return nil
	}
	v := int(*value)
	return &v
}

func uint8ToIntPtr(value *uint8) *int {
	if value == nil {
		return nil
	}
	v := int(*value)
	return &v
}

// cstringPairSpec defines a paired CString field that spans 2 registers.
type cstringPairSpec struct {
	addr1 uint16 // first register (first 6 chars)
	addr2 uint16 // second register (remaining chars)
}

var systemCStringPairSpecs = map[string]cstringPairSpec{
	"installer_name":         {addr1: system_installer_name_1, addr2: system_installer_name_2},
	"installer_phone_number": {addr1: system_installer_phone_1, addr2: system_installer_phone_2},
}

func (p *vaillantSemanticPoller) SetSystemConfig(ctx context.Context, fieldName string, rawValue string) vaillantConfigWriteResult {
	if p == nil {
		return vaillantConfigWriteResult{Success: false, Error: "system config writer unavailable"}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Handle paired CString fields (2 registers × 6 chars).
	if pair, ok := systemCStringPairSpecs[fieldName]; ok {
		return p.writeSystemCStringPair(ctx, fieldName, rawValue, pair)
	}

	// Bound validation consults only identity already held in the registry and
	// completes before any bus I/O. Missing API qualifiers stay unmatched.
	payload, spec, _, err := systemconfig.EncodeSystemConfigValueForController(fieldName, rawValue, p.controllerB524Identity())
	if err != nil {
		return vaillantConfigWriteResult{Success: false, Error: err.Error()}
	}

	return p.writeSystemSingleRegister(ctx, fieldName, rawValue, spec, payload)
}

func (p *vaillantSemanticPoller) writeSystemSingleRegister(ctx context.Context, fieldName, rawValue string, spec systemconfig.ConfigFieldSpec, payload []byte) vaillantConfigWriteResult {
	opcode := systemconfig.OpcodeLocal
	group := spec.Group()
	instance := systemconfig.Instance
	addr := spec.Addr()

	if err := p.writeB524Value(ctx, opcode, group, instance, addr, payload); err != nil {
		return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("b524 write failed: %v", err)}
	}

	readback, ok := p.readB524ValueLive(ctx, opcode, group, instance, addr)
	if !ok {
		return vaillantConfigWriteResult{Success: false, Error: "b524 write confirm failed: read-back unavailable"}
	}
	if err := systemconfig.ConfirmDecodableReadback(spec, readback); err != nil {
		return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("b524 write confirm failed: %v", err)}
	}
	if !systemconfig.ConfigReadbackMatchesWrite(spec, payload, readback) {
		return vaillantConfigWriteResult{Success: false, Error: "b524 write confirm failed: read-back mismatch"}
	}

	normalizedValue := strings.TrimSpace(rawValue)
	p.mu.Lock()
	p.updateSystemSnapshotLocked(systemSnapshotWithConfigValue(p.system, fieldName, normalizedValue))
	p.mu.Unlock()

	p.publishSystem(semanticSnapshotSourceLive)
	return vaillantConfigWriteResult{Success: true}
}

func (p *vaillantSemanticPoller) writeSystemCStringPair(ctx context.Context, fieldName, rawValue string, pair cstringPairSpec) vaillantConfigWriteResult {
	trimmed := strings.TrimSpace(rawValue)

	// Validate characters.
	for i := 0; i < len(trimmed); i++ {
		b := trimmed[i]
		if fieldName == "installer_phone_number" {
			if (b < '0' || b > '9') && b != '+' && b != '(' && b != ')' && b != ' ' {
				return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("invalid character '%c' at position %d (allowed: digits, +, (, ), space)", b, i)}
			}
		} else {
			if b < 0x20 || b > 0x7E {
				return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("invalid byte 0x%02X at position %d", b, i)}
			}
		}
	}
	if len(trimmed) > 12 {
		return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("string length %d exceeds max 12", len(trimmed))}
	}

	// Split into 2 parts of max 6 chars each.
	part1 := trimmed
	part2 := ""
	if len(trimmed) > 6 {
		part1 = trimmed[:6]
		part2 = trimmed[6:]
	}

	// Encode each part: null-padded to 7 bytes (6 chars + terminator).
	encode := func(s string) []byte {
		buf := make([]byte, 7)
		copy(buf, s)
		return buf
	}

	opcode := byte(0x02)
	group := byte(0x00)
	instance := byte(0x00)

	// Write part 1.
	if err := p.writeB524Value(ctx, opcode, group, instance, pair.addr1, encode(part1)); err != nil {
		return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("b524 write part1 failed: %v", err)}
	}
	// Write part 2.
	if err := p.writeB524Value(ctx, opcode, group, instance, pair.addr2, encode(part2)); err != nil {
		return vaillantConfigWriteResult{Success: false, Error: fmt.Sprintf("b524 write part2 failed: %v", err)}
	}

	p.mu.Lock()
	p.updateSystemSnapshotLocked(systemSnapshotWithConfigValue(p.system, fieldName, trimmed))
	p.mu.Unlock()

	p.publishSystem(semanticSnapshotSourceLive)
	return vaillantConfigWriteResult{Success: true}
}

func systemSnapshotWithConfigValue(existing *vaillantSystemSnapshot, fieldName, rawValue string) *vaillantSystemSnapshot {
	snapshot := cloneSystemSnapshot(existing)
	if snapshot == nil {
		snapshot = &vaillantSystemSnapshot{}
	}
	switch fieldName {
	case "maintenance_date":
		snapshot.MaintenanceDate = &rawValue
	case "installer_name":
		snapshot.InstallerName = &rawValue
	case "installer_phone_number":
		snapshot.InstallerPhone = &rawValue
	case "installer_access_code":
		if v, err := strconv.Atoi(rawValue); err == nil {
			u := uint16(v)
			snapshot.InstallerMenuCode = &u
		}
	}
	return snapshot
}
