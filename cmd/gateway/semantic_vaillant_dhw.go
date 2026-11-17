package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
)

func (p *vaillantSemanticPoller) markDHWUpdatedNowLocked() {
	if p == nil {
		return
	}
	p.dhwLastUpdateAt = p.now()
	semanticDHWUpdatesTotal.Add(1)
	log.Printf("semantic_dhw_update last_update_utc=%s", p.dhwLastUpdateAt.UTC().Format(time.RFC3339))
}

func (p *vaillantSemanticPoller) refreshDHWSlowConfig(ctx context.Context) semanticSnapshotSource {
	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		return semanticSnapshotSourceCache
	}

	attempted := make(semanticFieldSet)
	liveReadSuccess := false
	targetPtr := p.readDhwFloat(ctx, dhw_target_temp)
	if targetPtr != nil {
		liveReadSuccess = true
		attempted[dhwFieldTargetTempC] = struct{}{}
	}
	opModeRaw, opModeOK := p.readDhwUint16(ctx, dhw_operation_mode)
	if opModeOK {
		liveReadSuccess = true
		attempted[dhwFieldOperatingMode] = struct{}{}
		attempted[dhwFieldPreset] = struct{}{}
		attempted[dhwFieldDhwOperationModeRaw] = struct{}{}
	}
	var dhwHolidayStartDate, dhwHolidayEndDate string
	if raw, ok := p.readB524Value(ctx, localDHW.opcode, localDHW.group, dhwInstance, dhw_holiday_start_date); ok && len(raw) >= 3 {
		if date := decodeB524DateSuppressSentinel(raw); date != "" {
			dhwHolidayStartDate = date
		}
		liveReadSuccess = true
		attempted[dhwFieldHolidayStartDate] = struct{}{}
	}
	if raw, ok := p.readB524Value(ctx, localDHW.opcode, localDHW.group, dhwInstance, dhw_holiday_end_date); ok && len(raw) >= 3 {
		if date := decodeB524DateSuppressSentinel(raw); date != "" {
			dhwHolidayEndDate = date
		}
		liveReadSuccess = true
		attempted[dhwFieldHolidayEndDate] = struct{}{}
	}
	if !liveReadSuccess {
		return semanticSnapshotSourceCache
	}

	status := &vaillantDhwSnapshot{
		HolidayStartDate: dhwHolidayStartDate,
		HolidayEndDate:   dhwHolidayEndDate,
		TargetTempC:      targetPtr,
	}
	if opModeRaw != nil {
		_, cachedSpecial := p.cachedDHWDerivationInputs()
		status.OperatingMode, status.Preset = deriveDhwModeAndPreset(opModeRaw, cachedSpecial)
		status.ConfigurationDHWOperationMode = formatUintToken(*opModeRaw)
	}
	p.mu.Lock()
	if p.dhw == nil {
		p.dhw = &vaillantDhwSnapshot{}
	}
	mergeDhwSnapshotFields(p.dhw, status, semanticSnapshotSourceLive, attempted)
	p.markDHWUpdatedNowLocked()
	p.mu.Unlock()
	return semanticSnapshotSourceLive
}

func (p *vaillantSemanticPoller) cachedDHWDerivationInputs() (opMode, specialFunction *uint16) {
	if p == nil {
		return nil, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.dhw == nil {
		return nil, nil
	}
	opMode, _ = parseUint16Token(p.dhw.ConfigurationDHWOperationMode)
	specialFunction, _ = parseUint16Token(p.dhw.StateSpecialFunction)
	return opMode, specialFunction
}

func (p *vaillantSemanticPoller) refreshDHW(ctx context.Context) semanticSnapshotSource {
	controller, _ := p.snapshotZones()
	if controller == 0 {
		return p.sourceFromEbusdGrab(p.refreshDHWFromEbusdGrab(ctx))
	}

	attempted := make(semanticFieldSet)
	liveReadSuccess := false
	currentPtr := p.readDhwFloat(ctx, dhw_current_temp)
	if currentPtr != nil {
		liveReadSuccess = true
		attempted[dhwFieldCurrentTempC] = struct{}{}
	}
	sfModeRaw, sfModeOK := p.readDhwUint16(ctx, dhw_special_function)
	if sfModeOK {
		liveReadSuccess = true
		attempted[dhwFieldOperatingMode] = struct{}{}
		attempted[dhwFieldPreset] = struct{}{}
	}
	if sfModeOK {
		attempted[dhwFieldSpecialFunctionRaw] = struct{}{}
	}

	if !liveReadSuccess {
		return p.sourceFromEbusdGrab(p.refreshDHWFromEbusdGrab(ctx))
	}

	status := &vaillantDhwSnapshot{
		CurrentTempC: currentPtr,
	}
	opModeRaw, cachedSF := p.cachedDHWDerivationInputs()
	if opModeRaw != nil || hasMeaningfulSpecialFunction(sfModeRaw) {
		sfForDerive := sfModeRaw
		if sfForDerive == nil {
			sfForDerive = cachedSF
		}
		operatingMode, preset := deriveDhwModeAndPreset(opModeRaw, sfForDerive)
		if opModeRaw != nil {
			status.OperatingMode = operatingMode
		}
		status.Preset = preset
	}
	if sfModeRaw != nil {
		status.StateSpecialFunction = formatUintToken(*sfModeRaw)
	}

	p.mu.Lock()
	if p.dhw == nil {
		p.dhw = &vaillantDhwSnapshot{}
	}
	mergeDhwSnapshotFields(p.dhw, status, semanticSnapshotSourceLive, attempted)
	p.markDHWUpdatedNowLocked()
	p.mu.Unlock()
	return semanticSnapshotSourceLive
}

func (p *vaillantSemanticPoller) sourceFromEbusdGrab(ok bool) semanticSnapshotSource {
	if ok {
		if p != nil && p.transportConfig.Protocol == ebusgateway.TransportEbusdTCP {
			return semanticSnapshotSourceLive
		}
		return semanticSnapshotSourceCache
	}
	return semanticSnapshotSourceCache
}

// readDhwFloat reads a DHW register as float32 from OP=0x02/GG=0x01 only.
// OP=0x06/GG=0x01 is "Primary Heating Sources", NOT DHW — no fallback.
func (p *vaillantSemanticPoller) readDhwFloat(ctx context.Context, addr uint16) *float64 {
	value, ok := p.readB524Float32LE(ctx, localDHW.opcode, localDHW.group, dhwInstance, addr)
	if !ok {
		return nil
	}
	floatValue := value
	return &floatValue
}

// readDhwUint16 reads a DHW register as uint16 from OP=0x02/GG=0x01 only.
// OP=0x06/GG=0x01 is "Primary Heating Sources", NOT DHW — no fallback.
func (p *vaillantSemanticPoller) readDhwUint16(ctx context.Context, addr uint16) (*uint16, bool) {
	value, ok := p.readB524Uint16(ctx, localDHW.opcode, localDHW.group, dhwInstance, addr)
	if ok {
		return value, true
	}
	return nil, false
}

func (p *vaillantSemanticPoller) expireDHWIfStaleLocked(source semanticSnapshotSource) bool {
	if p == nil || source != semanticSnapshotSourceCache {
		return false
	}
	if p.dhw == nil || p.dhwStaleTTL <= 0 || p.dhwLastUpdateAt.IsZero() {
		return false
	}
	age := p.now().Sub(p.dhwLastUpdateAt)
	if age < p.dhwStaleTTL {
		return false
	}
	semanticDHWStaleExpiryTotal.Add(1)
	log.Printf(
		"semantic_dhw_expired ttl=%s age=%s last_update_utc=%s",
		p.dhwStaleTTL.Round(time.Second),
		age.Round(time.Second),
		p.dhwLastUpdateAt.UTC().Format(time.RFC3339),
	)
	p.dhw = nil
	p.dhwLastUpdateAt = time.Time{}
	return true
}

// dhwViewLocked builds the native hot water view from the retained snapshot.
// The caller holds p.mu and has already applied stale expiry.
func (p *vaillantSemanticPoller) dhwViewLocked(expired bool) vaillantDhwView {
	return vaillantDhwView{snapshot: cloneDhwSnapshot(p.dhw), expired: expired}
}

func hasLiveCylinderEvidence(snapshot *vaillantCylinderSnapshot) bool {
	return snapshot != nil && snapshot.TemperatureC != nil
}

func (p *vaillantSemanticPoller) readSolarSnapshot(ctx context.Context) (*vaillantSolarSnapshot, bool) {
	if p == nil {
		return nil, false
	}
	incoming := &vaillantSolarSnapshot{}
	readAny := false

	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_enabled); ok {
		readAny = true
		incoming.SolarEnabled = decodeB524BoolFromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_function_mode); ok {
		readAny = true
		incoming.FunctionMode = decodeB524BoolFromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_collector_temp); ok {
		readAny = true
		incoming.CollectorTemperatureC = decodeB524Float32FromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_return_temp); ok {
		readAny = true
		incoming.ReturnTemperatureC = decodeB524Float32FromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_pump_active); ok {
		readAny = true
		incoming.PumpActive = decodeB524BoolFromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_current_yield); ok {
		readAny = true
		incoming.CurrentYield = decodeB524Float32FromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_pump_hours); ok {
		readAny = true
		incoming.PumpHours = decodeB524Uint32FromRaw(raw)
	}

	if !readAny {
		return nil, false
	}
	return incoming, true
}

func (p *vaillantSemanticPoller) readCylinderSnapshots(ctx context.Context) (map[byte]*vaillantCylinderSnapshot, bool) {
	if p == nil {
		return nil, false
	}
	out := make(map[byte]*vaillantCylinderSnapshot, 2)
	readAny := false
	for instance := byte(0x00); instance <= 0x01; instance++ {
		incoming := &vaillantCylinderSnapshot{Instance: instance}
		instanceRead := false

		if raw, ok := p.readB524Value(ctx, localCylinders.opcode, localCylinders.group, instance, cylinder_max_setpoint); ok {
			instanceRead = true
			readAny = true
			incoming.MaxSetpointC = decodeB524Float32FromRaw(raw)
		}
		if raw, ok := p.readB524Value(ctx, localCylinders.opcode, localCylinders.group, instance, cylinder_charge_hysteresis); ok {
			instanceRead = true
			readAny = true
			incoming.ChargeHysteresis = decodeB524Float32FromRaw(raw)
		}
		if raw, ok := p.readB524Value(ctx, localCylinders.opcode, localCylinders.group, instance, cylinder_charge_offset); ok {
			instanceRead = true
			readAny = true
			incoming.ChargeOffset = decodeB524Float32FromRaw(raw)
		}
		if raw, ok := p.readB524Value(ctx, localCylinders.opcode, localCylinders.group, instance, cylinder_temperature); ok {
			instanceRead = true
			readAny = true
			incoming.TemperatureC = decodeB524Float32FromRaw(raw)
		}
		if instanceRead {
			out[instance] = incoming
		}
	}
	if !readAny {
		return nil, false
	}
	return out, true
}

func mergeSolarSnapshotNonDestructive(existing, incoming *vaillantSolarSnapshot) *vaillantSolarSnapshot {
	merged := cloneSolarSnapshot(existing)
	if merged == nil {
		merged = &vaillantSolarSnapshot{}
	}
	if incoming == nil {
		return merged
	}
	if incoming.CollectorTemperatureC != nil {
		merged.CollectorTemperatureC = cloneFloat64Ptr(incoming.CollectorTemperatureC)
	}
	if incoming.ReturnTemperatureC != nil {
		merged.ReturnTemperatureC = cloneFloat64Ptr(incoming.ReturnTemperatureC)
	}
	if incoming.PumpActive != nil {
		merged.PumpActive = cloneBoilerBoolPtr(incoming.PumpActive)
	}
	if incoming.CurrentYield != nil {
		merged.CurrentYield = cloneFloat64Ptr(incoming.CurrentYield)
	}
	if incoming.PumpHours != nil {
		merged.PumpHours = cloneUint32Ptr(incoming.PumpHours)
	}
	if incoming.SolarEnabled != nil {
		merged.SolarEnabled = cloneBoilerBoolPtr(incoming.SolarEnabled)
	}
	if incoming.FunctionMode != nil {
		merged.FunctionMode = cloneBoilerBoolPtr(incoming.FunctionMode)
	}
	return merged
}

func cloneSolarSnapshot(snapshot *vaillantSolarSnapshot) *vaillantSolarSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantSolarSnapshot{
		CollectorTemperatureC: cloneFloat64Ptr(snapshot.CollectorTemperatureC),
		ReturnTemperatureC:    cloneFloat64Ptr(snapshot.ReturnTemperatureC),
		PumpActive:            cloneBoilerBoolPtr(snapshot.PumpActive),
		CurrentYield:          cloneFloat64Ptr(snapshot.CurrentYield),
		PumpHours:             cloneUint32Ptr(snapshot.PumpHours),
		SolarEnabled:          cloneBoilerBoolPtr(snapshot.SolarEnabled),
		FunctionMode:          cloneBoilerBoolPtr(snapshot.FunctionMode),
	}
}

func mergeCylinderSnapshotNonDestructive(existing, incoming *vaillantCylinderSnapshot) *vaillantCylinderSnapshot {
	merged := cloneCylinderSnapshot(existing)
	if merged == nil {
		merged = &vaillantCylinderSnapshot{}
	}
	if incoming == nil {
		return merged
	}
	merged.Instance = incoming.Instance
	if incoming.TemperatureC != nil {
		merged.TemperatureC = cloneFloat64Ptr(incoming.TemperatureC)
	}
	if incoming.MaxSetpointC != nil {
		merged.MaxSetpointC = cloneFloat64Ptr(incoming.MaxSetpointC)
	}
	if incoming.ChargeHysteresis != nil {
		merged.ChargeHysteresis = cloneFloat64Ptr(incoming.ChargeHysteresis)
	}
	if incoming.ChargeOffset != nil {
		merged.ChargeOffset = cloneFloat64Ptr(incoming.ChargeOffset)
	}
	return merged
}

func mergeCylinderSnapshotMapNonDestructive(existing, incoming map[byte]*vaillantCylinderSnapshot) map[byte]*vaillantCylinderSnapshot {
	merged := cloneCylinderSnapshotsMap(existing)
	if merged == nil {
		merged = make(map[byte]*vaillantCylinderSnapshot)
	}
	for instance, snapshot := range incoming {
		merged[instance] = mergeCylinderSnapshotNonDestructive(merged[instance], snapshot)
	}
	return merged
}

func cloneCylinderSnapshot(snapshot *vaillantCylinderSnapshot) *vaillantCylinderSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantCylinderSnapshot{
		Instance:         snapshot.Instance,
		TemperatureC:     cloneFloat64Ptr(snapshot.TemperatureC),
		MaxSetpointC:     cloneFloat64Ptr(snapshot.MaxSetpointC),
		ChargeHysteresis: cloneFloat64Ptr(snapshot.ChargeHysteresis),
		ChargeOffset:     cloneFloat64Ptr(snapshot.ChargeOffset),
	}
}

func cloneCylinderSnapshotsMap(source map[byte]*vaillantCylinderSnapshot) map[byte]*vaillantCylinderSnapshot {
	if len(source) == 0 {
		return nil
	}
	out := make(map[byte]*vaillantCylinderSnapshot, len(source))
	for instance, snapshot := range source {
		out[instance] = cloneCylinderSnapshot(snapshot)
	}
	return out
}

func decodeDhwMode(raw int) string {
	switch raw {
	case 0:
		return "OFF"
	case 1:
		return "ON"
	case 2:
		return "AUTO"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", raw)
	}
}

func deriveDhwModeAndPreset(opMode, specialFunction *uint16) (string, string) {
	var mode string
	switch value := uint16Value(opMode, 1); value {
	case 0:
		mode = "off"
	case 1:
		mode = "auto"
	case 2:
		mode = "heat"
	default:
		mode = "auto"
	}
	preset := derivePresetFromSpecialFunctionAndOpMode(specialFunction, opMode)
	if preset == "" {
		switch mode {
		case "auto":
			preset = "schedule"
		default:
			preset = "manual"
		}
	}
	return mode, preset
}
