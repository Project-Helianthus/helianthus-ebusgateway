package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

func (p *vaillantSemanticPoller) refreshZoneDiscovery(ctx context.Context, startup bool) {
	present := make(map[byte]bool, 4)
	checked := make(map[byte]bool, 9)
	for instance := byte(0x00); instance <= 0x08; instance++ { // II_MAX=0x08 per Vaillant regulator spec
		var indexBytes []byte
		var ok bool
		if startup {
			indexBytes, ok = p.readB524Startup(ctx, localZones.opcode, localZones.group, instance, zone_index)
		} else {
			indexBytes, ok = p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_index)
		}
		if !ok {
			continue
		}
		checked[instance] = true
		if len(indexBytes) < 1 || indexBytes[0] == 0xFF {
			continue
		}
		present[instance] = true
	}

	source := p.reconcileDiscoveryPresence(ctx, checked, present)
	p.publishZones(source)
}

func (p *vaillantSemanticPoller) reconcileDiscoveryPresence(ctx context.Context, checked, present map[byte]bool) semanticSnapshotSource {
	if len(present) == 0 {
		grabPresent, ok := p.tryRefreshFromEbusdGrabWithInstances(ctx)
		if ok {
			if checked == nil {
				checked = make(map[byte]bool, len(grabPresent))
			}
			if present == nil {
				present = make(map[byte]bool, len(grabPresent))
			}
			for instance := range grabPresent {
				checked[instance] = true
				present[instance] = true
			}
			p.applyZonePresenceMissesOnly(checked, present)
			return semanticSnapshotSourceLive
		}
	}
	p.applyZonePresenceProbes(checked, present)
	if len(present) > 0 && p.startupZonesNeedImmediatePresence() {
		p.applyZonePresenceImmediate(checked, present)
		return semanticSnapshotSourceLive
	}
	if len(present) == 0 {
		return semanticSnapshotSourceCache
	}
	return semanticSnapshotSourceLive
}

func (p *vaillantSemanticPoller) startupZonesNeedImmediatePresence() bool {
	if p == nil || p.provider == nil {
		return false
	}
	if len(p.provider.Zones()) > 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.zones) == 0
}

func (p *vaillantSemanticPoller) applyZonePresenceImmediate(checked, present map[byte]bool) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.zones == nil {
		p.zones = make(map[byte]*vaillantZoneSnapshot)
	}
	if p.presence == nil {
		p.presence = make(map[byte]*zonePresenceRecord)
	}

	hitThreshold := p.zoneHitThresholdValue()
	for instance := range checked {
		if !present[instance] {
			p.markZoneMissingLocked(instance)
			continue
		}
		record := p.ensureZonePresenceRecordLocked(instance)
		previousState := record.State
		record.State = zonePresencePresent
		record.HitStreak = hitThreshold
		record.MissStreak = 0
		p.ensureZoneEntryLocked(instance)
		p.recordZonePresenceTransitionLocked(instance, previousState, record.State, record.HitStreak, record.MissStreak)
	}
	semanticZoneCount.Set(int64(len(p.zones)))
}

func (p *vaillantSemanticPoller) applyZonePresenceProbes(checked, present map[byte]bool) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.zones == nil {
		p.zones = make(map[byte]*vaillantZoneSnapshot)
	}
	if p.presence == nil {
		p.presence = make(map[byte]*zonePresenceRecord)
	}

	for instance := range checked {
		if present[instance] {
			p.markZonePresentLocked(instance)
			continue
		}
		p.markZoneMissingLocked(instance)
	}
}

func (p *vaillantSemanticPoller) applyZonePresenceMissesOnly(checked, present map[byte]bool) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.zones == nil {
		p.zones = make(map[byte]*vaillantZoneSnapshot)
	}
	if p.presence == nil {
		p.presence = make(map[byte]*zonePresenceRecord)
	}

	for instance := range checked {
		if present[instance] {
			continue
		}
		p.markZoneMissingLocked(instance)
	}
}

func (p *vaillantSemanticPoller) markZonePresentLocked(instance byte) {
	record := p.ensureZonePresenceRecordLocked(instance)
	previousState := record.State
	record.MissStreak = 0
	hitThreshold := p.zoneHitThresholdValue()

	switch record.State {
	case zonePresencePresent, zonePresenceSuspectMissing:
		record.State = zonePresencePresent
		if record.HitStreak < hitThreshold {
			record.HitStreak = hitThreshold
		}
		p.ensureZoneEntryLocked(instance)
	default:
		record.HitStreak++
		if record.HitStreak >= hitThreshold {
			record.State = zonePresencePresent
			record.HitStreak = hitThreshold
			p.ensureZoneEntryLocked(instance)
		} else {
			record.State = zonePresenceSuspectResurrect
		}
	}
	p.recordZonePresenceTransitionLocked(instance, previousState, record.State, record.HitStreak, record.MissStreak)
	semanticZoneCount.Set(int64(len(p.zones)))
}

func (p *vaillantSemanticPoller) markZoneMissingLocked(instance byte) {
	record := p.ensureZonePresenceRecordLocked(instance)
	previousState := record.State
	record.HitStreak = 0
	missThreshold := p.zoneMissThresholdValue()

	switch record.State {
	case zonePresencePresent, zonePresenceSuspectMissing:
		record.MissStreak++
		if record.MissStreak >= missThreshold {
			record.MissStreak = missThreshold
			record.State = zonePresenceAbsent
			delete(p.zones, instance)
			p.recordZonePresenceTransitionLocked(instance, previousState, record.State, record.HitStreak, record.MissStreak)
			semanticZoneCount.Set(int64(len(p.zones)))
			return
		}
		record.State = zonePresenceSuspectMissing
	default:
		record.MissStreak = 0
		record.State = zonePresenceAbsent
		delete(p.zones, instance)
	}
	p.recordZonePresenceTransitionLocked(instance, previousState, record.State, record.HitStreak, record.MissStreak)
	semanticZoneCount.Set(int64(len(p.zones)))
}

func (p *vaillantSemanticPoller) ensureZonePresenceRecordLocked(instance byte) *zonePresenceRecord {
	if p.presence == nil {
		p.presence = make(map[byte]*zonePresenceRecord)
	}
	record := p.presence[instance]
	if record == nil {
		record = &zonePresenceRecord{State: zonePresenceAbsent}
		p.presence[instance] = record
	}
	return record
}

func (p *vaillantSemanticPoller) ensureZoneEntryLocked(instance byte) {
	entry := p.zones[instance]
	if entry == nil {
		entry = &vaillantZoneSnapshot{Instance: instance}
		p.zones[instance] = entry
	}
	entry.Present = true
}

func (p *vaillantSemanticPoller) zoneMissThresholdValue() int {
	if p == nil || p.zoneMissThreshold <= 0 {
		return ebusgateway.DefaultSemanticZonePresenceMissThreshold
	}
	return p.zoneMissThreshold
}

func (p *vaillantSemanticPoller) zoneHitThresholdValue() int {
	if p == nil || p.zoneHitThreshold <= 0 {
		return ebusgateway.DefaultSemanticZonePresenceHitThreshold
	}
	return p.zoneHitThreshold
}

func (p *vaillantSemanticPoller) refreshConfig(ctx context.Context) {
	controller, zones := p.snapshotZones()
	grabHydrated := false
	if controller == 0 || len(zones) == 0 {
		if p.fallbackDiscoveryAllowed(controller) {
			p.refreshDiscovery(ctx)
			controller, zones = p.snapshotZones()
		} else {
			p.refreshBoilerAddress(ctx)
		}
	}
	if controller == 0 || len(zones) == 0 {
		if p.tryRefreshFromEbusdGrab(ctx) {
			controller, zones = p.snapshotZones()
			grabHydrated = true
		}
	}
	if controller == 0 || len(zones) == 0 {
		if controller != 0 {
			p.publishDHW(p.refreshDHWSlowConfig(ctx))
		}
		return
	}

	zoneLiveReadSuccess := false
	for _, instance := range zones {
		primaryName, primaryOK := p.readB524ZoneNamePart(ctx, instance, zone_name)
		prefix, prefixOK := p.readB524ZoneNamePart(ctx, instance, zone_name_prefix)
		suffix, suffixOK := p.readB524ZoneNamePart(ctx, instance, zone_name_suffix)
		if primaryOK || prefixOK || suffixOK {
			zoneLiveReadSuccess = true
		}

		incoming, slowOK := p.refreshZoneSlowConfigFields(ctx, instance)
		if slowOK {
			zoneLiveReadSuccess = true
		}
		if incoming == nil {
			incoming = &vaillantZoneSnapshot{}
		}
		incoming.Name = composeZoneName(primaryName, prefix, suffix)

		p.mu.Lock()
		if entry := p.zones[instance]; entry != nil {
			mergeZoneSnapshotFields(entry, incoming, semanticSnapshotSourceLive, zoneConfigRefreshFieldSet)
		}
		p.mu.Unlock()
	}

	dhwSource := p.refreshDHWSlowConfig(ctx)
	if !zoneLiveReadSuccess && p.tryRefreshFromEbusdGrab(ctx) {
		grabHydrated = true
	}

	zoneSource := semanticSnapshotSourceCache
	if zoneLiveReadSuccess || grabHydrated {
		zoneSource = semanticSnapshotSourceLive
	}
	p.publishZones(zoneSource)
	p.publishDHW(dhwSource)
}

func (p *vaillantSemanticPoller) refreshZoneSlowConfigFields(ctx context.Context, instance byte) (*vaillantZoneSnapshot, bool) {
	incoming := &vaillantZoneSnapshot{}
	liveReadSuccess := false

	var targetPtr *float64
	if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_target_temp); ok {
		target := value
		targetPtr = &target
		liveReadSuccess = true
	}
	if targetPtr == nil {
		if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_fallback_manual_temp); ok {
			target := value
			targetPtr = &target
			liveReadSuccess = true
		}
	}

	var humidity *float64
	if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_current_humidity); ok {
		currentHumidity := value
		humidity = &currentHumidity
		liveReadSuccess = true
	}

	zoneOpMode, zoneOpModeOK := p.readB524Uint16(ctx, localZones.opcode, localZones.group, instance, zone_heating_op_mode)
	if zoneOpModeOK {
		liveReadSuccess = true
	}

	var qvTempPtr, qvDurPtr *float64
	if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_quick_veto_temp); ok {
		v := value
		qvTempPtr = &v
		liveReadSuccess = true
	}
	if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_quick_veto_duration); ok {
		v := value
		qvDurPtr = &v
		liveReadSuccess = true
	}
	var qvEndTime, qvEndDate string
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_quick_veto_end_time); ok && len(raw) >= 2 {
		qvEndTime = fmt.Sprintf("%02d:%02d", raw[0], raw[1])
		liveReadSuccess = true
	}
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_quick_veto_end_date); ok && len(raw) >= 3 {
		year := 2000 + int(raw[2])
		qvEndDate = fmt.Sprintf("%04d-%02d-%02d", year, raw[1], raw[0])
		liveReadSuccess = true
	}

	var holidayStartDate, holidayEndDate, holidayStartTime, holidayEndTime string
	var holidaySetpointPtr *float64
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_holiday_start_date); ok && len(raw) >= 3 {
		if date := decodeB524DateSuppressSentinel(raw); date != "" {
			holidayStartDate = date
		}
		liveReadSuccess = true
	}
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_holiday_end_date); ok && len(raw) >= 3 {
		if date := decodeB524DateSuppressSentinel(raw); date != "" {
			holidayEndDate = date
		}
		liveReadSuccess = true
	}
	if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_holiday_setpoint); ok {
		v := value
		holidaySetpointPtr = &v
		liveReadSuccess = true
	}
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_holiday_end_time); ok && len(raw) >= 2 {
		holidayEndTime = fmt.Sprintf("%02d:%02d", raw[0], raw[1])
		liveReadSuccess = true
	}
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_holiday_start_time); ok && len(raw) >= 2 {
		holidayStartTime = fmt.Sprintf("%02d:%02d", raw[0], raw[1])
		liveReadSuccess = true
	}

	zoneRoomTemperatureZoneMappingRaw, zoneRoomTemperatureZoneMappingRawOK := p.readB524Uint16(ctx, localZones.opcode, localZones.group, instance, zone_room_temperature_zone_mapping_raw)
	if zoneRoomTemperatureZoneMappingRawOK {
		liveReadSuccess = true
	}
	circuitInstance := resolveAssociatedCircuitInstance(zoneRoomTemperatureZoneMappingRaw, instance)
	circuitType, hasCircuitType := p.readB524Uint16(ctx, localCircuits.opcode, localCircuits.group, circuitInstance, circuit_type)
	if hasCircuitType {
		liveReadSuccess = true
	}
	var associatedCircuitRaw *uint16
	if zoneRoomTemperatureZoneMappingRaw != nil {
		value := uint16(circuitInstance)
		associatedCircuitRaw = &value
	}

	_, cachedZoneSF, _, _ := p.cachedZoneDerivationInputs(instance)
	if zoneOpMode != nil || cachedZoneSF != nil || hasCircuitType {
		operatingMode, preset, allowedModes := deriveZoneModeAndPreset(zoneOpMode, cachedZoneSF, circuitType, hasCircuitType)
		if zoneOpMode != nil {
			incoming.OperatingMode = operatingMode
		}
		if zoneOpMode != nil || cachedZoneSF != nil {
			incoming.Preset = preset
		}
		if hasCircuitType {
			incoming.AllowedModes = allowedModes
		}
	}
	if zoneOpMode != nil {
		incoming.ConfigurationHeatingOperationMode = formatUintToken(*zoneOpMode)
	}

	incoming.TargetTempC = targetPtr
	incoming.HumidityPct = humidity
	incoming.QuickVetoTempC = qvTempPtr
	incoming.QuickVetoDurationH = qvDurPtr
	incoming.QuickVetoEndTime = qvEndTime
	incoming.QuickVetoEndDate = qvEndDate
	incoming.HolidayStartDate = holidayStartDate
	incoming.HolidayEndDate = holidayEndDate
	incoming.HolidaySetpointC = holidaySetpointPtr
	incoming.HolidayStartTime = holidayStartTime
	incoming.HolidayEndTime = holidayEndTime
	incoming.ConfigurationRoomTemperatureZoneMappingRaw = zoneRoomTemperatureZoneMappingRaw
	incoming.ConfigurationAssociatedCircuitRaw = associatedCircuitRaw
	incoming.ConfigurationCircuitTypeRaw = circuitType

	return incoming, liveReadSuccess
}

func (p *vaillantSemanticPoller) cachedZoneDerivationInputs(instance byte) (opMode, specialFunction, circuitType *uint16, hasCircuitType bool) {
	if p == nil {
		return nil, nil, nil, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	circuitInstance := instance
	if zone := p.zones[instance]; zone != nil {
		opMode, _ = parseUint16Token(zone.ConfigurationHeatingOperationMode)
		specialFunction, _ = parseUint16Token(zone.StateSpecialFunction)
		if zone.ConfigurationCircuitTypeRaw != nil {
			return opMode, specialFunction, cloneUint16Ptr(zone.ConfigurationCircuitTypeRaw), true
		}
		if zone.ConfigurationAssociatedCircuitRaw != nil && *zone.ConfigurationAssociatedCircuitRaw <= 0xFF {
			circuitInstance = byte(*zone.ConfigurationAssociatedCircuitRaw)
		}
	}
	if circuit := p.circuits[circuitInstance]; circuit != nil && circuit.CircuitTypeRaw != nil {
		return opMode, specialFunction, cloneUint16Ptr(circuit.CircuitTypeRaw), true
	}
	return opMode, specialFunction, nil, false
}

func (p *vaillantSemanticPoller) refreshState(ctx context.Context) {
	controller, zones := p.snapshotZones()
	grabHydrated := false
	if controller == 0 || len(zones) == 0 {
		if p.fallbackDiscoveryAllowed(controller) {
			p.refreshDiscovery(ctx)
			controller, zones = p.snapshotZones()
		} else {
			p.refreshBoilerAddress(ctx)
		}
	}
	if controller == 0 || len(zones) == 0 {
		if p.tryRefreshFromEbusdGrab(ctx) {
			controller, zones = p.snapshotZones()
			grabHydrated = true
		}
	}
	if controller == 0 || len(zones) == 0 {
		dhwSource := p.refreshDHW(ctx)
		p.publishDHW(dhwSource)
		return
	}

	liveReadSuccess := false
	for _, instance := range zones {
		var currentPtr *float64
		if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_current_temp); ok {
			current := value
			currentPtr = &current
			liveReadSuccess = true
		}

		zoneSF, zoneSFOK := p.readB524Uint16(ctx, localZones.opcode, localZones.group, instance, zone_special_function)
		zoneValve, zoneValveOK := p.readB524Uint16(ctx, localZones.opcode, localZones.group, instance, zone_valve_status)
		if zoneSFOK || zoneValveOK {
			liveReadSuccess = true
		}
		cachedOpMode, cachedZoneSF, circuitType, hasCircuitType := p.cachedZoneDerivationInputs(instance)
		zoneSFForDerive := zoneSF
		if zoneSFForDerive == nil {
			zoneSFForDerive = cachedZoneSF
		}

		allowedModes := deriveZoneAllowedModes(circuitType, hasCircuitType)
		hvacAction := deriveZoneHvacAction(zoneValve, circuitType, hasCircuitType)
		incoming := &vaillantZoneSnapshot{
			HvacAction:          hvacAction,
			CurrentTempC:        currentPtr,
			StateValveStatusRaw: zoneValve,
		}
		if cachedOpMode != nil || hasMeaningfulSpecialFunction(zoneSFForDerive) {
			operatingMode, preset, _ := deriveZoneModeAndPreset(cachedOpMode, zoneSFForDerive, circuitType, hasCircuitType)
			if cachedOpMode != nil {
				incoming.OperatingMode = operatingMode
			}
			incoming.Preset = preset
		}
		if hasCircuitType {
			incoming.AllowedModes = allowedModes
		}
		if zoneSF != nil {
			incoming.StateSpecialFunction = formatUintToken(*zoneSF)
		}

		p.mu.Lock()
		if entry := p.zones[instance]; entry != nil {
			mergeZoneSnapshotFields(entry, incoming, semanticSnapshotSourceLive, zoneFastStateFieldSet)
		}
		p.mu.Unlock()
	}
	if !liveReadSuccess && p.tryRefreshFromEbusdGrab(ctx) {
		grabHydrated = true
	}

	zoneSource := semanticSnapshotSourceCache
	if liveReadSuccess || grabHydrated {
		zoneSource = semanticSnapshotSourceLive
	}

	dhwSource := p.refreshDHW(ctx)
	p.publishZones(zoneSource)
	p.publishDHW(dhwSource)
}

func (p *vaillantSemanticPoller) snapshotZones() (byte, []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	controller := p.controller
	if controller == 0 || len(p.zones) == 0 {
		return controller, nil
	}
	instances := make([]byte, 0, len(p.zones))
	for instance := range p.zones {
		instances = append(instances, instance)
	}
	slices.Sort(instances)
	return controller, instances
}

func (p *vaillantSemanticPoller) mappedRadioRoomStateForZoneLocked(entry *vaillantZoneSnapshot) (*float64, *float64) {
	if p == nil || entry == nil {
		return nil, nil
	}
	mappingRaw := entry.ConfigurationRoomTemperatureZoneMappingRaw
	if mappingRaw == nil {
		mappingRaw = p.inferRadioRoomMappingForZoneLocked(entry.Instance)
	}
	if mappingRaw == nil {
		return nil, nil
	}
	mapping := *mappingRaw
	if mapping == 0 || mapping == 0xFFFF {
		return nil, nil
	}
	if decodeRoomTemperatureZoneMapping(&mapping) == nil {
		return nil, nil
	}

	var snapshot *vaillantRadioDeviceSnapshot
	switch mapping {
	case 1:
		snapshot = p.firstConnectedRadioDeviceLocked(remoteRegulators.group)
	default:
		if mapping > 0x100 {
			return nil, nil
		}
		snapshot = p.radioDevices[radioDeviceKey{Group: remoteThermostats.group, Instance: byte(mapping - 1)}]
	}
	if snapshot == nil || !radioDeviceConnected(snapshot) {
		return nil, nil
	}
	return cloneFloat64Ptr(snapshot.RoomTemperatureC), cloneFloat64Ptr(snapshot.RoomHumidityPct)
}

func (p *vaillantSemanticPoller) inferRadioRoomMappingForZoneLocked(instance byte) *uint16 {
	if p == nil {
		return nil
	}
	zoneNumber := int(instance) + 1
	matches := make([]byte, 0, 1)
	for key, snapshot := range p.radioDevices {
		if key.Group != remoteThermostats.group || snapshot == nil || !radioDeviceConnected(snapshot) || snapshot.ZoneAssignment == nil {
			continue
		}
		if int(*snapshot.ZoneAssignment) != zoneNumber {
			continue
		}
		matches = append(matches, key.Instance)
	}
	if len(matches) != 1 {
		return nil
	}
	mapping := uint16(matches[0]) + 1
	return &mapping
}

func (p *vaillantSemanticPoller) firstConnectedRadioDeviceLocked(group byte) *vaillantRadioDeviceSnapshot {
	if p == nil {
		return nil
	}
	var selected *vaillantRadioDeviceSnapshot
	var selectedInstance byte
	for key, snapshot := range p.radioDevices {
		if key.Group != group || snapshot == nil || !radioDeviceConnected(snapshot) {
			continue
		}
		if selected == nil || key.Instance < selectedInstance {
			selected = snapshot
			selectedInstance = key.Instance
		}
	}
	return selected
}

func radioDeviceConnected(snapshot *vaillantRadioDeviceSnapshot) bool {
	return snapshot != nil && snapshot.DeviceConnected != nil && *snapshot.DeviceConnected
}

func zoneEquals(a, b graphql.Zone) bool {
	if a.ID != b.ID || a.Name != b.Name {
		return false
	}
	// State comparison
	if a.State.HvacAction != b.State.HvacAction || a.State.SpecialFunction != b.State.SpecialFunction {
		return false
	}
	if !floatPtrEquals(a.State.CurrentTempC, b.State.CurrentTempC) {
		return false
	}
	if !floatPtrEquals(a.State.CurrentHumidityPct, b.State.CurrentHumidityPct) {
		return false
	}
	if !floatPtrEquals(a.State.HeatingDemandPct, b.State.HeatingDemandPct) {
		return false
	}
	if !floatPtrEquals(a.State.ValvePositionPct, b.State.ValvePositionPct) {
		return false
	}
	// Config comparison
	if a.Config.OperatingMode != b.Config.OperatingMode || a.Config.Preset != b.Config.Preset {
		return false
	}
	if !floatPtrEquals(a.Config.TargetTempC, b.Config.TargetTempC) {
		return false
	}
	if !slices.Equal(a.Config.AllowedModes, b.Config.AllowedModes) {
		return false
	}
	if a.Config.CircuitType != b.Config.CircuitType {
		return false
	}
	if !intPtrEquals(a.Config.AssociatedCircuit, b.Config.AssociatedCircuit) {
		return false
	}
	if !intPtrEquals(a.Config.RoomTemperatureZoneMapping, b.Config.RoomTemperatureZoneMapping) {
		return false
	}
	if a.Config.QuickVeto != b.Config.QuickVeto || a.Config.QuickVetoExpiry != b.Config.QuickVetoExpiry {
		return false
	}
	if !floatPtrEquals(a.Config.QuickVetoSetpointC, b.Config.QuickVetoSetpointC) {
		return false
	}
	if !floatPtrEquals(a.Config.QuickVetoDurationH, b.Config.QuickVetoDurationH) {
		return false
	}
	if a.Config.HolidayStartDate != b.Config.HolidayStartDate || a.Config.HolidayEndDate != b.Config.HolidayEndDate {
		return false
	}
	if !floatPtrEquals(a.Config.HolidaySetpointC, b.Config.HolidaySetpointC) {
		return false
	}
	if a.Config.HolidayStartTime != b.Config.HolidayStartTime || a.Config.HolidayEndTime != b.Config.HolidayEndTime {
		return false
	}
	return true
}

func (p *vaillantSemanticPoller) recordZonePresenceTransitionLocked(instance byte, previous, next zonePresenceState, hitStreak, missStreak int) {
	if previous == next {
		return
	}
	semanticZonePresenceTransitionsTotal.Add(fmt.Sprintf("%s->%s", previous, next), 1)
	log.Printf(
		"semantic_zone_presence_transition instance=%d from=%s to=%s hit_streak=%d miss_streak=%d zone_count=%d",
		instance,
		previous,
		next,
		hitStreak,
		missStreak,
		len(p.zones),
	)
}

func resolveAssociatedCircuitInstance(roomTemperatureZoneMapping *uint16, zoneInstance byte) byte {
	if roomTemperatureZoneMapping == nil {
		return zoneInstance
	}
	value := *roomTemperatureZoneMapping
	switch value {
	case 0xFF, 0xFFFF, 0:
		return zoneInstance
	default:
		if value >= 1 && value <= 0x20 {
			return byte(value - 1)
		}
		return zoneInstance
	}
}

func deriveZoneModeAndPreset(opMode, specialFunction, circuitType *uint16, hasCircuitType bool) (string, string, []string) {
	allowed := deriveZoneAllowedModes(circuitType, hasCircuitType)
	var mode string
	switch value := uint16Value(opMode, 1); value {
	case 0:
		mode = "off"
	case 1:
		mode = "auto"
	case 2:
		mode = deriveManualOperatingMode(circuitType, hasCircuitType)
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
	return mode, preset, allowed
}

func deriveZoneAllowedModes(circuitType *uint16, hasCircuitType bool) []string {
	if !hasCircuitType || circuitType == nil {
		return []string{"off", "auto", "heat"}
	}
	switch *circuitType {
	case 0:
		return []string{"off"}
	case 2:
		return []string{"off", "auto", "cool"}
	default:
		return []string{"off", "auto", "heat"}
	}
}

func deriveZoneHvacAction(valveStatus, circuitType *uint16, hasCircuitType bool) string {
	if valveStatus == nil {
		return ""
	}
	switch *valveStatus {
	case 0:
		return "idle"
	case 1:
		if hasCircuitType && circuitType != nil && *circuitType == 2 {
			return "cooling"
		}
		return "heating"
	default:
		return ""
	}
}

func composeZoneName(primary, prefix, suffix string) string {
	if primary = strings.TrimSpace(primary); primary != "" {
		return primary
	}
	parts := make([]string, 0, 2)
	if prefix = strings.TrimSpace(prefix); prefix != "" {
		parts = append(parts, prefix)
	}
	if suffix = strings.TrimSpace(suffix); suffix != "" {
		parts = append(parts, suffix)
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}
