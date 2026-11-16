package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

// preloadSemanticCache loads the semantic cache into the provider and
// converts it into the native hydration value for the poller.
func preloadSemanticCache(provider *graphql.LiveSemanticProvider, cacheStore *semanticCacheStore) (vaillantCacheHydration, bool) {
	if provider == nil || cacheStore == nil {
		return vaillantCacheHydration{}, false
	}
	snapshot, ok := cacheStore.Load()
	if !ok {
		return vaillantCacheHydration{}, false
	}
	if len(snapshot.Zones) > 0 {
		provider.SetZonesFromCache(snapshot.Zones)
	}
	if snapshot.DHW != nil {
		provider.SetDHWFromCache(snapshot.DHW)
	}
	if snapshot.Boiler != nil {
		provider.SetBoilerStatusFromCache(snapshot.Boiler)
	}
	return cacheHydrationFromSnapshot(snapshot), true
}

// cacheHydrationFromSnapshot converts a loaded semantic cache snapshot into
// the native hydration value. The hydrated boiler keeps the cached legacy
// return independently of B524 decoding.
func cacheHydrationFromSnapshot(snapshot semanticCacheSnapshot) vaillantCacheHydration {
	hydration := vaillantCacheHydration{persistedAt: snapshot.PersistedAt}
	if len(snapshot.Zones) > 0 {
		hydration.zones = make([]*vaillantZoneSnapshot, 0, len(snapshot.Zones))
	}
	for idx, zone := range snapshot.Zones {
		instance := zoneInstanceFromSemanticID(zone.ID, idx)
		hydration.zones = append(hydration.zones, zoneSnapshotFromSemanticZone(instance, zone))
	}
	if snapshot.DHW != nil {
		hydration.dhw = dhwSnapshotFromSemanticStatus(snapshot.DHW)
	}
	if snapshot.Boiler != nil {
		hydration.boiler = boilerSnapshotFromGraphQL(snapshot.Boiler)
		hydration.boiler.ReturnTemperatureC = cloneFloat64Ptr(snapshot.Boiler.State.ReturnTemperatureC)
	}
	return hydration
}

func zoneInstanceFromSemanticID(id string, fallbackIndex int) byte {
	trimmed := strings.TrimSpace(strings.ToLower(id))
	if strings.HasPrefix(trimmed, "zone-") {
		ordinalRaw := strings.TrimPrefix(trimmed, "zone-")
		if ordinal, err := strconv.Atoi(ordinalRaw); err == nil && ordinal > 0 && ordinal <= 256 {
			return byte(ordinal - 1)
		}
	}
	if fallbackIndex < 0 {
		return 0
	}
	if fallbackIndex > 255 {
		return 255
	}
	return byte(fallbackIndex)
}

func zoneSnapshotFromSemanticZone(instance byte, zone graphql.Zone) *vaillantZoneSnapshot {
	snapshot := &vaillantZoneSnapshot{
		Instance:             instance,
		Present:              true,
		Name:                 zone.Name,
		OperatingMode:        zone.Config.OperatingMode,
		Preset:               zone.Config.Preset,
		HvacAction:           zone.State.HvacAction,
		AllowedModes:         append([]string(nil), zone.Config.AllowedModes...),
		CurrentTempC:         cloneFloat64Ptr(zone.State.CurrentTempC),
		TargetTempC:          cloneFloat64Ptr(zone.Config.TargetTempC),
		HumidityPct:          cloneFloat64Ptr(zone.State.CurrentHumidityPct),
		StateSpecialFunction: zone.State.SpecialFunction,
	}
	if zone.Config.AssociatedCircuit != nil {
		v := uint16(*zone.Config.AssociatedCircuit)
		snapshot.ConfigurationAssociatedCircuitRaw = &v
	}
	if zone.Config.RoomTemperatureZoneMapping != nil {
		v := uint16(*zone.Config.RoomTemperatureZoneMapping)
		snapshot.ConfigurationRoomTemperatureZoneMappingRaw = &v
	}
	if zone.Config.CircuitType != "" {
		v := encodeCircuitTypeRaw(zone.Config.CircuitType)
		snapshot.ConfigurationCircuitTypeRaw = &v
	}
	if zone.State.ValvePositionPct != nil {
		v := uint16(*zone.State.ValvePositionPct * 655.35)
		snapshot.StateValveStatusRaw = &v
	}
	snapshot.QuickVetoTempC = cloneFloat64Ptr(zone.Config.QuickVetoSetpointC)
	snapshot.QuickVetoDurationH = cloneFloat64Ptr(zone.Config.QuickVetoDurationH)
	snapshot.HolidayStartDate = zone.Config.HolidayStartDate
	snapshot.HolidayEndDate = zone.Config.HolidayEndDate
	snapshot.HolidaySetpointC = cloneFloat64Ptr(zone.Config.HolidaySetpointC)
	snapshot.HolidayStartTime = zone.Config.HolidayStartTime
	snapshot.HolidayEndTime = zone.Config.HolidayEndTime
	seedZoneFreshness(snapshot, semanticSnapshotSourceCache, true)
	return snapshot
}

func encodeCircuitTypeRaw(circuitType string) uint16 {
	switch circuitType {
	case "radiator":
		return 0
	case "underfloor":
		return 1
	case "mixed":
		return 2
	default:
		var n uint16
		if _, err := fmt.Sscanf(circuitType, "unknown_%d", &n); err == nil {
			return n
		}
		return 0
	}
}

func dhwSnapshotFromSemanticStatus(status *graphql.DhwStatus) *vaillantDhwSnapshot {
	if status == nil {
		return nil
	}
	snapshot := &vaillantDhwSnapshot{
		OperatingMode:        status.Config.OperatingMode,
		Preset:               status.Config.Preset,
		CurrentTempC:         cloneFloat64Ptr(status.State.CurrentTempC),
		TargetTempC:          cloneFloat64Ptr(status.Config.TargetTempC),
		StateSpecialFunction: status.State.SpecialFunction,
		HolidayStartDate:     status.Config.HolidayStartDate,
		HolidayEndDate:       status.Config.HolidayEndDate,
	}
	seedDhwFreshness(snapshot, semanticSnapshotSourceCache, true)
	return snapshot
}

func boilerSnapshotFromGraphQL(status *graphql.BoilerStatus) *vaillantBoilerSnapshot {
	if status == nil {
		return nil
	}
	return mergeBoilerSnapshotNonDestructive(nil, &vaillantBoilerSnapshot{
		FlowTemperatureC:         cloneFloat64Ptr(status.State.FlowTemperatureC),
		ReturnTemperatureC:       cloneFloat64Ptr(status.State.ReturnTemperatureC),
		CentralHeatingPumpActive: cloneBoilerBoolPtr(status.State.CentralHeatingPumpActive),
		WaterPressureBar:         cloneFloat64Ptr(status.State.WaterPressureBar),
		ExternalPumpActive:       cloneBoilerBoolPtr(status.State.ExternalPumpActive),
		CirculationPumpActive:    cloneBoilerBoolPtr(status.State.CirculationPumpActive),
		GasValveActive:           cloneBoilerBoolPtr(status.State.GasValveActive),
		FlameActive:              cloneBoilerBoolPtr(status.State.FlameActive),
		DiverterValvePositionPct: cloneFloat64Ptr(status.State.DiverterValvePositionPct),
		FanSpeedRpm:              cloneBoilerIntPtr(status.State.FanSpeedRpm),
		TargetFanSpeedRpm:        cloneBoilerIntPtr(status.State.TargetFanSpeedRpm),
		IonisationVoltageUa:      cloneFloat64Ptr(status.State.IonisationVoltageUa),
		DhwWaterFlowLpm:          cloneFloat64Ptr(status.State.DhwWaterFlowLpm),
		DhwDemandActive:          cloneBoilerBoolPtr(status.State.DhwDemandActive),
		HeatingSwitchActive:      cloneBoilerBoolPtr(status.State.HeatingSwitchActive),
		StorageLoadPumpPct:       cloneFloat64Ptr(status.State.StorageLoadPumpPct),
		ModulationPct:            cloneFloat64Ptr(status.State.ModulationPct),
		PrimaryCircuitFlowLpm:    cloneFloat64Ptr(status.State.PrimaryCircuitFlowLpm),
		FlowTempDesiredC:         cloneFloat64Ptr(status.State.FlowTempDesiredC),
		DhwTempDesiredC:          cloneFloat64Ptr(status.State.DhwTempDesiredC),
		StateNumber:              cloneBoilerIntPtr(status.State.StateNumber),
		DhwTemperatureC:          cloneFloat64Ptr(status.State.DhwTemperatureC),
		DhwTargetTemperatureC:    cloneFloat64Ptr(status.State.DhwTargetTemperatureC),
		DhwOperatingMode:         cloneBoilerModeIntPtr(status.Config.DhwOperatingMode),
		FlowsetHcMaxC:            cloneFloat64Ptr(status.Config.FlowsetHcMaxC),
		FlowsetHwcMaxC:           cloneFloat64Ptr(status.Config.FlowsetHwcMaxC),
		PartloadHcKW:             cloneFloat64Ptr(status.Config.PartloadHcKW),
		PartloadHwcKW:            cloneFloat64Ptr(status.Config.PartloadHwcKW),
		HeatingStatusRaw:         cloneBoilerIntPtr(status.Diagnostics.HeatingStatusRaw),
		DhwStatusRaw:             cloneBoilerIntPtr(status.Diagnostics.DhwStatusRaw),
		CentralHeatingHours:      cloneFloat64Ptr(status.Diagnostics.CentralHeatingHours),
		DhwHours:                 cloneFloat64Ptr(status.Diagnostics.DhwHours),
		CentralHeatingStarts:     cloneBoilerIntPtr(status.Diagnostics.CentralHeatingStarts),
		DhwStarts:                cloneBoilerIntPtr(status.Diagnostics.DhwStarts),
		PumpHours:                cloneFloat64Ptr(status.Diagnostics.PumpHours),
		FanHours:                 cloneFloat64Ptr(status.Diagnostics.FanHours),
		DeactivationsIFC:         cloneBoilerIntPtr(status.Diagnostics.DeactivationsIFC),
		DeactivationsTemplimiter: cloneBoilerIntPtr(status.Diagnostics.DeactivationsTemplimiter),
	})
}

// persistsCache reports whether the owner holds a cache persister.
func (o *vaillantSemanticPublication) persistsCache() bool {
	return o.cache != nil
}

// persistCache saves the published zones, with the retained native zone
// values laid over them, and the published hot water and boiler status.
func (o *vaillantSemanticPublication) persistCache(overlays map[byte]vaillantZoneCacheOverlay) {
	_ = o.cache.Save(semanticCacheSnapshot{
		Zones:  cacheZones(o.provider.Zones(), overlays),
		DHW:    o.provider.DHW(),
		Boiler: o.provider.BoilerStatus(),
	})
}

func cacheZones(published []graphql.Zone, overlays map[byte]vaillantZoneCacheOverlay) []graphql.Zone {
	out := make([]graphql.Zone, 0, len(published))
	for idx, zone := range published {
		instance := zoneInstanceFromSemanticID(zone.ID, idx)
		if overlay, ok := overlays[instance]; ok {
			zone.State.CurrentTempC = cloneFloat64Ptr(overlay.currentTempC)
			zone.State.CurrentHumidityPct = cloneFloat64Ptr(overlay.humidityPct)
			zone.Config.RoomTemperatureZoneMapping = decodeRoomTemperatureZoneMapping(overlay.roomMappingRaw)
			zone.Config.AssociatedCircuit = decodeAssociatedCircuit(overlay.associatedCircuitRaw)
		}
		out = append(out, zone)
	}
	return out
}
