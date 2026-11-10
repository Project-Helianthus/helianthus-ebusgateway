package main

import "strings"

func setFieldFreshness(fields map[semanticFieldKey]semanticFieldFreshness, field semanticFieldKey, source semanticSnapshotSource, stale bool) {
	if fields == nil {
		return
	}
	fields[field] = semanticFieldFreshness{
		Source: source,
		Stale:  stale,
	}
}

func mergeStringField(target *string, incoming string, attempted bool, fields map[semanticFieldKey]semanticFieldFreshness, field semanticFieldKey, source semanticSnapshotSource) {
	if !attempted {
		return
	}
	if strings.TrimSpace(incoming) != "" {
		*target = incoming
		setFieldFreshness(fields, field, source, false)
		return
	}
	markFieldStale(fields, field)
}

func mergeStringSliceField(target *[]string, incoming []string, attempted bool, fields map[semanticFieldKey]semanticFieldFreshness, field semanticFieldKey, source semanticSnapshotSource) {
	if !attempted {
		return
	}
	if len(incoming) > 0 {
		*target = append([]string(nil), incoming...)
		setFieldFreshness(fields, field, source, false)
		return
	}
	markFieldStale(fields, field)
}

func mergeFloatField(target **float64, incoming *float64, attempted bool, fields map[semanticFieldKey]semanticFieldFreshness, field semanticFieldKey, source semanticSnapshotSource) {
	if !attempted {
		return
	}
	if incoming != nil {
		*target = cloneFloat64Ptr(incoming)
		setFieldFreshness(fields, field, source, false)
		return
	}
	markFieldStale(fields, field)
}

func mergeUint16Field(target **uint16, incoming *uint16, attempted bool, fields map[semanticFieldKey]semanticFieldFreshness, field semanticFieldKey, source semanticSnapshotSource) {
	if !attempted {
		return
	}
	if incoming != nil {
		*target = cloneUint16Ptr(incoming)
		setFieldFreshness(fields, field, source, false)
		return
	}
	markFieldStale(fields, field)
}

func mergeZoneSnapshotFields(entry *vaillantZoneSnapshot, incoming *vaillantZoneSnapshot, source semanticSnapshotSource, attempted semanticFieldSet) {
	if entry == nil || incoming == nil {
		return
	}
	fields := ensureFieldFreshnessMap(&entry.FieldFreshness)

	mergeStringField(&entry.Name, incoming.Name, attempted.has(zoneFieldName), fields, zoneFieldName, source)
	mergeStringField(&entry.OperatingMode, incoming.OperatingMode, attempted.has(zoneFieldOperatingMode), fields, zoneFieldOperatingMode, source)
	mergeStringField(&entry.Preset, incoming.Preset, attempted.has(zoneFieldPreset), fields, zoneFieldPreset, source)
	mergeStringField(&entry.HvacAction, incoming.HvacAction, attempted.has(zoneFieldHvacAction), fields, zoneFieldHvacAction, source)
	mergeStringSliceField(&entry.AllowedModes, incoming.AllowedModes, attempted.has(zoneFieldAllowedModes), fields, zoneFieldAllowedModes, source)
	mergeFloatField(&entry.CurrentTempC, incoming.CurrentTempC, attempted.has(zoneFieldCurrentTempC), fields, zoneFieldCurrentTempC, source)
	mergeFloatField(&entry.TargetTempC, incoming.TargetTempC, attempted.has(zoneFieldTargetTempC), fields, zoneFieldTargetTempC, source)
	mergeFloatField(&entry.HumidityPct, incoming.HumidityPct, attempted.has(zoneFieldCurrentHumidityPct), fields, zoneFieldCurrentHumidityPct, source)
	mergeStringField(&entry.ConfigurationHeatingOperationMode, incoming.ConfigurationHeatingOperationMode, attempted.has(zoneFieldZoneOperationModeRaw), fields, zoneFieldZoneOperationModeRaw, source)
	mergeStringField(&entry.StateSpecialFunction, incoming.StateSpecialFunction, attempted.has(zoneFieldSpecialFunctionRaw), fields, zoneFieldSpecialFunctionRaw, source)
	mergeUint16Field(&entry.ConfigurationRoomTemperatureZoneMappingRaw, incoming.ConfigurationRoomTemperatureZoneMappingRaw, attempted.has(zoneFieldRoomTemperatureZoneMappingRaw), fields, zoneFieldRoomTemperatureZoneMappingRaw, source)
	mergeUint16Field(&entry.ConfigurationAssociatedCircuitRaw, incoming.ConfigurationAssociatedCircuitRaw, attempted.has(zoneFieldZoneCircuitIndexRaw), fields, zoneFieldZoneCircuitIndexRaw, source)
	mergeUint16Field(&entry.ConfigurationCircuitTypeRaw, incoming.ConfigurationCircuitTypeRaw, attempted.has(zoneFieldCircuitTypeRaw), fields, zoneFieldCircuitTypeRaw, source)
	mergeUint16Field(&entry.StateValveStatusRaw, incoming.StateValveStatusRaw, attempted.has(zoneFieldZoneValveStatusRaw), fields, zoneFieldZoneValveStatusRaw, source)
	mergeFloatField(&entry.QuickVetoTempC, incoming.QuickVetoTempC, attempted.has(zoneFieldQuickVetoTempC), fields, zoneFieldQuickVetoTempC, source)
	mergeFloatField(&entry.QuickVetoDurationH, incoming.QuickVetoDurationH, attempted.has(zoneFieldQuickVetoDurationH), fields, zoneFieldQuickVetoDurationH, source)
	mergeStringField(&entry.QuickVetoEndTime, incoming.QuickVetoEndTime, attempted.has(zoneFieldQuickVetoEndTime), fields, zoneFieldQuickVetoEndTime, source)
	mergeStringField(&entry.QuickVetoEndDate, incoming.QuickVetoEndDate, attempted.has(zoneFieldQuickVetoEndDate), fields, zoneFieldQuickVetoEndDate, source)
	mergeStringField(&entry.HolidayStartDate, incoming.HolidayStartDate, attempted.has(zoneFieldHolidayStartDate), fields, zoneFieldHolidayStartDate, source)
	mergeStringField(&entry.HolidayEndDate, incoming.HolidayEndDate, attempted.has(zoneFieldHolidayEndDate), fields, zoneFieldHolidayEndDate, source)
	mergeFloatField(&entry.HolidaySetpointC, incoming.HolidaySetpointC, attempted.has(zoneFieldHolidaySetpointC), fields, zoneFieldHolidaySetpointC, source)
	mergeStringField(&entry.HolidayStartTime, incoming.HolidayStartTime, attempted.has(zoneFieldHolidayStartTime), fields, zoneFieldHolidayStartTime, source)
	mergeStringField(&entry.HolidayEndTime, incoming.HolidayEndTime, attempted.has(zoneFieldHolidayEndTime), fields, zoneFieldHolidayEndTime, source)
}

func mergeDhwSnapshotFields(entry *vaillantDhwSnapshot, incoming *vaillantDhwSnapshot, source semanticSnapshotSource, attempted semanticFieldSet) {
	if entry == nil || incoming == nil {
		return
	}
	fields := ensureFieldFreshnessMap(&entry.FieldFreshness)

	mergeStringField(&entry.OperatingMode, incoming.OperatingMode, attempted.has(dhwFieldOperatingMode), fields, dhwFieldOperatingMode, source)
	mergeStringField(&entry.Preset, incoming.Preset, attempted.has(dhwFieldPreset), fields, dhwFieldPreset, source)
	mergeFloatField(&entry.CurrentTempC, incoming.CurrentTempC, attempted.has(dhwFieldCurrentTempC), fields, dhwFieldCurrentTempC, source)
	mergeFloatField(&entry.TargetTempC, incoming.TargetTempC, attempted.has(dhwFieldTargetTempC), fields, dhwFieldTargetTempC, source)
	mergeStringField(&entry.ConfigurationDHWOperationMode, incoming.ConfigurationDHWOperationMode, attempted.has(dhwFieldDhwOperationModeRaw), fields, dhwFieldDhwOperationModeRaw, source)
	mergeStringField(&entry.StateSpecialFunction, incoming.StateSpecialFunction, attempted.has(dhwFieldSpecialFunctionRaw), fields, dhwFieldSpecialFunctionRaw, source)
	mergeStringField(&entry.HolidayStartDate, incoming.HolidayStartDate, attempted.has(dhwFieldHolidayStartDate), fields, dhwFieldHolidayStartDate, source)
	mergeStringField(&entry.HolidayEndDate, incoming.HolidayEndDate, attempted.has(dhwFieldHolidayEndDate), fields, dhwFieldHolidayEndDate, source)
}
