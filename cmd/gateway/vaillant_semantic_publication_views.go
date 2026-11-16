package main

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

// The publication owner maps the native views that the acquisition side
// builds under its snapshot lock to GraphQL values and pushes them to the
// provider and the broadcast hub. These methods read no poller state and hold
// no poller lock; each returns whether the publication happened, so that the
// poller trigger persists the semantic cache only after a publication.

// publishZoneViews publishes the zones of the given views. A cache
// publication without zones never replaces published zones.
func (o *vaillantSemanticPublication) publishZoneViews(views []vaillantZoneView, source semanticSnapshotSource) bool {
	zones := make([]graphql.Zone, 0, len(views))
	for _, view := range views {
		if view.snapshot == nil {
			continue
		}
		zones = append(zones, zoneFromView(view))
	}

	previous := o.provider.Zones()
	if source == semanticSnapshotSourceCache && len(zones) == 0 && len(previous) > 0 {
		return false
	}
	switch source {
	case semanticSnapshotSourceCache:
		o.provider.SetZonesFromCache(zones)
	default:
		o.provider.SetZones(zones)
	}

	if o.hub != nil {
		prevByID := make(map[string]graphql.Zone, len(previous))
		for _, z := range previous {
			prevByID[z.ID] = z
		}
		for _, zone := range zones {
			if !zoneEquals(prevByID[zone.ID], zone) {
				o.hub.PublishZoneUpdate(zone)
			}
		}
	}
	return true
}

// zoneFromView maps one native zone view to its GraphQL zone. The zone's own
// temperature, humidity and room mapping take precedence over the values
// derived from the radio devices.
func zoneFromView(view vaillantZoneView) graphql.Zone {
	entry := view.snapshot
	name := entry.Name
	if strings.TrimSpace(name) == "" {
		name = fmt.Sprintf("Zone %d", view.instance+1)
	}

	quickVetoActive := entry.Preset == "quickveto"
	var qvExpiry string
	if quickVetoActive && entry.QuickVetoEndDate != "" && entry.QuickVetoEndTime != "" {
		qvExpiry = entry.QuickVetoEndDate + "T" + entry.QuickVetoEndTime
	}
	currentTempC := entry.CurrentTempC
	if currentTempC == nil {
		currentTempC = view.radioRoomTempC
	}
	currentHumidityPct := entry.HumidityPct
	if currentHumidityPct == nil {
		currentHumidityPct = view.radioRoomHumidityPct
	}
	roomMappingRaw := entry.ConfigurationRoomTemperatureZoneMappingRaw
	if roomMappingRaw == nil {
		roomMappingRaw = view.inferredRoomMappingRaw
	}
	associatedCircuitRaw := entry.ConfigurationAssociatedCircuitRaw
	if associatedCircuitRaw == nil && roomMappingRaw != nil {
		value := uint16(resolveAssociatedCircuitInstance(roomMappingRaw, entry.Instance))
		associatedCircuitRaw = &value
	}
	return graphql.Zone{
		ID:   fmt.Sprintf("zone-%d", view.instance+1),
		Name: name,
		State: graphql.ZoneState{
			CurrentTempC:       currentTempC,
			CurrentHumidityPct: currentHumidityPct,
			HvacAction:         entry.HvacAction,
			SpecialFunction:    entry.StateSpecialFunction,
			ValvePositionPct:   decodeValvePositionPct(entry.StateValveStatusRaw),
		},
		Config: graphql.ZoneConfig{
			OperatingMode:              entry.OperatingMode,
			Preset:                     entry.Preset,
			TargetTempC:                entry.TargetTempC,
			AllowedModes:               append([]string(nil), entry.AllowedModes...),
			CircuitType:                decodeCircuitType(entry.ConfigurationCircuitTypeRaw),
			AssociatedCircuit:          decodeAssociatedCircuit(associatedCircuitRaw),
			RoomTemperatureZoneMapping: decodeRoomTemperatureZoneMapping(roomMappingRaw),
			QuickVeto:                  quickVetoActive,
			QuickVetoSetpointC:         entry.QuickVetoTempC,
			QuickVetoDurationH:         entry.QuickVetoDurationH,
			QuickVetoExpiry:            qvExpiry,
			HolidayStartDate:           entry.HolidayStartDate,
			HolidayEndDate:             entry.HolidayEndDate,
			HolidaySetpointC:           entry.HolidaySetpointC,
			HolidayStartTime:           entry.HolidayStartTime,
			HolidayEndTime:             entry.HolidayEndTime,
		},
	}
}

// publishDhwView publishes hot water from its native view. Without a
// snapshot it withdraws the published status, except for a cache
// publication over a published status that was not expired.
func (o *vaillantSemanticPublication) publishDhwView(view vaillantDhwView, source semanticSnapshotSource) bool {
	previous := o.provider.DHW()
	if view.snapshot == nil {
		if source == semanticSnapshotSourceCache && previous != nil && !view.expired {
			return false
		}
		switch source {
		case semanticSnapshotSourceCache:
			o.provider.SetDHWFromCache(nil)
		default:
			o.provider.SetDHW(nil)
		}
		if previous != nil && o.hub != nil {
			o.hub.PublishDHWUpdate(nil)
		}
		return true
	}

	current := dhwStatusFromSnapshot(view.snapshot)
	switch source {
	case semanticSnapshotSourceCache:
		o.provider.SetDHWFromCache(current)
	default:
		o.provider.SetDHW(current)
	}
	if o.hub != nil && !dhwEquals(previous, current) {
		o.hub.PublishDHWUpdate(current)
	}
	return true
}

func dhwStatusFromSnapshot(snapshot *vaillantDhwSnapshot) *graphql.DhwStatus {
	return &graphql.DhwStatus{
		State: graphql.DhwState{
			CurrentTempC:    snapshot.CurrentTempC,
			SpecialFunction: snapshot.StateSpecialFunction,
		},
		Config: graphql.DhwConfig{
			OperatingMode:    snapshot.OperatingMode,
			Preset:           snapshot.Preset,
			TargetTempC:      snapshot.TargetTempC,
			HolidayStartDate: snapshot.HolidayStartDate,
			HolidayEndDate:   snapshot.HolidayEndDate,
		},
	}
}

// publishCircuitsView publishes the active circuits of the native view. It
// publishes nothing while neither circuits nor a published list exist, and a
// cache publication without circuits never replaces published circuits.
func (o *vaillantSemanticPublication) publishCircuitsView(view vaillantCircuitsView, source semanticSnapshotSource) bool {
	out := make([]graphql.CircuitStatus, 0, len(view.circuits))
	for _, snapshot := range view.circuits {
		if snapshot == nil {
			continue
		}
		out = append(out, circuitStatusFromSnapshot(snapshot, view.managedByFunctionModule))
	}

	previous := o.provider.Circuits()
	if len(out) == 0 && previous == nil {
		return false
	}
	if source == semanticSnapshotSourceCache && len(out) == 0 && len(previous) > 0 {
		return false
	}
	switch source {
	case semanticSnapshotSourceCache:
		o.provider.SetCircuitsFromCache(out)
	default:
		o.provider.SetCircuits(out)
	}
	if o.hub != nil && !circuitsEqual(previous, out) {
		publisher := reflect.ValueOf(o.hub).MethodByName("PublishCircuitsUpdate")
		if publisher.IsValid() && publisher.Type().NumIn() == 1 {
			arg := reflect.ValueOf(out)
			paramType := publisher.Type().In(0)
			switch {
			case arg.Type().AssignableTo(paramType):
				publisher.Call([]reflect.Value{arg})
			case arg.Type().ConvertibleTo(paramType):
				publisher.Call([]reflect.Value{arg.Convert(paramType)})
			}
		}
	}
	return true
}

func circuitStatusFromSnapshot(snapshot *vaillantCircuitSnapshot, managedByFunctionModule bool) graphql.CircuitStatus {
	return graphql.CircuitStatus{
		Index:       int(snapshot.Instance),
		CircuitType: decodeHeatingCircuitTypeToken(snapshot.CircuitTypeRaw),
		HasMixer:    snapshot.MixerPositionPct != nil,
		State: graphql.CircuitState{
			PumpActive:       decodeUint16Bool(snapshot.PumpStatusRaw),
			MixerPositionPct: cloneFloat64Ptr(snapshot.MixerPositionPct),
			FlowTemperatureC: cloneFloat64Ptr(snapshot.FlowTemperatureC),
			FlowSetpointC:    cloneFloat64Ptr(snapshot.FlowSetpointC),
			CalcFlowTempC:    cloneFloat64Ptr(snapshot.CalcFlowTempC),
			CircuitState:     decodeCircuitStateToken(snapshot.CircuitStateRaw),
			Humidity:         cloneFloat64Ptr(snapshot.HumidityPct),
			DewPoint:         cloneFloat64Ptr(snapshot.DewPointC),
			PumpHours:        decodeUint32Float64(snapshot.PumpHoursRaw),
			PumpStarts:       decodeUint32Int(snapshot.PumpStartsRaw),
		},
		Config: graphql.CircuitConfig{
			HeatingCurve:        cloneFloat64Ptr(snapshot.HeatingCurve),
			FlowTempMaxC:        cloneFloat64Ptr(snapshot.FlowTempMaxC),
			FlowTempMinC:        cloneFloat64Ptr(snapshot.FlowTempMinC),
			SummerLimitC:        cloneFloat64Ptr(snapshot.SummerLimitC),
			SpecialFunctionMode: decodeUint8Int(snapshot.SpecialFunctionMode),
			RoomTempControl:     decodeRoomTempControlToken(snapshot.RoomTempControlRaw),
			CoolingEnabled:      decodeUint16Bool(snapshot.CoolingEnabledRaw),
		},
		ManagingDevice: circuitManagingDevice(managedByFunctionModule),
	}
}

// publishRadioDeviceViews publishes the radio devices of the given native
// views. A cache publication without devices never replaces published
// devices.
func (o *vaillantSemanticPublication) publishRadioDeviceViews(views []*vaillantRadioDeviceSnapshot, source semanticSnapshotSource) bool {
	out := make([]graphql.RadioDevice, 0, len(views))
	for _, snapshot := range views {
		if snapshot == nil {
			continue
		}
		out = append(out, radioDeviceFromSnapshot(snapshot))
	}

	previous := o.provider.RadioDevices()
	if source == semanticSnapshotSourceCache && len(out) == 0 && len(previous) > 0 {
		return false
	}
	switch source {
	case semanticSnapshotSourceCache:
		o.provider.SetRadioDevicesFromCache(out)
	default:
		o.provider.SetRadioDevices(out)
	}
	if o.hub != nil && !radioDevicesEqual(previous, out) {
		o.hub.PublishRadioDevicesUpdate(out)
	}
	return true
}

func radioDeviceFromSnapshot(snapshot *vaillantRadioDeviceSnapshot) graphql.RadioDevice {
	return graphql.RadioDevice{
		Group:                int(snapshot.Group),
		Instance:             int(snapshot.Instance),
		SlotMode:             snapshot.SlotMode,
		DeviceConnected:      cloneBoilerBoolPtr(snapshot.DeviceConnected),
		DeviceClassAddress:   uint8ToIntPtr(snapshot.DeviceClassAddress),
		DeviceModel:          snapshot.DeviceModel,
		FirmwareVersion:      cloneStringPtr(snapshot.FirmwareVersion),
		HardwareIdentifier:   uint16ToIntPtr(snapshot.HardwareIdentifier),
		RemoteControlAddress: uint8ToIntPtr(snapshot.RemoteControlAddress),
		DevicePaired:         cloneBoilerBoolPtr(snapshot.DevicePaired),
		ReceptionStrength:    uint8ToIntPtr(snapshot.ReceptionStrength),
		ZoneAssignment:       uint8ToIntPtr(snapshot.ZoneAssignment),
		RoomTemperatureC:     cloneFloat64Ptr(snapshot.RoomTemperatureC),
		RoomHumidityPct:      cloneFloat64Ptr(snapshot.RoomHumidityPct),
	}
}

// publishRegulatorCapability publishes the native controller capability as
// the GraphQL regulator capability; an unrecognised capability publishes as
// unknown.
func (o *vaillantSemanticPublication) publishRegulatorCapability(capability productids.ControllerCapability) {
	if o == nil || o.provider == nil {
		return
	}
	switch capability {
	case productids.ControllerPresent:
		o.provider.SetVaillantRegulatorCapability(graphql.VaillantRegulatorCapabilityPresent)
	case productids.ControllerNone:
		o.provider.SetVaillantRegulatorCapability(graphql.VaillantRegulatorCapabilityNone)
	default:
		o.provider.SetVaillantRegulatorCapability(graphql.VaillantRegulatorCapabilityUnknown)
	}
}

// publishBoilerStatusView publishes the boiler status from a detached native
// boiler snapshot. Without a snapshot it withdraws a published status and
// reports no publication. The caller holds boilerPublishMu.
func (o *vaillantSemanticPublication) publishBoilerStatusView(snapshot *vaillantBoilerSnapshot, source semanticSnapshotSource) bool {
	previous := o.provider.BoilerStatus()
	if snapshot == nil {
		if previous != nil {
			switch source {
			case semanticSnapshotSourceCache:
				o.provider.SetBoilerStatusFromCache(nil)
			default:
				o.provider.SetBoilerStatus(nil)
			}
		}
		return false
	}

	current := boilerStatusFromSnapshot(snapshot)
	switch source {
	case semanticSnapshotSourceCache:
		o.provider.SetBoilerStatusFromCache(current)
	default:
		o.provider.SetBoilerStatus(current)
	}
	if o.hub != nil && !boilerStatusEquals(previous, current) {
		o.hub.PublishBoilerStatusUpdate(current)
	}
	return true
}

// withdrawBoilerFlowTemperature withdraws the flow temperature of the
// published boiler status and pushes the changed status to the hub; it
// changes nothing while no flow temperature is published. The caller holds
// boilerPublishMu and has cleared the native flow temperature.
func (o *vaillantSemanticPublication) withdrawBoilerFlowTemperature() {
	if o == nil || o.provider == nil {
		return
	}
	status := o.provider.BoilerStatus()
	if status == nil || status.State.FlowTemperatureC == nil {
		return
	}
	status.State.FlowTemperatureC = nil
	o.provider.SetBoilerStatus(status)
	if o.hub != nil {
		o.hub.PublishBoilerStatusUpdate(status)
	}
}

func boilerStatusFromSnapshot(snapshot *vaillantBoilerSnapshot) *graphql.BoilerStatus {
	current := &graphql.BoilerStatus{
		State: graphql.BoilerState{
			FlowTemperatureC:         snapshot.FlowTemperatureC,
			ReturnTemperatureC:       snapshot.ReturnTemperatureC,
			CentralHeatingPumpActive: snapshot.CentralHeatingPumpActive,
			WaterPressureBar:         snapshot.WaterPressureBar,
			ExternalPumpActive:       snapshot.ExternalPumpActive,
			CirculationPumpActive:    snapshot.CirculationPumpActive,
			GasValveActive:           snapshot.GasValveActive,
			FlameActive:              snapshot.FlameActive,
			DiverterValvePositionPct: snapshot.DiverterValvePositionPct,
			FanSpeedRpm:              snapshot.FanSpeedRpm,
			TargetFanSpeedRpm:        snapshot.TargetFanSpeedRpm,
			IonisationVoltageUa:      snapshot.IonisationVoltageUa,
			DhwWaterFlowLpm:          snapshot.DhwWaterFlowLpm,
			DhwDemandActive:          snapshot.DhwDemandActive,
			HeatingSwitchActive:      snapshot.HeatingSwitchActive,
			StorageLoadPumpPct:       snapshot.StorageLoadPumpPct,
			ModulationPct:            snapshot.ModulationPct,
			PrimaryCircuitFlowLpm:    snapshot.PrimaryCircuitFlowLpm,
			FlowTempDesiredC:         snapshot.FlowTempDesiredC,
			DhwTempDesiredC:          snapshot.DhwTempDesiredC,
			StateNumber:              snapshot.StateNumber,
			DhwTemperatureC:          snapshot.DhwTemperatureC,
			DhwTargetTemperatureC:    snapshot.DhwTargetTemperatureC,
		},
		Config: graphql.BoilerConfig{
			FlowsetHcMaxC:     snapshot.FlowsetHcMaxC,
			FlowsetHwcMaxC:    snapshot.FlowsetHwcMaxC,
			PartloadHcKW:      snapshot.PartloadHcKW,
			PartloadHwcKW:     snapshot.PartloadHwcKW,
			InstallerMenuCode: snapshot.InstallerMenuCode,
			PhoneNumber:       cloneStringPtr(snapshot.PhoneNumber),
			HoursTillService:  snapshot.HoursTillService,
		},
		Diagnostics: graphql.BoilerDiagnostics{
			HeatingStatusRaw:         snapshot.HeatingStatusRaw,
			DhwStatusRaw:             snapshot.DhwStatusRaw,
			CentralHeatingHours:      snapshot.CentralHeatingHours,
			DhwHours:                 snapshot.DhwHours,
			CentralHeatingStarts:     snapshot.CentralHeatingStarts,
			DhwStarts:                snapshot.DhwStarts,
			PumpHours:                snapshot.PumpHours,
			FanHours:                 snapshot.FanHours,
			DeactivationsIFC:         snapshot.DeactivationsIFC,
			DeactivationsTemplimiter: snapshot.DeactivationsTemplimiter,
		},
	}
	if snapshot.DhwOperatingMode != nil {
		mode := decodeDhwMode(*snapshot.DhwOperatingMode)
		current.Config.DhwOperatingMode = &mode
	}
	return current
}

func boilerStatusEquals(a, b *graphql.BoilerStatus) bool {
	return reflect.DeepEqual(a, b)
}

// publishSystemView publishes the system status from its native view.
// Without a snapshot it withdraws the published status and reports no
// publication.
func (o *vaillantSemanticPublication) publishSystemView(view vaillantSystemView, source semanticSnapshotSource) bool {
	previous := o.provider.System()
	if view.snapshot == nil {
		switch source {
		case semanticSnapshotSourceCache:
			o.provider.SetSystemFromCache(nil)
		default:
			o.provider.SetSystem(nil)
		}
		return false
	}

	current := systemStatusFromView(view)
	switch source {
	case semanticSnapshotSourceCache:
		o.provider.SetSystemFromCache(current)
	default:
		o.provider.SetSystem(current)
	}

	if o.hub != nil && !systemStatusEquals(previous, current) {
		publisher := reflect.ValueOf(o.hub).MethodByName("PublishSystemUpdate")
		if publisher.IsValid() && publisher.Type().NumIn() == 1 {
			arg := reflect.ValueOf(current)
			paramType := publisher.Type().In(0)
			switch {
			case arg.Type().AssignableTo(paramType):
				publisher.Call([]reflect.Value{arg})
			case arg.Type().ConvertibleTo(paramType):
				publisher.Call([]reflect.Value{arg.Convert(paramType)})
			}
		}
	}
	return true
}

func systemStatusEquals(a, b *graphql.SystemStatus) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return boolPtrEquals(a.State.SystemOff, b.State.SystemOff) &&
		floatPtrEquals(a.State.SystemWaterPressure, b.State.SystemWaterPressure) &&
		floatPtrEquals(a.State.SystemFlowTemperature, b.State.SystemFlowTemperature) &&
		floatPtrEquals(a.State.OutdoorTemperature, b.State.OutdoorTemperature) &&
		floatPtrEquals(a.State.OutdoorTemperatureAvg24h, b.State.OutdoorTemperatureAvg24h) &&
		boolPtrEquals(a.State.MaintenanceDue, b.State.MaintenanceDue) &&
		floatPtrEquals(a.State.HwcCylinderTemperatureTop, b.State.HwcCylinderTemperatureTop) &&
		floatPtrEquals(a.State.HwcCylinderTemperatureBottom, b.State.HwcCylinderTemperatureBottom) &&
		boolPtrEquals(a.Config.AdaptiveHeatingCurve, b.Config.AdaptiveHeatingCurve) &&
		floatPtrEquals(a.Config.AlternativePoint, b.Config.AlternativePoint) &&
		floatPtrEquals(a.Config.HeatingCircuitBivalencePoint, b.Config.HeatingCircuitBivalencePoint) &&
		floatPtrEquals(a.Config.DhwBivalencePoint, b.Config.DhwBivalencePoint) &&
		configBoundEquals(a.Config.DhwBivalenceBound, b.Config.DhwBivalenceBound) &&
		floatPtrEquals(a.Config.HcEmergencyTemperature, b.Config.HcEmergencyTemperature) &&
		floatPtrEquals(a.Config.HwcMaxFlowTempDesired, b.Config.HwcMaxFlowTempDesired) &&
		intPtrEquals(a.Config.MaxRoomHumidity, b.Config.MaxRoomHumidity) &&
		stringPtrEquals(a.Config.MaintenanceDate, b.Config.MaintenanceDate) &&
		stringPtrEquals(a.Config.InstallerName, b.Config.InstallerName) &&
		stringPtrEquals(a.Config.InstallerPhone, b.Config.InstallerPhone) &&
		intPtrEquals(a.Config.InstallerMenuCode, b.Config.InstallerMenuCode) &&
		intPtrEquals(a.Properties.SystemScheme, b.Properties.SystemScheme) &&
		intPtrEquals(a.Properties.ModuleConfigurationVR71, b.Properties.ModuleConfigurationVR71)
}

func systemStatusFromView(view vaillantSystemView) *graphql.SystemStatus {
	snapshot := view.snapshot
	dhwBound := view.dhwBivalenceBound
	return &graphql.SystemStatus{
		State: graphql.SystemState{
			SystemOff:                    cloneBoilerBoolPtr(snapshot.SystemOff),
			SystemWaterPressure:          cloneFloat64Ptr(snapshot.SystemWaterPressure),
			SystemFlowTemperature:        cloneFloat64Ptr(snapshot.SystemFlowTemperature),
			OutdoorTemperature:           cloneFloat64Ptr(snapshot.OutdoorTemperature),
			OutdoorTemperatureAvg24h:     cloneFloat64Ptr(snapshot.OutdoorTemperatureAvg24h),
			MaintenanceDue:               cloneBoilerBoolPtr(snapshot.MaintenanceDue),
			HwcCylinderTemperatureTop:    cloneFloat64Ptr(snapshot.HwcCylinderTemperatureTop),
			HwcCylinderTemperatureBottom: cloneFloat64Ptr(snapshot.HwcCylinderTemperatureBottom),
		},
		Config: graphql.SystemConfig{
			AdaptiveHeatingCurve:         cloneBoilerBoolPtr(snapshot.AdaptiveHeatingCurve),
			AlternativePoint:             cloneFloat64Ptr(snapshot.AlternativePoint),
			HeatingCircuitBivalencePoint: cloneFloat64Ptr(snapshot.HeatingCircuitBivalencePoint),
			DhwBivalencePoint:            cloneFloat64Ptr(snapshot.DhwBivalencePoint),
			DhwBivalenceBound:            &dhwBound,
			HcEmergencyTemperature:       cloneFloat64Ptr(snapshot.HcEmergencyTemperature),
			HwcMaxFlowTempDesired:        cloneFloat64Ptr(snapshot.HwcMaxFlowTempDesired),
			MaxRoomHumidity:              uint16ToIntPtr(snapshot.MaxRoomHumidity),
			MaintenanceDate:              cloneStringPtr(snapshot.MaintenanceDate),
			InstallerName:                cloneStringPtr(snapshot.InstallerName),
			InstallerPhone:               cloneStringPtr(snapshot.InstallerPhone),
			InstallerMenuCode:            uint16ToIntPtr(snapshot.InstallerMenuCode),
		},
		Properties: graphql.SystemProperties{
			SystemScheme:            uint16ToIntPtr(snapshot.SystemScheme),
			ModuleConfigurationVR71: uint16ToIntPtr(snapshot.ModuleConfigurationVR71),
		},
	}
}
