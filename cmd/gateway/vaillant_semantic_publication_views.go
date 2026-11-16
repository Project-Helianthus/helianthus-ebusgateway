package main

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
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
