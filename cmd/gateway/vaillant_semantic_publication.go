package main

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

// vaillantSemanticPublication owns the collaborators through which the
// Vaillant poller publishes its snapshots: the GraphQL provider, the
// broadcast hub and the semantic cache persister. It holds no bus access.
//
// boilerPublishMu orders GraphQL boiler-status publication against the
// thermal withdrawal: publishBoilerStatus and the thermal publisher's
// control callback each hold it across their read, change and publish of
// the provider's boiler status, so a withdrawn flow temperature is never
// overwritten by an interleaved publication and neither path publishes a
// status the other is still changing. The lock guards no other state.
type vaillantSemanticPublication struct {
	provider *graphql.LiveSemanticProvider
	hub      *graphql.BroadcastHub
	cache    semanticCachePersister

	boilerPublishMu sync.Mutex
}

func newVaillantSemanticPublication(provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub, cache semanticCachePersister) *vaillantSemanticPublication {
	return &vaillantSemanticPublication{provider: provider, hub: hub, cache: cache}
}

// vaillantPublicationNoLock is the publish lock of a poller without a
// publication owner, such as the registry-only classifier built during the
// startup scan: it never publishes, so there is nothing to order.
type vaillantPublicationNoLock struct{}

func (vaillantPublicationNoLock) Lock()   {}
func (vaillantPublicationNoLock) Unlock() {}

// liveProvider returns the GraphQL provider, or nil when the poller has no
// publication owner.
func (p *vaillantSemanticPoller) liveProvider() *graphql.LiveSemanticProvider {
	if p == nil || p.publication == nil {
		return nil
	}
	return p.publication.provider
}

// broadcastHub returns the broadcast hub, or nil when the poller has no
// publication owner.
func (p *vaillantSemanticPoller) broadcastHub() *graphql.BroadcastHub {
	if p == nil || p.publication == nil {
		return nil
	}
	return p.publication.hub
}

// cachePersister returns the semantic cache persister, or nil when the
// poller has no publication owner.
func (p *vaillantSemanticPoller) cachePersister() semanticCachePersister {
	if p == nil || p.publication == nil {
		return nil
	}
	return p.publication.cache
}

// boilerPublishLock returns the owner's boiler publication lock, or a
// no-op lock when the poller has no publication owner.
func (p *vaillantSemanticPoller) boilerPublishLock() sync.Locker {
	if p == nil || p.publication == nil {
		return vaillantPublicationNoLock{}
	}
	return &p.publication.boilerPublishMu
}

func (p *vaillantSemanticPoller) publishRegulatorCapability(capability productids.ControllerCapability) {
	if p == nil || p.liveProvider() == nil {
		return
	}
	switch capability {
	case productids.ControllerPresent:
		p.liveProvider().SetVaillantRegulatorCapability(graphql.VaillantRegulatorCapabilityPresent)
	case productids.ControllerNone:
		p.liveProvider().SetVaillantRegulatorCapability(graphql.VaillantRegulatorCapabilityNone)
	default:
		p.liveProvider().SetVaillantRegulatorCapability(graphql.VaillantRegulatorCapabilityUnknown)
	}
}

func (p *vaillantSemanticPoller) publishZones(source semanticSnapshotSource) {
	if p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	instances := make([]byte, 0, len(p.zones))
	for instance := range p.zones {
		instances = append(instances, instance)
	}
	slices.Sort(instances)
	zones := make([]graphql.Zone, 0, len(instances))
	for _, instance := range instances {
		entry := p.zones[instance]
		if entry == nil {
			continue
		}

		name := entry.Name
		if strings.TrimSpace(name) == "" {
			name = fmt.Sprintf("Zone %d", instance+1)
		}

		quickVetoActive := entry.Preset == "quickveto"
		var qvExpiry string
		if quickVetoActive && entry.QuickVetoEndDate != "" && entry.QuickVetoEndTime != "" {
			qvExpiry = entry.QuickVetoEndDate + "T" + entry.QuickVetoEndTime
		}
		currentTempC := entry.CurrentTempC
		currentHumidityPct := entry.HumidityPct
		if currentTempC == nil || currentHumidityPct == nil {
			radioTempC, radioHumidityPct := p.mappedRadioRoomStateForZoneLocked(entry)
			if currentTempC == nil {
				currentTempC = radioTempC
			}
			if currentHumidityPct == nil {
				currentHumidityPct = radioHumidityPct
			}
		}
		roomMappingRaw := entry.ConfigurationRoomTemperatureZoneMappingRaw
		if roomMappingRaw == nil {
			roomMappingRaw = p.inferRadioRoomMappingForZoneLocked(entry.Instance)
		}
		associatedCircuitRaw := entry.ConfigurationAssociatedCircuitRaw
		if associatedCircuitRaw == nil && roomMappingRaw != nil {
			value := uint16(resolveAssociatedCircuitInstance(roomMappingRaw, entry.Instance))
			associatedCircuitRaw = &value
		}
		zone := graphql.Zone{
			ID:   fmt.Sprintf("zone-%d", instance+1),
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
		zones = append(zones, zone)
	}
	p.mu.Unlock()

	previous := p.liveProvider().Zones()
	if source == semanticSnapshotSourceCache && len(zones) == 0 && len(previous) > 0 {
		return
	}
	switch source {
	case semanticSnapshotSourceCache:
		p.liveProvider().SetZonesFromCache(zones)
	default:
		p.liveProvider().SetZones(zones)
	}

	if p.broadcastHub() != nil {
		prevByID := make(map[string]graphql.Zone, len(previous))
		for _, z := range previous {
			prevByID[z.ID] = z
		}
		for _, zone := range zones {
			if !zoneEquals(prevByID[zone.ID], zone) {
				p.broadcastHub().PublishZoneUpdate(zone)
			}
		}
	}
	p.persistSemanticCache(source)
}

func (p *vaillantSemanticPoller) publishEnergyTotals() {
	if p == nil || p.liveProvider() == nil || p.broadcastHub() == nil {
		return
	}
	totals := p.liveProvider().EnergyTotals()
	if totals == nil {
		return
	}
	p.broadcastHub().PublishEnergyUpdate(totals)
}

func (p *vaillantSemanticPoller) publishDHW(source semanticSnapshotSource) {
	if p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	expired := p.expireDHWIfStaleLocked(source)
	snapshot := p.dhw
	p.mu.Unlock()

	previous := p.liveProvider().DHW()
	if snapshot == nil {
		if source == semanticSnapshotSourceCache && previous != nil && !expired {
			return
		}
		switch source {
		case semanticSnapshotSourceCache:
			p.liveProvider().SetDHWFromCache(nil)
		default:
			p.liveProvider().SetDHW(nil)
		}
		if previous != nil && p.broadcastHub() != nil {
			p.broadcastHub().PublishDHWUpdate(nil)
		}
		if expired {
			p.persistSemanticSnapshot()
		} else {
			p.persistSemanticCache(source)
		}
		return
	}

	current := &graphql.DhwStatus{
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

	switch source {
	case semanticSnapshotSourceCache:
		p.liveProvider().SetDHWFromCache(current)
	default:
		p.liveProvider().SetDHW(current)
	}
	if p.broadcastHub() != nil && !dhwEquals(previous, current) {
		p.broadcastHub().PublishDHWUpdate(current)
	}
	p.persistSemanticCache(source)
}

func (p *vaillantSemanticPoller) publishCircuits(source semanticSnapshotSource) {
	if p == nil || p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	instances := make([]byte, 0, len(p.circuits))
	systemSnapshot := cloneSystemSnapshot(p.system)
	fm5Mode := p.fm5Mode
	for instance := range p.circuits {
		instances = append(instances, instance)
	}
	slices.Sort(instances)
	snapshots := make([]*vaillantCircuitSnapshot, 0, len(instances))
	for _, instance := range instances {
		entry := p.circuits[instance]
		if entry == nil || !entry.Active {
			continue
		}
		snapshots = append(snapshots, cloneCircuitSnapshot(entry))
	}
	p.mu.Unlock()

	out := make([]graphql.CircuitStatus, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot == nil {
			continue
		}
		status := graphql.CircuitStatus{
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
			ManagingDevice: deriveCircuitManagingDevice(systemSnapshot, fm5Mode),
		}
		out = append(out, status)
	}

	previous := p.liveProvider().Circuits()
	if len(out) == 0 && previous == nil {
		return
	}
	if source == semanticSnapshotSourceCache && len(out) == 0 && len(previous) > 0 {
		return
	}
	switch source {
	case semanticSnapshotSourceCache:
		p.liveProvider().SetCircuitsFromCache(out)
	default:
		p.liveProvider().SetCircuits(out)
	}
	if p.broadcastHub() != nil && !circuitsEqual(previous, out) {
		publisher := reflect.ValueOf(p.broadcastHub()).MethodByName("PublishCircuitsUpdate")
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
	p.persistSemanticCache(source)
}

func (p *vaillantSemanticPoller) publishBoilerStatus(source semanticSnapshotSource) {
	if p == nil || p.liveProvider() == nil {
		return
	}

	boilerPublish := p.boilerPublishLock()
	boilerPublish.Lock()
	defer boilerPublish.Unlock()

	p.mergePublishedBoilerFlow()
	p.mu.Lock()
	snapshot := cloneBoilerSnapshot(p.boiler)
	p.mu.Unlock()

	previous := p.liveProvider().BoilerStatus()

	if snapshot == nil {
		if previous != nil {
			switch source {
			case semanticSnapshotSourceCache:
				p.liveProvider().SetBoilerStatusFromCache(nil)
			default:
				p.liveProvider().SetBoilerStatus(nil)
			}
		}
		return
	}

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

	switch source {
	case semanticSnapshotSourceCache:
		p.liveProvider().SetBoilerStatusFromCache(current)
	default:
		p.liveProvider().SetBoilerStatus(current)
	}
	if p.broadcastHub() != nil && !boilerStatusEquals(previous, current) {
		p.broadcastHub().PublishBoilerStatusUpdate(current)
	}
	p.persistSemanticCache(source)
}

func (p *vaillantSemanticPoller) publishRadioDevices(source semanticSnapshotSource) {
	if p == nil || p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	keys := make([]radioDeviceKey, 0, len(p.radioDevices))
	for key := range p.radioDevices {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b radioDeviceKey) int {
		if a.Group != b.Group {
			if a.Group < b.Group {
				return -1
			}
			return 1
		}
		if a.Instance < b.Instance {
			return -1
		}
		if a.Instance > b.Instance {
			return 1
		}
		return 0
	})
	snapshots := make([]*vaillantRadioDeviceSnapshot, 0, len(keys))
	for _, key := range keys {
		snapshot := p.radioDevices[key]
		if snapshot == nil {
			continue
		}
		snapshots = append(snapshots, cloneRadioSnapshot(snapshot))
	}
	p.mu.Unlock()

	out := make([]graphql.RadioDevice, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot == nil {
			continue
		}
		out = append(out, graphql.RadioDevice{
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
		})
	}

	previous := p.liveProvider().RadioDevices()
	if source == semanticSnapshotSourceCache && len(out) == 0 && len(previous) > 0 {
		return
	}
	switch source {
	case semanticSnapshotSourceCache:
		p.liveProvider().SetRadioDevicesFromCache(out)
	default:
		p.liveProvider().SetRadioDevices(out)
	}
	if p.broadcastHub() != nil && !radioDevicesEqual(previous, out) {
		p.broadcastHub().PublishRadioDevicesUpdate(out)
	}
	p.persistSemanticCache(source)
}

func (p *vaillantSemanticPoller) publishSystem(source semanticSnapshotSource) {
	if p == nil || p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	snapshot := cloneSystemSnapshot(p.system)
	p.mu.Unlock()

	previous := p.liveProvider().System()
	if snapshot == nil {
		switch source {
		case semanticSnapshotSourceCache:
			p.liveProvider().SetSystemFromCache(nil)
		default:
			p.liveProvider().SetSystem(nil)
		}
		return
	}

	current := &graphql.SystemStatus{
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
	dhwBound := p.controllerConfigBound()
	current.Config.DhwBivalenceBound = &dhwBound

	switch source {
	case semanticSnapshotSourceCache:
		p.liveProvider().SetSystemFromCache(current)
	default:
		p.liveProvider().SetSystem(current)
	}

	if p.broadcastHub() != nil && !systemStatusEquals(previous, current) {
		publisher := reflect.ValueOf(p.broadcastHub()).MethodByName("PublishSystemUpdate")
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

	p.persistSemanticCache(source)
	p.publishCircuits(source)
}

func (p *vaillantSemanticPoller) publishFM5Semantic(source semanticSnapshotSource) {
	if p == nil || p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	verdict := p.fm5Interpretation
	solarSnapshot := cloneSolarSnapshot(p.solar)
	cylindersSnapshot := cloneCylinderSnapshotsMap(p.solarCylinders)
	p.mu.Unlock()

	switch source {
	case semanticSnapshotSourceCache:
		p.liveProvider().SetFM5InterpretationFromCache(verdict)
	default:
		p.liveProvider().SetFM5Interpretation(verdict)
	}

	if isFM5StructuralWithdrawal(verdict) || (solarSnapshot == nil && len(cylindersSnapshot) == 0) {
		emptySolar := &graphql.SolarStatus{}
		emptyCylinders := []graphql.CylinderStatus{}
		switch source {
		case semanticSnapshotSourceCache:
			p.liveProvider().SetSolarFromCache(emptySolar)
			p.liveProvider().SetCylindersFromCache(emptyCylinders)
		default:
			p.liveProvider().SetSolar(emptySolar)
			p.liveProvider().SetCylinders(emptyCylinders)
		}
		p.publishCircuits(source)
		return
	}

	var solar *graphql.SolarStatus
	if solarSnapshot != nil {
		solar = &graphql.SolarStatus{
			CollectorTemperatureC: cloneFloat64Ptr(solarSnapshot.CollectorTemperatureC),
			ReturnTemperatureC:    cloneFloat64Ptr(solarSnapshot.ReturnTemperatureC),
			PumpActive:            cloneBoilerBoolPtr(solarSnapshot.PumpActive),
			CurrentYield:          cloneFloat64Ptr(solarSnapshot.CurrentYield),
			PumpHours:             decodeUint32Float64(solarSnapshot.PumpHours),
			SolarEnabled:          cloneBoilerBoolPtr(solarSnapshot.SolarEnabled),
			FunctionMode:          cloneBoilerBoolPtr(solarSnapshot.FunctionMode),
		}
	}

	instances := make([]byte, 0, len(cylindersSnapshot))
	for instance := range cylindersSnapshot {
		instances = append(instances, instance)
	}
	slices.Sort(instances)
	cylinders := make([]graphql.CylinderStatus, 0, len(instances))
	for _, instance := range instances {
		snapshot := cylindersSnapshot[instance]
		if snapshot == nil || !hasLiveCylinderEvidence(snapshot) {
			continue
		}
		cylinders = append(cylinders, graphql.CylinderStatus{
			Index:             int(snapshot.Instance),
			TemperatureC:      cloneFloat64Ptr(snapshot.TemperatureC),
			MaxSetpointC:      cloneFloat64Ptr(snapshot.MaxSetpointC),
			ChargeHysteresisC: cloneFloat64Ptr(snapshot.ChargeHysteresis),
			ChargeOffsetC:     cloneFloat64Ptr(snapshot.ChargeOffset),
		})
	}

	switch source {
	case semanticSnapshotSourceCache:
		p.liveProvider().SetSolarFromCache(solar)
		p.liveProvider().SetCylindersFromCache(cylinders)
	default:
		p.liveProvider().SetSolar(solar)
		p.liveProvider().SetCylinders(cylinders)
	}

	p.publishCircuits(source)
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

func (p *vaillantSemanticPoller) persistSemanticCache(source semanticSnapshotSource) {
	if p == nil || source != semanticSnapshotSourceLive {
		return
	}
	p.persistSemanticSnapshot()
}

func (p *vaillantSemanticPoller) persistSemanticSnapshot() {
	if p == nil || p.cachePersister() == nil || p.liveProvider() == nil {
		return
	}
	p.mu.Lock()
	zones := p.cacheZonesLocked(p.liveProvider().Zones())
	p.mu.Unlock()
	_ = p.cachePersister().Save(semanticCacheSnapshot{
		Zones:  zones,
		DHW:    p.liveProvider().DHW(),
		Boiler: p.liveProvider().BoilerStatus(),
	})
}

func (p *vaillantSemanticPoller) cacheZonesLocked(published []graphql.Zone) []graphql.Zone {
	out := make([]graphql.Zone, 0, len(published))
	for idx, zone := range published {
		instance := zoneInstanceFromSemanticID(zone.ID, idx)
		if entry := p.zones[instance]; entry != nil {
			zone.State.CurrentTempC = cloneFloat64Ptr(entry.CurrentTempC)
			zone.State.CurrentHumidityPct = cloneFloat64Ptr(entry.HumidityPct)
			zone.Config.RoomTemperatureZoneMapping = decodeRoomTemperatureZoneMapping(entry.ConfigurationRoomTemperatureZoneMappingRaw)
			zone.Config.AssociatedCircuit = decodeAssociatedCircuit(entry.ConfigurationAssociatedCircuitRaw)
		}
		out = append(out, zone)
	}
	return out
}

func (adapter mcpSemanticProviderAdapter) System() *mcp.SystemStatus {
	if adapter.provider == nil {
		return nil
	}
	status := adapter.provider.System()
	if status == nil {
		return nil
	}
	return &mcp.SystemStatus{
		State: &mcp.SystemState{
			SystemOff:                    cloneBoolPtr(status.State.SystemOff),
			SystemWaterPressure:          cloneFloatPtr(status.State.SystemWaterPressure),
			SystemFlowTemperature:        cloneFloatPtr(status.State.SystemFlowTemperature),
			OutdoorTemperature:           cloneFloatPtr(status.State.OutdoorTemperature),
			OutdoorTemperatureAvg24h:     cloneFloatPtr(status.State.OutdoorTemperatureAvg24h),
			MaintenanceDue:               cloneBoolPtr(status.State.MaintenanceDue),
			HwcCylinderTemperatureTop:    cloneFloatPtr(status.State.HwcCylinderTemperatureTop),
			HwcCylinderTemperatureBottom: cloneFloatPtr(status.State.HwcCylinderTemperatureBottom),
		},
		Config: &mcp.SystemConfig{
			AdaptiveHeatingCurve:         cloneBoolPtr(status.Config.AdaptiveHeatingCurve),
			AlternativePoint:             cloneFloatPtr(status.Config.AlternativePoint),
			HeatingCircuitBivalencePoint: cloneFloatPtr(status.Config.HeatingCircuitBivalencePoint),
			DhwBivalencePoint:            cloneFloatPtr(status.Config.DhwBivalencePoint),
			DhwBivalenceBound:            mcpConfigBound(status.Config.DhwBivalenceBound),
			HcEmergencyTemperature:       cloneFloatPtr(status.Config.HcEmergencyTemperature),
			HwcMaxFlowTempDesired:        cloneFloatPtr(status.Config.HwcMaxFlowTempDesired),
			MaxRoomHumidity:              cloneIntPtr(status.Config.MaxRoomHumidity),
			MaintenanceDate:              cloneStringPtr(status.Config.MaintenanceDate),
			InstallerName:                cloneStringPtr(status.Config.InstallerName),
			InstallerPhone:               cloneStringPtr(status.Config.InstallerPhone),
			InstallerMenuCode:            cloneIntPtr(status.Config.InstallerMenuCode),
		},
		Properties: &mcp.SystemProperties{
			SystemScheme:            cloneIntPtr(status.Properties.SystemScheme),
			ModuleConfigurationVR71: cloneIntPtr(status.Properties.ModuleConfigurationVR71),
		},
	}
}
