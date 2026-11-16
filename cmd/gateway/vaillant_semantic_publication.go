package main

import (
	"reflect"
	"slices"
	"sync"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

// vaillantSemanticPublication owns the collaborators through which the
// Vaillant poller publishes its snapshots: the GraphQL provider, the
// broadcast hub and the semantic cache persister. It holds no bus access.
// Zones, hot water, circuits and radio devices reach it as native views that
// the poller builds under its snapshot lock; the owner maps them to GraphQL
// values in vaillant_semantic_publication_views.go.
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

// publishZones hands the native zone views to the publication owner and
// persists the semantic cache after a publication.
func (p *vaillantSemanticPoller) publishZones(source semanticSnapshotSource) {
	if p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	views := p.zoneViewsLocked()
	p.mu.Unlock()

	if p.publication.publishZoneViews(views, source) {
		p.persistSemanticCache(source)
	}
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

// publishDHW applies stale expiry, hands the native hot water view to the
// publication owner and persists the semantic cache after a publication; an
// expiry persists the snapshot whatever its source.
func (p *vaillantSemanticPoller) publishDHW(source semanticSnapshotSource) {
	if p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	expired := p.expireDHWIfStaleLocked(source)
	view := p.dhwViewLocked(expired)
	p.mu.Unlock()

	if !p.publication.publishDhwView(view, source) {
		return
	}
	if view.snapshot == nil && view.expired {
		p.persistSemanticSnapshot()
		return
	}
	p.persistSemanticCache(source)
}

// publishCircuits hands the native circuits view to the publication owner
// and persists the semantic cache after a publication.
func (p *vaillantSemanticPoller) publishCircuits(source semanticSnapshotSource) {
	if p == nil || p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	view := p.circuitsViewLocked()
	p.mu.Unlock()

	if p.publication.publishCircuitsView(view, source) {
		p.persistSemanticCache(source)
	}
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

// publishRadioDevices hands the native radio device views to the publication
// owner and persists the semantic cache after a publication.
func (p *vaillantSemanticPoller) publishRadioDevices(source semanticSnapshotSource) {
	if p == nil || p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	views := p.radioDeviceViewsLocked()
	p.mu.Unlock()

	if p.publication.publishRadioDeviceViews(views, source) {
		p.persistSemanticCache(source)
	}
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
