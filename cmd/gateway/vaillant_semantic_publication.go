package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// vaillantSemanticPublication owns the collaborators through which the
// Vaillant poller publishes its snapshots: the GraphQL provider, the
// broadcast hub and the semantic cache persister. It holds no bus access.
// Zones, hot water, circuits, radio devices, the boiler status and the system
// reach it as native views that the poller builds under its snapshot lock,
// and the regulator capability as the native controller capability; the
// owner maps them to GraphQL values in vaillant_semantic_publication_views.go.
// The FM5 verdict, solar and cylinders reach it as a native view that it maps
// in vaillant_semantic_publication_fm5.go, and it publishes the startup plane
// defaults and the startup schedule placeholder. It is also the energy ledger of a register acquisition: it derives the
// provider's energy keys from native channel, usage and window values.
// Adapter hardware info reaches it as a native value that it maps to the
// GraphQL form, and the hot water bivalence bound as the native
// configuration bound that it maps to the GraphQL and MCP forms. Cache
// persistence and the conversion of a loaded cache into the native
// hydration value live in vaillant_semantic_publication_cache.go.
//
// boilerPublishMu orders GraphQL boiler-status publication against the
// thermal withdrawal: publishBoilerStatus and the thermal publisher's
// control callback each hold it across their change of the native boiler
// snapshot and the owner's publication or withdrawal of the provider's
// boiler status, so a withdrawn flow temperature is never overwritten by an
// interleaved publication and neither path publishes a status the other is
// still changing. The lock guards no other state.
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

// energyLedger returns the publication owner as the energy ledger of a
// register acquisition, or nil when the poller has no provider to hold the
// ledger.
func (p *vaillantSemanticPoller) energyLedger() vaillantEnergyLedger {
	if p == nil || p.publication == nil || p.publication.provider == nil {
		return nil
	}
	return p.publication
}

// energyReferenceTime returns the provider's energy reference instant.
func (o *vaillantSemanticPublication) energyReferenceTime() time.Time {
	return o.provider.EnergyReferenceTime()
}

// applyLifetimeEnergy applies an undated lifetime register total under the
// provider's lifetime key.
func (o *vaillantSemanticPublication) applyLifetimeEnergy(channel, usage string, kwh float64) bool {
	return o.provider.ApplyEnergyFromRegister(o.provider.EnergyLifetimeKey(channel, usage), kwh)
}

// applyWindowEnergy applies a relative window register value under the
// provider's key dated against ref.
func (o *vaillantSemanticPublication) applyWindowEnergy(channel, usage, period, label string, ref time.Time, kwh float64) bool {
	return o.provider.ApplyEnergyFromRegister(o.provider.EnergyWindowKeyAt(channel, usage, period, label, ref), kwh)
}

// publishEnergyTotals pushes the provider's energy totals to the hub.
func (o *vaillantSemanticPublication) publishEnergyTotals() {
	if o == nil || o.provider == nil || o.hub == nil {
		return
	}
	totals := o.provider.EnergyTotals()
	if totals == nil {
		return
	}
	o.hub.PublishEnergyUpdate(totals)
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

// publishBoilerStatus merges the published thermal flow into the retained
// boiler snapshot, hands a detached copy to the publication owner and
// persists the semantic cache after a publication. It holds the owner's
// boiler publication lock across the merge, the copy and the publication.
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

	if p.publication.publishBoilerStatusView(snapshot, source) {
		p.persistSemanticCache(source)
	}
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

// publishSystem hands the native system view, with the hot water bivalence
// bound resolved for the discovered controller, to the publication owner;
// after a publication it persists the semantic cache and republishes the
// circuits.
func (p *vaillantSemanticPoller) publishSystem(source semanticSnapshotSource) {
	if p == nil || p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	view := vaillantSystemView{snapshot: cloneSystemSnapshot(p.system)}
	p.mu.Unlock()
	if view.snapshot != nil {
		view.dhwBivalenceBound = p.controllerConfigBound()
	}

	if !p.publication.publishSystemView(view, source) {
		return
	}
	p.persistSemanticCache(source)
	p.publishCircuits(source)
}

// publishStartupPlaneDefaults publishes an empty circuits, solar and
// cylinders plane for each of them that the provider does not hold yet, so
// that startup readers see published planes before the first acquisition.
func (o *vaillantSemanticPublication) publishStartupPlaneDefaults() {
	if o.provider.Circuits() == nil {
		o.provider.SetCircuits([]graphql.CircuitStatus{})
	}
	if o.provider.Solar() == nil {
		o.provider.SetSolar(&graphql.SolarStatus{})
	}
	if o.provider.Cylinders() == nil {
		o.provider.SetCylinders([]graphql.CylinderStatus{})
	}
}

// publishStartupSchedulePlaceholder publishes an empty schedule program list
// when the provider holds no schedules yet.
func (o *vaillantSemanticPublication) publishStartupSchedulePlaceholder() {
	if o.provider.Schedules() != nil {
		return
	}
	o.provider.SetSchedules(&graphql.ScheduleStatus{Programs: []graphql.ScheduleProgram{}})
}

// publishAdapterInfo maps the native adapter hardware info to its GraphQL
// form and hands it to the provider.
func (o *vaillantSemanticPublication) publishAdapterInfo(info vaillantAdapterInfo) {
	if o == nil || o.provider == nil {
		return
	}
	o.provider.SetAdapterHardwareInfo(adapterHardwareInfoFromNative(info))
}

func adapterHardwareInfoFromNative(native vaillantAdapterInfo) *graphql.AdapterHardwareInfo {
	info := &graphql.AdapterHardwareInfo{}

	if native.identity != nil {
		v := native.identity
		info.FirmwareVersion = fmt.Sprintf("0x%02x", v.Version)
		info.Features = v.Features
		info.Jumpers = v.Jumpers
		info.IsWiFi = v.IsWiFi
		info.IsEthernet = v.IsEthernet
		info.VersionResponseLen = v.VersionResponseLen()
		info.InfoSupported = v.SupportsInfo

		if v.HasChecksum {
			info.FirmwareChecksum = fmt.Sprintf("0x%04x", v.Checksum)
		}
		if v.HasBootloader {
			info.BootloaderVersion = fmt.Sprintf("0x%02x", v.BootloaderVersion)
			info.BootloaderChecksum = fmt.Sprintf("0x%04x", v.BootloaderChecksum)
		}

		var flags []string
		if v.Jumpers&0x01 != 0 {
			flags = append(flags, "enhanced")
		}
		if v.IsHighSpeed {
			flags = append(flags, "high_speed")
		}
		if v.IsEthernet {
			flags = append(flags, "ethernet")
		}
		if v.IsWiFi {
			flags = append(flags, "wifi")
		}
		if v.IsV31 {
			flags = append(flags, "v3.1")
		}
		if v.Jumpers&0x20 != 0 {
			flags = append(flags, "soft_config")
		}
		info.JumperFlags = flags
	}

	info.HardwareID = native.hardwareID
	info.HardwareConfig = native.hardwareConfig
	info.TemperatureC = native.temperatureC
	info.SupplyVoltageMV = native.supplyMV
	info.BusVoltageMaxDV = native.busMaxDV
	info.BusVoltageMinDV = native.busMinDV
	info.ResetCause = native.resetCause
	info.ResetCauseCode = native.resetCode
	info.RestartCount = native.restartCount
	info.WiFiRSSIDBm = native.wifiRSSI

	if !native.lastIdentity.IsZero() {
		t := native.lastIdentity
		info.LastIdentityQuery = &t
	}
	if !native.lastTelemetry.IsZero() {
		t := native.lastTelemetry
		info.LastTelemetryQuery = &t
	}
	return info
}

// configBoundEquals compares two published GraphQL configuration bounds.
func configBoundEquals(a, b *graphql.ConfigBound) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// mcpConfigBound maps a published GraphQL configuration bound to its MCP
// form.
func mcpConfigBound(bound *graphql.ConfigBound) *mcp.ConfigBound {
	if bound == nil {
		return nil
	}
	return &mcp.ConfigBound{
		NarrowedOnly: bound.NarrowedOnly,
		MinC:         bound.MinC,
		MaxC:         bound.MaxC,
		Source:       bound.Source,
		Reason:       bound.Reason,
		Profile:      bound.Profile,
	}
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
