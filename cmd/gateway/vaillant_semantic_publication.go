package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// vaillantSemanticPublication is the publication owner of the Vaillant
// poller: it holds the GraphQL provider, the broadcast hub and the semantic
// cache persister, and no bus access. The acquisition owner reaches it only
// through vaillantSemanticPublisher, whose methods speak native values.
// Zones, hot water, circuits, radio devices, the boiler status and the system
// reach it as native views that the poller builds under its snapshot lock,
// and the regulator capability as the native controller capability; the
// owner maps them to GraphQL values in vaillant_semantic_publication_views.go.
// The FM5 verdict, solar and cylinders reach it as a native view that it maps
// in vaillant_semantic_publication_fm5.go, and it publishes the startup plane
// defaults and the startup schedule placeholder. It is also the energy ledger
// of a register acquisition: it derives the provider's energy keys from
// native channel, usage and window values. Adapter hardware info reaches it
// as a native value that it maps to the GraphQL form, and the hot water
// bivalence bound as the native configuration bound that it maps to the
// GraphQL and MCP forms. Cache persistence and the conversion of a loaded
// cache into the native hydration value live in
// vaillant_semantic_publication_cache.go; the writer adapters that map the
// native write results live in vaillant_semantic_publication_writes.go.
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

func startVaillantSemanticPolling(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub, startupBarrier <-chan struct{}) *vaillantSemanticPoller {
	if gateway == nil || gateway.Bus == nil || gateway.Registry == nil || provider == nil {
		return nil
	}

	cacheStore := newSemanticCacheStore(cfg.SemanticCachePath, log.Printf)
	cacheHydration, cacheLoaded := preloadSemanticCache(provider, cacheStore)
	poller := newVaillantSemanticPoller(cfg, gateway, provider, hub, cacheStore)
	poller.startupBarrier = startupBarrier
	if cacheLoaded {
		poller.hydrateFromCache(cacheHydration)
	}
	poller.Start(ctx)
	return poller
}

// newVaillantSemanticPoller builds the acquisition owner with a publication
// owner over provider, hub and cache.
func newVaillantSemanticPoller(cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub, cache semanticCachePersister) *vaillantSemanticPoller {
	return newVaillantSemanticAcquisition(cfg, gateway, newVaillantSemanticPublication(provider, hub, cache))
}

// publicationOwner returns the poller's publication owner, or nil when the
// poller has none.
func publicationOwner(p *vaillantSemanticPoller) *vaillantSemanticPublication {
	if p == nil {
		return nil
	}
	owner, _ := p.publication.(*vaillantSemanticPublication)
	return owner
}

// live reports whether the owner holds a provider to publish to.
func (o *vaillantSemanticPublication) live() bool {
	return o != nil && o.provider != nil
}

// boilerPublishLock returns the owner's boiler publication lock.
func (o *vaillantSemanticPublication) boilerPublishLock() sync.Locker {
	return &o.boilerPublishMu
}

// setNoSupportedHeatSourcePath sets or clears the provider's startup reason
// for a bus without a supported heat source path and returns the reason and
// whether the provider changed.
func (o *vaillantSemanticPublication) setNoSupportedHeatSourcePath(active bool) (string, bool) {
	return graphql.SemanticStartupReasonNoSupportedHeatSourcePath, o.provider.SetNoSupportedHeatSourcePath(active)
}

// publishedPlanes reports which planes the provider publishes.
func (o *vaillantSemanticPublication) publishedPlanes() vaillantPublishedPlanes {
	cylinders := o.provider.Cylinders()
	return vaillantPublishedPlanes{
		zones:            o.provider.Zones() != nil,
		dhw:              o.provider.DHW() != nil,
		circuits:         o.provider.Circuits() != nil,
		system:           o.provider.System() != nil,
		radioDeviceCount: len(o.provider.RadioDevices()),
		solar:            o.provider.Solar() != nil,
		cylinders:        cylinders != nil,
		cylinderCount:    len(cylinders),
		boilerStatus:     o.provider.BoilerStatus() != nil,
	}
}

// publishedZoneCount returns the number of zones the provider publishes.
func (o *vaillantSemanticPublication) publishedZoneCount() int {
	return len(o.provider.Zones())
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

func circuitsEqual(a, b []graphql.CircuitStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		left := a[i]
		right := b[i]
		if left.Index != right.Index || left.CircuitType != right.CircuitType || left.HasMixer != right.HasMixer {
			return false
		}
		if !boolPtrEquals(left.State.PumpActive, right.State.PumpActive) {
			return false
		}
		if !floatPtrEquals(left.State.MixerPositionPct, right.State.MixerPositionPct) {
			return false
		}
		if !floatPtrEquals(left.State.FlowTemperatureC, right.State.FlowTemperatureC) {
			return false
		}
		if !floatPtrEquals(left.State.FlowSetpointC, right.State.FlowSetpointC) {
			return false
		}
		if !floatPtrEquals(left.State.CalcFlowTempC, right.State.CalcFlowTempC) {
			return false
		}
		if left.State.CircuitState != right.State.CircuitState {
			return false
		}
		if !floatPtrEquals(left.State.Humidity, right.State.Humidity) {
			return false
		}
		if !floatPtrEquals(left.State.DewPoint, right.State.DewPoint) {
			return false
		}
		if !floatPtrEquals(left.State.PumpHours, right.State.PumpHours) {
			return false
		}
		if !intPtrEquals(left.State.PumpStarts, right.State.PumpStarts) {
			return false
		}
		if !floatPtrEquals(left.Config.HeatingCurve, right.Config.HeatingCurve) {
			return false
		}
		if !floatPtrEquals(left.Config.FlowTempMaxC, right.Config.FlowTempMaxC) {
			return false
		}
		if !floatPtrEquals(left.Config.FlowTempMinC, right.Config.FlowTempMinC) {
			return false
		}
		if !floatPtrEquals(left.Config.SummerLimitC, right.Config.SummerLimitC) {
			return false
		}
		if !intPtrEquals(left.Config.SpecialFunctionMode, right.Config.SpecialFunctionMode) {
			return false
		}
		if left.Config.RoomTempControl != right.Config.RoomTempControl {
			return false
		}
		if !boolPtrEquals(left.Config.CoolingEnabled, right.Config.CoolingEnabled) {
			return false
		}
		if left.ManagingDevice.Role != right.ManagingDevice.Role {
			return false
		}
		if !stringPtrEquals(left.ManagingDevice.DeviceID, right.ManagingDevice.DeviceID) {
			return false
		}
		if !intPtrEquals(left.ManagingDevice.Address, right.ManagingDevice.Address) {
			return false
		}
	}
	return true
}

func deriveCircuitManagingDevice(system *vaillantSystemSnapshot, fm5Mode vaillantFM5Mode) graphql.ManagingDevice {
	return circuitManagingDevice(circuitsManagedByFunctionModule(system, fm5Mode))
}

// circuitManagingDevice returns a detached managing device for one circuit.
func circuitManagingDevice(managedByFunctionModule bool) graphql.ManagingDevice {
	if managedByFunctionModule {
		deviceID := circuitManagingDeviceVR71ID
		address := circuitManagingDeviceVR71Address
		return graphql.ManagingDevice{
			Role:     graphql.ManagingDeviceRoleFunctionModule,
			DeviceID: &deviceID,
			Address:  &address,
		}
	}
	return graphql.ManagingDevice{Role: graphql.ManagingDeviceRoleUnknown}
}

func dhwEquals(a, b *graphql.DhwStatus) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Config.OperatingMode != b.Config.OperatingMode || a.Config.Preset != b.Config.Preset {
		return false
	}
	if a.State.SpecialFunction != b.State.SpecialFunction {
		return false
	}
	if !floatPtrEquals(a.State.CurrentTempC, b.State.CurrentTempC) {
		return false
	}
	if !floatPtrEquals(a.Config.TargetTempC, b.Config.TargetTempC) {
		return false
	}
	if !floatPtrEquals(a.State.HeatingDemandPct, b.State.HeatingDemandPct) {
		return false
	}
	if a.Config.HolidayStartDate != b.Config.HolidayStartDate || a.Config.HolidayEndDate != b.Config.HolidayEndDate {
		return false
	}
	return true
}

func radioDevicesEqual(a, b []graphql.RadioDevice) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		left := a[i]
		right := b[i]
		if left.Group != right.Group || left.Instance != right.Instance || left.SlotMode != right.SlotMode {
			return false
		}
		if !boolPtrEquals(left.DeviceConnected, right.DeviceConnected) {
			return false
		}
		if !intPtrEquals(left.DeviceClassAddress, right.DeviceClassAddress) {
			return false
		}
		if left.DeviceModel != right.DeviceModel {
			return false
		}
		if !stringPtrEquals(left.FirmwareVersion, right.FirmwareVersion) {
			return false
		}
		if !intPtrEquals(left.HardwareIdentifier, right.HardwareIdentifier) {
			return false
		}
		if !intPtrEquals(left.RemoteControlAddress, right.RemoteControlAddress) {
			return false
		}
		if !boolPtrEquals(left.DevicePaired, right.DevicePaired) {
			return false
		}
		if !intPtrEquals(left.ReceptionStrength, right.ReceptionStrength) {
			return false
		}
		if !intPtrEquals(left.ZoneAssignment, right.ZoneAssignment) {
			return false
		}
		if !floatPtrEquals(left.RoomTemperatureC, right.RoomTemperatureC) {
			return false
		}
		if !floatPtrEquals(left.RoomHumidityPct, right.RoomHumidityPct) {
			return false
		}
	}
	return true
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
