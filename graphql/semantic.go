package graphql

import "sync"

type ZoneState struct {
	CurrentTempC       *float64
	CurrentHumidityPct *float64
	HvacAction         string
	SpecialFunction    string
	HeatingDemandPct   *float64
	ValvePositionPct   *float64
}

type ZoneConfig struct {
	OperatingMode     string
	Preset            string
	TargetTempC       *float64
	AllowedModes      []string
	CircuitType       string
	AssociatedCircuit *int
}

type Zone struct {
	ID     string
	Name   string
	State  ZoneState
	Config ZoneConfig
}

type DhwState struct {
	CurrentTempC     *float64
	SpecialFunction  string
	HeatingDemandPct *float64
}

type DhwConfig struct {
	OperatingMode string
	Preset        string
	TargetTempC   *float64
}

type DhwStatus struct {
	State  DhwState
	Config DhwConfig
}

type CircuitState struct {
	PumpActive       *bool
	MixerPositionPct *float64
	FlowTemperatureC *float64
	FlowSetpointC    *float64
	CalcFlowTempC    *float64
	CircuitState     string
	Humidity         *float64
	DewPoint         *float64
	PumpHours        *float64
	PumpStarts       *int
}

type CircuitConfig struct {
	HeatingCurve    *float64
	FlowTempMaxC    *float64
	FlowTempMinC    *float64
	SummerLimitC    *float64
	FrostProtC      *float64
	RoomTempControl string
	CoolingEnabled  *bool
}

type CircuitStatus struct {
	Index       int
	CircuitType string
	HasMixer    bool
	State       CircuitState
	Config      CircuitConfig
}

type RadioDevice struct {
	Group                int
	Instance             int
	SlotMode             string
	DeviceConnected      *bool
	DeviceClassAddress   *int
	DeviceModel          string
	FirmwareVersion      *string
	HardwareIdentifier   *int
	RemoteControlAddress *int
	DevicePaired         *bool
	ReceptionStrength    *int
	ZoneAssignment       *int
	RoomTemperatureC     *float64
	RoomHumidityPct      *float64
}

type EnergySeries struct {
	Today  float64
	Yearly []float64
}

type EnergyChannel struct {
	DHW     EnergySeries
	Climate EnergySeries
}

type EnergyTotals struct {
	Gas      EnergyChannel
	Electric EnergyChannel
	Solar    EnergyChannel
}

type BoilerState struct {
	FlowTemperatureC         *float64
	ReturnTemperatureC       *float64
	CentralHeatingPumpActive *bool
	DhwTemperatureC          *float64
	DhwTargetTemperatureC    *float64
}

type BoilerConfig struct {
	DhwOperatingMode *string
}

type BoilerDiagnostics struct {
	HeatingStatusRaw *int
	DhwStatusRaw     *int
}

type BoilerStatus struct {
	State       BoilerState
	Config      BoilerConfig
	Diagnostics BoilerDiagnostics
}

type SystemStatus struct {
	State      SystemState
	Config     SystemConfig
	Properties SystemProperties
}

type SystemState struct {
	SystemOff                    *bool
	SystemWaterPressure          *float64
	SystemFlowTemperature        *float64
	OutdoorTemperature           *float64
	OutdoorTemperatureAvg24h     *float64
	MaintenanceDue               *bool
	HwcCylinderTemperatureTop    *float64
	HwcCylinderTemperatureBottom *float64
}

type SystemConfig struct {
	AdaptiveHeatingCurve         *bool
	AlternativePoint             *float64
	HeatingCircuitBivalencePoint *float64
	DhwBivalencePoint            *float64
	HcEmergencyTemperature       *float64
	HwcMaxFlowTempDesired        *float64
	MaxRoomHumidity              *int
}

type SystemProperties struct {
	SystemScheme            *int
	ModuleConfigurationVR71 *int
	Vr71CircuitStartIndex   *int
}

type SemanticProvider interface {
	Zones() []Zone
	DHW() *DhwStatus
	Circuits() []CircuitStatus
	RadioDevices() []RadioDevice
	EnergyTotals() *EnergyTotals
	BoilerStatus() *BoilerStatus
	System() *SystemStatus
}

type staticSemanticProvider struct{}

func (staticSemanticProvider) Zones() []Zone {
	return nil
}

func (staticSemanticProvider) DHW() *DhwStatus {
	return nil
}

func (staticSemanticProvider) Circuits() []CircuitStatus {
	return nil
}

func (staticSemanticProvider) RadioDevices() []RadioDevice {
	return nil
}

func (staticSemanticProvider) EnergyTotals() *EnergyTotals {
	return nil
}

func (staticSemanticProvider) BoilerStatus() *BoilerStatus {
	return nil
}

func (staticSemanticProvider) System() *SystemStatus {
	return nil
}

var liveSystemSnapshots sync.Map

func (provider *LiveSemanticProvider) System() *SystemStatus {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	stored, ok := liveSystemSnapshots.Load(provider)
	if !ok || stored == nil {
		return nil
	}
	status, ok := stored.(*SystemStatus)
	if !ok || status == nil {
		return nil
	}
	return cloneSystemStatus(status)
}

func (provider *LiveSemanticProvider) SetSystem(status *SystemStatus) {
	if provider == nil {
		return
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if status == nil {
		liveSystemSnapshots.Delete(provider)
		return
	}
	liveSystemSnapshots.Store(provider, cloneSystemStatus(status))
}

func (provider *LiveSemanticProvider) SetSystemFromCache(status *SystemStatus) {
	provider.SetSystem(status)
}

func cloneSystemStatus(status *SystemStatus) *SystemStatus {
	if status == nil {
		return nil
	}
	cp := *status
	cp.State = cloneSystemState(cp.State)
	cp.Config = cloneSystemConfig(cp.Config)
	cp.Properties = cloneSystemProperties(cp.Properties)
	return &cp
}

func cloneSystemState(state SystemState) SystemState {
	if state.SystemOff != nil {
		v := *state.SystemOff
		state.SystemOff = &v
	}
	if state.SystemWaterPressure != nil {
		v := *state.SystemWaterPressure
		state.SystemWaterPressure = &v
	}
	if state.SystemFlowTemperature != nil {
		v := *state.SystemFlowTemperature
		state.SystemFlowTemperature = &v
	}
	if state.OutdoorTemperature != nil {
		v := *state.OutdoorTemperature
		state.OutdoorTemperature = &v
	}
	if state.OutdoorTemperatureAvg24h != nil {
		v := *state.OutdoorTemperatureAvg24h
		state.OutdoorTemperatureAvg24h = &v
	}
	if state.MaintenanceDue != nil {
		v := *state.MaintenanceDue
		state.MaintenanceDue = &v
	}
	if state.HwcCylinderTemperatureTop != nil {
		v := *state.HwcCylinderTemperatureTop
		state.HwcCylinderTemperatureTop = &v
	}
	if state.HwcCylinderTemperatureBottom != nil {
		v := *state.HwcCylinderTemperatureBottom
		state.HwcCylinderTemperatureBottom = &v
	}
	return state
}

func cloneSystemConfig(config SystemConfig) SystemConfig {
	if config.AdaptiveHeatingCurve != nil {
		v := *config.AdaptiveHeatingCurve
		config.AdaptiveHeatingCurve = &v
	}
	if config.AlternativePoint != nil {
		v := *config.AlternativePoint
		config.AlternativePoint = &v
	}
	if config.HeatingCircuitBivalencePoint != nil {
		v := *config.HeatingCircuitBivalencePoint
		config.HeatingCircuitBivalencePoint = &v
	}
	if config.DhwBivalencePoint != nil {
		v := *config.DhwBivalencePoint
		config.DhwBivalencePoint = &v
	}
	if config.HcEmergencyTemperature != nil {
		v := *config.HcEmergencyTemperature
		config.HcEmergencyTemperature = &v
	}
	if config.HwcMaxFlowTempDesired != nil {
		v := *config.HwcMaxFlowTempDesired
		config.HwcMaxFlowTempDesired = &v
	}
	if config.MaxRoomHumidity != nil {
		v := *config.MaxRoomHumidity
		config.MaxRoomHumidity = &v
	}
	return config
}

func cloneSystemProperties(properties SystemProperties) SystemProperties {
	if properties.SystemScheme != nil {
		v := *properties.SystemScheme
		properties.SystemScheme = &v
	}
	if properties.ModuleConfigurationVR71 != nil {
		v := *properties.ModuleConfigurationVR71
		properties.ModuleConfigurationVR71 = &v
	}
	if properties.Vr71CircuitStartIndex != nil {
		v := *properties.Vr71CircuitStartIndex
		properties.Vr71CircuitStartIndex = &v
	}
	return properties
}
