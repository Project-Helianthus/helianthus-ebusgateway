package graphql

import (
	"sync"
	"time"
)

type ZoneState struct {
	CurrentTempC       *float64
	CurrentHumidityPct *float64
	HvacAction         string
	SpecialFunction    string
	HeatingDemandPct   *float64
	ValvePositionPct   *float64
}

type ZoneConfig struct {
	OperatingMode              string
	Preset                     string
	TargetTempC                *float64
	AllowedModes               []string
	CircuitType                string
	AssociatedCircuit          *int
	RoomTemperatureZoneMapping *int
	QuickVeto                  bool
	QuickVetoSetpointC         *float64
	QuickVetoDurationH         *float64
	QuickVetoExpiry            string
	HolidayStartDate           string
	HolidayEndDate             string
	HolidaySetpointC           *float64
	HolidayStartTime           string
	HolidayEndTime             string
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
	OperatingMode    string
	Preset           string
	TargetTempC      *float64
	HolidayStartDate string
	HolidayEndDate   string
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

type ManagingDeviceRole string

const (
	ManagingDeviceRoleRegulator      ManagingDeviceRole = "REGULATOR"
	ManagingDeviceRoleFunctionModule ManagingDeviceRole = "FUNCTION_MODULE"
	ManagingDeviceRoleUnknown        ManagingDeviceRole = "UNKNOWN"
)

type ManagingDevice struct {
	Role     ManagingDeviceRole
	DeviceID *string
	Address  *int
}

type CircuitStatus struct {
	Index          int
	CircuitType    string
	HasMixer       bool
	State          CircuitState
	Config         CircuitConfig
	ManagingDevice ManagingDevice
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

type Fm5SemanticMode string

const (
	Fm5SemanticModeInterpreted Fm5SemanticMode = "INTERPRETED"
	Fm5SemanticModeGPIOOnly    Fm5SemanticMode = "GPIO_ONLY"
	Fm5SemanticModeAbsent      Fm5SemanticMode = "ABSENT"
)

type SolarStatus struct {
	CollectorTemperatureC *float64
	ReturnTemperatureC    *float64
	PumpActive            *bool
	CurrentYield          *float64
	PumpHours             *float64
	SolarEnabled          *bool
	FunctionMode          *bool
}

type CylinderStatus struct {
	Index             int
	TemperatureC      *float64
	MaxSetpointC      *float64
	ChargeHysteresisC *float64
	ChargeOffsetC     *float64
}

type EnergySeries struct {
	Today       float64
	Yearly      []float64
	Monthly     []float64
	TodayMeta   EnergyPointMeta
	YearlyMeta  []EnergyPointMeta
	MonthlyMeta []EnergyPointMeta
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

type EnergyFreshnessState string

const (
	EnergyFreshnessStateNeverSeen   EnergyFreshnessState = "never_seen"
	EnergyFreshnessStateFresh       EnergyFreshnessState = "fresh"
	EnergyFreshnessStateWarmingUp   EnergyFreshnessState = "warming_up"
	EnergyFreshnessStateStale       EnergyFreshnessState = "stale"
	EnergyFreshnessStateUnavailable EnergyFreshnessState = "unavailable"
)

type EnergyProvenance string

const (
	EnergyProvenanceNone      EnergyProvenance = "none"
	EnergyProvenanceRegister  EnergyProvenance = "register"
	EnergyProvenanceBroadcast EnergyProvenance = "broadcast"
)

type EnergyPointMeta struct {
	FreshnessState  EnergyFreshnessState
	Provenance      EnergyProvenance
	LastObservedUTC string
	AgeSeconds      float64
	Stale           bool
}

type BoilerState struct {
	FlowTemperatureC         *float64
	ReturnTemperatureC       *float64
	CentralHeatingPumpActive *bool
	WaterPressureBar         *float64
	ExternalPumpActive       *bool
	CirculationPumpActive    *bool
	GasValveActive           *bool
	FlameActive              *bool
	DiverterValvePositionPct *float64
	FanSpeedRpm              *int
	TargetFanSpeedRpm        *int
	IonisationVoltageUa      *float64
	DhwWaterFlowLpm          *float64
	DhwDemandActive          *bool
	HeatingSwitchActive      *bool
	StorageLoadPumpPct       *float64
	ModulationPct            *float64
	PrimaryCircuitFlowLpm    *float64
	FlowTempDesiredC         *float64
	DhwTempDesiredC          *float64
	StateNumber              *int
	DhwTemperatureC          *float64
	DhwTargetTemperatureC    *float64
}

type BoilerConfig struct {
	DhwOperatingMode  *string
	FlowsetHcMaxC     *float64
	FlowsetHwcMaxC    *float64
	PartloadHcKW      *float64
	PartloadHwcKW     *float64
	InstallerMenuCode *int
	PhoneNumber       *string
	HoursTillService  *int
}

type BoilerDiagnostics struct {
	HeatingStatusRaw         *int
	DhwStatusRaw             *int
	CentralHeatingHours      *float64
	DhwHours                 *float64
	CentralHeatingStarts     *int
	DhwStarts                *int
	PumpHours                *float64
	FanHours                 *float64
	DeactivationsIFC         *int
	DeactivationsTemplimiter *int
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
	MaintenanceDate              *string
	InstallerName1               *string
	InstallerName2               *string
	InstallerPhone1              *string
	InstallerPhone2              *string
	InstallerMenuCode            *int
}

type SystemProperties struct {
	SystemScheme            *int
	ModuleConfigurationVR71 *int
}

type ScheduleTimerSlot struct {
	StartHour      int
	StartMinute    int
	EndHour        int
	EndMinute      int
	TemperatureC   *float64
	TemperatureRaw *int
}

type ScheduleDayProgram struct {
	Weekday string
	Slots   []ScheduleTimerSlot
}

type ScheduleConfig struct {
	MaxSlots       int
	TimeResolution int
	MinDuration    int
	HasTemperature bool
	TempSlots      int
	MinTempC       *float64
	MaxTempC       *float64
}

type ScheduleProgram struct {
	Zone      int
	HC        string
	Config    *ScheduleConfig
	SlotsUsed []int
	Days      []ScheduleDayProgram
}

type ScheduleStatus struct {
	Programs []ScheduleProgram
}

type AdapterHardwareInfo struct {
	FirmwareVersion    string
	FirmwareChecksum   string
	BootloaderVersion  string
	BootloaderChecksum string
	HardwareID         string
	HardwareConfig     string
	Features           byte
	Jumpers            byte
	JumperFlags        []string
	IsWiFi             bool
	IsEthernet         bool
	TemperatureC       *float64
	SupplyVoltageMV    *int
	BusVoltageMaxDV    *int
	BusVoltageMinDV    *int
	ResetCause         *string
	ResetCauseCode     *byte
	RestartCount       *byte
	WiFiRSSIDBm        *int
	LastIdentityQuery  *time.Time
	LastTelemetryQuery *time.Time
	VersionResponseLen int
	InfoSupported      bool
}

type SemanticProvider interface {
	Zones() []Zone
	DHW() *DhwStatus
	Circuits() []CircuitStatus
	RadioDevices() []RadioDevice
	FM5SemanticMode() Fm5SemanticMode
	Solar() *SolarStatus
	Cylinders() []CylinderStatus
	EnergyTotals() *EnergyTotals
	BoilerStatus() *BoilerStatus
	System() *SystemStatus
	Schedules() *ScheduleStatus
	AdapterHardwareInfo() *AdapterHardwareInfo
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

func (staticSemanticProvider) FM5SemanticMode() Fm5SemanticMode {
	return Fm5SemanticModeAbsent
}

func (staticSemanticProvider) Solar() *SolarStatus {
	return nil
}

func (staticSemanticProvider) Cylinders() []CylinderStatus {
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

func (staticSemanticProvider) Schedules() *ScheduleStatus {
	return nil
}

func (staticSemanticProvider) AdapterHardwareInfo() *AdapterHardwareInfo {
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

var liveScheduleSnapshots sync.Map

func (provider *LiveSemanticProvider) Schedules() *ScheduleStatus {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	stored, ok := liveScheduleSnapshots.Load(provider)
	if !ok || stored == nil {
		return nil
	}
	status, ok := stored.(*ScheduleStatus)
	if !ok || status == nil {
		return nil
	}
	return cloneScheduleStatus(status)
}

func (provider *LiveSemanticProvider) SetSchedules(status *ScheduleStatus) {
	if provider == nil {
		return
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if status == nil {
		liveScheduleSnapshots.Delete(provider)
		return
	}
	liveScheduleSnapshots.Store(provider, cloneScheduleStatus(status))
}

var liveAdapterHWInfoSnapshots sync.Map

func (provider *LiveSemanticProvider) AdapterHardwareInfo() *AdapterHardwareInfo {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	stored, ok := liveAdapterHWInfoSnapshots.Load(provider)
	if !ok || stored == nil {
		return nil
	}
	info, ok := stored.(*AdapterHardwareInfo)
	if !ok || info == nil {
		return nil
	}
	return cloneAdapterHardwareInfo(info)
}

func (provider *LiveSemanticProvider) SetAdapterHardwareInfo(info *AdapterHardwareInfo) {
	if provider == nil {
		return
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if info == nil {
		liveAdapterHWInfoSnapshots.Delete(provider)
		return
	}
	liveAdapterHWInfoSnapshots.Store(provider, cloneAdapterHardwareInfo(info))
}

func cloneAdapterHardwareInfo(info *AdapterHardwareInfo) *AdapterHardwareInfo {
	if info == nil {
		return nil
	}
	cp := *info
	if info.JumperFlags != nil {
		cp.JumperFlags = make([]string, len(info.JumperFlags))
		copy(cp.JumperFlags, info.JumperFlags)
	}
	if info.TemperatureC != nil {
		v := *info.TemperatureC
		cp.TemperatureC = &v
	}
	if info.SupplyVoltageMV != nil {
		v := *info.SupplyVoltageMV
		cp.SupplyVoltageMV = &v
	}
	if info.BusVoltageMaxDV != nil {
		v := *info.BusVoltageMaxDV
		cp.BusVoltageMaxDV = &v
	}
	if info.BusVoltageMinDV != nil {
		v := *info.BusVoltageMinDV
		cp.BusVoltageMinDV = &v
	}
	if info.ResetCause != nil {
		v := *info.ResetCause
		cp.ResetCause = &v
	}
	if info.ResetCauseCode != nil {
		v := *info.ResetCauseCode
		cp.ResetCauseCode = &v
	}
	if info.RestartCount != nil {
		v := *info.RestartCount
		cp.RestartCount = &v
	}
	if info.WiFiRSSIDBm != nil {
		v := *info.WiFiRSSIDBm
		cp.WiFiRSSIDBm = &v
	}
	if info.LastIdentityQuery != nil {
		v := *info.LastIdentityQuery
		cp.LastIdentityQuery = &v
	}
	if info.LastTelemetryQuery != nil {
		v := *info.LastTelemetryQuery
		cp.LastTelemetryQuery = &v
	}
	return &cp
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
	if config.MaintenanceDate != nil {
		v := *config.MaintenanceDate
		config.MaintenanceDate = &v
	}
	if config.InstallerName1 != nil {
		v := *config.InstallerName1
		config.InstallerName1 = &v
	}
	if config.InstallerName2 != nil {
		v := *config.InstallerName2
		config.InstallerName2 = &v
	}
	if config.InstallerPhone1 != nil {
		v := *config.InstallerPhone1
		config.InstallerPhone1 = &v
	}
	if config.InstallerPhone2 != nil {
		v := *config.InstallerPhone2
		config.InstallerPhone2 = &v
	}
	if config.InstallerMenuCode != nil {
		v := *config.InstallerMenuCode
		config.InstallerMenuCode = &v
	}
	return config
}

func cloneScheduleStatus(status *ScheduleStatus) *ScheduleStatus {
	if status == nil {
		return nil
	}
	cp := ScheduleStatus{}
	if len(status.Programs) > 0 {
		cp.Programs = make([]ScheduleProgram, len(status.Programs))
		for i, prog := range status.Programs {
			cp.Programs[i] = cloneScheduleProgram(prog)
		}
	}
	return &cp
}

func cloneScheduleProgram(prog ScheduleProgram) ScheduleProgram {
	out := prog
	if prog.Config != nil {
		cfgCopy := *prog.Config
		if prog.Config.MinTempC != nil {
			v := *prog.Config.MinTempC
			cfgCopy.MinTempC = &v
		}
		if prog.Config.MaxTempC != nil {
			v := *prog.Config.MaxTempC
			cfgCopy.MaxTempC = &v
		}
		out.Config = &cfgCopy
	}
	if len(prog.SlotsUsed) > 0 {
		out.SlotsUsed = make([]int, len(prog.SlotsUsed))
		copy(out.SlotsUsed, prog.SlotsUsed)
	}
	if len(prog.Days) > 0 {
		out.Days = make([]ScheduleDayProgram, len(prog.Days))
		for i, day := range prog.Days {
			out.Days[i] = cloneScheduleDayProgram(day)
		}
	}
	return out
}

func cloneScheduleDayProgram(day ScheduleDayProgram) ScheduleDayProgram {
	out := day
	if len(day.Slots) > 0 {
		out.Slots = make([]ScheduleTimerSlot, len(day.Slots))
		for i, slot := range day.Slots {
			out.Slots[i] = cloneScheduleTimerSlot(slot)
		}
	}
	return out
}

func cloneScheduleTimerSlot(slot ScheduleTimerSlot) ScheduleTimerSlot {
	out := slot
	if slot.TemperatureC != nil {
		v := *slot.TemperatureC
		out.TemperatureC = &v
	}
	if slot.TemperatureRaw != nil {
		v := *slot.TemperatureRaw
		out.TemperatureRaw = &v
	}
	return out
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
	return properties
}
