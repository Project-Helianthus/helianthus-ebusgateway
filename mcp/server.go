package mcp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	rpcsource "github.com/Project-Helianthus/helianthus-ebusgateway/internal/rpc_source"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	ebusstdcat "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

type Registry interface {
	Iterate(func(registry.DeviceEntry) bool)
	Lookup(address byte) (registry.DeviceEntry, bool)
	// IterateSnapshots is the value-typed counterpart to Iterate
	// (added in helianthus-ebusreg P9). Each callback invocation
	// receives a DeviceEntrySnapshot taken under the registry's
	// RLock; the lock is released before the callback runs.
	// Race-free reads of identity fields without lock-free
	// dereferencing of live *deviceEntry pointers.
	IterateSnapshots(func(registry.DeviceEntrySnapshot) bool)
	// LookupEntrySnapshot is the value-typed counterpart to Lookup
	// (added in helianthus-ebusreg P9). Returns a value copy of the
	// entry's identity fields under the registry's RLock; race-free
	// for callers that don't need the live pointer or Planes /
	// Projections.
	LookupEntrySnapshot(address byte) (registry.DeviceEntrySnapshot, bool)
	// LookupSlot exposes the AddressSlot enum state so MCP responses
	// can project discovery_source / verification_state into
	// JSON-serializable strings (P3.5). Mirrors the same method on
	// *registry.DeviceRegistry.
	//
	// DEPRECATED for label projection: prefer LookupSlotSnapshot for
	// any code that reads `slot.DiscoverySource` / `.VerificationState`
	// outside the registry's lock. The live pointer can be mutated
	// concurrently by Register / RegisterPassiveObserved / MarkSlot*
	// callers; reading the int-typed enums is atomic in practice on
	// supported architectures but the race detector flags it under
	// contention. Retained for callers that need a stable pointer
	// for FirstObservedAt walks or device-attached probes.
	LookupSlot(address byte) (*registry.AddressSlot, bool)
	// LookupSlotSnapshot returns a value-typed snapshot of the slot
	// taken under the registry's RLock. Callers can read snapshot
	// fields without race risk (P8.1 / P8.3). Mirrors the same
	// method on *registry.DeviceRegistry.
	LookupSlotSnapshot(address byte) (registry.AddressSlotSnapshot, bool)
}

type Invoker interface {
	Invoke(ctx context.Context, plane router.Plane, methodName string, params map[string]any) (any, error)
}

type ServiceStatus struct {
	Status           string `json:"status"`
	FirmwareVersion  string `json:"firmware_version"`
	UpdatesAvailable bool   `json:"updates_available"`
	InitiatorAddress string `json:"initiator_address,omitempty"`
}

type StatusProvider interface {
	DaemonStatus() ServiceStatus
	AdapterStatus() ServiceStatus
}

type ZoneState struct {
	CurrentTempC       *float64 `json:"current_temp_c,omitempty"`
	CurrentHumidityPct *float64 `json:"current_humidity_pct,omitempty"`
	HvacAction         string   `json:"hvac_action,omitempty"`
	SpecialFunction    string   `json:"special_function,omitempty"`
	HeatingDemandPct   *float64 `json:"heating_demand_pct,omitempty"`
	ValvePositionPct   *float64 `json:"valve_position_pct,omitempty"`
}

type ZoneConfig struct {
	OperatingMode              string   `json:"operating_mode"`
	Preset                     string   `json:"preset"`
	TargetTempC                *float64 `json:"target_temp_c,omitempty"`
	AllowedModes               []string `json:"allowed_modes,omitempty"`
	CircuitType                string   `json:"circuit_type,omitempty"`
	AssociatedCircuit          *int     `json:"associated_circuit,omitempty"`
	RoomTemperatureZoneMapping *int     `json:"room_temperature_zone_mapping,omitempty"`
	QuickVeto                  bool     `json:"quick_veto"`
	QuickVetoSetpointC         *float64 `json:"quick_veto_setpoint_c,omitempty"`
	QuickVetoDurationH         *float64 `json:"quick_veto_duration_h,omitempty"`
	QuickVetoExpiry            string   `json:"quick_veto_expiry,omitempty"`
	HolidayStartDate           string   `json:"holiday_start_date,omitempty"`
	HolidayEndDate             string   `json:"holiday_end_date,omitempty"`
	HolidaySetpointC           *float64 `json:"holiday_setpoint_c,omitempty"`
	HolidayStartTime           string   `json:"holiday_start_time,omitempty"`
	HolidayEndTime             string   `json:"holiday_end_time,omitempty"`
}

type Zone struct {
	ID     string     `json:"id"`
	Name   string     `json:"name"`
	State  ZoneState  `json:"state"`
	Config ZoneConfig `json:"config"`
}

type DhwState struct {
	CurrentTempC     *float64 `json:"current_temp_c,omitempty"`
	SpecialFunction  string   `json:"special_function,omitempty"`
	HeatingDemandPct *float64 `json:"heating_demand_pct,omitempty"`
}

type DhwConfig struct {
	OperatingMode    string   `json:"operating_mode"`
	Preset           string   `json:"preset"`
	TargetTempC      *float64 `json:"target_temp_c,omitempty"`
	HolidayStartDate string   `json:"holiday_start_date,omitempty"`
	HolidayEndDate   string   `json:"holiday_end_date,omitempty"`
}

type DhwStatus struct {
	State  DhwState  `json:"state"`
	Config DhwConfig `json:"config"`
}

type EnergySeries struct {
	Today       float64           `json:"today"`
	Yearly      []float64         `json:"yearly"`
	Monthly     []float64         `json:"monthly,omitempty"`
	TodayMeta   EnergyPointMeta   `json:"today_meta"`
	YearlyMeta  []EnergyPointMeta `json:"yearly_meta,omitempty"`
	MonthlyMeta []EnergyPointMeta `json:"monthly_meta,omitempty"`
}

type EnergyPointMeta struct {
	FreshnessState  string  `json:"freshness_state"`
	Provenance      string  `json:"provenance"`
	LastObservedUTC string  `json:"last_observed_utc,omitempty"`
	AgeSeconds      float64 `json:"age_seconds,omitempty"`
	Stale           bool    `json:"stale"`
}

type EnergyChannel struct {
	DHW     EnergySeries `json:"dhw"`
	Climate EnergySeries `json:"climate"`
}

type EnergyTotals struct {
	Gas      EnergyChannel `json:"gas"`
	Electric EnergyChannel `json:"electric"`
	Solar    EnergyChannel `json:"solar"`
}

type BoilerState struct {
	FlowTemperatureC         *float64 `json:"flow_temperature_c,omitempty"`
	ReturnTemperatureC       *float64 `json:"return_temperature_c,omitempty"`
	CentralHeatingPumpActive *bool    `json:"central_heating_pump_active,omitempty"`
	WaterPressureBar         *float64 `json:"water_pressure_bar,omitempty"`
	ExternalPumpActive       *bool    `json:"external_pump_active,omitempty"`
	CirculationPumpActive    *bool    `json:"circulation_pump_active,omitempty"`
	GasValveActive           *bool    `json:"gas_valve_active,omitempty"`
	FlameActive              *bool    `json:"flame_active,omitempty"`
	DiverterValvePositionPct *float64 `json:"diverter_valve_position_pct,omitempty"`
	FanSpeedRpm              *int     `json:"fan_speed_rpm,omitempty"`
	TargetFanSpeedRpm        *int     `json:"target_fan_speed_rpm,omitempty"`
	IonisationVoltageUa      *float64 `json:"ionisation_voltage_ua,omitempty"`
	DhwWaterFlowLpm          *float64 `json:"dhw_water_flow_lpm,omitempty"`
	DhwDemandActive          *bool    `json:"dhw_demand_active,omitempty"`
	HeatingSwitchActive      *bool    `json:"heating_switch_active,omitempty"`
	StorageLoadPumpPct       *float64 `json:"storage_load_pump_pct,omitempty"`
	ModulationPct            *float64 `json:"modulation_pct,omitempty"`
	PrimaryCircuitFlowLpm    *float64 `json:"primary_circuit_flow_lpm,omitempty"`
	FlowTempDesiredC         *float64 `json:"flow_temp_desired_c,omitempty"`
	DhwTempDesiredC          *float64 `json:"dhw_temp_desired_c,omitempty"`
	StateNumber              *int     `json:"state_number,omitempty"`
	DhwTemperatureC          *float64 `json:"dhw_temperature_c,omitempty"`
	DhwTargetTemperatureC    *float64 `json:"dhw_target_temperature_c,omitempty"`
}

type BoilerConfig struct {
	DhwOperatingMode  *string  `json:"dhw_operating_mode,omitempty"`
	FlowsetHcMaxC     *float64 `json:"flowset_hc_max_c,omitempty"`
	FlowsetHwcMaxC    *float64 `json:"flowset_hwc_max_c,omitempty"`
	PartloadHcKW      *float64 `json:"partload_hc_kw,omitempty"`
	PartloadHwcKW     *float64 `json:"partload_hwc_kw,omitempty"`
	InstallerMenuCode *int     `json:"installer_menu_code,omitempty"`
	PhoneNumber       *string  `json:"phone_number,omitempty"`
	HoursTillService  *int     `json:"hours_till_service,omitempty"`
}

type BoilerDiagnostics struct {
	HeatingStatusRaw         *int     `json:"heating_status_raw,omitempty"`
	DhwStatusRaw             *int     `json:"dhw_status_raw,omitempty"`
	CentralHeatingHours      *float64 `json:"central_heating_hours,omitempty"`
	DhwHours                 *float64 `json:"dhw_hours,omitempty"`
	CentralHeatingStarts     *int     `json:"central_heating_starts,omitempty"`
	DhwStarts                *int     `json:"dhw_starts,omitempty"`
	PumpHours                *float64 `json:"pump_hours,omitempty"`
	FanHours                 *float64 `json:"fan_hours,omitempty"`
	DeactivationsIFC         *int     `json:"deactivations_ifc,omitempty"`
	DeactivationsTemplimiter *int     `json:"deactivations_templimiter,omitempty"`
}

type BoilerStatus struct {
	State       *BoilerState       `json:"state,omitempty"`
	Config      *BoilerConfig      `json:"config,omitempty"`
	Diagnostics *BoilerDiagnostics `json:"diagnostics,omitempty"`
}

type SystemState struct {
	SystemOff                    *bool    `json:"system_off,omitempty"`
	SystemWaterPressure          *float64 `json:"system_water_pressure,omitempty"`
	SystemFlowTemperature        *float64 `json:"system_flow_temperature,omitempty"`
	OutdoorTemperature           *float64 `json:"outdoor_temperature,omitempty"`
	OutdoorTemperatureAvg24h     *float64 `json:"outdoor_temperature_avg24h,omitempty"`
	MaintenanceDue               *bool    `json:"maintenance_due,omitempty"`
	HwcCylinderTemperatureTop    *float64 `json:"hwc_cylinder_temperature_top,omitempty"`
	HwcCylinderTemperatureBottom *float64 `json:"hwc_cylinder_temperature_bottom,omitempty"`
}

type SystemConfig struct {
	AdaptiveHeatingCurve         *bool    `json:"adaptive_heating_curve,omitempty"`
	AlternativePoint             *float64 `json:"alternative_point,omitempty"`
	HeatingCircuitBivalencePoint *float64 `json:"heating_circuit_bivalence_point,omitempty"`
	DhwBivalencePoint            *float64 `json:"dhw_bivalence_point,omitempty"`
	HcEmergencyTemperature       *float64 `json:"hc_emergency_temperature,omitempty"`
	HwcMaxFlowTempDesired        *float64 `json:"hwc_max_flow_temp_desired,omitempty"`
	MaxRoomHumidity              *int     `json:"max_room_humidity,omitempty"`
	MaintenanceDate              *string  `json:"maintenance_date,omitempty"`
	InstallerName                *string  `json:"installer_name,omitempty"`
	InstallerPhone               *string  `json:"installer_phone,omitempty"`
	InstallerMenuCode            *int     `json:"installer_menu_code,omitempty"`
}

type SystemProperties struct {
	SystemScheme            *int `json:"system_scheme,omitempty"`
	ModuleConfigurationVR71 *int `json:"module_configuration_vr71,omitempty"`
}

type SystemStatus struct {
	State      *SystemState      `json:"state,omitempty"`
	Config     *SystemConfig     `json:"config,omitempty"`
	Properties *SystemProperties `json:"properties,omitempty"`
}

type CircuitState struct {
	PumpActive       *bool    `json:"pump_active,omitempty"`
	MixerPositionPct *float64 `json:"mixer_position_pct,omitempty"`
	FlowTemperatureC *float64 `json:"flow_temperature_c,omitempty"`
	FlowSetpointC    *float64 `json:"flow_setpoint_c,omitempty"`
	CalcFlowTempC    *float64 `json:"calc_flow_temp_c,omitempty"`
	CircuitState     string   `json:"circuit_state,omitempty"`
	Humidity         *float64 `json:"humidity,omitempty"`
	DewPoint         *float64 `json:"dew_point,omitempty"`
	PumpHours        *float64 `json:"pump_hours,omitempty"`
	PumpStarts       *int     `json:"pump_starts,omitempty"`
}

type CircuitConfig struct {
	HeatingCurve    *float64 `json:"heating_curve,omitempty"`
	FlowTempMaxC    *float64 `json:"flow_temp_max_c,omitempty"`
	FlowTempMinC    *float64 `json:"flow_temp_min_c,omitempty"`
	SummerLimitC    *float64 `json:"summer_limit_c,omitempty"`
	FrostProtC      *float64 `json:"frost_prot_c,omitempty"`
	RoomTempControl string   `json:"room_temp_control,omitempty"`
	CoolingEnabled  *bool    `json:"cooling_enabled,omitempty"`
}

type ManagingDevice struct {
	Role     string  `json:"role"`
	DeviceID *string `json:"device_id,omitempty"`
	Address  *int    `json:"address,omitempty"`
}

type CircuitStatus struct {
	Index          int            `json:"index"`
	CircuitType    string         `json:"circuit_type"`
	HasMixer       bool           `json:"has_mixer"`
	State          CircuitState   `json:"state"`
	Config         CircuitConfig  `json:"config"`
	ManagingDevice ManagingDevice `json:"managing_device"`
}

type RadioDevice struct {
	Group                int      `json:"group"`
	Instance             int      `json:"instance"`
	SlotMode             string   `json:"slot_mode"`
	DeviceConnected      *bool    `json:"device_connected,omitempty"`
	DeviceClassAddress   *int     `json:"device_class_address,omitempty"`
	DeviceModel          string   `json:"device_model,omitempty"`
	FirmwareVersion      *string  `json:"firmware_version,omitempty"`
	HardwareIdentifier   *int     `json:"hardware_identifier,omitempty"`
	RemoteControlAddress *int     `json:"remote_control_address,omitempty"`
	DevicePaired         *bool    `json:"device_paired,omitempty"`
	ReceptionStrength    *int     `json:"reception_strength,omitempty"`
	ZoneAssignment       *int     `json:"zone_assignment,omitempty"`
	RoomTemperatureC     *float64 `json:"room_temperature_c,omitempty"`
	RoomHumidityPct      *float64 `json:"room_humidity_pct,omitempty"`
}

type Fm5SemanticMode string

const (
	Fm5SemanticModeInterpreted Fm5SemanticMode = "INTERPRETED"
	Fm5SemanticModeGPIOOnly    Fm5SemanticMode = "GPIO_ONLY"
	Fm5SemanticModeAbsent      Fm5SemanticMode = "ABSENT"
)

type Fm5SemanticDegradedReason string

type Fm5Interpretation struct {
	Mode             Fm5SemanticMode            `json:"mode"`
	DegradedReason   *Fm5SemanticDegradedReason `json:"degraded_reason"`
	EvidenceRevision string                     `json:"evidence_revision"`
}

func (value Fm5Interpretation) validate() error {
	if strings.TrimSpace(value.EvidenceRevision) == "" {
		return errors.New("fm5 evidence revision is required")
	}
	if value.Mode == Fm5SemanticModeGPIOOnly {
		if value.DegradedReason == nil || *value.DegradedReason != "CONFIGURATION_NOT_INTERPRETABLE" {
			return errors.New("fm5 GPIO_ONLY requires CONFIGURATION_NOT_INTERPRETABLE")
		}
		return nil
	}
	if value.Mode != Fm5SemanticModeInterpreted && value.Mode != Fm5SemanticModeAbsent {
		return errors.New("invalid fm5 semantic mode")
	}
	if value.DegradedReason == nil {
		return nil
	}
	switch *value.DegradedReason {
	case "CONTROLLER_UNREACHABLE",
		"CONFIGURATION_UNAVAILABLE",
		"SOLAR_ACQUISITION_FAILED",
		"CYLINDER_ACQUISITION_FAILED",
		"EVIDENCE_STALE",
		"INCOHERENT_ACQUISITION":
		return nil
	default:
		return errors.New("fm5 retained structural mode requires a transient degraded reason")
	}
}

func legacyFM5Interpretation(mode Fm5SemanticMode) Fm5Interpretation {
	if mode == "" {
		mode = Fm5SemanticModeAbsent
	}
	verdict := Fm5Interpretation{Mode: mode, EvidenceRevision: "legacy"}
	if mode == Fm5SemanticModeGPIOOnly {
		reason := Fm5SemanticDegradedReason("CONFIGURATION_NOT_INTERPRETABLE")
		verdict.DegradedReason = &reason
	}
	return verdict
}

type FM5InterpretationProvider interface {
	FM5Interpretation() Fm5Interpretation
}

type SolarStatus struct {
	CollectorTemperatureC *float64 `json:"collector_temperature_c,omitempty"`
	ReturnTemperatureC    *float64 `json:"return_temperature_c,omitempty"`
	PumpActive            *bool    `json:"pump_active,omitempty"`
	CurrentYield          *float64 `json:"current_yield,omitempty"`
	PumpHours             *float64 `json:"pump_hours,omitempty"`
	SolarEnabled          *bool    `json:"solar_enabled,omitempty"`
	FunctionMode          *bool    `json:"function_mode,omitempty"`
}

type CylinderStatus struct {
	Index             int      `json:"index"`
	TemperatureC      *float64 `json:"temperature_c,omitempty"`
	MaxSetpointC      *float64 `json:"max_setpoint_c,omitempty"`
	ChargeHysteresisC *float64 `json:"charge_hysteresis_c,omitempty"`
	ChargeOffsetC     *float64 `json:"charge_offset_c,omitempty"`
}

type ScheduleTimerSlot struct {
	StartHour      int      `json:"start_hour"`
	StartMinute    int      `json:"start_minute"`
	EndHour        int      `json:"end_hour"`
	EndMinute      int      `json:"end_minute"`
	TemperatureC   *float64 `json:"temperature_c,omitempty"`
	TemperatureRaw *int     `json:"temperature_raw,omitempty"`
}

type ScheduleDayProgram struct {
	Weekday string              `json:"weekday"`
	Slots   []ScheduleTimerSlot `json:"slots"`
}

type ScheduleConfig struct {
	MaxSlots       int      `json:"max_slots"`
	TimeResolution int      `json:"time_resolution"`
	MinDuration    int      `json:"min_duration"`
	HasTemperature bool     `json:"has_temperature"`
	TempSlots      int      `json:"temp_slots"`
	MinTempC       *float64 `json:"min_temp_c,omitempty"`
	MaxTempC       *float64 `json:"max_temp_c,omitempty"`
}

type ScheduleProgram struct {
	Zone      int                  `json:"zone"`
	HC        string               `json:"hc"`
	Config    *ScheduleConfig      `json:"config,omitempty"`
	SlotsUsed []int                `json:"slots_used,omitempty"`
	Days      []ScheduleDayProgram `json:"days,omitempty"`
}

type ScheduleStatus struct {
	Programs []ScheduleProgram `json:"programs"`
}

type TimeProgramSlot struct {
	StartHour    int      `json:"start_hour"`
	StartMinute  int      `json:"start_minute"`
	EndHour      int      `json:"end_hour"`
	EndMinute    int      `json:"end_minute"`
	TemperatureC *float64 `json:"temperature_c,omitempty"`
}

type TimeProgramSlotResult struct {
	SlotIndex int    `json:"slot_index"`
	Accepted  bool   `json:"accepted"`
	ErrorCode int    `json:"error_code"`
	ErrorDesc string `json:"error_description,omitempty"`
}

type TimeProgramWriteResult struct {
	Success     bool                    `json:"success"`
	SlotResults []TimeProgramSlotResult `json:"slot_results"`
	Error       string                  `json:"error,omitempty"`
}

type ScheduleWriter interface {
	SetZoneTimeProgram(ctx context.Context, zone int, weekday int, slots []TimeProgramSlot) (*TimeProgramWriteResult, error)
	SetDhwTimeProgram(ctx context.Context, weekday int, slots []TimeProgramSlot) (*TimeProgramWriteResult, error)
}

type ConfigSetResult struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type ConfigWriter interface {
	SetSystemConfig(ctx context.Context, field string, value string) ConfigSetResult
	SetBoilerConfig(ctx context.Context, field string, value string) ConfigSetResult
}

type AdapterHardwareInfo struct {
	FirmwareVersion    string   `json:"firmware_version"`
	FirmwareChecksum   string   `json:"firmware_checksum"`
	BootloaderVersion  string   `json:"bootloader_version"`
	BootloaderChecksum string   `json:"bootloader_checksum"`
	HardwareID         string   `json:"hardware_id"`
	HardwareConfig     string   `json:"hardware_config"`
	Features           byte     `json:"features"`
	Jumpers            byte     `json:"jumpers"`
	JumperFlags        []string `json:"jumper_flags"`
	IsWiFi             bool     `json:"is_wifi"`
	IsEthernet         bool     `json:"is_ethernet"`
	TemperatureC       *float64 `json:"temperature_c,omitempty"`
	SupplyVoltageMV    *int     `json:"supply_voltage_mv,omitempty"`
	BusVoltageMaxDV    *int     `json:"bus_voltage_max_dv,omitempty"`
	BusVoltageMinDV    *int     `json:"bus_voltage_min_dv,omitempty"`
	ResetCause         *string  `json:"reset_cause,omitempty"`
	ResetCauseCode     *byte    `json:"reset_cause_code,omitempty"`
	RestartCount       *byte    `json:"restart_count,omitempty"`
	WiFiRSSIDBm        *int     `json:"wifi_rssi_dbm,omitempty"`
	LastIdentityQuery  *string  `json:"last_identity_query,omitempty"`
	LastTelemetryQuery *string  `json:"last_telemetry_query,omitempty"`
	VersionResponseLen int      `json:"version_response_len"`
	InfoSupported      bool     `json:"info_supported"`
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

type Server struct {
	registry                    Registry
	invoker                     Invoker
	statusProvider              StatusProvider
	bus                         BusObservabilityProvider
	watch                       WatchSummaryProvider
	semantic                    SemanticProvider
	scheduleWriter              ScheduleWriter
	configWriter                ConfigWriter
	rpcSource                   byte
	rpcSourceAdmitted           bool
	rpcSourceProvider           func() (byte, bool)
	idempotencyMu               sync.Mutex
	idempotency                 map[string]idempotencyEntry
	snapshotMu                  sync.RWMutex
	snapshots                   map[string]snapshotState
	eebusV1Mu                   sync.RWMutex
	eebusV1                     *eebusV1Runtime
	eebusV1CommandRouter        EEBusV1CommandRouter
	synchronizedEvidenceCapture SynchronizedEvidenceCapture
	leafPromotionCapture        LeafPromotionCapture

	tools []Tool

	// ebusStandardServer dispatches the four ebus_standard MCP surfaces
	// (services.list, commands.list, command.get, decode). Installed via
	// RegisterEbusStandardTools during bootstrap; nil when disabled.
	ebusStandardServer EbusStandardSubServer
}

const (
	toolRuntimeStatusGetName             = "ebus.v1.runtime.status.get"
	toolBusSummaryGetName                = "ebus.v1.bus.summary.get"
	toolBusMessagesListName              = "ebus.v1.bus.messages.list"
	toolBusPeriodicityListName           = "ebus.v1.bus.periodicity.list"
	toolBusProtocolSpecimensListName     = "ebus.v1.bus.protocol_specimens.list"
	toolWatchSummaryGetName              = "ebus.v1.watch.summary.get"
	toolSemanticZonesGetName             = "ebus.v1.semantic.zones.get"
	toolSemanticCircuitsGetName          = "ebus.v1.semantic.circuits.get"
	toolSemanticRadioGetName             = "ebus.v1.semantic.radio_devices.get"
	toolSemanticFM5ModeGetName           = "ebus.v1.semantic.fm5_mode.get"
	toolSemanticFM5InterpretationGetName = "ebus.v1.semantic.fm5_interpretation.get"
	toolSemanticSolarGetName             = "ebus.v1.semantic.solar.get"
	toolSemanticCylindersGetName         = "ebus.v1.semantic.cylinders.get"
	toolSemanticDHWGetName               = "ebus.v1.semantic.dhw.get"
	toolSemanticEnergyGetName            = "ebus.v1.semantic.energy_totals.get"
	toolSemanticBoilerGetName            = "ebus.v1.semantic.boiler_status.get"
	toolSemanticSystemGetName            = "ebus.v1.semantic.system.get"
	toolSemanticSchedulesGetName         = "ebus.v1.semantic.schedules.get"
	toolSemanticSchedulesSetZoneName     = "ebus.v1.semantic.schedules.set_zone_time_program"
	toolSemanticSchedulesSetDhwName      = "ebus.v1.semantic.schedules.set_dhw_time_program"
	toolSemanticSystemSetConfigName      = "ebus.v1.semantic.system.set_config"
	toolSemanticBoilerSetConfigName      = "ebus.v1.semantic.boiler_status.set_config"
	toolSemanticAdapterInfoGetName       = "ebus.v1.semantic.adapter_info.get"
	toolSemanticSnapshotName             = "ebus.v1.semantic.snapshot.get"
	toolSnapshotCaptureName              = "ebus.v1.snapshot.capture"
	toolSnapshotDropName                 = "ebus.v1.snapshot.drop"
	toolDevicesV1Name                    = "ebus.v1.registry.devices.list"
	toolDeviceGetV1Name                  = "ebus.v1.registry.devices.get"
	toolPlanesListV1Name                 = "ebus.v1.registry.planes.list"
	toolMethodsListV1Name                = "ebus.v1.registry.methods.list"
	toolInvokeV1Name                     = "ebus.v1.rpc.invoke"
	toolDevicesLegacyName                = "ebus.devices"
	toolInvokeLegacyName                 = "ebus.invoke"
	methodMutabilityUnknown              = "unknown"
	methodMutabilityReadOnly             = "read_only"
	methodMutabilityMutating             = "mutating"
	methodDangerUnknown                  = "unknown"
	methodDangerSafe                     = "safe"
	methodDangerDangerous                = "dangerous"
	defaultInvokeTimeout                 = 3 * time.Second
	defaultIdempotencyTTL                = 30 * time.Second
	defaultSnapshotTTL                   = 5 * time.Minute
	defaultSnapshotReadTTL               = 10 * time.Second
)

var errInvokePermissionDenied = errors.New("invoke permission denied")
var errInvokeIdempotencyConflict = errors.New("invoke idempotency conflict")
var errSnapshotNotFound = errors.New("snapshot not found")

type staticStatusProvider struct{}
type staticSemanticProvider struct{}
type staticWatchSummaryProvider struct{}

func (staticStatusProvider) DaemonStatus() ServiceStatus {
	return ServiceStatus{
		Status:           "running",
		FirmwareVersion:  "",
		UpdatesAvailable: false,
		InitiatorAddress: "",
	}
}

func (staticStatusProvider) AdapterStatus() ServiceStatus {
	return ServiceStatus{
		Status:           "unknown",
		FirmwareVersion:  "",
		UpdatesAvailable: false,
	}
}

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

func (staticWatchSummaryProvider) Snapshot() WatchSummary {
	return WatchSummary{}
}

type idempotencyEntry struct {
	signature string
	result    any
	expiresAt time.Time
}

type invokeV1Policy struct {
	intent         string
	idempotencyKey string
	timeout        time.Duration
}

type envelopeConsistency struct {
	mode          string
	snapshotID    string
	dataTimestamp time.Time
}

type snapshotState struct {
	id        string
	createdAt time.Time
	expiresAt time.Time

	runtime        map[string]any
	busSummary     *BusSummary
	busMessages    []BusMessage
	busPeriodicity []BusPeriodicityEntry
	watchSummary   *WatchSummary
	zones          []Zone
	circuits       []CircuitStatus
	radio          []RadioDevice
	fm5Mode        Fm5SemanticMode
	fm5Verdict     Fm5Interpretation
	solar          *SolarStatus
	cylinders      []CylinderStatus
	dhw            *DhwStatus
	energy         *EnergyTotals
	boiler         *BoilerStatus
	system         *SystemStatus
	schedules      *ScheduleStatus
	adapterInfo    *AdapterHardwareInfo
	devices        []deviceInfo
	// addressSlots captures per-address discovery_source/verification_state
	// labels at snapshot creation time so devices.get(address=alias)
	// in SNAPSHOT mode returns the queried alias's labels (not the
	// primary's, which is what the cached deviceInfo carries).
	// Codex P3.5 review pass 4: without this, a merged DeviceEntry
	// whose aliases sit at different DiscoverySource levels would
	// project the primary's labels for all aliased queries in
	// SNAPSHOT mode, while LIVE mode correctly projected the queried
	// alias.
	addressSlots map[byte]addressSlotLabels
}

type addressSlotLabels struct {
	discovery    string
	verification string
}

func consistencyInputProperty() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"mode":        map[string]any{"type": "string", "enum": []string{"LIVE", "SNAPSHOT"}},
			"snapshot_id": map[string]any{"type": "string"},
		},
		"additionalProperties": false,
	}
}

func busObservabilityTools() []Tool {
	return []Tool{
		{
			Name:        toolBusSummaryGetName,
			Description: "Get observe-first bus capability, warmup, degraded, and bounded-list summary state.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolBusMessagesListName,
			Description: "List bounded recent bus-message records from the observe-first store.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"limit":       map[string]any{"type": "integer", "minimum": 1},
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolBusPeriodicityListName,
			Description: "List bounded observe-first bus periodicity summaries.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"limit":       map[string]any{"type": "integer", "minimum": 1},
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolBusProtocolSpecimensListName,
			Description: "List protocol specimen entries captured from passively observed eBUS frames for protocol families the gateway does not implement.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"family": map[string]any{"type": "string"},
					"limit":  map[string]any{"type": "integer", "minimum": 1},
				},
				"additionalProperties": false,
			},
		},
	}
}

func watchSummaryTools() []Tool {
	return []Tool{
		{
			Name:        toolWatchSummaryGetName,
			Description: "Get watch-summary surfaces computed from the ShadowCache projection.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
	}
}

func NewServer(reg Registry, invoker Invoker) (*Server, error) {
	if reg == nil {
		return nil, fmt.Errorf("mcp server missing registry: %w", ebuserrors.ErrInvalidPayload)
	}

	server := &Server{
		registry:       reg,
		invoker:        invoker,
		statusProvider: staticStatusProvider{},
		watch:          staticWatchSummaryProvider{},
		semantic:       staticSemanticProvider{},
		idempotency:    make(map[string]idempotencyEntry),
		snapshots:      make(map[string]snapshotState),
	}
	server.tools = []Tool{
		{
			Name:        toolRuntimeStatusGetName,
			Description: "Get runtime daemon and adapter status.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticZonesGetName,
			Description: "Get semantic zones snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticCircuitsGetName,
			Description: "Get semantic heating circuits snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticRadioGetName,
			Description: "Get semantic remote-slot radio devices snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticFM5ModeGetName,
			Description: "Get semantic FM5 mode snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticFM5InterpretationGetName,
			Description: "Get the atomic semantic FM5 mode, degraded reason, and evidence revision.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSolarGetName,
			Description: "Get semantic solar snapshot (interpreted mode only).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticCylindersGetName,
			Description: "Get semantic cylinders snapshot (interpreted mode only).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticDHWGetName,
			Description: "Get semantic domestic hot water snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticEnergyGetName,
			Description: "Get semantic energy totals snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticBoilerGetName,
			Description: "Get semantic boiler status snapshot (flow/return temps, pump, diagnostics).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSystemGetName,
			Description: "Get semantic system status snapshot (outdoor temp, water pressure, flow temp, maintenance, config).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticAdapterInfoGetName,
			Description: "Get adapter hardware identity and telemetry (firmware, temperature, voltages, WiFi RSSI, reset info).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSchedulesGetName,
			Description: "Get semantic weekly timer schedules snapshot (B555 protocol).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSchedulesSetZoneName,
			Description: "Write a zone heating time program for a specific weekday (B555 protocol). Writes individual slots sequentially (SC=1 per slot for reliability).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"zone":    map[string]any{"type": "integer", "minimum": 0, "maximum": 2},
					"weekday": map[string]any{"type": "integer", "minimum": 0, "maximum": 6, "description": "0=Monday, 6=Sunday"},
					"slots": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"start_hour":    map[string]any{"type": "integer", "minimum": 0, "maximum": 24},
								"start_minute":  map[string]any{"type": "integer", "minimum": 0, "maximum": 59},
								"end_hour":      map[string]any{"type": "integer", "minimum": 0, "maximum": 24},
								"end_minute":    map[string]any{"type": "integer", "minimum": 0, "maximum": 59},
								"temperature_c": map[string]any{"type": "number"},
							},
							"required": []string{"start_hour", "start_minute", "end_hour", "end_minute", "temperature_c"},
						},
					},
				},
				"required":             []string{"zone", "weekday", "slots"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSchedulesSetDhwName,
			Description: "Write a DHW (domestic hot water) time program for a specific weekday (B555 protocol). Temperature is optional — omit to keep current B524 setpoint (0xFFFF sentinel).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"weekday": map[string]any{"type": "integer", "minimum": 0, "maximum": 6, "description": "0=Monday, 6=Sunday"},
					"slots": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"start_hour":    map[string]any{"type": "integer", "minimum": 0, "maximum": 24},
								"start_minute":  map[string]any{"type": "integer", "minimum": 0, "maximum": 59},
								"end_hour":      map[string]any{"type": "integer", "minimum": 0, "maximum": 24},
								"end_minute":    map[string]any{"type": "integer", "minimum": 0, "maximum": 59},
								"temperature_c": map[string]any{"type": "number"},
							},
							"required": []string{"start_hour", "start_minute", "end_hour", "end_minute"},
						},
					},
				},
				"required":             []string{"weekday", "slots"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSystemSetConfigName,
			Description: "Write a system configuration field (B524 controller). Accepts camelCase field names matching GraphQL mutation fields.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"field": map[string]any{"type": "string", "description": "camelCase field name (e.g. installerName1, maintenanceDate, installerMenuCode)"},
					"value": map[string]any{"type": "string", "description": "Value to write (string representation)"},
				},
				"required":             []string{"field", "value"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticBoilerSetConfigName,
			Description: "Write a boiler configuration field (B509 BAI00). Accepts camelCase field names matching GraphQL mutation fields.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"field": map[string]any{"type": "string", "description": "camelCase field name (e.g. installerMenuCode, phoneNumber)"},
					"value": map[string]any{"type": "string", "description": "Value to write (string representation)"},
				},
				"required":             []string{"field", "value"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSemanticSnapshotName,
			Description: "Get a consistent semantic snapshot across selected semantic planes.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"planes": map[string]any{
						"type": "array",
						"items": map[string]any{
							"type": "string",
							"enum": []string{"runtime_status", "zones", "dhw", "energy_totals", "boiler_status", "system", "circuits", "radio_devices", "fm5_mode", "solar", "cylinders", "schedules", "adapter_info"},
						},
					},
					"timeout_ms": map[string]any{"type": "integer", "minimum": 1},
					"allow_partial": map[string]any{
						"type": "boolean",
					},
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolSnapshotCaptureName,
			Description: "Capture a read snapshot for deterministic MCP reads.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		},
		{
			Name:        toolSnapshotDropName,
			Description: "Drop a previously captured snapshot.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"snapshot_id": map[string]any{"type": "string"},
				},
				"required":             []string{"snapshot_id"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolDevicesV1Name,
			Description: "List devices discovered on the eBUS, including planes and methods.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolDeviceGetV1Name,
			Description: "Get one device by address.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address":     map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"consistency": consistencyInputProperty(),
				},
				"required":             []string{"address"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolPlanesListV1Name,
			Description: "List registry planes for one device address.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address":     map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"consistency": consistencyInputProperty(),
				},
				"required":             []string{"address"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolMethodsListV1Name,
			Description: "List registry methods for a device address and plane.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address":     map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"plane":       map[string]any{"type": "string"},
					"consistency": consistencyInputProperty(),
				},
				"required":             []string{"address", "plane"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolInvokeV1Name,
			Description: "Invoke a plane method on a device.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address":         map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"plane":           map[string]any{"type": "string"},
					"method":          map[string]any{"type": "string"},
					"params":          map[string]any{"type": "object"},
					"intent":          map[string]any{"type": "string", "enum": []string{"READ_ONLY", "MUTATE"}},
					"allow_dangerous": map[string]any{"type": "boolean"},
					"idempotency_key": map[string]any{"type": "string"},
					"timeout_ms":      map[string]any{"type": "integer", "minimum": 1},
				},
				"required":             []string{"address", "plane", "method", "intent", "allow_dangerous"},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolDevicesLegacyName,
			Description: "Compatibility alias for ebus.v1.registry.devices.list.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"consistency": consistencyInputProperty(),
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        toolInvokeLegacyName,
			Description: "Compatibility alias for ebus.v1.rpc.invoke.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"address": map[string]any{"type": "integer", "minimum": 0, "maximum": 255},
					"plane":   map[string]any{"type": "string"},
					"method":  map[string]any{"type": "string"},
					"params":  map[string]any{"type": "object"},
				},
				"required":             []string{"address", "plane", "method"},
				"additionalProperties": false,
			},
		},
	}

	// Wire the ebus_standard L7 MCP surfaces (M4_GATEWAY_MCP). The
	// embedded catalog is SHA256-pinned and consumed read-only; see
	// mcp/ebus_standard_wiring.go. Without this call the four
	// ebus.v1.ebus_standard.* surfaces are unreachable at runtime because
	// handleToolsCall rejects unknown tool names before dispatch.
	RegisterEbusStandardTools(server, ebusstdcat.MustEmbeddedCatalog())

	return server, nil
}

func (s *Server) hasToolNamed(name string) bool {
	if s == nil {
		return false
	}
	if _, ok := eebusV1CommandSpecForName(name); ok {
		return true
	}
	for _, tool := range s.tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}

func (s *Server) indexOfTool(name string) int {
	if s == nil {
		return -1
	}
	for i, tool := range s.tools {
		if tool.Name == name {
			return i
		}
	}
	return -1
}

func (s *Server) registerBusObservabilityTools() {
	if s == nil || s.hasToolNamed(toolBusSummaryGetName) {
		return
	}

	insertAt := s.indexOfTool(toolRuntimeStatusGetName) + 1
	if insertAt <= 0 || insertAt > len(s.tools) {
		s.tools = append(s.tools, busObservabilityTools()...)
		return
	}

	additions := busObservabilityTools()
	tools := make([]Tool, 0, len(s.tools)+len(additions))
	tools = append(tools, s.tools[:insertAt]...)
	tools = append(tools, additions...)
	tools = append(tools, s.tools[insertAt:]...)
	s.tools = tools
}

func (s *Server) registerWatchSummaryTools() {
	if s == nil || s.hasToolNamed(toolWatchSummaryGetName) {
		return
	}

	insertAt := s.indexOfTool(toolBusPeriodicityListName) + 1
	if insertAt <= 0 || insertAt > len(s.tools) {
		insertAt = s.indexOfTool(toolRuntimeStatusGetName) + 1
	}
	if insertAt <= 0 || insertAt > len(s.tools) {
		s.tools = append(s.tools, watchSummaryTools()...)
		return
	}

	additions := watchSummaryTools()
	tools := make([]Tool, 0, len(s.tools)+len(additions))
	tools = append(tools, s.tools[:insertAt]...)
	tools = append(tools, additions...)
	tools = append(tools, s.tools[insertAt:]...)
	s.tools = tools
}

func (s *Server) SetStatusProvider(provider StatusProvider) {
	if s == nil || provider == nil {
		return
	}
	s.statusProvider = provider
}

func (s *Server) SetBusObservabilityProvider(provider BusObservabilityProvider) {
	if s == nil || provider == nil {
		return
	}
	s.bus = provider
	s.registerBusObservabilityTools()
}

func (s *Server) SetWatchSummaryProvider(provider WatchSummaryProvider) {
	if s == nil || provider == nil {
		return
	}
	s.watch = provider
	s.registerWatchSummaryTools()
}

func (s *Server) SetSemanticProvider(provider SemanticProvider) {
	if s == nil || provider == nil {
		return
	}
	s.semantic = provider
}

func (s *Server) SetScheduleWriter(writer ScheduleWriter) {
	if s == nil || writer == nil {
		return
	}
	s.scheduleWriter = writer
}

func (s *Server) SetConfigWriter(writer ConfigWriter) {
	if s == nil || writer == nil {
		return
	}
	s.configWriter = writer
}

func (s *Server) SetAdmittedRPCSource(source byte) {
	if s == nil {
		return
	}
	s.rpcSource = source
	s.rpcSourceAdmitted = source != 0
}

func (s *Server) SetAdmittedRPCSourceProvider(provider func() (byte, bool)) {
	if s == nil {
		return
	}
	s.rpcSourceProvider = provider
}

func (s *Server) admittedRPCSource() (byte, bool) {
	if s == nil {
		return 0, false
	}
	if s.rpcSourceProvider != nil {
		return s.rpcSourceProvider()
	}
	return s.rpcSource, s.rpcSourceAdmitted
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.ServeHTTP)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r == nil {
		http.Error(w, "request missing", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		s.handlePost(w, r)
	default:
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handlePost(w http.ResponseWriter, r *http.Request) {
	const maximumRequestBodyBytes = 1 << 20
	body, err := io.ReadAll(io.LimitReader(r.Body, maximumRequestBodyBytes+1))
	if err != nil {
		http.Error(w, "read failed", http.StatusBadRequest)
		return
	}
	defer func() { _ = r.Body.Close() }()
	if len(body) > maximumRequestBodyBytes {
		s.writeRPCError(w, nil, rpcErrorInvalidRequest("request body too large"))
		return
	}

	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeRPCError(w, req.ID, rpcErrorInvalidRequest("invalid json"))
		return
	}
	if req.JSONRPC != "2.0" || req.Method == "" {
		s.writeRPCError(w, req.ID, rpcErrorInvalidRequest("invalid jsonrpc request"))
		return
	}

	result, rpcErr := s.dispatch(r.Context(), req.Method, req.Params)
	if req.ID == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if rpcErr != nil {
		s.writeRPCError(w, req.ID, rpcErr)
		return
	}
	s.writeRPCResult(w, req.ID, result)
}

func (s *Server) dispatch(ctx context.Context, method string, params json.RawMessage) (any, *rpcError) {
	switch method {
	case "initialize":
		return s.handleInitialize(params)
	case "tools/list":
		return s.handleToolsList(ctx)
	case "tools/call":
		return s.handleToolsCall(ctx, params)
	case "ping":
		return map[string]any{}, nil
	default:
		return nil, rpcErrorMethodNotFound(fmt.Sprintf("method %q not found", method))
	}
}

func (s *Server) handleInitialize(params json.RawMessage) (any, *rpcError) {
	var initParams map[string]any
	if len(params) > 0 {
		if err := json.Unmarshal(params, &initParams); err != nil {
			return nil, rpcErrorInvalidParams("initialize params invalid")
		}
	}

	sessionID, err := newSessionID()
	if err != nil {
		return nil, rpcErrorInternal("failed to generate session id")
	}

	return map[string]any{
		"protocolVersion": "2025-03-26",
		"capabilities": map[string]any{
			"tools": map[string]any{},
		},
		"serverInfo": map[string]any{
			"name":    "helianthus-ebusgateway",
			"version": "0.0.0",
		},
		"sessionId": sessionID,
	}, nil
}

func (s *Server) handleToolsList(ctx context.Context) (any, *rpcError) {
	if m8SourceScopeFromContext(ctx) {
		return map[string]any{"tools": s.m8SourceTools()}, nil
	}
	tools := s.tools
	if eebusV1BoundaryFromContext(ctx) == eebusV1OperatorBoundary {
		s.eebusV1Mu.RLock()
		commandsRegistered := !eebusV1NilCommandRouter(s.eebusV1CommandRouter)
		captureRegistered := !nilSynchronizedEvidenceCapture(s.synchronizedEvidenceCapture)
		promotionRegistered := !nilLeafPromotionCapture(s.leafPromotionCapture)
		m8SourceStateRegistered := s.eebusV1 != nil
		s.eebusV1Mu.RUnlock()
		if commandsRegistered || captureRegistered || promotionRegistered || m8SourceStateRegistered {
			tools = append([]Tool(nil), tools...)
		}
		if commandsRegistered {
			tools = append(tools, eebusV1CommandTools()...)
		}
		if captureRegistered {
			tools = append(tools, synchronizedEvidenceCaptureTool())
		}
		if promotionRegistered {
			tools = append(tools, leafPromotionCaptureTool())
		}
		if m8SourceStateRegistered {
			tools = append(tools, m8SourceStateTool())
		}
	}
	return map[string]any{
		"tools": tools,
	}, nil
}

type callToolParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type rawCallToolParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func (s *Server) handleToolsCall(ctx context.Context, params json.RawMessage) (any, *rpcError) {
	var rawCall rawCallToolParams
	if err := json.Unmarshal(params, &rawCall); err != nil {
		return nil, rpcErrorInvalidParams("tools/call params invalid")
	}
	if rawCall.Name == "" {
		return nil, rpcErrorInvalidParams("tools/call missing name")
	}
	if m8SourceScopeFromContext(ctx) {
		if !m8SourceToolAllowed(rawCall.Name) {
			return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", rawCall.Name))
		}
		if !m8SourceToolCallable(rawCall.Name) {
			return nil, rpcErrorInvalidParams(fmt.Sprintf("tool %q is not callable in read-only evidence scope", rawCall.Name))
		}
	}
	hasDuplicateKeys := eebusV1JSONHasDuplicateKeys(params)
	if rawCall.Name == synchronizedEvidenceCaptureToolName {
		invalidCallParams := hasDuplicateKeys || !synchronizedEvidenceCallParamsClosed(params)
		return s.handleSynchronizedEvidenceCaptureRaw(ctx, rawCall.Arguments, invalidCallParams)
	}
	if rawCall.Name == leafPromotionCaptureToolName {
		invalidCallParams := hasDuplicateKeys || !synchronizedEvidenceCallParamsClosed(params)
		return s.handleLeafPromotionCaptureRaw(ctx, rawCall.Arguments, invalidCallParams)
	}
	if rawCall.Name == m8SourceStateToolName {
		return s.handleM8SourceState(ctx, rawCall.Arguments)
	}
	if !s.hasToolNamed(rawCall.Name) {
		return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", rawCall.Name))
	}
	if spec, ok := eebusV1CommandSpecForName(rawCall.Name); ok {
		if hasDuplicateKeys {
			return eebusV1MalformedCommandResult(spec), nil
		}
		return s.handleEEBusV1CommandRawCall(ctx, spec, rawCall.Arguments), nil
	}
	if hasDuplicateKeys {
		return nil, rpcErrorInvalidParams("tools/call params contain duplicate object keys")
	}

	var call callToolParams
	if err := json.Unmarshal(params, &call); err != nil {
		return nil, rpcErrorInvalidParams("tools/call params invalid")
	}

	if result, handled := s.handleEbusStandardCall(call.Name, call.Arguments); handled {
		return result, nil
	}

	if result, handled := s.handleVaillantB503Call(ctx, call.Name, call.Arguments); handled {
		return result, nil
	}

	if result, handled := s.handleEEBusV1Call(ctx, call.Name, call.Arguments); handled {
		return result, nil
	}

	if result, handled := s.handleModbusV1Call(ctx, call.Name, call.Arguments); handled {
		return result, nil
	}

	switch call.Name {
	case toolRuntimeStatusGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		status := s.runtimeStatus(snapshot)
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(status, nil, consistency)), false), nil
	case toolBusSummaryGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotBusSummary(snapshot), nil, consistency)), false), nil
	case toolBusMessagesListName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		limit, err := parseOptionalLimit(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(nil, err, consistency)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotBusMessagesList(snapshot, limit), nil, consistency)), false), nil
	case toolBusPeriodicityListName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		limit, err := parseOptionalLimit(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(nil, err, consistency)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotBusPeriodicityList(snapshot, limit), nil, consistency)), false), nil
	case toolBusProtocolSpecimensListName:
		// Specimens are an append-only observability ring buffer — snapshot
		// consistency does not apply.  Always return live data.
		limit, err := parseOptionalLimit(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		family, err := parseOptionalFamily(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelope(s.snapshotProtocolSpecimens(family, limit), nil)), false), nil
	case toolWatchSummaryGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotWatchSummary(snapshot), nil, consistency)), false), nil
	case toolSemanticZonesGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		zones := s.snapshotZones(snapshot)
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(zones, nil, consistency)), false), nil
	case toolSemanticCircuitsGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		circuits := s.snapshotCircuits(snapshot)
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(circuits, nil, consistency)), false), nil
	case toolSemanticRadioGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		radio := s.snapshotRadioDevices(snapshot)
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(radio, nil, consistency)), false), nil
	case toolSemanticFM5ModeGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		mode := s.snapshotFM5Mode(snapshot)
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(mode, nil, consistency)), false), nil
	case toolSemanticFM5InterpretationGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		verdict := s.snapshotFM5Interpretation(snapshot)
		if verdict == (Fm5Interpretation{}) {
			return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(nil, nil, consistency)), false), nil
		}
		if err := verdict.validate(); err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(verdict, nil, consistency)), false), nil
	case toolSemanticSolarGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotSolar(snapshot), nil, consistency)), false), nil
	case toolSemanticCylindersGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotCylinders(snapshot), nil, consistency)), false), nil
	case toolSemanticDHWGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotDHW(snapshot), nil, consistency)), false), nil
	case toolSemanticEnergyGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotEnergyTotals(snapshot), nil, consistency)), false), nil
	case toolSemanticBoilerGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotBoilerStatus(snapshot), nil, consistency)), false), nil
	case toolSemanticSystemGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotSystem(snapshot), nil, consistency)), false), nil
	case toolSemanticAdapterInfoGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotAdapterInfo(snapshot), nil, consistency)), false), nil
	case toolSemanticSchedulesGetName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(s.snapshotSchedules(snapshot), nil, consistency)), false), nil
	case toolSemanticSystemSetConfigName:
		if s.configWriter == nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, fmt.Errorf("system config writer not available"))), true), nil
		}
		field, _ := call.Arguments["field"].(string)
		value, _ := call.Arguments["value"].(string)
		if field == "" {
			return callToolResultText(mustJSON(newToolEnvelope(nil, fmt.Errorf("field is required"))), true), nil
		}
		result := s.configWriter.SetSystemConfig(ctx, field, value)
		return callToolResultText(mustJSON(newToolEnvelope(result, nil)), !result.Success), nil
	case toolSemanticBoilerSetConfigName:
		if s.configWriter == nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, fmt.Errorf("boiler config writer not available"))), true), nil
		}
		field, _ := call.Arguments["field"].(string)
		value, _ := call.Arguments["value"].(string)
		if field == "" {
			return callToolResultText(mustJSON(newToolEnvelope(nil, fmt.Errorf("field is required"))), true), nil
		}
		result := s.configWriter.SetBoilerConfig(ctx, field, value)
		return callToolResultText(mustJSON(newToolEnvelope(result, nil)), !result.Success), nil
	case toolSemanticSchedulesSetZoneName:
		if s.scheduleWriter == nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, fmt.Errorf("schedule writer not available"))), true), nil
		}
		result, err := s.handleSetZoneTimeProgram(ctx, call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelope(result, nil)), !result.Success), nil
	case toolSemanticSchedulesSetDhwName:
		if s.scheduleWriter == nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, fmt.Errorf("schedule writer not available"))), true), nil
		}
		result, err := s.handleSetDhwTimeProgram(ctx, call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelope(result, nil)), !result.Success), nil
	case toolSemanticSnapshotName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		data, err := s.readSemanticSnapshot(ctx, call.Arguments, snapshot)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(nil, err, consistency)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(data, nil, consistency)), false), nil
	case toolSnapshotCaptureName:
		snapshotID, createdAt, err := s.captureSnapshot()
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		data := map[string]any{
			"snapshot_id": snapshotID,
			"created_at":  createdAt.UTC().Format(time.RFC3339Nano),
		}
		return callToolResultText(mustJSON(newToolEnvelope(data, nil)), false), nil
	case toolSnapshotDropName:
		if err := s.dropSnapshot(call.Arguments); err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelope(map[string]any{"dropped": true}, nil)), false), nil
	case toolDevicesV1Name, toolDevicesLegacyName:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		devices := s.listDevices(snapshot)
		text := mustJSON(newToolEnvelopeWithConsistency(devices, nil, consistency))
		return callToolResultText(text, false), nil
	case toolDeviceGetV1Name:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		device, err := s.getDevice(call.Arguments, snapshot)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(nil, err, consistency)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(device, nil, consistency)), false), nil
	case toolPlanesListV1Name:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		planes, err := s.listPlanes(call.Arguments, snapshot)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(nil, err, consistency)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(planes, nil, consistency)), false), nil
	case toolMethodsListV1Name:
		consistency, snapshot, err := s.resolveConsistency(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		methods, err := s.listMethods(call.Arguments, snapshot)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(nil, err, consistency)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelopeWithConsistency(methods, nil, consistency)), false), nil
	case toolInvokeV1Name:
		if s.invoker == nil {
			return nil, rpcErrorInternal("server missing invoker")
		}
		policy, err := s.enforceInvokeV1Safety(call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		// Normalize rpc source (inject admitted source if omitted) BEFORE
		// computing the idempotency signature, so caller-variance on
		// params.source (omitted vs. explicit admitted source) produces identical
		// signatures and thus identical cache keys. Without this, two
		// semantically identical MUTATE requests would hash differently
		// and the second would be rejected as "idempotency key reused
		// with different payload". See PR #505 r3106812547.
		rpcSource, rpcSourceAdmitted := s.admittedRPCSource()
		if _, err := enforceRPCSourceOnArgs(call.Arguments, rpcSource, rpcSourceAdmitted); err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		signature := ""
		if policy.intent == "MUTATE" {
			signature, err = buildInvokeIdempotencySignature(call.Arguments)
			if err != nil {
				return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
			}
			if cached, ok, err := s.lookupIdempotency(policy.idempotencyKey, signature); err != nil {
				return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
			} else if ok {
				return callToolResultText(mustJSON(newToolEnvelope(cached, nil)), false), nil
			}
		}

		invokeCtx := ctx
		cancel := func() {}
		if policy.timeout > 0 {
			invokeCtx, cancel = context.WithTimeout(ctx, policy.timeout)
		}
		defer cancel()

		out, err := s.invoke(invokeCtx, call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		if policy.intent == "MUTATE" {
			s.storeIdempotency(policy.idempotencyKey, signature, out)
		}
		return callToolResultText(mustJSON(newToolEnvelope(out, nil)), false), nil
	case toolInvokeLegacyName:
		if s.invoker == nil {
			return nil, rpcErrorInternal("server missing invoker")
		}
		out, err := s.invoke(ctx, call.Arguments)
		if err != nil {
			return callToolResultText(mustJSON(newToolEnvelope(nil, err)), true), nil
		}
		return callToolResultText(mustJSON(newToolEnvelope(out, nil)), false), nil
	default:
		return nil, rpcErrorInvalidParams(fmt.Sprintf("unknown tool %q", call.Name))
	}
}

func (s *Server) handleSetZoneTimeProgram(ctx context.Context, args map[string]any) (*TimeProgramWriteResult, error) {
	zoneRaw, ok := args["zone"]
	if !ok {
		return nil, fmt.Errorf("missing required parameter: zone")
	}
	zoneFloat, ok := zoneRaw.(float64)
	if !ok {
		return nil, fmt.Errorf("zone must be an integer")
	}
	zone, exact := intFromFloat(zoneFloat)
	if !exact {
		return nil, fmt.Errorf("zone must be an integer, got %v", zoneFloat)
	}

	weekdayRaw, ok := args["weekday"]
	if !ok {
		return nil, fmt.Errorf("missing required parameter: weekday")
	}
	weekdayFloat, ok := weekdayRaw.(float64)
	if !ok {
		return nil, fmt.Errorf("weekday must be an integer")
	}
	weekday, exact := intFromFloat(weekdayFloat)
	if !exact {
		return nil, fmt.Errorf("weekday must be an integer, got %v", weekdayFloat)
	}

	slotsRaw, ok := args["slots"]
	if !ok {
		return nil, fmt.Errorf("missing required parameter: slots")
	}
	slotsArr, ok := slotsRaw.([]any)
	if !ok {
		return nil, fmt.Errorf("slots must be an array")
	}

	slots := make([]TimeProgramSlot, 0, len(slotsArr))
	for i, raw := range slotsArr {
		slotMap, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("slot %d must be an object", i)
		}
		slot, err := parseTimeProgramSlot(slotMap, true)
		if err != nil {
			return nil, fmt.Errorf("slot %d: %w", i, err)
		}
		slots = append(slots, slot)
	}

	return s.scheduleWriter.SetZoneTimeProgram(ctx, zone, weekday, slots)
}

func (s *Server) handleSetDhwTimeProgram(ctx context.Context, args map[string]any) (*TimeProgramWriteResult, error) {
	weekdayRaw, ok := args["weekday"]
	if !ok {
		return nil, fmt.Errorf("missing required parameter: weekday")
	}
	weekdayFloat, ok := weekdayRaw.(float64)
	if !ok {
		return nil, fmt.Errorf("weekday must be an integer")
	}
	weekday, exact := intFromFloat(weekdayFloat)
	if !exact {
		return nil, fmt.Errorf("weekday must be an integer, got %v", weekdayFloat)
	}

	slotsRaw, ok := args["slots"]
	if !ok {
		return nil, fmt.Errorf("missing required parameter: slots")
	}
	slotsArr, ok := slotsRaw.([]any)
	if !ok {
		return nil, fmt.Errorf("slots must be an array")
	}

	slots := make([]TimeProgramSlot, 0, len(slotsArr))
	for i, raw := range slotsArr {
		slotMap, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("slot %d must be an object", i)
		}
		slot, err := parseTimeProgramSlot(slotMap, false)
		if err != nil {
			return nil, fmt.Errorf("slot %d: %w", i, err)
		}
		slots = append(slots, slot)
	}

	return s.scheduleWriter.SetDhwTimeProgram(ctx, weekday, slots)
}

// intFromFloat converts a float64 to int, returning false if the value has a
// fractional part. This prevents silent truncation of values like 1.9 → 1.
func intFromFloat(v float64) (int, bool) {
	i := int(v)
	return i, v == float64(i)
}

func parseOptionalLimit(args map[string]any) (int, error) {
	if args == nil {
		return 0, nil
	}
	raw, ok := args["limit"]
	if !ok || raw == nil {
		return 0, nil
	}
	switch value := raw.(type) {
	case int:
		if value <= 0 {
			return 0, fmt.Errorf("invalid limit: %w", ebuserrors.ErrInvalidPayload)
		}
		return value, nil
	case int64:
		if value <= 0 {
			return 0, fmt.Errorf("invalid limit: %w", ebuserrors.ErrInvalidPayload)
		}
		return int(value), nil
	case float64:
		limit, exact := intFromFloat(value)
		if !exact || limit <= 0 {
			return 0, fmt.Errorf("invalid limit: %w", ebuserrors.ErrInvalidPayload)
		}
		return limit, nil
	default:
		return 0, fmt.Errorf("invalid limit: %w", ebuserrors.ErrInvalidPayload)
	}
}

func parseOptionalFamily(args map[string]any) (string, error) {
	if args == nil {
		return "", nil
	}
	raw, ok := args["family"]
	if !ok || raw == nil {
		return "", nil
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("invalid family: %w", ebuserrors.ErrInvalidPayload)
	}
	return s, nil
}

func parseTimeProgramSlot(m map[string]any, tempRequired bool) (TimeProgramSlot, error) {
	var slot TimeProgramSlot

	startHourF, ok := m["start_hour"].(float64)
	if !ok {
		return slot, fmt.Errorf("start_hour is required")
	}
	sh, exact := intFromFloat(startHourF)
	if !exact {
		return slot, fmt.Errorf("start_hour must be an integer, got %v", startHourF)
	}
	slot.StartHour = sh

	startMinuteF, ok := m["start_minute"].(float64)
	if !ok {
		return slot, fmt.Errorf("start_minute is required")
	}
	sm, exact := intFromFloat(startMinuteF)
	if !exact {
		return slot, fmt.Errorf("start_minute must be an integer, got %v", startMinuteF)
	}
	slot.StartMinute = sm

	endHourF, ok := m["end_hour"].(float64)
	if !ok {
		return slot, fmt.Errorf("end_hour is required")
	}
	eh, exact := intFromFloat(endHourF)
	if !exact {
		return slot, fmt.Errorf("end_hour must be an integer, got %v", endHourF)
	}
	slot.EndHour = eh

	endMinuteF, ok := m["end_minute"].(float64)
	if !ok {
		return slot, fmt.Errorf("end_minute is required")
	}
	em, exact := intFromFloat(endMinuteF)
	if !exact {
		return slot, fmt.Errorf("end_minute must be an integer, got %v", endMinuteF)
	}
	slot.EndMinute = em

	if tempVal, ok := m["temperature_c"]; ok && tempVal != nil {
		tempFloat, ok := tempVal.(float64)
		if !ok {
			return slot, fmt.Errorf("temperature_c must be a number")
		}
		slot.TemperatureC = &tempFloat
	} else if tempRequired {
		return slot, fmt.Errorf("temperature_c is required")
	}

	return slot, nil
}

func (s *Server) resolveConsistency(args map[string]any) (envelopeConsistency, *snapshotState, error) {
	consistency := envelopeConsistency{
		mode:          "LIVE",
		dataTimestamp: time.Now().UTC(),
	}
	if args == nil {
		return consistency, nil, nil
	}
	raw, ok := args["consistency"]
	if !ok || raw == nil {
		return consistency, nil, nil
	}
	value, ok := raw.(map[string]any)
	if !ok {
		return envelopeConsistency{}, nil, fmt.Errorf("invalid consistency payload: %w", ebuserrors.ErrInvalidPayload)
	}
	mode, _ := value["mode"].(string)
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if mode == "" || mode == "LIVE" {
		return consistency, nil, nil
	}
	if mode != "SNAPSHOT" {
		return envelopeConsistency{}, nil, fmt.Errorf("invalid consistency mode %q: %w", mode, ebuserrors.ErrInvalidPayload)
	}
	snapshotID, _ := value["snapshot_id"].(string)
	snapshotID = strings.TrimSpace(snapshotID)
	if snapshotID == "" {
		return envelopeConsistency{}, nil, fmt.Errorf("missing consistency.snapshot_id: %w", ebuserrors.ErrInvalidPayload)
	}
	snapshot, ok := s.getSnapshot(snapshotID)
	if !ok {
		return envelopeConsistency{}, nil, fmt.Errorf("unknown snapshot %q: %w", snapshotID, errSnapshotNotFound)
	}
	consistency.mode = "SNAPSHOT"
	consistency.snapshotID = snapshotID
	consistency.dataTimestamp = snapshot.createdAt
	return consistency, &snapshot, nil
}

func (s *Server) captureSnapshot() (snapshotID string, createdAt time.Time, err error) {
	id, err := newSessionID()
	if err != nil {
		return "", time.Time{}, fmt.Errorf("snapshot id generation failed: %w", err)
	}
	now := time.Now().UTC()
	bus := s.snapshotBusObservability(nil)
	watch := s.snapshotWatchSummary(nil)
	snapshot := snapshotState{
		id:             id,
		createdAt:      now,
		expiresAt:      now.Add(defaultSnapshotTTL),
		runtime:        s.runtimeStatus(nil),
		busSummary:     cloneBusSummary(bus.Summary),
		busMessages:    cloneBusMessages(bus.Messages),
		busPeriodicity: cloneBusPeriodicity(bus.Periodicity),
		watchSummary:   cloneWatchSummary(watch),
		zones:          s.snapshotZones(nil),
		circuits:       s.snapshotCircuits(nil),
		radio:          s.snapshotRadioDevices(nil),
		fm5Mode:        s.snapshotFM5Mode(nil),
		fm5Verdict:     s.snapshotFM5Interpretation(nil),
		solar:          s.snapshotSolar(nil),
		cylinders:      s.snapshotCylinders(nil),
		dhw:            s.snapshotDHW(nil),
		energy:         s.snapshotEnergyTotals(nil),
		boiler:         s.snapshotBoilerStatus(nil),
		system:         s.snapshotSystem(nil),
		schedules:      s.snapshotSchedules(nil),
		adapterInfo:    s.snapshotAdapterInfo(nil),
		devices:        s.listDevices(nil),
		addressSlots:   s.snapshotAddressSlots(),
	}

	s.snapshotMu.Lock()
	s.cleanupExpiredSnapshotsLocked(now)
	s.snapshots[id] = cloneSnapshotState(snapshot)
	s.snapshotMu.Unlock()

	return id, now, nil
}

func (s *Server) dropSnapshot(args map[string]any) error {
	snapshotID, err := parseSnapshotID(args)
	if err != nil {
		return err
	}
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	if _, ok := s.snapshots[snapshotID]; !ok {
		return fmt.Errorf("unknown snapshot %q: %w", snapshotID, errSnapshotNotFound)
	}
	delete(s.snapshots, snapshotID)
	return nil
}

func (s *Server) getSnapshot(snapshotID string) (snapshotState, bool) {
	now := time.Now().UTC()
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	s.cleanupExpiredSnapshotsLocked(now)
	snapshot, ok := s.snapshots[snapshotID]
	if !ok {
		return snapshotState{}, false
	}
	return cloneSnapshotState(snapshot), true
}

func (s *Server) cleanupExpiredSnapshotsLocked(now time.Time) {
	for id, snapshot := range s.snapshots {
		if now.After(snapshot.expiresAt) {
			delete(s.snapshots, id)
		}
	}
}

func parseSnapshotID(args map[string]any) (string, error) {
	if args == nil {
		return "", fmt.Errorf("missing snapshot_id: %w", ebuserrors.ErrInvalidPayload)
	}
	snapshotID, _ := args["snapshot_id"].(string)
	snapshotID = strings.TrimSpace(snapshotID)
	if snapshotID == "" {
		return "", fmt.Errorf("missing snapshot_id: %w", ebuserrors.ErrInvalidPayload)
	}
	return snapshotID, nil
}

func cloneSnapshotState(snapshot snapshotState) snapshotState {
	var dhwCopy *DhwStatus
	if snapshot.dhw != nil {
		value := *snapshot.dhw
		dhwCopy = &value
	}
	var energyCopy *EnergyTotals
	if snapshot.energy != nil {
		value := *snapshot.energy
		value.Gas = cloneEnergyChannel(value.Gas)
		value.Electric = cloneEnergyChannel(value.Electric)
		value.Solar = cloneEnergyChannel(value.Solar)
		energyCopy = &value
	}
	var boilerCopy *BoilerStatus
	if snapshot.boiler != nil {
		boilerCopy = cloneMCPBoilerStatus(snapshot.boiler)
	}
	var systemCopy *SystemStatus
	if snapshot.system != nil {
		systemCopy = cloneMCPSystemStatus(snapshot.system)
	}
	var schedulesCopy *ScheduleStatus
	if snapshot.schedules != nil {
		schedulesCopy = cloneMCPScheduleStatus(snapshot.schedules)
	}
	var solarCopy *SolarStatus
	if snapshot.solar != nil {
		solarCopy = cloneMCPSolarStatus(snapshot.solar)
	}
	cylindersCopy := cloneCylinders(snapshot.cylinders)
	return snapshotState{
		id:             snapshot.id,
		createdAt:      snapshot.createdAt,
		expiresAt:      snapshot.expiresAt,
		runtime:        cloneMap(snapshot.runtime),
		busSummary:     cloneBusSummary(snapshot.busSummary),
		busMessages:    cloneBusMessages(snapshot.busMessages),
		busPeriodicity: cloneBusPeriodicity(snapshot.busPeriodicity),
		watchSummary:   cloneWatchSummary(snapshot.watchSummary),
		zones:          cloneZones(snapshot.zones),
		circuits:       cloneCircuits(snapshot.circuits),
		radio:          cloneRadioDevices(snapshot.radio),
		fm5Mode:        snapshot.fm5Mode,
		fm5Verdict:     snapshot.fm5Verdict,
		solar:          solarCopy,
		cylinders:      cylindersCopy,
		dhw:            dhwCopy,
		energy:         energyCopy,
		boiler:         boilerCopy,
		system:         systemCopy,
		schedules:      schedulesCopy,
		adapterInfo:    cloneMCPAdapterHardwareInfo(snapshot.adapterInfo),
		devices:        cloneDeviceInfoList(snapshot.devices),
		addressSlots:   cloneAddressSlotLabels(snapshot.addressSlots),
	}
}

func cloneAddressSlotLabels(in map[byte]addressSlotLabels) map[byte]addressSlotLabels {
	if in == nil {
		return nil
	}
	out := make(map[byte]addressSlotLabels, len(in))
	for addr, labels := range in {
		out[addr] = labels
	}
	return out
}

func newToolEnvelope(data any, err error) map[string]any {
	return newToolEnvelopeWithConsistency(data, err, envelopeConsistency{
		mode:          "LIVE",
		dataTimestamp: time.Now().UTC(),
	})
}

func newToolEnvelopeWithConsistency(data any, err error, consistency envelopeConsistency) map[string]any {
	mode := strings.TrimSpace(consistency.mode)
	if mode == "" {
		mode = "LIVE"
	}
	timestamp := consistency.dataTimestamp
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	consistencyMeta := map[string]any{
		"mode": mode,
	}
	if strings.TrimSpace(consistency.snapshotID) != "" {
		consistencyMeta["snapshot_id"] = consistency.snapshotID
	}

	meta := map[string]any{
		"contract": map[string]any{
			"name":  "helianthus-ebus-mcp",
			"major": 1,
			"minor": 0,
		},
		"consistency":    consistencyMeta,
		"data_timestamp": timestamp.UTC().Format(time.RFC3339Nano),
		"data_hash":      hashData(data),
	}

	var envelopeError any
	if err != nil {
		code, retriable, sourceLayer := classifyToolError(err)
		envelopeError = map[string]any{
			"code":         code,
			"message":      err.Error(),
			"retriable":    retriable,
			"source_layer": sourceLayer,
		}
	}

	return map[string]any{
		"meta":  meta,
		"data":  data,
		"error": envelopeError,
	}
}

func hashData(data any) string {
	raw, err := json.Marshal(data)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func classifyToolError(err error) (code string, retriable bool, sourceLayer string) {
	if code, ok := classifyB503Error(err); ok {
		return code, false, "gateway"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "TIMEOUT", true, "gateway"
	case errors.Is(err, errInvokePermissionDenied):
		return "PERMISSION_DENIED", false, "gateway"
	case errors.Is(err, errInvokeIdempotencyConflict):
		return "CONFLICT", false, "gateway"
	case errors.Is(err, errSnapshotNotFound):
		return "NOT_FOUND", false, "gateway"
	case errors.Is(err, errSourceSelectionNotActive):
		return "INVALID_ARGUMENT", false, "gateway"
	case errors.Is(err, rpcsource.ErrInvalidSource):
		// Client supplied an unsupported source type or a numeric value
		// that cannot be safely narrowed to a nonzero source byte. This is
		// a client input error, not a server fault.
		return "INVALID_ARGUMENT", false, "gateway"
	case errors.Is(err, ebuserrors.ErrInvalidPayload):
		return "INVALID_ARGUMENT", false, "ebusreg"
	case errors.Is(err, ebuserrors.ErrNoSuchDevice):
		return "NOT_FOUND", false, "ebusreg"
	case errors.Is(err, ebuserrors.ErrNACK):
		return "PROTOCOL_ERROR", false, "ebusgo"
	case errors.Is(err, ebuserrors.ErrTimeout):
		return "TIMEOUT", true, "ebusgo"
	case errors.Is(err, ebuserrors.ErrCRCMismatch):
		return "PROTOCOL_ERROR", true, "ebusgo"
	case errors.Is(err, ebuserrors.ErrTransportClosed):
		return "BUS_UNAVAILABLE", false, "ebusgo"
	case errors.Is(err, ebuserrors.ErrBusCollision):
		return "BUS_UNAVAILABLE", true, "ebusgo"
	case errors.Is(err, ebuserrors.ErrRetryExhausted):
		return "BUS_UNAVAILABLE", true, "ebusgo"
	default:
		return "INTERNAL", false, "gateway"
	}
}

func (s *Server) enforceInvokeV1Safety(args map[string]any) (invokeV1Policy, error) {
	policy := invokeV1Policy{
		timeout: defaultInvokeTimeout,
	}

	address, err := parseAddress(args["address"])
	if err != nil {
		return invokeV1Policy{}, err
	}
	planeName, _ := args["plane"].(string)
	if planeName == "" {
		return invokeV1Policy{}, fmt.Errorf("missing plane: %w", ebuserrors.ErrInvalidPayload)
	}
	methodName, _ := args["method"].(string)
	if methodName == "" {
		return invokeV1Policy{}, fmt.Errorf("missing method: %w", ebuserrors.ErrInvalidPayload)
	}
	intent, _ := args["intent"].(string)
	intent = strings.TrimSpace(intent)
	if intent == "" {
		return invokeV1Policy{}, fmt.Errorf("missing intent: %w", ebuserrors.ErrInvalidPayload)
	}
	policy.intent = intent
	allowDangerous, ok := args["allow_dangerous"].(bool)
	if !ok {
		return invokeV1Policy{}, fmt.Errorf("missing allow_dangerous: %w", ebuserrors.ErrInvalidPayload)
	}
	if timeout, err := parseInvokeTimeout(args["timeout_ms"]); err != nil {
		return invokeV1Policy{}, err
	} else if timeout > 0 {
		policy.timeout = timeout
	}

	methodKnown, methodReadOnly, err := s.lookupMethodMutability(address, planeName, methodName)
	if err != nil {
		return invokeV1Policy{}, err
	}

	switch intent {
	case "READ_ONLY":
		if !methodKnown || !methodReadOnly {
			return invokeV1Policy{}, fmt.Errorf("READ_ONLY intent denied for mutating/unknown method: %w", errInvokePermissionDenied)
		}
	case "MUTATE":
		if !allowDangerous {
			return invokeV1Policy{}, fmt.Errorf("MUTATE intent requires allow_dangerous=true: %w", errInvokePermissionDenied)
		}
		idempotencyKey, _ := args["idempotency_key"].(string)
		if strings.TrimSpace(idempotencyKey) == "" {
			return invokeV1Policy{}, fmt.Errorf("MUTATE intent requires idempotency_key: %w", errInvokePermissionDenied)
		}
		policy.idempotencyKey = strings.TrimSpace(idempotencyKey)
	default:
		return invokeV1Policy{}, fmt.Errorf("invalid intent %q: %w", intent, ebuserrors.ErrInvalidPayload)
	}

	return policy, nil
}

func (s *Server) lookupMethodMutability(address byte, planeName string, methodName string) (methodKnown bool, methodReadOnly bool, err error) {
	entry, ok := s.registry.Lookup(address)
	if !ok {
		return false, false, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}

	var plane registry.Plane
	for _, candidate := range entry.Planes() {
		if candidate != nil && candidate.Name() == planeName {
			plane = candidate
			break
		}
	}
	if plane == nil {
		return false, false, fmt.Errorf("unknown plane %q: %w", planeName, ebuserrors.ErrInvalidPayload)
	}

	for _, method := range plane.Methods() {
		if method != nil && method.Name() == methodName {
			return true, method.ReadOnly(), nil
		}
	}
	return false, false, nil
}

func parseInvokeTimeout(raw any) (time.Duration, error) {
	if raw == nil {
		return defaultInvokeTimeout, nil
	}

	switch value := raw.(type) {
	case int:
		if value <= 0 {
			return 0, fmt.Errorf("invalid timeout_ms: %w", ebuserrors.ErrInvalidPayload)
		}
		return time.Duration(value) * time.Millisecond, nil
	case int64:
		if value <= 0 {
			return 0, fmt.Errorf("invalid timeout_ms: %w", ebuserrors.ErrInvalidPayload)
		}
		return time.Duration(value) * time.Millisecond, nil
	case float64:
		if value <= 0 || value != float64(int(value)) {
			return 0, fmt.Errorf("invalid timeout_ms: %w", ebuserrors.ErrInvalidPayload)
		}
		return time.Duration(int(value)) * time.Millisecond, nil
	default:
		return 0, fmt.Errorf("invalid timeout_ms: %w", ebuserrors.ErrInvalidPayload)
	}
}

func buildInvokeIdempotencySignature(args map[string]any) (string, error) {
	payload := map[string]any{
		"address": args["address"],
		"plane":   args["plane"],
		"method":  args["method"],
		"params":  args["params"],
		"intent":  args["intent"],
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to canonicalize idempotency payload: %w", ebuserrors.ErrInvalidPayload)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Server) lookupIdempotency(key string, signature string) (any, bool, error) {
	now := time.Now()
	s.idempotencyMu.Lock()
	defer s.idempotencyMu.Unlock()

	for candidate, entry := range s.idempotency {
		if now.After(entry.expiresAt) {
			delete(s.idempotency, candidate)
		}
	}

	entry, ok := s.idempotency[key]
	if !ok {
		return nil, false, nil
	}
	if entry.signature != signature {
		return nil, false, fmt.Errorf("idempotency key reused with different payload: %w", errInvokeIdempotencyConflict)
	}
	return cloneJSONValue(entry.result), true, nil
}

func (s *Server) storeIdempotency(key string, signature string, result any) {
	if strings.TrimSpace(key) == "" {
		return
	}
	s.idempotencyMu.Lock()
	s.idempotency[key] = idempotencyEntry{
		signature: signature,
		result:    cloneJSONValue(result),
		expiresAt: time.Now().Add(defaultIdempotencyTTL),
	}
	s.idempotencyMu.Unlock()
}

func cloneJSONValue(value any) any {
	raw, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return value
	}
	return out
}

func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	out := make(map[string]any, len(source))
	for key, value := range source {
		out[key] = cloneJSONValue(value)
	}
	return out
}

func cloneZones(source []Zone) []Zone {
	if source != nil && len(source) == 0 {
		return []Zone{}
	}
	if len(source) == 0 {
		return nil
	}
	out := make([]Zone, len(source))
	copy(out, source)
	return out
}

func cloneCircuits(source []CircuitStatus) []CircuitStatus {
	if source != nil && len(source) == 0 {
		return []CircuitStatus{}
	}
	if len(source) == 0 {
		return nil
	}
	out := make([]CircuitStatus, len(source))
	for i, status := range source {
		out[i] = cloneCircuitStatus(status)
	}
	return out
}

func cloneCircuitStatus(source CircuitStatus) CircuitStatus {
	out := source
	if source.State.PumpActive != nil {
		v := *source.State.PumpActive
		out.State.PumpActive = &v
	}
	if source.State.MixerPositionPct != nil {
		v := *source.State.MixerPositionPct
		out.State.MixerPositionPct = &v
	}
	if source.State.FlowTemperatureC != nil {
		v := *source.State.FlowTemperatureC
		out.State.FlowTemperatureC = &v
	}
	if source.State.FlowSetpointC != nil {
		v := *source.State.FlowSetpointC
		out.State.FlowSetpointC = &v
	}
	if source.State.CalcFlowTempC != nil {
		v := *source.State.CalcFlowTempC
		out.State.CalcFlowTempC = &v
	}
	if source.State.Humidity != nil {
		v := *source.State.Humidity
		out.State.Humidity = &v
	}
	if source.State.DewPoint != nil {
		v := *source.State.DewPoint
		out.State.DewPoint = &v
	}
	if source.State.PumpHours != nil {
		v := *source.State.PumpHours
		out.State.PumpHours = &v
	}
	if source.State.PumpStarts != nil {
		v := *source.State.PumpStarts
		out.State.PumpStarts = &v
	}
	if source.Config.HeatingCurve != nil {
		v := *source.Config.HeatingCurve
		out.Config.HeatingCurve = &v
	}
	if source.Config.FlowTempMaxC != nil {
		v := *source.Config.FlowTempMaxC
		out.Config.FlowTempMaxC = &v
	}
	if source.Config.FlowTempMinC != nil {
		v := *source.Config.FlowTempMinC
		out.Config.FlowTempMinC = &v
	}
	if source.Config.SummerLimitC != nil {
		v := *source.Config.SummerLimitC
		out.Config.SummerLimitC = &v
	}
	if source.Config.FrostProtC != nil {
		v := *source.Config.FrostProtC
		out.Config.FrostProtC = &v
	}
	if source.Config.CoolingEnabled != nil {
		v := *source.Config.CoolingEnabled
		out.Config.CoolingEnabled = &v
	}
	if out.ManagingDevice.Role == "" {
		out.ManagingDevice.Role = "UNKNOWN"
	}
	if source.ManagingDevice.DeviceID != nil {
		v := *source.ManagingDevice.DeviceID
		out.ManagingDevice.DeviceID = &v
	}
	if source.ManagingDevice.Address != nil {
		v := *source.ManagingDevice.Address
		out.ManagingDevice.Address = &v
	}
	return out
}

func cloneRadioDevices(source []RadioDevice) []RadioDevice {
	if source == nil {
		return nil
	}
	out := make([]RadioDevice, len(source))
	for i, device := range source {
		out[i] = cloneRadioDevice(device)
	}
	return out
}

func cloneRadioDevice(source RadioDevice) RadioDevice {
	out := source
	if source.DeviceConnected != nil {
		v := *source.DeviceConnected
		out.DeviceConnected = &v
	}
	if source.DeviceClassAddress != nil {
		v := *source.DeviceClassAddress
		out.DeviceClassAddress = &v
	}
	if source.FirmwareVersion != nil {
		v := *source.FirmwareVersion
		out.FirmwareVersion = &v
	}
	if source.HardwareIdentifier != nil {
		v := *source.HardwareIdentifier
		out.HardwareIdentifier = &v
	}
	if source.RemoteControlAddress != nil {
		v := *source.RemoteControlAddress
		out.RemoteControlAddress = &v
	}
	if source.DevicePaired != nil {
		v := *source.DevicePaired
		out.DevicePaired = &v
	}
	if source.ReceptionStrength != nil {
		v := *source.ReceptionStrength
		out.ReceptionStrength = &v
	}
	if source.ZoneAssignment != nil {
		v := *source.ZoneAssignment
		out.ZoneAssignment = &v
	}
	if source.RoomTemperatureC != nil {
		v := *source.RoomTemperatureC
		out.RoomTemperatureC = &v
	}
	if source.RoomHumidityPct != nil {
		v := *source.RoomHumidityPct
		out.RoomHumidityPct = &v
	}
	return out
}

func cloneMethodInfoList(source []methodInfo) []methodInfo {
	if len(source) == 0 {
		return nil
	}
	out := make([]methodInfo, len(source))
	copy(out, source)
	return out
}

func clonePlaneInfoList(source []planeInfo) []planeInfo {
	if len(source) == 0 {
		return nil
	}
	out := make([]planeInfo, len(source))
	for i, plane := range source {
		out[i] = planeInfo{
			Name:     plane.Name,
			Routable: plane.Routable,
			Methods:  cloneMethodInfoList(plane.Methods),
		}
	}
	return out
}

func cloneDeviceInfoList(source []deviceInfo) []deviceInfo {
	if len(source) == 0 {
		return nil
	}
	out := make([]deviceInfo, len(source))
	for i, device := range source {
		var aliases []int
		if len(device.Addresses) > 0 {
			aliases = make([]int, len(device.Addresses))
			copy(aliases, device.Addresses)
		}
		out[i] = deviceInfo{
			Address:           device.Address,
			Addresses:         aliases,
			Manufacturer:      device.Manufacturer,
			DeviceID:          device.DeviceID,
			SoftwareVersion:   device.SoftwareVersion,
			HardwareVersion:   device.HardwareVersion,
			Planes:            clonePlaneInfoList(device.Planes),
			DiscoverySource:   device.DiscoverySource,
			VerificationState: device.VerificationState,
		}
	}
	return out
}

func findDeviceInfoByAddress(devices []deviceInfo, address byte) (deviceInfo, bool) {
	target := int(address)
	for _, device := range devices {
		if device.Address == target {
			return device, true
		}
		// A.7b — canonical-pair aliasing collapses paired addresses
		// (e.g. 0x10/0x15) into one DeviceEntry whose Address() is the
		// primary; aliased addresses are exposed via Addresses(). The
		// snapshot must respect that or queries by alias address would
		// return "unknown device" when live registry would resolve.
		for _, alias := range device.Addresses {
			if alias == target {
				return device, true
			}
		}
	}
	return deviceInfo{}, false
}

type deviceInfo struct {
	Address         int         `json:"address"`
	Addresses       []int       `json:"addresses,omitempty"`
	Manufacturer    string      `json:"manufacturer"`
	DeviceID        string      `json:"device_id"`
	SoftwareVersion string      `json:"software_version"`
	HardwareVersion string      `json:"hardware_version"`
	Planes          []planeInfo `json:"planes"`
	// DiscoverySource encodes how the gateway learned about this
	// address: "passive_observed" (AD05 inserter from passive frames),
	// "static_seed" (productids LoadSeedTable planted at boot),
	// "active_confirmed" (startup scan / probe), or "" when the
	// registry has no slot record. Operators querying
	// `ebus.v1.registry.devices.list` use this to distinguish a
	// pre-known taxonomy entry from one that the gateway actively
	// confirmed at runtime — required to verify the P3.5 contract.
	DiscoverySource string `json:"discovery_source,omitempty"`
	// VerificationState encodes corroboration depth:
	// "candidate" (e.g. just-seeded, no wire confirmation yet),
	// "corroborated_pending" (passively observed atop a seed),
	// "identity_confirmed" (active scan response). Empty when the
	// registry has no slot record.
	VerificationState string `json:"verification_state,omitempty"`
}

type planeInfo struct {
	Name     string       `json:"name"`
	Routable bool         `json:"routable"`
	Methods  []methodInfo `json:"methods"`
}

type methodInfo struct {
	Name       string `json:"name"`
	ReadOnly   bool   `json:"read_only"`
	Primary    int    `json:"primary"`
	Secondary  int    `json:"secondary"`
	Mutability string `json:"mutability"`
	Danger     string `json:"danger_level"`
	Routable   bool   `json:"routable"`
}

func (s *Server) listDevices(snapshot *snapshotState) []deviceInfo {
	if snapshot != nil {
		return cloneDeviceInfoList(snapshot.devices)
	}
	out := make([]deviceInfo, 0)
	// P9 — race-free identity-field reads via IterateSnapshots. Each
	// callback receives a value-typed DeviceEntrySnapshot built under
	// the registry's RLock; the lock is released before the callback
	// runs. Planes are still fetched via a separate Lookup (residual
	// race surface for the Planes interface tree — deferred to a
	// Plane-aware ebusreg snapshot API; documented in the PR body).
	s.registry.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		// list view: project the primary address's slot state.
		// Per-alias detail is reachable via devices.get(address=alias).
		out = append(out, buildDeviceInfoFromSnapshot(snap, s.registry, snap.PrimaryDisplayAddress()))
		return true
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Address != out[j].Address {
			return out[i].Address < out[j].Address
		}
		if out[i].Manufacturer != out[j].Manufacturer {
			return out[i].Manufacturer < out[j].Manufacturer
		}
		return out[i].DeviceID < out[j].DeviceID
	})
	return out
}

func (s *Server) getDevice(args map[string]any, snapshot *snapshotState) (deviceInfo, error) {
	address, err := parseAddress(args["address"])
	if err != nil {
		return deviceInfo{}, err
	}
	if snapshot != nil {
		device, ok := findDeviceInfoByAddress(snapshot.devices, address)
		if !ok {
			return deviceInfo{}, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
		}
		// Snapshot stores one deviceInfo per entry with primary-
		// projected labels. Override with the queried address's
		// snapshotted slot labels so devices.get(address=alias) in
		// SNAPSHOT mode matches LIVE mode behavior (Codex P3.5 review
		// pass 4 finding #1).
		if labels, ok := snapshot.addressSlots[address]; ok {
			device.DiscoverySource = labels.discovery
			device.VerificationState = labels.verification
		}
		return device, nil
	}
	// P9.1 — race-free identity-field reads via LookupEntrySnapshot.
	// Identity fields come from the value-typed snapshot; Planes are
	// fetched via a live Lookup inside buildDeviceInfoFromSnapshot
	// (residual race surface: Planes interface tree only).
	snap, ok := s.registry.LookupEntrySnapshot(address)
	if !ok {
		return deviceInfo{}, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}
	// devices.get(address=X): project the QUERIED address's slot state
	// (Codex P3.5 review pass 3 thread 3). For merged entries with
	// aliases at different DiscoverySource levels, the operator gets
	// the slot they asked about — not the primary's potentially-
	// different label.
	return buildDeviceInfoFromSnapshot(snap, s.registry, address), nil
}

func (s *Server) listPlanes(args map[string]any, snapshot *snapshotState) ([]planeInfo, error) {
	address, err := parseAddress(args["address"])
	if err != nil {
		return nil, err
	}
	if snapshot != nil {
		device, ok := findDeviceInfoByAddress(snapshot.devices, address)
		if !ok {
			return nil, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
		}
		return clonePlaneInfoList(device.Planes), nil
	}
	entry, ok := s.registry.Lookup(address)
	if !ok {
		return nil, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}
	return buildPlaneInfoList(entry.Planes()), nil
}

func (s *Server) listMethods(args map[string]any, snapshot *snapshotState) ([]methodInfo, error) {
	address, err := parseAddress(args["address"])
	if err != nil {
		return nil, err
	}
	planeName, _ := args["plane"].(string)
	planeName = strings.TrimSpace(planeName)
	if planeName == "" {
		return nil, fmt.Errorf("missing plane: %w", ebuserrors.ErrInvalidPayload)
	}

	if snapshot != nil {
		device, ok := findDeviceInfoByAddress(snapshot.devices, address)
		if !ok {
			return nil, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
		}
		for _, plane := range device.Planes {
			if plane.Name == planeName {
				return cloneMethodInfoList(plane.Methods), nil
			}
		}
		return nil, fmt.Errorf("unknown plane %q: %w", planeName, ebuserrors.ErrInvalidPayload)
	}

	entry, ok := s.registry.Lookup(address)
	if !ok {
		return nil, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}
	plane, found := findPlane(entry.Planes(), planeName)
	if !found {
		return nil, fmt.Errorf("unknown plane %q: %w", planeName, ebuserrors.ErrInvalidPayload)
	}

	return buildMethodInfoList(plane), nil
}

func (s *Server) runtimeStatus(snapshot *snapshotState) map[string]any {
	if snapshot != nil {
		return cloneMap(snapshot.runtime)
	}
	return map[string]any{
		"daemon_status":  s.statusProvider.DaemonStatus(),
		"adapter_status": s.statusProvider.AdapterStatus(),
	}
}

func (s *Server) snapshotBusObservability(snapshot *snapshotState) BusObservabilitySnapshot {
	if snapshot != nil {
		return BusObservabilitySnapshot{
			Summary:     cloneBusSummary(snapshot.busSummary),
			Messages:    cloneBusMessages(snapshot.busMessages),
			Periodicity: cloneBusPeriodicity(snapshot.busPeriodicity),
		}
	}
	if s == nil || s.bus == nil {
		return BusObservabilitySnapshot{}
	}
	return cloneBusObservabilitySnapshot(s.bus.Snapshot())
}

func (s *Server) snapshotBusSummary(snapshot *snapshotState) *BusSummary {
	return s.snapshotBusObservability(snapshot).Summary
}

func (s *Server) snapshotProtocolSpecimens(family string, limit int) *BusProtocolSpecimenList {
	if s == nil || s.bus == nil {
		return &BusProtocolSpecimenList{}
	}
	items := s.bus.ProtocolSpecimens(family)
	total := len(items)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return &BusProtocolSpecimenList{
		Items: items,
		Count: total,
	}
}

func (s *Server) snapshotBusMessagesList(snapshot *snapshotState, limit int) *BusMessagesList {
	bus := s.snapshotBusObservability(snapshot)
	result := &BusMessagesList{
		Items: trimBusMessages(bus.Messages, limit),
	}
	if bus.Summary != nil {
		result.Status = cloneBusObservabilityStatus(bus.Summary.Status)
		result.Count = bus.Summary.Messages.Count
		result.Capacity = bus.Summary.Messages.Capacity
		return result
	}
	result.Count = len(bus.Messages)
	result.Capacity = len(bus.Messages)
	return result
}

func (s *Server) snapshotBusPeriodicityList(snapshot *snapshotState, limit int) *BusPeriodicityList {
	bus := s.snapshotBusObservability(snapshot)
	result := &BusPeriodicityList{
		Items: trimBusPeriodicity(bus.Periodicity, limit),
	}
	if bus.Summary != nil {
		result.Status = cloneBusObservabilityStatus(bus.Summary.Status)
		result.Count = bus.Summary.Periodicity.Count
		result.Capacity = bus.Summary.Periodicity.Capacity
		return result
	}
	result.Count = len(bus.Periodicity)
	result.Capacity = len(bus.Periodicity)
	return result
}

func (s *Server) snapshotWatchSummary(snapshot *snapshotState) *WatchSummary {
	if snapshot != nil {
		return cloneWatchSummary(snapshot.watchSummary)
	}
	if s == nil || s.watch == nil {
		return nil
	}
	watchSummary := s.watch.Snapshot()
	return cloneWatchSummary(&watchSummary)
}

func (s *Server) snapshotZones(snapshot *snapshotState) []Zone {
	if snapshot != nil {
		return cloneZones(snapshot.zones)
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	zones := s.semantic.Zones()
	if zones == nil {
		return nil
	}
	out := make([]Zone, len(zones))
	copy(out, zones)
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func (s *Server) snapshotCircuits(snapshot *snapshotState) []CircuitStatus {
	if snapshot != nil {
		return cloneCircuits(snapshot.circuits)
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	circuits := s.semantic.Circuits()
	if circuits == nil {
		return nil
	}
	out := cloneCircuits(circuits)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Index != out[j].Index {
			return out[i].Index < out[j].Index
		}
		return out[i].CircuitType < out[j].CircuitType
	})
	return out
}

func (s *Server) snapshotRadioDevices(snapshot *snapshotState) []RadioDevice {
	if snapshot != nil {
		return cloneRadioDevices(snapshot.radio)
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	devices := s.semantic.RadioDevices()
	if devices == nil {
		return nil
	}
	out := cloneRadioDevices(devices)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		if out[i].Instance != out[j].Instance {
			return out[i].Instance < out[j].Instance
		}
		return out[i].DeviceModel < out[j].DeviceModel
	})
	return out
}

func (s *Server) snapshotFM5Mode(snapshot *snapshotState) Fm5SemanticMode {
	if snapshot != nil {
		if snapshot.fm5Mode == "" {
			return Fm5SemanticModeAbsent
		}
		return snapshot.fm5Mode
	}
	if s == nil || s.semantic == nil {
		return Fm5SemanticModeAbsent
	}
	mode := s.semantic.FM5SemanticMode()
	if mode == "" {
		return Fm5SemanticModeAbsent
	}
	return mode
}

func (s *Server) snapshotFM5Interpretation(snapshot *snapshotState) Fm5Interpretation {
	if snapshot != nil {
		return snapshot.fm5Verdict
	}
	if s != nil && s.semantic != nil {
		if provider, ok := s.semantic.(FM5InterpretationProvider); ok {
			return provider.FM5Interpretation()
		}
	}
	return legacyFM5Interpretation(s.snapshotFM5Mode(nil))
}

func (s *Server) snapshotSolar(snapshot *snapshotState) *SolarStatus {
	if snapshot != nil {
		if snapshot.solar == nil {
			return nil
		}
		return cloneMCPSolarStatus(snapshot.solar)
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	source := s.semantic.Solar()
	if source == nil {
		return nil
	}
	return cloneMCPSolarStatus(source)
}

func (s *Server) snapshotCylinders(snapshot *snapshotState) []CylinderStatus {
	if snapshot != nil {
		return cloneCylinders(snapshot.cylinders)
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	cylinders := s.semantic.Cylinders()
	if cylinders == nil {
		return nil
	}
	out := cloneCylinders(cylinders)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Index < out[j].Index
	})
	return out
}

func (s *Server) snapshotDHW(snapshot *snapshotState) *DhwStatus {
	if snapshot != nil {
		if snapshot.dhw == nil {
			return nil
		}
		copy := *snapshot.dhw
		return &copy
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	source := s.semantic.DHW()
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}

func (s *Server) snapshotEnergyTotals(snapshot *snapshotState) *EnergyTotals {
	if snapshot != nil {
		if snapshot.energy == nil {
			return nil
		}
		copy := *snapshot.energy
		copy.Gas = cloneEnergyChannel(copy.Gas)
		copy.Electric = cloneEnergyChannel(copy.Electric)
		copy.Solar = cloneEnergyChannel(copy.Solar)
		return &copy
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	source := s.semantic.EnergyTotals()
	if source == nil {
		return nil
	}
	copy := *source
	copy.Gas = cloneEnergyChannel(copy.Gas)
	copy.Electric = cloneEnergyChannel(copy.Electric)
	copy.Solar = cloneEnergyChannel(copy.Solar)
	return &copy
}

func (s *Server) snapshotBoilerStatus(snapshot *snapshotState) *BoilerStatus {
	if snapshot != nil {
		if snapshot.boiler == nil {
			return nil
		}
		return cloneMCPBoilerStatus(snapshot.boiler)
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	source := s.semantic.BoilerStatus()
	if source == nil {
		return nil
	}
	return cloneMCPBoilerStatus(source)
}

func (s *Server) snapshotSystem(snapshot *snapshotState) *SystemStatus {
	if snapshot != nil {
		if snapshot.system == nil {
			return nil
		}
		return cloneMCPSystemStatus(snapshot.system)
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	source := s.semantic.System()
	if source == nil {
		return nil
	}
	return cloneMCPSystemStatus(source)
}

func (s *Server) snapshotSchedules(snapshot *snapshotState) *ScheduleStatus {
	if snapshot != nil {
		if snapshot.schedules == nil {
			return nil
		}
		return cloneMCPScheduleStatus(snapshot.schedules)
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	source := s.semantic.Schedules()
	if source == nil {
		return nil
	}
	return cloneMCPScheduleStatus(source)
}

func cloneMCPScheduleStatus(status *ScheduleStatus) *ScheduleStatus {
	if status == nil {
		return nil
	}
	cp := ScheduleStatus{}
	if len(status.Programs) > 0 {
		cp.Programs = make([]ScheduleProgram, len(status.Programs))
		for i, prog := range status.Programs {
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
				for j, day := range prog.Days {
					dayCopy := day
					if len(day.Slots) > 0 {
						dayCopy.Slots = make([]ScheduleTimerSlot, len(day.Slots))
						for k, slot := range day.Slots {
							slotCopy := slot
							if slot.TemperatureC != nil {
								v := *slot.TemperatureC
								slotCopy.TemperatureC = &v
							}
							if slot.TemperatureRaw != nil {
								v := *slot.TemperatureRaw
								slotCopy.TemperatureRaw = &v
							}
							dayCopy.Slots[k] = slotCopy
						}
					}
					out.Days[j] = dayCopy
				}
			}
			cp.Programs[i] = out
		}
	}
	return &cp
}

func (s *Server) snapshotAdapterInfo(snapshot *snapshotState) *AdapterHardwareInfo {
	if snapshot != nil {
		if snapshot.adapterInfo == nil {
			return nil
		}
		return cloneMCPAdapterHardwareInfo(snapshot.adapterInfo)
	}
	if s == nil || s.semantic == nil {
		return nil
	}
	source := s.semantic.AdapterHardwareInfo()
	if source == nil {
		return nil
	}
	return cloneMCPAdapterHardwareInfo(source)
}

func cloneMCPAdapterHardwareInfo(info *AdapterHardwareInfo) *AdapterHardwareInfo {
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

func cloneMCPBoilerStatus(status *BoilerStatus) *BoilerStatus {
	if status == nil {
		return nil
	}
	cp := *status
	if cp.State != nil {
		s := *cp.State
		s.FlowTemperatureC = cloneFloatPointer(s.FlowTemperatureC)
		s.ReturnTemperatureC = cloneFloatPointer(s.ReturnTemperatureC)
		s.CentralHeatingPumpActive = cloneBoolPointer(s.CentralHeatingPumpActive)
		s.WaterPressureBar = cloneFloatPointer(s.WaterPressureBar)
		s.ExternalPumpActive = cloneBoolPointer(s.ExternalPumpActive)
		s.CirculationPumpActive = cloneBoolPointer(s.CirculationPumpActive)
		s.GasValveActive = cloneBoolPointer(s.GasValveActive)
		s.FlameActive = cloneBoolPointer(s.FlameActive)
		s.DiverterValvePositionPct = cloneFloatPointer(s.DiverterValvePositionPct)
		s.FanSpeedRpm = cloneIntPointer(s.FanSpeedRpm)
		s.TargetFanSpeedRpm = cloneIntPointer(s.TargetFanSpeedRpm)
		s.IonisationVoltageUa = cloneFloatPointer(s.IonisationVoltageUa)
		s.DhwWaterFlowLpm = cloneFloatPointer(s.DhwWaterFlowLpm)
		s.DhwDemandActive = cloneBoolPointer(s.DhwDemandActive)
		s.HeatingSwitchActive = cloneBoolPointer(s.HeatingSwitchActive)
		s.StorageLoadPumpPct = cloneFloatPointer(s.StorageLoadPumpPct)
		s.ModulationPct = cloneFloatPointer(s.ModulationPct)
		s.PrimaryCircuitFlowLpm = cloneFloatPointer(s.PrimaryCircuitFlowLpm)
		s.FlowTempDesiredC = cloneFloatPointer(s.FlowTempDesiredC)
		s.DhwTempDesiredC = cloneFloatPointer(s.DhwTempDesiredC)
		s.StateNumber = cloneIntPointer(s.StateNumber)
		s.DhwTemperatureC = cloneFloatPointer(s.DhwTemperatureC)
		s.DhwTargetTemperatureC = cloneFloatPointer(s.DhwTargetTemperatureC)
		cp.State = &s
	}
	if cp.Config != nil {
		c := *cp.Config
		c.DhwOperatingMode = cloneStringPointer(c.DhwOperatingMode)
		c.FlowsetHcMaxC = cloneFloatPointer(c.FlowsetHcMaxC)
		c.FlowsetHwcMaxC = cloneFloatPointer(c.FlowsetHwcMaxC)
		c.PartloadHcKW = cloneFloatPointer(c.PartloadHcKW)
		c.PartloadHwcKW = cloneFloatPointer(c.PartloadHwcKW)
		c.InstallerMenuCode = cloneIntPointer(c.InstallerMenuCode)
		c.PhoneNumber = cloneStringPointer(c.PhoneNumber)
		c.HoursTillService = cloneIntPointer(c.HoursTillService)
		cp.Config = &c
	}
	if cp.Diagnostics != nil {
		d := *cp.Diagnostics
		d.HeatingStatusRaw = cloneIntPointer(d.HeatingStatusRaw)
		d.DhwStatusRaw = cloneIntPointer(d.DhwStatusRaw)
		d.CentralHeatingHours = cloneFloatPointer(d.CentralHeatingHours)
		d.DhwHours = cloneFloatPointer(d.DhwHours)
		d.CentralHeatingStarts = cloneIntPointer(d.CentralHeatingStarts)
		d.DhwStarts = cloneIntPointer(d.DhwStarts)
		d.PumpHours = cloneFloatPointer(d.PumpHours)
		d.FanHours = cloneFloatPointer(d.FanHours)
		d.DeactivationsIFC = cloneIntPointer(d.DeactivationsIFC)
		d.DeactivationsTemplimiter = cloneIntPointer(d.DeactivationsTemplimiter)
		cp.Diagnostics = &d
	}
	return &cp
}

func cloneBoolPointer(value *bool) *bool {
	if value == nil {
		return nil
	}
	v := *value
	return &v
}

func cloneFloatPointer(value *float64) *float64 {
	if value == nil {
		return nil
	}
	v := *value
	return &v
}

func cloneIntPointer(value *int) *int {
	if value == nil {
		return nil
	}
	v := *value
	return &v
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	v := *value
	return &v
}

func cloneMCPSolarStatus(status *SolarStatus) *SolarStatus {
	if status == nil {
		return nil
	}
	cp := *status
	if cp.CollectorTemperatureC != nil {
		v := *cp.CollectorTemperatureC
		cp.CollectorTemperatureC = &v
	}
	if cp.ReturnTemperatureC != nil {
		v := *cp.ReturnTemperatureC
		cp.ReturnTemperatureC = &v
	}
	if cp.PumpActive != nil {
		v := *cp.PumpActive
		cp.PumpActive = &v
	}
	if cp.CurrentYield != nil {
		v := *cp.CurrentYield
		cp.CurrentYield = &v
	}
	if cp.PumpHours != nil {
		v := *cp.PumpHours
		cp.PumpHours = &v
	}
	if cp.SolarEnabled != nil {
		v := *cp.SolarEnabled
		cp.SolarEnabled = &v
	}
	if cp.FunctionMode != nil {
		v := *cp.FunctionMode
		cp.FunctionMode = &v
	}
	return &cp
}

func cloneCylinder(status CylinderStatus) CylinderStatus {
	out := status
	if out.TemperatureC != nil {
		v := *out.TemperatureC
		out.TemperatureC = &v
	}
	if out.MaxSetpointC != nil {
		v := *out.MaxSetpointC
		out.MaxSetpointC = &v
	}
	if out.ChargeHysteresisC != nil {
		v := *out.ChargeHysteresisC
		out.ChargeHysteresisC = &v
	}
	if out.ChargeOffsetC != nil {
		v := *out.ChargeOffsetC
		out.ChargeOffsetC = &v
	}
	return out
}

func cloneCylinders(values []CylinderStatus) []CylinderStatus {
	if values == nil {
		return nil
	}
	out := make([]CylinderStatus, len(values))
	for i := range values {
		out[i] = cloneCylinder(values[i])
	}
	return out
}

func cloneMCPSystemStatus(status *SystemStatus) *SystemStatus {
	if status == nil {
		return nil
	}
	cp := *status
	if cp.State != nil {
		state := *cp.State
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
		cp.State = &state
	}
	if cp.Config != nil {
		config := *cp.Config
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
		if config.InstallerName != nil {
			v := *config.InstallerName
			config.InstallerName = &v
		}
		if config.InstallerPhone != nil {
			v := *config.InstallerPhone
			config.InstallerPhone = &v
		}
		if config.InstallerMenuCode != nil {
			v := *config.InstallerMenuCode
			config.InstallerMenuCode = &v
		}
		cp.Config = &config
	}
	if cp.Properties != nil {
		properties := *cp.Properties
		if properties.SystemScheme != nil {
			v := *properties.SystemScheme
			properties.SystemScheme = &v
		}
		if properties.ModuleConfigurationVR71 != nil {
			v := *properties.ModuleConfigurationVR71
			properties.ModuleConfigurationVR71 = &v
		}
		cp.Properties = &properties
	}
	return &cp
}

type semanticSnapshotOptions struct {
	planes       []string
	timeout      time.Duration
	allowPartial bool
}

func (s *Server) readSemanticSnapshot(ctx context.Context, args map[string]any, snapshot *snapshotState) (map[string]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	options, err := parseSemanticSnapshotOptions(args)
	if err != nil {
		return nil, err
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()

	data := make(map[string]any, len(options.planes))
	completed := make([]string, 0, len(options.planes))
	errorPlanes := make([]map[string]any, 0)

	perPlane := options.timeout / time.Duration(len(options.planes))
	if perPlane <= 0 {
		perPlane = options.timeout
	}

	for _, plane := range options.planes {
		select {
		case <-deadlineCtx.Done():
			err := deadlineCtx.Err()
			if options.allowPartial {
				errorPlanes = append(errorPlanes, newSnapshotPlaneError(plane, err))
				continue
			}
			return nil, err
		default:
		}

		planeCtx, planeCancel := context.WithTimeout(deadlineCtx, perPlane)
		value, planeErr := s.readSemanticPlane(planeCtx, plane, snapshot)
		planeCancel()
		if planeErr != nil {
			if options.allowPartial {
				errorPlanes = append(errorPlanes, newSnapshotPlaneError(plane, planeErr))
				continue
			}
			return nil, planeErr
		}
		data[plane] = value
		completed = append(completed, plane)
	}

	result := map[string]any{
		"planes":           data,
		"completed_planes": completed,
	}
	if options.allowPartial && len(errorPlanes) > 0 {
		result["error_planes"] = errorPlanes
	}
	return result, nil
}

func parseSemanticSnapshotOptions(args map[string]any) (semanticSnapshotOptions, error) {
	options := semanticSnapshotOptions{
		planes:       []string{"runtime_status", "zones", "dhw", "energy_totals", "boiler_status", "system", "circuits", "radio_devices", "fm5_mode", "solar", "cylinders", "schedules", "adapter_info"},
		timeout:      defaultSnapshotReadTTL,
		allowPartial: false,
	}
	if args == nil {
		return options, nil
	}

	parsedPlanes, err := parseSemanticSnapshotPlanes(args["planes"])
	if err != nil {
		return semanticSnapshotOptions{}, err
	}
	if len(parsedPlanes) > 0 {
		options.planes = parsedPlanes
	}

	if rawTimeout, ok := args["timeout_ms"]; ok {
		timeout, err := parseInvokeTimeout(rawTimeout)
		if err != nil {
			return semanticSnapshotOptions{}, err
		}
		if timeout > 0 {
			options.timeout = timeout
		}
	}

	if rawPartial, ok := args["allow_partial"]; ok {
		value, ok := rawPartial.(bool)
		if !ok {
			return semanticSnapshotOptions{}, fmt.Errorf("invalid allow_partial: %w", ebuserrors.ErrInvalidPayload)
		}
		options.allowPartial = value
	}

	return options, nil
}

func parseSemanticSnapshotPlanes(raw any) ([]string, error) {
	if raw == nil {
		return nil, nil
	}

	items, ok := raw.([]any)
	if !ok {
		if typed, ok := raw.([]string); ok {
			if len(typed) == 0 {
				return nil, nil
			}
			items = make([]any, 0, len(typed))
			for _, value := range typed {
				items = append(items, value)
			}
		} else {
			return nil, fmt.Errorf("invalid planes: %w", ebuserrors.ErrInvalidPayload)
		}
	}

	parsed := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		value, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("invalid planes item: %w", ebuserrors.ErrInvalidPayload)
		}
		normalized := strings.ToLower(strings.TrimSpace(value))
		switch normalized {
		case "runtime_status", "zones", "dhw", "energy_totals", "boiler_status", "system", "circuits", "radio_devices", "fm5_mode", "solar", "cylinders", "schedules", "adapter_info":
		default:
			return nil, fmt.Errorf("unsupported plane %q: %w", value, ebuserrors.ErrInvalidPayload)
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		parsed = append(parsed, normalized)
	}
	return parsed, nil
}

func (s *Server) readSemanticPlane(ctx context.Context, plane string, snapshot *snapshotState) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var value any
	switch plane {
	case "runtime_status":
		value = s.runtimeStatus(snapshot)
	case "zones":
		value = s.snapshotZones(snapshot)
	case "dhw":
		value = s.snapshotDHW(snapshot)
	case "energy_totals":
		value = s.snapshotEnergyTotals(snapshot)
	case "boiler_status":
		value = s.snapshotBoilerStatus(snapshot)
	case "system":
		value = s.snapshotSystem(snapshot)
	case "circuits":
		value = s.snapshotCircuits(snapshot)
	case "radio_devices":
		value = s.snapshotRadioDevices(snapshot)
	case "fm5_mode":
		value = s.snapshotFM5Mode(snapshot)
	case "solar":
		value = s.snapshotSolar(snapshot)
	case "cylinders":
		value = s.snapshotCylinders(snapshot)
	case "schedules":
		value = s.snapshotSchedules(snapshot)
	case "adapter_info":
		value = s.snapshotAdapterInfo(snapshot)
	default:
		return nil, fmt.Errorf("unsupported plane %q: %w", plane, ebuserrors.ErrInvalidPayload)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return value, nil
}

func newSnapshotPlaneError(plane string, err error) map[string]any {
	code, retriable, sourceLayer := classifyToolError(err)
	return map[string]any{
		"plane":        plane,
		"code":         code,
		"message":      err.Error(),
		"retriable":    retriable,
		"source_layer": sourceLayer,
	}
}

func cloneEnergyChannel(channel EnergyChannel) EnergyChannel {
	channel.DHW = cloneEnergySeries(channel.DHW)
	channel.Climate = cloneEnergySeries(channel.Climate)
	return channel
}

func cloneEnergySeries(series EnergySeries) EnergySeries {
	if len(series.Yearly) > 0 {
		values := make([]float64, len(series.Yearly))
		copy(values, series.Yearly)
		series.Yearly = values
	}
	if len(series.Monthly) > 0 {
		values := make([]float64, len(series.Monthly))
		copy(values, series.Monthly)
		series.Monthly = values
	}
	if len(series.YearlyMeta) > 0 {
		values := make([]EnergyPointMeta, len(series.YearlyMeta))
		copy(values, series.YearlyMeta)
		series.YearlyMeta = values
	}
	if len(series.MonthlyMeta) > 0 {
		values := make([]EnergyPointMeta, len(series.MonthlyMeta))
		copy(values, series.MonthlyMeta)
		series.MonthlyMeta = values
	}
	return series
}

// buildDeviceInfoFromSnapshot materialises the JSON view of a device
// entry from a value-typed DeviceEntrySnapshot.
//
// preferredAddr selects which AddressSlot is consulted for the
// top-level discovery_source/verification_state projection. For
// `ebus.v1.registry.devices.get(address=X)` the caller MUST pass the
// queried address X so operators querying a specific alias see THAT
// alias's slot state — not the primary's. This matters when a merged
// DeviceEntry has aliases at different DiscoverySource levels (e.g.
// NETX3's 0x04 broadcast face stays at static_seed/candidate while
// 0xF1 advances to active_confirmed/identity_confirmed via active
// scan). For `devices.list` callers pass the primary address so the
// list view reflects the entry-level "best known" provenance; the
// per-alias state for each address remains queryable via
// `devices.get(address=alias)`.
//
// preferredAddr=0 is treated as "use the primary"; this keeps existing
// internal callers that do not need per-alias precision working.
//
// Race-free counterpart of the legacy buildDeviceInfo (P9.1 — that
// helper was removed because all known callers migrated). Identity
// fields (Manufacturer / DeviceID / version strings / Addresses)
// come from the value-typed snapshot, which is disconnected from
// registry storage and immune to concurrent Register writes.
//
// Planes are still fetched via a live registry.Lookup of the primary
// address. The Planes interface tree is NOT included in the
// DeviceEntrySnapshot (P9 SCOPE note in helianthus-ebusreg), so this
// path retains the live-pointer read for Planes ONLY. The race
// surface for Planes is much narrower than the full identity-field
// surface — tracked as P9.2+ residual.
func buildDeviceInfoFromSnapshot(snap registry.DeviceEntrySnapshot, reg Registry, preferredAddr byte) deviceInfo {
	primary := snap.PrimaryDisplayAddress()
	if preferredAddr == 0 {
		preferredAddr = primary
	}
	all := snap.Addresses
	var aliases []int
	if len(all) > 0 {
		aliases = make([]int, 0, len(all))
		for _, a := range all {
			aliases = append(aliases, int(a))
		}
	} else {
		aliases = []int{int(primary)}
	}
	discovery, verification := lookupDiscoveryLabels(reg, preferredAddr)
	// Plane data: P9.1+ residual — DeviceEntrySnapshot doesn't
	// include Planes (interface tree). Fetch via live Lookup; the
	// Planes() read is still race-prone with concurrent Register.
	var planes []planeInfo
	if reg != nil {
		if entry, ok := reg.Lookup(primary); ok && entry != nil {
			planes = buildPlaneInfoList(entry.Planes())
		}
	}
	return deviceInfo{
		Address:           int(primary),
		Addresses:         aliases,
		Manufacturer:      snap.Manufacturer,
		DeviceID:          snap.DeviceID,
		SoftwareVersion:   snap.SoftwareVersion,
		HardwareVersion:   snap.HardwareVersion,
		Planes:            planes,
		DiscoverySource:   discovery,
		VerificationState: verification,
	}
}

// snapshotAddressSlots captures per-address discovery_source /
// verification_state labels at snapshot creation time so the
// SNAPSHOT-mode devices.get path can project the queried address's
// slot state instead of falling back to the cached deviceInfo's
// primary-address labels (Codex P3.5 review pass 4).
//
// P9 — uses IterateSnapshots / LookupSlotSnapshot so the iteration
// reads are race-free with concurrent registry writers (Register /
// RegisterPassiveObserved / MarkSlot*). The previous Iterate path
// dereferenced live entry pointers outside the registry's lock.
//
// Iterates the registry's known device entries and projects each
// alias address. Empty when the registry has no entries.
func (s *Server) snapshotAddressSlots() map[byte]addressSlotLabels {
	out := make(map[byte]addressSlotLabels)
	if s.registry == nil {
		return nil
	}
	s.registry.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		for _, addr := range snap.Addresses {
			discovery, verification := lookupDiscoveryLabels(s.registry, addr)
			if discovery == "" && verification == "" {
				continue
			}
			out[addr] = addressSlotLabels{discovery: discovery, verification: verification}
		}
		return true
	})
	if len(out) == 0 {
		return nil
	}
	return out
}

// lookupDiscoveryLabels projects the registry AddressSlot's enum
// state for the given address into the JSON-friendly string labels
// operators see in `ebus.v1.registry.devices.list` and friends.
// Mirrors the canonical mapping in helianthus-ebusgateway's
// address_table.go so MCP and the address-table snapshot agree on
// label spellings. Returns ("", "") when reg is nil or the slot is
// missing.
//
// P8.3: switched from LookupSlot (live pointer) to LookupSlotSnapshot
// (value copy under registry RLock) so the enum reads are race-free
// with concurrent registry writers (Register / RegisterPassiveObserved
// / MarkSlot*). The race detector flags the live-pointer pattern
// under contention; the snapshot eliminates that race surface.
func lookupDiscoveryLabels(reg Registry, addr byte) (discovery string, verification string) {
	if reg == nil {
		return "", ""
	}
	snap, ok := reg.LookupSlotSnapshot(addr)
	if !ok {
		return "", ""
	}
	switch snap.DiscoverySource {
	case registry.DiscoverySourcePassiveObserved:
		discovery = "passive_observed"
	case registry.DiscoverySourceStaticSeed:
		discovery = "static_seed"
	case registry.DiscoverySourceActiveConfirmed:
		discovery = "active_confirmed"
	}
	switch snap.VerificationState {
	case registry.VerificationStateCandidate:
		verification = "candidate"
	case registry.VerificationStateCorroborated:
		verification = "corroborated_pending"
	case registry.VerificationStateIdentityConfirmed:
		verification = "identity_confirmed"
	}
	return discovery, verification
}

func buildPlaneInfoList(planes []registry.Plane) []planeInfo {
	out := make([]planeInfo, 0, len(planes))
	for _, plane := range planes {
		if plane == nil {
			continue
		}
		_, routable := plane.(router.Plane)
		out = append(out, planeInfo{
			Name:     plane.Name(),
			Routable: routable,
			Methods:  buildMethodInfoList(plane),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func buildMethodInfoList(plane registry.Plane) []methodInfo {
	if plane == nil {
		return nil
	}
	methods := plane.Methods()
	out := make([]methodInfo, 0, len(methods))
	for _, method := range methods {
		if method == nil {
			continue
		}
		template := method.Template()
		primary := 0
		secondary := 0
		if template != nil {
			primary = int(template.Primary())
			secondary = int(template.Secondary())
		}
		mutability, danger, routable := extractMethodMetadata(method, plane)
		out = append(out, methodInfo{
			Name:       method.Name(),
			ReadOnly:   method.ReadOnly(),
			Primary:    primary,
			Secondary:  secondary,
			Mutability: mutability,
			Danger:     danger,
			Routable:   routable,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		if out[i].Primary != out[j].Primary {
			return out[i].Primary < out[j].Primary
		}
		return out[i].Secondary < out[j].Secondary
	})
	return out
}

func extractMethodMetadata(method registry.Method, plane registry.Plane) (mutability string, danger string, routable bool) {
	if method == nil {
		return methodMutabilityUnknown, methodDangerUnknown, false
	}

	if method.ReadOnly() {
		mutability = methodMutabilityReadOnly
	} else {
		mutability = methodMutabilityMutating
	}
	if value, ok := invokeStringNoArg(method, "Mutability"); ok {
		if normalized, valid := normalizeMethodMutability(value); valid {
			mutability = normalized
		}
	}

	if mutability == methodMutabilityReadOnly {
		danger = methodDangerSafe
	} else {
		danger = methodDangerDangerous
	}
	if value, ok := invokeStringNoArg(method, "Danger"); ok {
		if normalized, valid := normalizeMethodDanger(value); valid {
			danger = normalized
		}
	}

	_, routable = plane.(router.Plane)
	if value, ok := invokeBoolNoArg(method, "Routable"); ok {
		routable = value
	}

	return mutability, danger, routable
}

func normalizeMethodMutability(raw string) (string, bool) {
	value := strings.ToLower(strings.TrimSpace(raw))
	value = strings.ReplaceAll(value, "-", "_")
	switch value {
	case methodMutabilityUnknown, methodMutabilityReadOnly, methodMutabilityMutating:
		return value, true
	default:
		return "", false
	}
}

func normalizeMethodDanger(raw string) (string, bool) {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch value {
	case methodDangerUnknown, methodDangerSafe, methodDangerDangerous:
		return value, true
	default:
		return "", false
	}
}

func invokeStringNoArg(target any, methodName string) (string, bool) {
	if target == nil {
		return "", false
	}
	value := reflect.ValueOf(target)
	method := value.MethodByName(methodName)
	if !method.IsValid() || method.Type().NumIn() != 0 || method.Type().NumOut() != 1 {
		return "", false
	}
	out := method.Call(nil)
	if len(out) != 1 {
		return "", false
	}
	return fmt.Sprint(out[0].Interface()), true
}

func invokeBoolNoArg(target any, methodName string) (bool, bool) {
	if target == nil {
		return false, false
	}
	value := reflect.ValueOf(target)
	method := value.MethodByName(methodName)
	if !method.IsValid() || method.Type().NumIn() != 0 || method.Type().NumOut() != 1 || method.Type().Out(0).Kind() != reflect.Bool {
		return false, false
	}
	out := method.Call(nil)
	if len(out) != 1 {
		return false, false
	}
	return out[0].Bool(), true
}

func findPlane(planes []registry.Plane, planeName string) (registry.Plane, bool) {
	for _, plane := range planes {
		if plane != nil && plane.Name() == planeName {
			return plane, true
		}
	}
	return nil, false
}

func (s *Server) invoke(ctx context.Context, args map[string]any) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("missing arguments: %w", ebuserrors.ErrInvalidPayload)
	}

	address, err := parseAddress(args["address"])
	if err != nil {
		return nil, err
	}
	planeName, _ := args["plane"].(string)
	if planeName == "" {
		return nil, fmt.Errorf("missing plane: %w", ebuserrors.ErrInvalidPayload)
	}
	methodName, _ := args["method"].(string)
	if methodName == "" {
		return nil, fmt.Errorf("missing method: %w", ebuserrors.ErrInvalidPayload)
	}
	// Gateway-owned rpc.invoke calls default to the admitted startup source.
	// An explicit params.source is accepted as the user override path after
	// byte-shape validation.
	rpcSource, rpcSourceAdmitted := s.admittedRPCSource()
	params, err := enforceRPCSourceOnArgs(args, rpcSource, rpcSourceAdmitted)
	if err != nil {
		return nil, err
	}

	entry, ok := s.registry.Lookup(address)
	if !ok {
		return nil, fmt.Errorf("unknown device 0x%02x: %w", address, ebuserrors.ErrNoSuchDevice)
	}

	var plane registry.Plane
	for _, candidate := range entry.Planes() {
		if candidate != nil && candidate.Name() == planeName {
			plane = candidate
			break
		}
	}
	if plane == nil {
		return nil, fmt.Errorf("unknown plane %q: %w", planeName, ebuserrors.ErrInvalidPayload)
	}

	routerPlane, ok := plane.(router.Plane)
	if !ok {
		return nil, fmt.Errorf("plane %q not invokable: %w", planeName, ebuserrors.ErrInvalidPayload)
	}

	return s.invoker.Invoke(ctx, routerPlane, methodName, params)
}

func parseAddress(raw any) (byte, error) {
	switch value := raw.(type) {
	case int:
		return toAddress(value)
	case int64:
		return toAddress(int(value))
	case float64:
		return toAddress(int(value))
	default:
		return 0, fmt.Errorf("invalid address: %w", ebuserrors.ErrInvalidPayload)
	}
}

func toAddress(value int) (byte, error) {
	if value < 0 || value > 0xFF {
		return 0, fmt.Errorf("invalid address: %w", ebuserrors.ErrInvalidPayload)
	}
	return byte(value), nil
}

type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema"`
}

func callToolResultText(text string, isError bool) map[string]any {
	return map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": text},
		},
		"isError": isError,
	}
}

func mustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(data)
}

func newSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *rpcError `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (s *Server) writeRPCResult(w http.ResponseWriter, id any, result any) {
	s.writeRPC(w, rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func (s *Server) writeRPCError(w http.ResponseWriter, id any, err *rpcError) {
	s.writeRPC(w, rpcResponse{JSONRPC: "2.0", ID: id, Error: err})
}

func (s *Server) writeRPC(w http.ResponseWriter, response rpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(response)
}

func rpcErrorInvalidRequest(message string) *rpcError {
	return &rpcError{Code: -32600, Message: message}
}

func rpcErrorMethodNotFound(message string) *rpcError {
	return &rpcError{Code: -32601, Message: message}
}

func rpcErrorInvalidParams(message string) *rpcError {
	return &rpcError{Code: -32602, Message: message}
}

func rpcErrorInternal(message string) *rpcError {
	return &rpcError{Code: -32603, Message: message}
}
