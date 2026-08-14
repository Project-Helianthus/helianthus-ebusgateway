package main

import (
	"context"
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"log"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

const (
	vaillantExtRegisterPrimary   = byte(0xB5)
	vaillantExtRegisterSecondary = byte(0x24)
	vaillantB509Primary          = byte(0xB5)
	vaillantB509Secondary        = byte(0x09)
	vaillantB509OpcodeRead       = byte(0x0D)
	vaillantB509OpcodeWrite      = byte(0x0E)
	vaillantB524OpcodeRead       = byte(0x06)
	vaillantB524OpcodeLocal      = byte(0x02)
	vaillantB524OpRead           = byte(0x00)
	vaillantB524OpWrite          = byte(0x01)

	// --- Zone registers: GG=0x03 / OP=0x02 ---

	// STATE registers (read-only live values)
	zone_current_temp        = uint16(0x000F) // state.current_room_temperature
	zone_special_function    = uint16(0x000E) // state.current_special_function
	zone_valve_status        = uint16(0x0012) // state.valve_status
	zone_current_humidity    = uint16(0x0028) // state.current_room_humidity
	zone_quick_veto_end_time = uint16(0x001E) // state.quick_veto.end_time (HH:MM:SS)
	zone_quick_veto_end_date = uint16(0x0024) // state.quick_veto.end_date (DD.MM.YY)

	// CONFIG registers (user-writable settings)
	zone_heating_op_mode                   = uint16(0x0006) // configuration.heating.operation_mode
	zone_target_temp                       = uint16(0x0022) // configuration.heating.desired_setpoint
	zone_fallback_manual_temp              = uint16(0x0014) // configuration.heating.manual_mode_setpoint
	zone_room_temperature_zone_mapping_raw = uint16(0x0013) // configuration.room_temperature_zone_mapping
	zone_quick_veto_temp                   = uint16(0x0008) // configuration.quick_veto.temperature (f32 LE)
	zone_quick_veto_duration               = uint16(0x0026) // configuration.quick_veto.duration_hours (f32 LE)
	zone_holiday_start_date                = uint16(0x0003) // configuration.holiday.start_date (DD.MM.YY)
	zone_holiday_end_date                  = uint16(0x0004) // configuration.holiday.end_date (DD.MM.YY)
	zone_holiday_setpoint                  = uint16(0x0005) // configuration.holiday.setpoint (f32 LE)
	zone_holiday_end_time                  = uint16(0x0020) // configuration.holiday.end_time (HH:MM)
	zone_holiday_start_time                = uint16(0x0021) // configuration.holiday.start_time (HH:MM)

	// PARAMS registers (identification / metadata)
	zone_name        = uint16(0x0016)
	zone_name_prefix = uint16(0x0017)
	zone_name_suffix = uint16(0x0018)
	zone_index       = uint16(0x001C)

	// --- Circuit registers: GG=0x02 / OP=0x02 ---

	// STATE registers (read-only live values)
	circuit_flow_setpoint  = uint16(0x0007) // flow_setpoint
	circuit_flow_temp      = uint16(0x0008) // flow_temperature (VF[x])
	circuit_circuit_state  = uint16(0x001B) // circuit_state
	circuit_pump_status    = uint16(0x001E) // pump_status
	circuit_calc_flow_temp = uint16(0x0020) // calculated_flow_temperature
	circuit_mixer_position = uint16(0x0021) // mixer_position_pct
	circuit_humidity       = uint16(0x0022) // room_humidity_pct
	circuit_dew_point      = uint16(0x0023) // dew_point_temperature
	circuit_pump_hours     = uint16(0x0024) // pump_operating_hours
	circuit_pump_starts    = uint16(0x0025) // pump_starts

	// CONFIG registers (user-writable settings)
	circuit_type              = uint16(0x0002) // configuration.heating_circuit_type / mixer_circuit_type_external
	circuit_cooling_enabled   = uint16(0x0006) // cooling_enabled
	circuit_heating_curve     = uint16(0x000F) // heating_curve
	circuit_flow_temp_max     = uint16(0x0010) // flow_temperature_max
	circuit_flow_temp_min     = uint16(0x0012) // flow_temperature_min
	circuit_summer_limit      = uint16(0x0014) // summer_limit
	circuit_room_temp_control = uint16(0x0015) // room_temperature_control_mode
	circuit_frost_protection  = uint16(0x001D) // frost_protection_threshold

	dhwPseudoCircuitInstance = byte(0x09)
	dhwPseudoCircuitTypeRaw  = uint16(0x0003)

	CircuitStateStandby = "standby"
	CircuitStateHeating = "heating"
	CircuitStateCooling = "cooling"

	// --- DHW registers: GG=0x01 / OP=0x02 ---

	// STATE registers (read-only live values)
	dhw_current_temp     = uint16(0x0005) // state.current_dhw_temperature
	dhw_special_function = uint16(0x000D) // state.current_special_function

	// CONFIG registers (user-writable settings)
	dhw_operation_mode     = uint16(0x0003) // configuration.domestic_hot_water.operation_mode
	dhw_target_temp        = uint16(0x0004) // configuration.domestic_hot_water.tapping_setpoint
	dhw_holiday_start_date = uint16(0x0009) // configuration.holiday.start_date (DD.MM.YY)
	dhw_holiday_end_date   = uint16(0x000A) // configuration.holiday.end_date (DD.MM.YY)

	dhwInstance = byte(0x00)

	// --- Device slot registers: OP=0x06 (remote) ---

	// STATE registers (read-only live values)
	device_slot_connected          = uint16(0x0001)
	device_slot_room_humidity      = uint16(0x0007)
	device_slot_room_temperature   = uint16(0x000F)
	device_slot_paired             = uint16(0x001E)
	device_slot_reception_strength = uint16(0x001F)

	// PARAMS registers (identification / metadata)
	device_slot_class_address          = uint16(0x0002)
	device_slot_firmware               = uint16(0x0004)
	device_slot_remote_control_address = uint16(0x0019)
	device_slot_hardware_identifier    = uint16(0x0023)
	device_slot_zone_assignment        = uint16(0x0025)

	solarInstance = byte(0x00)

	// --- Solar registers: GG=0x04 / OP=0x02 ---

	// STATE registers (read-only live values)
	solar_collector_temp = uint16(0x0003)
	solar_return_temp    = uint16(0x0007)
	solar_pump_active    = uint16(0x0008)
	solar_current_yield  = uint16(0x0009)
	solar_pump_hours     = uint16(0x000B)

	// CONFIG registers (user-writable settings)
	solar_enabled       = uint16(0x0001)
	solar_function_mode = uint16(0x0002)

	// --- Cylinder registers: GG=0x05 / OP=0x02 ---

	// STATE registers (read-only live values)
	cylinder_temperature = uint16(0x0004)

	// CONFIG registers (user-writable settings)
	cylinder_max_setpoint      = uint16(0x0001)
	cylinder_charge_hysteresis = uint16(0x0002)
	cylinder_charge_offset     = uint16(0x0003)
)

// b524GroupDef binds a B524 group number to its owning opcode.
// This eliminates the possibility of accidentally mixing OP/GG pairs.
// See AGENTS.md §1.3.1 B524 Register Namespace Contract.
type b524GroupDef struct {
	group  byte
	opcode byte
	name   string
}

// OP=0x02 (local/controller) groups.
var (
	localRegulator = b524GroupDef{group: 0x00, opcode: vaillantB524OpcodeLocal, name: "regulator_parameters"}
	localDHW       = b524GroupDef{group: 0x01, opcode: vaillantB524OpcodeLocal, name: "hot_water_circuit"}
	localCircuits  = b524GroupDef{group: 0x02, opcode: vaillantB524OpcodeLocal, name: "heating_circuits"}
	localZones     = b524GroupDef{group: 0x03, opcode: vaillantB524OpcodeLocal, name: "zones"}
	localSolar     = b524GroupDef{group: 0x04, opcode: vaillantB524OpcodeLocal, name: "solar_circuit"}
	localCylinders = b524GroupDef{group: 0x05, opcode: vaillantB524OpcodeLocal, name: "hot_water_cylinder"}
)

// OP=0x06 (remote/device) groups — all gated on device_connected (RR=0x0001).
var (
	remoteRegulators        = b524GroupDef{group: 0x09, opcode: vaillantB524OpcodeRead, name: "regulators"}
	remoteThermostats       = b524GroupDef{group: 0x0A, opcode: vaillantB524OpcodeRead, name: "thermostats"}
	remoteFunctionalModules = b524GroupDef{group: 0x0C, opcode: vaillantB524OpcodeRead, name: "functional_modules"}
)

// remoteDeviceGroups lists the OP=0x06 groups that the gateway actively polls.
var remoteDeviceGroups = []b524GroupDef{
	remoteRegulators,
	remoteThermostats,
	remoteFunctionalModules,
}

const (
	semanticStartupProbeTimeout        = 900 * time.Millisecond
	semanticStartupSlotFastMaxInstance = byte(0x02)
	semanticStartupSlotFullMaxInstance = byte(0x0A)
	semanticStartupCriticalDHWAttempts = 3
	semanticCircuitFullScanInterval    = 30 * time.Minute
	semanticCircuitPartialScanInterval = 5 * time.Minute
)

var (
	semanticStartupPrimingBudget     = 35 * time.Second
	semanticStartupPrimingRetryDelay = 500 * time.Millisecond
)

// deviceSlotKey identifies a single device slot by its OP=0x06 group+instance.
type deviceSlotKey struct {
	Group    byte
	Instance byte
}

// B5.55 timer/schedule protocol constants.
const (
	vaillantB555Primary   = byte(0xB5)
	vaillantB555Secondary = byte(0x55)

	b555OpcodeConfigRead = byte(0xA3)
	b555OpcodeSlotsRead  = byte(0xA4)
	b555OpcodeTimerRead  = byte(0xA5)
	b555OpcodeTimerWrite = byte(0xA6)

	b555HCHeating = byte(0x00)
	b555HCCooling = byte(0x01)
	b555HCDHW     = byte(0x02)
	b555HCCC      = byte(0x03)
	b555HCSilent  = byte(0x04)
	b555ZoneDHW   = byte(0xFF) // zone-agnostic selector for DHW/CC/Silent
)

var b555WeekdayNames = [7]string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}

var b555HCNames = map[byte]string{
	b555HCHeating: "heating",
	b555HCCooling: "cooling",
	b555HCDHW:     "dhw",
	b555HCCC:      "circulation",
	b555HCSilent:  "silent",
}

// --- Energy registers: GG=0x00/OP=0x02 (shared with system/regulator) ---
// energy4 type = ULG (unsigned 32-bit LE) in kWh.
const (
	// STATE registers (read-only counters)
	energy_fuel_sum_hc                    = uint16(0x0056) // PrFuelSumHc: total gas consumption heating
	energy_electricity_sum_hc             = uint16(0x0057) // PrEnergySumHc: total electricity consumption heating
	energy_electricity_sum_hwc            = uint16(0x0058) // PrEnergySumHwc: total electricity consumption hot water
	energy_fuel_sum_hwc                   = uint16(0x0059) // PrFuelSumHwc: total gas consumption hot water
	energy_fuel_sum_hc_this_month         = uint16(0x004E) // PrFuelSumHcThisMonth: gas heating this month
	energy_electricity_sum_hc_this_month  = uint16(0x004F) // PrEnergySumHcThisMonth: electricity heating this month
	energy_electricity_sum_hwc_this_month = uint16(0x0050) // PrEnergySumHwcThisMonth: electricity hot water this month
	energy_fuel_sum_hwc_this_month        = uint16(0x0051) // PrFuelSumHwcThisMonth: gas hot water this month
	energy_fuel_sum_hc_last_month         = uint16(0x0052) // PrFuelSumHcLastMonth: gas heating last month
	energy_electricity_sum_hc_last_month  = uint16(0x0053) // PrEnergySumHcLastMonth: electricity heating last month
	energy_electricity_sum_hwc_last_month = uint16(0x0054) // PrEnergySumHwcLastMonth: electricity hot water last month
	energy_fuel_sum_hwc_last_month        = uint16(0x0055) // PrFuelSumHwcLastMonth: gas hot water last month
)

type regulatorAbsenceState string

const (
	regulatorPresent      regulatorAbsenceState = "PRESENT"
	regulatorAbsenceGrace regulatorAbsenceState = "ABSENCE_GRACE"
	regulatorAbsent       regulatorAbsenceState = "ABSENT"
)

var (
	semanticReadBreakerTransitionsTotal  = expvar.NewMap("semantic_read_breaker_transitions_total")
	semanticReadBreakerSuppressedTotal   = expvar.NewMap("semantic_read_breaker_suppressed_total")
	semanticZonePresenceTransitionsTotal = expvar.NewMap("semantic_zone_presence_transitions_total")
	semanticZoneCount                    = expvar.NewInt("semantic_zone_count")
	semanticDHWStaleExpiryTotal          = expvar.NewInt("semantic_dhw_stale_expiry_total")
	semanticDHWUpdatesTotal              = expvar.NewInt("semantic_dhw_updates_total")
	semanticRegulatorState               = expvar.NewString("semantic_regulator_state")
	semanticRegulatorTransitionsTotal    = expvar.NewMap("semantic_regulator_transitions_total")

	passiveShadowSubscriberPriority = ebusgateway.DedupSubscriberCritical
	passiveShadowSubscriberBuffer   = 256
	passiveShadowRetryDelay         = 100 * time.Millisecond
	defaultFM5EvidenceTTL           = 30 * time.Minute
)

func startVaillantSemanticPolling(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub, startupBarrier <-chan struct{}) *vaillantSemanticPoller {
	if gateway == nil || gateway.Bus == nil || gateway.Registry == nil || provider == nil {
		return nil
	}

	cacheStore := newSemanticCacheStore(cfg.SemanticCachePath, log.Printf)
	cacheSnapshot, cacheLoaded := preloadSemanticCache(provider, cacheStore)
	poller := newVaillantSemanticPoller(cfg, gateway, provider, hub, cacheStore)
	poller.startupBarrier = startupBarrier
	if cacheLoaded {
		poller.hydrateFromCache(cacheSnapshot)
	}
	poller.Start(ctx)
	return poller
}

type vaillantSemanticPoller struct {
	scheduler *ebusgateway.SemanticReadScheduler
	tasks     *semanticTaskScheduler

	reg      *registry.DeviceRegistry
	bus      *protocol.Bus
	provider *graphql.LiveSemanticProvider
	hub      *graphql.BroadcastHub
	cache    semanticCachePersister
	shadow   *ebusgateway.ShadowCache

	transportConfig ebusgateway.TransportConfig
	watchObserver   ebusgateway.WatchObserver
	watchEfficiency ebusgateway.WatchEfficiencyObserver

	source                   byte
	companion                byte
	requestTimeout           time.Duration
	discoveryInterval        time.Duration
	configInterval           time.Duration
	stateInterval            time.Duration
	energyInterval           time.Duration
	scheduleInterval         time.Duration
	boilerFastInterval       time.Duration
	boilerMediumInterval     time.Duration
	boilerSlowInterval       time.Duration
	zoneMissThreshold        int
	zoneHitThreshold         int
	dhwStaleTTL              time.Duration
	deviceSlotRediscoveryTTL time.Duration
	fm5EvidenceTTL           time.Duration
	circuitFullScanInterval  time.Duration

	pollMu sync.Mutex
	readMu sync.Mutex

	// identityProbedAddresses tracks per-address probe attempts for
	// EnqueueAddressIdentityProbe (P5). Each address is probed at
	// most once per gateway lifetime; failures are not retried.
	identityProbedAddresses sync.Map // map[byte]struct{}

	catalog    productids.Catalog
	catalogErr error

	mu                          sync.Mutex
	controller                  byte
	boilerAddress               byte
	regulatorCapability         productids.ControllerCapability
	regAbsenceState             regulatorAbsenceState
	regAbsenceSince             time.Time
	registryDeviceCount         int
	regulatorRecheckInterval    time.Duration
	regulatorAbsenceGrace       time.Duration
	zones                       map[byte]*vaillantZoneSnapshot
	presence                    map[byte]*zonePresenceRecord
	dhw                         *vaillantDhwSnapshot
	dhwLastUpdateAt             time.Time
	boiler                      *vaillantBoilerSnapshot
	system                      *vaillantSystemSnapshot
	circuits                    map[byte]*vaillantCircuitSnapshot
	lastCircuitFullScanAt       time.Time
	lastCircuitFullScanComplete bool
	radioDevices                map[radioDeviceKey]*vaillantRadioDeviceSnapshot
	deviceSlotCache             map[deviceSlotKey]bool // OP=0x06 slots retained for steady-state detail refresh
	deviceSlotDiscoveryDone     bool
	deviceSlotDiscoveryAt       time.Time
	startupSemanticPrimed       bool
	startupRadioDevicesProbed   bool
	fm5Mode                     graphql.Fm5SemanticMode
	fm5Interpretation           graphql.Fm5Interpretation
	fm5EvidenceRevision         uint64
	fm5EvidenceGeneration       uint64
	fm5IdentityScanComplete     bool
	fm5IdentityIncoherent       bool
	fm5RegistryEvidenceIgnored  bool
	fm5IdentityObservedAt       time.Time
	solar                       *vaillantSolarSnapshot
	solarCylinders              map[byte]*vaillantCylinderSnapshot

	adapterInfo    *vaillantAdapterInfoState
	startupBarrier <-chan struct{}

	refreshFromEbusdGrabFn       func(context.Context) (map[byte]bool, bool)
	b524ProbeFn                  func(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool
	sendFrameFn                  func(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error)
	routerPlanesRefreshFn        func()
	withFM5ObservationGeneration func(func(uint64)) bool
	nowFn                        func() time.Time
}

type regulatorEnrichment struct {
	family   string
	deviceID string
}

type b524ProbeSpec struct {
	opcode   byte
	group    byte
	instance byte
	addr     uint16
}

// b524CapabilityProbes lists registers from different GG groups used for
// multi-register coherency verification during capability-first discovery.
var b524CapabilityProbes = []b524ProbeSpec{
	{opcode: localRegulator.opcode, group: localRegulator.group, instance: regulatorInstance, addr: 0x0001},
	{opcode: localDHW.opcode, group: localDHW.group, instance: dhwInstance, addr: dhw_operation_mode},
}

type zonePresenceState string

const (
	zonePresenceAbsent           zonePresenceState = "ABSENT"
	zonePresenceSuspectResurrect zonePresenceState = "SUSPECT_RESURRECT"
	zonePresencePresent          zonePresenceState = "PRESENT"
	zonePresenceSuspectMissing   zonePresenceState = "SUSPECT_MISSING"
)

type zonePresenceRecord struct {
	State      zonePresenceState
	HitStreak  int
	MissStreak int
}

type vaillantZoneSnapshot struct {
	Instance byte
	Present  bool

	Name string

	OperatingMode string
	Preset        string
	HvacAction    string
	AllowedModes  []string

	CurrentTempC *float64
	TargetTempC  *float64
	HumidityPct  *float64

	ConfigurationHeatingOperationMode          string
	StateSpecialFunction                       string
	ConfigurationRoomTemperatureZoneMappingRaw *uint16
	ConfigurationAssociatedCircuitRaw          *uint16
	ConfigurationCircuitTypeRaw                *uint16
	StateValveStatusRaw                        *uint16

	QuickVetoTempC     *float64
	QuickVetoDurationH *float64
	QuickVetoEndTime   string // "HH:MM" or "" if no veto
	QuickVetoEndDate   string // "YYYY-MM-DD" or "" if no veto

	HolidayStartDate string   // "YYYY-MM-DD" or ""
	HolidayEndDate   string   // "YYYY-MM-DD" or ""
	HolidaySetpointC *float64 // configured setpoint during holiday
	HolidayStartTime string   // "HH:MM" or ""
	HolidayEndTime   string   // "HH:MM" or ""

	FieldFreshness map[semanticFieldKey]semanticFieldFreshness
}

type vaillantDhwSnapshot struct {
	OperatingMode string
	Preset        string

	CurrentTempC *float64
	TargetTempC  *float64

	ConfigurationDHWOperationMode string
	StateSpecialFunction          string

	HolidayStartDate string // "YYYY-MM-DD" or ""
	HolidayEndDate   string // "YYYY-MM-DD" or ""

	FieldFreshness map[semanticFieldKey]semanticFieldFreshness
}

type vaillantCircuitSnapshot struct {
	Instance   byte
	Active     bool
	Controller byte

	CircuitTypeRaw     *uint16
	CoolingEnabledRaw  *uint16
	FlowSetpointC      *float64
	FlowTemperatureC   *float64
	HeatingCurve       *float64
	FlowTempMaxC       *float64
	FlowTempMinC       *float64
	SummerLimitC       *float64
	RoomTempControlRaw *uint16
	CircuitStateRaw    *uint16
	CircuitStateLiveAt time.Time
	FrostProtectionC   *float64
	PumpStatusRaw      *uint16
	PumpStatusLiveAt   time.Time
	CalcFlowTempC      *float64
	MixerPositionPct   *float64
	HumidityPct        *float64
	DewPointC          *float64
	PumpHoursRaw       *uint32
	PumpStartsRaw      *uint32
}

type radioDeviceKey struct {
	Group    byte
	Instance byte
}

type vaillantRadioDeviceSnapshot struct {
	Group                byte
	Instance             byte
	SlotMode             string
	DeviceConnected      *bool
	DeviceClassAddress   *uint8
	DeviceModel          string
	FirmwareVersion      *string
	HardwareIdentifier   *uint16
	RemoteControlAddress *uint8
	DevicePaired         *bool
	ReceptionStrength    *uint8
	ZoneAssignment       *uint8
	RoomTemperatureC     *float64
	RoomHumidityPct      *float64
}

type vaillantSolarSnapshot struct {
	CollectorTemperatureC *float64
	ReturnTemperatureC    *float64
	PumpActive            *bool
	CurrentYield          *float64
	PumpHours             *uint32
	SolarEnabled          *bool
	FunctionMode          *bool
}

type vaillantCylinderSnapshot struct {
	Instance         byte
	TemperatureC     *float64
	MaxSetpointC     *float64
	ChargeHysteresis *float64
	ChargeOffset     *float64
}

type semanticSnapshotSource uint8

const (
	semanticSnapshotSourceCache semanticSnapshotSource = iota
	semanticSnapshotSourceLive
)

type semanticFieldKey string

const (
	zoneFieldName                          semanticFieldKey = "zone.name"
	zoneFieldOperatingMode                 semanticFieldKey = "zone.operating_mode"
	zoneFieldPreset                        semanticFieldKey = "zone.preset"
	zoneFieldHvacAction                    semanticFieldKey = "zone.hvac_action"
	zoneFieldAllowedModes                  semanticFieldKey = "zone.allowed_modes"
	zoneFieldCurrentTempC                  semanticFieldKey = "zone.current_temp_c"
	zoneFieldTargetTempC                   semanticFieldKey = "zone.target_temp_c"
	zoneFieldCurrentHumidityPct            semanticFieldKey = "zone.current_humidity_pct"
	zoneFieldSpecialFunctionRaw            semanticFieldKey = "zone.special_function_raw"
	zoneFieldZoneOperationModeRaw          semanticFieldKey = "zone.operation_mode_raw"
	zoneFieldRoomTemperatureZoneMappingRaw semanticFieldKey = "zone.room_temperature_zone_mapping_raw"
	zoneFieldZoneCircuitIndexRaw           semanticFieldKey = "zone.circuit_index_raw"
	zoneFieldCircuitTypeRaw                semanticFieldKey = "zone.circuit_type_raw"
	zoneFieldZoneValveStatusRaw            semanticFieldKey = "zone.valve_status_raw"
	zoneFieldQuickVetoTempC                semanticFieldKey = "zone.quick_veto_temp_c"
	zoneFieldQuickVetoDurationH            semanticFieldKey = "zone.quick_veto_duration_h"
	zoneFieldQuickVetoEndTime              semanticFieldKey = "zone.quick_veto_end_time"
	zoneFieldQuickVetoEndDate              semanticFieldKey = "zone.quick_veto_end_date"
	zoneFieldHolidayStartDate              semanticFieldKey = "zone.holiday_start_date"
	zoneFieldHolidayEndDate                semanticFieldKey = "zone.holiday_end_date"
	zoneFieldHolidaySetpointC              semanticFieldKey = "zone.holiday_setpoint_c"
	zoneFieldHolidayStartTime              semanticFieldKey = "zone.holiday_start_time"
	zoneFieldHolidayEndTime                semanticFieldKey = "zone.holiday_end_time"
	dhwFieldOperatingMode                  semanticFieldKey = "dhw.operating_mode"
	dhwFieldPreset                         semanticFieldKey = "dhw.preset"
	dhwFieldCurrentTempC                   semanticFieldKey = "dhw.current_temp_c"
	dhwFieldTargetTempC                    semanticFieldKey = "dhw.target_temp_c"
	dhwFieldSpecialFunctionRaw             semanticFieldKey = "dhw.special_function_raw"
	dhwFieldDhwOperationModeRaw            semanticFieldKey = "dhw.operation_mode_raw"
	dhwFieldHolidayStartDate               semanticFieldKey = "dhw.holiday_start_date"
	dhwFieldHolidayEndDate                 semanticFieldKey = "dhw.holiday_end_date"
)

type semanticFieldFreshness struct {
	Source semanticSnapshotSource
	Stale  bool
}

type semanticFieldSet map[semanticFieldKey]struct{}

func newSemanticFieldSet(keys ...semanticFieldKey) semanticFieldSet {
	set := make(semanticFieldSet, len(keys))
	for _, key := range keys {
		set[key] = struct{}{}
	}
	return set
}

func (set semanticFieldSet) has(key semanticFieldKey) bool {
	if len(set) == 0 {
		return false
	}
	_, ok := set[key]
	return ok
}

type boilerStatusTier uint8

const (
	boilerStatusTierFast boilerStatusTier = iota
	boilerStatusTierMedium
	boilerStatusTierSlow
)

type boilerStatusTierSchedule struct {
	tier     boilerStatusTier
	interval time.Duration
	priority semanticTaskPriority
}

const (
	semanticTaskRefreshRegulatorCapability semanticTaskKey = "refresh_regulator_capability"
	semanticTaskRefreshDiscovery           semanticTaskKey = "refresh_discovery"
	semanticTaskRefreshConfig              semanticTaskKey = "refresh_config"
	semanticTaskRefreshState               semanticTaskKey = "refresh_state"
	semanticTaskRefreshCircuits            semanticTaskKey = "refresh_circuits"
	semanticTaskRefreshSystem              semanticTaskKey = "refresh_system"
	semanticTaskRefreshRadioDevices        semanticTaskKey = "refresh_radio_devices"
	semanticTaskRefreshEnergy              semanticTaskKey = "refresh_energy"
	semanticTaskRefreshSchedules           semanticTaskKey = "refresh_schedules"
	semanticTaskRefreshBoilerFast          semanticTaskKey = "refresh_boiler_fast"
	semanticTaskRefreshBoilerMedium        semanticTaskKey = "refresh_boiler_medium"
	semanticTaskRefreshBoilerSlow          semanticTaskKey = "refresh_boiler_slow"
)

var (
	zoneConfigRefreshFieldSet = newSemanticFieldSet(
		zoneFieldName,
		zoneFieldOperatingMode,
		zoneFieldPreset,
		zoneFieldAllowedModes,
		zoneFieldTargetTempC,
		zoneFieldCurrentHumidityPct,
		zoneFieldZoneOperationModeRaw,
		zoneFieldRoomTemperatureZoneMappingRaw,
		zoneFieldZoneCircuitIndexRaw,
		zoneFieldCircuitTypeRaw,
		zoneFieldQuickVetoTempC,
		zoneFieldQuickVetoDurationH,
		zoneFieldQuickVetoEndTime,
		zoneFieldQuickVetoEndDate,
		zoneFieldHolidayStartDate,
		zoneFieldHolidayEndDate,
		zoneFieldHolidaySetpointC,
		zoneFieldHolidayStartTime,
		zoneFieldHolidayEndTime,
	)
	zoneFastStateFieldSet = newSemanticFieldSet(
		zoneFieldOperatingMode,
		zoneFieldPreset,
		zoneFieldHvacAction,
		zoneFieldAllowedModes,
		zoneFieldCurrentTempC,
		zoneFieldSpecialFunctionRaw,
		zoneFieldZoneValveStatusRaw,
	)
	zoneGrabFieldSet = newSemanticFieldSet(
		zoneFieldName,
		zoneFieldOperatingMode,
		zoneFieldPreset,
		zoneFieldHvacAction,
		zoneFieldAllowedModes,
		zoneFieldCurrentTempC,
		zoneFieldTargetTempC,
		zoneFieldCurrentHumidityPct,
		zoneFieldZoneOperationModeRaw,
		zoneFieldSpecialFunctionRaw,
		zoneFieldRoomTemperatureZoneMappingRaw,
		zoneFieldZoneCircuitIndexRaw,
		zoneFieldZoneValveStatusRaw,
		zoneFieldCircuitTypeRaw,
	)
	dhwFieldSet = newSemanticFieldSet(
		dhwFieldOperatingMode,
		dhwFieldPreset,
		dhwFieldCurrentTempC,
		dhwFieldTargetTempC,
		dhwFieldDhwOperationModeRaw,
		dhwFieldSpecialFunctionRaw,
		dhwFieldHolidayStartDate,
		dhwFieldHolidayEndDate,
	)
)

func newVaillantSemanticPoller(cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub, cache semanticCachePersister) *vaillantSemanticPoller {
	catalog, catalogErr := productids.LoadCatalog()
	observeFirstFlags := normalizeObserveFirstFeatureFlagsForPoller(cfg)
	shadow := ebusgateway.NewShadowCache(ebusgateway.ShadowCacheOptions{
		FeatureFlags: observeFirstFlags,
	})
	poller := &vaillantSemanticPoller{
		scheduler:       ebusgateway.NewSemanticReadScheduler(),
		tasks:           newSemanticTaskScheduler(),
		reg:             gateway.Registry,
		bus:             gateway.Bus,
		provider:        provider,
		hub:             hub,
		cache:           cache,
		shadow:          shadow,
		transportConfig: cfg.TransportConfig,
		watchObserver:   cfg.WatchObserver,
		watchEfficiency: cfg.WatchEfficiencyObserver,
		source:          cfg.ScanSource,
		companion:       cfg.StartupCompanionTarget,

		requestTimeout:           cfg.SemanticRequestTimeout,
		discoveryInterval:        cfg.SemanticDiscoveryInterval,
		configInterval:           cfg.SemanticConfigInterval,
		stateInterval:            cfg.SemanticStateInterval,
		energyInterval:           cfg.SemanticEnergyInterval,
		scheduleInterval:         10 * time.Minute,
		boilerFastInterval:       30 * time.Second,
		boilerMediumInterval:     5 * time.Minute,
		boilerSlowInterval:       10 * time.Minute,
		zoneMissThreshold:        cfg.SemanticZonePresenceMissThreshold,
		zoneHitThreshold:         cfg.SemanticZonePresenceHitThreshold,
		dhwStaleTTL:              cfg.SemanticDHWStaleTTL,
		deviceSlotRediscoveryTTL: 30 * time.Minute,
		fm5EvidenceTTL:           defaultFM5EvidenceTTL,
		circuitFullScanInterval:  semanticCircuitFullScanInterval,

		catalog:    catalog,
		catalogErr: catalogErr,

		regulatorRecheckInterval: cfg.SemanticRegulatorRecheckInterval,
		regulatorAbsenceGrace:    cfg.SemanticRegulatorAbsenceGrace,
		regAbsenceState:          regulatorPresent,
		fm5Mode:                  graphql.Fm5SemanticModeAbsent,

		zones:          make(map[byte]*vaillantZoneSnapshot),
		presence:       make(map[byte]*zonePresenceRecord),
		circuits:       make(map[byte]*vaillantCircuitSnapshot),
		radioDevices:   make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot),
		solarCylinders: make(map[byte]*vaillantCylinderSnapshot),
		nowFn:          time.Now,
	}
	if poller.zoneMissThreshold <= 0 {
		poller.zoneMissThreshold = ebusgateway.DefaultSemanticZonePresenceMissThreshold
	}
	if poller.zoneHitThreshold <= 0 {
		poller.zoneHitThreshold = ebusgateway.DefaultSemanticZonePresenceHitThreshold
	}
	if poller.dhwStaleTTL <= 0 {
		poller.dhwStaleTTL = ebusgateway.DefaultSemanticDHWStaleTTL
	}
	if poller.energyInterval <= 0 {
		poller.energyInterval = ebusgateway.DefaultSemanticEnergyInterval
	}
	if poller.regulatorRecheckInterval <= 0 {
		poller.regulatorRecheckInterval = ebusgateway.DefaultSemanticRegulatorRecheckInterval
	}
	if poller.regulatorAbsenceGrace <= 0 {
		poller.regulatorAbsenceGrace = ebusgateway.DefaultSemanticRegulatorAbsenceGrace
	}
	semanticZoneCount.Set(0)
	semanticRegulatorState.Set(string(poller.regAbsenceState))
	poller.scheduler.SetShadowCache(shadow)
	poller.scheduler.SetCircuitBreaker(ebusgateway.SemanticReadCircuitBreakerOptions{
		FailureBudget:      cfg.SemanticReadBreakerFailureBudget,
		OpenCooldown:       cfg.SemanticReadBreakerOpenCooldown,
		HalfOpenProbeLimit: cfg.SemanticReadBreakerHalfOpenProbeLimit,
		OnTransition:       poller.onSemanticReadBreakerTransition,
		OnSuppressed:       poller.onSemanticReadBreakerSuppressed,
	})
	poller.adapterInfo = newVaillantAdapterInfoState(gateway.Bus, gateway.Transport, provider)
	poller.routerPlanesRefreshFn = func() { _ = gateway.RefreshRouterPlanes() }
	return poller
}

func normalizeObserveFirstFeatureFlagsForPoller(cfg ebusgateway.Config) ebusgateway.ObserveFirstFeatureFlags {
	if cfg.ExternalWritePolicy == "" &&
		!cfg.ObserveFirstEnabled &&
		!cfg.PassiveStateDirectApply &&
		!cfg.PassiveConfigDirectApply {
		return ebusgateway.NormalizeObserveFirstFeatureFlagsFromView(cfg.ObserveFirstFlags)
	}
	return ebusgateway.NormalizeObserveFirstFeatureFlags(
		cfg.ObserveFirstEnabled,
		cfg.PassiveStateDirectApply,
		cfg.PassiveConfigDirectApply,
		cfg.ExternalWritePolicy,
	)
}

func (p *vaillantSemanticPoller) onSemanticReadBreakerTransition(event ebusgateway.SemanticReadCircuitBreakerTransition) {
	if p == nil {
		return
	}
	semanticReadBreakerTransitionsTotal.Add(fmt.Sprintf("%s->%s", event.From, event.To), 1)
	log.Printf(
		"semantic_read_breaker_transition key=%q from=%s to=%s consecutive_failures=%d",
		event.Key,
		event.From,
		event.To,
		event.ConsecutiveFailures,
	)
}

func (p *vaillantSemanticPoller) onSemanticReadBreakerSuppressed(event ebusgateway.SemanticReadCircuitBreakerSuppression) {
	if p == nil {
		return
	}
	semanticReadBreakerSuppressedTotal.Add(string(event.State), 1)
	log.Printf(
		"semantic_read_breaker_suppressed key=%q state=%s retry_after=%s suppressed_total=%d",
		event.Key,
		event.State,
		event.RetryAfter.Round(time.Millisecond),
		event.SuppressedTotal,
	)
}

func preloadSemanticCache(provider *graphql.LiveSemanticProvider, cacheStore *semanticCacheStore) (semanticCacheSnapshot, bool) {
	if provider == nil || cacheStore == nil {
		return semanticCacheSnapshot{}, false
	}
	snapshot, ok := cacheStore.Load()
	if !ok {
		return semanticCacheSnapshot{}, false
	}
	if len(snapshot.Zones) > 0 {
		provider.SetZonesFromCache(snapshot.Zones)
	}
	if snapshot.DHW != nil {
		provider.SetDHWFromCache(snapshot.DHW)
	}
	if snapshot.Boiler != nil {
		provider.SetBoilerStatusFromCache(snapshot.Boiler)
	}
	return snapshot, true
}

func (p *vaillantSemanticPoller) hydrateFromCache(snapshot semanticCacheSnapshot) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.zones == nil {
		p.zones = make(map[byte]*vaillantZoneSnapshot)
	}
	if p.presence == nil {
		p.presence = make(map[byte]*zonePresenceRecord)
	}
	for idx, zone := range snapshot.Zones {
		instance := zoneInstanceFromSemanticID(zone.ID, idx)
		p.zones[instance] = zoneSnapshotFromSemanticZone(instance, zone)
		p.presence[instance] = &zonePresenceRecord{
			State:     zonePresencePresent,
			HitStreak: p.zoneHitThresholdValue(),
		}
	}
	if snapshot.DHW != nil {
		p.dhw = dhwSnapshotFromSemanticStatus(snapshot.DHW)
		if !snapshot.PersistedAt.IsZero() {
			p.dhwLastUpdateAt = snapshot.PersistedAt.UTC()
		} else {
			p.markDHWUpdatedNowLocked()
		}
	}
	if snapshot.Boiler != nil {
		p.boiler = boilerSnapshotFromGraphQL(snapshot.Boiler)
	}
	semanticZoneCount.Set(int64(len(p.zones)))
}

func zoneInstanceFromSemanticID(id string, fallbackIndex int) byte {
	trimmed := strings.TrimSpace(strings.ToLower(id))
	if strings.HasPrefix(trimmed, "zone-") {
		ordinalRaw := strings.TrimPrefix(trimmed, "zone-")
		if ordinal, err := strconv.Atoi(ordinalRaw); err == nil && ordinal > 0 && ordinal <= 256 {
			return byte(ordinal - 1)
		}
	}
	if fallbackIndex < 0 {
		return 0
	}
	if fallbackIndex > 255 {
		return 255
	}
	return byte(fallbackIndex)
}

func zoneSnapshotFromSemanticZone(instance byte, zone graphql.Zone) *vaillantZoneSnapshot {
	snapshot := &vaillantZoneSnapshot{
		Instance:             instance,
		Present:              true,
		Name:                 zone.Name,
		OperatingMode:        zone.Config.OperatingMode,
		Preset:               zone.Config.Preset,
		HvacAction:           zone.State.HvacAction,
		AllowedModes:         append([]string(nil), zone.Config.AllowedModes...),
		CurrentTempC:         cloneFloat64Ptr(zone.State.CurrentTempC),
		TargetTempC:          cloneFloat64Ptr(zone.Config.TargetTempC),
		HumidityPct:          cloneFloat64Ptr(zone.State.CurrentHumidityPct),
		StateSpecialFunction: zone.State.SpecialFunction,
	}
	if zone.Config.AssociatedCircuit != nil {
		v := uint16(*zone.Config.AssociatedCircuit)
		snapshot.ConfigurationAssociatedCircuitRaw = &v
	}
	if zone.Config.RoomTemperatureZoneMapping != nil {
		v := uint16(*zone.Config.RoomTemperatureZoneMapping)
		snapshot.ConfigurationRoomTemperatureZoneMappingRaw = &v
	}
	if zone.Config.CircuitType != "" {
		v := encodeCircuitTypeRaw(zone.Config.CircuitType)
		snapshot.ConfigurationCircuitTypeRaw = &v
	}
	if zone.State.ValvePositionPct != nil {
		v := uint16(*zone.State.ValvePositionPct * 655.35)
		snapshot.StateValveStatusRaw = &v
	}
	snapshot.QuickVetoTempC = cloneFloat64Ptr(zone.Config.QuickVetoSetpointC)
	snapshot.QuickVetoDurationH = cloneFloat64Ptr(zone.Config.QuickVetoDurationH)
	snapshot.HolidayStartDate = zone.Config.HolidayStartDate
	snapshot.HolidayEndDate = zone.Config.HolidayEndDate
	snapshot.HolidaySetpointC = cloneFloat64Ptr(zone.Config.HolidaySetpointC)
	snapshot.HolidayStartTime = zone.Config.HolidayStartTime
	snapshot.HolidayEndTime = zone.Config.HolidayEndTime
	seedZoneFreshness(snapshot, semanticSnapshotSourceCache, true)
	return snapshot
}

func encodeCircuitTypeRaw(circuitType string) uint16 {
	switch circuitType {
	case "radiator":
		return 0
	case "underfloor":
		return 1
	case "mixed":
		return 2
	default:
		var n uint16
		if _, err := fmt.Sscanf(circuitType, "unknown_%d", &n); err == nil {
			return n
		}
		return 0
	}
}

func dhwSnapshotFromSemanticStatus(status *graphql.DhwStatus) *vaillantDhwSnapshot {
	if status == nil {
		return nil
	}
	snapshot := &vaillantDhwSnapshot{
		OperatingMode:        status.Config.OperatingMode,
		Preset:               status.Config.Preset,
		CurrentTempC:         cloneFloat64Ptr(status.State.CurrentTempC),
		TargetTempC:          cloneFloat64Ptr(status.Config.TargetTempC),
		StateSpecialFunction: status.State.SpecialFunction,
		HolidayStartDate:     status.Config.HolidayStartDate,
		HolidayEndDate:       status.Config.HolidayEndDate,
	}
	seedDhwFreshness(snapshot, semanticSnapshotSourceCache, true)
	return snapshot
}

func cloneFloat64Ptr(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneUint16Ptr(value *uint16) *uint16 {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneUint8Ptr(value *uint8) *uint8 {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneUint32Ptr(value *uint32) *uint32 {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func ensureFieldFreshnessMap(entry *map[semanticFieldKey]semanticFieldFreshness) map[semanticFieldKey]semanticFieldFreshness {
	if *entry == nil {
		*entry = make(map[semanticFieldKey]semanticFieldFreshness)
	}
	return *entry
}

func setFieldFreshness(fields map[semanticFieldKey]semanticFieldFreshness, field semanticFieldKey, source semanticSnapshotSource, stale bool) {
	if fields == nil {
		return
	}
	fields[field] = semanticFieldFreshness{
		Source: source,
		Stale:  stale,
	}
}

func markFieldStale(fields map[semanticFieldKey]semanticFieldFreshness, field semanticFieldKey) {
	if fields == nil {
		return
	}
	freshness, ok := fields[field]
	if !ok {
		return
	}
	freshness.Stale = true
	fields[field] = freshness
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

func seedZoneFreshness(snapshot *vaillantZoneSnapshot, source semanticSnapshotSource, stale bool) {
	if snapshot == nil {
		return
	}
	fields := ensureFieldFreshnessMap(&snapshot.FieldFreshness)
	if strings.TrimSpace(snapshot.Name) != "" {
		setFieldFreshness(fields, zoneFieldName, source, stale)
	}
	if strings.TrimSpace(snapshot.OperatingMode) != "" {
		setFieldFreshness(fields, zoneFieldOperatingMode, source, stale)
	}
	if strings.TrimSpace(snapshot.Preset) != "" {
		setFieldFreshness(fields, zoneFieldPreset, source, stale)
	}
	if strings.TrimSpace(snapshot.HvacAction) != "" {
		setFieldFreshness(fields, zoneFieldHvacAction, source, stale)
	}
	if len(snapshot.AllowedModes) > 0 {
		setFieldFreshness(fields, zoneFieldAllowedModes, source, stale)
	}
	if snapshot.CurrentTempC != nil {
		setFieldFreshness(fields, zoneFieldCurrentTempC, source, stale)
	}
	if snapshot.TargetTempC != nil {
		setFieldFreshness(fields, zoneFieldTargetTempC, source, stale)
	}
	if snapshot.HumidityPct != nil {
		setFieldFreshness(fields, zoneFieldCurrentHumidityPct, source, stale)
	}
	if strings.TrimSpace(snapshot.ConfigurationHeatingOperationMode) != "" {
		setFieldFreshness(fields, zoneFieldZoneOperationModeRaw, source, stale)
	}
	if strings.TrimSpace(snapshot.StateSpecialFunction) != "" {
		setFieldFreshness(fields, zoneFieldSpecialFunctionRaw, source, stale)
	}
	if snapshot.ConfigurationRoomTemperatureZoneMappingRaw != nil {
		setFieldFreshness(fields, zoneFieldRoomTemperatureZoneMappingRaw, source, stale)
	}
	if snapshot.ConfigurationAssociatedCircuitRaw != nil {
		setFieldFreshness(fields, zoneFieldZoneCircuitIndexRaw, source, stale)
	}
	if snapshot.ConfigurationCircuitTypeRaw != nil {
		setFieldFreshness(fields, zoneFieldCircuitTypeRaw, source, stale)
	}
	if snapshot.StateValveStatusRaw != nil {
		setFieldFreshness(fields, zoneFieldZoneValveStatusRaw, source, stale)
	}
	if snapshot.QuickVetoTempC != nil {
		setFieldFreshness(fields, zoneFieldQuickVetoTempC, source, stale)
	}
	if snapshot.QuickVetoDurationH != nil {
		setFieldFreshness(fields, zoneFieldQuickVetoDurationH, source, stale)
	}
	if snapshot.QuickVetoEndTime != "" {
		setFieldFreshness(fields, zoneFieldQuickVetoEndTime, source, stale)
	}
	if snapshot.QuickVetoEndDate != "" {
		setFieldFreshness(fields, zoneFieldQuickVetoEndDate, source, stale)
	}
	if snapshot.HolidayStartDate != "" {
		setFieldFreshness(fields, zoneFieldHolidayStartDate, source, stale)
	}
	if snapshot.HolidayEndDate != "" {
		setFieldFreshness(fields, zoneFieldHolidayEndDate, source, stale)
	}
	if snapshot.HolidaySetpointC != nil {
		setFieldFreshness(fields, zoneFieldHolidaySetpointC, source, stale)
	}
	if snapshot.HolidayStartTime != "" {
		setFieldFreshness(fields, zoneFieldHolidayStartTime, source, stale)
	}
	if snapshot.HolidayEndTime != "" {
		setFieldFreshness(fields, zoneFieldHolidayEndTime, source, stale)
	}
}

func seedDhwFreshness(snapshot *vaillantDhwSnapshot, source semanticSnapshotSource, stale bool) {
	if snapshot == nil {
		return
	}
	fields := ensureFieldFreshnessMap(&snapshot.FieldFreshness)
	if strings.TrimSpace(snapshot.OperatingMode) != "" {
		setFieldFreshness(fields, dhwFieldOperatingMode, source, stale)
	}
	if strings.TrimSpace(snapshot.Preset) != "" {
		setFieldFreshness(fields, dhwFieldPreset, source, stale)
	}
	if snapshot.CurrentTempC != nil {
		setFieldFreshness(fields, dhwFieldCurrentTempC, source, stale)
	}
	if snapshot.TargetTempC != nil {
		setFieldFreshness(fields, dhwFieldTargetTempC, source, stale)
	}
	if strings.TrimSpace(snapshot.ConfigurationDHWOperationMode) != "" {
		setFieldFreshness(fields, dhwFieldDhwOperationModeRaw, source, stale)
	}
	if strings.TrimSpace(snapshot.StateSpecialFunction) != "" {
		setFieldFreshness(fields, dhwFieldSpecialFunctionRaw, source, stale)
	}
	if snapshot.HolidayStartDate != "" {
		setFieldFreshness(fields, dhwFieldHolidayStartDate, source, stale)
	}
	if snapshot.HolidayEndDate != "" {
		setFieldFreshness(fields, dhwFieldHolidayEndDate, source, stale)
	}
}

func (p *vaillantSemanticPoller) boilerStatusTierSchedules() []boilerStatusTierSchedule {
	if p == nil {
		return nil
	}
	return []boilerStatusTierSchedule{
		{tier: boilerStatusTierFast, interval: p.boilerFastInterval, priority: semanticTaskPriorityHigh},
		{tier: boilerStatusTierMedium, interval: p.boilerMediumInterval, priority: semanticTaskPriorityMedium},
		{tier: boilerStatusTierSlow, interval: p.boilerSlowInterval, priority: semanticTaskPriorityLow},
	}
}

func (p *vaillantSemanticPoller) boilerStatusTierTask(tier boilerStatusTier) func(context.Context) {
	return func(ctx context.Context) {
		if tier == boilerStatusTierFast {
			p.refreshBoilerStatus(ctx)
			return
		}
		p.refreshBoilerStatusTier(ctx, tier)
	}
}

func boilerStatusTierTaskKey(tier boilerStatusTier) semanticTaskKey {
	switch tier {
	case boilerStatusTierFast:
		return semanticTaskRefreshBoilerFast
	case boilerStatusTierMedium:
		return semanticTaskRefreshBoilerMedium
	case boilerStatusTierSlow:
		return semanticTaskRefreshBoilerSlow
	default:
		return semanticTaskKey("")
	}
}

func (p *vaillantSemanticPoller) enqueueBoilerStatusPriming(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if p.tasks == nil {
		for _, schedule := range p.boilerStatusTierSchedules() {
			p.boilerStatusTierTask(schedule.tier)(ctx)
		}
		return
	}
	for _, schedule := range p.boilerStatusTierSchedules() {
		p.enqueueTask(boilerStatusTierTaskKey(schedule.tier), schedule.priority, p.boilerStatusTierTask(schedule.tier))
	}
}

func (p *vaillantSemanticPoller) enqueueControllerSemanticPriming(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if p.tasks == nil {
		p.refreshConfig(ctx)
		p.refreshCircuits(ctx)
		p.refreshSystem(ctx)
		p.refreshRadioDevices(ctx)
		p.refreshEnergy(ctx)
		return
	}
	p.enqueueTask(semanticTaskRefreshConfig, semanticTaskPriorityHigh, p.refreshConfig)
	p.enqueueTask(semanticTaskRefreshCircuits, semanticTaskPriorityMedium, p.refreshCircuits)
	p.enqueueTask(semanticTaskRefreshSystem, semanticTaskPriorityMedium, p.refreshSystem)
	p.enqueueTask(semanticTaskRefreshRadioDevices, semanticTaskPriorityMedium, p.refreshRadioDevices)
	p.enqueueTask(semanticTaskRefreshEnergy, semanticTaskPriorityMedium, p.refreshEnergy)
}

func (p *vaillantSemanticPoller) refreshStartupSemanticPlanes(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		p.refreshDiscovery(ctx)
		p.mu.Lock()
		controller = p.controller
		p.mu.Unlock()
	}
	if controller == 0 {
		return
	}
	if p.provider == nil {
		return
	}

	p.publishStartupSchedules()
	p.publishStartupPlaneDefaults()
	primingCtx := ctx
	cancel := func() {}
	if semanticStartupPrimingBudget > 0 {
		primingCtx, cancel = context.WithTimeout(ctx, semanticStartupPrimingBudget)
	}
	defer cancel()

	attempts := 0
	for {
		attempts++
		status := p.startupL1PrimingStatus()
		if !status.dhw {
			p.refreshDHWStartupUntilReady(primingCtx, 1)
			status = p.startupL1PrimingStatus()
		}
		if !status.zones {
			p.refreshZoneDiscovery(primingCtx, true)
			status = p.startupL1PrimingStatus()
		}
		if !status.dhw {
			p.refreshDHWStartupUntilReady(primingCtx, 1)
		}
		if !status.system {
			p.refreshSystemStartup(primingCtx)
		}
		if !status.boilerStatus {
			p.refreshBoilerStatusStartup(primingCtx)
		}
		status = p.startupL1PrimingStatus()
		if !status.circuits {
			p.refreshCircuitsStartup(primingCtx)
		}
		if !status.radioDevices {
			p.refreshRadioDevicesStartup(primingCtx)
		}
		status = p.startupL1PrimingStatus()
		if !status.fm5Satisfied && status.fm5Evidence && !status.fm5GateKnown {
			p.refreshSystemStartup(primingCtx)
			status = p.startupL1PrimingStatus()
		}
		if status.system && (!status.fm5Satisfied || !status.solar || !status.cylinders) {
			p.refreshFM5SemanticStartup(primingCtx)
		}
		status = p.startupL1PrimingStatus()
		if status.ready() {
			log.Printf("semantic_startup_l1_priming result=ready attempts=%d %s", attempts, status.String())
			return
		}
		if semanticStartupPrimingBudget <= 0 {
			log.Printf("semantic_startup_l1_priming result=incomplete attempts=%d %s", attempts, status.String())
			return
		}

		delay := semanticStartupPrimingRetryDelay
		if delay <= 0 {
			delay = 50 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-primingCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			log.Printf("semantic_startup_l1_priming result=deadline attempts=%d %s", attempts, status.String())
			return
		case <-timer.C:
		}
	}
}

func (p *vaillantSemanticPoller) publishStartupPlaneDefaults() {
	if p == nil || p.provider == nil {
		return
	}
	if p.provider.Circuits() == nil {
		p.provider.SetCircuits([]graphql.CircuitStatus{})
	}
	if p.provider.Solar() == nil {
		p.provider.SetSolar(&graphql.SolarStatus{})
	}
	if p.provider.Cylinders() == nil {
		p.provider.SetCylinders([]graphql.CylinderStatus{})
	}
}

func (p *vaillantSemanticPoller) refreshStartupCriticalSemanticPlanes(ctx context.Context) {
	if p == nil || p.provider == nil {
		return
	}

	p.publishStartupSchedules()
	status := p.startupL1PrimingStatus()
	if !status.dhw {
		p.refreshDHWStartupUntilReady(ctx, semanticStartupCriticalDHWAttempts)
	}
	status = p.startupL1PrimingStatus()
	if !status.system {
		p.refreshSystemStartup(ctx)
	}
	status = p.startupL1PrimingStatus()
	if !status.boilerStatus {
		p.refreshBoilerStatusStartup(ctx)
	}
}

func (p *vaillantSemanticPoller) refreshDHWStartupUntilReady(ctx context.Context, attempts int) bool {
	if p == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if attempts <= 0 {
		attempts = 1
	}
	for attempt := 0; attempt < attempts; attempt++ {
		if p.startupL1PrimingStatus().dhw {
			return true
		}
		p.refreshDHWStartup(ctx)
		if p.startupL1PrimingStatus().dhw {
			return true
		}
		if attempt == attempts-1 {
			break
		}
		delay := semanticStartupPrimingRetryDelay
		if delay <= 0 {
			delay = 50 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return false
		case <-timer.C:
		}
	}
	return p.startupL1PrimingStatus().dhw
}

type startupL1PrimingStatus struct {
	zones        bool
	dhw          bool
	circuits     bool
	system       bool
	radioDevices bool
	fm5GateKnown bool
	fm5Evidence  bool
	fm5Required  bool
	fm5Satisfied bool
	solar        bool
	cylinders    bool
	boilerStatus bool
}

func (status startupL1PrimingStatus) ready() bool {
	return status.zones &&
		status.dhw &&
		status.circuits &&
		status.system &&
		status.radioDevices &&
		status.fm5Satisfied &&
		status.boilerStatus
}

func (status startupL1PrimingStatus) String() string {
	return fmt.Sprintf(
		"zones=%t dhw=%t circuits=%t system=%t radio_devices=%t fm5_gate_known=%t fm5_evidence=%t fm5_required=%t fm5_satisfied=%t solar=%t cylinders=%t boiler_status=%t",
		status.zones,
		status.dhw,
		status.circuits,
		status.system,
		status.radioDevices,
		status.fm5GateKnown,
		status.fm5Evidence,
		status.fm5Required,
		status.fm5Satisfied,
		status.solar,
		status.cylinders,
		status.boilerStatus,
	)
}

func (p *vaillantSemanticPoller) startupL1PrimingStatus() startupL1PrimingStatus {
	if p == nil || p.provider == nil {
		return startupL1PrimingStatus{}
	}
	p.mu.Lock()
	moduleConfig := (*uint16)(nil)
	if p.system != nil {
		moduleConfig = cloneUint16Ptr(p.system.ModuleConfigurationVR71)
	}
	fm5GateKnown := p.system != nil && p.system.ModuleConfigurationVR71 != nil
	radioSnapshots := make([]*vaillantRadioDeviceSnapshot, 0, len(p.radioDevices))
	for _, snapshot := range p.radioDevices {
		if snapshot != nil {
			radioSnapshots = append(radioSnapshots, cloneRadioSnapshot(snapshot))
		}
	}
	radioProbed := p.startupRadioDevicesProbed
	fm5RegistryEvidenceIgnored := p.fm5RegistryEvidenceIgnored
	p.mu.Unlock()
	liveFM5Evidence := hasFM5EvidenceFromRadioSnapshots(radioSnapshots)
	fm5Evidence := liveFM5Evidence || (!fm5RegistryEvidenceIgnored && p.hasFM5RegistryEvidence())
	fm5Required := fm5Evidence && moduleConfig != nil && *moduleConfig <= 2
	zonesReady := p.provider.Zones() != nil
	solarReady := p.provider.Solar() != nil
	cylinders := p.provider.Cylinders()
	cylindersPublished := cylinders != nil
	interpretedCylindersReady := len(cylinders) > 0
	fm5Mode := p.provider.FM5SemanticMode()
	fm5NonInterpretedPublished := fm5Evidence &&
		fm5Mode == graphql.Fm5SemanticModeGPIOOnly &&
		solarReady &&
		cylindersPublished
	fm5Satisfied := !fm5Evidence ||
		fm5NonInterpretedPublished ||
		(fm5GateKnown && !fm5Required) ||
		(fm5Required && solarReady && interpretedCylindersReady)
	return startupL1PrimingStatus{
		zones:        zonesReady,
		dhw:          p.provider.DHW() != nil,
		circuits:     p.provider.Circuits() != nil,
		system:       p.provider.System() != nil,
		radioDevices: radioProbed || len(p.provider.RadioDevices()) > 0,
		fm5GateKnown: fm5GateKnown,
		fm5Evidence:  fm5Evidence,
		fm5Required:  fm5Required,
		fm5Satisfied: fm5Satisfied,
		solar:        solarReady,
		cylinders:    cylindersPublished,
		boilerStatus: p.provider.BoilerStatus() != nil,
	}
}

func (p *vaillantSemanticPoller) startupProbeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, semanticStartupProbeTimeout)
}

func (p *vaillantSemanticPoller) readB524Startup(ctx context.Context, opcode, group, instance byte, addr uint16) ([]byte, bool) {
	probeCtx, cancel := p.startupProbeContext(ctx)
	defer cancel()
	return p.readB524ValueLive(probeCtx, opcode, group, instance, addr)
}

func (p *vaillantSemanticPoller) readB524Uint16Startup(ctx context.Context, opcode, group, instance byte, addr uint16) (*uint16, bool) {
	raw, ok := p.readB524Startup(ctx, opcode, group, instance, addr)
	if !ok {
		return nil, false
	}
	value, ok := decodeB524Uint16(raw)
	if !ok {
		return nil, false
	}
	return &value, true
}

func (p *vaillantSemanticPoller) publishStartupSchedules() {
	if p == nil || p.provider == nil || p.provider.Schedules() != nil {
		return
	}
	p.provider.SetSchedules(&graphql.ScheduleStatus{Programs: []graphql.ScheduleProgram{}})
}

func (p *vaillantSemanticPoller) refreshDHWStartup(ctx context.Context) {
	if p == nil {
		return
	}

	attempted := make(semanticFieldSet)
	status := &vaillantDhwSnapshot{}
	readAny := false
	if raw, ok := p.readB524Startup(ctx, localDHW.opcode, localDHW.group, dhwInstance, dhw_current_temp); ok {
		if value := decodeB524Float32FromRaw(raw); value != nil {
			status.CurrentTempC = value
			attempted[dhwFieldCurrentTempC] = struct{}{}
			readAny = true
		}
	}
	opModeRaw, opModeOK := p.readB524Uint16Startup(ctx, localDHW.opcode, localDHW.group, dhwInstance, dhw_operation_mode)
	if opModeOK {
		attempted[dhwFieldOperatingMode] = struct{}{}
		attempted[dhwFieldPreset] = struct{}{}
		attempted[dhwFieldDhwOperationModeRaw] = struct{}{}
		readAny = true
	}
	if opModeOK {
		status.OperatingMode, status.Preset = deriveDhwModeAndPreset(opModeRaw, nil)
		status.ConfigurationDHWOperationMode = formatUintToken(*opModeRaw)
	}

	p.mu.Lock()
	source := semanticSnapshotSourceCache
	if readAny {
		if p.dhw == nil {
			p.dhw = &vaillantDhwSnapshot{}
		}
		mergeDhwSnapshotFields(p.dhw, status, semanticSnapshotSourceLive, attempted)
		p.markDHWUpdatedNowLocked()
		source = semanticSnapshotSourceLive
	}
	hasSnapshot := p.dhw != nil
	p.mu.Unlock()
	if hasSnapshot {
		p.publishDHW(source)
	}
}

func (p *vaillantSemanticPoller) refreshCircuitsStartup(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	instances := allCircuitRefreshInstances()
	updates := make(map[byte]*vaillantCircuitSnapshot)
	inactive := make(map[byte]struct{})
	for _, instance := range instances {
		circuitTypeRaw, ok := p.readB524Uint16Startup(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_type)
		if !ok || circuitTypeRaw == nil {
			continue
		}
		switch *circuitTypeRaw {
		case 0x0000, 0x00FF, 0xFFFF:
			if snapshot, known := p.readDhwPseudoCircuitStartupEvidence(ctx, instance); snapshot != nil {
				snapshot.Controller = controller
				updates[instance] = snapshot
				continue
			} else if !known && instance == dhwPseudoCircuitInstance {
				continue
			}
			inactive[instance] = struct{}{}
			continue
		default:
			updates[instance] = &vaillantCircuitSnapshot{
				Instance:       instance,
				Active:         true,
				Controller:     controller,
				CircuitTypeRaw: cloneUint16Ptr(circuitTypeRaw),
			}
		}
	}
	if len(updates) == 0 && len(inactive) == 0 {
		p.mu.Lock()
		hasExisting := len(p.circuits) > 0
		p.mu.Unlock()
		if hasExisting {
			p.publishCircuits(semanticSnapshotSourceCache)
		}
		return
	}

	p.mu.Lock()
	if p.circuits == nil {
		p.circuits = make(map[byte]*vaillantCircuitSnapshot)
	}
	for instance := range inactive {
		delete(p.circuits, instance)
	}
	for instance, incoming := range updates {
		if incoming.CircuitStateRaw != nil {
			incoming.CircuitStateLiveAt = p.now()
		}
		if incoming.PumpStatusRaw != nil {
			incoming.PumpStatusLiveAt = p.now()
		}
		p.circuits[instance] = mergeCircuitSnapshotNonDestructive(p.circuits[instance], incoming)
	}
	p.lastCircuitFullScanAt = p.now()
	p.lastCircuitFullScanComplete = len(updates)+len(inactive) == len(instances)
	p.mu.Unlock()
	p.publishCircuits(semanticSnapshotSourceLive)
}

func (p *vaillantSemanticPoller) readDhwPseudoCircuitStartupEvidence(ctx context.Context, instance byte) (*vaillantCircuitSnapshot, bool) {
	if p == nil || instance != dhwPseudoCircuitInstance {
		return nil, false
	}
	snapshot := newDhwPseudoCircuitSnapshot(instance)
	flow, flowKnown := p.readDhwPseudoCircuitStartupTemperatureEvidence(ctx, instance, circuit_flow_temp)
	if isDhwPseudoCircuitTemperatureEvidence(flow) {
		snapshot.FlowTemperatureC = flow
	}
	calc, calcKnown := p.readDhwPseudoCircuitStartupTemperatureEvidence(ctx, instance, circuit_calc_flow_temp)
	if isDhwPseudoCircuitTemperatureEvidence(calc) {
		snapshot.CalcFlowTempC = calc
	}
	if snapshot.FlowTemperatureC == nil && snapshot.CalcFlowTempC == nil {
		return nil, flowKnown && calcKnown
	}
	return snapshot, true
}

func (p *vaillantSemanticPoller) readDhwPseudoCircuitStartupTemperatureEvidence(ctx context.Context, instance byte, addr uint16) (*float64, bool) {
	raw, ok := p.readB524Startup(ctx, localCircuits.opcode, localCircuits.group, instance, addr)
	if !ok {
		return nil, false
	}
	return decodeB524Float32FromRaw(raw), true
}

func (p *vaillantSemanticPoller) refreshSystemStartup(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	snapshot := &vaillantSystemSnapshot{Controller: controller}
	readAny := false
	if raw, ok := p.readB524Uint16Startup(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_scheme); ok && raw != nil {
		snapshot.SystemScheme = cloneUint16Ptr(raw)
		readAny = true
	}
	if raw, ok := p.readB524Uint16Startup(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_module_configuration_vr71); ok && raw != nil {
		snapshot.ModuleConfigurationVR71 = cloneUint16Ptr(raw)
		readAny = true
	}
	if raw, ok := p.readB524Startup(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_water_pressure); ok {
		if value := decodeB524Float32FromRaw(raw); value != nil {
			snapshot.SystemWaterPressure = value
			readAny = true
		}
	}

	p.mu.Lock()
	source := semanticSnapshotSourceCache
	if readAny {
		if snapshot.SystemFlowTemperature != nil {
			snapshot.SystemFlowTemperatureLiveAt = p.now()
		}
		p.updateSystemSnapshotLocked(mergeSystemSnapshotNonDestructive(p.system, snapshot))
		source = semanticSnapshotSourceLive
	}
	hasSnapshot := p.system != nil
	p.mu.Unlock()
	if hasSnapshot {
		p.publishSystem(source)
	}
}

func (p *vaillantSemanticPoller) refreshRadioDevicesStartup(ctx context.Context) {
	if p == nil {
		return
	}

	discovered := p.registryRadioDeviceSeeds()
	fullScanGroups := startupRadioFullScanGroups(discovered)
	fm5NamespaceComplete := fullScanGroups[remoteFunctionalModules.group]
	verified := make(map[radioDeviceKey]bool)
	readAny := false
	probeSlot := func(grp b524GroupDef, instance byte) {
		connectedRaw, ok := p.readB524Startup(ctx, grp.opcode, grp.group, instance, device_slot_connected)
		if !ok || len(connectedRaw) == 0 {
			if grp.group == remoteFunctionalModules.group {
				fm5NamespaceComplete = false
			}
			return
		}
		readAny = true
		connected := connectedRaw[0] == 1
		var classAddress *uint8
		var firmware *string
		var hardware *uint16
		if grp.group == remoteFunctionalModules.group {
			classRaw, classOK := p.readB524Startup(ctx, grp.opcode, grp.group, instance, device_slot_class_address)
			firmwareRaw, firmwareOK := p.readB524Startup(ctx, grp.opcode, grp.group, instance, device_slot_firmware)
			hardwareRaw, hardwareOK := p.readB524Startup(ctx, grp.opcode, grp.group, instance, device_slot_hardware_identifier)
			tuple := decodeFunctionalModuleIdentityTuple(
				connectedRaw, true,
				classRaw, classOK,
				firmwareRaw, firmwareOK,
				hardwareRaw, hardwareOK,
			)
			if !tuple.complete {
				fm5NamespaceComplete = false
			}
			connected = tuple.connected
			classAddress = tuple.classAddress
			firmware = tuple.firmware
			hardware = tuple.hardware
		} else {
			classAddress = p.readB524U8Startup(ctx, grp.opcode, grp.group, instance, device_slot_class_address)
		}
		key := radioDeviceKey{Group: grp.group, Instance: instance}
		verified[key] = true
		include, slotMode := startupRadioDeviceInclude(grp.group, connected, classAddress)
		if grp.group == remoteFunctionalModules.group && !include && hasRemoteIdentityEvidence(classAddress, firmware, hardware) {
			include = true
			slotMode = "inventory"
		}
		if !include {
			delete(discovered, key)
			return
		}
		discovered[key] = &vaillantRadioDeviceSnapshot{
			Group:              grp.group,
			Instance:           instance,
			SlotMode:           slotMode,
			DeviceConnected:    &connected,
			DeviceClassAddress: cloneUint8Ptr(classAddress),
			DeviceModel:        decodeRadioDeviceModel(classAddress),
			FirmwareVersion:    cloneStringPtr(firmware),
			HardwareIdentifier: cloneUint16Ptr(hardware),
		}
	}

	for _, grp := range remoteDeviceGroups {
		for instance := byte(0x00); instance <= semanticStartupSlotFastMaxInstance; instance++ {
			probeSlot(grp, instance)
		}
	}
	for _, grp := range remoteDeviceGroups {
		if fullScanGroups[grp.group] {
			for instance := semanticStartupSlotFastMaxInstance + 1; instance <= semanticStartupSlotFullMaxInstance; instance++ {
				probeSlot(grp, instance)
			}
		}
	}
	if readAny {
		for key, snapshot := range discovered {
			if snapshot != nil && snapshot.SlotMode == "registry" && !verified[key] {
				delete(discovered, key)
			}
		}
	}

	liveFM5Evidence := hasFM5EvidenceFromRadioMap(discovered)
	p.mu.Lock()
	p.startupRadioDevicesProbed = readAny
	p.fm5IdentityScanComplete = fm5NamespaceComplete
	p.fm5IdentityIncoherent = !fm5NamespaceComplete
	if fm5NamespaceComplete {
		p.fm5RegistryEvidenceIgnored = !liveFM5Evidence
	} else if liveFM5Evidence {
		p.fm5RegistryEvidenceIgnored = false
	}
	if len(discovered) == 0 {
		if readAny {
			p.radioDevices = make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
			p.fm5EvidenceGeneration++
			if fm5NamespaceComplete {
				p.fm5IdentityObservedAt = p.now()
			}
		}
		p.mu.Unlock()
		if readAny {
			p.publishRadioDevices(semanticSnapshotSourceLive)
		}
		return
	}
	if p.radioDevices == nil || readAny {
		p.radioDevices = make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
	}
	for key, snapshot := range discovered {
		p.radioDevices[key] = snapshot
	}
	if readAny {
		p.fm5EvidenceGeneration++
		if fm5NamespaceComplete || liveFM5Evidence {
			p.fm5IdentityObservedAt = p.now()
		}
	}
	p.mu.Unlock()
	source := semanticSnapshotSourceCache
	if readAny {
		source = semanticSnapshotSourceLive
	}
	p.publishRadioDevices(source)
}

func startupRadioFullScanGroups(discovered map[radioDeviceKey]*vaillantRadioDeviceSnapshot) map[byte]bool {
	seeded := make(map[byte]bool)
	highSeeded := make(map[byte]bool)
	for key := range discovered {
		seeded[key.Group] = true
		if key.Instance > semanticStartupSlotFastMaxInstance {
			highSeeded[key.Group] = true
		}
	}

	out := make(map[byte]bool)
	for _, grp := range remoteDeviceGroups {
		if !seeded[grp.group] || highSeeded[grp.group] {
			out[grp.group] = true
		}
	}
	return out
}

func startupRadioDeviceInclude(group byte, connected bool, classAddress *uint8) (bool, string) {
	switch group {
	case remoteRegulators.group, remoteThermostats.group:
		return connected, "startup"
	case remoteFunctionalModules.group:
		if connected {
			return true, "startup"
		}
		if classAddress != nil && *classAddress == circuitManagingDeviceVR71Address {
			return true, "inventory"
		}
		return false, "startup"
	default:
		return connected, "startup"
	}
}

func (p *vaillantSemanticPoller) registryRadioDeviceSeeds() map[radioDeviceKey]*vaillantRadioDeviceSnapshot {
	out := make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
	if p == nil || p.reg == nil {
		return out
	}
	nextInstance := map[byte]byte{}
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		addr := ebusgateway.SnapshotTargetAddressForRouting(snap)
		deviceID := normalizeDeviceID(snap.DeviceID)
		var group byte
		switch {
		case strings.HasPrefix(deviceID, "BASV") || addr == 0x15:
			group = remoteRegulators.group
		case strings.HasPrefix(deviceID, "VR71") || strings.HasPrefix(deviceID, "FM5") || addr == circuitManagingDeviceVR71Address:
			group = remoteFunctionalModules.group
		case strings.HasPrefix(deviceID, "VR92"):
			group = remoteThermostats.group
		default:
			return true
		}
		instance := nextInstance[group]
		nextInstance[group] = instance + 1
		classAddress := uint8(addr)
		connected := true
		out[radioDeviceKey{Group: group, Instance: instance}] = &vaillantRadioDeviceSnapshot{
			Group:              group,
			Instance:           instance,
			SlotMode:           "registry",
			DeviceConnected:    &connected,
			DeviceClassAddress: &classAddress,
			DeviceModel:        decodeRadioDeviceModel(&classAddress),
		}
		return true
	})
	return out
}

func (p *vaillantSemanticPoller) readB524U8Startup(ctx context.Context, opcode, group, instance byte, addr uint16) *uint8 {
	raw, ok := p.readB524Startup(ctx, opcode, group, instance, addr)
	if !ok || len(raw) == 0 {
		return nil
	}
	value := raw[0]
	return &value
}

func (p *vaillantSemanticPoller) refreshFM5SemanticStartup(ctx context.Context) {
	if p == nil || p.provider == nil {
		return
	}

	evidence := p.captureFM5Evidence()
	fm5GateSatisfied := evidence.moduleConfig != nil && *evidence.moduleConfig <= 2
	observedAt := p.now()
	evidenceStale := evidence.staleAt(observedAt, p.fm5EvidenceTTL)
	var incomingSolar *vaillantSolarSnapshot
	incomingCylinders := make(map[byte]*vaillantCylinderSnapshot)
	solarReadable := false
	cylindersReadable := false
	if evidence.controller != 0 && fm5GateSatisfied && !evidenceStale && !evidence.identityIncoherent {
		incomingSolar, solarReadable = p.readSolarSnapshotStartup(ctx)
		incomingCylinders, cylindersReadable = p.readCylinderSnapshotsStartup(ctx)
	}
	currentEvidence := p.captureFM5Evidence()
	incoherent := evidence.identityIncoherent || currentEvidence.identityIncoherent || !evidence.sameGeneration(currentEvidence)
	evidenceRevision := p.nextFM5EvidenceRevision(evidence.generation, currentEvidence.generation)
	negativeIdentityObserved := evidence.hasNegativeObservation() && currentEvidence.hasNegativeObservation()
	freshNegativeIdentityObserved := evidence.hasFreshNegativeObservation(observedAt, p.fm5EvidenceTTL) &&
		currentEvidence.hasFreshNegativeObservation(observedAt, p.fm5EvidenceTTL)
	var verdict graphql.Fm5Interpretation
	if freshNegativeIdentityObserved && !incoherent && evidence.controller != 0 && evidence.moduleConfig != nil {
		verdict = graphql.Fm5Interpretation{
			Mode:             graphql.Fm5SemanticModeAbsent,
			EvidenceRevision: evidenceRevision,
		}
	} else {
		verdict = deriveFM5Interpretation(
			evidence.controller != 0,
			evidence.moduleConfig,
			solarReadable,
			cylindersReadable,
			evidence.hasEvidence() || currentEvidence.hasEvidence() || negativeIdentityObserved,
			evidenceStale,
			incoherent,
			evidenceRevision,
		)
	}
	verdict = p.commitFM5Acquisition(currentEvidence, verdict, incomingSolar, incomingCylinders)
	for _, info := range fm5InventoryRegistryInfos(evidence.systemSnapshot, verdict.Mode) {
		p.reg.Register(preserveExistingRegistryMetadata(p.reg, info))
	}
	p.publishFM5Semantic(semanticSnapshotSourceLive)
}

func (p *vaillantSemanticPoller) readSolarSnapshotStartup(ctx context.Context) (*vaillantSolarSnapshot, bool) {
	incoming := &vaillantSolarSnapshot{}
	readAny := false
	if raw, ok := p.readB524Startup(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_enabled); ok {
		incoming.SolarEnabled = decodeB524BoolFromRaw(raw)
		readAny = true
	}
	if raw, ok := p.readB524Startup(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_collector_temp); ok {
		incoming.CollectorTemperatureC = decodeB524Float32FromRaw(raw)
		readAny = true
	}
	if raw, ok := p.readB524Startup(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_pump_active); ok {
		incoming.PumpActive = decodeB524BoolFromRaw(raw)
		readAny = true
	}
	if !readAny {
		return nil, false
	}
	return incoming, true
}

func (p *vaillantSemanticPoller) readCylinderSnapshotsStartup(ctx context.Context) (map[byte]*vaillantCylinderSnapshot, bool) {
	out := make(map[byte]*vaillantCylinderSnapshot, 2)
	for instance := byte(0x00); instance <= 0x01; instance++ {
		raw, ok := p.readB524Startup(ctx, localCylinders.opcode, localCylinders.group, instance, cylinder_temperature)
		if !ok {
			continue
		}
		out[instance] = &vaillantCylinderSnapshot{
			Instance:     instance,
			TemperatureC: decodeB524Float32FromRaw(raw),
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

func (p *vaillantSemanticPoller) refreshBoilerStatusStartup(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	boilerAddress := p.boilerAddress
	p.mu.Unlock()
	if boilerAddress == 0 {
		boilerAddress = p.findBoilerAddress()
	}

	snapshot := &vaillantBoilerSnapshot{}
	updated := false
	if boilerAddress != 0 {
		probeCtx, cancel := p.startupProbeContext(ctx)
		if value := p.readB509DATA2c(probeCtx, boilerAddress, boiler_b509_flow_temperature); value != nil {
			snapshot.FlowTemperatureC = value
			updated = true
		}
		cancel()
	}
	if raw, ok := p.readB524Startup(ctx, localDHW.opcode, localDHW.group, dhwInstance, dhw_current_temp); ok {
		if value := decodeB524Float32FromRaw(raw); value != nil {
			snapshot.DhwTemperatureC = value
			updated = true
		}
	}

	p.mu.Lock()
	if boilerAddress != 0 {
		p.boilerAddress = boilerAddress
	}
	source := semanticSnapshotSourceCache
	if updated {
		p.boiler = mergeBoilerSnapshotNonDestructive(p.boiler, snapshot)
		source = semanticSnapshotSourceLive
	}
	hasSnapshot := p.boiler != nil
	p.mu.Unlock()
	if hasSnapshot {
		p.publishBoilerStatus(source)
	}
}

func (p *vaillantSemanticPoller) Start(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	go p.tasks.run(ctx)

	// When a startup barrier is set, defer all bus traffic until the startup
	// scan first pass completes.  This prevents semantic bootstrap (B5.24
	// discovery) from racing ahead of the active scan on proxy-single paths.
	if p.startupBarrier != nil {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-p.startupBarrier:
			}
			log.Printf("semantic poller: startup barrier cleared, starting polling loops")
			p.startPollingLoops(ctx)
		}()
		return
	}

	p.startPollingLoops(ctx)
}

func (p *vaillantSemanticPoller) AttachPassiveShadowProducer(ctx context.Context, deduplicator *ebusgateway.ActivePassiveDeduplicator) error {
	if p == nil || deduplicator == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	subscription, err := deduplicator.Subscribe(
		"semantic-vaillant-shadow",
		passiveShadowSubscriberPriority,
		passiveShadowSubscriberBuffer,
	)
	if err != nil {
		return err
	}

	go func(subscription *ebusgateway.AdjudicatedPassiveSubscription) {
		for {
			closedUnexpectedly := false
		consume:
			for {
				select {
				case <-ctx.Done():
					subscription.Close()
					return
				case event, ok := <-subscription.Events():
					if !ok {
						closedUnexpectedly = ctx.Err() == nil
						break consume
					}
					p.handleAdjudicatedPassiveEvent(event)
				}
			}
			subscription.Close()
			if !closedUnexpectedly {
				return
			}
			for {
				if !waitForPassiveShadowRetry(ctx) {
					return
				}
				next, err := deduplicator.Subscribe(
					"semantic-vaillant-shadow",
					passiveShadowSubscriberPriority,
					passiveShadowSubscriberBuffer,
				)
				if err != nil {
					continue
				}
				subscription = next
				break
			}
		}
	}(subscription)

	return nil
}

func waitForPassiveShadowRetry(ctx context.Context) bool {
	delay := passiveShadowRetryDelay
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *vaillantSemanticPoller) handleAdjudicatedPassiveEvent(event ebusgateway.AdjudicatedPassiveEvent) {
	if p == nil || p.shadow == nil {
		return
	}
	if event.Disposition != ebusgateway.DedupDispositionUnmatchedThirdParty ||
		event.ObservabilityOnly ||
		event.LocalParticipantInbound ||
		event.MatchedActiveDuplicate {
		return
	}

	familyPolicy := event.Fingerprint.FamilyPolicy
	if !passiveShadowLaneEnabled(p.shadow.FeatureFlags(), familyPolicy) {
		return
	}

	key, ok := clonePassiveAdjudicatedWatchKey(event)
	if !ok || key == nil {
		return
	}
	runtime := p.bootstrapPassiveSharedWatchKey(key)

	switch familyPolicy.RequestIntent {
	case ebusgateway.ObserveFirstRequestIntentWrite:
		if !shouldInvalidatePassiveExternalWrite(familyPolicy) {
			return
		}
		invalidatedAt := event.Fingerprint.ObservedAt
		if invalidatedAt.IsZero() {
			invalidatedAt = p.now()
		}
		p.shadow.Invalidate(ebusgateway.ShadowInvalidation{
			Key:           key,
			Reason:        ebusgateway.ShadowInvalidationReasonExternalWrite,
			Source:        ebusgateway.ShadowInvalidationSourcePassive,
			InvalidatedAt: invalidatedAt,
		})
		return
	case ebusgateway.ObserveFirstRequestIntentRead:
	default:
		return
	}
	if event.SuppressShadow {
		return
	}

	if event.Fingerprint.ResponseClass != ebusgateway.DedupResponseValueBearing {
		return
	}
	value, ok := parsePassiveShadowPayload(event, key)
	if !ok || len(value) == 0 {
		return
	}

	observedAt := event.Fingerprint.ObservedAt
	if observedAt.IsZero() {
		observedAt = p.now()
	}
	result := p.shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      value,
		ObservedAt: observedAt,
	})
	p.emitWatchDirectApplyEfficiency(runtime, observedAt, true, result.Accepted)
	if !result.Accepted {
		log.Printf("semantic_passive_shadow_write_rejected key=%q reason=%s", key.Canonical(), result.Reason)
		return
	}
}

func passiveShadowLaneEnabled(flags ebusgateway.ObserveFirstFeatureFlags, policy ebusgateway.ObserveFirstFamilyPolicy) bool {
	if !flags.ObserveFirstEnabled() {
		return false
	}

	switch policy.RequestIntent {
	case ebusgateway.ObserveFirstRequestIntentRead:
		switch policy.DirectApplyPolicy {
		case ebusgateway.ObserveFirstDirectApplyPolicyStateDefault:
			return flags.PassiveStateDirectApply()
		case ebusgateway.ObserveFirstDirectApplyPolicyConfigOptIn:
			return flags.PassiveConfigDirectApply()
		default:
			return false
		}
	case ebusgateway.ObserveFirstRequestIntentWrite:
		if !policy.UsesRuntimeExternalWritePolicy {
			return false
		}
		return policy.EffectiveExternalWritePolicy != ebusgateway.ObserveFirstExternalWritePolicyRecordOnly
	default:
		return false
	}
}

func shouldInvalidatePassiveExternalWrite(policy ebusgateway.ObserveFirstFamilyPolicy) bool {
	if !policy.UsesRuntimeExternalWritePolicy {
		return false
	}
	switch policy.EffectiveExternalWritePolicy {
	case ebusgateway.ObserveFirstExternalWritePolicyInvalidateOnly,
		ebusgateway.ObserveFirstExternalWritePolicyRecordAndInvalidate:
		return true
	default:
		return false
	}
}

func clonePassiveAdjudicatedWatchKey(event ebusgateway.AdjudicatedPassiveEvent) (ebusgateway.WatchKey, bool) {
	return cloneSemanticWatchKey(event.Fingerprint.SharedWatchKey)
}

func cloneSemanticWatchKey(key ebusgateway.WatchKey) (ebusgateway.WatchKey, bool) {
	switch typed := key.(type) {
	case ebusgateway.B509WatchKey:
		cloned := ebusgateway.NewB509WatchKey(typed.Target, typed.RegisterAddress)
		return cloned, true
	case *ebusgateway.B509WatchKey:
		if typed == nil {
			return nil, false
		}
		cloned := ebusgateway.NewB509WatchKey(typed.Target, typed.RegisterAddress)
		return cloned, true
	case ebusgateway.B524WatchKey:
		cloned := ebusgateway.NewB524WatchKey(typed.Target, typed.Opcode, typed.Group, typed.Instance, typed.RegisterAddress)
		return cloned, true
	case *ebusgateway.B524WatchKey:
		if typed == nil {
			return nil, false
		}
		cloned := ebusgateway.NewB524WatchKey(typed.Target, typed.Opcode, typed.Group, typed.Instance, typed.RegisterAddress)
		return cloned, true
	default:
		return nil, false
	}
}

func parsePassiveShadowPayload(event ebusgateway.AdjudicatedPassiveEvent, key ebusgateway.WatchKey) ([]byte, bool) {
	if !event.Event.HasResponse {
		return nil, false
	}
	switch typed := key.(type) {
	case ebusgateway.B509WatchKey:
		return parseB509ReadPayload(event.Event.Response.Data, typed.RegisterAddress)
	case *ebusgateway.B509WatchKey:
		if typed == nil {
			return nil, false
		}
		return parseB509ReadPayload(event.Event.Response.Data, typed.RegisterAddress)
	case ebusgateway.B524WatchKey:
		return parseB524ReadPayload(event.Event.Response.Data, typed.Opcode, typed.Group, typed.Instance, typed.RegisterAddress)
	case *ebusgateway.B524WatchKey:
		if typed == nil {
			return nil, false
		}
		return parseB524ReadPayload(event.Event.Response.Data, typed.Opcode, typed.Group, typed.Instance, typed.RegisterAddress)
	default:
		return nil, false
	}
}

func (p *vaillantSemanticPoller) startPollingLoops(ctx context.Context) {
	// Discovery owns downstream startup/controller/boiler priming and avoids duplicate startup bursts.
	p.enqueueTask(semanticTaskRefreshDiscovery, semanticTaskPriorityHigh, p.refreshDiscovery)
	p.enqueueTask(semanticTaskRefreshSchedules, semanticTaskPriorityLow, p.refreshSchedules)

	go p.runLoop(ctx, p.regulatorRecheckInterval, semanticTaskRefreshRegulatorCapability, semanticTaskPriorityLow, p.refreshRegulatorCapability)
	go p.runLoop(ctx, p.discoveryInterval, semanticTaskRefreshDiscovery, semanticTaskPriorityLow, p.refreshDiscovery)
	go p.runLoop(ctx, p.configInterval, semanticTaskRefreshConfig, semanticTaskPriorityMedium, p.refreshConfig)
	go p.runLoop(ctx, p.stateInterval, semanticTaskRefreshState, semanticTaskPriorityHigh, p.refreshState)
	go p.runLoop(ctx, p.configInterval, semanticTaskRefreshCircuits, semanticTaskPriorityLow, p.refreshCircuits)
	go p.runLoop(ctx, p.configInterval, semanticTaskRefreshSystem, semanticTaskPriorityLow, p.refreshSystem)
	go p.runLoop(ctx, p.configInterval, semanticTaskRefreshRadioDevices, semanticTaskPriorityLow, p.refreshRadioDevices)
	go p.runLoop(ctx, p.energyInterval, semanticTaskRefreshEnergy, semanticTaskPriorityMedium, p.refreshEnergy)
	go p.runLoop(ctx, p.scheduleInterval, semanticTaskRefreshSchedules, semanticTaskPriorityLow, p.refreshSchedules)
	go p.adapterInfo.run(ctx)
	for _, schedule := range p.boilerStatusTierSchedules() {
		go p.runLoop(ctx, schedule.interval, boilerStatusTierTaskKey(schedule.tier), schedule.priority, p.boilerStatusTierTask(schedule.tier))
	}
}

func (p *vaillantSemanticPoller) runLoop(ctx context.Context, interval time.Duration, key semanticTaskKey, priority semanticTaskPriority, fn func(context.Context)) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.enqueueTask(key, priority, fn)
		}
	}
}

func (p *vaillantSemanticPoller) enqueueTask(key semanticTaskKey, priority semanticTaskPriority, fn func(context.Context)) {
	if p == nil || fn == nil {
		return
	}
	scheduler := p.tasks
	if scheduler == nil {
		fn(context.Background())
		return
	}
	err := scheduler.submitCoalesced(key, priority, func(taskCtx context.Context) {
		p.withPollLock(taskCtx, fn)
	})
	if errors.Is(err, errSemanticTaskQueueOverloaded) {
		log.Printf("semantic poll scheduler overloaded: skipping task key=%s priority=%d", key, priority)
		return
	}
	if err != nil {
		log.Printf("semantic poll scheduler submit failed key=%s err=%v", key, err)
	}
}

func (p *vaillantSemanticPoller) withPollLock(ctx context.Context, fn func(context.Context)) {
	if p == nil || fn == nil {
		return
	}
	p.pollMu.Lock()
	defer p.pollMu.Unlock()
	fn(ctx)
}

func (p *vaillantSemanticPoller) refreshRegulatorCapability(_ context.Context) {
	if p == nil || p.reg == nil {
		return
	}

	regCap := p.findRegulatorCapability()
	deviceCount := countRegistryDevices(p.reg)
	now := p.now()

	p.mu.Lock()
	prevCap := p.regulatorCapability
	prevAbsence := p.regAbsenceState
	prevDeviceCount := p.registryDeviceCount
	p.regulatorCapability = regCap
	p.registryDeviceCount = deviceCount

	// Absence grace FSM transitions.
	switch {
	case regCap == productids.ControllerPresent:
		if p.regAbsenceState != regulatorPresent {
			p.regAbsenceState = regulatorPresent
			p.regAbsenceSince = time.Time{}
		}
	case prevAbsence == regulatorPresent:
		// Was present, now not present — enter grace window.
		p.regAbsenceState = regulatorAbsenceGrace
		p.regAbsenceSince = now
	case prevAbsence == regulatorAbsenceGrace:
		if !p.regAbsenceSince.IsZero() && now.Sub(p.regAbsenceSince) >= p.regulatorAbsenceGrace {
			p.regAbsenceState = regulatorAbsent
		}
	}
	newAbsence := p.regAbsenceState
	p.mu.Unlock()
	semanticRegulatorState.Set(string(newAbsence))

	// Log capability changes.
	if regCap != prevCap {
		log.Printf("semantic_regulator_capability capability=%s", regCap.String())
	}

	// Log absence state transitions.
	if newAbsence != prevAbsence {
		semanticRegulatorTransitionsTotal.Add(fmt.Sprintf("%s->%s", prevAbsence, newAbsence), 1)
		log.Printf("semantic_regulator_transition from=%s to=%s capability=%s", prevAbsence, newAbsence, regCap.String())
		log.Printf("semantic_regulator_absence state=%s capability=%s", newAbsence, regCap.String())
	}

	// If device count changed, enqueue an immediate full discovery refresh.
	if deviceCount != prevDeviceCount && prevDeviceCount > 0 {
		log.Printf("semantic_regulator_recheck inventory_change prev=%d curr=%d", prevDeviceCount, deviceCount)
		p.enqueueTask(semanticTaskRefreshDiscovery, semanticTaskPriorityLow, p.refreshDiscovery)
	}
}

func (p *vaillantSemanticPoller) refreshDiscovery(ctx context.Context) {
	// Regulator capability is always recomputed, even when no B524 root is found.
	regCap := p.findRegulatorCapability()
	boilerAddress := p.findBoilerAddress()

	controller, discoveryErr := p.discoverB524Root(ctx)
	if discoveryErr != nil {
		log.Printf("semantic_b524_root: %v", discoveryErr)
		p.mu.Lock()
		prev := p.regulatorCapability
		prevController := p.controller
		prevBoilerAddress := p.boilerAddress
		p.controller = 0
		p.boilerAddress = boilerAddress
		p.regulatorCapability = regCap
		p.zones = make(map[byte]*vaillantZoneSnapshot)
		p.presence = make(map[byte]*zonePresenceRecord)
		p.circuits = make(map[byte]*vaillantCircuitSnapshot)
		p.fm5EvidenceGeneration++
		p.startupSemanticPrimed = false
		semanticZoneCount.Set(0)
		p.mu.Unlock()
		if regCap != prev {
			log.Printf("semantic_regulator_capability capability=%s", regCap.String())
		}
		if prevController != 0 {
			log.Printf("semantic_controller_discovery address=0x00 source=no_coherent_responder")
		}
		if boilerAddress != prevBoilerAddress && boilerAddress != 0 && boilerAddress != 0x08 {
			log.Printf("semantic_boiler_discovery address=0x%02x source=nonstandard", boilerAddress)
		}
		if boilerAddress != prevBoilerAddress && boilerAddress != 0 {
			p.enqueueBoilerStatusPriming(ctx)
		}
		p.publishZones(semanticSnapshotSourceCache)
		p.publishDHW(semanticSnapshotSourceCache)
		p.publishCircuits(semanticSnapshotSourceCache)
		p.publishRadioDevices(semanticSnapshotSourceCache)
		p.refreshFM5Semantic(ctx)
		return
	}

	// When B524 root discovery converges via the structural-fallback
	// path, the controller address may not yet be in the registry —
	// the directed startup probe that would normally register it
	// either has not run for this address (e.g. the regulator missed
	// the startup window) or did not yield enrichment. Register a
	// minimal Vaillant entry so GraphQL devices and the router plane
	// reflect the regulator. Registry de-dupes on address, so a
	// no-op when the controller is already known.
	registered := p.registerStructuralControllerIfMissing(controller)

	if enrichment := p.enrichRegulatorIdentity(controller); enrichment != nil {
		log.Printf("semantic_controller_enrichment address=0x%02x family=%s device=%s",
			controller, enrichment.family, enrichment.deviceID)
	}

	// Recompute regulator capability after structural registration:
	// the registry just gained a new Vaillant entry, so the
	// capability lookup must run against the post-registration
	// inventory. Without this recompute, p.regulatorCapability
	// stays at the pre-registration value (typically ControllerNone
	// when the registry held only the boiler) until the next
	// regulatorRecheckInterval tick — leaving status surfaces
	// reporting "no regulator" while semantic discovery already
	// has one.
	if registered {
		regCap = p.findRegulatorCapability()
	}

	p.mu.Lock()
	prev := p.regulatorCapability
	prevController := p.controller
	prevBoilerAddress := p.boilerAddress
	p.controller = controller
	if controller != prevController {
		p.fm5EvidenceGeneration++
	}
	p.boilerAddress = boilerAddress
	p.regulatorCapability = regCap
	primeStartup := !p.startupSemanticPrimed
	if primeStartup {
		p.startupSemanticPrimed = true
	}
	p.mu.Unlock()

	if primeStartup {
		p.refreshStartupCriticalSemanticPlanes(ctx)
	}
	p.refreshZoneDiscovery(ctx, primeStartup)
	if primeStartup {
		p.refreshStartupSemanticPlanes(ctx)
	}
	if regCap != prev {
		log.Printf("semantic_regulator_capability capability=%s", regCap.String())
	}
	if controller != prevController && controller != 0 {
		log.Printf("semantic_controller_discovery address=0x%02x", controller)
		p.enqueueControllerSemanticPriming(ctx)
	}
	if boilerAddress != prevBoilerAddress && boilerAddress != 0 && boilerAddress != 0x08 {
		log.Printf("semantic_boiler_discovery address=0x%02x source=nonstandard", boilerAddress)
	}
	if boilerAddress != prevBoilerAddress && boilerAddress != 0 {
		p.enqueueBoilerStatusPriming(ctx)
	}
}

func (p *vaillantSemanticPoller) refreshZoneDiscovery(ctx context.Context, startup bool) {
	present := make(map[byte]bool, 4)
	checked := make(map[byte]bool, 9)
	for instance := byte(0x00); instance <= 0x08; instance++ { // II_MAX=0x08 per Vaillant regulator spec
		var indexBytes []byte
		var ok bool
		if startup {
			indexBytes, ok = p.readB524Startup(ctx, localZones.opcode, localZones.group, instance, zone_index)
		} else {
			indexBytes, ok = p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_index)
		}
		if !ok {
			continue
		}
		checked[instance] = true
		if len(indexBytes) < 1 || indexBytes[0] == 0xFF {
			continue
		}
		present[instance] = true
	}

	source := p.reconcileDiscoveryPresence(ctx, checked, present)
	p.publishZones(source)
}

func (p *vaillantSemanticPoller) reconcileDiscoveryPresence(ctx context.Context, checked, present map[byte]bool) semanticSnapshotSource {
	if len(present) == 0 {
		grabPresent, ok := p.tryRefreshFromEbusdGrabWithInstances(ctx)
		if ok {
			if checked == nil {
				checked = make(map[byte]bool, len(grabPresent))
			}
			if present == nil {
				present = make(map[byte]bool, len(grabPresent))
			}
			for instance := range grabPresent {
				checked[instance] = true
				present[instance] = true
			}
			p.applyZonePresenceMissesOnly(checked, present)
			return semanticSnapshotSourceLive
		}
	}
	p.applyZonePresenceProbes(checked, present)
	if len(present) > 0 && p.startupZonesNeedImmediatePresence() {
		p.applyZonePresenceImmediate(checked, present)
		return semanticSnapshotSourceLive
	}
	if len(present) == 0 {
		return semanticSnapshotSourceCache
	}
	return semanticSnapshotSourceLive
}

func (p *vaillantSemanticPoller) startupZonesNeedImmediatePresence() bool {
	if p == nil || p.provider == nil {
		return false
	}
	if len(p.provider.Zones()) > 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.zones) == 0
}

func (p *vaillantSemanticPoller) applyZonePresenceImmediate(checked, present map[byte]bool) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.zones == nil {
		p.zones = make(map[byte]*vaillantZoneSnapshot)
	}
	if p.presence == nil {
		p.presence = make(map[byte]*zonePresenceRecord)
	}

	hitThreshold := p.zoneHitThresholdValue()
	for instance := range checked {
		if !present[instance] {
			p.markZoneMissingLocked(instance)
			continue
		}
		record := p.ensureZonePresenceRecordLocked(instance)
		previousState := record.State
		record.State = zonePresencePresent
		record.HitStreak = hitThreshold
		record.MissStreak = 0
		p.ensureZoneEntryLocked(instance)
		p.recordZonePresenceTransitionLocked(instance, previousState, record.State, record.HitStreak, record.MissStreak)
	}
	semanticZoneCount.Set(int64(len(p.zones)))
}

func (p *vaillantSemanticPoller) applyZonePresenceProbes(checked, present map[byte]bool) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.zones == nil {
		p.zones = make(map[byte]*vaillantZoneSnapshot)
	}
	if p.presence == nil {
		p.presence = make(map[byte]*zonePresenceRecord)
	}

	for instance := range checked {
		if present[instance] {
			p.markZonePresentLocked(instance)
			continue
		}
		p.markZoneMissingLocked(instance)
	}
}

func (p *vaillantSemanticPoller) applyZonePresenceMissesOnly(checked, present map[byte]bool) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.zones == nil {
		p.zones = make(map[byte]*vaillantZoneSnapshot)
	}
	if p.presence == nil {
		p.presence = make(map[byte]*zonePresenceRecord)
	}

	for instance := range checked {
		if present[instance] {
			continue
		}
		p.markZoneMissingLocked(instance)
	}
}

func (p *vaillantSemanticPoller) markZonePresentLocked(instance byte) {
	record := p.ensureZonePresenceRecordLocked(instance)
	previousState := record.State
	record.MissStreak = 0
	hitThreshold := p.zoneHitThresholdValue()

	switch record.State {
	case zonePresencePresent, zonePresenceSuspectMissing:
		record.State = zonePresencePresent
		if record.HitStreak < hitThreshold {
			record.HitStreak = hitThreshold
		}
		p.ensureZoneEntryLocked(instance)
	default:
		record.HitStreak++
		if record.HitStreak >= hitThreshold {
			record.State = zonePresencePresent
			record.HitStreak = hitThreshold
			p.ensureZoneEntryLocked(instance)
		} else {
			record.State = zonePresenceSuspectResurrect
		}
	}
	p.recordZonePresenceTransitionLocked(instance, previousState, record.State, record.HitStreak, record.MissStreak)
	semanticZoneCount.Set(int64(len(p.zones)))
}

func (p *vaillantSemanticPoller) markZoneMissingLocked(instance byte) {
	record := p.ensureZonePresenceRecordLocked(instance)
	previousState := record.State
	record.HitStreak = 0
	missThreshold := p.zoneMissThresholdValue()

	switch record.State {
	case zonePresencePresent, zonePresenceSuspectMissing:
		record.MissStreak++
		if record.MissStreak >= missThreshold {
			record.MissStreak = missThreshold
			record.State = zonePresenceAbsent
			delete(p.zones, instance)
			p.recordZonePresenceTransitionLocked(instance, previousState, record.State, record.HitStreak, record.MissStreak)
			semanticZoneCount.Set(int64(len(p.zones)))
			return
		}
		record.State = zonePresenceSuspectMissing
	default:
		record.MissStreak = 0
		record.State = zonePresenceAbsent
		delete(p.zones, instance)
	}
	p.recordZonePresenceTransitionLocked(instance, previousState, record.State, record.HitStreak, record.MissStreak)
	semanticZoneCount.Set(int64(len(p.zones)))
}

func (p *vaillantSemanticPoller) ensureZonePresenceRecordLocked(instance byte) *zonePresenceRecord {
	if p.presence == nil {
		p.presence = make(map[byte]*zonePresenceRecord)
	}
	record := p.presence[instance]
	if record == nil {
		record = &zonePresenceRecord{State: zonePresenceAbsent}
		p.presence[instance] = record
	}
	return record
}

func (p *vaillantSemanticPoller) ensureZoneEntryLocked(instance byte) {
	entry := p.zones[instance]
	if entry == nil {
		entry = &vaillantZoneSnapshot{Instance: instance}
		p.zones[instance] = entry
	}
	entry.Present = true
}

func (p *vaillantSemanticPoller) zoneMissThresholdValue() int {
	if p == nil || p.zoneMissThreshold <= 0 {
		return ebusgateway.DefaultSemanticZonePresenceMissThreshold
	}
	return p.zoneMissThreshold
}

func (p *vaillantSemanticPoller) zoneHitThresholdValue() int {
	if p == nil || p.zoneHitThreshold <= 0 {
		return ebusgateway.DefaultSemanticZonePresenceHitThreshold
	}
	return p.zoneHitThreshold
}

func (p *vaillantSemanticPoller) now() time.Time {
	if p == nil || p.nowFn == nil {
		return time.Now()
	}
	return p.nowFn()
}

func (p *vaillantSemanticPoller) markDHWUpdatedNowLocked() {
	if p == nil {
		return
	}
	p.dhwLastUpdateAt = p.now()
	semanticDHWUpdatesTotal.Add(1)
	log.Printf("semantic_dhw_update last_update_utc=%s", p.dhwLastUpdateAt.UTC().Format(time.RFC3339))
}

func (p *vaillantSemanticPoller) tryRefreshFromEbusdGrab(ctx context.Context) bool {
	_, ok := p.tryRefreshFromEbusdGrabWithInstances(ctx)
	return ok
}

func (p *vaillantSemanticPoller) tryRefreshFromEbusdGrabWithInstances(ctx context.Context) (map[byte]bool, bool) {
	if p == nil {
		return nil, false
	}
	if p.transportConfig.Protocol != ebusgateway.TransportEbusdTCP {
		return nil, false
	}
	if p.refreshFromEbusdGrabFn != nil {
		return p.refreshFromEbusdGrabFn(ctx)
	}
	return p.refreshFromEbusdGrabWithInstances(ctx)
}

func (p *vaillantSemanticPoller) refreshConfig(ctx context.Context) {
	controller, zones := p.snapshotZones()
	grabHydrated := false
	if controller == 0 || len(zones) == 0 {
		p.refreshDiscovery(ctx)
		controller, zones = p.snapshotZones()
	}
	if controller == 0 || len(zones) == 0 {
		if p.tryRefreshFromEbusdGrab(ctx) {
			controller, zones = p.snapshotZones()
			grabHydrated = true
		}
	}
	if controller == 0 || len(zones) == 0 {
		if controller != 0 {
			p.publishDHW(p.refreshDHWSlowConfig(ctx))
		}
		return
	}

	zoneLiveReadSuccess := false
	for _, instance := range zones {
		primaryName, primaryOK := p.readB524ZoneNamePart(ctx, instance, zone_name)
		prefix, prefixOK := p.readB524ZoneNamePart(ctx, instance, zone_name_prefix)
		suffix, suffixOK := p.readB524ZoneNamePart(ctx, instance, zone_name_suffix)
		if primaryOK || prefixOK || suffixOK {
			zoneLiveReadSuccess = true
		}

		incoming, slowOK := p.refreshZoneSlowConfigFields(ctx, instance)
		if slowOK {
			zoneLiveReadSuccess = true
		}
		if incoming == nil {
			incoming = &vaillantZoneSnapshot{}
		}
		incoming.Name = composeZoneName(primaryName, prefix, suffix)

		p.mu.Lock()
		if entry := p.zones[instance]; entry != nil {
			mergeZoneSnapshotFields(entry, incoming, semanticSnapshotSourceLive, zoneConfigRefreshFieldSet)
		}
		p.mu.Unlock()
	}

	dhwSource := p.refreshDHWSlowConfig(ctx)
	if !zoneLiveReadSuccess && p.tryRefreshFromEbusdGrab(ctx) {
		grabHydrated = true
	}

	zoneSource := semanticSnapshotSourceCache
	if zoneLiveReadSuccess || grabHydrated {
		zoneSource = semanticSnapshotSourceLive
	}
	p.publishZones(zoneSource)
	p.publishDHW(dhwSource)
}

func (p *vaillantSemanticPoller) refreshZoneSlowConfigFields(ctx context.Context, instance byte) (*vaillantZoneSnapshot, bool) {
	incoming := &vaillantZoneSnapshot{}
	liveReadSuccess := false

	var targetPtr *float64
	if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_target_temp); ok {
		target := value
		targetPtr = &target
		liveReadSuccess = true
	}
	if targetPtr == nil {
		if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_fallback_manual_temp); ok {
			target := value
			targetPtr = &target
			liveReadSuccess = true
		}
	}

	var humidity *float64
	if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_current_humidity); ok {
		currentHumidity := value
		humidity = &currentHumidity
		liveReadSuccess = true
	}

	zoneOpMode, zoneOpModeOK := p.readB524Uint16(ctx, localZones.opcode, localZones.group, instance, zone_heating_op_mode)
	if zoneOpModeOK {
		liveReadSuccess = true
	}

	var qvTempPtr, qvDurPtr *float64
	if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_quick_veto_temp); ok {
		v := value
		qvTempPtr = &v
		liveReadSuccess = true
	}
	if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_quick_veto_duration); ok {
		v := value
		qvDurPtr = &v
		liveReadSuccess = true
	}
	var qvEndTime, qvEndDate string
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_quick_veto_end_time); ok && len(raw) >= 2 {
		qvEndTime = fmt.Sprintf("%02d:%02d", raw[0], raw[1])
		liveReadSuccess = true
	}
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_quick_veto_end_date); ok && len(raw) >= 3 {
		year := 2000 + int(raw[2])
		qvEndDate = fmt.Sprintf("%04d-%02d-%02d", year, raw[1], raw[0])
		liveReadSuccess = true
	}

	var holidayStartDate, holidayEndDate, holidayStartTime, holidayEndTime string
	var holidaySetpointPtr *float64
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_holiday_start_date); ok && len(raw) >= 3 {
		if date := decodeB524DateSuppressSentinel(raw); date != "" {
			holidayStartDate = date
		}
		liveReadSuccess = true
	}
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_holiday_end_date); ok && len(raw) >= 3 {
		if date := decodeB524DateSuppressSentinel(raw); date != "" {
			holidayEndDate = date
		}
		liveReadSuccess = true
	}
	if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_holiday_setpoint); ok {
		v := value
		holidaySetpointPtr = &v
		liveReadSuccess = true
	}
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_holiday_end_time); ok && len(raw) >= 2 {
		holidayEndTime = fmt.Sprintf("%02d:%02d", raw[0], raw[1])
		liveReadSuccess = true
	}
	if raw, ok := p.readB524Value(ctx, localZones.opcode, localZones.group, instance, zone_holiday_start_time); ok && len(raw) >= 2 {
		holidayStartTime = fmt.Sprintf("%02d:%02d", raw[0], raw[1])
		liveReadSuccess = true
	}

	zoneRoomTemperatureZoneMappingRaw, zoneRoomTemperatureZoneMappingRawOK := p.readB524Uint16(ctx, localZones.opcode, localZones.group, instance, zone_room_temperature_zone_mapping_raw)
	if zoneRoomTemperatureZoneMappingRawOK {
		liveReadSuccess = true
	}
	circuitInstance := resolveAssociatedCircuitInstance(zoneRoomTemperatureZoneMappingRaw, instance)
	circuitType, hasCircuitType := p.readB524Uint16(ctx, localCircuits.opcode, localCircuits.group, circuitInstance, circuit_type)
	if hasCircuitType {
		liveReadSuccess = true
	}
	var associatedCircuitRaw *uint16
	if zoneRoomTemperatureZoneMappingRaw != nil {
		value := uint16(circuitInstance)
		associatedCircuitRaw = &value
	}

	_, cachedZoneSF, _, _ := p.cachedZoneDerivationInputs(instance)
	if zoneOpMode != nil || cachedZoneSF != nil || hasCircuitType {
		operatingMode, preset, allowedModes := deriveZoneModeAndPreset(zoneOpMode, cachedZoneSF, circuitType, hasCircuitType)
		if zoneOpMode != nil {
			incoming.OperatingMode = operatingMode
		}
		if zoneOpMode != nil || cachedZoneSF != nil {
			incoming.Preset = preset
		}
		if hasCircuitType {
			incoming.AllowedModes = allowedModes
		}
	}
	if zoneOpMode != nil {
		incoming.ConfigurationHeatingOperationMode = formatUintToken(*zoneOpMode)
	}

	incoming.TargetTempC = targetPtr
	incoming.HumidityPct = humidity
	incoming.QuickVetoTempC = qvTempPtr
	incoming.QuickVetoDurationH = qvDurPtr
	incoming.QuickVetoEndTime = qvEndTime
	incoming.QuickVetoEndDate = qvEndDate
	incoming.HolidayStartDate = holidayStartDate
	incoming.HolidayEndDate = holidayEndDate
	incoming.HolidaySetpointC = holidaySetpointPtr
	incoming.HolidayStartTime = holidayStartTime
	incoming.HolidayEndTime = holidayEndTime
	incoming.ConfigurationRoomTemperatureZoneMappingRaw = zoneRoomTemperatureZoneMappingRaw
	incoming.ConfigurationAssociatedCircuitRaw = associatedCircuitRaw
	incoming.ConfigurationCircuitTypeRaw = circuitType

	return incoming, liveReadSuccess
}

func (p *vaillantSemanticPoller) refreshDHWSlowConfig(ctx context.Context) semanticSnapshotSource {
	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		return semanticSnapshotSourceCache
	}

	attempted := make(semanticFieldSet)
	liveReadSuccess := false
	targetPtr := p.readDhwFloat(ctx, dhw_target_temp)
	if targetPtr != nil {
		liveReadSuccess = true
		attempted[dhwFieldTargetTempC] = struct{}{}
	}
	opModeRaw, opModeOK := p.readDhwUint16(ctx, dhw_operation_mode)
	if opModeOK {
		liveReadSuccess = true
		attempted[dhwFieldOperatingMode] = struct{}{}
		attempted[dhwFieldPreset] = struct{}{}
		attempted[dhwFieldDhwOperationModeRaw] = struct{}{}
	}
	var dhwHolidayStartDate, dhwHolidayEndDate string
	if raw, ok := p.readB524Value(ctx, localDHW.opcode, localDHW.group, dhwInstance, dhw_holiday_start_date); ok && len(raw) >= 3 {
		if date := decodeB524DateSuppressSentinel(raw); date != "" {
			dhwHolidayStartDate = date
		}
		liveReadSuccess = true
		attempted[dhwFieldHolidayStartDate] = struct{}{}
	}
	if raw, ok := p.readB524Value(ctx, localDHW.opcode, localDHW.group, dhwInstance, dhw_holiday_end_date); ok && len(raw) >= 3 {
		if date := decodeB524DateSuppressSentinel(raw); date != "" {
			dhwHolidayEndDate = date
		}
		liveReadSuccess = true
		attempted[dhwFieldHolidayEndDate] = struct{}{}
	}
	if !liveReadSuccess {
		return semanticSnapshotSourceCache
	}

	status := &vaillantDhwSnapshot{
		HolidayStartDate: dhwHolidayStartDate,
		HolidayEndDate:   dhwHolidayEndDate,
		TargetTempC:      targetPtr,
	}
	if opModeRaw != nil {
		_, cachedSpecial := p.cachedDHWDerivationInputs()
		status.OperatingMode, status.Preset = deriveDhwModeAndPreset(opModeRaw, cachedSpecial)
		status.ConfigurationDHWOperationMode = formatUintToken(*opModeRaw)
	}
	p.mu.Lock()
	if p.dhw == nil {
		p.dhw = &vaillantDhwSnapshot{}
	}
	mergeDhwSnapshotFields(p.dhw, status, semanticSnapshotSourceLive, attempted)
	p.markDHWUpdatedNowLocked()
	p.mu.Unlock()
	return semanticSnapshotSourceLive
}

func (p *vaillantSemanticPoller) cachedZoneDerivationInputs(instance byte) (opMode, specialFunction, circuitType *uint16, hasCircuitType bool) {
	if p == nil {
		return nil, nil, nil, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	circuitInstance := instance
	if zone := p.zones[instance]; zone != nil {
		opMode, _ = parseUint16Token(zone.ConfigurationHeatingOperationMode)
		specialFunction, _ = parseUint16Token(zone.StateSpecialFunction)
		if zone.ConfigurationCircuitTypeRaw != nil {
			return opMode, specialFunction, cloneUint16Ptr(zone.ConfigurationCircuitTypeRaw), true
		}
		if zone.ConfigurationAssociatedCircuitRaw != nil && *zone.ConfigurationAssociatedCircuitRaw <= 0xFF {
			circuitInstance = byte(*zone.ConfigurationAssociatedCircuitRaw)
		}
	}
	if circuit := p.circuits[circuitInstance]; circuit != nil && circuit.CircuitTypeRaw != nil {
		return opMode, specialFunction, cloneUint16Ptr(circuit.CircuitTypeRaw), true
	}
	return opMode, specialFunction, nil, false
}

func (p *vaillantSemanticPoller) cachedDHWDerivationInputs() (opMode, specialFunction *uint16) {
	if p == nil {
		return nil, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.dhw == nil {
		return nil, nil
	}
	opMode, _ = parseUint16Token(p.dhw.ConfigurationDHWOperationMode)
	specialFunction, _ = parseUint16Token(p.dhw.StateSpecialFunction)
	return opMode, specialFunction
}

func parseUint16Token(token string) (*uint16, bool) {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return nil, false
	}
	value, err := strconv.ParseUint(trimmed, 10, 16)
	if err != nil {
		return nil, false
	}
	parsed := uint16(value)
	return &parsed, true
}

func (p *vaillantSemanticPoller) refreshState(ctx context.Context) {
	controller, zones := p.snapshotZones()
	grabHydrated := false
	if controller == 0 || len(zones) == 0 {
		p.refreshDiscovery(ctx)
		controller, zones = p.snapshotZones()
	}
	if controller == 0 || len(zones) == 0 {
		if p.tryRefreshFromEbusdGrab(ctx) {
			controller, zones = p.snapshotZones()
			grabHydrated = true
		}
	}
	if controller == 0 || len(zones) == 0 {
		dhwSource := p.refreshDHW(ctx)
		p.publishDHW(dhwSource)
		return
	}

	liveReadSuccess := false
	for _, instance := range zones {
		var currentPtr *float64
		if value, ok := p.readB524Float32LE(ctx, localZones.opcode, localZones.group, instance, zone_current_temp); ok {
			current := value
			currentPtr = &current
			liveReadSuccess = true
		}

		zoneSF, zoneSFOK := p.readB524Uint16(ctx, localZones.opcode, localZones.group, instance, zone_special_function)
		zoneValve, zoneValveOK := p.readB524Uint16(ctx, localZones.opcode, localZones.group, instance, zone_valve_status)
		if zoneSFOK || zoneValveOK {
			liveReadSuccess = true
		}
		cachedOpMode, cachedZoneSF, circuitType, hasCircuitType := p.cachedZoneDerivationInputs(instance)
		zoneSFForDerive := zoneSF
		if zoneSFForDerive == nil {
			zoneSFForDerive = cachedZoneSF
		}

		allowedModes := deriveZoneAllowedModes(circuitType, hasCircuitType)
		hvacAction := deriveZoneHvacAction(zoneValve, circuitType, hasCircuitType)
		incoming := &vaillantZoneSnapshot{
			HvacAction:          hvacAction,
			CurrentTempC:        currentPtr,
			StateValveStatusRaw: zoneValve,
		}
		if cachedOpMode != nil || hasMeaningfulSpecialFunction(zoneSFForDerive) {
			operatingMode, preset, _ := deriveZoneModeAndPreset(cachedOpMode, zoneSFForDerive, circuitType, hasCircuitType)
			if cachedOpMode != nil {
				incoming.OperatingMode = operatingMode
			}
			incoming.Preset = preset
		}
		if hasCircuitType {
			incoming.AllowedModes = allowedModes
		}
		if zoneSF != nil {
			incoming.StateSpecialFunction = formatUintToken(*zoneSF)
		}

		p.mu.Lock()
		if entry := p.zones[instance]; entry != nil {
			mergeZoneSnapshotFields(entry, incoming, semanticSnapshotSourceLive, zoneFastStateFieldSet)
		}
		p.mu.Unlock()
	}
	if !liveReadSuccess && p.tryRefreshFromEbusdGrab(ctx) {
		grabHydrated = true
	}

	zoneSource := semanticSnapshotSourceCache
	if liveReadSuccess || grabHydrated {
		zoneSource = semanticSnapshotSourceLive
	}

	dhwSource := p.refreshDHW(ctx)
	p.publishZones(zoneSource)
	p.publishDHW(dhwSource)
}

func (p *vaillantSemanticPoller) snapshotZones() (byte, []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	controller := p.controller
	if controller == 0 || len(p.zones) == 0 {
		return controller, nil
	}
	instances := make([]byte, 0, len(p.zones))
	for instance := range p.zones {
		instances = append(instances, instance)
	}
	slices.Sort(instances)
	return controller, instances
}

func (p *vaillantSemanticPoller) publishZones(source semanticSnapshotSource) {
	if p.provider == nil {
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

	previous := p.provider.Zones()
	if source == semanticSnapshotSourceCache && len(zones) == 0 && len(previous) > 0 {
		return
	}
	switch source {
	case semanticSnapshotSourceCache:
		p.provider.SetZonesFromCache(zones)
	default:
		p.provider.SetZones(zones)
	}

	if p.hub != nil {
		prevByID := make(map[string]graphql.Zone, len(previous))
		for _, z := range previous {
			prevByID[z.ID] = z
		}
		for _, zone := range zones {
			if !zoneEquals(prevByID[zone.ID], zone) {
				p.hub.PublishZoneUpdate(zone)
			}
		}
	}
	p.persistSemanticCache(source)
}

func (p *vaillantSemanticPoller) mappedRadioRoomStateForZoneLocked(entry *vaillantZoneSnapshot) (*float64, *float64) {
	if p == nil || entry == nil {
		return nil, nil
	}
	mappingRaw := entry.ConfigurationRoomTemperatureZoneMappingRaw
	if mappingRaw == nil {
		mappingRaw = p.inferRadioRoomMappingForZoneLocked(entry.Instance)
	}
	if mappingRaw == nil {
		return nil, nil
	}
	mapping := *mappingRaw
	if mapping == 0 || mapping == 0xFFFF {
		return nil, nil
	}
	if decodeRoomTemperatureZoneMapping(&mapping) == nil {
		return nil, nil
	}

	var snapshot *vaillantRadioDeviceSnapshot
	switch mapping {
	case 1:
		snapshot = p.firstConnectedRadioDeviceLocked(remoteRegulators.group)
	default:
		if mapping > 0x100 {
			return nil, nil
		}
		snapshot = p.radioDevices[radioDeviceKey{Group: remoteThermostats.group, Instance: byte(mapping - 1)}]
	}
	if snapshot == nil || !radioDeviceConnected(snapshot) {
		return nil, nil
	}
	return cloneFloat64Ptr(snapshot.RoomTemperatureC), cloneFloat64Ptr(snapshot.RoomHumidityPct)
}

func (p *vaillantSemanticPoller) inferRadioRoomMappingForZoneLocked(instance byte) *uint16 {
	if p == nil {
		return nil
	}
	zoneNumber := int(instance) + 1
	matches := make([]byte, 0, 1)
	for key, snapshot := range p.radioDevices {
		if key.Group != remoteThermostats.group || snapshot == nil || !radioDeviceConnected(snapshot) || snapshot.ZoneAssignment == nil {
			continue
		}
		if int(*snapshot.ZoneAssignment) != zoneNumber {
			continue
		}
		matches = append(matches, key.Instance)
	}
	if len(matches) != 1 {
		return nil
	}
	mapping := uint16(matches[0]) + 1
	return &mapping
}

func (p *vaillantSemanticPoller) firstConnectedRadioDeviceLocked(group byte) *vaillantRadioDeviceSnapshot {
	if p == nil {
		return nil
	}
	var selected *vaillantRadioDeviceSnapshot
	var selectedInstance byte
	for key, snapshot := range p.radioDevices {
		if key.Group != group || snapshot == nil || !radioDeviceConnected(snapshot) {
			continue
		}
		if selected == nil || key.Instance < selectedInstance {
			selected = snapshot
			selectedInstance = key.Instance
		}
	}
	return selected
}

func radioDeviceConnected(snapshot *vaillantRadioDeviceSnapshot) bool {
	return snapshot != nil && snapshot.DeviceConnected != nil && *snapshot.DeviceConnected
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

type b524EnergyQuery struct {
	addr     uint16 // B5.24 register address
	channel  string // EnergyMergeKey.Channel
	usage    string // EnergyMergeKey.Usage
	period   string // EnergyMergeKey.Period ("year", "month")
	yearKind string // EnergyMergeKey.YearKind ("current", "previous")
}

var b524EnergyQueries = []b524EnergyQuery{
	// All-time totals (mapped as year/current).
	{energy_fuel_sum_hc, "gas", "climate", "year", "current"},
	{energy_fuel_sum_hwc, "gas", "hot_water", "year", "current"},
	{energy_electricity_sum_hc, "electricity", "climate", "year", "current"},
	{energy_electricity_sum_hwc, "electricity", "hot_water", "year", "current"},
	// Monthly: this month (month/current).
	{energy_fuel_sum_hc_this_month, "gas", "climate", "month", "current"},
	{energy_fuel_sum_hwc_this_month, "gas", "hot_water", "month", "current"},
	{energy_electricity_sum_hc_this_month, "electricity", "climate", "month", "current"},
	{energy_electricity_sum_hwc_this_month, "electricity", "hot_water", "month", "current"},
	// Monthly: last month (month/previous).
	{energy_fuel_sum_hc_last_month, "gas", "climate", "month", "previous"},
	{energy_fuel_sum_hwc_last_month, "gas", "hot_water", "month", "previous"},
	{energy_electricity_sum_hc_last_month, "electricity", "climate", "month", "previous"},
	{energy_electricity_sum_hwc_last_month, "electricity", "hot_water", "month", "previous"},
}

func (p *vaillantSemanticPoller) refreshEnergy(ctx context.Context) {
	if p == nil || p.provider == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		p.refreshFM5Semantic(ctx)
		return
	}

	accepted, failed := 0, 0
	for _, q := range b524EnergyQueries {
		val, ok := p.readB524Uint32LE(ctx, vaillantB524OpcodeLocal, localRegulator.group, regulatorInstance, q.addr)
		if !ok {
			failed++
			continue
		}
		kwh := float64(val)

		if p.provider.ApplyEnergyFromRegister(graphql.EnergyMergeKey{
			Channel: q.channel, Usage: q.usage, Period: q.period, YearKind: q.yearKind,
		}, kwh) {
			accepted++
		}
		// For all-time totals, lock day and year/previous with register-priority 0
		// to prevent broadcast double-counting (all-time total already includes them).
		if q.period == "year" && q.yearKind == "current" {
			p.provider.ApplyEnergyFromRegister(graphql.EnergyMergeKey{
				Channel: q.channel, Usage: q.usage, Period: "year", YearKind: "previous",
			}, 0)
			p.provider.ApplyEnergyFromRegister(graphql.EnergyMergeKey{
				Channel: q.channel, Usage: q.usage, Period: "day", YearKind: "",
			}, 0)
		}
	}
	if accepted > 0 || failed > 0 {
		log.Printf("semantic energy b524: accepted=%d failed=%d", accepted, failed)
	}
	if accepted > 0 {
		p.publishEnergyTotals()
	}
}

func (p *vaillantSemanticPoller) publishEnergyTotals() {
	if p == nil || p.provider == nil || p.hub == nil {
		return
	}
	totals := p.provider.EnergyTotals()
	if totals == nil {
		return
	}
	p.hub.PublishEnergyUpdate(totals)
}

func (p *vaillantSemanticPoller) refreshDHW(ctx context.Context) semanticSnapshotSource {
	controller, _ := p.snapshotZones()
	if controller == 0 {
		return p.sourceFromEbusdGrab(p.refreshDHWFromEbusdGrab(ctx))
	}

	attempted := make(semanticFieldSet)
	liveReadSuccess := false
	currentPtr := p.readDhwFloat(ctx, dhw_current_temp)
	if currentPtr != nil {
		liveReadSuccess = true
		attempted[dhwFieldCurrentTempC] = struct{}{}
	}
	sfModeRaw, sfModeOK := p.readDhwUint16(ctx, dhw_special_function)
	if sfModeOK {
		liveReadSuccess = true
		attempted[dhwFieldOperatingMode] = struct{}{}
		attempted[dhwFieldPreset] = struct{}{}
	}
	if sfModeOK {
		attempted[dhwFieldSpecialFunctionRaw] = struct{}{}
	}

	if !liveReadSuccess {
		return p.sourceFromEbusdGrab(p.refreshDHWFromEbusdGrab(ctx))
	}

	status := &vaillantDhwSnapshot{
		CurrentTempC: currentPtr,
	}
	opModeRaw, cachedSF := p.cachedDHWDerivationInputs()
	if opModeRaw != nil || hasMeaningfulSpecialFunction(sfModeRaw) {
		sfForDerive := sfModeRaw
		if sfForDerive == nil {
			sfForDerive = cachedSF
		}
		operatingMode, preset := deriveDhwModeAndPreset(opModeRaw, sfForDerive)
		if opModeRaw != nil {
			status.OperatingMode = operatingMode
		}
		status.Preset = preset
	}
	if sfModeRaw != nil {
		status.StateSpecialFunction = formatUintToken(*sfModeRaw)
	}

	p.mu.Lock()
	if p.dhw == nil {
		p.dhw = &vaillantDhwSnapshot{}
	}
	mergeDhwSnapshotFields(p.dhw, status, semanticSnapshotSourceLive, attempted)
	p.markDHWUpdatedNowLocked()
	p.mu.Unlock()
	return semanticSnapshotSourceLive
}

func (p *vaillantSemanticPoller) sourceFromEbusdGrab(ok bool) semanticSnapshotSource {
	if ok {
		if p != nil && p.transportConfig.Protocol == ebusgateway.TransportEbusdTCP {
			return semanticSnapshotSourceLive
		}
		return semanticSnapshotSourceCache
	}
	return semanticSnapshotSourceCache
}

// readDhwFloat reads a DHW register as float32 from OP=0x02/GG=0x01 only.
// OP=0x06/GG=0x01 is "Primary Heating Sources", NOT DHW — no fallback.
func (p *vaillantSemanticPoller) readDhwFloat(ctx context.Context, addr uint16) *float64 {
	value, ok := p.readB524Float32LE(ctx, localDHW.opcode, localDHW.group, dhwInstance, addr)
	if !ok {
		return nil
	}
	floatValue := value
	return &floatValue
}

// readDhwUint16 reads a DHW register as uint16 from OP=0x02/GG=0x01 only.
// OP=0x06/GG=0x01 is "Primary Heating Sources", NOT DHW — no fallback.
func (p *vaillantSemanticPoller) readDhwUint16(ctx context.Context, addr uint16) (*uint16, bool) {
	value, ok := p.readB524Uint16(ctx, localDHW.opcode, localDHW.group, dhwInstance, addr)
	if ok {
		return value, true
	}
	return nil, false
}

func (p *vaillantSemanticPoller) expireDHWIfStaleLocked(source semanticSnapshotSource) bool {
	if p == nil || source != semanticSnapshotSourceCache {
		return false
	}
	if p.dhw == nil || p.dhwStaleTTL <= 0 || p.dhwLastUpdateAt.IsZero() {
		return false
	}
	age := p.now().Sub(p.dhwLastUpdateAt)
	if age < p.dhwStaleTTL {
		return false
	}
	semanticDHWStaleExpiryTotal.Add(1)
	log.Printf(
		"semantic_dhw_expired ttl=%s age=%s last_update_utc=%s",
		p.dhwStaleTTL.Round(time.Second),
		age.Round(time.Second),
		p.dhwLastUpdateAt.UTC().Format(time.RFC3339),
	)
	p.dhw = nil
	p.dhwLastUpdateAt = time.Time{}
	return true
}

func (p *vaillantSemanticPoller) recordZonePresenceTransitionLocked(instance byte, previous, next zonePresenceState, hitStreak, missStreak int) {
	if previous == next {
		return
	}
	semanticZonePresenceTransitionsTotal.Add(fmt.Sprintf("%s->%s", previous, next), 1)
	log.Printf(
		"semantic_zone_presence_transition instance=%d from=%s to=%s hit_streak=%d miss_streak=%d zone_count=%d",
		instance,
		previous,
		next,
		hitStreak,
		missStreak,
		len(p.zones),
	)
}

func (p *vaillantSemanticPoller) publishDHW(source semanticSnapshotSource) {
	if p.provider == nil {
		return
	}

	p.mu.Lock()
	expired := p.expireDHWIfStaleLocked(source)
	snapshot := p.dhw
	p.mu.Unlock()

	previous := p.provider.DHW()
	if snapshot == nil {
		if source == semanticSnapshotSourceCache && previous != nil && !expired {
			return
		}
		switch source {
		case semanticSnapshotSourceCache:
			p.provider.SetDHWFromCache(nil)
		default:
			p.provider.SetDHW(nil)
		}
		if previous != nil && p.hub != nil {
			p.hub.PublishDHWUpdate(nil)
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
		p.provider.SetDHWFromCache(current)
	default:
		p.provider.SetDHW(current)
	}
	if p.hub != nil && !dhwEquals(previous, current) {
		p.hub.PublishDHWUpdate(current)
	}
	p.persistSemanticCache(source)
}

func (p *vaillantSemanticPoller) refreshCircuits(ctx context.Context) {
	if p == nil {
		return
	}

	now := p.now()
	p.mu.Lock()
	controller := p.controller
	instances, fullScan := p.circuitRefreshInstancesLocked(now)
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	discovered := make(map[byte]*vaillantCircuitSnapshot, 4)
	inactive := make(map[byte]struct{})
	probeSuccess := false

	for _, instance := range instances {
		circuitTypeRaw, ok := p.readB524Uint16(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_type)
		if !ok || circuitTypeRaw == nil {
			continue
		}
		probeSuccess = true
		switch *circuitTypeRaw {
		case 0x0000, 0x00FF, 0xFFFF:
			if snapshot, known := p.readDhwPseudoCircuitEvidence(ctx, instance); snapshot != nil {
				snapshot.Controller = controller
				discovered[instance] = snapshot
				continue
			} else if !known && instance == dhwPseudoCircuitInstance {
				continue
			}
			inactive[instance] = struct{}{}
		default:
			discovered[instance] = &vaillantCircuitSnapshot{
				Instance:       instance,
				Active:         true,
				Controller:     controller,
				CircuitTypeRaw: cloneUint16Ptr(circuitTypeRaw),
			}
		}
	}
	if !probeSuccess {
		if !fullScan {
			p.mu.Lock()
			p.lastCircuitFullScanAt = time.Time{}
			p.lastCircuitFullScanComplete = false
			p.mu.Unlock()
		}
		return
	}

	updates := make(map[byte]*vaillantCircuitSnapshot, len(discovered))
	anyRead := false
	for instance, discoveredSnapshot := range discovered {
		snapshot := cloneCircuitSnapshot(discoveredSnapshot)
		if snapshot == nil {
			continue
		}
		anyRead = true

		if raw, ok := p.readB524Uint16(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_cooling_enabled); ok && raw != nil {
			snapshot.CoolingEnabledRaw = cloneUint16Ptr(raw)
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_flow_setpoint); ok {
			v := value
			snapshot.FlowSetpointC = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_flow_temp); ok {
			v := value
			snapshot.FlowTemperatureC = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_heating_curve); ok {
			v := value
			snapshot.HeatingCurve = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_flow_temp_max); ok {
			v := value
			snapshot.FlowTempMaxC = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_flow_temp_min); ok {
			v := value
			snapshot.FlowTempMinC = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_summer_limit); ok {
			v := value
			snapshot.SummerLimitC = &v
			anyRead = true
		}
		if raw, ok := p.readB524Uint16(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_room_temp_control); ok && raw != nil {
			snapshot.RoomTempControlRaw = cloneUint16Ptr(raw)
			anyRead = true
		}
		if raw, ok := p.readB524Uint16(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_circuit_state); ok && raw != nil {
			snapshot.CircuitStateRaw = cloneUint16Ptr(raw)
			snapshot.CircuitStateLiveAt = now
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_frost_protection); ok {
			v := value
			snapshot.FrostProtectionC = &v
			anyRead = true
		}
		if raw, ok := p.readB524Uint16(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_pump_status); ok && raw != nil {
			snapshot.PumpStatusRaw = cloneUint16Ptr(raw)
			snapshot.PumpStatusLiveAt = now
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_calc_flow_temp); ok {
			v := value
			snapshot.CalcFlowTempC = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_mixer_position); ok {
			v := value
			snapshot.MixerPositionPct = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_humidity); ok {
			v := value
			snapshot.HumidityPct = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_dew_point); ok {
			v := value
			snapshot.DewPointC = &v
			anyRead = true
		}
		if value, ok := p.readB524Uint32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_pump_hours); ok {
			v := value
			snapshot.PumpHoursRaw = &v
			anyRead = true
		}
		if value, ok := p.readB524Uint32LE(ctx, localCircuits.opcode, localCircuits.group, instance, circuit_pump_starts); ok {
			v := value
			snapshot.PumpStartsRaw = &v
			anyRead = true
		}

		updates[instance] = snapshot
	}

	p.mu.Lock()
	if p.circuits == nil {
		p.circuits = make(map[byte]*vaillantCircuitSnapshot)
	}
	for instance := range inactive {
		delete(p.circuits, instance)
	}
	for instance, incoming := range updates {
		p.circuits[instance] = mergeCircuitSnapshotNonDestructive(p.circuits[instance], incoming)
	}
	if fullScan {
		p.lastCircuitFullScanAt = now
		p.lastCircuitFullScanComplete = len(discovered)+len(inactive) == len(instances)
	}
	p.mu.Unlock()

	source := semanticSnapshotSourceCache
	if anyRead || len(inactive) > 0 {
		source = semanticSnapshotSourceLive
	}
	p.publishCircuits(source)
}

func (p *vaillantSemanticPoller) readDhwPseudoCircuitEvidence(ctx context.Context, instance byte) (*vaillantCircuitSnapshot, bool) {
	if p == nil || instance != dhwPseudoCircuitInstance {
		return nil, false
	}
	snapshot := newDhwPseudoCircuitSnapshot(instance)
	flow, flowKnown := p.readDhwPseudoCircuitTemperatureEvidence(ctx, instance, circuit_flow_temp)
	if isDhwPseudoCircuitTemperatureEvidence(flow) {
		snapshot.FlowTemperatureC = flow
	}
	calc, calcKnown := p.readDhwPseudoCircuitTemperatureEvidence(ctx, instance, circuit_calc_flow_temp)
	if isDhwPseudoCircuitTemperatureEvidence(calc) {
		snapshot.CalcFlowTempC = calc
	}
	if snapshot.FlowTemperatureC == nil && snapshot.CalcFlowTempC == nil {
		return nil, flowKnown && calcKnown
	}
	return snapshot, true
}

func (p *vaillantSemanticPoller) readDhwPseudoCircuitTemperatureEvidence(ctx context.Context, instance byte, addr uint16) (*float64, bool) {
	raw, ok := p.readB524Value(ctx, localCircuits.opcode, localCircuits.group, instance, addr)
	if !ok {
		return nil, false
	}
	return decodeB524Float32FromRaw(raw), true
}

func newDhwPseudoCircuitSnapshot(instance byte) *vaillantCircuitSnapshot {
	circuitType := dhwPseudoCircuitTypeRaw
	return &vaillantCircuitSnapshot{
		Instance:       instance,
		Active:         true,
		CircuitTypeRaw: &circuitType,
	}
}

func isDhwPseudoCircuitTemperatureEvidence(value *float64) bool {
	if value == nil {
		return false
	}
	return *value >= 5 && *value <= 95
}

func allCircuitRefreshInstances() []byte {
	instances := make([]byte, 0, 0x0B)
	for instance := byte(0x00); instance <= 0x0A; instance++ {
		instances = append(instances, instance)
	}
	return instances
}

func (p *vaillantSemanticPoller) circuitRefreshInstancesLocked(now time.Time) ([]byte, bool) {
	if p == nil {
		return nil, false
	}
	interval := p.circuitFullScanInterval
	if interval <= 0 {
		interval = semanticCircuitFullScanInterval
	}
	if len(p.circuits) == 0 || p.lastCircuitFullScanAt.IsZero() || now.Sub(p.lastCircuitFullScanAt) >= interval {
		return allCircuitRefreshInstances(), true
	}
	if !p.lastCircuitFullScanComplete && now.Sub(p.lastCircuitFullScanAt) >= semanticCircuitPartialScanInterval {
		return allCircuitRefreshInstances(), true
	}

	instances := make([]byte, 0, len(p.circuits))
	for instance, snapshot := range p.circuits {
		if snapshot == nil || !snapshot.Active {
			continue
		}
		instances = append(instances, instance)
	}
	if len(instances) == 0 {
		return allCircuitRefreshInstances(), true
	}
	slices.Sort(instances)
	return instances, false
}

func mergeCircuitSnapshotNonDestructive(existing, incoming *vaillantCircuitSnapshot) *vaillantCircuitSnapshot {
	merged := cloneCircuitSnapshot(existing)
	if merged == nil {
		merged = &vaillantCircuitSnapshot{}
	}
	if incoming == nil {
		return merged
	}

	merged.Instance = incoming.Instance
	merged.Active = merged.Active || incoming.Active
	controllerChanged := incoming.Controller != 0 && incoming.Controller != merged.Controller
	if controllerChanged && incoming.CircuitStateRaw == nil {
		merged.CircuitStateRaw = nil
		merged.CircuitStateLiveAt = time.Time{}
	}
	if controllerChanged && incoming.PumpStatusRaw == nil {
		merged.PumpStatusRaw = nil
		merged.PumpStatusLiveAt = time.Time{}
	}
	if incoming.Controller != 0 {
		merged.Controller = incoming.Controller
	}
	if incoming.CircuitTypeRaw != nil {
		merged.CircuitTypeRaw = cloneUint16Ptr(incoming.CircuitTypeRaw)
	}
	if incoming.CoolingEnabledRaw != nil {
		merged.CoolingEnabledRaw = cloneUint16Ptr(incoming.CoolingEnabledRaw)
	}
	if incoming.FlowSetpointC != nil {
		merged.FlowSetpointC = cloneFloat64Ptr(incoming.FlowSetpointC)
	}
	if incoming.FlowTemperatureC != nil {
		merged.FlowTemperatureC = cloneFloat64Ptr(incoming.FlowTemperatureC)
	}
	if incoming.HeatingCurve != nil {
		merged.HeatingCurve = cloneFloat64Ptr(incoming.HeatingCurve)
	}
	if incoming.FlowTempMaxC != nil {
		merged.FlowTempMaxC = cloneFloat64Ptr(incoming.FlowTempMaxC)
	}
	if incoming.FlowTempMinC != nil {
		merged.FlowTempMinC = cloneFloat64Ptr(incoming.FlowTempMinC)
	}
	if incoming.SummerLimitC != nil {
		merged.SummerLimitC = cloneFloat64Ptr(incoming.SummerLimitC)
	}
	if incoming.RoomTempControlRaw != nil {
		merged.RoomTempControlRaw = cloneUint16Ptr(incoming.RoomTempControlRaw)
	}
	if incoming.CircuitStateRaw != nil {
		merged.CircuitStateRaw = cloneUint16Ptr(incoming.CircuitStateRaw)
		merged.CircuitStateLiveAt = incoming.CircuitStateLiveAt
	}
	if incoming.FrostProtectionC != nil {
		merged.FrostProtectionC = cloneFloat64Ptr(incoming.FrostProtectionC)
	}
	if incoming.PumpStatusRaw != nil {
		merged.PumpStatusRaw = cloneUint16Ptr(incoming.PumpStatusRaw)
		merged.PumpStatusLiveAt = incoming.PumpStatusLiveAt
	}
	if incoming.CalcFlowTempC != nil {
		merged.CalcFlowTempC = cloneFloat64Ptr(incoming.CalcFlowTempC)
	}
	if incoming.MixerPositionPct != nil {
		merged.MixerPositionPct = cloneFloat64Ptr(incoming.MixerPositionPct)
	}
	if incoming.HumidityPct != nil {
		merged.HumidityPct = cloneFloat64Ptr(incoming.HumidityPct)
	}
	if incoming.DewPointC != nil {
		merged.DewPointC = cloneFloat64Ptr(incoming.DewPointC)
	}
	if incoming.PumpHoursRaw != nil {
		merged.PumpHoursRaw = cloneUint32Ptr(incoming.PumpHoursRaw)
	}
	if incoming.PumpStartsRaw != nil {
		merged.PumpStartsRaw = cloneUint32Ptr(incoming.PumpStartsRaw)
	}
	return merged
}

func cloneCircuitSnapshot(snapshot *vaillantCircuitSnapshot) *vaillantCircuitSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantCircuitSnapshot{
		Instance:           snapshot.Instance,
		Active:             snapshot.Active,
		Controller:         snapshot.Controller,
		CircuitTypeRaw:     cloneUint16Ptr(snapshot.CircuitTypeRaw),
		CoolingEnabledRaw:  cloneUint16Ptr(snapshot.CoolingEnabledRaw),
		FlowSetpointC:      cloneFloat64Ptr(snapshot.FlowSetpointC),
		FlowTemperatureC:   cloneFloat64Ptr(snapshot.FlowTemperatureC),
		HeatingCurve:       cloneFloat64Ptr(snapshot.HeatingCurve),
		FlowTempMaxC:       cloneFloat64Ptr(snapshot.FlowTempMaxC),
		FlowTempMinC:       cloneFloat64Ptr(snapshot.FlowTempMinC),
		SummerLimitC:       cloneFloat64Ptr(snapshot.SummerLimitC),
		RoomTempControlRaw: cloneUint16Ptr(snapshot.RoomTempControlRaw),
		CircuitStateRaw:    cloneUint16Ptr(snapshot.CircuitStateRaw),
		CircuitStateLiveAt: snapshot.CircuitStateLiveAt,
		FrostProtectionC:   cloneFloat64Ptr(snapshot.FrostProtectionC),
		PumpStatusRaw:      cloneUint16Ptr(snapshot.PumpStatusRaw),
		PumpStatusLiveAt:   snapshot.PumpStatusLiveAt,
		CalcFlowTempC:      cloneFloat64Ptr(snapshot.CalcFlowTempC),
		MixerPositionPct:   cloneFloat64Ptr(snapshot.MixerPositionPct),
		HumidityPct:        cloneFloat64Ptr(snapshot.HumidityPct),
		DewPointC:          cloneFloat64Ptr(snapshot.DewPointC),
		PumpHoursRaw:       cloneUint32Ptr(snapshot.PumpHoursRaw),
		PumpStartsRaw:      cloneUint32Ptr(snapshot.PumpStartsRaw),
	}
}

func (p *vaillantSemanticPoller) publishCircuits(source semanticSnapshotSource) {
	if p == nil || p.provider == nil {
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
				HeatingCurve:    cloneFloat64Ptr(snapshot.HeatingCurve),
				FlowTempMaxC:    cloneFloat64Ptr(snapshot.FlowTempMaxC),
				FlowTempMinC:    cloneFloat64Ptr(snapshot.FlowTempMinC),
				SummerLimitC:    cloneFloat64Ptr(snapshot.SummerLimitC),
				FrostProtC:      cloneFloat64Ptr(snapshot.FrostProtectionC),
				RoomTempControl: decodeRoomTempControlToken(snapshot.RoomTempControlRaw),
				CoolingEnabled:  decodeUint16Bool(snapshot.CoolingEnabledRaw),
			},
			ManagingDevice: deriveCircuitManagingDevice(systemSnapshot, fm5Mode),
		}
		out = append(out, status)
	}

	previous := p.provider.Circuits()
	if len(out) == 0 && previous == nil {
		return
	}
	if source == semanticSnapshotSourceCache && len(out) == 0 && len(previous) > 0 {
		return
	}
	switch source {
	case semanticSnapshotSourceCache:
		p.provider.SetCircuitsFromCache(out)
	default:
		p.provider.SetCircuits(out)
	}
	if p.hub != nil && !circuitsEqual(previous, out) {
		publisher := reflect.ValueOf(p.hub).MethodByName("PublishCircuitsUpdate")
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
		if !floatPtrEquals(left.Config.FrostProtC, right.Config.FrostProtC) {
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

func decodeHeatingCircuitTypeToken(raw *uint16) string {
	if raw == nil {
		return ""
	}
	switch *raw {
	case 1:
		return "heating"
	case 2:
		return "fixed_value"
	case 3:
		return "dhw"
	case 4:
		return "return_increase"
	default:
		return fmt.Sprintf("unknown_%d", *raw)
	}
}

func decodeRoomTempControlToken(raw *uint16) string {
	if raw == nil {
		return ""
	}
	switch *raw {
	case 0:
		return "off"
	case 1:
		return "modulating"
	case 2:
		return "thermostat"
	default:
		return fmt.Sprintf("unknown_%d", *raw)
	}
}

var circuitStateNames = map[uint16]string{
	0: CircuitStateStandby,
	1: CircuitStateHeating,
	2: CircuitStateCooling,
}

func decodeCircuitStateToken(raw *uint16) string {
	if raw == nil {
		return ""
	}
	if name, ok := circuitStateNames[*raw]; ok {
		return name
	}
	return fmt.Sprintf("unknown_%d", *raw)
}

func decodeUint16Bool(raw *uint16) *bool {
	if raw == nil {
		return nil
	}
	parsed := *raw != 0
	return &parsed
}

func decodeUint32Float64(raw *uint32) *float64 {
	if raw == nil {
		return nil
	}
	parsed := float64(*raw)
	return &parsed
}

func decodeUint32Int(raw *uint32) *int {
	if raw == nil {
		return nil
	}
	parsed := int(*raw)
	return &parsed
}

// --- Boiler status ---

// B524 registers on the controller that mirror boiler operational data.
// Group 0x00 (regulator parameters), instance 0x00.
const (
	regulatorInstance = byte(0x00)

	// --- System registers: GG=0x00 / OP=0x02 ---

	// STATE registers (read-only live values)
	system_off                             = uint16(0x0007)
	system_water_pressure                  = uint16(0x0039)
	system_flow_temperature                = uint16(0x004B)
	system_outdoor_temperature             = uint16(0x0073)
	system_outdoor_temperature_avg_24h     = uint16(0x0095)
	system_maintenance_due                 = uint16(0x0096)
	system_hwc_cylinder_temperature_top    = uint16(0x009D)
	system_hwc_cylinder_temperature_bottom = uint16(0x009E)

	// CONFIG registers (user-writable settings)
	system_adaptive_heating_curve          = uint16(0x0014)
	system_alternative_point               = uint16(0x0022)
	system_heating_circuit_bivalence_point = uint16(0x0023)
	system_dhw_bivalence_point             = uint16(0x0001)
	system_hc_emergency_temperature        = uint16(0x0026)
	system_hwc_max_flow_temp_desired       = uint16(0x0046)
	system_max_room_humidity               = uint16(0x000E)
	system_maintenance_date                = uint16(0x002C)
	system_installer_name_1                = uint16(0x006C)
	system_installer_name_2                = uint16(0x006D)
	system_installer_phone_1               = uint16(0x006F)
	system_installer_phone_2               = uint16(0x0070)
	system_installer_menu_code             = uint16(0x0076)

	// PARAMS registers (system topology / identification)
	system_scheme                    = uint16(0x0036)
	system_module_configuration_vr71 = uint16(0x002F)

	// --- Boiler B509 direct registers on BAI00 ---
	boiler_b509_water_pressure            = uint16(0x0200)
	boiler_b509_flame_active              = uint16(0x0500)
	boiler_b509_partload_hc_kw            = uint16(0x0704)
	boiler_b509_partload_hwc_kw           = uint16(0x0804)
	boiler_b509_flowset_hc_max_c          = uint16(0x0E04)
	boiler_b509_flowset_hc_max_c_fallback = uint16(0xA500)
	boiler_b509_flowset_hwc_max_c         = uint16(0x0F04)
	boiler_b509_pump_hours                = uint16(0x1400)
	boiler_b509_flow_temperature          = uint16(0x1800)
	boiler_b509_fan_hours                 = uint16(0x1B00)
	boiler_b509_deactivations_ifc         = uint16(0x1F00)
	boiler_b509_hours_till_service        = uint16(0x2004)
	boiler_b509_deactivations_limit       = uint16(0x2000)
	boiler_b509_dhw_hours                 = uint16(0x2200)
	boiler_b509_dhw_starts                = uint16(0x2300)
	boiler_b509_target_fan_speed_rpm      = uint16(0x2400)
	boiler_b509_central_heating_hours     = uint16(0x2800)
	boiler_b509_central_heating_starts    = uint16(0x2900)
	boiler_b509_modulation_pct            = uint16(0x2E00)
	boiler_b509_flow_temp_desired_c       = uint16(0x3900)
	boiler_b509_external_pump_active      = uint16(0x3F00)
	boiler_b509_installer_menu_code       = uint16(0x4904)
	boiler_b509_central_heating_pump      = uint16(0x4400)
	boiler_b509_diverter_valve_position   = uint16(0x5400)
	boiler_b509_dhw_water_flow_lpm        = uint16(0x5500)
	boiler_b509_dhw_demand_active         = uint16(0x5800)
	boiler_b509_circulation_pump_active   = uint16(0x7B00)
	boiler_b509_phone_number              = uint16(0x8104)
	boiler_b509_fan_speed_rpm             = uint16(0x8300)
	boiler_b509_storage_load_pump_pct     = uint16(0x9E00)
	boiler_b509_ionisation_voltage_ua     = uint16(0xA400)
	boiler_b509_state_number              = uint16(0xAB00)
	boiler_b509_gas_valve_active          = uint16(0xBB00)
	boiler_b509_heating_switch_active     = uint16(0xF203)
	boiler_b509_dhw_temp_desired_c        = uint16(0xEA03)
	boiler_b509_primary_circuit_flow_lpm  = uint16(0xFB00)
)

type vaillantBoilerSnapshot struct {
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
	DhwOperatingMode         *int
	FlowsetHcMaxC            *float64
	FlowsetHwcMaxC           *float64
	PartloadHcKW             *float64
	PartloadHwcKW            *float64
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
	InstallerMenuCode        *int
	PhoneNumber              *string
	HoursTillService         *int
}

type vaillantSystemSnapshot struct {
	Controller byte

	// State
	SystemOff                    *bool
	SystemWaterPressure          *float64
	SystemFlowTemperature        *float64
	SystemFlowTemperatureLiveAt  time.Time
	OutdoorTemperature           *float64
	OutdoorTemperatureAvg24h     *float64
	MaintenanceDue               *bool
	HwcCylinderTemperatureTop    *float64
	HwcCylinderTemperatureBottom *float64

	// Config
	AdaptiveHeatingCurve         *bool
	AlternativePoint             *float64
	HeatingCircuitBivalencePoint *float64
	DhwBivalencePoint            *float64
	HcEmergencyTemperature       *float64
	HwcMaxFlowTempDesired        *float64
	MaxRoomHumidity              *uint16
	MaintenanceDate              *string
	InstallerName                *string
	InstallerPhone               *string
	InstallerMenuCode            *uint16

	// Properties
	SystemScheme            *uint16
	ModuleConfigurationVR71 *uint16
}

type boilerStatusField uint8

const (
	boilerStatusFieldFlowTemperature boilerStatusField = iota
	boilerStatusFieldPumpActive
	boilerStatusFieldHeatingStatusRaw
)

type boilerStatusRegisterDecoder uint8

const (
	boilerStatusRegisterDecoderFloat32 boilerStatusRegisterDecoder = iota
	boilerStatusRegisterDecoderUint16Bool
	boilerStatusRegisterDecoderUint16Int
)

type boilerStatusRegisterDefinition struct {
	field    boilerStatusField
	decoder  boilerStatusRegisterDecoder
	opcode   byte
	group    byte
	instance byte
	addr     uint16
}

// findBoilerAddress is retained for potential future use (e.g. broadcast sniffing).
// Currently unused — boiler data is read via B524 registers on the controller.

func boilerStatusRegisterDefinitionsForTier(tier boilerStatusTier) []boilerStatusRegisterDefinition {
	switch tier {
	case boilerStatusTierFast:
		return []boilerStatusRegisterDefinition{
			{
				field:    boilerStatusFieldFlowTemperature,
				decoder:  boilerStatusRegisterDecoderFloat32,
				opcode:   localRegulator.opcode,
				group:    localRegulator.group,
				instance: regulatorInstance,
				addr:     system_flow_temperature,
			},
			{
				field:    boilerStatusFieldPumpActive,
				decoder:  boilerStatusRegisterDecoderUint16Bool,
				opcode:   localCircuits.opcode,
				group:    localCircuits.group,
				instance: 0x00,
				addr:     circuit_pump_status,
			},
			{
				field:    boilerStatusFieldHeatingStatusRaw,
				decoder:  boilerStatusRegisterDecoderUint16Int,
				opcode:   localCircuits.opcode,
				group:    localCircuits.group,
				instance: 0x00,
				addr:     circuit_circuit_state,
			},
		}
	case boilerStatusTierMedium, boilerStatusTierSlow:
		// Reduced profile: scaffolding only. Additional mappings are added once
		// authoritative B524 semantics are promoted for boiler_status.
		return nil
	default:
		return nil
	}
}

// refreshBoilerStatus reads boiler operational data via B524 registers on the controller.
// The BAI boiler does not respond to direct B504 reads from third-party sources — it only
// accepts requests from its paired controller. Instead, the controller (VRC/BASV2) mirrors
// boiler data in its own B524 register space, which we can read reliably.
func (p *vaillantSemanticPoller) refreshBoilerStatus(ctx context.Context) {
	p.refreshBoilerStatusTier(ctx, boilerStatusTierFast)
}

func (p *vaillantSemanticPoller) refreshBoilerStatusTier(ctx context.Context, tier boilerStatusTier) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	boilerAddress := p.boilerAddress
	p.mu.Unlock()
	if controller == 0 && boilerAddress == 0 {
		return
	}

	snapshot := &vaillantBoilerSnapshot{}
	updated := false
	if controller != 0 {
		updated = p.refreshBoilerStatusB524(ctx, tier, snapshot) || updated
	}
	if boilerAddress != 0 {
		updated = p.refreshBoilerStatusB509(ctx, boilerAddress, tier, snapshot) || updated
	}
	dhwUpdated := false
	if tier == boilerStatusTierFast {
		dhwUpdated = p.mergeBoilerDHWFieldsFromSnapshot(snapshot)
	}

	// Preserve last-known values on transient/partial failures.
	if !updated {
		if dhwUpdated {
			p.mu.Lock()
			p.boiler = mergeBoilerSnapshotNonDestructive(p.boiler, snapshot)
			p.mu.Unlock()
		}
		return
	}

	p.mu.Lock()
	p.boiler = mergeBoilerSnapshotNonDestructive(p.boiler, snapshot)
	p.mu.Unlock()

	p.publishBoilerStatus(semanticSnapshotSourceLive)
}

func (p *vaillantSemanticPoller) refreshBoilerStatusB524(ctx context.Context, tier boilerStatusTier, snapshot *vaillantBoilerSnapshot) bool {
	if p == nil || snapshot == nil {
		return false
	}

	if tier == boilerStatusTierFast {
		return p.mergeBoilerMirrorFieldsFromSnapshots(snapshot)
	}

	updated := false
	registers := boilerStatusRegisterDefinitionsForTier(tier)
	for _, register := range registers {
		switch register.decoder {
		case boilerStatusRegisterDecoderFloat32:
			value, ok := p.readB524Float32LE(ctx, register.opcode, register.group, register.instance, register.addr)
			if !ok {
				continue
			}
			updated = true
			switch register.field {
			case boilerStatusFieldFlowTemperature:
				snapshot.FlowTemperatureC = &value
			}
		case boilerStatusRegisterDecoderUint16Bool:
			raw, ok := p.readB524Uint16(ctx, register.opcode, register.group, register.instance, register.addr)
			if !ok || raw == nil {
				continue
			}
			updated = true
			switch register.field {
			case boilerStatusFieldPumpActive:
				active := *raw != 0
				snapshot.CentralHeatingPumpActive = &active
			}
		case boilerStatusRegisterDecoderUint16Int:
			raw, ok := p.readB524Uint16(ctx, register.opcode, register.group, register.instance, register.addr)
			if !ok || raw == nil {
				continue
			}
			updated = true
			switch register.field {
			case boilerStatusFieldHeatingStatusRaw:
				status := int(*raw)
				snapshot.HeatingStatusRaw = &status
			}
		}
	}

	return updated
}

func (p *vaillantSemanticPoller) mergeBoilerMirrorFieldsFromSnapshots(snapshot *vaillantBoilerSnapshot) bool {
	if p == nil || snapshot == nil {
		return false
	}

	p.mu.Lock()
	controller := p.controller
	system := cloneSystemSnapshot(p.system)
	circuit := cloneCircuitSnapshot(p.circuits[0x00])
	p.mu.Unlock()
	if controller == 0 {
		return false
	}

	now := p.now()
	maxAge := p.boilerMirrorMaxAge()
	updated := false
	if system != nil && system.Controller == controller && system.SystemFlowTemperature != nil && semanticSnapshotObservedWithin(system.SystemFlowTemperatureLiveAt, now, maxAge) {
		snapshot.FlowTemperatureC = cloneFloat64Ptr(system.SystemFlowTemperature)
		updated = true
	}
	if circuit != nil && circuit.Controller == controller {
		if circuit.PumpStatusRaw != nil && semanticSnapshotObservedWithin(circuit.PumpStatusLiveAt, now, maxAge) {
			active := *circuit.PumpStatusRaw != 0
			snapshot.CentralHeatingPumpActive = &active
			updated = true
		}
		if circuit.CircuitStateRaw != nil && semanticSnapshotObservedWithin(circuit.CircuitStateLiveAt, now, maxAge) {
			status := int(*circuit.CircuitStateRaw)
			snapshot.HeatingStatusRaw = &status
			updated = true
		}
	}
	if updated {
		p.mergeBoilerDHWFieldsFromSnapshot(snapshot)
	}
	return updated
}

func (p *vaillantSemanticPoller) boilerMirrorMaxAge() time.Duration {
	interval := p.configInterval
	if interval <= 0 {
		interval = ebusgateway.DefaultConfig().SemanticConfigInterval
	}
	fastInterval := p.boilerFastInterval
	if fastInterval <= 0 {
		fastInterval = 30 * time.Second
	}
	return interval + fastInterval
}

func semanticSnapshotObservedWithin(observedAt, now time.Time, maxAge time.Duration) bool {
	if observedAt.IsZero() {
		return false
	}
	if maxAge <= 0 {
		return true
	}
	if now.Before(observedAt) {
		return true
	}
	return now.Sub(observedAt) <= maxAge
}

func (p *vaillantSemanticPoller) mergeBoilerDHWFieldsFromSnapshot(snapshot *vaillantBoilerSnapshot) bool {
	if p == nil || snapshot == nil {
		return false
	}

	p.mu.Lock()
	dhw := p.dhw
	if dhw == nil {
		p.mu.Unlock()
		return false
	}
	current := cloneFloat64Ptr(dhw.CurrentTempC)
	target := cloneFloat64Ptr(dhw.TargetTempC)
	operationMode := strings.TrimSpace(dhw.ConfigurationDHWOperationMode)
	p.mu.Unlock()

	updated := false
	if current != nil {
		snapshot.DhwTemperatureC = current
		updated = true
	}
	if target != nil {
		snapshot.DhwTargetTemperatureC = target
		updated = true
	}
	if operationMode != "" {
		if parsed, err := strconv.Atoi(operationMode); err == nil {
			snapshot.DhwOperatingMode = &parsed
			updated = true
		}
	}
	return updated
}

func (p *vaillantSemanticPoller) refreshBoilerStatusB509(ctx context.Context, boilerAddress byte, tier boilerStatusTier, snapshot *vaillantBoilerSnapshot) bool {
	if p == nil || snapshot == nil || boilerAddress == 0 {
		return false
	}

	updated := false
	switch tier {
	case boilerStatusTierFast:
		if value := p.readB509DATA2c(ctx, boilerAddress, boiler_b509_flow_temperature); value != nil {
			snapshot.FlowTemperatureC = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_central_heating_pump); value != nil {
			snapshot.CentralHeatingPumpActive = value
			updated = true
		}
		if value := p.readB509DATA2b(ctx, boilerAddress, boiler_b509_water_pressure); value != nil {
			snapshot.WaterPressureBar = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_flame_active); value != nil {
			snapshot.FlameActive = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_gas_valve_active); value != nil {
			snapshot.GasValveActive = value
			updated = true
		}
		if value := p.readB509UINInt(ctx, boilerAddress, boiler_b509_fan_speed_rpm); value != nil {
			snapshot.FanSpeedRpm = value
			updated = true
		}
		if value := p.readB509SINScaled(ctx, boilerAddress, boiler_b509_modulation_pct, 10); value != nil {
			snapshot.ModulationPct = value
			updated = true
		}
		if value := p.readB509UCHInt(ctx, boilerAddress, boiler_b509_state_number); value != nil {
			snapshot.StateNumber = value
			updated = true
		}
		if value := p.readB509UCHFloat(ctx, boilerAddress, boiler_b509_diverter_valve_position); value != nil {
			snapshot.DiverterValvePositionPct = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_dhw_demand_active); value != nil {
			snapshot.DhwDemandActive = value
			updated = true
		}
		if value := p.readB509UIN100(ctx, boilerAddress, boiler_b509_dhw_water_flow_lpm); value != nil {
			snapshot.DhwWaterFlowLpm = value
			updated = true
		}
	case boilerStatusTierMedium:
		if value := p.readB509BoilerConfigFloat(ctx, boilerAddress, "flowsetHcMaxC"); value != nil {
			snapshot.FlowsetHcMaxC = value
			updated = true
		}
		if value := p.readB509BoilerConfigFloat(ctx, boilerAddress, "flowsetHwcMaxC"); value != nil {
			snapshot.FlowsetHwcMaxC = value
			updated = true
		}
		if value := p.readB509BoilerConfigFloat(ctx, boilerAddress, "partloadHcKW"); value != nil {
			snapshot.PartloadHcKW = value
			updated = true
		}
		if value := p.readB509BoilerConfigFloat(ctx, boilerAddress, "partloadHwcKW"); value != nil {
			snapshot.PartloadHwcKW = value
			updated = true
		}
		if value := p.readB509Percent0(ctx, boilerAddress, boiler_b509_storage_load_pump_pct); value != nil {
			snapshot.StorageLoadPumpPct = value
			updated = true
		}
		if value := p.readB509DATA2c(ctx, boilerAddress, boiler_b509_flow_temp_desired_c); value != nil {
			snapshot.FlowTempDesiredC = value
			updated = true
		}
		if value := p.readB509DATA2c(ctx, boilerAddress, boiler_b509_dhw_temp_desired_c); value != nil {
			snapshot.DhwTempDesiredC = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_circulation_pump_active); value != nil {
			snapshot.CirculationPumpActive = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_external_pump_active); value != nil {
			snapshot.ExternalPumpActive = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boiler_b509_heating_switch_active); value != nil {
			snapshot.HeatingSwitchActive = value
			updated = true
		}
		if value := p.readB509UINInt(ctx, boilerAddress, boiler_b509_target_fan_speed_rpm); value != nil {
			snapshot.TargetFanSpeedRpm = value
			updated = true
		}
		if value := p.readB509SINScaled(ctx, boilerAddress, boiler_b509_ionisation_voltage_ua, 10); value != nil {
			snapshot.IonisationVoltageUa = value
			updated = true
		}
		if value := p.readB509UIN100(ctx, boilerAddress, boiler_b509_primary_circuit_flow_lpm); value != nil {
			snapshot.PrimaryCircuitFlowLpm = value
			updated = true
		}
	case boilerStatusTierSlow:
		if value := p.readB509Hoursum2(ctx, boilerAddress, boiler_b509_central_heating_hours); value != nil {
			snapshot.CentralHeatingHours = value
			updated = true
		}
		if value := p.readB509Hoursum2(ctx, boilerAddress, boiler_b509_dhw_hours); value != nil {
			snapshot.DhwHours = value
			updated = true
		}
		if value := p.readB509UINInt(ctx, boilerAddress, boiler_b509_central_heating_starts); value != nil {
			snapshot.CentralHeatingStarts = value
			updated = true
		}
		if value := p.readB509UINInt(ctx, boilerAddress, boiler_b509_dhw_starts); value != nil {
			snapshot.DhwStarts = value
			updated = true
		}
		if value := p.readB509Hoursum2(ctx, boilerAddress, boiler_b509_pump_hours); value != nil {
			snapshot.PumpHours = value
			updated = true
		}
		if value := p.readB509Hoursum2(ctx, boilerAddress, boiler_b509_fan_hours); value != nil {
			snapshot.FanHours = value
			updated = true
		}
		if value := p.readB509UCHInt(ctx, boilerAddress, boiler_b509_deactivations_ifc); value != nil {
			snapshot.DeactivationsIFC = value
			updated = true
		}
		if value := p.readB509UCHInt(ctx, boilerAddress, boiler_b509_deactivations_limit); value != nil {
			snapshot.DeactivationsTemplimiter = value
			updated = true
		}
		// Installer/maintenance config.
		if value := p.readB509UCHInt(ctx, boilerAddress, boiler_b509_installer_menu_code); value != nil {
			snapshot.InstallerMenuCode = value
			updated = true
		}
		if value := p.readB509PhoneBCD(ctx, boilerAddress, boiler_b509_phone_number); value != nil {
			snapshot.PhoneNumber = value
			updated = true
		}
		if value := p.readB509Hoursum2Int(ctx, boilerAddress, boiler_b509_hours_till_service); value != nil {
			snapshot.HoursTillService = value
			updated = true
		}
	}
	return updated
}

func (p *vaillantSemanticPoller) publishBoilerStatus(source semanticSnapshotSource) {
	if p == nil || p.provider == nil {
		return
	}

	p.mu.Lock()
	snapshot := p.boiler
	p.mu.Unlock()

	previous := p.provider.BoilerStatus()

	if snapshot == nil {
		if previous != nil {
			switch source {
			case semanticSnapshotSourceCache:
				p.provider.SetBoilerStatusFromCache(nil)
			default:
				p.provider.SetBoilerStatus(nil)
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
		p.provider.SetBoilerStatusFromCache(current)
	default:
		p.provider.SetBoilerStatus(current)
	}
	if p.hub != nil && !boilerStatusEquals(previous, current) {
		p.hub.PublishBoilerStatusUpdate(current)
	}
	p.persistSemanticCache(source)
}

func (p *vaillantSemanticPoller) refreshSystem(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	snapshot := &vaillantSystemSnapshot{Controller: controller}
	updated := false

	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_off); ok && raw != nil {
		value := *raw != 0
		snapshot.SystemOff = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_water_pressure); ok {
		snapshot.SystemWaterPressure = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_flow_temperature); ok {
		snapshot.SystemFlowTemperature = &value
		snapshot.SystemFlowTemperatureLiveAt = p.now()
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_outdoor_temperature); ok {
		snapshot.OutdoorTemperature = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_outdoor_temperature_avg_24h); ok {
		snapshot.OutdoorTemperatureAvg24h = &value
		updated = true
	}
	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_maintenance_due); ok && raw != nil {
		value := *raw != 0
		snapshot.MaintenanceDue = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_hwc_cylinder_temperature_top); ok {
		snapshot.HwcCylinderTemperatureTop = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_hwc_cylinder_temperature_bottom); ok {
		snapshot.HwcCylinderTemperatureBottom = &value
		updated = true
	}

	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_adaptive_heating_curve); ok && raw != nil {
		value := *raw != 0
		snapshot.AdaptiveHeatingCurve = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_alternative_point); ok {
		snapshot.AlternativePoint = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_heating_circuit_bivalence_point); ok {
		snapshot.HeatingCircuitBivalencePoint = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_dhw_bivalence_point); ok {
		snapshot.DhwBivalencePoint = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_hc_emergency_temperature); ok {
		snapshot.HcEmergencyTemperature = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_hwc_max_flow_temp_desired); ok {
		snapshot.HwcMaxFlowTempDesired = &value
		updated = true
	}
	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_max_room_humidity); ok && raw != nil {
		snapshot.MaxRoomHumidity = cloneUint16Ptr(raw)
		updated = true
	}

	// Installer/maintenance config (slow-config reads).
	if value, ok := p.readB524DateHDA3(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_maintenance_date); ok {
		snapshot.MaintenanceDate = &value
		updated = true
	}
	{
		// Combined installer name from 2 registers × 6 chars.
		name1, ok1 := p.readB524CStringSanitized(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_installer_name_1)
		name2, ok2 := p.readB524CStringSanitized(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_installer_name_2)
		if ok1 || ok2 {
			var p1, p2 string
			if ok1 {
				p1 = name1
			}
			if ok2 {
				p2 = name2
			}
			combined := strings.TrimRight(p1+p2, " \x00")
			snapshot.InstallerName = &combined
			updated = true
		}
	}
	{
		// Combined installer phone from 2 registers × 6 chars.
		phone1, ok1 := p.readB524CStringSanitized(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_installer_phone_1)
		phone2, ok2 := p.readB524CStringSanitized(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_installer_phone_2)
		if ok1 || ok2 {
			var p1, p2 string
			if ok1 {
				p1 = phone1
			}
			if ok2 {
				p2 = phone2
			}
			combined := strings.TrimRight(p1+p2, " \x00")
			snapshot.InstallerPhone = &combined
			updated = true
		}
	}
	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_installer_menu_code); ok && raw != nil {
		snapshot.InstallerMenuCode = cloneUint16Ptr(raw)
		updated = true
	}

	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_scheme); ok && raw != nil {
		snapshot.SystemScheme = cloneUint16Ptr(raw)
		updated = true
	}
	if raw, ok := p.readB524Uint16(ctx, localRegulator.opcode, localRegulator.group, regulatorInstance, system_module_configuration_vr71); ok && raw != nil {
		snapshot.ModuleConfigurationVR71 = cloneUint16Ptr(raw)
		updated = true
	}

	if !updated {
		return
	}

	p.mu.Lock()
	p.updateSystemSnapshotLocked(mergeSystemSnapshotNonDestructive(p.system, snapshot))
	p.mu.Unlock()

	p.publishSystem(semanticSnapshotSourceLive)
	p.refreshFM5Semantic(ctx)
}

// discoverDeviceSlots probes all OP=0x06 device slot groups to find which
// slots should be retained for steady-state detail refresh. Connected slots
// are retained. Disconnected functional module slots are retained only when
// identity registers prove inventory, preserving FM5 evidence without
// refreshing empty regulator/thermostat slots every config tick.
// This runs at startup and every deviceSlotRediscoveryTTL to avoid
// probing empty slots on every poll cycle (~30 timeouts eliminated).
func (p *vaillantSemanticPoller) discoverDeviceSlots(ctx context.Context) (map[deviceSlotKey]bool, bool) {
	active, observedAny, _, _ := p.discoverDeviceSlotsWithFM5Completeness(ctx)
	return active, observedAny
}

func (p *vaillantSemanticPoller) discoverDeviceSlotsWithFM5Completeness(ctx context.Context) (map[deviceSlotKey]bool, bool, bool, map[radioDeviceKey]*vaillantRadioDeviceSnapshot) {
	active := make(map[deviceSlotKey]bool)
	observedAny := false
	fm5NamespaceComplete := true
	fm5Snapshots := make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
	for _, grp := range remoteDeviceGroups {
		for instance := byte(0x00); instance <= 0x0A; instance++ {
			if grp.group == remoteFunctionalModules.group {
				snapshot, observed, complete := p.readFunctionalModuleIdentitySnapshot(ctx, instance)
				if !observed {
					fm5NamespaceComplete = false
					continue
				}
				observedAny = true
				if !complete {
					fm5NamespaceComplete = false
				}
				if snapshot != nil {
					active[deviceSlotKey{Group: grp.group, Instance: instance}] = true
					fm5Snapshots[radioDeviceKey{Group: grp.group, Instance: instance}] = snapshot
				}
				continue
			}

			connectedRaw := p.readB524U8(ctx, grp.opcode, grp.group, instance, device_slot_connected)
			if connectedRaw == nil {
				continue
			}
			observedAny = true
			if *connectedRaw == 1 {
				active[deviceSlotKey{Group: grp.group, Instance: instance}] = true
			}
		}
	}
	return active, observedAny, fm5NamespaceComplete, fm5Snapshots
}

func (p *vaillantSemanticPoller) readFunctionalModuleIdentitySnapshot(ctx context.Context, instance byte) (*vaillantRadioDeviceSnapshot, bool, bool) {
	connectedRaw, connectedOK := p.readB524Value(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, instance, device_slot_connected)
	if !connectedOK || len(connectedRaw) == 0 {
		return nil, false, false
	}
	classRaw, classOK := p.readB524Value(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, instance, device_slot_class_address)
	firmwareRaw, firmwareOK := p.readB524Value(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, instance, device_slot_firmware)
	hardwareRaw, hardwareOK := p.readB524Value(ctx, vaillantB524OpcodeRead, remoteFunctionalModules.group, instance, device_slot_hardware_identifier)
	tuple := decodeFunctionalModuleIdentityTuple(
		connectedRaw, connectedOK,
		classRaw, classOK,
		firmwareRaw, firmwareOK,
		hardwareRaw, hardwareOK,
	)
	if !tuple.connected && !hasRemoteIdentityEvidence(tuple.classAddress, tuple.firmware, tuple.hardware) {
		return nil, true, tuple.complete
	}
	slotMode := "active"
	if !tuple.connected {
		slotMode = "inventory"
	}
	return &vaillantRadioDeviceSnapshot{
		Group:              remoteFunctionalModules.group,
		Instance:           instance,
		SlotMode:           slotMode,
		DeviceConnected:    cloneBoilerBoolPtr(&tuple.connected),
		DeviceClassAddress: cloneUint8Ptr(tuple.classAddress),
		DeviceModel:        decodeRadioDeviceModel(tuple.classAddress),
		FirmwareVersion:    cloneStringPtr(tuple.firmware),
		HardwareIdentifier: cloneUint16Ptr(tuple.hardware),
	}, true, tuple.complete
}

type functionalModuleIdentityTuple struct {
	connected    bool
	classAddress *uint8
	firmware     *string
	hardware     *uint16
	complete     bool
}

func decodeFunctionalModuleIdentityTuple(
	connectedRaw []byte,
	connectedOK bool,
	classRaw []byte,
	classOK bool,
	firmwareRaw []byte,
	firmwareOK bool,
	hardwareRaw []byte,
	hardwareOK bool,
) functionalModuleIdentityTuple {
	connected, connectedValid := decodeExactB524Bool(connectedRaw)
	tuple := functionalModuleIdentityTuple{
		connected: connected,
		complete: connectedOK && connectedValid &&
			classOK && len(classRaw) >= 1 &&
			firmwareOK && len(firmwareRaw) >= 3 &&
			hardwareOK && len(hardwareRaw) >= 2,
	}
	if classOK && len(classRaw) >= 1 && classRaw[0] != 0xFF {
		value := classRaw[0]
		tuple.classAddress = &value
	}
	if firmwareOK && len(firmwareRaw) >= 3 {
		tuple.firmware = decodeB524FirmwareVersion(firmwareRaw)
	}
	if hardwareOK && len(hardwareRaw) >= 2 {
		if value, ok := decodeB524Uint16(hardwareRaw); ok && value != 0xFFFF {
			tuple.hardware = &value
		}
	}
	return tuple
}

func decodeExactB524Bool(raw []byte) (bool, bool) {
	if len(raw) == 0 {
		return false, false
	}
	switch raw[0] {
	case 0:
		return false, true
	case 1:
		return true, true
	default:
		return false, false
	}
}

func (p *vaillantSemanticPoller) refreshRadioDevices(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	needsDiscovery := !p.deviceSlotDiscoveryDone ||
		(p.deviceSlotRediscoveryTTL > 0 && p.now().Sub(p.deviceSlotDiscoveryAt) >= p.deviceSlotRediscoveryTTL)
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	// Phase 1: Device slot discovery — full scan of all OP=0x06 groups.
	// Runs at startup and every deviceSlotRediscoveryTTL (30min default).
	discoveryObserved := false
	fm5NamespaceComplete := false
	phaseOneFM5Snapshots := make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
	if needsDiscovery {
		activeSlots, observedAny, complete, snapshots := p.discoverDeviceSlotsWithFM5Completeness(ctx)
		discoveryObserved = observedAny
		fm5NamespaceComplete = complete
		phaseOneFM5Snapshots = snapshots
		if observedAny {
			p.mu.Lock()
			p.deviceSlotCache = activeSlots
			p.deviceSlotDiscoveryDone = true
			p.deviceSlotDiscoveryAt = p.now()
			p.mu.Unlock()
		}
	}

	p.mu.Lock()
	slots := p.deviceSlotCache
	p.mu.Unlock()

	if len(slots) == 0 {
		if discoveryObserved {
			fm5ScanComplete := fm5NamespaceComplete
			p.mu.Lock()
			retained := make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
			if !fm5ScanComplete {
				for key, snapshot := range p.radioDevices {
					if key.Group == remoteFunctionalModules.group {
						retained[key] = snapshot
					}
				}
			}
			p.radioDevices = retained
			p.fm5EvidenceGeneration++
			p.fm5IdentityScanComplete = fm5ScanComplete
			p.fm5IdentityIncoherent = !fm5ScanComplete
			if fm5ScanComplete {
				p.fm5RegistryEvidenceIgnored = true
				p.fm5IdentityObservedAt = p.now()
			}
			p.mu.Unlock()
			p.publishRadioDevices(semanticSnapshotSourceLive)
			p.refreshFM5Semantic(ctx)
		}
		return
	}

	// Phase 2: Read detail registers for cached active slots only.
	discovered := make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
	readAny := false
	fm5SlotReadAttempted := false
	fm5DetailIncomplete := false
	for key := range slots {
		group := key.Group
		instance := key.Instance
		opcode := vaillantB524OpcodeRead
		if group == remoteFunctionalModules.group {
			fm5SlotReadAttempted = true
			device, observed, complete := p.readFunctionalModuleIdentitySnapshot(ctx, instance)
			if !observed {
				fm5DetailIncomplete = true
				continue
			}
			readAny = true
			radioKey := radioDeviceKey{Group: group, Instance: instance}
			if !complete {
				fm5DetailIncomplete = true
				if phaseOne := phaseOneFM5Snapshots[radioKey]; phaseOne != nil {
					discovered[radioKey] = phaseOne
				} else if device != nil {
					discovered[radioKey] = device
				}
				continue
			}
			if phaseOne := phaseOneFM5Snapshots[radioKey]; phaseOne != nil && device == nil {
				fm5DetailIncomplete = true
				discovered[radioKey] = phaseOne
				continue
			}
			if device == nil {
				continue
			}
			if device.DeviceConnected != nil && *device.DeviceConnected {
				device.RemoteControlAddress = p.readB524U8(ctx, opcode, group, instance, device_slot_remote_control_address)
				device.DevicePaired = p.readB524Bool(ctx, opcode, group, instance, device_slot_paired)
				device.ReceptionStrength = p.readB524U8(ctx, opcode, group, instance, device_slot_reception_strength)
				device.ZoneAssignment = p.readB524U8(ctx, opcode, group, instance, device_slot_zone_assignment)
				device.RoomTemperatureC = p.readB524F32(ctx, opcode, group, instance, device_slot_room_temperature)
				device.RoomHumidityPct = p.readB524F32(ctx, opcode, group, instance, device_slot_room_humidity)
			}
			discovered[radioKey] = device
			continue
		}

		connectedRaw := p.readB524U8(ctx, opcode, group, instance, device_slot_connected)
		if connectedRaw == nil {
			continue
		}
		readAny = true

		connected := *connectedRaw == 1
		classAddress := p.readB524U8(ctx, opcode, group, instance, device_slot_class_address)
		firmware := p.readB524Firmware(ctx, opcode, group, instance, device_slot_firmware)
		hardware := p.readB524U16(ctx, opcode, group, instance, device_slot_hardware_identifier)

		slotMode := "active"
		include := false
		switch group {
		case remoteRegulators.group, remoteThermostats.group:
			include = connected
		default:
			include = connected
		}
		if !include {
			continue
		}

		device := &vaillantRadioDeviceSnapshot{
			Group:              group,
			Instance:           instance,
			SlotMode:           slotMode,
			DeviceConnected:    cloneBoilerBoolPtr(&connected),
			DeviceClassAddress: cloneUint8Ptr(classAddress),
			DeviceModel:        decodeRadioDeviceModel(classAddress),
			FirmwareVersion:    cloneStringPtr(firmware),
			HardwareIdentifier: cloneUint16Ptr(hardware),
		}
		if connected {
			device.RemoteControlAddress = p.readB524U8(ctx, opcode, group, instance, device_slot_remote_control_address)
			device.DevicePaired = p.readB524Bool(ctx, opcode, group, instance, device_slot_paired)
			device.ReceptionStrength = p.readB524U8(ctx, opcode, group, instance, device_slot_reception_strength)
			device.ZoneAssignment = p.readB524U8(ctx, opcode, group, instance, device_slot_zone_assignment)
			device.RoomTemperatureC = p.readB524F32(ctx, opcode, group, instance, device_slot_room_temperature)
			device.RoomHumidityPct = p.readB524F32(ctx, opcode, group, instance, device_slot_room_humidity)
		}
		discovered[radioDeviceKey{Group: group, Instance: instance}] = device
	}

	if !readAny && len(phaseOneFM5Snapshots) == 0 && !fm5SlotReadAttempted {
		return
	}

	for _, snapshot := range discovered {
		info, ok := radioInventoryRegistryInfo(snapshot)
		if !ok {
			continue
		}
		p.reg.Register(preserveExistingRegistryMetadata(p.reg, info))
	}

	currentFM5Evidence := hasFM5EvidenceFromRadioMap(discovered)
	authoritativeFM5Snapshot := needsDiscovery && discoveryObserved && fm5NamespaceComplete && !fm5DetailIncomplete
	fm5IdentityIncoherent := fm5DetailIncomplete ||
		(needsDiscovery && !authoritativeFM5Snapshot) ||
		(!needsDiscovery && fm5SlotReadAttempted && !currentFM5Evidence)
	p.mu.Lock()
	if !authoritativeFM5Snapshot {
		for key, snapshot := range p.radioDevices {
			if key.Group == remoteFunctionalModules.group && discovered[key] == nil {
				discovered[key] = snapshot
			}
		}
	}
	p.radioDevices = discovered
	p.fm5EvidenceGeneration++
	if needsDiscovery || fm5SlotReadAttempted {
		p.fm5IdentityScanComplete = authoritativeFM5Snapshot
		p.fm5IdentityIncoherent = fm5IdentityIncoherent
		if authoritativeFM5Snapshot {
			p.fm5RegistryEvidenceIgnored = !currentFM5Evidence
			p.fm5IdentityObservedAt = p.now()
		} else if currentFM5Evidence {
			p.fm5RegistryEvidenceIgnored = false
			if !fm5IdentityIncoherent {
				p.fm5IdentityObservedAt = p.now()
			}
		}
	}
	p.mu.Unlock()

	p.publishRadioDevices(semanticSnapshotSourceLive)
	p.refreshFM5Semantic(ctx)
}

func (p *vaillantSemanticPoller) publishRadioDevices(source semanticSnapshotSource) {
	if p == nil || p.provider == nil {
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

	previous := p.provider.RadioDevices()
	if source == semanticSnapshotSourceCache && len(out) == 0 && len(previous) > 0 {
		return
	}
	switch source {
	case semanticSnapshotSourceCache:
		p.provider.SetRadioDevicesFromCache(out)
	default:
		p.provider.SetRadioDevices(out)
	}
	if p.hub != nil && !radioDevicesEqual(previous, out) {
		p.hub.PublishRadioDevicesUpdate(out)
	}
	p.persistSemanticCache(source)
}

func cloneRadioSnapshot(snapshot *vaillantRadioDeviceSnapshot) *vaillantRadioDeviceSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantRadioDeviceSnapshot{
		Group:                snapshot.Group,
		Instance:             snapshot.Instance,
		SlotMode:             snapshot.SlotMode,
		DeviceConnected:      cloneBoilerBoolPtr(snapshot.DeviceConnected),
		DeviceClassAddress:   cloneUint8Ptr(snapshot.DeviceClassAddress),
		DeviceModel:          snapshot.DeviceModel,
		FirmwareVersion:      cloneStringPtr(snapshot.FirmwareVersion),
		HardwareIdentifier:   cloneUint16Ptr(snapshot.HardwareIdentifier),
		RemoteControlAddress: cloneUint8Ptr(snapshot.RemoteControlAddress),
		DevicePaired:         cloneBoilerBoolPtr(snapshot.DevicePaired),
		ReceptionStrength:    cloneUint8Ptr(snapshot.ReceptionStrength),
		ZoneAssignment:       cloneUint8Ptr(snapshot.ZoneAssignment),
		RoomTemperatureC:     cloneFloat64Ptr(snapshot.RoomTemperatureC),
		RoomHumidityPct:      cloneFloat64Ptr(snapshot.RoomHumidityPct),
	}
}

func (p *vaillantSemanticPoller) publishSystem(source semanticSnapshotSource) {
	if p == nil || p.provider == nil {
		return
	}

	p.mu.Lock()
	snapshot := cloneSystemSnapshot(p.system)
	p.mu.Unlock()

	previous := p.provider.System()
	if snapshot == nil {
		switch source {
		case semanticSnapshotSourceCache:
			p.provider.SetSystemFromCache(nil)
		default:
			p.provider.SetSystem(nil)
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

	switch source {
	case semanticSnapshotSourceCache:
		p.provider.SetSystemFromCache(current)
	default:
		p.provider.SetSystem(current)
	}

	if p.hub != nil && !systemStatusEquals(previous, current) {
		publisher := reflect.ValueOf(p.hub).MethodByName("PublishSystemUpdate")
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

func (p *vaillantSemanticPoller) refreshFM5Semantic(ctx context.Context) {
	if p == nil || p.provider == nil {
		return
	}

	evidence := p.captureFM5Evidence()
	fm5GateSatisfied := evidence.moduleConfig != nil && *evidence.moduleConfig <= 2
	observedAt := p.now()
	evidenceStale := evidence.staleAt(observedAt, p.fm5EvidenceTTL)

	var incomingSolar *vaillantSolarSnapshot
	incomingCylinders := make(map[byte]*vaillantCylinderSnapshot)
	solarReadable := false
	cylindersReadable := false
	if evidence.controller != 0 && fm5GateSatisfied && !evidenceStale && !evidence.identityIncoherent {
		incomingSolar, solarReadable = p.readSolarSnapshot(ctx)
		incomingCylinders, cylindersReadable = p.readCylinderSnapshots(ctx)
	}

	currentEvidence := p.captureFM5Evidence()
	incoherent := evidence.identityIncoherent || currentEvidence.identityIncoherent || !evidence.sameGeneration(currentEvidence)
	evidenceRevision := p.nextFM5EvidenceRevision(evidence.generation, currentEvidence.generation)
	negativeIdentityObserved := evidence.hasNegativeObservation() && currentEvidence.hasNegativeObservation()
	freshNegativeIdentityObserved := evidence.hasFreshNegativeObservation(observedAt, p.fm5EvidenceTTL) &&
		currentEvidence.hasFreshNegativeObservation(observedAt, p.fm5EvidenceTTL)
	var verdict graphql.Fm5Interpretation
	if freshNegativeIdentityObserved && !incoherent && evidence.controller != 0 && evidence.moduleConfig != nil {
		verdict = graphql.Fm5Interpretation{
			Mode:             graphql.Fm5SemanticModeAbsent,
			EvidenceRevision: evidenceRevision,
		}
	} else {
		verdict = deriveFM5Interpretation(
			evidence.controller != 0,
			evidence.moduleConfig,
			solarReadable,
			cylindersReadable,
			evidence.hasEvidence() || currentEvidence.hasEvidence() || negativeIdentityObserved,
			evidenceStale,
			incoherent,
			evidenceRevision,
		)
	}
	verdict = p.commitFM5Acquisition(currentEvidence, verdict, incomingSolar, incomingCylinders)
	for _, info := range fm5InventoryRegistryInfos(evidence.systemSnapshot, verdict.Mode) {
		p.reg.Register(preserveExistingRegistryMetadata(p.reg, info))
	}

	p.publishFM5Semantic(semanticSnapshotSourceLive)
}

func deriveFM5SemanticMode(controllerReachable, fm5GateSatisfied, solarReadable, cylindersReadable, hasEvidence bool) graphql.Fm5SemanticMode {
	if controllerReachable && fm5GateSatisfied && solarReadable && cylindersReadable {
		return graphql.Fm5SemanticModeInterpreted
	}
	if hasEvidence {
		return graphql.Fm5SemanticModeGPIOOnly
	}
	return graphql.Fm5SemanticModeAbsent
}

type fm5EvidenceCapture struct {
	controller                  byte
	moduleConfig                *uint16
	systemSnapshot              *vaillantSystemSnapshot
	radioSnapshots              []*vaillantRadioDeviceSnapshot
	registryEvidence            string
	registryGeneration          uint64
	registryCoherent            bool
	registryEvidenceIgnored     bool
	identityAcquisitionComplete bool
	identityIncoherent          bool
	identityObservedAt          time.Time
	generation                  uint64
}

func (p *vaillantSemanticPoller) captureFM5Evidence() fm5EvidenceCapture {
	if p == nil {
		return fm5EvidenceCapture{}
	}
	p.mu.Lock()
	capture := fm5EvidenceCapture{
		controller:                  p.controller,
		systemSnapshot:              cloneSystemSnapshot(p.system),
		registryEvidenceIgnored:     p.fm5RegistryEvidenceIgnored,
		identityAcquisitionComplete: p.fm5IdentityScanComplete,
		identityIncoherent:          p.fm5IdentityIncoherent,
		identityObservedAt:          p.fm5IdentityObservedAt,
		generation:                  p.fm5EvidenceGeneration,
		radioSnapshots:              make([]*vaillantRadioDeviceSnapshot, 0, len(p.radioDevices)),
	}
	if p.system != nil {
		capture.moduleConfig = cloneUint16Ptr(p.system.ModuleConfigurationVR71)
	}
	for _, snapshot := range p.radioDevices {
		if snapshot != nil {
			capture.radioSnapshots = append(capture.radioSnapshots, cloneRadioSnapshot(snapshot))
		}
	}
	p.mu.Unlock()

	capture.registryEvidence, capture.registryGeneration, capture.registryCoherent = p.captureFM5RegistryEvidence()
	if capture.hasEvidence() && capture.identityObservedAt.IsZero() {
		observedAt := p.now()
		p.mu.Lock()
		if p.fm5IdentityObservedAt.IsZero() {
			p.fm5IdentityObservedAt = observedAt
		}
		capture.identityObservedAt = p.fm5IdentityObservedAt
		capture.generation = p.fm5EvidenceGeneration
		p.mu.Unlock()
	}
	return capture
}

func (capture fm5EvidenceCapture) hasEvidence() bool {
	if hasFM5EvidenceFromRadioSnapshots(capture.radioSnapshots) {
		return true
	}
	return !capture.registryEvidenceIgnored && capture.registryEvidence != ""
}

func (capture fm5EvidenceCapture) staleAt(now time.Time, ttl time.Duration) bool {
	if (!capture.hasEvidence() && !capture.identityAcquisitionComplete) || capture.identityObservedAt.IsZero() || ttl <= 0 {
		return false
	}
	return now.Sub(capture.identityObservedAt) > ttl
}

func (capture fm5EvidenceCapture) hasFreshNegativeObservation(now time.Time, ttl time.Duration) bool {
	return capture.hasNegativeObservation() && !capture.staleAt(now, ttl)
}

func (capture fm5EvidenceCapture) hasNegativeObservation() bool {
	return !capture.hasEvidence() && capture.identityAcquisitionComplete && !capture.identityObservedAt.IsZero()
}

func (capture fm5EvidenceCapture) sameGeneration(other fm5EvidenceCapture) bool {
	return capture.registryCoherent && other.registryCoherent &&
		capture.generation == other.generation &&
		capture.controller == other.controller &&
		uint16PointersEqual(capture.moduleConfig, other.moduleConfig) &&
		capture.registryEvidenceIgnored == other.registryEvidenceIgnored &&
		capture.identityAcquisitionComplete == other.identityAcquisitionComplete &&
		capture.identityIncoherent == other.identityIncoherent &&
		capture.identityObservedAt.Equal(other.identityObservedAt) &&
		capture.registryGeneration == other.registryGeneration &&
		capture.registryEvidence == other.registryEvidence
}

func (capture fm5EvidenceCapture) matchesLockedPoller(p *vaillantSemanticPoller) bool {
	if p == nil {
		return false
	}
	var moduleConfig *uint16
	if p.system != nil {
		moduleConfig = p.system.ModuleConfigurationVR71
	}
	return capture.generation == p.fm5EvidenceGeneration &&
		capture.controller == p.controller &&
		uint16PointersEqual(capture.moduleConfig, moduleConfig) &&
		capture.registryEvidenceIgnored == p.fm5RegistryEvidenceIgnored &&
		capture.identityAcquisitionComplete == p.fm5IdentityScanComplete &&
		capture.identityIncoherent == p.fm5IdentityIncoherent &&
		capture.identityObservedAt.Equal(p.fm5IdentityObservedAt)
}

func (p *vaillantSemanticPoller) commitFM5Acquisition(
	captured fm5EvidenceCapture,
	verdict graphql.Fm5Interpretation,
	incomingSolar *vaillantSolarSnapshot,
	incomingCylinders map[byte]*vaillantCylinderSnapshot,
) graphql.Fm5Interpretation {
	commit := func(registryGeneration uint64, registryCoherent bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if !registryCoherent || !captured.registryCoherent ||
			captured.registryGeneration != registryGeneration ||
			!captured.matchesLockedPoller(p) {
			p.fm5EvidenceRevision++
			verdict = graphql.Fm5Interpretation{
				Mode:             graphql.Fm5SemanticModeGPIOOnly,
				DegradedReason:   graphql.Fm5SemanticDegradedReasonIncoherentAcquisition,
				EvidenceRevision: formatFM5EvidenceRevision(captured.generation, p.fm5EvidenceGeneration, p.fm5EvidenceRevision),
			}
		}
		verdict = retainFM5StructuralMode(p.fm5Interpretation, verdict)
		if verdict.Mode != "" {
			p.fm5Mode = verdict.Mode
		}
		p.fm5Interpretation = verdict
		p.solar, p.solarCylinders = applyFM5Acquisition(
			p.solar, p.solarCylinders, incomingSolar, incomingCylinders, verdict,
		)
	}

	withGeneration := p.withFM5ObservationGeneration
	if withGeneration == nil && p.reg != nil {
		withGeneration = p.reg.WithObservationGeneration
	}
	if withGeneration == nil {
		commit(0, true)
		return verdict
	}
	committed := false
	if withGeneration(func(current uint64) {
		// Do not call any DeviceRegistry method from this callback. The
		// registry read lock is deliberately held across the generation
		// comparison and semantic commit.
		commit(current, true)
		committed = true
	}) && committed {
		return verdict
	}
	commit(captured.registryGeneration, false)
	return verdict
}

func (p *vaillantSemanticPoller) updateSystemSnapshotLocked(snapshot *vaillantSystemSnapshot) {
	var previousConfig *uint16
	if p.system != nil {
		previousConfig = p.system.ModuleConfigurationVR71
	}
	var nextConfig *uint16
	if snapshot != nil {
		nextConfig = snapshot.ModuleConfigurationVR71
	}
	p.system = snapshot
	if !uint16PointersEqual(previousConfig, nextConfig) {
		p.fm5EvidenceGeneration++
	}
}

func uint16PointersEqual(left, right *uint16) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func deriveFM5Interpretation(
	controllerReachable bool,
	moduleConfig *uint16,
	solarReadable bool,
	cylindersReadable bool,
	hasEvidence bool,
	evidenceStale bool,
	incoherent bool,
	evidenceRevision string,
) graphql.Fm5Interpretation {
	verdict := graphql.Fm5Interpretation{EvidenceRevision: evidenceRevision}
	verdict.Mode = graphql.Fm5SemanticModeGPIOOnly
	switch {
	case incoherent:
		verdict.DegradedReason = graphql.Fm5SemanticDegradedReasonIncoherentAcquisition
	case !hasEvidence:
		return graphql.Fm5Interpretation{}
	case !controllerReachable:
		verdict.DegradedReason = graphql.Fm5SemanticDegradedReasonControllerUnreachable
	case moduleConfig == nil:
		verdict.DegradedReason = graphql.Fm5SemanticDegradedReasonConfigurationUnavailable
	case evidenceStale:
		verdict.DegradedReason = graphql.Fm5SemanticDegradedReasonEvidenceStale
	case *moduleConfig > 2:
		verdict.DegradedReason = graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable
	case !solarReadable:
		verdict.DegradedReason = graphql.Fm5SemanticDegradedReasonSolarAcquisitionFailed
	case !cylindersReadable:
		verdict.DegradedReason = graphql.Fm5SemanticDegradedReasonCylinderAcquisitionFailed
	default:
		verdict.Mode = graphql.Fm5SemanticModeInterpreted
	}
	return verdict
}

func retainFM5StructuralMode(previous, candidate graphql.Fm5Interpretation) graphql.Fm5Interpretation {
	if !isFM5TransientAcquisitionReason(candidate.DegradedReason) {
		return candidate
	}
	if previous.Validate() != nil {
		return graphql.Fm5Interpretation{}
	}
	switch previous.Mode {
	case graphql.Fm5SemanticModeInterpreted, graphql.Fm5SemanticModeAbsent:
		candidate.Mode = previous.Mode
		return candidate
	case graphql.Fm5SemanticModeGPIOOnly:
		previous.EvidenceRevision = candidate.EvidenceRevision
		return previous
	default:
		return graphql.Fm5Interpretation{}
	}
}

func isFM5TransientAcquisitionReason(reason graphql.Fm5SemanticDegradedReason) bool {
	switch reason {
	case graphql.Fm5SemanticDegradedReasonControllerUnreachable,
		graphql.Fm5SemanticDegradedReasonConfigurationUnavailable,
		graphql.Fm5SemanticDegradedReasonSolarAcquisitionFailed,
		graphql.Fm5SemanticDegradedReasonCylinderAcquisitionFailed,
		graphql.Fm5SemanticDegradedReasonEvidenceStale,
		graphql.Fm5SemanticDegradedReasonIncoherentAcquisition:
		return true
	default:
		return false
	}
}

func applyFM5Acquisition(
	previousSolar *vaillantSolarSnapshot,
	previousCylinders map[byte]*vaillantCylinderSnapshot,
	incomingSolar *vaillantSolarSnapshot,
	incomingCylinders map[byte]*vaillantCylinderSnapshot,
	verdict graphql.Fm5Interpretation,
) (*vaillantSolarSnapshot, map[byte]*vaillantCylinderSnapshot) {
	switch {
	case isFM5TransientAcquisitionReason(verdict.DegradedReason):
		return cloneSolarSnapshot(previousSolar), cloneCylinderSnapshotsMap(previousCylinders)
	case verdict.Mode == graphql.Fm5SemanticModeInterpreted:
		return mergeSolarSnapshotNonDestructive(previousSolar, incomingSolar),
			mergeCylinderSnapshotMapNonDestructive(previousCylinders, incomingCylinders)
	case isFM5StructuralWithdrawal(verdict):
		return nil, make(map[byte]*vaillantCylinderSnapshot)
	default:
		return cloneSolarSnapshot(previousSolar), cloneCylinderSnapshotsMap(previousCylinders)
	}
}

func isFM5StructuralWithdrawal(verdict graphql.Fm5Interpretation) bool {
	return (verdict.Mode == graphql.Fm5SemanticModeAbsent && verdict.DegradedReason == "") ||
		(verdict.Mode == graphql.Fm5SemanticModeGPIOOnly &&
			verdict.DegradedReason == graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable)
}

func (p *vaillantSemanticPoller) nextFM5EvidenceRevision(startGeneration, endGeneration uint64) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fm5EvidenceRevision++
	return formatFM5EvidenceRevision(startGeneration, endGeneration, p.fm5EvidenceRevision)
}

func formatFM5EvidenceRevision(startGeneration, endGeneration, acquisition uint64) string {
	if startGeneration != endGeneration {
		return fmt.Sprintf("fm5-g%d-%d-a%d", startGeneration, endGeneration, acquisition)
	}
	return fmt.Sprintf("fm5-g%d-a%d", endGeneration, acquisition)
}

func (p *vaillantSemanticPoller) publishFM5Semantic(source semanticSnapshotSource) {
	if p == nil || p.provider == nil {
		return
	}

	p.mu.Lock()
	verdict := p.fm5Interpretation
	solarSnapshot := cloneSolarSnapshot(p.solar)
	cylindersSnapshot := cloneCylinderSnapshotsMap(p.solarCylinders)
	p.mu.Unlock()

	switch source {
	case semanticSnapshotSourceCache:
		p.provider.SetFM5InterpretationFromCache(verdict)
	default:
		p.provider.SetFM5Interpretation(verdict)
	}

	if isFM5StructuralWithdrawal(verdict) || (solarSnapshot == nil && len(cylindersSnapshot) == 0) {
		emptySolar := &graphql.SolarStatus{}
		emptyCylinders := []graphql.CylinderStatus{}
		switch source {
		case semanticSnapshotSourceCache:
			p.provider.SetSolarFromCache(emptySolar)
			p.provider.SetCylindersFromCache(emptyCylinders)
		default:
			p.provider.SetSolar(emptySolar)
			p.provider.SetCylinders(emptyCylinders)
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
		p.provider.SetSolarFromCache(solar)
		p.provider.SetCylindersFromCache(cylinders)
	default:
		p.provider.SetSolar(solar)
		p.provider.SetCylinders(cylinders)
	}

	p.publishCircuits(source)
}

func hasLiveCylinderEvidence(snapshot *vaillantCylinderSnapshot) bool {
	return snapshot != nil && snapshot.TemperatureC != nil
}

func (p *vaillantSemanticPoller) readSolarSnapshot(ctx context.Context) (*vaillantSolarSnapshot, bool) {
	if p == nil {
		return nil, false
	}
	incoming := &vaillantSolarSnapshot{}
	readAny := false

	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_enabled); ok {
		readAny = true
		incoming.SolarEnabled = decodeB524BoolFromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_function_mode); ok {
		readAny = true
		incoming.FunctionMode = decodeB524BoolFromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_collector_temp); ok {
		readAny = true
		incoming.CollectorTemperatureC = decodeB524Float32FromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_return_temp); ok {
		readAny = true
		incoming.ReturnTemperatureC = decodeB524Float32FromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_pump_active); ok {
		readAny = true
		incoming.PumpActive = decodeB524BoolFromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_current_yield); ok {
		readAny = true
		incoming.CurrentYield = decodeB524Float32FromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, localSolar.opcode, localSolar.group, solarInstance, solar_pump_hours); ok {
		readAny = true
		incoming.PumpHours = decodeB524Uint32FromRaw(raw)
	}

	if !readAny {
		return nil, false
	}
	return incoming, true
}

func (p *vaillantSemanticPoller) readCylinderSnapshots(ctx context.Context) (map[byte]*vaillantCylinderSnapshot, bool) {
	if p == nil {
		return nil, false
	}
	out := make(map[byte]*vaillantCylinderSnapshot, 2)
	readAny := false
	for instance := byte(0x00); instance <= 0x01; instance++ {
		incoming := &vaillantCylinderSnapshot{Instance: instance}
		instanceRead := false

		if raw, ok := p.readB524Value(ctx, localCylinders.opcode, localCylinders.group, instance, cylinder_max_setpoint); ok {
			instanceRead = true
			readAny = true
			incoming.MaxSetpointC = decodeB524Float32FromRaw(raw)
		}
		if raw, ok := p.readB524Value(ctx, localCylinders.opcode, localCylinders.group, instance, cylinder_charge_hysteresis); ok {
			instanceRead = true
			readAny = true
			incoming.ChargeHysteresis = decodeB524Float32FromRaw(raw)
		}
		if raw, ok := p.readB524Value(ctx, localCylinders.opcode, localCylinders.group, instance, cylinder_charge_offset); ok {
			instanceRead = true
			readAny = true
			incoming.ChargeOffset = decodeB524Float32FromRaw(raw)
		}
		if raw, ok := p.readB524Value(ctx, localCylinders.opcode, localCylinders.group, instance, cylinder_temperature); ok {
			instanceRead = true
			readAny = true
			incoming.TemperatureC = decodeB524Float32FromRaw(raw)
		}
		if instanceRead {
			out[instance] = incoming
		}
	}
	if !readAny {
		return nil, false
	}
	return out, true
}

func hasFM5EvidenceFromRadioSnapshots(snapshots []*vaillantRadioDeviceSnapshot) bool {
	for _, snapshot := range snapshots {
		if snapshot == nil {
			continue
		}
		if hasRemoteIdentityEvidence(snapshot.DeviceClassAddress, snapshot.FirmwareVersion, snapshot.HardwareIdentifier) {
			return true
		}
		if snapshot.DeviceClassAddress != nil && *snapshot.DeviceClassAddress == 0x26 {
			return true
		}
		if strings.EqualFold(strings.TrimSpace(snapshot.DeviceModel), "VR71/FM5") {
			return true
		}
	}
	return false
}

func hasFM5EvidenceFromRadioMap(snapshots map[radioDeviceKey]*vaillantRadioDeviceSnapshot) bool {
	for _, snapshot := range snapshots {
		if hasFM5EvidenceFromRadioSnapshots([]*vaillantRadioDeviceSnapshot{snapshot}) {
			return true
		}
	}
	return false
}

func radioInventoryRegistryInfo(snapshot *vaillantRadioDeviceSnapshot) (registry.DeviceInfo, bool) {
	if snapshot == nil || snapshot.DeviceClassAddress == nil {
		return registry.DeviceInfo{}, false
	}
	switch *snapshot.DeviceClassAddress {
	case circuitManagingDeviceVR71Address:
		return registry.DeviceInfo{
			Address:      circuitManagingDeviceVR71Address,
			Manufacturer: "Vaillant",
			DeviceID:     circuitManagingDeviceVR71ID,
		}, true
	default:
		return registry.DeviceInfo{}, false
	}
}

func preserveExistingRegistryMetadata(reg *registry.DeviceRegistry, info registry.DeviceInfo) registry.DeviceInfo {
	if reg == nil {
		return info
	}
	// P9.3 — race-free identity-field reads via IterateSnapshots. The
	// previous Iterate path read entry.Manufacturer / DeviceID /
	// SerialNumber / etc. lock-free outside the registry RLock; under
	// concurrent Register writes those string reads could tear.
	reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if snap.PrimaryAddress != info.Address {
			return true
		}
		if info.Manufacturer == "" {
			info.Manufacturer = snap.Manufacturer
		}
		if info.DeviceID == "" {
			info.DeviceID = snap.DeviceID
		}
		if info.SerialNumber == "" {
			info.SerialNumber = snap.SerialNumber
		}
		if info.MacAddress == "" {
			info.MacAddress = snap.MacAddress
		}
		if info.SoftwareVersion == "" {
			info.SoftwareVersion = snap.SoftwareVersion
		}
		if info.HardwareVersion == "" {
			info.HardwareVersion = snap.HardwareVersion
		}
		return false
	})
	return info
}

func (p *vaillantSemanticPoller) hasFM5RegistryEvidence() bool {
	return p.fm5RegistryEvidenceFingerprint() != ""
}

func (p *vaillantSemanticPoller) captureFM5RegistryEvidence() (string, uint64, bool) {
	if p == nil || p.reg == nil {
		return "", 0, true
	}
	for range 3 {
		var before uint64
		if !p.reg.WithObservationGeneration(func(current uint64) { before = current }) {
			return "", 0, false
		}
		fingerprint := p.fm5RegistryEvidenceFingerprint()
		var after uint64
		if !p.reg.WithObservationGeneration(func(current uint64) { after = current }) {
			return fingerprint, before, false
		}
		if before == after {
			return fingerprint, after, true
		}
	}
	return p.fm5RegistryEvidenceFingerprint(), 0, false
}

func (p *vaillantSemanticPoller) fm5RegistryEvidenceFingerprint() string {
	if p == nil || p.reg == nil {
		return ""
	}
	// P9.3 — race-free DeviceID read via snapshot.
	identities := make([]string, 0, 1)
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		deviceID := normalizeDeviceID(snap.DeviceID)
		if strings.HasPrefix(deviceID, "VR71") || strings.HasPrefix(deviceID, "FM5") {
			identities = append(identities, fmt.Sprintf("%02x:%s", snap.PrimaryAddress, deviceID))
		}
		return true
	})
	slices.Sort(identities)
	return strings.Join(identities, "|")
}

func mergeSolarSnapshotNonDestructive(existing, incoming *vaillantSolarSnapshot) *vaillantSolarSnapshot {
	merged := cloneSolarSnapshot(existing)
	if merged == nil {
		merged = &vaillantSolarSnapshot{}
	}
	if incoming == nil {
		return merged
	}
	if incoming.CollectorTemperatureC != nil {
		merged.CollectorTemperatureC = cloneFloat64Ptr(incoming.CollectorTemperatureC)
	}
	if incoming.ReturnTemperatureC != nil {
		merged.ReturnTemperatureC = cloneFloat64Ptr(incoming.ReturnTemperatureC)
	}
	if incoming.PumpActive != nil {
		merged.PumpActive = cloneBoilerBoolPtr(incoming.PumpActive)
	}
	if incoming.CurrentYield != nil {
		merged.CurrentYield = cloneFloat64Ptr(incoming.CurrentYield)
	}
	if incoming.PumpHours != nil {
		merged.PumpHours = cloneUint32Ptr(incoming.PumpHours)
	}
	if incoming.SolarEnabled != nil {
		merged.SolarEnabled = cloneBoilerBoolPtr(incoming.SolarEnabled)
	}
	if incoming.FunctionMode != nil {
		merged.FunctionMode = cloneBoilerBoolPtr(incoming.FunctionMode)
	}
	return merged
}

func cloneSolarSnapshot(snapshot *vaillantSolarSnapshot) *vaillantSolarSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantSolarSnapshot{
		CollectorTemperatureC: cloneFloat64Ptr(snapshot.CollectorTemperatureC),
		ReturnTemperatureC:    cloneFloat64Ptr(snapshot.ReturnTemperatureC),
		PumpActive:            cloneBoilerBoolPtr(snapshot.PumpActive),
		CurrentYield:          cloneFloat64Ptr(snapshot.CurrentYield),
		PumpHours:             cloneUint32Ptr(snapshot.PumpHours),
		SolarEnabled:          cloneBoilerBoolPtr(snapshot.SolarEnabled),
		FunctionMode:          cloneBoilerBoolPtr(snapshot.FunctionMode),
	}
}

func mergeCylinderSnapshotNonDestructive(existing, incoming *vaillantCylinderSnapshot) *vaillantCylinderSnapshot {
	merged := cloneCylinderSnapshot(existing)
	if merged == nil {
		merged = &vaillantCylinderSnapshot{}
	}
	if incoming == nil {
		return merged
	}
	merged.Instance = incoming.Instance
	if incoming.TemperatureC != nil {
		merged.TemperatureC = cloneFloat64Ptr(incoming.TemperatureC)
	}
	if incoming.MaxSetpointC != nil {
		merged.MaxSetpointC = cloneFloat64Ptr(incoming.MaxSetpointC)
	}
	if incoming.ChargeHysteresis != nil {
		merged.ChargeHysteresis = cloneFloat64Ptr(incoming.ChargeHysteresis)
	}
	if incoming.ChargeOffset != nil {
		merged.ChargeOffset = cloneFloat64Ptr(incoming.ChargeOffset)
	}
	return merged
}

func mergeCylinderSnapshotMapNonDestructive(existing, incoming map[byte]*vaillantCylinderSnapshot) map[byte]*vaillantCylinderSnapshot {
	merged := cloneCylinderSnapshotsMap(existing)
	if merged == nil {
		merged = make(map[byte]*vaillantCylinderSnapshot)
	}
	for instance, snapshot := range incoming {
		merged[instance] = mergeCylinderSnapshotNonDestructive(merged[instance], snapshot)
	}
	return merged
}

func cloneCylinderSnapshot(snapshot *vaillantCylinderSnapshot) *vaillantCylinderSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantCylinderSnapshot{
		Instance:         snapshot.Instance,
		TemperatureC:     cloneFloat64Ptr(snapshot.TemperatureC),
		MaxSetpointC:     cloneFloat64Ptr(snapshot.MaxSetpointC),
		ChargeHysteresis: cloneFloat64Ptr(snapshot.ChargeHysteresis),
		ChargeOffset:     cloneFloat64Ptr(snapshot.ChargeOffset),
	}
}

func cloneCylinderSnapshotsMap(source map[byte]*vaillantCylinderSnapshot) map[byte]*vaillantCylinderSnapshot {
	if len(source) == 0 {
		return nil
	}
	out := make(map[byte]*vaillantCylinderSnapshot, len(source))
	for instance, snapshot := range source {
		out[instance] = cloneCylinderSnapshot(snapshot)
	}
	return out
}

func decodeB524BoolFromRaw(raw []byte) *bool {
	if len(raw) == 0 || raw[0] == 0xFF || raw[0] > 1 {
		return nil
	}
	value := raw[0] == 1
	return &value
}

func decodeB524Float32FromRaw(raw []byte) *float64 {
	if len(raw) < 4 {
		return nil
	}
	bits := binary.LittleEndian.Uint32(raw[:4])
	value := float64(math.Float32frombits(bits))
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	return &value
}

func decodeB524Uint32FromRaw(raw []byte) *uint32 {
	if len(raw) < 4 {
		return nil
	}
	value := binary.LittleEndian.Uint32(raw[:4])
	if value == 0xFFFFFFFF {
		return nil
	}
	return &value
}

func mergeSystemSnapshotNonDestructive(existing, incoming *vaillantSystemSnapshot) *vaillantSystemSnapshot {
	merged := cloneSystemSnapshot(existing)
	if merged == nil {
		merged = &vaillantSystemSnapshot{}
	}
	if incoming == nil {
		return merged
	}

	controllerChanged := incoming.Controller != 0 && incoming.Controller != merged.Controller
	if controllerChanged && incoming.SystemFlowTemperature == nil {
		merged.SystemFlowTemperature = nil
		merged.SystemFlowTemperatureLiveAt = time.Time{}
	}
	if incoming.Controller != 0 {
		merged.Controller = incoming.Controller
	}
	if incoming.SystemOff != nil {
		merged.SystemOff = cloneBoilerBoolPtr(incoming.SystemOff)
	}
	if incoming.SystemWaterPressure != nil {
		merged.SystemWaterPressure = cloneFloat64Ptr(incoming.SystemWaterPressure)
	}
	if incoming.SystemFlowTemperature != nil {
		merged.SystemFlowTemperature = cloneFloat64Ptr(incoming.SystemFlowTemperature)
		merged.SystemFlowTemperatureLiveAt = incoming.SystemFlowTemperatureLiveAt
	}
	if incoming.OutdoorTemperature != nil {
		merged.OutdoorTemperature = cloneFloat64Ptr(incoming.OutdoorTemperature)
	}
	if incoming.OutdoorTemperatureAvg24h != nil {
		merged.OutdoorTemperatureAvg24h = cloneFloat64Ptr(incoming.OutdoorTemperatureAvg24h)
	}
	if incoming.MaintenanceDue != nil {
		merged.MaintenanceDue = cloneBoilerBoolPtr(incoming.MaintenanceDue)
	}
	if incoming.HwcCylinderTemperatureTop != nil {
		merged.HwcCylinderTemperatureTop = cloneFloat64Ptr(incoming.HwcCylinderTemperatureTop)
	}
	if incoming.HwcCylinderTemperatureBottom != nil {
		merged.HwcCylinderTemperatureBottom = cloneFloat64Ptr(incoming.HwcCylinderTemperatureBottom)
	}
	if incoming.AdaptiveHeatingCurve != nil {
		merged.AdaptiveHeatingCurve = cloneBoilerBoolPtr(incoming.AdaptiveHeatingCurve)
	}
	if incoming.AlternativePoint != nil {
		merged.AlternativePoint = cloneFloat64Ptr(incoming.AlternativePoint)
	}
	if incoming.HeatingCircuitBivalencePoint != nil {
		merged.HeatingCircuitBivalencePoint = cloneFloat64Ptr(incoming.HeatingCircuitBivalencePoint)
	}
	if incoming.DhwBivalencePoint != nil {
		merged.DhwBivalencePoint = cloneFloat64Ptr(incoming.DhwBivalencePoint)
	}
	if incoming.HcEmergencyTemperature != nil {
		merged.HcEmergencyTemperature = cloneFloat64Ptr(incoming.HcEmergencyTemperature)
	}
	if incoming.HwcMaxFlowTempDesired != nil {
		merged.HwcMaxFlowTempDesired = cloneFloat64Ptr(incoming.HwcMaxFlowTempDesired)
	}
	if incoming.MaxRoomHumidity != nil {
		merged.MaxRoomHumidity = cloneUint16Ptr(incoming.MaxRoomHumidity)
	}
	if incoming.MaintenanceDate != nil {
		v := *incoming.MaintenanceDate
		merged.MaintenanceDate = &v
	}
	if incoming.InstallerName != nil {
		v := *incoming.InstallerName
		merged.InstallerName = &v
	}
	if incoming.InstallerPhone != nil {
		v := *incoming.InstallerPhone
		merged.InstallerPhone = &v
	}
	if incoming.InstallerMenuCode != nil {
		merged.InstallerMenuCode = cloneUint16Ptr(incoming.InstallerMenuCode)
	}
	if incoming.SystemScheme != nil {
		merged.SystemScheme = cloneUint16Ptr(incoming.SystemScheme)
	}
	if incoming.ModuleConfigurationVR71 != nil {
		merged.ModuleConfigurationVR71 = cloneUint16Ptr(incoming.ModuleConfigurationVR71)
	}
	return merged
}

func cloneSystemSnapshot(snapshot *vaillantSystemSnapshot) *vaillantSystemSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantSystemSnapshot{
		Controller:                   snapshot.Controller,
		SystemOff:                    cloneBoilerBoolPtr(snapshot.SystemOff),
		SystemWaterPressure:          cloneFloat64Ptr(snapshot.SystemWaterPressure),
		SystemFlowTemperature:        cloneFloat64Ptr(snapshot.SystemFlowTemperature),
		SystemFlowTemperatureLiveAt:  snapshot.SystemFlowTemperatureLiveAt,
		OutdoorTemperature:           cloneFloat64Ptr(snapshot.OutdoorTemperature),
		OutdoorTemperatureAvg24h:     cloneFloat64Ptr(snapshot.OutdoorTemperatureAvg24h),
		MaintenanceDue:               cloneBoilerBoolPtr(snapshot.MaintenanceDue),
		HwcCylinderTemperatureTop:    cloneFloat64Ptr(snapshot.HwcCylinderTemperatureTop),
		HwcCylinderTemperatureBottom: cloneFloat64Ptr(snapshot.HwcCylinderTemperatureBottom),
		AdaptiveHeatingCurve:         cloneBoilerBoolPtr(snapshot.AdaptiveHeatingCurve),
		AlternativePoint:             cloneFloat64Ptr(snapshot.AlternativePoint),
		HeatingCircuitBivalencePoint: cloneFloat64Ptr(snapshot.HeatingCircuitBivalencePoint),
		DhwBivalencePoint:            cloneFloat64Ptr(snapshot.DhwBivalencePoint),
		HcEmergencyTemperature:       cloneFloat64Ptr(snapshot.HcEmergencyTemperature),
		HwcMaxFlowTempDesired:        cloneFloat64Ptr(snapshot.HwcMaxFlowTempDesired),
		MaxRoomHumidity:              cloneUint16Ptr(snapshot.MaxRoomHumidity),
		MaintenanceDate:              cloneStringPtr(snapshot.MaintenanceDate),
		InstallerName:                cloneStringPtr(snapshot.InstallerName),
		InstallerPhone:               cloneStringPtr(snapshot.InstallerPhone),
		InstallerMenuCode:            cloneUint16Ptr(snapshot.InstallerMenuCode),
		SystemScheme:                 cloneUint16Ptr(snapshot.SystemScheme),
		ModuleConfigurationVR71:      cloneUint16Ptr(snapshot.ModuleConfigurationVR71),
	}
}

func uint16ToIntPtr(value *uint16) *int {
	if value == nil {
		return nil
	}
	v := int(*value)
	return &v
}

func uint8ToIntPtr(value *uint8) *int {
	if value == nil {
		return nil
	}
	v := int(*value)
	return &v
}

const (
	circuitManagingDeviceVR71ID      = "VR_71"
	circuitManagingDeviceVR71Address = 0x26
)

func fm5InventoryRegistryInfos(system *vaillantSystemSnapshot, fm5Mode graphql.Fm5SemanticMode) []registry.DeviceInfo {
	if system != nil &&
		system.SystemScheme != nil && *system.SystemScheme == 1 &&
		system.ModuleConfigurationVR71 != nil && *system.ModuleConfigurationVR71 == 2 &&
		fm5Mode == graphql.Fm5SemanticModeInterpreted {
		return []registry.DeviceInfo{{
			Address:      circuitManagingDeviceVR71Address,
			Manufacturer: "Vaillant",
			DeviceID:     circuitManagingDeviceVR71ID,
		}}
	}
	return nil
}

func deriveCircuitManagingDevice(system *vaillantSystemSnapshot, fm5Mode graphql.Fm5SemanticMode) graphql.ManagingDevice {
	if system != nil &&
		system.SystemScheme != nil && *system.SystemScheme == 1 &&
		system.ModuleConfigurationVR71 != nil && *system.ModuleConfigurationVR71 == 2 &&
		fm5Mode == graphql.Fm5SemanticModeInterpreted {
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

func decodeDhwMode(raw int) string {
	switch raw {
	case 0:
		return "OFF"
	case 1:
		return "ON"
	case 2:
		return "AUTO"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", raw)
	}
}

func boilerStatusEquals(a, b *graphql.BoilerStatus) bool {
	return reflect.DeepEqual(a, b)
}

func boolPtrEquals(a, b *bool) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func stringPtrEquals(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func intPtrEquals(a, b *int) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func mergeBoilerSnapshotNonDestructive(existing, incoming *vaillantBoilerSnapshot) *vaillantBoilerSnapshot {
	merged := cloneBoilerSnapshot(existing)
	if merged == nil {
		merged = &vaillantBoilerSnapshot{}
	}
	if incoming != nil {
		if incoming.FlowTemperatureC != nil {
			merged.FlowTemperatureC = cloneFloat64Ptr(incoming.FlowTemperatureC)
		}
		if incoming.CentralHeatingPumpActive != nil {
			merged.CentralHeatingPumpActive = cloneBoilerBoolPtr(incoming.CentralHeatingPumpActive)
		}
		if incoming.WaterPressureBar != nil {
			merged.WaterPressureBar = cloneFloat64Ptr(incoming.WaterPressureBar)
		}
		if incoming.ExternalPumpActive != nil {
			merged.ExternalPumpActive = cloneBoilerBoolPtr(incoming.ExternalPumpActive)
		}
		if incoming.CirculationPumpActive != nil {
			merged.CirculationPumpActive = cloneBoilerBoolPtr(incoming.CirculationPumpActive)
		}
		if incoming.GasValveActive != nil {
			merged.GasValveActive = cloneBoilerBoolPtr(incoming.GasValveActive)
		}
		if incoming.FlameActive != nil {
			merged.FlameActive = cloneBoilerBoolPtr(incoming.FlameActive)
		}
		if incoming.DiverterValvePositionPct != nil {
			merged.DiverterValvePositionPct = cloneFloat64Ptr(incoming.DiverterValvePositionPct)
		}
		if incoming.FanSpeedRpm != nil {
			merged.FanSpeedRpm = cloneBoilerIntPtr(incoming.FanSpeedRpm)
		}
		if incoming.TargetFanSpeedRpm != nil {
			merged.TargetFanSpeedRpm = cloneBoilerIntPtr(incoming.TargetFanSpeedRpm)
		}
		if incoming.IonisationVoltageUa != nil {
			merged.IonisationVoltageUa = cloneFloat64Ptr(incoming.IonisationVoltageUa)
		}
		if incoming.DhwWaterFlowLpm != nil {
			merged.DhwWaterFlowLpm = cloneFloat64Ptr(incoming.DhwWaterFlowLpm)
		}
		if incoming.DhwDemandActive != nil {
			merged.DhwDemandActive = cloneBoilerBoolPtr(incoming.DhwDemandActive)
		}
		if incoming.HeatingSwitchActive != nil {
			merged.HeatingSwitchActive = cloneBoilerBoolPtr(incoming.HeatingSwitchActive)
		}
		if incoming.StorageLoadPumpPct != nil {
			merged.StorageLoadPumpPct = cloneFloat64Ptr(incoming.StorageLoadPumpPct)
		}
		if incoming.ModulationPct != nil {
			merged.ModulationPct = cloneFloat64Ptr(incoming.ModulationPct)
		}
		if incoming.PrimaryCircuitFlowLpm != nil {
			merged.PrimaryCircuitFlowLpm = cloneFloat64Ptr(incoming.PrimaryCircuitFlowLpm)
		}
		if incoming.FlowTempDesiredC != nil {
			merged.FlowTempDesiredC = cloneFloat64Ptr(incoming.FlowTempDesiredC)
		}
		if incoming.DhwTempDesiredC != nil {
			merged.DhwTempDesiredC = cloneFloat64Ptr(incoming.DhwTempDesiredC)
		}
		if incoming.StateNumber != nil {
			merged.StateNumber = cloneBoilerIntPtr(incoming.StateNumber)
		}
		if incoming.DhwTemperatureC != nil {
			merged.DhwTemperatureC = cloneFloat64Ptr(incoming.DhwTemperatureC)
		}
		if incoming.DhwTargetTemperatureC != nil {
			merged.DhwTargetTemperatureC = cloneFloat64Ptr(incoming.DhwTargetTemperatureC)
		}
		if incoming.DhwOperatingMode != nil {
			merged.DhwOperatingMode = cloneBoilerIntPtr(incoming.DhwOperatingMode)
		}
		if incoming.FlowsetHcMaxC != nil {
			merged.FlowsetHcMaxC = cloneFloat64Ptr(incoming.FlowsetHcMaxC)
		}
		if incoming.FlowsetHwcMaxC != nil {
			merged.FlowsetHwcMaxC = cloneFloat64Ptr(incoming.FlowsetHwcMaxC)
		}
		if incoming.PartloadHcKW != nil {
			merged.PartloadHcKW = cloneFloat64Ptr(incoming.PartloadHcKW)
		}
		if incoming.PartloadHwcKW != nil {
			merged.PartloadHwcKW = cloneFloat64Ptr(incoming.PartloadHwcKW)
		}
		if incoming.HeatingStatusRaw != nil {
			merged.HeatingStatusRaw = cloneBoilerIntPtr(incoming.HeatingStatusRaw)
		}
		if incoming.DhwStatusRaw != nil {
			merged.DhwStatusRaw = cloneBoilerIntPtr(incoming.DhwStatusRaw)
		}
		if incoming.CentralHeatingHours != nil {
			merged.CentralHeatingHours = cloneFloat64Ptr(incoming.CentralHeatingHours)
		}
		if incoming.DhwHours != nil {
			merged.DhwHours = cloneFloat64Ptr(incoming.DhwHours)
		}
		if incoming.CentralHeatingStarts != nil {
			merged.CentralHeatingStarts = cloneBoilerIntPtr(incoming.CentralHeatingStarts)
		}
		if incoming.DhwStarts != nil {
			merged.DhwStarts = cloneBoilerIntPtr(incoming.DhwStarts)
		}
		if incoming.PumpHours != nil {
			merged.PumpHours = cloneFloat64Ptr(incoming.PumpHours)
		}
		if incoming.FanHours != nil {
			merged.FanHours = cloneFloat64Ptr(incoming.FanHours)
		}
		if incoming.DeactivationsIFC != nil {
			merged.DeactivationsIFC = cloneBoilerIntPtr(incoming.DeactivationsIFC)
		}
		if incoming.DeactivationsTemplimiter != nil {
			merged.DeactivationsTemplimiter = cloneBoilerIntPtr(incoming.DeactivationsTemplimiter)
		}
		if incoming.InstallerMenuCode != nil {
			merged.InstallerMenuCode = cloneBoilerIntPtr(incoming.InstallerMenuCode)
		}
		if incoming.PhoneNumber != nil {
			merged.PhoneNumber = cloneStringPtr(incoming.PhoneNumber)
		}
		if incoming.HoursTillService != nil {
			merged.HoursTillService = cloneBoilerIntPtr(incoming.HoursTillService)
		}
	}

	// Closed decision: do not map GG=0x02 RR=0x0008 as boiler return temperature.
	// Keep return temperature unset in B524-derived snapshots.
	merged.ReturnTemperatureC = nil
	return merged
}

func cloneBoilerSnapshot(snapshot *vaillantBoilerSnapshot) *vaillantBoilerSnapshot {
	if snapshot == nil {
		return nil
	}
	return &vaillantBoilerSnapshot{
		FlowTemperatureC:         cloneFloat64Ptr(snapshot.FlowTemperatureC),
		ReturnTemperatureC:       cloneFloat64Ptr(snapshot.ReturnTemperatureC),
		CentralHeatingPumpActive: cloneBoilerBoolPtr(snapshot.CentralHeatingPumpActive),
		WaterPressureBar:         cloneFloat64Ptr(snapshot.WaterPressureBar),
		ExternalPumpActive:       cloneBoilerBoolPtr(snapshot.ExternalPumpActive),
		CirculationPumpActive:    cloneBoilerBoolPtr(snapshot.CirculationPumpActive),
		GasValveActive:           cloneBoilerBoolPtr(snapshot.GasValveActive),
		FlameActive:              cloneBoilerBoolPtr(snapshot.FlameActive),
		DiverterValvePositionPct: cloneFloat64Ptr(snapshot.DiverterValvePositionPct),
		FanSpeedRpm:              cloneBoilerIntPtr(snapshot.FanSpeedRpm),
		TargetFanSpeedRpm:        cloneBoilerIntPtr(snapshot.TargetFanSpeedRpm),
		IonisationVoltageUa:      cloneFloat64Ptr(snapshot.IonisationVoltageUa),
		DhwWaterFlowLpm:          cloneFloat64Ptr(snapshot.DhwWaterFlowLpm),
		DhwDemandActive:          cloneBoilerBoolPtr(snapshot.DhwDemandActive),
		HeatingSwitchActive:      cloneBoilerBoolPtr(snapshot.HeatingSwitchActive),
		StorageLoadPumpPct:       cloneFloat64Ptr(snapshot.StorageLoadPumpPct),
		ModulationPct:            cloneFloat64Ptr(snapshot.ModulationPct),
		PrimaryCircuitFlowLpm:    cloneFloat64Ptr(snapshot.PrimaryCircuitFlowLpm),
		FlowTempDesiredC:         cloneFloat64Ptr(snapshot.FlowTempDesiredC),
		DhwTempDesiredC:          cloneFloat64Ptr(snapshot.DhwTempDesiredC),
		StateNumber:              cloneBoilerIntPtr(snapshot.StateNumber),
		DhwTemperatureC:          cloneFloat64Ptr(snapshot.DhwTemperatureC),
		DhwTargetTemperatureC:    cloneFloat64Ptr(snapshot.DhwTargetTemperatureC),
		DhwOperatingMode:         cloneBoilerIntPtr(snapshot.DhwOperatingMode),
		FlowsetHcMaxC:            cloneFloat64Ptr(snapshot.FlowsetHcMaxC),
		FlowsetHwcMaxC:           cloneFloat64Ptr(snapshot.FlowsetHwcMaxC),
		PartloadHcKW:             cloneFloat64Ptr(snapshot.PartloadHcKW),
		PartloadHwcKW:            cloneFloat64Ptr(snapshot.PartloadHwcKW),
		HeatingStatusRaw:         cloneBoilerIntPtr(snapshot.HeatingStatusRaw),
		DhwStatusRaw:             cloneBoilerIntPtr(snapshot.DhwStatusRaw),
		CentralHeatingHours:      cloneFloat64Ptr(snapshot.CentralHeatingHours),
		DhwHours:                 cloneFloat64Ptr(snapshot.DhwHours),
		CentralHeatingStarts:     cloneBoilerIntPtr(snapshot.CentralHeatingStarts),
		DhwStarts:                cloneBoilerIntPtr(snapshot.DhwStarts),
		PumpHours:                cloneFloat64Ptr(snapshot.PumpHours),
		FanHours:                 cloneFloat64Ptr(snapshot.FanHours),
		DeactivationsIFC:         cloneBoilerIntPtr(snapshot.DeactivationsIFC),
		DeactivationsTemplimiter: cloneBoilerIntPtr(snapshot.DeactivationsTemplimiter),
		InstallerMenuCode:        cloneBoilerIntPtr(snapshot.InstallerMenuCode),
		PhoneNumber:              cloneStringPtr(snapshot.PhoneNumber),
		HoursTillService:         cloneBoilerIntPtr(snapshot.HoursTillService),
	}
}

func cloneBoilerBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneBoilerIntPtr(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
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

func cloneBoilerModeIntPtr(mode *string) *int {
	if mode == nil {
		return nil
	}
	switch strings.ToUpper(strings.TrimSpace(*mode)) {
	case "OFF":
		value := 0
		return &value
	case "ON":
		value := 1
		return &value
	case "AUTO":
		value := 2
		return &value
	default:
		return nil
	}
}

func (p *vaillantSemanticPoller) persistSemanticCache(source semanticSnapshotSource) {
	if p == nil || source != semanticSnapshotSourceLive {
		return
	}
	p.persistSemanticSnapshot()
}

func (p *vaillantSemanticPoller) persistSemanticSnapshot() {
	if p == nil || p.cache == nil || p.provider == nil {
		return
	}
	p.mu.Lock()
	zones := p.cacheZonesLocked(p.provider.Zones())
	p.mu.Unlock()
	_ = p.cache.Save(semanticCacheSnapshot{
		Zones:  zones,
		DHW:    p.provider.DHW(),
		Boiler: p.provider.BoilerStatus(),
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

func decodeValvePositionPct(raw *uint16) *float64 {
	if raw == nil {
		return nil
	}
	pct := float64(*raw) / 655.35
	return &pct
}

func decodeB524DateSuppressSentinel(raw []byte) string {
	if len(raw) < 3 {
		return ""
	}
	day, month, year := int(raw[0]), int(raw[1]), 2000+int(raw[2])
	if year == 2015 && month == 1 && day == 1 {
		return ""
	}
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func decodeCircuitType(raw *uint16) string {
	if raw == nil {
		return ""
	}
	switch *raw {
	case 0:
		return "radiator"
	case 1:
		return "underfloor"
	case 2:
		return "mixed"
	default:
		return fmt.Sprintf("unknown_%d", *raw)
	}
}

func decodeAssociatedCircuit(raw *uint16) *int {
	if raw == nil {
		return nil
	}
	if *raw == 0xFF || *raw == 0xFFFF {
		return nil
	}
	v := int(*raw)
	return &v
}

func decodeRoomTemperatureZoneMapping(raw *uint16) *int {
	if raw == nil {
		return nil
	}
	if *raw == 0xFF || *raw == 0xFFFF {
		return nil
	}
	v := int(*raw)
	return &v
}

func floatPtrEquals(a, b *float64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func (p *vaillantSemanticPoller) findBoilerAddress() byte {
	if p == nil || p.reg == nil {
		return 0
	}

	// Phase C M-C6b: select the routing target byte for B5.09
	// boiler reads/writes. An aliased canonical pair (e.g. BAI
	// 0x03↔0x08) whose display is the initiator side (0x03) must
	// resolve to 0x08 here, otherwise refreshBoilerStatusB509
	// builds frames with Target: 0x03 and the read/write fails.
	// TargetAddressForRouting prefers AddressByRole(SlotRoleSlave)
	// and falls back to PrimaryDisplayAddress when no target-role
	// face exists.
	// P9.3 — race-free boiler-candidate enumeration via
	// IterateSnapshots + isBoilerDeviceSnapshotCandidate +
	// SnapshotTargetAddressForRouting.
	selected := byte(0)
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if !isBoilerDeviceSnapshotCandidate(snap) {
			return true
		}
		addr := ebusgateway.SnapshotTargetAddressForRouting(snap)
		if addr == 0x08 {
			selected = addr
			return false
		}
		if selected == 0 || addr < selected {
			selected = addr
		}
		return true
	})
	return selected
}

// isBoilerDeviceSnapshotCandidate is the snapshot-based counterpart of
// isBoilerDeviceCandidate (P9.3). Reads the snapshot's DeviceID
// (race-free) and applies the same prefix-based BAI/BMU classification.
func isBoilerDeviceSnapshotCandidate(snap registry.DeviceEntrySnapshot) bool {
	normalizedID := normalizeDeviceID(snap.DeviceID)
	if strings.HasPrefix(normalizedID, "BAI") {
		return true
	}
	return strings.HasPrefix(normalizedID, "BMU")
}

func normalizeDeviceID(value string) string {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "_", "")
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, " ", "")
	return normalized
}

// findRegulatorCapability iterates all devices in the registry and determines
// the regulator capability strictly from the product_ids catalog classification.
// Returns ControllerPresent if any Vaillant device has role=="Regulator",
// ControllerNone if all Vaillant devices have known non-regulator roles,
// ControllerUnknown if any device has an unknown classification or no serial.
func (p *vaillantSemanticPoller) findRegulatorCapability() productids.ControllerCapability {
	if p == nil || p.reg == nil || p.catalogErr != nil {
		return productids.ControllerUnknown
	}

	// P9.3 — race-free Manufacturer/SerialNumber reads via snapshot.
	foundPresent := false
	hasUnknown := false
	hasAnyDevice := false
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if !strings.EqualFold(snap.Manufacturer, "Vaillant") {
			return true
		}
		hasAnyDevice = true
		partNumber := extractPartNumberFromSerial(snap.SerialNumber)
		cap := p.catalog.ControllerCapability(partNumber)
		switch cap {
		case productids.ControllerPresent:
			foundPresent = true
			return false // stop iteration
		case productids.ControllerUnknown:
			hasUnknown = true
		}
		return true
	})

	if foundPresent {
		return productids.ControllerPresent
	}
	if !hasAnyDevice {
		return productids.ControllerUnknown
	}
	if hasUnknown {
		return productids.ControllerUnknown
	}
	return productids.ControllerNone
}

// extractPartNumberFromSerial extracts a 10-digit Vaillant part number from a
// device serial number string. The logic mirrors graphql.extractVaillantPartNumber.
func extractPartNumberFromSerial(serial string) string {
	serial = strings.TrimSpace(serial)
	if serial == "" {
		return ""
	}

	// Try dash-separated format: "YY-WW-CC-PPPPPPPPPP-SSSS-RRRRRR-XX"
	parts := strings.Split(serial, "-")
	if len(parts) >= 4 {
		partNumber := strings.TrimSpace(parts[3])
		if len(partNumber) == 10 && isAllDigits(partNumber) {
			return partNumber
		}
	}

	// Fallback: strip non-alphanumeric, take digits 6..16.
	compact := make([]byte, 0, len(serial))
	for i := 0; i < len(serial); i++ {
		ch := serial[i]
		if (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') {
			compact = append(compact, ch)
		}
	}
	if len(compact) >= 16 {
		candidate := string(compact[6:16])
		if len(candidate) == 10 && isAllDigits(candidate) {
			return candidate
		}
	}

	return ""
}

// isAllDigits returns true if the string is non-empty and contains only ASCII digits.
func isAllDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

type semanticReadWatchRuntime struct {
	key           ebusgateway.WatchKey
	descriptor    ebusgateway.WatchDescriptor
	hasDescriptor bool
	maxAge        time.Duration
}

func (p *vaillantSemanticPoller) prepareSemanticReadWatch(key ebusgateway.WatchKey) time.Duration {
	return p.prepareSemanticReadWatchRuntime(key).maxAge
}

func (p *vaillantSemanticPoller) prepareSemanticReadWatchRuntime(key ebusgateway.WatchKey) semanticReadWatchRuntime {
	return p.resolveSemanticReadWatchRuntime(
		key,
		semanticReadWatchDescriptorDefault,
		[]ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourcePoller},
		semanticReadActivationSources,
	)
}

func (p *vaillantSemanticPoller) bootstrapPassiveSharedWatchKey(key ebusgateway.WatchKey) semanticReadWatchRuntime {
	return p.resolveSemanticReadWatchRuntime(
		key,
		semanticReadWatchDescriptorPassiveFallback,
		[]ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourceTooling},
		semanticReadPassiveActivationSources,
	)
}

func (p *vaillantSemanticPoller) resolveSemanticReadWatchRuntime(
	key ebusgateway.WatchKey,
	fallback func(ebusgateway.WatchKey) ebusgateway.WatchDescriptor,
	defaultSources []ebusgateway.WatchActivationSource,
	sourceResolver func([]ebusgateway.WatchActivationSource) []ebusgateway.WatchActivationSource,
) semanticReadWatchRuntime {
	runtime := semanticReadWatchRuntime{
		key: key,
	}
	if key == nil {
		return runtime
	}

	descriptor := fallback(key)
	hasDescriptor := false
	sources := defaultSources
	if sourceResolver == nil {
		sourceResolver = semanticReadActivationSources
	}
	if p != nil && p.watchObserver != nil {
		observation := p.watchObserver.Observe(key)
		if observation.HasDescriptor {
			descriptor = semanticReadWatchDescriptorFromObservation(key, observation.Descriptor)
			hasDescriptor = true
		}
		sources = sourceResolver(observation.Sources)
	}

	maxAge, err := descriptor.EffectiveFreshnessTTL()
	if err != nil {
		descriptor = fallback(key)
		maxAge, _ = descriptor.EffectiveFreshnessTTL()
		hasDescriptor = false
	}
	if maxAge < 0 {
		maxAge = 0
	}

	if p != nil && p.shadow != nil {
		if err := p.shadow.BootstrapRuntimeDescriptor(descriptor, sources...); err != nil {
			log.Printf("semantic_read_shadow_bootstrap_failed key=%q err=%v", key.Canonical(), err)
		}
	}

	runtime.descriptor = descriptor
	runtime.hasDescriptor = hasDescriptor
	runtime.maxAge = maxAge
	return runtime
}

func (p *vaillantSemanticPoller) emitWatchReadEfficiency(runtime semanticReadWatchRuntime, maxAge time.Duration, stats ebusgateway.SemanticReadExecutionStats) {
	if p == nil || p.watchEfficiency == nil || runtime.key == nil {
		return
	}
	observedAt := p.now()
	p.watchEfficiency.ObserveWatchRead(ebusgateway.WatchEfficiencyReadEvent{
		Key:           runtime.key,
		Descriptor:    runtime.descriptor,
		HasDescriptor: runtime.hasDescriptor,
		MaxAge:        maxAge,
		Stats:         stats,
		ObservedAt:    observedAt,
	})
}

func (p *vaillantSemanticPoller) emitWatchDirectApplyEfficiency(runtime semanticReadWatchRuntime, observedAt time.Time, candidateEvaluated bool, accepted bool) {
	if p == nil || p.watchEfficiency == nil || runtime.key == nil {
		return
	}
	if observedAt.IsZero() {
		observedAt = p.now()
	}
	p.watchEfficiency.ObserveWatchDirectApply(ebusgateway.WatchEfficiencyDirectApplyEvent{
		Key:                runtime.key,
		Descriptor:         runtime.descriptor,
		HasDescriptor:      runtime.hasDescriptor,
		ObservedAt:         observedAt,
		CandidateEvaluated: candidateEvaluated,
		Accepted:           accepted,
	})
}

func semanticReadWatchDescriptorDefault(key ebusgateway.WatchKey) ebusgateway.WatchDescriptor {
	return semanticReadWatchDescriptorWithDecoderID(key, "semantic.read.poller")
}

func semanticReadWatchDescriptorPassiveFallback(key ebusgateway.WatchKey) ebusgateway.WatchDescriptor {
	return semanticReadWatchDescriptorWithDecoderID(key, "semantic.read.passive_fallback")
}

func semanticReadWatchDescriptorWithDecoderID(key ebusgateway.WatchKey, decoderID string) ebusgateway.WatchDescriptor {
	semanticClass, freshnessProfile, directApplyPolicy := semanticReadWatchDescriptorPolicy(key)
	return ebusgateway.WatchDescriptor{
		Key:               key,
		SemanticClass:     semanticClass,
		FreshnessProfile:  freshnessProfile,
		DecoderID:         decoderID,
		CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
		DirectApplyPolicy: directApplyPolicy,
	}
}

func semanticReadWatchDescriptorPolicy(key ebusgateway.WatchKey) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch typed := key.(type) {
	case ebusgateway.B524WatchKey:
		return semanticReadB524WatchDescriptorPolicy(typed)
	case *ebusgateway.B524WatchKey:
		if typed != nil {
			return semanticReadB524WatchDescriptorPolicy(*typed)
		}
	}
	return semanticReadStateFastDescriptorPolicy()
}

func semanticReadStateFastDescriptorPolicy() (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	return ebusgateway.WatchSemanticClassState, ebusgateway.WatchFreshnessProfileStateFast, ebusgateway.WatchDirectApplyPolicyStateDefault
}

func semanticReadStateSlowDescriptorPolicy() (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	return ebusgateway.WatchSemanticClassState, ebusgateway.WatchFreshnessProfileStateSlow, ebusgateway.WatchDirectApplyPolicyStateDefault
}

func semanticReadConfigDescriptorPolicy() (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	return ebusgateway.WatchSemanticClassConfig, ebusgateway.WatchFreshnessProfileConfig, ebusgateway.WatchDirectApplyPolicyConfigOptIn
}

func semanticReadDiscoveryDescriptorPolicy() (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	return ebusgateway.WatchSemanticClassDiscovery, ebusgateway.WatchFreshnessProfileDiscovery, ebusgateway.WatchDirectApplyPolicyNever
}

func semanticReadB524WatchDescriptorPolicy(key ebusgateway.B524WatchKey) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch key.Opcode {
	case vaillantB524OpcodeLocal:
		return semanticReadB524LocalDescriptorPolicy(key.Group, key.RegisterAddress)
	case vaillantB524OpcodeRead:
		return semanticReadB524RemoteDescriptorPolicy(key.Group, key.RegisterAddress)
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524LocalDescriptorPolicy(group byte, addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch group {
	case localZones.group:
		return semanticReadB524ZoneDescriptorPolicy(addr)
	case localDHW.group:
		return semanticReadB524DHWDescriptorPolicy(addr)
	case localCircuits.group:
		return semanticReadB524CircuitDescriptorPolicy(addr)
	case localRegulator.group:
		return semanticReadB524RegulatorDescriptorPolicy(addr)
	case localSolar.group:
		return semanticReadB524SolarDescriptorPolicy(addr)
	case localCylinders.group:
		return semanticReadB524CylinderDescriptorPolicy(addr)
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524RegulatorDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case system_off,
		system_water_pressure,
		system_flow_temperature,
		system_outdoor_temperature,
		system_maintenance_due,
		system_hwc_cylinder_temperature_top,
		system_hwc_cylinder_temperature_bottom:
		return semanticReadStateFastDescriptorPolicy()
	case system_outdoor_temperature_avg_24h,
		energy_fuel_sum_hc,
		energy_electricity_sum_hc,
		energy_electricity_sum_hwc,
		energy_fuel_sum_hwc,
		energy_fuel_sum_hc_this_month,
		energy_electricity_sum_hc_this_month,
		energy_electricity_sum_hwc_this_month,
		energy_fuel_sum_hwc_this_month,
		energy_fuel_sum_hc_last_month,
		energy_electricity_sum_hc_last_month,
		energy_electricity_sum_hwc_last_month,
		energy_fuel_sum_hwc_last_month:
		return semanticReadStateSlowDescriptorPolicy()
	case system_adaptive_heating_curve,
		system_alternative_point,
		system_heating_circuit_bivalence_point,
		system_dhw_bivalence_point,
		system_hc_emergency_temperature,
		system_hwc_max_flow_temp_desired,
		system_max_room_humidity,
		system_maintenance_date,
		system_installer_name_1,
		system_installer_name_2,
		system_installer_phone_1,
		system_installer_phone_2,
		system_installer_menu_code:
		return semanticReadConfigDescriptorPolicy()
	case system_scheme, system_module_configuration_vr71:
		return semanticReadDiscoveryDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524ZoneDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case zone_current_temp, zone_special_function, zone_valve_status:
		return semanticReadStateFastDescriptorPolicy()
	case zone_current_humidity, zone_quick_veto_end_time, zone_quick_veto_end_date:
		return semanticReadStateSlowDescriptorPolicy()
	case zone_heating_op_mode,
		zone_target_temp,
		zone_fallback_manual_temp,
		zone_room_temperature_zone_mapping_raw,
		zone_quick_veto_temp,
		zone_quick_veto_duration,
		zone_holiday_start_date,
		zone_holiday_end_date,
		zone_holiday_setpoint,
		zone_holiday_end_time,
		zone_holiday_start_time:
		return semanticReadConfigDescriptorPolicy()
	case zone_name, zone_name_prefix, zone_name_suffix, zone_index:
		return semanticReadDiscoveryDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524DHWDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case dhw_current_temp, dhw_special_function:
		return semanticReadStateFastDescriptorPolicy()
	case dhw_operation_mode, dhw_target_temp, dhw_holiday_start_date, dhw_holiday_end_date:
		return semanticReadConfigDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524CircuitDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case circuit_flow_setpoint,
		circuit_flow_temp,
		circuit_circuit_state,
		circuit_pump_status,
		circuit_calc_flow_temp,
		circuit_mixer_position:
		return semanticReadStateFastDescriptorPolicy()
	case circuit_humidity,
		circuit_dew_point,
		circuit_pump_hours,
		circuit_pump_starts:
		return semanticReadStateSlowDescriptorPolicy()
	case circuit_type,
		circuit_cooling_enabled,
		circuit_heating_curve,
		circuit_flow_temp_max,
		circuit_flow_temp_min,
		circuit_summer_limit,
		circuit_room_temp_control,
		circuit_frost_protection:
		return semanticReadConfigDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524SolarDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case solar_collector_temp, solar_return_temp, solar_pump_active, solar_current_yield:
		return semanticReadStateFastDescriptorPolicy()
	case solar_pump_hours:
		return semanticReadStateSlowDescriptorPolicy()
	case solar_enabled, solar_function_mode:
		return semanticReadConfigDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524CylinderDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case cylinder_temperature:
		return semanticReadStateFastDescriptorPolicy()
	case cylinder_max_setpoint, cylinder_charge_hysteresis, cylinder_charge_offset:
		return semanticReadConfigDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524RemoteDescriptorPolicy(group byte, addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch group {
	case remoteRegulators.group, remoteThermostats.group, remoteFunctionalModules.group:
		switch addr {
		case device_slot_connected:
			return semanticReadStateSlowDescriptorPolicy()
		case device_slot_room_humidity,
			device_slot_room_temperature,
			device_slot_paired,
			device_slot_reception_strength:
			return semanticReadStateSlowDescriptorPolicy()
		case device_slot_class_address,
			device_slot_firmware,
			device_slot_remote_control_address,
			device_slot_hardware_identifier,
			device_slot_zone_assignment:
			return semanticReadDiscoveryDescriptorPolicy()
		default:
			return semanticReadStateFastDescriptorPolicy()
		}
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadWatchDescriptorFromObservation(key ebusgateway.WatchKey, observed ebusgateway.WatchDescriptor) ebusgateway.WatchDescriptor {
	normalized := observed
	normalized.Key = key
	if normalized.SemanticClass == "" {
		normalized.SemanticClass = ebusgateway.WatchSemanticClassState
	}
	if normalized.FreshnessProfile == "" {
		normalized.FreshnessProfile = ebusgateway.WatchFreshnessProfileStateFast
	}
	if normalized.DecoderID == "" {
		normalized.DecoderID = "semantic.read.poller"
	}
	if normalized.CorrelationPolicy == "" {
		normalized.CorrelationPolicy = ebusgateway.WatchCorrelationPolicyRequestResponse
	}
	if normalized.DirectApplyPolicy == "" {
		normalized.DirectApplyPolicy = ebusgateway.WatchDirectApplyPolicyStateDefault
	}
	return normalized
}

func semanticReadActivationSources(sources []ebusgateway.WatchActivationSource) []ebusgateway.WatchActivationSource {
	if len(sources) == 0 {
		return []ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourcePoller}
	}

	out := make([]ebusgateway.WatchActivationSource, 0, len(sources)+1)
	seen := make(map[ebusgateway.WatchActivationSource]struct{}, len(sources)+1)
	for _, source := range sources {
		if source == "" {
			continue
		}
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}
		out = append(out, source)
	}
	if _, ok := seen[ebusgateway.WatchActivationSourcePoller]; !ok {
		out = append(out, ebusgateway.WatchActivationSourcePoller)
	}
	return out
}

func semanticReadPassiveActivationSources(sources []ebusgateway.WatchActivationSource) []ebusgateway.WatchActivationSource {
	if len(sources) == 0 {
		return []ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourceTooling}
	}

	seen := make(map[ebusgateway.WatchActivationSource]struct{}, len(sources))
	out := make([]ebusgateway.WatchActivationSource, 0, len(sources))
	for _, source := range sources {
		if source == "" {
			continue
		}
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}
		out = append(out, source)
	}
	if len(out) == 0 {
		return []ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourceTooling}
	}
	return out
}

func (p *vaillantSemanticPoller) readB509Value(ctx context.Context, target byte, addr uint16) ([]byte, bool) {
	return p.readB509ValueWithMaxAge(ctx, target, addr, -1)
}

func (p *vaillantSemanticPoller) readB509ValueLive(ctx context.Context, target byte, addr uint16) ([]byte, bool) {
	return p.readB509ValueWithMaxAge(ctx, target, addr, 0)
}

func (p *vaillantSemanticPoller) readB509ValueWithMaxAge(ctx context.Context, target byte, addr uint16, maxAgeOverride time.Duration) ([]byte, bool) {
	if p == nil || (p.bus == nil && p.sendFrameFn == nil) || target == 0 {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	timeout := p.requestTimeout
	p.mu.Unlock()
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	watchKey := ebusgateway.NewB509WatchKey(target, addr)
	watchRuntime := p.prepareSemanticReadWatchRuntime(watchKey)
	maxAge := watchRuntime.maxAge
	if maxAgeOverride >= 0 {
		maxAge = maxAgeOverride
	}
	value, stats, err := p.scheduler.GetWatchWithStats(ctx, watchKey, maxAge, func(ctx context.Context) ([]byte, error) {
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			p.readMu.Lock()
			reqCtx, cancel := context.WithTimeout(ctx, timeout)
			request := protocol.Frame{
				FrameType: protocol.FrameTypeInitiatorTarget,
				Source:    source,
				Target:    target,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509ReadSelector(addr),
			}
			response, err := p.sendSemanticFrame(reqCtx, request)
			cancel()
			p.readMu.Unlock()

			if err != nil {
				lastErr = err
			} else if response == nil {
				lastErr = fmt.Errorf("b509 read returned nil response")
			} else if payload, ok := parseB509ReadPayload(response.Data, addr); ok {
				return payload, nil
			} else {
				lastErr = fmt.Errorf("b509 read failed: target=0x%02x addr=0x%04x", target, addr)
			}

			if attempt < 2 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(75 * time.Millisecond):
				}
			}
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("b509 read failed")
		}
		return nil, lastErr
	})
	p.emitWatchReadEfficiency(watchRuntime, maxAge, stats)
	if stats.ActiveFetchSucceeded {
		select {
		case <-ctx.Done():
		case <-time.After(25 * time.Millisecond):
		}
	}
	if err != nil {
		return nil, false
	}
	return value, true
}

func (p *vaillantSemanticPoller) writeB509Value(ctx context.Context, target byte, addr uint16, value []byte) error {
	if p == nil || (p.bus == nil && p.sendFrameFn == nil) {
		return fmt.Errorf("b509 write unavailable")
	}
	if target == 0 {
		return fmt.Errorf("b509 write target is zero")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	timeout := p.requestTimeout
	p.mu.Unlock()
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		p.readMu.Lock()
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		request := protocol.Frame{
			FrameType: protocol.FrameTypeInitiatorTarget,
			Source:    source,
			Target:    target,
			Primary:   vaillantB509Primary,
			Secondary: vaillantB509Secondary,
			Data:      buildB509WriteSelector(addr, value),
		}
		response, err := p.sendSemanticFrame(reqCtx, request)
		cancel()
		p.readMu.Unlock()

		if err != nil {
			lastErr = err
		} else if response == nil {
			lastErr = fmt.Errorf("b509 write returned nil response")
		} else if parseB509WriteAck(response.Data, addr) {
			return nil
		} else {
			lastErr = fmt.Errorf("b509 write ack invalid: target=0x%02x addr=0x%04x", target, addr)
		}

		if attempt < 2 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(75 * time.Millisecond):
			}
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("b509 write failed")
	}
	return lastErr
}

func (p *vaillantSemanticPoller) sendSemanticFrame(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	if p == nil {
		return nil, fmt.Errorf("semantic poller unavailable")
	}
	if p.sendFrameFn != nil {
		return p.sendFrameFn(ctx, frame)
	}
	if p.bus == nil {
		return nil, fmt.Errorf("semantic bus unavailable")
	}
	return p.bus.Send(ctx, frame)
}

type boilerConfigFieldSpec struct {
	addrs []uint16
	min   float64
	max   float64
	codec boilerConfigCodec
}

type boilerConfigCodec uint8

const (
	boilerConfigCodecTempDATA2c boilerConfigCodec = iota
	boilerConfigCodecUCH
)

var boilerConfigFieldSpecs = map[string]boilerConfigFieldSpec{
	"flowsetHcMaxC":  {addrs: []uint16{boiler_b509_flowset_hc_max_c, boiler_b509_flowset_hc_max_c_fallback}, min: 20, max: 80, codec: boilerConfigCodecTempDATA2c},
	"flowsetHwcMaxC": {addrs: []uint16{boiler_b509_flowset_hwc_max_c}, min: 30, max: 65, codec: boilerConfigCodecTempDATA2c},
	"partloadHcKW":   {addrs: []uint16{boiler_b509_partload_hc_kw}, min: 0, max: 40, codec: boilerConfigCodecUCH},
	"partloadHwcKW":  {addrs: []uint16{boiler_b509_partload_hwc_kw}, min: 0, max: 40, codec: boilerConfigCodecUCH},
}

// cstringPairSpec defines a paired CString field that spans 2 registers.
type cstringPairSpec struct {
	addr1 uint16 // first register (first 6 chars)
	addr2 uint16 // second register (remaining chars)
}

var systemCStringPairSpecs = map[string]cstringPairSpec{
	"installerName":  {addr1: system_installer_name_1, addr2: system_installer_name_2},
	"installerPhone": {addr1: system_installer_phone_1, addr2: system_installer_phone_2},
}

func (p *vaillantSemanticPoller) SetSystemConfig(ctx context.Context, fieldName string, rawValue string) graphql.ConfigMutationResult {
	if p == nil {
		return graphql.ConfigMutationResult{Success: false, Error: "system config writer unavailable"}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Handle paired CString fields (2 registers × 6 chars).
	if pair, ok := systemCStringPairSpecs[fieldName]; ok {
		return p.writeSystemCStringPair(ctx, fieldName, rawValue, pair)
	}

	payload, spec, err := graphql.EncodeSystemConfigValue(fieldName, rawValue)
	if err != nil {
		return graphql.ConfigMutationResult{Success: false, Error: err.Error()}
	}

	return p.writeSystemSingleRegister(ctx, fieldName, rawValue, spec, payload)
}

func (p *vaillantSemanticPoller) writeSystemSingleRegister(ctx context.Context, fieldName, rawValue string, spec graphql.ConfigFieldSpec, payload []byte) graphql.ConfigMutationResult {
	opcode := byte(0x02)
	group := spec.Group()
	instance := byte(0x00)
	addr := spec.Addr()

	if err := p.writeB524Value(ctx, opcode, group, instance, addr, payload); err != nil {
		return graphql.ConfigMutationResult{Success: false, Error: fmt.Sprintf("b524 write failed: %v", err)}
	}

	readback, ok := p.readB524ValueLive(ctx, opcode, group, instance, addr)
	if !ok {
		return graphql.ConfigMutationResult{Success: false, Error: "b524 write confirm failed: read-back unavailable"}
	}
	if err := graphql.ConfirmDecodableReadback(spec, readback); err != nil {
		return graphql.ConfigMutationResult{Success: false, Error: fmt.Sprintf("b524 write confirm failed: %v", err)}
	}
	if !graphql.ConfigReadbackMatchesWrite(spec, payload, readback) {
		return graphql.ConfigMutationResult{Success: false, Error: "b524 write confirm failed: read-back mismatch"}
	}

	normalizedValue := strings.TrimSpace(rawValue)
	p.mu.Lock()
	p.updateSystemSnapshotLocked(systemSnapshotWithConfigValue(p.system, fieldName, normalizedValue))
	p.mu.Unlock()

	p.publishSystem(semanticSnapshotSourceLive)
	return graphql.ConfigMutationResult{Success: true}
}

func (p *vaillantSemanticPoller) writeSystemCStringPair(ctx context.Context, fieldName, rawValue string, pair cstringPairSpec) graphql.ConfigMutationResult {
	trimmed := strings.TrimSpace(rawValue)

	// Validate characters.
	for i := 0; i < len(trimmed); i++ {
		b := trimmed[i]
		if fieldName == "installerPhone" {
			if (b < '0' || b > '9') && b != '+' && b != '(' && b != ')' && b != ' ' {
				return graphql.ConfigMutationResult{Success: false, Error: fmt.Sprintf("invalid character '%c' at position %d (allowed: digits, +, (, ), space)", b, i)}
			}
		} else {
			if b < 0x20 || b > 0x7E {
				return graphql.ConfigMutationResult{Success: false, Error: fmt.Sprintf("invalid byte 0x%02X at position %d", b, i)}
			}
		}
	}
	if len(trimmed) > 12 {
		return graphql.ConfigMutationResult{Success: false, Error: fmt.Sprintf("string length %d exceeds max 12", len(trimmed))}
	}

	// Split into 2 parts of max 6 chars each.
	part1 := trimmed
	part2 := ""
	if len(trimmed) > 6 {
		part1 = trimmed[:6]
		part2 = trimmed[6:]
	}

	// Encode each part: null-padded to 7 bytes (6 chars + terminator).
	encode := func(s string) []byte {
		buf := make([]byte, 7)
		copy(buf, s)
		return buf
	}

	opcode := byte(0x02)
	group := byte(0x00)
	instance := byte(0x00)

	// Write part 1.
	if err := p.writeB524Value(ctx, opcode, group, instance, pair.addr1, encode(part1)); err != nil {
		return graphql.ConfigMutationResult{Success: false, Error: fmt.Sprintf("b524 write part1 failed: %v", err)}
	}
	// Write part 2.
	if err := p.writeB524Value(ctx, opcode, group, instance, pair.addr2, encode(part2)); err != nil {
		return graphql.ConfigMutationResult{Success: false, Error: fmt.Sprintf("b524 write part2 failed: %v", err)}
	}

	p.mu.Lock()
	p.updateSystemSnapshotLocked(systemSnapshotWithConfigValue(p.system, fieldName, trimmed))
	p.mu.Unlock()

	p.publishSystem(semanticSnapshotSourceLive)
	return graphql.ConfigMutationResult{Success: true}
}

func systemSnapshotWithConfigValue(existing *vaillantSystemSnapshot, fieldName, rawValue string) *vaillantSystemSnapshot {
	snapshot := cloneSystemSnapshot(existing)
	if snapshot == nil {
		snapshot = &vaillantSystemSnapshot{}
	}
	switch fieldName {
	case "maintenanceDate":
		snapshot.MaintenanceDate = &rawValue
	case "installerName":
		snapshot.InstallerName = &rawValue
	case "installerPhone":
		snapshot.InstallerPhone = &rawValue
	case "installerMenuCode":
		if v, err := strconv.Atoi(rawValue); err == nil {
			u := uint16(v)
			snapshot.InstallerMenuCode = &u
		}
	}
	return snapshot
}

func (p *vaillantSemanticPoller) SetBoilerConfig(ctx context.Context, fieldName string, rawValue string) graphql.BoilerConfigMutationResult {
	if p == nil {
		return graphql.BoilerConfigMutationResult{Success: false, Error: "boiler config writer unavailable"}
	}

	// Handle BCD phone number specially (not float64-based).
	if fieldName == "phoneNumber" {
		return p.writeBoilerPhoneBCD(ctx, rawValue)
	}
	// Handle boiler installer menu code as integer (not float64-based).
	if fieldName == "installerMenuCode" {
		return p.writeBoilerInstallerMenuCode(ctx, rawValue)
	}

	spec, ok := boilerConfigFieldSpecs[fieldName]
	if !ok {
		allKeys := make([]string, 0, len(boilerConfigFieldSpecs)+2)
		for key := range boilerConfigFieldSpecs {
			allKeys = append(allKeys, key)
		}
		allKeys = append(allKeys, "phoneNumber", "installerMenuCode")
		slices.Sort(allKeys)
		return graphql.BoilerConfigMutationResult{
			Success: false,
			Error:   fmt.Sprintf("unknown boiler field %q (allowed: %s)", fieldName, strings.Join(allKeys, ", ")),
		}
	}

	value, err := parseBoilerConfigValue(rawValue, spec)
	if err != nil {
		return graphql.BoilerConfigMutationResult{Success: false, Error: err.Error()}
	}

	p.mu.Lock()
	boilerAddress := p.boilerAddress
	p.mu.Unlock()
	if boilerAddress == 0 {
		return graphql.BoilerConfigMutationResult{Success: false, Error: "unsupported source: boiler B509 address unavailable"}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	payload, normalizedValue, err := encodeBoilerConfigPayload(fieldName, spec, value)
	if err != nil {
		return graphql.BoilerConfigMutationResult{Success: false, Error: err.Error()}
	}
	addr := p.resolveB509BoilerConfigAddr(ctx, boilerAddress, spec)
	if err := p.writeB509Value(ctx, boilerAddress, addr, payload); err != nil {
		return graphql.BoilerConfigMutationResult{Success: false, Error: fmt.Sprintf("b509 write failed: %v", err)}
	}
	p.fenceB509WriteConfirmShadow(boilerAddress, addr)
	readback, ok := p.readB509ValueLive(ctx, boilerAddress, addr)
	if !ok || len(readback) < len(payload) {
		return graphql.BoilerConfigMutationResult{Success: false, Error: "b509 write confirm failed: read-back unavailable"}
	}
	if !slices.Equal(readback[:len(payload)], payload) {
		return graphql.BoilerConfigMutationResult{Success: false, Error: "b509 write confirm failed: read-back mismatch"}
	}

	p.mu.Lock()
	p.boiler = boilerSnapshotWithConfigValue(p.boiler, fieldName, normalizedValue)
	p.mu.Unlock()

	p.publishBoilerStatus(semanticSnapshotSourceLive)
	return graphql.BoilerConfigMutationResult{Success: true}
}

func (p *vaillantSemanticPoller) fenceB509WriteConfirmShadow(target byte, addr uint16) {
	if p == nil || p.shadow == nil || target == 0 {
		return
	}

	key := ebusgateway.NewB509WatchKey(target, addr)
	p.shadow.Invalidate(ebusgateway.ShadowInvalidation{
		Key:           key,
		Reason:        ebusgateway.ShadowInvalidationReasonExternalWrite,
		Source:        ebusgateway.ShadowInvalidationSourceActive,
		InvalidatedAt: p.now(),
	})
}

func (p *vaillantSemanticPoller) writeBoilerPhoneBCD(ctx context.Context, rawValue string) graphql.BoilerConfigMutationResult {
	// Strip formatting characters, keep only digits.
	trimmed := strings.TrimSpace(rawValue)
	var digits []byte
	for _, b := range []byte(trimmed) {
		if b >= '0' && b <= '9' {
			digits = append(digits, b)
		}
	}
	if len(digits) > 16 {
		return graphql.BoilerConfigMutationResult{Success: false, Error: fmt.Sprintf("phone number has %d digits (max 16)", len(digits))}
	}

	// Encode digits to BCD (2 digits per byte, pad with 0xF).
	payload := make([]byte, 8)
	for i := range payload {
		payload[i] = 0xFF
	}
	for i := 0; i < len(digits); i += 2 {
		hi := digits[i] - '0'
		var lo byte = 0x0F
		if i+1 < len(digits) {
			lo = digits[i+1] - '0'
		}
		payload[i/2] = (hi << 4) | lo
	}

	p.mu.Lock()
	boilerAddress := p.boilerAddress
	p.mu.Unlock()
	if boilerAddress == 0 {
		return graphql.BoilerConfigMutationResult{Success: false, Error: "boiler B509 address unavailable"}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if err := p.writeB509Value(ctx, boilerAddress, boiler_b509_phone_number, payload); err != nil {
		return graphql.BoilerConfigMutationResult{Success: false, Error: fmt.Sprintf("b509 write failed: %v", err)}
	}

	decoded := string(digits)
	p.mu.Lock()
	p.boiler = boilerSnapshotWithStringConfigValue(p.boiler, "phoneNumber", decoded)
	p.mu.Unlock()

	p.publishBoilerStatus(semanticSnapshotSourceLive)
	return graphql.BoilerConfigMutationResult{Success: true}
}

func (p *vaillantSemanticPoller) writeBoilerInstallerMenuCode(ctx context.Context, rawValue string) graphql.BoilerConfigMutationResult {
	trimmed := strings.TrimSpace(rawValue)
	value, err := strconv.Atoi(trimmed)
	if err != nil {
		return graphql.BoilerConfigMutationResult{Success: false, Error: fmt.Sprintf("invalid integer %q: %v", rawValue, err)}
	}
	if value < 0 || value > 255 {
		return graphql.BoilerConfigMutationResult{Success: false, Error: fmt.Sprintf("value %d out of range [0, 255]", value)}
	}

	p.mu.Lock()
	boilerAddress := p.boilerAddress
	p.mu.Unlock()
	if boilerAddress == 0 {
		return graphql.BoilerConfigMutationResult{Success: false, Error: "boiler B509 address unavailable"}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	payload := []byte{byte(value)}
	if err := p.writeB509Value(ctx, boilerAddress, boiler_b509_installer_menu_code, payload); err != nil {
		return graphql.BoilerConfigMutationResult{Success: false, Error: fmt.Sprintf("b509 write failed: %v", err)}
	}

	p.mu.Lock()
	p.boiler = boilerSnapshotWithIntConfigValue(p.boiler, "installerMenuCode", value)
	p.mu.Unlock()

	p.publishBoilerStatus(semanticSnapshotSourceLive)
	return graphql.BoilerConfigMutationResult{Success: true}
}

func boilerSnapshotWithStringConfigValue(existing *vaillantBoilerSnapshot, fieldName, value string) *vaillantBoilerSnapshot {
	snapshot := cloneBoilerSnapshot(existing)
	if snapshot == nil {
		snapshot = &vaillantBoilerSnapshot{}
	}
	switch fieldName {
	case "phoneNumber":
		snapshot.PhoneNumber = &value
	}
	return snapshot
}

func boilerSnapshotWithIntConfigValue(existing *vaillantBoilerSnapshot, fieldName string, value int) *vaillantBoilerSnapshot {
	snapshot := cloneBoilerSnapshot(existing)
	if snapshot == nil {
		snapshot = &vaillantBoilerSnapshot{}
	}
	switch fieldName {
	case "installerMenuCode":
		snapshot.InstallerMenuCode = &value
	}
	return snapshot
}

func parseBoilerConfigValue(rawValue string, spec boilerConfigFieldSpec) (float64, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(rawValue), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid boiler value %q: %v", rawValue, err)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("invalid boiler value %q: finite number required", rawValue)
	}
	if value < spec.min || value > spec.max {
		return 0, fmt.Errorf("value %.4g out of range [%.4g, %.4g]", value, spec.min, spec.max)
	}
	return value, nil
}

func boilerSnapshotWithConfigValue(existing *vaillantBoilerSnapshot, fieldName string, value float64) *vaillantBoilerSnapshot {
	snapshot := cloneBoilerSnapshot(existing)
	if snapshot == nil {
		snapshot = &vaillantBoilerSnapshot{}
	}

	switch fieldName {
	case "flowsetHcMaxC":
		snapshot.FlowsetHcMaxC = cloneFloat64Ptr(&value)
	case "flowsetHwcMaxC":
		snapshot.FlowsetHwcMaxC = cloneFloat64Ptr(&value)
	case "partloadHcKW":
		snapshot.PartloadHcKW = cloneFloat64Ptr(&value)
	case "partloadHwcKW":
		snapshot.PartloadHwcKW = cloneFloat64Ptr(&value)
	}

	return snapshot
}

func encodeBoilerConfigPayload(fieldName string, spec boilerConfigFieldSpec, value float64) ([]byte, float64, error) {
	switch spec.codec {
	case boilerConfigCodecTempDATA2c:
		payload := encodeTempDATA2c(value)
		normalizedValue, ok := decodeDATA2c(payload)
		if !ok {
			return nil, 0, fmt.Errorf("invalid boiler value %.4g for %s: data2c normalization failed", value, fieldName)
		}
		return payload, normalizedValue, nil
	case boilerConfigCodecUCH:
		payload, ok := encodeUCH(value)
		if !ok {
			return nil, 0, fmt.Errorf("invalid boiler value %.4g for %s: whole-number kW required", value, fieldName)
		}
		return payload, float64(payload[0]), nil
	default:
		return nil, 0, fmt.Errorf("unsupported boiler field codec for %s", fieldName)
	}
}

func (p *vaillantSemanticPoller) readB509BoilerConfigFloat(ctx context.Context, target byte, fieldName string) *float64 {
	spec, ok := boilerConfigFieldSpecs[fieldName]
	if !ok {
		return nil
	}
	for _, addr := range spec.addrs {
		raw, ok := p.readB509Value(ctx, target, addr)
		if !ok {
			continue
		}
		value, ok := decodeBoilerConfigRaw(spec, raw)
		if !ok {
			continue
		}
		return &value
	}
	return nil
}

func (p *vaillantSemanticPoller) resolveB509BoilerConfigAddr(ctx context.Context, target byte, spec boilerConfigFieldSpec) uint16 {
	for _, addr := range spec.addrs {
		raw, ok := p.readB509Value(ctx, target, addr)
		if !ok {
			continue
		}
		if _, ok := decodeBoilerConfigRaw(spec, raw); ok {
			return addr
		}
	}
	if len(spec.addrs) == 0 {
		return 0
	}
	return spec.addrs[0]
}

func decodeBoilerConfigRaw(spec boilerConfigFieldSpec, raw []byte) (float64, bool) {
	switch spec.codec {
	case boilerConfigCodecTempDATA2c:
		return decodeDATA2c(raw)
	case boilerConfigCodecUCH:
		value, ok := decodeUCH(raw)
		return float64(value), ok
	default:
		return 0, false
	}
}

func (p *vaillantSemanticPoller) readB509DATA2b(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeDATA2b(raw)
	if !ok {
		return nil
	}
	return &value
}

func (p *vaillantSemanticPoller) readB509DATA2c(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeDATA2c(raw)
	if !ok {
		return nil
	}
	return &value
}

func (p *vaillantSemanticPoller) readB509OnOff(ctx context.Context, target byte, addr uint16) *bool {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeOnOff(raw)
	if !ok {
		return nil
	}
	return &value
}

func (p *vaillantSemanticPoller) readB509UINInt(ctx context.Context, target byte, addr uint16) *int {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeUIN(raw)
	if !ok {
		return nil
	}
	out := int(value)
	return &out
}

func (p *vaillantSemanticPoller) readB509SINScaled(ctx context.Context, target byte, addr uint16, divisor float64) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeSIN(raw)
	if !ok {
		return nil
	}
	out := float64(value)
	if divisor != 0 {
		out /= divisor
	}
	return &out
}

func (p *vaillantSemanticPoller) readB509UIN100(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeUIN100(raw)
	if !ok {
		return nil
	}
	return &value
}

func (p *vaillantSemanticPoller) readB509Percent0(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodePercent0(raw)
	if !ok {
		return nil
	}
	out := float64(value)
	return &out
}

func (p *vaillantSemanticPoller) readB509Hoursum2(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeHoursum2(raw)
	if !ok {
		return nil
	}
	return &value
}

func (p *vaillantSemanticPoller) readB509Hoursum2Int(ctx context.Context, target byte, addr uint16) *int {
	value := p.readB509Hoursum2(ctx, target, addr)
	if value == nil {
		return nil
	}
	v := int(*value)
	return &v
}

func (p *vaillantSemanticPoller) readB509PhoneBCD(ctx context.Context, target byte, addr uint16) *string {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok || len(raw) == 0 {
		return nil
	}
	s := decodeBCDPhone(raw)
	if s == "" {
		return nil
	}
	return &s
}

func decodeBCDPhone(b []byte) string {
	var buf strings.Builder
	for _, v := range b {
		hi, lo := v>>4, v&0x0F
		if hi <= 9 {
			buf.WriteByte('0' + hi)
		} else {
			break
		}
		if lo <= 9 {
			buf.WriteByte('0' + lo)
		} else {
			break
		}
	}
	return buf.String()
}

func (p *vaillantSemanticPoller) readB509UCHInt(ctx context.Context, target byte, addr uint16) *int {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeUCH(raw)
	if !ok {
		return nil
	}
	out := int(value)
	return &out
}

func (p *vaillantSemanticPoller) readB509UCHFloat(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeUCH(raw)
	if !ok {
		return nil
	}
	out := float64(value)
	return &out
}

func (p *vaillantSemanticPoller) readB524Value(ctx context.Context, opcode, group, instance byte, addr uint16) ([]byte, bool) {
	if p == nil || (p.bus == nil && p.sendFrameFn == nil) {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	target := p.controller
	timeout := p.requestTimeout
	p.mu.Unlock()

	if target == 0 {
		return nil, false
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	watchKey := ebusgateway.NewB524WatchKey(target, opcode, group, instance, addr)
	watchRuntime := p.prepareSemanticReadWatchRuntime(watchKey)
	maxAge := watchRuntime.maxAge
	value, stats, err := p.scheduler.GetWatchWithStats(ctx, watchKey, maxAge, func(ctx context.Context) ([]byte, error) {
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			p.readMu.Lock()

			reqCtx, cancel := context.WithTimeout(ctx, timeout)
			request := protocol.Frame{
				FrameType: protocol.FrameTypeInitiatorTarget,
				Source:    source,
				Target:    target,
				Primary:   vaillantExtRegisterPrimary,
				Secondary: vaillantExtRegisterSecondary,
				Data:      buildB524ReadSelector(opcode, group, instance, addr),
			}
			response, err := p.sendSemanticFrame(reqCtx, request)
			cancel()
			p.readMu.Unlock()

			if err != nil {
				lastErr = err
			} else if response == nil {
				lastErr = fmt.Errorf("b524 read returned nil response")
			} else {
				payload, ok := parseB524ReadPayload(response.Data, opcode, group, instance, addr)
				if ok {
					return payload, nil
				}
				lastErr = fmt.Errorf(
					"b524 read failed: opcode=0x%02x group=0x%02x instance=0x%02x addr=0x%04x",
					opcode,
					group,
					instance,
					addr,
				)
			}

			if attempt < 2 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(75 * time.Millisecond):
				}
			}
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("b524 read failed")
		}
		return nil, lastErr
	})
	p.emitWatchReadEfficiency(watchRuntime, maxAge, stats)
	if stats.ActiveFetchSucceeded {
		select {
		case <-ctx.Done():
		case <-time.After(25 * time.Millisecond):
		}
	}
	if err != nil {
		return nil, false
	}
	return value, true
}

func semanticReadBreakerKey(target, opcode, group, instance byte, addr uint16) string {
	return ebusgateway.NewB524WatchKey(target, opcode, group, instance, addr).Canonical()
}

func buildB509ReadSelector(addr uint16) []byte {
	return []byte{
		vaillantB509OpcodeRead,
		byte(addr >> 8),
		byte(addr),
	}
}

func buildB509WriteSelector(addr uint16, value []byte) []byte {
	data := make([]byte, 0, 3+len(value))
	data = append(data, vaillantB509OpcodeWrite, byte(addr>>8), byte(addr))
	data = append(data, value...)
	return data
}

func parseB509ReadPayload(payload []byte, addr uint16) ([]byte, bool) {
	if len(payload) == 0 {
		return nil, false
	}

	addrHi := byte(addr >> 8)
	addrLo := byte(addr)

	if len(payload) >= 3 && payload[0] == vaillantB509OpcodeRead && payload[1] == addrHi && payload[2] == addrLo {
		if len(payload) == 3 {
			return nil, false
		}
		return payload[3:], true
	}
	if len(payload) >= 2 && payload[0] == addrHi && payload[1] == addrLo {
		if len(payload) == 2 {
			return nil, false
		}
		return payload[2:], true
	}
	return payload, true
}

func parseB509WriteAck(payload []byte, addr uint16) bool {
	if len(payload) == 0 {
		return true
	}
	if len(payload) == 1 {
		return payload[0] != 0x00
	}

	addrHi := byte(addr >> 8)
	addrLo := byte(addr)

	if len(payload) >= 3 && payload[0] == vaillantB509OpcodeWrite && payload[1] == addrHi && payload[2] == addrLo {
		if len(payload) == 3 {
			return true
		}
		return payload[3] != 0x00
	}
	if len(payload) >= 2 && payload[0] == addrHi && payload[1] == addrLo {
		if len(payload) == 2 {
			return true
		}
		return payload[2] != 0x00
	}
	return payload[0] != 0x00
}

var (
	// Reserved for GW-2 wiring; keep implementations linked and lint-clean.
	_ = (*vaillantSemanticPoller).readB509Value
	_ = (*vaillantSemanticPoller).writeB509Value
)

func buildB524ReadSelector(opcode, group, instance byte, addr uint16) []byte {
	return []byte{
		opcode,
		vaillantB524OpRead,
		group,
		instance,
		byte(addr),
		byte(addr >> 8),
	}
}

func buildB524WriteSelector(opcode, group, instance byte, addr uint16, data []byte) []byte {
	selector := []byte{
		opcode,
		vaillantB524OpWrite,
		group,
		instance,
		byte(addr),
		byte(addr >> 8),
	}
	return append(selector, data...)
}

func parseB524ReadPayload(payload []byte, opcode, group, instance byte, addr uint16) ([]byte, bool) {
	if len(payload) == 0 {
		return nil, false
	}

	if len(payload) == 1 && payload[0] == 0x00 {
		return nil, false
	}
	if len(payload) < 4 {
		return nil, false
	}

	replyKind := payload[0]

	if len(payload) >= 5 {
		replyInstance := payload[1]
		replyGroup := payload[2]
		replyAddr := uint16(payload[3]) | uint16(payload[4])<<8
		if replyGroup == group && replyAddr == addr {
			if !matchesB524ReplyInstance(replyInstance, instance) {
				log.Printf(
					"b524 read mismatch: want opcode=0x%02x group=0x%02x instance=0x%02x addr=0x%04x; got reply-instance=0x%02x (group=0x%02x addr=0x%04x)",
					opcode,
					group,
					instance,
					addr,
					replyInstance,
					replyGroup,
					replyAddr,
				)
				return nil, false
			}
			if len(payload) == 5 {
				return nil, false
			}
			return payload[5:], true
		}
	}

	replyGroup := payload[1]
	replyAddr := uint16(payload[2]) | uint16(payload[3])<<8
	if replyGroup != group || replyAddr != addr {
		log.Printf(
			"b524 read mismatch: want opcode=0x%02x group=0x%02x instance=0x%02x addr=0x%04x; got kind=0x%02x group=0x%02x addr=0x%04x len=%d",
			opcode,
			group,
			instance,
			addr,
			replyKind,
			replyGroup,
			replyAddr,
			len(payload),
		)
		return nil, false
	}
	if len(payload) == 4 {
		return nil, false
	}
	return payload[4:], true
}

// isB524ProbeCoherent checks if a B524 read response proves the target
// understands B524 register reads. Unlike parseB524ReadPayload, this accepts
// header-only responses (no value bytes) as coherent — the device returned a
// valid B524 structure which proves capability.
func isB524ProbeCoherent(payload []byte, group byte, addr uint16) bool {
	return ebusgateway.IsB524ResponseCoherent(payload, group, addr)
}

// probeB524Register sends a single B524 read request to target and checks
// if the response is coherent. Retries up to 3 times with 200ms backoff
// to handle adapter-direct bus contention.
func (p *vaillantSemanticPoller) probeB524Register(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
	if p == nil || p.bus == nil {
		return false
	}

	p.mu.Lock()
	source := p.source
	timeout := p.requestTimeout
	p.mu.Unlock()

	// B524 probes need at least 5s per attempt in adapter-direct mode:
	// arbitration(~200ms) + frame(~220ms) + adapter-timeout(~550ms) +
	// collision-retry(~200ms) + second-attempt(~970ms) ≈ 2.1s minimum.
	// The config default SemanticRequestTimeout=2s is insufficient.
	const minB524ProbeTimeout = 5 * time.Second
	if timeout < minB524ProbeTimeout {
		timeout = minB524ProbeTimeout
	}

	// Retry probes up to 3 times — adapter-direct mode has bus contention
	// that can cause individual probes to timeout on busy buses.
	for attempt := 0; attempt < 3; attempt++ {
		// Back off between retries to avoid colliding with the same
		// bus contention pattern (50ms arbitration + waitForSyn).
		if attempt > 0 {
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return false
			}
		}

		data := buildB524ReadSelector(opcode, group, instance, addr)
		frame := protocol.Frame{
			FrameType: protocol.FrameTypeInitiatorTarget,
			Source:    source,
			Target:    target,
			Primary:   vaillantExtRegisterPrimary,
			Secondary: vaillantExtRegisterSecondary,
			Data:      data,
		}

		p.readMu.Lock()
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		response, err := p.bus.Send(reqCtx, frame)
		cancel()
		p.readMu.Unlock()

		if err != nil {
			if ctx.Err() != nil {
				return false // parent context cancelled
			}
			// Don't retry definitive rejections (NACK, no device) —
			// these are permanent, not transient bus contention.
			if ebuserrors.IsDefinitive(err) {
				return false
			}
			continue // retry on transient errors
		}
		if response == nil {
			continue
		}
		return isB524ProbeCoherent(response.Data, group, addr)
	}
	return false
}

// b524DiscoveryOptions controls how discoverB524RootWithOptions builds its
// candidate set. The default zero value (augmentStructural=false) is the
// registry-health-check semantic — answers "does the existing registry
// contain a coherent B524 root?" without probing addresses outside the
// registry. Set augmentStructural=true for the production discovery
// path used by refreshDiscovery, which unconditionally probes the
// bounded Vaillant structural target set even when the registry has
// not yet learned about the regulator.
type b524DiscoveryOptions struct {
	augmentStructural bool
}

// discoverB524Root is the production discovery path. It augments the
// registry-derived candidate list with the bounded Vaillant structural
// target set {0x08, 0x15, 0x26}, probes D1-ordered (0x15 first), and
// stops at the first coherent candidate.
//
// Structural augmentation is unconditional and idempotent: the dedup
// map collapses overlap, and the structural set is bounded to three
// non-broadcast unicast targets. The alternative — gating on registry
// size — has a hostile-registry hole where any stray non-Vaillant
// entry (e.g. {0x08 boiler, 0xEC passive-only responder}) suppresses
// the fallback exactly when it is needed.
//
// Probing structural addresses that are not yet in the registry is
// safe: the unicast probe either elicits a coherent response (in
// which case refreshDiscovery registers the controller post-
// discovery via registerStructuralControllerIfMissing) or it does
// not. Source-address invariant is upheld — the probe uses the
// existing admitted source via probeFn.
func (p *vaillantSemanticPoller) discoverB524Root(ctx context.Context) (byte, error) {
	return p.discoverB524RootWithOptions(ctx, b524DiscoveryOptions{augmentStructural: true})
}

// discoverB524RootInRegistry is the registry-health-check path. It
// considers ONLY candidates already in the registry. Used by startup
// scan helpers to ask "is the existing registry coherent?", which is
// a distinct question from "where is the regulator?".
func (p *vaillantSemanticPoller) discoverB524RootInRegistry(ctx context.Context) (byte, error) {
	return p.discoverB524RootWithOptions(ctx, b524DiscoveryOptions{augmentStructural: false})
}

func (p *vaillantSemanticPoller) discoverB524RootWithOptions(ctx context.Context, opts b524DiscoveryOptions) (byte, error) {
	if p == nil || p.reg == nil {
		return 0, fmt.Errorf("gateway.discoverB524Root: nil poller or registry")
	}

	probeFn := p.b524ProbeFn
	if probeFn == nil {
		probeFn = p.probeB524Register
	}

	// Source-address invariant applies uniformly across both
	// candidate entry points (registry iteration AND structural
	// fallback). A configured source / companion target that happens
	// to be already present in the registry — e.g. an ebusd preload
	// imports 0x26 before semantic discovery runs, or a prior
	// session left the companion address registered — would otherwise
	// bypass the structural-augmentation guard via the dedup map and
	// be probed unicast. The shared filter ensures any candidate
	// matching the admitted source or companion is dropped regardless
	// of how it entered the candidate set.
	skipReservedSourceCompanion := func(addr byte) bool {
		if addr == p.source {
			return true
		}
		if p.companion != 0 && addr == p.companion {
			return true
		}
		return false
	}
	candidateSet := make(map[byte]struct{})
	var candidates []byte
	// P9.3 — race-free routing-address enumeration via
	// IterateSnapshots + SnapshotTargetAddressForRouting. The
	// previous Iterate path called entry.AddressByRole and
	// entry.PrimaryDisplayAddress lock-free.
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		// Phase C M-C6b: B524 candidates are routing targets, not
		// display addresses. An aliased regulator entry (e.g.
		// 0x10↔0x15 displayed as 0x10) must be probed at 0x15
		// where the responder lives — probing 0x10 misses the
		// coherent responder and reports no B524 root.
		addr := ebusgateway.SnapshotTargetAddressForRouting(snap)
		if skipReservedSourceCompanion(addr) {
			return true
		}
		if _, dup := candidateSet[addr]; !dup {
			candidateSet[addr] = struct{}{}
			candidates = append(candidates, addr)
		}
		return true
	})

	if opts.augmentStructural {
		for _, addr := range ebusgateway.VaillantStructuralStartupProbeTargets {
			if _, dup := candidateSet[addr]; dup {
				continue
			}
			if skipReservedSourceCompanion(addr) {
				continue
			}
			candidateSet[addr] = struct{}{}
			candidates = append(candidates, addr)
		}
	}

	if len(candidates) == 0 {
		return 0, fmt.Errorf("gateway.discoverB524Root: no devices in registry")
	}

	slices.Sort(candidates)

	if _, has0x15 := candidateSet[0x15]; has0x15 {
		ordered := make([]byte, 0, len(candidates))
		ordered = append(ordered, 0x15)
		for _, c := range candidates {
			if c != 0x15 {
				ordered = append(ordered, c)
			}
		}
		candidates = ordered
	}

	for _, addr := range candidates {
		coherent := true
		for _, probe := range b524CapabilityProbes {
			if !probeFn(ctx, addr, probe.opcode, probe.group, probe.instance, probe.addr) {
				coherent = false
				break
			}
		}
		if coherent {
			log.Printf("semantic_b524_root_discovery address=0x%02x probed=%d", addr, len(candidates))
			return addr, nil
		}
	}

	addrs := make([]string, len(candidates))
	for i, c := range candidates {
		addrs[i] = fmt.Sprintf("0x%02x", c)
	}
	return 0, fmt.Errorf("gateway.discoverB524Root: no coherent responder among [%s]", strings.Join(addrs, ", "))
}

// ConfirmB524Coherent runs the multi-register B524 capability probe
// against target and returns true if every probe responds coherently.
// Used by the runtime passive-promotion pipeline to validate that a
// candidate address discovered via passive evidence actually
// implements the Vaillant extended-register protocol before
// registering it. The probe sources from the poller's admitted
// source — source-address invariant is preserved.
func (p *vaillantSemanticPoller) ConfirmB524Coherent(ctx context.Context, target byte) bool {
	if p == nil {
		return false
	}
	probeFn := p.b524ProbeFn
	if probeFn == nil {
		probeFn = p.probeB524Register
	}
	for _, probe := range b524CapabilityProbes {
		if !probeFn(ctx, target, probe.opcode, probe.group, probe.instance, probe.addr) {
			return false
		}
	}
	return true
}

// EnqueueDiscoveryRefresh signals the semantic poller to re-run
// discovery on the next available task slot. Used by the passive-
// promotion pipeline after a late-arrival candidate is registered,
// so the regulator surface populates without a gateway restart.
func (p *vaillantSemanticPoller) EnqueueDiscoveryRefresh() {
	if p == nil {
		return
	}
	p.enqueueTask(semanticTaskRefreshDiscovery, semanticTaskPriorityHigh, p.refreshDiscovery)
}

// EnqueueAddressIdentityProbe schedules a per-address identity
// probe for the given address. Runs registry.Scan against just
// that address using the gateway's current scan source — on
// success, the registry's M6 identity-merge path collapses
// canonical pairs that share identity into a single DeviceEntry.
//
// Phase post-C P5 (live validation 2026-05-08): closes the gap
// where passive-observed addresses (e.g. NETX3 0xF1, 0xF6, BASV2
// 0x10) sit in the registry forever with empty manufacturer /
// deviceID / serialNumber because no path probes them for
// identity post-insertion.
//
// Bounded + idempotent: probes each address at most once per
// gateway lifetime via p.identityProbedAddresses (sync.Map).
// Probes that fail (timeout, NACK, etc.) are NOT retried — the
// next gateway restart re-attempts. This is intentional: NETX3's
// 0x04 broadcast face does not respond to active probes by spec,
// so retry would just spam the bus.
func (p *vaillantSemanticPoller) EnqueueAddressIdentityProbe(addr byte) {
	if p == nil || p.bus == nil || p.reg == nil {
		return
	}
	if addr == 0 || addr == 0xFE || addr == 0xAA {
		return
	}
	// P5 round-8 (Codex P2 follow-up 2026-05-08): skip the gateway's
	// own admitted source + companion. The existing
	// discoverB524RootInRegistry candidate path explicitly drops
	// these (skipReservedSourceCompanion); this probe path must
	// honor the same protection so we don't emit identity traffic
	// to our own admitted address (or its companion).
	if addr == p.source {
		return
	}
	if p.companion != 0 && addr == p.companion {
		return
	}
	// P5 round-6 (Codex P2 follow-up 2026-05-08): resolve the input
	// address to its responder/target byte before probing. Active
	// identity reads (0x07/0x04 + B5.09 ScanID) must be addressed
	// to the responder face — sending them to an initiator/source
	// byte (e.g. BASV2 0x10, NETX3 0xF1) produces no response and
	// just times out. The existing startup scan path filters
	// initiator-capable addresses out of target lists for the same
	// reason.
	//
	// Resolution order:
	// 1. If addr is non-initiator-capable, it IS already a
	//    responder/target — probe directly.
	// 2. Otherwise, look up the entry containing addr and use
	//    TargetAddressForRouting (prefers SlotRoleSlave, falls
	//    back to PrimaryDisplayAddress when no target-role face
	//    exists).
	// 3. If neither yields a valid target, skip — initiator-only
	//    devices have no probable identity face.
	probeAddr := addr
	if protocol.IsInitiatorCapableAddress(addr) {
		// P9.3 — race-free identity-probe target resolution via
		// IterateSnapshots + SnapshotTargetAddressForRouting. The
		// semantic poller runs concurrently with passive inserter /
		// startup scan registry writes; the previous Iterate path
		// dereferenced live entry.Addresses() and TargetAddressForRouting.
		var resolvedTarget byte
		p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
			if ebusgateway.SnapshotContainsAddress(snap, addr) {
				resolvedTarget = ebusgateway.SnapshotTargetAddressForRouting(snap)
				return false
			}
			return true
		})
		if resolvedTarget == 0 || protocol.IsInitiatorCapableAddress(resolvedTarget) {
			// No target-role face on the entry, OR the entry's
			// only addresses are initiator-capable — skip.
			log.Printf("semantic_address_identity_probe address=0x%02X status=skipped reason=no_responder_face", addr)
			return
		}
		probeAddr = resolvedTarget
	}
	if _, alreadyProbed := p.identityProbedAddresses.LoadOrStore(probeAddr, struct{}{}); alreadyProbed {
		return
	}
	scheduler := p.tasks
	probeFn := func(ctx context.Context) {
		probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_, err := registry.ScanDirected(probeCtx, p.bus, p.reg, p.source, []byte{probeAddr})
		if err != nil {
			log.Printf("semantic_address_identity_probe address=0x%02X status=fail err=%v", probeAddr, err)
			return
		}
		log.Printf("semantic_address_identity_probe address=0x%02X status=ok", probeAddr)
	}
	if scheduler == nil {
		// No scheduler wired (unit tests, etc.) — run inline.
		probeFn(context.Background())
		return
	}
	// Codex P2 follow-up on PR #583 (2026-05-08): submit BEFORE
	// committing the per-address probed marker. If the task queue
	// is overloaded and submit fails, roll back the sync.Map entry
	// so the address remains eligible for retry on the next passive
	// observation. Without rollback, an overloaded queue at startup
	// permanently blocks identity enrichment for the dropped
	// addresses until the next gateway restart.
	err := scheduler.submit(semanticTaskPriorityLow, func(taskCtx context.Context) {
		p.withPollLock(taskCtx, probeFn)
	})
	if err != nil {
		// Rollback so the (resolved) address is eligible for retry.
		p.identityProbedAddresses.Delete(probeAddr)
		if errors.Is(err, errSemanticTaskQueueOverloaded) {
			log.Printf("semantic_address_identity_probe address=0x%02X status=enqueue_dropped reason=queue_overloaded", probeAddr)
			return
		}
		log.Printf("semantic_address_identity_probe address=0x%02X status=enqueue_failed err=%v", probeAddr, err)
	}
}

// registerStructuralControllerIfMissing registers a minimal Vaillant
// device entry for a B524-coherent controller address that is not yet
// in the registry. This closes the gap where discoverB524Root converges
// via the structural-fallback path (probing 0x15 / 0x26 even when they
// are absent from the registry) — the address must surface in
// GraphQL devices and on the router plane after discovery.
//
// The minimal entry is safe: the address has just satisfied the B524
// multi-register coherency check, which is strong evidence of a
// Vaillant regulator. Registry de-dupes on address, so this is a
// no-op for already-known controllers. Manufacturer is the only
// identity field set; richer fields (DeviceID, SerialNumber,
// SoftwareVersion) are populated naturally by subsequent identity
// reads or by the discovery promotion pipeline once that lands.
// registerStructuralControllerIfMissing returns true iff the registry
// gained a new entry as a result of this call (false when the
// controller was already known, was zero, or the poller was nil).
// Callers use the return value to decide whether downstream caches
// (e.g. regulator capability) need recomputation.
func (p *vaillantSemanticPoller) registerStructuralControllerIfMissing(controller byte) bool {
	if p == nil || p.reg == nil || controller == 0 {
		return false
	}
	// Phase C M-C6b: containment must check the full address set,
	// not just PrimaryDisplayAddress. A controller (e.g. 0x15) that
	// is already an alias face on a registered entry (e.g. an
	// aliased 0x10↔0x15 regulator displayed as 0x10) must be
	// treated as already-known, otherwise the function violates
	// its idempotence contract and re-registers + re-refreshes
	// router planes on every discovery cycle.
	// P9.3 — race-free address-membership check via IterateSnapshots.
	// The semantic poller runs concurrently with passive inserter /
	// startup scan / identity probe goroutines that mutate the
	// registry; the previous Iterate path read entry.Addresses() on
	// a live *deviceEntry pointer, racing with concurrent Register /
	// AliasAddresses writes.
	already := false
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if ebusgateway.SnapshotContainsAddress(snap, controller) {
			already = true
			return false
		}
		return true
	})
	if already {
		return false
	}
	p.reg.Register(registry.DeviceInfo{
		Address:      controller,
		Manufacturer: "Vaillant",
	})
	log.Printf("semantic_controller_registry_register address=0x%02x source=b524_structural_fallback", controller)
	if p.routerPlanesRefreshFn != nil {
		p.routerPlanesRefreshFn()
	}
	return true
}

// enrichRegulatorIdentity returns enrichment metadata for the B524 root device.
// Unknown identity is acceptable and does not block discovery.
func (p *vaillantSemanticPoller) enrichRegulatorIdentity(addr byte) *regulatorEnrichment {
	if p == nil || p.reg == nil {
		return nil
	}

	// Phase C M-C6b: discoverB524Root returns the routed controller
	// face (e.g. 0x15 for an aliased 0x10↔0x15 regulator). The
	// existing entry is displayed as 0x10, so matching only
	// PrimaryDisplayAddress would miss it and the enrichment
	// metadata/logging would be lost for the aliased path. Use the
	// full address-set membership check instead.
	//
	// P9.3 — race-free via IterateSnapshots; reads entry's DeviceID
	// from the value-typed snapshot (immune to concurrent Register
	// writes that would torn-read the deviceID string).
	var (
		matched  bool
		deviceID string
	)
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if ebusgateway.SnapshotContainsAddress(snap, addr) {
			matched = true
			deviceID = snap.DeviceID
			return false
		}
		return true
	})
	if !matched {
		return nil
	}

	normalizedID := normalizeDeviceID(deviceID)
	families := []string{"BASV2", "BASS2", "CTLV2", "CTLS2", "E7C00"}
	var family string
	for _, f := range families {
		if strings.HasPrefix(normalizedID, f) {
			family = f
			break
		}
	}

	return &regulatorEnrichment{
		family:   family,
		deviceID: deviceID,
	}
}

func matchesB524ReplyInstance(replyInstance, requestedInstance byte) bool {
	if replyInstance == requestedInstance {
		return true
	}
	if requestedInstance < 0xFF && replyInstance == requestedInstance+1 {
		return true
	}
	return false
}

func (p *vaillantSemanticPoller) writeB524Value(ctx context.Context, opcode, group, instance byte, addr uint16, data []byte) error {
	if p == nil || (p.bus == nil && p.sendFrameFn == nil) {
		return fmt.Errorf("b524 write unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	target := p.controller
	timeout := p.requestTimeout
	p.mu.Unlock()
	if target == 0 {
		return fmt.Errorf("b524 write target is zero")
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		p.readMu.Lock()
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		request := protocol.Frame{
			FrameType: protocol.FrameTypeInitiatorTarget,
			Source:    source,
			Target:    target,
			Primary:   vaillantExtRegisterPrimary,
			Secondary: vaillantExtRegisterSecondary,
			Data:      buildB524WriteSelector(opcode, group, instance, addr, data),
		}
		response, err := p.sendSemanticFrame(reqCtx, request)
		cancel()
		p.readMu.Unlock()

		if err != nil {
			lastErr = err
		} else if response != nil {
			// B524 write responses are typically short ACKs (< 4 bytes payload).
			return nil
		} else {
			lastErr = fmt.Errorf("b524 write returned nil response")
		}

		if attempt < 2 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(75 * time.Millisecond):
			}
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("b524 write failed")
	}
	return lastErr
}

func (p *vaillantSemanticPoller) readB524ValueLive(ctx context.Context, opcode, group, instance byte, addr uint16) ([]byte, bool) {
	if p == nil || (p.bus == nil && p.sendFrameFn == nil) {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	target := p.controller
	timeout := p.requestTimeout
	p.mu.Unlock()
	if target == 0 {
		return nil, false
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	for attempt := 0; attempt < 3; attempt++ {
		p.readMu.Lock()
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		request := protocol.Frame{
			FrameType: protocol.FrameTypeInitiatorTarget,
			Source:    source,
			Target:    target,
			Primary:   vaillantExtRegisterPrimary,
			Secondary: vaillantExtRegisterSecondary,
			Data:      buildB524ReadSelector(opcode, group, instance, addr),
		}
		response, err := p.sendSemanticFrame(reqCtx, request)
		cancel()
		p.readMu.Unlock()

		if err == nil && response != nil {
			payload, ok := parseB524ReadPayload(response.Data, opcode, group, instance, addr)
			if ok {
				return payload, true
			}
		}

		if attempt < 2 {
			select {
			case <-ctx.Done():
				return nil, false
			case <-time.After(75 * time.Millisecond):
			}
		}
	}
	return nil, false
}

func (p *vaillantSemanticPoller) readB524Float32LE(ctx context.Context, opcode, group, instance byte, addr uint16) (float64, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || len(raw) < 4 {
		return 0, false
	}
	bits := binary.LittleEndian.Uint32(raw[:4])
	value := float64(math.Float32frombits(bits))
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return value, true
}

func (p *vaillantSemanticPoller) readB524U8(ctx context.Context, opcode, group, instance byte, addr uint16) *uint8 {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || len(raw) == 0 {
		return nil
	}
	if raw[0] == 0xFF {
		return nil
	}
	v := raw[0]
	return &v
}

func (p *vaillantSemanticPoller) readB524U16(ctx context.Context, opcode, group, instance byte, addr uint16) *uint16 {
	v, ok := p.readB524Uint16(ctx, opcode, group, instance, addr)
	if !ok || v == nil {
		return nil
	}
	if *v == 0xFFFF {
		return nil
	}
	return v
}

func (p *vaillantSemanticPoller) readB524F32(ctx context.Context, opcode, group, instance byte, addr uint16) *float64 {
	v, ok := p.readB524Float32LE(ctx, opcode, group, instance, addr)
	if !ok {
		return nil
	}
	return &v
}

func (p *vaillantSemanticPoller) readB524Bool(ctx context.Context, opcode, group, instance byte, addr uint16) *bool {
	v := p.readB524U8(ctx, opcode, group, instance, addr)
	if v == nil {
		return nil
	}
	if *v > 1 {
		return nil
	}
	value := *v == 1
	return &value
}

func (p *vaillantSemanticPoller) readB524Firmware(ctx context.Context, opcode, group, instance byte, addr uint16) *string {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok {
		return nil
	}
	return decodeB524FirmwareVersion(raw)
}

func decodeB524FirmwareVersion(raw []byte) *string {
	if len(raw) < 3 {
		return nil
	}
	major, minor, patch := raw[0], raw[1], raw[2]
	if major == 0xFF && minor == 0xFF && patch == 0xFF {
		return nil
	}
	v := fmt.Sprintf("%02d.%02d.%02d", major, minor, patch)
	return &v
}

func (p *vaillantSemanticPoller) readB524CString(ctx context.Context, opcode, group, instance byte, addr uint16) (string, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || len(raw) == 0 {
		return "", false
	}
	trimmed := raw
	for i, b := range trimmed {
		if b == 0x00 {
			trimmed = trimmed[:i]
			break
		}
	}
	return string(trimmed), true
}

func (p *vaillantSemanticPoller) readB524CStringSanitized(ctx context.Context, opcode, group, instance byte, addr uint16) (string, bool) {
	value, ok := p.readB524CString(ctx, opcode, group, instance, addr)
	if !ok {
		return "", false
	}
	sanitized := make([]byte, len(value))
	for i := 0; i < len(value); i++ {
		if value[i] > 0x7F {
			sanitized[i] = '?'
		} else {
			sanitized[i] = value[i]
		}
	}
	return string(sanitized), true
}

func (p *vaillantSemanticPoller) readB524DateHDA3(ctx context.Context, opcode, group, instance byte, addr uint16) (string, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || len(raw) < 3 {
		return "", false
	}
	day, month, yearOffset := int(raw[0]), int(raw[1]), int(raw[2])
	if day < 1 || day > 31 || month < 1 || month > 12 || yearOffset > 99 {
		return "", false
	}
	year := 2000 + yearOffset
	iso := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	return iso, true
}

func (p *vaillantSemanticPoller) readB524ZoneNamePart(ctx context.Context, instance byte, addr uint16) (string, bool) {
	raw, ok := p.readB524CString(ctx, localZones.opcode, localZones.group, instance, addr)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(raw), true
}

func (p *vaillantSemanticPoller) readB524Uint16(ctx context.Context, opcode, group, instance byte, addr uint16) (*uint16, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || len(raw) == 0 {
		return nil, false
	}
	parsed, ok := decodeB524Uint16(raw)
	if !ok {
		return nil, false
	}
	return &parsed, true
}

func (p *vaillantSemanticPoller) readB524Uint32LE(ctx context.Context, opcode, group, instance byte, addr uint16) (uint32, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || len(raw) < 4 {
		return 0, false
	}
	v := binary.LittleEndian.Uint32(raw[:4])
	if v == 0xFFFFFFFF {
		return 0, false
	}
	return v, true
}

func resolveAssociatedCircuitInstance(roomTemperatureZoneMapping *uint16, zoneInstance byte) byte {
	if roomTemperatureZoneMapping == nil {
		return zoneInstance
	}
	value := *roomTemperatureZoneMapping
	switch value {
	case 0xFF, 0xFFFF, 0:
		return zoneInstance
	default:
		if value >= 1 && value <= 0x20 {
			return byte(value - 1)
		}
		return zoneInstance
	}
}

func deriveZoneModeAndPreset(opMode, specialFunction, circuitType *uint16, hasCircuitType bool) (string, string, []string) {
	allowed := deriveZoneAllowedModes(circuitType, hasCircuitType)
	var mode string
	switch value := uint16Value(opMode, 1); value {
	case 0:
		mode = "off"
	case 1:
		mode = "auto"
	case 2:
		mode = deriveManualOperatingMode(circuitType, hasCircuitType)
	default:
		mode = "auto"
	}

	preset := derivePresetFromSpecialFunctionAndOpMode(specialFunction, opMode)
	if preset == "" {
		switch mode {
		case "auto":
			preset = "schedule"
		default:
			preset = "manual"
		}
	}
	return mode, preset, allowed
}

func hasMeaningfulSpecialFunction(specialFunction *uint16) bool {
	return normalizeSpecialFunctionToken(specialFunction) != ""
}

func deriveDhwModeAndPreset(opMode, specialFunction *uint16) (string, string) {
	var mode string
	switch value := uint16Value(opMode, 1); value {
	case 0:
		mode = "off"
	case 1:
		mode = "auto"
	case 2:
		mode = "heat"
	default:
		mode = "auto"
	}
	preset := derivePresetFromSpecialFunctionAndOpMode(specialFunction, opMode)
	if preset == "" {
		switch mode {
		case "auto":
			preset = "schedule"
		default:
			preset = "manual"
		}
	}
	return mode, preset
}

func derivePresetFromSpecialFunctionAndOpMode(specialFunction, opMode *uint16) string {
	if token := normalizeSpecialFunctionToken(specialFunction); token != "" {
		switch token {
		case "quickveto":
			return "quickveto"
		case "away":
			return "away"
		}
	}
	switch uint16Value(opMode, 1) {
	case 1:
		return "schedule"
	case 2:
		return "manual"
	}
	return ""
}

func normalizeSpecialFunctionToken(specialFunction *uint16) string {
	if specialFunction == nil {
		return ""
	}
	switch *specialFunction {
	case 2:
		return "quickveto"
	case 3, 4:
		return "away"
	default:
		return ""
	}
}

func deriveManualOperatingMode(circuitType *uint16, hasCircuitType bool) string {
	if !hasCircuitType || circuitType == nil {
		return "heat"
	}
	switch *circuitType {
	case 2:
		return "cool"
	default:
		return "heat"
	}
}

func deriveZoneAllowedModes(circuitType *uint16, hasCircuitType bool) []string {
	if !hasCircuitType || circuitType == nil {
		return []string{"off", "auto", "heat"}
	}
	switch *circuitType {
	case 0:
		return []string{"off"}
	case 2:
		return []string{"off", "auto", "cool"}
	default:
		return []string{"off", "auto", "heat"}
	}
}

func decodeRadioDeviceModel(classAddress *uint8) string {
	if classAddress == nil {
		return ""
	}
	switch *classAddress {
	case 0x15:
		return "VRC720"
	case 0x26:
		return "VR71/FM5"
	case 0x35:
		return "VR92"
	default:
		return fmt.Sprintf("Unknown (0x%02X)", *classAddress)
	}
}

func hasRemoteIdentityEvidence(classAddress *uint8, firmware *string, hardware *uint16) bool {
	if classAddress != nil && *classAddress == 0x26 {
		return true
	}
	if firmware != nil && strings.TrimSpace(*firmware) != "" {
		return true
	}
	if hardware != nil && *hardware != 0 && *hardware != 0xFFFF {
		return true
	}
	return false
}

func deriveZoneHvacAction(valveStatus, circuitType *uint16, hasCircuitType bool) string {
	if valveStatus == nil {
		return ""
	}
	switch *valveStatus {
	case 0:
		return "idle"
	case 1:
		if hasCircuitType && circuitType != nil && *circuitType == 2 {
			return "cooling"
		}
		return "heating"
	default:
		return ""
	}
}

func uint16Value(value *uint16, fallback uint16) uint16 {
	if value == nil {
		return fallback
	}
	return *value
}

func formatUintToken(value uint16) string {
	return fmt.Sprintf("%d", value)
}

func decodeB524Uint16(payload []byte) (uint16, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	if len(payload) == 1 {
		return uint16(payload[0]), true
	}
	return binary.LittleEndian.Uint16(payload[:2]), true
}

func composeZoneName(primary, prefix, suffix string) string {
	if primary = strings.TrimSpace(primary); primary != "" {
		return primary
	}
	parts := make([]string, 0, 2)
	if prefix = strings.TrimSpace(prefix); prefix != "" {
		parts = append(parts, prefix)
	}
	if suffix = strings.TrimSpace(suffix); suffix != "" {
		parts = append(parts, suffix)
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

// readB555Frame sends a B555 frame and returns the responder payload.
func (p *vaillantSemanticPoller) readB555Frame(ctx context.Context, data []byte) ([]byte, bool) {
	if p == nil || p.bus == nil {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	target := p.controller
	timeout := p.requestTimeout
	p.mu.Unlock()

	if target == 0 {
		return nil, false
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	p.readMu.Lock()
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	request := protocol.Frame{
		FrameType: protocol.FrameTypeInitiatorTarget,
		Source:    source,
		Target:    target,
		Primary:   vaillantB555Primary,
		Secondary: vaillantB555Secondary,
		Data:      data,
	}
	response, err := p.bus.Send(reqCtx, request)
	cancel()
	p.readMu.Unlock()

	if err != nil || response == nil {
		return nil, false
	}
	return response.Data, true
}

func (p *vaillantSemanticPoller) writeB555Frame(ctx context.Context, data []byte) ([]byte, bool) {
	if p == nil || p.bus == nil {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	target := p.controller
	timeout := p.requestTimeout
	p.mu.Unlock()

	if target == 0 {
		return nil, false
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	p.readMu.Lock()
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	request := protocol.Frame{
		FrameType: protocol.FrameTypeInitiatorTarget,
		Source:    source,
		Target:    target,
		Primary:   vaillantB555Primary,
		Secondary: vaillantB555Secondary,
		Data:      data,
	}
	response, err := p.bus.Send(reqCtx, request)
	cancel()
	p.readMu.Unlock()

	if err != nil || response == nil {
		return nil, false
	}
	return response.Data, true
}

func (p *vaillantSemanticPoller) writeB555TimerSlot(ctx context.Context, zone, hc, weekday, slotIdx, slotCount, startHour, startMinute, endHour, endMinute byte, temperatureRaw uint16) (byte, error) {
	data := []byte{
		b555OpcodeTimerWrite,
		zone, hc, weekday,
		slotIdx, slotCount,
		startHour, startMinute,
		endHour, endMinute,
		byte(temperatureRaw & 0xFF), byte(temperatureRaw >> 8),
	}
	resp, ok := p.writeB555Frame(ctx, data)
	if !ok {
		return 0xFF, fmt.Errorf("b555 timer write: no response")
	}
	if len(resp) < 1 {
		return 0xFF, fmt.Errorf("b555 timer write: empty response")
	}
	return resp[0], nil
}

var b555ErrorDescriptions = map[byte]string{
	0x00: "accepted",
	0x01: "parameter out of range",
	0x03: "timer unavailable",
	0x06: "validation failure",
}

func b555ErrorDesc(code byte) string {
	if desc, ok := b555ErrorDescriptions[code]; ok {
		return desc
	}
	return fmt.Sprintf("unknown error 0x%02X", code)
}

func (p *vaillantSemanticPoller) setZoneTimeProgram(ctx context.Context, zone int, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	if p == nil {
		return nil, fmt.Errorf("schedule writer unavailable")
	}
	if zone < 0 || zone > 2 {
		return &mcp.TimeProgramWriteResult{Success: false, Error: "zone must be 0-2"}, nil
	}
	if weekday < 0 || weekday > 6 {
		return &mcp.TimeProgramWriteResult{Success: false, Error: "weekday must be 0-6"}, nil
	}
	if len(slots) == 0 {
		return &mcp.TimeProgramWriteResult{Success: false, Error: "slots array must not be empty"}, nil
	}

	cfg := p.readB555Config(ctx, byte(zone), b555HCHeating)
	if cfg == nil {
		return &mcp.TimeProgramWriteResult{Success: false, Error: "unable to read B555 config for zone heating"}, nil
	}

	maxSlots := cfg.MaxSlots
	if maxSlots <= 0 {
		maxSlots = 3
	}
	if len(slots) > maxSlots {
		return &mcp.TimeProgramWriteResult{
			Success: false,
			Error:   fmt.Sprintf("slot count %d exceeds max_slots %d", len(slots), maxSlots),
		}, nil
	}

	minTemp := 5.0
	maxTemp := 30.0
	if cfg.MinTempC != nil {
		minTemp = *cfg.MinTempC
	}
	if cfg.MaxTempC != nil {
		maxTemp = *cfg.MaxTempC
	}

	for i, slot := range slots {
		if slot.StartHour < 0 || slot.StartHour > 24 {
			return &mcp.TimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: start_hour %d out of range 0-24", i, slot.StartHour)}, nil
		}
		if slot.StartMinute < 0 || slot.StartMinute > 59 {
			return &mcp.TimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: start_minute %d out of range 0-59", i, slot.StartMinute)}, nil
		}
		if slot.EndHour < 0 || slot.EndHour > 24 {
			return &mcp.TimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: end_hour %d out of range 0-24", i, slot.EndHour)}, nil
		}
		if slot.EndMinute < 0 || slot.EndMinute > 59 {
			return &mcp.TimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: end_minute %d out of range 0-59", i, slot.EndMinute)}, nil
		}
		if slot.TemperatureC == nil {
			return &mcp.TimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: temperature_c is required for heating", i)}, nil
		}
		if *slot.TemperatureC < minTemp || *slot.TemperatureC > maxTemp {
			return &mcp.TimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: temperature_c %.1f out of range [%.0f, %.0f]", i, *slot.TemperatureC, minTemp, maxTemp)}, nil
		}
	}

	result := &mcp.TimeProgramWriteResult{
		Success:     true,
		SlotResults: make([]mcp.TimeProgramSlotResult, len(slots)),
	}

	for i, slot := range slots {
		tempRaw := uint16(math.Round(*slot.TemperatureC * 10))
		code, err := p.writeB555TimerSlot(ctx,
			byte(zone), b555HCHeating, byte(weekday),
			byte(i), byte(len(slots)),
			byte(slot.StartHour), byte(slot.StartMinute),
			byte(slot.EndHour), byte(slot.EndMinute),
			tempRaw,
		)
		if err != nil {
			result.SlotResults[i] = mcp.TimeProgramSlotResult{
				SlotIndex: i,
				Accepted:  false,
				ErrorCode: 0xFF,
				ErrorDesc: err.Error(),
			}
			result.Success = false
			continue
		}
		accepted := code == 0x00
		if !accepted {
			result.Success = false
		}
		result.SlotResults[i] = mcp.TimeProgramSlotResult{
			SlotIndex: i,
			Accepted:  accepted,
			ErrorCode: int(code),
			ErrorDesc: b555ErrorDesc(code),
		}
	}

	go p.refreshScheduleForProgram(context.Background(), byte(zone), b555HCHeating, byte(weekday))

	return result, nil
}

func (p *vaillantSemanticPoller) setDhwTimeProgram(ctx context.Context, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	if p == nil {
		return nil, fmt.Errorf("schedule writer unavailable")
	}
	if weekday < 0 || weekday > 6 {
		return &mcp.TimeProgramWriteResult{Success: false, Error: "weekday must be 0-6"}, nil
	}
	if len(slots) == 0 {
		return &mcp.TimeProgramWriteResult{Success: false, Error: "slots array must not be empty"}, nil
	}

	cfg := p.readB555Config(ctx, b555ZoneDHW, b555HCDHW)
	if cfg == nil {
		return &mcp.TimeProgramWriteResult{Success: false, Error: "unable to read B555 config for DHW"}, nil
	}

	maxSlots := cfg.MaxSlots
	if maxSlots <= 0 {
		maxSlots = 3
	}
	if len(slots) > maxSlots {
		return &mcp.TimeProgramWriteResult{
			Success: false,
			Error:   fmt.Sprintf("slot count %d exceeds max_slots %d", len(slots), maxSlots),
		}, nil
	}

	minTemp := 35.0
	maxTemp := 65.0
	if cfg.MinTempC != nil {
		minTemp = *cfg.MinTempC
	}
	if cfg.MaxTempC != nil {
		maxTemp = *cfg.MaxTempC
	}

	for i, slot := range slots {
		if slot.StartHour < 0 || slot.StartHour > 24 {
			return &mcp.TimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: start_hour %d out of range 0-24", i, slot.StartHour)}, nil
		}
		if slot.StartMinute < 0 || slot.StartMinute > 59 {
			return &mcp.TimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: start_minute %d out of range 0-59", i, slot.StartMinute)}, nil
		}
		if slot.EndHour < 0 || slot.EndHour > 24 {
			return &mcp.TimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: end_hour %d out of range 0-24", i, slot.EndHour)}, nil
		}
		if slot.EndMinute < 0 || slot.EndMinute > 59 {
			return &mcp.TimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: end_minute %d out of range 0-59", i, slot.EndMinute)}, nil
		}
		if slot.TemperatureC != nil {
			if *slot.TemperatureC < minTemp || *slot.TemperatureC > maxTemp {
				return &mcp.TimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: temperature_c %.1f out of range [%.0f, %.0f]", i, *slot.TemperatureC, minTemp, maxTemp)}, nil
			}
		}
	}

	result := &mcp.TimeProgramWriteResult{
		Success:     true,
		SlotResults: make([]mcp.TimeProgramSlotResult, len(slots)),
	}

	for i, slot := range slots {
		var tempRaw uint16
		if slot.TemperatureC != nil {
			tempRaw = uint16(math.Round(*slot.TemperatureC * 10))
		} else {
			tempRaw = 0xFFFF
		}
		code, err := p.writeB555TimerSlot(ctx,
			b555ZoneDHW, b555HCDHW, byte(weekday),
			byte(i), byte(len(slots)),
			byte(slot.StartHour), byte(slot.StartMinute),
			byte(slot.EndHour), byte(slot.EndMinute),
			tempRaw,
		)
		if err != nil {
			result.SlotResults[i] = mcp.TimeProgramSlotResult{
				SlotIndex: i,
				Accepted:  false,
				ErrorCode: 0xFF,
				ErrorDesc: err.Error(),
			}
			result.Success = false
			continue
		}
		accepted := code == 0x00
		if !accepted {
			result.Success = false
		}
		result.SlotResults[i] = mcp.TimeProgramSlotResult{
			SlotIndex: i,
			Accepted:  accepted,
			ErrorCode: int(code),
			ErrorDesc: b555ErrorDesc(code),
		}
	}

	go p.refreshScheduleForProgram(context.Background(), b555ZoneDHW, b555HCDHW, byte(weekday))

	return result, nil
}

func (p *vaillantSemanticPoller) SetZoneTimeProgram(ctx context.Context, zone int, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	return p.setZoneTimeProgram(ctx, zone, weekday, slots)
}

func (p *vaillantSemanticPoller) SetDhwTimeProgram(ctx context.Context, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	return p.setDhwTimeProgram(ctx, weekday, slots)
}

func (p *vaillantSemanticPoller) refreshScheduleForProgram(ctx context.Context, zone, hc, weekday byte) {
	if p == nil || p.bus == nil || p.provider == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	cfg := p.readB555Config(ctx, zone, hc)
	if cfg == nil {
		return
	}

	maxSlots := cfg.MaxSlots
	if maxSlots <= 0 {
		maxSlots = 3
	}

	slotsPerDay, _ := p.readB555Slots(ctx, zone, hc)

	slotCount := maxSlots
	if slotsPerDay != nil && int(weekday) < len(slotsPerDay) && slotsPerDay[weekday] < slotCount {
		slotCount = slotsPerDay[weekday]
	}

	var timerSlots []graphql.ScheduleTimerSlot
	for ss := 0; ss < slotCount; ss++ {
		slot := p.readB555Timer(ctx, zone, hc, weekday, byte(ss))
		if slot == nil {
			break
		}
		timerSlots = append(timerSlots, *slot)
	}

	status := p.provider.Schedules()
	if status == nil {
		return
	}

	hcName := b555HCNames[hc]
	if hcName == "" {
		hcName = fmt.Sprintf("unknown_%d", hc)
	}
	zoneIdx := int(zone)
	if zone == b555ZoneDHW {
		zoneIdx = 255
	}

	for i, prog := range status.Programs {
		if prog.Zone == zoneIdx && prog.HC == hcName {
			if int(weekday) < len(prog.Days) {
				status.Programs[i].Days[weekday] = graphql.ScheduleDayProgram{
					Weekday: b555WeekdayNames[weekday],
					Slots:   timerSlots,
				}
			}
			if slotsPerDay != nil {
				status.Programs[i].SlotsUsed = slotsPerDay
			}
			p.provider.SetSchedules(status)
			return
		}
	}
}

// readB555Config reads a B555 A3 config response for a zone+HC.
func (p *vaillantSemanticPoller) readB555Config(ctx context.Context, zone, hc byte) *graphql.ScheduleConfig {
	data, ok := p.readB555Frame(ctx, []byte{b555OpcodeConfigRead, zone, hc})
	if !ok || len(data) < 9 {
		return nil
	}
	status := data[0]
	if status != 0x00 {
		return nil
	}
	cfg := &graphql.ScheduleConfig{
		MaxSlots:       int(data[1]),
		TimeResolution: int(data[2]),
		MinDuration:    int(data[3]),
		HasTemperature: data[4] != 0,
		TempSlots:      int(data[5]),
	}
	// A3 layout: status(1)+max_slots(1)+time_res(1)+min_dur(1)+has_temp(1)+temp_slots(1)+min_temp(1)+max_temp(1)+pad(1) = 9 bytes
	// min_temp and max_temp are UCH (whole °C), sentinel 0xFF = unavailable.
	if data[6] != 0xFF {
		v := float64(data[6])
		cfg.MinTempC = &v
	}
	if data[7] != 0xFF {
		v := float64(data[7])
		cfg.MaxTempC = &v
	}
	return cfg
}

// readB555Slots reads B555 A4 slots-per-day response for a zone+HC.
func (p *vaillantSemanticPoller) readB555Slots(ctx context.Context, zone, hc byte) ([]int, bool) {
	data, ok := p.readB555Frame(ctx, []byte{b555OpcodeSlotsRead, zone, hc})
	if !ok || len(data) < 9 {
		return nil, false
	}
	status := data[0]
	if status != 0x00 {
		return nil, false
	}
	// A4 layout: status(1)+Mon(1)+Tue(1)+Wed(1)+Thu(1)+Fri(1)+Sat(1)+Sun(1)+pad(1) = 9 bytes
	slots := make([]int, 7)
	for i := 0; i < 7 && i+1 < len(data); i++ {
		slots[i] = int(data[i+1])
	}
	return slots, true
}

// readB555Timer reads a single timer slot from B555 A5.
func (p *vaillantSemanticPoller) readB555Timer(ctx context.Context, zone, hc, weekday, slotIdx byte) *graphql.ScheduleTimerSlot {
	data, ok := p.readB555Frame(ctx, []byte{b555OpcodeTimerRead, zone, hc, weekday, slotIdx})
	if !ok || len(data) < 7 {
		return nil
	}
	status := data[0]
	if status != 0x00 {
		return nil
	}
	slot := &graphql.ScheduleTimerSlot{
		StartHour:   int(data[1]),
		StartMinute: int(data[2]),
		EndHour:     int(data[3]),
		EndMinute:   int(data[4]),
	}
	tempRaw := uint16(data[5]) | uint16(data[6])<<8
	if tempRaw != 0xFFFF {
		v := float64(tempRaw) / 10.0
		slot.TemperatureC = &v
		rawInt := int(tempRaw)
		slot.TemperatureRaw = &rawInt
	}
	return slot
}

// b555ProgramSpec defines a (zone, hc) combination to poll.
type b555ProgramSpec struct {
	zone byte
	hc   byte
}

// refreshSchedules polls B555 timer schedules and publishes to the semantic provider.
func (p *vaillantSemanticPoller) refreshSchedules(ctx context.Context) {
	if p == nil || p.bus == nil || p.provider == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	// Define which (zone, HC) combinations to poll.
	// Per-zone programs: zone 0-2 × heating/cooling
	// Zone-agnostic programs: 0xFF × DHW/CC/Silent
	specs := []b555ProgramSpec{
		{zone: 0x00, hc: b555HCHeating},
		{zone: 0x01, hc: b555HCHeating},
		{zone: 0x02, hc: b555HCHeating},
		{zone: 0x00, hc: b555HCCooling},
		{zone: 0x01, hc: b555HCCooling},
		{zone: 0x02, hc: b555HCCooling},
		{zone: b555ZoneDHW, hc: b555HCDHW},
		{zone: b555ZoneDHW, hc: b555HCCC},
		{zone: b555ZoneDHW, hc: b555HCSilent},
	}

	var programs []graphql.ScheduleProgram

	for _, spec := range specs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Step 1: Read config (A3)
		cfg := p.readB555Config(ctx, spec.zone, spec.hc)
		if cfg == nil {
			continue
		}

		hcName := b555HCNames[spec.hc]
		if hcName == "" {
			hcName = fmt.Sprintf("unknown_%d", spec.hc)
		}
		zoneIdx := int(spec.zone)
		if spec.zone == b555ZoneDHW {
			zoneIdx = 255
		}

		prog := graphql.ScheduleProgram{
			Zone:   zoneIdx,
			HC:     hcName,
			Config: cfg,
		}

		// Step 2: Read slots per day (A4)
		slotsPerDay, ok := p.readB555Slots(ctx, spec.zone, spec.hc)
		if ok {
			prog.SlotsUsed = slotsPerDay
		}

		// Step 3: Read timer entries (A5) for each day/slot
		maxSlots := cfg.MaxSlots
		if maxSlots <= 0 {
			maxSlots = 3
		}

		days := make([]graphql.ScheduleDayProgram, 7)
		for dd := byte(0); dd < 7; dd++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			day := graphql.ScheduleDayProgram{
				Weekday: b555WeekdayNames[dd],
			}

			slotCount := maxSlots
			if ok && int(dd) < len(slotsPerDay) && slotsPerDay[dd] < slotCount {
				slotCount = slotsPerDay[dd]
			}

			for ss := 0; ss < slotCount; ss++ {
				slot := p.readB555Timer(ctx, spec.zone, spec.hc, dd, byte(ss))
				if slot == nil {
					break
				}
				day.Slots = append(day.Slots, *slot)
			}
			days[dd] = day
		}
		prog.Days = days

		programs = append(programs, prog)
	}

	if len(programs) > 0 {
		p.provider.SetSchedules(&graphql.ScheduleStatus{
			Programs: programs,
		})
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
