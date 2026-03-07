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

	vaillantGroupDHW       = byte(0x01)
	vaillantGroupCircuits  = byte(0x02)
	vaillantGroupZones     = byte(0x03)
	vaillantGroupSolar     = byte(0x04)
	vaillantGroupCylinders = byte(0x05)
	vaillantGroupRadio09   = byte(0x09)
	vaillantGroupRadio10   = byte(0x0A)
	vaillantGroupRadio0C   = byte(0x0C)

	zoneRegName                          = uint16(0x0016)
	zoneRegNamePrefix                    = uint16(0x0017)
	zoneRegNameSuffix                    = uint16(0x0018)
	zoneRegIndex                         = uint16(0x001C)
	zoneRegHeatingOpMode                 = uint16(0x0006) // configuration.heating.operation_mode
	zoneRegCurrentTemp                   = uint16(0x000F) // state.current_room_temperature
	zoneRegTargetTemp                    = uint16(0x0022) // configuration.heating.desired_setpoint
	zoneRegFallbackManualTemp            = uint16(0x0014) // configuration.heating.manual_mode_setpoint
	zoneRegSpecialFunction               = uint16(0x000E) // state.current_special_function
	zoneRegValveStatus                   = uint16(0x0012) // state.valve_status
	zoneRegRoomTemperatureZoneMappingRaw = uint16(0x0013) // configuration.room_temperature_zone_mapping
	zoneRegCurrentHumidity               = uint16(0x0028) // state.current_room_humidity

	circuitRegType            = uint16(0x0002) // configuration.heating_circuit_type / mixer_circuit_type_external
	circuitRegCoolingEnabled  = uint16(0x0006) // cooling_enabled
	circuitRegFlowSetpoint    = uint16(0x0007) // flow_setpoint
	circuitRegFlowTemp        = uint16(0x0008) // flow_temperature (VF[x])
	circuitRegHeatingCurve    = uint16(0x000F) // heating_curve
	circuitRegFlowTempMax     = uint16(0x0010) // flow_temperature_max
	circuitRegFlowTempMin     = uint16(0x0012) // flow_temperature_min
	circuitRegSummerLimit     = uint16(0x0014) // summer_limit
	circuitRegRoomTempControl = uint16(0x0015) // room_temperature_control_mode
	circuitRegCircuitState    = uint16(0x001B) // circuit_state
	circuitRegFrostProtection = uint16(0x001D) // frost_protection_threshold
	circuitRegPumpStatus      = uint16(0x001E) // pump_status
	circuitRegCalcFlowTemp    = uint16(0x0020) // calculated_flow_temperature
	circuitRegMixerPosition   = uint16(0x0021) // mixer_position_pct
	circuitRegHumidity        = uint16(0x0022) // room_humidity_pct
	circuitRegDewPoint        = uint16(0x0023) // dew_point_temperature
	circuitRegPumpHours       = uint16(0x0024) // pump_operating_hours
	circuitRegPumpStarts      = uint16(0x0025) // pump_starts

	dhwRegOperationMode   = uint16(0x0003) // configuration.domestic_hot_water.operation_mode
	dhwRegTargetTemp      = uint16(0x0004) // configuration.domestic_hot_water.tapping_setpoint
	dhwRegCurrentTemp     = uint16(0x0005) // state.current_dhw_temperature
	dhwRegSpecialFunction = uint16(0x000D) // state.current_special_function
	dhwInstance           = byte(0x00)

	radioRegDeviceConnected      = uint16(0x0001)
	radioRegDeviceClassAddress   = uint16(0x0002)
	radioRegDeviceFirmware       = uint16(0x0004)
	radioRegRoomHumidity         = uint16(0x0007)
	radioRegRoomTemperature      = uint16(0x000F)
	radioRegRemoteControlAddress = uint16(0x0019)
	radioRegDevicePaired         = uint16(0x001E)
	radioRegReceptionStrength    = uint16(0x001F)
	radioRegHardwareIdentifier   = uint16(0x0023)
	radioRegZoneAssignment       = uint16(0x0025)

	solarInstance = byte(0x00)

	solarRegEnabled       = uint16(0x0001)
	solarRegFunctionMode  = uint16(0x0002)
	solarRegCollectorTemp = uint16(0x0003)
	solarRegReturnTemp    = uint16(0x0007)
	solarRegPumpActive    = uint16(0x0008)
	solarRegCurrentYield  = uint16(0x0009)
	solarRegPumpHours     = uint16(0x000B)

	cylinderRegMaxSetpoint      = uint16(0x0001)
	cylinderRegChargeHysteresis = uint16(0x0002)
	cylinderRegChargeOffset     = uint16(0x0003)
	cylinderRegTemperature      = uint16(0x0004)
)

// B5.24 energy registers (VRC 720f TSP 15.720, group=0, instance=0).
// energy4 type = ULG (unsigned 32-bit LE) in kWh.
const (
	energyRegFuelSumHc    = uint16(0x0056) // PrFuelSumHc: total gas consumption heating
	energyRegEnergySumHc  = uint16(0x0057) // PrEnergySumHc: total electricity consumption heating
	energyRegEnergySumHwc = uint16(0x0058) // PrEnergySumHwc: total electricity consumption hot water
	energyRegFuelSumHwc   = uint16(0x0059) // PrFuelSumHwc: total gas consumption hot water
	energyGroup           = byte(0x00)
	energyInstance        = byte(0x00)
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
)

func startVaillantSemanticPolling(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub) *vaillantSemanticPoller {
	if gateway == nil || gateway.Bus == nil || gateway.Registry == nil || provider == nil {
		return nil
	}

	cacheStore := newSemanticCacheStore(cfg.SemanticCachePath, log.Printf)
	cacheSnapshot, cacheLoaded := preloadSemanticCache(provider, cacheStore)
	poller := newVaillantSemanticPoller(cfg, gateway, provider, hub, cacheStore)
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

	transportConfig ebusgateway.TransportConfig

	source               byte
	requestTimeout       time.Duration
	discoveryInterval    time.Duration
	configInterval       time.Duration
	stateInterval        time.Duration
	energyInterval       time.Duration
	boilerFastInterval   time.Duration
	boilerMediumInterval time.Duration
	boilerSlowInterval   time.Duration
	zoneMissThreshold    int
	zoneHitThreshold     int
	dhwStaleTTL          time.Duration

	pollMu sync.Mutex
	readMu sync.Mutex

	catalog    productids.Catalog
	catalogErr error

	mu                       sync.Mutex
	controller               byte
	boilerAddress            byte
	regulatorCapability      productids.ControllerCapability
	regAbsenceState          regulatorAbsenceState
	regAbsenceSince          time.Time
	registryDeviceCount      int
	regulatorRecheckInterval time.Duration
	regulatorAbsenceGrace    time.Duration
	zones                    map[byte]*vaillantZoneSnapshot
	presence                 map[byte]*zonePresenceRecord
	dhw                      *vaillantDhwSnapshot
	dhwLastUpdateAt          time.Time
	boiler                   *vaillantBoilerSnapshot
	system                   *vaillantSystemSnapshot
	circuits                 map[byte]*vaillantCircuitSnapshot
	radioDevices             map[radioDeviceKey]*vaillantRadioDeviceSnapshot
	fm5Mode                  graphql.Fm5SemanticMode
	solar                    *vaillantSolarSnapshot
	solarCylinders           map[byte]*vaillantCylinderSnapshot

	refreshFromEbusdGrabFn func(context.Context) (map[byte]bool, bool)
	nowFn                  func() time.Time
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

	FieldFreshness map[semanticFieldKey]semanticFieldFreshness
}

type vaillantDhwSnapshot struct {
	OperatingMode string
	Preset        string

	CurrentTempC *float64
	TargetTempC  *float64

	ConfigurationDHWOperationMode string
	StateSpecialFunction          string

	FieldFreshness map[semanticFieldKey]semanticFieldFreshness
}

type vaillantCircuitSnapshot struct {
	Instance byte
	Active   bool

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
	FrostProtectionC   *float64
	PumpStatusRaw      *uint16
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
	dhwFieldOperatingMode                  semanticFieldKey = "dhw.operating_mode"
	dhwFieldPreset                         semanticFieldKey = "dhw.preset"
	dhwFieldCurrentTempC                   semanticFieldKey = "dhw.current_temp_c"
	dhwFieldTargetTempC                    semanticFieldKey = "dhw.target_temp_c"
	dhwFieldSpecialFunctionRaw             semanticFieldKey = "dhw.special_function_raw"
	dhwFieldDhwOperationModeRaw            semanticFieldKey = "dhw.operation_mode_raw"
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

var (
	zoneConfigFieldSet = newSemanticFieldSet(
		zoneFieldName,
	)
	zoneStateFieldSet = newSemanticFieldSet(
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
	)
)

func newVaillantSemanticPoller(cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub, cache semanticCachePersister) *vaillantSemanticPoller {
	catalog, catalogErr := productids.LoadCatalog()
	poller := &vaillantSemanticPoller{
		scheduler:       ebusgateway.NewSemanticReadScheduler(),
		tasks:           newSemanticTaskScheduler(),
		reg:             gateway.Registry,
		bus:             gateway.Bus,
		provider:        provider,
		hub:             hub,
		cache:           cache,
		transportConfig: cfg.TransportConfig,
		source:          cfg.ScanSource,

		requestTimeout:       cfg.SemanticRequestTimeout,
		discoveryInterval:    cfg.SemanticDiscoveryInterval,
		configInterval:       cfg.SemanticConfigInterval,
		stateInterval:        cfg.SemanticStateInterval,
		energyInterval:       cfg.SemanticEnergyInterval,
		boilerFastInterval:   30 * time.Second,
		boilerMediumInterval: 5 * time.Minute,
		boilerSlowInterval:   10 * time.Minute,
		zoneMissThreshold:    cfg.SemanticZonePresenceMissThreshold,
		zoneHitThreshold:     cfg.SemanticZonePresenceHitThreshold,
		dhwStaleTTL:          cfg.SemanticDHWStaleTTL,

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
	poller.scheduler.SetCircuitBreaker(ebusgateway.SemanticReadCircuitBreakerOptions{
		FailureBudget:      cfg.SemanticReadBreakerFailureBudget,
		OpenCooldown:       cfg.SemanticReadBreakerOpenCooldown,
		HalfOpenProbeLimit: cfg.SemanticReadBreakerHalfOpenProbeLimit,
		OnTransition:       poller.onSemanticReadBreakerTransition,
		OnSuppressed:       poller.onSemanticReadBreakerSuppressed,
	})
	return poller
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
		p.enqueueTask(schedule.priority, p.boilerStatusTierTask(schedule.tier))
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
	p.enqueueTask(semanticTaskPriorityHigh, p.refreshConfig)
	p.enqueueTask(semanticTaskPriorityMedium, p.refreshCircuits)
	p.enqueueTask(semanticTaskPriorityMedium, p.refreshSystem)
	p.enqueueTask(semanticTaskPriorityMedium, p.refreshRadioDevices)
	p.enqueueTask(semanticTaskPriorityMedium, p.refreshEnergy)
}

func (p *vaillantSemanticPoller) Start(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	go p.tasks.run(ctx)

	// Discovery owns downstream controller/boiler priming and avoids duplicate startup bursts.
	p.enqueueTask(semanticTaskPriorityHigh, p.refreshDiscovery)
	p.enqueueTask(semanticTaskPriorityHigh, p.refreshState)

	go p.runLoop(ctx, p.regulatorRecheckInterval, semanticTaskPriorityLow, p.refreshRegulatorCapability)
	go p.runLoop(ctx, p.discoveryInterval, semanticTaskPriorityLow, p.refreshDiscovery)
	go p.runLoop(ctx, p.configInterval, semanticTaskPriorityMedium, p.refreshConfig)
	go p.runLoop(ctx, p.stateInterval, semanticTaskPriorityHigh, p.refreshState)
	go p.runLoop(ctx, p.configInterval, semanticTaskPriorityLow, p.refreshCircuits)
	go p.runLoop(ctx, p.configInterval, semanticTaskPriorityLow, p.refreshSystem)
	go p.runLoop(ctx, p.configInterval, semanticTaskPriorityLow, p.refreshRadioDevices)
	go p.runLoop(ctx, p.energyInterval, semanticTaskPriorityMedium, p.refreshEnergy)
	for _, schedule := range p.boilerStatusTierSchedules() {
		go p.runLoop(ctx, schedule.interval, schedule.priority, p.boilerStatusTierTask(schedule.tier))
	}
}

func (p *vaillantSemanticPoller) runLoop(ctx context.Context, interval time.Duration, priority semanticTaskPriority, fn func(context.Context)) {
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
			p.enqueueTask(priority, fn)
		}
	}
}

func (p *vaillantSemanticPoller) enqueueTask(priority semanticTaskPriority, fn func(context.Context)) {
	if p == nil || fn == nil {
		return
	}
	scheduler := p.tasks
	if scheduler == nil {
		fn(context.Background())
		return
	}
	err := scheduler.submit(priority, func(taskCtx context.Context) {
		p.withPollLock(taskCtx, fn)
	})
	if errors.Is(err, errSemanticTaskQueueOverloaded) {
		log.Printf("semantic poll scheduler overloaded: skipping task (priority=%d)", priority)
		return
	}
	if err != nil {
		log.Printf("semantic poll scheduler submit failed: %v", err)
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
		p.enqueueTask(semanticTaskPriorityLow, p.refreshDiscovery)
	}
}

func (p *vaillantSemanticPoller) refreshDiscovery(ctx context.Context) {
	// Regulator capability is always recomputed, even when BASV is missing.
	regCap := p.findRegulatorCapability()
	boilerAddress := p.findBoilerAddress()

	controller, ok := findDeviceAddressByPrefix(p.reg, "BASV")
	if !ok {
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
		p.radioDevices = make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
		p.fm5Mode = graphql.Fm5SemanticModeAbsent
		p.solar = nil
		p.solarCylinders = make(map[byte]*vaillantCylinderSnapshot)
		semanticZoneCount.Set(0)
		p.mu.Unlock()
		if regCap != prev {
			log.Printf("semantic_regulator_capability capability=%s", regCap.String())
		}
		if prevController != 0 {
			log.Printf("semantic_controller_discovery address=0x00 source=missing")
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

	p.mu.Lock()
	prev := p.regulatorCapability
	prevController := p.controller
	prevBoilerAddress := p.boilerAddress
	p.controller = controller
	p.boilerAddress = boilerAddress
	p.regulatorCapability = regCap
	p.mu.Unlock()
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

	present := make(map[byte]bool, 4)
	checked := make(map[byte]bool, 11)
	for instance := byte(0x00); instance <= 0x0A; instance++ {
		indexBytes, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegIndex)
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
	if len(present) == 0 {
		return semanticSnapshotSourceCache
	}
	return semanticSnapshotSourceLive
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
		return
	}

	liveReadSuccess := false
	for _, instance := range zones {
		primaryName, primaryOK := p.readB524ZoneNamePart(ctx, instance, zoneRegName)
		prefix, prefixOK := p.readB524ZoneNamePart(ctx, instance, zoneRegNamePrefix)
		suffix, suffixOK := p.readB524ZoneNamePart(ctx, instance, zoneRegNameSuffix)
		if primaryOK || prefixOK || suffixOK {
			liveReadSuccess = true
		}

		incoming := &vaillantZoneSnapshot{
			Name: composeZoneName(primaryName, prefix, suffix),
		}
		p.mu.Lock()
		if entry := p.zones[instance]; entry != nil {
			mergeZoneSnapshotFields(entry, incoming, semanticSnapshotSourceLive, zoneConfigFieldSet)
		}
		p.mu.Unlock()
	}
	if !liveReadSuccess && p.tryRefreshFromEbusdGrab(ctx) {
		grabHydrated = true
	}

	source := semanticSnapshotSourceCache
	if liveReadSuccess || grabHydrated {
		source = semanticSnapshotSourceLive
	}
	p.publishZones(source)
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
		var (
			currentPtr *float64
			targetPtr  *float64
			humidity   *float64
		)
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegCurrentTemp); ok {
			current := value
			currentPtr = &current
			liveReadSuccess = true
		}

		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegTargetTemp); ok {
			target := value
			targetPtr = &target
			liveReadSuccess = true
		}
		if targetPtr == nil {
			if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegFallbackManualTemp); ok {
				target := value
				targetPtr = &target
				liveReadSuccess = true
			}
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegCurrentHumidity); ok {
			currentHumidity := value
			humidity = &currentHumidity
			liveReadSuccess = true
		}

		zoneOpMode, zoneOpModeOK := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegHeatingOpMode)
		zoneSF, zoneSFOK := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegSpecialFunction)
		zoneValve, zoneValveOK := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegValveStatus)
		zoneRoomTemperatureZoneMappingRaw, zoneRoomTemperatureZoneMappingRawOK := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegRoomTemperatureZoneMappingRaw)
		if zoneOpModeOK || zoneSFOK || zoneValveOK || zoneRoomTemperatureZoneMappingRawOK {
			liveReadSuccess = true
		}
		circuitInstance := resolveAssociatedCircuitInstance(zoneRoomTemperatureZoneMappingRaw, instance)
		circuitType, hasCircuitType := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, circuitInstance, circuitRegType)
		if hasCircuitType {
			liveReadSuccess = true
		}
		var associatedCircuitRaw *uint16
		if zoneRoomTemperatureZoneMappingRaw != nil {
			value := uint16(circuitInstance)
			associatedCircuitRaw = &value
		}

		operatingMode, preset, allowedModes := deriveZoneModeAndPreset(zoneOpMode, zoneSF, circuitType, hasCircuitType)
		hvacAction := deriveZoneHvacAction(zoneValve, circuitType, hasCircuitType)
		incoming := &vaillantZoneSnapshot{
			OperatingMode: operatingMode,
			Preset:        preset,
			HvacAction:    hvacAction,
			AllowedModes:  allowedModes,
			CurrentTempC:  currentPtr,
			TargetTempC:   targetPtr,
			HumidityPct:   humidity,
			ConfigurationRoomTemperatureZoneMappingRaw: zoneRoomTemperatureZoneMappingRaw,
			ConfigurationAssociatedCircuitRaw:          associatedCircuitRaw,
			ConfigurationCircuitTypeRaw:                circuitType,
			StateValveStatusRaw:                        zoneValve,
		}
		if zoneOpMode != nil {
			incoming.ConfigurationHeatingOperationMode = formatUintToken(*zoneOpMode)
		}
		if zoneSF != nil {
			incoming.StateSpecialFunction = formatUintToken(*zoneSF)
		}

		p.mu.Lock()
		if entry := p.zones[instance]; entry != nil {
			mergeZoneSnapshotFields(entry, incoming, semanticSnapshotSourceLive, zoneStateFieldSet)
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

		zone := graphql.Zone{
			ID:   fmt.Sprintf("zone-%d", instance+1),
			Name: name,
			State: graphql.ZoneState{
				CurrentTempC:       entry.CurrentTempC,
				CurrentHumidityPct: entry.HumidityPct,
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
				AssociatedCircuit:          decodeAssociatedCircuit(entry.ConfigurationAssociatedCircuitRaw),
				RoomTemperatureZoneMapping: decodeRoomTemperatureZoneMapping(entry.ConfigurationRoomTemperatureZoneMappingRaw),
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
	return true
}

type b524EnergyQuery struct {
	addr    uint16 // B5.24 register address
	channel string // EnergyMergeKey.Channel
	usage   string // EnergyMergeKey.Usage
}

var b524EnergyQueries = []b524EnergyQuery{
	{energyRegFuelSumHc, "gas", "climate"},
	{energyRegFuelSumHwc, "gas", "hot_water"},
	{energyRegEnergySumHc, "electricity", "climate"},
	{energyRegEnergySumHwc, "electricity", "hot_water"},
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
		val, ok := p.readB524Uint32LE(ctx, vaillantB524OpcodeLocal, energyGroup, energyInstance, q.addr)
		if !ok {
			failed++
			continue
		}
		kwh := float64(val)

		// Write all-time total as year/current.
		if p.provider.ApplyEnergyFromRegister(graphql.EnergyMergeKey{
			Channel: q.channel, Usage: q.usage, Period: "year", YearKind: "current",
		}, kwh) {
			accepted++
		}
		// Lock day and year/previous with register-priority 0 to prevent
		// broadcast double-counting (all-time total already includes them).
		p.provider.ApplyEnergyFromRegister(graphql.EnergyMergeKey{
			Channel: q.channel, Usage: q.usage, Period: "year", YearKind: "previous",
		}, 0)
		p.provider.ApplyEnergyFromRegister(graphql.EnergyMergeKey{
			Channel: q.channel, Usage: q.usage, Period: "day", YearKind: "",
		}, 0)
	}
	if accepted > 0 || failed > 0 {
		log.Printf("semantic energy b524: accepted=%d failed=%d", accepted, failed)
	}
}

func (p *vaillantSemanticPoller) refreshDHW(ctx context.Context) semanticSnapshotSource {
	controller, _ := p.snapshotZones()
	if controller == 0 {
		if found, ok := findDeviceAddressByPrefix(p.reg, "BASV"); ok {
			p.mu.Lock()
			p.controller = found
			p.mu.Unlock()
			controller = found
		}
	}
	if controller == 0 {
		return p.sourceFromEbusdGrab(p.refreshDHWFromEbusdGrab(ctx))
	}

	attempted := make(semanticFieldSet)
	liveReadSuccess := false
	currentPtr := p.readDhwFloat(ctx, dhwRegCurrentTemp)
	if currentPtr != nil {
		liveReadSuccess = true
		attempted[dhwFieldCurrentTempC] = struct{}{}
	}
	targetPtr := p.readDhwFloat(ctx, dhwRegTargetTemp)
	if targetPtr != nil {
		liveReadSuccess = true
		attempted[dhwFieldTargetTempC] = struct{}{}
	}
	opModeRaw, opModeOK := p.readDhwUint16(ctx, dhwRegOperationMode)
	sfModeRaw, sfModeOK := p.readDhwUint16(ctx, dhwRegSpecialFunction)
	if opModeOK || sfModeOK {
		liveReadSuccess = true
		attempted[dhwFieldOperatingMode] = struct{}{}
		attempted[dhwFieldPreset] = struct{}{}
	}
	if opModeOK {
		attempted[dhwFieldDhwOperationModeRaw] = struct{}{}
	}
	if sfModeOK {
		attempted[dhwFieldSpecialFunctionRaw] = struct{}{}
	}

	if !liveReadSuccess {
		return p.sourceFromEbusdGrab(p.refreshDHWFromEbusdGrab(ctx))
	}

	status := &vaillantDhwSnapshot{
		CurrentTempC: currentPtr,
		TargetTempC:  targetPtr,
	}
	if attempted.has(dhwFieldOperatingMode) || attempted.has(dhwFieldPreset) {
		status.OperatingMode, status.Preset = deriveDhwModeAndPreset(opModeRaw, sfModeRaw)
	}
	if opModeRaw != nil {
		status.ConfigurationDHWOperationMode = formatUintToken(*opModeRaw)
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

func (p *vaillantSemanticPoller) readDhwFloat(ctx context.Context, addr uint16) *float64 {
	for _, opcode := range []byte{vaillantB524OpcodeLocal, vaillantB524OpcodeRead} {
		value, ok := p.readB524Float32LE(ctx, opcode, vaillantGroupDHW, dhwInstance, addr)
		if !ok {
			continue
		}
		floatValue := value
		return &floatValue
	}
	return nil
}

func (p *vaillantSemanticPoller) readDhwUint16(ctx context.Context, addr uint16) (*uint16, bool) {
	for _, opcode := range []byte{vaillantB524OpcodeLocal, vaillantB524OpcodeRead} {
		value, ok := p.readB524Uint16(ctx, opcode, vaillantGroupDHW, dhwInstance, addr)
		if ok {
			return value, true
		}
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
			OperatingMode: snapshot.OperatingMode,
			Preset:        snapshot.Preset,
			TargetTempC:   snapshot.TargetTempC,
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

	p.mu.Lock()
	controllerPresent := p.controller != 0
	p.mu.Unlock()
	if !controllerPresent {
		return
	}

	discovered := make(map[byte]*uint16, 4)
	inactive := make(map[byte]struct{})
	probeSuccess := false

	for instance := byte(0x00); instance <= 0x0A; instance++ {
		circuitTypeRaw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegType)
		if !ok || circuitTypeRaw == nil {
			continue
		}
		probeSuccess = true
		switch *circuitTypeRaw {
		case 0x0000, 0x00FF, 0xFFFF:
			inactive[instance] = struct{}{}
		default:
			discovered[instance] = cloneUint16Ptr(circuitTypeRaw)
		}
	}
	if !probeSuccess {
		return
	}

	updates := make(map[byte]*vaillantCircuitSnapshot, len(discovered))
	anyRead := false
	for instance, circuitTypeRaw := range discovered {
		snapshot := &vaillantCircuitSnapshot{
			Instance:       instance,
			Active:         true,
			CircuitTypeRaw: cloneUint16Ptr(circuitTypeRaw),
		}
		anyRead = true

		if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegCoolingEnabled); ok && raw != nil {
			snapshot.CoolingEnabledRaw = cloneUint16Ptr(raw)
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegFlowSetpoint); ok {
			v := value
			snapshot.FlowSetpointC = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegFlowTemp); ok {
			v := value
			snapshot.FlowTemperatureC = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegHeatingCurve); ok {
			v := value
			snapshot.HeatingCurve = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegFlowTempMax); ok {
			v := value
			snapshot.FlowTempMaxC = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegFlowTempMin); ok {
			v := value
			snapshot.FlowTempMinC = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegSummerLimit); ok {
			v := value
			snapshot.SummerLimitC = &v
			anyRead = true
		}
		if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegRoomTempControl); ok && raw != nil {
			snapshot.RoomTempControlRaw = cloneUint16Ptr(raw)
			anyRead = true
		}
		if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegCircuitState); ok && raw != nil {
			snapshot.CircuitStateRaw = cloneUint16Ptr(raw)
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegFrostProtection); ok {
			v := value
			snapshot.FrostProtectionC = &v
			anyRead = true
		}
		if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegPumpStatus); ok && raw != nil {
			snapshot.PumpStatusRaw = cloneUint16Ptr(raw)
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegCalcFlowTemp); ok {
			v := value
			snapshot.CalcFlowTempC = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegMixerPosition); ok {
			v := value
			snapshot.MixerPositionPct = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegHumidity); ok {
			v := value
			snapshot.HumidityPct = &v
			anyRead = true
		}
		if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegDewPoint); ok {
			v := value
			snapshot.DewPointC = &v
			anyRead = true
		}
		if value, ok := p.readB524Uint32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegPumpHours); ok {
			v := value
			snapshot.PumpHoursRaw = &v
			anyRead = true
		}
		if value, ok := p.readB524Uint32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, instance, circuitRegPumpStarts); ok {
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
	p.mu.Unlock()

	source := semanticSnapshotSourceCache
	if anyRead || len(inactive) > 0 {
		source = semanticSnapshotSourceLive
	}
	p.publishCircuits(source)
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
	}
	if incoming.FrostProtectionC != nil {
		merged.FrostProtectionC = cloneFloat64Ptr(incoming.FrostProtectionC)
	}
	if incoming.PumpStatusRaw != nil {
		merged.PumpStatusRaw = cloneUint16Ptr(incoming.PumpStatusRaw)
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
		FrostProtectionC:   cloneFloat64Ptr(snapshot.FrostProtectionC),
		PumpStatusRaw:      cloneUint16Ptr(snapshot.PumpStatusRaw),
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

func decodeCircuitStateToken(raw *uint16) string {
	if raw == nil {
		return ""
	}
	return formatUintToken(*raw)
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
	vaillantGroupRegulator = byte(0x00)
	regulatorInstance      = byte(0x00)

	// System state registers (GG=0x00, II=0x00).
	systemRegSystemOff                    = uint16(0x0007)
	systemRegSystemWaterPressure          = uint16(0x0039)
	systemRegSystemFlowTemperature        = uint16(0x004B)
	systemRegOutdoorTemperature           = uint16(0x0073)
	systemRegOutdoorTemperatureAvg24h     = uint16(0x0095)
	systemRegMaintenanceDue               = uint16(0x0096)
	systemRegHwcCylinderTemperatureTop    = uint16(0x009D)
	systemRegHwcCylinderTemperatureBottom = uint16(0x009E)

	// System config registers (GG=0x00, II=0x00).
	systemRegAdaptiveHeatingCurve         = uint16(0x0014)
	systemRegAlternativePoint             = uint16(0x0022)
	systemRegHeatingCircuitBivalencePoint = uint16(0x0023)
	systemRegDhwBivalencePoint            = uint16(0x0001)
	systemRegHcEmergencyTemperature       = uint16(0x0026)
	systemRegHwcMaxFlowTempDesired        = uint16(0x0046)
	systemRegMaxRoomHumidity              = uint16(0x000E)

	// System properties registers (GG=0x00, II=0x00).
	systemRegSystemScheme            = uint16(0x0036)
	systemRegModuleConfigurationVR71 = uint16(0x002F)

	// Boiler B509 direct registers on BAI00.
	boilerB509RegWaterPressure         = uint16(0x0200)
	boilerB509RegFlameActive           = uint16(0x0500)
	boilerB509RegPartloadHcKW          = uint16(0x0704)
	boilerB509RegPartloadHwcKW         = uint16(0x0804)
	boilerB509RegFlowsetHcMaxC         = uint16(0x0E04)
	boilerB509RegFlowsetHcMaxCFallback = uint16(0xA500)
	boilerB509RegFlowsetHwcMaxC        = uint16(0x0F04)
	boilerB509RegPumpHours             = uint16(0x1400)
	boilerB509RegFlowTemperature       = uint16(0x1800)
	boilerB509RegFanHours              = uint16(0x1B00)
	boilerB509RegDeactivationsIFC      = uint16(0x1F00)
	boilerB509RegDeactivationsLimit    = uint16(0x2000)
	boilerB509RegDhwHours              = uint16(0x2200)
	boilerB509RegDhwStarts             = uint16(0x2300)
	boilerB509RegTargetFanSpeedRpm     = uint16(0x2400)
	boilerB509RegCentralHeatingHours   = uint16(0x2800)
	boilerB509RegCentralHeatingStarts  = uint16(0x2900)
	boilerB509RegModulationPct         = uint16(0x2E00)
	boilerB509RegFlowTempDesiredC      = uint16(0x3900)
	boilerB509RegExternalPumpActive    = uint16(0x3F00)
	boilerB509RegCentralHeatingPump    = uint16(0x4400)
	boilerB509RegDiverterValvePosition = uint16(0x5400)
	boilerB509RegDhwWaterFlowLpm       = uint16(0x5500)
	boilerB509RegDhwDemandActive       = uint16(0x5800)
	boilerB509RegCirculationPumpActive = uint16(0x7B00)
	boilerB509RegFanSpeedRpm           = uint16(0x8300)
	boilerB509RegStorageLoadPumpPct    = uint16(0x9E00)
	boilerB509RegIonisationVoltageUa   = uint16(0xA400)
	boilerB509RegStateNumber           = uint16(0xAB00)
	boilerB509RegGasValveActive        = uint16(0xBB00)
	boilerB509RegHeatingSwitchActive   = uint16(0xF203)
	boilerB509RegDhwTempDesiredC       = uint16(0xEA03)
	boilerB509RegPrimaryCircuitFlowLpm = uint16(0xFB00)
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
}

type vaillantSystemSnapshot struct {
	// State
	SystemOff                    *bool
	SystemWaterPressure          *float64
	SystemFlowTemperature        *float64
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
				opcode:   vaillantB524OpcodeLocal,
				group:    vaillantGroupRegulator,
				instance: regulatorInstance,
				addr:     systemRegSystemFlowTemperature,
			},
			{
				field:    boilerStatusFieldPumpActive,
				decoder:  boilerStatusRegisterDecoderUint16Bool,
				opcode:   vaillantB524OpcodeLocal,
				group:    vaillantGroupCircuits,
				instance: 0x00,
				addr:     circuitRegPumpStatus,
			},
			{
				field:    boilerStatusFieldHeatingStatusRaw,
				decoder:  boilerStatusRegisterDecoderUint16Int,
				opcode:   vaillantB524OpcodeLocal,
				group:    vaillantGroupCircuits,
				instance: 0x00,
				addr:     circuitRegCircuitState,
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

	// Preserve last-known values on transient/partial failures.
	if !updated {
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

	if tier != boilerStatusTierFast {
		return updated
	}

	if value := p.readDhwFloat(ctx, dhwRegCurrentTemp); value != nil {
		snapshot.DhwTemperatureC = value
		updated = true
	}
	if value := p.readDhwFloat(ctx, dhwRegTargetTemp); value != nil {
		snapshot.DhwTargetTemperatureC = value
		updated = true
	}
	if raw, ok := p.readDhwUint16(ctx, dhwRegOperationMode); ok && raw != nil {
		value := int(*raw)
		snapshot.DhwOperatingMode = &value
		updated = true
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
		if value := p.readB509DATA2c(ctx, boilerAddress, boilerB509RegFlowTemperature); value != nil {
			snapshot.FlowTemperatureC = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boilerB509RegCentralHeatingPump); value != nil {
			snapshot.CentralHeatingPumpActive = value
			updated = true
		}
		if value := p.readB509DATA2b(ctx, boilerAddress, boilerB509RegWaterPressure); value != nil {
			snapshot.WaterPressureBar = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boilerB509RegFlameActive); value != nil {
			snapshot.FlameActive = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boilerB509RegGasValveActive); value != nil {
			snapshot.GasValveActive = value
			updated = true
		}
		if value := p.readB509UINInt(ctx, boilerAddress, boilerB509RegFanSpeedRpm); value != nil {
			snapshot.FanSpeedRpm = value
			updated = true
		}
		if value := p.readB509SINScaled(ctx, boilerAddress, boilerB509RegModulationPct, 10); value != nil {
			snapshot.ModulationPct = value
			updated = true
		}
		if value := p.readB509UCHInt(ctx, boilerAddress, boilerB509RegStateNumber); value != nil {
			snapshot.StateNumber = value
			updated = true
		}
		if value := p.readB509UCHFloat(ctx, boilerAddress, boilerB509RegDiverterValvePosition); value != nil {
			snapshot.DiverterValvePositionPct = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boilerB509RegDhwDemandActive); value != nil {
			snapshot.DhwDemandActive = value
			updated = true
		}
		if value := p.readB509UIN100(ctx, boilerAddress, boilerB509RegDhwWaterFlowLpm); value != nil {
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
		if value := p.readB509Percent0(ctx, boilerAddress, boilerB509RegStorageLoadPumpPct); value != nil {
			snapshot.StorageLoadPumpPct = value
			updated = true
		}
		if value := p.readB509DATA2c(ctx, boilerAddress, boilerB509RegFlowTempDesiredC); value != nil {
			snapshot.FlowTempDesiredC = value
			updated = true
		}
		if value := p.readB509DATA2c(ctx, boilerAddress, boilerB509RegDhwTempDesiredC); value != nil {
			snapshot.DhwTempDesiredC = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boilerB509RegCirculationPumpActive); value != nil {
			snapshot.CirculationPumpActive = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boilerB509RegExternalPumpActive); value != nil {
			snapshot.ExternalPumpActive = value
			updated = true
		}
		if value := p.readB509OnOff(ctx, boilerAddress, boilerB509RegHeatingSwitchActive); value != nil {
			snapshot.HeatingSwitchActive = value
			updated = true
		}
		if value := p.readB509UINInt(ctx, boilerAddress, boilerB509RegTargetFanSpeedRpm); value != nil {
			snapshot.TargetFanSpeedRpm = value
			updated = true
		}
		if value := p.readB509SINScaled(ctx, boilerAddress, boilerB509RegIonisationVoltageUa, 10); value != nil {
			snapshot.IonisationVoltageUa = value
			updated = true
		}
		if value := p.readB509UIN100(ctx, boilerAddress, boilerB509RegPrimaryCircuitFlowLpm); value != nil {
			snapshot.PrimaryCircuitFlowLpm = value
			updated = true
		}
	case boilerStatusTierSlow:
		if value := p.readB509Hoursum2(ctx, boilerAddress, boilerB509RegCentralHeatingHours); value != nil {
			snapshot.CentralHeatingHours = value
			updated = true
		}
		if value := p.readB509Hoursum2(ctx, boilerAddress, boilerB509RegDhwHours); value != nil {
			snapshot.DhwHours = value
			updated = true
		}
		if value := p.readB509UINInt(ctx, boilerAddress, boilerB509RegCentralHeatingStarts); value != nil {
			snapshot.CentralHeatingStarts = value
			updated = true
		}
		if value := p.readB509UINInt(ctx, boilerAddress, boilerB509RegDhwStarts); value != nil {
			snapshot.DhwStarts = value
			updated = true
		}
		if value := p.readB509Hoursum2(ctx, boilerAddress, boilerB509RegPumpHours); value != nil {
			snapshot.PumpHours = value
			updated = true
		}
		if value := p.readB509Hoursum2(ctx, boilerAddress, boilerB509RegFanHours); value != nil {
			snapshot.FanHours = value
			updated = true
		}
		if value := p.readB509UCHInt(ctx, boilerAddress, boilerB509RegDeactivationsIFC); value != nil {
			snapshot.DeactivationsIFC = value
			updated = true
		}
		if value := p.readB509UCHInt(ctx, boilerAddress, boilerB509RegDeactivationsLimit); value != nil {
			snapshot.DeactivationsTemplimiter = value
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
			FlowsetHcMaxC:  snapshot.FlowsetHcMaxC,
			FlowsetHwcMaxC: snapshot.FlowsetHwcMaxC,
			PartloadHcKW:   snapshot.PartloadHcKW,
			PartloadHwcKW:  snapshot.PartloadHwcKW,
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

	snapshot := &vaillantSystemSnapshot{}
	updated := false

	if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegSystemOff); ok && raw != nil {
		value := *raw != 0
		snapshot.SystemOff = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegSystemWaterPressure); ok {
		snapshot.SystemWaterPressure = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegSystemFlowTemperature); ok {
		snapshot.SystemFlowTemperature = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegOutdoorTemperature); ok {
		snapshot.OutdoorTemperature = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegOutdoorTemperatureAvg24h); ok {
		snapshot.OutdoorTemperatureAvg24h = &value
		updated = true
	}
	if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegMaintenanceDue); ok && raw != nil {
		value := *raw != 0
		snapshot.MaintenanceDue = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegHwcCylinderTemperatureTop); ok {
		snapshot.HwcCylinderTemperatureTop = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegHwcCylinderTemperatureBottom); ok {
		snapshot.HwcCylinderTemperatureBottom = &value
		updated = true
	}

	if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegAdaptiveHeatingCurve); ok && raw != nil {
		value := *raw != 0
		snapshot.AdaptiveHeatingCurve = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegAlternativePoint); ok {
		snapshot.AlternativePoint = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegHeatingCircuitBivalencePoint); ok {
		snapshot.HeatingCircuitBivalencePoint = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegDhwBivalencePoint); ok {
		snapshot.DhwBivalencePoint = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegHcEmergencyTemperature); ok {
		snapshot.HcEmergencyTemperature = &value
		updated = true
	}
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegHwcMaxFlowTempDesired); ok {
		snapshot.HwcMaxFlowTempDesired = &value
		updated = true
	}
	if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegMaxRoomHumidity); ok && raw != nil {
		snapshot.MaxRoomHumidity = cloneUint16Ptr(raw)
		updated = true
	}

	if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegSystemScheme); ok && raw != nil {
		snapshot.SystemScheme = cloneUint16Ptr(raw)
		updated = true
	}
	if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, systemRegModuleConfigurationVR71); ok && raw != nil {
		snapshot.ModuleConfigurationVR71 = cloneUint16Ptr(raw)
		updated = true
	}

	if !updated {
		return
	}

	p.mu.Lock()
	p.system = mergeSystemSnapshotNonDestructive(p.system, snapshot)
	p.mu.Unlock()

	p.publishSystem(semanticSnapshotSourceLive)
	p.refreshFM5Semantic(ctx)
}

func (p *vaillantSemanticPoller) refreshRadioDevices(ctx context.Context) {
	if p == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	discovered := make(map[radioDeviceKey]*vaillantRadioDeviceSnapshot)
	readAny := false
	for _, group := range []byte{vaillantGroupRadio09, vaillantGroupRadio10, vaillantGroupRadio0C} {
		for instance := byte(0x00); instance <= 0x0A; instance++ {
			connectedRaw := p.readB524U8(ctx, vaillantB524OpcodeRead, group, instance, radioRegDeviceConnected)
			if connectedRaw == nil {
				continue
			}
			readAny = true

			connected := *connectedRaw == 1
			classAddress := p.readB524U8(ctx, vaillantB524OpcodeRead, group, instance, radioRegDeviceClassAddress)
			firmware := p.readB524Firmware(ctx, vaillantB524OpcodeRead, group, instance, radioRegDeviceFirmware)
			hardware := p.readB524U16(ctx, vaillantB524OpcodeRead, group, instance, radioRegHardwareIdentifier)

			slotMode := "active"
			include := false
			switch group {
			case vaillantGroupRadio09, vaillantGroupRadio10:
				include = connected
			case vaillantGroupRadio0C:
				include = connected || hasRemoteIdentityEvidence(classAddress, firmware, hardware)
				if !connected {
					slotMode = "inventory"
				}
			}
			if !include {
				continue
			}

			device := &vaillantRadioDeviceSnapshot{
				Group:                group,
				Instance:             instance,
				SlotMode:             slotMode,
				DeviceConnected:      cloneBoilerBoolPtr(&connected),
				DeviceClassAddress:   cloneUint8Ptr(classAddress),
				DeviceModel:          decodeRadioDeviceModel(classAddress),
				FirmwareVersion:      cloneStringPtr(firmware),
				HardwareIdentifier:   cloneUint16Ptr(hardware),
				RemoteControlAddress: p.readB524U8(ctx, vaillantB524OpcodeRead, group, instance, radioRegRemoteControlAddress),
				DevicePaired:         p.readB524Bool(ctx, vaillantB524OpcodeRead, group, instance, radioRegDevicePaired),
				ReceptionStrength:    p.readB524U8(ctx, vaillantB524OpcodeRead, group, instance, radioRegReceptionStrength),
				ZoneAssignment:       p.readB524U8(ctx, vaillantB524OpcodeRead, group, instance, radioRegZoneAssignment),
				RoomTemperatureC:     p.readB524F32(ctx, vaillantB524OpcodeRead, group, instance, radioRegRoomTemperature),
				RoomHumidityPct:      p.readB524F32(ctx, vaillantB524OpcodeRead, group, instance, radioRegRoomHumidity),
			}
			discovered[radioDeviceKey{Group: group, Instance: instance}] = device
		}
	}

	if !readAny {
		return
	}

	p.mu.Lock()
	p.radioDevices = discovered
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
}

func (p *vaillantSemanticPoller) refreshFM5Semantic(ctx context.Context) {
	if p == nil || p.provider == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	var moduleConfig *uint16
	if p.system != nil {
		moduleConfig = cloneUint16Ptr(p.system.ModuleConfigurationVR71)
	}
	radioSnapshots := make([]*vaillantRadioDeviceSnapshot, 0, len(p.radioDevices))
	for _, snapshot := range p.radioDevices {
		if snapshot == nil {
			continue
		}
		radioSnapshots = append(radioSnapshots, cloneRadioSnapshot(snapshot))
	}
	p.mu.Unlock()

	hasFM5Evidence := hasFM5EvidenceFromRadioSnapshots(radioSnapshots) || p.hasFM5RegistryEvidence()
	fm5GateSatisfied := moduleConfig != nil && *moduleConfig <= 2

	var incomingSolar *vaillantSolarSnapshot
	incomingCylinders := make(map[byte]*vaillantCylinderSnapshot)
	solarReadable := false
	cylindersReadable := false
	if controller != 0 && fm5GateSatisfied {
		incomingSolar, solarReadable = p.readSolarSnapshot(ctx)
		incomingCylinders, cylindersReadable = p.readCylinderSnapshots(ctx)
	}

	nextMode := deriveFM5SemanticMode(controller != 0, fm5GateSatisfied, solarReadable, cylindersReadable, hasFM5Evidence)

	p.mu.Lock()
	p.fm5Mode = nextMode
	switch nextMode {
	case graphql.Fm5SemanticModeInterpreted:
		p.solar = mergeSolarSnapshotNonDestructive(p.solar, incomingSolar)
		p.solarCylinders = mergeCylinderSnapshotMapNonDestructive(p.solarCylinders, incomingCylinders)
	default:
		p.solar = nil
		p.solarCylinders = make(map[byte]*vaillantCylinderSnapshot)
	}
	p.mu.Unlock()

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

func (p *vaillantSemanticPoller) publishFM5Semantic(source semanticSnapshotSource) {
	if p == nil || p.provider == nil {
		return
	}

	p.mu.Lock()
	mode := p.fm5Mode
	if mode == "" {
		mode = graphql.Fm5SemanticModeAbsent
	}
	solarSnapshot := cloneSolarSnapshot(p.solar)
	cylindersSnapshot := cloneCylinderSnapshotsMap(p.solarCylinders)
	p.mu.Unlock()

	switch source {
	case semanticSnapshotSourceCache:
		p.provider.SetFM5SemanticModeFromCache(mode)
	default:
		p.provider.SetFM5SemanticMode(mode)
	}

	if mode != graphql.Fm5SemanticModeInterpreted {
		switch source {
		case semanticSnapshotSourceCache:
			p.provider.SetSolarFromCache(nil)
			p.provider.SetCylindersFromCache(nil)
		default:
			p.provider.SetSolar(nil)
			p.provider.SetCylinders(nil)
		}
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

	if raw, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupSolar, solarInstance, solarRegEnabled); ok {
		readAny = true
		incoming.SolarEnabled = decodeB524BoolFromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupSolar, solarInstance, solarRegFunctionMode); ok {
		readAny = true
		incoming.FunctionMode = decodeB524BoolFromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupSolar, solarInstance, solarRegCollectorTemp); ok {
		readAny = true
		incoming.CollectorTemperatureC = decodeB524Float32FromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupSolar, solarInstance, solarRegReturnTemp); ok {
		readAny = true
		incoming.ReturnTemperatureC = decodeB524Float32FromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupSolar, solarInstance, solarRegPumpActive); ok {
		readAny = true
		incoming.PumpActive = decodeB524BoolFromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupSolar, solarInstance, solarRegCurrentYield); ok {
		readAny = true
		incoming.CurrentYield = decodeB524Float32FromRaw(raw)
	}
	if raw, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupSolar, solarInstance, solarRegPumpHours); ok {
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

		if raw, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupCylinders, instance, cylinderRegMaxSetpoint); ok {
			instanceRead = true
			readAny = true
			incoming.MaxSetpointC = decodeB524Float32FromRaw(raw)
		}
		if raw, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupCylinders, instance, cylinderRegChargeHysteresis); ok {
			instanceRead = true
			readAny = true
			incoming.ChargeHysteresis = decodeB524Float32FromRaw(raw)
		}
		if raw, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupCylinders, instance, cylinderRegChargeOffset); ok {
			instanceRead = true
			readAny = true
			incoming.ChargeOffset = decodeB524Float32FromRaw(raw)
		}
		if raw, ok := p.readB524Value(ctx, vaillantB524OpcodeLocal, vaillantGroupCylinders, instance, cylinderRegTemperature); ok {
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

func (p *vaillantSemanticPoller) hasFM5RegistryEvidence() bool {
	if p == nil || p.reg == nil {
		return false
	}
	found := false
	p.reg.Iterate(func(entry registry.DeviceEntry) bool {
		if entry == nil {
			return true
		}
		deviceID := normalizeDeviceID(entry.DeviceID())
		if strings.HasPrefix(deviceID, "VR71") || strings.HasPrefix(deviceID, "FM5") {
			found = true
			return false
		}
		return true
	})
	return found
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

	if incoming.SystemOff != nil {
		merged.SystemOff = cloneBoilerBoolPtr(incoming.SystemOff)
	}
	if incoming.SystemWaterPressure != nil {
		merged.SystemWaterPressure = cloneFloat64Ptr(incoming.SystemWaterPressure)
	}
	if incoming.SystemFlowTemperature != nil {
		merged.SystemFlowTemperature = cloneFloat64Ptr(incoming.SystemFlowTemperature)
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
		SystemOff:                    cloneBoilerBoolPtr(snapshot.SystemOff),
		SystemWaterPressure:          cloneFloat64Ptr(snapshot.SystemWaterPressure),
		SystemFlowTemperature:        cloneFloat64Ptr(snapshot.SystemFlowTemperature),
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
	_ = p.cache.Save(semanticCacheSnapshot{
		Zones:  p.provider.Zones(),
		DHW:    p.provider.DHW(),
		Boiler: p.provider.BoilerStatus(),
	})
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
	return true
}

func decodeValvePositionPct(raw *uint16) *float64 {
	if raw == nil {
		return nil
	}
	pct := float64(*raw) / 655.35
	return &pct
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

// Deprecated: findDeviceAddressByPrefix uses a naming heuristic (device ID prefix match).
// For regulator detection, use findRegulatorCapability which relies on the product_ids catalog.
// Retained for boiler controller lookup (BASV prefix) until a catalog-based replacement is added.
func findDeviceAddressByPrefix(reg *registry.DeviceRegistry, wantedPrefix string) (byte, bool) {
	if reg == nil {
		return 0, false
	}
	wantedPrefix = normalizeDeviceID(wantedPrefix)
	if wantedPrefix == "" {
		return 0, false
	}

	var addr byte
	var found bool
	reg.Iterate(func(entry registry.DeviceEntry) bool {
		if entry == nil {
			return true
		}
		if strings.HasPrefix(normalizeDeviceID(entry.DeviceID()), wantedPrefix) {
			addr = entry.Address()
			found = true
			return false
		}
		return true
	})
	return addr, found
}

func (p *vaillantSemanticPoller) findBoilerAddress() byte {
	if p == nil || p.reg == nil {
		return 0
	}

	selected := byte(0)
	p.reg.Iterate(func(entry registry.DeviceEntry) bool {
		if entry == nil || !isBoilerDeviceCandidate(entry) {
			return true
		}
		addr := entry.Address()
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

func isBoilerDeviceCandidate(entry registry.DeviceEntry) bool {
	if entry == nil {
		return false
	}
	normalizedID := normalizeDeviceID(entry.DeviceID())
	if strings.HasPrefix(normalizedID, "BAI") {
		return true
	}
	// Best-effort fallback for boiler-family boards when the product-family byte
	// is not directly exposed by the registry contract.
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

	foundPresent := false
	hasUnknown := false
	hasAnyDevice := false
	p.reg.Iterate(func(entry registry.DeviceEntry) bool {
		if entry == nil {
			return true
		}
		if !strings.EqualFold(entry.Manufacturer(), "Vaillant") {
			return true
		}
		hasAnyDevice = true
		partNumber := extractPartNumberFromSerial(entry.SerialNumber())
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

func (p *vaillantSemanticPoller) readB509Value(ctx context.Context, target byte, addr uint16) ([]byte, bool) {
	if p == nil || p.bus == nil || target == 0 {
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

	key := semanticReadB509Key(target, addr)
	value, err := p.scheduler.Get(ctx, key, 500*time.Millisecond, func(ctx context.Context) ([]byte, error) {
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			p.readMu.Lock()
			reqCtx, cancel := context.WithTimeout(ctx, timeout)
			request := protocol.Frame{
				Source:    source,
				Target:    target,
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509ReadSelector(addr),
			}
			response, err := p.bus.Send(reqCtx, request)
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
	if err != nil {
		return nil, false
	}
	return value, true
}

func (p *vaillantSemanticPoller) writeB509Value(ctx context.Context, target byte, addr uint16, value []byte) error {
	if p == nil || p.bus == nil {
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
			Source:    source,
			Target:    target,
			Primary:   vaillantB509Primary,
			Secondary: vaillantB509Secondary,
			Data:      buildB509WriteSelector(addr, value),
		}
		response, err := p.bus.Send(reqCtx, request)
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
	"flowsetHcMaxC":  {addrs: []uint16{boilerB509RegFlowsetHcMaxC, boilerB509RegFlowsetHcMaxCFallback}, min: 20, max: 80, codec: boilerConfigCodecTempDATA2c},
	"flowsetHwcMaxC": {addrs: []uint16{boilerB509RegFlowsetHwcMaxC}, min: 30, max: 65, codec: boilerConfigCodecTempDATA2c},
	"partloadHcKW":   {addrs: []uint16{boilerB509RegPartloadHcKW}, min: 0, max: 40, codec: boilerConfigCodecUCH},
	"partloadHwcKW":  {addrs: []uint16{boilerB509RegPartloadHwcKW}, min: 0, max: 40, codec: boilerConfigCodecUCH},
}

func (p *vaillantSemanticPoller) SetBoilerConfig(ctx context.Context, fieldName string, rawValue string) graphql.BoilerConfigMutationResult {
	if p == nil {
		return graphql.BoilerConfigMutationResult{Success: false, Error: "boiler config writer unavailable"}
	}

	spec, ok := boilerConfigFieldSpecs[fieldName]
	if !ok {
		keys := make([]string, 0, len(boilerConfigFieldSpecs))
		for key := range boilerConfigFieldSpecs {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		return graphql.BoilerConfigMutationResult{
			Success: false,
			Error:   fmt.Sprintf("unknown boiler field %q (allowed: %s)", fieldName, strings.Join(keys, ", ")),
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
	readback, ok := p.readB509Value(ctx, boilerAddress, addr)
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

	key := semanticReadBreakerKey(target, opcode, group, instance, addr)
	value, err := p.scheduler.Get(ctx, key, 500*time.Millisecond, func(ctx context.Context) ([]byte, error) {
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			p.readMu.Lock()

			reqCtx, cancel := context.WithTimeout(ctx, timeout)
			request := protocol.Frame{
				Source:    source,
				Target:    target,
				Primary:   vaillantExtRegisterPrimary,
				Secondary: vaillantExtRegisterSecondary,
				Data:      buildB524ReadSelector(opcode, group, instance, addr),
			}
			response, err := p.bus.Send(reqCtx, request)
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
	if err != nil {
		return nil, false
	}
	return value, true
}

func semanticReadBreakerKey(target, opcode, group, instance byte, addr uint16) string {
	return fmt.Sprintf("b524:%02x:%02x:%02x:%02x:%04x", target, opcode, group, instance, addr)
}

func semanticReadB509Key(target byte, addr uint16) string {
	return fmt.Sprintf("b509:%02x:%04x", target, addr)
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

func matchesB524ReplyInstance(replyInstance, requestedInstance byte) bool {
	if replyInstance == requestedInstance {
		return true
	}
	if requestedInstance < 0xFF && replyInstance == requestedInstance+1 {
		return true
	}
	return false
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

func (p *vaillantSemanticPoller) readB524ZoneNamePart(ctx context.Context, instance byte, addr uint16) (string, bool) {
	raw, ok := p.readB524CString(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, addr)
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
		},
		Properties: &mcp.SystemProperties{
			SystemScheme:            cloneIntPtr(status.Properties.SystemScheme),
			ModuleConfigurationVR71: cloneIntPtr(status.Properties.ModuleConfigurationVR71),
		},
	}
}
