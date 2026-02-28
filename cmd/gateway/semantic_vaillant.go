package main

import (
	"context"
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"log"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

const (
	vaillantExtRegisterPrimary   = byte(0xB5)
	vaillantExtRegisterSecondary = byte(0x24)
	vaillantB524OpcodeRead       = byte(0x06)
	vaillantB524OpcodeLocal      = byte(0x02)
	vaillantB524OpRead           = byte(0x00)

	vaillantGroupDHW      = byte(0x01)
	vaillantGroupCircuits = byte(0x02)
	vaillantGroupZones    = byte(0x03)

	zoneRegName                 = uint16(0x0016)
	zoneRegNamePrefix           = uint16(0x0017)
	zoneRegNameSuffix           = uint16(0x0018)
	zoneRegIndex                = uint16(0x001C)
	zoneRegHeatingOpMode        = uint16(0x0006) // configuration.heating.operation_mode
	zoneRegCurrentTemp          = uint16(0x000F) // state.current_room_temperature
	zoneRegTargetTemp           = uint16(0x0022) // configuration.heating.desired_setpoint
	zoneRegFallbackManualTemp   = uint16(0x0014) // configuration.heating.manual_mode_setpoint
	zoneRegSpecialFunction      = uint16(0x000E) // state.current_special_function
	zoneRegValveStatus          = uint16(0x0012) // state.valve_status
	zoneRegAssociatedCircuitRaw = uint16(0x0013) // configuration.associated_circuit_index
	zoneRegCurrentHumidity      = uint16(0x0028) // state.current_room_humidity

	circuitRegType = uint16(0x0002) // configuration.heating_circuit_type / mixer_circuit_type_external

	dhwRegOperationMode   = uint16(0x0003) // configuration.domestic_hot_water.operation_mode
	dhwRegTargetTemp      = uint16(0x0004) // configuration.domestic_hot_water.tapping_setpoint
	dhwRegCurrentTemp     = uint16(0x0005) // state.current_dhw_temperature
	dhwRegSpecialFunction = uint16(0x000D) // state.current_special_function
	dhwInstance           = byte(0x00)
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

func startVaillantSemanticPolling(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub) {
	if gateway == nil || gateway.Bus == nil || gateway.Registry == nil || provider == nil {
		return
	}

	cacheStore := newSemanticCacheStore(cfg.SemanticCachePath, log.Printf)
	cacheSnapshot, cacheLoaded := preloadSemanticCache(provider, cacheStore)
	poller := newVaillantSemanticPoller(cfg, gateway, provider, hub, cacheStore)
	if cacheLoaded {
		poller.hydrateFromCache(cacheSnapshot)
	}
	poller.Start(ctx)
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

	source            byte
	requestTimeout    time.Duration
	discoveryInterval time.Duration
	configInterval    time.Duration
	stateInterval     time.Duration
	energyInterval    time.Duration
	boilerInterval    time.Duration
	zoneMissThreshold int
	zoneHitThreshold  int
	dhwStaleTTL       time.Duration

	pollMu sync.Mutex
	readMu sync.Mutex

	catalog    productids.Catalog
	catalogErr error

	mu                       sync.Mutex
	controller               byte
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
	boiler *vaillantBoilerSnapshot

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

	ConfigurationHeatingOperationMode string
	StateSpecialFunction              string
	ConfigurationAssociatedCircuitRaw *uint16
	ConfigurationCircuitTypeRaw       *uint16
	StateValveStatusRaw               *uint16

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

type semanticSnapshotSource uint8

const (
	semanticSnapshotSourceCache semanticSnapshotSource = iota
	semanticSnapshotSourceLive
)

type semanticFieldKey string

const (
	zoneFieldName                 semanticFieldKey = "zone.name"
	zoneFieldOperatingMode        semanticFieldKey = "zone.operating_mode"
	zoneFieldPreset               semanticFieldKey = "zone.preset"
	zoneFieldHvacAction           semanticFieldKey = "zone.hvac_action"
	zoneFieldAllowedModes         semanticFieldKey = "zone.allowed_modes"
	zoneFieldCurrentTempC         semanticFieldKey = "zone.current_temp_c"
	zoneFieldTargetTempC          semanticFieldKey = "zone.target_temp_c"
	zoneFieldCurrentHumidityPct   semanticFieldKey = "zone.current_humidity_pct"
	zoneFieldSpecialFunctionRaw   semanticFieldKey = "zone.special_function_raw"
	zoneFieldZoneOperationModeRaw semanticFieldKey = "zone.operation_mode_raw"
	zoneFieldZoneCircuitIndexRaw  semanticFieldKey = "zone.circuit_index_raw"
	zoneFieldCircuitTypeRaw       semanticFieldKey = "zone.circuit_type_raw"
	zoneFieldZoneValveStatusRaw   semanticFieldKey = "zone.valve_status_raw"
	dhwFieldOperatingMode         semanticFieldKey = "dhw.operating_mode"
	dhwFieldPreset                semanticFieldKey = "dhw.preset"
	dhwFieldCurrentTempC          semanticFieldKey = "dhw.current_temp_c"
	dhwFieldTargetTempC           semanticFieldKey = "dhw.target_temp_c"
	dhwFieldSpecialFunctionRaw    semanticFieldKey = "dhw.special_function_raw"
	dhwFieldDhwOperationModeRaw   semanticFieldKey = "dhw.operation_mode_raw"
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

		requestTimeout:    cfg.SemanticRequestTimeout,
		discoveryInterval: cfg.SemanticDiscoveryInterval,
		configInterval:    cfg.SemanticConfigInterval,
		stateInterval:     cfg.SemanticStateInterval,
		energyInterval:    cfg.SemanticEnergyInterval,
		boilerInterval:    30 * time.Second,
		zoneMissThreshold: cfg.SemanticZonePresenceMissThreshold,
		zoneHitThreshold:  cfg.SemanticZonePresenceHitThreshold,
		dhwStaleTTL:       cfg.SemanticDHWStaleTTL,

		catalog:    catalog,
		catalogErr: catalogErr,

		regulatorRecheckInterval: cfg.SemanticRegulatorRecheckInterval,
		regulatorAbsenceGrace:    cfg.SemanticRegulatorAbsenceGrace,
		regAbsenceState:          regulatorPresent,

		zones:    make(map[byte]*vaillantZoneSnapshot),
		presence: make(map[byte]*zonePresenceRecord),
		nowFn:    time.Now,
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
		Instance:                          instance,
		Present:                           true,
		Name:                              zone.Name,
		OperatingMode:                     zone.OperatingMode,
		Preset:                            zone.Preset,
		HvacAction:                        zone.HvacAction,
		AllowedModes:                      append([]string(nil), zone.AllowedModes...),
		CurrentTempC:                      cloneFloat64Ptr(zone.CurrentTempC),
		TargetTempC:                       cloneFloat64Ptr(zone.TargetTempC),
		HumidityPct:                       cloneFloat64Ptr(zone.CurrentHumidityPct),
		ConfigurationHeatingOperationMode: zone.ZoneOperationModeRaw,
		StateSpecialFunction:              zone.ZoneSpecialFunctionRaw,
		ConfigurationAssociatedCircuitRaw: parseUint16Token(zone.ZoneCircuitIndexRaw),
		ConfigurationCircuitTypeRaw:       parseUint16Token(zone.CircuitTypeRaw),
		StateValveStatusRaw:               parseUint16Token(zone.ZoneValveStatusRaw),
	}
	seedZoneFreshness(snapshot, semanticSnapshotSourceCache, true)
	return snapshot
}

func dhwSnapshotFromSemanticStatus(status *graphql.DhwStatus) *vaillantDhwSnapshot {
	if status == nil {
		return nil
	}
	snapshot := &vaillantDhwSnapshot{
		OperatingMode:                 status.OperatingMode,
		Preset:                        status.Preset,
		CurrentTempC:                  cloneFloat64Ptr(status.CurrentTempC),
		TargetTempC:                   cloneFloat64Ptr(status.TargetTempC),
		ConfigurationDHWOperationMode: status.DhwOperationModeRaw,
		StateSpecialFunction:          status.DhwSpecialFunctionRaw,
	}
	seedDhwFreshness(snapshot, semanticSnapshotSourceCache, true)
	return snapshot
}

func parseUint16Token(token string) *uint16 {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}
	value, err := strconv.ParseUint(token, 10, 16)
	if err != nil {
		return nil
	}
	out := uint16(value)
	return &out
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

func (p *vaillantSemanticPoller) Start(ctx context.Context) {
	if p == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	go p.tasks.run(ctx)

	// Prime quickly so HA can create entities on first coordinator refresh.
	p.enqueueTask(semanticTaskPriorityHigh, p.refreshDiscovery)
	p.enqueueTask(semanticTaskPriorityHigh, p.refreshConfig)
	p.enqueueTask(semanticTaskPriorityHigh, p.refreshState)
	p.enqueueTask(semanticTaskPriorityMedium, p.refreshEnergy)
	p.enqueueTask(semanticTaskPriorityMedium, p.refreshBoilerStatus)

	go p.runLoop(ctx, p.regulatorRecheckInterval, semanticTaskPriorityLow, p.refreshRegulatorCapability)
	go p.runLoop(ctx, p.discoveryInterval, semanticTaskPriorityLow, p.refreshDiscovery)
	go p.runLoop(ctx, p.configInterval, semanticTaskPriorityMedium, p.refreshConfig)
	go p.runLoop(ctx, p.stateInterval, semanticTaskPriorityHigh, p.refreshState)
	go p.runLoop(ctx, p.energyInterval, semanticTaskPriorityMedium, p.refreshEnergy)
	go p.runLoop(ctx, p.boilerInterval, semanticTaskPriorityHigh, p.refreshBoilerStatus)
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
		p.withPollLock(context.Background(), fn)
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

	controller, ok := findDeviceAddressByPrefix(p.reg, "BASV")
	if !ok {
		p.mu.Lock()
		prev := p.regulatorCapability
		p.controller = 0
		p.regulatorCapability = regCap
		p.zones = make(map[byte]*vaillantZoneSnapshot)
		p.presence = make(map[byte]*zonePresenceRecord)
		semanticZoneCount.Set(0)
		p.mu.Unlock()
		if regCap != prev {
			log.Printf("semantic_regulator_capability capability=%s", regCap.String())
		}
		p.publishZones(semanticSnapshotSourceCache)
		p.publishDHW(semanticSnapshotSourceCache)
		return
	}

	p.mu.Lock()
	prev := p.regulatorCapability
	p.controller = controller
	p.regulatorCapability = regCap
	p.mu.Unlock()
	if regCap != prev {
		log.Printf("semantic_regulator_capability capability=%s", regCap.String())
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
		zoneCircuitRaw, zoneCircuitRawOK := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegAssociatedCircuitRaw)
		if zoneOpModeOK || zoneSFOK || zoneValveOK || zoneCircuitRawOK {
			liveReadSuccess = true
		}
		circuitInstance := resolveCircuitInstance(zoneCircuitRaw, instance)
		circuitType, hasCircuitType := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, circuitInstance, circuitRegType)
		if hasCircuitType {
			liveReadSuccess = true
		}

		operatingMode, preset, allowedModes := deriveZoneModeAndPreset(zoneOpMode, zoneSF, circuitType, hasCircuitType)
		hvacAction := deriveZoneHvacAction(zoneValve, circuitType, hasCircuitType)
		incoming := &vaillantZoneSnapshot{
			OperatingMode:                     operatingMode,
			Preset:                            preset,
			HvacAction:                        hvacAction,
			AllowedModes:                      allowedModes,
			CurrentTempC:                      currentPtr,
			TargetTempC:                       targetPtr,
			HumidityPct:                       humidity,
			ConfigurationAssociatedCircuitRaw: zoneCircuitRaw,
			ConfigurationCircuitTypeRaw:       circuitType,
			StateValveStatusRaw:               zoneValve,
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
			ID:                     fmt.Sprintf("zone-%d", instance+1),
			Name:                   name,
			OperatingMode:          entry.OperatingMode,
			Preset:                 entry.Preset,
			HvacAction:             entry.HvacAction,
			AllowedModes:           append([]string(nil), entry.AllowedModes...),
			CurrentTempC:           entry.CurrentTempC,
			TargetTempC:            entry.TargetTempC,
			CurrentHumidityPct:     entry.HumidityPct,
			SpecialFunction:        entry.StateSpecialFunction,
			CircuitTypeRaw:         optionalUintToken(entry.ConfigurationCircuitTypeRaw),
			ZoneCircuitIndexRaw:    optionalUintToken(entry.ConfigurationAssociatedCircuitRaw),
			ZoneOperationModeRaw:   entry.ConfigurationHeatingOperationMode,
			ZoneValveStatusRaw:     optionalUintToken(entry.StateValveStatusRaw),
			ZoneSpecialFunctionRaw: entry.StateSpecialFunction,
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
	if a.OperatingMode != b.OperatingMode || a.Preset != b.Preset {
		return false
	}
	if a.HvacAction != b.HvacAction || a.SpecialFunction != b.SpecialFunction {
		return false
	}
	if a.CircuitTypeRaw != b.CircuitTypeRaw ||
		a.ZoneCircuitIndexRaw != b.ZoneCircuitIndexRaw ||
		a.ZoneOperationModeRaw != b.ZoneOperationModeRaw ||
		a.ZoneValveStatusRaw != b.ZoneValveStatusRaw ||
		a.ZoneSpecialFunctionRaw != b.ZoneSpecialFunctionRaw {
		return false
	}
	if !slices.Equal(a.AllowedModes, b.AllowedModes) {
		return false
	}
	if !floatPtrEquals(a.CurrentTempC, b.CurrentTempC) {
		return false
	}
	if !floatPtrEquals(a.TargetTempC, b.TargetTempC) {
		return false
	}
	if !floatPtrEquals(a.CurrentHumidityPct, b.CurrentHumidityPct) {
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
		OperatingMode:         snapshot.OperatingMode,
		Preset:                snapshot.Preset,
		CurrentTempC:          snapshot.CurrentTempC,
		TargetTempC:           snapshot.TargetTempC,
		SpecialFunction:       snapshot.StateSpecialFunction,
		DhwOperationModeRaw:   snapshot.ConfigurationDHWOperationMode,
		DhwSpecialFunctionRaw: snapshot.StateSpecialFunction,
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

// --- Boiler status ---

// B524 registers on the controller that mirror boiler operational data.
// Group 0x00 (regulator parameters), instance 0x00.
const (
	vaillantGroupRegulator = byte(0x00)
	regulatorInstance      = byte(0x00)

	// system_flow_temperature — float32 LE (°C)
	regulatorRegSystemFlowTemp = uint16(0x004B)
	// system_water_pressure — float32 LE (bar)
	regulatorRegSystemWaterPressure = uint16(0x0039)

	// Heating circuit registers (group 0x02)
	circuitRegFlowTemp    = uint16(0x0008) // heating_circuit_flow_temperature — float32 LE (°C)
	circuitRegPumpStatus  = uint16(0x001E) // pump_status — uint16 LE (0=off, !0=on)
	circuitRegCircuitState = uint16(0x001B) // circuit_state — uint16 LE
)

type vaillantBoilerSnapshot struct {
	FlowTemperatureC         *float64
	ReturnTemperatureC       *float64
	CentralHeatingPumpActive *bool
	DhwTemperatureC          *float64
	DhwTargetTemperatureC    *float64
	DhwOperatingMode         *int
	HeatingStatusRaw         *int
	DhwStatusRaw             *int
}

// findBoilerAddress is retained for potential future use (e.g. broadcast sniffing).
// Currently unused — boiler data is read via B524 registers on the controller.

// refreshBoilerStatus reads boiler operational data via B524 registers on the controller.
// The BAI boiler does not respond to direct B504 reads from third-party sources — it only
// accepts requests from its paired controller. Instead, the controller (VRC/BASV2) mirrors
// boiler data in its own B524 register space, which we can read reliably.
func (p *vaillantSemanticPoller) refreshBoilerStatus(ctx context.Context) {
	if p == nil {
		return
	}

	// We don't need the boiler address — we read from the controller's registers.
	// But confirm we have a controller to talk to.
	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	snapshot := &vaillantBoilerSnapshot{}

	// System flow temperature from regulator group (group=0x00, instance=0x00, reg=0x004B)
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupRegulator, regulatorInstance, regulatorRegSystemFlowTemp); ok {
		snapshot.FlowTemperatureC = &value
	}

	// Heating circuit 0 flow temperature (group=0x02, instance=0x00, reg=0x0008)
	// This is the per-circuit flow temp, may differ from system flow temp.
	// Use as return temperature proxy if system flow is the supply side.
	if value, ok := p.readB524Float32LE(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, 0x00, circuitRegFlowTemp); ok {
		snapshot.ReturnTemperatureC = &value
	}

	// Heating circuit 0 pump status (group=0x02, instance=0x00, reg=0x001E)
	if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, 0x00, circuitRegPumpStatus); ok && raw != nil {
		active := *raw != 0
		snapshot.CentralHeatingPumpActive = &active
	}

	// Heating circuit 0 state (group=0x02, instance=0x00, reg=0x001B)
	if raw, ok := p.readB524Uint16(ctx, vaillantB524OpcodeLocal, vaillantGroupCircuits, 0x00, circuitRegCircuitState); ok && raw != nil {
		status := int(*raw)
		snapshot.HeatingStatusRaw = &status
	}

	// Only update if we got at least some data — avoid wiping good state on transient bus errors.
	if snapshot.FlowTemperatureC == nil && snapshot.ReturnTemperatureC == nil &&
		snapshot.CentralHeatingPumpActive == nil && snapshot.HeatingStatusRaw == nil {
		return
	}

	p.mu.Lock()
	p.boiler = snapshot
	p.mu.Unlock()

	p.publishBoilerStatus(semanticSnapshotSourceLive)
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
			DhwTemperatureC:          snapshot.DhwTemperatureC,
			DhwTargetTemperatureC:    snapshot.DhwTargetTemperatureC,
		},
		Config: graphql.BoilerConfig{},
		Diagnostics: graphql.BoilerDiagnostics{
			HeatingStatusRaw: snapshot.HeatingStatusRaw,
			DhwStatusRaw:     snapshot.DhwStatusRaw,
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
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if !floatPtrEquals(a.State.FlowTemperatureC, b.State.FlowTemperatureC) {
		return false
	}
	if !floatPtrEquals(a.State.ReturnTemperatureC, b.State.ReturnTemperatureC) {
		return false
	}
	if !boolPtrEquals(a.State.CentralHeatingPumpActive, b.State.CentralHeatingPumpActive) {
		return false
	}
	if !floatPtrEquals(a.State.DhwTemperatureC, b.State.DhwTemperatureC) {
		return false
	}
	if !floatPtrEquals(a.State.DhwTargetTemperatureC, b.State.DhwTargetTemperatureC) {
		return false
	}
	if !stringPtrEquals(a.Config.DhwOperatingMode, b.Config.DhwOperatingMode) {
		return false
	}
	if !intPtrEquals(a.Diagnostics.HeatingStatusRaw, b.Diagnostics.HeatingStatusRaw) {
		return false
	}
	if !intPtrEquals(a.Diagnostics.DhwStatusRaw, b.Diagnostics.DhwStatusRaw) {
		return false
	}
	return true
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

func boilerSnapshotFromGraphQL(status *graphql.BoilerStatus) *vaillantBoilerSnapshot {
	if status == nil {
		return nil
	}
	return &vaillantBoilerSnapshot{
		FlowTemperatureC:         status.State.FlowTemperatureC,
		ReturnTemperatureC:       status.State.ReturnTemperatureC,
		CentralHeatingPumpActive: status.State.CentralHeatingPumpActive,
		DhwTemperatureC:          status.State.DhwTemperatureC,
		DhwTargetTemperatureC:    status.State.DhwTargetTemperatureC,
		HeatingStatusRaw:         status.Diagnostics.HeatingStatusRaw,
		DhwStatusRaw:             status.Diagnostics.DhwStatusRaw,
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
	if a.OperatingMode != b.OperatingMode || a.Preset != b.Preset {
		return false
	}
	if a.SpecialFunction != b.SpecialFunction ||
		a.DhwOperationModeRaw != b.DhwOperationModeRaw ||
		a.DhwSpecialFunctionRaw != b.DhwSpecialFunctionRaw {
		return false
	}
	if !floatPtrEquals(a.CurrentTempC, b.CurrentTempC) || !floatPtrEquals(a.TargetTempC, b.TargetTempC) {
		return false
	}
	return true
}

func optionalUintToken(value *uint16) string {
	if value == nil {
		return ""
	}
	return formatUintToken(*value)
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

func resolveCircuitInstance(associatedCircuit *uint16, zoneInstance byte) byte {
	if associatedCircuit == nil {
		return zoneInstance
	}
	value := *associatedCircuit
	switch value {
	case 0xFF, 0xFFFF:
		return zoneInstance
	default:
		if value <= 0x1F {
			return byte(value)
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
