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

	"github.com/d3vi1/helianthus-ebusgateway"
	"github.com/d3vi1/helianthus-ebusgateway/graphql"
	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusreg/registry"
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

var (
	semanticReadBreakerTransitionsTotal = expvar.NewMap("semantic_read_breaker_transitions_total")
	semanticReadBreakerSuppressedTotal  = expvar.NewMap("semantic_read_breaker_suppressed_total")
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
	zoneMissThreshold int
	zoneHitThreshold  int
	dhwStaleTTL       time.Duration

	pollMu sync.Mutex
	readMu sync.Mutex

	mu              sync.Mutex
	controller      byte
	zones           map[byte]*vaillantZoneSnapshot
	presence        map[byte]*zonePresenceRecord
	dhw             *vaillantDhwSnapshot
	dhwLastUpdateAt time.Time

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
		zoneMissThreshold: cfg.SemanticZonePresenceMissThreshold,
		zoneHitThreshold:  cfg.SemanticZonePresenceHitThreshold,
		dhwStaleTTL:       cfg.SemanticDHWStaleTTL,

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
		p.markDHWUpdatedNowLocked()
	}
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

	go p.runLoop(ctx, p.discoveryInterval, semanticTaskPriorityLow, p.refreshDiscovery)
	go p.runLoop(ctx, p.configInterval, semanticTaskPriorityMedium, p.refreshConfig)
	go p.runLoop(ctx, p.stateInterval, semanticTaskPriorityHigh, p.refreshState)
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

func (p *vaillantSemanticPoller) refreshDiscovery(ctx context.Context) {
	controller, ok := findDeviceAddressByPrefix(p.reg, "BASV")
	if !ok {
		p.mu.Lock()
		p.controller = 0
		p.zones = make(map[byte]*vaillantZoneSnapshot)
		p.presence = make(map[byte]*zonePresenceRecord)
		p.dhw = nil
		p.dhwLastUpdateAt = time.Time{}
		p.mu.Unlock()
		p.publishZones(semanticSnapshotSourceCache)
		p.publishDHW(semanticSnapshotSourceCache)
		return
	}

	p.mu.Lock()
	p.controller = controller
	p.mu.Unlock()

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
}

func (p *vaillantSemanticPoller) markZoneMissingLocked(instance byte) {
	record := p.ensureZonePresenceRecordLocked(instance)
	record.HitStreak = 0
	missThreshold := p.zoneMissThresholdValue()

	switch record.State {
	case zonePresencePresent, zonePresenceSuspectMissing:
		record.MissStreak++
		if record.MissStreak >= missThreshold {
			record.MissStreak = missThreshold
			record.State = zonePresenceAbsent
			delete(p.zones, instance)
			return
		}
		record.State = zonePresenceSuspectMissing
	default:
		record.MissStreak = 0
		record.State = zonePresenceAbsent
		delete(p.zones, instance)
	}
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
		Zones: p.provider.Zones(),
		DHW:   p.provider.DHW(),
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
		vaillantB524OpcodeRead,
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
