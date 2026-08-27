package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

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
