package main

import (
	"fmt"
	"log"
	"maps"
	"strings"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/systemconfig"
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
	// SpecialFunctionMode is the raw RR001D byte (provisional name).
	SpecialFunctionMode *uint8
	PumpStatusRaw       *uint16
	PumpStatusLiveAt    time.Time
	CalcFlowTempC       *float64
	MixerPositionPct    *float64
	HumidityPct         *float64
	DewPointC           *float64
	PumpHoursRaw        *uint32
	PumpStartsRaw       *uint32
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

// vaillantZoneView is the native view of one zone that the acquisition side
// hands to the publication owner. snapshot is a detached copy of the retained
// zone. The radio room values are taken from the mapped radio device only when
// the zone lacks its own temperature or humidity, and inferredRoomMappingRaw
// only when the zone carries no room temperature mapping.
type vaillantZoneView struct {
	instance               byte
	snapshot               *vaillantZoneSnapshot
	radioRoomTempC         *float64
	radioRoomHumidityPct   *float64
	inferredRoomMappingRaw *uint16
}

// vaillantDhwView is the native view of hot water handed to the publication
// owner: a detached copy of the retained snapshot, nil when none is retained,
// and whether this publication expired a stale snapshot.
type vaillantDhwView struct {
	snapshot *vaillantDhwSnapshot
	expired  bool
}

// vaillantCircuitsView is the native view of the active circuits handed to
// the publication owner, in instance order, with whether the function module
// manages them.
type vaillantCircuitsView struct {
	circuits                []*vaillantCircuitSnapshot
	managedByFunctionModule bool
}

// vaillantSystemView is the native view of the system handed to the
// publication owner: a detached copy of the retained snapshot, nil when none
// is retained, and the hot water bivalence bound resolved for the discovered
// controller, set only with a snapshot.
type vaillantSystemView struct {
	snapshot          *vaillantSystemSnapshot
	dhwBivalenceBound systemconfig.ConfigBound
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
		publication:     newVaillantSemanticPublication(provider, hub, cache),
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
		regAbsenceState:          regulatorAbsenceUnknown,
		fm5Mode:                  vaillantFM5ModeAbsent,

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
	poller.adapterInfo = newVaillantAdapterInfoState(gateway.Bus, gateway.Transport, poller.publication)
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

// hydrateFromCache seeds the retained zones, hot water and boiler from the
// native cache hydration value and takes ownership of its snapshots.
func (p *vaillantSemanticPoller) hydrateFromCache(hydration vaillantCacheHydration) {
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
	for _, zone := range hydration.zones {
		p.zones[zone.Instance] = zone
		p.presence[zone.Instance] = &zonePresenceRecord{
			State:     zonePresencePresent,
			HitStreak: p.zoneHitThresholdValue(),
		}
	}
	if hydration.dhw != nil {
		p.dhw = hydration.dhw
		if !hydration.persistedAt.IsZero() {
			p.dhwLastUpdateAt = hydration.persistedAt.UTC()
		} else {
			p.markDHWUpdatedNowLocked()
		}
	}
	if hydration.boiler != nil {
		p.boiler = hydration.boiler
	}
	semanticZoneCount.Set(int64(len(p.zones)))
}

func cloneZoneSnapshot(snapshot *vaillantZoneSnapshot) *vaillantZoneSnapshot {
	if snapshot == nil {
		return nil
	}
	cloned := *snapshot
	cloned.AllowedModes = append([]string(nil), snapshot.AllowedModes...)
	cloned.CurrentTempC = cloneFloat64Ptr(snapshot.CurrentTempC)
	cloned.TargetTempC = cloneFloat64Ptr(snapshot.TargetTempC)
	cloned.HumidityPct = cloneFloat64Ptr(snapshot.HumidityPct)
	cloned.ConfigurationRoomTemperatureZoneMappingRaw = cloneUint16Ptr(snapshot.ConfigurationRoomTemperatureZoneMappingRaw)
	cloned.ConfigurationAssociatedCircuitRaw = cloneUint16Ptr(snapshot.ConfigurationAssociatedCircuitRaw)
	cloned.ConfigurationCircuitTypeRaw = cloneUint16Ptr(snapshot.ConfigurationCircuitTypeRaw)
	cloned.StateValveStatusRaw = cloneUint16Ptr(snapshot.StateValveStatusRaw)
	cloned.QuickVetoTempC = cloneFloat64Ptr(snapshot.QuickVetoTempC)
	cloned.QuickVetoDurationH = cloneFloat64Ptr(snapshot.QuickVetoDurationH)
	cloned.HolidaySetpointC = cloneFloat64Ptr(snapshot.HolidaySetpointC)
	cloned.FieldFreshness = maps.Clone(snapshot.FieldFreshness)
	return &cloned
}

func cloneDhwSnapshot(snapshot *vaillantDhwSnapshot) *vaillantDhwSnapshot {
	if snapshot == nil {
		return nil
	}
	cloned := *snapshot
	cloned.CurrentTempC = cloneFloat64Ptr(snapshot.CurrentTempC)
	cloned.TargetTempC = cloneFloat64Ptr(snapshot.TargetTempC)
	cloned.FieldFreshness = maps.Clone(snapshot.FieldFreshness)
	return &cloned
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

// mergePublishedBoilerFlow replaces the legacy scalar even on withholding;
// value-only cache and system-flow mirrors cannot seed this field.
func (p *vaillantSemanticPoller) mergePublishedBoilerFlow() bool {
	p.mu.Lock()
	publisher, target := p.boilerThermal, p.boilerAddress
	p.mu.Unlock()
	var flow *float64
	if publisher != nil {
		flow = publisher.scalar(target)
	}
	p.mu.Lock()
	var previous *float64
	if p.boiler != nil {
		previous = p.boiler.FlowTemperatureC
	}
	changed := !floatPtrEquals(previous, flow)
	if p.boiler == nil && flow != nil {
		p.boiler = &vaillantBoilerSnapshot{}
	}
	if p.boiler != nil {
		p.boiler.FlowTemperatureC = flow
	}
	p.mu.Unlock()
	return changed
}
