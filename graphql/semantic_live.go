package graphql

import (
	"context"
	"expvar"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

type SemanticStartupPhase string

const (
	SemanticStartupPhaseBootInit         SemanticStartupPhase = "BOOT_INIT"
	SemanticStartupPhaseCacheLoadedStale SemanticStartupPhase = "CACHE_LOADED_STALE"
	SemanticStartupPhaseLiveWarmup       SemanticStartupPhase = "LIVE_WARMUP"
	SemanticStartupPhaseLiveReady        SemanticStartupPhase = "LIVE_READY"
	SemanticStartupPhaseDegraded         SemanticStartupPhase = "DEGRADED"
)

type semanticDataSource uint8

const (
	semanticDataSourceCache semanticDataSource = iota
	semanticDataSourceLive
)

type semanticStreamKind uint8

const (
	semanticStreamZones semanticStreamKind = iota
	semanticStreamDHW
	semanticStreamBoiler
	semanticStreamCircuits
	semanticStreamRadioDevices
)

type phaseTransitionLog struct {
	previous   SemanticStartupPhase
	next       SemanticStartupPhase
	reason     string
	cacheEpoch uint64
	liveEpoch  uint64
}

var (
	semanticStartupPhaseTransitionsTotal = expvar.NewMap("semantic_startup_phase_transitions_total")
	semanticStartupCurrentPhase          = expvar.NewString("semantic_startup_current_phase")
	semanticCacheEpoch                   = expvar.NewInt("semantic_cache_epoch")
	semanticLiveEpoch                    = expvar.NewInt("semantic_live_epoch")
)

// LiveSemanticProvider maintains semantic snapshots derived from bus data.
type LiveSemanticProvider struct {
	mu        sync.RWMutex
	zones     []Zone
	dhw       *DhwStatus
	circuits  []CircuitStatus
	radio     []RadioDevice
	fm5Mode   Fm5SemanticMode
	solar     *SolarStatus
	cylinders []CylinderStatus
	energy    *EnergyTotals
	boiler    *BoilerStatus

	energyMerge    *energyMergeStore
	energyRevision uint64

	phase              SemanticStartupPhase
	cacheEpoch         uint64
	liveEpoch          uint64
	zonePublished      bool
	dhwPublished       bool
	boilerPublished    bool
	circuitPublished   bool
	radioPublished     bool
	zoneLiveSeen       bool
	dhwLiveSeen        bool
	boilerLiveSeen     bool
	circuitLiveSeen    bool
	radioLiveSeen      bool
	bootMonitorStarted bool
	phaseLogger        func(string, ...any)
}

func NewLiveSemanticProvider() *LiveSemanticProvider {
	semanticStartupCurrentPhase.Set(string(SemanticStartupPhaseBootInit))
	semanticCacheEpoch.Set(0)
	semanticLiveEpoch.Set(0)

	return &LiveSemanticProvider{
		phase:       SemanticStartupPhaseBootInit,
		fm5Mode:     Fm5SemanticModeAbsent,
		energyMerge: newEnergyMergeStore(),
	}
}

func (provider *LiveSemanticProvider) StartBootFSM(ctx context.Context, bootLiveTimeout time.Duration, logf func(string, ...any)) {
	if provider == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	provider.mu.Lock()
	if logf == nil {
		logf = log.Printf
	}
	if provider.phaseLogger == nil {
		provider.phaseLogger = logf
	}
	if provider.bootMonitorStarted {
		provider.mu.Unlock()
		return
	}
	provider.bootMonitorStarted = true
	provider.mu.Unlock()

	if bootLiveTimeout <= 0 {
		return
	}

	go func() {
		timer := time.NewTimer(bootLiveTimeout)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			var transition *phaseTransitionLog
			provider.mu.Lock()
			if provider.phase != SemanticStartupPhaseLiveReady {
				reason := fmt.Sprintf("boot_live_timeout elapsed=%s", bootLiveTimeout)
				transition = provider.transitionPhaseLocked(SemanticStartupPhaseDegraded, reason)
			}
			provider.mu.Unlock()
			provider.logPhaseTransition(transition)
		}
	}()
}

func (provider *LiveSemanticProvider) StartupPhase() SemanticStartupPhase {
	if provider == nil {
		return SemanticStartupPhaseBootInit
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	return provider.phase
}

func (provider *LiveSemanticProvider) StartupEpochs() (cacheEpoch uint64, liveEpoch uint64) {
	if provider == nil {
		return 0, 0
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	return provider.cacheEpoch, provider.liveEpoch
}

func (provider *LiveSemanticProvider) Zones() []Zone {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if len(provider.zones) == 0 {
		return nil
	}
	zones := make([]Zone, len(provider.zones))
	for i, z := range provider.zones {
		zones[i] = cloneZone(z)
	}
	return zones
}

func (provider *LiveSemanticProvider) DHW() *DhwStatus {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if provider.dhw == nil {
		return nil
	}
	cp := cloneDhwStatus(provider.dhw)
	return cp
}

func (provider *LiveSemanticProvider) Circuits() []CircuitStatus {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if len(provider.circuits) == 0 {
		return nil
	}
	out := make([]CircuitStatus, len(provider.circuits))
	for i, status := range provider.circuits {
		out[i] = cloneCircuitStatus(status)
	}
	return out
}

func (provider *LiveSemanticProvider) RadioDevices() []RadioDevice {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if len(provider.radio) == 0 {
		return nil
	}
	out := make([]RadioDevice, len(provider.radio))
	for i, device := range provider.radio {
		out[i] = cloneRadioDevice(device)
	}
	return out
}

func (provider *LiveSemanticProvider) FM5SemanticMode() Fm5SemanticMode {
	if provider == nil {
		return Fm5SemanticModeAbsent
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if provider.fm5Mode == "" {
		return Fm5SemanticModeAbsent
	}
	return provider.fm5Mode
}

func (provider *LiveSemanticProvider) Solar() *SolarStatus {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	return cloneSolarStatus(provider.solar)
}

func (provider *LiveSemanticProvider) Cylinders() []CylinderStatus {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if len(provider.cylinders) == 0 {
		return nil
	}
	return cloneCylinderStatuses(provider.cylinders)
}

func (provider *LiveSemanticProvider) SetFM5SemanticMode(mode Fm5SemanticMode) {
	if provider == nil {
		return
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if strings.TrimSpace(string(mode)) == "" {
		mode = Fm5SemanticModeAbsent
	}
	provider.fm5Mode = mode
}

func (provider *LiveSemanticProvider) SetFM5SemanticModeFromCache(mode Fm5SemanticMode) {
	provider.SetFM5SemanticMode(mode)
}

func (provider *LiveSemanticProvider) SetSolar(status *SolarStatus) {
	if provider == nil {
		return
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.solar = cloneSolarStatus(status)
}

func (provider *LiveSemanticProvider) SetSolarFromCache(status *SolarStatus) {
	provider.SetSolar(status)
}

func (provider *LiveSemanticProvider) SetCylinders(cylinders []CylinderStatus) {
	if provider == nil {
		return
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(cylinders) == 0 {
		provider.cylinders = nil
		return
	}
	provider.cylinders = cloneCylinderStatuses(cylinders)
}

func (provider *LiveSemanticProvider) SetCylindersFromCache(cylinders []CylinderStatus) {
	provider.SetCylinders(cylinders)
}

func (provider *LiveSemanticProvider) EnergyTotals() *EnergyTotals {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if provider.energy == nil {
		return nil
	}
	copy := *provider.energy
	copy.Gas = cloneEnergyChannel(copy.Gas)
	copy.Electric = cloneEnergyChannel(copy.Electric)
	copy.Solar = cloneEnergyChannel(copy.Solar)
	return &copy
}

func (provider *LiveSemanticProvider) SetZones(zones []Zone) {
	provider.setZonesWithSource(zones, semanticDataSourceLive, "zones_live_update")
}

func (provider *LiveSemanticProvider) SetZonesFromCache(zones []Zone) {
	provider.setZonesWithSource(zones, semanticDataSourceCache, "zones_cache_update")
}

func (provider *LiveSemanticProvider) setZonesWithSource(zones []Zone, source semanticDataSource, reason string) {
	if provider == nil {
		return
	}
	zonesCopy := make([]Zone, len(zones))
	for i, z := range zones {
		zonesCopy[i] = cloneZone(z)
	}
	var transition *phaseTransitionLog
	provider.mu.Lock()
	provider.zones = zonesCopy
	if len(zonesCopy) > 0 {
		provider.zonePublished = true
		if source == semanticDataSourceLive {
			provider.zoneLiveSeen = true
		}
		transition = provider.recordEpochUpdateLocked(source, semanticStreamZones, reason)
	}
	provider.mu.Unlock()
	provider.logPhaseTransition(transition)
}

func (provider *LiveSemanticProvider) SetDHW(status *DhwStatus) {
	provider.setDHWWithSource(status, semanticDataSourceLive, "dhw_live_update")
}

func (provider *LiveSemanticProvider) SetDHWFromCache(status *DhwStatus) {
	provider.setDHWWithSource(status, semanticDataSourceCache, "dhw_cache_update")
}

func (provider *LiveSemanticProvider) setDHWWithSource(status *DhwStatus, source semanticDataSource, reason string) {
	if provider == nil {
		return
	}
	var transition *phaseTransitionLog
	provider.mu.Lock()
	if status == nil {
		provider.dhw = nil
		provider.mu.Unlock()
		return
	}
	cp := cloneDhwStatus(status)
	provider.dhw = cp
	provider.dhwPublished = true
	if source == semanticDataSourceLive {
		provider.dhwLiveSeen = true
	}
	transition = provider.recordEpochUpdateLocked(source, semanticStreamDHW, reason)
	provider.mu.Unlock()
	provider.logPhaseTransition(transition)
}

func (provider *LiveSemanticProvider) SetCircuits(circuits []CircuitStatus) {
	provider.setCircuitsWithSource(circuits, semanticDataSourceLive, "circuits_live_update")
}

func (provider *LiveSemanticProvider) SetCircuitsFromCache(circuits []CircuitStatus) {
	provider.setCircuitsWithSource(circuits, semanticDataSourceCache, "circuits_cache_update")
}

func (provider *LiveSemanticProvider) setCircuitsWithSource(circuits []CircuitStatus, source semanticDataSource, reason string) {
	if provider == nil {
		return
	}

	circuitsCopy := make([]CircuitStatus, len(circuits))
	for i, status := range circuits {
		circuitsCopy[i] = cloneCircuitStatus(status)
	}

	var transition *phaseTransitionLog
	provider.mu.Lock()
	if equalCircuitStatuses(provider.circuits, circuitsCopy) {
		if source == semanticDataSourceLive && len(circuitsCopy) > 0 {
			provider.circuitLiveSeen = true
		}
		provider.mu.Unlock()
		return
	}
	provider.circuits = circuitsCopy
	if len(circuitsCopy) > 0 {
		provider.circuitPublished = true
		if source == semanticDataSourceLive {
			provider.circuitLiveSeen = true
		}
		transition = provider.recordEpochUpdateLocked(source, semanticStreamCircuits, reason)
	}
	provider.mu.Unlock()
	provider.logPhaseTransition(transition)
}

func (provider *LiveSemanticProvider) SetRadioDevices(devices []RadioDevice) {
	provider.setRadioDevicesWithSource(devices, semanticDataSourceLive, "radio_devices_live_update")
}

func (provider *LiveSemanticProvider) SetRadioDevicesFromCache(devices []RadioDevice) {
	provider.setRadioDevicesWithSource(devices, semanticDataSourceCache, "radio_devices_cache_update")
}

func (provider *LiveSemanticProvider) setRadioDevicesWithSource(devices []RadioDevice, source semanticDataSource, reason string) {
	if provider == nil {
		return
	}

	devicesCopy := make([]RadioDevice, len(devices))
	for i, device := range devices {
		devicesCopy[i] = cloneRadioDevice(device)
	}

	var transition *phaseTransitionLog
	provider.mu.Lock()
	if equalRadioDevices(provider.radio, devicesCopy) {
		if source == semanticDataSourceLive && len(devicesCopy) > 0 {
			provider.radioLiveSeen = true
		}
		provider.mu.Unlock()
		return
	}
	provider.radio = devicesCopy
	if len(devicesCopy) > 0 {
		provider.radioPublished = true
		if source == semanticDataSourceLive {
			provider.radioLiveSeen = true
		}
		transition = provider.recordEpochUpdateLocked(source, semanticStreamRadioDevices, reason)
	}
	provider.mu.Unlock()
	provider.logPhaseTransition(transition)
}

func (provider *LiveSemanticProvider) BoilerStatus() *BoilerStatus {
	if provider == nil {
		return nil
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if provider.boiler == nil {
		return nil
	}
	cp := *provider.boiler
	cp.State = cloneBoilerState(cp.State)
	cp.Config = cloneBoilerConfig(cp.Config)
	cp.Diagnostics = cloneBoilerDiagnostics(cp.Diagnostics)
	return &cp
}

func (provider *LiveSemanticProvider) SetBoilerStatus(status *BoilerStatus) {
	provider.setBoilerStatusWithSource(status, semanticDataSourceLive, "boiler_live_update")
}

func (provider *LiveSemanticProvider) SetBoilerStatusFromCache(status *BoilerStatus) {
	provider.setBoilerStatusWithSource(status, semanticDataSourceCache, "boiler_cache_update")
}

func (provider *LiveSemanticProvider) setBoilerStatusWithSource(status *BoilerStatus, source semanticDataSource, reason string) {
	if provider == nil {
		return
	}
	var transition *phaseTransitionLog
	provider.mu.Lock()
	if status == nil {
		provider.boiler = nil
		provider.mu.Unlock()
		return
	}
	cp := *status
	cp.State = cloneBoilerState(cp.State)
	cp.Config = cloneBoilerConfig(cp.Config)
	cp.Diagnostics = cloneBoilerDiagnostics(cp.Diagnostics)
	provider.boiler = &cp
	provider.boilerPublished = true
	if source == semanticDataSourceLive {
		provider.boilerLiveSeen = true
	}
	transition = provider.recordEpochUpdateLocked(source, semanticStreamBoiler, reason)
	provider.mu.Unlock()
	provider.logPhaseTransition(transition)
}

func cloneBoilerState(state BoilerState) BoilerState {
	if state.FlowTemperatureC != nil {
		v := *state.FlowTemperatureC
		state.FlowTemperatureC = &v
	}
	if state.ReturnTemperatureC != nil {
		v := *state.ReturnTemperatureC
		state.ReturnTemperatureC = &v
	}
	if state.CentralHeatingPumpActive != nil {
		v := *state.CentralHeatingPumpActive
		state.CentralHeatingPumpActive = &v
	}
	if state.WaterPressureBar != nil {
		v := *state.WaterPressureBar
		state.WaterPressureBar = &v
	}
	if state.ExternalPumpActive != nil {
		v := *state.ExternalPumpActive
		state.ExternalPumpActive = &v
	}
	if state.CirculationPumpActive != nil {
		v := *state.CirculationPumpActive
		state.CirculationPumpActive = &v
	}
	if state.GasValveActive != nil {
		v := *state.GasValveActive
		state.GasValveActive = &v
	}
	if state.FlameActive != nil {
		v := *state.FlameActive
		state.FlameActive = &v
	}
	if state.DiverterValvePositionPct != nil {
		v := *state.DiverterValvePositionPct
		state.DiverterValvePositionPct = &v
	}
	if state.FanSpeedRpm != nil {
		v := *state.FanSpeedRpm
		state.FanSpeedRpm = &v
	}
	if state.TargetFanSpeedRpm != nil {
		v := *state.TargetFanSpeedRpm
		state.TargetFanSpeedRpm = &v
	}
	if state.IonisationVoltageUa != nil {
		v := *state.IonisationVoltageUa
		state.IonisationVoltageUa = &v
	}
	if state.DhwWaterFlowLpm != nil {
		v := *state.DhwWaterFlowLpm
		state.DhwWaterFlowLpm = &v
	}
	if state.DhwDemandActive != nil {
		v := *state.DhwDemandActive
		state.DhwDemandActive = &v
	}
	if state.HeatingSwitchActive != nil {
		v := *state.HeatingSwitchActive
		state.HeatingSwitchActive = &v
	}
	if state.StorageLoadPumpPct != nil {
		v := *state.StorageLoadPumpPct
		state.StorageLoadPumpPct = &v
	}
	if state.ModulationPct != nil {
		v := *state.ModulationPct
		state.ModulationPct = &v
	}
	if state.PrimaryCircuitFlowLpm != nil {
		v := *state.PrimaryCircuitFlowLpm
		state.PrimaryCircuitFlowLpm = &v
	}
	if state.FlowTempDesiredC != nil {
		v := *state.FlowTempDesiredC
		state.FlowTempDesiredC = &v
	}
	if state.DhwTempDesiredC != nil {
		v := *state.DhwTempDesiredC
		state.DhwTempDesiredC = &v
	}
	if state.StateNumber != nil {
		v := *state.StateNumber
		state.StateNumber = &v
	}
	if state.DhwTemperatureC != nil {
		v := *state.DhwTemperatureC
		state.DhwTemperatureC = &v
	}
	if state.DhwTargetTemperatureC != nil {
		v := *state.DhwTargetTemperatureC
		state.DhwTargetTemperatureC = &v
	}
	return state
}

func cloneBoilerConfig(config BoilerConfig) BoilerConfig {
	if config.DhwOperatingMode != nil {
		v := *config.DhwOperatingMode
		config.DhwOperatingMode = &v
	}
	if config.FlowsetHcMaxC != nil {
		v := *config.FlowsetHcMaxC
		config.FlowsetHcMaxC = &v
	}
	if config.FlowsetHwcMaxC != nil {
		v := *config.FlowsetHwcMaxC
		config.FlowsetHwcMaxC = &v
	}
	if config.PartloadHcKW != nil {
		v := *config.PartloadHcKW
		config.PartloadHcKW = &v
	}
	if config.PartloadHwcKW != nil {
		v := *config.PartloadHwcKW
		config.PartloadHwcKW = &v
	}
	return config
}

func cloneBoilerDiagnostics(diag BoilerDiagnostics) BoilerDiagnostics {
	if diag.HeatingStatusRaw != nil {
		v := *diag.HeatingStatusRaw
		diag.HeatingStatusRaw = &v
	}
	if diag.DhwStatusRaw != nil {
		v := *diag.DhwStatusRaw
		diag.DhwStatusRaw = &v
	}
	if diag.CentralHeatingHours != nil {
		v := *diag.CentralHeatingHours
		diag.CentralHeatingHours = &v
	}
	if diag.DhwHours != nil {
		v := *diag.DhwHours
		diag.DhwHours = &v
	}
	if diag.CentralHeatingStarts != nil {
		v := *diag.CentralHeatingStarts
		diag.CentralHeatingStarts = &v
	}
	if diag.DhwStarts != nil {
		v := *diag.DhwStarts
		diag.DhwStarts = &v
	}
	if diag.PumpHours != nil {
		v := *diag.PumpHours
		diag.PumpHours = &v
	}
	if diag.FanHours != nil {
		v := *diag.FanHours
		diag.FanHours = &v
	}
	if diag.DeactivationsIFC != nil {
		v := *diag.DeactivationsIFC
		diag.DeactivationsIFC = &v
	}
	if diag.DeactivationsTemplimiter != nil {
		v := *diag.DeactivationsTemplimiter
		diag.DeactivationsTemplimiter = &v
	}
	return diag
}

func (provider *LiveSemanticProvider) recordEpochUpdateLocked(source semanticDataSource, stream semanticStreamKind, reason string) *phaseTransitionLog {
	switch source {
	case semanticDataSourceCache:
		provider.cacheEpoch++
		semanticCacheEpoch.Set(int64(provider.cacheEpoch))
		if provider.liveEpoch == 0 && provider.phase != SemanticStartupPhaseDegraded {
			return provider.transitionPhaseLocked(SemanticStartupPhaseCacheLoadedStale, reason)
		}
	case semanticDataSourceLive:
		switch stream {
		case semanticStreamZones:
			provider.zoneLiveSeen = true
		case semanticStreamDHW:
			provider.dhwLiveSeen = true
		case semanticStreamBoiler:
			provider.boilerLiveSeen = true
		case semanticStreamCircuits:
			provider.circuitLiveSeen = true
		case semanticStreamRadioDevices:
			provider.radioLiveSeen = true
		}
		provider.liveEpoch++
		semanticLiveEpoch.Set(int64(provider.liveEpoch))
		if provider.liveEpoch == 1 {
			return provider.transitionPhaseLocked(SemanticStartupPhaseLiveWarmup, reason)
		}
		if provider.liveReadyCriteriaLocked() {
			return provider.transitionPhaseLocked(SemanticStartupPhaseLiveReady, reason)
		}
	}
	return nil
}

func (provider *LiveSemanticProvider) liveReadyCriteriaLocked() bool {
	if provider.zonePublished && !provider.zoneLiveSeen {
		return false
	}
	if provider.dhwPublished && !provider.dhwLiveSeen {
		return false
	}
	if provider.boilerPublished && !provider.boilerLiveSeen {
		return false
	}
	if provider.circuitPublished && !provider.circuitLiveSeen {
		return false
	}
	if provider.radioPublished && !provider.radioLiveSeen {
		return false
	}
	return provider.zoneLiveSeen || provider.dhwLiveSeen || provider.boilerLiveSeen || provider.circuitLiveSeen || provider.radioLiveSeen
}

func (provider *LiveSemanticProvider) transitionPhaseLocked(next SemanticStartupPhase, reason string) *phaseTransitionLog {
	if next == "" || provider.phase == next {
		return nil
	}
	previous := provider.phase
	provider.phase = next
	semanticStartupPhaseTransitionsTotal.Add(fmt.Sprintf("%s->%s", previous, next), 1)
	semanticStartupCurrentPhase.Set(string(next))
	return &phaseTransitionLog{
		previous:   previous,
		next:       next,
		reason:     reason,
		cacheEpoch: provider.cacheEpoch,
		liveEpoch:  provider.liveEpoch,
	}
}

func (provider *LiveSemanticProvider) logPhaseTransition(transition *phaseTransitionLog) {
	if provider == nil || transition == nil {
		return
	}
	logger := provider.phaseLogger
	if logger == nil {
		logger = log.Printf
	}
	logger(
		"semantic_startup_phase_transition from=%s to=%s reason=%q cache_epoch=%d live_epoch=%d",
		transition.previous,
		transition.next,
		transition.reason,
		transition.cacheEpoch,
		transition.liveEpoch,
	)
}

// ApplyBroadcast updates semantic snapshots based on router broadcasts.
// Returns the updated totals if energy data changed.
func (provider *LiveSemanticProvider) ApplyBroadcast(event router.BroadcastEvent) (*EnergyTotals, bool) {
	if provider == nil {
		return nil, false
	}
	if len(event.Values) == 0 {
		return nil, false
	}

	if updated := provider.applyEnergy(event.Values, time.Now()); !updated {
		return nil, false
	}
	return provider.EnergyTotals(), true
}

// ApplyEnergyFromRegister updates semantic energy snapshots from register reads.
// Returns true when the merge store accepted the value.
func (provider *LiveSemanticProvider) ApplyEnergyFromRegister(key EnergyMergeKey, kwh float64) bool {
	if provider == nil || provider.energyMerge == nil {
		return false
	}
	now := time.Now()
	return provider.applyEnergyPoint(key, kwh, EnergySourceRegister, now)
}

func (provider *LiveSemanticProvider) applyEnergy(values map[string]types.Value, now time.Time) bool {
	wh, ok := floatValue(values, "wh")
	if !ok {
		return false
	}
	source, ok := stringValue(values, "source")
	if !ok {
		return false
	}
	usage, ok := stringValue(values, "usage")
	if !ok {
		return false
	}
	period, ok := stringValue(values, "period")
	if !ok {
		return false
	}

	// Validate channel name.
	switch source {
	case "gas", "electricity", "solar":
	default:
		return false
	}

	// Validate usage name.
	switch usage {
	case "hot_water", "heating", "cooling":
	default:
		return false
	}

	kwh := wh / 1000.0

	// Build merge key and apply through the merge store.
	// The merge store has its own mutex; do NOT hold provider.mu here.
	var key energyMergeKey
	switch period {
	case "day":
		if !matchesToday(values, now) {
			return false
		}
		key = energyMergeKey{Channel: source, Usage: usage, Period: "day"}
	case "year":
		yearKind, ok := stringValue(values, "year_kind")
		if !ok {
			return false
		}
		switch yearKind {
		case "previous", "current":
		default:
			return false
		}
		key = energyMergeKey{Channel: source, Usage: usage, Period: "year", YearKind: yearKind}
	default:
		return false
	}

	return provider.applyEnergyPoint(key, kwh, EnergySourceBroadcast, now)
}

func (provider *LiveSemanticProvider) applyEnergyPoint(key energyMergeKey, kwh float64, source EnergyDataSource, now time.Time) bool {
	if provider == nil || provider.energyMerge == nil {
		return false
	}
	if !provider.energyMerge.Apply(key, kwh, source, now) {
		return false
	}

	// Rebuild the snapshot from the merge store and publish atomically.
	// Only publish if our revision is still the latest (prevents stale overwrites
	// from concurrent callers).
	rev := provider.energyMerge.Revision()
	snapshot := provider.energyMerge.Snapshot()
	provider.mu.Lock()
	if rev >= provider.energyRevision {
		provider.energy = snapshot
		provider.energyRevision = rev
	}
	provider.mu.Unlock()
	return true
}

func matchesToday(values map[string]types.Value, now time.Time) bool {
	day, okDay := uintValue(values, "day")
	month, okMonth := uintValue(values, "month")
	if okMonth && int(month) != int(now.Month()) {
		return false
	}
	if okDay && int(day) != now.Day() {
		return false
	}
	return true
}

func stringValue(values map[string]types.Value, key string) (string, bool) {
	value, ok := values[key]
	if !ok || !value.Valid {
		return "", false
	}
	stringValue, ok := value.Value.(string)
	if !ok {
		return "", false
	}
	return stringValue, true
}

func floatValue(values map[string]types.Value, key string) (float64, bool) {
	value, ok := values[key]
	if !ok || !value.Valid {
		return 0, false
	}
	switch typed := value.Value.(type) {
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	default:
		return 0, false
	}
}

func uintValue(values map[string]types.Value, key string) (uint, bool) {
	value, ok := values[key]
	if !ok || !value.Valid {
		return 0, false
	}
	switch typed := value.Value.(type) {
	case uint8:
		return uint(typed), true
	case uint16:
		return uint(typed), true
	case uint32:
		return uint(typed), true
	case uint64:
		return uint(typed), true
	case uint:
		return typed, true
	case int:
		if typed < 0 {
			return 0, false
		}
		return uint(typed), true
	case int32:
		if typed < 0 {
			return 0, false
		}
		return uint(typed), true
	case int64:
		if typed < 0 {
			return 0, false
		}
		return uint(typed), true
	default:
		return 0, false
	}
}

func cloneEnergyChannel(channel EnergyChannel) EnergyChannel {
	channel.DHW = cloneEnergySeries(channel.DHW)
	channel.Climate = cloneEnergySeries(channel.Climate)
	return channel
}

func cloneEnergySeries(series EnergySeries) EnergySeries {
	if len(series.Yearly) > 0 {
		copySlice := make([]float64, len(series.Yearly))
		copy(copySlice, series.Yearly)
		series.Yearly = copySlice
	}
	return series
}

func cloneZone(z Zone) Zone {
	out := z
	out.State = cloneZoneState(z.State)
	out.Config = cloneZoneConfig(z.Config)
	return out
}

func cloneZoneState(s ZoneState) ZoneState {
	out := s
	if s.CurrentTempC != nil {
		v := *s.CurrentTempC
		out.CurrentTempC = &v
	}
	if s.CurrentHumidityPct != nil {
		v := *s.CurrentHumidityPct
		out.CurrentHumidityPct = &v
	}
	if s.HeatingDemandPct != nil {
		v := *s.HeatingDemandPct
		out.HeatingDemandPct = &v
	}
	if s.ValvePositionPct != nil {
		v := *s.ValvePositionPct
		out.ValvePositionPct = &v
	}
	return out
}

func cloneZoneConfig(c ZoneConfig) ZoneConfig {
	out := c
	if c.TargetTempC != nil {
		v := *c.TargetTempC
		out.TargetTempC = &v
	}
	if len(c.AllowedModes) > 0 {
		out.AllowedModes = make([]string, len(c.AllowedModes))
		copy(out.AllowedModes, c.AllowedModes)
	}
	if c.AssociatedCircuit != nil {
		v := *c.AssociatedCircuit
		out.AssociatedCircuit = &v
	}
	if c.RoomTemperatureZoneMapping != nil {
		v := *c.RoomTemperatureZoneMapping
		out.RoomTemperatureZoneMapping = &v
	}
	return out
}

func cloneDhwStatus(s *DhwStatus) *DhwStatus {
	if s == nil {
		return nil
	}
	out := *s
	out.State = cloneDhwState(s.State)
	out.Config = cloneDhwConfig(s.Config)
	return &out
}

func cloneDhwState(s DhwState) DhwState {
	out := s
	if s.CurrentTempC != nil {
		v := *s.CurrentTempC
		out.CurrentTempC = &v
	}
	if s.HeatingDemandPct != nil {
		v := *s.HeatingDemandPct
		out.HeatingDemandPct = &v
	}
	return out
}

func cloneDhwConfig(c DhwConfig) DhwConfig {
	out := c
	if c.TargetTempC != nil {
		v := *c.TargetTempC
		out.TargetTempC = &v
	}
	return out
}

func cloneCircuitStatus(status CircuitStatus) CircuitStatus {
	out := status
	out.State = cloneCircuitState(status.State)
	out.Config = cloneCircuitConfig(status.Config)
	out.ManagingDevice = cloneManagingDevice(status.ManagingDevice)
	return out
}

func cloneManagingDevice(device ManagingDevice) ManagingDevice {
	out := device
	if device.Role == "" {
		out.Role = ManagingDeviceRoleUnknown
	}
	if device.DeviceID != nil {
		v := *device.DeviceID
		out.DeviceID = &v
	}
	if device.Address != nil {
		v := *device.Address
		out.Address = &v
	}
	return out
}

func cloneCircuitState(state CircuitState) CircuitState {
	out := state
	if state.PumpActive != nil {
		v := *state.PumpActive
		out.PumpActive = &v
	}
	if state.MixerPositionPct != nil {
		v := *state.MixerPositionPct
		out.MixerPositionPct = &v
	}
	if state.FlowTemperatureC != nil {
		v := *state.FlowTemperatureC
		out.FlowTemperatureC = &v
	}
	if state.FlowSetpointC != nil {
		v := *state.FlowSetpointC
		out.FlowSetpointC = &v
	}
	if state.CalcFlowTempC != nil {
		v := *state.CalcFlowTempC
		out.CalcFlowTempC = &v
	}
	if state.Humidity != nil {
		v := *state.Humidity
		out.Humidity = &v
	}
	if state.DewPoint != nil {
		v := *state.DewPoint
		out.DewPoint = &v
	}
	if state.PumpHours != nil {
		v := *state.PumpHours
		out.PumpHours = &v
	}
	if state.PumpStarts != nil {
		v := *state.PumpStarts
		out.PumpStarts = &v
	}
	return out
}

func cloneCircuitConfig(config CircuitConfig) CircuitConfig {
	out := config
	if config.HeatingCurve != nil {
		v := *config.HeatingCurve
		out.HeatingCurve = &v
	}
	if config.FlowTempMaxC != nil {
		v := *config.FlowTempMaxC
		out.FlowTempMaxC = &v
	}
	if config.FlowTempMinC != nil {
		v := *config.FlowTempMinC
		out.FlowTempMinC = &v
	}
	if config.SummerLimitC != nil {
		v := *config.SummerLimitC
		out.SummerLimitC = &v
	}
	if config.FrostProtC != nil {
		v := *config.FrostProtC
		out.FrostProtC = &v
	}
	if config.CoolingEnabled != nil {
		v := *config.CoolingEnabled
		out.CoolingEnabled = &v
	}
	return out
}

func cloneRadioDevice(device RadioDevice) RadioDevice {
	out := device
	if device.DeviceConnected != nil {
		v := *device.DeviceConnected
		out.DeviceConnected = &v
	}
	if device.DeviceClassAddress != nil {
		v := *device.DeviceClassAddress
		out.DeviceClassAddress = &v
	}
	if device.FirmwareVersion != nil {
		v := *device.FirmwareVersion
		out.FirmwareVersion = &v
	}
	if device.HardwareIdentifier != nil {
		v := *device.HardwareIdentifier
		out.HardwareIdentifier = &v
	}
	if device.RemoteControlAddress != nil {
		v := *device.RemoteControlAddress
		out.RemoteControlAddress = &v
	}
	if device.DevicePaired != nil {
		v := *device.DevicePaired
		out.DevicePaired = &v
	}
	if device.ReceptionStrength != nil {
		v := *device.ReceptionStrength
		out.ReceptionStrength = &v
	}
	if device.ZoneAssignment != nil {
		v := *device.ZoneAssignment
		out.ZoneAssignment = &v
	}
	if device.RoomTemperatureC != nil {
		v := *device.RoomTemperatureC
		out.RoomTemperatureC = &v
	}
	if device.RoomHumidityPct != nil {
		v := *device.RoomHumidityPct
		out.RoomHumidityPct = &v
	}
	return out
}

func cloneSolarStatus(status *SolarStatus) *SolarStatus {
	if status == nil {
		return nil
	}
	out := *status
	if status.CollectorTemperatureC != nil {
		v := *status.CollectorTemperatureC
		out.CollectorTemperatureC = &v
	}
	if status.ReturnTemperatureC != nil {
		v := *status.ReturnTemperatureC
		out.ReturnTemperatureC = &v
	}
	if status.PumpActive != nil {
		v := *status.PumpActive
		out.PumpActive = &v
	}
	if status.CurrentYield != nil {
		v := *status.CurrentYield
		out.CurrentYield = &v
	}
	if status.PumpHours != nil {
		v := *status.PumpHours
		out.PumpHours = &v
	}
	if status.SolarEnabled != nil {
		v := *status.SolarEnabled
		out.SolarEnabled = &v
	}
	if status.FunctionMode != nil {
		v := *status.FunctionMode
		out.FunctionMode = &v
	}
	return &out
}

func cloneCylinderStatus(status CylinderStatus) CylinderStatus {
	out := status
	if status.TemperatureC != nil {
		v := *status.TemperatureC
		out.TemperatureC = &v
	}
	if status.MaxSetpointC != nil {
		v := *status.MaxSetpointC
		out.MaxSetpointC = &v
	}
	if status.ChargeHysteresisC != nil {
		v := *status.ChargeHysteresisC
		out.ChargeHysteresisC = &v
	}
	if status.ChargeOffsetC != nil {
		v := *status.ChargeOffsetC
		out.ChargeOffsetC = &v
	}
	return out
}

func cloneCylinderStatuses(cylinders []CylinderStatus) []CylinderStatus {
	if len(cylinders) == 0 {
		return nil
	}
	out := make([]CylinderStatus, len(cylinders))
	for i := range cylinders {
		out[i] = cloneCylinderStatus(cylinders[i])
	}
	return out
}

func equalRadioDevices(left []RadioDevice, right []RadioDevice) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !equalRadioDevice(left[i], right[i]) {
			return false
		}
	}
	return true
}

func equalRadioDevice(left RadioDevice, right RadioDevice) bool {
	return left.Group == right.Group &&
		left.Instance == right.Instance &&
		left.SlotMode == right.SlotMode &&
		equalBoolPointer(left.DeviceConnected, right.DeviceConnected) &&
		equalIntPointer(left.DeviceClassAddress, right.DeviceClassAddress) &&
		left.DeviceModel == right.DeviceModel &&
		equalStringPointer(left.FirmwareVersion, right.FirmwareVersion) &&
		equalIntPointer(left.HardwareIdentifier, right.HardwareIdentifier) &&
		equalIntPointer(left.RemoteControlAddress, right.RemoteControlAddress) &&
		equalBoolPointer(left.DevicePaired, right.DevicePaired) &&
		equalIntPointer(left.ReceptionStrength, right.ReceptionStrength) &&
		equalIntPointer(left.ZoneAssignment, right.ZoneAssignment) &&
		equalFloat64Pointer(left.RoomTemperatureC, right.RoomTemperatureC) &&
		equalFloat64Pointer(left.RoomHumidityPct, right.RoomHumidityPct)
}

func equalCircuitStatuses(left []CircuitStatus, right []CircuitStatus) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !equalCircuitStatus(left[i], right[i]) {
			return false
		}
	}
	return true
}

func equalCircuitStatus(left CircuitStatus, right CircuitStatus) bool {
	if left.Index != right.Index ||
		left.CircuitType != right.CircuitType ||
		left.HasMixer != right.HasMixer {
		return false
	}
	if !equalCircuitState(left.State, right.State) {
		return false
	}
	return equalCircuitConfig(left.Config, right.Config)
}

func equalCircuitState(left CircuitState, right CircuitState) bool {
	if !equalBoolPointer(left.PumpActive, right.PumpActive) ||
		!equalFloat64Pointer(left.MixerPositionPct, right.MixerPositionPct) ||
		!equalFloat64Pointer(left.FlowTemperatureC, right.FlowTemperatureC) ||
		!equalFloat64Pointer(left.FlowSetpointC, right.FlowSetpointC) ||
		!equalFloat64Pointer(left.CalcFlowTempC, right.CalcFlowTempC) ||
		left.CircuitState != right.CircuitState ||
		!equalFloat64Pointer(left.Humidity, right.Humidity) ||
		!equalFloat64Pointer(left.DewPoint, right.DewPoint) ||
		!equalFloat64Pointer(left.PumpHours, right.PumpHours) ||
		!equalIntPointer(left.PumpStarts, right.PumpStarts) {
		return false
	}
	return true
}

func equalCircuitConfig(left CircuitConfig, right CircuitConfig) bool {
	if !equalFloat64Pointer(left.HeatingCurve, right.HeatingCurve) ||
		!equalFloat64Pointer(left.FlowTempMaxC, right.FlowTempMaxC) ||
		!equalFloat64Pointer(left.FlowTempMinC, right.FlowTempMinC) ||
		!equalFloat64Pointer(left.SummerLimitC, right.SummerLimitC) ||
		!equalFloat64Pointer(left.FrostProtC, right.FrostProtC) ||
		left.RoomTempControl != right.RoomTempControl ||
		!equalBoolPointer(left.CoolingEnabled, right.CoolingEnabled) {
		return false
	}
	return true
}

func equalFloat64Pointer(left *float64, right *float64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func equalBoolPointer(left *bool, right *bool) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func equalIntPointer(left *int, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func equalStringPointer(left *string, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

var _ SemanticProvider = (*LiveSemanticProvider)(nil)
