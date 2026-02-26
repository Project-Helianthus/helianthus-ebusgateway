package graphql

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/d3vi1/helianthus-ebusgo/types"
	"github.com/d3vi1/helianthus-ebusreg/router"
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
)

type phaseTransitionLog struct {
	previous   SemanticStartupPhase
	next       SemanticStartupPhase
	reason     string
	cacheEpoch uint64
	liveEpoch  uint64
}

// LiveSemanticProvider maintains semantic snapshots derived from bus data.
type LiveSemanticProvider struct {
	mu     sync.RWMutex
	zones  []Zone
	dhw    *DhwStatus
	energy *EnergyTotals

	energyMerge    *energyMergeStore
	energyRevision uint64

	phase              SemanticStartupPhase
	cacheEpoch         uint64
	liveEpoch          uint64
	zonePublished      bool
	dhwPublished       bool
	zoneLiveSeen       bool
	dhwLiveSeen        bool
	bootMonitorStarted bool
	phaseLogger        func(string, ...any)
}

func NewLiveSemanticProvider() *LiveSemanticProvider {
	return &LiveSemanticProvider{
		phase:       SemanticStartupPhaseBootInit,
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
	copy(zones, provider.zones)
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
	copy := *provider.dhw
	return &copy
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
	copy(zonesCopy, zones)
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
	copy := *status
	provider.dhw = &copy
	provider.dhwPublished = true
	if source == semanticDataSourceLive {
		provider.dhwLiveSeen = true
	}
	transition = provider.recordEpochUpdateLocked(source, semanticStreamDHW, reason)
	provider.mu.Unlock()
	provider.logPhaseTransition(transition)
}

func (provider *LiveSemanticProvider) recordEpochUpdateLocked(source semanticDataSource, stream semanticStreamKind, reason string) *phaseTransitionLog {
	switch source {
	case semanticDataSourceCache:
		provider.cacheEpoch++
		if provider.liveEpoch == 0 && provider.phase != SemanticStartupPhaseDegraded {
			return provider.transitionPhaseLocked(SemanticStartupPhaseCacheLoadedStale, reason)
		}
	case semanticDataSourceLive:
		switch stream {
		case semanticStreamZones:
			provider.zoneLiveSeen = true
		case semanticStreamDHW:
			provider.dhwLiveSeen = true
		}
		provider.liveEpoch++
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
	return provider.zoneLiveSeen || provider.dhwLiveSeen
}

func (provider *LiveSemanticProvider) transitionPhaseLocked(next SemanticStartupPhase, reason string) *phaseTransitionLog {
	if next == "" || provider.phase == next {
		return nil
	}
	previous := provider.phase
	provider.phase = next
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

var _ SemanticProvider = (*LiveSemanticProvider)(nil)
