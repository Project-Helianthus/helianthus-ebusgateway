package graphql

import (
	"expvar"
	"log"
	"sync"
	"time"
)

// EnergyDataSource indicates where an energy data point originated.
type EnergyDataSource uint8

const (
	// EnergySourceBroadcast is a low-confidence source from bus broadcasts.
	EnergySourceBroadcast EnergyDataSource = iota
	// EnergySourceRegister is a high-confidence source from direct register reads.
	EnergySourceRegister
)

var (
	semanticEnergyMergesTotal     = expvar.NewMap("semantic_energy_merges_total")
	semanticEnergyRejectionsTotal = expvar.NewMap("semantic_energy_rejections_total")
	energyBroadcastSelectors      = expvar.NewMap("energy_broadcast_selectors")
	energyBroadcastTransitions    = expvar.NewMap("energy_broadcast_freshness_transitions_total")
)

const (
	energyBroadcastWarmupTTL      = 3 * time.Minute
	energyBroadcastUnavailableTTL = 45 * time.Minute
)

// energyDataPoint tracks a single energy value with its source and ingest time.
type energyDataPoint struct {
	Value    float64
	Source   EnergyDataSource
	IngestAt time.Time
}

// energyMergeKey uniquely identifies an energy data point.
// Usage is canonicalized: "heating" and "cooling" both map to "climate".
type energyMergeKey struct {
	Channel  string // "gas", "electricity", "solar"
	Usage    string // "hot_water", "climate" (canonicalized from heating/cooling)
	Period   string // "day", "year", "month"
	YearKind string // "" for day, "previous"/"current" for year/month
}

// EnergyMergeKey is the external key contract for register/broadcast energy merge inputs.
type EnergyMergeKey = energyMergeKey

// canonicalizeUsage maps raw usage strings to canonical merge key values.
// "heating" and "cooling" both target the same Climate series.
func canonicalizeUsage(usage string) string {
	switch usage {
	case "heating", "cooling":
		return "climate"
	default:
		return usage
	}
}

// energyMergeStore holds all tracked energy data points, indexed by a composite key.
//
// Merge rules (truth table):
//
//	| Existing Source | Incoming Source | Incoming Newer? | Action                              |
//	|-----------------|----------------|-----------------|-------------------------------------|
//	| none            | any            | -               | accept                              |
//	| broadcast       | register       | any             | accept (register always wins)       |
//	| broadcast       | broadcast      | yes             | accept                              |
//	| broadcast       | broadcast      | no              | reject (monotonic)                  |
//	| register        | register       | yes             | accept                              |
//	| register        | register       | no              | reject (monotonic)                  |
//	| register        | broadcast      | any             | reject (broadcast never overwrites) |
type energyMergeStore struct {
	mu                   sync.RWMutex
	points               map[energyMergeKey]energyDataPoint
	revision             uint64
	broadcastStates      map[energyMergeKey]EnergyFreshnessState
	broadcastStateCounts map[EnergyFreshnessState]int
}

func newEnergyMergeStore() *energyMergeStore {
	store := &energyMergeStore{
		points:               make(map[energyMergeKey]energyDataPoint),
		broadcastStates:      make(map[energyMergeKey]EnergyFreshnessState),
		broadcastStateCounts: make(map[EnergyFreshnessState]int),
	}
	store.reconcileBroadcastStatesLocked(time.Now(), "")
	return store
}

// Apply attempts to merge an incoming energy value into the store.
// Returns true if the value was accepted according to the merge rules.
// The key's Usage is canonicalized internally.
func (s *energyMergeStore) Apply(key energyMergeKey, value float64, source EnergyDataSource, ingestAt time.Time) bool {
	key.Usage = canonicalizeUsage(key.Usage)
	sourceLabel := energySourceLabel(source)
	if ingestAt.IsZero() {
		ingestAt = time.Now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.points[key]
	if !exists {
		// No existing point: accept any source.
		s.points[key] = energyDataPoint{Value: value, Source: source, IngestAt: ingestAt}
		s.revision++
		semanticEnergyMergesTotal.Add(sourceLabel, 1)
		s.reconcileBroadcastStatesLocked(ingestAt, "")
		log.Printf("semantic_energy_merge_accept source=%s", sourceLabel)
		return true
	}

	switch {
	case existing.Source == EnergySourceRegister && source == EnergySourceBroadcast:
		// Broadcast never overwrites register.
		semanticEnergyRejectionsTotal.Add("source_downgrade", 1)
		log.Printf("semantic_energy_merge_reject reason=source_downgrade source=%s existing_source=%s", sourceLabel, energySourceLabel(existing.Source))
		return false
	case existing.Source == EnergySourceBroadcast && source == EnergySourceRegister:
		// Register always wins over broadcast, regardless of timestamp.
		s.points[key] = energyDataPoint{Value: value, Source: source, IngestAt: ingestAt}
		s.revision++
		semanticEnergyMergesTotal.Add(sourceLabel, 1)
		s.reconcileBroadcastStatesLocked(ingestAt, "")
		log.Printf("semantic_energy_merge_accept source=%s", sourceLabel)
		return true
	default:
		// Same source: enforce monotonic ingest timestamp.
		if !ingestAt.After(existing.IngestAt) {
			semanticEnergyRejectionsTotal.Add("monotonic", 1)
			log.Printf("semantic_energy_merge_reject reason=monotonic source=%s", sourceLabel)
			return false
		}
		s.points[key] = energyDataPoint{Value: value, Source: source, IngestAt: ingestAt}
		s.revision++
		semanticEnergyMergesTotal.Add(sourceLabel, 1)
		s.reconcileBroadcastStatesLocked(ingestAt, "")
		log.Printf("semantic_energy_merge_accept source=%s", sourceLabel)
		return true
	}
}

func energySourceLabel(source EnergyDataSource) string {
	switch source {
	case EnergySourceRegister:
		return "register"
	default:
		return "broadcast"
	}
}

// Revision returns the current store revision. Each accepted Apply increments it.
func (s *energyMergeStore) Revision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

// Snapshot builds an EnergyTotals from all current points in the store.
func (s *energyMergeStore) Snapshot() *EnergyTotals {
	return s.SnapshotWithContext(time.Now(), "")
}

func (s *energyMergeStore) SnapshotWithContext(now time.Time, passiveState string) *EnergyTotals {
	if s == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	totals := &EnergyTotals{}

	for key, point := range s.points {
		channel := mergeSelectChannel(totals, key.Channel)
		if channel == nil {
			continue
		}
		series := mergeSelectUsage(channel, key.Usage)
		if series == nil {
			continue
		}

		switch key.Period {
		case "day":
			series.Today = point.Value
			series.TodayMeta = energyPointMetaFromDataPoint(&point, energyFreshnessStateFor(point, now, passiveState), now)
		case "year":
			if len(series.Yearly) < 2 {
				series.Yearly = make([]float64, 2)
			}
			if len(series.YearlyMeta) < 2 {
				series.YearlyMeta = make([]EnergyPointMeta, 2)
			}
			switch key.YearKind {
			case "previous":
				series.Yearly[0] = point.Value
				series.YearlyMeta[0] = energyPointMetaFromDataPoint(&point, energyFreshnessStateFor(point, now, passiveState), now)
			case "current":
				series.Yearly[1] = point.Value
				series.YearlyMeta[1] = energyPointMetaFromDataPoint(&point, energyFreshnessStateFor(point, now, passiveState), now)
			}
		case "month":
			if len(series.Monthly) < 2 {
				series.Monthly = make([]float64, 2)
			}
			if len(series.MonthlyMeta) < 2 {
				series.MonthlyMeta = make([]EnergyPointMeta, 2)
			}
			switch key.YearKind {
			case "previous":
				series.Monthly[0] = point.Value
				series.MonthlyMeta[0] = energyPointMetaFromDataPoint(&point, energyFreshnessStateFor(point, now, passiveState), now)
			case "current":
				series.Monthly[1] = point.Value
				series.MonthlyMeta[1] = energyPointMetaFromDataPoint(&point, energyFreshnessStateFor(point, now, passiveState), now)
			}
		}
	}

	// Ensure all B516 selectors are observable with explicit freshness metadata,
	// even when the value has never been seen.
	for _, key := range energyBroadcastSelectorCatalog() {
		channel := mergeSelectChannel(totals, key.Channel)
		if channel == nil {
			continue
		}
		series := mergeSelectUsage(channel, key.Usage)
		if series == nil {
			continue
		}
		point, exists := s.points[key]
		state := energyFreshnessForOptionalPoint(exists, point, now, passiveState)
		meta := energyPointMetaFromOptionalPoint(exists, point, state, now)
		switch key.Period {
		case "day":
			if series.TodayMeta.FreshnessState == "" {
				series.TodayMeta = meta
			}
		case "year":
			if len(series.YearlyMeta) < 2 {
				series.YearlyMeta = make([]EnergyPointMeta, 2)
			}
			if key.YearKind == "previous" && series.YearlyMeta[0].FreshnessState == "" {
				series.YearlyMeta[0] = meta
			}
			if key.YearKind == "current" && series.YearlyMeta[1].FreshnessState == "" {
				series.YearlyMeta[1] = meta
			}
		}
	}

	s.reconcileBroadcastStatesLocked(now, passiveState)

	return totals
}

// RefreshFreshnessMetricsWithContext recomputes freshness selector gauges/counters
// for the current clock/passive state without requiring a concurrent energy read/apply.
func (s *energyMergeStore) RefreshFreshnessMetricsWithContext(now time.Time, passiveState string) {
	if s == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconcileBroadcastStatesLocked(now, passiveState)
}

func energyPointMetaFromDataPoint(point *energyDataPoint, state EnergyFreshnessState, now time.Time) EnergyPointMeta {
	if point == nil {
		return EnergyPointMeta{
			FreshnessState: state,
			Provenance:     EnergyProvenanceNone,
		}
	}
	return energyPointMetaFromOptionalPoint(true, *point, state, now)
}

func energyPointMetaFromOptionalPoint(exists bool, point energyDataPoint, state EnergyFreshnessState, now time.Time) EnergyPointMeta {
	meta := EnergyPointMeta{
		FreshnessState: state,
		Provenance:     EnergyProvenanceNone,
	}
	if !exists {
		return meta
	}
	if point.Source == EnergySourceRegister {
		meta.Provenance = EnergyProvenanceRegister
	} else {
		meta.Provenance = EnergyProvenanceBroadcast
	}
	if !point.IngestAt.IsZero() {
		meta.LastObservedUTC = point.IngestAt.UTC().Format(time.RFC3339)
		age := now.Sub(point.IngestAt)
		if age < 0 {
			age = 0
		}
		meta.AgeSeconds = age.Seconds()
	}
	meta.Stale = state == EnergyFreshnessStateStale || state == EnergyFreshnessStateUnavailable
	return meta
}

func energyFreshnessStateFor(point energyDataPoint, now time.Time, passiveState string) EnergyFreshnessState {
	return energyFreshnessForOptionalPoint(true, point, now, passiveState)
}

func energyFreshnessForOptionalPoint(exists bool, point energyDataPoint, now time.Time, passiveState string) EnergyFreshnessState {
	if !exists {
		switch passiveState {
		case "unavailable":
			return EnergyFreshnessStateUnavailable
		case "warming_up":
			return EnergyFreshnessStateWarmingUp
		default:
			return EnergyFreshnessStateNeverSeen
		}
	}

	if point.Source == EnergySourceRegister {
		return EnergyFreshnessStateFresh
	}

	age := now.Sub(point.IngestAt)
	if age < 0 {
		age = 0
	}
	switch passiveState {
	case "warming_up":
		return EnergyFreshnessStateWarmingUp
	case "unavailable":
		if age >= energyBroadcastUnavailableTTL {
			return EnergyFreshnessStateUnavailable
		}
		return EnergyFreshnessStateStale
	}
	if age <= energyBroadcastWarmupTTL {
		return EnergyFreshnessStateWarmingUp
	}
	return EnergyFreshnessStateStale
}

func (s *energyMergeStore) reconcileBroadcastStatesLocked(now time.Time, passiveState string) {
	nextCounts := make(map[EnergyFreshnessState]int)
	for _, key := range energyBroadcastSelectorCatalog() {
		point, exists := s.points[key]
		nextState := energyFreshnessForOptionalPoint(exists, point, now, passiveState)
		prevState := s.broadcastStates[key]
		if prevState == "" {
			prevState = EnergyFreshnessStateNeverSeen
		}
		if prevState != nextState {
			energyBroadcastTransitions.Add(string(prevState)+"->"+string(nextState), 1)
		}
		s.broadcastStates[key] = nextState
		nextCounts[nextState]++
	}
	for _, state := range energyFreshnessStates() {
		delta := nextCounts[state] - s.broadcastStateCounts[state]
		if delta != 0 {
			energyBroadcastSelectors.Add(string(state), int64(delta))
		}
		s.broadcastStateCounts[state] = nextCounts[state]
	}
}

func energyFreshnessStates() []EnergyFreshnessState {
	return []EnergyFreshnessState{
		EnergyFreshnessStateNeverSeen,
		EnergyFreshnessStateFresh,
		EnergyFreshnessStateWarmingUp,
		EnergyFreshnessStateStale,
		EnergyFreshnessStateUnavailable,
	}
}

func energyBroadcastSelectorCatalog() []energyMergeKey {
	channels := []string{"gas", "electricity", "solar"}
	usages := []string{"hot_water", "climate"}
	out := make([]energyMergeKey, 0, len(channels)*len(usages)*3)
	for _, channel := range channels {
		for _, usage := range usages {
			out = append(out, energyMergeKey{
				Channel: channel,
				Usage:   usage,
				Period:  "day",
			})
			out = append(out, energyMergeKey{
				Channel:  channel,
				Usage:    usage,
				Period:   "year",
				YearKind: "previous",
			})
			out = append(out, energyMergeKey{
				Channel:  channel,
				Usage:    usage,
				Period:   "year",
				YearKind: "current",
			})
		}
	}
	return out
}

// mergeSelectUsage returns a pointer to the EnergySeries for the canonical usage.
func mergeSelectUsage(channel *EnergyChannel, usage string) *EnergySeries {
	if channel == nil {
		return nil
	}
	switch usage {
	case "hot_water":
		return &channel.DHW
	case "climate":
		return &channel.Climate
	default:
		return nil
	}
}

// mergeSelectChannel returns a pointer to the EnergyChannel for the given
// channel name, or nil if unrecognised.
func mergeSelectChannel(totals *EnergyTotals, channel string) *EnergyChannel {
	if totals == nil {
		return nil
	}
	switch channel {
	case "gas":
		return &totals.Gas
	case "electricity":
		return &totals.Electric
	case "solar":
		return &totals.Solar
	default:
		return nil
	}
}
