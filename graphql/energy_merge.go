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

// energyMergeKey uniquely identifies an energy data point by its absolute
// window identity. Usage is canonicalized: "heating" and "cooling" both map
// to "climate".
//
// Day keys carry Year/Month/Day, month keys Year/Month, year keys Year; unused
// components are zero. The public two-slot (previous/current) series is a
// projection of these keys against a reference date at snapshot time, so
// the stored identity never goes stale across a year or month boundary.
//
// Period "all" is the lifetime total (register-primary): it carries no date and
// is never keyed by a reference year. Its documented compatibility mapping is
// the current-year slot, with the previous-year slot and today held at zero.
type energyMergeKey struct {
	Channel string // "gas", "electricity", "solar"
	Usage   string // "hot_water", "climate" (canonicalized from heating/cooling)
	Period  string // "day", "year", "month", or "all" (lifetime total)
	Year    int
	Month   int
	Day     int
}

// EnergyMergeKey is the external key contract for register/broadcast energy merge inputs.
type EnergyMergeKey = energyMergeKey

// energyWindowKey builds the dated key of a relative window against a
// reference date. label is "current" or "previous" for year/month periods and
// ignored for "day" (always the reference day).
func energyWindowKey(channel, usage, period, label string, ref time.Time) energyMergeKey {
	key := energyMergeKey{Channel: channel, Usage: usage, Period: period}
	switch period {
	case "day":
		key.Year, key.Month, key.Day = ref.Year(), int(ref.Month()), ref.Day()
	case "year":
		key.Year = ref.Year()
		if label == "previous" {
			key.Year--
		}
	case "month":
		first := time.Date(ref.Year(), ref.Month(), 1, 0, 0, 0, 0, time.UTC)
		if label == "previous" {
			first = first.AddDate(0, -1, 0)
		}
		key.Year, key.Month = first.Year(), int(first.Month())
	}
	return key
}

// energyLifetimeKey is the undated key of a channel/usage lifetime total.
func energyLifetimeKey(channel, usage string) energyMergeKey {
	return energyMergeKey{Channel: channel, Usage: usage, Period: "all"}
}

// energySlotFor projects a dated key onto the public two-slot series
// (0 = previous, 1 = current) relative to ref. For the day period it reports
// slot 1 when the key is the reference day. ok is false when the window lies
// outside the public projection.
func energySlotFor(key energyMergeKey, ref time.Time) (int, bool) {
	switch key.Period {
	case "day":
		return 1, key.Year == ref.Year() && key.Month == int(ref.Month()) && key.Day == ref.Day()
	case "year":
		switch key.Year {
		case ref.Year():
			return 1, true
		case ref.Year() - 1:
			return 0, true
		}
	case "month":
		if k := energyWindowKey(key.Channel, key.Usage, "month", "current", ref); k.Year == key.Year && k.Month == key.Month {
			return 1, true
		}
		if k := energyWindowKey(key.Channel, key.Usage, "month", "previous", ref); k.Year == key.Year && k.Month == key.Month {
			return 0, true
		}
	}
	return 0, false
}

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
	return &energyMergeStore{
		points:               make(map[energyMergeKey]energyDataPoint),
		broadcastStates:      make(map[energyMergeKey]EnergyFreshnessState),
		broadcastStateCounts: make(map[EnergyFreshnessState]int),
	}
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

	if key.Period == "day" {
		s.pruneOlderDaysLocked(key)
	}

	existing, exists := s.points[key]
	if !exists {
		// No existing point: accept any source.
		s.points[key] = energyDataPoint{Value: value, Source: source, IngestAt: ingestAt}
		s.revision++
		semanticEnergyMergesTotal.Add(sourceLabel, 1)
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
		log.Printf("semantic_energy_merge_accept source=%s", sourceLabel)
		return true
	}
}

// pruneOlderDaysLocked drops day points of the same channel/usage dated before
// key, so day identities do not accumulate.
func (s *energyMergeStore) pruneOlderDaysLocked(key energyMergeKey) {
	for existing := range s.points {
		if existing.Period != "day" || existing.Channel != key.Channel || existing.Usage != key.Usage {
			continue
		}
		if [3]int{existing.Year, existing.Month, existing.Day} == [3]int{key.Year, key.Month, key.Day} {
			continue
		}
		if existing.Year < key.Year ||
			(existing.Year == key.Year && (existing.Month < key.Month || (existing.Month == key.Month && existing.Day < key.Day))) {
			delete(s.points, existing)
		}
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
	s.mu.RLock()
	defer s.mu.RUnlock()

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

		slot, inWindow := energySlotFor(key, now)
		if !inWindow {
			continue
		}
		meta := energyPointMetaFromDataPoint(&point, energyFreshnessStateFor(point, now, passiveState), now)
		switch key.Period {
		case "day":
			series.Today = point.Value
			series.TodayMeta = meta
		case "year":
			if len(series.Yearly) < 2 {
				series.Yearly = make([]float64, 2)
			}
			if len(series.YearlyMeta) < 2 {
				series.YearlyMeta = make([]EnergyPointMeta, 2)
			}
			series.Yearly[slot] = point.Value
			series.YearlyMeta[slot] = meta
		case "month":
			if len(series.Monthly) < 2 {
				series.Monthly = make([]float64, 2)
			}
			if len(series.MonthlyMeta) < 2 {
				series.MonthlyMeta = make([]EnergyPointMeta, 2)
			}
			series.Monthly[slot] = point.Value
			series.MonthlyMeta[slot] = meta
		}
	}

	s.applyLifetimeOverlayLocked(totals, now, passiveState)

	// Ensure all energy selector slots are observable with explicit freshness
	// metadata, even when a value has never been seen.
	for _, key := range energyMetadataSelectorCatalog(now) {
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
		slot, _ := energySlotFor(key, now)
		switch key.Period {
		case "day":
			if series.TodayMeta.FreshnessState == "" {
				series.TodayMeta = meta
			}
		case "year":
			if len(series.YearlyMeta) < 2 {
				series.YearlyMeta = make([]EnergyPointMeta, 2)
			}
			if series.YearlyMeta[slot].FreshnessState == "" {
				series.YearlyMeta[slot] = meta
			}
		case "month":
			if len(series.MonthlyMeta) < 2 {
				series.MonthlyMeta = make([]EnergyPointMeta, 2)
			}
			if series.MonthlyMeta[slot].FreshnessState == "" {
				series.MonthlyMeta[slot] = meta
			}
		}
	}

	return totals
}

// applyLifetimeOverlayLocked projects register lifetime totals onto the public
// series: the total occupies the current-year slot unless a newer register
// year point for the reference year exists (broadcast never displaces a
// register value); the previous-year slot and today are held at zero unless a
// register-sourced dated point exists for them.
func (s *energyMergeStore) applyLifetimeOverlayLocked(totals *EnergyTotals, now time.Time, passiveState string) {
	for _, channelName := range []string{"gas", "electricity", "solar"} {
		channel := mergeSelectChannel(totals, channelName)
		for _, usage := range []string{"hot_water", "climate"} {
			life, ok := s.points[energyLifetimeKey(channelName, usage)]
			if !ok {
				continue
			}
			series := mergeSelectUsage(channel, usage)
			lifeMeta := energyPointMetaFromDataPoint(&life, energyFreshnessStateFor(life, now, passiveState), now)
			if len(series.Yearly) < 2 {
				series.Yearly = make([]float64, 2)
			}
			if len(series.YearlyMeta) < 2 {
				series.YearlyMeta = make([]EnergyPointMeta, 2)
			}

			dated := func(period, label string) (energyDataPoint, bool) {
				p, ok := s.points[energyWindowKey(channelName, usage, period, label, now)]
				return p, ok
			}

			// Current slot: lifetime total unless a newer register year point wins.
			cur, curOK := dated("year", "current")
			if curOK && cur.Source == EnergySourceRegister && cur.IngestAt.After(life.IngestAt) {
				series.Yearly[1] = cur.Value
				series.YearlyMeta[1] = energyPointMetaFromDataPoint(&cur, energyFreshnessStateFor(cur, now, passiveState), now)
			} else {
				series.Yearly[1] = life.Value
				series.YearlyMeta[1] = lifeMeta
			}

			// Previous slot: documented zero lock unless a register point exists.
			prev, prevOK := dated("year", "previous")
			if prevOK && prev.Source == EnergySourceRegister {
				series.Yearly[0] = prev.Value
				series.YearlyMeta[0] = energyPointMetaFromDataPoint(&prev, energyFreshnessStateFor(prev, now, passiveState), now)
			} else {
				series.Yearly[0] = 0
				series.YearlyMeta[0] = lifeMeta
			}

			// Today: zero lock unless a register day point exists.
			day, dayOK := dated("day", "")
			if dayOK && day.Source == EnergySourceRegister {
				series.Today = day.Value
				series.TodayMeta = energyPointMetaFromDataPoint(&day, energyFreshnessStateFor(day, now, passiveState), now)
			} else {
				series.Today = 0
				series.TodayMeta = lifeMeta
			}
		}
	}
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
		age := now.Sub(point.IngestAt)
		if age < 0 {
			age = 0
		}
		if age >= energyBroadcastUnavailableTTL {
			return EnergyFreshnessStateStale
		}
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
	nextStates := make(map[energyMergeKey]EnergyFreshnessState)
	for _, key := range energyBroadcastSelectorCatalog(now) {
		point, exists := s.points[key]
		if exists && point.Source == EnergySourceRegister {
			// Broadcast freshness tracks passive/broadcast observability, not
			// register-read availability. Treat register-backed selectors as unseen.
			exists = false
		}
		nextState := energyFreshnessForOptionalPoint(exists, point, now, passiveState)
		prevState := s.broadcastStates[key]
		if prevState == "" {
			prevState = EnergyFreshnessStateNeverSeen
		}
		if prevState != nextState {
			energyBroadcastTransitions.Add(string(prevState)+"->"+string(nextState), 1)
		}
		nextStates[key] = nextState
		nextCounts[nextState]++
	}
	// Rebuilt per reference date so keys of past windows do not accumulate.
	s.broadcastStates = nextStates
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

func energyMetadataSelectorCatalog(ref time.Time) []energyMergeKey {
	return energySelectorCatalog(true, ref)
}

func energyBroadcastSelectorCatalog(ref time.Time) []energyMergeKey {
	return energySelectorCatalog(false, ref)
}

// energySelectorCatalog lists the dated keys of every public energy slot
// relative to the reference date.
func energySelectorCatalog(includeMonth bool, ref time.Time) []energyMergeKey {
	channels := []string{"gas", "electricity", "solar"}
	usages := []string{"hot_water", "climate"}
	periodVariants := 3
	if includeMonth {
		periodVariants = 5
	}
	out := make([]energyMergeKey, 0, len(channels)*len(usages)*periodVariants)
	for _, channel := range channels {
		for _, usage := range usages {
			out = append(out, energyWindowKey(channel, usage, "day", "", ref))
			out = append(out, energyWindowKey(channel, usage, "year", "previous", ref))
			out = append(out, energyWindowKey(channel, usage, "year", "current", ref))
			if includeMonth {
				out = append(out, energyWindowKey(channel, usage, "month", "previous", ref))
				out = append(out, energyWindowKey(channel, usage, "month", "current", ref))
			}
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
