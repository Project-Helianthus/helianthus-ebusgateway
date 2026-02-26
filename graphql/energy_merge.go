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
	Period   string // "day", "year"
	YearKind string // "" for day, "previous"/"current" for year
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
	mu       sync.RWMutex
	points   map[energyMergeKey]energyDataPoint
	revision uint64
}

func newEnergyMergeStore() *energyMergeStore {
	return &energyMergeStore{
		points: make(map[energyMergeKey]energyDataPoint),
	}
}

// Apply attempts to merge an incoming energy value into the store.
// Returns true if the value was accepted according to the merge rules.
// The key's Usage is canonicalized internally.
func (s *energyMergeStore) Apply(key energyMergeKey, value float64, source EnergyDataSource, ingestAt time.Time) bool {
	key.Usage = canonicalizeUsage(key.Usage)
	sourceLabel := energySourceLabel(source)

	s.mu.Lock()
	defer s.mu.Unlock()

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
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.points) == 0 {
		return nil
	}

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
		case "year":
			if len(series.Yearly) < 2 {
				series.Yearly = make([]float64, 2)
			}
			switch key.YearKind {
			case "previous":
				series.Yearly[0] = point.Value
			case "current":
				series.Yearly[1] = point.Value
			}
		}
	}

	return totals
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
