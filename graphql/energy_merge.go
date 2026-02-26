package graphql

import (
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

// energyDataPoint tracks a single energy value with its source and ingest time.
type energyDataPoint struct {
	Value    float64
	Source   EnergyDataSource
	IngestAt time.Time
}

// energyMergeKey uniquely identifies an energy data point.
type energyMergeKey struct {
	Channel  string // "gas", "electricity", "solar"
	Usage    string // "hot_water", "heating", "cooling"
	Period   string // "day", "year"
	YearKind string // "" for day, "previous"/"current" for year
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
	mu     sync.RWMutex
	points map[energyMergeKey]energyDataPoint
}

func newEnergyMergeStore() *energyMergeStore {
	return &energyMergeStore{
		points: make(map[energyMergeKey]energyDataPoint),
	}
}

// Apply attempts to merge an incoming energy value into the store.
// Returns true if the value was accepted according to the merge rules.
func (s *energyMergeStore) Apply(key energyMergeKey, value float64, source EnergyDataSource, ingestAt time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.points[key]
	if !exists {
		// No existing point: accept any source.
		s.points[key] = energyDataPoint{Value: value, Source: source, IngestAt: ingestAt}
		return true
	}

	switch {
	case existing.Source == EnergySourceRegister && source == EnergySourceBroadcast:
		// Broadcast never overwrites register.
		return false
	case existing.Source == EnergySourceBroadcast && source == EnergySourceRegister:
		// Register always wins over broadcast, regardless of timestamp.
		s.points[key] = energyDataPoint{Value: value, Source: source, IngestAt: ingestAt}
		return true
	default:
		// Same source: enforce monotonic ingest timestamp.
		if !ingestAt.After(existing.IngestAt) {
			return false
		}
		s.points[key] = energyDataPoint{Value: value, Source: source, IngestAt: ingestAt}
		return true
	}
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
		series := selectUsageSeries(channel, key.Usage)
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
