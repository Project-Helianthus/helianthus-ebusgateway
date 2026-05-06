package ebusgateway

import (
	"expvar"
	"fmt"
	"log"
	"sync"
	"time"
)

// StartupSourceSelectionMetrics bundles the 11 expvar surfaces exposed by the
// startup source-selection plan.
// Names and semantics match the plan exactly; callers publish this
// bundle via expvar.Publish at startup and update counters/gauges as
// runtime events occur.
type StartupSourceSelectionMetrics struct {
	DegradedTotal                  *expvar.Int
	State                          *expvar.Int
	ExplicitSourceActive           *expvar.Int
	WarmupEventsSeen               *expvar.Int
	WarmupCyclesTotal              *expvar.Int
	ExplicitValidateOnlyTotal      *expvar.Int
	ExplicitSourceConflictDetected *expvar.Int
	DegradedEscalated              *expvar.Int
	DegradedSinceMs                *expvar.Int
	ConsecutiveFailures            *expvar.Int
	DegradedCumulativeMs           *expvar.Int

	mu sync.Mutex

	warmupCycleSeq uint64
}

var (
	defaultStartupSourceSelectionMetrics     *StartupSourceSelectionMetrics
	defaultStartupSourceSelectionMetricsOnce sync.Once
)

func StartupSourceSelectionExpvarNames() []string {
	return []string{
		"startup_source_selection_degraded_total",
		"startup_source_selection_state",
		"startup_source_selection_explicit_source_active",
		"startup_source_selection_warmup_events_seen",
		"startup_source_selection_warmup_cycles_total",
		"startup_source_selection_explicit_validate_only_total",
		"startup_source_selection_explicit_source_conflict_detected",
		"startup_source_selection_degraded_escalated",
		"startup_source_selection_degraded_since_ms",
		"startup_source_selection_consecutive_failures",
		"startup_source_selection_degraded_cumulative_ms",
	}
}

// NewStartupSourceSelectionMetrics allocates the metric bundle. Caller decides
// whether to publish via expvar.Publish or keep per-instance (for tests).
func NewStartupSourceSelectionMetrics() *StartupSourceSelectionMetrics {
	return &StartupSourceSelectionMetrics{
		DegradedTotal:                  new(expvar.Int),
		State:                          new(expvar.Int),
		ExplicitSourceActive:           new(expvar.Int),
		WarmupEventsSeen:               new(expvar.Int),
		WarmupCyclesTotal:              new(expvar.Int),
		ExplicitValidateOnlyTotal:      new(expvar.Int),
		ExplicitSourceConflictDetected: new(expvar.Int),
		DegradedEscalated:              new(expvar.Int),
		DegradedSinceMs:                new(expvar.Int),
		ConsecutiveFailures:            new(expvar.Int),
		DegradedCumulativeMs:           new(expvar.Int),
	}
}

// GetOrInitStartupSourceSelectionMetrics returns the process-global
// StartupSourceSelectionMetrics instance, creating it and publishing its
// expvars on first call.
func GetOrInitStartupSourceSelectionMetrics() *StartupSourceSelectionMetrics {
	defaultStartupSourceSelectionMetricsOnce.Do(func() {
		defaultStartupSourceSelectionMetrics = NewStartupSourceSelectionMetrics()
		defaultStartupSourceSelectionMetrics.Publish()
		EmitStartupResetWarn(log.Printf)
	})
	return defaultStartupSourceSelectionMetrics
}

// Publish registers all expvars under the "startup_source_selection_" prefix.
// Idempotent-safe behavior: does NOT re-register if Publish was already
// called (expvar panics on duplicate publishes); caller must only call
// once per process. In tests, use metrics without publishing.
func (m *StartupSourceSelectionMetrics) Publish() {
	names := StartupSourceSelectionExpvarNames()
	expvar.Publish(names[0], m.DegradedTotal)
	expvar.Publish(names[1], m.State)
	expvar.Publish(names[2], m.ExplicitSourceActive)
	expvar.Publish(names[3], m.WarmupEventsSeen)
	expvar.Publish(names[4], m.WarmupCyclesTotal)
	expvar.Publish(names[5], m.ExplicitValidateOnlyTotal)
	expvar.Publish(names[6], m.ExplicitSourceConflictDetected)
	expvar.Publish(names[7], m.DegradedEscalated)
	expvar.Publish(names[8], m.DegradedSinceMs)
	expvar.Publish(names[9], m.ConsecutiveFailures)
	expvar.Publish(names[10], m.DegradedCumulativeMs)
}

// StartWarmupCycle resets WarmupEventsSeen to 0 and increments
// WarmupCyclesTotal. Returns the new cycle sequence number.
func (m *StartupSourceSelectionMetrics) StartWarmupCycle() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.warmupCycleSeq++
	m.WarmupEventsSeen.Set(0)
	m.WarmupCyclesTotal.Add(1)
	return m.warmupCycleSeq
}

// RecordWarmupEvent increments WarmupEventsSeen by 1.
func (m *StartupSourceSelectionMetrics) RecordWarmupEvent() {
	m.WarmupEventsSeen.Add(1)
}

// MarkDegraded sets State=2, increments DegradedTotal, records
// DegradedSinceMs if not already set.
func (m *StartupSourceSelectionMetrics) MarkDegraded(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.State.Value() != 2 {
		m.State.Set(2)
		m.DegradedTotal.Add(1)
		m.DegradedSinceMs.Set(now.UnixMilli())
	}
}

// MarkActive sets State=1 and clears DegradedSinceMs. Holds m.mu so the
// pair {State, DegradedSinceMs} updates atomically vs MarkDegraded /
// MarkPending — prevents the AD20-reviewer-flagged race where concurrent
// transitions could double-count DegradedTotal.
func (m *StartupSourceSelectionMetrics) MarkActive() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.State.Set(1)
	m.DegradedSinceMs.Set(0)
}

// MarkPending sets State=0. Holds m.mu (see MarkActive note).
func (m *StartupSourceSelectionMetrics) MarkPending() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.State.Set(0)
}

// SetExplicitSourceActive(true) sets ExplicitSourceActive=1; false sets 0.
func (m *StartupSourceSelectionMetrics) SetExplicitSourceActive(active bool) {
	if active {
		m.ExplicitSourceActive.Set(1)
	} else {
		m.ExplicitSourceActive.Set(0)
	}
}

// RecordExplicitValidateOnly increments ExplicitValidateOnlyTotal.
func (m *StartupSourceSelectionMetrics) RecordExplicitValidateOnly() {
	m.ExplicitValidateOnlyTotal.Add(1)
}

// SetExplicitSourceConflictDetected flips ExplicitSourceConflictDetected to 1
// (latching for the current run; a cycle reset would restart it).
func (m *StartupSourceSelectionMetrics) SetExplicitSourceConflictDetected() {
	m.ExplicitSourceConflictDetected.Set(1)
}

// SetDegradedEscalated sets the latch (0 or 1).
func (m *StartupSourceSelectionMetrics) SetDegradedEscalated(latched bool) {
	if latched {
		m.DegradedEscalated.Set(1)
	} else {
		m.DegradedEscalated.Set(0)
	}
}

// SetConsecutiveFailures records the current count.
func (m *StartupSourceSelectionMetrics) SetConsecutiveFailures(n int) {
	m.ConsecutiveFailures.Set(int64(n))
}

// SetDegradedCumulativeMs records the current rolling-window cumulative.
func (m *StartupSourceSelectionMetrics) SetDegradedCumulativeMs(ms uint64) {
	m.DegradedCumulativeMs.Set(int64(ms))
}

// EmitStartupResetWarn is a package-level log helper for the AD17
// restart-reset WARN line emitted once on process start. Callers use
// this immediately after NewStartupSourceSelectionMetrics to satisfy AD17's
// observability contract.
func EmitStartupResetWarn(logger func(format string, args ...interface{})) {
	logger("WARN: startup source selection escalation accumulator zeroed reason=process_start")
}

// ValidateSourceSelectionMode returns nil if v is one of the four
// enum values the plan admits per AD23. Returns a FATAL-level error
// otherwise — callers should refuse to emit artifacts with an out-of-
// range value.
func ValidateSourceSelectionMode(v string) error {
	switch v {
	case "source_selection", "explicit_validate_only", "degraded_transport_blind", "degraded_no_events":
		return nil
	default:
		return fmt.Errorf("FATAL: source_selection.mode=%q is out of enum {source_selection,explicit_validate_only,degraded_transport_blind,degraded_no_events} (SAS M4)", v)
	}
}
