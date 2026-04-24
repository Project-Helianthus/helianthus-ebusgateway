package ebusgateway

import (
	"expvar"
	"fmt"
	"log"
	"sync"
	"time"
)

// StartupAdmissionMetrics bundles the 11 expvar surfaces exposed by the
// startup-admission-discovery plan (per plan §M5 expvar_surfaces).
// Names and semantics match the plan exactly; callers publish this
// bundle via expvar.Publish at startup and update counters/gauges as
// runtime events occur.
type StartupAdmissionMetrics struct {
	DegradedTotal             *expvar.Int
	State                     *expvar.Int
	OverrideActive            *expvar.Int
	WarmupEventsSeen          *expvar.Int
	WarmupCyclesTotal         *expvar.Int
	OverrideBypassTotal       *expvar.Int
	OverrideConflictDetected  *expvar.Int
	DegradedEscalated         *expvar.Int
	DegradedSinceMs           *expvar.Int
	ConsecutiveRejoinFailures *expvar.Int
	DegradedCumulativeMs      *expvar.Int

	mu sync.Mutex

	warmupCycleSeq uint64
}

var (
	defaultStartupAdmissionMetrics     *StartupAdmissionMetrics
	defaultStartupAdmissionMetricsOnce sync.Once
)

// NewStartupAdmissionMetrics allocates the metric bundle. Caller decides
// whether to publish via expvar.Publish or keep per-instance (for tests).
func NewStartupAdmissionMetrics() *StartupAdmissionMetrics {
	return &StartupAdmissionMetrics{
		DegradedTotal:             new(expvar.Int),
		State:                     new(expvar.Int),
		OverrideActive:            new(expvar.Int),
		WarmupEventsSeen:          new(expvar.Int),
		WarmupCyclesTotal:         new(expvar.Int),
		OverrideBypassTotal:       new(expvar.Int),
		OverrideConflictDetected:  new(expvar.Int),
		DegradedEscalated:         new(expvar.Int),
		DegradedSinceMs:           new(expvar.Int),
		ConsecutiveRejoinFailures: new(expvar.Int),
		DegradedCumulativeMs:      new(expvar.Int),
	}
}

// GetOrInitStartupAdmissionMetrics returns the process-global
// StartupAdmissionMetrics instance, creating it and publishing its
// expvars on first call.
func GetOrInitStartupAdmissionMetrics() *StartupAdmissionMetrics {
	defaultStartupAdmissionMetricsOnce.Do(func() {
		defaultStartupAdmissionMetrics = NewStartupAdmissionMetrics()
		defaultStartupAdmissionMetrics.Publish()
		EmitStartupResetWarn(log.Printf)
	})
	return defaultStartupAdmissionMetrics
}

// Publish registers all expvars under the "startup_admission_" prefix.
// Idempotent-safe behavior: does NOT re-register if Publish was already
// called (expvar panics on duplicate publishes); caller must only call
// once per process. In tests, use metrics without publishing.
func (m *StartupAdmissionMetrics) Publish() {
	expvar.Publish("startup_admission_degraded_total", m.DegradedTotal)
	expvar.Publish("startup_admission_state", m.State)
	expvar.Publish("startup_admission_override_active", m.OverrideActive)
	expvar.Publish("startup_admission_warmup_events_seen", m.WarmupEventsSeen)
	expvar.Publish("startup_admission_warmup_cycles_total", m.WarmupCyclesTotal)
	expvar.Publish("startup_admission_override_bypass_total", m.OverrideBypassTotal)
	expvar.Publish("startup_admission_override_conflict_detected", m.OverrideConflictDetected)
	expvar.Publish("startup_admission_degraded_escalated", m.DegradedEscalated)
	expvar.Publish("startup_admission_degraded_since_ms", m.DegradedSinceMs)
	expvar.Publish("startup_admission_consecutive_rejoin_failures", m.ConsecutiveRejoinFailures)
	expvar.Publish("startup_admission_degraded_cumulative_ms", m.DegradedCumulativeMs)
}

// StartWarmupCycle resets WarmupEventsSeen to 0 and increments
// WarmupCyclesTotal. Returns the new cycle sequence number.
func (m *StartupAdmissionMetrics) StartWarmupCycle() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.warmupCycleSeq++
	m.WarmupEventsSeen.Set(0)
	m.WarmupCyclesTotal.Add(1)
	return m.warmupCycleSeq
}

// RecordWarmupEvent increments WarmupEventsSeen by 1.
func (m *StartupAdmissionMetrics) RecordWarmupEvent() {
	m.WarmupEventsSeen.Add(1)
}

// MarkDegraded sets State=2, increments DegradedTotal, records
// DegradedSinceMs if not already set.
func (m *StartupAdmissionMetrics) MarkDegraded(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.State.Value() != 2 {
		m.State.Set(2)
		m.DegradedTotal.Add(1)
		m.DegradedSinceMs.Set(now.UnixMilli())
	}
}

// MarkActive sets State=1 and clears DegradedSinceMs.
func (m *StartupAdmissionMetrics) MarkActive() {
	m.State.Set(1)
	m.DegradedSinceMs.Set(0)
}

// MarkPending sets State=0.
func (m *StartupAdmissionMetrics) MarkPending() {
	m.State.Set(0)
}

// SetOverrideActive(true) sets OverrideActive=1; SetOverrideActive(false) sets 0.
func (m *StartupAdmissionMetrics) SetOverrideActive(active bool) {
	if active {
		m.OverrideActive.Set(1)
	} else {
		m.OverrideActive.Set(0)
	}
}

// RecordOverrideBypass increments OverrideBypassTotal (monotonic per
// admission cycle that selected the override path).
func (m *StartupAdmissionMetrics) RecordOverrideBypass() {
	m.OverrideBypassTotal.Add(1)
}

// SetOverrideConflictDetected flips OverrideConflictDetected to 1
// (latching for the current run; a cycle reset would restart it).
func (m *StartupAdmissionMetrics) SetOverrideConflictDetected() {
	m.OverrideConflictDetected.Set(1)
}

// SetDegradedEscalated sets the latch (0 or 1).
func (m *StartupAdmissionMetrics) SetDegradedEscalated(latched bool) {
	if latched {
		m.DegradedEscalated.Set(1)
	} else {
		m.DegradedEscalated.Set(0)
	}
}

// SetConsecutiveRejoinFailures records the current count.
func (m *StartupAdmissionMetrics) SetConsecutiveRejoinFailures(n int) {
	m.ConsecutiveRejoinFailures.Set(int64(n))
}

// SetDegradedCumulativeMs records the current rolling-window cumulative.
func (m *StartupAdmissionMetrics) SetDegradedCumulativeMs(ms uint64) {
	m.DegradedCumulativeMs.Set(int64(ms))
}

// EmitStartupResetWarn is a package-level log helper for the AD17
// restart-reset WARN line emitted once on process start. Callers use
// this immediately after NewStartupAdmissionMetrics to satisfy AD17's
// observability contract.
func EmitStartupResetWarn(logger func(format string, args ...interface{})) {
	logger("WARN: startup admission escalation accumulator zeroed reason=process_start")
}

// ValidateAdmissionPathSelected returns nil if v is one of the four
// enum values the plan admits per AD23. Returns a FATAL-level error
// otherwise — callers should refuse to emit artifacts with an out-of-
// range value.
func ValidateAdmissionPathSelected(v string) error {
	switch v {
	case "join", "override", "degraded_transport_blind", "degraded_no_events":
		return nil
	default:
		return fmt.Errorf("FATAL: admission_path_selected=%q is out of enum {join,override,degraded_transport_blind,degraded_no_events} (AD23)", v)
	}
}
