package ebusgateway

import (
	"strings"
	"testing"
	"time"
)

func TestStartupAdmissionMetrics_StartWarmupCycle(t *testing.T) {
	m := NewStartupAdmissionMetrics()
	m.RecordWarmupEvent()
	m.RecordWarmupEvent()
	if got := m.WarmupEventsSeen.Value(); got != 2 {
		t.Errorf("expected events_seen=2 before cycle reset, got %d", got)
	}
	cycle := m.StartWarmupCycle()
	if cycle != 1 {
		t.Errorf("expected first cycle=1, got %d", cycle)
	}
	if got := m.WarmupEventsSeen.Value(); got != 0 {
		t.Errorf("expected events_seen reset to 0, got %d", got)
	}
	if got := m.WarmupCyclesTotal.Value(); got != 1 {
		t.Errorf("expected cycles_total=1, got %d", got)
	}
}

func TestStartupAdmissionMetrics_StateTransitions(t *testing.T) {
	m := NewStartupAdmissionMetrics()
	now := time.Now()
	m.MarkPending()
	if m.State.Value() != 0 {
		t.Errorf("pending: state=%d", m.State.Value())
	}
	m.MarkDegraded(now)
	if m.State.Value() != 2 {
		t.Errorf("degraded: state=%d", m.State.Value())
	}
	if m.DegradedSinceMs.Value() != now.UnixMilli() {
		t.Errorf("degraded_since_ms not set")
	}
	if m.DegradedTotal.Value() != 1 {
		t.Errorf("degraded_total: %d", m.DegradedTotal.Value())
	}
	m.MarkActive()
	if m.State.Value() != 1 {
		t.Errorf("active: state=%d", m.State.Value())
	}
	if m.DegradedSinceMs.Value() != 0 {
		t.Errorf("degraded_since_ms not cleared on active: %d", m.DegradedSinceMs.Value())
	}
}

func TestValidateAdmissionPathSelected(t *testing.T) {
	for _, ok := range []string{"join", "override", "degraded_transport_blind", "degraded_no_events"} {
		if err := ValidateAdmissionPathSelected(ok); err != nil {
			t.Errorf("valid value %q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "static_fallback", "bogus", "JOIN", "unknown"} {
		err := ValidateAdmissionPathSelected(bad)
		if err == nil {
			t.Errorf("invalid value %q accepted", bad)
			continue
		}
		if !strings.HasPrefix(err.Error(), "FATAL:") {
			t.Errorf("expected FATAL: prefix, got %q", err.Error())
		}
	}
}

func TestEmitStartupResetWarn(t *testing.T) {
	var captured string
	EmitStartupResetWarn(func(format string, args ...interface{}) {
		captured = format
	})
	if !strings.Contains(captured, "accumulator zeroed") {
		t.Errorf("expected accumulator-zeroed WARN, got %q", captured)
	}
	if !strings.Contains(captured, "reason=process_start") {
		t.Errorf("expected reason=process_start, got %q", captured)
	}
}
