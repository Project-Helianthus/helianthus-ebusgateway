package ebusgateway

import (
	"testing"
	"time"
)

func TestRejoinBackoffSchedule(t *testing.T) {
	cases := []struct {
		attempt  int
		base     int
		max      int
		wantSecs int
	}{
		{1, 5, 60, 5},
		{2, 5, 60, 10},
		{3, 5, 60, 20},
		{4, 5, 60, 40},
		{5, 5, 60, 60},
		{10, 5, 60, 60},
		{0, 5, 60, 5},
	}
	for _, tc := range cases {
		got := RejoinBackoffSchedule(tc.attempt, tc.base, tc.max)
		wantDur := time.Duration(tc.wantSecs) * time.Second
		if got != wantDur {
			t.Errorf("attempt=%d base=%d max=%d: got %v, want %v", tc.attempt, tc.base, tc.max, got, wantDur)
		}
	}
}

func TestDegradedModeAccumulator_K5_FailureCountEscalation(t *testing.T) {
	a := NewDegradedModeAccumulator()
	for i := 0; i < 4; i++ {
		if a.RecordFailure() {
			t.Fatalf("escalated too early at failure %d", i+1)
		}
	}
	if !a.RecordFailure() {
		t.Fatal("expected escalation at 5th failure (K=5)")
	}
	if !a.Escalated() {
		t.Fatal("latch should be set after escalation")
	}
}

func TestDegradedModeAccumulator_LatchSuppressesFurtherEscalation(t *testing.T) {
	a := NewDegradedModeAccumulator()
	for i := 0; i < 5; i++ {
		a.RecordFailure()
	}
	if !a.Escalated() {
		t.Fatal("should be escalated")
	}
	for i := 0; i < 5; i++ {
		if a.RecordFailure() {
			t.Errorf("escalation re-fired on failure %d (should latch-suppress)", i+1)
		}
	}
}

func TestDegradedModeAccumulator_ClearLatchOnStabilityWindow(t *testing.T) {
	a := NewDegradedModeAccumulator()
	for i := 0; i < 5; i++ {
		a.RecordFailure()
	}
	if !a.Escalated() {
		t.Fatal("should be escalated")
	}
	a.ClearLatch()
	if a.Escalated() {
		t.Fatal("latch should be cleared")
	}
	a.RecordSuccess()
	for i := 0; i < 5; i++ {
		a.RecordFailure()
	}
	if !a.Escalated() {
		t.Fatal("expected re-escalation after latch clear")
	}
}

func TestDegradedModeAccumulator_RecordSuccessResetsConsecutiveFailures(t *testing.T) {
	a := NewDegradedModeAccumulator()
	a.RecordFailure()
	a.RecordFailure()
	a.RecordSuccess()
	for i := 0; i < 4; i++ {
		if a.RecordFailure() {
			t.Fatalf("escalated after reset at failure %d", i+1)
		}
	}
	if !a.RecordFailure() {
		t.Fatal("expected escalation at 5th failure after reset")
	}
}

// TestDegradedModeAccumulator_T5min_ContinuousDegradedEscalation drives the
// continuous-time threshold path (T=5min) using a fake clock. Plan AD17:
// "escalation fires on EITHER K=5 consecutive rejoin failures OR T=5min
// cumulative degraded time within rolling 15-min window, whichever fires
// first." This test covers the T=5min path which K=5 tests do not exercise.
func TestDegradedModeAccumulator_T5min_ContinuousDegradedEscalation(t *testing.T) {
	a := NewDegradedModeAccumulator()
	base := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return base }

	// Initial RecordFailure marks degraded-since=base; not yet escalated
	// (only 1 consecutive failure).
	if a.RecordFailure() {
		t.Fatal("escalated too early on first failure")
	}

	// Advance to base+299s (just under threshold). Tick degraded each second.
	// Should not escalate yet.
	for sec := 1; sec < 300; sec++ {
		a.now = func() time.Time { return base.Add(time.Duration(sec) * time.Second) }
		if a.RecordDegradedTick() {
			t.Fatalf("escalated too early at sec=%d (expected escalation at sec>=300)", sec)
		}
	}

	// At base+300s the continuous threshold fires.
	a.now = func() time.Time { return base.Add(300 * time.Second) }
	if !a.RecordDegradedTick() {
		t.Fatal("expected escalation at base+300s continuous degraded duration")
	}
	if !a.Escalated() {
		t.Fatal("latch should be set after T=5min escalation")
	}
}

// TestDegradedModeAccumulator_T5min_CumulativeRollingWindow drives the
// cumulative-buckets path. With state_min_stability_s+continuous_threshold_s
// frozen at 300, accumulating ≥300000 ms of degraded-state samples in the
// 900-bucket ring buffer should escalate even without a single continuous
// degraded-since window of 300s.
func TestDegradedModeAccumulator_T5min_CumulativeRollingWindow(t *testing.T) {
	a := NewDegradedModeAccumulator()
	base := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return base }

	// Walk the bucket index manually by directly setting buckets (simpler
	// than scripting flap-RecordFailure cycles with intermediate Success).
	// Fill 300 buckets with 1000ms each = 300000 ms total = exact threshold.
	a.mu.Lock()
	for i := 0; i < 300; i++ {
		a.buckets[i] = 1000
	}
	a.mu.Unlock()

	// One observation tick to trigger the cumulative-sum check.
	a.now = func() time.Time { return base.Add(301 * time.Second) }
	if !a.RecordDegradedTick() {
		t.Fatal("expected escalation when cumulative buckets sum reached threshold")
	}
	if !a.Escalated() {
		t.Fatal("latch should be set after cumulative-window escalation")
	}
}
