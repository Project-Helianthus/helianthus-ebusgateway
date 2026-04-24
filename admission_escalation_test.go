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
