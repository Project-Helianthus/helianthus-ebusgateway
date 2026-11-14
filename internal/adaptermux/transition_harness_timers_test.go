package adaptermux

import "testing"

// The transition harness stop leaves no mux timer armed: the START deadline of
// a selection in flight is stopped.
func TestTransitionHarnessStopStopsStartDeadlineInFlight(t *testing.T) {
	h := newTransitionHarness(t, transitionUpstreamAsync)
	h.bid(1, transitionSessionSource)
	h.syn()
	h.captureDeadline()
	deadline := h.deadline
	h.stop()
	if deadline.Stop() {
		t.Fatal("START deadline timer still armed after the harness stopped")
	}
}

// The transition harness stop leaves no mux timer armed: the absorb reset of a
// pending stale-answer barrier is stopped and its reference cleared.
func TestTransitionHarnessStopStopsPendingAbsorbReset(t *testing.T) {
	h := newTransitionHarness(t, transitionUpstreamAsync)
	h.bid(1, transitionSessionSource)
	h.syn()
	h.m.cancelPendingStart(1)
	h.m.stateMu.Lock()
	absorb, timer := h.m.pendingStartAbsorb, h.m.pendingAbsorbResetTimer
	h.m.stateMu.Unlock()
	if absorb == 0 || timer == nil {
		t.Fatalf("fixture: absorb=%d timer armed=%v, want a pending barrier", absorb, timer != nil)
	}
	h.stop()
	h.m.stateMu.Lock()
	ref := h.m.pendingAbsorbResetTimer
	h.m.stateMu.Unlock()
	if ref != nil {
		t.Error("absorb reset timer reference not cleared after the harness stopped")
	}
	if timer.Stop() {
		t.Error("absorb reset timer still armed after the harness stopped")
	}
}
