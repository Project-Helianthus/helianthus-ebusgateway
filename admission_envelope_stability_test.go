package ebusgateway

import (
	"testing"
	"time"
)

func TestAdmissionStabilityWindow_DoesNotFlipOnTransient(t *testing.T) {
	w := NewAdmissionStabilityWindow(30)
	base := time.Now()
	w.now = func() time.Time { return base }
	state, flipped := w.Observe("active")
	if flipped {
		t.Errorf("first observation: unexpected flip")
	}
	if state != "" {
		t.Errorf("expected emitted still empty, got %q", state)
	}
	w.now = func() time.Time { return base.Add(10 * time.Second) }
	_, flipped = w.Observe("degraded")
	if flipped {
		t.Errorf("10s not enough; should not flip")
	}
	w.now = func() time.Time { return base.Add(15 * time.Second) }
	_, flipped = w.Observe("active")
	if flipped {
		t.Errorf("pending reset; should not flip")
	}
}

func TestAdmissionStabilityWindow_FlipsAfterWindowElapsed(t *testing.T) {
	w := NewAdmissionStabilityWindow(30)
	base := time.Now()
	w.now = func() time.Time { return base }
	w.Observe("active")
	w.now = func() time.Time { return base.Add(31 * time.Second) }
	_, flipped := w.Observe("active")
	if !flipped {
		t.Errorf("expected flip after 31s continuous active")
	}
	if w.EmittedState() != "active" {
		t.Errorf("emitted state should be 'active', got %q", w.EmittedState())
	}
}
