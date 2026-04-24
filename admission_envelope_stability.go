package ebusgateway

import (
	"sync"
	"time"
)

// AdmissionStabilityWindow gates emission of bus_admission state transitions
// through the configured state_min_stability_s window. A new state is
// admitted to the envelope body ONLY after it has been stable for the full
// window duration; transient flaps (state changes within the window) do
// not flip the envelope. Matches AD08 flap mitigation.
type AdmissionStabilityWindow struct {
	mu            sync.Mutex
	windowSeconds int
	emittedState  string
	pendingState  string
	pendingSince  time.Time
	now           func() time.Time
}

func NewAdmissionStabilityWindow(windowSeconds int) *AdmissionStabilityWindow {
	return &AdmissionStabilityWindow{
		windowSeconds: windowSeconds,
		now:           time.Now,
	}
}

// Observe records a state change observation. Returns (newEmittedState,
// flipped) where flipped reports whether the envelope should update.
func (w *AdmissionStabilityWindow) Observe(state string) (emitted string, flipped bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := w.now()
	if state == w.emittedState {
		w.pendingState = ""
		w.pendingSince = time.Time{}
		return w.emittedState, false
	}
	if state != w.pendingState {
		w.pendingState = state
		w.pendingSince = now
		return w.emittedState, false
	}
	if now.Sub(w.pendingSince) >= time.Duration(w.windowSeconds)*time.Second {
		w.emittedState = state
		w.pendingState = ""
		w.pendingSince = time.Time{}
		return w.emittedState, true
	}
	return w.emittedState, false
}

// EmittedState returns the state currently reflected in the envelope.
func (w *AdmissionStabilityWindow) EmittedState() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.emittedState
}
