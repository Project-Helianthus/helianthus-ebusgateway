package adaptermux

import (
	"fmt"
	"sync/atomic"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
)

// outOfStepBadCycles is the number of consecutive bad START cycles that
// detect an adapter running one host command behind.
const outOfStepBadCycles = 3

// holdReasonAdapterOutOfStep names the active-arbitration hold set by the
// out-of-step detector.
const holdReasonAdapterOutOfStep = "adapter_out_of_step"

// AdapterStepMemory remembers, across mux generations of one runtime, whether
// the single link reset of the current out-of-step episode is spent, and the
// active-arbitration hold with the mux that set it. It is safe for concurrent
// use and holds no lock: every field is atomic, so it may be used while the
// caller holds the mux state lock.
type AdapterStepMemory struct {
	resetSpent atomic.Bool
	hold       atomic.Pointer[adapterStepHold]
}

type adapterStepHold struct {
	reason string
	owner  *Mux
}

// NewAdapterStepMemory returns a memory with the link reset available and no
// hold.
func NewAdapterStepMemory() *AdapterStepMemory { return &AdapterStepMemory{} }

// ActiveArbitrationHoldReason returns the reason of the current hold, or ""
// when active arbitration is not held. Lock-free.
func (memory *AdapterStepMemory) ActiveArbitrationHoldReason() string {
	if memory == nil {
		return ""
	}
	if hold := memory.hold.Load(); hold != nil {
		return hold.reason
	}
	return ""
}

func (memory *AdapterStepMemory) holdReasonOf(owner *Mux) string {
	if memory == nil {
		return ""
	}
	if hold := memory.hold.Load(); hold != nil && hold.owner == owner {
		return hold.reason
	}
	return ""
}

// spendReset takes the episode's single link reset and reports whether it was
// still available. A mux without memory (built without New) always may.
func (memory *AdapterStepMemory) spendReset() bool {
	return memory == nil || memory.resetSpent.CompareAndSwap(false, true)
}

// restoreReset makes the link reset available again.
func (memory *AdapterStepMemory) restoreReset() {
	if memory != nil {
		memory.resetSpent.Store(false)
	}
}

func (memory *AdapterStepMemory) setHold(owner *Mux, reason string) {
	if memory != nil {
		memory.hold.Store(&adapterStepHold{reason: reason, owner: owner})
	}
}

// clearHoldOf clears the hold only when owner set it, so a retiring mux never
// clears the hold of its successor.
func (memory *AdapterStepMemory) clearHoldOf(owner *Mux) {
	if memory == nil {
		return
	}
	for {
		hold := memory.hold.Load()
		if hold == nil || hold.owner != owner || memory.hold.CompareAndSwap(hold, nil) {
			return
		}
	}
}

// adapterStepDetector is the out-of-step detector state. Guarded by stateMu.
// A START cycle runs from one RequestStart write to the next; it is bad when,
// before the next START write, a STARTED names another initiator than the
// pending one, a stale-answer barrier armed for "deadline" or
// "started-mismatch" is cleared without its answer, or a STARTED arrives with
// nothing pending. Each cycle counts once. The run of consecutive bad cycles
// ends at a completed transaction, at an answer the barrier consumed and at
// every lifecycle boundary.
type adapterStepDetector struct {
	cycle      uint64 // START cycles written by this mux
	cycleBad   bool   // the current cycle was already counted bad
	run        int    // consecutive bad cycles
	badCycles  uint64 // bad cycles counted by this mux
	detections uint64
	linkResets uint64
	held       bool
	// holdSkip* rate-limit the selection skip line while held.
	holdSkipLastEmit   time.Time
	holdSkipSuppressed uint64
}

// adapterStepAction is what a detection asks for, applied outside stateMu.
type adapterStepAction uint8

const (
	adapterStepNone adapterStepAction = iota
	adapterStepLinkReset
	adapterStepHoldSet
)

// AdapterStepMemory returns the memory this mux shares with its lifecycle
// owner (or its private one).
func (m *Mux) AdapterStepMemory() *AdapterStepMemory { return m.stepMemory }

// ActiveArbitrationHoldReason returns the reason this mux holds active
// arbitration for, or "". Lock-free.
func (m *Mux) ActiveArbitrationHoldReason() string {
	if m == nil {
		return ""
	}
	return m.stepMemory.holdReasonOf(m)
}

// stepStartCycleLocked opens a START cycle at a RequestStart write.
func (m *Mux) stepStartCycleLocked() {
	m.step.cycle++
	m.step.cycleBad = false
}

// stepBadCycleLocked marks the current START cycle bad and returns the action
// of a detection. Caller holds stateMu and applies the action after unlocking.
func (m *Mux) stepBadCycleLocked() adapterStepAction {
	if m.step.cycleBad || m.step.held {
		return adapterStepNone
	}
	m.step.cycleBad = true
	m.step.badCycles++
	m.step.run++
	if m.step.run < outOfStepBadCycles {
		return adapterStepNone
	}
	m.step.run = 0
	m.step.detections++
	if m.stepMemory.spendReset() {
		m.step.linkResets++
		return adapterStepLinkReset
	}
	m.step.held = true
	m.stepMemory.setHold(m, holdReasonAdapterOutOfStep)
	return adapterStepHoldSet
}

// stepInStepLocked ends the run at an answer the stale-answer barrier
// consumed: late, but in step. Caller holds stateMu.
func (m *Mux) stepInStepLocked() { m.step.run = 0 }

// stepCompletedLocked ends the run at a completed transaction and makes the
// link reset available again. Caller holds stateMu.
func (m *Mux) stepCompletedLocked() {
	m.step.run = 0
	m.stepMemory.restoreReset()
}

// stepBoundaryLocked ends the run and the hold at a lifecycle boundary and
// reports whether a hold ended. Caller holds stateMu.
func (m *Mux) stepBoundaryLocked() bool {
	m.step.run = 0
	m.step.cycleBad = false
	m.step.holdSkipLastEmit, m.step.holdSkipSuppressed = time.Time{}, 0
	wasHeld := m.step.held
	m.step.held = false
	if wasHeld {
		m.stepMemory.clearHoldOf(m)
	}
	return wasHeld
}

// stepHoldSkipLocked reports whether selection is held and returns the skip
// line to log after unlocking, at most once per second ("" when suppressed).
// Caller holds stateMu.
func (m *Mux) stepHoldSkipLocked(now time.Time) (bool, string) {
	if !m.step.held {
		return false, ""
	}
	if !m.step.holdSkipLastEmit.IsZero() && now.Sub(m.step.holdSkipLastEmit) < time.Second {
		m.step.holdSkipSuppressed++
		return true, ""
	}
	suppressed := m.step.holdSkipSuppressed
	m.step.holdSkipSuppressed, m.step.holdSkipLastEmit = 0, now
	line := "adaptermux: tryGrantAndStart skipped: active arbitration held (" + holdReasonAdapterOutOfStep + ")"
	if suppressed > 0 {
		line = fmt.Sprintf("%s (%d suppressed)", line, suppressed)
	}
	return true, line
}

// applyStepAction carries out a detection outside stateMu. A link reset closes
// the current upstream connection, so readLoop's read-error branch runs
// reconnect(): in place without a lifecycle owner, delegated to it otherwise.
func (m *Mux) applyStepAction(action adapterStepAction) {
	switch action {
	case adapterStepLinkReset:
		m.logger.Printf("adaptermux: adapter_out_of_step bad_start_cycles=%d action=link_reset", outOfStepBadCycles)
		m.connMu.Lock()
		c := m.conn
		m.connMu.Unlock()
		if c != nil {
			if err := c.Close(); err != nil {
				m.logger.Printf("adaptermux: adapter_out_of_step link reset close: %v", err)
			}
		}
	case adapterStepHoldSet:
		m.logger.Printf("adaptermux: active_arbitration_hold state=set reason=%s bad_start_cycles=%d", holdReasonAdapterOutOfStep, outOfStepBadCycles)
	}
}

// logStepHoldCleared logs the end of a hold at a lifecycle boundary, outside
// stateMu.
func (m *Mux) logStepHoldCleared(wasHeld bool, trigger string) {
	if wasHeld {
		m.logger.Printf("adaptermux: active_arbitration_hold state=cleared trigger=%s", trigger)
	}
}

// gatewayAdmissionError is the error of a gateway bid that reached its
// admission bound. While active arbitration is held it names the hold; the
// error class stays ErrTimeout.
func (m *Mux) gatewayAdmissionError() error {
	if reason := m.ActiveArbitrationHoldReason(); reason != "" {
		return fmt.Errorf("adaptermux: gateway admission: active arbitration held (%s): %w", reason, ebuserrors.ErrTimeout)
	}
	return fmt.Errorf("adaptermux: gateway admission: %w", ebuserrors.ErrTimeout)
}
