package v8classifier

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol/telegram_fsm"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Phase 3 Step B3.4: tests for the per-byte telegram FSM driver
// added by Observe. Each test pins one slice of the FSM-classifier
// integration:
//
//   - Lifecycle: New() creates a fresh FSM in non-Off modes; ModeOff
//     leaves fsm=nil.
//   - StreamEventStarted / StreamEventFailed → EnterPassiveTracking
//     (passive observation; QQ already consumed via event.Data).
//   - StreamEventReset → ResetToIdle.
//   - StreamEventByte → Feed → Decision counter increment.
//   - Decision counters faithfully record what the FSM library
//     emits (verified by feeding canonical telegram shapes).
//   - Nil-receiver safety for the new accessors.

// observeEvent feeds an arbitrary StreamEvent to the classifier
// (helper to reduce noise in the FSM-state assertions below).
func observeEvent(c *Classifier, e transport.StreamEvent) {
	c.Observe(e, time.Unix(0, 0))
}

// TestFSM_NewClassifier_OffMode_NilFSM pins the production-default:
// ModeOff yields a nil fsm field, no allocation. All FSM accessors
// return zero values.
func TestFSM_NewClassifier_OffMode_NilFSM(t *testing.T) {
	t.Parallel()
	c := New(ModeOff)
	if got := c.FSMState(); got != telegram_fsm.StateIdle {
		t.Errorf("FSMState()=%v; want StateIdle in ModeOff", got)
	}
	if got := c.FSMInternalState(); got != telegram_fsm.StateIdle {
		t.Errorf("FSMInternalState()=%v; want StateIdle in ModeOff", got)
	}
	if c.FSMIsPassive() {
		t.Error("FSMIsPassive()=true in ModeOff; want false")
	}
	// Drive a byte through Observe. ModeOff → no-op, no counters.
	observeEvent(c, transport.StreamEvent{
		Kind: transport.StreamEventByte,
		Byte: 0xAA, WasEscaped: true,
	})
	if got := c.FsmForwardTotal(); got != 0 {
		t.Errorf("FsmForwardTotal()=%d; want 0 in ModeOff", got)
	}
}

// TestFSM_NewClassifier_ShadowMode_FreshFSM pins the shadow-mode
// invariant: a fresh classifier in ModeShadow has a non-nil FSM in
// StateIdle.
func TestFSM_NewClassifier_ShadowMode_FreshFSM(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	if got := c.FSMState(); got != telegram_fsm.StateIdle {
		t.Errorf("FSMState()=%v; want StateIdle for fresh classifier", got)
	}
	if c.FSMIsPassive() {
		t.Error("FSMIsPassive()=true at construction; want false")
	}
}

// TestFSM_StreamEventStarted_EntersPassiveTracking pins the
// StreamEventStarted → EnterPassiveTracking mapping. After the
// event, the FSM should be in StatePassiveTracking (composite) and
// IsPassive should be true.
func TestFSM_StreamEventStarted_EntersPassiveTracking(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	observeEvent(c, transport.StreamEvent{
		Kind: transport.StreamEventStarted,
		Data: 0x71, // winning QQ — the gateway's own initiator
	})
	if got := c.FSMState(); got != telegram_fsm.StatePassiveTracking {
		t.Errorf("FSMState()=%v; want StatePassiveTracking after StreamEventStarted", got)
	}
	if !c.FSMIsPassive() {
		t.Error("FSMIsPassive()=false; want true after StreamEventStarted")
	}
	if got := c.FsmEnterPassiveTotal(); got != 1 {
		t.Errorf("FsmEnterPassiveTotal()=%d; want 1", got)
	}
}

// TestFSM_StreamEventFailed_EntersPassiveTracking pins the
// StreamEventFailed → EnterPassiveTracking mapping. Same observable
// effect as Started (both signal "telegram is starting; event.Data
// is QQ"); the classifier doesn't distinguish.
func TestFSM_StreamEventFailed_EntersPassiveTracking(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	observeEvent(c, transport.StreamEvent{
		Kind: transport.StreamEventFailed,
		Data: 0x10, // foreign initiator's QQ
	})
	if got := c.FSMState(); got != telegram_fsm.StatePassiveTracking {
		t.Errorf("FSMState()=%v; want StatePassiveTracking after StreamEventFailed", got)
	}
	if !c.FSMIsPassive() {
		t.Error("FSMIsPassive()=false; want true after StreamEventFailed")
	}
	if got := c.FsmEnterPassiveTotal(); got != 1 {
		t.Errorf("FsmEnterPassiveTotal()=%d; want 1", got)
	}
}

// TestFSM_StreamEventReset_ResetsToIdle pins the
// StreamEventReset → ResetToIdle mapping. Even if the FSM was
// mid-telegram, Reset must drive it back to StateIdle.
func TestFSM_StreamEventReset_ResetsToIdle(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)

	// First, enter passive tracking.
	observeEvent(c, transport.StreamEvent{
		Kind: transport.StreamEventStarted,
		Data: 0x71,
	})
	if got := c.FSMState(); got != telegram_fsm.StatePassiveTracking {
		t.Fatalf("precondition: FSMState()=%v; want StatePassiveTracking", got)
	}

	// Now reset.
	observeEvent(c, transport.StreamEvent{Kind: transport.StreamEventReset})
	if got := c.FSMState(); got != telegram_fsm.StateIdle {
		t.Errorf("FSMState()=%v; want StateIdle after StreamEventReset", got)
	}
	if c.FSMIsPassive() {
		t.Error("FSMIsPassive()=true after reset; want false")
	}
	if got := c.FsmResetTotal(); got != 1 {
		t.Errorf("FsmResetTotal()=%d; want 1", got)
	}
}

// TestFSM_StreamEventByte_FeedsForwardDecision pins the canonical
// path: a benign payload byte fed mid-telegram should produce a
// DecisionForward and increment FsmForwardTotal.
//
// Telegram shape: 0x71 (QQ, consumed via Started.Data) → 0x10 (ZZ,
// fed via StreamEventByte) → ... The 0x10 in MASTER_HEADER byte 1
// is a plain destination byte and the FSM should emit Forward.
func TestFSM_StreamEventByte_FeedsForwardDecision(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	observeEvent(c, transport.StreamEvent{
		Kind: transport.StreamEventStarted,
		Data: 0x71,
	})

	// Feed ZZ = 0x10 (broadcast or directed destination, plain).
	observeEvent(c, transport.StreamEvent{
		Kind: transport.StreamEventByte,
		Byte: 0x10,
		// wasEscaped=false (plain byte)
	})
	if got := c.FsmForwardTotal(); got != 1 {
		t.Errorf("FsmForwardTotal()=%d; want 1 (one forward decision)", got)
	}
	if got := c.FsmDropAaInjectionTotal(); got != 0 {
		t.Errorf("FsmDropAaInjectionTotal()=%d; want 0", got)
	}
	if got := c.FsmProtocolFaultTotal(); got != 0 {
		t.Errorf("FsmProtocolFaultTotal()=%d; want 0", got)
	}
}

// TestFSM_StreamEventByte_OffMode_DoesNotFeedFSM pins that ModeOff
// completely bypasses the FSM driver — even if Observe were called
// the nil fsm guard prevents counter increments.
func TestFSM_StreamEventByte_OffMode_DoesNotFeedFSM(t *testing.T) {
	t.Parallel()
	c := New(ModeOff)
	for i := 0; i < 10; i++ {
		observeEvent(c, transport.StreamEvent{
			Kind: transport.StreamEventByte,
			Byte: byte(i),
		})
	}
	if got := c.FsmForwardTotal(); got != 0 {
		t.Errorf("FsmForwardTotal()=%d; want 0 in ModeOff", got)
	}
	if got := c.FsmDropAaInjectionTotal(); got != 0 {
		t.Errorf("FsmDropAaInjectionTotal()=%d; want 0 in ModeOff", got)
	}
	if got := c.FsmProtocolFaultTotal(); got != 0 {
		t.Errorf("FsmProtocolFaultTotal()=%d; want 0 in ModeOff", got)
	}
}

// TestFSM_NilReceiverSafe pins that all new FSM-related accessors
// accept a nil receiver and return zero values.
func TestFSM_NilReceiverSafe(t *testing.T) {
	t.Parallel()
	var c *Classifier
	if got := c.FSMState(); got != telegram_fsm.StateIdle {
		t.Errorf("nil.FSMState()=%v; want StateIdle", got)
	}
	if got := c.FSMInternalState(); got != telegram_fsm.StateIdle {
		t.Errorf("nil.FSMInternalState()=%v; want StateIdle", got)
	}
	if c.FSMIsPassive() {
		t.Error("nil.FSMIsPassive()=true; want false")
	}
	if got := c.FsmForwardTotal(); got != 0 {
		t.Errorf("nil.FsmForwardTotal()=%d; want 0", got)
	}
	if got := c.FsmDropAaInjectionTotal(); got != 0 {
		t.Errorf("nil.FsmDropAaInjectionTotal()=%d; want 0", got)
	}
	if got := c.FsmProtocolFaultTotal(); got != 0 {
		t.Errorf("nil.FsmProtocolFaultTotal()=%d; want 0", got)
	}
	if got := c.FsmEnterPassiveTotal(); got != 0 {
		t.Errorf("nil.FsmEnterPassiveTotal()=%d; want 0", got)
	}
	if got := c.FsmResetTotal(); got != 0 {
		t.Errorf("nil.FsmResetTotal()=%d; want 0", got)
	}
}

// TestFSM_EnforceMode_DoesNotDropYet pins the B3.4 SCAFFOLD
// invariant: even when the FSM emits a DecisionDropAaInjection
// (which B3.6 will translate to drop=true in ModeEnforce), B3.4
// still returns drop=false from Observe. The counter increments,
// but no behavioral change. Future-regression guard.
func TestFSM_EnforceMode_DoesNotDropYet(t *testing.T) {
	t.Parallel()
	c := New(ModeEnforce)
	now := time.Unix(0, 0)

	// Enter passive tracking so subsequent AA bytes are
	// classified mid-frame (where the FSM would emit
	// DropAaInjection per v8 §4).
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted,
		Data: 0x71,
	}, now)

	// Feed a wire AUTO-SYN mid-frame — the FSM should emit
	// DropAaInjection.
	drop := c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte,
		Byte: 0xAA,
		// wasEscaped=false (real wire SYN, mid-frame = AA-injection)
	}, now)
	if drop {
		t.Error("Observe(mid-frame wire 0xAA) returned drop=true in ModeEnforce; B3.4 must return drop=false until B3.6 wires real filtering")
	}

	// The FSM should have counted the drop decision.
	if got := c.FsmDropAaInjectionTotal(); got == 0 {
		t.Errorf("FsmDropAaInjectionTotal()=0; expected non-zero (FSM should emit DropAaInjection for mid-frame wire SYN)")
	}
}

// TestFSM_MultipleTelegrams_StateLifecycle pins the end-to-end
// state lifecycle across multiple telegram boundaries: idle →
// passive → byte forward → reset → idle → passive again.
func TestFSM_MultipleTelegrams_StateLifecycle(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)

	// Telegram 1.
	observeEvent(c, transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x71})
	if !c.FSMIsPassive() {
		t.Fatal("after Started #1: not passive")
	}
	observeEvent(c, transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10})
	observeEvent(c, transport.StreamEvent{Kind: transport.StreamEventReset})
	if c.FSMIsPassive() {
		t.Fatal("after Reset #1: still passive")
	}

	// Telegram 2.
	observeEvent(c, transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x10})
	if !c.FSMIsPassive() {
		t.Fatal("after Started #2: not passive")
	}

	// Final counters.
	if got := c.FsmEnterPassiveTotal(); got != 2 {
		t.Errorf("FsmEnterPassiveTotal()=%d; want 2", got)
	}
	if got := c.FsmResetTotal(); got != 1 {
		t.Errorf("FsmResetTotal()=%d; want 1", got)
	}
	if got := c.FsmForwardTotal(); got != 1 {
		t.Errorf("FsmForwardTotal()=%d; want 1 (one byte forwarded between Started #1 and Reset)", got)
	}
}
