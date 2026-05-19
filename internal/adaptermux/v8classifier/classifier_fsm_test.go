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

// TestFSM_EnforceMode_DropsAaInjection pins the B3.6b
// behavioral contract: when the FSM emits DecisionDropAaInjection
// AND mode == ModeEnforce, Observe returns drop=true AND the
// drop counter (EnforceDropsAppliedTotal) increments. Replaces
// the B3.4 SCAFFOLD-only TestFSM_EnforceMode_DoesNotDropYet.
//
// This is the v8 §4 AA-injection filter contract — operators
// promoting from Shadow to Enforce expect mid-frame wire SYNs to
// be filtered out of cross-proxy byte streams.
func TestFSM_EnforceMode_DropsAaInjection(t *testing.T) {
	t.Parallel()
	c := New(ModeEnforce)
	now := time.Unix(0, 0)

	// Enter passive tracking so subsequent AA bytes are
	// classified mid-frame (where the FSM emits DropAaInjection
	// per v8 §4).
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted,
		Data: 0x71,
	}, now)

	// Feed a wire AUTO-SYN mid-frame — the FSM emits
	// DropAaInjection AND ModeEnforce propagates drop=true.
	drop := c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte,
		Byte: 0xAA,
		// wasEscaped=false (real wire SYN, mid-frame = AA-injection)
	}, now)
	if !drop {
		t.Error("Observe(mid-frame wire 0xAA) returned drop=false in ModeEnforce; B3.6b MUST drop AA-injection")
	}

	// Counter accounting:
	//   - FsmDropAaInjectionTotal records every FSM decision (any mode)
	//   - EnforceDropsAppliedTotal records the subset where Enforce
	//     actually applied the drop
	if got := c.FsmDropAaInjectionTotal(); got != 1 {
		t.Errorf("FsmDropAaInjectionTotal()=%d; want 1", got)
	}
	if got := c.EnforceDropsAppliedTotal(); got != 1 {
		t.Errorf("EnforceDropsAppliedTotal()=%d; want 1", got)
	}
}

// TestFSM_ShadowMode_AaInjectionCounted_NotDropped pins the
// Shadow-mode contract: the FSM emits DecisionDropAaInjection
// (and FsmDropAaInjectionTotal increments), but Observe still
// returns drop=false and EnforceDropsAppliedTotal stays at 0.
// The difference between the two counters during a Shadow
// validation run is the "would have dropped" estimate operators
// use to project enforce impact.
func TestFSM_ShadowMode_AaInjectionCounted_NotDropped(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	now := time.Unix(0, 0)

	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted,
		Data: 0x71,
	}, now)
	drop := c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte,
		Byte: 0xAA,
	}, now)
	if drop {
		t.Error("Observe in ModeShadow returned drop=true; Shadow must NEVER drop (observation-only contract)")
	}
	if got := c.FsmDropAaInjectionTotal(); got != 1 {
		t.Errorf("FsmDropAaInjectionTotal()=%d; want 1 (FSM decision still counted in Shadow)", got)
	}
	if got := c.EnforceDropsAppliedTotal(); got != 0 {
		t.Errorf("EnforceDropsAppliedTotal()=%d; want 0 (Shadow never applies drops)", got)
	}
}

// TestFSM_EnforceMode_ProtocolFaultStillForwards pins v8 I10:
// PROTOCOL_FAULT bytes are FORWARDED to all observers regardless
// of mode. Only AA-injection is filtered.
func TestFSM_EnforceMode_ProtocolFaultStillForwards(t *testing.T) {
	t.Parallel()
	c := New(ModeEnforce)
	now := time.Unix(0, 0)

	// Drive the FSM into the position where the next byte
	// triggers ProtocolFault (NN>16 in MASTER_HEADER byte 4).
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted, Data: 0x71,
	}, now)
	for _, b := range []byte{0x10, 0xB5, 0x16} {
		c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventByte, Byte: b,
		}, now)
	}
	// NN=0xFF — exceeds the 16-byte cap, FSM emits ProtocolFault.
	drop := c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0xFF,
	}, now)
	if drop {
		t.Error("Observe(ProtocolFault byte) returned drop=true; v8 I10 requires fault bytes to be FORWARDED")
	}
	if got := c.FsmProtocolFaultTotal(); got != 1 {
		t.Errorf("FsmProtocolFaultTotal()=%d; want 1", got)
	}
	// EnforceDropsAppliedTotal MUST stay at 0 — protocol faults
	// don't count as drops.
	if got := c.EnforceDropsAppliedTotal(); got != 0 {
		t.Errorf("EnforceDropsAppliedTotal()=%d; want 0 (faults are forwarded, not dropped)", got)
	}
}

// TestFSM_EnforceMode_LegitimateBytesNotDropped pins that
// Forward-class bytes (most of the telegram) are NOT dropped
// even in ModeEnforce. Only DropAaInjection triggers a drop.
func TestFSM_EnforceMode_LegitimateBytesNotDropped(t *testing.T) {
	t.Parallel()
	c := New(ModeEnforce)
	now := time.Unix(0, 0)

	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted, Data: 0x71,
	}, now)
	// Plain payload bytes — all Forward decisions.
	for _, b := range []byte{0x10, 0xB5, 0x16} {
		drop := c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventByte, Byte: b,
		}, now)
		if drop {
			t.Errorf("Observe(legitimate byte 0x%02X) returned drop=true; want false", b)
		}
	}
	if got := c.EnforceDropsAppliedTotal(); got != 0 {
		t.Errorf("EnforceDropsAppliedTotal()=%d; want 0 (only AA-injection drops)", got)
	}
}

// TestFSM_StateAborted_ContinuationCountsFaults pins the documented
// behavior when the FSM enters StateAborted: subsequent Feed calls
// continue to emit DecisionProtocolFault until a Started / Failed /
// Reset arrives. This is the FSM library's own contract; the
// classifier just records it.
//
// Operators reading FsmProtocolFaultTotal during a fault cascade
// will see N+1 increments for an N-byte trailing tail after the
// abort. Per Codex round-1 MEDIUM finding on PR #641: B3.4
// intentionally does NOT auto-reset on first fault — that would
// hide the cascade depth from operators. B3.6+ may revisit if
// the operator UX requires it; until then the test pins the
// "keep counting" behavior.
func TestFSM_StateAborted_ContinuationCountsFaults(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	now := time.Unix(0, 0)

	// Enter passive tracking, then feed a byte that triggers a
	// protocol fault. The simplest trigger is a wire-byte stream
	// where MASTER_HEADER receives a byte with WasEscaped=true
	// (escape-decoded payload byte at QQ position is invalid by
	// the FSM's per-phase rules — QQ MUST be plain). The FSM
	// emits DecisionProtocolFault and transitions to StateAborted.
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted,
		Data: 0x71,
	}, now)
	// MASTER_HEADER byte 1 (ZZ destination). Plain byte 0x10 is
	// valid; FSM stays clean.
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte,
		Byte: 0x10,
	}, now)

	// Now feed a byte that the FSM will treat as a fault. Per the
	// telegram_fsm library, a wire AUTO-SYN (Byte=0xAA, WasEscaped=
	// false) in the middle of MASTER_HEADER is AA-injection and
	// gets DecisionDropAaInjection (NOT a fault). The fault
	// trigger differs by phase. To force a fault, use the
	// MASTER_HEADER NN-byte position: if NN > 16 the FSM faults.
	// Pre-position the FSM to MASTER_HEADER byte 4 (NN slot) by
	// feeding 3 more plain bytes (PB SB at bytes 2-3, then byte 4
	// is NN).
	for _, b := range []byte{0xB5, 0x16} {
		c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventByte, Byte: b,
		}, now)
	}
	// Now byte 4 is NN. Feed 0xFF (NN=255, exceeds the 16-byte
	// cap per v8) — FSM emits DecisionProtocolFault.
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0xFF,
	}, now)

	if got := c.FsmProtocolFaultTotal(); got != 1 {
		t.Errorf("after first fault: FsmProtocolFaultTotal()=%d; want 1", got)
	}
	if got := c.FSMState(); got != telegram_fsm.StateAborted {
		t.Errorf("after fault: FSMState()=%v; want StateAborted", got)
	}

	// Continuation: feed 5 more bytes. Each should also emit
	// DecisionProtocolFault (FSM stays in StateAborted until
	// Reset/Started/Failed).
	for i := 0; i < 5; i++ {
		c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventByte, Byte: byte(i),
		}, now)
	}
	if got := c.FsmProtocolFaultTotal(); got != 6 {
		t.Errorf("after continuation: FsmProtocolFaultTotal()=%d; want 6 (1 initial + 5 continuation)", got)
	}
	if got := c.FSMState(); got != telegram_fsm.StateAborted {
		t.Errorf("during continuation: FSMState()=%v; want StateAborted (no auto-reset)", got)
	}

	// A Reset clears the fault cascade.
	c.Observe(transport.StreamEvent{Kind: transport.StreamEventReset}, now)
	if got := c.FSMState(); got != telegram_fsm.StateIdle {
		t.Errorf("after Reset: FSMState()=%v; want StateIdle", got)
	}
	// Subsequent bytes after Reset go to the Idle handler, which
	// has its own per-byte semantics (mostly plain → Forward).
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0x55,
	}, now)
	// The protocol fault counter must NOT increment further once
	// we're back in StateIdle.
	if got := c.FsmProtocolFaultTotal(); got != 6 {
		t.Errorf("after Reset+plain byte: FsmProtocolFaultTotal()=%d; want 6 (no further faults)", got)
	}
}

// TestFSM_Accessors_ConcurrentReader_DataRaceFree pins the Codex
// round-1 HIGH fix: FSMState/FSMInternalState/FSMIsPassive read
// from atomic snapshots that Observe publishes after each FSM
// mutation. The accessors MUST NOT touch the underlying FSM
// (telegram_fsm.Machine is not thread-safe).
//
// This test launches concurrent readers AND a single Observe
// goroutine; under `go test -race` any direct read of the FSM
// would fire a race condition. Passing under -race proves the
// atomic-snapshot pattern is correctly applied.
func TestFSM_Accessors_ConcurrentReader_DataRaceFree(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)

	// Pre-populate with a known state.
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted, Data: 0x71,
	}, time.Unix(0, 0))

	stop := make(chan struct{})
	defer close(stop)

	// Two reader goroutines that hammer the accessors.
	for i := 0; i < 2; i++ {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					_ = c.FSMState()
					_ = c.FSMInternalState()
					_ = c.FSMIsPassive()
					_ = c.FsmForwardTotal()
					_ = c.FsmDropAaInjectionTotal()
					_ = c.FsmProtocolFaultTotal()
				}
			}
		}()
	}

	// Single producer Observe-driving goroutine (per the
	// Classifier concurrency contract — Observe is single-
	// producer, the readers above are MULTI-CONSUMER which IS
	// allowed).
	now := time.Unix(0, 0)
	for i := 0; i < 200; i++ {
		c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventByte, Byte: byte(i),
		}, now)
		if i%20 == 0 {
			c.Observe(transport.StreamEvent{
				Kind: transport.StreamEventReset,
			}, now)
			c.Observe(transport.StreamEvent{
				Kind: transport.StreamEventStarted, Data: 0x71,
			}, now)
		}
	}
	// The test asserts no race fires via -race instrumentation.
	// We also do a final sanity check that counters are
	// monotonically positive (they should be — we fed 200
	// bytes).
	if got := c.ObservedBytesTotal(); got < 200 {
		t.Errorf("ObservedBytesTotal()=%d; want >= 200", got)
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
