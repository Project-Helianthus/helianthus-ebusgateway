package v8classifier

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol/telegram_fsm"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// admin_event_aa_drop_test.go pins F-NEW-26 (2026-05-21): every
// telegram_fsm.DecisionDropAaInjection in ModeShadow or ModeEnforce
// emits a per-event ClassifierAdminEvent of kind
// AdminEventKindAaInjectionDrop. Operators draining the ring via
// /debug/v8/admin-events get the byte value + FSM state at decision
// time so they can classify the shadow→enforce promotion candidates
// as true- or false-positive by the actual wire pattern, not by
// aggregate counter only.

// TestClassifier_DropAaInjection_EmitsAdminEvent_ShadowMode pins
// that ModeShadow emits the per-event detail alongside the
// counter increment. Without this surface, the shadow→enforce
// promotion gate has no byte-level evidence.
func TestClassifier_DropAaInjection_EmitsAdminEvent_ShadowMode(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	now := time.Unix(42, 0)

	// Drive to mid-frame so the FSM classifies the next 0xAA as
	// AA-injection (per v8 §4: mid-frame wire 0xAA without an
	// escape prefix is the AA-injection signal).
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted, Data: 0x71,
	}, now)
	drop := c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0xAA,
	}, now)
	if drop {
		t.Fatal("Observe in ModeShadow returned drop=true; precondition violated (Shadow must not drop)")
	}

	// Aggregate counter increments (the established contract).
	if got := c.FsmDropAaInjectionTotal(); got != 1 {
		t.Fatalf("FsmDropAaInjectionTotal()=%d; want 1 (precondition for admin emit)", got)
	}
	if got := c.ShadowWouldHaveDroppedTotal(); got != 1 {
		t.Fatalf("ShadowWouldHaveDroppedTotal()=%d; want 1", got)
	}

	// The new contract: the same drop also emits a per-event
	// admin event into the ring buffer.
	events, dropped := c.DrainAdminEvents()
	if dropped != 0 {
		t.Errorf("DrainAdminEvents: dropped=%d; want 0 (no ring saturation in this trivial test)", dropped)
	}
	if len(events) != 1 {
		t.Fatalf("DrainAdminEvents: got %d events; want 1 (per-drop emit broken)", len(events))
	}

	ev := events[0]
	if ev.Kind != AdminEventKindAaInjectionDrop {
		t.Errorf("event Kind=%v; want AdminEventKindAaInjectionDrop", ev.Kind)
	}
	if ev.Byte != 0xAA {
		t.Errorf("event Byte=0x%02X; want 0xAA (the AA-injection that triggered the drop)", ev.Byte)
	}
	if ev.WasEscaped {
		t.Error("event WasEscaped=true; want false (raw wire 0xAA, not escape-decoded)")
	}
	if ev.At != now {
		t.Errorf("event At=%v; want %v (now threaded from Observe per Codex round-1 MEDIUM)", ev.At, now)
	}
	// FSMState should be a MASTER_HEADER variant — the FSM has
	// just consumed QQ from the STARTED stream event and the
	// next byte (the 0xAA wire byte) lands in the master header
	// region where AA-injection detection runs. Exact state
	// label varies with FSM internals; we only check non-zero
	// so future FSM-state refactors don't break this regression.
	if ev.FSMState == telegram_fsm.StateIdle {
		t.Errorf("event FSMState=%v; want non-idle (FSM was mid-telegram at drop time)", ev.FSMState)
	}
}

// TestClassifier_DropAaInjection_EmitsAdminEvent_EnforceMode pins
// the same contract for ModeEnforce. The enforce mode applies
// the drop (drop=true) AND emits the event — the operator's
// audit log of what got filtered.
func TestClassifier_DropAaInjection_EmitsAdminEvent_EnforceMode(t *testing.T) {
	t.Parallel()
	c := New(ModeEnforce)
	now := time.Unix(7, 0)

	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted, Data: 0x71,
	}, now)
	drop := c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0xAA,
	}, now)
	if !drop {
		t.Fatal("Observe in ModeEnforce returned drop=false; precondition violated (Enforce must drop AA-injection)")
	}
	if got := c.EnforceDropsAppliedTotal(); got != 1 {
		t.Fatalf("EnforceDropsAppliedTotal()=%d; want 1 (precondition)", got)
	}

	events, _ := c.DrainAdminEvents()
	if len(events) != 1 {
		t.Fatalf("DrainAdminEvents: got %d events; want 1 (enforce-mode emit broken)", len(events))
	}
	if events[0].Kind != AdminEventKindAaInjectionDrop {
		t.Errorf("event Kind=%v; want AdminEventKindAaInjectionDrop", events[0].Kind)
	}
	if events[0].Byte != 0xAA {
		t.Errorf("event Byte=0x%02X; want 0xAA", events[0].Byte)
	}
}

// TestClassifier_DropAaInjection_NoEmit_ModeOff pins that
// ModeOff does NOT emit (the FSM does not run; the drop
// decision is unreachable). This is the zero-overhead contract.
func TestClassifier_DropAaInjection_NoEmit_ModeOff(t *testing.T) {
	t.Parallel()
	c := New(ModeOff)
	now := time.Unix(0, 0)

	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventStarted, Data: 0x71,
	}, now)
	c.Observe(transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0xAA,
	}, now)

	// Counter must stay 0 (FSM doesn't run).
	if got := c.FsmDropAaInjectionTotal(); got != 0 {
		t.Errorf("FsmDropAaInjectionTotal()=%d in ModeOff; want 0", got)
	}

	// Ring must be empty.
	events, _ := c.DrainAdminEvents()
	if len(events) != 0 {
		t.Errorf("DrainAdminEvents in ModeOff: got %d events; want 0 (zero-overhead contract)", len(events))
	}
}

// TestAdminEventKindAaInjectionDrop_String pins the label.
func TestAdminEventKindAaInjectionDrop_String(t *testing.T) {
	t.Parallel()
	if got := AdminEventKindAaInjectionDrop.String(); got != "aa_injection_drop" {
		t.Errorf("AdminEventKindAaInjectionDrop.String() = %q; want %q",
			got, "aa_injection_drop")
	}
}
