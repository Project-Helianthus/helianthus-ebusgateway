package v8classifier

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Phase 3 Step B3.2: tests for the Classifier scaffold. These pin
// the no-op / counter-only behavior that subsequent stacked PRs
// (B3.3..B3.7) will extend with real classification logic.

func TestClassifier_ZeroValue_IsModeOff(t *testing.T) {
	t.Parallel()

	var c Classifier
	if got := c.Mode(); got != ModeOff {
		t.Errorf("zero-value Classifier.Mode() = %v; want ModeOff", got)
	}
}

func TestClassifier_New_PinsMode(t *testing.T) {
	t.Parallel()

	for _, m := range []Mode{ModeOff, ModeShadow, ModeEnforce} {
		c := New(m)
		if got := c.Mode(); got != m {
			t.Errorf("New(%v).Mode() = %v; want %v", m, got, m)
		}
	}
}

func TestClassifier_Observe_OffMode_IsNoOp(t *testing.T) {
	t.Parallel()

	c := New(ModeOff)
	now := time.Unix(0, 0)
	for i := 0; i < 100; i++ {
		drop := c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventByte,
			Byte: byte(i),
		}, now)
		if drop {
			t.Errorf("Observe(...) returned drop=true in ModeOff; must always return false (no-op)")
		}
	}
	if got := c.ObservedBytesTotal(); got != 0 {
		t.Errorf("ObservedBytesTotal() = %d; want 0 in ModeOff (no observation)", got)
	}
}

func TestClassifier_Observe_ShadowMode_CountsButDoesNotDrop(t *testing.T) {
	t.Parallel()

	c := New(ModeShadow)
	now := time.Unix(0, 0)
	const n = 50
	for i := 0; i < n; i++ {
		drop := c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventByte,
			Byte: byte(i),
		}, now)
		if drop {
			t.Errorf("iter %d: Observe(...) returned drop=true in ModeShadow; must NOT alter byte stream in shadow", i)
		}
	}
	if got := c.ObservedBytesTotal(); got != n {
		t.Errorf("ObservedBytesTotal() = %d; want %d in ModeShadow", got, n)
	}
}

func TestClassifier_Observe_EnforceMode_IdleStateBytesNotDropped(t *testing.T) {
	t.Parallel()

	// Phase 3 Step B3.6b: even in ModeEnforce, bytes the FSM
	// classifies as Forward (most non-frame traffic and any byte
	// observed without a Started/Failed predecessor) are NOT
	// dropped. Replaces the B3.2 SCAFFOLD-ONLY test — the
	// invariant has shifted from "ModeEnforce drops nothing" to
	// "ModeEnforce drops AA-injection only" (which requires
	// being mid-frame; bytes in IDLE state get Forward and pass
	// through cleanly).
	c := New(ModeEnforce)
	now := time.Unix(0, 0)
	const n = 25
	for i := 0; i < n; i++ {
		drop := c.Observe(transport.StreamEvent{
			Kind: transport.StreamEventByte,
			Byte: byte(i),
		}, now)
		if drop {
			t.Errorf("iter %d: Observe(byte 0x%02X in IDLE) returned drop=true; want false (only AA-injection mid-frame drops)", i, byte(i))
		}
	}
	if got := c.ObservedBytesTotal(); got != n {
		t.Errorf("ObservedBytesTotal() = %d; want %d in ModeEnforce", got, n)
	}
	if got := c.EnforceDropsAppliedTotal(); got != 0 {
		t.Errorf("EnforceDropsAppliedTotal() = %d; want 0 (no AA-injection in IDLE)", got)
	}
}

func TestClassifier_OnAdminEvent_OffMode_IsNoOp(t *testing.T) {
	t.Parallel()

	c := New(ModeOff)
	now := time.Unix(0, 0)
	for _, kind := range []transport.AdminEventKind{
		transport.AdminEventEscapePendingTimeout,
		transport.AdminEventEscapeRecovery,
		transport.AdminEventEscapeBudgetExhausted,
	} {
		c.OnAdminEvent(transport.AdminEvent{Kind: kind}, now)
	}
	if got := c.ObservedAdminEventsTotal(); got != 0 {
		t.Errorf("ObservedAdminEventsTotal() = %d; want 0 in ModeOff", got)
	}
}

func TestClassifier_OnAdminEvent_ShadowMode_CountsNonNone(t *testing.T) {
	t.Parallel()

	c := New(ModeShadow)
	now := time.Unix(0, 0)

	// Three real events.
	c.OnAdminEvent(transport.AdminEvent{Kind: transport.AdminEventEscapePendingTimeout}, now)
	c.OnAdminEvent(transport.AdminEvent{Kind: transport.AdminEventEscapeRecovery}, now)
	c.OnAdminEvent(transport.AdminEvent{Kind: transport.AdminEventEscapeBudgetExhausted}, now)

	// One AdminEventNone — must NOT count.
	c.OnAdminEvent(transport.AdminEvent{Kind: transport.AdminEventNone}, now)

	if got := c.ObservedAdminEventsTotal(); got != 3 {
		t.Errorf("ObservedAdminEventsTotal() = %d; want 3 (None must not count)", got)
	}
}

func TestClassifier_NilReceiverSafe(t *testing.T) {
	t.Parallel()

	// All accessors must accept a nil receiver and return sensible
	// zero-value answers. This lets adaptermux callsites use
	// `c.Observe(...)` without nil-checking when the classifier is
	// not configured (mode=off + classifier=nil is equivalent
	// behavior).
	var c *Classifier
	if got := c.Mode(); got != ModeOff {
		t.Errorf("nil.Mode() = %v; want ModeOff", got)
	}
	if drop := c.Observe(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}, time.Unix(0, 0)); drop {
		t.Errorf("nil.Observe() returned drop=true; want false (nil-safe no-op)")
	}
	c.OnAdminEvent(transport.AdminEvent{Kind: transport.AdminEventEscapePendingTimeout}, time.Unix(0, 0))
	if got := c.ObservedBytesTotal(); got != 0 {
		t.Errorf("nil.ObservedBytesTotal() = %d; want 0", got)
	}
	if got := c.ObservedAdminEventsTotal(); got != 0 {
		t.Errorf("nil.ObservedAdminEventsTotal() = %d; want 0", got)
	}
}

func TestClassifier_Observe_AllStreamEventKinds_DoNotPanic(t *testing.T) {
	t.Parallel()

	// Defensive: the classifier must accept every StreamEvent kind
	// the transport can produce without panicking. The
	// ENHTransport / ENSTransport emits StreamEventByte,
	// StreamEventWireSyn, StreamEventStarted, StreamEventFailed,
	// StreamEventReset (and possibly more).
	c := New(ModeShadow)
	now := time.Unix(0, 0)
	kinds := []transport.StreamEventKind{
		transport.StreamEventByte,
		transport.StreamEventWireSyn,
		transport.StreamEventStarted,
		transport.StreamEventFailed,
		transport.StreamEventReset,
	}
	for _, k := range kinds {
		_ = c.Observe(transport.StreamEvent{Kind: k, Byte: 0xAA}, now)
	}
	if got := c.ObservedBytesTotal(); got != uint64(len(kinds)) {
		t.Errorf("ObservedBytesTotal() = %d; want %d (all kinds counted)", got, len(kinds))
	}
}
