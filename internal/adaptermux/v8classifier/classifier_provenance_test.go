package v8classifier

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Phase 3 Step B3.3: tests for the per-byte escape-decoder
// provenance taxonomy added by the Observe path. Each test pins
// one of the four (Byte, WasEscaped) → counter mappings, plus the
// defensive fall-through branches and the cross-counter invariants.

// observeByte is a tiny helper that constructs a StreamEventByte
// with the given (Byte, WasEscaped) and feeds it to the classifier.
// Reduces noise in the assertions below.
func observeByte(c *Classifier, b byte, wasEscaped bool) {
	c.Observe(transport.StreamEvent{
		Kind:       transport.StreamEventByte,
		Byte:       b,
		WasEscaped: wasEscaped,
	}, time.Unix(0, 0))
}

// TestProvenance_EscapedPayloadAa pins the v8 §1.4 happy path: a
// (0xAA, WasEscaped=true) event represents a data byte equal to
// the SYN value, transported as 0xA9 0x01 on the wire. It MUST
// land in escapedPayloadAaTotal, NOT wireAutoSynTotal.
func TestProvenance_EscapedPayloadAa(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	const n = 5
	for i := 0; i < n; i++ {
		observeByte(c, 0xAA, true)
	}
	if got := c.EscapedPayloadAaTotal(); got != n {
		t.Errorf("EscapedPayloadAaTotal()=%d; want %d", got, n)
	}
	if got := c.WireAutoSynTotal(); got != 0 {
		t.Errorf("WireAutoSynTotal()=%d; want 0 (must not bleed into wire-syn bucket)", got)
	}
	if got := c.EscapedPayloadEscTotal(); got != 0 {
		t.Errorf("EscapedPayloadEscTotal()=%d; want 0", got)
	}
	if got := c.PlainByteTotal(); got != 0 {
		t.Errorf("PlainByteTotal()=%d; want 0", got)
	}
}

// TestProvenance_EscapedPayloadEsc pins the rarer escape pair
// 0xA9 0x00 → logical 0xA9.
func TestProvenance_EscapedPayloadEsc(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	observeByte(c, 0xA9, true)
	observeByte(c, 0xA9, true)
	if got := c.EscapedPayloadEscTotal(); got != 2 {
		t.Errorf("EscapedPayloadEscTotal()=%d; want 2", got)
	}
	if got := c.EscapedPayloadAaTotal(); got != 0 {
		t.Errorf("EscapedPayloadAaTotal()=%d; want 0", got)
	}
}

// TestProvenance_WireAutoSyn pins the canonical SYN: (0xAA, false)
// is a real wire AUTO-SYN, NOT a payload-AA. This is the
// provenance distinction the FSM (B3.4) and AA filter (B3.6)
// depend on.
func TestProvenance_WireAutoSyn(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	for i := 0; i < 10; i++ {
		observeByte(c, 0xAA, false)
	}
	if got := c.WireAutoSynTotal(); got != 10 {
		t.Errorf("WireAutoSynTotal()=%d; want 10", got)
	}
	if got := c.EscapedPayloadAaTotal(); got != 0 {
		t.Errorf("EscapedPayloadAaTotal()=%d; want 0 (must not bleed into payload bucket)", got)
	}
}

// TestProvenance_PlainByte covers regular data bytes: any
// (Byte != 0xAA, WasEscaped=false). Source/target addresses,
// PB/SB, NN, CRC, ACK/NACK, body bytes that don't equal 0xAA all
// land here.
func TestProvenance_PlainByte(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	plainBytes := []byte{0x00, 0x01, 0x08, 0x10, 0x55, 0x71, 0x7F, 0xB5, 0xFE, 0xFF}
	for _, b := range plainBytes {
		observeByte(c, b, false)
	}
	if got := c.PlainByteTotal(); got != uint64(len(plainBytes)) {
		t.Errorf("PlainByteTotal()=%d; want %d", got, len(plainBytes))
	}
	if got := c.WireAutoSynTotal(); got != 0 {
		t.Errorf("WireAutoSynTotal()=%d; want 0", got)
	}
	if got := c.EscapedPayloadAaTotal(); got != 0 {
		t.Errorf("EscapedPayloadAaTotal()=%d; want 0", got)
	}
}

// TestProvenance_DefensiveFallThrough_BareA9NotEscaped covers the
// defensive branch: a (0xA9, WasEscaped=false) tuple should not
// occur in a well-formed v8 stream (a bare 0xA9 wire byte is the
// escape lead, not an emitted decoded byte), but if it ever does,
// it lands in plainByteTotal (not silently dropped).
func TestProvenance_DefensiveFallThrough_BareA9NotEscaped(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	observeByte(c, 0xA9, false)
	if got := c.PlainByteTotal(); got != 1 {
		t.Errorf("PlainByteTotal()=%d; want 1 (bare 0xA9 is defensively counted as plain)", got)
	}
}

// TestProvenance_DefensiveFallThrough_UnexpectedEscaped covers the
// other defensive branch: a (b, WasEscaped=true) tuple where b is
// neither 0xAA nor 0xA9. The v8 escape decoder should never emit
// this, but if it does (bug or future protocol extension), the
// byte is defensively counted as plain — NOT silently dropped.
func TestProvenance_DefensiveFallThrough_UnexpectedEscaped(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	for _, b := range []byte{0x00, 0x55, 0x80, 0xFF} {
		observeByte(c, b, true)
	}
	if got := c.PlainByteTotal(); got != 4 {
		t.Errorf("PlainByteTotal()=%d; want 4 (unexpected escaped bytes defensively counted as plain)", got)
	}
	if got := c.EscapedPayloadAaTotal(); got != 0 {
		t.Errorf("EscapedPayloadAaTotal()=%d; want 0", got)
	}
}

// TestProvenance_SumInvariant pins the core invariant: for any
// stream of byte-bearing events, the four provenance counters sum
// to the count of byte-bearing events observed.
//
// F-NEW-27 (2026-05-21): StreamEventWireSyn IS a byte-bearing event
// even though its kind label differs from StreamEventByte. Per
// mux.go:1946-1952 it routes downstream as
// `onReceived(event.Byte, wasEscaped=false)` — identical to a
// StreamEventByte carrying 0xAA with WasEscaped=false. The classifier
// MUST classify it the same way; otherwise it bypasses the v8 filter
// (PR introducing this test had assumed WireSyn was a meta-event
// like Started/Failed/Reset — that assumption proved wrong in
// production where the bypass produced sustained round9 entries
// in enforce mode). Started, Failed, and Reset remain pure meta-
// events that do NOT contribute to provenance buckets.
func TestProvenance_SumInvariant(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	now := time.Unix(0, 0)

	// Mixed byte-bearing event stream.
	for _, e := range []transport.StreamEvent{
		{Kind: transport.StreamEventByte, Byte: 0xAA, WasEscaped: true},  // payload AA
		{Kind: transport.StreamEventByte, Byte: 0xAA, WasEscaped: false}, // wire SYN
		{Kind: transport.StreamEventByte, Byte: 0x55, WasEscaped: false}, // plain
		{Kind: transport.StreamEventByte, Byte: 0xA9, WasEscaped: true},  // payload ESC
		{Kind: transport.StreamEventByte, Byte: 0x71, WasEscaped: false}, // plain
		// StreamEventWireSyn IS a byte-bearing event after F-NEW-27 —
		// it lands in the wire_auto_syn bucket (kind forces
		// WasEscaped=false regardless of event.WasEscaped).
		{Kind: transport.StreamEventWireSyn, Byte: 0xAA},
		// Pure meta-events — observed but NOT classified.
		{Kind: transport.StreamEventStarted, Data: 0x71},
		{Kind: transport.StreamEventFailed, Data: 0x10},
		{Kind: transport.StreamEventReset},
	} {
		c.Observe(e, now)
	}

	const byteBearingEventCount = 6 // 5 StreamEventByte + 1 StreamEventWireSyn

	sum := c.EscapedPayloadAaTotal() +
		c.EscapedPayloadEscTotal() +
		c.WireAutoSynTotal() +
		c.PlainByteTotal()
	if sum != byteBearingEventCount {
		t.Errorf("sum of provenance counters = %d; want %d (one bucket per byte-bearing event including WireSyn)",
			sum, byteBearingEventCount)
	}

	// Per-bucket breakdown.
	if got := c.EscapedPayloadAaTotal(); got != 1 {
		t.Errorf("EscapedPayloadAaTotal()=%d; want 1", got)
	}
	if got := c.EscapedPayloadEscTotal(); got != 1 {
		t.Errorf("EscapedPayloadEscTotal()=%d; want 1", got)
	}
	// WireAutoSynTotal = 2: one StreamEventByte(0xAA, !escaped) +
	// one StreamEventWireSyn (forced-escape-false).
	if got := c.WireAutoSynTotal(); got != 2 {
		t.Errorf("WireAutoSynTotal()=%d; want 2 (1 Byte-stream + 1 WireSyn — F-NEW-27 closes the bypass)", got)
	}
	if got := c.PlainByteTotal(); got != 2 {
		t.Errorf("PlainByteTotal()=%d; want 2", got)
	}

	// All event kinds DID increment observedBytesTotal (per B3.2
	// contract — every StreamEvent kind counts there). Pure
	// meta-events (Started/Failed/Reset) do NOT touch any
	// provenance bucket.
	if got := c.ObservedBytesTotal(); got != 9 {
		t.Errorf("ObservedBytesTotal()=%d; want 9 (5 Byte + 1 WireSyn + 3 meta)", got)
	}
}

// TestProvenance_OffMode_AllCountersZero pins the production-default
// invariant: ModeOff yields ZERO observation overhead — no
// provenance bucket ever increments.
func TestProvenance_OffMode_AllCountersZero(t *testing.T) {
	t.Parallel()
	c := New(ModeOff)
	for i := 0; i < 20; i++ {
		observeByte(c, 0xAA, true)
		observeByte(c, 0xAA, false)
		observeByte(c, 0x55, false)
	}
	if got := c.EscapedPayloadAaTotal(); got != 0 {
		t.Errorf("EscapedPayloadAaTotal()=%d; want 0 in ModeOff", got)
	}
	if got := c.WireAutoSynTotal(); got != 0 {
		t.Errorf("WireAutoSynTotal()=%d; want 0 in ModeOff", got)
	}
	if got := c.PlainByteTotal(); got != 0 {
		t.Errorf("PlainByteTotal()=%d; want 0 in ModeOff", got)
	}
	if got := c.EscapedPayloadEscTotal(); got != 0 {
		t.Errorf("EscapedPayloadEscTotal()=%d; want 0 in ModeOff", got)
	}
}

// TestProvenance_NilReceiverSafe pins that all four new accessors
// accept a nil receiver and return 0. Lets adaptermux callsites
// use them without nil-checking.
func TestProvenance_NilReceiverSafe(t *testing.T) {
	t.Parallel()
	var c *Classifier
	if got := c.EscapedPayloadAaTotal(); got != 0 {
		t.Errorf("nil.EscapedPayloadAaTotal()=%d; want 0", got)
	}
	if got := c.EscapedPayloadEscTotal(); got != 0 {
		t.Errorf("nil.EscapedPayloadEscTotal()=%d; want 0", got)
	}
	if got := c.WireAutoSynTotal(); got != 0 {
		t.Errorf("nil.WireAutoSynTotal()=%d; want 0", got)
	}
	if got := c.PlainByteTotal(); got != 0 {
		t.Errorf("nil.PlainByteTotal()=%d; want 0", got)
	}
}

// TestClassifyByteProvenance_TableDriven exercises classifyByteProvenance
// directly (NOT through Observe) so the four-way routing branch has
// dedicated, isolated coverage. Per Codex round-1 hygiene note on
// PR #640: the method's doc-comment claims testability as the
// extraction rationale; this is the test that earns that claim.
//
// Each row asserts: feeding (b, wasEscaped) to classifyByteProvenance
// increments exactly the expected counter by 1 and leaves the
// other three at 0.
func TestClassifyByteProvenance_TableDriven(t *testing.T) {
	t.Parallel()

	rows := []struct {
		name                                                  string
		b                                                     byte
		wasEscaped                                            bool
		wantPayloadAa, wantPayloadEsc, wantWireSyn, wantPlain uint64
	}{
		// Canonical taxonomy.
		{"PayloadAA", 0xAA, true, 1, 0, 0, 0},
		{"PayloadESC", 0xA9, true, 0, 1, 0, 0},
		{"WireSYN", 0xAA, false, 0, 0, 1, 0},
		// Several plain-byte representatives across the byte range.
		{"Plain_0x00", 0x00, false, 0, 0, 0, 1},
		{"Plain_0x55", 0x55, false, 0, 0, 0, 1},
		{"Plain_0x71_gateway", 0x71, false, 0, 0, 0, 1},
		{"Plain_0xA8_belowAA", 0xA8, false, 0, 0, 0, 1},
		{"Plain_0xAB_aboveAA", 0xAB, false, 0, 0, 0, 1},
		{"Plain_0xFE_broadcast", 0xFE, false, 0, 0, 0, 1},
		{"Plain_0xFF", 0xFF, false, 0, 0, 0, 1},
		// Defensive fall-throughs (impossible in a well-formed
		// v8 stream but folded into plain by design).
		{"Defensive_bareA9_notEscaped", 0xA9, false, 0, 0, 0, 1},
		{"Defensive_0x42_escaped", 0x42, true, 0, 0, 0, 1},
		{"Defensive_0xFF_escaped", 0xFF, true, 0, 0, 0, 1},
		{"Defensive_0x00_escaped", 0x00, true, 0, 0, 0, 1},
	}

	for _, row := range rows {
		row := row
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			// Fresh classifier per row — isolation.
			c := New(ModeShadow)
			c.classifyByteProvenance(row.b, row.wasEscaped)

			if got := c.EscapedPayloadAaTotal(); got != row.wantPayloadAa {
				t.Errorf("EscapedPayloadAaTotal()=%d; want %d", got, row.wantPayloadAa)
			}
			if got := c.EscapedPayloadEscTotal(); got != row.wantPayloadEsc {
				t.Errorf("EscapedPayloadEscTotal()=%d; want %d", got, row.wantPayloadEsc)
			}
			if got := c.WireAutoSynTotal(); got != row.wantWireSyn {
				t.Errorf("WireAutoSynTotal()=%d; want %d", got, row.wantWireSyn)
			}
			if got := c.PlainByteTotal(); got != row.wantPlain {
				t.Errorf("PlainByteTotal()=%d; want %d", got, row.wantPlain)
			}

			// Sum invariant always holds: exactly one counter
			// incremented per call.
			sum := c.EscapedPayloadAaTotal() + c.EscapedPayloadEscTotal() +
				c.WireAutoSynTotal() + c.PlainByteTotal()
			if sum != 1 {
				t.Errorf("sum=%d; want 1 (exactly one bucket per call)", sum)
			}
		})
	}
}

// TestProvenance_EnforceMode_IdleStateAllBytesForwarded pins the
// B3.6b contract for IDLE-state bytes: the four canonical
// provenance shapes plus the two defensive fall-throughs are ALL
// forwarded (drop=false) when fed in IDLE. The FSM's IDLE handler
// emits Forward for every byte regardless of provenance, so no
// drops fire. AA-injection drops require MID-FRAME context (see
// TestFSM_EnforceMode_DropsAaInjection for that path).
//
// Replaces the B3.3 SCAFFOLD-only TestProvenance_EnforceMode_DoesNotDropYet.
func TestProvenance_EnforceMode_IdleStateAllBytesForwarded(t *testing.T) {
	t.Parallel()
	c := New(ModeEnforce)
	now := time.Unix(0, 0)
	// Feed the four canonical shapes plus the two defensive
	// fall-throughs. NONE should produce drop=true in IDLE.
	for _, e := range []transport.StreamEvent{
		{Kind: transport.StreamEventByte, Byte: 0xAA, WasEscaped: true},
		{Kind: transport.StreamEventByte, Byte: 0xA9, WasEscaped: true},
		{Kind: transport.StreamEventByte, Byte: 0xAA, WasEscaped: false},
		{Kind: transport.StreamEventByte, Byte: 0x55, WasEscaped: false},
		{Kind: transport.StreamEventByte, Byte: 0xA9, WasEscaped: false}, // defensive
		{Kind: transport.StreamEventByte, Byte: 0x42, WasEscaped: true},  // defensive
	} {
		if drop := c.Observe(e, now); drop {
			t.Errorf("Observe(%+v) in IDLE returned drop=true; want false (IDLE forwards all bytes)", e)
		}
	}
	if got := c.EnforceDropsAppliedTotal(); got != 0 {
		t.Errorf("EnforceDropsAppliedTotal() = %d; want 0 (no AA-injection in IDLE)", got)
	}
}
