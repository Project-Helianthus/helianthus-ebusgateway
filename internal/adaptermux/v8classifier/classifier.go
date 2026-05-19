package v8classifier

import (
	"sync/atomic"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Classifier is the v8 classifier surface for the adapter
// multiplexer. Phase 3 Step B3.2 (this file): SCAFFOLD ONLY. The
// methods exist with the production signature but their bodies
// are placeholder no-ops (in ModeOff) or counter-only stubs (in
// ModeShadow / ModeEnforce).
//
// The classifier instance is owned by adaptermux.Mux and lives for
// the duration of the mux. Per-session classifier state will be
// introduced in B3.4 (FSM wiring). For now the surface is mux-wide.
//
// Observe / OnAdminEvent are the two hooks the mux's read loop must
// call on every byte / admin event. In ModeOff both are constant-
// time no-ops; in ModeShadow they increment observation counters
// but do NOT alter the byte stream; in ModeEnforce they will (in
// future PRs) make the filtering decisions the gateway acts on.
//
// Concurrency contract:
//
//   - Observe and OnAdminEvent MUST be called serially by the
//     adaptermux.Mux read goroutine (single producer). B3.2 only
//     uses atomic counters which are race-tolerant on their own,
//     BUT subsequent PRs (B3.3 escape decoder, B3.4 per-session
//     FSM, B3.5 pacer + L_rtt) will add mutable state that is
//     NOT safe under concurrent Observe calls. Any future caller
//     that wants to invoke Observe from a different goroutine
//     MUST first redesign the classifier for concurrent producers.
//   - Accessors (Mode, ObservedBytesTotal, ObservedAdminEventsTotal)
//     are safe to call from any goroutine (atomic loads).
type Classifier struct {
	// mode is the runtime mode. Immutable after construction.
	mode Mode

	// observedBytesTotal counts every transport.StreamEvent that
	// passes through Observe in non-off mode. Includes
	// StreamEventByte (with WasEscaped=true|false),
	// StreamEventWireSyn, and any other stream event the mux
	// routes through the classifier. Exposed for shadow-mode
	// instrumentation to confirm the classifier is actually seeing
	// the bytes the mux sees.
	observedBytesTotal atomic.Uint64

	// observedAdminEventsTotal counts every non-None
	// transport.AdminEvent passing through OnAdminEvent in non-off
	// mode. Per v8 §1.1 / I1 admin events are out-of-band; this
	// counter just tracks the upstream signal so shadow-mode
	// operators can confirm the classifier is wired to the
	// ENHTransport's admin event surface.
	observedAdminEventsTotal atomic.Uint64

	// Phase 3 Step B3.3 (escape-decoder observer counters): the v8
	// design (§1.3 / §1.4) needs to distinguish four byte-class
	// shapes emerging from the upstream ENHTransport's AA-aware
	// escape decoder, ALL emitted as StreamEventByte but with
	// different (Byte, WasEscaped) tuples:
	//
	//   (1) escapedPayloadAaTotal — Byte=0xAA, WasEscaped=true.
	//       A data byte that happens to equal the SYN value,
	//       transported on the wire as the escape pair 0xA9 0x01.
	//       The B3.4 telegram FSM treats this as PAYLOAD; the
	//       B3.6 AA-filter uses this counter to confirm the
	//       proxy is correctly delivering payload-AA bytes
	//       (vs dropping them as injection noise).
	//
	//   (2) escapedPayloadEscTotal — Byte=0xA9, WasEscaped=true.
	//       A data byte that happens to equal the ESC lead value,
	//       transported on the wire as the escape pair 0xA9 0x00.
	//       Rare in production traffic; useful for operator
	//       sanity-checking. The B3.4 FSM treats this as PAYLOAD.
	//
	//   (3) wireAutoSynTotal — Byte=0xAA, WasEscaped=false.
	//       A REAL wire AUTO-SYN byte (bus-idle marker). The
	//       B3.4 FSM treats this as either inter-telegram filler
	//       (when IDLE) or invariant violation (when mid-frame —
	//       AA-injection that the v8 escape decoder did NOT
	//       absorb because it was inter-frame, not mid-pair).
	//
	//   (4) plainByteTotal — any other (Byte, WasEscaped=false)
	//       tuple. Regular data bytes (telegram header, CRC,
	//       ACK/NACK, source/target/PB/SB, NN, body bytes that
	//       don't equal 0xAA or 0xA9).
	//
	// Invariants:
	//   - (1) + (2) + (3) + (4) == count of StreamEventByte
	//     events passing through Observe (excludes WireSyn /
	//     Started / Failed / Reset which have their own counter
	//     paths via observedBytesTotal but NOT via these four).
	//   - Each counter monotonically non-decreasing.
	//   - Atomic, race-tolerant, single-producer write side per
	//     the Concurrency contract above.
	//
	// These counters are the building blocks for:
	//   - B3.4 telegram FSM transitions (which need the
	//     payload-vs-SYN distinction)
	//   - B3.5 per-session pacer (which needs to know whether a
	//     0xAA is filler or payload)
	//   - B3.6 AA-injection filter (the ModeEnforce drop
	//     decision keys on WasEscaped + FSM-phase)
	//   - B3.7 shadow-mode divergence comparison (counts the
	//     same shapes the existing pre-v8 path would have
	//     applied round-7/round-9 mitigations to)
	escapedPayloadAaTotal  atomic.Uint64
	escapedPayloadEscTotal atomic.Uint64
	wireAutoSynTotal       atomic.Uint64
	plainByteTotal         atomic.Uint64
}

// New constructs a Classifier in the given mode. The zero-value
// Classifier (i.e. mode=ModeOff) is also valid; New exists for
// explicit construction.
func New(mode Mode) *Classifier {
	return &Classifier{mode: mode}
}

// Mode returns the immutable runtime mode of the classifier.
// Exposed for diagnostics, expvar surfaces, and tests.
func (c *Classifier) Mode() Mode {
	if c == nil {
		return ModeOff
	}
	return c.mode
}

// Observe is the per-byte hook the mux's read loop calls on every
// transport.StreamEvent that flows through the upstream. The hook
// is allocation-free on the hot path.
//
// Phase 3 Step B3.3 (this file): in ModeOff the hook is a constant-
// time no-op. In ModeShadow / ModeEnforce the hook:
//
//   - increments observedBytesTotal (every StreamEvent kind);
//   - for StreamEventByte ONLY, classifies the byte into one of
//     four provenance buckets based on (Byte, WasEscaped) and
//     increments the matching counter (escapedPayloadAaTotal /
//     escapedPayloadEscTotal / wireAutoSynTotal / plainByteTotal).
//     See the field doc on Classifier for the full taxonomy.
//
// Future stacked PRs add:
//
//	B3.4: per-session telegram FSM instances (consumes the
//	      provenance counters' underlying signal to drive
//	      classify-then-transition rules from v8 §4).
//	B3.5: per-session pacer + L_rtt EMA.
//	B3.6: admin channel wiring + AA-injection drop decisions
//	      (this is where Observe starts returning drop=true in
//	      ModeEnforce for the right (Byte=0xAA, WasEscaped=false,
//	      FSM phase != IDLE) tuple).
//	B3.7: shadow-mode divergence comparison vs the current path.
//
// The `now` parameter is the monotonic-clock observation time for
// this event (v8 I0). Callers that don't care about clock semantics
// may pass time.Time{} — B3.3 ignores it; B3.5+ use it for L_rtt /
// pacer.
//
// Returns true if the caller should DROP this event from emission
// to sessions. In ModeOff / ModeShadow the return is always false.
// In B3.3 ModeEnforce also returns false (real filtering lands in
// B3.6). Callers MAY ignore the return value until B3.6.
func (c *Classifier) Observe(event transport.StreamEvent, now time.Time) (drop bool) {
	if c == nil || c.mode == ModeOff {
		return false
	}
	c.observedBytesTotal.Add(1)

	// Provenance classification fires only for StreamEventByte.
	// WireSyn / Started / Failed / Reset / unknown kinds bypass —
	// they have their own semantic meaning and do NOT carry a
	// (Byte, WasEscaped) tuple the escape-decoder provenance
	// taxonomy applies to.
	if event.Kind == transport.StreamEventByte {
		c.classifyByteProvenance(event.Byte, event.WasEscaped)
	}
	_ = now
	return false
}

// classifyByteProvenance increments exactly one of the four
// provenance counters based on the (b, wasEscaped) tuple. Extracted
// for testability — the routing logic is small but the four-way
// branch deserves its own coverage in
// classifier_provenance_test.go.
//
// The taxonomy (per the Classifier struct field doc):
//
//	(0xAA, true)  → escapedPayloadAaTotal   — payload 0xAA byte
//	(0xA9, true)  → escapedPayloadEscTotal  — payload 0xA9 byte
//	(0xAA, false) → wireAutoSynTotal        — real wire AUTO-SYN
//	anything else → plainByteTotal          — regular data byte
//
// The "anything else" branch covers:
//   - all (0x00..0xA8, false)
//   - (0xA9, false) — should be impossible in a well-formed v8
//     ENHTransport stream (a bare 0xA9 wire byte is the escape
//     lead, not an emitted decoded byte), but counted defensively
//     as plain to avoid silent drops.
//   - all (0xAB..0xFF, false)
//   - any (b, true) where b is neither 0xAA nor 0xA9 — should be
//     impossible (the v8 escape decoder only emits wasEscaped=true
//     for the two valid escape pairs), but counted defensively.
//
// The defensive branches mean ALL StreamEventByte events
// monotonically increment exactly one counter, regardless of
// upstream bug or future protocol extension.
func (c *Classifier) classifyByteProvenance(b byte, wasEscaped bool) {
	if wasEscaped {
		switch b {
		case 0xAA:
			c.escapedPayloadAaTotal.Add(1)
			return
		case 0xA9:
			c.escapedPayloadEscTotal.Add(1)
			return
		default:
			// Defensive: escape decoder emitted wasEscaped=true
			// for an unexpected byte. Treat as plain to avoid
			// silent drop; downstream FSM will surface the
			// anomaly via its own validation paths.
			c.plainByteTotal.Add(1)
			return
		}
	}
	// wasEscaped == false.
	if b == 0xAA {
		c.wireAutoSynTotal.Add(1)
		return
	}
	c.plainByteTotal.Add(1)
}

// OnAdminEvent is the per-admin-event hook that B3.6 will call
// when the upstream transport surfaces an admin diagnostic
// (escape-pending timeout, escape recovery, escape budget
// exhausted — see helianthus-ebusgo/transport AdminEvent kinds).
// Per v8 §1.1 / I1 admin events flow on the proxy admin channel,
// NEVER into the client byte streams.
//
// NOT WIRED IN B3.2. The current ebusgo transport surface
// exposes admin events only through internal counters
// (EscapePendingTimeoutTotal, EscapeRecoveryTotal,
// EscapeBudgetExhaustedTotal, EscapeAaAbsorbedTotal) — not as a
// separate stream. B3.6 (admin channel) extends the
// transport-to-mux boundary with a "ReadAdminEvent" surface and
// wires it to this hook. Until then OnAdminEvent is a callable
// API for direct unit testing but NOT invoked from adaptermux.Mux.
//
// Behavior: in ModeOff this is a no-op. In ModeShadow / ModeEnforce
// it increments observedAdminEventsTotal. AdminEventNone is filtered
// out (only non-None events count).
func (c *Classifier) OnAdminEvent(admin transport.AdminEvent, now time.Time) {
	if c == nil || c.mode == ModeOff || admin.Kind == transport.AdminEventNone {
		return
	}
	c.observedAdminEventsTotal.Add(1)
	_ = now
}

// ObservedBytesTotal returns the cumulative count of stream events
// observed by Observe in non-off mode. Returns 0 in ModeOff. Safe
// to call from any goroutine.
func (c *Classifier) ObservedBytesTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.observedBytesTotal.Load()
}

// ObservedAdminEventsTotal returns the cumulative count of non-None
// admin events observed by OnAdminEvent in non-off mode. Returns 0
// in ModeOff. Safe to call from any goroutine.
func (c *Classifier) ObservedAdminEventsTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.observedAdminEventsTotal.Load()
}

// EscapedPayloadAaTotal returns the cumulative count of payload
// 0xAA bytes (Byte=0xAA, WasEscaped=true — wire-transported as the
// escape pair 0xA9 0x01). Per Phase 3 Step B3.3 / v8 §1.4 this
// distinguishes a real data byte equal to the SYN value from a
// bus-idle wire AUTO-SYN (see WireAutoSynTotal). Returns 0 in
// ModeOff. Safe to call from any goroutine.
func (c *Classifier) EscapedPayloadAaTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.escapedPayloadAaTotal.Load()
}

// EscapedPayloadEscTotal returns the cumulative count of payload
// 0xA9 bytes (Byte=0xA9, WasEscaped=true — wire-transported as the
// escape pair 0xA9 0x00). Rare in production traffic; exposed for
// operator sanity-checking and shadow-mode comparison. Returns 0
// in ModeOff. Safe to call from any goroutine.
func (c *Classifier) EscapedPayloadEscTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.escapedPayloadEscTotal.Load()
}

// WireAutoSynTotal returns the cumulative count of real wire
// AUTO-SYN bytes (Byte=0xAA, WasEscaped=false). The bus-idle
// marker. Per v8 §1.4 the FSM-driven AA filter (B3.6) decides
// per-byte whether to forward the byte or drop it as
// AA-injection based on the FSM phase; the count here records
// every such byte regardless of forwarding decision. Returns 0
// in ModeOff. Safe to call from any goroutine.
func (c *Classifier) WireAutoSynTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.wireAutoSynTotal.Load()
}

// PlainByteTotal returns the cumulative count of regular data
// bytes — any (Byte, WasEscaped=false) tuple where Byte != 0xAA,
// plus the defensive fall-through bucket for any unexpected
// (WasEscaped=true, Byte not in {0xAA, 0xA9}) tuple. Returns 0
// in ModeOff. Safe to call from any goroutine.
func (c *Classifier) PlainByteTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.plainByteTotal.Load()
}
