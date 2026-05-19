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
// Phase 3 Step B3.2 (this file): in ModeOff the hook is a constant-
// time no-op. In ModeShadow and ModeEnforce the hook increments
// observedBytesTotal. Subsequent PRs will add:
//
//   B3.3: route StreamEventByte / StreamEventWireSyn through the v8
//         AA-aware escape decoder (already in ebusgo transport).
//   B3.4: per-session telegram FSM instances.
//   B3.5: per-session pacer + L_rtt EMA.
//   B3.6: admin channel for PROTOCOL_FAULT + queue overflow.
//   B3.7: shadow-mode divergence comparison vs the current path.
//
// The `now` parameter is the monotonic-clock observation time for
// this event (v8 I0). Callers that don't care about clock semantics
// may pass time.Time{} — the scaffold ignores it; future PRs use
// it for L_rtt / pacer.
//
// Returns true if the caller should DROP this event from emission
// to sessions. In ModeOff / ModeShadow the return is always false.
// ModeEnforce will start returning true in B3.3 for filtered
// AA-injection bytes. Callers in B3.2 may ignore the return value.
func (c *Classifier) Observe(event transport.StreamEvent, now time.Time) (drop bool) {
	if c == nil || c.mode == ModeOff {
		return false
	}
	c.observedBytesTotal.Add(1)
	// ModeShadow and ModeEnforce: no behavioral change yet. B3.3+
	// will populate this path with real classification logic.
	_ = event
	_ = now
	return false
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
