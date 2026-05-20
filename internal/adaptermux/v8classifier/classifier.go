package v8classifier

import (
	"sync/atomic"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol/telegram_fsm"
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

	// Phase 3 Step B3.4 (per-byte telegram FSM driver, v8 §3 / §4):
	// fsm is the single mux-wide telegram FSM instance the classifier
	// drives. The instance is created in New() when mode != ModeOff;
	// it remains nil in ModeOff (zero-allocation default).
	//
	// FSM lifecycle:
	//   - StreamEventByte → fsm.Feed(byte, wasEscaped). The returned
	//     Decision goes into one of the three fsmDecision*Total
	//     counters below.
	//   - StreamEventStarted / StreamEventFailed → fsm.EnterPassiveTracking().
	//     Both events signal "a telegram is starting on the wire and
	//     the event Data carries QQ"; the classifier observes the
	//     subsequent bytes as PASSIVE_TRACKING from MASTER_HEADER
	//     byte 1 onward (matching the FSM library's EnterPassiveTracking
	//     contract: QQ already consumed via event.Data, next byte
	//     is ZZ).
	//   - StreamEventReset → fsm.ResetToIdle(). Transport reset
	//     invalidates any in-flight telegram state.
	//
	// Per the Concurrency contract above, fsm is single-producer
	// (mux read goroutine). FSM state accessors (FSMState,
	// FSMInternalState, IsPassiveTracking) return point-in-time
	// snapshots and are safe to call from other goroutines for
	// diagnostics; tests using the accessors must accept that the
	// state may change between the call and any subsequent read.
	//
	// B3.4 is OBSERVE-ONLY. The Decision returned by Feed is counted
	// but does NOT yet affect emission (B3.6 wires the
	// DecisionDropAaInjection → drop=true path in ModeEnforce).
	fsm *telegram_fsm.Machine

	// FSM decision counters. The three values that telegram_fsm.Decision
	// can take (Forward, DropAaInjection, ProtocolFault) each get
	// their own counter. Sum across the three == count of
	// StreamEventByte events that the FSM Feed processed, with two
	// caveats: (1) when fsm is nil (ModeOff) NONE of these
	// increment; (2) bytes arriving while the FSM is in
	// StateAborted will continue through Feed but the Decision
	// breakdown depends on the FSM library's own contract — see
	// telegram_fsm.go.
	fsmForwardTotal         atomic.Uint64
	fsmDropAaInjectionTotal atomic.Uint64
	fsmProtocolFaultTotal   atomic.Uint64

	// FSM lifecycle event counters. Pin how many times we entered
	// passive tracking vs reset, for shadow-mode operators who want
	// to confirm the classifier is seeing the same telegram
	// boundaries the legacy passive observer sees.
	fsmEnterPassiveTotal atomic.Uint64
	fsmResetTotal        atomic.Uint64

	// Atomic snapshots of the FSM's externally-visible state. Per
	// Codex round-1 review on PR #641: telegram_fsm.Machine is NOT
	// thread-safe (its Machine.State() reads the same mutable
	// fields Observe writes via Feed/Enter*/Reset). Direct
	// accessors that touched c.fsm would race against the mux
	// read goroutine's Observe calls.
	//
	// Fix: Observe writes these atomic snapshots AFTER each FSM
	// mutation. The accessors (FSMState/FSMInternalState/
	// FSMIsPassive) read ONLY from the atomics — never touch
	// c.fsm — so concurrent diagnostics readers are race-free.
	//
	// Encoding: telegram_fsm.State is uint8; we store it in
	// atomic.Uint32 (Go has no atomic.Uint8). FSMIsPassive uses
	// atomic.Bool.
	fsmStateSnapshot         atomic.Uint32 // telegram_fsm.State cast to uint32
	fsmInternalStateSnapshot atomic.Uint32 // telegram_fsm.State cast to uint32
	fsmIsPassiveSnapshot     atomic.Bool

	// Phase 3 Step B3.6a (v8 §1.1 / I1): per-classifier outbound
	// admin-event ring buffer. Records PROTOCOL_FAULT and (in
	// later B3.6 PRs) echo-deadline / queue-overflow events.
	// Allocation-free emit on the hot path; the buffer drops the
	// OLDEST entry on overflow (FIFO) so the producer never
	// blocks on a slow consumer.
	//
	// Per v8 invariant I1: these events flow OUT-OF-BAND only.
	// They MUST NOT be serialized into any cross-proxy session
	// byte stream. The ring buffer is drained by a
	// proxy-operator-facing surface (admin /debug endpoint,
	// expvar, or a poll goroutine that pushes to logs) — none of
	// those paths bridge back to client byte streams.
	adminEvents adminEventBuffer

	// Phase 3 Step B3.6b (v8 §4 AA-injection filter): cumulative
	// count of bytes the classifier ACTUALLY DROPPED in
	// ModeEnforce. Distinct from fsmDropAaInjectionTotal:
	//   - fsmDropAaInjectionTotal counts every time the FSM
	//     returned DecisionDropAaInjection, in ALL modes
	//     (including Shadow, where the drop is observed but NOT
	//     applied — the byte still reaches onReceived).
	//   - enforceDropsAppliedTotal counts only the cases where
	//     mode == Enforce AND Observe returned drop=true to its
	//     caller (the mux's readLoop).
	// In ModeShadow this counter stays at 0 even when
	// fsmDropAaInjectionTotal accumulates. The difference between
	// the two counters under sustained Shadow operation is the
	// "would have dropped if we were enforcing" estimate — useful
	// for pre-enforce validation runs (Step C live-bus
	// validation, B3.7).
	enforceDropsAppliedTotal atomic.Uint64
}

// New constructs a Classifier in the given mode. The zero-value
// Classifier (i.e. mode=ModeOff) is also valid; New exists for
// explicit construction.
//
// Phase 3 Step B3.4: when mode != ModeOff the classifier owns a
// fresh telegram_fsm.Machine (StateIdle). In ModeOff the fsm field
// remains nil, preserving the zero-allocation default.
func New(mode Mode) *Classifier {
	c := &Classifier{mode: mode}
	if mode != ModeOff {
		c.fsm = telegram_fsm.New()
	}
	return c
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
//
// Phase 3 Step B3.6b (v8 §4 AA-injection filter): ModeEnforce now
// returns true when the byte is StreamEventByte AND the FSM
// returns DecisionDropAaInjection. The caller (Mux.readLoop) MUST
// honor this signal by skipping the byte's dispatch to onReceived
// — otherwise sessions would still see the AA-injection bytes and
// the v8 filter would be a no-op. The drop is also counted in
// enforceDropsAppliedTotal so operators can distinguish
// "would-have-dropped" (fsmDropAaInjectionTotal, all modes) from
// "actually-dropped" (enforceDropsAppliedTotal, Enforce only).
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
	switch event.Kind {
	case transport.StreamEventByte:
		c.classifyByteProvenance(event.Byte, event.WasEscaped)
		// driveFSMByte returns true when the byte should be
		// dropped in the current mode. Propagate to the caller.
		drop = c.driveFSMByte(event.Byte, event.WasEscaped, now)
		if drop {
			c.enforceDropsAppliedTotal.Add(1)
		}
		return drop
	case transport.StreamEventStarted, transport.StreamEventFailed:
		// Both events signal "a telegram is starting on the wire and
		// event.Data carries QQ" — the FSM's EnterPassiveTracking
		// contract matches both cases (QQ already consumed,
		// MASTER_HEADER byte 1 is the next byte fed). For mux-wide
		// observation we do NOT distinguish "we won" from
		// "someone else won" here; that distinction is per-session
		// and lands in B3.5+. EnterArbitrating is intentionally NOT
		// called by the mux-wide classifier — the wire stream is
		// observed passively in both cases.
		c.fsmEnterPassive(event)
	case transport.StreamEventReset:
		c.fsmReset()
	}
	_ = now
	return false
}

// driveFSMByte feeds one wire byte into the FSM, counts the
// returned Decision, AND updates the atomic state snapshots so
// concurrent diagnostic readers see fresh values. Single producer
// per the Classifier concurrency contract.
//
// Returns true when the caller (Observe) should propagate
// drop=true to the mux's readLoop. This is governed by the v8 §4
// AA-injection filter rules:
//
//   - ModeOff / ModeShadow: always returns false (Shadow OBSERVES
//     the FSM decision but does NOT yet alter the byte stream).
//   - ModeEnforce + DecisionDropAaInjection: returns true. The
//     byte is dropped from session dispatch. The drop counter
//     (enforceDropsAppliedTotal) is bumped in Observe.
//   - ModeEnforce + DecisionProtocolFault: returns false — per
//     v8 invariant I10, PROTOCOL_FAULT bytes are STILL forwarded
//     to all observers; the admin event is the operator-facing
//     notification only.
//
// The `now` parameter is the monotonic-clock observation time
// for this byte, threaded down from Observe (per Codex round-1
// MEDIUM on PR #643).
//
// Phase 3 Step B3.6a: DecisionProtocolFault emits a
// ClassifierAdminEvent into the per-classifier admin-event ring
// buffer (out-of-band per v8 I1 — NEVER into client byte streams).
//
// NOTE on counter/event consistency: fsmProtocolFaultTotal
// increments BEFORE the admin emit. A dashboard concurrently
// sampling the counter and draining events may briefly observe
// `counter > drained_events + dropped`. The drains are NOT an
// atomic snapshot with the counters — eventual consistency only.
// If a future operator dashboard needs exact correlation, a
// combined snapshot API would have to be added. (Codex round-1
// LOW on PR #643 — explicit documentation.)
func (c *Classifier) driveFSMByte(b byte, wasEscaped bool, now time.Time) (drop bool) {
	if c.fsm == nil {
		return false
	}
	decision := c.fsm.Feed(b, wasEscaped)
	switch decision {
	case telegram_fsm.DecisionForward:
		c.fsmForwardTotal.Add(1)
	case telegram_fsm.DecisionDropAaInjection:
		c.fsmDropAaInjectionTotal.Add(1)
		// Phase 3 Step B3.6b: ModeEnforce honors the FSM's drop
		// decision. ModeShadow counts but does NOT alter the byte
		// stream — operators run shadow first to validate the
		// classifier against the wire before promoting to enforce.
		if c.mode == ModeEnforce {
			drop = true
		}
	case telegram_fsm.DecisionProtocolFault:
		c.fsmProtocolFaultTotal.Add(1)
		// Emit out-of-band admin event. Per v8 invariant I10 the
		// byte is STILL forwarded to all observers (PROTOCOL_FAULT
		// visibility); the admin event is the operator-facing
		// notification only. drop stays false — fault bytes are
		// NEVER filtered out regardless of mode.
		c.adminEvents.emit(ClassifierAdminEvent{
			At:         now,
			Kind:       AdminEventKindProtocolFault,
			FSMState:   c.fsm.State(),
			Byte:       b,
			WasEscaped: wasEscaped,
		})
	}
	c.publishFSMSnapshotLocked()
	return drop
}

// publishFSMSnapshotLocked writes the FSM's current externally-
// visible state into the atomic snapshot fields. "Locked" here
// means "called from the single-producer Observe path" — the
// snapshot writes are atomics, no mutex involved. Concurrent
// readers (FSMState/FSMInternalState/FSMIsPassive) read ONLY from
// these atomics, so they never race against c.fsm's mutable
// fields.
//
// MUST be called after every FSM mutation in driveFSMByte,
// fsmEnterPassive, and fsmReset.
func (c *Classifier) publishFSMSnapshotLocked() {
	c.fsmStateSnapshot.Store(uint32(c.fsm.State()))
	c.fsmInternalStateSnapshot.Store(uint32(c.fsm.InternalState()))
	c.fsmIsPassiveSnapshot.Store(c.fsm.IsPassive())
}

// fsmEnterPassive routes a StreamEventStarted or StreamEventFailed
// into the FSM's passive-tracking entry. Per v8 §3.1: the event's
// Data byte IS the winner's QQ; EnterPassiveTracking sets the FSM
// to MASTER_HEADER byte 1 (skipping over QQ).
//
// If the FSM is not currently in StateIdle (e.g. a stale Started
// arrives while we're still tracking a prior telegram), the
// FSM library's EnterPassiveTracking implementation clears the
// per-telegram fields (masterNN, masterBytesConsumed, masterDest,
// retx counters, slaveNN) BEFORE setting the new
// StateMasterHeader / passive=true. It does NOT call ResetToIdle
// internally, but the field-clear is equivalent for the
// classifier's purposes — no stale telegram-1 fields leak into
// telegram-2 tracking. If the FSM library ever adds new
// per-telegram fields that EnterPassiveTracking doesn't clear,
// this site would need an explicit ResetToIdle prefix. (Codex
// round-1 LOW finding on PR #641, 2026-05-19.)
//
// This matches the wire-truth invariant: an actual STARTED/FAILED
// on the wire MUST drive the classifier's view, even if our
// prior state expected a different continuation.
func (c *Classifier) fsmEnterPassive(_ transport.StreamEvent) {
	if c.fsm == nil {
		return
	}
	c.fsm.EnterPassiveTracking()
	c.fsmEnterPassiveTotal.Add(1)
	c.publishFSMSnapshotLocked()
}

// fsmReset routes a StreamEventReset into the FSM's hard reset.
// Transport-level reset invalidates any in-flight telegram state.
func (c *Classifier) fsmReset() {
	if c.fsm == nil {
		return
	}
	c.fsm.ResetToIdle()
	c.fsmResetTotal.Add(1)
	c.publishFSMSnapshotLocked()
}

// classifyByteProvenance increments exactly one of the four
// provenance counters based on the (b, wasEscaped) tuple. Extracted
// to keep Observe small and to support direct table-test coverage
// of the four-way branch (see
// classifier_provenance_test.go::TestClassifyByteProvenance_TableDriven).
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
//     lead, not an emitted decoded byte). Folded into plain so the
//     sum invariant holds; B3.3 INTENTIONALLY does NOT surface
//     this as a separate anomaly counter — that is a follow-up
//     enhancement once B3.4+ has the FSM context to decide whether
//     the anomaly is meaningful (mid-frame anomaly vs harmless
//     idle artifact).
//   - all (0xAB..0xFF, false)
//   - any (b, true) where b is neither 0xAA nor 0xA9 — should be
//     impossible (the v8 escape decoder only emits wasEscaped=true
//     for the two valid escape pairs). Same B3.3-intentional
//     "fold into plain, don't surface separately" treatment.
//
// The defensive branches mean ALL StreamEventByte events
// monotonically increment exactly one counter, regardless of
// upstream bug or future protocol extension. If operators want
// distinct visibility into these "impossible" cases, a future
// PR can split out anomaly counters without breaking any existing
// accessor's contract — the sum invariant is preserved either way.
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

// FSMState returns the current point-in-time snapshot of the
// telegram FSM's externally-visible state (StatePassiveTracking
// composite collapsed via Machine.State). Returns telegram_fsm.StateIdle
// when fsm is nil (ModeOff or pre-construction).
//
// Race-safe via the fsmStateSnapshot atomic: this accessor reads
// ONLY the atomic field, never touches c.fsm directly. Concurrent
// callers may see a snapshot one or two Observe calls stale but
// will never see a torn read. (Codex round-1 HIGH finding on
// PR #641: telegram_fsm.Machine is not thread-safe.)
//
// Phase 3 Step B3.4 exposes this primarily for shadow-mode
// operator-visibility and for the wiring tests that assert the
// FSM is actually being driven through telegram boundaries.
func (c *Classifier) FSMState() telegram_fsm.State {
	if c == nil {
		return telegram_fsm.StateIdle
	}
	return telegram_fsm.State(c.fsmStateSnapshot.Load())
}

// FSMInternalState returns the FSM's raw sub-phase (un-collapsed
// by the PassiveTracking composite mapping). Returns
// telegram_fsm.StateIdle when fsm is nil. Useful for tests that
// want to assert "the FSM is in MASTER_DATA right now" without
// the StatePassiveTracking collapse hiding the sub-phase.
//
// Race-safe via the fsmInternalStateSnapshot atomic; same
// stale-but-not-torn guarantee as FSMState.
func (c *Classifier) FSMInternalState() telegram_fsm.State {
	if c == nil {
		return telegram_fsm.StateIdle
	}
	return telegram_fsm.State(c.fsmInternalStateSnapshot.Load())
}

// FSMIsPassive reports whether the FSM is currently in
// PASSIVE_TRACKING (we're observing a foreign initiator's
// telegram). Returns false when fsm is nil.
//
// Race-safe via the fsmIsPassiveSnapshot atomic; same
// stale-but-not-torn guarantee as FSMState.
func (c *Classifier) FSMIsPassive() bool {
	if c == nil {
		return false
	}
	return c.fsmIsPassiveSnapshot.Load()
}

// FsmForwardTotal returns the cumulative count of
// telegram_fsm.DecisionForward decisions the FSM emitted for
// StreamEventByte events observed in non-off mode. Returns 0 in
// ModeOff. Safe to call from any goroutine.
func (c *Classifier) FsmForwardTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.fsmForwardTotal.Load()
}

// FsmDropAaInjectionTotal returns the cumulative count of
// telegram_fsm.DecisionDropAaInjection decisions. Per v8 §4 these
// are the bytes the AA-injection filter would drop in ModeEnforce.
// In B3.4 the drop is NOT applied — the byte still flows to
// downstream observers — but the counter exposes how often the
// filter WOULD have fired. Returns 0 in ModeOff. Safe to call
// from any goroutine.
func (c *Classifier) FsmDropAaInjectionTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.fsmDropAaInjectionTotal.Load()
}

// FsmProtocolFaultTotal returns the cumulative count of
// telegram_fsm.DecisionProtocolFault decisions. Per v8 invariant
// I10: PROTOCOL_FAULT bytes are FORWARDED to all observers PLUS an
// admin event is emitted; B3.6 wires the admin-event side.
// Returns 0 in ModeOff. Safe to call from any goroutine.
func (c *Classifier) FsmProtocolFaultTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.fsmProtocolFaultTotal.Load()
}

// FsmEnterPassiveTotal returns the cumulative count of times the
// classifier invoked fsm.EnterPassiveTracking() in response to a
// StreamEventStarted or StreamEventFailed. Returns 0 in ModeOff.
// Safe to call from any goroutine.
func (c *Classifier) FsmEnterPassiveTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.fsmEnterPassiveTotal.Load()
}

// FsmResetTotal returns the cumulative count of times the classifier
// invoked fsm.ResetToIdle() in response to a StreamEventReset.
// Returns 0 in ModeOff. Safe to call from any goroutine.
func (c *Classifier) FsmResetTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.fsmResetTotal.Load()
}

// DrainAdminEvents returns the buffered outbound admin events
// (drained — the buffer is emptied) plus the cumulative drop
// count since the last Drain. Per v8 invariant I1: these events
// are out-of-band — callers MUST NOT forward them into any
// cross-proxy session byte stream. Acceptable destinations:
// operator logs, expvar, /debug endpoint, Prometheus metrics
// scrape.
//
// Returns (nil, 0) when the classifier is nil or has nothing
// to drain. The dropped count is non-zero only when the
// admin-event ring buffer overflowed since the last Drain.
//
// Safe to call from any goroutine — the internal mutex
// serializes drains vs the single-producer EmitAdminEvent path.
// Consumers SHOULD drain at a steady cadence (every few
// seconds in production); a stuck consumer eventually drops
// the OLDEST events FIFO, surfaced via the dropped counter.
func (c *Classifier) DrainAdminEvents() (events []ClassifierAdminEvent, dropped uint64) {
	if c == nil {
		return nil, 0
	}
	return c.adminEvents.drain()
}

// PendingAdminEvents returns the current buffer occupancy
// without draining. Useful for adaptive-drain consumers that
// only call Drain when the buffer has events. Returns 0 when
// the classifier is nil. Safe to call from any goroutine.
func (c *Classifier) PendingAdminEvents() int {
	if c == nil {
		return 0
	}
	return c.adminEvents.pending()
}

// NewPacerForSession returns a new per-session Pacer wired to
// this classifier's admin-event buffer (Phase 3 Step B3.6e). The
// pacer's watchdog timeouts (soft / hard) emit ClassifierAdminEvent
// entries with the appropriate kind, routed through the same ring
// buffer that ProtocolFault uses (per v8 invariant I1 — admin
// events stay out-of-band, never cross into client byte streams).
//
// Callers (typically Mux.ensureSessionPacer in adaptermux) use
// this in place of v8classifier.NewPacer so the watchdog is
// pre-wired without having to call SetAdminEventEmitter
// separately at every construction site.
//
// Returns nil if the classifier is nil (so call sites in ModeOff
// where m.v8 == nil get a defensive nil rather than panicking).
func (c *Classifier) NewPacerForSession() *Pacer {
	if c == nil {
		return nil
	}
	p := NewPacer()
	p.SetAdminEventEmitter(func(kind AdminEventKind, at time.Time) {
		c.adminEvents.emit(ClassifierAdminEvent{
			At:   at,
			Kind: kind,
		})
	})
	return p
}

// EnforceDropsAppliedTotal returns the cumulative count of bytes
// the classifier ACTUALLY DROPPED from session dispatch under
// ModeEnforce. Phase 3 Step B3.6b (v8 §4 AA-injection filter).
//
// Relationship to fsmDropAaInjectionTotal:
//   - fsmDropAaInjectionTotal: count of FSM DecisionDropAaInjection
//     verdicts, in ALL modes (Shadow counts even though it doesn't
//     enforce).
//   - EnforceDropsAppliedTotal: subset where mode == Enforce AND
//     Observe returned drop=true to the mux's readLoop.
//
// During a Shadow validation run before enforce promotion, the
// difference (fsmDropAaInjectionTotal - EnforceDropsAppliedTotal)
// is the "would have dropped" estimate operators can use to
// project enforce impact.
//
// Returns 0 in ModeOff or when the classifier is nil. Safe to
// call from any goroutine.
func (c *Classifier) EnforceDropsAppliedTotal() uint64 {
	if c == nil {
		return 0
	}
	return c.enforceDropsAppliedTotal.Load()
}

// ShadowWouldHaveDroppedTotal returns the cumulative count of
// bytes the classifier WOULD have dropped under ModeEnforce but
// did NOT drop because the runtime is in ModeShadow. Phase 3
// Step B3.7 (shadow-mode divergence comparison vs the legacy
// pre-v8 path).
//
// Semantics:
//   - ModeShadow: returns fsmDropAaInjectionTotal — every FSM
//     drop verdict was observed but the byte was forwarded
//     unchanged to the session dispatch path. This counter IS
//     the divergence signal: it counts the deltas between what
//     v8 would do and what the legacy filter does (the legacy
//     filter let these bytes through to the mux; v8 would
//     have dropped them).
//   - ModeEnforce: returns 0. In enforce mode there is no
//     "would have" — drops are applied. Use EnforceDropsAppliedTotal
//     for the actually-dropped count.
//   - ModeOff: returns 0. The classifier doesn't run.
//   - nil classifier: returns 0.
//
// Operator usage (canary rollout):
//   1. Deploy with HELIANTHUS_V8_CLASSIFIER_MODE=shadow.
//   2. Monitor this counter during a representative production
//      window (e.g. 24h of bus activity).
//   3. If the counter stays at 0 or stays bounded (no growth
//      under normal load), the classifier's enforce-mode impact
//      would be zero (or bounded). Promote to enforce.
//   4. If the counter grows under normal load, the classifier
//      is flagging legitimate bytes — investigate before
//      promoting (likely a false-positive AA-injection
//      classification).
//
// Prometheus alert (deployment/prometheus-alerts.md):
//
//	HelianthusV8ShadowWouldHaveDroppedTotalGrowing: alert on
//	rate(helianthus_v8_shadow_would_have_dropped_total[5m]) > 0
//	while classifier_mode=shadow. Page operators to assess
//	enforce-mode safety BEFORE rolling out.
//
// Safe to call from any goroutine.
func (c *Classifier) ShadowWouldHaveDroppedTotal() uint64 {
	if c == nil {
		return 0
	}
	if c.mode != ModeShadow {
		return 0
	}
	return c.fsmDropAaInjectionTotal.Load()
}
