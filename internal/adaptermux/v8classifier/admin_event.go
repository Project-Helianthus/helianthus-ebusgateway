package v8classifier

import (
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol/telegram_fsm"
)

// Phase 3 Step B3.6a (frame-atomic-visibility v8 §1.1 / I1): the
// classifier's outbound admin-event surface.
//
// Per v8 invariant I1: admin events flow on the proxy admin
// channel, NEVER into the client byte streams. This file adds the
// per-classifier ring buffer that records admin events for
// operators (logging, expvar, /debug/admin endpoint, etc.) without
// any way to escape into the cross-proxy byte streams.
//
// Two distinct admin-event sources:
//
//   1. INBOUND from the upstream transport (escape decoder admin
//      events: pending timeout, recovery, budget exhausted). The
//      OnAdminEvent method already exists from B3.2 — it counts
//      these for diagnostics but does NOT yet record them as
//      ClassifierAdminEvent entries. B3.6a does NOT wire OnAdminEvent
//      into the ring buffer because the upstream transport's
//      ReadAdminEvent surface doesn't exist yet (separate Phase 1
//      follow-up in ebusgo).
//
//   2. OUTBOUND from the classifier itself. When the FSM emits
//      DecisionProtocolFault, the Observe path auto-records an
//      AdminEventProtocolFault entry. B3.6c-d will add
//      AdminEventEchoSoftTimeout, AdminEventEchoHardTimeout, and
//      AdminEventQueueOverflow when the pacer wiring lands.
//
// Concurrency:
//   - EmitAdminEvent is called from the single mux-read-goroutine
//     producer (per the Classifier-wide single-producer contract).
//   - DrainAdminEvents is multi-consumer safe via the internal
//     mutex.
//   - Ring buffer drops the OLDEST entry on overflow (never blocks
//     the producer). The dropped count is reported on Drain so
//     operators can detect saturation.

// AdminEventKind enumerates the classifier's outbound admin event
// kinds. The numeric values are stable for log/expvar emission.
type AdminEventKind int

const (
	// AdminEventKindNone is the zero value. Should not appear in
	// recorded events (used as a sentinel in tests).
	AdminEventKindNone AdminEventKind = iota

	// AdminEventKindProtocolFault is emitted when the FSM returns
	// telegram_fsm.DecisionProtocolFault for a byte. Per v8
	// invariant I10 the byte is STILL forwarded to all observers
	// (PROTOCOL_FAULT visibility); this admin event is the
	// out-of-band notification for the proxy operator.
	AdminEventKindProtocolFault

	// AdminEventKindEchoSoftTimeout is emitted when the
	// echo-watchdog soft deadline fires for an active write.
	// B3.6c wires this when the pacer + watchdog land.
	AdminEventKindEchoSoftTimeout

	// AdminEventKindEchoHardTimeout is emitted when the
	// echo-watchdog hard deadline fires for an active write.
	// B3.6c wires this.
	AdminEventKindEchoHardTimeout

	// AdminEventKindQueueOverflow is emitted when the per-session
	// admin-event ring buffer drops entries. Pre-emit (the
	// producer detects the next emit would overflow). B3.6c+
	// expand the scope to per-session sendCh overflow too.
	AdminEventKindQueueOverflow

	// AdminEventKindAaInjectionDrop is emitted when the FSM
	// returns telegram_fsm.DecisionDropAaInjection — a mid-frame
	// wire AUTO-SYN (0xAA) the v8 classifier identifies as an
	// AA-injection event. In ModeShadow the byte is COUNTED only
	// (ShadowWouldHaveDroppedTotal increments); in ModeEnforce
	// the byte is dropped from the cross-proxy stream
	// (EnforceDropsAppliedTotal increments). In ModeOff the FSM
	// does not run and this kind is unreachable.
	//
	// The per-event surface is the operator's introspection
	// channel for the shadow→enforce promotion gate: each event
	// captures the wire byte and FSM state at the time of the
	// would-have-drop decision, so the operator can correlate
	// the aggregate counter against the actual byte patterns and
	// decide whether the classifier is flagging true protocol
	// garbage (safe to promote) or legitimate traffic (over-
	// eager — do not promote).
	//
	// Per v8 §1.1 / I1 these events live OUT-OF-BAND and MUST
	// NOT be serialized into any cross-proxy session's byte
	// stream — the v8 invariant that motivated the whole
	// admin-event channel design.
	AdminEventKindAaInjectionDrop
)

// String returns the canonical lowercase label for the admin
// event kind. Stable for log lines and expvar labels.
func (k AdminEventKind) String() string {
	switch k {
	case AdminEventKindNone:
		return "none"
	case AdminEventKindProtocolFault:
		return "protocol_fault"
	case AdminEventKindEchoSoftTimeout:
		return "echo_soft_timeout"
	case AdminEventKindEchoHardTimeout:
		return "echo_hard_timeout"
	case AdminEventKindQueueOverflow:
		return "queue_overflow"
	case AdminEventKindAaInjectionDrop:
		return "aa_injection_drop"
	default:
		return "unknown"
	}
}

// ClassifierAdminEvent is a recorded admin event ready for
// consumption by the proxy's admin-channel surface (operator
// logging, expvar, /debug endpoints). The struct is allocation-
// free on the recording hot path; consumers receive a slice copy
// via DrainAdminEvents.
//
// Per v8 §1.1 / I1 these events live OUT-OF-BAND. They MUST NOT
// be serialized into any cross-proxy session's byte stream.
type ClassifierAdminEvent struct {
	// At is the monotonic-clock time when the event was emitted.
	At time.Time

	// Kind is the event taxonomy bucket.
	Kind AdminEventKind

	// FSMState is the classifier's FSM state at the time of
	// emission (collapsed via Machine.State). Lets operators
	// correlate faults to the telegram phase they occurred in.
	FSMState telegram_fsm.State

	// Byte is the wire byte that triggered the event (for
	// ProtocolFault — the offending byte the FSM rejected).
	// Zero for events that aren't byte-triggered (queue overflow,
	// echo timeouts).
	Byte byte

	// WasEscaped reports whether Byte arrived as an escape-decoded
	// payload (true) or as a raw wire byte (false). Same
	// provenance signal the FSM's Feed used. Zero for non-byte
	// events.
	WasEscaped bool
}

// adminEventBufferCap is the bounded size of the per-classifier
// admin-event ring buffer. Sized to absorb a short burst of
// faults during a malformed telegram cascade without forcing the
// hot path to block on a slow consumer. Operators draining at
// realistic rates (every few seconds) will never see drops; a
// stuck consumer drops the OLDEST events first.
//
// IMPORTANT (Codex round-1 LOW on PR #643): the cap is
// intentionally small (64) so the O(N) copy-and-shift on
// overflow stays bounded. If a future PR raises this cap, the
// emit() implementation MUST switch from copy(b.events,
// b.events[1:]) to a proper head/tail ring index pattern — the
// current copy approach is acceptable at 64 but becomes
// avoidable churn during fault storms at larger caps.
const adminEventBufferCap = 64

// adminEventBuffer is the per-classifier ring buffer. Lives
// inside the Classifier struct (see classifier.go); helper
// methods on Classifier itself wrap the access pattern.
type adminEventBuffer struct {
	mu sync.Mutex

	// events is a fixed-size ring buffer. We use a slice rather
	// than a real ring (head/tail indices) because Drain returns
	// the contents anyway — the slice form makes that O(N) copy
	// and reset trivial.
	events []ClassifierAdminEvent

	// dropped counts events that the producer skipped because
	// the buffer was full. Reported by Drain so operators see
	// saturation explicitly.
	dropped uint64
}

// emit records one admin event. If the buffer is at
// adminEventBufferCap, the OLDEST entry is overwritten (FIFO
// drop) and the dropped counter increments. The producer never
// blocks.
//
// Events with Kind == AdminEventKindNone are REJECTED at the
// emit boundary (Codex round-1 LOW on PR #643). The None
// sentinel exists only as the zero value and has no production
// caller; guarding here enforces the documented invariant that
// "None should not appear in recorded events".
//
// Called from the single-producer mux read goroutine. The mutex
// protects against concurrent Drain calls from a consumer
// goroutine.
func (b *adminEventBuffer) emit(ev ClassifierAdminEvent) {
	if ev.Kind == AdminEventKindNone {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) >= adminEventBufferCap {
		// Drop oldest, slide remaining. This is O(N) per drop but
		// only fires under sustained saturation — typical
		// production has zero drops. See adminEventBufferCap doc
		// for why O(N) is acceptable at cap=64 and what to switch
		// to if the cap ever grows.
		copy(b.events, b.events[1:])
		b.events = b.events[:len(b.events)-1]
		b.dropped++
	}
	b.events = append(b.events, ev)
}

// drain returns a copy of all buffered events and clears the
// buffer. The dropped counter is also returned and reset.
// Multi-consumer safe via the mutex.
func (b *adminEventBuffer) drain() (events []ClassifierAdminEvent, dropped uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == 0 {
		dropped = b.dropped
		b.dropped = 0
		return nil, dropped
	}
	events = make([]ClassifierAdminEvent, len(b.events))
	copy(events, b.events)
	b.events = b.events[:0]
	dropped = b.dropped
	b.dropped = 0
	return events, dropped
}

// pending reports the current buffer occupancy without draining.
// Safe to call from any goroutine.
func (b *adminEventBuffer) pending() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}

// peek returns a copy of buffered events AND the cumulative
// dropped counter, WITHOUT clearing the buffer or resetting
// the counter. Both values are returned from a single mutex
// acquire so they form an internally-consistent snapshot
// (Codex round-2 MEDIUM on PR #657: separate peek + counter
// methods would race against concurrent drain/overflow emit).
//
// Used by ad-hoc operator tooling (browser, curl + jq) that
// should not consume a long-running poller's evidence stream.
// F-NEW-26 follow-up — Codex round-1 LOW on PR #657 closed
// the GET-drain footgun for concurrent consumers.
//
// The dropped counter returned here is cumulative since the
// last drain() call (NOT since process start) — drain resets
// it atomically with the event-slice clear. Mixed peek+drain
// consumers see it snap to zero on drain; peek-only consumers
// see it grow monotonically until the next drain.
//
// Multi-consumer safe via the mutex.
func (b *adminEventBuffer) peek() (events []ClassifierAdminEvent, dropped uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == 0 {
		return nil, b.dropped
	}
	events = make([]ClassifierAdminEvent, len(b.events))
	copy(events, b.events)
	return events, b.dropped
}
