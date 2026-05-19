package v8classifier

import (
	"sync"
	"time"
)

// Phase 3 Step B3.5 (frame-atomic-visibility v8 §1.4 / §4.5,
// invariant I3): the per-session output pacer and L_rtt EMA.
//
// Two responsibilities, distinct but co-located because they share
// the "this session's view of wire timing" state:
//
//   (1) Output pacer — schedules egress byte emissions so the
//       client sees ~τ_wire_byte (4.17ms) between bytes, matching
//       the natural cadence a real ENS adapter would deliver at
//       2400 baud. Without this, the proxy would burst-flush a
//       buffered telegram in a single TCP write (<1 ms perceived
//       by the client), violating §0 transparency.
//
//   (2) L_rtt EMA — tracks the round-trip time of the gateway's
//       own active-mode echoes (bytes we wrote to the wire and
//       observed echoed back). Per v8 §1.4 we ABANDON passive-mode
//       sampling entirely (Codex MAJOR finding on v7) — the
//       passive estimate poisoned the EMA with target think-time
//       and adapter jitter. Active-only sampling gives a clean
//       link-latency signal.
//
//       Grace-bootstrap regime per v8 §1.4:
//         - On long idle (>= GraceIdleThreshold), the next 3
//           echoes are GRACE_BOOTSTRAP samples.
//         - During grace, echo soft-timeout = L_rtt_EMA + 500ms;
//           hard-timeout is disabled.
//         - After 3 grace samples, normal-mode deadlines fire:
//         - Normal soft = L_rtt_EMA + 100ms.
//         - Normal hard = 2 * L_rtt_EMA + 200ms.
//
// PR scope contract — B3.5 lands the COMPUTATION, not the wiring
// into actual session emission. Schedule, BeforeActiveWrite,
// RecordEcho, and EchoDeadlines can be called by tests and
// (eventually) by callsites in adaptermux; they return timing
// decisions without applying them.
//
// B3.6 integration map (per Codex round-2 MEDIUM on PR #642 —
// these two halves of the Pacer hook into DIFFERENT adaptermux
// paths and must NOT be conflated):
//
//   - Output pacer (Schedule, LastScheduledEmit):
//     hooks into session.go's TCP-egress path that pushes bytes
//     back to the cross-proxy CLIENT. Per v8 §4.5 ("intra-
//     telegram pacing"), each per-session sendCh-drain → conn.Write
//     emission gets paced at τ_wire_byte. The relevant choke
//     point is session.writeLoop (the goroutine draining sendCh
//     to the client TCP socket).
//
//   - L_rtt regime (BeforeActiveWrite, RecordEcho, EchoDeadlines,
//     ResetLrtt, Lrtt, InGraceBootstrap):
//     hooks into the gateway's ADAPTER-WRITE path that puts bytes
//     onto the eBUS wire. Per v8 §1.4 active-only sampling, the
//     relevant choke point is mux.go's sendLoop / doSend (or
//     wherever bytes reach transport.Write). The watchdog must
//     arm BEFORE tr.Write fires, in this sequence:
//        BeforeActiveWrite(now)        → maybe enter grace
//        soft, hard, hardEnabled := EchoDeadlines()
//        arm watchdog with (soft, hard, hardEnabled)
//        tr.Write(byte)                → byte on wire
//        ... wait for echo ...
//        RecordEcho(echoRtt, now)      → EMA update + consume grace
//
//   - Conflating the two halves (applying BeforeActiveWrite at
//     session.writeLoop, or applying Schedule at mux.doSend) would
//     defeat the design — the v8 timing decisions hinge on these
//     call sites being correctly distinguished.
//
// Concurrency: Pacer is a per-session state machine. A given
// Pacer instance MUST be accessed serially by one goroutine
// (typically the session's writer). The struct has a mutex to
// guard internal state — this makes accessors safe to call from
// any goroutine for diagnostics, but Schedule/RecordEcho callers
// must still serialize per-session.

const (
	// TauWireByte is the canonical inter-byte spacing of the eBUS
	// wire at 2400 baud with 10-bit UART framing (1 start + 8 data
	// + 1 stop). Computed as 10 bit-periods × 1/2400 sec/bit =
	// 4.1667 ms, rounded to 4167 microseconds. Per v8 §4.5 the
	// pacer schedules egress bytes at this cadence so cross-proxy
	// clients see naturally-paced wire bytes.
	TauWireByte = 4167 * time.Microsecond

	// LrttBootstrapInitial is the conservative initial L_rtt EMA
	// value on connect or after Reset. Per v8 §1.4. Bootstrap
	// large to avoid premature soft-timeouts during the first
	// echo before any samples have converged.
	LrttBootstrapInitial = 100 * time.Millisecond

	// LrttEmaAlpha is the exponential-moving-average weight for
	// new L_rtt samples. Per v8 §1.4. α=0.3 → new sample
	// contributes 30%, history contributes 70%. Balances
	// responsiveness vs noise rejection.
	LrttEmaAlpha = 0.3

	// GraceIdleThreshold is the duration of active-mode idle
	// (no echoes recorded) that triggers grace-bootstrap on the
	// next active write. Per v8 §1.4: ">30 s without active
	// write" → soft-only deadlines for the first 3 echoes.
	GraceIdleThreshold = 30 * time.Second

	// GraceBootstrapSamples is the number of consecutive echo
	// samples treated as grace after long idle. Per v8 §1.4.
	GraceBootstrapSamples = 3

	// SoftDeadlineGrace is the soft-timeout slack during grace
	// mode. Per v8 §1.4: "L_rtt_EMA + 500 ms (very loose); hard
	// disabled".
	SoftDeadlineGrace = 500 * time.Millisecond

	// SoftDeadlineNormal is the soft-timeout slack during normal
	// mode. Per v8 §1.4: "L_rtt_EMA + 100 ms".
	SoftDeadlineNormal = 100 * time.Millisecond

	// HardDeadlineNormalAdd is the additive component of the
	// hard-timeout deadline during normal mode. Per v8 §1.4:
	// "hard = 2 × L_rtt_EMA + 200 ms".
	HardDeadlineNormalAdd = 200 * time.Millisecond
)

// Pacer carries per-session output-pacing and L_rtt EMA state.
// Construct via NewPacer; the zero value is NOT valid (the
// internal mutex and the L_rtt bootstrap value need
// initialization).
type Pacer struct {
	mu sync.Mutex

	// tau is the inter-byte cadence the pacer enforces.
	// Constant after construction — exposed as a field for
	// future per-session symbol_rate adaptation (B3.7 may
	// re-introduce dynamic tau if cross-proxy session pacing
	// needs to vary).
	tau time.Duration

	// lastScheduledEmit is the wall-clock time at which the most
	// recent byte was scheduled to emit (NOT necessarily when it
	// actually emitted — that's the caller's responsibility).
	// Schedule() advances this on every call to maintain the
	// inter-byte cadence regardless of caller scheduling jitter.
	lastScheduledEmit time.Time

	// scheduledInitialized tracks whether Schedule has ever been
	// called. The first Schedule call emits at `now` (no
	// retroactive padding to bootstrap a cadence the caller
	// can't observe); subsequent calls enforce the τ cadence.
	//
	// Per Codex round-1 LOW on PR #642: previously this state
	// was inferred from lastScheduledEmit.IsZero(). That broke
	// if a caller ever passed time.Time{} as `now` — the first
	// Schedule(zero) would emit at zero, set lastScheduledEmit
	// to zero, and the NEXT call would again skip cadence
	// enforcement. The explicit boolean removes the zero-time
	// sentinel leak.
	scheduledInitialized bool

	// lRttEMA is the current exponential-moving-average estimate
	// of echo round-trip time. Bootstrapped to
	// LrttBootstrapInitial, updated by RecordEcho per v8 §1.4
	// active-only sampling.
	lRttEMA time.Duration

	// graceRemaining counts how many more echoes will be treated
	// as grace-bootstrap samples (soft-only deadlines, no hard
	// timeout). Decremented by RecordEcho. Set to
	// GraceBootstrapSamples on transition from long-idle to
	// active. Zero == normal mode.
	graceRemaining int

	// lastEchoAt is the wall-clock time of the most recent echo
	// observed via RecordEcho. Used to detect the long-idle
	// transition (now - lastEchoAt >= GraceIdleThreshold → enter
	// grace bootstrap on the next echo).
	//
	// Zero value means "no echoes observed yet". The first
	// RecordEcho call after construction does NOT trigger grace
	// bootstrap because there was no "prior active period" to be
	// idle from; bootstrap is the construction state.
	lastEchoAt time.Time

	// recordedSamples counts how many RecordEcho calls have
	// arrived. Useful for tests and diagnostics that need to
	// distinguish "pre-bootstrap" from "post-first-sample".
	recordedSamples uint64

	// beforeActiveWriteTotal counts how many BeforeActiveWrite
	// calls arrived. Phase 3 Step B3.6c — useful for operator
	// dashboards to confirm the adapter-write path is correctly
	// invoking the pacer's pre-write hook.
	beforeActiveWriteTotal uint64

	// Phase 3 Step B3.6e — echo watchdog state.
	//
	// adminEventEmit is the callback invoked when a soft or hard
	// echo deadline fires. nil disables watchdog admin-event
	// emission — counters still increment, no admin event is
	// recorded. Wired by Classifier.NewPacerForSession (which
	// routes the callback into the classifier's adminEvents ring).
	adminEventEmit AdminEventEmitFunc

	// softTimer and hardTimer are the active watchdog timers. Set
	// by ArmEchoWatchdog, cleared by CancelEchoWatchdog or by the
	// timer fire callback. nil when no active write is
	// outstanding. The Pacer tracks AT MOST ONE outstanding
	// write's watchdog — production callers follow
	// bus.sendRawWithEcho's strict "write one, wait for echo,
	// write next" cadence, so pipelined-write contention is not
	// produced. Concurrent calls to ArmEchoWatchdog cancel the
	// prior arm (the first byte's watchdog is replaced by the
	// second byte's — the first byte's echo arrival simply finds
	// no timer to cancel).
	softTimer *time.Timer
	hardTimer *time.Timer

	// armGeneration counts ArmEchoWatchdog invocations. Each
	// arm's timer-fire closures capture the generation value at
	// arm time (BY VALUE) and onSoftTimeout / onHardTimeout
	// no-op if the captured generation no longer matches
	// p.armGeneration at fire time.
	//
	// Codex round-1 MUST FIX #1 on PR #647: pointer-identity
	// against `p.softTimer` was the initial attempt, but the
	// `softT` variable that the closure captures by reference
	// is itself written by the Arm goroutine and read by the
	// timer-fire goroutine — the race detector (correctly)
	// flags that even though the temporal ordering would make
	// the read safe in practice. The generation counter is
	// captured by VALUE inside each closure, so the closure
	// holds a stable snapshot that doesn't race with the next
	// arm's writes.
	armGeneration uint64

	// softTimeoutTotal and hardTimeoutTotal count fired timeouts.
	// Diagnostic counters for operator dashboards and tests —
	// confirm the watchdog actually fires when echoes are
	// delayed, distinguishing it from no-arm misconfigurations.
	softTimeoutTotal uint64
	hardTimeoutTotal uint64

	// onTimeoutEnterHook is a TEST-ONLY synchronization hook,
	// invoked by onSoftTimeout / onHardTimeout BEFORE the
	// p.mu.Lock() that performs the generation check. Production
	// callers leave it nil; the
	// TestPacer_ArmEchoWatchdog_StaleFireRace_Deterministic test
	// uses it to deterministically interleave a Cancel call
	// between the timer fire and the lock acquisition — the
	// exact race that Codex round-1 MUST FIX #1 closes.
	//
	// Per Codex round-2 MAJOR #1 on PR #647: the earlier
	// counter-poll-based race test only exercised the race after
	// the fire callback had fully completed, which the
	// generation guard didn't actually defend against. The hook
	// pauses the callback at the perfect spot for the race to
	// be deterministic.
	//
	// Unexported (lowercase). Test access uses a same-package
	// helper in pacer_test.go.
	onTimeoutEnterHook func()
}

// AdminEventEmitFunc is the callback signature for echo-watchdog
// timeout admin events. Invoked by the soft and hard timer fire
// paths with the kind (EchoSoftTimeout / EchoHardTimeout) and the
// wall-clock time of the fire. The receiver is expected to route
// the event into the classifier's adminEvents ring buffer (or to
// any other operator-visible surface).
//
// CONTRACT (Codex round-1 LOW on PR #647): the callback runs
// AFTER the Pacer's internal mutex is dropped — so callback code
// MAY re-enter Pacer methods (e.g. WatchdogArmed, SoftTimeoutTotal,
// even ArmEchoWatchdog) without deadlock. The mutex-drop is
// intentional precisely to enable diagnostic call-backs. The
// callback MUST NOT block indefinitely (the time.AfterFunc
// goroutine is single-purpose) and MUST be safe for concurrent
// invocation if multiple pacers share a sink.
type AdminEventEmitFunc func(kind AdminEventKind, at time.Time)

// NewPacer returns a Pacer with default τ_wire_byte and
// LrttBootstrapInitial L_rtt EMA. Caller may override tau via
// SetTau (e.g. for per-session symbol_rate adaptation in B3.7).
func NewPacer() *Pacer {
	return &Pacer{
		tau:     TauWireByte,
		lRttEMA: LrttBootstrapInitial,
	}
}

// SetTau overrides the inter-byte cadence. Exposed for B3.7
// per-session symbol_rate adaptation; until then callers should
// rely on the constructor default.
//
// Per Codex round-1 MEDIUM on PR #642: invalid values (tau <= 0)
// are REJECTED silently — the prior tau is preserved. A negative
// tau would otherwise let `lastScheduledEmit.Add(p.tau)` move
// backwards, defeating monotonic pacing. A zero tau would
// collapse a burst into identical emit times (no pacing at all).
//
// Returns true if the new tau was accepted, false if rejected.
// Callers that want the "must apply or fail loudly" contract
// can assert on the bool.
func (p *Pacer) SetTau(tau time.Duration) (accepted bool) {
	if tau <= 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tau = tau
	return true
}

// Schedule reports when the next byte should be emitted given the
// current wall-clock time `now`. The returned time is
// max(now, lastScheduledEmit + tau), and lastScheduledEmit is
// advanced to the returned value.
//
// Caller usage: session.writeLoop (the goroutine that drains
// sendCh to the client's TCP socket — NOT the adapter-write
// path) calls Schedule(time.Now()) for each byte before
// conn.Write, sleeps until the returned time, then writes.
//
// PR B3.5 scope: this method is callable but session.writeLoop
// is NOT yet wired to use it. B3.6 integrates Schedule into
// session.go's sendCh-draining loop (TCP egress to the
// cross-proxy CLIENT — NOT the active-write path that goes to
// the adapter via mux.go).
func (p *Pacer) Schedule(now time.Time) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	var emitAt time.Time
	if !p.scheduledInitialized {
		// First call — emit immediately. No retroactive padding.
		// The explicit scheduledInitialized flag (not
		// lastScheduledEmit.IsZero) prevents a caller passing
		// time.Time{} as `now` from re-triggering this branch on
		// the SECOND call.
		emitAt = now
		p.scheduledInitialized = true
	} else {
		next := p.lastScheduledEmit.Add(p.tau)
		if next.After(now) {
			emitAt = next
		} else {
			emitAt = now
		}
	}
	p.lastScheduledEmit = emitAt
	return emitAt
}

// LastScheduledEmit returns the most recent Schedule output.
// Returns zero time if Schedule has never been called. Safe to
// call from any goroutine.
func (p *Pacer) LastScheduledEmit() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastScheduledEmit
}

// BeforeActiveWrite is called by the ADAPTER-WRITE path (NOT the
// session TCP-egress path) BEFORE arming the echo-watchdog for a
// new active write. The choke point is mux.go's sendLoop or
// doSend — wherever the byte is about to hit transport.Write.
// Per Codex round-1 MAJOR on PR #642: grace bootstrap MUST enter
// before the first echo of a long-idle write, so the watchdog
// uses grace deadlines on the very first post-idle echo. Doing
// the long-idle check inside RecordEcho was too late — by then
// the first echo had already been waited on with normal
// deadlines.
//
// Calling at the wrong site (e.g. session.writeLoop / sendCh
// draining) DEFEATS this fix because sendCh-draining is the TCP
// egress back to the cross-proxy client, which does NOT cause
// adapter echoes. Per Codex round-2 MEDIUM on PR #642: do NOT
// conflate these two paths.
//
// Behavior:
//   - If the pacer has prior echo history (lastEchoAt non-zero)
//     AND now - lastEchoAt >= GraceIdleThreshold, sets
//     graceRemaining = GraceBootstrapSamples.
//   - If never sampled (lastEchoAt zero), does nothing — the
//     construction state already represents bootstrap; no need
//     to re-enter grace.
//   - If we already have grace samples queued, takes the max
//     (idempotent if called twice before any RecordEcho).
//
// Callers SHOULD invoke BeforeActiveWrite immediately before
// querying EchoDeadlines() for the first byte of an active write.
// Calling it more than once before the corresponding RecordEcho
// is safe (the second call is a no-op as long as
// graceRemaining is already at GraceBootstrapSamples).
func (p *Pacer) BeforeActiveWrite(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.beforeActiveWriteTotal++
	if p.lastEchoAt.IsZero() {
		return
	}
	if now.Sub(p.lastEchoAt) < GraceIdleThreshold {
		return
	}
	if p.graceRemaining < GraceBootstrapSamples {
		p.graceRemaining = GraceBootstrapSamples
	}
}

// BeforeActiveWriteTotal returns the cumulative count of
// BeforeActiveWrite invocations. Phase 3 Step B3.6c — useful for
// confirming the adapter-write path is correctly invoking the
// pacer's pre-write hook. Safe to call from any goroutine.
func (p *Pacer) BeforeActiveWriteTotal() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.beforeActiveWriteTotal
}

// RecordEcho feeds an observed echo round-trip time into the
// L_rtt EMA. Per v8 §1.4 this is called ONLY in active mode
// (echoes of the gateway's own writes); passive-mode bytes do
// NOT contribute samples.
//
// Per Codex round-1 MEDIUM on PR #642: invalid samples (rtt <= 0)
// are REJECTED — neither the EMA, the lastEchoAt anchor, nor the
// recordedSamples counter is updated. A future call site that
// computes rtt from mismatched clock anchors would otherwise
// poison the EMA. Validation chosen over silent clamping so
// callers see the rejection in the recordedSamples counter (it
// won't increment).
//
// Grace bootstrap entry is now handled by BeforeActiveWrite (B3.5
// round-1 MAJOR fix). RecordEcho only decrements the grace
// counter as samples arrive, consuming the slots set by
// BeforeActiveWrite.
//
// During grace bootstrap, the EMA still updates normally; only
// the deadline computation differs (see EchoDeadlines).
func (p *Pacer) RecordEcho(rtt time.Duration, now time.Time) {
	if rtt <= 0 {
		// Invalid sample — reject. Documented contract: callers
		// must compute rtt from a single clock source with
		// observed write time predating echo time.
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	// Standard EMA update: new = α × sample + (1-α) × old.
	// Done in float64 to avoid integer-truncation drift over many
	// samples; result is rounded back to time.Duration.
	alpha := LrttEmaAlpha
	newEma := alpha*float64(rtt) + (1.0-alpha)*float64(p.lRttEMA)
	p.lRttEMA = time.Duration(newEma)

	p.lastEchoAt = now
	p.recordedSamples++

	// Decrement grace counter AFTER the EMA update so this
	// echo's sample counts as one of the bootstrap samples per
	// v8 §1.4 ("3 grace samples").
	if p.graceRemaining > 0 {
		p.graceRemaining--
	}
}

// Lrtt returns the current L_rtt EMA estimate. Safe to call from
// any goroutine.
func (p *Pacer) Lrtt() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lRttEMA
}

// InGraceBootstrap reports whether the pacer is currently in
// grace-bootstrap mode (next echo's deadline uses the loose
// grace slack rather than the normal slack). Safe to call from
// any goroutine.
func (p *Pacer) InGraceBootstrap() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.graceRemaining > 0
}

// RecordedSamples returns the cumulative count of RecordEcho
// calls since construction. Safe to call from any goroutine.
func (p *Pacer) RecordedSamples() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.recordedSamples
}

// EchoDeadlines returns the deadlines for the NEXT echo,
// expressed as durations relative to the write time. Per v8 §1.4:
//
//   - Grace mode (graceRemaining > 0):
//     soft        = L_rtt_EMA + 500ms
//     hard        = 0 (caller MUST ignore this value)
//     hardEnabled = false
//
//   - Normal mode (graceRemaining == 0):
//     soft        = L_rtt_EMA + 100ms
//     hard        = 2 × L_rtt_EMA + 200ms
//     hardEnabled = true
//
// Per Codex round-1 LOW on PR #642: the signature returns an
// explicit hardEnabled bool rather than relying on hard=0 as a
// "disabled" sentinel. This protects against the legitimate-zero
// edge case (e.g. a future configuration where L_rtt could land
// on zero would otherwise conflate "disabled" with "compute to
// zero"). Callers MUST consult hardEnabled before honoring hard.
//
// Safe to call from any goroutine.
func (p *Pacer) EchoDeadlines() (soft, hard time.Duration, hardEnabled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.graceRemaining > 0 {
		return p.lRttEMA + SoftDeadlineGrace, 0, false
	}
	return p.lRttEMA + SoftDeadlineNormal,
		2*p.lRttEMA + HardDeadlineNormalAdd,
		true
}

// ResetLrtt resets the L_rtt EMA to the bootstrap value, clears
// the lastEchoAt anchor, and resets grace state. Used on session
// reconnect or transport reset boundary. Pacing state
// (lastScheduledEmit) is preserved — the pacer remains in cadence
// across an L_rtt reset.
func (p *Pacer) ResetLrtt() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lRttEMA = LrttBootstrapInitial
	p.lastEchoAt = time.Time{}
	p.graceRemaining = 0
}

// SetAdminEventEmitter registers the callback invoked when the
// soft or hard echo deadline fires (Phase 3 Step B3.6e). nil
// disables admin-event emission — the counters still increment so
// tests / operators can verify the watchdog mechanics, but no
// ClassifierAdminEvent is recorded. Typically wired once at
// pacer construction by Classifier.NewPacerForSession.
//
// Safe to call from any goroutine. Subsequent calls overwrite the
// prior callback; setting nil disables emission immediately
// (already-armed timers fire with whatever emitter was current at
// fire time — re-arming via ArmEchoWatchdog atomically picks up
// the new emitter).
func (p *Pacer) SetAdminEventEmitter(f AdminEventEmitFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.adminEventEmit = f
}

// ArmEchoWatchdog arms the soft (and conditionally hard) echo
// deadline timers for the current outstanding write. Called by
// the gateway send path IMMEDIATELY after BeforeActiveWrite — the
// deadlines computed here use the (possibly grace-bootstrapped)
// L_rtt state that BeforeActiveWrite established. Per Codex
// round-1 MAJOR on PR #642 the grace-bootstrap entry MUST precede
// the watchdog arm; this method takes the current grace state at
// face value.
//
// Cancellation semantics (Codex round-1 MUST FIX #1 on PR #647):
//   - Any prior arm is cancelled via Stop(); the new timers are
//     created with their pointer captured in the fire closure so
//     stale fires from cancelled timers are no-op'd by an
//     identity check inside onSoftTimeout / onHardTimeout. This
//     closes the Stop()-returns-false race where the
//     already-running fire callback would otherwise clear a
//     newer arm's timer pointer.
//   - The Pacer tracks AT MOST ONE outstanding write's
//     watchdog; a second ArmEchoWatchdog before the prior echo
//     lands replaces the prior watchdog rather than stacking a
//     second one.
//
// The `now` parameter is currently unused; it is reserved as a
// future hook for emitting an arm-time admin event (Codex round-1
// LOW #2 on PR #647) and keeps the call-site signature consistent
// with BeforeActiveWrite / RecordEcho for diagnostic logging.
//
// Fire semantics:
//   - Soft timer fires after `soft` duration (always). Callback
//     identity-checks against p.softTimer, then increments
//     softTimeoutTotal and emits AdminEventKindEchoSoftTimeout via
//     the registered emitter.
//   - Hard timer fires after `hard` duration ONLY when grace mode
//     is inactive (hardEnabled == true). Callback identity-checks
//     against p.hardTimer, then increments hardTimeoutTotal and
//     emits AdminEventKindEchoHardTimeout.
//
// Per EchoDeadlines semantics: in grace mode the soft deadline is
// L_rtt_EMA + 500ms and the hard deadline is suppressed; in
// normal mode soft = L_rtt_EMA + 100ms, hard = 2×L_rtt_EMA + 200ms.
//
// Thread-safety: the arm path holds the Pacer mutex while
// stopping prior timers and starting new ones. The fire callback
// reacquires the mutex briefly to identity-check + bump counters
// + read the emitter; the emitter callback itself runs WITHOUT
// the mutex (so the emitter MAY safely re-enter Pacer methods).
func (p *Pacer) ArmEchoWatchdog(now time.Time) {
	_ = now // reserved for future arm-time diagnostics
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancelTimersLocked()
	var soft, hard time.Duration
	var hardEnabled bool
	if p.graceRemaining > 0 {
		soft = p.lRttEMA + SoftDeadlineGrace
		hardEnabled = false
	} else {
		soft = p.lRttEMA + SoftDeadlineNormal
		hard = 2*p.lRttEMA + HardDeadlineNormalAdd
		hardEnabled = true
	}
	// Codex round-1 MUST FIX #1 on PR #647: capture the
	// generation BY VALUE in each fire closure. cancelTimersLocked
	// already bumped armGeneration above, so `gen` here is the
	// post-bump value that the closure binds. Any stale fire
	// from a previously-cancelled timer carries the old
	// generation and gets no-op'd in onSoftTimeout /
	// onHardTimeout via the armGeneration mismatch check.
	gen := p.armGeneration
	p.softTimer = time.AfterFunc(soft, func() { p.onSoftTimeout(gen) })
	if hardEnabled {
		p.hardTimer = time.AfterFunc(hard, func() { p.onHardTimeout(gen) })
	}
}

// CancelEchoWatchdog cancels both watchdog timers without firing.
// Called by the mux when the matching echo lands AND when the
// session closes / write fails / arbitration is lost — any
// pre-completion path that ends an outstanding write needs to
// disarm the watchdog so a delayed timer fire after the abort
// doesn't emit a spurious admin event.
//
// Idempotent: cancelling already-stopped timers is safe. Safe to
// call from any goroutine.
func (p *Pacer) CancelEchoWatchdog() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancelTimersLocked()
}

// cancelTimersLocked is the lock-held helper used by both the
// public CancelEchoWatchdog and the ArmEchoWatchdog re-arm path.
// Stops both timers, nils the fields, AND bumps armGeneration so
// any timer callback that was already queued / running but
// hadn't yet acquired p.mu sees a generation mismatch and
// no-ops. Caller MUST hold p.mu.
//
// Codex round-1 MUST FIX #1 on PR #647: bumping the generation
// here (rather than only on Arm) gives CancelEchoWatchdog the
// same race-safety as ArmEchoWatchdog's re-arm.
func (p *Pacer) cancelTimersLocked() {
	if p.softTimer != nil {
		p.softTimer.Stop()
		p.softTimer = nil
	}
	if p.hardTimer != nil {
		p.hardTimer.Stop()
		p.hardTimer = nil
	}
	p.armGeneration++
}

// onSoftTimeout is the soft-timer fire callback. Increments the
// counter, captures the emitter under the mutex, then drops the
// mutex BEFORE invoking the emitter so the emitter callback may
// freely re-enter Pacer methods.
//
// Codex round-1 MUST FIX #1 on PR #647: takes `gen` (the
// generation value captured BY VALUE in the AfterFunc closure at
// arm time) and no-ops if it no longer matches p.armGeneration.
// Codex round-2 LOW #1 comment refresh: this is GENERATION
// checking, not pointer identity. The earlier pointer-identity
// approach was abandoned because the closure capturing `softT`
// by reference raced (race detector flagged the unsynchronized
// read in the timer-fire goroutine against the write in the
// arm goroutine). Generation values are captured by value into
// the closure — no shared mutable state.
//
// This guards against the Stop()-returns-false race: when
// ArmEchoWatchdog cancels a timer that has already fired (or is
// in the process of firing), the stale callback would otherwise
// (a) increment the counter for a write whose echo already
// landed and (b) clear the NEWER arm's timer pointer
// (`p.softTimer = nil`), breaking cancel semantics for the
// in-flight write. Generation-checking makes both effects
// impossible.
//
// Generation overflow is not practically reachable (2^64
// arm/cancel pairs takes centuries at any realistic rate).
// Codex round-2 confirmed not blocking.
func (p *Pacer) onSoftTimeout(gen uint64) {
	now := time.Now()
	// Codex round-2 MAJOR #1 on PR #647: test-only hook fires
	// BEFORE the lock acquisition so a test can deterministically
	// race a Cancel call against this fire callback. nil in
	// production; lock-free read is fine because tests serialize
	// the set vs the timer-fire arrival.
	if hook := p.onTimeoutEnterHook; hook != nil {
		hook()
	}
	p.mu.Lock()
	if p.armGeneration != gen {
		// Stale fire — this callback belonged to a previously
		// armed timer that has since been cancelled / replaced.
		// Discard silently; do NOT clear p.softTimer (it belongs
		// to a newer arm) and do NOT increment the counter.
		p.mu.Unlock()
		return
	}
	p.softTimeoutTotal++
	emit := p.adminEventEmit
	// Clear our own timer field so a subsequent
	// ArmEchoWatchdog's cancelTimersLocked correctly observes the
	// fired state.
	p.softTimer = nil
	p.mu.Unlock()
	if emit != nil {
		emit(AdminEventKindEchoSoftTimeout, now)
	}
}

// onHardTimeout is the hard-timer fire callback. Same locking +
// generation-check pattern as onSoftTimeout. Codex round-2 LOW
// #1: comment refresh — generation, not pointer identity.
func (p *Pacer) onHardTimeout(gen uint64) {
	now := time.Now()
	// Codex round-2 MAJOR #1 on PR #647: same test-only hook
	// pattern as onSoftTimeout. Production: nil; tests: optional
	// synchronization point.
	if hook := p.onTimeoutEnterHook; hook != nil {
		hook()
	}
	p.mu.Lock()
	if p.armGeneration != gen {
		p.mu.Unlock()
		return
	}
	p.hardTimeoutTotal++
	emit := p.adminEventEmit
	p.hardTimer = nil
	p.mu.Unlock()
	if emit != nil {
		emit(AdminEventKindEchoHardTimeout, now)
	}
}

// SoftTimeoutTotal returns the cumulative number of soft-deadline
// fires. Safe to call from any goroutine.
func (p *Pacer) SoftTimeoutTotal() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.softTimeoutTotal
}

// HardTimeoutTotal returns the cumulative number of hard-deadline
// fires. Safe to call from any goroutine.
func (p *Pacer) HardTimeoutTotal() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hardTimeoutTotal
}

// WatchdogArmed reports whether the echo watchdog currently has
// at least one timer outstanding. Used by tests to verify
// arm/cancel correctness without resorting to timing-based
// observations. Safe to call from any goroutine.
func (p *Pacer) WatchdogArmed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.softTimer != nil || p.hardTimer != nil
}
