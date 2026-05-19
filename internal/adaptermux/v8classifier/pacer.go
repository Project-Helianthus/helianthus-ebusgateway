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
// into actual session emission. Schedule and RecordEcho can be
// called by tests and (eventually) by session.go; they return
// timing decisions without applying them. B3.6 will integrate the
// pacer into session.go's sendCh draining path so the timing
// decisions actually affect emission.
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
	//
	// Zero value (time.Time{}) means "no byte scheduled yet";
	// the first Schedule call emits immediately (no
	// retroactive padding to bootstrap a cadence the caller
	// can't observe).
	lastScheduledEmit time.Time

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
}

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
func (p *Pacer) SetTau(tau time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tau = tau
}

// Schedule reports when the next byte should be emitted given the
// current wall-clock time `now`. The returned time is
// max(now, lastScheduledEmit + tau), and lastScheduledEmit is
// advanced to the returned value.
//
// Caller usage: a session writer goroutine that has a buffered
// byte ready calls Schedule(time.Now()), then sleeps until the
// returned time before pushing the byte to the client TCP egress.
//
// PR B3.5 scope: this method is callable but the session writer
// is NOT yet wired to use it. B3.6 integrates Schedule into
// session.go's sendCh draining.
func (p *Pacer) Schedule(now time.Time) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	var emitAt time.Time
	if p.lastScheduledEmit.IsZero() {
		// First call — emit immediately. No retroactive padding.
		emitAt = now
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

// RecordEcho feeds an observed echo round-trip time into the
// L_rtt EMA. Per v8 §1.4 this is called ONLY in active mode
// (echoes of the gateway's own writes); passive-mode bytes do
// NOT contribute samples.
//
// The first call after long idle (now - lastEchoAt >=
// GraceIdleThreshold) sets graceRemaining =
// GraceBootstrapSamples, putting the next GraceBootstrapSamples
// echoes into grace-bootstrap mode.
//
// During grace bootstrap, the EMA still updates normally; only
// the deadline computation differs (see EchoDeadlines).
func (p *Pacer) RecordEcho(rtt time.Duration, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Long-idle detection: if we have prior echo history AND the
	// gap exceeds the grace threshold, enter grace bootstrap.
	if !p.lastEchoAt.IsZero() && now.Sub(p.lastEchoAt) >= GraceIdleThreshold {
		p.graceRemaining = GraceBootstrapSamples
	}

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

// EchoDeadlines returns the (soft, hard) deadlines for the NEXT
// echo, expressed as durations relative to the write time. Per
// v8 §1.4:
//
//   - Grace mode (graceRemaining > 0):
//     soft = L_rtt_EMA + 500ms
//     hard = 0 (disabled — caller MUST NOT enforce a hard
//     timeout while grace is active)
//
//   - Normal mode (graceRemaining == 0):
//     soft = L_rtt_EMA + 100ms
//     hard = 2 × L_rtt_EMA + 200ms
//
// Callers SHOULD treat hard=0 as "no hard deadline; rely on
// transport-layer timeouts". B3.6 will wire these into the
// session's echo-watchdog timer.
//
// Safe to call from any goroutine.
func (p *Pacer) EchoDeadlines() (soft, hard time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.graceRemaining > 0 {
		return p.lRttEMA + SoftDeadlineGrace, 0
	}
	return p.lRttEMA + SoftDeadlineNormal,
		2*p.lRttEMA + HardDeadlineNormalAdd
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
