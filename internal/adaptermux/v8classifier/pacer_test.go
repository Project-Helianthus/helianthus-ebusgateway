package v8classifier

import (
	"testing"
	"time"
)

// Phase 3 Step B3.5: tests for the per-session output Pacer and
// L_rtt EMA. The pacer is purely a computation layer in B3.5 —
// callers can invoke Schedule/RecordEcho/EchoDeadlines and inspect
// the results, but no production code yet acts on those decisions.
// B3.6 wires the pacer into session.go's emission path.

// Helper: t0 is the canonical synthetic-time anchor for tests.
var t0 = time.Unix(1_000_000_000, 0)

// TestPacer_NewPacer_DefaultState pins the constructor invariants.
func TestPacer_NewPacer_DefaultState(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	if got := p.Lrtt(); got != LrttBootstrapInitial {
		t.Errorf("Lrtt() at construction = %v; want %v (bootstrap)", got, LrttBootstrapInitial)
	}
	if p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=true at construction; want false (not entered yet)")
	}
	if got := p.RecordedSamples(); got != 0 {
		t.Errorf("RecordedSamples()=%d; want 0 at construction", got)
	}
	if !p.LastScheduledEmit().IsZero() {
		t.Error("LastScheduledEmit()=non-zero at construction; want zero")
	}
}

// TestPacer_Schedule_FirstCallEmitsImmediately pins that the very
// first byte after construction emits at `now` without retroactive
// padding (no fictitious "previous emit at -∞" anchor).
func TestPacer_Schedule_FirstCallEmitsImmediately(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	emit := p.Schedule(t0)
	if !emit.Equal(t0) {
		t.Errorf("first Schedule(t0) = %v; want t0 (%v)", emit, t0)
	}
}

// TestPacer_Schedule_SubsequentCallEnforcesCadence pins that the
// second byte is scheduled at first + τ when the caller's `now` is
// within τ of the first emit (caller is "ahead" of cadence).
func TestPacer_Schedule_SubsequentCallEnforcesCadence(t *testing.T) {
	t.Parallel()
	p := NewPacer()

	first := p.Schedule(t0)
	// Caller calls Schedule again 1 ms later (within τ=4.17ms);
	// pacer must schedule the second byte at first + τ to enforce
	// cadence.
	tooEarly := t0.Add(1 * time.Millisecond)
	second := p.Schedule(tooEarly)
	want := first.Add(TauWireByte)
	if !second.Equal(want) {
		t.Errorf("second Schedule = %v; want %v (first + τ_wire_byte)", second, want)
	}
}

// TestPacer_Schedule_CallerLateAdvancesCadence pins that if the
// caller is LATE (now > last + τ), the pacer schedules at `now`
// and the new lastScheduledEmit is `now` — the cadence advances
// rather than back-filling.
func TestPacer_Schedule_CallerLateAdvancesCadence(t *testing.T) {
	t.Parallel()
	p := NewPacer()

	p.Schedule(t0)
	late := t0.Add(100 * time.Millisecond) // way past τ
	emit := p.Schedule(late)
	if !emit.Equal(late) {
		t.Errorf("late Schedule = %v; want %v (late time)", emit, late)
	}
	if got := p.LastScheduledEmit(); !got.Equal(late) {
		t.Errorf("LastScheduledEmit()=%v; want %v after late call", got, late)
	}
}

// TestPacer_Schedule_BurstSequence pins the canonical pattern: a
// 6-byte burst arriving "instantly" (all calls with the same
// `now`) gets spaced at τ intervals.
func TestPacer_Schedule_BurstSequence(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	const n = 6
	emits := make([]time.Time, n)
	for i := 0; i < n; i++ {
		emits[i] = p.Schedule(t0)
	}
	// emits[0] = t0; emits[i] = t0 + i*τ.
	for i := 0; i < n; i++ {
		want := t0.Add(time.Duration(i) * TauWireByte)
		if !emits[i].Equal(want) {
			t.Errorf("emit[%d] = %v; want %v", i, emits[i], want)
		}
	}
}

// TestPacer_RecordEcho_EMAConvergence pins that repeated identical
// samples converge the EMA toward the sample value.
func TestPacer_RecordEcho_EMAConvergence(t *testing.T) {
	t.Parallel()
	p := NewPacer()

	const sample = 50 * time.Millisecond
	// 100 samples should converge the EMA far below the initial
	// 100ms toward the sample (50ms) — α=0.3 means after N
	// samples, the residual error decays as (0.7)^N.
	for i := 0; i < 100; i++ {
		p.RecordEcho(sample, t0.Add(time.Duration(i)*100*time.Millisecond))
	}
	// After 100 samples of 50ms with bootstrap 100ms, the EMA
	// should be very close to 50ms.
	got := p.Lrtt()
	if got > 55*time.Millisecond || got < 45*time.Millisecond {
		t.Errorf("Lrtt() after 100×50ms samples = %v; want ~50ms (converged)", got)
	}
}

// TestPacer_RecordEcho_FirstSampleUpdatesEMA pins the first-sample
// behavior: with bootstrap=100ms and α=0.3, a 50ms sample produces
// EMA = 0.3 × 50 + 0.7 × 100 = 85ms.
func TestPacer_RecordEcho_FirstSampleUpdatesEMA(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	p.RecordEcho(50*time.Millisecond, t0)
	got := p.Lrtt()
	// 0.3 × 50 + 0.7 × 100 = 15 + 70 = 85 ms (allow ±1 ms float
	// drift).
	want := 85 * time.Millisecond
	delta := got - want
	if delta < -1*time.Millisecond || delta > 1*time.Millisecond {
		t.Errorf("Lrtt() after first sample = %v; want %v (±1ms)", got, want)
	}
	if got := p.RecordedSamples(); got != 1 {
		t.Errorf("RecordedSamples()=%d; want 1", got)
	}
}

// TestPacer_GraceBootstrap_TriggeredByBeforeActiveWrite pins the
// v8 §1.4 long-idle trigger via the new BeforeActiveWrite path
// (Codex round-1 MAJOR fix): a write call after >=30s idle from
// the last echo sets graceRemaining = GraceBootstrapSamples.
// Critically, this fires BEFORE the first post-idle echo, so
// EchoDeadlines() called between BeforeActiveWrite and the first
// RecordEcho already returns grace deadlines.
func TestPacer_GraceBootstrap_TriggeredByBeforeActiveWrite(t *testing.T) {
	t.Parallel()
	p := NewPacer()

	// First sample establishes lastEchoAt.
	p.RecordEcho(50*time.Millisecond, t0)
	if p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=true after first sample; want false (no prior idle)")
	}

	// Long-idle write: 31s later, call BeforeActiveWrite BEFORE
	// any new echo arrives. This is the critical fix for the
	// round-1 MAJOR — the FIRST post-idle echo's deadline must
	// already be in grace mode at the moment the watchdog arms.
	tLate := t0.Add(31 * time.Second)
	p.BeforeActiveWrite(tLate)

	if !p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=false after BeforeActiveWrite + long idle; want true")
	}
	_, _, hardEnabled := p.EchoDeadlines()
	if hardEnabled {
		t.Error("hardEnabled=true after BeforeActiveWrite; want false (grace must protect FIRST post-idle echo)")
	}

	// Three samples consume grace: 3 → 2 → 1 → 0.
	p.RecordEcho(50*time.Millisecond, tLate)
	if !p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=false after 1st grace echo; want true")
	}
	p.RecordEcho(50*time.Millisecond, tLate.Add(100*time.Millisecond))
	if !p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=false after 2nd grace echo; want true")
	}
	p.RecordEcho(50*time.Millisecond, tLate.Add(200*time.Millisecond))
	if p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=true after 3rd grace echo; want false (grace exhausted)")
	}
}

// TestPacer_GraceBootstrap_NotTriggeredByShortIdle pins the
// boundary: BeforeActiveWrite with idle < 30s does NOT enter grace.
func TestPacer_GraceBootstrap_NotTriggeredByShortIdle(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	p.RecordEcho(50*time.Millisecond, t0)
	// 29 seconds — below the 30s threshold.
	p.BeforeActiveWrite(t0.Add(29 * time.Second))
	if p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=true after 29s idle; want false")
	}
}

// TestPacer_GraceBootstrap_BoundaryExactly30s pins the >=
// boundary: exactly 30 s of idle DOES trigger grace.
func TestPacer_GraceBootstrap_BoundaryExactly30s(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	p.RecordEcho(50*time.Millisecond, t0)
	// Exactly 30 seconds — at the >= boundary.
	p.BeforeActiveWrite(t0.Add(GraceIdleThreshold))
	if !p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=false at exactly 30s idle; want true (>= boundary)")
	}
}

// TestPacer_BeforeActiveWrite_NoPriorHistoryIsNoOp pins that
// calling BeforeActiveWrite on a fresh pacer (no prior
// RecordEcho) is a no-op — the construction state already
// represents bootstrap, no need to re-enter grace.
func TestPacer_BeforeActiveWrite_NoPriorHistoryIsNoOp(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	p.BeforeActiveWrite(t0)
	if p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=true on fresh pacer after BeforeActiveWrite; want false (no prior history)")
	}
}

// TestPacer_BeforeActiveWrite_Idempotent pins that calling
// BeforeActiveWrite twice (before any RecordEcho) does not
// double-arm grace beyond GraceBootstrapSamples.
func TestPacer_BeforeActiveWrite_Idempotent(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	p.RecordEcho(50*time.Millisecond, t0)
	p.BeforeActiveWrite(t0.Add(31 * time.Second))
	p.BeforeActiveWrite(t0.Add(32 * time.Second))
	// Both calls should leave graceRemaining at exactly
	// GraceBootstrapSamples (not 2× that).
	for i := 0; i < GraceBootstrapSamples; i++ {
		if !p.InGraceBootstrap() {
			t.Errorf("iter %d: InGraceBootstrap()=false; want true", i)
		}
		p.RecordEcho(50*time.Millisecond, t0.Add(time.Duration(32+i)*time.Second))
	}
	if p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=true after consuming all grace samples; want false (idempotent — not double-armed)")
	}
}

// TestPacer_EchoDeadlines_NormalMode pins the v8 §1.4 formula:
// soft = L_rtt + 100ms, hard = 2×L_rtt + 200ms, hardEnabled = true.
func TestPacer_EchoDeadlines_NormalMode(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	// At construction: L_rtt = 100ms, no grace.
	soft, hard, hardEnabled := p.EchoDeadlines()
	wantSoft := 100*time.Millisecond + SoftDeadlineNormal
	wantHard := 2*100*time.Millisecond + HardDeadlineNormalAdd
	if soft != wantSoft {
		t.Errorf("soft=%v; want %v", soft, wantSoft)
	}
	if hard != wantHard {
		t.Errorf("hard=%v; want %v", hard, wantHard)
	}
	if !hardEnabled {
		t.Error("hardEnabled=false in normal mode; want true")
	}
}

// TestPacer_EchoDeadlines_GraceMode pins that during grace, soft
// uses the loose 500ms slack, hardEnabled is false (so the hard
// value MUST be ignored by callers).
func TestPacer_EchoDeadlines_GraceMode(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	// Trigger grace via BeforeActiveWrite after long idle.
	p.RecordEcho(50*time.Millisecond, t0)
	p.BeforeActiveWrite(t0.Add(31 * time.Second))
	if !p.InGraceBootstrap() {
		t.Fatal("precondition: should be in grace after BeforeActiveWrite + long idle")
	}
	soft, hard, hardEnabled := p.EchoDeadlines()
	// L_rtt should be ~85ms after one 50ms sample (100→85).
	if hardEnabled {
		t.Errorf("hardEnabled=true in grace mode; want false")
	}
	// hard value MUST be 0 when disabled (the contract):
	if hard != 0 {
		t.Errorf("hard=%v in grace mode; want 0", hard)
	}
	if soft <= 500*time.Millisecond {
		t.Errorf("soft=%v in grace mode; want > 500ms (L_rtt + 500ms slack)", soft)
	}
}

// TestPacer_ResetLrtt pins ResetLrtt clears the EMA, idle anchor,
// and grace state — but does NOT touch the pacing state.
func TestPacer_ResetLrtt(t *testing.T) {
	t.Parallel()
	p := NewPacer()

	// Build up state.
	p.RecordEcho(50*time.Millisecond, t0)
	p.BeforeActiveWrite(t0.Add(31 * time.Second))
	scheduledBefore := p.Schedule(t0.Add(31 * time.Second))
	if !p.InGraceBootstrap() {
		t.Fatal("precondition: should be in grace")
	}
	if p.Lrtt() == LrttBootstrapInitial {
		t.Fatal("precondition: Lrtt should have moved off bootstrap")
	}

	p.ResetLrtt()

	if got := p.Lrtt(); got != LrttBootstrapInitial {
		t.Errorf("after ResetLrtt: Lrtt()=%v; want %v (bootstrap)", got, LrttBootstrapInitial)
	}
	if p.InGraceBootstrap() {
		t.Error("after ResetLrtt: InGraceBootstrap()=true; want false")
	}
	// Pacing state preserved.
	if got := p.LastScheduledEmit(); !got.Equal(scheduledBefore) {
		t.Errorf("after ResetLrtt: LastScheduledEmit()=%v; want %v (pacing preserved)", got, scheduledBefore)
	}
}

// TestPacer_TauOverride pins SetTau and that subsequent Schedule
// calls use the new τ. Per Codex round-1 fix on PR #642: SetTau
// returns bool indicating accepted/rejected. Valid value → true.
func TestPacer_TauOverride(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	const customTau = 10 * time.Millisecond
	if !p.SetTau(customTau) {
		t.Fatal("SetTau(10ms) returned false; want true (valid value)")
	}

	p.Schedule(t0)
	emit := p.Schedule(t0)
	want := t0.Add(customTau)
	if !emit.Equal(want) {
		t.Errorf("Schedule with custom τ = %v; want %v", emit, want)
	}
}

// TestPacer_ConcurrentRead pins that the accessors (Lrtt,
// InGraceBootstrap, RecordedSamples, LastScheduledEmit,
// EchoDeadlines) are safe to call from another goroutine while a
// single producer is calling Schedule/RecordEcho.
//
// The mutex inside Pacer makes this race-free; the test fires
// under -race to prove it.
func TestPacer_ConcurrentRead(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	stop := make(chan struct{})
	defer close(stop)

	// Reader goroutine.
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = p.Lrtt()
				_ = p.InGraceBootstrap()
				_ = p.RecordedSamples()
				_ = p.LastScheduledEmit()
				_, _, _ = p.EchoDeadlines()
			}
		}
	}()

	// Producer: 1000 Schedule + interleaved RecordEcho calls.
	now := t0
	for i := 0; i < 1000; i++ {
		p.Schedule(now)
		if i%5 == 0 {
			p.RecordEcho(time.Duration(40+i)*time.Millisecond, now)
		}
		now = now.Add(1 * time.Millisecond)
	}
	if got := p.RecordedSamples(); got != 200 {
		t.Errorf("RecordedSamples()=%d; want 200 (1000/5)", got)
	}
}

// TestPacer_RecordEcho_RejectsInvalidRtt pins the Codex round-1
// MEDIUM fix: rtt <= 0 is rejected without updating the EMA, the
// lastEchoAt anchor, or the recordedSamples counter. A future
// caller computing rtt from mismatched clocks otherwise poisons
// the EMA.
func TestPacer_RecordEcho_RejectsInvalidRtt(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	lrttBefore := p.Lrtt()

	// Negative rtt — reject.
	p.RecordEcho(-1*time.Millisecond, t0)
	if got := p.Lrtt(); got != lrttBefore {
		t.Errorf("Lrtt()=%v after negative RTT; want unchanged %v", got, lrttBefore)
	}
	if got := p.RecordedSamples(); got != 0 {
		t.Errorf("RecordedSamples()=%d after negative RTT; want 0 (rejected)", got)
	}

	// Zero rtt — also reject.
	p.RecordEcho(0, t0)
	if got := p.Lrtt(); got != lrttBefore {
		t.Errorf("Lrtt()=%v after zero RTT; want unchanged %v", got, lrttBefore)
	}
	if got := p.RecordedSamples(); got != 0 {
		t.Errorf("RecordedSamples()=%d after zero RTT; want 0 (rejected)", got)
	}

	// Positive rtt — accept.
	p.RecordEcho(50*time.Millisecond, t0)
	if got := p.Lrtt(); got == lrttBefore {
		t.Errorf("Lrtt()=%v after valid RTT; want changed", got)
	}
	if got := p.RecordedSamples(); got != 1 {
		t.Errorf("RecordedSamples()=%d after valid RTT; want 1", got)
	}
}

// TestPacer_SetTau_RejectsInvalid pins the Codex round-1 MEDIUM
// fix: zero or negative tau is rejected; the prior tau is
// preserved. The bool return reports rejection.
func TestPacer_SetTau_RejectsInvalid(t *testing.T) {
	t.Parallel()
	p := NewPacer()

	// Establish a known custom tau.
	if !p.SetTau(5 * time.Millisecond) {
		t.Fatal("SetTau(5ms) rejected; precondition failed")
	}

	// Now try to set zero — must reject.
	if p.SetTau(0) {
		t.Error("SetTau(0) returned true; want false (rejected)")
	}
	// And negative — must reject.
	if p.SetTau(-1 * time.Second) {
		t.Error("SetTau(-1s) returned true; want false (rejected)")
	}

	// The previously-set tau must be preserved. Verify by
	// observing Schedule behavior.
	p.Schedule(t0)
	emit := p.Schedule(t0)
	want := t0.Add(5 * time.Millisecond)
	if !emit.Equal(want) {
		t.Errorf("Schedule after rejected SetTau = %v; want %v (prior 5ms preserved)", emit, want)
	}
}

// TestPacer_Schedule_ZeroNow_NoSentinelLeak pins the Codex
// round-1 LOW fix: passing time.Time{} (zero) as `now` does NOT
// re-trigger the "first call" branch on the next Schedule, even
// though lastScheduledEmit is now zero. The explicit
// scheduledInitialized bool removes the zero-time sentinel leak.
func TestPacer_Schedule_ZeroNow_NoSentinelLeak(t *testing.T) {
	t.Parallel()
	p := NewPacer()

	// First call with zero now — accepted, emits at zero.
	emit1 := p.Schedule(time.Time{})
	if !emit1.IsZero() {
		t.Errorf("first Schedule(zero) = %v; want zero", emit1)
	}

	// Second call with t0 — must NOT re-trigger the "first call"
	// branch (would return t0 immediately); MUST enforce the τ
	// cadence from the prior zero-time emit.
	//
	// emit2 = max(t0, zero + τ_wire_byte) = t0 if t0 > τ from
	// zero (which it is, since t0 is far past the epoch).
	emit2 := p.Schedule(t0)
	want := t0
	if !emit2.Equal(want) {
		t.Errorf("Schedule after zero-now: %v; want %v (cadence enforced)", emit2, want)
	}

	// Third call with t0 — now must enforce cadence vs emit2.
	emit3 := p.Schedule(t0)
	want3 := emit2.Add(TauWireByte)
	if !emit3.Equal(want3) {
		t.Errorf("third Schedule(t0): %v; want %v", emit3, want3)
	}
}
