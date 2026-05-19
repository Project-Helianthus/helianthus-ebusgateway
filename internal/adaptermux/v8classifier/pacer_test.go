package v8classifier

import (
	"sync"
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

// TestPacer_BeforeActiveWriteTotal pins the Phase 3 Step B3.6c
// counter — every BeforeActiveWrite call increments, regardless
// of whether the call triggers grace bootstrap.
func TestPacer_BeforeActiveWriteTotal(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	if got := p.BeforeActiveWriteTotal(); got != 0 {
		t.Errorf("BeforeActiveWriteTotal() at construction = %d; want 0", got)
	}

	// 5 BeforeActiveWrite calls (no prior history → no grace
	// trigger, just counter increments).
	for i := 0; i < 5; i++ {
		p.BeforeActiveWrite(t0.Add(time.Duration(i) * time.Second))
	}
	if got := p.BeforeActiveWriteTotal(); got != 5 {
		t.Errorf("BeforeActiveWriteTotal() = %d; want 5", got)
	}

	// Verify the counter increments even when grace fires (long
	// idle from a prior recorded sample).
	p.RecordEcho(50*time.Millisecond, t0)
	p.BeforeActiveWrite(t0.Add(31 * time.Second))
	if got := p.BeforeActiveWriteTotal(); got != 6 {
		t.Errorf("BeforeActiveWriteTotal() after grace fire = %d; want 6", got)
	}
	if !p.InGraceBootstrap() {
		t.Error("grace not entered after long-idle BeforeActiveWrite")
	}
}

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

// =============================================================
// Phase 3 Step B3.6e — echo watchdog tests
// =============================================================
//
// These tests exercise the soft/hard deadline timer fire path
// against a Pacer with an artificially small L_rtt EMA so the
// timers fire fast enough to validate the wiring without slowing
// the test suite. The default L_rtt bootstrap is 100ms; to get
// sub-100ms soft deadlines we use SetTau (no — tau doesn't affect
// deadlines) — instead we feed RecordEcho samples that pull the
// EMA down to a small value, OR we accept the ~100ms wait for
// the default bootstrap to fire.
//
// For tests we shrink the deadlines by feeding a tiny RecordEcho
// sample (e.g. 1ms) which collapses the EMA quickly. Soft becomes
// ~100ms+1ms*α (still ~100ms) — so we use a different approach:
// run with the default L_rtt and just wait ~120ms for soft. Hard
// is 2*L_rtt + 200ms = ~400ms. Tests must tolerate this latency.

// TestPacer_Watchdog_NotArmedAtConstruction pins the production-
// default state: a freshly-constructed Pacer has no watchdog
// armed. WatchdogArmed() is false; SoftTimeoutTotal /
// HardTimeoutTotal are zero.
func TestPacer_Watchdog_NotArmedAtConstruction(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	if p.WatchdogArmed() {
		t.Error("WatchdogArmed() = true at construction; want false")
	}
	if got := p.SoftTimeoutTotal(); got != 0 {
		t.Errorf("SoftTimeoutTotal() = %d at construction; want 0", got)
	}
	if got := p.HardTimeoutTotal(); got != 0 {
		t.Errorf("HardTimeoutTotal() = %d at construction; want 0", got)
	}
}

// TestPacer_ArmEchoWatchdog_ArmsTimers pins the basic arm
// contract: after ArmEchoWatchdog, WatchdogArmed reports true.
// Then CancelEchoWatchdog clears the state.
func TestPacer_ArmEchoWatchdog_ArmsTimers(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	p.ArmEchoWatchdog(t0)
	if !p.WatchdogArmed() {
		t.Error("WatchdogArmed() = false after Arm; want true")
	}
	p.CancelEchoWatchdog()
	if p.WatchdogArmed() {
		t.Error("WatchdogArmed() = true after Cancel; want false")
	}
}

// TestPacer_CancelEchoWatchdog_BeforeFire pins that a cancel
// before the timer fires prevents the soft/hard counters from
// incrementing and prevents any emitter callback.
func TestPacer_CancelEchoWatchdog_BeforeFire(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	var emittedKinds []AdminEventKind
	var mu emitMutex
	p.SetAdminEventEmitter(func(kind AdminEventKind, at time.Time) {
		mu.Lock()
		emittedKinds = append(emittedKinds, kind)
		mu.Unlock()
	})

	p.ArmEchoWatchdog(time.Now())
	// Cancel immediately, well before the soft deadline (~100ms).
	p.CancelEchoWatchdog()
	// Wait past the would-be soft deadline to confirm no fire.
	time.Sleep(150 * time.Millisecond)

	if got := p.SoftTimeoutTotal(); got != 0 {
		t.Errorf("SoftTimeoutTotal() after cancel = %d; want 0", got)
	}
	if got := p.HardTimeoutTotal(); got != 0 {
		t.Errorf("HardTimeoutTotal() after cancel = %d; want 0", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(emittedKinds) != 0 {
		t.Errorf("emittedKinds after cancel = %v; want empty", emittedKinds)
	}
}

// TestPacer_SoftDeadlineFires pins that the soft timer fires
// after the configured deadline AND emits a soft-timeout admin
// event. Default L_rtt bootstrap = 100ms, soft normal = +100ms
// → ~200ms total. Cancel the hard timer immediately after the
// soft fires (Codex round-1 MEDIUM #2 on PR #647: do not let
// the hard timer fire here — it would race the per-test deadline
// on slow CI and pollute the assertion).
func TestPacer_SoftDeadlineFires(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	var emitted []AdminEventKind
	var mu emitMutex
	p.SetAdminEventEmitter(func(kind AdminEventKind, at time.Time) {
		mu.Lock()
		emitted = append(emitted, kind)
		mu.Unlock()
	})

	p.ArmEchoWatchdog(time.Now())
	// Codex round-1 MEDIUM #2 on PR #647: poll the counter
	// instead of sleeping a fixed duration. Robust on slow CI.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.SoftTimeoutTotal() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := p.SoftTimeoutTotal(); got != 1 {
		t.Errorf("SoftTimeoutTotal() = %d after soft window; want 1", got)
	}
	// Cancel the hard timer before it can fire — keeps the
	// assertion below honest regardless of CI scheduling.
	p.CancelEchoWatchdog()
	if got := p.HardTimeoutTotal(); got != 0 {
		t.Errorf("HardTimeoutTotal() = %d after cancel; want 0", got)
	}
	mu.Lock()
	gotKinds := append([]AdminEventKind{}, emitted...)
	mu.Unlock()
	if len(gotKinds) != 1 || gotKinds[0] != AdminEventKindEchoSoftTimeout {
		t.Errorf("emitted kinds = %v; want [EchoSoftTimeout]", gotKinds)
	}
}

// TestPacer_HardDeadlineFires pins that the hard timer fires
// after the configured deadline AND emits a hard-timeout admin
// event. Soft fires first, then hard. Codex round-1 MEDIUM #2 on
// PR #647: poll instead of fixed-sleep.
func TestPacer_HardDeadlineFires(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	var emitted []AdminEventKind
	var mu emitMutex
	p.SetAdminEventEmitter(func(kind AdminEventKind, at time.Time) {
		mu.Lock()
		emitted = append(emitted, kind)
		mu.Unlock()
	})

	p.ArmEchoWatchdog(time.Now())
	// Poll the hard counter. L_rtt=100ms, hard=2*100+200=400ms;
	// generous 2s deadline tolerates slow CI.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.HardTimeoutTotal() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := p.SoftTimeoutTotal(); got != 1 {
		t.Errorf("SoftTimeoutTotal() = %d after hard window; want 1", got)
	}
	if got := p.HardTimeoutTotal(); got != 1 {
		t.Errorf("HardTimeoutTotal() = %d after hard window; want 1", got)
	}
	mu.Lock()
	gotKinds := append([]AdminEventKind{}, emitted...)
	mu.Unlock()
	if len(gotKinds) != 2 {
		t.Fatalf("emitted kinds = %v; want 2 (soft then hard)", gotKinds)
	}
	if gotKinds[0] != AdminEventKindEchoSoftTimeout {
		t.Errorf("emitted[0] = %v; want EchoSoftTimeout", gotKinds[0])
	}
	if gotKinds[1] != AdminEventKindEchoHardTimeout {
		t.Errorf("emitted[1] = %v; want EchoHardTimeout", gotKinds[1])
	}
}

// TestPacer_GraceMode_HardTimerNotArmed pins the grace-mode
// invariant: when graceRemaining > 0, EchoDeadlines reports
// hardEnabled=false, and ArmEchoWatchdog must NOT arm the hard
// timer. Even if we wait past where hard would have fired, only
// soft fires.
func TestPacer_GraceMode_HardTimerNotArmed(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	// Trigger grace by simulating long-idle BeforeActiveWrite.
	// First seed a prior echo so lastEchoAt is non-zero.
	p.RecordEcho(50*time.Millisecond, t0)
	// Now call BeforeActiveWrite far in the future (>= 30s) to
	// trip grace.
	p.BeforeActiveWrite(t0.Add(60 * time.Second))

	var emitted []AdminEventKind
	var mu emitMutex
	p.SetAdminEventEmitter(func(kind AdminEventKind, at time.Time) {
		mu.Lock()
		emitted = append(emitted, kind)
		mu.Unlock()
	})

	p.ArmEchoWatchdog(time.Now())
	// Codex round-1 MEDIUM #2 on PR #647: poll the soft counter
	// (grace soft = L_rtt + 500ms ≈ 550ms). 3s deadline tolerates
	// slow CI; if hard were erroneously armed at 400ms we'd see
	// it long before this deadline.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.SoftTimeoutTotal() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := p.HardTimeoutTotal(); got != 0 {
		t.Errorf("HardTimeoutTotal() = %d in grace mode; want 0 (hard timer must not be armed)", got)
	}
	if got := p.SoftTimeoutTotal(); got != 1 {
		t.Errorf("SoftTimeoutTotal() = %d in grace mode; want 1 (soft must still fire)", got)
	}
	mu.Lock()
	gotKinds := append([]AdminEventKind{}, emitted...)
	mu.Unlock()
	if len(gotKinds) != 1 || gotKinds[0] != AdminEventKindEchoSoftTimeout {
		t.Errorf("emitted kinds = %v; want [EchoSoftTimeout] only", gotKinds)
	}
}

// TestPacer_ArmEchoWatchdog_ReplacesPriorArm pins that a second
// Arm before the first echo cancels the first arm's timers — at
// most ONE outstanding watchdog per pacer. The first arm's soft
// deadline must NOT fire if the second arm replaces it before
// that deadline.
func TestPacer_ArmEchoWatchdog_ReplacesPriorArm(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	p.SetAdminEventEmitter(func(kind AdminEventKind, at time.Time) {})

	p.ArmEchoWatchdog(time.Now())
	// Re-arm well before soft deadline (~200ms).
	time.Sleep(20 * time.Millisecond)
	p.ArmEchoWatchdog(time.Now())
	// Cancel before the second arm's deadline too.
	time.Sleep(20 * time.Millisecond)
	p.CancelEchoWatchdog()
	// Wait past where either arm could fire.
	time.Sleep(300 * time.Millisecond)

	if got := p.SoftTimeoutTotal(); got != 0 {
		t.Errorf("SoftTimeoutTotal() after re-arm + cancel = %d; want 0", got)
	}
}

// TestPacer_ArmEchoWatchdog_StaleFireRaceNoOp pins the Codex
// round-1 MUST FIX #1 invariant: when ArmEchoWatchdog cancels a
// timer whose Stop() returns false (fired or firing), the
// already-queued fire callback must NOT increment counters and
// must NOT clear the NEWER arm's timer pointer.
//
// To exercise the Stop()-returns-false path we use the
// short-lived pacer pattern: arm with the default L_rtt (soft
// ~200ms), let the soft timer FIRE, then re-arm. The fired
// callback for the first arm has finished (incremented counter
// once). A subsequent re-arm should NOT see the first arm's
// callback continue to clear the new timer. We then cancel the
// second arm and confirm the counter stayed at 1 (only the
// first arm fired) — proving the second arm's timer was clean
// and not cleared by a stale fire.
func TestPacer_ArmEchoWatchdog_StaleFireRaceNoOp(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	p.SetAdminEventEmitter(func(kind AdminEventKind, at time.Time) {})

	// First arm — let it fire its soft deadline.
	p.ArmEchoWatchdog(time.Now())
	// Poll for the fire instead of sleeping a fixed duration —
	// robust on slow CI.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.SoftTimeoutTotal() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := p.SoftTimeoutTotal(); got != 1 {
		t.Fatalf("first arm SoftTimeoutTotal = %d; want 1 (precondition)", got)
	}

	// Second arm — the first arm's timer has already fired and
	// completed, so its callback is in some state that cannot
	// affect this arm IF the generation-bump pattern works.
	p.ArmEchoWatchdog(time.Now())
	if !p.WatchdogArmed() {
		t.Fatal("WatchdogArmed=false after second arm; the first arm's stale callback must have cleared it (regression of MUST FIX #1)")
	}

	// Cancel cleanly. The second arm should not have fired.
	p.CancelEchoWatchdog()
	if got := p.SoftTimeoutTotal(); got != 1 {
		t.Errorf("SoftTimeoutTotal after re-arm + cancel = %d; want 1 (second arm must NOT fire and stale first-arm callback must NOT re-increment)", got)
	}
}

// TestPacer_EmitterMayReenterPacer pins the Codex round-1 LOW #1
// contract refinement: the emitter callback runs AFTER the
// Pacer mutex is dropped, so it MAY safely re-enter Pacer
// methods. We verify by having the emitter call WatchdogArmed
// and SoftTimeoutTotal from inside the callback — neither
// would return without deadlock if the contract were broken.
func TestPacer_EmitterMayReenterPacer(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	var mu sync.Mutex
	var observedSoftTotal uint64
	var firstFire sync.Once
	firstSeen := make(chan struct{})
	p.SetAdminEventEmitter(func(kind AdminEventKind, at time.Time) {
		// Re-enter the pacer from the emitter callback. If the
		// pacer mutex were still held at this point this would
		// deadlock and the test would time out.
		armed := p.WatchdogArmed()
		soft := p.SoftTimeoutTotal()
		mu.Lock()
		observedSoftTotal = soft
		mu.Unlock()
		_ = armed
		firstFire.Do(func() { close(firstSeen) })
	})

	p.ArmEchoWatchdog(time.Now())
	select {
	case <-firstSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("emitter callback did not complete within 2s — contract broken (mutex held during emit, or no fire)")
	}
	// Cancel any remaining timers so the hard fire doesn't race
	// the test exit.
	p.CancelEchoWatchdog()

	mu.Lock()
	got := observedSoftTotal
	mu.Unlock()
	if got < 1 {
		t.Errorf("observedSoftTotal inside emitter = %d; want >= 1", got)
	}
}

// TestPacer_NilEmitter_CountersStillIncrement pins that the
// counters increment even when no emitter callback is registered.
// Diagnostic-only callers (tests, smoke harnesses) need this
// signal without having to wire a full emitter.
func TestPacer_NilEmitter_CountersStillIncrement(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	// Explicitly DO NOT set emitter (or set to nil).
	p.ArmEchoWatchdog(time.Now())
	// Codex round-1 MEDIUM #2 on PR #647: poll instead of fixed
	// sleep.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.SoftTimeoutTotal() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := p.SoftTimeoutTotal(); got != 1 {
		t.Errorf("SoftTimeoutTotal() with nil emitter = %d; want 1", got)
	}
	// Cancel to prevent the hard timer from polluting subsequent
	// state if this test ever runs longer than expected.
	p.CancelEchoWatchdog()
}

// TestClassifier_NewPacerForSession_WiresAdminEvents pins the
// integration contract: NewPacerForSession returns a Pacer whose
// echo-watchdog timeouts surface in the classifier's adminEvents
// ring buffer. Soft fires → drain returns an
// AdminEventKindEchoSoftTimeout entry.
func TestClassifier_NewPacerForSession_WiresAdminEvents(t *testing.T) {
	t.Parallel()
	c := New(ModeShadow)
	p := c.NewPacerForSession()
	if p == nil {
		t.Fatal("NewPacerForSession() = nil; want non-nil")
	}
	// Arm watchdog and poll for the soft fire (Codex round-1
	// MEDIUM #2 on PR #647: counter polling, not fixed sleep).
	p.ArmEchoWatchdog(time.Now())
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.SoftTimeoutTotal() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Cancel hard timer so the event count stays at 1 (soft only).
	p.CancelEchoWatchdog()

	events, dropped := c.DrainAdminEvents()
	if dropped != 0 {
		t.Errorf("dropped = %d; want 0", dropped)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d; want 1", len(events))
	}
	if events[0].Kind != AdminEventKindEchoSoftTimeout {
		t.Errorf("events[0].Kind = %v; want EchoSoftTimeout", events[0].Kind)
	}
}

// TestClassifier_NewPacerForSession_NilClassifier pins the
// defensive nil-return: calling NewPacerForSession on a nil
// classifier (which can happen during ModeOff cleanup paths)
// returns nil rather than panicking.
func TestClassifier_NewPacerForSession_NilClassifier(t *testing.T) {
	t.Parallel()
	var c *Classifier
	if got := c.NewPacerForSession(); got != nil {
		t.Errorf("nil-classifier NewPacerForSession() = %v; want nil", got)
	}
}

// emitMutex is a thin sync.Mutex wrapper so the watchdog tests
// can guard their shared `emitted` slice from the timer-fire
// goroutine without litering the test bodies with sync imports.
type emitMutex struct {
	m sync.Mutex
}

func (e *emitMutex) Lock()   { e.m.Lock() }
func (e *emitMutex) Unlock() { e.m.Unlock() }
