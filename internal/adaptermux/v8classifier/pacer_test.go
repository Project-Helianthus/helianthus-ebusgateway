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

// TestPacer_GraceBootstrap_TriggeredByLongIdle pins the v8 §1.4
// long-idle trigger: a RecordEcho call with now-lastEcho >= 30s
// sets graceRemaining to 3.
func TestPacer_GraceBootstrap_TriggeredByLongIdle(t *testing.T) {
	t.Parallel()
	p := NewPacer()

	// First sample establishes lastEchoAt.
	p.RecordEcho(50*time.Millisecond, t0)
	if p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=true after first sample; want false (no prior idle)")
	}

	// Long-idle: next sample 31s later → grace bootstrap.
	tLate := t0.Add(31 * time.Second)
	p.RecordEcho(50*time.Millisecond, tLate)
	// After this call: graceRemaining was set to 3, then
	// decremented to 2 by the RecordEcho's tail.
	if !p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=false after long-idle sample; want true (entered grace)")
	}

	// Two more samples → graceRemaining 1 → 0 (exits grace).
	p.RecordEcho(50*time.Millisecond, tLate.Add(100*time.Millisecond))
	if !p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=false after 2nd grace sample; want true (still in grace)")
	}
	p.RecordEcho(50*time.Millisecond, tLate.Add(200*time.Millisecond))
	if p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=true after 3rd grace sample; want false (grace exhausted)")
	}
}

// TestPacer_GraceBootstrap_NotTriggeredByShortIdle pins the
// boundary: idle < 30s does NOT enter grace.
func TestPacer_GraceBootstrap_NotTriggeredByShortIdle(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	p.RecordEcho(50*time.Millisecond, t0)
	// 29 seconds — below the 30s threshold.
	p.RecordEcho(50*time.Millisecond, t0.Add(29*time.Second))
	if p.InGraceBootstrap() {
		t.Error("InGraceBootstrap()=true after 29s idle; want false")
	}
}

// TestPacer_EchoDeadlines_NormalMode pins the v8 §1.4 formula:
// soft = L_rtt + 100ms, hard = 2×L_rtt + 200ms.
func TestPacer_EchoDeadlines_NormalMode(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	// At construction: L_rtt = 100ms, no grace.
	soft, hard := p.EchoDeadlines()
	wantSoft := 100*time.Millisecond + SoftDeadlineNormal
	wantHard := 2*100*time.Millisecond + HardDeadlineNormalAdd
	if soft != wantSoft {
		t.Errorf("soft=%v; want %v", soft, wantSoft)
	}
	if hard != wantHard {
		t.Errorf("hard=%v; want %v", hard, wantHard)
	}
}

// TestPacer_EchoDeadlines_GraceMode pins that during grace, soft
// uses the loose 500ms slack and hard is disabled (returns 0).
func TestPacer_EchoDeadlines_GraceMode(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	// Trigger grace by long-idle pattern.
	p.RecordEcho(50*time.Millisecond, t0)
	p.RecordEcho(50*time.Millisecond, t0.Add(31*time.Second))
	if !p.InGraceBootstrap() {
		t.Fatal("precondition: should be in grace")
	}
	soft, hard := p.EchoDeadlines()
	// L_rtt should be ~85ms after two 50ms samples (first 100 → 85,
	// second 85 → 79.5). Either way the formula adds 500ms slack.
	if hard != 0 {
		t.Errorf("hard=%v in grace mode; want 0 (disabled)", hard)
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
	p.RecordEcho(50*time.Millisecond, t0.Add(31*time.Second))
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
// calls use the new τ.
func TestPacer_TauOverride(t *testing.T) {
	t.Parallel()
	p := NewPacer()
	const customTau = 10 * time.Millisecond
	p.SetTau(customTau)

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
				_, _ = p.EchoDeadlines()
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
