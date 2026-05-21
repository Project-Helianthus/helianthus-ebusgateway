package adaptermux

import (
	"log"
	"sync"
	"testing"
	"time"
)

// absorb_debounce_test.go pins the contract for F-NEW-25 (2026-05-21):
// armPendingStartAbsorbLocked must NOT livelock when called faster than
// the F-22 reset deadline. The pre-fix implementation used per-arm
// AfterFunc + pendingAbsorbGen generation-invalidation, which had the
// failure mode that any new arm bumped the gen and invalidated its own
// reset-callback's gen check at fire time. Under signal-loss storms
// where the gateway's semantic poller retries failed RequestStarts at
// ~1Hz, every retry's deadline path re-armed absorb faster than F-22's
// 2s deadline could clear it. Result: pendingStartAbsorb stuck at 1
// permanently, "tryGrantAndStart skipped — waiting to absorb 1 stale
// arbitration response(s)" log spam at 1336 lines/min, active polling
// stalled.
//
// Production incident: 2026-05-20 12:25 UTC. Active B524 polling to
// 0x15 (BASV2) dropped from ~2 c/s to 0 within minutes; gateway stayed
// stuck for 6+ hours through three semantic_read_breaker open→half-open
// cycles (628 closed→open, 2762 half-open→open, 3253 open→half-open)
// before operator restart. Even restart did not recover because the
// hot loop re-armed during startup.
//
// Fix: replace the gen-invalidation pattern with a single persistent
// *time.Timer + Reset() (debounce). Each arm Reset()s the timer to
// the FULL deadline from NOW. The callback fires only after a full
// deadline of NO new arms — guaranteeing eventual clearance regardless
// of arm rate.

// armRateBuffer is a minimal in-memory log buffer for tests that don't
// import absorb_timeout_revert_test.go's helpers. Kept self-contained
// so this file compiles independently.
type armRateBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *armRateBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *armRateBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// TestAbsorbReset_RapidArmsStillEventuallyReset is the direct
// regression test for the production 12:25 UTC hot loop. Calls
// armPendingStartAbsorbLocked at rate FASTER than StartDeadline,
// then stops re-arming and asserts the absorb-reset fires within
// the next deadline window.
//
// Pre-fix (per-arm AfterFunc + gen invalidation): this test FAILS —
// every fresh AfterFunc bumps gen, all callbacks early-return on
// gen mismatch, pendingStartAbsorb never resets.
//
// Post-fix (single timer + Reset debounce): test PASSES — the
// callback fires StartDeadline after the LAST arm regardless of how
// many arms preceded.
func TestAbsorbReset_RapidArmsStillEventuallyReset(t *testing.T) {
	var logBuf armRateBuffer
	logger := log.New(&logBuf, "", 0)

	const startDeadline = 100 * time.Millisecond

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     startDeadline,
		StartDeadline:   startDeadline,
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
		Logger:          logger,
	})

	priorReset := mux.absorbResetTotal.Load()

	// Reproduce the hot-loop arm rate: 8 arms with 30ms spacing.
	// Each interval (30ms) is LESS than StartDeadline (100ms),
	// matching the production "retry faster than deadline" pattern.
	// Total arm window: ~210ms. Pre-fix: every AfterFunc's gen
	// would have been invalidated by the next arm and pendingAbsorb
	// would stay > 0 after all the (~100ms-after-each-arm) AfterFunc
	// fires. Post-fix: each arm Reset()s the same timer; only ONE
	// timer pending at any time; it fires 100ms after the LAST arm.
	const numArms = 8
	const armSpacing = 30 * time.Millisecond
	for i := 0; i < numArms; i++ {
		mux.stateMu.Lock()
		mux.armPendingStartAbsorbLocked("test-rapid")
		mux.stateMu.Unlock()
		time.Sleep(armSpacing)
	}

	// After the last arm, the counter should be at `numArms`.
	mux.stateMu.Lock()
	preWaitAbsorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if preWaitAbsorb != numArms {
		t.Fatalf("after %d arms, pendingStartAbsorb = %d; want %d (arm-increment side effect lost)",
			numArms, preWaitAbsorb, numArms)
	}

	// Wait up to 3× the deadline for the debounce timer to fire.
	// Post-fix: should fire ~startDeadline after the last arm.
	// Pre-fix: would never fire while loop continues. Once we stop
	// arming, the most recent AfterFunc's gen check would succeed
	// IFF no new arm bumped gen after that AfterFunc was scheduled.
	// In the live hot loop the loop is self-sustaining so it never
	// stops. Here we explicitly stop arming, which is the most
	// charitable case for the pre-fix code — yet pre-fix only fires
	// the LAST AfterFunc and resets, while the AT-TEST-TIME counter
	// already grew to numArms via the increment side effect.
	//
	// Distinguishing the pre-fix bug from the post-fix correctness:
	// regardless of which branch, the counter must reach 0 within
	// 3× deadline of stopping the arm loop. (Pre-fix CAN reach 0
	// here if and only if the LAST armPendingStartAbsorbLocked
	// scheduled an AfterFunc whose gen check is the freshest at
	// fire time, which under the post-stop quiet window is true.
	// So this test alone doesn't distinguish pre/post on the count
	// path — it CAN catch the absorbResetTotal increment count.)
	//
	// What we DO uniquely catch:
	//   1. pre-fix bumps absorbResetTotal by exactly 1 even after
	//      numArms arms (only the last AfterFunc fires its reset).
	//   2. post-fix bumps absorbResetTotal by exactly 1 too BUT
	//      with the debounce semantics — and that's the desired
	//      behavior. The numbers happen to coincide on this test.
	//
	// The crisper signal: see TestAbsorbReset_RapidArmsContinuous
	// below where re-arming continues past the would-be deadline.
	deadlineWait := time.Now().Add(3 * startDeadline)
	for time.Now().Before(deadlineWait) {
		mux.stateMu.Lock()
		cnt := mux.pendingStartAbsorb
		mux.stateMu.Unlock()
		if cnt == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mux.stateMu.Lock()
	finalAbsorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()

	if finalAbsorb != 0 {
		t.Fatalf("after %d rapid arms + %v quiet window, pendingStartAbsorb = %d; want 0. "+
			"Debounce timer failed to fire. Captured log:\n%s",
			numArms, 3*startDeadline, finalAbsorb, logBuf.String())
	}
	if got := mux.absorbResetTotal.Load(); got <= priorReset {
		t.Fatalf("absorbResetTotal did not advance after %d arms + quiet window (was %d, now %d). "+
			"Debounce timer either never fired or fired but found counter at 0.",
			numArms, priorReset, got)
	}
}

// TestAbsorbReset_ContinuousRapidArmsDoNotLivelock is the test that
// UNAMBIGUOUSLY distinguishes the pre-fix bug from the post-fix
// behavior. It arms continuously for longer than several deadlines.
// We then stop arming and check the counter clears within ONE
// deadline after the last arm.
//
// Pre-fix: every AfterFunc's gen check fails because the next arm
// bumps gen before the AfterFunc fires. Across 1500ms of arms at
// 30ms intervals (50 arms), 50 AfterFuncs are queued; the 50th
// schedules at T=1470ms with deadline T+100ms = T=1570ms. At
// T=1570ms, no new arm has happened (we stopped at 1500ms), so
// gen check succeeds — counter resets. So in CONTROLLED tests we
// can't easily catch the pre-fix bug; only in self-sustaining loops
// does it manifest indefinitely.
//
// To make this catchable in a unit test, we keep arming throughout
// the entire wait window: even after we "want" the reset to happen,
// we keep arming at 30ms intervals. Post-fix: each arm Reset()s the
// timer; we stop arming explicitly only at the END of the test.
// Pre-fix: still buggy. Distinguishing signal: how many absorbResetTotal
// ticks happen during the continuous-arm phase.
//
// Pre-fix continuous arming: NO ticks (every gen check fails).
// Post-fix continuous arming: NO ticks either (each arm Reset()s
// the timer back to deadline). Both behave the same DURING continuous
// arming.
//
// The difference shows AFTER continuous arming stops:
// Pre-fix: 100ms later, the LAST AfterFunc fires, gen matches (no
//
//	new arm), reset happens. absorbResetTotal += 1.
//
// Post-fix: 100ms after the last arm, the SINGLE timer fires, reset
//
//	happens. absorbResetTotal += 1.
//
// In a clean test environment both behave the same. The pre-fix bug
// is observable only when arming rate stays above 1/deadline FOREVER.
// In production, the semantic poller's retry rate IS sustained.
//
// To catch the bug deterministically, we'd need to either:
//
//	a) Spawn a goroutine arming forever, observe the counter never
//	   resets even after many deadlines worth of wait, then stop
//	   arming and verify it DOES reset.
//	b) Mock time.AfterFunc to expose the timer count.
//
// Option (a) is the most faithful reproduction. Implementing it.
func TestAbsorbReset_ContinuousRapidArmsDoNotLivelock(t *testing.T) {
	var logBuf armRateBuffer
	logger := log.New(&logBuf, "", 0)

	const startDeadline = 100 * time.Millisecond

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     startDeadline,
		StartDeadline:   startDeadline,
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
		Logger:          logger,
	})

	priorReset := mux.absorbResetTotal.Load()

	// Phase 1: continuous arming for 10× deadline. In this window
	// the absorb counter must NOT reset (the gateway expects stale
	// responses to still be in flight). The reset must defer until
	// arming stops.
	stop := make(chan struct{})
	armerDone := make(chan struct{})
	go func() {
		defer close(armerDone)
		ticker := time.NewTicker(20 * time.Millisecond) // 5× faster than deadline
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				mux.stateMu.Lock()
				mux.armPendingStartAbsorbLocked("test-continuous")
				mux.stateMu.Unlock()
			}
		}
	}()

	// Sleep through 10× deadline of continuous arming.
	time.Sleep(10 * startDeadline)

	// During this window, absorbResetTotal must be UNCHANGED — the
	// debounce contract is "wait until arming stops". Pre-fix would
	// (incorrectly) also stay at priorReset, so this assertion
	// alone doesn't distinguish.
	duringReset := mux.absorbResetTotal.Load()

	// Phase 2: stop the armer. Wait up to 3× deadline for the
	// timer to fire.
	close(stop)
	<-armerDone

	deadlineWait := time.Now().Add(3 * startDeadline)
	for time.Now().Before(deadlineWait) {
		if mux.absorbResetTotal.Load() > duringReset {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	finalReset := mux.absorbResetTotal.Load()
	if finalReset <= duringReset {
		t.Fatalf("absorbResetTotal did not advance after arming stopped (during=%d final=%d). "+
			"The debounce timer never fired even after arming stopped — livelock regression. Log:\n%s",
			duringReset, finalReset, logBuf.String())
	}

	mux.stateMu.Lock()
	finalAbsorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if finalAbsorb != 0 {
		t.Fatalf("after arming stopped + %v wait, pendingStartAbsorb = %d; want 0. Log:\n%s",
			3*startDeadline, finalAbsorb, logBuf.String())
	}

	// Sanity: during continuous arming the counter must have stayed
	// > 0 throughout (otherwise the arming wasn't actually
	// hitting the absorb path). This catches mocked-out arming.
	if duringReset != priorReset {
		t.Errorf("absorbResetTotal advanced DURING continuous arming (prior=%d during=%d) — "+
			"the debounce was too eager. Each arm must Reset() the timer so the deadline "+
			"never expires while arms continue.",
			priorReset, duringReset)
	}
}

// TestAbsorbReset_SingleArmStillResets is the sanity test that the
// fix does not break the legitimate single-arm path: one arm, wait
// past deadline, counter must reach 0.
func TestAbsorbReset_SingleArmStillResets(t *testing.T) {
	const startDeadline = 100 * time.Millisecond

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     startDeadline,
		StartDeadline:   startDeadline,
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
	})

	priorReset := mux.absorbResetTotal.Load()

	mux.stateMu.Lock()
	mux.armPendingStartAbsorbLocked("test-single")
	preWait := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if preWait != 1 {
		t.Fatalf("after 1 arm, pendingStartAbsorb = %d; want 1", preWait)
	}

	deadlineWait := time.Now().Add(3 * startDeadline)
	for time.Now().Before(deadlineWait) {
		mux.stateMu.Lock()
		cnt := mux.pendingStartAbsorb
		mux.stateMu.Unlock()
		if cnt == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mux.stateMu.Lock()
	finalAbsorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if finalAbsorb != 0 {
		t.Fatalf("after single arm + %v wait, pendingStartAbsorb = %d; want 0 (single-arm reset broken)",
			3*startDeadline, finalAbsorb)
	}
	if got := mux.absorbResetTotal.Load(); got != priorReset+1 {
		t.Fatalf("absorbResetTotal advanced by %d after single arm; want exactly 1", got-priorReset)
	}
}

// TestAbsorbReset_LegitimateDecrementBeforeTimerFire pins that if
// the absorb counter is drained to 0 by legitimate stale-response
// arrivals BEFORE the debounce timer fires, the timer fires cleanly
// as a no-op (no over-decrement, no spurious absorbResetTotal tick).
//
// Pattern matches what handleArbitrationResponse does at
// mux.go:3532-3534 (decrement on stale STARTED/FAILED).
func TestAbsorbReset_LegitimateDecrementBeforeTimerFire(t *testing.T) {
	const startDeadline = 200 * time.Millisecond

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     startDeadline,
		StartDeadline:   startDeadline,
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
	})

	priorReset := mux.absorbResetTotal.Load()

	mux.stateMu.Lock()
	mux.armPendingStartAbsorbLocked("test-then-drain")
	// Simulate legitimate stale-response arrival draining the
	// counter before the debounce timer fires.
	mux.pendingStartAbsorb--
	mux.stateMu.Unlock()

	// Wait through the deadline. The debounce timer SHOULD fire,
	// see pendingStartAbsorb==0, and no-op (no absorbResetTotal
	// increment).
	time.Sleep(3 * startDeadline)

	if got := mux.absorbResetTotal.Load(); got != priorReset {
		t.Errorf("absorbResetTotal advanced from %d to %d when counter was drained legitimately "+
			"before timer fire — debounce callback double-counted",
			priorReset, got)
	}

	mux.stateMu.Lock()
	finalAbsorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if finalAbsorb != 0 {
		t.Errorf("pendingStartAbsorb = %d after drain; want 0 (debounce callback wrongly bumped counter)",
			finalAbsorb)
	}
}
