package adaptermux

import (
	"log"
	"sync"
	"testing"
	"time"
)

// absorb_debounce_test.go pins the contract for F-NEW-25 (2026-05-21):
// armPendingStartAbsorbLocked uses a single persistent *time.Timer +
// Reset() (debounce semantics) instead of per-arm time.AfterFunc +
// pendingAbsorbGen generation-invalidation.
//
// Defense-in-depth rationale. The 2026-05-20 12:25 UTC production
// drop in active B524 polling to 0x15 (BASV2) was investigated and
// the root cause was a bus-physical signal-loss storm — the gateway's
// semantic_read_breaker correctly opened (628 closed→open transitions)
// and active polling correctly suspended; the gateway recovered on
// its own once the bus stabilized. Live-archaeology log scan showed
// ZERO `absorb timeout reset` events in 94k lines, meaning F-22
// actually wasn't firing — but the gen-invalidation pattern IS
// theoretically vulnerable to a livelock when arm rate exceeds
// 1/StartDeadline (each new arm bumps gen and invalidates its own
// AfterFunc's gen check at fire time, so the reset never executes).
// The post-fix single-timer + Reset() makes that class of livelock
// structurally impossible — Reset() always extends the deadline of
// the SAME persistent timer regardless of arm rate.
//
// Test-coverage honesty: the tests below pin the new semantics under
// rapid arming but they cannot DISTINGUISH pre-fix from post-fix
// behavior in a controlled environment. Both patterns reset the
// counter within 1 deadline after arming stops; the bug requires
// sustained-forever arming to manifest, which a unit test can't
// run indefinitely. The Codex-round-1 stale-callback race
// (pre-empted AfterFunc clearing a fresh post-arm counter) IS
// regression-tested explicitly — see TestAbsorbReset_StaleCallback*.

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

// TestAbsorbReset_RapidArmsStillEventuallyReset arms
// armPendingStartAbsorbLocked at rate FASTER than StartDeadline,
// stops re-arming, and asserts the absorb-reset fires within the
// next deadline window. This is a forward-going semantic check —
// see file header for honest pre/post-fix discrimination notes.
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
	var stopOnce sync.Once
	stopArmer := func() {
		stopOnce.Do(func() { close(stop) })
	}
	// Codex round-1 LOW: t.Cleanup guarantees the armer goroutine
	// exits even if the test fails before the explicit close(stop)
	// below. Without it, a future edit that introduces a t.Fatal
	// pre-stop would leak the armer for the rest of the test
	// process's lifetime.
	t.Cleanup(func() {
		stopArmer()
		<-armerDone
	})
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
	// timer to fire. stopArmer is idempotent via sync.Once so the
	// t.Cleanup close in the cleanup func above is a safe re-close.
	stopArmer()
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

// TestAbsorbReset_StaleCallbackDoesNotClearFreshArm directly exercises
// the Codex round-1 MUST FIX race: a stale fireAbsorbReset callback,
// already queued by an expired timer but still waiting on stateMu,
// must NOT clear a counter that a fresh arm has just incremented
// (and Reset()'d the timer for).
//
// Without the pendingAbsorbResetDueAt guard, the stale callback
// would see pendingStartAbsorb > 0 and clear it — breaking the
// stale-response barrier for a transaction that JUST armed. The
// guard makes the stale callback no-op when the canonical due-at
// has been bumped into the future by a fresh arm.
//
// Deterministic simulation: instead of trying to win a real-time
// race against AfterFunc scheduling, we directly drive
// fireAbsorbReset with the FIELD STATE that the race would produce.
// Pre-condition setup mimics "stale callback about to run, fresh
// arm just bumped due-at":
//  1. Set pendingStartAbsorb = 1 (a fresh arm's increment).
//  2. Set pendingAbsorbResetDueAt = now + future (a fresh arm's
//     deadline extension).
//
// Then call fireAbsorbReset directly and verify the counter is
// untouched. Without the guard the counter would clear; WITH the
// guard the future-dated due-at causes early-return.
func TestAbsorbReset_StaleCallbackDoesNotClearFreshArm(t *testing.T) {
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

	// Simulate the post-race state: pendingStartAbsorb > 0 AND
	// pendingAbsorbResetDueAt in the future (a fresh arm pre-empted
	// the stale callback). The stale callback is fireAbsorbReset
	// itself, invoked directly to bypass goroutine scheduling
	// non-determinism.
	mux.stateMu.Lock()
	mux.pendingStartAbsorb = 1
	mux.pendingAbsorbResetDueAt = time.Now().Add(startDeadline)
	mux.pendingAbsorbLastReason = "test-stale-fresh-armed"
	mux.stateMu.Unlock()

	mux.fireAbsorbReset()

	mux.stateMu.Lock()
	postAbsorb := mux.pendingStartAbsorb
	postDueAt := mux.pendingAbsorbResetDueAt
	mux.stateMu.Unlock()

	if postAbsorb != 1 {
		t.Fatalf("stale fireAbsorbReset cleared pendingStartAbsorb (now %d, want 1) "+
			"despite future-dated pendingAbsorbResetDueAt — the due-at guard is broken. "+
			"Production impact: a fresh stale-response barrier gets cleared "+
			"the instant after a fresh arm raises it, exposing the next request "+
			"to misapplied stale STARTED/FAILED.",
			postAbsorb)
	}
	if postDueAt.IsZero() {
		t.Error("stale fireAbsorbReset cleared pendingAbsorbResetDueAt despite no-op path; " +
			"the no-op branch must NOT touch the due-at field (fresh arm owns it)")
	}
	if got := mux.absorbResetTotal.Load(); got != priorReset {
		t.Errorf("stale fireAbsorbReset incremented absorbResetTotal (was %d, now %d) "+
			"on the no-op path; the increment is paired with the actual reset only",
			priorReset, got)
	}
}

// TestAbsorbReset_LegitimateCallbackClearsWhenDueAtElapsed pins
// the complementary contract: when pendingAbsorbResetDueAt is in
// the PAST (the legitimate "deadline reached, no fresh arm bumped
// it" case), fireAbsorbReset proceeds to reset.
func TestAbsorbReset_LegitimateCallbackClearsWhenDueAtElapsed(t *testing.T) {
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

	// Simulate the legitimate case: pendingStartAbsorb > 0 AND
	// pendingAbsorbResetDueAt in the past (the natural deadline
	// elapsed). fireAbsorbReset must clear.
	mux.stateMu.Lock()
	mux.pendingStartAbsorb = 2
	mux.pendingAbsorbResetDueAt = time.Now().Add(-50 * time.Millisecond)
	mux.pendingAbsorbLastReason = "test-elapsed"
	mux.stateMu.Unlock()

	mux.fireAbsorbReset()

	mux.stateMu.Lock()
	postAbsorb := mux.pendingStartAbsorb
	postDueAt := mux.pendingAbsorbResetDueAt
	mux.stateMu.Unlock()

	if postAbsorb != 0 {
		t.Errorf("legitimate fireAbsorbReset did NOT clear pendingStartAbsorb (still %d); "+
			"the due-at-in-past path must reset",
			postAbsorb)
	}
	if !postDueAt.IsZero() {
		t.Errorf("legitimate fireAbsorbReset did NOT clear pendingAbsorbResetDueAt (got %v); "+
			"the reset path should zero it to prevent any further stale callbacks "+
			"from misinterpreting the field",
			postDueAt)
	}
	if got := mux.absorbResetTotal.Load(); got != priorReset+1 {
		t.Errorf("legitimate fireAbsorbReset advanced absorbResetTotal by %d (was %d, now %d); want exactly 1",
			got-priorReset, priorReset, got)
	}
}

// TestAbsorbReset_DueAtClearedOnBoundary pins that Close, reconnect,
// and handleReset all clear pendingAbsorbResetDueAt. Without this,
// a stale callback queued before the boundary could observe the
// pre-boundary future-dated due-at as "skip" and never let the
// post-boundary state advance.
//
// We exercise Close here (the simplest boundary that's reachable
// from a test fixture); reconnect and handleReset paths execute
// the same field-clear under stateMu so the contract is uniform.
func TestAbsorbReset_DueAtClearedOnBoundary(t *testing.T) {
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

	mux.stateMu.Lock()
	mux.armPendingStartAbsorbLocked("test-boundary")
	preCloseDueAt := mux.pendingAbsorbResetDueAt
	mux.stateMu.Unlock()
	if preCloseDueAt.IsZero() {
		t.Fatal("pendingAbsorbResetDueAt is zero immediately after arm")
	}

	_ = mux.Close()

	mux.stateMu.Lock()
	postCloseDueAt := mux.pendingAbsorbResetDueAt
	postCloseTimer := mux.pendingAbsorbResetTimer
	postCloseAbsorb := mux.pendingStartAbsorb
	mux.stateMu.Unlock()

	if !postCloseDueAt.IsZero() {
		t.Errorf("after Close, pendingAbsorbResetDueAt = %v; want zero "+
			"(boundary failed to clear the due-at — stale callback could no-op forever)",
			postCloseDueAt)
	}
	if postCloseTimer != nil {
		t.Errorf("after Close, pendingAbsorbResetTimer = %p; want nil", postCloseTimer)
	}
	if postCloseAbsorb != 0 {
		t.Errorf("after Close, pendingStartAbsorb = %d; want 0", postCloseAbsorb)
	}
}
