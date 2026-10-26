package runtimecontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

// Issue #1367: a manager-owned failure replacement after RUNNING must reach
// the service view (helianthus.runtime.list, Portal readback) instead of the
// view keeping the RUNNING of the last service-driven start.

const nativeDriver DriverID = "fam.alpha"

// nativeLive waits for the auto-started generation and returns the
// correlation the service admitted it with.
func nativeLive(t *testing.T, f *svcFixture) LifecycleCorrelation {
	t.Helper()
	f.waitSettled(nativeDriver, drivermanager.ObservedRunning)
	var corr LifecycleCorrelation
	for _, call := range f.execs[nativeDriver].snapshot() {
		if call.Op == "start" {
			corr = call.Corr
		}
	}
	if corr.Epoch == "" || corr.Generation == 0 {
		t.Fatalf("no admitted correlation recorded: %+v", f.execs[nativeDriver].snapshot())
	}
	return corr
}

// nativeConsumed waits until the reconciler took the queued observation.
func nativeConsumed(t *testing.T, f *svcFixture) {
	t.Helper()
	d := f.svc.drivers[nativeDriver]
	waitFor(t, "queued native observation consumed", func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.native == nil
	})
}

func nativeWaitStatus(t *testing.T, f *svcFixture, state drivermanager.ObservedState, reason drivermanager.ReasonCode) DriverStatus {
	t.Helper()
	var status DriverStatus
	waitFor(t, string(state)+"/"+string(reason), func() bool {
		status = f.status(nativeDriver)
		return status.ObservedState == state && status.Reason == reason
	})
	return status
}

func TestNativeObservationProjectsManagerRecoveryAfterRunning(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
	corr := nativeLive(t, f)
	before := f.status(nativeDriver)
	cursor := f.svc.List().EventCursor

	steps := []NativeObservation{
		{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled},
		{State: drivermanager.ObservedStarting, Reason: drivermanager.ReasonStartRequested},
		{State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone},
		{State: drivermanager.ObservedDegraded, Reason: drivermanager.ReasonCapabilityDegraded},
		{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled},
		{State: drivermanager.ObservedFailed, Reason: drivermanager.ReasonRetryExhausted},
	}
	for _, step := range steps {
		if !f.svc.ObserveNative(corr, step) {
			t.Fatalf("ObserveNative(%+v) refused", step)
		}
		status := nativeWaitStatus(t, f, step.State, step.Reason)
		if status.DesiredState != before.DesiredState || status.DesiredRevision != before.DesiredRevision || status.SafetyQuarantined {
			t.Fatalf("native observation changed desired state or quarantine: before=%+v after=%+v", before, status)
		}
	}
	page := f.svc.EventsSince(cursor)
	var seen []drivermanager.ObservedState
	for _, ev := range page.Events {
		if ev.Kind != EventObservedChanged || ev.DriverID != nativeDriver {
			continue
		}
		if ev.Epoch != corr.Epoch || ev.Generation != corr.Generation {
			t.Fatalf("event correlation = %s/%d; want the admitted %s/%d", ev.Epoch, ev.Generation, corr.Epoch, corr.Generation)
		}
		seen = append(seen, ev.ObservedState)
	}
	if len(seen) != len(steps) {
		t.Fatalf("observed events = %v; want one per native step %d", seen, len(steps))
	}
	if f.execs[nativeDriver].count("stop") != 0 {
		t.Fatal("a native observation ran an executor Stop")
	}

	// FAILED keeps the generation's cleanup obligations: RESTART withdraws
	// exactly the admitted correlation and opens a successor.
	f.mustDecide(MethodRestart, nativeDriver)
	f.waitSettled(nativeDriver, drivermanager.ObservedRunning)
	var stopped []LifecycleCorrelation
	for _, call := range f.execs[nativeDriver].snapshot() {
		if call.Op == "stop" {
			stopped = append(stopped, call.Corr)
		}
	}
	if len(stopped) == 0 || stopped[0].Epoch != corr.Epoch || stopped[0].Generation != corr.Generation {
		t.Fatalf("RESTART after native FAILED stopped %+v; want the admitted generation first", stopped)
	}
}

// Every compared correlation field is varied on its own: the driver, the
// source epoch and the driver generation. SourceID and Sequence are not
// compared because the service correlation of an untyped start carries
// neither; the bridge supplies them, and its epoch and generation already
// identify the admitted generation.
func TestNativeObservationRejectsForeignAndStaleCorrelation(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver), "fam.beta")})
	corr := nativeLive(t, f)
	f.waitSettled("fam.beta", drivermanager.ObservedRunning)
	backoff := NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled}

	unknown := corr
	unknown.DriverID = "fam.gamma"
	if f.svc.ObserveNative(unknown, backoff) {
		t.Fatal("observation for an unregistered driver was queued")
	}
	for name, mutate := range map[string]func(*LifecycleCorrelation){
		"empty epoch":       func(c *LifecycleCorrelation) { c.Epoch = "" },
		"zero generation":   func(c *LifecycleCorrelation) { c.Generation = 0 },
		"empty driver":      func(c *LifecycleCorrelation) { c.DriverID = "" },
		"noncanonical name": func(c *LifecycleCorrelation) { c.DriverID = "Fam.Alpha" },
	} {
		bad := corr
		mutate(&bad)
		if f.svc.ObserveNative(bad, backoff) {
			t.Fatalf("%s: observation queued", name)
		}
	}

	f.quiesce()
	revision := f.svc.List().Revision
	cursor := f.svc.List().EventCursor
	for name, mutate := range map[string]func(*LifecycleCorrelation){
		"other epoch":      func(c *LifecycleCorrelation) { c.Epoch += ".stale" },
		"older generation": func(c *LifecycleCorrelation) { c.Generation++ },
		"other driver":     func(c *LifecycleCorrelation) { c.DriverID = "fam.beta" },
	} {
		stale := corr
		mutate(&stale)
		if !f.svc.ObserveNative(stale, backoff) {
			t.Fatalf("%s: well-formed observation not queued", name)
		}
		target := stale.DriverID
		d := f.svc.drivers[target]
		waitFor(t, name+" consumed", func() bool {
			d.mu.Lock()
			defer d.mu.Unlock()
			return d.native == nil
		})
		for _, id := range []DriverID{nativeDriver, "fam.beta"} {
			if got := f.status(id); got.ObservedState != drivermanager.ObservedRunning {
				t.Fatalf("%s: %s observed %s/%s; want RUNNING kept", name, id, got.ObservedState, got.Reason)
			}
		}
		if got := f.svc.List().Revision; got != revision {
			t.Fatalf("%s: catalog revision %d -> %d; a stale observation committed: %+v", name, revision, got, f.svc.EventsSince(cursor).Events)
		}
	}
	// Positive control: the exact correlation is applied.
	if !f.svc.ObserveNative(corr, backoff) {
		t.Fatal("exact correlation refused")
	}
	nativeWaitStatus(t, f, drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled)
}

func TestNativeObservationRefusesServiceOwnedVocabulary(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
	corr := nativeLive(t, f)
	for name, obs := range map[string]NativeObservation{
		"stopping":            {State: drivermanager.ObservedStopping, Reason: drivermanager.ReasonStopRequested},
		"stopped":             {State: drivermanager.ObservedStopped, Reason: drivermanager.ReasonOperatorStopped},
		"disabled":            {State: drivermanager.ObservedDisabled, Reason: drivermanager.ReasonConfigDisabled},
		"empty state":         {Reason: drivermanager.ReasonNone},
		"empty reason":        {State: drivermanager.ObservedBackoff},
		"running with reason": {State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonRetryScheduled},
		"quarantined backoff": {State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, Quarantined: true},
		"quarantined running": {State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone, Quarantined: true},
	} {
		if f.svc.ObserveNative(corr, obs) {
			t.Fatalf("%s: observation queued", name)
		}
	}
	if got := f.status(nativeDriver); got.ObservedState != drivermanager.ObservedRunning {
		t.Fatalf("observed %s; want RUNNING kept", got.ObservedState)
	}
	f.svc.Close()
	if f.svc.ObserveNative(corr, NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled}) {
		t.Fatal("closed service queued an observation")
	}
}

// A queued observation never overwrites a service transition: it waits while
// a work slot exists and is dropped once the admitted generation is gone.
func TestNativeObservationNeverOverridesServiceStop(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
	corr := nativeLive(t, f)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	// Registered after the fixture, so a failed assertion still releases the
	// executor before the service waits for its reconciler.
	t.Cleanup(unblock)
	f.execs[nativeDriver].setStop(func(context.Context, LifecycleCorrelation) StopOutcome {
		close(entered)
		<-release
		return StopOutcome{NativeClosed: true, SemanticCleaned: true}
	})
	f.mustDecide(MethodStop, nativeDriver)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("STOP did not reach the executor")
	}
	if !f.svc.ObserveNative(corr, NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled}) {
		t.Fatal("well-formed observation not queued")
	}
	time.Sleep(20 * time.Millisecond)
	if got := f.status(nativeDriver); got.ObservedState != drivermanager.ObservedStopping {
		t.Fatalf("observed %s during STOP; want STOPPING kept", got.ObservedState)
	}
	unblock()
	f.waitSettled(nativeDriver, drivermanager.ObservedStopped)
	f.svc.wakeDriver(f.svc.drivers[nativeDriver])
	nativeConsumed(t, f)
	if got := f.status(nativeDriver); got.ObservedState != drivermanager.ObservedStopped {
		t.Fatalf("observed %s after STOP; the withdrawn generation's observation was applied", got.ObservedState)
	}
}

func TestNativeObservationQuarantineBlocksAdmission(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
	corr := nativeLive(t, f)
	if !f.svc.ObserveNative(corr, NativeObservation{State: drivermanager.ObservedFailed, Reason: drivermanager.ReasonCloseUnconfirmed, Quarantined: true}) {
		t.Fatal("quarantining observation refused")
	}
	status := nativeWaitStatus(t, f, drivermanager.ObservedFailed, drivermanager.ReasonCloseUnconfirmed)
	if !status.SafetyQuarantined {
		t.Fatalf("status = %+v; want safety quarantined", status)
	}
	starts := f.execs[nativeDriver].count("start")
	// START while desired RUNNING stays the CTL-06 no-op; RESTART is refused.
	if _, err := f.decide(MethodStart, nativeDriver); err != nil {
		t.Fatalf("START no-op after native quarantine = %v", err)
	}
	_, err := f.decide(MethodRestart, nativeDriver)
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Code != RefusalFailedPrecondition {
		t.Fatalf("RESTART after native quarantine = %v; want FAILED_PRECONDITION", err)
	}
	if got := f.execs[nativeDriver].count("start"); got != starts {
		t.Fatalf("executor starts %d -> %d after native quarantine; want none", starts, got)
	}
	// Quarantine is terminal for this process: the reconciler takes no step
	// for the driver, so a later observation is never applied.
	if !f.svc.ObserveNative(corr, NativeObservation{State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone}) {
		t.Fatal("well-formed observation not queued")
	}
	time.Sleep(50 * time.Millisecond)
	if got := f.status(nativeDriver); got.ObservedState != drivermanager.ObservedFailed || !got.SafetyQuarantined {
		t.Fatalf("observed %+v; quarantine was overwritten", got)
	}
}

// A decision that raises its hold between the reconciler's check and the
// apply, then fails to persist, must not lose the queued terminal FAILED.
func TestNativeObservationSurvivesDecisionHoldThatReleases(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
	corr := nativeLive(t, f)
	d := f.svc.drivers[nativeDriver]

	d.mu.Lock()
	d.deciding++
	d.native = &pendingNative{corr: corr, obs: NativeObservation{State: drivermanager.ObservedFailed, Reason: drivermanager.ReasonRetryExhausted}}
	d.mu.Unlock()

	rec, err := f.svc.store.Driver(nativeDriver)
	if err != nil {
		t.Fatal(err)
	}
	f.svc.applyNative(d, rec)
	d.mu.Lock()
	kept := d.native != nil
	d.mu.Unlock()
	if !kept {
		t.Fatal("applyNative discarded the queued observation during a decision hold")
	}

	f.svc.releaseDecision(d, false)
	nativeWaitStatus(t, f, drivermanager.ObservedFailed, drivermanager.ReasonRetryExhausted)
}

// A stale-generation callback arriving while a valid observation is queued
// must not replace it.
func TestNativeObservationStaleCallbackKeepsQueuedObservation(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
	corr := nativeLive(t, f)
	d := f.svc.drivers[nativeDriver]

	d.mu.Lock()
	d.deciding++
	d.mu.Unlock()
	if !f.svc.ObserveNative(corr, NativeObservation{State: drivermanager.ObservedFailed, Reason: drivermanager.ReasonRetryExhausted, Quarantined: true}) {
		t.Fatal("valid observation was not queued")
	}
	stale := corr
	stale.Generation++
	f.svc.ObserveNative(stale, NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled})
	d.mu.Lock()
	kept := d.native != nil && d.native.obs.State == drivermanager.ObservedFailed
	d.mu.Unlock()
	if !kept {
		t.Fatal("stale callback replaced the queued FAILED observation")
	}
	f.svc.releaseDecision(d, false)
	nativeWaitStatus(t, f, drivermanager.ObservedFailed, drivermanager.ReasonRetryExhausted)
}

// A record read before an accepted transition released its hold must not
// let the queued observation through once a work slot exists.
func TestNativeObservationRereadsWorkSlot(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
	corr := nativeLive(t, f)
	d := f.svc.drivers[nativeDriver]
	stale, err := f.svc.store.Driver(nativeDriver)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.execs[nativeDriver].setStop(func(context.Context, LifecycleCorrelation) StopOutcome {
		once.Do(func() { close(entered) })
		<-release
		return StopOutcome{NativeClosed: true, SemanticCleaned: true}
	})
	defer close(release)
	f.mustDecide(MethodRestart, nativeDriver)
	<-entered
	if f.record(nativeDriver).Current == nil {
		t.Fatal("expected a committed work slot")
	}
	d.mu.Lock()
	d.deciding, d.inflight, d.untyped, d.ambiguous = 0, nil, nil, nil
	d.live = &attempt{corr: corr}
	d.corr = corr
	d.native = &pendingNative{corr: corr, obs: NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled}}
	d.mu.Unlock()
	before := f.status(nativeDriver)
	f.svc.applyNative(d, stale)
	if got := f.status(nativeDriver); got.ObservedState == drivermanager.ObservedBackoff && before.ObservedState != got.ObservedState {
		t.Fatalf("native observation published over a committed work slot: %+v", got)
	}
}
