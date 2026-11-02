package runtimecontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

// Issue #1371: an executor whose native owner took the opening generation
// into its own persistent recovery returns a RecoveringStart. The service
// admits the generation (its cleanup obligations pass to it, so STOP and
// RESTART withdraw it) and publishes the owner's BACKOFF with its schedule
// instead of RUNNING. The owner's later transitions arrive through
// ObserveNative. A RecoveringStart the service cannot admit fails closed: the
// generation is withdrawn and the driver ends FAILED.

func issue1371Recovering(attempt int, due time.Time) *RecoveringStart {
	return &RecoveringStart{Observation: NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled,
		NextAttempt: attempt, NextAttemptUTC: due}}
}

func issue1371Calls(f *svcFixture, op string) []LifecycleCorrelation {
	var out []LifecycleCorrelation
	for _, call := range f.execs[nativeDriver].snapshot() {
		if call.Op == op {
			out = append(out, call.Corr)
		}
	}
	return out
}

func issue1371RequireNoLifecycleError(t *testing.T, f *svcFixture) {
	t.Helper()
	for _, err := range f.errs.list() {
		var lifecycle *LifecycleError
		if errors.As(err, &lifecycle) {
			t.Fatalf("an admitted recovery was reported as an executor failure: %v", err)
		}
	}
}

// The process-start (untyped) attempt.
func TestIssue1371RecoveringProcessStartIsAdmittedIntoNativeRecovery(t *testing.T) {
	due := time.Date(2026, time.October, 26, 7, 0, 1, 0, time.FixedZone("CET", 3600))
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver), "fam.beta"), setup: func(f *svcFixture) {
		f.execs[nativeDriver].setStart(func(context.Context, LifecycleCorrelation) error { return issue1371Recovering(1, due) })
	}})
	status := nativeWaitStatus(t, f, drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled)
	if status.NextAttempt != 1 || !status.NextAttemptUTC.Equal(due) || status.NextAttemptUTC.Location() != time.UTC {
		t.Fatalf("view schedule = %d at %s; want 1 at %s in UTC", status.NextAttempt, status.NextAttemptUTC, due.UTC())
	}
	if status.DesiredState != DesiredRunning || status.SafetyQuarantined {
		t.Fatalf("view = %+v; want desired RUNNING, not quarantined", status)
	}
	f.waitSettled("fam.beta", drivermanager.ObservedRunning)
	starts := issue1371Calls(f, "start")
	if len(starts) != 1 || len(issue1371Calls(f, "stop")) != 0 {
		t.Fatalf("starts %+v, stops %d; want one start and no withdrawal (the service never retries a native recovery)", starts, len(issue1371Calls(f, "stop")))
	}
	issue1371RequireNoLifecycleError(t, f)
	corr := starts[0]

	// The admitted correlation receives the owner's later transitions.
	for _, step := range []NativeObservation{
		{State: drivermanager.ObservedStarting, Reason: drivermanager.ReasonStartRequested},
		{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, NextAttempt: 2, NextAttemptUTC: due.Add(2 * time.Second)},
		{State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone},
	} {
		if !f.svc.ObserveNative(corr, step) {
			t.Fatalf("ObserveNative(%+v) refused", step)
		}
		got := nativeWaitStatus(t, f, step.State, step.Reason)
		if got.NextAttempt != step.NextAttempt {
			t.Fatalf("%s view next attempt = %d; want %d", step.State, got.NextAttempt, step.NextAttempt)
		}
	}

	// STOP withdraws exactly the admitted generation.
	f.mustDecide(MethodStop, nativeDriver)
	f.waitSettled(nativeDriver, drivermanager.ObservedStopped)
	if stops := issue1371Calls(f, "stop"); len(stops) == 0 || stops[0].Epoch != corr.Epoch || stops[0].Generation != corr.Generation {
		t.Fatalf("STOP withdrew %+v; want the admitted %s/%d", stops, corr.Epoch, corr.Generation)
	}
}

// The typed START, then a RESTART during the recovery.
func TestIssue1371RecoveringTypedStartIsAdmittedAndRestartWithdrawsIt(t *testing.T) {
	due := time.Date(2026, time.October, 26, 7, 0, 1, 0, time.UTC)
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
	f.waitSettled(nativeDriver, drivermanager.ObservedRunning)
	f.mustDecide(MethodStop, nativeDriver)
	f.waitSettled(nativeDriver, drivermanager.ObservedStopped)

	f.execs[nativeDriver].setStart(func(context.Context, LifecycleCorrelation) error { return issue1371Recovering(1, due) })
	started := f.mustDecide(MethodStart, nativeDriver)
	f.waitSettled(nativeDriver, drivermanager.ObservedBackoff)
	status := f.status(nativeDriver)
	if status.Reason != drivermanager.ReasonRetryScheduled || status.NextAttempt != 1 || !status.NextAttemptUTC.Equal(due) || status.DesiredRevision != started.DesiredRevision {
		t.Fatalf("view = %+v; want BACKOFF/RETRY_SCHEDULED next attempt 1 at %s, revision %d", status, due, started.DesiredRevision)
	}
	diags := f.svc.Diagnostics(nativeDriver)
	if len(diags) == 0 || diags[len(diags)-1].Kind != KindStart || diags[len(diags)-1].Outcome != "BACKOFF" {
		t.Fatalf("diagnostics = %+v; want the START ending admitted into BACKOFF", diags)
	}
	issue1371RequireNoLifecycleError(t, f)
	starts := issue1371Calls(f, "start")
	corr := starts[len(starts)-1]
	stopsBefore := len(issue1371Calls(f, "stop"))
	if !f.svc.ObserveNative(corr, NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, NextAttempt: 2, NextAttemptUTC: due.Add(time.Second)}) {
		t.Fatal("the admitted recovery refused its owner's next BACKOFF")
	}
	waitFor(t, "next attempt 2", func() bool { return f.status(nativeDriver).NextAttempt == 2 })

	// RESTART withdraws the recovering generation first, then starts anew.
	f.execs[nativeDriver].setStart(nil)
	f.mustDecide(MethodRestart, nativeDriver)
	f.waitSettled(nativeDriver, drivermanager.ObservedRunning)
	stops := issue1371Calls(f, "stop")[stopsBefore:]
	if len(stops) == 0 || stops[0].Epoch != corr.Epoch || stops[0].Generation != corr.Generation {
		t.Fatalf("RESTART withdrew %+v; want the recovering %s/%d first", stops, corr.Epoch, corr.Generation)
	}
	if last := issue1371Calls(f, "start"); last[len(last)-1].Epoch == corr.Epoch {
		t.Fatalf("RESTART reused the recovering epoch %s", corr.Epoch)
	}
	if got := f.status(nativeDriver); got.NextAttempt != 0 || !got.NextAttemptUTC.IsZero() {
		t.Fatalf("RUNNING view kept a schedule: %+v", got)
	}
}

// Each field the admission checks is varied on its own. A refused outcome is
// a failed start whose generation is withdrawn, on both start paths.
func TestIssue1371MalformedRecoveringStartFailsClosed(t *testing.T) {
	due := time.Date(2026, time.October, 26, 7, 0, 1, 0, time.UTC)
	backoff := func(mutate func(*NativeObservation)) *RecoveringStart {
		start := issue1371Recovering(1, due)
		mutate(&start.Observation)
		return start
	}
	variants := map[string]*RecoveringStart{
		"state RUNNING": backoff(func(o *NativeObservation) {
			o.State, o.Reason, o.NextAttempt, o.NextAttemptUTC = drivermanager.ObservedRunning, drivermanager.ReasonNone, 0, time.Time{}
		}),
		"state STARTING": backoff(func(o *NativeObservation) {
			o.State, o.Reason, o.NextAttempt, o.NextAttemptUTC = drivermanager.ObservedStarting, drivermanager.ReasonStartRequested, 0, time.Time{}
		}),
		"state FAILED": backoff(func(o *NativeObservation) {
			o.State, o.Reason, o.NextAttempt, o.NextAttemptUTC = drivermanager.ObservedFailed, drivermanager.ReasonConfigInvalid, 0, time.Time{}
		}),
		"reason not scheduled":  backoff(func(o *NativeObservation) { o.Reason = drivermanager.ReasonDependencyUnavailable }),
		"quarantined":           backoff(func(o *NativeObservation) { o.Quarantined = true }),
		"ordinal without due":   backoff(func(o *NativeObservation) { o.NextAttemptUTC = time.Time{} }),
		"due without ordinal":   backoff(func(o *NativeObservation) { o.NextAttempt = 0 }),
		"negative ordinal":      backoff(func(o *NativeObservation) { o.NextAttempt = -1 }),
		"no observation at all": {},
	}
	for name, outcome := range variants {
		outcome := outcome
		t.Run("process start/"+name, func(t *testing.T) {
			f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver)), setup: func(f *svcFixture) {
				f.execs[nativeDriver].setStart(func(context.Context, LifecycleCorrelation) error { return outcome })
			}})
			issue1371RequireWithdrawnFailure(t, f)
		})
		t.Run("typed start/"+name, func(t *testing.T) {
			f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
			f.waitSettled(nativeDriver, drivermanager.ObservedRunning)
			f.mustDecide(MethodStop, nativeDriver)
			f.waitSettled(nativeDriver, drivermanager.ObservedStopped)
			f.execs[nativeDriver].setStart(func(context.Context, LifecycleCorrelation) error { return outcome })
			f.mustDecide(MethodStart, nativeDriver)
			issue1371RequireWithdrawnFailure(t, f)
		})
	}
	// Positive control: the unmutated outcome is admitted.
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver)), setup: func(f *svcFixture) {
		f.execs[nativeDriver].setStart(func(context.Context, LifecycleCorrelation) error { return issue1371Recovering(1, due) })
	}})
	nativeWaitStatus(t, f, drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled)
}

func issue1371RequireWithdrawnFailure(t *testing.T, f *svcFixture) {
	t.Helper()
	status := nativeWaitStatus(t, f, drivermanager.ObservedFailed, drivermanager.ReasonInternalError)
	if status.NextAttempt != 0 {
		t.Fatalf("FAILED view kept a schedule: %+v", status)
	}
	starts := issue1371Calls(f, "start")
	last := starts[len(starts)-1]
	waitFor(t, "the refused generation withdrawn", func() bool {
		for _, stop := range issue1371Calls(f, "stop") {
			if stop.Epoch == last.Epoch && stop.Generation == last.Generation {
				return true
			}
		}
		return false
	})
	if d := f.svc.drivers[nativeDriver]; func() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.live != nil }() {
		t.Fatal("a refused recovery was admitted as the live generation")
	}
}
