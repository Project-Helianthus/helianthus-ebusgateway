package runtimecontrol

// Issue #1375: the cached DriverStatus carries the failure that scheduled a
// BACKOFF retry (its closed class and its address-free cause text) beside the
// schedule, and DriverStatus.Recovery renders the recovery view that the MCP
// runtime tools and the Portal publish. The view exists only while the driver
// is in BACKOFF with a schedule.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

var issue1375Due = time.Date(2000, time.January, 1, 0, 0, 8, 0, time.UTC)

func issue1375Backoff(mutate func(*DriverStatus)) DriverStatus {
	status := DriverStatus{
		DriverID: "ebus.primary", Enabled: true, DesiredState: DesiredRunning, DesiredRevision: 7, Revision: 29,
		ObservedState: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled,
		NextAttempt: 3, NextAttemptUTC: issue1375Due,
		LastFailureClass: drivermanager.FailureDialFailed, LastFailureCause: "dial: connection refused",
	}
	if mutate != nil {
		mutate(&status)
	}
	return status
}

func TestIssue1375RecoveryViewMirrorsTheContractFixture(t *testing.T) {
	view, ok := issue1375Backoff(nil).Recovery()
	want := RecoveryView{Attempt: 3, NextAttemptAt: "2000-01-01T00:00:08Z", FailureClass: drivermanager.FailureDialFailed, FailureCause: "dial: connection refused"}
	if !ok || view != want {
		t.Fatalf("Recovery() = %+v, %t; want %+v", view, ok, want)
	}
}

// Every field of the entry that decides the view is varied on its own.
func TestIssue1375RecoveryViewExistsOnlyInBackoffWithASchedule(t *testing.T) {
	for _, state := range []drivermanager.ObservedState{
		drivermanager.ObservedDisabled, drivermanager.ObservedStopped, drivermanager.ObservedStarting, drivermanager.ObservedRunning,
		drivermanager.ObservedDegraded, drivermanager.ObservedStopping, drivermanager.ObservedFailed,
	} {
		// The schedule fields and the reason RETRY_SCHEDULED are set: neither admits
		// the view outside BACKOFF.
		if view, ok := issue1375Backoff(func(s *DriverStatus) { s.ObservedState = state }).Recovery(); ok {
			t.Errorf("%s with a schedule has a recovery view: %+v", state, view)
		}
	}
	for name, mutate := range map[string]func(*DriverStatus){
		"attempt zero":             func(s *DriverStatus) { s.NextAttempt = 0 },
		"attempt negative":         func(s *DriverStatus) { s.NextAttempt = -3 },
		"no due time":              func(s *DriverStatus) { s.NextAttemptUTC = time.Time{} },
		"year beyond 9999":         func(s *DriverStatus) { s.NextAttemptUTC = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
		"no schedule at all":       func(s *DriverStatus) { s.NextAttempt, s.NextAttemptUTC = 0, time.Time{} },
		"RUNNING, RETRY_SCHEDULED": func(s *DriverStatus) { s.ObservedState = drivermanager.ObservedRunning },
	} {
		if view, ok := issue1375Backoff(mutate).Recovery(); ok {
			t.Errorf("%s: recovery view %+v; want none", name, view)
		}
	}
	// The state decides, not the reason code: a BACKOFF whose reason is not
	// RETRY_SCHEDULED still publishes its schedule.
	if _, ok := issue1375Backoff(func(s *DriverStatus) { s.Reason = drivermanager.ReasonNone }).Recovery(); !ok {
		t.Error("a BACKOFF with a schedule lost its view because of its reason code")
	}
}

func TestIssue1375RecoveryViewFormatsTheDueTimeInUTC(t *testing.T) {
	cet := time.FixedZone("CET", 3600)
	for name, tc := range map[string]struct {
		due  time.Time
		want string
	}{
		"whole seconds":      {issue1375Due, "2000-01-01T00:00:08Z"},
		"offset zone":        {time.Date(2000, 1, 1, 1, 0, 8, 0, cet), "2000-01-01T00:00:08Z"},
		"fraction":           {issue1375Due.Add(500 * time.Millisecond), "2000-01-01T00:00:08.5Z"},
		"nanosecond":         {issue1375Due.Add(123456789 * time.Nanosecond), "2000-01-01T00:00:08.123456789Z"},
		"leap day":           {time.Date(2000, 2, 29, 23, 59, 59, 0, time.UTC), "2000-02-29T23:59:59Z"},
		"year 1, not offset": {time.Date(1, 1, 1, 0, 0, 1, 0, time.UTC), "0001-01-01T00:00:01Z"},
	} {
		view, ok := issue1375Backoff(func(s *DriverStatus) { s.NextAttemptUTC = tc.due }).Recovery()
		if !ok || view.NextAttemptAt != tc.want {
			t.Errorf("%s: next_attempt_at = %q, %t; want %q", name, view.NextAttemptAt, ok, tc.want)
		}
	}
}

func TestIssue1375RecoveryViewClassAndCause(t *testing.T) {
	for _, class := range []drivermanager.FailureClass{drivermanager.FailureDialFailed, drivermanager.FailureTimeout, drivermanager.FailureTransportClosed, drivermanager.FailureOther} {
		view, ok := issue1375Backoff(func(s *DriverStatus) { s.LastFailureClass = class }).Recovery()
		if !ok || view.FailureClass != class {
			t.Errorf("class %q: view %+v, %t", class, view, ok)
		}
	}
	// No class or an unlisted one is OTHER, and the schedule is still published.
	for _, class := range []drivermanager.FailureClass{"", "dial_failed", "NONE", "DIAL_FAILED "} {
		view, ok := issue1375Backoff(func(s *DriverStatus) { s.LastFailureClass = class }).Recovery()
		if !ok || view.FailureClass != drivermanager.FailureOther || view.Attempt != 3 {
			t.Errorf("class %q: view %+v, %t; want OTHER with the schedule", class, view, ok)
		}
	}
	// The cause is checked again at the view, whatever produced the status.
	view, _ := issue1375Backoff(func(s *DriverStatus) {
		s.LastFailureCause = "dial tcp 192.0.2.10:9000: connection refused\n" + strings.Repeat("x", 300)
	}).Recovery()
	if len(view.FailureCause) > MaxFailureCauseChars || contractRejectsCause(view.FailureCause) || !strings.HasPrefix(view.FailureCause, "dial tcp <redacted> connection refused x") {
		t.Errorf("cause = %q; want it redacted and bounded to %d characters", view.FailureCause, MaxFailureCauseChars)
	}
	if view, _ := issue1375Backoff(func(s *DriverStatus) { s.LastFailureCause = "" }).Recovery(); view.FailureCause != "" {
		t.Errorf("empty cause = %q", view.FailureCause)
	}
}

func issue1375Failure(attempt int, class drivermanager.FailureClass, cause string) NativeObservation {
	return NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled,
		NextAttempt: attempt, NextAttemptUTC: issue1375Due.Add(time.Duration(attempt) * time.Second),
		FailureClass: class, FailureCause: cause}
}

func TestIssue1375NativeObservationFailureReachesTheServiceView(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver), "fam.beta")})
	corr := nativeLive(t, f)
	f.waitSettled("fam.beta", drivermanager.ObservedRunning)
	for _, step := range []NativeObservation{
		issue1375Failure(1, drivermanager.FailureTransportClosed, "connection lost (adapter_connection_lost)"),
		issue1375Failure(2, drivermanager.FailureDialFailed, "connect: connection refused"),
		issue1375Failure(3, drivermanager.FailureTimeout, "i/o timeout"),
		// The service re-checks the cause: an address that reached it is redacted.
		issue1375Failure(4, drivermanager.FailureOther, "dial tcp 192.0.2.10:9000: connection refused"),
	} {
		if !f.svc.ObserveNative(corr, step) {
			t.Fatalf("ObserveNative(%+v) refused", step)
		}
		var status DriverStatus
		waitFor(t, "BACKOFF attempt in the view", func() bool {
			status = f.status(nativeDriver)
			return status.ObservedState == drivermanager.ObservedBackoff && status.NextAttempt == step.NextAttempt
		})
		wantCause := sanitizeFailureCause(step.FailureCause)
		if status.LastFailureClass != step.FailureClass || status.LastFailureCause != wantCause {
			t.Fatalf("attempt %d view failure = %q/%q; want %q/%q", step.NextAttempt, status.LastFailureClass, status.LastFailureCause, step.FailureClass, wantCause)
		}
		view, ok := status.Recovery()
		if !ok || view.Attempt != step.NextAttempt || view.FailureClass != step.FailureClass || view.FailureCause != wantCause {
			t.Fatalf("attempt %d recovery view = %+v, %t", step.NextAttempt, view, ok)
		}
		for _, entry := range f.svc.List().Drivers {
			if entry.DriverID != nativeDriver && (entry.LastFailureClass != "" || entry.LastFailureCause != "") {
				t.Fatalf("failure leaked to %s: %+v", entry.DriverID, entry)
			}
		}
	}
	if got := f.status(nativeDriver).LastFailureCause; strings.Contains(got, "192.0.2.10") {
		t.Fatalf("view cause = %q names an address", got)
	}
	// Leaving BACKOFF clears the failure with the schedule.
	for _, step := range []NativeObservation{
		{State: drivermanager.ObservedStarting, Reason: drivermanager.ReasonStartRequested},
		{State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone},
	} {
		if !f.svc.ObserveNative(corr, step) {
			t.Fatalf("ObserveNative(%+v) refused", step)
		}
		status := nativeWaitStatus(t, f, step.State, step.Reason)
		if status.LastFailureClass != "" || status.LastFailureCause != "" {
			t.Fatalf("%s view kept a failure: %+v", step.State, status)
		}
		if view, ok := status.Recovery(); ok {
			t.Fatalf("%s has a recovery view: %+v", step.State, view)
		}
	}
	// A BACKOFF that reports no failure shows none: nothing is carried over.
	if !f.svc.ObserveNative(corr, issue1375Failure(5, "", "")) {
		t.Fatal("BACKOFF without a failure refused")
	}
	status := nativeWaitStatus(t, f, drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled)
	if status.LastFailureClass != "" || status.LastFailureCause != "" || status.NextAttempt != 5 {
		t.Fatalf("view = %+v; want the schedule without a failure", status)
	}
}

// A failure outside a BACKOFF, a cause without its class and an unlisted class
// are refused before they can be queued.
func TestIssue1375NativeObservationRefusesAnIncoherentFailure(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
	corr := nativeLive(t, f)
	for name, obs := range map[string]NativeObservation{
		"RUNNING with a class":   {State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone, FailureClass: drivermanager.FailureOther},
		"RUNNING with a cause":   {State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone, FailureCause: "x"},
		"STARTING with a class":  {State: drivermanager.ObservedStarting, Reason: drivermanager.ReasonStartRequested, FailureClass: drivermanager.FailureTimeout},
		"DEGRADED with a class":  {State: drivermanager.ObservedDegraded, Reason: drivermanager.ReasonCapabilityDegraded, FailureClass: drivermanager.FailureTimeout},
		"FAILED with a class":    {State: drivermanager.ObservedFailed, Reason: drivermanager.ReasonConfigInvalid, FailureClass: drivermanager.FailureOther},
		"FAILED with a cause":    {State: drivermanager.ObservedFailed, Reason: drivermanager.ReasonConfigInvalid, FailureCause: "x"},
		"BACKOFF cause no class": {State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, FailureCause: "x"},
		"BACKOFF unlisted class": {State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, FailureClass: "dial_failed"},
	} {
		if f.svc.ObserveNative(corr, obs) {
			t.Errorf("%s: observation queued", name)
		}
	}
	// Positive controls: the class alone, and a BACKOFF without a schedule.
	for name, obs := range map[string]NativeObservation{
		"class only":               {State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, FailureClass: drivermanager.FailureOther},
		"failure without schedule": {State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, FailureClass: drivermanager.FailureTimeout, FailureCause: "i/o timeout"},
		"full coherent":            issue1375Failure(2, drivermanager.FailureDialFailed, "connect: connection refused"),
		"only the symbol":          issue1375Failure(3, drivermanager.FailureTransportClosed, ""),
	} {
		if !f.svc.ObserveNative(corr, obs) {
			t.Errorf("%s: coherent observation refused", name)
		}
		nativeConsumed(t, f)
	}
}

// The opening START admitted into the native recovery carries its failure in
// the BACKOFF it publishes instead of RUNNING.
func TestIssue1375RecoveringStartPublishesItsFailure(t *testing.T) {
	recovering := func(class drivermanager.FailureClass, cause string) *RecoveringStart {
		return &RecoveringStart{Observation: issue1375Failure(1, class, cause)}
	}
	t.Run("process start", func(t *testing.T) {
		f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver), "fam.beta"), setup: func(f *svcFixture) {
			f.execs[nativeDriver].setStart(func(context.Context, LifecycleCorrelation) error {
				return recovering(drivermanager.FailureDialFailed, "dial tcp 192.0.2.10:9000: connection refused")
			})
		}})
		status := nativeWaitStatus(t, f, drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled)
		if status.LastFailureClass != drivermanager.FailureDialFailed || status.LastFailureCause != "dial tcp <redacted> connection refused" {
			t.Fatalf("view failure = %q/%q; want DIAL_FAILED with the address redacted", status.LastFailureClass, status.LastFailureCause)
		}
		if view, ok := status.Recovery(); !ok || view.Attempt != 1 {
			t.Fatalf("recovery view = %+v, %t", view, ok)
		}
	})

	t.Run("typed START", func(t *testing.T) {
		f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
		f.waitSettled(nativeDriver, drivermanager.ObservedRunning)
		f.mustDecide(MethodStop, nativeDriver)
		f.waitSettled(nativeDriver, drivermanager.ObservedStopped)
		f.execs[nativeDriver].setStart(func(context.Context, LifecycleCorrelation) error {
			return recovering(drivermanager.FailureTimeout, "ebus: no response within timeout window")
		})
		f.mustDecide(MethodStart, nativeDriver)
		f.waitSettled(nativeDriver, drivermanager.ObservedBackoff)
		status := f.status(nativeDriver)
		if status.LastFailureClass != drivermanager.FailureTimeout || status.LastFailureCause != "ebus: no response within timeout window" {
			t.Fatalf("view failure = %q/%q; want TIMEOUT with its cause", status.LastFailureClass, status.LastFailureCause)
		}
		// STOP leaves nothing behind.
		f.mustDecide(MethodStop, nativeDriver)
		f.waitSettled(nativeDriver, drivermanager.ObservedStopped)
		if stopped := f.status(nativeDriver); stopped.LastFailureClass != "" || stopped.LastFailureCause != "" || stopped.NextAttempt != 0 {
			t.Fatalf("STOPPED view kept a failure: %+v", stopped)
		}
	})

	t.Run("incoherent failure fails the start closed", func(t *testing.T) {
		f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver), "fam.beta"), setup: func(f *svcFixture) {
			f.execs[nativeDriver].setStart(func(context.Context, LifecycleCorrelation) error {
				return &RecoveringStart{Observation: issue1375Failure(1, "dial_failed", "x")}
			})
		}})
		f.waitSettled(nativeDriver, drivermanager.ObservedFailed)
		if status := f.status(nativeDriver); status.LastFailureClass != "" || status.LastFailureCause != "" {
			t.Fatalf("FAILED view carries a failure: %+v", status)
		}
	})
}
