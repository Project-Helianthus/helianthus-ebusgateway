package runtimecontrol

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

// Issue #1369: an executor BACKOFF carries its schedule (the next attempt's
// ordinal and due time) into the service view, so List and Get show when the
// persistent recovery tries again. The schedule exists only in BACKOFF.

func TestNativeObservationBackoffScheduleReachesServiceView(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver), "fam.beta")})
	corr := nativeLive(t, f)
	f.waitSettled("fam.beta", drivermanager.ObservedRunning)
	due := time.Date(2026, time.October, 25, 18, 32, 25, 0, time.FixedZone("CET", 3600))

	for _, step := range []NativeObservation{
		{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, NextAttempt: 6, NextAttemptUTC: due},
		// Only the schedule changes: the next series step is still published.
		{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, NextAttempt: 7, NextAttemptUTC: due.Add(2 * time.Second)},
	} {
		if !f.svc.ObserveNative(corr, step) {
			t.Fatalf("ObserveNative(%+v) refused", step)
		}
		var status DriverStatus
		waitFor(t, "scheduled BACKOFF in the view", func() bool {
			status = f.status(nativeDriver)
			return status.ObservedState == drivermanager.ObservedBackoff && status.NextAttempt == step.NextAttempt
		})
		if !status.NextAttemptUTC.Equal(step.NextAttemptUTC) || status.NextAttemptUTC.Location() != time.UTC {
			t.Fatalf("view next attempt = %d at %s; want %d at %s in UTC", status.NextAttempt, status.NextAttemptUTC, step.NextAttempt, step.NextAttemptUTC.UTC())
		}
		listed := f.svc.List()
		for _, entry := range listed.Drivers {
			switch entry.DriverID {
			case nativeDriver:
				if entry.NextAttempt != step.NextAttempt || !entry.NextAttemptUTC.Equal(step.NextAttemptUTC) {
					t.Fatalf("List entry = %+v; want the scheduled attempt", entry)
				}
			default:
				if entry.NextAttempt != 0 || !entry.NextAttemptUTC.IsZero() {
					t.Fatalf("schedule leaked to %s: %+v", entry.DriverID, entry)
				}
			}
		}
	}

	// Leaving BACKOFF clears the schedule.
	for _, step := range []NativeObservation{
		{State: drivermanager.ObservedStarting, Reason: drivermanager.ReasonStartRequested},
		{State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone},
	} {
		if !f.svc.ObserveNative(corr, step) {
			t.Fatalf("ObserveNative(%+v) refused", step)
		}
		status := nativeWaitStatus(t, f, step.State, step.Reason)
		if status.NextAttempt != 0 || !status.NextAttemptUTC.IsZero() {
			t.Fatalf("%s view kept a schedule: %+v", step.State, status)
		}
	}
}

// Each schedule field is varied on its own against each state: a schedule
// outside BACKOFF, an ordinal without a due time and a due time without an
// ordinal are refused before they can be queued.
func TestNativeObservationRefusesIncoherentSchedule(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver))})
	corr := nativeLive(t, f)
	due := time.Date(2026, time.October, 25, 18, 32, 25, 0, time.UTC)
	for name, obs := range map[string]NativeObservation{
		"ordinal without due time": {State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, NextAttempt: 3},
		"due time without ordinal": {State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, NextAttemptUTC: due},
		"negative ordinal":         {State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, NextAttempt: -1, NextAttemptUTC: due},
		"RUNNING with schedule":    {State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone, NextAttempt: 3, NextAttemptUTC: due},
		"STARTING with schedule":   {State: drivermanager.ObservedStarting, Reason: drivermanager.ReasonStartRequested, NextAttempt: 3, NextAttemptUTC: due},
		"DEGRADED with schedule":   {State: drivermanager.ObservedDegraded, Reason: drivermanager.ReasonCapabilityDegraded, NextAttempt: 3, NextAttemptUTC: due},
		"FAILED with schedule":     {State: drivermanager.ObservedFailed, Reason: drivermanager.ReasonConfigInvalid, NextAttempt: 3, NextAttemptUTC: due},
		"FAILED with due time":     {State: drivermanager.ObservedFailed, Reason: drivermanager.ReasonConfigInvalid, NextAttemptUTC: due},
	} {
		if f.svc.ObserveNative(corr, obs) {
			t.Fatalf("%s: observation queued", name)
		}
	}
	// Positive control: the coherent schedule is accepted.
	if !f.svc.ObserveNative(corr, NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, NextAttempt: 3, NextAttemptUTC: due}) {
		t.Fatal("coherent BACKOFF schedule refused")
	}
	nativeConsumed(t, f)
}
