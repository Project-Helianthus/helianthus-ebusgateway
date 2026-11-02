package main

// Issue #1375: the bridge hands the failure that DriverManager recorded with a
// scheduled retry to the service, beside the schedule, both for the opening
// START admitted into recovery and for the retries that follow RUNNING. The
// service then publishes it in the recovery view.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

func issue1375Snapshot(attempt int, detail drivermanager.FailureDetail) drivermanager.Snapshot {
	due := time.Date(2026, time.October, 26, 7, 0, 1, 0, time.UTC)
	return drivermanager.Snapshot{DriverID: primaryEBusDriverID, DesiredState: drivermanager.DesiredRunning, ObservedState: drivermanager.ObservedBackoff,
		Reason: drivermanager.Reason{Code: drivermanager.ReasonRetryScheduled, Retryable: true}, Revision: 8,
		Retry: &drivermanager.RetrySnapshot{Eligible: true, BudgetRemaining: 5, NotBeforeUTC: due, Attempt: attempt, Persistent: true, LastFailure: detail}}
}

func issue1375Details() []drivermanager.FailureDetail {
	return []drivermanager.FailureDetail{
		{Class: drivermanager.FailureDialFailed, Cause: "connect: connection refused"},
		{Class: drivermanager.FailureTimeout, Cause: "i/o timeout"},
		{Class: drivermanager.FailureTransportClosed, Cause: "connection lost (adapter_connection_lost)"},
		{Class: drivermanager.FailureOther, Cause: "ebus: payload does not match expected schema"},
		// Each field on its own.
		{Class: drivermanager.FailureTimeout, Cause: "connect: connection refused"},
		{Class: drivermanager.FailureDialFailed, Cause: "i/o timeout"},
		{Class: drivermanager.FailureOther},
		{},
	}
}

func issue1375Key() ebusControlKey {
	return ebusControlKey{DriverID: primaryEBusDriverID, SourceID: ctlSource, Epoch: "inc1.ebus.primary.1", Generation: 1}
}

func TestIssue1375BridgeForwardsTheLastFailureOfTheOpeningRecovery(t *testing.T) {
	for _, detail := range issue1375Details() {
		epoch := newEBusControlEpoch(issue1375Key(), nil)
		epoch.launched, epoch.dispatched, epoch.dispatchRevision = true, true, 5
		bridge := &ebusControlRuntime{id: primaryEBusDriverID, source: ctlSource, current: epoch, history: map[runtimecontrol.SourceEpoch]*ebusControlEpoch{}}
		snapshot := issue1375Snapshot(1, detail)
		want := runtimecontrol.NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled,
			NextAttempt: 1, NextAttemptUTC: snapshot.Retry.NotBeforeUTC, FailureClass: detail.Class, FailureCause: detail.Cause}
		_, obs, ok := bridge.NativeObservation(snapshot)
		if !ok || obs != want {
			t.Fatalf("admitting NativeObservation for %+v = %+v, %t; want %+v", detail, obs, ok, want)
		}
		var recovering *runtimecontrol.RecoveringStart
		if err := bridge.awaitStart(context.Background(), epoch); !errors.As(err, &recovering) || recovering.Observation != want {
			t.Fatalf("joined start for %+v = %v; want the RecoveringStart with the failure", detail, err)
		}
	}
}

func TestIssue1375BridgeForwardsTheLastFailureOfEveryRetry(t *testing.T) {
	for attempt, detail := range issue1375Details() {
		epoch := newEBusControlEpoch(issue1375Key(), nil)
		epoch.native = 1
		close(epoch.startDone)
		bridge := &ebusControlRuntime{id: primaryEBusDriverID, source: ctlSource, current: epoch, history: map[runtimecontrol.SourceEpoch]*ebusControlEpoch{}}
		snapshot := issue1375Snapshot(attempt+1, detail)
		want := runtimecontrol.NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled,
			NextAttempt: attempt + 1, NextAttemptUTC: snapshot.Retry.NotBeforeUTC, FailureClass: detail.Class, FailureCause: detail.Cause}
		if _, obs, ok := bridge.NativeObservation(snapshot); !ok || obs != want {
			t.Fatalf("NativeObservation for %+v = %+v, %t; want %+v", detail, obs, ok, want)
		}
	}

	// Outside BACKOFF nothing of the failure is forwarded, whatever a stale
	// schedule still holds.
	for _, state := range []drivermanager.ObservedState{drivermanager.ObservedStarting, drivermanager.ObservedRunning, drivermanager.ObservedDegraded} {
		epoch := newEBusControlEpoch(issue1375Key(), nil)
		epoch.native = 7
		close(epoch.startDone)
		bridge := &ebusControlRuntime{id: primaryEBusDriverID, source: ctlSource, current: epoch, history: map[runtimecontrol.SourceEpoch]*ebusControlEpoch{}}
		snapshot := issue1375Snapshot(3, drivermanager.FailureDetail{Class: drivermanager.FailureTimeout, Cause: "i/o timeout"})
		snapshot.ObservedState, snapshot.Generation = state, 7
		snapshot.Reason = drivermanager.Reason{Code: drivermanager.ReasonNone}
		if state == drivermanager.ObservedStarting {
			snapshot.Reason = drivermanager.Reason{Code: drivermanager.ReasonStartRequested}
		}
		if state == drivermanager.ObservedDegraded {
			snapshot.Reason = drivermanager.Reason{Code: drivermanager.ReasonCapabilityDegraded}
		}
		_, obs, ok := bridge.NativeObservation(snapshot)
		if !ok || obs.FailureClass != "" || obs.FailureCause != "" || obs.NextAttempt != 0 {
			t.Fatalf("%s observation = %+v, %t; want no schedule and no failure", state, obs, ok)
		}
	}
}
