package main

// Issue #1375: the Portal runtime view shows when a driver in backoff tries
// again. The row of a driver the service reports in BACKOFF with a schedule
// carries the same recovery object as the MCP runtime tools; every other state
// has none.

import (
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func issue1375PortalStatus(mutate func(*runtimecontrol.DriverStatus)) runtimecontrol.DriverStatus {
	status := runtimecontrol.DriverStatus{
		DriverID: managedSunSpecID, Enabled: true, DesiredState: runtimecontrol.DesiredRunning,
		ObservedState: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled,
		NextAttempt: 3, NextAttemptUTC: time.Date(2000, 1, 1, 1, 0, 8, 0, time.FixedZone("CET", 3600)),
		LastFailureClass: drivermanager.FailureDialFailed, LastFailureCause: "dial: connection refused",
	}
	if mutate != nil {
		mutate(&status)
	}
	return status
}

func issue1375PortalRow(t *testing.T, status runtimecontrol.DriverStatus, observed bool) portal.ConfigDriverState {
	t.Helper()
	id := managedSunSpecID
	runtime := &gatewayPortalConfigRuntime{applied: map[runtimecontrol.DriverID]portalConfigApplication{id: {host: true, enabled: true}}}
	record := &portalconfig.Record{Format: portalconfig.Format, Revision: "1", Drivers: []portalconfig.Driver{{ID: string(id), Enabled: true}}}
	states := runtime.driverStates(record, func(runtimecontrol.DriverID) (runtimecontrol.DriverStatus, bool) { return status, observed })
	if len(states) != 1 {
		t.Fatalf("rows = %d; want 1", len(states))
	}
	return states[0]
}

func TestIssue1375PortalRowShowsTheScheduleOfADriverInBackoff(t *testing.T) {
	row := issue1375PortalRow(t, issue1375PortalStatus(nil), true)
	want := portal.ConfigDriverRecovery{Attempt: 3, NextAttemptAt: "2000-01-01T00:00:08Z", LastFailureClass: "DIAL_FAILED", LastFailureCause: "dial: connection refused"}
	if row.Recovery == nil || *row.Recovery != want {
		t.Fatalf("recovery = %+v; want %+v (due time in UTC)", row.Recovery, want)
	}
	if row.Status != "pending" || row.Reason != "backoff" || row.Effective {
		t.Fatalf("row = %+v; want pending/backoff, not effective", row)
	}
}

// Every field of the entry that decides the member is varied on its own, and
// the state is varied over all eight.
func TestIssue1375PortalRowHasNoScheduleOutsideBackoff(t *testing.T) {
	for _, state := range []drivermanager.ObservedState{
		drivermanager.ObservedDisabled, drivermanager.ObservedStopped, drivermanager.ObservedStarting, drivermanager.ObservedRunning,
		drivermanager.ObservedDegraded, drivermanager.ObservedStopping, drivermanager.ObservedFailed,
	} {
		row := issue1375PortalRow(t, issue1375PortalStatus(func(s *runtimecontrol.DriverStatus) { s.ObservedState = state }), true)
		if row.Recovery != nil {
			t.Errorf("%s with a leftover schedule: recovery %+v", state, row.Recovery)
		}
	}
	for name, mutate := range map[string]func(*runtimecontrol.DriverStatus){
		"no attempt":  func(s *runtimecontrol.DriverStatus) { s.NextAttempt = 0 },
		"no due time": func(s *runtimecontrol.DriverStatus) { s.NextAttemptUTC = time.Time{} },
	} {
		if row := issue1375PortalRow(t, issue1375PortalStatus(mutate), true); row.Recovery != nil {
			t.Errorf("%s: recovery %+v", name, row.Recovery)
		}
	}
	if row := issue1375PortalRow(t, issue1375PortalStatus(nil), false); row.Recovery != nil {
		t.Errorf("no readback: recovery %+v", row.Recovery)
	}
}

func TestIssue1375PortalRowFollowsTheClassAndTheCause(t *testing.T) {
	for _, class := range []drivermanager.FailureClass{drivermanager.FailureDialFailed, drivermanager.FailureTimeout, drivermanager.FailureTransportClosed, drivermanager.FailureOther, ""} {
		row := issue1375PortalRow(t, issue1375PortalStatus(func(s *runtimecontrol.DriverStatus) { s.LastFailureClass = class }), true)
		want := string(class)
		if class == "" {
			want = "OTHER"
		}
		if row.Recovery == nil || row.Recovery.LastFailureClass != want {
			t.Errorf("class %q: recovery %+v; want %s", class, row.Recovery, want)
		}
	}
	row := issue1375PortalRow(t, issue1375PortalStatus(func(s *runtimecontrol.DriverStatus) {
		s.LastFailureCause = "dial tcp 192.0.2.10:9000: connection refused " + strings.Repeat("x", 200)
	}), true)
	if row.Recovery == nil || len(row.Recovery.LastFailureCause) > 160 || strings.Contains(row.Recovery.LastFailureCause, "192.0.2.10") {
		t.Fatalf("cause = %+v; want it redacted and bounded to 160 characters", row.Recovery)
	}
}
