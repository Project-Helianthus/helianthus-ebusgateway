package drivermanager

// Issue #1375: a retry that DriverManager schedules reports the failure that
// scheduled it. The driver's classifier supplies the detail (a closed class and
// the address-free cause text) with the failure of a start or replacement
// attempt, and the reporter of a lost connection supplies it with the
// reported Failure. The manager stores it in RetrySnapshot.LastFailure beside
// the schedule and never interprets it: it takes no part in any retry
// decision. A class outside the closed set is stored as FailureOther, and a
// failure without a detail leaves the zero value.

import (
	"context"
	"errors"
	"testing"
	"time"
)

var (
	errDetailDial    = errors.New("synthetic dial failure")
	errDetailTimeout = errors.New("synthetic attempt timeout")
	errDetailUnknown = errors.New("synthetic unclassified failure")
	errDetailBogus   = errors.New("synthetic failure with an unlisted class")
	errDetailBare    = errors.New("synthetic failure without detail")
	errDetailCause   = errors.New("synthetic failure with a cause only")
)

// detailOf is the scripted classifier: every error has its own detail.
func detailOf(err error) FailureDetail {
	switch {
	case errors.Is(err, errDetailDial):
		return FailureDetail{Class: FailureDialFailed, Cause: "connect: connection refused"}
	case errors.Is(err, errDetailTimeout):
		return FailureDetail{Class: FailureTimeout, Cause: "i/o timeout"}
	case errors.Is(err, errDetailUnknown):
		return FailureDetail{Class: FailureOther, Cause: "start failed"}
	case errors.Is(err, errDetailBogus):
		return FailureDetail{Class: FailureClass("NOT_A_CLASS"), Cause: "bogus class"}
	case errors.Is(err, errDetailCause):
		return FailureDetail{Cause: "cause without class"}
	}
	return FailureDetail{}
}

func newDetailManager(t *testing.T, runtime Runtime, clock *persistentClock, policy RetryPolicy) *Manager {
	t.Helper()
	manager, err := New(Config{
		Now:         clock.Now,
		RetryJitter: clock.Jitter,
		RetryWait:   clock.Wait,
		Drivers: []DriverConfig{{
			ID: "ebus.primary", Enabled: true, Runtime: runtime,
			Capabilities: []Capability{CapabilityRead},
			ClassifyError: func(err error) Failure {
				return Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}, Detail: detailOf(err)}
			},
			Retry: policy,
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	clock.manager = manager
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	return manager
}

func TestIssue1375RetrySnapshotReportsTheFailureThatScheduledIt(t *testing.T) {
	// The reported loss schedules retry 1. Replacement attempts 1..6 fail with
	// the errors below, so retries 2..7 each report their own attempt's
	// failure, across the fast budget (five) and into the persistent phase.
	failures := []error{errDetailDial, errDetailTimeout, errDetailUnknown, errDetailBogus, errDetailBare, errDetailCause}
	want := []FailureDetail{
		{Class: FailureTransportClosed, Cause: "connection lost (adapter_connection_lost)"}, // the reported loss
		{Class: FailureDialFailed, Cause: "connect: connection refused"},
		{Class: FailureTimeout, Cause: "i/o timeout"},
		{Class: FailureOther, Cause: "start failed"},
		{Class: FailureOther, Cause: "bogus class"}, // an unlisted class is stored as OTHER
		{}, // no detail from the classifier: the zero value
		{Class: FailureOther, Cause: "cause without class"},
	}
	clock := newPersistentClock()
	runtime := &persistentRuntime{outcome: func(replace int) error {
		if replace <= len(failures) {
			return failures[replace-1]
		}
		return nil
	}}
	manager := newDetailManager(t, runtime, clock, persistentPolicy())
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	loss := Failure{
		Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true},
		Detail: FailureDetail{Class: FailureTransportClosed, Cause: "connection lost (adapter_connection_lost)"},
	}
	if !manager.ReportFailure("ebus.primary", Correlation{Generation: running.Generation}, loss) {
		t.Fatal("ReportFailure refused the stable RUNNING correlation")
	}
	recovered := requireObservedState(t, manager, "ebus.primary", ObservedRunning, 5*time.Second)
	if recovered.Retry != nil {
		t.Fatalf("recovered snapshot keeps a retry: %#v", recovered.Retry)
	}

	retries := clock.recorded()
	if len(retries) != len(want) {
		t.Fatalf("scheduled retries = %d; want %d", len(retries), len(want))
	}
	for n, retry := range retries {
		schedule := retry.snapshot.Retry
		if schedule == nil || schedule.Attempt != n+1 {
			t.Fatalf("retry %d schedule = %#v; want attempt %d", n+1, schedule, n+1)
		}
		if schedule.LastFailure != want[n] {
			t.Fatalf("retry %d last failure = %#v; want %#v", n+1, schedule.LastFailure, want[n])
		}
	}
}

func TestIssue1375FailureDetailDoesNotChangeTheSchedule(t *testing.T) {
	// Two runs with the same failures differ only in the detail the classifier
	// attaches: the attempts, delays and not-before instants are identical.
	run := func(classify func(error) FailureDetail) []persistentRetry {
		clock := newPersistentClock()
		runtime := &persistentRuntime{outcome: func(replace int) error {
			if replace <= 7 {
				return errDetailDial
			}
			return nil
		}}
		manager, err := New(Config{
			Now: clock.Now, RetryJitter: clock.Jitter, RetryWait: clock.Wait,
			Drivers: []DriverConfig{{
				ID: "ebus.primary", Enabled: true, Runtime: runtime, Capabilities: []Capability{CapabilityRead},
				ClassifyError: func(err error) Failure {
					return Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}, Detail: classify(err)}
				},
				Retry: persistentPolicy(),
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		clock.manager = manager
		t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
		if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
			t.Fatal(err)
		}
		running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
		manager.ReportFailure("ebus.primary", Correlation{Generation: running.Generation}, Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}})
		requireObservedState(t, manager, "ebus.primary", ObservedRunning, 5*time.Second)
		return clock.recorded()
	}
	with := run(detailOf)
	without := run(func(error) FailureDetail { return FailureDetail{} })
	if len(with) != len(without) || len(with) == 0 {
		t.Fatalf("retries = %d with detail, %d without", len(with), len(without))
	}
	for n := range with {
		a, b := with[n], without[n]
		if a.delay != b.delay || a.snapshot.Retry.Attempt != b.snapshot.Retry.Attempt ||
			a.snapshot.Retry.BudgetRemaining != b.snapshot.Retry.BudgetRemaining ||
			!a.snapshot.Retry.NotBeforeUTC.Equal(b.snapshot.Retry.NotBeforeUTC) {
			t.Fatalf("retry %d schedule differs with a detail: %#v vs %#v", n+1, a.snapshot.Retry, b.snapshot.Retry)
		}
	}
}

func TestIssue1375NoRetrySnapshotOutsideBackoffCarriesADetail(t *testing.T) {
	// A terminal failure schedules nothing, so its FAILED snapshot has no retry
	// and therefore no detail, whatever the classifier attached.
	clock := newPersistentClock()
	runtime := &persistentRuntime{outcome: func(int) error { return errPersistentConfig }}
	manager, err := New(Config{
		Now: clock.Now, RetryJitter: clock.Jitter, RetryWait: clock.Wait,
		Drivers: []DriverConfig{{
			ID: "ebus.primary", Enabled: true, Runtime: runtime, Capabilities: []Capability{CapabilityRead},
			ClassifyError: func(error) Failure {
				return Failure{Reason: Reason{Code: ReasonConfigInvalid}, Detail: FailureDetail{Class: FailureOther, Cause: "terminal"}}
			},
			Retry: persistentPolicy(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	clock.manager = manager
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
		t.Fatal(err)
	}
	running := requireObservedState(t, manager, "ebus.primary", ObservedRunning, time.Second)
	manager.ReportFailure("ebus.primary", Correlation{Generation: running.Generation}, Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}})
	failed := requireObservedState(t, manager, "ebus.primary", ObservedFailed, 5*time.Second)
	if failed.Retry != nil {
		t.Fatalf("FAILED snapshot carries a retry: %#v", failed.Retry)
	}
}

func TestIssue1375FailureClassClosedSet(t *testing.T) {
	for class, valid := range map[FailureClass]bool{
		FailureDialFailed: true, FailureTimeout: true, FailureTransportClosed: true, FailureOther: true,
		"": false, "dial_failed": false, "NONE": false, "DIAL_FAILED ": false,
	} {
		if class.Valid() != valid {
			t.Errorf("FailureClass(%q).Valid() = %t; want %t", class, class.Valid(), valid)
		}
	}
	if len(map[FailureClass]bool{FailureDialFailed: true, FailureTimeout: true, FailureTransportClosed: true, FailureOther: true}) != 4 {
		t.Fatal("failure classes are not four distinct values")
	}
}
