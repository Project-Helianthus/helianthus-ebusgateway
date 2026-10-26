package drivermanager

// Issue #1371: a lifecycle bridge admits a failed opening start into the
// persistent recovery only when the manager says that the scheduled retry
// belongs to a persistent series. RetrySnapshot.Persistent carries that: it
// is true for every retry of a series under a persistent policy, in the fast
// phase and after it, and false under a bounded policy.

import (
	"context"
	"sync"
	"testing"
	"time"
)

// alwaysFailingRuntime fails every construction with a retryable error.
type alwaysFailingRuntime struct{ persistentRuntime }

func (*alwaysFailingRuntime) Start(context.Context) (uint64, error)   { return 0, errPersistentDial }
func (*alwaysFailingRuntime) Replace(context.Context) (uint64, error) { return 0, errPersistentDial }

func TestIssue1371RetrySnapshotReportsThePersistentSeries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		persistent bool
		// retries is how many scheduled retries the wait lets run before it
		// stops the series by refusing the next one.
		retries int
		want    int
	}{
		{"persistent policy, fast and persistent phases", true, 4, 5},
		{"bounded policy", false, 4, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var schedules []RetrySnapshot
			var manager *Manager
			wait := func(ctx context.Context, _ time.Duration) bool {
				snapshot, _ := manager.Snapshot("ebus.primary")
				mu.Lock()
				defer mu.Unlock()
				if snapshot.Retry != nil {
					schedules = append(schedules, *snapshot.Retry)
				}
				return len(schedules) <= tc.retries && ctx.Err() == nil
			}
			policy := RetryPolicy{Budget: 2, InitialDelay: time.Second, MaxDelay: 2 * time.Second, JitterRatio: -1}
			if tc.persistent {
				policy.Persistent = PersistentRetry{Enabled: true, InitialDelay: time.Second, MaxDelay: 4 * time.Second}
			}
			var err error
			manager, err = New(Config{
				Drivers: []DriverConfig{{
					ID: "ebus.primary", Enabled: true, Runtime: &alwaysFailingRuntime{},
					Capabilities: []Capability{CapabilityRead},
					ClassifyError: func(error) Failure {
						return Failure{Reason: Reason{Code: ReasonDependencyUnavailable, Retryable: true}}
					},
					Retry: policy,
				}},
				RetryWait: wait,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
			if err := manager.Start(context.Background(), "ebus.primary"); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				mu.Lock()
				got := len(schedules)
				mu.Unlock()
				snapshot, _ := manager.Snapshot("ebus.primary")
				if got >= tc.want && (snapshot.ObservedState == ObservedBackoff || snapshot.ObservedState == ObservedFailed) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("scheduled retries = %d; want %d (state %s)", got, tc.want, snapshot.ObservedState)
				}
				time.Sleep(time.Millisecond)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(schedules) != tc.want {
				t.Fatalf("scheduled retries = %d; want %d", len(schedules), tc.want)
			}
			for n, schedule := range schedules {
				if schedule.Attempt != n+1 || schedule.Persistent != tc.persistent {
					t.Fatalf("retry %d schedule = %+v; want attempt %d with Persistent=%t", n+1, schedule, n+1, tc.persistent)
				}
			}
			if !tc.persistent {
				if snapshot, _ := manager.Snapshot("ebus.primary"); snapshot.ObservedState != ObservedFailed || snapshot.Reason.Code != ReasonRetryExhausted {
					t.Fatalf("bounded series ended %s/%s; want FAILED/RETRY_EXHAUSTED", snapshot.ObservedState, snapshot.Reason.Code)
				}
			}
		})
	}
}
