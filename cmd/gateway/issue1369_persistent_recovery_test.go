package main

// Issue #1369: while ebus.primary is desired RUNNING, its lifecycle owner
// (DriverManager behind the runtime control bridge) never ends a recovery by
// itself. After the five fast attempts the series continues with capped
// exponential backoff; every attempt is logged with its number, phase and
// reason, every failure with its error class and cause text, and the service
// view (the source of helianthus.runtime.list and the Portal readback) shows
// BACKOFF with the next attempt's number and due time. Only a failure that
// retrying cannot heal ends FAILED, with its own reason; an operator RESTART
// during the backoff recovers at once and resets the schedule.
//
// The tests drive the production composition against the scripted loopback
// ENH endpoint of issue1367_recovery_test.go with the production retry
// policy. The AM27 threshold is narrowed as there, and the manager's clock,
// jitter source and retry wait are injected (ebusDriverRetryClock): the wait
// records each scheduled retry, waits until the service view shows it, and
// advances the manual clock by the delay instead of sleeping, so every delay,
// cap and jitter bound is asserted exactly.

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

// issue1369Retry is one scheduled retry seen by the injected retry wait.
type issue1369Retry struct {
	delay time.Duration
	// clock is the manager clock when the retry was scheduled.
	clock    time.Time
	snapshot drivermanager.Snapshot
	// view is the service view once it showed this BACKOFF; viewOK reports
	// that it did within the bound.
	view   runtimecontrol.DriverStatus
	viewOK bool
}

type issue1369Action int

const (
	issue1369Proceed issue1369Action = iota
	// issue1369Block holds the retry wait until it is canceled, like a
	// persistent delay of minutes that an operator action interrupts.
	issue1369Block
)

// issue1369Clock is the injected manager clock, jitter source and retry wait.
type issue1369Clock struct {
	mu       sync.Mutex
	now      time.Time
	sign     func(n int) int
	jitters  int
	retries  []issue1369Retry
	onRetry  func(attempt int) issue1369Action
	process  atomic.Pointer[rctlProcess]
	blocked  chan int
	released atomic.Int32
}

// issue1369NoJitter keeps every delay at its nominal value.
func issue1369NoJitter(int) int { return 0 }

// issue1369ExtremeJitter alternates the largest positive and negative jitter.
func issue1369ExtremeJitter(n int) int {
	if n%2 == 1 {
		return 1
	}
	return -1
}

func newIssue1369Clock(sign func(n int) int) *issue1369Clock {
	return &issue1369Clock{now: time.Now().UTC().Truncate(time.Second), sign: sign, blocked: make(chan int, 4)}
}

func (clock *issue1369Clock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *issue1369Clock) Jitter(limit time.Duration) time.Duration {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.jitters++
	return time.Duration(clock.sign(clock.jitters)) * limit
}

func (clock *issue1369Clock) setOnRetry(fn func(attempt int) issue1369Action) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.onRetry = fn
}

func (clock *issue1369Clock) wait(ctx context.Context, delay time.Duration) bool {
	process := clock.process.Load()
	if process == nil {
		return false
	}
	retry := issue1369Retry{delay: delay, clock: clock.Now(), snapshot: process.control.driver.Snapshot()}
	attempt := 0
	if schedule := retry.snapshot.Retry; schedule != nil {
		attempt = schedule.Attempt
		retry.view, retry.viewOK = issue1369AwaitView(ctx, process, *schedule)
	}
	clock.mu.Lock()
	clock.retries = append(clock.retries, retry)
	onRetry := clock.onRetry
	clock.mu.Unlock()
	if onRetry != nil && onRetry(attempt) == issue1369Block {
		clock.blocked <- attempt
		<-ctx.Done()
		clock.released.Add(1)
		return false
	}
	clock.mu.Lock()
	clock.now = clock.now.Add(delay)
	clock.mu.Unlock()
	return ctx.Err() == nil
}

// issue1369AwaitView waits until the service view shows the scheduled
// BACKOFF: the next attempt's number and due time.
func issue1369AwaitView(ctx context.Context, process *rctlProcess, schedule drivermanager.RetrySnapshot) (runtimecontrol.DriverStatus, bool) {
	deadline := time.Now().Add(rctlBound)
	for {
		status, err := process.control.Service().Get(primaryEBusDriverID)
		if err == nil && status.ObservedState == drivermanager.ObservedBackoff && status.Reason == drivermanager.ReasonRetryScheduled &&
			status.NextAttempt == schedule.Attempt && status.NextAttemptUTC.Equal(schedule.NotBeforeUTC) {
			return status, true
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return status, false
		}
		time.Sleep(time.Millisecond)
	}
}

func (clock *issue1369Clock) recorded() []issue1369Retry {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return append([]issue1369Retry(nil), clock.retries...)
}

func (clock *issue1369Clock) waitRetries(t *testing.T, n int, logs *issue1367Logs) []issue1369Retry {
	t.Helper()
	deadline := time.Now().Add(3 * rctlBound)
	for {
		if retries := clock.recorded(); len(retries) >= n {
			return retries
		}
		if time.Now().After(deadline) {
			t.Fatalf("scheduled retries = %d; want %d; recovery log:\n%s", len(clock.recorded()), n, issue1367RecoveryLines(logs.text()))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (clock *issue1369Clock) waitBlocked(t *testing.T, attempt int, logs *issue1367Logs) {
	t.Helper()
	select {
	case got := <-clock.blocked:
		if got != attempt {
			t.Fatalf("blocked at attempt %d; want %d", got, attempt)
		}
	case <-time.After(3 * rctlBound):
		t.Fatalf("no retry wait reached attempt %d; recovery log:\n%s", attempt, issue1367RecoveryLines(logs.text()))
	}
}

// withIssue1369Seams narrows the AM27 threshold, keeps the production retry
// policy and injects clock.
func withIssue1369Seams(t *testing.T, clock *issue1369Clock) {
	t.Helper()
	previousThreshold, previousPolicy, previousClock := adapterDirectBlackholeThreshold, ebusDriverRetryPolicy, ebusDriverRetryClock
	adapterDirectBlackholeThreshold = issue1367BlackholeThreshold
	ebusDriverRetryClock = ebusRetryClock{now: clock.Now, jitter: clock.Jitter, wait: clock.wait}
	t.Cleanup(func() {
		adapterDirectBlackholeThreshold, ebusDriverRetryPolicy, ebusDriverRetryClock = previousThreshold, previousPolicy, previousClock
	})
	if policy := ebusDriverRetryPolicy; policy.Budget != 5 || policy.InitialDelay != time.Second || policy.MaxDelay != 30*time.Second ||
		policy.JitterRatio != 0.2 || !policy.Persistent.Enabled || policy.Persistent.InitialDelay != time.Second || policy.Persistent.MaxDelay != 5*time.Minute {
		t.Fatalf("ebus.primary retry policy = %+v; want five fast attempts from 1s to 30s, then 1s to 5m, 20%% jitter", policy)
	}
}

func startIssue1369Process(t *testing.T, clock *issue1369Clock, endpoint *issue1367Endpoint, protocol ebusgateway.TransportProtocol) *rctlProcess {
	t.Helper()
	process := startIssue1367Process(t, endpoint, protocol)
	clock.process.Store(process)
	process.waitObserved(drivermanager.ObservedRunning)
	requireIssue1367Version(t, process, 1)
	return process
}

// issue1369ExpectedDelay is the delay of the n-th retry (from zero) of a
// series under the production policy, for a jitter at its extreme of the
// given sign. It is written out from the policy's statement, not from the
// implementation: nominal = initial * 2^index capped, a jitter of a fifth of
// the nominal, the result clamped to the phase.
func issue1369ExpectedDelay(n, sign int) time.Duration {
	initial, ceiling, index := time.Second, 30*time.Second, n
	if n >= 5 {
		ceiling, index = 5*time.Minute, n-5
	}
	nominal := initial
	for i := 0; i < index && nominal < ceiling; i++ {
		nominal *= 2
	}
	if nominal > ceiling {
		nominal = ceiling
	}
	delay := nominal + time.Duration(sign)*nominal/5
	if delay < initial {
		delay = initial
	}
	if delay > ceiling {
		delay = ceiling
	}
	return delay
}

// issue1369RequireSchedule asserts every scheduled retry: BACKOFF with the
// attempt's number, the fast budget left, the expected delay, the due time on
// the manager clock, and the same schedule in the service view.
func issue1369RequireSchedule(t *testing.T, retries []issue1369Retry, desired runtimecontrol.DesiredRevision, sign func(n int) int) {
	t.Helper()
	for n, retry := range retries {
		attempt := n + 1
		snapshot := retry.snapshot
		if snapshot.ObservedState != drivermanager.ObservedBackoff || snapshot.Reason.Code != drivermanager.ReasonRetryScheduled || snapshot.Retry == nil {
			t.Fatalf("retry %d manager snapshot = %s/%s %+v; want BACKOFF/RETRY_SCHEDULED with a schedule", attempt, snapshot.ObservedState, snapshot.Reason.Code, snapshot.Retry)
		}
		remaining := 0
		if n < 5 {
			remaining = 5 - n
		}
		if snapshot.Retry.Attempt != attempt || snapshot.Retry.BudgetRemaining != remaining {
			t.Fatalf("retry %d schedule = %+v; want attempt %d with %d fast attempts left", attempt, snapshot.Retry, attempt, remaining)
		}
		if want := issue1369ExpectedDelay(n, sign(n+1)); retry.delay != want {
			t.Fatalf("retry %d delay = %s; want %s", attempt, retry.delay, want)
		}
		if !snapshot.Retry.NotBeforeUTC.Equal(retry.clock.Add(retry.delay)) {
			t.Fatalf("retry %d due = %s; want %s", attempt, snapshot.Retry.NotBeforeUTC, retry.clock.Add(retry.delay))
		}
		if !retry.viewOK {
			t.Fatalf("retry %d: service view %s/%s next attempt %d at %s; want BACKOFF/RETRY_SCHEDULED next attempt %d at %s",
				attempt, retry.view.ObservedState, retry.view.Reason, retry.view.NextAttempt, retry.view.NextAttemptUTC, attempt, snapshot.Retry.NotBeforeUTC)
		}
		if retry.view.DesiredState != runtimecontrol.DesiredRunning || retry.view.DesiredRevision != desired || retry.view.SafetyQuarantined {
			t.Fatalf("retry %d: service view %+v; want desired RUNNING at revision %d, not quarantined", attempt, retry.view, desired)
		}
	}
}

// issue1369AttemptLines is the ordered log of the retries of one series:
// each BACKOFF with its schedule, the attempt with its phase and reason, and
// each failed attempt with its error class and cause text.
func issue1369AttemptLines(retries []issue1369Retry, failures int, lossReason string) []string {
	var lines []string
	for n := range retries {
		attempt := n + 1
		phase, reason := "fast", lossReason
		if attempt > 5 {
			phase = "persistent"
		}
		if attempt > 1 {
			reason = "retry_after_dial_refused"
		}
		lines = append(lines,
			fmt.Sprintf("ebus.primary: lifecycle BACKOFF (reason=RETRY_SCHEDULED, next_attempt=%d, next_attempt_at=%s, delay=%s, phase=%s)",
				attempt, retries[n].snapshot.Retry.NotBeforeUTC.UTC().Format(ebusRecoveryTimeLayout), retries[n].delay.Round(time.Millisecond), phase),
			fmt.Sprintf("ebus.primary: recovery attempt %d (reason=%s, phase=%s)", attempt, reason, phase))
		if attempt <= failures {
			lines = append(lines, fmt.Sprintf(`ebus.primary: recovery attempt %d failed (class=dial_refused, error="connect: connection refused")`, attempt))
		}
	}
	return lines
}

// issue1369RequireNoEndpointInRecoveryLines asserts that the recovery lines
// name neither the endpoint's host nor its port.
func issue1369RequireNoEndpointInRecoveryLines(t *testing.T, logs *issue1367Logs, endpoint *issue1367Endpoint) {
	t.Helper()
	host, port, err := net.SplitHostPort(endpoint.address)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(logs.text(), "\n") {
		if strings.Contains(line, "ebus.primary:") && (strings.Contains(line, host) || strings.Contains(line, ":"+port)) {
			t.Fatalf("recovery line names the endpoint: %q", line)
		}
	}
}

func issue1369RequireNoTerminalEvent(t *testing.T, process *rctlProcess, cursor uint64) {
	t.Helper()
	events := issue1367ObservedSince(process, cursor)
	for _, event := range events {
		if event.ObservedState == drivermanager.ObservedFailed {
			t.Fatalf("recovery published a terminal state: %+v", event)
		}
		if event.Epoch != events[0].Epoch || event.Generation != events[0].Generation {
			t.Fatalf("recovery changed the lifecycle correlation (a restart): %+v", events)
		}
	}
}

func TestIssue1369AdapterDirectStallHealsAfterFastAttemptsWithoutRestart(t *testing.T) {
	clock := newIssue1369Clock(issue1369NoJitter)
	withIssue1369Seams(t, clock)
	logs := captureIssue1367Logs(t)
	endpoint := newIssue1367Endpoint(t, true)
	process := startIssue1369Process(t, clock, endpoint, ebusgateway.TransportAdapterDirect)
	first := process.control.driver.Snapshot()
	before := process.status()
	cursor := process.control.Service().List().EventCursor

	// Every dial is refused through the five fast attempts and the first two
	// persistent ones; the adapter accepts again before the eighth attempt.
	const healAt = 8
	clock.setOnRetry(func(attempt int) issue1369Action {
		if attempt == healAt {
			endpoint.open()
		}
		return issue1369Proceed
	})
	endpoint.refuse()
	endpoint.stall()

	retries := clock.waitRetries(t, healAt, logs)
	status := waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
	issue1369RequireSchedule(t, retries, before.DesiredRevision, issue1369NoJitter)
	if len(clock.recorded()) != healAt {
		t.Fatalf("scheduled retries = %d; want %d", len(clock.recorded()), healAt)
	}
	if status.NextAttempt != 0 || !status.NextAttemptUTC.IsZero() || status.DesiredRevision != before.DesiredRevision {
		t.Fatalf("recovered view = %+v; want no schedule and the unchanged desired revision %d (no restart)", status, before.DesiredRevision)
	}
	if got, reason := portalConfigReadbackStatus(status); got != "effective" || reason != "running" {
		t.Fatalf("Portal readback after recovery = %s/%s; want effective/running", got, reason)
	}
	requireIssue1367List(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone)
	recovered := process.control.driver.Snapshot()
	if recovered.ObservedState != drivermanager.ObservedRunning || recovered.Generation != first.Generation+1 || recovered.Retry != nil {
		t.Fatalf("manager snapshot = %s generation %d retry %+v; want RUNNING generation %d", recovered.ObservedState, recovered.Generation, recovered.Retry, first.Generation+1)
	}
	if endpoint.sessionCount() != 2 {
		t.Fatalf("endpoint sessions = %d; want the first and the healed one", endpoint.sessionCount())
	}
	requireIssue1367Version(t, process, 2)
	issue1369RequireNoTerminalEvent(t, process, cursor)

	ordered := []string{
		"triggering reconnect (AM27)",
		fmt.Sprintf("ebus.primary: connection lost (reason=adapter_connection_lost, generation=%d); recovery continues while desired RUNNING (5 fast attempts, then backoff from 1s doubling to 5m0s)", first.Generation),
		"ebus.primary: recovery attempt 1 (reason=adapter_connection_lost, phase=fast)",
	}
	logs.requireOrdered(t, ordered...)
	lines := issue1369AttemptLines(retries, healAt-1, ebusLossAdapterConnection)
	lines = append(lines,
		fmt.Sprintf("ebus.primary: recovery attempt %d reconnected (generation=%d); awaiting admission", healAt, recovered.Generation),
		fmt.Sprintf("ebus.primary: recovered on attempt %d (generation=%d)", healAt, recovered.Generation),
		fmt.Sprintf("ebus.primary: lifecycle RUNNING (generation=%d, recovered)", recovered.Generation))
	logs.requireOrdered(t, lines...)
	if logs.count("ebus.primary: lifecycle FAILED") != 0 {
		t.Fatalf("recovery logged a terminal state:\n%s", issue1367RecoveryLines(logs.text()))
	}
	issue1369RequireNoEndpointInRecoveryLines(t, logs, endpoint)
	issue1367RequireOneDelegation(t, logs)
}

func TestIssue1369ProxyClassLossHealsAfterFastAttemptsWithoutRestart(t *testing.T) {
	clock := newIssue1369Clock(issue1369NoJitter)
	withIssue1369Seams(t, clock)
	logs := captureIssue1367Logs(t)
	endpoint := newIssue1367Endpoint(t, false)
	process := startIssue1369Process(t, clock, endpoint, ebusgateway.TransportENH)
	first := process.control.driver.Snapshot()
	before := process.status()
	cursor := process.control.Service().List().EventCursor

	const healAt = 7
	clock.setOnRetry(func(attempt int) issue1369Action {
		if attempt == healAt {
			endpoint.open()
		}
		return issue1369Proceed
	})
	endpoint.refuse()
	endpoint.drop()
	issue1367DetectProxyLoss(t, process, logs)

	retries := clock.waitRetries(t, healAt, logs)
	status := waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
	issue1369RequireSchedule(t, retries, before.DesiredRevision, issue1369NoJitter)
	if status.NextAttempt != 0 || status.DesiredRevision != before.DesiredRevision {
		t.Fatalf("recovered view = %+v; want no schedule and no restart", status)
	}
	requireIssue1367List(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone)
	recovered := process.control.driver.Snapshot()
	if recovered.Generation != first.Generation+1 {
		t.Fatalf("manager generation = %d; want %d", recovered.Generation, first.Generation+1)
	}
	requireIssue1367Version(t, process, endpoint.sessionCount())
	issue1369RequireNoTerminalEvent(t, process, cursor)

	logs.requireOrdered(t,
		fmt.Sprintf("ebus.primary: connection lost (reason=transport_closed, generation=%d); recovery continues while desired RUNNING (5 fast attempts, then backoff from 1s doubling to 5m0s)", first.Generation),
		"ebus.primary: recovery attempt 1 (reason=transport_closed, phase=fast)")
	lines := issue1369AttemptLines(retries, healAt-1, ebusLossTransportClosed)
	lines = append(lines,
		fmt.Sprintf("ebus.primary: recovered on attempt %d (generation=%d)", healAt, recovered.Generation),
		fmt.Sprintf("ebus.primary: lifecycle RUNNING (generation=%d, recovered)", recovered.Generation))
	logs.requireOrdered(t, lines...)
	if logs.count("ebus.primary: lifecycle FAILED") != 0 {
		t.Fatalf("recovery logged a terminal state:\n%s", issue1367RecoveryLines(logs.text()))
	}
	issue1369RequireNoEndpointInRecoveryLines(t, logs, endpoint)
}

func TestIssue1369PersistentBackoffCapAndJitterBoundsUntilExplicitStop(t *testing.T) {
	clock := newIssue1369Clock(issue1369ExtremeJitter)
	withIssue1369Seams(t, clock)
	logs := captureIssue1367Logs(t)
	endpoint := newIssue1367Endpoint(t, false)
	process := startIssue1369Process(t, clock, endpoint, ebusgateway.TransportENH)
	before := process.status()
	cursor := process.control.Service().List().EventCursor

	// The endpoint never comes back. Twenty retries reach the five-minute
	// cap and stay on it; the twentieth waits until it is canceled.
	const total = 20
	clock.setOnRetry(func(attempt int) issue1369Action {
		if attempt == total {
			return issue1369Block
		}
		return issue1369Proceed
	})
	endpoint.refuse()
	endpoint.drop()
	issue1367DetectProxyLoss(t, process, logs)
	clock.waitBlocked(t, total, logs)

	retries := clock.recorded()
	if len(retries) != total {
		t.Fatalf("scheduled retries = %d; want %d", len(retries), total)
	}
	issue1369RequireSchedule(t, retries, before.DesiredRevision, issue1369ExtremeJitter)
	for n, retry := range retries {
		nominal := issue1369ExpectedDelay(n, 0)
		ceiling, floor := 30*time.Second, time.Second
		if n >= 5 {
			ceiling = 5 * time.Minute
		}
		low, high := nominal-nominal/5, nominal+nominal/5
		if low < floor {
			low = floor
		}
		if high > ceiling {
			high = ceiling
		}
		if retry.delay < low || retry.delay > high {
			t.Fatalf("retry %d delay %s outside the jitter bounds [%s, %s] of %s", n+1, retry.delay, low, high, nominal)
		}
	}
	// Literal anchors: the persistent phase restarts at one second (a
	// negative jitter floors there), the cap is five minutes, and on the cap
	// only a negative jitter moves the delay, by a fifth.
	for n, want := range map[int]time.Duration{4: 19200 * time.Millisecond, 5: time.Second, 6: 2400 * time.Millisecond, 14: 5 * time.Minute, 15: 4 * time.Minute, 18: 5 * time.Minute, 19: 4 * time.Minute} {
		if retries[n].delay != want {
			t.Fatalf("retry %d delay = %s; want %s", n+1, retries[n].delay, want)
		}
	}
	waiting := process.status()
	if waiting.ObservedState != drivermanager.ObservedBackoff || waiting.NextAttempt != total || !waiting.NextAttemptUTC.Equal(retries[total-1].snapshot.Retry.NotBeforeUTC) {
		t.Fatalf("service view while waiting = %+v; want BACKOFF with next attempt %d", waiting, total)
	}
	requireIssue1367List(t, process, drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled)
	if got, reason := portalConfigReadbackStatus(waiting); got != "pending" || reason != "backoff" {
		t.Fatalf("Portal readback while waiting = %s/%s; want pending/backoff", got, reason)
	}
	issue1369RequireNoTerminalEvent(t, process, cursor)
	// The last attempt is still waiting: its BACKOFF is the final line.
	lines := issue1369AttemptLines(retries, total-1, ebusLossTransportClosed)
	logs.requireOrdered(t, lines[:len(lines)-1]...)
	if logs.count(fmt.Sprintf("ebus.primary: recovery attempt %d ", total)) != 0 {
		t.Fatal("the waiting attempt ran before its wait ended")
	}
	if logs.count("ebus.primary: lifecycle FAILED") != 0 {
		t.Fatal("persistent recovery logged a terminal state")
	}

	// An explicit stop ends the recovery: STOPPED, no schedule, no attempt
	// after it and no terminal FAILED.
	process.decide(runtimecontrol.MethodStop)
	stopped := process.waitObserved(drivermanager.ObservedStopped)
	if stopped.NextAttempt != 0 || !stopped.NextAttemptUTC.IsZero() {
		t.Fatalf("stopped view kept a schedule: %+v", stopped)
	}
	if snapshot := process.control.driver.Snapshot(); snapshot.ObservedState != drivermanager.ObservedStopped || snapshot.DesiredState != drivermanager.DesiredStopped {
		t.Fatalf("manager snapshot after STOP = %s desired %s", snapshot.ObservedState, snapshot.DesiredState)
	}
	if clock.released.Load() != 1 {
		t.Fatalf("canceled retry waits = %d; want the one STOP canceled", clock.released.Load())
	}
	attempts := logs.count("ebus.primary: recovery attempt ")
	time.Sleep(50 * time.Millisecond)
	if got := logs.count("ebus.primary: recovery attempt "); got != attempts || len(clock.recorded()) != total {
		t.Fatalf("recovery went on after the explicit stop: %d -> %d lines, %d retries", attempts, got, len(clock.recorded()))
	}
	if logs.count("ebus.primary: lifecycle FAILED") != 0 {
		t.Fatal("explicit stop logged a terminal FAILED")
	}
}

// A failure that retrying cannot heal still ends the recovery in the
// terminal state with its own reason: the endpoint is reachable again but
// rejects the configured protocol's INIT, which the gateway classifies as a
// rejected configuration.
func TestIssue1369NonHealingRecoveryFailureEndsTerminalWithItsReason(t *testing.T) {
	for _, tc := range []struct {
		name     string
		protocol ebusgateway.TransportProtocol
		stream   bool
		loss     string
	}{
		{"adapter_direct", ebusgateway.TransportAdapterDirect, true, ebusLossAdapterConnection},
		{"proxy_class", ebusgateway.TransportENH, false, ebusLossTransportClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newIssue1369Clock(issue1369NoJitter)
			withIssue1369Seams(t, clock)
			logs := captureIssue1367Logs(t)
			endpoint := newIssue1367Endpoint(t, tc.stream)
			process := startIssue1369Process(t, clock, endpoint, tc.protocol)
			before := process.status()

			endpoint.rejectInit.Store(true)
			if tc.stream {
				endpoint.stall()
			} else {
				endpoint.drop()
				issue1367DetectProxyLoss(t, process, logs)
			}
			failed := waitIssue1367Status(t, process, drivermanager.ObservedFailed, drivermanager.ReasonConfigInvalid, logs)
			if failed.SafetyQuarantined || failed.DesiredState != runtimecontrol.DesiredRunning || failed.NextAttempt != 0 {
				t.Fatalf("terminal view = %+v; want desired RUNNING, not quarantined, no schedule", failed)
			}
			requireIssue1367List(t, process, drivermanager.ObservedFailed, drivermanager.ReasonConfigInvalid)
			if got, reason := portalConfigReadbackStatus(failed); got != "error" || reason != "config_invalid" {
				t.Fatalf("Portal readback = %s/%s; want error/config_invalid", got, reason)
			}
			if snapshot := process.control.driver.Snapshot(); snapshot.ObservedState != drivermanager.ObservedFailed || snapshot.Reason.Code != drivermanager.ReasonConfigInvalid {
				t.Fatalf("manager snapshot = %s/%s; want FAILED/CONFIG_INVALID", snapshot.ObservedState, snapshot.Reason.Code)
			}
			retries := clock.recorded()
			issue1369RequireSchedule(t, retries, before.DesiredRevision, issue1369NoJitter)
			if len(retries) != 1 {
				t.Fatalf("scheduled retries = %d; want only the one after the loss", len(retries))
			}
			logs.requireOrdered(t,
				fmt.Sprintf("ebus.primary: recovery attempt 1 (reason=%s, phase=fast)", tc.loss),
				`ebus.primary: recovery attempt 1 failed (class=invalid_payload, error="ebus: payload does not match expected schema")`,
				"ebus.primary: lifecycle FAILED (reason=CONFIG_INVALID, safety_quarantined=false); retrying cannot heal this condition")
			time.Sleep(50 * time.Millisecond)
			if logs.count("ebus.primary: recovery attempt 2") != 0 || len(clock.recorded()) != 1 {
				t.Fatalf("recovery retried a non-healing failure:\n%s", issue1367RecoveryLines(logs.text()))
			}

			// Once the condition is corrected an operator RESTART recovers.
			endpoint.rejectInit.Store(false)
			process.decide(runtimecontrol.MethodRestart)
			waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
			requireIssue1367Version(t, process, endpoint.sessionCount())
		})
	}
}

func TestIssue1369RestartDuringPersistentBackoffRecoversAndResetsSchedule(t *testing.T) {
	clock := newIssue1369Clock(issue1369NoJitter)
	withIssue1369Seams(t, clock)
	logs := captureIssue1367Logs(t)
	endpoint := newIssue1367Endpoint(t, false)
	process := startIssue1369Process(t, clock, endpoint, ebusgateway.TransportENH)
	before := process.status()

	// The seventh attempt is due in the persistent phase; its wait holds
	// like a delay of minutes until something cancels it.
	const blockAt = 7
	clock.setOnRetry(func(attempt int) issue1369Action {
		if attempt == blockAt {
			return issue1369Block
		}
		return issue1369Proceed
	})
	endpoint.refuse()
	endpoint.drop()
	issue1367DetectProxyLoss(t, process, logs)
	clock.waitBlocked(t, blockAt, logs)
	waiting := process.status()
	if waiting.ObservedState != drivermanager.ObservedBackoff || waiting.NextAttempt != blockAt {
		t.Fatalf("service view = %+v; want BACKOFF with next attempt %d", waiting, blockAt)
	}
	if retry := process.control.driver.Snapshot().Retry; retry == nil || retry.BudgetRemaining != 0 {
		t.Fatalf("manager schedule = %+v; want the persistent phase", retry)
	}

	// The adapter is back; the operator does not wait for the backoff.
	endpoint.open()
	process.decide(runtimecontrol.MethodRestart)
	restarted := waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
	if restarted.DesiredRevision <= before.DesiredRevision {
		t.Fatalf("restarted view = %+v; want a new desired revision after %d", restarted, before.DesiredRevision)
	}
	if clock.released.Load() != 1 {
		t.Fatalf("canceled retry waits = %d; want the one the RESTART canceled", clock.released.Load())
	}
	requireIssue1367Version(t, process, endpoint.sessionCount())
	if len(clock.recorded()) != blockAt {
		t.Fatalf("scheduled retries = %d; want none after the RESTART", len(clock.recorded()))
	}

	// The RESTART reset the schedule: the next loss opens a new series at
	// the first fast attempt with the full budget and the first delay.
	restartedGeneration := process.control.driver.Snapshot().Generation
	clock.setOnRetry(func(attempt int) issue1369Action {
		if attempt == 2 {
			endpoint.open()
		}
		return issue1369Proceed
	})
	endpoint.refuse()
	endpoint.drop()
	issue1367DetectProxyLoss(t, process, logs)
	retries := clock.waitRetries(t, blockAt+2, logs)[blockAt:]
	waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
	issue1369RequireSchedule(t, retries, restarted.DesiredRevision, issue1369NoJitter)
	if retries[0].delay != time.Second || retries[1].delay != 2*time.Second {
		t.Fatalf("delays after the RESTART = %s, %s; want the fast schedule from 1s", retries[0].delay, retries[1].delay)
	}
	logs.requireOrdered(t,
		fmt.Sprintf("ebus.primary: connection lost (reason=transport_closed, generation=%d)", restartedGeneration),
		"ebus.primary: recovery attempt 1 (reason=transport_closed, phase=fast)",
		"ebus.primary: recovery attempt 2 (reason=retry_after_dial_refused, phase=fast)",
		"ebus.primary: recovered on attempt 2 (generation=")
	requireIssue1367Version(t, process, endpoint.sessionCount())
}
