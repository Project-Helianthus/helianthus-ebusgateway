package main

// Issue #1371: the opening START of ebus.primary (process start, START after
// STOP, RESTART) no longer ends FAILED when its first attempt fails for a
// retryable reason (a dial failure, a timeout, a transport close). The bridge
// admits the START into DriverManager's persistent recovery as soon as the
// manager schedules the first retry, so the service's operation budget (30 s
// in production) bounds only that first attempt. The recovery then has the
// policy, state reporting and logging of a recovery after a stall: BACKOFF
// with the next attempt's number and due time in the service view (the
// source of helianthus.runtime.list and the Portal readback), every attempt
// logged with its number, phase, reason, error class and cause text, and
// RUNNING once the endpoint appears, without a restart. A terminal class
// still fails the START with its own reason, the proxy class behaves the
// same, and an operator RESTART during the boot backoff recovers at once.
//
// The process tests drive the production composition against the scripted
// loopback endpoint of issue1367_recovery_test.go, with the production retry
// policy and the injected manager clock of issue1369_persistent_recovery_test.go.
// The budget test composes the same manager, bridge, service and watch with a
// narrowed service budget, which the process composition does not expose.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// issue1371BootSchedule is the start line of a boot refused by the endpoint.
const issue1371BootSchedule = `ebus.primary: start attempt 1 failed (class=dial_refused, error="connect: connection refused"); recovery continues while desired RUNNING (5 fast attempts, then backoff from 1s doubling to 5m0s)`

// withIssue1371Seams installs the #1369 seams and a retry wait that, before
// the #1369 wait, waits for the process (the opening START schedules its
// first retry before the HTTP shell, and so the process, is up) and for the
// BACKOFF line of the scheduled attempt, so the log order of each attempt is
// deterministic.
func withIssue1371Seams(t *testing.T, clock *issue1369Clock, logs *issue1367Logs) {
	t.Helper()
	withIssue1369Seams(t, clock)
	ebusDriverRetryClock.wait = func(ctx context.Context, delay time.Duration) bool {
		deadline := time.Now().Add(3 * rctlBound)
		for clock.process.Load() == nil {
			if ctx.Err() != nil || time.Now().After(deadline) {
				return false
			}
			time.Sleep(time.Millisecond)
		}
		if retry := clock.process.Load().control.driver.Snapshot().Retry; retry != nil {
			line := fmt.Sprintf("ebus.primary: lifecycle BACKOFF (reason=RETRY_SCHEDULED, next_attempt=%d, next_attempt_at=%s,",
				retry.Attempt, retry.NotBeforeUTC.UTC().Format(ebusRecoveryTimeLayout))
			for !strings.Contains(logs.text(), line) && ctx.Err() == nil && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
		}
		return clock.wait(ctx, delay)
	}
}

func startIssue1371Process(t *testing.T, clock *issue1369Clock, endpoint *issue1367Endpoint, protocol ebusgateway.TransportProtocol) *rctlProcess {
	t.Helper()
	process := startIssue1367Process(t, endpoint, protocol)
	clock.process.Store(process)
	return process
}

// issue1371RequireOneEpoch asserts the boot's observed events: no FAILED,
// one lifecycle correlation throughout (no restart), a BACKOFF, and RUNNING
// only as the final event once the endpoint is up (want RUNNING), never
// while it is down.
func issue1371RequireOneEpoch(t *testing.T, process *rctlProcess, want drivermanager.ObservedState) runtimecontrol.Event {
	t.Helper()
	var events []runtimecontrol.Event
	for _, event := range issue1367ObservedSince(process, 0) {
		if event.Epoch != "" {
			events = append(events, event)
		}
	}
	if len(events) == 0 || events[len(events)-1].ObservedState != want {
		t.Fatalf("observed events = %+v; want the last one %s", events, want)
	}
	backoff, running := false, 0
	for _, event := range events {
		if event.ObservedState == drivermanager.ObservedFailed {
			t.Fatalf("the boot published a terminal state: %+v", events)
		}
		if event.ObservedState == drivermanager.ObservedRunning {
			running++
		}
		if event.Epoch != events[0].Epoch || event.Generation != events[0].Generation {
			t.Fatalf("the boot changed the lifecycle correlation (a restart): %+v", events)
		}
		backoff = backoff || event.ObservedState == drivermanager.ObservedBackoff
	}
	if !backoff {
		t.Fatalf("observed events = %+v; want the boot BACKOFF", events)
	}
	if wantRunning := map[bool]int{true: 1, false: 0}[want == drivermanager.ObservedRunning]; running != wantRunning {
		t.Fatalf("observed events = %+v; want %d RUNNING (RUNNING only once the endpoint is up)", events, wantRunning)
	}
	return events[0]
}

func TestIssue1371DownAtBootBacksOffThenRunsWhenTheEndpointAppears(t *testing.T) {
	for _, tc := range []struct {
		name     string
		protocol ebusgateway.TransportProtocol
		stream   bool
	}{
		{"adapter_direct", ebusgateway.TransportAdapterDirect, true},
		{"proxy_class", ebusgateway.TransportENH, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newIssue1369Clock(issue1369NoJitter)
			logs := captureIssue1367Logs(t)
			withIssue1371Seams(t, clock, logs)
			endpoint := newIssue1367Endpoint(t, tc.stream)
			endpoint.refuse()

			// The adapter stays down through the five fast attempts and the
			// first persistent one; the service view is inspected while the
			// sixth waits, and the adapter appears before the seventh.
			const pauseAt, healAt = 6, 7
			paused, resume := make(chan struct{}), make(chan struct{})
			var release sync.Once
			clock.setOnRetry(func(attempt int) issue1369Action {
				switch attempt {
				case pauseAt:
					close(paused)
					<-resume
				case healAt:
					endpoint.open()
				}
				return issue1369Proceed
			})
			process := startIssue1371Process(t, clock, endpoint, tc.protocol)
			t.Cleanup(func() { release.Do(func() { close(resume) }) })
			select {
			case <-paused:
			case <-time.After(3 * rctlBound):
				t.Fatalf("the boot recovery never reached attempt %d; recovery log:\n%s", pauseAt, issue1367RecoveryLines(logs.text()))
			}

			waiting := process.status()
			schedule := process.control.driver.Snapshot()
			if waiting.ObservedState != drivermanager.ObservedBackoff || waiting.Reason != drivermanager.ReasonRetryScheduled ||
				waiting.NextAttempt != pauseAt || schedule.Retry == nil || !waiting.NextAttemptUTC.Equal(schedule.Retry.NotBeforeUTC) {
				t.Fatalf("service view while down = %+v (manager %+v); want BACKOFF/RETRY_SCHEDULED with next attempt %d", waiting, schedule.Retry, pauseAt)
			}
			if waiting.DesiredState != runtimecontrol.DesiredRunning || waiting.SafetyQuarantined {
				t.Fatalf("service view while down = %+v; want desired RUNNING, not quarantined", waiting)
			}
			if !schedule.Retry.Persistent || schedule.Retry.BudgetRemaining != 0 || schedule.DesiredState != drivermanager.DesiredRunning {
				t.Fatalf("manager schedule = %+v desired %s; want the persistent phase while desired RUNNING", schedule.Retry, schedule.DesiredState)
			}
			requireIssue1367List(t, process, drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled)
			if got, reason := portalConfigReadbackStatus(waiting); got != "pending" || reason != "backoff" {
				t.Fatalf("Portal readback while down = %s/%s; want pending/backoff", got, reason)
			}
			if got := process.adapterStatus(); got != adapterStatusUnavailable {
				t.Fatalf("adapter status while down = %q; want %q", got, adapterStatusUnavailable)
			}
			issue1371RequireOneEpoch(t, process, drivermanager.ObservedBackoff)
			release.Do(func() { close(resume) })

			status := waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
			retries := clock.recorded()
			if len(retries) != healAt {
				t.Fatalf("scheduled retries = %d; want %d", len(retries), healAt)
			}
			issue1369RequireSchedule(t, retries, waiting.DesiredRevision, issue1369NoJitter)
			if status.NextAttempt != 0 || !status.NextAttemptUTC.IsZero() || status.DesiredRevision != waiting.DesiredRevision {
				t.Fatalf("running view = %+v; want no schedule and the unchanged desired revision %d (no restart)", status, waiting.DesiredRevision)
			}
			if got, reason := portalConfigReadbackStatus(status); got != "effective" || reason != "running" {
				t.Fatalf("Portal readback once up = %s/%s; want effective/running", got, reason)
			}
			requireIssue1367List(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone)
			if endpoint.sessionCount() != 1 {
				t.Fatalf("endpoint sessions = %d; want the one that came up", endpoint.sessionCount())
			}
			requireIssue1367Version(t, process, 1)
			issue1371RequireOneEpoch(t, process, drivermanager.ObservedRunning)

			running := process.control.driver.Snapshot()
			lines := []string{issue1371BootSchedule}
			lines = append(lines, issue1369AttemptLines(retries, healAt-1, "retry_after_dial_refused")...)
			lines = append(lines,
				fmt.Sprintf("ebus.primary: recovery attempt %d reconnected (generation=%d); awaiting admission", healAt, running.Generation),
				fmt.Sprintf("ebus.primary: recovered on attempt %d (generation=%d)", healAt, running.Generation),
				fmt.Sprintf("ebus.primary: lifecycle RUNNING (generation=%d, recovered)", running.Generation))
			logs.requireOrdered(t, lines...)
			if logs.count("ebus.primary: lifecycle FAILED") != 0 || logs.count("ebus.primary: start attempt 2") != 0 {
				t.Fatalf("the boot recovery logged a terminal state or a second start:\n%s", issue1367RecoveryLines(logs.text()))
			}
			issue1369RequireNoEndpointInRecoveryLines(t, logs, endpoint)
		})
	}
}

// A failure that retrying cannot heal still fails the opening START, with
// its own reason and without a retry.
func TestIssue1371TerminalClassAtBootFailsTheStartWithItsReason(t *testing.T) {
	for _, tc := range []struct {
		name     string
		protocol ebusgateway.TransportProtocol
		stream   bool
	}{
		{"adapter_direct", ebusgateway.TransportAdapterDirect, true},
		{"proxy_class", ebusgateway.TransportENH, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newIssue1369Clock(issue1369NoJitter)
			logs := captureIssue1367Logs(t)
			withIssue1371Seams(t, clock, logs)
			endpoint := newIssue1367Endpoint(t, tc.stream)
			endpoint.rejectInit.Store(true)
			process := startIssue1371Process(t, clock, endpoint, tc.protocol)

			failed := waitIssue1367Status(t, process, drivermanager.ObservedFailed, drivermanager.ReasonConfigInvalid, logs)
			if failed.SafetyQuarantined || failed.DesiredState != runtimecontrol.DesiredRunning || failed.NextAttempt != 0 {
				t.Fatalf("terminal view = %+v; want desired RUNNING, not quarantined, no schedule", failed)
			}
			requireIssue1367List(t, process, drivermanager.ObservedFailed, drivermanager.ReasonConfigInvalid)
			if got, reason := portalConfigReadbackStatus(failed); got != "error" || reason != "config_invalid" {
				t.Fatalf("Portal readback = %s/%s; want error/config_invalid", got, reason)
			}
			time.Sleep(50 * time.Millisecond)
			if len(clock.recorded()) != 0 {
				t.Fatalf("scheduled retries = %d; want none after a terminal class", len(clock.recorded()))
			}
			for _, event := range issue1367ObservedSince(process, 0) {
				if event.ObservedState == drivermanager.ObservedBackoff {
					t.Fatalf("a terminal class at boot published BACKOFF: %+v", event)
				}
			}
			logs.requireOrdered(t, `ebus.primary: start attempt 1 failed (class=invalid_payload, error="ebus: payload does not match expected schema")`)
			if logs.count("recovery continues") != 0 || logs.count("ebus.primary: recovery attempt") != 0 {
				t.Fatalf("a terminal class at boot opened a recovery:\n%s", issue1367RecoveryLines(logs.text()))
			}

			// Once the condition is corrected an operator RESTART recovers.
			endpoint.rejectInit.Store(false)
			process.decide(runtimecontrol.MethodRestart)
			waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
			requireIssue1367Version(t, process, endpoint.sessionCount())
		})
	}
}

func TestIssue1371RestartDuringBootBackoffRecoversAndResetsSchedule(t *testing.T) {
	clock := newIssue1369Clock(issue1369NoJitter)
	logs := captureIssue1367Logs(t)
	withIssue1371Seams(t, clock, logs)
	endpoint := newIssue1367Endpoint(t, false)
	endpoint.refuse()

	// The sixth attempt is due in the persistent phase; its wait holds like a
	// delay of minutes until something cancels it.
	const blockAt = 6
	clock.setOnRetry(func(attempt int) issue1369Action {
		if attempt == blockAt {
			return issue1369Block
		}
		return issue1369Proceed
	})
	process := startIssue1371Process(t, clock, endpoint, ebusgateway.TransportENH)
	clock.waitBlocked(t, blockAt, logs)
	waiting := process.status()
	if waiting.ObservedState != drivermanager.ObservedBackoff || waiting.NextAttempt != blockAt {
		t.Fatalf("service view = %+v; want BACKOFF with next attempt %d", waiting, blockAt)
	}
	boot := issue1371RequireOneEpoch(t, process, drivermanager.ObservedBackoff)

	// The adapter is back; the operator does not wait for the backoff.
	endpoint.open()
	process.decide(runtimecontrol.MethodRestart)
	restarted := waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
	if restarted.DesiredRevision <= waiting.DesiredRevision || restarted.NextAttempt != 0 {
		t.Fatalf("restarted view = %+v; want a new desired revision after %d and no schedule", restarted, waiting.DesiredRevision)
	}
	if clock.released.Load() != 1 {
		t.Fatalf("canceled retry waits = %d; want the one the RESTART canceled", clock.released.Load())
	}
	if len(clock.recorded()) != blockAt || logs.count(fmt.Sprintf("ebus.primary: recovery attempt %d ", blockAt)) != 0 {
		t.Fatalf("the boot recovery went on after the RESTART: %d retries\n%s", len(clock.recorded()), issue1367RecoveryLines(logs.text()))
	}
	events := issue1367ObservedSince(process, 0)
	if last := events[len(events)-1]; last.Epoch == boot.Epoch {
		t.Fatalf("RESTART kept the boot epoch %s", boot.Epoch)
	}
	if endpoint.sessionCount() != 1 {
		t.Fatalf("endpoint sessions = %d; want one", endpoint.sessionCount())
	}
	requireIssue1367Version(t, process, 1)

	// The RESTART reset the schedule: the next loss opens a new series at the
	// first fast attempt with the full budget and the first delay.
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
		issue1371BootSchedule,
		fmt.Sprintf("ebus.primary: connection lost (reason=transport_closed, generation=%d)", restartedGeneration),
		"ebus.primary: recovery attempt 1 (reason=transport_closed, phase=fast)",
		"ebus.primary: recovery attempt 2 (reason=retry_after_dial_refused, phase=fast)",
		"ebus.primary: recovered on attempt 2 (generation=")
	requireIssue1367Version(t, process, endpoint.sessionCount())
}

// issue1371Stack is the manager, bridge, service and manager watch of the
// process composition, with a service operation budget the test chooses.
type issue1371Stack struct {
	ctrl    *ebusDriverController
	bridge  *ebusControlRuntime
	service *runtimecontrol.Service
}

func newIssue1371Stack(t *testing.T, cfg ebusgateway.Config, budget time.Duration) *issue1371Stack {
	t.Helper()
	ctrl, err := newEBusDriverController(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hub, err := semhub.New(semhub.Options{Journal: semhub.NewMemoryJournal()})
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := newEBusControlRuntime(ebusControlConfig{Manager: ctrl.manager, Hub: hub, Source: ctlSource, ClockEpoch: "clock:issue1371", CleanupTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := bridge.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	state := runtimestate.New(runtimestate.Options{Path: ctlStatePath(t), ControlWaitBound: 2 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if _, err := state.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := state.BootstrapControl(context.Background(), []runtimestate.ControlDriverConfig{{DriverID: primaryEBusDriverID, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	service, err := runtimecontrol.New(runtimecontrol.Options{
		Store: &ctlStore{m: state}, Recovery: ctlNoRecovery{}, Incarnation: "inc1",
		Drivers:         []runtimecontrol.DriverConfig{{DriverID: primaryEBusDriverID, Executor: bridge}},
		OperationBudget: budget, RetryDelay: func(int) time.Duration { return 2 * time.Millisecond },
	})
	if err != nil {
		t.Fatal(err)
	}
	watchCtx, stopWatch := context.WithCancel(context.Background())
	watchDone := make(chan struct{})
	forwarder := &ebusNativeForwarder{bridge: bridge, service: service, recovery: &ctrl.runtime.recovery}
	go func() {
		defer close(watchDone)
		for {
			snapshot, changed, _ := ctrl.manager.Watch(primaryEBusDriverID)
			forwarder.forward(snapshot)
			select {
			case <-watchCtx.Done():
				return
			case <-changed:
			}
		}
	}()
	t.Cleanup(func() {
		service.Close()
		stopWatch()
		<-watchDone
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ctrl.manager.Shutdown(ctx)
		_ = bridge.Close(ctx)
		_ = state.Stop(context.Background())
	})
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return &issue1371Stack{ctrl: ctrl, bridge: bridge, service: service}
}

func (stack *issue1371Stack) status(t *testing.T) runtimecontrol.DriverStatus {
	t.Helper()
	status, err := stack.service.Get(primaryEBusDriverID)
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func (stack *issue1371Stack) waitStatus(t *testing.T, state drivermanager.ObservedState, reason drivermanager.ReasonCode, logs *issue1367Logs) runtimecontrol.DriverStatus {
	t.Helper()
	deadline := time.Now().Add(rctlBound)
	for {
		status := stack.status(t)
		if status.ObservedState == state && status.Reason == reason {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("service view %s/%s; want %s/%s; recovery log:\n%s", status.ObservedState, status.Reason, state, reason, issue1367RecoveryLines(logs.text()))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func issue1371ENHConfig(address string) ebusgateway.Config {
	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = nil
	cfg.TransportConfig.Protocol = ebusgateway.TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = address
	cfg.TransportConfig.DialTimeout = time.Second
	cfg.TransportConfig.Dial = nil
	cfg.BroadcastListen = false
	cfg.ProxyListenAddr = ""
	return cfg
}

// The service's operation budget bounds the opening attempt only. A boot
// recovery outlives it in BACKOFF and reaches RUNNING without a restart; an
// opening attempt that is still in flight when the budget ends fails the
// START, as before.
func TestIssue1371OperationBudgetBoundsOnlyTheOpeningAttempt(t *testing.T) {
	const budget = 300 * time.Millisecond

	t.Run("recovery outlives the budget", func(t *testing.T) {
		clock := newIssue1369Clock(issue1369NoJitter)
		withIssue1369Seams(t, clock)
		logs := captureIssue1367Logs(t)
		gate := make(chan struct{})
		var release sync.Once
		t.Cleanup(func() { release.Do(func() { close(gate) }) })
		firstWait := make(chan struct{})
		var first sync.Once
		ebusDriverRetryClock.wait = func(ctx context.Context, delay time.Duration) bool {
			first.Do(func() { close(firstWait) })
			select {
			case <-gate:
			case <-ctx.Done():
				return false
			}
			return ctx.Err() == nil
		}
		endpoint := newIssue1367Endpoint(t, false)
		endpoint.refuse()
		stack := newIssue1371Stack(t, issue1371ENHConfig(endpoint.address), budget)

		select {
		case <-firstWait:
		case <-time.After(rctlBound):
			t.Fatalf("no retry was scheduled after the refused opening attempt; recovery log:\n%s", issue1367RecoveryLines(logs.text()))
		}
		admitted := stack.waitStatus(t, drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled, logs)
		if admitted.NextAttempt != 1 || admitted.NextAttemptUTC.IsZero() {
			t.Fatalf("admitted view = %+v; want the first retry's schedule", admitted)
		}
		// Three budgets later the recovery is still the visible state.
		time.Sleep(3 * budget)
		if late := stack.status(t); late.ObservedState != drivermanager.ObservedBackoff || late.Reason != drivermanager.ReasonRetryScheduled || late.NextAttempt != 1 {
			t.Fatalf("view after the budget = %+v; want the boot BACKOFF, not a terminal state", late)
		}
		if snapshot := stack.ctrl.Snapshot(); snapshot.ObservedState != drivermanager.ObservedBackoff || snapshot.DesiredState != drivermanager.DesiredRunning {
			t.Fatalf("manager after the budget = %s desired %s; want BACKOFF desired RUNNING", snapshot.ObservedState, snapshot.DesiredState)
		}
		for _, event := range stack.service.EventsSince(0).Events {
			if event.Kind == runtimecontrol.EventObservedChanged && (event.ObservedState == drivermanager.ObservedRunning || event.ObservedState == drivermanager.ObservedFailed) {
				t.Fatalf("the boot recovery published %s while the endpoint was down: %+v", event.ObservedState, event)
			}
		}

		endpoint.open()
		release.Do(func() { close(gate) })
		running := stack.waitStatus(t, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
		if running.DesiredRevision != admitted.DesiredRevision || running.NextAttempt != 0 {
			t.Fatalf("running view = %+v; want the unchanged desired revision %d (no restart) and no schedule", running, admitted.DesiredRevision)
		}
		requester, ok := stack.ctrl.active.(transport.InfoRequester)
		if !ok {
			t.Fatal("stable transport has no INFO requester")
		}
		if data, err := requester.RequestInfo(transport.AdapterInfoVersion); err != nil || len(data) != 1 || data[0] != 1 {
			t.Fatalf("version query = %v, %v; want the first session's payload", data, err)
		}
		for _, diagnostic := range stack.service.Diagnostics(primaryEBusDriverID) {
			if diagnostic.Outcome == "FAILED" {
				t.Fatalf("diagnostics = %+v; want no failed start", stack.service.Diagnostics(primaryEBusDriverID))
			}
		}
	})

	t.Run("opening attempt in flight past the budget", func(t *testing.T) {
		clock := newIssue1369Clock(issue1369NoJitter)
		withIssue1369Seams(t, clock)
		logs := captureIssue1367Logs(t)
		dialed := make(chan struct{}, 1)
		cfg := issue1371ENHConfig("adapter.invalid:9999")
		cfg.TransportConfig.Dial = func(ctx context.Context, _, _ string, _ time.Duration) (net.Conn, error) {
			select {
			case dialed <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, errors.New("synthetic dial canceled")
		}
		stack := newIssue1371Stack(t, cfg, budget)
		select {
		case <-dialed:
		case <-time.After(rctlBound):
			t.Fatal("the opening attempt never dialed")
		}
		failed := stack.waitStatus(t, drivermanager.ObservedFailed, drivermanager.ReasonRetryExhausted, logs)
		if failed.NextAttempt != 0 {
			t.Fatalf("failed view kept a schedule: %+v", failed)
		}
		for _, event := range stack.service.EventsSince(0).Events {
			if event.Kind == runtimecontrol.EventObservedChanged && event.ObservedState == drivermanager.ObservedBackoff {
				t.Fatalf("an attempt the budget ended was admitted into recovery: %+v", event)
			}
		}
		deadline := time.Now().Add(rctlBound)
		for {
			snapshot := stack.ctrl.Snapshot()
			if snapshot.ObservedState == drivermanager.ObservedStopped && snapshot.ActiveOperation == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("manager after the failed START = %s (operation %d); want the attempt withdrawn", snapshot.ObservedState, snapshot.ActiveOperation)
			}
			time.Sleep(2 * time.Millisecond)
		}
	})
}

// recoveryHandoverLocked admits the opening START only for a manager
// snapshot of a persistent retry scheduled for this epoch's dispatched,
// in-flight start. Every condition is varied on its own against a
// qualifying base; the base itself is admitted once and stays admitted.
func TestIssue1371RecoveryHandoverAdmitsOnlyThisEpochsPersistentRetry(t *testing.T) {
	due := time.Date(2026, time.October, 26, 7, 0, 1, 0, time.UTC)
	base := func() drivermanager.Snapshot {
		return drivermanager.Snapshot{DriverID: primaryEBusDriverID, DesiredState: drivermanager.DesiredRunning, ObservedState: drivermanager.ObservedBackoff,
			Reason: drivermanager.Reason{Code: drivermanager.ReasonRetryScheduled, Retryable: true}, Revision: 8,
			Retry: &drivermanager.RetrySnapshot{Eligible: true, BudgetRemaining: 5, NotBeforeUTC: due, Attempt: 1, Persistent: true}}
	}
	type fixture struct {
		bridge *ebusControlRuntime
		epoch  *ebusControlEpoch
	}
	setup := func() fixture {
		key := ebusControlKey{DriverID: primaryEBusDriverID, SourceID: ctlSource, Epoch: "inc1.ebus.primary.1", Generation: 1, Sequence: 0}
		epoch := newEBusControlEpoch(key, nil)
		epoch.launched, epoch.dispatched, epoch.dispatchRevision = true, true, 5
		return fixture{bridge: &ebusControlRuntime{id: primaryEBusDriverID, source: ctlSource, current: epoch, history: map[runtimecontrol.SourceEpoch]*ebusControlEpoch{}}, epoch: epoch}
	}
	for name, mutate := range map[string]func(*fixture, *drivermanager.Snapshot){
		"epoch not current":        func(f *fixture, _ *drivermanager.Snapshot) { f.bridge.current = newEBusControlEpoch(f.epoch.key, nil) },
		"epoch fenced":             func(f *fixture, _ *drivermanager.Snapshot) { f.bridge.fenceLocked(f.epoch) },
		"start not dispatched":     func(f *fixture, _ *drivermanager.Snapshot) { f.epoch.dispatched = false },
		"bridge quarantined":       func(f *fixture, _ *drivermanager.Snapshot) { f.bridge.quarantined = true },
		"opening START finished":   func(f *fixture, _ *drivermanager.Snapshot) { close(f.epoch.startDone) },
		"state STARTING":           func(_ *fixture, s *drivermanager.Snapshot) { s.ObservedState = drivermanager.ObservedStarting },
		"state FAILED":             func(_ *fixture, s *drivermanager.Snapshot) { s.ObservedState = drivermanager.ObservedFailed },
		"state RUNNING":            func(_ *fixture, s *drivermanager.Snapshot) { s.ObservedState = drivermanager.ObservedRunning },
		"reason not scheduled":     func(_ *fixture, s *drivermanager.Snapshot) { s.Reason.Code = drivermanager.ReasonDependencyUnavailable },
		"desired STOPPED":          func(_ *fixture, s *drivermanager.Snapshot) { s.DesiredState = drivermanager.DesiredStopped },
		"manager quarantined":      func(_ *fixture, s *drivermanager.Snapshot) { s.SafetyQuarantined = true },
		"revision at the dispatch": func(_ *fixture, s *drivermanager.Snapshot) { s.Revision = 5 },
		"revision before dispatch": func(_ *fixture, s *drivermanager.Snapshot) { s.Revision = 4 },
		"no schedule":              func(_ *fixture, s *drivermanager.Snapshot) { s.Retry = nil },
		"bounded series":           func(_ *fixture, s *drivermanager.Snapshot) { s.Retry.Persistent = false },
		"no attempt ordinal":       func(_ *fixture, s *drivermanager.Snapshot) { s.Retry.Attempt = 0 },
		"no due time":              func(_ *fixture, s *drivermanager.Snapshot) { s.Retry.NotBeforeUTC = time.Time{} },
	} {
		f := setup()
		snapshot := base()
		mutate(&f, &snapshot)
		f.bridge.mu.Lock()
		admitted := f.bridge.recoveryHandoverLocked(f.epoch, snapshot)
		recovering := f.epoch.recovering
		f.bridge.mu.Unlock()
		if admitted || recovering {
			t.Fatalf("%s: the opening START was admitted into recovery", name)
		}
	}

	// The base is admitted, from the manager watch as well, and stays
	// admitted for any later snapshot.
	f := setup()
	corr, obs, ok := f.bridge.NativeObservation(base())
	want := runtimecontrol.NativeObservation{State: drivermanager.ObservedBackoff, Reason: drivermanager.ReasonRetryScheduled, NextAttempt: 1, NextAttemptUTC: due}
	if !ok || obs != want || corr.Epoch != f.epoch.key.Epoch || corr.Generation != f.epoch.key.Generation {
		t.Fatalf("NativeObservation(base) = %+v, %+v, %t; want the admitted BACKOFF of epoch %s", corr, obs, ok, f.epoch.key.Epoch)
	}
	select {
	case <-f.epoch.startDone:
	default:
		t.Fatal("admission left the opening START in flight")
	}
	if f.epoch.startErr != nil || !f.bridge.openedInRecovery(f.epoch.key.Epoch) || f.bridge.openedInRecovery("inc1.ebus.primary.2") {
		t.Fatalf("admission recorded err=%v recovering=%t", f.epoch.startErr, f.epoch.recovering)
	}
	later := base()
	later.ObservedState, later.Retry = drivermanager.ObservedStarting, nil
	f.bridge.mu.Lock()
	stays := f.bridge.recoveryHandoverLocked(f.epoch, later)
	f.bridge.mu.Unlock()
	if !stays || f.epoch.recoveryObs != want {
		t.Fatalf("admission changed for a later snapshot: %t %+v", stays, f.epoch.recoveryObs)
	}
	var recovering *runtimecontrol.RecoveringStart
	if err := f.bridge.awaitStart(context.Background(), f.epoch); !errors.As(err, &recovering) || recovering.Observation != want {
		t.Fatalf("joined start = %v; want the recorded RecoveringStart", err)
	}
}

// A manager watch that coalesced the boot recovery may forward RUNNING as
// the first transition of an epoch whose opening START was admitted into
// recovery. It is the recovered RUNNING: it is logged as such, whereas the
// first forwarded RUNNING of an epoch that opened RUNNING is not new.
func TestIssue1371ForwarderLogsTheFirstRunningOfARecoveringEpoch(t *testing.T) {
	for _, tc := range []struct {
		name       string
		recovering bool
		lines      int
	}{
		{"opened in recovery", true, 1},
		{"opened RUNNING", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := ebusControlKey{DriverID: primaryEBusDriverID, SourceID: ctlSource, Epoch: "inc1.ebus.primary.1", Generation: 1}
			epoch := newEBusControlEpoch(key, nil)
			epoch.recovering = tc.recovering
			bridge := &ebusControlRuntime{id: primaryEBusDriverID, source: ctlSource, current: epoch, history: map[runtimecontrol.SourceEpoch]*ebusControlEpoch{}}
			sink := &issue1367LineSink{}
			forwarder := &ebusNativeForwarder{bridge: bridge, logf: sink.logf}
			corr := runtimecontrol.LifecycleCorrelation{DriverID: key.DriverID, SourceID: key.SourceID, Epoch: key.Epoch, Generation: key.Generation}
			running := runtimecontrol.NativeObservation{State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone}
			if forwarder.logTransition(corr, running, drivermanager.Snapshot{Generation: 3}, func() {}) {
				t.Fatal("the forwarder took over a publication without a recovery log")
			}
			got := 0
			for _, line := range sink.snapshot() {
				if line == "ebus.primary: lifecycle RUNNING (generation=3, recovered)" {
					got++
				}
			}
			if got != tc.lines {
				t.Fatalf("recovered RUNNING lines = %d; want %d; lines: %q", got, tc.lines, sink.snapshot())
			}
		})
	}
}
