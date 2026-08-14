package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

type sunSpecLiveSmokeFakeDriver struct {
	polls          []sunSpecLiveSmokePollResult
	pollCalls      []sunSpecLiveSmokeAttempt
	reconnectCalls int
	pollFn         func(context.Context, sunSpecLiveSmokeAttempt) (sunSpecLiveSmokePollResult, error)
}

func (driver *sunSpecLiveSmokeFakeDriver) Poll(ctx context.Context, attempt sunSpecLiveSmokeAttempt) (sunSpecLiveSmokePollResult, error) {
	driver.pollCalls = append(driver.pollCalls, attempt)
	if driver.pollFn != nil {
		return driver.pollFn(ctx, attempt)
	}
	if len(driver.polls) == 0 {
		return sunSpecLiveSmokePollResult{}, errors.New("unexpected third poll")
	}
	result := driver.polls[0]
	driver.polls = driver.polls[1:]
	return result, result.Err
}

func (driver *sunSpecLiveSmokeFakeDriver) Reconnect(context.Context) error {
	driver.reconnectCalls++
	return nil
}

type sunSpecLiveSmokeFakeQualifier struct {
	qualifications []sunSpecLiveSmokeQualification
	attempts       []sunSpecLiveSmokeAttempt
}

func (qualifier *sunSpecLiveSmokeFakeQualifier) Qualify(_ context.Context, attempt sunSpecLiveSmokeAttempt, _ sunSpecLiveSmokePollResult) sunSpecLiveSmokeQualification {
	qualifier.attempts = append(qualifier.attempts, attempt)
	result := qualifier.qualifications[0]
	qualifier.qualifications = qualifier.qualifications[1:]
	return result
}

func TestRunSunSpecLiveSmokeMapsQualificationDecision(t *testing.T) {
	for _, test := range []struct {
		name          string
		qualification sunSpecLiveSmokeQualification
		want          sunSpecLiveSmokeDecision
	}{
		{name: "matched", qualification: goSunSpecQualification(), want: sunSpecLiveSmokeDecisionGO},
		{name: "not qualified", qualification: sunSpecLiveSmokeQualification{Outcome: modbusadapter.SunSpecQualificationNoGo}, want: sunSpecLiveSmokeDecisionNoGo},
		{name: "stop", qualification: sunSpecLiveSmokeQualification{Outcome: modbusadapter.SunSpecQualificationStop}, want: sunSpecLiveSmokeDecisionStop},
		{name: "qualifier error", qualification: sunSpecLiveSmokeQualification{Err: errors.New("qualifier failure")}, want: sunSpecLiveSmokeDecisionStop},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver := &sunSpecLiveSmokeFakeDriver{polls: []sunSpecLiveSmokePollResult{{}}}
			qualifier := &sunSpecLiveSmokeFakeQualifier{qualifications: []sunSpecLiveSmokeQualification{test.qualification}}

			result := runSunSpecLiveSmoke(context.Background(), time.Second, driver, qualifier, func(string, ...any) {})
			if result.Decision != test.want {
				t.Fatalf("decision = %q; want %q", result.Decision, test.want)
			}
			if len(driver.pollCalls) != 1 || len(qualifier.attempts) != 1 || driver.reconnectCalls != 0 {
				t.Fatalf("polls=%d qualifications=%d reconnects=%d; want 1, 1, 0", len(driver.pollCalls), len(qualifier.attempts), driver.reconnectCalls)
			}
		})
	}
}

func TestRunSunSpecLiveSmokeReconnectsOnceOnlyForReconnectRequiredError(t *testing.T) {
	driver := &sunSpecLiveSmokeFakeDriver{polls: []sunSpecLiveSmokePollResult{
		{Snapshot: sunSpecLiveSmokeSnapshot{ReconnectRequired: true}, Err: errors.New("first transport failure")},
		{},
	}}
	qualifier := &sunSpecLiveSmokeFakeQualifier{qualifications: []sunSpecLiveSmokeQualification{goSunSpecQualification()}}

	result := runSunSpecLiveSmoke(context.Background(), time.Second, driver, qualifier, func(string, ...any) {})
	if result.Decision != sunSpecLiveSmokeDecisionGO {
		t.Fatalf("decision = %q; want GO", result.Decision)
	}
	if driver.reconnectCalls != 1 || len(driver.pollCalls) != 2 || len(qualifier.attempts) != 1 {
		t.Fatalf("reconnects=%d polls=%d qualifications=%d; want 1, 2, 1", driver.reconnectCalls, len(driver.pollCalls), len(qualifier.attempts))
	}
	first, final := driver.pollCalls[0], driver.pollCalls[1]
	if first.PollID == 0 || first.DeadlineID == 0 || final.PollID == 0 || final.DeadlineID == 0 {
		t.Fatalf("attempt IDs = %#v, %#v; want nonzero poll and deadline IDs", first, final)
	}
	if first.PollID == final.PollID || first.DeadlineID == final.DeadlineID {
		t.Fatalf("attempt IDs reused across reconnect: %#v, %#v", first, final)
	}
	if qualifier.attempts[0].DeadlineID != final.DeadlineID || qualifier.attempts[0].Deadline <= 0 {
		t.Fatalf("qualifier attempt = %#v; want final total deadline", qualifier.attempts[0])
	}
}

func TestRunSunSpecLiveSmokeEnforcesTotalAttemptDeadlineOnPollContext(t *testing.T) {
	const attemptTimeout = 25 * time.Millisecond
	var sawDeadline bool
	var pollErr error
	driver := &sunSpecLiveSmokeFakeDriver{pollFn: func(ctx context.Context, _ sunSpecLiveSmokeAttempt) (sunSpecLiveSmokePollResult, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return sunSpecLiveSmokePollResult{}, errors.New("poll context has no deadline")
		}
		sawDeadline = true
		if remaining := time.Until(deadline); remaining <= 0 || remaining > attemptTimeout {
			return sunSpecLiveSmokePollResult{}, fmt.Errorf("poll deadline remaining = %s; want within (0, %s]", remaining, attemptTimeout)
		}
		<-ctx.Done()
		pollErr = ctx.Err()
		return sunSpecLiveSmokePollResult{}, pollErr
	}}

	started := time.Now()
	result := runSunSpecLiveSmoke(context.Background(), attemptTimeout, driver, &sunSpecLiveSmokeFakeQualifier{}, func(string, ...any) {})
	elapsed := time.Since(started)

	if result.Decision != sunSpecLiveSmokeDecisionStop {
		t.Fatalf("decision = %q; want STOP", result.Decision)
	}
	if !sawDeadline || !errors.Is(pollErr, context.DeadlineExceeded) {
		t.Fatalf("saw deadline=%v poll error=%v; want real expired poll context", sawDeadline, pollErr)
	}
	if elapsed < attemptTimeout || elapsed > 500*time.Millisecond {
		t.Fatalf("elapsed = %s; want bounded by attempt timeout", elapsed)
	}
}

func TestSunSpecLiveSmokeWorkerCloseCancelsAndJoinsPollBeforeReturning(t *testing.T) {
	pollStarted := make(chan struct{})
	cancelObserved := make(chan struct{})
	allowPollExit := make(chan struct{})
	pollExited := make(chan struct{})
	driver := &sunSpecLiveSmokeFakeDriver{pollFn: func(ctx context.Context, _ sunSpecLiveSmokeAttempt) (sunSpecLiveSmokePollResult, error) {
		close(pollStarted)
		<-ctx.Done()
		close(cancelObserved)
		<-allowPollExit
		defer close(pollExited)
		return sunSpecLiveSmokePollResult{}, ctx.Err()
	}}
	worker := newSunSpecLiveSmokeWorker(context.Background(), time.Minute, driver, &sunSpecLiveSmokeFakeQualifier{}, func(string, ...any) {})
	worker.Start()

	select {
	case <-pollStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not start Poll")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- worker.Close() }()
	select {
	case <-cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel Poll context")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before Poll exited: %v", err)
	default:
	}

	close(allowPollExit)
	select {
	case <-pollExited:
	case <-time.After(time.Second):
		t.Fatal("Poll did not exit after cancellation")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close error = %v; want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not join exited Poll")
	}
}

func TestRunSunSpecLiveSmokeDoesNotReconnectOutsideRecoverableError(t *testing.T) {
	for _, test := range []struct {
		name string
		poll sunSpecLiveSmokePollResult
		qual sunSpecLiveSmokeQualification
	}{
		{name: "successful no-go", qual: sunSpecLiveSmokeQualification{Outcome: modbusadapter.SunSpecQualificationNoGo}},
		{name: "successful stop", qual: sunSpecLiveSmokeQualification{Outcome: modbusadapter.SunSpecQualificationStop}},
		{name: "error without reconnect required", poll: sunSpecLiveSmokePollResult{Err: errors.New("non-recoverable transport failure")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			driver := &sunSpecLiveSmokeFakeDriver{polls: []sunSpecLiveSmokePollResult{test.poll}}
			qualifier := &sunSpecLiveSmokeFakeQualifier{qualifications: []sunSpecLiveSmokeQualification{test.qual}}

			result := runSunSpecLiveSmoke(context.Background(), time.Second, driver, qualifier, func(string, ...any) {})
			if result.Decision == sunSpecLiveSmokeDecisionGO {
				t.Fatalf("decision = GO; want NO_GO or STOP")
			}
			if driver.reconnectCalls != 0 || len(driver.pollCalls) != 1 {
				t.Fatalf("reconnects=%d polls=%d; want 0, 1", driver.reconnectCalls, len(driver.pollCalls))
			}
		})
	}
}

func TestRunSunSpecLiveSmokeSanitizesFailureAndNoGoEvidence(t *testing.T) {
	const endpointSentinel = "tcp://endpoint-sentinel.invalid:502"
	const errorSentinel = "raw-error-sentinel"

	driver := &sunSpecLiveSmokeFakeDriver{polls: []sunSpecLiveSmokePollResult{{
		Snapshot: sunSpecLiveSmokeSnapshot{Endpoint: endpointSentinel},
		Err:      fmt.Errorf("dial %s: %s", endpointSentinel, errorSentinel),
	}}}
	var logs []string
	result := runSunSpecLiveSmoke(context.Background(), time.Second, driver, &sunSpecLiveSmokeFakeQualifier{}, func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})
	if result.Decision != sunSpecLiveSmokeDecisionStop {
		t.Fatalf("error decision = %q; want STOP", result.Decision)
	}
	assertSunSpecLiveSmokeSanitized(t, endpointSentinel, errorSentinel, result, logs)

	noGoDriver := &sunSpecLiveSmokeFakeDriver{polls: []sunSpecLiveSmokePollResult{{}}}
	noGoQualifier := &sunSpecLiveSmokeFakeQualifier{qualifications: []sunSpecLiveSmokeQualification{{
		Outcome:    modbusadapter.SunSpecQualificationNoGo,
		Sample:     "sample-must-not-be-published",
		Capability: "capability-must-not-be-published",
		Flavor:     "flavor-must-not-be-published",
	}}}
	noGo := runSunSpecLiveSmoke(context.Background(), time.Second, noGoDriver, noGoQualifier, func(string, ...any) {})
	if noGo.Decision != sunSpecLiveSmokeDecisionNoGo || noGo.Sample != "" || noGo.Capability != "" || noGo.Flavor != "" {
		t.Fatalf("NO_GO result = %#v; want no sample, capability, or flavor", noGo)
	}
}

func goSunSpecQualification() sunSpecLiveSmokeQualification {
	return sunSpecLiveSmokeQualification{
		Outcome:    modbusadapter.SunSpecQualificationGO,
		Sample:     "sample-1",
		Capability: modbusreg.SunSpecThreePhaseMonitoringCapabilityID,
		Flavor:     modbusreg.SunSpecFroniusObservedFlavorID,
	}
}

func assertSunSpecLiveSmokeSanitized(t *testing.T, endpointSentinel, errorSentinel string, result sunSpecLiveSmokeResult, logs []string) {
	t.Helper()
	visible := fmt.Sprintf("%#v %v", result, logs)
	for _, sentinel := range []string{endpointSentinel, errorSentinel} {
		if strings.Contains(visible, sentinel) {
			t.Fatalf("sanitized result/log leaks %q: %s", sentinel, visible)
		}
	}
}
