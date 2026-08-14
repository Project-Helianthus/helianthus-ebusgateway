package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type sunSpecLiveSmokeFakeDriver struct {
	polls          []sunSpecLiveSmokePollResult
	pollCalls      []sunSpecLiveSmokeAttempt
	reconnectCalls int
}

func (driver *sunSpecLiveSmokeFakeDriver) Poll(_ context.Context, attempt sunSpecLiveSmokeAttempt) (sunSpecLiveSmokePollResult, error) {
	driver.pollCalls = append(driver.pollCalls, attempt)
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
		{name: "supported", qualification: sunSpecLiveSmokeQualification{Supported: true}, want: sunSpecLiveSmokeDecisionGO},
		{name: "unsupported profile", qualification: sunSpecLiveSmokeQualification{UnsupportedProfile: true}, want: sunSpecLiveSmokeDecisionNoGo},
		{name: "incoherent", qualification: sunSpecLiveSmokeQualification{Incoherent: true}, want: sunSpecLiveSmokeDecisionStop},
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
	qualifier := &sunSpecLiveSmokeFakeQualifier{qualifications: []sunSpecLiveSmokeQualification{{Supported: true}}}

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

func TestRunSunSpecLiveSmokeDoesNotReconnectOutsideRecoverableError(t *testing.T) {
	for _, test := range []struct {
		name string
		poll sunSpecLiveSmokePollResult
		qual sunSpecLiveSmokeQualification
	}{
		{name: "successful unsupported profile", qual: sunSpecLiveSmokeQualification{UnsupportedProfile: true}},
		{name: "successful incoherent", qual: sunSpecLiveSmokeQualification{Incoherent: true}},
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
		UnsupportedProfile: true,
		Sample:             "sample-must-not-be-published",
		Profile:            "profile-must-not-be-published",
	}}}
	noGo := runSunSpecLiveSmoke(context.Background(), time.Second, noGoDriver, noGoQualifier, func(string, ...any) {})
	if noGo.Decision != sunSpecLiveSmokeDecisionNoGo || noGo.Sample != "" || noGo.Profile != "" {
		t.Fatalf("NO_GO result = %#v; want no sample or profile", noGo)
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
