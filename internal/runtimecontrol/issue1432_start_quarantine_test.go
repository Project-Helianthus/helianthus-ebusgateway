package runtimecontrol

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

type issue1432CloseError struct{ retry bool }

func (e issue1432CloseError) Error() string   { return "unproven predecessor retirement" }
func (e issue1432CloseError) Retryable() bool { return e.retry }
func (e issue1432CloseError) ReasonCode() drivermanager.ReasonCode {
	return drivermanager.ReasonCloseUnconfirmed
}

// A FAILED observation can precede terminal slot pruning. Only that busy
// refusal may be retried; admission or any other refusal still fails the test.
func issue1432RequireQuarantinedRestart(t *testing.T, f *svcFixture) {
	t.Helper()
	waitFor(t, "quarantined RESTART refusal after terminal pruning", func() bool {
		_, err := f.decide(MethodRestart, nativeDriver)
		var refusal *Refusal
		if errors.As(err, &refusal) && refusal.Code == RefusalTransitionBusy {
			return false
		}
		if !errors.As(err, &refusal) || refusal.Code != RefusalFailedPrecondition {
			t.Fatalf("RESTART = %v; want FAILED_PRECONDITION", err)
		}
		return true
	})
}

func TestIssue1432StartCloseUnconfirmedQuarantines(t *testing.T) {
	for _, typed := range []bool{false, true} {
		for _, retry := range []bool{false, true} {
			t.Run(fmt.Sprintf("typed=%t/retryable=%t", typed, retry), func(t *testing.T) {
				fail := func(context.Context, LifecycleCorrelation) error {
					return fmt.Errorf("start: %w", issue1432CloseError{retry: retry})
				}
				opts := svcOpts{drivers: enabled(string(nativeDriver))}
				if !typed {
					opts.setup = func(f *svcFixture) { f.execs[nativeDriver].setStart(fail) }
				}
				f := newFixture(t, opts)
				if typed {
					f.waitSettled(nativeDriver, drivermanager.ObservedRunning)
					f.execs[nativeDriver].setStart(fail)
					f.mustDecide(MethodRestart, nativeDriver)
				}
				f.waitObserved(nativeDriver, drivermanager.ObservedFailed)
				status := f.status(nativeDriver)
				if status.Reason != drivermanager.ReasonCloseUnconfirmed || !status.SafetyQuarantined {
					t.Fatalf("status = %+v; want FAILED/CLOSE_UNCONFIRMED with quarantine", status)
				}
				starts := f.execs[nativeDriver].count("start")
				wantStarts := 1
				if typed {
					wantStarts++
				}
				if starts != wantStarts {
					t.Fatalf("starts = %d; want %d without retrying unconfirmed close", starts, wantStarts)
				}
				issue1432RequireQuarantinedRestart(t, f)
				if got := f.execs[nativeDriver].count("start"); got != starts {
					t.Fatalf("starts after refused RESTART = %d; want %d", got, starts)
				}
			})
		}
	}
}

func TestIssue1432QuarantineContinuesCleanup(t *testing.T) {
	for _, failure := range []string{"charge", "result", "semantic"} {
		t.Run(failure, func(t *testing.T) {
			var store *workFaultStore
			f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver)),
				wrap: func(s Store) Store { store = &workFaultStore{Store: s}; return store }})
			f.waitSettled(nativeDriver, drivermanager.ObservedRunning)
			f.mustDecide(MethodStop, nativeDriver)
			f.waitSettled(nativeDriver, drivermanager.ObservedStopped)
			starts := f.execs[nativeDriver].count("start")
			stops := f.execs[nativeDriver].count("stop")
			f.execs[nativeDriver].setStart(func(context.Context, LifecycleCorrelation) error {
				if failure == "charge" {
					store.failFrom.Store(int32(maxCommitAttempts))
				}
				return issue1432CloseError{}
			})
			var cleanupCalls atomic.Int32
			f.execs[nativeDriver].setStop(func(context.Context, LifecycleCorrelation) StopOutcome {
				first := cleanupCalls.Add(1) == 1
				if first && failure == "result" {
					store.failFrom.Store(int32(maxCommitAttempts))
				}
				return StopOutcome{NativeClosed: true, SemanticCleaned: !first || failure != "semantic"}
			})
			f.mustDecide(MethodStart, nativeDriver)
			f.waitSettled(nativeDriver, drivermanager.ObservedFailed)
			status := f.status(nativeDriver)
			if status.Reason != drivermanager.ReasonCloseUnconfirmed || !status.SafetyQuarantined {
				t.Fatalf("status after cleanup = %+v; want persistent close quarantine", status)
			}
			wantStops := 2
			if failure == "charge" {
				wantStops = 1
			}
			if got := f.execs[nativeDriver].count("stop") - stops; got != wantStops {
				t.Fatalf("cleanup Stops = %d; want %d", got, wantStops)
			}
			if got := f.execs[nativeDriver].count("start") - starts; got != 1 {
				t.Fatalf("Starts = %d; want one failed start and no replacement", got)
			}
			issue1432RequireQuarantinedRestart(t, f)
		})
	}
}

func TestIssue1432QuarantineSurvivesCleanupExhaustion(t *testing.T) {
	for _, exhaustion := range []string{"attempts", "deadline"} {
		t.Run(exhaustion, func(t *testing.T) {
			var expired atomic.Bool
			f := newFixture(t, svcOpts{drivers: enabled(string(nativeDriver)),
				opts: func(o *Options) {
					o.MaxAttempts = 2
					o.Now = func() time.Time {
						now := time.Now()
						if expired.Load() {
							return now.Add(time.Minute)
						}
						return now
					}
				}})
			f.waitSettled(nativeDriver, drivermanager.ObservedRunning)
			f.mustDecide(MethodStop, nativeDriver)
			f.waitSettled(nativeDriver, drivermanager.ObservedStopped)
			starts := f.execs[nativeDriver].count("start")
			stops := f.execs[nativeDriver].count("stop")
			f.execs[nativeDriver].setStart(func(context.Context, LifecycleCorrelation) error {
				return issue1432CloseError{}
			})
			f.execs[nativeDriver].setStop(func(context.Context, LifecycleCorrelation) StopOutcome {
				if exhaustion == "deadline" {
					expired.Store(true)
				}
				return StopOutcome{NativeClosed: true, SemanticCleaned: false}
			})
			f.mustDecide(MethodStart, nativeDriver)
			waitFor(t, "cleanup exhaustion", func() bool {
				return f.status(nativeDriver).Reason == drivermanager.ReasonRetryExhausted
			})
			status := f.status(nativeDriver)
			if status.ObservedState != drivermanager.ObservedFailed || !status.SafetyQuarantined {
				t.Fatalf("status after exhaustion = %+v; want FAILED with quarantine", status)
			}
			cur := f.record(nativeDriver).Current
			if cur == nil || cur.Phase != PhaseCleanup || cur.NativeClosePending || !cur.SemanticCleanupPending {
				t.Fatalf("unfinished semantic cleanup not retained: %+v", cur)
			}
			wantStops := 2
			if exhaustion == "deadline" {
				wantStops = 1
			}
			if got := f.execs[nativeDriver].count("stop") - stops; got != wantStops {
				t.Fatalf("cleanup Stops = %d; want %d", got, wantStops)
			}
			found := false
			for _, event := range f.svc.EventsSince(0).Events {
				if event.DriverID == nativeDriver && event.Kind == EventObservedChanged && event.Reason == drivermanager.ReasonRetryExhausted {
					found = true
					if !event.SafetyQuarantined {
						t.Fatalf("exhaustion event lost quarantine: %+v", event)
					}
				}
			}
			if !found {
				t.Fatal("missing cleanup exhaustion event")
			}
			issue1432RequireQuarantinedRestart(t, f)
			if got := f.execs[nativeDriver].count("start") - starts; got != 1 {
				t.Fatalf("Starts = %d; want one failed start and no replacement", got)
			}
		})
	}
}

func TestIssue1432RestartUnconfirmedPredecessorDoesNotRetryClose(t *testing.T) {
	for _, semanticCleaned := range []bool{false, true} {
		t.Run(fmt.Sprintf("semantic-cleaned=%t", semanticCleaned), func(t *testing.T) {
			svc, store, exec := p10Service(t, nativeDriver, 7)
			d := svc.drivers[nativeDriver]
			d.live = &attempt{corr: LifecycleCorrelation{DriverID: nativeDriver, Epoch: "predecessor", Generation: 1}}
			store.record.DesiredState = DesiredRunning
			store.record.Current = &Transition{Kind: KindRestart, Phase: PhasePending, DesiredRevision: 7, Sequence: 2}
			exec.setStop(func(context.Context, LifecycleCorrelation) StopOutcome {
				return StopOutcome{NativeClosed: false, SemanticCleaned: semanticCleaned}
			})
			svc.reconcileStep(d)
			status, err := svc.Get(nativeDriver)
			if err != nil || !status.SafetyQuarantined || status.Reason != drivermanager.ReasonCloseUnconfirmed {
				t.Fatalf("unconfirmed predecessor status = %+v, err=%v", status, err)
			}
			// Deliver later reconciliation wakes explicitly; no sleeps or worker
			// timing can hide a repeated native close behind quarantine.
			for i := 0; i < 3; i++ {
				svc.reconcileStep(d)
			}
			if got := exec.count("stop"); got != 1 || exec.count("start") != 0 {
				t.Fatalf("quarantined predecessor: stops=%d starts=%d; want one stop and no successor", got, exec.count("start"))
			}
			rec, err := store.Driver(nativeDriver)
			if err != nil || rec.Current == nil || !rec.Current.NativeClosePending || rec.Current.SemanticCleanupPending == semanticCleaned || rec.Current.AttemptsUsed != 1 {
				t.Fatalf("cleanup obligations or budget changed: %+v, err=%v", rec.Current, err)
			}
			_, err = svc.Decide(context.Background(), MethodRestart, Request{DriverID: nativeDriver, ExpectedRevision: 7})
			var refusal *Refusal
			if !errors.As(err, &refusal) || refusal.Code != RefusalFailedPrecondition {
				t.Fatalf("RESTART = %v; want FAILED_PRECONDITION", err)
			}
		})
	}
}
