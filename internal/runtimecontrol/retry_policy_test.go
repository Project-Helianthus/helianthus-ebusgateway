package runtimecontrol

// Tests for the optional per-driver RetryPolicy. Every fixture is synthetic:
// generic family driver IDs (fam.*), injected executors and a temporary
// state file.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

// reasonedRetryErr is a retryable executor failure with a closed reason.
type reasonedRetryErr struct{}

func (reasonedRetryErr) Error() string   { return "provider warming" }
func (reasonedRetryErr) Retryable() bool { return true }
func (reasonedRetryErr) ReasonCode() drivermanager.ReasonCode {
	return drivermanager.ReasonProviderUnavailable
}

// startTimes records the correlation and time of every executor Start.
type startTimes struct {
	mu    sync.Mutex
	corrs []LifecycleCorrelation
	at    []time.Time
}

func (r *startTimes) record(c LifecycleCorrelation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.corrs = append(r.corrs, c)
	r.at = append(r.at, time.Now())
}

func (r *startTimes) snapshot() ([]LifecycleCorrelation, []time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]LifecycleCorrelation(nil), r.corrs...), append([]time.Time(nil), r.at...)
}

// assertPolicyAttempts checks one epoch of n attempts: generations 1..n of a
// single epoch, each retry at least delay after the previous attempt.
func assertPolicyAttempts(t *testing.T, corrs []LifecycleCorrelation, at []time.Time, n int, delay time.Duration) {
	t.Helper()
	if len(corrs) != n {
		t.Fatalf("start attempts = %d, want %d", len(corrs), n)
	}
	for i, c := range corrs {
		if c.Epoch != corrs[0].Epoch || c.Generation != DriverGeneration(i+1) {
			t.Fatalf("attempt %d correlation = %+v, want generation %d of epoch %s", i+1, c, i+1, corrs[0].Epoch)
		}
		if i > 0 && at[i].Sub(at[i-1]) < delay {
			t.Fatalf("attempt %d followed attempt %d after %v, want at least %v", i+1, i, at[i].Sub(at[i-1]), delay)
		}
	}
}

// The policy narrows only its own driver: the untyped process-start attempt
// of fam.alpha stops after its three attempts with its own delay, while
// fam.beta keeps the service limit and delay.
func TestServiceDriverRetryPolicyBoundsOnlyItsDriver(t *testing.T) {
	alpha, beta := DriverID("fam.alpha"), DriverID("fam.beta")
	const delay = 40 * time.Millisecond
	alphaStarts := &startTimes{}
	f := newFixture(t, svcOpts{
		drivers: enabled(string(alpha), string(beta)),
		opts: func(o *Options) {
			for i := range o.Drivers {
				if o.Drivers[i].DriverID == alpha {
					o.Drivers[i].Retry = RetryPolicy{MaxAttempts: 3, Delay: delay}
				}
			}
		},
		setup: func(f *svcFixture) {
			f.execs[alpha].setStart(func(_ context.Context, c LifecycleCorrelation) error {
				alphaStarts.record(c)
				return reasonedRetryErr{}
			})
			f.execs[beta].setStart(func(context.Context, LifecycleCorrelation) error { return retryErr{"never"} })
		},
	})
	f.waitObserved(alpha, drivermanager.ObservedFailed)
	f.waitObserved(beta, drivermanager.ObservedFailed)
	corrs, at := alphaStarts.snapshot()
	assertPolicyAttempts(t, corrs, at, 3, delay)
	if st := f.status(alpha); st.Reason != drivermanager.ReasonProviderUnavailable {
		t.Fatalf("alpha exhausted = %+v, want the executor's PROVIDER_UNAVAILABLE", st)
	}
	if n := f.execs[beta].count("start"); n != DefaultMaxAttempts {
		t.Fatalf("beta start attempts = %d, want the service limit %d", n, DefaultMaxAttempts)
	}
	if st := f.status(beta); st.Reason != drivermanager.ReasonRetryExhausted {
		t.Fatalf("beta exhausted = %+v, want RETRY_EXHAUSTED", st)
	}
}

// A typed START uses the same policy: three attempts of one epoch, spaced by
// the driver's delay, ending FAILED with the executor's reason.
func TestServiceDriverRetryPolicyBoundsTypedStart(t *testing.T) {
	id := DriverID("fam.alpha")
	const delay = 40 * time.Millisecond
	starts := &startTimes{}
	f := newFixture(t, svcOpts{
		drivers: enabled(string(id)),
		opts:    func(o *Options) { o.Drivers[0].Retry = RetryPolicy{MaxAttempts: 3, Delay: delay} },
	})
	f.waitSettled(id, drivermanager.ObservedRunning)
	f.mustDecide(MethodStop, id)
	f.waitSettled(id, drivermanager.ObservedStopped)
	f.execs[id].setStart(func(_ context.Context, c LifecycleCorrelation) error {
		starts.record(c)
		return reasonedRetryErr{}
	})
	f.mustDecide(MethodStart, id)
	f.waitSettled(id, drivermanager.ObservedFailed)
	corrs, at := starts.snapshot()
	assertPolicyAttempts(t, corrs, at, 3, delay)
	if st := f.status(id); st.Reason != drivermanager.ReasonProviderUnavailable || st.DesiredState != DesiredRunning {
		t.Fatalf("exhausted START = %+v, want FAILED/PROVIDER_UNAVAILABLE with desired RUNNING", st)
	}
}

// The policy never raises the service limit and never retries a
// non-retryable failure.
func TestServiceDriverRetryPolicyNeverWidensTheServiceBound(t *testing.T) {
	t.Run("service limit", func(t *testing.T) {
		id := DriverID("fam.alpha")
		f := newFixture(t, svcOpts{
			drivers: enabled(string(id)),
			opts: func(o *Options) {
				o.MaxAttempts = 2
				o.Drivers[0].Retry = RetryPolicy{MaxAttempts: 5, Delay: time.Millisecond}
			},
			setup: func(f *svcFixture) {
				f.execs[id].setStart(func(context.Context, LifecycleCorrelation) error { return reasonedRetryErr{} })
			},
		})
		f.waitObserved(id, drivermanager.ObservedFailed)
		if n := f.execs[id].count("start"); n != 2 {
			t.Fatalf("start attempts = %d, want the service limit 2", n)
		}
	})
	t.Run("non-retryable", func(t *testing.T) {
		id := DriverID("fam.alpha")
		f := newFixture(t, svcOpts{
			drivers: enabled(string(id)),
			opts:    func(o *Options) { o.Drivers[0].Retry = RetryPolicy{MaxAttempts: 3, Delay: time.Millisecond} },
			setup: func(f *svcFixture) {
				f.execs[id].setStart(func(context.Context, LifecycleCorrelation) error { return errors.New("closed failure") })
			},
		})
		f.waitObserved(id, drivermanager.ObservedFailed)
		if n := f.execs[id].count("start"); n != 1 {
			t.Fatalf("start attempts = %d, want 1 for a non-retryable failure", n)
		}
		if st := f.status(id); st.Reason != drivermanager.ReasonInternalError {
			t.Fatalf("closed failure = %+v, want INTERNAL_ERROR", st)
		}
	})
}

func TestServiceDriverRetryPolicyValidation(t *testing.T) {
	f := newFixture(t, svcOpts{drivers: enabled("fam.alpha"), noStart: true})
	exec := &fakeExec{id: "fam.alpha"}
	for name, policy := range map[string]RetryPolicy{
		"negative attempts": {MaxAttempts: -1},
		"attempts above 8":  {MaxAttempts: DefaultMaxAttempts + 1},
		"negative delay":    {Delay: -time.Millisecond},
		"delay above 30s":   {Delay: DefaultOperationBudget + time.Nanosecond},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(Options{Store: f.store, Recovery: f.rec, Incarnation: "inc1",
				Drivers: []DriverConfig{{DriverID: "fam.alpha", Executor: exec, Retry: policy}}})
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New error = %v, want ErrInvalidConfig", err)
			}
		})
	}
	svc, err := New(Options{Store: f.store, Recovery: f.rec, Incarnation: "inc1",
		Drivers: []DriverConfig{{DriverID: "fam.alpha", Executor: exec, Retry: RetryPolicy{MaxAttempts: DefaultMaxAttempts, Delay: DefaultOperationBudget}}}})
	if err != nil {
		t.Fatalf("New refused the boundary policy: %v", err)
	}
	svc.Close()
}
