package ebusgateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSemanticReadScheduler_CoalescesConcurrentGets(t *testing.T) {
	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return time.Unix(0, 0) }

	var calls int32
	fetch := func(ctx context.Context) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(25 * time.Millisecond)
		return []byte{0x01, 0x02}, nil
	}

	var wg sync.WaitGroup
	wg.Add(2)

	results := make([][]byte, 2)
	errs := make([]error, 2)

	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			value, err := scheduler.Get(context.Background(), "k", time.Second, fetch)
			results[i] = value
			errs[i] = err
		}(i)
	}

	wg.Wait()

	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 fetch call, got %d", calls)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("result %d: unexpected error: %v", i, err)
		}
		if got := results[i]; len(got) != 2 || got[0] != 0x01 || got[1] != 0x02 {
			t.Fatalf("result %d: unexpected value: %v", i, got)
		}
	}
}

func TestSemanticReadScheduler_UsesCacheWithinMaxAge(t *testing.T) {
	now := time.Unix(0, 0)
	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return now }

	var calls int32
	fetch := func(ctx context.Context) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte{0xAA}, nil
	}

	value1, err := scheduler.Get(context.Background(), "k", time.Second, fetch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(value1) != 1 || value1[0] != 0xAA {
		t.Fatalf("unexpected value1: %v", value1)
	}

	value2, err := scheduler.Get(context.Background(), "k", time.Second, fetch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(value2) != 1 || value2[0] != 0xAA {
		t.Fatalf("unexpected value2: %v", value2)
	}

	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 fetch call, got %d", calls)
	}
}

func TestSemanticReadScheduler_RefreshesAfterMaxAge(t *testing.T) {
	now := time.Unix(0, 0)
	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return now }

	var calls int32
	fetch := func(ctx context.Context) ([]byte, error) {
		call := atomic.AddInt32(&calls, 1)
		return []byte{byte(call)}, nil
	}

	value1, err := scheduler.Get(context.Background(), "k", 10*time.Millisecond, fetch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(value1) != 1 || value1[0] != 0x01 {
		t.Fatalf("unexpected value1: %v", value1)
	}

	now = now.Add(25 * time.Millisecond)

	value2, err := scheduler.Get(context.Background(), "k", 10*time.Millisecond, fetch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(value2) != 1 || value2[0] != 0x02 {
		t.Fatalf("unexpected value2: %v", value2)
	}

	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expected 2 fetch calls, got %d", calls)
	}
}

func TestSemanticReadScheduler_CircuitBreaker_OpensAndSuppresses(t *testing.T) {
	now := time.Unix(0, 0)
	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return now }

	var transitions []SemanticReadCircuitBreakerTransition
	var suppressions []SemanticReadCircuitBreakerSuppression
	scheduler.SetCircuitBreaker(SemanticReadCircuitBreakerOptions{
		FailureBudget:      2,
		OpenCooldown:       15 * time.Second,
		HalfOpenProbeLimit: 1,
		OnTransition: func(event SemanticReadCircuitBreakerTransition) {
			transitions = append(transitions, event)
		},
		OnSuppressed: func(event SemanticReadCircuitBreakerSuppression) {
			suppressions = append(suppressions, event)
		},
	})

	var calls int32
	fetch := func(context.Context) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("read failed")
	}

	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("first call err = %v; want non-open failure", err)
	}
	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("second call err = %v; want non-open failure", err)
	}

	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); !errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("third call err = %v; want ErrSemanticReadCircuitOpen", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("fetch calls = %d; want 2", got)
	}

	now = now.Add(15 * time.Second)
	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("half-open probe err = %v; want non-open failure", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("fetch calls after half-open probe = %d; want 3", got)
	}

	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); !errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("post-probe call err = %v; want ErrSemanticReadCircuitOpen", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("fetch calls after suppression = %d; want 3", got)
	}

	if len(transitions) != 3 {
		t.Fatalf("transition count = %d; want 3", len(transitions))
	}
	if transitions[0].From != SemanticReadCircuitStateClosed || transitions[0].To != SemanticReadCircuitStateOpen {
		t.Fatalf("transition[0] = %s->%s; want closed->open", transitions[0].From, transitions[0].To)
	}
	if transitions[1].From != SemanticReadCircuitStateOpen || transitions[1].To != SemanticReadCircuitStateHalfOpen {
		t.Fatalf("transition[1] = %s->%s; want open->half-open", transitions[1].From, transitions[1].To)
	}
	if transitions[2].From != SemanticReadCircuitStateHalfOpen || transitions[2].To != SemanticReadCircuitStateOpen {
		t.Fatalf("transition[2] = %s->%s; want half-open->open", transitions[2].From, transitions[2].To)
	}

	if len(suppressions) != 2 {
		t.Fatalf("suppression count = %d; want 2", len(suppressions))
	}
	if suppressions[0].SuppressedTotal != 1 || suppressions[1].SuppressedTotal != 2 {
		t.Fatalf("suppressed totals = [%d,%d]; want [1,2]", suppressions[0].SuppressedTotal, suppressions[1].SuppressedTotal)
	}
	for i, suppression := range suppressions {
		if suppression.State != SemanticReadCircuitStateOpen {
			t.Fatalf("suppression[%d] state = %s; want open", i, suppression.State)
		}
	}
}

func TestSemanticReadScheduler_CircuitBreaker_HalfOpenSuccessResetsBudget(t *testing.T) {
	now := time.Unix(0, 0)
	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return now }

	var transitions []SemanticReadCircuitBreakerTransition
	scheduler.SetCircuitBreaker(SemanticReadCircuitBreakerOptions{
		FailureBudget:      2,
		OpenCooldown:       15 * time.Second,
		HalfOpenProbeLimit: 1,
		OnTransition: func(event SemanticReadCircuitBreakerTransition) {
			transitions = append(transitions, event)
		},
	})

	var calls int32
	fetch := func(context.Context) ([]byte, error) {
		call := atomic.AddInt32(&calls, 1)
		switch call {
		case 1, 2:
			return nil, errors.New("fail-closed")
		case 3:
			return []byte{0x7A}, nil
		default:
			return nil, errors.New("fail-after-recover")
		}
	}

	_, _ = scheduler.Get(context.Background(), "k", 0, fetch)
	_, _ = scheduler.Get(context.Background(), "k", 0, fetch)
	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); !errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("open-state call err = %v; want ErrSemanticReadCircuitOpen", err)
	}

	now = now.Add(15 * time.Second)
	value, err := scheduler.Get(context.Background(), "k", 0, fetch)
	if err != nil {
		t.Fatalf("half-open recovery err = %v; want nil", err)
	}
	if len(value) != 1 || value[0] != 0x7A {
		t.Fatalf("half-open recovery value = %v; want [0x7a]", value)
	}

	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("first post-recovery failure err = %v; want non-open failure", err)
	}
	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("second post-recovery failure err = %v; want non-open failure", err)
	}
	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); !errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("third post-recovery call err = %v; want ErrSemanticReadCircuitOpen", err)
	}

	if got := atomic.LoadInt32(&calls); got != 5 {
		t.Fatalf("fetch calls = %d; want 5", got)
	}
	if len(transitions) < 4 {
		t.Fatalf("transition count = %d; want at least 4", len(transitions))
	}
	if transitions[0].From != SemanticReadCircuitStateClosed || transitions[0].To != SemanticReadCircuitStateOpen {
		t.Fatalf("transition[0] = %s->%s; want closed->open", transitions[0].From, transitions[0].To)
	}
	if transitions[1].From != SemanticReadCircuitStateOpen || transitions[1].To != SemanticReadCircuitStateHalfOpen {
		t.Fatalf("transition[1] = %s->%s; want open->half-open", transitions[1].From, transitions[1].To)
	}
	if transitions[2].From != SemanticReadCircuitStateHalfOpen || transitions[2].To != SemanticReadCircuitStateClosed {
		t.Fatalf("transition[2] = %s->%s; want half-open->closed", transitions[2].From, transitions[2].To)
	}
	if transitions[len(transitions)-1].From != SemanticReadCircuitStateClosed || transitions[len(transitions)-1].To != SemanticReadCircuitStateOpen {
		last := transitions[len(transitions)-1]
		t.Fatalf("last transition = %s->%s; want closed->open", last.From, last.To)
	}
}
