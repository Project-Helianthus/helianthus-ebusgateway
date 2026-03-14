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

func TestSemanticReadScheduler_CircuitBreaker_HalfOpenCoalescesConcurrentProbe(t *testing.T) {
	now := time.Unix(0, 0)
	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return now }
	maxAge := 500 * time.Millisecond
	scheduler.SetCircuitBreaker(SemanticReadCircuitBreakerOptions{
		FailureBudget:      1,
		OpenCooldown:       10 * time.Second,
		HalfOpenProbeLimit: 1,
	})

	var calls int32
	fetch := func(context.Context) ([]byte, error) {
		call := atomic.AddInt32(&calls, 1)
		if call == 1 {
			return nil, errors.New("initial fail")
		}
		time.Sleep(25 * time.Millisecond)
		return []byte{0x42}, nil
	}

	if _, err := scheduler.Get(context.Background(), "k", maxAge, fetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("initial failure err = %v; want non-open failure", err)
	}
	if _, err := scheduler.Get(context.Background(), "k", maxAge, fetch); !errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("open-state err = %v; want ErrSemanticReadCircuitOpen", err)
	}

	now = now.Add(10 * time.Second)
	probeStarted := make(chan struct{}, 1)
	releaseProbe := make(chan struct{})
	fetch = func(context.Context) ([]byte, error) {
		call := atomic.AddInt32(&calls, 1)
		if call == 1 {
			return nil, errors.New("initial fail")
		}
		if call == 2 {
			select {
			case probeStarted <- struct{}{}:
			default:
			}
		}
		<-releaseProbe
		return []byte{0x42}, nil
	}

	var wg sync.WaitGroup
	wg.Add(2)
	results := make([][]byte, 2)
	errs := make([]error, 2)
	go func() {
		defer wg.Done()
		value, err := scheduler.Get(context.Background(), "k", maxAge, fetch)
		results[0] = value
		errs[0] = err
	}()
	<-probeStarted
	go func() {
		defer wg.Done()
		value, err := scheduler.Get(context.Background(), "k", maxAge, fetch)
		results[1] = value
		errs[1] = err
	}()
	close(releaseProbe)
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("fetch calls = %d; want 2 (initial fail + one half-open probe)", got)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("result[%d] err = %v; want nil", i, err)
		}
		if len(results[i]) != 1 || results[i][0] != 0x42 {
			t.Fatalf("result[%d] value = %v; want [0x42]", i, results[i])
		}
	}
}

func TestSemanticReadScheduler_CircuitBreaker_DisabledWhenFailureBudgetZero(t *testing.T) {
	scheduler := NewSemanticReadScheduler()
	scheduler.SetCircuitBreaker(SemanticReadCircuitBreakerOptions{
		FailureBudget: 0,
	})

	var calls int32
	fetch := func(context.Context) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("read failed")
	}

	for attempt := 0; attempt < 3; attempt++ {
		if _, err := scheduler.Get(context.Background(), "k", 0, fetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
			t.Fatalf("attempt %d err = %v; want non-open failure when breaker disabled", attempt+1, err)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("fetch calls = %d; want 3 when breaker disabled", got)
	}
}

func TestSemanticReadScheduler_CircuitBreaker_HalfOpenProbeLimitRespected(t *testing.T) {
	now := time.Unix(0, 0)
	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return now }
	scheduler.SetCircuitBreaker(SemanticReadCircuitBreakerOptions{
		FailureBudget:      1,
		OpenCooldown:       10 * time.Second,
		HalfOpenProbeLimit: 2,
	})

	var calls int32
	fetch := func(context.Context) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("read failed")
	}

	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("initial failure err = %v; want non-open failure", err)
	}
	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); !errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("open-state err = %v; want ErrSemanticReadCircuitOpen", err)
	}

	now = now.Add(10 * time.Second)
	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("half-open probe #1 err = %v; want non-open failure", err)
	}
	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("half-open probe #2 err = %v; want non-open failure", err)
	}
	if _, err := scheduler.Get(context.Background(), "k", 0, fetch); !errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("post-probe err = %v; want ErrSemanticReadCircuitOpen", err)
	}

	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("fetch calls = %d; want 3 (initial failure + 2 half-open probes)", got)
	}
}

func TestSemanticReadScheduler_GetWatchReturnsShadowHitWithoutFetch(t *testing.T) {
	t.Parallel()

	key := NewB509WatchKey(0x08, 0x0200)
	now := time.Unix(100, 0)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := newTestShadowCache(t, catalog, activations, now, ShadowCacheOptions{})
	writeShadow(t, cache, key, ShadowWriteSourcePassive, now, []byte{0xAB})

	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return now }
	scheduler.SetShadowCache(cache)

	var fetchCalls int32
	value, err := scheduler.GetWatch(context.Background(), key, time.Second, func(context.Context) ([]byte, error) {
		atomic.AddInt32(&fetchCalls, 1)
		return []byte{0xCD}, nil
	})
	if err != nil {
		t.Fatalf("GetWatch() error = %v", err)
	}
	if len(value) != 1 || value[0] != 0xAB {
		t.Fatalf("GetWatch() value = %v; want shadow value [0xab]", value)
	}
	if got := atomic.LoadInt32(&fetchCalls); got != 0 {
		t.Fatalf("fetch calls = %d; want 0 on eligible shadow hit", got)
	}
}

func TestSemanticReadScheduler_GetWatchWithStats_TracksShadowHit(t *testing.T) {
	t.Parallel()

	key := NewB509WatchKey(0x08, 0x0200)
	now := time.Unix(100, 0)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := newTestShadowCache(t, catalog, activations, now, ShadowCacheOptions{})
	writeShadow(t, cache, key, ShadowWriteSourcePassive, now, []byte{0xAB})

	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return now }
	scheduler.SetShadowCache(cache)

	value, stats, err := scheduler.GetWatchWithStats(context.Background(), key, time.Second, func(context.Context) ([]byte, error) {
		return []byte{0xCD}, nil
	})
	if err != nil {
		t.Fatalf("GetWatchWithStats() error = %v", err)
	}
	if len(value) != 1 || value[0] != 0xAB {
		t.Fatalf("GetWatchWithStats() value = %v; want shadow value [0xab]", value)
	}
	if !stats.ServedFromShadow {
		t.Fatal("stats.ServedFromShadow = false; want true")
	}
	if stats.ActiveFetchAttempted {
		t.Fatal("stats.ActiveFetchAttempted = true; want false on shadow hit")
	}
	if stats.ActiveFetchSucceeded {
		t.Fatal("stats.ActiveFetchSucceeded = true; want false on shadow hit")
	}
	if stats.ActiveFetchDuration != 0 {
		t.Fatalf("stats.ActiveFetchDuration = %s; want 0", stats.ActiveFetchDuration)
	}
}

func TestSemanticReadScheduler_GetWatchWithStats_TracksActiveDuration(t *testing.T) {
	t.Parallel()

	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	now := time.Unix(200, 0)

	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return now }

	value, stats, err := scheduler.GetWatchWithStats(context.Background(), key, 0, func(context.Context) ([]byte, error) {
		now = now.Add(250 * time.Millisecond)
		return []byte{0x42}, nil
	})
	if err != nil {
		t.Fatalf("GetWatchWithStats() error = %v", err)
	}
	if len(value) != 1 || value[0] != 0x42 {
		t.Fatalf("GetWatchWithStats() value = %v; want [0x42]", value)
	}
	if stats.ServedFromShadow {
		t.Fatal("stats.ServedFromShadow = true; want false for active read")
	}
	if !stats.ActiveFetchAttempted {
		t.Fatal("stats.ActiveFetchAttempted = false; want true for active read")
	}
	if !stats.ActiveFetchSucceeded {
		t.Fatal("stats.ActiveFetchSucceeded = false; want true for successful active read")
	}
	if stats.ActiveFetchDuration != 250*time.Millisecond {
		t.Fatalf("stats.ActiveFetchDuration = %s; want 250ms", stats.ActiveFetchDuration)
	}
}

func TestSemanticReadScheduler_RevalidatesShadowWhenInvalidatedBeforeLock(t *testing.T) {
	t.Parallel()

	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	now := time.Unix(200, 0)

	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := NewShadowCache(ShadowCacheOptions{
		Catalog:               catalog,
		Activations:           activations,
		Capacity:              8,
		PinnedCapacity:        4,
		WriteConfirmPinnedCap: 2,
		Now:                   func() time.Time { return now },
	})
	writeShadow(t, cache, key, ShadowWriteSourcePassive, now, []byte{0x10})

	candidate := consultSemanticReadShadow(cache, key, time.Second)
	if candidate == nil {
		t.Fatal("consultSemanticReadShadow() = nil; want candidate")
	}

	cache.Invalidate(ShadowInvalidation{
		Key:           key,
		Reason:        ShadowInvalidationReasonExternalWrite,
		Source:        ShadowInvalidationSourcePassive,
		InvalidatedAt: now,
	})

	snapshot, ok := revalidateSemanticReadShadowCandidate(cache, key, candidate, time.Second, now)
	if ok {
		t.Fatal("revalidateSemanticReadShadowCandidate() = ok; want invalid after generation advance")
	}
	if snapshot.Generation == candidate.generation {
		t.Fatalf("snapshot generation = %d; want generation change after invalidation", snapshot.Generation)
	}

	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return now }
	scheduler.SetShadowCache(cache)

	var fetchCalls int32
	value, err := scheduler.GetWatch(context.Background(), key, time.Second, func(context.Context) ([]byte, error) {
		atomic.AddInt32(&fetchCalls, 1)
		return []byte{0x44}, nil
	})
	if err != nil {
		t.Fatalf("GetWatch() error = %v", err)
	}
	if len(value) != 1 || value[0] != 0x44 {
		t.Fatalf("GetWatch() value = %v; want recomputed [0x44]", value)
	}
	if got := atomic.LoadInt32(&fetchCalls); got != 1 {
		t.Fatalf("fetch calls = %d; want 1 after invalidation-before-lock revalidation", got)
	}
}

func TestSemanticReadScheduler_RecomputesWhenActiveWriteRejectedSameGeneration(t *testing.T) {
	t.Parallel()

	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	maxAge := 5 * time.Second

	var nowUnix atomic.Int64
	nowUnix.Store(time.Unix(200, 0).UnixNano())
	nowFn := func() time.Time { return time.Unix(0, nowUnix.Load()) }

	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := NewShadowCache(ShadowCacheOptions{
		Catalog:               catalog,
		Activations:           activations,
		Capacity:              8,
		PinnedCapacity:        4,
		WriteConfirmPinnedCap: 2,
		Now:                   nowFn,
	})
	writeShadow(t, cache, key, ShadowWriteSourcePassive, time.Unix(100, 0), []byte{0x20})

	scheduler := NewSemanticReadScheduler()
	scheduler.now = nowFn
	scheduler.SetShadowCache(cache)

	firstFetchStarted := make(chan struct{}, 1)
	releaseFirstFetch := make(chan struct{})
	var fetchCalls int32

	fetch := func(context.Context) ([]byte, error) {
		call := atomic.AddInt32(&fetchCalls, 1)
		if call == 1 {
			select {
			case firstFetchStarted <- struct{}{}:
			default:
			}
			<-releaseFirstFetch
			return []byte{0x11}, nil
		}
		nowUnix.Store(time.Unix(202, 0).UnixNano())
		return []byte{0x33}, nil
	}

	type getResult struct {
		value []byte
		err   error
	}
	resultCh := make(chan getResult, 1)
	go func() {
		value, err := scheduler.GetWatch(context.Background(), key, maxAge, fetch)
		resultCh <- getResult{value: value, err: err}
	}()

	select {
	case <-firstFetchStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first fetch start")
	}

	result := cache.Write(ShadowWrite{
		Key:        key,
		Source:     ShadowWriteSourcePassive,
		Confidence: ShadowConfidenceHigh,
		Value:      []byte{0x22},
		ObservedAt: time.Unix(201, 0),
	})
	if !result.Accepted {
		t.Fatalf("passive write rejected: %s", result.Reason)
	}

	close(releaseFirstFetch)

	select {
	case out := <-resultCh:
		if out.err != nil {
			t.Fatalf("GetWatch() error = %v", out.err)
		}
		if len(out.value) != 1 || out.value[0] != 0x33 {
			t.Fatalf("GetWatch() value = %v; want second recompute value [0x33]", out.value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetWatch() timed out")
	}

	if got := atomic.LoadInt32(&fetchCalls); got != 2 {
		t.Fatalf("fetch calls = %d; want 2 after stale same-generation active completion", got)
	}
}

func TestSemanticReadScheduler_SupersededInFlightHalfOpenProbeDoesNotReopen(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)
	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := newTestShadowCache(t, catalog, activations, now, ShadowCacheOptions{})

	scheduler := NewSemanticReadScheduler()
	scheduler.now = func() time.Time { return now }
	scheduler.SetShadowCache(cache)

	var transitions []SemanticReadCircuitBreakerTransition
	scheduler.SetCircuitBreaker(SemanticReadCircuitBreakerOptions{
		FailureBudget:      1,
		OpenCooldown:       10 * time.Second,
		HalfOpenProbeLimit: 1,
		OnTransition: func(event SemanticReadCircuitBreakerTransition) {
			transitions = append(transitions, event)
		},
	})

	failFetch := func(context.Context) ([]byte, error) {
		return nil, errors.New("initial failure")
	}
	if _, err := scheduler.GetWatch(context.Background(), key, 0, failFetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("initial failure err = %v; want non-open failure", err)
	}
	if _, err := scheduler.GetWatch(context.Background(), key, 0, failFetch); !errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("open-state err = %v; want ErrSemanticReadCircuitOpen", err)
	}

	now = now.Add(10 * time.Second)

	firstFetchStarted := make(chan struct{}, 1)
	releaseFirstFetch := make(chan struct{})
	var fetchCalls int32
	liveFetch := func(context.Context) ([]byte, error) {
		call := atomic.AddInt32(&fetchCalls, 1)
		if call == 1 {
			select {
			case firstFetchStarted <- struct{}{}:
			default:
			}
			<-releaseFirstFetch
			return []byte{0x11}, nil
		}
		return []byte{0x44}, nil
	}

	type getResult struct {
		value []byte
		err   error
	}
	resultCh := make(chan getResult, 1)
	go func() {
		value, err := scheduler.GetWatch(context.Background(), key, 0, liveFetch)
		resultCh <- getResult{value: value, err: err}
	}()

	select {
	case <-firstFetchStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for half-open probe fetch")
	}

	write := cache.Write(ShadowWrite{
		Key:        key,
		Source:     ShadowWriteSourcePassive,
		Confidence: ShadowConfidenceHigh,
		Value:      []byte{0x22},
		ObservedAt: now.Add(time.Second),
	})
	if !write.Accepted {
		t.Fatalf("passive write rejected during half-open supersede setup: %s", write.Reason)
	}
	now = now.Add(2 * time.Second)
	close(releaseFirstFetch)

	select {
	case out := <-resultCh:
		if out.err != nil {
			t.Fatalf("half-open superseded call err = %v; want nil after recompute", out.err)
		}
		if len(out.value) != 1 || out.value[0] != 0x44 {
			t.Fatalf("half-open superseded call value = %v; want [0x44]", out.value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("half-open superseded call timed out")
	}

	if got := atomic.LoadInt32(&fetchCalls); got != 2 {
		t.Fatalf("fetch calls = %d; want 2 (superseded probe + recompute)", got)
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
	if transitions[2].From != SemanticReadCircuitStateHalfOpen || transitions[2].To != SemanticReadCircuitStateClosed {
		t.Fatalf("transition[2] = %s->%s; want half-open->closed", transitions[2].From, transitions[2].To)
	}
}

func TestSemanticReadScheduler_SupersededInFlightDoesNotBurnFailureBudget(t *testing.T) {
	t.Parallel()

	key := NewB524WatchKey(0x15, 0x06, 0x03, 0x00, 0x001C)

	now := time.Unix(100, 0)
	nowFn := func() time.Time { return now }

	catalog, activations := testShadowCatalogAndActivations(t, []WatchKey{key}, WatchActivationSourcePoller)
	cache := NewShadowCache(ShadowCacheOptions{
		Catalog:               catalog,
		Activations:           activations,
		Capacity:              8,
		PinnedCapacity:        4,
		WriteConfirmPinnedCap: 2,
		Now:                   nowFn,
	})

	scheduler := NewSemanticReadScheduler()
	scheduler.now = nowFn
	scheduler.SetShadowCache(cache)
	scheduler.SetCircuitBreaker(SemanticReadCircuitBreakerOptions{
		FailureBudget:      2,
		OpenCooldown:       time.Hour,
		HalfOpenProbeLimit: 1,
	})

	var supersedeFetchCalls int32
	supersedeFetch := func(context.Context) ([]byte, error) {
		call := atomic.AddInt32(&supersedeFetchCalls, 1)
		if call == 1 {
			observedAt := now.Add(time.Second)
			result := cache.Write(ShadowWrite{
				Key:        key,
				Source:     ShadowWriteSourcePassive,
				Confidence: ShadowConfidenceHigh,
				Value:      []byte{0x21},
				ObservedAt: observedAt,
			})
			if !result.Accepted {
				return nil, errors.New("passive write rejected while preparing superseded completion")
			}
			now = observedAt.Add(time.Second)
			return []byte{0x30}, nil
		}
		return []byte{0x44}, nil
	}

	supersededValue, err := scheduler.GetWatch(context.Background(), key, 0, supersedeFetch)
	if err != nil {
		t.Fatalf("superseded call err = %v; want nil after recompute", err)
	}
	if len(supersededValue) != 1 || supersededValue[0] != 0x44 {
		t.Fatalf("superseded call value = %v; want [0x44] after recompute", supersededValue)
	}
	if got := atomic.LoadInt32(&supersedeFetchCalls); got != 2 {
		t.Fatalf("supersede fetch calls = %d; want 2 (initial + recompute)", got)
	}

	var failCalls int32
	failFetch := func(context.Context) ([]byte, error) {
		atomic.AddInt32(&failCalls, 1)
		return nil, errors.New("active failure")
	}

	if _, err := scheduler.GetWatch(context.Background(), key, 0, failFetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("first active failure err = %v; want non-open failure", err)
	}
	if got := atomic.LoadInt32(&failCalls); got != 1 {
		t.Fatalf("fail fetch calls after first active failure = %d; want 1", got)
	}

	if _, err := scheduler.GetWatch(context.Background(), key, 0, failFetch); err == nil || errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("second active failure err = %v; want non-open failure (transition to open happens after this call)", err)
	}
	if got := atomic.LoadInt32(&failCalls); got != 2 {
		t.Fatalf("fail fetch calls after second active failure = %d; want 2", got)
	}

	if _, err := scheduler.GetWatch(context.Background(), key, 0, failFetch); !errors.Is(err, ErrSemanticReadCircuitOpen) {
		t.Fatalf("third active failure err = %v; want ErrSemanticReadCircuitOpen", err)
	}
	if got := atomic.LoadInt32(&failCalls); got != 2 {
		t.Fatalf("fail fetch calls after open suppression = %d; want 2", got)
	}
}
