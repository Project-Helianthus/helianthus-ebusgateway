package ebusgateway

import (
	"context"
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
