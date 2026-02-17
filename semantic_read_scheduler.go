package ebusgateway

import (
	"context"
	"sync"
	"time"
)

// SemanticReadScheduler coalesces identical semantic reads and provides a small
// in-memory cache to avoid multiplying eBUS traffic under multiple consumers.
//
// It is intentionally generic: callers choose the key and the maxAge policy.
type SemanticReadScheduler struct {
	mu      sync.Mutex
	entries map[string]*semanticReadEntry
	now     func() time.Time
}

type semanticReadEntry struct {
	running bool
	done    chan struct{}

	lastOKAt time.Time
	lastOK   []byte
	lastErr  error
}

func NewSemanticReadScheduler() *SemanticReadScheduler {
	return &SemanticReadScheduler{
		entries: make(map[string]*semanticReadEntry),
		now:     time.Now,
	}
}

// Get returns a cached value when it is fresh, otherwise it performs fetch once
// and shares the result with concurrent callers.
//
// If fetch fails, the last successful value (if any) remains cached.
func (s *SemanticReadScheduler) Get(ctx context.Context, key string, maxAge time.Duration, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	if s == nil {
		return fetch(ctx)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if key == "" {
		return fetch(ctx)
	}
	if fetch == nil {
		return nil, context.Canceled
	}

	for {
		s.mu.Lock()
		entry := s.entries[key]
		if entry == nil {
			entry = &semanticReadEntry{}
			s.entries[key] = entry
		}

		if !entry.running && len(entry.lastOK) > 0 && maxAge > 0 {
			age := s.now().Sub(entry.lastOKAt)
			if age >= 0 && age <= maxAge {
				value := append([]byte(nil), entry.lastOK...)
				s.mu.Unlock()
				return value, nil
			}
		}

		if entry.running {
			done := entry.done
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				continue
			}
		}

		entry.running = true
		entry.done = make(chan struct{})
		done := entry.done
		s.mu.Unlock()

		value, err := fetch(ctx)

		s.mu.Lock()
		entry.running = false
		if err == nil {
			entry.lastOKAt = s.now()
			entry.lastOK = append(entry.lastOK[:0], value...)
			entry.lastErr = nil
		} else {
			entry.lastErr = err
		}
		close(done)
		s.mu.Unlock()

		if err != nil {
			return nil, err
		}
		return append([]byte(nil), value...), nil
	}
}

