package ebusgateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultSemanticReadFailureBudget      = 2
	DefaultSemanticReadOpenCooldown       = 15 * time.Second
	DefaultSemanticReadHalfOpenProbeLimit = 1
)

var ErrSemanticReadCircuitOpen = errors.New("semantic read circuit breaker open")

type SemanticReadCircuitState string

const (
	SemanticReadCircuitStateClosed   SemanticReadCircuitState = "closed"
	SemanticReadCircuitStateOpen     SemanticReadCircuitState = "open"
	SemanticReadCircuitStateHalfOpen SemanticReadCircuitState = "half-open"
)

type SemanticReadCircuitBreakerTransition struct {
	Key                 string
	From                SemanticReadCircuitState
	To                  SemanticReadCircuitState
	ConsecutiveFailures int
}

type SemanticReadCircuitBreakerSuppression struct {
	Key             string
	State           SemanticReadCircuitState
	SuppressedTotal uint64
	RetryAfter      time.Duration
}

type SemanticReadCircuitBreakerOptions struct {
	FailureBudget      int
	OpenCooldown       time.Duration
	HalfOpenProbeLimit int
	OnTransition       func(SemanticReadCircuitBreakerTransition)
	OnSuppressed       func(SemanticReadCircuitBreakerSuppression)
}

// SemanticReadScheduler coalesces identical semantic reads and provides a small
// in-memory cache to avoid multiplying eBUS traffic under multiple consumers.
//
// It is intentionally generic: callers choose the key and the maxAge policy.
type SemanticReadScheduler struct {
	mu      sync.Mutex
	entries map[string]*semanticReadEntry
	now     func() time.Time
	breaker semanticReadCircuitBreakerConfig
}

type semanticReadEntry struct {
	running bool
	done    chan struct{}

	lastOKAt time.Time
	lastOK   []byte
	lastErr  error

	breakerState            SemanticReadCircuitState
	consecutiveFailures     int
	openedAt                time.Time
	halfOpenProbesRemaining int
	suppressedTotal         uint64
}

type semanticReadCircuitBreakerConfig struct {
	enabled            bool
	failureBudget      int
	openCooldown       time.Duration
	halfOpenProbeLimit int
	onTransition       func(SemanticReadCircuitBreakerTransition)
	onSuppressed       func(SemanticReadCircuitBreakerSuppression)
}

func NewSemanticReadScheduler() *SemanticReadScheduler {
	return &SemanticReadScheduler{
		entries: make(map[string]*semanticReadEntry),
		now:     time.Now,
	}
}

func (s *SemanticReadScheduler) SetCircuitBreaker(options SemanticReadCircuitBreakerOptions) {
	if s == nil {
		return
	}

	settings := normalizeSemanticReadCircuitBreakerOptions(options)

	s.mu.Lock()
	s.breaker = settings
	for _, entry := range s.entries {
		entry.lastErr = nil
		if entry.breakerState == "" {
			entry.breakerState = SemanticReadCircuitStateClosed
		}
		if settings.enabled {
			continue
		}
		entry.breakerState = SemanticReadCircuitStateClosed
		entry.consecutiveFailures = 0
		entry.openedAt = time.Time{}
		entry.halfOpenProbesRemaining = 0
	}
	s.mu.Unlock()
}

func normalizeSemanticReadCircuitBreakerOptions(options SemanticReadCircuitBreakerOptions) semanticReadCircuitBreakerConfig {
	failureBudget := options.FailureBudget
	openCooldown := options.OpenCooldown
	if openCooldown == 0 {
		openCooldown = DefaultSemanticReadOpenCooldown
	}
	halfOpenProbeLimit := options.HalfOpenProbeLimit
	if halfOpenProbeLimit == 0 {
		halfOpenProbeLimit = DefaultSemanticReadHalfOpenProbeLimit
	}
	if halfOpenProbeLimit < 1 {
		halfOpenProbeLimit = 1
	}
	if openCooldown < 0 {
		openCooldown = 0
	}
	return semanticReadCircuitBreakerConfig{
		enabled:            failureBudget > 0,
		failureBudget:      failureBudget,
		openCooldown:       openCooldown,
		halfOpenProbeLimit: halfOpenProbeLimit,
		onTransition:       options.OnTransition,
		onSuppressed:       options.OnSuppressed,
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
		var preTransition *SemanticReadCircuitBreakerTransition
		var preSuppression *SemanticReadCircuitBreakerSuppression
		var preErr error

		s.mu.Lock()
		entry := s.entries[key]
		if entry == nil {
			entry = &semanticReadEntry{
				breakerState: SemanticReadCircuitStateClosed,
			}
			s.entries[key] = entry
		} else if entry.breakerState == "" {
			entry.breakerState = SemanticReadCircuitStateClosed
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

		preTransition, preSuppression, preErr = s.guardBreakerLocked(key, entry, s.now())
		if preErr != nil {
			s.mu.Unlock()
			s.emitBreakerTransition(preTransition)
			s.emitBreakerSuppression(preSuppression)
			return nil, preErr
		}

		if entry.breakerState == SemanticReadCircuitStateHalfOpen {
			entry.halfOpenProbesRemaining--
		}

		entry.running = true
		entry.done = make(chan struct{})
		done := entry.done
		s.mu.Unlock()
		s.emitBreakerTransition(preTransition)

		value, err := fetch(ctx)

		var postTransition *SemanticReadCircuitBreakerTransition
		s.mu.Lock()
		entry.running = false
		if err == nil {
			entry.lastOKAt = s.now()
			entry.lastOK = append(entry.lastOK[:0], value...)
			entry.lastErr = nil
			entry.consecutiveFailures = 0
			postTransition = s.transitionBreakerLocked(key, entry, SemanticReadCircuitStateClosed)
		} else {
			entry.lastErr = err
			postTransition = s.recordFailureLocked(key, entry)
		}
		close(done)
		s.mu.Unlock()
		s.emitBreakerTransition(postTransition)

		if err != nil {
			return nil, err
		}
		return append([]byte(nil), value...), nil
	}
}

func (s *SemanticReadScheduler) guardBreakerLocked(key string, entry *semanticReadEntry, now time.Time) (*SemanticReadCircuitBreakerTransition, *SemanticReadCircuitBreakerSuppression, error) {
	if !s.breaker.enabled {
		return nil, nil, nil
	}

	switch entry.breakerState {
	case SemanticReadCircuitStateOpen:
		if s.breaker.openCooldown <= 0 || now.Sub(entry.openedAt) >= s.breaker.openCooldown {
			transition := s.transitionBreakerLocked(key, entry, SemanticReadCircuitStateHalfOpen)
			entry.halfOpenProbesRemaining = s.breaker.halfOpenProbeLimit
			return transition, nil, nil
		}

		retryAfter := s.breaker.openCooldown - now.Sub(entry.openedAt)
		if retryAfter < 0 {
			retryAfter = 0
		}
		entry.suppressedTotal++
		suppression := &SemanticReadCircuitBreakerSuppression{
			Key:             key,
			State:           SemanticReadCircuitStateOpen,
			SuppressedTotal: entry.suppressedTotal,
			RetryAfter:      retryAfter,
		}
		return nil, suppression, fmt.Errorf(
			"%w: key=%s retry_after=%s",
			ErrSemanticReadCircuitOpen,
			key,
			retryAfter.Round(time.Millisecond),
		)
	case SemanticReadCircuitStateHalfOpen:
		if entry.halfOpenProbesRemaining > 0 {
			return nil, nil, nil
		}
		transition := s.transitionBreakerLocked(key, entry, SemanticReadCircuitStateOpen)
		entry.openedAt = now
		entry.suppressedTotal++
		retryAfter := s.breaker.openCooldown
		if retryAfter < 0 {
			retryAfter = 0
		}
		suppression := &SemanticReadCircuitBreakerSuppression{
			Key:             key,
			State:           SemanticReadCircuitStateOpen,
			SuppressedTotal: entry.suppressedTotal,
			RetryAfter:      retryAfter,
		}
		return transition, suppression, fmt.Errorf(
			"%w: key=%s retry_after=%s",
			ErrSemanticReadCircuitOpen,
			key,
			retryAfter.Round(time.Millisecond),
		)
	default:
		return nil, nil, nil
	}
}

func (s *SemanticReadScheduler) recordFailureLocked(key string, entry *semanticReadEntry) *SemanticReadCircuitBreakerTransition {
	if !s.breaker.enabled {
		return nil
	}
	entry.consecutiveFailures++
	switch entry.breakerState {
	case SemanticReadCircuitStateHalfOpen:
		if entry.halfOpenProbesRemaining > 0 {
			return nil
		}
		transition := s.transitionBreakerLocked(key, entry, SemanticReadCircuitStateOpen)
		entry.openedAt = s.now()
		return transition
	case SemanticReadCircuitStateClosed:
		if entry.consecutiveFailures >= s.breaker.failureBudget {
			transition := s.transitionBreakerLocked(key, entry, SemanticReadCircuitStateOpen)
			entry.openedAt = s.now()
			return transition
		}
	}
	return nil
}

func (s *SemanticReadScheduler) transitionBreakerLocked(key string, entry *semanticReadEntry, to SemanticReadCircuitState) *SemanticReadCircuitBreakerTransition {
	if !s.breaker.enabled {
		return nil
	}
	from := entry.breakerState
	if from == "" {
		from = SemanticReadCircuitStateClosed
	}
	if to == "" {
		to = SemanticReadCircuitStateClosed
	}
	if from == to {
		return nil
	}

	entry.breakerState = to
	switch to {
	case SemanticReadCircuitStateClosed:
		entry.consecutiveFailures = 0
		entry.openedAt = time.Time{}
		entry.halfOpenProbesRemaining = s.breaker.halfOpenProbeLimit
	case SemanticReadCircuitStateHalfOpen:
		entry.halfOpenProbesRemaining = s.breaker.halfOpenProbeLimit
	case SemanticReadCircuitStateOpen:
		entry.halfOpenProbesRemaining = 0
	}

	return &SemanticReadCircuitBreakerTransition{
		Key:                 key,
		From:                from,
		To:                  to,
		ConsecutiveFailures: entry.consecutiveFailures,
	}
}

func (s *SemanticReadScheduler) emitBreakerTransition(event *SemanticReadCircuitBreakerTransition) {
	if s == nil || event == nil {
		return
	}
	s.mu.Lock()
	callback := s.breaker.onTransition
	s.mu.Unlock()
	if callback != nil {
		callback(*event)
	}
}

func (s *SemanticReadScheduler) emitBreakerSuppression(event *SemanticReadCircuitBreakerSuppression) {
	if s == nil || event == nil {
		return
	}
	s.mu.Lock()
	callback := s.breaker.onSuppressed
	s.mu.Unlock()
	if callback != nil {
		callback(*event)
	}
}
