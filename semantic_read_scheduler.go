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

	semanticReadShadowMaxPreActivePasses = 2
	semanticReadShadowMaxRecomputeCycles = 1
)

var (
	ErrSemanticReadCircuitOpen           = errors.New("semantic read circuit breaker open")
	ErrSemanticReadSupersededInFlight    = errors.New("semantic read superseded while in flight")
	ErrSemanticReadRevalidationExhausted = errors.New("semantic read shadow revalidation exhausted")
)

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
	shadow  *ShadowCache
}

type semanticReadEntry struct {
	running bool
	done    chan struct{}

	lastOKAt time.Time
	lastOK   []byte
	lastErr  error

	lastOKShadowGeneration uint64
	lastOKHasGeneration    bool
	lastOKFromShadow       bool

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

type semanticReadShadowCandidate struct {
	value      []byte
	generation uint64
}

type semanticReadShadowWriteResult struct {
	attempted       bool
	startGeneration uint64
	result          ShadowWriteResult
}

func NewSemanticReadScheduler() *SemanticReadScheduler {
	return &SemanticReadScheduler{
		entries: make(map[string]*semanticReadEntry),
		now:     time.Now,
	}
}

func (s *SemanticReadScheduler) SetShadowCache(cache *ShadowCache) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.shadow = cache
	s.mu.Unlock()
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
	return s.getWithWatchKey(ctx, key, nil, maxAge, fetch)
}

func (s *SemanticReadScheduler) GetWatch(ctx context.Context, key WatchKey, maxAge time.Duration, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	if key == nil {
		return s.getWithWatchKey(ctx, "", nil, maxAge, fetch)
	}
	return s.getWithWatchKey(ctx, key.Canonical(), key, maxAge, fetch)
}

func (s *SemanticReadScheduler) getWithWatchKey(ctx context.Context, key string, watchKey WatchKey, maxAge time.Duration, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
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

	shadow := s.shadowSnapshot()
	shadowPassesRemaining := semanticReadShadowMaxPreActivePasses
	recomputeRemaining := semanticReadShadowMaxRecomputeCycles

	for {
		var preTransition *SemanticReadCircuitBreakerTransition
		var preSuppression *SemanticReadCircuitBreakerSuppression
		var preErr error
		var candidate *semanticReadShadowCandidate

		if shadow != nil && watchKey != nil && maxAge > 0 && shadowPassesRemaining > 0 {
			candidate = consultSemanticReadShadow(shadow, watchKey, maxAge)
			shadowPassesRemaining--
		}

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

		if candidate != nil {
			if snapshot, ok := revalidateSemanticReadShadowCandidate(shadow, watchKey, candidate, maxAge, s.now()); ok {
				entry.lastOKAt = s.now()
				entry.lastOK = append(entry.lastOK[:0], candidate.value...)
				entry.lastErr = nil
				entry.lastOKShadowGeneration = snapshot.Generation
				entry.lastOKHasGeneration = true
				entry.lastOKFromShadow = true
				value := append([]byte(nil), entry.lastOK...)
				s.mu.Unlock()
				return value, nil
			}
			if shadowPassesRemaining > 0 {
				s.mu.Unlock()
				continue
			}
		}

		if !entry.running && len(entry.lastOK) > 0 && maxAge > 0 {
			age := s.now().Sub(entry.lastOKAt)
			if age >= 0 && age <= maxAge {
				if shadow == nil || watchKey == nil || !entry.lastOKHasGeneration {
					value := append([]byte(nil), entry.lastOK...)
					s.mu.Unlock()
					return value, nil
				}

				snapshot := shadow.SnapshotEligibility(watchKey)
				if snapshot.Generation != entry.lastOKShadowGeneration {
					clearSemanticReadCacheLocked(entry)
				} else if entry.lastOKFromShadow && !shadowSnapshotEligibleForMaxAge(snapshot, maxAge, s.now()) {
					clearSemanticReadCacheLocked(entry)
				} else {
					value := append([]byte(nil), entry.lastOK...)
					s.mu.Unlock()
					return value, nil
				}
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

		startGeneration := uint64(0)
		if shadow != nil && watchKey != nil {
			startGeneration = shadow.SnapshotEligibility(watchKey).Generation
		}

		value, err := fetch(ctx)
		shadowWrite := semanticReadShadowWriteResult{
			startGeneration: startGeneration,
		}
		if err == nil && shadow != nil && watchKey != nil {
			shadowWrite.attempted = true
			shadowWrite.result = shadow.Write(ShadowWrite{
				Key:             watchKey,
				Source:          ShadowWriteSourceActiveConfirmed,
				Confidence:      ShadowConfidenceHigh,
				Value:           value,
				ObservedAt:      s.now(),
				StartGeneration: startGeneration,
			})
			if !shadowWrite.result.Accepted {
				switch shadowWrite.result.Reason {
				case ShadowWriteRejectionReasonGenerationAdvanced,
					ShadowWriteRejectionReasonStaleTimestamp,
					ShadowWriteRejectionReasonSameTimestampConflict:
					err = ErrSemanticReadSupersededInFlight
				}
			}
		}

		var postTransition *SemanticReadCircuitBreakerTransition
		invalidatedInFlight := false
		s.mu.Lock()
		entry.running = false
		if errors.Is(err, ErrSemanticReadSupersededInFlight) {
			invalidatedInFlight = true
		}
		if err == nil && shadow != nil && watchKey != nil {
			currentGeneration := shadow.SnapshotEligibility(watchKey).Generation
			expectedGeneration := startGeneration
			if shadowWrite.attempted && shadowWrite.result.Accepted {
				expectedGeneration = shadowWrite.result.Generation
			}
			if currentGeneration != expectedGeneration {
				err = ErrSemanticReadSupersededInFlight
				invalidatedInFlight = true
			}
		}
		if err == nil {
			entry.lastOKAt = s.now()
			entry.lastOK = append(entry.lastOK[:0], value...)
			entry.lastErr = nil
			entry.lastOKFromShadow = false
			if shadow != nil && watchKey != nil {
				entry.lastOKHasGeneration = true
				if shadowWrite.attempted && shadowWrite.result.Accepted {
					entry.lastOKShadowGeneration = shadowWrite.result.Generation
				} else {
					entry.lastOKShadowGeneration = startGeneration
				}
			} else {
				entry.lastOKHasGeneration = false
				entry.lastOKShadowGeneration = 0
			}
			entry.consecutiveFailures = 0
			postTransition = s.transitionBreakerLocked(key, entry, SemanticReadCircuitStateClosed)
		} else {
			entry.lastErr = err
			if invalidatedInFlight {
				clearSemanticReadCacheLocked(entry)
				if s.breaker.enabled && entry.breakerState == SemanticReadCircuitStateHalfOpen && entry.halfOpenProbesRemaining < s.breaker.halfOpenProbeLimit {
					entry.halfOpenProbesRemaining++
				}
			} else {
				postTransition = s.recordFailureLocked(key, entry)
			}
		}
		close(done)
		s.mu.Unlock()
		s.emitBreakerTransition(postTransition)

		if err != nil {
			if invalidatedInFlight {
				if recomputeRemaining > 0 {
					recomputeRemaining--
					shadowPassesRemaining = semanticReadShadowMaxPreActivePasses
					continue
				}
				return nil, fmt.Errorf("%w: key=%s", ErrSemanticReadRevalidationExhausted, key)
			}
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

func (s *SemanticReadScheduler) shadowSnapshot() *ShadowCache {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	cache := s.shadow
	s.mu.Unlock()
	return cache
}

func consultSemanticReadShadow(cache *ShadowCache, key WatchKey, maxAge time.Duration) *semanticReadShadowCandidate {
	if cache == nil || key == nil || maxAge <= 0 {
		return nil
	}
	result := cache.Lookup(key, maxAge)
	if !result.Found || !result.Eligible || result.Entry.State != ShadowEntryStatePresent {
		return nil
	}
	return &semanticReadShadowCandidate{
		value:      append([]byte(nil), result.Entry.Value...),
		generation: result.Entry.Generation,
	}
}

func revalidateSemanticReadShadowCandidate(cache *ShadowCache, key WatchKey, candidate *semanticReadShadowCandidate, maxAge time.Duration, now time.Time) (ShadowEligibilitySnapshot, bool) {
	if cache == nil || key == nil || candidate == nil {
		return ShadowEligibilitySnapshot{}, false
	}
	snapshot := cache.SnapshotEligibility(key)
	if snapshot.Generation != candidate.generation {
		return snapshot, false
	}
	if !shadowSnapshotEligibleForMaxAge(snapshot, maxAge, now) {
		return snapshot, false
	}
	return snapshot, true
}

func shadowSnapshotEligibleForMaxAge(snapshot ShadowEligibilitySnapshot, maxAge time.Duration, now time.Time) bool {
	if !snapshot.Present || !snapshot.Eligible || snapshot.State != ShadowEntryStatePresent {
		return false
	}
	if maxAge <= 0 {
		return false
	}
	if !snapshot.ExpiresAt.IsZero() && now.After(snapshot.ExpiresAt) {
		return false
	}
	age := now.Sub(snapshot.ObservedAt)
	return age >= 0 && age <= maxAge
}

func clearSemanticReadCacheLocked(entry *semanticReadEntry) {
	if entry == nil {
		return
	}
	entry.lastOKAt = time.Time{}
	entry.lastOK = entry.lastOK[:0]
	entry.lastOKShadowGeneration = 0
	entry.lastOKHasGeneration = false
	entry.lastOKFromShadow = false
}
