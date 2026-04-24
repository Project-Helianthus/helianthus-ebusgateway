package ebusgateway

import (
	"sync"
	"time"
)

const (
	StartupAdmissionRejoinBackoffBaseSeconds = 5
	StartupAdmissionRejoinBackoffMaxSeconds  = 60

	StartupAdmissionEscalationFailureCountThreshold = 5
	StartupAdmissionEscalationDurationSeconds       = 300
	StartupAdmissionEscalationWindowSeconds         = 900
)

// RejoinBackoffSchedule returns the nth attempt's backoff duration:
// Base * 2^(n-1), capped at Max. attempt=1 → Base; attempt=2 → 2*Base; etc.
func RejoinBackoffSchedule(attempt int, baseSeconds, maxSeconds int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	secs := baseSeconds
	for i := 1; i < attempt; i++ {
		secs *= 2
		if secs >= maxSeconds {
			return time.Duration(maxSeconds) * time.Second
		}
	}
	return time.Duration(secs) * time.Second
}

// DegradedModeAccumulator tracks both consecutive rejoin failures and a
// rolling-window cumulative degraded-duration counter. Escalation fires
// when EITHER threshold is reached. Per AD17, the accumulator is
// in-process only — no restart persistence.
type DegradedModeAccumulator struct {
	mu  sync.Mutex
	now func() time.Time

	consecutiveFailures int

	buckets     [900]uint16
	bucketIndex int
	bucketStart time.Time

	inDegradedSince time.Time
	escalated       bool

	continuousThresholdSeconds int
	windowSeconds              int
	failureCountThreshold      int
}

func NewDegradedModeAccumulator() *DegradedModeAccumulator {
	return &DegradedModeAccumulator{
		now:                        time.Now,
		continuousThresholdSeconds: StartupAdmissionEscalationDurationSeconds,
		windowSeconds:              StartupAdmissionEscalationWindowSeconds,
		failureCountThreshold:      StartupAdmissionEscalationFailureCountThreshold,
	}
}

// RecordFailure increments the consecutive failure counter and updates the
// accumulator with the failure moment. Returns true if the accumulator
// crossed the escalation threshold as a result of this call.
func (a *DegradedModeAccumulator) RecordFailure() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.consecutiveFailures++
	now := a.now()
	if a.inDegradedSince.IsZero() {
		a.inDegradedSince = now
		a.bucketStart = now
		a.bucketIndex = 0
	}
	return a.checkEscalationLocked(now)
}

// RecordDegradedTick advances the cumulative accumulator by one second of
// degraded-state occupancy. Called by a 1-second ticker in runtime when
// admission state is degraded. Returns true if the accumulator crossed the
// escalation threshold.
func (a *DegradedModeAccumulator) RecordDegradedTick() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := a.now()
	a.advanceBucketsLocked(now)
	a.buckets[a.bucketIndex] = 1000
	return a.checkEscalationLocked(now)
}

// RecordSuccess clears the consecutive failure counter. Does NOT clear the
// rolling accumulator — flaps remain visible in the window. Latch clears
// only after state_min_stability_s of continuous active (caller handles
// the stability window separately).
func (a *DegradedModeAccumulator) RecordSuccess() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.consecutiveFailures = 0
	a.inDegradedSince = time.Time{}
}

// ClearLatch clears the escalated flag after state_min_stability_s of
// continuous active state. Caller is responsible for the stability timer;
// this method only flips the flag.
func (a *DegradedModeAccumulator) ClearLatch() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.escalated = false
}

// Escalated reports whether the latch is currently set.
func (a *DegradedModeAccumulator) Escalated() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.escalated
}

// CumulativeDegradedMs returns the sum of degraded-ms across all buckets in
// the rolling window.
func (a *DegradedModeAccumulator) CumulativeDegradedMs() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	var sum uint64
	for _, b := range a.buckets {
		sum += uint64(b)
	}
	return sum
}

// advanceBucketsLocked rotates the ring buffer forward based on elapsed
// wall-clock time since the last advance. Zero-fills skipped buckets.
func (a *DegradedModeAccumulator) advanceBucketsLocked(now time.Time) {
	if a.bucketStart.IsZero() {
		a.bucketStart = now
		a.bucketIndex = 0
		return
	}
	elapsed := now.Sub(a.bucketStart)
	if elapsed < time.Second {
		return
	}
	secsElapsed := int(elapsed.Seconds())
	for i := 0; i < secsElapsed; i++ {
		a.bucketIndex = (a.bucketIndex + 1) % len(a.buckets)
		a.buckets[a.bucketIndex] = 0
	}
	a.bucketStart = a.bucketStart.Add(time.Duration(secsElapsed) * time.Second)
}

func (a *DegradedModeAccumulator) checkEscalationLocked(now time.Time) bool {
	if a.escalated {
		return false
	}
	if a.consecutiveFailures >= a.failureCountThreshold {
		a.escalated = true
		return true
	}
	if !a.inDegradedSince.IsZero() {
		continuousSeconds := int(now.Sub(a.inDegradedSince).Seconds())
		if continuousSeconds >= a.continuousThresholdSeconds {
			a.escalated = true
			return true
		}
	}
	var cumMs uint64
	for _, b := range a.buckets {
		cumMs += uint64(b)
	}
	if cumMs >= uint64(a.continuousThresholdSeconds)*1000 {
		a.escalated = true
		return true
	}
	return false
}
