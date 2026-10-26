package runtimecontrol

import (
	"errors"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

// NativeObservation is one executor-originated lifecycle observation of the
// generation that a driver's last successful START admitted, RUNNING or into
// the native owner's recovery (RecoveringStart). It reports what the native
// lifecycle owner did on its own after that admission: a failure replacement
// or retry in BACKOFF, one of its STARTING attempts, the recovered RUNNING or
// DEGRADED generation, or a terminal FAILED for a failure that retrying
// cannot heal (with Quarantined for an unconfirmed close).
//
// A BACKOFF observation may carry the owner's schedule: NextAttempt is the
// ordinal, from one, of the scheduled retry within the current failure series
// and NextAttemptUTC the time it is due. Both are set together or not at all,
// and only for BACKOFF.
type NativeObservation struct {
	State          drivermanager.ObservedState
	Reason         drivermanager.ReasonCode
	Quarantined    bool
	NextAttempt    int
	NextAttemptUTC time.Time
}

// RecoveringStart is the Start outcome of an executor whose native lifecycle
// owner took the opening generation into its own persistent recovery: the
// first attempt failed for a retryable reason, and the owner keeps retrying
// under its own schedule while the driver is desired RUNNING. The service
// admits the generation as it admits a RUNNING one, so its cleanup
// obligations pass to the admitted generation and a later STOP or RESTART
// withdraws it. It publishes Observation instead of RUNNING. The owner's
// later transitions arrive through Service.ObserveNative.
//
// Observation must be BACKOFF with RETRY_SCHEDULED, optionally with its
// schedule. Any other value is treated as a failed Start, so the service
// settles the generation's cleanup obligations and nothing stays open.
type RecoveringStart struct {
	Observation NativeObservation
}

func (r *RecoveringStart) Error() string {
	return "runtimecontrol: start admitted into native recovery"
}

// recoveringStart returns the observation of a valid RecoveringStart outcome.
func recoveringStart(err error) (NativeObservation, bool) {
	var recovering *RecoveringStart
	if !errors.As(err, &recovering) || recovering == nil {
		return NativeObservation{}, false
	}
	obs := recovering.Observation
	if obs.State != drivermanager.ObservedBackoff || obs.Reason != drivermanager.ReasonRetryScheduled || !nativeObservable(obs) {
		return NativeObservation{}, false
	}
	return obs, true
}

// pendingNative is the newest queued executor observation of one driver.
// Only the latest is kept: each one reports the owner's current state.
type pendingNative struct {
	corr LifecycleCorrelation
	obs  NativeObservation
}

// nativeObservable admits the closed vocabulary an executor may report.
// STOPPING, STOPPED and DISABLED belong to the service's own STOP path and
// are never accepted from an executor.
func nativeObservable(obs NativeObservation) bool {
	if obs.Reason == "" {
		return false
	}
	scheduled := obs.NextAttempt != 0 || !obs.NextAttemptUTC.IsZero()
	if scheduled && (obs.State != drivermanager.ObservedBackoff || obs.NextAttempt < 1 || obs.NextAttemptUTC.IsZero()) {
		return false
	}
	switch obs.State {
	case drivermanager.ObservedRunning:
		return obs.Reason == drivermanager.ReasonNone && !obs.Quarantined
	case drivermanager.ObservedDegraded, drivermanager.ObservedStarting, drivermanager.ObservedBackoff:
		return !obs.Quarantined
	case drivermanager.ObservedFailed:
		return true
	}
	return false
}

// ObserveNative queues an executor-originated observation for the admitted
// generation named by corr and wakes the driver's reconciler, which is the
// only goroutine that publishes the driver's observations. The reconciler
// applies it only while no lifecycle slot, decision or attempt is in flight
// and corr still names the admitted generation (its source epoch and driver
// generation); otherwise it is dropped. It never changes desired state, work
// slots, budgets or the admitted generation's cleanup obligations, so a
// FAILED generation is still withdrawn by a later STOP or RESTART. It reports
// whether the observation was queued.
func (s *Service) ObserveNative(corr LifecycleCorrelation, obs NativeObservation) bool {
	if s == nil || !nativeObservable(obs) {
		return false
	}
	d, ok := s.drivers[corr.DriverID]
	if !ok || corr.Epoch == "" || corr.Generation == 0 {
		return false
	}
	s.lifeMu.Lock()
	live := s.started && !s.closed
	s.lifeMu.Unlock()
	if !live {
		return false
	}
	d.mu.Lock()
	// A callback for a generation other than the admitted one would only be
	// dropped by the reconciler; it must not first replace a queued
	// observation of the admitted generation.
	if d.live != nil && (corr.Epoch != d.live.corr.Epoch || corr.Generation != d.live.corr.Generation) {
		d.mu.Unlock()
		s.wakeDriver(d)
		return true
	}
	d.native = &pendingNative{corr: corr, obs: obs}
	d.mu.Unlock()
	s.wakeDriver(d)
	return true
}

// applyNative publishes the queued executor observation from the reconciler
// once the driver has no current work slot. A quarantining observation also
// blocks admission exactly as a service-observed unconfirmed close does.
func (s *Service) applyNative(d *driverState, rec ControlRecord) {
	d.mu.Lock()
	pending := d.native
	// A decision, attempt or ambiguous hold raised after the reconciler's own
	// check keeps the observation queued: the release wakes the reconciler,
	// which then applies it or drops it as stale.
	if pending != nil && (d.inflight != nil || d.untyped != nil || d.deciding > 0 || d.ambiguous != nil) {
		d.mu.Unlock()
		return
	}
	d.native = nil
	live := d.live
	if pending != nil {
		// The record the reconciler read may predate a decision that committed
		// and released since: re-read the durable slot under the guards, which
		// are clear, so an accepted STOP or RESTART is always seen.
		fresh, err := s.store.Driver(d.id)
		if err != nil || fresh.Current != nil {
			d.mu.Unlock()
			return
		}
		rec = fresh
	}
	apply := pending != nil && live != nil && rec.DesiredState == DesiredRunning &&
		!d.quarantined && !d.recoveryBlocked &&
		pending.corr.Epoch == live.corr.Epoch && pending.corr.Generation == live.corr.Generation &&
		d.corr.Epoch == live.corr.Epoch && d.corr.Generation == live.corr.Generation
	if apply && pending.obs.Quarantined {
		d.quarantined = true
	}
	if apply {
		// The decision guard spans the publication: a STOP or RESTART that
		// begins now is refused as busy instead of committing between the
		// checks above and the published observation.
		d.deciding++
	}
	d.mu.Unlock()
	if !apply {
		return
	}
	defer s.releaseDecision(d, false)
	s.observe(d, observation{state: pending.obs.State, reason: pending.obs.Reason, quarantined: pending.obs.Quarantined,
		nextAttempt: pending.obs.NextAttempt, nextAttemptUTC: pending.obs.NextAttemptUTC.UTC()})
}
