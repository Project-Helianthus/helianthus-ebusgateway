package runtimecontrol

import "github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"

// NativeObservation is one executor-originated lifecycle observation of the
// generation that a driver's last successful START admitted. It reports what
// the native lifecycle owner did on its own after RUNNING: a failure
// replacement in BACKOFF, one of its STARTING attempts, the recovered RUNNING
// or DEGRADED generation, or a terminal FAILED once its bounded retry budget
// is spent (with Quarantined for an unconfirmed close).
type NativeObservation struct {
	State       drivermanager.ObservedState
	Reason      drivermanager.ReasonCode
	Quarantined bool
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
	apply := pending != nil && live != nil && rec.DesiredState == DesiredRunning &&
		!d.quarantined && !d.recoveryBlocked &&
		pending.corr.Epoch == live.corr.Epoch && pending.corr.Generation == live.corr.Generation &&
		d.corr.Epoch == live.corr.Epoch && d.corr.Generation == live.corr.Generation
	if apply && pending.obs.Quarantined {
		d.quarantined = true
	}
	d.mu.Unlock()
	if !apply {
		return
	}
	s.observe(d, observation{state: pending.obs.State, reason: pending.obs.Reason, quarantined: pending.obs.Quarantined})
}
