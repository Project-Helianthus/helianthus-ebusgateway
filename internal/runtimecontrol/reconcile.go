package runtimecontrol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

// Bounded reconciliation. Each driver has one reconciler goroutine that reads
// its committed typed slots and makes one durable step at a time. Work is
// derived only from the slots (and, for a driver with no slot and desired
// RUNNING, from the process-start untyped attempt); desired state alone never
// creates typed work, and completed operations leave no durable record.
//
// Slot semantics used by the reconciler:
//
//   - PENDING: accepted, not begun; no correlation and no obligation yet.
//   - ACTIVE: the slot's correlation (epoch and generation) is being started
//     or stopped; both cleanup obligations are pending.
//   - CLEANUP: the slot is settling; with both obligations cleared it is
//     terminal and is pruned once its D5 material is secured.
//   - A current START with a reserved successor is canceled: its attempt is
//     fenced and only its cleanup obligations run.

func (s *Service) worker(d *driverState) {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-d.wake:
		}
		s.reconcile(d)
	}
}

func (s *Service) reconcile(d *driverState) {
	for s.ctx.Err() == nil {
		progressed, delay := s.reconcileStep(d)
		if delay > 0 {
			time.AfterFunc(delay, func() { s.wakeDriver(d) })
			return
		}
		if !progressed {
			return
		}
	}
}

func (s *Service) retryLater() (bool, time.Duration) { return false, s.retryDelay(1) }

func (s *Service) reconcileStep(d *driverState) (bool, time.Duration) {
	rec, err := s.store.Driver(d.id)
	if err != nil || rec.Failed || rec.ReadOnly || !rec.Enabled {
		return false, 0
	}
	d.mu.Lock()
	// The slots are read before the hold is checked: a decision raises its
	// deciding count before it commits, so a committed slot seen here with
	// no decision held already has its acceptance published. A held decision
	// owns the driver until then (or until its ambiguous commit is settled)
	// and wakes the reconciler when it is released.
	held := d.deciding > 0
	blocked := d.quarantined || d.recoveryBlocked
	needsUntyped := !d.autoStartDone && d.live == nil && d.untyped == nil
	d.mu.Unlock()
	if held || blocked {
		return false, 0
	}
	t := rec.Current
	if t == nil {
		if rec.DesiredState == DesiredRunning && needsUntyped {
			s.runUntyped(d)
			return true, 0
		}
		// Executor-originated observations are serialized with every
		// service transition: they are applied only here, by the driver's
		// own reconciler, while no work slot exists.
		s.applyNative(d, rec)
		return false, 0
	}
	if t.Phase == PhaseCleanup && !t.NativeClosePending && !t.SemanticCleanupPending {
		return s.finish(d, rec, *t)
	}
	if t.Phase == PhaseCleanup && s.cleanupSpent(*t) {
		// The persisted attempt limit or the nonrenewable deadline is spent:
		// no further Stop runs, whether this is a retry or a recovery. The
		// obligations stay durable and are reported, never renewed.
		return s.afterCleanup(d, *t)
	}
	switch {
	case t.Phase == PhaseCleanup && t.Kind == KindStop:
		return s.stepStop(d, *t)
	case t.Phase == PhaseCleanup:
		return s.stepCleanup(d, *t)
	case t.Kind == KindCancel || (t.Kind == KindStart && rec.Reserved != nil):
		return s.stepCancel(d, *t)
	case t.Kind == KindStop:
		return s.stepStop(d, *t)
	case t.Kind == KindRestart:
		return s.stepRestart(d, *t)
	case t.Kind == KindStart:
		return s.stepStart(d, *t)
	}
	return false, 0
}

// startLimit bounds the START attempts of a typed slot. MaxAttempts governs
// start attempts only: the cleanup Stop owns one further reserved attempt
// (cleanupLimit), so a limit of one cannot let START consume the attempt that
// native close and semantic cleanup need. The store caps a slot's persisted
// attempts at DefaultMaxAttempts, so a limit at that cap leaves the reserve
// by starting one attempt fewer.
func (s *Service) startLimit() int {
	if s.maxAttempts >= DefaultMaxAttempts {
		return DefaultMaxAttempts - 1
	}
	return s.maxAttempts
}

// driverRetryAllows applies one driver's RetryPolicy after a retryable
// failure of start attempt n of the current epoch (the epoch's generation
// counts its attempts). The service limits still apply on their own.
func (d *driverState) driverRetryAllows(n int) bool {
	return d.retry.MaxAttempts == 0 || n < d.retry.MaxAttempts
}

// retryDelayFor is the backoff before the next start attempt of a driver:
// its own RetryPolicy delay when set, otherwise the service retry delay.
func (s *Service) retryDelayFor(d *driverState, attempt int) time.Duration {
	if d.retry.Delay > 0 {
		return d.retry.Delay
	}
	return s.retryDelay(attempt)
}

// cleanupLimit is the highest persisted AttemptsUsed a slot may reach: the
// start attempts plus the single reserved cleanup attempt. Each cleanup Stop
// is charged durably before it runs and the slot's original deadline is never
// renewed.
func (s *Service) cleanupLimit() int {
	return s.startLimit() + 1
}

// cleanupSpent reports whether a slot's persisted attempt limit or deadline
// leaves no budget for another cleanup Stop.
func (s *Service) cleanupSpent(t Transition) bool {
	return t.AttemptsUsed >= s.cleanupLimit() || (!t.DeadlineUTC.IsZero() && !s.now().Before(t.DeadlineUTC))
}

func sameIdentity(a, b *Transition) bool {
	return a != nil && b != nil && a.Kind == b.Kind && a.Sequence == b.Sequence && a.DesiredRevision == b.DesiredRevision
}

func phaseRank(p TransitionPhase) int {
	switch p {
	case PhasePending:
		return 0
	case PhaseActive:
		return 1
	case PhaseCleanup:
		return 2
	}
	return -1
}

// checkWorkUpdate is the service-side progression rule for one stored
// transition identity. Besides phase, attempts and deadline monotonicity it
// permits only valid correlation progression and never re-activates a
// cleared cleanup obligation:
//
//   - an unassigned correlation (empty epoch) may be assigned once;
//   - within the same epoch the generation may only grow, and an obligation
//     may only go from pending to cleared, except at the PENDING to ACTIVE
//     step where the new attempt incurs it;
//   - a new epoch replaces the old one only after both of the old
//     correlation's obligations are cleared (the new correlation then incurs
//     its own);
//   - an established epoch is never removed.
func checkWorkUpdate(stored, next *Transition) error {
	if stored == nil || next == nil {
		return nil
	}
	if !sameIdentity(stored, next) {
		return fmt.Errorf("%w: transition identity changed", ErrInvalidWork)
	}
	switch {
	case phaseRank(next.Phase) < phaseRank(stored.Phase) || phaseRank(next.Phase) < 0:
		return fmt.Errorf("%w: phase moves backwards", ErrInvalidWork)
	case next.AttemptsUsed < stored.AttemptsUsed:
		return fmt.Errorf("%w: attempts used decrease", ErrInvalidWork)
	case !stored.DeadlineUTC.IsZero() && (next.DeadlineUTC.IsZero() || next.DeadlineUTC.After(stored.DeadlineUTC)):
		return fmt.Errorf("%w: established deadline extended or removed", ErrInvalidWork)
	}
	switch {
	case stored.Epoch == "":
		return nil
	case next.Epoch == "":
		return fmt.Errorf("%w: established correlation removed", ErrInvalidWork)
	case next.Epoch != stored.Epoch:
		if stored.NativeClosePending || stored.SemanticCleanupPending {
			return fmt.Errorf("%w: correlation replaced before its obligations cleared", ErrInvalidWork)
		}
		return nil
	case next.Generation < stored.Generation:
		return fmt.Errorf("%w: generation decreases", ErrInvalidWork)
	}
	incurring := stored.Phase == PhasePending && next.Phase == PhaseActive
	if !incurring && ((!stored.NativeClosePending && next.NativeClosePending) ||
		(!stored.SemanticCleanupPending && next.SemanticCleanupPending)) {
		return fmt.Errorf("%w: cleared cleanup obligation re-activated", ErrInvalidWork)
	}
	return nil
}

// commitCurrent durably updates the current slot, carrying the freshly read
// reserved slot. A decision that lands between the read and the commit makes
// the store refuse the stale pair; the update is then retried on a new read.
func (s *Service) commitCurrent(d *driverState, next Transition) error {
	var lastErr error
	for i := 0; i < maxCommitAttempts; i++ {
		if s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		rec, err := s.store.Driver(d.id)
		if err != nil {
			return err
		}
		if !sameIdentity(rec.Current, &next) {
			return ErrStaleSlot
		}
		if err := checkWorkUpdate(rec.Current, &next); err != nil {
			s.report(err)
			return err
		}
		lastErr = s.store.CommitWork(s.ctx, d.id, &next, rec.Reserved)
		if lastErr == nil {
			return nil
		}
		var refusal *Refusal
		if errors.As(lastErr, &refusal) {
			break
		}
	}
	s.report(lastErr)
	s.armReadback()
	return lastErr
}

func (s *Service) corr(d *driverState, t Transition) LifecycleCorrelation {
	return LifecycleCorrelation{DriverID: d.id, Epoch: t.Epoch, Generation: t.Generation, Sequence: t.Sequence}
}

// newEpoch allocates a source epoch for this incarnation (final D1: a new
// epoch on process start, RESTART and START after STOP).
func (s *Service) newEpoch(d *driverState) SourceEpoch {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.epochOrdinal++
	return SourceEpoch(fmt.Sprintf("%s.%s.%d", s.incarnation, d.id, d.epochOrdinal))
}

func (s *Service) newAttempt(corr LifecycleCorrelation, deadline time.Time, sequence LifecycleSequence) *attempt {
	ctx, cancel := context.WithTimeout(s.ctx, deadline.Sub(s.now()))
	return &attempt{corr: corr, ctx: ctx, cancel: cancel, sequence: sequence, deadline: deadline}
}

// settle waits for in-flight decisions on the driver, then runs fn under the
// driver mutex unless the attempt was fenced. It reports whether fn ran.
func (s *Service) settle(d *driverState, a *attempt, fn func()) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for d.deciding > 0 && s.ctx.Err() == nil {
		d.cond.Wait()
	}
	if a.fenced {
		return false
	}
	fn()
	return true
}

// setCorr records the correlation the driver's next observations refer to.
func (s *Service) setCorr(d *driverState, c LifecycleCorrelation) {
	d.mu.Lock()
	d.corr = c
	d.mu.Unlock()
}

func (s *Service) quarantine(d *driverState, t Transition) {
	d.mu.Lock()
	d.quarantined = true
	d.mu.Unlock()
	s.observe(d, observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonCloseUnconfirmed, quarantined: true})
	s.appendDiagnostic(d, t, "CLOSE_UNCONFIRMED")
}

// settleObligations runs one Stop for the slot's correlation and returns the
// slot moved to CLEANUP with the obligations the stop proved cleared. The
// cleanup attempt is charged durably before the executor is invoked, so
// process loss during Stop, or a failed commit of its result, can never
// grant an extra attempt on recovery. An error means the charge was not
// committed and no Stop ran.
func (s *Service) settleObligations(d *driverState, t Transition) (Transition, error) {
	next := t
	next.Phase = PhaseCleanup
	if t.Epoch == "" {
		// The slot never began: no correlation, no obligation incurred.
		next.NativeClosePending, next.SemanticCleanupPending = false, false
		return next, nil
	}
	if t.NativeClosePending || t.SemanticCleanupPending {
		adopted := d.cleanupChargeTo != 0 && d.cleanupChargeSeq == t.Sequence && t.AttemptsUsed == d.cleanupChargeTo
		d.cleanupChargeTo = 0
		if !adopted && t.AttemptsUsed >= s.cleanupLimit() {
			// No persisted attempt is left to charge: running Stop uncharged
			// would grant an extra attempt on every recovery. The obligations
			// stay durable and the caller reports the cleanup as stalled.
			return next, nil
		}
		charged := t
		if !adopted {
			// A charge whose commit outcome was unknown and that readback
			// adopted is already stored: it is not charged again.
			charged.AttemptsUsed++
			d.cleanupChargeSeq, d.cleanupChargeTo = t.Sequence, charged.AttemptsUsed
			if err := s.commitCurrent(d, charged); err != nil {
				return t, err
			}
			d.cleanupChargeTo = 0
		}
		next.AttemptsUsed = charged.AttemptsUsed
		out := s.stopBounded(d, s.corr(d, t), t.DeadlineUTC)
		next.NativeClosePending = t.NativeClosePending && !out.NativeClosed
		next.SemanticCleanupPending = t.SemanticCleanupPending && !out.SemanticCleaned
	}
	return next, nil
}

// stopBounded runs one executor Stop under a context that ends at the
// persisted deadline, never later than one operation budget from now. The
// deadline is never renewed: once it has passed, Stop receives an
// already-expired context (no time is granted, so only an immediate
// withdrawal can be proven), and the caller treats any obligation it leaves
// as stalled. Stop must be idempotent for a correlation whose native close is
// already proven: it is repeated to retry semantic cleanup.
func (s *Service) stopBounded(d *driverState, corr LifecycleCorrelation, deadline time.Time) StopOutcome {
	limit := s.budget
	if !deadline.IsZero() {
		if remaining := deadline.Sub(s.now()); remaining < limit {
			limit = remaining // non-positive once expired: an already-expired context
		}
	}
	ctx, cancel := context.WithTimeout(s.ctx, limit)
	defer cancel()
	return d.exec.Stop(ctx, corr)
}

// semanticRetry bounds unfinished semantic cleanup: it retries only while
// the persisted attempt limit and the persisted deadline leave budget (and
// within the in-process series bound); otherwise the cleanup has stalled.
func (s *Service) semanticRetry(d *driverState, t Transition) (bool, time.Duration) {
	if s.cleanupSpent(t) {
		return s.stallCleanup(d, t)
	}
	d.mu.Lock()
	d.cleanupRetries++
	exhausted := d.cleanupRetries >= s.maxAttempts
	d.mu.Unlock()
	if exhausted {
		return s.stallCleanup(d, t)
	}
	return s.retryLater()
}

// stallCleanup ends a cleanup whose budget is spent: the driver is reported
// FAILED with a CLEANUP_STALLED diagnostic, admission of START and RESTART is
// blocked (recoveryBlocked) and the reconciler takes no further step in this
// process. The obligation stays durable; no budget or deadline is renewed.
func (s *Service) stallCleanup(d *driverState, t Transition) (bool, time.Duration) {
	d.mu.Lock()
	d.recoveryBlocked = true
	d.cleanupRetries = 0
	d.mu.Unlock()
	s.observe(d, observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonRetryExhausted})
	s.appendDiagnostic(d, t, "CLEANUP_STALLED")
	return false, 0
}

func (s *Service) releaseLive(d *driverState, t Transition) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.live != nil && d.live.corr.Epoch == t.Epoch && d.live.corr.Generation == t.Generation {
		d.live = nil
	}
}

// afterCleanup applies the result of a cleanup commit: an unconfirmed native
// close quarantines the driver (no automatic retry in this process), pending
// semantic cleanup retries later, and settled obligations continue.
func (s *Service) afterCleanup(d *driverState, next Transition) (bool, time.Duration) {
	if !next.NativeClosePending {
		s.releaseLive(d, next)
	}
	switch {
	case next.NativeClosePending:
		s.quarantine(d, next)
		return false, 0
	case next.SemanticCleanupPending:
		return s.semanticRetry(d, next)
	}
	d.mu.Lock()
	d.cleanupRetries = 0
	d.mu.Unlock()
	return true, 0
}

// stepCancel fences the slot's attempt and settles its cleanup obligations
// with the original correlation. The old attempt terminates as canceled; its
// cleanup is never discarded.
func (s *Service) stepCancel(d *driverState, t Transition) (bool, time.Duration) {
	d.mu.Lock()
	for _, a := range []*attempt{d.untyped, d.inflight} {
		if a != nil {
			a.fenced = true
			a.cancel()
		}
	}
	d.untyped, d.inflight = nil, nil
	d.mu.Unlock()
	if t.Epoch != "" {
		s.setCorr(d, s.corr(d, t))
	}
	s.observe(d, observation{state: drivermanager.ObservedStopping, reason: drivermanager.ReasonStopRequested})
	next, err := s.settleObligations(d, t)
	if err != nil {
		return s.retryLater()
	}
	if err := s.commitCurrent(d, next); err != nil {
		return s.retryLater()
	}
	if progressed, delay := s.afterCleanup(d, next); !progressed {
		return progressed, delay
	}
	s.appendDiagnostic(d, next, "CANCELED")
	return true, 0
}

// stepCleanup settles the remaining obligations of a slot that already
// reached CLEANUP (a failed or canceled attempt), without a new attempt.
func (s *Service) stepCleanup(d *driverState, t Transition) (bool, time.Duration) {
	next, err := s.settleObligations(d, t)
	if err != nil {
		return s.retryLater()
	}
	if err := s.commitCurrent(d, next); err != nil {
		return s.retryLater()
	}
	return s.afterCleanup(d, next)
}

// stepStop withdraws the admitted generation, if any, and publishes STOPPED
// only after both obligations are durably cleared.
func (s *Service) stepStop(d *driverState, t Transition) (bool, time.Duration) {
	d.mu.Lock()
	live := d.live
	d.mu.Unlock()
	switch {
	case t.Epoch != "":
		s.setCorr(d, s.corr(d, t))
	case t.Phase == PhasePending && live != nil:
		s.setCorr(d, live.corr)
	}
	s.observe(d, observation{state: drivermanager.ObservedStopping, reason: drivermanager.ReasonStopRequested})
	if t.Phase == PhasePending {
		next := t
		if live == nil {
			next.Phase = PhaseCleanup
		} else {
			next.Phase = PhaseActive
			next.Epoch, next.Generation = live.corr.Epoch, live.corr.Generation
			next.NativeClosePending, next.SemanticCleanupPending = true, true
			next.DeadlineUTC = s.now().Add(s.budget).UTC()
		}
		if err := s.commitCurrent(d, next); err != nil {
			return s.retryLater()
		}
		t = next
	}
	if t.NativeClosePending || t.SemanticCleanupPending {
		next, err := s.settleObligations(d, t)
		if err != nil {
			return s.retryLater()
		}
		if err := s.commitCurrent(d, next); err != nil {
			return s.retryLater()
		}
		if progressed, delay := s.afterCleanup(d, next); !progressed {
			return progressed, delay
		}
		t = next
	}
	d.mu.Lock()
	d.autoStartDone = true
	d.mu.Unlock()
	s.observe(d, observation{state: drivermanager.ObservedStopped, reason: drivermanager.ReasonOperatorStopped})
	s.appendDiagnostic(d, t, "STOPPED")
	return true, 0
}

// stepRestart withdraws the admitted generation, then performs one
// replacement under a new epoch. An unfinished RESTART recovered after
// process loss still performs its replacement.
func (s *Service) stepRestart(d *driverState, t Transition) (bool, time.Duration) {
	if t.Phase == PhasePending {
		d.mu.Lock()
		live := d.live
		d.mu.Unlock()
		if live != nil {
			// Persist the predecessor's correlation and both cleanup
			// obligations before any Stop, so process loss during the
			// withdrawal recovers them.
			s.setCorr(d, live.corr)
			s.observe(d, observation{state: drivermanager.ObservedStopping, reason: drivermanager.ReasonStopRequested})
			next := t
			next.Phase = PhaseActive
			next.Epoch, next.Generation = live.corr.Epoch, live.corr.Generation
			next.NativeClosePending, next.SemanticCleanupPending = true, true
			next.DeadlineUTC = s.now().Add(s.budget).UTC()
			if err := s.commitCurrent(d, next); err != nil {
				return s.retryLater()
			}
			t = next
		}
	}
	return s.stepStart(d, t)
}

// stepStart runs the slot's bounded attempts. A correlation left by an
// earlier incarnation is historical: its obligations are settled with the
// old correlation before a new epoch is assigned.
func (s *Service) stepStart(d *driverState, t Transition) (bool, time.Duration) {
	d.mu.Lock()
	a := d.inflight
	d.mu.Unlock()
	if a != nil && a.sequence == t.Sequence && a.admitted {
		// The generation is admitted but the terminal commit failed; retry it.
		return s.completeStart(d, t, a)
	}
	if a == nil || a.sequence != t.Sequence || t.Epoch == "" {
		next := t
		if t.Epoch != "" && (t.NativeClosePending || t.SemanticCleanupPending) {
			s.setCorr(d, s.corr(d, t))
			settled, err := s.settleObligations(d, t)
			if err != nil {
				return s.retryLater()
			}
			settled.Phase = t.Phase
			if err := s.commitCurrent(d, settled); err != nil {
				return s.retryLater()
			}
			if settled.NativeClosePending {
				s.quarantine(d, settled)
				return false, 0
			}
			s.releaseLive(d, settled)
			if settled.SemanticCleanupPending {
				return s.semanticRetry(d, settled)
			}
			d.mu.Lock()
			d.cleanupRetries = 0
			d.mu.Unlock()
			next = settled
		}
		if t.Epoch != "" && s.startSpent(next) {
			// The predecessor's cleanup used the last start attempt (or the
			// nonrenewable deadline passed): no replacement is created and
			// budgets are never renewed.
			return s.endSpent(d, next)
		}
		next.Phase = PhaseActive
		next.Epoch = s.newEpoch(d)
		next.Generation = 1
		next.NativeClosePending, next.SemanticCleanupPending = true, true
		if next.DeadlineUTC.IsZero() {
			next.DeadlineUTC = s.now().Add(s.budget).UTC()
		}
		if err := s.commitCurrent(d, next); err != nil {
			return s.retryLater()
		}
		t = next
		a = s.newAttempt(s.corr(d, t), t.DeadlineUTC, t.Sequence)
		d.mu.Lock()
		d.inflight = a
		d.mu.Unlock()
	}
	return s.runTypedAttempt(d, t, a)
}

// startSpent reports whether a slot has no start attempt or time left.
func (s *Service) startSpent(t Transition) bool {
	return t.AttemptsUsed >= s.startLimit() || (!t.DeadlineUTC.IsZero() && !s.now().Before(t.DeadlineUTC))
}

// endSpent terminates a slot whose obligations are cleared but whose start
// budget is spent: it ends FAILED without a replacement generation.
func (s *Service) endSpent(d *driverState, t Transition) (bool, time.Duration) {
	done := t
	done.Phase = PhaseCleanup
	done.NativeClosePending, done.SemanticCleanupPending = false, false
	if err := s.commitCurrent(d, done); err != nil {
		if errors.Is(err, ErrStaleSlot) {
			return true, 0
		}
		return s.retryLater()
	}
	s.releaseLive(d, done)
	d.mu.Lock()
	d.autoStartDone = true
	d.mu.Unlock()
	s.observe(d, observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonRetryExhausted})
	s.appendDiagnostic(d, done, "FAILED")
	return true, 0
}

// waitDecisions blocks while a decision on the driver is between its planner
// and its published acceptance, so a result is classified only after that
// decision committed or was rejected.
func (s *Service) waitDecisions(d *driverState) {
	d.mu.Lock()
	for d.deciding > 0 && s.ctx.Err() == nil {
		d.cond.Wait()
	}
	d.mu.Unlock()
}

func (s *Service) runTypedAttempt(d *driverState, t Transition, a *attempt) (bool, time.Duration) {
	for {
		if s.ctx.Err() != nil {
			return false, 0
		}
		if s.fencedByStore(d, t) || s.isFenced(d, a) {
			return true, 0
		}
		if a.pendingGen {
			// A retry's generation increment failed to commit: commit it
			// before any further Start so the executor never sees the
			// previous generation twice.
			next := t
			next.Generation++
			if err := s.commitCurrent(d, next); err != nil {
				if errors.Is(err, ErrStaleSlot) {
					return true, 0
				}
				return s.retryLater()
			}
			t = next
			a.pendingGen = false
		}
		adopted := a.chargedTo != 0 && t.AttemptsUsed == a.chargedTo
		if (!adopted && t.AttemptsUsed >= s.startLimit()) || !s.now().Before(t.DeadlineUTC) {
			return s.exhaust(d, t, drivermanager.ReasonRetryExhausted)
		}
		if !adopted {
			next := t
			next.AttemptsUsed++
			a.chargedTo = next.AttemptsUsed
			if err := s.commitCurrent(d, next); err != nil {
				if errors.Is(err, ErrStaleSlot) {
					return true, 0
				}
				return s.retryLater()
			}
			t = next
		}
		a.chargedTo = 0
		a.corr = s.corr(d, t)
		s.setCorr(d, a.corr)
		s.observe(d, observation{state: drivermanager.ObservedStarting, reason: drivermanager.ReasonStartRequested})
		err := d.exec.Start(a.ctx, a.corr)
		if s.ctx.Err() != nil {
			return false, 0
		}
		s.waitDecisions(d)
		if s.fencedByStore(d, t) {
			return true, 0
		}
		recovery, recovering := recoveringStart(err)
		if (err == nil || recovering) && (a.ctx.Err() != nil || !s.now().Before(t.DeadlineUTC)) {
			// A success after the attempt's budget is never admitted; its
			// generation is withdrawn through the slot's obligations.
			if s.isFenced(d, a) {
				return true, 0
			}
			return s.exhaust(d, t, drivermanager.ReasonRetryExhausted)
		}
		if err == nil || recovering {
			admittedObs := admissionObservation(a.corr, recovery, recovering)
			expired := false
			admitted := s.settle(d, a, func() {
				// The hold may outlast the budget: revalidate it here.
				if a.ctx.Err() != nil || !s.now().Before(t.DeadlineUTC) {
					expired = true
					return
				}
				a.admitted = true
				a.recovering = recovering
				d.live = a
				d.autoStartDone = true
				d.corr = a.corr
				s.stageObservation(d.id, admittedObs)
			})
			if !admitted {
				return true, 0
			}
			if expired {
				return s.exhaust(d, t, drivermanager.ReasonRetryExhausted)
			}
			s.commitObserved(d, admittedObs)
			return s.completeStart(d, t, a)
		}
		s.report(&LifecycleError{DriverID: d.id, Kind: t.Kind, Err: err})
		if s.isFenced(d, a) {
			return true, 0
		}
		if errors.Is(a.ctx.Err(), context.DeadlineExceeded) {
			return s.exhaust(d, t, drivermanager.ReasonRetryExhausted)
		}
		if retryable(err) && t.AttemptsUsed < s.startLimit() && d.driverRetryAllows(int(t.Generation)) && s.now().Before(t.DeadlineUTC) {
			s.observe(d, observation{state: drivermanager.ObservedBackoff, reason: drivermanager.ReasonRetryScheduled})
			if !sleepCtx(a.ctx, s.retryDelayFor(d, t.AttemptsUsed)) {
				if s.isFenced(d, a) || s.ctx.Err() != nil {
					return true, 0
				}
				continue // the budget expired during the backoff
			}
			next := t
			next.Generation++
			if err := s.commitCurrent(d, next); err != nil {
				a.pendingGen = true
				return s.retryLater()
			}
			t = next
			continue
		}
		return s.exhaust(d, t, failureReason(err))
	}
}

// malformedRecoveringStart reports a RecoveringStart whose observation the
// service refuses: the executor left a native recovery running that cannot
// be admitted, so the generation must be withdrawn like a failed start's.
func malformedRecoveringStart(err error) bool {
	var recovering *RecoveringStart
	if !errors.As(err, &recovering) {
		return false
	}
	_, ok := recoveringStart(err)
	return !ok
}

// admissionObservation is the observation published when a start admits its
// generation: RUNNING, or the native owner's recovery BACKOFF with its
// schedule for a RecoveringStart.
func admissionObservation(corr LifecycleCorrelation, recovery NativeObservation, recovering bool) observation {
	if !recovering {
		return observation{state: drivermanager.ObservedRunning, reason: drivermanager.ReasonNone, epoch: corr.Epoch, generation: corr.Generation}
	}
	return observation{state: recovery.State, reason: recovery.Reason, epoch: corr.Epoch, generation: corr.Generation,
		nextAttempt: recovery.NextAttempt, nextAttemptUTC: recovery.NextAttemptUTC.UTC()}
}

// completeStart marks the admitted slot terminal: its obligations now belong
// to the admitted generation, which a later STOP or RESTART withdraws.
func (s *Service) completeStart(d *driverState, t Transition, a *attempt) (bool, time.Duration) {
	done := t
	done.Phase = PhaseCleanup
	done.NativeClosePending, done.SemanticCleanupPending = false, false
	if err := s.commitCurrent(d, done); err != nil {
		if errors.Is(err, ErrStaleSlot) {
			return true, 0
		}
		return s.retryLater()
	}
	d.mu.Lock()
	if d.inflight == a {
		d.inflight = nil
	}
	d.mu.Unlock()
	a.cancel()
	outcome := "RUNNING"
	if a.recovering {
		outcome = "BACKOFF"
	}
	s.appendDiagnostic(d, done, outcome)
	return true, 0
}

// exhaust ends the slot's attempts as FAILED and settles its obligations.
// Desired RUNNING stays; recovery of a FAILED driver is a RESTART, never a
// START no-op.
func (s *Service) exhaust(d *driverState, t Transition, reason drivermanager.ReasonCode) (bool, time.Duration) {
	if s.ctx.Err() != nil {
		return false, 0
	}
	s.observe(d, observation{state: drivermanager.ObservedFailed, reason: reason})
	next, err := s.settleObligations(d, t)
	if err != nil {
		return s.retryLater()
	}
	if err := s.commitCurrent(d, next); err != nil {
		return s.retryLater()
	}
	d.mu.Lock()
	if d.inflight != nil && d.inflight.sequence == t.Sequence {
		d.inflight.cancel()
		d.inflight = nil
	}
	d.autoStartDone = true
	d.mu.Unlock()
	if progressed, delay := s.afterCleanup(d, next); !progressed {
		return progressed, delay
	}
	s.appendDiagnostic(d, next, "FAILED")
	return true, 0
}

// fencedByStore reports whether a STOP or RESTART took the reserved slot
// behind this START, which cancels it.
func (s *Service) fencedByStore(d *driverState, t Transition) bool {
	if t.Kind != KindStart {
		return false
	}
	rec, err := s.store.Driver(d.id)
	return err == nil && rec.Reserved != nil && sameIdentity(rec.Current, &t)
}

func (s *Service) isFenced(d *driverState, a *attempt) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return a.fenced
}

// runUntyped is the in-memory process-start attempt for a driver with
// durable desired RUNNING and no typed work. It persists nothing; a STOP or
// RESTART accepted meanwhile synthesizes its cancellation as typed work.
func (s *Service) runUntyped(d *driverState) {
	deadline := s.now().Add(s.budget)
	corr := LifecycleCorrelation{DriverID: d.id, Epoch: s.newEpoch(d), Generation: 1}
	a := s.newAttempt(corr, deadline, 0)
	defer a.cancel()
	// Install the attempt only when no decision is between its planner and
	// its commit, then re-read the slots: a STOP or RESTART accepted before
	// the installation owns the driver and no attempt may begin.
	d.mu.Lock()
	for d.deciding > 0 && s.ctx.Err() == nil {
		d.cond.Wait()
	}
	d.untyped = a
	d.mu.Unlock()
	if rec, err := s.store.Driver(d.id); err != nil || rec.Current != nil {
		d.mu.Lock()
		if d.untyped == a {
			d.untyped = nil
		}
		d.mu.Unlock()
		if err != nil {
			s.report(err)
		}
		return
	}
	attempts := 0
	var withdrawLate func(drivermanager.ReasonCode)
	// finishUntyped publishes the attempt's terminal observation; admit
	// installs the generation as the admitted one (RUNNING, or the native
	// owner's recovery). An admission is revalidated under the hold: the
	// wait behind a decision may outlast the budget, and an expired success
	// is never admitted. It reports false in that case, leaving the late
	// generation to withdrawLate.
	finishUntyped := func(obs observation, admit bool) bool {
		expired := false
		admitted := s.settle(d, a, func() {
			if admit && (a.ctx.Err() != nil || !s.now().Before(deadline)) {
				expired = true
				return
			}
			obs.epoch, obs.generation = a.corr.Epoch, a.corr.Generation
			d.corr = a.corr
			d.untyped = nil
			d.autoStartDone = true
			if admit {
				a.admitted = true
				a.recovering = obs.state != drivermanager.ObservedRunning
				d.live = a
			}
			s.stageObservation(d.id, obs)
		})
		if expired {
			return false
		}
		if admitted {
			s.commitObserved(d, obs)
		}
		return true
	}
	withdrawLate = func(reason drivermanager.ReasonCode) {
		// A success after the attempt's budget is never admitted, and a
		// malformed RecoveringStart cannot be: the started generation is
		// withdrawn, and an unconfirmed close quarantines the driver. The
		// withdrawal stays bounded by the attempt's original deadline: no
		// new deadline is computed, so a withdrawal after that deadline gets
		// one Stop with an already-expired context and no retry. A proven
		// withdrawal ends FAILED with reason.
		var out StopOutcome
		for i := 0; i < s.maxAttempts; i++ {
			out = s.stopBounded(d, a.corr, deadline)
			if !out.NativeClosed || out.SemanticCleaned || !s.now().Before(deadline) {
				break
			}
		}
		if !out.NativeClosed {
			s.settle(d, a, func() {
				d.corr, d.untyped, d.autoStartDone = a.corr, nil, true
			})
			s.quarantine(d, Transition{Kind: KindStart})
			return
		}
		if !out.SemanticCleaned {
			// No slot retains this cleanup: block admission so a later
			// START or RESTART cannot proceed over unfinished withdrawal.
			s.settle(d, a, func() {
				d.corr, d.untyped, d.autoStartDone = a.corr, nil, true
				d.recoveryBlocked = true
			})
			s.appendDiagnostic(d, Transition{Kind: KindStart}, "CLEANUP_STALLED")
			s.observe(d, observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonRetryExhausted})
			return
		}
		finishUntyped(observation{state: drivermanager.ObservedFailed, reason: reason}, false)
	}
	for {
		if s.ctx.Err() != nil || s.isFenced(d, a) {
			return
		}
		if attempts >= s.maxAttempts || !s.now().Before(deadline) {
			finishUntyped(observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonRetryExhausted}, false)
			return
		}
		attempts++
		d.mu.Lock()
		a.attemptsUsed = attempts
		current := a.corr
		d.corr = current
		d.mu.Unlock()
		s.observe(d, observation{state: drivermanager.ObservedStarting, reason: drivermanager.ReasonStartRequested})
		err := d.exec.Start(a.ctx, current)
		if s.ctx.Err() != nil {
			return
		}
		s.waitDecisions(d)
		if rec, rerr := s.store.Driver(d.id); rerr == nil && rec.Current != nil {
			return // a cancellation slot owns this correlation now
		}
		recovery, recovering := recoveringStart(err)
		if (err == nil || recovering) && (a.ctx.Err() != nil || !s.now().Before(deadline)) {
			withdrawLate(drivermanager.ReasonRetryExhausted)
			return
		}
		if err == nil || recovering {
			if !finishUntyped(admissionObservation(current, recovery, recovering), true) {
				withdrawLate(drivermanager.ReasonRetryExhausted)
			}
			return
		}
		s.report(&LifecycleError{DriverID: d.id, Kind: KindStart, Err: err})
		if s.isFenced(d, a) {
			return
		}
		if malformedRecoveringStart(err) {
			// The executor reported a native recovery it left running,
			// with an observation the service cannot admit: withdraw it.
			withdrawLate(drivermanager.ReasonInternalError)
			return
		}
		if errors.Is(a.ctx.Err(), context.DeadlineExceeded) {
			finishUntyped(observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonRetryExhausted}, false)
			return
		}
		if retryable(err) && attempts < s.maxAttempts && d.driverRetryAllows(attempts) && s.now().Before(deadline) {
			s.observe(d, observation{state: drivermanager.ObservedBackoff, reason: drivermanager.ReasonRetryScheduled})
			sleepCtx(a.ctx, s.retryDelayFor(d, attempts))
			proceed := s.settle(d, a, func() { a.corr.Generation++ })
			if !proceed {
				return
			}
			continue
		}
		finishUntyped(observation{state: drivermanager.ObservedFailed, reason: failureReason(err)}, false)
		return
	}
}

// finish secures the terminal slot's D5 material, then prunes the slot,
// promoting the reserved transition. Pruning with no reserved slot excludes
// decisions on this driver for the duration of the commit so a newly
// reserved transition can never be dropped.
func (s *Service) finish(d *driverState, rec ControlRecord, t Transition) (bool, time.Duration) {
	material, err := s.recovery.Secure(s.ctx, d.id, t)
	if err != nil {
		s.report(&SemanticError{DriverID: d.id, Op: "secure", Err: err})
		return s.retryLater()
	}
	if material != nil {
		if verr := validateMaterial(material); verr != nil {
			s.report(&SemanticError{DriverID: d.id, Op: "secure", Err: verr})
			return s.retryLater()
		}
		if s.ctx.Err() != nil {
			return false, 0
		}
		if err := s.store.CommitRecovery(s.ctx, cloneMaterial(material)); err != nil {
			s.report(err)
			s.armReadback()
			return s.retryLater()
		}
	}
	if rec.Reserved == nil {
		d.mu.Lock()
		for d.deciding > 0 && s.ctx.Err() == nil {
			d.cond.Wait()
		}
		d.pruning = true
		d.mu.Unlock()
		defer func() {
			d.mu.Lock()
			d.pruning = false
			d.mu.Unlock()
		}()
		fresh, err := s.store.Driver(d.id)
		if err != nil || !sameIdentity(fresh.Current, &t) {
			return s.retryLater()
		}
		rec = fresh
	}
	if s.ctx.Err() != nil {
		return false, 0
	}
	if err := s.store.CommitWork(s.ctx, d.id, cloneTransition(rec.Reserved), nil); err != nil {
		s.report(err)
		s.armReadback()
		return s.retryLater()
	}
	if t.Kind == KindStop && rec.Reserved == nil {
		// A terminal STOP recovered from an earlier incarnation is pruned
		// without a new stop; its settled outcome is STOPPED.
		if t.Epoch != "" {
			s.setCorr(d, s.corr(d, t))
		}
		s.observe(d, observation{state: drivermanager.ObservedStopped, reason: drivermanager.ReasonOperatorStopped})
	}
	return true, 0
}

func validateMaterial(m *RecoveryMaterial) error {
	if len(m.DriverIDs) == 0 {
		return fmt.Errorf("%w: no driver ids", ErrInvalidRecovery)
	}
	for i, id := range m.DriverIDs {
		if !canonicalDriverID(id) || (i > 0 && m.DriverIDs[i-1] >= id) {
			return fmt.Errorf("%w: driver ids must be canonical, unique and sorted", ErrInvalidRecovery)
		}
	}
	return nil
}

func retryable(err error) bool {
	var r interface{ Retryable() bool }
	return errors.As(err, &r) && r.Retryable()
}

// failureReason maps an executor failure to a closed categorical reason. Raw
// error text never reaches a view, event or diagnostic.
func failureReason(err error) drivermanager.ReasonCode {
	var r interface {
		ReasonCode() drivermanager.ReasonCode
	}
	if errors.As(err, &r) {
		switch code := r.ReasonCode(); code {
		case drivermanager.ReasonProviderUnavailable, drivermanager.ReasonDependencyUnavailable,
			drivermanager.ReasonConfigInvalid, drivermanager.ReasonRuntimeNotReady:
			return code
		}
	}
	if retryable(err) {
		return drivermanager.ReasonRetryExhausted
	}
	return drivermanager.ReasonInternalError
}

func sleepCtx(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
