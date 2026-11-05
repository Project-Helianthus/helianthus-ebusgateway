package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

// modbusStartRetryPolicy is the bounded backoff between the re-issued starts
// of a managed Modbus driver whose START failed: Initial before the first
// retry, doubling per retry up to Max. Retries continue while the driver is
// desired RUNNING, so an endpoint that comes up late is still reached.
type modbusStartRetryPolicy struct {
	Initial time.Duration
	Max     time.Duration
}

// gatewayModbusStartRetryPolicy is the production policy; tests shorten it.
var gatewayModbusStartRetryPolicy = modbusStartRetryPolicy{Initial: 5 * time.Second, Max: 5 * time.Minute}

// modbusStartRetryDecideBudget bounds one re-issued RESTART decision.
const modbusStartRetryDecideBudget = 10 * time.Second

// delay is the backoff before retry n, counted from one.
func (p modbusStartRetryPolicy) delay(n int) time.Duration {
	d := p.Initial
	for i := 1; i < n && d < p.Max; i++ {
		d *= 2
	}
	if d > p.Max {
		d = p.Max
	}
	return d
}

// modbusStartRetryable reports a failed START that a later attempt may cure:
// the endpoint or a dependency was unavailable, or the service's own retry
// budget ran out. Configuration, internal and close failures, quarantine and
// withheld drivers are never retried.
func modbusStartRetryable(status runtimecontrol.DriverStatus) bool {
	if !status.Enabled || status.DesiredState != runtimecontrol.DesiredRunning || status.ObservedState != drivermanager.ObservedFailed || status.SafetyQuarantined {
		return false
	}
	switch status.Reason {
	case drivermanager.ReasonProviderUnavailable, drivermanager.ReasonDependencyUnavailable,
		drivermanager.ReasonRuntimeNotReady, drivermanager.ReasonRetryExhausted:
		return true
	}
	return false
}

// retryable excludes exhausted semantic cleanup from start exhaustion. The
// diagnostic is advisory for scheduling only; Decide remains the authority
// for admission, including a cleanup stall racing this check.
func (retry *modbusStartRetry) retryable(status runtimecontrol.DriverStatus) bool {
	if !modbusStartRetryable(status) {
		return false
	}
	if status.Reason == drivermanager.ReasonRetryExhausted {
		for _, diagnostic := range retry.service.Diagnostics(status.DriverID) {
			if diagnostic.Outcome == "CLEANUP_STALLED" {
				return false
			}
		}
	}
	return true
}

// modbusStartRetryDriver is one supervised driver's failure series. due is
// zero while no retry is scheduled. revision is the desired revision of the
// last counted failure and the one its RESTART expects: every retry opens a
// new desired revision, so one failure is counted once.
type modbusStartRetryDriver struct {
	attempts int
	revision runtimecontrol.DesiredRevision
	due      time.Time
}

// modbusStartRetry re-issues the START of managed Modbus drivers whose start
// failed. It follows the control service's committed events: each failed
// attempt observed while the driver is desired RUNNING schedules one retry,
// an ordinary RESTART decision against the failed desired revision, exactly
// as an operator restart. The retry is therefore durable, visible in the
// runtime-control view as a new desired revision and its STARTING
// observation, and refused once anything else changed the driver. A STOP,
// disable, quarantine or non-transient failure cancels the scheduled retry;
// RUNNING ends the series.
type modbusStartRetry struct {
	service *runtimecontrol.Service
	policy  modbusStartRetryPolicy
	drivers map[runtimecontrol.DriverID]*modbusStartRetryDriver
	now     func() time.Time
	cancel  context.CancelFunc
	done    chan struct{}
}

func startModbusStartRetry(service *runtimecontrol.Service, ids []runtimecontrol.DriverID, policy modbusStartRetryPolicy) *modbusStartRetry {
	if service == nil || len(ids) == 0 || policy.Initial <= 0 || policy.Max < policy.Initial {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	retry := &modbusStartRetry{service: service, policy: policy, drivers: make(map[runtimecontrol.DriverID]*modbusStartRetryDriver, len(ids)), now: time.Now, cancel: cancel, done: make(chan struct{})}
	for _, id := range ids {
		retry.drivers[id] = &modbusStartRetryDriver{}
	}
	go retry.run(ctx)
	return retry
}

// Close stops scheduling and joins an in-flight decision.
func (retry *modbusStartRetry) Close() {
	if retry == nil {
		return
	}
	retry.cancel()
	<-retry.done
}

func (retry *modbusStartRetry) run(ctx context.Context) {
	defer close(retry.done)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	cursor := retry.resync()
	for {
		changed := retry.service.EventNotify()
		page := retry.service.EventsSince(cursor)
		cursor = page.Next
		if page.Resync {
			cursor = retry.resync()
		}
		for _, event := range page.Events {
			retry.observe(event)
		}
		next := retry.fire(ctx)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		var wake <-chan time.Time
		if !next.IsZero() {
			timer.Reset(max(next.Sub(retry.now()), 0))
			wake = timer.C
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-wake:
		}
	}
}

// resync reconciles every supervised driver with the catalog and returns the
// catalog's event cursor. A failed driver with no retry scheduled, or whose
// scheduled retry belongs to an older desired revision, is scheduled afresh
// for the revision the catalog shows; any other driver loses a scheduled
// retry. It covers a start that failed before the supervisor started and
// events dropped by the bounded log.
func (retry *modbusStartRetry) resync() uint64 {
	catalog := retry.service.List()
	for _, status := range catalog.Drivers {
		state, ok := retry.drivers[status.DriverID]
		if !ok {
			continue
		}
		switch {
		case retry.retryable(status):
			if state.due.IsZero() || state.revision != status.DesiredRevision {
				retry.schedule(status.DriverID, state, status.DesiredRevision, status.Reason)
			}
		case status.DesiredState != runtimecontrol.DesiredRunning || status.ObservedState == drivermanager.ObservedRunning || status.ObservedState == drivermanager.ObservedDegraded:
			*state = modbusStartRetryDriver{}
		default:
			state.due = time.Time{}
		}
	}
	return catalog.EventCursor
}

// observe applies one committed event. An accepted decision carries the
// previous observation, so only its desired state is read.
func (retry *modbusStartRetry) observe(event runtimecontrol.Event) {
	state, ok := retry.drivers[event.DriverID]
	if !ok {
		return
	}
	if event.DesiredState != runtimecontrol.DesiredRunning {
		*state = modbusStartRetryDriver{}
		return
	}
	if event.Kind != runtimecontrol.EventObservedChanged {
		return
	}
	status := runtimecontrol.DriverStatus{DriverID: event.DriverID, Enabled: true, DesiredState: event.DesiredState, ObservedState: event.ObservedState, Reason: event.Reason, SafetyQuarantined: event.SafetyQuarantined}
	switch {
	case event.ObservedState == drivermanager.ObservedRunning || event.ObservedState == drivermanager.ObservedDegraded:
		*state = modbusStartRetryDriver{}
	case event.ObservedState != drivermanager.ObservedFailed:
	case !retry.retryable(status):
		state.due = time.Time{}
	case state.revision != event.DesiredRevision:
		retry.schedule(event.DriverID, state, event.DesiredRevision, event.Reason)
	}
}

func (retry *modbusStartRetry) schedule(id runtimecontrol.DriverID, state *modbusStartRetryDriver, revision runtimecontrol.DesiredRevision, reason drivermanager.ReasonCode) {
	// Admission can be transiently busy while the failed slot is pruned.
	// Rearming that same revision is not another failed native START.
	if state.attempts == 0 || state.revision != revision {
		state.attempts++
	}
	delay := retry.policy.delay(state.attempts)
	state.revision, state.due = revision, retry.now().Add(delay)
	log.Printf("Modbus driver start failed; retrying driver=%s reason=%s attempt=%d delay=%s", id, reason, state.attempts, delay)
}

// fire decides every retry that is due and returns the earliest one still
// scheduled, zero when none is. A driver that changed since its failure is
// left to its next event.
func (retry *modbusStartRetry) fire(ctx context.Context) time.Time {
	var next time.Time
	for id, state := range retry.drivers {
		if state.due.IsZero() {
			continue
		}
		if retry.now().Before(state.due) {
			if next.IsZero() || state.due.Before(next) {
				next = state.due
			}
			continue
		}
		state.due = time.Time{}
		status, err := retry.service.Get(id)
		if err != nil || !retry.retryable(status) || status.DesiredRevision != state.revision {
			continue
		}
		decideCtx, cancel := context.WithTimeout(ctx, modbusStartRetryDecideBudget)
		_, err = retry.service.Decide(decideCtx, runtimecontrol.MethodRestart, runtimecontrol.Request{DriverID: id, ExpectedRevision: state.revision})
		cancel()
		if err != nil && ctx.Err() == nil {
			var refusal *runtimecontrol.Refusal
			if errors.As(err, &refusal) && !refusal.Retryable() {
				continue
			}
			// A decision that was not accepted leaves the failure standing:
			// schedule the next retry of the same failure.
			log.Printf("Modbus driver start retry not accepted; driver=%s attempt=%d", id, state.attempts)
			retry.schedule(id, state, status.DesiredRevision, status.Reason)
			if next.IsZero() || state.due.Before(next) {
				next = state.due
			}
		}
	}
	return next
}

// modbusStartRetryIDs names the composed managed Modbus drivers: every Modbus
// TCP instance and the Growatt RS-485 driver. Withheld or uncomposed drivers
// fail deterministically and are not supervised.
func (sidecars *managedSidecars) modbusStartRetryIDs() []runtimecontrol.DriverID {
	if sidecars == nil {
		return nil
	}
	var ids []runtimecontrol.DriverID
	for _, s := range sidecars.entries {
		if s.runtime == nil || s.withheld != nil || (!s.isModbusTCP() && s.descriptor.DriverID != managedGrowattID) {
			continue
		}
		ids = append(ids, s.descriptor.DriverID)
	}
	return ids
}
