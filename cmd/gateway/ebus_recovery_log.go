package main

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Closed trigger categories of an ebus.primary connection loss. They name the
// signal that reached the lifecycle owner, never an endpoint.
const (
	// ebusLossAdapterConnection is the adapter-direct mux's terminal loss
	// boundary: a terminal upstream read error or the AM27 blackhole.
	ebusLossAdapterConnection = "adapter_connection_lost"
	// ebusLossProxyListener is a fatal exit of the adapter-direct proxy
	// listener, which retires the generation like an upstream loss.
	ebusLossProxyListener = "proxy_listener_failed"
	// ebusLossTransportClosed is a closed provider transport observed by a
	// consumer call (the proxy-class ENH/ENS endpoint, for example).
	ebusLossTransportClosed = "transport_closed"
	ebusLossProviderFailed  = "provider_failed"
)

// ebusConnectionLoss is the error a generation's health reports to the
// lifecycle owner. It unwraps to the adapter reset, so the loss keeps its
// retryable dependency classification, and carries the trigger category.
type ebusConnectionLoss struct{ reason string }

func (loss *ebusConnectionLoss) Error() string {
	return "ebus: connection lost (" + loss.reason + ")"
}

func (*ebusConnectionLoss) Unwrap() error { return ebuserrors.ErrAdapterReset }

// ebusConnectionLossReason maps a reported failure to its closed trigger
// category.
func ebusConnectionLossReason(err error) string {
	var loss *ebusConnectionLoss
	switch {
	case errors.As(err, &loss) && loss.reason != "":
		return loss.reason
	case errors.Is(err, ebuserrors.ErrAdapterReset):
		return ebusLossAdapterConnection
	case errors.Is(err, ebuserrors.ErrTransportClosed):
		return ebusLossTransportClosed
	}
	return ebusLossProviderFailed
}

// ebusRecoveryFailureCategory maps one failed replacement attempt to a closed
// category. Native error text is never logged: it can carry the endpoint.
func ebusRecoveryFailureCategory(err error) string {
	var opErr *net.OpError
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	case errors.Is(err, transport.ErrDriverSafetyQuarantined):
		return "close_unconfirmed"
	case errors.Is(err, transport.ErrDriverStopTimeout):
		return "drain_timeout"
	case errors.As(err, &opErr) && opErr.Op == "listen":
		return "proxy_listen_failed"
	case errors.As(err, &opErr) && opErr.Op == "dial":
		switch {
		case errors.Is(err, syscall.ECONNREFUSED):
			return "dial_refused"
		case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
			return "dial_unreachable"
		case opErr.Timeout():
			return "dial_timeout"
		}
		return "dial_failed"
	case errors.Is(err, ebuserrors.ErrTimeout):
		return "handshake_timeout"
	case errors.Is(err, ebuserrors.ErrInvalidPayload):
		return "invalid_payload"
	case errors.Is(err, transport.ErrDriverUnavailable):
		return "runtime_unavailable"
	}
	return "start_failed"
}

// ebusRecoveryLog records the bounded recovery DriverManager runs after a
// reported ebus.primary connection loss: the loss with its trigger, every
// replacement attempt with its reason (the trigger, then the previous
// attempt's failure category) and outcome, and the admitted recovered
// generation. Lines carry closed categories and counters only.
type ebusRecoveryLog struct {
	mu     sync.Mutex
	budget int
	logf   func(string, ...any)

	active  bool
	attempt int
	reason  string
	// pending is the generation the latest attempt constructed and that is
	// not yet admitted; a following attempt means its admission was refused.
	pending uint64
	// bound is the latest generation whose health binding completed.
	bound uint64
	// deferred is the RUNNING watch line held back until the pending
	// generation is bound, so the recovered line stays ordered before it.
	deferred func()
	// deferredPublish is the service publication that belongs to deferred: it
	// is released only by the health binding, never by a loss or a reset.
	deferredPublish func()
}

func (l *ebusRecoveryLog) printf(format string, args ...any) {
	if l.logf != nil {
		l.logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// reportLoss runs report (the manager's failure report) under the log lock
// and opens a recovery series only when the report was accepted, so a stale
// or repeated report logs nothing and the first attempt always names the
// loss as its reason.
func (l *ebusRecoveryLog) reportLoss(generation uint64, reason string, report func() bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !report() {
		return
	}
	l.active, l.attempt, l.reason, l.pending = true, 0, reason, 0
	l.flushDeferred(false)
	l.printf("ebus.primary: connection lost (reason=%s, generation=%d); bounded recovery scheduled (at most %d attempts)", reason, generation, l.budget)
}

// continuing reports an open series that still has attempts left.
func (l *ebusRecoveryLog) continuing() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active && l.attempt < l.budget
}

// attemptStarted logs one replacement attempt before its construction.
func (l *ebusRecoveryLog) attemptStarted() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active {
		l.active, l.attempt, l.reason = true, 0, "replacement_requested"
	}
	if l.pending != 0 {
		l.reason, l.pending = "retry_after_admission_refused", 0
	}
	l.attempt++
	l.printf("ebus.primary: recovery attempt %d/%d (reason=%s)", l.attempt, l.budget, l.reason)
	return l.attempt
}

// attemptFinished logs the construction outcome of one attempt. A built
// generation is recovered only once DriverManager admits it (admitted).
func (l *ebusRecoveryLog) attemptFinished(attempt int, generation uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active || attempt != l.attempt {
		return
	}
	if err != nil {
		category := ebusRecoveryFailureCategory(err)
		l.reason = "retry_after_" + category
		l.printf("ebus.primary: recovery attempt %d/%d failed (category=%s)", attempt, l.budget, category)
		return
	}
	l.pending = generation
	l.printf("ebus.primary: recovery attempt %d/%d reconnected (generation=%d); awaiting admission", attempt, l.budget, generation)
}

// flushDeferred emits a held RUNNING line; the caller holds the lock.
func (l *ebusRecoveryLog) flushDeferred(publish bool) {
	line, pub := l.deferred, l.deferredPublish
	l.deferred, l.deferredPublish = nil, nil
	if line != nil {
		line()
	}
	if publish && pub != nil {
		pub()
	}
}

// complete closes the series with the recovered line; the caller holds the lock.
func (l *ebusRecoveryLog) complete(generation uint64) {
	l.printf("ebus.primary: recovered on attempt %d/%d (generation=%d)", l.attempt, l.budget, generation)
	l.active, l.attempt, l.reason, l.pending = false, 0, "", 0
}

// admitted records that the health binding of generation completed, the
// point at which a loss that raced construction has been replayed. It closes
// the series when that generation is the one the latest attempt built. A
// loss replayed at binding reopened the series and cleared pending, so it is
// not reported as a recovery.
func (l *ebusRecoveryLog) admitted(generation uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.bound = generation
	if l.active && l.pending != 0 && l.pending == generation {
		l.complete(generation)
	}
	l.flushDeferred(true)
}

// running handles the RUNNING watch notification of generation. The recovered
// line is gated on the health binding: when the generation is still unbound,
// line is held until admitted or a replayed loss releases it. Otherwise line
// follows any recovered line immediately. publish, the service publication of
// the RUNNING observation, is held and released with the line, and dropped
// with it when a replayed loss or a newer notification supersedes it.
func (l *ebusRecoveryLog) running(generation uint64, line, publish func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active && l.pending != 0 && l.pending == generation {
		if l.bound != generation {
			l.flushDeferred(false)
			l.deferred, l.deferredPublish = line, publish
			return
		}
		l.complete(generation)
	}
	l.flushDeferred(false)
	line()
	publish()
}

// reset ends any series without a line: an explicit start or stop is not a
// recovery outcome.
func (l *ebusRecoveryLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.active, l.attempt, l.reason, l.pending = false, 0, "", 0
	l.flushDeferred(false)
}

// ebusNativeForwarder projects DriverManager's own ebus.primary transitions
// after RUNNING into the runtime control service, the owner of
// helianthus.runtime.list and of the Portal driver readback, and logs the
// visible state with its bound. It runs on the manager watch only.
type ebusNativeForwarder struct {
	bridge  *ebusControlRuntime
	service *runtimecontrol.Service
	logf    func(string, ...any)
	now     func() time.Time
	// recovery, when set, closes its series before the RUNNING line of a
	// recovered generation, so the recovered line is never logged after it.
	recovery *ebusRecoveryLog

	epoch runtimecontrol.SourceEpoch
	last  runtimecontrol.NativeObservation
}

func (f *ebusNativeForwarder) printf(format string, args ...any) {
	if f.logf != nil {
		f.logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// forward hands one manager snapshot to the service when it reports the
// current epoch's manager-owned lifecycle.
func (f *ebusNativeForwarder) forward(snapshot drivermanager.Snapshot) {
	if f == nil || f.bridge == nil || f.service == nil {
		return
	}
	corr, obs, ok := f.bridge.NativeObservation(snapshot)
	if !ok {
		return
	}
	publish := func() { f.service.ObserveNative(corr, obs) }
	if !f.logTransition(corr, obs, snapshot, publish) {
		publish()
	}
}

// logTransition logs the transition and reports whether it took over the
// service publication: a recovered RUNNING is published by the recovery log
// at the health-binding boundary, together with its line.
func (f *ebusNativeForwarder) logTransition(corr runtimecontrol.LifecycleCorrelation, obs runtimecontrol.NativeObservation, snapshot drivermanager.Snapshot, publish func()) bool {
	if corr.Epoch != f.epoch {
		// A new epoch opens with the service's own RUNNING.
		f.epoch, f.last = corr.Epoch, runtimecontrol.NativeObservation{State: drivermanager.ObservedRunning, Reason: drivermanager.ReasonNone}
	}
	previous := f.last
	if previous == obs {
		return false
	}
	f.last = obs
	switch obs.State {
	case drivermanager.ObservedBackoff:
		remaining, wait := 0, time.Duration(0)
		if snapshot.Retry != nil {
			remaining = snapshot.Retry.BudgetRemaining
			now := time.Now
			if f.now != nil {
				now = f.now
			}
			if wait = snapshot.Retry.NotBeforeUTC.Sub(now()); wait < 0 {
				wait = 0
			}
		}
		f.printf("ebus.primary: lifecycle BACKOFF (reason=%s, attempts_remaining=%d, next_attempt_in=%s)", obs.Reason, remaining, wait.Round(time.Millisecond))
	case drivermanager.ObservedRunning:
		line := func() {
			f.printf("ebus.primary: lifecycle RUNNING (generation=%d, recovered)", snapshot.Generation)
		}
		if f.recovery != nil {
			f.recovery.running(snapshot.Generation, line, publish)
			return true
		}
		line()
	case drivermanager.ObservedDegraded:
		f.printf("ebus.primary: lifecycle DEGRADED (reason=%s)", obs.Reason)
	case drivermanager.ObservedFailed:
		f.printf("ebus.primary: lifecycle FAILED (reason=%s, safety_quarantined=%t); automatic recovery ended, request helianthus.runtime.restart", obs.Reason, obs.Quarantined)
	}
	return false
}
