package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusadmin"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const (
	eebusStartupMaxAttempts = 3
	eebusStartupBackoff     = 5 * time.Second
)

type eebusLifecycleState string

const (
	eebusLifecycleDisabled eebusLifecycleState = "disabled"
	eebusLifecycleStarting eebusLifecycleState = "starting"
	eebusLifecycleRunning  eebusLifecycleState = "running"
	eebusLifecycleBackoff  eebusLifecycleState = "backoff"
	eebusLifecycleDegraded eebusLifecycleState = "degraded"
	eebusLifecycleStopped  eebusLifecycleState = "stopped"
)

type eebusRestartPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

type eebusRuntimeStarter func(context.Context) (*eebusRuntimeAdapter, eebusruntime.AdminV1, bool, error)
type eebusRestartWait func(context.Context, time.Duration) bool

type eebusRuntimeLifecycleOptions struct {
	policy eebusRestartPolicy
	start  eebusRuntimeStarter
	wait   eebusRestartWait
}

type eebusRuntimeLifecycleSnapshot struct {
	State            eebusLifecycleState
	Attempts         int
	Revision         uint64
	AdminAvailable   bool
	TimerOutstanding bool
	DegradedReason   eebusadmin.EEBusDegradedReason
}

type eebusStartupFailure struct {
	reason eebusadmin.EEBusDegradedReason
	err    error
}

func (failure *eebusStartupFailure) Error() string { return failure.err.Error() }

// Unwrap replaces, rather than extends, the gateway's existing public error
// layer. That keeps previously sanitized cause-chain depth stable.
func (failure *eebusStartupFailure) Unwrap() error { return errors.Unwrap(failure.err) }
func (failure *eebusStartupFailure) Is(target error) bool {
	return errors.Is(failure.err, target)
}

func markEEBusStartupFailure(reason eebusadmin.EEBusDegradedReason, err error) error {
	if err == nil {
		return nil
	}
	return &eebusStartupFailure{reason: reason, err: err}
}

func eebusStartupFailureReason(err error) eebusadmin.EEBusDegradedReason {
	var failure *eebusStartupFailure
	if errors.As(err, &failure) && failure.reason != "" {
		return failure.reason
	}
	return eebusadmin.EEBusDegradedReasonUnknownStartupFailure
}

func classifyEEBusConfigFailure(err error) eebusadmin.EEBusDegradedReason {
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "resolve eebus interface") ||
		strings.Contains(message, "interface resolved") ||
		strings.Contains(message, "interface address selection") {
		return eebusadmin.EEBusDegradedReasonListenerUnavailable
	}
	return eebusadmin.EEBusDegradedReasonConfigurationInvalid
}

func classifyEEBusFactoryFailure(err error) eebusadmin.EEBusDegradedReason {
	message := strings.ToLower(err.Error())
	for _, marker := range []string{"local identity", "certificate", "private key", " ski", "ski "} {
		if strings.Contains(message, marker) {
			return eebusadmin.EEBusDegradedReasonLocalIdentityUnavailable
		}
	}
	for _, marker := range []string{"listener", "listen ", "bind ", "address already in use"} {
		if strings.Contains(message, marker) {
			return eebusadmin.EEBusDegradedReasonListenerUnavailable
		}
	}
	return eebusadmin.EEBusDegradedReasonRuntimeFactoryUnavailable
}

// eebusRuntimeLifecycle owns only startup reconstruction and capability
// replacement. It deliberately exposes no operator start/stop/restart API;
// that broader surface belongs to the later generic DriverManager.
type eebusRuntimeLifecycle struct {
	mu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	start  eebusRuntimeStarter
	wait   eebusRestartWait
	policy eebusRestartPolicy

	slot    *eebusRuntimeSlot
	adapter *eebusRuntimeAdapter
	handler http.Handler
	admin   eebusruntime.AdminV1

	state            eebusLifecycleState
	attempts         int
	revision         uint64
	adminAvailable   bool
	timerOutstanding bool
	degradedReason   eebusadmin.EEBusDegradedReason

	recovery sync.WaitGroup
	stopOnce sync.Once
	stopErr  error
}

// startEEBusAdminAwareRuntime always returns the stable lifecycle seam when
// eeBUS is configured, including when the first construction attempt fails.
// The initial error remains available to the caller for a sanitized log, while
// the lifecycle owns the bounded reconstruction window.
func startEEBusAdminAwareRuntime(ctx context.Context, config ebusgateway.Config) (*eebusRuntimeAdapter, eebusruntime.AdminV1, *eebusRuntimeLifecycle, bool, error) {
	resolver := resolveEEBusInterfaceAddressesFn
	operatorFactory := newEEBusOperatorRuntimeFn
	runtimeFactory := newEEBusRuntimeFn
	lifecycle, initialErr := newEEBusRuntimeLifecycle(ctx, config.EEBusConfig.Enabled, eebusRuntimeLifecycleOptions{
		policy: eebusRestartPolicy{MaxAttempts: eebusStartupMaxAttempts, Backoff: eebusStartupBackoff},
		wait:   waitEEBusRestart,
		start: func(attemptCtx context.Context) (*eebusRuntimeAdapter, eebusruntime.AdminV1, bool, error) {
			return startEEBusAdminAwareRuntimeOnce(attemptCtx, config, resolver, operatorFactory, runtimeFactory)
		},
	})
	return lifecycle.Adapter(), lifecycle.Admin(), lifecycle, lifecycle.AdminAvailable(), initialErr
}

// startEEBusAdminAwareRuntimeOnce attempts the typed operator-capable runtime
// first. If that private capability cannot be built, the public read-only
// runtime remains available and only the HTTP operator boundary degrades.
func startEEBusAdminAwareRuntimeOnce(
	ctx context.Context,
	config ebusgateway.Config,
	resolver eebusInterfaceAddressResolver,
	operatorFactory eebusOperatorRuntimeFactory,
	runtimeFactory eebusRuntimeFactory,
) (*eebusRuntimeAdapter, eebusruntime.AdminV1, bool, error) {
	var operatorErr error
	if config.EEBusConfig.Enabled {
		adapter, admin, runtimeErr := startEEBusOperatorRuntime(ctx, config.EEBusConfig, resolver, operatorFactory)
		if runtimeErr == nil {
			return adapter, admin, true, nil
		}
		operatorErr = runtimeErr
		log.Printf("eeBUS operator boundary unavailable reason=operator_runtime")
	}
	adapter, err := startEEBusRuntime(ctx, config.EEBusConfig, resolver, runtimeFactory)
	if err != nil {
		return nil, nil, false, errors.Join(operatorErr, err)
	}
	if adapter != nil && operatorErr != nil {
		adapter.startupDegradedReason = eebusStartupFailureReason(operatorErr)
	}
	return adapter, nil, false, nil
}

func newEEBusRuntimeLifecycle(
	parent context.Context,
	enabled bool,
	options eebusRuntimeLifecycleOptions,
) (*eebusRuntimeLifecycle, error) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	lifecycle := &eebusRuntimeLifecycle{
		ctx: ctx, cancel: cancel,
		start: options.start, wait: options.wait, policy: options.policy,
		handler: unavailableEEBusAdminHandler(eebusadmin.ReadinessV1{
			ProcessReadiness: eebusadmin.ProcessReadinessReady,
			EEBusReadiness:   eebusadmin.EEBusReadinessDisabled,
		}),
		state: eebusLifecycleDisabled, revision: 1,
	}
	if !enabled {
		return lifecycle, nil
	}
	if lifecycle.start == nil {
		cancel()
		return nil, errors.New("enabled eeBUS lifecycle requires a starter")
	}
	if lifecycle.wait == nil {
		lifecycle.wait = waitEEBusRestart
	}
	if lifecycle.policy.MaxAttempts <= 0 {
		lifecycle.policy.MaxAttempts = eebusStartupMaxAttempts
	}
	if lifecycle.policy.Backoff <= 0 {
		lifecycle.policy.Backoff = eebusStartupBackoff
	}
	lifecycle.slot = newEEBusRuntimeSlot(nil)
	lifecycle.slot.setCancel(cancel)
	lifecycle.adapter = &eebusRuntimeAdapter{runtime: lifecycle.slot}

	initialErr, complete := lifecycle.attempt(false)
	if !complete {
		lifecycle.scheduleRecovery()
	}
	return lifecycle, initialErr
}

func (lifecycle *eebusRuntimeLifecycle) attempt(retire bool) (error, bool) {
	if lifecycle == nil {
		return errors.New("eeBUS lifecycle unavailable"), false
	}
	if retire {
		lifecycle.publishUnavailable(eebusLifecycleStarting, "")
		lifecycle.slot.Retire()
	} else {
		lifecycle.setState(eebusLifecycleStarting, false)
	}

	lifecycle.mu.Lock()
	lifecycle.attempts++
	lifecycle.revision++
	lifecycle.mu.Unlock()

	adapter, admin, adminAvailable, err := lifecycle.start(lifecycle.ctx)
	reason := eebusStartupFailureReason(err)
	if err == nil && adapter != nil && adapter.startupDegradedReason != "" {
		reason = adapter.startupDegradedReason
	}
	if err != nil && adapter == nil {
		lifecycle.publishUnavailable(eebusLifecycleDegraded, reason)
		return err, false
	}
	if adapter == nil || adapter.runtime == nil {
		lifecycle.publishUnavailable(eebusLifecycleDegraded, eebusadmin.EEBusDegradedReasonRuntimeFactoryUnavailable)
		return errors.New("eeBUS starter returned no runtime"), false
	}
	if adminAvailable && admin == nil {
		_ = adapter.Shutdown()
		lifecycle.publishUnavailable(eebusLifecycleDegraded, eebusadmin.EEBusDegradedReasonAdminBoundaryUnavailable)
		return errors.New("eeBUS starter returned incomplete admin capability"), false
	}

	if !adminAvailable && err == nil && reason == eebusadmin.EEBusDegradedReasonUnknownStartupFailure {
		reason = eebusadmin.EEBusDegradedReasonAdminBoundaryUnavailable
	}
	handler := unavailableEEBusAdminHandler(eebusadmin.ReadinessV1{
		ProcessReadiness:    eebusadmin.ProcessReadinessReady,
		EEBusReadiness:      eebusadmin.EEBusReadinessDegraded,
		EEBusDegradedReason: reason,
	})
	if adminAvailable {
		var handlerErr error
		handler, handlerErr = newEEBusAdminHandler(adapter, admin, eebusadmin.ReadinessV1{
			ProcessReadiness: eebusadmin.ProcessReadinessReady,
			EEBusReadiness:   eebusadmin.EEBusReadinessReady,
		})
		if handlerErr != nil {
			admin = nil
			adminAvailable = false
			err = errors.Join(err, handlerErr)
			reason = eebusadmin.EEBusDegradedReasonAdminBoundaryUnavailable
			handler = unavailableEEBusAdminHandler(eebusadmin.ReadinessV1{
				ProcessReadiness:    eebusadmin.ProcessReadinessReady,
				EEBusReadiness:      eebusadmin.EEBusReadinessDegraded,
				EEBusDegradedReason: reason,
			})
		}
	}
	if !lifecycle.slot.Replace(adapter.runtime) {
		lifecycle.publishUnavailable(eebusLifecycleStopped, eebusadmin.EEBusDegradedReasonUnknownStartupFailure)
		return context.Canceled, false
	}

	lifecycle.mu.Lock()
	lifecycle.handler = handler
	lifecycle.admin = admin
	lifecycle.adminAvailable = adminAvailable
	lifecycle.timerOutstanding = false
	if adminAvailable {
		lifecycle.state = eebusLifecycleRunning
		lifecycle.degradedReason = ""
	} else {
		lifecycle.state = eebusLifecycleDegraded
		lifecycle.degradedReason = reason
	}
	lifecycle.revision++
	lifecycle.mu.Unlock()
	return err, adminAvailable
}

func (lifecycle *eebusRuntimeLifecycle) scheduleRecovery() {
	if lifecycle == nil {
		return
	}
	lifecycle.mu.Lock()
	if lifecycle.state == eebusLifecycleDisabled || lifecycle.state == eebusLifecycleStopped || lifecycle.attempts >= lifecycle.policy.MaxAttempts {
		lifecycle.state = eebusLifecycleDegraded
		lifecycle.timerOutstanding = false
		lifecycle.revision++
		lifecycle.mu.Unlock()
		return
	}
	lifecycle.state = eebusLifecycleBackoff
	lifecycle.timerOutstanding = true
	lifecycle.revision++
	lifecycle.mu.Unlock()

	lifecycle.recovery.Add(1)
	go lifecycle.recover()
}

func (lifecycle *eebusRuntimeLifecycle) recover() {
	defer lifecycle.recovery.Done()
	for {
		if !lifecycle.wait(lifecycle.ctx, lifecycle.policy.Backoff) {
			lifecycle.markStoppedIfCancelled()
			return
		}
		lifecycle.mu.Lock()
		lifecycle.timerOutstanding = false
		lifecycle.revision++
		lifecycle.mu.Unlock()

		_, complete := lifecycle.attempt(true)
		if complete {
			return
		}

		lifecycle.mu.Lock()
		if lifecycle.ctx.Err() != nil {
			lifecycle.state = eebusLifecycleStopped
			lifecycle.timerOutstanding = false
			lifecycle.revision++
			lifecycle.mu.Unlock()
			return
		}
		if lifecycle.attempts >= lifecycle.policy.MaxAttempts {
			lifecycle.state = eebusLifecycleDegraded
			lifecycle.timerOutstanding = false
			lifecycle.revision++
			lifecycle.mu.Unlock()
			return
		}
		lifecycle.state = eebusLifecycleBackoff
		lifecycle.timerOutstanding = true
		lifecycle.revision++
		lifecycle.mu.Unlock()
	}
}

func (lifecycle *eebusRuntimeLifecycle) publishUnavailable(state eebusLifecycleState, reason eebusadmin.EEBusDegradedReason) {
	if lifecycle == nil {
		return
	}
	lifecycle.mu.Lock()
	readiness := eebusReadinessForLifecycle(eebusRuntimeLifecycleSnapshot{State: state, DegradedReason: reason})
	lifecycle.handler = unavailableEEBusAdminHandler(readiness)
	lifecycle.admin = nil
	lifecycle.adminAvailable = false
	lifecycle.state = state
	lifecycle.degradedReason = readiness.EEBusDegradedReason
	lifecycle.timerOutstanding = false
	lifecycle.revision++
	lifecycle.mu.Unlock()
}

func (lifecycle *eebusRuntimeLifecycle) setState(state eebusLifecycleState, timerOutstanding bool) {
	if lifecycle == nil {
		return
	}
	lifecycle.mu.Lock()
	lifecycle.state = state
	lifecycle.timerOutstanding = timerOutstanding
	readiness := eebusReadinessForLifecycle(eebusRuntimeLifecycleSnapshot{State: state, DegradedReason: lifecycle.degradedReason})
	if state != eebusLifecycleRunning {
		lifecycle.handler = unavailableEEBusAdminHandler(readiness)
	}
	lifecycle.degradedReason = readiness.EEBusDegradedReason
	lifecycle.revision++
	lifecycle.mu.Unlock()
}

func (lifecycle *eebusRuntimeLifecycle) markStoppedIfCancelled() {
	if lifecycle == nil || lifecycle.ctx.Err() == nil {
		return
	}
	lifecycle.publishUnavailable(eebusLifecycleStopped, lifecycle.LifecycleSnapshot().DegradedReason)
}

func (lifecycle *eebusRuntimeLifecycle) Adapter() *eebusRuntimeAdapter {
	if lifecycle == nil {
		return nil
	}
	return lifecycle.adapter
}

func (lifecycle *eebusRuntimeLifecycle) Admin() eebusruntime.AdminV1 {
	if lifecycle == nil {
		return nil
	}
	lifecycle.mu.RLock()
	defer lifecycle.mu.RUnlock()
	return lifecycle.admin
}

func (lifecycle *eebusRuntimeLifecycle) AdminAvailable() bool {
	if lifecycle == nil {
		return false
	}
	lifecycle.mu.RLock()
	defer lifecycle.mu.RUnlock()
	return lifecycle.adminAvailable
}

func (lifecycle *eebusRuntimeLifecycle) Configured() bool {
	return lifecycle != nil && lifecycle.adapter != nil
}

func (lifecycle *eebusRuntimeLifecycle) LifecycleSnapshot() eebusRuntimeLifecycleSnapshot {
	if lifecycle == nil {
		return eebusRuntimeLifecycleSnapshot{State: eebusLifecycleDisabled}
	}
	lifecycle.mu.RLock()
	defer lifecycle.mu.RUnlock()
	return eebusRuntimeLifecycleSnapshot{
		State: lifecycle.state, Attempts: lifecycle.attempts, Revision: lifecycle.revision,
		AdminAvailable: lifecycle.adminAvailable, TimerOutstanding: lifecycle.timerOutstanding,
		DegradedReason: lifecycle.degradedReason,
	}
}

func (lifecycle *eebusRuntimeLifecycle) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if lifecycle == nil {
		unavailableEEBusAdminHandler(eebusReadinessForLifecycle(eebusRuntimeLifecycleSnapshot{State: eebusLifecycleDisabled})).ServeHTTP(writer, request)
		return
	}
	lifecycle.mu.RLock()
	defer lifecycle.mu.RUnlock()
	lifecycle.handler.ServeHTTP(writer, request)
}

func (lifecycle *eebusRuntimeLifecycle) Shutdown() error {
	if lifecycle == nil {
		return nil
	}
	lifecycle.stopOnce.Do(func() {
		lifecycle.cancel()
		lifecycle.recovery.Wait()
		lifecycle.publishUnavailable(eebusLifecycleStopped, lifecycle.LifecycleSnapshot().DegradedReason)
		if lifecycle.adapter != nil {
			lifecycle.stopErr = lifecycle.adapter.Shutdown()
		}
	})
	return lifecycle.stopErr
}

func waitEEBusRestart(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func newEEBusAdminHandler(eebusAdapter *eebusRuntimeAdapter, eebusAdmin eebusruntime.AdminV1, readiness eebusadmin.ReadinessV1) (http.Handler, error) {
	config := eebusadmin.Config{Admin: eebusAdmin, Raw: eebusAdapter, Readiness: func() eebusadmin.ReadinessV1 { return readiness }}
	config.Audit = func(event eebusadmin.AuditEvent) {
		log.Printf("eebus_admin_audit action=%s scope=host_operator request_id=%s idempotency=%s prior=%s resulting=%s reason=%s timestamp=%s",
			event.Action, event.RequestID, event.IdempotencyOutcome, event.PriorStateClass,
			event.ResultingStateClass, event.Reason, event.Timestamp.UTC().Format(time.RFC3339Nano))
	}
	return eebusadmin.NewServer(config)
}

func unavailableEEBusAdminHandler(readiness eebusadmin.ReadinessV1) http.Handler {
	return eebusadmin.NewUnavailableHandlerWithReadiness(func() eebusadmin.ReadinessV1 { return readiness })
}

func eebusReadinessForLifecycle(snapshot eebusRuntimeLifecycleSnapshot) eebusadmin.ReadinessV1 {
	readiness := eebusadmin.ReadinessV1{ProcessReadiness: eebusadmin.ProcessReadinessReady}
	switch snapshot.State {
	case eebusLifecycleDisabled:
		readiness.EEBusReadiness = eebusadmin.EEBusReadinessDisabled
	case eebusLifecycleStarting:
		readiness.EEBusReadiness = eebusadmin.EEBusReadinessStarting
	case eebusLifecycleRunning:
		readiness.EEBusReadiness = eebusadmin.EEBusReadinessReady
	default:
		readiness.EEBusReadiness = eebusadmin.EEBusReadinessDegraded
		readiness.EEBusDegradedReason = snapshot.DegradedReason
		if readiness.EEBusDegradedReason == "" {
			readiness.EEBusDegradedReason = eebusadmin.EEBusDegradedReasonUnknownStartupFailure
		}
	}
	return readiness
}
