package ebusgateway

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

const broadcastSupervisorRecoveryWindow = 5 * time.Second

var (
	observeFirstBroadcastSupervisorState            = expvar.NewString("observe_first_broadcast_supervisor_state")
	observeFirstBroadcastSupervisorTransitionsTotal = expvar.NewMap("observe_first_broadcast_supervisor_transitions_total")
	observeFirstBroadcastSupervisorFaultsTotal      = expvar.NewMap("observe_first_broadcast_supervisor_faults_total")
	observeFirstBroadcastSupervisorResubscribeTotal = expvar.NewInt("observe_first_broadcast_supervisor_resubscribe_total")
)

type BroadcastListener struct {
	router        *router.BusEventRouter
	reconstructor *PassiveTransactionReconstructor
	ownsSource    bool
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup

	stateMu            sync.Mutex
	degraded           bool
	recoveryGeneration atomic.Uint64
}

func StartBroadcastListener(ctx context.Context, cfg Config, router *router.BusEventRouter) (*BroadcastListener, error) {
	return StartBroadcastListenerWithTransport(ctx, cfg, router, nil)
}

func StartBroadcastListenerWithTransport(ctx context.Context, cfg Config, router *router.BusEventRouter, wrap func(transport.RawTransport) transport.RawTransport) (*BroadcastListener, error) {
	if router == nil {
		return nil, fmt.Errorf("broadcast listener missing router: %w", ebuserrors.ErrInvalidPayload)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	listenerCtx, cancel := context.WithCancel(ctx)
	reconstructor := newPassiveTransactionReconstructorCore(cfg)
	reconstructor.ctx, reconstructor.cancel = context.WithCancel(listenerCtx)

	subscription, err := reconstructor.Subscribe("broadcast-listener", PassiveSubscriberCritical, 0)
	if err != nil {
		cancel()
		return nil, err
	}
	tap, err := StartPassiveBusTapWithTransport(reconstructor.ctx, reconstructor.cfg, reconstructor, wrap)
	if err != nil {
		subscription.Close()
		reconstructor.cancel()
		cancel()
		return nil, err
	}
	reconstructor.tap = tap
	listener := &BroadcastListener{
		router:        router,
		reconstructor: reconstructor,
		ownsSource:    true,
		ctx:           listenerCtx,
		cancel:        cancel,
	}
	listener.wg.Add(1)
	observeFirstBroadcastSupervisorState.Set("healthy")
	go listener.run(subscription)
	return listener, nil
}

func StartBroadcastListenerWithReconstructor(ctx context.Context, router *router.BusEventRouter, reconstructor *PassiveTransactionReconstructor) (*BroadcastListener, error) {
	if router == nil {
		return nil, fmt.Errorf("broadcast listener missing router: %w", ebuserrors.ErrInvalidPayload)
	}
	if reconstructor == nil {
		return nil, fmt.Errorf("broadcast listener missing reconstructor: %w", ebuserrors.ErrInvalidPayload)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	listenerCtx, cancel := context.WithCancel(ctx)
	subscription, err := reconstructor.Subscribe("broadcast-listener", PassiveSubscriberCritical, 0)
	if err != nil {
		cancel()
		return nil, err
	}

	listener := &BroadcastListener{
		router:        router,
		reconstructor: reconstructor,
		ctx:           listenerCtx,
		cancel:        cancel,
	}
	listener.wg.Add(1)
	observeFirstBroadcastSupervisorState.Set("healthy")
	go listener.run(subscription)
	return listener, nil
}

func (listener *BroadcastListener) Start(ctx context.Context) {
	_ = ctx
}

func (listener *BroadcastListener) Close() error {
	if listener == nil {
		return nil
	}
	if listener.cancel != nil {
		listener.cancel()
	}
	listener.wg.Wait()
	if listener.reconstructor == nil || !listener.ownsSource {
		return nil
	}
	return listener.reconstructor.Close()
}

func (listener *BroadcastListener) run(initial *PassiveClassifiedSubscription) {
	defer listener.wg.Done()

	subscription := initial
	for {
		if subscription == nil {
			next, err := listener.reconstructor.Subscribe("broadcast-listener", PassiveSubscriberCritical, 0)
			if err != nil {
				listener.markDegraded("resubscribe_failed")
				return
			}
			observeFirstBroadcastSupervisorResubscribeTotal.Add(1)
			subscription = next
		}
		if !listener.consume(subscription) {
			return
		}
		subscription = nil
	}
}

func (listener *BroadcastListener) consume(subscription *PassiveClassifiedSubscription) bool {
	defer subscription.Close()

	for {
		select {
		case <-listener.ctx.Done():
			return false
		case event, ok := <-subscription.Events():
			if !ok {
				return listener.ctx.Err() == nil
			}
			if event.Kind != PassiveClassifiedEventBroadcastFrame || listener.router == nil {
				listener.markHealthy("post_reset_event")
				continue
			}
			if listener.handleRouterFault(listener.router.HandleBroadcast(event.Request)) {
				return true
			}
			listener.markHealthy("post_reset_event")
		}
	}
}

func (listener *BroadcastListener) handleRouterFault(errs []error) bool {
	overflow := false
	for _, err := range errs {
		if errors.Is(err, router.ErrBroadcastEventOverflow) {
			overflow = true
			break
		}
	}
	if !overflow {
		return false
	}
	observeFirstBroadcastSupervisorFaultsTotal.Add("router_overflow", 1)
	listener.markDegraded("router_overflow")
	return true
}

func (listener *BroadcastListener) markDegraded(reason string) {
	if listener == nil {
		return
	}

	generation := listener.recoveryGeneration.Add(1)

	listener.stateMu.Lock()
	if !listener.degraded {
		observeFirstBroadcastSupervisorTransitionsTotal.Add(fmt.Sprintf("healthy->degraded:%s", reason), 1)
	}
	listener.degraded = true
	observeFirstBroadcastSupervisorState.Set("degraded")
	listener.stateMu.Unlock()

	go listener.recoverAfterFaultFreeWindow(generation)
}

func (listener *BroadcastListener) recoverAfterFaultFreeWindow(generation uint64) {
	timer := time.NewTimer(broadcastSupervisorRecoveryWindow)
	defer timer.Stop()

	select {
	case <-listener.ctx.Done():
		return
	case <-timer.C:
		listener.markHealthyIfGeneration(generation, "fault_free_window")
	}
}

func (listener *BroadcastListener) markHealthy(reason string) {
	if listener == nil {
		return
	}
	listener.markHealthyIfGeneration(0, reason)
}

func (listener *BroadcastListener) markHealthyIfGeneration(generation uint64, reason string) {
	listener.stateMu.Lock()
	defer listener.stateMu.Unlock()

	if !listener.degraded {
		return
	}
	if generation != 0 && listener.recoveryGeneration.Load() != generation {
		return
	}

	listener.degraded = false
	listener.recoveryGeneration.Add(1)
	observeFirstBroadcastSupervisorTransitionsTotal.Add(fmt.Sprintf("degraded->healthy:%s", reason), 1)
	observeFirstBroadcastSupervisorState.Set("healthy")
}
