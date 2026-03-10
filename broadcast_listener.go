package ebusgateway

import (
	"context"
	"fmt"
	"sync"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

type BroadcastListener struct {
	router        *router.BusEventRouter
	reconstructor *PassiveTransactionReconstructor
	ownsSource    bool
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
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
				return
			}
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
			if listener.router == nil || event.Kind != PassiveClassifiedEventBroadcastFrame {
				continue
			}
			_ = listener.router.HandleBroadcast(event.Request)
		}
	}
}
