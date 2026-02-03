package graphql

import (
	"context"
	"fmt"
	"sync"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/router"
)

// broadcastBufferSize buffers bursts so router broadcasts never block on slow clients.
const broadcastBufferSize = 16

type BroadcastEvent struct {
	Source    byte
	Target    byte
	Primary   byte
	Secondary byte
	Data      []byte
}

type broadcastKey struct {
	primary   byte
	secondary byte
}

type broadcastSubscriber struct {
	id        uint64
	ch        chan interface{}
	closeOnce sync.Once
}

func (sub *broadcastSubscriber) close() {
	sub.closeOnce.Do(func() { close(sub.ch) })
}

type BroadcastHub struct {
	name string

	mu          sync.RWMutex
	subscribers map[broadcastKey]map[uint64]*broadcastSubscriber
	zoneSubs    map[uint64]*broadcastSubscriber
	dhwSubs     map[uint64]*broadcastSubscriber
	energySubs  map[uint64]*broadcastSubscriber
	nextID      uint64
	onChange    func()
}

func NewBroadcastHub(onChange func()) *BroadcastHub {
	return &BroadcastHub{
		name:        "graphql_broadcasts",
		subscribers: make(map[broadcastKey]map[uint64]*broadcastSubscriber),
		zoneSubs:    make(map[uint64]*broadcastSubscriber),
		dhwSubs:     make(map[uint64]*broadcastSubscriber),
		energySubs:  make(map[uint64]*broadcastSubscriber),
		onChange:    onChange,
	}
}

func (hub *BroadcastHub) Name() string {
	if hub == nil {
		return ""
	}
	return hub.name
}

func (hub *BroadcastHub) Methods() []registry.Method {
	return nil
}

func (hub *BroadcastHub) Subscriptions() []router.Subscription {
	if hub == nil {
		return nil
	}

	hub.mu.RLock()
	keys := make([]router.Subscription, 0, len(hub.subscribers))
	for key := range hub.subscribers {
		keys = append(keys, router.Subscription{Primary: key.primary, Secondary: key.secondary})
	}
	hub.mu.RUnlock()
	return keys
}

func (hub *BroadcastHub) OnBroadcast(frame protocol.Frame) error {
	if hub == nil {
		return nil
	}

	key := broadcastKey{primary: frame.Primary, secondary: frame.Secondary}

	hub.mu.RLock()
	subs := hub.subscribers[key]
	if len(subs) == 0 {
		hub.mu.RUnlock()
		return nil
	}
	list := make([]*broadcastSubscriber, 0, len(subs))
	for _, sub := range subs {
		list = append(list, sub)
	}
	hub.mu.RUnlock()

	data := append([]byte(nil), frame.Data...)
	event := BroadcastEvent{
		Source:    frame.Source,
		Target:    frame.Target,
		Primary:   frame.Primary,
		Secondary: frame.Secondary,
		Data:      data,
	}

	for _, sub := range list {
		select {
		case sub.ch <- event:
		default:
		}
	}

	return nil
}

func (hub *BroadcastHub) BuildRequest(registry.Method, map[string]any) (protocol.Frame, error) {
	return protocol.Frame{}, fmt.Errorf("broadcast hub does not send requests: %w", ebuserrors.ErrInvalidPayload)
}

func (hub *BroadcastHub) DecodeResponse(registry.Method, protocol.Frame, map[string]any) (any, error) {
	return nil, fmt.Errorf("broadcast hub does not decode responses: %w", ebuserrors.ErrInvalidPayload)
}

func (hub *BroadcastHub) Subscribe(ctx context.Context, primary, secondary byte) (chan interface{}, error) {
	if hub == nil {
		return nil, fmt.Errorf("broadcast hub missing: %w", ebuserrors.ErrInvalidPayload)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	key := broadcastKey{primary: primary, secondary: secondary}
	sub := &broadcastSubscriber{
		ch: make(chan interface{}, broadcastBufferSize),
	}

	var notify bool
	hub.mu.Lock()
	subscribers, ok := hub.subscribers[key]
	if !ok {
		subscribers = make(map[uint64]*broadcastSubscriber)
		hub.subscribers[key] = subscribers
		notify = true
	}
	hub.nextID++
	sub.id = hub.nextID
	subscribers[sub.id] = sub
	id := sub.id
	hub.mu.Unlock()

	if notify {
		hub.notifyChange()
	}

	// Goroutine exits when ctx.Done() closes; removes the subscriber entry.
	go func(id uint64) {
		<-ctx.Done()
		hub.removeSubscriber(key, id)
	}(id)

	return sub.ch, nil
}

func (hub *BroadcastHub) SubscribeZones(ctx context.Context) (chan interface{}, error) {
	return hub.subscribeSemantic(ctx, hub.zoneSubs)
}

func (hub *BroadcastHub) SubscribeDHW(ctx context.Context) (chan interface{}, error) {
	return hub.subscribeSemantic(ctx, hub.dhwSubs)
}

func (hub *BroadcastHub) SubscribeEnergy(ctx context.Context) (chan interface{}, error) {
	return hub.subscribeSemantic(ctx, hub.energySubs)
}

func (hub *BroadcastHub) PublishZoneUpdate(zone Zone) {
	hub.publishSemantic(hub.zoneSubs, zone)
}

func (hub *BroadcastHub) PublishDHWUpdate(status *DhwStatus) {
	if status == nil {
		return
	}
	hub.publishSemantic(hub.dhwSubs, status)
}

func (hub *BroadcastHub) PublishEnergyUpdate(totals *EnergyTotals) {
	if totals == nil {
		return
	}
	hub.publishSemantic(hub.energySubs, totals)
}

func (hub *BroadcastHub) subscribeSemantic(ctx context.Context, subscribers map[uint64]*broadcastSubscriber) (chan interface{}, error) {
	if hub == nil {
		return nil, fmt.Errorf("broadcast hub missing: %w", ebuserrors.ErrInvalidPayload)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	sub := &broadcastSubscriber{
		ch: make(chan interface{}, broadcastBufferSize),
	}

	hub.mu.Lock()
	hub.nextID++
	sub.id = hub.nextID
	subscribers[sub.id] = sub
	id := sub.id
	hub.mu.Unlock()

	go func(id uint64) {
		<-ctx.Done()
		hub.removeSemanticSubscriber(subscribers, id)
	}(id)

	return sub.ch, nil
}

func (hub *BroadcastHub) removeSemanticSubscriber(subscribers map[uint64]*broadcastSubscriber, id uint64) {
	if hub == nil {
		return
	}
	hub.mu.Lock()
	sub := subscribers[id]
	delete(subscribers, id)
	hub.mu.Unlock()

	if sub != nil {
		sub.close()
	}
}

func (hub *BroadcastHub) publishSemantic(subscribers map[uint64]*broadcastSubscriber, payload any) {
	if hub == nil {
		return
	}

	hub.mu.RLock()
	list := make([]*broadcastSubscriber, 0, len(subscribers))
	for _, sub := range subscribers {
		list = append(list, sub)
	}
	hub.mu.RUnlock()

	for _, sub := range list {
		select {
		case sub.ch <- payload:
		default:
		}
	}
}

func (hub *BroadcastHub) notifyChange() {
	if hub == nil || hub.onChange == nil {
		return
	}
	hub.onChange()
}

func (hub *BroadcastHub) removeSubscriber(key broadcastKey, id uint64) {
	if hub == nil {
		return
	}

	var (
		sub          *broadcastSubscriber
		notifyChange bool
	)

	hub.mu.Lock()
	subscribers, ok := hub.subscribers[key]
	if ok {
		sub = subscribers[id]
		delete(subscribers, id)
		if len(subscribers) == 0 {
			delete(hub.subscribers, key)
			notifyChange = true
		}
	}
	hub.mu.Unlock()

	if sub != nil {
		sub.close()
	}
	if notifyChange {
		hub.notifyChange()
	}
}

var _ router.Plane = (*BroadcastHub)(nil)
