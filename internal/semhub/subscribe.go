package semhub

import (
	"context"
	"errors"
	"sync"
)

// SubscriberQueueLimit bounds every subscriber queue. When it is full the
// oldest entries are dropped and replaced by one coalesced Overflow.
const SubscriberQueueLimit = 256

// replayRingLimit bounds the in-process replay ring used by Since.
const replayRingLimit = 256

// ErrResyncRequired reports a cursor that cannot be continued: another hub
// incarnation, a sequence beyond the latest, or older than the replay ring.
// The consumer re-reads the current state.
var ErrResyncRequired = errors.New("semhub: resync required")

// ErrSubscriptionClosed reports a closed subscription.
var ErrSubscriptionClosed = errors.New("semhub: subscription closed")

// Cursor names one position of the event stream of one hub incarnation.
type Cursor struct {
	Incarnation string `json:"incarnation"`
	Seq         uint64 `json:"seq"`
}

type eventBus struct {
	mu          sync.Mutex
	incarnation string
	seq         uint64
	ring        []Event
	subscribers map[*Subscription]struct{}
	queueLimit  int
}

func newEventBus(incarnation string, queueLimit int) *eventBus {
	return &eventBus{incarnation: incarnation, subscribers: make(map[*Subscription]struct{}), queueLimit: queueLimit}
}

// emit assigns the next HubSeq and enqueues the event for every subscriber.
// It never blocks on a subscriber and never runs subscriber code.
func (bus *eventBus) emit(event Event) uint64 {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	bus.seq++
	event.HubSeq = bus.seq
	bus.ring = append(bus.ring, event)
	if len(bus.ring) > replayRingLimit {
		bus.ring = append([]Event(nil), bus.ring[len(bus.ring)-replayRingLimit:]...)
	}
	for subscriber := range bus.subscribers {
		subscriber.push(event)
	}
	return event.HubSeq
}

func (bus *eventBus) cursor() Cursor {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return Cursor{Incarnation: bus.incarnation, Seq: bus.seq}
}

func (bus *eventBus) since(cursor Cursor) ([]Event, error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if cursor.Incarnation != bus.incarnation || cursor.Seq > bus.seq {
		return nil, ErrResyncRequired
	}
	oldest := bus.seq + 1
	if len(bus.ring) != 0 {
		oldest = bus.ring[0].HubSeq
	}
	if cursor.Seq+1 < oldest {
		return nil, ErrResyncRequired
	}
	out := make([]Event, 0, bus.seq-cursor.Seq)
	for _, event := range bus.ring {
		if event.HubSeq > cursor.Seq {
			out = append(out, cloneEvent(event))
		}
	}
	return out, nil
}

func (bus *eventBus) subscribe() *Subscription {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	subscription := &Subscription{bus: bus, limit: bus.queueLimit, notify: make(chan struct{}, 1), done: make(chan struct{})}
	bus.subscribers[subscription] = struct{}{}
	return subscription
}

// Subscription delivers events in HubSeq order, at most once, from a bounded
// queue. A slow consumer loses the oldest entries and then receives exactly
// one Overflow{Dropped, ResumeSeq} before the next delivered event.
type Subscription struct {
	bus     *eventBus
	mu      sync.Mutex
	limit   int
	queue   []Event
	dropped uint64
	closed  bool
	notify  chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (s *Subscription) push(event Event) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if len(s.queue) >= s.limit {
		s.queue = s.queue[1:]
		s.dropped++
	}
	s.queue = append(s.queue, cloneEvent(event))
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// Next returns the next event, waiting until one is queued, the context ends
// or the subscription closes.
func (s *Subscription) Next(ctx context.Context) (Event, error) {
	for {
		s.mu.Lock()
		if s.dropped != 0 && len(s.queue) != 0 {
			event := Event{Kind: EventOverflow, Dropped: s.dropped, ResumeSeq: s.queue[0].HubSeq}
			s.dropped = 0
			s.mu.Unlock()
			return event, nil
		}
		if len(s.queue) != 0 {
			event := s.queue[0]
			s.queue = s.queue[1:]
			s.mu.Unlock()
			return event, nil
		}
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return Event{}, ErrSubscriptionClosed
		}
		select {
		case <-s.notify:
		case <-s.done:
		case <-ctx.Done():
			return Event{}, ctx.Err()
		}
	}
}

// Close detaches the subscription. It is idempotent.
func (s *Subscription) Close() {
	s.once.Do(func() {
		s.bus.mu.Lock()
		delete(s.bus.subscribers, s)
		s.bus.mu.Unlock()
		s.mu.Lock()
		s.closed = true
		s.queue = nil
		s.mu.Unlock()
		close(s.done)
	})
}

func cloneEvent(event Event) Event {
	if event.Control != nil {
		control := *event.Control
		event.Control = &control
	}
	if event.Revisions != nil {
		revisions := *event.Revisions
		event.Revisions = &revisions
	}
	event.Drivers = append([]string(nil), event.Drivers...)
	return event
}
