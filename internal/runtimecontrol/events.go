package runtimecontrol

import (
	"sync"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

// EventKind is the closed kind of one service event.
type EventKind string

const (
	// EventDesiredAccepted follows a durably committed changing decision.
	EventDesiredAccepted EventKind = "DESIRED_ACCEPTED"
	// EventObservedChanged follows a durably committed observed-view
	// transaction (a catalog revision advance).
	EventObservedChanged EventKind = "OBSERVED_CHANGED"
)

// Event bounds.
const (
	DefaultEventCapacity = 256
	MaxEventCapacity     = 4096
)

// Event is one categorical notification emitted only after the transition it
// reports is durable and visible in the catalog view of CatalogRevision.
// Cursor orders events for resynchronisation; it is not an operation handle
// and never identifies a request. Epoch and Generation name the lifecycle
// correlation the driver's observation refers to (empty before the driver's
// first attempt in this process); for an accepted decision they are the
// driver's correlation at acceptance. Events carry no operation ID, endpoint,
// credential, raw error, principal or authority material.
type Event struct {
	Cursor            uint64
	Kind              EventKind
	DriverID          DriverID
	CatalogRevision   CatalogRevision
	DesiredState      DesiredState
	DesiredRevision   DesiredRevision
	ObservedState     drivermanager.ObservedState
	Reason            drivermanager.ReasonCode
	SafetyQuarantined bool
	Epoch             SourceEpoch
	Generation        DriverGeneration
}

// EventPage is one read of the bounded event log. When Resync is true the
// requested cursor fell out of the retained window: the reader discards its
// state, reloads List and continues from the catalog's EventCursor.
type EventPage struct {
	Events []Event
	Next   uint64
	Resync bool
}

// eventLog is a bounded ring of committed events. It never blocks a writer:
// a slow reader loses the window and resynchronises.
type eventLog struct {
	mu       sync.Mutex
	capacity int
	ring     []Event
	start    int    // index of the oldest retained event
	count    int    // retained events
	last     uint64 // cursor of the newest event, 0 when none
	notify   chan struct{}
}

func newEventLog(capacity int) *eventLog {
	switch {
	case capacity <= 0:
		capacity = DefaultEventCapacity
	case capacity > MaxEventCapacity:
		capacity = MaxEventCapacity
	}
	return &eventLog{capacity: capacity, ring: make([]Event, capacity), notify: make(chan struct{})}
}

// append assigns the next cursor, retains the event and wakes waiters.
func (l *eventLog) append(event Event) uint64 {
	l.mu.Lock()
	l.last++
	event.Cursor = l.last
	index := (l.start + l.count) % l.capacity
	if l.count == l.capacity {
		l.ring[l.start] = event
		l.start = (l.start + 1) % l.capacity
	} else {
		l.ring[index] = event
		l.count++
	}
	close(l.notify)
	l.notify = make(chan struct{})
	cursor := l.last
	l.mu.Unlock()
	return cursor
}

// markGap records that events were discarded before they received a cursor:
// it consumes one cursor and empties the window, so every reader positioned
// before the gap is told to resynchronise.
func (l *eventLog) markGap() {
	l.mu.Lock()
	l.last++
	l.start, l.count = 0, 0
	l.mu.Unlock()
}

func (l *eventLog) cursor() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.last
}

// since returns every retained event after the given cursor, or Resync when
// events after it were dropped from the window.
func (l *eventLog) since(after uint64) EventPage {
	l.mu.Lock()
	defer l.mu.Unlock()
	if after > l.last {
		return EventPage{Next: l.last, Resync: true}
	}
	oldest := l.last - uint64(l.count) + 1
	if l.count == 0 {
		oldest = l.last + 1
	}
	if after+1 < oldest {
		return EventPage{Next: l.last, Resync: true}
	}
	page := EventPage{Next: l.last}
	for c := after + 1; c <= l.last; c++ {
		offset := int(c - oldest)
		page.Events = append(page.Events, l.ring[(l.start+offset)%l.capacity])
	}
	return page
}

func (l *eventLog) wait() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.notify
}

// EventsSince returns the committed events after cursor. A cursor of 0 reads
// from the beginning of the retained window, or asks for a resync when the
// window already dropped events.
func (s *Service) EventsSince(cursor uint64) EventPage { return s.events.since(cursor) }

// EventNotify returns a channel closed when the next event is appended.
func (s *Service) EventNotify() <-chan struct{} { return s.events.wait() }
