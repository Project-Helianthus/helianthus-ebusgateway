package identification

import (
	"sort"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// RingPerAddress bounds how many successful 07 04
// exchanges are retained per target address. With at most 256 addresses the
// store holds at most 256*RingPerAddress exchanges.
const RingPerAddress = 8

// Exchange is the raw, undecoded record of one directed 07 04
// identification exchange as seen by the bus observer.
//
// Request holds the frame bytes QQ ZZ PB SB NN [data] (source, target,
// primary, secondary, data length, data). Response holds the target response
// bytes NN [data] (data length, data). The CRC bytes and ACK bytes are not
// part of protocol.BusEvent and are therefore not retained. Nothing here is
// decoded, normalised or qualified.
type Exchange struct {
	Address    byte
	Sequence   uint64
	SessionID  string
	CapturedAt time.Time
	Request    []byte
	Response   []byte
}

// Snapshot is a copy of the retained exchanges ordered
// by address, then by capture sequence (oldest first).
type Snapshot struct {
	SessionID string
	Exchanges []Exchange
}

// Store is a protocol.BusObserver that retains the
// complete request and response bytes of successful directed 07 04 exchanges.
// Only successful completed exchanges are evidence: an attempt-complete event
// must carry protocol.BusOutcomeSuccess, a request and a response (the same
// success requirement as the active/passive deduplicator). Failed, timed-out,
// NACKed or response-less attempts never enter the ring.
//
// Session identity derivation: the store fixes its session identity from the
// wall-clock time of the first bus event of any kind it observes after
// construction, formatted "observer-<UTC 20060102T150405.000000000Z>". It
// identifies the observer run (gateway process epoch), not an individual scan
// pass; every exchange captured by this store shares it. The store never reads
// scan-loop state.
type Store struct {
	mu        sync.Mutex
	now       func() time.Time
	sessionID string
	sequence  uint64
	rings     map[byte][]Exchange

	// snapshotHook is a test seam invoked by Snapshot after the lock is
	// released and before bytes are copied. Nil in production.
	snapshotHook func()
}

// NewStore returns an empty store. A nil clock uses
// time.Now.
func NewStore(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{now: now, rings: make(map[byte][]Exchange)}
}

// OnBusEvent implements protocol.BusObserver. It copies every retained byte.
func (store *Store) OnBusEvent(event protocol.BusEvent) error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.sessionID == "" {
		store.sessionID = "observer-" + store.now().UTC().Format("20060102T150405.000000000Z")
	}
	if event.Kind != protocol.BusEventAttemptComplete || event.Outcome != protocol.BusOutcomeSuccess || !event.HasRequest || !event.HasResponse {
		return nil
	}
	req := event.Request
	if req.Primary != 0x07 || req.Secondary != 0x04 || req.Target == 0xFE {
		return nil
	}

	request := make([]byte, 0, 5+len(req.Data))
	request = append(request, req.Source, req.Target, req.Primary, req.Secondary, byte(len(req.Data)))
	request = append(request, req.Data...)
	response := make([]byte, 0, 1+len(event.Response.Data))
	response = append(response, byte(len(event.Response.Data)))
	response = append(response, event.Response.Data...)

	store.sequence++
	ring := append(store.rings[req.Target], Exchange{
		Address:    req.Target,
		Sequence:   store.sequence,
		SessionID:  store.sessionID,
		CapturedAt: store.now().UTC(),
		Request:    request,
		Response:   response,
	})
	if len(ring) > RingPerAddress {
		ring = append([]Exchange(nil), ring[len(ring)-RingPerAddress:]...)
	}
	store.rings[req.Target] = ring
	return nil
}

// Snapshot returns a deep copy of the retained exchanges. Only immutable
// record references are gathered under the lock (retained byte slices are
// never mutated after insertion); byte copying happens after it is released so
// a read never stalls bus events for longer than a short struct copy.
func (store *Store) Snapshot() Snapshot {
	if store == nil {
		return Snapshot{}
	}
	store.mu.Lock()
	addresses := make([]int, 0, len(store.rings))
	for address := range store.rings {
		addresses = append(addresses, int(address))
	}
	sort.Ints(addresses)
	out := Snapshot{SessionID: store.sessionID}
	for _, address := range addresses {
		out.Exchanges = append(out.Exchanges, store.rings[byte(address)]...)
	}
	store.mu.Unlock()

	if store.snapshotHook != nil {
		store.snapshotHook()
	}
	for i := range out.Exchanges {
		out.Exchanges[i].Request = append([]byte(nil), out.Exchanges[i].Request...)
		out.Exchanges[i].Response = append([]byte(nil), out.Exchanges[i].Response...)
	}
	return out
}
