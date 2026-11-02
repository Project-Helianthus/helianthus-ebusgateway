package identification

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func identEvent(target byte, response []byte) protocol.BusEvent {
	return protocol.BusEvent{
		Kind:        protocol.BusEventAttemptComplete,
		Outcome:     protocol.BusOutcomeSuccess,
		HasRequest:  true,
		HasResponse: true,
		Request:     protocol.Frame{Source: 0x71, Target: target, Primary: 0x07, Secondary: 0x04},
		Response:    protocol.Frame{Data: response},
	}
}

func TestIdentificationStoreRetainsCompleteExchangeBytes(t *testing.T) {
	clock := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	store := NewStore(func() time.Time { clock = clock.Add(time.Second); return clock })
	payload := []byte{0xB5, 'B', 'A', 'I', '0', '0', 0x01, 0x02, 0x03, 0x04}
	if err := store.OnBusEvent(identEvent(0x08, payload)); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}
	payload[0] = 0x00 // the bus owns the slice; retained bytes must be copies

	snap := store.Snapshot()
	if len(snap.Exchanges) != 1 {
		t.Fatalf("exchanges = %d, want 1", len(snap.Exchanges))
	}
	ex := snap.Exchanges[0]
	if ex.Address != 0x08 || ex.Sequence != 1 {
		t.Fatalf("exchange = %+v", ex)
	}
	if got, want := ex.Request, []byte{0x71, 0x08, 0x07, 0x04, 0x00}; string(got) != string(want) {
		t.Fatalf("request = %x, want %x", got, want)
	}
	if got, want := ex.Response, []byte{0x0A, 0xB5, 'B', 'A', 'I', '0', '0', 0x01, 0x02, 0x03, 0x04}; string(got) != string(want) {
		t.Fatalf("response = %x, want %x", got, want)
	}
	if ex.SessionID == "" || ex.SessionID != snap.SessionID {
		t.Fatalf("session id = %q / %q", ex.SessionID, snap.SessionID)
	}
	if ex.CapturedAt.IsZero() {
		t.Fatalf("captured_at not set")
	}
}

func TestIdentificationStoreFiltersAndBounds(t *testing.T) {
	store := NewStore(nil)
	other := identEvent(0x08, []byte{1})
	other.Request.Primary = 0xB5
	noResp := identEvent(0x08, nil)
	noResp.HasResponse = false
	wrongKind := identEvent(0x08, []byte{1})
	wrongKind.Kind = protocol.BusEventTimeout
	broadcast := identEvent(0xFE, []byte{1})
	for _, ev := range []protocol.BusEvent{other, noResp, wrongKind, broadcast} {
		_ = store.OnBusEvent(ev)
	}
	if n := len(store.Snapshot().Exchanges); n != 0 {
		t.Fatalf("filtered events retained: %d", n)
	}
	for i := 0; i < RingPerAddress+3; i++ {
		_ = store.OnBusEvent(identEvent(0x15, []byte{byte(i)}))
	}
	snap := store.Snapshot()
	if len(snap.Exchanges) != RingPerAddress {
		t.Fatalf("ring = %d, want %d", len(snap.Exchanges), RingPerAddress)
	}
	if snap.Exchanges[0].Sequence != 4 {
		t.Fatalf("oldest retained sequence = %d, want 4", snap.Exchanges[0].Sequence)
	}
}

func TestIdentificationEvidenceSessionDerivedFromFirstEvent(t *testing.T) {
	first := time.Date(2026, 10, 14, 9, 0, 0, 123, time.UTC)
	ticks := []time.Time{first, first.Add(time.Hour), first.Add(2 * time.Hour)}
	store := NewStore(func() time.Time { t := ticks[0]; ticks = ticks[1:]; return t })
	if store.Snapshot().SessionID != "" {
		t.Fatalf("session id set before any event")
	}
	_ = store.OnBusEvent(protocol.BusEvent{Kind: protocol.BusEventTX}) // first event of any kind fixes the session
	want := store.Snapshot().SessionID
	if want != "observer-20261014T090000.000000123Z" {
		t.Fatalf("session id = %q", want)
	}
	_ = store.OnBusEvent(identEvent(0x08, []byte{1}))
	if got := store.Snapshot().Exchanges[0].SessionID; got != want {
		t.Fatalf("session id changed: %q != %q", got, want)
	}
}

func TestIdentificationEvidenceRetainsOnlySuccessfulOutcome(t *testing.T) {
	store := NewStore(nil)
	for _, outcome := range []protocol.BusOutcomeClass{
		protocol.BusOutcomeUnknown, protocol.BusOutcomeTimeout, protocol.BusOutcomeNACK, protocol.BusOutcomeCRCMismatch,
		protocol.BusOutcomeEchoMismatch, protocol.BusOutcomeCollision, protocol.BusOutcomeObserverFault, protocol.BusOutcomeAdapterReset,
	} {
		failed := identEvent(0x08, []byte{1, 2, 3})
		failed.Outcome = outcome
		_ = store.OnBusEvent(failed)
	}
	if n := len(store.Snapshot().Exchanges); n != 0 {
		t.Fatalf("non-success completed attempts retained: %d", n)
	}
	_ = store.OnBusEvent(identEvent(0x08, []byte{1, 2, 3}))
	if n := len(store.Snapshot().Exchanges); n != 1 {
		t.Fatalf("success exchange not retained: %d", n)
	}
}

// TestIdentificationEvidenceWriteDuringSnapshotCopy is deterministic: the test
// hook performs a write after Snapshot released the lock and before it copied
// bytes, so a write is provably interleaved with the read regardless of
// scheduling or GOMAXPROCS. The in-flight snapshot must be unaffected, and
// the write must be visible to the next snapshot.
func TestIdentificationEvidenceWriteDuringSnapshotCopy(t *testing.T) {
	store := NewStore(nil)
	_ = store.OnBusEvent(identEvent(0x08, []byte{7, 2, 3}))
	var before, during uint64
	hookRan := false
	store.snapshotHook = func() {
		hookRan = true
		store.mu.Lock()
		before = store.sequence
		store.mu.Unlock()
		// Fill and wrap the same ring to replace retained records mid-copy.
		for i := 0; i < RingPerAddress+2; i++ {
			_ = store.OnBusEvent(identEvent(0x08, []byte{0xEE, 9, 9}))
		}
		store.mu.Lock()
		during = store.sequence
		store.mu.Unlock()
	}
	snapshots := make(chan Snapshot, 1)
	go func() { snapshots <- store.Snapshot() }()
	var snap Snapshot
	select {
	case snap = <-snapshots:
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot callback blocked a writer; snapshot must release the lock before the callback")
	}
	store.snapshotHook = nil
	if !hookRan || during <= before {
		t.Fatalf("no write occurred between snapshot start and end (hook=%v seq %d->%d)", hookRan, before, during)
	}
	if len(snap.Exchanges) != 1 || snap.Exchanges[0].Sequence != 1 || snap.Exchanges[0].Response[1] != 7 {
		t.Fatalf("in-flight snapshot affected by concurrent writes: %+v", snap.Exchanges)
	}
	if after := store.Snapshot(); len(after.Exchanges) != RingPerAddress || after.Exchanges[len(after.Exchanges)-1].Sequence != during {
		t.Fatalf("subsequent snapshot does not reflect the writes: %d exchanges", len(after.Exchanges))
	}
}

// TestIdentificationEvidenceConcurrentReadWrite exercises the lock discipline
// under -race. It asserts only consistency; overlap is proven
// deterministically by TestIdentificationEvidenceWriteDuringSnapshotCopy.
func TestIdentificationEvidenceConcurrentReadWrite(t *testing.T) {
	store := NewStore(nil)
	for w := 0; w < 4; w++ {
		_ = store.OnBusEvent(identEvent(byte(0x08+w), []byte{0, 2, 3}))
	}
	done := make(chan struct{})
	var started, wg sync.WaitGroup
	started.Add(4)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			started.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				_ = store.OnBusEvent(identEvent(byte(0x08+w), []byte{byte(i), 2, 3}))
				runtime.Gosched()
			}
		}(w)
	}
	started.Wait()
	for r := 0; r < 200; r++ {
		snap := store.Snapshot()
		if len(snap.Exchanges) < 4 {
			t.Fatalf("snapshot %d has %d exchanges, want >= 4", r, len(snap.Exchanges))
		}
		for _, ex := range snap.Exchanges {
			if len(ex.Request) != 5 || len(ex.Response) != 4 || int(ex.Response[0]) != len(ex.Response)-1 || ex.Response[2] != 2 || ex.Response[3] != 3 {
				t.Fatalf("torn exchange: %+v", ex)
			}
			ex.Response[0] = 0xFF // mutating a snapshot must not affect the store
		}
		runtime.Gosched()
	}
	close(done)
	wg.Wait()
	for _, ex := range store.Snapshot().Exchanges {
		if ex.Response[0] != 3 {
			t.Fatalf("snapshot mutation leaked into the store: %+v", ex)
		}
	}
}

func TestIdentificationStoreRejectsWithoutReplacingHistory(t *testing.T) {
	cases := []struct {
		name   string
		change func(*protocol.BusEvent)
	}{
		{"kind", func(e *protocol.BusEvent) { e.Kind = protocol.BusEventTimeout }},
		{"request absent", func(e *protocol.BusEvent) { e.HasRequest = false }},
		{"response absent", func(e *protocol.BusEvent) { e.HasResponse = false }},
		{"primary", func(e *protocol.BusEvent) { e.Request.Primary = 0x08 }},
		{"secondary", func(e *protocol.BusEvent) { e.Request.Secondary = 0x05 }},
		{"broadcast", func(e *protocol.BusEvent) { e.Request.Target = 0xFE }},
	}
	for _, outcome := range []protocol.BusOutcomeClass{protocol.BusOutcomeUnknown, protocol.BusOutcomeTimeout, protocol.BusOutcomeNACK,
		protocol.BusOutcomeCRCMismatch, protocol.BusOutcomeEchoMismatch, protocol.BusOutcomeCollision, protocol.BusOutcomeObserverFault, protocol.BusOutcomeAdapterReset} {
		outcome := outcome
		cases = append(cases, struct {
			name   string
			change func(*protocol.BusEvent)
		}{name: fmt.Sprintf("outcome-%d", outcome), change: func(e *protocol.BusEvent) { e.Outcome = outcome }})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			captured := time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC)
			store := NewStore(func() time.Time { return captured })
			_ = store.OnBusEvent(identEvent(0x08, []byte{7}))
			expected := Snapshot{SessionID: "observer-20000102T030405.000000000Z", Exchanges: []Exchange{{
				Address: 0x08, Sequence: 1, SessionID: "observer-20000102T030405.000000000Z", CapturedAt: captured,
				Request: []byte{0x71, 0x08, 0x07, 0x04, 0}, Response: []byte{1, 7}}}}
			event := identEvent(0x08, []byte{9})
			tc.change(&event)
			_ = store.OnBusEvent(event)
			if got := store.Snapshot(); !reflect.DeepEqual(got, expected) {
				t.Fatalf("rejected event changed history: %+v", got)
			}
			_ = store.OnBusEvent(identEvent(0x08, []byte{8}))
			if got := store.Snapshot(); len(got.Exchanges) != 2 || got.Exchanges[1].Sequence != 2 {
				t.Fatalf("rejection consumed sequence: %+v", got)
			}
		})
	}
}

func TestIdentificationStoreIndependentFieldsAndCopies(t *testing.T) {
	// Each case changes one observer input field, leaving the others fixed.
	for _, tc := range []struct {
		name              string
		source, target    byte
		request, response []byte
	}{
		{"baseline", 0x71, 0x15, []byte{2, 3}, []byte{4, 5}},
		{"source", 0x72, 0x15, []byte{2, 3}, []byte{4, 5}},
		{"target", 0x71, 0x08, []byte{2, 3}, []byte{4, 5}},
		{"request", 0x71, 0x15, []byte{2, 9}, []byte{4, 5}},
		{"response", 0x71, 0x15, []byte{2, 3}, []byte{4, 9}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tick := time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC)
			store := NewStore(func() time.Time { result := tick; tick = tick.Add(time.Second); return result })
			responseInput := make([]byte, len(tc.response), 16)
			copy(responseInput, tc.response)
			event := identEvent(tc.target, responseInput)
			event.Request.Source = tc.source
			event.Request.Data = make([]byte, len(tc.request), 16)
			copy(event.Request.Data, tc.request)
			wantRequest := append([]byte{tc.source, tc.target, 7, 4, 2}, tc.request...)
			wantResponse := append([]byte{2}, tc.response...)
			_ = store.OnBusEvent(event)
			event.Request.Data[0] = 0xFF
			event.Response.Data[0] = 0xFF
			for read := 0; read < 2; read++ {
				snap := store.Snapshot()
				if len(snap.Exchanges) != 1 {
					t.Fatalf("exchanges=%d", len(snap.Exchanges))
				}
				ex := snap.Exchanges[0]
				if ex.Address != tc.target {
					t.Fatalf("address=%x want %x", ex.Address, tc.target)
				}
				if ex.Sequence != 1 {
					t.Fatalf("sequence=%d", ex.Sequence)
				}
				if snap.SessionID != "observer-20000102T030405.000000000Z" || ex.SessionID != "observer-20000102T030405.000000000Z" {
					t.Fatalf("session=%q/%q", snap.SessionID, ex.SessionID)
				}
				if !ex.CapturedAt.Equal(time.Date(2000, 1, 2, 3, 4, 6, 0, time.UTC)) {
					t.Fatalf("capture time changed: %v", ex.CapturedAt)
				}
				if !reflect.DeepEqual(ex.Request, wantRequest) {
					t.Fatalf("request=%x want %x", ex.Request, wantRequest)
				}
				if !reflect.DeepEqual(ex.Response, wantResponse) {
					t.Fatalf("response=%x want %x", ex.Response, wantResponse)
				}
				ex.Request[0] = 0
				ex.Request[len(ex.Request)-1] = 0
				ex.Response[0] = 0
				ex.Response[len(ex.Response)-1] = 0
			}
		})
	}
}

func TestIdentificationStoreAddressOrderAndIndependentCapacity(t *testing.T) {
	store := NewStore(nil)
	_ = store.OnBusEvent(identEvent(0x15, []byte{99}))
	for i := 0; i < 11; i++ {
		_ = store.OnBusEvent(identEvent(0x08, []byte{byte(i)}))
	}
	snap := store.Snapshot()
	if len(snap.Exchanges) != 9 {
		t.Fatalf("total=%d want 9", len(snap.Exchanges))
	}
	for i, ex := range snap.Exchanges[:8] {
		if ex.Address != 0x08 || ex.Sequence != uint64(i+5) || !reflect.DeepEqual(ex.Response, []byte{1, byte(i + 3)}) {
			t.Fatalf("ring[%d]=%+v", i, ex)
		}
	}
	last := snap.Exchanges[8]
	if last.Address != 0x15 || last.Sequence != 1 || !reflect.DeepEqual(last.Response, []byte{1, 99}) {
		t.Fatalf("other target changed: %+v", last)
	}
}

func TestIdentificationStoreNil(t *testing.T) {
	var store *Store
	if err := store.OnBusEvent(identEvent(0x08, []byte{1})); err != nil {
		t.Fatal(err)
	}
	if got := store.Snapshot(); !reflect.DeepEqual(got, Snapshot{}) {
		t.Fatalf("nil snapshot=%+v", got)
	}
}

func TestIdentificationStoreSessionAndCaptureClocks(t *testing.T) {
	base := time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		name           string
		first, capture time.Time
		session        string
	}{
		{"baseline", base, base.Add(time.Hour), "observer-20000102T030405.000000000Z"},
		{"first event only", base.Add(time.Second), base.Add(time.Hour), "observer-20000102T030406.000000000Z"},
		{"capture only", base, base.Add(2 * time.Hour), "observer-20000102T030405.000000000Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ticks := []time.Time{tc.first, tc.capture}
			store := NewStore(func() time.Time {
				if len(ticks) == 0 {
					t.Fatal("unexpected clock read")
				}
				result := ticks[0]
				ticks = ticks[1:]
				return result
			})
			_ = store.OnBusEvent(protocol.BusEvent{Kind: protocol.BusEventTX})
			_ = store.OnBusEvent(identEvent(0x08, []byte{1}))
			for i := 0; i < 2; i++ {
				snap := store.Snapshot()
				if len(snap.Exchanges) != 1 {
					t.Fatalf("exchanges=%d", len(snap.Exchanges))
				}
				if snap.SessionID != tc.session || snap.Exchanges[0].SessionID != tc.session {
					t.Fatalf("session=%q/%q", snap.SessionID, snap.Exchanges[0].SessionID)
				}
				if !snap.Exchanges[0].CapturedAt.Equal(tc.capture) {
					t.Fatalf("capture=%v want %v", snap.Exchanges[0].CapturedAt, tc.capture)
				}
			}
		})
	}
}
