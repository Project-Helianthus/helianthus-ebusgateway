package ebusgateway

import (
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func identEvent(target byte, response []byte) protocol.BusEvent {
	return protocol.BusEvent{
		Kind:        protocol.BusEventAttemptComplete,
		HasRequest:  true,
		HasResponse: true,
		Request:     protocol.Frame{Source: 0x71, Target: target, Primary: 0x07, Secondary: 0x04},
		Response:    protocol.Frame{Data: response},
	}
}

func TestIdentificationEvidenceStoreRetainsCompleteExchangeBytes(t *testing.T) {
	clock := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	store := NewIdentificationEvidenceStore(func() time.Time { clock = clock.Add(time.Second); return clock })
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

func TestIdentificationEvidenceStoreFiltersAndBounds(t *testing.T) {
	store := NewIdentificationEvidenceStore(nil)
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
	for i := 0; i < IdentificationEvidenceRingPerAddress+3; i++ {
		_ = store.OnBusEvent(identEvent(0x15, []byte{byte(i)}))
	}
	snap := store.Snapshot()
	if len(snap.Exchanges) != IdentificationEvidenceRingPerAddress {
		t.Fatalf("ring = %d, want %d", len(snap.Exchanges), IdentificationEvidenceRingPerAddress)
	}
	if snap.Exchanges[0].Sequence != 4 {
		t.Fatalf("oldest retained sequence = %d, want 4", snap.Exchanges[0].Sequence)
	}
}

func TestIdentificationEvidenceSessionDerivedFromFirstEvent(t *testing.T) {
	first := time.Date(2026, 10, 14, 9, 0, 0, 123, time.UTC)
	ticks := []time.Time{first, first.Add(time.Hour), first.Add(2 * time.Hour)}
	store := NewIdentificationEvidenceStore(func() time.Time { t := ticks[0]; ticks = ticks[1:]; return t })
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

func TestIdentificationEvidenceConcurrentReadWrite(t *testing.T) {
	store := NewIdentificationEvidenceStore(nil)
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				_ = store.OnBusEvent(identEvent(byte(0x08+w), []byte{byte(i), 2, 3}))
			}
		}(w)
	}
	for r := 0; r < 200; r++ {
		snap := store.Snapshot()
		for _, ex := range snap.Exchanges {
			if len(ex.Request) != 5 || ex.Response[0] != 3 {
				t.Errorf("torn exchange: %+v", ex)
			}
			if len(ex.Response) > 0 {
				ex.Response[0] = 0xFF // mutating a snapshot must not affect the store
			}
		}
	}
	close(done)
	wg.Wait()
	for _, ex := range store.Snapshot().Exchanges {
		if ex.Response[0] != 3 {
			t.Fatalf("snapshot mutation leaked into the store: %+v", ex)
		}
	}
}
