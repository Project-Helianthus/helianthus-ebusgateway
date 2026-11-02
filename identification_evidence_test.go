package ebusgateway

import (
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/ebus/evidence/identification"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func identEvent(target byte, response []byte) protocol.BusEvent {
	return protocol.BusEvent{
		Kind: protocol.BusEventAttemptComplete, Outcome: protocol.BusOutcomeSuccess,
		HasRequest: true, HasResponse: true,
		Request:  protocol.Frame{Source: 0x71, Target: target, Primary: 0x07, Secondary: 0x04},
		Response: protocol.Frame{Data: response},
	}
}

func TestIdentificationFacadeDelegates(t *testing.T) {
	captured := time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC)
	native := identification.NewStore(func() time.Time { return captured })
	facade := &IdentificationEvidenceStore{store: native}
	if err := facade.OnBusEvent(identEvent(0x15, []byte{9})); err != nil {
		t.Fatal(err)
	}
	// A facade write must reach the exact native instance, independently of reads.
	want := IdentificationEvidenceSnapshot{SessionID: "observer-20000102T030405.000000006Z", Exchanges: []IdentificationExchange{{
		Address: 0x15, Sequence: 1, SessionID: "observer-20000102T030405.000000006Z", CapturedAt: captured,
		Request: []byte{0x71, 0x15, 0x07, 0x04, 0}, Response: []byte{1, 9},
	}}}
	if got := native.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("native capture = %+v, want %+v", got, want)
	}
	// A direct native write must be visible through the facade as well.
	if err := native.OnBusEvent(identEvent(0x08, []byte{7})); err != nil {
		t.Fatal(err)
	}
	want.Exchanges = append([]IdentificationExchange{{Address: 0x08, Sequence: 2, SessionID: want.SessionID, CapturedAt: captured,
		Request: []byte{0x71, 0x08, 0x07, 0x04, 0}, Response: []byte{1, 7}}}, want.Exchanges...)
	if got := facade.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("facade snapshot = %+v, want %+v", got, want)
	}
	if IdentificationEvidenceRingPerAddress != 8 {
		t.Fatal("retention contract changed")
	}
}

func TestIdentificationFacadeConstructionAndNil(t *testing.T) {
	var absent *IdentificationEvidenceStore
	for _, store := range []*IdentificationEvidenceStore{absent, {}} {
		if err := store.OnBusEvent(identEvent(0x08, []byte{1})); err != nil {
			t.Fatal(err)
		}
		if got := store.Snapshot(); !reflect.DeepEqual(got, IdentificationEvidenceSnapshot{}) {
			t.Fatalf("empty facade = %+v", got)
		}
	}
	captured := time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC)
	store := NewIdentificationEvidenceStore(func() time.Time { return captured })
	if store.store == nil {
		t.Fatal("constructor did not allocate native store")
	}
	if err := store.OnBusEvent(identEvent(0x08, []byte{1})); err != nil {
		t.Fatal(err)
	}
	got := store.Snapshot()
	if len(got.Exchanges) != 1 || got.SessionID != "observer-20000102T030405.000000000Z" || !got.Exchanges[0].CapturedAt.Equal(captured) {
		t.Fatalf("constructed facade = %+v", got)
	}
}
