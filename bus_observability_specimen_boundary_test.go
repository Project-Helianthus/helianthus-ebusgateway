package ebusgateway

import (
	"reflect"
	"testing"
	"time"
)

func TestProtocolSpecimenBoundaryProjection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	store := NewBusObservabilityStore(cfg)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close observability store: %v", err)
		}
	})
	base := time.Unix(1, 0).UTC()
	store.now = func() time.Time { return base.Add(time.Second) }
	store.passive.state = "available"
	store.passive.startupWindowClosed = true
	event := observabilityPassiveTransactionEvent(base, 1, 2, 7, 4)
	store.OnPassiveClassifiedEvent(event)
	duplicate := observabilityPassiveTransactionEvent(base.Add(time.Second), 1, 2, 7, 4)
	store.OnPassiveClassifiedEvent(duplicate)
	event.Request.Data[0] = 9
	event.Response.Data[0] = 10
	want := []ProtocolSpecimenExport{{Family: "0704", Source: 1, Target: 2, FrameType: "initiator_target", RequestHex: "01", ResponseHex: "02", RequestLen: 7, ResponseLen: 3, Outcome: "success", FirstSeenAt: base, LastSeenAt: base.Add(time.Second), Count: 2}}
	got := store.ProtocolSpecimens("")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("projection = %+v, want %+v", got, want)
	}
	got[0].RequestHex = "changed"
	if !reflect.DeepEqual(store.ProtocolSpecimens("0704"), want) {
		t.Fatal("export mutated retained state")
	}
	if len(store.ProtocolSpecimens("synthetic-absent")) != 0 {
		t.Fatal("family filter ignored")
	}
	summary := store.Snapshot().Summary
	if summary.SpecimenFamilies != 1 || summary.SpecimenCount != 1 {
		t.Fatalf("summary counts = %d/%d", summary.SpecimenFamilies, summary.SpecimenCount)
	}
	// Implemented families and request-less events remain ineligible at the facade.
	for _, secondary := range []byte{9, 0x16, 0x24, 0x55} {
		store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base, 1, 2, 0xb5, secondary))
	}
	missing := observabilityPassiveTransactionEvent(base, 1, 2, 7, 5)
	missing.HasRequest = false
	store.OnPassiveClassifiedEvent(missing)
	if !reflect.DeepEqual(store.ProtocolSpecimens(""), want) {
		t.Fatal("ineligible event admitted")
	}
}

func TestProtocolSpecimenBoundaryCapacities(t *testing.T) {
	store := NewBusObservabilityStore(DefaultConfig())
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close observability store: %v", err)
		}
	})
	base := time.Unix(1, 0).UTC()
	for i := 0; i < 33; i++ {
		event := observabilityPassiveTransactionEvent(base, 1, 2, 7, 4)
		event.Request.Data = []byte{byte(i)}
		store.pushSpecimenLocked("synthetic-a", event, "synthetic", "Unknown", base)
	}
	got := store.ProtocolSpecimens("synthetic-a")
	if len(got) != 32 || got[0].RequestHex != "01" || got[31].RequestHex != "20" {
		t.Fatalf("eviction/export order = %+v", got)
	}
	for i := 0; i < 64; i++ {
		event := observabilityPassiveTransactionEvent(base, 1, 2, 7, 4)
		store.pushSpecimenLocked(string(rune('a'+i)), event, "synthetic", "Unknown", base)
	}
	summary := store.Snapshot().Summary
	if summary.SpecimenFamilies != 64 || summary.SpecimenCount != 95 {
		t.Fatalf("bounded summary = %d/%d, want 64/95", summary.SpecimenFamilies, summary.SpecimenCount)
	}
	if len(store.ProtocolSpecimens(string(rune('a'+63)))) != 0 {
		t.Fatal("65th family admitted")
	}
}
