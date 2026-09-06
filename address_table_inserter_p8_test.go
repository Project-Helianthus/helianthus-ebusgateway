package ebusgateway

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// TestATRInserter_P8_PassiveInsertStampsPassiveObservedLabel asserts
// that a passive insertion via the AddressTableInserter labels the
// underlying registry AddressSlot with
// DiscoverySourcePassiveObserved/VerificationStateCorroborated — NOT
// the ActiveConfirmed/IdentityConfirmed labels Register stamps.
//
// This is the central P8 contract on the gateway side: pre-P8 the
// inserter called `Register + MarkSlotPassiveObserved` and the
// monotonic ladder made the second call a no-op, so passively-
// observed slots were misreported as `active_confirmed` in operator
// surfaces. After the switch to RegisterPassiveObserved, the slot
// stamps should reflect the actual provenance.
func TestATRInserter_P8_PassiveInsertStampsPassiveObservedLabel(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	table := NewAddressTable(reg)
	inserter := NewAddressTableInserter(table, DefaultConfig())

	event := atrPassiveTransactionEvent(time.Now().UTC(), 0x10, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	regSlot, ok := reg.LookupSlot(0x99)
	if !ok || regSlot == nil {
		t.Fatalf("registry.LookupSlot(0x99) ok=%v slot=%v", ok, regSlot)
	}
	if regSlot.DiscoverySource != registry.DiscoverySourcePassiveObserved {
		t.Errorf("regSlot.DiscoverySource = %v; want PassiveObserved (P8 contract — passive insert must NOT stamp ActiveConfirmed)", regSlot.DiscoverySource)
	}
	if regSlot.VerificationState != registry.VerificationStateCorroborated {
		t.Errorf("regSlot.VerificationState = %v; want Corroborated", regSlot.VerificationState)
	}
}

// TestATRInserter_P8_SourceInsertStampsPassiveObservedLabel covers
// the request-source insert path (PR #564 / TestFirstObservation_
// SourceInserted): when the request initiator is admitted via
// passive observation, its slot should also wear the
// passive-observed label, not active-confirmed.
func TestATRInserter_P8_SourceInsertStampsPassiveObservedLabel(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	table := NewAddressTable(reg)
	inserter := NewAddressTableInserter(table, DefaultConfig())

	event := atrPassiveTransactionEvent(time.Now().UTC(), 0xF1, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	srcSlot, ok := reg.LookupSlot(0xF1)
	if !ok || srcSlot == nil {
		t.Fatalf("registry.LookupSlot(0xF1) ok=%v slot=%v (initiator must also insert)", ok, srcSlot)
	}
	if srcSlot.DiscoverySource != registry.DiscoverySourcePassiveObserved {
		t.Errorf("srcSlot.DiscoverySource = %v; want PassiveObserved", srcSlot.DiscoverySource)
	}
}

// TestATRInserter_P8_AddressTableProjectsLiveRegistryLabel covers the
// cache-coherence half of P8 (Codex P8 gateway review MINOR FINDING_1):
// after a passive insertion lands the slot at "passive_observed", a
// subsequent registry mutation (e.g. directed scan advancing verification
// to identity_confirmed) MUST be visible through AddressTable.Lookup
// despite the cached AddressSlot in t.slots. Pre-fix the cached strings
// were captured at insert time and went stale; post-fix Lookup
// reprojects from slot.RegistrySlot's live enum.
func TestATRInserter_P8_AddressTableProjectsLiveRegistryLabel(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	table := NewAddressTable(reg)
	inserter := NewAddressTableInserter(table, DefaultConfig())

	// Passive insertion lands the slot at passive_observed.
	event := atrPassiveTransactionEvent(time.Now().UTC(), 0x10, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)
	pre, ok := table.Lookup(0x99)
	if !ok || pre == nil {
		t.Fatalf("Lookup(0x99) ok=%v slot=%v", ok, pre)
	}
	if pre.DiscoverySource != "passive_observed" {
		t.Fatalf("pre-condition: DiscoverySource = %q; want passive_observed", pre.DiscoverySource)
	}

	// Now active evidence advances verification while retaining passive origin.
	reg.Register(registry.DeviceInfo{
		Address:      0x99,
		Manufacturer: "Vaillant",
		DeviceID:     "TEST",
		SerialNumber: "SN-99",
	})

	// AddressTable.Lookup MUST reflect the verification advance despite the cached
	// AddressSlot still being in t.slots from the passive insertion.
	post, ok := table.Lookup(0x99)
	if !ok || post == nil {
		t.Fatalf("post-upgrade Lookup(0x99) ok=%v slot=%v", ok, post)
	}
	if post.DiscoverySource != "passive_observed" {
		t.Errorf("post-confirmation DiscoverySource = %q; want retained passive_observed", post.DiscoverySource)
	}
	if post.VerificationState != "identity_confirmed" {
		t.Errorf("post-upgrade VerificationState = %q; want identity_confirmed", post.VerificationState)
	}
}

// TestATRInserter_P8_AddressTableSlotMirrorsRegistryLabel ties the
// AddressTable's projection (`slots[addr].DiscoverySource` /
// `.VerificationState`) to the registry's underlying slot. The
// address_table.go projection (lines 158-164 area) maps the
// registry's DiscoverySource enum to a string label; we verify the
// post-P8 projection emits "passive_observed" for a passive insert.
func TestATRInserter_P8_AddressTableSlotMirrorsRegistryLabel(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	table := NewAddressTable(reg)
	inserter := NewAddressTableInserter(table, DefaultConfig())

	event := atrPassiveTransactionEvent(time.Now().UTC(), 0x10, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	slot, ok := table.Lookup(0x99)
	if !ok || slot == nil {
		t.Fatalf("AddressTable.Lookup(0x99) ok=%v slot=%v", ok, slot)
	}
	if slot.DiscoverySource != "passive_observed" {
		t.Errorf("AddressTable slot.DiscoverySource = %q; want \"passive_observed\"", slot.DiscoverySource)
	}
	if slot.RegistrySlot == nil {
		t.Errorf("AddressTable slot.RegistrySlot is nil; want pointer to registry slot (cached projection)")
	}
}
