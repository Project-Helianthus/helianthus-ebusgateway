// RED tests for cruise plan address-table-registry-w19-26 M0B/gateway — references M3/M4 + M2A + M1 + M2 symbols that intentionally don't exist yet.
package ebusgateway

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func TestFirstObservation_PositiveACKOnly(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())

	event := atrPassiveTransactionEvent(time.Now().UTC(), 0x10, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	slot := requireATRSlot(t, table, 0x99)
	if slot.VerificationState != "corroborated_pending" {
		t.Fatalf("slot[0x99].VerificationState = %q; want corroborated_pending", slot.VerificationState)
	}
}

func TestFirstObservation_NoNACKInsertion(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())

	event := atrPassiveTransactionEvent(time.Now().UTC(), 0x10, 0x99, protocol.SymbolNack)
	inserter.OnPassiveClassifiedEvent(event)

	assertATRSlotAbsent(t, table, 0x99)
}

func TestFirstObservation_SelfSourceExcluded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AdmittedSource = func() byte { return 0x7F }
	table, inserter := newATRInsertionHarness(t, cfg)

	admittedSource := cfg.AdmittedSource()
	event := atrPassiveTransactionEvent(time.Now().UTC(), admittedSource, 0x88, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	assertATRSlotAbsent(t, table, admittedSource)
}

func TestCompanionInsert_RequiresCorroboration(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := time.Now().UTC()

	first := atrPassiveTransactionEvent(base, 0xF1, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(first)
	if got := inserter.observationsByAddr[0xF1].PositiveACKCount; got != 1 {
		t.Fatalf("observationsByAddr[0xF1].PositiveACKCount after first observation = %d; want 1", got)
	}
	assertATRSlotAbsent(t, table, 0xF6)

	second := atrPassiveTransactionEvent(base.Add(2*time.Second), 0xF1, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(second)
	if got := inserter.observationsByAddr[0xF1].PositiveACKCount; got != 2 {
		t.Fatalf("observationsByAddr[0xF1].PositiveACKCount after second observation = %d; want 2", got)
	}
	requireATRSlot(t, table, 0xF6)
}

// TestFirstObservation_SourceInserted asserts that on a positive ACK the
// initiator address (request src) is inserted as an "initiator" slot — not
// just the target. Per architecture/atr/03-ack-nack-insertion-rules.md
// "Address Eligibility":
//
//	"request `src` MAY create a slot if it is not the gateway's own admitted source"
//
// PR #564 implemented dst insertion + companion-after-corroboration but
// missed src insertion, so initiator addresses like NETX3 0xF1 never
// landed in the registry passively even though their traffic was observed.
func TestFirstObservation_SourceInserted(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())

	event := atrPassiveTransactionEvent(time.Now().UTC(), 0xF1, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	srcSlot := requireATRSlot(t, table, 0xF1)
	if srcSlot.Role != "initiator" {
		t.Fatalf("slot[0xF1].Role = %q; want initiator", srcSlot.Role)
	}
	if srcSlot.DiscoverySource != "passive_observed" {
		t.Fatalf("slot[0xF1].DiscoverySource = %q; want passive_observed", srcSlot.DiscoverySource)
	}
	if srcSlot.VerificationState != "corroborated_pending" {
		t.Fatalf("slot[0xF1].VerificationState = %q; want corroborated_pending", srcSlot.VerificationState)
	}

	dstSlot := requireATRSlot(t, table, 0x99)
	if dstSlot.Role != "target" {
		t.Fatalf("slot[0x99].Role = %q; want target", dstSlot.Role)
	}
}

// TestCanonicalAliasing_SourceCompanionPair_AfterSecondCorroboration asserts
// that when both a canonical source AND its canonical companion land in the
// registry, the inserter aliases them as a single device — so MCP/GraphQL
// queries return one device with two addresses, not two unrelated entries.
//
// Per architecture/ebus_standard/12-source-address-table.md, 0x10 ↔ 0x15 is
// the canonical Heating regulator pair. After the inserter sees:
//  1. positive ACK from 0x10 → some-target  (inserts 0x10 as initiator)
//  2. positive ACK from some-source → 0x15  (inserts 0x15 as target — but
//     the companion-pair gate requires 2 observations of source for
//     companion(source) insertion; here 0x15 is inserted as direct dst)
//
// At the moment both 0x10 and 0x15 are present in the table, the inserter
// MUST call AliasAddresses(0x10, 0x15) so registry.Lookup(0x10) and
// registry.Lookup(0x15) return the same DeviceEntry.
func TestCanonicalAliasing_SourceCompanionPair_AfterBothObserved(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := time.Now().UTC()

	// Step 1: observe 0x10 → 0x33-style-target (positive ACK) → inserts
	// 0x10 as initiator + the target as target.
	first := atrPassiveTransactionEvent(base, 0x10, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(first)
	requireATRSlot(t, table, 0x10)

	// Step 2: observe a different source emitting to 0x15 (canonical
	// companion of 0x10) with positive ACK → inserts 0x15 as target.
	// Now both 0x10 and 0x15 are present — the inserter MUST alias them.
	second := atrPassiveTransactionEvent(base.Add(time.Second), 0x71, 0x15, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(second)
	requireATRSlot(t, table, 0x15)

	// Step 3: assert canonical aliasing — both addresses resolve to the
	// same DeviceEntry in the wrapped registry, and the entry exposes
	// both addresses via Addresses().
	entryA, okA := table.reg.Lookup(0x10)
	entryB, okB := table.reg.Lookup(0x15)
	if !okA || !okB {
		t.Fatalf("registry.Lookup ok values: 0x10=%v 0x15=%v; want both true", okA, okB)
	}
	if entryA.PrimaryDisplayAddress() != entryB.PrimaryDisplayAddress() {
		t.Fatalf("canonical pair 0x10 ↔ 0x15 not aliased: 0x10 entry primary=0x%02X, 0x15 entry primary=0x%02X; want same primary", entryA.PrimaryDisplayAddress(), entryB.PrimaryDisplayAddress())
	}
	addrs := entryA.Addresses()
	hasMaster, hasCompanion := false, false
	for _, a := range addrs {
		if a == 0x10 {
			hasMaster = true
		}
		if a == 0x15 {
			hasCompanion = true
		}
	}
	if !hasMaster || !hasCompanion {
		t.Fatalf("DeviceEntry.Addresses() = %v; want both 0x10 and 0x15", addrs)
	}
}

// TestCanonicalAliasing_FFAndZeroFour_BothDirections asserts the wrap-pair
// aliasing for 0xFF ↔ 0x04. NETX3 publishes a target at 0x04 whose canonical
// source is 0xFF (per architecture/ebus_standard/12-source-address-table.md
// row 25, with byte-modulo arithmetic 0xFF + 0x05 = 0x04). Tests both
// orderings: 0xFF first then 0x04 (forward) and 0x04 first then 0xFF
// (reverse).
func TestCanonicalAliasing_FFAndZeroFour_BothDirections(t *testing.T) {
	t.Parallel()

	t.Run("FF_then_04", func(t *testing.T) {
		table, inserter := newATRInsertionHarness(t, DefaultConfig())
		base := time.Now().UTC()

		// 0xFF as src (frame-start, not NACK byte) → inserts 0xFF.
		ff := atrPassiveTransactionEvent(base, 0xFF, 0x99, protocol.SymbolAck)
		inserter.OnPassiveClassifiedEvent(ff)
		requireATRSlot(t, table, 0xFF)

		// Some-source → 0x04 with positive ACK → inserts 0x04.
		zero4 := atrPassiveTransactionEvent(base.Add(time.Second), 0x10, 0x04, protocol.SymbolAck)
		inserter.OnPassiveClassifiedEvent(zero4)
		requireATRSlot(t, table, 0x04)

		entryFF, okFF := table.reg.Lookup(0xFF)
		entry04, ok04 := table.reg.Lookup(0x04)
		if !okFF || !ok04 {
			t.Fatalf("registry.Lookup ok: 0xFF=%v 0x04=%v", okFF, ok04)
		}
		if entryFF.PrimaryDisplayAddress() != entry04.PrimaryDisplayAddress() {
			t.Fatalf("0xFF ↔ 0x04 wrap-pair not aliased: 0xFF primary=0x%02X, 0x04 primary=0x%02X", entryFF.PrimaryDisplayAddress(), entry04.PrimaryDisplayAddress())
		}
	})

	t.Run("04_then_FF", func(t *testing.T) {
		table, inserter := newATRInsertionHarness(t, DefaultConfig())
		base := time.Now().UTC()

		// Reverse ordering: 0x04 inserted first via dst observation.
		zero4 := atrPassiveTransactionEvent(base, 0x10, 0x04, protocol.SymbolAck)
		inserter.OnPassiveClassifiedEvent(zero4)
		requireATRSlot(t, table, 0x04)

		ff := atrPassiveTransactionEvent(base.Add(time.Second), 0xFF, 0x99, protocol.SymbolAck)
		inserter.OnPassiveClassifiedEvent(ff)
		requireATRSlot(t, table, 0xFF)

		entryFF, _ := table.reg.Lookup(0xFF)
		entry04, _ := table.reg.Lookup(0x04)
		if entryFF.PrimaryDisplayAddress() != entry04.PrimaryDisplayAddress() {
			t.Fatalf("0xFF ↔ 0x04 wrap-pair (reverse order) not aliased: 0xFF primary=0x%02X, 0x04 primary=0x%02X", entryFF.PrimaryDisplayAddress(), entry04.PrimaryDisplayAddress())
		}
	})
}

// TestCanonicalAliasing_NonCanonicalAddresses_NotAliased asserts that
// non-canonical companions (e.g. real but non-table addresses 0x26 / 0xEC
// for VR_71 / SOL00) are NOT aliased even if they appear in the registry
// alongside other addresses. Aliasing only fires for the 25 canonical
// pairs from sourceAddressTableV1.
func TestCanonicalAliasing_NonCanonicalAddresses_NotAliased(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := time.Now().UTC()

	// 0x26 (VR_71 target-only address) is observed as target. Its hypothetical
	// "companion" via math 0x21 is not in the canonical table, and the
	// canonical-table-backed lookup correctly rejects it.
	first := atrPassiveTransactionEvent(base, 0x10, 0x26, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(first)
	requireATRSlot(t, table, 0x26)

	// 0xEC (SOL00 target-only address) is observed as target. Same situation.
	second := atrPassiveTransactionEvent(base.Add(time.Second), 0x10, 0xEC, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(second)
	requireATRSlot(t, table, 0xEC)

	// 0x26 and 0xEC must remain separate device entries.
	entryA, okA := table.reg.Lookup(0x26)
	entryB, okB := table.reg.Lookup(0xEC)
	if !okA || !okB {
		t.Fatalf("registry.Lookup ok: 0x26=%v 0xEC=%v; want both true", okA, okB)
	}
	if entryA.PrimaryDisplayAddress() == entryB.PrimaryDisplayAddress() {
		t.Fatalf("non-canonical addresses 0x26 and 0xEC unexpectedly aliased to primary 0x%02X", entryA.PrimaryDisplayAddress())
	}
}

// TestAddressSlot_TierAndFreeUse_PopulatedFromCanonicalTable asserts that
// when a canonical source/companion address lands in the AddressTable, its
// PriorityTier and FreeUse fields are populated from the docs-owned eBUS
// standard table (sourceAddressTableV1) — operators can then see at a
// glance whether an observed address is preallocated (e.g. 0xF1 p1
// "Heating regulator") or free-use (e.g. 0x7F p4 "free-use Burner controller 8
// recommendation").
func TestAddressSlot_TierAndFreeUse_PopulatedFromCanonicalTable(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := time.Now().UTC()

	// 0xF1 (Heating regulator p1, NOT free-use)
	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base, 0xF1, 0x99, protocol.SymbolAck))
	slot, _ := table.Lookup(0xF1)
	if slot == nil {
		t.Fatalf("Lookup(0xF1) = nil after insertion")
	}
	if string(slot.PriorityTier) != "p1" {
		t.Fatalf("slot[0xF1].PriorityTier = %q; want p1", slot.PriorityTier)
	}
	if slot.FreeUse {
		t.Fatalf("slot[0xF1].FreeUse = true; want false (Heating regulator preallocated)")
	}

	// 0x7F (free-use Burner controller 8 recommendation, p4)
	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(base.Add(time.Second), 0x7F, 0x88, protocol.SymbolAck))
	slot, _ = table.Lookup(0x7F)
	if slot == nil {
		t.Fatalf("Lookup(0x7F) = nil after insertion")
	}
	if string(slot.PriorityTier) != "p4" {
		t.Fatalf("slot[0x7F].PriorityTier = %q; want p4", slot.PriorityTier)
	}
	if !slot.FreeUse {
		t.Fatalf("slot[0x7F].FreeUse = false; want true (free-use Burner controller 8 recommendation)")
	}
}

// TestAddressSlot_CompanionInheritsTierFromSourceRow asserts that when a
// canonical companion (e.g. 0x15, 0xF6, 0x04, 0x84) is inserted as target
// — without its source ever being observed — the slot's PriorityTier and
// FreeUse fields are populated from the row's source side. Both halves of
// a canonical pair share the same tier/free-use because the eBUS standard
// table row defines a single class for the pair.
func TestAddressSlot_CompanionInheritsTierFromSourceRow(t *testing.T) {
	cases := []struct {
		addr     byte
		wantTier string
		wantFree bool
	}{
		{0x15, "p0", false}, // companion of 0x10 (Heating regulator p0)
		{0xF6, "p1", false}, // companion of 0xF1 (Heating regulator p1)
		{0x04, "p4", false}, // companion of 0xFF (PC p4) — wrap pair
		{0x84, "p4", true},  // companion of 0x7F (free-use Burner controller 8 p4)
		{0x38, "p2", false}, // companion of 0x33 (Burner controller 3 p2)
	}
	for _, tc := range cases {
		tc := tc
		t.Run("companion_0x"+itoaHex(tc.addr), func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.AdmittedSource = func() byte { return 0x71 }
			table, inserter := newATRInsertionHarness(t, cfg)
			// Insert via 0x10 → companion as target with positive ACK.
			inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(time.Now().UTC(), 0x10, tc.addr, protocol.SymbolAck))
			slot, _ := table.Lookup(tc.addr)
			if slot == nil {
				t.Fatalf("Lookup(0x%02X) = nil", tc.addr)
			}
			if string(slot.PriorityTier) != tc.wantTier {
				t.Fatalf("slot[0x%02X].PriorityTier = %q; want %q", tc.addr, slot.PriorityTier, tc.wantTier)
			}
			if slot.FreeUse != tc.wantFree {
				t.Fatalf("slot[0x%02X].FreeUse = %v; want %v", tc.addr, slot.FreeUse, tc.wantFree)
			}
		})
	}
}

func itoaHex(b byte) string {
	const hex = "0123456789ABCDEF"
	return string([]byte{hex[b>>4], hex[b&0x0F]})
}

// TestAddressSlot_NonCanonical_TierEmpty asserts that addresses NOT in the
// canonical table (e.g. 0x26 VR_71 target-only) get empty PriorityTier and
// FreeUse=false, since they have no canonical row.
func TestAddressSlot_NonCanonical_TierEmpty(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AdmittedSource = func() byte { return 0x7F }
	table, inserter := newATRInsertionHarness(t, cfg)

	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(time.Now().UTC(), 0x10, 0x26, protocol.SymbolAck))
	slot, _ := table.Lookup(0x26)
	if slot == nil {
		t.Fatalf("Lookup(0x26) = nil after insertion")
	}
	if string(slot.PriorityTier) != "" {
		t.Fatalf("slot[0x26].PriorityTier = %q; want empty (non-canonical)", slot.PriorityTier)
	}
	if slot.FreeUse {
		t.Fatalf("slot[0x26].FreeUse = true; want false (non-canonical)")
	}
}

// TestCanonicalAliasing_BothPreRegistered_AliasOnObservation asserts that
// when both halves of a canonical pair were registered BEFORE the inserter
// runs (e.g. via startup active scan or static seed), a subsequent passive
// observation of either half still triggers the alias merge. Without this,
// devices that the active scan picks up as separate entries (the common
// case for 0x10/0x15 BASV2 controller pair) would never become aliased.
func TestCanonicalAliasing_BothPreRegistered_AliasOnObservation(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())

	// Pre-register both 0x10 and 0x15 as if startup active scan had
	// discovered them as separate entries. Force them apart by giving
	// each a distinct identity so registry.Register doesn't auto-merge.
	table.reg.Register(registry.DeviceInfo{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC700", SerialNumber: "SN-VRC"})
	table.reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2", SerialNumber: "SN-BASV2"})

	entry10Pre, _ := table.reg.Lookup(0x10)
	entry15Pre, _ := table.reg.Lookup(0x15)
	if entry10Pre.PrimaryDisplayAddress() == entry15Pre.PrimaryDisplayAddress() {
		t.Fatalf("test setup: 0x10 and 0x15 unexpectedly share primary 0x%02X (Register auto-merged)", entry10Pre.PrimaryDisplayAddress())
	}

	// Observe passive traffic with 0x10 as src. Inserter sees 0x10 in
	// table.Lookup → existing → MUST still attempt alias of 0x10's
	// canonical companion 0x15.
	event := atrPassiveTransactionEvent(time.Now().UTC(), 0x10, 0x99, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(event)

	entry10, _ := table.reg.Lookup(0x10)
	entry15, _ := table.reg.Lookup(0x15)
	if entry10.PrimaryDisplayAddress() != entry15.PrimaryDisplayAddress() {
		t.Fatalf("pre-registered canonical pair 0x10 ↔ 0x15 not aliased after passive observation: 0x10 primary=0x%02X, 0x15 primary=0x%02X", entry10.PrimaryDisplayAddress(), entry15.PrimaryDisplayAddress())
	}
}

func TestFFDisambiguation_FrameStartVsACKPosition(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := time.Now().UTC()

	frameStartFF := atrPassiveTransactionEvent(base, 0xFF, 0x04, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(frameStartFF)
	before := requireATRSlot(t, table, 0xFF)

	nackPositionFF := atrPassiveTransactionEvent(base.Add(time.Second), 0x10, 0x99, protocol.SymbolNack)
	inserter.OnPassiveClassifiedEvent(nackPositionFF)
	after := requireATRSlot(t, table, 0xFF)

	if after.VerificationState != before.VerificationState {
		t.Fatalf("slot[0xFF].VerificationState changed after NACK-position 0xFF: got %q; want %q", after.VerificationState, before.VerificationState)
	}
}

func newATRInsertionHarness(t *testing.T, cfg Config) (*AddressTable, *AddressTableInserter) {
	t.Helper()

	table := NewAddressTable()
	inserter := NewAddressTableInserter(table, cfg)
	return table, inserter
}

func atrPassiveTransactionEvent(observedAt time.Time, source, target, ack byte) PassiveClassifiedEvent {
	event := observabilityPassiveTransactionEvent(observedAt, source, target, 0xB5, 0x24)
	event.ACKCorrelation = PassiveACKCorrelation{
		Byte:            ack,
		Position:        PassiveACKPositionRequestACK,
		CompleteRequest: true,
		Correlator:      PassiveACKCorrelatorM2A,
	}
	return event
}

func requireATRSlot(t *testing.T, table *AddressTable, address byte) *AddressSlot {
	t.Helper()

	slot, ok := table.Lookup(address)
	if !ok {
		t.Fatalf("AddressTable.Lookup(0x%02X) ok = false; want true", address)
	}
	if slot == nil {
		t.Fatalf("AddressTable.Lookup(0x%02X) slot = nil; want AddressSlot", address)
	}
	return slot
}

func assertATRSlotAbsent(t *testing.T, table *AddressTable, address byte) {
	t.Helper()

	slot, ok := table.Lookup(address)
	if ok || slot != nil {
		t.Fatalf("AddressTable.Lookup(0x%02X) = (%+v, %v); want (nil, false)", address, slot, ok)
	}
}
