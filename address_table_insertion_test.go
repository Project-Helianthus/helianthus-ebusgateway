// RED tests for cruise plan address-table-registry-w19-26 M0B/gateway — references M3/M4 + M2A + M1 + M2 symbols that intentionally don't exist yet.
package ebusgateway

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
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
	// same DeviceEntry in the wrapped registry.
	entryA, okA := table.reg.Lookup(0x10)
	entryB, okB := table.reg.Lookup(0x15)
	if !okA || !okB {
		t.Fatalf("registry.Lookup ok values: 0x10=%v 0x15=%v; want both true", okA, okB)
	}
	if entryA.Address() != entryB.Address() {
		t.Fatalf("canonical pair 0x10 ↔ 0x15 not aliased: 0x10 entry primary=0x%02X, 0x15 entry primary=0x%02X; want same primary", entryA.Address(), entryB.Address())
	}
}

// TestCanonicalAliasing_NonCanonicalAddresses_NotAliased asserts that
// non-canonical companions (e.g. real but non-table addresses 0x26 / 0xEC
// for VR_71 / SOL00) are NOT aliased even if they appear in the registry
// alongside other addresses. Aliasing only fires for the 25 canonical
// pairs from sourceAddressTableV1.
func TestCanonicalAliasing_NonCanonicalAddresses_NotAliased(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())
	base := time.Now().UTC()

	// 0x26 (VR_71 slave) is observed as target. Its hypothetical
	// "companion" via math 0x21 is not in the canonical table, and the
	// canonical-table-backed lookup correctly rejects it.
	first := atrPassiveTransactionEvent(base, 0x10, 0x26, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(first)
	requireATRSlot(t, table, 0x26)

	// 0xEC (SOL00 slave) is observed as target. Same situation.
	second := atrPassiveTransactionEvent(base.Add(time.Second), 0x10, 0xEC, protocol.SymbolAck)
	inserter.OnPassiveClassifiedEvent(second)
	requireATRSlot(t, table, 0xEC)

	// 0x26 and 0xEC must remain separate device entries.
	entryA, okA := table.reg.Lookup(0x26)
	entryB, okB := table.reg.Lookup(0xEC)
	if !okA || !okB {
		t.Fatalf("registry.Lookup ok: 0x26=%v 0xEC=%v; want both true", okA, okB)
	}
	if entryA.Address() == entryB.Address() {
		t.Fatalf("non-canonical addresses 0x26 and 0xEC unexpectedly aliased to primary 0x%02X", entryA.Address())
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
