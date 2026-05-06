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
