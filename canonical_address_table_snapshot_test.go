package ebusgateway

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// TestCanonicalAddressTableSnapshot_AllRowsPresent asserts the snapshot
// returns exactly 50 entries (25 canonical sources + 25 canonical
// companions from sourceAddressTableV1).
func TestCanonicalAddressTableSnapshot_AllRowsPresent(t *testing.T) {
	table := NewAddressTable()
	snap := table.CanonicalAddressTableSnapshot()
	if got, want := len(snap), 50; got != want {
		t.Fatalf("CanonicalAddressTableSnapshot returned %d entries; want %d (25 source + 25 companion)", got, want)
	}

	// Confirm every canonical source and companion appears exactly once.
	seen := make(map[byte]int, 50)
	for _, view := range snap {
		seen[view.Address]++
	}
	for _, addr := range []byte{0x00, 0x05, 0x10, 0x15, 0xF1, 0xF6, 0xFF, 0x04, 0x33, 0x38, 0x7F, 0x84} {
		if seen[addr] != 1 {
			t.Errorf("address 0x%02X appears %d times; want exactly 1", addr, seen[addr])
		}
	}
}

// TestCanonicalAddressTableSnapshot_NeverObservedDefault asserts an empty
// AddressTable yields all 50 entries with Observed=false +
// DiscoverySource="never_observed".
func TestCanonicalAddressTableSnapshot_NeverObservedDefault(t *testing.T) {
	table := NewAddressTable()
	snap := table.CanonicalAddressTableSnapshot()
	for _, view := range snap {
		if view.Observed {
			t.Errorf("view[0x%02X].Observed = true; want false on empty table", view.Address)
		}
		if view.DiscoverySource != "never_observed" {
			t.Errorf("view[0x%02X].DiscoverySource = %q; want never_observed", view.Address, view.DiscoverySource)
		}
		if view.VerificationState != "never_observed" {
			t.Errorf("view[0x%02X].VerificationState = %q; want never_observed", view.Address, view.VerificationState)
		}
	}
}

// TestCanonicalAddressTableSnapshot_ObservedReflected asserts that after
// the inserter promotes a canonical address (e.g. 0xF1 source), the
// snapshot row for 0xF1 has Observed=true with DiscoverySource set.
// Companions and other addresses remain "never_observed".
func TestCanonicalAddressTableSnapshot_ObservedReflected(t *testing.T) {
	table, inserter := newATRInsertionHarness(t, DefaultConfig())

	// Observe 0xF1 → 0x99 with positive ACK → inserts 0xF1 as initiator.
	inserter.OnPassiveClassifiedEvent(atrPassiveTransactionEvent(time.Now().UTC(), 0xF1, 0x99, protocol.SymbolAck))

	snap := table.CanonicalAddressTableSnapshot()
	var f1View, f6View *CanonicalAddressView
	for i := range snap {
		if snap[i].Address == 0xF1 {
			f1View = &snap[i]
		}
		if snap[i].Address == 0xF6 {
			f6View = &snap[i]
		}
	}
	if f1View == nil || f6View == nil {
		t.Fatalf("snapshot missing 0xF1 or 0xF6 view: 0xF1=%v 0xF6=%v", f1View, f6View)
	}

	if !f1View.Observed {
		t.Errorf("0xF1 view.Observed = false; want true after passive observation")
	}
	if f1View.DiscoverySource != "passive_observed" {
		t.Errorf("0xF1 view.DiscoverySource = %q; want passive_observed", f1View.DiscoverySource)
	}
	if string(f1View.PriorityTier) != "p1" {
		t.Errorf("0xF1 view.PriorityTier = %q; want p1", f1View.PriorityTier)
	}

	// 0xF6 was NOT directly observed and the companion-corroboration gate
	// hasn't fired (only one observation of 0xF1) — so 0xF6 stays
	// never_observed in this snapshot.
	if f6View.Observed {
		t.Errorf("0xF6 view.Observed = true; want false (companion-corroboration not yet satisfied)")
	}
}

// TestCanonicalAddressTableSnapshot_PeerAndRoleCorrect asserts each row
// surfaces the correct peer address and role label.
func TestCanonicalAddressTableSnapshot_PeerAndRoleCorrect(t *testing.T) {
	table := NewAddressTable()
	snap := table.CanonicalAddressTableSnapshot()

	cases := []struct {
		addr     byte
		wantRole string
		wantPeer byte
	}{
		{0x10, "initiator", 0x15},
		{0x15, "target", 0x10},
		{0xF1, "initiator", 0xF6},
		{0xF6, "target", 0xF1},
		{0xFF, "initiator", 0x04},
		{0x04, "target", 0xFF},
		{0x7F, "initiator", 0x84},
		{0x84, "target", 0x7F},
	}
	for _, tc := range cases {
		var found *CanonicalAddressView
		for i := range snap {
			if snap[i].Address == tc.addr {
				found = &snap[i]
				break
			}
		}
		if found == nil {
			t.Errorf("snapshot missing 0x%02X", tc.addr)
			continue
		}
		if found.Role != tc.wantRole {
			t.Errorf("0x%02X view.Role = %q; want %q", tc.addr, found.Role, tc.wantRole)
		}
		if found.PeerAddress != tc.wantPeer {
			t.Errorf("0x%02X view.PeerAddress = 0x%02X; want 0x%02X", tc.addr, found.PeerAddress, tc.wantPeer)
		}
	}
}
