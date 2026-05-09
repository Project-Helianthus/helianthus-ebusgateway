package ebusgateway

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// P9.4 — BackfillUnidentifiedAddresses uses IterateSnapshots.
//
// Pre-P9.4 the backfill iteration read entry.Manufacturer() /
// SerialNumber() / Addresses() on a live *deviceEntry pointer
// outside the registry's RLock. Backfill runs once after startup
// but the passive inserter can be writing concurrently (passive
// frames arriving from the bus). Race detector would flag the
// reads against concurrent Register / RegisterPassiveObserved
// writes.

// TestBackfillUnidentifiedAddresses_ProbesUnidentifiedOnly verifies
// the probe fn is called for entries that need identification
// (empty Manufacturer OR Vaillant + empty SerialNumber) and NOT
// for fully-identified Vaillant entries.
func TestBackfillUnidentifiedAddresses_ProbesUnidentifiedOnly(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x10}) // empty manufacturer → probe
	reg.Register(registry.DeviceInfo{
		Address:      0x20,
		Manufacturer: "Vaillant",
		// empty SerialNumber → probe
	})
	reg.Register(registry.DeviceInfo{
		Address:      0x30,
		Manufacturer: "Vaillant",
		SerialNumber: "SN-FULLY-IDENTIFIED", // → no probe
	})

	table := NewAddressTable(reg)
	inserter := NewAddressTableInserter(table, DefaultConfig())

	probed := make(map[byte]int)
	inserter.SetEnrichmentIdentityProbeFn(func(addr byte) {
		probed[addr]++
	})

	inserter.BackfillUnidentifiedAddresses()

	if probed[0x10] == 0 {
		t.Errorf("0x10 (empty manufacturer) was not probed; want >= 1 probe call")
	}
	if probed[0x20] == 0 {
		t.Errorf("0x20 (Vaillant + empty SerialNumber) was not probed; want >= 1 probe call")
	}
	if probed[0x30] != 0 {
		t.Errorf("0x30 (fully identified) was probed %d times; want 0", probed[0x30])
	}
}

// TestBackfillUnidentifiedAddresses_NilProbeFnIsNoop verifies the
// graceful early-return when no probe fn is wired (e.g. tests that
// don't enable enrichment).
func TestBackfillUnidentifiedAddresses_NilProbeFnIsNoop(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x10})

	table := NewAddressTable(reg)
	inserter := NewAddressTableInserter(table, DefaultConfig())
	// No SetEnrichmentIdentityProbeFn — should not panic.

	inserter.BackfillUnidentifiedAddresses()
}

// TestBackfillUnidentifiedAddresses_ProbesAllAliasesForMergedEntries
// verifies that aliased canonical pairs (e.g. 0x10↔0x15 NETX3) get
// probed at every face when the entry is unidentified. The snapshot's
// Addresses slice covers all faces — same semantic as the previous
// entry.Addresses() iteration.
func TestBackfillUnidentifiedAddresses_ProbesAllAliasesForMergedEntries(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x10, SerialNumber: "SN-X"})
	reg.Register(registry.DeviceInfo{Address: 0x15, SerialNumber: "SN-Y"})
	if err := reg.AliasAddresses(0x10, 0x15); err != nil {
		t.Fatalf("AliasAddresses error = %v", err)
	}

	table := NewAddressTable(reg)
	inserter := NewAddressTableInserter(table, DefaultConfig())

	probed := make(map[byte]int)
	inserter.SetEnrichmentIdentityProbeFn(func(addr byte) {
		probed[addr]++
	})

	inserter.BackfillUnidentifiedAddresses()

	// Both aliases should have been probed (entry has empty
	// Manufacturer because Register was called without one).
	if probed[0x10] == 0 {
		t.Errorf("0x10 alias was not probed")
	}
	if probed[0x15] == 0 {
		t.Errorf("0x15 alias was not probed")
	}
}
