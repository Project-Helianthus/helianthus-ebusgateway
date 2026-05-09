package ebusgateway

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// P9.2 — SnapshotContainsAddress helper.
//
// Race-free counterpart of EntryContainsAddress for callers that
// have already taken a DeviceEntrySnapshot. Reads from the
// snapshot's Addresses slice (a value-typed copy taken under the
// registry's RLock) — disconnected from registry storage and
// immune to concurrent Register / RegisterPassiveObserved /
// AliasAddresses writes.

func TestSnapshotContainsAddress_HitsPrimary(t *testing.T) {
	t.Parallel()

	snap := registry.DeviceEntrySnapshot{
		PrimaryAddress: 0x10,
		Addresses:      []byte{0x10, 0x15},
	}

	if !SnapshotContainsAddress(snap, 0x10) {
		t.Errorf("SnapshotContainsAddress(snap, 0x10) = false; want true (primary in Addresses)")
	}
}

func TestSnapshotContainsAddress_HitsAlias(t *testing.T) {
	t.Parallel()

	snap := registry.DeviceEntrySnapshot{
		PrimaryAddress: 0x10,
		Addresses:      []byte{0x10, 0x15},
	}

	if !SnapshotContainsAddress(snap, 0x15) {
		t.Errorf("SnapshotContainsAddress(snap, 0x15) = false; want true (alias in Addresses)")
	}
}

func TestSnapshotContainsAddress_MissReturnsFalse(t *testing.T) {
	t.Parallel()

	snap := registry.DeviceEntrySnapshot{
		PrimaryAddress: 0x10,
		Addresses:      []byte{0x10, 0x15},
	}

	if SnapshotContainsAddress(snap, 0x42) {
		t.Errorf("SnapshotContainsAddress(snap, 0x42) = true; want false")
	}
}

func TestSnapshotContainsAddress_EmptySliceReturnsFalse(t *testing.T) {
	t.Parallel()

	snap := registry.DeviceEntrySnapshot{}

	if SnapshotContainsAddress(snap, 0x10) {
		t.Errorf("SnapshotContainsAddress(empty, 0x10) = true; want false")
	}
}

// TestSnapshotContainsAddress_AliasResolutionParity verifies the
// behavior-equivalence contract between EntryContainsAddress (live
// pointer) and SnapshotContainsAddress (snapshot) on an aliased
// canonical pair. The promoter's registryContains migration depends
// on this parity: both APIs must report the same membership truth on
// the same registry state, otherwise the migration would change
// observable behaviour.
//
// NOTE (Codex P9.2 review pass 1 NIT FINDING_2): this is NOT a
// regression test for the promoter's specific IterateSnapshots vs
// Iterate routing — registryContains is unexported and the promoter
// uses *registry.DeviceRegistry (concrete type, not interface) so a
// counter-tracking mock isn't substitutable without refactoring the
// promoter to take a Registry interface. The migration's correctness
// is verified by:
//
//  1. Code inspection of the diff (one-line API swap at
//     passive_discovery_promoter.go:420 — Iterate → IterateSnapshots,
//     EntryContainsAddress → SnapshotContainsAddress).
//  2. This behavior-parity test on the underlying helpers.
//  3. -race ./... on the full suite (race detector authoritative).
//
// TestSnapshotTargetAddressForRouting_ParityWithLive verifies the
// same parity contract for the routing helper added in P9.3:
// SnapshotTargetAddressForRouting must return the same byte as
// TargetAddressForRouting for the same registered entry. The
// post-P9.3 semantic_vaillant migration depends on this parity.
func TestSnapshotTargetAddressForRouting_ParityWithLive(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x10, SerialNumber: "SN-X"})
	reg.Register(registry.DeviceInfo{Address: 0x15, SerialNumber: "SN-Y"})
	if err := reg.AliasAddresses(0x10, 0x15); err != nil {
		t.Fatalf("AliasAddresses error = %v", err)
	}

	entry, _ := reg.Lookup(0x10)
	snap, _ := reg.LookupEntrySnapshot(0x10)

	gotEntry := TargetAddressForRouting(entry)
	gotSnap := SnapshotTargetAddressForRouting(snap)
	if gotEntry != gotSnap {
		t.Errorf("TargetAddressForRouting=0x%02X != SnapshotTargetAddressForRouting=0x%02X (parity violation)", gotEntry, gotSnap)
	}
	// At minimum the result must be one of the entry's addresses.
	addrs := snap.Addresses
	found := false
	for _, a := range addrs {
		if a == gotSnap {
			found = true
		}
	}
	if !found {
		t.Errorf("SnapshotTargetAddressForRouting returned 0x%02X but Addresses=%v", gotSnap, addrs)
	}
}

func TestSnapshotContainsAddress_AliasResolutionParity(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x10, SerialNumber: "SN-X"})
	reg.Register(registry.DeviceInfo{Address: 0x15, SerialNumber: "SN-Y"})
	if err := reg.AliasAddresses(0x10, 0x15); err != nil {
		t.Fatalf("AliasAddresses error = %v", err)
	}

	// Take both views of the same entry: live pointer (used by
	// EntryContainsAddress) and snapshot (used by
	// SnapshotContainsAddress). Both APIs must agree for every test
	// address — that's the parity the P9.2 promoter migration depends
	// on.
	entry, entryOK := reg.Lookup(0x10)
	if !entryOK {
		t.Fatalf("Lookup(0x10) ok=false")
	}
	snap, snapOK := reg.LookupEntrySnapshot(0x10)
	if !snapOK {
		t.Fatalf("LookupEntrySnapshot(0x10) ok=false")
	}

	cases := []struct {
		addr byte
		want bool
		desc string
	}{
		{0x10, true, "primary"},
		{0x15, true, "alias"},
		{0x42, false, "miss"},
	}
	for _, tc := range cases {
		gotEntry := EntryContainsAddress(entry, tc.addr)
		gotSnap := SnapshotContainsAddress(snap, tc.addr)
		if gotEntry != tc.want {
			t.Errorf("%s: EntryContainsAddress(entry, 0x%02X) = %v; want %v", tc.desc, tc.addr, gotEntry, tc.want)
		}
		if gotSnap != tc.want {
			t.Errorf("%s: SnapshotContainsAddress(snap, 0x%02X) = %v; want %v", tc.desc, tc.addr, gotSnap, tc.want)
		}
		if gotEntry != gotSnap {
			t.Errorf("%s: EntryContainsAddress=%v != SnapshotContainsAddress=%v for 0x%02X (parity violation — promoter migration would change behavior)", tc.desc, gotEntry, gotSnap, tc.addr)
		}
	}
}
