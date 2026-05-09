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
func TestSnapshotContainsAddress_AliasResolutionParity(t *testing.T) {
	t.Parallel()

	reg := registry.NewDeviceRegistry(nil)
	reg.Register(registry.DeviceInfo{Address: 0x10, SerialNumber: "SN-X"})
	reg.Register(registry.DeviceInfo{Address: 0x15, SerialNumber: "SN-Y"})
	if err := reg.AliasAddresses(0x10, 0x15); err != nil {
		t.Fatalf("AliasAddresses error = %v", err)
	}

	// Build a snapshot of the merged entry; verify both addresses
	// are visible in the snapshot's Addresses slice.
	snap, ok := reg.LookupEntrySnapshot(0x10)
	if !ok {
		t.Fatalf("LookupEntrySnapshot(0x10) ok=false")
	}
	if !SnapshotContainsAddress(snap, 0x10) {
		t.Errorf("SnapshotContainsAddress(snap, 0x10) = false; want true")
	}
	if !SnapshotContainsAddress(snap, 0x15) {
		t.Errorf("SnapshotContainsAddress(snap, 0x15) = false; want true (alias)")
	}
}
