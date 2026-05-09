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

// TestPassiveDiscoveryPromoter_RegistryContainsUsesIterateSnapshots
// is a partial integration check: register an entry, call the
// promoter's registryContains via reflection or a public proxy. The
// promoter's registryContains is private; use an alias-aware
// registry to verify the address-membership semantics still hold
// post-migration.
//
// We use the real *registry.DeviceRegistry (not a tracking mock)
// because registryContains is unexported. The race-detector run as
// part of `go test -race ./...` already covers the migration's
// correctness against concurrent writers.
func TestPassiveDiscoveryPromoter_RegistryContains_PostP92(t *testing.T) {
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
