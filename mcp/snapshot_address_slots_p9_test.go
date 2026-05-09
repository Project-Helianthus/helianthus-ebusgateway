package mcp

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// P9 — snapshotAddressSlots uses IterateSnapshots / LookupSlotSnapshot.

// iterateTrackingRegistry counts which iteration API was called.
type iterateTrackingRegistry struct {
	iterateCalls         int
	iterateSnapshotCalls int
	snapshots            []registry.DeviceEntrySnapshot
	slotSnapshots        map[byte]registry.AddressSlotSnapshot
}

func (r *iterateTrackingRegistry) Iterate(fn func(registry.DeviceEntry) bool) {
	r.iterateCalls++
	// Should NOT be called by snapshotAddressSlots after P9.
}

func (r *iterateTrackingRegistry) Lookup(byte) (registry.DeviceEntry, bool) {
	return nil, false
}

func (r *iterateTrackingRegistry) LookupSlot(byte) (*registry.AddressSlot, bool) {
	return nil, false
}

func (r *iterateTrackingRegistry) LookupSlotSnapshot(addr byte) (registry.AddressSlotSnapshot, bool) {
	if snap, ok := r.slotSnapshots[addr]; ok {
		return snap, true
	}
	return registry.AddressSlotSnapshot{}, false
}

func (r *iterateTrackingRegistry) IterateSnapshots(fn func(registry.DeviceEntrySnapshot) bool) {
	r.iterateSnapshotCalls++
	for _, snap := range r.snapshots {
		if !fn(snap) {
			return
		}
	}
}

func (r *iterateTrackingRegistry) LookupEntrySnapshot(byte) (registry.DeviceEntrySnapshot, bool) {
	return registry.DeviceEntrySnapshot{}, false
}

// TestSnapshotAddressSlots_UsesIterateSnapshots proves the P9 contract:
// snapshotAddressSlots iterates via IterateSnapshots (race-free) and
// no longer calls Iterate (which dereferences live entry pointers
// outside the registry's lock).
func TestSnapshotAddressSlots_UsesIterateSnapshots(t *testing.T) {
	t.Parallel()

	reg := &iterateTrackingRegistry{
		snapshots: []registry.DeviceEntrySnapshot{
			{
				PrimaryAddress: 0x10,
				Addresses:      []byte{0x10, 0x15},
				Manufacturer:   "Vaillant",
			},
		},
		slotSnapshots: map[byte]registry.AddressSlotSnapshot{
			0x10: {
				DiscoverySource:   registry.DiscoverySourcePassiveObserved,
				VerificationState: registry.VerificationStateCorroborated,
			},
			0x15: {
				DiscoverySource:   registry.DiscoverySourcePassiveObserved,
				VerificationState: registry.VerificationStateCorroborated,
			},
		},
	}

	server := &Server{registry: reg}
	out := server.snapshotAddressSlots()

	if reg.iterateSnapshotCalls != 1 {
		t.Errorf("IterateSnapshots called %d times; want 1", reg.iterateSnapshotCalls)
	}
	if reg.iterateCalls != 0 {
		t.Errorf("Iterate called %d times; want 0 (P9 contract: snapshot iteration only)", reg.iterateCalls)
	}
	if len(out) != 2 {
		t.Errorf("output map size = %d; want 2 (both 0x10 and 0x15)", len(out))
	}
	if out[0x10].discovery != "passive_observed" {
		t.Errorf("out[0x10].discovery = %q; want passive_observed", out[0x10].discovery)
	}
	if out[0x15].discovery != "passive_observed" {
		t.Errorf("out[0x15].discovery = %q; want passive_observed", out[0x15].discovery)
	}
}

// TestSnapshotAddressSlots_NilRegistryReturnsNil verifies the early
// nil-guard.
func TestSnapshotAddressSlots_NilRegistryReturnsNil(t *testing.T) {
	t.Parallel()
	server := &Server{registry: nil}
	if got := server.snapshotAddressSlots(); got != nil {
		t.Errorf("snapshotAddressSlots() = %v; want nil for nil registry", got)
	}
}

// TestSnapshotAddressSlots_EmptyRegistryReturnsNil verifies that an
// empty registry produces nil (not an empty map).
func TestSnapshotAddressSlots_EmptyRegistryReturnsNil(t *testing.T) {
	t.Parallel()
	reg := &iterateTrackingRegistry{}
	server := &Server{registry: reg}
	if got := server.snapshotAddressSlots(); got != nil {
		t.Errorf("snapshotAddressSlots() = %v; want nil for empty registry", got)
	}
}
