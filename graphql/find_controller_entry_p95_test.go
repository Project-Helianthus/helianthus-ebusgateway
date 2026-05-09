package graphql

import (
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// P9.5 — findControllerEntry uses IterateSnapshots for DeviceID scan.
//
// Pre-P9.5 the iteration read entry.DeviceID() lock-free; Post-P9.5
// it reads from a value-typed snapshot. This test pins the snapshot
// path with a tracking mock that counts which iteration API was
// called. Codex P9.5 review pass 1 NIT FINDING_2 noted that
// existing mutationTestRegistry only implements Iterate (not
// IterateSnapshots), so without this test the new path isn't
// directly exercised in regression.

type findControllerTrackingRegistry struct {
	iterateCalls         int
	iterateSnapshotCalls int
	lookupCalls          int
	lookupSnapCalls      int
	snapshots            []registry.DeviceEntrySnapshot
	entries              map[byte]registry.DeviceEntry
	snapByAddr           map[byte]registry.DeviceEntrySnapshot
}

func (r *findControllerTrackingRegistry) Lookup(addr byte) (registry.DeviceEntry, bool) {
	r.lookupCalls++
	entry, ok := r.entries[addr]
	return entry, ok
}

func (r *findControllerTrackingRegistry) Iterate(fn func(registry.DeviceEntry) bool) {
	r.iterateCalls++
}

func (r *findControllerTrackingRegistry) IterateSnapshots(fn func(registry.DeviceEntrySnapshot) bool) {
	r.iterateSnapshotCalls++
	for _, snap := range r.snapshots {
		if !fn(snap) {
			return
		}
	}
}

func (r *findControllerTrackingRegistry) LookupEntrySnapshot(addr byte) (registry.DeviceEntrySnapshot, bool) {
	r.lookupSnapCalls++
	if snap, ok := r.snapByAddr[addr]; ok {
		return snap, true
	}
	return registry.DeviceEntrySnapshot{}, false
}

// trackingControllerEntry is a minimal DeviceEntry implementation
// returned by the mock's Lookup. Only methods exercised by
// findControllerEntry's caller pattern are implemented (Manufacturer,
// DeviceID, Planes — return safe values).
type trackingControllerEntry struct {
	primaryAddress byte
	deviceID       string
}

func (e *trackingControllerEntry) AddressByRole(registry.SlotRole) (byte, bool) {
	return e.primaryAddress, true
}
func (e *trackingControllerEntry) PrimaryDisplayAddress() byte { return e.primaryAddress }
func (e *trackingControllerEntry) Addresses() []byte           { return []byte{e.primaryAddress} }
func (e *trackingControllerEntry) Manufacturer() string        { return "Vaillant" }
func (e *trackingControllerEntry) DeviceID() string            { return e.deviceID }
func (e *trackingControllerEntry) SerialNumber() string        { return "" }
func (e *trackingControllerEntry) MacAddress() string          { return "" }
func (e *trackingControllerEntry) SoftwareVersion() string     { return "" }
func (e *trackingControllerEntry) HardwareVersion() string     { return "" }
func (e *trackingControllerEntry) Planes() []registry.Plane    { return nil }
func (e *trackingControllerEntry) Projections() []registry.Projection {
	return nil
}

// TestFindControllerEntry_UsesSnapshotIteration proves the P9.5
// contract: findControllerEntry calls IterateSnapshots (NOT Iterate)
// for the BASV2 DeviceID prefix scan.
func TestFindControllerEntry_UsesSnapshotIteration(t *testing.T) {
	t.Parallel()

	basvEntry := &trackingControllerEntry{primaryAddress: 0x10, deviceID: "BASV2X"}
	reg := &findControllerTrackingRegistry{
		snapshots: []registry.DeviceEntrySnapshot{
			{PrimaryAddress: 0x10, DeviceID: "BASV2X"},
		},
		entries: map[byte]registry.DeviceEntry{
			0x10: basvEntry,
		},
		snapByAddr: map[byte]registry.DeviceEntrySnapshot{
			0x10: {PrimaryAddress: 0x10, DeviceID: "BASV2X"},
		},
	}

	got, err := findControllerEntry(reg)
	if err != nil {
		t.Fatalf("findControllerEntry error = %v", err)
	}
	if got != basvEntry {
		t.Errorf("returned entry = %v; want the BASV2 entry at 0x10", got)
	}

	if reg.iterateSnapshotCalls != 1 {
		t.Errorf("IterateSnapshots called %d times; want 1 (P9.5 contract)", reg.iterateSnapshotCalls)
	}
	if reg.iterateCalls != 0 {
		t.Errorf("Iterate called %d times; want 0 (P9.5 contract: snapshot path is primary)", reg.iterateCalls)
	}
	if reg.lookupSnapCalls != 1 {
		t.Errorf("LookupEntrySnapshot called %d times; want 1 (TOCTOU re-validation)", reg.lookupSnapCalls)
	}
}

// TestFindControllerEntry_TOCTOUFallsBackOnRevalidationFailure
// proves the Codex P9.5 pass 1 MINOR FINDING_1 fix: when the
// snapshot iteration finds a BASV2 candidate at addr but a
// concurrent Register has since swapped the entry (LookupEntrySnapshot
// at the same addr no longer reports BASV-prefixed DeviceID), the
// function falls back to the canonical-address Lookup instead of
// returning a non-BASV entry's Planes.
func TestFindControllerEntry_TOCTOUFallsBackOnRevalidationFailure(t *testing.T) {
	t.Parallel()

	basvEntry := &trackingControllerEntry{primaryAddress: 0x10, deviceID: "BASV2X"}
	fallbackEntry := &trackingControllerEntry{primaryAddress: mutationControllerFallbackAddr, deviceID: "BASV2-FALLBACK"}
	reg := &findControllerTrackingRegistry{
		snapshots: []registry.DeviceEntrySnapshot{
			// IterateSnapshots reports BASV at 0x10
			{PrimaryAddress: 0x10, DeviceID: "BASV2X"},
		},
		entries: map[byte]registry.DeviceEntry{
			0x10:                            basvEntry,
			mutationControllerFallbackAddr:  fallbackEntry,
		},
		snapByAddr: map[byte]registry.DeviceEntrySnapshot{
			// Re-validation: 0x10 has been swapped to a non-BASV entry.
			0x10: {PrimaryAddress: 0x10, DeviceID: "OTHER-DEVICE"},
		},
	}

	got, err := findControllerEntry(reg)
	if err != nil {
		t.Fatalf("findControllerEntry error = %v", err)
	}
	// On re-validation failure we must NOT return the original BASV
	// entry — we must use the fallback path.
	if got == basvEntry {
		t.Errorf("returned the snapshot-found entry despite TOCTOU re-validation failure; should have used fallback")
	}
	if got != fallbackEntry {
		t.Errorf("returned entry = %v; want the fallback entry at 0x%02X", got, mutationControllerFallbackAddr)
	}
	if reg.lookupSnapCalls != 1 {
		t.Errorf("LookupEntrySnapshot called %d times; want 1 (re-validation step)", reg.lookupSnapCalls)
	}
	deviceID := strings.ToUpper(strings.TrimSpace(got.DeviceID()))
	if !strings.HasPrefix(deviceID, "BASV") {
		t.Errorf("returned entry DeviceID = %q; want BASV-prefixed (fallback should be canonical BASV2 address)", deviceID)
	}
}

// TestFindControllerEntry_AbsentBASVUsesFallback proves the
// existing fallback path: when no BASV entry exists in the
// registry, the function returns the entry at
// mutationControllerFallbackAddr.
func TestFindControllerEntry_AbsentBASVUsesFallback(t *testing.T) {
	t.Parallel()

	fallbackEntry := &trackingControllerEntry{primaryAddress: mutationControllerFallbackAddr, deviceID: "BASV2-FALLBACK"}
	reg := &findControllerTrackingRegistry{
		snapshots: []registry.DeviceEntrySnapshot{
			{PrimaryAddress: 0x20, DeviceID: "OTHER"},
		},
		entries: map[byte]registry.DeviceEntry{
			mutationControllerFallbackAddr: fallbackEntry,
		},
		snapByAddr: map[byte]registry.DeviceEntrySnapshot{},
	}

	got, err := findControllerEntry(reg)
	if err != nil {
		t.Fatalf("findControllerEntry error = %v", err)
	}
	if got != fallbackEntry {
		t.Errorf("returned entry = %v; want the fallback entry", got)
	}
}
