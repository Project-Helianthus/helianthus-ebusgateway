package mcp

import (
	"errors"
	"testing"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// P9.1 — getDevice uses LookupEntrySnapshot (race-free identity).

// getDeviceTrackingRegistry counts which lookup API was called by
// getDevice. Pre-P9.1 the call routed through Lookup (live entry);
// post-P9.1 it routes through LookupEntrySnapshot.
type getDeviceTrackingRegistry struct {
	lookupCalls         int
	lookupSnapshotCalls int
	snapshot            registry.DeviceEntrySnapshot
	hasSnapshot         bool
	slotSnap            registry.AddressSlotSnapshot
	hasSlotSnap         bool
}

func (r *getDeviceTrackingRegistry) Iterate(func(registry.DeviceEntry) bool) {}

func (r *getDeviceTrackingRegistry) Lookup(byte) (registry.DeviceEntry, bool) {
	r.lookupCalls++
	return nil, false
}

func (r *getDeviceTrackingRegistry) LookupSlot(byte) (*registry.AddressSlot, bool) {
	return nil, false
}

func (r *getDeviceTrackingRegistry) LookupSlotSnapshot(byte) (registry.AddressSlotSnapshot, bool) {
	if r.hasSlotSnap {
		return r.slotSnap, true
	}
	return registry.AddressSlotSnapshot{}, false
}

func (r *getDeviceTrackingRegistry) IterateSnapshots(func(registry.DeviceEntrySnapshot) bool) {
}

func (r *getDeviceTrackingRegistry) LookupEntrySnapshot(byte) (registry.DeviceEntrySnapshot, bool) {
	r.lookupSnapshotCalls++
	if r.hasSnapshot {
		return r.snapshot, true
	}
	return registry.DeviceEntrySnapshot{}, false
}

// TestGetDevice_UsesLookupEntrySnapshot proves the P9.1 contract:
// getDevice calls LookupEntrySnapshot for identity fields and does
// NOT use the live-pointer Lookup path FOR IDENTITY FIELDS.
//
// NOTE: getDevice's buildDeviceInfoFromSnapshot helper still calls
// reg.Lookup ONCE to fetch Planes (residual surface, P9.2+
// follow-up). The assertion `lookupCalls <= 1` accommodates that
// expected residual; tightening the assertion to `lookupCalls == 0`
// would be a regression because the Plane fetch is the documented
// behavior, not a leftover live-pointer identity read.
func TestGetDevice_UsesLookupEntrySnapshot(t *testing.T) {
	t.Parallel()

	reg := &getDeviceTrackingRegistry{
		hasSnapshot: true,
		snapshot: registry.DeviceEntrySnapshot{
			PrimaryAddress: 0x10,
			Addresses:      []byte{0x10, 0x15},
			Manufacturer:   "Vaillant",
			DeviceID:       "BASV2",
		},
		hasSlotSnap: true,
		slotSnap: registry.AddressSlotSnapshot{
			DiscoverySource:   registry.DiscoverySourcePassiveObserved,
			VerificationState: registry.VerificationStateCorroborated,
		},
	}

	server := &Server{registry: reg}
	dev, err := server.getDevice(map[string]any{"address": float64(0x10)}, nil)
	if err != nil {
		t.Fatalf("getDevice error = %v", err)
	}

	if reg.lookupSnapshotCalls != 1 {
		t.Errorf("LookupEntrySnapshot called %d times; want 1 (P9.1 contract: primary identity-read path)", reg.lookupSnapshotCalls)
	}
	// reg.lookupCalls is permitted to be 1 (Planes fetch from
	// buildDeviceInfoFromSnapshot's residual). Strictly assert it
	// wasn't called more than the Plane fetch.
	if reg.lookupCalls > 1 {
		t.Errorf("Lookup called %d times; want at most 1 (Planes residual)", reg.lookupCalls)
	}

	if dev.Manufacturer != "Vaillant" {
		t.Errorf("Manufacturer = %q; want Vaillant", dev.Manufacturer)
	}
	if dev.DeviceID != "BASV2" {
		t.Errorf("DeviceID = %q; want BASV2", dev.DeviceID)
	}
	if dev.DiscoverySource != "passive_observed" {
		t.Errorf("DiscoverySource = %q; want passive_observed", dev.DiscoverySource)
	}
}

// TestGetDevice_AbsentEntryReturnsError verifies the error path
// (LookupEntrySnapshot returns false → ErrNoSuchDevice).
//
// Locks down two contracts:
//   - The error wraps ebuserrors.ErrNoSuchDevice (operator-visible
//     contract — MCP returns a specific error code for unknown
//     addresses).
//   - The live-pointer Lookup path is NEVER consulted on snapshot
//     miss (P9.1 contract — no fallback that would reintroduce the
//     race surface).
func TestGetDevice_AbsentEntryReturnsError(t *testing.T) {
	t.Parallel()

	reg := &getDeviceTrackingRegistry{hasSnapshot: false}
	server := &Server{registry: reg}
	_, err := server.getDevice(map[string]any{"address": float64(0x42)}, nil)
	if err == nil {
		t.Fatal("getDevice for absent entry: err=nil; want ErrNoSuchDevice")
	}
	if !errors.Is(err, ebuserrors.ErrNoSuchDevice) {
		t.Errorf("err = %v; want errors.Is(err, ErrNoSuchDevice)", err)
	}
	if reg.lookupSnapshotCalls != 1 {
		t.Errorf("LookupEntrySnapshot called %d times; want 1", reg.lookupSnapshotCalls)
	}
	if reg.lookupCalls != 0 {
		t.Errorf("Lookup called %d times; want 0 (P9.1 contract: snapshot miss must NOT fall back to live Lookup)", reg.lookupCalls)
	}
}
