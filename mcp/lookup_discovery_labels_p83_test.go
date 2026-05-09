package mcp

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// P8.3 — lookupDiscoveryLabels uses LookupSlotSnapshot.
//
// Pre-P8.3 lookupDiscoveryLabels called LookupSlot (live pointer)
// and read slot.DiscoverySource / .VerificationState lock-free —
// race-prone with concurrent Register / RegisterPassiveObserved /
// MarkSlot* writers. Switched to LookupSlotSnapshot (value copy
// under registry RLock).

// trackingRegistry is a Registry implementation that counts which
// methods were called. Used to prove lookupDiscoveryLabels does NOT
// fall back to LookupSlot for the enum reads.
type trackingRegistry struct {
	lookupSlotCalls         int
	lookupSlotSnapshotCalls int
	snap                    registry.AddressSlotSnapshot
	hasSnap                 bool
}

func (t *trackingRegistry) Iterate(func(registry.DeviceEntry) bool) {}

func (t *trackingRegistry) Lookup(byte) (registry.DeviceEntry, bool) { return nil, false }

func (t *trackingRegistry) LookupSlot(byte) (*registry.AddressSlot, bool) {
	t.lookupSlotCalls++
	return nil, false
}

func (t *trackingRegistry) LookupSlotSnapshot(byte) (registry.AddressSlotSnapshot, bool) {
	t.lookupSlotSnapshotCalls++
	if t.hasSnap {
		return t.snap, true
	}
	return registry.AddressSlotSnapshot{}, false
}

func TestLookupDiscoveryLabels_UsesSnapshotPath(t *testing.T) {
	t.Parallel()

	reg := &trackingRegistry{
		hasSnap: true,
		snap: registry.AddressSlotSnapshot{
			Addr:              0x10,
			Role:              registry.SlotRoleMaster,
			DiscoverySource:   registry.DiscoverySourcePassiveObserved,
			VerificationState: registry.VerificationStateCorroborated,
			FirstObservedAt:   time.Now(),
			LastObservedAt:    time.Now(),
			DeviceAttached:    true,
		},
	}

	discovery, verification := lookupDiscoveryLabels(reg, 0x10)

	if reg.lookupSlotSnapshotCalls != 1 {
		t.Errorf("LookupSlotSnapshot called %d times; want 1 (P8.3 contract: snapshot path is the primary)", reg.lookupSlotSnapshotCalls)
	}
	if reg.lookupSlotCalls != 0 {
		t.Errorf("LookupSlot called %d times; want 0 (P8.3 contract: live-pointer path is no longer used for label reads)", reg.lookupSlotCalls)
	}
	if discovery != "passive_observed" {
		t.Errorf("discovery = %q; want passive_observed", discovery)
	}
	if verification != "corroborated_pending" {
		t.Errorf("verification = %q; want corroborated_pending", verification)
	}
}

func TestLookupDiscoveryLabels_AbsentSnapshotReturnsEmpty(t *testing.T) {
	t.Parallel()

	reg := &trackingRegistry{hasSnap: false}

	discovery, verification := lookupDiscoveryLabels(reg, 0x99)
	if discovery != "" || verification != "" {
		t.Errorf("absent slot: discovery=%q verification=%q; want both empty", discovery, verification)
	}
	if reg.lookupSlotSnapshotCalls != 1 {
		t.Errorf("LookupSlotSnapshot called %d times; want 1", reg.lookupSlotSnapshotCalls)
	}
	if reg.lookupSlotCalls != 0 {
		t.Errorf("LookupSlot called %d times; want 0 (no fallback to live pointer)", reg.lookupSlotCalls)
	}
}

func TestLookupDiscoveryLabels_NilRegistryReturnsEmpty(t *testing.T) {
	t.Parallel()

	discovery, verification := lookupDiscoveryLabels(nil, 0x10)
	if discovery != "" || verification != "" {
		t.Errorf("nil registry: discovery=%q verification=%q; want both empty", discovery, verification)
	}
}

func TestLookupDiscoveryLabels_StaticSeedAndActiveConfirmedProjections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		discovery        registry.DiscoverySource
		verification     registry.VerificationState
		wantDiscovery    string
		wantVerification string
	}{
		{
			name:             "static_seed_candidate",
			discovery:        registry.DiscoverySourceStaticSeed,
			verification:     registry.VerificationStateCandidate,
			wantDiscovery:    "static_seed",
			wantVerification: "candidate",
		},
		{
			name:             "active_confirmed_identity",
			discovery:        registry.DiscoverySourceActiveConfirmed,
			verification:     registry.VerificationStateIdentityConfirmed,
			wantDiscovery:    "active_confirmed",
			wantVerification: "identity_confirmed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := &trackingRegistry{
				hasSnap: true,
				snap: registry.AddressSlotSnapshot{
					Addr:              0x10,
					DiscoverySource:   tc.discovery,
					VerificationState: tc.verification,
				},
			}
			discovery, verification := lookupDiscoveryLabels(reg, 0x10)
			if discovery != tc.wantDiscovery {
				t.Errorf("discovery = %q; want %q", discovery, tc.wantDiscovery)
			}
			if verification != tc.wantVerification {
				t.Errorf("verification = %q; want %q", verification, tc.wantVerification)
			}
		})
	}
}
