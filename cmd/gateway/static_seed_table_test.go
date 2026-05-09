package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// TestApplyStaticSeedTable_StampsStaticSeedLabel asserts the central
// P3.5 contract: applyStaticSeedTable plants identity for the
// productids seed entries via registry.RegisterStaticSeed, so each
// resulting AddressSlot is labelled
// DiscoverySource=DiscoverySourceStaticSeed and
// VerificationState=VerificationStateCandidate (NOT the
// ActiveConfirmed/IdentityConfirmed labels Register would stamp).
//
// Coverage: every address in productids.LoadSeedTable(true) must
// appear with the static_seed/candidate labels after applyStaticSeedTable.
// This locks in the operator-visible discovery_source label going
// through the address-table snapshot at
// helianthus-ebusgateway/address_table.go.
func TestApplyStaticSeedTable_StampsStaticSeedLabel(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	applyStaticSeedTable(reg)

	// Each NETX3 face + each BASV2 face from productids.LoadSeedTable(true).
	expectedAddresses := []byte{0xF1, 0xF6, 0x04, 0x15, 0xEC}
	for _, addr := range expectedAddresses {
		slot, ok := reg.LookupSlot(addr)
		if !ok || slot == nil {
			t.Errorf("LookupSlot(0x%02X) ok=%v slot=%v; want non-nil seeded slot", addr, ok, slot)
			continue
		}
		if got, want := slot.DiscoverySource, registry.DiscoverySourceStaticSeed; got != want {
			t.Errorf("slot[0x%02X].DiscoverySource = %v; want %v (must NOT be active_confirmed for a static-seeded entry)", addr, got, want)
		}
		if got, want := slot.VerificationState, registry.VerificationStateCandidate; got != want {
			t.Errorf("slot[0x%02X].VerificationState = %v; want %v", addr, got, want)
		}
		if entry, ok := reg.Lookup(addr); !ok || entry == nil {
			t.Errorf("Lookup(0x%02X) ok=%v entry=%v; want non-nil", addr, ok, entry)
		} else {
			if got, want := entry.Manufacturer(), "Vaillant"; got != want {
				t.Errorf("entry[0x%02X].Manufacturer() = %q; want %q", addr, got, want)
			}
		}
	}
}

// TestApplyStaticSeedTable_RoleMapping asserts the gateway-side
// role-string → registry.SlotRole mapping. Roles in
// productids.SeedAddressEntry are free-form strings; we map at this
// seam to typed enum values so registry stays
// productids-agnostic.
//
// Maps:
//   - "initiator" → SlotRoleMaster (initiator-role on the bus)
//   - "target"    → SlotRoleSlave  (target-role on the bus)
//   - anything else → SlotRoleUnknown (registry's monotonic guard
//     leaves Role untouched; passive observation or active scan
//     fills it later)
//
// productids.LoadSeedTable assigns:
//   - 0xF1 → "initiator" (NETX3 master face)  → SlotRoleMaster
//   - 0xF6 / 0x04 → "target" (NETX3 target faces) → SlotRoleSlave
//   - 0x15 / 0xEC → "target" (BASV2 target faces) → SlotRoleSlave
func TestApplyStaticSeedTable_RoleMapping(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	applyStaticSeedTable(reg)

	cases := []struct {
		addr byte
		role registry.SlotRole
	}{
		{0xF1, registry.SlotRoleMaster},
		{0xF6, registry.SlotRoleSlave},
		{0x04, registry.SlotRoleSlave},
		{0x15, registry.SlotRoleSlave},
		{0xEC, registry.SlotRoleSlave},
	}
	for _, tc := range cases {
		slot, ok := reg.LookupSlot(tc.addr)
		if !ok || slot == nil {
			t.Errorf("LookupSlot(0x%02X) ok=%v slot=%v", tc.addr, ok, slot)
			continue
		}
		if slot.Role != tc.role {
			t.Errorf("slot[0x%02X].Role = %v; want %v", tc.addr, slot.Role, tc.role)
		}
	}
}
