package ebusgateway

import "github.com/Project-Helianthus/helianthus-ebusreg/registry"

// TargetAddressForRouting returns the routing-correct target byte for
// an M2S write. Prefers AddressByRole(SlotRoleSlave) so an aliased
// canonical pair (e.g. BAI 0x03↔0x08) returns the target byte 0x08
// rather than the alias-primary which may be the initiator. Falls
// back to PrimaryDisplayAddress only when no target-role face exists
// (single-initiator device, or a face with SlotRoleUnknown that the
// AddressClass-fallback in registry.AddressByRole cannot classify).
//
// Phase C M-C6b helper. M-C7 will replace these per-callsite usages
// with explicit Frame.FrameType + Frame.Validate flow at the
// semantic API boundary.
func TargetAddressForRouting(entry registry.DeviceEntry) byte {
	if entry == nil {
		return 0
	}
	if addr, ok := entry.AddressByRole(registry.SlotRoleSlave); ok {
		return addr
	}
	return entry.PrimaryDisplayAddress()
}

// EntryContainsAddress reports whether the entry's full address set
// (including aliases) contains addr. Use this for membership/lookup
// checks where any face on the entry should match — e.g. register
// dump target resolution and registry containment filters in passive
// discovery — instead of comparing only PrimaryDisplayAddress, which
// would miss the non-display side of an aliased canonical pair.
//
// Phase C M-C6b helper.
func EntryContainsAddress(entry registry.DeviceEntry, addr byte) bool {
	if entry == nil {
		return false
	}
	for _, a := range entry.Addresses() {
		if a == addr {
			return true
		}
	}
	return false
}
