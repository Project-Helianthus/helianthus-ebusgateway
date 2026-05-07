package ebusgateway

import "github.com/Project-Helianthus/helianthus-ebusreg/registry"

// targetAddressForRouting returns the routing-correct target byte for
// an M2S write. Prefers AddressByRole(SlotRoleSlave) so an aliased
// canonical pair (e.g. BAI 0x03↔0x08) returns the slave byte 0x08
// rather than the alias-primary which may be the initiator. Falls
// back to PrimaryDisplayAddress only when no slave-role face exists
// (single-master device, or a face with SlotRoleUnknown that the
// AddressClass-fallback in registry.AddressByRole cannot classify).
//
// Phase C M-C6b helper. M-C7 will replace these per-callsite usages
// with explicit Frame.FrameType + Frame.Validate flow at the
// semantic API boundary.
func targetAddressForRouting(entry registry.DeviceEntry) byte {
	if entry == nil {
		return 0
	}
	if addr, ok := entry.AddressByRole(registry.SlotRoleSlave); ok {
		return addr
	}
	return entry.PrimaryDisplayAddress()
}
