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

// SnapshotTargetAddressForRouting is the value-typed counterpart of
// TargetAddressForRouting for callers iterating value-typed
// DeviceEntrySnapshot via IterateSnapshots / LookupEntrySnapshot.
//
// P9.3 — closes the lock-free read race surface for B524 root
// candidate enumeration in the semantic Vaillant poller. Reads from
// the snapshot's Faces slice (already a value-typed copy taken under
// the registry's RLock) and falls back to the snapshot's
// PrimaryDisplayAddress.
func SnapshotTargetAddressForRouting(snap registry.DeviceEntrySnapshot) byte {
	if addr, ok := snap.AddressByRole(registry.SlotRoleSlave); ok {
		return addr
	}
	return snap.PrimaryDisplayAddress()
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

// SnapshotContainsAddress is the value-typed counterpart of
// EntryContainsAddress for callers that have already taken a
// DeviceEntrySnapshot via LookupEntrySnapshot / IterateSnapshots.
//
// P9.2 — race-free address-membership check. Pre-P9.2 callers had to
// take an EntryContainsAddress(entry, addr) on a live *deviceEntry
// pointer (entry.Addresses() reads through to mutable storage); this
// helper reads the snapshot's Addresses slice (already a value-typed
// copy taken under the registry's RLock).
func SnapshotContainsAddress(snap registry.DeviceEntrySnapshot, addr byte) bool {
	for _, a := range snap.Addresses {
		if a == addr {
			return true
		}
	}
	return false
}
