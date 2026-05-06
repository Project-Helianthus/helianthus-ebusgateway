package ebusgateway

import (
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type AddressSlot struct {
	Addr              byte
	Role              string
	DiscoverySource   string
	VerificationState string
	// PriorityTier is the eBUS standard priority class (p0..p4) when
	// Addr is a canonical source from sourceAddressTableV1, else "".
	PriorityTier protocol.SourceAddressPriorityIndex
	// FreeUse is true when Addr is a canonical source AND the eBUS
	// standard table marks the row free-use (e.g. 0x07, 0x17, 0x7F,
	// 0x1F/0x3F/0x7F/0xF7). False for non-canonical addresses or
	// preallocated canonical sources.
	FreeUse      bool
	RegistrySlot *registry.AddressSlot
}

// canonicalSlotMetadata returns the priority tier and free-use flag for
// addr from the eBUS standard table. Both halves of a canonical pair
// (source AND companion) inherit the same tier/free-use from the row's
// source side: 0x10 and 0x15 are both p0/preallocated, 0x7F and 0x84 are
// both p4/free-use, 0xFF and 0x04 are both p4/preallocated. Returns
// ("", false) only for addresses outside the canonical 25 pairs.
func canonicalSlotMetadata(addr byte) (protocol.SourceAddressPriorityIndex, bool) {
	source := addr
	if !protocol.IsCanonicalSource(source) {
		canonicalSource, ok := protocol.SourceOfCompanion(addr)
		if !ok {
			return "", false
		}
		source = canonicalSource
	}
	tier, ok := protocol.SourceTier(source)
	if !ok {
		return "", false
	}
	free, _ := protocol.IsFreeUseSource(source)
	return tier, free
}

type AddressTable struct {
	reg   *registry.DeviceRegistry
	slots map[byte]*AddressSlot
}

func NewAddressTable(regs ...*registry.DeviceRegistry) *AddressTable {
	var reg *registry.DeviceRegistry
	if len(regs) > 0 {
		reg = regs[0]
	}
	if reg == nil {
		reg = registry.NewDeviceRegistry(nil)
	}
	return &AddressTable{
		reg:   reg,
		slots: make(map[byte]*AddressSlot),
	}
}

func (t *AddressTable) Lookup(addr byte) (*AddressSlot, bool) {
	if t == nil {
		return nil, false
	}
	if slot, ok := t.slots[addr]; ok && slot != nil {
		return slot, true
	}
	// Consult the wrapped registry so static seeds and active-discovered
	// devices are visible here, preventing maybeInsert from rewriting
	// their metadata (Codex P2: preserve-existing-registry-slots).
	if t.reg != nil {
		if regSlot, ok := t.reg.LookupSlot(addr); ok && regSlot != nil {
			role := ""
			switch regSlot.Role {
			case registry.SlotRoleMaster:
				role = "initiator"
			case registry.SlotRoleSlave:
				role = "target"
			}
			discovery := ""
			switch regSlot.DiscoverySource {
			case registry.DiscoverySourcePassiveObserved:
				discovery = "passive_observed"
			case registry.DiscoverySourceStaticSeed:
				discovery = "static_seed"
			case registry.DiscoverySourceActiveConfirmed:
				discovery = "active_confirmed"
			}
			verification := ""
			switch regSlot.VerificationState {
			case registry.VerificationStateCandidate:
				verification = "candidate"
			case registry.VerificationStateCorroborated:
				verification = "corroborated_pending"
			case registry.VerificationStateIdentityConfirmed:
				verification = "identity_confirmed"
			}
			tier, free := canonicalSlotMetadata(addr)
			return &AddressSlot{
				Addr:              addr,
				Role:              role,
				DiscoverySource:   discovery,
				VerificationState: verification,
				PriorityTier:      tier,
				FreeUse:           free,
				RegistrySlot:      regSlot,
			}, true
		}
	}
	return nil, false
}
