package ebusgateway

import "github.com/Project-Helianthus/helianthus-ebusreg/registry"

type AddressSlot struct {
	Addr              byte
	Role              string
	DiscoverySource   string
	VerificationState string
	RegistrySlot      *registry.AddressSlot
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
			return &AddressSlot{
				Addr:              addr,
				Role:              role,
				DiscoverySource:   discovery,
				VerificationState: verification,
				RegistrySlot:      regSlot,
			}, true
		}
	}
	return nil, false
}
