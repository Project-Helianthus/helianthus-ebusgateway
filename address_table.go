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
	slot, ok := t.slots[addr]
	if !ok || slot == nil {
		return nil, false
	}
	return slot, true
}
