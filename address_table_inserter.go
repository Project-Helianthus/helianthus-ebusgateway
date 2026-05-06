package ebusgateway

import (
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type AddressObservation struct {
	PositiveACKCount int
}

type AddressTableInserter struct {
	table              *AddressTable
	cfg                Config
	observationsByAddr map[byte]AddressObservation
}

func NewAddressTableInserter(table *AddressTable, cfg Config) *AddressTableInserter {
	return &AddressTableInserter{
		table:              table,
		cfg:                cfg,
		observationsByAddr: make(map[byte]AddressObservation),
	}
}

func (i *AddressTableInserter) OnPassiveClassifiedEvent(event PassiveClassifiedEvent) {
	if i == nil || !event.ACKCorrelation.CompleteRequest || event.ACKCorrelation.Correlator != PassiveACKCorrelatorM2A {
		return
	}
	corr := event.ACKCorrelation
	if corr.Position != PassiveACKPositionRequestACK {
		return
	}

	srcAddr := event.Request.Source
	dstAddr := event.Request.Target
	admittedSrc := i.admittedSource()

	if corr.Byte == protocol.SymbolNack {
		return
	}
	if corr.Byte != protocol.SymbolAck {
		return
	}

	if srcAddr == 0xFF {
		i.maybeInsert(0xFF, "master", admittedSrc, event.ObservedAt)
		return
	}
	if srcAddr == admittedSrc {
		return
	}

	i.maybeInsert(dstAddr, "slave", admittedSrc, event.ObservedAt)
	observation := i.observationsByAddr[srcAddr]
	observation.PositiveACKCount++
	i.observationsByAddr[srcAddr] = observation

	if observation.PositiveACKCount < 2 {
		return
	}
	if companion, ok := protocol.Companion(srcAddr); ok {
		i.maybeInsert(companion, "slave", admittedSrc, event.ObservedAt)
	}
}

func (i *AddressTableInserter) admittedSource() byte {
	if i == nil {
		return 0
	}
	if i.cfg.AdmittedSource != nil {
		return i.cfg.AdmittedSource()
	}
	return i.cfg.ScanSource
}

func (i *AddressTableInserter) maybeInsert(addr byte, role string, admittedSrc byte, observedAt time.Time) {
	if i == nil || i.table == nil || i.table.reg == nil {
		return
	}
	if addr == admittedSrc || addr == 0xFE {
		return
	}
	if _, exists := i.table.Lookup(addr); exists {
		return
	}

	i.table.reg.Register(registry.DeviceInfo{Address: addr})
	registrySlot, _ := i.table.reg.LookupSlot(addr)
	if registrySlot != nil {
		registrySlot.DiscoverySource = registry.DiscoverySourcePassiveObserved
		registrySlot.VerificationState = registry.VerificationStateCorroborated
		switch role {
		case "master":
			registrySlot.Role = registry.SlotRoleMaster
		case "slave":
			registrySlot.Role = registry.SlotRoleSlave
		}
		if registrySlot.FirstObservedAt.IsZero() {
			registrySlot.FirstObservedAt = observedAt
		}
		registrySlot.LastObservedAt = observedAt
	}

	i.table.slots[addr] = &AddressSlot{
		Addr:              addr,
		Role:              role,
		DiscoverySource:   "passive_observed",
		VerificationState: "corroborated_pending",
		RegistrySlot:      registrySlot,
	}
}
