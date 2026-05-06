package ebusgateway

import (
	"log"
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
		i.maybeInsert(0xFF, "initiator", admittedSrc, event.ObservedAt)
		return
	}
	if srcAddr == admittedSrc {
		return
	}

	// Insert request src as initiator (AD05 Address Eligibility:
	// "request src MAY create a slot if it is not the gateway's own
	// admitted source"). PR #564 missed this — observed-only initiators
	// like NETX3 0xF1 never landed in the registry passively
	// even though their traffic was seen on the bus.
	i.maybeInsert(srcAddr, "initiator", admittedSrc, event.ObservedAt)

	i.maybeInsert(dstAddr, "target", admittedSrc, event.ObservedAt)
	observation := i.observationsByAddr[srcAddr]
	observation.PositiveACKCount++
	i.observationsByAddr[srcAddr] = observation

	if observation.PositiveACKCount < 2 {
		return
	}
	if companion, ok := protocol.Companion(srcAddr); ok {
		i.maybeInsert(companion, "target", admittedSrc, event.ObservedAt)
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
		// Already in registry (active scan, prior passive observation,
		// or static seed) — but still attempt canonical-pair aliasing
		// in case the companion was registered separately and never
		// went through the new-insert alias path. Idempotent.
		i.maybeAliasCanonicalCompanion(addr)
		return
	}

	i.table.reg.Register(registry.DeviceInfo{Address: addr})
	registrySlot, _ := i.table.reg.LookupSlot(addr)
	if registrySlot != nil {
		registrySlot.DiscoverySource = registry.DiscoverySourcePassiveObserved
		registrySlot.VerificationState = registry.VerificationStateCorroborated
		switch role {
		case "initiator":
			registrySlot.Role = registry.SlotRoleMaster
		case "target":
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

	// A.7c — runtime audit trail. Every passive insertion is logged at
	// INFO with addr/role/admitted-source/observation-time so operator
	// (and post-mortem diagnostics) can correlate registry growth with
	// specific passive frames. Closes the visibility gap behind the
	// 0x38 false-positive forensics from Grafana cross-check.
	log.Printf("address_table_inserter insert addr=0x%02X role=%s admitted_src=0x%02X observed_at=%s",
		addr, role, admittedSrc, observedAt.Format(time.RFC3339Nano))

	// A.7b — canonical-pair aliasing. If addr is one half of a canonical
	// pair from the docs-owned eBUS standard table (sourceAddressTableV1)
	// and the other half is already in the registry, alias them into a
	// single DeviceEntry so MCP/GraphQL queries return one device with
	// two addresses. Aliasing only fires for the 25 canonical pairs;
	// non-canonical neighbours (e.g. 0x26 / 0xEC) are never aliased here.
	i.maybeAliasCanonicalCompanion(addr)
}

// maybeAliasCanonicalCompanion calls registry.AliasAddresses(addr, companion)
// when addr is one half of a canonical eBUS source/companion pair AND the
// other half is already present in the wrapped registry. Idempotent.
func (i *AddressTableInserter) maybeAliasCanonicalCompanion(addr byte) {
	if i == nil || i.table == nil || i.table.reg == nil {
		return
	}
	companion, ok := protocol.Companion(addr)
	if !ok {
		return
	}
	entryAddr, exists := i.table.reg.Lookup(companion)
	if !exists {
		return
	}
	// Only attempt the alias if the two addresses still resolve to
	// distinct DeviceEntries; otherwise it's already aliased and we
	// don't need to log noise on every observation of the pair.
	primaryAddr, primaryOk := i.table.reg.Lookup(addr)
	if primaryOk && primaryAddr.Address() == entryAddr.Address() {
		return
	}
	if err := i.table.reg.AliasAddresses(addr, companion); err != nil {
		log.Printf("address_table_inserter alias_failed addr=0x%02X companion=0x%02X err=%v",
			addr, companion, err)
		return
	}
	log.Printf("address_table_inserter alias addr=0x%02X companion=0x%02X",
		addr, companion)
}
