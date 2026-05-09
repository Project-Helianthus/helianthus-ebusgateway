package ebusgateway

import (
	"sync"

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

// CanonicalAddressView is a snapshot of one canonical eBUS address as seen
// by the AddressTable: the eBUS standard table row metadata (tier, role,
// free-use, peer) combined with this address's runtime observation state.
//
// Observation state distinguishes:
//   - "never_observed": the address is in the canonical table but no
//     passive ACK / active scan has placed it in the registry.
//   - "passive_observed": placed by AD05 inserter from passive frames.
//   - "static_seed": placed by EnableStaticSeedTable.
//   - "active_confirmed": placed by startup scan / probe.
//
// This view exists so MCP/GraphQL consumers can distinguish "this canonical
// address has not yet emitted traffic" from "this canonical address is not
// real on this bus" — important for A.7e audit + operator UX.
type CanonicalAddressView struct {
	Address           byte
	Role              string // "initiator" or "target"
	PeerAddress       byte
	PriorityTier      protocol.SourceAddressPriorityIndex
	FreeUse           bool
	Description       string
	Observed          bool
	DiscoverySource   string
	VerificationState string
}

// CanonicalAddressTableSnapshot returns a snapshot of all 50 canonical eBUS
// addresses (25 sources + 25 companions per
// architecture/ebus_standard/12-source-address-table.md), with each row
// labelled by its current runtime observation state. Addresses present in
// the wrapped registry are marked Observed=true with their actual
// DiscoverySource; addresses not yet observed are marked Observed=false
// with DiscoverySource="never_observed".
func (t *AddressTable) CanonicalAddressTableSnapshot() []CanonicalAddressView {
	if t == nil {
		return nil
	}
	rows := protocol.SourceAddressTableRows()
	out := make([]CanonicalAddressView, 0, len(rows)*2)
	for _, row := range rows {
		out = append(out,
			t.canonicalAddressView(row.Source, "initiator", row.Companion, string(row.CanonicalDescription), row.PriorityIndex, row.FreeUse),
			t.canonicalAddressView(row.Companion, "target", row.Source, string(row.CanonicalDescription), row.PriorityIndex, row.FreeUse),
		)
	}
	return out
}

func (t *AddressTable) canonicalAddressView(addr byte, role string, peer byte, desc string, tier protocol.SourceAddressPriorityIndex, free bool) CanonicalAddressView {
	view := CanonicalAddressView{
		Address:           addr,
		Role:              role,
		PeerAddress:       peer,
		PriorityTier:      tier,
		FreeUse:           free,
		Description:       desc,
		DiscoverySource:   "never_observed",
		VerificationState: "never_observed",
	}
	if slot, ok := t.Lookup(addr); ok && slot != nil {
		view.Observed = true
		if slot.DiscoverySource != "" {
			view.DiscoverySource = slot.DiscoverySource
		}
		if slot.VerificationState != "" {
			view.VerificationState = slot.VerificationState
		}
	}
	return view
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

// projectDiscoverySourceLabel maps the registry's DiscoverySource enum
// to the AddressTable's externally-visible string label. Exposed at
// package scope so both Lookup branches (cached + registry-fallback)
// share the same projection — preventing drift between the two paths.
func projectDiscoverySourceLabel(source registry.DiscoverySource) string {
	switch source {
	case registry.DiscoverySourcePassiveObserved:
		return "passive_observed"
	case registry.DiscoverySourceStaticSeed:
		return "static_seed"
	case registry.DiscoverySourceActiveConfirmed:
		return "active_confirmed"
	}
	return ""
}

// projectVerificationStateLabel mirrors projectDiscoverySourceLabel for
// the VerificationState enum.
func projectVerificationStateLabel(state registry.VerificationState) string {
	switch state {
	case registry.VerificationStateCandidate:
		return "candidate"
	case registry.VerificationStateCorroborated:
		return "corroborated_pending"
	case registry.VerificationStateIdentityConfirmed:
		return "identity_confirmed"
	}
	return ""
}

type AddressTable struct {
	reg *registry.DeviceRegistry

	// slotsMu guards the slots map for concurrent access between the
	// AddressTableInserter (which writes via OnPassiveClassifiedEvent
	// from the passive reconstructor's goroutine) and Lookup readers
	// (MCP / GraphQL / address-table snapshot consumers, all on
	// distinct goroutines). P8.2 — close the pre-existing
	// concurrent-map-access surface flagged by Codex on PR #590.
	// RWMutex over plain Mutex because reads dominate writes (passive
	// inserts are bounded by new-address-discovery rate; Lookup runs
	// per MCP query / GraphQL request).
	slotsMu sync.RWMutex
	slots   map[byte]*AddressSlot
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
	// P8.2 — slotsMu RLock guards the t.slots map read against the
	// inserter's concurrent map write at address_table_inserter.go.
	// Pre-P8.2 these reads were unsynced — Go's runtime detects
	// concurrent map writes (panic) but read+write produces a torn
	// read that the race detector also flags. The cached slot's
	// label fields are then reprojected from a registry snapshot
	// (P8.1) outside the local lock.
	t.slotsMu.RLock()
	cached, hit := t.slots[addr]
	t.slotsMu.RUnlock()

	// P8.1 — race-free reprojection via LookupSlotSnapshot.
	//
	// Both branches below project DiscoverySource / VerificationState
	// from a value-typed AddressSlotSnapshot taken under r.mu.RLock.
	// Pre-P8.1 these fields were read lock-free through a live
	// AddressSlot pointer (worked in practice because the underlying
	// enums are int-typed and atomic on Go's supported architectures,
	// but the race detector flags it under sufficient contention).
	// LookupSlotSnapshot's lock-protected value copy eliminates that
	// race surface — registry writers must acquire r.mu.Lock() which
	// blocks behind LookupSlotSnapshot's RLock before mutating slot
	// fields.
	if hit && cached != nil {
		if t.reg != nil {
			if snap, snapOK := t.reg.LookupSlotSnapshot(addr); snapOK {
				view := *cached
				view.DiscoverySource = projectDiscoverySourceLabel(snap.DiscoverySource)
				view.VerificationState = projectVerificationStateLabel(snap.VerificationState)
				return &view, true
			}
		}
		return cached, true
	}
	// Consult the wrapped registry so static seeds and active-discovered
	// devices are visible here, preventing maybeInsert from rewriting
	// their metadata (Codex P2: preserve-existing-registry-slots).
	if t.reg != nil {
		if snap, ok := t.reg.LookupSlotSnapshot(addr); ok {
			role := ""
			switch snap.Role {
			case registry.SlotRoleMaster:
				role = "initiator"
			case registry.SlotRoleSlave:
				role = "target"
			}
			tier, free := canonicalSlotMetadata(addr)
			// RegistrySlot pointer is still surfaced for fields not
			// covered by the snapshot (e.g. FirstObservedAt /
			// LastObservedAt are in the snapshot, but external
			// projection consumers may walk to slot.Device for
			// identity). Use LookupSlot (not LookupSlotSnapshot
			// alone) because the AddressSlot field expects a
			// pointer; the snapshot already protected the
			// race-prone enum reads.
			var regSlot *registry.AddressSlot
			if liveSlot, ok := t.reg.LookupSlot(addr); ok {
				regSlot = liveSlot
			}
			return &AddressSlot{
				Addr:              addr,
				Role:              role,
				DiscoverySource:   projectDiscoverySourceLabel(snap.DiscoverySource),
				VerificationState: projectVerificationStateLabel(snap.VerificationState),
				PriorityTier:      tier,
				FreeUse:           free,
				RegistrySlot:      regSlot,
			}, true
		}
	}
	return nil, false
}
