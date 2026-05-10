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

// RuntimeStateObserver is the runtime-state cache notification hook.
// AddressTableInserter calls it once per passive observation (new insert
// OR refresh of an existing slot) so the runtimestate.Manager can
// populate/refresh known_bus_members[] for M5 revalidation.
//
// observedAt is the bus-event timestamp; reportedSource is the
// runtimestate.LastSource string (e.g. "passive_observed").
//
// Production wiring lives in cmd/gateway/main.go, calling
// runtimeStateMgr.UpsertKnownBusMember. The hook is fired
// synchronously inside maybeInsert; observers MUST be cheap (the
// Manager's UpsertKnownBusMember is a brief mu-guarded field swap and
// is safe to call at bus-event rate).
type RuntimeStateObserver func(addr byte, observedAt time.Time, reportedSource string)

type AddressTableInserter struct {
	table              *AddressTable
	cfg                Config
	observationsByAddr map[byte]AddressObservation
	// enrichmentRefreshFn is called once per new passive slot insertion.
	// Wired to semanticPoller.EnqueueDiscoveryRefresh in production: the
	// next discovery cycle re-runs B524 root coherency on the controller.
	// Nil-safe: skipped when not wired (unit tests, etc.).
	enrichmentRefreshFn func()
	// enrichmentIdentityProbeFn is called once per new passive slot
	// insertion with the inserted address. Wired to
	// semanticPoller.EnqueueAddressIdentityProbe in production: probes
	// the new address with 0x07/0x04 + B5.09 ScanID and re-Registers
	// with full identity (Manufacturer + DeviceID + SerialNumber).
	// That triggers M6 identity-merge in helianthus-ebusreg, grouping
	// addresses with matching identity into a single DeviceEntry
	// (e.g. NETX3 0xF1 + 0xF6 + 0x04 + 0xFF all collapse into one
	// device once enrichment runs). Phase post-C P5 (live validation
	// 2026-05-08): without this hook, passive-observed entries stay
	// at empty manufacturer indefinitely. Nil-safe.
	enrichmentIdentityProbeFn func(addr byte)
	// runtimeStateObserver is called once per passive observation so
	// the runtimestate.Manager can populate/refresh known_bus_members[]
	// for M5_ADDRESS_TABLE_REVALIDATE. Nil-safe.
	runtimeStateObserver RuntimeStateObserver
}

func NewAddressTableInserter(table *AddressTable, cfg Config) *AddressTableInserter {
	return &AddressTableInserter{
		table:              table,
		cfg:                cfg,
		observationsByAddr: make(map[byte]AddressObservation),
	}
}

// SetEnrichmentRefreshFn wires the post-insertion B524 root re-discovery
// trigger so the regulator surface populates without a gateway restart.
// See AddressTableInserter.enrichmentRefreshFn for semantics.
func (i *AddressTableInserter) SetEnrichmentRefreshFn(fn func()) {
	if i == nil {
		return
	}
	i.enrichmentRefreshFn = fn
}

// SetEnrichmentIdentityProbeFn wires the post-insertion per-address
// identity probe so new passive slots get a 0x07/0x04 + B5.09 ScanID
// read against them. See AddressTableInserter.enrichmentIdentityProbeFn.
//
// Phase post-C P5 (live validation 2026-05-08): without this hook
// passive-observed entries (e.g. NETX3 0xF1↔0xF6) stay at empty
// manufacturer / deviceID / serialNumber forever, which prevents
// identity-merge from grouping aliased faces.
//
// This setter is fire-only: it installs the fn pointer and returns
// without invoking it. Callers that need to backfill addresses
// inserted before the hook was wired should call
// BackfillUnidentifiedAddresses AFTER the gateway's startup barrier
// (semantic scheduler readiness) clears — see main.go for the wiring.
func (i *AddressTableInserter) SetEnrichmentIdentityProbeFn(fn func(addr byte)) {
	if i == nil {
		return
	}
	i.enrichmentIdentityProbeFn = fn
}

// SetRuntimeStateObserver wires the runtime-state Manager hook. The
// inserter calls fn once per passive observation (new insert OR refresh
// of an existing slot) so known_bus_members[] gets populated and
// LastSeenAt stays current as a basis for M5 revalidation ordering.
// Codex P2 follow-up on PR #615 — without this wiring, runtime_state.json's
// known_bus_members[] stays empty after a fresh install and M5 has
// nothing to revalidate.
func (i *AddressTableInserter) SetRuntimeStateObserver(fn RuntimeStateObserver) {
	if i == nil {
		return
	}
	i.runtimeStateObserver = fn
}

// BackfillUnidentifiedAddresses iterates the registry's existing
// entries and invokes the wired enrichmentIdentityProbeFn for any
// address that looks unidentified (manufacturer empty OR Vaillant +
// serial empty). Used to close the race where a passive observation
// during the gateway's startup-admission window lands in the
// registry before SetEnrichmentIdentityProbeFn was wired.
//
// Callers MUST defer the invocation until the gateway's startup
// barrier (admittedSource finalization) has closed — otherwise the
// probe submissions will race the startup directed scan and emit
// bus traffic during the admission validation window. See P5
// round-3 finding (Codex P2 on PR #583, 2026-05-08).
//
// The probe fn is idempotent (per-address sync.Map in
// EnqueueAddressIdentityProbe), so repeat calls are harmless.
func (i *AddressTableInserter) BackfillUnidentifiedAddresses() {
	if i == nil || i.enrichmentIdentityProbeFn == nil || i.table == nil || i.table.reg == nil {
		return
	}
	fn := i.enrichmentIdentityProbeFn
	// P9.4 — race-free identity-field reads via IterateSnapshots.
	// Backfill runs once after the startup barrier closes, but the
	// passive inserter can be writing to the registry concurrently
	// (passive frames arriving on the bus). The previous Iterate
	// path read entry.Manufacturer / SerialNumber / Addresses
	// lock-free; the snapshot path reads value-typed copies under
	// the registry's RLock.
	i.table.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		manufacturer := snap.Manufacturer
		serial := snap.SerialNumber
		needsProbe := manufacturer == "" || (manufacturer == "Vaillant" && serial == "")
		if !needsProbe {
			return true
		}
		for _, addr := range snap.Addresses {
			fn(addr)
		}
		return true
	})
}

func (i *AddressTableInserter) OnPassiveClassifiedEvent(event PassiveClassifiedEvent) {
	if i == nil {
		return
	}
	// P4 (post-Phase-C live validation 2026-05-08): broadcast frames
	// have a separate insertion path because they have no ACK
	// correlation by construction (target == 0xFE). Vaillant's
	// spontaneous broadcasts (DCF time, energy totals, system status
	// from NETX3) carry the broadcaster's source byte that the
	// inserter must learn — without this branch, devices that emit
	// only broadcast traffic (e.g. NETX3's 0xFF face) never land in
	// the registry.
	if event.Kind == PassiveClassifiedEventBroadcastFrame {
		i.handleBroadcastFrameEvent(event)
		return
	}
	// P1 (post-Phase-C live validation 2026-05-08): only fully-
	// completed M-T transactions create registry slots via the
	// ACK-correlation path. Abandoned transactions in any phase —
	// particularly phase-3 no_response abandons that retain a
	// populated ACKCorrelation from the prior successful ACK
	// observation — must NOT promote their src/dst into the
	// registry. Live evidence: NETX3's identity-scan probes (e.g.
	// 0xF1 → 0x07/0x04 → 0x24 ACKed but no response) were inserting
	// phantom 0x24, 0x84, etc. as registry entries.
	//
	// This aligns with BusObservabilityStore.recordEvidenceFromEvent-
	// Locked which already excludes AbandonedTransaction events.
	if event.Kind != PassiveClassifiedEventTransaction {
		return
	}
	if !event.ACKCorrelation.CompleteRequest || event.ACKCorrelation.Correlator != PassiveACKCorrelatorM2A {
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

// handleBroadcastFrameEvent inserts the broadcaster's source byte
// as initiator. Phase post-C P4: broadcast frames (target == 0xFE)
// have no ACKCorrelation by construction — receivers don't ACK
// broadcasts in the eBUS protocol. Without this branch, devices
// that emit only broadcast traffic (e.g. NETX3 0xFF) would never
// be observed by the inserter, leaving the registry blind to them.
//
// Treats only fully-classified broadcast frames (HasRequest=true,
// Request.Target=0xFE). Doesn't insert the target (always 0xFE,
// the broadcast indicator), only the source.
//
// Same admittedSrc and self-source filtering as the M2A path.
func (i *AddressTableInserter) handleBroadcastFrameEvent(event PassiveClassifiedEvent) {
	if !event.HasRequest {
		return
	}
	if event.FrameType != protocol.FrameTypeBroadcast {
		return
	}
	if event.Request.Target != protocol.AddressBroadcast {
		return
	}
	srcAddr := event.Request.Source
	admittedSrc := i.admittedSource()
	if srcAddr == admittedSrc {
		return
	}
	if srcAddr == 0xFE || srcAddr == 0xAA {
		return
	}
	// P4 round-2 (Codex P2 follow-up 2026-05-08): gate broadcast
	// insertion on canonical-source membership. The established
	// passive-discovery contract treats broadcast sources as
	// presence-only ("not used as a discovery probe target", per
	// bus_observability_store.go:709-716 + passive_discovery_promoter
	// _test.go:155-158). A bus that emits routine broadcasts from
	// non-canonical addresses (e.g. 0x07 initiator-class non-canonical,
	// or 0x15 BASV2 target-class that broadcasts presence) must not get
	// registry slots from broadcast traffic alone — the existing
	// promoter contract requires later corroborating M-T transaction
	// evidence before registration.
	//
	// We only register broadcast sources that ARE canonical eBUS
	// initiators (per protocol.IsCanonicalSource). Concretely this
	// covers the NETX3 0xFF↔0x04 case (0xFF is a canonical source
	// in the v1 table) without leaking presence-only signal into
	// the registry for non-canonical broadcasters.
	if !protocol.IsCanonicalSource(srcAddr) {
		return
	}
	// Treat each broadcast emission from a canonical source as one
	// observation. Insert + alias only after 2× corroboration —
	// matches the M-T path's PositiveACKCount<2 gate so a single
	// stray frame can't promote a slot.
	observation := i.observationsByAddr[srcAddr]
	observation.PositiveACKCount++
	i.observationsByAddr[srcAddr] = observation
	if observation.PositiveACKCount < 2 {
		return
	}
	i.maybeInsert(srcAddr, "initiator", admittedSrc, event.ObservedAt)
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
	// Notify runtime-state on every passive observation (new insert OR
	// refresh of existing slot). The Manager.UpsertKnownBusMember call
	// behind this hook is a brief mu-guarded field swap and is safe to
	// call at bus-event rate; the persister batches writes on the
	// 15-min ticker. Codex P2 follow-up on PR #615.
	if i.runtimeStateObserver != nil {
		i.runtimeStateObserver(addr, observedAt, "passive_observed")
	}
	if _, exists := i.table.Lookup(addr); exists {
		// Already in registry (active scan, prior passive observation,
		// or static seed) — but still attempt canonical-pair aliasing
		// in case the companion was registered separately and never
		// went through the new-insert alias path. Idempotent.
		i.maybeAliasCanonicalCompanion(addr)
		// P5 round-9 (Codex P2 follow-up 2026-05-08): also re-fire
		// the identity probe on the existing-address path. The
		// probe fn is idempotent via sync.Map; this matters when a
		// previous probe attempt was rolled back due to queue
		// overload (EnqueueAddressIdentityProbe.Delete on submit
		// failure). Without this, a passive slot that hits the
		// queue-overload window stays anonymous until a gateway
		// restart, even though later passive observations of the
		// same address keep arriving.
		if i.enrichmentIdentityProbeFn != nil {
			i.enrichmentIdentityProbeFn(addr)
		}
		return
	}

	// P8 — atomic identity-merge + correct passive-observed label.
	//
	// Pre-P8 the inserter called Register(DeviceInfo{}) followed by
	// MarkSlotPassiveObserved. Register stamped the slot at
	// DiscoverySourceActiveConfirmed/VerificationStateIdentityConfirmed,
	// which the monotonic ladder then prevented MarkSlotPassiveObserved
	// from downgrading. Net: every passive insertion was misreported
	// as `active_confirmed` in operator surfaces. RegisterPassiveObserved
	// performs identity-merge AND the passive-label stamp atomically
	// under one r.mu acquisition with the correct
	// DiscoverySourcePassiveObserved/VerificationStateCorroborated
	// labels — see helianthus-ebusreg PR #138.
	var slotRole registry.SlotRole
	switch role {
	case "initiator":
		slotRole = registry.SlotRoleMaster
	case "target":
		slotRole = registry.SlotRoleSlave
	}
	i.table.reg.RegisterPassiveObserved(registry.DeviceInfo{Address: addr}, slotRole, observedAt)
	// Capture the slot pointer for the AddressTable's cached
	// `slots[addr].RegistrySlot` field. RegisterPassiveObserved already
	// attaches slot.Device, so this LookupSlot is purely for the
	// projection cache (Codex P8 ebusreg review NIT FINDING_3 — kept
	// intentionally to preserve the AddressTable.Lookup cached-slot
	// shape; no internal readers depend on it but external projections
	// in address_table.go do).
	registrySlot, _ := i.table.reg.LookupSlot(addr)

	tier, free := canonicalSlotMetadata(addr)
	// P8.2 — guard the slots map mutation with the AddressTable's
	// slotsMu so concurrent Lookup readers don't observe a partially-
	// constructed map state. Closes the pre-existing concurrent-map-
	// access surface flagged by Codex on PR #590.
	i.table.slotsMu.Lock()
	i.table.slots[addr] = &AddressSlot{
		Addr:              addr,
		Role:              role,
		DiscoverySource:   "passive_observed",
		VerificationState: "corroborated_pending",
		PriorityTier:      tier,
		FreeUse:           free,
		RegistrySlot:      registrySlot,
	}
	i.table.slotsMu.Unlock()

	// A.7c — runtime audit trail. Every passive insertion is logged at
	// INFO with addr/role/admitted-source/observation-time so operator
	// (and post-mortem diagnostics) can correlate registry growth with
	// specific passive frames. Closes the visibility gap behind the
	// 0x38 false-positive forensics from Grafana cross-check.
	log.Printf("address_table_inserter insert addr=0x%02X role=%s admitted_src=0x%02X observed_at=%s",
		addr, role, admittedSrc, observedAt.Format(time.RFC3339Nano))

	// M6 enrichment trigger — schedule a semantic-poller discovery
	// refresh so the regulator surface populates without a gateway
	// restart. Note: this is a B524-root coherency re-run, NOT an
	// identity probe of the just-inserted address (see
	// enrichmentIdentityProbeFn below for the latter).
	if i.enrichmentRefreshFn != nil {
		i.enrichmentRefreshFn()
	}

	// A.7b — canonical-pair aliasing. If addr is one half of a canonical
	// pair from the docs-owned eBUS standard table (sourceAddressTableV1)
	// and the other half is already in the registry, alias them into a
	// single DeviceEntry so MCP/GraphQL queries return one device with
	// two addresses. Aliasing only fires for the 25 canonical pairs;
	// non-canonical neighbours (e.g. 0x26 / 0xEC) are never aliased here.
	i.maybeAliasCanonicalCompanion(addr)

	// P5 (post-Phase-C live validation 2026-05-08): per-address
	// identity probe of the just-inserted slot. Triggers a
	// 0x07/0x04 + B5.09 ScanID read against the responder face;
	// on success the registry's M6 identity-merge path collapses
	// canonical pairs that share identity into a single DeviceEntry.
	//
	// MUST run AFTER maybeAliasCanonicalCompanion so that when the
	// just-inserted address has a canonical companion already in
	// the registry, the alias merge happens first and the
	// EnqueueAddressIdentityProbe lookup sees the merged entry
	// (with its target-role face) — not a fresh initiator-only
	// entry that would resolve to no_responder_face. (Codex P2
	// round-7 finding 2026-05-08 on PR #583.)
	//
	// Without this hook, passive-observed entries (e.g. NETX3
	// 0xF1↔0xF6) stay at empty manufacturer / deviceID /
	// serialNumber forever, which prevents identity-merge from
	// grouping aliased faces. The probe is bounded + idempotent
	// per the wired implementation (see semanticPoller.
	// EnqueueAddressIdentityProbe).
	if i.enrichmentIdentityProbeFn != nil {
		i.enrichmentIdentityProbeFn(addr)
	}
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
	if primaryOk && primaryAddr.PrimaryDisplayAddress() == entryAddr.PrimaryDisplayAddress() {
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
