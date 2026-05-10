package main

import (
	"context"
	"expvar"
	"log"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// revalidationRotation tracks which cached addresses have been probed in
// the current rotation round. This is in-process state (deliberately not
// persisted) — a process restart re-reads runtime_state.json and starts
// fresh with all members eligible.
//
// Rotation rule (Codex P2 follow-up on PR #615): the Revalidator
// orders members by last_seen_at DESC, so without rotation the
// MOST-RECENTLY-ACTIVE 32 members would be probed every cycle and
// the older 32 (which got postponed in cycle 1) would never reach the
// probe window. The rotation excludes already-probed members from
// subsequent cycles until ALL members have been probed once; then the
// round resets and the next 15-min cycle starts a fresh rotation.
//
// "Probed" here means OutcomeResponder OR OutcomeNoReply — outcomes
// where the bus was actually touched. OutcomeSkippedPassiveRefresh
// does NOT advance the rotation; postponements (cap / ctx-abort)
// don't either, by definition.
type revalidationRotation struct {
	mu     sync.Mutex
	probed map[byte]struct{}
}

func newRevalidationRotation() *revalidationRotation {
	return &revalidationRotation{probed: make(map[byte]struct{})}
}

// shouldSkip reports whether addr has already been probed in the current
// rotation round.
func (r *revalidationRotation) shouldSkip(addr byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.probed[addr]
	return ok
}

// markProbed records that addr was probed (responder or no_reply) in the
// current rotation round.
func (r *revalidationRotation) markProbed(addr byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probed[addr] = struct{}{}
}

// resetIfRoundComplete returns true and clears the round if every cached
// addr in the supplied list is already probed (i.e. nothing left to
// rotate to). Caller invokes BEFORE filtering so the next cycle sees a
// fresh round.
func (r *revalidationRotation) resetIfRoundComplete(addrs []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(addrs) == 0 {
		return false
	}
	for _, a := range addrs {
		if _, ok := r.probed[a]; !ok {
			return false
		}
	}
	r.probed = make(map[byte]struct{})
	return true
}

// revalidateOutcomesTotal is the M5_ADDRESS_TABLE_REVALIDATE per-outcome
// counter exposed via /debug/vars. Labels: responder, no_reply,
// skipped_passive_refresh.
//
// Plan: runtime-state-w19-26.locked M5(d)+(e). Naming follows existing
// helianthus_runtime_state_* prefix for cross-product consistency.
var revalidateOutcomesTotal = expvar.NewMap("ebus_runtime_state_revalidate_total")

// revalidateRuntimeStateMembers triggers one revalidation cycle against the
// cached known_bus_members[] now that admission warmup has completed.
//
// On responder outcome: refresh confidence=verified + last_source=directed_07_04
// via Manager.UpsertKnownBusMember (the address-table inserter normal path
// will pick up identity refinement separately via passive observation).
// On no_reply outcome: AD23 immediate eviction via Manager.EvictKnownBusMember.
// On skipped_passive_refresh: no cache mutation (member already refreshed
// by warmup-window passive traffic via the existing inserter path).
//
// Returns synchronously after one cycle. Caller (the activeProbePassed
// goroutine in main.run) decides whether to schedule a follow-up cycle
// for postponed members; the current implementation lets the next
// 15-min Manager.Start tick handle them naturally. (TODO: explicit
// next-cycle schedule for the postponed list to honor M5 acceptance
// "synthetic test with 64 cached members ... next cycle probes them"
// without waiting for the periodic ticker.)
func revalidateRuntimeStateMembers(
	ctx context.Context,
	mgr *runtimestate.Manager,
	gw *ebusgateway.Gateway,
	cfg ebusgateway.Config,
	admittedSource byte,
	rotation *revalidationRotation,
) runtimestate.Result {
	if mgr == nil || gw == nil || gw.Bus == nil || gw.Registry == nil {
		return runtimestate.Result{}
	}
	state := mgr.State()
	if state == nil || state.EBus == nil || len(state.EBus.KnownBusMembers) == 0 {
		return runtimestate.Result{}
	}

	// Rotation: probe only members that haven't been probed yet in the
	// current round. When all cached addrs have been probed once, reset
	// and start a fresh round so re-emerging activity gets re-validated.
	// (Codex P2 follow-up on PR #615 — the Revalidator's
	// last_seen_at-DESC ordering would otherwise repeatedly probe the
	// same newest 32 members, starving postponed older members.)
	allAddrs := make([]byte, 0, len(state.EBus.KnownBusMembers))
	for _, m := range state.EBus.KnownBusMembers {
		allAddrs = append(allAddrs, m.Addr)
	}
	if rotation != nil {
		if rotation.resetIfRoundComplete(allAddrs) {
			log.Printf("runtime_state revalidate: rotation round complete (%d members probed); starting new round", len(allAddrs))
		}
	}
	eligibleMembers := state.EBus.KnownBusMembers
	if rotation != nil {
		eligibleMembers = make([]runtimestate.KnownBusMember, 0, len(state.EBus.KnownBusMembers))
		for _, m := range state.EBus.KnownBusMembers {
			if rotation.shouldSkip(m.Addr) {
				continue
			}
			eligibleMembers = append(eligibleMembers, m)
		}
		if len(eligibleMembers) == 0 {
			// All cached addrs have been probed; nothing eligible
			// this cycle. Will rotate fresh on the next tick.
			return runtimestate.Result{}
		}
	}

	// Respect the caller's ScanRequestTimeout per-probe so a stuck
	// responder can't block the entire 32-probe burst.
	probeBus := &timeoutBus{bus: gw.Bus, timeout: cfg.ScanRequestTimeout}

	// Sub-context lets the probe abort the cycle on transport-level
	// errors. Without this, ScanDirected returning a non-nil error for
	// a bus / arbitration / adapter-disconnect failure would map to
	// `false` → OutcomeNoReply → AD23 eviction, deleting valid cached
	// members during a transient bus failure (Codex P2 follow-up on
	// PR #615). On cancellation the Revalidator's existing ctx.Err()
	// recheck postpones the rest of the cycle for retry next tick.
	probeCtx, cancelProbe := context.WithCancel(ctx)
	defer cancelProbe()

	probe := func(_ context.Context, target byte) bool {
		entries, err := registry.ScanDirected(probeCtx, probeBus, gw.Registry, admittedSource, []byte{target})
		if err != nil {
			// Transport / arbitration / adapter-level failure.
			// Cancel the sub-context so the Revalidator's
			// post-probe ctx.Err() recheck postpones this member +
			// the rest of the cycle. Returning false on a
			// healthy bus would have meant "no reply"; with ctx
			// cancelled it's interpreted as "cycle aborted".
			//
			// We log here rather than letting the failure stay
			// silent — the cycle abort is observable but the
			// reason should be too.
			log.Printf("runtime_state revalidate: ScanDirected target=0x%02X failed (cycle aborted, members postponed): %v", target, err)
			cancelProbe()
			return false
		}
		// Responder iff the scan returned an entry whose addresses
		// include the probed target. ScanDirected only sends 07 04 to
		// the requested target, so any non-empty result with this
		// address is a confirmed responder.
		for _, e := range entries {
			for _, addr := range e.Addresses() {
				if addr == target {
					return true
				}
			}
		}
		return false
	}

	revalidator := &runtimestate.Revalidator{
		Probe: probe,
		// PassivelyRefreshedFn intentionally nil for now: the Revalidator
		// always probes. Wiring busObservability evidence as the
		// passive-refresh signal is a follow-up — leaving it nil today
		// is conservative (more probes, but no missed evictions).
		Counter: revalidateCounterAdapter{},
	}

	result := revalidator.Run(probeCtx, eligibleMembers)

	// Build an addr→cached entry index so responder refreshes can MERGE
	// presence metadata with existing identity/companion_addr instead of
	// replacing them. Manager.UpsertKnownBusMember is replace-by-addr,
	// not merge, so a sparse refresh entry would drop cached
	// Identity/CompanionAddr until the next inserter enrichment fires.
	// (Codex P2 follow-up on PR #615.)
	cachedByAddr := make(map[byte]runtimestate.KnownBusMember, len(state.EBus.KnownBusMembers))
	for _, m := range state.EBus.KnownBusMembers {
		cachedByAddr[m.Addr] = m
	}

	for _, p := range result.Probed {
		switch p.Outcome {
		case runtimestate.OutcomeResponder:
			refreshed := cachedByAddr[p.Addr] // zero value when address wasn't cached
			refreshed.Addr = p.Addr
			// LastSeenAt deliberately preserved from the prior entry —
			// updating it would create sticky last_seen_at-DESC
			// ordering that prevents postponed older members from
			// reaching the probe window on the next cycle (Codex P2
			// follow-up on PR #615). Passive observation of bus
			// traffic is the authoritative LastSeenAt updater; a
			// directed-probe responder confirms presence without
			// implying new activity.
			refreshed.LastSource = runtimestate.LastSourceDirected0704
			refreshed.Confidence = runtimestate.ConfidenceVerified
			// Identity / CompanionAddr preserved from prior entry. The
			// address-table-inserter normal path will refine identity
			// independently when richer data arrives via passive
			// observation.
			mgr.UpsertKnownBusMember(refreshed)
			if rotation != nil {
				rotation.markProbed(p.Addr)
			}
		case runtimestate.OutcomeNoReply:
			mgr.EvictKnownBusMember(p.Addr)
			if rotation != nil {
				rotation.markProbed(p.Addr)
			}
		case runtimestate.OutcomeSkippedPassiveRefresh:
			// No-op: member already refreshed via warmup-window passive
			// observation; the address-table inserter normal path is
			// authoritative. Also DOES NOT advance the rotation —
			// passive-refreshed members weren't bus-touched, so they
			// still need a chance at the probe window in this round.
		}
	}

	if len(result.Postponed) > 0 {
		log.Printf("runtime_state revalidate: %d cached members postponed (cap=%d, next cycle will resume)",
			len(result.Postponed), runtimestate.RevalidationCap)
	}
	return result
}

// startRuntimeStateRevalidator runs an immediate revalidation cycle and
// then schedules periodic cycles at the supplied cadence (defaults to
// 15 min when interval ≤ 0). Each cycle reads the current member list
// from Manager.State so postponed members from earlier cycles are
// naturally re-included on the next tick — no separate Postponed
// queue is needed. (Codex P2 follow-up on PR #615 — without periodic
// cycles, postponed members beyond cap would stay stale until process
// restart.)
//
// Returns immediately; the cycle goroutine exits on ctx cancellation.
func startRuntimeStateRevalidator(
	ctx context.Context,
	mgr *runtimestate.Manager,
	gw *ebusgateway.Gateway,
	cfg ebusgateway.Config,
	admittedSource byte,
	interval time.Duration,
) {
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	rotation := newRevalidationRotation()
	go func() {
		// First cycle runs immediately so cached responders refresh
		// confidence within ~5 s of admission per M5 acceptance.
		revalidateRuntimeStateMembers(ctx, mgr, gw, cfg, admittedSource, rotation)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				revalidateRuntimeStateMembers(ctx, mgr, gw, cfg, admittedSource, rotation)
			}
		}
	}()
}

// revalidateCounterAdapter bridges runtimestate.RevalidationOutcome to the
// expvar map. Each Inc adds 1 to the per-outcome key.
type revalidateCounterAdapter struct{}

// Inc satisfies runtimestate.RevalidationCounter.
func (revalidateCounterAdapter) Inc(o runtimestate.RevalidationOutcome) {
	revalidateOutcomesTotal.Add(string(o), 1)
}

// finalizeRuntimeStateForAdmittedSource performs the M4 ebus.self write-back
// and starts the M5 periodic revalidator for any admitted source — whether
// it came from a SourceAddressSelector warmup, an explicit operator
// override, an ebusd-tcp fallback, or any other path that finalizes
// builder.SetAdmittedMutationSource. Codex P2 follow-up on PR #615
// (without this, override and static-fallback admissions skipped the
// runtime-state finalization entirely and cached known_bus_members[]
// stayed stale indefinitely on those transports).
//
// hasCompanion gates whether companion is written into ebus.self.
// Synchronous static-path admissions don't have a SourceAddressSelection
// result and therefore no companion to record (CompanionTarget remains
// nil per AD03 — the Manager preserves nil for "no valid companion per
// bit-pattern rule" rather than synthesising one).
func finalizeRuntimeStateForAdmittedSource(
	ctx context.Context,
	mgr *runtimestate.Manager,
	gw *ebusgateway.Gateway,
	cfg ebusgateway.Config,
	admittedSource byte,
	companion byte,
	selectionMethod runtimestate.SelectionMethod,
	hasCompanion bool,
) {
	self := runtimestate.Self{
		LastAdmittedSource: admittedSource,
		LastAdmittedAt:     time.Now().UTC(),
		SelectionMethod:    selectionMethod,
	}
	if hasCompanion {
		self.CompanionTarget = &companion
	}
	mgr.UpdateSelf(self)
	startRuntimeStateRevalidator(ctx, mgr, gw, cfg, admittedSource, 0)
}
