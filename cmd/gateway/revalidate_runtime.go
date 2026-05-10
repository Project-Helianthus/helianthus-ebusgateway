package main

import (
	"context"
	"expvar"
	"log"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

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
) runtimestate.Result {
	if mgr == nil || gw == nil || gw.Bus == nil || gw.Registry == nil {
		return runtimestate.Result{}
	}
	state := mgr.State()
	if state == nil || state.EBus == nil || len(state.EBus.KnownBusMembers) == 0 {
		return runtimestate.Result{}
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

	result := revalidator.Run(probeCtx, state.EBus.KnownBusMembers)

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

	now := time.Now().UTC()
	for _, p := range result.Probed {
		switch p.Outcome {
		case runtimestate.OutcomeResponder:
			refreshed := cachedByAddr[p.Addr] // zero value when address wasn't cached
			refreshed.Addr = p.Addr
			refreshed.LastSeenAt = now
			refreshed.LastSource = runtimestate.LastSourceDirected0704
			refreshed.Confidence = runtimestate.ConfidenceVerified
			// Identity / CompanionAddr preserved from prior entry. The
			// address-table-inserter normal path will refine identity
			// independently when richer data arrives via passive
			// observation.
			mgr.UpsertKnownBusMember(refreshed)
		case runtimestate.OutcomeNoReply:
			mgr.EvictKnownBusMember(p.Addr)
		case runtimestate.OutcomeSkippedPassiveRefresh:
			// No-op: member already refreshed via warmup-window passive
			// observation; the address-table inserter normal path is
			// authoritative.
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
	go func() {
		// First cycle runs immediately so cached responders refresh
		// confidence within ~5 s of admission per M5 acceptance.
		revalidateRuntimeStateMembers(ctx, mgr, gw, cfg, admittedSource)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				revalidateRuntimeStateMembers(ctx, mgr, gw, cfg, admittedSource)
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
