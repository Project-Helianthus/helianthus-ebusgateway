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

	probe := func(probeCtx context.Context, target byte) bool {
		entries, err := registry.ScanDirected(probeCtx, probeBus, gw.Registry, admittedSource, []byte{target})
		if err != nil {
			// Errors here include context cancellation (Revalidator
			// will recheck ctx.Err() and postpone), or transport
			// failures that we treat as "no_reply" since the device
			// effectively didn't answer this cycle.
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

	result := revalidator.Run(ctx, state.EBus.KnownBusMembers)

	now := time.Now().UTC()
	for _, p := range result.Probed {
		switch p.Outcome {
		case runtimestate.OutcomeResponder:
			mgr.UpsertKnownBusMember(runtimestate.KnownBusMember{
				Addr:       p.Addr,
				LastSeenAt: now,
				LastSource: runtimestate.LastSourceDirected0704,
				Confidence: runtimestate.ConfidenceVerified,
			})
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

// revalidateCounterAdapter bridges runtimestate.RevalidationOutcome to the
// expvar map. Each Inc adds 1 to the per-outcome key.
type revalidateCounterAdapter struct{}

// Inc satisfies runtimestate.RevalidationCounter.
func (revalidateCounterAdapter) Inc(o runtimestate.RevalidationOutcome) {
	revalidateOutcomesTotal.Add(string(o), 1)
}
