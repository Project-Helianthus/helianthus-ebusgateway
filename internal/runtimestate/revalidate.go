package runtimestate

import (
	"context"
	"sort"
)

// RevalidationOutcome classifies the result of a single member revalidation
// per the runtime-state-w19-26.locked M5_ADDRESS_TABLE_REVALIDATE telemetry
// label set.
type RevalidationOutcome string

const (
	// OutcomeResponder: directed 07 04 produced a responder identification.
	// Caller should refresh confidence=verified, last_source=directed_07_04,
	// last_seen_at=now via Manager.UpsertKnownBusMember and let the
	// address-table-inserter normal path enrich identity.
	OutcomeResponder RevalidationOutcome = "responder"
	// OutcomeNoReply: directed 07 04 timed out / NACKed. Per AD23 the
	// caller MUST evict the member immediately via Manager.EvictKnownBusMember
	// and increment the counter.
	OutcomeNoReply RevalidationOutcome = "no_reply"
	// OutcomeSkippedPassiveRefresh: member was passively re-observed during
	// warmup, so the directed probe was skipped (M5 constraint (a)). No
	// cache mutation; counter increments for observability.
	OutcomeSkippedPassiveRefresh RevalidationOutcome = "skipped_passive_refresh"
)

// RevalidationCap is the per-cycle maximum number of directed probes the
// gateway will issue (M5 constraint (c)). Rationale: ~84 ms wall-clock per
// 07 04 probe under contention → 32 probes ~ 2.7 s peak burst, well under
// SourceAddressSelector warmup tail latency.
const RevalidationCap = 32

// RevalidationProbeFn issues a directed 07 04 probe against the target
// address and returns true on responder identification, false on no-reply
// (timeout / NACK / collision after retries). Errors that are not "probe
// outcome" semantics (context cancellation, transport failure) MUST be
// surfaced through ctx.Err()/panic — Revalidator does not consult them.
type RevalidationProbeFn func(ctx context.Context, target byte) bool

// PassivelyRefreshedFn returns true when the given address was already
// re-observed during warmup. Members for which it returns true are counted
// as skipped_passive_refresh and DO NOT consume cap slots — the cap
// applies exclusively to bus-touching probes.
type PassivelyRefreshedFn func(addr byte) bool

// RevalidationCounter is the metric hook for outcome telemetry. Wired to a
// Prometheus counter labelled
// `ebus_runtime_state_revalidate_total{outcome=...}`.
type RevalidationCounter interface {
	Inc(outcome RevalidationOutcome)
}

// NopRevalidationCounter is a no-op metric hook for tests.
type NopRevalidationCounter struct{}

// Inc satisfies RevalidationCounter.
func (NopRevalidationCounter) Inc(RevalidationOutcome) {}

// Revalidator probes cached known_bus_members[] after admission warmup
// completes. It refreshes presence for live members (responder → caller
// upserts confidence=verified) and evicts ghosts (no_reply → caller calls
// EvictKnownBusMember per AD23).
//
// The Revalidator is pure logic: it does not touch the runtimestate Manager
// directly. Probe / PassivelyRefreshed / Counter are injected by the
// caller, who also performs the cache mutations from the returned Result.
// This separation keeps the Revalidator unit-testable without standing up
// a Manager + bus.
type Revalidator struct {
	Probe              RevalidationProbeFn
	PassivelyRefreshed PassivelyRefreshedFn
	Counter            RevalidationCounter
	// Cap overrides RevalidationCap when non-zero. Tests use Cap to
	// exercise small fixtures.
	Cap int
}

// ProbeResult describes the outcome for a single member during a
// Revalidator cycle.
type ProbeResult struct {
	Addr    byte
	Outcome RevalidationOutcome
}

// Result is one revalidation cycle's snapshot. Probed lists the members
// processed in order (skipped + actually-probed); Postponed lists members
// beyond cap that were not touched and stay at unchanged confidence per
// M5 constraint "Postponement".
type Result struct {
	Probed    []ProbeResult
	Postponed []byte
}

// orderedMembers returns members sorted by last_seen_at DESC (most recently
// active first), tie-break addr ASC, per M5 ordering rule.
func orderedMembers(members []KnownBusMember) []KnownBusMember {
	out := make([]KnownBusMember, len(members))
	copy(out, members)
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].LastSeenAt.Equal(out[j].LastSeenAt) {
			return out[i].LastSeenAt.After(out[j].LastSeenAt)
		}
		return out[i].Addr < out[j].Addr
	})
	return out
}

// Run executes one revalidation cycle. Members are ordered by last_seen_at
// DESC (tie-break addr ASC) and processed sequentially:
//
//   - Members for which PassivelyRefreshed returns true → counted as
//     skipped_passive_refresh; cap NOT consumed.
//   - Other members → directed probe; outcome counted; cap consumed by 1.
//   - Once cap probes have been issued, remaining unprocessed members are
//     stopped and returned in Postponed for the next cycle.
//
// PassivelyRefreshed and Counter MAY be nil — Run defaults them to "never
// refreshed" / no-op respectively. Probe MUST be non-nil; Run panics
// otherwise (a nil probe would silently report every member as no_reply
// AD23 → mass eviction, which is precisely the failure mode the test
// suite exists to catch).
func (r *Revalidator) Run(ctx context.Context, members []KnownBusMember) Result {
	if r.Probe == nil {
		panic("runtimestate.Revalidator.Run: Probe is nil; refusing to silently report every member as no_reply (AD23 mass eviction risk)")
	}
	cap := r.Cap
	if cap == 0 {
		cap = RevalidationCap
	}
	counter := r.Counter
	if counter == nil {
		counter = NopRevalidationCounter{}
	}
	passivelyRefreshed := r.PassivelyRefreshed
	if passivelyRefreshed == nil {
		passivelyRefreshed = func(byte) bool { return false }
	}

	ordered := orderedMembers(members)
	result := Result{
		Probed:    make([]ProbeResult, 0, len(ordered)),
		Postponed: nil,
	}
	probesIssued := 0
	for i, m := range ordered {
		if err := ctx.Err(); err != nil {
			// Context cancelled mid-cycle: postpone the rest. Already-
			// processed entries stay in Probed; their Counter.Inc has
			// already fired.
			result.Postponed = collectAddrs(ordered[i:])
			return result
		}
		if passivelyRefreshed(m.Addr) {
			counter.Inc(OutcomeSkippedPassiveRefresh)
			result.Probed = append(result.Probed, ProbeResult{Addr: m.Addr, Outcome: OutcomeSkippedPassiveRefresh})
			continue
		}
		if probesIssued >= cap {
			result.Postponed = collectAddrs(ordered[i:])
			return result
		}
		probesIssued++
		var outcome RevalidationOutcome
		if r.Probe(ctx, m.Addr) {
			outcome = OutcomeResponder
		} else {
			outcome = OutcomeNoReply
		}
		counter.Inc(outcome)
		result.Probed = append(result.Probed, ProbeResult{Addr: m.Addr, Outcome: outcome})
	}
	return result
}

func collectAddrs(members []KnownBusMember) []byte {
	if len(members) == 0 {
		return nil
	}
	out := make([]byte, len(members))
	for i, m := range members {
		out[i] = m.Addr
	}
	return out
}
