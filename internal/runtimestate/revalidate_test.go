package runtimestate

import (
	"context"
	"sync"
	"testing"
	"time"
)

// recordingCounter captures Inc calls in order so tests can assert telemetry.
type recordingCounter struct {
	mu       sync.Mutex
	outcomes []RevalidationOutcome
}

func (c *recordingCounter) Inc(o RevalidationOutcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.outcomes = append(c.outcomes, o)
}

func (c *recordingCounter) Counts() map[RevalidationOutcome]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[RevalidationOutcome]int)
	for _, o := range c.outcomes {
		out[o]++
	}
	return out
}

func makeMember(addr byte, lastSeenAt time.Time) KnownBusMember {
	return KnownBusMember{
		Addr:       addr,
		LastSeenAt: lastSeenAt,
		LastSource: LastSourcePassiveObserved,
		Confidence: ConfidenceCorroborated,
	}
}

// -----------------------------------------------------------------------------
// Ordering: last_seen_at DESC, tie-break addr ASC.
// -----------------------------------------------------------------------------

func TestRevalidator_OrdersByLastSeenAtDescThenAddrAsc(t *testing.T) {
	t0 := time.Date(2026, 5, 10, 19, 0, 0, 0, time.UTC)
	members := []KnownBusMember{
		makeMember(0x08, t0.Add(-3*time.Minute)),
		makeMember(0x15, t0),
		makeMember(0xF6, t0),                  // tie with 0x15 → addr ASC means 0x15 first
		makeMember(0x26, t0.Add(-1*time.Minute)),
		makeMember(0x04, t0.Add(-3*time.Minute)), // tie with 0x08 → addr ASC means 0x04 first
	}
	wantOrder := []byte{0x15, 0xF6, 0x26, 0x04, 0x08}

	probedOrder := []byte{}
	r := &Revalidator{
		Probe: func(_ context.Context, addr byte) bool {
			probedOrder = append(probedOrder, addr)
			return true
		},
	}
	r.Run(context.Background(), members)

	if len(probedOrder) != len(wantOrder) {
		t.Fatalf("probed %d members; want %d", len(probedOrder), len(wantOrder))
	}
	for i := range probedOrder {
		if probedOrder[i] != wantOrder[i] {
			t.Errorf("probe order[%d] = 0x%02x; want 0x%02x (full got=%x want=%x)",
				i, probedOrder[i], wantOrder[i], probedOrder, wantOrder)
		}
	}
}

// -----------------------------------------------------------------------------
// Cap: synthetic 64-member acceptance test (M5 acceptance criterion).
// -----------------------------------------------------------------------------

func TestRevalidator_Cap32_64MemberFixture_FirstHalfProbedRestPostponed(t *testing.T) {
	t0 := time.Date(2026, 5, 10, 19, 0, 0, 0, time.UTC)
	members := make([]KnownBusMember, 0, 64)
	// Build 64 members with strictly decreasing last_seen_at so the
	// ordering is unambiguous: addr 0x01 is the most recently seen,
	// addr 0x40 the oldest. Cap=32 means the first 32 (0x01..0x20) must
	// be probed; the remaining 32 (0x21..0x40) must be postponed.
	for i := 0; i < 64; i++ {
		members = append(members, makeMember(byte(0x01+i), t0.Add(-time.Duration(i)*time.Second)))
	}

	probed := []byte{}
	counter := &recordingCounter{}
	r := &Revalidator{
		Probe: func(_ context.Context, addr byte) bool {
			probed = append(probed, addr)
			return true
		},
		Counter: counter,
	}
	result := r.Run(context.Background(), members)

	if len(probed) != 32 {
		t.Fatalf("probed count = %d; want 32 (RevalidationCap)", len(probed))
	}
	if len(result.Postponed) != 32 {
		t.Fatalf("postponed count = %d; want 32", len(result.Postponed))
	}
	for i, addr := range probed {
		want := byte(0x01 + i)
		if addr != want {
			t.Errorf("probed[%d] = 0x%02x; want 0x%02x", i, addr, want)
		}
	}
	for i, addr := range result.Postponed {
		want := byte(0x21 + i)
		if addr != want {
			t.Errorf("postponed[%d] = 0x%02x; want 0x%02x", i, addr, want)
		}
	}
	if counter.Counts()[OutcomeResponder] != 32 {
		t.Errorf("responder count = %d; want 32", counter.Counts()[OutcomeResponder])
	}
}

// -----------------------------------------------------------------------------
// Skip-passive-refresh: passively-observed members do NOT consume cap slots.
// -----------------------------------------------------------------------------

func TestRevalidator_PassivelyRefreshedMembersDoNotConsumeCap(t *testing.T) {
	t0 := time.Date(2026, 5, 10, 19, 0, 0, 0, time.UTC)
	// 5 members ordered most-recent-first: 0x01, 0x02, 0x03, 0x04, 0x05.
	// Passively refreshed: {0x02, 0x04}. Cap=2 (probes).
	// Cap applies to PROBES only, so passively-refreshed members are
	// telemetry-only and continue to be processed past the cap. The
	// post-cap iteration stops only at the first PROBE-eligible member.
	// Expected:
	//   0x01: probed (cap=1)
	//   0x02: passive skip (no cap consumed)
	//   0x03: probed (cap=2)
	//   0x04: passive skip (no cap consumed; cap full but passives still flow)
	//   0x05: would probe; cap full → postponed; loop returns.
	members := []KnownBusMember{
		makeMember(0x01, t0),
		makeMember(0x02, t0.Add(-1*time.Second)),
		makeMember(0x03, t0.Add(-2*time.Second)),
		makeMember(0x04, t0.Add(-3*time.Second)),
		makeMember(0x05, t0.Add(-4*time.Second)),
	}
	passivelyRefreshed := map[byte]bool{0x02: true, 0x04: true}

	probed := []byte{}
	counter := &recordingCounter{}
	r := &Revalidator{
		Probe: func(_ context.Context, addr byte) bool {
			probed = append(probed, addr)
			return true
		},
		PassivelyRefreshed: func(addr byte) bool { return passivelyRefreshed[addr] },
		Counter:            counter,
		Cap:                2,
	}
	result := r.Run(context.Background(), members)

	if len(probed) != 2 {
		t.Fatalf("probed count = %d; want 2 (cap)", len(probed))
	}
	if probed[0] != 0x01 || probed[1] != 0x03 {
		t.Errorf("probed = %x; want [0x01, 0x03] (skipping 0x02 passive)", probed)
	}
	if len(result.Postponed) != 1 || result.Postponed[0] != 0x05 {
		t.Errorf("postponed = %x; want [0x05]", result.Postponed)
	}
	// Counter: 2× responder, 2× skipped_passive_refresh (0x02 and 0x04).
	// 0x04's passive skip IS counted because passively-refreshed members
	// don't consume cap slots — only probe-eligible members trigger the
	// cap-full → postpone branch (which captured 0x05).
	wantCounts := map[RevalidationOutcome]int{
		OutcomeResponder:             2,
		OutcomeSkippedPassiveRefresh: 2,
	}
	for outcome, want := range wantCounts {
		if got := counter.Counts()[outcome]; got != want {
			t.Errorf("counter[%s] = %d; want %d", outcome, got, want)
		}
	}
	if got := counter.Counts()[OutcomeNoReply]; got != 0 {
		t.Errorf("counter[no_reply] = %d; want 0", got)
	}
	// Result.Probed includes both probes AND skips, in order:
	wantProbedOrder := []ProbeResult{
		{Addr: 0x01, Outcome: OutcomeResponder},
		{Addr: 0x02, Outcome: OutcomeSkippedPassiveRefresh},
		{Addr: 0x03, Outcome: OutcomeResponder},
		{Addr: 0x04, Outcome: OutcomeSkippedPassiveRefresh},
	}
	if len(result.Probed) != len(wantProbedOrder) {
		t.Fatalf("Result.Probed length = %d; want %d", len(result.Probed), len(wantProbedOrder))
	}
	for i, want := range wantProbedOrder {
		if result.Probed[i] != want {
			t.Errorf("Result.Probed[%d] = %+v; want %+v", i, result.Probed[i], want)
		}
	}
}

// -----------------------------------------------------------------------------
// No-reply outcome (AD23 immediate eviction signal).
// -----------------------------------------------------------------------------

func TestRevalidator_NoReplyOutcomeReportedAndCounted(t *testing.T) {
	t0 := time.Date(2026, 5, 10, 19, 0, 0, 0, time.UTC)
	members := []KnownBusMember{
		makeMember(0x08, t0),                  // responder
		makeMember(0x15, t0.Add(-1*time.Second)), // no-reply
		makeMember(0xF6, t0.Add(-2*time.Second)), // responder
	}
	counter := &recordingCounter{}
	r := &Revalidator{
		Probe: func(_ context.Context, addr byte) bool {
			return addr != 0x15 // 0x15 returns no-reply
		},
		Counter: counter,
	}
	result := r.Run(context.Background(), members)

	if len(result.Probed) != 3 {
		t.Fatalf("probed count = %d; want 3", len(result.Probed))
	}
	wantOutcomes := []RevalidationOutcome{OutcomeResponder, OutcomeNoReply, OutcomeResponder}
	for i, want := range wantOutcomes {
		if result.Probed[i].Outcome != want {
			t.Errorf("probed[%d].Outcome = %s; want %s (addr=0x%02x)",
				i, result.Probed[i].Outcome, want, result.Probed[i].Addr)
		}
	}
	if counter.Counts()[OutcomeNoReply] != 1 {
		t.Errorf("counter[no_reply] = %d; want 1", counter.Counts()[OutcomeNoReply])
	}
	if counter.Counts()[OutcomeResponder] != 2 {
		t.Errorf("counter[responder] = %d; want 2", counter.Counts()[OutcomeResponder])
	}
}

// -----------------------------------------------------------------------------
// Empty members list.
// -----------------------------------------------------------------------------

func TestRevalidator_EmptyMembersIsNoOp(t *testing.T) {
	counter := &recordingCounter{}
	probed := false
	r := &Revalidator{
		Probe:   func(context.Context, byte) bool { probed = true; return true },
		Counter: counter,
	}
	result := r.Run(context.Background(), nil)
	if probed {
		t.Errorf("probe was invoked on empty members list")
	}
	if len(result.Probed) != 0 {
		t.Errorf("probed count = %d; want 0", len(result.Probed))
	}
	if len(result.Postponed) != 0 {
		t.Errorf("postponed count = %d; want 0", len(result.Postponed))
	}
	if len(counter.outcomes) != 0 {
		t.Errorf("counter increments = %d; want 0", len(counter.outcomes))
	}
}

// -----------------------------------------------------------------------------
// Context cancellation mid-cycle.
// -----------------------------------------------------------------------------

func TestRevalidator_ContextCancellationPostponesUnprocessed(t *testing.T) {
	t0 := time.Date(2026, 5, 10, 19, 0, 0, 0, time.UTC)
	members := []KnownBusMember{
		makeMember(0x01, t0),
		makeMember(0x02, t0.Add(-1*time.Second)),
		makeMember(0x03, t0.Add(-2*time.Second)),
		makeMember(0x04, t0.Add(-3*time.Second)),
	}
	counter := &recordingCounter{}
	ctx, cancel := context.WithCancel(context.Background())
	probedCount := 0
	r := &Revalidator{
		Probe: func(_ context.Context, _ byte) bool {
			probedCount++
			if probedCount == 2 {
				// Cancel after the second probe completes; the next loop
				// iteration must observe ctx.Err() and postpone the rest.
				cancel()
			}
			return true
		},
		Counter: counter,
	}
	result := r.Run(ctx, members)

	if probedCount != 2 {
		t.Fatalf("probed count = %d; want 2 (cancelled mid-cycle)", probedCount)
	}
	if len(result.Postponed) != 2 || result.Postponed[0] != 0x03 || result.Postponed[1] != 0x04 {
		t.Errorf("postponed = %x; want [0x03, 0x04]", result.Postponed)
	}
}

// -----------------------------------------------------------------------------
// Cap-full + passive suffix: passive members AFTER a cap-full probe-eligible
// member still get telemetry counted (cap applies exclusively to bus probes).
// Codex P2 follow-up on PR #614.
// -----------------------------------------------------------------------------

func TestRevalidator_PassiveRefreshAfterCapStillFlows(t *testing.T) {
	t0 := time.Date(2026, 5, 10, 19, 0, 0, 0, time.UTC)
	// Codex's exact fixture: [probe, probe, active-miss, passive-refresh].
	// Cap=2 → first two probed; third is probe-eligible but cap full
	// → postponed; fourth is passive-refresh and must be telemetry-
	// counted, NOT lumped into Postponed.
	members := []KnownBusMember{
		makeMember(0x01, t0),                     // probe
		makeMember(0x02, t0.Add(-1*time.Second)), // probe
		makeMember(0x03, t0.Add(-2*time.Second)), // active-miss → postponed
		makeMember(0x04, t0.Add(-3*time.Second)), // passive-refresh → counted
	}
	passivelyRefreshed := map[byte]bool{0x04: true}

	probed := []byte{}
	counter := &recordingCounter{}
	r := &Revalidator{
		Probe: func(_ context.Context, addr byte) bool {
			probed = append(probed, addr)
			return true
		},
		PassivelyRefreshed: func(addr byte) bool { return passivelyRefreshed[addr] },
		Counter:            counter,
		Cap:                2,
	}
	result := r.Run(context.Background(), members)

	if len(probed) != 2 || probed[0] != 0x01 || probed[1] != 0x02 {
		t.Errorf("probed = %x; want [0x01, 0x02]", probed)
	}
	// 0x03 hits cap → Postponed; 0x04 keeps flowing (passive-refresh).
	if len(result.Postponed) != 1 || result.Postponed[0] != 0x03 {
		t.Errorf("Postponed = %x; want [0x03] only (0x04 is passive, must be counted not postponed)",
			result.Postponed)
	}
	// Counter must have the skipped_passive_refresh increment for 0x04
	// even though cap was full when we reached it.
	if got := counter.Counts()[OutcomeSkippedPassiveRefresh]; got != 1 {
		t.Errorf("counter[skipped_passive_refresh] = %d; want 1 (post-cap passive must still flow)", got)
	}
	if got := counter.Counts()[OutcomeResponder]; got != 2 {
		t.Errorf("counter[responder] = %d; want 2", got)
	}
	// Result.Probed must include the post-cap passive-refresh entry.
	wantProbedOrder := []ProbeResult{
		{Addr: 0x01, Outcome: OutcomeResponder},
		{Addr: 0x02, Outcome: OutcomeResponder},
		{Addr: 0x04, Outcome: OutcomeSkippedPassiveRefresh},
	}
	if len(result.Probed) != len(wantProbedOrder) {
		t.Fatalf("Probed length = %d; want %d", len(result.Probed), len(wantProbedOrder))
	}
	for i, want := range wantProbedOrder {
		if result.Probed[i] != want {
			t.Errorf("Probed[%d] = %+v; want %+v", i, result.Probed[i], want)
		}
	}
}

// -----------------------------------------------------------------------------
// AD23 guardrail: a probe that returns false because ctx was cancelled
// in-flight MUST NOT be mapped to OutcomeNoReply (which the caller would
// turn into eviction). The post-probe ctx.Err() recheck postpones the
// member instead. Codex P1 follow-up on PR #614.
// -----------------------------------------------------------------------------

func TestRevalidator_CtxCancelledDuringProbeIsPostponedNotEvicted(t *testing.T) {
	t0 := time.Date(2026, 5, 10, 19, 0, 0, 0, time.UTC)
	members := []KnownBusMember{
		makeMember(0x01, t0),
		makeMember(0x02, t0.Add(-1*time.Second)),
		makeMember(0x03, t0.Add(-2*time.Second)),
	}
	counter := &recordingCounter{}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Revalidator{
		Probe: func(probeCtx context.Context, addr byte) bool {
			if addr == 0x02 {
				// Simulate a ctx-aware probe whose underlying transport
				// returns false after the parent cancels mid-flight.
				cancel()
				return false
			}
			return true
		},
		Counter: counter,
	}
	result := r.Run(ctx, members)

	// 0x01 was probed normally (responder).
	// 0x02 returned false but ctx is cancelled → postpone (NOT no_reply).
	// 0x03 was never reached.
	if len(result.Probed) != 1 {
		t.Fatalf("Probed length = %d; want 1 (only 0x01 has a real outcome)", len(result.Probed))
	}
	if result.Probed[0].Addr != 0x01 || result.Probed[0].Outcome != OutcomeResponder {
		t.Errorf("Probed[0] = %+v; want {0x01, responder}", result.Probed[0])
	}
	if len(result.Postponed) != 2 || result.Postponed[0] != 0x02 || result.Postponed[1] != 0x03 {
		t.Errorf("Postponed = %x; want [0x02, 0x03] (cancelled member + untouched)", result.Postponed)
	}
	// Critical: counter must NOT have a no_reply increment for 0x02.
	if got := counter.Counts()[OutcomeNoReply]; got != 0 {
		t.Errorf("counter[no_reply] = %d; want 0 (ctx-cancelled probe must not be evicted)", got)
	}
	if got := counter.Counts()[OutcomeResponder]; got != 1 {
		t.Errorf("counter[responder] = %d; want 1", got)
	}
}

// -----------------------------------------------------------------------------
// Nil-probe panic guardrail (catches AD23 mass-eviction misconfiguration).
// -----------------------------------------------------------------------------

func TestRevalidator_NilProbePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic on nil Probe")
		}
	}()
	(&Revalidator{}).Run(context.Background(), []KnownBusMember{makeMember(0x01, time.Now())})
}

// -----------------------------------------------------------------------------
// Default cap when r.Cap == 0 uses RevalidationCap (32).
// -----------------------------------------------------------------------------

func TestRevalidator_DefaultCapIsRevalidationCap(t *testing.T) {
	t0 := time.Date(2026, 5, 10, 19, 0, 0, 0, time.UTC)
	members := make([]KnownBusMember, 0, 40)
	for i := 0; i < 40; i++ {
		members = append(members, makeMember(byte(0x01+i), t0.Add(-time.Duration(i)*time.Second)))
	}
	probedCount := 0
	r := &Revalidator{
		Probe: func(context.Context, byte) bool { probedCount++; return true },
		// Cap not set — should default to RevalidationCap (32).
	}
	r.Run(context.Background(), members)
	if probedCount != RevalidationCap {
		t.Errorf("probed count = %d; want RevalidationCap=%d", probedCount, RevalidationCap)
	}
}
