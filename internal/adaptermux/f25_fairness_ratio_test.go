package adaptermux

import (
	"testing"
	"time"
)

// f25_fairness_ratio_test.go pins F-25 (batch-22, iter2, 2026-05-14).
// The fix lowers DefaultFairnessRatio from the legacy 4 (75% gateway /
// 25% external) to 2 (50/50 under contention), promotes the ratio to
// Mux.Config.FairnessRatio, and threads it through arbitrator.setPolicy.
//
// Live evidence pre-F-25 (iter1, 17:40-17:52 window post F-22 revert +
// F-21 + F-24 inert): ebusd `arbitration won in invalid state` x33 /
// 12 min; tight-scan-08 success 11/60 (batch 1) and 5/60 (batch 2)
// vs 95% target. Hypothesis: gateway dominance of bus slots was the
// dominant fault; tightening the share to 50/50 should give ebusd's
// bus state machine larger inter-poll windows and reduce
// invalid-state collisions.

// TestF25_DefaultFairnessRatioIsTwo pins the new default at the
// constant level so a future "tweak the constant" change makes the
// test fail loudly. Iter1's live evidence and consultant analysis
// converged on ratio=2 as the correct 50/50 split for tight-scan
// stability.
func TestF25_DefaultFairnessRatioIsTwo(t *testing.T) {
	if DefaultFairnessRatio != 2 {
		t.Fatalf("F-25 regression: DefaultFairnessRatio = %d; want 2 (50/50 gateway/external split under contention). The legacy value of 4 produced 75/25 dominance and starved ebusd of bus slots; see iter1-result-20260514T145344Z.md.",
			DefaultFairnessRatio)
	}
}

// TestF25_ZeroRatioMapsToDefault verifies that the zero-value sentinel
// resolves to DefaultFairnessRatio inside the arbitrator's tryGrant
// path (operator-friendly: Config.FairnessRatio=0 → default).
func TestF25_ZeroRatioMapsToDefault(t *testing.T) {
	arb := newArbitrator()
	arb.setPolicy(24*time.Hour, 0, 0) // zero -> default

	// Run DefaultFairnessRatio*3 contended rounds and assert the
	// external grant share matches the default ratio.
	totalRounds := DefaultFairnessRatio * 3
	external, gateway := runContendedRotation(t, arb, totalRounds)
	wantExternal := totalRounds / DefaultFairnessRatio
	wantGateway := totalRounds - wantExternal
	if external != wantExternal || gateway != wantGateway {
		t.Fatalf("F-25 zero-ratio default: ext=%d gw=%d; want ext=%d gw=%d (ratio=%d, rounds=%d)",
			external, gateway, wantExternal, wantGateway, DefaultFairnessRatio, totalRounds)
	}
}

// TestF25_CustomRatioApplied verifies operator-tuneable ratios. With
// ratio=3 (33% external), the split is 1-of-3 external per contended
// rotation.
func TestF25_CustomRatioApplied(t *testing.T) {
	const customRatio = 3
	arb := newArbitrator()
	arb.setPolicy(24*time.Hour, customRatio, 0)

	totalRounds := customRatio * 4 // 4 cycles
	external, gateway := runContendedRotation(t, arb, totalRounds)
	wantExternal := totalRounds / customRatio
	wantGateway := totalRounds - wantExternal
	if external != wantExternal || gateway != wantGateway {
		t.Fatalf("F-25 custom ratio=%d: ext=%d gw=%d; want ext=%d gw=%d",
			customRatio, external, gateway, wantExternal, wantGateway)
	}
}

// TestF25_NegativeRatioClampedToDefault is the defensive escape: a
// negative ratio is clamped to DefaultFairnessRatio rather than
// degenerating into "external never gets a slot" or panicking.
func TestF25_NegativeRatioClampedToDefault(t *testing.T) {
	arb := newArbitrator()
	arb.setPolicy(24*time.Hour, -7, 0)

	totalRounds := DefaultFairnessRatio * 2
	external, gateway := runContendedRotation(t, arb, totalRounds)
	wantExternal := totalRounds / DefaultFairnessRatio
	wantGateway := totalRounds - wantExternal
	if external != wantExternal || gateway != wantGateway {
		t.Fatalf("F-25 negative-ratio clamp: ext=%d gw=%d; want ext=%d gw=%d (clamp target=%d)",
			external, gateway, wantExternal, wantGateway, DefaultFairnessRatio)
	}
}

// TestF25_RatioOneClampsToDefault pins the post-PR-#633-P2-fix
// behavior (commit aec342f, 2026-05-15): operators who configure
// FairnessRatio=1 expecting the documented "alternating / 50-50
// behavior" now get the behavior they intended.
//
// Pre-fix, ratio=1 degenerated to "external every rotation" (the
// fairnessCounter reached the threshold on the first increment, so
// preferExternalThisGrant fired every contended grant) — starving
// the gateway entirely whenever external had a non-empty queue. The
// fix clamps fairnessRatio<=1 to DefaultFairnessRatio in setPolicy,
// so an operator who sets ratio=1 now gets the same G,E,G,E,...
// alternation that ratio=2 has always produced.
func TestF25_RatioOneClampsToDefault(t *testing.T) {
	arb := newArbitrator()
	arb.setPolicy(24*time.Hour, 1, 0)

	totalRounds := 8
	external, gateway := runContendedRotation(t, arb, totalRounds)
	// With ratio=1 clamped to DefaultFairnessRatio (2), the rotation
	// alternates: G,E,G,E,... Across 8 contended rounds we expect
	// 4 gateway grants + 4 external grants.
	if external != 4 || gateway != 4 {
		t.Fatalf("PR #633 P2 fix: ratio=1 should clamp to default and alternate 50/50; got ext=%d gw=%d (want 4/4)", external, gateway)
	}
}

// runContendedRotation is the shared scaffold: enqueue one gateway +
// one external request each round, observe the grant outcome, cancel
// the loser so the per-session-one-pending invariant holds, and
// loop. Returns (externalGrants, gatewayGrants).
func runContendedRotation(t *testing.T, arb *arbitrator, rounds int) (external, gateway int) {
	t.Helper()
	for i := 0; i < rounds; i++ {
		gwCh := arb.requestStart(gatewaySessionID, 0x71)
		extCh := arb.requestStart(1, 0x31)

		sessionID, _, notify, granted := tryGrantLegacy(arb)
		if !granted {
			t.Fatalf("round %d: expected grant", i)
		}
		arb.confirmOwnership(sessionID, 0)
		notify <- startResult{granted: true}

		if sessionID == gatewaySessionID {
			gateway++
			arb.cancelStart(1)
			<-extCh
		} else {
			external++
			arb.cancelStart(gatewaySessionID)
			<-gwCh
		}
		arb.releaseOwnership(sessionID)
	}
	return external, gateway
}
