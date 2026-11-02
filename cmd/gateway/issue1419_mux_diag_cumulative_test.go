package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux"
)

type issue1419DiagClassifier struct {
	activeTxnClassifier
	snapshot adaptermux.ActiveTxnSnapshot
}

func (c *issue1419DiagClassifier) ActiveTxnSnapshot() adaptermux.ActiveTxnSnapshot {
	return c.snapshot
}

func issue1419Snapshot(lead, suffix, stale uint64, maxOwnership uint64) adaptermux.ActiveTxnSnapshot {
	return adaptermux.ActiveTxnSnapshot{
		EscapePairLeadFailures:    lead,
		EscapePairSuffixFailures:  suffix,
		StaleActiveBytesDiscarded: stale,
		InactiveReasonCounts:      map[adaptermux.ActiveTxnInactiveReason]uint64{adaptermux.ActiveTxnInactiveReason("max_ownership"): maxOwnership},
	}
}

// Kills: exporting the live generation's raw totals, which decrease on a
// driver replacement and discard the retired generation's increments.
func TestIssue1419MuxDiagTotalsStayMonotoneAcrossReplacement(t *testing.T) {
	counter := &ebusV8CumulativeCounter{}
	first := &issue1419DiagClassifier{snapshot: issue1419Snapshot(3, 1, 7, 2)}
	got, genBefore := counter.diagSnapshot(first.snapshot)
	if got.EscapePairLeadFailures != 3 || got.StaleActiveBytesDiscarded != 7 {
		t.Fatalf("first generation totals = %+v", got)
	}

	counter.retireDiag(first)
	// No generation admitted: the live snapshot is zero.
	got, genGap := counter.diagSnapshot(adaptermux.ActiveTxnSnapshot{})
	if got.EscapePairLeadFailures != 3 || got.EscapePairSuffixFailures != 1 || got.StaleActiveBytesDiscarded != 7 ||
		got.InactiveReasonCounts["max_ownership"] != 2 {
		t.Fatalf("totals with no generation admitted = %+v, want retired increments kept", got)
	}
	if genGap == genBefore {
		t.Fatal("generation identifier did not change on retirement")
	}

	// Increments in the second generation add on top.
	got, _ = counter.diagSnapshot(issue1419Snapshot(2, 0, 1, 1))
	if got.EscapePairLeadFailures != 5 || got.EscapePairSuffixFailures != 1 || got.StaleActiveBytesDiscarded != 8 ||
		got.InactiveReasonCounts["max_ownership"] != 3 {
		t.Fatalf("totals in second generation = %+v, want 5/1/8/3", got)
	}
}

// Kills: summing a live snapshot read before a retirement with the retired
// offset that already contains the same generation (double count, ratcheted
// by the floor).
func TestIssue1419MuxDiagRetirementBetweenReadAndFoldCountsOnce(t *testing.T) {
	counter := &ebusV8CumulativeCounter{}
	first := &issue1419DiagClassifier{snapshot: issue1419Snapshot(3, 1, 7, 2)}
	readAt := counter.currentGeneration()
	live := first.ActiveTxnSnapshot() // live read of generation one
	counter.retireDiag(first)         // retirement lands before the fold

	got, _ := counter.diagSnapshotAt(live, readAt)
	if got.EscapePairLeadFailures != 3 || got.StaleActiveBytesDiscarded != 7 || got.InactiveReasonCounts["max_ownership"] != 2 {
		t.Fatalf("totals = %+v, want generation one exactly once (3/7/2)", got)
	}
	got, _ = counter.diagSnapshot(issue1419Snapshot(1, 0, 1, 1))
	if got.EscapePairLeadFailures != 4 || got.StaleActiveBytesDiscarded != 8 || got.InactiveReasonCounts["max_ownership"] != 3 {
		t.Fatalf("totals after second generation = %+v, want 4/8/3 (floor must not ratchet)", got)
	}
}

// Same interleaving for the V8 cumulative total.
func TestIssue1419V8TotalRetirementBetweenReadAndFoldCountsOnce(t *testing.T) {
	counter := &ebusV8CumulativeCounter{}
	readAt := counter.currentGeneration()
	counter.mu.Lock() // retirement of a generation whose live total was 5
	counter.retired += 5
	counter.floor = counter.retired
	counter.generation++
	counter.mu.Unlock()
	if got := counter.totalAt(5, readAt); got != 5 {
		t.Fatalf("total = %d, want 5 (generation counted once)", got)
	}
	if got := counter.totalAt(2, counter.currentGeneration()); got != 7 {
		t.Fatalf("total after second generation = %d, want 7", got)
	}
}
