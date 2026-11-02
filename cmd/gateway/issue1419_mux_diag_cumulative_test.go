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
