package ebusgateway

import (
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func echoMismatchTestStore() (*BusObservabilityStore, *time.Time) {
	store := NewBusObservabilityStore(DefaultConfig())
	now := time.Unix(1_700_000_000, 0)
	store.now = func() time.Time { return now }
	return store, &now
}

func echoMismatchEvent(observed byte, escaped bool) protocol.BusEvent {
	return protocol.BusEvent{
		Kind:           protocol.BusEventEchoMismatch,
		Outcome:        protocol.BusOutcomeEchoMismatch,
		Byte:           observed,
		EchoWasEscaped: escaped,
	}
}

func metricLine(t *testing.T, store *BusObservabilityStore, series string, want string) {
	t.Helper()
	for _, line := range strings.Split(store.RenderPrometheus(), "\n") {
		if strings.HasPrefix(line, series+" ") {
			if line != series+" "+want {
				t.Errorf("%s = %q; want %q", series, strings.TrimPrefix(line, series+" "), want)
			}
			return
		}
	}
	if want != "0" {
		t.Errorf("series %s missing; want %s", series, want)
	}
}

const echoMismatchErrorSeries = `ebus_errors_total{class="echo_mismatch",phase="request",scope="active"}`

func subclassSeries(name string) string {
	return `ebus_active_echo_mismatch_subclass_total{subclass="` + name + `"}`
}

// Kills: counting the outcome re-emit as a second observation, and labelling
// the observed byte 0x42 as post_grant_ack.
func TestEchoMismatchGenericCountedOnceWithAndWithoutReemit(t *testing.T) {
	t.Parallel()
	want := classifyEchoMismatchSubclass(0x42, false)
	if !strings.HasPrefix(want, "post_grant_collision_") {
		t.Fatalf("classifier(0x42) = %q; want post_grant_collision_*", want)
	}
	for _, reemit := range []bool{true, false} {
		store, _ := echoMismatchTestStore()
		if err := store.OnBusEvent(echoMismatchEvent(0x42, false)); err != nil {
			t.Fatal(err)
		}
		if reemit {
			if err := store.OnBusEvent(protocol.BusEvent{Kind: protocol.BusEventEchoMismatch, Outcome: protocol.BusOutcomeEchoMismatch}); err != nil {
				t.Fatal(err)
			}
		}
		metricLine(t, store, echoMismatchErrorSeries, "1")
		metricLine(t, store, subclassSeries(want), "1")
		metricLine(t, store, subclassSeries("post_grant_ack"), "0")
		metricLine(t, store, subclassSeries("byte_unknown"), "0")
	}
}

// Kills: an unbounded re-emit window and a window never cleared by a
// consumed re-emit.
func TestEchoMismatchByteLessOutsideReemitWindowIsByteUnknown(t *testing.T) {
	t.Parallel()
	store, now := echoMismatchTestStore()
	_ = store.OnBusEvent(echoMismatchEvent(0x42, false))
	*now = now.Add(echoMismatchReemitWindow + time.Millisecond)
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	metricLine(t, store, echoMismatchErrorSeries, "2")
	metricLine(t, store, subclassSeries("byte_unknown"), "1")

	_ = store.OnBusEvent(echoMismatchEvent(0x42, true))
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	*now = now.Add(echoMismatchReemitWindow + time.Millisecond)
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	metricLine(t, store, echoMismatchErrorSeries, "4")
	metricLine(t, store, subclassSeries("byte_unknown"), "2")
}

// Kills: counting a real zero-byte mismatch and its outcome re-emit twice.
func TestEchoMismatchZeroByteAndReemitCountedOnce(t *testing.T) {
	t.Parallel()
	store, _ := echoMismatchTestStore()
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	metricLine(t, store, echoMismatchErrorSeries, "1")
	metricLine(t, store, subclassSeries("byte_unknown"), "1")
}

// Kills: deduplicating a byte-less event whose request differs from the
// preceding byte-bearing event.
func TestEchoMismatchDifferentRequestIsNotReemit(t *testing.T) {
	t.Parallel()
	store, _ := echoMismatchTestStore()
	first := echoMismatchEvent(0x42, false)
	first.HasRequest = true
	first.Request = protocol.Frame{Source: 0x10, Target: 0x15, Primary: 0xB5, Secondary: 0x09, Data: []byte{1}}
	other := echoMismatchEvent(0, false)
	other.HasRequest = true
	other.Request = protocol.Frame{Source: 0x10, Target: 0x15, Primary: 0xB5, Secondary: 0x09, Data: []byte{2}}
	_ = store.OnBusEvent(first)
	_ = store.OnBusEvent(other)
	metricLine(t, store, echoMismatchErrorSeries, "2")
	metricLine(t, store, subclassSeries("byte_unknown"), "1")
}

// Kills: carrying escape-pair attribution across a mux counter reset.
func TestEchoMismatchEscapePairCounterResetIsAttributed(t *testing.T) {
	t.Parallel()
	store, now := echoMismatchTestStore()
	var lead uint64 = 5
	store.SetEscapePairFailureProvider(func() (uint64, uint64) { return lead, 0 })
	for i := 0; i < 5; i++ {
		_ = store.OnBusEvent(echoMismatchEvent(0, false))
	}
	lead = 1
	*now = now.Add(time.Second)
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	metricLine(t, store, subclassSeries("escape_pair_verify"), "6")
}

// Kills: a re-emit window that survives an intervening event of another kind.
func TestEchoMismatchReemitWindowClearedByOtherEvent(t *testing.T) {
	t.Parallel()
	store, _ := echoMismatchTestStore()
	_ = store.OnBusEvent(echoMismatchEvent(0x42, false))
	_ = store.OnBusEvent(protocol.BusEvent{Kind: protocol.BusEventTimeout, Outcome: protocol.BusOutcomeTimeout})
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	metricLine(t, store, subclassSeries("byte_unknown"), "1")
}

// Kills: labelling byte-less events post_grant_ack, and labelling a
// byte-less event escape_pair_verify without a mux failure behind it.
func TestEchoMismatchByteLessIsByteUnknown(t *testing.T) {
	t.Parallel()
	store, _ := echoMismatchTestStore()
	store.SetEscapePairFailureProvider(func() (uint64, uint64) { return 0, 0 })
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	metricLine(t, store, echoMismatchErrorSeries, "1")
	metricLine(t, store, subclassSeries("byte_unknown"), "1")
	metricLine(t, store, subclassSeries("post_grant_ack"), "0")
	metricLine(t, store, subclassSeries("escape_pair_verify"), "0")
}

// Kills: attributing one mux failure to several events, ignoring either step,
// and attributing before the failure exists.
func TestEchoMismatchEscapePairFailureGetsOwnSubclass(t *testing.T) {
	t.Parallel()
	store, _ := echoMismatchTestStore()
	var lead, suffix uint64
	store.SetEscapePairFailureProvider(func() (uint64, uint64) { return lead, suffix })

	lead = 1
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	metricLine(t, store, subclassSeries("escape_pair_verify"), "1")
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	metricLine(t, store, subclassSeries("escape_pair_verify"), "1")
	metricLine(t, store, subclassSeries("byte_unknown"), "1")

	suffix = 1
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	metricLine(t, store, subclassSeries("escape_pair_verify"), "2")
	metricLine(t, store, echoMismatchErrorSeries, "3")
	metricLine(t, store, subclassSeries("post_grant_ack"), "0")
}

// Kills: a byte-bearing mismatch resetting the escape-pair attribution so its
// byte-less re-emit is counted again as escape_pair_verify.
func TestEchoMismatchMixedSequenceAfterEscapePairCountsOnce(t *testing.T) {
	t.Parallel()
	store, _ := echoMismatchTestStore()
	var lead uint64 = 1
	store.SetEscapePairFailureProvider(func() (uint64, uint64) { return lead, 0 })
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	_ = store.OnBusEvent(echoMismatchEvent(0x42, false))
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	metricLine(t, store, echoMismatchErrorSeries, "2")
	metricLine(t, store, subclassSeries("escape_pair_verify"), "1")
}

// Kills: swapped or dropped diagnostics, a step label that is not lead or
// suffix, and address- or serial-bearing labels.
func TestAdaptermuxDiagnosticsExported(t *testing.T) {
	t.Parallel()
	store, _ := echoMismatchTestStore()
	store.SetAdaptermuxDiagProvider(func() AdaptermuxDiagSnapshot {
		return AdaptermuxDiagSnapshot{
			InactiveReasonCounts:      map[string]uint64{"transaction_done": 11, "max_ownership": 12},
			StaleActiveBytesDiscarded: 13,
			InterWriteDrainTotal:      14,
			ForeignInitiatorReleases:  15,
			EscapePairLeadFailures:    16,
			EscapePairSuffixFailures:  17,
		}
	})
	metricLine(t, store, `ebus_adaptermux_transaction_endings_total{reason="transaction_done"}`, "11")
	metricLine(t, store, `ebus_adaptermux_transaction_endings_total{reason="max_ownership"}`, "12")
	metricLine(t, store, `ebus_adaptermux_stale_active_bytes_discarded_total`, "13")
	metricLine(t, store, `ebus_adaptermux_inter_write_drained_bytes_total`, "14")
	metricLine(t, store, `ebus_adaptermux_foreign_initiator_releases_total`, "15")
	metricLine(t, store, `ebus_adaptermux_escape_pair_verify_failures_total{step="lead"}`, "16")
	metricLine(t, store, `ebus_adaptermux_escape_pair_verify_failures_total{step="suffix"}`, "17")
}

// Kills: a suppressed byte-less re-emit still booking the attempt in the
// frame and byte series, and a no-echo escape-pair failure (surfaced as a
// timeout event) being consumed by a later unrelated byte-less mismatch.
func TestEchoMismatchReemitSkipsFrameBookkeeping(t *testing.T) {
	t.Parallel()
	store, _ := echoMismatchTestStore()
	req := protocol.Frame{Source: 0x10, Target: 0x15, Primary: 0xB5, Secondary: 0x09, Data: []byte{1}}
	first := echoMismatchEvent(0x42, false)
	first.HasRequest, first.Request = true, req
	again := echoMismatchEvent(0, false)
	again.HasRequest, again.Request = true, req
	_ = store.OnBusEvent(first)
	before := store.RenderPrometheus()
	_ = store.OnBusEvent(again)
	if after := store.RenderPrometheus(); after != before {
		t.Errorf("suppressed re-emit changed the metrics:\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestEchoMismatchTimeoutSettlesEscapePairFailure(t *testing.T) {
	t.Parallel()
	store, now := echoMismatchTestStore()
	var lead uint64
	store.SetEscapePairFailureProvider(func() (uint64, uint64) { return lead, 0 })
	lead = 1
	_ = store.OnBusEvent(protocol.BusEvent{Kind: protocol.BusEventTimeout, Outcome: protocol.BusOutcomeTimeout})
	*now = now.Add(time.Second)
	_ = store.OnBusEvent(echoMismatchEvent(0, false))
	metricLine(t, store, echoMismatchErrorSeries, "1")
	metricLine(t, store, subclassSeries("byte_unknown"), "1")
	metricLine(t, store, subclassSeries("escape_pair_verify"), "0")
}
