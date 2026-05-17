package ebusgateway

import (
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// P10 — echo_mismatch subclass classification.
//
// Operator directive (2026-05-09): "After P7.1 the passive-stream
// error rate dropped dramatically. Apart from CRC + collisions we
// shouldn't see much else." The residual ~300 echo_mismatch counter
// hides several distinct phenomena (post-grant collision, stale
// adapter buffer, wire bit-flip). P10 surfaces them as separate
// subclass labels on `ebus_errors_total{class="echo_mismatch"}`.
//
// Batch-23 round-4 (2026-05-17, Attack 10 hypothesis): the
// previously-monolithic `pre_echo_syn` label is split by the
// upstream-emitted EchoWasEscaped flag into `pre_echo_syn_raw`
// (real wire SYN intrusion) vs `pre_echo_syn_escaped_data`
// (escape-decoded payload 0xAA from a third-party frame, wire
// `A9 01`). The legacy `pre_echo_syn` label is RETIRED.

// TestClassify_PreEchoSyn_Raw pins the batch-23 round-4 split:
// 0xAA with EchoWasEscaped=false is a REAL wire SYN — classified
// as `pre_echo_syn_raw` (mux SYN-suppression leak class).
func TestClassify_PreEchoSyn_Raw(t *testing.T) {
	t.Parallel()

	if got := classifyEchoMismatchSubclass(protocol.SymbolSyn, false); got != "pre_echo_syn_raw" {
		t.Errorf("classifyEchoMismatchSubclass(0xAA, wasEscaped=false) = %q; want pre_echo_syn_raw (real wire SYN intrusion — Attack 1/3 class; mux SYN-suppression leak)", got)
	}
}

// TestClassify_PreEchoSyn_EscapedData pins the Attack 10 case:
// 0xAA with EchoWasEscaped=true is an escape-decoded payload byte
// from a third-party frame (wire `A9 01` → logical 0xAA via the
// ENH transport's unescape). Classified as
// `pre_echo_syn_escaped_data` to separate this from
// gateway-internal SYN-suppression bugs.
func TestClassify_PreEchoSyn_EscapedData(t *testing.T) {
	t.Parallel()

	if got := classifyEchoMismatchSubclass(protocol.SymbolSyn, true); got != "pre_echo_syn_escaped_data" {
		t.Errorf("classifyEchoMismatchSubclass(0xAA, wasEscaped=true) = %q; want pre_echo_syn_escaped_data (Attack 10: escape-decoded data byte from third-party frame's payload; wire-contention class)", got)
	}
}

// TestClassify_OtherSubclassesUnchanged_WithWasEscaped verifies
// the WasEscaped discriminator only affects the 0xAA case.
// Non-SYN bytes return the same subclass regardless of the flag —
// the discriminator semantics only matter for 0xAA because that is
// the only byte value that ENH transports legitimately escape.
func TestClassify_OtherSubclassesUnchanged_WithWasEscaped(t *testing.T) {
	t.Parallel()

	cases := []struct {
		readByte byte
		want     string
	}{
		{protocol.SymbolEscape, "post_grant_reserved"},
		{protocol.SymbolAck, "post_grant_ack"},
		{protocol.SymbolNack, "post_grant_nack"},
		{0xF1, "post_grant_collision_initiator"},
		{0x71, "post_grant_collision_initiator"},
		{0x15, "post_grant_collision_target"},
		{protocol.AddressBroadcast, "post_grant_collision_target"},
	}
	for _, tc := range cases {
		got1 := classifyEchoMismatchSubclass(tc.readByte, false)
		got2 := classifyEchoMismatchSubclass(tc.readByte, true)
		if got1 != tc.want {
			t.Errorf("classifyEchoMismatchSubclass(0x%02X, false) = %q; want %q", tc.readByte, got1, tc.want)
		}
		if got2 != tc.want {
			t.Errorf("classifyEchoMismatchSubclass(0x%02X, true) = %q; want %q (WasEscaped must not affect non-0xAA classification)", tc.readByte, got2, tc.want)
		}
	}
}

func TestClassifyEchoMismatchSubclass_AckByte(t *testing.T) {
	t.Parallel()

	if got := classifyEchoMismatchSubclass(protocol.SymbolAck, false); got != "post_grant_ack" {
		t.Errorf("classifyEchoMismatchSubclass(0x00=ACK) = %q; want post_grant_ack", got)
	}
}

func TestClassifyEchoMismatchSubclass_NackByte(t *testing.T) {
	t.Parallel()

	if got := classifyEchoMismatchSubclass(protocol.SymbolNack, false); got != "post_grant_nack" {
		t.Errorf("classifyEchoMismatchSubclass(0xFF=NACK) = %q; want post_grant_nack", got)
	}
}

func TestClassifyEchoMismatchSubclass_EscapeByte(t *testing.T) {
	t.Parallel()

	if got := classifyEchoMismatchSubclass(protocol.SymbolEscape, false); got != "post_grant_reserved" {
		t.Errorf("classifyEchoMismatchSubclass(0xA9=ESCAPE) = %q; want post_grant_reserved", got)
	}
}

// TestClassifyEchoMismatchSubclass_InitiatorAddresses verifies that
// canonical initiator-class addresses (0x10, 0x30, 0xF1, 0x71, etc.)
// classify as post_grant_collision_initiator. These represent the
// dominant residual case observed live (e.g.
// `writePrefix=15 readPrefix=F1 15 B5 24 ...`).
func TestClassifyEchoMismatchSubclass_InitiatorAddresses(t *testing.T) {
	t.Parallel()

	// Sample of P0/P1 initiator-class addresses from sourceAddressTableV1.
	cases := []byte{0x10, 0x30, 0xF1, 0x71, 0x03, 0x13}
	for _, addr := range cases {
		if got := classifyEchoMismatchSubclass(addr, false); got != "post_grant_collision_initiator" {
			t.Errorf("classifyEchoMismatchSubclass(0x%02X) = %q; want post_grant_collision_initiator", addr, got)
		}
	}
}

// TestClassifyEchoMismatchSubclass_TargetAddresses verifies target-class
// addresses (most non-canonical bytes) classify as
// post_grant_collision_target.
func TestClassifyEchoMismatchSubclass_TargetAddresses(t *testing.T) {
	t.Parallel()

	// Sample target-class targets: companion bytes from the table
	// AND non-canonical addresses default to target.
	cases := []byte{0x15, 0x35, 0x08, 0x26, 0x42}
	for _, addr := range cases {
		if got := classifyEchoMismatchSubclass(addr, false); got != "post_grant_collision_target" {
			t.Errorf("classifyEchoMismatchSubclass(0x%02X) = %q; want post_grant_collision_target", addr, got)
		}
	}
}

// TestClassifyEchoMismatchSubclass_BroadcastByte verifies that 0xFE
// (broadcast) classifies as post_grant_collision_target — broadcasts
// indicate another initiator was mid-frame on the wire.
func TestClassifyEchoMismatchSubclass_BroadcastByte(t *testing.T) {
	t.Parallel()

	if got := classifyEchoMismatchSubclass(protocol.AddressBroadcast, false); got != "post_grant_collision_target" {
		t.Errorf("classifyEchoMismatchSubclass(0xFE=broadcast) = %q; want post_grant_collision_target", got)
	}
}

// TestRecordActiveError_EchoMismatchSubclassPropagates exercises the
// full path: BusObservabilityStore.OnBusEvent receives a
// BusEventEchoMismatch with a specific Byte; the rendered Prometheus
// output must:
//   - emit the unchanged `ebus_errors_total{class="echo_mismatch"}`
//     counter (preserves backward-compat for existing alerts —
//     Codex P10 review pass 1 MAJOR FINDING_1)
//   - emit a parallel `ebus_active_echo_mismatch_subclass_total{
//     subclass="..."}` counter with the inferred subclass
func TestRecordActiveError_EchoMismatchSubclassPropagates(t *testing.T) {
	t.Parallel()

	store := NewBusObservabilityStore(DefaultConfig())

	// Simulate live echo_mismatch with byte=0xF1 (post_grant_collision_initiator).
	err := store.OnBusEvent(protocol.BusEvent{
		Kind:    protocol.BusEventEchoMismatch,
		Outcome: protocol.BusOutcomeEchoMismatch,
		Byte:    0xF1,
	})
	if err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	metrics := store.RenderPrometheus()

	// The legacy ebus_errors_total{class=echo_mismatch} series is
	// unchanged (no subclass label dimension on it). Existing
	// alerts that filter only on class=echo_mismatch keep matching
	// the same single time series.
	wantLegacy := `ebus_errors_total{class="echo_mismatch",phase="request",scope="active"} 1`
	if !strings.Contains(metrics, wantLegacy) {
		t.Errorf("metrics missing legacy line %q:\n%s", wantLegacy, metrics)
	}

	// The new parallel counter exposes the subclass breakdown.
	wantSubclass := `ebus_active_echo_mismatch_subclass_total{subclass="post_grant_collision_initiator"} 1`
	if !strings.Contains(metrics, wantSubclass) {
		t.Errorf("metrics missing subclass line %q:\n%s", wantSubclass, metrics)
	}
}

// TestRecordActiveError_PreEchoSynSplitByWasEscaped pins the
// batch-23 round-4 plumbing end-to-end through
// BusObservabilityStore: a BusEventEchoMismatch with Byte=0xAA
// AND EchoWasEscaped=false emits the `pre_echo_syn_raw` subclass;
// the same byte with EchoWasEscaped=true emits
// `pre_echo_syn_escaped_data`. The legacy `pre_echo_syn` label
// MUST NOT appear in either case (RETIRED label).
func TestRecordActiveError_PreEchoSynSplitByWasEscaped(t *testing.T) {
	t.Parallel()

	store := NewBusObservabilityStore(DefaultConfig())

	// Raw wire SYN intrusion.
	if err := store.OnBusEvent(protocol.BusEvent{
		Kind:           protocol.BusEventEchoMismatch,
		Outcome:        protocol.BusOutcomeEchoMismatch,
		Byte:           protocol.SymbolSyn,
		EchoWasEscaped: false,
	}); err != nil {
		t.Fatalf("OnBusEvent (raw) error = %v", err)
	}

	// Attack 10: escape-decoded payload 0xAA from third-party frame.
	if err := store.OnBusEvent(protocol.BusEvent{
		Kind:           protocol.BusEventEchoMismatch,
		Outcome:        protocol.BusOutcomeEchoMismatch,
		Byte:           protocol.SymbolSyn,
		EchoWasEscaped: true,
	}); err != nil {
		t.Fatalf("OnBusEvent (escaped) error = %v", err)
	}

	metrics := store.RenderPrometheus()

	wantRaw := `ebus_active_echo_mismatch_subclass_total{subclass="pre_echo_syn_raw"} 1`
	if !strings.Contains(metrics, wantRaw) {
		t.Errorf("metrics missing raw line %q:\n%s", wantRaw, metrics)
	}
	wantEscaped := `ebus_active_echo_mismatch_subclass_total{subclass="pre_echo_syn_escaped_data"} 1`
	if !strings.Contains(metrics, wantEscaped) {
		t.Errorf("metrics missing escaped-data line %q:\n%s", wantEscaped, metrics)
	}
	// Legacy label MUST NOT appear as a sample (HELP text retains the
	// note that it's retired, but no counter line should be emitted).
	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		if strings.Contains(line, `subclass="pre_echo_syn"`) {
			t.Errorf("RETIRED legacy label `pre_echo_syn` re-appeared as a sample (batch-23 round-4 contract violation): %q", line)
		}
	}
}

// TestRecordActiveError_NonEchoMismatchDoesNotEmitSubclassMetric
// verifies that other error classes (timeout, nack, crc_mismatch)
// do NOT bump the new echo_mismatch subclass counter — Codex P10
// review pass 1 NIT FINDING_3 fix: assert against the literal
// metric line, not via fragile substring search ordering.
func TestRecordActiveError_NonEchoMismatchDoesNotEmitSubclassMetric(t *testing.T) {
	t.Parallel()

	store := NewBusObservabilityStore(DefaultConfig())

	if err := store.OnBusEvent(protocol.BusEvent{
		Kind:    protocol.BusEventTimeout,
		Outcome: protocol.BusOutcomeTimeout,
	}); err != nil {
		t.Fatalf("OnBusEvent timeout error = %v", err)
	}

	metrics := store.RenderPrometheus()

	// timeout error MUST appear in the legacy errors_total series
	// (unchanged shape).
	wantLegacy := `ebus_errors_total{class="timeout",phase="terminal",scope="active"} 1`
	if !strings.Contains(metrics, wantLegacy) {
		t.Errorf("metrics missing legacy timeout line %q:\n%s", wantLegacy, metrics)
	}

	// The new subclass counter MUST NOT have any non-zero entries
	// for a timeout-only event. The metric type may still be
	// emitted (Prometheus convention: declare type even when no
	// samples), but no counter LINE should appear.
	for _, line := range strings.Split(metrics, "\n") {
		// Skip help / type declarations and empty lines.
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		if strings.HasPrefix(line, "ebus_active_echo_mismatch_subclass_total") {
			t.Errorf("subclass metric emitted for non-echo-mismatch event: %q\nfull output:\n%s", line, metrics)
		}
	}
}
