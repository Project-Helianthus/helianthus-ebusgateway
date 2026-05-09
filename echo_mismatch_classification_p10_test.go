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

func TestClassifyEchoMismatchSubclass_PreEchoSYN(t *testing.T) {
	t.Parallel()

	if got := classifyEchoMismatchSubclass(protocol.SymbolSyn); got != "pre_echo_syn" {
		t.Errorf("classifyEchoMismatchSubclass(0xAA) = %q; want pre_echo_syn (byte-value-based label for 0xAA in echo position; semantic interpretation per P10.1 doc-comment is approximate — could be mux suppression leak OR escape-decoded data byte)", got)
	}
}

func TestClassifyEchoMismatchSubclass_AckByte(t *testing.T) {
	t.Parallel()

	if got := classifyEchoMismatchSubclass(protocol.SymbolAck); got != "post_grant_ack" {
		t.Errorf("classifyEchoMismatchSubclass(0x00=ACK) = %q; want post_grant_ack", got)
	}
}

func TestClassifyEchoMismatchSubclass_NackByte(t *testing.T) {
	t.Parallel()

	if got := classifyEchoMismatchSubclass(protocol.SymbolNack); got != "post_grant_nack" {
		t.Errorf("classifyEchoMismatchSubclass(0xFF=NACK) = %q; want post_grant_nack", got)
	}
}

func TestClassifyEchoMismatchSubclass_EscapeByte(t *testing.T) {
	t.Parallel()

	if got := classifyEchoMismatchSubclass(protocol.SymbolEscape); got != "post_grant_reserved" {
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
		if got := classifyEchoMismatchSubclass(addr); got != "post_grant_collision_initiator" {
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
		if got := classifyEchoMismatchSubclass(addr); got != "post_grant_collision_target" {
			t.Errorf("classifyEchoMismatchSubclass(0x%02X) = %q; want post_grant_collision_target", addr, got)
		}
	}
}

// TestClassifyEchoMismatchSubclass_BroadcastByte verifies that 0xFE
// (broadcast) classifies as post_grant_collision_target — broadcasts
// indicate another initiator was mid-frame on the wire.
func TestClassifyEchoMismatchSubclass_BroadcastByte(t *testing.T) {
	t.Parallel()

	if got := classifyEchoMismatchSubclass(protocol.AddressBroadcast); got != "post_grant_collision_target" {
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
