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
		t.Errorf("classifyEchoMismatchSubclass(0xAA) = %q; want pre_echo_syn (mux suppression canary)", got)
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
// canonical master-class addresses (0x10, 0x30, 0xF1, 0x71, etc.)
// classify as post_grant_collision_initiator. These represent the
// dominant residual case observed live (e.g.
// `writePrefix=15 readPrefix=F1 15 B5 24 ...`).
func TestClassifyEchoMismatchSubclass_InitiatorAddresses(t *testing.T) {
	t.Parallel()

	// Sample of P0/P1 master-class addresses from sourceAddressTableV1.
	cases := []byte{0x10, 0x30, 0xF1, 0x71, 0x03, 0x13}
	for _, addr := range cases {
		if got := classifyEchoMismatchSubclass(addr); got != "post_grant_collision_initiator" {
			t.Errorf("classifyEchoMismatchSubclass(0x%02X) = %q; want post_grant_collision_initiator", addr, got)
		}
	}
}

// TestClassifyEchoMismatchSubclass_TargetAddresses verifies slave-class
// addresses (most non-canonical bytes) classify as
// post_grant_collision_target.
func TestClassifyEchoMismatchSubclass_TargetAddresses(t *testing.T) {
	t.Parallel()

	// Sample slave-class targets: companion bytes from the table
	// AND non-canonical addresses default to slave.
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
// output must include the corresponding subclass label.
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
	want := `class="echo_mismatch"`
	if !strings.Contains(metrics, want) {
		t.Fatalf("metrics missing %s:\n%s", want, metrics)
	}
	wantSubclass := `subclass="post_grant_collision_initiator"`
	if !strings.Contains(metrics, wantSubclass) {
		t.Errorf("metrics missing %s:\n%s", wantSubclass, metrics)
	}
}

// TestRecordActiveError_NonEchoMismatchHasEmptySubclass verifies that
// other error classes (timeout, nack, crc_mismatch) do NOT get a
// subclass label populated — backward-compat check. The rendered
// Prometheus output should show subclass="" for non-echo-mismatch
// errors.
func TestRecordActiveError_NonEchoMismatchHasEmptySubclass(t *testing.T) {
	t.Parallel()

	store := NewBusObservabilityStore(DefaultConfig())

	if err := store.OnBusEvent(protocol.BusEvent{
		Kind:    protocol.BusEventTimeout,
		Outcome: protocol.BusOutcomeTimeout,
	}); err != nil {
		t.Fatalf("OnBusEvent timeout error = %v", err)
	}

	metrics := store.RenderPrometheus()
	// timeout class should appear with empty subclass label.
	if !strings.Contains(metrics, `class="timeout"`) {
		t.Fatalf("metrics missing class=timeout:\n%s", metrics)
	}
	// Should NOT have a populated subclass for timeout.
	if strings.Contains(metrics, `class="timeout"`) && strings.Contains(metrics, `class="timeout"`+`,phase=`) {
		// regex would be cleaner but this guards against the obvious
		// "timeout error tagged with non-empty subclass" regression.
		// Empty subclass appears as subclass="" in the output.
		bad := []string{
			`class="timeout",subclass="post_grant`,
			`class="timeout",subclass="bit_flip`,
			`class="timeout",subclass="pre_echo_syn`,
		}
		for _, b := range bad {
			if strings.Contains(metrics, b) {
				t.Errorf("timeout error should NOT have subclass=%q; output:\n%s", b, metrics)
			}
		}
	}
}
