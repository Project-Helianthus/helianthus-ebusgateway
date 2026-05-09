package ebusgateway

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// P7 — LEN-based request completion invariant.
//
// Real eBUS wire (Spec_Prot_7 §3) for an M2T transaction has NO SYN
// between command CRC and target ACK. The parser must detect request
// completion at LEN+CRC reach (len(requestRaw) == 6 + LEN_byte) and
// transition to passivePhaseWaitACK proactively, instead of waiting
// for a trailing SYN that doesn't arrive until the entire transaction
// is over.
//
// This regression suite locks in the post-Mode-C behavior.

// TestPassiveTransactionReconstructor_M2T_RealWireShape verifies that
// the reconstructor classifies a real-wire M2T transaction (no SYN
// between command CRC and target ACK) as a successful Transaction
// event. Pre-P7 this case abandoned with corrupted_request because
// the parser kept accumulating ACK + response bytes into requestRaw
// until the trailing SYN, then parseFrame failed on length mismatch.
func TestPassiveTransactionReconstructor_M2T_RealWireShape(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x01},
	}
	// Real wire: SRC DST PB SB LEN data CRC ACK RESP_LEN data RESP_CRC
	// ACK SYN. requestFrameBytes appends nothing past CRC.
	wire := append([]byte{protocol.SymbolSyn}, requestFrameBytes(request)...)
	wire = append(wire, protocol.SymbolAck)
	wire = append(wire, responseSegmentBytes([]byte{0x11, 0x55})...)
	wire = append(wire, protocol.SymbolAck, protocol.SymbolSyn)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventTransaction)
	if event.FrameType != protocol.FrameTypeInitiatorTarget {
		t.Errorf("frame type = %v; want FrameTypeInitiatorTarget", event.FrameType)
	}
	if !event.HasResponse {
		t.Errorf("HasResponse = false; want true")
	}
	if got, want := len(event.Response.Data), 2; got != want {
		t.Errorf("response data len = %d; want %d", got, want)
	}
}

// TestPassiveTransactionReconstructor_M2I_RealWireShape verifies the
// real-wire shape for M2I (initiator/initiator) transactions, which
// have NO SYN between command CRC and the target's ACK and have NO
// response phase — wire is SRC DST PB SB LEN data CRC ACK SYN.
func TestPassiveTransactionReconstructor_M2I_RealWireShape(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	frame := protocol.Frame{
		Source:    0x30,
		Target:    0x10,
		Primary:   0x01,
		Secondary: 0x02,
		Data:      []byte{0x03},
	}
	wire := append([]byte{protocol.SymbolSyn}, requestFrameBytes(frame)...)
	wire = append(wire, protocol.SymbolAck, protocol.SymbolSyn)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventMasterFrame)
	if event.FrameType != protocol.FrameTypeInitiatorInitiator {
		t.Errorf("frame type = %v; want FrameTypeInitiatorInitiator", event.FrameType)
	}
}

// TestPassiveTransactionReconstructor_BroadcastStillWorksAtLENBoundary
// verifies that broadcast frames continue to work after the P7 fix.
// Broadcast wire IS [request_bytes][SYN] — no ACK or response phase —
// so the parser hits LEN+CRC at len(requestRaw) == 6 + LEN_byte, the
// LEN-based early-transition emits the BroadcastFrame event and resets
// to Idle, and the trailing SYN re-engages Layer 1 via the
// passivePhaseIdle handler.
func TestPassiveTransactionReconstructor_BroadcastStillWorksAtLENBoundary(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	frame := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x42, 0x99},
	}
	// Broadcast wire shape: SRC DST PB SB LEN data CRC SYN.
	wire := append([]byte{protocol.SymbolSyn}, frameBytes(frame)...)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	if event.FrameType != protocol.FrameTypeBroadcast {
		t.Errorf("frame type = %v; want FrameTypeBroadcast", event.FrameType)
	}
	if got, want := len(event.Request.Data), 2; got != want {
		t.Errorf("request data len = %d; want %d", got, want)
	}
}

// TestPassiveTransactionReconstructor_M2T_BackToBack verifies that two
// consecutive M2T transactions (with NO inter-frame SYN gap inside
// each transaction, but a SYN between transactions per real wire) both
// classify as Transaction events.
func TestPassiveTransactionReconstructor_M2T_BackToBack(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	first := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x01},
	}
	second := protocol.Frame{
		Source:    0xF1,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x06, 0x02, 0x00, 0x03, 0x01, 0x14},
	}

	wire := []byte{protocol.SymbolSyn}
	wire = append(wire, requestFrameBytes(first)...)
	wire = append(wire, protocol.SymbolAck)
	// Response data avoids 0xAA — see P7.1 follow-up note in
	// handleRequestSymbolLocked: the parser still treats logical 0xAA
	// in data as SYN until escape-vs-wire metadata is propagated
	// through PassiveTapEvent.
	wire = append(wire, responseSegmentBytes([]byte{0x42})...)
	wire = append(wire, protocol.SymbolAck, protocol.SymbolSyn)
	// Inter-transaction idle marker. Real wire emits ≥1 SYN here.
	wire = append(wire, protocol.SymbolSyn)
	wire = append(wire, requestFrameBytes(second)...)
	wire = append(wire, protocol.SymbolAck)
	wire = append(wire, responseSegmentBytes([]byte{0x42, 0x99})...)
	wire = append(wire, protocol.SymbolAck, protocol.SymbolSyn)

	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventTransaction)
	requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventTransaction)

	snapshot := reconstructor.Snapshot()
	if got := snapshot.AbandonsByReason["corrupted_request"]; got != 0 {
		t.Errorf("AbandonsByReason[corrupted_request] = %d; want 0 (real-wire M2T must not abandon)", got)
	}
}

// TestPassiveTransactionReconstructor_BroadcastDefersToSYNTrigger covers
// the Codex P7 review pass 3 contract: broadcast frames whose request
// bytes structurally complete at LEN+CRC reach MUST NOT be classified
// before the trailing SYN arrives. Real broadcast wire is
// [SRC..CRC][SYN] and the broadcast event's timing observables
// (RequestEnd, Terminal, ObservedAt) must reflect the SYN timestamp.
//
// Feeds [SYN] SRC DST=0xFE PB SB LEN data CRC <gap> SYN with a delay
// before SYN; asserts the BroadcastFrame event is emitted at the SYN
// timestamp, NOT at the CRC byte's timestamp. Also asserts that a
// TRUNCATED broadcast (CRC reached but no trailing SYN, then transport
// reset) does NOT produce a spurious BroadcastFrame event before the
// SYN.
func TestPassiveTransactionReconstructor_BroadcastDefersToSYNTrigger(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	frame := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x01, 0x02},
	}

	// Feed leading SYN + request bytes WITHOUT trailing SYN. At
	// LEN+CRC reach, commitRequestFrameLocked sees frameType=Broadcast
	// and returns ok=false (defers to SYN path). No event yet.
	requestBytes := requestFrameBytes(frame)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), append([]byte{protocol.SymbolSyn}, requestBytes...))
	assertNoPassiveClassifiedEvent(t, subscription, 50*time.Millisecond)

	// Now feed the trailing SYN at a LATER timestamp; expect the
	// BroadcastFrame event to use THIS timestamp (not the CRC byte's).
	synTimestamp := time.Unix(0, 0).Add(500 * time.Millisecond)
	feedPassiveSymbolsRaw(reconstructor, synTimestamp, []byte{protocol.SymbolSyn})

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	if !event.ObservedAt.Equal(synTimestamp) {
		t.Errorf("event.ObservedAt = %v; want %v (broadcast must be timestamped at trailing SYN, not CRC byte)", event.ObservedAt, synTimestamp)
	}
	if !event.Timing.Terminal.Equal(synTimestamp) {
		t.Errorf("event.Timing.Terminal = %v; want %v", event.Timing.Terminal, synTimestamp)
	}
}

// TestPassiveTransactionReconstructor_M2T_CRCMismatchAtLENBoundary
// covers the parseFrame-failure path inside the LEN-based early
// transition. When LEN+CRC is reached but parseFrame fails (bad CRC),
// the parser MUST keep accumulating bytes — the trailing SYN path
// then classifies the abandon, preserving the pre-P7 corrupted_request
// classification for genuinely corrupt frames.
func TestPassiveTransactionReconstructor_M2T_CRCMismatchAtLENBoundary(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x01},
	}
	// Build the request with a deliberately wrong CRC byte.
	requestRaw := requestFrameBytes(request)
	requestRaw[len(requestRaw)-1] ^= 0xFF // corrupt CRC

	wire := append([]byte{protocol.SymbolSyn}, requestRaw...)
	// Append more wire bytes; eventual trailing SYN triggers abandon.
	wire = append(wire, protocol.SymbolAck)
	wire = append(wire, 0x02, 0x11, 0x22)
	wire = append(wire, protocol.SymbolSyn)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonCorruptedRequest {
		t.Errorf("abandon reason = %q; want %q", event.AbandonReason, PassiveAbandonReasonCorruptedRequest)
	}
}
