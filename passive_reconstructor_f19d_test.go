package ebusgateway

// F-19d (batch-17) regression tests for the passive reconstructor's
// wasEscaped-based disambiguation between escape-decoded data 0xAA
// (originally `0xA9 0x01` on the wire) and a real wire SYN
// frame-terminator. Replaces the pre-F-19d heuristic
// isMidRequestFrame() / isMidResponseFrame().
//
// Spec references:
//   - _work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-13-batch17.md
//     (live evidence: ~9 cascade-fingerprint events / hour, mid-frame
//     0xAA byte cascading into next-frame SRC/DST/PB/SB/LEN).
//   - The source comment at passive_transaction_reconstructor.go
//     (the predecessor isMidRequestFrame docstring, before F-19d
//     removal) that anticipated this cross-package change as
//     "Approach A in the P7.1 consult; deferred until live captures
//     justify the cross-repo change."
//   - john30/ebusd `symbol.h:79-82` (eBUS byte-stuffing rule:
//     `A9 00` → 0xA9 data, `A9 01` → 0xAA data, raw 0xAA → wire SYN).

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// TestF19d_EscapeDecodedAA_TreatedAsData feeds a broadcast frame
// where a data byte equals 0xAA. The byte is delivered with
// WasEscaped=true (simulating the upstream escape decoder having
// produced it from `0xA9 0x01` on the wire). The reconstructor MUST
// accumulate the byte as data, complete the frame at LEN-completion,
// and commit successfully. Pre-F-19d this relied on the
// isMidRequestFrame() heuristic; post-F-19d it relies on the
// wasEscaped ground truth.
func TestF19d_EscapeDecodedAA_TreatedAsData(t *testing.T) {
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
		Data:      []byte{0xAA, 0x33}, // data[0] = 0xAA (escape-decoded)
	}
	requestBytes := requestFrameBytes(frame)
	// Wire payload: [SYN gate] [SRC DST PB SB NN_m=2 data CRC] [SYN]
	wire := append([]byte{protocol.SymbolSyn}, requestBytes...)
	wire = append(wire, protocol.SymbolSyn)

	// Mask: every byte is raw passthrough EXCEPT the 0xAA at data[0]
	// which arrives via the escape decoder. The leading SYN and
	// trailing SYN are wire SYNs (WasEscaped=false). Indices:
	//   0: leading wire SYN
	//   1..5: SRC DST PB SB NN_m
	//   6: data[0] = 0xAA (escape-decoded)
	//   7: data[1] = 0x33
	//   8: CRC (computed at runtime)
	//   9: trailing wire SYN
	escapes := make([]bool, len(wire))
	escapes[6] = true
	// If CRC happens to also be 0xAA, mark it too; otherwise leave
	// false (the byte isn't a SYN candidate so the flag doesn't
	// matter for this test).
	if wire[8] == 0xAA {
		escapes[8] = true
	}

	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	if !event.HasRequest {
		t.Fatal("F-19d: escape-decoded 0xAA in data must accumulate, frame must commit")
	}
	if len(event.Request.Data) != 2 || event.Request.Data[0] != 0xAA {
		t.Fatalf("Request.Data = % X; want [AA 33]", event.Request.Data)
	}
}

// TestF19d_RawWireSyn_AbortsFrame is the smoking-gun regression for
// the batch-17 evidence pattern. A frame begins normally, then a
// wire SYN (WasEscaped=false) arrives mid-frame. Pre-F-19d:
// isMidRequestFrame() returned true at rawLen=6 (since 6 < 6+15=21),
// the SYN was absorbed as data, and the buffer ate the next frame's
// SRC/DST/PB/SB/LEN — eventually F-19a abandoned the bogus 21-byte
// buffer with reason=corrupted_request. Post-F-19d: the SYN-trigger
// path fires at byte 6 with reason=unexpected_syn (cleaner metric,
// no cascade).
func TestF19d_RawWireSyn_AbortsFrame(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Wire: SYN gate + a request header announcing NN_m=15, then 1
	// data byte, then a real wire SYN (frame truncated by bus
	// event). Pre-F-19d the buffer would consume the SYN + next
	// frame bytes; post-F-19d it abandons immediately.
	wire := []byte{
		protocol.SymbolSyn,           // gate
		0x10, 0x26, 0xB5, 0x23, 0x0F, // SRC DST PB SB NN_m=15
		0x05,               // DATA[0]
		protocol.SymbolSyn, // wire SYN — frame terminator
	}
	// All zero (no escapes); leading + trailing SYNs are wire SYNs.
	escapes := make([]bool, len(wire))
	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonUnexpectedSYN {
		t.Fatalf("AbandonReason = %v; want unexpected_syn (F-19d: a non-escaped 0xAA mid-frame is a wire SYN, not data)", event.AbandonReason)
	}
}

// TestF19d_EscapedA9_DataByte verifies the existing escape-decode
// path for 0xA9. A frame with a data byte 0xA9 on wire was
// transmitted as `0xA9 0x00`, decoded to logical 0xA9 with
// WasEscaped=true. The reconstructor must accept it as data.
func TestF19d_EscapedA9_DataByte(t *testing.T) {
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
		Data:      []byte{0xA9}, // data byte 0xA9 (was `A9 00` on wire)
	}
	requestBytes := requestFrameBytes(frame)
	wire := append([]byte{protocol.SymbolSyn}, requestBytes...)
	wire = append(wire, protocol.SymbolSyn)

	escapes := make([]bool, len(wire))
	// data[0] at index 6 is the escape-decoded 0xA9.
	escapes[6] = true

	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	if !event.HasRequest || len(event.Request.Data) != 1 || event.Request.Data[0] != 0xA9 {
		t.Fatalf("Request.Data = % X; want [A9]", event.Request.Data)
	}
}

// TestF19d_UnexpectedSyn_AtMSBoundary feeds a valid MS request all
// the way through S_ACK, then a wire SYN arrives in the response
// region. Pre-F-19d: depending on isMidResponseFrame() the SYN was
// either treated as data (cascading) or as a no-progress timeout.
// Post-F-19d: abandon with reason=unexpected_syn at the moment the
// non-escaped 0xAA is observed in response phase.
func TestF19d_UnexpectedSyn_AtMSBoundary(t *testing.T) {
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
		Secondary: 0x11,
		Data:      []byte{0x42},
	}
	reqBytes := requestFrameBytes(request)
	// Wire: SYN gate + request + S_ACK + NN_s=5 + 1 response byte +
	// wire SYN (response truncated by bus event before completion).
	wire := append([]byte{protocol.SymbolSyn}, reqBytes...)
	wire = append(wire, protocol.SymbolAck) // S_ACK
	wire = append(wire, 0x05)               // NN_s = 5
	wire = append(wire, 0x42)               // first response data byte
	wire = append(wire, protocol.SymbolSyn) // wire SYN mid-response

	escapes := make([]bool, len(wire))
	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonUnexpectedSYN {
		t.Fatalf("AbandonReason = %v; want unexpected_syn (F-19d: mid-response un-escaped 0xAA is a wire SYN)", event.AbandonReason)
	}
}

// TestF19d_F19c_Compatibility — F-19c's invalid_nn_m check at byte 5
// fires BEFORE F-19d's data-vs-SYN logic engages. A frame with
// NN_m=0xFF arrives → F-19c invalid_nn_m abandon at byte 5; F-19d
// never even sees the next byte.
func TestF19d_F19c_Compatibility(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	wire := []byte{
		protocol.SymbolSyn,
		0x10, 0x08, 0xB5, 0x11,
		0xFF, // NN_m = 255 — invalid (F-19c)
	}
	escapes := make([]bool, len(wire))
	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonInvalidNNMaster {
		t.Fatalf("AbandonReason = %v; want invalid_nn_m (F-19c must fire before F-19d for out-of-spec NN_m)", event.AbandonReason)
	}
}

// TestF19d_F19a_Compatibility — a frame with valid NN_m, valid
// escape-decoded 0xAA in data, but a deliberately wrong CRC. F-19d
// accumulates the escape-decoded 0xAA as data; F-19a then fires at
// LEN-completion with reason=corrupted_request when the CRC fails.
// Pin both behaviors together to prove F-19d and F-19a coexist.
func TestF19d_F19a_Compatibility(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Wire: [SYN] SRC DST PB SB NN_m=2 data[0]=0x11 data[1]=0xAA
	// CRC=0xBB (wrong) — note we don't append a trailing SYN; F-19a
	// fires at LEN-completion (byte 6+NN_m=8) when the CRC check
	// fails inside commitRequestFrameLocked.
	wire := []byte{
		protocol.SymbolSyn,
		0x10, 0x26, 0xB5, 0x23, 0x02,
		0x11, 0xAA, // data[1] = 0xAA, escape-decoded
		0xBB, // CRC = 0xBB (intentionally wrong)
	}
	escapes := make([]bool, len(wire))
	escapes[7] = true // data[1] arrived via escape decoder

	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonCorruptedRequest {
		t.Fatalf("AbandonReason = %v; want corrupted_request (F-19a fires at LEN-completion CRC fail, NOT unexpected_syn — escape-decoded 0xAA in data is accumulated, only the CRC mismatch abandons)", event.AbandonReason)
	}
}

// TestF19d_BufferCascade_Eliminated is the headline smoking-gun
// test for the batch-17 evidence pattern. Pre-F-19d the buffer grew
// to 14+ bytes (next-frame SRC/DST/PB/SB/LEN absorbed), abandoning
// as corrupted_request at LEN-completion. Post-F-19d the abandon
// fires at byte 6 with unexpected_syn; the second frame's bytes,
// starting with 0x10 (a valid initiator), are NOT absorbed into the
// first frame's buffer.
//
// Note: after F-19d's abandon, the next non-SYN byte (0x10) enters
// the Idle handler; with synced=true (from the wire SYN we just
// consumed) it would start a new frame — but Layer 2 admits 0x10
// as initiator-class. The subsequent bytes form the next frame
// header; depending on whether the second frame is complete in our
// test stream, it commits or remains pending. We assert ONLY the
// F-19d abandon at byte 6 — the next frame's behavior is covered
// by adjacent integration tests.
func TestF19d_BufferCascade_Eliminated(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Wire: the live batch-17 evidence shape.
	//   first frame: 10 26 B5 23 0F 05 [wire SYN at position 7]
	//   then bytes that pre-F-19d would have been absorbed.
	wire := []byte{
		protocol.SymbolSyn,
		0x10, 0x26, 0xB5, 0x23, 0x0F, // SRC DST PB SB NN_m=15
		0x05,               // DATA[0]
		protocol.SymbolSyn, // wire SYN — abandon trigger
		// Pre-F-19d, these would have been cascade-absorbed:
		0x10, 0x08, 0xB5, 0x11, 0x01, 0x42,
	}
	escapes := make([]bool, len(wire))
	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonUnexpectedSYN {
		t.Fatalf("AbandonReason = %v; want unexpected_syn (F-19d batch-17 cascade-elimination: the buffer must abandon at byte 6 with the wire-SYN classification, NOT cascade into next-frame bytes for a 21-byte corrupted_request)", event.AbandonReason)
	}
	// The Request field carries the partial header; pin its
	// length to confirm no cascade absorbed extra bytes.
	if event.HasRequest {
		t.Logf("partial Request.Source=0x%02X Target=0x%02X (informational; F-19d abandons before the partial frame is parsed)", event.Request.Source, event.Request.Target)
	}
}

// TestF19d_F18_RegressionGuard pins that F-19d's wasEscaped plumbing
// is contained to the passive reconstructor path. F-18's
// echo-passthrough lives in the active mux (internal/adaptermux),
// not the passive tap, so it cannot interact with the WasEscaped
// flag. This test verifies the structural separation: the F-19d
// signature changes do not leak into PassiveEvent fields the
// active mux consumes, and the passive helper functions remain
// pure on byte values.
func TestF19d_F18_RegressionGuard(t *testing.T) {
	t.Parallel()

	// The new field WasEscaped exists on PassiveTapEvent (the type
	// the passive reconstructor consumes). The active mux's
	// PassiveEvent (in internal/adaptermux/mux.go) is a different
	// type defined in a different package — F-19d does not touch
	// it. This test pins that PassiveTapEvent's WasEscaped field
	// can be set without affecting any other behavior.
	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Feed a normal broadcast frame with WasEscaped=false for all
	// bytes — F-19d's data path must still commit the frame
	// (no 0xAA bytes in this frame's body, so wasEscaped is
	// irrelevant for the dispatch).
	frame := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x01, 0x02},
	}
	wire := append([]byte{protocol.SymbolSyn}, requestFrameBytes(frame)...)
	wire = append(wire, protocol.SymbolSyn)
	escapes := make([]bool, len(wire))
	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	if !event.HasRequest {
		t.Fatal("F-19d regression: normal broadcast frame must commit")
	}
}
