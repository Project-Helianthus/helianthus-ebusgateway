package ebusgateway

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// P7.1 — escape-decoded 0xAA disambiguation.
//
// The passive bus tap's escape decoder converts wire `0xA9 0x01` into
// logical `protocol.SymbolSyn` (0xAA). Pre-P7.1, the reconstructor's
// SYN checks could not distinguish a real wire-SYN frame terminator
// from a logical 0xAA data byte, so frames whose data or CRC region
// contained a logical 0xAA were abandoned at the SYN branch. P7.1
// adds length-aware predicates (isMidRequestFrame / isMidResponseFrame)
// that route SYN-valued bytes through the data-accumulation path while
// the buffer is structurally below the LEN-declared length.
//
// This regression suite locks in the post-P7.1 behavior.

// TestPassiveReconstructor_P71_M2T_RequestData0xAA_Classified verifies
// that an M2T transaction whose request data contains a logical 0xAA
// byte (mid-frame, between LEN and CRC) classifies as a Transaction
// event instead of abandoning at the SYN branch.
func TestPassiveReconstructor_P71_M2T_RequestData0xAA_Classified(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Request data carries a logical 0xAA byte. requestFrameBytes
	// stamps SRC DST PB SB LEN data CRC; CRC is whatever the helper
	// computes — that's covered by the natural CRC validation path.
	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{0x01, 0xAA, 0x02},
	}
	wire := append([]byte{protocol.SymbolSyn}, requestFrameBytes(request)...)
	wire = append(wire, protocol.SymbolAck)
	// Response data MUST NOT contain 0xAA in this test (avoids
	// commingling with the response-side fix; that's covered by the
	// _M2T_ResponseData0xAA_Classified test below).
	wire = append(wire, responseSegmentBytes([]byte{0x11, 0x55})...)
	wire = append(wire, protocol.SymbolAck, protocol.SymbolSyn)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventTransaction)
	if event.FrameType != protocol.FrameTypeInitiatorTarget {
		t.Errorf("frame type = %v; want FrameTypeInitiatorTarget", event.FrameType)
	}
	if got, want := len(event.Request.Data), 3; got != want {
		t.Errorf("request data len = %d; want %d", got, want)
	}
	if event.Request.Data[1] != 0xAA {
		t.Errorf("request.Data[1] = 0x%02X; want 0xAA (mid-frame logical 0xAA)", event.Request.Data[1])
	}

	snapshot := reconstructor.Snapshot()
	if got := snapshot.AbandonsByReason["corrupted_request"]; got != 0 {
		t.Errorf("AbandonsByReason[corrupted_request] = %d; want 0 (request data 0xAA must not abandon)", got)
	}
}

// TestPassiveReconstructor_P71_M2T_ResponseData0xAA_Classified verifies
// the symmetric response-side case: an M2T transaction whose response
// data contains a logical 0xAA byte classifies cleanly. Pre-P7.1 the
// SYN-valued byte mid-response would have been treated as
// unexpected_syn and abandoned.
func TestPassiveReconstructor_P71_M2T_ResponseData0xAA_Classified(t *testing.T) {
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
	wire := append([]byte{protocol.SymbolSyn}, requestFrameBytes(request)...)
	wire = append(wire, protocol.SymbolAck)
	// Response data carries a logical 0xAA byte.
	wire = append(wire, responseSegmentBytes([]byte{0x11, 0xAA, 0x55})...)
	wire = append(wire, protocol.SymbolAck, protocol.SymbolSyn)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventTransaction)
	if !event.HasResponse {
		t.Errorf("HasResponse = false; want true")
	}
	if got, want := len(event.Response.Data), 3; got != want {
		t.Errorf("response data len = %d; want %d", got, want)
	}
	if event.Response.Data[1] != 0xAA {
		t.Errorf("response.Data[1] = 0x%02X; want 0xAA (mid-response logical 0xAA)", event.Response.Data[1])
	}

	snapshot := reconstructor.Snapshot()
	if got := snapshot.AbandonsByReason["unexpected_syn"]; got != 0 {
		t.Errorf("AbandonsByReason[unexpected_syn] = %d; want 0 (response data 0xAA must not abandon)", got)
	}
}

// TestPassiveReconstructor_P71_Broadcast_Data0xAA_Classified verifies
// the broadcast-frame case — pre-P7.1, a broadcast whose data contained
// 0xAA would have abandoned at the SYN branch (since broadcast commit
// defers to the trailing wire SYN per Codex P7 Pass 3). With P7.1 the
// 0xAA byte is treated as data and the trailing SYN reaches the
// LEN-based commit cleanly.
func TestPassiveReconstructor_P71_Broadcast_Data0xAA_Classified(t *testing.T) {
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
		Data:      []byte{0x42, 0xAA, 0x99},
	}
	wire := append([]byte{protocol.SymbolSyn}, frameBytes(frame)...)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	if event.FrameType != protocol.FrameTypeBroadcast {
		t.Errorf("frame type = %v; want FrameTypeBroadcast", event.FrameType)
	}
	if got, want := len(event.Request.Data), 3; got != want {
		t.Errorf("request data len = %d; want %d", got, want)
	}
	if event.Request.Data[1] != 0xAA {
		t.Errorf("request.Data[1] = 0x%02X; want 0xAA (broadcast data 0xAA)", event.Request.Data[1])
	}

	snapshot := reconstructor.Snapshot()
	if got := snapshot.AbandonsByReason["corrupted_request"]; got != 0 {
		t.Errorf("AbandonsByReason[corrupted_request] = %d; want 0 (broadcast data 0xAA must not abandon)", got)
	}
}

// TestPassiveReconstructor_P71_RequestCRC0xAA_Commits constructs a
// request frame whose CRC byte (terminal byte at position 6+LEN-1)
// equals 0xAA. Verifies that the LEN-based commit at line 593-602
// fires correctly: the 0xAA byte is appended as data (because
// isMidRequestFrame returns true at that position), then the next-cycle
// length check `len(requestRaw) == 6+LEN` triggers commit.
func TestPassiveReconstructor_P71_RequestCRC0xAA_Commits(t *testing.T) {
	t.Parallel()

	// Brute-force a payload whose CRC is exactly 0xAA. The first
	// 5-byte data combination that produces CRC=0xAA wins.
	var data []byte
	for i := 0; i < 256 && data == nil; i++ {
		candidateData := []byte{byte(i)}
		header := []byte{0x10, 0x08, 0xB5, 0x09, byte(len(candidateData))}
		full := append(header, candidateData...)
		if protocol.CRC(full) == 0xAA {
			data = candidateData
			break
		}
	}
	if data == nil {
		// Fallback: single-byte search exhausted; widen to 2 bytes.
		for i := 0; i < 256 && data == nil; i++ {
			for j := 0; j < 256 && data == nil; j++ {
				candidateData := []byte{byte(i), byte(j)}
				header := []byte{0x10, 0x08, 0xB5, 0x09, byte(len(candidateData))}
				full := append(header, candidateData...)
				if protocol.CRC(full) == 0xAA {
					data = candidateData
					break
				}
			}
		}
	}
	if data == nil {
		t.Fatal("could not synthesise a payload with CRC=0xAA in 1- or 2-byte space")
	}

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
		Data:      data,
	}
	requestBytes := requestFrameBytes(request)
	if requestBytes[len(requestBytes)-1] != 0xAA {
		t.Fatalf("synthesised request CRC = 0x%02X; want 0xAA", requestBytes[len(requestBytes)-1])
	}

	wire := append([]byte{protocol.SymbolSyn}, requestBytes...)
	wire = append(wire, protocol.SymbolAck)
	wire = append(wire, responseSegmentBytes([]byte{0x42})...)
	wire = append(wire, protocol.SymbolAck, protocol.SymbolSyn)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventTransaction)
	if event.FrameType != protocol.FrameTypeInitiatorTarget {
		t.Errorf("frame type = %v; want FrameTypeInitiatorTarget", event.FrameType)
	}

	snapshot := reconstructor.Snapshot()
	if got := snapshot.AbandonsByReason["corrupted_request"]; got != 0 {
		t.Errorf("AbandonsByReason[corrupted_request] = %d; want 0 (CRC=0xAA must commit)", got)
	}
}

// TestPassiveReconstructor_P71_ResponseCRC0xAA_Commits is the
// response-region symmetric of RequestCRC0xAA — Codex P7.1 review
// addition. Synthesises a response payload whose response CRC equals
// 0xAA and verifies the transaction classifies cleanly.
func TestPassiveReconstructor_P71_ResponseCRC0xAA_Commits(t *testing.T) {
	t.Parallel()

	// Brute-force a response payload whose CRC is 0xAA. Response CRC
	// scope is `[RESP_LEN, RESP_DATA…]`.
	var responseData []byte
	for i := 0; i < 256 && responseData == nil; i++ {
		candidate := []byte{byte(i)}
		hdr := []byte{byte(len(candidate))}
		full := append(hdr, candidate...)
		if protocol.CRC(full) == 0xAA {
			responseData = candidate
			break
		}
	}
	if responseData == nil {
		for i := 0; i < 256 && responseData == nil; i++ {
			for j := 0; j < 256 && responseData == nil; j++ {
				candidate := []byte{byte(i), byte(j)}
				hdr := []byte{byte(len(candidate))}
				full := append(hdr, candidate...)
				if protocol.CRC(full) == 0xAA {
					responseData = candidate
					break
				}
			}
		}
	}
	if responseData == nil {
		t.Fatal("could not synthesise a response with CRC=0xAA in 1- or 2-byte space")
	}

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
	wire := append([]byte{protocol.SymbolSyn}, requestFrameBytes(request)...)
	wire = append(wire, protocol.SymbolAck)
	respBytes := responseSegmentBytes(responseData)
	if respBytes[len(respBytes)-1] != 0xAA {
		t.Fatalf("synthesised response CRC = 0x%02X; want 0xAA", respBytes[len(respBytes)-1])
	}
	wire = append(wire, respBytes...)
	wire = append(wire, protocol.SymbolAck, protocol.SymbolSyn)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventTransaction)
	if !event.HasResponse {
		t.Errorf("HasResponse = false; want true")
	}

	snapshot := reconstructor.Snapshot()
	if got := snapshot.AbandonsByReason["unexpected_syn"]; got != 0 {
		t.Errorf("AbandonsByReason[unexpected_syn] = %d; want 0 (response CRC=0xAA must commit)", got)
	}
}

// TestPassiveReconstructor_P71_WireSYNBeforeLENStillAbandons proves the
// negative case: a SYN-valued byte arriving while requestRaw is below
// 5 bytes (LEN byte not yet observed) is still treated as a wire SYN
// and abandons. This covers the "isMidRequestFrame returns false when
// len < 5" branch and ensures the fix does not silently mask
// genuinely-corrupt arbitration fragments.
func TestPassiveReconstructor_P71_WireSYNBeforeLENStillAbandons(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Wire: SYN (gates Layer 1) + master-class src + DST + PB + SB + SYN.
	// 4 bytes accumulated when the SYN arrives — still below LEN
	// position (5). isMidRequestFrame returns false → SYN-handling path
	// → abandon. requestRaw len > 3 disqualifies the
	// arbitration_fragment classification, so the abandon reason is
	// corrupted_request — proving the predicate didn't silently swallow
	// the premature SYN as data.
	wire := []byte{
		protocol.SymbolSyn, // gate
		0x10,               // master src (BASV2-master)
		0x08,               // DST (BAI)
		0xB5,               // PB
		0x09,               // SB
		protocol.SymbolSyn, // structurally-impossible SYN at LEN position
	}
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonCorruptedRequest {
		t.Errorf("AbandonReason = %v; want corrupted_request (premature SYN must still abandon)", event.AbandonReason)
	}
}

// TestPassiveReconstructor_P71_M2I_RequestData0xAA_Classified covers
// the M2I (initiator/initiator) request-only path with a 0xAA in data.
func TestPassiveReconstructor_P71_M2I_RequestData0xAA_Classified(t *testing.T) {
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
		Data:      []byte{0x03, 0xAA, 0x04},
	}
	wire := append([]byte{protocol.SymbolSyn}, requestFrameBytes(frame)...)
	wire = append(wire, protocol.SymbolAck, protocol.SymbolSyn)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventMasterFrame)
	if event.FrameType != protocol.FrameTypeInitiatorInitiator {
		t.Errorf("frame type = %v; want FrameTypeInitiatorInitiator", event.FrameType)
	}
	if got, want := len(event.Request.Data), 3; got != want {
		t.Errorf("request data len = %d; want %d", got, want)
	}
	if event.Request.Data[1] != 0xAA {
		t.Errorf("request.Data[1] = 0x%02X; want 0xAA", event.Request.Data[1])
	}
}
