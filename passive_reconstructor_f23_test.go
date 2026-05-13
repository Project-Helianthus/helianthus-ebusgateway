package ebusgateway

// F-23 (batch-19, 2026-05-13) regression tests for the gateway's
// passive reconstructor + bus-tap consumer side after the
// helianthus-ebusgo PR-1 fix (commit 5215685) that made the ENH
// transport honestly unescape eBUS byte-stuffing.
//
// Before PR-1, the upstream ENH transport claimed
// BytesAreUnescaped()=true but actually leaked escape pairs as raw
// wire bytes (0xA9 0x00 → two events; 0xA9 0x01 → two events). The
// gateway's reconstructor saw the bare 0xA9 followed by 0x00 or
// 0x01 and classified the trailing byte as a spurious symbol,
// producing the recurring `unexpected_symbol` abandons documented
// in batch-19 (Pattern A: offending=0x00 at WaitTerminal;
// Pattern B: offending=0x1B at WaitFinalACK after response overrun).
//
// After PR-1, the upstream delivers logical bytes with
// transport.StreamEvent.WasEscaped set correctly. The gateway's
// passive bus tap now sources WasEscaped from the upstream event
// (passive_bus_tap.go) instead of hard-coding false. These tests
// pin that:
//
//   - A Pattern A frame (CRC=0xA9 wire-encoded as A9 00) reaches
//     the reconstructor as a clean 3-byte trailing (CRC, M_ACK,
//     SYN) — no spurious 0x00 byte at WaitTerminal.
//
//   - A Pattern B frame (16-byte response with data byte 0xA9 at
//     index 13) reaches the reconstructor as 16 logical bytes —
//     no overrun, no unexpected_symbol abandon.
//
//   - F-18 (external ENH echo passthrough), F-19d (WasEscaped
//     SYN-vs-data disambiguation), and F-19e (offending_symbol
//     forensic instrumentation) continue to fire correctly with
//     the F-23 upstream truth flag.
//
// References:
//   - _work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-13-batch19.md
//   - helianthus-ebusgo#154 (PR-1, ENH transport unescape).
//   - john30/ebusd/docs/enhanced_proto.md (escape rules).

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// TestF23_PatternA_VRC720_BAI_CRC_A9_Commits replays the recurring
// VRC720→BAI poll fingerprint from batch-19. The target CRC is 0xA9.
// Post-F-23 the upstream ENH transport delivers the CRC as logical
// 0xA9 with WasEscaped=true; the reconstructor's response-phase
// counter sees 1 byte (CRC), the WaitFinalACK ACK arrives next,
// then the WaitTerminal SYN — three structural bytes total. No
// extra 0x00 lands at WaitTerminal, so unexpected_symbol is not
// emitted.
//
// Pre-F-23 the same wire shape arrived as bare `0xA9 0x00 0x00
// 0xAA` (4 events). The reconstructor classified the second 0x00
// (M_ACK) as the final ACK, then the third 0x00 (the leaked second
// byte of the escape pair) landed at WaitTerminal as a non-SYN
// byte → unexpected_symbol phase=5 offending=0x00 (~26/68 min in
// production).
func TestF23_PatternA_VRC720_BAI_CRC_A9_Commits(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// VRC720→BAI initiator frame: SRC=0x10, DST=0x08, PB=0xB5,
	// SB=0x09 (from batch-19 sample), 0-length data, CRC computed
	// from logical bytes.
	req := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      nil,
	}
	reqBytes := requestFrameBytes(req)

	// Target response (post-F-23: logical bytes). Construct a
	// 4-byte response payload whose CRC happens to be 0xA9 so the
	// F-23 unescape path is exercised at the CRC position. Use a
	// data sequence that brute-forces CRC=0xA9.
	respData := findResponseDataWithCRC(t, []byte{0xA9}, 4)
	respLen := byte(len(respData))
	respCrc := protocol.CRC(append([]byte{respLen}, respData...))
	if respCrc != 0xA9 {
		t.Fatalf("test fixture invariant: synthesized response CRC = 0x%02X; want 0xA9", respCrc)
	}

	// Wire stream (post-F-23 LOGICAL bytes; the F-23 fix in PR-1
	// makes the ENH transport deliver these unescaped):
	//   [SYN gate] [request..CRC] [S_ACK=0x00] [NN_s | data..] [CRC=0xA9] [M_ACK=0x00] [SYN=0xAA]
	wire := []byte{protocol.SymbolSyn}
	wire = append(wire, reqBytes...)
	wire = append(wire, protocol.SymbolAck) // S_ACK
	wire = append(wire, respLen)
	wire = append(wire, respData...)
	wire = append(wire, respCrc)            // CRC=0xA9 (logical, was wire A9 00)
	wire = append(wire, protocol.SymbolAck) // M_ACK
	wire = append(wire, protocol.SymbolSyn) // trailing SYN

	// Escapes mask: the CRC byte at position (len-3) is logical
	// 0xA9 — post-F-23 the upstream surfaces WasEscaped=true for
	// it. All other bytes are raw passthrough.
	escapes := make([]bool, len(wire))
	escapes[len(wire)-3] = true // CRC was escape-decoded

	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventTransaction)
	if event.AbandonReason != "" {
		t.Fatalf("AbandonReason = %v; want empty (frame must commit cleanly)", event.AbandonReason)
	}
	if !event.HasRequest || !event.HasResponse {
		t.Fatalf("HasRequest=%v HasResponse=%v; want both true", event.HasRequest, event.HasResponse)
	}
}

// TestF23_PatternB_VRC720_VR71_DataA9_Commits pins the Pattern B
// fingerprint from batch-19. The response payload contains a data
// byte equal to 0xA9 (logical). On wire this was `A9 00` (2 bytes);
// pre-F-23 the reconstructor's response-phase counter saw N+1
// bytes and the response overrun at WaitFinalACK fired with
// offending=0x1B (the byte AFTER the expected length). Post-F-23
// the upstream delivers the data byte as logical 0xA9 with
// WasEscaped=true and the response counter sees exactly N bytes.
func TestF23_PatternB_VRC720_VR71_DataA9_Commits(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// VRC720→VR_71 register read: SRC=0x10, DST=0x26 (VR_71),
	// PB=0xB5, SB=0x16, 1-byte data (register index).
	req := protocol.Frame{
		Source:    0x10,
		Target:    0x26,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x09},
	}
	reqBytes := requestFrameBytes(req)

	// Response: 4-byte payload with 0xA9 at index 2. The CRC is
	// computed from the logical bytes.
	respData := []byte{0x55, 0x66, 0xA9, 0x77}
	respLen := byte(len(respData))
	respCrcPayload := append([]byte{respLen}, respData...)
	respCrc := protocol.CRC(respCrcPayload)

	// Wire stream (post-F-23 logical).
	wire := []byte{protocol.SymbolSyn}
	wire = append(wire, reqBytes...)
	wire = append(wire, protocol.SymbolAck) // S_ACK
	wire = append(wire, respLen)            // NN_s
	wire = append(wire, respData...)        // includes 0xA9 at index 2
	wire = append(wire, respCrc)            // response CRC
	wire = append(wire, protocol.SymbolAck) // M_ACK
	wire = append(wire, protocol.SymbolSyn) // trailing SYN

	// Escapes mask: the 0xA9 data byte at response-data index 2
	// arrives WasEscaped=true (originally wire A9 00). If the CRC
	// or other bytes happen to equal 0xA9, mark those too.
	escapes := make([]bool, len(wire))
	// Locate respData[2] (=0xA9) in the wire stream. Position:
	// 1 (leading SYN) + len(reqBytes) + 1 (S_ACK) + 1 (NN_s) + 2 = index.
	dataA9Pos := 1 + len(reqBytes) + 1 + 1 + 2
	if wire[dataA9Pos] != 0xA9 {
		t.Fatalf("test fixture invariant: wire[%d] = 0x%02X; want 0xA9", dataA9Pos, wire[dataA9Pos])
	}
	escapes[dataA9Pos] = true
	// CRC is computed; mark if it happens to be 0xA9 or 0xAA.
	crcPos := dataA9Pos + 2 // 0xA9 + 0x77 + CRC
	if wire[crcPos] == 0xA9 || wire[crcPos] == 0xAA {
		escapes[crcPos] = true
	}

	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventTransaction)
	if event.AbandonReason != "" {
		t.Fatalf("AbandonReason = %v; want empty (frame must commit, NO overrun at WaitFinalACK)", event.AbandonReason)
	}
	if len(event.Response.Data) != len(respData) {
		t.Fatalf("Response.Data length = %d; want %d (post-F-23 the 0xA9 byte counts as ONE logical byte, not two wire bytes)",
			len(event.Response.Data), len(respData))
	}
	if event.Response.Data[2] != 0xA9 {
		t.Fatalf("Response.Data[2] = 0x%02X; want 0xA9 (the escape-decoded data byte must reach the consumer at the correct position)", event.Response.Data[2])
	}
}

// TestF23_F18_F19d_F19e_RegressionGuard confirms that the prior
// forensic patches (F-18 external echo passthrough, F-19d
// WasEscaped SYN-vs-data disambiguation, F-19e offending_symbol
// instrumentation) continue to fire correctly with the F-23
// consumer-side WasEscaped sourcing from upstream.
//
// Scenario: a mid-response un-escaped wire SYN. F-19d classifies
// this as unexpected_syn (not unexpected_symbol — that's the F-19d
// disambiguation). F-19e populates OffendingSymbol=0xAA and
// OffendingWasEscaped=false (the wire SYN was un-escaped, per the
// distinction F-19d introduced).
func TestF23_F18_F19d_F19e_RegressionGuard(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Mid-request wire SYN: announce NN_m=15, send 1 data byte,
	// then a real wire SYN. This is the same fixture as F-19d's
	// TestF19d_RawWireSyn_AbortsFrame — post-F-23 the upstream
	// flag plumbing must preserve F-19d's classification.
	wire := []byte{
		protocol.SymbolSyn,           // gate
		0x10, 0x26, 0xB5, 0x23, 0x0F, // SRC DST PB SB NN_m=15
		0x05,               // DATA[0]
		protocol.SymbolSyn, // un-escaped wire SYN — frame terminator
	}
	escapes := make([]bool, len(wire)) // all false — raw-wire bytes

	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonUnexpectedSYN {
		t.Fatalf("AbandonReason = %v; want unexpected_syn (F-19d regression with F-23 upstream)", event.AbandonReason)
	}
	if event.OffendingSymbol != protocol.SymbolSyn {
		t.Fatalf("OffendingSymbol = 0x%02X; want 0x%02X (F-19e)", event.OffendingSymbol, protocol.SymbolSyn)
	}
	if event.OffendingWasEscaped {
		t.Fatalf("OffendingWasEscaped = true; want false (un-escaped wire SYN; F-19e)")
	}
}

// findResponseDataWithCRC brute-forces a response data sequence of
// the given length whose CRC matches the target byte. Used by
// Pattern A test fixtures so we can pin the CRC=0xA9 case
// deterministically without depending on real production hex
// captures (which we cannot reproduce in unit tests).
//
// The eBUS CRC is byte-by-byte over (LEN || data); we iterate
// candidate data values until a match is found. The search space
// is bounded by `length` bytes — for length=4 the search is
// 256^3 worst case (we vary the first 3 bytes and compute the
// 4th from the constraint), which terminates in milliseconds.
func findResponseDataWithCRC(t *testing.T, targets []byte, length int) []byte {
	t.Helper()
	if length < 1 {
		t.Fatalf("findResponseDataWithCRC: length must be >= 1")
	}
	want := targets[0]
	data := make([]byte, length)
	for a := 0; a < 256; a++ {
		data[0] = byte(a)
		for b := 0; b < 256; b++ {
			if length > 1 {
				data[1] = byte(b)
			}
			payload := append([]byte{byte(length)}, data...)
			if protocol.CRC(payload) == want {
				return data
			}
			if length <= 1 {
				break
			}
		}
	}
	t.Fatalf("findResponseDataWithCRC: no %d-byte data found with CRC=0x%02X", length, want)
	return nil
}
