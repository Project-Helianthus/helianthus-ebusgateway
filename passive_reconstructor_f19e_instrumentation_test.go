package ebusgateway

// F-19e (batch-18) forensic instrumentation tests for the passive
// reconstructor's `unexpected_symbol` abandons. F-19e is
// INSTRUMENTATION-ONLY — no behavior changes. The tests pin:
//
//   1. The `OffendingSymbol` + `OffendingWasEscaped` fields propagate
//      from the symbol-handler to the emitted PassiveClassifiedEvent
//      at each of the three WaitACK / WaitFinalACK / WaitTerminal
//      abandon sites where `unexpected_symbol` currently fires in
//      production (~0.7 events/min per
//      _work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-13-batch18.md).
//
//   2. The forensic log line emitted by logForensicsLocked includes
//      the new `offending_symbol=0xXX offending_was_escaped=v` tokens
//      so post-deploy bucketing can grep them.
//
//   3. F-19c and F-19d forensic behavior is unchanged — the new
//      fields are additive; legacy req_raw/resp_raw/phase/src/dst
//      tokens still emit unchanged, and the AbandonReason set is
//      stable.
//
// Spec references:
//   - _work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-13-batch18.md
//     (live evidence: 50 unexpected_symbol events / 69 min).
//   - F-19d batch-17 wasEscaped plumbing (passive_bus_tap.go +
//     passive_transaction_reconstructor.go).
//   - john30/ebusd `symbol.h:79-82` (escape rule: A9 00→A9, A9 01→AA,
//     raw AA → wire SYN).

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// TestF19e_UnexpectedSymbol_AtWaitACK drives the reconstructor through
// a valid M2T request and then feeds a byte that is neither ACK
// (0x00), NACK (0xFF), nor SYN (0xAA) at the WaitACK phase. The
// default arm of handleACKSymbolLocked must abandon with
// reason=unexpected_symbol AND populate event.OffendingSymbol with
// the exact byte (0x42) plus event.OffendingWasEscaped=false.
func TestF19e_UnexpectedSymbol_AtWaitACK(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	req := protocol.Frame{
		Source:    0x10,
		Target:    0x08, // BAI — target-class, drives M2T
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x55},
	}
	reqBytes := requestFrameBytes(req)

	// Wire: [SYN gate] [request bytes ending in CRC] [0x42 offending]
	wire := append([]byte{protocol.SymbolSyn}, reqBytes...)
	wire = append(wire, 0x42)

	// Explicit all-false mask — every byte is raw-wire, no escape
	// decoding. 0x42 is not 0xAA so the mask state is irrelevant at
	// that index; the explicit false documents intent for the
	// post-CRC byte.
	escapes := make([]bool, len(wire))
	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonUnexpectedSymbol {
		t.Fatalf("AbandonReason = %v; want unexpected_symbol", event.AbandonReason)
	}
	if event.OffendingSymbol != 0x42 {
		t.Fatalf("OffendingSymbol = 0x%02X; want 0x42 (the byte that arrived at WaitACK default arm)", event.OffendingSymbol)
	}
	if event.OffendingWasEscaped {
		t.Fatalf("OffendingWasEscaped = true; want false (raw-wire byte, not escape-decoded)")
	}
}

// TestF19e_UnexpectedSymbol_AtWaitFinalACK drives a complete request
// + ACK + response, then feeds a non-ACK/NACK/SYN byte at WaitFinalACK.
// The default arm of handleFinalACKSymbolLocked must abandon with
// reason=unexpected_symbol and capture the offending byte.
func TestF19e_UnexpectedSymbol_AtWaitFinalACK(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	req := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x55},
	}
	reqBytes := requestFrameBytes(req)

	// Response: NN_s=0x01, data=0x77, CRC computed from those two
	// bytes. The reconstructor only requires len(responseRaw) ==
	// responseExpectedLen to advance to WaitFinalACK — the CRC
	// validity is recorded separately on state.responseCRCValid but
	// does NOT block the phase transition. We compute the correct
	// CRC anyway so the path through parsePassiveResponse is the
	// clean "frame parsed" branch.
	respPayload := []byte{0x01, 0x77}
	respCRC := protocol.CRC(respPayload)
	resp := append(respPayload, respCRC)

	// Wire layout:
	//   [SYN gate] [request..CRC] [target ACK = 0x00] [resp..CRC]
	//   [0x42 offending]
	wire := append([]byte{protocol.SymbolSyn}, reqBytes...)
	wire = append(wire, protocol.SymbolAck) // S_ACK
	wire = append(wire, resp...)
	wire = append(wire, 0x42) // offending at WaitFinalACK

	escapes := make([]bool, len(wire))
	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonUnexpectedSymbol {
		t.Fatalf("AbandonReason = %v; want unexpected_symbol", event.AbandonReason)
	}
	if event.OffendingSymbol != 0x42 {
		t.Fatalf("OffendingSymbol = 0x%02X; want 0x42 (the byte that arrived at WaitFinalACK default arm)", event.OffendingSymbol)
	}
	if event.OffendingWasEscaped {
		t.Fatalf("OffendingWasEscaped = true; want false")
	}
}

// TestF19e_UnexpectedSymbol_AtWaitTerminal drives an M2I (initiator-
// initiator) transaction: request + ACK transitions directly to
// WaitTerminal (no response phase). The next byte must be a wire SYN;
// any other byte triggers the non-SYN abandon path with
// reason=unexpected_symbol.
func TestF19e_UnexpectedSymbol_AtWaitTerminal(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// M2I: target is also an initiator address. 0x71 has nibbles 7
	// and 1, both in the initiator-nibble set {0,1,3,7,F}, so
	// protocol.FrameType resolves to InitiatorInitiator.
	req := protocol.Frame{
		Source:    0x10,
		Target:    0x71,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x55},
	}
	reqBytes := requestFrameBytes(req)

	wire := append([]byte{protocol.SymbolSyn}, reqBytes...)
	wire = append(wire, protocol.SymbolAck) // target ACK; for M2I this advances directly to WaitTerminal
	wire = append(wire, 0x42)               // offending non-SYN byte

	escapes := make([]bool, len(wire))
	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonUnexpectedSymbol {
		t.Fatalf("AbandonReason = %v; want unexpected_symbol", event.AbandonReason)
	}
	if event.OffendingSymbol != 0x42 {
		t.Fatalf("OffendingSymbol = 0x%02X; want 0x42 (the byte that arrived at WaitTerminal non-SYN guard)", event.OffendingSymbol)
	}
	if event.OffendingWasEscaped {
		t.Fatalf("OffendingWasEscaped = true; want false")
	}
}

// TestF19e_LogLineFormat captures the forensic log emitted by
// logForensicsLocked for an UnexpectedSymbol abandon and asserts the
// new tokens render exactly as `offending_symbol=0x42
// offending_was_escaped=false`. Pins the wire format so post-deploy
// log-bucketing scripts can grep deterministically.
func TestF19e_LogLineFormat(t *testing.T) {
	// No t.Parallel(): the test captures the global log writer.

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	req := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x55},
	}
	reqBytes := requestFrameBytes(req)
	wire := append([]byte{protocol.SymbolSyn}, reqBytes...)
	wire = append(wire, 0x42) // offending at WaitACK default

	var logBuf bytes.Buffer
	restore := captureGoLog(&logBuf)
	defer restore()

	escapes := make([]bool, len(wire))
	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonUnexpectedSymbol {
		t.Fatalf("AbandonReason = %v; want unexpected_symbol", event.AbandonReason)
	}

	logText := logBuf.String()
	// Exact-token pins. `0x%02X` upper-cases the hex digits to match
	// the existing src/dst/prim/sec rendering convention used by
	// logForensicsLocked.
	if !strings.Contains(logText, "offending_symbol=0x42") {
		t.Fatalf("forensic log missing `offending_symbol=0x42` token. Got:\n%s", logText)
	}
	if !strings.Contains(logText, "offending_was_escaped=false") {
		t.Fatalf("forensic log missing `offending_was_escaped=false` token. Got:\n%s", logText)
	}
	// The new tokens must appear AFTER the existing observed_at
	// token so legacy regexes anchored at the leading fields keep
	// matching. Verify ordering:
	idxObservedAt := strings.Index(logText, "observed_at=")
	idxOffending := strings.Index(logText, "offending_symbol=")
	if idxObservedAt < 0 || idxOffending < 0 || idxOffending < idxObservedAt {
		t.Fatalf("`offending_symbol=` must appear after `observed_at=` to preserve legacy regex compatibility. Got:\n%s", logText)
	}
	// Sanity: existing reason/phase/src/dst tokens still present.
	for _, want := range []string{
		"reason=unexpected_symbol",
		"phase=",
		"src=0x10",
		"dst=0x08",
		"prim=0xB5",
		"sec=0x16",
	} {
		if !strings.Contains(logText, want) {
			t.Fatalf("F-19e instrumentation must not regress legacy log tokens: missing %q in:\n%s", want, logText)
		}
	}
}

// TestF19e_F19c_UnchangedForensics replays the live-evidence
// invalid_nn_m=0x84 wire pattern from F-19c (batch-16) and asserts:
//
//	(a) the abandon reason is still invalid_nn_m (no F-19c regression);
//	(b) OffendingSymbol equals 0x84 (the bogus NN_m), with
//	    OffendingWasEscaped=false (NN_m=0x84 cannot arise from an
//	    escape sequence in this synthetic feed);
//	(c) the legacy forensic log tokens (req_raw=, reason=) still
//	    emit alongside the new offending tokens.
func TestF19e_F19c_UnchangedForensics(t *testing.T) {
	// No t.Parallel(): captures global log writer.

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Replicates the batch-16 live evidence: 0x10 0x08 0x00 0x09 0x84.
	// At rawLen=5, the per-offset NN_m check fires.
	wire := []byte{
		protocol.SymbolSyn,
		0x10, 0x08, 0x00, 0x09,
		0x84, // NN_m = 132 — invalid
	}

	var logBuf bytes.Buffer
	restore := captureGoLog(&logBuf)
	defer restore()

	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonInvalidNNMaster {
		t.Fatalf("AbandonReason = %v; want invalid_nn_m (F-19c regression guard)", event.AbandonReason)
	}
	if event.OffendingSymbol != 0x84 {
		t.Fatalf("OffendingSymbol = 0x%02X; want 0x84 (the bogus NN_m)", event.OffendingSymbol)
	}
	if event.OffendingWasEscaped {
		t.Fatalf("OffendingWasEscaped = true; want false (auto-escape mask never marks NN_m position)")
	}

	logText := logBuf.String()
	for _, want := range []string{
		"reason=invalid_nn_m",
		"req_raw=10 08 00 09 84",
		"offending_symbol=0x84",
		"offending_was_escaped=false",
	} {
		if !strings.Contains(logText, want) {
			t.Fatalf("F-19c forensic log regression — missing %q in:\n%s", want, logText)
		}
	}
}

// TestF19e_F19d_UnchangedForensics replays an F-19d mid-frame wire-SYN
// abort pattern and asserts the OffendingWasEscaped field reflects the
// upstream truth — a raw wire SYN is wasEscaped=false. Pins that
// F-19d's wasEscaped plumbing is still flowing through the new
// instrumentation channel without being masked or inverted.
func TestF19e_F19d_UnchangedForensics(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Mid-request wire SYN: announce NN_m=15 then send 1 data byte
	// followed by an un-escaped 0xAA. F-19d's SYN-trigger path fires
	// with reason=unexpected_syn.
	wire := []byte{
		protocol.SymbolSyn,           // gate
		0x10, 0x26, 0xB5, 0x23, 0x0F, // SRC DST PB SB NN_m=15
		0x05,               // DATA[0]
		protocol.SymbolSyn, // un-escaped wire SYN — mid-frame terminator
	}
	escapes := make([]bool, len(wire)) // all false — every byte is raw-wire
	feedPassiveSymbolsRawWithEscapes(reconstructor, time.Unix(0, 0), wire, escapes)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonUnexpectedSYN {
		t.Fatalf("AbandonReason = %v; want unexpected_syn (F-19d regression guard)", event.AbandonReason)
	}
	if event.OffendingSymbol != protocol.SymbolSyn {
		t.Fatalf("OffendingSymbol = 0x%02X; want 0x%02X (the wire SYN that terminated the frame)", event.OffendingSymbol, protocol.SymbolSyn)
	}
	if event.OffendingWasEscaped {
		t.Fatalf("OffendingWasEscaped = true; want false (un-escaped wire SYN)")
	}
}
