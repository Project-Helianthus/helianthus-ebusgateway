package ebusgateway

// F-19 regression tests
// (_work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-12-batch15.md).
//
// F-19a — LEN-completion parseFrame failure must abandon early instead
// of continuing to absorb bytes from the next frame. The pre-F-19a
// path "kept accumulating", cascading the corruption into the next
// frame's buffer. Live evidence (batch-14): ~146 src=0x10 abandons per
// 30k log lines, all matching the `req_raw=10 26 B5 23 01 AA AA`
// shape (LEN=1 buffer where both 0xAA bytes were absorbed as
// data+CRC and the CRC check failed).
//
// F-19b — A 4-byte buffer reaching SB but not LEN is structurally an
// arbitration fragment (lost arbitration / wire byte loss), not a
// corrupted_request. The pre-F-19b threshold `<= 3` mis-classified
// these. Live evidence (batch-14): ~115 src=0xF1 abandons per 30k
// lines, all in the 4-byte truncated shape.

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// TestPassiveReconstructor_F19a_LenCompletionCRCFail_AbandonsEarly is
// the headline F-19a invariant: the operator's `req_raw=10 26 B5 23
// 01 AA AA` example must produce ONE abandon event at the second
// 0xAA's timestamp, with no carryover of buffered bytes into the next
// frame.
func TestPassiveReconstructor_F19a_LenCompletionCRCFail_AbandonsEarly(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Wire: SYN gate + the operator's example bytes.
	// eBUS CRC8(0x9B) of `10 26 B5 23 01 AA` is 0x7C — so when both
	// 0xAA bytes are absorbed as data+CRC by the F-19a-affected
	// predicate path, the buffer's CRC check (CRC byte == 0xAA)
	// fails (expected 0x7C). Pre-F-19a: parser continued accumulating
	// until the next SYN/over-length. Post-F-19a: abandon at this
	// exact byte boundary.
	wire := []byte{
		protocol.SymbolSyn, // gate
		0x10,               // SRC
		0x26,               // DST
		0xB5,               // PB
		0x23,               // SB
		0x01,               // LEN=1
		0xAA,               // DATA[0] — absorbed by isMidRequestFrame
		0xAA,               // CRC position — also absorbed; CRC check
		// will fail (real CRC is 0x7C).
	}
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonCorruptedRequest {
		t.Errorf("AbandonReason = %v; want corrupted_request (F-19a: parseFrame failure at LEN-completion must abandon as corrupted_request when not self-echo / scan-collision)", event.AbandonReason)
	}

	// F-19a Layer-1 invariant: no wire SYN was consumed when the
	// buffer hit len=6+LEN, so the next observed wire SYN must
	// re-engage the synced gate. Feed a valid follow-up frame to
	// pin this: a broadcast frame from a different SRC.
	follow := protocol.Frame{
		Source:    0x30,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x07},
	}
	feedPassiveSymbols(reconstructor, time.Unix(0, 1), frameBytes(follow))

	next := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	if next.Request.Source != 0x30 {
		t.Errorf("follow-up broadcast Request.Source = 0x%02X; want 0x30 (F-19a: the abandoned buffer must not have leaked into the next frame's reconstruction)", next.Request.Source)
	}
}

// TestPassiveReconstructor_F19a_PreservesClassification_SelfEcho pins
// angry-tester Finding C: the new early-abandon path must replicate
// the SYN-triggered path's classification (self_echo / scan_collision)
// — not silently downgrade everything to corrupted_request.
func TestPassiveReconstructor_F19a_PreservesClassification_SelfEcho(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	reconstructor.SetLocalAddressSnapshotter(testLocalSnapshotter{address: 0x71, known: true})
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Same shape as the headline F-19a test but with SRC=0x71 (the
	// gateway's own address). isSelfOriginatedRaw matches → abandon
	// classifies as self_echo, not corrupted_request.
	wire := []byte{
		protocol.SymbolSyn,
		0x71, // gateway SRC
		0x26,
		0xB5,
		0x23,
		0x01,
		0xAA,
		0xAA,
	}
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonSelfEcho {
		t.Errorf("AbandonReason = %v; want self_echo (F-19a classification replication: SRC=0x71 must classify as self_echo even on the new early-abandon path)", event.AbandonReason)
	}
}

// TestPassiveReconstructor_F19a_PreservesClassification_ScanCollision
// pins the other half of angry-tester Finding C: scan-probe shaped
// requests (PB=0x07 SB=0x04) that fail CRC at LEN-completion must
// classify as scan_collision, not corrupted_request.
func TestPassiveReconstructor_F19a_PreservesClassification_ScanCollision(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Wire: SYN gate + a scan probe (PB=0x07 SB=0x04) with garbled
	// data+CRC bytes (both 0xAA). isScanProbeRaw matches via the
	// raw[2]==0x07 raw[3]==0x04 check, so abandon classifies as
	// scan_collision.
	wire := []byte{
		protocol.SymbolSyn,
		0x10, // SRC
		0xFE, // DST (broadcast — typical scan dest)
		0x07, // PB (scan)
		0x04, // SB (scan)
		0x01, // LEN
		0xAA, // DATA[0]
		0xAA, // CRC position (will fail CRC validation)
	}
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonScanCollision {
		t.Errorf("AbandonReason = %v; want scan_collision (F-19a classification replication: PB=0x07 SB=0x04 must classify as scan_collision on the new early-abandon path)", event.AbandonReason)
	}
}

// TestPassiveReconstructor_F19b_FourByteFragment_ClassifiesAsArbitrationFragment
// pins the F-19b reclassification: a 4-byte buffer reaching SB but
// not LEN must classify as arbitration_fragment (was
// corrupted_request pre-F-19b). The operator's `req_raw=F1 15 B5 24`
// example exemplifies this shape.
func TestPassiveReconstructor_F19b_FourByteFragment_ClassifiesAsArbitrationFragment(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Wire: SYN gate + the operator's example: 4 bytes then SYN at
	// LEN position. Layer 1 syncs, 4 bytes accumulate, SYN at
	// position 5 → isMidRequestFrame returns false → SYN-handling
	// path. Pre-F-19b: corrupted_request (len > 3). Post-F-19b:
	// arbitration_fragment (len < 5).
	wire := []byte{
		protocol.SymbolSyn,
		0xF1, // SRC (NETX3-like initiator)
		0x15, // DST
		0xB5, // PB
		0x24, // SB
		protocol.SymbolSyn,
	}
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonArbitrationFragment {
		t.Errorf("AbandonReason = %v; want arbitration_fragment (F-19b: len<5 truncation must classify as arbitration_fragment, not corrupted_request)", event.AbandonReason)
	}
}

// TestPassiveReconstructor_F19_NoRegression_ValidLEN1Frame proves the
// fix does NOT regress legitimate frames where data or CRC happens to
// be 0xAA (P7.1 invariant). Constructs a LEN=1 broadcast frame with
// DATA[0]=0xAA and the actual computed CRC; the frame must commit.
func TestPassiveReconstructor_F19_NoRegression_ValidLEN1Frame(t *testing.T) {
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
		Data:      []byte{0xAA}, // legitimate 0xAA payload byte
	}
	// frameBytes appends the actual computed CRC + trailing SYN. The
	// CRC of this specific frame won't equal 0xAA by accident, so this
	// exercises the "0xAA in data but CRC is the real value" case.
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), frameBytes(frame))

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	if !event.HasRequest {
		t.Fatal("F-19 regression: valid LEN=1 frame with DATA[0]=0xAA must commit, not abandon")
	}
	if len(event.Request.Data) != 1 || event.Request.Data[0] != 0xAA {
		t.Fatalf("F-19 regression: Request.Data = % X; want [AA]", event.Request.Data)
	}
}

// TestPassiveReconstructor_F19_NoRegression_BroadcastDeferToSYN is the
// regression guard for the disambiguation fix in the implementation:
// when parseFrame succeeds at LEN-completion but the frame is a
// Broadcast (whose canonical commit is on the trailing SYN per the
// commitRequestFrameLocked docstring), the new F-19a code path must
// NOT abandon. It must fall through to the SYN-trigger path which
// dispatches the broadcast cleanly.
func TestPassiveReconstructor_F19_NoRegression_BroadcastDeferToSYN(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Standard broadcast: parseFrame succeeds at LEN-completion but
	// commitRequestFrameLocked defers to SYN-trigger for canonical
	// timing.
	frame := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x01, 0x02},
	}
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), frameBytes(frame))

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	if !event.HasRequest {
		t.Fatal("F-19a regression: broadcast frames where parseFrame succeeds at LEN-completion must defer to SYN-trigger and commit, NOT abandon")
	}
}

// TestPassiveReconstructor_F19a_LayerOneInvariant_NoAfterSynReset pins
// the Layer 1 invariant: after F-19a's early abandon, the synced gate
// must be re-engaged via the NEXT wire SYN (not held over from before
// the abandon). Implementation calls resetStateLocked (NOT
// resetStateLockedAfterSyn) because no wire SYN was consumed at the
// LEN+CRC boundary. This test verifies the Layer 1 gate behavior by
// feeding a non-SYN byte immediately after the abandon and confirming
// it does NOT engage frame collection.
func TestPassiveReconstructor_F19a_LayerOneInvariant_NoAfterSynReset(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Step 1: trigger F-19a abandon.
	wire := []byte{
		protocol.SymbolSyn,
		0x10,
		0x26,
		0xB5,
		0x23,
		0x01,
		0xAA,
		0xAA,
	}
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	event := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventAbandonedTransaction)
	if event.AbandonReason != PassiveAbandonReasonCorruptedRequest {
		t.Fatalf("setup: F-19a abandon AbandonReason = %v; want corrupted_request", event.AbandonReason)
	}

	// Step 2: feed a non-SYN byte. The synced gate should be CLOSED
	// (resetStateLocked was called, not resetStateLockedAfterSyn).
	// The byte should be dropped silently — no new abandon event.
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 1), []byte{0x30, 0x08, 0xB5, 0x09})

	// Step 3: feed a wire SYN + a valid follow-up frame. The SYN
	// re-engages the synced gate via the Idle handler; the follow-up
	// frame must reconstruct cleanly.
	follow := protocol.Frame{
		Source:    0x30,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x07},
	}
	wireFollow := append([]byte{protocol.SymbolSyn}, frameBytes(follow)...)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 2), wireFollow)

	next := requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	if next.Request.Source != 0x30 {
		t.Fatalf("F-19a Layer 1 invariant: follow-up frame Source = 0x%02X; want 0x30 (the dropped bytes between abandon and SYN must not have polluted the next frame's reconstruction)", next.Request.Source)
	}
}
