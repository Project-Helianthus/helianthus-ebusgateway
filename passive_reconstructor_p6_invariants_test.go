package ebusgateway

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// P6 Layer 1 — inter-frame SYN gate.
//
// Verifies that two complete frames fed back-to-back (each terminated
// by its own SymbolSyn) both classify cleanly. Layer 1 must not break
// the common golden case where the trailing SYN of frame N satisfies
// the inter-frame invariant for frame N+1.
func TestPassiveTransactionReconstructor_RequiresSYNBetweenFrames(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	first := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x01},
	}
	second := protocol.Frame{
		Source:    0x30,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x02},
	}
	payload := append(frameBytes(first), frameBytes(second)...)
	feedPassiveSymbols(reconstructor, time.Unix(0, 0), payload)

	requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
	requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
}

// P6 Layer 1 — drop bytes when the parser is unsynced.
//
// Drives the parser into Idle/synced=false via a transport-reset event
// (handleTransportDiscontinuityLocked → resetStateLocked, which clears
// synced). Then feeds 4 garbage initiator-class bytes — they must be
// dropped silently and counted in PrefixResyncSkippedTotal. Finally a
// SYN + a valid frame must classify normally.
func TestPassiveTransactionReconstructor_DropsBytesUntilSYN_AfterAbandon(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Drive Idle/synced=false via a transport-reset event. This is the
	// real production path: a transport reset / decode fault always
	// requires the parser to re-observe a SYN before accepting frames.
	reconstructor.OnPassiveTapEvent(PassiveTapEvent{
		Kind:       PassiveTapEventReset,
		ObservedAt: time.Unix(0, 0),
	})

	// Feed 4 garbage initiator-class bytes — they must be dropped and
	// counted (Layer 1 cascade applies on each, awaitingResync stays
	// false because none triggers Layer 2 rejection).
	garbage := []byte{0x10, 0x30, 0x71, 0xF1}
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0).Add(time.Second), garbage)

	snapshot := reconstructor.Snapshot()
	if got := snapshot.PrefixResyncSkippedTotal; got != 4 {
		t.Fatalf("PrefixResyncSkippedTotal = %d; want 4 (post-reset garbage drops)", got)
	}
	if got := snapshot.InvalidSrcClassSkippedTotal; got != 0 {
		t.Fatalf("InvalidSrcClassSkippedTotal = %d; want 0", got)
	}

	// Now feed SYN + valid frame; the valid frame must classify.
	valid := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x42},
	}
	resync := append([]byte{protocol.SymbolSyn}, frameBytes(valid)...)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0).Add(2*time.Second), resync)

	requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
}

// P6 Layer 1 — startup-from-cold requires SYN.
//
// Fresh reconstructor; inject non-SYN initiator-class bytes BEFORE any
// SYN. They must be dropped, NO abandon event must fire (the parser
// never enters the request phase), and PrefixResyncSkippedTotal must
// increment exactly once per dropped byte. After SYN + valid frame
// the parser must recover.
func TestPassiveTransactionReconstructor_StartupRequiresSYN(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Pre-SYN garbage — uses feedPassiveSymbolsRaw to bypass the
	// auto-prepend so we can assert behavior on the raw cold-start
	// invariant.
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), []byte{0x10, 0x30, 0xF1})

	snapshot := reconstructor.Snapshot()
	if got := snapshot.PrefixResyncSkippedTotal; got != 3 {
		t.Fatalf("PrefixResyncSkippedTotal after cold pre-SYN = %d; want 3", got)
	}
	if got := snapshot.InvalidSrcClassSkippedTotal; got != 0 {
		t.Fatalf("InvalidSrcClassSkippedTotal = %d; want 0 (cold pre-SYN should NOT trigger Layer 2)", got)
	}
	assertNoPassiveClassifiedEvent(t, subscription, 50*time.Millisecond)

	// Recovery: SYN + valid frame.
	frame := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x01},
	}
	resync := append([]byte{protocol.SymbolSyn}, frameBytes(frame)...)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0).Add(time.Second), resync)
	requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
}

// P6 Layer 2 — operator-confirmed Mode B (target-class byte in source
// position).
//
// Wire signature: [SYN] [TGT=0x26 VR_71 target] [PB=0xB5] [SB=0x23]
// [LEN=0x01] [data] [CRC] [SYN]. The actual SRC byte (likely 0x10
// BASV2 initiator) was eaten upstream. The reconstructor MUST reject
// 0x26 as src (target-class), increment InvalidSrcClassSkippedTotal
// exactly once, suppress the cascading PrefixResyncSkippedTotal
// counter on the PB/SB/data/CRC bytes that follow, and emit NO abandon
// event (the previous behavior would have been a corrupted_request
// abandon).
func TestPassiveTransactionReconstructor_RejectsNonMasterSrc(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Wire body uses non-SYN data bytes (0xAA == SymbolSyn would
	// prematurely re-engage synced=true mid-cascade). The CRC value is
	// irrelevant — Layer 2 rejection happens BEFORE parseFrame.
	wire := []byte{protocol.SymbolSyn, 0x26, 0xB5, 0x23, 0x01, 0x42, 0x99, protocol.SymbolSyn}
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	assertNoPassiveClassifiedEvent(t, subscription, 50*time.Millisecond)

	snapshot := reconstructor.Snapshot()
	if got := snapshot.InvalidSrcClassSkippedTotal; got != 1 {
		t.Fatalf("InvalidSrcClassSkippedTotal = %d; want 1 (Mode B target-as-src)", got)
	}
	if got := snapshot.PrefixResyncSkippedTotal; got != 0 {
		t.Fatalf("PrefixResyncSkippedTotal = %d; want 0 (cascade must be suppressed via awaitingResync)", got)
	}
}

// P6 Layer 2 — broadcast byte (0xFE) in source position.
//
// Same Mode B failure mode, different DST byte category.
func TestPassiveTransactionReconstructor_RejectsBroadcastSrc(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Non-SYN data bytes only (avoid 0xAA mid-cascade).
	wire := []byte{protocol.SymbolSyn, 0xFE, 0xB5, 0x16, 0x01, 0x42, 0x99, protocol.SymbolSyn}
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	assertNoPassiveClassifiedEvent(t, subscription, 50*time.Millisecond)

	snapshot := reconstructor.Snapshot()
	if got := snapshot.InvalidSrcClassSkippedTotal; got != 1 {
		t.Fatalf("InvalidSrcClassSkippedTotal = %d; want 1 (Mode B broadcast-as-src)", got)
	}
	if got := snapshot.PrefixResyncSkippedTotal; got != 0 {
		t.Fatalf("PrefixResyncSkippedTotal = %d; want 0 (cascade suppressed)", got)
	}
}

// P6 Layer 2 — reserved byte (0xA9 ESCAPE) in source position.
//
// Verifies cascade suppression AND that the parser recovers cleanly
// after the trailing SYN: a valid frame fed after the rejection must
// classify normally.
func TestPassiveTransactionReconstructor_RejectsReservedSrc(t *testing.T) {
	t.Parallel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	subscription, err := reconstructor.Subscribe("test", PassiveSubscriberCritical, 8)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	// Cascade body uses only non-SYN bytes (0xAA == SymbolSyn would
	// prematurely re-engage synced=true and let a subsequent byte be
	// taken as a new request source).
	wire := []byte{protocol.SymbolSyn, 0xA9, 0x55, 0x42, 0x00, 0x33, protocol.SymbolSyn}
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	snapshot := reconstructor.Snapshot()
	if got := snapshot.InvalidSrcClassSkippedTotal; got != 1 {
		t.Fatalf("InvalidSrcClassSkippedTotal = %d; want 1 (reserved-as-src)", got)
	}
	if got := snapshot.PrefixResyncSkippedTotal; got != 0 {
		t.Fatalf("PrefixResyncSkippedTotal = %d; want 0 (cascade suppressed via awaitingResync)", got)
	}
	assertNoPassiveClassifiedEvent(t, subscription, 50*time.Millisecond)

	// After the trailing SYN, parser must accept a valid frame.
	valid := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x99},
	}
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0).Add(time.Second), frameBytes(valid))
	requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)
}

// P6 Layer 2 — negative control: initiator-class byte in source position
// is accepted; neither counter increments.
func TestPassiveTransactionReconstructor_AcceptsMasterSrc(t *testing.T) {
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
		Data:      []byte{0x01},
	}
	wire := append([]byte{protocol.SymbolSyn}, frameBytes(frame)...)
	feedPassiveSymbolsRaw(reconstructor, time.Unix(0, 0), wire)

	requirePassiveClassifiedEvent(t, subscription, PassiveClassifiedEventBroadcastFrame)

	snapshot := reconstructor.Snapshot()
	if got := snapshot.InvalidSrcClassSkippedTotal; got != 0 {
		t.Fatalf("InvalidSrcClassSkippedTotal = %d; want 0 (initiator src must pass)", got)
	}
	if got := snapshot.PrefixResyncSkippedTotal; got != 0 {
		t.Fatalf("PrefixResyncSkippedTotal = %d; want 0 (initiator src must pass)", got)
	}
}
