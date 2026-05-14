package adaptermux

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func TestWirePhase_IdleOnSYN(t *testing.T) {
	var tracker wirePhaseTracker
	tracker.reset(wirePhaseIdle)

	event := tracker.advance(protocol.SymbolSyn)
	if event != wirePhaseEventSYNIdle {
		t.Fatalf("expected SYNIdle, got %d", event)
	}
	if !tracker.isIdle() {
		t.Fatal("expected idle after SYN")
	}
}

func TestWirePhase_FullTransaction(t *testing.T) {
	// Simulate a complete initiator-target transaction:
	// SRC=0x71 DST=0x08 PB=0xB5 SB=0x24 LEN=0x02 DATA[0]=0x00 DATA[1]=0x01 CRC=0xXX
	// → ACK → LEN=0x01 DATA[0]=0x42 CRC=0xXX → ACK
	var tracker wirePhaseTracker
	tracker.startRequest()

	// Request bytes: SRC, DST, PB, SB, LEN, DATA[0], DATA[1], CRC
	events := []struct {
		symbol byte
		want   wirePhaseEvent
	}{
		{0x71, wirePhaseEventNone},            // SRC
		{0x08, wirePhaseEventNone},            // DST
		{0xB5, wirePhaseEventNone},            // PB
		{0x24, wirePhaseEventNone},            // SB
		{0x02, wirePhaseEventNone},            // LEN=2
		{0x00, wirePhaseEventNone},            // DATA[0]
		{0x01, wirePhaseEventNone},            // DATA[1]
		{0x99, wirePhaseEventRequestComplete}, // CRC → request complete
	}

	for i, tc := range events {
		got := tracker.advance(tc.symbol)
		if got != tc.want {
			t.Fatalf("request byte %d (0x%02x): got event %d, want %d", i, tc.symbol, got, tc.want)
		}
	}

	if tracker.initiator() != 0x71 {
		t.Fatalf("initiator = 0x%02x, want 0x71", tracker.initiator())
	}
	if tracker.target() != 0x08 {
		t.Fatalf("target = 0x%02x, want 0x08", tracker.target())
	}

	// ACK from target.
	got := tracker.advance(protocol.SymbolAck)
	if got != wirePhaseEventCmdACK {
		t.Fatalf("ACK: got event %d, want CmdACK", got)
	}

	// Response: LEN=1, DATA[0], CRC.
	got = tracker.advance(0x01) // LEN=1
	if got != wirePhaseEventNone {
		t.Fatalf("response LEN: got event %d, want None", got)
	}

	got = tracker.advance(0x42) // DATA[0]
	if got != wirePhaseEventNone {
		t.Fatalf("response DATA: got event %d, want None", got)
	}

	got = tracker.advance(0xBB) // CRC → response done
	if got != wirePhaseEventResponseDone {
		t.Fatalf("response CRC: got event %d, want ResponseDone", got)
	}

	// F-21 (batch-20): final ACK now defers to trailing SYN. Final
	// ACK byte returns None and transitions to WaitTerminalSyn.
	got = tracker.advance(protocol.SymbolAck)
	if got != wirePhaseEventNone {
		t.Fatalf("final ACK: got event %d, want None (F-21 deferred terminal)", got)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after final ACK = %d, want WaitTerminalSyn (%d) (F-21)", tracker.phase, wirePhaseWaitTerminalSyn)
	}

	// F-21: trailing wire SYN fires TransactionDone and resets to Idle.
	got = tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("trailing SYN: got event %d, want TransactionDone (F-21)", got)
	}
	if !tracker.isIdle() {
		t.Fatal("expected idle after trailing SYN (F-21)")
	}
}

func TestWirePhase_BroadcastTransaction(t *testing.T) {
	// Broadcast: DST=0xFE → ACK completes transaction (no response).
	var tracker wirePhaseTracker
	tracker.startRequest()

	// SRC=0x71, DST=0xFE, PB=0x07, SB=0x04, LEN=0x00, CRC
	for _, b := range []byte{0x71, 0xFE, 0x07, 0x04, 0x00} {
		tracker.advance(b)
	}
	got := tracker.advance(0xCC) // CRC → request complete
	if got != wirePhaseEventRequestComplete {
		t.Fatalf("CRC: got event %d, want RequestComplete", got)
	}

	// F-21 (batch-20): broadcast ACK now defers to trailing SYN.
	got = tracker.advance(protocol.SymbolAck)
	if got != wirePhaseEventNone {
		t.Fatalf("broadcast ACK: got event %d, want None (F-21 deferred terminal)", got)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after broadcast ACK = %d, want WaitTerminalSyn (F-21)", tracker.phase)
	}
	got = tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("broadcast trailing SYN: got event %d, want TransactionDone (F-21)", got)
	}
	if !tracker.isIdle() {
		t.Fatal("expected idle after broadcast trailing SYN (F-21)")
	}
}

func TestWirePhase_NACK_FirstRetries(t *testing.T) {
	// AM2: first CMD NACK retries without re-arbitration.
	var tracker wirePhaseTracker
	tracker.startRequest()

	// Minimal request: SRC, DST, PB, SB, LEN=0, CRC
	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x00} {
		tracker.advance(b)
	}
	got := tracker.advance(0xDD) // CRC
	if got != wirePhaseEventRequestComplete {
		t.Fatalf("CRC: got event %d, want RequestComplete", got)
	}

	// First NACK -> retry (not idle).
	got = tracker.advance(protocol.SymbolNack)
	if got != wirePhaseEventCmdNACKRetry {
		t.Fatalf("first NACK: got event %d, want CmdNACKRetry", got)
	}
	if tracker.phase != wirePhaseCollectRequest {
		t.Fatalf("phase after first NACK = %d, want CollectRequest", tracker.phase)
	}
	if tracker.isIdle() {
		t.Fatal("should not be idle after first NACK (retry expected)")
	}
}

func TestWirePhase_NACK_SecondGoesToWaitTerminalSyn(t *testing.T) {
	// AM2 + F-21 (batch-20): second CMD NACK now defers to trailing
	// SYN. The byte returns None and transitions to WaitTerminalSyn;
	// the trailing SYN fires TransactionDone. F-21 deviation from the
	// pre-revert build (which returned CmdNACK at this byte position
	// and reset to Idle): the non-SYN release block at mux.go
	// previously released ownership on CmdNACK, which rejected
	// ebusd's trailing-SYN ENH_REQ_SEND. CmdNACK is now folded into
	// TransactionDone at the SYN observation; the diagnostic
	// distinction (ReasonCmdNACK) is irrelevant for external paths.
	var tracker wirePhaseTracker
	tracker.startRequest()

	// First request + first NACK -> retry.
	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x00} {
		tracker.advance(b)
	}
	tracker.advance(0xDD)                // CRC
	tracker.advance(protocol.SymbolNack) // first NACK -> retry

	// Retransmitted request: SRC, DST, PB, SB, LEN=0, CRC
	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x00} {
		tracker.advance(b)
	}
	got := tracker.advance(0xDD) // CRC -> request complete again
	if got != wirePhaseEventRequestComplete {
		t.Fatalf("retry CRC: got event %d, want RequestComplete", got)
	}

	// F-21: second NACK -> None + WaitTerminalSyn (was CmdNACK + Idle).
	got = tracker.advance(protocol.SymbolNack)
	if got != wirePhaseEventNone {
		t.Fatalf("second NACK: got event %d, want None (F-21 deferred terminal)", got)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after second NACK = %d, want WaitTerminalSyn (F-21)", tracker.phase)
	}
	// Trailing SYN -> TransactionDone + Idle.
	got = tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("trailing SYN: got event %d, want TransactionDone (F-21)", got)
	}
	if !tracker.isIdle() {
		t.Fatal("expected idle after trailing SYN (F-21)")
	}
}

func TestWirePhase_NACK_RetryThenACK(t *testing.T) {
	// AM2: first NACK retries, retransmit succeeds with ACK.
	var tracker wirePhaseTracker
	tracker.startRequest()

	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x00} {
		tracker.advance(b)
	}
	tracker.advance(0xDD)                // CRC
	tracker.advance(protocol.SymbolNack) // first NACK -> retry

	// Retransmitted request.
	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x00} {
		tracker.advance(b)
	}
	tracker.advance(0xDD) // CRC

	// ACK on retry -> proceed to response.
	got := tracker.advance(protocol.SymbolAck)
	if got != wirePhaseEventCmdACK {
		t.Fatalf("ACK after retry: got event %d, want CmdACK", got)
	}
}

func TestWirePhase_GarbledByteInWaitCmdAck(t *testing.T) {
	// AM21: non-ACK, non-NACK byte is protocol error -> idle, no retry.
	var tracker wirePhaseTracker
	tracker.startRequest()

	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x00} {
		tracker.advance(b)
	}
	tracker.advance(0xDD) // CRC

	// Garbled byte (not ACK=0x00, not NACK=0xFF, not SYN=0xAA).
	got := tracker.advance(0x42)
	if got != wirePhaseEventCmdNACK {
		t.Fatalf("garbled byte: got event %d, want CmdNACK", got)
	}
	if !tracker.isIdle() {
		t.Fatal("expected idle after garbled byte (no retry)")
	}
	// Verify cmdRetried was NOT set (garbled bytes skip retry).
	if tracker.cmdRetried {
		t.Fatal("cmdRetried should not be set on garbled byte")
	}
}

func TestWirePhase_SYNDuringWaitCmdAck(t *testing.T) {
	var tracker wirePhaseTracker
	tracker.startRequest()

	// Send request bytes to reach WaitCmdAck
	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x00} {
		tracker.advance(b)
	}
	tracker.advance(0xDD) // CRC → WaitCmdAck

	// SYN during WaitCmdAck → timeout
	got := tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventSYNTimeout {
		t.Fatalf("SYN during WaitCmdAck: got event %d, want SYNTimeout", got)
	}
	if !tracker.isIdle() {
		t.Fatal("expected idle after SYN timeout")
	}
}

func TestWirePhase_SYNDuringCollectRequest(t *testing.T) {
	var tracker wirePhaseTracker
	tracker.startRequest()

	// Send only 2 request bytes, then SYN.
	tracker.advance(0x71) // SRC
	tracker.advance(0x08) // DST

	got := tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventSYNTimeout {
		t.Fatalf("SYN during CollectRequest: got event %d, want SYNTimeout", got)
	}
}

func TestWirePhase_SYNDuringFreshGrant_IsIdle(t *testing.T) {
	// After startRequestWithSource (ArbitrationSendsSource=true),
	// requestBytesSeen=1 (only pre-loaded SRC). SYN at this point is
	// normal inter-transaction bus idle, NOT a timeout.
	var tracker wirePhaseTracker
	tracker.startRequestWithSource(0x71)

	got := tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventSYNIdle {
		t.Fatalf("SYN during fresh grant: got event %d, want SYNIdle (%d)", got, wirePhaseEventSYNIdle)
	}
}

func TestWirePhase_SYNDuringActiveCollect_IsTimeout(t *testing.T) {
	// After sending real bytes (requestBytesSeen > 1), SYN during
	// CollectRequest IS a timeout — the request was interrupted.
	var tracker wirePhaseTracker
	tracker.startRequestWithSource(0x71)

	tracker.advance(0x15) // DST — requestBytesSeen=2 now

	got := tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventSYNTimeout {
		t.Fatalf("SYN during active collect: got event %d, want SYNTimeout (%d)", got, wirePhaseEventSYNTimeout)
	}
}

func TestWirePhase_EscapeByteInPayload(t *testing.T) {
	// Verify that 0xA9 (SymbolEscape) in payload data is handled
	// correctly as a regular data byte by the phase tracker.
	// The tracker operates on post-ENH-decode logical bytes, so 0xA9
	// is just a regular data value — no escape handling involved.
	var tracker wirePhaseTracker
	tracker.startRequest()

	// SRC=0x71, DST=0x08, PB=0xB5, SB=0x24, LEN=2
	// DATA[0]=0xA9 (escape byte as data), DATA[1]=0x42, CRC=0xCC
	// Total: 5 header + 2 data + 1 CRC = 8 bytes
	request := []byte{0x71, 0x08, 0xB5, 0x24, 0x02, protocol.SymbolEscape, 0x42}
	for i, b := range request {
		got := tracker.advance(b)
		if got != wirePhaseEventNone {
			t.Fatalf("byte %d (0x%02x): got event %d, want None", i+1, b, got)
		}
	}

	// 8th byte is CRC → request complete.
	got := tracker.advance(0xCC)
	if got != wirePhaseEventRequestComplete {
		t.Fatalf("CRC: got event %d, want RequestComplete", got)
	}
}

func TestWirePhase_StartRequestWithSource_B524(t *testing.T) {
	// Simulate a B524 request to BASV2 (0x15) where ArbitrationSendsSource
	// is true — SRC (0x71) was already sent during arbitration and is NOT
	// echoed as a data byte. The tracker must pre-load SRC so byte
	// counting matches the actual on-wire frame.
	//
	// On-wire frame: SRC(0x71) DST(0x15) PB(0xB5) SB(0x24) LEN(0x06) D0-D5 CRC
	// Bytes seen by tracker (no SRC echo): DST PB SB LEN D0 D1 D2 D3 D4 D5 CRC
	var tracker wirePhaseTracker
	tracker.startRequestWithSource(0x71)

	if tracker.requestBytesSeen != 1 {
		t.Fatalf("requestBytesSeen after startRequestWithSource = %d; want 1", tracker.requestBytesSeen)
	}
	if tracker.requestSrc != 0x71 {
		t.Fatalf("requestSrc = 0x%02X; want 0x71", tracker.requestSrc)
	}

	// Feed the 11 bytes the gateway sends (SRC excluded).
	request := []byte{
		0x15,       // DST = BASV2
		0xB5,       // PB  = vaillant manufacturer
		0x24,       // SB  = extended register access
		0x06,       // LEN = 6 data bytes
		0x02, 0x00, // D0-D1: opcode=local, read
		0x00, 0x00, // D2-D3: group=0, instance=0
		0x01, 0x00, // D4-D5: addr=0x0001
		0x42, // CRC (placeholder)
	}

	var lastEvent wirePhaseEvent
	for i, b := range request {
		lastEvent = tracker.advance(b)
		if i < len(request)-1 && lastEvent == wirePhaseEventRequestComplete {
			t.Fatalf("premature RequestComplete at byte %d (0x%02X)", i+1, b)
		}
	}

	if lastEvent != wirePhaseEventRequestComplete {
		t.Fatalf("after 11 bytes: event = %d; want wirePhaseEventRequestComplete (%d)", lastEvent, wirePhaseEventRequestComplete)
	}
	if tracker.phase != wirePhaseWaitCmdAck {
		t.Fatalf("phase = %d; want wirePhaseWaitCmdAck (%d)", tracker.phase, wirePhaseWaitCmdAck)
	}
	if tracker.requestDst != 0x15 {
		t.Fatalf("requestDst = 0x%02X; want 0x15", tracker.requestDst)
	}

	// ACK from target.
	event := tracker.advance(protocol.SymbolAck)
	if event != wirePhaseEventCmdACK {
		t.Fatalf("ACK event = %d; want wirePhaseEventCmdACK (%d)", event, wirePhaseEventCmdACK)
	}
}

func TestWirePhase_StartRequestWithSource_vs_StartRequest(t *testing.T) {
	// Without pre-loaded SRC, the tracker miscounts: "LEN" is captured
	// from a data byte, causing premature WaitCmdAck.
	var bad wirePhaseTracker
	bad.startRequest() // no SRC pre-loaded

	request := []byte{
		0x15, 0xB5, 0x24, 0x06,
		0x02, 0x00, 0x00, 0x00, 0x01, 0x00,
		0x42,
	}

	premature := false
	for i, b := range request {
		ev := bad.advance(b)
		if ev == wirePhaseEventRequestComplete && i < len(request)-1 {
			premature = true
			break
		}
	}
	if !premature {
		t.Fatal("expected premature RequestComplete with startRequest() (no SRC), but got correct counting")
	}

	// With pre-loaded SRC, counting is correct.
	var good wirePhaseTracker
	good.startRequestWithSource(0x71)

	premature = false
	for i, b := range request {
		ev := good.advance(b)
		if ev == wirePhaseEventRequestComplete && i < len(request)-1 {
			premature = true
			break
		}
	}
	if premature {
		t.Fatal("startRequestWithSource still produced premature RequestComplete")
	}
}

func TestWirePhase_ZeroLengthResponse(t *testing.T) {
	var tracker wirePhaseTracker
	tracker.startRequest()

	// SRC, DST, PB, SB, LEN=0, CRC → ACK → ResponseLEN=0, CRC → ACK
	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x00} {
		tracker.advance(b)
	}
	tracker.advance(0xDD)               // CRC → WaitCmdAck
	tracker.advance(protocol.SymbolAck) // ACK → WaitResponseLen

	// Response LEN=0: only CRC follows.
	got := tracker.advance(0x00) // LEN=0
	if got != wirePhaseEventNone {
		t.Fatalf("response LEN=0: got event %d, want None", got)
	}

	// With LEN=0, we need 0 data bytes + 1 CRC = 1 byte remaining.
	got = tracker.advance(0xEE) // CRC → ResponseDone
	if got != wirePhaseEventResponseDone {
		t.Fatalf("response CRC: got event %d, want ResponseDone", got)
	}

	// F-21 (batch-20): final ACK defers to trailing SYN.
	got = tracker.advance(protocol.SymbolAck)
	if got != wirePhaseEventNone {
		t.Fatalf("final ACK: got event %d, want None (F-21 deferred terminal)", got)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after final ACK = %d, want WaitTerminalSyn (F-21)", tracker.phase)
	}
	got = tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("trailing SYN: got event %d, want TransactionDone (F-21)", got)
	}
}

func TestWirePhase_NACKResponseRetry(t *testing.T) {
	// Simulate initiator-target transaction where initiator NACKs the
	// response CRC and target retries. The wire phase must track the
	// retry response instead of resetting to idle.
	var tracker wirePhaseTracker
	tracker.startRequest()

	// Send request: SRC DST PB SB LEN=1 DATA[0] CRC
	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x01, 0x42} {
		tracker.advance(b)
	}
	tracker.advance(0xCC) // CRC -> RequestComplete -> WaitCmdAck

	// Target ACK.
	tracker.advance(protocol.SymbolAck) // -> WaitResponseLen

	// First response: LEN=1 DATA CRC.
	tracker.advance(0x01)        // LEN=1
	tracker.advance(0xAB)        // DATA[0]
	got := tracker.advance(0xDD) // CRC -> ResponseDone -> WaitResponseAck
	if got != wirePhaseEventResponseDone {
		t.Fatalf("first response CRC: event=%d, want ResponseDone", got)
	}

	// Initiator NACKs -- CRC was bad. Must transition to WaitResponseLen.
	got = tracker.advance(protocol.SymbolNack)
	if got != wirePhaseEventNone {
		t.Fatalf("NACK: event=%d, want None (retry expected)", got)
	}
	if tracker.phase != wirePhaseWaitResponseLen {
		t.Fatalf("phase after NACK=%d, want WaitResponseLen (%d)", tracker.phase, wirePhaseWaitResponseLen)
	}

	// Retry response: LEN=1 DATA CRC.
	tracker.advance(0x01)       // LEN=1
	tracker.advance(0xAB)       // DATA[0]
	got = tracker.advance(0xEE) // CRC -> ResponseDone -> WaitResponseAck
	if got != wirePhaseEventResponseDone {
		t.Fatalf("retry response CRC: event=%d, want ResponseDone", got)
	}

	// F-21 (batch-20): retry ACK defers to trailing SYN.
	got = tracker.advance(protocol.SymbolAck)
	if got != wirePhaseEventNone {
		t.Fatalf("retry ACK: event=%d, want None (F-21 deferred terminal)", got)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after retry ACK = %d, want WaitTerminalSyn (F-21)", tracker.phase)
	}
	got = tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("trailing SYN: event=%d, want TransactionDone (F-21)", got)
	}
	if !tracker.isIdle() {
		t.Fatal("expected idle after trailing SYN (F-21)")
	}
}

func TestWirePhase_ResponseDoubleNACK(t *testing.T) {
	// AM3: second response NACK ends the transaction.
	var tracker wirePhaseTracker
	tracker.startRequest()

	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x01, 0x42} {
		tracker.advance(b)
	}
	tracker.advance(0xCC)               // CRC
	tracker.advance(protocol.SymbolAck) // -> WaitResponseLen

	// First response + NACK (retry).
	tracker.advance(0x01)                // LEN
	tracker.advance(0xAB)                // DATA
	tracker.advance(0xDD)                // CRC -> ResponseDone
	tracker.advance(protocol.SymbolNack) // first NACK -> retry

	// Retry response + second NACK.
	tracker.advance(0x01)                       // LEN
	tracker.advance(0xAB)                       // DATA
	tracker.advance(0xEE)                       // CRC -> ResponseDone
	// F-21 (batch-20): second response NACK defers to trailing SYN.
	got := tracker.advance(protocol.SymbolNack)
	if got != wirePhaseEventNone {
		t.Fatalf("second response NACK: event=%d, want None (F-21 deferred terminal)", got)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after second response NACK = %d, want WaitTerminalSyn (F-21)", tracker.phase)
	}
	got = tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("trailing SYN: event=%d, want TransactionDone (F-21)", got)
	}
	if !tracker.isIdle() {
		t.Fatal("expected idle after trailing SYN (F-21)")
	}
}

// TestWirePhase_InitiatorToInitiator verifies that ACK for an i2i frame
// (DST is an initiator-capable address) transitions directly to
// TransactionDone without waiting for a response phase (AM1 fix).
func TestWirePhase_InitiatorToInitiator(t *testing.T) {
	var tracker wirePhaseTracker
	tracker.startRequest()

	// Simulate i2i request: SRC=0x71, DST=0x10 (both initiator-capable).
	tracker.advance(0x71)       // SRC
	tracker.advance(0x10)       // DST (initiator-capable)
	tracker.advance(0x05)       // PB
	tracker.advance(0x07)       // SB
	tracker.advance(0x01)       // LEN=1
	tracker.advance(0x42)       // DATA[0]
	ev := tracker.advance(0xCC) // CRC -> RequestComplete
	if ev != wirePhaseEventRequestComplete {
		t.Fatalf("expected RequestComplete, got %d", ev)
	}

	// F-21 (batch-20): i2i ACK defers to trailing SYN. Pre-F-21 the
	// AM1 fast-track returned TransactionDone immediately; post-F-21
	// it transitions to WaitTerminalSyn and waits for the SYN.
	ev = tracker.advance(protocol.SymbolAck)
	if ev != wirePhaseEventNone {
		t.Fatalf("i2i ACK: expected None (F-21 deferred terminal), got %d", ev)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after i2i ACK = %d, want WaitTerminalSyn (F-21)", tracker.phase)
	}
	ev = tracker.advance(protocol.SymbolSyn)
	if ev != wirePhaseEventTransactionDone {
		t.Fatalf("i2i trailing SYN: expected TransactionDone, got %d", ev)
	}
	if !tracker.isIdle() {
		t.Fatal("expected idle after i2i trailing SYN (F-21)")
	}
}

// TestWirePhase_InitiatorToTarget verifies that ACK for a normal frame
// (DST is NOT initiator-capable) transitions to WaitResponseLen.
func TestWirePhase_InitiatorToTarget(t *testing.T) {
	var tracker wirePhaseTracker
	tracker.startRequest()

	// SRC=0x71, DST=0x08 (target, not initiator-capable).
	tracker.advance(0x71) // SRC
	tracker.advance(0x08) // DST (NOT initiator-capable)
	tracker.advance(0x05) // PB
	tracker.advance(0x07) // SB
	tracker.advance(0x01) // LEN=1
	tracker.advance(0x42) // DATA[0]
	tracker.advance(0xCC) // CRC -> RequestComplete

	// ACK for normal target: should transition to WaitResponseLen.
	ev := tracker.advance(protocol.SymbolAck)
	if ev != wirePhaseEventCmdACK {
		t.Fatalf("normal ACK: expected CmdACK, got %d", ev)
	}
	if tracker.isIdle() {
		t.Fatal("should NOT be idle — expecting response")
	}
}
