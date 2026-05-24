package adaptermux

import (
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// TestE2E_ScanB504_ToTarget0x08_SuccessFullFlow drives a synthetic green-
// path scan transaction through the real mux plumbing:
//
//  1. Gateway grants arbitration with source 0x71.
//  2. The active-path caller writes a 0x07/0x04 identification request
//     targeted at 0x08 (QQ=0x71 ZZ=0x08 PB=0x07 SB=0x04 NN=0x00 CRC).
//  3. The fake upstream echoes every byte written (simulating the bus
//     loopback of the gateway's own transmission).
//  4. The fake upstream delivers a valid responder ACK + identification
//     response frame + final SYN 0xAA.
//  5. The test reads bytes back through mux.ActiveTransport().ReadByte()
//     (the same path protocol.Bus uses) until it has consumed the whole
//     response including the trailing SYN, or until timeout.
//
// Assertion: the echoed request bytes AND the full response AND the
// terminating SYN must all be delivered through activeCh to the Send
// consumer. A missed final SYN indicates the "onSYNLocked consumed the
// terminator before activePathExpectsBytes() was reached" hypothesis.
//
// If this test PASSES, the mux delivers the terminator — the runtime
// ok=0 timeouts=46 soak failure is elsewhere (framing/parse/upstream).
// If this test FAILS (final SYN not delivered within the overall
// timeout), the hypothesis is confirmed: the SYN-before-read guard
// clears gatewayTxnActive the moment bytesRead>0 AND the trailing SYN
// arrives, and activePathExpectsBytes() returns false at deliverToActive
// time.
func TestE2E_ScanB504_ToTarget0x08_SuccessFullFlow(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// Grant gateway ownership (initiator = 0x71).
	grantGateway(t, mux, mock, 0x71)

	at := mux.ActiveTransport()

	// --- Request bytes the gateway would write ---
	// eBUS request layout: QQ ZZ PB SB NN [data] CRC
	// No data -> NN=0x00, CRC computed over QQ ZZ PB SB NN (ignored by
	// this test — we only care about byte plumbing, not CRC validity).
	request := []byte{0x71, 0x08, 0x07, 0x04, 0x00, 0x00 /*CRC placeholder*/}

	// Write request bytes (simulates what protocol.Bus does during
	// sendRawWithEcho).
	if _, err := at.Write(request); err != nil {
		t.Fatalf("at.Write err=%v", err)
	}

	// Async upstream fake: echo every write, then send a canned responder
	// response. Start after Write so the synthetic bus traffic preserves
	// request-before-echo ordering.
	//
	// Round-6 (batch-24): the final SYN terminator is no longer injected
	// as a bare wire SYN. The real bus.Bus.Send path always ends with
	// sendEndOfMessage → sendRawWithEcho(SymbolSyn), and round-6's
	// terminator gate now requires that explicit write (so a wire SYN
	// without a matching recordSent is correctly classified as a wire
	// intrusion). The test models this by:
	//   - response goroutine echoes the request bytes + response payload
	//     + listens on a channel for the "issue terminator now" signal
	//   - main goroutine consumes all non-SYN bytes, THEN signals the
	//     response goroutine to inject the terminator wire echo, THEN
	//     issues at.Write(SymbolSyn) via gatewayEndOfMessage
	terminatorReady := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Echo each request byte back.
		for _, b := range request {
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
		}

		// Responder ACK=0x00, then response: NN=0x05, 5 data bytes, CRC=0x00,
		// then initiator ACK=0x00.
		response := []byte{
			0x00,                         // responder ACK
			0x05,                         // NN (response data length)
			0x56, 0x41, 0x49, 0x4C, 0x4C, // "VAILL" identification data
			0x00, // response CRC placeholder
			0x00, // initiator ACK
		}
		for _, b := range response {
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
		}

		// Wait for main goroutine to consume the response and signal
		// readiness for the terminator. Then echo SymbolSyn so the
		// gateway echo queue (armed by gatewayEndOfMessage) matches.
		<-terminatorReady
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	}()

	// Read back every byte we expect to appear on activeCh in order:
	//   echoed request (len(request) bytes)
	//   + response (9 bytes: responder ACK, NN, 5 data, CRC, initiator ACK)
	// Then issue gatewayEndOfMessage (writes terminator SYN, arms recordSent),
	// signal the response goroutine to inject the SYN echo, then read the
	// terminator (16th byte).
	preTerminatorExpected := len(request) + 9
	totalExpected := preTerminatorExpected + 1
	var gotMu sync.Mutex
	got := make([]byte, 0, totalExpected)
	snapshotGot := func() []byte {
		gotMu.Lock()
		defer gotMu.Unlock()
		return append([]byte(nil), got...)
	}
	deadline := time.Now().Add(2 * time.Second)

	// Phase 1: read everything up to (but not including) the terminator.
	preReadDone := make(chan error, 1)
	go func() {
		for len(got) < preTerminatorExpected {
			b, err := at.ReadByte()
			if err != nil {
				preReadDone <- err
				return
			}
			gotMu.Lock()
			got = append(got, b)
			gotMu.Unlock()
		}
		preReadDone <- nil
	}()

	select {
	case err := <-preReadDone:
		if err != nil {
			gotSnapshot := snapshotGot()
			t.Logf("Phase-1 ReadByte error after %d bytes consumed: %v", len(gotSnapshot), err)
			t.Logf("bytes consumed so far: % X", gotSnapshot)
			dumpSynDiag(t, mux)
			t.Fatalf("Phase-1 read error after %d bytes: %v", len(gotSnapshot), err)
		}
	case <-time.After(time.Until(deadline)):
		gotSnapshot := snapshotGot()
		t.Logf("Phase-1 timeout after %d/%d bytes consumed", len(gotSnapshot), preTerminatorExpected)
		t.Logf("bytes consumed: % X", gotSnapshot)
		dumpSynDiag(t, mux)
		t.Fatalf("Phase-1 timeout: %d/%d bytes consumed", len(gotSnapshot), preTerminatorExpected)
	}

	// Phase 2: emulate sendEndOfMessage. Issue the terminator write via
	// the active path so recordSent populates gatewayEcho with [SymbolSyn],
	// then signal the response goroutine to inject the wire echo.
	// F-NEW-28 (2026-05-21): use writeTerminatorSyn helper so the
	// structural-SYN provenance flag flows through the gateway echo
	// tracker, matching bus.go's sendEndOfMessage path.
	go writeTerminatorSyn(at)
	time.Sleep(20 * time.Millisecond) // let recordSent run
	terminatorReady <- struct{}{}

	// Phase 3: read the terminator byte from activeCh.
	readDone := make(chan error, 1)
	go func() {
		b, err := at.ReadByte()
		if err != nil {
			readDone <- err
			return
		}
		gotMu.Lock()
		got = append(got, b)
		gotMu.Unlock()
		readDone <- nil
	}()

	select {
	case err := <-readDone:
		gotSnapshot := snapshotGot()
		if err != nil {
			t.Logf("Phase-3 ReadByte error after %d bytes consumed: %v", len(gotSnapshot), err)
			t.Logf("bytes consumed so far: % X", gotSnapshot)
			dumpSynDiag(t, mux)
			wg.Wait()
			t.Fatalf("Phase-3 read error: %v — terminator SYN echo may have been "+
				"suppressed by round-6 P10.2 gate despite explicit sendEndOfMessage. "+
				"See SYN ring dump above.", err)
		}
	case <-time.After(time.Until(deadline)):
		gotSnapshot := snapshotGot()
		t.Logf("Phase-3 timeout after %d/%d bytes consumed", len(gotSnapshot), totalExpected)
		t.Logf("bytes consumed: % X", gotSnapshot)
		dumpSynDiag(t, mux)
		t.Fatalf("Phase-3 timeout: terminator SYN never delivered to activeCh after explicit " +
			"sendEndOfMessage. Round-6 regression — the hasPendingEcho+SymbolSyn branch of " +
			"the terminator gate failed to fire.")
	}

	wg.Wait()

	// If we reach here the full sequence was consumed. Verify the tail
	// is the trailing SYN.
	if len(got) != totalExpected {
		t.Fatalf("len(got)=%d, want %d", len(got), totalExpected)
	}
	if got[len(got)-1] != protocol.SymbolSyn {
		t.Fatalf("last byte=0x%02X, want SYN 0xAA — terminator missed", got[len(got)-1])
	}

	// Cross-check: SYN diag ring recorded the terminator and the mux
	// cleared gatewayTxnActive at some point during the flow.
	dumpSynDiag(t, mux)
}

// dumpSynDiag logs the mux's SYN diag ring in chronological order. Used
// by the E2E test to surface evidence when a failure/skip fires.
func dumpSynDiag(t *testing.T, mux *Mux) {
	t.Helper()
	entries := mux.SynDiagSnapshot()
	t.Logf("SynDiagSnapshot: %d entries", len(entries))
	for i, e := range entries {
		t.Logf("  [%d] txnID=%d owner=%d gwBefore=%v gwAfter=%v lastWritten=0x%02X(has=%v) bytesRead=%d synDelivered=%v reason=%q",
			i, e.TxnID, e.OwnerID, e.GwActiveBefore, e.GwActiveAfter,
			e.LastWrittenByte, e.HasLastWrittenByte, e.BytesRead,
			e.SynDeliveredToActive, e.InactiveReason)
	}
}
