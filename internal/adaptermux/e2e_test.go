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
	// response + SYN terminator. Start after Write so the synthetic bus traffic
	// preserves request-before-echo ordering.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Echo each request byte back.
		for _, b := range request {
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
		}

		// Responder ACK=0x00, then response: NN=0x05, 5 data bytes, CRC=0x00,
		// then initiator ACK=0x00, then trailing SYN=0xAA.
		response := []byte{
			0x00,                         // responder ACK
			0x05,                         // NN (response data length)
			0x56, 0x41, 0x49, 0x4C, 0x4C, // "VAILL" identification data
			0x00,               // response CRC placeholder
			0x00,               // initiator ACK
			protocol.SymbolSyn, // trailing SYN terminator
		}
		for _, b := range response {
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
		}
	}()

	// Read back every byte we expect to appear on activeCh in order:
	//   echoed request (len(request) bytes)
	//   + response (10 bytes including final SYN)
	// The final SYN is the critical one — if it never arrives within
	// the 2s budget, the hypothesis is confirmed.
	totalExpected := len(request) + 10
	var gotMu sync.Mutex
	got := make([]byte, 0, totalExpected)
	snapshotGot := func() []byte {
		gotMu.Lock()
		defer gotMu.Unlock()
		return append([]byte(nil), got...)
	}
	deadline := time.Now().Add(2 * time.Second)

	readDone := make(chan error, 1)
	go func() {
		for len(got) < totalExpected {
			b, err := at.ReadByte()
			if err != nil {
				readDone <- err
				return
			}
			gotMu.Lock()
			got = append(got, b)
			gotMu.Unlock()
		}
		readDone <- nil
	}()

	select {
	case err := <-readDone:
		gotSnapshot := snapshotGot()
		if err != nil {
			t.Logf("ReadByte error after %d bytes consumed: %v", len(gotSnapshot), err)
			t.Logf("bytes consumed so far: % X", gotSnapshot)
			dumpSynDiag(t, mux)
			wg.Wait()
			t.Fatalf("E2E read error after %d bytes: %v — final SYN echo may have been "+
				"consumed by SYN-before-read branch (bytesRead>0 path) before "+
				"activePathExpectsBytes() was re-evaluated. See SYN ring dump above. "+
				"C4 (PR #502): this path used to t.Skip; any deviation from the green "+
				"path is a regression and must fail CI.", len(gotSnapshot), err)
		}
	case <-time.After(time.Until(deadline)):
		gotSnapshot := snapshotGot()
		t.Logf("E2E timeout after %d/%d bytes consumed", len(gotSnapshot), totalExpected)
		t.Logf("bytes consumed: % X", gotSnapshot)
		dumpSynDiag(t, mux)
		// Do NOT wg.Wait() here — the reader goroutine is still blocked
		// on ReadByte; cleanup cancels ctx which releases it after the
		// test returns. C4 (PR #502): this path used to t.Skip; a
		// regression in active-path terminator delivery must fail CI.
		t.Fatalf("E2E timeout after %d/%d bytes — final SYN echo may be consumed "+
			"by onSYNLocked before active-path delivery. See SYN ring dump above "+
			"for gwActiveBefore/After transitions and synDeliveredToActive flags.",
			len(gotSnapshot), totalExpected)
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
