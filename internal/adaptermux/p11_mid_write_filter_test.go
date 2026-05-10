package adaptermux

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// P11 — verify activePathExpectsByte rejects stale third-party tail
// bytes that arrive mid-write (echoCursor < writePrefixLen).
//
// Production motivation: post_grant_ack accounted for ~52% of all
// echo_mismatch events (312/600 in the 2026-05-10 soak window). Live
// pattern: BASV2 (0x10) finishes a 0704 scan probe, the adapter has
// the trailing 0x00 ACK byte buffered in the TCP/ENH pipeline; the
// gateway wins arbitration immediately, writes its source byte (e.g.
// 0x71), the adapter echoes 0x71 back; while bus.Send is mid-write
// awaiting echo of byte 2 (e.g. 0x15 target), the buffered 0x00
// finally pops out of the pipeline. Pre-P11 the gate
// `bytesDeliveredToActive>0 → return true` let it through; bus.Send
// read 0x00 in echo position → echo_mismatch (subclass=post_grant_ack).
//
// P11 fix: the gate uses echoCursor as the mid-write/response phase
// boundary. While echoCursor < writePrefixLen, only writePrefix[
// echoCursor] passes; everything else routes to passive. Once all
// writes echo, the gate opens for response-phase delivery.
func TestP11_MidWriteStaleByteRejected(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Step 1: write source byte 0x71, echo arrives, consume.
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write(0x71) err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	if b, err := at.ReadByte(); err != nil || b != 0x71 {
		t.Fatalf("ReadByte echo of 0x71 = (0x%02X, %v); want (0x71, nil)", b, err)
	}

	// Step 2: write target byte 0x15. expectedEchoes = [0x15],
	// writePrefixLen = 2, echoCursor = 1 (matched 0x71). Mid-write
	// state is now established.
	if _, err := at.Write([]byte{0x15}); err != nil {
		t.Fatalf("Write(0x15) err=%v", err)
	}
	// Yield for sendLoop to record the prefix.
	time.Sleep(20 * time.Millisecond)

	// Step 3: simulate stale 0x00 from BASV2's prior frame tail
	// arriving BEFORE the real echo of 0x15. Pre-P11 this would have
	// reached activeCh and bus.Send.ReadByte would return 0x00.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x00}

	// Step 4: now feed the legitimate echo of 0x15.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x15}

	// Step 5: bus.Send.ReadByte should observe 0x15 (the real echo),
	// NOT 0x00 (the stale byte). P11 filter rejects 0x00 because
	// writePrefix[echoCursor] == 0x15 != 0x00.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		b, err := at.ReadByte()
		if err == nil {
			if b == 0x00 {
				t.Fatalf("ReadByte returned stale 0x00 — P11 filter regressed (post_grant_ack would fire)")
			}
			if b == 0x15 {
				return // success
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not observe legitimate 0x15 echo within 1s")
}

// P11 — once all writes have been echoed (echoCursor == writePrefixLen),
// the response-phase gate opens. ANY byte (even 0x00) reaches
// bus.Send for downstream interpretation. Guards against an
// over-aggressive filter regression that would block legitimate
// target ACK bytes.
func TestP11_ResponsePhaseAcceptsAnyByte(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Write + echo + consume so we leave the mid-write phase
	// (echoCursor == writePrefixLen).
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte echo err=%v", err)
	}

	// Now in response phase. Inject 0x00 (target ACK) — must be
	// delivered.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x00}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		b, err := at.ReadByte()
		if err == nil && b == 0x00 {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("response-phase 0x00 was filtered — P11 over-aggressive (target ACK delivery broken)")
}
