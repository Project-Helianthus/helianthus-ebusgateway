package adaptermux

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

func TestMux_ArbitrationRevalidate_ForwardsFirstForeignMasterByte(t *testing.T) {
	const (
		expectedEcho  byte = 0x15
		foreignMaster byte = 0xF1
	)
	if got := protocol.AddressClassOf(foreignMaster); got != protocol.AddressClassMaster {
		t.Fatalf("test premise broken: AddressClassOf(0x%02X) = %v; want initiator class", foreignMaster, got)
	}

	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	if _, err := at.Write([]byte{expectedEcho}); err != nil {
		t.Fatalf("Write(0x%02X) err=%v", expectedEcho, err)
	}

	mux.stateMu.Lock()
	if got := mux.gatewayEcho.matchCount(); got != 0 {
		t.Fatalf("matchCount after first write = %d; want 0", got)
	}
	if got := mux.gatewayEcho.writeCount(); got != 1 {
		t.Fatalf("writeCount after first write = %d; want 1", got)
	}
	if head, pending := mux.gatewayEcho.peekNextExpected(); !pending || head != expectedEcho {
		t.Fatalf("peekNextExpected = (0x%02X,%v); want (0x%02X,true)", head, pending, expectedEcho)
	}
	mux.stateMu.Unlock()

	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: foreignMaster}
	expectActiveByte(t, mux, foreignMaster)

	if !mux.activeTxn.firstByteSuspectArbLoss.Load() {
		t.Fatal("firstByteSuspectArbLoss=false; want true after first foreign initiator byte")
	}
}

func TestMux_AfterFirstEchoMatch_StaleByteStillDropped(t *testing.T) {
	const (
		firstEcho     byte = 0x15
		secondEcho    byte = 0xB5
		foreignMaster byte = 0xF1
	)
	if got := protocol.AddressClassOf(foreignMaster); got != protocol.AddressClassMaster {
		t.Fatalf("test premise broken: AddressClassOf(0x%02X) = %v; want initiator class", foreignMaster, got)
	}

	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	if _, err := at.Write([]byte{firstEcho}); err != nil {
		t.Fatalf("Write(0x%02X) err=%v", firstEcho, err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: firstEcho}
	expectActiveByte(t, mux, firstEcho)

	mux.stateMu.Lock()
	if got := mux.gatewayEcho.matchCount(); got != 1 {
		t.Fatalf("matchCount after first echo = %d; want 1", got)
	}
	if got := mux.gatewayEcho.writeCount(); got != 1 {
		t.Fatalf("writeCount after first echo = %d; want 1", got)
	}
	if head, pending := mux.gatewayEcho.peekNextExpected(); pending {
		t.Fatalf("peekNextExpected after first echo = (0x%02X,true); want no pending echo", head)
	}
	mux.stateMu.Unlock()

	if _, err := at.Write([]byte{secondEcho}); err != nil {
		t.Fatalf("Write(0x%02X) err=%v", secondEcho, err)
	}

	mux.stateMu.Lock()
	if got := mux.gatewayEcho.matchCount(); got != 1 {
		t.Fatalf("matchCount after second write = %d; want 1", got)
	}
	if got := mux.gatewayEcho.writeCount(); got != 2 {
		t.Fatalf("writeCount after second write = %d; want 2", got)
	}
	if head, pending := mux.gatewayEcho.peekNextExpected(); !pending || head != secondEcho {
		t.Fatalf("peekNextExpected = (0x%02X,%v); want (0x%02X,true)", head, pending, secondEcho)
	}
	mux.stateMu.Unlock()

	beforeReadPrefix := len(mux.ActiveTxnSnapshot().ReadPrefix)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: foreignMaster}
	waitForReadPrefixLen(t, mux, beforeReadPrefix+1)

	expectNoActiveByte(t, mux)
	if mux.activeTxn.firstByteSuspectArbLoss.Load() {
		t.Fatal("firstByteSuspectArbLoss=true after post-first-echo stale byte; want false")
	}
}

func TestMux_ArbitrationRevalidate_DoesNotFireOnNonMasterNoise(t *testing.T) {
	const (
		expectedEcho   byte = 0x15
		nonMasterNoise byte = 0x55
	)
	if got := protocol.AddressClassOf(nonMasterNoise); got == protocol.AddressClassMaster {
		t.Fatalf("test premise broken: AddressClassOf(0x%02X) = initiator class; want non-initiator class", nonMasterNoise)
	}

	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	if _, err := at.Write([]byte{expectedEcho}); err != nil {
		t.Fatalf("Write(0x%02X) err=%v", expectedEcho, err)
	}

	mux.stateMu.Lock()
	if got := mux.gatewayEcho.matchCount(); got != 0 {
		t.Fatalf("matchCount after first write = %d; want 0", got)
	}
	if got := mux.gatewayEcho.writeCount(); got != 1 {
		t.Fatalf("writeCount after first write = %d; want 1", got)
	}
	if head, pending := mux.gatewayEcho.peekNextExpected(); !pending || head != expectedEcho {
		t.Fatalf("peekNextExpected = (0x%02X,%v); want (0x%02X,true)", head, pending, expectedEcho)
	}
	mux.stateMu.Unlock()

	beforeReadPrefix := len(mux.ActiveTxnSnapshot().ReadPrefix)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: nonMasterNoise}
	waitForReadPrefixLen(t, mux, beforeReadPrefix+1)

	expectNoActiveByte(t, mux)
	if mux.activeTxn.firstByteSuspectArbLoss.Load() {
		t.Fatal("firstByteSuspectArbLoss=true after non-initiator noise; want false")
	}
}

func expectActiveByte(t *testing.T, mux *Mux, want byte) {
	t.Helper()
	select {
	case ev := <-mux.activeCh:
		if ev.kind != activeEventByte {
			t.Fatalf("activeCh event kind = %d; want byte", ev.kind)
		}
		if ev.b != want {
			t.Fatalf("activeCh byte = 0x%02X; want 0x%02X", ev.b, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for activeCh byte 0x%02X", want)
	}
}

func expectNoActiveByte(t *testing.T, mux *Mux) {
	t.Helper()
	select {
	case ev := <-mux.activeCh:
		t.Fatalf("unexpected activeCh event: kind=%d byte=0x%02X err=%v", ev.kind, ev.b, ev.err)
	case <-time.After(150 * time.Millisecond):
	}
}

func waitForReadPrefixLen(t *testing.T, mux *Mux, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := len(mux.ActiveTxnSnapshot().ReadPrefix); got >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("readPrefix length did not reach %d within timeout", want)
}
