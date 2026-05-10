package adaptermux

import (
	"bytes"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// ---------------------------------------------------------------------
// Terminal-class diagnostics: echo-only / invalid-frame / success-like
// ---------------------------------------------------------------------

// TestTxnClass_EchoOnlyStream proves that when every byte the gateway
// observes on the wire is an echo of bytes it wrote (position-wise match
// into the write prefix), the terminal class is EchoOnlyTimeout. This
// is the shape of the current RPi production failure where scan probes
// write request bytes but no peer response ever arrives.
func TestTxnClass_EchoOnlyStream(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	at := mux.ActiveTransport()
	req := []byte{0x71, 0x08, 0x07, 0x04, 0x00}
	n, err := at.Write(req)
	if err != nil || n != len(req) {
		t.Fatalf("write n=%d err=%v", n, err)
	}

	// Feed back an exact echo of the written bytes (what the adapter
	// observes on the wire during normal transmission).
	for _, b := range req {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(50 * time.Millisecond)
	// Consume so bytesRead > 0 and prefix capture mirrors reality.
	for range req {
		if _, err := at.ReadByte(); err != nil {
			t.Fatalf("ReadByte err=%v", err)
		}
	}

	// Force terminal inactive via markActiveReadTimeout (simulates the
	// bus.Send read-loop timing out when no response frame arrives).
	mux.markActiveReadTimeout()

	snap := mux.ActiveTxnSnapshot()
	if snap.TxnClass != TxnClassEchoOnlyTimeout {
		t.Fatalf("TxnClass = %q, want %q (writes=%d reads=%d echo=%d nonEcho=%d syn=%d reason=%s)",
			snap.TxnClass, TxnClassEchoOnlyTimeout,
			snap.BytesWritten, snap.BytesRead, snap.EchoLike, snap.NonEcho, snap.SynMarkers, snap.InactiveReason)
	}
	if snap.NonEcho != 0 {
		t.Errorf("NonEcho = %d, want 0 (all bytes echoed)", snap.NonEcho)
	}
	if snap.EchoLike == 0 {
		t.Errorf("EchoLike = 0, want >0 (bytes should be echo-matched)")
	}
}

// TestTxnClass_NonEchoInvalidFrame proves that when the gateway sees
// bytes that do NOT match its write prefix position-wise AND no SYN
// terminator arrives, the terminal class is NonEchoInvalidFrame.
func TestTxnClass_NonEchoInvalidFrame(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	at := mux.ActiveTransport()
	req := []byte{0xAA, 0xBB} // distinctive write prefix
	if _, err := at.Write(req); err != nil {
		t.Fatalf("write err=%v", err)
	}

	// Feed back bytes that do NOT match the write prefix. No SYN.
	// P11 — these bytes are now FILTERED from activeCh (mid-write
	// non-match), but they still reach recordReadPrefixAndClassify
	// before the filter and contribute to the nonEcho counter for
	// classification purposes. Do not attempt to ReadByte — those
	// reads would time out under the P11 filter.
	noise := []byte{0x11, 0x22, 0x33}
	for _, b := range noise {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(50 * time.Millisecond)

	mux.markActiveReadTimeout()

	snap := mux.ActiveTxnSnapshot()
	if snap.TxnClass != TxnClassNonEchoInvalidFrame {
		t.Fatalf("TxnClass = %q, want %q (echo=%d nonEcho=%d syn=%d)",
			snap.TxnClass, TxnClassNonEchoInvalidFrame,
			snap.EchoLike, snap.NonEcho, snap.SynMarkers)
	}
	if snap.NonEcho == 0 {
		t.Errorf("NonEcho = 0, want >0")
	}
	if snap.SynMarkers != 0 {
		t.Errorf("SynMarkers = %d, want 0", snap.SynMarkers)
	}
}

// TestTxnClass_SuccessLike proves that a plausible response sequence —
// non-echo response bytes followed by a SYN terminator — classifies as
// SuccessLike even when the terminal reason is a SYN-based close.
func TestTxnClass_SuccessLike(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	at := mux.ActiveTransport()
	req := []byte{0x71, 0x08}
	if _, err := at.Write(req); err != nil {
		t.Fatalf("write err=%v", err)
	}

	// Feed back the gateway echo first, then a non-echo response
	// prefix and a SYN terminator.
	for _, b := range req {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(20 * time.Millisecond)
	for range req {
		if _, err := at.ReadByte(); err != nil {
			t.Fatalf("ReadByte err=%v", err)
		}
	}

	resp := []byte{0x10, 0x05, 0x42, 0xCD}
	for _, b := range resp {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(30 * time.Millisecond)
	for range resp {
		if _, err := at.ReadByte(); err != nil {
			t.Fatalf("ReadByte err=%v", err)
		}
	}
	// Trailing SYN — lifecycle-correct end-of-txn with bytesRead>0.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if snap.TxnClass != TxnClassSuccessLike {
		t.Fatalf("TxnClass = %q, want %q (echo=%d nonEcho=%d syn=%d reason=%s reads=%d writes=%d)",
			snap.TxnClass, TxnClassSuccessLike,
			snap.EchoLike, snap.NonEcho, snap.SynMarkers, snap.InactiveReason,
			snap.BytesRead, snap.BytesWritten)
	}
	if snap.NonEcho == 0 || snap.SynMarkers == 0 {
		t.Errorf("NonEcho=%d SynMarkers=%d, want both >0", snap.NonEcho, snap.SynMarkers)
	}
}

// TestTxnClass_SYNTerminatorEchoOnly proves that a trailing SYN after
// echo-only traffic does not classify as SuccessLike. The transaction
// lifecycle closed cleanly, but the mux never observed a non-echo
// response byte from the target.
func TestTxnClass_SYNTerminatorEchoOnly(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	at := mux.ActiveTransport()
	req := []byte{0x08, 0xB5, 0x09, 0x03, 0x0D, 0x54, 0x00, 0xAA}
	if _, err := at.Write(req); err != nil {
		t.Fatalf("write err=%v", err)
	}

	for _, b := range req {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(30 * time.Millisecond)
	for range req {
		if _, err := at.ReadByte(); err != nil {
			t.Fatalf("ReadByte err=%v", err)
		}
	}

	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if snap.InactiveReason != ReasonSYNTerminator {
		t.Fatalf("InactiveReason = %q, want %q", snap.InactiveReason, ReasonSYNTerminator)
	}
	if snap.TxnClass != TxnClassEchoOnlyTimeout {
		t.Fatalf("TxnClass = %q, want %q (echo=%d nonEcho=%d syn=%d reads=%d writes=%d)",
			snap.TxnClass, TxnClassEchoOnlyTimeout,
			snap.EchoLike, snap.NonEcho, snap.SynMarkers, snap.BytesRead, snap.BytesWritten)
	}
	if snap.NonEcho != 0 {
		t.Errorf("NonEcho = %d, want 0", snap.NonEcho)
	}
	if !bytes.Equal(snap.WritePrefix, snap.ReadPrefix) {
		t.Errorf("WritePrefix = % X ReadPrefix = % X; want identical echo-only prefixes", snap.WritePrefix, snap.ReadPrefix)
	}
}

func TestTxnClass_SYNTerminatorUsesWritePrefixBeforeBytesWritten(t *testing.T) {
	mux, _, _, cleanup := newP3TestMux(t)
	defer cleanup()

	mux.recordGatewayGrant(0x71, 0)
	mux.stateMu.Lock()
	mux.gatewayTxnActive = true
	mux.recordWritePrefix(0x71)
	mux.recordReadPrefixAndClassify(0x71)
	mux.recordReadPrefixAndClassify(0x10)
	mux.recordReadPrefixAndClassify(protocol.SymbolSyn)
	mux.gatewayTxnActive = false
	mux.recordGatewayInactive(ReasonSYNTerminator)
	mux.stateMu.Unlock()

	snap := mux.ActiveTxnSnapshot()
	if snap.BytesWritten != 0 {
		t.Fatalf("BytesWritten = %d, want 0 for pre-writer-counter race", snap.BytesWritten)
	}
	if snap.TxnClass != TxnClassSuccessLike {
		t.Fatalf("TxnClass = %q, want %q when writePrefix exists before BytesWritten increments", snap.TxnClass, TxnClassSuccessLike)
	}
}

// TestTxnClass_DelayedEchoAfterLeadingSYNs proves that leading SYN
// chatter does not permanently misalign echo classification for the
// next real echoed request byte.
func TestTxnClass_DelayedEchoAfterLeadingSYNs(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	at := mux.ActiveTransport()
	req := []byte{0x15, 0xB5}
	if _, err := at.Write(req); err != nil {
		t.Fatalf("write err=%v", err)
	}

	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x15}
	time.Sleep(30 * time.Millisecond)

	if b, err := at.ReadByte(); err != nil || b != 0x15 {
		t.Fatalf("ReadByte = (0x%02X,%v), want (0x15,nil)", b, err)
	}

	snap := mux.ActiveTxnSnapshot()
	if snap.EchoLike != 1 {
		t.Fatalf("EchoLike = %d, want 1", snap.EchoLike)
	}
	if snap.NonEcho != 0 {
		t.Fatalf("NonEcho = %d, want 0 for delayed echoed byte", snap.NonEcho)
	}
}

// TestTxnClass_BoundedWriteReadCapture proves that first-N prefix capture
// is bounded at txnPrefixCap (8) even when the txn writes and reads far
// more than that.
func TestTxnClass_BoundedWriteReadCapture(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	at := mux.ActiveTransport()
	// Write 20 bytes — prefix must cap at 8.
	big := make([]byte, 20)
	for i := range big {
		big[i] = byte(i + 1)
	}
	if _, err := at.Write(big); err != nil {
		t.Fatalf("write err=%v", err)
	}

	// Feed 20 bytes back (non-echo, distinct values) — read prefix
	// must also cap at 8.
	// P11 — these are filtered from activeCh (mid-write non-match)
	// but recordReadPrefixAndClassify runs BEFORE the filter, so
	// readPrefix is still populated. Don't attempt ReadByte (would
	// timeout under the P11 filter).
	for i := 0; i < 20; i++ {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: byte(0x80 + i)}
	}
	time.Sleep(60 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if len(snap.WritePrefix) != txnPrefixCap {
		t.Fatalf("WritePrefix len = %d, want %d", len(snap.WritePrefix), txnPrefixCap)
	}
	if len(snap.ReadPrefix) != txnPrefixCap {
		t.Fatalf("ReadPrefix len = %d, want %d", len(snap.ReadPrefix), txnPrefixCap)
	}
	// WritePrefix should be first 8 written bytes.
	want := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	if !bytes.Equal(snap.WritePrefix, want) {
		t.Errorf("WritePrefix = % X, want % X", snap.WritePrefix, want)
	}
	// ReadPrefix should be first 8 read bytes.
	wantR := []byte{0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87}
	if !bytes.Equal(snap.ReadPrefix, wantR) {
		t.Errorf("ReadPrefix = % X, want % X", snap.ReadPrefix, wantR)
	}
}

// TestTxnClass_SchemaError_OverridesCandidateNoParse proves that
// MarkSchemaError elevates a "candidate but no parse" classification to
// SchemaError so the scan diagnostic distinguishes "response arrived but
// payload schema rejected" from "response never arrived".
func TestTxnClass_SchemaError_OverridesCandidateNoParse(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("write err=%v", err)
	}

	// Feed the echoed write byte, then non-echo bytes and a SYN —
	// classification at inactive time becomes SuccessLike. Then mark
	// schema error via the API and ensure snapshot reflects SchemaError.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	time.Sleep(20 * time.Millisecond)
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10}
	time.Sleep(20 * time.Millisecond)
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	pre := mux.ActiveTxnSnapshot()
	if pre.TxnClass != TxnClassSuccessLike {
		t.Fatalf("pre-schema TxnClass = %q, want %q", pre.TxnClass, TxnClassSuccessLike)
	}

	mux.MarkSchemaError()

	post := mux.ActiveTxnSnapshot()
	if post.TxnClass != TxnClassSchemaError {
		t.Fatalf("post-schema TxnClass = %q, want %q", post.TxnClass, TxnClassSchemaError)
	}
	if post.LastTxnClass != TxnClassSchemaError {
		t.Fatalf("post-schema LastTxnClass = %q, want %q", post.LastTxnClass, TxnClassSchemaError)
	}
}

// TestLastTxnClass_AfterTerminal proves that LastTxnClass on the Mux
// reflects the classification of the most recently completed gateway
// transaction. This is the stable value the optional scan classifier
// (statsBus.classifier) samples after each bus.Send.
func TestLastTxnClass_AfterTerminal(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	_, _ = at.Write([]byte{0xAA})
	// Feed mismatched byte, no SYN → NonEchoInvalidFrame on timeout.
	// P11 — 0x55 is filtered from activeCh (mid-write, doesn't match
	// 0xAA), but recordReadPrefixAndClassify counts it as nonEcho
	// before the filter, which is what classifies the txn.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x55}
	time.Sleep(20 * time.Millisecond)
	mux.markActiveReadTimeout()

	if got := mux.LastTxnClass(); got != string(TxnClassNonEchoInvalidFrame) {
		t.Fatalf("LastTxnClass after terminal = %q, want %q", got, TxnClassNonEchoInvalidFrame)
	}
}
