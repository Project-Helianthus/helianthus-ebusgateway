package adaptermux

// Issue #1003 regressions: raw ENH symbol continuity and echo timing for
// external ENH sessions (contract docs/architecture/mux-raw-symbol-continuity.md).
//
// The upstream double below supplies the raw view through the same
// ReadEventWithRawSymbol shape as helianthus-ebusgo's ENH transport. Its
// adapter emulation echoes every SEND symbol through a real
// transport.EbusEscapeDecoder fed with a fixed observation time, so raw and
// logical items pair exactly as the transport pairs them. Every wait below
// is event driven; the only timeouts are failure bounds.

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
)

const rawTestBound = 2 * time.Second

type rawStep struct {
	ev        transport.StreamEvent
	symbol    byte
	hasSymbol bool
}

type rawUpstream struct {
	mu        sync.Mutex
	steps     chan rawStep
	readDone  <-chan struct{}
	writes    []byte
	starts    []byte
	writeHook func(byte)
	startHook func(byte)
	decoder   transport.EbusEscapeDecoder
	unescaped bool
	// closeCh is closed by Close; a Write still in progress at that point
	// fails, as a transport write does when its connection is closed.
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newRawUpstream() *rawUpstream {
	return &rawUpstream{steps: make(chan rawStep, 1024), unescaped: true, closeCh: make(chan struct{})}
}

func (u *rawUpstream) ReadEventWithRawSymbol() (transport.StreamEvent, byte, bool, error) {
	select {
	case <-u.readDone:
		return transport.StreamEvent{}, 0, false, io.EOF
	case st := <-u.steps:
		return st.ev, st.symbol, st.hasSymbol, nil
	}
}

func (u *rawUpstream) ReadByte() (byte, error) {
	return 0, errors.New("rawUpstream: logical byte reads are not part of this double")
}

func (u *rawUpstream) Write(p []byte) (int, error) {
	u.mu.Lock()
	u.writes = append(u.writes, p...)
	hook := u.writeHook
	u.mu.Unlock()
	if hook != nil {
		for _, b := range p {
			hook(b)
		}
	}
	select {
	case <-u.closeCh:
		return 0, net.ErrClosed
	default:
	}
	return len(p), nil
}

func (u *rawUpstream) Close() error {
	u.closeOnce.Do(func() { close(u.closeCh) })
	return nil
}

func (u *rawUpstream) RequestStart(initiator byte) error {
	u.mu.Lock()
	u.starts = append(u.starts, initiator)
	hook := u.startHook
	u.mu.Unlock()
	if hook != nil {
		hook(initiator)
	}
	return nil
}

func (u *rawUpstream) ArbitrationSendsSource() bool { return true }

func (u *rawUpstream) BytesAreUnescaped() bool { return u.unescaped }

func (u *rawUpstream) setWriteHook(hook func(byte)) {
	u.mu.Lock()
	u.writeHook = hook
	u.mu.Unlock()
}

func (u *rawUpstream) setStartHook(hook func(byte)) {
	u.mu.Lock()
	u.startHook = hook
	u.mu.Unlock()
}

func (u *rawUpstream) written() []byte {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]byte(nil), u.writes...)
}

// observe models the adapter reporting one RECEIVED wire symbol and the
// transport pairing it with the logical item it completes.
func (u *rawUpstream) observe(symbol byte) rawStep {
	u.mu.Lock()
	decoded, ok, escaped, _ := u.decoder.Feed(symbol, time.Unix(0, 0))
	u.mu.Unlock()
	st := rawStep{symbol: symbol, hasSymbol: true}
	if ok {
		st.ev = transport.StreamEvent{Kind: transport.StreamEventByte, Byte: decoded, WasEscaped: escaped}
	}
	return st
}

func (u *rawUpstream) wire(symbols ...byte) {
	for _, symbol := range symbols {
		u.steps <- u.observe(symbol)
	}
}

func (u *rawUpstream) started(initiator byte) {
	u.mu.Lock()
	u.decoder.Reset()
	u.mu.Unlock()
	u.steps <- rawStep{ev: transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}}
}

// abandonRecorder captures the external abandon watchdog instead of
// scheduling it on the wall clock.
type abandonRecorder struct {
	armed chan abandonArm
}

type abandonArm struct {
	after time.Duration
	fire  func()
}

func newRawContinuityMux(t *testing.T) (*Mux, *rawUpstream, *abandonRecorder) {
	t.Helper()
	return newRawContinuityMuxMode(t, v8classifier.ModeOff)
}

func newRawContinuityMuxMode(t *testing.T, mode v8classifier.Mode) (*Mux, *rawUpstream, *abandonRecorder) {
	t.Helper()
	up := newRawUpstream()
	mux := New(Config{
		V8ClassifierMode:         mode,
		Protocol:                 "enh",
		Network:                  "tcp",
		Address:                  "127.0.0.1:0",
		ReadTimeout:              200 * time.Millisecond,
		PendingStartTTL:          24 * time.Hour,
		SYNInterval:              time.Hour,
		ExternalStartStaleness:   -1,
		PostExternalReleaseGrace: -1,
		ExternalSessionSYNGrace:  time.Hour,
		Logger:                   log.New(io.Discard, "", 0),
	})
	rec := &abandonRecorder{armed: make(chan abandonArm, 8)}
	mux.externalAbandonAfter = func(d time.Duration, f func()) {
		rec.armed <- abandonArm{after: d, fire: f}
	}
	ctx, cancel := context.WithCancel(context.Background())
	mux.ctx, mux.cancel = ctx, cancel
	up.readDone = ctx.Done()
	mux.connMu.Lock()
	mux.upstream = up
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()

	var once sync.Once
	t.Cleanup(func() {
		once.Do(func() {
			mux.closeMu.Lock()
			mux.closing.Store(true)
			mux.closeMu.Unlock()
			cancel()
			mux.wg.Wait()
			closeOrLog(t, mux, "raw continuity mux")
		})
	})
	return mux, up, rec
}

// enhTestClient is the external ENH client end of a session pipe.
type enhTestClient struct {
	t    *testing.T
	conn net.Conn
}

func addRawClient(t *testing.T, mux *Mux) (uint64, *enhTestClient) {
	t.Helper()
	client, server := net.Pipe()
	id := mux.AddSession(server)
	if id == 0 {
		closeOrLog(t, client, "rejected client")
		t.Fatal("AddSession returned zero")
	}
	mux.sessionsMu.Lock()
	sess := mux.sessions[id]
	mux.sessionsMu.Unlock()
	t.Cleanup(func() {
		mux.RemoveSession(id)
		closeOrLog(t, client, "session client")
		sess.wg.Wait()
	})
	return id, &enhTestClient{t: t, conn: client}
}

func (c *enhTestClient) request(cmd transport.ENHCommand, data byte) {
	c.t.Helper()
	seq := transport.EncodeENH(cmd, data)
	if err := c.conn.SetWriteDeadline(time.Now().Add(rawTestBound)); err != nil {
		c.t.Fatalf("SetWriteDeadline: %v", err)
	}
	if _, err := c.conn.Write(seq[:]); err != nil {
		c.t.Fatalf("client write %v 0x%02X: %v", cmd, data, err)
	}
}

// next reads one ENH response frame (short-form bytes are RECEIVED).
func (c *enhTestClient) next() transport.ENHFrame {
	c.t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(rawTestBound)); err != nil {
		c.t.Fatalf("SetReadDeadline: %v", err)
	}
	var b [2]byte
	if _, err := io.ReadFull(c.conn, b[:1]); err != nil {
		c.t.Fatalf("client read: %v", err)
	}
	if b[0] < 0x80 {
		return transport.ENHFrame{Command: transport.ENHResReceived, Data: b[0]}
	}
	if _, err := io.ReadFull(c.conn, b[1:]); err != nil {
		c.t.Fatalf("client read second byte: %v", err)
	}
	frame, err := transport.DecodeENH(b[0], b[1])
	if err != nil {
		c.t.Fatalf("client decode % X: %v", b, err)
	}
	return frame
}

func (c *enhTestClient) expect(cmd transport.ENHCommand, data byte) {
	c.t.Helper()
	got := c.next()
	if got.Command != cmd || got.Data != data {
		c.t.Fatalf("client frame = {cmd=0x%X data=0x%02X}; want {cmd=0x%X data=0x%02X}", got.Command, got.Data, cmd, data)
	}
}

func (c *enhTestClient) expectReceived(symbols ...byte) {
	c.t.Helper()
	for _, symbol := range symbols {
		c.expect(transport.ENHResReceived, symbol)
	}
}

type passiveSymbol struct {
	symbol  byte
	escaped bool
}

func collectPassiveSymbols(mux *Mux) <-chan passiveSymbol {
	ch := make(chan passiveSymbol, 256)
	mux.SetPassiveCallback(func(ev PassiveEvent) {
		if ev.Kind == PassiveEventSymbol {
			ch <- passiveSymbol{symbol: ev.Symbol, escaped: ev.WasEscaped}
		}
	})
	return ch
}

func expectPassive(t *testing.T, ch <-chan passiveSymbol, want ...passiveSymbol) {
	t.Helper()
	timer := time.NewTimer(rawTestBound)
	defer timer.Stop()
	for i, w := range want {
		select {
		case got := <-ch:
			if got != w {
				t.Fatalf("passive[%d] = %+v; want %+v", i, got, w)
			}
		case <-timer.C:
			t.Fatalf("passive[%d]: not observed; want %+v", i, w)
		}
	}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// reservedCRCData returns a non-reserved data byte d for which
// protocol.CRC(append(prefix, d)) equals crc.
func reservedCRCData(t *testing.T, prefix []byte, crc byte) byte {
	t.Helper()
	for d := 0; d < 0x100; d++ {
		b := byte(d)
		if b == protocol.SymbolEscape || b == protocol.SymbolSyn {
			continue
		}
		if protocol.CRC(append(append([]byte(nil), prefix...), b)) == crc {
			return b
		}
	}
	t.Fatalf("no data byte yields CRC 0x%02X for prefix % X", crc, prefix)
	return 0
}

// TestRawContinuity_ResponsePairsAndStructuralSynReachSessionsRaw: every
// external session receives each observed wire symbol as its own RECEIVED,
// the escape lead before its suffix exists; passive reconstruction receives
// the logical stream with provenance and never a raw suffix as data.
func TestRawContinuity_ResponsePairsAndStructuralSynReachSessionsRaw(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	passive := collectPassiveSymbols(mux)
	_, owner := addRawClient(t, mux)
	_, monitor := addRawClient(t, mux)

	// Lead only: both sessions see it before any suffix is observed.
	up.wire(0xA9)
	owner.expectReceived(0xA9)
	monitor.expectReceived(0xA9)

	up.wire(0x00, 0xA9, 0x01, protocol.SymbolSyn)
	owner.expectReceived(0x00, 0xA9, 0x01, protocol.SymbolSyn)
	monitor.expectReceived(0x00, 0xA9, 0x01, protocol.SymbolSyn)

	expectPassive(t, passive,
		passiveSymbol{0xA9, true},
		passiveSymbol{protocol.SymbolSyn, true},
		passiveSymbol{protocol.SymbolSyn, false},
	)
	select {
	case extra := <-passive:
		t.Fatalf("unexpected extra passive symbol %+v (raw suffix leaked as data?)", extra)
	default:
	}
}

// TestRawContinuity_ClientWaitsForEachRawSendEcho drives a raw-symbol ENH
// client that sends one SEND at a time and waits for that symbol's echo,
// with a reserved CRC in the request (0xA9 as A9 00) and in the response
// (0xAA as A9 01). Only the raw view lets the client see the echo of the
// lone lead and send the suffix.
func TestRawContinuity_ClientWaitsForEachRawSendEcho(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	passive := collectPassiveSymbols(mux)
	_, owner := addRawClient(t, mux)
	_, monitor := addRawClient(t, mux)

	up.setStartHook(func(initiator byte) { up.started(initiator) })
	up.setWriteHook(func(symbol byte) { up.wire(symbol) })

	const source, target, pb, sb = 0x31, 0x08, 0xB5, 0x09
	reqData := reservedCRCData(t, []byte{source, target, pb, sb, 0x01}, protocol.SymbolEscape)
	rspData := reservedCRCData(t, []byte{0x01}, protocol.SymbolSyn)

	owner.request(transport.ENHReqStart, source)
	owner.expect(transport.ENHResStarted, source)
	monitor.expectReceived(source)

	request := []byte{target, pb, sb, 0x01, reqData, 0xA9, 0x00}
	for _, symbol := range request {
		owner.request(transport.ENHReqSend, symbol)
		// The client advances only after this exact symbol's echo.
		owner.expectReceived(symbol)
	}

	// Target response: ACK, LEN, DATA, CRC 0xAA on the wire as A9 01.
	response := []byte{0x00, 0x01, rspData, 0xA9, 0x01}
	up.wire(response...)
	owner.expectReceived(response...)

	for _, symbol := range []byte{0x00, protocol.SymbolSyn} {
		owner.request(transport.ENHReqSend, symbol)
		owner.expectReceived(symbol)
	}

	wantWrites := append(append([]byte(nil), request...), 0x00, protocol.SymbolSyn)
	if got := up.written(); !equalBytes(got, wantWrites) {
		t.Fatalf("upstream SEND symbols = % X; want exactly the client's raw symbols % X", got, wantWrites)
	}

	wantMonitor := append(append(append([]byte(nil), request...), response...), 0x00, protocol.SymbolSyn)
	monitor.expectReceived(wantMonitor...)

	expectPassive(t, passive,
		passiveSymbol{source, false},
		passiveSymbol{target, false},
		passiveSymbol{pb, false},
		passiveSymbol{sb, false},
		passiveSymbol{0x01, false},
		passiveSymbol{reqData, false},
		passiveSymbol{0xA9, true},
		passiveSymbol{0x00, false},
		passiveSymbol{0x01, false},
		passiveSymbol{rspData, false},
		passiveSymbol{protocol.SymbolSyn, true},
		passiveSymbol{0x00, false},
		passiveSymbol{protocol.SymbolSyn, false},
	)
}

func grantRawGateway(t *testing.T, mux *Mux, up *rawUpstream, initiator byte) {
	t.Helper()
	up.setStartHook(func(b byte) { up.started(b) })
	ch := mux.requestStartForSession(gatewaySessionID, initiator)
	select {
	case r := <-ch:
		if !r.granted {
			t.Fatalf("gateway grant failed: %v", r.err)
		}
	case <-time.After(rawTestBound):
		t.Fatal("gateway grant not observed")
	}
}

func readActive(t *testing.T, at transport.RawTransport) (byte, bool) {
	t.Helper()
	b, escaped, err := at.(transport.EscapeFlaggedReader).ReadByteWithEscape()
	if err != nil {
		t.Fatalf("active read: %v", err)
	}
	return b, escaped
}

// TestRawContinuity_GatewayReservedBytesSendAndVerifyBothRawSymbols: a
// gateway logical payload 0xA9 or 0xAA is sent as its two raw symbols back
// to back (no round trip between them), the write completes only after the
// lead's raw echo was observed, and the logical echo returned to
// protocol.Bus carries escape provenance. A structural SYN is one raw 0xAA.
func TestRawContinuity_GatewayReservedBytesSendAndVerifyBothRawSymbols(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	suffixWritten := make(chan byte, 1)
	var pairLead atomic.Bool
	up.setWriteHook(func(symbol byte) {
		switch {
		case symbol == protocol.SymbolEscape && !pairLead.Load():
			// Withhold the lead's echo: the suffix must still follow
			// immediately, and the write must not complete yet.
			pairLead.Store(true)
		case pairLead.Load():
			pairLead.Store(false)
			suffixWritten <- symbol
		default:
			up.wire(symbol)
		}
	})

	if _, err := at.Write([]byte{0xFE}); err != nil {
		t.Fatalf("Write DST: %v", err)
	}
	if b, escaped := readActive(t, at); b != 0xFE || escaped {
		t.Fatalf("DST echo = 0x%02X escaped=%v", b, escaped)
	}

	for _, tc := range []struct {
		logical byte
		symbols []byte
	}{
		{logical: protocol.SymbolEscape, symbols: []byte{0xA9, 0x00}},
		{logical: protocol.SymbolSyn, symbols: []byte{0xA9, 0x01}},
	} {
		before := len(up.written())
		done := make(chan error, 1)
		go func() {
			_, err := at.Write([]byte{tc.logical})
			done <- err
		}()
		select {
		case suffix := <-suffixWritten:
			if suffix != tc.symbols[1] {
				t.Fatalf("suffix for logical 0x%02X = 0x%02X; want 0x%02X", tc.logical, suffix, tc.symbols[1])
			}
		case <-time.After(rawTestBound):
			t.Fatalf("suffix for logical 0x%02X was not sent while the lead echo was outstanding", tc.logical)
		}
		if got := up.written()[before:]; !equalBytes(got, tc.symbols) {
			t.Fatalf("raw symbols for logical 0x%02X = % X; want % X", tc.logical, got, tc.symbols)
		}
		select {
		case err := <-done:
			t.Fatalf("Write logical 0x%02X completed (err=%v) before the lead's raw echo was observed", tc.logical, err)
		default:
		}
		up.wire(tc.symbols...)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Write logical 0x%02X: %v", tc.logical, err)
			}
		case <-time.After(rawTestBound):
			t.Fatalf("Write logical 0x%02X did not complete", tc.logical)
		}
		if b, escaped := readActive(t, at); b != tc.logical || !escaped {
			t.Fatalf("logical echo = 0x%02X escaped=%v; want 0x%02X escaped=true", b, escaped, tc.logical)
		}
	}

	before := len(up.written())
	writeTerminatorSyn(at)
	if got := up.written()[before:]; !equalBytes(got, []byte{protocol.SymbolSyn}) {
		t.Fatalf("structural SYN raw symbols = % X; want [AA]", got)
	}
	if b, escaped := readActive(t, at); b != protocol.SymbolSyn || escaped {
		t.Fatalf("terminator echo = 0x%02X escaped=%v; want raw SYN", b, escaped)
	}
}

// TestRawContinuity_GatewayLeadMismatchFailsAsCollision: when the first
// eligible raw symbol after the write is not the lead, the write fails as a
// bus collision.
func TestRawContinuity_GatewayLeadMismatchFailsAsCollision(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	up.setWriteHook(func(symbol byte) {
		if symbol == 0x00 {
			// A foreign symbol where the lead's echo should be.
			up.wire(0x55)
		}
	})
	_, err := at.Write([]byte{protocol.SymbolEscape})
	if !errors.Is(err, ebuserrors.ErrBusCollision) {
		t.Fatalf("Write error = %v; want ErrBusCollision", err)
	}
	if got := up.written(); !equalBytes(got, []byte{0xA9, 0x00}) {
		t.Fatalf("upstream SEND symbols = % X; want A9 00", got)
	}
	// The failed pair hands the transaction to the bounded collision
	// recovery lifecycle: protocol.Bus waits for two SYN symbols before it
	// retries, so they must reach the active path, after which Retry
	// releases the grant.
	lifecycle := at.(transport.CollisionRecoveryLifecycle)
	token := lifecycle.CollisionRecoveryToken()
	if token == 0 {
		t.Fatal("no collision recovery token after a lead-echo collision")
	}
	up.wire(protocol.SymbolSyn, protocol.SymbolSyn)
	for i := 0; i < protocol.CollisionRetryResyncSYNCount; i++ {
		if b, _ := readActive(t, at); b != protocol.SymbolSyn {
			t.Fatalf("recovery read %d = 0x%02X; want SYN", i, b)
		}
	}
	lifecycle.CompleteCollisionRecovery(token, transport.CollisionRecoveryRetry)
	mux.stateMu.Lock()
	active := mux.gatewayTxnActive
	mux.stateMu.Unlock()
	if active {
		t.Fatal("gateway transaction still active after collision recovery completed")
	}
	if _, _, owned := mux.arb.owner(); owned {
		t.Fatal("grant not released after collision recovery")
	}
}

// TestRawContinuity_GatewayLeadIgnoresUnsolicitedStructuralSyn: a raw 0xAA
// that completes a logical wire SYN is not eligible to answer a pending
// payload lead; the real lead echo after it completes the write.
func TestRawContinuity_GatewayLeadIgnoresUnsolicitedStructuralSyn(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	up.setWriteHook(func(symbol byte) {
		if symbol == 0x00 {
			up.wire(protocol.SymbolSyn, 0xA9, 0x00)
		}
	})
	if _, err := at.Write([]byte{protocol.SymbolEscape}); err != nil {
		t.Fatalf("Write error = %v; want success (the structural SYN is not the lead's echo)", err)
	}
	if b, escaped := readActive(t, at); b != protocol.SymbolEscape || !escaped {
		t.Fatalf("logical echo = 0x%02X escaped=%v; want 0xA9 escaped=true", b, escaped)
	}
}

// TestRawContinuity_GatewayLeadRetiredByAdapterReset: a RESETTED boundary
// retires the outstanding lead; nothing observed after it can acknowledge
// the old write.
func TestRawContinuity_GatewayLeadRetiredByAdapterReset(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	up.setWriteHook(func(symbol byte) {
		if symbol == 0x00 {
			up.steps <- rawStep{ev: transport.StreamEvent{Kind: transport.StreamEventReset}}
		}
	})
	_, err := at.Write([]byte{protocol.SymbolEscape})
	if !errors.Is(err, ebuserrors.ErrAdapterReset) {
		t.Fatalf("Write error = %v; want ErrAdapterReset", err)
	}
	if mux.gatewayLeadWait.Load() != nil {
		t.Fatal("lead wait survived the reset boundary")
	}
}

// TestRawContinuity_ResetBetweenLeadAndSuffixSuppressesSuffix: a RESETTED
// boundary processed after the lead write retires the wait and releases
// ownership, so the suffix is never transmitted.
func TestRawContinuity_ResetBetweenLeadAndSuffixSuppressesSuffix(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	// The lead is written under the ownership lock, so the boundary is
	// injected at the commit point between the lead and the suffix.
	mux.gatewaySuffixCommitHook = func() { mux.handleReset() }
	_, err := at.Write([]byte{protocol.SymbolEscape})
	if !errors.Is(err, ebuserrors.ErrAdapterReset) {
		t.Fatalf("Write error = %v; want ErrAdapterReset", err)
	}
	up.mu.Lock()
	writes := append([]byte(nil), up.writes...)
	up.mu.Unlock()
	if len(writes) == 0 || writes[len(writes)-1] != protocol.SymbolEscape {
		t.Fatalf("upstream writes = % X; want the lead as the last symbol and no suffix", writes)
	}
}

// TestRawContinuity_ResetAfterLeadEchoBeforeSuffixSuppressesSuffix: the lead
// echo is observed first (resolving the wait), then a RESETTED boundary is
// processed before the suffix write; the suffix must still not be sent.
func TestRawContinuity_ResetAfterLeadEchoBeforeSuffixSuppressesSuffix(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	mux.gatewaySuffixCommitHook = func() {
		mux.observeGatewayLeadEcho(protocol.SymbolEscape, transport.StreamEvent{})
		mux.handleReset()
	}
	_, err := at.Write([]byte{protocol.SymbolEscape})
	if !errors.Is(err, ebuserrors.ErrAdapterReset) {
		t.Fatalf("Write error = %v; want ErrAdapterReset", err)
	}
	up.mu.Lock()
	writes := append([]byte(nil), up.writes...)
	up.mu.Unlock()
	if len(writes) == 0 || writes[len(writes)-1] != protocol.SymbolEscape {
		t.Fatalf("upstream writes = % X; want the lead as the last symbol and no suffix", writes)
	}
}

// TestRawContinuity_GatewayLeadRetiredByClose: closing the mux fails an
// outstanding lead wait instead of leaving the write blocked.
func TestRawContinuity_GatewayLeadRetiredByClose(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	pairSent := make(chan struct{}, 1)
	up.setWriteHook(func(symbol byte) {
		if symbol == 0x00 {
			pairSent <- struct{}{}
		}
	})
	done := make(chan error, 1)
	go func() {
		_, err := at.Write([]byte{protocol.SymbolEscape})
		done <- err
	}()
	select {
	case <-pairSent:
	case <-time.After(rawTestBound):
		t.Fatal("escape pair was not sent")
	}
	if err := mux.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Write succeeded although its lead echo was never observed")
		}
	case <-time.After(rawTestBound):
		t.Fatal("Write stayed blocked after Close")
	}
	if mux.gatewayLeadWait.Load() != nil {
		t.Fatal("lead wait survived Close")
	}
}

// TestRawContinuity_ProtocolBusReservedCRC sends a broadcast whose CRC is
// 0xA9 through protocol.Bus: the mux transmits and verifies A9 00 and the
// request completes.
func TestRawContinuity_ProtocolBusReservedCRC(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	up.setStartHook(func(initiator byte) { up.started(initiator) })
	up.setWriteHook(func(symbol byte) { up.wire(symbol) })

	const source, pb, sb = 0x10, 0xB5, 0x16
	data := reservedCRCData(t, []byte{source, protocol.AddressBroadcast, pb, sb, 0x01}, protocol.SymbolEscape)

	config := protocol.DefaultBusConfig()
	config.ReconnectRetries = 0
	bus := protocol.NewBus(mux.ActiveTransport(), config, 8)
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	bus.Run(runCtx)

	reqCtx, cancel := context.WithTimeout(context.Background(), rawTestBound)
	defer cancel()
	if _, err := bus.Send(reqCtx, protocol.Frame{
		Source:    source,
		Target:    protocol.AddressBroadcast,
		Primary:   pb,
		Secondary: sb,
		Data:      []byte{data},
	}); err != nil {
		t.Fatalf("Send = %v", err)
	}
	want := []byte{protocol.AddressBroadcast, pb, sb, 0x01, data, 0xA9, 0x00, protocol.SymbolSyn}
	if got := up.written(); !equalBytes(got, want) {
		t.Fatalf("upstream SEND symbols = % X; want % X", got, want)
	}
}

// TestRawContinuity_GrantVisibleBehindQueuedPassiveTraffic: STARTED keeps
// its stream position behind older queued passive symbols (no reordering),
// and the abandon watchdog starts only once STARTED reached the client, so
// the abandon policy measures the time the client actually had.
func TestRawContinuity_GrantVisibleBehindQueuedPassiveTraffic(t *testing.T) {
	mux, up, rec := newRawContinuityMux(t)
	passive := collectPassiveSymbols(mux)
	id, owner := addRawClient(t, mux)
	up.setStartHook(func(initiator byte) {
		up.started(initiator)
		// A symbol observed after the grant must stay behind STARTED.
		up.wire(0x08)
	})

	queued := []byte{0x10, 0xFE, 0xB5, 0x16, 0x01, 0xA9, 0x00, 0x42}
	up.wire(queued...)
	expectPassive(t, passive,
		passiveSymbol{0x10, false}, passiveSymbol{0xFE, false}, passiveSymbol{0xB5, false},
		passiveSymbol{0x16, false}, passiveSymbol{0x01, false}, passiveSymbol{0xA9, true},
		passiveSymbol{0x42, false},
	)
	// The client has read nothing: its writer is blocked on the first
	// queued symbol. Let the next START take the idle grant path.
	mux.stateMu.Lock()
	mux.lastWireActivity = time.Time{}
	mux.stateMu.Unlock()
	owner.request(transport.ENHReqStart, 0x31)
	expectPassive(t, passive, passiveSymbol{0x31, false}, passiveSymbol{0x08, false})

	if !mux.arb.isOwner(id) {
		t.Fatal("session is not the bus owner after the grant")
	}
	select {
	case arm := <-rec.armed:
		t.Fatalf("abandon watchdog armed (after %v) while STARTED was still queued behind passive traffic", arm.after)
	default:
	}

	owner.expectReceived(queued...)
	owner.expect(transport.ENHResStarted, 0x31)
	var arm abandonArm
	select {
	case arm = <-rec.armed:
	case <-time.After(rawTestBound):
		t.Fatal("abandon watchdog was not armed after STARTED reached the client")
	}
	if arm.after != externalAbandonDeadline {
		t.Fatalf("abandon deadline = %v; want %v", arm.after, externalAbandonDeadline)
	}
	owner.expectReceived(0x08)

	// No SEND followed the visible grant: the deadline releases ownership.
	arm.fire()
	if mux.arb.isOwner(id) {
		t.Fatal("abandon watchdog did not release an unused visible grant")
	}
}

// TestRawContinuity_VisibleGrantUsedBeforeDeadlineIsKept: a SEND that
// follows the visible STARTED keeps the grant when the deadline fires.
func TestRawContinuity_VisibleGrantUsedBeforeDeadlineIsKept(t *testing.T) {
	mux, up, rec := newRawContinuityMux(t)
	id, owner := addRawClient(t, mux)
	up.setStartHook(func(initiator byte) { up.started(initiator) })
	up.setWriteHook(func(symbol byte) { up.wire(symbol) })

	owner.request(transport.ENHReqStart, 0x31)
	owner.expect(transport.ENHResStarted, 0x31)
	var arm abandonArm
	select {
	case arm = <-rec.armed:
	case <-time.After(rawTestBound):
		t.Fatal("abandon watchdog was not armed after STARTED reached the client")
	}
	owner.request(transport.ENHReqSend, 0x08)
	owner.expectReceived(0x08)
	arm.fire()
	if !mux.arb.isOwner(id) {
		t.Fatal("abandon watchdog released a grant that was used")
	}
}

// writeRecorderUpstream records SEND symbols. It supplies no raw view, so a
// gateway escape pair needs no echo to complete.
type writeRecorderUpstream struct {
	mu        sync.Mutex
	writes    []byte
	unescaped bool
}

func (u *writeRecorderUpstream) ReadByte() (byte, error) { return 0, io.EOF }
func (u *writeRecorderUpstream) Close() error            { return nil }
func (u *writeRecorderUpstream) BytesAreUnescaped() bool { return u.unescaped }
func (u *writeRecorderUpstream) Write(p []byte) (int, error) {
	u.mu.Lock()
	u.writes = append(u.writes, p...)
	u.mu.Unlock()
	return len(p), nil
}

// TestRawContinuity_WriteNormalizationIsIndependentOfReceiveFlag drives the
// production send path: the gateway write encoding follows the active
// path's advertised write side (ENH/ENS raw-symbol SEND and a logical
// receive side), the receive flag alone never enables it, and external
// sessions always pass raw symbols through.
func TestRawContinuity_WriteNormalizationIsIndependentOfReceiveFlag(t *testing.T) {
	type send struct {
		session    uint64
		data       byte
		structural bool
		want       []byte
	}
	for _, tc := range []struct {
		name         string
		protocol     string
		readsLogical bool
		wantLogical  bool
		sends        []send
	}{
		{name: "enh logical both ways", protocol: "enh", readsLogical: true, wantLogical: true, sends: []send{
			{session: gatewaySessionID, data: 0xA9, want: []byte{0xA9, 0x00}},
			{session: gatewaySessionID, data: 0xAA, want: []byte{0xA9, 0x01}},
			{session: gatewaySessionID, data: 0xAA, structural: true, want: []byte{0xAA}},
			{session: 7, data: 0xA9, want: []byte{0xA9}},
		}},
		{name: "ens logical both ways", protocol: "ens", readsLogical: true, wantLogical: true, sends: []send{
			{session: gatewaySessionID, data: 0xA9, want: []byte{0xA9, 0x00}},
		}},
		{name: "raw receive side", protocol: "enh", readsLogical: false, wantLogical: false, sends: []send{
			{session: gatewaySessionID, data: 0xA9, want: []byte{0xA9}},
		}},
		{name: "send side not raw-symbol", protocol: "plain", readsLogical: true, wantLogical: false, sends: []send{
			{session: gatewaySessionID, data: 0xA9, want: []byte{0xA9}},
			{session: gatewaySessionID, data: 0xAA, want: []byte{0xAA}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mux := New(Config{Protocol: tc.protocol, Logger: log.New(io.Discard, "", 0)})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mux.ctx, mux.cancel = ctx, cancel
			up := &writeRecorderUpstream{unescaped: tc.readsLogical}
			mux.upstream = up
			if got := mux.activeReadsAreLogical(); got != tc.readsLogical {
				t.Fatalf("activeReadsAreLogical = %v; want %v", got, tc.readsLogical)
			}
			if got := mux.ActiveTransport().(transport.EscapeAware).BytesAreUnescaped(); got != tc.wantLogical {
				t.Fatalf("BytesAreUnescaped = %v; want %v", got, tc.wantLogical)
			}
			for _, sd := range tc.sends {
				mux.arb.forceRelease()
				mux.arb.confirmOwnership(sd.session, 0x10)
				up.mu.Lock()
				up.writes = nil
				up.mu.Unlock()
				if err := mux.doSendNormalized(sd.session, sd.data, sd.structural); err != nil {
					t.Fatalf("doSendNormalized(session %d, 0x%02X, structural=%v): %v", sd.session, sd.data, sd.structural, err)
				}
				up.mu.Lock()
				got := append([]byte(nil), up.writes...)
				up.mu.Unlock()
				if !equalBytes(got, sd.want) {
					t.Fatalf("session %d 0x%02X structural=%v sent % X; want % X", sd.session, sd.data, sd.structural, got, sd.want)
				}
			}
		})
	}
}

func TestRawContinuity_GatewaySendSymbols(t *testing.T) {
	for _, tc := range []struct {
		data       byte
		structural bool
		want       []byte
	}{
		{data: 0xA9, want: []byte{0xA9, 0x00}},
		{data: 0xA9, structural: true, want: []byte{0xA9, 0x00}},
		{data: 0xAA, want: []byte{0xA9, 0x01}},
		{data: 0xAA, structural: true, want: []byte{0xAA}},
		{data: 0x00, want: []byte{0x00}},
		{data: 0x55, structural: true, want: []byte{0x55}},
	} {
		if got := gatewaySendSymbols(tc.data, tc.structural); !equalBytes(got, tc.want) {
			t.Errorf("gatewaySendSymbols(0x%02X, structural=%v) = % X; want % X", tc.data, tc.structural, got, tc.want)
		}
	}
}

// TestSessionPacer_ReservesOneSlotPerWireSymbol pins the output cadence to
// eBUS symbols: a long-form RECEIVED (two ENH bytes) reserves one slot, and
// a control frame reserves none.
func TestSessionPacer_ReservesOneSlotPerWireSymbol(t *testing.T) {
	kinds := map[sessionFrameKind]int{
		sessionFrameReceived:  1,
		sessionFrameStarted:   1,
		sessionFrameFailed:    1,
		sessionFrameResetted:  0,
		sessionFrameErrorEBUS: 0,
		sessionFrameErrorHost: 0,
		sessionFrameInfo:      0,
	}
	for kind, want := range kinds {
		if got := sessionFrameSymbols(kind); got != want {
			t.Errorf("sessionFrameSymbols(%d) = %d; want %d", kind, got, want)
		}
	}

	pacer := v8classifier.NewPacer()
	tau := v8classifier.TauWireByte
	base := time.Unix(1000, 0)
	emit := func(kind sessionFrameKind) (time.Time, bool) {
		return reserveSymbolSlots(pacer, base, sessionFrameSymbols(kind))
	}
	if got, paced := emit(sessionFrameReceived); !paced || !got.Equal(base) {
		t.Fatalf("first RECEIVED emit = %v paced=%v; want %v", got, paced, base)
	}
	// RECEIVED 0xA9 is a two-byte ENH frame but one wire symbol.
	if got, _ := emit(sessionFrameReceived); !got.Equal(base.Add(tau)) {
		t.Fatalf("long-form RECEIVED emit = %v; want %v", got, base.Add(tau))
	}
	if got, _ := emit(sessionFrameStarted); !got.Equal(base.Add(2 * tau)) {
		t.Fatalf("STARTED emit = %v; want %v (one slot per previous symbol, not per ENH byte)", got, base.Add(2*tau))
	}
	if _, paced := emit(sessionFrameResetted); paced {
		t.Fatal("RESETTED reserved a wire-symbol slot")
	}
	if got, _ := emit(sessionFrameReceived); !got.Equal(base.Add(3 * tau)) {
		t.Fatalf("RECEIVED after RESETTED emit = %v; want %v", got, base.Add(3*tau))
	}
}

// TestRawContinuity_EnforceDropRemovesRawSymbolToo: when the v8 classifier
// in enforce mode drops a mid-frame wire SYN, the raw symbol that produced
// it is withheld from sessions as well; the next symbol follows directly.
func TestRawContinuity_EnforceDropRemovesRawSymbolToo(t *testing.T) {
	mux, up, _ := newRawContinuityMuxMode(t, v8classifier.ModeEnforce)
	_, monitor := addRawClient(t, mux)

	// A foreign arbitration result puts the classifier's FSM mid-frame.
	up.steps <- rawStep{ev: transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x71}}
	monitor.expectReceived(0x71)

	up.wire(protocol.SymbolSyn, 0x10)
	monitor.expectReceived(0x10)
	if got := mux.V8Classifier().EnforceDropsAppliedTotal(); got != 1 {
		t.Fatalf("EnforceDropsAppliedTotal = %d; want 1", got)
	}
}

// TestRawContinuity_EnforceDropsAbsorbedAAInsidePendingPair: a raw-only 0xAA
// observed while an escape lead is pending has no logical item; in enforce
// mode the AA-injection filter still withholds it from sessions.
func TestRawContinuity_EnforceDropsAbsorbedAAInsidePendingPair(t *testing.T) {
	mux, up, _ := newRawContinuityMuxMode(t, v8classifier.ModeEnforce)
	_, monitor := addRawClient(t, mux)

	up.steps <- rawStep{ev: transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x71}}
	monitor.expectReceived(0x71)

	up.wire(0xA9)
	monitor.expectReceived(0xA9)
	up.steps <- rawStep{symbol: protocol.SymbolSyn, hasSymbol: true}
	up.wire(0x10)
	monitor.expectReceived(0x10)
	if got := mux.V8Classifier().EnforceDropsAppliedTotal(); got != 1 {
		t.Fatalf("EnforceDropsAppliedTotal = %d; want 1", got)
	}
}

// TestSessionPacer_LongFormReceivedReservesOneSlot drives writeFrame: after
// one long-form RECEIVED frame (two ENH bytes, one wire symbol) the pacer's
// last reserved slot is that frame's own emit time. Reserving a slot per ENH
// byte would leave it one tau after the emit, i.e. after the client already
// read the frame.
func TestSessionPacer_LongFormReceivedReservesOneSlot(t *testing.T) {
	mux, _, _ := newRawContinuityMuxMode(t, v8classifier.ModeShadow)
	id, client := addRawClient(t, mux)
	pacer := mux.SessionPacer(id)
	if pacer == nil {
		t.Fatal("SessionPacer = nil in shadow mode")
	}
	mux.sessionsMu.Lock()
	sess := mux.sessions[id]
	mux.sessionsMu.Unlock()
	sess.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: 0xA9}
	client.expectReceived(0xA9)
	readDone := time.Now()
	if last := pacer.LastScheduledEmit(); last.IsZero() || last.After(readDone) {
		t.Fatalf("last reserved slot %v; want the frame's own emit time, not after the client read it at %v", last, readDone)
	}
}

// TestRawContinuity_RetireAtSuffixCommitPointWithholdsSuffix interleaves an
// adapter RESETTED at the exact commit point of an escape pair: after the
// lead was written, before the suffix commit. The reset fences the
// ownership period, so the suffix never reaches the upstream and the write
// fails exactly once.
func TestRawContinuity_RetireAtSuffixCommitPointWithholdsSuffix(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	var resets atomic.Int32
	mux.gatewaySuffixCommitHook = func() {
		resets.Add(1)
		mux.handleReset()
	}
	failuresBefore := mux.activeTxn.writeErrTotal.Load()
	_, err := at.Write([]byte{protocol.SymbolEscape})
	if !errors.Is(err, ebuserrors.ErrAdapterReset) {
		t.Fatalf("Write error = %v; want ErrAdapterReset", err)
	}
	if got := resets.Load(); got != 1 {
		t.Fatalf("commit-point hook ran %d times; want 1", got)
	}
	if got := up.written(); !equalBytes(got, []byte{protocol.SymbolEscape}) {
		t.Fatalf("upstream SEND symbols = % X; want only the lead (no suffix after the reset)", got)
	}
	if got := mux.activeTxn.writeErrTotal.Load() - failuresBefore; got != 1 {
		t.Fatalf("active write failures = %d; want exactly 1", got)
	}
	if mux.gatewayLeadWait.Load() != nil {
		t.Fatal("pair wait survived the reset boundary")
	}
}

// TestRawContinuity_ReleaseAtSuffixCommitPointWithholdsSuffix: an ownership
// release at the commit point ends the ownership period the lead was sent
// in; the suffix is not transmitted.
func TestRawContinuity_ReleaseAtSuffixCommitPointWithholdsSuffix(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	mux.gatewaySuffixCommitHook = func() { mux.arb.releaseOwnership(gatewaySessionID) }
	_, err := at.Write([]byte{protocol.SymbolEscape})
	if !errors.Is(err, errNotBusOwner) {
		t.Fatalf("Write error = %v; want errNotBusOwner", err)
	}
	if got := up.written(); !equalBytes(got, []byte{protocol.SymbolEscape}) {
		t.Fatalf("upstream SEND symbols = % X; want only the lead", got)
	}
}

// TestRawContinuity_SuffixWriteHoldsTheOwnershipLock: the suffix is written
// while the arbitrator lock is held, the lock every lifecycle fence and
// ownership transition needs, so none of them can run between the commit
// check and the write.
func TestRawContinuity_SuffixWriteHoldsTheOwnershipLock(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	var checked atomic.Bool
	up.setWriteHook(func(symbol byte) {
		if symbol == 0x00 {
			checked.Store(true)
			if mux.arb.mu.TryLock() {
				mux.arb.mu.Unlock()
				t.Error("suffix written without holding the arbitrator lock; a reset could interleave")
			}
		}
		up.wire(symbol)
	})
	if _, err := at.Write([]byte{protocol.SymbolEscape}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !checked.Load() {
		t.Fatal("suffix write was not observed")
	}
	if b, escaped := readActive(t, at); b != protocol.SymbolEscape || !escaped {
		t.Fatalf("logical echo = 0x%02X escaped=%v; want 0xA9 escaped=true", b, escaped)
	}
}

// TestRawContinuity_MalformedSuffixEchoFailsThePair: the raw echoes
// A9 FF A9 00 follow the gateway's A9 00. The symbol immediately after the
// lead's echo is FF, so the pair fails as a collision; the later well-formed
// pair does not acknowledge it. Sessions still receive every observed symbol.
func TestRawContinuity_MalformedSuffixEchoFailsThePair(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	_, monitor := addRawClient(t, mux)
	grantRawGateway(t, mux, up, 0x10)
	monitor.expectReceived(0x10)
	at := mux.ActiveTransport()

	up.setWriteHook(func(symbol byte) {
		if symbol == 0x00 {
			up.wire(0xA9, 0xFF, 0xA9, 0x00)
		}
	})
	failuresBefore := mux.activeTxn.writeErrTotal.Load()
	_, err := at.Write([]byte{protocol.SymbolEscape})
	if !errors.Is(err, ebuserrors.ErrBusCollision) {
		t.Fatalf("Write error = %v; want ErrBusCollision (malformed suffix echo)", err)
	}
	if got := mux.activeTxn.writeErrTotal.Load() - failuresBefore; got != 1 {
		t.Fatalf("active write failures = %d; want exactly 1", got)
	}
	monitor.expectReceived(0xA9, 0xFF, 0xA9, 0x00)
	// The collision hands the transaction to the bounded recovery
	// lifecycle (see the lead-mismatch test) rather than abandoning it.
	if mux.ActiveTransport().(transport.CollisionRecoveryLifecycle).CollisionRecoveryToken() == 0 {
		t.Fatal("no collision recovery token after the failed pair")
	}
}

// TestRawContinuity_ResetBeforeLeadCommitWithholdsBothSymbols: a reset that
// lands after the ownership check but before the lead is committed (the
// pre-arm window) puts nothing on the bus.
func TestRawContinuity_ResetBeforeLeadCommitWithholdsBothSymbols(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	mux.gatewayLeadCommitHook = func() { mux.handleReset() }
	_, err := at.Write([]byte{protocol.SymbolEscape})
	if !errors.Is(err, ebuserrors.ErrAdapterReset) {
		t.Fatalf("Write error = %v; want ErrAdapterReset", err)
	}
	if got := up.written(); len(got) != 0 {
		t.Fatalf("upstream SEND symbols = % X; want none after the pre-commit reset", got)
	}
	if mux.gatewayLeadWait.Load() != nil {
		t.Fatal("pair wait survived the reset boundary")
	}
}

// TestRawContinuity_LifecycleFenceDoesNotWaitForStalledCommit: a stalled
// suffix write holds the arbitrator lock; the Close/reconnect fence must still
// return so the upstream close that unblocks the write can run.
func TestRawContinuity_LifecycleFenceDoesNotWaitForStalledCommit(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	stalled := make(chan struct{})
	release := make(chan struct{})
	up.setWriteHook(func(symbol byte) {
		if symbol == 0x00 {
			close(stalled)
			<-release
		}
		up.wire(symbol)
	})
	done := make(chan error, 1)
	go func() {
		_, err := at.Write([]byte{protocol.SymbolEscape})
		done <- err
	}()
	<-stalled
	fenced := make(chan struct{})
	go func() {
		mux.retireGatewayLeadWait(errors.New("adaptermux: closed"))
		close(fenced)
	}()
	select {
	case <-fenced:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("lifecycle fence blocked behind a stalled suffix write")
	}
	close(release)
	<-done
}

// TestRawContinuity_EnforceDroppedAAStillDecidesThePendingSuffix: in enforce
// mode the v8 filter drops the raw-only 0xAA observed right after the lead's
// echo of a gateway escape pair. The verifier still sees the true wire order
// A9 AA 00, so the pair fails as a malformed echo and the later 00 never
// acknowledges it; sessions keep the filtered view (no AA).
func TestRawContinuity_EnforceDroppedAAStillDecidesThePendingSuffix(t *testing.T) {
	mux, up, _ := newRawContinuityMuxMode(t, v8classifier.ModeEnforce)
	_, monitor := addRawClient(t, mux)
	grantRawGateway(t, mux, up, 0x10)
	monitor.expectReceived(0x10)
	at := mux.ActiveTransport()

	up.setWriteHook(func(symbol byte) {
		if symbol == 0x00 {
			up.wire(0xA9, protocol.SymbolSyn, 0x00)
		}
	})
	dropsBefore := mux.V8Classifier().EnforceDropsAppliedTotal()
	_, err := at.Write([]byte{protocol.SymbolEscape})
	if !errors.Is(err, ebuserrors.ErrBusCollision) {
		t.Fatalf("Write error = %v; want ErrBusCollision (A9 AA 00 is a malformed echo)", err)
	}
	monitor.expectReceived(0xA9, 0x00)
	if got := mux.V8Classifier().EnforceDropsAppliedTotal() - dropsBefore; got != 1 {
		t.Fatalf("enforce drops = %d; want 1 (the AA between lead and suffix)", got)
	}
}

// TestRawContinuity_CloseUnblocksStalledSuffixWrite calls the real Close
// while a gateway escape suffix write is stalled inside its ownership
// commit. Close must close the upstream first, which fails the stalled
// write, and return promptly instead of waiting behind the commit.
func TestRawContinuity_CloseUnblocksStalledSuffixWrite(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	at := mux.ActiveTransport()

	stalled := make(chan struct{})
	up.setWriteHook(func(symbol byte) {
		if symbol == 0x00 {
			close(stalled)
			<-up.closeCh // the write stays in flight until the upstream closes
		}
	})
	done := make(chan error, 1)
	go func() {
		_, err := at.Write([]byte{protocol.SymbolEscape})
		done <- err
	}()
	select {
	case <-stalled:
	case <-time.After(rawTestBound):
		t.Fatal("suffix write did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- mux.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(rawTestBound):
		_ = up.Close() // unblock the stalled write so the fixture can exit
		t.Fatal("Close blocked behind a stalled escape suffix write")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stalled suffix write reported success after Close")
		}
	case <-time.After(rawTestBound):
		t.Fatal("stalled suffix write did not fail after Close")
	}
}
