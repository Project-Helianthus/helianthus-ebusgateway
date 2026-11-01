//go:build linux || darwin

package adaptermux

// Issue #1416: a gateway telegram containing a logical 0xA9 or 0xAA must reach
// the wire and complete. The adapter below transmits one SEND at a time and
// takes a wire symbol's time to echo it, so a suffix sent back to back with its
// lead is lost, and the real ENH transport sits between it and the mux, so a
// suffix echo beyond the decoder's escape-pending cap splits the pair.

import (
	"bytes"
	"context"
	"errors"
	"expvar"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/ptytest"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
)

const (
	escapePairTestGuard = 10 * time.Second
	// escapePairIdleSyn is the idle SYN cadence of the synthetic bus.
	escapePairIdleSyn = 40 * time.Millisecond
	escapePairSource  = 0x31
)

// escapePairLink is what the adapter needs from a pty peer or a TCP connection.
type escapePairLink interface {
	SetReadDeadline(time.Time) error
	Read([]byte) (int, error)
	Write([]byte) (int, error)
}

type escapePairInbound struct {
	message transport.ENHMessage
	at      time.Time
}

type escapePairOutbound struct {
	frame [2]byte
	at    time.Time
}

// pacedAdapter is a synthetic ENH adapter. A SEND that arrives while the
// previous symbol is still on the wire is lost. Every accepted SEND is echoed
// one wire symbol later; oneWay delays each direction of the link.
type pacedAdapter struct {
	link   escapePairLink
	symbol time.Duration
	oneWay time.Duration
	done   chan struct{}

	inbound  chan escapePairInbound
	outbound chan escapePairOutbound

	// busyUntil, owner and pendingStart belong to the event loop goroutine.
	busyUntil    time.Time
	owner        bool
	pendingStart *byte
	afterLead    bool

	mu           sync.Mutex
	wire         []byte
	rejected     int
	suffixEchoAs *byte // echoes the symbol after each lead as this value
	leadEchoAs   *byte // echoes each lead as this value
}

func servePacedAdapter(t *testing.T, link escapePairLink, oneWay time.Duration) *pacedAdapter {
	t.Helper()
	a := &pacedAdapter{
		link:     link,
		symbol:   v8classifier.TauWireByte,
		oneWay:   oneWay,
		done:     make(chan struct{}),
		inbound:  make(chan escapePairInbound, 256),
		outbound: make(chan escapePairOutbound, 1024),
	}
	t.Cleanup(func() { close(a.done) })
	go a.readLoop()  // goroutine: synthetic adapter; ends with the test
	go a.eventLoop() // goroutine: synthetic adapter; ends with the test
	go a.writeLoop() // goroutine: synthetic adapter; ends with the test
	return a
}

func (a *pacedAdapter) readLoop() {
	var parser transport.ENHParser
	buf := make([]byte, 64)
	for {
		select {
		case <-a.done:
			return
		default:
		}
		_ = a.link.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
		n, err := a.link.Read(buf)
		now := time.Now()
		for _, b := range buf[:max(n, 0)] {
			message, ok, feedErr := parser.Feed(b)
			if feedErr == nil && ok && message.Kind == transport.ENHMessageFrame {
				a.inbound <- escapePairInbound{message: message, at: now.Add(a.oneWay)}
			}
		}
		switch {
		case err == nil, errors.Is(err, os.ErrDeadlineExceeded):
		case errors.Is(err, syscall.EAGAIN):
			time.Sleep(time.Millisecond)
		default:
			return
		}
	}
}

func (a *pacedAdapter) eventLoop() {
	ticker := time.NewTicker(escapePairIdleSyn)
	defer ticker.Stop()
	for {
		select {
		case <-a.done:
			return
		case in := <-a.inbound:
			time.Sleep(time.Until(in.at))
			a.handle(in.message)
		case <-ticker.C:
			a.tick()
		}
	}
}

func (a *pacedAdapter) writeLoop() {
	for {
		select {
		case <-a.done:
			return
		case out := <-a.outbound:
			time.Sleep(time.Until(out.at))
			frame := out.frame[:]
			for len(frame) > 0 {
				n, err := a.link.Write(frame)
				frame = frame[max(n, 0):]
				if errors.Is(err, syscall.EAGAIN) {
					time.Sleep(time.Millisecond)
				} else if err != nil {
					return
				}
			}
		}
	}
}

func (a *pacedAdapter) send(command transport.ENHCommand, data byte, after time.Duration) {
	a.outbound <- escapePairOutbound{frame: transport.EncodeENH(command, data), at: time.Now().Add(after + a.oneWay)}
}

func (a *pacedAdapter) handle(message transport.ENHMessage) {
	switch message.Command {
	case transport.ENHReqInit:
		a.send(transport.ENHResResetted, message.Data, 0)
	case transport.ENHReqInfo:
		a.send(transport.ENHResInfo, 0, 0)
	case transport.ENHReqStart:
		initiator := message.Data
		a.pendingStart = &initiator
	case transport.ENHReqSend:
		now := time.Now()
		a.mu.Lock()
		defer a.mu.Unlock()
		if now.Before(a.busyUntil) {
			a.rejected++
			return
		}
		a.busyUntil = now.Add(a.symbol)
		a.wire = append(a.wire, message.Data)
		echo := message.Data
		switch {
		case a.afterLead && a.suffixEchoAs != nil:
			echo = *a.suffixEchoAs
		case message.Data == protocol.SymbolEscape && a.leadEchoAs != nil:
			echo = *a.leadEchoAs
		}
		a.afterLead = message.Data == protocol.SymbolEscape
		if message.Data == protocol.SymbolSyn {
			a.owner = false
		}
		a.send(transport.ENHResReceived, echo, a.symbol)
	}
}

// tick is one idle bus step: a pending START is granted, else a SYN goes out.
func (a *pacedAdapter) tick() {
	a.mu.Lock()
	busy := time.Now().Before(a.busyUntil)
	a.mu.Unlock()
	if a.owner || busy {
		return
	}
	if a.pendingStart != nil {
		a.send(transport.ENHResStarted, *a.pendingStart, 0)
		a.pendingStart = nil
		a.owner = true
		return
	}
	a.send(transport.ENHResReceived, protocol.SymbolSyn, 0)
}

func (a *pacedAdapter) takeWire() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	wire := a.wire
	a.wire = nil
	return wire
}

func (a *pacedAdapter) rejectedSends() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.rejected
}

func (a *pacedAdapter) corrupt(lead, suffix *byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.leadEchoAs, a.suffixEchoAs = lead, suffix
}

// escapeTelegram is a broadcast whose expected SEND symbols are derived here,
// independently of protocol.Bus: a CRC of polynomial 0x9B over the source
// address (STARTED carries it) and the escape-expanded telegram.
type escapeTelegram struct {
	name string
	data []byte
	wire []byte
}

func testEscapeExpand(symbols []byte) []byte {
	var out []byte
	for _, b := range symbols {
		switch b {
		case protocol.SymbolEscape:
			out = append(out, protocol.SymbolEscape, 0x00)
		case protocol.SymbolSyn:
			out = append(out, protocol.SymbolEscape, 0x01)
		default:
			out = append(out, b)
		}
	}
	return out
}

func testEbusCRC(expanded []byte) byte {
	var crc byte
	for _, b := range expanded {
		for bit := 0; bit < 8; bit++ {
			if crc&0x80 != 0 {
				crc = crc<<1 ^ 0x9B
			} else {
				crc <<= 1
			}
		}
		crc ^= b
	}
	return crc
}

func newEscapeTelegram(name string, data []byte) (escapeTelegram, byte) {
	header := []byte{protocol.AddressBroadcast, 0xB5, 0x16, byte(len(data))}
	body := testEscapeExpand(append(append([]byte(nil), header...), data...))
	crc := testEbusCRC(append([]byte{escapePairSource}, body...))
	wire := append(append(body, testEscapeExpand([]byte{crc})...), protocol.SymbolSyn)
	return escapeTelegram{name: name, data: data, wire: wire}, crc
}

// escapeTelegramVectors generates telegrams with CRC 0xA9, with CRC 0xAA and
// with each reserved value in the payload.
func escapeTelegramVectors(t *testing.T) []escapeTelegram {
	t.Helper()
	findLast := func(name string, prefix []byte, accept func(crc byte) bool) escapeTelegram {
		for d := 0; d < 0x100; d++ {
			b := byte(d)
			if b == protocol.SymbolEscape || b == protocol.SymbolSyn {
				continue
			}
			telegram, crc := newEscapeTelegram(name, append(append([]byte(nil), prefix...), b))
			if accept(crc) {
				return telegram
			}
		}
		t.Fatalf("no data byte for vector %s", name)
		return escapeTelegram{}
	}
	plainCRC := func(crc byte) bool { return crc != protocol.SymbolEscape && crc != protocol.SymbolSyn }
	return []escapeTelegram{
		findLast("crc A9", nil, func(crc byte) bool { return crc == protocol.SymbolEscape }),
		findLast("crc AA", nil, func(crc byte) bool { return crc == protocol.SymbolSyn }),
		findLast("payload A9", []byte{protocol.SymbolEscape}, plainCRC),
		findLast("payload AA", []byte{protocol.SymbolSyn}, plainCRC),
	}
}

// driveEscapeTelegrams sends every vector through protocol.Bus and asserts each
// completes first time, with exactly the expected symbols on the wire.
func driveEscapeTelegrams(t *testing.T, mux *Mux, adapter *pacedAdapter) {
	t.Helper()
	config := protocol.DefaultBusConfig()
	config.ReconnectRetries = 0
	bus := protocol.NewBus(mux.ActiveTransport(), config, 8)
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	bus.Run(runCtx)

	for _, telegram := range escapeTelegramVectors(t) {
		adapter.takeWire()
		reqCtx, cancel := context.WithTimeout(context.Background(), escapePairTestGuard)
		_, err := bus.Send(reqCtx, protocol.Frame{
			Source:    escapePairSource,
			Target:    protocol.AddressBroadcast,
			Primary:   0xB5,
			Secondary: 0x16,
			Data:      telegram.data,
		})
		cancel()
		if err != nil {
			t.Fatalf("%s: Send = %v", telegram.name, err)
		}
		if got := adapter.takeWire(); !bytes.Equal(got, telegram.wire) {
			t.Fatalf("%s: wire symbols = % X; want % X (a retry repeats them)", telegram.name, got, telegram.wire)
		}
	}

	if got := adapter.rejectedSends(); got != 0 {
		t.Errorf("adapter lost %d SEND requests sent while it was transmitting", got)
	}
	if got := mux.GatewayEscapeEchoSnapshot(); got != (GatewayEscapeEchoSnapshot{}) {
		t.Errorf("escape echo failures = %+v; want none", got)
	}
	if got := mux.activeTxn.writeErrTotal.Load(); got != 0 {
		t.Errorf("active write failures = %d; want 0", got)
	}
	if got := mux.ActiveTxnSnapshot().InactiveReasonCounts[ReasonArbitrationCollision]; got != 0 {
		t.Errorf("transactions ended as %s = %d; want 0", ReasonArbitrationCollision, got)
	}
	mux.connMu.Lock()
	upstream, ok := mux.upstream.(interface{ EscapePendingTimeoutTotal() uint64 })
	mux.connMu.Unlock()
	if !ok {
		t.Fatal("upstream reports no escape-pending timeouts")
	}
	if got := upstream.EscapePendingTimeoutTotal(); got != 0 {
		t.Errorf("decoder abandoned %d leads", got)
	}
}

func startPtyEscapeMux(t *testing.T, logs serialLogLines) (*Mux, *pacedAdapter) {
	t.Helper()
	pair := ptytest.Open(t)
	adapter := servePacedAdapter(t, pair.Peer, 0)
	mux := newSerialTestMux("ens", pair.DevicePath, logs)
	if err := mux.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = mux.Close() })
	if got := pair.Baud(t); got != 115200 {
		t.Fatalf("speed = %d, want 115200", got)
	}
	return mux, adapter
}

// Kills: a suffix written back to back with its lead (the adapter loses it), a
// wrong encoding of either reserved value, and any retry of a vector.
func TestEscapePair_SerialEnsVectorsComplete(t *testing.T) {
	mux, adapter := startPtyEscapeMux(t, nil)
	driveEscapeTelegrams(t, mux, adapter)
}

// Kills: a suffix held back for the lead's echo without a bound shorter than
// the decoder cap. Above the cap the echo would come too late for the decoder
// to keep the lead, so the pair must be sent inside the bound.
func TestEscapePair_TCPEnhVectorsComplete(t *testing.T) {
	for _, test := range []struct {
		name      string
		roundTrip time.Duration
	}{
		{"echo arrives inside the bound", gatewaySuffixPaceBound / 2},
		{"round trip above the decoder cap", transport.EscapePendingTimeout + 8*time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			defer func() { _ = listener.Close() }()
			started := make(chan *pacedAdapter, 1)
			go func() { // goroutine: ends with the single connection or the listener's close
				conn, acceptErr := listener.Accept()
				if acceptErr != nil {
					close(started)
					return
				}
				t.Cleanup(func() { _ = conn.Close() })
				started <- servePacedAdapter(t, conn, test.roundTrip/2)
			}()

			mux := New(Config{
				Protocol:     "enh",
				Network:      "tcp",
				Address:      listener.Addr().String(),
				ReadTimeout:  20 * time.Millisecond,
				WriteTimeout: time.Second,
			})
			if err := mux.Start(context.Background()); err != nil {
				t.Fatalf("start: %v", err)
			}
			defer func() { _ = mux.Close() }()
			adapter, ok := <-started
			if !ok {
				t.Fatal("mux never connected")
			}
			driveEscapeTelegrams(t, mux, adapter)
		})
	}
}

func escapeFailureCount(key string) int64 {
	if v, ok := adaptermuxGatewayEscapeEchoFailuresTotal.Get(key).(*expvar.Int); ok {
		return v.Value()
	}
	return 0
}

// Each row varies one field of the failure record. Kills: a failure classed as
// anything but a collision (cause), the step not recorded (step), the wrong
// symbol of the pair reported (written: suffix 0x00 against 0x01, lead 0xA9),
// the observed symbol dropped or fixed (observed), a mismatch not logged, and
// a suffix sent after its lead was already refuted (wire).
func TestEscapePair_SerialWrongEchoFailsAsCollisionLoggedAndCounted(t *testing.T) {
	wrongSuffix, wrongSuffix2, wrongLead := byte(0xFF), byte(0x7E), byte(0x55)
	for _, test := range []struct {
		name         string
		data         byte
		leadEcho     *byte
		suffixEcho   *byte
		wantStep     gatewayEscapeStep
		wantWritten  byte
		wantObserved byte
		wantWire     []byte
	}{
		{"suffix of A9", protocol.SymbolEscape, nil, &wrongSuffix, gatewayEscapeStepSuffix, 0x00, wrongSuffix, []byte{0xA9, 0x00}},
		{"suffix of AA", protocol.SymbolSyn, nil, &wrongSuffix2, gatewayEscapeStepSuffix, 0x01, wrongSuffix2, []byte{0xA9, 0x01}},
		{"lead", protocol.SymbolSyn, &wrongLead, nil, gatewayEscapeStepLead, 0xA9, wrongLead, []byte{0xA9}},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs := make(serialLogLines, 256)
			mux, adapter := startPtyEscapeMux(t, logs)
			at := mux.ActiveTransport()
			if err := at.(interface{ StartArbitration(byte) error }).StartArbitration(escapePairSource); err != nil {
				t.Fatalf("StartArbitration: %v", err)
			}
			adapter.takeWire()
			adapter.corrupt(test.leadEcho, test.suffixEcho)
			key := fmt.Sprintf("%s_observed_0x%02X", test.wantStep, test.wantObserved)
			countBefore := escapeFailureCount(key)

			_, err := at.Write([]byte{test.data})
			if !errors.Is(err, ebuserrors.ErrBusCollision) {
				t.Fatalf("Write = %v; want ErrBusCollision", err)
			}
			var echoErr *gatewayEscapeEchoError
			if !errors.As(err, &echoErr) {
				t.Fatalf("Write = %T; want the typed verification error", err)
			}
			if echoErr.step != test.wantStep || echoErr.written != test.wantWritten ||
				echoErr.observed != test.wantObserved || !echoErr.echoObserved {
				t.Fatalf("failure = %+v; want step %s written 0x%02X observed 0x%02X", echoErr, test.wantStep, test.wantWritten, test.wantObserved)
			}
			if got := adapter.takeWire(); !bytes.Equal(got, test.wantWire) {
				t.Fatalf("wire symbols = % X; want % X", got, test.wantWire)
			}

			want := GatewayEscapeEchoSnapshot{
				LastStep:         string(test.wantStep),
				LastWritten:      test.wantWritten,
				LastObserved:     test.wantObserved,
				LastEchoObserved: true,
			}
			if test.wantStep == gatewayEscapeStepLead {
				want.LeadFailures = 1
			} else {
				want.SuffixFailures = 1
			}
			if got := mux.GatewayEscapeEchoSnapshot(); got != want {
				t.Fatalf("snapshot = %+v; want %+v", got, want)
			}
			if got := escapeFailureCount(key) - countBefore; got != 1 {
				t.Fatalf("expvar %s moved by %d; want 1", key, got)
			}
			logs.wait(t, fmt.Sprintf("step=%s written=0x%02X observed=0x%02X", test.wantStep, test.wantWritten, test.wantObserved))
			if at.(transport.CollisionRecoveryLifecycle).CollisionRecoveryToken() == 0 {
				t.Fatal("no collision recovery token after the failed pair")
			}
		})
	}
}

// Kills: a bound that leaves the decoder no margin, or none for the adapter to
// finish the lead.
func TestEscapePair_PaceBoundStaysBetweenSymbolTimeAndDecoderCap(t *testing.T) {
	if gatewaySuffixPaceBound < 2*v8classifier.TauWireByte {
		t.Fatalf("bound %v leaves the adapter under two wire symbols", gatewaySuffixPaceBound)
	}
	if transport.EscapePendingTimeout-gatewaySuffixPaceBound < gatewaySuffixPaceBound {
		t.Fatalf("bound %v leaves under its own length of the %v decoder cap", gatewaySuffixPaceBound, transport.EscapePendingTimeout)
	}
}

// Kills: the suffix waiting out the bound although the echo arrived (the echo
// is the release) and the suffix not waiting when no echo arrives.
func TestEscapePair_SuffixFollowsTheEchoOrTheBound(t *testing.T) {
	measure := func(echoLead bool) time.Duration {
		mux, up, _ := newRawContinuityMux(t)
		grantRawGateway(t, mux, up, 0x10)
		var leadAt, suffixAt time.Time
		up.setWriteHook(func(symbol byte) {
			if symbol == protocol.SymbolEscape {
				leadAt = time.Now()
				if echoLead {
					up.wire(symbol)
				}
				return
			}
			suffixAt = time.Now()
			if !echoLead {
				up.wire(protocol.SymbolEscape)
			}
			up.wire(symbol)
		})
		if _, err := mux.ActiveTransport().Write([]byte{protocol.SymbolEscape}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		return suffixAt.Sub(leadAt)
	}
	if gap := measure(false); gap < gatewaySuffixPaceBound {
		t.Fatalf("suffix %v after the lead with no echo; want at least %v", gap, gatewaySuffixPaceBound)
	}
	// A loaded host can delay one run past the bound, not all of them.
	for attempt := 0; ; attempt++ {
		gap := measure(true)
		if gap < gatewaySuffixPaceBound {
			return
		}
		if attempt == 4 {
			t.Fatalf("suffix still %v after an echoed lead; want under %v", gap, gatewaySuffixPaceBound)
		}
	}
}

// Kills: accepting a symbol observed before the suffix was written as its echo.
func TestEscapePair_SymbolBeforeTheSuffixWriteIsNotItsEcho(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	w := mux.armGatewayLeadWait(protocol.SymbolEscape, 0x00)
	mux.observeGatewayLeadEcho(protocol.SymbolEscape, transport.StreamEvent{})
	mux.observeGatewayLeadEcho(0x00, transport.StreamEvent{})
	err := mux.paceGatewaySuffix(w)
	var echoErr *gatewayEscapeEchoError
	if !errors.As(err, &echoErr) || !echoErr.beforeWrite || echoErr.step != gatewayEscapeStepSuffix {
		t.Fatalf("pace = %v; want a suffix failure observed before the write", err)
	}
	if !errors.Is(err, ebuserrors.ErrBusCollision) {
		t.Fatalf("pace = %v; want ErrBusCollision", err)
	}
}

// Kills: a missing echo not counted, attributed to the wrong step, or reclassed
// from a timeout. Each row varies which echoes the adapter delivers.
func TestEscapePair_MissingEchoIsCountedAsTimeoutOfItsStep(t *testing.T) {
	for _, test := range []struct {
		name      string
		echoLead  bool
		wantStep  gatewayEscapeStep
		wantWrote byte
	}{
		{"lead", false, gatewayEscapeStepLead, protocol.SymbolEscape},
		{"suffix", true, gatewayEscapeStepSuffix, 0x01},
	} {
		t.Run(test.name, func(t *testing.T) {
			mux, up, _ := newRawContinuityMux(t)
			grantRawGateway(t, mux, up, 0x10)
			up.setWriteHook(func(symbol byte) {
				if test.echoLead && symbol == protocol.SymbolEscape {
					up.wire(symbol)
				}
			})
			_, err := mux.ActiveTransport().Write([]byte{protocol.SymbolSyn})
			if !errors.Is(err, ebuserrors.ErrTimeout) {
				t.Fatalf("Write = %v; want ErrTimeout", err)
			}
			got := mux.GatewayEscapeEchoSnapshot()
			if got.LastStep != string(test.wantStep) || got.LastWritten != test.wantWrote || got.LastEchoObserved {
				t.Fatalf("snapshot = %+v; want step %s written 0x%02X with no echo", got, test.wantStep, test.wantWrote)
			}
			if got.LeadFailures+got.SuffixFailures != 1 {
				t.Fatalf("snapshot = %+v; want one failure", got)
			}
		})
	}
}

// Kills: a lifecycle boundary counted as a verification failure.
func TestEscapePair_RetiredWaitIsNotAVerificationFailure(t *testing.T) {
	mux, up, _ := newRawContinuityMux(t)
	grantRawGateway(t, mux, up, 0x10)
	mux.gatewaySuffixCommitHook = func() { mux.handleReset() }
	if _, err := mux.ActiveTransport().Write([]byte{protocol.SymbolEscape}); !errors.Is(err, ebuserrors.ErrAdapterReset) {
		t.Fatalf("Write = %v; want ErrAdapterReset", err)
	}
	if got := mux.GatewayEscapeEchoSnapshot(); got != (GatewayEscapeEchoSnapshot{}) {
		t.Fatalf("snapshot = %+v; want none", got)
	}
}

// Kills: a log line per failure, a counter tied to the rate limit, and a line
// after the interval that hides how many failures it stands for.
func TestEscapePair_FailureLogIsRateLimitedAndCountsAreNot(t *testing.T) {
	var logs bytes.Buffer
	mux := New(Config{Protocol: "enh", Logger: log.New(&logs, "", 0)})
	failure := &gatewayEscapeEchoError{
		step: gatewayEscapeStepSuffix, lead: protocol.SymbolEscape, written: 0x01,
		observed: 0xFF, echoObserved: true, cause: ebuserrors.ErrBusCollision,
	}
	start := time.Unix(1000, 0)
	mux.recordGatewayEscapeEchoFailure(failure, start)
	mux.recordGatewayEscapeEchoFailure(failure, start.Add(time.Second))
	mux.recordGatewayEscapeEchoFailure(failure, start.Add(gatewayEscapeEchoLogInterval-time.Nanosecond))
	if got := strings.Count(logs.String(), "\n"); got != 1 {
		t.Fatalf("log lines inside the interval = %d; want 1:\n%s", got, logs.String())
	}
	mux.recordGatewayEscapeEchoFailure(failure, start.Add(gatewayEscapeEchoLogInterval))
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[1], "suppressed=2") {
		t.Fatalf("log = %q; want a second line reporting two suppressed failures", lines)
	}
	if got := mux.GatewayEscapeEchoSnapshot().SuffixFailures; got != 4 {
		t.Fatalf("SuffixFailures = %d; want 4", got)
	}
}
