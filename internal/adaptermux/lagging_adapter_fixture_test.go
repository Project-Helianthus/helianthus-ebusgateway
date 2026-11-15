package adaptermux

import (
	"context"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// This file holds a loopback fake enhanced-protocol adapter and two bus
// clients for the out-of-step tests. The fake answers INIT with RESETTED and
// INFO at once, emits SYN at a set period while nobody owns the bus, and plays
// the target of every telegram: after the sixth request byte of an owner it
// answers ACK, LEN=1, one data byte and CRC. Host commands are START and SEND
// frames; INIT and INFO are always answered at once.

const (
	fakeTargetAddress     byte = 0x08
	fakeForeignInitiator  byte = 0x10
	fakeGatewayInitiator  byte = 0x7F
	fakeExternalInitiator byte = 0x31
	fakeOwnerIdleTimeout       = 50 * time.Millisecond
)

// fakeRequest is the six request bytes a client sends after its grant
// (target, PB, SB, LEN, one data byte, CRC); the adapter's source byte was put
// on the wire by the START.
var fakeRequest = []byte{fakeTargetAddress, 0xB5, 0x11, 0x01, 0x01, 0x77}

// fakeResponse is the target's answer: ACK, LEN, one data byte and CRC.
var fakeResponse = []byte{protocol.SymbolAck, 0x01, 0x42, 0x5A}

// fakeForeignTelegram is a complete telegram of another initiator, with its
// terminal SYN.
var fakeForeignTelegram = []byte{fakeForeignInitiator, fakeTargetAddress, 0xB5, 0x11, 0x01, 0x02, 0x6C,
	protocol.SymbolAck, 0x01, 0x43, 0x5B, protocol.SymbolAck, protocol.SymbolSyn}

type fakeAdapterMode uint8

const (
	fakeAdapterHealthy     fakeAdapterMode = iota
	fakeAdapterOneBehind                   // from host command lagFrom on, command N runs when N+1 arrives
	fakeAdapterStartedLate                 // STARTED is sent startedDelay after START
)

type fakeAdapterConfig struct {
	mode          fakeAdapterMode
	lagFrom       int  // first host command of a connection that is run one behind
	lagSurvives   bool // a new connection lags from its first host command once the lag began
	startedDelay  time.Duration
	synPeriod     time.Duration
	foreignPeriod time.Duration // zero: no foreign telegram
}

type fakeCommand struct {
	start bool
	data  byte
}

// fakeAdapter is the loopback adapter. Every field below mu is guarded by mu;
// a write to the mux happens under mu, so frames never interleave.
type fakeAdapter struct {
	cfg  fakeAdapterConfig
	ln   net.Listener
	done chan struct{}
	wg   sync.WaitGroup

	mu           sync.Mutex
	conns        []net.Conn
	conn         net.Conn // the connection that answered INFO last
	connections  int
	hostCommands int
	lagging      bool
	lagBegan     bool
	held         *fakeCommand
	owned        bool
	ownerActive  time.Time
	requestBytes int
	responded    bool
	startWrites  []time.Time
	strayData    int
}

func newFakeAdapter(t *testing.T, cfg fakeAdapterConfig) *fakeAdapter {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake adapter: %v", err)
	}
	if cfg.synPeriod <= 0 {
		cfg.synPeriod = 15 * time.Millisecond
	}
	a := &fakeAdapter{cfg: cfg, ln: ln, done: make(chan struct{})}
	a.wg.Add(2)
	go a.acceptLoop() // goroutine: ends when the listener is closed at cleanup
	go a.busLoop()    // goroutine: ends when done is closed at cleanup
	t.Cleanup(a.close)
	return a
}

func (a *fakeAdapter) addr() string { return a.ln.Addr().String() }

func (a *fakeAdapter) close() {
	close(a.done)
	_ = a.ln.Close()
	a.mu.Lock()
	for _, c := range a.conns {
		_ = c.Close()
	}
	a.mu.Unlock()
	a.wg.Wait()
}

func (a *fakeAdapter) acceptLoop() {
	defer a.wg.Done()
	for {
		c, err := a.ln.Accept()
		if err != nil {
			return
		}
		a.mu.Lock()
		a.conns = append(a.conns, c)
		a.connections++
		a.mu.Unlock()
		a.wg.Add(1)
		go a.serve(c) // goroutine: ends when the connection is closed by the mux or at cleanup
	}
}

func (a *fakeAdapter) serve(c net.Conn) {
	defer a.wg.Done()
	defer func() { _ = c.Close() }()
	var parser transport.ENHParser
	buf := make([]byte, 64)
	for {
		n, err := c.Read(buf)
		if err != nil {
			a.mu.Lock()
			if a.conn == c {
				a.conn = nil
			}
			a.mu.Unlock()
			return
		}
		for _, b := range buf[:n] {
			msg, ok, perr := parser.Feed(b)
			if perr != nil || !ok {
				continue
			}
			a.handle(c, msg)
		}
	}
}

func (a *fakeAdapter) handle(c net.Conn, msg transport.ENHMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch msg.Command {
	case transport.ENHReqInit:
		writeENH(c, transport.ENHResResetted, 0x01)
	case transport.ENHReqInfo:
		writeENH(c, transport.ENHResInfo, 0x01)
		writeENH(c, transport.ENHResInfo, 0x00)
		a.beginConnectionLocked(c)
	case transport.ENHReqStart:
		a.hostCommandLocked(fakeCommand{start: true, data: msg.Data})
	case transport.ENHReqSend:
		a.hostCommandLocked(fakeCommand{data: msg.Data})
	}
}

// beginConnectionLocked makes c the bus connection once its setup is done.
func (a *fakeAdapter) beginConnectionLocked(c net.Conn) {
	a.conn = c
	a.hostCommands = 0
	a.held = nil
	a.owned = false
	a.lagging = a.cfg.mode == fakeAdapterOneBehind && a.cfg.lagSurvives && a.lagBegan
}

func (a *fakeAdapter) hostCommandLocked(cmd fakeCommand) {
	a.hostCommands++
	if cmd.start {
		a.startWrites = append(a.startWrites, time.Now())
	}
	if a.cfg.mode == fakeAdapterOneBehind && a.cfg.lagFrom > 0 && a.hostCommands >= a.cfg.lagFrom && !a.lagging && !a.lagBegan {
		a.lagging, a.lagBegan = true, true
	}
	if !a.lagging {
		a.executeLocked(cmd)
		return
	}
	previous := a.held
	a.held = &cmd
	if previous != nil {
		a.executeLocked(*previous)
	}
}

func (a *fakeAdapter) executeLocked(cmd fakeCommand) {
	if cmd.start {
		if a.cfg.mode == fakeAdapterStartedLate && a.cfg.startedDelay > 0 {
			a.wg.Add(1)
			go a.lateStarted(cmd.data) // goroutine: ends after the delay or at cleanup
			return
		}
		a.grantLocked(cmd.data)
		return
	}
	if !a.owned {
		a.strayData++
		a.emitLocked(transport.ENHResReceived, cmd.data)
		return
	}
	a.ownerActive = time.Now()
	a.emitLocked(transport.ENHResReceived, cmd.data)
	if cmd.data == protocol.SymbolSyn {
		a.owned = false
		return
	}
	a.requestBytes++
	if a.requestBytes == len(fakeRequest) && !a.responded {
		a.responded = true
		for _, b := range fakeResponse {
			a.emitLocked(transport.ENHResReceived, b)
		}
	}
}

func (a *fakeAdapter) grantLocked(initiator byte) {
	a.emitLocked(transport.ENHResStarted, initiator)
	a.owned, a.ownerActive = true, time.Now()
	a.requestBytes, a.responded = 0, false
}

func (a *fakeAdapter) lateStarted(initiator byte) {
	defer a.wg.Done()
	select {
	case <-time.After(a.cfg.startedDelay):
	case <-a.done:
		return
	}
	a.mu.Lock()
	a.grantLocked(initiator)
	a.mu.Unlock()
}

// busLoop emits SYN while nobody owns the bus and a foreign telegram at the
// foreign period. An owner that stays silent for fakeOwnerIdleTimeout loses
// the bus, as on a real bus whose SYN generator resumes.
func (a *fakeAdapter) busLoop() {
	defer a.wg.Done()
	syn := time.NewTicker(a.cfg.synPeriod)
	defer syn.Stop()
	lastForeign := time.Now()
	for {
		select {
		case <-a.done:
			return
		case <-syn.C:
		}
		a.mu.Lock()
		if a.owned && time.Since(a.ownerActive) > fakeOwnerIdleTimeout {
			a.owned = false
		}
		if !a.owned && a.conn != nil {
			if a.cfg.foreignPeriod > 0 && time.Since(lastForeign) >= a.cfg.foreignPeriod {
				lastForeign = time.Now()
				for _, b := range fakeForeignTelegram {
					a.emitLocked(transport.ENHResReceived, b)
				}
			} else {
				a.emitLocked(transport.ENHResReceived, protocol.SymbolSyn)
			}
		}
		a.mu.Unlock()
	}
}

func (a *fakeAdapter) emitLocked(cmd transport.ENHCommand, data byte) {
	if a.conn != nil {
		writeENH(a.conn, cmd, data)
	}
}

// resetted sends an adapter RESETTED on the bus connection; afterwards the
// adapter answers in step, also on later connections.
func (a *fakeAdapter) resetted() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cfg.mode = fakeAdapterHealthy
	a.lagging, a.lagBegan = false, false
	a.held, a.owned = nil, false
	a.emitLocked(transport.ENHResResetted, 0x01)
}

func writeENH(c net.Conn, cmd transport.ENHCommand, data byte) {
	frame := transport.EncodeENH(cmd, data)
	_ = c.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = c.Write(frame[:])
}

func (a *fakeAdapter) startsSince(since time.Time) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, at := range a.startWrites {
		if !at.Before(since) {
			n++
		}
	}
	return n
}

func (a *fakeAdapter) stats() (starts, connections, stray int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.startWrites), a.connections, a.strayData
}

// lineLog is a concurrency-safe log sink that keeps every line.
type lineLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *lineLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	l.lines = append(l.lines, strings.TrimRight(string(p), "\n"))
	l.mu.Unlock()
	return len(p), nil
}

func (l *lineLog) count(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// laggingMuxOptions configures startLaggingMux.
type laggingMuxOptions struct {
	cfg      Config
	external *externalClientOptions // nil: no proxy listener and no external client
	gateway  bool
	// connectionLost, when set, is installed as the mux's lifecycle owner
	// before Start, so a link reset is delegated as under the eBUS driver.
	connectionLost func()
}

// laggingRun is one mux under test with its clients and observations.
type laggingRun struct {
	mux      *Mux
	log      *lineLog
	gateway  *gatewayBusClient
	external *externalBusClient
	// passiveSyn and passiveForeign count SYN and foreign-initiator symbols
	// delivered to the passive callback.
	passiveSyn     atomic.Uint64
	passiveForeign atomic.Uint64
}

// startLaggingMux starts a mux against the fake adapter with the given
// clients. Every goroutine ends at test cleanup.
func startLaggingMux(t *testing.T, a *fakeAdapter, opts laggingMuxOptions) *laggingRun {
	t.Helper()
	run := &laggingRun{log: &lineLog{}}
	cfg := opts.cfg
	cfg.Protocol, cfg.Network, cfg.Address = "ens", "tcp", a.addr()
	cfg.Logger = log.New(run.log, "", 0)
	cfg.LatencyHistogramReportInterval = -1
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 50 * time.Millisecond
	}
	m := New(cfg)
	m.SetPassiveCallback(func(ev PassiveEvent) {
		if ev.Kind != PassiveEventSymbol {
			return
		}
		switch ev.Symbol {
		case protocol.SymbolSyn:
			run.passiveSyn.Add(1)
		case fakeForeignInitiator:
			run.passiveForeign.Add(1)
		}
	})
	if opts.connectionLost != nil {
		m.SetConnectionLostCallback(opts.connectionLost)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start mux: %v", err)
	}
	t.Cleanup(func() { cancel(); _ = m.Close() })
	run.mux = m
	if opts.external != nil {
		pl, err := NewProxyListener(ctx, m, "127.0.0.1:0", log.New(io.Discard, "", 0))
		if err != nil {
			t.Fatalf("proxy listener: %v", err)
		}
		t.Cleanup(func() { _ = pl.Close() })
		run.external = startExternalBusClient(t, pl.Addr().String(), *opts.external)
	}
	if opts.gateway {
		run.gateway = startGatewayBusClient(t, m)
	}
	return run
}

// gatewayBid is one StartArbitration call of the gateway-side client.
type gatewayBid struct {
	begin, end time.Time
	err        error
}

// gatewayBusClient runs gateway transactions on the mux's active transport:
// StartArbitration, six request bytes with echo, four response bytes, ACK and
// a structural SYN.
type gatewayBusClient struct {
	tr        *activeTransport
	mu        sync.Mutex
	bids      []gatewayBid
	completed int
}

func startGatewayBusClient(t *testing.T, m *Mux) *gatewayBusClient {
	t.Helper()
	c := &gatewayBusClient{tr: m.ActiveTransport().(*activeTransport)}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // goroutine: ends when ctx is cancelled at cleanup
		defer wg.Done()
		for ctx.Err() == nil {
			begin := time.Now()
			err := c.tr.StartArbitration(fakeGatewayInitiator)
			c.mu.Lock()
			c.bids = append(c.bids, gatewayBid{begin: begin, end: time.Now(), err: err})
			c.mu.Unlock()
			if err == nil && c.transaction() {
				c.mu.Lock()
				c.completed++
				c.mu.Unlock()
				continue
			}
			select {
			case <-ctx.Done():
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	t.Cleanup(func() { cancel(); _ = m.Close(); wg.Wait() })
	return c
}

func (c *gatewayBusClient) transaction() bool {
	for _, b := range fakeRequest {
		if !c.writeEcho(b, false) {
			return false
		}
	}
	for _, want := range fakeResponse {
		if got, err := c.tr.ReadByte(); err != nil || got != want {
			return false
		}
	}
	return c.writeEcho(protocol.SymbolAck, false) && c.writeEcho(protocol.SymbolSyn, true)
}

func (c *gatewayBusClient) writeEcho(b byte, structural bool) bool {
	if structural {
		c.tr.SignalNextWriteIsStructuralSyn()
	}
	if _, err := c.tr.Write([]byte{b}); err != nil {
		return false
	}
	got, err := c.tr.ReadByte()
	return err == nil && got == b
}

func (c *gatewayBusClient) completions() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed
}

func (c *gatewayBusClient) bidsSince(since time.Time) []gatewayBid {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []gatewayBid
	for _, bid := range c.bids {
		if !bid.begin.Before(since) {
			out = append(out, bid)
		}
	}
	return out
}

// externalClientOptions shape the external client's behaviour.
type externalClientOptions struct {
	abandon      bool          // never SEND after STARTED
	cancelWithAA bool          // send START 0xAA when a START stays unanswered
	poll         time.Duration // pause between two transactions
}

// externalBusClient is an enhanced-protocol client on the proxy listener: it
// sends START up to four times 130 ms apart and SENDs only when STARTED
// arrives while it waits.
type externalBusClient struct {
	conn      net.Conn
	opts      externalClientOptions
	events    chan transport.ENHMessage
	syn       atomic.Uint64
	foreign   atomic.Uint64
	completed atomic.Uint64
}

func startExternalBusClient(t *testing.T, addr string, opts externalClientOptions) *externalBusClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial proxy listener: %v", err)
	}
	c := &externalBusClient{conn: conn, opts: opts, events: make(chan transport.ENHMessage, 8192)}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.readLoop() }() // goroutine: ends when conn is closed at cleanup
	go func() { defer wg.Done(); c.run(ctx) }()   // goroutine: ends when ctx is cancelled at cleanup
	t.Cleanup(func() { cancel(); _ = conn.Close(); wg.Wait() })
	return c
}

func (c *externalBusClient) readLoop() {
	var parser transport.ENHParser
	buf := make([]byte, 256)
	for {
		n, err := c.conn.Read(buf)
		if err != nil {
			return
		}
		for _, b := range buf[:n] {
			msg, ok, perr := parser.Feed(b)
			if perr != nil || !ok {
				continue
			}
			if msg.Command == transport.ENHResReceived {
				switch msg.Data {
				case protocol.SymbolSyn:
					c.syn.Add(1)
				case fakeForeignInitiator:
					c.foreign.Add(1)
				}
			}
			select {
			case c.events <- msg:
			default:
			}
		}
	}
}

func (c *externalBusClient) send(cmd transport.ENHCommand, data byte) {
	frame := transport.EncodeENH(cmd, data)
	_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = c.conn.Write(frame[:])
}

// next returns the next frame other than a received SYN, or false at the deadline.
func (c *externalBusClient) next(ctx context.Context, deadline time.Time) (transport.ENHMessage, bool) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return transport.ENHMessage{}, false
		case <-timer.C:
			return transport.ENHMessage{}, false
		case msg := <-c.events:
			if msg.Command == transport.ENHResReceived && msg.Data == protocol.SymbolSyn {
				continue
			}
			return msg, true
		}
	}
}

func (c *externalBusClient) drain() {
	for {
		select {
		case <-c.events:
		default:
			return
		}
	}
}

func (c *externalBusClient) run(ctx context.Context) {
	c.send(transport.ENHReqInit, 0x01)
	for ctx.Err() == nil {
		c.drain()
		if c.bid(ctx) {
			if !c.opts.abandon && c.transaction(ctx) {
				c.completed.Add(1)
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(c.opts.poll):
		}
	}
}

// bid sends START up to four times 130 ms apart and reports whether STARTED
// for this client arrived while it waited.
func (c *externalBusClient) bid(ctx context.Context) bool {
	for attempt := 0; attempt < 4; attempt++ {
		c.send(transport.ENHReqStart, fakeExternalInitiator)
		deadline := time.Now().Add(130 * time.Millisecond)
		for {
			msg, ok := c.next(ctx, deadline)
			if !ok {
				break
			}
			if msg.Command == transport.ENHResStarted && msg.Data == fakeExternalInitiator {
				return true
			}
			if msg.Command == transport.ENHResFailed || msg.Command == transport.ENHResResetted {
				return false
			}
		}
		if c.opts.cancelWithAA {
			c.send(transport.ENHReqStart, protocol.SymbolSyn)
		}
	}
	return false
}

func (c *externalBusClient) transaction(ctx context.Context) bool {
	for _, b := range fakeRequest {
		if !c.sendEcho(ctx, b) {
			return false
		}
	}
	for _, want := range fakeResponse {
		if !c.expectReceived(ctx, want) {
			return false
		}
	}
	return c.sendEcho(ctx, protocol.SymbolAck) && c.sendEcho(ctx, protocol.SymbolSyn)
}

func (c *externalBusClient) sendEcho(ctx context.Context, b byte) bool {
	c.send(transport.ENHReqSend, b)
	return c.expectReceived(ctx, b)
}

func (c *externalBusClient) expectReceived(ctx context.Context, want byte) bool {
	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return false
		case msg := <-c.events:
			if msg.Command != transport.ENHResReceived {
				return false
			}
			if msg.Data == want {
				return true
			}
			if msg.Data == protocol.SymbolSyn {
				return false
			}
		case <-time.After(time.Until(deadline)):
			return false
		}
	}
}
