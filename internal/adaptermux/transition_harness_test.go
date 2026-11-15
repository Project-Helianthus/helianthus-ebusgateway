package adaptermux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// The transition harness drives the real AdapterMux handlers (readLoop,
// onLogicalReceived, onSYNLocked through a wire SYN, tryGrantAndStart,
// RemoveSession, handleReset) one input at a time and observes two things:
//
//   - transitionState, the mux state the transition handlers read and write;
//   - the ordered effect log: adapter calls and passive events in the order the
//     handlers made them, then the active path, every session stream and every
//     START result, each in its own FIFO order.
//
// Every timer the handlers arm is configured to one hour, so nothing fires
// unless a row fires it explicitly. The read loop and the send loop run for
// the whole harness lifetime and end through context cancellation.

// transitionTimerHorizon keeps every mux timer and grace period out of reach
// of a test run, so a row is decided by its input alone.
const transitionTimerHorizon = time.Hour

// transitionWaitBudget bounds every wait for an asynchronous handler step. It
// only detects a hang; a row never depends on how long a step takes.
const transitionWaitBudget = 10 * time.Second

// Source addresses used by the harness. Each is an initiator address; the
// foreign source belongs to no local session.
const (
	transitionSessionSource byte = 0x31 // session 1
	transitionOtherSource   byte = 0x71 // session 2
	transitionGatewaySource byte = 0x7F // gateway active path
	transitionForeignSource byte = 0x10 // a bus participant outside the mux
	transitionDataSymbol    byte = 0x52 // a data symbol that is no initiator address
	transitionGatewayData   byte = 0x15 // first request symbol the gateway writes, no initiator address
)

var errTransitionUpstreamClosed = errors.New("transition harness: upstream closed")

// transitionUpstreamMode selects the arbitration and read capabilities of the
// fake upstream.
type transitionUpstreamMode uint8

const (
	// transitionUpstreamAsync offers the non-blocking RequestStart.
	transitionUpstreamAsync transitionUpstreamMode = iota
	// transitionUpstreamBlocking offers only the blocking StartArbitration.
	transitionUpstreamBlocking
	// transitionUpstreamRawView offers RequestStart and the raw-symbol view.
	transitionUpstreamRawView
)

// transitionItem is one scripted upstream item: a logical or control event,
// optionally the raw wire symbol that completed it, or a read error.
type transitionItem struct {
	event  transport.StreamEvent
	raw    byte
	hasRaw bool
	err    error
}

// transitionRecorder is the sequenced part of the effect log. Adapter calls
// and passive events are recorded synchronously by the goroutine that makes
// them, so their relative order is the order the handlers produced them.
type transitionRecorder struct {
	mu      sync.Mutex
	entries []string
}

func (r *transitionRecorder) add(format string, args ...any) {
	r.mu.Lock()
	r.entries = append(r.entries, fmt.Sprintf(format, args...))
	r.mu.Unlock()
}

func (r *transitionRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.entries
	r.entries = nil
	return out
}

// transitionUpstream is the scripted adapter. ReadEvent first announces on
// ready that the read loop has finished the previous item, then waits for the
// next scripted item.
type transitionUpstream struct {
	rec       *transitionRecorder
	items     chan transitionItem
	ready     chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func newTransitionUpstream(rec *transitionRecorder) *transitionUpstream {
	return &transitionUpstream{
		rec:    rec,
		items:  make(chan transitionItem),
		ready:  make(chan struct{}),
		closed: make(chan struct{}),
	}
}

func (u *transitionUpstream) next() (transitionItem, error) {
	select {
	case u.ready <- struct{}{}:
	case <-u.closed:
		return transitionItem{}, errTransitionUpstreamClosed
	}
	select {
	case item := <-u.items:
		return item, nil
	case <-u.closed:
		return transitionItem{}, errTransitionUpstreamClosed
	}
}

func (u *transitionUpstream) ReadEvent() (transport.StreamEvent, error) {
	item, err := u.next()
	if err != nil {
		return transport.StreamEvent{}, err
	}
	if item.err != nil {
		return transport.StreamEvent{}, item.err
	}
	return item.event, nil
}

func (u *transitionUpstream) ReadByte() (byte, error) {
	return 0, errors.New("transition harness: ReadByte is not scripted")
}

func (u *transitionUpstream) Write(p []byte) (int, error) {
	u.rec.add("adapter: Write % X", p)
	return len(p), nil
}

func (u *transitionUpstream) Close() error {
	u.closeOnce.Do(func() { close(u.closed) })
	return nil
}

// transitionAsyncUpstream answers START asynchronously through the stream.
type transitionAsyncUpstream struct {
	*transitionUpstream
}

func (u transitionAsyncUpstream) RequestStart(initiator byte) error {
	u.rec.add("adapter: RequestStart 0x%02X", initiator)
	return nil
}

// transitionRawViewUpstream also supplies the raw wire symbol of every item.
type transitionRawViewUpstream struct {
	transitionAsyncUpstream
}

func (u transitionRawViewUpstream) ReadEventWithRawSymbol() (transport.StreamEvent, byte, bool, error) {
	item, err := u.next()
	if err != nil {
		return transport.StreamEvent{}, 0, false, err
	}
	if item.err != nil {
		return transport.StreamEvent{}, 0, false, item.err
	}
	return item.event, item.raw, item.hasRaw, nil
}

// transitionBlockingUpstream holds StartArbitration until the row releases it.
type transitionBlockingUpstream struct {
	*transitionUpstream
	calls   chan byte
	release chan error
}

func (u transitionBlockingUpstream) StartArbitration(initiator byte) error {
	u.rec.add("adapter: StartArbitration 0x%02X", initiator)
	select {
	case u.calls <- initiator:
	case <-u.closed:
		return errTransitionUpstreamClosed
	}
	select {
	case err := <-u.release:
		return err
	case <-u.closed:
		return errTransitionUpstreamClosed
	}
}

// transitionConn is the connection of a recording session. The harness reads
// the session stream from its send channel; no writer goroutine runs.
type transitionConn struct{}

func (transitionConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (transitionConn) Write(p []byte) (int, error)      { return len(p), nil }
func (transitionConn) Close() error                     { return nil }
func (transitionConn) LocalAddr() net.Addr              { return nil }
func (transitionConn) RemoteAddr() net.Addr             { return nil }
func (transitionConn) SetDeadline(time.Time) error      { return nil }
func (transitionConn) SetReadDeadline(time.Time) error  { return nil }
func (transitionConn) SetWriteDeadline(time.Time) error { return nil }

// transitionBid is one START waiter and the results it received so far.
type transitionBid struct {
	label   string
	ch      <-chan startResult
	results []startResult
}

// transitionHarness owns one Mux built with New and never started.
type transitionHarness struct {
	t        testing.TB
	m        *Mux
	mode     transitionUpstreamMode
	rec      *transitionRecorder
	up       *transitionUpstream
	blocking transitionBlockingUpstream
	sessions []*session
	bids     []*transitionBid
	cancel   context.CancelFunc
	stopped  bool
	// source is the address the fixture selected, used by the STARTED input
	// that names the selected source.
	source byte
	// selected labels the waiter of the selection the fixture made.
	selected string
	// deadline is the START deadline timer of the selection the fixture made.
	deadline *time.Timer
}

func newTransitionHarness(t testing.TB, mode transitionUpstreamMode) *transitionHarness {
	t.Helper()
	m := New(Config{
		Protocol:                    "enh",
		Network:                     "tcp",
		Address:                     "127.0.0.1:0",
		ReadTimeout:                 transitionTimerHorizon,
		MaxOwnershipDuration:        transitionTimerHorizon,
		GatewayMaxOwnershipDuration: transitionTimerHorizon,
		IdleReleaseGrace:            transitionTimerHorizon,
		ExternalSessionSYNGrace:     transitionTimerHorizon,
		StartDeadline:               transitionTimerHorizon,
		BlackholeThreshold:          transitionTimerHorizon,
		Logger:                      log.New(io.Discard, "", 0),
	})
	h := &transitionHarness{t: t, m: m, mode: mode, rec: &transitionRecorder{}, source: transitionSessionSource}
	h.up = newTransitionUpstream(h.rec)
	var upstream transport.RawTransport
	switch mode {
	case transitionUpstreamAsync:
		upstream = transitionAsyncUpstream{h.up}
	case transitionUpstreamRawView:
		upstream = transitionRawViewUpstream{transitionAsyncUpstream{h.up}}
	case transitionUpstreamBlocking:
		h.blocking = transitionBlockingUpstream{transitionUpstream: h.up, calls: make(chan byte), release: make(chan error)}
		upstream = h.blocking
	}
	m.externalAbandonAfter = func(d time.Duration, _ func()) {
		h.rec.add("abandon watchdog armed for %v", d)
	}
	m.SetPassiveCallback(func(ev PassiveEvent) {
		h.rec.add("%s", transitionPassiveText(ev))
	})
	ctx, cancel := context.WithCancel(context.Background())
	m.ctx, m.cancel = ctx, cancel
	h.cancel = cancel
	m.connMu.Lock()
	m.upstream = upstream
	m.connMu.Unlock()
	for _, id := range []uint64{1, 2} {
		sess := &session{
			id:     id,
			conn:   transitionConn{},
			mux:    m,
			sendCh: make(chan sessionFrame, 256),
			done:   make(chan struct{}),
		}
		m.sessionsMu.Lock()
		m.sessions[id] = sess
		m.sessionsMu.Unlock()
		h.sessions = append(h.sessions, sess)
	}
	m.wg.Add(2)
	go m.readLoop()
	go m.sendLoop()
	h.awaitReadLoopIdle()
	t.Cleanup(h.stop)
	return h
}

// stop cancels the context, closes the upstream so a blocked read or START
// call returns, and waits for the mux goroutines while draining every channel
// they might still send on. It then stops the mux timers.
func (h *transitionHarness) stop() {
	if h.stopped {
		return
	}
	h.stopped = true
	h.cancel()
	_ = h.up.Close()
	done := make(chan struct{})
	go func() {
		h.m.wg.Wait()
		close(done)
	}()
	deadline := time.After(transitionWaitBudget)
	for {
		select {
		case <-done:
			h.stopTimers()
			return
		case <-deadline:
			h.t.Errorf("transition harness: mux goroutines did not stop within %v", transitionWaitBudget)
			h.stopTimers()
			return
		case <-time.After(time.Millisecond):
			for _, bid := range h.bids {
				select {
				case <-bid.ch:
				default:
				}
			}
			for _, sess := range h.sessions {
				select {
				case <-sess.sendCh:
				default:
				}
			}
		}
	}
}

// stopTimers stops the START deadline of a selection still in flight and the
// absorb reset timer, and clears the absorb timer reference, so no mux timer
// outlives the harness.
func (h *transitionHarness) stopTimers() {
	h.m.stateMu.Lock()
	defer h.m.stateMu.Unlock()
	if h.m.pendingStart != nil && h.m.pendingStart.deadline != nil {
		h.m.pendingStart.deadline.Stop()
	}
	if h.deadline != nil {
		h.deadline.Stop()
	}
	if h.m.pendingAbsorbResetTimer != nil {
		h.m.pendingAbsorbResetTimer.Stop()
		h.m.pendingAbsorbResetTimer = nil
	}
}

func (h *transitionHarness) awaitReadLoopIdle() {
	h.t.Helper()
	select {
	case <-h.up.ready:
	case <-time.After(transitionWaitBudget):
		h.t.Fatalf("transition harness: read loop did not return to the upstream within %v", transitionWaitBudget)
	}
}

// feed hands one item to the read loop and returns once the loop has
// finished processing it and asks the upstream for the next one.
func (h *transitionHarness) feed(item transitionItem) {
	h.t.Helper()
	select {
	case h.up.items <- item:
	case <-time.After(transitionWaitBudget):
		h.t.Fatalf("transition harness: read loop did not accept an item within %v", transitionWaitBudget)
	}
	h.awaitReadLoopIdle()
}

func (h *transitionHarness) feedStarted(source byte) {
	h.t.Helper()
	h.feed(transitionItem{event: transport.StreamEvent{Kind: transport.StreamEventStarted, Data: source}})
}

func (h *transitionHarness) feedFailed(source byte) {
	h.t.Helper()
	h.feed(transitionItem{event: transport.StreamEvent{Kind: transport.StreamEventFailed, Data: source}})
}

func (h *transitionHarness) feedReset() {
	h.t.Helper()
	h.feed(transitionItem{event: transport.StreamEvent{Kind: transport.StreamEventReset}})
}

// feedWireSyn hands a wire SYN to the read loop, with its raw symbol when the
// upstream supplies the raw view.
func (h *transitionHarness) feedWireSyn() {
	h.t.Helper()
	item := transitionItem{event: transport.StreamEvent{Kind: transport.StreamEventWireSyn, Byte: protocol.SymbolSyn}}
	if h.mode == transitionUpstreamRawView {
		item.raw, item.hasRaw = protocol.SymbolSyn, true
	}
	h.feed(item)
}

// syn applies a wire SYN through onLogicalReceived, which runs onSYNLocked.
func (h *transitionHarness) syn() {
	h.m.onLogicalReceived(protocol.SymbolSyn, false, true)
}

// symbol applies one received logical symbol through onLogicalReceived.
func (h *transitionHarness) symbol(b byte) {
	h.m.onLogicalReceived(b, false, true)
}

// bid submits a START through the mux and keeps its waiter.
func (h *transitionHarness) bid(sessionID uint64, initiator byte) {
	ch := h.m.requestStartForSession(sessionID, initiator)
	h.bids = append(h.bids, &transitionBid{label: fmt.Sprintf("%s 0x%02X", transitionParty(sessionID), initiator), ch: ch})
}

// awaitResult waits for the next result of the waiter with the given label.
func (h *transitionHarness) awaitResult(label string) {
	h.t.Helper()
	for i := len(h.bids) - 1; i >= 0; i-- {
		bid := h.bids[i]
		if bid.label != label {
			continue
		}
		select {
		case res := <-bid.ch:
			bid.results = append(bid.results, res)
		case <-time.After(transitionWaitBudget):
			h.t.Fatalf("transition harness: no START result for %s within %v", label, transitionWaitBudget)
		}
		return
	}
	h.t.Fatalf("transition harness: no waiter %s", label)
}

// captureDeadline keeps the START deadline timer of the current selection.
func (h *transitionHarness) captureDeadline() {
	h.m.stateMu.Lock()
	defer h.m.stateMu.Unlock()
	if h.m.pendingStart == nil {
		h.t.Fatalf("transition harness: no START selected")
	}
	h.deadline = h.m.pendingStart.deadline
}

// fireDeadline runs the captured START deadline callback now and waits for the
// result it delivers to the selected waiter.
func (h *transitionHarness) fireDeadline(label string) {
	h.t.Helper()
	if h.deadline == nil {
		h.t.Fatalf("transition harness: no START deadline captured")
	}
	h.deadline.Reset(0)
	h.awaitResult(label)
}

// gatewayWrite sends one gateway byte through the real send loop and waits
// for the write result.
func (h *transitionHarness) gatewayWrite(b byte, structural bool) {
	h.t.Helper()
	result := make(chan error, 1)
	select {
	case h.m.activeSendCh <- sendRequest{sessionID: gatewaySessionID, data: b, structural: structural, result: result}:
	case <-time.After(transitionWaitBudget):
		h.t.Fatalf("transition harness: send loop did not accept a gateway write within %v", transitionWaitBudget)
	}
	select {
	case err := <-result:
		if err != nil {
			h.t.Fatalf("transition harness: gateway write 0x%02X: %v", b, err)
		}
	case <-time.After(transitionWaitBudget):
		h.t.Fatalf("transition harness: gateway write 0x%02X did not complete within %v", b, transitionWaitBudget)
	}
}

// awaitStartCall waits until the blocking upstream received StartArbitration.
func (h *transitionHarness) awaitStartCall() {
	h.t.Helper()
	select {
	case <-h.blocking.calls:
	case <-time.After(transitionWaitBudget):
		h.t.Fatalf("transition harness: StartArbitration was not called within %v", transitionWaitBudget)
	}
}

// releaseStartCall returns err from the held StartArbitration call, waits for
// the selected waiter's result and then for the call's goroutine to clear the
// blocking-call barrier.
func (h *transitionHarness) releaseStartCall(err error, label string) {
	h.t.Helper()
	select {
	case h.blocking.release <- err:
	case <-time.After(transitionWaitBudget):
		h.t.Fatalf("transition harness: StartArbitration was not waiting for its result")
	}
	h.awaitResult(label)
	deadline := time.Now().Add(transitionWaitBudget)
	for {
		h.m.stateMu.Lock()
		active := h.m.blockingArbActive
		h.m.stateMu.Unlock()
		if !active {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("transition harness: blocking START call did not finish within %v", transitionWaitBudget)
		}
		time.Sleep(time.Millisecond)
	}
}

// discardEffects drops the effects of fixture steps.
func (h *transitionHarness) discardEffects() {
	_ = h.effects()
}

// effects returns the ordered effect log since the previous call: adapter
// calls and passive events in call order, then the active path, the streams
// of session 1 and session 2 and the START results in waiter order.
func (h *transitionHarness) effects() []string {
	out := h.rec.take()
	for {
		select {
		case ev := <-h.m.activeCh:
			out = append(out, transitionActiveText(ev))
			continue
		default:
		}
		break
	}
	for _, sess := range h.sessions {
		for {
			select {
			case frame := <-sess.sendCh:
				out = append(out, fmt.Sprintf("session %d: %s", sess.id, transitionFrameText(frame)))
				continue
			default:
			}
			break
		}
	}
	for _, bid := range h.bids {
		for {
			select {
			case res := <-bid.ch:
				bid.results = append(bid.results, res)
				continue
			default:
			}
			break
		}
		for _, res := range bid.results {
			out = append(out, fmt.Sprintf("result %s: %s", bid.label, transitionResultText(res)))
		}
		bid.results = nil
	}
	if out == nil {
		out = []string{}
	}
	return out
}

// transitionState is the mux state the transition handlers decide on. The
// zero value is an idle mux: no owner, no selection, no barrier, wire phase
// idle and no queued bid.
type transitionState struct {
	owner            string // "", "gateway" or "session N"
	gatewayTxn       bool   // gateway transaction active
	start            string // selected START awaiting its adapter result: "party 0xNN"
	startResponse    bool   // the adapter result of that START arrived
	startBlocking    bool   // that START uses the blocking StartArbitration call
	blockingCall     bool   // a blocking StartArbitration call is outstanding
	absorb           int    // stale arbitration results still to absorb
	regrantFence     bool   // no selection before the next SYN
	retiredSelection bool   // late results of a retired selection are recognized
	recovery         string // arbitration recovery kind, "" when none
	resyncOwed       uint8  // wire SYNs still owed to the gateway active path
	phase            string // wire phase, "" when idle
	localLosers      string // waiters told they lost locally, "party, party"
	bids             string // queued bids, then a deferred gateway loser
}

func (s transitionState) String() string {
	var parts []string
	text := func(name, value string) {
		if value != "" {
			parts = append(parts, fmt.Sprintf("%s: %q", name, value))
		}
	}
	flag := func(name string, value bool) {
		if value {
			parts = append(parts, name+": true")
		}
	}
	count := func(name string, value int) {
		if value != 0 {
			parts = append(parts, fmt.Sprintf("%s: %d", name, value))
		}
	}
	text("owner", s.owner)
	flag("gatewayTxn", s.gatewayTxn)
	text("start", s.start)
	flag("startResponse", s.startResponse)
	flag("startBlocking", s.startBlocking)
	flag("blockingCall", s.blockingCall)
	count("absorb", s.absorb)
	flag("regrantFence", s.regrantFence)
	flag("retiredSelection", s.retiredSelection)
	text("recovery", s.recovery)
	count("resyncOwed", int(s.resyncOwed))
	text("phase", s.phase)
	text("localLosers", s.localLosers)
	text("bids", s.bids)
	return "transitionState{" + strings.Join(parts, ", ") + "}"
}

// state reads transitionState under the state lock.
func (h *transitionHarness) state() transitionState {
	m := h.m
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	var s transitionState
	if id, _, owned := m.arb.owner(); owned {
		s.owner = transitionParty(id)
	}
	s.gatewayTxn = m.gatewayTxnActive
	if p := m.pendingStart; p != nil {
		s.start = fmt.Sprintf("%s 0x%02X", transitionParty(p.sessionID), p.initiator)
		s.startResponse = p.responseReceived
		s.startBlocking = p.blockingArb
	}
	s.blockingCall = m.blockingArbActive
	s.absorb = m.pendingStartAbsorb
	s.regrantFence = m.deferRegrantUntilNextSyn
	s.retiredSelection = m.retiredSelection
	s.recovery = transitionRecoveryText(m.arbitrationRecovery)
	s.resyncOwed = m.collisionResyncOwed
	s.phase = transitionPhaseText(m.phase.phase)
	losers := make([]string, 0, len(m.localLosers))
	for _, req := range m.localLosers {
		losers = append(losers, transitionParty(req.sessionID))
	}
	s.localLosers = strings.Join(losers, ", ")
	m.arb.mu.Lock()
	var bids []string
	if req := m.arb.pendingGateway; req != nil {
		bids = append(bids, fmt.Sprintf("%s 0x%02X", transitionParty(req.sessionID), req.initiator))
	}
	for _, req := range m.arb.pendingExternal {
		bids = append(bids, fmt.Sprintf("%s 0x%02X", transitionParty(req.sessionID), req.initiator))
	}
	if req := m.arb.deferredGatewayLoser; req != nil {
		bids = append(bids, fmt.Sprintf("deferred %s 0x%02X", transitionParty(req.sessionID), req.initiator))
	}
	m.arb.mu.Unlock()
	s.bids = strings.Join(bids, ", ")
	return s
}

func transitionParty(sessionID uint64) string {
	if sessionID == gatewaySessionID {
		return "gateway"
	}
	return fmt.Sprintf("session %d", sessionID)
}

func transitionRecoveryText(r arbitrationRecovery) string {
	switch r {
	case arbitrationRecoveryNone:
		return ""
	case arbitrationRecoveryAtSYN:
		return "at_syn"
	case arbitrationRecoveryReset:
		return "reset"
	case arbitrationRecoveryForeign:
		return "foreign"
	case arbitrationRecoveryForeignAfterAA:
		return "foreign_after_syn_symbol"
	default:
		return fmt.Sprintf("recovery(%d)", r)
	}
}

func transitionPhaseText(p wirePhase) string {
	switch p {
	case wirePhaseIdle:
		return ""
	case wirePhaseCollectRequest:
		return "collect_request"
	case wirePhaseWaitCmdAck:
		return "wait_command_ack"
	case wirePhaseWaitResponseLen:
		return "wait_response_length"
	case wirePhaseWaitResponseBody:
		return "wait_response_body"
	case wirePhaseWaitResponseAck:
		return "wait_response_ack"
	case wirePhaseWaitTerminalSyn:
		return "wait_terminal_syn"
	default:
		return fmt.Sprintf("phase(%d)", p)
	}
}

func transitionPassiveText(ev PassiveEvent) string {
	switch ev.Kind {
	case PassiveEventSymbol:
		if ev.WasEscaped {
			return fmt.Sprintf("passive: symbol 0x%02X escaped", ev.Symbol)
		}
		return fmt.Sprintf("passive: symbol 0x%02X", ev.Symbol)
	case PassiveEventConnected:
		return "passive: connected"
	case PassiveEventDisconnected:
		return "passive: disconnected"
	case PassiveEventReset:
		return "passive: reset"
	default:
		return fmt.Sprintf("passive: kind %d", ev.Kind)
	}
}

func transitionActiveText(ev activeEvent) string {
	if ev.kind == activeEventError {
		return fmt.Sprintf("active: error %v", ev.err)
	}
	if ev.wasEscaped {
		return fmt.Sprintf("active: 0x%02X escaped", ev.b)
	}
	return fmt.Sprintf("active: 0x%02X", ev.b)
}

func transitionFrameText(frame sessionFrame) string {
	switch frame.kind {
	case sessionFrameReceived:
		return fmt.Sprintf("RECEIVED 0x%02X", frame.payload)
	case sessionFrameStarted:
		return fmt.Sprintf("STARTED 0x%02X", frame.payload)
	case sessionFrameFailed:
		return fmt.Sprintf("FAILED 0x%02X", frame.payload)
	case sessionFrameResetted:
		return fmt.Sprintf("RESETTED 0x%02X", frame.payload)
	case sessionFrameErrorEBUS:
		return "ERROR_EBUS"
	case sessionFrameErrorHost:
		return "ERROR_HOST"
	case sessionFrameInfo:
		return fmt.Sprintf("INFO 0x%02X", frame.payload)
	default:
		return fmt.Sprintf("frame %d 0x%02X", frame.kind, frame.payload)
	}
}

func transitionResultText(res startResult) string {
	var parts []string
	if res.granted {
		parts = append(parts, "granted")
	}
	if res.cancelled {
		parts = append(parts, "cancelled")
	}
	parts = append(parts, fmt.Sprintf("source 0x%02X", res.initiator))
	if res.startedQueued {
		parts = append(parts, "started queued")
	}
	if res.failedQueued {
		parts = append(parts, "failed queued")
	}
	if res.err != nil {
		parts = append(parts, "error "+res.err.Error())
	}
	return strings.Join(parts, ", ")
}
