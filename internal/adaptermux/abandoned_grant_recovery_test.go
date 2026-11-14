package adaptermux

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// externalGrantRecoveryModeFixture is t02LiveTelegramFixture with the
// classifier mode as a parameter (ModeOff leaves the classifier nil, as New
// does). It also returns the event feeder so a test can deliver a raw-only
// item. The mode is fixed before readLoop starts.
func externalGrantRecoveryModeFixture(t *testing.T, mode v8classifier.Mode, raw, wrote bool) (m *Mux, mock *p3MockTransport, plain func(...byte), send func(upstreamItem)) {
	t.Helper()
	f := newExternalGrantRecoveryFixture(t, externalGrantRecoveryOptions{mode: mode, raw: raw, wrote: wrote})
	return f.m, f.mock, f.plain, f.send
}

// externalGrantRecoveryOptions configure newExternalGrantRecoveryFixture. Every
// option is applied before readLoop starts.
type externalGrantRecoveryOptions struct {
	mode       v8classifier.Mode
	raw, wrote bool
	// idleGrace, when set, replaces ExternalSessionSYNGrace so that the
	// external idle-gap release cannot fire however slowly the test runs.
	idleGrace time.Duration
	logger    *log.Logger // nil: discard
	// owner is the session granted at setup and initiator its address.
	owner     uint64
	initiator byte
}

// externalGrantRecoveryFixture is a mux with three sessions whose read loop
// processes one fed item at a time, after the grant of the configured owner.
type externalGrantRecoveryFixture struct {
	m       *Mux
	mock    *p3MockTransport
	plain   func(...byte)
	send    func(upstreamItem)
	passive func() []byte
}

func newExternalGrantRecoveryFixture(t *testing.T, opts externalGrantRecoveryOptions) *externalGrantRecoveryFixture {
	t.Helper()
	if opts.owner == 0 && opts.initiator == 0 {
		opts.owner, opts.initiator = 1, 0x31
	}
	m, mock := newT02Mux(t)
	m.logger = log.New(io.Discard, "", 0)
	if opts.logger != nil {
		m.logger = opts.logger
	}
	if opts.idleGrace > 0 {
		m.cfg.ExternalSessionSYNGrace = opts.idleGrace
	}
	var passiveMu sync.Mutex
	var seen []byte
	m.SetPassiveCallback(func(ev PassiveEvent) {
		passiveMu.Lock()
		seen = append(seen, ev.Symbol)
		passiveMu.Unlock()
	})
	m.v8 = nil
	if opts.mode != v8classifier.ModeOff {
		m.v8 = v8classifier.New(opts.mode)
	}
	tr := &t02ExitTransport{p3MockTransport: mock, steps: make(chan t02ExitStep)}
	mock.readDone = m.ctx.Done()
	m.upstream = tr
	if opts.raw {
		m.upstream = &t02RawExitTransport{tr}
	}
	m.wg.Add(1)
	go m.readLoop()
	t.Cleanup(func() { m.cancel(); m.wg.Wait() })
	f := &externalGrantRecoveryFixture{m: m, mock: mock}
	f.send = func(item upstreamItem) {
		t.Helper()
		step := t02ExitStep{item: item, processed: make(chan struct{})}
		select {
		case tr.steps <- step:
		case <-time.After(time.Second):
			t.Fatal("readLoop not reading")
		}
		select {
		case <-step.processed:
		case <-time.After(time.Second):
			t.Fatal("readLoop did not finish dispatch")
		}
	}
	event := func(ev transport.StreamEvent) {
		t.Helper()
		item := upstreamItem{event: ev}
		if opts.raw && (ev.Kind == transport.StreamEventByte || ev.Kind == transport.StreamEventWireSyn) {
			item.hasRaw, item.rawSymbol = true, ev.Byte
		}
		f.send(item)
	}
	f.plain = func(bs ...byte) {
		t.Helper()
		for _, b := range bs {
			event(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b})
		}
	}
	f.passive = func() []byte {
		passiveMu.Lock()
		defer passiveMu.Unlock()
		out := seen
		seen = nil
		return out
	}
	srv, cli := net.Pipe()
	t.Cleanup(func() { _ = srv.Close(); _ = cli.Close() })
	m.sessions[1].conn = srv

	ch := m.requestStartForSession(opts.owner, opts.initiator)
	f.plain(protocol.SymbolSyn)
	event(transport.StreamEvent{Kind: transport.StreamEventStarted, Data: opts.initiator})
	if !m.arb.isOwner(opts.owner) {
		t.Fatalf("session %d did not become the owner", opts.owner)
	}
	if opts.owner == gatewaySessionID {
		if res := <-ch; !res.granted {
			t.Fatalf("gateway grant result %+v", res)
		}
	} else {
		m.sessions[opts.owner].grantDelivered.Store(true)
	}
	if opts.wrote {
		m.stateMu.Lock()
		m.phase.markOwnerWrite()
		m.stateMu.Unlock()
	}
	for _, id := range []uint64{1, 2, 3} {
		externalGrantRecoveryDrain(m.sessions[id])
	}
	f.passive()
	return f
}

// externalGrantRecoveryCaptureWatchdog arms the abandon watchdog for session
// 1's current grant and returns its callback instead of scheduling it.
func externalGrantRecoveryCaptureWatchdog(t *testing.T, m *Mux) func() {
	t.Helper()
	var fire func()
	m.externalAbandonAfter = func(d time.Duration, f func()) {
		if d != externalAbandonDeadline {
			t.Errorf("watchdog armed for %v, want %v", d, externalAbandonDeadline)
		}
		fire = f
	}
	m.armExternalAbandonWatchdog(m.sessions[1], uint32(m.sessions[1].grantGeneration.Load()))
	if fire == nil {
		t.Fatal("abandon watchdog was not armed")
	}
	return fire
}

// externalGrantRecoveryDrain empties a session's queue and returns the
// symbols it carried as RECEIVED frames.
func externalGrantRecoveryDrain(s *session) []byte {
	var got []byte
	for {
		select {
		case f := <-s.sendCh:
			if f.kind == sessionFrameReceived {
				got = append(got, f.payload)
			}
		default:
			return got
		}
	}
}

func externalGrantRecoveryCount(bs []byte, want byte) int {
	n := 0
	for _, b := range bs {
		if b == want {
			n++
		}
	}
	return n
}

// externalGrantRecoveryNewStarts returns the START requests written after
// the first `before` ones.
func externalGrantRecoveryNewStarts(mock *p3MockTransport, before int) []byte {
	got := mock.getStartRequests()
	if len(got) < before {
		return nil
	}
	return append([]byte(nil), got[before:]...)
}

func externalGrantRecoveryViews() []bool { return []bool{false, true} }

func externalGrantRecoveryViewName(raw bool) string {
	if raw {
		return "raw"
	}
	return "logical"
}

// externalGrantRecoveryRequireRecovered feeds count SYNs and asserts that the
// waiting bid of session 2 was started and that idle SYN reaches session 2.
func externalGrantRecoveryRequireRecovered(t *testing.T, m *Mux, mock *p3MockTransport, plain func(...byte), before, count int) {
	t.Helper()
	externalGrantRecoveryRequireRecoveredFor(t, m, mock, plain, 2, 0x03, before, count)
	if m.arb.isOwner(1) {
		t.Fatal("session 1 still owns the bus")
	}
}

// externalGrantRecoveryRequireRecoveredFor feeds count SYNs and asserts that
// the waiting bid of session id was started with initiator and that idle SYN
// reaches that session.
func externalGrantRecoveryRequireRecoveredFor(t *testing.T, m *Mux, mock *p3MockTransport, plain func(...byte), id uint64, initiator byte, before, count int) {
	t.Helper()
	var delivered []byte
	for i := 0; i < count; i++ {
		plain(protocol.SymbolSyn)
		delivered = append(delivered, externalGrantRecoveryDrain(m.sessions[id])...)
	}
	if starts := externalGrantRecoveryNewStarts(mock, before); externalGrantRecoveryCount(starts, initiator) != 1 {
		t.Fatalf("waiting bid not started within %d SYNs: new START requests %x", count, starts)
	}
	if externalGrantRecoveryCount(delivered, protocol.SymbolSyn) == 0 {
		t.Fatalf("no SYN of %d reached the waiting session: %x", count, delivered)
	}
}

// (a) A grant its client never used: the abandon watchdog releases it, and
// with the classifier enforcing, idle SYN and the waiting bid recover within
// the following SYNs instead of waiting for other bus traffic.
func TestExternalGrantRecoveryAbandonedGrantRecoversInEnforceMode(t *testing.T) {
	for _, raw := range externalGrantRecoveryViews() {
		t.Run(externalGrantRecoveryViewName(raw), func(t *testing.T) {
			m, mock, plain, _ := t02LiveTelegramFixture(t, raw, false)
			fire := externalGrantRecoveryCaptureWatchdog(t, m)
			fire()
			if m.arb.isOwner(1) {
				t.Fatal("watchdog did not release the unused grant")
			}
			before := len(mock.getStartRequests())
			m.requestStartForSession(2, 0x03)
			externalGrantRecoveryRequireRecovered(t, m, mock, plain, before, 3)
		})
	}
}

// (b) Selection-timing pin: with the classifier off or in shadow mode the
// waiting bid is selected at the first SYN after the watchdog release.
func TestExternalGrantRecoveryAbandonedGrantSelectsAtFirstSYNWithoutEnforce(t *testing.T) {
	for _, mode := range []v8classifier.Mode{v8classifier.ModeOff, v8classifier.ModeShadow} {
		for _, raw := range externalGrantRecoveryViews() {
			t.Run(fmt.Sprintf("%v/%s", mode, externalGrantRecoveryViewName(raw)), func(t *testing.T) {
				m, mock, plain, _ := externalGrantRecoveryModeFixture(t, mode, raw, false)
				fire := externalGrantRecoveryCaptureWatchdog(t, m)
				fire()
				if m.arb.isOwner(1) {
					t.Fatal("watchdog did not release the unused grant")
				}
				before := len(mock.getStartRequests())
				m.requestStartForSession(2, 0x03)
				if starts := externalGrantRecoveryNewStarts(mock, before); len(starts) != 0 {
					t.Fatalf("START written before any SYN: %x", starts)
				}
				plain(protocol.SymbolSyn)
				if starts := externalGrantRecoveryNewStarts(mock, before); len(starts) != 1 || starts[0] != 0x03 {
					t.Fatalf("waiting bid not selected at the first SYN: %x", starts)
				}
				if got := externalGrantRecoveryDrain(m.sessions[2]); externalGrantRecoveryCount(got, protocol.SymbolSyn) != 1 {
					t.Fatalf("first SYN not delivered to the waiting session: %x", got)
				}
			})
		}
	}
}

// externalGrantRecoveryStoppedOwner leaves session 1 in the middle of its own
// request (QQ from STARTED, then ZZ PB SB on the wire) and queues session 2's bid.
func externalGrantRecoveryStoppedOwner(t *testing.T, m *Mux, plain func(...byte), idle, owned time.Duration) {
	t.Helper()
	plain(0x08, 0xb5, 0x04)
	m.stateMu.Lock()
	live := m.phase.telegramLive()
	now := time.Now()
	m.lastWireActivity = now.Add(-idle)
	if owned > 0 {
		m.busOwned = now.Add(-owned)
	}
	m.stateMu.Unlock()
	if !live {
		t.Fatal("owner's telegram is not live")
	}
	m.requestStartForSession(2, 0x03)
	externalGrantRecoveryDrain(m.sessions[2])
}

// (c) An external owner that stopped in mid-telegram on a quiet bus: the
// classifier withholds the SYNs that follow, and the external idle gap is
// applied to them, so the owner is released and arbitration recovers.
func TestExternalGrantRecoveryStoppedOwnerReleasedAtIdleGap(t *testing.T) {
	for _, raw := range externalGrantRecoveryViews() {
		t.Run(externalGrantRecoveryViewName(raw), func(t *testing.T) {
			m, mock, plain, _ := t02LiveTelegramFixture(t, raw, true)
			externalGrantRecoveryStoppedOwner(t, m, plain, time.Second, 0)
			before := len(mock.getStartRequests())
			externalGrantRecoveryRequireRecovered(t, m, mock, plain, before, 4)
		})
	}
}

// (c, raw-only item) The same release when the first withheld SYN is a raw
// symbol without a logical item.
func TestExternalGrantRecoveryStoppedOwnerReleasedAtRawOnlySYN(t *testing.T) {
	m, mock, plain, send := externalGrantRecoveryModeFixture(t, v8classifier.ModeEnforce, true, true)
	externalGrantRecoveryStoppedOwner(t, m, plain, time.Second, 0)
	before := len(mock.getStartRequests())
	send(upstreamItem{hasRaw: true, rawSymbol: protocol.SymbolSyn})
	if m.arb.isOwner(1) {
		t.Fatal("withheld raw-only SYN did not release the idle owner")
	}
	if got := externalGrantRecoveryDrain(m.sessions[2]); len(got) != 0 {
		t.Fatalf("withheld raw-only SYN was delivered: %x", got)
	}
	externalGrantRecoveryRequireRecovered(t, m, mock, plain, before, 3)
}

// (d) The ownership cap applies to withheld SYN too, even while the owner's
// last wire activity is fresh. The idle grace is an hour, so only the cap
// can release the owner however slowly the test runs.
func TestExternalGrantRecoveryStoppedOwnerReleasedAtOwnershipCap(t *testing.T) {
	for _, raw := range externalGrantRecoveryViews() {
		t.Run(externalGrantRecoveryViewName(raw), func(t *testing.T) {
			f := newExternalGrantRecoveryFixture(t, externalGrantRecoveryOptions{
				mode: v8classifier.ModeEnforce, raw: raw, wrote: true, idleGrace: time.Hour,
			})
			m, mock, plain := f.m, f.mock, f.plain
			limit := m.ownershipDurationLimit(1)
			externalGrantRecoveryStoppedOwner(t, m, plain, 0, limit+time.Second)
			before := len(mock.getStartRequests())
			externalGrantRecoveryRequireRecovered(t, m, mock, plain, before, 4)
		})
	}
}

// (e) Guard: an owner whose telegram is live and whose wire activity is
// fresh keeps the bus when one injected AA arrives between its bytes, and
// that AA stays withheld from sessions and the passive stream. The idle grace
// is an hour, so a stall of the test host cannot release the owner.
func TestExternalGrantRecoveryLiveOwnerKeepsBusOnInjectedAA(t *testing.T) {
	for _, raw := range externalGrantRecoveryViews() {
		t.Run(externalGrantRecoveryViewName(raw), func(t *testing.T) {
			f := newExternalGrantRecoveryFixture(t, externalGrantRecoveryOptions{
				mode: v8classifier.ModeEnforce, raw: raw, wrote: true, idleGrace: time.Hour,
			})
			m, mock, plain, passive := f.m, f.mock, f.plain, f.passive
			plain(0x08, 0xb5, 0x04)
			m.requestStartForSession(2, 0x03)
			before := len(mock.getStartRequests())
			plain(protocol.SymbolSyn)
			plain(0x01, 0x55)
			if !m.arb.isOwner(1) {
				t.Fatal("live owner lost the bus on an injected AA")
			}
			if starts := externalGrantRecoveryNewStarts(mock, before); len(starts) != 0 {
				t.Fatalf("injected AA admitted START: %x", starts)
			}
			if got := externalGrantRecoveryDrain(m.sessions[2]); externalGrantRecoveryCount(got, protocol.SymbolSyn) != 0 {
				t.Fatalf("injected AA reached the waiting session: %x", got)
			}
			if got := passive(); externalGrantRecoveryCount(got, protocol.SymbolSyn) != 0 {
				t.Fatalf("injected AA reached the passive stream: %x", got)
			}
		})
	}
}

// (f) Guard: the watchdog and the first SEND are ordered by the state lock.
// An accepted SEND keeps the grant; a SEND after the release is refused.
func TestExternalGrantRecoveryWatchdogAndSendAreOrdered(t *testing.T) {
	t.Run("send_first", func(t *testing.T) {
		m, _, _, _ := t02LiveTelegramFixture(t, false, false)
		fire := externalGrantRecoveryCaptureWatchdog(t, m)
		done := make(chan struct{})
		go func() { m.sessions[1].handleSend(0x08); close(done) }()
		select {
		case req := <-m.activeSendCh:
			req.result <- nil
		case <-time.After(time.Second):
			t.Fatal("SEND did not reach the active path")
		}
		<-done
		fire()
		if !m.arb.isOwner(1) {
			t.Fatal("watchdog released a grant whose SEND was accepted")
		}
	})
	t.Run("watchdog_first", func(t *testing.T) {
		m, _, _, _ := t02LiveTelegramFixture(t, false, false)
		fire := externalGrantRecoveryCaptureWatchdog(t, m)
		fire()
		if m.arb.isOwner(1) {
			t.Fatal("watchdog did not release the unused grant")
		}
		m.sessions[1].handleSend(0x08)
		select {
		case req := <-m.activeSendCh:
			t.Fatalf("SEND after the release reached the active path: %+v", req)
		default:
		}
		hostErrors := 0
		for len(m.sessions[1].sendCh) > 0 {
			if f := <-m.sessions[1].sendCh; f.kind == sessionFrameErrorHost {
				hostErrors++
			}
		}
		if hostErrors != 1 {
			t.Fatalf("SEND after the release answered %d host errors, want 1", hostErrors)
		}
	})
}

// pausingLogWriter blocks the first write that contains substr until resume
// is closed, after signalling hit. The logger's output lock is held meanwhile,
// so any other log line waits too.
type pausingLogWriter struct {
	substr string
	hit    chan struct{}
	resume chan struct{}
	once   sync.Once
}

func (w *pausingLogWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), w.substr) {
		w.once.Do(func() {
			close(w.hit)
			<-w.resume
		})
	}
	return len(p), nil
}

// (g) The abandon counter is counted in the critical section of the owner
// check. The watchdog fires while the accepted SEND is paused at its
// "forwarded" log line, after the owner check and before the SEND reaches the
// adapter; it must keep the grant. Counting the SEND anywhere after that line
// (after forwarding or at its result) lets the watchdog release it.
func TestExternalGrantRecoveryWatchdogSeesSendCountedWithOwnerCheck(t *testing.T) {
	writer := &pausingLogWriter{substr: "SEND 0x08 forwarded", hit: make(chan struct{}), resume: make(chan struct{})}
	f := newExternalGrantRecoveryFixture(t, externalGrantRecoveryOptions{
		mode: v8classifier.ModeEnforce, logger: log.New(writer, "", 0),
	})
	m := f.m
	fire := externalGrantRecoveryCaptureWatchdog(t, m)
	done := make(chan struct{})
	go func() { m.sessions[1].handleSend(0x08); close(done) }() // goroutine: ends when the SEND result is delivered below
	select {
	case <-writer.hit:
	case <-time.After(time.Second):
		close(writer.resume)
		t.Fatal("SEND did not pass the owner check")
	}
	fired := make(chan struct{})
	go func() { fire(); close(fired) }() // goroutine: a release logs and waits for resume
	select {
	case <-fired:
	case <-time.After(200 * time.Millisecond):
	}
	close(writer.resume)
	select {
	case req := <-m.activeSendCh:
		req.result <- nil
	case <-time.After(time.Second):
		t.Fatal("SEND did not reach the active path")
	}
	<-done
	<-fired
	if !m.arb.isOwner(1) {
		t.Fatal("watchdog released a grant whose SEND had passed the owner check")
	}
}

// (h) The gateway-owner branch of the withheld-SYN release: a gateway grant
// past its ownership guard is released at a SYN the classifier withholds, its
// live transaction ends with max_ownership and the release is logged.
func TestExternalGrantRecoveryWithheldSYNReleasesGatewayAtOwnershipGuard(t *testing.T) {
	for _, raw := range externalGrantRecoveryViews() {
		t.Run(externalGrantRecoveryViewName(raw), func(t *testing.T) {
			lines := &lineLog{}
			f := newExternalGrantRecoveryFixture(t, externalGrantRecoveryOptions{
				mode: v8classifier.ModeEnforce, raw: raw, logger: log.New(lines, "", 0),
				owner: gatewaySessionID, initiator: 0x7F,
			})
			m := f.m
			m.stateMu.Lock()
			m.gatewayTxnActive = true
			m.stateMu.Unlock()
			f.plain(0x08, 0xb5, 0x04)
			before := m.ActiveTxnSnapshot().InactiveReasonCounts[ReasonMaxOwnership]
			m.stateMu.Lock()
			m.busOwned = time.Now().Add(-m.ownershipDurationLimit(gatewaySessionID) - time.Second)
			m.lastWireActivity = time.Now()
			m.stateMu.Unlock()
			f.plain(protocol.SymbolSyn)
			if m.arb.isOwner(gatewaySessionID) {
				t.Fatal("gateway still owns the bus past its ownership guard")
			}
			snap := m.ActiveTxnSnapshot()
			if snap.Active || snap.InactiveReasonCounts[ReasonMaxOwnership] != before+1 {
				t.Fatalf("gateway transaction active=%v max_ownership=%d, want ended once", snap.Active, snap.InactiveReasonCounts[ReasonMaxOwnership])
			}
			if lines.count("(classifier withheld SYN: maximum ownership") != 1 {
				t.Fatal("release did not come from the withheld-SYN path")
			}
		})
	}
}

// (i) One session alone: after the watchdog released its unused grant, the
// same session's next bid is started and idle SYN reaches it.
func TestExternalGrantRecoverySingleSessionRecoversAfterAbandonedGrant(t *testing.T) {
	for _, raw := range externalGrantRecoveryViews() {
		t.Run(externalGrantRecoveryViewName(raw), func(t *testing.T) {
			m, mock, plain, _ := t02LiveTelegramFixture(t, raw, false)
			m.sessionsMu.Lock()
			delete(m.sessions, 2)
			delete(m.sessions, 3)
			m.sessionsMu.Unlock()
			fire := externalGrantRecoveryCaptureWatchdog(t, m)
			fire()
			if m.arb.isOwner(1) {
				t.Fatal("watchdog did not release the unused grant")
			}
			before := len(mock.getStartRequests())
			m.requestStartForSession(1, 0x31)
			externalGrantRecoveryRequireRecoveredFor(t, m, mock, plain, 1, 0x31, before, 3)
		})
	}
}
