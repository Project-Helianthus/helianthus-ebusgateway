package main

// Issue #1367: after the adaptermux AM27 blackhole trigger delegates
// recovery, the ebus.primary lifecycle owner (DriverManager behind the
// runtime control bridge) re-establishes the adapter-direct connection with a
// logged backoff, and leaves RUNNING for a visible state meanwhile.
// helianthus.runtime.list, the Portal readback and the adapter status must
// never keep RUNNING over a closed transport. The proxy-class (ENH) endpoint
// goes through the same owner and is covered by twin tests.
//
// Every test drives the production composition (run, with the rctl process
// seams) against a scripted loopback ENH endpoint. Only two test seams are
// narrowed: the mux AM27 threshold (production keeps 30 s) and the delays of
// the native retry policy (its shape is kept: five fast attempts, then the
// persistent phase of issue #1369). The tests of the persistent phase itself
// are in issue1369_persistent_recovery_test.go.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

const (
	issue1367BlackholeThreshold = 300 * time.Millisecond
	issue1367RetryInitial       = 300 * time.Millisecond
	issue1367RetryMax           = 600 * time.Millisecond
)

// issue1367Schedule is the schedule text of the narrowed policy.
const issue1367Schedule = "recovery continues while desired RUNNING (5 fast attempts, then backoff from 300ms doubling to 600ms)"

// issue1367Endpoint is a scripted ENH endpoint. Every session answers INIT
// and INFO (the version payload is the session index, so a recovered
// generation is distinguishable); a streaming endpoint (adapter-direct)
// sends one SYN every 10 ms after INFO until the session is stalled, which
// keeps the TCP connection open and silent like the observed blackhole. With
// rejectInit set, a session answers INIT with an eBUS error; initReply, when
// set, scripts every INIT answer instead; with closeOnAccept set, every
// accepted session is closed at once. The gateway treats all of these as a
// retryable handshake failure of the adapter, never as a rejected
// configuration.
type issue1367Endpoint struct {
	t          *testing.T
	stream     bool
	address    string
	rejectInit atomic.Bool
	// initReply answers the n-th INIT request (from one) of the session with
	// the given index; an empty answer is silence.
	initReply     atomic.Pointer[func(session, request int) []byte]
	closeOnAccept atomic.Bool

	mu       sync.Mutex
	listener net.Listener
	accepts  chan struct{}
	sessions map[*issue1367Session]struct{}
	opened   int
	wg       sync.WaitGroup
	// syns counts streamed SYN symbols across sessions.
	syns atomic.Int64
}

type issue1367Session struct {
	conn      net.Conn
	index     int
	writeMu   sync.Mutex
	stall     chan struct{}
	stallOnce sync.Once
}

func newIssue1367Endpoint(t *testing.T, stream bool) *issue1367Endpoint {
	t.Helper()
	endpoint := &issue1367Endpoint{t: t, stream: stream, address: "127.0.0.1:0", sessions: map[*issue1367Session]struct{}{}}
	endpoint.open()
	t.Cleanup(func() {
		endpoint.refuse()
		endpoint.drop()
		endpoint.wg.Wait()
	})
	return endpoint
}

// open (re)binds the endpoint's address and accepts sessions.
func (endpoint *issue1367Endpoint) open() {
	endpoint.t.Helper()
	listener, err := net.Listen("tcp", endpoint.address)
	if err != nil {
		endpoint.t.Fatalf("listen scripted endpoint: %v", err)
	}
	endpoint.mu.Lock()
	endpoint.listener = listener
	endpoint.address = listener.Addr().String()
	accepts := make(chan struct{})
	endpoint.accepts = accepts
	endpoint.mu.Unlock()
	go func() {
		defer close(accepts)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			endpoint.mu.Lock()
			endpoint.opened++
			session := &issue1367Session{conn: conn, index: endpoint.opened, stall: make(chan struct{})}
			endpoint.sessions[session] = struct{}{}
			endpoint.wg.Add(1)
			endpoint.mu.Unlock()
			if endpoint.closeOnAccept.Load() {
				_ = conn.Close()
			}
			go endpoint.serve(session)
		}
	}()
}

// refuse closes the listener: later dials are refused, open sessions stay.
func (endpoint *issue1367Endpoint) refuse() {
	endpoint.mu.Lock()
	listener, accepts := endpoint.listener, endpoint.accepts
	endpoint.listener = nil
	endpoint.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
		<-accepts
	}
}

// stall silences every open session without closing it, once the bus was
// demonstrably active: AM27 only treats a silence after received data as a
// blackhole, a never-active bus is legitimately quiet.
func (endpoint *issue1367Endpoint) stall() {
	endpoint.t.Helper()
	deadline := time.Now().Add(rctlBound)
	for start := endpoint.syns.Load(); endpoint.syns.Load() < start+5; {
		if time.Now().After(deadline) {
			endpoint.t.Fatal("scripted endpoint streamed no SYN before the stall")
		}
		time.Sleep(time.Millisecond)
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	for session := range endpoint.sessions {
		session.stallOnce.Do(func() { close(session.stall) })
	}
}

// drop closes every open session (a proxy that went away).
func (endpoint *issue1367Endpoint) drop() {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	for session := range endpoint.sessions {
		_ = session.conn.Close()
	}
}

func (endpoint *issue1367Endpoint) sessionCount() int {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	return endpoint.opened
}

func (session *issue1367Session) write(frames ...[2]byte) error {
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	for _, frame := range frames {
		if _, err := session.conn.Write(frame[:]); err != nil {
			return err
		}
	}
	return nil
}

func (session *issue1367Session) writeRaw(data []byte) error {
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	_, err := session.conn.Write(data)
	return err
}

func (endpoint *issue1367Endpoint) serve(session *issue1367Session) {
	defer endpoint.wg.Done()
	defer func() {
		_ = session.conn.Close()
		endpoint.mu.Lock()
		delete(endpoint.sessions, session)
		endpoint.mu.Unlock()
	}()
	ready := make(chan struct{})
	var readyOnce sync.Once
	// ended releases the streamer of a session that closed before INFO (an
	// INIT the endpoint rejected, for example).
	ended := make(chan struct{})
	defer close(ended)
	if endpoint.stream {
		endpoint.wg.Add(1)
		go func() {
			defer endpoint.wg.Done()
			select {
			case <-ready:
			case <-session.stall:
				return
			case <-ended:
				return
			}
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-session.stall:
					return
				case <-ticker.C:
					if session.write(transport.EncodeENH(transport.ENHResReceived, 0xAA)) != nil {
						return
					}
					endpoint.syns.Add(1)
				}
			}
		}()
	}
	pair := make([]byte, 2)
	inits := 0
	for {
		if _, err := io.ReadFull(session.conn, pair[:1]); err != nil {
			return
		}
		if pair[0]&0x80 == 0 {
			continue // unencoded data byte
		}
		if _, err := io.ReadFull(session.conn, pair[1:]); err != nil {
			return
		}
		frame, err := transport.DecodeENH(pair[0], pair[1])
		if err != nil {
			continue
		}
		switch frame.Command {
		case transport.ENHReqInit:
			inits++
			resetted := transport.EncodeENH(transport.ENHResResetted, 0x01)
			reply := resetted[:]
			if endpoint.rejectInit.Load() {
				rejected := transport.EncodeENH(transport.ENHResErrorEBUS, 0x01)
				reply = rejected[:]
			}
			if script := endpoint.initReply.Load(); script != nil {
				reply = (*script)(session.index, inits)
			}
			if len(reply) > 0 && session.writeRaw(reply) != nil {
				return
			}
		case transport.ENHReqInfo:
			if session.write(transport.EncodeENH(transport.ENHResInfo, 0x01), transport.EncodeENH(transport.ENHResInfo, byte(session.index))) != nil {
				return
			}
			readyOnce.Do(func() { close(ready) })
		}
	}
}

// issue1367Logs captures the process log for ordered line assertions.
type issue1367Logs struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	hooks []func(string)
}

func captureIssue1367Logs(t *testing.T) *issue1367Logs {
	t.Helper()
	logs := &issue1367Logs{}
	previous := log.Writer()
	log.SetOutput(logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	return logs
}

func (logs *issue1367Logs) Write(p []byte) (int, error) {
	logs.mu.Lock()
	logs.buf.Write(p)
	hooks := append([]func(string){}, logs.hooks...)
	logs.mu.Unlock()
	for _, hook := range hooks {
		hook(string(p))
	}
	return len(p), nil
}

func (logs *issue1367Logs) text() string {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	return logs.buf.String()
}

// onLine runs fn once, the first time a line containing want is logged.
func (logs *issue1367Logs) onLine(want string, fn func()) {
	var once sync.Once
	logs.mu.Lock()
	logs.hooks = append(logs.hooks, func(line string) {
		if strings.Contains(line, want) {
			once.Do(func() { go fn() })
		}
	})
	logs.mu.Unlock()
}

// requireOrdered asserts that every fragment occurs, in order.
func (logs *issue1367Logs) requireOrdered(t *testing.T, fragments ...string) {
	t.Helper()
	text := logs.text()
	at := 0
	for _, fragment := range fragments {
		index := strings.Index(text[at:], fragment)
		if index < 0 {
			t.Fatalf("log lacks %q after offset %d; recovery log:\n%s", fragment, at, issue1367RecoveryLines(text))
		}
		at += index + len(fragment)
	}
}

func issue1367RecoveryLines(text string) string {
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, "ebus.primary:") || strings.Contains(line, "AM27") || strings.Contains(line, "lifecycle owner") {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

func (logs *issue1367Logs) count(fragment string) int {
	return strings.Count(logs.text(), fragment)
}

// withIssue1367Seams narrows the AM27 threshold and the retry delays.
func withIssue1367Seams(t *testing.T) {
	t.Helper()
	previousThreshold, previousPolicy := adapterDirectBlackholeThreshold, ebusDriverRetryPolicy
	adapterDirectBlackholeThreshold = issue1367BlackholeThreshold
	ebusDriverRetryPolicy = drivermanager.RetryPolicy{Budget: defaultEBusRetryBudget, InitialDelay: issue1367RetryInitial, MaxDelay: issue1367RetryMax, JitterRatio: -1,
		Persistent: drivermanager.PersistentRetry{Enabled: true, InitialDelay: issue1367RetryInitial, MaxDelay: issue1367RetryMax}}
	t.Cleanup(func() { adapterDirectBlackholeThreshold, ebusDriverRetryPolicy = previousThreshold, previousPolicy })
}

func startIssue1367Process(t *testing.T, endpoint *issue1367Endpoint, protocol ebusgateway.TransportProtocol) *rctlProcess {
	t.Helper()
	env := newRCtlEnv(t)
	return env.start(rctlProcessOptions{mcp: true, config: func(cfg *ebusgateway.Config) {
		cfg.TransportConfig.Protocol = protocol
		cfg.TransportConfig.Network = "tcp"
		cfg.TransportConfig.Address = endpoint.address
		cfg.TransportConfig.Dial = nil
		cfg.TransportConfig.DialTimeout = time.Second
		if protocol == ebusgateway.TransportAdapterDirect {
			// The observed candidate also exposed the adapter through the
			// proxy listener; each generation binds its own.
			cfg.ProxyListenAddr = "127.0.0.1:0"
		}
	}})
}

// issue1367Version is the version INFO query the adapter-info poller issues
// through the stable eBUS transport.
func issue1367Version(process *rctlProcess) ([]byte, error) {
	requester, ok := process.control.driver.active.(transport.InfoRequester)
	if !ok {
		return nil, errors.New("stable transport has no INFO requester")
	}
	return requester.RequestInfo(transport.AdapterInfoVersion)
}

func requireIssue1367Version(t *testing.T, process *rctlProcess, session int) {
	t.Helper()
	data, err := issue1367Version(process)
	if err != nil || len(data) != 1 || int(data[0]) != session {
		t.Fatalf("version query = %v, %v; want session %d's payload", data, err, session)
	}
}

// waitIssue1367Status polls the service view (the source of
// helianthus.runtime.list and the Portal readback) for one state.
func waitIssue1367Status(t *testing.T, process *rctlProcess, state drivermanager.ObservedState, reason drivermanager.ReasonCode, logs *issue1367Logs) runtimecontrol.DriverStatus {
	t.Helper()
	deadline := time.Now().Add(rctlBound)
	for {
		status := process.status()
		if status.ObservedState == state && status.Reason == reason {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("service view %s/%s; want %s/%s; recovery log:\n%s", status.ObservedState, status.Reason, state, reason, issue1367RecoveryLines(logs.text()))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// requireIssue1367List asserts helianthus.runtime.list over the ordinary MCP
// endpoint shows ebus.primary in the given state.
func requireIssue1367List(t *testing.T, process *rctlProcess, state drivermanager.ObservedState, reason drivermanager.ReasonCode) {
	t.Helper()
	for _, driver := range process.mcpList().Drivers {
		if driver.DriverID != primaryEBusDriverID {
			continue
		}
		if driver.ObservedState != string(state) || driver.Reason != string(reason) {
			t.Fatalf("helianthus.runtime.list ebus.primary = %s/%s; want %s/%s", driver.ObservedState, driver.Reason, state, reason)
		}
		return
	}
	t.Fatal("helianthus.runtime.list has no ebus.primary entry")
}

// issue1367ObservedSince returns ebus.primary's committed observed events.
func issue1367ObservedSince(process *rctlProcess, cursor uint64) []runtimecontrol.Event {
	var events []runtimecontrol.Event
	for _, event := range process.control.Service().EventsSince(cursor).Events {
		if event.Kind == runtimecontrol.EventObservedChanged && event.DriverID == primaryEBusDriverID {
			events = append(events, event)
		}
	}
	return events
}

func issue1367RequireRecoveringView(t *testing.T, process *rctlProcess, logs *issue1367Logs) {
	t.Helper()
	status := waitIssue1367Status(t, process, drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled, logs)
	if got, reason := portalConfigReadbackStatus(status); got != "pending" || reason != "backoff" {
		t.Fatalf("Portal readback during recovery = %s/%s; want pending/backoff", got, reason)
	}
	if _, err := issue1367Version(process); !errors.Is(err, ebuserrors.ErrTransportClosed) {
		t.Fatalf("version query during recovery = %v; want transport connection closed", err)
	}
	if got := process.adapterStatus(); got != adapterStatusUnavailable {
		t.Fatalf("adapter status during recovery = %q; want %q", got, adapterStatusUnavailable)
	}
}

func TestIssue1367AdapterDirectStallPastAM27RecoversThroughLoggedBackoff(t *testing.T) {
	withIssue1367Seams(t)
	logs := captureIssue1367Logs(t)
	endpoint := newIssue1367Endpoint(t, true)
	process := startIssue1367Process(t, endpoint, ebusgateway.TransportAdapterDirect)
	process.waitObserved(drivermanager.ObservedRunning)
	requireIssue1367Version(t, process, 1)
	first := process.control.driver.Snapshot()
	cursor := process.control.Service().List().EventCursor

	// The first reconnect attempt is refused; the endpoint comes back once
	// that failure is logged, so the second attempt reconnects.
	endpoint.refuse()
	logs.onLine("ebus.primary: recovery attempt 1 failed (class=dial_refused", endpoint.open)
	endpoint.stall()

	issue1367RequireRecoveringView(t, process, logs)
	requireIssue1367List(t, process, drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled)

	status := waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
	if got, reason := portalConfigReadbackStatus(status); got != "effective" || reason != "running" {
		t.Fatalf("Portal readback after recovery = %s/%s; want effective/running", got, reason)
	}
	requireIssue1367List(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone)
	requireIssue1367Version(t, process, endpoint.sessionCount())
	if endpoint.sessionCount() != 2 {
		t.Fatalf("endpoint sessions = %d; want exactly one reconnect", endpoint.sessionCount())
	}
	recovered := process.control.driver.Snapshot()
	if recovered.ObservedState != drivermanager.ObservedRunning || recovered.Generation != first.Generation+1 {
		t.Fatalf("manager snapshot = %s generation %d; want RUNNING generation %d", recovered.ObservedState, recovered.Generation, first.Generation+1)
	}
	if got := process.adapterStatus(); got == adapterStatusUnavailable {
		t.Fatalf("adapter status after recovery = %q", got)
	}

	events := issue1367ObservedSince(process, cursor)
	if len(events) < 2 || events[0].ObservedState == drivermanager.ObservedRunning || events[len(events)-1].ObservedState != drivermanager.ObservedRunning {
		t.Fatalf("service view transitions = %+v; want RUNNING left during recovery and restored", events)
	}
	for _, event := range events {
		if event.Epoch != events[0].Epoch || event.Generation != events[0].Generation {
			t.Fatalf("recovery changed the lifecycle correlation: %+v", events)
		}
	}
	// The owner is notified inside the mux's delegation, before the mux logs
	// "connection recovery delegated to lifecycle owner".
	logs.requireOrdered(t,
		"triggering reconnect (AM27)",
		fmt.Sprintf("ebus.primary: connection lost (reason=adapter_connection_lost, generation=%d); %s", first.Generation, issue1367Schedule),
		"ebus.primary: lifecycle BACKOFF (reason=RETRY_SCHEDULED, next_attempt=1, next_attempt_at=",
		"ebus.primary: recovery attempt 1 (reason=adapter_connection_lost, phase=fast)",
		`ebus.primary: recovery attempt 1 failed (class=dial_refused, error="connect: connection refused")`,
		"ebus.primary: recovery attempt 2 (reason=retry_after_dial_refused, phase=fast)",
		fmt.Sprintf("ebus.primary: recovery attempt 2 reconnected (generation=%d); awaiting admission", recovered.Generation),
		fmt.Sprintf("ebus.primary: recovered on attempt 2 (generation=%d)", recovered.Generation),
		fmt.Sprintf("ebus.primary: lifecycle RUNNING (generation=%d, recovered)", recovered.Generation),
	)
	issue1367RequireOneDelegation(t, logs)
}

func issue1367RequireOneDelegation(t *testing.T, logs *issue1367Logs) {
	t.Helper()
	if got := logs.count("triggering reconnect (AM27)"); got != 1 {
		t.Fatalf("AM27 triggered %d times; want once", got)
	}
	if got := logs.count("adaptermux: connection recovery delegated to lifecycle owner"); got != 1 {
		t.Fatalf("recovery delegated %d times; want once", got)
	}
}

func TestIssue1367ProxyClassTransportLossRecoversThroughSameLifecycleOwner(t *testing.T) {
	withIssue1367Seams(t)
	logs := captureIssue1367Logs(t)
	endpoint := newIssue1367Endpoint(t, false)
	process := startIssue1367Process(t, endpoint, ebusgateway.TransportENH)
	process.waitObserved(drivermanager.ObservedRunning)
	requireIssue1367Version(t, process, 1)
	first := process.control.driver.Snapshot()
	cursor := process.control.Service().List().EventCursor

	endpoint.refuse()
	logs.onLine("ebus.primary: recovery attempt 1 failed (class=dial_refused", endpoint.open)
	endpoint.drop()
	// The proxy class has no mux: the closed transport is observed by the
	// next consumer call, as the adapter-info poller does.
	issue1367DetectProxyLoss(t, process, logs)

	issue1367RequireRecoveringView(t, process, logs)
	requireIssue1367List(t, process, drivermanager.ObservedBackoff, drivermanager.ReasonRetryScheduled)
	waitIssue1367Status(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone, logs)
	requireIssue1367List(t, process, drivermanager.ObservedRunning, drivermanager.ReasonNone)
	requireIssue1367Version(t, process, endpoint.sessionCount())
	recovered := process.control.driver.Snapshot()
	if recovered.Generation != first.Generation+1 {
		t.Fatalf("manager generation = %d; want %d", recovered.Generation, first.Generation+1)
	}
	events := issue1367ObservedSince(process, cursor)
	if len(events) < 2 || events[0].ObservedState == drivermanager.ObservedRunning || events[len(events)-1].ObservedState != drivermanager.ObservedRunning {
		t.Fatalf("service view transitions = %+v; want RUNNING left during recovery and restored", events)
	}
	logs.requireOrdered(t,
		fmt.Sprintf("ebus.primary: connection lost (reason=transport_closed, generation=%d); %s", first.Generation, issue1367Schedule),
		"ebus.primary: lifecycle BACKOFF (reason=RETRY_SCHEDULED, next_attempt=1, next_attempt_at=",
		"ebus.primary: recovery attempt 1 (reason=transport_closed, phase=fast)",
		`ebus.primary: recovery attempt 1 failed (class=dial_refused, error="connect: connection refused")`,
		"ebus.primary: recovery attempt 2 (reason=retry_after_dial_refused, phase=fast)",
		fmt.Sprintf("ebus.primary: recovered on attempt 2 (generation=%d)", recovered.Generation),
		fmt.Sprintf("ebus.primary: lifecycle RUNNING (generation=%d, recovered)", recovered.Generation),
	)
}

// issue1367DetectProxyLoss issues consumer INFO queries until the manager
// withdrew the lost generation.
func issue1367DetectProxyLoss(t *testing.T, process *rctlProcess, logs *issue1367Logs) {
	t.Helper()
	deadline := time.Now().Add(rctlBound)
	for process.control.driver.Snapshot().ObservedState == drivermanager.ObservedRunning {
		_, _ = issue1367Version(process)
		if time.Now().After(deadline) {
			t.Fatalf("closed proxy transport never reached the lifecycle owner; recovery log:\n%s", issue1367RecoveryLines(logs.text()))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestIssue1367RecoveryFailureCategoriesAreClosed(t *testing.T) {
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := refused.Addr().String()
	_ = refused.Close()
	_, dialErr := net.Dial("tcp", address)
	if dialErr == nil {
		t.Fatal("dial to a closed loopback port succeeded")
	}
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"dial refused":        {fmt.Errorf("adaptermux: initial connect: dial tcp/%s: %w", address, dialErr), "dial_refused"},
		"listen":              {&net.OpError{Op: "listen", Net: "tcp", Err: errors.New("in use")}, "proxy_listen_failed"},
		"dial timeout":        {&net.OpError{Op: "dial", Net: "tcp", Err: issue1367Timeout{}}, "dial_timeout"},
		"dial other":          {&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("other")}, "dial_failed"},
		"handshake timeout":   {fmt.Errorf("adaptermux: INIT handshake failed: %w", ebuserrors.ErrTimeout), "handshake_timeout"},
		"invalid payload":     {fmt.Errorf("x: %w", ebuserrors.ErrInvalidPayload), "invalid_payload"},
		"transport closed":    {fmt.Errorf("enh transport read closed: %w", ebuserrors.ErrTransportClosed), "transport_closed"},
		"config rejected":     {rejectEBusConfig(fmt.Errorf("x: %w", ebuserrors.ErrInvalidPayload)), "config_rejected"},
		"quarantined":         {transport.ErrDriverSafetyQuarantined, "close_unconfirmed"},
		"drain timeout":       {transport.ErrDriverStopTimeout, "drain_timeout"},
		"runtime unavailable": {transport.ErrDriverUnavailable, "runtime_unavailable"},
		"other":               {errors.New("boom at adapter.invalid:9999"), "start_failed"},
	} {
		if got := ebusRecoveryFailureCategory(tc.err); got != tc.want {
			t.Fatalf("%s: category = %q; want %q", name, got, tc.want)
		}
		if got := ebusRecoveryFailureCategory(tc.err); strings.ContainsAny(got, ".:/ ") {
			t.Fatalf("%s: category %q is not closed vocabulary", name, got)
		}
	}
	if got := ebusConnectionLossReason(&ebusConnectionLoss{reason: ebusLossProxyListener}); got != ebusLossProxyListener {
		t.Fatalf("listener loss reason = %q", got)
	}
	if !errors.Is(&ebusConnectionLoss{reason: ebusLossProxyListener}, ebuserrors.ErrAdapterReset) {
		t.Fatal("connection loss no longer classifies as an adapter reset")
	}
}

type issue1367Timeout struct{}

func (issue1367Timeout) Error() string   { return "i/o timeout" }
func (issue1367Timeout) Timeout() bool   { return true }
func (issue1367Timeout) Temporary() bool { return true }
