package ebusgateway

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

type passiveEventRecorder struct {
	mu     sync.Mutex
	events []PassiveTapEvent
	notify chan struct{}
}

func newPassiveEventRecorder() *passiveEventRecorder {
	return &passiveEventRecorder{
		notify: make(chan struct{}, 32),
	}
}

func (recorder *passiveEventRecorder) OnPassiveTapEvent(event PassiveTapEvent) {
	recorder.mu.Lock()
	recorder.events = append(recorder.events, event)
	recorder.mu.Unlock()
	select {
	case recorder.notify <- struct{}{}:
	default:
	}
}

func (recorder *passiveEventRecorder) snapshot() []PassiveTapEvent {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]PassiveTapEvent(nil), recorder.events...)
}

func TestStartPassiveBusTapRejectsEbusdTCP(t *testing.T) {
	t.Parallel()

	for _, protocolName := range []TransportProtocol{TransportEbusdTCP, TransportProtocol("ebusd")} {
		protocolName := protocolName
		t.Run(string(protocolName), func(t *testing.T) {
			t.Parallel()

			cfg := DefaultConfig()
			cfg.TransportConfig = TransportConfig{
				Protocol: protocolName,
				Network:  "tcp",
				Address:  "127.0.0.1:9999",
			}

			if _, err := StartPassiveBusTap(context.Background(), cfg, newPassiveEventRecorder()); err == nil {
				t.Fatal("StartPassiveBusTap error = nil; want unsupported transport error")
			}
		})
	}
}

func TestPassiveBusTap_WrappedResetAndDecodeFaultsStillSurface(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	recorder := newPassiveEventRecorder()

	cfg := DefaultConfig()
	cfg.TransportConfig = TransportConfig{
		Protocol:     TransportENH,
		Network:      "tcp",
		Address:      "127.0.0.1:9999",
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
		DialTimeout:  time.Second,
		Dial: func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
			return client, nil
		},
	}
	cfg.PassiveAbsenceThreshold = time.Second
	cfg.PassiveReconnectInitialDelay = time.Second
	cfg.PassiveReconnectMaxDelay = time.Second

	go func() {
		defer func() { _ = server.Close() }()
		reset := transport.EncodeENH(transport.ENHResResetted, 0x00)
		payload := append([]byte{}, reset[:]...)
		payload = append(payload, enhReceivedBytes([]byte{0x11, protocol.SymbolEscape, 0x02, 0x22})...)
		_, _ = server.Write(payload)
	}()

	wrap := func(inner transport.RawTransport) transport.RawTransport {
		inner = &loggingTransport{inner: inner, logger: log.New(io.Discard, "", 0)}
		return newWireLogTransport(inner, &wireLogger{writer: io.Discard}, "passive")
	}

	tap, err := StartPassiveBusTapWithTransport(context.Background(), cfg, recorder, wrap)
	if err != nil {
		t.Fatalf("StartPassiveBusTapWithTransport error = %v", err)
	}
	defer func() {
		if err := tap.Close(); err != nil {
			t.Fatalf("Close error = %v", err)
		}
	}()

	events := waitForPassiveEvents(t, recorder, 2*time.Second, func(events []PassiveTapEvent) bool {
		return countPassiveEventKind(events, PassiveTapEventReset) >= 1 &&
			countPassiveEventKind(events, PassiveTapEventDecodeFault) >= 1 &&
			hasPassiveSymbols(events, 0x11, 0x22)
	})

	if !hasPassiveSymbols(events, 0x11, 0x22) {
		t.Fatalf("symbol events = %v; want [0x11, 0x22]", passiveSymbols(events))
	}
}

func TestPassiveBusTap_ReconnectsAfterDisconnect(t *testing.T) {
	t.Parallel()

	recorder := newPassiveEventRecorder()
	var dialMu sync.Mutex
	dialCount := 0

	cfg := DefaultConfig()
	cfg.TransportConfig = TransportConfig{
		Protocol:     TransportENH,
		Network:      "tcp",
		Address:      "127.0.0.1:9999",
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
		DialTimeout:  time.Second,
		Dial: func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
			dialMu.Lock()
			defer dialMu.Unlock()
			dialCount++
			client, server := net.Pipe()
			switch dialCount {
			case 1:
				go writePassivePayload(server, enhReceivedBytes([]byte{0x11}))
			case 2:
				go writePassivePayload(server, enhReceivedBytes([]byte{0x22}))
			default:
				_ = server.Close()
				return nil, fmt.Errorf("unexpected extra reconnect")
			}
			return client, nil
		},
	}
	cfg.PassiveAbsenceThreshold = time.Second
	cfg.PassiveReconnectInitialDelay = 5 * time.Millisecond
	cfg.PassiveReconnectMaxDelay = 5 * time.Millisecond

	tap, err := StartPassiveBusTap(context.Background(), cfg, recorder)
	if err != nil {
		t.Fatalf("StartPassiveBusTap error = %v", err)
	}
	defer func() {
		if err := tap.Close(); err != nil {
			t.Fatalf("Close error = %v", err)
		}
	}()

	events := waitForPassiveEvents(t, recorder, 2*time.Second, func(events []PassiveTapEvent) bool {
		return hasPassiveSymbols(events, 0x11, 0x22) &&
			countPassiveEventKind(events, PassiveTapEventConnected) >= 2 &&
			countPassiveEventKind(events, PassiveTapEventDisconnected) >= 1
	})

	if !hasPassiveSymbols(events, 0x11, 0x22) {
		t.Fatalf("symbol events = %v; want reconnect sequence [0x11, 0x22]", passiveSymbols(events))
	}

	snapshot := tap.Snapshot()
	if snapshot.ConnectCount < 2 {
		t.Fatalf("ConnectCount = %d; want >= 2", snapshot.ConnectCount)
	}
	if snapshot.DisconnectCount < 1 {
		t.Fatalf("DisconnectCount = %d; want >= 1", snapshot.DisconnectCount)
	}
}

func TestPassiveBusTap_ProxyLikeEndpointDoesNotReconnectOnReadTimeoutSilence(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	defer func() { _ = server.Close() }()

	recorder := newPassiveEventRecorder()
	cfg := DefaultConfig()
	cfg.TransportConfig = TransportConfig{
		Protocol:     TransportENS,
		Network:      "tcp",
		Address:      "127.0.0.1:19001",
		ReadTimeout:  20 * time.Millisecond,
		WriteTimeout: 20 * time.Millisecond,
		DialTimeout:  time.Second,
		Dial: func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
			return client, nil
		},
	}
	cfg.PassiveAbsenceThreshold = 60 * time.Millisecond
	cfg.PassiveReconnectInitialDelay = 5 * time.Millisecond
	cfg.PassiveReconnectMaxDelay = 5 * time.Millisecond

	tap, err := StartPassiveBusTap(context.Background(), cfg, recorder)
	if err != nil {
		t.Fatalf("StartPassiveBusTap error = %v", err)
	}
	defer func() {
		if err := tap.Close(); err != nil {
			t.Fatalf("Close error = %v", err)
		}
	}()

	waitForPassiveEvents(t, recorder, 2*time.Second, func(events []PassiveTapEvent) bool {
		return countPassiveEventKind(events, PassiveTapEventReadTimeout) >= 3
	})

	time.Sleep(100 * time.Millisecond)

	snapshot := tap.Snapshot()
	if !snapshot.Connected {
		t.Fatal("Connected = false; want true for proxy-like silent observer session")
	}
	if snapshot.ConnectCount != 1 {
		t.Fatalf("ConnectCount = %d; want 1", snapshot.ConnectCount)
	}
	if snapshot.DisconnectCount != 0 {
		t.Fatalf("DisconnectCount = %d; want 0", snapshot.DisconnectCount)
	}
}

func TestPassiveBusTap_LoopbackENHStillReconnectsOnReadTimeoutSilence(t *testing.T) {
	t.Parallel()

	recorder := newPassiveEventRecorder()
	keepStreaming := make(chan struct{})
	var serversMu sync.Mutex
	var servers []net.Conn
	var dialMu sync.Mutex
	dialCount := 0

	cfg := DefaultConfig()
	cfg.TransportConfig = TransportConfig{
		Protocol:     TransportENH,
		Network:      "tcp",
		Address:      "127.0.0.1:9999",
		ReadTimeout:  20 * time.Millisecond,
		WriteTimeout: 20 * time.Millisecond,
		DialTimeout:  time.Second,
		Dial: func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
			dialMu.Lock()
			defer dialMu.Unlock()
			dialCount++

			client, server := net.Pipe()
			serversMu.Lock()
			servers = append(servers, server)
			serversMu.Unlock()

			switch dialCount {
			case 1:
				// Stay silent so the absence threshold forces a reconnect.
			case 2:
				go func() {
					ticker := time.NewTicker(15 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-keepStreaming:
							return
						case <-ticker.C:
							if _, err := server.Write(enhReceivedBytes([]byte{0x44})); err != nil {
								return
							}
						}
					}
				}()
			default:
				_ = server.Close()
				return nil, fmt.Errorf("unexpected extra reconnect")
			}

			return client, nil
		},
	}
	cfg.PassiveAbsenceThreshold = 60 * time.Millisecond
	cfg.PassiveReconnectInitialDelay = 5 * time.Millisecond
	cfg.PassiveReconnectMaxDelay = 5 * time.Millisecond

	t.Cleanup(func() {
		close(keepStreaming)
		serversMu.Lock()
		defer serversMu.Unlock()
		for _, server := range servers {
			_ = server.Close()
		}
	})

	tap, err := StartPassiveBusTap(context.Background(), cfg, recorder)
	if err != nil {
		t.Fatalf("StartPassiveBusTap error = %v", err)
	}
	defer func() {
		if err := tap.Close(); err != nil {
			t.Fatalf("Close error = %v", err)
		}
	}()

	events := waitForPassiveEvents(t, recorder, 2*time.Second, func(events []PassiveTapEvent) bool {
		return countPassiveEventKind(events, PassiveTapEventDisconnected) >= 1 &&
			countPassiveEventKind(events, PassiveTapEventConnected) >= 2 &&
			hasPassiveSymbols(events, 0x44)
	})

	if !hasPassiveSymbols(events, 0x44) {
		t.Fatalf("symbol events = %v; want reconnect recovery symbol [0x44]", passiveSymbols(events))
	}

	snapshot := tap.Snapshot()
	if !snapshot.Connected {
		t.Fatal("Connected = false; want true after loopback ENH reconnect")
	}
	if snapshot.ConnectCount < 2 {
		t.Fatalf("ConnectCount = %d; want >= 2", snapshot.ConnectCount)
	}
	if snapshot.DisconnectCount < 1 {
		t.Fatalf("DisconnectCount = %d; want >= 1", snapshot.DisconnectCount)
	}
}

func TestBroadcastListener_RoutesBroadcastFramesViaPassiveTap(t *testing.T) {
	client, server := net.Pipe()
	cfg := DefaultConfig()
	cfg.TransportConfig = TransportConfig{
		Protocol:     TransportENH,
		Network:      "tcp",
		Address:      "127.0.0.1:9999",
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
		DialTimeout:  time.Second,
		Dial: func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
			return client, nil
		},
	}
	cfg.PassiveAbsenceThreshold = time.Second
	cfg.PassiveReconnectInitialDelay = time.Second
	cfg.PassiveReconnectMaxDelay = time.Second

	plane := &mockPlane{
		name: "broadcast",
		subscriptions: []router.Subscription{
			{Primary: 0xB5, Secondary: 0x16},
		},
	}
	eventRouter := router.NewBusEventRouter(nil)
	eventRouter.SetPlanes([]router.Plane{plane})

	go func() {
		defer func() { _ = server.Close() }()
		unicast := protocol.Frame{
			Source:    0x10,
			Target:    0x08,
			Primary:   0xB5,
			Secondary: 0x16,
			Data:      []byte{0x01, 0x02},
		}
		broadcast := protocol.Frame{
			Source:    0x10,
			Target:    protocol.AddressBroadcast,
			Primary:   0xB5,
			Secondary: 0x16,
			Data:      []byte{0x55, 0x66},
		}
		payload := append([]byte{}, enhReceivedBytes(frameBytes(unicast))...)
		payload = append(payload, enhReceivedBytes([]byte{protocol.SymbolAck})...)
		payload = append(payload, enhReceivedBytes(responseSegmentBytes([]byte{0x01, 0x02}))...)
		payload = append(payload, enhReceivedBytes([]byte{protocol.SymbolAck, protocol.SymbolSyn})...)
		payload = append(payload, enhReceivedBytes(frameBytes(broadcast))...)
		_, _ = server.Write(payload)
	}()

	listener, err := StartBroadcastListener(context.Background(), cfg, eventRouter)
	if err != nil {
		t.Fatalf("StartBroadcastListener error = %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Fatalf("Close error = %v", err)
		}
	}()

	waitForCondition(t, 2*time.Second, func() bool {
		return plane.BroadcastCount() == 1
	})
	if got := plane.BroadcastCount(); got != 1 {
		t.Fatalf("OnBroadcast calls = %d; want 1", got)
	}
}

func TestBroadcastListener_RouterOverflowMarksDegradedAndResubscribes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reconstructor := newPassiveTransactionReconstructorCore(DefaultConfig())
	reconstructor.ctx, reconstructor.cancel = context.WithCancel(ctx)
	defer func() {
		if err := reconstructor.Close(); err != nil {
			t.Fatalf("reconstructor close: %v", err)
		}
	}()

	plane := &decodingRouterPlane{
		mockPlane: &mockPlane{
			name: "broadcast",
			subscriptions: []router.Subscription{
				{Primary: 0xB5, Secondary: 0x16},
			},
		},
		decoded: map[string]types.Value{
			"energy": {Value: uint8(1), Valid: true},
		},
		handled: true,
	}
	eventRouter := router.NewBusEventRouter(nil)
	eventRouter.SetPlanes([]router.Plane{plane})

	beforeFaults := expvarMapInt64Value(observeFirstBroadcastSupervisorFaultsTotal, "router_overflow")
	beforeResubscribes := observeFirstBroadcastSupervisorResubscribeTotal.Value()
	listener, err := StartBroadcastListenerWithReconstructor(ctx, eventRouter, reconstructor)
	if err != nil {
		t.Fatalf("StartBroadcastListenerWithReconstructor error = %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Fatalf("listener close: %v", err)
		}
	}()

	broadcast := protocol.Frame{
		Source:    0x10,
		Target:    protocol.AddressBroadcast,
		Primary:   0xB5,
		Secondary: 0x16,
		Data:      []byte{0x55, 0x66},
	}
	for i := 0; i < 65; i++ {
		reconstructor.publish(PassiveClassifiedEvent{
			Kind:       PassiveClassifiedEventBroadcastFrame,
			Request:    broadcast,
			HasRequest: true,
			ObservedAt: time.Unix(0, int64(i+1)),
		})
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return observeFirstBroadcastSupervisorState.Value() == "degraded" &&
			expvarMapInt64Value(observeFirstBroadcastSupervisorFaultsTotal, "router_overflow") >= beforeFaults+1 &&
			observeFirstBroadcastSupervisorResubscribeTotal.Value() >= beforeResubscribes+1
	})

	drainBroadcastEvents(eventRouter.Events())

	reconstructor.publish(PassiveClassifiedEvent{
		Kind:       PassiveClassifiedEventBroadcastFrame,
		Request:    broadcast,
		HasRequest: true,
		ObservedAt: time.Unix(0, 1000),
	})

	waitForCondition(t, 2*time.Second, func() bool {
		return observeFirstBroadcastSupervisorState.Value() == "healthy"
	})

	select {
	case event := <-eventRouter.Events():
		if event.Plane != "broadcast" {
			t.Fatalf("event.Plane = %q; want broadcast", event.Plane)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for post-resubscribe broadcast event")
	}
}

func TestBroadcastListener_RecoveryWindowTracksLatestFault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener := &BroadcastListener{ctx: ctx, recoveryWindow: 40 * time.Millisecond}
	observeFirstBroadcastSupervisorState.Set("healthy")

	listener.markDegraded("router_overflow")
	time.Sleep(20 * time.Millisecond)
	listener.markDegraded("router_overflow")

	time.Sleep(30 * time.Millisecond)
	if !broadcastListenerIsDegraded(listener) {
		t.Fatal("listener degraded = false; want true before latest fault-free window elapses")
	}

	waitForCondition(t, 250*time.Millisecond, func() bool {
		return !broadcastListenerIsDegraded(listener) && observeFirstBroadcastSupervisorState.Value() == "healthy"
	})
}

func TestBroadcastListener_TerminalResubscribeFailureStaysDegraded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener := &BroadcastListener{ctx: ctx, recoveryWindow: 30 * time.Millisecond}
	observeFirstBroadcastSupervisorState.Set("healthy")

	listener.markDegraded("router_overflow")
	listener.markDegraded("resubscribe_failed")

	time.Sleep(2 * listener.recoveryWindow)
	if !broadcastListenerIsDegraded(listener) {
		t.Fatal("listener degraded = false; want terminal degraded state")
	}
	if got := observeFirstBroadcastSupervisorState.Value(); got != "degraded" {
		t.Fatalf("supervisor state = %q; want degraded", got)
	}
}

type decodingRouterPlane struct {
	*mockPlane
	decoded map[string]types.Value
	handled bool
	err     error
}

func (plane *decodingRouterPlane) DecodeBroadcast(frame protocol.Frame) (map[string]types.Value, bool, error) {
	return plane.decoded, plane.handled, plane.err
}

func waitForPassiveEvents(t *testing.T, recorder *passiveEventRecorder, timeout time.Duration, ready func([]PassiveTapEvent) bool) []PassiveTapEvent {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		events := recorder.snapshot()
		if ready(events) {
			return events
		}

		select {
		case <-recorder.notify:
		case <-deadline.C:
			t.Fatalf("timeout waiting for passive events; got %#v", events)
		}
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, ready func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

func broadcastListenerIsDegraded(listener *BroadcastListener) bool {
	listener.stateMu.Lock()
	defer listener.stateMu.Unlock()
	return listener.degraded
}

func drainBroadcastEvents(ch <-chan router.BroadcastEvent) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func expvarMapInt64Value(m *expvar.Map, key string) int64 {
	if m == nil {
		return 0
	}
	variable := m.Get(key)
	if variable == nil {
		return 0
	}
	counter, ok := variable.(*expvar.Int)
	if !ok {
		return 0
	}
	return counter.Value()
}

func countPassiveEventKind(events []PassiveTapEvent, kind PassiveTapEventKind) int {
	count := 0
	for _, event := range events {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

func passiveSymbols(events []PassiveTapEvent) []byte {
	out := make([]byte, 0, len(events))
	for _, event := range events {
		if event.Kind == PassiveTapEventSymbol {
			out = append(out, event.Symbol)
		}
	}
	return out
}

func hasPassiveSymbols(events []PassiveTapEvent, want ...byte) bool {
	got := passiveSymbols(events)
	if len(got) < len(want) {
		return false
	}
	index := 0
	for _, symbol := range got {
		if symbol != want[index] {
			continue
		}
		index++
		if index == len(want) {
			return true
		}
	}
	return false
}

func enhReceivedBytes(values []byte) []byte {
	out := make([]byte, 0, len(values)*2)
	for _, value := range values {
		seq := transport.EncodeENH(transport.ENHResReceived, value)
		out = append(out, seq[0], seq[1])
	}
	return out
}

func frameBytes(frame protocol.Frame) []byte {
	raw := make([]byte, 0, 7+len(frame.Data))
	raw = append(raw, frame.Source, frame.Target, frame.Primary, frame.Secondary, byte(len(frame.Data)))
	raw = append(raw, frame.Data...)
	raw = append(raw, protocol.CRC(raw), protocol.SymbolSyn)
	return raw
}

func writePassivePayload(conn net.Conn, payload []byte) {
	defer func() { _ = conn.Close() }()
	_, _ = conn.Write(payload)
}
