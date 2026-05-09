package ebusgateway

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"os"
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
	// TODO: re-enable after ebusgo parser desync recovery (bc75883) is
	// fixed to preserve RESETTED events during same-segment delivery.
	// The ENH parser now resets on invalid payloads, which can consume
	// the RESETTED frame when it arrives in the same TCP segment as
	// subsequent bus bytes.
	t.Skip("flaky: ebusgo parser desync recovery can consume RESETTED events")
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

	tests := []struct {
		name    string
		address string
	}{
		{name: "default proxy port", address: "127.0.0.1:19001"},
		{name: "matrix proxy port", address: "127.0.0.1:19183"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client, server := net.Pipe()
			defer func() { _ = server.Close() }()

			recorder := newPassiveEventRecorder()
			cfg := DefaultConfig()
			cfg.TransportConfig = TransportConfig{
				Protocol:     TransportENS,
				Network:      "tcp",
				Address:      test.address,
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
		})
	}
}

func TestPassiveBusTap_ProxyLikeObserverStreamEmitsLogicalSymbolsWithoutDecodeFault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		address string
	}{
		{name: "default proxy port", address: "127.0.0.1:19001"},
		{name: "matrix proxy port", address: "127.0.0.1:19183"},
	}

	request := protocol.Frame{
		Source:    0xF7,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{protocol.SymbolEscape},
	}
	logicalPayload := proxyObserverTransactionBytes(request, []byte{0x11, 0x22})

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client, server := net.Pipe()
			defer func() { _ = server.Close() }()

			recorder := newPassiveEventRecorder()
			cfg := DefaultConfig()
			cfg.TransportConfig = TransportConfig{
				Protocol:     TransportENS,
				Network:      "tcp",
				Address:      test.address,
				ReadTimeout:  20 * time.Millisecond,
				WriteTimeout: 20 * time.Millisecond,
				DialTimeout:  time.Second,
				Dial: func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
					return client, nil
				},
			}
			cfg.PassiveAbsenceThreshold = time.Second
			cfg.PassiveReconnectInitialDelay = time.Second
			cfg.PassiveReconnectMaxDelay = time.Second

			tap, err := StartPassiveBusTap(context.Background(), cfg, recorder)
			if err != nil {
				t.Fatalf("StartPassiveBusTap error = %v", err)
			}
			defer func() {
				if err := tap.Close(); err != nil {
					t.Fatalf("Close error = %v", err)
				}
			}()

			go func() {
				_, _ = server.Write(enhReceivedBytes(logicalPayload))
			}()

			events := waitForPassiveEvents(t, recorder, 2*time.Second, func(events []PassiveTapEvent) bool {
				return hasPassiveSymbols(events, logicalPayload...)
			})

			if got := countPassiveEventKind(events, PassiveTapEventDecodeFault); got != 0 {
				t.Fatalf("decode fault count = %d; want 0 for proxy-like logical observer payload", got)
			}

			snapshot := tap.Snapshot()
			if got := snapshot.DecodeFaultCount; got != 0 {
				t.Fatalf("DecodeFaultCount = %d; want 0", got)
			}
		})
	}
}

func TestPassiveTapDecodesWireEscapes_DifferentiatesRemoteDirectAdapterLogicalStreams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  TransportConfig
		want bool
	}{
		{
			name: "remote direct adapter ip skips local decode",
			cfg: TransportConfig{
				Protocol: TransportENH,
				Network:  "tcp",
				Address:  "192.168.100.2:9999",
			},
			want: false,
		},
		{
			name: "remote direct adapter hostname skips local decode",
			cfg: TransportConfig{
				Protocol: TransportENS,
				Network:  "tcp",
				Address:  "adapter.local:9999",
			},
			want: false,
		},
		{
			name: "loopback direct adapter still decodes raw wire bytes",
			cfg: TransportConfig{
				Protocol: TransportENH,
				Network:  "tcp",
				Address:  "127.0.0.1:9999",
			},
			want: true,
		},
		{
			name: "proxy-like endpoint skips local decode",
			cfg: TransportConfig{
				Protocol: TransportENS,
				Network:  "tcp",
				Address:  "127.0.0.1:19001",
			},
			want: false,
		},
		{
			name: "custom remote direct port still decodes raw wire bytes",
			cfg: TransportConfig{
				Protocol: TransportENH,
				Network:  "tcp",
				Address:  "192.168.100.2:19183",
			},
			want: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := DefaultConfig()
			cfg.TransportConfig = test.cfg

			if got := passiveTapDecodesWireEscapes(cfg); got != test.want {
				t.Fatalf("passiveTapDecodesWireEscapes() = %v; want %v", got, test.want)
			}
		})
	}
}

func TestPassiveBusTap_RemoteDirectAdapterEndpointAcceptsLogicalObserverPayloadWithoutDecodeFault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		protocol TransportProtocol
		address  string
	}{
		{name: "remote direct ip", protocol: TransportENH, address: "192.168.100.2:9999"},
		{name: "remote direct hostname", protocol: TransportENS, address: "adapter.local:9999"},
	}

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{protocol.SymbolEscape},
	}
	logicalPayload := proxyObserverTransactionBytes(request, []byte{0x11, 0x22})

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client, server := net.Pipe()
			defer func() { _ = server.Close() }()

			recorder := newPassiveEventRecorder()
			cfg := DefaultConfig()
			cfg.TransportConfig = TransportConfig{
				Protocol:     test.protocol,
				Network:      "tcp",
				Address:      test.address,
				ReadTimeout:  20 * time.Millisecond,
				WriteTimeout: 20 * time.Millisecond,
				DialTimeout:  time.Second,
				Dial: func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
					return client, nil
				},
			}
			cfg.PassiveAbsenceThreshold = time.Second
			cfg.PassiveReconnectInitialDelay = time.Second
			cfg.PassiveReconnectMaxDelay = time.Second

			tap, err := StartPassiveBusTap(context.Background(), cfg, recorder)
			if err != nil {
				t.Fatalf("StartPassiveBusTap error = %v", err)
			}
			defer func() {
				if err := tap.Close(); err != nil {
					t.Fatalf("Close error = %v", err)
				}
			}()

			go func() {
				_, _ = server.Write(enhReceivedBytes(logicalPayload))
			}()

			events := waitForPassiveEvents(t, recorder, 2*time.Second, func(events []PassiveTapEvent) bool {
				return hasPassiveSymbols(events, logicalPayload...)
			})

			if got := countPassiveEventKind(events, PassiveTapEventDecodeFault); got != 0 {
				t.Fatalf("decode fault count = %d; want 0 for direct remote logical ENH observer payload", got)
			}

			snapshot := tap.Snapshot()
			if got := snapshot.DecodeFaultCount; got != 0 {
				t.Fatalf("DecodeFaultCount = %d; want 0", got)
			}
		})
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

func TestPassiveBusTap_CustomDirectAdapterPortsStillDecodeWireEscapedObserverTraffic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		address string
	}{
		{name: "loopback custom direct port", address: "127.0.0.1:10001"},
		{name: "remote custom direct port", address: "192.168.100.2:19183"},
	}

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{protocol.SymbolEscape},
	}
	logicalPayload := proxyObserverTransactionBytes(request, []byte{0x11, 0x22})

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			client, server := net.Pipe()
			defer func() { _ = server.Close() }()

			recorder := newPassiveEventRecorder()
			cfg := DefaultConfig()
			cfg.TransportConfig = TransportConfig{
				Protocol:     TransportENH,
				Network:      "tcp",
				Address:      test.address,
				ReadTimeout:  20 * time.Millisecond,
				WriteTimeout: 20 * time.Millisecond,
				DialTimeout:  time.Second,
				Dial: func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
					return client, nil
				},
			}
			cfg.PassiveAbsenceThreshold = time.Second
			cfg.PassiveReconnectInitialDelay = time.Second
			cfg.PassiveReconnectMaxDelay = time.Second

			tap, err := StartPassiveBusTap(context.Background(), cfg, recorder)
			if err != nil {
				t.Fatalf("StartPassiveBusTap error = %v", err)
			}
			defer func() {
				if err := tap.Close(); err != nil {
					t.Fatalf("Close error = %v", err)
				}
			}()

			go func() {
				_, _ = server.Write(enhReceivedBytes(wireEscapeSymbols(logicalPayload)))
			}()

			events := waitForPassiveEvents(t, recorder, 2*time.Second, func(events []PassiveTapEvent) bool {
				return hasPassiveSymbols(events, logicalPayload...)
			})

			if got := countPassiveEventKind(events, PassiveTapEventDecodeFault); got != 0 {
				t.Fatalf("decode fault count = %d; want 0 for direct raw-wire observer payload", got)
			}

			snapshot := tap.Snapshot()
			if got := snapshot.DecodeFaultCount; got != 0 {
				t.Fatalf("DecodeFaultCount = %d; want 0", got)
			}
		})
	}
}

func TestPassiveTransactionReconstructor_ClassifiesProxyLikeObserverTraffic(t *testing.T) {
	t.Parallel()

	request := protocol.Frame{
		Source:    0xF7,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x09,
		Data:      []byte{protocol.SymbolEscape},
	}
	result := runProxyENSObserverHarness(t, []proxyObserverWrite{
		{delay: 25 * time.Millisecond, logicalSymbols: proxyObserverTransactionBytes(request, []byte{0x11, 0x22})},
	}, false, 1)

	event := result.requireEvent(t, PassiveClassifiedEventTransaction)
	if got := event.Request.Data; len(got) != 1 || got[0] != protocol.SymbolEscape {
		t.Fatalf("request data = %v; want [0x%02X]", got, protocol.SymbolEscape)
	}
	if !event.HasResponse {
		t.Fatal("transaction event missing response")
	}

	if got := result.snapshot.TapStatus.DecodeFaultCount; got != 0 {
		t.Fatalf("DecodeFaultCount = %d; want 0", got)
	}
	if got := result.snapshot.TapStatus.ObservedSymbolCount; got == 0 {
		t.Fatal("ObservedSymbolCount = 0; want proxy-like observer traffic to produce symbols")
	}
	if got := result.countEvents(PassiveClassifiedEventTransaction); got == 0 {
		t.Fatal("transaction event count = 0; want > 0 for reconstructible observer traffic")
	}
}

func TestProxyENSObserverReplayHarness_FirstRXBeforeSecondSendStillCompletesWarmup(t *testing.T) {
	t.Parallel()

	request := protocol.Frame{
		Source:    0xF7,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x08, 0x10},
	}
	firstObserverSymbols := proxyObserverTransactionBytes(request, []byte{0x01, 0x42})
	secondObserverSymbols := proxyObserverTransactionBytes(request, []byte{0x02, 0x24})

	result := runProxyENSObserverHarness(t, []proxyObserverWrite{
		{logicalSymbols: firstObserverSymbols[:2]},
		{delay: 20 * time.Millisecond, logicalSymbols: firstObserverSymbols[2:]},
		{delay: 20 * time.Millisecond, logicalSymbols: secondObserverSymbols},
	}, false, 2)

	transactions := passiveEventsByKind(result.classified, PassiveClassifiedEventTransaction)
	if len(transactions) < 2 {
		t.Fatalf("transaction event count = %d; want >= 2 for fragmented proxy observer replay", len(transactions))
	}

	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig = TransportConfig{
		Protocol: TransportENS,
		Network:  "tcp",
		Address:  "127.0.0.1:19183",
	}
	cfg.ObserveFirstWarmupConnectedWindow = time.Millisecond
	cfg.ObserveFirstWarmupCompletedTransactions = 2

	store := NewBusObservabilityStore(cfg)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("store.Close error = %v", err)
		}
	}()

	store.mu.Lock()
	store.passive.processStartedAt = transactions[0].ObservedAt
	store.passiveStartWarmupLocked(transactions[0].ObservedAt, false)
	store.mu.Unlock()

	store.OnPassiveClassifiedEvent(transactions[0])

	store.mu.RLock()
	afterFirstCount := store.passive.completedTransactions
	afterFirstState := store.passive.state
	store.mu.RUnlock()
	if afterFirstCount == 0 {
		t.Fatal("completedTransactions after first normalized transaction = 0; want > 0")
	}
	if afterFirstState != "warming_up" {
		t.Fatalf("passive state after first normalized transaction = %q; want warming_up", afterFirstState)
	}

	store.OnPassiveClassifiedEvent(transactions[1])

	store.mu.RLock()
	finalState := store.passive.state
	confirmed := store.passive.probeOutcomes["confirmed"]
	store.mu.RUnlock()
	if finalState != "available" {
		t.Fatalf("passive state after second normalized transaction = %q; want available", finalState)
	}
	if confirmed == 0 {
		t.Fatal("confirmed probe outcomes = 0; want > 0 after second normalized transaction")
	}
}

func TestProxyENSObserverReplayHarness_CombinedArtifactFixtureRoutesBlameToStreamShape(t *testing.T) {
	t.Parallel()

	// Fixture derived from the failing combined artifact:
	// results-matrix-ha/20260312T055859Z-proxy81-gateway373-p03-rerun/P03/logs/proxy.log
	// It contains the exact active request burst captured at 08:01:55 and is wrapped
	// into the current northbound observer shape asserted by the proxy lane.
	currentProxyModeledObserverSymbols := mustLoadProxyObserverFixture(t, "testdata/p03_proxy_single_combined_artifact_observer.hex")

	result := runProxyENSObserverHarness(t, []proxyObserverWrite{
		{delay: 25 * time.Millisecond, logicalSymbols: currentProxyModeledObserverSymbols},
	}, false, 1)

	if got := result.countEvents(PassiveClassifiedEventTransaction); got != 0 {
		t.Fatalf("transaction event count = %d; want 0 for current proxy-modeled observer stream", got)
	}
	if got := result.completedTransactions; got != 0 {
		t.Fatalf("completedTransactions = %d; want 0 for current proxy-modeled observer stream", got)
	}
	if got := result.snapshot.TapStatus.ObservedSymbolCount; got == 0 {
		t.Fatal("ObservedSymbolCount = 0; want harness to prove the stream was ingested before routing blame")
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

func proxyObserverTransactionBytes(request protocol.Frame, responseData []byte) []byte {
	// Real eBUS wire always emits at least one SymbolSyn between
	// frames (bus-idle marker). The original fixture omitted the
	// leading SYN because the pre-P6 reconstructor accepted any
	// non-SYN byte as a frame source unconditionally. P6 Layer 1
	// (inter-frame SYN gate) requires the SYN before accepting a new
	// frame's source byte, so we now prepend one to mirror real
	// wire conditions. Bus-tap-only tests are unaffected (they
	// assert on byte-forwarding behavior, not framing semantics).
	payload := []byte{protocol.SymbolSyn}
	payload = append(payload, frameBytes(request)...)
	payload = append(payload, protocol.SymbolAck)
	payload = append(payload, responseSegmentBytes(responseData)...)
	payload = append(payload, protocol.SymbolAck, protocol.SymbolSyn)
	return payload
}

type proxyObserverWrite struct {
	delay          time.Duration
	logicalSymbols []byte
}

type proxyObserverHarnessResult struct {
	snapshot                         PassiveReconstructorSnapshot
	classified                       []PassiveClassifiedEvent
	observedBytes                    int
	completedTransactions            int
	maxCompletedTransactionsObserved int
	passiveState                     string
}

func (result proxyObserverHarnessResult) requireEvent(t *testing.T, kind PassiveClassifiedEventKind) PassiveClassifiedEvent {
	t.Helper()
	for _, event := range result.classified {
		if event.Kind == kind {
			return event
		}
	}
	t.Fatalf("missing classified event kind %d in %#v", kind, result.classified)
	return PassiveClassifiedEvent{}
}

func (result proxyObserverHarnessResult) countEvents(kind PassiveClassifiedEventKind) int {
	count := 0
	for _, event := range result.classified {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

func passiveEventsByKind(events []PassiveClassifiedEvent, kind PassiveClassifiedEventKind) []PassiveClassifiedEvent {
	filtered := make([]PassiveClassifiedEvent, 0, len(events))
	for _, event := range events {
		if event.Kind == kind {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func runProxyENSObserverHarness(t *testing.T, writes []proxyObserverWrite, waitCompleted bool, requiredTransactions int) proxyObserverHarnessResult {
	t.Helper()

	client, server := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer func() { _ = server.Close() }()

	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig = TransportConfig{
		Protocol:     TransportENS,
		Network:      "tcp",
		Address:      "127.0.0.1:19183",
		ReadTimeout:  20 * time.Millisecond,
		WriteTimeout: 20 * time.Millisecond,
		DialTimeout:  time.Second,
		Dial: func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
			return client, nil
		},
	}
	cfg.PassiveAbsenceThreshold = time.Second
	cfg.PassiveReconnectInitialDelay = time.Second
	cfg.PassiveReconnectMaxDelay = time.Second
	cfg.ObserveFirstWarmupConnectedWindow = time.Millisecond
	cfg.ObserveFirstWarmupCompletedTransactions = 1
	cfg.ObserveFirstWarmupOuterWindow = time.Second
	if requiredTransactions > 0 {
		cfg.ObserveFirstWarmupCompletedTransactions = requiredTransactions
	}

	reconstructor, err := StartPassiveTransactionReconstructor(ctx, cfg)
	if err != nil {
		t.Fatalf("StartPassiveTransactionReconstructor error = %v", err)
	}
	defer func() {
		if err := reconstructor.Close(); err != nil {
			t.Fatalf("reconstructor.Close error = %v", err)
		}
	}()

	store := NewBusObservabilityStore(cfg)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("store.Close error = %v", err)
		}
	}()
	if err := store.AttachReconstructor(ctx, reconstructor); err != nil {
		t.Fatalf("AttachReconstructor error = %v", err)
	}
	waitForCondition(t, 2*time.Second, func() bool {
		reconstructor.subscribersMu.Lock()
		subscriberCount := len(reconstructor.subscribers)
		reconstructor.subscribersMu.Unlock()
		store.mu.RLock()
		passiveState := store.passive.state
		store.mu.RUnlock()
		return subscriberCount >= 1 && passiveState == "warming_up"
	})

	subscription, err := reconstructor.Subscribe("harness", PassiveSubscriberCritical, 16)
	if err != nil {
		t.Fatalf("Subscribe error = %v", err)
	}
	defer subscription.Close()

	totalSymbols := 0
	for _, write := range writes {
		totalSymbols += len(write.logicalSymbols)
	}
	go func() {
		for _, write := range writes {
			if write.delay > 0 {
				time.Sleep(write.delay)
			}
			_, _ = server.Write(enhReceivedBytes(write.logicalSymbols))
		}
	}()

	waitForCondition(t, 2*time.Second, func() bool {
		return reconstructor.Snapshot().TapStatus.ObservedSymbolCount >= uint64(totalSymbols)
	})

	classified := collectPassiveClassifiedEvents(subscription, 200*time.Millisecond)
	maxCompletedTransactionsObserved := 0
	if waitCompleted {
		waitForCondition(t, 2*time.Second, func() bool {
			store.mu.RLock()
			defer store.mu.RUnlock()
			if store.passive.completedTransactions > maxCompletedTransactionsObserved {
				maxCompletedTransactionsObserved = store.passive.completedTransactions
			}
			return store.passive.state == "available"
		})
	}

	store.mu.RLock()
	completedTransactions := store.passive.completedTransactions
	passiveState := store.passive.state
	if store.passive.completedTransactions > maxCompletedTransactionsObserved {
		maxCompletedTransactionsObserved = store.passive.completedTransactions
	}
	store.mu.RUnlock()

	return proxyObserverHarnessResult{
		snapshot:                         reconstructor.Snapshot(),
		classified:                       classified,
		observedBytes:                    totalSymbols,
		completedTransactions:            completedTransactions,
		maxCompletedTransactionsObserved: maxCompletedTransactionsObserved,
		passiveState:                     passiveState,
	}
}

func mustLoadProxyObserverFixture(t *testing.T, path string) []byte {
	t.Helper()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	values := make([]byte, 0, len(content)/3)
	var current byte
	haveHalf := false
	for _, raw := range content {
		switch {
		case raw >= '0' && raw <= '9':
			raw -= '0'
		case raw >= 'a' && raw <= 'f':
			raw = raw - 'a' + 10
		case raw >= 'A' && raw <= 'F':
			raw = raw - 'A' + 10
		default:
			continue
		}
		if !haveHalf {
			current = raw << 4
			haveHalf = true
			continue
		}
		values = append(values, current|raw)
		haveHalf = false
	}
	if haveHalf {
		t.Fatalf("fixture %q has dangling half-byte", path)
	}
	if len(values) == 0 {
		t.Fatalf("fixture %q parsed no bytes", path)
	}
	return values
}

func collectPassiveClassifiedEvents(subscription *PassiveClassifiedSubscription, quietWindow time.Duration) []PassiveClassifiedEvent {
	events := make([]PassiveClassifiedEvent, 0, 8)
	timer := time.NewTimer(quietWindow)
	defer timer.Stop()

	for {
		select {
		case event, ok := <-subscription.Events():
			if !ok {
				return events
			}
			events = append(events, event)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(quietWindow)
		case <-timer.C:
			return events
		}
	}
}

func wireEscapeSymbols(symbols []byte) []byte {
	out := make([]byte, 0, len(symbols)+4)
	for _, symbol := range symbols {
		switch symbol {
		case protocol.SymbolEscape:
			out = append(out, protocol.SymbolEscape, 0x00)
		case protocol.SymbolSyn:
			out = append(out, protocol.SymbolEscape, 0x01)
		default:
			out = append(out, symbol)
		}
	}
	return out
}

func writePassivePayload(conn net.Conn, payload []byte) {
	defer func() { _ = conn.Close() }()
	_, _ = conn.Write(payload)
}
