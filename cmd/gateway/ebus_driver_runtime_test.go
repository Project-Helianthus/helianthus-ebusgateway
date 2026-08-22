package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

type issue851BlockingRawTransport struct {
	readStarted chan struct{}
	readResult  chan issue851ReadResult
	closed      chan struct{}
	closeOnce   sync.Once
}

type issue851ReadResult struct {
	value byte
	err   error
}

type issue851EnhancedArbitrationRaw struct {
	readStarted        chan struct{}
	readResult         chan transport.StreamEvent
	arbitrationEntered chan struct{}
	arbitrationRelease chan struct{}
	closed             chan struct{}
	closeOnce          sync.Once
}

type issue851TerminalPassiveRaw struct {
	err       error
	closed    chan struct{}
	closeOnce sync.Once
}

type issue851ReadSignalingRaw struct {
	transport.RawTransport
	readStarted chan struct{}
	readOnce    sync.Once
}

type issue851CancelBeforeByteRaw struct {
	cancel context.CancelFunc
	value  byte
}

type issue851ProxyLifecycle struct {
	ready  atomic.Bool
	fences atomic.Int32
}

type issue851OneShotPassiveRaw struct {
	closed          atomic.Bool
	closeCalls      atomic.Int32
	reads           atomic.Int32
	readsAfterClose atomic.Int32
}

func (raw *issue851OneShotPassiveRaw) ReadByte() (byte, error) {
	raw.reads.Add(1)
	if raw.closed.Load() {
		raw.readsAfterClose.Add(1)
		return 0, ebuserrors.ErrTransportClosed
	}
	return 0x55, nil
}

func (*issue851OneShotPassiveRaw) Write([]byte) (int, error) {
	return 0, transport.ErrDriverUnavailable
}

func (raw *issue851OneShotPassiveRaw) Close() error {
	raw.closeCalls.Add(1)
	raw.closed.Store(true)
	return nil
}

func newIssue851ProxyLifecycle() *issue851ProxyLifecycle {
	lifecycle := &issue851ProxyLifecycle{}
	lifecycle.ready.Store(true)
	return lifecycle
}

func (lifecycle *issue851ProxyLifecycle) FenceManagedConnection() {
	lifecycle.fences.Add(1)
	lifecycle.ready.Store(false)
}

func (lifecycle *issue851ProxyLifecycle) ManagedConnectionReady() bool {
	return lifecycle != nil && lifecycle.ready.Load()
}

func newIssue851AdapterServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake adapter: %v", err)
	}
	var connectionMu sync.Mutex
	connections := make(map[net.Conn]struct{})
	var connectionWG sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			connectionMu.Lock()
			connections[connection] = struct{}{}
			connectionMu.Unlock()
			connectionWG.Add(1)
			go func() {
				defer connectionWG.Done()
				defer func() {
					connectionMu.Lock()
					delete(connections, connection)
					connectionMu.Unlock()
					_ = connection.Close()
				}()
				request := make([]byte, 2)
				if _, readErr := io.ReadFull(connection, request); readErr != nil {
					return
				}
				response := transport.EncodeENH(transport.ENHResResetted, 0x01)
				if _, writeErr := connection.Write(response[:]); writeErr != nil {
					return
				}
				// adaptermux always probes INFO version once during startup. A
				// one-byte payload is deliberately non-parseable, which proves the
				// request path without triggering the optional paced INFO sweep.
				infoRequest := make([]byte, 2)
				if _, readErr := io.ReadFull(connection, infoRequest); readErr != nil {
					return
				}
				infoLength := transport.EncodeENH(transport.ENHResInfo, 0x01)
				infoValue := transport.EncodeENH(transport.ENHResInfo, 0x00)
				infoResponse := append(infoLength[:], infoValue[:]...)
				if _, writeErr := connection.Write(infoResponse); writeErr != nil {
					return
				}
				_, _ = io.Copy(io.Discard, connection)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		connectionMu.Lock()
		for connection := range connections {
			_ = connection.Close()
		}
		connectionMu.Unlock()
		connectionWG.Wait()
	})
	return listener.Addr().String()
}

func (raw issue851CancelBeforeByteRaw) ReadByte() (byte, error) {
	// Make generation withdrawal and the old byte ready in the same pump
	// turn. A select-only fence can leak the byte; post-receive cancellation
	// dominance must discard it deterministically.
	raw.cancel()
	return raw.value, nil
}

func (issue851CancelBeforeByteRaw) Write(data []byte) (int, error) { return len(data), nil }
func (issue851CancelBeforeByteRaw) Close() error                   { return nil }

func (raw *issue851ReadSignalingRaw) ReadByte() (byte, error) {
	raw.readOnce.Do(func() { close(raw.readStarted) })
	return raw.RawTransport.ReadByte()
}

func newIssue851TerminalPassiveRaw(err error) *issue851TerminalPassiveRaw {
	return &issue851TerminalPassiveRaw{err: err, closed: make(chan struct{})}
}

func (raw *issue851TerminalPassiveRaw) ReadByte() (byte, error) {
	select {
	case <-raw.closed:
		return 0, ebuserrors.ErrTransportClosed
	default:
		return 0, raw.err
	}
}

func (raw *issue851TerminalPassiveRaw) ReadEvent() (transport.StreamEvent, error) {
	_, err := raw.ReadByte()
	return transport.StreamEvent{}, err
}

func (*issue851TerminalPassiveRaw) Write([]byte) (int, error) {
	return 0, transport.ErrDriverUnavailable
}

func (raw *issue851TerminalPassiveRaw) Close() error {
	raw.closeOnce.Do(func() { close(raw.closed) })
	return nil
}

type issue851Classifier struct {
	v8 *v8classifier.Classifier
}

func (*issue851Classifier) LastTxnClass() string { return "" }
func (classifier *issue851Classifier) V8Classifier() *v8classifier.Classifier {
	return classifier.v8
}

func newIssue851BlockingRawTransport() *issue851BlockingRawTransport {
	return &issue851BlockingRawTransport{
		readStarted: make(chan struct{}, 1),
		readResult:  make(chan issue851ReadResult),
		closed:      make(chan struct{}),
	}
}

func newIssue851EnhancedArbitrationRaw() *issue851EnhancedArbitrationRaw {
	return &issue851EnhancedArbitrationRaw{
		readStarted:        make(chan struct{}, 1),
		readResult:         make(chan transport.StreamEvent),
		arbitrationEntered: make(chan struct{}),
		arbitrationRelease: make(chan struct{}),
		closed:             make(chan struct{}),
	}
}

func (raw *issue851BlockingRawTransport) ReadByte() (byte, error) {
	select {
	case raw.readStarted <- struct{}{}:
	default:
	}
	select {
	case result := <-raw.readResult:
		return result.value, result.err
	case <-raw.closed:
		return 0, ebuserrors.ErrTransportClosed
	}
}

func (*issue851BlockingRawTransport) Write(data []byte) (int, error) { return len(data), nil }

func (raw *issue851BlockingRawTransport) Close() error {
	raw.closeOnce.Do(func() { close(raw.closed) })
	return nil
}

func (raw *issue851EnhancedArbitrationRaw) ReadEvent() (transport.StreamEvent, error) {
	select {
	case raw.readStarted <- struct{}{}:
	default:
	}
	select {
	case event := <-raw.readResult:
		return event, nil
	case <-raw.closed:
		return transport.StreamEvent{}, ebuserrors.ErrTransportClosed
	}
}

func (raw *issue851EnhancedArbitrationRaw) ReadByte() (byte, error) {
	event, err := raw.ReadEvent()
	return event.Byte, err
}

func (*issue851EnhancedArbitrationRaw) Write(data []byte) (int, error) { return len(data), nil }

func (raw *issue851EnhancedArbitrationRaw) StartArbitration(byte) error {
	close(raw.arbitrationEntered)
	select {
	case <-raw.arbitrationRelease:
		return nil
	case <-raw.closed:
		return ebuserrors.ErrTransportClosed
	}
}

func (raw *issue851EnhancedArbitrationRaw) Close() error {
	raw.closeOnce.Do(func() { close(raw.closed) })
	return nil
}

func TestIssue851ManagedReadLoopDrainsBeforeCloseAndReplaceFencesOldGeneration(t *testing.T) {
	var mu sync.Mutex
	var generations []*issue851BlockingRawTransport
	factory := func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		raw := newIssue851BlockingRawTransport()
		mu.Lock()
		generations = append(generations, raw)
		mu.Unlock()
		return newManagedEBusGeneration(ctx, raw, raw.Close), nil
	}
	runtime := transport.NewDriverRuntime(factory, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
	slot := newEBusStableTransport(runtime, ebusgateway.TransportTCPPlain, nil)

	if _, err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start generation 1: %v", err)
	}
	mu.Lock()
	first := generations[0]
	mu.Unlock()
	readDone := make(chan error, 1)
	go func() {
		_, err := slot.ReadByte()
		readDone <- err
	}()
	select {
	case <-first.readStarted:
	case <-time.After(time.Second):
		t.Fatal("generation 1 adapter-owned read pump did not start")
	}

	if generation, err := runtime.Replace(context.Background()); err != nil {
		t.Fatalf("Replace() error = %v", err)
	} else if generation != 2 {
		t.Fatalf("Replace() generation = %d, want 2", generation)
	}
	if runtime.SafetyQuarantined() {
		t.Fatal("normal blocking read loop caused CLOSE_UNCONFIRMED quarantine")
	}
	select {
	case err := <-readDone:
		if !errors.Is(err, ebuserrors.ErrTransportClosed) && !errors.Is(err, context.Canceled) {
			t.Fatalf("old generation ReadByte error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("old generation stable-slot reader was not fenced")
	}
	select {
	case <-first.closed:
	case <-time.After(time.Second):
		t.Fatal("old raw transport did not close after admitted reader drained")
	}

	mu.Lock()
	second := generations[1]
	mu.Unlock()
	newReadDone := make(chan issue851ReadResult, 1)
	go func() {
		value, err := slot.ReadByte()
		newReadDone <- issue851ReadResult{value: value, err: err}
	}()
	select {
	case <-second.readStarted:
	case <-time.After(time.Second):
		t.Fatal("generation 2 read demand did not reach raw transport")
	}
	second.readResult <- issue851ReadResult{value: 0x5a}
	if result := <-newReadDone; result.err != nil || result.value != 0x5a {
		value, err := result.value, result.err
		t.Fatalf("new generation ReadByte = (0x%02X, %v), want (0x5A, nil)", value, err)
	}
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("Stop generation 2: %v", err)
	}
}

func TestIssue851EbusdActiveReadRetiresWithTransportLevelClose(t *testing.T) {
	for _, action := range []string{"stop", "replace"} {
		t.Run(action, func(t *testing.T) {
			cfg := ebusgateway.DefaultConfig()
			cfg.BroadcastListen = false
			cfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
			cfg.TransportConfig.Network = "tcp"
			cfg.TransportConfig.Address = "127.0.0.1:8888"
			var mu sync.Mutex
			var servers []net.Conn
			var raws []*issue851ReadSignalingRaw
			cfg.TransportConfig.Dial = func(context.Context, string, string, time.Duration) (net.Conn, error) {
				client, server := net.Pipe()
				mu.Lock()
				servers = append(servers, server)
				mu.Unlock()
				return client, nil
			}
			t.Cleanup(func() {
				mu.Lock()
				defer mu.Unlock()
				for _, server := range servers {
					_ = server.Close()
				}
			})
			factory := func(ctx context.Context) (*transport.ManagedRawTransport, error) {
				active, activeClose, err := ebusgateway.OpenEBusDriverTransport(ctx, cfg)
				if err != nil {
					return nil, err
				}
				raw := &issue851ReadSignalingRaw{RawTransport: active, readStarted: make(chan struct{})}
				mu.Lock()
				raws = append(raws, raw)
				mu.Unlock()
				return newManagedEBusGeneration(ctx, raw, activeClose), nil
			}
			runtime := transport.NewDriverRuntime(factory, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
			stable := newEBusStableTransport(runtime, ebusgateway.TransportEbusdTCP, nil)
			if _, err := runtime.Start(context.Background()); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			mu.Lock()
			first := raws[0]
			mu.Unlock()
			readDone := make(chan error, 1)
			go func() {
				_, err := stable.ReadByte()
				readDone <- err
			}()
			select {
			case <-first.readStarted:
			case <-time.After(time.Second):
				t.Fatal("ebusd-tcp read did not enter the transport-local wait")
			}

			switch action {
			case "stop":
				if err := runtime.Stop(context.Background()); err != nil {
					t.Fatalf("Stop() error = %v", err)
				}
			case "replace":
				if generation, err := runtime.Replace(context.Background()); err != nil {
					t.Fatalf("Replace() error = %v", err)
				} else if generation != 2 {
					t.Fatalf("Replace() generation = %d, want 2", generation)
				}
				if err := runtime.Stop(context.Background()); err != nil {
					t.Fatalf("Stop replacement error = %v", err)
				}
			}
			if runtime.SafetyQuarantined() {
				t.Fatal("ordinary ebusd-tcp retirement entered safety quarantine")
			}
			select {
			case err := <-readDone:
				if !errors.Is(err, ebuserrors.ErrTransportClosed) && !errors.Is(err, context.Canceled) {
					t.Fatalf("retired ebusd-tcp read error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("transport-level close did not wake ebusd-tcp reader")
			}
		})
	}
}

func TestIssue851CanceledGenerationDominatesConcurrentlyReadyOldByte(t *testing.T) {
	for iteration := 0; iteration < 200; iteration++ {
		generationCtx, cancelGeneration := context.WithCancel(context.Background())
		closing := &atomic.Bool{}
		endpoint := newManagedEBusEndpoint(
			generationCtx,
			issue851CancelBeforeByteRaw{cancel: cancelGeneration, value: 0x5a},
			closing,
		)
		resultCh := make(chan issue851ReadResult, 1)
		go func() {
			value, _, err := endpoint.nextByte()
			resultCh <- issue851ReadResult{value: value, err: err}
		}()

		select {
		case result := <-resultCh:
			if result.err == nil || result.value == 0x5a {
				t.Fatalf("iteration %d returned retired-generation byte: %#v", iteration, result)
			}
			if !errors.Is(result.err, ebuserrors.ErrTransportClosed) {
				t.Fatalf("iteration %d error = %v, want transport closed", iteration, result.err)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d canceled read did not return", iteration)
		}
		select {
		case <-endpoint.pumpDone:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d read pump did not retire", iteration)
		}
	}
}

func TestIssue851EnhancedReadPumpWaitsForDemandAndDoesNotStealArbitration(t *testing.T) {
	raw := newIssue851EnhancedArbitrationRaw()
	runtime := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		return newManagedEBusGeneration(ctx, raw, raw.Close), nil
	}, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
	slot := newEBusStableTransport(runtime, ebusgateway.TransportENH, nil)
	if _, err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Stable-slot Close belongs to the process shell, not a driver generation.
	if err := slot.Close(); err != nil {
		t.Fatalf("stable slot Close() error = %v", err)
	}
	select {
	case <-raw.closed:
		t.Fatal("stable-slot Close retired the active generation")
	default:
	}

	// The adapter-owned pump exists, but it must not prefetch. In particular,
	// it must not hold the ENH/ENS read path while START arbitration consumes
	// its own control response.
	select {
	case <-raw.readStarted:
		t.Fatal("read pump touched raw transport without protocol demand")
	case <-time.After(25 * time.Millisecond):
	}
	starter := slot.(interface{ StartArbitration(byte) error })
	arbitrationDone := make(chan error, 1)
	go func() { arbitrationDone <- starter.StartArbitration(0x31) }()
	select {
	case <-raw.arbitrationEntered:
	case <-time.After(time.Second):
		t.Fatal("StartArbitration did not reach enhanced transport")
	}
	select {
	case <-raw.readStarted:
		t.Fatal("read pump raced an active START arbitration")
	case <-time.After(25 * time.Millisecond):
	}
	close(raw.arbitrationRelease)
	if err := <-arbitrationDone; err != nil {
		t.Fatalf("StartArbitration() error = %v", err)
	}

	readDone := make(chan issue851ReadResult, 1)
	go func() {
		value, err := slot.ReadByte()
		readDone <- issue851ReadResult{value: value, err: err}
	}()
	select {
	case <-raw.readStarted:
	case <-time.After(time.Second):
		t.Fatal("explicit ReadByte demand did not reach enhanced transport")
	}
	raw.readResult <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x5a}
	if result := <-readDone; result.err != nil || result.value != 0x5a {
		t.Fatalf("ReadByte() = (0x%02X, %v), want (0x5A, nil)", result.value, result.err)
	}
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestIssue851StableTransportShapeMatchesConfiguredFamily(t *testing.T) {
	runtime := transport.NewDriverRuntime(nil, transport.DriverRuntimeConfig{})

	for _, protocol := range []ebusgateway.TransportProtocol{ebusgateway.TransportENH, ebusgateway.TransportENS} {
		slot := newEBusStableTransport(runtime, protocol, nil)
		passive := newEBusStablePassiveTransport(directEBusSlotInvoker{runtime: runtime}, protocol, nil)
		if _, ok := slot.(transport.EscapeAware); !ok {
			t.Errorf("%s slot does not preserve EscapeAware", protocol)
		}
		if _, ok := slot.(transport.EscapeFlaggedReader); !ok {
			t.Errorf("%s slot does not preserve EscapeFlaggedReader", protocol)
		}
		if _, ok := slot.(interface{ StartArbitration(byte) error }); !ok {
			t.Errorf("%s slot does not preserve enhanced arbitration", protocol)
		}
		if _, ok := passive.(transport.EscapeAware); !ok {
			t.Errorf("%s passive slot does not preserve EscapeAware", protocol)
		}
		if _, ok := passive.(transport.StreamEventReader); !ok {
			t.Errorf("%s passive slot does not preserve StreamEventReader", protocol)
		}
	}

	for _, protocol := range []ebusgateway.TransportProtocol{ebusgateway.TransportTCPPlain, ebusgateway.TransportUDPPlain, ebusgateway.TransportEbusdTCP} {
		slot := newEBusStableTransport(runtime, protocol, nil)
		passive := newEBusStablePassiveTransport(directEBusSlotInvoker{runtime: runtime}, protocol, nil)
		if _, ok := slot.(transport.EscapeAware); ok {
			t.Errorf("%s slot unexpectedly advertises EscapeAware", protocol)
		}
		if _, ok := slot.(transport.EscapeFlaggedReader); ok {
			t.Errorf("%s slot unexpectedly advertises EscapeFlaggedReader", protocol)
		}
		if _, ok := slot.(interface{ StartArbitration(byte) error }); ok {
			t.Errorf("%s slot unexpectedly advertises enhanced arbitration", protocol)
		}
		if _, ok := passive.(transport.EscapeAware); ok {
			t.Errorf("%s passive slot unexpectedly advertises EscapeAware", protocol)
		}
		if _, ok := passive.(transport.StreamEventReader); ok {
			t.Errorf("%s passive slot unexpectedly advertises StreamEventReader", protocol)
		}
	}
}

func TestIssue851TransportURIOverrideSelectsPlainStableShape(t *testing.T) {
	for _, endpoint := range []string{
		"tcp-plain://127.0.0.1:9999",
		"udp-plain://127.0.0.1:9999",
		"ebusd://127.0.0.1:8888",
	} {
		cfg := ebusgateway.DefaultConfig()
		cfg.TransportConfig.Protocol = ebusgateway.TransportENH
		cfg.TransportConfig.Address = endpoint
		protocol := configuredEBusDriverProtocol(cfg)
		slot := newEBusStableTransport(transport.NewDriverRuntime(nil, transport.DriverRuntimeConfig{}), protocol, nil)
		if _, ok := slot.(transport.EscapeAware); ok {
			t.Errorf("endpoint %q resolved protocol %q with enhanced EscapeAware shape", endpoint, protocol)
		}
		if _, ok := slot.(interface{ StartArbitration(byte) error }); ok {
			t.Errorf("endpoint %q resolved protocol %q with enhanced arbitration shape", endpoint, protocol)
		}
	}
}

func TestIssue851ActiveCallbackTimeoutQuarantinesWithoutClosingRawTransport(t *testing.T) {
	raw := newIssue851BlockingRawTransport()
	runtime := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		return newManagedEBusGeneration(ctx, raw, raw.Close), nil
	}, transport.DriverRuntimeConfig{DrainTimeout: 20 * time.Millisecond})
	if _, err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	invokeDone := make(chan error, 1)
	go func() {
		invokeDone <- runtime.Invoke(context.Background(), func(transport.RawTransport) error {
			close(callbackEntered)
			<-callbackRelease
			return nil
		})
	}()
	<-callbackEntered

	if err := runtime.Stop(context.Background()); !errors.Is(err, transport.ErrDriverSafetyQuarantined) {
		t.Fatalf("Stop() error = %v, want ErrDriverSafetyQuarantined", err)
	}
	select {
	case <-raw.closed:
		t.Fatal("raw transport closed beneath an active admitted callback")
	default:
	}
	close(callbackRelease)
	if err := <-invokeDone; err != nil {
		t.Fatalf("admitted callback error = %v", err)
	}
	_ = raw.Close()
}

func TestIssue851ManagerStopReachesRuntimeWithIgnoringContextFactory(t *testing.T) {
	factoryStarted := make(chan struct{})
	factoryRelease := make(chan struct{})
	raw := newIssue851BlockingRawTransport()
	runtimeSeam := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		close(factoryStarted)
		<-factoryRelease // deliberately ignores ctx
		return newManagedEBusGeneration(ctx, raw, raw.Close), nil
	}, transport.DriverRuntimeConfig{DrainTimeout: 20 * time.Millisecond})
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID:           primaryEBusDriverID,
		Enabled:      true,
		Runtime:      &ebusDriverRuntime{runtime: runtimeSeam},
		Capabilities: []drivermanager.Capability{drivermanager.CapabilityRead},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	startDone := make(chan error, 1)
	go func() { startDone <- manager.Start(context.Background(), primaryEBusDriverID) }()
	select {
	case <-factoryStarted:
	case <-time.After(time.Second):
		t.Fatal("factory did not start")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- manager.Stop(context.Background(), primaryEBusDriverID) }()
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Stop could not reach DriverRuntime bounded cancellation")
	}
	snapshot, _ := manager.Snapshot(primaryEBusDriverID)
	if !snapshot.SafetyQuarantined || snapshot.Reason.Code != drivermanager.ReasonCloseUnconfirmed {
		t.Fatalf("snapshot after uncooperative factory = %#v", snapshot)
	}
	close(factoryRelease)
	select {
	case err := <-startDone:
		if err != nil {
			t.Fatalf("Start() returned manager error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not retire canceled construction after release")
	}
}

func TestIssue851PassiveCloseInterruptsReadWithoutOwningGeneration(t *testing.T) {
	active := newIssue851BlockingRawTransport()
	passive := newIssue851BlockingRawTransport()
	runtime := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		return newManagedEBusGenerationParts(ctx, active, passive, nil, func() error {
			return errors.Join(active.Close(), passive.Close())
		}), nil
	}, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
	if _, err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	slot := newEBusStablePassiveTransport(directEBusSlotInvoker{runtime: runtime}, ebusgateway.TransportENH, nil)
	readDone := make(chan error, 1)
	go func() {
		_, err := slot.(transport.StreamEventReader).ReadEvent()
		readDone <- err
	}()
	select {
	case <-passive.readStarted:
	case <-time.After(time.Second):
		t.Fatal("passive read did not reach adapter-owned pump")
	}
	if err := slot.Close(); err != nil {
		t.Fatalf("stable passive Close() error = %v", err)
	}
	select {
	case err := <-readDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("interrupted ReadEvent() error = %v, want context.Canceled", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("stable passive Close did not interrupt current read")
	}
	select {
	case <-passive.closed:
		t.Fatal("stable passive Close retired the driver generation")
	default:
	}
	if runtime.SafetyQuarantined() {
		t.Fatal("interrupting the passive consumer quarantined the generation")
	}
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("runtime Stop() error = %v", err)
	}
}

func TestIssue851PassiveDisconnectReportsFailureAndReplacesGeneration(t *testing.T) {
	var mu sync.Mutex
	var activeGenerations []*issue851BlockingRawTransport
	factoryCalls := 0
	runtimeSeam := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		mu.Lock()
		factoryCalls++
		call := factoryCalls
		active := newIssue851BlockingRawTransport()
		activeGenerations = append(activeGenerations, active)
		mu.Unlock()
		var passive transport.RawTransport = newIssue851BlockingRawTransport()
		if call == 1 {
			passive = newIssue851TerminalPassiveRaw(errors.New("passive provider disconnected"))
		}
		return newManagedEBusGenerationParts(ctx, active, passive, nil, func() error {
			return errors.Join(active.Close(), passive.Close())
		}), nil
	}, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
	runtime := &ebusDriverRuntime{runtime: runtimeSeam}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID:           primaryEBusDriverID,
		Enabled:      true,
		Runtime:      runtime,
		Capabilities: []drivermanager.Capability{drivermanager.CapabilityRead, drivermanager.CapabilityWrite},
		ClassifyError: func(error) drivermanager.Failure {
			return drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}}
		},
		Retry: drivermanager.RetryPolicy{Budget: 1, InitialDelay: 5 * time.Millisecond, MaxDelay: 5 * time.Millisecond},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()
	reporter := func(correlation drivermanager.Correlation, rawErr error) {
		manager.ReportFailure(primaryEBusDriverID, correlation, drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}})
	}
	activeSlot := newManagedEBusStableTransport(manager, ebusgateway.TransportTCPPlain, reporter)
	passiveSlot := newEBusStablePassiveTransport(managedEBusSlotInvoker{manager: manager, id: primaryEBusDriverID}, ebusgateway.TransportENH, reporter)
	if err := manager.Start(context.Background(), primaryEBusDriverID); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if written, err := activeSlot.Write([]byte{0x01}); err != nil || written != 1 {
		t.Fatalf("active Write before passive failure = (%d, %v)", written, err)
	}
	if _, err := passiveSlot.(transport.StreamEventReader).ReadEvent(); err == nil {
		t.Fatal("passive disconnect did not surface an error")
	}
	deadline := time.Now().Add(time.Second)
	for {
		snapshot, _ := manager.Snapshot(primaryEBusDriverID)
		if snapshot.ObservedState == drivermanager.ObservedRunning && snapshot.Generation == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("passive failure did not replace generation: %#v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	first := activeGenerations[0]
	calls := factoryCalls
	mu.Unlock()
	if calls != 2 {
		t.Fatalf("factory calls = %d, want 2", calls)
	}
	select {
	case <-first.closed:
	case <-time.After(time.Second):
		t.Fatal("old generation did not close after passive disconnect")
	}
	if written, err := activeSlot.Write([]byte{0x02}); err != nil || written != 1 {
		t.Fatalf("active Write after passive replacement = (%d, %v)", written, err)
	}
}

func TestIssue851AdapterDirectConnectionLossWithdrawsCapabilitiesAndRecoversOnce(t *testing.T) {
	var mu sync.Mutex
	var healthByGeneration []*ebusGenerationHealth
	var rawByGeneration []*issue851TerminalPassiveRaw
	factoryCalls := 0
	runtimeSeam := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		health := &ebusGenerationHealth{}
		raw := newIssue851TerminalPassiveRaw(ebuserrors.ErrTimeout)
		managed := newManagedEBusGeneration(ctx, raw, raw.Close)
		managed.Transport.(*managedEBusGeneration).health = health
		mu.Lock()
		factoryCalls++
		healthByGeneration = append(healthByGeneration, health)
		rawByGeneration = append(rawByGeneration, raw)
		mu.Unlock()
		return managed, nil
	}, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
	runtime := &ebusDriverRuntime{runtime: runtimeSeam}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID:           primaryEBusDriverID,
		Enabled:      true,
		Runtime:      runtime,
		Capabilities: []drivermanager.Capability{drivermanager.CapabilityRead, drivermanager.CapabilityWrite},
		ClassifyError: func(error) drivermanager.Failure {
			return drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}}
		},
		Retry: drivermanager.RetryPolicy{Budget: 1, InitialDelay: 25 * time.Millisecond, MaxDelay: 25 * time.Millisecond},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()
	runtime.reporter = func(correlation drivermanager.Correlation, rawErr error) {
		manager.ReportFailure(primaryEBusDriverID, correlation, classifyEBusDriverError(rawErr))
	}
	stable := newManagedEBusStableTransport(manager, ebusgateway.TransportAdapterDirect, runtime.reporter)
	if err := manager.Start(context.Background(), primaryEBusDriverID); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	first := requireEBusDriverObservedState(t, manager, drivermanager.ObservedRunning, time.Second)
	if first.Generation != 1 {
		t.Fatalf("first generation = %d, want 1", first.Generation)
	}

	// A normal idle timeout is an operation outcome, not a connection-health
	// transition. It must not withdraw capabilities or churn the generation.
	if _, err := stable.ReadByte(); !errors.Is(err, ebuserrors.ErrTimeout) {
		t.Fatalf("ReadByte() error = %v, want ErrTimeout", err)
	}
	afterTimeout, _ := manager.Snapshot(primaryEBusDriverID)
	if afterTimeout.ObservedState != drivermanager.ObservedRunning || afterTimeout.Generation != first.Generation || afterTimeout.Revision != first.Revision {
		t.Fatalf("idle timeout changed lifecycle snapshot: before=%#v after=%#v", first, afterTimeout)
	}

	mu.Lock()
	firstHealth := healthByGeneration[0]
	firstRaw := rawByGeneration[0]
	mu.Unlock()
	firstHealth.connectionLost()
	backoff := requireEBusDriverObservedState(t, manager, drivermanager.ObservedBackoff, time.Second)
	if backoff.Generation != first.Generation || len(backoff.EffectiveCapabilities) != 0 || backoff.Reason.Code != drivermanager.ReasonRetryScheduled {
		t.Fatalf("connection-loss BACKOFF snapshot = %#v", backoff)
	}

	second := requireEBusDriverObservedState(t, manager, drivermanager.ObservedRunning, time.Second)
	if second.Generation != first.Generation+1 || len(second.EffectiveCapabilities) != len(second.Capabilities) {
		t.Fatalf("recovered snapshot = %#v, want generation %d with full capabilities", second, first.Generation+1)
	}
	mu.Lock()
	calls := factoryCalls
	mu.Unlock()
	if calls != 2 {
		t.Fatalf("factory calls = %d, want exactly 2", calls)
	}
	select {
	case <-firstRaw.closed:
	case <-time.After(time.Second):
		t.Fatal("lost generation was not proven closed during replacement")
	}

	// The old mux may observe another terminal edge while it is retiring.
	// Its generation health is one-shot and cannot start generation 3.
	firstHealth.connectionLost()
	time.Sleep(50 * time.Millisecond)
	afterStale, _ := manager.Snapshot(primaryEBusDriverID)
	mu.Lock()
	calls = factoryCalls
	mu.Unlock()
	if afterStale.ObservedState != drivermanager.ObservedRunning || afterStale.Generation != second.Generation || calls != 2 {
		t.Fatalf("stale old connection signal changed recovered generation: snapshot=%#v calls=%d", afterStale, calls)
	}
}

func TestIssue851AdapterDirectBroadcastPassiveBridgeRetiresWithGeneration(t *testing.T) {
	for _, action := range []string{"stop", "loss_replace"} {
		t.Run(action, func(t *testing.T) {
			cfg := ebusgateway.DefaultConfig()
			cfg.BroadcastListen = true
			cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
			cfg.TransportConfig.Network = "tcp"
			cfg.TransportConfig.Address = newIssue851AdapterServer(t)
			cfg.TransportConfig.DialTimeout = time.Second

			controller, err := newEBusDriverController(cfg)
			if err != nil {
				t.Fatalf("newEBusDriverController() error = %v", err)
			}
			defer func() { _ = controller.Shutdown(context.Background()) }()
			if err := controller.Start(context.Background()); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			firstSnapshot := requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedRunning, 2*time.Second)
			var first *managedEBusGeneration
			if _, err := controller.manager.Invoke(context.Background(), primaryEBusDriverID, drivermanager.CapabilityRead, func(provider any) error {
				first, _ = provider.(*managedEBusGeneration)
				return nil
			}); err != nil || first == nil || first.passive == nil || first.health == nil {
				t.Fatalf("capture broadcast generation = (%T, %v), passive=%v health=%v", first, err, first != nil && first.passive != nil, first != nil && first.health != nil)
			}

			// Mux.Start publishes one connected/reset boundary before the factory
			// returns. Drain it, then hand one demand directly to the generation's
			// adapter-owned pump. The send completes only after the pump accepts the
			// request; with no further fake-adapter traffic it is then blocked in the
			// generation-owned passive bridge.
			reader, ok := first.passive.raw.(transport.StreamEventReader)
			if !ok {
				t.Fatalf("passive bridge = %T, want StreamEventReader", first.passive.raw)
			}
			if _, err := reader.ReadEvent(); err != nil {
				t.Fatalf("drain passive connected boundary: %v", err)
			}
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			staleResult := make(chan managedEBusReadResult, 1)
			select {
			case first.passive.requests <- managedEBusReadRequest{ctx: requestCtx, result: staleResult}:
			case <-time.After(time.Second):
				t.Fatal("passive generation pump did not accept blocking demand")
			}

			switch action {
			case "stop":
				stopCtx, cancelStop := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancelStop()
				if err := controller.manager.Stop(stopCtx, primaryEBusDriverID); err != nil {
					t.Fatalf("Stop() error = %v", err)
				}
				stopped := requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedStopped, time.Second)
				if stopped.SafetyQuarantined || stopped.Generation != firstSnapshot.Generation {
					t.Fatalf("broadcast Stop snapshot = %#v", stopped)
				}
			case "loss_replace":
				// Physical mux loss fences adapter work before its correlated owner
				// callback publishes BACKOFF.
				first.fenceProxy()
				first.health.connectionLost()
				requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedBackoff, time.Second)
				if err := controller.Start(context.Background()); err != nil {
					t.Fatalf("expedite recovery Start() error = %v", err)
				}
				second := requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedRunning, 3*time.Second)
				if second.SafetyQuarantined || second.Generation != firstSnapshot.Generation+1 {
					t.Fatalf("broadcast loss recovery snapshot = %#v", second)
				}
				first.health.connectionLost()
				time.Sleep(25 * time.Millisecond)
				afterStale, _ := controller.manager.Snapshot(primaryEBusDriverID)
				if afterStale.ObservedState != drivermanager.ObservedRunning || afterStale.Generation != second.Generation || afterStale.Revision != second.Revision {
					t.Fatalf("stale broadcast generation changed successor: before=%#v after=%#v", second, afterStale)
				}
			}

			select {
			case <-first.passive.pumpDone:
			case <-time.After(time.Second):
				t.Fatal("retired broadcast passive pump did not close")
			}
			select {
			case result := <-staleResult:
				t.Fatalf("retired broadcast generation published stale result: %#v", result)
			default:
			}
		})
	}
}

func TestIssue851ProxyReadinessStaysDegradedOnAdapterDialFailure(t *testing.T) {
	unused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve unused adapter address: %v", err)
	}
	adapterAddress := unused.Addr().String()
	if err := unused.Close(); err != nil {
		t.Fatalf("release unused adapter address: %v", err)
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = false
	cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = adapterAddress
	cfg.TransportConfig.DialTimeout = 100 * time.Millisecond
	cfg.ProxyListenAddr = "127.0.0.1:0"
	controller, err := newEBusDriverController(cfg)
	if err != nil {
		t.Fatalf("newEBusDriverController() error = %v", err)
	}
	defer func() { _ = controller.Shutdown(context.Background()) }()

	if got := controller.ProxyReadiness(); got != ebusProxyReadinessDegraded {
		t.Fatalf("pre-construction proxy readiness = %q, want DEGRADED", got)
	}
	if err := controller.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedBackoff, time.Second)
	if got := controller.ProxyReadiness(); got != ebusProxyReadinessDegraded {
		t.Fatalf("dial-failure proxy readiness = %q, want DEGRADED", got)
	}
}

func TestIssue851ProxyReadinessStaysDegradedOnListenerBindFailure(t *testing.T) {
	adapterAddress := newIssue851AdapterServer(t)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy proxy address: %v", err)
	}
	defer func() { _ = occupied.Close() }()

	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = false
	cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = adapterAddress
	cfg.TransportConfig.DialTimeout = time.Second
	cfg.ProxyListenAddr = occupied.Addr().String()
	controller, err := newEBusDriverController(cfg)
	if err != nil {
		t.Fatalf("newEBusDriverController() error = %v", err)
	}
	defer func() { _ = controller.Shutdown(context.Background()) }()

	if err := controller.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedBackoff, time.Second)
	if got := controller.ProxyReadiness(); got != ebusProxyReadinessDegraded {
		t.Fatalf("bind-failure proxy readiness = %q, want DEGRADED", got)
	}
}

func TestIssue851ProxyReadinessWithdrawsAndRecoversWithAdmittedListener(t *testing.T) {
	adapterAddress := newIssue851AdapterServer(t)
	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = false
	cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = adapterAddress
	cfg.TransportConfig.DialTimeout = time.Second
	cfg.ProxyListenAddr = "127.0.0.1:0"
	controller, err := newEBusDriverController(cfg)
	if err != nil {
		t.Fatalf("newEBusDriverController() error = %v", err)
	}
	defer func() { _ = controller.Shutdown(context.Background()) }()

	if err := controller.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	first := requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedRunning, 2*time.Second)
	if got := controller.ProxyReadiness(); got != ebusProxyReadinessReady {
		t.Fatalf("bound generation proxy readiness = %q, want READY", got)
	}

	if accepted := controller.manager.ReportFailure(
		primaryEBusDriverID,
		drivermanager.Correlation{Generation: first.Generation},
		drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}},
	); !accepted {
		t.Fatal("current-generation proxy withdrawal was rejected")
	}
	requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedBackoff, time.Second)
	if got := controller.ProxyReadiness(); got != ebusProxyReadinessDegraded {
		t.Fatalf("withdrawn generation proxy readiness = %q, want DEGRADED", got)
	}

	// Explicit repeated RUNNING intent expedites the already-scheduled
	// replacement. The next generation cannot publish READY until its own
	// listener has bound and DriverManager admits that exact generation.
	if err := controller.Start(context.Background()); err != nil {
		t.Fatalf("recovery Start() error = %v", err)
	}
	second := requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedRunning, 2*time.Second)
	if second.Generation != first.Generation+1 {
		t.Fatalf("recovered generation = %d, want %d", second.Generation, first.Generation+1)
	}
	if got := controller.ProxyReadiness(); got != ebusProxyReadinessReady {
		t.Fatalf("recovered proxy readiness = %q, want READY", got)
	}
}

func TestIssue851ManagerWithdrawalFencesCurrentProxyGeneration(t *testing.T) {
	for _, action := range []string{"stop", "replace"} {
		t.Run(action, func(t *testing.T) {
			var mu sync.Mutex
			var lifecycles []*issue851ProxyLifecycle
			runtimeSeam := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
				raw := newIssue851BlockingRawTransport()
				lifecycle := newIssue851ProxyLifecycle()
				managed := newManagedEBusGeneration(ctx, raw, raw.Close)
				generation := managed.Transport.(*managedEBusGeneration)
				generation.proxyListenerBound = true
				generation.proxyLifecycle = lifecycle
				mu.Lock()
				lifecycles = append(lifecycles, lifecycle)
				mu.Unlock()
				return managed, nil
			}, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
			runtime := &ebusDriverRuntime{runtime: runtimeSeam}
			manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
				ID: primaryEBusDriverID, Enabled: true, Runtime: runtime,
				Capabilities: []drivermanager.Capability{drivermanager.CapabilityRead, drivermanager.CapabilityWrite},
			}}})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			controller := &ebusDriverController{manager: manager, proxyConfigured: true}
			defer func() { _ = manager.Shutdown(context.Background()) }()
			if err := manager.Start(context.Background(), primaryEBusDriverID); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			if got := controller.ProxyReadiness(); got != ebusProxyReadinessReady {
				t.Fatalf("pre-withdrawal readiness = %q, want READY", got)
			}
			mu.Lock()
			first := lifecycles[0]
			mu.Unlock()

			switch action {
			case "stop":
				if err := manager.Stop(context.Background(), primaryEBusDriverID); err != nil {
					t.Fatalf("Stop() error = %v", err)
				}
				if got := controller.ProxyReadiness(); got != ebusProxyReadinessDegraded {
					t.Fatalf("post-stop readiness = %q, want DEGRADED", got)
				}
			case "replace":
				if err := manager.Replace(context.Background(), primaryEBusDriverID); err != nil {
					t.Fatalf("Replace() error = %v", err)
				}
				if got := controller.ProxyReadiness(); got != ebusProxyReadinessReady {
					t.Fatalf("replacement readiness = %q, want READY", got)
				}
			}
			if first.ready.Load() || first.fences.Load() == 0 {
				t.Fatalf("old proxy generation was not fenced before %s: ready=%v fences=%d", action, first.ready.Load(), first.fences.Load())
			}
		})
	}
}

func TestIssue851ProxyReadinessDropsAtMuxFenceBeforeManagerFailure(t *testing.T) {
	raw := newIssue851BlockingRawTransport()
	lifecycle := newIssue851ProxyLifecycle()
	runtimeSeam := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		managed := newManagedEBusGeneration(ctx, raw, raw.Close)
		generation := managed.Transport.(*managedEBusGeneration)
		generation.proxyListenerBound = true
		generation.proxyLifecycle = lifecycle
		return managed, nil
	}, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
	runtime := &ebusDriverRuntime{runtime: runtimeSeam}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID: primaryEBusDriverID, Enabled: true, Runtime: runtime,
		Capabilities: []drivermanager.Capability{drivermanager.CapabilityRead},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()
	controller := &ebusDriverController{manager: manager, proxyConfigured: true}
	if err := manager.Start(context.Background(), primaryEBusDriverID); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got := controller.ProxyReadiness(); got != ebusProxyReadinessReady {
		t.Fatalf("healthy readiness = %q, want READY", got)
	}

	// This is the reachable interval after adaptermux fenced physical loss but
	// before its owner callback publishes DriverManager BACKOFF.
	lifecycle.FenceManagedConnection()
	if snapshot := controller.Snapshot(); snapshot.ObservedState != drivermanager.ObservedRunning {
		t.Fatalf("manager state changed before failure callback: %#v", snapshot)
	}
	if got := controller.ProxyReadiness(); got != ebusProxyReadinessDegraded {
		t.Fatalf("loss-fenced readiness = %q, want DEGRADED", got)
	}
}

func TestIssue851FatalProxyAcceptRecoversOnceAndRejectsStaleGeneration(t *testing.T) {
	adapterAddress := newIssue851AdapterServer(t)
	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = false
	cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = adapterAddress
	cfg.TransportConfig.DialTimeout = time.Second
	cfg.ProxyListenAddr = "127.0.0.1:0"
	controller, err := newEBusDriverController(cfg)
	if err != nil {
		t.Fatalf("newEBusDriverController() error = %v", err)
	}
	defer func() { _ = controller.Shutdown(context.Background()) }()
	if err := controller.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	firstSnapshot := requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedRunning, 2*time.Second)
	var firstGeneration *managedEBusGeneration
	if _, err := controller.manager.Invoke(context.Background(), primaryEBusDriverID, drivermanager.CapabilityRead, func(provider any) error {
		firstGeneration, _ = provider.(*managedEBusGeneration)
		return nil
	}); err != nil || firstGeneration == nil {
		t.Fatalf("capture first generation = (%T, %v)", firstGeneration, err)
	}
	firstMux, ok := firstGeneration.proxyLifecycle.(*adaptermux.Mux)
	if !ok || firstGeneration.health == nil {
		t.Fatalf("first generation listener lifecycle = %T health=%v", firstGeneration.proxyLifecycle, firstGeneration.health != nil)
	}
	onFatal := adapterDirectProxyFatalCallback(firstMux, firstGeneration.health.connectionLost)
	if onFatal == nil {
		t.Fatal("managed proxy fatal callback is nil")
	}
	onFatal(net.ErrClosed)
	backoff := requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedBackoff, time.Second)
	if got := controller.ProxyReadiness(); got != ebusProxyReadinessDegraded {
		t.Fatalf("fatal-Accept readiness = %q, want DEGRADED", got)
	}
	// The same listener/mux can surface follow-on errors while teardown drains.
	// Generation health and the wire-level owner are one-shot, so this cannot
	// schedule a second replacement or consume another retry.
	onFatal(errors.New("stale accept-loop completion"))
	afterDuplicate, _ := controller.manager.Snapshot(primaryEBusDriverID)
	if afterDuplicate.Revision != backoff.Revision || afterDuplicate.Attempt != backoff.Attempt {
		t.Fatalf("duplicate fatal Accept changed recovery: before=%#v after=%#v", backoff, afterDuplicate)
	}

	if err := controller.Start(context.Background()); err != nil {
		t.Fatalf("expedite recovery Start() error = %v", err)
	}
	second := requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedRunning, 2*time.Second)
	if second.Generation != firstSnapshot.Generation+1 || controller.ProxyReadiness() != ebusProxyReadinessReady {
		t.Fatalf("fatal-Accept recovery snapshot=%#v readiness=%q", second, controller.ProxyReadiness())
	}
	// A late callback from the retired listener is generation-correlated and
	// must not fence or replace the healthy successor.
	onFatal(errors.New("late retired-listener error"))
	time.Sleep(25 * time.Millisecond)
	afterStale, _ := controller.manager.Snapshot(primaryEBusDriverID)
	if afterStale.ObservedState != drivermanager.ObservedRunning || afterStale.Generation != second.Generation || afterStale.Revision != second.Revision {
		t.Fatalf("stale listener callback changed successor: before=%#v after=%#v", second, afterStale)
	}
}

func TestIssue851InjectedTransportIsNotReadmittedAfterReplacement(t *testing.T) {
	raw := newIssue851BlockingRawTransport()
	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = raw
	cfg.TransportConfig.Protocol = ebusgateway.TransportTCPPlain
	cfg.BroadcastListen = false
	controller, err := newEBusDriverController(cfg)
	if err != nil {
		t.Fatalf("newEBusDriverController() error = %v", err)
	}
	defer func() { _ = controller.Shutdown(context.Background()) }()
	if err := controller.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for controller.Snapshot().ObservedState != drivermanager.ObservedRunning {
		if time.Now().After(deadline) {
			t.Fatalf("injected generation did not start: %#v", controller.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
	if err := controller.manager.Replace(context.Background(), primaryEBusDriverID); err != nil {
		t.Fatalf("Replace() manager error = %v", err)
	}
	snapshot := controller.Snapshot()
	if snapshot.ObservedState != drivermanager.ObservedFailed || snapshot.Reason.Code != drivermanager.ReasonProviderUnavailable || snapshot.Generation != 1 {
		t.Fatalf("replacement reused injected transport: %#v", snapshot)
	}
	select {
	case <-raw.closed:
	case <-time.After(time.Second):
		t.Fatal("one-shot injected transport was not closed")
	}
}

func TestIssue851InjectedPassiveTransportIsNotReadmittedAfterReplacement(t *testing.T) {
	passive := &issue851OneShotPassiveRaw{}
	var serversMu sync.Mutex
	var servers []net.Conn
	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = true
	cfg.PassiveTransport = passive
	cfg.TransportConfig.Protocol = ebusgateway.TransportTCPPlain
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "fresh-active.invalid:9999"
	cfg.TransportConfig.Dial = func(context.Context, string, string, time.Duration) (net.Conn, error) {
		client, server := net.Pipe()
		serversMu.Lock()
		servers = append(servers, server)
		serversMu.Unlock()
		return client, nil
	}
	defer func() {
		serversMu.Lock()
		defer serversMu.Unlock()
		for _, server := range servers {
			_ = server.Close()
		}
	}()

	runtimeSeam := transport.NewDriverRuntime(
		newEBusDriverFactory(cfg, ebusgateway.TransportTCPPlain, true, &ebusV8CumulativeCounter{}),
		transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond},
	)
	runtime := &ebusDriverRuntime{runtime: runtimeSeam}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID: primaryEBusDriverID, Enabled: true, Runtime: runtime,
		Capabilities: []drivermanager.Capability{drivermanager.CapabilityRead},
	}}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()
	passiveSlot := newEBusStablePassiveTransport(
		managedEBusSlotInvoker{manager: manager, id: primaryEBusDriverID},
		ebusgateway.TransportTCPPlain,
		nil,
	)
	if err := manager.Start(context.Background(), primaryEBusDriverID); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := manager.Replace(context.Background(), primaryEBusDriverID); err != nil {
		t.Fatalf("Replace() manager error = %v", err)
	}
	// Exercise the stable passive slot after replacement intent. A reused
	// generation would call ReadByte on the already-closed injected pointer;
	// the one-shot factory must instead leave the manager unavailable.
	if _, err := passiveSlot.ReadByte(); err == nil {
		t.Fatal("passive slot unexpectedly reached a replacement provider")
	}
	snapshot, _ := manager.Snapshot(primaryEBusDriverID)
	if snapshot.ObservedState != drivermanager.ObservedFailed || snapshot.Reason.Code != drivermanager.ReasonProviderUnavailable || snapshot.Generation != 1 {
		t.Fatalf("passive replacement snapshot = %#v, want generation-1 PROVIDER_UNAVAILABLE", snapshot)
	}
	if got := passive.closeCalls.Load(); got != 1 {
		t.Fatalf("injected passive Close calls = %d, want exactly 1", got)
	}
	if got := passive.reads.Load(); got != 0 {
		t.Fatalf("retired injected passive was accessed after replacement: reads=%d after_close=%d", got, passive.readsAfterClose.Load())
	}
}

func TestIssue851InvalidTransportURIIsImmediateConfigFailure(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = false
	cfg.TransportConfig.Address = "unsupported://adapter.invalid:9999"
	controller, err := newEBusDriverController(cfg)
	if err != nil {
		t.Fatalf("newEBusDriverController() error = %v", err)
	}
	defer func() { _ = controller.Shutdown(context.Background()) }()
	if err := controller.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	failed := requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedFailed, time.Second)
	if failed.Reason.Code != drivermanager.ReasonConfigInvalid || failed.Reason.Retryable || failed.Retry != nil || failed.Generation != 0 || failed.Attempt != 1 {
		t.Fatalf("invalid URI snapshot = %#v", failed)
	}
	time.Sleep(25 * time.Millisecond)
	after, _ := controller.manager.Snapshot(primaryEBusDriverID)
	if after.Revision != failed.Revision || after.Attempt != 1 || after.ObservedState != drivermanager.ObservedFailed {
		t.Fatalf("invalid URI unexpectedly retried: before=%#v after=%#v", failed, after)
	}
}

func TestIssue851TCPFamilyURIWithoutPortFailsConfigBeforeDialOrRetry(t *testing.T) {
	var dials atomic.Int32
	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = false
	cfg.TransportConfig.Address = "tcp-plain://adapter.invalid"
	cfg.TransportConfig.Dial = func(context.Context, string, string, time.Duration) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("dial must not be reached")
	}
	controller, err := newEBusDriverController(cfg)
	if err != nil {
		t.Fatalf("newEBusDriverController() error = %v", err)
	}
	defer func() { _ = controller.Shutdown(context.Background()) }()
	if err := controller.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	failed := requireEBusDriverObservedState(t, controller.manager, drivermanager.ObservedFailed, 250*time.Millisecond)
	if failed.Reason.Code != drivermanager.ReasonConfigInvalid || failed.Reason.Retryable || failed.Retry != nil || failed.Generation != 0 || failed.Attempt != 1 {
		t.Fatalf("invalid host:port snapshot = %#v", failed)
	}
	if got := dials.Load(); got != 0 {
		t.Fatalf("invalid host:port dial calls = %d, want 0", got)
	}
	time.Sleep(25 * time.Millisecond)
	after, _ := controller.manager.Snapshot(primaryEBusDriverID)
	if after.Revision != failed.Revision || after.Attempt != 1 || after.ObservedState != drivermanager.ObservedFailed {
		t.Fatalf("invalid host:port unexpectedly retried: before=%#v after=%#v", failed, after)
	}
}

func TestIssue851AdminClassifierResolvesCurrentGenerationWithoutMetricsScrape(t *testing.T) {
	originalStatic := v8AdminEventsCurrentClassifier.Load()
	originalProvider := v8AdminEventsCurrentProvider.Load()
	t.Cleanup(func() {
		v8AdminEventsCurrentClassifier.Store(originalStatic)
		v8AdminEventsCurrentProvider.Store(originalProvider)
	})
	first := v8classifier.New(v8classifier.ModeShadow)
	second := v8classifier.New(v8classifier.ModeShadow)
	classifiers := []*v8classifier.Classifier{first, second}
	index := 0
	runtime := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		raw := newIssue851BlockingRawTransport()
		classifier := &issue851Classifier{v8: classifiers[index]}
		index++
		return newManagedEBusGenerationParts(ctx, raw, nil, classifier, raw.Close), nil
	}, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
	stable := &ebusStableClassifier{invoker: directEBusSlotInvoker{runtime: runtime}}
	v8AdminEventsCurrentClassifier.Store(nil)
	v8AdminEventsCurrentProvider.Store(&v8AdminClassifierProvider{withClassifier: stable.WithV8Classifier})
	currentClassifier := func() *v8classifier.Classifier {
		var current *v8classifier.Classifier
		withCurrentV8AdminClassifier(func(classifier *v8classifier.Classifier) { current = classifier })
		return current
	}
	if _, err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got := currentClassifier(); got != first {
		t.Fatalf("generation 1 admin classifier = %p, want %p", got, first)
	}
	if _, err := runtime.Replace(context.Background()); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	if got := currentClassifier(); got != second {
		t.Fatalf("generation 2 admin classifier = %p, want %p", got, second)
	}
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if got := currentClassifier(); got != nil {
		t.Fatalf("stopped admin classifier = %p, want nil", got)
	}
}

func TestIssue851V8ShadowTotalIsProcessMonotonicAcrossReplacement(t *testing.T) {
	first := v8classifier.New(v8classifier.ModeShadow)
	second := v8classifier.New(v8classifier.ModeShadow)
	classifiers := []*v8classifier.Classifier{first, second}
	counter := &ebusV8CumulativeCounter{}
	index := 0
	runtime := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		if index >= len(classifiers) {
			return nil, errors.New("unexpected extra generation")
		}
		classifier := &issue851Classifier{v8: classifiers[index]}
		index++
		raw := newIssue851BlockingRawTransport()
		closeFn := func() error {
			err := raw.Close()
			counter.retire(classifier)
			return err
		}
		return newManagedEBusGenerationParts(ctx, raw, nil, classifier, closeFn), nil
	}, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
	defer func() { _ = runtime.Stop(context.Background()) }()
	stable := &ebusStableClassifier{
		invoker: directEBusSlotInvoker{runtime: runtime},
		counter: counter,
	}
	observeShadowDrop := func(classifier *v8classifier.Classifier) {
		now := time.Unix(0, 0)
		classifier.Observe(transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x71}, now)
		classifier.Observe(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}, now)
	}

	if _, err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	observeShadowDrop(first)
	if got := stable.V8ShadowWouldHaveDroppedTotal(); got != 1 {
		t.Fatalf("generation 1 process total = %d, want 1", got)
	}
	if _, err := runtime.Replace(context.Background()); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	if got := stable.V8ShadowWouldHaveDroppedTotal(); got != 1 {
		t.Fatalf("post-replacement process total = %d, want retained 1", got)
	}

	// Do not scrape generation 2 before retirement. Its final increment must
	// still be folded into the process total by the close-proof path.
	observeShadowDrop(second)
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if got := stable.V8ShadowWouldHaveDroppedTotal(); got != 2 {
		t.Fatalf("post-stop process total = %d, want unscraped generation 2 increment retained", got)
	}
}

func requireEBusDriverObservedState(t *testing.T, manager *drivermanager.Manager, want drivermanager.ObservedState, timeout time.Duration) drivermanager.Snapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if snapshot, ok := manager.Snapshot(primaryEBusDriverID); ok && snapshot.ObservedState == want {
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
	snapshot, _ := manager.Snapshot(primaryEBusDriverID)
	t.Fatalf("eBUS driver state = %s, want %s; snapshot=%#v", snapshot.ObservedState, want, snapshot)
	return drivermanager.Snapshot{}
}
