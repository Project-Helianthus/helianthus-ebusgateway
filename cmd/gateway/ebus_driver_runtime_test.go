package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
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
		passive := newEBusStablePassiveTransport(directEBusSlotInvoker{runtime: runtime}, protocol)
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
		passive := newEBusStablePassiveTransport(directEBusSlotInvoker{runtime: runtime}, protocol)
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
