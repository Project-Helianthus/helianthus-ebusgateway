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

func newIssue851BlockingRawTransport() *issue851BlockingRawTransport {
	return &issue851BlockingRawTransport{
		readStarted: make(chan struct{}, 1),
		readResult:  make(chan issue851ReadResult),
		closed:      make(chan struct{}),
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
	second.readResult <- issue851ReadResult{value: 0x5a}
	if value, err := slot.ReadByte(); err != nil || value != 0x5a {
		t.Fatalf("new generation ReadByte = (0x%02X, %v), want (0x5A, nil)", value, err)
	}
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("Stop generation 2: %v", err)
	}
}

func TestIssue851StableTransportShapeMatchesConfiguredFamily(t *testing.T) {
	runtime := transport.NewDriverRuntime(nil, transport.DriverRuntimeConfig{})

	for _, protocol := range []ebusgateway.TransportProtocol{ebusgateway.TransportENH, ebusgateway.TransportENS} {
		slot := newEBusStableTransport(runtime, protocol, nil)
		if _, ok := slot.(transport.EscapeAware); !ok {
			t.Errorf("%s slot does not preserve EscapeAware", protocol)
		}
		if _, ok := slot.(transport.EscapeFlaggedReader); !ok {
			t.Errorf("%s slot does not preserve EscapeFlaggedReader", protocol)
		}
		if _, ok := slot.(interface{ StartArbitration(byte) error }); !ok {
			t.Errorf("%s slot does not preserve enhanced arbitration", protocol)
		}
	}

	for _, protocol := range []ebusgateway.TransportProtocol{ebusgateway.TransportTCPPlain, ebusgateway.TransportUDPPlain, ebusgateway.TransportEbusdTCP} {
		slot := newEBusStableTransport(runtime, protocol, nil)
		if _, ok := slot.(transport.EscapeAware); ok {
			t.Errorf("%s slot unexpectedly advertises EscapeAware", protocol)
		}
		if _, ok := slot.(transport.EscapeFlaggedReader); ok {
			t.Errorf("%s slot unexpectedly advertises EscapeFlaggedReader", protocol)
		}
		if _, ok := slot.(interface{ StartArbitration(byte) error }); ok {
			t.Errorf("%s slot unexpectedly advertises enhanced arbitration", protocol)
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
}
