package modbusadapter

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

type fakeEndpoint struct {
	mu sync.Mutex

	openCalls    int
	enqueueCalls int
	cancelCalls  int
	closeCalls   int
	lastPlan     modbus.TCPReadPlan
	openErr      error
	closeErr     error
}

func (endpoint *fakeEndpoint) OpenConnection(net.Conn) (modbus.TCPConnectionHandle, error) {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	endpoint.openCalls++
	return modbus.TCPConnectionHandle{}, endpoint.openErr
}

func (endpoint *fakeEndpoint) EnqueueRead(plan modbus.TCPReadPlan) (modbus.TCPRequestHandle, error) {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	endpoint.enqueueCalls++
	endpoint.lastPlan = plan
	return modbus.TCPRequestHandle{}, nil
}

func (*fakeEndpoint) Dispatch() (modbus.TCPDispatch, bool) {
	return modbus.TCPDispatch{}, false
}

func (*fakeEndpoint) Write(context.Context, modbus.TCPDispatch) (modbus.OwnerTransition, error) {
	return modbus.OwnerTransition{}, nil
}

func (*fakeEndpoint) Read(context.Context, modbus.TCPConnectionHandle) (modbus.TCPReadBatch, error) {
	return modbus.TCPReadBatch{}, nil
}

func (endpoint *fakeEndpoint) Cancel(modbus.TCPRequestHandle) error {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	endpoint.cancelCalls++
	return nil
}

func (*fakeEndpoint) Snapshot() modbus.TCPEndpointSnapshot {
	return modbus.TCPEndpointSnapshot{Endpoint: "tcp://127.0.0.1:1502"}
}

func (endpoint *fakeEndpoint) Close() error {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	endpoint.closeCalls++
	return endpoint.closeErr
}

func validConfig() Config {
	return Config{
		Enabled: true,
		Endpoint: modbus.TCPEndpointConfig{
			Endpoint: "tcp://127.0.0.1:1502",
		},
		DialTimeout: time.Second,
	}
}

func TestStartDisabledIsInert(t *testing.T) {
	factoryCalled := false
	dialerCalled := false
	adapter, err := Start(
		context.Background(),
		Config{},
		func(context.Context, string, string) (net.Conn, error) {
			dialerCalled = true
			return nil, errors.New("unexpected dial")
		},
		func(modbus.TCPEndpointConfig) (Endpoint, error) {
			factoryCalled = true
			return nil, errors.New("unexpected factory")
		},
	)
	if err != nil || adapter != nil {
		t.Fatalf("Start(disabled) = %#v, %v; want nil, nil", adapter, err)
	}
	if factoryCalled || dialerCalled {
		t.Fatalf("disabled adapter invoked factory=%v dialer=%v", factoryCalled, dialerCalled)
	}
}

func TestStartOwnsExactlyOneEndpointAndForwardsReadIdentity(t *testing.T) {
	endpoint := &fakeEndpoint{}
	client, peer := net.Pipe()
	defer func() { _ = peer.Close() }()

	factoryCalls := 0
	dialCalls := 0
	adapter, err := Start(
		context.Background(),
		validConfig(),
		func(_ context.Context, network, address string) (net.Conn, error) {
			dialCalls++
			if network != "tcp" || address != "127.0.0.1:1502" {
				t.Fatalf("dial target = %s/%s", network, address)
			}
			return client, nil
		},
		func(config modbus.TCPEndpointConfig) (Endpoint, error) {
			factoryCalls++
			if config.Endpoint != "tcp://127.0.0.1:1502" {
				t.Fatalf("factory endpoint = %q", config.Endpoint)
			}
			return endpoint, nil
		},
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if factoryCalls != 1 || dialCalls != 1 || endpoint.openCalls != 1 {
		t.Fatalf("ownership calls factory/dial/open = %d/%d/%d; want 1/1/1", factoryCalls, dialCalls, endpoint.openCalls)
	}

	request, err := modbus.NewReadRegistersRequest(modbus.FunctionReadInputRegisters, 100, 2)
	if err != nil {
		t.Fatalf("NewReadRegistersRequest: %v", err)
	}
	plan := ReadPlan{
		UnitID:             7,
		AuthorizationScope: "readonly:site-a",
		PollGeneration:     41,
		DeadlineIdentity:   12,
		Timeout:            900 * time.Millisecond,
		Reads: []modbus.TCPLogicalRead{{
			LogicalViewID: 1001,
			Request:       request,
		}},
	}
	if _, err := adapter.EnqueueRead(plan); err != nil {
		t.Fatalf("EnqueueRead: %v", err)
	}
	got := endpoint.lastPlan
	if got.UnitID != plan.UnitID ||
		got.AuthorizationScope != plan.AuthorizationScope ||
		got.PollGeneration != plan.PollGeneration ||
		got.DeadlineIdentity != plan.DeadlineIdentity ||
		got.Timeout != plan.Timeout ||
		!reflect.DeepEqual(got.Reads, plan.Reads) {
		t.Fatalf("forwarded plan changed identity: got=%+v want=%+v", got, plan)
	}
	if endpoint.enqueueCalls != 1 {
		t.Fatalf("enqueue calls = %d; want 1", endpoint.enqueueCalls)
	}

	if err := adapter.Cancel(modbus.TCPRequestHandle{}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if endpoint.cancelCalls != 1 {
		t.Fatalf("cancel calls = %d; want 1", endpoint.cancelCalls)
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("Close(second): %v", err)
	}
	if endpoint.closeCalls != 1 {
		t.Fatalf("endpoint close calls = %d; want 1", endpoint.closeCalls)
	}
}

func TestStartFailureClosesConstructedEndpointAndSocket(t *testing.T) {
	endpoint := &fakeEndpoint{openErr: errors.New("open failed")}
	client, peer := net.Pipe()
	defer func() { _ = peer.Close() }()

	adapter, err := Start(
		context.Background(),
		validConfig(),
		func(context.Context, string, string) (net.Conn, error) { return client, nil },
		func(modbus.TCPEndpointConfig) (Endpoint, error) { return endpoint, nil },
	)
	if err == nil || adapter != nil {
		t.Fatalf("Start(open failure) = %#v, %v; want nil, error", adapter, err)
	}
	if endpoint.closeCalls != 1 {
		t.Fatalf("endpoint close calls = %d; want 1", endpoint.closeCalls)
	}
	if err := client.SetDeadline(time.Now()); err == nil {
		t.Fatal("dialed socket remained open after OpenConnection failure")
	}
}
