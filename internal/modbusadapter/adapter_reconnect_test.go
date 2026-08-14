package modbusadapter

import (
	"context"
	"net"
	"testing"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

func TestAdapterReconnectRetiresOldGenerationBeforeFreshDialAndNeverRetriesStaleRead(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan struct{}, 2)
	go func() {
		for range 2 {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- struct{}{}
			go func() {
				defer func() { _ = connection.Close() }()
				<-time.After(time.Second)
			}()
		}
	}()

	config := integrationConfig(t, "tcp://"+listener.Addr().String())
	adapter, err := Start(context.Background(), config, realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	oldConnection := adapter.connection
	request, err := modbus.NewReadRegistersRequest(modbus.FunctionReadHoldingRegisters, 40000, 1)
	if err != nil {
		t.Fatalf("NewReadRegistersRequest: %v", err)
	}
	stale, err := adapter.EnqueueRead(ReadPlan{
		UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", PollGeneration: 61,
		DeadlineIdentity: 71, Timeout: time.Second,
		Reads: []modbus.TCPLogicalRead{{LogicalViewID: 81, Request: request}},
	})
	if err != nil {
		t.Fatalf("EnqueueRead(stale): %v", err)
	}

	// Reconnect is adapter orchestration only: the endpoint owns its configured
	// backoff. The adapter must retire the old handle before dialing this same
	// remote again, and must not carry an admitted old-generation read forward.
	if err := adapter.Reconnect(context.Background()); err != nil {
		t.Fatalf("Reconnect: %v", err)
	}
	for range 2 {
		select {
		case <-accepted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for reconnect accepts")
		}
	}
	if adapter.connection.Generation() == oldConnection.Generation() {
		t.Fatalf("reconnect reused transport generation %d", oldConnection.Generation())
	}
	if dispatch, ok := adapter.Dispatch(); ok && dispatch.RequestID() == stale.RequestID() {
		t.Fatalf("stale request %d dispatched after reconnect", stale.RequestID())
	}
	if snapshot := adapter.Snapshot(); snapshot.Resources.QueuedRequests != 0 {
		t.Fatalf("stale queued work survived reconnect: %+v", snapshot.Resources)
	}
}
