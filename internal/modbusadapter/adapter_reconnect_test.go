package modbusadapter

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
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
func TestAdapterReconnectAfterSocketLossRetiresFailedGenerationAndHonorsBackoff(t *testing.T) {
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
				// The first FC03 reaches a real peer, then loses its socket before
				// any response. The endpoint must own recovery classification.
				request := make([]byte, 12)
				_, _ = io.ReadFull(connection, request)
			}()
		}
	}()
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	old := adapter.connection
	request, err := modbus.NewReadRegistersRequest(modbus.FunctionReadHoldingRegisters, 40000, 1)
	if err != nil {
		t.Fatalf("NewReadRegistersRequest: %v", err)
	}
	stale, err := adapter.EnqueueRead(ReadPlan{UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", PollGeneration: 62, DeadlineIdentity: 72, Timeout: time.Second, Reads: []modbus.TCPLogicalRead{{LogicalViewID: 82, Request: request}}})
	if err != nil {
		t.Fatalf("EnqueueRead: %v", err)
	}
	dispatch, ok := adapter.Dispatch()
	if !ok || dispatch.RequestID() != stale.RequestID() {
		t.Fatalf("Dispatch = %#v, %v; want failed request %d", dispatch, ok, stale.RequestID())
	}
	if _, err := adapter.Write(context.Background(), dispatch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := adapter.Read(context.Background()); err == nil {
		t.Fatal("ExecuteRead after peer socket loss unexpectedly succeeded")
	}
	if snapshot := adapter.Snapshot(); !snapshot.ReconnectRequired {
		t.Fatalf("socket-loss endpoint state = %#v; want reconnect required", snapshot)
	}
	if err := adapter.Reconnect(context.Background()); err != nil {
		t.Fatalf("Reconnect after socket loss: %v", err)
	}
	for range 2 {
		select {
		case <-accepted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for original and recovered socket")
		}
	}
	if adapter.connection.Generation() == old.Generation() {
		t.Fatalf("reconnect reused failed transport generation %d", old.Generation())
	}
	if dispatch, ok := adapter.Dispatch(); ok && dispatch.RequestID() == stale.RequestID() {
		t.Fatalf("stale failed request %d was retried on recovered generation", stale.RequestID())
	}
}

func TestAdapterExecuteReadWithReconnectSerializesConcurrentRecovery(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	peerDone := make(chan error, 1)
	go func() {
		first, acceptErr := listener.Accept()
		if acceptErr != nil {
			peerDone <- acceptErr
			return
		}
		request := make([]byte, 12)
		_, readErr := io.ReadFull(first, request)
		_ = first.Close()
		if readErr != nil {
			peerDone <- readErr
			return
		}
		second, acceptErr := listener.Accept()
		if acceptErr != nil {
			peerDone <- acceptErr
			return
		}
		defer func() { _ = second.Close() }()
		for index := 0; index < 2; index++ {
			if _, readErr = io.ReadFull(second, request); readErr != nil {
				peerDone <- readErr
				return
			}
			response := make([]byte, 11)
			copy(response[0:4], request[0:4])
			binary.BigEndian.PutUint16(response[4:6], 5)
			response[6], response[7], response[8] = request[6], request[7], 2
			binary.BigEndian.PutUint16(response[9:11], uint16(0x1200+index))
			if _, writeErr := second.Write(response); writeErr != nil {
				peerDone <- writeErr
				return
			}
		}
		peerDone <- nil
	}()

	adapter, err := Start(
		context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()),
		realDialer, realFactory,
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	request, err := modbus.NewReadRegistersRequest(modbus.FunctionReadHoldingRegisters, 40000, 1)
	if err != nil {
		t.Fatalf("NewReadRegistersRequest: %v", err)
	}
	type result struct {
		batch modbus.TCPReadBatch
		err   error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var callers sync.WaitGroup
	for caller := range 2 {
		callers.Add(1)
		go func(caller int) {
			defer callers.Done()
			<-start
			batch, callErr := adapter.ExecuteReadWithReconnect(context.Background(), ReadPlan{
				UnitID: 1, AuthorizationScope: "mcp:modbus.raw.read",
				PollGeneration: uint64(caller + 1), DeadlineIdentity: uint64(caller + 1),
				Timeout: time.Second,
				Reads:   []modbus.TCPLogicalRead{{LogicalViewID: uint64(caller + 1), Request: request}},
			})
			results <- result{batch: batch, err: callErr}
		}(caller)
	}
	close(start)
	callers.Wait()
	close(results)
	for result := range results {
		if result.err != nil {
			t.Fatalf("ExecuteReadWithReconnect: %v", result.err)
		}
		if len(result.batch.Views) != 1 || result.batch.Views[0].Provenance().Wire.TransportGeneration < 2 {
			t.Fatalf("recovered batch = %#v", result.batch)
		}
	}
	if err := <-peerDone; err != nil {
		t.Fatalf("peer: %v", err)
	}
	if generation := adapter.connection.Generation(); generation != 2 {
		t.Fatalf("connection generation = %d; want exactly one reconnect to generation 2", generation)
	}
	if snapshot := adapter.Snapshot(); snapshot.ReconnectRequired || !snapshot.Healthy {
		t.Fatalf("post-recovery snapshot = %#v", snapshot)
	}
}
