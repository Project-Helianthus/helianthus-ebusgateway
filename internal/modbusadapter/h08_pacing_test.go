package modbusadapter

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

// h08PacingEndpoint records when each request starts (enqueue) and completes
// (cancel after the refused dispatch), and the largest number of requests that
// were ever in flight together.
type h08PacingEndpoint struct {
	fakeEndpoint
	mu             sync.Mutex
	connected      []time.Time
	starts         []time.Time
	ends           []time.Time
	inFlight       int
	maxFlight      int
	enqueueEntered chan struct{}
	enqueueRelease chan struct{}
}

func (endpoint *h08PacingEndpoint) OpenConnection(conn net.Conn) (modbus.TCPConnectionHandle, error) {
	endpoint.mu.Lock()
	endpoint.connected = append(endpoint.connected, time.Now())
	endpoint.mu.Unlock()
	return endpoint.fakeEndpoint.OpenConnection(conn)
}

func (endpoint *h08PacingEndpoint) EnqueueRead(plan modbus.TCPReadPlan) (modbus.TCPRequestHandle, error) {
	if endpoint.enqueueEntered != nil {
		close(endpoint.enqueueEntered)
		<-endpoint.enqueueRelease
	}
	endpoint.mu.Lock()
	endpoint.starts = append(endpoint.starts, time.Now())
	endpoint.inFlight++
	endpoint.maxFlight = max(endpoint.maxFlight, endpoint.inFlight)
	endpoint.mu.Unlock()
	// Hold the transaction open briefly so overlapping callers would show.
	time.Sleep(2 * time.Millisecond)
	return endpoint.fakeEndpoint.EnqueueRead(plan)
}

func (endpoint *h08PacingEndpoint) Cancel(handle modbus.TCPRequestHandle) error {
	endpoint.mu.Lock()
	endpoint.ends = append(endpoint.ends, time.Now())
	endpoint.inFlight--
	endpoint.mu.Unlock()
	return endpoint.fakeEndpoint.Cancel(handle)
}

// CloseConnection and WaitReconnect make the fake reconnectable.
func (*h08PacingEndpoint) CloseConnection(modbus.TCPConnectionHandle) error { return nil }
func (*h08PacingEndpoint) WaitReconnect(context.Context, modbus.TCPRequestHandle, modbus.DelayWaiter) error {
	return nil
}

func h08PacedAdapter(t *testing.T, pacing *Pacing) (*Adapter, *h08PacingEndpoint) {
	t.Helper()
	endpoint := &h08PacingEndpoint{}
	dial := func(context.Context, string, string) (net.Conn, error) {
		client, peer := net.Pipe()
		t.Cleanup(func() { _ = peer.Close(); _ = client.Close() })
		return client, nil
	}
	config := validConfig()
	config.Pacing = pacing
	adapter, err := Start(context.Background(), config, dial,
		func(modbus.TCPEndpointConfig) (Endpoint, error) { return endpoint, nil })
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	return adapter, endpoint
}

func h08ReadPlan(t *testing.T) ReadPlan {
	t.Helper()
	request, err := modbus.NewReadRegistersRequest(modbus.FunctionReadHoldingRegisters, 40000, 2)
	if err != nil {
		t.Fatal(err)
	}
	return ReadPlan{UnitID: 1, AuthorizationScope: "h08:pacing", PollGeneration: 1, DeadlineIdentity: 1, Timeout: time.Second,
		Reads: []modbus.TCPLogicalRead{{LogicalViewID: 1, Request: request}}}
}

// TestH08PacingSerializesConcurrentCallersWithGapAndSettle fails before the
// pacing gate: concurrent callers then start back to back with no settle wait
// and no gap between transactions.
func TestH08PacingSerializesConcurrentCallersWithGapAndSettle(t *testing.T) {
	const gap, settle = 30 * time.Millisecond, 45 * time.Millisecond
	adapter, endpoint := h08PacedAdapter(t, &Pacing{Gap: gap, Settle: settle})
	plan := h08ReadPlan(t)
	var wg sync.WaitGroup
	for caller := 0; caller < 4; caller++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2; i++ {
				// Mixed entry points share one gate: plain reads and the
				// reconnect-owning read used by on-demand callers.
				if i == 0 {
					_, _ = adapter.ExecuteRead(context.Background(), plan)
				} else {
					_, _ = adapter.ExecuteReadWithReconnect(context.Background(), plan)
				}
			}
		}()
	}
	wg.Wait()
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if len(endpoint.starts) < 8 || len(endpoint.starts) != len(endpoint.ends) {
		t.Fatalf("requests started=%d completed=%d", len(endpoint.starts), len(endpoint.ends))
	}
	if endpoint.maxFlight != 1 {
		t.Fatalf("requests in flight together = %d, want 1", endpoint.maxFlight)
	}
	if first := endpoint.starts[0].Sub(endpoint.connected[0]); first < settle {
		t.Fatalf("first request %v after connect, want at least the settle wait %v", first, settle)
	}
	for i := 1; i < len(endpoint.starts); i++ {
		if idle := endpoint.starts[i].Sub(endpoint.ends[i-1]); idle < gap {
			t.Fatalf("request %d started %v after the previous completion, want at least %v", i, idle, gap)
		}
	}
}

func TestH08PacingSettlesAgainAfterReconnect(t *testing.T) {
	const gap, settle = 10 * time.Millisecond, 60 * time.Millisecond
	adapter, endpoint := h08PacedAdapter(t, &Pacing{Gap: gap, Settle: settle})
	plan := h08ReadPlan(t)
	_, _ = adapter.ExecuteRead(context.Background(), plan)
	// A fresh connection restarts the settle wait for the next request.
	if err := adapter.Reconnect(context.Background()); err != nil {
		t.Fatalf("Reconnect: %v", err)
	}
	_, _ = adapter.ExecuteRead(context.Background(), plan)
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if len(endpoint.starts) != 2 || len(endpoint.connected) != 2 {
		t.Fatalf("requests = %d connects = %d", len(endpoint.starts), len(endpoint.connected))
	}
	if idle := endpoint.starts[1].Sub(endpoint.connected[1]); idle < settle {
		t.Fatalf("request after reconnect waited %v after the connect, want at least the settle wait %v", idle, settle)
	}
}

func TestH08PacingWaitEndsWithTheCallerBoundAndSendsNothing(t *testing.T) {
	adapter, endpoint := h08PacedAdapter(t, &Pacing{Gap: time.Second, Settle: 2 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := adapter.ExecuteRead(ctx, h08ReadPlan(t))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("paced read past its bound = %v, want the caller deadline", err)
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if len(endpoint.starts) != 0 {
		t.Fatalf("a request was sent although the gate had not opened: %d", len(endpoint.starts))
	}
}

// TestH08NilPacingKeepsConfigurationV1Unpaced pins configuration v1: no settle
// wait and no gap are added to its requests.
func TestH08NilPacingKeepsConfigurationV1Unpaced(t *testing.T) {
	adapter, endpoint := h08PacedAdapter(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = adapter.ExecuteRead(ctx, h08ReadPlan(t))
	_, _ = adapter.ExecuteRead(ctx, h08ReadPlan(t))
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if len(endpoint.starts) != 2 {
		t.Fatalf("v1 requests = %d, want 2", len(endpoint.starts))
	}
	if adapter.config.Pacing != nil {
		t.Fatal("v1 adapter carries a pacing gate")
	}
}

func TestH08PacingContendedAdmissionEndsWithCallerBound(t *testing.T) {
	for _, operation := range []string{"read", "read_with_reconnect", "reconnect"} {
		t.Run(operation, func(t *testing.T) {
			adapter, endpoint := h08PacedAdapter(t, &Pacing{})
			endpoint.enqueueEntered = make(chan struct{})
			endpoint.enqueueRelease = make(chan struct{})
			plan := h08ReadPlan(t)
			var releaseOnce sync.Once
			releaseWorker := func() { releaseOnce.Do(func() { close(endpoint.enqueueRelease) }) }
			workerDone := make(chan error, 1)
			go func() {
				_, err := adapter.ExecuteRead(context.Background(), plan)
				workerDone <- err
			}()
			defer func() {
				releaseWorker()
				<-workerDone
			}()
			select {
			case <-endpoint.enqueueEntered:
			case <-time.After(time.Second):
				t.Fatal("worker did not reach endpoint admission")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			callerDone := make(chan error, 1)
			go func() {
				var err error
				switch operation {
				case "read":
					_, err = adapter.ExecuteRead(ctx, plan)
				case "read_with_reconnect":
					_, err = adapter.ExecuteReadWithReconnect(ctx, plan)
				case "reconnect":
					err = adapter.Reconnect(ctx)
				}
				callerDone <- err
			}()
			select {
			case err := <-callerDone:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("contended caller = %v, want deadline", err)
				}
			case <-time.After(time.Second):
				releaseWorker()
				<-callerDone
				t.Error("caller remained blocked behind active worker after its deadline")
			}
			endpoint.mu.Lock()
			defer endpoint.mu.Unlock()
			if len(endpoint.starts) != 0 || len(endpoint.connected) != 1 {
				t.Errorf("expired admission sent requests or reconnected: requests=%d connects=%d", len(endpoint.starts), len(endpoint.connected))
			}
		})
	}
}
