package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

type countingModbusMCPAdapter struct {
	reads             int
	reconnects        int
	reconnectRequired bool
	reconnectErr      error
	reconnectWait     bool
	readErrors        []error
	plans             []modbusadapter.ReadPlan
}

func (adapter *countingModbusMCPAdapter) ExecuteRead(_ context.Context, plan modbusadapter.ReadPlan) (modbus.TCPReadBatch, error) {
	adapter.reads++
	adapter.plans = append(adapter.plans, plan)
	if len(adapter.readErrors) >= adapter.reads {
		return modbus.TCPReadBatch{}, adapter.readErrors[adapter.reads-1]
	}
	return modbus.TCPReadBatch{}, errors.New("fixture transport unavailable")
}

func (adapter *countingModbusMCPAdapter) ExecuteReadWithReconnect(ctx context.Context, plan modbusadapter.ReadPlan) (modbus.TCPReadBatch, error) {
	batch, err := adapter.ExecuteRead(ctx, plan)
	if err == nil || !adapter.reconnectRequired {
		return batch, err
	}
	if reconnectErr := adapter.Reconnect(ctx); reconnectErr != nil {
		return modbus.TCPReadBatch{}, reconnectErr
	}
	adapter.reconnectRequired = false
	return adapter.ExecuteRead(ctx, plan)
}

func (adapter *countingModbusMCPAdapter) Snapshot() modbus.TCPEndpointSnapshot {
	return modbus.TCPEndpointSnapshot{ReconnectRequired: adapter.reconnectRequired}
}

func (adapter *countingModbusMCPAdapter) Reconnect(ctx context.Context) error {
	adapter.reconnects++
	if adapter.reconnectWait {
		<-ctx.Done()
		return ctx.Err()
	}
	return adapter.reconnectErr
}

func (*countingModbusMCPAdapter) ProfileObservation(string, string) (modbusadapter.ProfileObservationRecord, bool) {
	return modbusadapter.ProfileObservationRecord{}, false
}

func (*countingModbusMCPAdapter) SunSpecQualificationObservation(string, string) (modbusreg.SunSpecQualificationObservation, []byte, bool) {
	return modbusreg.SunSpecQualificationObservation{}, nil, false
}

func (*countingModbusMCPAdapter) CanonicalPVSnapshot(string, string) (pv.Snapshot, time.Time, bool) {
	return pv.Snapshot{}, time.Time{}, false
}

func TestNewGatewayModbusMCPProviderDisabledIsInert(t *testing.T) {
	if provider := newGatewayModbusMCPProvider(nil); provider != nil {
		t.Fatalf("nil adapter produced provider %T", provider)
	}
}

func TestEndpointReferenceDoesNotExposeEndpoint(t *testing.T) {
	endpoint := "tcp://192.0.2.10:502"
	reference := endpointReference(endpoint)
	if reference == endpoint || len(reference) != len("sha256:")+64 {
		t.Fatalf("endpoint reference = %q", reference)
	}
	if reference != endpointReference(endpoint) {
		t.Fatal("endpoint reference is nondeterministic")
	}
}

func TestRedactModbusEndpointsCoversNestedObservationProvenance(t *testing.T) {
	const endpoint = "tcp://operator:secret@192.0.2.10:502"
	observation := map[string]any{
		"endpoint":          endpoint,
		"endpoint_identity": endpoint,
		"dependencies": []any{map[string]any{
			"view": map[string]any{"Endpoint": endpoint, "words": []any{1.0, 2.0}},
		}},
	}

	redactModbusEndpoints(observation)
	if got := observation["endpoint"]; got != endpointReference(endpoint) {
		t.Fatalf("top-level endpoint = %q", got)
	}
	if got := observation["endpoint_identity"]; got != endpointReference(endpoint) {
		t.Fatalf("endpoint identity = %q", got)
	}
	view := observation["dependencies"].([]any)[0].(map[string]any)["view"].(map[string]any)
	if got := view["Endpoint"]; got != endpointReference(endpoint) {
		t.Fatalf("nested endpoint = %q", got)
	}
	if strings.Contains(view["Endpoint"].(string), "secret") {
		t.Fatal("nested endpoint leaked credential material")
	}
}

func TestGatewayModbusMCPProviderRejectsBurstBeforeWireIO(t *testing.T) {
	adapter := &countingModbusMCPAdapter{}
	now := time.Unix(1_800_000_000, 0)
	provider := &gatewayModbusMCPProvider{adapter: adapter, now: func() time.Time { return now }}
	request := mcp.ModbusRawReadRequest{UnitID: 1, Function: 3, Offset: 0, Quantity: 1}
	for range mcp.ModbusV1MaxRawReadsPerWindow {
		_, _ = provider.RawRead(context.Background(), request)
	}
	if _, err := provider.RawRead(context.Background(), request); !errors.Is(err, mcp.ErrModbusV1ResourceExhausted) {
		t.Fatalf("burst error = %v", err)
	}
	if adapter.reads != mcp.ModbusV1MaxRawReadsPerWindow {
		t.Fatalf("wire reads = %d; want %d", adapter.reads, mcp.ModbusV1MaxRawReadsPerWindow)
	}
	now = now.Add(mcp.ModbusV1RawReadWindow)
	_, _ = provider.RawRead(context.Background(), request)
	if adapter.reads != mcp.ModbusV1MaxRawReadsPerWindow+1 {
		t.Fatalf("wire reads after refill = %d", adapter.reads)
	}
}

func TestGatewayModbusMCPProviderReconnectsAndRetriesOnceInsideOneQuotaAdmission(t *testing.T) {
	firstErr := errors.New("fixture retryable transport failure")
	secondErr := errors.New("fixture retry remained unavailable")
	adapter := &countingModbusMCPAdapter{
		reconnectRequired: true,
		readErrors:        []error{firstErr, secondErr},
	}
	provider := &gatewayModbusMCPProvider{adapter: adapter, now: time.Now}
	request := mcp.ModbusRawReadRequest{UnitID: 7, Function: 3, Offset: 40000, Quantity: 2}

	_, err := provider.RawRead(context.Background(), request)
	if !errors.Is(err, secondErr) {
		t.Fatalf("RawRead error = %v; want retry error", err)
	}
	if adapter.reads != 2 || adapter.reconnects != 1 {
		t.Fatalf("physical attempts/reconnects = %d/%d; want 2/1", adapter.reads, adapter.reconnects)
	}
	if provider.rateN != 1 {
		t.Fatalf("quota admissions = %d; want 1", provider.rateN)
	}
	if len(adapter.plans) != 2 || !reflect.DeepEqual(adapter.plans[0], adapter.plans[1]) {
		t.Fatalf("retry changed immutable admitted plan: %#v", adapter.plans)
	}
}

func TestGatewayModbusMCPProviderDoesNotReconnectWithoutOwnerAuthorization(t *testing.T) {
	adapter := &countingModbusMCPAdapter{
		readErrors: []error{errors.New("permanent provider failure")},
	}
	provider := &gatewayModbusMCPProvider{adapter: adapter, now: time.Now}
	request := mcp.ModbusRawReadRequest{UnitID: 7, Function: 3, Offset: 40000, Quantity: 1}

	_, _ = provider.RawRead(context.Background(), request)
	if adapter.reads != 1 || adapter.reconnects != 0 {
		t.Fatalf("physical attempts/reconnects = %d/%d; want 1/0", adapter.reads, adapter.reconnects)
	}
}

func TestGatewayModbusMCPProviderStopsWhenReconnectFails(t *testing.T) {
	firstErr := errors.New("fixture retryable transport failure")
	reconnectErr := errors.New("fixture reconnect failure")
	adapter := &countingModbusMCPAdapter{
		reconnectRequired: true,
		reconnectErr:      reconnectErr,
		readErrors:        []error{firstErr},
	}
	provider := &gatewayModbusMCPProvider{adapter: adapter, now: time.Now}
	request := mcp.ModbusRawReadRequest{UnitID: 7, Function: 3, Offset: 40000, Quantity: 1}

	_, err := provider.RawRead(context.Background(), request)
	if !errors.Is(err, reconnectErr) {
		t.Fatalf("RawRead error = %v; want reconnect error", err)
	}
	if adapter.reads != 1 || adapter.reconnects != 1 {
		t.Fatalf("physical attempts/reconnects = %d/%d; want 1/1", adapter.reads, adapter.reconnects)
	}
}

func TestGatewayModbusMCPProviderReconnectHonorsShorterCallerDeadline(t *testing.T) {
	adapter := &countingModbusMCPAdapter{
		reconnectRequired: true,
		reconnectWait:     true,
		readErrors:        []error{errors.New("fixture retryable transport failure")},
	}
	provider := &gatewayModbusMCPProvider{adapter: adapter, now: time.Now}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := provider.RawRead(ctx, mcp.ModbusRawReadRequest{
		UnitID: 7, Function: 3, Offset: 40000, Quantity: 1,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RawRead error = %v; want caller deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded reconnect took %s", elapsed)
	}
	if adapter.reads != 1 || adapter.reconnects != 1 {
		t.Fatalf("physical attempts/reconnects = %d/%d; want 1/1", adapter.reads, adapter.reconnects)
	}
}

func TestGatewayModbusMCPProviderReturnsOnlyRecoveredConnectionData(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	peerDone := make(chan error, 1)
	requests := make(chan [12]byte, 2)
	go func() {
		for attempt := 0; attempt < 2; attempt++ {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				peerDone <- acceptErr
				return
			}
			var request [12]byte
			_, readErr := io.ReadFull(connection, request[:])
			if readErr != nil {
				_ = connection.Close()
				peerDone <- readErr
				return
			}
			requests <- request
			if attempt == 0 {
				_ = connection.Close()
				continue
			}
			response := make([]byte, 11)
			copy(response[0:4], request[0:4])
			binary.BigEndian.PutUint16(response[4:6], 5)
			response[6], response[7], response[8] = request[6], request[7], 2
			binary.BigEndian.PutUint16(response[9:11], 0x1234)
			_, writeErr := connection.Write(response)
			_ = connection.Close()
			peerDone <- writeErr
			return
		}
	}()

	config, err := mapModbusRuntimeConfig(ebusgateway.ModbusTCPConfig{
		Enabled: true, Endpoint: "tcp://" + listener.Addr().String(), DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("map Modbus config: %v", err)
	}
	config.Endpoint.Backoff.Floor = time.Millisecond
	config.Endpoint.Backoff.Ceiling = time.Millisecond
	config.Endpoint.Backoff.MaxAttempts = 1
	adapter, err := modbusadapter.Start(
		context.Background(), config,
		(&net.Dialer{}).DialContext,
		func(config modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
			return modbus.NewTCPEndpoint(config)
		},
	)
	if err != nil {
		t.Fatalf("start Modbus adapter: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	provider := newGatewayModbusMCPProvider(adapter).(*gatewayModbusMCPProvider)
	result, err := provider.RawRead(context.Background(), mcp.ModbusRawReadRequest{
		UnitID: 1, Function: 3, Offset: 40000, Quantity: 1,
	})
	if err != nil {
		t.Fatalf("RawRead after recoverable socket loss: %v", err)
	}
	if !reflect.DeepEqual(result.Words, []uint16{0x1234}) || result.TransportGeneration < 2 || result.ConnectionID < 2 {
		t.Fatalf("recovered result = %#v", result)
	}
	if provider.rateN != 1 {
		t.Fatalf("quota admissions = %d; want 1", provider.rateN)
	}
	first, second := <-requests, <-requests
	if !reflect.DeepEqual(first[6:], second[6:]) {
		t.Fatalf("retry changed immutable Modbus PDU: first=%x second=%x", first[6:], second[6:])
	}
	if err := <-peerDone; err != nil {
		t.Fatalf("fake peer: %v", err)
	}
}

func TestGatewayModbusMCPProviderReportsUnavailableForUnretainedQualificationSample(t *testing.T) {
	provider := &gatewayModbusMCPProvider{adapter: &countingModbusMCPAdapter{}}
	_, err := provider.ProfileObservation(context.Background(), "sunspec.inverter.three_phase.monitoring@1.0.0", "sunspec-44-94")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("unretained qualification MCP error = %v; want unavailable", err)
	}
}
