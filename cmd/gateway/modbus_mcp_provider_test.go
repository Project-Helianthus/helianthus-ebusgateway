package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

type countingModbusMCPAdapter struct{ reads int }

func (adapter *countingModbusMCPAdapter) ExecuteRead(context.Context, modbusadapter.ReadPlan) (modbus.TCPReadBatch, error) {
	adapter.reads++
	return modbus.TCPReadBatch{}, errors.New("fixture transport unavailable")
}

func (*countingModbusMCPAdapter) ProfileObservation(string, string) (modbusadapter.ProfileObservationRecord, bool) {
	return modbusadapter.ProfileObservationRecord{}, false
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

func TestGatewayModbusMCPProviderReportsUnavailableForUnretainedQualificationSample(t *testing.T) {
	provider := &gatewayModbusMCPProvider{adapter: &countingModbusMCPAdapter{}}
	_, err := provider.ProfileObservation(context.Background(), "sunspec.inverter.three_phase.monitoring@1.0.0", "sunspec-44-94")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("unretained qualification MCP error = %v; want unavailable", err)
	}
}
