package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

type growattEndpointFake struct {
	mu         sync.Mutex
	words      map[uint16][]uint16
	calls      [][2]uint16
	unitIDs    []byte
	failAt     int
	mismatch   int
	generation uint64
	closed     int
	recovers   int
	recoverErr error
}

func TestPortalRawModbusUsesOnlyTCPAvailableComposition(t *testing.T) {
	var disabledRuntime *growattBMSRS485ProductionProvider
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	for name, provider := range map[string]mcp.ModbusV1Provider{
		"disabled": newGatewayModbusMCPProviderWithGrowatt(nil, disabledRuntime),
		"tcp-only": newGatewayModbusMCPProviderWithGrowatt(&modbusadapter.Adapter{}, disabledRuntime),
		"bms-only": newGatewayModbusMCPProviderWithGrowatt(nil, runtime),
		"tcp-bms":  newGatewayModbusMCPProviderWithGrowatt(&modbusadapter.Adapter{}, runtime),
	} {
		t.Run(name, func(t *testing.T) {
			portalProvider := portalTCPModbusProvider(provider)
			wantTCP := name == "tcp-only" || name == "tcp-bms"
			if (portalProvider != nil) != wantTCP {
				t.Fatalf("portal provider=%T; want TCP=%t", portalProvider, wantTCP)
			}
			handler := portal.NewHandler(portal.Options{RawModbusEnabled: true, ModbusProvider: portalProvider})
			bootstrap := httptest.NewRecorder()
			handler.ServeHTTP(bootstrap, httptest.NewRequest(http.MethodGet, "/api/v1/bootstrap", nil))
			var payload map[string]any
			if err := json.Unmarshal(bootstrap.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			capabilities := payload["capabilities"].(map[string]any)
			if got := capabilities["modbus_raw_read"]; got != wantTCP {
				t.Fatalf("bootstrap capability=%#v", got)
			}
			raw := httptest.NewRecorder()
			handler.ServeHTTP(raw, httptest.NewRequest(http.MethodPost, "/api/v1/explorer/modbus/raw-read", nil))
			if wantTCP && raw.Code != http.StatusUnsupportedMediaType || !wantTCP && raw.Code != http.StatusNotFound {
				t.Fatalf("raw route status=%d wantTCP=%t", raw.Code, wantTCP)
			}
		})
	}
}

func growattBMSProductionWords() map[uint16][]uint16 {
	identity := make([]uint16, 7)
	identity[0], identity[1] = 0x0102, 0x0304
	status := make([]uint16, 29)
	status[0], status[1] = 0x0204, 0x0301
	status[6], status[8], status[9], status[10], status[11] = 2, 75, 5200, 0xff9c, 25
	status[13], status[14], status[17] = 3200, 5000, 110
	extension := make([]uint16, 12)
	extension[0], extension[1], extension[2], extension[4], extension[5], extension[6] = 100, 123, 3300, 512, 5, 6
	return map[uint16][]uint16{0x0001: identity, 0x000d: status, 0x0100: extension, 0x010d: {0, 0}}
}

func growattBMSProductionReadPDU(words []uint16) []byte {
	pdu := make([]byte, 2+len(words)*2)
	pdu[0], pdu[1] = byte(modbus.FunctionReadHoldingRegisters), byte(len(words)*2)
	for index, word := range words {
		pdu[2+index*2], pdu[3+index*2] = byte(word>>8), byte(word)
	}
	return pdu
}

func (fake *growattEndpointFake) Read(_ context.Context, unit byte, request modbus.ReadRegistersRequest) (modbus.ReadRegistersResponse, modbus.RTUReadEvidence, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.unitIDs = append(fake.unitIDs, unit)
	fake.calls = append(fake.calls, [2]uint16{request.Offset(), request.Quantity()})
	index := len(fake.calls) - 1
	if index == fake.failAt {
		return modbus.ReadRegistersResponse{}, modbus.RTUReadEvidence{TerminalOutcome: "transport_fault"}, errors.New("fixture transport fault")
	}
	response, err := modbus.DecodeReadRegistersResponse(request, growattBMSProductionReadPDU(fake.words[request.Offset()]))
	if err != nil {
		return modbus.ReadRegistersResponse{}, modbus.RTUReadEvidence{}, err
	}
	if index == fake.mismatch {
		response.Provenance.Offset++
	}
	words := append([]uint16(nil), fake.words[request.Offset()]...)
	return response, modbus.RTUReadEvidence{
		Generation: fake.generation, RequestID: uint64(index + 1), UnitID: unit, Function: request.Function(),
		Offset: request.Offset(), Quantity: request.Quantity(), RequestADU: []byte{byte(unit), byte(request.Function()), byte(request.Offset() >> 8), byte(request.Offset())},
		ResponseADU: []byte{byte(unit), byte(request.Function()), byte(len(words) * 2)}, Words: words,
		Current: true, IntegrityValid: true, TerminalOutcome: "success", ReceiptWall: time.Unix(1_800_000_000, int64(index)),
		ReceivedAt: time.Duration(index+1) * time.Millisecond, ClockEpoch: growattBMSRS485ClockEpoch,
	}, nil
}

func (fake *growattEndpointFake) Recover(context.Context) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.recovers++
	if fake.recoverErr != nil {
		return fake.recoverErr
	}
	fake.generation++
	return nil
}

func (fake *growattEndpointFake) Close() error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.closed++
	return nil
}

func growattProductionConfig() ebusgateway.GrowattBMSRS485Config {
	return ebusgateway.GrowattBMSRS485Config{
		Enabled: true, AssetID: "asset:growatt-bms-a", SourceID: "growatt-bms-a", SourceEpoch: "source-epoch-1", DriverGeneration: 1, UnitID: 7,
		SerialPath: "/dev/fixture", Baud: 9600, Parity: "even", StopBits: 1,
		ResponseTimeout: time.Second, MaxResponseDelay: 100 * time.Millisecond, MaxQuiescence: 200 * time.Millisecond,
	}
}

func startGrowattRuntimeWithFake(t *testing.T, config ebusgateway.GrowattBMSRS485Config, fake *growattEndpointFake) *growattBMSRS485ProductionProvider {
	t.Helper()
	original := openGrowattBMSRTUEndpoint
	openGrowattBMSRTUEndpoint = func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { return fake, nil }
	t.Cleanup(func() { openGrowattBMSRTUEndpoint = original })
	runtime, err := startGrowattBMSRS485Runtime(config)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestGrowattBMSRS485ProductionCompositionBindsFourReadsAndImmutableEvidence(t *testing.T) {
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 4}
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	provider := newGatewayModbusMCPProviderWithGrowatt(nil, runtime)
	if _, ok := provider.(mcp.GrowattBMSRS485V202Provider); !ok {
		t.Fatalf("provider %T does not compose Growatt runtime", provider)
	}
	observation, err := provider.(mcp.GrowattBMSRS485V202Provider).GrowattBMSRS485V202(context.Background())
	if err != nil || observation.Status.OutboundAllowed() || observation.ReceiptWall.IsZero() {
		t.Fatalf("observation/error=%#v/%v", observation, err)
	}
	want := [][2]uint16{{0x0001, 7}, {0x000d, 29}, {0x0100, 12}, {0x010d, 2}}
	if !reflect.DeepEqual(fake.calls, want) || len(fake.unitIDs) != 4 {
		t.Fatalf("wire calls/units=%#v/%#v", fake.calls, fake.unitIDs)
	}
	evidence, ok := runtime.LastObservationEvidence()
	if !ok || evidence.SourceEpoch != "source-epoch-1" || evidence.DriverGeneration != 1 || evidence.Qualification != "qualified" || evidence.OutboundAllowed || len(evidence.Slices) != 4 {
		t.Fatalf("evidence=%#v/%t", evidence, ok)
	}
	if !observation.ReceiptWall.Equal(evidence.ReceiptWall) {
		t.Fatalf("observation receipt=%s evidence receipt=%s", observation.ReceiptWall, evidence.ReceiptWall)
	}
	if evidence.Slices[0].TransportGeneration != 4 || evidence.Slices[0].RequestADUHex == "" || evidence.Slices[0].ResponseADUHex == "" {
		t.Fatalf("slice evidence=%#v", evidence.Slices[0])
	}
	evidence.Slices[0].Words[0] = 0xffff
	again, ok := runtime.LastObservationEvidence()
	if !ok || again.Slices[0].Words[0] == 0xffff {
		t.Fatalf("evidence was mutable: %#v", again)
	}
	if err := runtime.Close(); err != nil || fake.closed != 1 {
		t.Fatalf("close/error=%d/%v", fake.closed, err)
	}
}

func TestGrowattBMSRS485SemanticStoragePublishesOneAtomicSemRegView(t *testing.T) {
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 4}
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	view, err := runtime.GrowattStorageSemanticCurrent(context.Background())
	if err != nil {
		t.Fatalf("semantic storage publish: %v", err)
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var public map[string]any
	if err := json.Unmarshal(encoded, &public); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"snapshot", "evaluation", "selections", "projection"} {
		if _, ok := public[key]; !ok {
			t.Fatalf("semantic storage view lacks %s: %s", key, encoded)
		}
	}
	if _, ok := runtime.storage.Current("asset:growatt-bms-a"); !ok {
		t.Fatal("published storage asset is not available through its configured asset ID")
	}
}

func TestGrowattBMSRS485SemanticStorageSerializesConcurrentObservations(t *testing.T) {
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 4}
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	var group sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := runtime.GrowattStorageSemanticCurrent(context.Background())
			errs <- err
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent semantic storage publish: %v", err)
		}
	}
	if _, ok := runtime.storage.Current("asset:growatt-bms-a"); !ok {
		t.Fatal("concurrent publication lost configured asset view")
	}
}

func TestGatewayModbusMCPProviderGrowattOptionalInterfaceMatrix(t *testing.T) {
	// This matches runGatewayLifecycle: startGrowattBMSRS485Runtime returns a
	// concrete nil pointer when disabled, which must not become a non-nil MCP
	// optional-provider interface.
	var disabledRuntime *growattBMSRS485ProductionProvider
	if provider := newGatewayModbusMCPProviderWithGrowatt(nil, disabledRuntime); provider != nil {
		t.Fatalf("disabled provider=%T", provider)
	}
	tcpOnly := newGatewayModbusMCPProviderWithGrowatt(&modbusadapter.Adapter{}, disabledRuntime)
	if _, ok := tcpOnly.(mcp.GrowattBMSRS485V202Provider); ok {
		t.Fatalf("TCP-only provider unexpectedly implements Growatt optional interface: %T", tcpOnly)
	}
	if availability, ok := tcpOnly.(interface{ ModbusV1CoreAvailable() bool }); !ok || !availability.ModbusV1CoreAvailable() {
		t.Fatalf("TCP-only core availability=%T/%t", tcpOnly, ok)
	}
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	for name, provider := range map[string]mcp.ModbusV1Provider{
		"bms-only": newGatewayModbusMCPProviderWithGrowatt(nil, runtime),
		"tcp-bms":  newGatewayModbusMCPProviderWithGrowatt(&modbusadapter.Adapter{}, runtime),
	} {
		t.Run(name, func(t *testing.T) {
			growatt, ok := provider.(mcp.GrowattBMSRS485V202Provider)
			if !ok {
				t.Fatalf("provider %T omits Growatt optional interface", provider)
			}
			availability, available := provider.(interface{ ModbusV1CoreAvailable() bool })
			if !available || availability.ModbusV1CoreAvailable() != (name == "tcp-bms") {
				core := provider.(gatewayGrowattBMSMCPProvider).gatewayModbusMCPProvider
				t.Fatalf("%s core availability=%T/%t/%t adapter=%#v", name, provider, available, availability.ModbusV1CoreAvailable(), core.adapter)
			}
			if _, err := growatt.GrowattBMSRS485V202(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGrowattBMSRS485ProductionCompositionFailsClosed(t *testing.T) {
	for name, config := range map[string]ebusgateway.GrowattBMSRS485Config{
		"disabled-active": {SourceID: "retained"},
		"wrong-unit":      func() ebusgateway.GrowattBMSRS485Config { c := growattProductionConfig(); c.UnitID = 248; return c }(),
		"missing-epoch":   func() ebusgateway.GrowattBMSRS485Config { c := growattProductionConfig(); c.SourceEpoch = ""; return c }(),
	} {
		t.Run(name, func(t *testing.T) {
			called := false
			original := openGrowattBMSRTUEndpoint
			openGrowattBMSRTUEndpoint = func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { called = true; return nil, nil }
			t.Cleanup(func() { openGrowattBMSRTUEndpoint = original })
			if _, err := startGrowattBMSRS485Runtime(config); err == nil || called {
				t.Fatalf("err/open=%v/%t", err, called)
			}
		})
	}
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: 2, mismatch: -1, generation: 1}
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	if _, err := runtime.GrowattBMSRS485V202(context.Background()); err == nil || len(fake.calls) != 3 {
		t.Fatalf("partial failure error/calls=%v/%#v", err, fake.calls)
	}
	if _, ok := runtime.LastObservationEvidence(); ok {
		t.Fatal("partial failure retained a qualified observation")
	}
	stale := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 0}
	runtime = startGrowattRuntimeWithFake(t, growattProductionConfig(), stale)
	if _, err := runtime.GrowattBMSRS485V202(context.Background()); err == nil || len(stale.calls) != 1 {
		t.Fatalf("stale generation error/calls=%v/%#v", err, stale.calls)
	}
	if _, ok := runtime.LastObservationEvidence(); ok {
		t.Fatal("stale generation retained a qualified observation")
	}
}

func TestGrowattBMSRS485ProductionCompositionRecoversOnlyBeforeLaterSample(t *testing.T) {
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: 1, mismatch: -1, generation: 1}
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	if _, err := runtime.GrowattBMSRS485V202(context.Background()); err == nil || len(fake.calls) != 2 || fake.recovers != 0 {
		t.Fatalf("failed sample calls/recover=%#v/%d", fake.calls, fake.recovers)
	}
	fake.mu.Lock()
	fake.failAt = -1
	fake.mu.Unlock()
	if _, err := runtime.GrowattBMSRS485V202(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fake.calls) != 6 || fake.recovers != 1 {
		t.Fatalf("later sample calls/recover=%#v/%d", fake.calls, fake.recovers)
	}
	evidence, ok := runtime.LastObservationEvidence()
	if !ok || evidence.Slices[0].TransportGeneration != 2 || len(evidence.Slices) != 4 {
		t.Fatalf("recovered evidence=%#v/%t", evidence, ok)
	}
}

func TestGrowattBMSRS485ProductionCompositionFailedRecoverFailsClosed(t *testing.T) {
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: 0, mismatch: -1, generation: 1, recoverErr: errors.New("recovery unavailable")}
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	if _, err := runtime.GrowattBMSRS485V202(context.Background()); err == nil {
		t.Fatal("failed sample unexpectedly succeeded")
	}
	if _, err := runtime.GrowattBMSRS485V202(context.Background()); !errors.Is(err, fake.recoverErr) || len(fake.calls) != 1 || fake.recovers != 1 {
		t.Fatalf("recover error/calls/attempts=%v/%#v/%d", err, fake.calls, fake.recovers)
	}
}

func TestGrowattBMSRS485ProductionCompositionSerializesPollRecoveryAndClose(t *testing.T) {
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: 0, mismatch: -1, generation: 1}
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	_, _ = runtime.GrowattBMSRS485V202(context.Background())
	fake.mu.Lock()
	fake.failAt = -1
	fake.mu.Unlock()
	if _, err := runtime.GrowattBMSRS485V202(context.Background()); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, _ = runtime.GrowattBMSRS485V202(context.Background())
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		_ = runtime.Close()
	}()
	wait.Wait()
	if fake.recovers != 1 || fake.closed != 1 {
		t.Fatalf("recover/close=%d/%d", fake.recovers, fake.closed)
	}
}

func TestGrowattBMSRS485ProductionCompositionRejectsResponseBindingAndTracksEpochGeneration(t *testing.T) {
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: 1, generation: 1}
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	if _, err := runtime.GrowattBMSRS485V202(context.Background()); err == nil || len(fake.calls) != 2 {
		t.Fatalf("binding error/calls=%v/%#v", err, fake.calls)
	}
	if _, ok := runtime.LastObservationEvidence(); ok {
		t.Fatal("mismatched response retained a qualified observation")
	}

	second := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 2}
	config := growattProductionConfig()
	config.SourceEpoch, config.DriverGeneration = "source-epoch-2", 2
	reconnected := startGrowattRuntimeWithFake(t, config, second)
	if _, err := reconnected.GrowattBMSRS485V202(context.Background()); err != nil {
		t.Fatal(err)
	}
	evidence, ok := reconnected.LastObservationEvidence()
	if !ok || evidence.SourceEpoch != "source-epoch-2" || evidence.DriverGeneration != 2 || evidence.Slices[0].TransportGeneration != 2 {
		t.Fatalf("reconnected evidence=%#v/%t", evidence, ok)
	}
}
