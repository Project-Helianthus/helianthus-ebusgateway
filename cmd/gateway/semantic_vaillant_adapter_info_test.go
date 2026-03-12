package main

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

type adapterInfoResponse struct {
	data []byte
	err  error
}

type stubAdapterInfoTransport struct {
	mu        sync.Mutex
	responses map[transport.AdapterInfoID][]adapterInfoResponse
	calls     []transport.AdapterInfoID
}

func (s *stubAdapterInfoTransport) ReadByte() (byte, error) { return 0, nil }

func (s *stubAdapterInfoTransport) Write([]byte) (int, error) { return 0, nil }

func (s *stubAdapterInfoTransport) Close() error { return nil }

func (s *stubAdapterInfoTransport) RequestInfo(id transport.AdapterInfoID) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls = append(s.calls, id)
	queue := s.responses[id]
	if len(queue) == 0 {
		return nil, errors.New("unexpected info request")
	}
	resp := queue[0]
	s.responses[id] = queue[1:]
	if resp.err != nil {
		return nil, resp.err
	}
	return append([]byte(nil), resp.data...), nil
}

func (s *stubAdapterInfoTransport) callCount(id transport.AdapterInfoID) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for _, call := range s.calls {
		if call == id {
			count++
		}
	}
	return count
}

func TestVaillantAdapterInfoStateRefreshCycleRetriesIdentity(t *testing.T) {
	t.Parallel()

	raw := &stubAdapterInfoTransport{
		responses: map[transport.AdapterInfoID][]adapterInfoResponse{
			transport.AdapterInfoVersion: {
				{err: errors.New("warmup")},
				{data: []byte{0x31, 0x01, 0x12, 0x34, 0x0C}},
			},
			transport.AdapterInfoHardwareID: {
				{data: []byte{0xDE, 0xAD}},
			},
			transport.AdapterInfoHardwareConf: {
				{data: []byte{0xBE, 0xEF}},
			},
			transport.AdapterInfoTemperature: {
				{data: []byte{0x00, 0x19}},
			},
			transport.AdapterInfoSupplyVolt: {
				{data: []byte{0x09, 0xC4}},
			},
			transport.AdapterInfoBusVoltage: {
				{data: []byte{0x96, 0x82}},
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := protocol.NewBus(raw, protocol.DefaultBusConfig(), 0)
	bus.Run(ctx)

	provider := graphql.NewLiveSemanticProvider()
	state := newVaillantAdapterInfoState(bus, raw, provider)
	if state == nil {
		t.Fatal("newVaillantAdapterInfoState() returned nil")
	}

	state.refreshCycle(ctx)
	state.refreshCycle(ctx)

	info := provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() = nil")
	}
	if got := raw.callCount(transport.AdapterInfoVersion); got != 2 {
		t.Fatalf("AdapterInfoVersion call count = %d; want 2", got)
	}
	if got := info.FirmwareVersion; got != "0x31" {
		t.Fatalf("FirmwareVersion = %q; want 0x31", got)
	}
	if got := info.HardwareID; got != "dead" {
		t.Fatalf("HardwareID = %q; want dead", got)
	}
	if got := info.HardwareConfig; got != "beef" {
		t.Fatalf("HardwareConfig = %q; want beef", got)
	}
	if info.TemperatureC == nil || *info.TemperatureC != 25 {
		t.Fatalf("TemperatureC = %v; want 25", info.TemperatureC)
	}
	if info.SupplyVoltageMV == nil || *info.SupplyVoltageMV != 2500 {
		t.Fatalf("SupplyVoltageMV = %v; want 2500", info.SupplyVoltageMV)
	}
	if info.BusVoltageMaxDV == nil || *info.BusVoltageMaxDV != 150 {
		t.Fatalf("BusVoltageMaxDV = %v; want 150", info.BusVoltageMaxDV)
	}
	if info.BusVoltageMinDV == nil || *info.BusVoltageMinDV != 130 {
		t.Fatalf("BusVoltageMinDV = %v; want 130", info.BusVoltageMinDV)
	}
	if !info.InfoSupported {
		t.Fatal("InfoSupported = false; want true")
	}
	if got := info.VersionResponseLen; got != 5 {
		t.Fatalf("VersionResponseLen = %d; want 5", got)
	}
}
