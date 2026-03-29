package main

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
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

type stubRawTransport struct{}

func (s *stubAdapterInfoTransport) ReadByte() (byte, error) { return 0, nil }

func (s *stubAdapterInfoTransport) Write([]byte) (int, error) { return 0, nil }

func (s *stubAdapterInfoTransport) Close() error { return nil }

func (stubRawTransport) ReadByte() (byte, error) { return 0, nil }

func (stubRawTransport) Write([]byte) (int, error) { return 0, nil }

func (stubRawTransport) Close() error { return nil }

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

func TestVaillantAdapterInfoStateRefreshCycleRetriesPartialIdentity(t *testing.T) {
	t.Parallel()

	raw := &stubAdapterInfoTransport{
		responses: map[transport.AdapterInfoID][]adapterInfoResponse{
			transport.AdapterInfoVersion: {
				{data: []byte{0x31, 0x01, 0x12, 0x34, 0x0C}},
				{data: []byte{0x31, 0x01, 0x12, 0x34, 0x0C}},
			},
			transport.AdapterInfoHardwareID: {
				{err: errors.New("warmup")},
				{data: []byte{0xDE, 0xAD}},
			},
			transport.AdapterInfoHardwareConf: {
				{err: errors.New("warmup")},
				{data: []byte{0xBE, 0xEF}},
			},
			transport.AdapterInfoTemperature: {
				{data: []byte{0x00, 0x19}},
				{data: []byte{0x00, 0x19}},
			},
			transport.AdapterInfoSupplyVolt: {
				{data: []byte{0x09, 0xC4}},
				{data: []byte{0x09, 0xC4}},
			},
			transport.AdapterInfoBusVoltage: {
				{data: []byte{0x96, 0x82}},
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
	if got := raw.callCount(transport.AdapterInfoHardwareID); got != 2 {
		t.Fatalf("AdapterInfoHardwareID call count = %d; want 2", got)
	}
	if got := raw.callCount(transport.AdapterInfoHardwareConf); got != 2 {
		t.Fatalf("AdapterInfoHardwareConf call count = %d; want 2", got)
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

func TestVaillantAdapterInfoStateRefreshCycleRebootstrapsAfterTransportClose(t *testing.T) {
	t.Parallel()

	raw := &stubAdapterInfoTransport{
		responses: map[transport.AdapterInfoID][]adapterInfoResponse{
			transport.AdapterInfoVersion: {
				{data: []byte{0x31, 0x01, 0x12, 0x34, 0x0C}},
				{data: []byte{0x32, 0x01, 0x56, 0x78, 0x0C}},
			},
			transport.AdapterInfoHardwareID: {
				{data: []byte{0xAA, 0xAA}},
				{data: []byte{0xBB, 0xBB}},
			},
			transport.AdapterInfoHardwareConf: {
				{data: []byte{0xCC, 0xCC}},
				{data: []byte{0xDD, 0xDD}},
			},
			transport.AdapterInfoTemperature: {
				{data: []byte{0x00, 0x19}},
				{err: ebuserrors.ErrTransportClosed},
				{data: []byte{0x00, 0x1A}},
			},
			transport.AdapterInfoSupplyVolt: {
				{data: []byte{0x09, 0xC4}},
				{data: []byte{0x09, 0xD8}},
			},
			transport.AdapterInfoBusVoltage: {
				{data: []byte{0x96, 0x82}},
				{data: []byte{0x97, 0x83}},
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
	info := provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() after bootstrap = nil")
	}
	if got := info.HardwareID; got != "aaaa" {
		t.Fatalf("HardwareID after bootstrap = %q; want aaaa", got)
	}
	if got := info.HardwareConfig; got != "cccc" {
		t.Fatalf("HardwareConfig after bootstrap = %q; want cccc", got)
	}

	state.refreshCycle(ctx)
	info = provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() after transport close = nil")
	}
	if got := info.HardwareID; got != "" {
		t.Fatalf("HardwareID after transport close = %q; want empty", got)
	}
	if got := info.HardwareConfig; got != "" {
		t.Fatalf("HardwareConfig after transport close = %q; want empty", got)
	}
	if info.TemperatureC != nil {
		t.Fatalf("TemperatureC after transport close = %v; want nil", info.TemperatureC)
	}

	state.refreshCycle(ctx)
	info = provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() after rebootstrap = nil")
	}
	if got := raw.callCount(transport.AdapterInfoVersion); got != 2 {
		t.Fatalf("AdapterInfoVersion call count = %d; want 2", got)
	}
	if got := info.FirmwareVersion; got != "0x32" {
		t.Fatalf("FirmwareVersion after rebootstrap = %q; want 0x32", got)
	}
	if got := info.HardwareID; got != "bbbb" {
		t.Fatalf("HardwareID after rebootstrap = %q; want bbbb", got)
	}
	if got := info.HardwareConfig; got != "dddd" {
		t.Fatalf("HardwareConfig after rebootstrap = %q; want dddd", got)
	}
	if info.TemperatureC == nil || *info.TemperatureC != 26 {
		t.Fatalf("TemperatureC after rebootstrap = %v; want 26", info.TemperatureC)
	}
}

func TestVaillantAdapterInfoStateUnsupportedTransportPublishesContract(t *testing.T) {
	t.Parallel()

	raw := stubRawTransport{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := protocol.NewBus(raw, protocol.DefaultBusConfig(), 0)
	bus.Run(ctx)

	provider := graphql.NewLiveSemanticProvider()
	state := newVaillantAdapterInfoState(bus, raw, provider)
	if state == nil {
		t.Fatal("newVaillantAdapterInfoState() returned nil")
	}

	info := provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() after constructor = nil; want unsupported contract")
	}
	if info.InfoSupported {
		t.Fatal("InfoSupported = true; want false for unsupported transport")
	}
	if got := info.VersionResponseLen; got != 0 {
		t.Fatalf("VersionResponseLen = %d; want 0 for unsupported transport", got)
	}
	if portalInfo := mapPortalAdapterInfo(info); portalInfo == nil {
		t.Fatal("mapPortalAdapterInfo() = nil; want unsupported contract")
	} else if portalInfo.InfoSupported {
		t.Fatal("portal adapter info InfoSupported = true; want false")
	}
	if mcpInfo := (mcpSemanticProviderAdapter{provider: provider}).AdapterHardwareInfo(); mcpInfo == nil {
		t.Fatal("mcp AdapterHardwareInfo() = nil; want unsupported contract")
	} else if mcpInfo.InfoSupported {
		t.Fatal("mcp adapter info InfoSupported = true; want false")
	}

	state.refreshCycle(ctx)

	info = provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() after refreshCycle = nil; want unsupported contract")
	}
	if info.InfoSupported {
		t.Fatal("InfoSupported after refreshCycle = true; want false for unsupported transport")
	}
}

func TestVaillantAdapterInfoStateUnsupportedTransitionClearsTelemetry(t *testing.T) {
	t.Parallel()

	raw := &stubAdapterInfoTransport{
		responses: map[transport.AdapterInfoID][]adapterInfoResponse{
			transport.AdapterInfoVersion: {
				{data: []byte{0x31, 0x01, 0x12, 0x34, 0x18, 0x10, 0xAB, 0xCD}},
				{data: []byte{0x32, 0x00}},
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
			transport.AdapterInfoResetInfo: {
				{data: []byte{0x04, 0x07}},
			},
			transport.AdapterInfoWiFiRSSI: {
				{data: []byte{0xA6}},
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

	info := provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() after supported refresh = nil")
	}
	if !info.InfoSupported {
		t.Fatal("InfoSupported after supported refresh = false; want true")
	}
	if got := info.HardwareID; got != "dead" {
		t.Fatalf("HardwareID after supported refresh = %q; want dead", got)
	}
	if got := info.HardwareConfig; got != "beef" {
		t.Fatalf("HardwareConfig after supported refresh = %q; want beef", got)
	}
	if info.TemperatureC == nil || *info.TemperatureC != 25 {
		t.Fatalf("TemperatureC after supported refresh = %v; want 25", info.TemperatureC)
	}
	if info.SupplyVoltageMV == nil || *info.SupplyVoltageMV != 2500 {
		t.Fatalf("SupplyVoltageMV after supported refresh = %v; want 2500", info.SupplyVoltageMV)
	}
	if info.BusVoltageMaxDV == nil || *info.BusVoltageMaxDV != 150 {
		t.Fatalf("BusVoltageMaxDV after supported refresh = %v; want 150", info.BusVoltageMaxDV)
	}
	if info.BusVoltageMinDV == nil || *info.BusVoltageMinDV != 130 {
		t.Fatalf("BusVoltageMinDV after supported refresh = %v; want 130", info.BusVoltageMinDV)
	}
	if info.ResetCause == nil || *info.ResetCause != "clear" {
		t.Fatalf("ResetCause after supported refresh = %v; want clear", info.ResetCause)
	}
	if info.ResetCauseCode == nil || *info.ResetCauseCode != 0x04 {
		t.Fatalf("ResetCauseCode after supported refresh = %v; want 0x04", info.ResetCauseCode)
	}
	if info.RestartCount == nil || *info.RestartCount != 0x07 {
		t.Fatalf("RestartCount after supported refresh = %v; want 0x07", info.RestartCount)
	}
	if info.WiFiRSSIDBm == nil || *info.WiFiRSSIDBm != -90 {
		t.Fatalf("WiFiRSSIDBm after supported refresh = %v; want -90", info.WiFiRSSIDBm)
	}
	if info.LastTelemetryQuery == nil {
		t.Fatal("LastTelemetryQuery after supported refresh = nil; want timestamp")
	}

	state.refreshIdentity(ctx)
	state.publish()

	info = provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() after unsupported refresh = nil")
	}
	if info.InfoSupported {
		t.Fatal("InfoSupported after unsupported refresh = true; want false")
	}
	if got := info.FirmwareVersion; got != "0x32" {
		t.Fatalf("FirmwareVersion after unsupported refresh = %q; want 0x32", got)
	}
	if got := info.HardwareID; got != "" {
		t.Fatalf("HardwareID after unsupported refresh = %q; want empty", got)
	}
	if got := info.HardwareConfig; got != "" {
		t.Fatalf("HardwareConfig after unsupported refresh = %q; want empty", got)
	}
	if info.TemperatureC != nil {
		t.Fatalf("TemperatureC after unsupported refresh = %v; want nil", info.TemperatureC)
	}
	if info.SupplyVoltageMV != nil {
		t.Fatalf("SupplyVoltageMV after unsupported refresh = %v; want nil", info.SupplyVoltageMV)
	}
	if info.BusVoltageMaxDV != nil {
		t.Fatalf("BusVoltageMaxDV after unsupported refresh = %v; want nil", info.BusVoltageMaxDV)
	}
	if info.BusVoltageMinDV != nil {
		t.Fatalf("BusVoltageMinDV after unsupported refresh = %v; want nil", info.BusVoltageMinDV)
	}
	if info.ResetCause != nil {
		t.Fatalf("ResetCause after unsupported refresh = %v; want nil", info.ResetCause)
	}
	if info.ResetCauseCode != nil {
		t.Fatalf("ResetCauseCode after unsupported refresh = %v; want nil", info.ResetCauseCode)
	}
	if info.RestartCount != nil {
		t.Fatalf("RestartCount after unsupported refresh = %v; want nil", info.RestartCount)
	}
	if info.WiFiRSSIDBm != nil {
		t.Fatalf("WiFiRSSIDBm after unsupported refresh = %v; want nil", info.WiFiRSSIDBm)
	}
	if info.LastTelemetryQuery != nil {
		t.Fatalf("LastTelemetryQuery after unsupported refresh = %v; want nil", info.LastTelemetryQuery)
	}
	if info.LastIdentityQuery == nil {
		t.Fatal("LastIdentityQuery after unsupported refresh = nil; want timestamp")
	}
	if got := raw.callCount(transport.AdapterInfoVersion); got != 2 {
		t.Fatalf("AdapterInfoVersion call count = %d; want 2", got)
	}
	if got := raw.callCount(transport.AdapterInfoHardwareID); got != 1 {
		t.Fatalf("AdapterInfoHardwareID call count = %d; want 1", got)
	}
	if got := raw.callCount(transport.AdapterInfoHardwareConf); got != 1 {
		t.Fatalf("AdapterInfoHardwareConf call count = %d; want 1", got)
	}
	if got := raw.callCount(transport.AdapterInfoTemperature); got != 1 {
		t.Fatalf("AdapterInfoTemperature call count = %d; want 1", got)
	}
	if got := raw.callCount(transport.AdapterInfoSupplyVolt); got != 1 {
		t.Fatalf("AdapterInfoSupplyVolt call count = %d; want 1", got)
	}
	if got := raw.callCount(transport.AdapterInfoBusVoltage); got != 1 {
		t.Fatalf("AdapterInfoBusVoltage call count = %d; want 1", got)
	}
	if got := raw.callCount(transport.AdapterInfoResetInfo); got != 1 {
		t.Fatalf("AdapterInfoResetInfo call count = %d; want 1", got)
	}
	if got := raw.callCount(transport.AdapterInfoWiFiRSSI); got != 1 {
		t.Fatalf("AdapterInfoWiFiRSSI call count = %d; want 1", got)
	}
}
