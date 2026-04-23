package main

import (
	"context"
	"errors"
	"expvar"
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

func TestVaillantAdapterInfoStateSupportedTransportSeedsEmptyContractBeforeRefresh(t *testing.T) {
	raw := &stubAdapterInfoTransport{
		responses: map[transport.AdapterInfoID][]adapterInfoResponse{},
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

	info := provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() after constructor = nil; want seeded empty contract")
	}
	if info.FirmwareVersion != "" {
		t.Fatalf("FirmwareVersion after constructor = %q; want empty", info.FirmwareVersion)
	}
	if info.InfoSupported {
		t.Fatal("InfoSupported after constructor = true; want false until identity refresh succeeds")
	}
}

func TestVaillantAdapterInfoStateUsesBusRawTransportRequesterWhenConstructorTransportCannotProbeInfo(t *testing.T) {
	raw := &stubAdapterInfoTransport{
		responses: map[transport.AdapterInfoID][]adapterInfoResponse{
			transport.AdapterInfoVersion: {
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
	state := newVaillantAdapterInfoState(bus, stubRawTransport{}, provider)
	if state == nil {
		t.Fatal("newVaillantAdapterInfoState() returned nil")
	}

	state.refreshCycle(ctx)

	info := provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() after refreshCycle = nil")
	}
	if got := info.FirmwareVersion; got != "0x31" {
		t.Fatalf("FirmwareVersion after refreshCycle = %q; want 0x31", got)
	}
	if !info.InfoSupported {
		t.Fatal("InfoSupported after refreshCycle = false; want true")
	}
	if got := info.HardwareID; got != "dead" {
		t.Fatalf("HardwareID after refreshCycle = %q; want dead", got)
	}
	if info.LastIdentityQuery == nil {
		t.Fatal("LastIdentityQuery after refreshCycle = nil; want timestamp")
	}
	if got := raw.callCount(transport.AdapterInfoVersion); got != 1 {
		t.Fatalf("AdapterInfoVersion call count = %d; want 1", got)
	}
}

func TestVaillantAdapterInfoStateUnsupportedTransitionClearsTelemetry(t *testing.T) {
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
	if info.WiFiRSSIDBm != nil {
		t.Fatalf("WiFiRSSIDBm after supported refresh = %v; want nil", info.WiFiRSSIDBm)
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
	if got := raw.callCount(transport.AdapterInfoWiFiRSSI); got != 0 {
		t.Fatalf("AdapterInfoWiFiRSSI call count = %d; want 0", got)
	}
}

func TestVaillantAdapterInfoStateResetInfoFailureClearsPreviousState(t *testing.T) {
	raw := &stubAdapterInfoTransport{
		responses: map[transport.AdapterInfoID][]adapterInfoResponse{
			transport.AdapterInfoVersion: {
				{data: []byte{0x31, 0x01, 0x12, 0x34, 0x18, 0x10, 0xAB, 0xCD}},
				{data: []byte{0x31, 0x01, 0x12, 0x34, 0x18, 0x10, 0xAB, 0xCD}},
			},
			transport.AdapterInfoHardwareID: {
				{data: []byte{0xDE, 0xAD}},
				{data: []byte{0xDE, 0xAD}},
			},
			transport.AdapterInfoHardwareConf: {
				{data: []byte{0xBE, 0xEF}},
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
				{err: errors.New("transient reset info failure")},
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
	if info.ResetCause == nil || *info.ResetCause != "clear" {
		t.Fatalf("ResetCause after supported refresh = %v; want clear", info.ResetCause)
	}
	if info.RestartCount == nil || *info.RestartCount != 0x07 {
		t.Fatalf("RestartCount after supported refresh = %v; want 0x07", info.RestartCount)
	}
	if got := expvarFloatValue(t, "ebus_adapter_restart_count"); got != 7 {
		t.Fatalf("restart count expvar after supported refresh = %v; want 7", got)
	}

	state.refreshIdentity(ctx)
	state.publish()

	info = provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() after reset_info failure = nil")
	}
	if info.ResetCause != nil {
		t.Fatalf("ResetCause after reset_info failure = %v; want nil", info.ResetCause)
	}
	if info.ResetCauseCode != nil {
		t.Fatalf("ResetCauseCode after reset_info failure = %v; want nil", info.ResetCauseCode)
	}
	if info.RestartCount != nil {
		t.Fatalf("RestartCount after reset_info failure = %v; want nil", info.RestartCount)
	}
	if got := expvarFloatValue(t, "ebus_adapter_restart_count"); got != 0 {
		t.Fatalf("restart count expvar after reset_info failure = %v; want 0", got)
	}
}

func TestVaillantAdapterInfoStateRetriesResetInfoAfterTransientFailure(t *testing.T) {
	raw := &stubAdapterInfoTransport{
		responses: map[transport.AdapterInfoID][]adapterInfoResponse{
			transport.AdapterInfoVersion: {
				{data: []byte{0x31, 0x01, 0x12, 0x34, 0x18, 0x10, 0xAB, 0xCD}},
				{data: []byte{0x31, 0x01, 0x12, 0x34, 0x18, 0x10, 0xAB, 0xCD}},
			},
			transport.AdapterInfoHardwareID: {
				{data: []byte{0xDE, 0xAD}},
				{data: []byte{0xDE, 0xAD}},
			},
			transport.AdapterInfoHardwareConf: {
				{data: []byte{0xBE, 0xEF}},
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
			transport.AdapterInfoResetInfo: {
				{err: errors.New("transient reset info failure")},
				{data: []byte{0x04, 0x08}},
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
		t.Fatal("AdapterHardwareInfo() after reset_info failure = nil")
	}
	if info.ResetCause != nil {
		t.Fatalf("ResetCause after reset_info failure = %v; want nil", info.ResetCause)
	}
	if got := raw.callCount(transport.AdapterInfoResetInfo); got != 1 {
		t.Fatalf("AdapterInfoResetInfo call count after failure = %d; want 1", got)
	}

	state.refreshCycle(ctx)
	info = provider.AdapterHardwareInfo()
	if info == nil {
		t.Fatal("AdapterHardwareInfo() after reset_info retry = nil")
	}
	if info.ResetCause == nil || *info.ResetCause != "clear" {
		t.Fatalf("ResetCause after reset_info retry = %v; want clear", info.ResetCause)
	}
	if info.RestartCount == nil || *info.RestartCount != 0x08 {
		t.Fatalf("RestartCount after reset_info retry = %v; want 0x08", info.RestartCount)
	}
	if got := raw.callCount(transport.AdapterInfoResetInfo); got != 2 {
		t.Fatalf("AdapterInfoResetInfo call count after retry = %d; want 2", got)
	}
}

func TestVaillantAdapterInfoStateExpvarTelemetryResetsOnUnsupportedAndInvalidation(t *testing.T) {
	rawUnsupported := &stubAdapterInfoTransport{
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
	ctxUnsupported, cancelUnsupported := context.WithCancel(context.Background())
	defer cancelUnsupported()

	busUnsupported := protocol.NewBus(rawUnsupported, protocol.DefaultBusConfig(), 0)
	busUnsupported.Run(ctxUnsupported)

	providerUnsupported := graphql.NewLiveSemanticProvider()
	stateUnsupported := newVaillantAdapterInfoState(busUnsupported, rawUnsupported, providerUnsupported)
	if stateUnsupported == nil {
		t.Fatal("newVaillantAdapterInfoState() returned nil")
	}

	stateUnsupported.refreshCycle(ctxUnsupported)
	if got := expvarFloatValue(t, "ebus_adapter_temperature_celsius"); got != 25 {
		t.Fatalf("temperature expvar before unsupported transition = %v; want 25", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_supply_voltage_millivolts"); got != 2500 {
		t.Fatalf("supply voltage expvar before unsupported transition = %v; want 2500", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_bus_voltage_max_decivolts"); got != 150 {
		t.Fatalf("bus voltage max expvar before unsupported transition = %v; want 150", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_restart_count"); got != 7 {
		t.Fatalf("restart count expvar before unsupported transition = %v; want 7", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_wifi_rssi_dbm"); got != 0 {
		t.Fatalf("wifi rssi expvar before unsupported transition = %v; want 0", got)
	}

	stateUnsupported.refreshIdentity(ctxUnsupported)
	stateUnsupported.publish()

	if got := expvarIntValue(t, "ebus_adapter_info_supported"); got != 0 {
		t.Fatalf("supported expvar after unsupported transition = %d; want 0", got)
	}
	if got := expvarIntValue(t, "ebus_adapter_info_health"); got != 1 {
		t.Fatalf("health expvar after unsupported transition = %d; want 1", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_temperature_celsius"); got != 0 {
		t.Fatalf("temperature expvar after unsupported transition = %v; want 0", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_supply_voltage_millivolts"); got != 0 {
		t.Fatalf("supply voltage expvar after unsupported transition = %v; want 0", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_bus_voltage_max_decivolts"); got != 0 {
		t.Fatalf("bus voltage max expvar after unsupported transition = %v; want 0", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_bus_voltage_min_decivolts"); got != 0 {
		t.Fatalf("bus voltage min expvar after unsupported transition = %v; want 0", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_restart_count"); got != 0 {
		t.Fatalf("restart count expvar after unsupported transition = %v; want 0", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_wifi_rssi_dbm"); got != 0 {
		t.Fatalf("wifi rssi expvar after unsupported transition = %v; want 0", got)
	}

	rawInvalidated := &stubAdapterInfoTransport{
		responses: map[transport.AdapterInfoID][]adapterInfoResponse{
			transport.AdapterInfoVersion: {
				{data: []byte{0x31, 0x01, 0x12, 0x34, 0x18, 0x10, 0xAB, 0xCD}},
				{err: ebuserrors.ErrTransportClosed},
			},
			transport.AdapterInfoHardwareID: {
				{data: []byte{0xAA, 0xAA}},
			},
			transport.AdapterInfoHardwareConf: {
				{data: []byte{0xCC, 0xCC}},
			},
			transport.AdapterInfoTemperature: {
				{data: []byte{0x00, 0x19}},
				{err: ebuserrors.ErrTransportClosed},
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
	ctxInvalidated, cancelInvalidated := context.WithCancel(context.Background())
	defer cancelInvalidated()

	busInvalidated := protocol.NewBus(rawInvalidated, protocol.DefaultBusConfig(), 0)
	busInvalidated.Run(ctxInvalidated)

	providerInvalidated := graphql.NewLiveSemanticProvider()
	stateInvalidated := newVaillantAdapterInfoState(busInvalidated, rawInvalidated, providerInvalidated)
	if stateInvalidated == nil {
		t.Fatal("newVaillantAdapterInfoState() returned nil")
	}

	stateInvalidated.refreshCycle(ctxInvalidated)
	if got := expvarFloatValue(t, "ebus_adapter_temperature_celsius"); got != 25 {
		t.Fatalf("temperature expvar before invalidation = %v; want 25", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_restart_count"); got != 7 {
		t.Fatalf("restart count expvar before invalidation = %v; want 7", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_wifi_rssi_dbm"); got != 0 {
		t.Fatalf("wifi rssi expvar before invalidation = %v; want 0", got)
	}

	stateInvalidated.refreshCycle(ctxInvalidated)

	if got := expvarIntValue(t, "ebus_adapter_info_supported"); got != 0 {
		t.Fatalf("supported expvar after invalidation = %d; want 0", got)
	}
	if got := expvarIntValue(t, "ebus_adapter_info_health"); got != 0 {
		t.Fatalf("health expvar after invalidation = %d; want 0", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_temperature_celsius"); got != 0 {
		t.Fatalf("temperature expvar after invalidation = %v; want 0", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_supply_voltage_millivolts"); got != 0 {
		t.Fatalf("supply voltage expvar after invalidation = %v; want 0", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_bus_voltage_max_decivolts"); got != 0 {
		t.Fatalf("bus voltage max expvar after invalidation = %v; want 0", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_bus_voltage_min_decivolts"); got != 0 {
		t.Fatalf("bus voltage min expvar after invalidation = %v; want 0", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_restart_count"); got != 0 {
		t.Fatalf("restart count expvar after invalidation = %v; want 0", got)
	}
	if got := expvarFloatValue(t, "ebus_adapter_wifi_rssi_dbm"); got != 0 {
		t.Fatalf("wifi rssi expvar after invalidation = %v; want 0", got)
	}
}

func expvarIntValue(t *testing.T, name string) int64 {
	t.Helper()

	variable := expvar.Get(name)
	if variable == nil {
		t.Fatalf("expvar %q = nil", name)
	}
	counter, ok := variable.(*expvar.Int)
	if !ok {
		t.Fatalf("expvar %q type = %T; want *expvar.Int", name, variable)
	}
	return counter.Value()
}

func expvarFloatValue(t *testing.T, name string) float64 {
	t.Helper()

	variable := expvar.Get(name)
	if variable == nil {
		t.Fatalf("expvar %q = nil", name)
	}
	counter, ok := variable.(*expvar.Float)
	if !ok {
		t.Fatalf("expvar %q type = %T; want *expvar.Float", name, variable)
	}
	return counter.Value()
}
