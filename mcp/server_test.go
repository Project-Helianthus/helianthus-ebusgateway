package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	"github.com/Project-Helianthus/helianthus-ebusreg/schema"
)

type testRegistry struct {
	entries map[byte]registry.DeviceEntry
	order   []byte
}

func (r *testRegistry) Iterate(fn func(registry.DeviceEntry) bool) {
	for _, addr := range r.order {
		entry := r.entries[addr]
		if entry == nil {
			continue
		}
		if !fn(entry) {
			return
		}
	}
}

func (r *testRegistry) Lookup(address byte) (registry.DeviceEntry, bool) {
	entry, ok := r.entries[address]
	return entry, ok
}

type testEntry struct {
	info   registry.DeviceInfo
	planes []registry.Plane
}

func (e testEntry) Address() byte            { return e.info.Address }
func (e testEntry) Addresses() []byte        { return []byte{e.info.Address} }
func (e testEntry) Manufacturer() string     { return e.info.Manufacturer }
func (e testEntry) DeviceID() string         { return e.info.DeviceID }
func (e testEntry) SerialNumber() string     { return e.info.SerialNumber }
func (e testEntry) MacAddress() string       { return e.info.MacAddress }
func (e testEntry) SoftwareVersion() string  { return e.info.SoftwareVersion }
func (e testEntry) HardwareVersion() string  { return e.info.HardwareVersion }
func (e testEntry) Planes() []registry.Plane { return e.planes }
func (e testEntry) Projections() []registry.Projection {
	return nil
}

type testTemplate struct {
	primary   byte
	secondary byte
}

func (t testTemplate) Primary() byte   { return t.primary }
func (t testTemplate) Secondary() byte { return t.secondary }

type testMethod struct {
	name     string
	readOnly bool
	template registry.FrameTemplate
}

func (m testMethod) Name() string                     { return m.name }
func (m testMethod) ReadOnly() bool                   { return m.readOnly }
func (m testMethod) Template() registry.FrameTemplate { return m.template }
func (m testMethod) ResponseSchema() schema.SchemaSelector {
	return schema.SchemaSelector{}
}

type testMetadataMethod struct {
	testMethod
	mutability string
	danger     string
	routable   bool
}

func (m testMetadataMethod) Mutability() string { return m.mutability }
func (m testMetadataMethod) Danger() string     { return m.danger }
func (m testMetadataMethod) Routable() bool     { return m.routable }

type testPlane struct {
	name      string
	methods   []registry.Method
	lastBuilt string
}

func (p *testPlane) Name() string               { return p.name }
func (p *testPlane) Methods() []registry.Method { return p.methods }
func (p *testPlane) Subscriptions() []router.Subscription {
	return nil
}
func (p *testPlane) OnBroadcast(protocol.Frame) error { return nil }
func (p *testPlane) BuildRequest(registry.Method, map[string]any) (protocol.Frame, error) {
	p.lastBuilt = "ok"
	return protocol.Frame{}, nil
}
func (p *testPlane) DecodeResponse(registry.Method, protocol.Frame, map[string]any) (any, error) {
	return map[string]any{"ok": true}, nil
}

type testPlainPlane struct {
	name    string
	methods []registry.Method
}

func (p testPlainPlane) Name() string               { return p.name }
func (p testPlainPlane) Methods() []registry.Method { return p.methods }

type testInvoker struct {
	calls []invokeCall
}

type invokeCall struct {
	plane  string
	method string
}

func (i *testInvoker) Invoke(ctx context.Context, plane router.Plane, methodName string, params map[string]any) (any, error) {
	i.calls = append(i.calls, invokeCall{plane: plane.Name(), method: methodName})
	return map[string]any{"ok": true}, nil
}

type slowInvoker struct {
	delay time.Duration
	calls int
}

func (i *slowInvoker) Invoke(ctx context.Context, plane router.Plane, methodName string, params map[string]any) (any, error) {
	i.calls++
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(i.delay):
		return map[string]any{"ok": true}, nil
	}
}

type testStatusProvider struct {
	daemon  ServiceStatus
	adapter ServiceStatus
}

func (p testStatusProvider) DaemonStatus() ServiceStatus {
	return p.daemon
}

func (p testStatusProvider) AdapterStatus() ServiceStatus {
	return p.adapter
}

type testBusObservabilityProvider struct {
	snapshot BusObservabilitySnapshot
}

func (p *testBusObservabilityProvider) Snapshot() BusObservabilitySnapshot {
	if p == nil {
		return BusObservabilitySnapshot{}
	}
	return cloneBusObservabilitySnapshot(p.snapshot)
}

func (p *testBusObservabilityProvider) ProtocolSpecimens(family string) []BusProtocolSpecimen {
	return nil
}

type testWatchSummaryProvider struct {
	summary WatchSummary
}

func (p *testWatchSummaryProvider) Snapshot() WatchSummary {
	if p == nil {
		return WatchSummary{}
	}
	copy := cloneWatchSummary(&p.summary)
	if copy == nil {
		return WatchSummary{}
	}
	return *copy
}

type testSemanticProvider struct {
	zones         []Zone
	circuits      []CircuitStatus
	radio         []RadioDevice
	fm5Mode       Fm5SemanticMode
	solar         *SolarStatus
	cylinders     []CylinderStatus
	dhw           *DhwStatus
	energy        *EnergyTotals
	boiler        *BoilerStatus
	system        *SystemStatus
	schedules     *ScheduleStatus
	adapterInfo   *AdapterHardwareInfo
	zonesDelay    time.Duration
	circuitsDelay time.Duration
	radioDelay    time.Duration
	dhwDelay      time.Duration
	energyDelay   time.Duration
}

func (p testSemanticProvider) Zones() []Zone {
	if p.zonesDelay > 0 {
		time.Sleep(p.zonesDelay)
	}
	if len(p.zones) == 0 {
		return nil
	}
	out := make([]Zone, len(p.zones))
	copy(out, p.zones)
	return out
}

func (p testSemanticProvider) DHW() *DhwStatus {
	if p.dhwDelay > 0 {
		time.Sleep(p.dhwDelay)
	}
	if p.dhw == nil {
		return nil
	}
	copy := *p.dhw
	return &copy
}

func (p testSemanticProvider) Circuits() []CircuitStatus {
	if p.circuitsDelay > 0 {
		time.Sleep(p.circuitsDelay)
	}
	if len(p.circuits) == 0 {
		return nil
	}
	return cloneCircuits(p.circuits)
}

func (p testSemanticProvider) RadioDevices() []RadioDevice {
	if p.radioDelay > 0 {
		time.Sleep(p.radioDelay)
	}
	if len(p.radio) == 0 {
		return nil
	}
	return cloneRadioDevices(p.radio)
}

func (p testSemanticProvider) FM5SemanticMode() Fm5SemanticMode {
	if p.fm5Mode == "" {
		return Fm5SemanticModeAbsent
	}
	return p.fm5Mode
}

func (p testSemanticProvider) Solar() *SolarStatus {
	return cloneMCPSolarStatus(p.solar)
}

func (p testSemanticProvider) Cylinders() []CylinderStatus {
	return cloneCylinders(p.cylinders)
}

func (p testSemanticProvider) BoilerStatus() *BoilerStatus {
	if p.boiler == nil {
		return nil
	}
	copy := *p.boiler
	return &copy
}

func (p testSemanticProvider) System() *SystemStatus {
	if p.system == nil {
		return nil
	}
	return cloneMCPSystemStatus(p.system)
}

func (p testSemanticProvider) Schedules() *ScheduleStatus {
	if p.schedules == nil {
		return nil
	}
	return cloneMCPScheduleStatus(p.schedules)
}

func (p testSemanticProvider) AdapterHardwareInfo() *AdapterHardwareInfo {
	return cloneMCPAdapterHardwareInfo(p.adapterInfo)
}

func (p testSemanticProvider) EnergyTotals() *EnergyTotals {
	if p.energyDelay > 0 {
		time.Sleep(p.energyDelay)
	}
	if p.energy == nil {
		return nil
	}
	copy := *p.energy
	copy.Gas = cloneEnergyChannel(copy.Gas)
	copy.Electric = cloneEnergyChannel(copy.Electric)
	copy.Solar = cloneEnergyChannel(copy.Solar)
	return &copy
}

func TestServer_InitializeAndTools(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	res := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "initialize", Params: json.RawMessage(`{}`)})
	if res.Error != nil {
		t.Fatalf("initialize error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("initialize result type = %T; want map", res.Result)
	}
	if resultMap["protocolVersion"] == "" {
		t.Fatalf("missing protocolVersion in initialize result")
	}
	if resultMap["sessionId"] == "" {
		t.Fatalf("missing sessionId in initialize result")
	}

	res = doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 2, Method: "tools/list", Params: nil})
	if res.Error != nil {
		t.Fatalf("tools/list error = %+v", res.Error)
	}
	resultMap, ok = res.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list result type = %T; want map", res.Result)
	}
	tools, ok := resultMap["tools"].([]any)
	if !ok || len(tools) < 24 {
		t.Fatalf("tools = %#v; want at least 24 tools", resultMap["tools"])
	}
	for _, name := range []string{
		toolRuntimeStatusGetName,
		toolSemanticZonesGetName,
		toolSemanticCircuitsGetName,
		toolSemanticRadioGetName,
		toolSemanticFM5ModeGetName,
		toolSemanticSolarGetName,
		toolSemanticCylindersGetName,
		toolSemanticDHWGetName,
		toolSemanticEnergyGetName,
		toolSemanticBoilerGetName,
		toolSemanticSystemGetName,
		toolSemanticSnapshotName,
		toolSnapshotCaptureName,
		toolSnapshotDropName,
		toolDevicesV1Name,
		toolDeviceGetV1Name,
		toolPlanesListV1Name,
		toolMethodsListV1Name,
		toolInvokeV1Name,
		toolDevicesLegacyName,
		toolInvokeLegacyName,
	} {
		if !hasToolName(tools, name) {
			t.Fatalf("tools list missing %q", name)
		}
	}
	invokeV1 := findToolByName(tools, toolInvokeV1Name)
	if invokeV1 == nil {
		t.Fatalf("missing %q in tools list", toolInvokeV1Name)
	}
	inputSchema, ok := invokeV1["inputSchema"].(map[string]any)
	if !ok {
		t.Fatalf("invoke v1 inputSchema type = %T; want map", invokeV1["inputSchema"])
	}
	properties, ok := inputSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("invoke v1 properties type = %T; want map", inputSchema["properties"])
	}
	for _, key := range []string{"intent", "allow_dangerous", "idempotency_key", "timeout_ms"} {
		if _, ok := properties[key]; !ok {
			t.Fatalf("invoke v1 properties missing %q", key)
		}
	}
}

func TestServer_BusObservabilityToolsRequireConfiguredProvider(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	res := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list", Params: nil})
	if res.Error != nil {
		t.Fatalf("tools/list error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list result type = %T; want map", res.Result)
	}
	tools, ok := resultMap["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list tools type = %T; want []any", resultMap["tools"])
	}
	for _, name := range []string{
		toolBusSummaryGetName,
		toolBusMessagesListName,
		toolBusPeriodicityListName,
		toolBusProtocolSpecimensListName,
	} {
		if hasToolName(tools, name) {
			t.Fatalf("tools list unexpectedly included %q without bus provider", name)
		}
	}

	res = doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"` + toolBusSummaryGetName + `","arguments":{}}`),
	})
	if res.Error == nil {
		t.Fatalf("bus summary call error = nil; want unknown tool")
	}
	if want := `unknown tool "` + toolBusSummaryGetName + `"`; res.Error.Message != want {
		t.Fatalf("bus summary call error = %q; want %q", res.Error.Message, want)
	}

	server.SetBusObservabilityProvider(&testBusObservabilityProvider{
		snapshot: BusObservabilitySnapshot{
			Summary: &BusSummary{
				Status: &BusObservabilityStatus{
					Capability: BusObservabilityCapability{PassiveState: "unavailable"},
				},
			},
		},
	})

	res = doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 3, Method: "tools/list", Params: nil})
	if res.Error != nil {
		t.Fatalf("tools/list after bus provider error = %+v", res.Error)
	}
	resultMap, ok = res.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list after bus provider result type = %T; want map", res.Result)
	}
	tools, ok = resultMap["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list after bus provider tools type = %T; want []any", resultMap["tools"])
	}
	for _, name := range []string{
		toolBusSummaryGetName,
		toolBusMessagesListName,
		toolBusPeriodicityListName,
		toolBusProtocolSpecimensListName,
	} {
		if !hasToolName(tools, name) {
			t.Fatalf("tools list missing %q after bus provider configured", name)
		}
	}

	// Verify protocol_specimens.list returns valid output.
	res = doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      10,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"` + toolBusProtocolSpecimensListName + `","arguments":{}}`),
	})
	if res.Error != nil {
		t.Fatalf("protocol_specimens.list error = %+v", res.Error)
	}

	// Verify family filter does not error.
	res = doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      11,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"` + toolBusProtocolSpecimensListName + `","arguments":{"family":"B510","limit":5}}`),
	})
	if res.Error != nil {
		t.Fatalf("protocol_specimens.list with family filter error = %+v", res.Error)
	}

	// Verify invalid family type returns error.
	res = doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      12,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"` + toolBusProtocolSpecimensListName + `","arguments":{"family":123}}`),
	})
	if res.Error != nil {
		t.Fatalf("protocol_specimens.list should return tool result, not RPC error, for invalid family")
	}
}

func TestServer_WatchSummaryToolRequiresConfiguredProvider(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	res := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list", Params: nil})
	if res.Error != nil {
		t.Fatalf("tools/list error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list result type = %T; want map", res.Result)
	}
	tools, ok := resultMap["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list tools type = %T; want []any", resultMap["tools"])
	}
	if hasToolName(tools, toolWatchSummaryGetName) {
		t.Fatalf("tools list unexpectedly included %q without watch-summary provider", toolWatchSummaryGetName)
	}

	res = doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"` + toolWatchSummaryGetName + `","arguments":{}}`),
	})
	if res.Error == nil {
		t.Fatalf("watch summary call error = nil; want unknown tool")
	}
	if want := `unknown tool "` + toolWatchSummaryGetName + `"`; res.Error.Message != want {
		t.Fatalf("watch summary call error = %q; want %q", res.Error.Message, want)
	}

	server.SetWatchSummaryProvider(&testWatchSummaryProvider{
		summary: WatchSummary{
			ActivationCounts: WatchSummaryActivationCounts{
				ActiveKeys: 3,
			},
			Degraded: WatchSummaryDegraded{
				ShadowingEnabled: true,
			},
		},
	})

	res = doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 3, Method: "tools/list", Params: nil})
	if res.Error != nil {
		t.Fatalf("tools/list after watch provider error = %+v", res.Error)
	}
	resultMap, ok = res.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list after watch provider result type = %T; want map", res.Result)
	}
	tools, ok = resultMap["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list after watch provider tools type = %T; want []any", resultMap["tools"])
	}
	if !hasToolName(tools, toolWatchSummaryGetName) {
		t.Fatalf("tools list missing %q after watch provider configured", toolWatchSummaryGetName)
	}

	envelope := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      4,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"` + toolWatchSummaryGetName + `","arguments":{}}`),
	}))
	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("watch summary data type = %T; want map", envelope["data"])
	}
	activationCounts, ok := data["activation_counts"].(map[string]any)
	if !ok {
		t.Fatalf("watch summary activation_counts type = %T; want map", data["activation_counts"])
	}
	if got, _ := activationCounts["active_keys"].(float64); int(got) != 3 {
		t.Fatalf("watch summary activation_counts.active_keys = %v; want 3", activationCounts["active_keys"])
	}
}

func TestServer_ToolsCallDevicesAndInvoke(t *testing.T) {
	plane := &testPlane{
		name: "heating",
		methods: []registry.Method{
			testMethod{
				name:     "get_status",
				readOnly: true,
				template: testTemplate{primary: 0xB5, secondary: 0x04},
			},
		},
	}
	entry := testEntry{
		info: registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "vaillant",
			DeviceID:        "device-a",
			SoftwareVersion: "1.0",
			HardwareVersion: "7603",
		},
		planes: []registry.Plane{plane},
	}
	reg := &testRegistry{
		entries: map[byte]registry.DeviceEntry{0x08: entry},
		order:   []byte{0x08},
	}
	invoker := &testInvoker{}
	server, err := NewServer(reg, invoker)
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.registry.devices.list","arguments":{}}`),
	})
	if res.Error != nil {
		t.Fatalf("tools/call devices error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("devices call result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); isError {
		t.Fatalf("devices call isError=true; want false")
	}
	content, ok := resultMap["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("devices call content = %#v; want 1 item", resultMap["content"])
	}
	contentItem, ok := content[0].(map[string]any)
	if !ok {
		t.Fatalf("content item type = %T; want map", content[0])
	}
	text, _ := contentItem["text"].(string)
	if text == "" {
		t.Fatalf("content.text empty")
	}
	envelope := parseEnvelope(t, text)
	meta, ok := envelope["meta"].(map[string]any)
	if !ok {
		t.Fatalf("devices envelope meta type = %T; want map", envelope["meta"])
	}
	if hash, _ := meta["data_hash"].(string); hash == "" {
		t.Fatalf("devices envelope data_hash empty")
	}
	contract, ok := meta["contract"].(map[string]any)
	if !ok {
		t.Fatalf("devices envelope contract type = %T; want map", meta["contract"])
	}
	if major, _ := contract["major"].(float64); major != 1 {
		t.Fatalf("devices envelope contract.major = %v; want 1", contract["major"])
	}
	if envelope["error"] != nil {
		t.Fatalf("devices envelope error = %#v; want nil", envelope["error"])
	}
	if envelope["data"] == nil {
		t.Fatal("devices envelope data is nil")
	}

	res = doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.rpc.invoke","arguments":{"address":8,"plane":"heating","method":"get_status","params":{},"intent":"READ_ONLY","allow_dangerous":false}}`),
	})
	if res.Error != nil {
		t.Fatalf("tools/call invoke error = %+v", res.Error)
	}
	resultMap, ok = res.Result.(map[string]any)
	if !ok {
		t.Fatalf("invoke call result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); isError {
		t.Fatalf("invoke call isError=true; want false")
	}
	content, ok = resultMap["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("invoke call content = %#v; want 1 item", resultMap["content"])
	}
	contentItem, ok = content[0].(map[string]any)
	if !ok {
		t.Fatalf("invoke content item type = %T; want map", content[0])
	}
	text, _ = contentItem["text"].(string)
	if text == "" {
		t.Fatalf("invoke content.text empty")
	}
	envelope = parseEnvelope(t, text)
	if envelope["error"] != nil {
		t.Fatalf("invoke envelope error = %#v; want nil", envelope["error"])
	}
	if envelope["data"] == nil {
		t.Fatal("invoke envelope data is nil")
	}
	if len(invoker.calls) != 1 || invoker.calls[0].plane != "heating" || invoker.calls[0].method != "get_status" {
		t.Fatalf("invoker calls = %+v; want heating/get_status", invoker.calls)
	}
}

func TestServer_ToolsCallRuntimeStatus(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetStatusProvider(testStatusProvider{
		daemon: ServiceStatus{
			Status:           "running",
			FirmwareVersion:  "1.2.3",
			UpdatesAvailable: false,
			InitiatorAddress: "0x15",
		},
		adapter: ServiceStatus{
			Status:           "connected",
			FirmwareVersion:  "2.0.0",
			UpdatesAvailable: true,
		},
	})

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.runtime.status.get","arguments":{}}`),
	})
	if res.Error != nil {
		t.Fatalf("tools/call runtime status error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("runtime status result type = %T; want map", res.Result)
	}
	content, ok := resultMap["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("runtime status content = %#v; want 1 item", resultMap["content"])
	}
	contentItem, ok := content[0].(map[string]any)
	if !ok {
		t.Fatalf("runtime status content item type = %T; want map", content[0])
	}
	text, _ := contentItem["text"].(string)
	envelope := parseEnvelope(t, text)
	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("runtime status data type = %T; want map", envelope["data"])
	}
	daemon, ok := data["daemon_status"].(map[string]any)
	if !ok {
		t.Fatalf("daemon_status type = %T; want map", data["daemon_status"])
	}
	if got, _ := daemon["initiator_address"].(string); got != "0x15" {
		t.Fatalf("daemon initiator_address = %q; want 0x15", got)
	}
	adapter, ok := data["adapter_status"].(map[string]any)
	if !ok {
		t.Fatalf("adapter_status type = %T; want map", data["adapter_status"])
	}
	if got, _ := adapter["status"].(string); got != "connected" {
		t.Fatalf("adapter status = %q; want connected", got)
	}
}

func TestServer_ToolsCallBusObservability(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	base := time.Date(2026, time.March, 12, 12, 0, 0, 0, time.UTC)
	server.SetBusObservabilityProvider(&testBusObservabilityProvider{
		snapshot: BusObservabilitySnapshot{
			Summary: &BusSummary{
				LastUpdatedAt: &base,
				Status: &BusObservabilityStatus{
					LastUpdatedAt:  &base,
					TransportClass: "ens",
					Capability: BusObservabilityCapability{
						ActiveSupported:    true,
						PassiveSupported:   true,
						BroadcastSupported: true,
						PassiveAvailable:   false,
						PassiveState:       "warming_up",
						EndpointState:      "connected",
						TapConnected:       true,
					},
					Warmup: BusObservabilityWarmup{
						State:                 "warming_up",
						Blocker:               "completed_transactions",
						ElapsedSeconds:        12,
						CompletedTransactions: 3,
						RequiredTransactions:  20,
					},
					TimingQuality: BusObservabilityTimingQuality{
						Active:      "estimated",
						Passive:     "estimated",
						Busy:        "estimated",
						Periodicity: "estimated",
					},
					Degraded: BusObservabilityDegraded{
						Active:  true,
						Reasons: []string{"dedup_degraded"},
					},
					FeatureFlags: ObserveFirstFeatureFlagState{
						ObserveFirstEnabled:      true,
						PassiveStateDirectApply:  false,
						PassiveConfigDirectApply: false,
						ExternalWritePolicy:      "record_only",
						LastUpdatedAt:            &base,
						Normalizations: []string{
							"config_requires_state",
							"state_disabled_forces_record_only",
						},
					},
				},
				Messages:    BusBoundedListSummary{Count: 3, Capacity: 16},
				Periodicity: BusBoundedListSummary{Count: 2, Capacity: 8},
				Counters: BusObservabilityCounters{
					SeriesBudgetOverflowTotal:      1,
					PeriodicityBudgetOverflowTotal: 2,
				},
			},
			Messages: []BusMessage{
				{Scope: "active", Family: "B509", FrameType: "initiator_target", Outcome: "success", ObservedAt: base, SourceAddress: 0x08, TargetAddress: 0x15, RequestLen: 6, ResponseLen: 4},
				{Scope: "passive", Family: "B524", FrameType: "broadcast", Outcome: "success", ObservedAt: base.Add(2 * time.Second), SourceAddress: 0x15, TargetAddress: 0xfe, RequestLen: 8, ResponseLen: 6},
				{Scope: "active", Family: "other", FrameType: "abandoned_partial", Outcome: "timeout", ObservedAt: base.Add(4 * time.Second), SourceAddress: 0x26, TargetAddress: 0x08, RequestLen: 7, ResponseLen: 0},
			},
			Periodicity: []BusPeriodicityEntry{
				{SourceBucket: "0x08", TargetBucket: "0x15", Primary: 0xB5, Secondary: 0x09, Family: "B509", State: "warming_up", LastSeen: base.Add(10 * time.Second), SampleCount: 1, LastInterval: "15s", MeanInterval: "15s", MinInterval: "15s", MaxInterval: "15s"},
				{SourceBucket: "0x15", TargetBucket: "0xfe", Primary: 0xB5, Secondary: 0x24, Family: "B524", State: "available", LastSeen: base.Add(40 * time.Second), SampleCount: 4, LastInterval: "30s", MeanInterval: "29s", MinInterval: "28s", MaxInterval: "31s"},
			},
		},
	})

	summaryEnvelope := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.bus.summary.get","arguments":{}}`),
	}))
	summaryData, ok := summaryEnvelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("bus summary data type = %T; want map", summaryEnvelope["data"])
	}
	status, ok := summaryData["status"].(map[string]any)
	if !ok {
		t.Fatalf("bus summary status type = %T; want map", summaryData["status"])
	}
	capability, ok := status["capability"].(map[string]any)
	if !ok {
		t.Fatalf("bus summary capability type = %T; want map", status["capability"])
	}
	if got, _ := capability["passive_state"].(string); got != "warming_up" {
		t.Fatalf("bus summary passive_state = %q; want warming_up", got)
	}
	warmup, ok := status["warmup"].(map[string]any)
	if !ok {
		t.Fatalf("bus summary warmup type = %T; want map", status["warmup"])
	}
	if got, _ := warmup["blocker"].(string); got != "completed_transactions" {
		t.Fatalf("bus summary warmup.blocker = %q; want completed_transactions", got)
	}
	degraded, ok := status["degraded"].(map[string]any)
	if !ok {
		t.Fatalf("bus summary degraded type = %T; want map", status["degraded"])
	}
	if got, _ := degraded["active"].(bool); !got {
		t.Fatalf("bus summary degraded.active = %v; want true", degraded["active"])
	}
	featureFlags, ok := status["feature_flags"].(map[string]any)
	if !ok {
		t.Fatalf("bus summary feature_flags type = %T; want map", status["feature_flags"])
	}
	if got, _ := featureFlags["external_write_policy"].(string); got != "record_only" {
		t.Fatalf("bus summary feature_flags.external_write_policy = %q; want record_only", got)
	}
	if got, _ := featureFlags["passive_state_direct_apply"].(bool); got {
		t.Fatalf("bus summary feature_flags.passive_state_direct_apply = %v; want false", got)
	}
	if normalizations, _ := featureFlags["normalizations"].([]any); len(normalizations) != 2 {
		t.Fatalf("bus summary feature_flags.normalizations = %#v; want 2 entries", featureFlags["normalizations"])
	}
	if got, _ := summaryData["last_updated_at"].(string); got != base.Format(time.RFC3339Nano) {
		t.Fatalf("bus summary last_updated_at = %q; want %s", got, base.Format(time.RFC3339Nano))
	}
	if got, _ := status["last_updated_at"].(string); got != base.Format(time.RFC3339Nano) {
		t.Fatalf("bus summary status.last_updated_at = %q; want %s", got, base.Format(time.RFC3339Nano))
	}
	if got, _ := featureFlags["last_updated_at"].(string); got != base.Format(time.RFC3339Nano) {
		t.Fatalf("bus summary feature_flags.last_updated_at = %q; want %s", got, base.Format(time.RFC3339Nano))
	}

	messageEnvelope := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.bus.messages.list","arguments":{"limit":2}}`),
	}))
	messageData, ok := messageEnvelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("bus messages data type = %T; want map", messageEnvelope["data"])
	}
	if got, _ := messageData["count"].(float64); int(got) != 3 {
		t.Fatalf("bus messages count = %v; want 3", messageData["count"])
	}
	items, ok := messageData["items"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("bus messages items = %#v; want 2 items", messageData["items"])
	}
	firstItem, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("bus messages first item type = %T; want map", items[0])
	}
	if got, _ := firstItem["family"].(string); got != "B524" {
		t.Fatalf("bus messages first family = %q; want B524", got)
	}

	periodicityEnvelope := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.bus.periodicity.list","arguments":{"limit":1}}`),
	}))
	periodicityData, ok := periodicityEnvelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("bus periodicity data type = %T; want map", periodicityEnvelope["data"])
	}
	if got, _ := periodicityData["capacity"].(float64); int(got) != 8 {
		t.Fatalf("bus periodicity capacity = %v; want 8", periodicityData["capacity"])
	}
	periodicityItems, ok := periodicityData["items"].([]any)
	if !ok || len(periodicityItems) != 1 {
		t.Fatalf("bus periodicity items = %#v; want 1 item", periodicityData["items"])
	}
	periodicityItem, ok := periodicityItems[0].(map[string]any)
	if !ok {
		t.Fatalf("bus periodicity first item type = %T; want map", periodicityItems[0])
	}
	if got, _ := periodicityItem["state"].(string); got != "available" {
		t.Fatalf("bus periodicity state = %q; want available", got)
	}
	if got, _ := periodicityItem["last_interval"].(string); got != "30s" {
		t.Fatalf("bus periodicity last_interval = %q; want 30s", got)
	}
}

func TestServer_ToolsCallSemanticSnapshots(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetSemanticProvider(testSemanticProvider{
		zones: []Zone{
			{ID: "zone-b", Name: "Bedroom", Config: ZoneConfig{OperatingMode: "AUTO", Preset: "COMFORT"}},
			{ID: "zone-a", Name: "Living", Config: ZoneConfig{OperatingMode: "AUTO", Preset: "COMFORT"}},
		},
		circuits: []CircuitStatus{
			{Index: 1, CircuitType: "fixed_value", ManagingDevice: ManagingDevice{Role: "UNKNOWN"}},
			{Index: 0, CircuitType: "heating", ManagingDevice: ManagingDevice{Role: "FUNCTION_MODULE", DeviceID: stringPtr("VR_71"), Address: intPtr(0x26)}},
		},
		radio: []RadioDevice{
			{Group: 0x0A, Instance: 1, DeviceModel: "VR92"},
			{Group: 0x09, Instance: 1, DeviceModel: "VRC720"},
		},
		fm5Mode: Fm5SemanticModeInterpreted,
		solar: &SolarStatus{
			CollectorTemperatureC: floatPtr(72.5),
			PumpActive:            boolPtr(true),
		},
		cylinders: []CylinderStatus{
			{Index: 0, TemperatureC: floatPtr(48.0)},
			{Index: 1, TemperatureC: floatPtr(46.0)},
		},
		dhw: &DhwStatus{
			Config: DhwConfig{
				OperatingMode: "AUTO",
				Preset:        "ECO",
			},
		},
		energy: &EnergyTotals{
			Gas: EnergyChannel{
				DHW: EnergySeries{
					Today:     1.25,
					Yearly:    []float64{10, 20},
					TodayMeta: EnergyPointMeta{FreshnessState: "fresh", Provenance: "register", Stale: false},
				},
				Climate: EnergySeries{
					Today:     2.5,
					Yearly:    []float64{30, 40},
					TodayMeta: EnergyPointMeta{FreshnessState: "stale", Provenance: "broadcast", Stale: true},
				},
			},
		},
		system: &SystemStatus{
			State: &SystemState{
				SystemOff:             boolPtr(false),
				SystemWaterPressure:   floatPtr(1.5),
				OutdoorTemperature:    floatPtr(6.0),
				SystemFlowTemperature: floatPtr(32.0),
			},
			Config: &SystemConfig{
				MaxRoomHumidity: intPtr(70),
			},
			Properties: &SystemProperties{
				SystemScheme:            intPtr(8),
				ModuleConfigurationVR71: intPtr(2),
			},
		},
	})

	t.Run("zones sorted", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      1,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.zones.get","arguments":{}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].([]any)
		if !ok || len(data) != 2 {
			t.Fatalf("zones data = %#v; want 2 entries", envelope["data"])
		}
		first, _ := data[0].(map[string]any)
		second, _ := data[1].(map[string]any)
		if id, _ := first["id"].(string); id != "zone-a" {
			t.Fatalf("first zone id = %q; want zone-a", id)
		}
		if id, _ := second["id"].(string); id != "zone-b" {
			t.Fatalf("second zone id = %q; want zone-b", id)
		}
	})

	t.Run("circuits sorted", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      20,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.circuits.get","arguments":{}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].([]any)
		if !ok || len(data) != 2 {
			t.Fatalf("circuits data = %#v; want 2 entries", envelope["data"])
		}
		first, _ := data[0].(map[string]any)
		second, _ := data[1].(map[string]any)
		if index, _ := first["index"].(float64); int(index) != 0 {
			t.Fatalf("first circuit index = %v; want 0", first["index"])
		}
		if circuitType, _ := first["circuit_type"].(string); circuitType != "heating" {
			t.Fatalf("first circuit type = %q; want heating", circuitType)
		}
		managing, ok := first["managing_device"].(map[string]any)
		if !ok {
			t.Fatalf("first circuit managing_device type = %T; want map", first["managing_device"])
		}
		if role, _ := managing["role"].(string); role != "FUNCTION_MODULE" {
			t.Fatalf("first circuit managing_device.role = %q; want FUNCTION_MODULE", role)
		}
		if deviceID, _ := managing["device_id"].(string); deviceID != "VR_71" {
			t.Fatalf("first circuit managing_device.device_id = %q; want VR_71", deviceID)
		}
		if index, _ := second["index"].(float64); int(index) != 1 {
			t.Fatalf("second circuit index = %v; want 1", second["index"])
		}
		if circuitType, _ := second["circuit_type"].(string); circuitType != "fixed_value" {
			t.Fatalf("second circuit type = %q; want fixed_value", circuitType)
		}
	})

	t.Run("radio payload sorted", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      21,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.radio_devices.get","arguments":{}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].([]any)
		if !ok || len(data) != 2 {
			t.Fatalf("radio data = %#v; want 2 entries", envelope["data"])
		}
		first, _ := data[0].(map[string]any)
		second, _ := data[1].(map[string]any)
		if group, _ := first["group"].(float64); int(group) != 0x09 {
			t.Fatalf("first radio group = %v; want 0x09", first["group"])
		}
		if group, _ := second["group"].(float64); int(group) != 0x0A {
			t.Fatalf("second radio group = %v; want 0x0A", second["group"])
		}
	})

	t.Run("fm5 mode payload", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      22,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.fm5_mode.get","arguments":{}}`),
		})
		envelope := envelopeFromResult(t, res)
		mode, ok := envelope["data"].(string)
		if !ok {
			t.Fatalf("fm5 mode type = %T; want string", envelope["data"])
		}
		if mode != string(Fm5SemanticModeInterpreted) {
			t.Fatalf("fm5 mode = %q; want %q", mode, Fm5SemanticModeInterpreted)
		}
	})

	t.Run("solar payload", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      23,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.solar.get","arguments":{}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].(map[string]any)
		if !ok {
			t.Fatalf("solar data type = %T; want map", envelope["data"])
		}
		if got, _ := data["collector_temperature_c"].(float64); got != 72.5 {
			t.Fatalf("solar collector_temperature_c = %v; want 72.5", data["collector_temperature_c"])
		}
	})

	t.Run("cylinders payload", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      24,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.cylinders.get","arguments":{}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].([]any)
		if !ok || len(data) != 2 {
			t.Fatalf("cylinders data = %#v; want 2 entries", envelope["data"])
		}
		first, _ := data[0].(map[string]any)
		if idx, _ := first["index"].(float64); int(idx) != 0 {
			t.Fatalf("first cylinder index = %v; want 0", first["index"])
		}
	})

	t.Run("dhw payload", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      2,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.dhw.get","arguments":{}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].(map[string]any)
		if !ok {
			t.Fatalf("dhw data type = %T; want map", envelope["data"])
		}
		config, _ := data["config"].(map[string]any)
		if preset, _ := config["preset"].(string); preset != "ECO" {
			t.Fatalf("dhw config.preset = %q; want ECO", preset)
		}
	})

	t.Run("energy totals payload", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      3,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.energy_totals.get","arguments":{}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].(map[string]any)
		if !ok {
			t.Fatalf("energy data type = %T; want map", envelope["data"])
		}
		gas, ok := data["gas"].(map[string]any)
		if !ok {
			t.Fatalf("energy gas type = %T; want map", data["gas"])
		}
		dhw, ok := gas["dhw"].(map[string]any)
		if !ok {
			t.Fatalf("energy gas.dhw type = %T; want map", gas["dhw"])
		}
		if today, _ := dhw["today"].(float64); today != 1.25 {
			t.Fatalf("energy gas.dhw.today = %v; want 1.25", dhw["today"])
		}
		todayMeta, ok := dhw["today_meta"].(map[string]any)
		if !ok {
			t.Fatalf("energy gas.dhw.today_meta type = %T; want map", dhw["today_meta"])
		}
		if got, _ := todayMeta["freshness_state"].(string); got != "fresh" {
			t.Fatalf("energy gas.dhw.today_meta.freshness_state = %q; want fresh", got)
		}
		if got, _ := todayMeta["provenance"].(string); got != "register" {
			t.Fatalf("energy gas.dhw.today_meta.provenance = %q; want register", got)
		}
	})

	t.Run("system payload", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      30,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.system.get","arguments":{}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].(map[string]any)
		if !ok {
			t.Fatalf("system data type = %T; want map", envelope["data"])
		}
		props, ok := data["properties"].(map[string]any)
		if !ok {
			t.Fatalf("system properties type = %T; want map", data["properties"])
		}
		if _, exists := props["vr71_circuit_start_index"]; exists {
			t.Fatalf("system properties unexpectedly contain vr71_circuit_start_index: %#v", props["vr71_circuit_start_index"])
		}
	})

	t.Run("semantic snapshot default planes", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      4,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.snapshot.get","arguments":{}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].(map[string]any)
		if !ok {
			t.Fatalf("snapshot data type = %T; want map", envelope["data"])
		}
		completed, ok := data["completed_planes"].([]any)
		if !ok || len(completed) != 13 {
			t.Fatalf("snapshot completed_planes = %#v; want 13 entries", data["completed_planes"])
		}
		planes, ok := data["planes"].(map[string]any)
		if !ok {
			t.Fatalf("snapshot planes type = %T; want map", data["planes"])
		}
		for _, key := range []string{"runtime_status", "zones", "dhw", "energy_totals", "boiler_status", "system", "circuits", "radio_devices", "fm5_mode", "solar", "cylinders"} {
			if _, ok := planes[key]; !ok {
				t.Fatalf("snapshot planes missing %q", key)
			}
		}
	})

	t.Run("semantic snapshot supports circuits plane", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      5,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.snapshot.get","arguments":{"planes":["circuits"]}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].(map[string]any)
		if !ok {
			t.Fatalf("snapshot data type = %T; want map", envelope["data"])
		}
		planes, ok := data["planes"].(map[string]any)
		if !ok {
			t.Fatalf("snapshot planes type = %T; want map", data["planes"])
		}
		if _, ok := planes["circuits"]; !ok {
			t.Fatalf("snapshot planes missing circuits: %#v", planes)
		}
	})

	t.Run("semantic snapshot rejects unknown plane", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      6,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.snapshot.get","arguments":{"planes":["unknown"]}}`),
		})
		assertToolErrorCode(t, res, "INVALID_ARGUMENT")
	})

	t.Run("semantic snapshot timeout atomic", func(t *testing.T) {
		slowServer, err := NewServer(reg, &testInvoker{})
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		slowServer.SetStatusProvider(testStatusProvider{
			daemon: ServiceStatus{Status: "running"},
			adapter: ServiceStatus{
				Status: "connected",
			},
		})
		slowServer.SetSemanticProvider(testSemanticProvider{
			zones:      []Zone{{ID: "zone-a", Name: "Living"}},
			dhw:        &DhwStatus{Config: DhwConfig{OperatingMode: "AUTO"}},
			zonesDelay: 20 * time.Millisecond,
		})

		res := doRPC(t, slowServer.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      7,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.snapshot.get","arguments":{"planes":["zones","dhw"],"timeout_ms":10}}`),
		})
		assertToolErrorCode(t, res, "TIMEOUT")
	})

	t.Run("semantic snapshot timeout partial", func(t *testing.T) {
		slowServer, err := NewServer(reg, &testInvoker{})
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		slowServer.SetStatusProvider(testStatusProvider{
			daemon: ServiceStatus{Status: "running"},
			adapter: ServiceStatus{
				Status: "connected",
			},
		})
		slowServer.SetSemanticProvider(testSemanticProvider{
			zones:      []Zone{{ID: "zone-a", Name: "Living"}},
			dhw:        &DhwStatus{Config: DhwConfig{OperatingMode: "AUTO"}},
			zonesDelay: 20 * time.Millisecond,
		})

		res := doRPC(t, slowServer.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      8,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.snapshot.get","arguments":{"planes":["zones","dhw"],"timeout_ms":35,"allow_partial":true}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].(map[string]any)
		if !ok {
			t.Fatalf("partial snapshot data type = %T; want map", envelope["data"])
		}
		completed, ok := data["completed_planes"].([]any)
		if !ok || len(completed) == 0 {
			t.Fatalf("partial snapshot completed_planes = %#v; want at least one", data["completed_planes"])
		}
		if _, ok := data["error_planes"].([]any); !ok {
			t.Fatalf("partial snapshot error_planes type = %T; want []any", data["error_planes"])
		}
	})

	t.Run("semantic snapshot timeout partial no duplicate plane errors", func(t *testing.T) {
		slowServer, err := NewServer(reg, &testInvoker{})
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		slowServer.SetStatusProvider(testStatusProvider{
			daemon: ServiceStatus{Status: "running"},
			adapter: ServiceStatus{
				Status: "connected",
			},
		})
		slowServer.SetSemanticProvider(testSemanticProvider{
			zones:      []Zone{{ID: "zone-a", Name: "Living"}},
			dhw:        &DhwStatus{Config: DhwConfig{OperatingMode: "AUTO"}},
			zonesDelay: 5 * time.Millisecond,
		})

		res := doRPC(t, slowServer.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      9,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.semantic.snapshot.get","arguments":{"planes":["zones","dhw"],"timeout_ms":1,"allow_partial":true}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].(map[string]any)
		if !ok {
			t.Fatalf("partial snapshot data type = %T; want map", envelope["data"])
		}
		errorPlanes, ok := data["error_planes"].([]any)
		if !ok {
			t.Fatalf("partial snapshot error_planes type = %T; want []any", data["error_planes"])
		}
		if len(errorPlanes) != 2 {
			t.Fatalf("partial snapshot error_planes len = %d; want 2", len(errorPlanes))
		}
		seen := make(map[string]int, len(errorPlanes))
		for _, rawPlane := range errorPlanes {
			planeErr, ok := rawPlane.(map[string]any)
			if !ok {
				t.Fatalf("error plane type = %T; want map", rawPlane)
			}
			planeName, _ := planeErr["plane"].(string)
			seen[planeName]++
		}
		for _, planeName := range []string{"zones", "dhw"} {
			if seen[planeName] != 1 {
				t.Fatalf("plane %q error count = %d; want 1", planeName, seen[planeName])
			}
		}
	})
}

func TestServer_ToolsCallBoilerStatus_ReducedShape(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	flow := 52.5
	ret := 47.0
	pump := true
	heatingStatus := 2
	server.SetSemanticProvider(testSemanticProvider{
		boiler: &BoilerStatus{
			State: &BoilerState{
				FlowTemperatureC:         &flow,
				ReturnTemperatureC:       &ret,
				CentralHeatingPumpActive: &pump,
			},
			Diagnostics: &BoilerDiagnostics{
				HeatingStatusRaw: &heatingStatus,
			},
		},
	})

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.semantic.boiler_status.get","arguments":{}}`),
	})
	envelope := envelopeFromResult(t, res)
	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("boiler data type = %T; want map", envelope["data"])
	}
	if _, ok := data["config"]; ok {
		t.Fatalf("boiler data unexpectedly contains config: %#v", data["config"])
	}
	state, ok := data["state"].(map[string]any)
	if !ok {
		t.Fatalf("boiler state type = %T; want map", data["state"])
	}
	if got, _ := state["flow_temperature_c"].(float64); got != flow {
		t.Fatalf("boiler state.flow_temperature_c = %v; want %v", state["flow_temperature_c"], flow)
	}
	if got, _ := state["return_temperature_c"].(float64); got != ret {
		t.Fatalf("boiler state.return_temperature_c = %v; want %v", state["return_temperature_c"], ret)
	}
	if got, _ := state["central_heating_pump_active"].(bool); got != pump {
		t.Fatalf("boiler state.central_heating_pump_active = %v; want %v", state["central_heating_pump_active"], pump)
	}
	if _, ok := state["dhw_temperature_c"]; ok {
		t.Fatalf("boiler state unexpectedly contains dhw_temperature_c")
	}
	if _, ok := state["dhw_target_temperature_c"]; ok {
		t.Fatalf("boiler state unexpectedly contains dhw_target_temperature_c")
	}
	diagnostics, ok := data["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("boiler diagnostics type = %T; want map", data["diagnostics"])
	}
	if got, _ := diagnostics["heating_status_raw"].(float64); int(got) != heatingStatus {
		t.Fatalf("boiler diagnostics.heating_status_raw = %v; want %d", diagnostics["heating_status_raw"], heatingStatus)
	}
	if _, ok := diagnostics["dhw_status_raw"]; ok {
		t.Fatalf("boiler diagnostics unexpectedly contains dhw_status_raw")
	}
}

func TestServer_SnapshotConsistencyMode(t *testing.T) {
	plane := &testPlane{
		name: "heating",
		methods: []registry.Method{
			testMethod{
				name:     "get_status",
				readOnly: true,
				template: testTemplate{primary: 0xB5, secondary: 0x04},
			},
		},
	}
	reg := &testRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x08: testEntry{
				info: registry.DeviceInfo{
					Address:         0x08,
					Manufacturer:    "vaillant",
					DeviceID:        "device-a",
					SoftwareVersion: "1.0",
					HardwareVersion: "7603",
				},
				planes: []registry.Plane{plane},
			},
		},
		order: []byte{0x08},
	}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetStatusProvider(testStatusProvider{
		daemon: ServiceStatus{Status: "running"},
		adapter: ServiceStatus{
			Status: "unknown",
		},
	})

	capture := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.snapshot.capture","arguments":{}}`),
	})
	captureEnvelope := envelopeFromResult(t, capture)
	captureData, ok := captureEnvelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("capture data type = %T; want map", captureEnvelope["data"])
	}
	snapshotID, _ := captureData["snapshot_id"].(string)
	if snapshotID == "" {
		t.Fatal("capture snapshot_id empty")
	}

	server.SetStatusProvider(testStatusProvider{
		daemon: ServiceStatus{Status: "changed"},
		adapter: ServiceStatus{
			Status: "changed",
		},
	})
	reg.entries[0x08] = testEntry{
		info: registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "vaillant",
			DeviceID:        "device-b",
			SoftwareVersion: "2.0",
			HardwareVersion: "7603",
		},
		planes: []registry.Plane{plane},
	}

	liveRuntime := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.runtime.status.get","arguments":{}}`),
	}))
	liveRuntimeData, _ := liveRuntime["data"].(map[string]any)
	liveDaemon, _ := liveRuntimeData["daemon_status"].(map[string]any)
	if status, _ := liveDaemon["status"].(string); status != "changed" {
		t.Fatalf("live daemon status = %q; want changed", status)
	}

	snapshotRuntime := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.runtime.status.get","arguments":{"consistency":{"mode":"SNAPSHOT","snapshot_id":"` + snapshotID + `"}}}`),
	}))
	snapshotRuntimeData, _ := snapshotRuntime["data"].(map[string]any)
	snapshotDaemon, _ := snapshotRuntimeData["daemon_status"].(map[string]any)
	if status, _ := snapshotDaemon["status"].(string); status != "running" {
		t.Fatalf("snapshot daemon status = %q; want running", status)
	}
	snapshotMeta, _ := snapshotRuntime["meta"].(map[string]any)
	consistencyMeta, _ := snapshotMeta["consistency"].(map[string]any)
	if mode, _ := consistencyMeta["mode"].(string); mode != "SNAPSHOT" {
		t.Fatalf("snapshot consistency mode = %q; want SNAPSHOT", mode)
	}

	firstSnapshotDevices := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      4,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.registry.devices.list","arguments":{"consistency":{"mode":"SNAPSHOT","snapshot_id":"` + snapshotID + `"}}}`),
	}))
	secondSnapshotDevices := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      5,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.registry.devices.list","arguments":{"consistency":{"mode":"SNAPSHOT","snapshot_id":"` + snapshotID + `"}}}`),
	}))
	firstMeta, _ := firstSnapshotDevices["meta"].(map[string]any)
	secondMeta, _ := secondSnapshotDevices["meta"].(map[string]any)
	firstHash, _ := firstMeta["data_hash"].(string)
	secondHash, _ := secondMeta["data_hash"].(string)
	if firstHash == "" || secondHash == "" || firstHash != secondHash {
		t.Fatalf("snapshot data_hash mismatch: first=%q second=%q", firstHash, secondHash)
	}
	firstData, _ := firstSnapshotDevices["data"].([]any)
	if len(firstData) != 1 {
		t.Fatalf("snapshot devices len = %d; want 1", len(firstData))
	}
	firstDevice, _ := firstData[0].(map[string]any)
	if deviceID, _ := firstDevice["device_id"].(string); deviceID != "device-a" {
		t.Fatalf("snapshot device_id = %q; want device-a", deviceID)
	}

	drop := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      6,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.snapshot.drop","arguments":{"snapshot_id":"` + snapshotID + `"}}`),
	})
	dropEnvelope := envelopeFromResult(t, drop)
	dropData, _ := dropEnvelope["data"].(map[string]any)
	if dropped, _ := dropData["dropped"].(bool); !dropped {
		t.Fatalf("snapshot drop returned dropped=%v; want true", dropData["dropped"])
	}

	missing := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      7,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.registry.devices.list","arguments":{"consistency":{"mode":"SNAPSHOT","snapshot_id":"` + snapshotID + `"}}}`),
	})
	assertToolErrorCode(t, missing, "NOT_FOUND")
}

func TestServer_SnapshotConsistencyAdapterInfo(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	original := &AdapterHardwareInfo{
		FirmwareVersion:    "1.2.3",
		HardwareID:         "deadbeef",
		HardwareConfig:     "c0ffee",
		JumperFlags:        []string{"JP1", "JP2"},
		TemperatureC:       floatPtr(24.5),
		SupplyVoltageMV:    intPtr(2500),
		BusVoltageMaxDV:    intPtr(150),
		BusVoltageMinDV:    intPtr(130),
		ResetCause:         stringPtr("power_on"),
		ResetCauseCode:     bytePtr(0x04),
		RestartCount:       bytePtr(0x07),
		WiFiRSSIDBm:        intPtr(-62),
		LastIdentityQuery:  stringPtr("2026-03-12T13:00:00Z"),
		LastTelemetryQuery: stringPtr("2026-03-12T13:01:00Z"),
		VersionResponseLen: 8,
		InfoSupported:      true,
	}
	changed := &AdapterHardwareInfo{
		FirmwareVersion:    "9.9.9",
		HardwareID:         "changed",
		HardwareConfig:     "changed",
		JumperFlags:        []string{"JP9"},
		VersionResponseLen: 2,
		InfoSupported:      false,
	}

	server.SetSemanticProvider(testSemanticProvider{adapterInfo: original})

	capture := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.snapshot.capture","arguments":{}}`),
	}))
	captureData, ok := capture["data"].(map[string]any)
	if !ok {
		t.Fatalf("capture data type = %T; want map", capture["data"])
	}
	snapshotID, _ := captureData["snapshot_id"].(string)
	if snapshotID == "" {
		t.Fatal("capture snapshot_id empty")
	}

	server.SetSemanticProvider(testSemanticProvider{adapterInfo: changed})

	live := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.semantic.adapter_info.get","arguments":{}}`),
	}))
	liveData, ok := live["data"].(map[string]any)
	if !ok {
		t.Fatalf("live adapter_info data type = %T; want map", live["data"])
	}
	if got, _ := liveData["hardware_id"].(string); got != "changed" {
		t.Fatalf("live adapter_info hardware_id = %q; want changed", got)
	}
	if got, _ := liveData["info_supported"].(bool); got {
		t.Fatalf("live adapter_info info_supported = %v; want false", got)
	}

	snapshot := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.semantic.adapter_info.get","arguments":{"consistency":{"mode":"SNAPSHOT","snapshot_id":"` + snapshotID + `"}}}`),
	}))
	snapshotData, ok := snapshot["data"].(map[string]any)
	if !ok {
		t.Fatalf("snapshot adapter_info data type = %T; want map", snapshot["data"])
	}
	if got, _ := snapshotData["hardware_id"].(string); got != "deadbeef" {
		t.Fatalf("snapshot adapter_info hardware_id = %q; want deadbeef", got)
	}
	if got, _ := snapshotData["hardware_config"].(string); got != "c0ffee" {
		t.Fatalf("snapshot adapter_info hardware_config = %q; want c0ffee", got)
	}
	if got, _ := snapshotData["info_supported"].(bool); !got {
		t.Fatalf("snapshot adapter_info info_supported = %v; want true", got)
	}
	if got, _ := snapshotData["version_response_len"].(float64); int(got) != 8 {
		t.Fatalf("snapshot adapter_info version_response_len = %v; want 8", snapshotData["version_response_len"])
	}
	jumperFlags, ok := snapshotData["jumper_flags"].([]any)
	if !ok || len(jumperFlags) != 2 {
		t.Fatalf("snapshot adapter_info jumper_flags = %#v; want 2 entries", snapshotData["jumper_flags"])
	}
	if got, _ := jumperFlags[0].(string); got != "JP1" {
		t.Fatalf("snapshot adapter_info jumper_flags[0] = %q; want JP1", got)
	}
	if got, _ := jumperFlags[1].(string); got != "JP2" {
		t.Fatalf("snapshot adapter_info jumper_flags[1] = %q; want JP2", got)
	}

	snapshotPlane := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      4,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.semantic.snapshot.get","arguments":{"planes":["adapter_info"],"consistency":{"mode":"SNAPSHOT","snapshot_id":"` + snapshotID + `"}}}`),
	}))
	snapshotPlaneData, ok := snapshotPlane["data"].(map[string]any)
	if !ok {
		t.Fatalf("snapshot plane data type = %T; want map", snapshotPlane["data"])
	}
	planes, ok := snapshotPlaneData["planes"].(map[string]any)
	if !ok {
		t.Fatalf("snapshot plane planes type = %T; want map", snapshotPlaneData["planes"])
	}
	adapterPlane, ok := planes["adapter_info"].(map[string]any)
	if !ok {
		t.Fatalf("snapshot plane adapter_info type = %T; want map", planes["adapter_info"])
	}
	if got, _ := adapterPlane["hardware_id"].(string); got != "deadbeef" {
		t.Fatalf("snapshot plane adapter_info hardware_id = %q; want deadbeef", got)
	}
}

func TestServer_BusObservabilitySnapshotConsistency(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	base := time.Date(2026, time.March, 12, 13, 0, 0, 0, time.UTC)
	provider := &testBusObservabilityProvider{
		snapshot: BusObservabilitySnapshot{
			Summary: &BusSummary{
				Status: &BusObservabilityStatus{
					TransportClass: "ens",
					Capability: BusObservabilityCapability{
						ActiveSupported:    true,
						PassiveSupported:   true,
						BroadcastSupported: true,
						PassiveAvailable:   true,
						PassiveState:       "available",
						EndpointState:      "connected",
						TapConnected:       true,
					},
					Warmup: BusObservabilityWarmup{State: "available"},
					TimingQuality: BusObservabilityTimingQuality{
						Active:      "estimated",
						Passive:     "estimated",
						Busy:        "estimated",
						Periodicity: "estimated",
					},
				},
				Messages:    BusBoundedListSummary{Count: 1, Capacity: 8},
				Periodicity: BusBoundedListSummary{Count: 1, Capacity: 4},
			},
			Messages: []BusMessage{
				{Scope: "active", Family: "B509", FrameType: "initiator_target", Outcome: "success", ObservedAt: base, SourceAddress: 0x08, TargetAddress: 0x15, RequestLen: 6, ResponseLen: 4},
			},
			Periodicity: []BusPeriodicityEntry{
				{SourceBucket: "0x08", TargetBucket: "0x15", Primary: 0xB5, Secondary: 0x09, Family: "B509", State: "available", LastSeen: base.Add(30 * time.Second), SampleCount: 3, LastInterval: "30s", MeanInterval: "30s", MinInterval: "30s", MaxInterval: "30s"},
			},
		},
	}
	server.SetBusObservabilityProvider(provider)

	capture := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.snapshot.capture","arguments":{}}`),
	}))
	captureData, ok := capture["data"].(map[string]any)
	if !ok {
		t.Fatalf("capture data type = %T; want map", capture["data"])
	}
	snapshotID, _ := captureData["snapshot_id"].(string)
	if snapshotID == "" {
		t.Fatal("capture snapshot_id empty")
	}

	provider.snapshot = BusObservabilitySnapshot{
		Summary: &BusSummary{
			Status: &BusObservabilityStatus{
				TransportClass: "ebusd-tcp",
				Capability: BusObservabilityCapability{
					ActiveSupported:    true,
					PassiveSupported:   false,
					BroadcastSupported: false,
					PassiveAvailable:   false,
					PassiveState:       "unavailable",
					PassiveReason:      "unsupported_or_misconfigured",
					EndpointState:      "unsupported_or_misconfigured",
				},
				Warmup: BusObservabilityWarmup{State: "unavailable"},
				TimingQuality: BusObservabilityTimingQuality{
					Active:      "unavailable",
					Passive:     "unavailable",
					Busy:        "unavailable",
					Periodicity: "unavailable",
				},
				Degraded: BusObservabilityDegraded{
					Active:  true,
					Reasons: []string{"unsupported_or_misconfigured"},
				},
			},
			Messages:    BusBoundedListSummary{Count: 2, Capacity: 8},
			Periodicity: BusBoundedListSummary{Count: 2, Capacity: 4},
		},
		Messages: []BusMessage{
			{Scope: "active", Family: "B509", FrameType: "initiator_target", Outcome: "success", ObservedAt: base, SourceAddress: 0x08, TargetAddress: 0x15, RequestLen: 6, ResponseLen: 4},
			{Scope: "active", Family: "other", FrameType: "abandoned_partial", Outcome: "timeout", ObservedAt: base.Add(time.Minute), SourceAddress: 0x26, TargetAddress: 0x08, RequestLen: 7, ResponseLen: 0},
		},
		Periodicity: []BusPeriodicityEntry{
			{SourceBucket: "0x08", TargetBucket: "0x15", Primary: 0xB5, Secondary: 0x09, Family: "B509", State: "available", LastSeen: base.Add(30 * time.Second), SampleCount: 3, LastInterval: "30s", MeanInterval: "30s", MinInterval: "30s", MaxInterval: "30s"},
			{SourceBucket: "0x15", TargetBucket: "0xfe", Primary: 0xB5, Secondary: 0x24, Family: "B524", State: "warming_up", LastSeen: base.Add(90 * time.Second), SampleCount: 1, LastInterval: "1m", MeanInterval: "1m", MinInterval: "1m", MaxInterval: "1m"},
		},
	}

	liveSummary := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.bus.summary.get","arguments":{}}`),
	}))
	liveSummaryData, _ := liveSummary["data"].(map[string]any)
	liveStatus, _ := liveSummaryData["status"].(map[string]any)
	liveCapability, _ := liveStatus["capability"].(map[string]any)
	if got, _ := liveCapability["passive_state"].(string); got != "unavailable" {
		t.Fatalf("live bus passive_state = %q; want unavailable", got)
	}

	snapshotSummary := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.bus.summary.get","arguments":{"consistency":{"mode":"SNAPSHOT","snapshot_id":"` + snapshotID + `"}}}`),
	}))
	snapshotSummaryData, _ := snapshotSummary["data"].(map[string]any)
	snapshotStatus, _ := snapshotSummaryData["status"].(map[string]any)
	snapshotCapability, _ := snapshotStatus["capability"].(map[string]any)
	if got, _ := snapshotCapability["passive_state"].(string); got != "available" {
		t.Fatalf("snapshot bus passive_state = %q; want available", got)
	}

	snapshotMessages := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      4,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.bus.messages.list","arguments":{"consistency":{"mode":"SNAPSHOT","snapshot_id":"` + snapshotID + `"}}}`),
	}))
	snapshotMessagesData, _ := snapshotMessages["data"].(map[string]any)
	if got, _ := snapshotMessagesData["count"].(float64); int(got) != 1 {
		t.Fatalf("snapshot bus messages count = %v; want 1", snapshotMessagesData["count"])
	}
	snapshotMessageItems, _ := snapshotMessagesData["items"].([]any)
	if len(snapshotMessageItems) != 1 {
		t.Fatalf("snapshot bus message items len = %d; want 1", len(snapshotMessageItems))
	}

	snapshotPeriodicity := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      5,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.bus.periodicity.list","arguments":{"consistency":{"mode":"SNAPSHOT","snapshot_id":"` + snapshotID + `"}}}`),
	}))
	snapshotPeriodicityData, _ := snapshotPeriodicity["data"].(map[string]any)
	if got, _ := snapshotPeriodicityData["count"].(float64); int(got) != 1 {
		t.Fatalf("snapshot bus periodicity count = %v; want 1", snapshotPeriodicityData["count"])
	}
	snapshotPeriodicityItems, _ := snapshotPeriodicityData["items"].([]any)
	if len(snapshotPeriodicityItems) != 1 {
		t.Fatalf("snapshot bus periodicity items len = %d; want 1", len(snapshotPeriodicityItems))
	}
}

func TestServer_WatchSummarySnapshotConsistency(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	base := time.Date(2026, time.March, 12, 14, 0, 0, 0, time.UTC)
	provider := &testWatchSummaryProvider{
		summary: WatchSummary{
			LastUpdatedAt: &base,
			ActivationCounts: WatchSummaryActivationCounts{
				ActiveKeys: 1,
			},
			Degraded: WatchSummaryDegraded{
				Active:           false,
				ShadowingEnabled: true,
			},
		},
	}
	server.SetWatchSummaryProvider(provider)

	capture := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.snapshot.capture","arguments":{}}`),
	}))
	captureData, ok := capture["data"].(map[string]any)
	if !ok {
		t.Fatalf("capture data type = %T; want map", capture["data"])
	}
	snapshotID, _ := captureData["snapshot_id"].(string)
	if snapshotID == "" {
		t.Fatal("capture snapshot_id empty")
	}

	updatedAt := base.Add(1 * time.Minute)
	provider.summary = WatchSummary{
		LastUpdatedAt: &updatedAt,
		ActivationCounts: WatchSummaryActivationCounts{
			ActiveKeys: 7,
		},
		Degraded: WatchSummaryDegraded{
			Active:               true,
			ShadowingEnabled:     false,
			PinnedBudgetDegraded: true,
			Reasons:              []string{"shadow_pinned_budget_degraded"},
		},
	}

	live := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.watch.summary.get","arguments":{}}`),
	}))
	liveData, _ := live["data"].(map[string]any)
	if got, _ := liveData["last_updated_at"].(string); got != updatedAt.Format(time.RFC3339Nano) {
		t.Fatalf("live watch summary last_updated_at = %q; want %s", got, updatedAt.Format(time.RFC3339Nano))
	}
	liveActivation, _ := liveData["activation_counts"].(map[string]any)
	if got, _ := liveActivation["active_keys"].(float64); int(got) != 7 {
		t.Fatalf("live watch summary active_keys = %v; want 7", liveActivation["active_keys"])
	}

	snap := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.watch.summary.get","arguments":{"consistency":{"mode":"SNAPSHOT","snapshot_id":"` + snapshotID + `"}}}`),
	}))
	snapshotData, _ := snap["data"].(map[string]any)
	if got, _ := snapshotData["last_updated_at"].(string); got != base.Format(time.RFC3339Nano) {
		t.Fatalf("snapshot watch summary last_updated_at = %q; want %s", got, base.Format(time.RFC3339Nano))
	}
	snapshotActivation, _ := snapshotData["activation_counts"].(map[string]any)
	if got, _ := snapshotActivation["active_keys"].(float64); int(got) != 1 {
		t.Fatalf("snapshot watch summary active_keys = %v; want 1", snapshotActivation["active_keys"])
	}
}

func TestServer_RegistryReadToolsOrderingAndMetadata(t *testing.T) {
	heatingPlane := &testPlane{
		name: "heating",
		methods: []registry.Method{
			testMethod{
				name:     "zeta",
				readOnly: true,
				template: testTemplate{primary: 0xB5, secondary: 0x0A},
			},
			testMetadataMethod{
				testMethod: testMethod{
					name:     "alpha",
					readOnly: true,
					template: testTemplate{primary: 0xB5, secondary: 0x04},
				},
				mutability: "unknown",
				danger:     "dangerous",
				routable:   false,
			},
		},
	}
	plainPlane := testPlainPlane{
		name: "alpha",
		methods: []registry.Method{
			testMethod{
				name:     "noop",
				readOnly: true,
				template: testTemplate{primary: 0xB5, secondary: 0x01},
			},
		},
	}

	reg := &testRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x10: testEntry{
				info: registry.DeviceInfo{
					Address:         0x10,
					Manufacturer:    "vaillant",
					DeviceID:        "device-b",
					SoftwareVersion: "1.0",
					HardwareVersion: "7603",
				},
				planes: []registry.Plane{heatingPlane, plainPlane},
			},
			0x08: testEntry{
				info: registry.DeviceInfo{
					Address:         0x08,
					Manufacturer:    "vaillant",
					DeviceID:        "device-a",
					SoftwareVersion: "1.0",
					HardwareVersion: "7603",
				},
				planes: []registry.Plane{plainPlane},
			},
		},
		order: []byte{0x10, 0x08},
	}

	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	t.Run("devices list sorted by address", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      1,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.registry.devices.list","arguments":{}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].([]any)
		if !ok || len(data) != 2 {
			t.Fatalf("devices data = %#v; want 2 entries", envelope["data"])
		}
		first, _ := data[0].(map[string]any)
		if address, _ := first["address"].(float64); int(address) != 0x08 {
			t.Fatalf("first device address = %v; want 8", first["address"])
		}
	})

	t.Run("device get by address", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      2,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.registry.devices.get","arguments":{"address":16}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].(map[string]any)
		if !ok {
			t.Fatalf("device get data type = %T; want map", envelope["data"])
		}
		if address, _ := data["address"].(float64); int(address) != 0x10 {
			t.Fatalf("device get address = %v; want 16", data["address"])
		}
	})

	t.Run("planes list sorted and routable", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      3,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.registry.planes.list","arguments":{"address":16}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].([]any)
		if !ok || len(data) != 2 {
			t.Fatalf("planes data = %#v; want 2 entries", envelope["data"])
		}
		first, _ := data[0].(map[string]any)
		second, _ := data[1].(map[string]any)
		if name, _ := first["name"].(string); name != "alpha" {
			t.Fatalf("first plane name = %q; want alpha", name)
		}
		if name, _ := second["name"].(string); name != "heating" {
			t.Fatalf("second plane name = %q; want heating", name)
		}
		if routable, _ := first["routable"].(bool); routable {
			t.Fatalf("alpha routable = true; want false")
		}
		if routable, _ := second["routable"].(bool); !routable {
			t.Fatalf("heating routable = false; want true")
		}
	})

	t.Run("methods list sorted with metadata", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      4,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.registry.methods.list","arguments":{"address":16,"plane":"heating"}}`),
		})
		envelope := envelopeFromResult(t, res)
		data, ok := envelope["data"].([]any)
		if !ok || len(data) != 2 {
			t.Fatalf("methods data = %#v; want 2 entries", envelope["data"])
		}
		first, _ := data[0].(map[string]any)
		second, _ := data[1].(map[string]any)
		if name, _ := first["name"].(string); name != "alpha" {
			t.Fatalf("first method name = %q; want alpha", name)
		}
		if name, _ := second["name"].(string); name != "zeta" {
			t.Fatalf("second method name = %q; want zeta", name)
		}
		if mutability, _ := first["mutability"].(string); mutability != methodMutabilityUnknown {
			t.Fatalf("alpha mutability = %q; want %q", mutability, methodMutabilityUnknown)
		}
		if danger, _ := first["danger_level"].(string); danger != methodDangerDangerous {
			t.Fatalf("alpha danger = %q; want %q", danger, methodDangerDangerous)
		}
		if routable, _ := first["routable"].(bool); routable {
			t.Fatalf("alpha routable = true; want false (explicit override)")
		}
	})
}

func TestServer_ToolsCallLegacyAliases(t *testing.T) {
	plane := &testPlane{
		name: "heating",
		methods: []registry.Method{
			testMethod{
				name:     "get_status",
				readOnly: true,
				template: testTemplate{primary: 0xB5, secondary: 0x04},
			},
		},
	}
	entry := testEntry{
		info: registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "vaillant",
			DeviceID:        "device-a",
			SoftwareVersion: "1.0",
			HardwareVersion: "7603",
		},
		planes: []registry.Plane{plane},
	}
	reg := &testRegistry{
		entries: map[byte]registry.DeviceEntry{0x08: entry},
		order:   []byte{0x08},
	}
	invoker := &testInvoker{}
	server, err := NewServer(reg, invoker)
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.devices","arguments":{}}`),
	})
	if res.Error != nil {
		t.Fatalf("legacy devices error = %+v", res.Error)
	}

	res = doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.invoke","arguments":{"address":8,"plane":"heating","method":"get_status","params":{}}}`),
	})
	if res.Error != nil {
		t.Fatalf("legacy invoke error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("legacy invoke result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); isError {
		t.Fatalf("legacy invoke isError=true; want false")
	}
}

func TestServer_ToolsCallInvokeErrorEnvelope(t *testing.T) {
	plane := &testPlane{
		name:    "heating",
		methods: []registry.Method{},
	}
	entry := testEntry{
		info: registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "vaillant",
			DeviceID:        "device-a",
			SoftwareVersion: "1.0",
			HardwareVersion: "7603",
		},
		planes: []registry.Plane{plane},
	}
	reg := &testRegistry{
		entries: map[byte]registry.DeviceEntry{0x08: entry},
		order:   []byte{0x08},
	}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.invoke","arguments":{"address":9,"plane":"heating","method":"get_status","params":{}}}`),
	})
	if res.Error != nil {
		t.Fatalf("tools/call invoke error response should be content-level, got rpc error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("invoke error result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); !isError {
		t.Fatalf("invoke error result isError=false; want true")
	}
	content, ok := resultMap["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("invoke error content = %#v; want 1 item", resultMap["content"])
	}
	contentItem, ok := content[0].(map[string]any)
	if !ok {
		t.Fatalf("invoke error content item type = %T; want map", content[0])
	}
	text, _ := contentItem["text"].(string)
	if text == "" {
		t.Fatal("invoke error content.text empty")
	}
	envelope := parseEnvelope(t, text)
	errorPayload, ok := envelope["error"].(map[string]any)
	if !ok {
		t.Fatalf("invoke error envelope error type = %T; want map", envelope["error"])
	}
	if code, _ := errorPayload["code"].(string); code != "NOT_FOUND" {
		t.Fatalf("invoke error code = %q; want NOT_FOUND", code)
	}
	if source, _ := errorPayload["source_layer"].(string); source != "ebusreg" {
		t.Fatalf("invoke source_layer = %q; want ebusreg", source)
	}
}

func TestServer_ToolsCallInvokeV1SafetyGuards(t *testing.T) {
	plane := &testPlane{
		name: "heating",
		methods: []registry.Method{
			testMethod{
				name:     "set_target",
				readOnly: false,
				template: testTemplate{primary: 0xB5, secondary: 0x05},
			},
		},
	}
	entry := testEntry{
		info: registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "vaillant",
			DeviceID:        "device-a",
			SoftwareVersion: "1.0",
			HardwareVersion: "7603",
		},
		planes: []registry.Plane{plane},
	}
	reg := &testRegistry{
		entries: map[byte]registry.DeviceEntry{0x08: entry},
		order:   []byte{0x08},
	}
	invoker := &testInvoker{}
	server, err := NewServer(reg, invoker)
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	t.Run("missing intent", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      1,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.rpc.invoke","arguments":{"address":8,"plane":"heating","method":"set_target","params":{},"allow_dangerous":false}}`),
		})
		assertToolErrorCode(t, res, "INVALID_ARGUMENT")
	})

	t.Run("read only intent denied for mutating method", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      2,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.rpc.invoke","arguments":{"address":8,"plane":"heating","method":"set_target","params":{},"intent":"READ_ONLY","allow_dangerous":false}}`),
		})
		assertToolErrorCode(t, res, "PERMISSION_DENIED")
	})

	t.Run("mutate intent requires idempotency key", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      3,
			Method:  "tools/call",
			Params:  json.RawMessage(`{"name":"ebus.v1.rpc.invoke","arguments":{"address":8,"plane":"heating","method":"set_target","params":{},"intent":"MUTATE","allow_dangerous":true}}`),
		})
		assertToolErrorCode(t, res, "PERMISSION_DENIED")
	})
}

func TestServer_ToolsCallInvokeV1Idempotency(t *testing.T) {
	plane := &testPlane{
		name: "heating",
		methods: []registry.Method{
			testMethod{
				name:     "set_target",
				readOnly: false,
				template: testTemplate{primary: 0xB5, secondary: 0x05},
			},
		},
	}
	entry := testEntry{
		info: registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "vaillant",
			DeviceID:        "device-a",
			SoftwareVersion: "1.0",
			HardwareVersion: "7603",
		},
		planes: []registry.Plane{plane},
	}
	reg := &testRegistry{
		entries: map[byte]registry.DeviceEntry{0x08: entry},
		order:   []byte{0x08},
	}
	invoker := &testInvoker{}
	server, err := NewServer(reg, invoker)
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	first := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.rpc.invoke","arguments":{"address":8,"plane":"heating","method":"set_target","params":{"target_c":21.5},"intent":"MUTATE","allow_dangerous":true,"idempotency_key":"abc123"}}`),
	})
	if first.Error != nil {
		t.Fatalf("first invoke rpc error = %+v", first.Error)
	}

	second := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.rpc.invoke","arguments":{"address":8,"plane":"heating","method":"set_target","params":{"target_c":21.5},"intent":"MUTATE","allow_dangerous":true,"idempotency_key":"abc123"}}`),
	})
	if second.Error != nil {
		t.Fatalf("second invoke rpc error = %+v", second.Error)
	}
	if len(invoker.calls) != 1 {
		t.Fatalf("invoker calls = %d; want 1 (deduped)", len(invoker.calls))
	}

	conflict := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.rpc.invoke","arguments":{"address":8,"plane":"heating","method":"set_target","params":{"target_c":22.0},"intent":"MUTATE","allow_dangerous":true,"idempotency_key":"abc123"}}`),
	})
	assertToolErrorCode(t, conflict, "CONFLICT")
}

func TestServer_ToolsCallInvokeV1Timeout(t *testing.T) {
	plane := &testPlane{
		name: "heating",
		methods: []registry.Method{
			testMethod{
				name:     "get_status",
				readOnly: true,
				template: testTemplate{primary: 0xB5, secondary: 0x04},
			},
		},
	}
	entry := testEntry{
		info: registry.DeviceInfo{
			Address:         0x08,
			Manufacturer:    "vaillant",
			DeviceID:        "device-a",
			SoftwareVersion: "1.0",
			HardwareVersion: "7603",
		},
		planes: []registry.Plane{plane},
	}
	reg := &testRegistry{
		entries: map[byte]registry.DeviceEntry{0x08: entry},
		order:   []byte{0x08},
	}
	invoker := &slowInvoker{delay: 50 * time.Millisecond}
	server, err := NewServer(reg, invoker)
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.rpc.invoke","arguments":{"address":8,"plane":"heating","method":"get_status","params":{},"intent":"READ_ONLY","allow_dangerous":false,"timeout_ms":1}}`),
	})
	assertToolErrorCode(t, res, "TIMEOUT")
}

func TestClassifyToolError_KnownMappings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		err         error
		code        string
		retriable   bool
		sourceLayer string
	}{
		{name: "invalid payload", err: ebuserrors.ErrInvalidPayload, code: "INVALID_ARGUMENT", retriable: false, sourceLayer: "ebusreg"},
		{name: "no such device", err: ebuserrors.ErrNoSuchDevice, code: "NOT_FOUND", retriable: false, sourceLayer: "ebusreg"},
		{name: "nack", err: ebuserrors.ErrNACK, code: "PROTOCOL_ERROR", retriable: false, sourceLayer: "ebusgo"},
		{name: "timeout", err: ebuserrors.ErrTimeout, code: "TIMEOUT", retriable: true, sourceLayer: "ebusgo"},
		{name: "crc mismatch", err: ebuserrors.ErrCRCMismatch, code: "PROTOCOL_ERROR", retriable: true, sourceLayer: "ebusgo"},
		{name: "transport closed", err: ebuserrors.ErrTransportClosed, code: "BUS_UNAVAILABLE", retriable: false, sourceLayer: "ebusgo"},
		{name: "bus collision", err: ebuserrors.ErrBusCollision, code: "BUS_UNAVAILABLE", retriable: true, sourceLayer: "ebusgo"},
		{name: "retry exhausted", err: ebuserrors.ErrRetryExhausted, code: "BUS_UNAVAILABLE", retriable: true, sourceLayer: "ebusgo"},
		{name: "deadline exceeded", err: context.DeadlineExceeded, code: "TIMEOUT", retriable: true, sourceLayer: "gateway"},
		{name: "idempotency conflict", err: errInvokeIdempotencyConflict, code: "CONFLICT", retriable: false, sourceLayer: "gateway"},
		{name: "snapshot not found", err: errSnapshotNotFound, code: "NOT_FOUND", retriable: false, sourceLayer: "gateway"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			code, retriable, sourceLayer := classifyToolError(tc.err)
			if code != tc.code || retriable != tc.retriable || sourceLayer != tc.sourceLayer {
				t.Fatalf("classifyToolError(%s) = (%q,%v,%q); want (%q,%v,%q)",
					tc.name, code, retriable, sourceLayer, tc.code, tc.retriable, tc.sourceLayer)
			}
		})
	}
}

func boolPtr(value bool) *bool {
	v := value
	return &v
}

func floatPtr(value float64) *float64 {
	v := value
	return &v
}

func intPtr(value int) *int {
	v := value
	return &v
}

func bytePtr(value byte) *byte {
	v := value
	return &v
}

func stringPtr(value string) *string {
	v := value
	return &v
}

func parseEnvelope(t *testing.T, text string) map[string]any {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal([]byte(text), &envelope); err != nil {
		t.Fatalf("Unmarshal envelope error = %v. text=%q", err, text)
	}
	return envelope
}

func envelopeFromResult(t *testing.T, res rpcResponse) map[string]any {
	t.Helper()
	if res.Error != nil {
		t.Fatalf("unexpected rpc error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T; want map", res.Result)
	}
	content, ok := resultMap["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("content = %#v; want 1 item", resultMap["content"])
	}
	contentItem, ok := content[0].(map[string]any)
	if !ok {
		t.Fatalf("content item type = %T; want map", content[0])
	}
	text, _ := contentItem["text"].(string)
	if text == "" {
		t.Fatal("content.text empty")
	}
	return parseEnvelope(t, text)
}

func hasToolName(tools []any, name string) bool {
	for _, raw := range tools {
		toolMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if got, _ := toolMap["name"].(string); got == name {
			return true
		}
	}
	return false
}

func findToolByName(tools []any, name string) map[string]any {
	for _, raw := range tools {
		toolMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if got, _ := toolMap["name"].(string); got == name {
			return toolMap
		}
	}
	return nil
}

func assertToolErrorCode(t *testing.T, res rpcResponse, wantCode string) {
	t.Helper()
	if res.Error != nil {
		t.Fatalf("unexpected rpc error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); !isError {
		t.Fatalf("isError=false; want true")
	}
	content, ok := resultMap["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("content = %#v; want 1 item", resultMap["content"])
	}
	contentItem, ok := content[0].(map[string]any)
	if !ok {
		t.Fatalf("content item type = %T; want map", content[0])
	}
	text, _ := contentItem["text"].(string)
	if text == "" {
		t.Fatal("content.text empty")
	}
	envelope := parseEnvelope(t, text)
	errorPayload, ok := envelope["error"].(map[string]any)
	if !ok {
		t.Fatalf("error payload type = %T; want map", envelope["error"])
	}
	if code, _ := errorPayload["code"].(string); code != wantCode {
		t.Fatalf("error code = %q; want %q", code, wantCode)
	}
}

func doRPC(t *testing.T, handler http.Handler, req rpcRequest) rpcResponse {
	t.Helper()

	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Marshal request error = %v", err)
	}
	r := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(raw))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	var res rpcResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("Unmarshal response error = %v. body=%q", err, w.Body.String())
	}
	return res
}

type testScheduleWriter struct {
	zoneResult *TimeProgramWriteResult
	zoneErr    error
	dhwResult  *TimeProgramWriteResult
	dhwErr     error
	lastZone   int
	lastDay    int
	lastSlots  []TimeProgramSlot
}

func (w *testScheduleWriter) SetZoneTimeProgram(ctx context.Context, zone int, weekday int, slots []TimeProgramSlot) (*TimeProgramWriteResult, error) {
	w.lastZone = zone
	w.lastDay = weekday
	w.lastSlots = slots
	return w.zoneResult, w.zoneErr
}

func (w *testScheduleWriter) SetDhwTimeProgram(ctx context.Context, weekday int, slots []TimeProgramSlot) (*TimeProgramWriteResult, error) {
	w.lastDay = weekday
	w.lastSlots = slots
	return w.dhwResult, w.dhwErr
}

func TestScheduleWriteNoWriter(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolSemanticSchedulesSetZoneName + `","arguments":{"zone":0,"weekday":0,"slots":[]}}`),
	})
	if res.Error != nil {
		t.Fatalf("unexpected rpc error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); !isError {
		t.Fatalf("expected isError=true when writer is nil")
	}
}

func TestScheduleWriteZoneSuccess(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	writer := &testScheduleWriter{
		zoneResult: &TimeProgramWriteResult{
			Success: true,
			SlotResults: []TimeProgramSlotResult{
				{SlotIndex: 0, Accepted: true, ErrorCode: 0, ErrorDesc: "accepted"},
			},
		},
	}
	server.SetScheduleWriter(writer)

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolSemanticSchedulesSetZoneName + `","arguments":{"zone":1,"weekday":3,"slots":[{"start_hour":6,"start_minute":0,"end_hour":22,"end_minute":0,"temperature_c":21.5}]}}`),
	})

	envelope := envelopeFromResult(t, res)
	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("data type = %T; want map", envelope["data"])
	}
	if success, _ := data["success"].(bool); !success {
		t.Fatalf("expected success=true, got %v", data)
	}
	if writer.lastZone != 1 {
		t.Fatalf("zone = %d; want 1", writer.lastZone)
	}
	if writer.lastDay != 3 {
		t.Fatalf("weekday = %d; want 3", writer.lastDay)
	}
	if len(writer.lastSlots) != 1 {
		t.Fatalf("slots len = %d; want 1", len(writer.lastSlots))
	}
	slot := writer.lastSlots[0]
	if slot.StartHour != 6 || slot.StartMinute != 0 || slot.EndHour != 22 || slot.EndMinute != 0 {
		t.Fatalf("slot times = %d:%d-%d:%d; want 6:00-22:00", slot.StartHour, slot.StartMinute, slot.EndHour, slot.EndMinute)
	}
	if slot.TemperatureC == nil || *slot.TemperatureC != 21.5 {
		t.Fatalf("slot temp = %v; want 21.5", slot.TemperatureC)
	}
}

func TestScheduleWriteDhwSuccess(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	writer := &testScheduleWriter{
		dhwResult: &TimeProgramWriteResult{
			Success: true,
			SlotResults: []TimeProgramSlotResult{
				{SlotIndex: 0, Accepted: true, ErrorCode: 0, ErrorDesc: "accepted"},
			},
		},
	}
	server.SetScheduleWriter(writer)

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolSemanticSchedulesSetDhwName + `","arguments":{"weekday":5,"slots":[{"start_hour":5,"start_minute":30,"end_hour":23,"end_minute":0}]}}`),
	})

	envelope := envelopeFromResult(t, res)
	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("data type = %T; want map", envelope["data"])
	}
	if success, _ := data["success"].(bool); !success {
		t.Fatalf("expected success=true, got %v", data)
	}
	if writer.lastDay != 5 {
		t.Fatalf("weekday = %d; want 5", writer.lastDay)
	}
	if len(writer.lastSlots) != 1 {
		t.Fatalf("slots len = %d; want 1", len(writer.lastSlots))
	}
	if writer.lastSlots[0].TemperatureC != nil {
		t.Fatalf("dhw slot temp should be nil (sentinel), got %v", *writer.lastSlots[0].TemperatureC)
	}
}

func TestScheduleWriteDhwWithTemp(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	writer := &testScheduleWriter{
		dhwResult: &TimeProgramWriteResult{
			Success: true,
			SlotResults: []TimeProgramSlotResult{
				{SlotIndex: 0, Accepted: true, ErrorCode: 0, ErrorDesc: "accepted"},
			},
		},
	}
	server.SetScheduleWriter(writer)

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolSemanticSchedulesSetDhwName + `","arguments":{"weekday":0,"slots":[{"start_hour":6,"start_minute":0,"end_hour":22,"end_minute":0,"temperature_c":55.0}]}}`),
	})

	envelope := envelopeFromResult(t, res)
	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("data type = %T; want map", envelope["data"])
	}
	if success, _ := data["success"].(bool); !success {
		t.Fatalf("expected success=true, got %v", data)
	}
	if writer.lastSlots[0].TemperatureC == nil {
		t.Fatal("expected temperature to be set")
	}
	if *writer.lastSlots[0].TemperatureC != 55.0 {
		t.Fatalf("temp = %f; want 55.0", *writer.lastSlots[0].TemperatureC)
	}
}

func TestScheduleWriteZoneMissingTemp(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	writer := &testScheduleWriter{}
	server.SetScheduleWriter(writer)

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolSemanticSchedulesSetZoneName + `","arguments":{"zone":0,"weekday":0,"slots":[{"start_hour":6,"start_minute":0,"end_hour":22,"end_minute":0}]}}`),
	})
	if res.Error != nil {
		t.Fatalf("unexpected rpc error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); !isError {
		t.Fatalf("expected isError=true when temp missing for zone")
	}
}

func TestScheduleWriteZoneRejectsFloatWeekday(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	writer := &testScheduleWriter{
		zoneResult: &TimeProgramWriteResult{Success: true},
	}
	server.SetScheduleWriter(writer)

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolSemanticSchedulesSetZoneName + `","arguments":{"zone":0,"weekday":1.5,"slots":[{"start_hour":6,"start_minute":0,"end_hour":22,"end_minute":0,"temperature_c":21.5}]}}`),
	})
	if res.Error != nil {
		t.Fatalf("unexpected rpc error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); !isError {
		t.Fatalf("expected isError=true for fractional weekday")
	}
}

func TestScheduleWriteZoneRejectsFloatZone(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	writer := &testScheduleWriter{
		zoneResult: &TimeProgramWriteResult{Success: true},
	}
	server.SetScheduleWriter(writer)

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolSemanticSchedulesSetZoneName + `","arguments":{"zone":1.9,"weekday":0,"slots":[{"start_hour":6,"start_minute":0,"end_hour":22,"end_minute":0,"temperature_c":21.5}]}}`),
	})
	if res.Error != nil {
		t.Fatalf("unexpected rpc error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); !isError {
		t.Fatalf("expected isError=true for fractional zone")
	}
}

func TestScheduleWriteZoneRejectsFloatHour(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	writer := &testScheduleWriter{
		zoneResult: &TimeProgramWriteResult{Success: true},
	}
	server.SetScheduleWriter(writer)

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolSemanticSchedulesSetZoneName + `","arguments":{"zone":0,"weekday":0,"slots":[{"start_hour":6.5,"start_minute":0,"end_hour":22,"end_minute":0,"temperature_c":21.5}]}}`),
	})
	if res.Error != nil {
		t.Fatalf("unexpected rpc error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); !isError {
		t.Fatalf("expected isError=true for fractional start_hour")
	}
}

func TestScheduleWriteDhwRejectsFloatWeekday(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	writer := &testScheduleWriter{
		dhwResult: &TimeProgramWriteResult{Success: true},
	}
	server.SetScheduleWriter(writer)

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolSemanticSchedulesSetDhwName + `","arguments":{"weekday":2.7,"slots":[{"start_hour":6,"start_minute":0,"end_hour":22,"end_minute":0}]}}`),
	})
	if res.Error != nil {
		t.Fatalf("unexpected rpc error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); !isError {
		t.Fatalf("expected isError=true for fractional weekday in DHW")
	}
}

func TestScheduleWriteZoneEmptySlots(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	writer := &testScheduleWriter{
		zoneResult: &TimeProgramWriteResult{Success: false, Error: "slots array must not be empty"},
	}
	server.SetScheduleWriter(writer)

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolSemanticSchedulesSetZoneName + `","arguments":{"zone":0,"weekday":0,"slots":[]}}`),
	})
	if res.Error != nil {
		t.Fatalf("unexpected rpc error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); !isError {
		t.Fatalf("expected isError=true for empty slots")
	}
}

func TestScheduleWriteDhwEmptySlots(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	writer := &testScheduleWriter{
		dhwResult: &TimeProgramWriteResult{Success: false, Error: "slots array must not be empty"},
	}
	server.SetScheduleWriter(writer)

	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolSemanticSchedulesSetDhwName + `","arguments":{"weekday":0,"slots":[]}}`),
	})
	if res.Error != nil {
		t.Fatalf("unexpected rpc error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T; want map", res.Result)
	}
	if isError, _ := resultMap["isError"].(bool); !isError {
		t.Fatalf("expected isError=true for empty slots in DHW")
	}
}

func TestParseTimeProgramSlotRejectsFloatMinute(t *testing.T) {
	m := map[string]any{
		"start_hour":    float64(6),
		"start_minute":  float64(30.5),
		"end_hour":      float64(22),
		"end_minute":    float64(0),
		"temperature_c": float64(21.5),
	}
	_, err := parseTimeProgramSlot(m, true)
	if err == nil {
		t.Fatal("expected error for fractional start_minute")
	}
}

func TestParseTimeProgramSlotRejectsFloatEndMinute(t *testing.T) {
	m := map[string]any{
		"start_hour":    float64(6),
		"start_minute":  float64(0),
		"end_hour":      float64(22),
		"end_minute":    float64(15.3),
		"temperature_c": float64(21.5),
	}
	_, err := parseTimeProgramSlot(m, true)
	if err == nil {
		t.Fatal("expected error for fractional end_minute")
	}
}

func TestIntFromFloat(t *testing.T) {
	tests := []struct {
		in      float64
		wantInt int
		wantOK  bool
	}{
		{0.0, 0, true},
		{1.0, 1, true},
		{6.0, 6, true},
		{22.5, 22, false},
		{1.9, 1, false},
		{-1.0, -1, true},
		{3.000000000000001, 3, false},
	}
	for _, tt := range tests {
		i, ok := intFromFloat(tt.in)
		if ok != tt.wantOK {
			t.Errorf("intFromFloat(%v): ok=%v; want %v", tt.in, ok, tt.wantOK)
		}
		if ok && i != tt.wantInt {
			t.Errorf("intFromFloat(%v): i=%d; want %d", tt.in, i, tt.wantInt)
		}
	}
}

func TestParseTimeProgramSlotTempRequired(t *testing.T) {
	m := map[string]any{
		"start_hour":   float64(6),
		"start_minute": float64(0),
		"end_hour":     float64(22),
		"end_minute":   float64(0),
	}
	_, err := parseTimeProgramSlot(m, true)
	if err == nil {
		t.Fatal("expected error for missing temperature_c with tempRequired=true")
	}
}

func TestParseTimeProgramSlotTempOptional(t *testing.T) {
	m := map[string]any{
		"start_hour":   float64(6),
		"start_minute": float64(0),
		"end_hour":     float64(22),
		"end_minute":   float64(0),
	}
	slot, err := parseTimeProgramSlot(m, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slot.TemperatureC != nil {
		t.Fatalf("expected nil temp, got %v", *slot.TemperatureC)
	}
}

func TestParseTimeProgramSlotWithTemp(t *testing.T) {
	m := map[string]any{
		"start_hour":    float64(7),
		"start_minute":  float64(30),
		"end_hour":      float64(20),
		"end_minute":    float64(15),
		"temperature_c": float64(22.5),
	}
	slot, err := parseTimeProgramSlot(m, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slot.StartHour != 7 || slot.StartMinute != 30 || slot.EndHour != 20 || slot.EndMinute != 15 {
		t.Fatalf("unexpected times: %d:%d-%d:%d", slot.StartHour, slot.StartMinute, slot.EndHour, slot.EndMinute)
	}
	if slot.TemperatureC == nil || *slot.TemperatureC != 22.5 {
		t.Fatalf("temp = %v; want 22.5", slot.TemperatureC)
	}
}
