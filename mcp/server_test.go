package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/router"
	"github.com/d3vi1/helianthus-ebusreg/schema"
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

type testSemanticProvider struct {
	zones       []Zone
	dhw         *DhwStatus
	energy      *EnergyTotals
	boiler      *BoilerStatus
	zonesDelay  time.Duration
	dhwDelay    time.Duration
	energyDelay time.Duration
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

func (p testSemanticProvider) BoilerStatus() *BoilerStatus {
	if p.boiler == nil {
		return nil
	}
	copy := *p.boiler
	return &copy
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
	if !ok || len(tools) < 15 {
		t.Fatalf("tools = %#v; want at least 15 tools", resultMap["tools"])
	}
	for _, name := range []string{
		toolRuntimeStatusGetName,
		toolSemanticZonesGetName,
		toolSemanticDHWGetName,
		toolSemanticEnergyGetName,
		toolSemanticBoilerGetName,
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
		dhw: &DhwStatus{
			Config: DhwConfig{
				OperatingMode: "AUTO",
				Preset:        "ECO",
			},
		},
		energy: &EnergyTotals{
			Gas: EnergyChannel{
				DHW:     EnergySeries{Today: 1.25, Yearly: []float64{10, 20}},
				Climate: EnergySeries{Today: 2.5, Yearly: []float64{30, 40}},
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
		if !ok || len(completed) != 5 {
			t.Fatalf("snapshot completed_planes = %#v; want 5 entries", data["completed_planes"])
		}
		planes, ok := data["planes"].(map[string]any)
		if !ok {
			t.Fatalf("snapshot planes type = %T; want map", data["planes"])
		}
		for _, key := range []string{"runtime_status", "zones", "dhw", "energy_totals", "boiler_status"} {
			if _, ok := planes[key]; !ok {
				t.Fatalf("snapshot planes missing %q", key)
			}
		}
	})

	t.Run("semantic snapshot rejects unknown plane", func(t *testing.T) {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0",
			ID:      5,
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
			ID:      6,
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
			ID:      7,
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
			ID:      8,
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
