package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
	if !ok || len(tools) < 2 {
		t.Fatalf("tools = %#v; want at least 2 tools", resultMap["tools"])
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
		Params:  json.RawMessage(`{"name":"ebus.devices","arguments":{}}`),
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

	res = doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.invoke","arguments":{"address":8,"plane":"heating","method":"get_status","params":{}}}`),
	})
	if res.Error != nil {
		t.Fatalf("tools/call invoke error = %+v", res.Error)
	}
	if len(invoker.calls) != 1 || invoker.calls[0].plane != "heating" || invoker.calls[0].method != "get_status" {
		t.Fatalf("invoker calls = %+v; want heating/get_status", invoker.calls)
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
