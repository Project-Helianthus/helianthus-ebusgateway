package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
	if !ok || len(tools) < 4 {
		t.Fatalf("tools = %#v; want at least 4 tools", resultMap["tools"])
	}
	for _, name := range []string{
		toolDevicesV1Name,
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
	for _, key := range []string{"intent", "allow_dangerous", "idempotency_key"} {
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
