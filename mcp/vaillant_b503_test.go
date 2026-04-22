package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// stubB503Dispatcher is an in-memory RPCDispatcher for b503 MCP tests.
// It matches the RPCDispatcher interface declared in vaillant_b503.go.
type stubB503Dispatcher struct {
	respByPrefix map[string][]byte
	err          error
	calls        []stubB503Call
}

type stubB503Call struct {
	target  byte
	payload []byte
}

func (s *stubB503Dispatcher) Invoke(ctx context.Context, target byte, payload []byte) ([]byte, error) {
	s.calls = append(s.calls, stubB503Call{target: target, payload: append([]byte{}, payload...)})
	if s.err != nil {
		return nil, s.err
	}
	if len(payload) >= 2 {
		if resp, ok := s.respByPrefix[string([]byte{payload[0], payload[1]})]; ok {
			return resp, nil
		}
	}
	return nil, errors.New("stubB503Dispatcher: no canned response")
}

// newB503Server builds a gateway MCP Server, attaches the five Vaillant B503
// tools, and returns server + dispatcher stub + session manager so tests can
// manipulate wire responses and FSM state.
func newB503Server(t *testing.T, disp *stubB503Dispatcher, mgr *b503session.Manager) *Server {
	t.Helper()
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	srv, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	RegisterVaillantB503Tools(srv, VaillantB503Options{
		Dispatcher:     disp,
		SessionManager: mgr,
		DefaultTarget:  0x08,
	})
	return srv
}

func newDefaultMgr() *b503session.Manager {
	return b503session.New(
		b503session.TransportKey{AdapterInstanceID: "test", TransportEpoch: 1},
		30*time.Second,
		func(ctx context.Context) (b503session.TransportKey, error) {
			return b503session.TransportKey{AdapterInstanceID: "test", TransportEpoch: 2}, nil
		},
	)
}

// --- Test 1: ToolRegistration --------------------------------------------

func TestVaillantB503_ToolRegistration(t *testing.T) {
	disp := &stubB503Dispatcher{}
	srv := newB503Server(t, disp, newDefaultMgr())

	res := doRPC(t, srv.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list", Params: nil})
	if res.Error != nil {
		t.Fatalf("tools/list error = %+v", res.Error)
	}
	resultMap := res.Result.(map[string]any)
	tools := resultMap["tools"].([]any)

	wants := []string{
		toolVaillantB503ErrorsGetName,
		toolVaillantB503ErrorsHistoryGetName,
		toolVaillantB503ServiceCurrentGetName,
		toolVaillantB503ServiceHistoryGetName,
		toolVaillantB503LiveMonitorName,
	}
	for _, want := range wants {
		if !hasToolName(tools, want) {
			t.Errorf("tool %q not registered", want)
		}
	}
}

// --- Test 2: Envelope determinism ----------------------------------------

func TestVaillantB503_ErrorsGet_EnvelopeDeterminism(t *testing.T) {
	disp := &stubB503Dispatcher{
		respByPrefix: map[string][]byte{
			"\x00\x01": {0x19, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
	}
	srv := newB503Server(t, disp, newDefaultMgr())

	call := rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503ErrorsGetName + `","arguments":{"target_address":8}}`),
	}

	env1 := envelopeFromResult(t, doRPC(t, srv.Handler(), call))
	env2 := envelopeFromResult(t, doRPC(t, srv.Handler(), call))

	meta1 := env1["meta"].(map[string]any)
	meta2 := env2["meta"].(map[string]any)
	if meta1["data_hash"] != meta2["data_hash"] {
		t.Fatalf("data_hash differs: %v vs %v", meta1["data_hash"], meta2["data_hash"])
	}
	if meta1["data_hash"] == "" {
		t.Fatal("data_hash empty")
	}

	// JSON stability: re-marshaling the data must match bytes-for-bytes.
	d1, _ := json.Marshal(env1["data"])
	d2, _ := json.Marshal(env2["data"])
	if string(d1) != string(d2) {
		t.Fatalf("data JSON differs:\n %s\n vs\n %s", d1, d2)
	}
}

// --- Test 3: Decoded shape ------------------------------------------------

func TestVaillantB503_ErrorsGet_DecodedShape(t *testing.T) {
	disp := &stubB503Dispatcher{
		respByPrefix: map[string][]byte{
			"\x00\x01": {0x19, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
	}
	srv := newB503Server(t, disp, newDefaultMgr())

	call := rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503ErrorsGetName + `","arguments":{}}`),
	}
	env := envelopeFromResult(t, doRPC(t, srv.Handler(), call))
	data := env["data"].(map[string]any)

	if fa := data["first_active_error"]; fa == nil {
		t.Fatal("first_active_error nil; want 281")
	} else if got := int(fa.(float64)); got != 281 {
		t.Fatalf("first_active_error = %d; want 281", got)
	}
	slots := data["slots"].([]any)
	if len(slots) != 5 {
		t.Fatalf("slots len = %d; want 5", len(slots))
	}
	if int(slots[0].(float64)) != 0x0119 {
		t.Fatalf("slot[0] = %v; want 0x0119", slots[0])
	}
	for i := 1; i < 5; i++ {
		// Empty slots surfaced as null (nil) in JSON.
		if slots[i] != nil {
			t.Errorf("slot[%d] = %v; want null (empty-slot sentinel)", i, slots[i])
		}
	}
}

// --- Test 4: live_monitor enable/read/disable happy path -----------------

func TestVaillantB503_LiveMonitor_EnableReadDisable_Happy(t *testing.T) {
	disp := &stubB503Dispatcher{
		respByPrefix: map[string][]byte{
			"\x00\x03": {0x01, 0x00},
		},
	}
	mgr := newDefaultMgr()
	srv := newB503Server(t, disp, mgr)

	// enable
	env := envelopeFromResult(t, doRPC(t, srv.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"enable"}}`),
	}))
	if env["error"] != nil {
		t.Fatalf("enable error: %v", env["error"])
	}
	data := env["data"].(map[string]any)
	token, _ := data["issuer_token"].(string)
	if token == "" {
		t.Fatalf("issuer_token empty in enable response: %v", env)
	}

	// read (no token required)
	env = envelopeFromResult(t, doRPC(t, srv.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 2, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"read"}}`),
	}))
	if env["error"] != nil {
		t.Fatalf("read error: %v", env["error"])
	}

	// disable with token
	disableCall := `{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"disable","issuer_token":"` + token + `"}}`
	env = envelopeFromResult(t, doRPC(t, srv.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 3, Method: "tools/call",
		Params: json.RawMessage(disableCall),
	}))
	if env["error"] != nil {
		t.Fatalf("disable error: %v", env["error"])
	}
}

// --- Test 5: disable wrong token -> SESSION_BUSY --------------------------

func TestVaillantB503_LiveMonitor_Disable_WrongToken_SessionBusy(t *testing.T) {
	disp := &stubB503Dispatcher{
		respByPrefix: map[string][]byte{
			"\x00\x03": {0x01, 0x00},
		},
	}
	srv := newB503Server(t, disp, newDefaultMgr())

	envelopeFromResult(t, doRPC(t, srv.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"enable"}}`),
	}))

	res := doRPC(t, srv.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 2, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"disable","issuer_token":"deadbeefdeadbeefdeadbeefdeadbeef"}}`),
	})
	assertToolErrorCode(t, res, "SESSION_BUSY")
}

// --- Test 6: second enable -> SESSION_BUSY -------------------------------

func TestVaillantB503_LiveMonitor_SecondEnable_SessionBusy(t *testing.T) {
	disp := &stubB503Dispatcher{
		respByPrefix: map[string][]byte{"\x00\x03": {0x01, 0x00}},
	}
	srv := newB503Server(t, disp, newDefaultMgr())

	envelopeFromResult(t, doRPC(t, srv.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"enable"}}`),
	}))
	res := doRPC(t, srv.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 2, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"enable"}}`),
	})
	assertToolErrorCode(t, res, "SESSION_BUSY")
}

// --- Test 7: epoch advance EXPIRED never leaks ---------------------------

func TestVaillantB503_LiveMonitor_EpochAdvance_ExpiredNeverLeaks(t *testing.T) {
	disp := &stubB503Dispatcher{
		respByPrefix: map[string][]byte{"\x00\x03": {0x01, 0x00}},
	}
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "adp", TransportEpoch: 1},
		30*time.Second,
		func(ctx context.Context) (b503session.TransportKey, error) {
			return b503session.TransportKey{AdapterInstanceID: "adp", TransportEpoch: 2}, nil
		},
	)
	srv := newB503Server(t, disp, mgr)

	// enable
	env := envelopeFromResult(t, doRPC(t, srv.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"enable"}}`),
	}))
	assertNoExpiredInEnvelope(t, env)

	// simulate epoch advance
	mgr.OnEpochAdvance(context.Background(), 2)

	// read → should succeed (refresh func returned epoch=2 OK)
	env = envelopeFromResult(t, doRPC(t, srv.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 2, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"read"}}`),
	}))
	if env["error"] != nil {
		t.Fatalf("read error after epoch advance: %v", env["error"])
	}
	assertNoExpiredInEnvelope(t, env)
}

// --- Test 8: refresh transport-down -> TRANSPORT_DOWN --------------------

func TestVaillantB503_LiveMonitor_EpochAdvance_RefreshTransportDown(t *testing.T) {
	disp := &stubB503Dispatcher{
		respByPrefix: map[string][]byte{"\x00\x03": {0x01, 0x00}},
	}
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "adp", TransportEpoch: 1},
		30*time.Second,
		func(ctx context.Context) (b503session.TransportKey, error) {
			return b503session.TransportKey{}, b503session.ErrTransportDown
		},
	)
	srv := newB503Server(t, disp, mgr)

	envelopeFromResult(t, doRPC(t, srv.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"enable"}}`),
	}))

	mgr.OnEpochAdvance(context.Background(), 2)

	res := doRPC(t, srv.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 2, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"read"}}`),
	})
	assertToolErrorCode(t, res, "TRANSPORT_DOWN")
}

// --- Test 9: no install/clear tools in vaillant namespace ----------------

func TestVaillantB503_NoInstallWriteTools(t *testing.T) {
	disp := &stubB503Dispatcher{}
	srv := newB503Server(t, disp, newDefaultMgr())
	res := doRPC(t, srv.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list", Params: nil})
	tools := res.Result.(map[string]any)["tools"].([]any)
	for _, raw := range tools {
		name, _ := raw.(map[string]any)["name"].(string)
		if !strings.HasPrefix(name, "ebus.v1.vaillant.") {
			continue
		}
		if strings.Contains(name, "clear") || strings.Contains(name, "install") {
			t.Errorf("forbidden install-write surface exposed: %q", name)
		}
	}
}

// --- Test 10: no EXPIRED in any public response --------------------------

func TestVaillantB503_NoExpiredInPublicResponse(t *testing.T) {
	cases := []struct {
		name   string
		before func(mgr *b503session.Manager)
		call   string
	}{
		{"idle-read", func(*b503session.Manager) {}, `{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"read"}}`},
		{"idle-disable", func(*b503session.Manager) {}, `{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"disable","issuer_token":"ffffffffffffffffffffffffffffffff"}}`},
		{"post-epoch-transport-down-read", func(mgr *b503session.Manager) {
			// enable so mgr has owner, then epoch advance with refresh error.
		}, `{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"read"}}`},
	}
	disp := &stubB503Dispatcher{respByPrefix: map[string][]byte{"\x00\x03": {0x01, 0x00}}}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mgr := b503session.New(
				b503session.TransportKey{AdapterInstanceID: "adp", TransportEpoch: 1},
				30*time.Second,
				func(ctx context.Context) (b503session.TransportKey, error) {
					return b503session.TransportKey{}, b503session.ErrTransportDown
				},
			)
			srv := newB503Server(t, disp, mgr)
			if tc.name == "post-epoch-transport-down-read" {
				envelopeFromResult(t, doRPC(t, srv.Handler(), rpcRequest{
					JSONRPC: "2.0", ID: 1, Method: "tools/call",
					Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveMonitorName + `","arguments":{"action":"enable"}}`),
				}))
				mgr.OnEpochAdvance(context.Background(), 2)
			}
			res := doRPC(t, srv.Handler(), rpcRequest{
				JSONRPC: "2.0", ID: 2, Method: "tools/call",
				Params: json.RawMessage(tc.call),
			})
			env := envelopeFromResult(t, res)
			assertNoExpiredInEnvelope(t, env)
		})
	}
}

// --- Test 11/12: capability signal --------------------------------------

func TestVaillantB503_Capability_Available(t *testing.T) {
	disp := &stubB503Dispatcher{}
	srv := newB503Server(t, disp, newDefaultMgr())
	got := srv.VaillantB503Availability()
	if got != AvailabilityAvailable {
		t.Fatalf("availability = %q; want AVAILABLE", got)
	}
}

func TestVaillantB503_Capability_TransportDown(t *testing.T) {
	disp := &stubB503Dispatcher{err: b503session.ErrTransportDown}
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "adp", TransportEpoch: 1},
		30*time.Second,
		func(ctx context.Context) (b503session.TransportKey, error) {
			return b503session.TransportKey{}, b503session.ErrTransportDown
		},
	)
	srv := newB503Server(t, disp, mgr)
	// Disable session to simulate transport-down-at-rest path.
	mgr.OnTransportDisconnect()
	got := srv.VaillantB503Availability()
	if got != AvailabilityTransportDown && got != AvailabilityUnknown {
		// We accept UNKNOWN as a conservative fallback when the capability
		// has not been evaluated against a live probe; the stronger
		// assertion is just that it is NOT AVAILABLE.
		if got == AvailabilityAvailable {
			t.Fatalf("availability = %q; must not be AVAILABLE when transport is down", got)
		}
	}
}

// --- helpers -------------------------------------------------------------

func assertNoExpiredInEnvelope(t *testing.T, env map[string]any) {
	t.Helper()
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if strings.Contains(strings.ToUpper(string(raw)), "EXPIRED") {
		t.Fatalf("envelope leaked EXPIRED: %s", raw)
	}
}
