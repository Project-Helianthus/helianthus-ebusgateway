package portal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	estd "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
)

// fakeEbusStandardServer is a test double that implements
// PortalEbusStandardServer without requiring a real catalog.
type fakeEbusStandardServer struct {
	servicesListData  map[string]any
	commandsListFn    func(pb *uint8) (map[string]any, error)
	commandGetFn      func(id string) (map[string]any, error)
	decodeFn          func(in estd.DecodeInput) (map[string]any, error)
	lastCommandsPB    *uint8
	lastCommandGetID  string
	lastDecodeInput   estd.DecodeInput
	commandsListCalls int
}

func (f *fakeEbusStandardServer) ServicesList() map[string]any {
	if f.servicesListData != nil {
		return f.servicesListData
	}
	return map[string]any{
		"namespace":       "ebus_standard",
		"catalog_version": "v-test",
		"plan_sha256":     "deadbeef",
		"services": []map[string]any{
			{"pb": 5, "name": "identification", "description": "", "command_count": 2},
		},
	}
}

func (f *fakeEbusStandardServer) CommandsList(pb *uint8) (map[string]any, error) {
	f.commandsListCalls++
	if pb != nil {
		v := *pb
		f.lastCommandsPB = &v
	} else {
		f.lastCommandsPB = nil
	}
	if f.commandsListFn != nil {
		return f.commandsListFn(pb)
	}
	return map[string]any{
		"namespace":       "ebus_standard",
		"catalog_version": "v-test",
		"commands": []map[string]any{
			{"id": "cmd.a", "name": "a", "pb": 5, "sb": 0, "safety_class": "read_only_safe"},
		},
	}, nil
}

func (f *fakeEbusStandardServer) CommandGet(id string) (map[string]any, error) {
	f.lastCommandGetID = id
	if f.commandGetFn != nil {
		return f.commandGetFn(id)
	}
	if id == "" {
		return nil, fmt.Errorf("id: %w", estd.ErrInvalidPayload)
	}
	if id == "unknown" {
		return nil, fmt.Errorf("id=%q: %w", id, estd.ErrUnknownCommand)
	}
	return map[string]any{
		"namespace":       "ebus_standard",
		"catalog_version": "v-test",
		"command": map[string]any{
			"id":           id,
			"name":         "Test command",
			"safety_class": "frontier_experimental",
		},
	}, nil
}

func (f *fakeEbusStandardServer) Decode(in estd.DecodeInput) (map[string]any, error) {
	f.lastDecodeInput = in
	if f.decodeFn != nil {
		return f.decodeFn(in)
	}
	// Mirror the real sub-server's required-selector validation so the
	// route test for "missing direction" exercises the full stack.
	if in.Direction == "" {
		return nil, fmt.Errorf("direction: %w: required (empty is not a wildcard)", estd.ErrInvalidPayload)
	}
	if in.FrameType == "" {
		return nil, fmt.Errorf("frame_type: %w: required (empty is not a wildcard)", estd.ErrInvalidPayload)
	}
	return map[string]any{
		"namespace":       "ebus_standard",
		"catalog_version": "v-test",
		"command_id":      "cmd.decoded",
		"raw_bytes":       []int{0xAA, 0xBB},
		"fields":          []map[string]any{},
		"validity":        "catalog_identified",
	}, nil
}

func newEbusStandardHandler(sub PortalEbusStandardServer) http.Handler {
	return NewHandler(Options{
		GatewayVersion:     "test",
		BuildID:            "test",
		EbusStandardServer: sub,
	})
}

func decodeEnvelope(t *testing.T, rec *httptest.ResponseRecorder) (meta, data map[string]any, envErr map[string]any) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal body=%q: %v", rec.Body.String(), err)
	}
	meta, _ = payload["meta"].(map[string]any)
	data, _ = payload["data"].(map[string]any)
	envErr, _ = payload["error"].(map[string]any)
	return meta, data, envErr
}

func assertEnvelopeShape(t *testing.T, meta map[string]any) {
	t.Helper()
	if meta == nil {
		t.Fatalf("meta missing")
	}
	contract, _ := meta["contract"].(map[string]any)
	if contract["name"] != "helianthus-ebus-mcp" {
		t.Fatalf("meta.contract.name = %v; want helianthus-ebus-mcp", contract["name"])
	}
	cons, _ := meta["consistency"].(map[string]any)
	if cons["mode"] != "LIVE" {
		t.Fatalf("meta.consistency.mode = %v; want LIVE", cons["mode"])
	}
	if hash, ok := meta["data_hash"].(string); !ok || len(hash) != 64 {
		t.Fatalf("meta.data_hash missing or not 64-hex: %v", meta["data_hash"])
	}
}

func TestEbusStandardServicesList_Envelope(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/services", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", rec.Code, rec.Body.String())
	}
	meta, data, envErr := decodeEnvelope(t, rec)
	assertEnvelopeShape(t, meta)
	if envErr != nil {
		t.Fatalf("error = %v; want nil", envErr)
	}
	if data["namespace"] != "ebus_standard" {
		t.Fatalf("data.namespace = %v", data["namespace"])
	}
}

func TestEbusStandardCommandsList_PBFilter(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/commands?pb=5", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200; body=%s", rec.Code, rec.Body.String())
	}
	meta, _, envErr := decodeEnvelope(t, rec)
	assertEnvelopeShape(t, meta)
	if envErr != nil {
		t.Fatalf("error = %v; want nil", envErr)
	}
	if fake.lastCommandsPB == nil || *fake.lastCommandsPB != 5 {
		t.Fatalf("pb filter not forwarded: %v", fake.lastCommandsPB)
	}
}

func TestEbusStandardCommandsList_NoFilter(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/commands", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rec.Code, rec.Body.String())
	}
	if fake.lastCommandsPB != nil {
		t.Fatalf("pb should be nil when absent, got %v", *fake.lastCommandsPB)
	}
}

func TestEbusStandardCommandsList_InvalidPB(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/commands?pb=banana", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Invalid pb must NOT silently pass through as "unfiltered".
	_, _, envErr := decodeEnvelope(t, rec)
	if envErr == nil {
		t.Fatalf("expected envelope.error for malformed pb, got nil")
	}
	if envErr["code"] != "INVALID_PAYLOAD" {
		t.Fatalf("error.code = %v; want INVALID_PAYLOAD", envErr["code"])
	}
}

func TestEbusStandardCommandGet_OpenEnumPassThrough(t *testing.T) {
	fake := &fakeEbusStandardServer{
		commandGetFn: func(id string) (map[string]any, error) {
			return map[string]any{
				"namespace":       "ebus_standard",
				"catalog_version": "v-test",
				"command": map[string]any{
					"id":           id,
					"name":         "Future",
					"safety_class": "brand_new_class_xyz", // unknown enum — must pass through
				},
			}, nil
		},
	}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/command?id=cmd.future", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rec.Code, rec.Body.String())
	}
	_, data, _ := decodeEnvelope(t, rec)
	cmd, _ := data["command"].(map[string]any)
	if cmd["safety_class"] != "brand_new_class_xyz" {
		t.Fatalf("unknown safety_class was not passed through: %v", cmd["safety_class"])
	}
	if fake.lastCommandGetID != "cmd.future" {
		t.Fatalf("id not forwarded: %v", fake.lastCommandGetID)
	}
}

func TestEbusStandardCommandGet_UnknownCommand(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/command?id=unknown", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr == nil || envErr["code"] != "UNKNOWN_COMMAND" {
		t.Fatalf("expected UNKNOWN_COMMAND envelope error, got %v", envErr)
	}
}

func TestEbusStandardCommandGet_MissingID(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/command", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr == nil || envErr["code"] != "INVALID_PAYLOAD" {
		t.Fatalf("expected INVALID_PAYLOAD for missing id, got %v", envErr)
	}
}

func TestEbusStandardDecode_Happy(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	url := "/api/v1/ebus-standard/decode?pb=5&sb=4&direction=master_to_slave&frame_type=MM&payload_hex=aabb"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rec.Code, rec.Body.String())
	}
	meta, data, envErr := decodeEnvelope(t, rec)
	assertEnvelopeShape(t, meta)
	if envErr != nil {
		t.Fatalf("error = %v; want nil", envErr)
	}
	if data["command_id"] != "cmd.decoded" {
		t.Fatalf("data.command_id = %v", data["command_id"])
	}
	in := fake.lastDecodeInput
	if in.PB != 5 || in.SB != 4 || in.Direction != "master_to_slave" || in.FrameType != "MM" || in.PayloadHex != "aabb" {
		t.Fatalf("decode input not forwarded: %+v", in)
	}
}

func TestEbusStandardDecode_MissingDirection(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	url := "/api/v1/ebus-standard/decode?pb=5&sb=4&frame_type=MM&payload_hex=aa"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr == nil || envErr["code"] != "INVALID_PAYLOAD" {
		t.Fatalf("expected INVALID_PAYLOAD for missing direction, got %v", envErr)
	}
}

func TestEbusStandardDecode_MissingPayloadHex_ReturnsInvalidPayload(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	// payload_hex key entirely omitted from query string.
	url := "/api/v1/ebus-standard/decode?pb=5&sb=4&direction=master_to_slave&frame_type=MM"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr == nil || envErr["code"] != "INVALID_PAYLOAD" {
		t.Fatalf("expected INVALID_PAYLOAD for missing payload_hex, got %v", envErr)
	}
	msg, _ := envErr["message"].(string)
	if !strings.Contains(msg, "payload_hex") {
		t.Fatalf("error.message should mention payload_hex, got %q", msg)
	}
	// Sub-server must NOT be invoked when the handler rejected up-front:
	// the fake's Decode would otherwise record lastDecodeInput.
	var zero estd.DecodeInput
	if fake.lastDecodeInput != zero {
		t.Fatalf("sub-server Decode must not be invoked for missing payload_hex, got %+v", fake.lastDecodeInput)
	}
}

func TestEbusStandardDecode_ExplicitEmptyPayloadHex_PassesThroughToBackend(t *testing.T) {
	// Backend Decode contract accepts empty payloads (hex.DecodeString("")
	// returns nil). Explicit empty payload_hex= must reach the backend so
	// the catalog-lookup semantics apply unchanged.
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	url := "/api/v1/ebus-standard/decode?pb=5&sb=4&direction=master_to_slave&frame_type=MM&payload_hex="
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rec.Code, rec.Body.String())
	}
	_, _, envErr := decodeEnvelope(t, rec)
	if envErr != nil {
		t.Fatalf("explicit empty payload_hex should pass through, got envelope error: %v", envErr)
	}
	in := fake.lastDecodeInput
	if in.PayloadHex != "" {
		t.Fatalf("explicit-empty payload_hex must be forwarded as \"\", got %q", in.PayloadHex)
	}
	if in.PB != 5 || in.SB != 4 || in.Direction != "master_to_slave" || in.FrameType != "MM" {
		t.Fatalf("selectors not forwarded: %+v", in)
	}
}

func TestEbusStandardNilSubServer_404(t *testing.T) {
	h := NewHandler(Options{GatewayVersion: "test", BuildID: "test"})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/services", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d; want 404 when sub-server absent; body=%s", rec.Code, rec.Body.String())
	}
}

func TestEbusStandardMethodNotAllowed(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/ebus-standard/services", strings.NewReader(""))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d; want 405", rec.Code)
	}
}

// TestCapabilities_EbusStandardFlagReflectsOptions verifies that the
// /api/v1/bootstrap `capabilities.ebus_standard` flag is true iff
// Options.EbusStandardServer is non-nil. The frontend gates the L7
// nav button and section auto-activation on this flag — see the
// matching applyCapabilityState / activateSection wiring in app.js
// and the l7-catalog.test.mjs regression tests. Codex P2 on PR #507.
func TestCapabilities_EbusStandardFlagReflectsOptions(t *testing.T) {
	cases := []struct {
		name   string
		server PortalEbusStandardServer
		want   bool
	}{
		{name: "nil sub-server → flag false", server: nil, want: false},
		{name: "non-nil sub-server → flag true", server: &fakeEbusStandardServer{}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(Options{EbusStandardServer: tc.server})
			req := httptest.NewRequest(http.MethodGet, "/api/v1/bootstrap", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d; want 200", rec.Code)
			}
			var payload map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			caps, ok := payload["capabilities"].(map[string]any)
			if !ok {
				t.Fatalf("bootstrap missing capabilities map; got: %v", payload)
			}
			got, present := caps["ebus_standard"]
			if !present {
				t.Fatalf("capabilities.ebus_standard missing; got keys: %v", caps)
			}
			if got != tc.want {
				t.Fatalf("capabilities.ebus_standard=%v; want %v", got, tc.want)
			}
		})
	}
}
