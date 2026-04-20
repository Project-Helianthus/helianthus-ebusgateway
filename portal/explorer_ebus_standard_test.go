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

// TestEbusStandardCommandsList_EmptyPB asserts that an explicit empty pb
// (?pb=) is rejected with INVALID_PAYLOAD rather than silently mapped to
// the unfiltered path. Mirrors the payload_hex fix (e363e1b7) — presence
// check distinguishes missing from explicitly-empty. Without this guard a
// malformed client request (e.g. JS building `?pb=${val}` with val="") would
// silently return the full unfiltered catalog, hiding the bug.
func TestEbusStandardCommandsList_EmptyPB(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/commands?pb=", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr == nil {
		t.Fatalf("expected envelope.error for empty pb=, got nil")
	}
	if envErr["code"] != "INVALID_PAYLOAD" {
		t.Fatalf("error.code = %v; want INVALID_PAYLOAD", envErr["code"])
	}
	// Sub-server CommandsList MUST NOT be invoked on malformed input.
	if fake.lastCommandsPB != nil {
		t.Fatalf("CommandsList should not be invoked on empty pb=, got pb=%v",
			*fake.lastCommandsPB)
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

// TestEbusStandardCommandsList_PBLeadingZero_DecimalNoOctal guards against
// Go's base-0 octal auto-detection for the pb filter. With explicit base=10,
// "010" parses as decimal 10 (not octal 8, not hex 0x10). The MCP tool
// schema documents pb as integer [0,255] decimal; round 9 restored the
// decimal contract. Codex P2 on PR #507 (comment #3109782014).
func TestEbusStandardCommandsList_PBLeadingZero_DecimalNoOctal(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/commands?pb=010", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr != nil {
		t.Fatalf("pb=010 must parse as decimal 10, got envelope error: %v", envErr)
	}
	if fake.lastCommandsPB == nil {
		t.Fatalf("pb filter not forwarded (nil)")
	}
	if *fake.lastCommandsPB != 10 {
		t.Fatalf("pb=010 must parse as decimal 10, got %d", *fake.lastCommandsPB)
	}
}

// TestEbusStandardCommandsList_PB_DecimalDefault asserts the MCP-schema-
// aligned decimal contract: pb=10 → 10, NOT 0x10=16 (round 8 regression).
func TestEbusStandardCommandsList_PB_DecimalDefault(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/commands?pb=10", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr != nil {
		t.Fatalf("pb=10 must parse as decimal 10, got envelope error: %v", envErr)
	}
	if fake.lastCommandsPB == nil || *fake.lastCommandsPB != 10 {
		t.Fatalf("pb=10 must parse to decimal 10, got %v", fake.lastCommandsPB)
	}
}

// TestEbusStandardCommandsList_PB_HexWith0xPrefix asserts the hex escape
// hatch: pb=0x10 → 16.
func TestEbusStandardCommandsList_PB_HexWith0xPrefix(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/commands?pb=0x10", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr != nil {
		t.Fatalf("pb=0x10 must parse as hex 16, got envelope error: %v", envErr)
	}
	if fake.lastCommandsPB == nil || *fake.lastCommandsPB != 0x10 {
		t.Fatalf("pb=0x10 must parse to 16, got %v", fake.lastCommandsPB)
	}
}

// TestEbusStandardCommandsList_PB_BareHexRejected asserts that bare hex
// inputs (no 0x prefix) are rejected — operators must type 0xff for hex.
// This matches the MCP schema's decimal-integer contract.
func TestEbusStandardCommandsList_PB_BareHexRejected(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/commands?pb=ff", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr == nil || envErr["code"] != "INVALID_PAYLOAD" {
		t.Fatalf("expected INVALID_PAYLOAD for bare-hex pb=ff, got %v", envErr)
	}
	if fake.lastCommandsPB != nil {
		t.Fatalf("CommandsList must not be invoked on bare-hex input, got pb=%v",
			*fake.lastCommandsPB)
	}
}

// TestEbusStandardCommandsList_PB08_AcceptedAsDecimal asserts pb=08 parses
// as decimal 8 (no Go octal rejection under explicit base=10).
func TestEbusStandardCommandsList_PB08_AcceptedAsDecimal(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/commands?pb=08", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr != nil {
		t.Fatalf("pb=08 must parse as decimal 8, got envelope error: %v", envErr)
	}
	if fake.lastCommandsPB == nil || *fake.lastCommandsPB != 8 {
		t.Fatalf("pb=08 must parse to decimal 8, got %v", fake.lastCommandsPB)
	}
}

// TestEbusStandardCommandsList_PB0xPrefix verifies the "0x" prefix is
// stripped (case-insensitive) before hex parsing.
func TestEbusStandardCommandsList_PB0xPrefix(t *testing.T) {
	for _, raw := range []string{"0x10", "0X10", "0xff", "0XFF"} {
		t.Run(raw, func(t *testing.T) {
			fake := &fakeEbusStandardServer{}
			h := newEbusStandardHandler(fake)

			req := httptest.NewRequest(http.MethodGet,
				"/api/v1/ebus-standard/commands?pb="+raw, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			_, _, envErr := decodeEnvelope(t, rec)
			if envErr != nil {
				t.Fatalf("pb=%s must parse, got envelope error: %v", raw, envErr)
			}
			if fake.lastCommandsPB == nil {
				t.Fatalf("pb=%s not forwarded", raw)
			}
		})
	}
}

// TestEbusStandardCommandsList_PBOverflow_Rejected ensures values exceeding
// 1 byte are rejected (bitSize=8 semantics). Under the decimal contract,
// pb=256 overflows uint8 and MUST yield INVALID_PAYLOAD. (pb=100 is now
// valid decimal 100, so the overflow boundary moves to 256.)
func TestEbusStandardCommandsList_PBOverflow_Rejected(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/commands?pb=256", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr == nil || envErr["code"] != "INVALID_PAYLOAD" {
		t.Fatalf("expected INVALID_PAYLOAD for pb=256 (decimal overflows 1 byte), got %v", envErr)
	}
	if fake.lastCommandsPB != nil {
		t.Fatalf("CommandsList must not be invoked on overflow, got pb=%v",
			*fake.lastCommandsPB)
	}
}

// TestEbusStandardCommandsList_PBHexOverflow_Rejected: explicit 0x-prefix
// hex that exceeds 1 byte (0x100=256) must also yield INVALID_PAYLOAD.
func TestEbusStandardCommandsList_PBHexOverflow_Rejected(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ebus-standard/commands?pb=0x100", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr == nil || envErr["code"] != "INVALID_PAYLOAD" {
		t.Fatalf("expected INVALID_PAYLOAD for pb=0x100 overflow, got %v", envErr)
	}
}

// TestEbusStandardDecode_PBSBLeadingZero_DecimalNoOctal guards the decode
// route's pb/sb parsing against Go base-0 octal auto-detection.
func TestEbusStandardDecode_PBSBLeadingZero_DecimalNoOctal(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	url := "/api/v1/ebus-standard/decode?pb=010&sb=08&direction=master_to_slave&frame_type=MM&payload_hex=aa"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr != nil {
		t.Fatalf("pb=010&sb=08 must parse as decimal, got envelope error: %v", envErr)
	}
	in := fake.lastDecodeInput
	if in.PB != 10 {
		t.Fatalf("pb=010 must parse as decimal 10, got %d", in.PB)
	}
	if in.SB != 8 {
		t.Fatalf("sb=08 must parse as decimal 8, got %d", in.SB)
	}
}

// TestEbusStandardDecode_PBSBMixedHexDecimal exercises the smart parser:
// pb=0x10 (hex) + sb=4 (decimal) both accepted and forwarded correctly.
func TestEbusStandardDecode_PBSBMixedHexDecimal(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	url := "/api/v1/ebus-standard/decode?pb=0x10&sb=4&direction=master_to_slave&frame_type=MM&payload_hex=aa"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr != nil {
		t.Fatalf("pb=0x10&sb=4 must parse, got envelope error: %v", envErr)
	}
	in := fake.lastDecodeInput
	if in.PB != 0x10 {
		t.Fatalf("pb=0x10 must parse as hex 16, got %d", in.PB)
	}
	if in.SB != 4 {
		t.Fatalf("sb=4 must parse as decimal 4, got %d", in.SB)
	}
}

// TestEbusStandardDecode_SBBanana_Rejected ensures invalid SB values are
// rejected with INVALID_PAYLOAD — regression guard that the helper's parse
// error bubbles up through the decode route's selector validation.
func TestEbusStandardDecode_SBBanana_Rejected(t *testing.T) {
	fake := &fakeEbusStandardServer{}
	h := newEbusStandardHandler(fake)

	url := "/api/v1/ebus-standard/decode?pb=5&sb=banana&direction=master_to_slave&frame_type=MM&payload_hex=aa"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	_, _, envErr := decodeEnvelope(t, rec)
	if envErr == nil || envErr["code"] != "INVALID_PAYLOAD" {
		t.Fatalf("expected INVALID_PAYLOAD for sb=banana, got %v", envErr)
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
