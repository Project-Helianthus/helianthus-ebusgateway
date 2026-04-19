package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	estd "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func TestRegisterEbusStandardTools_AddsFourSurfaces(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	want := map[string]bool{
		estd.ToolServicesList: false,
		estd.ToolCommandsList: false,
		estd.ToolCommandGet:   false,
		estd.ToolDecode:       false,
	}
	for _, tool := range s.tools {
		if _, ok := want[tool.Name]; ok {
			want[tool.Name] = true
		}
	}
	for name, ok := range want {
		if !ok {
			t.Fatalf("tool %q not registered", name)
		}
	}
}

func TestHandleEbusStandardCall_ServicesList_EnvelopeShape(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	result, handled := s.handleEbusStandardCall(estd.ToolServicesList, map[string]any{})
	if !handled {
		t.Fatal("services.list must be handled")
	}
	content := result["content"].([]map[string]any)
	text := content[0]["text"].(string)
	var env map[string]any
	if err := json.Unmarshal([]byte(text), &env); err != nil {
		t.Fatalf("envelope is not valid JSON: %v", err)
	}
	if _, ok := env["meta"]; !ok {
		t.Fatal("envelope missing meta")
	}
	if _, ok := env["data"]; !ok {
		t.Fatal("envelope missing data")
	}
	if _, ok := env["error"]; !ok {
		t.Fatal("envelope missing error")
	}
	meta := env["meta"].(map[string]any)
	if h, ok := meta["data_hash"].(string); !ok || len(h) != 64 {
		t.Fatalf("meta.data_hash malformed: %v", meta["data_hash"])
	}
}

func TestHandleEbusStandardCall_UnknownNotHandled(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	_, handled := s.handleEbusStandardCall("ebus.v1.unknown", nil)
	if handled {
		t.Fatal("unknown tool must not be claimed by ebus_standard dispatcher")
	}
}

func TestHandleEbusStandardCall_CommandsListFiltersByPB(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	result, handled := s.handleEbusStandardCall(estd.ToolCommandsList, map[string]any{"pb": 3})
	if !handled {
		t.Fatal("commands.list must be handled")
	}
	text := result["content"].([]map[string]any)[0]["text"].(string)
	if !strings.Contains(text, `"pb":3`) {
		t.Fatalf("expected pb:3 in commands envelope, got %s", text)
	}
}

// TestHandleEbusStandardCall_CommandsListRejectsInvalidPB pins the
// contract that a malformed `pb` argument becomes an INVALID_PAYLOAD
// error rather than being silently dropped (which would return the
// unfiltered catalog as a success). Regression for PR #505 r3106745676.
func TestHandleEbusStandardCall_CommandsListRejectsInvalidPB(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	result, handled := s.handleEbusStandardCall(estd.ToolCommandsList, map[string]any{"pb": "not-a-number"})
	if !handled {
		t.Fatal("commands.list must be handled")
	}
	if isErr, _ := result["isError"].(bool); !isErr {
		t.Fatalf("expected isError=true on malformed pb, got result=%+v", result)
	}
	text := result["content"].([]map[string]any)[0]["text"].(string)
	var env map[string]any
	if err := json.Unmarshal([]byte(text), &env); err != nil {
		t.Fatalf("envelope is not valid JSON: %v", err)
	}
	errObj, ok := env["error"].(map[string]any)
	if !ok || errObj == nil {
		t.Fatalf("envelope.error missing on malformed pb: %s", text)
	}
	if code, _ := errObj["code"].(string); code != "INVALID_PAYLOAD" {
		t.Fatalf("error.code = %q, want INVALID_PAYLOAD", code)
	}
}

// TestHandleEbusStandardCall_CommandsListAbsentPBUnfiltered pins that
// omitting `pb` still yields an unfiltered success (preserving prior
// behavior for well-formed input). Regression for PR #505 r3106745676.
func TestHandleEbusStandardCall_CommandsListAbsentPBUnfiltered(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	result, handled := s.handleEbusStandardCall(estd.ToolCommandsList, map[string]any{})
	if !handled {
		t.Fatal("commands.list must be handled")
	}
	if isErr, _ := result["isError"].(bool); isErr {
		t.Fatalf("unfiltered commands.list must not be an error, got result=%+v", result)
	}
}

// TestHandleEbusStandardCall_EnvelopeContainsConsistencyMeta pins that
// ebus_standard envelopes carry meta.consistency.mode alongside the
// rest of ebus.v1.* surfaces so clients reading meta.consistency.mode
// don't break on the four ebus_standard tools. Regression for PR #505
// r3106745674.
func TestHandleEbusStandardCall_EnvelopeContainsConsistencyMeta(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	result, handled := s.handleEbusStandardCall(estd.ToolServicesList, map[string]any{})
	if !handled {
		t.Fatal("services.list must be handled")
	}
	text := result["content"].([]map[string]any)[0]["text"].(string)
	var env map[string]any
	if err := json.Unmarshal([]byte(text), &env); err != nil {
		t.Fatalf("envelope JSON: %v", err)
	}
	meta, _ := env["meta"].(map[string]any)
	consistency, ok := meta["consistency"].(map[string]any)
	if !ok || consistency == nil {
		t.Fatalf("meta.consistency missing: %s", text)
	}
	if mode, _ := consistency["mode"].(string); mode != "LIVE" {
		t.Fatalf("meta.consistency.mode = %q, want LIVE", mode)
	}
}

// TestNewServer_WiresEbusStandardSurfaces pins the bootstrap contract:
// NewServer must register the four ebus.v1.ebus_standard.* tools so
// handleToolsCall dispatches them instead of returning "unknown tool".
// Regression for PR #505 comment id=3106729472.
func TestNewServer_WiresEbusStandardSurfaces(t *testing.T) {
	reg := &testRegistry{entries: map[byte]registry.DeviceEntry{}}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	required := []string{
		estd.ToolServicesList,
		estd.ToolCommandsList,
		estd.ToolCommandGet,
		estd.ToolDecode,
	}
	for _, name := range required {
		if !server.hasToolNamed(name) {
			t.Fatalf("tool %q not registered on NewServer — handleToolsCall will reject as unknown", name)
		}
	}
	if server.ebusStandardServer == nil {
		t.Fatal("ebusStandardServer sub-dispatcher not installed by NewServer")
	}
}

// TestHandleEbusStandardCall_DecodeRejectsMissingSelectors pins the
// contract that the decode surface REQUIRES direction + frame_type
// alongside pb/sb/payload_hex. Without them, a catalog with multiple
// commands on the same (pb, sb) would silently return the wrong
// command. Regression for PR #505 r3106756020.
func TestHandleEbusStandardCall_DecodeRejectsMissingSelectors(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	// direction + frame_type absent
	result, handled := s.handleEbusStandardCall(estd.ToolDecode, map[string]any{
		"pb":          3,
		"sb":          4,
		"payload_hex": "0102",
	})
	if !handled {
		t.Fatal("decode must be handled")
	}
	if isErr, _ := result["isError"].(bool); !isErr {
		t.Fatalf("expected isError=true when direction/frame_type missing, got result=%+v", result)
	}
	text := result["content"].([]map[string]any)[0]["text"].(string)
	var env map[string]any
	if err := json.Unmarshal([]byte(text), &env); err != nil {
		t.Fatalf("envelope JSON: %v", err)
	}
	errObj, ok := env["error"].(map[string]any)
	if !ok || errObj == nil {
		t.Fatalf("envelope.error missing: %s", text)
	}
	if code, _ := errObj["code"].(string); code != "INVALID_PAYLOAD" {
		t.Fatalf("error.code = %q, want INVALID_PAYLOAD", code)
	}
}

// TestHandleEbusStandardCall_DecodeAcceptsFullSelectors pins the happy
// path: a fully-qualified decode (pb, sb, direction, frame_type,
// payload_hex) still succeeds after the tightening above.
func TestHandleEbusStandardCall_DecodeAcceptsFullSelectors(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	result, handled := s.handleEbusStandardCall(estd.ToolDecode, map[string]any{
		"pb":          3,
		"sb":          4,
		"direction":   "request",
		"frame_type":  "addressed",
		"payload_hex": "0102",
	})
	if !handled {
		t.Fatal("decode must be handled")
	}
	if isErr, _ := result["isError"].(bool); isErr {
		text := result["content"].([]map[string]any)[0]["text"].(string)
		t.Fatalf("decode with full selectors must not error, got: %s", text)
	}
}

// TestToUint8_RejectsFractionalFloat64 pins the contract that a JSON
// float value like 3.9 is rejected as INVALID_PAYLOAD rather than
// silently truncated to 3. Parallel to the toByteSource.float64 fix
// in round-2. Regression for PR #505 r3106756021.
func TestToUint8_RejectsFractionalFloat64(t *testing.T) {
	if _, ok := toUint8(float64(3.9)); ok {
		t.Fatal("toUint8(3.9) must be rejected (fractional)")
	}
	if _, ok := toUint8(float64(3.0001)); ok {
		t.Fatal("toUint8(3.0001) must be rejected (fractional)")
	}
	if v, ok := toUint8(float64(3.0)); !ok || v != 3 {
		t.Fatalf("toUint8(3.0) = (%d, %v), want (3, true)", v, ok)
	}
	if _, ok := toUint8(float64(-0.5)); ok {
		t.Fatal("toUint8(-0.5) must be rejected")
	}
	if _, ok := toUint8(float64(255.5)); ok {
		t.Fatal("toUint8(255.5) must be rejected")
	}
	if v, ok := toUint8(float64(255.0)); !ok || v != 255 {
		t.Fatalf("toUint8(255.0) = (%d, %v), want (255, true)", v, ok)
	}
}

// TestHandleEbusStandardCall_DecodeRejectsFractionalPB pins end-to-end
// that a fractional pb value flows through the dispatcher as
// INVALID_PAYLOAD. Regression for PR #505 r3106756021.
func TestHandleEbusStandardCall_DecodeRejectsFractionalPB(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	result, handled := s.handleEbusStandardCall(estd.ToolDecode, map[string]any{
		"pb":          3.9,
		"sb":          4,
		"direction":   "request",
		"frame_type":  "addressed",
		"payload_hex": "0102",
	})
	if !handled {
		t.Fatal("decode must be handled")
	}
	if isErr, _ := result["isError"].(bool); !isErr {
		t.Fatalf("expected isError=true on fractional pb, got %+v", result)
	}
	text := result["content"].([]map[string]any)[0]["text"].(string)
	var env map[string]any
	if err := json.Unmarshal([]byte(text), &env); err != nil {
		t.Fatalf("envelope JSON: %v", err)
	}
	errObj, _ := env["error"].(map[string]any)
	if code, _ := errObj["code"].(string); code != "INVALID_PAYLOAD" {
		t.Fatalf("error.code = %q, want INVALID_PAYLOAD", code)
	}
}

// TestHandleEbusStandardCall_DecodeRejectsMissingPayloadHex pins that
// an absent or non-string `payload_hex` key becomes INVALID_PAYLOAD
// rather than being silently defaulted. An EMPTY STRING is accepted
// (it is the standard representation of a zero-byte payload and
// hex.DecodeString("") succeeds) — see
// TestHandleEbusStandardCall_DecodeAcceptsEmptyPayloadHex below.
// Regression for PR #505 r3106794322 + r3106904917.
func TestHandleEbusStandardCall_DecodeRejectsMissingPayloadHex(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	cases := []struct {
		name string
		args map[string]any
	}{
		{"absent", map[string]any{
			"pb":         3,
			"sb":         4,
			"direction":  "request",
			"frame_type": "addressed",
		}},
		{"non_string", map[string]any{
			"pb":          3,
			"sb":          4,
			"direction":   "request",
			"frame_type":  "addressed",
			"payload_hex": 42,
		}},
		{"null", map[string]any{
			"pb":          3,
			"sb":          4,
			"direction":   "request",
			"frame_type":  "addressed",
			"payload_hex": nil,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, handled := s.handleEbusStandardCall(estd.ToolDecode, tc.args)
			if !handled {
				t.Fatal("decode must be handled")
			}
			if isErr, _ := result["isError"].(bool); !isErr {
				t.Fatalf("expected isError=true on malformed payload_hex, got %+v", result)
			}
			text := result["content"].([]map[string]any)[0]["text"].(string)
			var env map[string]any
			if err := json.Unmarshal([]byte(text), &env); err != nil {
				t.Fatalf("envelope JSON: %v", err)
			}
			errObj, _ := env["error"].(map[string]any)
			if code, _ := errObj["code"].(string); code != "INVALID_PAYLOAD" {
				t.Fatalf("error.code = %q, want INVALID_PAYLOAD", code)
			}
		})
	}
}

// TestHandleEbusStandardCall_CommandGetRejectsMissingID pins that a
// missing/empty/non-string `id` becomes INVALID_PAYLOAD rather than
// flowing through as UNKNOWN_COMMAND. Regression for PR #505
// r3106794325.
func TestHandleEbusStandardCall_CommandGetRejectsMissingID(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	cases := []struct {
		name string
		args map[string]any
	}{
		{"absent", map[string]any{}},
		{"empty_string", map[string]any{"id": ""}},
		{"non_string_int", map[string]any{"id": 42}},
		{"null", map[string]any{"id": nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, handled := s.handleEbusStandardCall(estd.ToolCommandGet, tc.args)
			if !handled {
				t.Fatal("command.get must be handled")
			}
			if isErr, _ := result["isError"].(bool); !isErr {
				t.Fatalf("expected isError=true, got %+v", result)
			}
			text := result["content"].([]map[string]any)[0]["text"].(string)
			var env map[string]any
			if err := json.Unmarshal([]byte(text), &env); err != nil {
				t.Fatalf("envelope JSON: %v", err)
			}
			errObj, _ := env["error"].(map[string]any)
			if code, _ := errObj["code"].(string); code != "INVALID_PAYLOAD" {
				t.Fatalf("error.code = %q, want INVALID_PAYLOAD", code)
			}
		})
	}
}

// TestHandleEbusStandardCall_CommandGetDispatchesNonexistent pins that
// a well-formed but nonexistent id still reaches the catalog lookup
// and returns its native error (UNKNOWN_COMMAND) rather than being
// short-circuited by the new INVALID_PAYLOAD guard.
func TestHandleEbusStandardCall_CommandGetDispatchesNonexistent(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())
	result, handled := s.handleEbusStandardCall(estd.ToolCommandGet, map[string]any{"id": "definitely-not-a-real-command-id"})
	if !handled {
		t.Fatal("command.get must be handled")
	}
	// It will be an error envelope, but the error code must NOT be
	// INVALID_PAYLOAD — it must be whatever the catalog raises for an
	// unknown id (preserving existing semantics).
	text := result["content"].([]map[string]any)[0]["text"].(string)
	var env map[string]any
	if err := json.Unmarshal([]byte(text), &env); err != nil {
		t.Fatalf("envelope JSON: %v", err)
	}
	errObj, _ := env["error"].(map[string]any)
	if code, _ := errObj["code"].(string); code == "INVALID_PAYLOAD" {
		t.Fatalf("nonexistent id must not map to INVALID_PAYLOAD; got %s", text)
	}
}

// TestNewServer_DispatchesEbusStandardServicesList pins end-to-end that
// a tools/call for ebus.v1.ebus_standard.services.list is dispatched
// (not rejected as unknown) by a default NewServer instance.
func TestNewServer_DispatchesEbusStandardServicesList(t *testing.T) {
	reg := &testRegistry{entries: map[byte]registry.DeviceEntry{}}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	params, err := json.Marshal(map[string]any{
		"name":      estd.ToolServicesList,
		"arguments": map[string]any{},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	result, rpcErr := server.handleToolsCall(context.Background(), params)
	if rpcErr != nil {
		t.Fatalf("handleToolsCall rejected ebus_standard.services.list: %+v", rpcErr)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result not a map: %T", result)
	}
	content, ok := m["content"].([]map[string]any)
	if !ok || len(content) == 0 {
		t.Fatalf("missing content in tools/call result: %+v", m)
	}
	if isErr, _ := m["isError"].(bool); isErr {
		t.Fatalf("services.list returned error envelope: %+v", m)
	}
}

// TestHandleEbusStandardCall_DecodeAcceptsEmptyPayloadHex is the
// regression for PR #505 r3106904917. An empty `payload_hex` string is
// the standard representation of a zero-byte payload — hex.DecodeString
// ("") succeeds and returns []byte{}. The wiring layer must NOT reject
// it as INVALID_PAYLOAD; it must pass it through to the underlying
// Decode implementation, which then reports UNKNOWN_COMMAND /
// identity-specific behavior as usual.
//
// Distinction pinned by this test:
//   - payload_hex absent         → INVALID_PAYLOAD (missing required)
//   - payload_hex: ""            → VALID, zero-byte decode proceeds
//   - payload_hex: "00AA"        → VALID, 2-byte decode
func TestHandleEbusStandardCall_DecodeAcceptsEmptyPayloadHex(t *testing.T) {
	s := &Server{}
	RegisterEbusStandardTools(s, ebusstd.MustEmbeddedCatalog())

	// Case 1: empty string — must NOT be INVALID_PAYLOAD. It may still
	// produce UNKNOWN_COMMAND if the (pb,sb,direction,frame_type) tuple
	// is not in the catalog, but the wiring gate must be cleared.
	args := map[string]any{
		"pb":          0xAB, // arbitrary pb/sb unlikely to match any catalog row
		"sb":          0xCD,
		"direction":   "request",
		"frame_type":  "addressed",
		"payload_hex": "",
	}
	result, handled := s.handleEbusStandardCall(estd.ToolDecode, args)
	if !handled {
		t.Fatal("decode must be handled")
	}
	text := result["content"].([]map[string]any)[0]["text"].(string)
	var env map[string]any
	if err := json.Unmarshal([]byte(text), &env); err != nil {
		t.Fatalf("envelope JSON: %v", err)
	}
	if errObj, ok := env["error"].(map[string]any); ok && errObj != nil {
		if code, _ := errObj["code"].(string); code == "INVALID_PAYLOAD" {
			t.Fatalf("empty payload_hex must NOT be rejected as INVALID_PAYLOAD, got envelope=%+v", env)
		}
	}

	// Case 2: absent — still INVALID_PAYLOAD (covered in sibling test,
	// re-asserted here as a nearby contrast).
	absentArgs := map[string]any{
		"pb":         0xAB,
		"sb":         0xCD,
		"direction":  "request",
		"frame_type": "addressed",
	}
	result2, _ := s.handleEbusStandardCall(estd.ToolDecode, absentArgs)
	text2 := result2["content"].([]map[string]any)[0]["text"].(string)
	var env2 map[string]any
	if err := json.Unmarshal([]byte(text2), &env2); err != nil {
		t.Fatalf("envelope JSON: %v", err)
	}
	errObj2, _ := env2["error"].(map[string]any)
	if code, _ := errObj2["code"].(string); code != "INVALID_PAYLOAD" {
		t.Fatalf("absent payload_hex: error.code = %q, want INVALID_PAYLOAD", code)
	}

	// Case 3: "00AA" — present, non-empty, valid hex must also clear
	// the wiring gate (may still fail catalog match, but not wiring).
	hexArgs := map[string]any{
		"pb":          0xAB,
		"sb":          0xCD,
		"direction":   "request",
		"frame_type":  "addressed",
		"payload_hex": "00AA",
	}
	result3, _ := s.handleEbusStandardCall(estd.ToolDecode, hexArgs)
	text3 := result3["content"].([]map[string]any)[0]["text"].(string)
	var env3 map[string]any
	if err := json.Unmarshal([]byte(text3), &env3); err != nil {
		t.Fatalf("envelope JSON: %v", err)
	}
	if errObj, ok := env3["error"].(map[string]any); ok && errObj != nil {
		if code, _ := errObj["code"].(string); code == "INVALID_PAYLOAD" {
			t.Fatalf("valid hex payload must NOT be rejected as INVALID_PAYLOAD, got envelope=%+v", env3)
		}
	}
}
