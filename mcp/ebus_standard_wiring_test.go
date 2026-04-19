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
