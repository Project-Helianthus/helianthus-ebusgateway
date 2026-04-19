package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	estd "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
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
