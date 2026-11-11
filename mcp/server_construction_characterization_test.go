package mcp

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func TestNewServerKeepsPublicToolsListManifest(t *testing.T) {
	t.Parallel()

	server, err := NewServer(&testRegistry{entries: map[byte]registry.DeviceEntry{}}, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	response := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	if response.Error != nil {
		t.Fatalf("tools/list error = %+v", response.Error)
	}
	result, ok := response.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list result type = %T; want map", response.Result)
	}
	manifest, err := json.Marshal(result["tools"])
	if err != nil {
		t.Fatalf("marshal tools/list manifest: %v", err)
	}

	canonical := namingDocumentedTools(t, "## Current default registration contract")
	if !bytes.Equal(manifest, canonical) {
		t.Fatal("tools/list differs from documented registration contract")
	}
}
func namingDocumentedTools(t *testing.T, heading string) []byte {
	t.Helper()
	document, err := os.ReadFile("../docs/mcp-tool-inventory.md")
	if err != nil {
		t.Fatal(err)
	}
	section := strings.SplitN(string(document), heading, 2)
	if len(section) != 2 {
		t.Fatal("missing documented registration contract")
	}
	block := strings.SplitN(section[1], "```json\n", 2)
	if len(block) != 2 {
		t.Fatal("missing registration snapshot")
	}
	raw := strings.SplitN(block[1], "\n```", 2)[0]
	var expected any
	if err := json.Unmarshal([]byte(raw), &expected); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
