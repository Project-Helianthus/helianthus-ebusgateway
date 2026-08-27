package mcp

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

const publicToolsListManifestSHA256 = "86a0db977fe30fc6ea7970c0c844aa865a497bea1bca5914903753cb921663d1"

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
	got := fmt.Sprintf("%x", sha256.Sum256(manifest))
	if got != publicToolsListManifestSHA256 {
		t.Fatalf("tools/list manifest SHA-256 = %s; want %s", got, publicToolsListManifestSHA256)
	}
}
