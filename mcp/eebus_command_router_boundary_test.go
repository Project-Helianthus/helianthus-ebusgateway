package mcp

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const issue747MainEEBusManifestSHA256 = "2d28e20a07d6338ca8873907be284de92701b18b5140023e875a8d14136acec6"

func TestIssue747CurrentEEBusMCPInventoryRemainsUnchanged(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)

	beforePublicList := provider.callCount()
	publicTools := issue747EEBusTools(t, server.Handler())
	if after := provider.callCount(); after != beforePublicList {
		t.Fatalf("public tools/list contacted provider: %d -> %d", beforePublicList, after)
	}
	beforeOperatorList := provider.callCount()
	operatorTools := issue747EEBusTools(t, issue743OperatorHandler(t, server))
	if after := provider.callCount(); after != beforeOperatorList {
		t.Fatalf("operator tools/list contacted provider: %d -> %d", beforeOperatorList, after)
	}
	publicNames := issue747ToolNames(publicTools)
	operatorNames := issue747ToolNames(operatorTools)
	wantNames := append([]string(nil), msp06ToolNames...)
	sort.Strings(wantNames)

	if !reflect.DeepEqual(publicNames, wantNames) {
		t.Fatalf("public eeBUS tools = %v, want exact existing inventory %v", publicNames, wantNames)
	}
	if !reflect.DeepEqual(operatorNames, wantNames) {
		t.Fatalf("operator eeBUS tools = %v, want exact existing inventory %v", operatorNames, wantNames)
	}

	publicManifest, err := json.Marshal(publicTools)
	if err != nil {
		t.Fatal(err)
	}
	operatorManifest, err := json.Marshal(operatorTools)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(publicManifest, operatorManifest) {
		t.Fatalf("public and operator eeBUS manifests differ:\npublic: %s\noperator: %s",
			publicManifest, operatorManifest)
	}
	gotDigest := sha256.Sum256(publicManifest)
	if got := fmt.Sprintf("%x", gotDigest); got != issue747MainEEBusManifestSHA256 {
		t.Fatalf("eeBUS manifest SHA-256 = %s, want main golden %s\nmanifest: %s",
			got, issue747MainEEBusManifestSHA256, publicManifest)
	}
	if bytes.Contains(publicManifest, []byte("candidate_ref")) {
		t.Fatalf("current eeBUS manifest exposes candidate_ref: %s", publicManifest)
	}
}

func TestIssue747MCPHasNoDirectFeatureWriteOrRollbackPath(t *testing.T) {
	provider := &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")}
	server, _ := msp06TestServer(t, provider)

	for boundary, handler := range map[string]http.Handler{
		"public":   server.Handler(),
		"operator": issue743OperatorHandler(t, server),
	} {
		t.Run(boundary, func(t *testing.T) {
			for _, tool := range []string{
				"eebus.v1.features.get",
				"eebus.v1.features.data.get",
				"eebus.v1.features.data.set",
				"eebus.v1.mutations.get",
				"eebus.v1.mutations.rollback",
			} {
				before := provider.callCount()
				params, err := json.Marshal(map[string]any{
					"name":      tool,
					"arguments": map[string]any{},
				})
				if err != nil {
					t.Fatal(err)
				}
				response := doRPC(t, handler, rpcRequest{
					JSONRPC: "2.0",
					ID:      1,
					Method:  "tools/call",
					Params:  params,
				})
				if response.Error == nil || !strings.Contains(response.Error.Message, "unknown tool") {
					t.Fatalf("%s call to %q error = %+v, want unknown tool", boundary, tool, response.Error)
				}
				if after := provider.callCount(); after != before {
					t.Fatalf("%s call to %q contacted provider: %d -> %d", boundary, tool, before, after)
				}
			}
		})
	}
}

func issue747EEBusTools(t *testing.T, handler http.Handler) []map[string]any {
	t.Helper()
	response := doRPC(t, handler, rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	if response.Error != nil {
		t.Fatalf("tools/list error = %+v", response.Error)
	}
	result, ok := response.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list result type = %T, want map", response.Result)
	}
	rawTools, ok := result["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list tools type = %T, want []any", result["tools"])
	}

	tools := make([]map[string]any, 0, len(msp06ToolNames))
	for _, rawTool := range rawTools {
		tool, ok := rawTool.(map[string]any)
		if !ok {
			t.Fatalf("tool type = %T, want map", rawTool)
		}
		name, _ := tool["name"].(string)
		if strings.HasPrefix(name, "eebus.") {
			tools = append(tools, tool)
		}
	}
	sort.Slice(tools, func(left, right int) bool {
		leftName, _ := tools[left]["name"].(string)
		rightName, _ := tools[right]["name"].(string)
		return leftName < rightName
	})
	return tools
}

func issue747ToolNames(tools []map[string]any) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		name, _ := tool["name"].(string)
		names = append(names, name)
	}
	return names
}
