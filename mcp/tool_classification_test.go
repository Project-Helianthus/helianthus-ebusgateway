package mcp

import (
	"strings"
	"testing"

	"github.com/d3vi1/helianthus-ebusreg/registry"
)

type toolClass string

const (
	toolClassCoreStable   toolClass = "core_stable"
	toolClassLegacy       toolClass = "legacy_alias"
	toolClassExperimental toolClass = "experimental"
)

func TestToolClassificationPolicy(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	res := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list", Params: nil})
	if res.Error != nil {
		t.Fatalf("tools/list error = %+v", res.Error)
	}
	resultMap, ok := res.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list result type = %T; want map", res.Result)
	}
	tools, ok := resultMap["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list tools type = %T; want []any", resultMap["tools"])
	}

	classification := map[string]toolClass{
		toolRuntimeStatusGetName:  toolClassCoreStable,
		toolSemanticZonesGetName:  toolClassCoreStable,
		toolSemanticDHWGetName:    toolClassCoreStable,
		toolSemanticEnergyGetName: toolClassCoreStable,
		toolSemanticBoilerGetName: toolClassCoreStable,
		toolSemanticSnapshotName:  toolClassCoreStable,
		toolSnapshotCaptureName:   toolClassCoreStable,
		toolSnapshotDropName:      toolClassCoreStable,
		toolDevicesV1Name:         toolClassCoreStable,
		toolDeviceGetV1Name:       toolClassCoreStable,
		toolPlanesListV1Name:      toolClassCoreStable,
		toolMethodsListV1Name:     toolClassCoreStable,
		toolInvokeV1Name:          toolClassCoreStable,
		toolDevicesLegacyName:     toolClassLegacy,
		toolInvokeLegacyName:      toolClassLegacy,
	}

	experimentalCount := 0
	for _, raw := range tools {
		toolMap, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("tool entry type = %T; want map", raw)
		}
		name, _ := toolMap["name"].(string)
		class, ok := classification[name]
		if !ok {
			t.Fatalf("tool %q missing classification", name)
		}
		if strings.HasPrefix(name, "ebus.v1.") && class != toolClassCoreStable {
			t.Fatalf("tool %q is v1 but classified as %q", name, class)
		}
		if strings.HasPrefix(name, "ebus.experimental.") {
			if class != toolClassExperimental {
				t.Fatalf("experimental tool %q must be classified experimental, got %q", name, class)
			}
			experimentalCount++
		}
	}

	if experimentalCount != 0 {
		t.Fatalf("experimental tool count = %d; want 0 in showroom surface", experimentalCount)
	}
}
