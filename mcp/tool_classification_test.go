package mcp

import (
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	estd "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

type toolClass string

const (
	toolClassCoreStable    toolClass = "core_stable"
	toolClassLegacy        toolClass = "legacy_alias"
	toolClassReservedDraft toolClass = "reserved_draft"
	toolClassExperimental  toolClass = "experimental"
)

func toolClassificationPolicy() map[string]toolClass {
	return map[string]toolClass{
		toolRuntimeStatusGetName:             toolClassCoreStable,
		toolBusSummaryGetName:                toolClassReservedDraft,
		toolBusMessagesListName:              toolClassReservedDraft,
		toolBusPeriodicityListName:           toolClassReservedDraft,
		toolWatchSummaryGetName:              toolClassReservedDraft,
		toolSemanticZonesGetName:             toolClassCoreStable,
		toolSemanticCircuitsGetName:          toolClassCoreStable,
		toolSemanticRadioGetName:             toolClassCoreStable,
		toolSemanticFM5ModeGetName:           toolClassCoreStable,
		toolSemanticFM5InterpretationGetName: toolClassCoreStable,
		toolSemanticSolarGetName:             toolClassCoreStable,
		toolSemanticCylindersGetName:         toolClassCoreStable,
		toolSemanticDHWGetName:               toolClassCoreStable,
		toolSemanticEnergyGetName:            toolClassCoreStable,
		toolSemanticBoilerGetName:            toolClassCoreStable,
		toolSemanticSystemGetName:            toolClassCoreStable,
		toolSemanticSchedulesGetName:         toolClassCoreStable,
		toolSemanticSchedulesSetZoneName:     toolClassCoreStable,
		toolSemanticSchedulesSetDhwName:      toolClassCoreStable,
		toolSemanticSystemSetConfigName:      toolClassCoreStable,
		toolSemanticBoilerSetConfigName:      toolClassCoreStable,
		toolSemanticAdapterInfoGetName:       toolClassCoreStable,
		toolSemanticSnapshotName:             toolClassCoreStable,
		toolSnapshotCaptureName:              toolClassCoreStable,
		toolSnapshotDropName:                 toolClassCoreStable,
		toolDevicesV1Name:                    toolClassCoreStable,
		toolDeviceGetV1Name:                  toolClassCoreStable,
		toolPlanesListV1Name:                 toolClassCoreStable,
		toolMethodsListV1Name:                toolClassCoreStable,
		toolInvokeV1Name:                     toolClassCoreStable,
		toolDevicesLegacyName:                toolClassLegacy,
		toolInvokeLegacyName:                 toolClassLegacy,
		estd.ToolServicesList:                toolClassCoreStable,
		estd.ToolCommandsList:                toolClassCoreStable,
		estd.ToolCommandGet:                  toolClassCoreStable,
		estd.ToolDecode:                      toolClassCoreStable,

		// Owner-only raw eeBUS command surface (execution-plans#71).
		string(eebusraw.ToolV1FeaturesGet):       toolClassCoreStable,
		string(eebusraw.ToolV1FeaturesDataGet):   toolClassCoreStable,
		string(eebusraw.ToolV1FeaturesDataSet):   toolClassCoreStable,
		string(eebusraw.ToolV1MutationsGet):      toolClassCoreStable,
		string(eebusraw.ToolV1MutationsRollback): toolClassCoreStable,
		// Vaillant B503 surface (M2a_GATEWAY_MCP / execution-plans#19).
		// Provider-scoped namespace parallel to ebus_standard; tools surface
		// a dedicated capability signal (meta.capabilities.vaillant_b503).
		toolVaillantB503ErrorsGetName:         toolClassCoreStable,
		toolVaillantB503ErrorsHistoryGetName:  toolClassCoreStable,
		toolVaillantB503ServiceCurrentGetName: toolClassCoreStable,
		toolVaillantB503ServiceHistoryGetName: toolClassCoreStable,
		toolVaillantB503LiveMonitorName:       toolClassCoreStable,
	}
}

// testInstallB503ForClassification wires the 5 Vaillant B503 tools onto
// server using no-op stubs. Used by TestToolClassificationPolicy so the
// production-wiring ebus.v1.vaillant.* tool names are checked against
// toolClassificationPolicy() in CI.
func testInstallB503ForClassification(server *Server) {
	mgr := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "classification-test", TransportEpoch: 1},
		0,
		nil,
	)
	RegisterVaillantB503Tools(server, VaillantB503Options{
		Dispatcher:     &stubB503Dispatcher{},
		SessionManager: mgr,
		DefaultTarget:  0x08,
	})
}

func isReservedDraftTool(name string) bool {
	switch name {
	case toolBusSummaryGetName, toolBusMessagesListName, toolBusPeriodicityListName, toolWatchSummaryGetName:
		return true
	default:
		return false
	}
}

func TestToolClassificationPolicy(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	// Install production-wired dynamic tools so the policy check covers the
	// registered operator inventory, not only the default NewServer tools.
	testInstallB503ForClassification(server)
	if err := server.RegisterEEBusV1CommandRouter(&issue749TypedRouter{}); err != nil {
		t.Fatalf("RegisterEEBusV1CommandRouter error = %v", err)
	}

	res := doRPC(t, issue743OperatorHandler(t, server), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list", Params: nil})
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

	classification := toolClassificationPolicy()

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
		if isReservedDraftTool(name) && class != toolClassReservedDraft {
			t.Fatalf("reserved draft tool %q classified as %q; want %q", name, class, toolClassReservedDraft)
		}
		if strings.HasPrefix(name, "ebus.v1.") && !isReservedDraftTool(name) && class != toolClassCoreStable {
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

func TestBusObservabilityToolClassificationReservation(t *testing.T) {
	reg := &testRegistry{entries: make(map[byte]registry.DeviceEntry)}
	server, err := NewServer(reg, &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetBusObservabilityProvider(&testBusObservabilityProvider{
		snapshot: BusObservabilitySnapshot{
			Summary: &BusSummary{
				Status: &BusObservabilityStatus{
					Capability: BusObservabilityCapability{PassiveState: "unavailable"},
				},
			},
		},
	})
	server.SetWatchSummaryProvider(&testWatchSummaryProvider{
		summary: WatchSummary{
			Degraded: WatchSummaryDegraded{
				ShadowingEnabled: true,
			},
		},
	})

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

	classification := toolClassificationPolicy()
	for _, name := range []string{
		toolBusSummaryGetName,
		toolBusMessagesListName,
		toolBusPeriodicityListName,
		toolWatchSummaryGetName,
	} {
		if !hasToolName(tools, name) {
			t.Fatalf("tools list missing reserved draft tool %q", name)
		}
		if class := classification[name]; class != toolClassReservedDraft {
			t.Fatalf("classification[%q] = %q; want %q", name, class, toolClassReservedDraft)
		}
	}
}
