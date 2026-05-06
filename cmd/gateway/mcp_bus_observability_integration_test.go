package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

const (
	mcpToolBusSummaryGet      = "ebus.v1.bus.summary.get"
	mcpToolBusMessagesList    = "ebus.v1.bus.messages.list"
	mcpToolBusPeriodicityList = "ebus.v1.bus.periodicity.list"
)

type emptyMCPRegistry struct{}

func (emptyMCPRegistry) Iterate(func(registry.DeviceEntry) bool) {}

func (emptyMCPRegistry) Lookup(byte) (registry.DeviceEntry, bool) {
	return nil, false
}

func TestMCPBusObservabilityProviderAdapterWiresRealStore(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
	cfg.SemanticStateInterval = 7 * time.Minute

	store := ebusgateway.NewBusObservabilityStore(cfg)
	if store == nil {
		t.Fatal("NewBusObservabilityStore() = nil")
	}
	startupAt := time.Date(2026, time.March, 12, 17, 45, 0, 0, time.UTC)
	store.SetStartupSurfaceProvider(func() *ebusgateway.BusObservabilityStartup {
		return &ebusgateway.BusObservabilityStartup{
			LastUpdatedAt: &startupAt,
			Phase:         "LIVE_READY",
			CacheEpoch:    4,
			LiveEpoch:     9,
		}
	})
	store.RecordBusAdmissionTransition("active", 0x7F, 0x08, "active_probe_passed")
	if err := store.OnBusEvent(protocol.BusEvent{
		Kind: protocol.BusEventAttemptComplete,
		Request: protocol.Frame{
			Source:    0x08,
			Target:    0x15,
			Primary:   0xB5,
			Secondary: 0x09,
			Data:      []byte{0x03},
		},
		HasRequest: true,
	}); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetBusObservabilityProvider(newMCPBusObservabilityProvider(store))

	tools := mcpToolsList(t, server.Handler())
	for _, name := range []string{
		mcpToolBusSummaryGet,
		mcpToolBusMessagesList,
		mcpToolBusPeriodicityList,
	} {
		if !mcpToolsContain(tools, name) {
			t.Fatalf("tools/list missing %q after real store wiring", name)
		}
	}

	summaryEnvelope := mcpCallToolEnvelope(t, server.Handler(), mcpToolBusSummaryGet, `{}`)
	summaryData, ok := summaryEnvelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("summary envelope data type = %T; want map", summaryEnvelope["data"])
	}
	status, ok := summaryData["status"].(map[string]any)
	if !ok {
		t.Fatalf("summary status type = %T; want map", summaryData["status"])
	}
	capability, ok := status["capability"].(map[string]any)
	if !ok {
		t.Fatalf("summary capability type = %T; want map", status["capability"])
	}
	if got, _ := capability["passive_state"].(string); got != "unavailable" {
		t.Fatalf("summary passive_state = %q; want unavailable", got)
	}
	if got, _ := capability["passive_reason"].(string); got != "unsupported_or_misconfigured" {
		t.Fatalf("summary passive_reason = %q; want unsupported_or_misconfigured", got)
	}
	if got, _ := summaryData["last_updated_at"].(string); got == "" {
		t.Fatalf("summary last_updated_at missing")
	} else if _, err := time.Parse(time.RFC3339Nano, got); err != nil {
		t.Fatalf("summary last_updated_at parse: %v", err)
	}
	if got, _ := status["last_updated_at"].(string); got == "" {
		t.Fatalf("summary status.last_updated_at missing")
	} else if _, err := time.Parse(time.RFC3339Nano, got); err != nil {
		t.Fatalf("summary status.last_updated_at parse: %v", err)
	}
	if got, _ := status["publisher_cadence_sec"].(float64); got != 420 {
		t.Fatalf("summary publisher_cadence_sec = %v; want 420", status["publisher_cadence_sec"])
	}
	if got, _ := status["publisher_cadence_source"].(string); got != "config.semantic_state_interval" {
		t.Fatalf("summary publisher_cadence_source = %q; want config.semantic_state_interval", got)
	}
	timingQuality, ok := status["timing_quality"].(map[string]any)
	if !ok {
		t.Fatalf("summary timing_quality type = %T; want map", status["timing_quality"])
	}
	if got, _ := timingQuality["passive"].(string); got != "unavailable" {
		t.Fatalf("summary timing_quality.passive = %q; want unavailable", got)
	}
	degraded, ok := status["degraded"].(map[string]any)
	if !ok {
		t.Fatalf("summary degraded type = %T; want map", status["degraded"])
	}
	if got, _ := degraded["active"].(bool); !got {
		t.Fatalf("summary degraded.active = %v; want true", degraded["active"])
	}
	featureFlags, ok := status["feature_flags"].(map[string]any)
	if !ok {
		t.Fatalf("summary feature_flags type = %T; want map", status["feature_flags"])
	}
	if got, _ := featureFlags["last_updated_at"].(string); got == "" {
		t.Fatalf("summary feature_flags.last_updated_at missing")
	} else if _, err := time.Parse(time.RFC3339Nano, got); err != nil {
		t.Fatalf("summary feature_flags.last_updated_at parse: %v", err)
	}
	startup, ok := status["startup"].(map[string]any)
	if !ok {
		t.Fatalf("summary startup type = %T; want map", status["startup"])
	}
	if got, _ := startup["last_updated_at"].(string); got == "" {
		t.Fatalf("summary startup.last_updated_at missing")
	} else if _, err := time.Parse(time.RFC3339Nano, got); err != nil {
		t.Fatalf("summary startup.last_updated_at parse: %v", err)
	} else if got != startupAt.Format(time.RFC3339Nano) {
		t.Fatalf("summary startup.last_updated_at = %q; want %s", got, startupAt.Format(time.RFC3339Nano))
	}
	if got, _ := startup["phase"].(string); got != "LIVE_READY" {
		t.Fatalf("summary startup.phase = %q; want LIVE_READY", got)
	}
	busAdmission, ok := status["bus_admission"].(map[string]any)
	if !ok {
		t.Fatalf("summary bus_admission type = %T; want map", status["bus_admission"])
	}
	for _, legacyKey := range []string{"state", "source", "companion_target", "reason"} {
		if _, ok := busAdmission[legacyKey]; ok {
			t.Fatalf("summary bus_admission contains legacy flat key %q: %#v", legacyKey, busAdmission)
		}
	}
	sourceSelection, ok := busAdmission["source_selection"].(map[string]any)
	if !ok {
		t.Fatalf("summary bus_admission.source_selection type = %T; want map", busAdmission["source_selection"])
	}
	if got, _ := sourceSelection["state"].(string); got != "active" {
		t.Fatalf("summary source_selection.state = %q; want active", got)
	}
	if got, _ := sourceSelection["outcome"].(string); got != "active_probe_passed" {
		t.Fatalf("summary source_selection.outcome = %q; want active_probe_passed", got)
	}
	if got, _ := sourceSelection["selected_source"].(float64); int(got) != 0x7F {
		t.Fatalf("summary source_selection.selected_source = %v; want 127", sourceSelection["selected_source"])
	}
	if got, _ := sourceSelection["companion_target"].(float64); int(got) != 0x08 {
		t.Fatalf("summary source_selection.companion_target = %v; want 8", sourceSelection["companion_target"])
	}
	if got, _ := sourceSelection["reason"].(string); got != "active_probe_passed" {
		t.Fatalf("summary source_selection.reason = %q; want active_probe_passed", got)
	}
	activeProbe, ok := sourceSelection["active_probe"].(map[string]any)
	if !ok {
		t.Fatalf("summary source_selection.active_probe type = %T; want map", sourceSelection["active_probe"])
	}
	if got, _ := activeProbe["target"].(float64); int(got) != 0x08 {
		t.Fatalf("summary source_selection.active_probe.target = %v; want 8", activeProbe["target"])
	}
	if got, _ := activeProbe["status"].(string); got != "active_probe_passed" {
		t.Fatalf("summary source_selection.active_probe.status = %q; want active_probe_passed", got)
	}

	messagesEnvelope := mcpCallToolEnvelope(t, server.Handler(), mcpToolBusMessagesList, `{"limit":1}`)
	messagesData, ok := messagesEnvelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("messages envelope data type = %T; want map", messagesEnvelope["data"])
	}
	if got, _ := messagesData["count"].(float64); int(got) != 1 {
		t.Fatalf("messages count = %v; want 1", messagesData["count"])
	}
	items, ok := messagesData["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("messages items = %#v; want 1 item", messagesData["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("messages item type = %T; want map", items[0])
	}
	if got, _ := item["family"].(string); got != "B509" {
		t.Fatalf("messages family = %q; want B509", got)
	}
}

func mcpToolsList(t *testing.T, handler http.Handler) []any {
	t.Helper()

	response := mcpRPC(t, handler, `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	if response.Error != nil {
		t.Fatalf("tools/list error = %+v", response.Error)
	}
	if response.Result == nil {
		t.Fatal("tools/list result = nil")
	}
	resultMap := response.Result
	tools, ok := resultMap["tools"].([]any)
	if !ok {
		t.Fatalf("tools/list tools type = %T; want []any", resultMap["tools"])
	}
	return tools
}

func mcpToolsContain(tools []any, name string) bool {
	for _, raw := range tools {
		toolMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if got, _ := toolMap["name"].(string); got == name {
			return true
		}
	}
	return false
}

func mcpCallToolEnvelope(t *testing.T, handler http.Handler, name string, arguments string) map[string]any {
	t.Helper()

	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + name + `","arguments":` + arguments + `}}`
	response := mcpRPC(t, handler, body)
	if response.Error != nil {
		t.Fatalf("tools/call %s error = %+v", name, response.Error)
	}
	if response.Result == nil {
		t.Fatalf("tools/call %s result = nil", name)
	}

	content, ok := response.Result["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("tools/call %s content = %#v; want 1 item", name, response.Result["content"])
	}
	contentItem, ok := content[0].(map[string]any)
	if !ok {
		t.Fatalf("tools/call %s content item type = %T; want map", name, content[0])
	}
	text, _ := contentItem["text"].(string)
	if text == "" {
		t.Fatalf("tools/call %s content.text empty", name)
	}

	var envelope map[string]any
	if err := json.Unmarshal([]byte(text), &envelope); err != nil {
		t.Fatalf("tools/call %s envelope unmarshal: %v", name, err)
	}
	if envelope["error"] != nil {
		t.Fatalf("tools/call %s envelope error = %#v; want nil", name, envelope["error"])
	}
	return envelope
}

type mcpRPCResponse struct {
	Result map[string]any `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func mcpRPC(t *testing.T, handler http.Handler, body string) mcpRPCResponse {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("rpc status = %d; want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var response mcpRPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("rpc response unmarshal: %v body=%s", err, rec.Body.String())
	}
	return response
}
