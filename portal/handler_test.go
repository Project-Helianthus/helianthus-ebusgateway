package portal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var canonicalPortalSemanticKeys = []string{
	"zones",
	"dhw",
	"energy_totals",
	"boiler_status",
	"system",
	"circuits",
	"radio_devices",
	"fm5_semantic_mode",
	"solar",
	"cylinders",
	"captured_utc",
}

func TestHandlerIndex(t *testing.T) {
	h := NewHandler(Options{GraphQLPath: "/graphql"})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<portal-shell") {
		t.Fatalf("index missing portal shell content")
	}
	if !strings.Contains(body, "/graphql") {
		t.Fatalf("index missing graphql path")
	}
}

func TestHandlerAssets(t *testing.T) {
	h := NewHandler(Options{})
	req := httptest.NewRequest(http.MethodGet, "/assets/app.css", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "--bg") {
		t.Fatalf("app.css missing expected content")
	}
	if got := rec.Header().Get("Cache-Control"); got == "" {
		t.Fatalf("cache-control missing")
	}
	if got := rec.Header().Get("ETag"); got == "" {
		t.Fatalf("etag missing")
	}
}

func TestHandlerAssets_NoMilestonePlaceholders(t *testing.T) {
	h := NewHandler(Options{})
	req := httptest.NewRequest(http.MethodGet, "/assets/app.js", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, banned := range []string{
		"Coming Soon",
		"M0 skeleton",
		"(M1)",
		"(M2)",
		"(M3)",
		"(M5)",
	} {
		if strings.Contains(body, banned) {
			t.Fatalf("asset app.js still contains banned placeholder text %q", banned)
		}
	}
}

func TestHandlerAssetsNotModified(t *testing.T) {
	h := NewHandler(Options{})

	firstReq := httptest.NewRequest(http.MethodGet, "/assets/app.js", nil)
	firstRec := httptest.NewRecorder()
	h.ServeHTTP(firstRec, firstReq)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d; want %d", firstRec.Code, http.StatusOK)
	}
	etag := firstRec.Header().Get("ETag")
	if etag == "" {
		t.Fatalf("etag missing on initial response")
	}

	req := httptest.NewRequest(http.MethodGet, "/assets/app.js", nil)
	req.Header.Set("If-None-Match", etag)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusNotModified)
	}
}

func TestHealthEndpoint(t *testing.T) {
	h := NewHandler(Options{GatewayVersion: "test-v", BuildID: "test-b"})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload["status"] != "ok" {
		t.Fatalf("status=%v; want ok", payload["status"])
	}
	if payload["gateway_version"] != "test-v" {
		t.Fatalf("gateway_version=%v; want test-v", payload["gateway_version"])
	}
	if payload["build_id"] != "test-b" {
		t.Fatalf("build_id=%v; want test-b", payload["build_id"])
	}
	if _, err := time.Parse(time.RFC3339, payload["time_utc"].(string)); err != nil {
		t.Fatalf("time_utc not RFC3339: %v", err)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q; want no-store", got)
	}
}

func TestBootstrapEndpoint(t *testing.T) {
	h := NewHandler(Options{
		GraphQLPath:      "/graphql",
		SnapshotPath:     "/snapshot",
		SubscriptionPath: "/graphql/subscriptions",
		MCPPath:          "/mcp",
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720"}}
		},
		ListSemantic: func() SemanticSnapshot {
			return SemanticSnapshot{Zones: []SemanticZone{{ID: "z1", Name: "Zone 1"}}}
		},
		GetBusObservability: func() any {
			return map[string]any{
				"status": map[string]any{
					"transport_class": "ens",
					"warmup": map[string]any{
						"state": "available",
					},
					"degraded": map[string]any{
						"active": false,
					},
				},
			}
		},
		ListProjections: func() []ProjectionDevice {
			return []ProjectionDevice{{Address: 0x10, Projections: []ProjectionSummary{{Plane: "Service"}}}}
		},
		GetProjection: func(address byte, plane string) (ProjectionGraph, bool) {
			return ProjectionGraph{}, true
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bootstrap", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload["ui_version"] != "m0" {
		t.Fatalf("ui_version=%v; want m0", payload["ui_version"])
	}
	endpoints := payload["endpoints"].(map[string]any)
	if endpoints["graphql"] != "/graphql" {
		t.Fatalf("graphql endpoint=%v; want /graphql", endpoints["graphql"])
	}
	if endpoints["search"] != "/portal/api/v1/search" {
		t.Fatalf("search endpoint=%v; want /portal/api/v1/search", endpoints["search"])
	}
	if endpoints["bus_observability"] != "/portal/api/v1/bus/observability" {
		t.Fatalf("bus_observability endpoint=%v; want /portal/api/v1/bus/observability", endpoints["bus_observability"])
	}
	if endpoints["timeline"] != "/portal/api/v1/timeline/events" {
		t.Fatalf("timeline endpoint=%v; want /portal/api/v1/timeline/events", endpoints["timeline"])
	}
	if endpoints["provenance"] != "/portal/api/v1/provenance/events" {
		t.Fatalf("provenance endpoint=%v; want /portal/api/v1/provenance/events", endpoints["provenance"])
	}
	if endpoints["snapshots"] != "/portal/api/v1/snapshots" {
		t.Fatalf("snapshots endpoint=%v; want /portal/api/v1/snapshots", endpoints["snapshots"])
	}
	if endpoints["capture"] != "/portal/api/v1/snapshots/capture" {
		t.Fatalf("capture endpoint=%v; want /portal/api/v1/snapshots/capture", endpoints["capture"])
	}
	if endpoints["retention"] != "/portal/api/v1/snapshots/retention" {
		t.Fatalf("retention endpoint=%v; want /portal/api/v1/snapshots/retention", endpoints["retention"])
	}
	if endpoints["snapshot_diff"] != "/portal/api/v1/snapshots/diff" {
		t.Fatalf("snapshot_diff endpoint=%v; want /portal/api/v1/snapshots/diff", endpoints["snapshot_diff"])
	}
	if endpoints["sessions"] != "/portal/api/v1/sessions" {
		t.Fatalf("sessions endpoint=%v; want /portal/api/v1/sessions", endpoints["sessions"])
	}
	if endpoints["session_save"] != "/portal/api/v1/sessions/save" {
		t.Fatalf("session_save endpoint=%v; want /portal/api/v1/sessions/save", endpoints["session_save"])
	}
	if endpoints["session_load"] != "/portal/api/v1/sessions/load" {
		t.Fatalf("session_load endpoint=%v; want /portal/api/v1/sessions/load", endpoints["session_load"])
	}
	if endpoints["issue_draft"] != "/portal/api/v1/issues/draft" {
		t.Fatalf("issue_draft endpoint=%v; want /portal/api/v1/issues/draft", endpoints["issue_draft"])
	}
	if endpoints["issue_export"] != "/portal/api/v1/issues/export" {
		t.Fatalf("issue_export endpoint=%v; want /portal/api/v1/issues/export", endpoints["issue_export"])
	}
	capabilities := payload["capabilities"].(map[string]any)
	if capabilities["registry"] != true {
		t.Fatalf("capabilities.registry=%v; want true", capabilities["registry"])
	}
	if capabilities["semantic"] != true {
		t.Fatalf("capabilities.semantic=%v; want true", capabilities["semantic"])
	}
	if capabilities["bus_observability"] != true {
		t.Fatalf("capabilities.bus_observability=%v; want true", capabilities["bus_observability"])
	}
	if capabilities["projection"] != true {
		t.Fatalf("capabilities.projection=%v; want true", capabilities["projection"])
	}
	if capabilities["search"] != true {
		t.Fatalf("capabilities.search=%v; want true", capabilities["search"])
	}
	if capabilities["stream"] != true {
		t.Fatalf("capabilities.stream=%v; want true", capabilities["stream"])
	}
	if capabilities["timeline"] != true {
		t.Fatalf("capabilities.timeline=%v; want true", capabilities["timeline"])
	}
	if capabilities["provenance"] != true {
		t.Fatalf("capabilities.provenance=%v; want true", capabilities["provenance"])
	}
	if capabilities["snapshots"] != true {
		t.Fatalf("capabilities.snapshots=%v; want true", capabilities["snapshots"])
	}
	if capabilities["snapshot_diff"] != true {
		t.Fatalf("capabilities.snapshot_diff=%v; want true", capabilities["snapshot_diff"])
	}
	if capabilities["sessions"] != true {
		t.Fatalf("capabilities.sessions=%v; want true", capabilities["sessions"])
	}
	if capabilities["issue_builder"] != true {
		t.Fatalf("capabilities.issue_builder=%v; want true", capabilities["issue_builder"])
	}
	if capabilities["migration"] != false {
		t.Fatalf("capabilities.migration=%v; want false", capabilities["migration"])
	}
}

func TestAPIMethodNotAllowed(t *testing.T) {
	h := NewHandler(Options{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/health", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("Allow=%q; want GET", got)
	}
}

func TestBusObservabilityEndpointUnavailable(t *testing.T) {
	h := NewHandler(Options{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bus/observability", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestBusObservabilityEndpoint(t *testing.T) {
	h := NewHandler(Options{
		GetBusObservability: func() any {
			return map[string]any{
				"status": map[string]any{
					"transport_class": "ebusd-tcp",
					"warmup": map[string]any{
						"state": "warming_up",
					},
					"degraded": map[string]any{
						"active": true,
						"reasons": []string{
							"unsupported_or_misconfigured",
						},
					},
				},
			}
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bus/observability", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	status, ok := payload["status"].(map[string]any)
	if !ok {
		t.Fatalf("status missing or invalid")
	}
	if status["transport_class"] != "ebusd-tcp" {
		t.Fatalf("status.transport_class=%v; want ebusd-tcp", status["transport_class"])
	}
}

func TestMountedPortalEndpoints(t *testing.T) {
	portalPath := "/portal"
	portalHandler := NewHandler(Options{})
	mux := http.NewServeMux()
	mux.Handle(portalPath+"/", http.StripPrefix(portalPath, portalHandler))
	mux.HandleFunc(portalPath, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, portalPath+"/", http.StatusMovedPermanently)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/portal", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMovedPermanently {
		t.Fatalf("redirect status=%d; want %d", rec.Code, http.StatusMovedPermanently)
	}
	if rec.Header().Get("Location") != "/portal/" {
		t.Fatalf("redirect location=%q; want /portal/", rec.Header().Get("Location"))
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/portal/api/v1/bootstrap", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("bootstrap status=%d; want %d", rec.Code, http.StatusOK)
	}
	var bootstrap map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &bootstrap); err != nil {
		t.Fatalf("bootstrap unmarshal: %v", err)
	}
	if bootstrap["ui_version"] != "m0" {
		t.Fatalf("ui_version=%v; want m0", bootstrap["ui_version"])
	}
}

func TestRegistryDevicesEndpoint(t *testing.T) {
	h := NewHandler(Options{
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{
				{
					Address:      0x10,
					Manufacturer: "Vaillant",
					DeviceID:     "VRC720",
					Planes: []RegistryPlane{
						{Name: "system", Methods: []string{"get_status"}},
					},
				},
				{
					Address:      0x08,
					Manufacturer: "Vaillant",
					DeviceID:     "BAI",
					Planes: []RegistryPlane{
						{Name: "heating", Methods: []string{"get_operational_data"}},
					},
				},
			}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/registry/devices?limit=1", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(payload["count"].(float64)) != 2 {
		t.Fatalf("count=%v; want 2", payload["count"])
	}
	items := payload["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("len(items)=%d; want 1 due to limit", len(items))
	}
	first := items[0].(map[string]any)
	if int(first["address"].(float64)) != 0x08 {
		t.Fatalf("first.address=%v; want 8 sorted asc", first["address"])
	}
}

func TestRegistryDevicesEndpoint_Filter(t *testing.T) {
	h := NewHandler(Options{
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{
				{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720", Planes: []RegistryPlane{{Name: "system", Methods: []string{"get_status"}}}},
				{Address: 0x08, Manufacturer: "Vaillant", DeviceID: "BAI", Planes: []RegistryPlane{{Name: "heating", Methods: []string{"get_operational_data"}}}},
			}
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/registry/devices?q=operational", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(payload["count"].(float64)) != 1 {
		t.Fatalf("count=%v; want 1", payload["count"])
	}
	items := payload["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("len(items)=%d; want 1", len(items))
	}
	if int(items[0].(map[string]any)["address"].(float64)) != 0x08 {
		t.Fatalf("address=%v; want 8", items[0].(map[string]any)["address"])
	}
}

func TestSemanticSnapshotEndpoint(t *testing.T) {
	current := 43.5
	target := 45.0
	flowTemperature := 54.2
	dhwOperatingMode := "auto"
	systemFlowTemperature := 31.4
	adaptiveHeatingCurve := true
	systemScheme := 8
	pumpActive := true
	flowSetpoint := 35.0
	heatingCurve := 0.8
	h := NewHandler(Options{
		ListSemantic: func() SemanticSnapshot {
			return SemanticSnapshot{
				Zones: []SemanticZone{
					{
						ID:   "zone_1",
						Name: "Living",
						State: SemanticZoneState{
							CurrentTempC: &current,
						},
						Config: SemanticZoneConfig{
							TargetTempC: &target,
						},
					},
				},
				DHW: &SemanticDHW{
					State: SemanticDhwState{
						CurrentTempC: &current,
					},
					Config: SemanticDhwConfig{OperatingMode: "auto"},
				},
				BoilerStatus: &SemanticBoilerStatus{
					State: SemanticBoilerState{
						FlowTemperatureC: &flowTemperature,
					},
					Config: SemanticBoilerConfig{
						DhwOperatingMode: &dhwOperatingMode,
					},
				},
				System: &SemanticSystemStatus{
					State: SemanticSystemState{
						SystemFlowTemperature: &systemFlowTemperature,
					},
					Config: SemanticSystemConfig{
						AdaptiveHeatingCurve: &adaptiveHeatingCurve,
					},
					Properties: SemanticSystemProperties{
						SystemScheme: &systemScheme,
					},
				},
				Circuits: []SemanticCircuit{
					{
						Index:       1,
						CircuitType: "DIRECT",
						HasMixer:    false,
						State: SemanticCircuitState{
							PumpActive:    &pumpActive,
							FlowSetpointC: &flowSetpoint,
						},
						Config: SemanticCircuitConfig{
							HeatingCurve: &heatingCurve,
						},
						ManagingDevice: SemanticManagingDevice{
							Role:     "FUNCTION_MODULE",
							DeviceID: stringPtr("VR_71"),
							Address:  intPtr(0x26),
						},
					},
				},
				CapturedUTC: "2026-02-23T22:00:00Z",
			}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/semantic/snapshot", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload["captured_utc"] != "2026-02-23T22:00:00Z" {
		t.Fatalf("captured_utc=%v", payload["captured_utc"])
	}
	zones := payload["zones"].([]any)
	if len(zones) != 1 {
		t.Fatalf("zones=%d; want 1", len(zones))
	}
	zone := zones[0].(map[string]any)
	if zone["name"] != "Living" {
		t.Fatalf("zone.name=%v; want Living", zone["name"])
	}
	zoneState := zone["state"].(map[string]any)
	if zoneState["current_temp_c"] != current {
		t.Fatalf("zone.state.current_temp_c=%v; want %v", zoneState["current_temp_c"], current)
	}
	zoneConfig := zone["config"].(map[string]any)
	if zoneConfig["target_temp_c"] != target {
		t.Fatalf("zone.config.target_temp_c=%v; want %v", zoneConfig["target_temp_c"], target)
	}
	dhw := payload["dhw"].(map[string]any)
	dhwState := dhw["state"].(map[string]any)
	if dhwState["current_temp_c"] != current {
		t.Fatalf("dhw.state.current_temp_c=%v; want %v", dhwState["current_temp_c"], current)
	}
	dhwConfig := dhw["config"].(map[string]any)
	if dhwConfig["operating_mode"] != "auto" {
		t.Fatalf("dhw.config.operating_mode=%v; want auto", dhwConfig["operating_mode"])
	}
	boiler := payload["boiler_status"].(map[string]any)
	boilerState := boiler["state"].(map[string]any)
	if boilerState["flow_temperature_c"] != flowTemperature {
		t.Fatalf("boiler.state.flow_temperature_c=%v; want %v", boilerState["flow_temperature_c"], flowTemperature)
	}
	boilerConfig := boiler["config"].(map[string]any)
	if boilerConfig["dhw_operating_mode"] != dhwOperatingMode {
		t.Fatalf("boiler.config.dhw_operating_mode=%v; want %v", boilerConfig["dhw_operating_mode"], dhwOperatingMode)
	}
	system := payload["system"].(map[string]any)
	systemState := system["state"].(map[string]any)
	if systemState["system_flow_temperature"] != systemFlowTemperature {
		t.Fatalf("system.state.system_flow_temperature=%v; want %v", systemState["system_flow_temperature"], systemFlowTemperature)
	}
	systemConfig := system["config"].(map[string]any)
	if systemConfig["adaptive_heating_curve"] != adaptiveHeatingCurve {
		t.Fatalf("system.config.adaptive_heating_curve=%v; want %v", systemConfig["adaptive_heating_curve"], adaptiveHeatingCurve)
	}
	systemProperties := system["properties"].(map[string]any)
	if int(systemProperties["system_scheme"].(float64)) != systemScheme {
		t.Fatalf("system.properties.system_scheme=%v; want %d", systemProperties["system_scheme"], systemScheme)
	}
	circuits := payload["circuits"].([]any)
	if len(circuits) != 1 {
		t.Fatalf("circuits=%d; want 1", len(circuits))
	}
	circuit := circuits[0].(map[string]any)
	if int(circuit["index"].(float64)) != 1 {
		t.Fatalf("circuit.index=%v; want 1", circuit["index"])
	}
	if circuit["circuit_type"] != "DIRECT" {
		t.Fatalf("circuit.circuit_type=%v; want DIRECT", circuit["circuit_type"])
	}
	if circuit["has_mixer"] != false {
		t.Fatalf("circuit.has_mixer=%v; want false", circuit["has_mixer"])
	}
	circuitState := circuit["state"].(map[string]any)
	if circuitState["pump_active"] != pumpActive {
		t.Fatalf("circuit.state.pump_active=%v; want %v", circuitState["pump_active"], pumpActive)
	}
	if circuitState["flow_setpoint_c"] != flowSetpoint {
		t.Fatalf("circuit.state.flow_setpoint_c=%v; want %v", circuitState["flow_setpoint_c"], flowSetpoint)
	}
	circuitConfig := circuit["config"].(map[string]any)
	if circuitConfig["heating_curve"] != heatingCurve {
		t.Fatalf("circuit.config.heating_curve=%v; want %v", circuitConfig["heating_curve"], heatingCurve)
	}
	managingDevice := circuit["managing_device"].(map[string]any)
	if managingDevice["role"] != "FUNCTION_MODULE" {
		t.Fatalf("circuit.managing_device.role=%v; want FUNCTION_MODULE", managingDevice["role"])
	}
	if managingDevice["device_id"] != "VR_71" {
		t.Fatalf("circuit.managing_device.device_id=%v; want VR_71", managingDevice["device_id"])
	}
}

func TestSemanticSnapshotEndpoint_DefaultWhenMissingProvider(t *testing.T) {
	h := NewHandler(Options{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/semantic/snapshot", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	zones := payload["zones"].([]any)
	if len(zones) != 0 {
		t.Fatalf("zones=%d; want 0", len(zones))
	}
}

func TestSemanticSnapshotEndpoint_ExtensionFamilies(t *testing.T) {
	deviceConnected := true
	devicePaired := true
	deviceClassAddress := 0x15
	firmwareVersion := "09.03"
	hardwareIdentifier := 4711
	remoteControlAddress := 0x01
	receptionStrength := 88
	zoneAssignment := 2
	roomTemperature := 22.5
	roomHumidity := 44.0
	solarCollectorTemperature := 62.5
	solarReturnTemperature := 45.1
	solarPumpActive := true
	solarYield := 3.4
	solarPumpHours := 104.0
	solarEnabled := true
	solarFunctionMode := false
	cylinderTemperature := 49.5
	cylinderMaxSetpoint := 59.0
	cylinderChargeHysteresis := 5.0
	cylinderChargeOffset := 2.0

	h := NewHandler(Options{
		ListSemantic: func() SemanticSnapshot {
			return SemanticSnapshot{
				RadioDevices: []SemanticRadioDevice{
					{
						Group:                0,
						Instance:             1,
						SlotMode:             "THERMOSTAT",
						DeviceConnected:      &deviceConnected,
						DeviceClassAddress:   &deviceClassAddress,
						DeviceModel:          "VR92",
						FirmwareVersion:      &firmwareVersion,
						HardwareIdentifier:   &hardwareIdentifier,
						RemoteControlAddress: &remoteControlAddress,
						DevicePaired:         &devicePaired,
						ReceptionStrength:    &receptionStrength,
						ZoneAssignment:       &zoneAssignment,
						RoomTemperatureC:     &roomTemperature,
						RoomHumidityPct:      &roomHumidity,
					},
				},
				FM5Mode: "INTERPRETED",
				Solar: &SemanticSolarStatus{
					CollectorTemperatureC: &solarCollectorTemperature,
					ReturnTemperatureC:    &solarReturnTemperature,
					PumpActive:            &solarPumpActive,
					CurrentYield:          &solarYield,
					PumpHours:             &solarPumpHours,
					SolarEnabled:          &solarEnabled,
					FunctionMode:          &solarFunctionMode,
				},
				Cylinders: []SemanticCylinder{
					{
						Index:             1,
						TemperatureC:      &cylinderTemperature,
						MaxSetpointC:      &cylinderMaxSetpoint,
						ChargeHysteresisC: &cylinderChargeHysteresis,
						ChargeOffsetC:     &cylinderChargeOffset,
					},
				},
				CapturedUTC: "2026-02-23T22:05:00Z",
			}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/semantic/snapshot", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload["fm5_semantic_mode"] != "INTERPRETED" {
		t.Fatalf("fm5_semantic_mode=%v; want INTERPRETED", payload["fm5_semantic_mode"])
	}
	radioDevices := payload["radio_devices"].([]any)
	if len(radioDevices) != 1 {
		t.Fatalf("radio_devices=%d; want 1", len(radioDevices))
	}
	radio := radioDevices[0].(map[string]any)
	if radio["device_model"] != "VR92" {
		t.Fatalf("radio.device_model=%v; want VR92", radio["device_model"])
	}
	if radio["slot_mode"] != "THERMOSTAT" {
		t.Fatalf("radio.slot_mode=%v; want THERMOSTAT", radio["slot_mode"])
	}
	if int(radio["zone_assignment"].(float64)) != zoneAssignment {
		t.Fatalf("radio.zone_assignment=%v; want %d", radio["zone_assignment"], zoneAssignment)
	}
	solar := payload["solar"].(map[string]any)
	if solar["collector_temperature_c"] != solarCollectorTemperature {
		t.Fatalf("solar.collector_temperature_c=%v; want %v", solar["collector_temperature_c"], solarCollectorTemperature)
	}
	if solar["pump_active"] != solarPumpActive {
		t.Fatalf("solar.pump_active=%v; want %v", solar["pump_active"], solarPumpActive)
	}
	cylinders := payload["cylinders"].([]any)
	if len(cylinders) != 1 {
		t.Fatalf("cylinders=%d; want 1", len(cylinders))
	}
	cylinder := cylinders[0].(map[string]any)
	if int(cylinder["index"].(float64)) != 1 {
		t.Fatalf("cylinder.index=%v; want 1", cylinder["index"])
	}
	if cylinder["temperature_c"] != cylinderTemperature {
		t.Fatalf("cylinder.temperature_c=%v; want %v", cylinder["temperature_c"], cylinderTemperature)
	}
}

func TestSemanticSnapshotEndpoint_CanonicalFamilyInventory(t *testing.T) {
	dhwMode := "AUTO"
	systemScheme := 1
	zoneAssignment := 2
	solarEnabled := false
	solarCollector := 120.5
	cylinderTemperature := 58.1

	h := NewHandler(Options{
		ListSemantic: func() SemanticSnapshot {
			return SemanticSnapshot{
				Zones:  []SemanticZone{{ID: "zone-1", Name: "Parter"}},
				DHW:    &SemanticDHW{Config: SemanticDhwConfig{OperatingMode: "heat"}},
				Energy: &SemanticEnergyTotals{},
				BoilerStatus: &SemanticBoilerStatus{
					Config: SemanticBoilerConfig{DhwOperatingMode: &dhwMode},
				},
				System: &SemanticSystemStatus{
					Properties: SemanticSystemProperties{SystemScheme: &systemScheme},
				},
				Circuits: []SemanticCircuit{
					{Index: 0, CircuitType: "heating"},
				},
				RadioDevices: []SemanticRadioDevice{
					{Group: 0, Instance: 1, DeviceModel: "VR92", ZoneAssignment: &zoneAssignment},
				},
				FM5Mode: "INTERPRETED",
				Solar: &SemanticSolarStatus{
					CollectorTemperatureC: &solarCollector,
					SolarEnabled:          &solarEnabled,
				},
				Cylinders: []SemanticCylinder{
					{Index: 0, TemperatureC: &cylinderTemperature},
				},
				CapturedUTC: "2026-02-23T23:00:00Z",
			}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/semantic/snapshot", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, key := range canonicalPortalSemanticKeys {
		if _, ok := payload[key]; !ok {
			t.Fatalf("semantic snapshot missing key %q: %#v", key, payload)
		}
	}
}

func TestProjectionDevicesEndpoint(t *testing.T) {
	h := NewHandler(Options{
		ListProjections: func() []ProjectionDevice {
			return []ProjectionDevice{
				{
					Address:      0x10,
					DeviceID:     "VRC720",
					Manufacturer: "Vaillant",
					Projections: []ProjectionSummary{
						{Plane: "Service", NodeCount: 12, EdgeCount: 10},
					},
				},
				{
					Address:      0x08,
					DeviceID:     "BAI",
					Manufacturer: "Vaillant",
					Projections: []ProjectionSummary{
						{Plane: "Observability", NodeCount: 5, EdgeCount: 4},
					},
				},
			}
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/projection/devices?limit=1", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(payload["count"].(float64)) != 2 {
		t.Fatalf("count=%v; want 2", payload["count"])
	}
	items := payload["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("len(items)=%d; want 1 due limit", len(items))
	}
	if int(items[0].(map[string]any)["address"].(float64)) != 0x08 {
		t.Fatalf("address=%v; want 8 sorted asc", items[0].(map[string]any)["address"])
	}
}

func TestProjectionGraphEndpoint(t *testing.T) {
	h := NewHandler(Options{
		GetProjection: func(address byte, plane string) (ProjectionGraph, bool) {
			if address != 0x10 || !strings.EqualFold(plane, "Service") {
				return ProjectionGraph{}, false
			}
			return ProjectionGraph{
				Address: 0x10,
				Plane:   "Service",
				Nodes: []ProjectionNode{
					{ID: "n1", Path: "Service:/n1", CanonicalPath: "Service:/n1"},
				},
				Edges: []ProjectionEdge{
					{ID: "e1", From: "n1", To: "n1"},
				},
			}, true
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/projection/graph?address=0x10&plane=Service", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/projection/graph?address=0x10", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing plane status=%d; want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestSearchEndpoint(t *testing.T) {
	current := 21.7
	h := NewHandler(Options{
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{
				{
					Address:      0x10,
					Manufacturer: "Vaillant",
					DeviceID:     "VRC720",
					Planes: []RegistryPlane{
						{Name: "system", Methods: []string{"get_status"}},
					},
				},
			}
		},
		ListSemantic: func() SemanticSnapshot {
			return SemanticSnapshot{
				Zones: []SemanticZone{
					{ID: "zone_1", Name: "Living", Config: SemanticZoneConfig{OperatingMode: "auto"}, State: SemanticZoneState{CurrentTempC: &current}},
				},
				DHW: &SemanticDHW{Config: SemanticDhwConfig{OperatingMode: "auto"}},
			}
		},
		ListProjections: func() []ProjectionDevice {
			return []ProjectionDevice{
				{
					Address:      0x10,
					DeviceID:     "VRC720",
					DisplayName:  "sensoCOMFORT",
					Manufacturer: "Vaillant",
					Projections: []ProjectionSummary{
						{Plane: "Service", NodeCount: 1, EdgeCount: 1},
					},
				},
			}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/search?q=service&limit=10", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload["query"] != "service" {
		t.Fatalf("query=%v; want service", payload["query"])
	}
	items := payload["items"].([]any)
	if len(items) == 0 {
		t.Fatalf("expected at least one search result")
	}
	first := items[0].(map[string]any)
	if first["layer"] == "" {
		t.Fatalf("layer missing on first result")
	}
	if first["title"] == "" {
		t.Fatalf("title missing on first result")
	}
}

func TestSearchEndpoint_EmptyQuery(t *testing.T) {
	h := NewHandler(Options{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/search?q=&limit=10", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(payload["count"].(float64)) != 0 {
		t.Fatalf("count=%v; want 0", payload["count"])
	}
}

func TestSearchEndpoint_SemanticFamilyCoverage(t *testing.T) {
	dhwOperatingMode := "AUTO"
	systemScheme := 1
	zoneAssignment := 2
	firmwareVersion := "09.03"
	solarEnabled := false
	solarCollector := 138.7
	cylinderTemperature := 58.1

	h := NewHandler(Options{
		ListSemantic: func() SemanticSnapshot {
			return SemanticSnapshot{
				Energy: &SemanticEnergyTotals{},
				BoilerStatus: &SemanticBoilerStatus{
					Config: SemanticBoilerConfig{
						DhwOperatingMode: &dhwOperatingMode,
					},
				},
				System: &SemanticSystemStatus{
					Properties: SemanticSystemProperties{
						SystemScheme: &systemScheme,
					},
				},
				Circuits: []SemanticCircuit{
					{Index: 1, CircuitType: "heating", State: SemanticCircuitState{CircuitState: "active"}, Config: SemanticCircuitConfig{RoomTempControl: "thermostat"}},
				},
				RadioDevices: []SemanticRadioDevice{
					{Group: 0, Instance: 1, DeviceModel: "VR92", SlotMode: "THERMOSTAT", ZoneAssignment: &zoneAssignment, FirmwareVersion: &firmwareVersion},
				},
				FM5Mode: "INTERPRETED",
				Solar: &SemanticSolarStatus{
					CollectorTemperatureC: &solarCollector,
					SolarEnabled:          &solarEnabled,
				},
				Cylinders: []SemanticCylinder{
					{Index: 0, TemperatureC: &cylinderTemperature},
				},
			}
		},
	})

	testCases := []struct {
		query     string
		wantKind  string
		wantTitle string
	}{
		{query: "boiler", wantKind: "boiler", wantTitle: "Boiler"},
		{query: "system", wantKind: "system", wantTitle: "System"},
		{query: "solar", wantKind: "solar", wantTitle: "Solar"},
		{query: "energy", wantKind: "energy", wantTitle: "Energy Totals"},
		{query: "circuit", wantKind: "circuit", wantTitle: "Circuit 1"},
		{query: "vr92", wantKind: "radio", wantTitle: "Radio VR92"},
		{query: "fm5", wantKind: "fm5", wantTitle: "FM5"},
		{query: "cylinder", wantKind: "cylinder", wantTitle: "Cylinder 0"},
	}

	for _, tc := range testCases {
		t.Run(tc.query, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/search?q="+tc.query+"&limit=20", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
			}

			var payload map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			items := payload["items"].([]any)
			if len(items) == 0 {
				t.Fatalf("expected at least one search result for %q", tc.query)
			}

			found := false
			for _, item := range items {
				entry := item.(map[string]any)
				if entry["layer"] == "semantic" && entry["kind"] == tc.wantKind && entry["title"] == tc.wantTitle {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("missing semantic result kind=%q title=%q in %v", tc.wantKind, tc.wantTitle, items)
			}
		})
	}
}

func TestStreamEndpoint(t *testing.T) {
	h := NewHandler(Options{
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{
				{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720"},
			}
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream?layers=registry&interval_ms=10&max_events_per_second=10&max_events=1", nil)
	ctx, cancel := context.WithTimeout(req.Context(), time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type=%q; want text/event-stream", got)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: update") {
		t.Fatalf("stream body missing update event: %q", body)
	}
	if !strings.Contains(body, "\"layer\":\"composite\"") {
		t.Fatalf("stream payload missing composite layer: %q", body)
	}
	if !strings.Contains(body, "\"registry\":{") {
		t.Fatalf("stream composite payload missing registry section: %q", body)
	}
}

func TestStreamEndpoint_UnavailableWithoutProviders(t *testing.T) {
	h := NewHandler(Options{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestTimelineEventsEndpoint(t *testing.T) {
	h := NewHandler(Options{
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720"}}
		},
	})

	streamReq := httptest.NewRequest(http.MethodGet, "/api/v1/stream?layers=registry&interval_ms=10&max_events_per_second=10&max_events=1", nil)
	ctx, cancel := context.WithTimeout(streamReq.Context(), time.Second)
	defer cancel()
	streamReq = streamReq.WithContext(ctx)
	streamRec := httptest.NewRecorder()
	h.ServeHTTP(streamRec, streamReq)
	if streamRec.Code != http.StatusOK {
		t.Fatalf("stream status=%d; want %d", streamRec.Code, http.StatusOK)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/timeline/events?limit=10&layer=composite", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(payload["count"].(float64)) < 1 {
		t.Fatalf("count=%v; want >=1", payload["count"])
	}
	items := payload["items"].([]any)
	first := items[0].(map[string]any)
	if first["layer"] != "composite" {
		t.Fatalf("layer=%v; want composite", first["layer"])
	}

	correlation := first["correlation_id"].(string)
	req = httptest.NewRequest(http.MethodGet, "/api/v1/timeline/events?correlation_id="+correlation, nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("correlation status=%d; want %d", rec.Code, http.StatusOK)
	}
	var filtered map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &filtered); err != nil {
		t.Fatalf("filtered unmarshal: %v", err)
	}
	if int(filtered["count"].(float64)) < 1 {
		t.Fatalf("filtered count=%v; want >=1", filtered["count"])
	}
}

func TestTimelineEventsEndpoint_InvalidSince(t *testing.T) {
	h := NewHandler(Options{
		ListRegistry: func() []RegistryDevice { return []RegistryDevice{} },
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/timeline/events?since=invalid", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestProvenanceEventsEndpoint(t *testing.T) {
	h := NewHandler(Options{
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720"}}
		},
	})

	streamReq := httptest.NewRequest(http.MethodGet, "/api/v1/stream?layers=registry&interval_ms=10&max_events_per_second=10&max_events=1", nil)
	ctx, cancel := context.WithTimeout(streamReq.Context(), time.Second)
	defer cancel()
	streamReq = streamReq.WithContext(ctx)
	streamRec := httptest.NewRecorder()
	h.ServeHTTP(streamRec, streamReq)
	if streamRec.Code != http.StatusOK {
		t.Fatalf("stream status=%d; want %d", streamRec.Code, http.StatusOK)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/provenance/events?limit=5&layer=composite", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(payload["count"].(float64)) < 1 {
		t.Fatalf("count=%v; want >=1", payload["count"])
	}
	items := payload["items"].([]any)
	first := items[0].(map[string]any)
	if first["layer"] != "composite" {
		t.Fatalf("layer=%v; want composite", first["layer"])
	}
	if first["source"] == "" {
		t.Fatalf("source missing")
	}
	if first["confidence"] == nil {
		t.Fatalf("confidence missing")
	}
}

func TestSnapshotsCaptureAndRetention(t *testing.T) {
	h := NewHandler(Options{
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720"}}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/retention?max_snapshots=2", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("retention status=%d; want %d", rec.Code, http.StatusOK)
	}

	for _, label := range []string{"a", "b", "c"} {
		req = httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/capture?label="+label, nil)
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("capture status=%d; want %d", rec.Code, http.StatusOK)
		}
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/snapshots?limit=10", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status=%d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(payload["stored_count"].(float64)) != 2 {
		t.Fatalf("stored_count=%v; want 2", payload["stored_count"])
	}
	items := payload["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("len(items)=%d; want 2", len(items))
	}
	first := items[0].(map[string]any)
	if first["label"] != "c" {
		t.Fatalf("first.label=%v; want c newest first", first["label"])
	}
}

func TestSnapshotsCaptureUnavailableWithoutProviders(t *testing.T) {
	h := NewHandler(Options{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/capture", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestSnapshotsDiff_DefaultLatestPair(t *testing.T) {
	h := NewHandler(Options{
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720"}}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/capture?label=first", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first capture status=%d; want %d", rec.Code, http.StatusOK)
	}

	// Change upstream payload to ensure a detectable diff.
	h.(*handler).opts.ListRegistry = func() []RegistryDevice {
		return []RegistryDevice{{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720B"}}
	}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/capture?label=second", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("second capture status=%d; want %d", rec.Code, http.StatusOK)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/diff", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("diff status=%d; want %d", rec.Code, http.StatusOK)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(payload["change_count"].(float64)) < 1 {
		t.Fatalf("change_count=%v; want >=1", payload["change_count"])
	}
	items := payload["items"].([]any)
	if len(items) < 1 {
		t.Fatalf("len(items)=%d; want >=1", len(items))
	}
}

func TestSnapshotsDiff_WithExplicitIDs(t *testing.T) {
	h := NewHandler(Options{
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720"}}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/capture?label=a", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var aPayload map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &aPayload)
	aID := aPayload["snapshot"].(map[string]any)["id"].(string)

	h.(*handler).opts.ListRegistry = func() []RegistryDevice {
		return []RegistryDevice{{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720X"}}
	}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/capture?label=b", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var bPayload map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &bPayload)
	bID := bPayload["snapshot"].(map[string]any)["id"].(string)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/diff?from_id="+aID+"&to_id="+bID+"&limit=5", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusOK)
	}
}

func TestSnapshotsDiff_ValidationErrors(t *testing.T) {
	h := NewHandler(Options{
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720"}}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/diff", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d; want %d when snapshots missing", rec.Code, http.StatusNotFound)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/capture?label=only-one", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/diff?from_id=snap-1", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d; want %d for partial id params", rec.Code, http.StatusBadRequest)
	}
}

func TestSessionsSaveLoadAndList(t *testing.T) {
	h := NewHandler(Options{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/save?name=investigation-a&search_query=service&timeline_correlation=reg-1&snapshot_from_id=snap-1&snapshot_to_id=snap-2", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("save status=%d; want %d", rec.Code, http.StatusOK)
	}
	var savePayload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &savePayload); err != nil {
		t.Fatalf("save unmarshal: %v", err)
	}
	session := savePayload["session"].(map[string]any)
	id := session["id"].(string)
	if id == "" {
		t.Fatalf("saved session id missing")
	}
	if session["name"] != "investigation-a" {
		t.Fatalf("session.name=%v; want investigation-a", session["name"])
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/sessions?limit=10", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status=%d; want %d", rec.Code, http.StatusOK)
	}
	var listPayload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("list unmarshal: %v", err)
	}
	if int(listPayload["count"].(float64)) < 1 {
		t.Fatalf("list count=%v; want >=1", listPayload["count"])
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/sessions/load?id="+id, nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("load status=%d; want %d", rec.Code, http.StatusOK)
	}
	var loadPayload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &loadPayload); err != nil {
		t.Fatalf("load unmarshal: %v", err)
	}
	loaded := loadPayload["session"].(map[string]any)
	if loaded["id"] != id {
		t.Fatalf("loaded id=%v; want %s", loaded["id"], id)
	}
}

func TestSessionsLoadValidation(t *testing.T) {
	h := NewHandler(Options{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/load", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d; want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestIssueDraftAndExportEndpoints(t *testing.T) {
	h := NewHandler(Options{
		GatewayVersion: "test-v",
		BuildID:        "test-b",
		ListRegistry: func() []RegistryDevice {
			return []RegistryDevice{{Address: 0x10, Manufacturer: "Vaillant", DeviceID: "VRC720"}}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/issues/draft?title=Draft+Title&observation=Obs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("draft status=%d; want %d", rec.Code, http.StatusOK)
	}
	var draft map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &draft); err != nil {
		t.Fatalf("draft unmarshal: %v", err)
	}
	if draft["title"] != "Draft Title" {
		t.Fatalf("title=%v; want Draft Title", draft["title"])
	}
	if !strings.Contains(draft["markdown"].(string), "## 1) Context") {
		t.Fatalf("markdown missing context section")
	}
	if _, ok := draft["evidence"].(map[string]any); !ok {
		t.Fatalf("evidence missing or invalid")
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/issues/export?title=Draft+Title", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("export status=%d; want %d", rec.Code, http.StatusOK)
	}
	var bundle map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &bundle); err != nil {
		t.Fatalf("export unmarshal: %v", err)
	}
	if bundle["format_version"] != "helianthus-issue-bundle/v1" {
		t.Fatalf("format_version=%v", bundle["format_version"])
	}
	if bundle["filename_hint"] == "" {
		t.Fatalf("filename_hint missing")
	}
}

func TestVRCExplorerDeprecationEndpointRemoved(t *testing.T) {
	h := NewHandler(Options{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/deprecation/vrc-explorer", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d; want %d (endpoint should be removed)", rec.Code, http.StatusNotFound)
	}
}

func TestClassifyRoute_EbusStandardServices(t *testing.T) {
	if got := classifyRoute("/api/v1/ebus-standard/services"); got != "api.ebus_standard.services" {
		t.Fatalf("classifyRoute(services) = %q; want api.ebus_standard.services", got)
	}
}

func TestClassifyRoute_EbusStandardCommands(t *testing.T) {
	if got := classifyRoute("/api/v1/ebus-standard/commands"); got != "api.ebus_standard.commands" {
		t.Fatalf("classifyRoute(commands) = %q; want api.ebus_standard.commands", got)
	}
	if got := classifyRoute("/api/v1/ebus-standard/commands?pb=5"); got != "api.ebus_standard.commands" {
		t.Fatalf("classifyRoute(commands?pb=5) = %q; want api.ebus_standard.commands", got)
	}
}

func TestClassifyRoute_EbusStandardCommandGet(t *testing.T) {
	if got := classifyRoute("/api/v1/ebus-standard/command"); got != "api.ebus_standard.command" {
		t.Fatalf("classifyRoute(command) = %q; want api.ebus_standard.command", got)
	}
	if got := classifyRoute("/api/v1/ebus-standard/command?id=cmd.future"); got != "api.ebus_standard.command" {
		t.Fatalf("classifyRoute(command?id=...) = %q; want api.ebus_standard.command", got)
	}
}

func TestClassifyRoute_EbusStandardDecode(t *testing.T) {
	if got := classifyRoute("/api/v1/ebus-standard/decode"); got != "api.ebus_standard.decode" {
		t.Fatalf("classifyRoute(decode) = %q; want api.ebus_standard.decode", got)
	}
}

func TestClassifyRoute_EbusStandardNotStatic(t *testing.T) {
	paths := []string{
		"/api/v1/ebus-standard/services",
		"/api/v1/ebus-standard/commands",
		"/api/v1/ebus-standard/command",
		"/api/v1/ebus-standard/decode",
		"/api/v1/ebus-standard/future-endpoint",
	}
	for _, p := range paths {
		if got := classifyRoute(p); got == "static" {
			t.Fatalf("classifyRoute(%q) = static; want api.ebus_standard.*", p)
		}
	}
}

func stringPtr(value string) *string {
	v := value
	return &v
}

func intPtr(value int) *int {
	v := value
	return &v
}

// M8_F7_VaillantRESTShimReturns404 — F7 cruise-consult decision = B
// (GraphQL-only). matrix/M6a-vaillant-b503.md §10 records the rationale.
// Any /api/v1/vaillant/* request MUST return 404 (not 405, not 200) so
// the GraphQL-only contract is mechanically asserted.
func TestM8_F7_VaillantRESTShim_Returns404(t *testing.T) {
	h := NewHandler(Options{})
	paths := []string{
		"/api/v1/vaillant/errors",
		"/api/v1/vaillant/service/current",
		"/api/v1/vaillant/live-monitor",
		"/api/v1/vaillant/foo/bar",
	}
	for _, p := range paths {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("GET %s status=%d; want 404 (F7 GraphQL-only decision per matrix §10)",
				p, rec.Code)
		}
	}
}
