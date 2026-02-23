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
	if endpoints["timeline"] != "/portal/api/v1/timeline/events" {
		t.Fatalf("timeline endpoint=%v; want /portal/api/v1/timeline/events", endpoints["timeline"])
	}
	if endpoints["provenance"] != "/portal/api/v1/provenance/events" {
		t.Fatalf("provenance endpoint=%v; want /portal/api/v1/provenance/events", endpoints["provenance"])
	}
	capabilities := payload["capabilities"].(map[string]any)
	if capabilities["registry"] != true {
		t.Fatalf("capabilities.registry=%v; want true", capabilities["registry"])
	}
	if capabilities["semantic"] != true {
		t.Fatalf("capabilities.semantic=%v; want true", capabilities["semantic"])
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
	h := NewHandler(Options{
		ListSemantic: func() SemanticSnapshot {
			return SemanticSnapshot{
				Zones: []SemanticZone{
					{ID: "zone_1", Name: "Living", CurrentTempC: &current},
				},
				DHW: &SemanticDHW{
					OperatingMode: "auto",
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
					{ID: "zone_1", Name: "Living", OperatingMode: "auto", CurrentTempC: &current},
				},
				DHW: &SemanticDHW{OperatingMode: "auto"},
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
	if !strings.Contains(body, "\"layer\":\"registry\"") {
		t.Fatalf("stream payload missing registry layer: %q", body)
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

	req := httptest.NewRequest(http.MethodGet, "/api/v1/timeline/events?limit=10&layer=registry", nil)
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
	if first["layer"] != "registry" {
		t.Fatalf("layer=%v; want registry", first["layer"])
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

	req := httptest.NewRequest(http.MethodGet, "/api/v1/provenance/events?limit=5&layer=registry", nil)
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
	if first["layer"] != "registry" {
		t.Fatalf("layer=%v; want registry", first["layer"])
	}
	if first["source"] == "" {
		t.Fatalf("source missing")
	}
	if first["confidence"] == nil {
		t.Fatalf("confidence missing")
	}
}
