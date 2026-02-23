package portal

import (
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
	capabilities := payload["capabilities"].(map[string]any)
	if capabilities["registry"] != true {
		t.Fatalf("capabilities.registry=%v; want true", capabilities["registry"])
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
