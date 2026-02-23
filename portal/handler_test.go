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
}

func TestBootstrapEndpoint(t *testing.T) {
	h := NewHandler(Options{
		GraphQLPath:      "/graphql",
		SnapshotPath:     "/snapshot",
		SubscriptionPath: "/graphql/subscriptions",
		MCPPath:          "/mcp",
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
