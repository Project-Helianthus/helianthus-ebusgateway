package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func TestPortalConfigProductionComposition(t *testing.T) { testPortalConfigComposition(t, true) }
func TestPortalConfigOfflineComposition(t *testing.T)    { testPortalConfigComposition(t, false) }

func testPortalConfigComposition(t *testing.T, production bool) {
	for _, broken := range []bool{false, true} {
		name := "round_trip"
		if broken {
			name = "failed_load"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "portal-config.json")
			if broken {
				if err := os.WriteFile(path, []byte(`{"private_marker":true}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var process *rctlProcess
			cfg := ebusgateway.DefaultConfig()
			cfg.PortalConfigRoot = root
			registry := runtimecontrol.NewRegistry(nil)
			descriptor, err := newEBusPrimaryDescriptor(runtimecontrol.NativeContractRef{Owner: "example.org/native", Contract: "synthetic", Version: "synthetic"}, ctlSource, newEBusPrimaryLifecycleAdapter(cfg))
			if err != nil {
				t.Fatal(err)
			}
			if err = registry.Register(descriptor); err != nil {
				t.Fatal(err)
			}
			var handler http.Handler
			var store *portalconfig.Store
			base := "http://gateway.test"
			if production {
				env := newRCtlEnv(t)
				env.seed(true, nil)
				process = env.start(rctlProcessOptions{mcp: true, config: func(c *ebusgateway.Config) { c.PortalConfigRoot = root }})
				if process.mcpURL == "" {
					t.Fatal("production HTTP listener unavailable; see process shutdown error")
				}
				base = strings.TrimSuffix(process.mcpURL, "/mcp")
				registry = process.control.Registry()
			} else {
				store = openGatewayPortalConfig(cfg, registry)
				defer func() { _ = store.Close() }()
				mux := http.NewServeMux()
				registerHTTPPortalRoutes(mux, "/portal", portal.Options{Config: gatewayPortalConfigCallbacks(store)}, nil)
				handler = mux
			}
			request := func(method, route string, body []byte, want int) []byte {
				t.Helper()
				req, err := http.NewRequest(method, base+route, bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				var res *http.Response
				if handler != nil {
					recorder := httptest.NewRecorder()
					handler.ServeHTTP(recorder, req)
					res = recorder.Result()
				} else {
					res, err = http.DefaultClient.Do(req)
					if err != nil {
						t.Fatal(err)
					}
				}
				defer func() { _ = res.Body.Close() }()
				data, err := io.ReadAll(res.Body)
				if err != nil || res.StatusCode != want {
					t.Fatalf("%s %s: status %d, %s, %v", method, route, res.StatusCode, data, err)
				}
				if bytes.Contains(data, []byte("private_marker")) || bytes.Contains(data, []byte(root)) {
					t.Fatal("private storage data leaked")
				}
				return data
			}
			request(http.MethodGet, "/portal/", nil, http.StatusOK)
			request(http.MethodGet, "/portal/api/v1/bootstrap", nil, http.StatusOK)
			var view struct {
				Desired *portalconfig.Record `json:"desired"`
				Error   *portalconfig.Error  `json:"error"`
			}
			status := http.StatusOK
			if broken {
				status = http.StatusBadRequest
			}
			if err := json.Unmarshal(request(http.MethodGet, "/portal/api/v1/config", nil, status), &view); err != nil {
				t.Fatal(err)
			}
			if broken {
				if view.Desired != nil || view.Error == nil || view.Error.Code != "invalid_record" {
					t.Fatalf("failed-load view: %+v", view)
				}
				record := portalconfig.Record{Format: portalconfig.Format, Revision: "0", Drivers: []portalconfig.Driver{{ID: string(primaryEBusDriverID), Enabled: true}}}
				body, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				request(http.MethodPut, "/portal/api/v1/config", body, http.StatusBadRequest)
				data, err := os.ReadFile(path)
				if err != nil || string(data) != `{"private_marker":true}` {
					t.Fatal("failed load overwrote storage", err)
				}
				return
			}
			if view.Desired == nil || len(view.Desired.Drivers) != 1 || view.Desired.Drivers[0].ID != string(primaryEBusDriverID) || !view.Desired.Drivers[0].Enabled || view.Desired.Sharing != (portalconfig.Sharing{}) || view.Desired.AdapterProxy || view.Desired.OutputBindings.Matter {
				t.Fatalf("host seed: %+v", view.Desired)
			}
			view.Desired.Sharing.GraphQL = true
			body, err := json.Marshal(view.Desired)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(request(http.MethodPut, "/portal/api/v1/config", body, http.StatusOK), &view); err != nil {
				t.Fatal(err)
			}
			if view.Desired.Revision != "1" || !view.Desired.Sharing.GraphQL {
				t.Fatalf("saved view: %+v", view.Desired)
			}
			request(http.MethodPut, "/portal/api/v1/config", body, http.StatusConflict)
			// Closing the HTTP listener must release its exclusive writer for restart.
			if process != nil {
				process.stop()
			} else if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := openGatewayPortalConfig(cfg, registry)
			defer func() { _ = reopened.Close() }()
			got, err := reopened.Read()
			if err != nil || got.Revision != "1" || !got.Sharing.GraphQL {
				t.Fatalf("restart: %+v, %v", got, err)
			}
		})
	}
}

func TestPortalConfigHostConstraints(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.PortalConfigRoot = t.TempDir()
	cfg.GraphQLPath = ""
	cfg.MCPPath = ""
	cfg.ProxyListenAddr = ""
	store := openGatewayPortalConfig(cfg, nil)
	defer func() { _ = store.Close() }()
	initial, err := store.Read()
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"driver", "graphql", "mcp", "proxy", "matter"} {
		record := initial
		switch field {
		case "driver":
			record.Drivers = []portalconfig.Driver{{ID: "family.unmanaged", Enabled: true}}
		case "graphql":
			record.Sharing.GraphQL = true
		case "mcp":
			record.Sharing.MCP = true
		case "proxy":
			record.AdapterProxy = true
		case "matter":
			record.OutputBindings.Matter = true
		}
		if _, err := store.Update(record); err == nil || err.Error() != "unsupported_selection" {
			t.Fatalf("%s accepted: %v", field, err)
		}
	}
}

func TestPortalConfigListenerFailureReleasesWriter(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.PortalConfigRoot = t.TempDir()
	cfg.HTTPAddr = "invalid-listen-address"
	store := openGatewayPortalConfig(cfg, nil)
	if _, _, err := startHTTPControlPlaneListener(context.Background(), cfg, http.NewServeMux(), nil, nil, nil, store); err == nil {
		t.Fatal("invalid listener address accepted")
	}
	reopened := openGatewayPortalConfig(cfg, nil)
	defer func() { _ = reopened.Close() }()
	if _, err := reopened.Read(); err != nil {
		t.Fatalf("startup failure retained writer lock: %v", err)
	}
}

func TestPortalConfigDisabledDriverSeed(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.PortalConfigRoot = t.TempDir()
	registry := runtimecontrol.NewRegistry(nil)
	descriptor, err := newEBusPrimaryDescriptor(runtimecontrol.NativeContractRef{Owner: "example.org/native", Contract: "synthetic", Version: "synthetic"}, ctlSource, newEBusPrimaryLifecycleAdapter(cfg))
	if err != nil {
		t.Fatal(err)
	}
	descriptor.Disposition = runtimecontrol.DispositionDisabled
	if err := registry.Register(descriptor); err != nil {
		t.Fatal(err)
	}
	store := openGatewayPortalConfig(cfg, registry)
	defer func() { _ = store.Close() }()
	got, err := store.Read()
	if err != nil || len(got.Drivers) != 1 || got.Drivers[0].Enabled {
		t.Fatalf("disabled host seed: %+v, %v", got, err)
	}
}
