package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	"github.com/gorilla/websocket"
)

type portalConfigHijackRecorder struct {
	*httptest.ResponseRecorder
	conn net.Conn
}

func (w *portalConfigHijackRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}

func TestHTTPControlPlanePreservesResponseFlushing(t *testing.T) {
	recorder := httptest.NewRecorder()
	handler := &httpControlPlaneHandler{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.(http.Flusher).Flush()
		}),
	}
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/subscription", nil))
	if !recorder.Flushed {
		t.Fatal("response wrapper did not flush the underlying writer")
	}
}

func TestPortalConfigHijackRetirementOffline(t *testing.T) {
	for _, hijackOK := range []bool{true, false} {
		t.Run(fmt.Sprintf("hijack_success_%t", hijackOK), func(t *testing.T) {
			cfg := ebusgateway.DefaultConfig()
			cfg.PortalConfigRoot = t.TempDir()
			store := openGatewayPortalConfig(cfg, nil)
			defer func() { _ = store.Close() }()
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			defer close(release)
			handler := &httpControlPlaneHandler{
				closeResources: func() { _ = store.Close() },
				Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					_, _, err := w.(http.Hijacker).Hijack()
					if (err == nil) != hijackOK {
						t.Errorf("Hijack error = %v; success wanted %t", err, hijackOK)
					}
					close(entered)
					<-release
				}),
			}
			var writer http.ResponseWriter = httptest.NewRecorder()
			if hijackOK {
				conn, peer := net.Pipe()
				defer func() { _ = conn.Close() }()
				defer func() { _ = peer.Close() }()
				writer = &portalConfigHijackRecorder{ResponseRecorder: httptest.NewRecorder(), conn: conn}
			}
			go func() {
				defer close(done)
				handler.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/subscription", nil))
			}()
			<-entered
			handler.retire()
			reopened := openGatewayPortalConfig(cfg, nil)
			_, err := reopened.Read()
			_ = reopened.Close()
			if (err == nil) != hijackOK {
				t.Fatalf("replacement store error = %v; available wanted %t", err, hijackOK)
			}
			rejected := httptest.NewRecorder()
			handler.ServeHTTP(rejected, httptest.NewRequest(http.MethodGet, "/subscription", nil))
			if rejected.Code != http.StatusServiceUnavailable {
				t.Fatalf("retired admission status = %d", rejected.Code)
			}
			release <- struct{}{}
			<-done // The deferred release after hijack must not decrement twice.
			handler.mu.Lock()
			active := handler.active
			handler.mu.Unlock()
			if active != 0 {
				t.Fatalf("active requests after handler return = %d", active)
			}
			reopened = openGatewayPortalConfig(cfg, nil)
			defer func() { _ = reopened.Close() }()
			if _, err := reopened.Read(); err != nil {
				t.Fatalf("writer retained after handler return: %v", err)
			}
		})
	}
}

func TestPortalConfigShutdownReleasesWriterWithOpenWebSocket(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.PortalConfigRoot = t.TempDir()
	cfg.HTTPAddr = "127.0.0.1:0"
	store := openGatewayPortalConfig(cfg, nil)
	defer func() { _ = store.Close() }()
	done := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/subscription", func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, _, _ = conn.ReadMessage()
	})
	server, _, err := startHTTPControlPlaneListener(context.Background(), cfg, mux, nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+server.Addr+"/subscription", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := shutdownHTTPControlPlane(server); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("WebSocket handler returned before the peer disconnected")
	default:
	}
	reopened := openGatewayPortalConfig(cfg, nil)
	defer func() { _ = reopened.Close() }()
	if _, err := reopened.Read(); err != nil {
		t.Fatalf("open WebSocket retained writer lock: %v", err)
	}
	_ = conn.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WebSocket handler did not exit after peer disconnect")
	}
}

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
			cfg.HTTPAddr = net.JoinHostPort(net.IPv6loopback.String(), "8080")
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
				registerHTTPPortalRoutes(mux, "/portal", portal.Options{Config: gatewayPortalConfigCallbacks(store, cfg)}, nil)
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

func TestPortalConfigShutdownRetainsWriterUntilRequestDrains(t *testing.T) {
	testPortalConfigRetainsWriterUntilRequestDrains(t, true)
}

func TestPortalConfigRetirementRetainsWriterUntilRequestDrainsOffline(t *testing.T) {
	testPortalConfigRetainsWriterUntilRequestDrains(t, false)
}

func testPortalConfigRetainsWriterUntilRequestDrains(t *testing.T, listen bool) {
	cfg := ebusgateway.DefaultConfig()
	cfg.PortalConfigRoot = t.TempDir()
	cfg.HTTPAddr = "127.0.0.1:0"
	store := openGatewayPortalConfig(cfg, nil)
	defer func() { _ = store.Close() }()
	record, err := store.Read()
	if err != nil {
		t.Fatal(err)
	}
	record.Sharing.GraphQL = true
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	portalMux := http.NewServeMux()
	registerHTTPPortalRoutes(portalMux, "/portal", portal.Options{Config: gatewayPortalConfigCallbacks(store, cfg)}, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/portal/", func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release // Keep the admitted PUT before body decoding and Store.Update.
		portalMux.ServeHTTP(w, r)
	})
	server := &http.Server{Addr: "gateway.test", Handler: &httpControlPlaneHandler{Handler: mux, closeResources: func() { _ = store.Close() }}}
	if listen {
		server, _, err = startHTTPControlPlaneListener(context.Background(), cfg, mux, nil, nil, nil, store)
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() { _ = server.Close() }()
	defer func() { close(release) }()
	client := &http.Client{Timeout: 5 * time.Second}
	requestDone := make(chan error, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPut, "http://"+server.Addr+"/portal/api/v1/config", bytes.NewReader(body))
		if err == nil {
			var res *http.Response
			if listen {
				res, err = client.Do(req)
			} else {
				recorder := httptest.NewRecorder()
				server.Handler.ServeHTTP(recorder, req)
				res = recorder.Result()
			}
			if err == nil {
				defer func() { _ = res.Body.Close() }()
				if res.StatusCode != http.StatusOK {
					err = fmt.Errorf("PUT status = %d; want %d", res.StatusCode, http.StatusOK)
				} else {
					_, err = io.Copy(io.Discard, res.Body)
				}
			}
		}
		requestDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("PUT did not enter handler")
	}
	shutdownDone := make(chan error, 1)
	handler := server.Handler.(*httpControlPlaneHandler)
	go func() {
		if listen {
			shutdownDone <- shutdownHTTPControlPlane(server)
		} else {
			handler.retire() // Simulate Serve returning while the PUT is active.
			shutdownDone <- nil
		}
	}()
	// Wait for admission retirement while the PUT is still active.
	deadline := time.Now().Add(5 * time.Second)
	for {
		handler.mu.Lock()
		retired := handler.retired
		handler.mu.Unlock()
		if retired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Serve did not retire admission")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := store.Read(); err != nil {
		t.Fatalf("store closed before request drained: %v", err)
	}
	blocked := openGatewayPortalConfig(cfg, nil)
	defer func() { _ = blocked.Close() }()
	if _, err := blocked.Read(); err == nil {
		t.Fatal("writer lock released before request drained")
	}
	release <- struct{}{}
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	reopened := openGatewayPortalConfig(cfg, nil)
	defer func() { _ = reopened.Close() }()
	got, err := reopened.Read()
	if err != nil || got.Revision != "1" || !got.Sharing.GraphQL {
		t.Fatalf("drained PUT was not persisted or writer was not released: %+v, %v", got, err)
	}
}

func TestPortalConfigHostListenerPlan(t *testing.T) {
	endpoint := func(host net.IP, port string) string { return net.JoinHostPort(host.String(), port) }
	for _, mode := range []string{"cohost", "proxy_collision", "wildcard_local", "empty_host", "empty_host_ipv6_collision", "ipv6", "hostname", "ephemeral", "missing_routes", "invalid_zone"} {
		t.Run(mode, func(t *testing.T) {
			cfg := ebusgateway.DefaultConfig()
			cfg.PortalConfigRoot = t.TempDir()
			cfg.HTTPAddr = endpoint(net.IPv6loopback, "8080")
			cfg.ProxyListenAddr = endpoint(net.IPv4zero, "8081")
			wantReason, wantError := "listener_application_pending", ""
			switch mode {
			case "proxy_collision":
				cfg.ProxyListenAddr = cfg.HTTPAddr
				wantReason, wantError = "listener_collision", "listener_conflict"
			case "wildcard_local":
				cfg.HTTPAddr = endpoint(net.IPv4zero, "8080")
			case "empty_host":
				cfg.HTTPAddr = net.JoinHostPort("", "8080")
			case "empty_host_ipv6_collision":
				cfg.HTTPAddr = ":8080"
				cfg.ProxyListenAddr = endpoint(net.IPv6loopback, "8080")
				wantReason, wantError = "listener_collision", "listener_conflict"
			case "ipv6":
				cfg.ProxyListenAddr = endpoint(net.IPv4zero, "8080")
			case "hostname":
				cfg.HTTPAddr = net.JoinHostPort("gateway.test", "8080")
				wantReason = "evidence_unavailable"
			case "ephemeral":
				cfg.HTTPAddr = endpoint(net.IPv6loopback, "0")
				wantReason = "evidence_unavailable"
			case "missing_routes":
				cfg.GraphQLPath, cfg.MCPPath, cfg.ProxyListenAddr = "", "", ""
				wantReason = "evidence_unavailable"
			case "invalid_zone":
				cfg.HTTPAddr = net.JoinHostPort(net.IPv6loopback.String()+"%synthetic", "8080")
				wantReason, wantError = "invalid_binding", "invalid_binding"
			}
			store := openGatewayPortalConfig(cfg, nil)
			defer func() { _ = store.Close() }()
			callbacks := gatewayPortalConfigCallbacks(store, cfg)
			if callbacks.Bindings == nil {
				t.Fatal("gateway omitted host binding callback")
			}
			record, err := store.Read()
			if err != nil {
				t.Fatal(err)
			}
			record.Sharing = portalconfig.Sharing{GraphQL: true, MCP: true}
			record.AdapterProxy = true
			plan := portalconfig.DeriveListenerPlan(record, callbacks.Bindings(context.Background()))
			if plan.Surfaces[1].Reason != wantReason || plan.Surfaces[2].Reason != wantReason {
				t.Fatalf("host plan: %+v", plan)
			}
			if mode == "cohost" || mode == "ipv6" || mode == "wildcard_local" || mode == "empty_host" {
				if len(plan.Conflicts) != 0 || plan.Surfaces[0].Binding == nil || *plan.Surfaces[0].Binding != *plan.Surfaces[1].Binding || *plan.Surfaces[1].Binding != *plan.Surfaces[2].Binding {
					t.Fatalf("HTTP routes did not cohost: %+v", plan)
				}
			}
			if mode == "missing_routes" {
				record.Sharing, record.AdapterProxy = portalconfig.Sharing{}, false
			}
			body, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			handler := portal.NewHandler(portal.Options{Config: callbacks})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/api/v1/config", bytes.NewReader(body)))
			var view struct {
				Error *portalconfig.Error       `json:"error"`
				Plan  portalconfig.ListenerPlan `json:"listener_plan"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
				t.Fatal(err)
			}
			if wantError != "" {
				if response.Code != http.StatusBadRequest || view.Error == nil || view.Error.Code != wantError {
					t.Fatalf("invalid host edit accepted: %d %s", response.Code, response.Body.String())
				}
				accepted, err := store.Read()
				if err != nil || accepted.Revision != "0" || accepted.Sharing.GraphQL || accepted.AdapterProxy {
					t.Fatalf("rejected edit changed history: %+v, %v", accepted, err)
				}
				return
			}
			if response.Code != http.StatusOK || view.Error != nil {
				t.Fatalf("valid host edit rejected: %d %s", response.Code, response.Body.String())
			}
			response = httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/config", nil))
			if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil || response.Code != http.StatusOK {
				t.Fatalf("GET plan failed: %v %s", err, response.Body.String())
			}
			for _, surface := range view.Plan.Surfaces {
				if surface.Effective {
					t.Fatal("host configuration claimed runtime application", surface)
				}
			}
		})
	}
}

func TestPortalConfigDefaultHTTPBindingPUT(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.PortalConfigRoot = t.TempDir()
	registry := runtimecontrol.NewRegistry(nil)
	descriptor, err := newEBusPrimaryDescriptor(runtimecontrol.NativeContractRef{Owner: "example.org/native", Contract: "synthetic", Version: "synthetic"}, ctlSource, newEBusPrimaryLifecycleAdapter(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(descriptor); err != nil {
		t.Fatal(err)
	}
	store := openGatewayPortalConfig(cfg, registry)
	defer func() { _ = store.Close() }()
	handler := portal.NewHandler(portal.Options{Config: gatewayPortalConfigCallbacks(store, cfg)})
	record, err := store.Read()
	if err != nil {
		t.Fatal(err)
	}
	for _, edit := range []string{"driver_only", "enable_sharing", "disable_all_sharing"} {
		switch edit {
		case "driver_only":
			record.Drivers[0].Enabled = false
		case "enable_sharing":
			record.Sharing = portalconfig.Sharing{GraphQL: true, MCP: true}
		case "disable_all_sharing":
			record.Sharing = portalconfig.Sharing{}
		}
		body, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/api/v1/config", bytes.NewReader(body)))
		if response.Code != http.StatusOK {
			t.Fatalf("%s PUT: %d %s", edit, response.Code, response.Body.String())
		}
		var view struct {
			Desired portalconfig.Record       `json:"desired"`
			Plan    portalconfig.ListenerPlan `json:"listener_plan"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
			t.Fatal(err)
		}
		local := view.Plan.Surfaces[0]
		if local.Binding == nil || !local.Binding.Address.IsUnspecified() || local.Binding.Port != 8080 || local.Scope != "shared" || local.Reason != "listener_application_pending" {
			t.Fatalf("%s local plan: %+v", edit, local)
		}
		accepted, err := store.Read()
		if err != nil || accepted.Revision == record.Revision || accepted.Drivers[0].Enabled || accepted.Sharing != record.Sharing || view.Desired.Revision != accepted.Revision {
			t.Fatalf("%s persistence: %+v, %v", edit, accepted, err)
		}
		record = accepted
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openGatewayPortalConfig(cfg, registry)
	defer func() { _ = reopened.Close() }()
	accepted, err := reopened.Read()
	if err != nil || accepted.Revision != record.Revision || accepted.Drivers[0].Enabled || accepted.Sharing != (portalconfig.Sharing{}) {
		t.Fatalf("restart persistence: %+v, %v", accepted, err)
	}
}

func TestPortalConfigExplicitHTTPAddress(t *testing.T) {
	for _, address := range []string{"0.0.0.0", "::", "192.0.2.1"} {
		for _, collision := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/collision_%t", address, collision), func(t *testing.T) {
				cfg := ebusgateway.DefaultConfig()
				cfg.PortalConfigRoot = t.TempDir()
				cfg.HTTPAddr = net.JoinHostPort(address, "8080")
				cfg.ProxyListenAddr = net.JoinHostPort(address, "8081")
				if collision {
					cfg.ProxyListenAddr = cfg.HTTPAddr
				}
				store := openGatewayPortalConfig(cfg, nil)
				defer func() { _ = store.Close() }()
				callbacks := gatewayPortalConfigCallbacks(store, cfg)
				record, err := store.Read()
				if err != nil {
					t.Fatal(err)
				}
				record.Sharing = portalconfig.Sharing{GraphQL: true, MCP: true}
				record.AdapterProxy = true
				plan := portalconfig.DeriveListenerPlan(record, callbacks.Bindings(context.Background()))
				for _, surface := range plan.Surfaces[:3] {
					if surface.Binding == nil || surface.Binding.Address.String() != address || surface.Binding.Port != 8080 || surface.Scope != "shared" {
						t.Fatalf("explicit address lost: %+v", surface)
					}
				}
				body, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				response := httptest.NewRecorder()
				portal.NewHandler(portal.Options{Config: callbacks}).ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/api/v1/config", bytes.NewReader(body)))
				accepted, err := store.Read()
				if err != nil {
					t.Fatal(err)
				}
				if collision {
					var view struct {
						Error *portalconfig.Error `json:"error"`
					}
					if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
						t.Fatal(err)
					}
					if len(plan.Conflicts) != 3 || response.Code != http.StatusBadRequest || view.Error == nil || view.Error.Code != "listener_conflict" || accepted.Revision != "0" || accepted.Sharing != (portalconfig.Sharing{}) || accepted.AdapterProxy {
						t.Fatalf("collision accepted: %+v, %d %s, %+v", plan, response.Code, response.Body.String(), accepted)
					}
				} else if len(plan.Conflicts) != 0 || response.Code != http.StatusOK || accepted.Revision != "1" || accepted.Sharing != record.Sharing || !accepted.AdapterProxy {
					t.Fatalf("explicit binding rejected: %+v, %d %s, %+v", plan, response.Code, response.Body.String(), accepted)
				}
			})
		}
	}
}

func TestGatewayPortalHTTPAddress(t *testing.T) {
	for _, tc := range []struct {
		endpoint, address string
	}{
		{":8080", "::"},
		{"127.0.0.1:8080", "127.0.0.1"},
		{"[::1]:8080", "::1"},
		{"127.0.0.2:8080", "127.0.0.2"},
		{"0.0.0.0:8080", "0.0.0.0"},
		{"[::]:8080", "::"},
		{"192.0.2.1:8080", "192.0.2.1"},
	} {
		t.Run(tc.endpoint, func(t *testing.T) {
			listenAddress, binding := gatewayPortalHTTPAddress(tc.endpoint)
			if listenAddress != tc.endpoint || !binding.Available || binding.Owner != "http" || binding.Address.String() != tc.address || binding.Port != 8080 {
				t.Fatalf("HTTP address mapping: %q, %+v", listenAddress, binding)
			}
			cfg := ebusgateway.DefaultConfig()
			cfg.HTTPAddr = tc.endpoint
			callbacks := gatewayPortalConfigCallbacks(nil, cfg)
			a := callbacks.Bindings(context.Background())
			if a.Local != binding || a.GraphQL != binding || a.MCP != binding {
				t.Fatalf("HTTP plan differs from shared mapping: %+v", a)
			}
			plan := portalconfig.DeriveListenerPlan(portalconfig.Record{}, a)
			local := plan.Surfaces[0]
			wantScope := "shared"
			if binding.Address.IsLoopback() {
				wantScope = "local_only"
			}
			if local.Binding == nil || *local.Binding != binding || local.Scope != wantScope || local.Status != "pending" || local.Reason != "listener_application_pending" || local.Effective || len(plan.Conflicts) != 0 {
				t.Fatalf("HTTP local plan: %+v", plan)
			}
		})
	}
}

func TestPortalConfigHTTPListenerBind(t *testing.T) {
	for _, host := range []string{"", "127.0.0.1", "::1", "0.0.0.0", "::"} {
		t.Run("host_"+host, func(t *testing.T) {
			cfg := ebusgateway.DefaultConfig()
			cfg.MDNSAdvertise = false
			// Let the OS allocate a port; the configured host has the same bind
			// semantics as the fixed-port plan tested above.
			endpoint := net.JoinHostPort(host, "0")
			cfg.HTTPAddr, _ = gatewayPortalHTTPAddress(endpoint)
			server, _, err := startHTTPControlPlaneListener(context.Background(), cfg, http.NewServeMux(), nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = server.Close() }()
			actualHost, port, err := net.SplitHostPort(server.Addr)
			if err != nil || port == "0" {
				t.Fatalf("actual listener endpoint: %q, %v", server.Addr, err)
			}
			actual := net.ParseIP(actualHost)
			if actual == nil {
				t.Fatalf("actual listener host: %q", actualHost)
			}
			wantLoopback := net.ParseIP(host).IsLoopback()
			if actual.IsLoopback() != wantLoopback || actual.IsUnspecified() == wantLoopback {
				t.Fatalf("configured host %q bound as %q", host, actualHost)
			}
			if wantLoopback && !actual.Equal(net.ParseIP(host)) {
				t.Fatalf("explicit loopback host changed: %q", actualHost)
			}
		})
	}
}
