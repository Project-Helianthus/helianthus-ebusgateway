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
	registerHTTPPortalRoutes(portalMux, "/portal", portal.Options{Config: gatewayPortalConfigCallbacks(store)}, nil)
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
