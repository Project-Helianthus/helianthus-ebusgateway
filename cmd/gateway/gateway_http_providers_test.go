package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func httpProviderFunction(t *testing.T, file, name string) *ast.FuncDecl {
	t.Helper()
	tree, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range tree.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
			return fn
		}
	}
	t.Fatalf("function %s missing from %s", name, file)
	return nil
}

// Validate call-site ordering within the caller, then inspect the delegate in
// its own AST. Positions from separate files are never compared.
func httpProviderCompositionOwner(t *testing.T) *ast.FuncDecl {
	t.Helper()
	caller := httpProviderFunction(t, "gateway_http_server.go", "startHTTPServer")
	owner := httpProviderFunction(t, "gateway_http_providers.go", "newHTTPControlPlaneProviders")
	calls := map[string][]*ast.CallExpr{}
	ast.Inspect(caller.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			calls[types.ExprString(call.Fun)] = append(calls[types.ExprString(call.Fun)], call)
		}
		return true
	})
	delegated := calls["newHTTPControlPlaneProviders"]
	if len(delegated) != 1 {
		t.Fatalf("provider delegate calls = %d; want 1", len(delegated))
	}
	for _, name := range []string{"registerHTTPControlPlaneCoreRoutes", "configureGatewayPortalSemanticRead", "startHTTPControlPlaneListener"} {
		if len(calls[name]) == 0 {
			t.Fatalf("caller lost %s", name)
		}
		for _, call := range calls[name] {
			if call.Pos() <= delegated[0].End() {
				t.Fatalf("%s precedes completed delegation", name)
			}
		}
	}
	want := []string{"cfg", "gateway", "builder", "hub", "semanticProvider", "eebusProvider", "eebusCommandRouter", "modbusProvider", "busObservability", "watchSummaryProvider", "ebusSourceProvider", "buildInfo", "daemonUpdatesAvailable", "ebusDriver", "runtimeControl"}
	if len(delegated[0].Args) != len(want) {
		t.Fatal("delegate argument count changed")
	}
	for i, arg := range delegated[0].Args {
		if types.ExprString(arg) != want[i] {
			t.Fatalf("delegate argument %d must forward %s", i, want[i])
		}
	}
	found := false
	for i, stmt := range caller.Body.List {
		assign, ok := stmt.(*ast.AssignStmt)
		if !ok || len(assign.Rhs) != 1 || assign.Rhs[0] != delegated[0] {
			continue
		}
		if types.ExprString(assign.Lhs[0]) != "providers" || types.ExprString(assign.Lhs[1]) != "err" {
			t.Fatal("delegate result not retained")
		}
		check, ok := caller.Body.List[i+1].(*ast.IfStmt)
		if !ok || types.ExprString(check.Cond) != "err != nil" || len(check.Body.List) != 1 {
			t.Fatal("delegate failure must return immediately")
		}
		ret, ok := check.Body.List[0].(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 3 || types.ExprString(ret.Results[2]) != "err" {
			t.Fatal("delegate error is not propagated")
		}
		found = true
	}
	if !found {
		t.Fatal("delegate must execute unconditionally after validation")
	}
	return owner
}

func TestHTTPProviderCompositionDelegation(t *testing.T) { httpProviderCompositionOwner(t) }

type httpProviderLeaf struct {
	*eebusRuntimeAdapter
	leaf    mcp.LeafPromotionCapture
	lookups int
}

func (p *httpProviderLeaf) LeafPromotionCapture() mcp.LeafPromotionCapture {
	p.lookups++
	return p.leaf
}

type httpProviderCapture struct{}

func (*httpProviderCapture) CaptureLeafPromotion(context.Context, mcp.LeafPromotionCaptureRequest) mcp.LeafPromotionCaptureReceipt {
	return mcp.LeafPromotionCaptureReceipt{}
}

type httpProviderWatch struct{}

func (*httpProviderWatch) Snapshot() mcp.WatchSummary { return mcp.WatchSummary{} }

func httpProviderTools(t *testing.T, server *mcp.Server) json.RawMessage {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	var response struct {
		Result json.RawMessage
		Error  any
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || response.Error != nil {
		t.Fatalf("tools/list = %d %s", rec.Code, rec.Body.String())
	}
	return response.Result
}

func TestHTTPProviderCompositionAvailability(t *testing.T) {
	profiles := gatewayInventoryProfiles(t)
	// Independent switches cover absent, enabled and partially supplied owners.
	for mask := 0; mask < 256; mask++ {
		t.Run(stringMask(mask), func(t *testing.T) {
			cfg := ebusgateway.DefaultConfig()
			cfg.EvidenceOneShotEnabled = mask&4 != 0
			if mask&128 != 0 {
				cfg.PortalPath = ""
			}
			gw := &ebusgateway.Gateway{Registry: registry.NewDeviceRegistry(nil)}
			builder := graphql.NewBuilder(gw.Registry, nil)
			runtime := &msp06GatewayRuntime{}
			adapter := &eebusRuntimeAdapter{runtime: runtime}
			leaf := &httpProviderLeaf{eebusRuntimeAdapter: adapter}
			if mask&8 != 0 {
				leaf.leaf = &httpProviderCapture{}
			}
			var provider mcp.EEBusV1Provider
			if mask&1 != 0 {
				provider = leaf
			}
			var router mcp.EEBusV1CommandRouter
			if mask&2 != 0 {
				router = eebusMCPCommandRouter(adapter)
			}
			var bus *ebusgateway.BusObservabilityStore
			if mask&16 != 0 {
				bus = ebusgateway.NewBusObservabilityStore(cfg)
			}
			var watch mcp.WatchSummaryProvider
			if mask&32 != 0 {
				watch = &httpProviderWatch{}
			}
			var modbus mcp.ModbusV1Provider
			if mask&64 != 0 {
				modbus = newGatewayModbusMCPProviderWithRuntimes(nil, &growattBMSRS485ProductionProvider{}, nil)
			}
			composed, err := newHTTPControlPlaneProviders(cfg, gw, builder, graphql.NewBroadcastHub(nil), nil, provider, router, modbus, bus, watch, nil, gatewayBuildInfo{ReleaseVersion: "0.7.0"}, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			wanted := map[string]bool{}
			selected := []string{"semantic_disabled", "runtime_disabled"}
			if modbus != nil {
				selected = append(selected, "modbus_production_2")
			}
			if provider != nil {
				selected = append(selected, "eebus_enabled")
			}
			if bus != nil {
				selected = append(selected, "identification_enabled")
			}
			if watch != nil {
				selected = append(selected, "watch_enabled")
			}
			for _, profile := range selected {
				for _, name := range profiles[profile] {
					wanted[name] = true
				}
			}
			want := []string{}
			for name := range wanted {
				want = append(want, name)
			}
			sort.Strings(want)
			if err := gatewayInventoryEqual(gatewayInventoryNames(t, composed.mcpServer), want); err != nil {
				t.Fatal(err)
			}
			raw := httpProviderTools(t, composed.mcpServer)
			if !bytes.Equal(raw, httpProviderTools(t, composed.mcpServer)) {
				t.Fatal("tools/schema listing is nondeterministic")
			}
			if runtime.snapshotCalls != 0 || runtime.featuresCalls != 0 {
				t.Fatal("composition acquired native evidence")
			}
			if leaf.lookups != boolCount(provider != nil) {
				t.Fatalf("leaf lookups = %d", leaf.lookups)
			}
			mux := http.NewServeMux()
			plan := newHTTPControlPlaneRoutePlan(cfg, bus != nil)
			registerHTTPControlPlaneCoreRoutes(mux, plan, "", newGatewayMetricsHandler(bus, composed.semanticMetrics), composed.queryHandler, composed.snapshotHandler, composed.subscriptionHandler, composed.mcpServer, http.NotFoundHandler())
			var opts portal.Options
			configureGatewayPortalSemanticRead(&opts, composed.sharedSemanticProvider)
			registerHTTPPortalRoutes(mux, plan.portalPath, opts, nil)
			for _, path := range []string{cfg.GraphQLPath, cfg.SnapshotPath, cfg.SubscriptionPath, cfg.MCPPath, "/graphql/portal/v1", "/admin/eebus/v1/"} {
				_, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, path, nil))
				if pattern != path {
					t.Fatalf("route %s matched %q", path, pattern)
				}
			}
			_, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, cfg.MetricsPath, nil))
			if (pattern == cfg.MetricsPath) != (bus != nil) {
				t.Fatalf("metrics route = %q with bus=%v", pattern, bus != nil)
			}
			if cfg.PortalPath != "" {
				_, pattern = mux.Handler(httptest.NewRequest(http.MethodGet, cfg.PortalPath+"/", nil))
				if pattern != cfg.PortalPath+"/" {
					t.Fatal("enabled Portal route missing")
				}
			} else {
				_, pattern = mux.Handler(httptest.NewRequest(http.MethodGet, "/portal/", nil))
				if pattern != "" {
					t.Fatalf("disabled Portal route present: %s", pattern)
				}
			}
			if dir := os.Getenv("HTTP_PROVIDER_CHARACTERIZATION_OUTPUT"); dir != "" {
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, stringMask(mask)+".json"), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			// A second registration fails iff composition already installed that owner.
			checks := []struct {
				name       string
				registered bool
				err        error
			}{
				{"provider", provider != nil, composed.mcpServer.RegisterEEBusV1Provider(adapter)},
				{"router", router != nil, composed.mcpServer.RegisterEEBusV1CommandRouter(eebusMCPCommandRouter(adapter))},
				{"leaf", provider != nil && leaf.leaf != nil, composed.mcpServer.RegisterLeafPromotionCapture(&httpProviderCapture{})},
				{"evidence", cfg.EvidenceOneShotEnabled && provider != nil && router != nil, composed.mcpServer.RegisterSynchronizedEvidenceCapture(&synchronizedEvidenceOneShotRuntime{})},
			}
			for _, check := range checks {
				if check.registered {
					if check.err == nil || !strings.Contains(check.err.Error(), "already registered") {
						t.Fatalf("%s duplicate = %v", check.name, check.err)
					}
				} else if check.err != nil {
					t.Fatalf("%s unexpectedly registered: %v", check.name, check.err)
				}
			}
		})
	}
}

func boolCount(v bool) int {
	if v {
		return 1
	}
	return 0
}
func stringMask(mask int) string { return fmt.Sprintf("%03d", mask) }

func TestHTTPProviderCompositionEarlyFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		builder bool
		version string
		want    string
	}{
		{"missing_builder", false, "0.7.0", "graphql builder missing for semantic operation route"},
		{"invalid_identity", true, "", "configure MCP build identity: mcp server version is invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gw := &ebusgateway.Gateway{Registry: registry.NewDeviceRegistry(nil)}
			var builder *graphql.Builder
			if tc.builder {
				builder = graphql.NewBuilder(gw.Registry, nil)
			}
			provider := &httpProviderLeaf{eebusRuntimeAdapter: &eebusRuntimeAdapter{runtime: &msp06GatewayRuntime{}}}
			got, err := newHTTPControlPlaneProviders(ebusgateway.DefaultConfig(), gw, builder, graphql.NewBroadcastHub(nil), nil, provider, nil, nil, nil, nil, nil, gatewayBuildInfo{ReleaseVersion: tc.version}, nil, nil, nil)
			if got != nil || err == nil || err.Error() != tc.want {
				t.Fatalf("composition = %v, %v; want nil, %s", got, err, tc.want)
			}
			if provider.lookups != 0 {
				t.Fatal("registration continued after early failure")
			}
		})
	}
}

type httpProviderHub struct {
	calls int
	err   error
}

func (h *httpProviderHub) AssetIDs() []semreg.AssetID { return nil }
func (h *httpProviderHub) Capture(context.Context, []semreg.AssetID, func() time.Time) (semhub.CoherentCapture, error) {
	h.calls++
	return semhub.CoherentCapture{}, h.err
}

func TestHTTPProviderCompositionSharedProvider(t *testing.T) {
	gw := &ebusgateway.Gateway{Registry: registry.NewDeviceRegistry(nil)}
	builder := graphql.NewBuilder(gw.Registry, nil)
	hubOwner, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := newHTTPControlPlaneProviders(ebusgateway.DefaultConfig(), gw, builder, graphql.NewBroadcastHub(nil), nil, nil, nil, nil, nil, nil, nil, gatewayBuildInfo{ReleaseVersion: "0.7.0"}, nil, nil, &gatewayRuntimeControl{hub: hubOwner})
	if err != nil {
		t.Fatal(err)
	}
	if err := mcp.RegisterSemanticSnapshotTool(got.mcpServer, got.sharedSemanticProvider); err == nil || err.Error() != "semantic snapshot already registered" {
		t.Fatalf("duplicate shared registration = %v", err)
	}
	pointer := reflect.ValueOf(got.sharedSemanticProvider).Pointer()
	for name, field := range map[string]reflect.Value{
		"GraphQL": reflect.ValueOf(builder).Elem().FieldByName("semanticSnapshot"),
		"MCP":     reflect.ValueOf(got.mcpServer).Elem().FieldByName("sharedSemanticProvider"),
	} {
		if field.IsNil() || field.Elem().Pointer() != pointer {
			t.Fatalf("%s uses another provider", name)
		}
	}
	if got.semanticMetrics.provider != got.sharedSemanticProvider {
		t.Fatal("metrics uses another provider")
	}
	if got.sharedSemanticProvider.hub != semanticHubCapture(hubOwner) {
		t.Fatal("shared provider lost runtime-control hub")
	}
	got.sharedSemanticProvider.hub = nil
	if _, _, err := got.sharedSemanticProvider.Capture(context.Background(), nil); !errors.Is(err, semhub.ErrCaptureUnavailable) {
		t.Fatalf("missing hub = %v", err)
	}
	var opts portal.Options
	configureGatewayPortalSemanticRead(&opts, got.sharedSemanticProvider)
	if _, err := opts.SharedSemanticView(context.Background()); !errors.Is(err, semhub.ErrCaptureUnavailable) {
		t.Fatalf("missing Portal hub = %v", err)
	}
	sentinel := errors.New("synthetic capture unavailable")
	hub := &httpProviderHub{err: sentinel}
	got.sharedSemanticProvider.hub = hub
	if _, err := opts.SharedSemanticView(context.Background()); !errors.Is(err, sentinel) || hub.calls != 1 {
		t.Fatalf("Portal did not retain shared provider: %v, calls %d", err, hub.calls)
	}
}

func httpProviderCallerBindings(caller *ast.FuncDecl) (map[string][]*ast.CallExpr, map[string]string) {
	callerCalls := map[string][]*ast.CallExpr{}
	aliases := map[string]string{}
	ast.Inspect(caller.Body, func(node ast.Node) bool {
		if assign, ok := node.(*ast.AssignStmt); ok && len(assign.Lhs) == len(assign.Rhs) {
			for i, rhs := range assign.Rhs {
				aliases[types.ExprString(assign.Lhs[i])] = types.ExprString(rhs)
			}
		}
		if call, ok := node.(*ast.CallExpr); ok {
			callerCalls[types.ExprString(call.Fun)] = append(callerCalls[types.ExprString(call.Fun)], call)
		}
		return true
	})
	return callerCalls, aliases
}
