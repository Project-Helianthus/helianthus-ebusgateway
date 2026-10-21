package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func TestHTTPControlPlaneRouteManifestIsDeterministic(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.GraphQLPath = "/graphql"
	cfg.SnapshotPath = "/snapshot"
	cfg.SubscriptionPath = "/subscription"
	cfg.MCPPath = "/mcp"
	cfg.MetricsPath = "/metrics"
	cfg.DumpUploadPath = "dump"
	cfg.UIPath = "/ui"
	cfg.PortalPath = "/portal"

	want := []string{
		"/metrics",
		"/debug/vars",
		"/debug/v8/admin-events",
		"/graphql",
		"/snapshot",
		"/subscription",
		"/mcp",
		"/graphql/portal/v1",
		"/admin/eebus/v1/",
		"/dump",
		"/ui/",
		"/ui",
		"/portal/",
		"/portal",
	}
	if got := httpControlPlaneRouteManifest(cfg, true); !reflect.DeepEqual(got, want) {
		t.Fatalf("route manifest = %#v; want %#v", got, want)
	}
}

func TestHTTPControlPlaneRouteManifestOmitsDisabledOptionalRoutes(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.GraphQLPath = "/graphql"
	cfg.SnapshotPath = "/snapshot"
	cfg.SubscriptionPath = "/subscription"
	cfg.MCPPath = "/mcp"
	cfg.DumpUploadPath = ""
	cfg.UIPath = ""
	cfg.PortalPath = ""

	want := []string{
		"/debug/vars",
		"/debug/v8/admin-events",
		"/graphql",
		"/snapshot",
		"/subscription",
		"/mcp",
		"/graphql/portal/v1",
		"/admin/eebus/v1/",
	}
	if got := httpControlPlaneRouteManifest(cfg, false); !reflect.DeepEqual(got, want) {
		t.Fatalf("route manifest = %#v; want %#v", got, want)
	}
}

func TestPortalCatalogRouteIsReservedAgainstConfigurableRoutes(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.GraphQLPath = "/graphql/portal/v1"
	if err := validatePortalCatalogRoute(cfg); err == nil {
		t.Fatal("expected reserved route collision")
	}
	cfg.GraphQLPath = "/graphql"
	if err := validatePortalCatalogRoute(cfg); err != nil {
		t.Fatal(err)
	}
	for _, set := range []func(*ebusgateway.Config){
		func(c *ebusgateway.Config) { c.UIPath = "graphql/portal/v1" },
		func(c *ebusgateway.Config) { c.DumpUploadPath = "graphql/portal/v1" },
	} {
		candidate := ebusgateway.DefaultConfig()
		set(&candidate)
		if err := validatePortalCatalogRoute(candidate); err == nil {
			t.Fatal("normalized route collision accepted")
		}
	}
}

func TestMainStartsControlPlaneBeforeWarmupAndRetiresItInLIFOOrder(t *testing.T) {
	source, err := os.ReadFile("gateway_run_lifecycle.go")
	if err != nil {
		t.Fatalf("read gateway_run_lifecycle.go: %v", err)
	}
	text := string(source)

	start := strings.Index(text, "server, advertiser, err := startHTTPServerFn(")
	warmup := strings.Index(text, "control.BindTasks(ctx, generationTasks")
	if start < 0 || warmup < 0 || start > warmup || strings.Contains(text, "WaitRunning(") {
		t.Fatalf("control-plane start must precede the generation-owned post-start tasks, with no WaitRunning gate: start=%d tasks=%d", start, warmup)
	}

	last := -1
	for _, closeCall := range []string{
		"listener.Close()",
		"deduplicator.Close()",
		"reconstructor.Close()",
		"busObservability.Close()",
		"advertiser.Close()",
		"shutdownHTTPControlPlane(server)",
	} {
		next := strings.Index(text, closeCall)
		if next < 0 || next < last {
			t.Fatalf("teardown call %q is absent or out of order", closeCall)
		}
		last = next
	}
}

func TestRunOrchestrationKeepsConfigurationAndStartupSignalsDistinct(t *testing.T) {
	mainSource, err := os.ReadFile("gateway_run_lifecycle.go")
	if err != nil {
		t.Fatalf("read gateway_run_lifecycle.go: %v", err)
	}
	mainText := string(mainSource)
	config := strings.Index(mainText, "prepareGatewayRunConfig(&cfg)")
	firstSidecar := strings.Index(mainText, "openSynchronizedEvidenceRuntime")
	if config < 0 || firstSidecar < 0 || config > firstSidecar {
		t.Fatalf("configuration phase must remain before resource startup: config=%d sidecar=%d", config, firstSidecar)
	}

	signalSource, err := os.ReadFile("startup_scan.go")
	if err != nil {
		t.Fatalf("read startup_scan.go: %v", err)
	}
	for _, signal := range []string{
		"firstPassDone", "semanticBootstrapReady", "activeProbePassed", "admissionFailed",
	} {
		if !strings.Contains(string(signalSource), signal) {
			t.Fatalf("startup signal %q is not distinct", signal)
		}
	}
}

func TestRunOrchestrationKeepsDriverPreparationBeforeShutdownAndStart(t *testing.T) {
	source, err := os.ReadFile("gateway_run_lifecycle.go")
	if err != nil {
		t.Fatalf("read gateway_run_lifecycle.go: %v", err)
	}
	text := string(source)

	prepare := strings.Index(text, "prepareGatewayEBusDriver(&cfg, sidecars.driverConfigs()...)")
	shutdown := strings.Index(text, "ebusDriver.Shutdown(stopCtx)")
	start := strings.Index(text, "startGatewayRuntimeControl(ctx")
	if prepare < 0 || shutdown < 0 || start < 0 || prepare > shutdown || shutdown > start || strings.Contains(text, "ebusDriver.Start(") {
		t.Fatalf("driver preparation, LIFO shutdown defer, and single-owner control start order changed: prepare=%d shutdown=%d start=%d", prepare, shutdown, start)
	}
}

func TestRunOrchestrationKeepsDeferredShutdownStackInAcquisitionOrder(t *testing.T) {
	source, err := os.ReadFile("gateway_run_lifecycle.go")
	if err != nil {
		t.Fatalf("read gateway_run_lifecycle.go: %v", err)
	}
	text := string(source)

	last := -1
	for _, closeCall := range []string{
		"evidenceRuntime.Close()",
		"runtimeStateMgr.Stop(stopCtx)",
		"ebusDriver.Shutdown(stopCtx)",
		"control.CloseAdmission(closeCtx)",
		"control.CloseNative(closeCtx)",
		"teslaHSCRetained.Close()",
		"m2mRuntime.Close()",
		"artifactBuilder.EmitToFile(artifactPath)",
		"gateway.Close()",
		"shutdownHTTPControlPlane(server)",
	} {
		next := strings.Index(text, closeCall)
		if next < 0 || next < last {
			t.Fatalf("defer acquisition order changed at %q: previous=%d current=%d", closeCall, last, next)
		}
		last = next
	}
}

func TestRunOrchestrationKeepsFlagsConfigurationAdmissionAndStartupBarriersOrdered(t *testing.T) {
	mainSource, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	lifecycleSource, err := os.ReadFile("gateway_run_lifecycle.go")
	if err != nil {
		t.Fatalf("read gateway_run_lifecycle.go: %v", err)
	}
	mainText := string(mainSource)
	lifecycleText := string(lifecycleSource)

	assertOrder := func(text string, labels ...string) {
		t.Helper()
		last := -1
		for _, label := range labels {
			next := strings.Index(text, label)
			if next < 0 || next < last {
				t.Fatalf("orchestration order changed at %q: previous=%d current=%d", label, last, next)
			}
			last = next
		}
	}

	assertOrder(mainText, "flag.Parse()", "applyTransportSourcePolicy(&cfg)", "run(ctx, cfg)")
	assertOrder(lifecycleText,
		"prepareGatewayRunConfig(&cfg)",
		"prepareGatewayEBusDriver(&cfg, sidecars.driverConfigs()...)",
		"startGatewayRuntimeControl(ctx",
		"ResolveAdmissionPath(cfg.TransportConfig.Protocol)",
	)
	assertOrder(lifecycleText,
		"semanticBarrier = make(chan struct{})",
		"startDiscoveryScanLoopFn(genCtx, startupCfg, gateway, builder, adapterClassifier)",
		"startBackgroundFullScanFn(runCtx, startupCfg, gateway, builder, startupScanSignals.semanticBootstrapReady)",
	)
	assertOrder(lifecycleText, "insertSubscribeOnce sync.Once", "insertSubscribeOnce.Do(func()")
}

func TestHTTPPortalBootstrapMatchesMountedCatalog(t *testing.T) {
	for _, mount := range []string{"/portal", "/console/portal"} {
		t.Run(mount, func(t *testing.T) {
			mux := http.NewServeMux()
			contributions, err := newGatewayPortalCatalogContributions()
			if err != nil {
				t.Fatal(err)
			}
			source := newGatewayPortalCatalogSource(nil, nil, nil, contributions)
			registerHTTPPortalRoutes(mux, mount, portal.Options{}, source)
			res := httptest.NewRecorder()
			mux.ServeHTTP(res, httptest.NewRequest(http.MethodGet, mount+"/api/v1/bootstrap", nil))
			if res.Code != http.StatusOK {
				t.Fatalf("bootstrap status %d: %s", res.Code, res.Body.String())
			}
			var bootstrap struct {
				Read struct {
					BasePath    string   `json:"base_path"`
					Unavailable []string `json:"unavailable_operations"`
				} `json:"portal_read"`
				Endpoints map[string]string `json:"endpoints"`
			}
			if err := json.Unmarshal(res.Body.Bytes(), &bootstrap); err != nil {
				t.Fatal(err)
			}
			if bootstrap.Read.BasePath != mount+"/" {
				t.Fatalf("base_path = %q", bootstrap.Read.BasePath)
			}
			if !reflect.DeepEqual(bootstrap.Read.Unavailable, []string{"semanticSnapshot"}) {
				t.Fatalf("unavailable_operations = %v", bootstrap.Read.Unavailable)
			}
			catalog := httptest.NewRecorder()
			mux.ServeHTTP(catalog, httptest.NewRequest(http.MethodPost, bootstrap.Endpoints["portal_catalog"], strings.NewReader(`{"operationName":"PortalCatalogV1"}`)))
			if catalog.Code != http.StatusOK {
				t.Fatalf("catalog status %d: %s", catalog.Code, catalog.Body.String())
			}
			var result struct {
				Data struct {
					Catalog json.RawMessage `json:"portalCatalogV1"`
				} `json:"data"`
				Errors json.RawMessage `json:"errors"`
			}
			if err := json.Unmarshal(catalog.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if len(result.Data.Catalog) == 0 || len(result.Errors) != 0 {
				t.Fatalf("catalog response: %s", catalog.Body.String())
			}
		})
	}
}
