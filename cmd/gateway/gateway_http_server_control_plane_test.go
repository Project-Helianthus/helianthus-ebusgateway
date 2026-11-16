package main

import (
	"encoding/json"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func TestM06PortalReadinessWithNilLifecycleAndManagedAdapter(t *testing.T) {
	entry := &managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: managedEEBusID, Disposition: runtimecontrol.DispositionEnabled}}
	adapter := &eebusRuntimeAdapter{managed: &managedSidecarBindings{sidecars: &managedSidecars{entries: []*managedSidecar{entry}}}}
	readiness := gatewayPortalReadiness(adapter, nil, func() string { return "READY" })
	want := portal.RuntimeReadiness{
		ProcessReadiness: "READY", HTTPReadiness: "READY", ProxyReadiness: "READY",
		EEBusReadiness: "DEGRADED", EEBusDegradedReason: "RUNTIME_FACTORY_UNAVAILABLE",
	}
	if got := readiness(); got != want {
		t.Fatalf("managed readiness = %+v; want %+v", got, want)
	}
	entry.descriptor.Disposition = runtimecontrol.DispositionDisabled
	want.EEBusReadiness, want.EEBusDegradedReason = "DISABLED", ""
	if got := readiness(); got != want {
		t.Fatalf("updated managed readiness = %+v; want %+v", got, want)
	}
}

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
	_, compositionText := lifecycleCompositionSources(t, token.NewFileSet())
	if start < 0 || warmup < 0 || start > warmup || strings.Contains(compositionText, "WaitRunning(") {
		t.Fatalf("control-plane start must precede the generation-owned post-start tasks, with no WaitRunning gate: start=%d tasks=%d", start, warmup)
	}

	// TestGatewayProcessShellShutdownSequenceThroughLifecycle asserts the
	// resulting release order: generation tasks, then consumers.
	delegate := strings.Index(text, `shell.onShutdownFunc("consumers", consumerShutdown.shutdown)`)
	join := strings.LastIndex(text, "control.CloseAdmission(closeCtx)")
	if delegate < start || join < delegate {
		t.Fatal("consumer shutdown must be registered after HTTP acquisition and before generation join")
	}
	source, err = os.ReadFile("gateway_consumer_shutdown.go")
	if err != nil {
		t.Fatal(err)
	}
	text = string(source)
	last := -1
	for _, closeCall := range []string{
		"s.stopTransportWatch()", "s.publishRetirement()", "s.drainHTTP()",
		`s.close("broadcast listener close", surface.listener)`,
		`s.close("deduplicator close", s.deduplicator)`,
		`s.close("reconstructor close", surface.reconstructor)`,
		`s.close("bus observability close", s.busObservability)`,
		`s.close("mdns close", s.advertiser)`,
	} {
		next := strings.Index(text, closeCall)
		if next < 0 || next < last {
			t.Fatalf("teardown call %q is absent or out of order", closeCall)
		}
		last = next
	}
}

// The configuration phase before resource startup is asserted by
// TestGatewayProcessShellShutdownSequenceThroughLifecycle (run configuration
// is the first acquisition) and by
// TestGatewayProcessShellReleasesOnlyAcquiredResourcesOnFailure (a run
// configuration failure acquires nothing else).
func TestRunOrchestrationKeepsStartupSignalsDistinct(t *testing.T) {
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

// The order of driver preparation, its shutdown registration and the
// single-owner control start is asserted by
// TestGatewayProcessShellShutdownSequenceThroughLifecycle
// (acquire:eBUS driver -> register:eBUS driver -> acquire:runtime control).
// The absence of a direct driver start stays a source pin over every
// lifecycle composition file.
func TestRunOrchestrationNeverStartsTheEBusDriverDirectly(t *testing.T) {
	_, text := lifecycleCompositionSources(t, token.NewFileSet())
	if !strings.Contains(text, "prepareGatewayEBusDriver(shell.cfg, sidecars.driverConfigs()...)") || !strings.Contains(text, "startGatewayRuntimeControl(shell.ctx") {
		t.Fatal("the process shell no longer prepares the eBUS driver and starts the runtime control")
	}
	if strings.Contains(text, "ebusDriver.Start(") {
		t.Fatal("the lifecycle starts the eBUS driver directly; the runtime control is its single owner")
	}
}

// The deferred shutdown stack is the process-shell shutdown sequence;
// TestGatewayProcessShellShutdownSequenceThroughLifecycle asserts its complete
// release order. The M2M release is registered by the lifecycle itself between
// the retained EVSE owner and the final source-selection artifact.
func TestRunOrchestrationRegistersTheM2MReleaseInAcquisitionOrder(t *testing.T) {
	source, err := os.ReadFile("gateway_run_lifecycle.go")
	if err != nil {
		t.Fatalf("read gateway_run_lifecycle.go: %v", err)
	}
	text := string(source)
	last := -1
	for _, step := range []string{
		"shell.acquireResources()",
		"m2mRuntime, err := newProfileM2MRuntime(",
		`shell.onShutdown("M2M sidecar"`,
		"m2mRuntime.Close()",
		"shell.acquireEEBusSurfaces()",
		`shell.onShutdownFunc("final source-selection artifact", startupSelection.emitFinalArtifact)`,
	} {
		next := strings.Index(text, step)
		if next < 0 || next < last || strings.Count(text, step) != 1 {
			t.Fatalf("M2M release registration order changed at %q: previous=%d current=%d", step, last, next)
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
	// The run configuration, the driver preparation and the runtime control
	// start are process-shell acquisitions;
	// TestGatewayProcessShellShutdownSequenceThroughLifecycle asserts their
	// order and that every shell acquisition precedes the registration of the
	// final source-selection artifact, which follows the startup source
	// selection.
	assertOrder(lifecycleText,
		"shell.acquireResources()",
		"newStartupSourceSelection(cfg)",
		`shell.onShutdownFunc("final source-selection artifact", startupSelection.emitFinalArtifact)`,
	)
	// The semantic barrier and the once-only inserter subscription are owned
	// by retainedEBusConsumers; TestRetainedEBusConsumersClosesSemanticBarrierOnce,
	// TestRetainedEBusConsumersCreatesBarrierOnlyForDeferredStartupScan and
	// TestRetainedEBusConsumersSubscribesInserterOnceAfterAdmission pin them.
	// The lifecycle creates that owner, then the generation owners, before
	// it binds the generation tasks. The startup discovery scan and the
	// background full scan are methods of ebusGenerationOwners:
	// TestRuntimeControlCharacterizationLifecycleOrderingHealthy pins the
	// semantic poller, the discovery scan and the background full scan in that
	// order, and TestEBusGenerationOwnersTaskGraph pins the task names and
	// their prerequisites.
	assertOrder(lifecycleText,
		"newRetainedEBusConsumers(ctx",
		"retained.openSemanticBarrierOnProcessExit()",
		"newEBusGenerationOwners(ctx",
		"newEBusGenerationTaskRunner(generationOwners.taskConfig())",
		"control.BindTasks(ctx, generationTasks",
	)
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
			if !reflect.DeepEqual(bootstrap.Read.Unavailable, []string{"semantic_snapshot"}) {
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
