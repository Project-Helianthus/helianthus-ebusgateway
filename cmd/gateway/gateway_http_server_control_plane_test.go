package main

import (
	"os"
	"reflect"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
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
		"/admin/eebus/v1/",
	}
	if got := httpControlPlaneRouteManifest(cfg, false); !reflect.DeepEqual(got, want) {
		t.Fatalf("route manifest = %#v; want %#v", got, want)
	}
}

func TestMainStartsControlPlaneBeforeWarmupAndRetiresItInLIFOOrder(t *testing.T) {
	source, err := os.ReadFile("gateway_run_lifecycle.go")
	if err != nil {
		t.Fatalf("read gateway_run_lifecycle.go: %v", err)
	}
	text := string(source)

	start := strings.Index(text, "server, advertiser, err := startHTTPServerFn(")
	warmup := strings.Index(text, "ebusDriver.WaitRunning(ctx)")
	if start < 0 || warmup < 0 || start > warmup {
		t.Fatalf("control-plane start must precede WaitRunning warmup: start=%d warmup=%d", start, warmup)
	}

	last := -1
	for _, closeCall := range []string{
		"listener.Close()",
		"deduplicator.Close()",
		"reconstructor.Close()",
		"busObservability.Close()",
		"advertiser.Close()",
		"server.Close()",
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

	prepare := strings.Index(text, "prepareGatewayEBusDriver(&cfg)")
	shutdown := strings.Index(text, "ebusDriver.Shutdown(stopCtx)")
	start := strings.Index(text, "ebusDriver.Start(ctx)")
	if prepare < 0 || shutdown < 0 || start < 0 || prepare > shutdown || shutdown > start {
		t.Fatalf("driver preparation, LIFO shutdown defer, and start order changed: prepare=%d shutdown=%d start=%d", prepare, shutdown, start)
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
		"modbusAdapter.Close()",
		"m2mRuntime.Close()",
		"eebusLifecycle.Shutdown()",
		"runtimeStateMgr.Stop(stopCtx)",
		"ebusDriver.Shutdown(stopCtx)",
		"artifactBuilder.EmitToFile(artifactPath)",
		"gateway.Close()",
		"server.Close()",
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
		"prepareGatewayEBusDriver(&cfg)",
		"ebusDriver.Start(ctx)",
		"ResolveAdmissionPath(cfg.TransportConfig.Protocol)",
	)
	assertOrder(lifecycleText,
		"semanticBarrier = make(chan struct{})",
		"startDiscoveryScanLoopFn(ctx, startupCfg, gateway, builder, adapterClassifier)",
		"go startBackgroundFullScanFn(ctx, startupCfg, gateway, builder, startupScanSignals.semanticBootstrapReady)",
	)
	assertOrder(lifecycleText, "insertSubscribeOnce sync.Once", "insertSubscribeOnce.Do(func()")
}
