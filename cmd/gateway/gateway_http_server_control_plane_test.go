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
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
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
