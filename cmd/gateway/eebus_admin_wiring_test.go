package main

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

func TestIssue809BrokenCredentialsDegradeOnlyAdminBoundary(t *testing.T) {
	originalResolver := resolveEEBusInterfaceAddressesFn
	originalFactory := newEEBusOperatorRuntimeFn
	originalRuntimeFactory := newEEBusRuntimeFn
	t.Cleanup(func() {
		resolveEEBusInterfaceAddressesFn = originalResolver
		newEEBusOperatorRuntimeFn = originalFactory
		newEEBusRuntimeFn = originalRuntimeFactory
	})

	runtime := &msp05bRuntime{}
	resolveEEBusInterfaceAddressesFn = func(string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
	}
	newEEBusOperatorRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
		t.Fatal("broken credentials must not start the operator runtime")
		return nil, nil, errors.New("unreachable")
	}
	newEEBusRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		return runtime, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.EEBusConfig = msp05bEnabledConfig()
	cfg.EEBusAdminConfig.Enabled = true
	cfg.EEBusAdminConfig.OwnerOrigin = "http://gateway.test:8080"
	cfg.EEBusAdminConfig.OwnerSecretPath = t.TempDir() + "/missing-owner"
	cfg.EEBusAdminConfig.HASecretPath = t.TempDir() + "/missing-ha"
	adapter, admin, _, available, err := startEEBusAdminAwareRuntime(context.Background(), cfg)
	if err != nil {
		t.Fatalf("fallback runtime: %v", err)
	}
	if adapter == nil || admin != nil || available {
		t.Fatalf("fallback adapter/admin/available=%v/%v/%v", adapter, admin, available)
	}
	if runtime.startCalls != 1 {
		t.Fatalf("ordinary runtime start=%d, want 1", runtime.startCalls)
	}
	_ = adapter.Shutdown()
}

func TestIssue809GatewayMountsAdminDirectlyAndPortalGetsOnlyEndpointMetadata(t *testing.T) {
	content, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(content)
	for _, required := range []string{
		`mux.Handle("/admin/eebus/v1/", eebusAdminHandler)`,
		`eebusAdminHandler := eebusadmin.NewUnavailableHandler()`,
		`Admin: eebusAdmin, Raw: eebusAdapter, Auth: eebusAdminAuthConfig`,
		`Audit: func(event eebusadmin.AuditEvent)`,
		`EEBusAdminPath: func() string`,
	} {
		if !strings.Contains(source, required) {
			t.Errorf("main.go missing eeBUS admin wiring %q", required)
		}
	}
	portalSource, err := os.ReadFile("../../portal/handler.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"operator-mcp.sock", "trust-store", "StateRoot", "eebusruntime.AdminV1"} {
		if strings.Contains(string(portalSource), forbidden) {
			t.Errorf("Portal Go boundary directly owns private eeBUS runtime token %q", forbidden)
		}
	}
}
