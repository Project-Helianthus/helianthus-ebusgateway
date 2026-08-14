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

func TestIssue809RunRejectsProtectedCredentialsBeforeStartingOperatorRuntime(t *testing.T) {
	originalResolver := resolveEEBusInterfaceAddressesFn
	originalFactory := newEEBusOperatorRuntimeFn
	t.Cleanup(func() {
		resolveEEBusInterfaceAddressesFn = originalResolver
		newEEBusOperatorRuntimeFn = originalFactory
	})

	runtime := &msp05bRuntime{}
	resolveEEBusInterfaceAddressesFn = func(string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
	}
	newEEBusOperatorRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
		return runtime, issue809AdminStub{}, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.EEBusConfig = msp05bEnabledConfig()
	cfg.EEBusAdminConfig.Enabled = true
	cfg.EEBusAdminConfig.OwnerOrigin = "http://gateway.test:8080"
	cfg.EEBusAdminConfig.OwnerSecretPath = t.TempDir() + "/missing-owner"
	cfg.EEBusAdminConfig.HASecretPath = t.TempDir() + "/missing-ha"
	err := run(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "eeBUS admin boundary") {
		t.Fatalf("run error=%v, want sanitized admin boundary failure", err)
	}
	if runtime.startCalls != 0 || runtime.stopCalls != 0 {
		t.Fatalf("operator runtime start/shutdown=%d/%d, want 0/0", runtime.startCalls, runtime.stopCalls)
	}
	if errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), cfg.EEBusAdminConfig.OwnerSecretPath) {
		t.Fatalf("admin startup error leaks protected path: %v", err)
	}
}

func TestIssue809GatewayMountsAdminDirectlyAndPortalGetsOnlyEndpointMetadata(t *testing.T) {
	content, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(content)
	for _, required := range []string{
		`mux.Handle("/admin/eebus/v1/", eebusAdminHandler)`,
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
