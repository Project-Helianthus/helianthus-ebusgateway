package main

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"reflect"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

func TestIssue817EEBusAutomaticallyStartsTheOperatorRuntime(t *testing.T) {
	originalResolver := resolveEEBusInterfaceAddressesFn
	originalOperatorFactory := newEEBusOperatorRuntimeFn
	originalRuntimeFactory := newEEBusRuntimeFn
	t.Cleanup(func() {
		resolveEEBusInterfaceAddressesFn = originalResolver
		newEEBusOperatorRuntimeFn = originalOperatorFactory
		newEEBusRuntimeFn = originalRuntimeFactory
	})

	runtime := &msp05bRuntime{}
	admin := issue809AdminStub{}
	operatorCalls := 0
	ordinaryCalls := 0
	resolveEEBusInterfaceAddressesFn = func(string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
	}
	newEEBusOperatorRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
		operatorCalls++
		return runtime, admin, nil
	}
	newEEBusRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		ordinaryCalls++
		return &msp05bRuntime{}, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.EEBusConfig = msp05bEnabledConfig()
	adapter, gotAdmin, available, err := issue817StartAdminAwareRuntime(t, context.Background(), cfg)
	if err != nil {
		t.Fatalf("start eeBUS operator runtime: %v", err)
	}
	if adapter == nil || gotAdmin == nil || !available || operatorCalls != 1 || ordinaryCalls != 0 || runtime.startCalls != 1 {
		t.Fatalf("adapter/admin/available/operator/ordinary/starts=%v/%v/%v/%d/%d/%d, want automatic operator runtime", adapter, gotAdmin, available, operatorCalls, ordinaryCalls, runtime.startCalls)
	}
	_ = adapter.Shutdown()
}

func TestIssue817OperatorBoundaryFailureKeepsPublicEEBusRuntimeRunning(t *testing.T) {
	originalResolver := resolveEEBusInterfaceAddressesFn
	originalOperatorFactory := newEEBusOperatorRuntimeFn
	originalRuntimeFactory := newEEBusRuntimeFn
	t.Cleanup(func() {
		resolveEEBusInterfaceAddressesFn = originalResolver
		newEEBusOperatorRuntimeFn = originalOperatorFactory
		newEEBusRuntimeFn = originalRuntimeFactory
	})

	operatorCalls := 0
	publicRuntime := &msp05bRuntime{}
	resolveEEBusInterfaceAddressesFn = func(string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
	}
	newEEBusOperatorRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
		operatorCalls++
		return nil, nil, errors.New("operator capability unavailable")
	}
	newEEBusRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		return publicRuntime, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.EEBusConfig = msp05bEnabledConfig()
	adapter, admin, available, err := issue817StartAdminAwareRuntime(t, context.Background(), cfg)
	if err != nil || adapter == nil || admin != nil || available || operatorCalls != 1 || publicRuntime.startCalls != 1 {
		t.Fatalf("fallback err=%v adapter=%v admin=%v available=%v operator=%d public starts=%d", err, adapter, admin, available, operatorCalls, publicRuntime.startCalls)
	}
	_ = adapter.Shutdown()
}

func TestIssue817GatewayMountsOneCredentialFreeTypedOperatorBoundary(t *testing.T) {
	content, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(content)
	for _, required := range []string{
		`mux.Handle("/admin/eebus/v1/", eebusAdminHandler)`,
		`eebusAdminHandler := eebusadmin.NewUnavailableHandler()`,
		`Admin: eebusAdmin`,
		`Raw: eebusAdapter`,
	} {
		if !strings.Contains(source, required) {
			t.Errorf("main.go missing eeBUS operator wiring %q", required)
		}
	}
	for _, forbidden := range []string{
		"eebusAdminAuthConfig",
		"Auth: eebusAdminAuthConfig",
		"PrincipalPortalOwner",
		"PrincipalHAIntegration",
		"principal=%s",
	} {
		if strings.Contains(source, forbidden) {
			t.Errorf("main.go retains split auth/principal wiring %q", forbidden)
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

func issue817StartAdminAwareRuntime(t *testing.T, ctx context.Context, cfg ebusgateway.Config) (*eebusRuntimeAdapter, eebusruntime.AdminV1, bool, error) {
	t.Helper()
	results := reflect.ValueOf(startEEBusAdminAwareRuntime).Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(cfg)})
	if len(results) != 4 && len(results) != 5 {
		t.Fatalf("startEEBusAdminAwareRuntime returns %d values, want adapter/admin/available/error", len(results))
	}
	var adapter *eebusRuntimeAdapter
	if !results[0].IsNil() {
		adapter = results[0].Interface().(*eebusRuntimeAdapter)
	}
	var admin eebusruntime.AdminV1
	if !results[1].IsNil() {
		admin = results[1].Interface().(eebusruntime.AdminV1)
	}
	availableIndex := 2
	errorIndex := 3
	if len(results) == 5 {
		availableIndex = 3
		errorIndex = 4
	}
	var err error
	if !results[errorIndex].IsNil() {
		err = results[errorIndex].Interface().(error)
	}
	return adapter, admin, results[availableIndex].Bool(), err
}
