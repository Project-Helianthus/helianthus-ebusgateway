package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusadmin"
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
	httpContent, err := os.ReadFile("gateway_http_server.go")
	if err != nil {
		t.Fatal(err)
	}
	controlPlaneContent, err := os.ReadFile("gateway_http_control_plane.go")
	if err != nil {
		t.Fatal(err)
	}
	configContent, err := os.ReadFile("eebus_admin_config.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(content) + "\n" + string(httpContent) + "\n" + string(controlPlaneContent) + "\n" + string(configContent)
	for _, required := range []string{
		`mux.Handle("/admin/eebus/v1/", eebusAdminHandler)`,
		`eebusAdminHandler := eebusadmin.NewUnavailableHandler()`,
		`Admin: eebusAdmin`,
		`Raw: eebusAdapter`,
	} {
		if !strings.Contains(source, required) {
			t.Errorf("gateway bootstrap missing eeBUS operator wiring %q", required)
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
			t.Errorf("gateway bootstrap retains split auth/principal wiring %q", forbidden)
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

type issue846RestartWaiter struct {
	durations chan time.Duration
	releases  chan struct{}
	active    atomic.Int32
	maxActive atomic.Int32
}

func newIssue846RestartWaiter() *issue846RestartWaiter {
	return &issue846RestartWaiter{
		durations: make(chan time.Duration, 8),
		releases:  make(chan struct{}, 8),
	}
}

func (waiter *issue846RestartWaiter) Wait(ctx context.Context, delay time.Duration) bool {
	active := waiter.active.Add(1)
	defer waiter.active.Add(-1)
	for {
		maximum := waiter.maxActive.Load()
		if active <= maximum || waiter.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	waiter.durations <- delay
	select {
	case <-ctx.Done():
		return false
	case <-waiter.releases:
		return true
	}
}

func TestIssue846LifecycleUsesBoundedBackoffAndRecoversOneRuntimeAdminPair(t *testing.T) {
	waiter := newIssue846RestartWaiter()
	firstRuntime := &msp05bRuntime{}
	recoveredRuntime := &msp05bRuntime{}
	var attempts atomic.Int32

	lifecycle, initialErr := newEEBusRuntimeLifecycle(context.Background(), true, eebusRuntimeLifecycleOptions{
		policy: eebusRestartPolicy{MaxAttempts: 3, Backoff: time.Minute},
		wait:   waiter.Wait,
		start: func(context.Context) (*eebusRuntimeAdapter, eebusruntime.AdminV1, bool, error) {
			switch attempts.Add(1) {
			case 1:
				return &eebusRuntimeAdapter{runtime: firstRuntime}, nil, false, nil
			case 2:
				return nil, nil, false, errors.New("listener still unavailable")
			default:
				return &eebusRuntimeAdapter{runtime: recoveredRuntime}, issue809AdminStub{}, true, nil
			}
		},
	})
	if initialErr != nil {
		t.Fatalf("partial initial runtime = %v; want contained degraded result", initialErr)
	}
	t.Cleanup(func() { _ = lifecycle.Shutdown() })

	initial := lifecycle.LifecycleSnapshot()
	if initial.State != eebusLifecycleBackoff || initial.Attempts != 1 || initial.AdminAvailable || initial.Revision == 0 {
		t.Fatalf("initial lifecycle = %#v; want one degraded attempt in backoff", initial)
	}
	initialRevision := initial.Revision
	unavailableResponse := httptest.NewRecorder()
	lifecycle.ServeHTTP(unavailableResponse, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/unknown", nil))
	if unavailableResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("initial Admin handler status = %d; want closed unavailable", unavailableResponse.Code)
	}
	if delay := <-waiter.durations; delay <= 0 {
		t.Fatalf("first restart delay = %s; want nonzero backoff", delay)
	}
	waiter.releases <- struct{}{}
	if delay := <-waiter.durations; delay <= 0 {
		t.Fatalf("second restart delay = %s; want nonzero backoff", delay)
	}
	waiter.releases <- struct{}{}

	issue846WaitForLifecycleState(t, lifecycle, eebusLifecycleRunning)
	recovered := lifecycle.LifecycleSnapshot()
	if recovered.Attempts != 3 || !recovered.AdminAvailable || recovered.Revision <= initialRevision || recovered.TimerOutstanding {
		t.Fatalf("recovered lifecycle = %#v; want one available replacement after three attempts", recovered)
	}
	if firstRuntime.stopCalls != 1 {
		t.Fatalf("replaced partial runtime shutdown calls = %d; want one", firstRuntime.stopCalls)
	}
	if waiter.maxActive.Load() != 1 {
		t.Fatalf("maximum concurrent restart waits = %d; want one outstanding timer", waiter.maxActive.Load())
	}
	availableResponse := httptest.NewRecorder()
	lifecycle.ServeHTTP(availableResponse, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/unknown", nil))
	if availableResponse.Code != http.StatusNotFound {
		t.Fatalf("recovered Admin handler status = %d; want live typed handler", availableResponse.Code)
	}
}

func TestIssue846LifecycleStopsAfterFiniteAttempts(t *testing.T) {
	waiter := newIssue846RestartWaiter()
	var attempts atomic.Int32
	lifecycle, _ := newEEBusRuntimeLifecycle(context.Background(), true, eebusRuntimeLifecycleOptions{
		policy: eebusRestartPolicy{MaxAttempts: 3, Backoff: time.Second},
		wait:   waiter.Wait,
		start: func(context.Context) (*eebusRuntimeAdapter, eebusruntime.AdminV1, bool, error) {
			attempts.Add(1)
			return nil, nil, false, errors.New("startup unavailable")
		},
	})
	t.Cleanup(func() { _ = lifecycle.Shutdown() })

	for range 2 {
		if delay := <-waiter.durations; delay <= 0 {
			t.Fatalf("restart delay = %s; want nonzero", delay)
		}
		waiter.releases <- struct{}{}
	}
	issue846WaitForLifecycleState(t, lifecycle, eebusLifecycleDegraded)
	snapshot := lifecycle.LifecycleSnapshot()
	if snapshot.Attempts != 3 || snapshot.TimerOutstanding || attempts.Load() != 3 {
		t.Fatalf("exhausted lifecycle = %#v calls=%d; want finite three-attempt window", snapshot, attempts.Load())
	}
	select {
	case delay := <-waiter.durations:
		t.Fatalf("unexpected fourth restart timer with delay %s", delay)
	default:
	}
}

func TestIssue846LifecycleCancellationStopsOutstandingTimerAndRuntime(t *testing.T) {
	waiter := newIssue846RestartWaiter()
	runtime := &msp05bRuntime{}
	var attempts atomic.Int32
	lifecycle, _ := newEEBusRuntimeLifecycle(context.Background(), true, eebusRuntimeLifecycleOptions{
		policy: eebusRestartPolicy{MaxAttempts: 3, Backoff: time.Second},
		wait:   waiter.Wait,
		start: func(context.Context) (*eebusRuntimeAdapter, eebusruntime.AdminV1, bool, error) {
			attempts.Add(1)
			return &eebusRuntimeAdapter{runtime: runtime}, nil, false, nil
		},
	})
	<-waiter.durations
	if err := lifecycle.Shutdown(); err != nil {
		t.Fatalf("lifecycle shutdown: %v", err)
	}
	snapshot := lifecycle.LifecycleSnapshot()
	if snapshot.State != eebusLifecycleStopped || snapshot.TimerOutstanding || attempts.Load() != 1 {
		t.Fatalf("cancelled lifecycle = %#v calls=%d; want stopped before retry", snapshot, attempts.Load())
	}
	if runtime.stopCalls != 1 {
		t.Fatalf("active runtime shutdown calls = %d; want one", runtime.stopCalls)
	}
}

func issue846WaitForLifecycleState(t *testing.T, lifecycle *eebusRuntimeLifecycle, want eebusLifecycleState) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if lifecycle.LifecycleSnapshot().State == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("lifecycle state = %s; want %s", lifecycle.LifecycleSnapshot().State, want)
}

func TestIssue846GatewayReadinessProjectsLifecycleAndConditionalProxy(t *testing.T) {
	tests := []struct {
		name           string
		proxyReadiness string
		lifecycle      eebusRuntimeLifecycleSnapshot
		wantProxy      string
		wantEEBus      string
		wantReason     eebusadmin.EEBusDegradedReason
	}{
		{name: "disabled", lifecycle: eebusRuntimeLifecycleSnapshot{State: eebusLifecycleDisabled}, wantProxy: "DISABLED", wantEEBus: "DISABLED"},
		{name: "starting with unavailable proxy", proxyReadiness: "DEGRADED", lifecycle: eebusRuntimeLifecycleSnapshot{State: eebusLifecycleStarting}, wantProxy: "DEGRADED", wantEEBus: "STARTING"},
		{name: "initial failure backoff", lifecycle: eebusRuntimeLifecycleSnapshot{State: eebusLifecycleBackoff, DegradedReason: eebusadmin.EEBusDegradedReasonListenerUnavailable}, wantProxy: "DISABLED", wantEEBus: "DEGRADED", wantReason: eebusadmin.EEBusDegradedReasonListenerUnavailable},
		{name: "recovered", proxyReadiness: "READY", lifecycle: eebusRuntimeLifecycleSnapshot{State: eebusLifecycleRunning}, wantProxy: "READY", wantEEBus: "READY"},
		{name: "unknown failure", lifecycle: eebusRuntimeLifecycleSnapshot{State: eebusLifecycleDegraded, DegradedReason: eebusadmin.EEBusDegradedReasonUnknownStartupFailure}, wantProxy: "DISABLED", wantEEBus: "DEGRADED", wantReason: eebusadmin.EEBusDegradedReasonUnknownStartupFailure},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var proxyReadiness func() string
			if test.proxyReadiness != "" {
				proxyReadiness = func() string { return test.proxyReadiness }
			}
			got := projectGatewayReadiness(proxyReadiness, test.lifecycle)
			if got.ProcessReadiness != "READY" || got.HTTPReadiness != "READY" || got.ProxyReadiness != test.wantProxy || got.EEBusReadiness != test.wantEEBus || got.EEBusDegradedReason != string(test.wantReason) {
				t.Fatalf("readiness=%#v; want process/http READY proxy=%s eeBUS=%s reason=%s", got, test.wantProxy, test.wantEEBus, test.wantReason)
			}
			if test.wantEEBus != "DEGRADED" && got.EEBusDegradedReason != "" {
				t.Fatalf("non-degraded readiness leaked reason: %#v", got)
			}
		})
	}
}

func TestIssue846StartupFailureReasonsUseClosedCanonicalMapping(t *testing.T) {
	tests := []struct {
		name string
		got  eebusadmin.EEBusDegradedReason
		want eebusadmin.EEBusDegradedReason
	}{
		{name: "configuration", got: classifyEEBusConfigFailure(errors.New("enabled eeBUS configuration requires a state root")), want: eebusadmin.EEBusDegradedReasonConfigurationInvalid},
		{name: "interface resolution", got: classifyEEBusConfigFailure(errors.New("resolve eeBUS interface addresses: unavailable")), want: eebusadmin.EEBusDegradedReasonListenerUnavailable},
		{name: "local identity", got: classifyEEBusFactoryFailure(errors.New("load local identity certificate")), want: eebusadmin.EEBusDegradedReasonLocalIdentityUnavailable},
		{name: "listener", got: classifyEEBusFactoryFailure(errors.New("listener bind failed")), want: eebusadmin.EEBusDegradedReasonListenerUnavailable},
		{name: "runtime factory", got: classifyEEBusFactoryFailure(errors.New("backend construction failed")), want: eebusadmin.EEBusDegradedReasonRuntimeFactoryUnavailable},
		{name: "admin boundary", got: eebusStartupFailureReason(markEEBusStartupFailure(eebusadmin.EEBusDegradedReasonAdminBoundaryUnavailable, errors.New("incomplete capability pair"))), want: eebusadmin.EEBusDegradedReasonAdminBoundaryUnavailable},
		{name: "unknown", got: eebusStartupFailureReason(errors.New("future startup failure")), want: eebusadmin.EEBusDegradedReasonUnknownStartupFailure},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.got != test.want {
				t.Fatalf("reason=%s; want %s", test.got, test.want)
			}
		})
	}
}
