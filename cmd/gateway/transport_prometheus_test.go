package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusadmin"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

func TestModbusRuntimeTransportStatusDoesNotClaimDisabledOrFailedRuntimeReady(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  ebusgateway.ModbusTCPConfig
		started bool
		state   ebusgateway.TransportRuntimeState
		reason  string
		outcome ebusgateway.TransportRuntimeOutcome
	}{
		{name: "disabled", state: ebusgateway.TransportRuntimeStateDisabled, reason: ebusgateway.TransportRuntimeReasonNotConfigured, outcome: ebusgateway.TransportRuntimeOutcomeUnavailable},
		{name: "startup failed", config: ebusgateway.ModbusTCPConfig{Enabled: true}, state: ebusgateway.TransportRuntimeStateDegraded, reason: ebusgateway.TransportRuntimeReasonStartupFailed, outcome: ebusgateway.TransportRuntimeOutcomeUnavailable},
		{name: "started", config: ebusgateway.ModbusTCPConfig{Enabled: true}, started: true, state: ebusgateway.TransportRuntimeStateReady, reason: ebusgateway.TransportRuntimeReasonNone, outcome: ebusgateway.TransportRuntimeOutcomeAvailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := modbusRuntimeTransportStatus(tc.config, tc.started)
			if got.State != tc.state || got.Reason != tc.reason || got.Outcome != tc.outcome {
				t.Fatalf("runtime status = %#v; want state=%q reason=%q outcome=%q", got, tc.state, tc.reason, tc.outcome)
			}
		})
	}
}

func TestEEBusTransportObserverFencesDelayedInitialDelivery(t *testing.T) {
	store := ebusgateway.NewBusObservabilityStore(ebusgateway.DefaultConfig())
	lifecycle := &eebusRuntimeLifecycle{state: eebusLifecycleDegraded, revision: 1}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		lifecycle.SetTransportRuntimeStatusObserver(func(status ebusgateway.TransportRuntimeStatus) {
			if status.Revision == 1 {
				close(entered)
				<-release
			}
			store.SetTransportRuntimeStatus(status)
		})
		close(done)
	}()
	<-entered
	lifecycle.setState(eebusLifecycleRunning, false)
	close(release)
	<-done
	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `protocol="eebus",reason="none",state="ready"`) || strings.Contains(metrics, `protocol="eebus",reason="startup_failed",state="degraded"`) {
		t.Fatalf("delayed initial observer write replaced ready state:\n%s", metrics)
	}
}

func TestEEBusPublicRuntimeFallbackIsTransportReady(t *testing.T) {
	runtime := &msp05bRuntime{}
	lifecycle, err := newEEBusRuntimeLifecycle(context.Background(), true, eebusRuntimeLifecycleOptions{
		policy: eebusRestartPolicy{MaxAttempts: 3, Backoff: time.Second},
		start: func(context.Context) (*eebusRuntimeAdapter, eebusruntime.AdminV1, bool, error) {
			return &eebusRuntimeAdapter{runtime: runtime, startupDegradedReason: eebusadmin.EEBusDegradedReasonAdminBoundaryUnavailable}, nil, false, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lifecycle.Shutdown() }()
	snapshot := lifecycle.LifecycleSnapshot()
	if !snapshot.RuntimeAvailable || snapshot.AdminAvailable || (snapshot.State != eebusLifecycleDegraded && snapshot.State != eebusLifecycleBackoff) {
		t.Fatalf("fallback lifecycle = %#v", snapshot)
	}
	status := eebusRuntimeTransportStatus(snapshot)
	if status.State != ebusgateway.TransportRuntimeStateReady || status.Outcome != ebusgateway.TransportRuntimeOutcomeAvailable {
		t.Fatalf("fallback transport status = %#v", status)
	}
}

func TestGatewayPublishesRetirementBeforeObservabilityClose(t *testing.T) {
	store := ebusgateway.NewBusObservabilityStore(ebusgateway.DefaultConfig())
	eebus := &eebusRuntimeLifecycle{state: eebusLifecycleRunning, revision: 1, runtimeAvailable: true, adapter: &eebusRuntimeAdapter{}}
	eebus.SetTransportRuntimeStatusObserver(store.SetTransportRuntimeStatus)
	publishGatewayTransportRetirement(store, true, eebus)
	metrics := store.RenderPrometheus()
	for _, want := range []string{
		`protocol="modbus_tcp",reason="shutdown",state="retired"`,
		`protocol="eebus",reason="shutdown",state="retired"`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("retirement was not observable before close: %q\n%s", want, metrics)
		}
	}

	source, err := os.ReadFile("gateway_run_lifecycle.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	retire := strings.Index(text, "publishGatewayTransportRetirement(busObservability, modbusAdapter != nil, eebusLifecycle)")
	close := strings.Index(text, "busObservability.Close()")
	if retire < 0 || close < 0 {
		t.Fatalf("missing retirement or observability close: retire=%d close=%d", retire, close)
	}
	retireDefer := strings.LastIndex(text[:retire], "defer func() {")
	if retireDefer < 0 || close > retireDefer {
		t.Fatalf("retirement defer must be registered after control-plane close defer: retire=%d close=%d defer=%d", retire, close, retireDefer)
	}
}

func TestEEBusTransportRetirementFencesDelayedRecoveryDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := ebusgateway.NewBusObservabilityStore(ebusgateway.DefaultConfig())
	runtime := &msp05bRuntime{}
	waitEntered := make(chan struct{})
	releaseWait := make(chan struct{})
	recoveryNotification := make(chan struct{})
	releaseNotification := make(chan struct{})
	recoveryAttempted := make(chan struct{})
	var waitMu sync.Mutex
	waitCalls := 0
	var notificationOnce sync.Once
	var startMu sync.Mutex
	startCalls := 0
	lifecycle, err := newEEBusRuntimeLifecycle(ctx, true, eebusRuntimeLifecycleOptions{
		policy: eebusRestartPolicy{MaxAttempts: 3, Backoff: time.Second},
		start: func(context.Context) (*eebusRuntimeAdapter, eebusruntime.AdminV1, bool, error) {
			startMu.Lock()
			startCalls++
			call := startCalls
			startMu.Unlock()
			if call == 1 {
				return &eebusRuntimeAdapter{runtime: runtime, startupDegradedReason: eebusadmin.EEBusDegradedReasonAdminBoundaryUnavailable}, nil, false, nil
			}
			close(recoveryAttempted)
			return nil, nil, false, errors.New("bounded optional-admin recovery failed")
		},
		wait: func(waitCtx context.Context, _ time.Duration) bool {
			waitMu.Lock()
			waitCalls++
			call := waitCalls
			waitMu.Unlock()
			if call == 1 {
				close(waitEntered)
				select {
				case <-releaseWait:
					return true
				case <-waitCtx.Done():
					return false
				}
			}
			<-waitCtx.Done()
			return false
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lifecycle.Shutdown() }()
	<-waitEntered
	blockedRevision := lifecycle.LifecycleSnapshot().Revision + 1
	lifecycle.SetTransportRuntimeStatusObserver(func(status ebusgateway.TransportRuntimeStatus) {
		if status.Revision == blockedRevision {
			notificationOnce.Do(func() {
				close(recoveryNotification)
				select {
				case <-releaseNotification:
				case <-ctx.Done():
				}
			})
		}
		store.SetTransportRuntimeStatus(status)
	})
	close(releaseWait)
	<-recoveryNotification

	publishGatewayTransportRetirement(store, false, lifecycle)
	if runtime.stopCalls != 0 {
		t.Fatalf("retirement stopped native runtime before its existing owner: stopCalls=%d", runtime.stopCalls)
	}
	assertRetired := func(stage string) {
		metrics := store.RenderPrometheus()
		if !strings.Contains(metrics, `protocol="eebus",reason="shutdown",state="retired"`) {
			t.Fatalf("eeBUS metric was not terminal after %s:\n%s", stage, metrics)
		}
	}
	assertRetired("retirement publication")

	// This resumes a recovery notification that captured the old observer and
	// revision before retirement. The store rejects it, and subsequent recovery
	// transitions find the observer detached.
	close(releaseNotification)
	<-recoveryAttempted
	assertRetired("delayed recovery and later transition")
}

func TestGatewayControlPlaneContextDefersSharedCancellation(t *testing.T) {
	type contextKey struct{}
	shared, cancelShared := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "retirement"))
	defer cancelShared()
	control, cancelControl := newGatewayControlPlaneContext(shared)
	defer cancelControl()
	cancelShared()
	<-shared.Done()
	if control.Err() != nil || control.Value(contextKey{}) != "retirement" {
		t.Fatalf("control context lost lifecycle ordering or value: err=%v value=%v", control.Err(), control.Value(contextKey{}))
	}
	cancelControl()
	<-control.Done()
}

func TestMetricsListenerPublishesRetirementBeforeSharedContextShutdown(t *testing.T) {
	sharedCtx, cancelShared := context.WithCancel(context.Background())
	defer cancelShared()
	controlCtx, cancelControl := newGatewayControlPlaneContext(sharedCtx)
	defer cancelControl()
	store := ebusgateway.NewBusObservabilityStore(ebusgateway.DefaultConfig())
	store.SetTransportRuntimeStatus(modbusRuntimeTransportStatus(ebusgateway.ModbusTCPConfig{Enabled: true}, true))
	eebus := &eebusRuntimeLifecycle{state: eebusLifecycleRunning, revision: 1, runtimeAvailable: true, adapter: &eebusRuntimeAdapter{}}
	eebus.SetTransportRuntimeStatusObserver(store.SetTransportRuntimeStatus)
	mux := http.NewServeMux()
	mux.Handle("/metrics", store.MetricsHandler())
	cfg := ebusgateway.DefaultConfig()
	cfg.HTTPAddr = "127.0.0.1:0"
	server, _, err := startHTTPControlPlaneListener(controlCtx, cfg, mux, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	server.RegisterOnShutdown(func() { close(closed) })
	cancelShared()
	<-sharedCtx.Done()
	publishGatewayTransportRetirement(store, true, eebus)
	response, err := http.Get("http://" + server.Addr + "/metrics")
	if err != nil {
		t.Fatalf("final metrics scrape: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	for _, want := range []string{`protocol="modbus_tcp",reason="shutdown",state="retired"`, `protocol="eebus",reason="shutdown",state="retired"`} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("final metrics missing %q:\n%s", want, body)
		}
	}
	if err := shutdownHTTPControlPlane(server); err != nil {
		t.Fatal(err)
	}
	<-closed
	if _, err := http.Get("http://" + server.Addr + "/metrics"); err == nil {
		t.Fatal("metrics listener remained open after control-plane cancellation")
	}
}

func TestHTTPControlPlaneShutdownDrainsActiveRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/active", func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		_, _ = w.Write([]byte("retired"))
	})
	cfg := ebusgateway.DefaultConfig()
	cfg.HTTPAddr = "127.0.0.1:0"
	server, _, err := startHTTPControlPlaneListener(ctx, cfg, mux, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		r, e := http.Get("http://" + server.Addr + "/active")
		if e == nil {
			_, e = io.ReadAll(r.Body)
			c := r.Body.Close()
			if e == nil {
				e = c
			}
		}
		done <- e
	}()
	<-entered
	shutdown := make(chan error, 1)
	go func() { shutdown <- shutdownHTTPControlPlane(server) }()
	select {
	case e := <-shutdown:
		t.Fatalf("shutdown returned before request completion: %v", e)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	if _, err := http.Get("http://" + server.Addr + "/active"); err == nil {
		t.Fatal("listener stayed open")
	}
}

func TestHTTPControlPlaneShutdownFallsBackToCloseAfterCanceledDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/blocked", func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		_, _ = w.Write([]byte("late"))
	})
	cfg := ebusgateway.DefaultConfig()
	cfg.HTTPAddr = "127.0.0.1:0"
	server, _, err := startHTTPControlPlaneListener(ctx, cfg, mux, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	clientDone := make(chan struct{})
	go func() {
		response, requestErr := http.Get("http://" + server.Addr + "/blocked")
		if requestErr == nil {
			_, _ = io.ReadAll(response.Body)
			_ = response.Body.Close()
		}
		close(clientDone)
	}()
	<-entered
	drainCtx, cancelDrain := context.WithCancel(context.Background())
	cancelDrain()
	err = shutdownHTTPControlPlaneWithContext(server, drainCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error = %v; want canceled drain evidence", err)
	}
	close(release)
	<-clientDone
	if _, err := http.Get("http://" + server.Addr + "/blocked"); err == nil {
		t.Fatal("Close fallback left listener open")
	}
}

func TestEEBusAdminRequestDoesNotBlockTransportRetirement(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	lifecycle := &eebusRuntimeLifecycle{state: eebusLifecycleRunning, runtimeAvailable: true, adapter: &eebusRuntimeAdapter{}, handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { close(entered); <-release })}
	requestDone := make(chan struct{})
	go func() {
		lifecycle.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/", nil))
		close(requestDone)
	}()
	<-entered
	retired := make(chan struct{})
	go func() { lifecycle.PublishTransportRetirement(); close(retired) }()
	select {
	case <-retired:
	case <-time.After(time.Second):
		t.Fatal("retirement blocked on active admin request")
	}
	close(release)
	<-requestDone
}

func TestEEBusRuntimeTransportStatusUsesOnlyLifecycleState(t *testing.T) {
	for _, tc := range []struct {
		state eebusLifecycleState
		want  ebusgateway.TransportRuntimeState
	}{
		{state: eebusLifecycleDisabled, want: ebusgateway.TransportRuntimeStateDisabled},
		{state: eebusLifecycleStarting, want: ebusgateway.TransportRuntimeStateStarting},
		{state: eebusLifecycleBackoff, want: ebusgateway.TransportRuntimeStateDegraded},
		{state: eebusLifecycleRunning, want: ebusgateway.TransportRuntimeStateReady},
		{state: eebusLifecycleDegraded, want: ebusgateway.TransportRuntimeStateDegraded},
		{state: eebusLifecycleStopped, want: ebusgateway.TransportRuntimeStateRetired},
	} {
		got := eebusRuntimeTransportStatus(eebusRuntimeLifecycleSnapshot{State: tc.state})
		if got.State != tc.want {
			t.Fatalf("lifecycle %q transport state = %q; want %q", tc.state, got.State, tc.want)
		}
	}
}

func TestEEBusTransportStatusObserverReceivesLifecycleTransitionsOnly(t *testing.T) {
	lifecycle := &eebusRuntimeLifecycle{state: eebusLifecycleDisabled}
	var observed []ebusgateway.TransportRuntimeStatus
	lifecycle.SetTransportRuntimeStatusObserver(func(status ebusgateway.TransportRuntimeStatus) {
		observed = append(observed, status)
	})
	lifecycle.setState(eebusLifecycleStarting, false)
	lifecycle.setState(eebusLifecycleRunning, false)

	if len(observed) != 3 {
		t.Fatalf("observer calls = %d; want attachment plus two lifecycle transitions", len(observed))
	}
	for index, want := range []ebusgateway.TransportRuntimeState{
		ebusgateway.TransportRuntimeStateDisabled,
		ebusgateway.TransportRuntimeStateStarting,
		ebusgateway.TransportRuntimeStateReady,
	} {
		if observed[index].State != want {
			t.Fatalf("observer state[%d] = %q; want %q", index, observed[index].State, want)
		}
	}
}
