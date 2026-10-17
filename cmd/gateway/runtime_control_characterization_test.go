package main

// G5 PR 1a.1 characterization baseline (issue #1147).
//
// These tests pin the CURRENT gateway composition before the G5 production
// edits (1a.2 registry seam, 1b durable control, 1c bridge):
//
//   - runGatewayLifecycle startup ordering through the existing package
//     seams (release-checker sidecar, runtime-state load, eBUS driver
//     preparation and asynchronous start, observe-first wiring, HTTP shell,
//     WaitRunning, semantic poller, discovery scan, background full scan),
//     including the slow and failing bootstrap shell behaviour as observed;
//   - B503 session issuance (epoch advance on driver activation) and
//     withdrawal (owner release on driver withdrawal) across start, replace,
//     reported failure, recovery, stop and late observer registration;
//   - GraphQL daemon/adapter status and MCP runtime status from the gateway
//     status providers across admission states;
//   - Portal health/bootstrap readiness projection;
//   - instance-GUID handling at runtime-state initialisation.
//
// They describe what the one-shot process does today; they do not claim the
// current code supports durable lifecycle control (that acceptance is J15).
// Every pinned value carries a "changes when" note naming the later
// production change that may legitimately alter it. A later PR that changes a
// value must update the expectation here and justify it in its PR body.
//
// Regeneration: expectations are inline. On mismatch each test prints the
// complete observed block between "--- observed ---" markers:
//
//	go test ./cmd/gateway -run Characterization -count=1 -v
//
// Determinism: ordering is established by synchronous seam callbacks and
// channels, never by sleeps. time.After appears only as a fail-safe bound on
// a channel wait that, on current production, completes immediately. Wall
// clock values in payloads (portal time_utc) are asserted and replaced.
// Inputs are synthetic; transports are in-process fakes (net.Pipe or a
// failing dialer), so no device, socket bind or network is used.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusadmin"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/releasecheck"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

const (
	rccGUID      = "00000000-0000-4000-8000-0000000000c1"
	rccGUIDOther = "00000000-0000-4000-8000-0000000000c2"
	// rccFailSafe bounds channel waits that complete immediately on current
	// production; it is never used to establish ordering.
	rccFailSafe = 10 * time.Second
)

type rccPinned struct {
	label     string
	got       string
	want      string
	changesBy string
}

func requireRCCPinned(t *testing.T, pins []rccPinned) {
	t.Helper()
	failed := false
	for _, pin := range pins {
		if pin.got != pin.want {
			failed = true
			t.Errorf("%s drifted (changes when: %s)\n want: %s\n  got: %s", pin.label, pin.changesBy, pin.want, pin.got)
		}
	}
	if failed {
		var observed strings.Builder
		for _, pin := range pins {
			fmt.Fprintf(&observed, "%s: %q\n", pin.label, pin.got)
		}
		t.Logf("--- observed ---\n%s--- observed ---", observed.String())
	}
}

type rccRecorder struct {
	mu     sync.Mutex
	events []string
}

func (recorder *rccRecorder) add(format string, args ...any) {
	recorder.mu.Lock()
	recorder.events = append(recorder.events, fmt.Sprintf(format, args...))
	recorder.mu.Unlock()
}

func (recorder *rccRecorder) joined() string {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return strings.Join(recorder.events, " -> ")
}

func rccFileState(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "absent"
	}
	var file struct {
		Meta struct {
			InstanceGUID string `json:"instance_guid"`
		} `json:"meta"`
	}
	if json.Unmarshal(data, &file) != nil {
		return "corrupt"
	}
	if file.Meta.InstanceGUID == "" {
		return "present(guid=empty)"
	}
	if file.Meta.InstanceGUID == rccGUID {
		return "present(guid=configured)"
	}
	return "present(guid=other)"
}

func rccTransportClass(raw transport.RawTransport) string {
	if raw == nil {
		return "nil"
	}
	_, err := raw.ReadByte()
	switch {
	case errors.Is(err, transport.ErrDriverUnavailable):
		return "driver_unavailable"
	case err == nil:
		return "readable"
	default:
		return "other_error"
	}
}

type rccDialMode int

const (
	rccDialHealthyAfterRelease rccDialMode = iota
	rccDialBlockedUntilCanceled
	rccDialFailing
)

type rccLifecycleOptions struct {
	dial              rccDialMode
	scanOnStart       bool
	cancelAt          string // "http" or "background"
	releaseDialAtHTTP bool
}

type rccLifecycleResult struct {
	events    string
	runErr    error
	dialCalls int32
}

// runRCCLifecycle drives runGatewayLifecycle through the existing seams and
// returns the ordered seam trace. The runtime-state file is observed at each
// seam to place the runtime-state load in the ordering.
func runRCCLifecycle(t *testing.T, options rccLifecycleOptions) rccLifecycleResult {
	t.Helper()
	originalWire := wireObserveFirstObserversFn
	originalDiscovery := startDiscoveryScanLoopFn
	originalSemantic := startVaillantSemanticPollingFn
	originalHTTP := startHTTPServerFn
	originalBackground := startBackgroundFullScanFn
	originalRelease := startGatewayReleaseChecker
	t.Cleanup(func() {
		wireObserveFirstObserversFn = originalWire
		startDiscoveryScanLoopFn = originalDiscovery
		startVaillantSemanticPollingFn = originalSemantic
		startHTTPServerFn = originalHTTP
		startBackgroundFullScanFn = originalBackground
		startGatewayReleaseChecker = originalRelease
	})

	statePath := filepath.Join(t.TempDir(), "runtime_state.json")
	recorder := &rccRecorder{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dialStarted := make(chan struct{}, 1)
	dialRelease := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(dialRelease) }) }
	defer release()
	var dialCalls atomic.Int32

	startGatewayReleaseChecker = func(*releasecheck.Checker, context.Context) {
		recorder.add("release_checker(runtime_state=%s)", rccFileState(statePath))
	}
	wireObserveFirstObserversFn = func(cfg *ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		recorder.add("observe_first_wiring(runtime_state=%s ebus_transport_prepared=%v)", rccFileState(statePath), cfg.Transport != nil)
		return nil, nil, nil
	}
	startHTTPServerFn = func(_ context.Context, _ ebusgateway.Config, gateway *ebusgateway.Gateway, builder *graphql.Builder, _ *graphql.BroadcastHub, _ graphql.SemanticProvider, schedule mcp.ScheduleWriter, _ mcp.ConfigWriter, _ *ebusgateway.BusObservabilityStore, _ *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		// The eBUS construction attempt must already be in flight when the
		// HTTP shell starts; waiting here (fail-safe bounded) proves the
		// driver start was issued before HTTP.
		constructionStarted := "true"
		select {
		case <-dialStarted:
		case <-time.After(rccFailSafe):
			constructionStarted = "false"
		}
		_, admitted := builder.AdmittedMutationSource()
		writeResult, _ := schedule.SetZoneTimeProgram(context.Background(), 0, 0, nil)
		recorder.add("http_shell(runtime_state=%s construction_started=%s transport=%s admitted=%v schedule_write=%s)",
			rccFileState(statePath), constructionStarted, rccTransportClass(gateway.Transport), admitted, writeResult.Error)
		if options.releaseDialAtHTTP {
			release()
		}
		if options.cancelAt == "http" {
			cancel()
		}
		return nil, nil, nil
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		recorder.add("semantic_poller")
		return nil
	}
	startDiscoveryScanLoopFn = func(_ context.Context, cfg ebusgateway.Config, _ *ebusgateway.Gateway, builder *graphql.Builder, _ activeTxnClassifier) startupScanSignals {
		_, admitted := builder.AdmittedMutationSource()
		recorder.add("discovery_scan(admitted=%v)", admitted)
		return startupScanSignals{}
	}
	startBackgroundFullScanFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, <-chan struct{}) {
		recorder.add("background_full_scan")
		if options.cancelAt == "background" {
			cancel()
		}
	}

	client, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close(); _ = client.Close() })
	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = nil
	cfg.TransportConfig.Protocol = ebusgateway.TransportTCPPlain
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "characterization.invalid:1"
	cfg.TransportConfig.DialTimeout = time.Second
	cfg.TransportConfig.Dial = func(dialCtx context.Context, _, _ string, _ time.Duration) (net.Conn, error) {
		dialCalls.Add(1)
		select {
		case dialStarted <- struct{}{}:
		default:
		}
		switch options.dial {
		case rccDialFailing:
			return nil, errors.New("characterization: endpoint offline")
		case rccDialBlockedUntilCanceled:
			<-dialCtx.Done()
			return nil, dialCtx.Err()
		default:
			select {
			case <-dialRelease:
				return client, nil
			case <-dialCtx.Done():
				return nil, dialCtx.Err()
			}
		}
	}
	cfg.BroadcastListen = false
	cfg.ScanOnStart = options.scanOnStart
	cfg.ScanInterval = time.Hour
	cfg.RuntimeStatePath = statePath
	cfg.InstanceGUID = rccGUID
	cfg.InstanceGUIDSource = "runtime_state"

	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(3 * rccFailSafe):
		t.Fatalf("runGatewayLifecycle did not return; trace so far: %s", recorder.joined())
	}
	return rccLifecycleResult{events: recorder.joined(), runErr: runErr, dialCalls: dialCalls.Load()}
}

func rccErr(err error) string {
	if err == nil {
		return "nil"
	}
	return "error"
}

// TestRuntimeControlCharacterizationLifecycleOrderingHealthy pins the startup
// order for a delayed but healthy eBUS endpoint with startup scan enabled.
func TestRuntimeControlCharacterizationLifecycleOrderingHealthy(t *testing.T) {
	result := runRCCLifecycle(t, rccLifecycleOptions{dial: rccDialHealthyAfterRelease, scanOnStart: true, cancelAt: "background", releaseDialAtHTTP: true})
	requireRCCPinned(t, []rccPinned{
		{
			label: "startup order",
			got:   result.events,
			want: "release_checker(runtime_state=absent)" +
				" -> observe_first_wiring(runtime_state=present(guid=configured) ebus_transport_prepared=true)" +
				" -> http_shell(runtime_state=present(guid=configured) construction_started=true transport=driver_unavailable admitted=false schedule_write=source selection not active)" +
				" -> semantic_poller -> discovery_scan(admitted=false) -> background_full_scan",
			changesBy: "1a.2 registry seam or 1b durable control reorders sidecars, runtime-state load, driver start, HTTP shell or the WaitRunning-gated scan phase",
		},
		{label: "run result", got: rccErr(result.runErr), want: "nil", changesBy: "1b durable control returns a service-level stop error"},
		{label: "dial attempts", got: fmt.Sprint(result.dialCalls), want: "1", changesBy: "1b durable control dials more than once for a healthy first generation"},
	})
}

// TestRuntimeControlCharacterizationSlowAndFailingBootstrapShell pins the
// HTTP-shell-first behaviour when eBUS construction is slow or failing.
func TestRuntimeControlCharacterizationSlowAndFailingBootstrapShell(t *testing.T) {
	const shellPrefix = "release_checker(runtime_state=absent)" +
		" -> observe_first_wiring(runtime_state=present(guid=configured) ebus_transport_prepared=true)" +
		" -> http_shell(runtime_state=present(guid=configured) construction_started=true transport=driver_unavailable admitted=false schedule_write=source selection not active)"
	cases := []struct {
		name      string
		options   rccLifecycleOptions
		want      string
		changesBy string
	}{
		{
			name:      "slow_construction_scan_on_start_canceled_before_running",
			options:   rccLifecycleOptions{dial: rccDialBlockedUntilCanceled, scanOnStart: true, cancelAt: "http"},
			want:      shellPrefix,
			changesBy: "1b durable control starts the semantic poller or scans without waiting for the first RUNNING generation (WaitRunning gate today returns before any scan work when canceled)",
		},
		{
			name:      "failing_construction_scan_on_start_canceled_before_running",
			options:   rccLifecycleOptions{dial: rccDialFailing, scanOnStart: true, cancelAt: "http"},
			want:      shellPrefix,
			changesBy: "1b durable control changes WaitRunning gating while the driver is in BACKOFF",
		},
		{
			name:      "failing_construction_without_startup_scan",
			options:   rccLifecycleOptions{dial: rccDialFailing, scanOnStart: false, cancelAt: "background"},
			want:      shellPrefix + " -> semantic_poller -> discovery_scan(admitted=false) -> background_full_scan",
			changesBy: "1b durable control gates the non-scan startup path on driver health (today it proceeds without WaitRunning)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := runRCCLifecycle(t, tc.options)
			requireRCCPinned(t, []rccPinned{
				{label: "startup order", got: result.events, want: tc.want, changesBy: tc.changesBy},
				{label: "run result", got: rccErr(result.runErr), want: "nil", changesBy: "1b durable control reports bootstrap failure as a process error (today the eBUS failure is categorical driver state, never fatal)"},
				{label: "construction attempted", got: fmt.Sprint(result.dialCalls >= 1), want: "true", changesBy: "1a.2 registry seam defers construction until a control request"},
			})
		})
	}
}

// rccLifecycleObserver records the private driver lifecycle events and
// forwards them to the production B503 runtime.
type rccLifecycleObserver struct {
	target *b503Runtime
	mu     sync.Mutex
	events []string
}

func (observer *rccLifecycleObserver) EBusDriverActivated(correlation drivermanager.Correlation) {
	observer.mu.Lock()
	observer.events = append(observer.events, fmt.Sprintf("activated g%d op%d", correlation.Generation, correlation.Operation))
	observer.mu.Unlock()
	observer.target.EBusDriverActivated(correlation)
}

func (observer *rccLifecycleObserver) EBusDriverWithdrawn(correlation drivermanager.Correlation) {
	observer.mu.Lock()
	observer.events = append(observer.events, fmt.Sprintf("withdrawn g%d op%d", correlation.Generation, correlation.Operation))
	observer.mu.Unlock()
	observer.target.EBusDriverWithdrawn(correlation)
}

func (observer *rccLifecycleObserver) take() []string {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	events := observer.events
	observer.events = nil
	return events
}

type rccB503Harness struct {
	manager    *drivermanager.Manager
	runtime    *ebusDriverRuntime
	controller *ebusDriverController
	b503       *b503Runtime
}

func newRCCB503Harness(t *testing.T) *rccB503Harness {
	t.Helper()
	runtimeSeam := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		raw := newIssue851BlockingRawTransport()
		return newManagedEBusGeneration(ctx, raw, raw.Close), nil
	}, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
	runtime := &ebusDriverRuntime{runtime: runtimeSeam}
	manager, err := drivermanager.New(drivermanager.Config{
		RetryJitter: func(time.Duration) time.Duration { return 0 },
		Drivers: []drivermanager.DriverConfig{{
			ID:            primaryEBusDriverID,
			Enabled:       true,
			Runtime:       runtime,
			Capabilities:  []drivermanager.Capability{drivermanager.CapabilityRead, drivermanager.CapabilityWrite},
			ClassifyError: classifyEBusDriverError,
			Retry:         drivermanager.RetryPolicy{Budget: 1, InitialDelay: time.Hour, MaxDelay: time.Hour, JitterRatio: -1},
		}},
	})
	if err != nil {
		t.Fatalf("drivermanager.New() error = %v", err)
	}
	controller := &ebusDriverController{manager: manager, runtime: runtime}
	b503rt := &b503Runtime{manager: b503session.New(b503session.TransportKey{AdapterInstanceID: "gateway"}, 30*time.Second, b503StubRefresh)}
	b503rt.manager.SetCleanupDispatcher(func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK}
	})
	t.Cleanup(func() { _ = controller.Shutdown(context.Background()) })
	return &rccB503Harness{manager: manager, runtime: runtime, controller: controller, b503: b503rt}
}

func (harness *rccB503Harness) render(observer *rccLifecycleObserver) string {
	snapshot, _ := harness.manager.Snapshot(primaryEBusDriverID)
	_, cleanup := harness.b503.manager.CleanupObligation()
	status := harness.b503.manager.StatusSnapshot()
	return fmt.Sprintf("events=[%s] driver=%s/g%d b503_epoch=%d b503_state=%d owned=%v cleanup_pending=%v",
		strings.Join(observer.take(), "; "), snapshot.ObservedState, snapshot.Generation,
		harness.b503.manager.LifecycleTransportKey().TransportEpoch, status.State, status.Owned, cleanup)
}

// TestRuntimeControlCharacterizationB503IssuanceAndWithdrawalOrder pins the
// exact activation/withdrawal sequence the private lifecycle observer (and
// therefore the B503 session runtime) receives from DriverManager.
func TestRuntimeControlCharacterizationB503IssuanceAndWithdrawalOrder(t *testing.T) {
	harness := newRCCB503Harness(t)
	observer := &rccLifecycleObserver{target: harness.b503}
	harness.controller.SetLifecycleObserver(observer)
	ctx := context.Background()
	pins := []rccPinned{}
	pin := func(label, want, changesBy string) {
		pins = append(pins, rccPinned{label: label, got: harness.render(observer), want: want, changesBy: changesBy})
	}

	pin("registered before start", "events=[] driver=STOPPED/g0 b503_epoch=0 b503_state=0 owned=false cleanup_pending=false",
		"1a.2 registry seam activates B503 before the first generation")
	if err := harness.manager.Start(ctx, primaryEBusDriverID); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	pin("start issues epoch", "events=[activated g1 op0] driver=RUNNING/g1 b503_epoch=1 b503_state=0 owned=false cleanup_pending=false",
		"1c bridge moves B503 issuance off driver activation")
	if _, err := harness.b503.manager.Enable(ctx); err != nil {
		t.Fatalf("B503 Enable() error = %v", err)
	}
	pin("issuer enabled", "events=[] driver=RUNNING/g1 b503_epoch=1 b503_state=2 owned=true cleanup_pending=false",
		"b503session ownership model changes")
	if err := harness.manager.Replace(ctx, primaryEBusDriverID); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	pin("replace withdraws then reissues", "events=[withdrawn g1 op0; activated g2 op0] driver=RUNNING/g2 b503_epoch=2 b503_state=0 owned=false cleanup_pending=true",
		"1c bridge stops withdrawing B503 before replacement or keeps the owner across generations")
	if accepted := harness.manager.ReportFailure(primaryEBusDriverID, drivermanager.Correlation{Generation: 2}, classifyEBusDriverError(errors.New("characterization: link lost"))); !accepted {
		t.Fatal("ReportFailure() rejected the current generation")
	}
	pin("reported failure withdraws", "events=[withdrawn g2 op0] driver=BACKOFF/g2 b503_epoch=2 b503_state=0 owned=false cleanup_pending=true",
		"1b durable control changes failure withdrawal or manufactures an epoch on loss")
	if err := harness.manager.Start(ctx, primaryEBusDriverID); err != nil {
		t.Fatalf("recovery Start() error = %v", err)
	}
	pin("recovery reissues", "events=[activated g3 op0] driver=RUNNING/g3 b503_epoch=3 b503_state=0 owned=false cleanup_pending=true",
		"1b durable control changes recovery issuance or clears cleanup on recovery")
	if err := harness.manager.Stop(ctx, primaryEBusDriverID); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	pin("stop withdraws", "events=[withdrawn g3 op0] driver=STOPPED/g3 b503_epoch=3 b503_state=0 owned=false cleanup_pending=true",
		"1c bridge drops the B503 withdrawal on stop")
	if err := harness.controller.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	pin("shutdown after stop is silent", "events=[] driver=STOPPED/g3 b503_epoch=3 b503_state=0 owned=false cleanup_pending=true",
		"1b durable control re-withdraws on process shutdown")
	requireRCCPinned(t, pins)
}

// TestRuntimeControlCharacterizationB503LateObserverReplay pins that a
// lifecycle observer registered after RUNNING receives exactly one replayed
// activation and that a second registration is refused.
func TestRuntimeControlCharacterizationB503LateObserverReplay(t *testing.T) {
	harness := newRCCB503Harness(t)
	if err := harness.manager.Start(context.Background(), primaryEBusDriverID); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	first := &rccLifecycleObserver{target: harness.b503}
	harness.controller.SetLifecycleObserver(first)
	firstRender := harness.render(first)
	second := &rccLifecycleObserver{target: harness.b503}
	harness.controller.SetLifecycleObserver(second)
	if err := harness.manager.Stop(context.Background(), primaryEBusDriverID); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	requireRCCPinned(t, []rccPinned{
		{label: "late registration replay", got: firstRender, want: "events=[activated g1 op0] driver=RUNNING/g1 b503_epoch=1 b503_state=0 owned=false cleanup_pending=false",
			changesBy: "1a.2 registry seam registers B503 before driver start (no replay) or replays more than once"},
		{label: "second observer refused", got: strings.Join(second.take(), "; "), want: "",
			changesBy: "1a.2 registry seam allows several lifecycle observers"},
		{label: "first observer sees stop", got: strings.Join(first.take(), "; "), want: "withdrawn g1 op0",
			changesBy: "1c bridge drops the B503 withdrawal on stop"},
	})
}

// TestRuntimeControlCharacterizationStatusSurfaces pins GraphQL daemon/adapter
// status and the MCP runtime status data produced by the gateway status
// providers across driver admission states.
func TestRuntimeControlCharacterizationStatusSurfaces(t *testing.T) {
	buildInfo := gatewayBuildInfo{ReleaseVersion: "0.0.0-char", BuildID: "char-build"}
	semantic := graphql.NewLiveSemanticProvider()
	semantic.SetAdapterHardwareInfo(&graphql.AdapterHardwareInfo{FirmwareVersion: "char-adapter-1"})

	stoppedManager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID: primaryEBusDriverID, Enabled: true, Capabilities: []drivermanager.Capability{drivermanager.CapabilityWrite},
	}}})
	if err != nil {
		t.Fatalf("drivermanager.New() error = %v", err)
	}
	t.Cleanup(func() { _ = stoppedManager.Shutdown(context.Background()) })
	stoppedController := &ebusDriverController{manager: stoppedManager}

	cases := []struct {
		name      string
		semantic  graphql.SemanticProvider
		admitted  func() (byte, bool)
		updates   func() bool
		want      string
		changesBy string
	}{
		{
			name:      "pre_admission_no_adapter_identity",
			admitted:  func() (byte, bool) { return 0, false },
			want:      `graphql={"adapterStatus":{"firmwareVersion":"","initiatorAddress":"","status":"unknown","updatesAvailable":false},"daemonStatus":{"firmwareVersion":"0.0.0-char","initiatorAddress":"","status":"running","updatesAvailable":false}} mcp={"adapter_status":{"firmware_version":"","status":"unknown","updates_available":false},"daemon_status":{"firmware_version":"0.0.0-char","initiator_address":"auto","status":"running","updates_available":false},"vaillant_regulator_capability":"UNKNOWN"}`,
			changesBy: "1b durable control unifies the pre-admission initiator projection (GraphQL empty vs MCP auto today) or adds driver state to status",
		},
		{
			name:      "admitted_with_adapter_identity_and_cached_update",
			semantic:  semantic,
			admitted:  func() (byte, bool) { return 0x1A, true },
			updates:   func() bool { return true },
			want:      `graphql={"adapterStatus":{"firmwareVersion":"char-adapter-1","initiatorAddress":"","status":"running","updatesAvailable":false},"daemonStatus":{"firmwareVersion":"0.0.0-char","initiatorAddress":"0x1A","status":"running","updatesAvailable":true}} mcp={"adapter_status":{"firmware_version":"char-adapter-1","status":"running","updates_available":false},"daemon_status":{"firmware_version":"0.0.0-char","initiator_address":"0x1A","status":"running","updates_available":true},"vaillant_regulator_capability":"UNKNOWN"}`,
			changesBy: "1b durable control or 1c bridge changes admitted status projection",
		},
		{
			name:      "selected_source_but_driver_not_running",
			semantic:  semantic,
			admitted:  newManagedEBusSourceProvider(stoppedController, func() (byte, bool) { return 0x1A, true }),
			want:      `graphql={"adapterStatus":{"firmwareVersion":"char-adapter-1","initiatorAddress":"","status":"running","updatesAvailable":false},"daemonStatus":{"firmwareVersion":"0.0.0-char","initiatorAddress":"","status":"running","updatesAvailable":false}} mcp={"adapter_status":{"firmware_version":"char-adapter-1","status":"running","updates_available":false},"daemon_status":{"firmware_version":"0.0.0-char","initiator_address":"auto","status":"running","updates_available":false},"vaillant_regulator_capability":"UNKNOWN"}`,
			changesBy: "1a.2 registry seam stops intersecting the selected source with live driver WRITE admission",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder := graphql.NewBuilder(nil, nil)
			builder.SetStatusProvider(newRuntimeStatusProviderForBuild(tc.semantic, tc.admitted, buildInfo, tc.updates))
			handler, err := graphql.NewHandler(builder)
			if err != nil {
				t.Fatalf("graphql.NewHandler() error = %v", err)
			}
			request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ daemonStatus { status firmwareVersion updatesAvailable initiatorAddress } adapterStatus { status firmwareVersion updatesAvailable initiatorAddress } }"}`))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			var graphQLPayload struct {
				Data   map[string]any `json:"data"`
				Errors any            `json:"errors"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &graphQLPayload); err != nil || graphQLPayload.Errors != nil {
				t.Fatalf("GraphQL response = %s (err=%v)", response.Body.String(), err)
			}
			graphQLData, _ := json.Marshal(graphQLPayload.Data)

			server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
			if err != nil {
				t.Fatalf("mcp.NewServer() error = %v", err)
			}
			server.SetStatusProvider(newMCPRuntimeStatusProviderForBuild(tc.semantic, tc.admitted, buildInfo, tc.updates))
			envelope := mcpCallToolEnvelope(t, server.Handler(), "ebus.v1.runtime.status.get", `{}`)
			mcpData, _ := json.Marshal(envelope["data"])
			requireRCCPinned(t, []rccPinned{{
				label:     "status surfaces",
				got:       fmt.Sprintf("graphql=%s mcp=%s", graphQLData, mcpData),
				want:      tc.want,
				changesBy: tc.changesBy,
			}})
		})
	}
}

// TestRuntimeControlCharacterizationPortalReadiness pins the readiness
// projection and how Portal health/bootstrap expose it.
func TestRuntimeControlCharacterizationPortalReadiness(t *testing.T) {
	ready := func() string { return ebusProxyReadinessReady }
	degraded := func() string { return ebusProxyReadinessDegraded }
	cases := []struct {
		name      string
		proxy     func() string
		snapshot  eebusRuntimeLifecycleSnapshot
		want      string
		changesBy string
	}{
		{name: "eebus_disabled_no_proxy", snapshot: eebusRuntimeLifecycleSnapshot{State: eebusLifecycleDisabled},
			want:      `readiness={"process_readiness":"READY","http_readiness":"READY","proxy_readiness":"DISABLED","eebus_readiness":"DISABLED"} eebus_admin_capability=false eebus_admin_endpoint=""`,
			changesBy: "1b durable control derives process/HTTP readiness from service state instead of constant READY"},
		{name: "eebus_starting_proxy_degraded", proxy: degraded, snapshot: eebusRuntimeLifecycleSnapshot{State: eebusLifecycleStarting},
			want:      `readiness={"process_readiness":"READY","http_readiness":"READY","proxy_readiness":"DEGRADED","eebus_readiness":"STARTING"} eebus_admin_capability=false eebus_admin_endpoint=""`,
			changesBy: "1b durable control exposes STARTING as not-ready process readiness"},
		{name: "eebus_running_proxy_ready", proxy: ready, snapshot: eebusRuntimeLifecycleSnapshot{State: eebusLifecycleRunning},
			want:      `readiness={"process_readiness":"READY","http_readiness":"READY","proxy_readiness":"READY","eebus_readiness":"READY"} eebus_admin_capability=true eebus_admin_endpoint="/eebus-admin"`,
			changesBy: "1c bridge changes how eeBUS admin availability is gated"},
		{name: "eebus_backoff_without_reason", proxy: ready, snapshot: eebusRuntimeLifecycleSnapshot{State: eebusLifecycleBackoff},
			want:      `readiness={"process_readiness":"READY","http_readiness":"READY","proxy_readiness":"READY","eebus_readiness":"DEGRADED","eebus_degraded_reason":"UNKNOWN_STARTUP_FAILURE"} eebus_admin_capability=false eebus_admin_endpoint=""`,
			changesBy: "1b durable control distinguishes BACKOFF from DEGRADED in readiness"},
		{name: "eebus_degraded_with_reason", snapshot: eebusRuntimeLifecycleSnapshot{State: eebusLifecycleDegraded, DegradedReason: eebusadmin.EEBusDegradedReasonListenerUnavailable},
			want:      `readiness={"process_readiness":"READY","http_readiness":"READY","proxy_readiness":"DISABLED","eebus_readiness":"DEGRADED","eebus_degraded_reason":"LISTENER_UNAVAILABLE"} eebus_admin_capability=false eebus_admin_endpoint=""`,
			changesBy: "eeBUS degraded reason vocabulary changes"},
		{name: "eebus_stopped", snapshot: eebusRuntimeLifecycleSnapshot{State: eebusLifecycleStopped},
			want:      `readiness={"process_readiness":"READY","http_readiness":"READY","proxy_readiness":"DISABLED","eebus_readiness":"DEGRADED","eebus_degraded_reason":"UNKNOWN_STARTUP_FAILURE"} eebus_admin_capability=false eebus_admin_endpoint=""`,
			changesBy: "1b durable control reports an operator-stopped sidecar as STOPPED instead of DEGRADED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := tc.snapshot
			proxy := tc.proxy
			handler := portal.NewHandler(portal.Options{
				GatewayVersion: "0.0.0-char",
				BuildID:        "char-build",
				EEBusAdminPath: "/eebus-admin",
				Readiness:      func() portal.RuntimeReadiness { return projectGatewayReadiness(proxy, snapshot) },
			})
			health := httptest.NewRecorder()
			handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
			var healthPayload map[string]json.RawMessage
			if err := json.Unmarshal(health.Body.Bytes(), &healthPayload); err != nil {
				t.Fatalf("health body = %s: %v", health.Body.String(), err)
			}
			var timeUTC string
			_ = json.Unmarshal(healthPayload["time_utc"], &timeUTC)
			if _, err := time.Parse(time.RFC3339, timeUTC); err != nil {
				t.Errorf("health time_utc %q is not RFC 3339 (changes when: health drops wall-clock time)", timeUTC)
			}
			var status, version, build string
			_ = json.Unmarshal(healthPayload["status"], &status)
			_ = json.Unmarshal(healthPayload["gateway_version"], &version)
			_ = json.Unmarshal(healthPayload["build_id"], &build)

			bootstrap := httptest.NewRecorder()
			handler.ServeHTTP(bootstrap, httptest.NewRequest(http.MethodGet, "/api/v1/bootstrap", nil))
			var bootstrapPayload struct {
				Capabilities map[string]bool   `json:"capabilities"`
				Endpoints    map[string]string `json:"endpoints"`
			}
			if err := json.Unmarshal(bootstrap.Body.Bytes(), &bootstrapPayload); err != nil {
				t.Fatalf("bootstrap body = %s: %v", bootstrap.Body.String(), err)
			}
			requireRCCPinned(t, []rccPinned{
				{label: "health envelope", got: fmt.Sprintf("status=%s version=%s build=%s code=%d", status, version, build, health.Code),
					want: "status=ok version=0.0.0-char build=char-build code=200", changesBy: "1b durable control reports service state in health status"},
				{label: "readiness", got: fmt.Sprintf("readiness=%s eebus_admin_capability=%v eebus_admin_endpoint=%q", healthPayload["readiness"], bootstrapPayload.Capabilities["eebus_admin"], bootstrapPayload.Endpoints["eebus_admin"]),
					want: tc.want, changesBy: tc.changesBy},
			})
		})
	}
}

// TestRuntimeControlCharacterizationRuntimeStateGUID pins how the configured
// instance GUID is normalised and persisted by runtime-state initialisation.
func TestRuntimeControlCharacterizationRuntimeStateGUID(t *testing.T) {
	normalizations := []struct {
		input string
		want  string
	}{
		{input: "  " + strings.ToUpper(rccGUID) + "  ", want: rccGUID},
		{input: "", want: ""},
		{input: "00000000-0000-1000-8000-0000000000c1", want: "error"},
		{input: "not-a-guid", want: "error"},
	}
	var normalized []string
	for _, tc := range normalizations {
		got, err := normalizeInstanceGUID(tc.input)
		if err != nil {
			got = "error"
		}
		normalized = append(normalized, fmt.Sprintf("%q=>%q", strings.TrimSpace(tc.input), got))
	}

	type guidCase struct {
		name     string
		existing string
		guid     string
		want     string
	}
	persisted := func(t *testing.T, tc guidCase) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "runtime_state.json")
		if tc.existing != "" {
			body := fmt.Sprintf(`{"schema_version":1,"meta":{"instance_guid":%q,"written_at":"2001-02-03T04:05:06Z"}}`, tc.existing)
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
		}
		cfg := ebusgateway.DefaultConfig()
		cfg.RuntimeStatePath = path
		cfg.InstanceGUID = tc.guid
		cfg.InstanceGUIDSource = "runtime_state"
		manager, state := initRuntimeStateManager(context.Background(), cfg, gatewayBuildInfo{ReleaseVersion: "0.0.0-char", BuildID: "char-build"})
		stopCtx, cancel := context.WithTimeout(context.Background(), rccFailSafe)
		defer cancel()
		defer func() { _ = manager.Stop(stopCtx) }()
		loaded := state.Meta.InstanceGUID
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Sprintf("loaded=%q persisted=<none>", loaded)
		}
		var file struct {
			Meta struct {
				InstanceGUID string `json:"instance_guid"`
			} `json:"meta"`
		}
		_ = json.Unmarshal(data, &file)
		return fmt.Sprintf("loaded=%q persisted=%q", loaded, file.Meta.InstanceGUID)
	}
	cases := []guidCase{
		{name: "configured_fresh_install", guid: rccGUID, want: `loaded="" persisted="` + rccGUID + `"`},
		{name: "unconfigured_retains_persisted", existing: rccGUIDOther, want: `loaded="` + rccGUIDOther + `" persisted="` + rccGUIDOther + `"`},
		{name: "unconfigured_fresh_install", want: `loaded="" persisted=""`},
		{name: "configured_overrides_persisted", existing: rccGUIDOther, guid: rccGUID, want: `loaded="` + rccGUIDOther + `" persisted="` + rccGUID + `"`},
	}
	pins := []rccPinned{{
		label:     "normalizeInstanceGUID",
		got:       strings.Join(normalized, " "),
		want:      `"` + strings.ToUpper(rccGUID) + `"=>"` + rccGUID + `" ""=>"" "00000000-0000-1000-8000-0000000000c1"=>"error" "not-a-guid"=>"error"`,
		changesBy: "1b durable control derives the GUID itself or accepts non-v4 identifiers",
	}}
	for _, tc := range cases {
		pins = append(pins, rccPinned{
			label:     "runtime-state GUID " + tc.name,
			got:       persisted(t, tc),
			want:      tc.want,
			changesBy: "1b durable control generates a GUID when none is configured, or makes the persisted GUID authoritative over the configured one",
		})
	}
	requireRCCPinned(t, pins)
}
