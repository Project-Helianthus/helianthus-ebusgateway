package main

// G5 PR 1c.3 (issue #1222): fake-endpoint integration of the production
// runtime control composition through runGatewayLifecycle.
//
// Every test drives the real composition: the durable runtime_control store
// (runtimestate, file backed in a temp dir), the desired-state service, the
// frozen registry, the semantic hub, the ebus.primary manager-to-hub bridge,
// the generation task runner and the shell. Only the package seams that the
// existing lifecycle tests already replace are stubbed (observe-first
// wiring, HTTP listener, semantic poller, discovery and background scans);
// the eBUS endpoint is an in-process net.Pipe behind the configured dialer.
// No device or network is used, the only socket bind is the loopback
// ephemeral listener of the ordinary MCP tests (G5 PR 2, below), and every
// wait is bounded.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/releasecheck"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

const (
	rctlGUID   = "00000000-0000-4000-8000-0000000000d5"
	rctlBound  = 10 * time.Second
	rctlSource = 0x31
	// rctlReadBound is the stated responsiveness bound for cached
	// list/get/status reads while eBUS prepares, starts, fails or stops.
	rctlReadBound = 250 * time.Millisecond
)

type rctlDialMode int32

const (
	rctlDialHealthy rctlDialMode = iota
	rctlDialBlocked
	rctlDialFailing
)

// rctlConn records the close of one dialled endpoint.
type rctlConn struct {
	net.Conn
	env   *rctlEnv
	index int
	once  sync.Once
}

func (conn *rctlConn) Close() error {
	conn.once.Do(func() {
		conn.env.event("close %d", conn.index)
		conn.env.closed[conn.index].Store(true)
	})
	return conn.Conn.Close()
}

// rctlEnv is the process-independent fixture: the durable state path, the
// fake endpoint and the ordered trace of endpoint and task events.
type rctlEnv struct {
	t         *testing.T
	statePath string
	guid      string
	mode      atomic.Int32
	dials     atomic.Int32
	closed    [64]atomic.Bool
	peersMu   sync.Mutex
	peers     []net.Conn
	release   chan struct{}
	traceMu   sync.Mutex
	trace     []string
	discovery atomic.Int32
	semantic  atomic.Int32
	// background counts running background-scan tasks.
	background atomic.Int32
}

func newRCtlEnv(t *testing.T) *rctlEnv {
	t.Helper()
	env := &rctlEnv{t: t, statePath: filepath.Join(t.TempDir(), "runtime_state.json"), guid: rctlGUID, release: make(chan struct{})}
	t.Cleanup(func() {
		env.peersMu.Lock()
		defer env.peersMu.Unlock()
		for _, peer := range env.peers {
			_ = peer.Close()
		}
	})
	return env
}

func (env *rctlEnv) event(format string, args ...any) {
	env.traceMu.Lock()
	env.trace = append(env.trace, fmt.Sprintf(format, args...))
	env.traceMu.Unlock()
}

func (env *rctlEnv) events() []string {
	env.traceMu.Lock()
	defer env.traceMu.Unlock()
	return append([]string(nil), env.trace...)
}

func (env *rctlEnv) index(entry string) int {
	for i, got := range env.events() {
		if got == entry {
			return i
		}
	}
	return -1
}

func (env *rctlEnv) dial(ctx context.Context, _, _ string, _ time.Duration) (net.Conn, error) {
	n := int(env.dials.Add(1))
	env.event("dial %d", n)
	switch rctlDialMode(env.mode.Load()) {
	case rctlDialFailing:
		return nil, errors.New("rctl: endpoint offline")
	case rctlDialBlocked:
		select {
		case <-env.release:
		case <-ctx.Done():
		}
		return nil, errors.New("rctl: endpoint never answered")
	}
	client, peer := net.Pipe()
	env.peersMu.Lock()
	env.peers = append(env.peers, peer)
	env.peersMu.Unlock()
	return &rctlConn{Conn: client, env: env, index: n}, nil
}

func (env *rctlEnv) config() ebusgateway.Config {
	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = nil
	cfg.TransportConfig.Protocol = ebusgateway.TransportTCPPlain
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "rctl.invalid:1"
	cfg.TransportConfig.DialTimeout = time.Second
	cfg.TransportConfig.Dial = env.dial
	cfg.BroadcastListen = false
	cfg.ScanOnStart = true
	cfg.ScanInterval = time.Hour
	cfg.ScanSource = rctlSource
	cfg.ScanSourceAuto = false
	cfg.RuntimeStatePath = env.statePath
	cfg.InstanceGUID = env.guid
	cfg.InstanceGUIDSource = "runtime_state"
	return cfg
}

// rctlProcess is one runGatewayLifecycle incarnation.
type rctlProcess struct {
	env     *rctlEnv
	cancel  context.CancelFunc
	done    chan error
	control *gatewayRuntimeControl
	builder *graphql.Builder
	shellAt time.Duration
	// mcpURL is the ordinary MCP endpoint of the production HTTP shell when
	// the process runs with rctlProcessOptions.mcp.
	mcpURL string
}

type rctlProcessOptions struct {
	mutate func(*gatewayRuntimeControlOptions)
	config func(*ebusgateway.Config)
	// mcp runs the production startHTTPServer (with eeBUS absent) on a
	// loopback listener instead of the stub shell, so the ordinary MCP
	// surface of the composed process is reachable over HTTP.
	mcp bool
}

// start runs one process and returns once its HTTP shell started. The
// shell must start within the bound whatever eBUS does.
func (env *rctlEnv) start(opts rctlProcessOptions) *rctlProcess {
	t := env.t
	t.Helper()
	originalWire := wireObserveFirstObserversFn
	originalDiscovery := startDiscoveryScanLoopFn
	originalSemantic := startVaillantSemanticPollingFn
	originalHTTP := startHTTPServerFn
	originalBackground := startBackgroundFullScanFn
	originalRelease := startGatewayReleaseChecker
	originalOptions := gatewayRuntimeControlOptionsFor
	originalStarted := gatewayRuntimeControlStarted
	t.Cleanup(func() {
		wireObserveFirstObserversFn = originalWire
		startDiscoveryScanLoopFn = originalDiscovery
		startVaillantSemanticPollingFn = originalSemantic
		startHTTPServerFn = originalHTTP
		startBackgroundFullScanFn = originalBackground
		startGatewayReleaseChecker = originalRelease
		gatewayRuntimeControlOptionsFor = originalOptions
		gatewayRuntimeControlStarted = originalStarted
	})

	process := &rctlProcess{env: env, done: make(chan error, 1)}
	controlReady := make(chan *gatewayRuntimeControl, 1)
	// composed is written and read on the run goroutine only: the started
	// hook runs before the HTTP shell starts.
	var composed *gatewayRuntimeControl
	shell := make(chan *graphql.Builder, 1)
	startGatewayReleaseChecker = func(*releasecheck.Checker, context.Context) {}
	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		env.semantic.Add(1)
		env.event("semantic_poller")
		return nil
	}
	startDiscoveryScanLoopFn = func(ctx context.Context, cfg ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.Builder, _ activeTxnClassifier) startupScanSignals {
		n := env.discovery.Add(1)
		env.event("discovery %d", n)
		// The synthetic startup scan validates the configured source at
		// once; the bootstrap signal stays open, so background scanning
		// waits on it inside its own task.
		passed, done, loopDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		close(passed)
		close(done)
		// The synthetic scan loop exits only after the generation context
		// ends, and a little later, so the join is observable.
		go func() {
			<-ctx.Done()
			time.Sleep(20 * time.Millisecond)
			env.event("discovery_exit %d", n)
			close(loopDone)
		}()
		return startupScanSignals{firstPassDone: done, activeProbePassed: passed, done: loopDone}
	}
	startBackgroundFullScanFn = func(ctx context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.Builder, _ <-chan struct{}) {
		env.background.Add(1)
		env.event("background_start")
		<-ctx.Done()
		env.event("background_end")
		env.background.Add(-1)
	}
	startHTTPServerFn = func(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, builder *graphql.Builder, hub *graphql.BroadcastHub, semanticProvider graphql.SemanticProvider, scheduleWriter mcp.ScheduleWriter, configWriter mcp.ConfigWriter, busObservability *ebusgateway.BusObservabilityStore, _ *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		env.event("http_shell")
		if !opts.mcp {
			shell <- builder
			return nil, nil, nil
		}
		cfg.HTTPAddr = "127.0.0.1:0"
		cfg.MDNSAdvertise = false
		server, advertiser, err := startHTTPServer(ctx, cfg, gateway, builder, hub, semanticProvider, nil, nil, nil, nil,
			scheduleWriter, configWriter, busObservability, nil, http.NotFoundHandler(), nil, nil, nil,
			gatewayBuildInfo{ReleaseVersion: "0.0.0-rctl", BuildID: "rctl"}, func() bool { return false }, composed.driver, composed)
		if err == nil {
			process.mcpURL = "http://" + server.Addr + cfg.MCPPath
		}
		shell <- builder
		return server, advertiser, err
	}
	gatewayRuntimeControlOptionsFor = func(o gatewayRuntimeControlOptions) gatewayRuntimeControlOptions {
		o.CleanupTimeout = 2 * time.Second
		if opts.mutate != nil {
			opts.mutate(&o)
		}
		return o
	}
	gatewayRuntimeControlStarted = func(control *gatewayRuntimeControl) {
		composed = control
		controlReady <- control
	}

	cfg := env.config()
	if opts.config != nil {
		opts.config(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	process.cancel = cancel
	began := time.Now()
	go func() { process.done <- run(ctx, cfg) }()
	select {
	case process.control = <-controlReady:
	case err := <-process.done:
		t.Fatalf("run returned before control composition: %v", err)
	case <-time.After(rctlBound):
		t.Fatal("control composition did not run")
	}
	select {
	case process.builder = <-shell:
		process.shellAt = time.Since(began)
	case err := <-process.done:
		t.Fatalf("run returned before the HTTP shell started: %v", err)
	case <-time.After(rctlBound):
		t.Fatal("HTTP shell did not start")
	}
	t.Cleanup(func() { process.stop() })
	return process
}

// stop cancels the process and waits for run to return nil.
func (process *rctlProcess) stop() {
	t := process.env.t
	if process.cancel == nil {
		return
	}
	process.cancel()
	process.cancel = nil
	select {
	case err := <-process.done:
		if err != nil {
			t.Errorf("run returned %v on process shutdown; want nil", err)
		}
	case <-time.After(3 * rctlBound):
		t.Error("run did not return after process shutdown")
	}
}

func (process *rctlProcess) status() runtimecontrol.DriverStatus {
	t := process.env.t
	t.Helper()
	begin := time.Now()
	status, err := process.control.Service().Get(primaryEBusDriverID)
	if elapsed := time.Since(begin); elapsed > rctlReadBound {
		t.Fatalf("Get took %s; want within %s", elapsed, rctlReadBound)
	}
	if err != nil {
		t.Fatalf("Get(%s) = %v", primaryEBusDriverID, err)
	}
	return status
}

func (process *rctlProcess) waitObserved(want drivermanager.ObservedState) runtimecontrol.DriverStatus {
	t := process.env.t
	t.Helper()
	deadline := time.Now().Add(rctlBound)
	for {
		status := process.status()
		if status.ObservedState == want {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("observed %s/%s; want %s (trace %v)", status.ObservedState, status.Reason, want, process.env.events())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (process *rctlProcess) decide(method runtimecontrol.Method) runtimecontrol.DesiredView {
	t := process.env.t
	t.Helper()
	return rctlDecide(t, process.control.Service(), method, primaryEBusDriverID)
}

// rctlDecide submits one decision at the current revision, retrying only
// the refusal table's retryable rows (for example the documented short
// pruning window) within the bound.
func rctlDecide(t *testing.T, service *runtimecontrol.Service, method runtimecontrol.Method, id runtimecontrol.DriverID) runtimecontrol.DesiredView {
	t.Helper()
	deadline := time.Now().Add(rctlBound)
	for {
		status, err := service.Get(id)
		if err != nil {
			t.Fatalf("Get(%s) = %v", id, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
		view, err := service.Decide(ctx, method, runtimecontrol.Request{DriverID: id, ExpectedRevision: status.DesiredRevision})
		cancel()
		if err == nil {
			return view
		}
		var refusal *runtimecontrol.Refusal
		if !errors.As(err, &refusal) || !refusal.Retryable() || time.Now().After(deadline) {
			t.Fatalf("%s = %v", method, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (process *rctlProcess) adapterStatus() string {
	t := process.env.t
	t.Helper()
	handler, err := graphql.NewHandler(process.builder)
	if err != nil {
		t.Fatalf("graphql.NewHandler() = %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ adapterStatus { status } daemonStatus { status } }"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	begin := time.Now()
	handler.ServeHTTP(response, request)
	if elapsed := time.Since(begin); elapsed > rctlReadBound {
		t.Fatalf("GraphQL status took %s; want within %s", elapsed, rctlReadBound)
	}
	var payload struct {
		Data struct {
			Adapter struct{ Status string } `json:"adapterStatus"`
			Daemon  struct{ Status string } `json:"daemonStatus"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("GraphQL status = %s: %v", response.Body.String(), err)
	}
	if payload.Data.Daemon.Status != "running" {
		t.Fatalf("daemon status = %q; want running (daemon describes the process)", payload.Data.Daemon.Status)
	}
	return payload.Data.Adapter.Status
}

func (env *rctlEnv) waitFor(what string, condition func() bool) {
	env.t.Helper()
	deadline := time.Now().Add(rctlBound)
	for !condition() {
		if time.Now().After(deadline) {
			env.t.Fatalf("timed out waiting for %s (trace %v)", what, env.events())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// storedRecord reads the committed control record of ebus.primary from a
// fresh store instance after the process released the store lock.
func (env *rctlEnv) storedRecord() runtimestate.ControlDriverView {
	t := env.t
	t.Helper()
	manager := runtimestate.New(runtimestate.Options{Path: env.statePath})
	if _, err := manager.Load(context.Background()); err != nil {
		t.Fatalf("Load() = %v", err)
	}
	defer func() { _ = manager.Stop(context.Background()) }()
	if _, err := manager.BootstrapControl(context.Background(), []runtimestate.ControlDriverConfig{{DriverID: primaryEBusDriverID, Enabled: true}}); err != nil {
		t.Fatalf("BootstrapControl() = %v", err)
	}
	view, err := manager.ControlDriver(primaryEBusDriverID)
	if err != nil {
		t.Fatalf("ControlDriver() = %v", err)
	}
	return view
}

// seed prepares durable state before a process starts: GUID, the one-time
// seed and an optional desired STOPPED decision or D5 material.
func (env *rctlEnv) seed(stopped bool, recovery *runtimestate.ControlRecovery) {
	t := env.t
	t.Helper()
	ctx := context.Background()
	manager := runtimestate.New(runtimestate.Options{Path: env.statePath})
	if _, err := manager.Load(ctx); err != nil {
		t.Fatalf("Load() = %v", err)
	}
	defer func() { _ = manager.Stop(ctx) }()
	if err := manager.EagerPersistInstanceGUID(ctx, env.guid, runtimestate.IdentitySourceRuntimeState); err != nil {
		t.Fatalf("EagerPersistInstanceGUID() = %v", err)
	}
	if _, err := manager.BootstrapControl(ctx, []runtimestate.ControlDriverConfig{{DriverID: primaryEBusDriverID, Enabled: true}}); err != nil {
		t.Fatalf("BootstrapControl() = %v", err)
	}
	if stopped {
		view, err := manager.ControlDriver(primaryEBusDriverID)
		if err != nil {
			t.Fatalf("ControlDriver() = %v", err)
		}
		decideCtx, cancel := context.WithTimeout(ctx, rctlBound)
		defer cancel()
		_, err = manager.DecideControl(decideCtx, runtimestate.ControlStop, runtimestate.ControlRequest{DriverID: primaryEBusDriverID, ExpectedRevision: view.DesiredRevision},
			func(in runtimestate.ControlPlanInput) (runtimestate.ControlPlan, *runtimestate.ControlRefusal) {
				return runtimestate.ControlPlan{Current: &runtimestate.ControlTransition{Kind: runtimestate.TransitionStop, Phase: runtimestate.PhasePending}}, nil
			})
		if err != nil {
			t.Fatalf("seed STOP = %v", err)
		}
		// Settle the seeded STOP like a completed operation: nothing ever
		// opened, so the slot is cleared without a correlation.
		if err := manager.CommitControlWork(ctx, primaryEBusDriverID, nil, nil); err != nil {
			t.Fatalf("settle seeded STOP = %v", err)
		}
	}
	if recovery != nil {
		if err := manager.CommitControlRecovery(ctx, recovery); err != nil {
			t.Fatalf("CommitControlRecovery() = %v", err)
		}
	}
}

// TestRuntimeControlLifecycleDurableStoppedBootAndLaterControl boots with
// durable STOPPED: zero endpoint opens while HTTP, list, get and status stay
// responsive; later START, STOP, START and RESTART all work in the same
// process, with close-before-start, generation task join and rerun, the
// admitted source withdrawn on STOP, and shutdown preserving durable intent
// without a synthetic STOP.
func TestRuntimeControlLifecycleDurableStoppedBootAndLaterControl(t *testing.T) {
	env := newRCtlEnv(t)
	env.seed(true, nil)
	process := env.start(rctlProcessOptions{})

	// Durable STOPPED: the inert source is prepared, nothing opens.
	stopped := process.waitObserved(drivermanager.ObservedStopped)
	if stopped.DesiredState != runtimecontrol.DesiredStopped {
		t.Fatalf("desired = %s; want STOPPED", stopped.DesiredState)
	}
	time.Sleep(50 * time.Millisecond)
	if got := env.dials.Load(); got != 0 {
		t.Fatalf("endpoint opens while desired STOPPED = %d; want 0", got)
	}
	list := process.control.Service().List()
	if len(list.Drivers) != 1 || list.Drivers[0].DriverID != primaryEBusDriverID {
		t.Fatalf("service list = %+v; want ebus.primary only", list.Drivers)
	}
	registered := process.control.Registry().List()
	if len(registered.Drivers) != 1 || registered.Drivers[0].DriverID != primaryEBusDriverID || len(registered.Drivers[0].Sources) != 1 || len(registered.Drivers[0].Hooks) != 0 {
		t.Fatalf("registry = %+v; want lifecycle-only ebus.primary with one source and no hooks", registered.Drivers)
	}
	wantSource, _ := ebusPrimarySourceID(rctlGUID)
	if registered.Drivers[0].Sources[0] != wantSource {
		t.Fatalf("registered source = %s; want the identity derived from DriverID and the configured GUID", registered.Drivers[0].Sources[0])
	}
	if registered.Drivers[0].NativeContract.Owner != ebusPrimaryNativeOwner || registered.Drivers[0].NativeContract.Version == "" {
		t.Fatalf("native contract = %+v; want the exact ebusgo module reference", registered.Drivers[0].NativeContract)
	}
	if got := process.adapterStatus(); got != adapterStatusStopped {
		t.Fatalf("adapter status while STOPPED = %q; want %q", got, adapterStatusStopped)
	}
	if env.discovery.Load() != 0 || env.semantic.Load() != 0 {
		t.Fatalf("post-start work ran with no admitted generation: trace %v", env.events())
	}

	// START from STOPPED: one endpoint, tasks once.
	process.decide(runtimecontrol.MethodStart)
	process.waitObserved(drivermanager.ObservedRunning)
	env.waitFor("first generation tasks", func() bool { return env.background.Load() == 1 })
	if got := env.dials.Load(); got != 1 {
		t.Fatalf("endpoint opens after START = %d; want 1", got)
	}
	if source, ok := process.builder.AdmittedMutationSource(); !ok || source != rctlSource {
		t.Fatalf("admitted source after START = %#x/%v; want %#x", source, ok, rctlSource)
	}
	if got := process.adapterStatus(); got == adapterStatusStopped || got == adapterStatusUnavailable {
		t.Fatalf("adapter status while RUNNING = %q; want the admitted projection", got)
	}
	firstKey, ok := ebusControlGeneration(process.control.bridge)
	if !ok {
		t.Fatal("no admitted generation after START")
	}

	// STOP: tasks are joined and the endpoint is closed before STOPPED is
	// visible; the admitted source is withdrawn; a late callback of the
	// stopped generation is rejected.
	process.decide(runtimecontrol.MethodStop)
	process.waitObserved(drivermanager.ObservedStopped)
	if env.background.Load() != 0 || env.index("background_end") < 0 {
		t.Fatalf("background task not joined before STOPPED: trace %v", env.events())
	}
	if env.index("discovery_exit 1") < 0 {
		t.Fatalf("discovery scan loop not joined before STOPPED: trace %v", env.events())
	}
	if !env.closed[1].Load() {
		t.Fatal("first endpoint still open after STOPPED")
	}
	if _, ok := process.builder.AdmittedMutationSource(); ok {
		t.Fatal("admitted source survived STOP")
	}
	if got := process.adapterStatus(); got != adapterStatusStopped {
		t.Fatalf("adapter status after STOP = %q; want %q", got, adapterStatusStopped)
	}
	if process.control.bridge.ReportFailure(firstKey.Control, drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}}) {
		t.Fatal("late failure report of the stopped generation was accepted")
	}
	if _, err := process.control.bridge.PublicationHandle(firstKey.Control); err == nil {
		t.Fatal("stopped generation obtained a publication handle")
	}

	// START again: the tasks rerun for the new generation.
	process.decide(runtimecontrol.MethodStart)
	process.waitObserved(drivermanager.ObservedRunning)
	env.waitFor("second generation tasks", func() bool { return env.discovery.Load() == 2 && env.background.Load() == 1 })
	secondKey, _ := ebusControlGeneration(process.control.bridge)
	if secondKey.Control.Epoch == firstKey.Control.Epoch || secondKey.Native <= firstKey.Native {
		t.Fatalf("START after STOP reused epoch or native generation: %+v then %+v", firstKey, secondKey)
	}

	// RESTART: the predecessor endpoint closes before the successor dials,
	// and the predecessor's tasks are joined before the successor's start.
	process.decide(runtimecontrol.MethodRestart)
	env.waitFor("restart dial", func() bool { return env.dials.Load() == 3 })
	process.waitObserved(drivermanager.ObservedRunning)
	env.waitFor("third generation tasks", func() bool { return env.discovery.Load() == 3 && env.background.Load() == 1 })
	if exitAt, discoveryAt := env.index("discovery_exit 2"), env.index("discovery 3"); exitAt < 0 || discoveryAt < 0 || exitAt > discoveryAt {
		t.Fatalf("RESTART started the successor's scan before the predecessor's scan loop was joined: trace %v", env.events())
	}
	if closeAt, dialAt := env.index("close 2"), env.index("dial 3"); closeAt < 0 || dialAt < 0 || closeAt > dialAt {
		t.Fatalf("RESTART did not close the predecessor before the successor dialled: trace %v", env.events())
	}
	if env.semantic.Load() != 1 {
		t.Fatalf("process-scoped semantic poller created %d times; want once", env.semantic.Load())
	}
	before := process.status()

	// Process shutdown: no synthetic STOP, durable intent unchanged.
	process.stop()
	stored := env.storedRecord()
	if stored.DesiredState != runtimestate.DesiredRunning || stored.DesiredRevision != uint64(before.DesiredRevision) {
		t.Fatalf("stored intent after shutdown = %s/%d; want RUNNING/%d", stored.DesiredState, stored.DesiredRevision, before.DesiredRevision)
	}
	if stored.Current != nil && stored.Current.Kind == runtimestate.TransitionStop {
		t.Fatalf("process shutdown created a synthetic STOP: %+v", stored.Current)
	}
}

// TestRuntimeControlLifecycleShellStartsWhateverEBusDoes covers a slow
// start, a failing start, corrupt control state and a missing instance GUID:
// the HTTP shell starts and reads stay responsive in every case; corrupt
// state and a missing GUID present FAILED with zero endpoint opens and no
// RUNNING fallback.
func TestRuntimeControlLifecycleShellStartsWhateverEBusDoes(t *testing.T) {
	t.Run("slow_start", func(t *testing.T) {
		env := newRCtlEnv(t)
		env.mode.Store(int32(rctlDialBlocked))
		process := env.start(rctlProcessOptions{})
		env.waitFor("blocked dial", func() bool { return env.dials.Load() >= 1 })
		if status := process.status(); status.ObservedState != drivermanager.ObservedStarting {
			t.Fatalf("observed %s while the start is blocked; want STARTING", status.ObservedState)
		}
		if got := process.adapterStatus(); got != adapterStatusUnavailable {
			t.Fatalf("adapter status while starting = %q; want %q", got, adapterStatusUnavailable)
		}
		close(env.release)
	})
	t.Run("failing_start", func(t *testing.T) {
		env := newRCtlEnv(t)
		env.mode.Store(int32(rctlDialFailing))
		process := env.start(rctlProcessOptions{})
		env.waitFor("failing dial", func() bool { return env.dials.Load() >= 1 })
		status := process.status()
		if status.ObservedState == drivermanager.ObservedRunning {
			t.Fatal("failing endpoint reported RUNNING")
		}
		if got := process.adapterStatus(); got != adapterStatusUnavailable {
			t.Fatalf("adapter status while failing = %q; want %q", got, adapterStatusUnavailable)
		}
	})
	t.Run("corrupt_control_state", func(t *testing.T) {
		env := newRCtlEnv(t)
		if err := os.WriteFile(env.statePath, []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		process := env.start(rctlProcessOptions{})
		status := process.waitObserved(drivermanager.ObservedFailed)
		time.Sleep(50 * time.Millisecond)
		if env.dials.Load() != 0 || status.DesiredState == runtimecontrol.DesiredRunning {
			t.Fatalf("corrupt control state opened %d endpoints with desired %s; want 0 and no RUNNING fallback", env.dials.Load(), status.DesiredState)
		}
	})
	t.Run("no_instance_guid", func(t *testing.T) {
		env := newRCtlEnv(t)
		env.guid = ""
		process := env.start(rctlProcessOptions{})
		process.waitObserved(drivermanager.ObservedFailed)
		time.Sleep(50 * time.Millisecond)
		if env.dials.Load() != 0 {
			t.Fatalf("missing GUID opened %d endpoints; want 0", env.dials.Load())
		}
		if process.control.withheld == nil || !errors.Is(process.control.withheld, errEBusPrimaryIdentityUnavailable) {
			t.Fatalf("withheld = %v; want the missing source identity", process.control.withheld)
		}
		if got := len(process.control.Registry().List().Drivers); got != 0 {
			t.Fatalf("registered %d drivers without a source identity; want 0", got)
		}
	})
}

// TestRuntimeControlLifecycleDisabledConfigWins keeps durable desired
// RUNNING untouched while the configured disposition is disabled.
func TestRuntimeControlLifecycleDisabledConfigWins(t *testing.T) {
	env := newRCtlEnv(t)
	env.seed(false, nil)
	before := env.storedRecord()
	process := env.start(rctlProcessOptions{mutate: func(o *gatewayRuntimeControlOptions) { o.Disabled = true }})
	status := process.waitObserved(drivermanager.ObservedDisabled)
	time.Sleep(50 * time.Millisecond)
	if env.dials.Load() != 0 {
		t.Fatalf("disabled driver opened %d endpoints; want 0", env.dials.Load())
	}
	if status.Enabled {
		t.Fatal("disabled driver reported enabled")
	}
	process.stop()
	after := env.storedRecord()
	if after.DesiredState != before.DesiredState || after.DesiredRevision != before.DesiredRevision {
		t.Fatalf("disabled configuration rewrote durable intent: %s/%d then %s/%d", before.DesiredState, before.DesiredRevision, after.DesiredState, after.DesiredRevision)
	}
}

// rctlManagedRecovery is a synthetic managed D5 adapter: it records the
// order of its restoration against endpoint opens and preparation.
type rctlManagedRecovery struct {
	env      *rctlEnv
	hub      *semhub.Hub
	source   runtimecontrol.SourceID
	restored []byte
	fail     bool
}

func (r *rctlManagedRecovery) Restore(_ context.Context, material *runtimecontrol.RecoveryMaterial) error {
	_, admitted := r.hub.Source(semregSourceID(r.source))
	r.env.event("restore dials=%d source_admitted=%v", r.env.dials.Load(), admitted)
	if material != nil {
		r.restored = append([]byte(nil), material.Payload...)
	}
	if r.fail {
		return &runtimecontrol.RestoreError{Drivers: []runtimecontrol.DriverID{primaryEBusDriverID}, Err: errors.New("rctl: synthetic restore failure")}
	}
	return nil
}

func (r *rctlManagedRecovery) Secure(context.Context, runtimecontrol.DriverID, runtimecontrol.Transition) (*runtimecontrol.RecoveryMaterial, error) {
	return nil, nil
}

// TestRuntimeControlLifecycleNonemptyRecoveryPrecedesNativeOpen runs
// committed, nonempty D5 material through the actual service, store and hub
// composition: restoration runs before the inert source admission and before
// any endpoint opens; a managed restore failure withholds ebus.primary
// FAILED with the shell up; with no managed owner the material fails the
// named driver closed and stays committed unchanged.
func TestRuntimeControlLifecycleNonemptyRecoveryPrecedesNativeOpen(t *testing.T) {
	material := &runtimestate.ControlRecovery{DriverIDs: []string{primaryEBusDriverID}, MaxDesiredRevision: 1, Payload: []byte(`{"synthetic":"d5"}`)}
	source, err := ebusPrimarySourceID(rctlGUID)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("managed_restore_then_prepare_then_open", func(t *testing.T) {
		env := newRCtlEnv(t)
		env.seed(false, material)
		var recovery *rctlManagedRecovery
		process := env.start(rctlProcessOptions{mutate: func(o *gatewayRuntimeControlOptions) {
			recovery = &rctlManagedRecovery{env: env, hub: o.Hub, source: source}
			o.Recovery = recovery
		}})
		process.waitObserved(drivermanager.ObservedRunning)
		if got := env.events(); len(got) == 0 || got[0] != "restore dials=0 source_admitted=false" {
			t.Fatalf("trace = %v; want restoration first, before source admission and any endpoint", got)
		}
		if string(recovery.restored) != string(material.Payload) {
			t.Fatalf("restored payload = %q; want the committed bytes", recovery.restored)
		}
		if env.index("dial 1") < 0 {
			t.Fatal("no endpoint opened after a successful restoration")
		}
	})
	t.Run("managed_restore_failure_withholds", func(t *testing.T) {
		env := newRCtlEnv(t)
		env.seed(false, material)
		process := env.start(rctlProcessOptions{mutate: func(o *gatewayRuntimeControlOptions) {
			o.Recovery = &rctlManagedRecovery{env: env, hub: o.Hub, source: source, fail: true}
		}})
		status := process.waitObserved(drivermanager.ObservedFailed)
		time.Sleep(50 * time.Millisecond)
		if env.dials.Load() != 0 || status.Reason != drivermanager.ReasonDependencyUnavailable {
			t.Fatalf("restore failure: dials=%d reason=%s; want 0 and DEPENDENCY_UNAVAILABLE", env.dials.Load(), status.Reason)
		}
		if _, admitted := process.control.bridge.hub.Source(semregSourceID(source)); admitted {
			t.Fatal("source prepared although restoration withheld its driver")
		}
	})
	t.Run("unowned_material_fails_closed_and_is_retained", func(t *testing.T) {
		env := newRCtlEnv(t)
		env.seed(false, material)
		process := env.start(rctlProcessOptions{})
		process.waitObserved(drivermanager.ObservedFailed)
		time.Sleep(50 * time.Millisecond)
		if env.dials.Load() != 0 {
			t.Fatalf("unowned recovery material opened %d endpoints; want 0", env.dials.Load())
		}
		process.stop()
		manager := runtimestate.New(runtimestate.Options{Path: env.statePath})
		if _, err := manager.Load(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = manager.Stop(context.Background()) }()
		kept := manager.ControlRecoveryPayload()
		if kept == nil || string(kept.Payload) != string(material.Payload) {
			t.Fatalf("recovery material after restart = %+v; want the committed bytes unchanged", kept)
		}
	})
}

// TestRuntimeControlTwoDriversStayResponsive composes the control service
// with ebus.primary (real bridge over a fake endpoint, real store and hub)
// and a second fixture driver whose start blocks: ebus.primary decisions,
// list and get stay responsive and its lifecycle completes while the other
// driver is blocked.
func TestRuntimeControlTwoDriversStayResponsive(t *testing.T) {
	env := newRCtlEnv(t)
	ctx := context.Background()
	store := runtimestate.New(runtimestate.Options{Path: env.statePath})
	if _, err := store.Load(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Stop(context.Background()) })
	if err := store.EagerPersistInstanceGUID(ctx, rctlGUID, runtimestate.IdentitySourceRuntimeState); err != nil {
		t.Fatal(err)
	}
	const fixture = runtimecontrol.DriverID("fixture.blocked")
	if _, err := store.BootstrapControl(ctx, []runtimestate.ControlDriverConfig{{DriverID: primaryEBusDriverID, Enabled: true}, {DriverID: string(fixture), Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	cfg := env.config()
	driver, err := prepareGatewayEBusDriver(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Shutdown(context.Background()) })
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	registry := runtimecontrol.NewRegistry(controlHubOwnership{hub: hub})
	bridge, err := composeEBusPrimary(registry, gatewayRuntimeControlOptions{Driver: driver, Hub: hub, InstanceGUID: rctlGUID, CleanupTimeout: 2 * time.Second}, "rctl-two")
	if err != nil {
		t.Fatal(err)
	}
	executor := &ebusControlExecutor{bridge: bridge}
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })
	other := &rctlBlockingExecutor{release: blocked}
	service, err := runtimecontrol.New(runtimecontrol.Options{
		Store: &runtimeControlStore{m: store},
		Recovery: &controlRecovery{drivers: []runtimecontrol.DriverID{fixture, primaryEBusDriverID},
			prepare: map[runtimecontrol.DriverID]func(context.Context) error{primaryEBusDriverID: executor.prepare}},
		Drivers:     []runtimecontrol.DriverConfig{{DriverID: primaryEBusDriverID, Executor: executor}, {DriverID: fixture, Executor: other}},
		Incarnation: "rctl-two",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)
	env.waitFor("blocked fixture start", func() bool { return other.started.Load() })

	read := func() runtimecontrol.DriverStatus {
		begin := time.Now()
		list := service.List()
		status, err := service.Get(primaryEBusDriverID)
		if elapsed := time.Since(begin); elapsed > rctlReadBound || err != nil || len(list.Drivers) != 2 {
			t.Fatalf("list/get took %s (err %v, %d drivers); want both drivers within %s", elapsed, err, len(list.Drivers), rctlReadBound)
		}
		return status
	}
	env.waitFor("ebus.primary RUNNING beside a blocked driver", func() bool { return read().ObservedState == drivermanager.ObservedRunning })
	decide := func(method runtimecontrol.Method) {
		read()
		begin := time.Now()
		rctlDecide(t, service, method, primaryEBusDriverID)
		if elapsed := time.Since(begin); elapsed > time.Second {
			t.Fatalf("%s took %s beside a blocked driver", method, elapsed)
		}
	}
	decide(runtimecontrol.MethodStop)
	env.waitFor("ebus.primary STOPPED beside a blocked driver", func() bool { return read().ObservedState == drivermanager.ObservedStopped })
	decide(runtimecontrol.MethodStart)
	env.waitFor("ebus.primary RUNNING again", func() bool { return read().ObservedState == drivermanager.ObservedRunning })
	if fixtureStatus, _ := service.Get(fixture); fixtureStatus.ObservedState != drivermanager.ObservedStarting {
		t.Fatalf("fixture observed %s; want it still STARTING (blocked)", fixtureStatus.ObservedState)
	}
}

type rctlBlockingExecutor struct {
	release <-chan struct{}
	started atomic.Bool
}

func (executor *rctlBlockingExecutor) Start(ctx context.Context, _ runtimecontrol.LifecycleCorrelation) error {
	executor.started.Store(true)
	select {
	case <-executor.release:
	case <-ctx.Done():
	}
	return errors.New("rctl: fixture never became ready")
}

func (executor *rctlBlockingExecutor) Stop(context.Context, runtimecontrol.LifecycleCorrelation) runtimecontrol.StopOutcome {
	return runtimecontrol.StopOutcome{NativeClosed: true, SemanticCleaned: true}
}

// TestRuntimeControlDrainingTasksBlockReplacement proves that a predecessor
// generation whose task cannot join keeps the next START refused with a
// truthful categorical reason, and that the start proceeds once it joined.
func TestRuntimeControlDrainingTasksBlockReplacement(t *testing.T) {
	env := newRCtlEnv(t)
	env.seed(true, nil)
	stuck := make(chan struct{})
	var stuckOnce sync.Once
	releaseStuck := func() { stuckOnce.Do(func() { close(stuck) }) }
	t.Cleanup(releaseStuck)
	process := env.start(rctlProcessOptions{})
	// The background stub of this test ignores cancellation until released.
	startBackgroundFullScanFn = func(ctx context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.Builder, _ <-chan struct{}) {
		env.background.Add(1)
		<-ctx.Done()
		<-stuck
		env.background.Add(-1)
	}
	process.waitObserved(drivermanager.ObservedStopped)
	process.decide(runtimecontrol.MethodStart)
	process.waitObserved(drivermanager.ObservedRunning)
	env.waitFor("task running", func() bool { return env.background.Load() == 1 })
	process.decide(runtimecontrol.MethodStop)
	process.waitObserved(drivermanager.ObservedStopped)
	runner, _ := process.control.executor.tasks()
	if phase := runner.Status().Phase; phase != ebusGenerationDraining {
		t.Fatalf("runner phase after STOP with a stuck task = %s; want DRAINING", phase)
	}
	process.decide(runtimecontrol.MethodStart)
	failed := process.waitObserved(drivermanager.ObservedFailed)
	if failed.Reason != drivermanager.ReasonDependencyUnavailable {
		t.Fatalf("start over a draining predecessor: reason %s; want DEPENDENCY_UNAVAILABLE", failed.Reason)
	}
	if got := env.dials.Load(); got != 1 {
		t.Fatalf("endpoint opens = %d; want 1 (no successor while draining)", got)
	}
	releaseStuck()
	env.waitFor("drain joined", func() bool {
		_, _ = runner.Sync(context.Background(), ebusGenerationKey{}, false)
		return runner.Status().Phase == ebusGenerationIdle
	})
	process.decide(runtimecontrol.MethodRestart)
	process.waitObserved(drivermanager.ObservedRunning)
}

func semregSourceID(id runtimecontrol.SourceID) semreg.SourceID { return semreg.SourceID(id) }

// TestRuntimeControlUnconfirmedCloseReportsFailedCloseUnconfirmed drives the
// composed executor (bridge plus runner) through the real service and store:
// a native close the manager cannot confirm is never trusted as a nil Stop.
// The bridge reports the UNCONFIRMED close class as an unproven native
// obligation while the semantic retirement completes independently, and the
// service presents FAILED/CLOSE_UNCONFIRMED, quarantined, with replacement
// blocked even after semantic success.
func TestRuntimeControlUnconfirmedCloseReportsFailedCloseUnconfirmed(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1, drain: 50 * time.Millisecond, noPrepare: true})
	plant.gateNextClose()
	ctx := context.Background()
	store := runtimestate.New(runtimestate.Options{Path: filepath.Join(t.TempDir(), "runtime_state.json")})
	if _, err := store.Load(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Stop(context.Background()) })
	if err := store.EagerPersistInstanceGUID(ctx, rctlGUID, runtimestate.IdentitySourceRuntimeState); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BootstrapControl(ctx, []runtimestate.ControlDriverConfig{{DriverID: primaryEBusDriverID, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	executor := &ebusControlExecutor{bridge: plant.bridge}
	service, err := runtimecontrol.New(runtimecontrol.Options{
		Store: &runtimeControlStore{m: store},
		Recovery: &controlRecovery{drivers: []runtimecontrol.DriverID{primaryEBusDriverID},
			prepare: map[runtimecontrol.DriverID]func(context.Context) error{primaryEBusDriverID: executor.prepare}},
		Drivers:     []runtimecontrol.DriverConfig{{DriverID: primaryEBusDriverID, Executor: executor}},
		Incarnation: "rctl-unconfirmed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)
	get := func() runtimecontrol.DriverStatus {
		status, err := service.Get(primaryEBusDriverID)
		if err != nil {
			t.Fatal(err)
		}
		return status
	}
	ctlWait(t, "seeded RUNNING", func() bool { return get().ObservedState == drivermanager.ObservedRunning })
	rctlDecide(t, service, runtimecontrol.MethodStop, primaryEBusDriverID)
	ctlWait(t, "FAILED/CLOSE_UNCONFIRMED", func() bool {
		status := get()
		return status.ObservedState == drivermanager.ObservedFailed && status.Reason == drivermanager.ReasonCloseUnconfirmed
	})
	status := get()
	if !status.SafetyQuarantined {
		t.Fatalf("status = %+v; want quarantined", status)
	}
	if view := plant.bridge.View(); view.CloseClass != drivermanager.CloseUnconfirmed || !view.Quarantined {
		t.Fatalf("bridge view = %+v; want the UNCONFIRMED close class and quarantine", view)
	}
	if retired := plant.successionRecords(); len(retired) != 0 {
		t.Fatalf("a successor was journalled after an unconfirmed close: %+v", retired)
	}
	before := plant.constructed()
	startCtx, cancel := context.WithTimeout(ctx, rctlBound)
	defer cancel()
	_, err = service.Decide(startCtx, runtimecontrol.MethodStart, runtimecontrol.Request{DriverID: primaryEBusDriverID, ExpectedRevision: status.DesiredRevision})
	var refusal *runtimecontrol.Refusal
	if !errors.As(err, &refusal) || refusal.Code != runtimecontrol.RefusalFailedPrecondition {
		t.Fatalf("START after an unconfirmed close = %v; want FAILED_PRECONDITION (quarantine blocks admission)", err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := plant.constructed(); got != before {
		t.Fatalf("START after an unconfirmed close constructed %d endpoints; want none", got-before)
	}
	if after := get(); after.ObservedState != drivermanager.ObservedFailed || after.Reason != drivermanager.ReasonCloseUnconfirmed {
		t.Fatalf("after START: %+v; want FAILED/CLOSE_UNCONFIRMED retained", after)
	}
}

// withRuntimeControlTestState gives a lifecycle test a writable durable
// control store and a configured instance GUID, so the one-time RUNNING seed
// applies and ebus.primary is reconciled as in a provisioned installation.
// Without both, the composition presents eBUS FAILED (control-load failure
// or missing source identity) while the shell still starts.
func withRuntimeControlTestState(t *testing.T, cfg *ebusgateway.Config) {
	t.Helper()
	cfg.RuntimeStatePath = filepath.Join(t.TempDir(), "runtime_state.json")
	cfg.InstanceGUID = rctlGUID
	cfg.InstanceGUIDSource = "runtime_state"
}

// TestRuntimeControlNativeContractDevelMarker covers builds without an exact
// linked ebusgo version: a workspace build (devel version) and a Replace
// directive name the module path with the devel marker, registration is
// admitted and the registry presents the reference verbatim; an exact
// module version is named as is.
func TestRuntimeControlNativeContractDevelMarker(t *testing.T) {
	devel := runtimecontrol.NativeContractRef{Owner: ebusPrimaryNativeOwner, Contract: ebusPrimaryNativeContract, Version: ebusPrimaryNativeDevelVersion}
	cases := []struct {
		name string
		info *debug.BuildInfo
		ok   bool
		want string
	}{
		{name: "workspace devel", ok: true, info: &debug.BuildInfo{Deps: []*debug.Module{{Path: ebusPrimaryNativeOwner, Version: "(devel)"}}}, want: ebusPrimaryNativeDevelVersion},
		{name: "replace directive", ok: true, info: &debug.BuildInfo{Deps: []*debug.Module{{Path: ebusPrimaryNativeOwner, Version: "v0.5.0", Replace: &debug.Module{Path: "../helianthus-ebusgo"}}}}, want: ebusPrimaryNativeDevelVersion},
		{name: "no build information", ok: false, want: ebusPrimaryNativeDevelVersion},
		{name: "module absent", ok: true, info: &debug.BuildInfo{}, want: ebusPrimaryNativeDevelVersion},
		{name: "exact version", ok: true, info: &debug.BuildInfo{Deps: []*debug.Module{{Path: ebusPrimaryNativeOwner, Version: "v0.5.1"}}}, want: "v0.5.1"},
	}
	for _, tc := range cases {
		ref, err := ebusPrimaryNativeContractFrom(tc.info, tc.ok)
		if err != nil || ref.Owner != devel.Owner || ref.Contract != devel.Contract || ref.Version != tc.want {
			t.Fatalf("%s: ref=%+v err=%v; want version %q", tc.name, ref, err, tc.want)
		}
	}

	// Through the production composition: the devel reference is admitted
	// and presented verbatim, and the driver still reconciles to RUNNING.
	original := readEBusPrimaryNativeContract
	readEBusPrimaryNativeContract = func() (runtimecontrol.NativeContractRef, error) { return devel, nil }
	t.Cleanup(func() { readEBusPrimaryNativeContract = original })
	env := newRCtlEnv(t)
	process := env.start(rctlProcessOptions{})
	process.waitObserved(drivermanager.ObservedRunning)
	got, err := process.control.Registry().Get(primaryEBusDriverID)
	if err != nil || got.Driver.NativeContract != devel {
		t.Fatalf("registry entry = %+v (err %v); want the devel reference verbatim", got.Driver.NativeContract, err)
	}
}

// TestRuntimeControlFailedStartupTaskDegradesAdapterStatus: a failed
// start-critical generation task keeps the shell and the native generation
// up but is presented on the adapter status as degraded, naming the task
// (MCP condition, GraphQL status); the condition clears with the generation.
func TestRuntimeControlFailedStartupTaskDegradesAdapterStatus(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	executor := &ebusControlExecutor{bridge: plant.bridge}
	control := &gatewayRuntimeControl{bridge: plant.bridge, executor: executor, driver: plant.ctrl}
	t.Cleanup(func() { ebusAdapterConditions.Delete(plant.ctrl) })
	var dependent atomic.Int32
	runner, err := newEBusGenerationTaskRunner(ebusGenerationTaskConfig{
		Tasks: []ebusGenerationTask{
			{Name: ebusTaskSourceAdmission, Run: func(context.Context, *ebusGenerationScope) error { return nil }},
			{Name: ebusTaskSemanticSurface, Requires: []string{ebusTaskSourceAdmission}, Run: func(context.Context, *ebusGenerationScope) error {
				return errors.New("rctl: synthetic passive shadow attach failure")
			}},
			{Name: ebusTaskStartupScan, Requires: []string{ebusTaskSemanticSurface}, Run: func(context.Context, *ebusGenerationScope) error {
				dependent.Add(1)
				return nil
			}},
		},
		Admitted: ebusControlGenerationAdmitted(plant.bridge),
	})
	if err != nil {
		t.Fatal(err)
	}
	control.BindTasks(context.Background(), runner, nil)
	semantic := graphql.NewLiveSemanticProvider()
	semantic.SetAdapterHardwareInfo(&graphql.AdapterHardwareInfo{FirmwareVersion: "fw-rctl"})
	lifecycle, condition := ebusAdapterLifecycleStatus(plant.ctrl), ebusAdapterConditionFor(plant.ctrl)
	mcpStatus := func() mcp.ServiceStatus {
		return newMCPRuntimeStatusProviderForControl(semantic, nil, gatewayBuildInfo{}, nil, lifecycle, condition).AdapterStatus()
	}
	gqlStatus := func() graphql.ServiceStatus {
		return newRuntimeStatusProviderForControl(semantic, nil, gatewayBuildInfo{}, nil, lifecycle).AdapterStatus()
	}

	corr := ctlCorr("inc.degraded.e1", 1, 1)
	if err := executor.Start(ctlCtx(t), corr); err != nil {
		t.Fatal(err)
	}
	ctlWait(t, "failed start-critical task recorded", func() bool { return control.StartupCondition() != "" })
	want := "generation task " + ebusTaskSemanticSurface + " failed"
	if got := mcpStatus(); got.Status != adapterStatusDegraded || got.Condition != want || got.FirmwareVersion != "fw-rctl" {
		t.Fatalf("MCP adapter status = %+v; want degraded naming %q", got, want)
	}
	if got := gqlStatus(); got.Status != adapterStatusDegraded {
		t.Fatalf("GraphQL adapter status = %+v; want degraded", got)
	}
	if plant.snapshot().ObservedState != drivermanager.ObservedRunning || dependent.Load() != 0 {
		t.Fatalf("native generation %s, dependents ran %d; want RUNNING kept and dependents skipped", plant.snapshot().ObservedState, dependent.Load())
	}
	plant.bridge.Stop(ctlCtx(t), corr)
	executor.sync(context.Background())
	if got := mcpStatus(); got.Status != adapterStatusStopped || got.Condition != "" {
		t.Fatalf("MCP adapter status after STOP = %+v; want stopped with no condition", got)
	}
}

// A failed process-surface step (passive shadow attach) must not mark the
// process surface ready: the next admitted generation retries the missing
// step and the degraded condition clears only after it succeeded.
func TestRuntimeControlSurfaceRetriedAfterFailedSetup(t *testing.T) {
	env := newRCtlEnv(t)
	env.seed(false, nil)
	dedup, err := ebusgateway.NewActivePassiveDeduplicator(ebusgateway.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	originalAttach := attachPassiveShadowProducerFn
	t.Cleanup(func() { attachPassiveShadowProducerFn = originalAttach })
	var attaches, pollers atomic.Int32
	attachPassiveShadowProducerFn = func(*vaillantSemanticPoller, context.Context, *ebusgateway.ActivePassiveDeduplicator) error {
		if attaches.Add(1) == 1 {
			return errors.New("rctl: synthetic passive shadow attach failure")
		}
		return nil
	}
	process := env.start(rctlProcessOptions{config: func(cfg *ebusgateway.Config) {
		cfg.ObserveFirstEnabled = true
		cfg.PassiveStateDirectApply = true
		wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
			return nil, dedup, nil
		}
		startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
			pollers.Add(1)
			return &vaillantSemanticPoller{}
		}
	}})
	process.waitObserved(drivermanager.ObservedRunning)
	env.waitFor("first attach attempt", func() bool { return attaches.Load() == 1 })
	env.waitFor("degraded condition", func() bool { return process.control.StartupCondition() != "" })

	process.decide(runtimecontrol.MethodRestart)
	env.waitFor("retried attach", func() bool { return attaches.Load() == 2 })
	env.waitFor("condition cleared", func() bool { return process.control.StartupCondition() == "" })
	if got := pollers.Load(); got != 1 {
		t.Fatalf("semantic poller started %d times; want once", got)
	}
	env.waitFor("retried generation scan", func() bool { return env.discovery.Load() == 1 && env.background.Load() == 1 })
	process.decide(runtimecontrol.MethodRestart)
	env.waitFor("next generation scan", func() bool { return env.discovery.Load() == 2 })
	if got := attaches.Load(); got != 2 {
		t.Fatalf("attach attempts = %d; want no repeat after success", got)
	}
}

// The retained semantic poller is rebound to every admitted generation's
// initiator and companion instead of keeping the first generation's.
func TestRuntimeControlRetainedPollerRebindsSourcePerGeneration(t *testing.T) {
	const sentinel = 0xEE
	env := newRCtlEnv(t)
	env.seed(false, nil)
	poller := &vaillantSemanticPoller{source: sentinel, companion: sentinel}
	var started ebusgateway.Config
	process := env.start(rctlProcessOptions{config: func(cfg *ebusgateway.Config) {
		startVaillantSemanticPollingFn = func(_ context.Context, cfg ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.LiveSemanticProvider, _ *graphql.BroadcastHub, _ <-chan struct{}) *vaillantSemanticPoller {
			started = cfg
			return poller
		}
	}})
	process.waitObserved(drivermanager.ObservedRunning)
	env.waitFor("poller rebound to the admitted generation", func() bool {
		source, _ := poller.sourceAndCompanion()
		return source != sentinel
	})
	if source, companion := poller.sourceAndCompanion(); source != started.ScanSource || companion != started.StartupCompanionTarget {
		t.Fatalf("poller source/companion = %#02x/%#02x; want %#02x/%#02x", source, companion, started.ScanSource, started.StartupCompanionTarget)
	}
	poller.setSourceAndCompanion(sentinel, sentinel)
	process.decide(runtimecontrol.MethodRestart)
	env.waitFor("successor generation rebinds the poller", func() bool {
		source, _ := poller.sourceAndCompanion()
		return source != sentinel
	})
}

// The deduplicator observer is bound in the pass that creates the poller,
// before the fallible capture step, so a retried generation cannot leave the
// deduplicator as its own observer.
func TestRuntimeControlWatchObserverBoundBeforeCaptureStep(t *testing.T) {
	raw, err := os.ReadFile("gateway_run_lifecycle.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	observer := strings.Index(text, "deduplicator.SetWatchObserver(poller.shadow)")
	capture := strings.Index(text, "newLeafPromotionCaptureRuntime(cfg.EEBusConfig.StateRoot")
	if observer < 0 || capture < 0 || observer > capture {
		t.Fatalf("watch observer binding at %d must precede capture construction at %d", observer, capture)
	}
}

// Delayed identity enrichment launched by the startup scan is joinable, so a
// generation can wait for it before a successor starts.
func TestRuntimeControlDelayedIdentityEnrichmentIsJoinable(t *testing.T) {
	gateway, err := ebusgateway.New(context.Background(), ebusgateway.Config{Transport: transport.NewLoopback()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gateway.Close() })
	gateway.Registry.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "Vaillant", DeviceID: "BASV2"})

	originalEnrich, originalDelay := enrichVaillantIdentityFn, postStartupIdentityRetryDelay
	t.Cleanup(func() {
		enrichVaillantIdentityFn, postStartupIdentityRetryDelay = originalEnrich, originalDelay
	})
	postStartupIdentityRetryDelay = time.Millisecond
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce sync.Once
	enrichVaillantIdentityFn = func(context.Context, *ebusgateway.Gateway, ebusgateway.Config) {
		enterOnce.Do(func() { close(entered) })
		<-release
	}
	schedulePostStartupIdentityRetry(context.Background(), gateway, nil, ebusgateway.DefaultConfig(), nil)
	select {
	case <-entered:
	case <-time.After(rctlBound):
		t.Fatal("delayed enrichment did not start")
	}
	joined := make(chan struct{})
	go func() { postStartupIdentityRetryWG.Wait(); close(joined) }()
	select {
	case <-joined:
		t.Fatal("join returned while delayed enrichment was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-joined:
	case <-time.After(rctlBound):
		t.Fatal("join did not return after delayed enrichment finished")
	}
}

// TestRuntimeControlReplacementAdmissionWithdrawsPredecessorSource: the
// bridge's replacement admission gate clears the predecessor's admitted
// source before the replacement can be admitted, also while the drain has
// not joined.
func TestRuntimeControlReplacementAdmissionWithdrawsPredecessorSource(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	runner, err := newEBusGenerationTaskRunner(ebusGenerationTaskConfig{
		Tasks: []ebusGenerationTask{{Name: "idle", Run: func(ctx context.Context, _ *ebusGenerationScope) error {
			<-ctx.Done()
			return nil
		}}},
		Admitted: ebusControlGenerationAdmitted(plant.bridge),
	})
	if err != nil {
		t.Fatal(err)
	}
	var withdrawals []bool
	executor := &ebusControlExecutor{bridge: plant.bridge}
	executor.bind(runner, func(force bool) { withdrawals = append(withdrawals, force) })
	if err := executor.Start(ctlCtx(t), ctlCorr("inc.withdraw.e1", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if runner.Status().Phase != ebusGenerationRunning {
		t.Fatalf("runner phase = %s; want RUNNING", runner.Status().Phase)
	}
	withdrawals = nil
	if err := executor.admitGate(ctlCtx(t)); err != nil {
		t.Fatal(err)
	}
	if len(withdrawals) != 1 || !withdrawals[0] {
		t.Fatalf("withdrawals = %v; want one forced withdrawal before the replacement is admitted", withdrawals)
	}
}

// A generation fenced before its semantic-surface install creates no
// process-scoped consumer; an unfenced one installs and reports the error.
func TestRuntimeControlSurfaceInstallRefusedAfterFence(t *testing.T) {
	adm := &genAdmission{}
	scopes := make(chan *ebusGenerationScope, 1)
	runner := genRunner(t, ebusGenerationTaskConfig{Admitted: adm.admitted, JoinTimeout: 50 * time.Millisecond, Tasks: []ebusGenerationTask{
		{Name: "surface", Run: func(_ context.Context, scope *ebusGenerationScope) error { scopes <- scope; return nil }},
	}})
	key := genKey("inc.surface.e1", 1, 1, 1)
	adm.set(key)
	if err := runner.Admit(key); err != nil {
		t.Fatal(err)
	}
	scope := <-scopes
	want := errors.New("rctl: install failure")
	if err := installProcessSurface(scope, func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("live install = %v; want the install error", err)
	}
	if !runner.Fence(key) {
		t.Fatal("Fence reported false")
	}
	installs := 0
	if err := installProcessSurface(scope, func() error { installs++; return want }); err != nil || installs != 0 {
		t.Fatalf("fenced install = %v after %d installs; want none", err, installs)
	}
}

// A STOP whose generation fence times out behind an in-flight effect defers
// the bridge STOP fence: the native endpoint stays open and nothing is
// reported cleaned, until a later STOP finds the generation serialized.
func TestRuntimeControlStopDefersBridgeFenceWhileEffectInFlight(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	scopes := make(chan *ebusGenerationScope, 1)
	runner, err := newEBusGenerationTaskRunner(ebusGenerationTaskConfig{
		Tasks: []ebusGenerationTask{{Name: "idle", Run: func(_ context.Context, scope *ebusGenerationScope) error {
			scopes <- scope
			return nil
		}}},
		Admitted:    ebusControlGenerationAdmitted(plant.bridge),
		JoinTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	executor := &ebusControlExecutor{bridge: plant.bridge}
	executor.bind(runner, func(bool) {})
	corr := ctlCorr("inc.defer.e1", 1, 1)
	if err := executor.Start(ctlCtx(t), corr); err != nil {
		t.Fatal(err)
	}
	var scope *ebusGenerationScope
	select {
	case scope = <-scopes:
	case <-time.After(rctlBound):
		t.Fatal("task did not start")
	}
	inCommit, release := make(chan struct{}), make(chan struct{})
	go scope.Commit(func() { close(inCommit); <-release })
	select {
	case <-inCommit:
	case <-time.After(rctlBound):
		t.Fatal("commit did not start")
	}
	out := executor.Stop(ctlCtx(t), corr)
	if out.NativeClosed || out.SemanticCleaned {
		t.Fatalf("STOP outcome = %+v; want nothing cleaned while the effect is in flight", out)
	}
	if view := plant.bridge.View(); !view.Admitted {
		t.Fatalf("bridge view = %+v; want the generation still admitted (bridge fence deferred)", view)
	}
	close(release)
	var final runtimecontrol.StopOutcome
	ctlWait(t, "deferred STOP completes", func() bool {
		final = executor.Stop(ctlCtx(t), corr)
		return final.NativeClosed
	})
	if !final.SemanticCleaned {
		t.Fatalf("final STOP outcome = %+v; want cleaned", final)
	}
}

// --- G5 PR 2 (issue #1227): the composed control service through ordinary MCP ---
//
// The tests below rerun the fake-endpoint runGatewayLifecycle integration
// through the five helianthus.runtime tools of the production HTTP shell
// (eeBUS absent, loopback listener), and compare the MCP outcomes with the
// same requests made directly against the durable service.

var rctlMCPToolNames = []string{
	mcp.RuntimeControlListTool,
	mcp.RuntimeControlGetTool,
	mcp.RuntimeControlStartTool,
	mcp.RuntimeControlStopTool,
	mcp.RuntimeControlRestartTool,
}

// rctlWireDriver is the D4 wire projection of one driver.
type rctlWireDriver struct {
	DriverID          string `json:"driver_id"`
	Enabled           bool   `json:"enabled"`
	DesiredState      string `json:"desired_state"`
	DesiredRevision   string `json:"desired_revision"`
	ObservedState     string `json:"observed_state"`
	Reason            string `json:"reason"`
	SafetyQuarantined bool   `json:"safety_quarantined"`
	Revision          string `json:"revision"`
}

type rctlWireList struct {
	CatalogRevision string           `json:"catalog_revision"`
	Drivers         []rctlWireDriver `json:"drivers"`
}

// rctlToolResult is one decoded tools/call envelope; the data hash was
// verified against the exact serialized data bytes.
type rctlToolResult struct {
	isError   bool
	data      json.RawMessage
	code      string
	message   string
	retriable bool
	layer     string
	meta      map[string]any
}

type rctlRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

var rctlHTTPClient = &http.Client{Timeout: rctlBound}

func rctlRPCBody(method, params string) string {
	body := `{"jsonrpc":"2.0","id":1,"method":"` + method + `"`
	if params != "" {
		body += `,"params":` + params
	}
	return body + `}`
}

func rctlDecodeRPC(t *testing.T, raw []byte) (json.RawMessage, *rctlRPCError) {
	t.Helper()
	var response struct {
		Result json.RawMessage `json:"result"`
		Error  *rctlRPCError   `json:"error"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatalf("JSON-RPC response %q: %v", raw, err)
	}
	return response.Result, response.Error
}

func rctlDecodeTool(t *testing.T, result json.RawMessage) rctlToolResult {
	t.Helper()
	var call struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(result, &call); err != nil || len(call.Content) != 1 {
		t.Fatalf("tools/call result %s: %v", result, err)
	}
	var envelope struct {
		Meta  map[string]any  `json:"meta"`
		Data  json.RawMessage `json:"data"`
		Error *struct {
			Code        string `json:"code"`
			Message     string `json:"message"`
			Retriable   bool   `json:"retriable"`
			SourceLayer string `json:"source_layer"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(call.Content[0].Text), &envelope); err != nil {
		t.Fatalf("envelope %q: %v", call.Content[0].Text, err)
	}
	sum := sha256.Sum256(envelope.Data)
	if envelope.Meta["data_hash"] != hex.EncodeToString(sum[:]) {
		t.Fatalf("data_hash is not SHA-256 of the serialized data: %s", call.Content[0].Text)
	}
	out := rctlToolResult{isError: call.IsError, data: envelope.Data, meta: envelope.Meta}
	if envelope.Error != nil {
		out.code, out.message, out.retriable, out.layer = envelope.Error.Code, envelope.Error.Message, envelope.Error.Retriable, envelope.Error.SourceLayer
	}
	if out.isError != (envelope.Error != nil) {
		t.Fatalf("isError %v disagrees with error %v", out.isError, envelope.Error)
	}
	return out
}

// rctlPost sends one JSON-RPC request to the ordinary MCP HTTP endpoint.
func rctlPost(t *testing.T, url, method, params string) (json.RawMessage, *rctlRPCError) {
	t.Helper()
	response, err := rctlHTTPClient.Post(url, "application/json", strings.NewReader(rctlRPCBody(method, params)))
	if err != nil {
		t.Fatalf("POST %s: %v", method, err)
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return rctlDecodeRPC(t, raw)
}

func rctlCallParams(name, args string) string {
	return `{"name":"` + name + `","arguments":` + args + `}`
}

func rctlMutationArgs(id runtimecontrol.DriverID, expected string) string {
	return fmt.Sprintf(`{"DriverID":%q,"ExpectedRevision":%q}`, id, expected)
}

func (process *rctlProcess) tool(name, args string) rctlToolResult {
	t := process.env.t
	t.Helper()
	if process.mcpURL == "" {
		t.Fatal("process has no ordinary MCP endpoint (start it with mcp: true)")
	}
	result, rpcErr := rctlPost(t, process.mcpURL, "tools/call", rctlCallParams(name, args))
	if rpcErr != nil {
		t.Fatalf("%s %s: JSON-RPC error %+v", name, args, rpcErr)
	}
	return rctlDecodeTool(t, result)
}

func (process *rctlProcess) mcpToolNames() map[string]bool {
	t := process.env.t
	t.Helper()
	result, rpcErr := rctlPost(t, process.mcpURL, "tools/list", "")
	if rpcErr != nil {
		t.Fatalf("tools/list: %+v", rpcErr)
	}
	var listed struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(result, &listed); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tool := range listed.Tools {
		names[tool.Name] = true
	}
	return names
}

func (process *rctlProcess) requireMCPTools() {
	t := process.env.t
	t.Helper()
	names := process.mcpToolNames()
	for _, name := range rctlMCPToolNames {
		if !names[name] {
			t.Fatalf("ordinary tools/list lacks %s", name)
		}
	}
}

// mcpList reads helianthus.runtime.list within the read bound.
func (process *rctlProcess) mcpList() rctlWireList {
	t := process.env.t
	t.Helper()
	begin := time.Now()
	result := process.tool(mcp.RuntimeControlListTool, `{}`)
	if elapsed := time.Since(begin); elapsed > rctlReadBound {
		t.Fatalf("list took %s; want within %s", elapsed, rctlReadBound)
	}
	if result.isError {
		t.Fatalf("list = %s %s", result.code, result.message)
	}
	var list rctlWireList
	if err := json.Unmarshal(result.data, &list); err != nil {
		t.Fatal(err)
	}
	for i, driver := range list.Drivers {
		if driver.Revision != list.CatalogRevision {
			t.Fatalf("list entry %s revision %s differs from catalog revision %s", driver.DriverID, driver.Revision, list.CatalogRevision)
		}
		if i > 0 && list.Drivers[i-1].DriverID >= driver.DriverID {
			t.Fatalf("list not in bytewise driver_id order: %+v", list.Drivers)
		}
	}
	return list
}

// mcpGet reads helianthus.runtime.get for ebus.primary within the read bound.
func (process *rctlProcess) mcpGet() rctlWireDriver {
	t := process.env.t
	t.Helper()
	begin := time.Now()
	result := process.tool(mcp.RuntimeControlGetTool, `{"DriverID":"`+primaryEBusDriverID+`"}`)
	if elapsed := time.Since(begin); elapsed > rctlReadBound {
		t.Fatalf("get took %s; want within %s", elapsed, rctlReadBound)
	}
	if result.isError {
		t.Fatalf("get = %s %s", result.code, result.message)
	}
	var data struct {
		CatalogRevision string         `json:"catalog_revision"`
		Driver          rctlWireDriver `json:"driver"`
	}
	if err := json.Unmarshal(result.data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Driver.Revision != data.CatalogRevision {
		t.Fatalf("get revision %s differs from its catalog revision %s", data.Driver.Revision, data.CatalogRevision)
	}
	return data.Driver
}

func (process *rctlProcess) mcpWaitObserved(want drivermanager.ObservedState) rctlWireDriver {
	t := process.env.t
	t.Helper()
	deadline := time.Now().Add(rctlBound)
	for {
		driver := process.mcpGet()
		if driver.ObservedState == string(want) {
			return driver
		}
		if time.Now().After(deadline) {
			t.Fatalf("MCP get observed %s/%s; want %s (trace %v)", driver.ObservedState, driver.Reason, want, process.env.events())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// mcpDecide submits one mutation for ebus.primary, retrying only
// TRANSITION_BUSY (the documented pruning window) with the same revision.
func (process *rctlProcess) mcpDecide(tool, expected string) rctlToolResult {
	process.env.t.Helper()
	deadline := time.Now().Add(rctlBound)
	for {
		result := process.tool(tool, rctlMutationArgs(primaryEBusDriverID, expected))
		if result.code != string(runtimecontrol.RefusalTransitionBusy) || time.Now().After(deadline) {
			return result
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// rctlWantDesired requires an accepted or no-op answer whose data is exactly
// {desired_state, desired_revision}.
func rctlWantDesired(t *testing.T, what string, result rctlToolResult, state runtimecontrol.DesiredState, revision uint64) {
	t.Helper()
	want := fmt.Sprintf(`{"desired_state":%q,"desired_revision":"%d"}`, state, revision)
	if result.isError || string(result.data) != want {
		t.Fatalf("%s = data %s error %s %q; want %s", what, result.data, result.code, result.message, want)
	}
}

func rctlRevision(t *testing.T, text string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil || strconv.FormatUint(value, 10) != text {
		t.Fatalf("revision %q is not a canonical decimal string", text)
	}
	return value
}

// rctlSameOutcome compares one MCP result with the outcome of the same
// request made directly against the service.
func rctlSameOutcome(t *testing.T, what string, got rctlToolResult, view runtimecontrol.DesiredView, err error) {
	t.Helper()
	if err == nil {
		want, _ := json.Marshal(view)
		if got.isError || string(got.data) != string(want) {
			t.Fatalf("%s: MCP %s %s, service accepted %s", what, got.code, got.data, want)
		}
		return
	}
	var refusal *runtimecontrol.Refusal
	if !errors.As(err, &refusal) {
		t.Fatalf("%s: service error %v is not a refusal", what, err)
	}
	wantData := "null"
	if refusal.Code == runtimecontrol.RefusalRevisionConflict && refusal.View != nil {
		raw, _ := json.Marshal(*refusal.View)
		wantData = string(raw)
	}
	if !got.isError || got.code != string(refusal.Code) || got.retriable != refusal.Retryable() || got.layer != "gateway" || string(got.data) != wantData {
		t.Fatalf("%s: MCP %s retriable=%v data=%s layer=%s; service %s retriable=%v data=%s", what, got.code, got.retriable, got.data, got.layer, refusal.Code, refusal.Retryable(), wantData)
	}
	if strings.Contains(got.message, "/") || strings.Contains(got.message, "runtimecontrol:") {
		t.Fatalf("%s: message %q is not sanitized", what, got.message)
	}
}

func (process *rctlProcess) mcpAdapterStatus() string {
	t := process.env.t
	t.Helper()
	result := process.tool("ebus.v1.runtime.status.get", `{}`)
	var data struct {
		Adapter struct {
			Status string `json:"status"`
		} `json:"adapter_status"`
	}
	if result.isError || json.Unmarshal(result.data, &data) != nil {
		t.Fatalf("ebus.v1.runtime.status.get = %s %s", result.code, result.data)
	}
	return data.Adapter.Status
}

// requireStatusParity keeps the existing eBUS MCP/GraphQL adapter status
// parity on the production HTTP shell.
func (process *rctlProcess) requireStatusParity(want string) {
	t := process.env.t
	t.Helper()
	mcpStatus, graphqlStatus := process.mcpAdapterStatus(), process.adapterStatus()
	if mcpStatus != graphqlStatus || (want != "" && mcpStatus != want) {
		t.Fatalf("adapter status MCP %q GraphQL %q; want both %q", mcpStatus, graphqlStatus, want)
	}
}

// requireNoLifecycleGraphQL proves that no lifecycle field, mutation or
// subscription was added to GraphQL.
func (process *rctlProcess) requireNoLifecycleGraphQL() {
	t := process.env.t
	t.Helper()
	handler, err := graphql.NewHandler(process.builder)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ __schema { queryType { fields { name } } mutationType { fields { name } } subscriptionType { fields { name } } } }"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	type fields struct {
		Fields []struct {
			Name string `json:"name"`
		} `json:"fields"`
	}
	var payload struct {
		Data struct {
			Schema struct {
				Query        *fields `json:"queryType"`
				Mutation     *fields `json:"mutationType"`
				Subscription *fields `json:"subscriptionType"`
			} `json:"__schema"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil || payload.Data.Schema.Query == nil {
		t.Fatalf("GraphQL introspection = %s: %v", response.Body.String(), err)
	}
	for _, root := range []*fields{payload.Data.Schema.Query, payload.Data.Schema.Mutation, payload.Data.Schema.Subscription} {
		if root == nil {
			continue
		}
		for _, field := range root.Fields {
			name := strings.ToLower(field.Name)
			for _, forbidden := range []string{"lifecycle", "runtimecontrol", "desiredstate", "desiredrevision", "startdriver", "stopdriver", "restartdriver", "driverstatus"} {
				if strings.Contains(name, forbidden) {
					t.Fatalf("GraphQL exposes lifecycle field %q", field.Name)
				}
			}
			if name == "driver" || name == "drivers" {
				t.Fatalf("GraphQL exposes lifecycle field %q", field.Name)
			}
		}
	}
}

// TestRuntimeControlMCPLifecycleThroughOrdinaryHTTP reruns the durable
// STOPPED boot through the ordinary MCP tools of the production HTTP shell
// with eeBUS absent: discovery and reads while eBUS is STOPPED with zero
// endpoint opens; START, STOP and RESTART answer after durable acceptance
// with desired state and revision only, and completion is observed through
// get; same-desired START and STOP are no-ops without a key record; every
// accepted RESTART executes one replacement, and a lost RESTART response is
// resolved by readback (old revision conflicts, a fresh revision restarts
// again). Status parity with GraphQL holds and GraphQL gains no lifecycle
// shape.
func TestRuntimeControlMCPLifecycleThroughOrdinaryHTTP(t *testing.T) {
	env := newRCtlEnv(t)
	env.seed(true, nil)
	process := env.start(rctlProcessOptions{mcp: true})
	process.requireMCPTools()
	service := process.control.Service()
	store := process.control.store

	stopped := process.mcpWaitObserved(drivermanager.ObservedStopped)
	if stopped.DesiredState != string(runtimecontrol.DesiredStopped) || !stopped.Enabled || stopped.DriverID != primaryEBusDriverID {
		t.Fatalf("durable STOPPED boot reads %+v", stopped)
	}
	list := process.mcpList()
	cached := service.List()
	if len(list.Drivers) != 1 || list.CatalogRevision != strconv.FormatUint(uint64(cached.Revision), 10) ||
		list.Drivers[0].DesiredRevision != strconv.FormatUint(uint64(cached.Drivers[0].DesiredRevision), 10) {
		t.Fatalf("MCP list %+v differs from the service's cached view %+v", list, cached)
	}
	for i := 0; i < 5; i++ {
		process.mcpList()
		process.mcpGet()
	}
	time.Sleep(50 * time.Millisecond)
	if got := env.dials.Load(); got != 0 {
		t.Fatalf("endpoint opens after MCP reads while STOPPED = %d; want 0", got)
	}
	process.requireStatusParity(adapterStatusStopped)
	process.requireNoLifecycleGraphQL()

	rev := rctlRevision(t, stopped.DesiredRevision)

	// START: answered after durable acceptance; completion from get.
	start := process.mcpDecide(mcp.RuntimeControlStartTool, strconv.FormatUint(rev, 10))
	rctlWantDesired(t, "START", start, runtimecontrol.DesiredRunning, rev+1)
	if record, err := store.ControlDriver(primaryEBusDriverID); err != nil || record.DesiredState != runtimestate.DesiredRunning || record.DesiredRevision != rev+1 {
		t.Fatalf("committed record when START answered = %+v (%v); want RUNNING/%d", record, err, rev+1)
	}
	rev++
	process.mcpWaitObserved(drivermanager.ObservedRunning)
	if got := env.dials.Load(); got != 1 {
		t.Fatalf("endpoint opens after START = %d; want 1", got)
	}
	process.requireStatusParity("")

	// Same-desired START: a no-op at the same revision, nothing opened,
	// nothing written.
	noop := process.mcpDecide(mcp.RuntimeControlStartTool, strconv.FormatUint(rev, 10))
	rctlWantDesired(t, "same-desired START", noop, runtimecontrol.DesiredRunning, rev)
	time.Sleep(50 * time.Millisecond)
	if record, _ := store.ControlDriver(primaryEBusDriverID); record.DesiredRevision != rev || env.dials.Load() != 1 {
		t.Fatalf("same-desired START changed revision %d or opened %d endpoints", record.DesiredRevision, env.dials.Load())
	}

	// A stale expected revision conflicts with the cached desired view,
	// exactly as the service answers the same request.
	stale := process.tool(mcp.RuntimeControlStopTool, rctlMutationArgs(primaryEBusDriverID, strconv.FormatUint(rev-1, 10)))
	ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
	view, err := service.Control(ctx, runtimecontrol.MethodStop, []byte(rctlMutationArgs(primaryEBusDriverID, strconv.FormatUint(rev-1, 10))))
	cancel()
	rctlSameOutcome(t, "stale STOP", stale, view, err)
	if stale.code != string(runtimecontrol.RefusalRevisionConflict) {
		t.Fatalf("stale STOP = %s; want REVISION_CONFLICT", stale.code)
	}

	// STOP, then a same-desired STOP, then RESTART while STOPPED.
	rctlWantDesired(t, "STOP", process.mcpDecide(mcp.RuntimeControlStopTool, strconv.FormatUint(rev, 10)), runtimecontrol.DesiredStopped, rev+1)
	rev++
	process.mcpWaitObserved(drivermanager.ObservedStopped)
	if !env.closed[1].Load() {
		t.Fatal("endpoint still open after STOPPED was observed")
	}
	process.requireStatusParity(adapterStatusStopped)
	rctlWantDesired(t, "same-desired STOP", process.mcpDecide(mcp.RuntimeControlStopTool, strconv.FormatUint(rev, 10)), runtimecontrol.DesiredStopped, rev)
	restartStopped := process.mcpDecide(mcp.RuntimeControlRestartTool, strconv.FormatUint(rev, 10))
	if restartStopped.code != string(runtimecontrol.RefusalFailedPrecondition) || restartStopped.retriable || string(restartStopped.data) != "null" {
		t.Fatalf("RESTART while desired STOPPED = %s retriable=%v data=%s; want FAILED_PRECONDITION", restartStopped.code, restartStopped.retriable, restartStopped.data)
	}

	// START, then two accepted RESTARTs: each takes a new revision and runs
	// exactly one replacement.
	rctlWantDesired(t, "second START", process.mcpDecide(mcp.RuntimeControlStartTool, strconv.FormatUint(rev, 10)), runtimecontrol.DesiredRunning, rev+1)
	rev++
	process.mcpWaitObserved(drivermanager.ObservedRunning)
	for dials := int32(3); dials <= 4; dials++ {
		rctlWantDesired(t, "RESTART", process.mcpDecide(mcp.RuntimeControlRestartTool, strconv.FormatUint(rev, 10)), runtimecontrol.DesiredRunning, rev+1)
		rev++
		env.waitFor("restart replacement dial", func() bool { return env.dials.Load() == dials })
		process.mcpWaitObserved(drivermanager.ObservedRunning)
	}

	// Lost RESTART response: the accepted call's answer is dropped; the
	// retry with the old revision conflicts; readback shows the new
	// revision; after settlement a fresh-revision RESTART restarts again.
	lost := process.mcpDecide(mcp.RuntimeControlRestartTool, strconv.FormatUint(rev, 10))
	if lost.isError {
		t.Fatalf("RESTART = %s", lost.code)
	}
	retry := process.tool(mcp.RuntimeControlRestartTool, rctlMutationArgs(primaryEBusDriverID, strconv.FormatUint(rev, 10)))
	wantView := fmt.Sprintf(`{"desired_state":"RUNNING","desired_revision":"%d"}`, rev+1)
	if retry.code != string(runtimecontrol.RefusalRevisionConflict) || string(retry.data) != wantView {
		t.Fatalf("old-revision RESTART retry = %s %s; want REVISION_CONFLICT with %s", retry.code, retry.data, wantView)
	}
	rev++
	if readback := process.mcpGet(); readback.DesiredRevision != strconv.FormatUint(rev, 10) || readback.DesiredState != "RUNNING" {
		t.Fatalf("readback after a lost RESTART = %+v; want RUNNING/%d", readback, rev)
	}
	env.waitFor("lost RESTART replacement dial", func() bool { return env.dials.Load() == 5 })
	process.mcpWaitObserved(drivermanager.ObservedRunning)
	rctlWantDesired(t, "fresh-revision RESTART", process.mcpDecide(mcp.RuntimeControlRestartTool, strconv.FormatUint(rev, 10)), runtimecontrol.DesiredRunning, rev+1)
	rev++
	env.waitFor("another restart", func() bool { return env.dials.Load() == 6 })
	process.mcpWaitObserved(drivermanager.ObservedRunning)
	process.requireStatusParity("")
	if closeAt, dialAt := env.index("close 5"), env.index("dial 6"); closeAt < 0 || dialAt < 0 || closeAt > dialAt {
		t.Fatalf("RESTART through MCP did not close the predecessor before the successor dialled: trace %v", env.events())
	}

	// Shutdown keeps the last accepted intent; no synthetic STOP.
	process.stop()
	stored := env.storedRecord()
	if stored.DesiredState != runtimestate.DesiredRunning || stored.DesiredRevision != rev {
		t.Fatalf("stored intent after shutdown = %s/%d; want RUNNING/%d", stored.DesiredState, stored.DesiredRevision, rev)
	}
}

// TestRuntimeControlMCPToolsWhileEBusFailed: with corrupt control state or
// no instance GUID, eBUS is FAILED, yet the five tools stay listed and the
// reads answer; a mutation is refused exactly as the service refuses it.
func TestRuntimeControlMCPToolsWhileEBusFailed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*rctlEnv)
	}{
		{name: "corrupt_control_state", prepare: func(env *rctlEnv) {
			if err := os.WriteFile(env.statePath, []byte("{not json"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "no_instance_guid", prepare: func(env *rctlEnv) { env.guid = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newRCtlEnv(t)
			tc.prepare(env)
			process := env.start(rctlProcessOptions{mcp: true})
			process.requireMCPTools()
			failed := process.mcpWaitObserved(drivermanager.ObservedFailed)
			t.Logf("FAILED view: enabled=%v desired=%s/%s reason=%s revision=%s", failed.Enabled, failed.DesiredState, failed.DesiredRevision, failed.Reason, failed.Revision)
			if list := process.mcpList(); len(list.Drivers) != 1 || list.Drivers[0].ObservedState != string(drivermanager.ObservedFailed) {
				t.Fatalf("list while FAILED = %+v", list)
			}
			expected := failed.DesiredRevision
			if expected == "0" {
				expected = "1"
			}
			args := rctlMutationArgs(primaryEBusDriverID, expected)
			for _, method := range []runtimecontrol.Method{runtimecontrol.MethodStart, runtimecontrol.MethodRestart} {
				name := mcp.RuntimeControlStartTool
				if method == runtimecontrol.MethodRestart {
					name = mcp.RuntimeControlRestartTool
				}
				got := process.tool(name, args)
				ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
				view, err := process.control.Service().Control(ctx, method, []byte(args))
				cancel()
				if err == nil {
					t.Fatalf("%s accepted while eBUS FAILED: %+v", method, view)
				}
				rctlSameOutcome(t, string(method), got, view, err)
				t.Logf("%s while eBUS FAILED: %s retriable=%v %q", method, got.code, got.retriable, got.message)
			}
			time.Sleep(50 * time.Millisecond)
			if env.dials.Load() != 0 {
				t.Fatalf("endpoint opens while FAILED = %d; want 0", env.dials.Load())
			}
		})
	}
}

// TestRuntimeControlMCPDisabledDriverStaysVisible: a disabled driver stays
// listed through MCP; START, STOP and RESTART are FAILED_PRECONDITION at
// refusal row 4 as the service answers them, and durable intent is kept.
func TestRuntimeControlMCPDisabledDriverStaysVisible(t *testing.T) {
	env := newRCtlEnv(t)
	env.seed(false, nil)
	before := env.storedRecord()
	process := env.start(rctlProcessOptions{mcp: true, mutate: func(o *gatewayRuntimeControlOptions) { o.Disabled = true }})
	process.requireMCPTools()
	disabled := process.mcpWaitObserved(drivermanager.ObservedDisabled)
	if disabled.Enabled {
		t.Fatalf("disabled driver reads %+v", disabled)
	}
	if list := process.mcpList(); len(list.Drivers) != 1 || list.Drivers[0].Enabled {
		t.Fatalf("list = %+v; want the disabled driver visible", list)
	}
	args := rctlMutationArgs(primaryEBusDriverID, disabled.DesiredRevision)
	for method, name := range map[runtimecontrol.Method]string{
		runtimecontrol.MethodStart:   mcp.RuntimeControlStartTool,
		runtimecontrol.MethodStop:    mcp.RuntimeControlStopTool,
		runtimecontrol.MethodRestart: mcp.RuntimeControlRestartTool,
	} {
		got := process.tool(name, args)
		if got.code != string(runtimecontrol.RefusalFailedPrecondition) || got.retriable {
			t.Fatalf("%s on a disabled driver = %s retriable=%v", method, got.code, got.retriable)
		}
		ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
		view, err := process.control.Service().Control(ctx, method, []byte(args))
		cancel()
		rctlSameOutcome(t, string(method), got, view, err)
	}
	process.stop()
	after := env.storedRecord()
	if after.DesiredState != before.DesiredState || after.DesiredRevision != before.DesiredRevision || env.dials.Load() != 0 {
		t.Fatalf("disabled driver: intent %s/%d then %s/%d, %d endpoint opens", before.DesiredState, before.DesiredRevision, after.DesiredState, after.DesiredRevision, env.dials.Load())
	}
}

// rctlFaultHooks injects persistence faults into the durable store: a
// failing temporary-file write (a pre-commit failure) and a write held at a
// gate (the namespace-wide serialization point stays held).
type rctlFaultHooks struct {
	runtimestate.DefaultFilesystemHooks
	failWrite atomic.Bool
	mu        sync.Mutex
	gate      chan struct{}
	entered   chan struct{}
}

func (hooks *rctlFaultHooks) WriteFile(path string, data []byte, perm uint32) error {
	if hooks.failWrite.Load() {
		return &os.PathError{Op: "open", Path: path, Err: syscall.ENOSPC}
	}
	hooks.mu.Lock()
	gate, entered := hooks.gate, hooks.entered
	hooks.mu.Unlock()
	if gate != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-gate
	}
	return hooks.DefaultFilesystemHooks.WriteFile(path, data, perm)
}

func (hooks *rctlFaultHooks) hold() (<-chan struct{}, func()) {
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	gate, entered := make(chan struct{}), make(chan struct{}, 1)
	hooks.gate, hooks.entered = gate, entered
	var once sync.Once
	return entered, func() {
		once.Do(func() {
			hooks.mu.Lock()
			hooks.gate, hooks.entered = nil, nil
			hooks.mu.Unlock()
			close(gate)
		})
	}
}

type rctlCountingExecutor struct{ starts, stops atomic.Int32 }

func (executor *rctlCountingExecutor) Start(context.Context, runtimecontrol.LifecycleCorrelation) error {
	executor.starts.Add(1)
	return nil
}

func (executor *rctlCountingExecutor) Stop(context.Context, runtimecontrol.LifecycleCorrelation) runtimecontrol.StopOutcome {
	executor.stops.Add(1)
	return runtimecontrol.StopOutcome{NativeClosed: true, SemanticCleaned: true}
}

type rctlNoRecovery struct{}

func (rctlNoRecovery) Restore(context.Context, *runtimecontrol.RecoveryMaterial) error { return nil }
func (rctlNoRecovery) Secure(context.Context, runtimecontrol.DriverID, runtimecontrol.Transition) (*runtimecontrol.RecoveryMaterial, error) {
	return nil, nil
}

type rctlCountingInvoker struct{ calls atomic.Int32 }

func (invoker *rctlCountingInvoker) Invoke(context.Context, router.Plane, string, map[string]any) (any, error) {
	invoker.calls.Add(1)
	return nil, errors.New("rctl: native invoke is not a lifecycle path")
}

// TestRuntimeControlMCPDurableServiceEquivalence drives the production registration
// helper over the durable service with its real store (fault-injected file
// hooks, two-driver namespace plus a disabled driver) through the ordinary
// MCP HTTP handler, and compares every outcome with the same request made
// directly against the service: success, same-desired no-op, invalid,
// unregistered, disabled, stale revision, busy namespace, expired deadline
// and pre-commit persistence failure. Reads stay immediate while a decision
// holds the serialization point, and never start an executor.
func TestRuntimeControlMCPDurableServiceEquivalence(t *testing.T) {
	const (
		second   = runtimecontrol.DriverID("fixture.second")
		disabled = runtimecontrol.DriverID("fixture.disabled")
	)
	ctx := context.Background()
	hooks := &rctlFaultHooks{}
	store := runtimestate.New(runtimestate.Options{Path: filepath.Join(t.TempDir(), "runtime_state.json"), FsHooks: hooks, ControlWaitBound: 150 * time.Millisecond})
	if _, err := store.Load(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Stop(context.Background()) })
	if err := store.EagerPersistInstanceGUID(ctx, rctlGUID, runtimestate.IdentitySourceRuntimeState); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BootstrapControl(ctx, []runtimestate.ControlDriverConfig{
		{DriverID: primaryEBusDriverID, Enabled: true}, {DriverID: string(second), Enabled: true}, {DriverID: string(disabled), Enabled: false},
	}); err != nil {
		t.Fatal(err)
	}
	executors := map[runtimecontrol.DriverID]*rctlCountingExecutor{primaryEBusDriverID: {}, second: {}, disabled: {}}
	var drivers []runtimecontrol.DriverConfig
	for id, executor := range executors {
		drivers = append(drivers, runtimecontrol.DriverConfig{DriverID: id, Executor: executor})
	}
	service, err := runtimecontrol.New(runtimecontrol.Options{Store: &runtimeControlStore{m: store}, Recovery: rctlNoRecovery{}, Drivers: drivers, Incarnation: "rctl-mcp"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)

	invoker := &rctlCountingInvoker{}
	server, err := mcp.NewServer(registry.NewDeviceRegistry(nil), invoker)
	if err != nil {
		t.Fatal(err)
	}
	if err := registerRuntimeControlMCPTools(server, &gatewayRuntimeControl{service: service}); err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	call := func(callCtx context.Context, name, args string) rctlToolResult {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(rctlRPCBody("tools/call", rctlCallParams(name, args)))).WithContext(callCtx)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		result, rpcErr := rctlDecodeRPC(t, recorder.Body.Bytes())
		if rpcErr != nil {
			t.Fatalf("%s %s: JSON-RPC error %+v", name, args, rpcErr)
		}
		return rctlDecodeTool(t, result)
	}
	direct := func(callCtx context.Context, method runtimecontrol.Method, args string) (runtimecontrol.DesiredView, error) {
		bounded, cancel := context.WithTimeout(callCtx, rctlBound)
		defer cancel()
		return service.Control(bounded, method, []byte(args))
	}
	status := func(id runtimecontrol.DriverID) runtimecontrol.DriverStatus {
		t.Helper()
		got, err := service.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	revision := func(id runtimecontrol.DriverID) string {
		return strconv.FormatUint(uint64(status(id).DesiredRevision), 10)
	}
	waitObserved := func(id runtimecontrol.DriverID, want drivermanager.ObservedState) {
		t.Helper()
		ctlWait(t, string(id)+" "+string(want), func() bool { return status(id).ObservedState == want })
	}
	decideMCP := func(name string, id runtimecontrol.DriverID, expected string) rctlToolResult {
		t.Helper()
		deadline := time.Now().Add(rctlBound)
		for {
			got := call(ctx, name, rctlMutationArgs(id, expected))
			if got.code != string(runtimecontrol.RefusalTransitionBusy) || time.Now().After(deadline) {
				return got
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitObserved(primaryEBusDriverID, drivermanager.ObservedRunning)
	waitObserved(second, drivermanager.ObservedRunning)
	waitObserved(disabled, drivermanager.ObservedDisabled)

	// Reads: one coherent cached view, no executor call.
	starts := executors[primaryEBusDriverID].starts.Load() + executors[second].starts.Load()
	listed := call(ctx, mcp.RuntimeControlListTool, `{}`)
	var list rctlWireList
	if err := json.Unmarshal(listed.data, &list); err != nil || len(list.Drivers) != 3 {
		t.Fatalf("list = %s (%v)", listed.data, err)
	}
	if list.Drivers[0].DriverID != primaryEBusDriverID || list.Drivers[1].DriverID != string(disabled) || list.Drivers[2].DriverID != string(second) {
		t.Fatalf("list order = %+v; want bytewise driver_id order", list.Drivers)
	}
	if list.Drivers[1].Enabled || list.Drivers[1].ObservedState != string(drivermanager.ObservedDisabled) {
		t.Fatalf("disabled driver reads %+v", list.Drivers[1])
	}
	for i := 0; i < 5; i++ {
		call(ctx, mcp.RuntimeControlGetTool, `{"DriverID":"fixture.second"}`)
	}
	if got := executors[primaryEBusDriverID].starts.Load() + executors[second].starts.Load(); got != starts {
		t.Fatalf("reads started %d executors", got-starts)
	}

	// Success, then a same-desired no-op that writes nothing.
	rev := revision(primaryEBusDriverID)
	accepted := decideMCP(mcp.RuntimeControlStopTool, primaryEBusDriverID, rev)
	if accepted.isError {
		t.Fatalf("STOP = %s %s", accepted.code, accepted.message)
	}
	committed, _ := store.ControlDriver(primaryEBusDriverID)
	wantAccepted, _ := json.Marshal(runtimecontrol.DesiredView{DesiredState: runtimecontrol.DesiredStopped, DesiredRevision: runtimecontrol.DesiredRevision(committed.DesiredRevision)})
	if string(accepted.data) != string(wantAccepted) || committed.DesiredState != runtimestate.DesiredStopped || strconv.FormatUint(committed.DesiredRevision, 10) == rev {
		t.Fatalf("STOP answered %s; committed record %+v", accepted.data, committed)
	}
	waitObserved(primaryEBusDriverID, drivermanager.ObservedStopped)
	catalogBefore := service.List().Revision
	noop := call(ctx, mcp.RuntimeControlStopTool, rctlMutationArgs(primaryEBusDriverID, revision(primaryEBusDriverID)))
	view, err := direct(ctx, runtimecontrol.MethodStop, rctlMutationArgs(primaryEBusDriverID, revision(primaryEBusDriverID)))
	rctlSameOutcome(t, "same-desired STOP", noop, view, err)
	if service.List().Revision != catalogBefore {
		t.Fatal("a same-desired no-op advanced the catalog revision")
	}

	// Refusals that change nothing: identical through MCP and directly.
	for _, tc := range []struct {
		name   string
		tool   string
		method runtimecontrol.Method
		args   string
		code   runtimecontrol.RefusalCode
	}{
		{"invalid", mcp.RuntimeControlStartTool, runtimecontrol.MethodStart, `{"DriverID":"ebus.primary"}`, runtimecontrol.RefusalInvalidArgument},
		{"unknown_member", mcp.RuntimeControlStartTool, runtimecontrol.MethodStart, `{"DriverID":"ebus.primary","ExpectedRevision":"1","receipt":"r"}`, runtimecontrol.RefusalInvalidArgument},
		{"unregistered", mcp.RuntimeControlStartTool, runtimecontrol.MethodStart, rctlMutationArgs("fixture.unknown", "1"), runtimecontrol.RefusalNotFound},
		{"disabled", mcp.RuntimeControlStartTool, runtimecontrol.MethodStart, rctlMutationArgs(disabled, revision(disabled)), runtimecontrol.RefusalFailedPrecondition},
		{"stale", mcp.RuntimeControlStartTool, runtimecontrol.MethodStart, rctlMutationArgs(primaryEBusDriverID, rev), runtimecontrol.RefusalRevisionConflict},
		{"restart_while_stopped", mcp.RuntimeControlRestartTool, runtimecontrol.MethodRestart, rctlMutationArgs(primaryEBusDriverID, revision(primaryEBusDriverID)), runtimecontrol.RefusalFailedPrecondition},
	} {
		got := call(ctx, tc.tool, tc.args)
		view, err := direct(ctx, tc.method, tc.args)
		rctlSameOutcome(t, tc.name, got, view, err)
		if got.code != string(tc.code) {
			t.Fatalf("%s = %s; want %s", tc.name, got.code, tc.code)
		}
	}

	// Expired caller deadline: the adapter keeps the earlier deadline.
	expired, cancelExpired := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancelExpired()
	late := call(expired, mcp.RuntimeControlStartTool, rctlMutationArgs(primaryEBusDriverID, revision(primaryEBusDriverID)))
	view, err = service.Control(expired, runtimecontrol.MethodStart, []byte(rctlMutationArgs(primaryEBusDriverID, revision(primaryEBusDriverID))))
	rctlSameOutcome(t, "expired deadline", late, view, err)
	if late.code != string(runtimecontrol.RefusalDeadlineExceeded) || !late.retriable {
		t.Fatalf("expired deadline = %s retriable=%v; want retriable DEADLINE_EXCEEDED", late.code, late.retriable)
	}

	// Busy namespace: a decision whose durable write is held keeps the
	// serialization point; reads stay immediate, a current-revision
	// decision for another driver is TRANSITION_BUSY and a stale one is
	// REVISION_CONFLICT, through MCP exactly as directly.
	entered, release := hooks.hold()
	defer release()
	held := make(chan rctlToolResult, 1)
	startRev := revision(primaryEBusDriverID)
	go func() {
		held <- call(ctx, mcp.RuntimeControlStartTool, rctlMutationArgs(primaryEBusDriverID, startRev))
	}()
	select {
	case <-entered:
	case <-time.After(rctlBound):
		t.Fatal("held decision never reached its durable write")
	}
	begin := time.Now()
	call(ctx, mcp.RuntimeControlListTool, `{}`)
	call(ctx, mcp.RuntimeControlGetTool, `{"DriverID":"fixture.second"}`)
	if elapsed := time.Since(begin); elapsed > rctlReadBound {
		t.Fatalf("reads took %s while a decision held the serialization point", elapsed)
	}
	secondRev := status(second).DesiredRevision
	current := rctlMutationArgs(second, strconv.FormatUint(uint64(secondRev), 10))
	busy := call(ctx, mcp.RuntimeControlStopTool, current)
	view, err = direct(ctx, runtimecontrol.MethodStop, current)
	rctlSameOutcome(t, "busy", busy, view, err)
	if busy.code != string(runtimecontrol.RefusalTransitionBusy) || !busy.retriable {
		t.Fatalf("busy namespace = %s; want retriable TRANSITION_BUSY", busy.code)
	}
	staleSecond := rctlMutationArgs(second, strconv.FormatUint(uint64(secondRev)-1, 10))
	conflict := call(ctx, mcp.RuntimeControlStopTool, staleSecond)
	view, err = direct(ctx, runtimecontrol.MethodStop, staleSecond)
	rctlSameOutcome(t, "stale while busy", conflict, view, err)
	if conflict.code != string(runtimecontrol.RefusalRevisionConflict) {
		t.Fatalf("stale decision while busy = %s; want REVISION_CONFLICT", conflict.code)
	}
	release()
	if got := <-held; got.isError {
		t.Fatalf("held START = %s %s", got.code, got.message)
	}
	waitObserved(primaryEBusDriverID, drivermanager.ObservedRunning)

	// Pre-commit persistence failure: refused UNAVAILABLE (retryable),
	// nothing accepted, no path in the message; then the same request
	// succeeds once storage recovers.
	hooks.failWrite.Store(true)
	beforeFailure, _ := store.ControlDriver(string(second))
	failure := call(ctx, mcp.RuntimeControlStopTool, current)
	view, err = direct(ctx, runtimecontrol.MethodStop, current)
	rctlSameOutcome(t, "pre-commit failure", failure, view, err)
	if failure.code != string(runtimecontrol.RefusalUnavailable) || !failure.retriable {
		t.Fatalf("pre-commit failure = %s retriable=%v; want retriable UNAVAILABLE", failure.code, failure.retriable)
	}
	if afterFailure, _ := store.ControlDriver(string(second)); afterFailure.DesiredRevision != beforeFailure.DesiredRevision || afterFailure.DesiredState != beforeFailure.DesiredState {
		t.Fatalf("pre-commit failure changed the record: %+v then %+v", beforeFailure, afterFailure)
	}
	hooks.failWrite.Store(false)
	if recovered := decideMCP(mcp.RuntimeControlStopTool, second, strconv.FormatUint(uint64(secondRev), 10)); recovered.isError {
		t.Fatalf("STOP after storage recovered = %s %s", recovered.code, recovered.message)
	}
	waitObserved(second, drivermanager.ObservedStopped)
	if got := invoker.calls.Load(); got != 0 {
		t.Fatalf("lifecycle tools invoked native RPC %d times", got)
	}
}

// TestRuntimeControlMCPProductionWiring pins the production composition the
// harness cannot reach behaviourally: startHTTPServer registers the
// lifecycle tools unconditionally (not behind an eeBUS or driver condition)
// before the MCP handler is mounted, and the lifecycle closure hands the
// composed control path to startHTTPServer.
func TestRuntimeControlMCPProductionWiring(t *testing.T) {
	fset := token.NewFileSet()
	serverFile, err := parser.ParseFile(fset, "gateway_http_server.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var registerAt, mountAt token.Pos
	guarded := false
	for _, declaration := range serverFile.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "startHTTPServer" {
			continue
		}
		last := function.Type.Params.List[len(function.Type.Params.List)-1]
		if star, ok := last.Type.(*ast.StarExpr); !ok || fmt.Sprint(star.X) != "gatewayRuntimeControl" {
			t.Fatalf("startHTTPServer's last parameter is not *gatewayRuntimeControl")
		}
		for _, statement := range function.Body.List {
			ast.Inspect(statement, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if ident, ok := call.Fun.(*ast.Ident); ok {
					switch ident.Name {
					case "registerRuntimeControlMCPTools":
						registerAt = call.Pos()
						// Only the error check of the call itself may wrap it.
						if ifStatement, isIf := statement.(*ast.IfStmt); isIf {
							guarded = ifStatement.Init == nil || call.Pos() < ifStatement.Init.Pos() || call.End() > ifStatement.Init.End()
						} else if _, isExpr := statement.(*ast.ExprStmt); !isExpr {
							guarded = true
						}
					case "registerHTTPControlPlaneCoreRoutes":
						mountAt = call.Pos()
					}
				}
				return true
			})
		}
	}
	if !registerAt.IsValid() || !mountAt.IsValid() || registerAt > mountAt || guarded {
		t.Fatalf("startHTTPServer must register the lifecycle tools unconditionally before mounting MCP: register=%v mount=%v guarded=%v", registerAt, mountAt, guarded)
	}

	lifecycleFile, err := parser.ParseFile(fset, "gateway_run_lifecycle.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	passed := 0
	ast.Inspect(lifecycleFile, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "startHTTPServer" {
			if last, ok := call.Args[len(call.Args)-1].(*ast.Ident); ok && last.Name == "control" {
				passed++
			}
		}
		return true
	})
	if passed != 1 {
		t.Fatalf("gateway_run_lifecycle.go hands the composed control to startHTTPServer %d times; want 1", passed)
	}
}
