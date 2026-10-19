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
// No device, socket bind or network is used, and every wait is bounded.

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
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
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
}

type rctlProcessOptions struct {
	mutate func(*gatewayRuntimeControlOptions)
	config func(*ebusgateway.Config)
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
	startHTTPServerFn = func(_ context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, builder *graphql.Builder, _ *graphql.BroadcastHub, _ graphql.SemanticProvider, _ mcp.ScheduleWriter, _ mcp.ConfigWriter, _ *ebusgateway.BusObservabilityStore, _ *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		env.event("http_shell")
		shell <- builder
		return nil, nil, nil
	}
	gatewayRuntimeControlOptionsFor = func(o gatewayRuntimeControlOptions) gatewayRuntimeControlOptions {
		o.CleanupTimeout = 2 * time.Second
		if opts.mutate != nil {
			opts.mutate(&o)
		}
		return o
	}
	gatewayRuntimeControlStarted = func(control *gatewayRuntimeControl) { controlReady <- control }

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
