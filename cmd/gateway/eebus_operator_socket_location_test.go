package main

// M71 regression (issue #1360): the eeBUS v1 operator MCP socket lives beside
// the eeBUS state root, never inside it, and a stale socket an earlier build
// left inside the root is cleared before the eeBUS store opens.
//
// The process is the production lifecycle (run) with the production HTTP
// control plane and the add-on eeBUS flag set; every value is synthetic. The
// replaced parts are the eBUS endpoint (the rctl in-process pipe), the
// control-plane TCP listener (in memory), the interface resolver and the
// eebusreg factories. The factories build a working runtime whose Start opens
// the state root the way the eeBUS store does, after the operator endpoint
// started, as production orders it since the eeBUS start became asynchronous.
// On Linux the operator endpoint is the production AF_UNIX transport; other
// platforms have no peer-credential proof, so a plain owner-only listener at
// the path the control plane requests stands in for it.

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/releasecheck"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const (
	m71Interface = "end0"
	// m71StoreRejected is how the eeBUS facade reports a store whose root
	// layout it rejected.
	m71StoreRejected = "eebus runtime protected credentials are unavailable: protected runtime material: state unavailable"
	m71RemovedLine   = "eeBUS state root: removed stale operator-mcp.sock left by an earlier gateway build"
	m71LeftLine      = "eeBUS state root: operator-mcp.sock left in place"
	// The operator socket location lines: the fallback line carries the
	// reason category only, the unavailable line the cause category only.
	m71FallbackLine    = "eeBUS operator MCP socket: default location unusable"
	m71UnavailableLine = "eeBUS operator MCP endpoint unavailable"
)

// m71StoreTemporary matches the store's manifest temporaries.
var m71StoreTemporary = regexp.MustCompile(`^\.tmp-manifest-[a-z0-9]+$`)

// m71StoreEntry reports whether name is an entry the eeBUS store accepts in
// its root (helianthus-eebusreg v0.1.36, the store layout verification).
func m71StoreEntry(name string) bool {
	switch name {
	case "LOCK", "MANIFEST.A", "MANIFEST.B", "generations", "eebusmutation":
		return true
	}
	return m71StoreTemporary.MatchString(name)
}

// m71OpenStore opens the state root like the eeBUS store: an entry it does not
// own rejects the layout, which the facade reports as protected credentials
// unavailable; rejected receives the entry name. A first open creates the
// lock, the first manifest slot and the generations directory.
func m71OpenStore(root string, rejected func(string)) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		rejected("<unreadable root>")
		return errors.New(m71StoreRejected)
	}
	for _, entry := range entries {
		if !m71StoreEntry(entry.Name()) {
			rejected(entry.Name())
			return errors.New(m71StoreRejected)
		}
	}
	for _, name := range []string{"LOCK", "MANIFEST.A"} {
		file, err := os.OpenFile(filepath.Join(root, name), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	if err := os.Mkdir(filepath.Join(root, "generations"), 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return nil
}

// m71StoreRuntime is a working eeBUS runtime whose Start opens the store once
// the operator endpoint has started.
type m71StoreRuntime struct {
	msp05bRuntime
	root      string
	endpoints <-chan struct{}
	rejected  func(string)
}

func (runtime *m71StoreRuntime) Start(ctx context.Context) error {
	select {
	case <-runtime.endpoints:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(rctlBound):
		// The control plane assertions report a missing endpoint.
	}
	return m71OpenStore(runtime.root, runtime.rejected)
}

type m71LogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *m71LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *m71LogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// m71CaptureLog redirects the process log for the test. It is registered
// before the process starts, so it is restored after the process stopped.
func m71CaptureLog(t *testing.T) *m71LogBuffer {
	t.Helper()
	logs := &m71LogBuffer{}
	original := log.Writer()
	log.SetOutput(logs)
	t.Cleanup(func() { log.SetOutput(original) })
	return logs
}

func m71Lines(text, marker string) []string {
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, marker) {
			lines = append(lines, line)
		}
	}
	return lines
}

// m71Base returns a short private directory: AF_UNIX socket paths are bounded
// (104 bytes on macOS), and the state root and its operator sibling live
// below it.
func m71Base(t *testing.T) string {
	t.Helper()
	base, err := os.MkdirTemp("/tmp", "m71-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	if resolved, err := filepath.EvalSymlinks(base); err == nil {
		base = resolved
	}
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	return base
}

// m71StateRoot creates <base>/eebus with the store's owner-only mode.
func m71StateRoot(t *testing.T) (base, stateRoot string) {
	t.Helper()
	base = m71Base(t)
	stateRoot = filepath.Join(base, "eebus")
	if err := os.Mkdir(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	return base, stateRoot
}

// m71StaleSocket leaves a socket file without a listener at path, as a
// process that exited without unlinking it does.
func m71StaleSocket(t *testing.T, path string) {
	t.Helper()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

// m71Inventory lists the state root.
func m71Inventory(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

// m71AssertStoreOnly fails unless the state root holds store entries only and
// the store opened (its lock, first manifest slot and generations exist).
func m71AssertStoreOnly(t *testing.T, root string) {
	t.Helper()
	names := m71Inventory(t, root)
	for _, name := range names {
		if !m71StoreEntry(name) {
			t.Fatalf("eeBUS state root holds %q, which the store rejects; entries %q", name, names)
		}
	}
	for _, name := range []string{"LOCK", "MANIFEST.A", "generations"} {
		if _, err := os.Lstat(filepath.Join(root, name)); err != nil {
			t.Fatalf("eeBUS store did not open: %s missing (%v); entries %q", name, err, names)
		}
	}
}

// m71AddonEEBusConfig parses the eeBUS arguments the add-on wrapper emits.
func m71AddonEEBusConfig(t *testing.T, stateRoot string) ebusgateway.EEBusConfig {
	t.Helper()
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("addon", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{
		"-eebus-enabled=true",
		"-eebus-listen-port", "4712",
		"-eebus-interfaces", m71Interface,
		"-eebus-subnets", "192.0.2.0/24",
		"-eebus-state-root", stateRoot,
		"-eebus-discovery-enabled=true",
		"-eebus-remote-ski-allowlist", strings.Repeat("ab", 20),
		"-eebus-pairing-window-mode", "closed",
	}); err != nil {
		t.Fatalf("parse add-on eeBUS flags: %v", err)
	}
	return cfg.EEBusConfig
}

// m71PortableOperatorEndpoint stands in for the Linux-only production
// transport: an owner-only directory and socket at the requested path.
func m71PortableOperatorEndpoint(socketPath string) (io.Closer, error) {
	directory := filepath.Dir(socketPath)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}

type m71Process struct {
	t        *testing.T
	cancel   context.CancelFunc
	done     chan error
	control  *gatewayRuntimeControl
	sidecars *managedSidecars

	mu            sync.Mutex
	socketPaths   []string
	constructions []bool
	rejections    []string
}

func (p *m71Process) operatorSockets() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.socketPaths...)
}

func (p *m71Process) storeRejections() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.rejections...)
}

func (p *m71Process) natives() []bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]bool(nil), p.constructions...)
}

func (p *m71Process) stop() {
	if p.cancel == nil {
		return
	}
	p.cancel()
	p.cancel = nil
	select {
	case err := <-p.done:
		if err != nil {
			p.t.Errorf("run returned %v on process shutdown; want nil", err)
		}
	case <-time.After(3 * rctlBound):
		p.t.Error("run did not return after process shutdown")
	}
}

// m71Options varies the production lifecycle of m71Start.
type m71Options struct {
	// configure adjusts the configuration after the add-on eeBUS flags.
	configure func(*ebusgateway.Config)
	// operatorError makes the operator endpoint fail to start at a path.
	operatorError func(socketPath string) error
}

// m71Start runs the production lifecycle with the add-on eeBUS configuration
// and waits until eebus.primary settles at RUNNING or FAILED.
func m71Start(t *testing.T, stateRoot string) (*m71Process, runtimecontrol.DriverStatus) {
	t.Helper()
	return m71StartWith(t, stateRoot, m71Options{})
}

func m71StartWith(t *testing.T, stateRoot string, opts m71Options) (*m71Process, runtimecontrol.DriverStatus) {
	t.Helper()
	env := newRCtlEnv(t)
	p := &m71Process{t: t, done: make(chan error, 1)}
	listener := newM57Listener()

	release, polling, discovery, background, httpServer := startGatewayReleaseChecker, startVaillantSemanticPollingFn, startDiscoveryScanLoopFn, startBackgroundFullScanFn, startHTTPServerFn
	options, started, listen, operator := gatewayRuntimeControlOptionsFor, gatewayRuntimeControlStarted, listenHTTPControlPlane, startEEBusOperatorEndpoint
	resolver, operatorFactory, factory := resolveEEBusInterfaceAddressesFn, newEEBusOperatorRuntimeFn, newEEBusRuntimeFn
	t.Cleanup(func() {
		startGatewayReleaseChecker, startVaillantSemanticPollingFn, startDiscoveryScanLoopFn, startBackgroundFullScanFn, startHTTPServerFn = release, polling, discovery, background, httpServer
		gatewayRuntimeControlOptionsFor, gatewayRuntimeControlStarted, listenHTTPControlPlane, startEEBusOperatorEndpoint = options, started, listen, operator
		resolveEEBusInterfaceAddressesFn, newEEBusOperatorRuntimeFn, newEEBusRuntimeFn = resolver, operatorFactory, factory
	})

	startGatewayReleaseChecker = func(*releasecheck.Checker, context.Context) {}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return nil
	}
	startDiscoveryScanLoopFn = func(ctx context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.Builder, _ activeTxnClassifier) startupScanSignals {
		passed, first, loopDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		close(passed)
		close(first)
		go func() {
			<-ctx.Done()
			close(loopDone)
		}()
		return startupScanSignals{firstPassDone: first, activeProbePassed: passed, done: loopDone}
	}
	startBackgroundFullScanFn = func(ctx context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.Builder, _ <-chan struct{}) {
		<-ctx.Done()
	}
	// The production HTTP server and control plane run.
	startHTTPServerFn = nil
	gatewayRuntimeControlOptionsFor = func(o gatewayRuntimeControlOptions) gatewayRuntimeControlOptions {
		p.sidecars = o.Sidecars
		return o
	}
	controlReady := make(chan *gatewayRuntimeControl, 1)
	gatewayRuntimeControlStarted = func(control *gatewayRuntimeControl) { controlReady <- control }
	listened := make(chan struct{}, 1)
	listenHTTPControlPlane = func(string, string) (net.Listener, error) {
		listened <- struct{}{}
		return listener, nil
	}
	endpoints := make(chan struct{})
	endpointStarted := sync.OnceFunc(func() { close(endpoints) })
	startEEBusOperatorEndpoint = func(server *mcp.Server, ctx context.Context, socketPath string) (io.Closer, error) {
		defer endpointStarted()
		p.mu.Lock()
		p.socketPaths = append(p.socketPaths, socketPath)
		p.mu.Unlock()
		if opts.operatorError != nil {
			if err := opts.operatorError(socketPath); err != nil {
				return nil, err
			}
		}
		if runtime.GOOS == "linux" {
			return operator(server, ctx, socketPath)
		}
		return m71PortableOperatorEndpoint(socketPath)
	}
	resolveEEBusInterfaceAddressesFn = func(name string) ([]netip.Addr, error) {
		if name != m71Interface {
			return nil, errors.New("unexpected interface")
		}
		return []netip.Addr{netip.MustParseAddr("192.0.2.4"), netip.MustParseAddr("2001:db8::4")}, nil
	}
	construct := func(operator bool, config eebusruntime.Config) eebusruntime.Runtime {
		p.mu.Lock()
		p.constructions = append(p.constructions, operator)
		p.mu.Unlock()
		return &m71StoreRuntime{root: config.StateRoot, endpoints: endpoints, rejected: func(name string) {
			p.mu.Lock()
			p.rejections = append(p.rejections, name)
			p.mu.Unlock()
		}}
	}
	newEEBusOperatorRuntimeFn = func(config eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
		return construct(true, config), issue809AdminStub{}, nil
	}
	newEEBusRuntimeFn = func(config eebusruntime.Config) (eebusruntime.Runtime, error) {
		return construct(false, config), nil
	}

	cfg := env.config()
	cfg.HTTPAddr = "127.0.0.1:0"
	cfg.MDNSAdvertise = false
	cfg.PortalConfigRoot = t.TempDir()
	cfg.EEBusConfig = m71AddonEEBusConfig(t, stateRoot)
	if opts.configure != nil {
		opts.configure(&cfg)
	}
	if !cfg.M2MGraphQL.Disabled() || cfg.ProxyListenAddr != "" {
		t.Fatal("fixture configuration would open a listener outside the control-plane seam")
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go func() { p.done <- run(ctx, cfg) }()
	t.Cleanup(p.stop)
	select {
	case p.control = <-controlReady:
	case err := <-p.done:
		p.cancel = nil
		cancel()
		t.Fatalf("run returned before runtime control composition: %v", err)
	case <-time.After(rctlBound):
		t.Fatal("runtime control was not composed")
	}
	select {
	case <-listened:
	case err := <-p.done:
		p.cancel = nil
		cancel()
		t.Fatalf("run returned before the control plane served: %v", err)
	case <-time.After(rctlBound):
		t.Fatal("control plane did not start")
	}

	service := p.control.Service()
	deadline := time.Now().Add(rctlBound)
	for {
		status, err := service.Get(managedEEBusID)
		if err == nil && (status.ObservedState == drivermanager.ObservedRunning || status.ObservedState == drivermanager.ObservedFailed) {
			return p, status
		}
		if time.Now().After(deadline) {
			t.Fatalf("eebus.primary did not settle: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}
}

// m71AssertRunningWithOperatorSocketBeside checks the settled process: the
// operator runtime is RUNNING on its first construction, the store opened with
// store entries only, and the live operator socket is the owner-only sibling
// <state-root>-operator/operator-mcp.sock.
func m71AssertRunningWithOperatorSocketBeside(t *testing.T, p *m71Process, status runtimecontrol.DriverStatus, stateRoot string) {
	t.Helper()
	m71AssertRunningWithOperatorSocketAt(t, p, status, stateRoot, filepath.Join(stateRoot+"-operator", mcp.EEBusV1OperatorSocketName))
}

// m71AssertRunningWithOperatorSocketAt is the same check for a live owner-only
// operator socket at want, which must lie outside the state root.
func m71AssertRunningWithOperatorSocketAt(t *testing.T, p *m71Process, status runtimecontrol.DriverStatus, stateRoot, want string) {
	t.Helper()
	lifecycle := (&managedSidecarBindings{sidecars: p.sidecars}).eebusLifecycleSnapshot()
	if status.ObservedState != drivermanager.ObservedRunning || status.Reason != drivermanager.ReasonNone {
		t.Fatalf("eebus.primary = %s/%s degraded=%q; want RUNNING/NONE (store rejected entries %q; state root entries %q)", status.ObservedState, status.Reason, lifecycle.DegradedReason, p.storeRejections(), m71Inventory(t, stateRoot))
	}
	if rejections := p.storeRejections(); len(rejections) != 0 {
		t.Fatalf("eeBUS store rejected state root entries %q", rejections)
	}
	if natives := p.natives(); len(natives) != 1 || !natives[0] {
		t.Fatalf("native constructions (operator?) = %v; want one operator runtime", natives)
	}
	if lifecycle.State != eebusLifecycleRunning || !lifecycle.AdminAvailable || lifecycle.DegradedReason != "" {
		t.Fatalf("eeBUS lifecycle = %+v; want running with the admin boundary", lifecycle)
	}
	m71AssertStoreOnly(t, stateRoot)

	if sockets := p.operatorSockets(); len(sockets) != 1 || sockets[0] != want {
		t.Fatalf("operator endpoint paths = %q; want [%q]", sockets, want)
	}
	if strings.HasPrefix(want, stateRoot+string(filepath.Separator)) {
		t.Fatalf("operator socket %q lies inside the state root", want)
	}
	socket, err := os.Lstat(want)
	if err != nil || socket.Mode().Type() != os.ModeSocket || socket.Mode().Perm() != 0o600 {
		t.Fatalf("operator socket = %v (%v); want an owner-only socket", socket, err)
	}
	directory, err := os.Stat(filepath.Dir(want))
	if err != nil || directory.Mode().Perm() != 0o700 {
		t.Fatalf("operator socket directory = %v (%v); want mode 0700", directory, err)
	}
	connection, err := net.DialTimeout("unix", want, time.Second)
	if err != nil {
		t.Fatalf("operator socket does not accept connections: %v", err)
	}
	_ = connection.Close()
}

// With a fresh state root the operator socket is created beside it, and the
// store, opened after the endpoint started, accepts the root.
func TestM71OperatorSocketLivesBesideTheEEBusStateRoot(t *testing.T) {
	logs := m71CaptureLog(t)
	_, stateRoot := m71StateRoot(t)
	p, status := m71Start(t, stateRoot)
	m71AssertRunningWithOperatorSocketBeside(t, p, status, stateRoot)
	text := logs.String()
	if lines := append(m71Lines(text, m71RemovedLine), m71Lines(text, m71LeftLine)...); len(lines) != 0 {
		t.Fatalf("fresh state root logged a legacy socket outcome: %q", lines)
	}
	// The add-on default stays the sibling: no fallback, no degraded endpoint.
	if lines := append(m71Lines(text, m71FallbackLine), m71Lines(text, m71UnavailableLine)...); len(lines) != 0 {
		t.Fatalf("add-on default location logged a fallback or an unavailable endpoint: %q", lines)
	}
	if readiness := projectGatewayReadiness(nil, (&managedSidecarBindings{sidecars: p.sidecars}).eebusLifecycleSnapshot()); readiness.EEBusReadiness != "READY" || readiness.EEBusDegradedReason != "" {
		t.Fatalf("eeBUS readiness = %s/%s; want READY", readiness.EEBusReadiness, readiness.EEBusDegradedReason)
	}
}

// A stale operator socket an earlier build left inside the state root is
// removed before the store opens and logged once without a path; the store
// entries already there stay untouched.
func TestM71StaleOperatorSocketInTheStateRootIsClearedBeforeTheStoreOpens(t *testing.T) {
	logs := m71CaptureLog(t)
	base, stateRoot := m71StateRoot(t)
	m71StaleSocket(t, filepath.Join(stateRoot, eebusLegacyOperatorSocketName))
	manifest := []byte("synthetic manifest slot")
	if err := os.WriteFile(filepath.Join(stateRoot, "MANIFEST.A"), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(stateRoot, "eebusmutation"), 0o700); err != nil {
		t.Fatal(err)
	}

	p, status := m71Start(t, stateRoot)
	m71AssertRunningWithOperatorSocketBeside(t, p, status, stateRoot)
	if _, err := os.Lstat(filepath.Join(stateRoot, eebusLegacyOperatorSocketName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale operator socket still in the state root: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(stateRoot, "MANIFEST.A")); err != nil || !bytes.Equal(got, manifest) {
		t.Fatalf("store manifest changed: %q (%v)", got, err)
	}
	if info, err := os.Lstat(filepath.Join(stateRoot, "eebusmutation")); err != nil || !info.IsDir() {
		t.Fatalf("store mutation directory changed: %v (%v)", info, err)
	}
	text := logs.String()
	if lines := m71Lines(text, m71RemovedLine); len(lines) != 1 {
		t.Fatalf("legacy socket removal lines = %q; want exactly one", lines)
	}
	if lines := m71Lines(text, m71LeftLine); len(lines) != 0 {
		t.Fatalf("legacy socket left-in-place lines = %q; want none", lines)
	}
	if strings.Contains(text, base) {
		t.Fatalf("log contains the state root path:\n%s", text)
	}
}

// The startup cleanup removes only an owned socket without a listener under
// the exact legacy name, only for an enabled eeBUS configuration with a valid
// state root. Each case catches one production mutation:
//   - near names: matching anything but the exact name;
//   - regular file, directory: dropping the socket-type check;
//   - symlink: following links (Stat instead of Lstat) before removal;
//   - active listener: dropping the refused-connection proof;
//   - disabled, relative root: running without a validated enabled root.
//
// Socket ownership by another user needs a second account and is not covered.
func TestM71LegacyOperatorSocketCleanupTouchesOnlyAStaleOwnedSocketByName(t *testing.T) {
	type fixture struct {
		root      string
		elsewhere string
	}
	legacy := func(f fixture) string { return filepath.Join(f.root, eebusLegacyOperatorSocketName) }
	for _, test := range []struct {
		name    string
		enabled bool
		root    func(string) string
		prepare func(*testing.T, fixture) func(*testing.T)
		want    eebusLegacyOperatorSocketOutcome
		line    string
	}{
		{name: "absent", enabled: true, want: eebusLegacyOperatorSocketAbsent},
		{name: "stale socket", enabled: true, want: eebusLegacyOperatorSocketRemoved, line: m71RemovedLine,
			prepare: func(t *testing.T, f fixture) func(*testing.T) {
				m71StaleSocket(t, legacy(f))
				return func(t *testing.T) {
					if _, err := os.Lstat(legacy(f)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("stale socket not removed: %v", err)
					}
				}
			}},
		{name: "near names", enabled: true, want: eebusLegacyOperatorSocketAbsent,
			prepare: func(t *testing.T, f fixture) func(*testing.T) {
				names := []string{"operator.sock", "operator-mcp.sock.old", "operator-mcp.sock2", ".operator-mcp.sock"}
				for _, name := range names {
					m71StaleSocket(t, filepath.Join(f.root, name))
				}
				return func(t *testing.T) {
					for _, name := range names {
						if info, err := os.Lstat(filepath.Join(f.root, name)); err != nil || info.Mode().Type() != os.ModeSocket {
							t.Fatalf("%s changed: %v (%v)", name, info, err)
						}
					}
				}
			}},
		{name: "regular file", enabled: true, want: eebusLegacyOperatorSocketNotOwnedSocket, line: m71LeftLine + " reason=not_owned_socket",
			prepare: func(t *testing.T, f fixture) func(*testing.T) {
				if err := os.WriteFile(legacy(f), []byte("not a socket"), 0o600); err != nil {
					t.Fatal(err)
				}
				return func(t *testing.T) {
					if got, err := os.ReadFile(legacy(f)); err != nil || string(got) != "not a socket" {
						t.Fatalf("regular file changed: %q (%v)", got, err)
					}
				}
			}},
		{name: "directory", enabled: true, want: eebusLegacyOperatorSocketNotOwnedSocket, line: m71LeftLine + " reason=not_owned_socket",
			prepare: func(t *testing.T, f fixture) func(*testing.T) {
				if err := os.Mkdir(legacy(f), 0o700); err != nil {
					t.Fatal(err)
				}
				return func(t *testing.T) {
					if info, err := os.Lstat(legacy(f)); err != nil || !info.IsDir() {
						t.Fatalf("directory changed: %v (%v)", info, err)
					}
				}
			}},
		{name: "symlink to a stale socket", enabled: true, want: eebusLegacyOperatorSocketNotOwnedSocket, line: m71LeftLine + " reason=not_owned_socket",
			prepare: func(t *testing.T, f fixture) func(*testing.T) {
				target := filepath.Join(f.elsewhere, "target.sock")
				m71StaleSocket(t, target)
				if err := os.Symlink(target, legacy(f)); err != nil {
					t.Fatal(err)
				}
				return func(t *testing.T) {
					if info, err := os.Lstat(legacy(f)); err != nil || info.Mode().Type() != os.ModeSymlink {
						t.Fatalf("symlink changed: %v (%v)", info, err)
					}
					if info, err := os.Lstat(target); err != nil || info.Mode().Type() != os.ModeSocket {
						t.Fatalf("symlink target changed: %v (%v)", info, err)
					}
				}
			}},
		{name: "active listener", enabled: true, want: eebusLegacyOperatorSocketActive, line: m71LeftLine + " reason=active_listener",
			prepare: func(t *testing.T, f fixture) func(*testing.T) {
				active, err := net.Listen("unix", legacy(f))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = active.Close() })
				go func() {
					for {
						connection, err := active.Accept()
						if err != nil {
							return
						}
						_ = connection.Close()
					}
				}()
				return func(t *testing.T) {
					connection, err := net.DialTimeout("unix", legacy(f), time.Second)
					if err != nil {
						t.Fatalf("active listener lost its socket: %v", err)
					}
					_ = connection.Close()
				}
			}},
		{name: "disabled", enabled: false, want: eebusLegacyOperatorSocketAbsent,
			prepare: func(t *testing.T, f fixture) func(*testing.T) {
				m71StaleSocket(t, legacy(f))
				return func(t *testing.T) {
					if _, err := os.Lstat(legacy(f)); err != nil {
						t.Fatalf("disabled eeBUS touched the socket: %v", err)
					}
				}
			}},
		{name: "relative state root", enabled: true, root: func(string) string { return "eebus" }, want: eebusLegacyOperatorSocketAbsent,
			prepare: func(t *testing.T, f fixture) func(*testing.T) {
				m71StaleSocket(t, legacy(f))
				return func(t *testing.T) {
					if _, err := os.Lstat(legacy(f)); err != nil {
						t.Fatalf("invalid state root touched the socket: %v", err)
					}
				}
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs := m71CaptureLog(t)
			base, root := m71StateRoot(t)
			f := fixture{root: root, elsewhere: base}
			verify := func(*testing.T) {}
			if test.prepare != nil {
				verify = test.prepare(t, f)
			}
			configured := root
			if test.root != nil {
				configured = test.root(root)
			}
			got := clearLegacyEEBusOperatorSocket(eebusEnabledStateRoot(ebusgateway.Config{EEBusConfig: ebusgateway.EEBusConfig{Enabled: test.enabled, StateRoot: configured}}))
			if got != test.want {
				t.Fatalf("outcome = %s; want %s", got, test.want)
			}
			verify(t)
			text := logs.String()
			lines := m71Lines(text, "eeBUS state root:")
			if test.line == "" {
				if len(lines) != 0 {
					t.Fatalf("log lines = %q; want none", lines)
				}
				return
			}
			if len(lines) != 1 || !strings.HasSuffix(lines[0], test.line) {
				t.Fatalf("log lines = %q; want one ending %q", lines, test.line)
			}
			if strings.Contains(text, base) {
				t.Fatalf("log contains a path:\n%s", text)
			}
		})
	}
}

// A cleanup failure reaches the log as its operating-system category only.
func TestM71LegacyOperatorSocketCauseCarriesNoPath(t *testing.T) {
	for _, test := range []struct {
		err  error
		want string
	}{
		{err: &os.PathError{Op: "remove", Path: "/synthetic/eebus/operator-mcp.sock", Err: syscall.EACCES}, want: syscall.EACCES.Error()},
		{err: &net.OpError{Op: "dial", Net: "unix", Addr: &net.UnixAddr{Name: "/synthetic/eebus/operator-mcp.sock", Net: "unix"}, Err: os.NewSyscallError("connect", syscall.EINVAL)}, want: syscall.EINVAL.Error()},
		{err: errEEBusLegacyOperatorSocketChanged, want: errEEBusLegacyOperatorSocketChanged.Error()},
		{err: errors.New("/synthetic/eebus/operator-mcp.sock: opaque"), want: "unavailable"},
	} {
		if got := eebusLegacyOperatorSocketCause(test.err); got != test.want || strings.Contains(got, "/synthetic") {
			t.Errorf("cause(%v) = %q; want %q", test.err, got, test.want)
		}
	}
}
