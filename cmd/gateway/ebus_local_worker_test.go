package main

import (
	"context"
	"errors"
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/localworkerhost"
)

var aa27Self = localworkerhost.Identity{
	Build:      localworkerhost.DigestOf([]byte("aa27-gateway-build-a")),
	Executable: localworkerhost.DigestOf([]byte("aa27-gateway-image-a")),
	ABI:        localworkerhost.ABIVersion,
}

var aa27Layout = []localworkerhost.Descriptor{
	{Role: localworkerhost.RoleControl, Size: localworkerhost.ReportBytes},
	{Role: localworkerhost.RoleMapping, Size: 4096},
}

var aa27Policy = localworkerhost.RuntimePolicy{GC: "off", MemoryLimit: "64MiB"}

const aa27Executable = "/opt/gateway/bin/gateway"

func aa27ValidChildArgv() []string {
	plan := localworkerhost.ChildPlan{
		Identity:      aa27Self,
		Generation:    localworkerhost.Generation{Worker: 2, Stream: 3, Registration: 4, Token: 5},
		Descriptors:   aa27Layout,
		Runtime:       aa27Policy,
		LeaseInterval: 100 * time.Millisecond,
	}
	return plan.Argv(aa27Executable)
}

// aa27ChildPorts is a counting fake of every child port. It has no device,
// listener, launch or transmit port to count because none exists.
type aa27ChildPorts struct {
	mu        sync.Mutex
	self      localworkerhost.Identity
	observed  map[int]localworkerhost.Observed
	env       map[string]string
	control   io.ReadWriteCloser
	tick      chan time.Time
	calls     map[string]int
	controlFD int
}

func newAA27ChildPorts(control io.ReadWriteCloser) *aa27ChildPorts {
	return &aa27ChildPorts{
		self: aa27Self,
		observed: map[int]localworkerhost.Observed{
			3: {FD: 3, Kind: localworkerhost.KindStream},
			4: {FD: 4, Kind: localworkerhost.KindMapping, Size: 4096},
		},
		env:     map[string]string{"GOGC": "off", "GOMEMLIMIT": "64MiB"},
		control: control,
		tick:    make(chan time.Time),
		calls:   map[string]int{},
	}
}

func (p *aa27ChildPorts) count(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[name]++
}

func (p *aa27ChildPorts) total() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, calls := range p.calls {
		n += calls
	}
	return n
}

func (p *aa27ChildPorts) called(name string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[name]
}

func (p *aa27ChildPorts) Identity() (localworkerhost.Identity, error) {
	p.count("identity")
	return p.self, nil
}

func (p *aa27ChildPorts) Inspect(fd int) (localworkerhost.Observed, error) {
	p.count("inspect")
	observed, ok := p.observed[fd]
	if !ok {
		return localworkerhost.Observed{}, errors.New("aa27: descriptor not inherited")
	}
	return observed, nil
}

func (p *aa27ChildPorts) Getenv(key string) string {
	p.count("getenv")
	return p.env[key]
}

func (p *aa27ChildPorts) Control(fd int) (io.ReadWriteCloser, error) {
	p.count("control")
	p.mu.Lock()
	p.controlFD = fd
	p.mu.Unlock()
	if p.control == nil {
		return nil, errors.New("aa27: no control")
	}
	return p.control, nil
}

func (p *aa27ChildPorts) openedFD() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.controlFD
}

func (p *aa27ChildPorts) After(time.Duration) <-chan time.Time {
	p.count("after")
	return p.tick
}

func TestAA27MainDispatchesTheChildBeforeAnyGatewayConstruction(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var body *ast.BlockStmt
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "main" && fn.Recv == nil {
			body = fn.Body
		}
	}
	if body == nil || len(body.List) == 0 {
		t.Fatal("main() is missing")
	}
	first, ok := body.List[0].(*ast.IfStmt)
	if !ok || !aa27Calls(first.Init, "dispatchEBusLocalWorker") || !aa27Calls(first.Body, "Exit") {
		t.Fatal("the first statement of main() must dispatch the local worker and exit")
	}
	gatewayCalls := map[string]bool{"DefaultConfig": true, "bindFlags": true, "bindMatterLibraryFlags": true, "Parse": true, "NotifyContext": true, "run": true, "runGatewayLifecycle": true, "applyTransportSourcePolicy": true, "resolveModbusEndpointFile": true}
	ast.Inspect(first, func(node ast.Node) bool {
		if name := aa27CallName(node); gatewayCalls[name] {
			t.Errorf("worker dispatch reaches gateway call %s", name)
		}
		return true
	})
	for _, decl := range file.Decls {
		if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.VAR {
			t.Errorf("main.go declares package state before dispatch at %v", gen.Pos())
		}
	}

	// The worker file must not reach the gateway either.
	worker, err := parser.ParseFile(token.NewFileSet(), "ebus_local_worker.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(worker, func(node ast.Node) bool {
		if name := aa27CallName(node); gatewayCalls[name] || name == "Listen" || name == "OpenFile" {
			t.Errorf("ebus_local_worker.go calls %s", name)
		}
		return true
	})
}

func aa27CallName(node ast.Node) string {
	call, ok := node.(*ast.CallExpr)
	if !ok {
		return ""
	}
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}

func aa27Calls(node ast.Node, name string) bool {
	found := false
	if node == nil {
		return false
	}
	ast.Inspect(node, func(n ast.Node) bool {
		if aa27CallName(n) == name {
			found = true
		}
		return !found
	})
	return found
}

func TestAA27DispatchIsSelectedByArgvOneOnlyAndIndependentOfTheMatterOption(t *testing.T) {
	cases := []struct {
		name     string
		argv     []string
		selected bool
	}{
		{"default", []string{aa27Executable}, false},
		{"matter option enabled", []string{aa27Executable, "-matter-library-enabled"}, false},
		{"child flag after a gateway flag", []string{aa27Executable, "-matter-library-enabled", localworkerhost.ChildFlag}, false},
		{"child flag alone", []string{aa27Executable, localworkerhost.ChildFlag}, true},
		{"child with matter option", append(aa27ValidChildArgv(), "-matter-library-enabled"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			factories := 0
			ports := newAA27ChildPorts(nil)
			code, selected := dispatchEBusLocalWorker(tc.argv, func() ebusLocalWorkerChildPorts { factories++; return ports })
			if selected != tc.selected {
				t.Fatalf("selected = %v, want %v", selected, tc.selected)
			}
			if !tc.selected {
				if factories != 0 || ports.total() != 0 {
					t.Fatal("the normal path constructed child ports")
				}
				return
			}
			if code != ebusLocalWorkerExitRefused || ports.total() != 0 {
				t.Fatalf("code=%d port calls=%d; want refusal before any port", code, ports.total())
			}
		})
	}

	// On the normal path the gateway's own flag set rejects the child flag.
	fs := flag.NewFlagSet("aa27", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfg := ebusgateway.DefaultConfig()
	bindFlags(fs, &cfg)
	bindMatterProfile(fs)
	if err := fs.Parse([]string{localworkerhost.ChildFlag}); err == nil {
		t.Fatal("the gateway flag set accepted the worker selector")
	}
}

func TestAA27ChildFlagAloneWithProductionPortsTouchesNothing(t *testing.T) {
	code, selected := dispatchEBusLocalWorker([]string{aa27Executable, localworkerhost.ChildFlag}, newEBusLocalWorkerChildPorts)
	if !selected || code != ebusLocalWorkerExitRefused {
		t.Fatalf("code=%d selected=%v", code, selected)
	}
}

func TestAA27ChildRefusesEveryMismatchBeforeReadiness(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*aa27ChildPorts)
		mutation string
	}{
		{"parent build differs", func(p *aa27ChildPorts) { p.self.Build = localworkerhost.DigestOf([]byte("aa27-gateway-build-b")) }, "skip the build comparison"},
		{"executable replaced", func(p *aa27ChildPorts) { p.self.Executable = localworkerhost.DigestOf([]byte("aa27-gateway-image-b")) }, "skip the executable digest comparison"},
		{"child ABI differs", func(p *aa27ChildPorts) { p.self.ABI = localworkerhost.ABIVersion + 1 }, "skip the ABI comparison"},
		{"control role behind fd", func(p *aa27ChildPorts) {
			p.observed[3] = localworkerhost.Observed{FD: 3, Kind: localworkerhost.KindMapping, Size: 4096}
		}, "accept any kind for control"},
		{"mapping role behind fd", func(p *aa27ChildPorts) {
			p.observed[4] = localworkerhost.Observed{FD: 4, Kind: localworkerhost.KindStream}
		}, "accept any kind for mapping"},
		{"mapping size", func(p *aa27ChildPorts) {
			p.observed[4] = localworkerhost.Observed{FD: 4, Kind: localworkerhost.KindMapping, Size: 8192}
		}, "skip the mapping size comparison"},
		{"descriptor missing", func(p *aa27ChildPorts) { delete(p.observed, 4) }, "skip inspecting the mapping descriptor"},
		{"gateway GOGC inherited", func(p *aa27ChildPorts) { p.env["GOGC"] = "100" }, "skip the child GC policy check"},
		{"gateway GOMEMLIMIT inherited", func(p *aa27ChildPorts) { p.env["GOMEMLIMIT"] = "2GiB" }, "skip the child memory-limit check"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ports := newAA27ChildPorts(nil)
			tc.mutate(ports)
			if code := runEBusLocalWorkerChild(aa27ValidChildArgv()[1:], ports); code != ebusLocalWorkerExitRefused {
				t.Fatalf("code = %d, want refusal (catches: %s)", code, tc.mutation)
			}
			if ports.called("control") != 0 || ports.called("after") != 0 {
				t.Fatalf("a refused child opened control or waited (catches: %s)", tc.mutation)
			}
		})
	}
}

// aa27Loopback re-executes the child entry in-process: argv, environment and
// descriptors come only from the LaunchSpec, and control is an in-memory pipe.
type aa27Loopback struct {
	t        *testing.T
	self     localworkerhost.Identity
	reports  chan localworkerhost.Report
	mu       sync.Mutex
	children []*aa27LoopbackChild
}

type aa27LoopbackChild struct {
	ports  *aa27ChildPorts
	parent net.Conn
	code   int
	done   chan struct{}
}

func (c *aa27LoopbackChild) Wait() error {
	<-c.done
	return nil
}
func (c *aa27LoopbackChild) Stop() error { return c.parent.Close() }
func (c *aa27LoopbackChild) Kill() error { return c.parent.Close() }

type aa27Resource struct {
	role localworkerhost.Role
	size uint64
}

func (r aa27Resource) Role() localworkerhost.Role { return r.role }
func (r aa27Resource) Size() uint64               { return r.size }
func (r aa27Resource) Close() error               { return nil }

type aa27Allocator struct{}

func (aa27Allocator) Allocate(d localworkerhost.Descriptor) (localworkerhost.Resource, error) {
	return aa27Resource{role: d.Role, size: d.Size}, nil
}

func (l *aa27Loopback) Launch(spec localworkerhost.LaunchSpec) (localworkerhost.Process, error) {
	parentEnd, childEnd := net.Pipe()
	ports := newAA27ChildPorts(childEnd)
	ports.self = l.self
	ports.env = map[string]string{}
	for _, entry := range spec.Env {
		key, value, _ := strings.Cut(entry, "=")
		ports.env[key] = value
	}
	ports.observed = map[int]localworkerhost.Observed{}
	for i, resource := range spec.Files {
		fd := localworkerhost.FirstDescriptor + i
		observed := localworkerhost.Observed{FD: fd, Kind: localworkerhost.KindStream}
		if resource.Role() == localworkerhost.RoleMapping {
			observed = localworkerhost.Observed{FD: fd, Kind: localworkerhost.KindMapping, Size: resource.Size()}
		}
		ports.observed[fd] = observed
	}
	child := &aa27LoopbackChild{ports: ports, parent: parentEnd, done: make(chan struct{})}
	reader := make(chan struct{})
	go func() {
		defer close(reader)
		for {
			raw := make([]byte, localworkerhost.ReportBytes)
			if _, err := io.ReadFull(parentEnd, raw); err != nil {
				return
			}
			report, err := localworkerhost.DecodeReport(raw)
			if err != nil {
				l.t.Errorf("DecodeReport: %v", err)
				return
			}
			l.reports <- report
		}
	}()
	go func() {
		child.code = runEBusLocalWorkerChild(spec.Args[1:], ports)
		_ = parentEnd.Close()
		<-reader
		close(child.done)
	}()
	l.mu.Lock()
	l.children = append(l.children, child)
	l.mu.Unlock()
	return child, nil
}

type aa27Clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *aa27Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *aa27Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
func (c *aa27Clock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

type aa27Backend struct{}

func (aa27Backend) Allocator() localworkerhost.Allocator   { return aa27Allocator{} }
func (aa27Backend) Layout() []localworkerhost.Descriptor   { return aa27Layout }
func (aa27Backend) Runtime() localworkerhost.RuntimePolicy { return aa27Policy }

func aa27Parent(launcher localworkerhost.Launcher, clock localworkerhost.Clock) ebusLocalWorkerParent {
	return ebusLocalWorkerParent{
		Self:        aa27Self,
		Executable:  aa27Executable,
		ProcessArgs: []string{aa27Executable, "-matter-library-enabled=true"},
		Env:         []string{"PATH=/usr/bin", "GOGC=100", "GOMEMLIMIT=2GiB"},
		Launcher:    launcher,
		Clock:       clock,
	}
}

func TestAA27LoopbackChildReportsPassiveReadinessAndLeasesUntilStopped(t *testing.T) {
	loop := &aa27Loopback{t: t, self: aa27Self, reports: make(chan localworkerhost.Report, 4)}
	clock := &aa27Clock{now: time.Unix(2000, 0)}
	sup, err := composeEBusLocalWorker(aa27Backend{}, aa27Parent(loop, clock))
	if err != nil {
		t.Fatalf("composeEBusLocalWorker: %v", err)
	}
	for round := 1; round <= 2; round++ {
		if round == 1 {
			err = sup.Start()
		} else {
			err = sup.Restart(context.Background())
		}
		if err != nil {
			t.Fatalf("round %d start: %v", round, err)
		}
		ready := <-loop.reports
		if ready.Kind != localworkerhost.ReportReady || ready.TXActive || ready.State != localworkerhost.WorkerPassive {
			t.Fatalf("round %d readiness = %+v", round, ready)
		}
		if err := sup.Observe(ready); err != nil {
			t.Fatalf("round %d Observe ready: %v", round, err)
		}
		gen := sup.Status().Generation
		if gen.Worker != uint64(round) || gen.Stream != uint64(round) || gen.Registration != uint64(round) || gen.Token != uint64(round) {
			t.Fatalf("round %d generation = %+v", round, gen)
		}
		loop.mu.Lock()
		child := loop.children[len(loop.children)-1]
		loop.mu.Unlock()
		if fd := child.ports.openedFD(); fd != localworkerhost.FirstDescriptor {
			t.Fatalf("control fd = %d", fd)
		}
		child.ports.tick <- clock.Now()
		lease := <-loop.reports
		clock.Advance(100 * time.Millisecond)
		if lease.Kind != localworkerhost.ReportLease || lease.Sequence != 1 || lease.TXActive {
			t.Fatalf("round %d lease = %+v", round, lease)
		}
		if err := sup.Observe(lease); err != nil {
			t.Fatalf("round %d Observe lease: %v", round, err)
		}
	}
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	loop.mu.Lock()
	defer loop.mu.Unlock()
	if len(loop.children) != 2 {
		t.Fatalf("launches = %d", len(loop.children))
	}
	for i, child := range loop.children {
		<-child.done
		if child.code != ebusLocalWorkerExitStopped {
			t.Fatalf("child %d exit = %d", i, child.code)
		}
	}
}

func TestAA27LoopbackChildFromAReplacedExecutableNeverBecomesReady(t *testing.T) {
	replaced := aa27Self
	replaced.Executable = localworkerhost.DigestOf([]byte("aa27-gateway-image-replaced"))
	loop := &aa27Loopback{t: t, self: replaced, reports: make(chan localworkerhost.Report, 1)}
	sup, err := composeEBusLocalWorker(aa27Backend{}, aa27Parent(loop, &aa27Clock{now: time.Unix(2000, 0)}))
	if err != nil {
		t.Fatal(err)
	}
	if err := sup.Start(); err != nil {
		t.Fatal(err)
	}
	loop.mu.Lock()
	child := loop.children[0]
	loop.mu.Unlock()
	<-child.done
	if child.code != ebusLocalWorkerExitRefused || child.ports.called("control") != 0 {
		t.Fatalf("replaced child exit=%d control=%d", child.code, child.ports.called("control"))
	}
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := sup.Status().State; state != localworkerhost.StateStopped {
		t.Fatalf("state = %s", state)
	}
}

type aa27CountingLauncher struct{ launches int }

func (l *aa27CountingLauncher) Launch(localworkerhost.LaunchSpec) (localworkerhost.Process, error) {
	l.launches++
	return nil, errors.New("aa27: unexpected launch")
}

func TestAA27DefaultParentHasNoBackendAndLaunchesNothing(t *testing.T) {
	launcher := &aa27CountingLauncher{}
	sup, err := composeEBusLocalWorker(nil, aa27Parent(launcher, systemEBusLocalWorkerClock{}))
	if !errors.Is(err, errEBusLocalWorkerUnavailable) || sup != nil || launcher.launches != 0 {
		t.Fatalf("compose without backend = %v, %v, launches %d", sup, err, launcher.launches)
	}
	child := aa27Parent(launcher, systemEBusLocalWorkerClock{})
	child.ProcessArgs = aa27ValidChildArgv()
	if _, err := composeEBusLocalWorker(aa27Backend{}, child); !errors.Is(err, localworkerhost.ErrRecursion) {
		t.Fatalf("compose inside a child = %v, want ErrRecursion", err)
	}
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"composeEBusLocalWorker", "execEBusLocalWorkerLauncher", "localworkerhost.New"} {
		if strings.Contains(string(source), name) {
			t.Fatalf("main.go reaches %s on the default path", name)
		}
	}
}

type aa27FileResource struct {
	aa27Resource
	file *os.File
}

func (r aa27FileResource) File() *os.File { return r.file }

func TestAA27ExecLauncherBuildsAnExplicitCommandWithoutAShell(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	argv := aa27ValidChildArgv()
	spec := localworkerhost.LaunchSpec{
		Path:  aa27Executable,
		Args:  argv,
		Env:   []string{"GOGC=off", "GOMEMLIMIT=64MiB"},
		Files: []localworkerhost.Resource{aa27FileResource{aa27Resource{localworkerhost.RoleControl, localworkerhost.ReportBytes}, reader}, aa27FileResource{aa27Resource{localworkerhost.RoleMapping, 4096}, writer}},
	}
	cmd, err := execEBusLocalWorkerLauncher{}.command(spec)
	if err != nil {
		t.Fatalf("command: %v", err)
	}
	if cmd.Path != aa27Executable || strings.Join(cmd.Args, " ") != strings.Join(argv, " ") || strings.Join(cmd.Env, " ") != "GOGC=off GOMEMLIMIT=64MiB" {
		t.Fatalf("cmd = %q %q %q", cmd.Path, cmd.Args, cmd.Env)
	}
	if len(cmd.ExtraFiles) != 2 || cmd.ExtraFiles[0] != reader || cmd.ExtraFiles[1] != writer || cmd.Process != nil {
		t.Fatal("descriptors not inherited in layout order, or the command was started")
	}
	shell := spec
	shell.Args = []string{"/bin/sh", "-c", aa27Executable}
	if _, err := (execEBusLocalWorkerLauncher{}).command(shell); !errors.Is(err, localworkerhost.ErrArgv) {
		t.Fatalf("shell argv = %v", err)
	}
	opaque := spec
	opaque.Files = []localworkerhost.Resource{aa27Resource{localworkerhost.RoleControl, localworkerhost.ReportBytes}}
	if _, err := (execEBusLocalWorkerLauncher{}).command(opaque); !errors.Is(err, localworkerhost.ErrDescriptor) {
		t.Fatalf("non-inheritable resource = %v", err)
	}
}

func TestAA27NormalPathDispatchPrecedesProfileBinding(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	last := -1
	for _, label := range []string{"dispatchEBusLocalWorker(", "ebusgateway.DefaultConfig()", "bindFlags(flag.CommandLine, &cfg)", "bindMatterProfile(flag.CommandLine)", "flag.Parse()", "profile(run)", "run(ctx, cfg)"} {
		next := strings.Index(text, label)
		if next <= last {
			t.Fatalf("main.go order changed at %q", label)
		}
		last = next
	}
}
