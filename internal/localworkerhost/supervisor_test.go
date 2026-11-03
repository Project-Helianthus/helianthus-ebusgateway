package localworkerhost

import (
	"context"
	"errors"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	aa27Self = Identity{Build: DigestOf([]byte("aa27-build-a")), Executable: DigestOf([]byte("aa27-image-a")), ABI: ABIVersion}

	aa27Layout = []Descriptor{{Role: RoleControl, Size: ReportBytes}, {Role: RoleMapping, Size: 4096}}
	aa27Policy = RuntimePolicy{GC: "off", MemoryLimit: "64MiB"}
)

const (
	aa27StopGrace = time.Second
	aa27KillGrace = 2 * time.Second
)

// aa27Clock is injected time. After fires only for durations listed in fire.
type aa27Clock struct {
	mu   sync.Mutex
	now  time.Time
	fire map[time.Duration]bool
}

func newAA27Clock() *aa27Clock {
	return &aa27Clock{now: time.Unix(1000, 0), fire: map[time.Duration]bool{}}
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

func (c *aa27Clock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if c.fire[d] {
		ch <- c.now.Add(d)
	}
	return ch
}

type aa27Resource struct {
	role   Role
	size   uint64
	mu     sync.Mutex
	closes int
}

func (r *aa27Resource) Role() Role   { return r.role }
func (r *aa27Resource) Size() uint64 { return r.size }
func (r *aa27Resource) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closes++
	return nil
}

func (r *aa27Resource) closed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closes
}

type aa27Allocator struct {
	mu        sync.Mutex
	resources []*aa27Resource
	wrongRole bool
	wrongSize bool
}

func (a *aa27Allocator) Allocate(want Descriptor) (Resource, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	resource := &aa27Resource{role: want.Role, size: want.Size}
	if a.wrongRole && want.Role == RoleMapping {
		resource.role = RoleControl
	}
	if a.wrongSize && want.Role == RoleMapping {
		resource.size = want.Size + 8
	}
	a.resources = append(a.resources, resource)
	return resource, nil
}

func (a *aa27Allocator) all() []*aa27Resource {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]*aa27Resource(nil), a.resources...)
}

type aa27Process struct {
	exitOnStop bool
	exitOnKill bool
	once       sync.Once
	exited     chan struct{}
	mu         sync.Mutex
	stops      int
	kills      int
}

func newAA27Process(exitOnStop, exitOnKill bool) *aa27Process {
	return &aa27Process{exitOnStop: exitOnStop, exitOnKill: exitOnKill, exited: make(chan struct{})}
}

func (p *aa27Process) exit() { p.once.Do(func() { close(p.exited) }) }

func (p *aa27Process) Wait() error {
	<-p.exited
	return nil
}

func (p *aa27Process) Stop() error {
	p.mu.Lock()
	p.stops++
	p.mu.Unlock()
	if p.exitOnStop {
		p.exit()
	}
	return nil
}

func (p *aa27Process) Kill() error {
	p.mu.Lock()
	p.kills++
	p.mu.Unlock()
	if p.exitOnKill {
		p.exit()
	}
	return nil
}

func (p *aa27Process) counts() (stops, kills int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stops, p.kills
}

type aa27Launcher struct {
	mu         sync.Mutex
	specs      []LaunchSpec
	processes  []*aa27Process
	fail       error
	exitOnStop bool
	exitOnKill bool
}

func (l *aa27Launcher) Launch(spec LaunchSpec) (Process, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.specs = append(l.specs, spec)
	if l.fail != nil {
		return nil, l.fail
	}
	process := newAA27Process(l.exitOnStop, l.exitOnKill)
	l.processes = append(l.processes, process)
	return process, nil
}

func (l *aa27Launcher) last() (LaunchSpec, *aa27Process) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var process *aa27Process
	if len(l.processes) > 0 {
		process = l.processes[len(l.processes)-1]
	}
	return l.specs[len(l.specs)-1], process
}

type aa27Fixture struct {
	sup       *Supervisor
	clock     *aa27Clock
	launcher  *aa27Launcher
	allocator *aa27Allocator
}

func aa27Config(clock *aa27Clock, launcher *aa27Launcher, allocator *aa27Allocator) Config {
	return Config{
		Self:          aa27Self,
		Executable:    "/opt/gateway/bin/gateway",
		ProcessArgs:   []string{"/opt/gateway/bin/gateway", "-matter-library-enabled=true"},
		BaseEnv:       []string{"PATH=/usr/bin", "GOGC=50", "GOMEMLIMIT=2GiB", "GODEBUG=madvdontneed=1", "GOMAXPROCS=4", "LANG=C"},
		Descriptors:   aa27Layout,
		Runtime:       aa27Policy,
		LeaseInterval: 100 * time.Millisecond,
		LeaseTTL:      time.Second,
		ReadyTimeout:  5 * time.Second,
		StopGrace:     aa27StopGrace,
		KillGrace:     aa27KillGrace,
		Allocator:     allocator,
		Launcher:      launcher,
		Clock:         clock,
	}
}

func newAA27Fixture(t *testing.T) *aa27Fixture {
	t.Helper()
	f := &aa27Fixture{clock: newAA27Clock(), launcher: &aa27Launcher{exitOnStop: true, exitOnKill: true}, allocator: &aa27Allocator{}}
	sup, err := New(aa27Config(f.clock, f.launcher, f.allocator))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.sup = sup
	t.Cleanup(func() { aa27Reap(t, f) })
	return f
}

// aa27Reap is the joined cleanup: it stops the child, waits for its watcher
// and checks that the process-wide child slot is free again.
func aa27Reap(t *testing.T, f *aa27Fixture) {
	t.Helper()
	f.sup.mu.Lock()
	c := f.sup.child
	f.sup.mu.Unlock()
	if c != nil {
		f.launcher.mu.Lock()
		for _, process := range f.launcher.processes {
			process.exit()
		}
		f.launcher.mu.Unlock()
		<-c.done
	}
	if liveChild.Load() {
		t.Fatal("process-wide child slot still claimed after cleanup")
	}
}

func (f *aa27Fixture) ready(t *testing.T) Report {
	t.Helper()
	report := Report{Kind: ReportReady, Identity: aa27Self, Generation: f.sup.Status().Generation, Descriptors: aa27Layout, State: WorkerPassive}
	if err := f.sup.Observe(report); err != nil {
		t.Fatalf("Observe ready: %v", err)
	}
	return report
}

func aa27StartReady(t *testing.T) *aa27Fixture {
	t.Helper()
	f := newAA27Fixture(t)
	if err := f.sup.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	f.ready(t)
	return f
}

func TestAA27StartLaunchesExplicitArgvEnvironmentAndFixedDescriptors(t *testing.T) {
	f := aa27StartReady(t)
	spec, _ := f.launcher.last()
	if spec.Path != "/opt/gateway/bin/gateway" || spec.Args[0] != spec.Path || spec.Args[1] != ChildFlag {
		t.Fatalf("launch path/argv = %q %q", spec.Path, spec.Args)
	}
	for _, arg := range spec.Args[2:] {
		if strings.HasPrefix(arg, "-") {
			t.Fatalf("child argv carries a gateway flag %q", arg)
		}
	}
	plan, err := ParseChildArgv(spec.Args[1:])
	if err != nil {
		t.Fatalf("ParseChildArgv(launched argv): %v", err)
	}
	want := Generation{Worker: 1, Stream: 1, Registration: 1, Token: 1}
	if plan.Identity != aa27Self || plan.Generation != want || !layoutEqual(plan.Descriptors, aa27Layout) || plan.Runtime != aa27Policy || plan.LeaseInterval != 100*time.Millisecond {
		t.Fatalf("launched plan = %+v", plan)
	}
	if spec.Generation != want {
		t.Fatalf("spec generation = %+v", spec.Generation)
	}
	if len(spec.Files) != 2 || spec.Files[0].Role() != RoleControl || spec.Files[0].Size() != ReportBytes || spec.Files[1].Role() != RoleMapping || spec.Files[1].Size() != 4096 {
		t.Fatalf("inherited files = %+v", spec.Files)
	}
	if got := f.sup.Status().State; got != StateReady {
		t.Fatalf("state = %s, want ready", got)
	}
	if err := f.sup.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	for _, resource := range f.allocator.all() {
		if resource.closed() != 1 {
			t.Fatalf("resource %+v closed %d times after reap", resource.role, resource.closed())
		}
	}
	if got := f.sup.Status().State; got != StateStopped {
		t.Fatalf("state after stop = %s", got)
	}
}

func TestAA27ChildEnvironmentNeverInheritsGatewayRuntimeSettings(t *testing.T) {
	for _, key := range []string{"GOGC", "GOMEMLIMIT", "GODEBUG", "GOMAXPROCS"} {
		t.Run(key, func(t *testing.T) {
			env := ChildEnv([]string{"PATH=/usr/bin", key + "=parent-value", "LANG=C"}, aa27Policy)
			for _, entry := range env {
				if strings.Contains(entry, "parent-value") {
					t.Fatalf("child env inherited %q", entry)
				}
			}
			joined := strings.Join(env, "\n")
			for _, want := range []string{"PATH=/usr/bin", "LANG=C", "GOGC=off", "GOMEMLIMIT=64MiB"} {
				if !strings.Contains(joined, want) {
					t.Fatalf("child env %q lacks %q", env, want)
				}
			}
		})
	}
	f := newAA27Fixture(t)
	if err := f.sup.Start(); err != nil {
		t.Fatal(err)
	}
	spec, _ := f.launcher.last()
	gogc := 0
	for _, entry := range spec.Env {
		if strings.HasPrefix(entry, "GOGC=") {
			gogc++
			if entry != "GOGC=off" {
				t.Fatalf("launched GOGC = %q", entry)
			}
		}
		if entry == "GOGC=50" || entry == "GOMEMLIMIT=2GiB" || strings.HasPrefix(entry, "GODEBUG=") || strings.HasPrefix(entry, "GOMAXPROCS=") {
			t.Fatalf("launched env inherited gateway runtime setting %q", entry)
		}
	}
	if gogc != 1 {
		t.Fatalf("launched env has %d GOGC entries", gogc)
	}
}

func aa27ValidPlan() ChildPlan {
	return ChildPlan{
		Identity:      aa27Self,
		Generation:    Generation{Worker: 3, Stream: 5, Registration: 7, Token: 11},
		Descriptors:   append([]Descriptor(nil), aa27Layout...),
		Runtime:       aa27Policy,
		LeaseInterval: 100 * time.Millisecond,
	}
}

func aa27ValidObserved() []Observed {
	return []Observed{{FD: 3, Kind: KindStream}, {FD: 4, Kind: KindMapping, Size: 4096}}
}

func aa27Env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestAA27ValidateChildVariesEveryIdentityDescriptorAndPolicyField(t *testing.T) {
	validEnv := map[string]string{"GOGC": "off", "GOMEMLIMIT": "64MiB"}
	if err := ValidateChild(aa27ValidPlan(), aa27Self, aa27ValidObserved(), aa27Env(validEnv)); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	cases := []struct {
		name     string
		mutate   func(*ChildPlan, *Identity, []Observed, map[string]string)
		want     error
		mutation string
	}{
		{"plan build", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) {
			p.Identity.Build = DigestOf([]byte("aa27-build-b"))
		}, ErrBuild, "drop Build from the identity comparison"},
		{"plan executable", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) {
			p.Identity.Executable = DigestOf([]byte("aa27-image-b"))
		}, ErrBuild, "drop Executable from the identity comparison"},
		{"plan ABI", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) { p.Identity.ABI = ABIVersion + 1 }, ErrABI, "drop the plan ABI check"},
		{"child ABI", func(_ *ChildPlan, s *Identity, _ []Observed, _ map[string]string) { s.ABI = ABIVersion + 1 }, ErrABI, "drop the self ABI check"},
		{"child build", func(_ *ChildPlan, s *Identity, _ []Observed, _ map[string]string) {
			s.Build = DigestOf([]byte("aa27-build-c"))
		}, ErrBuild, "trust the plan identity without the child's own build"},
		{"child executable after replacement", func(_ *ChildPlan, s *Identity, _ []Observed, _ map[string]string) {
			s.Executable = DigestOf([]byte("aa27-image-replaced"))
		}, ErrBuild, "skip the executable image digest"},
		{"zero worker", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) { p.Generation.Worker = 0 }, ErrGeneration, "drop Worker from Generation.valid"},
		{"zero stream", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) { p.Generation.Stream = 0 }, ErrGeneration, "drop Stream from Generation.valid"},
		{"zero registration", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) { p.Generation.Registration = 0 }, ErrGeneration, "drop Registration from Generation.valid"},
		{"zero token", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) { p.Generation.Token = 0 }, ErrGeneration, "drop Token from Generation.valid"},
		{"control role", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) {
			p.Descriptors[0].Role = RoleMapping
		}, ErrDescriptor, "drop the control role check"},
		{"control size", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) {
			p.Descriptors[0].Size = ReportBytes + 8
		}, ErrDescriptor, "drop the control size check"},
		{"mapping role", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) {
			p.Descriptors[1].Role = RoleControl
		}, ErrDescriptor, "drop the mapping role check"},
		{"mapping size unaligned", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) { p.Descriptors[1].Size = 4100 }, ErrDescriptor, "drop the mapping alignment check"},
		{"mapping size above bound", func(p *ChildPlan, _ *Identity, _ []Observed, _ map[string]string) {
			p.Descriptors[1].Size = MaxMappingBytes + 8
		}, ErrDescriptor, "drop the mapping upper bound"},
		{"control fd number", func(_ *ChildPlan, _ *Identity, o []Observed, _ map[string]string) { o[0].FD = 5 }, ErrDescriptor, "drop the descriptor number check"},
		{"control kind", func(_ *ChildPlan, _ *Identity, o []Observed, _ map[string]string) { o[0].Kind = KindMapping }, ErrDescriptor, "accept any kind behind the control role"},
		{"mapping kind", func(_ *ChildPlan, _ *Identity, o []Observed, _ map[string]string) { o[1].Kind = KindStream }, ErrDescriptor, "accept any kind behind the mapping role"},
		{"mapping observed size", func(_ *ChildPlan, _ *Identity, o []Observed, _ map[string]string) { o[1].Size = 8192 }, ErrDescriptor, "drop the observed mapping size check"},
		{"inherited gateway GOGC", func(_ *ChildPlan, _ *Identity, _ []Observed, e map[string]string) { e["GOGC"] = "50" }, ErrRuntimePolicy, "drop the GOGC check"},
		{"inherited gateway GOMEMLIMIT", func(_ *ChildPlan, _ *Identity, _ []Observed, e map[string]string) { e["GOMEMLIMIT"] = "2GiB" }, ErrRuntimePolicy, "drop the GOMEMLIMIT check"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, self, observed := aa27ValidPlan(), aa27Self, aa27ValidObserved()
			env := map[string]string{"GOGC": "off", "GOMEMLIMIT": "64MiB"}
			tc.mutate(&plan, &self, observed, env)
			if err := ValidateChild(plan, self, observed, aa27Env(env)); !errors.Is(err, tc.want) {
				t.Fatalf("ValidateChild = %v, want %v (catches: %s)", err, tc.want, tc.mutation)
			}
		})
	}
	if err := ValidateChild(aa27ValidPlan(), aa27Self, aa27ValidObserved()[:1], aa27Env(validEnv)); !errors.Is(err, ErrDescriptor) {
		t.Fatalf("missing descriptor = %v", err)
	}
}

func TestAA27ParseChildArgvAcceptsOnlyTheExactPlan(t *testing.T) {
	plan := aa27ValidPlan()
	argv := plan.Argv("/opt/gateway/bin/gateway")
	parsed, err := ParseChildArgv(argv[1:])
	if err != nil {
		t.Fatalf("positive control: %v", err)
	}
	if parsed.Identity != plan.Identity || parsed.Generation != plan.Generation || !layoutEqual(parsed.Descriptors, plan.Descriptors) || parsed.Runtime != plan.Runtime || parsed.LeaseInterval != plan.LeaseInterval {
		t.Fatalf("round trip = %+v", parsed)
	}
	args := argv[1:]
	cases := map[string][]string{
		"flag alone":            {ChildFlag},
		"trailing gateway flag": append(append([]string(nil), args...), "-matter-library-enabled"),
		"missing field":         args[:len(args)-1],
		"wrong selector":        append([]string{"--internal-other"}, args[1:]...),
	}
	swapped := append([]string(nil), args...)
	swapped[4], swapped[5] = swapped[5], swapped[4]
	cases["reordered fields"] = swapped
	for i, key := range childArgvKeys {
		bad := append([]string(nil), args...)
		bad[i+1] = key + "="
		cases["empty "+key] = bad
	}
	for _, field := range []string{"build", "executable"} {
		bad := append([]string(nil), args...)
		index := 1
		if field == "executable" {
			index = 2
		}
		bad[index] = field + "=zz"
		cases["bad digest "+field] = bad
	}
	for i, key := range childArgvKeys[2:9] {
		bad := append([]string(nil), args...)
		bad[i+3] = key + "=x"
		cases["non-numeric "+key] = bad
	}
	nonPositive := append([]string(nil), args...)
	nonPositive[len(nonPositive)-1] = "lease-ms=0"
	cases["zero lease"] = nonPositive
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseChildArgv(bad); !errors.Is(err, ErrArgv) {
				t.Fatalf("ParseChildArgv = %v, want ErrArgv", err)
			}
		})
	}
	if !SelectsChild(argv) || SelectsChild([]string{"gateway", "-x", ChildFlag}) || SelectsChild([]string{ChildFlag}) {
		t.Fatal("SelectsChild must accept the flag only as argv[1]")
	}
}

func TestAA27ObserveFencesEveryReportFieldMismatch(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*Report)
		want     error
		mutation string
	}{
		{"build", func(r *Report) { r.Identity.Build = DigestOf([]byte("aa27-build-b")) }, ErrBuild, "drop Build from the report identity check"},
		{"executable", func(r *Report) { r.Identity.Executable = DigestOf([]byte("aa27-image-b")) }, ErrBuild, "drop Executable from the report identity check"},
		{"ABI", func(r *Report) { r.Identity.ABI = ABIVersion + 1 }, ErrABI, "drop the report ABI check"},
		{"future worker", func(r *Report) { r.Generation.Worker++ }, ErrGeneration, "compare only Stream/Registration/Token"},
		{"stream", func(r *Report) { r.Generation.Stream++ }, ErrGeneration, "drop Stream from the generation fence"},
		{"registration", func(r *Report) { r.Generation.Registration++ }, ErrGeneration, "drop Registration from the generation fence"},
		{"token", func(r *Report) { r.Generation.Token++ }, ErrGeneration, "drop the owner Token from the generation fence"},
		{"descriptor role", func(r *Report) { r.Descriptors[1].Role = RoleControl }, ErrDescriptor, "drop the reported role comparison"},
		{"descriptor size", func(r *Report) { r.Descriptors[1].Size = 8192 }, ErrDescriptor, "drop the reported size comparison"},
		{"descriptor missing", func(r *Report) { r.Descriptors = r.Descriptors[:1] }, ErrDescriptor, "accept a partial descriptor report"},
		{"state not passive", func(r *Report) { r.State = WorkerPassive + 1 }, ErrNotPassive, "accept a non-passive readiness"},
		{"tx active", func(r *Report) { r.TXActive = true }, ErrNotPassive, "accept TX-active readiness"},
		{"ready sequence", func(r *Report) { r.Sequence = 1 }, ErrReport, "accept a sequenced readiness"},
		{"unknown kind", func(r *Report) { r.Kind = 9 }, ErrReport, "accept unknown report kinds"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAA27Fixture(t)
			if err := f.sup.Start(); err != nil {
				t.Fatal(err)
			}
			report := Report{Kind: ReportReady, Identity: aa27Self, Generation: f.sup.Status().Generation, Descriptors: append([]Descriptor(nil), aa27Layout...), State: WorkerPassive}
			tc.mutate(&report)
			if err := f.sup.Observe(report); !errors.Is(err, tc.want) {
				t.Fatalf("Observe = %v, want %v (catches: %s)", err, tc.want, tc.mutation)
			}
			status := f.sup.Status()
			if status.State != StateFenced || !errors.Is(status.Reason, tc.want) {
				t.Fatalf("status = %+v, want fenced by %v", status, tc.want)
			}
			gen := status.Generation
			if err := f.sup.PublishPlan(PlanRef{Worker: gen.Worker, Stream: gen.Stream, Registration: gen.Registration, Plan: 1}); !errors.Is(err, ErrNotReady) {
				t.Fatalf("fenced PublishPlan = %v", err)
			}
		})
	}
}

func TestAA27LeasesAdvanceStrictlyAndExpireOnInjectedTime(t *testing.T) {
	lease := func(f *aa27Fixture, sequence uint64) error {
		return f.sup.Observe(Report{Kind: ReportLease, Identity: aa27Self, Generation: f.sup.Status().Generation, State: WorkerPassive, Sequence: sequence})
	}
	t.Run("in order", func(t *testing.T) {
		f := aa27StartReady(t)
		for sequence := uint64(1); sequence <= 3; sequence++ {
			f.clock.Advance(900 * time.Millisecond)
			if err := lease(f, sequence); err != nil {
				t.Fatalf("lease %d: %v", sequence, err)
			}
			if err := f.sup.Tick(); err != nil {
				t.Fatalf("Tick after renewal: %v", err)
			}
		}
	})
	for name, sequence := range map[string]uint64{"replayed": 0, "skipped": 2} {
		t.Run(name, func(t *testing.T) {
			f := aa27StartReady(t)
			if err := lease(f, sequence); !errors.Is(err, ErrLease) {
				t.Fatalf("lease %d = %v, want ErrLease", sequence, err)
			}
		})
	}
	t.Run("late lease cannot revive", func(t *testing.T) {
		f := aa27StartReady(t)
		f.clock.Advance(time.Second)
		if err := lease(f, 1); !errors.Is(err, ErrLeaseExpired) {
			t.Fatalf("late lease = %v", err)
		}
	})
	t.Run("tick fences at expiry, not before", func(t *testing.T) {
		f := aa27StartReady(t)
		f.clock.Advance(time.Second - time.Nanosecond)
		if err := f.sup.Tick(); err != nil {
			t.Fatalf("Tick before expiry: %v", err)
		}
		f.clock.Advance(time.Nanosecond)
		if err := f.sup.Tick(); !errors.Is(err, ErrLeaseExpired) || f.sup.Status().State != StateFenced {
			t.Fatalf("Tick at expiry = %v, state %s", err, f.sup.Status().State)
		}
	})
	t.Run("tx-active lease", func(t *testing.T) {
		f := aa27StartReady(t)
		err := f.sup.Observe(Report{Kind: ReportLease, Identity: aa27Self, Generation: f.sup.Status().Generation, State: WorkerPassive, TXActive: true, Sequence: 1})
		if !errors.Is(err, ErrNotPassive) {
			t.Fatalf("TX-active lease = %v", err)
		}
	})
	t.Run("readiness deadline", func(t *testing.T) {
		f := newAA27Fixture(t)
		if err := f.sup.Start(); err != nil {
			t.Fatal(err)
		}
		f.clock.Advance(5 * time.Second)
		if err := f.sup.Tick(); !errors.Is(err, ErrReadyTimeout) {
			t.Fatalf("Tick past ready deadline = %v", err)
		}
	})
	t.Run("late readiness", func(t *testing.T) {
		f := newAA27Fixture(t)
		if err := f.sup.Start(); err != nil {
			t.Fatal(err)
		}
		f.clock.Advance(5 * time.Second)
		report := Report{Kind: ReportReady, Identity: aa27Self, Generation: f.sup.Status().Generation, Descriptors: aa27Layout, State: WorkerPassive}
		if err := f.sup.Observe(report); !errors.Is(err, ErrReadyTimeout) {
			t.Fatalf("late ready = %v", err)
		}
	})
}

func TestAA27RestartAdvancesEveryEpochAndReplaysNothing(t *testing.T) {
	f := aa27StartReady(t)
	old := f.sup.Status().Generation
	oldReady := Report{Kind: ReportReady, Identity: aa27Self, Generation: old, Descriptors: aa27Layout, State: WorkerPassive}
	if err := f.sup.PublishPlan(PlanRef{Worker: old.Worker, Stream: old.Stream, Registration: old.Registration, Plan: 1}); err != nil {
		t.Fatalf("PublishPlan: %v", err)
	}
	if err := f.sup.Enqueue(Command{Worker: old.Worker, Stream: old.Stream, Token: old.Token, Sequence: 1}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	_, firstProcess := f.launcher.last()
	if err := f.sup.Restart(context.Background()); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if stops, _ := firstProcess.counts(); stops != 1 {
		t.Fatalf("predecessor stops = %d", stops)
	}
	status := f.sup.Status()
	gen := status.Generation
	if gen.Worker != old.Worker+1 || gen.Stream != old.Stream+1 || gen.Registration != old.Registration+1 || gen.Token != old.Token+1 {
		t.Fatalf("restart generation %+v did not advance every field of %+v", gen, old)
	}
	if status.Plans != 0 || status.Pending != 0 || status.Cancelled != 1 || len(f.sup.Drain()) != 0 {
		t.Fatalf("restart carried state: %+v", status)
	}
	resources := f.allocator.all()
	if len(resources) != 4 || resources[0].closed() != 1 || resources[1].closed() != 1 || resources[2].closed() != 0 || resources[3].closed() != 0 {
		t.Fatalf("restart did not use fresh descriptors after releasing the predecessor's")
	}
	if err := f.sup.Observe(oldReady); !errors.Is(err, ErrStale) || f.sup.Status().State != StateStarting {
		t.Fatalf("predecessor readiness = %v, state %s", err, f.sup.Status().State)
	}
	f.ready(t)

	planCases := map[string]PlanRef{
		"old worker":       {Worker: old.Worker, Stream: gen.Stream, Registration: gen.Registration, Plan: 2},
		"old stream":       {Worker: gen.Worker, Stream: old.Stream, Registration: gen.Registration, Plan: 2},
		"old registration": {Worker: gen.Worker, Stream: gen.Stream, Registration: old.Registration, Plan: 2},
	}
	for name, ref := range planCases {
		if err := f.sup.PublishPlan(ref); !errors.Is(err, ErrStale) {
			t.Fatalf("%s plan = %v, want ErrStale", name, err)
		}
	}
	commandCases := map[string]Command{
		"old worker": {Worker: old.Worker, Stream: gen.Stream, Token: gen.Token, Sequence: 1},
		"old stream": {Worker: gen.Worker, Stream: old.Stream, Token: gen.Token, Sequence: 1},
		"old token":  {Worker: gen.Worker, Stream: gen.Stream, Token: old.Token, Sequence: 1},
	}
	for name, command := range commandCases {
		if err := f.sup.Enqueue(command); !errors.Is(err, ErrStale) {
			t.Fatalf("%s command = %v, want ErrStale", name, err)
		}
	}
	fresh := PlanRef{Worker: gen.Worker, Stream: gen.Stream, Registration: gen.Registration, Plan: 1}
	if err := f.sup.PublishPlan(fresh); err != nil {
		t.Fatalf("fresh plan: %v", err)
	}
	if err := f.sup.PublishPlan(fresh); !errors.Is(err, ErrPlan) {
		t.Fatalf("duplicate plan = %v", err)
	}
	if err := f.sup.Enqueue(Command{Worker: gen.Worker, Stream: gen.Stream, Token: gen.Token, Sequence: 1}); err != nil {
		t.Fatalf("fresh command: %v", err)
	}
	if drained := f.sup.Drain(); len(drained) != 1 || drained[0].Token != gen.Token {
		t.Fatalf("Drain = %+v", drained)
	}
}

func TestAA27FailedLaunchConsumesItsGeneration(t *testing.T) {
	f := newAA27Fixture(t)
	f.launcher.fail = errors.New("aa27 launch refused")
	if err := f.sup.Start(); err == nil {
		t.Fatal("Start with a failing launcher succeeded")
	}
	for _, resource := range f.allocator.all() {
		if resource.closed() != 1 {
			t.Fatal("failed launch leaked a descriptor")
		}
	}
	if liveChild.Load() {
		t.Fatal("failed launch kept the child slot")
	}
	f.launcher.fail = nil
	if err := f.sup.Start(); err != nil {
		t.Fatal(err)
	}
	if got := f.sup.Status().Generation.Worker; got != 2 {
		t.Fatalf("worker epoch after failed launch = %d, want 2", got)
	}
}

func TestAA27StopEscalatesAndAnUnreapedChildBlocksEveryStart(t *testing.T) {
	t.Run("graceful", func(t *testing.T) {
		f := aa27StartReady(t)
		_, process := f.launcher.last()
		if err := f.sup.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if stops, kills := process.counts(); stops != 1 || kills != 0 {
			t.Fatalf("stops=%d kills=%d", stops, kills)
		}
	})
	t.Run("kill after stop grace", func(t *testing.T) {
		f := newAA27Fixture(t)
		f.launcher.exitOnStop = false
		f.clock.fire[aa27StopGrace] = true
		if err := f.sup.Start(); err != nil {
			t.Fatal(err)
		}
		_, process := f.launcher.last()
		if err := f.sup.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if stops, kills := process.counts(); stops != 1 || kills != 1 {
			t.Fatalf("stops=%d kills=%d", stops, kills)
		}
		if f.sup.Status().State != StateStopped {
			t.Fatalf("state = %s", f.sup.Status().State)
		}
	})
	t.Run("kill after cancelled context", func(t *testing.T) {
		f := newAA27Fixture(t)
		f.launcher.exitOnStop = false
		if err := f.sup.Start(); err != nil {
			t.Fatal(err)
		}
		_, process := f.launcher.last()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := f.sup.Stop(ctx); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if _, kills := process.counts(); kills != 1 {
			t.Fatalf("kills = %d", kills)
		}
	})
	t.Run("unreaped", func(t *testing.T) {
		f := newAA27Fixture(t)
		f.launcher.exitOnStop, f.launcher.exitOnKill = false, false
		f.clock.fire[aa27StopGrace], f.clock.fire[aa27KillGrace] = true, true
		if err := f.sup.Start(); err != nil {
			t.Fatal(err)
		}
		_, process := f.launcher.last()
		if err := f.sup.Stop(context.Background()); !errors.Is(err, ErrUnreaped) {
			t.Fatalf("Stop = %v, want ErrUnreaped", err)
		}
		if f.sup.Status().State != StateUnreaped {
			t.Fatalf("state = %s", f.sup.Status().State)
		}
		for _, resource := range f.allocator.all() {
			if resource.closed() != 0 {
				t.Fatal("an unreaped child's descriptors were released")
			}
		}
		if err := f.sup.Start(); !errors.Is(err, ErrUnreaped) {
			t.Fatalf("Start while unreaped = %v", err)
		}
		other, err := New(aa27Config(newAA27Clock(), &aa27Launcher{}, &aa27Allocator{}))
		if err != nil {
			t.Fatal(err)
		}
		if err := other.Start(); !errors.Is(err, ErrDuplicate) {
			t.Fatalf("second supervisor Start = %v, want ErrDuplicate", err)
		}
		f.sup.mu.Lock()
		c := f.sup.child
		f.sup.mu.Unlock()
		process.exit()
		<-c.done
		if f.sup.Status().State != StateStopped {
			t.Fatalf("late exit state = %s", f.sup.Status().State)
		}
		if err := f.sup.Start(); err != nil {
			t.Fatalf("Start after late reap: %v", err)
		}
		if got := f.sup.Status().Generation.Worker; got != 2 {
			t.Fatalf("worker epoch = %d", got)
		}
	})
}

func TestAA27UnexpectedExitFencesAndReleases(t *testing.T) {
	f := aa27StartReady(t)
	gen := f.sup.Status().Generation
	if err := f.sup.PublishPlan(PlanRef{Worker: gen.Worker, Stream: gen.Stream, Registration: gen.Registration, Plan: 1}); err != nil {
		t.Fatal(err)
	}
	f.sup.mu.Lock()
	c := f.sup.child
	f.sup.mu.Unlock()
	_, process := f.launcher.last()
	process.exit()
	<-c.done
	status := f.sup.Status()
	if status.State != StateStopped || !errors.Is(status.Reason, ErrExited) || status.Plans != 0 {
		t.Fatalf("status after exit = %+v", status)
	}
	if liveChild.Load() {
		t.Fatal("exited child kept the slot")
	}
}

func TestAA27DuplicateChildAndRecursionAreRefused(t *testing.T) {
	for name, args := range map[string][]string{
		"child argv": {"/opt/gateway/bin/gateway", ChildFlag},
		"flag later": {"/opt/gateway/bin/gateway", "-x", ChildFlag},
	} {
		cfg := aa27Config(newAA27Clock(), &aa27Launcher{}, &aa27Allocator{})
		cfg.ProcessArgs = args
		if _, err := New(cfg); !errors.Is(err, ErrRecursion) {
			t.Fatalf("%s: New = %v, want ErrRecursion", name, err)
		}
	}
	f := aa27StartReady(t)
	if err := f.sup.Start(); !errors.Is(err, ErrRunning) {
		t.Fatalf("second Start = %v, want ErrRunning", err)
	}
	launcher := &aa27Launcher{}
	other, err := New(aa27Config(newAA27Clock(), launcher, &aa27Allocator{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Start(); !errors.Is(err, ErrDuplicate) || len(launcher.specs) != 0 {
		t.Fatalf("second supervisor Start = %v, launches %d", err, len(launcher.specs))
	}
}

func TestAA27AllocatorMismatchRefusesLaunch(t *testing.T) {
	for name, configure := range map[string]func(*aa27Allocator){
		"role": func(a *aa27Allocator) { a.wrongRole = true },
		"size": func(a *aa27Allocator) { a.wrongSize = true },
	} {
		t.Run(name, func(t *testing.T) {
			f := newAA27Fixture(t)
			configure(f.allocator)
			if err := f.sup.Start(); !errors.Is(err, ErrDescriptor) {
				t.Fatalf("Start = %v, want ErrDescriptor", err)
			}
			if len(f.launcher.specs) != 0 || liveChild.Load() {
				t.Fatal("mismatched descriptor reached launch or kept the slot")
			}
			for _, resource := range f.allocator.all() {
				if resource.closed() != 1 {
					t.Fatal("mismatched allocation leaked a descriptor")
				}
			}
		})
	}
}

func TestAA27ConfigRejectsMissingPortsAndUnfixedLayout(t *testing.T) {
	cases := map[string]func(*Config){
		"relative executable": func(c *Config) { c.Executable = "gateway" },
		"zero build":          func(c *Config) { c.Self.Build = Digest{} },
		"zero executable":     func(c *Config) { c.Self.Executable = Digest{} },
		"wrong ABI":           func(c *Config) { c.Self.ABI = ABIVersion + 1 },
		"layout order":        func(c *Config) { c.Descriptors = []Descriptor{aa27Layout[1], aa27Layout[0]} },
		"empty GC policy":     func(c *Config) { c.Runtime.GC = "" },
		"empty memory limit":  func(c *Config) { c.Runtime.MemoryLimit = "" },
		"lease ttl":           func(c *Config) { c.LeaseTTL = c.LeaseInterval },
		"kill grace":          func(c *Config) { c.KillGrace = 0 },
		"allocator":           func(c *Config) { c.Allocator = nil },
		"launcher":            func(c *Config) { c.Launcher = nil },
		"clock":               func(c *Config) { c.Clock = nil },
	}
	for name, mutate := range cases {
		cfg := aa27Config(newAA27Clock(), &aa27Launcher{}, &aa27Allocator{})
		mutate(&cfg)
		if _, err := New(cfg); !errors.Is(err, ErrConfig) {
			t.Fatalf("%s: New = %v, want ErrConfig", name, err)
		}
	}
}

func TestAA27ReportEncodingRoundTripsAndRefusesCorruption(t *testing.T) {
	report := Report{Kind: ReportReady, Identity: aa27Self, Generation: Generation{Worker: 3, Stream: 5, Registration: 7, Token: 11}, Descriptors: aa27Layout, State: WorkerPassive}
	raw, err := EncodeReport(report)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeReport(raw[:])
	if err != nil {
		t.Fatalf("DecodeReport: %v", err)
	}
	if decoded.Identity != report.Identity || decoded.Generation != report.Generation || !layoutEqual(decoded.Descriptors, report.Descriptors) || decoded.State != report.State || decoded.TXActive || decoded.Kind != report.Kind {
		t.Fatalf("round trip = %+v", decoded)
	}
	for name, offset := range map[string]int{"magic": 0, "tx flag": 6, "descriptor count": 7, "reserved": 12, "descriptor padding": 121} {
		t.Run(name, func(t *testing.T) {
			bad := raw
			bad[offset] ^= 0x7f
			if _, err := DecodeReport(bad[:]); !errors.Is(err, ErrReport) {
				t.Fatalf("corrupted %s decoded: %v", name, err)
			}
		})
	}
	if _, err := DecodeReport(raw[:ReportBytes-1]); !errors.Is(err, ErrReport) {
		t.Fatal("short report decoded")
	}
}

func TestAA27HostPackageCannotReachDevicesOrGatewayRuntime(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "supervisor.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{"os": true, "os/exec": true, "syscall": true, "net": true, "runtime": true, "runtime/debug": true, "unsafe": true, "golang.org/x/sys/unix": true}
	for _, imported := range file.Imports {
		path, _ := strconv.Unquote(imported.Path.Value)
		if forbidden[path] {
			t.Fatalf("supervisor.go imports %q", path)
		}
	}
}
