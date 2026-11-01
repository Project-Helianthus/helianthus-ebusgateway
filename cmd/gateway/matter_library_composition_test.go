package main

import (
	"context"
	"errors"
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"sync"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/matter/libraryhost"
)

var aa17Selection = libraryhost.Selection{Tag: "tag-a", Target: "target-a", Profile: "profile-a", Evidence: "evidence-a"}

// aa17Ports is a fixture binding with every port call counted.
type aa17Ports struct {
	mu       sync.Mutex
	captures int
	news     int
	closes   int
	absent   bool
}

func (p *aa17Ports) Capture(context.Context) (libraryhost.Capture, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.captures++
	return libraryhost.Capture{Generations: libraryhost.Generations{Source: 1, Binding: 2, NativeDriver: 3}}, nil
}

func (p *aa17Ports) Qualify(_ context.Context, request libraryhost.Request) ([]libraryhost.Row, error) {
	if p.absent {
		return nil, nil
	}
	rows := []libraryhost.Row{}
	for _, kind := range []libraryhost.RowKind{libraryhost.RowTag, libraryhost.RowBuild, libraryhost.RowRuntime, libraryhost.RowProfile, libraryhost.RowAdmission} {
		rows = append(rows, libraryhost.Row{Kind: kind, Verdict: libraryhost.VerdictPass, Origin: libraryhost.ClassSyntheticFixture, Selection: request.Selection, Generations: request.Generations, Token: request.Token})
	}
	return rows, nil
}

func (p *aa17Ports) Class() libraryhost.BindingClass { return libraryhost.ClassSyntheticFixture }

func (p *aa17Ports) New(context.Context, *libraryhost.Session) (libraryhost.Binding, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.news++
	return p, nil
}

func (p *aa17Ports) Close(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closes++
	return nil
}

func (p *aa17Ports) counts() (captures, news, closes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.captures, p.news, p.closes
}

func aa17Composer(ports *aa17Ports, calls *int) matterLibraryComposer {
	return func() (libraryhost.Config, error) {
		*calls++
		return libraryhost.Config{Selection: aa17Selection, Factory: ports, Qualifier: ports, Snapshots: ports}, nil
	}
}

func aa17ParseOption(t *testing.T, args ...string) *matterLibraryOption {
	t.Helper()
	fs := flag.NewFlagSet("aa17", flag.ContinueOnError)
	option := bindMatterLibraryFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return option
}

func TestAA17FlagIsDefaultFalseOnAnExplicitFlagSet(t *testing.T) {
	fs := flag.NewFlagSet("aa17", flag.ContinueOnError)
	option := bindMatterLibraryFlags(fs)
	registered := fs.Lookup(matterLibraryEnabledFlag)
	if registered == nil || registered.DefValue != "false" || matterLibraryEnabledFlag != "matter-library-enabled" {
		t.Fatalf("flag=%+v", registered)
	}
	if option.enabled {
		t.Fatal("unparsed option is enabled")
	}
	if flag.CommandLine.Lookup(matterLibraryEnabledFlag) != nil {
		t.Fatal("option leaked onto the global flag set")
	}
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{"-matter-library-enabled"}, true},
		{[]string{"-matter-library-enabled=true"}, true},
		{[]string{"-matter-library-enabled=false"}, false},
	} {
		if got := aa17ParseOption(t, tc.args...).enabled; got != tc.want {
			t.Fatalf("args=%v enabled=%v", tc.args, got)
		}
	}
	if option := bindMatterLibraryFlags(nil); option == nil || option.enabled {
		t.Fatalf("nil flag set option=%+v", option)
	}
	// Binding the existing gateway flags on the same set must not collide.
	cfg := ebusgateway.DefaultConfig()
	both := flag.NewFlagSet("aa17-both", flag.ContinueOnError)
	bindFlags(both, &cfg)
	bindMatterLibraryFlags(both)
}

func TestAA17DisabledConstructsNothing(t *testing.T) {
	for name, option := range map[string]*matterLibraryOption{
		"default": aa17ParseOption(t),
		"nil":     nil,
		"zero":    {},
	} {
		t.Run(name, func(t *testing.T) {
			ports := &aa17Ports{}
			calls := 0
			owner := newMatterLibraryComposition(option, aa17Composer(ports, &calls))
			active, err := owner.Start(context.Background())
			if active || err != nil {
				t.Fatalf("active=%v err=%v", active, err)
			}
			if err := owner.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			captures, news, _ := ports.counts()
			if calls != 0 || captures != 0 || news != 0 || owner.host != nil {
				t.Fatalf("calls=%d captures=%d news=%d host=%v", calls, captures, news, owner.host)
			}
			if state := owner.Status().State; state != libraryhost.StateDisabled {
				t.Fatalf("state=%s", state)
			}
		})
	}
}

func TestAA17EnabledWithoutCompleteInputsIsUnavailable(t *testing.T) {
	enabled := aa17ParseOption(t, "-matter-library-enabled")
	requireUnavailable := func(t *testing.T, owner *matterLibraryComposition, reason libraryhost.Reason) {
		t.Helper()
		active, err := owner.Start(context.Background())
		if active || !errors.Is(err, libraryhost.ErrUnavailable) {
			t.Fatalf("active=%v err=%v", active, err)
		}
		status := owner.Status()
		if status.State != libraryhost.StateUnavailable || status.Reason != reason {
			t.Fatalf("status=%+v", status)
		}
	}
	t.Run("no-composer", func(t *testing.T) {
		requireUnavailable(t, newMatterLibraryComposition(enabled, nil), matterLibraryReasonCompositionMissing)
	})
	t.Run("composer-failure-text-is-not-retained", func(t *testing.T) {
		owner := newMatterLibraryComposition(enabled, func() (libraryhost.Config, error) {
			return libraryhost.Config{}, errors.New("synthetic-private-detail")
		})
		_, err := owner.Start(context.Background())
		if err == nil || strings.Contains(err.Error(), "synthetic-private-detail") {
			t.Fatalf("err=%v", err)
		}
		requireUnavailable(t, owner, matterLibraryReasonCompositionFailed)
	})
	t.Run("empty-composition", func(t *testing.T) {
		owner := newMatterLibraryComposition(enabled, func() (libraryhost.Config, error) { return libraryhost.Config{Selection: aa17Selection}, nil })
		requireUnavailable(t, owner, libraryhost.ReasonFactoryMissing)
	})
	t.Run("absent-evidence", func(t *testing.T) {
		ports := &aa17Ports{absent: true}
		calls := 0
		owner := newMatterLibraryComposition(enabled, aa17Composer(ports, &calls))
		requireUnavailable(t, owner, libraryhost.ReasonEvidenceAbsent)
		if _, news, _ := ports.counts(); news != 0 || calls != 1 {
			t.Fatalf("news=%d composer calls=%d", news, calls)
		}
	})
}

func TestAA17CompositionThreeCyclesLeaveNothingOwned(t *testing.T) {
	ports := &aa17Ports{}
	calls := 0
	owner := newMatterLibraryComposition(aa17ParseOption(t, "-matter-library-enabled"), aa17Composer(ports, &calls))
	ctx := context.Background()
	for cycle := 1; cycle <= 3; cycle++ {
		active, err := owner.Start(ctx)
		if !active || err != nil {
			t.Fatalf("active=%v err=%v", active, err)
		}
		if err := owner.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		status := owner.Status()
		if status.State != libraryhost.StateStopped || status.OwnedWork != 0 || status.Resources != 0 || status.Epoch != uint64(cycle) {
			t.Fatalf("cycle %d status=%+v", cycle, status)
		}
		if _, news, closes := ports.counts(); news != cycle || closes != cycle {
			t.Fatalf("cycle %d news=%d closes=%d", cycle, news, closes)
		}
	}
	if calls != 1 {
		t.Fatalf("composer calls=%d", calls)
	}
}

// aa17Owner records lifecycle order and can hold its stop open.
type aa17Owner struct {
	mu        sync.Mutex
	events    []string
	active    bool
	startErr  error
	stopErr   error
	stopHook  func(context.Context)
	stopCalls int
}

func (o *aa17Owner) record(event string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
}

func (o *aa17Owner) log() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return strings.Join(o.events, ",")
}

func (o *aa17Owner) Start(context.Context) (bool, error) {
	o.record("start")
	return o.active, o.startErr
}

func (o *aa17Owner) Stop(ctx context.Context) error {
	o.mu.Lock()
	o.stopCalls++
	hook := o.stopHook
	o.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	o.record("stop")
	return o.stopErr
}

func TestAA17InactiveOwnerLeavesTheLifecycleUnchanged(t *testing.T) {
	type ctxKey struct{}
	parent := context.WithValue(context.Background(), ctxKey{}, "parent")
	for _, tc := range []struct {
		name  string
		owner matterLibraryOwner
	}{
		{"nil-owner", nil},
		{"disabled-composition", newMatterLibraryComposition(aa17ParseOption(t), nil)},
		{"unavailable-composition", newMatterLibraryComposition(aa17ParseOption(t, "-matter-library-enabled"), nil)},
		{"unavailable-is-not-a-gateway-failure", &aa17Owner{startErr: &libraryhost.UnavailableError{Reason: libraryhost.ReasonEvidenceAbsent}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantErr := errors.New("synthetic gateway result")
			var gotCtx context.Context
			runs := 0
			err := runGatewayLifecycleWithMatterLibrary(parent, ebusgateway.Config{HTTPAddr: "synthetic"}, tc.owner, func(ctx context.Context, cfg ebusgateway.Config) error {
				runs++
				gotCtx = ctx
				if cfg.HTTPAddr != "synthetic" {
					t.Fatalf("config changed: %q", cfg.HTTPAddr)
				}
				return wantErr
			})
			if !errors.Is(err, wantErr) || runs != 1 || gotCtx != parent {
				t.Fatalf("err=%v runs=%d same ctx=%v", err, runs, gotCtx == parent)
			}
			if owner, ok := tc.owner.(*aa17Owner); ok && owner.stopCalls != 0 {
				t.Fatalf("stop calls=%d", owner.stopCalls)
			}
		})
	}
	if err := runGatewayLifecycleWithMatterLibrary(parent, ebusgateway.Config{}, nil, nil); !errors.Is(err, errMatterLibraryLifecycleMissing) {
		t.Fatalf("err=%v", err)
	}
}

func TestAA17ShutdownStopsTheLibraryBeforeReleasingTheGateway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner := &aa17Owner{active: true}
	gatewayCancelled := make(chan struct{})
	holdStop := make(chan struct{})
	stopEntered := make(chan struct{})
	owner.stopHook = func(context.Context) {
		close(stopEntered)
		<-holdStop
	}
	running := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runGatewayLifecycleWithMatterLibrary(ctx, ebusgateway.Config{}, owner, func(runCtx context.Context, _ ebusgateway.Config) error {
			owner.record("run")
			close(running)
			<-runCtx.Done()
			owner.record("gateway-cancelled")
			close(gatewayCancelled)
			return nil
		})
	}()
	<-running
	cancel()
	<-stopEntered
	select {
	case <-gatewayCancelled:
		t.Fatal("gateway released before the library stop finished")
	default:
	}
	close(holdStop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := owner.log(); got != "start,run,stop,gateway-cancelled" {
		t.Fatalf("events=%s", got)
	}
}

func TestAA17GatewayExitJoinsTheStopAndKeepsBothErrors(t *testing.T) {
	gatewayErr := errors.New("synthetic gateway exit")
	stopErr := errors.New("synthetic stop failure")
	owner := &aa17Owner{active: true, stopErr: stopErr}
	err := runGatewayLifecycleWithMatterLibrary(context.Background(), ebusgateway.Config{}, owner, func(context.Context, ebusgateway.Config) error {
		owner.record("run")
		return gatewayErr
	})
	if !errors.Is(err, gatewayErr) || !errors.Is(err, stopErr) {
		t.Fatalf("err=%v", err)
	}
	if got := owner.log(); got != "start,run,stop" || owner.stopCalls != 1 {
		t.Fatalf("events=%s stops=%d", got, owner.stopCalls)
	}
}

func TestAA17StopBudgetBoundsAHungLibrary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	budget, expire := context.WithCancel(context.Background())
	defer expire()
	owner := &aa17Owner{active: true, stopErr: libraryhost.ErrQuiesce}
	var stopCtx context.Context
	owner.stopHook = func(ctx context.Context) {
		stopCtx = ctx
		expire()
		<-ctx.Done()
	}
	err := superviseMatterLibrary(ctx, ebusgateway.Config{}, owner, func(runCtx context.Context, _ ebusgateway.Config) error {
		cancel()
		<-runCtx.Done()
		return nil
	}, func(context.Context) (context.Context, context.CancelFunc) {
		return budget, func() {}
	})
	if !errors.Is(err, libraryhost.ErrQuiesce) || stopCtx != budget {
		t.Fatalf("err=%v same budget=%v", err, stopCtx == budget)
	}

	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	bounded, release := matterLibraryStopContext(parent)
	defer release()
	if _, finite := bounded.Deadline(); !finite || bounded.Err() != nil {
		t.Fatalf("production stop context finite=%v err=%v", finite, bounded.Err())
	}
}

func TestAA17LifecycleCompositionRunsAndReleasesTheRealHost(t *testing.T) {
	ports := &aa17Ports{}
	calls := 0
	owner := newMatterLibraryComposition(aa17ParseOption(t, "-matter-library-enabled"), aa17Composer(ports, &calls))
	for cycle := 1; cycle <= 3; cycle++ {
		ctx, cancel := context.WithCancel(context.Background())
		err := runGatewayLifecycleWithMatterLibrary(ctx, ebusgateway.Config{}, owner, func(runCtx context.Context, _ ebusgateway.Config) error {
			if state := owner.Status().State; state != libraryhost.StateRunning {
				t.Fatalf("state during run=%s", state)
			}
			cancel()
			<-runCtx.Done()
			return nil
		})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		status := owner.Status()
		if status.State != libraryhost.StateStopped || status.Resources != 0 || status.OwnedWork != 0 {
			t.Fatalf("cycle %d status=%+v", cycle, status)
		}
		if _, news, closes := ports.counts(); news != cycle || closes != cycle {
			t.Fatalf("cycle %d news=%d closes=%d", cycle, news, closes)
		}
	}
}

func aa17CompositionViolations(t *testing.T, source any) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "matter_library_composition.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var violations []string
	for _, spec := range file.Imports {
		path := strings.Trim(spec.Path.Value, `"`)
		if path == "net/http" || strings.Contains(path, "go-fabric") || strings.Contains(path, "matter/feedv1") || strings.Contains(path, "internal/adaptermux") {
			violations = append(violations, "import "+path)
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.FuncDecl:
			if n.Name.Name == "init" && n.Recv == nil {
				violations = append(violations, "init")
			}
		case *ast.SelectorExpr:
			if ident, ok := n.X.(*ast.Ident); ok && ident.Name == "flag" && n.Sel.Name == "CommandLine" {
				violations = append(violations, "flag.CommandLine")
			}
		}
		return true
	})
	return violations
}

func TestAA17CompositionHasNoInitRegistrationOrGlobalBinding(t *testing.T) {
	if got := aa17CompositionViolations(t, nil); len(got) != 0 {
		t.Fatalf("violations=%v", got)
	}
	for _, tc := range []struct{ source, want string }{
		{"package main\nfunc init() {}\n", "init"},
		{"package main\nimport \"flag\"\nvar _ = flag.CommandLine\n", "flag.CommandLine"},
		{"package main\nimport _ \"net/http\"\n", "import net/http"},
		{"package main\nimport _ \"example.invalid/go-fabric\"\n", "import example.invalid/go-fabric"},
		{"package main\nimport _ \"example.invalid/matter/feedv1\"\n", "import example.invalid/matter/feedv1"},
		{"package main\nimport _ \"example.invalid/internal/adaptermux\"\n", "import example.invalid/internal/adaptermux"},
	} {
		if got := aa17CompositionViolations(t, tc.source); len(got) != 1 || got[0] != tc.want {
			t.Fatalf("control %q violations=%v", tc.want, got)
		}
	}
}
