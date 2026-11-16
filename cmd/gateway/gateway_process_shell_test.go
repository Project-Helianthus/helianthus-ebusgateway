package main

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/releasecheck"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
)

type processShellTrace struct {
	mu    sync.Mutex
	steps []string
}

func (trace *processShellTrace) observe(step string) {
	trace.mu.Lock()
	trace.steps = append(trace.steps, step)
	trace.mu.Unlock()
}

func (trace *processShellTrace) snapshot() []string {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	return append([]string(nil), trace.steps...)
}

// phase returns the step names of one phase ("acquire", "register" or
// "release") in the order they were observed.
func (trace *processShellTrace) phase(name string) []string {
	var steps []string
	for _, step := range trace.snapshot() {
		if rest, ok := strings.CutPrefix(step, name+":"); ok {
			steps = append(steps, rest)
		}
	}
	return steps
}

func observeProcessShellSteps(t *testing.T) *processShellTrace {
	t.Helper()
	trace := &processShellTrace{}
	original := gatewayProcessShellStepObserver
	gatewayProcessShellStepObserver = trace.observe
	t.Cleanup(func() { gatewayProcessShellStepObserver = original })
	return trace
}

// useFullBuildIdentity gives the process a release version and a full build
// revision, which the synchronized evidence recorder requires.
func useFullBuildIdentity(t *testing.T) {
	t.Helper()
	originalVersion, originalID := buildVersion, buildID
	release, revision, ok := strings.Cut(synchronizedEvidenceTestVersion, "+git.")
	if !ok {
		t.Fatalf("evidence test version has no build revision: %q", synchronizedEvidenceTestVersion)
	}
	buildVersion, buildID = release, revision
	t.Cleanup(func() { buildVersion, buildID = originalVersion, originalID })
}

func requireProcessShellSteps(t *testing.T, label string, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s\n want: %s\n  got: %s", label, strings.Join(want, " -> "), strings.Join(got, " -> "))
	}
}

// processShellAcquisitionOrder is the order in which the shell acquires its
// resources; each name is observed before the acquisition runs.
var processShellAcquisitionOrder = []string{
	"run configuration",
	"semantic hub preflight",
	"release checker",
	"synchronized evidence sidecar",
	"portal contribution registry",
	"runtime state",
	"portal configuration",
	"stale eeBUS operator socket clean-up",
	"managed sidecars",
	"eBUS driver",
	"runtime control",
	"retained EVSE owner",
	"eeBUS adapter",
	"eeBUS admin handler",
	"evidence configuration",
}

// apiCompositionAcquisitionOrder is the order of the API composition steps
// that every started process runs; the static seed runs only when it is
// enabled, right after the gateway.
var apiCompositionAcquisitionOrder = []string{
	"startup source selection",
	"observe-first observers",
	"gateway",
	"graphql builder",
	"address-table inserter",
	"router plane",
	"semantic runtime",
	"late-bound writers",
	"graphql builder start",
	"HTTP control plane",
}

func processAcquisitionOrder(composition ...string) []string {
	return append(append([]string(nil), processShellAcquisitionOrder...), composition...)
}

// The complete process, with every optional resource the in-process harness
// can provide (synchronized evidence sidecar, retained EVSE owner, bus
// observability, static seed), acquires the shell resources and builds the API
// composition in order, registers each release right after its acquisition and
// releases them last in, first out. The HTTP control plane starts before the
// generation tasks are registered and bound, and its context is canceled only
// after the generation tasks have joined and the consumers have closed.
func TestGatewayProcessShellShutdownSequenceThroughLifecycle(t *testing.T) {
	useFullBuildIdentity(t)
	trace := observeProcessShellSteps(t)
	result := runRCCLifecycle(t, rccLifecycleOptions{
		dial: rccDialHealthyAfterRelease, scanOnStart: true, cancelAt: "background", releaseDialAtHTTP: true,
		observability: true,
		configure: func(cfg *ebusgateway.Config) {
			cfg.EvidenceRecorderConfig = enabledSynchronizedEvidenceConfig(t)
			cfg.ModbusTCPConfig.TeslaGen3HSC = teslaRetainedConfig()
			cfg.EnableStaticSeedTable = true
		},
	})
	if result.runErr != nil {
		t.Fatalf("run result = %v; want nil", result.runErr)
	}
	requireProcessShellSteps(t, "complete process-shell trace", trace.snapshot(), []string{
		"register:endpoint redaction",
		"acquire:run configuration",
		"acquire:semantic hub preflight",
		"acquire:release checker",
		"acquire:synchronized evidence sidecar",
		"register:synchronized evidence sidecar",
		"acquire:portal contribution registry",
		"acquire:runtime state",
		"register:runtime state",
		"acquire:portal configuration",
		"register:portal configuration",
		"acquire:stale eeBUS operator socket clean-up",
		"acquire:managed sidecars",
		"acquire:eBUS driver",
		"register:eBUS driver",
		"acquire:runtime control",
		"register:runtime control",
		"acquire:retained EVSE owner",
		"register:retained EVSE owner",
		"acquire:eeBUS adapter",
		"acquire:eeBUS admin handler",
		"acquire:evidence configuration",
		"acquire:startup source selection",
		"register:final source-selection artifact",
		"acquire:observe-first observers",
		"register:transport watch",
		"acquire:gateway",
		"register:gateway",
		"acquire:static seed",
		"acquire:graphql builder",
		"acquire:address-table inserter",
		"acquire:router plane",
		"acquire:semantic runtime",
		"acquire:late-bound writers",
		"acquire:graphql builder start",
		"register:control-plane context",
		"acquire:HTTP control plane",
		"register:consumers",
		"register:generation tasks",
		"release:generation tasks",
		"release:consumers",
		"release:control-plane context",
		"release:gateway",
		"release:transport watch",
		"release:final source-selection artifact",
		"release:retained EVSE owner",
		"release:runtime control",
		"release:eBUS driver",
		"release:portal configuration",
		"release:runtime state",
		"release:synchronized evidence sidecar",
		"release:endpoint redaction",
	})
	// The RCC harness records the HTTP shell start before any generation task.
	if shell, poller := strings.Index(result.events, "http_shell("), strings.Index(result.events, "semantic_poller"); shell < 0 || poller < shell {
		t.Fatalf("the HTTP control plane did not start before the generation tasks: %s", result.events)
	}
}

// Without the optional resources the shell registers no release for them;
// the order of the remaining releases is unchanged.
func TestGatewayProcessShellOmitsReleasesOfAbsentResources(t *testing.T) {
	trace := observeProcessShellSteps(t)
	result := runRCCLifecycle(t, rccLifecycleOptions{dial: rccDialHealthyAfterRelease, scanOnStart: true, cancelAt: "background", releaseDialAtHTTP: true})
	if result.runErr != nil {
		t.Fatalf("run result = %v; want nil", result.runErr)
	}
	requireProcessShellSteps(t, "acquisition order", trace.phase("acquire"), processAcquisitionOrder(apiCompositionAcquisitionOrder...))
	requireProcessShellSteps(t, "release order", trace.phase("release"), []string{
		"generation tasks",
		"consumers",
		"control-plane context",
		"gateway",
		"final source-selection artifact",
		"runtime control",
		"eBUS driver",
		"portal configuration",
		"runtime state",
		"endpoint redaction",
	})
}

// A failed acquisition stops the startup with the same error text and runs
// only the releases registered before it.
func TestGatewayProcessShellReleasesOnlyAcquiredResourcesOnFailure(t *testing.T) {
	t.Run("run configuration", func(t *testing.T) {
		trace := observeProcessShellSteps(t)
		result := runRCCLifecycle(t, rccLifecycleOptions{configure: func(cfg *ebusgateway.Config) {
			cfg.EvidenceRecorderConfig.Enabled = true
			cfg.EvidenceRecorderConfig.StateRoot = ""
		}})
		if result.runErr == nil || !strings.HasPrefix(result.runErr.Error(), "validate synchronized evidence config: ") {
			t.Fatalf("run result = %v; want the run configuration error", result.runErr)
		}
		requireProcessShellSteps(t, "acquisitions", trace.phase("acquire"), []string{"run configuration"})
		requireProcessShellSteps(t, "releases", trace.phase("release"), []string{"endpoint redaction"})
	})
	t.Run("evidence configuration", func(t *testing.T) {
		useFullBuildIdentity(t)
		original := synchronizedEvidenceEntropy
		synchronizedEvidenceEntropy = iotest.ErrReader(errors.New("entropy unavailable"))
		t.Cleanup(func() { synchronizedEvidenceEntropy = original })
		trace := observeProcessShellSteps(t)
		result := runRCCLifecycle(t, rccLifecycleOptions{configure: func(cfg *ebusgateway.Config) {
			cfg.EvidenceRecorderConfig = enabledSynchronizedEvidenceConfig(t)
			cfg.ModbusTCPConfig.TeslaGen3HSC = teslaRetainedConfig()
		}})
		if result.runErr == nil || result.runErr.Error() != "configure synchronized evidence sidecar: initialize synchronized evidence pseudonym key" {
			t.Fatalf("run result = %v; want the evidence configuration error", result.runErr)
		}
		requireProcessShellSteps(t, "acquisitions", trace.phase("acquire"), processShellAcquisitionOrder)
		requireProcessShellSteps(t, "releases", trace.phase("release"), []string{
			"retained EVSE owner",
			"runtime control",
			"eBUS driver",
			"portal configuration",
			"runtime state",
			"synchronized evidence sidecar",
			"endpoint redaction",
		})
	})
}

// A failed API composition step stops the startup with its own error and runs
// only the releases registered before it, last in, first out.
func TestGatewayProcessShellReleasesOnlyComposedAPIOnFailure(t *testing.T) {
	t.Run("observe-first observers", func(t *testing.T) {
		trace := observeProcessShellSteps(t)
		result := runRCCLifecycle(t, rccLifecycleOptions{configure: func(*ebusgateway.Config) {
			// runRCCLifecycle restores the seam it replaced when the test ends.
			wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
				return nil, nil, errors.New("synthetic observer failure")
			}
		}})
		if result.runErr == nil || result.runErr.Error() != "synthetic observer failure" {
			t.Fatalf("run result = %v; want the observer failure", result.runErr)
		}
		requireProcessShellSteps(t, "acquisitions", trace.phase("acquire"), processAcquisitionOrder("startup source selection", "observe-first observers"))
		requireProcessShellSteps(t, "releases", trace.phase("release"), []string{
			"final source-selection artifact",
			"runtime control",
			"eBUS driver",
			"portal configuration",
			"runtime state",
			"endpoint redaction",
		})
	})
	t.Run("HTTP control plane", func(t *testing.T) {
		trace := observeProcessShellSteps(t)
		result := runRCCLifecycle(t, rccLifecycleOptions{observability: true, configure: func(*ebusgateway.Config) {
			startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
				return nil, nil, errors.New("synthetic control plane failure")
			}
		}})
		if result.runErr == nil || result.runErr.Error() != "synthetic control plane failure" {
			t.Fatalf("run result = %v; want the control plane failure", result.runErr)
		}
		requireProcessShellSteps(t, "acquisitions", trace.phase("acquire"), processAcquisitionOrder(apiCompositionAcquisitionOrder...))
		requireProcessShellSteps(t, "releases", trace.phase("release"), []string{
			"control-plane context",
			"gateway",
			"transport watch",
			"final source-selection artifact",
			"runtime control",
			"eBUS driver",
			"portal configuration",
			"runtime state",
			"endpoint redaction",
		})
	})
}

type apiCompositionInputs struct {
	ctx    context.Context
	shell  *gatewayProcessShell
	cfg    *ebusgateway.Config
	source catalogv1.SourceCapture
}

// completeAPICompositionInputs returns a process shell that holds every
// collaborator the API composition requires. Nothing in it is started.
func completeAPICompositionInputs(t *testing.T) apiCompositionInputs {
	t.Helper()
	cfg := ebusgateway.DefaultConfig()
	ctx := context.Background()
	shell, err := newGatewayProcessShell(ctx, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	shell.runtimeStateMgr = &runtimestate.Manager{}
	shell.semanticHub = &semhub.Hub{}
	shell.releaseChecker = releasecheck.New(releasecheck.Options{CurrentVersion: "0.0.0-test"})
	shell.bindings = &managedSidecarBindings{}
	shell.ebusDriver = &ebusDriverController{}
	shell.control = &gatewayRuntimeControl{}
	shell.eebusAdapter = &eebusRuntimeAdapter{}
	shell.eebusAdminHandler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	return apiCompositionInputs{ctx: ctx, shell: shell, cfg: &cfg, source: newGatewayPortalCatalogSource(nil, nil, nil, contributions)}
}

// Each required collaborator of the API composition is rejected with its own
// error before any step runs; a complete set is accepted without a step.
func TestGatewayAPICompositionRejectsMissingWiring(t *testing.T) {
	var missingContext context.Context
	cases := []struct {
		name   string
		remove func(*apiCompositionInputs)
	}{
		{"process context", func(in *apiCompositionInputs) { in.ctx = missingContext }},
		{"process shell", func(in *apiCompositionInputs) { in.shell = nil }},
		{"run configuration", func(in *apiCompositionInputs) { in.cfg = nil }},
		{"Portal catalog source", func(in *apiCompositionInputs) { in.source = nil }},
		{"process shell run configuration", func(in *apiCompositionInputs) {
			other := *in.cfg
			in.cfg = &other
		}},
		{"runtime state", func(in *apiCompositionInputs) { in.shell.runtimeStateMgr = nil }},
		{"semantic publication hub", func(in *apiCompositionInputs) { in.shell.semanticHub = nil }},
		{"release checker", func(in *apiCompositionInputs) { in.shell.releaseChecker = nil }},
		{"managed sidecar bindings", func(in *apiCompositionInputs) { in.shell.bindings = nil }},
		{"eBUS driver", func(in *apiCompositionInputs) { in.shell.ebusDriver = nil }},
		{"runtime control", func(in *apiCompositionInputs) { in.shell.control = nil }},
		{"eeBUS adapter", func(in *apiCompositionInputs) { in.shell.eebusAdapter = nil }},
		{"eeBUS admin handler", func(in *apiCompositionInputs) { in.shell.eebusAdminHandler = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := completeAPICompositionInputs(t)
			tc.remove(&inputs)
			trace := observeProcessShellSteps(t)
			api, err := newGatewayAPIComposition(inputs.ctx, inputs.shell, inputs.cfg, inputs.source)
			want := "gateway API composition: " + tc.name + " is required"
			if api != nil || err == nil || err.Error() != want {
				t.Fatalf("newGatewayAPIComposition() = %v, %v; want nil, %q", api, err, want)
			}
			if steps := trace.snapshot(); len(steps) != 0 {
				t.Fatalf("a rejected composition ran steps: %v", steps)
			}
		})
	}
	t.Run("complete", func(t *testing.T) {
		inputs := completeAPICompositionInputs(t)
		trace := observeProcessShellSteps(t)
		api, err := newGatewayAPIComposition(inputs.ctx, inputs.shell, inputs.cfg, inputs.source)
		if api == nil || err != nil {
			t.Fatalf("newGatewayAPIComposition() = %v, %v; want a composition", api, err)
		}
		if steps := trace.snapshot(); len(steps) != 0 {
			t.Fatalf("constructing the composition ran steps: %v", steps)
		}
		// serve runs after the retained eBUS consumers exist; without them it
		// starts nothing.
		if err := api.serve(nil); err == nil || err.Error() != "gateway API composition: retained eBUS consumers are required" {
			t.Fatalf("serve(nil) = %v; want the missing retained consumers error", err)
		}
		if steps := trace.snapshot(); len(steps) != 0 {
			t.Fatalf("a rejected serve ran steps: %v", steps)
		}
	})
}

// Releases join their errors into the process result last in, first out;
// logged releases leave it unchanged; the endpoint redaction runs last over
// the joined result and sees endpoints resolved after the shell was created.
func TestGatewayProcessShellJoinsReleaseErrorsAndRedactsLast(t *testing.T) {
	trace := observeProcessShellSteps(t)
	cfg := ebusgateway.DefaultConfig()
	shell, err := newGatewayProcessShell(context.Background(), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	var ran []string
	shell.onShutdown("first", func(result error) error {
		ran = append(ran, "first")
		return errors.Join(result, errors.New("shutdown first: dial "+h08EndpointA+" failed"))
	})
	shell.onShutdownFunc("logged", func() { ran = append(ran, "logged") })
	shell.onShutdown("second", func(result error) error {
		ran = append(ran, "second")
		return errors.Join(result, errors.New("shutdown second: closed"))
	})
	// The run configuration resolves the endpoint document after the shell
	// exists; the redaction reads the configuration when it runs.
	cfg.ModbusTCPConfig.Endpoints = []ebusgateway.ModbusTCPEndpointConfig{{Label: "default", Family: "sunspec", Endpoint: h08EndpointA}}

	got := shell.runShutdown(errors.New("process stopped"))
	want := "process stopped\nshutdown second: closed\nshutdown first: dial " + redactedModbusEndpoint + " failed"
	if got == nil || got.Error() != want {
		t.Fatalf("shutdown result = %q; want %q", got, want)
	}
	if strings.Contains(got.Error(), "sunspec-a") {
		t.Fatalf("shutdown result leaks a file-sourced endpoint: %v", got)
	}
	requireProcessShellSteps(t, "release order", trace.phase("release"), []string{"second", "logged", "first", "endpoint redaction"})
	requireProcessShellSteps(t, "releases run", ran, []string{"second", "logged", "first"})

	if again := shell.runShutdown(nil); again != nil {
		t.Fatalf("second shutdown result = %v; want nil", again)
	}
	if len(trace.phase("release")) != 4 {
		t.Fatalf("the shutdown sequence ran twice: %v", trace.phase("release"))
	}

	clean, err := newGatewayProcessShell(context.Background(), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := clean.runShutdown(nil); got != nil {
		t.Fatalf("shutdown without errors = %v; want nil", got)
	}
}

// A release that panics does not stop the earlier releases, exactly as with
// stacked deferred statements; the panic continues afterwards.
func TestGatewayProcessShellRunsEarlierReleasesWhenAReleasePanics(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	shell, err := newGatewayProcessShell(context.Background(), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	var ran []string
	shell.onShutdownFunc("first", func() { ran = append(ran, "first") })
	shell.onShutdownFunc("panicking", func() {
		ran = append(ran, "panicking")
		panic("release failed")
	})
	shell.onShutdownFunc("last", func() { ran = append(ran, "last") })
	recovered := func() (value any) {
		defer func() { value = recover() }()
		_ = shell.runShutdown(nil)
		return nil
	}()
	if recovered != "release failed" {
		t.Fatalf("recovered = %v; want the release panic", recovered)
	}
	requireProcessShellSteps(t, "releases run", ran, []string{"last", "panicking", "first"})
}

func TestGatewayProcessShellRejectsMissingWiring(t *testing.T) {
	trace := observeProcessShellSteps(t)
	cfg := ebusgateway.DefaultConfig()
	var missingContext context.Context
	for _, tc := range []struct {
		name string
		ctx  context.Context
		cfg  *ebusgateway.Config
		want string
	}{
		{name: "process context", ctx: missingContext, cfg: &cfg, want: "gateway process shell: process context is required"},
		{name: "gateway configuration", ctx: context.Background(), cfg: nil, want: "gateway process shell: gateway configuration is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shell, err := newGatewayProcessShell(tc.ctx, tc.cfg)
			if shell != nil || err == nil || err.Error() != tc.want {
				t.Fatalf("newGatewayProcessShell() = %v, %v; want nil, %q", shell, err, tc.want)
			}
		})
	}
	if steps := trace.snapshot(); len(steps) != 0 {
		t.Fatalf("a rejected shell registered steps: %v", steps)
	}
}
