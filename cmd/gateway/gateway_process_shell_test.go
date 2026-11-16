package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
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

// The complete process, with every optional resource the in-process harness
// can provide (synchronized evidence sidecar, retained EVSE owner, bus
// observability), acquires the shell resources in order, registers each
// release right after its acquisition and releases them last in, first out.
func TestGatewayProcessShellShutdownSequenceThroughLifecycle(t *testing.T) {
	useFullBuildIdentity(t)
	trace := observeProcessShellSteps(t)
	result := runRCCLifecycle(t, rccLifecycleOptions{
		dial: rccDialHealthyAfterRelease, scanOnStart: true, cancelAt: "background", releaseDialAtHTTP: true,
		observability: true,
		configure: func(cfg *ebusgateway.Config) {
			cfg.EvidenceRecorderConfig = enabledSynchronizedEvidenceConfig(t)
			cfg.ModbusTCPConfig.TeslaGen3HSC = teslaRetainedConfig()
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
		"register:final source-selection artifact",
		"register:transport watch",
		"register:gateway",
		"register:control-plane context",
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
	requireProcessShellSteps(t, "acquisition order", trace.phase("acquire"), processShellAcquisitionOrder)
}

// Without the optional resources the shell registers no release for them;
// the order of the remaining releases is unchanged.
func TestGatewayProcessShellOmitsReleasesOfAbsentResources(t *testing.T) {
	trace := observeProcessShellSteps(t)
	result := runRCCLifecycle(t, rccLifecycleOptions{dial: rccDialHealthyAfterRelease, scanOnStart: true, cancelAt: "background", releaseDialAtHTTP: true})
	if result.runErr != nil {
		t.Fatalf("run result = %v; want nil", result.runErr)
	}
	requireProcessShellSteps(t, "acquisition order", trace.phase("acquire"), processShellAcquisitionOrder)
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
