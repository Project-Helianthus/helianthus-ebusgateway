package main

// M69 regression: an eebus.primary start failure under the production
// startup is classified, logged once per failed start and retried within the
// bounded window, and the working factory still reaches RUNNING. The flag
// set is the one the Home Assistant add-on wrapper passes; every value is
// synthetic (documentation addresses, a repeated-pattern SKI, a temporary
// state root). Only the eebusreg factories and the interface resolver are
// replaced.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusadmin"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const (
	m69Interface     = "end0"
	m69FailureLine   = "eeBUS runtime unavailable; continuing without eeBUS"
	m69RetryDelay    = 60 * time.Millisecond
	m69ListenerCause = "listen tcp 192.0.2.4:4712: bind: address already in use"
)

var m69RemoteSKI = strings.Repeat("ab", 20)

type m69LogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *m69LogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *m69LogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// m69CaptureLog redirects the process log for the test. It is registered
// before the process starts, so it is restored after the process stopped.
func m69CaptureLog(t *testing.T) *m69LogBuffer {
	t.Helper()
	logs := &m69LogBuffer{}
	original := log.Writer()
	log.SetOutput(logs)
	t.Cleanup(func() { log.SetOutput(original) })
	return logs
}

// m69ShortRetry keeps the production attempt bound and shortens only the
// delay, so a test observes all three attempts quickly.
func m69ShortRetry(t *testing.T) {
	t.Helper()
	original := eebusManagedStartRetry
	t.Cleanup(func() { eebusManagedStartRetry = original })
	eebusManagedStartRetry.Delay = m69RetryDelay
}

// m69AddonEEBusConfig parses the eeBUS arguments the add-on wrapper emits.
func m69AddonEEBusConfig(t *testing.T, stateRoot string) ebusgateway.EEBusConfig {
	t.Helper()
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("addon", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{
		"-eebus-enabled=true",
		"-eebus-listen-port", "4712",
		"-eebus-interfaces", m69Interface,
		"-eebus-subnets", "192.0.2.0/24",
		"-eebus-state-root", stateRoot,
		"-eebus-discovery-enabled=true",
		"-eebus-remote-ski-allowlist", m69RemoteSKI,
		"-eebus-pairing-window-mode", "closed",
	}); err != nil {
		t.Fatalf("parse add-on eeBUS flags: %v", err)
	}
	return cfg.EEBusConfig
}

// m69Construction is one native construction: which factory ran and when.
type m69Construction struct {
	operator bool
	config   eebusruntime.Config
	at       time.Time
}

type m69Natives struct {
	mu   sync.Mutex
	list []m69Construction
}

func (n *m69Natives) snapshot() []m69Construction {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]m69Construction(nil), n.list...)
}

// m69Seams installs the interface resolver and the native factories before
// run() builds the eeBUS descriptor, which captures them by value. Each
// factory receives its own 1-based call number.
func m69Seams(t *testing.T, operator func(call int) eebusruntime.Runtime, ordinary func(call int) eebusruntime.Runtime) *m69Natives {
	t.Helper()
	originalResolver, originalOperator, originalOrdinary := resolveEEBusInterfaceAddressesFn, newEEBusOperatorRuntimeFn, newEEBusRuntimeFn
	t.Cleanup(func() {
		resolveEEBusInterfaceAddressesFn, newEEBusOperatorRuntimeFn, newEEBusRuntimeFn = originalResolver, originalOperator, originalOrdinary
	})
	natives := &m69Natives{}
	record := func(isOperator bool, c eebusruntime.Config) int {
		natives.mu.Lock()
		defer natives.mu.Unlock()
		natives.list = append(natives.list, m69Construction{operator: isOperator, config: c, at: time.Now()})
		calls := 0
		for _, n := range natives.list {
			if n.operator == isOperator {
				calls++
			}
		}
		return calls
	}
	resolveEEBusInterfaceAddressesFn = func(name string) ([]netip.Addr, error) {
		if name != m69Interface {
			return nil, errors.New("unexpected interface")
		}
		return []netip.Addr{
			netip.MustParseAddr("192.0.2.4"),
			netip.MustParseAddr("2001:db8::4"),
			netip.MustParseAddr("fe80::4").WithZone(m69Interface),
		}, nil
	}
	newEEBusOperatorRuntimeFn = func(c eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
		return operator(record(true, c)), issue809AdminStub{}, nil
	}
	newEEBusRuntimeFn = func(c eebusruntime.Config) (eebusruntime.Runtime, error) {
		return ordinary(record(false, c)), nil
	}
	return natives
}

func m69Healthy(int) eebusruntime.Runtime { return &msp05bRuntime{} }

func m69Failing(cause string) func(int) eebusruntime.Runtime {
	return func(int) eebusruntime.Runtime { return &msp05bRuntime{startErr: errors.New(cause)} }
}

// m69Start runs the production startup with the add-on eeBUS configuration
// and waits until eebus.primary settles at RUNNING or FAILED.
func m69Start(t *testing.T, eebus ebusgateway.EEBusConfig) (*rctlProcess, *managedSidecars, runtimecontrol.DriverStatus) {
	t.Helper()
	var sidecars *managedSidecars
	process := newRCtlEnv(t).start(rctlProcessOptions{
		config: func(cfg *ebusgateway.Config) { cfg.EEBusConfig = eebus },
		mutate: func(o *gatewayRuntimeControlOptions) { sidecars = o.Sidecars },
	})
	service := process.control.Service()
	var status runtimecontrol.DriverStatus
	deadline := time.Now().Add(rctlBound)
	for {
		var err error
		status, err = service.Get(managedEEBusID)
		if err == nil && (status.ObservedState == drivermanager.ObservedRunning || status.ObservedState == drivermanager.ObservedFailed) {
			return process, sidecars, status
		}
		if time.Now().After(deadline) {
			t.Fatalf("eebus.primary did not settle: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}
}

func m69AdminReadiness(t *testing.T, sidecars *managedSidecars) (int, eebusadmin.ReadinessV1) {
	t.Helper()
	bindings := &managedSidecarBindings{sidecars: sidecars}
	response := httptest.NewRecorder()
	bindings.serveEEBusAdmin(response, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil))
	var envelope struct {
		Data struct {
			Readiness eebusadmin.ReadinessV1 `json:"readiness"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("admin status body %q: %v", response.Body.String(), err)
	}
	return response.Code, envelope.Data.Readiness
}

// m69Events returns the service events of eebus.primary.
func m69Events(process *rctlProcess) []runtimecontrol.Event {
	var events []runtimecontrol.Event
	for _, event := range process.control.Service().EventsSince(0).Events {
		if event.DriverID == managedEEBusID && event.Kind == runtimecontrol.EventObservedChanged {
			events = append(events, event)
		}
	}
	return events
}

func m69FailureLines(text string) []string {
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, m69FailureLine) {
			lines = append(lines, line)
		}
	}
	return lines
}

// The working factory still composes and starts with the add-on flags, with
// one native construction and no failure line.
func TestM69AddonEEBusWorkingFactoryReachesRunning(t *testing.T) {
	logs := m69CaptureLog(t)
	m69ShortRetry(t)
	stateRoot := issue755SecureStateRoot(t)
	natives := m69Seams(t, m69Healthy, m69Healthy)
	_, sidecars, status := m69Start(t, m69AddonEEBusConfig(t, stateRoot))
	if status.ObservedState != drivermanager.ObservedRunning || status.Reason != drivermanager.ReasonNone {
		t.Fatalf("eebus.primary = %s/%s; want RUNNING/NONE", status.ObservedState, status.Reason)
	}
	constructions := natives.snapshot()
	if len(constructions) != 1 || !constructions[0].operator {
		t.Fatalf("native constructions = %+v; want one operator runtime", constructions)
	}
	got := constructions[0].config
	if got.StateRoot != stateRoot || got.Interface != m69Interface || got.ListenAddress != netip.MustParseAddrPort("192.0.2.4:4712") ||
		!got.DiscoveryEnabled || len(got.Remotes) != 1 || got.PairingPolicy != eebusruntime.PairingPolicyClosed {
		t.Fatalf("native config = %+v", got)
	}
	if lifecycle := (&managedSidecarBindings{sidecars: sidecars}).eebusLifecycleSnapshot(); lifecycle.State != eebusLifecycleRunning || !lifecycle.AdminAvailable || lifecycle.DegradedReason != "" {
		t.Fatalf("eeBUS lifecycle = %+v; want running with the admin boundary", lifecycle)
	}
	if lines := m69FailureLines(logs.String()); len(lines) != 0 {
		t.Fatalf("working start logged a failure: %q", lines)
	}
}

// A native start failure in both stages is classified, logged once per
// failed start with the stage and the categories, retried three times with
// the policy delay, and ends FAILED/PROVIDER_UNAVAILABLE, never
// INTERNAL_ERROR.
func TestM69AddonEEBusStartFailureIsClassifiedLoggedAndRetried(t *testing.T) {
	for _, test := range []struct {
		name  string
		cause func(stateRoot string) string
		want  eebusadmin.EEBusDegradedReason
	}{
		{name: "listener", cause: func(string) string { return m69ListenerCause }, want: eebusadmin.EEBusDegradedReasonListenerUnavailable},
		{name: "protected credentials", cause: func(stateRoot string) string {
			return "eebus runtime protected credentials are unavailable: open " + stateRoot + "/identity/" + strings.Repeat("cd", 20) + ": permission denied"
		}, want: eebusadmin.EEBusDegradedReasonLocalIdentityUnavailable},
		{name: "unclassified", cause: func(string) string { return "native backend refused the session" }, want: eebusadmin.EEBusDegradedReasonRuntimeFactoryUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs := m69CaptureLog(t)
			m69ShortRetry(t)
			stateRoot := issue755SecureStateRoot(t)
			cause := test.cause(stateRoot)
			natives := m69Seams(t, m69Failing(cause), m69Failing(cause))
			process, sidecars, status := m69Start(t, m69AddonEEBusConfig(t, stateRoot))

			if status.ObservedState != drivermanager.ObservedFailed || status.Reason != drivermanager.ReasonProviderUnavailable {
				t.Fatalf("eebus.primary = %s/%s; want FAILED/PROVIDER_UNAVAILABLE", status.ObservedState, status.Reason)
			}
			snapshot, _ := process.control.driver.manager.Snapshot(string(managedEEBusID))
			if snapshot.ObservedState != drivermanager.ObservedFailed || snapshot.Reason.Code != drivermanager.ReasonProviderUnavailable {
				t.Fatalf("manager = %s/%s; want FAILED/PROVIDER_UNAVAILABLE", snapshot.ObservedState, snapshot.Reason.Code)
			}

			// Three start attempts, each an operator construction followed by
			// the read-only fallback, the retries at least the policy delay
			// apart.
			constructions := natives.snapshot()
			if len(constructions) != 2*eebusStartupMaxAttempts {
				t.Fatalf("native constructions = %d; want %d (operator and fallback per attempt)", len(constructions), 2*eebusStartupMaxAttempts)
			}
			var operatorAt []time.Time
			for i, c := range constructions {
				if c.operator != (i%2 == 0) {
					t.Fatalf("construction %d operator=%t; want operator then fallback per attempt", i, c.operator)
				}
				if c.operator {
					operatorAt = append(operatorAt, c.at)
				}
			}
			for i := 1; i < len(operatorAt); i++ {
				if gap := operatorAt[i].Sub(operatorAt[i-1]); gap < m69RetryDelay {
					t.Fatalf("attempt %d followed attempt %d after %v; want at least %v", i+1, i, gap, m69RetryDelay)
				}
			}
			var backoffs, generation int
			for _, event := range m69Events(process) {
				if event.ObservedState == drivermanager.ObservedBackoff && event.Reason == drivermanager.ReasonRetryScheduled {
					backoffs++
				}
				if event.ObservedState == drivermanager.ObservedFailed {
					generation = int(event.Generation)
				}
			}
			if backoffs != eebusStartupMaxAttempts-1 || generation != eebusStartupMaxAttempts {
				t.Fatalf("service events: %d BACKOFF, FAILED at generation %d; want %d and %d", backoffs, generation, eebusStartupMaxAttempts-1, eebusStartupMaxAttempts)
			}

			// One sanitized line per failed start, naming the stage and both
			// categories; the native cause, the state root, the SKIs and the
			// addresses never reach the log.
			text := logs.String()
			wantLine := m69FailureLine + " stage=read_only_fallback reason=" + string(test.want) + " operator_reason=" + string(test.want)
			lines := m69FailureLines(text)
			if len(lines) != eebusStartupMaxAttempts {
				t.Fatalf("failure lines = %q; want %d", lines, eebusStartupMaxAttempts)
			}
			for _, line := range lines {
				if !strings.HasSuffix(line, wantLine) {
					t.Fatalf("failure line %q; want suffix %q", line, wantLine)
				}
			}
			if !strings.Contains(text, `lifecycle START failed for "eebus.primary": driver unavailable (PROVIDER_UNAVAILABLE)`) {
				t.Fatalf("runtime control line lacks the reason; log:\n%s", text)
			}
			for _, secret := range []string{cause, stateRoot, m69RemoteSKI, strings.Repeat("cd", 20), "192.0.2.4", "2001:db8", "permission denied", "address already in use", "refused the session"} {
				if strings.Contains(text, secret) {
					t.Fatalf("log contains %q:\n%s", secret, text)
				}
			}

			code, readiness := m69AdminReadiness(t, sidecars)
			if code != http.StatusServiceUnavailable || readiness.EEBusReadiness != eebusadmin.EEBusReadinessDegraded || readiness.EEBusDegradedReason != test.want {
				t.Fatalf("admin status = %d %+v; want 503 DEGRADED/%s", code, readiness, test.want)
			}
		})
	}
}

// A failure that clears within the window recovers: the second attempt
// reaches RUNNING after one failure line.
func TestM69AddonEEBusRetryRecoversWithinTheWindow(t *testing.T) {
	logs := m69CaptureLog(t)
	m69ShortRetry(t)
	failFirst := func(call int) eebusruntime.Runtime {
		if call == 1 {
			return &msp05bRuntime{startErr: errors.New(m69ListenerCause)}
		}
		return &msp05bRuntime{}
	}
	natives := m69Seams(t, failFirst, failFirst)
	process, sidecars, status := m69Start(t, m69AddonEEBusConfig(t, issue755SecureStateRoot(t)))
	if status.ObservedState != drivermanager.ObservedRunning {
		t.Fatalf("eebus.primary = %s/%s; want RUNNING after one retry", status.ObservedState, status.Reason)
	}
	constructions := natives.snapshot()
	if len(constructions) != 3 || !constructions[0].operator || constructions[1].operator || !constructions[2].operator {
		t.Fatalf("native constructions = %+v; want failed operator, failed fallback, working operator", constructions)
	}
	if lines := m69FailureLines(logs.String()); len(lines) != 1 {
		t.Fatalf("failure lines = %q; want exactly one", lines)
	}
	backoffs := 0
	for _, event := range m69Events(process) {
		if event.ObservedState == drivermanager.ObservedBackoff {
			backoffs++
		}
	}
	if backoffs != 1 {
		t.Fatalf("BACKOFF events = %d; want 1", backoffs)
	}
	if lifecycle := (&managedSidecarBindings{sidecars: sidecars}).eebusLifecycleSnapshot(); lifecycle.State != eebusLifecycleRunning || !lifecycle.AdminAvailable || lifecycle.DegradedReason != "" {
		t.Fatalf("eeBUS lifecycle = %+v; want running with the admin boundary", lifecycle)
	}
}

// The production policy is the earlier lifecycle's three attempts five
// seconds apart, and it is installed for eebus.primary only.
func TestM69EEBusRetryPolicyIsScopedToEEBus(t *testing.T) {
	if eebusManagedStartRetry != (runtimecontrol.RetryPolicy{MaxAttempts: 3, Delay: 5 * time.Second}) {
		t.Fatalf("eeBUS start retry = %+v; want 3 attempts 5s apart", eebusManagedStartRetry)
	}
	m06Factories(t, "", "")
	sidecars := prepareManagedSidecars(context.Background(), ebusgateway.DefaultConfig(), nil, "", nil, nil)
	_, service := sidecars.serviceConfigs()
	if len(service) != 3 {
		t.Fatalf("service configs = %d; want 3 sidecars", len(service))
	}
	for _, config := range service {
		want := runtimecontrol.RetryPolicy{}
		if config.DriverID == managedEEBusID {
			want = eebusManagedStartRetry
		}
		if config.Retry != want {
			t.Fatalf("%s retry = %+v; want %+v", config.DriverID, config.Retry, want)
		}
	}
}

// A failing sibling keeps its non-retryable policy: one attempt, now
// reported as PROVIDER_UNAVAILABLE instead of INTERNAL_ERROR.
func TestM69FailedSiblingIsNotRetriedAndReportsItsReason(t *testing.T) {
	natives := m06Factories(t, managedGrowattID, "")
	process := newRCtlEnv(t).start(rctlProcessOptions{})
	var status runtimecontrol.DriverStatus
	m06Wait(t, func() bool {
		status, _ = process.control.Service().Get(managedGrowattID)
		return status.ObservedState == drivermanager.ObservedFailed
	})
	if status.Reason != drivermanager.ReasonProviderUnavailable {
		t.Fatalf("failed Growatt = %+v; want PROVIDER_UNAVAILABLE", status)
	}
	if n := natives[managedGrowattID].starts.Load(); n != 1 {
		t.Fatalf("Growatt start attempts = %d; want 1 (no retry policy)", n)
	}
}

// After a failed START, only the service retry of the same transition (a
// later generation of the same driver, source, epoch and sequence) starts
// afresh, and a STOP of a later generation of that epoch covers the failed
// one. Each mutated field below is one the production check compares.
func TestM69FailedStartAdmitsOnlyItsRetrySuccessor(t *testing.T) {
	failed := runtimecontrol.LifecycleCorrelation{DriverID: managedEEBusID, SourceID: "synthetic-source", Epoch: "synthetic-epoch", Generation: 1, Sequence: 4}
	newEntry := func(t *testing.T) (*managedSidecar, *m06Runtime) {
		t.Helper()
		native := &m06Runtime{fail: true}
		runtime := &managedSidecarRuntime{Runtime: native}
		entry := &managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: managedEEBusID, Sources: []runtimecontrol.SourceID{"synthetic-source"}}, runtime: runtime, retry: eebusManagedStartRetry}
		manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: string(managedEEBusID), Enabled: true, Runtime: runtime, ClassifyError: entry.classifyStartFailure}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
		entry.manager = manager
		err = entry.Start(context.Background(), failed)
		var typed *managedSidecarStartError
		if !errors.As(err, &typed) || typed.ReasonCode() != drivermanager.ReasonProviderUnavailable || !typed.Retryable() || !errors.Is(err, drivermanager.ErrUnavailable) {
			t.Fatalf("failed START = %v; want a retryable PROVIDER_UNAVAILABLE that is still ErrUnavailable", err)
		}
		return entry, native
	}
	t.Run("repeated key returns the recorded failure", func(t *testing.T) {
		entry, native := newEntry(t)
		if err := entry.Start(context.Background(), failed); err == nil || native.starts.Load() != 1 {
			t.Fatalf("repeated START = %v with %d native starts; want the recorded failure, no second start", err, native.starts.Load())
		}
	})
	t.Run("retry successor starts afresh", func(t *testing.T) {
		entry, native := newEntry(t)
		native.fail = false
		successor := failed
		successor.Generation++
		if err := entry.Start(context.Background(), successor); err != nil || native.starts.Load() != 2 || !native.running.Load() {
			t.Fatalf("successor START = %v, starts=%d; want a fresh admitted start", err, native.starts.Load())
		}
		if err := entry.Start(context.Background(), failed); !errors.Is(err, errEBusControlStale) {
			t.Fatalf("late START of the failed generation = %v; want stale", err)
		}
	})
	// Every field of the key varies, each in an otherwise valid successor.
	// Epoch: dropping the epoch comparison in supersedesFailedLocked.
	// Sequence: dropping the sequence comparison in Start. SourceID and
	// DriverID: the correlation normalization ahead of that check (the
	// supersession check repeats both). Generation: the earlier-generation
	// case below drops the "later generation" comparison.
	for field, mutate := range map[string]func(*runtimecontrol.LifecycleCorrelation){
		"Epoch":    func(c *runtimecontrol.LifecycleCorrelation) { c.Generation++; c.Epoch = "other-epoch" },
		"Sequence": func(c *runtimecontrol.LifecycleCorrelation) { c.Generation++; c.Sequence++ },
		"SourceID": func(c *runtimecontrol.LifecycleCorrelation) { c.Generation++; c.SourceID = "other-source" },
		"DriverID": func(c *runtimecontrol.LifecycleCorrelation) { c.Generation++; c.DriverID = managedSunSpecID },
	} {
		t.Run("refuses changed "+field, func(t *testing.T) {
			entry, native := newEntry(t)
			native.fail = false
			changed := failed
			mutate(&changed)
			if err := entry.Start(context.Background(), changed); err == nil || native.starts.Load() != 1 {
				t.Fatalf("START with changed %s = %v, starts=%d; want refused without a native start", field, err, native.starts.Load())
			}
		})
	}
	// A failed START at generation 2 refuses generation 1 of its epoch.
	t.Run("refuses an earlier generation", func(t *testing.T) {
		entry, native := newEntry(t)
		native.fail = false
		entry.mu.Lock()
		entry.key.Generation = 2
		entry.mu.Unlock()
		if err := entry.Start(context.Background(), failed); !errors.Is(err, errEBusControlStale) || native.starts.Load() != 1 {
			t.Fatalf("earlier generation START = %v; want stale", err)
		}
	})
	t.Run("stop of a later generation covers the failed start", func(t *testing.T) {
		entry, native := newEntry(t)
		stop := failed
		stop.Generation, stop.Sequence = 2, 9
		if outcome := entry.Stop(context.Background(), stop); !outcome.NativeClosed || !outcome.SemanticCleaned {
			t.Fatalf("STOP of the later generation = %+v; want proven", outcome)
		}
		for _, key := range []runtimecontrol.LifecycleCorrelation{failed, stop} {
			if err := entry.Start(context.Background(), key); !errors.Is(err, errEBusControlStale) || native.starts.Load() != 1 {
				t.Fatalf("START after STOP of %+v = %v; want stale", key, err)
			}
		}
	})
	t.Run("stop of another epoch is refused", func(t *testing.T) {
		entry, _ := newEntry(t)
		stop := failed
		stop.Generation, stop.Epoch = 2, "other-epoch"
		if outcome := entry.Stop(context.Background(), stop); outcome.NativeClosed || outcome.SemanticCleaned {
			t.Fatalf("STOP of another epoch = %+v; want refused", outcome)
		}
	})
}
