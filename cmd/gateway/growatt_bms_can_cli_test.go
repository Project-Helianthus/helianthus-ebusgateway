package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

func can03Parse(t *testing.T, args ...string) ebusgateway.Config {
	t.Helper()
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestCAN03FlagsDefaultsParsingAndHelp(t *testing.T) {
	if cfg := can03Parse(t); cfg.GrowattBMSCANConfig != (ebusgateway.GrowattBMSCANConfig{}) {
		t.Fatalf("default CAN config = %+v", cfg.GrowattBMSCANConfig)
	}
	cfg := can03Parse(t, "-growatt-bms-can-enabled", "-growatt-bms-can-interface=can7", "-growatt-bms-can-listen-only-confirmed", "-growatt-bms-can-stale-after=1750ms")
	want := ebusgateway.GrowattBMSCANConfig{Enabled: true, Interface: "can7", ListenOnlyConfirmed: true, StaleAfter: 1750 * time.Millisecond}
	if cfg.GrowattBMSCANConfig != want {
		t.Fatalf("CAN config = %+v; want %+v", cfg.GrowattBMSCANConfig, want)
	}
	// A second independent parse must neither inherit nor overwrite the first.
	other := can03Parse(t, "-growatt-bms-can-enabled=false", "-growatt-bms-can-interface=can8", "-growatt-bms-can-listen-only-confirmed=false", "-growatt-bms-can-stale-after=3s")
	if other.GrowattBMSCANConfig != (ebusgateway.GrowattBMSCANConfig{Interface: "can8", StaleAfter: 3 * time.Second}) || cfg.GrowattBMSCANConfig != want {
		t.Fatal("flag sets did not retain independent typed configurations")
	}
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	var help bytes.Buffer
	fs.SetOutput(&help)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"--help"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help = %v", err)
	}
	for _, name := range []string{"growatt-bms-can-enabled", "growatt-bms-can-interface", "growatt-bms-can-listen-only-confirmed", "growatt-bms-can-stale-after"} {
		if !strings.Contains(help.String(), "-"+name) {
			t.Errorf("help lacks %s", name)
		}
	}
	for _, value := range []string{"invalid", "1"} {
		fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
		bindFlags(fs, &cfg)
		if err := fs.Parse([]string{"-growatt-bms-can-stale-after=" + value}); err == nil {
			t.Fatalf("accepted invalid duration %q", value)
		}
	}
}

func TestCAN03ManagedConfigurationRefusesBeforeOpen(t *testing.T) {
	valid := growattBMSCANConfig{Enabled: true, Interface: "can7", ListenOnlyConfirmed: true, StaleAfter: time.Second}
	for _, tc := range []struct {
		name   string
		mutate func(*growattBMSCANConfig)
		want   error
	}{
		{"listen-only", func(c *growattBMSCANConfig) { c.ListenOnlyConfirmed = false }, errGrowattBMSCANListenOnly},
		{"interface", func(c *growattBMSCANConfig) { c.Interface = "" }, errGrowattBMSCANConfig},
		{"zero stale", func(c *growattBMSCANConfig) { c.StaleAfter = 0 }, errGrowattBMSCANConfig},
		{"negative stale", func(c *growattBMSCANConfig) { c.StaleAfter = -time.Second }, errGrowattBMSCANConfig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			adapter := newGrowattBMSCANManagedFactory(cfg, func(string) (growattBMSCANReceiver, error) {
				t.Fatal("invalid config opened receiver")
				return nil, nil
			})
			native, err := adapter.Construct(context.Background(), runtimecontrol.Construction{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = native.Start(context.Background()); !errors.Is(err, tc.want) {
				t.Fatalf("start = %v; want %v", err, tc.want)
			}
			if failure := (&managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: managedGrowattCANID}}).classifyStartFailure(err); failure.Reason.Code != drivermanager.ReasonConfigInvalid {
				t.Fatalf("classification = %+v", failure)
			}
		})
	}
}

func can03FakeOpener(t *testing.T, failAttempts int32) *atomic.Int32 {
	t.Helper()
	original := gatewayManagedSidecarDescriptorsFor
	var mu sync.Mutex
	var receivers []*growattBMSCANFakeReceiver
	t.Cleanup(func() {
		gatewayManagedSidecarDescriptorsFor = original
		mu.Lock()
		defer mu.Unlock()
		for _, receiver := range receivers {
			if receiver.closeCount() != 1 {
				t.Errorf("receiver closes=%d; want 1", receiver.closeCount())
			}
		}
	})
	var opens atomic.Int32
	opener := func(name string) (growattBMSCANReceiver, error) {
		if name != "can7" {
			t.Errorf("open name = %q", name)
		}
		if opens.Add(1) <= failAttempts {
			return nil, errors.New("synthetic receiver unavailable")
		}
		receiver := newGrowattBMSCANFakeReceiver(growattBMSCANTestInterface)
		mu.Lock()
		receivers = append(receivers, receiver)
		mu.Unlock()
		return receiver, nil
	}
	gatewayManagedSidecarDescriptorsFor = func(descriptors []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
		for i := range descriptors {
			if descriptors[i].DriverID == managedGrowattCANID {
				adapter := descriptors[i].Adapter
				descriptors[i].Adapter = runtimecontrol.NewAdapter(func(ctx context.Context, c runtimecontrol.Construction) (*growattBMSCANManagedDriver, error) {
					native, err := adapter.Construct(ctx, c)
					if err != nil {
						return nil, err
					}
					driver, ok := native.(*growattBMSCANManagedDriver)
					if !ok {
						return nil, errors.New("CAN descriptor lost managed driver")
					}
					want := growattBMSCANConfig{Enabled: true, Interface: "can7", ListenOnlyConfirmed: true, StaleAfter: 2 * time.Second}
					if driver.config.ListenOnlyConfirmed && driver.config != want {
						t.Errorf("composed CAN config = %+v; want %+v", driver.config, want)
					}
					driver.open = opener
					return driver, nil
				})
			}
		}
		return descriptors
	}

	return &opens
}

func TestCAN03DisabledCompositionIsInert(t *testing.T) {
	logs := captureIssue1367Logs(t)
	cfg := can03Parse(t)
	sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), rctlGUID, nil, nil)
	for _, entry := range sidecars.entries {
		if entry.descriptor.DriverID == managedGrowattCANID {
			if entry.descriptor.Disposition != runtimecontrol.DispositionDisabled || entry.runtime != nil || entry.withheld != nil {
				t.Fatal("default disabled CAN constructed or refused")
			}
			if strings.Contains(logs.text(), "Growatt BMS CAN configuration refused") {
				t.Fatal("default disabled CAN emitted a configuration refusal")
			}
			return
		}
	}
	t.Fatal("CAN descriptor absent")
}

func TestCAN03DisabledActiveFieldsAreRefusedBeforeConstruction(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"interface", []string{"-growatt-bms-can-interface=can7"}},
		{"listen-only", []string{"-growatt-bms-can-listen-only-confirmed"}},
		{"stale-after", []string{"-growatt-bms-can-stale-after=2s"}},
		{"negative stale-after", []string{"-growatt-bms-can-stale-after=-1s"}},
		{"all active fields", []string{"-growatt-bms-can-enabled=false", "-growatt-bms-can-interface=can7", "-growatt-bms-can-listen-only-confirmed", "-growatt-bms-can-stale-after=2s"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureIssue1367Logs(t)
			original := gatewayManagedSidecarDescriptorsFor
			t.Cleanup(func() { gatewayManagedSidecarDescriptorsFor = original })
			gatewayManagedSidecarDescriptorsFor = func(descriptors []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
				for i := range descriptors {
					if descriptors[i].DriverID == managedGrowattCANID {
						descriptors[i].Adapter = runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*growattBMSCANManagedDriver, error) {
							t.Fatal("disabled active CAN configuration constructed a native driver")
							return nil, nil
						})
					}
				}
				return descriptors
			}
			cfg := can03Parse(t, tc.args...)
			sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), rctlGUID, nil, nil)
			var can *managedSidecar
			for _, entry := range sidecars.entries {
				if entry.descriptor.DriverID == managedGrowattCANID {
					can = entry
				}
			}
			if can == nil {
				t.Fatal("CAN descriptor absent")
			}
			if can.descriptor.Disposition != runtimecontrol.DispositionDisabled || can.runtime != nil {
				t.Fatal("invalid disabled configuration changed host eligibility or constructed a runtime")
			}
			if !errors.Is(can.withheld, errGrowattBMSCANDisabledConfig) {
				t.Errorf("withheld = %v; want disabled active-fields refusal", can.withheld)
			}
			if err := can.Start(context.Background(), runtimecontrol.LifecycleCorrelation{}); !errors.Is(err, errGrowattBMSCANDisabledConfig) {
				t.Errorf("start = %v; want disabled active-fields refusal", err)
			}
			if !strings.Contains(logs.text(), "Growatt BMS CAN configuration refused: disabled observer contains active fields reason=CONFIG_INVALID") {
				t.Error("startup did not deliver the categorical configuration-refusal diagnostic")
			}
			if strings.Contains(logs.text(), "can7") {
				t.Error("configuration-refusal diagnostic leaked the configured interface")
			}
		})
	}
}

func TestCAN03FixedOwnershipPrecedesConfigurableRS485(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled RS485"
		if enabled {
			name = "enabled RS485"
		}
		t.Run(name, func(t *testing.T) {
			opens := can03FakeOpener(t, 0)
			factory := gatewayManagedSidecarDescriptorsFor
			gatewayManagedSidecarDescriptorsFor = func(descriptors []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
				descriptors = factory(descriptors)
				for i := range descriptors {
					if descriptors[i].DriverID == managedGrowattID {
						descriptors[i].Adapter = runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*m06Runtime, error) {
							t.Error("colliding RS485 configuration constructed a runtime")
							return nil, errors.New("colliding RS485 must be withheld")
						})
					}
				}
				return descriptors
			}
			var sidecars *managedSidecars
			process := newRCtlEnv(t).start(rctlProcessOptions{
				config: func(cfg *ebusgateway.Config) {
					cfg.GrowattBMSCANConfig = growattBMSCANConfig{Enabled: true, Interface: "can7", ListenOnlyConfirmed: true, StaleAfter: 2 * time.Second}
					cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled = enabled
					cfg.ModbusTCPConfig.GrowattBMSRS485.SourceID = string(managedGrowattCANID) + ":" + rctlGUID
				},
				mutate: func(opts *gatewayRuntimeControlOptions) { sidecars = opts.Sidecars },
			})
			defer process.stop()
			bindings := &managedSidecarBindings{sidecars: sidecars}
			if err := bindings.entry(managedGrowattCANID).withheld; err != nil {
				t.Fatalf("fixed CAN ownership refused: %v", err)
			}
			if err := bindings.entry(managedGrowattID).withheld; !errors.Is(err, runtimecontrol.ErrDuplicateSourceID) {
				t.Fatalf("RS485 refusal = %v; want duplicate source ownership", err)
			}
			owners := 0
			for _, entry := range sidecars.registry.List().Drivers {
				for _, source := range entry.Sources {
					if source == runtimecontrol.SourceID(string(managedGrowattCANID)+":"+rctlGUID) {
						owners++
						if entry.DriverID != managedGrowattCANID {
							t.Fatalf("fixed CAN source owned by %s", entry.DriverID)
						}
					}
				}
			}
			if owners != 1 {
				t.Fatalf("CAN source owners = %d; want exactly one", owners)
			}
			m06Wait(t, func() bool {
				status, err := process.control.Service().Get(managedGrowattCANID)
				return err == nil && status.ObservedState == drivermanager.ObservedRunning
			})
			if opens.Load() != 1 {
				t.Fatalf("CAN opens = %d; want one", opens.Load())
			}
			want := drivermanager.ObservedDisabled
			if enabled {
				want = drivermanager.ObservedFailed
			}
			m06Wait(t, func() bool {
				status, err := process.control.Service().Get(managedGrowattID)
				return err == nil && status.Enabled == enabled && status.ObservedState == want
			})
		})
	}
}

func TestCAN03StartupCompositionRetriesAndLifecycle(t *testing.T) {
	// Inject the only descriptor opener. No physical interface is inspected.
	opens := can03FakeOpener(t, 1)
	cfg := can03Parse(t, "-growatt-bms-can-enabled", "-growatt-bms-can-interface=can7", "-growatt-bms-can-listen-only-confirmed", "-growatt-bms-can-stale-after=2s")
	env := newRCtlEnv(t)
	process := env.start(rctlProcessOptions{
		config: func(c *ebusgateway.Config) { c.GrowattBMSCANConfig = cfg.GrowattBMSCANConfig },
		mutate: func(opts *gatewayRuntimeControlOptions) {
			for _, entry := range opts.Sidecars.entries {
				if entry.descriptor.DriverID == managedGrowattCANID {
					if entry.retry.MaxAttempts != 3 || entry.retry.Delay != 5*time.Second {
						t.Errorf("CAN retry policy = %+v", entry.retry)
					}
					entry.retry.Delay = time.Millisecond
				}
			}
		},
	})
	service := process.control.Service()
	wait := func(state drivermanager.ObservedState) runtimecontrol.DriverStatus {
		t.Helper()
		var status runtimecontrol.DriverStatus
		m06Wait(t, func() bool {
			var err error
			status, err = service.Get(managedGrowattCANID)
			return err == nil && status.ObservedState == state
		})
		return status
	}
	before := wait(drivermanager.ObservedRunning)
	if opens.Load() != 2 {
		t.Fatalf("startup opens=%d; want failed attempt plus successful retry", opens.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
	defer cancel()
	if _, err := service.Decide(ctx, runtimecontrol.MethodRestart, runtimecontrol.Request{DriverID: managedGrowattCANID, ExpectedRevision: before.DesiredRevision}); err != nil {
		t.Fatal(err)
	}
	m06Wait(t, func() bool {
		status, err := service.Get(managedGrowattCANID)
		return err == nil && status.ObservedState == drivermanager.ObservedRunning && opens.Load() == 3
	})
	running := wait(drivermanager.ObservedRunning)
	// RUNNING is observable before the preceding restart's terminal slot is
	// pruned. Wait only for that explicit transient, with the same revision.
	m06Wait(t, func() bool {
		_, err := service.Decide(ctx, runtimecontrol.MethodStop, runtimecontrol.Request{DriverID: managedGrowattCANID, ExpectedRevision: running.DesiredRevision})
		var refusal *runtimecontrol.Refusal
		if errors.As(err, &refusal) && refusal.Code == runtimecontrol.RefusalTransitionBusy && refusal.Row == 8 && refusal.Reason == "terminal work slot is being pruned" {
			return false
		}
		if err != nil {
			t.Fatal(err)
		}
		return true
	})
	wait(drivermanager.ObservedStopped)
	process.stop()
}

func TestCAN03StartupFailurePolicy(t *testing.T) {
	for _, tc := range []struct {
		name         string
		confirmed    bool
		failAttempts int32
		wantOpens    int32
		wantReason   drivermanager.ReasonCode
	}{
		{"listen-only refusal", false, 0, 0, drivermanager.ReasonConfigInvalid},
		{"receiver retry exhaustion", true, 100, 3, drivermanager.ReasonProviderUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opens := can03FakeOpener(t, tc.failAttempts)
			cfg := can03Parse(t, "-growatt-bms-can-enabled", "-growatt-bms-can-interface=can7", "-growatt-bms-can-stale-after=2s")
			cfg.GrowattBMSCANConfig.ListenOnlyConfirmed = tc.confirmed
			process := newRCtlEnv(t).start(rctlProcessOptions{
				config: func(c *ebusgateway.Config) { c.GrowattBMSCANConfig = cfg.GrowattBMSCANConfig },
				mutate: func(opts *gatewayRuntimeControlOptions) {
					for _, entry := range opts.Sidecars.entries {
						if entry.descriptor.DriverID == managedGrowattCANID {
							entry.retry.Delay = time.Millisecond
						}
					}
				},
			})
			m06Wait(t, func() bool {
				status, err := process.control.Service().Get(managedGrowattCANID)
				return err == nil && status.ObservedState == drivermanager.ObservedFailed && opens.Load() == tc.wantOpens
			})
			status, err := process.control.Service().Get(managedGrowattCANID)
			if err != nil || status.Reason != tc.wantReason {
				t.Fatalf("failure status = %+v; error = %v", status, err)
			}
			backoffs := 0
			for _, event := range process.control.Service().EventsSince(0).Events {
				if event.DriverID == managedGrowattCANID && event.Kind == runtimecontrol.EventObservedChanged && event.ObservedState == drivermanager.ObservedBackoff {
					backoffs++
				}
			}
			wantBackoffs := 0
			if tc.confirmed {
				wantBackoffs = 2
			}
			if backoffs != wantBackoffs {
				t.Fatalf("backoffs=%d; want %d", backoffs, wantBackoffs)
			}
			process.stop()
		})
	}
}

type can03CloseFailureReceiver struct {
	*growattBMSCANFakeReceiver
	err error
}

func (receiver can03CloseFailureReceiver) Close() error {
	return errors.Join(receiver.growattBMSCANFakeReceiver.Close(), receiver.err)
}

func TestCAN03FailedCleanupQuarantinesReceiver(t *testing.T) {
	for _, stage := range []string{"identity", "canceled start", "stop"} {
		t.Run(stage, func(t *testing.T) {
			identity := growattBMSCANTestInterface
			if stage == "identity" {
				identity.Index = 0
			}
			receiver := newGrowattBMSCANFakeReceiver(identity)
			closeErr := errors.New("synthetic close unconfirmed")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opens := 0
			driver := &growattBMSCANManagedDriver{
				config: growattBMSCANEnabledConfig(),
				open: func(string) (growattBMSCANReceiver, error) {
					opens++
					if stage == "canceled start" {
						cancel()
					}
					return can03CloseFailureReceiver{receiver, closeErr}, nil
				},
			}
			_, err := driver.Start(ctx)
			if stage == "stop" {
				if err != nil {
					t.Fatal(err)
				}
				err = driver.Stop(context.Background())
			}
			if !errors.Is(err, closeErr) || !driver.SafetyQuarantined() {
				t.Fatalf("cleanup error=%v quarantined=%v; want close failure quarantined", err, driver.SafetyQuarantined())
			}
			if _, err := driver.Start(context.Background()); !errors.Is(err, closeErr) {
				t.Fatalf("restart after failed close = %v", err)
			}
			if opens != 1 || receiver.closeCount() != 1 {
				t.Fatalf("opens=%d closes=%d; want one attempt and one close", opens, receiver.closeCount())
			}
		})
	}
}

func TestCAN03ReceiveFailureReachesLifecycleAndRestart(t *testing.T) {
	for _, early := range []bool{false, true} {
		name := "after admission"
		if early {
			name = "during startup"
		}
		t.Run(name, func(t *testing.T) {
			original := gatewayManagedSidecarDescriptorsFor
			t.Cleanup(func() { gatewayManagedSidecarDescriptorsFor = original })
			constructed := make(chan *growattBMSCANManagedDriver, 1)
			opened := make(chan *growattBMSCANFakeReceiver, 2)
			openCount := 0
			gatewayManagedSidecarDescriptorsFor = func(descriptors []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
				for i := range descriptors {
					if descriptors[i].DriverID != managedGrowattCANID {
						continue
					}
					adapter := descriptors[i].Adapter
					descriptors[i].Adapter = runtimecontrol.NewAdapter(func(ctx context.Context, c runtimecontrol.Construction) (*growattBMSCANManagedDriver, error) {
						native, err := adapter.Construct(ctx, c)
						if err != nil {
							return nil, err
						}
						driver := native.(*growattBMSCANManagedDriver)
						driver.open = func(string) (growattBMSCANReceiver, error) {
							receiver := newGrowattBMSCANFakeReceiver(growattBMSCANTestInterface)
							opened <- receiver
							openCount++
							if early && openCount == 1 {
								receiver.failures = make(chan error, 1)
								receiver.failures <- errors.New("synthetic immediate receive failure")
							}
							return receiver, nil
						}
						constructed <- driver
						return driver, nil
					})
				}
				return descriptors
			}
			process := newRCtlEnv(t).start(rctlProcessOptions{
				config: func(c *ebusgateway.Config) { c.GrowattBMSCANConfig = growattBMSCANEnabledConfig() },
			})
			driver := <-constructed
			service := process.control.Service()
			wait := func(state drivermanager.ObservedState) runtimecontrol.DriverStatus {
				t.Helper()
				var status runtimecontrol.DriverStatus
				m06Wait(t, func() bool {
					var err error
					status, err = service.Get(managedGrowattCANID)
					return err == nil && status.ObservedState == state
				})
				return status
			}
			if !early {
				wait(drivermanager.ObservedRunning)
			}
			first := <-opened
			driver.mu.Lock()
			oldNative := drivermanager.Correlation{Generation: driver.generation}
			oldLifecycle := driver.lifecycle
			driver.mu.Unlock()
			if !early {
				first.failures <- errors.New("synthetic receive stopped")
			}
			failed := wait(drivermanager.ObservedFailed)
			if failed.Reason != drivermanager.ReasonProviderUnavailable || failed.SafetyQuarantined {
				t.Fatalf("receive failure status = %+v", failed)
			}
			snapshot, ok := process.control.driver.manager.Snapshot(string(managedGrowattCANID))
			if !ok || snapshot.ObservedState != drivermanager.ObservedFailed || len(snapshot.EffectiveCapabilities) != 0 {
				t.Fatalf("manager after receive failure = %+v", snapshot)
			}
			ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
			defer cancel()
			if _, err := service.Decide(ctx, runtimecontrol.MethodRestart, runtimecontrol.Request{DriverID: managedGrowattCANID, ExpectedRevision: failed.DesiredRevision}); err != nil {
				t.Fatal(err)
			}
			wait(drivermanager.ObservedRunning)
			second := <-opened
			if first.closeCount() != 1 {
				t.Fatalf("failed receiver closes=%d; want retired before restart", first.closeCount())
			}
			// A delayed callback from the failed receiver cannot withdraw its successor.
			driver.reportFailure(oldNative, oldLifecycle)
			snapshot, _ = process.control.driver.manager.Snapshot(string(managedGrowattCANID))
			if snapshot.ObservedState != drivermanager.ObservedRunning || snapshot.Generation == oldNative.Generation {
				t.Fatalf("stale receive failure changed successor = %+v", snapshot)
			}
			if status, err := service.Get(managedGrowattCANID); err != nil || status.ObservedState != drivermanager.ObservedRunning {
				t.Fatalf("service after stale failure = %+v, %v", status, err)
			}
			process.stop()
			if second.closeCount() != 1 {
				t.Fatalf("successor closes=%d; want joined shutdown", second.closeCount())
			}
		})
	}
}

func TestCAN03ReceiveFailureBeforeAdmissionIsReported(t *testing.T) {
	receiver := newGrowattBMSCANFakeReceiver(growattBMSCANTestInterface)
	driver := &growattBMSCANManagedDriver{
		config: growattBMSCANEnabledConfig(),
		open:   func(string) (growattBMSCANReceiver, error) { return receiver, nil },
	}
	lifecycle := runtimecontrol.LifecycleCorrelation{DriverID: managedGrowattCANID, SourceID: "synthetic-source", Epoch: "synthetic-epoch", Generation: 1}
	if err := driver.BindLifecycle(lifecycle); err != nil {
		t.Fatal(err)
	}
	generation, err := driver.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Stop(context.Background()) })
	receiver.failures <- errors.New("synthetic early receive failure")
	<-driver.driver.done
	reported := make(chan struct{}, 1)
	driver.reportFailure = func(native drivermanager.Correlation, got runtimecontrol.LifecycleCorrelation) {
		if native.Generation != generation || got != lifecycle {
			t.Errorf("failure correlations = %+v, %+v", native, got)
		}
		reported <- struct{}{}
	}
	driver.BindCorrelation(drivermanager.Correlation{Generation: generation})
	select {
	case <-reported:
	case <-time.After(time.Second):
		t.Fatal("receive failure before admission was lost")
	}
}
