package main

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

func m05Options(t *testing.T, factory eebusRuntimeFactory) eebusManagedDriverOptions {
	t.Helper()
	cfg := msp05bEnabledConfig()
	cfg.StateRoot = t.TempDir()
	prefix := netip.MustParsePrefix(cfg.Subnets[0])
	return eebusManagedDriverOptions{Config: cfg, Factory: factory, Resolve: func(string) ([]netip.Addr, error) {
		return []netip.Addr{prefix.Addr().Next()}, nil
	}}
}

func TestM05DescriptorLifecycleAndNativeConfig(t *testing.T) {
	var runtimes []*msp05bRuntime
	var configs []eebusruntime.Config
	options := m05Options(t, func(cfg eebusruntime.Config) (eebusruntime.Runtime, error) {
		configs = append(configs, cfg)
		native := &msp05bRuntime{}
		runtimes = append(runtimes, native)
		return native, nil
	})
	options.Config.RemoteSKIAllowlist = []string{strings.Repeat("ab", 20)}
	expected, err := mapEEBusRuntimeConfig(options.Config, options.Resolve)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := newEEBusManagedDriverDescriptor("eebus.fixture", []runtimecontrol.SourceID{"eebus-source"}, options)
	if descriptor.Direction != runtimecontrol.DirectionSouthbound || descriptor.Disposition != runtimecontrol.DispositionEnabled || len(descriptor.Hooks) != 0 {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	// Mutating caller-owned slices must not change the later native configuration.
	options.Config.Interfaces[0] = "changed"
	options.Config.Subnets[0] = "invalid"
	options.Config.RemoteSKIAllowlist[0] = "invalid"
	runtime, err := descriptor.Adapter.Construct(context.Background(), runtimecontrol.Construction{})
	if err != nil {
		t.Fatal(err)
	}
	driver := runtime.(*eebusManagedDriver)
	if len(runtimes) != 0 {
		t.Fatal("descriptor constructed native runtime eagerly")
	}
	first, err := driver.Start(context.Background())
	if err != nil || first != 1 {
		t.Fatalf("start = %d, %v", first, err)
	}
	if same, err := driver.Start(context.Background()); err != nil || same != first {
		t.Fatalf("duplicate start = %d, %v", same, err)
	}
	second, err := driver.Replace(context.Background())
	if err != nil || second != 2 {
		t.Fatalf("replace = %d, %v", second, err)
	}
	called := false
	if err := driver.WithGeneration(first, func(*eebusRuntimeAdapter, eebusruntime.AdminV1) error { called = true; return nil }); !errors.Is(err, drivermanager.ErrUnavailable) || called {
		t.Fatal("old generation callback admitted")
	}
	if err := driver.WithGeneration(second, func(*eebusRuntimeAdapter, eebusruntime.AdminV1) error { called = true; return nil }); err != nil || !called {
		t.Fatal("current callback rejected")
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := driver.WithGeneration(second, func(*eebusRuntimeAdapter, eebusruntime.AdminV1) error { t.Fatal("stopped callback ran"); return nil }); !errors.Is(err, drivermanager.ErrUnavailable) {
		t.Fatal(err)
	}
	if third, err := driver.Start(context.Background()); err != nil || third != 3 {
		t.Fatalf("restart after stop = %d, %v", third, err)
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, native := range runtimes {
		if native.startCalls != 1 || native.stopCalls != 1 {
			t.Fatalf("native calls = %#v", native)
		}
	}
	for _, cfg := range configs {
		if !reflect.DeepEqual(cfg, expected) {
			t.Fatal("native protected-store/pairing/trust configuration changed")
		}
	}
	if driver.Generation() != 3 || driver.Revision() != 6 || driver.SafetyQuarantined() {
		t.Fatal("incorrect lifecycle counters/state")
	}
}

func TestM05PartialFailureClosesExactlyOnce(t *testing.T) {
	for _, stage := range []string{"factory", "start", "operator-factory", "operator-start", "operator-incomplete"} {
		t.Run(stage, func(t *testing.T) {
			failure := errors.New("synthetic failure")
			native := &msp05bRuntime{}
			options := m05Options(t, nil)
			var factoryErr error
			if stage == "factory" || stage == "operator-factory" {
				factoryErr = failure
			}
			if stage == "start" || stage == "operator-start" {
				native.startErr = failure
			}
			options.Factory = func(eebusruntime.Config) (eebusruntime.Runtime, error) { return native, factoryErr }
			if stage == "operator-factory" || stage == "operator-start" || stage == "operator-incomplete" {
				options.OperatorFactory = func(eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
					if stage == "operator-incomplete" {
						return native, nil, nil
					}
					return native, issue809AdminStub{}, factoryErr
				}
			}
			driver := newEEBusManagedDriver(options)
			if _, err := driver.Start(context.Background()); err == nil {
				t.Fatal("partial failure admitted")
			}
			if err := driver.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			if native.stopCalls != 1 || driver.Generation() != 0 || driver.Revision() != 0 {
				t.Fatalf("close=%d generation=%d", native.stopCalls, driver.Generation())
			}
		})
	}
}

func TestM05FailedStartupCloseRetainsQuarantine(t *testing.T) {
	for _, stage := range []string{"factory", "start", "operator-factory", "operator-start", "operator-incomplete", "late-start", "operator-late-start"} {
		t.Run(stage, func(t *testing.T) {
			startupErr := errors.New("synthetic startup failure")
			closeErr := errors.New("synthetic close failure")
			native := &msp05bRuntime{shutdownErr: closeErr}
			var runtime eebusruntime.Runtime = native
			late := &m05LateStartRuntime{entered: make(chan struct{}), release: make(chan struct{})}
			if strings.Contains(stage, "late-start") {
				late.shutdownErr = closeErr
				native = &late.msp05bRuntime
				runtime = late
			}
			var factoryErr error
			if stage == "factory" || stage == "operator-factory" {
				factoryErr = startupErr
			}
			if stage == "start" || stage == "operator-start" {
				native.startErr = startupErr
			}
			calls := 0
			options := m05Options(t, func(eebusruntime.Config) (eebusruntime.Runtime, error) {
				calls++
				return runtime, factoryErr
			})
			if strings.HasPrefix(stage, "operator-") {
				options.OperatorFactory = func(eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
					calls++
					if stage == "operator-incomplete" {
						return runtime, nil, nil
					}
					return runtime, issue809AdminStub{}, factoryErr
				}
			}
			driver := newEEBusManagedDriver(options)
			var err error
			if strings.Contains(stage, "late-start") {
				done := make(chan error, 1)
				go func() { _, err := driver.Start(context.Background()); done <- err }()
				<-late.entered
				driver.FenceWithdrawal()
				close(late.release)
				err = <-done
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("late start = %v", err)
				}
			} else {
				_, err = driver.Start(context.Background())
				if stage != "operator-incomplete" && !errors.Is(err, startupErr) {
					t.Fatalf("start lost original failure: %v", err)
				}
			}
			if !errors.Is(err, closeErr) || !driver.SafetyQuarantined() {
				t.Fatalf("start = %v, quarantined = %v", err, driver.SafetyQuarantined())
			}
			for attempt := 0; attempt < 2; attempt++ {
				if err := driver.Stop(context.Background()); !errors.Is(err, closeErr) {
					t.Fatalf("stop = %v", err)
				}
				if _, err := driver.Start(context.Background()); !errors.Is(err, drivermanager.ErrUnavailable) {
					t.Fatalf("restart = %v", err)
				}
				if _, err := driver.Replace(context.Background()); !errors.Is(err, closeErr) {
					t.Fatalf("replace = %v", err)
				}
			}
			if err := driver.WithGeneration(0, func(*eebusRuntimeAdapter, eebusruntime.AdminV1) error {
				t.Fatal("failed startup callback admitted")
				return nil
			}); !errors.Is(err, drivermanager.ErrUnavailable) {
				t.Fatal(err)
			}
			if calls != 1 || native.stopCalls != 1 || driver.Generation() != 0 || driver.Revision() != 0 {
				t.Fatalf("factory=%d close=%d generation=%d revision=%d", calls, native.stopCalls, driver.Generation(), driver.Revision())
			}
		})
	}
}

func TestM05StopDrainsCallbacksAndHonorsDeadline(t *testing.T) {
	native := &msp05bRuntime{}
	driver := newEEBusManagedDriver(m05Options(t, func(eebusruntime.Config) (eebusruntime.Runtime, error) { return native, nil }))
	generation, err := driver.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var published atomic.Int32
	go func() {
		done <- driver.WithGeneration(generation, func(*eebusRuntimeAdapter, eebusruntime.AdminV1) error {
			close(entered)
			<-release
			published.Add(1)
			return nil
		})
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := driver.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop = %v", err)
	}
	if err := driver.WithGeneration(generation, func(*eebusRuntimeAdapter, eebusruntime.AdminV1) error { t.Fatal("fenced callback ran"); return nil }); !errors.Is(err, drivermanager.ErrUnavailable) {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if native.stopCalls != 1 || published.Load() != 1 {
		t.Fatal("callback drain or exact close failed")
	}
}

func TestM05NativeSafeguardsFailBeforeFactory(t *testing.T) {
	for _, mutate := range []func(*ebusgateway.EEBusConfig){
		func(c *ebusgateway.EEBusConfig) { c.PairingWindowMode = "open" },
		func(c *ebusgateway.EEBusConfig) { c.CertificatePath = "legacy" },
		func(c *ebusgateway.EEBusConfig) { c.PrivateKeyPath = "legacy" },
		func(c *ebusgateway.EEBusConfig) { c.TrustStorePath = "legacy" },
		func(c *ebusgateway.EEBusConfig) { c.RemoteSKIAllowlist = []string{"invalid"} },
	} {
		options := m05Options(t, func(eebusruntime.Config) (eebusruntime.Runtime, error) {
			t.Fatal("native safeguard bypassed")
			return nil, nil
		})
		mutate(&options.Config)
		if _, err := newEEBusManagedDriver(options).Start(context.Background()); err == nil {
			t.Fatal("unsafe configuration accepted")
		}
	}
}

type m05LateStartRuntime struct {
	msp05bRuntime
	entered chan struct{}
	release chan struct{}
}

func (runtime *m05LateStartRuntime) Start(context.Context) error {
	runtime.startCalls++
	close(runtime.entered)
	<-runtime.release
	return nil
}

func TestM05StopCancelsLateNativeStart(t *testing.T) {
	native := &m05LateStartRuntime{entered: make(chan struct{}), release: make(chan struct{})}
	driver := newEEBusManagedDriver(m05Options(t, func(eebusruntime.Config) (eebusruntime.Runtime, error) { return native, nil }))
	done := make(chan error, 1)
	go func() { _, err := driver.Start(context.Background()); done <- err }()
	<-native.entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := driver.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop = %v", err)
	}
	close(native.release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("late start = %v", err)
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.Generation() != 0 || native.stopCalls != 1 {
		t.Fatal("late start admitted or leaked")
	}
	// A canceled lock acquisition must not retain the operation token.
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := driver.Start(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestM05CloseFailureBlocksReplacement(t *testing.T) {
	failure := errors.New("synthetic close failure")
	native := &msp05bRuntime{shutdownErr: failure}
	calls := 0
	driver := newEEBusManagedDriver(m05Options(t, func(eebusruntime.Config) (eebusruntime.Runtime, error) { calls++; return native, nil }))
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Replace(context.Background()); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := driver.Stop(context.Background()); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if _, err := driver.Start(context.Background()); !errors.Is(err, drivermanager.ErrUnavailable) {
		t.Fatal(err)
	}
	if calls != 1 || native.stopCalls != 1 || !driver.SafetyQuarantined() {
		t.Fatal("failed close lost quarantine or retried native close")
	}
}

func TestM05OperatorCapabilityRemainsNative(t *testing.T) {
	native := &msp05bRuntime{}
	admin := &issue809AdminStub{}
	options := m05Options(t, func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		t.Fatal("operator factory fell back to public factory")
		return nil, nil
	})
	expected, err := mapEEBusRuntimeConfig(options.Config, options.Resolve)
	if err != nil {
		t.Fatal(err)
	}
	options.OperatorFactory = func(cfg eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
		if !reflect.DeepEqual(cfg, expected) {
			t.Fatal("operator native configuration changed")
		}
		return native, admin, nil
	}
	driver := newEEBusManagedDriver(options)
	generation, err := driver.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.WithGeneration(generation, func(adapter *eebusRuntimeAdapter, capability eebusruntime.AdminV1) error {
		if adapter.runtime != native || capability != admin {
			t.Fatal("native capability replaced")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if native.stopCalls != 1 {
		t.Fatal("operator runtime leaked")
	}
}
