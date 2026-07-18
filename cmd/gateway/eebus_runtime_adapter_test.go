package main

import (
	"context"
	"errors"
	"net/netip"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

type msp05bRuntime struct {
	startErr    error
	shutdownErr error
	startCalls  int
	stopCalls   int
}

func (runtime *msp05bRuntime) Start(context.Context) error {
	runtime.startCalls++
	return runtime.startErr
}

func (runtime *msp05bRuntime) Shutdown() error {
	runtime.stopCalls++
	return runtime.shutdownErr
}

func (*msp05bRuntime) Snapshot() (eebusruntime.SnapshotV1, error) {
	return eebusruntime.SnapshotV1{}, nil
}

func (*msp05bRuntime) PairingState() ([]eebusruntime.PairingObservationV1, error) {
	return nil, nil
}

func msp05bEnabledConfig() ebusgateway.EEBusConfig {
	return ebusgateway.EEBusConfig{
		Enabled:            true,
		ListenPort:         4712,
		Interfaces:         []string{"fixture0"},
		Subnets:            []string{"192.0.2.0/24"},
		StateRoot:          "/var/lib/helianthus/eebus",
		RemoteSKIAllowlist: []string{},
		PairingWindowMode:  ebusgateway.EEBusPairingWindowClosed,
	}
}

func TestMSP05BStartEEBusRuntimeDisabledMakesZeroCalls(t *testing.T) {
	resolverCalls := 0
	factoryCalls := 0
	runtime := &msp05bRuntime{}

	adapter, err := startEEBusRuntime(
		context.Background(),
		ebusgateway.DefaultEEBusConfig(),
		func(string) ([]netip.Addr, error) {
			resolverCalls++
			return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
		},
		func(eebusruntime.Config) (eebusruntime.Runtime, error) {
			factoryCalls++
			return runtime, nil
		},
	)
	if err != nil || adapter != nil {
		t.Fatalf("disabled start = (%v, %v); want (nil, nil)", adapter, err)
	}
	if resolverCalls != 0 || factoryCalls != 0 || runtime.startCalls != 0 || runtime.stopCalls != 0 {
		t.Fatalf("disabled calls resolver=%d factory=%d start=%d shutdown=%d; want all zero",
			resolverCalls, factoryCalls, runtime.startCalls, runtime.stopCalls)
	}
}

func TestMSP05BStartEEBusRuntimeFailureShutsConstructedRuntimeOnce(t *testing.T) {
	resolver := func(string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
	}
	shutdownErr := errors.New("sidecar shutdown failed")
	tests := []struct {
		name       string
		factoryErr error
		startErr   error
	}{
		{name: "factory failure", factoryErr: errors.New("runtime factory failed")},
		{name: "start failure", startErr: errors.New("runtime start failed")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := &msp05bRuntime{startErr: test.startErr, shutdownErr: shutdownErr}
			adapter, err := startEEBusRuntime(
				context.Background(),
				msp05bEnabledConfig(),
				resolver,
				func(eebusruntime.Config) (eebusruntime.Runtime, error) {
					return runtime, test.factoryErr
				},
			)
			primaryErr := test.factoryErr
			wantStartCalls := 0
			if primaryErr == nil {
				primaryErr = test.startErr
				wantStartCalls = 1
			}
			if adapter != nil {
				t.Fatalf("adapter = %v; want nil after failure", adapter)
			}
			if !errors.Is(err, primaryErr) || !errors.Is(err, shutdownErr) {
				t.Fatalf("error = %v; want primary and shutdown causes", err)
			}
			if runtime.startCalls != wantStartCalls || runtime.stopCalls != 1 {
				t.Fatalf("calls start=%d shutdown=%d; want start=%d shutdown=1",
					runtime.startCalls, runtime.stopCalls, wantStartCalls)
			}
		})
	}
}

func TestMSP05BStartedAdapterShutdownIsIdempotent(t *testing.T) {
	runtime := &msp05bRuntime{shutdownErr: errors.New("sidecar shutdown failed")}
	adapter, err := startEEBusRuntime(
		context.Background(),
		msp05bEnabledConfig(),
		func(string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
		},
		func(eebusruntime.Config) (eebusruntime.Runtime, error) {
			return runtime, nil
		},
	)
	if err != nil || adapter == nil {
		t.Fatalf("start adapter = (%v, %v); want non-nil and nil error", adapter, err)
	}
	firstErr := adapter.Shutdown()
	secondErr := adapter.Shutdown()
	if !errors.Is(firstErr, runtime.shutdownErr) || !errors.Is(secondErr, runtime.shutdownErr) {
		t.Fatalf("shutdown errors = (%v, %v); want retained shutdown cause", firstErr, secondErr)
	}
	if runtime.startCalls != 1 || runtime.stopCalls != 1 {
		t.Fatalf("calls start=%d shutdown=%d; want exactly one each", runtime.startCalls, runtime.stopCalls)
	}
}

func TestMSP05BRunJoinsLaterErrorWithSidecarShutdown(t *testing.T) {
	originalResolver := resolveEEBusInterfaceAddressesFn
	originalFactory := newEEBusRuntimeFn
	originalWire := wireObserveFirstObserversFn
	t.Cleanup(func() {
		resolveEEBusInterfaceAddressesFn = originalResolver
		newEEBusRuntimeFn = originalFactory
		wireObserveFirstObserversFn = originalWire
	})

	runtime := &msp05bRuntime{shutdownErr: errors.New("sidecar shutdown failed")}
	laterErr := errors.New("later gateway startup failed")
	resolveEEBusInterfaceAddressesFn = func(string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
	}
	newEEBusRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		return runtime, nil
	}
	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, laterErr
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.EEBusConfig = msp05bEnabledConfig()
	err := run(context.Background(), cfg)
	if !errors.Is(err, laterErr) || !errors.Is(err, runtime.shutdownErr) {
		t.Fatalf("run error = %v; want later gateway and sidecar shutdown causes", err)
	}
	if runtime.startCalls != 1 || runtime.stopCalls != 1 {
		t.Fatalf("calls start=%d shutdown=%d; want exactly one each", runtime.startCalls, runtime.stopCalls)
	}
}
