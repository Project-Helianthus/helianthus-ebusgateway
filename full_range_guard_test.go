package ebusgateway

import (
	"context"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type testScanBus struct {
	calls int
}

func (bus *testScanBus) Send(context.Context, protocol.Frame) (*protocol.Frame, error) {
	bus.calls++
	return nil, nil
}

func TestScanWithFullRangeGuard(t *testing.T) {
	origScanFn := scanWithFullRangeGuardScanFn
	origScanDirectedFn := scanWithFullRangeGuardScanDirectedFn
	defer func() {
		scanWithFullRangeGuardScanFn = origScanFn
		scanWithFullRangeGuardScanDirectedFn = origScanDirectedFn
	}()

	reg := registry.NewDeviceRegistry(nil)
	bus := &testScanBus{}

	t.Run("join-capable nil targets diag off rejects", func(t *testing.T) {
		_, err := scanWithFullRangeGuard(context.Background(), bus, reg, 0xF0, nil, TransportAdmissionJoinCapable, false, false)
		if err == nil || !strings.Contains(err.Error(), "disabled by default") {
			t.Fatalf("expected disabled-by-default error, got %v", err)
		}
		if bus.calls != 0 {
			t.Fatalf("guard reject should not touch bus, got %d calls", bus.calls)
		}
	})

	t.Run("join-capable nil targets diag on without root rejects", func(t *testing.T) {
		_, err := scanWithFullRangeGuard(context.Background(), bus, reg, 0xF0, nil, TransportAdmissionJoinCapable, true, false)
		if err == nil || !strings.Contains(err.Error(), "no Vaillant root candidate yet") {
			t.Fatalf("expected no-root error, got %v", err)
		}
		if bus.calls != 0 {
			t.Fatalf("guard reject should not touch bus, got %d calls", bus.calls)
		}
	})

	t.Run("join-capable nil targets diag on with root proceeds via legacy scan", func(t *testing.T) {
		called := false
		scanWithFullRangeGuardScanFn = func(context.Context, registry.ScanBus, *registry.DeviceRegistry, byte, []byte) ([]registry.DeviceEntry, error) {
			called = true
			return nil, nil
		}
		_, err := scanWithFullRangeGuard(context.Background(), bus, reg, 0xF0, nil, TransportAdmissionJoinCapable, true, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !called {
			t.Fatal("expected legacy Scan path to be used")
		}
	})

	t.Run("join-capable explicit targets use ScanDirected", func(t *testing.T) {
		scanCalled := false
		directedCalled := false
		scanWithFullRangeGuardScanFn = func(context.Context, registry.ScanBus, *registry.DeviceRegistry, byte, []byte) ([]registry.DeviceEntry, error) {
			scanCalled = true
			return nil, nil
		}
		scanWithFullRangeGuardScanDirectedFn = func(context.Context, registry.ScanBus, *registry.DeviceRegistry, byte, []byte) ([]registry.DeviceEntry, error) {
			directedCalled = true
			return nil, nil
		}
		_, err := scanWithFullRangeGuard(context.Background(), bus, reg, 0xF0, []byte{0x08}, TransportAdmissionJoinCapable, false, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if scanCalled || !directedCalled {
			t.Fatalf("expected ScanDirected only, scan=%v directed=%v", scanCalled, directedCalled)
		}
	})

	t.Run("static-fallback nil targets preserves legacy scan", func(t *testing.T) {
		called := false
		scanWithFullRangeGuardScanFn = func(context.Context, registry.ScanBus, *registry.DeviceRegistry, byte, []byte) ([]registry.DeviceEntry, error) {
			called = true
			return nil, nil
		}
		_, err := scanWithFullRangeGuard(context.Background(), bus, reg, 0x31, nil, TransportAdmissionStaticFallback, false, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !called {
			t.Fatal("expected legacy Scan path for static-fallback")
		}
	})
}
