package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func TestH10AssetIdentity(t *testing.T) {
	// Every variable field of the finalized key is varied: configured label,
	// class token and routed unit. Domain and trailing separators are checked
	// independently against the public derivation, catching concatenation edits.
	for _, label := range []string{"alpha", "beta"} {
		for _, class := range []string{"pv", "storage"} {
			for _, unit := range []byte{1, 2} {
				sum := sha256.Sum256([]byte("helianthus.modbus.asset.v1\x00" + label + "\x00" + class + "\x00" + strconv.Itoa(int(unit)) + "\x00"))
				want := class + "-asset-" + hex.EncodeToString(sum[:])[:32]
				if got := huaweiAssetID(label, class, unit); string(got) != want {
					t.Fatalf("derivation: got %s want %s", got, want)
				}
			}
		}
	}
}

func TestH10EMMAFailsClosed(t *testing.T) {
	d, ok := modbusreg.HuaweiRecordedProviderDisposition(modbusreg.HuaweiProviderPathEMMA)
	if !ok {
		t.Fatal("missing disposition")
	}
	e := modbusreg.EvaluateHuaweiProviderEnablement(d)
	if e.Admitted || e.Reason != modbusreg.HuaweiProviderNoAdmittedChildProfile {
		t.Fatalf("EMMA: %+v", e)
	}
	if _, err := newHuaweiRegistryProvider(modbusTCPFamilyHuaweiEMMA, modbusreg.HuaweiProviderConfig{}); err == nil {
		t.Fatal("EMMA acquisition admitted")
	}
}

func TestH10NativeCountersUseClosedLabels(t *testing.T) {
	var counters huaweiWireCounterStore
	for _, family := range []modbusTCPFamily{modbusTCPFamilyHuaweiSmartLogger, modbusTCPFamilyHuaweiSDongle} {
		for _, function := range []modbus.FunctionCode{modbus.FunctionReadHoldingRegisters, modbus.FunctionEncapsulatedInterface} {
			for _, kind := range []modbus.TCPTransportEventKind{modbus.TCPEventWriteInvocation, modbus.TCPEventResponseReceive} {
				counters.record(family, modbus.TCPTransportEvent{RequestedFunction: function, Kind: kind, UnitID: 1, Endpoint: "must-stay-native", RawADUHex: "03"})
				counters.record(family, modbus.TCPTransportEvent{RequestedFunction: function, Kind: kind, UnitID: 2})
			}
		}
	}
	counters.record(modbusTCPFamilyHuaweiEMMA, modbus.TCPTransportEvent{RequestedFunction: 3, Kind: modbus.TCPEventWriteInvocation})
	text := counters.text()
	if len(counters.totals) != 8 || !strings.Contains(text, `{family="huawei.smartlogger",function="fc03",outcome="write_invocation"} 2`) || strings.Contains(text, "unit=") || strings.Contains(text, "instance=") || strings.Contains(text, "endpoint=") || strings.Contains(text, "must-stay-native") {
		t.Fatalf("counter labels: %s", text)
	}
}

type h10SeparateFenceWork struct {
	fail   atomic.Bool
	closed int
}

func (w *h10SeparateFenceWork) Start()       {}
func (w *h10SeparateFenceWork) Close() error { w.closed++; return nil }
func (w *h10SeparateFenceWork) FenceSemantic() error {
	if w.fail.Load() {
		return errors.New("synthetic semantic fence failure")
	}
	return nil
}
func TestH10SemanticFenceRetryPreservesNativeCleanup(t *testing.T) {
	work := &h10SeparateFenceWork{}
	work.fail.Store(true)
	endpoint := &m03Endpoint{}
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) { return endpoint, nil }, func(context.Context, *modbusadapter.Adapter) modbusManagedWork { return work })
	if _, err := driver.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := driver.Stop(context.Background()); err == nil {
		t.Fatal("semantic fence failure hidden")
	}
	if driver.SafetyQuarantined() {
		t.Fatal("semantic failure misclassified as native close failure")
	}
	if work.closed != 1 || endpoint.closes.Load() != 1 {
		t.Fatal("native cleanup did not run exactly once")
	}
	work.fail.Store(false)
	if err := driver.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if work.closed != 1 || endpoint.closes.Load() != 1 {
		t.Fatal("semantic retry repeated native close")
	}
}

func TestH10CompositionAdmitsOnlyNativeProviderPaths(t *testing.T) {
	transport := h08InstallTransport(t)
	h08FakeSiblings(t)
	cfg := ebusgateway.DefaultConfig()
	cfg.ModbusTCPConfig = h08V2(
		ebusgateway.ModbusTCPEndpointConfig{Label: "alpha", Family: "huawei.smartlogger", Endpoint: h08EndpointA, Units: []byte{1, 2}},
		ebusgateway.ModbusTCPEndpointConfig{Label: "beta", Family: "huawei.sdongle", Endpoint: h08EndpointB, Units: []byte{1}},
		ebusgateway.ModbusTCPEndpointConfig{Label: "gamma", Family: "huawei.emma", Endpoint: h08EndpointA + "3", Units: []byte{1}},
	)
	sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), rctlGUID, nil, nil)
	bindings := &managedSidecarBindings{sidecars: sidecars}
	for _, id := range []runtimecontrol.DriverID{"modbus.huawei.smartlogger.alpha", "modbus.huawei.sdongle.beta"} {
		entry := bindings.entry(id)
		if entry == nil || entry.withheld != nil || entry.runtime == nil {
			t.Fatal("admitted provider not composed")
		}
	}
	emma := bindings.entry("modbus.huawei.emma.gamma")
	if emma == nil || emma.withheld == nil || emma.runtime != nil || emma.modbusTCP.withheld != "NO_ADMITTED_CHILD_PROFILE" {
		t.Fatal("EMMA acquisition not withheld")
	}
	provider := newManagedModbusMCPProvider(bindings, cfg.ModbusTCPConfig, nil)
	status, err := provider.HuaweiInstanceStatus(context.Background(), "huawei.emma", "gamma")
	if err != nil || status.Disposition != "NO_ADMITTED_CHILD_PROFILE" || status.LiveQualified || status.OutboundAllowed || len(status.PublishedAssetIDs) != 0 {
		t.Fatal("EMMA status promoted acquisition")
	}
	if transport.totalDials() != 0 || transport.factories.Load() != 0 {
		t.Fatal("inert composition performed endpoint I/O")
	}
}

// The status cycle and the published assets share one lock, so a concurrent
// reader never pairs one cycle with the assets of another.
func TestH10StatusCycleSharesPublicationLock(t *testing.T) {
	p := &huaweiPVPublication{label: "alpha", active: func() bool { return false }, published: map[byte]semreg.AssetID{}, incarnations: map[byte]uint64{}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := uint64(1); i <= 200; i++ {
			_ = p.Publish(modbusreg.HuaweiProviderCycle{Source: "alpha", Cycle: i})
		}
	}()
	for i := 0; i < 200; i++ {
		p.mu.Lock()
		_ = p.latest.Cycle
		p.mu.Unlock()
	}
	<-done
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.latest.Cycle != 200 {
		t.Fatalf("latest cycle not recorded under publication lock: %d", p.latest.Cycle)
	}
}
