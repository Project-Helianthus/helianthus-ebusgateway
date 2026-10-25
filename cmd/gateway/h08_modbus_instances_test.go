package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

// h08Addr is a synthetic net.Addr so an injected dial error carries endpoint
// material exactly like a real one.
type h08Addr string

func (h08Addr) Network() string  { return "tcp" }
func (a h08Addr) String() string { return string(a) }

// h08Transport replaces the Modbus TCP dialer and endpoint factory. Addresses
// listed as failing refuse every dial with an endpoint-bearing error; every
// other address gets an inert in-memory connection.
type h08Transport struct {
	mu        sync.Mutex
	dials     map[string]int
	failing   map[string]bool
	factories atomic.Int32
}

func h08InstallTransport(t *testing.T, failing ...string) *h08Transport {
	t.Helper()
	transport := &h08Transport{dials: map[string]int{}, failing: map[string]bool{}}
	for _, address := range failing {
		transport.failing[address] = true
	}
	originalDial, originalFactory := dialModbusEndpointFn, newModbusEndpointFn
	t.Cleanup(func() { dialModbusEndpointFn, newModbusEndpointFn = originalDial, originalFactory })
	dialModbusEndpointFn = func(_ context.Context, network, address string) (net.Conn, error) {
		transport.mu.Lock()
		transport.dials[address]++
		fail := transport.failing[address]
		transport.mu.Unlock()
		if fail {
			return nil, &net.OpError{Op: "dial", Net: network, Addr: h08Addr(address), Err: errors.New("connection refused")}
		}
		client, peer := net.Pipe()
		t.Cleanup(func() { _ = peer.Close(); _ = client.Close() })
		return client, nil
	}
	newModbusEndpointFn = func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		transport.factories.Add(1)
		return &modbusRunLifecycleEndpoint{}, nil
	}
	return transport
}

func (transport *h08Transport) dialed(address string) int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.dials[address]
}

func (transport *h08Transport) totalDials() int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	total := 0
	for _, n := range transport.dials {
		total += n
	}
	return total
}

// h08FakeSiblings keeps the real Modbus TCP descriptors and replaces only the
// serial Growatt and eeBUS sidecars with always-healthy enabled runtimes.
func h08FakeSiblings(t *testing.T) map[runtimecontrol.DriverID]*m06Runtime {
	t.Helper()
	original := gatewayManagedSidecarDescriptorsFor
	t.Cleanup(func() { gatewayManagedSidecarDescriptorsFor = original })
	runtimes := map[runtimecontrol.DriverID]*m06Runtime{managedGrowattID: {}, managedEEBusID: {}}
	gatewayManagedSidecarDescriptorsFor = func(descriptors []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
		for i := range descriptors {
			runtime, ok := runtimes[descriptors[i].DriverID]
			if !ok {
				continue
			}
			descriptors[i].Disposition = runtimecontrol.DispositionEnabled
			descriptors[i].Adapter = runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*m06Runtime, error) {
				return runtime, nil
			})
		}
		return descriptors
	}
	return runtimes
}

// h08CaptureLog records every standard log line for the test's duration.
func h08CaptureLog(t *testing.T) func() string {
	t.Helper()
	var mu sync.Mutex
	var buffer bytes.Buffer
	writer := log.Writer()
	log.SetOutput(h08LockedWriter{mu: &mu, buffer: &buffer})
	t.Cleanup(func() { log.SetOutput(writer) })
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return buffer.String()
	}
}

type h08LockedWriter struct {
	mu     *sync.Mutex
	buffer *bytes.Buffer
}

func (w h08LockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.Write(p)
}

func h08V2(entries ...ebusgateway.ModbusTCPEndpointConfig) ebusgateway.ModbusTCPConfig {
	for i := range entries {
		if entries[i].DialTimeout == 0 {
			entries[i].DialTimeout = defaultModbusTCPDialTimeout
		}
		if entries[i].Pacing == (ebusgateway.ModbusTCPPacing{}) {
			entries[i].Pacing = defaultModbusTCPPacing(modbusTCPFamily(entries[i].Family))
		}
	}
	return ebusgateway.ModbusTCPConfig{Enabled: true, Endpoints: entries}
}

func h08Status(t *testing.T, process *rctlProcess, id runtimecontrol.DriverID) runtimecontrol.DriverStatus {
	t.Helper()
	status, err := process.control.Service().Get(id)
	if err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	return status
}

func h08AwaitStates(t *testing.T, process *rctlProcess, want map[runtimecontrol.DriverID]drivermanager.ObservedState) {
	t.Helper()
	service := process.control.Service()
	ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
	defer cancel()
	for {
		changed := service.EventNotify()
		done := true
		for id, state := range want {
			status, err := service.Get(id)
			if err != nil || status.ObservedState != state {
				done = false
			}
		}
		if done {
			return
		}
		select {
		case <-changed:
		case <-ctx.Done():
			got := map[runtimecontrol.DriverID]string{}
			for id := range want {
				status, _ := service.Get(id)
				got[id] = string(status.ObservedState) + "/" + string(status.Reason)
			}
			t.Fatalf("states = %v, want %v", got, want)
		}
	}
}

// h08RenderComposition is the characterization of one composed descriptor
// set: identity, disposition, source, native owner, construction, durable
// registration and service configuration. Native versions and the instance
// GUID are normalized.
func h08RenderComposition(t *testing.T, cfg ebusgateway.Config) string {
	t.Helper()
	sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), rctlGUID, nil, nil)
	var b strings.Builder
	for _, s := range sidecars.entries {
		d := s.descriptor
		fmt.Fprintf(&b, "descriptor %s %s %s sources=%v native=%s/%s withheld=%t constructed=%t\n", d.DriverID, d.Direction, d.Disposition, d.Sources, d.NativeContract.Owner, d.NativeContract.Contract, s.withheld != nil, s.runtime != nil)
		if s.runtime != nil {
			if driver, ok := s.runtime.Runtime.(*modbusManagedDriver); ok {
				fmt.Fprintf(&b, "  modbus enabled=%t endpoint=%s dial=%s paced=%t\n", driver.config.Enabled, driver.config.Endpoint, driver.config.DialTimeout, driver.pacing != nil)
			}
		}
	}
	for _, d := range sidecars.registry.List().Drivers {
		fmt.Fprintf(&b, "registered %s\n", d.DriverID)
	}
	store, service := sidecars.serviceConfigs()
	for _, c := range store {
		fmt.Fprintf(&b, "store %s enabled=%t\n", c.DriverID, c.Enabled)
	}
	for _, c := range service {
		fmt.Fprintf(&b, "service %s retry=%d\n", c.DriverID, c.Retry.MaxAttempts)
	}
	return strings.ReplaceAll(b.String(), rctlGUID, "<guid>")
}

// TestH08V1CompositionIsGoldenEquivalent characterizes configuration v1. It
// was recorded against the composition before configuration v2 existed and
// passes unchanged after it: driver ID, source, durable registration,
// service configuration and transport limits.
func TestH08V1CompositionIsGoldenEquivalent(t *testing.T) {
	h08InstallTransport(t)
	enabled := ebusgateway.DefaultConfig()
	enabled.ModbusTCPConfig = ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: "tcp://192.0.2.40:502", DialTimeout: 3 * time.Second}
	disabled := ebusgateway.DefaultConfig()
	for name, tc := range map[string]struct {
		cfg  ebusgateway.Config
		want string
	}{
		"enabled": {enabled, `descriptor modbus.sunspec southbound enabled sources=[modbus.sunspec:<guid>] native=github.com/Project-Helianthus/helianthus-modbus/TCPEndpoint withheld=false constructed=true
  modbus enabled=true endpoint=tcp://192.0.2.40:502 dial=3s paced=false
descriptor growatt.bms.rs485 southbound disabled sources=[growatt.bms.rs485:<guid>] native=github.com/Project-Helianthus/helianthus-modbus/RTUProductionEndpoint withheld=false constructed=false
descriptor eebus.primary southbound disabled sources=[eebus.primary:<guid>] native=github.com/Project-Helianthus/helianthus-eebusreg/` + managedEEBusContractForGolden(t) + ` withheld=false constructed=false
registered ebus.primary
registered eebus.primary
registered growatt.bms.rs485
registered modbus.sunspec
store modbus.sunspec enabled=true
store growatt.bms.rs485 enabled=false
store eebus.primary enabled=false
service modbus.sunspec retry=0
service growatt.bms.rs485 retry=0
service eebus.primary retry=3
`},
		"disabled": {disabled, `descriptor modbus.sunspec southbound disabled sources=[modbus.sunspec:<guid>] native=github.com/Project-Helianthus/helianthus-modbus/TCPEndpoint withheld=false constructed=false
descriptor growatt.bms.rs485 southbound disabled sources=[growatt.bms.rs485:<guid>] native=github.com/Project-Helianthus/helianthus-modbus/RTUProductionEndpoint withheld=false constructed=false
descriptor eebus.primary southbound disabled sources=[eebus.primary:<guid>] native=github.com/Project-Helianthus/helianthus-eebusreg/` + managedEEBusContractForGolden(t) + ` withheld=false constructed=false
registered ebus.primary
registered eebus.primary
registered growatt.bms.rs485
registered modbus.sunspec
store modbus.sunspec enabled=false
store growatt.bms.rs485 enabled=false
store eebus.primary enabled=false
service modbus.sunspec retry=0
service growatt.bms.rs485 retry=0
service eebus.primary retry=3
`},
	} {
		t.Run(name, func(t *testing.T) {
			if got := h08RenderComposition(t, tc.cfg); got != tc.want {
				t.Fatalf("v1 composition changed:\n%s\nwant:\n%s", got, tc.want)
			}
		})
	}
	mapped, err := mapModbusInstanceRuntimeConfig(enabled.ModbusTCPConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	limits := mapped.Endpoint
	if mapped.Pacing != nil || limits.PoolLimits.MaxConnections != 1 || limits.PoolLimits.Connection.MaxInFlight != 4 || limits.SchedulerLimits.MaxInFlightRequests != 4 ||
		limits.SchedulerLimits.SharedBurstSlots != 16 || limits.Backoff.Floor != time.Second || limits.Backoff.Ceiling != 30*time.Second {
		t.Fatalf("v1 transport limits changed: %+v pacing=%+v", limits, mapped.Pacing)
	}
	instances, err := modbusTCPInstancesFor(enabled.ModbusTCPConfig)
	if err != nil || len(instances) != 1 || !instances[0].v1Slot || instances[0].v2 || instances[0].unit != 1 || instances[0].cycle != 15*time.Second {
		t.Fatalf("v1 instance = %+v, %v", instances, err)
	}
}

func managedEEBusContractForGolden(t *testing.T) string {
	t.Helper()
	contract := managedEEBusDescriptor(ebusgateway.DefaultConfig(), "eebus.primary").NativeContract.Contract
	if contract == "" {
		t.Fatal("eeBUS native contract missing")
	}
	return contract
}

// TestH08V1LogLinesStayUnchanged pins the categorical lines of the v1 slot
// and keys every other instance by driver ID, never by endpoint.
func TestH08V1LogLinesStayUnchanged(t *testing.T) {
	output := h08CaptureLog(t)
	v1, _ := modbusTCPInstancesFor(ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: "tcp://192.0.2.40:502", DialTimeout: time.Second})
	modbusTCPLogf(v1[0])("modbus sunspec live smoke decision=%s", "GO")
	(&managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: managedSunSpecID}, modbusTCP: &v1[0]}).classifyStartFailure(errors.New("dial tcp 192.0.2.40:502: refused"))
	v2, err := modbusTCPInstancesFor(h08V2(
		ebusgateway.ModbusTCPEndpointConfig{Label: "default", Family: "sunspec", Endpoint: h08EndpointA},
		ebusgateway.ModbusTCPEndpointConfig{Label: "roof", Family: "sunspec", Endpoint: h08EndpointB},
	))
	if err != nil {
		t.Fatal(err)
	}
	modbusTCPLogf(v2[1])("modbus sunspec live smoke decision=%s", "NO_GO")
	(&managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: v2[1].driverID}, modbusTCP: &v2[1]}).classifyStartFailure(errors.New("dial tcp sunspec-b.example.invalid:1502: refused"))
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(output()), "\n") {
		if index := strings.Index(line, "modbus sunspec"); index >= 0 {
			line = line[index:]
		} else if index := strings.Index(line, "Modbus TCP"); index >= 0 {
			line = line[index:]
		}
		lines = append(lines, line)
	}
	want := []string{
		"modbus sunspec live smoke decision=GO",
		"Modbus TCP unavailable; continuing without Modbus",
		"modbus sunspec live smoke decision=NO_GO driver=modbus.sunspec.roof",
		"Modbus TCP instance unavailable; continuing driver=modbus.sunspec.roof",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("log lines:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
}

// TestH08SunSpecAndHuaweiEntriesRegisterTwoInstances fails before
// configuration v2: only modbus.sunspec is ever registered.
func TestH08SunSpecAndHuaweiEntriesRegisterTwoInstances(t *testing.T) {
	transport := h08InstallTransport(t)
	output := h08CaptureLog(t)
	cfg := ebusgateway.DefaultConfig()
	cfg.ModbusTCPConfig = h08V2(
		ebusgateway.ModbusTCPEndpointConfig{Label: "default", Family: "sunspec", Endpoint: h08EndpointA},
		ebusgateway.ModbusTCPEndpointConfig{Label: "site-a", Family: "huawei.emma", Endpoint: "tcp://emma-a.example.invalid:502", Units: []byte{1, 2}},
	)
	sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), rctlGUID, nil, nil)
	var ids []runtimecontrol.DriverID
	for _, d := range sidecars.registry.List().Drivers {
		ids = append(ids, d.DriverID)
	}
	if fmt.Sprint(ids) != "[ebus.primary eebus.primary growatt.bms.rs485 modbus.huawei.emma.site-a modbus.sunspec]" {
		t.Fatalf("registered = %v", ids)
	}
	bindings := &managedSidecarBindings{sidecars: sidecars}
	sunspec, huawei := bindings.entry(managedSunSpecID), bindings.entry("modbus.huawei.emma.site-a")
	if sunspec == nil || sunspec.runtime == nil || sunspec.withheld != nil || !sunspec.modbusTCP.v1Slot {
		t.Fatal("sunspec default entry is not the composed v1 slot")
	}
	if sunspec.descriptor.Sources[0] != runtimecontrol.SourceID("modbus.sunspec:"+rctlGUID) || huawei.descriptor.Sources[0] != runtimecontrol.SourceID("modbus.huawei.emma.site-a:"+rctlGUID) {
		t.Fatalf("sources = %v / %v", sunspec.descriptor.Sources, huawei.descriptor.Sources)
	}
	if huawei == nil || huawei.runtime != nil || huawei.withheld == nil || huawei.descriptor.Disposition != runtimecontrol.DispositionEnabled {
		t.Fatal("Huawei entry is not a listed, withheld, unconstructed instance")
	}
	if code := failureReasonForH08(huawei.withheld); code != drivermanager.ReasonProviderUnavailable {
		t.Fatalf("withheld reason = %s", code)
	}
	if !strings.Contains(output(), "Modbus TCP instance withheld driver=modbus.huawei.emma.site-a family=huawei.emma posture=WITHHELD reason=FAMILY_NOT_ADMITTED") {
		t.Fatalf("withheld instance logged no closed reason: %s", output())
	}
	if transport.totalDials() != 0 || transport.factories.Load() != 0 {
		t.Fatal("composition dialed or constructed an endpoint")
	}
	driver := sunspec.runtime.Runtime.(*modbusManagedDriver)
	if driver.pacing == nil || driver.pacing.Gap != 200*time.Millisecond || driver.pacing.Cycle != 15*time.Second {
		t.Fatalf("v2 sunspec instance pacing = %+v", driver.pacing)
	}
	mapped, err := mapModbusInstanceRuntimeConfig(driver.config, driver.pacing)
	if err != nil {
		t.Fatal(err)
	}
	if mapped.Endpoint.PoolLimits.MaxConnections != 1 || mapped.Endpoint.PoolLimits.Connection.MaxInFlight != 1 || mapped.Endpoint.SchedulerLimits.MaxInFlightRequests != 1 ||
		mapped.Endpoint.Backoff.Floor != 5*time.Second || mapped.Endpoint.Backoff.Ceiling != 300*time.Second ||
		mapped.Pacing == nil || mapped.Pacing.Gap != 200*time.Millisecond || mapped.Pacing.Settle != time.Second {
		t.Fatalf("v2 transport = %+v pacing=%+v", mapped.Endpoint.PoolLimits, mapped.Pacing)
	}
}

func failureReasonForH08(err error) drivermanager.ReasonCode {
	var reasoned interface {
		ReasonCode() drivermanager.ReasonCode
	}
	if errors.As(err, &reasoned) {
		return reasoned.ReasonCode()
	}
	return ""
}

// TestH08DuplicateRefusesTheSetWithZeroFactoryCalls covers duplicate label,
// endpoint and driver ID in a programmatic configuration that bypassed the
// document: the whole Modbus TCP set is refused, nothing is constructed or
// dialed, and the other sidecars still register.
func TestH08DuplicateRefusesTheSetWithZeroFactoryCalls(t *testing.T) {
	for name, entries := range map[string][]ebusgateway.ModbusTCPEndpointConfig{
		"label":     {{Label: "roof", Family: "sunspec", Endpoint: h08EndpointA}, {Label: "roof", Family: "huawei.emma", Endpoint: h08EndpointB}},
		"endpoint":  {{Label: "default", Family: "sunspec", Endpoint: h08EndpointA}, {Label: "roof", Family: "sunspec", Endpoint: "tcp://SUNSPEC-A.example.invalid.:0502"}},
		"driver ID": {{Label: "default", Family: "sunspec", Endpoint: h08EndpointA}, {Label: "default", Family: "sunspec", Endpoint: h08EndpointB}},
	} {
		t.Run(name, func(t *testing.T) {
			transport := h08InstallTransport(t)
			constructed := atomic.Int32{}
			original := gatewayManagedSidecarDescriptorsFor
			t.Cleanup(func() { gatewayManagedSidecarDescriptorsFor = original })
			gatewayManagedSidecarDescriptorsFor = func(d []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
				for i := range d {
					if d[i].DriverID != managedGrowattID && d[i].DriverID != managedEEBusID {
						constructed.Add(1)
					}
				}
				return d
			}
			cfg := ebusgateway.DefaultConfig()
			cfg.ModbusTCPConfig = h08V2(entries...)
			sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), rctlGUID, nil, nil)
			var ids []runtimecontrol.DriverID
			for _, d := range sidecars.registry.List().Drivers {
				ids = append(ids, d.DriverID)
			}
			if fmt.Sprint(ids) != "[ebus.primary eebus.primary growatt.bms.rs485]" {
				t.Fatalf("registered = %v", ids)
			}
			for _, s := range sidecars.entries {
				if s.isModbusTCP() {
					t.Fatalf("refused set left instance %s", s.descriptor.DriverID)
				}
			}
			if constructed.Load() != 0 || transport.factories.Load() != 0 || transport.totalDials() != 0 {
				t.Fatalf("refused set produced descriptors=%d factories=%d dials=%d", constructed.Load(), transport.factories.Load(), transport.totalDials())
			}
		})
	}
}

// TestH08DialFailureOnOneInstanceLeavesTheOtherRunning composes the real
// lifecycle: one sunspec instance refuses every dial, the other runs, and
// eBUS, eeBUS, HTTP and MCP are unaffected. No endpoint reaches a log line or
// a runtime surface. It fails before configuration v2: the second instance
// does not exist.
func TestH08DialFailureOnOneInstanceLeavesTheOtherRunning(t *testing.T) {
	transport := h08InstallTransport(t, "sunspec-a.example.invalid:502")
	siblings := h08FakeSiblings(t)
	output := h08CaptureLog(t)
	env := newRCtlEnv(t)
	process := env.start(rctlProcessOptions{mcp: true, config: func(cfg *ebusgateway.Config) {
		cfg.ModbusTCPConfig = h08V2(
			ebusgateway.ModbusTCPEndpointConfig{Label: "default", Family: "sunspec", Endpoint: h08EndpointA},
			ebusgateway.ModbusTCPEndpointConfig{Label: "roof", Family: "sunspec", Endpoint: h08EndpointB},
		)
	}})
	h08AwaitStates(t, process, map[runtimecontrol.DriverID]drivermanager.ObservedState{
		managedSunSpecID:      drivermanager.ObservedFailed,
		"modbus.sunspec.roof": drivermanager.ObservedRunning,
		managedEEBusID:        drivermanager.ObservedRunning,
		managedGrowattID:      drivermanager.ObservedRunning,
		primaryEBusDriverID:   drivermanager.ObservedRunning,
	})
	if transport.dialed("sunspec-a.example.invalid:502") == 0 || transport.dialed("sunspec-b.example.invalid:1502") != 1 {
		t.Fatalf("dials = %v", transport.dials)
	}
	if siblings[managedEEBusID].stops.Load() != 0 || !siblings[managedEEBusID].running.Load() {
		t.Fatal("eeBUS was affected by the Modbus instance failure")
	}
	if process.adapterStatus() == "" {
		t.Fatal("HTTP/GraphQL shell lost its status")
	}
	list := process.tool(mcp.RuntimeControlListTool, `{}`)
	var wire rctlWireList
	if list.isError || json.Unmarshal(list.data, &wire) != nil {
		t.Fatalf("MCP list unavailable: %+v", list)
	}
	listed := map[string]rctlWireDriver{}
	var order []string
	for _, d := range wire.Drivers {
		listed[d.DriverID] = d
		order = append(order, d.DriverID)
	}
	if fmt.Sprint(order) != "[ebus.primary eebus.primary growatt.bms.rs485 modbus.sunspec modbus.sunspec.roof]" {
		t.Fatalf("MCP list order = %v", order)
	}
	if listed["modbus.sunspec"].ObservedState != "FAILED" || listed["modbus.sunspec.roof"].ObservedState != "RUNNING" {
		t.Fatalf("MCP list = %+v", wire.Drivers)
	}
	process.stop()
	for _, secret := range []string{"sunspec-a", "sunspec-b", "example.invalid", ":502", ":1502"} {
		if strings.Contains(output(), secret) {
			t.Fatalf("an endpoint reached the log (%q):\n%s", secret, output())
		}
		if strings.Contains(string(list.data), secret) {
			t.Fatalf("an endpoint reached the runtime list (%q)", secret)
		}
	}
	if !strings.Contains(output(), "Modbus TCP instance unavailable; continuing driver=modbus.sunspec") {
		t.Fatalf("the failing instance logged no categorical line:\n%s", output())
	}
}

// TestH08WithheldHuaweiInstanceListsWithItsClosedReason runs the real
// lifecycle with a sunspec and a Huawei entry: the Huawei instance is listed
// FAILED with PROVIDER_UNAVAILABLE, its endpoint is never dialed and the
// sunspec instance runs.
func TestH08WithheldHuaweiInstanceListsWithItsClosedReason(t *testing.T) {
	transport := h08InstallTransport(t)
	h08FakeSiblings(t)
	output := h08CaptureLog(t)
	env := newRCtlEnv(t)
	process := env.start(rctlProcessOptions{config: func(cfg *ebusgateway.Config) {
		cfg.ModbusTCPConfig = h08V2(
			ebusgateway.ModbusTCPEndpointConfig{Label: "default", Family: "sunspec", Endpoint: h08EndpointA},
			ebusgateway.ModbusTCPEndpointConfig{Label: "site-a", Family: "huawei.smartlogger", Endpoint: "tcp://logger-a.example.invalid:502"},
		)
	}})
	h08AwaitStates(t, process, map[runtimecontrol.DriverID]drivermanager.ObservedState{
		managedSunSpecID:                   drivermanager.ObservedRunning,
		"modbus.huawei.smartlogger.site-a": drivermanager.ObservedFailed,
	})
	withheld := h08Status(t, process, "modbus.huawei.smartlogger.site-a")
	if withheld.Reason != drivermanager.ReasonProviderUnavailable || !withheld.Enabled || withheld.SafetyQuarantined {
		t.Fatalf("withheld instance = %+v", withheld)
	}
	if transport.dialed("logger-a.example.invalid:502") != 0 || transport.factories.Load() != 1 {
		t.Fatalf("withheld instance touched its endpoint: dials=%v factories=%d", transport.dials, transport.factories.Load())
	}
	if !strings.Contains(output(), "reason=FAMILY_NOT_ADMITTED") || strings.Contains(output(), "logger-a") {
		t.Fatalf("log = %s", output())
	}
	process.stop()
}

// TestH08RestartingOneInstanceLeavesTheOtherGenerationUnchanged drives the
// existing helianthus.runtime.restart tool with the instance's driver ID. The
// other instance keeps its generation, desired revision and native runtime.
func TestH08RestartingOneInstanceLeavesTheOtherGenerationUnchanged(t *testing.T) {
	transport := h08InstallTransport(t)
	h08FakeSiblings(t)
	env := newRCtlEnv(t)
	process := env.start(rctlProcessOptions{mcp: true, config: func(cfg *ebusgateway.Config) {
		cfg.ModbusTCPConfig = h08V2(
			ebusgateway.ModbusTCPEndpointConfig{Label: "default", Family: "sunspec", Endpoint: h08EndpointA},
			ebusgateway.ModbusTCPEndpointConfig{Label: "roof", Family: "sunspec", Endpoint: h08EndpointB},
		)
	}})
	h08AwaitStates(t, process, map[runtimecontrol.DriverID]drivermanager.ObservedState{
		managedSunSpecID: drivermanager.ObservedRunning, "modbus.sunspec.roof": drivermanager.ObservedRunning,
	})
	manager := process.control.driver.manager
	before, _ := manager.Snapshot(string(managedSunSpecID))
	beforeStatus := h08Status(t, process, managedSunSpecID)
	roof := h08Status(t, process, "modbus.sunspec.roof")
	roofBefore, _ := manager.Snapshot("modbus.sunspec.roof")
	result := process.tool(mcp.RuntimeControlRestartTool, rctlMutationArgs("modbus.sunspec.roof", strconv.FormatUint(uint64(roof.DesiredRevision), 10)))
	if result.isError {
		t.Fatalf("restart refused: %s %s", result.code, result.message)
	}
	ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
	defer cancel()
	for {
		changed := process.control.Service().EventNotify()
		after, _ := manager.Snapshot("modbus.sunspec.roof")
		if after.ObservedState == drivermanager.ObservedRunning && after.Generation > roofBefore.Generation {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("restarted instance did not reach a successor generation: %+v", after)
		}
	}
	after, _ := manager.Snapshot(string(managedSunSpecID))
	afterStatus := h08Status(t, process, managedSunSpecID)
	if after.Generation != before.Generation || after.ObservedState != drivermanager.ObservedRunning || afterStatus.DesiredRevision != beforeStatus.DesiredRevision {
		t.Fatalf("restart of roof changed default: generation %d -> %d, revision %d -> %d", before.Generation, after.Generation, beforeStatus.DesiredRevision, afterStatus.DesiredRevision)
	}
	if transport.dialed("sunspec-a.example.invalid:502") != 1 || transport.dialed("sunspec-b.example.invalid:1502") != 2 {
		t.Fatalf("dials after restart = %v", transport.dials)
	}
	process.stop()
}

// TestH08MigrationKeepsTheDurableStateOfTheV1Driver moves a stopped v1 driver
// to configuration v2: the default entry keeps modbus.sunspec's desired state
// and revision, and a newly added label has no record and reads STOPPED.
func TestH08MigrationKeepsTheDurableStateOfTheV1Driver(t *testing.T) {
	h08InstallTransport(t)
	h08FakeSiblings(t)
	env := newRCtlEnv(t)
	v1 := env.start(rctlProcessOptions{config: func(cfg *ebusgateway.Config) {
		cfg.ModbusTCPConfig = ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: h08EndpointA, DialTimeout: time.Second}
	}})
	h08AwaitStates(t, v1, map[runtimecontrol.DriverID]drivermanager.ObservedState{managedSunSpecID: drivermanager.ObservedRunning})
	running := h08Status(t, v1, managedSunSpecID)
	ctx, cancel := context.WithTimeout(context.Background(), rctlBound)
	defer cancel()
	if _, err := v1.control.Service().Decide(ctx, runtimecontrol.MethodStop, runtimecontrol.Request{DriverID: managedSunSpecID, ExpectedRevision: running.DesiredRevision}); err != nil {
		t.Fatal(err)
	}
	h08AwaitStates(t, v1, map[runtimecontrol.DriverID]drivermanager.ObservedState{managedSunSpecID: drivermanager.ObservedStopped})
	stopped := h08Status(t, v1, managedSunSpecID)
	v1.stop()

	v2 := env.start(rctlProcessOptions{config: func(cfg *ebusgateway.Config) {
		cfg.ModbusTCPConfig = h08V2(
			ebusgateway.ModbusTCPEndpointConfig{Label: "default", Family: "sunspec", Endpoint: h08EndpointA},
			ebusgateway.ModbusTCPEndpointConfig{Label: "roof", Family: "sunspec", Endpoint: h08EndpointB},
		)
	}})
	h08AwaitStates(t, v2, map[runtimecontrol.DriverID]drivermanager.ObservedState{
		managedSunSpecID: drivermanager.ObservedStopped, "modbus.sunspec.roof": drivermanager.ObservedStopped,
	})
	migrated := h08Status(t, v2, managedSunSpecID)
	if migrated.DesiredState != runtimecontrol.DesiredStopped || migrated.DesiredRevision != stopped.DesiredRevision {
		t.Fatalf("migrated default = %+v, v1 record = %+v", migrated, stopped)
	}
	added := h08Status(t, v2, "modbus.sunspec.roof")
	if added.DesiredState != runtimecontrol.DesiredStopped {
		t.Fatalf("label added after bootstrap = %+v, want STOPPED until an accepted START", added)
	}
	v2.stop()
}

// TestH08TransportStatusAggregatesEveryInstance: the one Modbus TCP transport
// status is ready only while every enabled, unwithheld instance runs; a
// withheld family neither blocks nor fakes readiness.
func TestH08TransportStatusAggregatesEveryInstance(t *testing.T) {
	transport := h08InstallTransport(t)
	natives := map[runtimecontrol.DriverID]*m06Runtime{managedSunSpecID: {}, "modbus.sunspec.roof": {fail: true}}
	original := gatewayManagedSidecarDescriptorsFor
	t.Cleanup(func() { gatewayManagedSidecarDescriptorsFor = original })
	gatewayManagedSidecarDescriptorsFor = func(d []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
		for i := range d {
			if native, ok := natives[d[i].DriverID]; ok {
				d[i].Adapter = runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*m06Runtime, error) { return native, nil })
			}
		}
		return d
	}
	cfg := ebusgateway.DefaultConfig()
	cfg.ModbusTCPConfig = h08V2(
		ebusgateway.ModbusTCPEndpointConfig{Label: "default", Family: "sunspec", Endpoint: h08EndpointA},
		ebusgateway.ModbusTCPEndpointConfig{Label: "roof", Family: "sunspec", Endpoint: h08EndpointB},
		ebusgateway.ModbusTCPEndpointConfig{Label: "site-b", Family: "huawei.sdongle", Endpoint: "tcp://dongle-b.example.invalid:502"},
	)
	sidecars := prepareManagedSidecars(context.Background(), cfg, newTestSemanticHub(t), rctlGUID, nil, nil)
	manager, err := drivermanager.New(drivermanager.Config{Drivers: sidecars.driverConfigs()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()
	sidecars.bind(manager, nil)
	bindings := &managedSidecarBindings{sidecars: sidecars}
	start := func(id runtimecontrol.DriverID, generation runtimecontrol.DriverGeneration) {
		_ = bindings.entry(id).Start(context.Background(), runtimecontrol.LifecycleCorrelation{DriverID: id, Epoch: "synthetic-epoch", Generation: generation, Sequence: 1})
	}
	if bindings.modbusTCPStarted() {
		t.Fatal("ready before any instance started")
	}
	start(managedSunSpecID, 1)
	start("modbus.sunspec.roof", 1)
	if bindings.modbusTCPStarted() {
		t.Fatal("ready while the roof instance failed")
	}
	if status := modbusRuntimeTransportStatus(cfg.ModbusTCPConfig, bindings.modbusTCPStarted()); status.State != ebusgateway.TransportRuntimeStateDegraded {
		t.Fatalf("aggregate with a failed instance = %+v", status)
	}
	// The service retry of a failed START is a later generation of the epoch.
	natives["modbus.sunspec.roof"].fail = false
	start("modbus.sunspec.roof", 2)
	if !bindings.modbusTCPStarted() {
		t.Fatal("not ready although every composed instance runs and only the withheld family is down")
	}
	if transport.totalDials() != 0 {
		t.Fatal("status reads performed endpoint I/O")
	}
}

// TestH08SingleEndpointSurfacesAddressTheV1SlotOnly: raw reads, retained
// observations, the PV projection and the Portal PV panel stay bound to the v1
// endpoint or the v2 sunspec entry labelled default, and are unavailable as an
// unconfigured provider otherwise.
func TestH08SingleEndpointSurfacesAddressTheV1SlotOnly(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  ebusgateway.ModbusTCPConfig
		want bool
	}{
		"v1 enabled":         {ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: h08EndpointA, DialTimeout: time.Second}, true},
		"v1 disabled":        {ebusgateway.ModbusTCPConfig{}, false},
		"v2 default sunspec": {h08V2(ebusgateway.ModbusTCPEndpointConfig{Label: "default", Family: "sunspec", Endpoint: h08EndpointA}), true},
		"v2 roof only":       {h08V2(ebusgateway.ModbusTCPEndpointConfig{Label: "roof", Family: "sunspec", Endpoint: h08EndpointB}), false},
		"v2 default emma":    {h08V2(ebusgateway.ModbusTCPEndpointConfig{Label: "default", Family: "huawei.emma", Endpoint: h08EndpointA}), false},
	} {
		t.Run(name, func(t *testing.T) {
			full := ebusgateway.DefaultConfig()
			full.ModbusTCPConfig = tc.cfg
			provider := newManagedModbusMCPProvider(&managedSidecarBindings{}, tc.cfg, nil)
			if provider.ModbusV1CoreAvailable() != tc.want || portalConfigHostEnabled(full, managedSunSpecID) != tc.want {
				t.Fatalf("v1 slot availability = %t / %t, want %t", provider.ModbusV1CoreAvailable(), portalConfigHostEnabled(full, managedSunSpecID), tc.want)
			}
		})
	}
}

// TestH08OneEntryDefaultDocumentMatchesV1ExceptPacing is the migration-table
// obligation: a one-entry default document and the v1 configuration give the
// same driver ID, source, durable registration, service configuration and
// tool behavior, and their pacing differs exactly as the table states.
func TestH08OneEntryDefaultDocumentMatchesV1ExceptPacing(t *testing.T) {
	h08InstallTransport(t)
	v1 := ebusgateway.DefaultConfig()
	v1.ModbusTCPConfig = ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: h08EndpointA, DialTimeout: 5 * time.Second}
	v2 := ebusgateway.DefaultConfig()
	v2.ModbusTCPConfig = h08V2(ebusgateway.ModbusTCPEndpointConfig{Label: "default", Family: "sunspec", Endpoint: h08EndpointA})
	identity := func(rendered string) string {
		return strings.Replace(rendered, "paced=false", "paced=<pacing>", 1)
	}
	gotV1, gotV2 := h08RenderComposition(t, v1), h08RenderComposition(t, v2)
	if identity(gotV1) != strings.Replace(gotV2, "paced=true", "paced=<pacing>", 1) {
		t.Fatalf("identity, state or registration differ:\nv1:\n%s\nv2:\n%s", gotV1, gotV2)
	}
	if newManagedModbusMCPProvider(&managedSidecarBindings{}, v1.ModbusTCPConfig, nil).ModbusV1CoreAvailable() !=
		newManagedModbusMCPProvider(&managedSidecarBindings{}, v2.ModbusTCPConfig, nil).ModbusV1CoreAvailable() {
		t.Fatal("tool availability differs across the move")
	}
	instancesV1, _ := modbusTCPInstancesFor(v1.ModbusTCPConfig)
	instancesV2, _ := modbusTCPInstancesFor(v2.ModbusTCPConfig)
	if instancesV1[0].cycle != instancesV2[0].cycle || instancesV1[0].unit != instancesV2[0].unit || !instancesV2[0].v1Slot {
		t.Fatalf("cycle or unit differ: %+v vs %+v", instancesV1[0], instancesV2[0])
	}
	mappedV1, err := mapModbusInstanceRuntimeConfig(instancesV1[0].config, instancesV1[0].pacing)
	if err != nil {
		t.Fatal(err)
	}
	mappedV2, err := mapModbusInstanceRuntimeConfig(instancesV2[0].config, instancesV2[0].pacing)
	if err != nil {
		t.Fatal(err)
	}
	// Pacing is asserted to differ: four in flight against one, 1-30 s against
	// 5-300 s backoff, and no gap or settle wait against both.
	if mappedV1.Endpoint.PoolLimits.Connection.MaxInFlight != 4 || mappedV2.Endpoint.PoolLimits.Connection.MaxInFlight != 1 ||
		mappedV1.Endpoint.Backoff.Floor != time.Second || mappedV2.Endpoint.Backoff.Floor != 5*time.Second ||
		mappedV1.Endpoint.Backoff.Ceiling != 30*time.Second || mappedV2.Endpoint.Backoff.Ceiling != 300*time.Second ||
		mappedV1.Pacing != nil || mappedV2.Pacing == nil || mappedV2.Pacing.Gap != 200*time.Millisecond || mappedV2.Pacing.Settle != time.Second {
		t.Fatalf("pacing across the move: v1 %+v / %+v, v2 %+v / %+v", mappedV1.Endpoint.PoolLimits, mappedV1.Pacing, mappedV2.Endpoint.PoolLimits, mappedV2.Pacing)
	}
}

// TestH08PacedInstanceKeepsAttemptBound pins that pacing consumes the
// existing acquisition budget instead of extending it.
func TestH08PacedInstanceKeepsAttemptBound(t *testing.T) {
	pacing := ebusgateway.ModbusTCPPacing{Gap: 2 * time.Second, Settle: 10 * time.Second, Cycle: 15 * time.Second}
	work := newManagedSunSpecInstanceWork(context.Background(), nil, nil, 1, modbusTCPInstance{pacing: &pacing, unit: 1, cycle: pacing.Cycle})
	if work.attempt != sunSpecLiveSmokeAttemptTimeout {
		t.Fatalf("paced attempt bound = %s; want %s", work.attempt, sunSpecLiveSmokeAttemptTimeout)
	}
	if !work.startToStart {
		t.Fatal("paced instance must measure its cycle between starts")
	}
}

// TestH08WorkerCycleIsMeasuredBetweenStarts pins that a cycle which used part
// of the interval shortens the wait for a start-to-start worker.
func TestH08WorkerCycleIsMeasuredBetweenStarts(t *testing.T) {
	delays := make(chan time.Duration, 1)
	driver := &sunSpecLiveSmokeFakeDriver{}
	driver.pollFn = func(context.Context, sunSpecLiveSmokeAttempt) (sunSpecLiveSmokePollResult, error) {
		time.Sleep(300 * time.Millisecond)
		return sunSpecLiveSmokePollResult{}, errors.New("failed")
	}
	qualifier := &sunSpecLiveSmokeFakeQualifier{}
	worker := newSunSpecLiveSmokeWorkerCore(context.Background(), time.Second, 2*time.Second, true, driver, qualifier, func(string, ...any) {},
		func(ctx context.Context, delay time.Duration) error {
			select {
			case delays <- delay:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		})
	worker.Start()
	defer func() { _ = worker.Close() }()
	select {
	case delay := <-delays:
		if delay > 1800*time.Millisecond || delay <= 0 {
			t.Fatalf("start-to-start delay = %s; want below 1.8s", delay)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker never waited")
	}
}
