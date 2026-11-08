package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// h11CrossResponder answers a provider of one family with another family's
// synthetic fixture: a request that fixture recorded gets its recorded answer,
// and any other request gets an illegal-data-address exception. Nothing is
// matched by order, so a foreign request sequence is served deterministically.
type h11CrossResponder struct {
	mu      sync.Mutex
	answers map[string][]byte
}

func h11NewCrossResponder(fixture h10Replay) *h11CrossResponder {
	r := &h11CrossResponder{answers: map[string][]byte{}}
	for _, cycle := range fixture.Cycles {
		for _, step := range cycle.Steps {
			key := string(append([]byte{step.Unit}, step.Request...))
			if _, seen := r.answers[key]; !seen && len(step.Response) != 0 {
				r.answers[key] = step.Response
			}
		}
	}
	return r
}

func (r *h11CrossResponder) answer(unit byte, pdu []byte) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if response, ok := r.answers[string(append([]byte{unit}, pdu...))]; ok {
		return response
	}
	return []byte{pdu[0] | 0x80, 0x02}
}

func h11CrossLoopback(t *testing.T, responder *h11CrossResponder) string {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		peer, err := listener.AcceptTCP()
		if err != nil {
			return
		}
		defer func() { _ = peer.Close() }()
		for {
			header := make([]byte, 7)
			if _, err := io.ReadFull(peer, header); err != nil {
				return
			}
			size := int(binary.BigEndian.Uint16(header[4:6])) - 1
			if size < 1 || size > modbus.MaxTolerantPDUSize {
				return
			}
			pdu := make([]byte, size)
			if _, err := io.ReadFull(peer, pdu); err != nil {
				return
			}
			if pdu[0] != 3 && (pdu[0] != 43 || len(pdu) != 4 || pdu[1] != 14) {
				return
			}
			response := responder.answer(header[6], pdu)
			frame := append([]byte(nil), header...)
			binary.BigEndian.PutUint16(frame[4:6], uint16(len(response)+1))
			if _, err := peer.Write(append(frame, response...)); err != nil {
				return
			}
		}
	}()
	return "tcp://" + listener.Addr().String()
}

// h11CrossCycle runs the real registry provider of the declared family once
// against the other family's fixture.
func h11CrossCycle(t *testing.T, declared modbusTCPFamily, faced h10Replay, units []byte, label string) (modbusreg.HuaweiProviderCycle, error) {
	t.Helper()
	responder := h11NewCrossResponder(faced)
	clock := &h10Clock{now: time.Now().Add(time.Second)}
	config, err := mapModbusRuntimeConfig(ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: h11CrossLoopback(t, responder), DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	config.Clock = clock.Now
	if config.SemanticPV, err = modbusadapter.NewSemanticPVClass(newTestSemanticHub(t)); err != nil {
		t.Fatal(err)
	}
	config.Endpoint.Framing = modbus.TCPFramingTolerant
	config.Endpoint.MaxBufferedBytes = 7 + modbus.MaxTolerantPDUSize
	adapter, err := modbusadapter.Start(context.Background(), config, func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}, func(config modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return modbus.NewTCPEndpoint(config)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	session, err := modbusadapter.NewReadOnlySession(adapter)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := newHuaweiRegistryProvider(declared, modbusreg.HuaweiProviderConfig{Source: label, AllowedUnits: units, Clock: clock.Now, Wait: clock.Wait})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	if err := provider.Start(session); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return provider.Refresh(ctx)
}

// h11Assets lists the PV assets a published cycle owns: one per admitted child.
func h11Assets(label string, cycle modbusreg.HuaweiProviderCycle) []string {
	var out []string
	for _, child := range cycle.Children {
		if child.Evidence != modbusreg.HuaweiProviderEvidenceWithheld {
			out = append(out, string(huaweiAssetID(label, "pv", child.UnitID)))
		}
	}
	sort.Strings(out)
	return out
}

// h11AssertNoStorageOrMeter proves that nothing outside the PV pack exists:
// no storage or infrastructure asset, fact or retained observation. An absent
// value is unknown and is never published as zero.
func h11AssertNoStorageOrMeter(t *testing.T, hub *semhub.Hub) {
	t.Helper()
	for _, id := range hub.AssetIDs() {
		if !strings.HasPrefix(string(id), "pv-asset-") {
			t.Fatalf("non-PV asset %q published", id)
		}
		view, err := hub.Inspect(id)
		if err != nil {
			t.Fatal(err)
		}
		for _, envelope := range view.Snapshot.Facts {
			if envelope.Key.PackID != huaweiPVPack || strings.HasPrefix(string(envelope.Key.FactID), "storage.") || strings.HasPrefix(string(envelope.Key.FactID), "infrastructure.") {
				t.Fatalf("asset %q carries %s/%s", id, envelope.Key.PackID, envelope.Key.FactID)
			}
		}
		for _, record := range view.Snapshot.Retained {
			if record.Candidate.Key.PackID != huaweiPVPack {
				t.Fatalf("asset %q retains a %s observation", id, record.Candidate.Key.PackID)
			}
		}
	}
}

func h11LossKinds(cycle modbusreg.HuaweiProviderCycle) map[modbusreg.HuaweiProviderLossKind]bool {
	out := map[modbusreg.HuaweiProviderLossKind]bool{}
	for _, loss := range cycle.Loss {
		out[loss.Kind] = true
	}
	return out
}

// The storage and gateway-meter mapping gates admit no battery profile and
// decode no meter, so the gateway publishes no Storage or Infrastructure fact.
// The native cycle names both as explicit losses instead.
func TestH11StorageAndMeterStayWithheldUnknownNeverZero(t *testing.T) {
	for _, name := range []string{"smartlogger", "sdongle"} {
		t.Run(name, func(t *testing.T) {
			provider, _, _, clock := h10Native(t, h10Load(t, name))
			cycle := h10Refresh(t, provider, clock)
			hub := newTestSemanticHub(t)
			publication, err := newHuaweiPVPublication(hub, "h11-storage", "source:h11-storage", "alpha", nil, func() bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			if err := publication.Publish(cycle); err != nil {
				t.Fatal(err)
			}
			if len(publication.published) == 0 {
				t.Fatal("fixture published no PV asset to compare against")
			}
			admitted := 0
			for _, child := range cycle.Children {
				if child.Evidence == modbusreg.HuaweiProviderEvidenceWithheld {
					continue
				}
				admitted++
				if child.Reading.BatteryFactsAdmitted() || child.Reading.MeterFactsAdmitted() {
					t.Fatalf("unit %d: the native reading admits battery or meter facts; the mapping gates must be revised first", child.UnitID)
				}
			}
			if admitted == 0 {
				t.Fatal("fixture has no identified inverter")
			}
			loss := h11LossKinds(cycle)
			// The absent battery group and the withheld capacity are an explicit
			// native loss; every meter value is an explicit native loss.
			if !loss[modbusreg.HuaweiProviderLossBattery] || !loss[modbusreg.HuaweiProviderLossMeter] {
				t.Fatalf("losses = %v, want explicit battery and meter losses", loss)
			}
			h11AssertNoStorageOrMeter(t, hub)
			for _, id := range []semreg.DefinitionID{"storage.state.soc", "storage.pack.power", "storage.energy.charged", "storage.energy.discharged", "storage.capacity.charge", "storage.capacity.discharge", "infrastructure.power.import_active", "infrastructure.power.export_active", "infrastructure.energy.import", "infrastructure.energy.export"} {
				for _, asset := range publication.published {
					view, _ := hub.Inspect(asset)
					for _, envelope := range view.Snapshot.Facts {
						if envelope.Key.FactID == id {
							t.Fatalf("%s published on %s", id, asset)
						}
					}
				}
			}
		})
	}
}

// The Huawei instance contribution validates against the closed native member
// set, coexists per instance and is withdrawn with its exact generation.
func TestH11PortalContributionValidatesAndWithdrawsWithGeneration(t *testing.T) {
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	// Existing primary descriptors carry no diagnostics and still validate.
	for _, manifest := range []contributionv1.Manifest{gatewayPortalPVDescriptor(), gatewayPortalStorageDescriptor(), gatewayPortalEVSEDescriptor()} {
		if _, err := contributionv1.Canonicalize(manifest, contributions.index); err != nil {
			t.Fatalf("%s: %v", manifest.ManifestID, err)
		}
	}
	alpha, beta := "modbus.huawei.smartlogger.alpha", "modbus.huawei.sdongle.beta"
	if _, err := contributionv1.Canonicalize(gatewayPortalHuaweiDescriptor(alpha), contributions.index); err != nil {
		t.Fatal(err)
	}
	if got := len(gatewayPortalHuaweiDescriptor(alpha).Diagnostics); got != len(huaweiPortalDiagnostics) {
		t.Fatalf("diagnostics = %d", got)
	}
	// Each identity field of a native member is checked: a foreign member,
	// another contract owner, name or version, and a non-field kind all fail.
	for name, mutate := range map[string]func(*contributionv1.Manifest){
		"member":   func(m *contributionv1.Manifest) { m.Diagnostics[0].MemberID = "children.serial" },
		"owner":    func(m *contributionv1.Manifest) { m.Contributor.NativeContract.Owner = "helianthus" },
		"contract": func(m *contributionv1.Manifest) { m.Contributor.NativeContract.Contract = "portal-catalog" },
		"version":  func(m *contributionv1.Manifest) { m.Contributor.NativeContract.Version = "2.0.0" },
		"kind":     func(m *contributionv1.Manifest) { m.Diagnostics[0].Kind = "action" },
	} {
		manifest := gatewayPortalHuaweiDescriptor(alpha)
		mutate(&manifest)
		if _, err := contributionv1.Canonicalize(manifest, contributions.index); err == nil {
			t.Fatalf("%s mutation admitted", name)
		}
	}
	active := true
	first := &huaweiPortalLease{contributions: contributions, owner: contributionv1.DriverGeneration{DriverID: alpha, Generation: 1}}
	other := &huaweiPortalLease{contributions: contributions, owner: contributionv1.DriverGeneration{DriverID: beta, Generation: 1}}
	if err := first.publishActive(active); err != nil {
		t.Fatal(err)
	}
	if err := other.publishActive(active); err != nil {
		t.Fatal(err)
	}
	if !contributions.accepts(alpha, huaweiPortalManifestID) || !contributions.accepts(beta, huaweiPortalManifestID) {
		t.Fatal("per-instance contributions not both accepted")
	}
	first.withdraw()
	if contributions.accepts(alpha, huaweiPortalManifestID) || !contributions.accepts(beta, huaweiPortalManifestID) {
		t.Fatal("withdrawal was not scoped to its instance")
	}
	// A withdrawn lease never reopens, and its generation stays fenced.
	if err := first.publishActive(active); err != nil || contributions.accepts(alpha, huaweiPortalManifestID) {
		t.Fatal("withdrawn lease republished")
	}
	if err := contributions.PublishGeneration(first.owner, []contributionv1.Manifest{gatewayPortalHuaweiDescriptor(alpha)}); err == nil {
		t.Fatal("withdrawn generation reopened")
	}
	second := &huaweiPortalLease{contributions: contributions, owner: contributionv1.DriverGeneration{DriverID: alpha, Generation: 2}}
	if err := second.publishActive(active); err != nil || !contributions.accepts(alpha, huaweiPortalManifestID) {
		t.Fatalf("successor generation refused: %v", err)
	}
	// A stale withdrawal of the predecessor cannot remove the successor.
	first.withdraw()
	if contributions.WithdrawGeneration(first.owner) || !contributions.accepts(alpha, huaweiPortalManifestID) {
		t.Fatal("stale withdrawal removed the successor")
	}
	// An inactive publication scope contributes nothing.
	idle := &huaweiPortalLease{contributions: contributions, owner: contributionv1.DriverGeneration{DriverID: "modbus.huawei.sdongle.idle", Generation: 1}}
	if err := idle.publishActive(false); err != nil || contributions.accepts("modbus.huawei.sdongle.idle", huaweiPortalManifestID) {
		t.Fatal("inactive generation contributed")
	}
}

// h11ContributionWork contributes like the Huawei work and records, at the
// contribution withdrawal, whether the publication scope was still active.
type h11ContributionWork struct {
	adapter   *modbusadapter.Adapter
	lease     *huaweiPortalLease
	failStart error
	startErr  error
	// withdrawnActive is 0 before the contribution withdrawal, then 1 when the
	// scope was still active and 2 when it had already closed.
	withdrawnActive atomic.Int32
}

func (w *h11ContributionWork) Start() {
	if w.startErr = w.lease.publish(w.adapter); w.startErr == nil {
		w.startErr = w.failStart
	}
}
func (w *h11ContributionWork) StartError() error { return w.startErr }
func (w *h11ContributionWork) Close() error      { w.lease.withdraw(); return nil }
func (w *h11ContributionWork) WithdrawContribution() {
	state := int32(2)
	if w.adapter.PublicationActive() {
		state = 1
	}
	w.withdrawnActive.CompareAndSwap(0, state)
	w.lease.withdraw()
}

var _ modbusManagedContributionWork = (*huaweiManagedWork)(nil)

// STOP, the manager's withdrawal fence and failed-startup retirement withdraw
// the Portal contribution while the publication scope is still active, so no
// catalog request can see a descriptor whose status and assets are withdrawn.
func TestH11PortalContributionWithdrawnBeforePublicationScopeCloses(t *testing.T) {
	const driverID = "modbus.huawei.smartlogger.alpha"
	for _, path := range []string{"Stop", "FenceWithdrawal", "FailedStartup"} {
		t.Run(path, func(t *testing.T) {
			contributions, err := newGatewayPortalCatalogContributions()
			if err != nil {
				t.Fatal(err)
			}
			var work *h11ContributionWork
			driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) { return &m03Endpoint{}, nil },
				func(_ context.Context, adapter *modbusadapter.Adapter) modbusManagedWork {
					work = &h11ContributionWork{adapter: adapter, lease: &huaweiPortalLease{contributions: contributions, owner: contributionv1.DriverGeneration{DriverID: driverID, Generation: 1}}}
					if path == "FailedStartup" {
						work.failStart = errors.New("synthetic startup failure after contribution")
					}
					return work
				})
			ctx, cancel := context.WithTimeout(context.Background(), m03Watchdog)
			defer cancel()
			_, startErr := driver.Start(ctx)
			switch path {
			case "FailedStartup":
				if startErr == nil {
					t.Fatal("startup did not fail")
				}
			default:
				if startErr != nil {
					t.Fatal(startErr)
				}
				if !contributions.accepts(driverID, huaweiPortalManifestID) {
					t.Fatal("running generation did not contribute")
				}
				if path == "FenceWithdrawal" {
					driver.FenceWithdrawal()
					if contributions.accepts(driverID, huaweiPortalManifestID) {
						t.Fatal("withdrawal fence left the contribution admitted")
					}
				}
				if err := driver.Stop(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if got := work.withdrawnActive.Load(); got != 1 {
				t.Fatalf("contribution withdrawal before scope close = %d, want 1 (withdrawn while the scope was active)", got)
			}
			if contributions.accepts(driverID, huaweiPortalManifestID) {
				t.Fatal("contribution outlived its generation")
			}
		})
	}
	// The Huawei work withdraws its own lease through the same hook.
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	lease := &huaweiPortalLease{contributions: contributions, owner: contributionv1.DriverGeneration{DriverID: driverID, Generation: 1}}
	if err := lease.publishActive(true); err != nil || !contributions.accepts(driverID, huaweiPortalManifestID) {
		t.Fatalf("lease not admitted: %v", err)
	}
	(&huaweiManagedWork{portal: lease}).WithdrawContribution()
	if contributions.accepts(driverID, huaweiPortalManifestID) {
		t.Fatal("Huawei work hook left the contribution admitted")
	}
}

// h11Provider serves one recorded registry cycle to the managed lifecycle.
func h11InstallProviders(t *testing.T, cycles map[string]modbusreg.HuaweiProviderCycle) *atomic.Int32 {
	t.Helper()
	var built atomic.Int32
	original := newHuaweiRegistryProviderFn
	t.Cleanup(func() { newHuaweiRegistryProviderFn = original })
	newHuaweiRegistryProviderFn = func(family modbusTCPFamily, config modbusreg.HuaweiProviderConfig) (huaweiRegistryProvider, error) {
		cycle, ok := cycles[config.Source]
		if !ok {
			return nil, errors.New("no synthetic cycle for instance")
		}
		built.Add(1)
		return &h10SingleCycleProvider{cycle: cycle}, nil
	}
	return &built
}

// One composed fixture: synthetic SmartLogger, S-Dongle and EMMA instances,
// two instances declared as one family that face the other family's fixture,
// and the existing SunSpec instance, all through the managed setup entrypoint
// and lifecycle. Lifecycle, publication, GraphQL, MCP and Portal agree; STOP
// and RESTART are isolated per instance.
func TestH11ComposedHuaweiPathOffline(t *testing.T) {
	smartlogger, sdongle := h10Load(t, "smartlogger"), h10Load(t, "sdongle")
	native, _, _, clock := h10Native(t, smartlogger)
	alphaCycle := h10Refresh(t, native, clock)
	native, _, _, clock = h10Native(t, sdongle)
	betaCycle := h10Refresh(t, native, clock)
	betaCycle.Source = "beta"
	deltaCycle, err := h11CrossCycle(t, modbusTCPFamilyHuaweiSmartLogger, sdongle, smartlogger.AllowedUnits, "delta")
	if err != nil {
		t.Fatal(err)
	}
	epsilonCycle, err := h11CrossCycle(t, modbusTCPFamilyHuaweiSDongle, smartlogger, sdongle.AllowedUnits, "epsilon")
	if err != nil {
		t.Fatal(err)
	}
	for name, cycle := range map[string]modbusreg.HuaweiProviderCycle{"delta": deltaCycle, "epsilon": epsilonCycle} {
		if cycle.Gateway.State == modbusreg.HuaweiProviderGatewayIdentified || len(cycle.Children) != 0 {
			t.Fatalf("%s: a foreign family fixture was identified: %+v", name, cycle.Gateway)
		}
	}
	built := h11InstallProviders(t, map[string]modbusreg.HuaweiProviderCycle{"alpha": alphaCycle, "beta": betaCycle, "delta": deltaCycle, "epsilon": epsilonCycle})
	h08InstallTransport(t)
	h08FakeSiblings(t)
	cfg := ebusgateway.DefaultConfig()
	cfg.ModbusTCPConfig = h08V2(
		ebusgateway.ModbusTCPEndpointConfig{Label: "default", Family: "sunspec", Endpoint: h08EndpointA},
		ebusgateway.ModbusTCPEndpointConfig{Label: "alpha", Family: "huawei.smartlogger", Endpoint: "tcp://logger-a.example.invalid:502", Units: smartlogger.AllowedUnits},
		ebusgateway.ModbusTCPEndpointConfig{Label: "beta", Family: "huawei.sdongle", Endpoint: "tcp://dongle-b.example.invalid:502", Units: sdongle.AllowedUnits},
		ebusgateway.ModbusTCPEndpointConfig{Label: "gamma", Family: "huawei.emma", Endpoint: "tcp://emma-c.example.invalid:502", Units: []byte{1}},
		ebusgateway.ModbusTCPEndpointConfig{Label: "delta", Family: "huawei.smartlogger", Endpoint: "tcp://logger-d.example.invalid:502", Units: smartlogger.AllowedUnits},
		ebusgateway.ModbusTCPEndpointConfig{Label: "epsilon", Family: "huawei.sdongle", Endpoint: "tcp://dongle-e.example.invalid:502", Units: sdongle.AllowedUnits},
	)
	hub := newTestSemanticHub(t)
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	sidecars := prepareManagedSidecars(context.Background(), cfg, hub, rctlGUID, contributions, nil)
	manager, err := drivermanager.New(drivermanager.Config{Drivers: sidecars.driverConfigs()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	sidecars.bind(manager, nil)
	bindings := &managedSidecarBindings{sidecars: sidecars}
	ids := map[string]runtimecontrol.DriverID{"alpha": "modbus.huawei.smartlogger.alpha", "beta": "modbus.huawei.sdongle.beta", "gamma": "modbus.huawei.emma.gamma", "delta": "modbus.huawei.smartlogger.delta", "epsilon": "modbus.huawei.sdongle.epsilon"}
	families := map[string]string{"alpha": "huawei.smartlogger", "beta": "huawei.sdongle", "gamma": "huawei.emma", "delta": "huawei.smartlogger", "epsilon": "huawei.sdongle"}
	correlation := func(id runtimecontrol.DriverID, epoch string, sequence uint64) runtimecontrol.LifecycleCorrelation {
		return runtimecontrol.LifecycleCorrelation{DriverID: id, Epoch: runtimecontrol.SourceEpoch(epoch), Generation: 1, Sequence: runtimecontrol.LifecycleSequence(sequence)}
	}
	running := []runtimecontrol.DriverID{managedSunSpecID, ids["alpha"], ids["beta"], ids["delta"], ids["epsilon"]}
	for _, id := range running {
		if err := bindings.entry(id).Start(context.Background(), correlation(id, "h11-epoch", 1)); err != nil {
			t.Fatalf("START %s: %v", id, err)
		}
	}
	if err := bindings.entry(ids["gamma"]).Start(context.Background(), correlation(ids["gamma"], "h11-epoch", 1)); err == nil {
		t.Fatal("withheld EMMA instance started")
	}
	provider := newManagedModbusMCPProvider(bindings, cfg.ModbusTCPConfig, nil)
	status := func(label string) (mcp.HuaweiInstanceStatus, error) {
		return provider.HuaweiInstanceStatus(context.Background(), families[label], label)
	}
	want := map[string][]string{"alpha": h11Assets("alpha", alphaCycle), "beta": h11Assets("beta", betaCycle), "delta": nil, "epsilon": nil}
	if len(want["alpha"]) < 2 || len(want["beta"]) != 1 {
		t.Fatalf("fixture assets = %v", want)
	}
	m03Eventually(t, "every running instance published its first cycle", func() bool {
		for label, assets := range want {
			got, err := status(label)
			if err != nil || got.Disposition == string(modbusreg.HuaweiProviderCycleIncomplete) || !reflect.DeepEqual(got.PublishedAssetIDs, append([]string{}, assets...)) {
				return false
			}
		}
		return true
	})
	// Lifecycle: every composed instance runs; EMMA is withheld and never built.
	for _, id := range running {
		snapshot, ok := manager.Snapshot(string(id))
		if !ok || snapshot.ObservedState != drivermanager.ObservedRunning {
			t.Fatalf("%s observed %+v", id, snapshot.ObservedState)
		}
	}
	if built.Load() != 4 {
		t.Fatalf("providers built = %d, want one per composed Huawei instance", built.Load())
	}
	gamma, err := status("gamma")
	if err != nil || gamma.Disposition != "NO_ADMITTED_CHILD_PROFILE" || len(gamma.PublishedAssetIDs) != 0 || gamma.LiveQualified || gamma.OutboundAllowed {
		t.Fatalf("EMMA status = %+v, %v", gamma, err)
	}
	// MCP: identified instances report routing, freshness and explicit
	// storage and meter losses; the cross-family instances publish nothing.
	for label := range want {
		got, _ := status(label)
		if got.LiveQualified || got.OutboundAllowed {
			t.Fatalf("%s promoted authority", label)
		}
		if label == "delta" || label == "epsilon" {
			if got.Qualified || got.Disposition != string(modbusreg.HuaweiProviderCycleInsufficient) || len(got.Children) != 0 {
				t.Fatalf("%s facing a foreign family = %+v", label, got)
			}
			continue
		}
		if !got.Qualified || !slices.Contains(got.Loss, string(modbusreg.HuaweiProviderLossBattery)) || !slices.Contains(got.Loss, string(modbusreg.HuaweiProviderLossMeter)) {
			t.Fatalf("%s status = %+v", label, got)
		}
		for _, child := range got.Children {
			// A routed child either publishes its class and asset, or is
			// withheld with a reason and no asset.
			withheld := child.Evidence == string(modbusreg.HuaweiProviderEvidenceWithheld)
			if child.Evidence == "" || withheld && (child.Reason == "" || child.AssetID != "") || !withheld && (child.Family == "" || child.AssetID == "") {
				t.Fatalf("%s child routing = %+v", label, child)
			}
		}
	}
	// Publication: only the identified instances' PV assets exist.
	var published []string
	for _, assets := range want {
		published = append(published, assets...)
	}
	sort.Strings(published)
	var inHub []string
	for _, id := range hub.AssetIDs() {
		if hub.Readable(id) {
			inHub = append(inHub, string(id))
		}
	}
	sort.Strings(inHub)
	if !reflect.DeepEqual(inHub, published) {
		t.Fatalf("readable assets = %v, want %v", inHub, published)
	}
	h11AssertNoStorageOrMeter(t, hub)
	// GraphQL and MCP read the same SemReg snapshot of every published asset.
	snapshots := newGatewaySemanticSnapshotProvider(&gatewayRuntimeControl{hub: hub})
	// One evaluation instant, at or after every published receipt.
	evaluated := alphaCycle.CompletedAt
	if betaCycle.CompletedAt.After(evaluated) {
		evaluated = betaCycle.CompletedAt
	}
	snapshots.now = func() time.Time { return evaluated }
	gql, mcpHandler := m11Handlers(t, snapshots)
	for _, asset := range published {
		selection := m11Selection(reflect.TypeOf(semanticview.View{}))
		query := fmt.Sprintf("{a:semanticSnapshot(assetIds:[%q])%s b:semanticSnapshot(assetIds:[%q])%s}", asset, selection, asset, selection)
		response := mcpRPC(t, mcpHandler, fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"semantic.v1.snapshot.get","arguments":{"asset_ids":[%q]}}}`, asset))
		if response.Error != nil {
			t.Fatal(response.Error)
		}
		var envelope map[string]any
		if err := json.Unmarshal([]byte(response.Result["content"].([]any)[0].(map[string]any)["text"].(string)), &envelope); err != nil {
			t.Fatal(err)
		}
		m11AssertParity(t, m11GraphQL(t, gql, query), envelope)
	}
	// Portal: one contribution per running instance beside the SunSpec panel,
	// none for the withheld EMMA instance, and no Huawei product resource.
	portalState := func() map[string]uint64 {
		out := map[string]uint64{}
		for _, accepted := range contributions.snapshot().Accepted {
			out[accepted.Key.DriverID+"/"+accepted.Key.ManifestID] = accepted.Generation
		}
		return out
	}
	wantPortal := map[string]uint64{"pv.primary/portal.pv": 1}
	for _, label := range []string{"alpha", "beta", "delta", "epsilon"} {
		wantPortal[string(ids[label])+"/"+huaweiPortalManifestID] = 1
	}
	if got := portalState(); !reflect.DeepEqual(got, wantPortal) {
		t.Fatalf("portal contributions = %v, want %v", got, wantPortal)
	}
	composer, err := contributions.composer(newGatewayPortalCatalogSource(nil, nil, nil, contributions, bindings))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := composer.Catalog()
	if err != nil {
		t.Fatal(err)
	}
	descriptors := 0
	for _, descriptor := range catalog.Descriptors {
		if descriptor.ManifestID == huaweiPortalManifestID {
			descriptors++
			if len(descriptor.Diagnostics) != len(huaweiPortalDiagnostics) || len(descriptor.Fields) != 0 {
				t.Fatalf("descriptor %s = %+v", descriptor.DriverID, descriptor)
			}
		}
	}
	for _, resource := range catalog.Resources {
		if resource.ContributionManifestID == huaweiPortalManifestID {
			t.Fatal("Huawei contribution rendered a product resource")
		}
	}
	if descriptors != 4 {
		t.Fatalf("catalog Huawei descriptors = %d", descriptors)
	}
	// STOP withdraws only the stopped instance's assets and contribution.
	view, err := hub.Inspect(semreg.AssetID(want["alpha"][0]))
	if err != nil || len(view.Snapshot.Facts) == 0 {
		t.Fatal("alpha facts missing before STOP")
	}
	firstEpoch := *view.Snapshot.Facts[0].Candidates[0].SourceEpochID
	if outcome := bindings.entry(ids["alpha"]).Stop(context.Background(), correlation(ids["alpha"], "h11-epoch", 1)); !outcome.NativeClosed || !outcome.SemanticCleaned {
		t.Fatalf("STOP alpha = %+v", outcome)
	}
	for _, asset := range want["alpha"] {
		h10AssertWithdrawn(t, hub, semreg.AssetID(asset))
	}
	for _, asset := range want["beta"] {
		if !hub.Readable(semreg.AssetID(asset)) {
			t.Fatal("STOP alpha withdrew a sibling asset")
		}
	}
	if _, err := status("alpha"); err == nil {
		t.Fatal("stopped instance still reports a status")
	}
	delete(wantPortal, string(ids["alpha"])+"/"+huaweiPortalManifestID)
	if got := portalState(); !reflect.DeepEqual(got, wantPortal) {
		t.Fatalf("portal after STOP = %v, want %v", got, wantPortal)
	}
	// RESTART admits a successor epoch and a strictly later Portal generation.
	if err := bindings.entry(ids["alpha"]).Start(context.Background(), correlation(ids["alpha"], "h11-restart", 2)); err != nil {
		t.Fatalf("RESTART alpha: %v", err)
	}
	m03Eventually(t, "alpha successor publication", func() bool {
		got, err := status("alpha")
		return err == nil && reflect.DeepEqual(got.PublishedAssetIDs, want["alpha"])
	})
	view, err = hub.Inspect(semreg.AssetID(want["alpha"][0]))
	if err != nil || len(view.Snapshot.Facts) == 0 || *view.Snapshot.Facts[0].Candidates[0].SourceEpochID == firstEpoch {
		t.Fatal("RESTART did not publish under a successor epoch")
	}
	wantPortal[string(ids["alpha"])+"/"+huaweiPortalManifestID] = 2
	if got := portalState(); !reflect.DeepEqual(got, wantPortal) {
		t.Fatalf("portal after RESTART = %v, want %v", got, wantPortal)
	}
	if built.Load() != 5 {
		t.Fatalf("RESTART built %d providers, want a fresh provider", built.Load())
	}
	h11AssertNoStorageOrMeter(t, hub)
	for _, id := range running {
		_ = bindings.entry(id).Stop(context.Background(), correlation(id, map[bool]string{true: "h11-restart", false: "h11-epoch"}[id == ids["alpha"]], map[bool]uint64{true: 2, false: 1}[id == ids["alpha"]]))
	}
	if got := portalState(); len(got) != 0 {
		t.Fatalf("portal after final STOP = %v", got)
	}
}
