package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	"github.com/Project-Helianthus/helianthus-modbusreg/huawei"
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
	d, ok := huawei.RecordedProviderDisposition(huawei.ProviderPathEMMA)
	if !ok {
		t.Fatal("missing disposition")
	}
	e := huawei.EvaluateHuaweiProviderEnablement(d)
	if e.Admitted || e.Reason != huawei.ProviderNoAdmittedChildProfile {
		t.Fatalf("EMMA: %+v", e)
	}
	if _, err := newHuaweiRegistryProvider(modbusTCPFamilyHuaweiEMMA, huawei.ProviderConfig{}); err == nil {
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
			_ = p.Publish(huawei.ProviderCycle{Source: "alpha", Cycle: i})
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

// Stop and a failed startup's retirement can fence the same generation
// concurrently, and Stop retries the fence after a native failure. Every pass
// shares the single epoch retirement, so none of them reports the already
// retired epoch as a failure.
func TestH10ConcurrentSemanticFencesShareOneRetirement(t *testing.T) {
	fixture := h10Load(t, "smartlogger")
	provider, _, _, clock := h10Native(t, fixture)
	cycle := h10Refresh(t, provider, clock)
	hub := newTestSemanticHub(t)
	var active atomic.Bool
	active.Store(true)
	publication, err := newHuaweiPVPublication(hub, "huawei-fence", "source:huawei-fence", "alpha", nil, active.Load)
	if err != nil {
		t.Fatal(err)
	}
	if err := publication.Publish(cycle); err != nil {
		t.Fatal(err)
	}
	active.Store(false)
	work := &huaweiManagedWork{publication: publication}
	const passes = 8
	start := make(chan struct{})
	results := make(chan error, passes)
	for i := 0; i < passes; i++ {
		go func() {
			<-start
			results <- work.FenceSemantic()
		}()
	}
	close(start)
	for i := 0; i < passes; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent fence pass failed: %v", err)
		}
	}
	if err := work.FenceSemantic(); err != nil {
		t.Fatalf("retried fence after retirement failed: %v", err)
	}
	for _, asset := range publication.published {
		h10AssertWithdrawn(t, hub, asset)
	}
	if _, err := newHuaweiPVPublication(hub, "huawei-fence", "source:huawei-fence", "alpha", publication.handle, func() bool { return true }); err != nil {
		t.Fatalf("successor refused after shared retirement: %v", err)
	}
}

// huaweiGraphView is the graph of one logger child's home asset.
type huaweiGraphView struct {
	snapshot  semreg.Snapshot
	resources map[semreg.ResourceID]semreg.Resource
	byKind    map[semreg.DefinitionID][]semreg.Resource
}

func huaweiGraphOf(t *testing.T, hub interface {
	Inspect(semreg.AssetID) (semhub.InspectView, error)
}, asset semreg.AssetID) huaweiGraphView {
	t.Helper()
	view, err := hub.Inspect(asset)
	if err != nil {
		t.Fatal(err)
	}
	out := huaweiGraphView{snapshot: view.Snapshot, resources: map[semreg.ResourceID]semreg.Resource{}, byKind: map[semreg.DefinitionID][]semreg.Resource{}}
	for _, resource := range view.Snapshot.Resources {
		out.resources[resource.ResourceID] = resource
		if resource.State == semreg.ResourceCurrent {
			out.byKind[resource.Kind] = append(out.byKind[resource.Kind], resource)
		}
	}
	return out
}

// one returns the single current resource of kind.
func (g huaweiGraphView) one(t *testing.T, kind semreg.DefinitionID) semreg.Resource {
	t.Helper()
	if len(g.byKind[kind]) != 1 {
		t.Fatalf("%d current %s resources, want 1", len(g.byKind[kind]), kind)
	}
	return g.byKind[kind][0]
}

// subjectKinds is the subject kind of each fact's resource dimension, as the
// element catalog declares it for the PV pack.
var huaweiGraphSubjectKinds = map[semreg.DefinitionID]semreg.DefinitionID{
	"pv.dimension.input":    "resource.dc_input",
	"pv.dimension.inverter": "resource.inverter",
	"pv.dimension.system":   "resource.pv_system",
}

// identity is every resource identifier of a child by role, for comparison
// across identity events.
type huaweiGraphIdentity struct {
	system, inverter semreg.ResourceID
	inputs           map[string]semreg.ResourceID
	dimensions       map[string]string
}

// check verifies the child's graph against the re-based gate and returns its
// identifiers: one PV system function and one inverter on operator slots, a
// DC input component per published input at its native position, no phase,
// every fact keyed by the identifier of its subject, the system and input
// relations, and identity links that name the observed resources.
func (g huaweiGraphView) check(t *testing.T, asset semreg.AssetID, source semreg.SourceID) huaweiGraphIdentity {
	t.Helper()
	system, inverter := g.one(t, "resource.pv_system"), g.one(t, "resource.inverter")
	if system.Anchor.Class != semreg.AnchorFunction || system.Anchor.Basis != semreg.AnchorOperatorSlot || system.Origin.Kind != semreg.OriginOperator {
		t.Fatalf("system anchor %+v origin %s", system.Anchor, system.Origin.Kind)
	}
	if inverter.Anchor.Class != semreg.AnchorEquipment || inverter.Anchor.Basis != semreg.AnchorOperatorSlot || inverter.Origin.Kind != semreg.OriginOperator || inverter.Anchor.Parent != nil {
		t.Fatalf("inverter anchor %+v origin %s", inverter.Anchor, inverter.Origin.Kind)
	}
	if len(g.byKind["resource.phase"]) != 0 {
		t.Fatal("a phase was allocated although no profile admits a per-phase word")
	}
	identity := huaweiGraphIdentity{system: system.ResourceID, inverter: inverter.ResourceID, inputs: map[string]semreg.ResourceID{}, dimensions: map[string]string{}}
	for _, input := range g.byKind["resource.dc_input"] {
		if input.Anchor.Class != semreg.AnchorComponent || input.Anchor.Basis != semreg.AnchorNativeSlot || input.Origin.Kind != semreg.OriginNativeObservation || input.Anchor.Parent == nil || *input.Anchor.Parent != inverter.ResourceID || input.Anchor.Slot == nil {
			t.Fatalf("input anchor %+v", input.Anchor)
		}
		identity.inputs[*input.Anchor.Slot] = input.ResourceID
	}
	if len(identity.inputs) == 0 {
		t.Fatal("no DC input published")
	}
	seen := map[semreg.ResourceID]bool{}
	for _, resource := range g.snapshot.Resources {
		id := resource.ResourceID
		if seen[id] || string(id) == string(asset) || string(id) == string(source) || !strings.HasPrefix(string(id), "resource:") {
			t.Fatalf("resource identifier %q reused or equal to another identity", id)
		}
		seen[id] = true
	}
	energy := false
	for _, envelope := range g.snapshot.Facts {
		if len(envelope.Key.Dimensions) != 1 {
			t.Fatalf("fact %s has %d dimensions", envelope.Key.FactID, len(envelope.Key.Dimensions))
		}
		dimension := envelope.Key.Dimensions[0]
		value := semreg.ResourceID(*dimension.Value.Text)
		subject, ok := g.resources[value]
		if !ok || subject.State != semreg.ResourceCurrent || subject.Kind != huaweiGraphSubjectKinds[dimension.ID] {
			t.Fatalf("fact %s dimension %s=%q names no current %s", envelope.Key.FactID, dimension.ID, value, huaweiGraphSubjectKinds[dimension.ID])
		}
		key := string(envelope.Key.FactID)
		if subject.Kind == "resource.dc_input" {
			key += "@" + *subject.Anchor.Slot
		}
		identity.dimensions[key] = string(value)
		if envelope.Key.FactID == "pv.energy.generated" {
			energy = true
			if value != system.ResourceID {
				t.Fatal("lifetime energy is not about the child's PV system")
			}
		}
	}
	if !energy {
		t.Fatal("lifetime energy not published")
	}
	relations := map[string]semreg.RelationInstance{}
	for _, relation := range g.snapshot.Relations {
		relations[string(relation.Definition.ID)+"|"+string(relation.From)+"|"+string(relation.To)] = relation
	}
	// The energy counter's coverage is the one system-to-inverter relation:
	// the system groups exactly this child's inverter.
	covered := 0
	for _, relation := range g.snapshot.Relations {
		if relation.Definition.ID == "pv.relation.system_inverter" {
			covered++
			if relation.From != system.ResourceID || relation.To != inverter.ResourceID || relation.BasisClass != semreg.RelationGateRule || relation.State != semreg.LinkQualified {
				t.Fatalf("system relation %+v", relation)
			}
		}
	}
	if covered != 1 {
		t.Fatalf("%d system-to-inverter relations, want 1", covered)
	}
	for slot, input := range identity.inputs {
		if _, ok := relations["pv.relation.inverter_input|"+string(inverter.ResourceID)+"|"+string(input)]; !ok {
			t.Fatalf("input %s has no inverter relation", slot)
		}
	}
	named := map[semreg.ResourceID]bool{}
	for _, link := range g.snapshot.IdentityLinks {
		if link.State == semreg.LinkQualified {
			if link.Resource == nil {
				t.Fatal("a qualified identity link names no resource")
			}
			named[*link.Resource] = true
		}
	}
	if !named[inverter.ResourceID] || !named[system.ResourceID] {
		t.Fatal("identity links do not name the observed inverter and system")
	}
	return identity
}

// TestHuaweiPVGraphPublishesResourcesWithAllocatedIdentifiers: a logger child
// publishes its inverter, its DC inputs and its PV system as resources with
// allocated identifiers and the gate's anchors, and every fact is keyed by the
// identifier of its subject. A restart into a new epoch and a re-addressed
// endpoint keep every identifier; a replaced child gets a new inverter and new
// inputs and keeps its system.
func TestHuaweiPVGraphPublishesResourcesWithAllocatedIdentifiers(t *testing.T) {
	for _, name := range []string{"sdongle", "smartlogger"} {
		t.Run(name, func(t *testing.T) {
			fixture := h10Load(t, name)
			provider, _, _, clock := h10Native(t, fixture)
			cycle := h10Refresh(t, provider, clock)
			hub := newTestSemanticHub(t)
			active := true
			first, err := newHuaweiPVPublication(hub, "graph-test", "source:graph-test", "alpha", nil, func() bool { return active })
			if err != nil {
				t.Fatal(err)
			}
			if err := first.Publish(cycle); err != nil {
				t.Fatal(err)
			}
			if len(first.published) == 0 {
				t.Fatal("fixture admits no child")
			}
			var unit byte
			for candidate := range first.published {
				if unit == 0 || candidate < unit {
					unit = candidate
				}
			}
			asset := huaweiAssetID("alpha", "pv", unit)
			before := huaweiGraphOf(t, hub, asset).check(t, asset, "source:graph-test")

			// Restart: the stopped epoch is retired and a successor epoch
			// publishes through a fresh native session.
			active = false
			if err := first.Fence(); err != nil {
				t.Fatal(err)
			}
			successor, err := newHuaweiPVPublication(hub, "graph-test", "source:graph-test", "alpha", first.handle, func() bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			freshProvider, _, _, freshClock := h10Native(t, fixture)
			if !freshClock.Now().After(cycle.CompletedAt) {
				_ = freshClock.Wait(context.Background(), cycle.CompletedAt.Sub(freshClock.Now())+time.Second)
			}
			next := h10Refresh(t, freshProvider, freshClock)
			if err := successor.Publish(next); err != nil {
				t.Fatal(err)
			}
			if successor.handle.Epoch() == first.handle.Epoch() {
				t.Fatal("restart kept the epoch")
			}
			restarted := huaweiGraphOf(t, hub, asset).check(t, asset, "source:graph-test")
			if !reflect.DeepEqual(restarted, before) {
				t.Fatalf("restart changed identifiers:\n%+v\n%+v", before, restarted)
			}

			// Re-addressing: the endpoint address is no anchor input, so a
			// gateway that reaches the same configured instance through
			// another address and another managed source allocates the same
			// identifiers.
			readdressed := newTestSemanticHub(t)
			moved, err := newHuaweiPVPublication(readdressed, "graph-test-moved", "source:graph-test-moved", "alpha", nil, func() bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			if err := moved.Publish(cycle); err != nil {
				t.Fatal(err)
			}
			if got := huaweiGraphOf(t, readdressed, asset).check(t, asset, "source:graph-test-moved"); !reflect.DeepEqual(got, before) {
				t.Fatalf("re-addressing changed identifiers:\n%+v\n%+v", before, got)
			}

			// Replacement: the native owner reports another unit at the
			// child (identity mismatch), then admits it again.
			replaced := next
			replaced.Cycle++
			replaced.CompletedAt = replaced.CompletedAt.Add(time.Second)
			replaced.Children = append([]huawei.ProviderChildObservation(nil), next.Children...)
			for index := range replaced.Children {
				if replaced.Children[index].UnitID == unit {
					replaced.Children[index].Evidence = huawei.ProviderEvidenceWithheld
					replaced.Children[index].Reason = huawei.ProviderReasonIdentityMismatch
				}
			}
			if err := successor.Publish(replaced); err != nil {
				t.Fatal(err)
			}
			readmitted := next
			readmitted.Cycle = replaced.Cycle + 1
			readmitted.CompletedAt = replaced.CompletedAt.Add(time.Second)
			if err := successor.Publish(readmitted); err != nil {
				t.Fatal(err)
			}
			graph := huaweiGraphOf(t, hub, asset)
			after := graph.check(t, asset, "source:graph-test")
			if after.system != before.system {
				t.Fatal("replacement changed the PV system")
			}
			if after.inverter == before.inverter {
				t.Fatal("replacement kept the inverter identifier")
			}
			for slot, input := range after.inputs {
				if input == before.inputs[slot] {
					t.Fatalf("replacement kept input %s", slot)
				}
			}
			if old := graph.resources[before.inverter]; old.State == semreg.ResourceCurrent {
				t.Fatal("the replaced inverter is still current")
			}
		})
	}
}
