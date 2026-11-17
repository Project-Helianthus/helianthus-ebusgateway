package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-modbusreg/huawei"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
)

// huaweiRelationsFixture publishes the first cycle of a fixture under label
// on hub and returns the publication, the cycle and the lowest published unit.
func huaweiRelationsFixture(t *testing.T, hub *semhub.Hub, name, label string) (*huaweiPVPublication, huawei.ProviderCycle, byte) {
	t.Helper()
	fixture := h10Load(t, name)
	provider, _, _, clock := h10Native(t, fixture)
	cycle := h10Refresh(t, provider, clock)
	cycle.Source = label
	publication, err := newHuaweiPVPublication(hub, "relations-"+label, "source:relations-"+label, label, nil, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if err := publication.Publish(cycle); err != nil {
		t.Fatal(err)
	}
	var unit byte
	for candidate := range publication.published {
		if unit == 0 || candidate < unit {
			unit = candidate
		}
	}
	if unit == 0 {
		t.Fatal("fixture admits no child")
	}
	return publication, cycle, unit
}

// huaweiRelationsNext is the cycle after cycle with the same children, edited
// by edit.
func huaweiRelationsNext(cycle huawei.ProviderCycle, edit func(*huawei.ProviderChildObservation)) huawei.ProviderCycle {
	next := cycle
	next.Cycle++
	next.CompletedAt = cycle.CompletedAt.Add(time.Second)
	next.Children = append([]huawei.ProviderChildObservation(nil), cycle.Children...)
	if edit != nil {
		for index := range next.Children {
			edit(&next.Children[index])
		}
	}
	return next
}

// huaweiRelationsBinding returns the binding that origin names in snapshot.
func huaweiRelationsBinding(t *testing.T, snapshot semreg.Snapshot, origin semreg.OriginRef) semreg.NativeBinding {
	t.Helper()
	if origin.BindingID == nil {
		t.Fatal("relation origin names no binding")
	}
	for _, binding := range snapshot.Bindings {
		if binding.BindingID == *origin.BindingID {
			return binding
		}
	}
	t.Fatalf("relation origin binding %s not in the asset", *origin.BindingID)
	return semreg.NativeBinding{}
}

// huaweiRelationsLinked returns the resource the binding's identity link names.
func huaweiRelationsLinked(snapshot semreg.Snapshot, binding semreg.NativeBindingID) (semreg.IdentityLink, bool) {
	for _, link := range snapshot.IdentityLinks {
		if link.BindingID == binding {
			return link, true
		}
	}
	return semreg.IdentityLink{}, false
}

// huaweiRelationsCheckGate checks each relation instance of a child asset
// against the gate rows: the definition, its basis class, the kinds of both
// ends, a gate basis reference, and an origin binding of the publishing
// source that observes the starting end.
func huaweiRelationsCheckGate(t *testing.T, g huaweiGraphView, source semreg.SourceID) {
	t.Helper()
	if len(g.snapshot.RelationAbsences) != 0 {
		t.Fatalf("the gate states no absence statement, the asset holds %d", len(g.snapshot.RelationAbsences))
	}
	want := map[semreg.DefinitionID][2]semreg.DefinitionID{
		"pv.relation.system_inverter": {"resource.pv_system", "resource.inverter"},
		"pv.relation.inverter_input":  {"resource.inverter", "resource.dc_input"},
	}
	inputs := 0
	for _, relation := range g.snapshot.Relations {
		ends, ok := want[relation.Definition.ID]
		if !ok || relation.Definition.Pack.ID != huaweiPVPack {
			t.Fatalf("relation %s is not a gate row of the child asset", relation.Definition.ID)
		}
		if relation.BasisClass != semreg.RelationGateRule || relation.State != semreg.LinkQualified || relation.Origin.Kind != semreg.OriginNativeObservation {
			t.Fatalf("relation %s basis %s state %s origin %s", relation.Definition.ID, relation.BasisClass, relation.State, relation.Origin.Kind)
		}
		if g.resources[relation.From].Kind != ends[0] || g.resources[relation.To].Kind != ends[1] {
			t.Fatalf("relation %s ends %s -> %s", relation.Definition.ID, g.resources[relation.From].Kind, g.resources[relation.To].Kind)
		}
		gated := false
		for _, ref := range relation.Basis {
			gated = gated || ref.Owner == huaweiPVPack && ref.Kind == "gate.huawei.pv.relation"
		}
		if !gated {
			t.Fatalf("relation %s carries no gate basis", relation.Definition.ID)
		}
		binding := huaweiRelationsBinding(t, g.snapshot, relation.Origin)
		if binding.SourceID != source || binding.State != semreg.BindingCurrent {
			t.Fatalf("relation %s origin binding %+v", relation.Definition.ID, binding)
		}
		link, ok := huaweiRelationsLinked(g.snapshot, binding.BindingID)
		if !ok || link.Resource == nil || *link.Resource != relation.From {
			t.Fatalf("relation %s origin binding does not observe its starting end", relation.Definition.ID)
		}
		if relation.Definition.ID == "pv.relation.inverter_input" {
			inputs++
		}
	}
	if inputs != len(g.byKind["resource.dc_input"]) {
		t.Fatalf("%d inverter-input relations for %d published inputs", inputs, len(g.byKind["resource.dc_input"]))
	}
}

// huaweiRelationsLogger checks the logger asset: one current generic
// equipment resource on the operator slot of the configured instance, with an
// operator origin, observed by a current binding of the publishing source.
func huaweiRelationsLogger(t *testing.T, hub *semhub.Hub, label string, source semreg.SourceID) huaweiGraphView {
	t.Helper()
	asset := huaweiLoggerAssetID(label)
	g := huaweiGraphOf(t, hub, asset)
	logger := g.one(t, "resource.equipment")
	if logger.Anchor.Class != semreg.AnchorEquipment || logger.Anchor.Basis != semreg.AnchorOperatorSlot || logger.Origin.Kind != semreg.OriginOperator || logger.Anchor.Parent != nil || logger.Anchor.Slot == nil || *logger.Anchor.Slot != label {
		t.Fatalf("logger anchor %+v origin %s", logger.Anchor, logger.Origin.Kind)
	}
	if len(g.snapshot.Resources) != 1 || len(g.snapshot.Facts) != 0 || len(g.snapshot.RelationAbsences) != 0 {
		t.Fatalf("logger asset holds %d resources, %d facts, %d absence statements", len(g.snapshot.Resources), len(g.snapshot.Facts), len(g.snapshot.RelationAbsences))
	}
	observed := false
	for _, binding := range g.snapshot.Bindings {
		if binding.SourceID != source || binding.State != semreg.BindingCurrent {
			continue
		}
		if link, ok := huaweiRelationsLinked(g.snapshot, binding.BindingID); ok && link.State == semreg.LinkQualified && link.Resource != nil && *link.Resource == logger.ResourceID {
			observed = true
		}
	}
	if !observed {
		t.Fatal("no current binding of the source observes the logger")
	}
	return g
}

// TestHuaweiPVRelationsLoggerAssetIdentity: the logger asset is keyed by the
// configured instance label alone, under its own domain, so it never equals
// a child's asset. The label is the only field of the key and is varied.
func TestHuaweiPVRelationsLoggerAssetIdentity(t *testing.T) {
	seen := map[semreg.AssetID]string{}
	for _, label := range []string{"alpha", "beta"} {
		sum := sha256.Sum256([]byte("helianthus.modbus.gateway_path.v1\x00" + label + "\x00"))
		want := semreg.AssetID("pv-asset-" + hex.EncodeToString(sum[:])[:32])
		got := huaweiLoggerAssetID(label)
		if got != want {
			t.Fatalf("logger asset of %s: got %s want %s", label, got, want)
		}
		if other, ok := seen[got]; ok {
			t.Fatalf("labels %s and %s share a logger asset", other, label)
		}
		seen[got] = label
		for unit := 0; unit <= 255; unit++ {
			if huaweiAssetID(label, "pv", byte(unit)) == got {
				t.Fatalf("logger asset of %s equals the asset of unit %d", label, unit)
			}
		}
	}
}

// TestHuaweiPVRelationsPublishGateInstances: every published child carries
// the gate's system-to-inverter and inverter-to-input instances with the
// gate-rule basis and its evidence; the logger is generic equipment in its
// own asset; and every instance is coherent in a hub cut that includes every
// declared asset. The relays instances follow the gate once the kernel can
// name the core definition; until then the logger asset holds none.
func TestHuaweiPVRelationsPublishGateInstances(t *testing.T) {
	for _, name := range []string{"sdongle", "smartlogger"} {
		t.Run(name, func(t *testing.T) {
			hub := newTestSemanticHub(t)
			publication, _, _ := huaweiRelationsFixture(t, hub, name, "alpha")
			source := publication.handle.Source()
			inverters := map[semreg.ResourceID]bool{}
			for _, asset := range publication.published {
				g := huaweiGraphOf(t, hub, asset)
				g.check(t, asset, source)
				huaweiRelationsCheckGate(t, g, source)
				inverters[g.one(t, "resource.inverter").ResourceID] = true
			}
			logger := huaweiRelationsLogger(t, hub, "alpha", source)
			loggerID := logger.one(t, "resource.equipment").ResourceID
			for _, asset := range publication.published {
				if _, held := huaweiGraphOf(t, hub, asset).resources[loggerID]; held {
					t.Fatal("the logger resource is held by a child asset")
				}
			}
			ref, relays := huaweiRelaysRef()
			if !relays {
				if len(logger.snapshot.Relations) != 0 {
					t.Fatal("relays instances published although the kernel cannot name the definition")
				}
				if publication.relaysWithheld() != huaweiRelaysUnresolved {
					t.Fatalf("relays withholding reason %q", publication.relaysWithheld())
				}
			} else {
				if len(logger.snapshot.Relations) != len(inverters) {
					t.Fatalf("%d relays instances for %d inverters", len(logger.snapshot.Relations), len(inverters))
				}
				for _, relation := range logger.snapshot.Relations {
					if relation.Definition != ref || relation.From != loggerID || !inverters[relation.To] || relation.BasisClass != semreg.RelationNativeObservation || relation.State != semreg.LinkQualified {
						t.Fatalf("relays instance %+v", relation)
					}
				}
			}

			cut, err := hub.CaptureCut(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(cut.NotIncluded) != 0 || cut.Cut.Completeness() != graph.CutComplete {
				t.Fatalf("cut not complete: %+v", cut.NotIncluded)
			}
			members := map[semreg.AssetID]bool{}
			for _, member := range cut.Cut.Members() {
				members[member.AssetID] = true
			}
			if !members[huaweiLoggerAssetID("alpha")] {
				t.Fatal("the logger asset is not a cut member")
			}
			for _, finding := range cut.Cut.Findings() {
				t.Fatalf("cut finding %s", finding.Kind)
			}
			total := 0
			for _, asset := range publication.published {
				total += len(huaweiGraphOf(t, hub, asset).snapshot.Relations)
			}
			total += len(logger.snapshot.Relations)
			if len(cut.Cut.Relations()) != total || total == 0 {
				t.Fatalf("cut holds %d relations, the assets %d", len(cut.Cut.Relations()), total)
			}
			for _, relation := range cut.Cut.Relations() {
				if relation.Coherence != graph.CoherenceCoherent || relation.State != semreg.LinkQualified {
					t.Fatalf("relation %s %s -> %s is %s/%s in the cut", relation.Definition.ID, relation.From, relation.To, relation.State, relation.Coherence)
				}
			}
		})
	}
}

// TestHuaweiPVRelationsWithheldChildLeavesStructuralRecord: a child the native
// owner withholds keeps its resources as withdrawn records under their
// identifiers, so its structure is stated rather than silent. The gate states
// no absence statement, so none is published and the publisher refuses one.
// Re-admission makes the same identifiers current again.
func TestHuaweiPVRelationsWithheldChildLeavesStructuralRecord(t *testing.T) {
	hub := newTestSemanticHub(t)
	publication, cycle, unit := huaweiRelationsFixture(t, hub, "smartlogger", "alpha")
	asset := huaweiAssetID("alpha", "pv", unit)
	before := huaweiGraphOf(t, hub, asset)
	identity := before.check(t, asset, publication.handle.Source())

	withheld := huaweiRelationsNext(cycle, func(child *huawei.ProviderChildObservation) {
		if child.UnitID == unit {
			child.Evidence = huawei.ProviderEvidenceWithheld
			child.Reason = huawei.ProviderReasonChildTimeout
		}
	})
	if err := publication.Publish(withheld); err != nil {
		t.Fatal(err)
	}
	g := huaweiGraphOf(t, hub, asset)
	if len(g.snapshot.Relations) != 0 || len(g.snapshot.Facts) != 0 {
		t.Fatalf("withheld child keeps %d relations and %d facts", len(g.snapshot.Relations), len(g.snapshot.Facts))
	}
	if len(g.snapshot.RelationAbsences) != 0 {
		t.Fatal("an absence statement the gate does not state was published")
	}
	ids := []semreg.ResourceID{identity.system, identity.inverter}
	for _, input := range identity.inputs {
		ids = append(ids, input)
	}
	for _, id := range ids {
		resource, ok := g.resources[id]
		if !ok || resource.State != semreg.ResourceWithdrawn {
			t.Fatalf("withheld child's resource %s is %q, want a withdrawn record", id, resource.State)
		}
	}
	if len(g.snapshot.Resources) != len(before.snapshot.Resources) {
		t.Fatalf("withheld child holds %d resource records, before %d", len(g.snapshot.Resources), len(before.snapshot.Resources))
	}
	if _, ok := publication.inverters[unit]; ok {
		t.Fatal("the withheld child's inverter is still a relay endpoint")
	}
	for _, side := range []semreg.AbsenceSide{semreg.AbsenceFrom, semreg.AbsenceTo} {
		for _, definition := range []semreg.DefinitionRef{huaweiRelationSystemInverter, huaweiRelationInverterInput} {
			absence := semreg.RelationAbsence{Definition: definition, Resource: identity.inverter, Side: side, State: semreg.LinkQualified, BasisClass: semreg.RelationGateRule}
			if err := huaweiCheckAbsence(absence); !errors.Is(err, errHuaweiAbsenceNotStated) {
				t.Fatalf("absence %s/%s admitted: %v", definition.ID, side, err)
			}
		}
	}

	readmitted := huaweiRelationsNext(withheld, func(child *huawei.ProviderChildObservation) {
		for _, original := range cycle.Children {
			if original.UnitID == child.UnitID {
				*child = original
			}
		}
	})
	if err := publication.Publish(readmitted); err != nil {
		t.Fatal(err)
	}
	after := huaweiGraphOf(t, hub, asset)
	again := after.check(t, asset, publication.handle.Source())
	if again.system != identity.system || again.inverter != identity.inverter {
		t.Fatal("re-admission changed the child's identifiers")
	}
	huaweiRelationsCheckGate(t, after, publication.handle.Source())
	if publication.inverters[unit] != identity.inverter {
		t.Fatal("the re-admitted inverter is not a relay endpoint")
	}
}

// TestHuaweiPVRelationsFenceRemovesNativeBasisKeepsConfigured: retiring the
// epoch removes every gate-rule and native-observation instance in the same
// snapshot, while the operator-origin resources stay current and the hub's
// operator identity declaration stays pending; the successor's batch carries
// it through the transaction accessor.
func TestHuaweiPVRelationsFenceRemovesNativeBasisKeepsConfigured(t *testing.T) {
	hub := newTestSemanticHub(t)
	publication, cycle, unit := huaweiRelationsFixture(t, hub, "smartlogger", "alpha")
	source := publication.handle.Source()
	asset := huaweiAssetID("alpha", "pv", unit)
	g := huaweiGraphOf(t, hub, asset)
	identity := g.check(t, asset, source)
	var native semreg.EvidenceRef
	for _, link := range g.snapshot.IdentityLinks {
		if link.Resource != nil && *link.Resource == identity.inverter && link.State == semreg.LinkQualified {
			for _, binding := range g.snapshot.Bindings {
				if binding.BindingID == link.BindingID {
					native = binding.NativeResource
				}
			}
		}
	}
	if native.Digest == "" {
		t.Fatal("no binding observes the inverter")
	}
	declaration := semreg.EvidenceRef{Owner: semhub.OperatorEvidenceOwner, Kind: "operator.declaration", Digest: semreg.Digest("sha256:" + huaweiHash("operator declaration", string(identity.inverter))), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
	if err := hub.SetOperatorLinks(asset, source, []semhub.OperatorLink{{Native: native, Resource: identity.inverter, Basis: []semreg.EvidenceRef{declaration}}}); err != nil {
		t.Fatal(err)
	}
	declared := func(snapshot semreg.Snapshot) int {
		count := 0
		for _, link := range snapshot.IdentityLinks {
			current := false
			for _, binding := range snapshot.Bindings {
				current = current || binding.BindingID == link.BindingID && binding.State == semreg.BindingCurrent
			}
			for _, ref := range link.Basis {
				if current && ref == declaration && link.State == semreg.LinkQualified && link.Resource != nil && *link.Resource == identity.inverter {
					count++
				}
			}
		}
		return count
	}
	next := huaweiRelationsNext(cycle, nil)
	if err := publication.Publish(next); err != nil {
		t.Fatal(err)
	}
	g = huaweiGraphOf(t, hub, asset)
	if declared(g.snapshot) != 1 {
		t.Fatal("the pending operator identity link was not carried by the publisher's batch")
	}
	huaweiRelationsCheckGate(t, g, source)

	if err := publication.Fence(); err != nil {
		t.Fatal(err)
	}
	g = huaweiGraphOf(t, hub, asset)
	if len(g.snapshot.Relations) != 0 {
		t.Fatalf("the retired epoch left %d native-basis relations", len(g.snapshot.Relations))
	}
	for _, id := range []semreg.ResourceID{identity.system, identity.inverter} {
		if g.resources[id].State != semreg.ResourceCurrent {
			t.Fatalf("operator-origin resource %s is %s after the fence", id, g.resources[id].State)
		}
	}
	logger := huaweiGraphOf(t, hub, huaweiLoggerAssetID("alpha"))
	if logger.one(t, "resource.equipment").State != semreg.ResourceCurrent || len(logger.snapshot.Relations) != 0 {
		t.Fatal("the fence changed the configured logger or left a native relays instance")
	}

	successor, err := newHuaweiPVPublication(hub, "relations-alpha", string(source), "alpha", publication.handle, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if err := successor.Publish(huaweiRelationsNext(next, nil)); err != nil {
		t.Fatal(err)
	}
	g = huaweiGraphOf(t, hub, asset)
	if declared(g.snapshot) != 1 {
		t.Fatal("the successor did not carry the operator identity declaration")
	}
	huaweiRelationsCheckGate(t, g, source)
	for _, relation := range g.snapshot.Relations {
		if binding := huaweiRelationsBinding(t, g.snapshot, relation.Origin); binding.SourceEpochID != successor.handle.Epoch() {
			t.Fatal("a relation of the retired epoch survived")
		}
	}
	huaweiRelationsLogger(t, hub, "alpha", source)
}

// huaweiRelationsEnds is every resource the asset holds current, by kind.
func huaweiRelationsEnds(g huaweiGraphView) map[semreg.ResourceID]semreg.DefinitionID {
	out := map[semreg.ResourceID]semreg.DefinitionID{}
	for id, resource := range g.resources {
		if resource.State == semreg.ResourceCurrent {
			out[id] = resource.Kind
		}
	}
	return out
}

// TestHuaweiPVRelationsRefuseFalseJoinAndDanglingEndpoint: a relation the gate
// does not state is refused whatever the catalog admits: a join by label or
// index with a configuration basis, a definition with the wrong end kinds, or
// a relays instance on a basis the gate does not name (false join). A far end
// the publication has not published, another instance's resource or an
// unallocated identifier is refused (dangling endpoint). One unit reached
// through two labels stays two unjoined inverters in the cut.
func TestHuaweiPVRelationsRefuseFalseJoinAndDanglingEndpoint(t *testing.T) {
	hub := newTestSemanticHub(t)
	alpha, _, unit := huaweiRelationsFixture(t, hub, "smartlogger", "alpha")
	beta, _, betaUnit := huaweiRelationsFixture(t, hub, "smartlogger", "beta")
	if unit != betaUnit {
		t.Fatal("the two labels do not reach the same unit")
	}
	a := huaweiGraphOf(t, hub, huaweiAssetID("alpha", "pv", unit))
	b := huaweiGraphOf(t, hub, huaweiAssetID("beta", "pv", unit))
	ai, bi := a.check(t, huaweiAssetID("alpha", "pv", unit), alpha.handle.Source()), b.check(t, huaweiAssetID("beta", "pv", unit), beta.handle.Source())
	if ai.inverter == bi.inverter || ai.system == bi.system {
		t.Fatal("one unit reached through two labels was joined into one resource")
	}
	var accepted semreg.RelationInstance
	for _, relation := range a.snapshot.Relations {
		if relation.Definition == huaweiRelationSystemInverter {
			accepted = relation
		}
	}
	ends := huaweiRelationsEnds(a)
	if err := huaweiCheckRelation(accepted, ends); err != nil {
		t.Fatalf("the published gate instance is refused: %v", err)
	}
	var anyInput semreg.ResourceID
	for _, input := range ai.inputs {
		anyInput = input
	}
	loggerID := huaweiGraphOf(t, hub, huaweiLoggerAssetID("alpha")).one(t, "resource.equipment").ResourceID
	withEnds := func(extra map[semreg.ResourceID]semreg.DefinitionID) map[semreg.ResourceID]semreg.DefinitionID {
		out := map[semreg.ResourceID]semreg.DefinitionID{}
		for id, kind := range ends {
			out[id] = kind
		}
		for id, kind := range extra {
			out[id] = kind
		}
		return out
	}
	relaysRef := semreg.DefinitionRef{Pack: huaweiRelationSystemInverter.Pack, ID: huaweiRelaysID, Version: "1.0.0"}
	operator := semreg.OriginRef{OriginID: "origin:relations-test:operator", Kind: semreg.OriginOperator, Evidence: accepted.Basis}
	edit := func(change func(*semreg.RelationInstance)) semreg.RelationInstance {
		relation := accepted
		relation.Basis = append([]semreg.EvidenceRef(nil), accepted.Basis...)
		change(&relation)
		return relation
	}
	falseJoins := map[string]struct {
		relation semreg.RelationInstance
		ends     map[semreg.ResourceID]semreg.DefinitionID
	}{
		// basis: a configuration basis derived from the label.
		"system by label": {edit(func(r *semreg.RelationInstance) {
			r.BasisClass, r.Origin = semreg.RelationConfiguration, operator
		}), ends},
		// basis class only: a native-observation class the gate does not give
		// this definition, origin and basis list unchanged.
		"system on another basis class": {edit(func(r *semreg.RelationInstance) { r.BasisClass = semreg.RelationNativeObservation }), ends},
		// basis kept, origin: an operator origin under a gate-rule basis.
		"system by operator origin": {edit(func(r *semreg.RelationInstance) { r.Origin = operator }), ends},
		// definition: the input relation between the system and the inverter.
		"input by index": {edit(func(r *semreg.RelationInstance) { r.Definition = huaweiRelationInverterInput }), ends},
		// from: an input as the grouping system.
		"system from input": {edit(func(r *semreg.RelationInstance) { r.From = anyInput }), ends},
		// to: the system groups an input.
		"system to input": {edit(func(r *semreg.RelationInstance) { r.To = anyInput }), ends},
		// definition outside the gate: a pack relation the gate leaves unknown.
		"inverter to phase": {edit(func(r *semreg.RelationInstance) {
			r.Definition = semreg.DefinitionRef{Pack: huaweiRelationSystemInverter.Pack, ID: "pv.relation.inverter_phase", Version: "1.0.0"}
		}), ends},
		// relays on a configuration basis: the catalog admits it, the gate does not.
		"relays by configuration": {edit(func(r *semreg.RelationInstance) {
			r.Definition, r.From, r.To, r.BasisClass, r.Origin = relaysRef, loggerID, ai.inverter, semreg.RelationConfiguration, operator
		}), withEnds(map[semreg.ResourceID]semreg.DefinitionID{loggerID: huaweiKindEquipment})},
		// relays in reverse: the inverter relays the logger.
		"relays reversed": {edit(func(r *semreg.RelationInstance) {
			r.Definition, r.From, r.To, r.BasisClass = relaysRef, ai.inverter, loggerID, semreg.RelationNativeObservation
		}), withEnds(map[semreg.ResourceID]semreg.DefinitionID{loggerID: huaweiKindEquipment})},
		// basis: an empty basis list.
		"system without basis": {edit(func(r *semreg.RelationInstance) { r.Basis = nil }), ends},
	}
	for name, test := range falseJoins {
		if err := huaweiCheckRelation(test.relation, test.ends); !errors.Is(err, errHuaweiFalseJoin) {
			t.Fatalf("%s: got %v, want a false join", name, err)
		}
	}
	dangling := map[string]semreg.RelationInstance{
		// to: the other label's inverter, which this publication never published.
		"inverter of another label": edit(func(r *semreg.RelationInstance) { r.To = bi.inverter }),
		// from: the other label's system.
		"system of another label": edit(func(r *semreg.RelationInstance) { r.From = bi.system }),
		// to: an identifier the allocator never allocated.
		"unallocated inverter": edit(func(r *semreg.RelationInstance) { r.To = "resource:unallocated-inverter" }),
		// relays to an inverter that is not published.
		"relays to an unpublished inverter": edit(func(r *semreg.RelationInstance) {
			r.Definition, r.From, r.To, r.BasisClass = relaysRef, loggerID, "resource:unallocated-inverter", semreg.RelationNativeObservation
		}),
	}
	for name, relation := range dangling {
		if err := huaweiCheckRelation(relation, withEnds(map[semreg.ResourceID]semreg.DefinitionID{loggerID: huaweiKindEquipment})); !errors.Is(err, errHuaweiDanglingEndpoint) {
			t.Fatalf("%s: got %v, want a dangling endpoint", name, err)
		}
	}
	// The relays builder names only inverters whose child batch committed.
	alpha.mu.Lock()
	alpha.inverters[200] = "resource:unallocated-inverter"
	relays := alpha.relays(relaysRef, loggerID, "binding:relations-test")
	delete(alpha.inverters, 200)
	var unpublished []semreg.RelationInstance
	for _, relation := range relays {
		if relation.To == "resource:unallocated-inverter" {
			unpublished = append(unpublished, relation)
		}
	}
	_, refused := alpha.checkLoggerRelations(unpublished, loggerID)
	committed := alpha.relays(relaysRef, loggerID, "binding:relations-test")
	alpha.mu.Unlock()
	if len(relays) != len(committed)+1 || !errors.Is(refused, errHuaweiDanglingEndpoint) {
		t.Fatalf("relays to an unpublished inverter: got %v", refused)
	}
	for _, relation := range committed {
		if relation.From != loggerID || relation.BasisClass != semreg.RelationNativeObservation || relation.Origin.Kind != semreg.OriginNativeObservation || relation.Origin.BindingID == nil || *relation.Origin.BindingID != "binding:relations-test" {
			t.Fatalf("relays instance %+v", relation)
		}
	}

	cut, err := hub.CaptureCut(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range cut.Cut.Findings() {
		t.Fatalf("two labels produced the cut finding %s", finding.Kind)
	}
	alphaOwn, betaOwn := huaweiRelationsEnds(a), huaweiRelationsEnds(b)
	for _, relation := range cut.Cut.Relations() {
		_, fromAlpha := alphaOwn[relation.From]
		_, toAlpha := alphaOwn[relation.To]
		_, fromBeta := betaOwn[relation.From]
		_, toBeta := betaOwn[relation.To]
		if fromAlpha && toBeta || fromBeta && toAlpha {
			t.Fatalf("relation %s joins the two labels", relation.Definition.ID)
		}
	}
}
