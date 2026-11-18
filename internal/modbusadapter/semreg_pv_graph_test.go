package modbusadapter

import (
	"sort"
	"strings"
	"testing"

	sunspectest "github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter/testfixture"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semgraph"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	"github.com/Project-Helianthus/helianthus-modbusreg/sunspec"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// TestSemRegPVGraphPublishesSingleDeviceResourcesAndWithholdsMultiDeviceChain
// is the graph publication of one SunSpec inverter and the withhold of a
// chain that carries more than one device.
func TestSemRegPVGraphPublishesSingleDeviceResourcesAndWithholdsMultiDeviceChain(t *testing.T) {
	t.Run("single device", func(t *testing.T) {
		draft := pvCoreDraft(t, observedFroniusFloatControlsWords(), "source-epoch:pv:graph", 1, 2_000)
		plain := pvGraphPublish(t, draft, false)
		graphed := pvGraphPublish(t, draft, true)
		if plain.snapshot.AssetID != graphed.snapshot.AssetID {
			t.Fatalf("asset = %s, plain %s", graphed.snapshot.AssetID, plain.snapshot.AssetID)
		}
		inverter, phases, system := pvGraphExpectResources(t, graphed.snapshot, draft.assetID, draft.operatorSlot)
		pvGraphExpectSubjects(t, graphed.snapshot, inverter, phases, system)
		pvGraphExpectSelectionIdentifier(t, draft, plain, graphed, inverter)
		if len(graphed.snapshot.Relations) != 0 || len(graphed.snapshot.RelationAbsences) != 0 {
			t.Fatal("graph publication carried a relation")
		}
		pvGraphRefresh(t, draft)
		pvGraphSecondUnit(t, draft)
	})
	t.Run("multi device", func(t *testing.T) {
		snapshot := pvGraphChainSnapshot(t, sunspectest.Words(1, 65, 1, 65, 0xffff, 0))
		commons := 0
		for _, occurrence := range snapshot.Occurrences() {
			if occurrence.ModelID() == 1 {
				commons++
			}
		}
		if commons < 2 || len(snapshot.RawWords()) == 0 {
			t.Fatalf("native chain commons=%d raw=%d", commons, len(snapshot.RawWords()))
		}
		rawBefore := append([]uint16(nil), snapshot.RawWords()...)
		registry, err := sunspec.NewStandardSunSpecDecoderRegistry(sunspec.ModelsRevisionV1)
		if err != nil {
			t.Fatal(err)
		}
		capability := registry.EvaluateThreePhaseMonitoring(snapshot)
		result := (&SunSpecProducer{registry: registry}).classify(SunSpecPollIdentity{PollGeneration: 1, DeadlineIdentity: 1}, snapshot, true)
		if result.SemanticWithhold != "MULTI_DEVICE_UNATTRIBUTED" {
			t.Fatalf("withhold = %q", result.SemanticWithhold)
		}
		wantOutcome := SunSpecQualificationNoGo
		if !capability.Admitted() && capability.Reason() == sunspec.CapabilityReasonInvalidChain {
			wantOutcome = SunSpecQualificationStop
		}
		if capability.Admitted() {
			wantOutcome = SunSpecQualificationStop
		}
		if result.Outcome != wantOutcome || result.CapabilityID != capability.ProfileID() || result.CapabilityReason != capability.Reason() || result.SampleID != "" || result.ObservationCount != 0 || result.FlavorID != "" || len(result.Chain.RawWords()) != 0 || len(result.Chain.Occurrences()) != 0 {
			t.Fatalf("native posture = %#v, capability admitted=%v reason=%s", result, capability.Admitted(), capability.Reason())
		}
		rawAfter := snapshot.RawWords()
		if commonsAfter := pvGraphCommonCount(snapshot); commonsAfter != commons || len(rawAfter) != len(rawBefore) {
			t.Fatal("classification changed the native chain")
		}
		for index := range rawBefore {
			if rawAfter[index] != rawBefore[index] {
				t.Fatalf("classification changed native word %d", index)
			}
		}
	})
}

func pvGraphRefresh(t *testing.T, first pvPublicationDraft) {
	t.Helper()
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	class, err := NewSemanticPVClass(hub, true)
	if err != nil {
		t.Fatal(err)
	}
	core, err := newPVPublicationCore(class)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.ingest(first); err != nil {
		t.Fatal(err)
	}
	words := observedFroniusFloatControlsWords()
	pvCoreSetFloat(words, 20, 4_321.5)
	second := pvCoreDraft(t, words, string(first.lifecycle.sourceEpochID), 1, 3_000)
	if second.assetID != first.assetID {
		t.Fatal("refresh changed the asset")
	}
	if _, err := core.ingest(second); err != nil {
		t.Fatal(err)
	}
	current := core.assets[first.assetID].current
	if current == nil {
		t.Fatal("refresh dropped the current view")
	}
	inverters := 0
	for _, resource := range current.snapshot.Resources {
		if resource.Kind == "resource.inverter" && resource.State == semreg.ResourceCurrent {
			inverters++
		}
	}
	if inverters != 1 {
		t.Fatalf("refresh inverter resources = %d", inverters)
	}
}

// pvGraphSecondUnit publishes a second common identity on the same core.
// A fixed slot, or a slot taken from the asset identifier or the
// authorization scope, does not survive these two drafts.
func pvGraphSecondUnit(t *testing.T, first pvPublicationDraft) {
	t.Helper()
	sameWords := observedFroniusFloatControlsWords()
	sameScope := pvCoreDraftFromScope(t, sameWords, "fixture:pv-publication-core-b", string(first.lifecycle.sourceEpochID), 1, 3_500)
	if sameScope.assetID != first.assetID || sameScope.operatorSlot != first.operatorSlot || sameScope.source.SourceID == first.source.SourceID {
		t.Fatal("authorization scope changed the asset or the operator slot")
	}
	words := observedFroniusFloatControlsWords()
	if len(words) <= 52 {
		t.Fatal("fixture has no common identity word")
	}
	words[52] ^= 1
	second := pvCoreDraftFromScope(t, words, "fixture:pv-publication-core-b", string(first.lifecycle.sourceEpochID), 1, 4_000)
	if second.assetID == first.assetID || second.operatorSlot == first.operatorSlot || second.operatorSlot == "" {
		t.Fatal("common identity did not change the asset and the operator slot")
	}
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	class, err := NewSemanticPVClass(hub, true)
	if err != nil {
		t.Fatal(err)
	}
	core, err := newPVPublicationCore(class)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.ingest(first); err != nil {
		t.Fatal(err)
	}
	if _, err := core.ingest(second); err != nil {
		t.Fatal(err)
	}
	firstInverter := pvGraphCurrentKind(t, core, first.assetID, "resource.inverter")
	secondInverter := pvGraphCurrentKind(t, core, second.assetID, "resource.inverter")
	if firstInverter == secondInverter || strings.Contains(string(firstInverter), string(first.assetID)) || strings.Contains(string(secondInverter), string(second.assetID)) {
		t.Fatalf("second unit reused a resource: %s %s", firstInverter, secondInverter)
	}
}

func pvCoreDraftFromScope(t *testing.T, words []uint16, scope, epoch string, generation uint64, receipt uint64) pvPublicationDraft {
	t.Helper()
	observation := pvCoreScopedObservation(t, words, receipt/10+1, receipt/10+2, scope)
	draft, err := buildPVPublicationDraft(observation, pvCoreLifecycle(epoch, generation, receipt))
	if err != nil {
		t.Fatal(err)
	}
	return draft
}

func pvGraphCurrentKind(t *testing.T, core *pvPublicationCore, asset semreg.AssetID, kind semreg.DefinitionID) semreg.ResourceID {
	t.Helper()
	held := core.assets[asset]
	if held == nil || held.current == nil {
		t.Fatalf("asset %s has no current view", asset)
	}
	current := held.current
	var found semreg.ResourceID
	for _, resource := range current.snapshot.Resources {
		if resource.Kind == kind && resource.State == semreg.ResourceCurrent {
			if found != "" {
				t.Fatalf("asset %s has two %s resources", asset, kind)
			}
			found = resource.ResourceID
		}
	}
	if found == "" {
		t.Fatalf("asset %s has no %s resource", asset, kind)
	}
	return found
}

func pvGraphPublish(t *testing.T, draft pvPublicationDraft, graph bool) *pvPublicationView {
	t.Helper()
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	class, err := NewSemanticPVClass(hub, graph)
	if err != nil {
		t.Fatal(err)
	}
	core, err := newPVPublicationCore(class)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.ingest(draft); err != nil {
		t.Fatal(err)
	}
	current := core.assets[draft.assetID].current
	if current == nil {
		t.Fatal("publication produced no current view")
	}
	state, err := hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	want := semhub.GraphNotEnforced
	if graph {
		want = semhub.GraphEnforced
	}
	for _, asset := range state.Assets {
		if asset.Asset == draft.assetID && asset.Enforcement != want {
			t.Fatalf("enforcement = %s, want %s", asset.Enforcement, want)
		}
	}
	return current
}

func pvGraphExpectResources(t *testing.T, snapshot semreg.Snapshot, asset semreg.AssetID, slot string) (semreg.ResourceID, map[string]semreg.ResourceID, semreg.ResourceID) {
	t.Helper()
	assetSuffix := strings.TrimPrefix(string(asset), "pv-asset-")
	if slot == "" || slot == string(asset) || slot == assetSuffix || strings.Contains(string(asset), slot) {
		t.Fatalf("operator slot is the asset identifier: %s", slot)
	}
	store, err := semgraph.OpenStore("")
	if err != nil {
		t.Fatal(err)
	}
	allocator, err := semgraph.NewAllocator(store)
	if err != nil {
		t.Fatal(err)
	}
	evidence := pvGraphEvidence("sunspec.operator.slot", slot)
	inverter, _, err := allocator.Resolve("resource.inverter", semgraph.Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: slot, Evidence: []semreg.EvidenceRef{evidence}})
	if err != nil {
		t.Fatal(err)
	}
	system, _, err := allocator.Resolve("resource.pv_system", semgraph.Anchor{Class: semreg.AnchorFunction, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: slot, Evidence: []semreg.EvidenceRef{evidence}})
	if err != nil {
		t.Fatal(err)
	}
	phases := map[string]semreg.ResourceID{}
	for _, phase := range []string{"L1", "L2", "L3"} {
		phaseEvidence := pvGraphEvidence("sunspec.phase.position", phase)
		id, _, err := allocator.Resolve("resource.phase", semgraph.Anchor{Class: semreg.AnchorComponent, Basis: semreg.AnchorNativeSlot, Source: semgraph.SourceNativePosition, Parent: inverter, Slot: phase, Evidence: []semreg.EvidenceRef{phaseEvidence}})
		if err != nil {
			t.Fatal(err)
		}
		phases[phase] = id
	}
	got := map[semreg.DefinitionID][]semreg.ResourceID{}
	for _, resource := range snapshot.Resources {
		if resource.State != semreg.ResourceCurrent {
			t.Fatalf("resource %s is %s", resource.ResourceID, resource.State)
		}
		if resource.Kind == "resource.inverter" || resource.Kind == "resource.pv_system" {
			if resource.Anchor.Slot == nil || *resource.Anchor.Slot != slot {
				t.Fatalf("%s slot = %v", resource.Kind, resource.Anchor.Slot)
			}
		}
		if err := resource.ResourceID.Validate(); err != nil || len(resource.ResourceID) != len("resource:")+32 {
			t.Fatalf("resource identifier %s", resource.ResourceID)
		}
		text := string(resource.ResourceID)
		if text == string(asset) || strings.Contains(text, string(asset)) {
			t.Fatalf("resource identifier derived from the asset: %s", resource.ResourceID)
		}
		got[resource.Kind] = append(got[resource.Kind], resource.ResourceID)
	}
	if len(got["resource.inverter"]) != 1 || got["resource.inverter"][0] != inverter {
		t.Fatalf("inverter resources = %v, allocated %s", got["resource.inverter"], inverter)
	}
	if len(got["resource.pv_system"]) != 1 || got["resource.pv_system"][0] != system {
		t.Fatalf("PV-system resources = %v, allocated %s", got["resource.pv_system"], system)
	}
	if len(got["resource.phase"]) != 3 {
		t.Fatalf("phase resources = %v", got["resource.phase"])
	}
	for _, slot := range []string{"L1", "L2", "L3"} {
		found := false
		for _, id := range got["resource.phase"] {
			if id == phases[slot] {
				found = true
			}
		}
		if !found {
			t.Fatalf("phase %s identifier %s is not published in %v", slot, phases[slot], got["resource.phase"])
		}
	}
	// The allocator key of these anchors is kind, parent and slot
	// (semgraph.Anchor.Key). Each mutation below is one of those fields,
	// and each must yield a different identifier than the published one:
	//   - slot: pvGraphOperator and pvGraphPhase pass the slot into Resolve
	//   - kind: pvGraphOperator passes resource.inverter, not resource.pv_system
	//   - parent: pvGraphPhase passes the inverter, not the PV system
	otherSlot, _, err := allocator.Resolve("resource.inverter", semgraph.Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: slot + "x", Evidence: []semreg.EvidenceRef{evidence}})
	if err != nil || otherSlot == inverter {
		t.Fatalf("slot mutation kept the inverter identifier %s (%v)", otherSlot, err)
	}
	assetSlot, _, err := allocator.Resolve("resource.inverter", semgraph.Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: assetSuffix, Evidence: []semreg.EvidenceRef{evidence}})
	if err != nil || assetSlot == inverter {
		t.Fatalf("asset-identifier slot kept the inverter identifier %s (%v)", assetSlot, err)
	}
	assetHashSlot := pvCoreHash("pv-operator-slot", []byte(asset))[:32]
	hashedAsset, _, err := allocator.Resolve("resource.inverter", semgraph.Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: assetHashSlot, Evidence: []semreg.EvidenceRef{pvGraphEvidence("sunspec.operator.slot", assetHashSlot)}})
	if err != nil || hashedAsset == inverter {
		t.Fatalf("asset-identifier digest slot kept the inverter identifier %s (%v)", hashedAsset, err)
	}
	fixed, _, err := allocator.Resolve("resource.inverter", semgraph.Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: "pv-inverter", Evidence: []semreg.EvidenceRef{pvGraphEvidence("sunspec.operator.slot", "pv-inverter")}})
	if err != nil || fixed == inverter {
		t.Fatalf("fixed slot kept the inverter identifier %s (%v)", fixed, err)
	}
	otherKind, _, err := allocator.Resolve("resource.pv_system", semgraph.Anchor{Class: semreg.AnchorFunction, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: slot, Evidence: []semreg.EvidenceRef{evidence}})
	if err != nil || otherKind == inverter {
		t.Fatalf("kind mutation kept the inverter identifier %s (%v)", otherKind, err)
	}
	otherParent, _, err := allocator.Resolve("resource.phase", semgraph.Anchor{Class: semreg.AnchorComponent, Basis: semreg.AnchorNativeSlot, Source: semgraph.SourceNativePosition, Parent: system, Slot: "L1", Evidence: []semreg.EvidenceRef{evidence}})
	if err != nil || otherParent == phases["L1"] {
		t.Fatalf("parent mutation kept the phase identifier %s (%v)", otherParent, err)
	}
	return inverter, phases, system
}

func pvGraphExpectSubjects(t *testing.T, snapshot semreg.Snapshot, inverter semreg.ResourceID, phases map[string]semreg.ResourceID, system semreg.ResourceID) {
	t.Helper()
	energyID, energyText := pvGraphDimension(t, snapshot, "pv.energy.generated")
	if energyID != "pv.dimension.system" || energyText != string(system) || energyText == string(inverter) {
		t.Fatalf("lifetime energy dimension = %s %s", energyID, energyText)
	}
	for _, factID := range []semreg.DefinitionID{"pv.ac.aggregate_active_power", "pv.ac.frequency", "pv.temperature.inverter", "pv.status.operating"} {
		id, text := pvGraphDimension(t, snapshot, factID)
		if id != "pv.dimension.inverter" || text != string(inverter) {
			t.Fatalf("%s dimension = %s %s", factID, id, text)
		}
	}
	var phaseTexts []string
	for _, envelope := range snapshot.Facts {
		if envelope.Key.FactID != "pv.ac.current" && envelope.Key.FactID != "pv.ac.voltage" {
			continue
		}
		if len(envelope.Key.Dimensions) != 1 || envelope.Key.Dimensions[0].ID != "pv.dimension.phase" || envelope.Key.Dimensions[0].Value.Text == nil {
			t.Fatalf("%s dimensions = %+v", envelope.Key.FactID, envelope.Key.Dimensions)
		}
		phaseTexts = append(phaseTexts, *envelope.Key.Dimensions[0].Value.Text)
	}
	sort.Strings(phaseTexts)
	want := []string{string(phases["L1"]), string(phases["L1"]), string(phases["L2"]), string(phases["L2"]), string(phases["L3"]), string(phases["L3"])}
	sort.Strings(want)
	if len(phaseTexts) != len(want) {
		t.Fatalf("phase dimensions = %v", phaseTexts)
	}
	for index := range want {
		if phaseTexts[index] != want[index] {
			t.Fatalf("phase dimensions = %v", phaseTexts)
		}
	}
	links := map[semreg.NativeBindingID]semreg.ResourceID{}
	for _, link := range snapshot.IdentityLinks {
		if link.Resource == nil {
			t.Fatal("identity link names no resource")
		}
		links[link.BindingID] = *link.Resource
	}
	energy := pvGraphCandidate(t, snapshot, "pv.energy.generated")
	if energy.BindingID == nil || links[*energy.BindingID] != system {
		t.Fatal("lifetime energy binding does not observe the PV system")
	}
	power := pvGraphCandidate(t, snapshot, "pv.ac.aggregate_active_power")
	if power.BindingID == nil || links[*power.BindingID] != inverter {
		t.Fatal("inverter fact binding does not observe the inverter")
	}
	for _, service := range snapshot.Services {
		if service.Resource == nil {
			t.Fatalf("service %s names no resource", service.Definition.ID)
		}
		switch service.Definition.ID {
		case "pv.service.system":
			if *service.Resource != system {
				t.Fatal("system service resource")
			}
		case "pv.service.inverter":
			if *service.Resource != inverter {
				t.Fatal("inverter service resource")
			}
		case "pv.service.phase":
			if *service.Resource != phases["L1"] && *service.Resource != phases["L2"] && *service.Resource != phases["L3"] {
				t.Fatal("phase service resource")
			}
		default:
			t.Fatalf("unexpected service %s", service.Definition.ID)
		}
	}
}

// pvGraphExpectSelectionIdentifier checks the presentation candidate. The
// production identifier is the hash of three fields, and each mutation is a
// different published candidate:
//   - asset: pvPublicationCandidate hashes draft.assetID
//   - binding: pvPublicationCandidate hashes the primary binding, including
//     when lifetime energy is observed through the PV-system binding
//   - native field: pvPublicationCandidate hashes mapping.nativeID
//
// A hash that also includes the allocated resource identifier must not equal
// the published candidate.
func pvGraphExpectSelectionIdentifier(t *testing.T, draft pvPublicationDraft, plain, graphed *pvPublicationView, inverter semreg.ResourceID) {
	t.Helper()
	const nativeID = "inverter.ac.power.active"
	want := pvGraphCandidateID(draft.assetID, draft.binding.BindingID, nativeID)
	if pvGraphSelected(t, graphed, "pv.ac.aggregate_active_power") != want || pvGraphSelected(t, plain, "pv.ac.aggregate_active_power") != want {
		t.Fatal("presentation candidate changed with the resource identifier")
	}
	for _, selection := range graphed.selections {
		if selection.SelectedCandidate == want && (selection.PolicyID != pvSelectionPolicyID || selection.PolicyVersion != pvSelectionPolicyVersion) {
			t.Fatalf("selection policy = %s %s", selection.PolicyID, selection.PolicyVersion)
		}
	}
	asset := pvGraphCandidateID("pv-asset-00000000000000000000000000000000", draft.binding.BindingID, nativeID)
	binding := pvGraphCandidateID(draft.assetID, "binding:semantic-pv:00000000000000000000000000000000", nativeID)
	native := pvGraphCandidateID(draft.assetID, draft.binding.BindingID, "inverter.ac.frequency")
	withResource := pvGraphCandidateID(draft.assetID, draft.binding.BindingID, nativeID+"\x00"+string(inverter))
	if asset == want || binding == want || native == want || withResource == want {
		t.Fatalf("identifier ignored a key field: asset=%s binding=%s native=%s resource=%s published=%s", asset, binding, native, withResource, want)
	}
}

func pvGraphCandidateID(asset semreg.AssetID, binding semreg.NativeBindingID, nativeID string) semreg.CandidateID {
	sum := pvCoreHash("pv-candidate", []byte(string(asset)+"\x00"+string(binding)+"\x00"+nativeID))
	return semreg.CandidateID("candidate:semantic-pv:" + sum[:32])
}

func pvGraphSelected(t *testing.T, view *pvPublicationView, factID semreg.DefinitionID) semreg.CandidateID {
	t.Helper()
	for _, selection := range view.selections {
		if selection.Key.FactID == factID {
			return selection.SelectedCandidate
		}
	}
	t.Fatalf("no selection for %s", factID)
	return ""
}

func pvGraphDimension(t *testing.T, snapshot semreg.Snapshot, factID semreg.DefinitionID) (semreg.DefinitionID, string) {
	t.Helper()
	for _, envelope := range snapshot.Facts {
		if envelope.Key.FactID != factID || len(envelope.Key.Dimensions) != 1 || envelope.Key.Dimensions[0].Value.Text == nil {
			continue
		}
		return envelope.Key.Dimensions[0].ID, *envelope.Key.Dimensions[0].Value.Text
	}
	t.Fatalf("missing %s", factID)
	return "", ""
}

func pvGraphCandidate(t *testing.T, snapshot semreg.Snapshot, factID semreg.DefinitionID) semreg.FactCandidate {
	t.Helper()
	for _, envelope := range snapshot.Facts {
		if envelope.Key.FactID == factID && len(envelope.Candidates) == 1 {
			return envelope.Candidates[0]
		}
	}
	t.Fatalf("missing candidate %s", factID)
	return semreg.FactCandidate{}
}

func pvGraphCommonCount(snapshot sunspec.ChainSnapshot) int {
	count := 0
	for _, occurrence := range snapshot.Occurrences() {
		if occurrence.ModelID() == 1 {
			count++
		}
	}
	return count
}

func pvGraphChainSnapshot(t *testing.T, words []uint16) sunspec.ChainSnapshot {
	t.Helper()
	registry, err := sunspec.NewStandardSunSpecDecoderRegistry(sunspec.ModelsRevisionV1)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := sunspec.NewChainPlan(sunspec.ChainPlanSpec{
		SchemaRevision: sunspec.ModelsRevisionV1,
		BaseCandidates: []uint16{sunSpecBaseAddress},
		Limits:         sunspec.ChainLimits{MaxTotalWords: sunSpecMaxTotalWords, MaxOccurrences: sunSpecMaxOccurrences},
		DecoderKeys:    registry.DecoderKeys(),
	})
	if err != nil {
		t.Fatal(err)
	}
	chain, offset := sunspec.NewChain(plan), 0
	var completed sunspec.ChainSnapshot
	for id := uint64(1); ; id++ {
		requests := chain.NextRequests()
		if len(requests) != 1 {
			t.Fatalf("fixture replay requests=%d", len(requests))
		}
		request := requests[0]
		count := int(request.WordCount())
		if offset+count > len(words) {
			t.Fatal("fixture replay exceeded immutable words")
		}
		view, err := modbusreg.NewLogicalViewSnapshot(modbusreg.LogicalViewRecord{
			LogicalViewID: id, WireResponseID: id + 100, PhysicalRequestID: id + 200,
			Endpoint: "fixture", ConnectionID: 4, Transport: modbusreg.TransportTCP, TransportGeneration: 5,
			UnitID: 1, RequestedFunction: modbusreg.FunctionReadHoldingRegisters, ReceivedFunction: modbusreg.FunctionReadHoldingRegisters,
			Table: modbusreg.HoldingRegisters, PhysicalOffset: request.Address(), PhysicalWordCount: request.WordCount(),
			AuthorizationScope: "fixture:pv-graph", PollGeneration: 1, DeadlineIdentity: 2,
			LogicalOffset: request.Address(), LogicalWordCount: request.WordCount(), SliceOffset: 0, SliceWordCount: request.WordCount(),
			Words: append([]uint16(nil), words[offset:offset+count]...), WireResponseBytes: []byte{byte(id)},
		})
		if err != nil {
			t.Fatal(err)
		}
		offset += count
		completed, err = chain.AdmitReplay(request, view)
		if err != nil {
			t.Fatal(err)
		}
		if len(completed.RawWords()) != 0 {
			return completed
		}
	}
}
