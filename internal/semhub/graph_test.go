package semhub

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
)

// The graph fixture publishes against the accepted PV pack, which the element
// catalog covers, so a graph-enforced kernel can be built for it. The
// synthetic hub pack has no catalog entry.
var graphPVPack = semreg.PackRef{ID: "helianthus.pack.pv", Version: "1.0.0"}

const (
	graphInverter semreg.ResourceID = "resource:semhubtest:inverter-1"
	graphMissing  semreg.ResourceID = "resource:semhubtest:inverter-9"
)

func graphPVSpec(id semreg.AssetID, src semreg.SourceID, driver string, enforced bool, contributors ...ContributorSpec) AssetSpec {
	return AssetSpec{AssetID: id, SourceID: src, Driver: driver, Validators: []semreg.PackValidator{pv.New()}, Graph: enforced, Contributors: contributors}
}

// graphInverterResource is an operator-configured inverter resource.
func graphInverterResource() semreg.Resource {
	slot := "inverter-1"
	return semreg.Resource{ResourceID: graphInverter, Kind: "resource.inverter", Anchor: semreg.ResourceAnchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Slot: &slot, Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.anchor")}}, State: semreg.ResourceCurrent, Origin: graphTestOrigin(), Revision: "1"}
}

// graphPVBatch builds one PV publication from the staged transaction state:
// the source, binding and identity link on the first batch of an incarnation,
// optional resource upserts, and one frequency fact whose inverter dimension
// names subject. Under enforcement (linked) the identity link names the
// inverter resource the binding observes; without it the link names none.
// The candidate is the publishing source's own.
func graphPVBatch(t *testing.T, tx *Tx, at int64, linked bool, subject semreg.ResourceID, resources ...semreg.Resource) semreg.PublicationBatch {
	t.Helper()
	inc := tx.Incarnation()
	current, exists := tx.Current()
	generation := semreg.Uint64(strconv.FormatUint(inc.Generation, 10))
	binding, err := NativeBindingID("pv_graph", inc.Asset, inc.Source, "semhubtest.profile.v1", inc.Epoch, inc.Generation)
	if err != nil {
		t.Fatal(err)
	}
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
	}
	hasBinding, hasSource := false, false
	for _, existing := range current.Bindings {
		hasBinding = hasBinding || existing.BindingID == binding
	}
	for _, existing := range current.Sources {
		hasSource = hasSource || existing.SourceID == inc.Source && existing.SourceEpochID == inc.Epoch
	}
	epoch, source := inc.Epoch, inc.Source
	evidence := testEvidence("semhubtest.observation")
	candidateID := semreg.CandidateID("candidate:pv_graph:" + idHash("candidate", string(inc.Asset), string(binding), string(subject)))
	revision := semreg.Uint64("1")
	for _, envelope := range current.Facts {
		for _, candidate := range envelope.Candidates {
			if candidate.CandidateID == candidateID {
				revision = incrementUint(candidate.Revision)
			}
		}
	}
	text := string(subject)
	frequency := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "5", Exponent10: 1}, Unit: "unit.hertz"}}
	fact := semreg.FactCandidate{CandidateID: candidateID, Key: semreg.FactKey{PackID: graphPVPack.ID, PackVersion: graphPVPack.Version, FactID: "pv.ac.frequency", Dimensions: []semreg.Dimension{{ID: "pv.dimension.inverter", Value: semreg.Value{Kind: semreg.ValueText, Text: &text}}}}, Value: &frequency,
		Quality:         semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:           semreg.Times{ReceivedAt: testWall(at), ReceiptMonotonic: testMono(at), EvaluatedAt: testWall(at), EvaluateMonotonic: testMono(at)},
		FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:semhub-test", Version: "1.0.0", FreshForNS: "60000000000", RetainForNS: "300000000000", MaxWallUncertaintyNS: "0"},
		BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
		Origin:   semreg.OriginRef{OriginID: semreg.OriginID("origin:pv_graph:" + idHash("origin", string(candidateID))), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}},
		Evidence: []semreg.EvidenceRef{evidence}, Revision: revision}
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:pv_graph:" + idHash("batch", string(inc.Asset), string(source), string(epoch), string(generation), string(tx.NextSequence()))), AssetID: inc.Asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: tx.NextSequence(), ExpectedSemanticRevision: expected, ObservedAt: testWall(at),
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: tx.SourceRetirements(), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, ResourceUpserts: resources, FactUpserts: []semreg.FactCandidate{fact}, FactWithdrawals: []semreg.CandidateID{},
		ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	if !hasSource {
		batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "semhubtest_protocol", ProfileID: "semhubtest.profile.v1", ProfileVersion: "1", RegistryEvidence: testEvidence("semhubtest.registry"), StartedAt: testWall(0), State: semreg.SourceCurrent, Revision: "1"}}
	}
	if !hasBinding {
		batch.BindingUpserts = []semreg.NativeBinding{{BindingID: binding, AssetID: inc.Asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: testEvidence("semhubtest.native"), State: semreg.BindingCurrent, Revision: "1"}}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{{AssetID: inc.Asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: "1"}}
		if linked {
			resource := graphInverter
			batch.IdentityLinkUpserts[0].Resource = &resource
		}
	}
	sealGraphTestBatch(t, &batch)
	return batch
}

// publishPV runs one PV transaction and returns the Apply error, or the
// applied batch after a successful commit. A refused batch is aborted.
func publishPV(t *testing.T, hub *Hub, handle *Handle, id semreg.AssetID, at int64, linked bool, subject semreg.ResourceID, resources ...semreg.Resource) (semreg.PublicationBatch, error) {
	t.Helper()
	tx, err := hub.Begin(handle, id)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	batch := graphPVBatch(t, tx, at, linked, subject, resources...)
	if _, _, err := tx.Apply(batch, testMono(at)); err != nil {
		return batch, err
	}
	if outcome, err := tx.Commit(nil); err != nil || outcome != Accepted {
		t.Fatalf("commit=%v/%v", outcome, err)
	}
	return batch, nil
}

// requireUnchanged asserts that a refused batch left the snapshot, the
// cursor and the event stream as they were.
func requireUnchanged(t *testing.T, hub *Hub, id semreg.AssetID, before InspectView, cursor Cursor) {
	t.Helper()
	after, err := hub.Inspect(id)
	if err != nil || after.Present != before.Present || string(after.Canonical) != string(before.Canonical) || hub.Cursor() != cursor {
		t.Fatalf("refused batch changed the snapshot or cursor (err=%v)", err)
	}
	if events, err := hub.Since(cursor); err != nil || len(events) != 0 {
		t.Fatalf("refused batch emitted %d events (%v)", len(events), err)
	}
}

func graphStateOf(t *testing.T, hub *Hub) GraphState {
	t.Helper()
	state, err := hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	return state
}

// An asset declared with Graph gets a graph-enforced kernel built with the
// embedded catalog, whose digest is SemReg's pinned digest. It accepts a
// resource upsert with a fact about that resource and refuses a fact whose
// resource dimension names no current resource.
func TestHubGraphEnforcedAssetPublishesResourcesUnderTheCatalog(t *testing.T) {
	hub := newTestHub(t)
	if _, err := hub.Declare(graphPVSpec("asset:pv", "source:pv", "driver-pv", true)); err != nil {
		t.Fatalf("graph-enforced declaration: %v", err)
	}
	state := graphStateOf(t, hub)
	if state.CatalogDigest != graph.PinnedDigest || len(state.Assets) != 1 || state.Assets[0] != (AssetGraph{Asset: "asset:pv", Driver: "driver-pv", Enforcement: GraphEnforced}) {
		t.Fatalf("graph state=%+v want digest %q", state, graph.PinnedDigest)
	}
	embedded, err := graph.Embedded()
	if err != nil || embedded.Digest() != state.CatalogDigest {
		t.Fatalf("embedded catalog digest differs from the reported one (%v)", err)
	}
	handle := admitLegacy(t, hub, "source:pv", "driver-pv", "epoch:pv", 1)

	// A fact about a resource that does not exist yet is refused on the first
	// batch, and the refusal leaves nothing.
	before, cursor := InspectView{}, hub.Cursor()
	if _, err := publishPV(t, hub, handle, "asset:pv", 1, true, graphInverter); errorID(err) != semreg.DanglingReference {
		t.Fatalf("fact about an absent resource=%v", err)
	}
	requireUnchanged(t, hub, "asset:pv", before, cursor)

	if _, err := publishPV(t, hub, handle, "asset:pv", 1, true, graphInverter, graphInverterResource()); err != nil {
		t.Fatalf("resource upsert with a fact about it: %v", err)
	}
	view, err := hub.Inspect("asset:pv")
	if err != nil || len(view.Snapshot.Resources) != 1 || view.Snapshot.Resources[0].ResourceID != graphInverter {
		t.Fatalf("resources=%+v (%v)", view.Snapshot.Resources, err)
	}

	cursor = hub.Cursor()
	if _, err := publishPV(t, hub, handle, "asset:pv", 2, true, graphMissing); errorID(err) != semreg.DanglingReference {
		t.Fatalf("fact about a resource that is not current=%v", err)
	}
	requireUnchanged(t, hub, "asset:pv", view, cursor)
	if _, err := publishPV(t, hub, handle, "asset:pv", 3, true, graphInverter); err != nil {
		t.Fatalf("later fact about the current resource: %v", err)
	}
}

// An asset declared without Graph is built exactly as before: the hub's
// canonical bytes equal those of a kernel constructed without the catalog
// that applies the same batches, whether the flag is omitted or false, and
// every graph member is refused.
func TestHubGraphNotEnforcedAssetBehavesAsBefore(t *testing.T) {
	for name, spec := range map[string]AssetSpec{
		"omitted": {AssetID: "asset:pv", SourceID: "source:pv", Driver: "driver-pv", Validators: []semreg.PackValidator{pv.New()}},
		"false":   graphPVSpec("asset:pv", "source:pv", "driver-pv", false),
	} {
		t.Run(name, func(t *testing.T) {
			hub := newTestHub(t)
			if _, err := hub.Declare(spec); err != nil {
				t.Fatal(err)
			}
			if state := graphStateOf(t, hub); len(state.Assets) != 1 || state.Assets[0].Enforcement != GraphNotEnforced {
				t.Fatalf("graph state=%+v", state)
			}
			handle := admitLegacy(t, hub, "source:pv", "driver-pv", "epoch:pv", 1)
			cursor := hub.Cursor()
			if _, err := publishPV(t, hub, handle, "asset:pv", 1, false, graphInverter, graphInverterResource()); errorID(err) != semreg.MembershipMissing {
				t.Fatalf("resource upsert on a kernel without the catalog=%v", err)
			}
			requireUnchanged(t, hub, "asset:pv", InspectView{}, cursor)

			reference, err := semreg.NewPublicationKernel("asset:pv", pv.New())
			if err != nil {
				t.Fatal(err)
			}
			for at := int64(1); at <= 2; at++ {
				// Today's text dimension value is not checked against a resource.
				batch, err := publishPV(t, hub, handle, "asset:pv", at, false, graphMissing)
				if err != nil {
					t.Fatalf("publication without graph members: %v", err)
				}
				if _, _, err := reference.Apply(batch, testMono(at)); err != nil {
					t.Fatal(err)
				}
				_, canonical, _ := reference.Current()
				view, err := hub.Inspect("asset:pv")
				if err != nil || string(view.Canonical) != string(canonical) {
					t.Fatalf("batch %d: hub bytes differ from a kernel without the catalog (%v)", at, err)
				}
			}
		})
	}
	for name, change := range graphMemberVariants() {
		t.Run("explicitly false refuses "+name, func(t *testing.T) {
			hub := newTestHub(t)
			if _, err := hub.Declare(AssetSpec{AssetID: "asset:a", SourceID: "source:a", Driver: "driver-a", Validators: []semreg.PackValidator{newTestValidator()}, Graph: false}); err != nil {
				t.Fatal(err)
			}
			handle := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 1)
			if err := applyGraphMember(t, hub, handle, "asset:a", change, 1, 1); errorID(err) != semreg.MembershipMissing {
				t.Fatalf("%s=%v", name, err)
			}
		})
	}
}

// A declaration that asks for enforcement with a pack the catalog does not
// cover fails at declaration with the kernel's error and claims nothing; the
// same refusal applies through a class, which reports it as a refused
// declaration.
func TestHubGraphUncoveredPackFailsAtDeclaration(t *testing.T) {
	hub := newTestHub(t)
	claims := claimsSnapshot(hub)
	cursor := hub.Cursor()
	spec := AssetSpec{AssetID: "asset:a", SourceID: "source:a", Driver: "driver-a", Validators: []semreg.PackValidator{newTestValidator()}, Graph: true}
	if _, err := hub.Declare(spec); errorID(err) != semreg.MembershipMissing {
		t.Fatalf("uncovered pack under enforcement=%v", err)
	}
	// A covered pack beside an uncovered one is still refused.
	mixed := spec
	mixed.Validators = []semreg.PackValidator{pv.New(), newTestValidator()}
	if _, err := hub.Declare(mixed); errorID(err) != semreg.MembershipMissing {
		t.Fatalf("partly uncovered packs under enforcement=%v", err)
	}
	if !sameClaims(claims, claimsSnapshot(hub)) || len(hub.Sources()) != 0 || len(graphStateOf(t, hub).Assets) != 0 || hub.Cursor() != cursor {
		t.Fatal("a refused declaration left a claim, a source, an asset or an event")
	}

	class, err := hub.DeclareClass(ClassSpec{Name: "class-a", Driver: "driver-a", Validators: func() []semreg.PackValidator { return []semreg.PackValidator{newTestValidator()} }, Graph: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := class.Declare("asset:a", "source:a"); errorID(err) != semreg.MembershipMissing {
		t.Fatalf("class declaration of an uncovered pack under enforcement=%v", err)
	}
	events, err := hub.Since(cursor)
	if err != nil || len(events) != 1 || events[0].Kind != EventDeclareRefused {
		t.Fatalf("class refusal events=%+v (%v)", events, err)
	}

	// The refusal reserved nothing: the asset is declared without enforcement.
	spec.Graph = false
	if _, err := hub.Declare(spec); err != nil {
		t.Fatalf("declaration without enforcement after the refusal: %v", err)
	}
}

// Enforcement is fixed at declaration: a repeated declaration that asks for
// the other state is a conflict, through the hub and through a class, and
// the asset keeps the state it was declared with.
func TestHubGraphCannotBeSwitchedAfterDeclaration(t *testing.T) {
	for _, declared := range []bool{false, true} {
		hub := newTestHub(t)
		first, err := hub.Declare(graphPVSpec("asset:pv", "source:pv", "driver-pv", declared))
		if err != nil {
			t.Fatal(err)
		}
		again, err := hub.Declare(graphPVSpec("asset:pv", "source:pv", "driver-pv", declared))
		if err != nil || again.asset != first.asset {
			t.Fatalf("identical redeclaration=%v", err)
		}
		if _, err := hub.Declare(graphPVSpec("asset:pv", "source:pv", "driver-pv", !declared)); !errors.Is(err, ErrDeclarationConflict) {
			t.Fatalf("declared %v, redeclared %v=%v", declared, !declared, err)
		}
		class, err := hub.DeclareClass(ClassSpec{Name: "class-pv", Driver: "driver-pv", Validators: func() []semreg.PackValidator { return []semreg.PackValidator{pv.New()} }, Graph: !declared})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := class.Declare("asset:pv", "source:pv"); !errors.Is(err, ErrDeclarationConflict) {
			t.Fatalf("class redeclaration with the other state=%v", err)
		}
		want := GraphNotEnforced
		if declared {
			want = GraphEnforced
		}
		if state := graphStateOf(t, hub); len(state.Assets) != 1 || state.Assets[0].Enforcement != want {
			t.Fatalf("graph state after refused switch=%+v", state)
		}
	}
}

// A contributor of a graph-enforced asset publishes through the asset's
// kernel, so the same rules apply: its resource upsert is accepted and its
// fact about a resource that is not current is refused. On an asset declared
// without enforcement the contributor's resource upsert is refused.
func TestHubGraphContributorPublishesUnderTheSameRules(t *testing.T) {
	peer := ContributorSpec{SourceID: "source:peer", Driver: "driver-peer", Validators: []semreg.PackValidator{pv.New()}}
	for _, enforced := range []bool{true, false} {
		hub := newTestHub(t)
		if _, err := hub.Declare(graphPVSpec("asset:pv", "source:pv", "driver-pv", enforced, peer)); err != nil {
			t.Fatal(err)
		}
		contributor := mustAdmit(t, hub, "source:peer", "driver-peer")
		cursor := hub.Cursor()
		_, err := publishPV(t, hub, contributor, "asset:pv", 1, enforced, graphInverter, graphInverterResource())
		if !enforced {
			if errorID(err) != semreg.MembershipMissing {
				t.Fatalf("contributor resource upsert without enforcement=%v", err)
			}
			requireUnchanged(t, hub, "asset:pv", InspectView{}, cursor)
			continue
		}
		if err != nil {
			t.Fatalf("contributor resource upsert under enforcement: %v", err)
		}
		view, err := hub.Inspect("asset:pv")
		if err != nil || len(view.Snapshot.Resources) != 1 {
			t.Fatalf("resources=%d (%v)", len(view.Snapshot.Resources), err)
		}
		cursor = hub.Cursor()
		if _, err := publishPV(t, hub, contributor, "asset:pv", 2, enforced, graphMissing); errorID(err) != semreg.DanglingReference {
			t.Fatalf("contributor fact about a resource that is not current=%v", err)
		}
		requireUnchanged(t, hub, "asset:pv", view, cursor)

		// The owner publishes about the contributor's resource under the same
		// kernel.
		owner := mustAdmit(t, hub, "source:pv", "driver-pv")
		if _, err := publishPV(t, hub, owner, "asset:pv", 3, enforced, graphInverter); err != nil {
			t.Fatalf("owner fact about the contributor's resource: %v", err)
		}
		if _, err := publishPV(t, hub, owner, "asset:pv", 4, enforced, graphMissing); errorID(err) != semreg.DanglingReference {
			t.Fatalf("owner fact about a resource that is not current=%v", err)
		}
	}
}

// GraphState lists every declared asset, in bytewise asset order, with its
// driver and its enforcement, whether it was declared directly or through a
// class, and reports the catalog digest once.
func TestHubGraphStateListsEveryDeclaredAsset(t *testing.T) {
	hub := newTestHub(t)
	if state := graphStateOf(t, hub); state.CatalogDigest != graph.PinnedDigest || state.Assets == nil || len(state.Assets) != 0 {
		t.Fatalf("empty hub graph state=%+v", state)
	}
	mustDeclare(t, hub, "asset:d", "source:d", "driver-d", nil)
	if _, err := hub.Declare(graphPVSpec("asset:c", "source:c", "driver-c", true)); err != nil {
		t.Fatal(err)
	}
	enforced, err := hub.DeclareClass(ClassSpec{Name: "class-b", Driver: "driver-b", Validators: func() []semreg.PackValidator { return []semreg.PackValidator{pv.New()} }, Graph: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enforced.Declare("asset:b", "source:b"); err != nil {
		t.Fatal(err)
	}
	plain, err := hub.DeclareClass(ClassSpec{Name: "class-a", Driver: "driver-a", Validators: func() []semreg.PackValidator { return []semreg.PackValidator{pv.New()} }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Declare("asset:a", "source:a"); err != nil {
		t.Fatal(err)
	}
	state := graphStateOf(t, hub)
	want := []AssetGraph{
		{Asset: "asset:a", Driver: "driver-a", Enforcement: GraphNotEnforced},
		{Asset: "asset:b", Driver: "driver-b", Enforcement: GraphEnforced},
		{Asset: "asset:c", Driver: "driver-c", Enforcement: GraphEnforced},
		{Asset: "asset:d", Driver: "driver-d", Enforcement: GraphNotEnforced},
	}
	if state.CatalogDigest != graph.PinnedDigest || len(state.Assets) != len(want) {
		t.Fatalf("graph state=%+v", state)
	}
	for index := range want {
		if state.Assets[index] != want[index] {
			t.Fatalf("asset %d=%+v want %+v", index, state.Assets[index], want[index])
		}
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(encoded), string(graph.PinnedDigest)) != 1 || strings.Count(string(encoded), `"enforced"`) != 2 || strings.Count(string(encoded), `"not_enforced"`) != 2 {
		t.Fatalf("graph state encoding=%s", encoded)
	}
	// The returned list is detached from the hub.
	state.Assets[0].Enforcement = GraphEnforced
	if graphStateOf(t, hub).Assets[0].Enforcement != GraphNotEnforced {
		t.Fatal("graph state shares storage with the hub")
	}
}
