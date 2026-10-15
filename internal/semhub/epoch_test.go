package semhub

import (
	"context"
	"errors"
	"strings"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type otherValidator struct{ testValidator }

func (otherValidator) Pack() semreg.PackRef {
	return semreg.PackRef{ID: "helianthus.pack.semhubother", Version: "1.0.0"}
}
func (v otherValidator) Definitions() semreg.DefinitionIndex {
	index := v.testValidator.Definitions()
	index.Pack = v.Pack()
	for _, refs := range [][]semreg.DefinitionRef{index.Fields, index.Services, index.Capabilities} {
		for i := range refs {
			refs[i].Pack = v.Pack()
			refs[i].ID = semreg.DefinitionID(strings.Replace(string(refs[i].ID), "semhubtest", "semhubother", 1))
		}
	}
	return index
}

func TestDeclarationIsSingleSourceAndFrozen(t *testing.T) {
	hub := newTestHub(t)
	first := mustDeclare(t, hub, "asset:d", "source:d", "driver-d", nil)
	again := mustDeclare(t, hub, "asset:d", "source:d", "driver-d", nil)
	if first.asset != again.asset {
		t.Fatal("identical declaration constructed a second kernel")
	}
	if _, err := hub.Declare(AssetSpec{AssetID: "asset:d", SourceID: "source:other", Driver: "driver-d", Validators: []semreg.PackValidator{newTestValidator()}}); !errors.Is(err, ErrDeclarationConflict) {
		t.Fatalf("second source=%v", err)
	}
	if _, err := hub.Declare(AssetSpec{AssetID: "asset:d", SourceID: "source:d", Driver: "driver-d", Validators: []semreg.PackValidator{otherValidator{testValidator{pack: testPackRef}}}}); !errors.Is(err, ErrDeclarationConflict) {
		t.Fatalf("other validators=%v", err)
	}
	if _, err := hub.Declare(AssetSpec{AssetID: "same", SourceID: "same", Driver: "driver-d", Validators: []semreg.PackValidator{newTestValidator()}}); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("asset equal to source=%v", err)
	}
}

func TestCrossDriverCollisionsNameDriverLabelsOnly(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:shared", "source:growatt", "growatt_storage", nil)
	for name, spec := range map[string]AssetSpec{
		"same asset":            {AssetID: "asset:shared", SourceID: "source:tesla", Driver: "tesla_evse"},
		"same source":           {AssetID: "asset:tesla", SourceID: "source:growatt", Driver: "tesla_evse"},
		"asset is other source": {AssetID: "source:growatt", SourceID: "source:tesla", Driver: "tesla_evse"},
	} {
		spec.Validators = []semreg.PackValidator{newTestValidator()}
		_, err := hub.Declare(spec)
		var collision *CollisionError
		if !errors.As(err, &collision) || collision.Drivers != [2]string{"tesla_evse", "growatt_storage"} {
			t.Fatalf("%s: error=%v", name, err)
		}
		if strings.Contains(err.Error(), "asset:") || strings.Contains(err.Error(), "source:") {
			t.Fatalf("%s: collision error carries identity material: %v", name, err)
		}
	}
	if _, err := hub.AdmitSource(Admission{Source: "source:growatt", Driver: "tesla_evse", Legacy: LegacyTeslaEVSE, Epoch: "e", Generation: 1}); err == nil {
		t.Fatal("a second owner admitted a source")
	}
}

func TestClassDeclarationRefusalIsPerAssetAndEmitted(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "pv-asset-taken", "source:static", "growatt_storage", nil)
	class, err := hub.DeclareClass(ClassSpec{Name: "pv", Driver: "pv_sunspec", Validators: func() []semreg.PackValidator { return []semreg.PackValidator{newTestValidator()} }})
	if err != nil {
		t.Fatal(err)
	}
	subscription := hub.Subscribe()
	defer subscription.Close()
	if _, err := class.Declare("pv-asset-taken", "source:pv"); err == nil {
		t.Fatal("colliding dynamic asset declared")
	}
	event, err := subscription.Next(context.Background())
	if err != nil || event.Kind != EventDeclareRefused || len(event.Drivers) != 2 || event.Drivers[0] != "pv_sunspec" || event.Drivers[1] != "growatt_storage" || event.Asset != "" {
		t.Fatalf("refusal event=%+v err=%v", event, err)
	}
	if _, err := class.Declare("pv-asset-free", "source:pv"); err != nil {
		t.Fatalf("refusal was not per asset: %v", err)
	}
}

func TestNativeAdmissionAllocatesEpochsBeforeTheEndpointOpens(t *testing.T) {
	hubA, hubB := newTestHub(t), newTestHub(t)
	opened := 0
	start := func(hub *Hub, admission Admission) (*Handle, error) {
		handle, err := hub.AdmitSource(admission)
		if err != nil {
			return nil, err
		}
		opened++
		return handle, nil
	}
	if _, err := start(hubA, Admission{Source: "source:n", Driver: "native", Epoch: "caller-epoch"}); err == nil || opened != 0 {
		t.Fatalf("native caller epoch accepted or endpoint opened: %v/%d", err, opened)
	}
	if _, err := start(hubA, Admission{Source: " bad", Driver: "native"}); err == nil || opened != 0 {
		t.Fatalf("invalid source admitted or endpoint opened: %v/%d", err, opened)
	}
	a, err := start(hubA, Admission{Source: "source:n", Driver: "native"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := start(hubB, Admission{Source: "source:n", Driver: "native"})
	if err != nil {
		t.Fatal(err)
	}
	if a.Generation() != 1 || b.Generation() != 1 || a.Epoch() == b.Epoch() || ValidateOpaque(string(a.Epoch())) != nil {
		t.Fatalf("epochs=%q/%q generations=%d/%d", a.Epoch(), b.Epoch(), a.Generation(), b.Generation())
	}
	if _, err := hubA.AdmitSource(Admission{Source: "source:n", Driver: "native"}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("second native admission in one incarnation=%v", err)
	}
}

func TestLegacyAdmissionRules(t *testing.T) {
	hubA, hubB := newTestHub(t), newTestHub(t)
	admit := func(hub *Hub, epoch semreg.SourceEpochID, generation uint64) (*Handle, error) {
		return hub.AdmitSource(Admission{Source: "source:l", Driver: "growatt_storage", Legacy: LegacyGrowattStorage, Epoch: epoch, Generation: generation})
	}
	if _, err := admit(hubA, "epoch:l", 0); !errors.Is(err, ErrAdmission) {
		t.Fatalf("zero generation=%v", err)
	}
	if _, err := admit(hubA, " epoch", 1); !errors.Is(err, ErrAdmission) {
		t.Fatalf("invalid epoch=%v", err)
	}
	a, err := admit(hubA, "epoch:l", 7)
	if err != nil || a.Epoch() != "epoch:l" || a.Generation() != 7 {
		t.Fatalf("declared epoch not recorded verbatim: %+v/%v", a, err)
	}
	if b, err := admit(hubB, "epoch:l", 7); err != nil || b.Epoch() != "epoch:l" {
		t.Fatalf("epoch repeated across incarnations refused: %v", err)
	}
	mustDeclare(t, hubA, "asset:l", "source:l", "growatt_storage", nil)
	mustPublish(t, hubA, a, "asset:l", "legacy", 1, 1)
	if _, err := hubA.RetireEpoch(RetireRequest{Source: "source:l", Epoch: "epoch:l", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(2), Monotonic: testMono(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := admit(hubA, "epoch:l", 8); err == nil {
		t.Fatal("epoch repeated within an incarnation after retirement")
	}
}

func TestEveryLifecycleEventCarriesSourceEpochAndGeneration(t *testing.T) {
	f := newControlFixture(t)
	if _, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.Activate(f.handle, 2); err != nil {
		t.Fatal(err)
	}
	events, err := f.hub.Since(Cursor{Incarnation: f.hub.Incarnation()})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Source == "" || event.Epoch == "" || event.Generation == 0 {
			t.Fatalf("event without lifecycle identity: %+v", event)
		}
	}
	records := f.hub.journal.Records()
	if len(records) != 3 || records[0].Kind != EventEpochAdmitted || records[1].Kind != EventGenerationFenced || records[2].Kind != EventGenerationActivated {
		t.Fatalf("journal=%+v", records)
	}
}

func TestProvisionalSourceQualifiesIdempotentlyAndRetiresTogether(t *testing.T) {
	hub := newTestHub(t)
	provisional, err := hub.AdmitProvisional("pv_sunspec", LegacyPVSunSpec, "source-epoch:semantic-pv:x", 1)
	if err != nil {
		t.Fatal(err)
	}
	first, err := provisional.Qualify("source:semantic-pv:unit1")
	if err != nil {
		t.Fatal(err)
	}
	again, _ := provisional.Qualify("source:semantic-pv:unit1")
	second, _ := provisional.Qualify("source:semantic-pv:unit2")
	if first != again || second.Epoch() != first.Epoch() || second.Generation() != 1 || !first.Incarnation("a").First {
		t.Fatal("qualification is not idempotent per tuple or does not share the provisional epoch")
	}
	mustDeclare(t, hub, "pv-asset-1", "source:semantic-pv:unit1", "pv_sunspec", nil)
	mustPublish(t, hub, first, "pv-asset-1", "pv", 1, 1)
	before, _ := hub.Inspect("pv-asset-1")
	if err := provisional.Retire(); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Begin(first, "pv-asset-1"); errorID(err) != semreg.StaleSourceEpoch {
		t.Fatalf("retired provisional handle=%v", err)
	}
	if _, err := provisional.Qualify("source:semantic-pv:unit3"); errorID(err) != semreg.StaleSourceEpoch {
		t.Fatalf("qualify after retirement=%v", err)
	}
	after, _ := hub.Inspect("pv-asset-1")
	if after.Snapshot.SnapshotID != before.Snapshot.SnapshotID {
		t.Fatal("hub-level retirement changed the retained public state")
	}
}

// The legacy successor sequence: fence the current generation, admit a new
// epoch with generation current+1 on the same kernel, and retire the older
// epoch in the first batch of the new epoch.
func TestLegacySuccessorPublishesOnTheRetainedKernel(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:s", "source:s", "tesla_evse", nil)
	admit := func(epoch semreg.SourceEpochID, generation uint64) (*Handle, error) {
		return hub.AdmitSource(Admission{Source: "source:s", Driver: "tesla_evse", Legacy: LegacyTeslaEVSE, Epoch: epoch, Generation: generation})
	}
	first, err := admit("epoch:s1", 7)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, first, "asset:s", "successor", 1, 1)
	if _, err := admit("epoch:s2", 8); err == nil {
		t.Fatal("successor admitted before the fence")
	}
	if _, err := hub.Fence(testFence("source:s", "epoch:s1", 7, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := admit("epoch:s1", 8); err == nil {
		t.Fatal("successor reused the fenced epoch")
	}
	successor, err := admit("epoch:s2", 8)
	if err != nil {
		t.Fatal(err)
	}
	if successor.Incarnation("asset:s").First {
		t.Fatal("successor admission reported as first")
	}
	tx, err := hub.Begin(successor, "asset:s")
	if err != nil {
		t.Fatal(err)
	}
	if retirements := tx.SourceRetirements(); len(retirements) != 1 || retirements[0] != "epoch:s1" {
		t.Fatalf("pending retirements=%v", retirements)
	}
	batch := testBatch(t, tx, "successor", 2, 3)
	if _, _, err := tx.Apply(batch, testMono(3)); err != nil {
		t.Fatalf("first successor publication: %v", err)
	}
	if outcome, err := tx.Commit(nil); err != nil || outcome != Accepted {
		t.Fatalf("commit=%v/%v", outcome, err)
	}
	view, _ := hub.Inspect("asset:s")
	states := map[semreg.SourceEpochID]semreg.SourceState{}
	for _, descriptor := range view.Snapshot.Sources {
		states[descriptor.SourceEpochID] = descriptor.State
	}
	if states["epoch:s1"] != semreg.SourceRetired || states["epoch:s2"] != semreg.SourceCurrent {
		t.Fatalf("sources=%+v", view.Snapshot.Sources)
	}
	for _, service := range view.Snapshot.Services {
		if (service.SourceEpochID == "epoch:s1") != (service.Availability == semreg.AvailabilityWithdrawn) {
			t.Fatalf("service=%+v", service)
		}
	}
}

func TestRetiredProvisionalEpochAdmitsANewEpochForTheSameSource(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "pv-asset-r", "source:semantic-pv:r", "pv_sunspec", nil)
	first, err := hub.AdmitProvisional("pv_sunspec", LegacyPVSunSpec, "source-epoch:semantic-pv:one", 1)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := first.Qualify("source:semantic-pv:r")
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, handle, "pv-asset-r", "pv", 1, 1)
	second, _ := hub.AdmitProvisional("pv_sunspec", LegacyPVSunSpec, "source-epoch:semantic-pv:two", 1)
	if _, err := second.Qualify("source:semantic-pv:r"); err == nil {
		t.Fatal("new epoch admitted while the previous one is live")
	}
	if err := first.Retire(); err != nil {
		t.Fatal(err)
	}
	next, err := second.Qualify("source:semantic-pv:r")
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, next, "pv-asset-r", "pv", 2, 2)
	view, _ := hub.Inspect("pv-asset-r")
	if len(view.Snapshot.Sources) != 2 || view.Snapshot.Sources[0].State == view.Snapshot.Sources[1].State {
		t.Fatalf("sources=%+v", view.Snapshot.Sources)
	}
}

func TestDeclarationReprojectionCanBeBoundAfterDeclaration(t *testing.T) {
	hub := newTestHub(t)
	declaration := mustDeclare(t, hub, "asset:late", "source:late", "driver-late", nil)
	handle := admitLegacy(t, hub, "source:late", "driver-late", "epoch:late", 1)
	mustPublish(t, hub, handle, "asset:late", "late", 1, 1)
	calls := 0
	again := mustDeclare(t, hub, "asset:late", "source:late", "driver-late", nil)
	again.SetReprojection(func(ControlEvent) error { calls++; return nil })
	if declaration.Asset() != again.Asset() || declaration.Hub() != hub {
		t.Fatal("re-declaration returned another asset")
	}
	if _, err := hub.Fence(testFence("source:late", "epoch:late", 1, 2)); err != nil || calls != 1 {
		t.Fatalf("late reprojection calls=%d err=%v", calls, err)
	}
	class, _ := hub.DeclareClass(ClassSpec{Name: "c", Driver: "d", Validators: func() []semreg.PackValidator { return []semreg.PackValidator{newTestValidator()} }})
	if class.Hub() != hub {
		t.Fatal("class hub accessor")
	}
}
