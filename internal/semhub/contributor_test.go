package semhub

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// The shared-asset fixture: owner A and declared contributor B on one asset;
// C is an admitted source that is not attached to it.
const (
	sharedAsset  semreg.AssetID  = "asset:shared"
	sharedOwner  semreg.SourceID = "source:a"
	sharedPeer   semreg.SourceID = "source:b"
	sharedOther  semreg.SourceID = "source:c"
	sharedDriver                 = "driver-a"
	peerDriver                   = "driver-b"
	otherDriver                  = "driver-c"
)

func sharedSpec(contributors ...ContributorSpec) AssetSpec {
	return AssetSpec{AssetID: sharedAsset, SourceID: sharedOwner, Driver: sharedDriver, Validators: []semreg.PackValidator{newTestValidator()}, Contributors: contributors}
}

func peerSpec() ContributorSpec {
	return ContributorSpec{SourceID: sharedPeer, Driver: peerDriver, Validators: []semreg.PackValidator{newTestValidator()}}
}

func mustDeclareShared(t *testing.T, hub *Hub) *Declaration {
	t.Helper()
	declaration, err := hub.Declare(sharedSpec(peerSpec()))
	if err != nil {
		t.Fatal(err)
	}
	return declaration
}

func mustAdmit(t *testing.T, hub *Hub, src semreg.SourceID, driver string) *Handle {
	t.Helper()
	handle, err := hub.AdmitSource(Admission{Source: src, Driver: driver})
	if err != nil {
		t.Fatalf("admit %s: %v", src, err)
	}
	return handle
}

// claimsSnapshot copies the hub identifier space.
func claimsSnapshot(hub *Hub) map[string]idOwner {
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	out := make(map[string]idOwner, len(hub.ids))
	for id, owner := range hub.ids {
		out[id] = owner
	}
	return out
}

func sameClaims(left, right map[string]idOwner) bool {
	if len(left) != len(right) {
		return false
	}
	for id, owner := range left {
		if right[id] != owner {
			return false
		}
	}
	return true
}

// mustKind returns the one identifier of kind in ids.
func mustKind(t *testing.T, ids map[string]Kind, kind Kind) string {
	t.Helper()
	for id, got := range ids {
		if got == kind {
			return id
		}
	}
	t.Fatalf("no %s identifier", kind)
	return ""
}

func collisionDrivers(err error) ([2]string, bool) {
	var collision *CollisionError
	if errors.As(err, &collision) {
		return collision.Drivers, true
	}
	return [2]string{}, false
}

// contributorBatch builds one batch of the transaction's source with empty
// collections, then lets mutate fill it, and seals it.
func contributorBatch(t *testing.T, tx *Tx, at int64, mutate func(*semreg.PublicationBatch)) semreg.PublicationBatch {
	t.Helper()
	inc := tx.Incarnation()
	expected := semreg.Uint64("0")
	if current, exists := tx.Current(); exists {
		expected = current.Revisions.Semantic
	}
	generation := strconv.FormatUint(inc.Generation, 10)
	sequence := tx.NextSequence()
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:contributor:" + idHash("contributor-batch", string(inc.Asset), string(inc.Source), string(inc.Epoch), generation, string(sequence))), AssetID: inc.Asset, SourceID: inc.Source, SourceEpochID: inc.Epoch, DriverGeneration: semreg.Uint64(generation), Sequence: sequence, ExpectedSemanticRevision: expected, ObservedAt: testWall(at),
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: []semreg.SourceEpochID{}, BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{},
		ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	mutate(&batch)
	digest, err := batch.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	batch.BatchDigest = digest
	return batch
}

// publishBatch runs one transaction whose batch is built by contributorBatch.
func publishBatch(t *testing.T, hub *Hub, handle *Handle, id semreg.AssetID, at int64, mutate func(*semreg.PublicationBatch)) (Outcome, error) {
	t.Helper()
	tx, err := hub.Begin(handle, id)
	if err != nil {
		return "", err
	}
	defer tx.Abort()
	if _, _, err := tx.Apply(contributorBatch(t, tx, at, mutate), testMono(at)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	return tx.Commit(nil)
}

// observedOf returns the observed candidate the source published on asset.
func observedOf(t *testing.T, hub *Hub, id semreg.AssetID, src semreg.SourceID) semreg.FactCandidate {
	t.Helper()
	snapshot := currentSnapshot(t, hub, id)
	for _, envelope := range snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			if candidate.BindingID != nil && candidate.Origin.SourceID != nil && *candidate.Origin.SourceID == src {
				return candidate
			}
		}
	}
	t.Fatalf("no observed candidate of %s", src)
	return semreg.FactCandidate{}
}

func currentSnapshot(t *testing.T, hub *Hub, id semreg.AssetID) semreg.Snapshot {
	t.Helper()
	hub.mu.RLock()
	kernel := hub.assets[id].kernel
	hub.mu.RUnlock()
	snapshot, _, ok := kernel.Current()
	if !ok {
		t.Fatalf("asset %s has no snapshot", id)
	}
	return snapshot
}

// derivedCandidate is one inferred candidate without a binding, derived from
// one observed input.
func derivedCandidate(id semreg.CandidateID, input semreg.FactCandidate, revision semreg.Uint64, value, at int64) semreg.FactCandidate {
	evidence := testEvidence("semhubtest.derivation")
	dimension := "derived"
	number := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: testDecimal(value), Unit: "unit.watt"}}
	path := semreg.SourcePathRef{BindingID: *input.BindingID, SourceID: *input.Origin.SourceID, SourceEpochID: *input.SourceEpochID, DriverGeneration: *input.DriverGeneration}
	return semreg.FactCandidate{CandidateID: id, Key: semreg.FactKey{PackID: testPackRef.ID, PackVersion: testPackRef.Version, FactID: "semhubtest.value", Dimensions: []semreg.Dimension{{ID: "semhubtest.dimension.asset", Value: semreg.Value{Kind: semreg.ValueText, Text: &dimension}}}}, Value: &number,
		Quality:         semreg.Quality{Assertion: semreg.AssertionInferred, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:           semreg.Times{ReceivedAt: testWall(at), ReceiptMonotonic: testMono(at), EvaluatedAt: testWall(at), EvaluateMonotonic: testMono(at)},
		FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:semhub-test", Version: "1.0.0", FreshForNS: "60000000000", RetainForNS: "300000000000", MaxWallUncertaintyNS: "0"},
		Origin:          semreg.OriginRef{OriginID: semreg.OriginID("origin:derived:" + idHash("origin", string(id))), Kind: semreg.OriginDerived, Evidence: []semreg.EvidenceRef{evidence}},
		Evidence:        []semreg.EvidenceRef{evidence},
		Derivation:      &semreg.Derivation{Algorithm: "semhubtest.algorithm.copy", Version: "1.0.0", Inputs: []semreg.DerivationInput{{CandidateID: input.CandidateID, CandidateRevision: input.Revision, SourcePaths: []semreg.SourcePathRef{path}}}, Evidence: []semreg.EvidenceRef{}},
		Revision:        revision}
}

// TestContributorUndeclaredClaimantKeepsTodaysErrors asserts again that a
// second claimant the owner did not name keeps today's refusals, on a plain
// asset and on an asset with a declared contributor.
func TestContributorUndeclaredClaimantKeepsTodaysErrors(t *testing.T) {
	plain := newSharedTestHub(t)
	mustDeclare(t, plain, sharedAsset, sharedOwner, sharedDriver, nil)
	shared := newSharedTestHub(t)
	mustDeclareShared(t, shared)
	for name, hub := range map[string]*Hub{"plain": plain, "shared": shared} {
		before := claimsSnapshot(hub)
		_, err := hub.Declare(AssetSpec{AssetID: sharedAsset, SourceID: sharedOther, Driver: otherDriver, Validators: []semreg.PackValidator{newTestValidator()}})
		if drivers, ok := collisionDrivers(err); !ok || drivers != [2]string{otherDriver, sharedDriver} {
			t.Fatalf("%s: undeclared second driver=%v", name, err)
		}
		if _, err := hub.Declare(AssetSpec{AssetID: sharedAsset, SourceID: "source:a2", Driver: sharedDriver, Validators: []semreg.PackValidator{newTestValidator()}}); !errors.Is(err, ErrDeclarationConflict) {
			t.Fatalf("%s: same driver with another source=%v", name, err)
		}
		if _, err := hub.Declare(AssetSpec{AssetID: sharedAsset, SourceID: sharedOwner, Driver: sharedDriver, Validators: []semreg.PackValidator{otherValidator{testValidator{pack: testPackRef}}}}); !errors.Is(err, ErrDeclarationConflict) {
			t.Fatalf("%s: same driver with other packs=%v", name, err)
		}
		if !sameClaims(before, claimsSnapshot(hub)) {
			t.Fatalf("%s: a refused declaration changed the claims", name)
		}
	}
	// A declared contributor is not the owner: declaring the asset as its own
	// single-source asset stays a collision with the owner's driver.
	_, err := shared.Declare(AssetSpec{AssetID: sharedAsset, SourceID: sharedPeer, Driver: peerDriver, Validators: []semreg.PackValidator{newTestValidator()}})
	if drivers, ok := collisionDrivers(err); !ok || drivers != [2]string{peerDriver, sharedDriver} {
		t.Fatalf("contributor redeclaring as owner=%v", err)
	}
}

// TestContributorProductionHubRefusesContributorList proves the production
// guard: a hub built by New refuses a contributor list, declares nothing and
// claims nothing.
func TestContributorProductionHubRefusesContributorList(t *testing.T) {
	hub, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = hub.Declare(sharedSpec(peerSpec()))
	if !errors.Is(err, ErrDeclarationConflict) || !strings.Contains(err.Error(), "several sources per asset are not enabled") {
		t.Fatalf("production contributor list=%v", err)
	}
	hub.mu.RLock()
	assets, sources, ids := len(hub.assets), len(hub.sources), len(hub.ids)
	hub.mu.RUnlock()
	if assets != 0 || sources != 0 || ids != 0 {
		t.Fatalf("refused declaration left state: assets=%d sources=%d ids=%d", assets, sources, ids)
	}
	if _, err := hub.Begin(nil, sharedAsset); !errors.Is(err, ErrUnknownAsset) {
		t.Fatalf("refused asset is declared: %v", err)
	}
	// Nothing was claimed: another driver may take the contributor source, and
	// the owner may still declare the asset with its single source.
	mustAdmit(t, hub, sharedPeer, otherDriver)
	if _, err := hub.Declare(sharedSpec()); err != nil {
		t.Fatalf("single-source declaration after the refusal: %v", err)
	}
	if _, err := hub.Declare(sharedSpec(ContributorSpec{SourceID: "source:d", Driver: "driver-d", Validators: []semreg.PackValidator{newTestValidator()}})); !errors.Is(err, ErrDeclarationConflict) {
		t.Fatalf("production redeclaration with a contributor=%v", err)
	}
}

// TestContributorDeclarationRules covers the declaration rules of a
// contributor list on a hub built for package tests.
func TestContributorDeclarationRules(t *testing.T) {
	refusals := map[string]struct {
		spec     AssetSpec
		conflict bool
	}{
		"contributor is the owner source": {sharedSpec(ContributorSpec{SourceID: sharedOwner, Driver: peerDriver, Validators: []semreg.PackValidator{newTestValidator()}}), true},
		"duplicate contributor":           {sharedSpec(peerSpec(), peerSpec()), true},
		"duplicate contributor source":    {sharedSpec(peerSpec(), ContributorSpec{SourceID: sharedPeer, Driver: otherDriver, Validators: []semreg.PackValidator{newTestValidator()}}), true},
		"contributor source is the asset": {sharedSpec(ContributorSpec{SourceID: semreg.SourceID(sharedAsset), Driver: peerDriver, Validators: []semreg.PackValidator{newTestValidator()}}), false},
		"contributor without driver":      {sharedSpec(ContributorSpec{SourceID: sharedPeer, Validators: []semreg.PackValidator{newTestValidator()}}), false},
		"contributor without packs":       {sharedSpec(ContributorSpec{SourceID: sharedPeer, Driver: peerDriver}), false},
		"contributor with a nil pack":     {sharedSpec(ContributorSpec{SourceID: sharedPeer, Driver: peerDriver, Validators: []semreg.PackValidator{nil}}), false},
		"contributor with invalid source": {sharedSpec(ContributorSpec{SourceID: "", Driver: peerDriver, Validators: []semreg.PackValidator{newTestValidator()}}), false},
	}
	for name, refusal := range refusals {
		hub := newSharedTestHub(t)
		_, err := hub.Declare(refusal.spec)
		if refusal.conflict && !errors.Is(err, ErrDeclarationConflict) || !refusal.conflict && !errors.Is(err, ErrInvalidIdentity) {
			t.Fatalf("%s: error=%v", name, err)
		}
		if len(claimsSnapshot(hub)) != 0 {
			t.Fatalf("%s: refused declaration claimed identifiers", name)
		}
	}

	// A contributor source claimed by another driver stays a collision, and
	// the refusal declares and claims nothing new.
	hub := newSharedTestHub(t)
	mustDeclare(t, hub, "asset:x", sharedPeer, "driver-x", nil)
	before := claimsSnapshot(hub)
	_, err := hub.Declare(sharedSpec(peerSpec()))
	if drivers, ok := collisionDrivers(err); !ok || drivers != [2]string{peerDriver, "driver-x"} {
		t.Fatalf("contributor source of another driver=%v", err)
	}
	if !sameClaims(before, claimsSnapshot(hub)) {
		t.Fatal("collision changed the claims")
	}

	// Accepted: the contributor source is claimed for its driver and the asset
	// joins the contributor source's asset set.
	hub = newSharedTestHub(t)
	third := ContributorSpec{SourceID: sharedOther, Driver: otherDriver, Validators: []semreg.PackValidator{newTestValidator()}}
	first, err := hub.Declare(sharedSpec(peerSpec(), third))
	if err != nil {
		t.Fatal(err)
	}
	claims := claimsSnapshot(hub)
	for id, want := range map[string]idOwner{string(sharedAsset): {sharedDriver, KindAsset}, string(sharedOwner): {sharedDriver, KindSource}, string(sharedPeer): {peerDriver, KindSource}, string(sharedOther): {otherDriver, KindSource}} {
		if claims[id] != want {
			t.Fatalf("claim %s=%+v want %+v", id, claims[id], want)
		}
	}
	hub.mu.RLock()
	for _, src := range []semreg.SourceID{sharedOwner, sharedPeer, sharedOther} {
		if _, ok := hub.sources[src].assets[sharedAsset]; !ok {
			t.Errorf("asset missing from the asset set of %s", src)
		}
	}
	hub.mu.RUnlock()
	if first.Source() != sharedOwner {
		t.Fatalf("declaration source=%s", first.Source())
	}

	// A repeated identical declaration returns the existing one, in any
	// contributor order.
	for name, spec := range map[string]AssetSpec{"same order": sharedSpec(peerSpec(), third), "reversed order": sharedSpec(third, peerSpec())} {
		again, err := hub.Declare(spec)
		if err != nil || again.asset != first.asset {
			t.Fatalf("%s: repeated identical declaration=%v", name, err)
		}
	}

	// Every field of a contributor is part of the declaration: varying the
	// source, the driver or the packs of one contributor, or the list itself,
	// is a conflict that claims nothing.
	before = claimsSnapshot(hub)
	variants := map[string]AssetSpec{
		"contributor source": sharedSpec(ContributorSpec{SourceID: "source:b2", Driver: peerDriver, Validators: []semreg.PackValidator{newTestValidator()}}, third),
		"contributor driver": sharedSpec(ContributorSpec{SourceID: sharedPeer, Driver: "driver-b2", Validators: []semreg.PackValidator{newTestValidator()}}, third),
		"contributor added":  sharedSpec(peerSpec(), third, ContributorSpec{SourceID: "source:e", Driver: "driver-e", Validators: []semreg.PackValidator{newTestValidator()}}),
		"contributor gone":   sharedSpec(peerSpec()),
		"no contributors":    sharedSpec(),
	}
	for name, spec := range variants {
		if _, err := hub.Declare(spec); !errors.Is(err, ErrDeclarationConflict) {
			t.Fatalf("%s: repeated declaration with another contributor list=%v", name, err)
		}
	}
	if !sameClaims(before, claimsSnapshot(hub)) {
		t.Fatal("a refused repeated declaration changed the claims")
	}

	// The packs of a contributor are compared too, within the asset's packs.
	other := otherValidator{testValidator{pack: testPackRef}}
	twoPacks := func(peer ...semreg.PackValidator) AssetSpec {
		spec := sharedSpec(ContributorSpec{SourceID: sharedPeer, Driver: peerDriver, Validators: peer})
		spec.Validators = []semreg.PackValidator{newTestValidator(), other}
		return spec
	}
	hub = newSharedTestHub(t)
	first, err = hub.Declare(twoPacks(newTestValidator()))
	if err != nil {
		t.Fatal(err)
	}
	if again, err := hub.Declare(twoPacks(newTestValidator())); err != nil || again.asset != first.asset {
		t.Fatalf("repeated identical two-pack declaration=%v", err)
	}
	for name, spec := range map[string]AssetSpec{"other pack": twoPacks(other), "more packs": twoPacks(newTestValidator(), other)} {
		if _, err := hub.Declare(spec); !errors.Is(err, ErrDeclarationConflict) {
			t.Fatalf("contributor %s: repeated declaration=%v", name, err)
		}
	}
}

// TestContributorPacksMustBeSubsetOfAssetPacks refuses a contributor whose
// packs the asset's kernel registry does not hold.
func TestContributorPacksMustBeSubsetOfAssetPacks(t *testing.T) {
	other := otherValidator{testValidator{pack: testPackRef}}
	cases := []struct {
		name        string
		asset, peer []semreg.PackValidator
		accepted    bool
	}{
		{"foreign pack", []semreg.PackValidator{newTestValidator()}, []semreg.PackValidator{other}, false},
		{"superset", []semreg.PackValidator{newTestValidator()}, []semreg.PackValidator{newTestValidator(), other}, false},
		{"equal", []semreg.PackValidator{newTestValidator(), other}, []semreg.PackValidator{newTestValidator(), other}, true},
		{"proper subset", []semreg.PackValidator{newTestValidator(), other}, []semreg.PackValidator{other}, true},
	}
	for _, c := range cases {
		hub := newSharedTestHub(t)
		_, err := hub.Declare(AssetSpec{AssetID: sharedAsset, SourceID: sharedOwner, Driver: sharedDriver, Validators: c.asset, Contributors: []ContributorSpec{{SourceID: sharedPeer, Driver: peerDriver, Validators: c.peer}}})
		if c.accepted {
			if err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
			continue
		}
		if !errors.Is(err, ErrDeclarationConflict) {
			t.Fatalf("%s: error=%v", c.name, err)
		}
		hub.mu.RLock()
		assets, ids := len(hub.assets), len(hub.ids)
		hub.mu.RUnlock()
		if assets != 0 || ids != 0 {
			t.Fatalf("%s: refused declaration left assets=%d ids=%d", c.name, assets, ids)
		}
	}
}

// TestContributorBatchStaysInDeclaredPacks refuses a contributor batch that
// publishes a pack the contributor did not declare, while the owner may.
func TestContributorBatchStaysInDeclaredPacks(t *testing.T) {
	hub := newSharedTestHub(t)
	other := otherValidator{testValidator{pack: testPackRef}}
	if _, err := hub.Declare(AssetSpec{AssetID: sharedAsset, SourceID: sharedOwner, Driver: sharedDriver, Validators: []semreg.PackValidator{newTestValidator(), other},
		Contributors: []ContributorSpec{{SourceID: sharedPeer, Driver: peerDriver, Validators: []semreg.PackValidator{other}}}}); err != nil {
		t.Fatal(err)
	}
	a := mustAdmit(t, hub, sharedOwner, sharedDriver)
	b := mustAdmit(t, hub, sharedPeer, peerDriver)
	tx, err := hub.Begin(b, sharedAsset)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = tx.Apply(testBatch(t, tx, "fam_b", 10, 1), testMono(1))
	tx.Abort()
	if !errors.Is(err, ErrTransaction) {
		t.Fatalf("contributor batch in an undeclared pack=%v", err)
	}
	mustPublish(t, hub, a, sharedAsset, "fam_a", 20, 2)
}

// TestContributorPublishesOnSharedAsset admits a declared contributor and
// publishes through it; a source the owner did not name is refused at Begin.
func TestContributorPublishesOnSharedAsset(t *testing.T) {
	hub := newSharedTestHub(t)
	mustDeclareShared(t, hub)
	mustDeclare(t, hub, "asset:c", sharedOther, otherDriver, nil)
	a := mustAdmit(t, hub, sharedOwner, sharedDriver)
	b := mustAdmit(t, hub, sharedPeer, peerDriver)
	c := mustAdmit(t, hub, sharedOther, otherDriver)

	tx, err := hub.Begin(b, sharedAsset)
	if err != nil {
		t.Fatalf("contributor Begin: %v", err)
	}
	if _, _, err := tx.Apply(testBatch(t, tx, "fam_b", 10, 1), testMono(1)); err != nil {
		t.Fatalf("contributor Apply: %v", err)
	}
	if outcome, err := tx.Commit(nil); err != nil || outcome != Accepted {
		t.Fatalf("contributor Commit=%v/%v", outcome, err)
	}
	mustPublish(t, hub, a, sharedAsset, "fam_a", 20, 2)
	mustPublish(t, hub, b, sharedAsset, "fam_b", 11, 3)

	sources := map[semreg.SourceID]bool{}
	for _, descriptor := range currentSnapshot(t, hub, sharedAsset).Sources {
		sources[descriptor.SourceID] = true
	}
	if len(sources) != 2 || !sources[sharedOwner] || !sources[sharedPeer] {
		t.Fatalf("shared asset sources=%v", sources)
	}

	cursor := hub.Cursor()
	if _, err := hub.Begin(c, sharedAsset); !errors.Is(err, ErrTransaction) || !strings.Contains(err.Error(), "asset is declared with another source") {
		t.Fatalf("undeclared source Begin=%v", err)
	}
	if _, err := hub.BeginGeneration(c, sharedAsset, 2); !errors.Is(err, ErrTransaction) || !strings.Contains(err.Error(), "asset is declared with another source") {
		t.Fatalf("undeclared source BeginGeneration=%v", err)
	}
	if hub.Cursor() != cursor {
		t.Fatal("a refused Begin emitted an event")
	}
	// The contributor's handle stays refused on an asset that does not name it.
	if _, err := hub.Begin(b, "asset:c"); !errors.Is(err, ErrTransaction) {
		t.Fatalf("contributor Begin on an asset that does not name it=%v", err)
	}
	mustPublish(t, hub, c, "asset:c", "fam_c", 1, 4)
}

// TestContributorNestedClaimsFollowPublishingDriver claims a contributor's
// binding, service and capability identifiers for the contributor's driver.
func TestContributorNestedClaimsFollowPublishingDriver(t *testing.T) {
	hub := newSharedTestHub(t)
	mustDeclareShared(t, hub)
	mustDeclare(t, hub, "asset:a-only", sharedOwner, sharedDriver, nil)
	a := mustAdmit(t, hub, sharedOwner, sharedDriver)
	b := mustAdmit(t, hub, sharedPeer, peerDriver)
	mustPublish(t, hub, a, sharedAsset, "fam_a", 1, 1)
	mustPublish(t, hub, b, sharedAsset, "fam_b", 2, 2)

	nested := func(family string, handle *Handle) map[string]Kind {
		generation := strconv.FormatUint(handle.Generation(), 10)
		binding, err := NativeBindingID(family, sharedAsset, handle.Source(), "semhubtest.profile.v1", handle.Epoch(), handle.Generation())
		if err != nil {
			t.Fatal(err)
		}
		service, err := NativeID(KindService, family, string(sharedAsset), "semhubtest.service.main", string(handle.Epoch()), generation)
		if err != nil {
			t.Fatal(err)
		}
		capability, err := NativeID(KindCapability, family, string(sharedAsset), "semhubtest.capability.read", string(handle.Epoch()), generation)
		if err != nil {
			t.Fatal(err)
		}
		return map[string]Kind{string(binding): KindBinding, service: KindService, capability: KindCapability}
	}
	claims := claimsSnapshot(hub)
	for driver, ids := range map[string]map[string]Kind{sharedDriver: nested("fam_a", a), peerDriver: nested("fam_b", b)} {
		for id, kind := range ids {
			if claims[id] != (idOwner{driver: driver, kind: kind}) {
				t.Fatalf("claim of %s %s=%+v want driver %s", kind, id, claims[id], driver)
			}
		}
	}

	// B upserting its own already claimed service again is checked against
	// B's driver, not the asset's, and accepted.
	var peerService semreg.ServiceInstance
	for _, service := range currentSnapshot(t, hub, sharedAsset).Services {
		if string(service.InstanceID) == mustKind(t, nested("fam_b", b), KindService) {
			peerService = service
		}
	}
	if peerService.InstanceID == "" {
		t.Fatal("the contributor's service is missing from the snapshot")
	}
	if outcome, err := publishBatch(t, hub, b, sharedAsset, 3, func(batch *semreg.PublicationBatch) {
		batch.ServiceUpserts = []semreg.ServiceInstance{peerService}
	}); err != nil || outcome != Accepted {
		t.Fatalf("contributor upserting its own service again=%v/%v", outcome, err)
	}
	if !sameClaims(claims, claimsSnapshot(hub)) {
		t.Fatal("a repeated upsert of a claimed service changed the claims")
	}

	// A's later batch upserting B's binding identifier, on an asset whose
	// kernel has never seen it, is refused by the hub claim.
	mustPublish(t, hub, a, "asset:a-only", "fam_a", 3, 3)
	peerBinding := semreg.NativeBindingID(mustKind(t, nested("fam_b", b), KindBinding))
	cursor, records := hub.Cursor(), len(hub.journal.Records())
	before := currentSnapshot(t, hub, "asset:a-only")
	_, err := publishBatch(t, hub, a, "asset:a-only", 4, func(batch *semreg.PublicationBatch) {
		batch.BindingUpserts = []semreg.NativeBinding{{BindingID: peerBinding, AssetID: batch.AssetID, SourceID: batch.SourceID, SourceEpochID: batch.SourceEpochID, DriverGeneration: batch.DriverGeneration, NativeResource: testEvidence("semhubtest.native"), State: semreg.BindingCurrent, Revision: "1"}}
	})
	if drivers, ok := collisionDrivers(err); !ok || drivers != [2]string{sharedDriver, peerDriver} {
		t.Fatalf("owner upserting the contributor's binding=%v", err)
	}
	if hub.Cursor() != cursor || len(hub.journal.Records()) != records || currentSnapshot(t, hub, "asset:a-only").Revisions != before.Revisions {
		t.Fatal("a refused commit changed the hub")
	}
}

// TestContributorInferredCandidateBelongsToCreator refuses another driver's
// revision or withdrawal of an inferred candidate on a shared asset.
func TestContributorInferredCandidateBelongsToCreator(t *testing.T) {
	hub := newSharedTestHub(t)
	mustDeclareShared(t, hub)
	a := mustAdmit(t, hub, sharedOwner, sharedDriver)
	b := mustAdmit(t, hub, sharedPeer, peerDriver)
	mustPublish(t, hub, a, sharedAsset, "fam_a", 1, 1)
	mustPublish(t, hub, b, sharedAsset, "fam_b", 2, 2)
	input := observedOf(t, hub, sharedAsset, sharedOwner)
	const derived semreg.CandidateID = "candidate:derived:shared"

	upsert := func(revision semreg.Uint64, value, at int64) func(*semreg.PublicationBatch) {
		return func(batch *semreg.PublicationBatch) {
			batch.FactUpserts = []semreg.FactCandidate{derivedCandidate(derived, input, revision, value, at)}
		}
	}
	withdraw := func(batch *semreg.PublicationBatch) { batch.FactWithdrawals = []semreg.CandidateID{derived} }
	accepted := func(name string, handle *Handle, at int64, mutate func(*semreg.PublicationBatch)) {
		t.Helper()
		if outcome, err := publishBatch(t, hub, handle, sharedAsset, at, mutate); err != nil || outcome != Accepted {
			t.Fatalf("%s=%v/%v", name, outcome, err)
		}
	}
	refused := func(name string, at int64, mutate func(*semreg.PublicationBatch)) {
		t.Helper()
		cursor, records := hub.Cursor(), len(hub.journal.Records())
		before := currentSnapshot(t, hub, sharedAsset)
		claims := claimsSnapshot(hub)
		_, err := publishBatch(t, hub, b, sharedAsset, at, mutate)
		if drivers, ok := collisionDrivers(err); !ok || drivers != [2]string{peerDriver, sharedDriver} {
			t.Fatalf("%s=%v", name, err)
		}
		if hub.Cursor() != cursor || len(hub.journal.Records()) != records || currentSnapshot(t, hub, sharedAsset).Revisions != before.Revisions || !sameClaims(claims, claimsSnapshot(hub)) {
			t.Fatalf("%s: a refused commit changed the hub", name)
		}
	}

	accepted("owner creates", a, 3, upsert("1", 5, 3))
	hub.mu.RLock()
	creator, recorded := hub.assets[sharedAsset].inferred[derived], len(hub.assets[sharedAsset].inferred)
	hub.mu.RUnlock()
	// Only the candidate without a binding is recorded; the observed
	// candidates of A and B are left to the kernel's per-source ownership.
	if creator != sharedDriver || recorded != 1 {
		t.Fatalf("inferred creator=%q records=%d", creator, recorded)
	}
	refused("contributor revision", 4, upsert("2", 6, 4))
	refused("contributor withdrawal", 5, withdraw)
	accepted("owner revision", a, 6, upsert("2", 7, 6))
	refused("contributor revision after the owner's", 7, upsert("3", 8, 7))
	accepted("owner withdrawal", a, 8, withdraw)
}

// TestContributorSingleSourceAssetRecordsNoInferredCreator keeps a
// single-source asset free of inferred-record ownership.
func TestContributorSingleSourceAssetRecordsNoInferredCreator(t *testing.T) {
	hub := newSharedTestHub(t)
	mustDeclare(t, hub, sharedAsset, sharedOwner, sharedDriver, nil)
	a := mustAdmit(t, hub, sharedOwner, sharedDriver)
	mustPublish(t, hub, a, sharedAsset, "fam_a", 1, 1)
	input := observedOf(t, hub, sharedAsset, sharedOwner)
	if outcome, err := publishBatch(t, hub, a, sharedAsset, 2, func(batch *semreg.PublicationBatch) {
		batch.FactUpserts = []semreg.FactCandidate{derivedCandidate("candidate:derived:plain", input, "1", 5, 2)}
	}); err != nil || outcome != Accepted {
		t.Fatalf("single-source inferred upsert=%v/%v", outcome, err)
	}
	hub.mu.RLock()
	recorded := len(hub.assets[sharedAsset].inferred)
	hub.mu.RUnlock()
	if recorded != 0 {
		t.Fatalf("single-source asset recorded %d inferred creators", recorded)
	}
}
