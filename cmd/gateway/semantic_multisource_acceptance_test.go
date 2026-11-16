package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semgraph"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semselect"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
)

// Two synthetic families observe one unit: the owner family holds the asset
// and the contributor family is declared on it through a pairing. Neither
// names a device family, a model or a unit.
const (
	pairingAsset        semreg.AssetID  = "asset:pairtest:unit"
	pairingOwnerSource  semreg.SourceID = "source:pairtest:owner"
	pairingPeerSource   semreg.SourceID = "source:pairtest:peer"
	pairingOwnerDriver                  = "pairtest_owner"
	pairingPeerDriver                   = "pairtest_peer"
	pairingThirdDriver                  = "pairtest_third"
	pairingResourceKind                 = semreg.DefinitionID("resource.inverter")
	pairingUnitSlot                     = "unit-1"
	pairingWallBase                     = int64(1_800_000_000)
	pairingFactID                       = semreg.DefinitionID("pv.ac.aggregate_active_power")
)

var pairingPVPack = pv.New().Pack()

// pairingHash is a domain-separated digest of parts, so the fixture holds no
// literal digest.
func pairingHash(parts ...string) string {
	sum := sha256.Sum256([]byte("pairtest\x00" + strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func pairingEvidence(kind semreg.DefinitionID, label string) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: semreg.DefinitionID(pairingPVPack.ID), Kind: kind, Digest: semreg.Digest("sha256:" + pairingHash("evidence", string(kind), label)), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

func pairingWall(at int64) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt((pairingWallBase+at)*1_000_000_000, 10)), ClockID: "wall.utc", UncertaintyNS: "0"}
}

func pairingMono(at int64) semreg.MonotonicPoint {
	return semreg.MonotonicPoint{ClockEpochID: "clock:pairtest", Nanoseconds: semreg.Uint64(strconv.FormatInt(at*1_000_000_000, 10))}
}

func pairingWatts(watts int64) semreg.Decimal {
	exponent := int32(0)
	for watts != 0 && watts%10 == 0 {
		watts /= 10
		exponent++
	}
	return semreg.Decimal{Coefficient: strconv.FormatInt(watts, 10), Exponent10: exponent}
}

// pairingUnitAnchor is the operator declaration both families resolve for
// the one unit they observe; the evidence names the observing path only.
func pairingUnitAnchor(path string) semgraph.Anchor {
	return semgraph.Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: pairingUnitSlot, Evidence: []semreg.EvidenceRef{pairingEvidence("pairtest.anchor", path)}}
}

// pairingDeclaration is the declared pair: the owner's asset with its
// contributor, each with a share of the asset's bounds.
func pairingDeclaration() semanticPairing {
	return semanticPairing{
		Owner:       semhub.AssetSpec{AssetID: pairingAsset, SourceID: pairingOwnerSource, Driver: pairingOwnerDriver, Validators: []semreg.PackValidator{pv.New()}, Graph: true},
		OwnerBudget: semanticCapacityBudget{SourceDescriptors: 16, Bindings: 64, IdentityLinks: 64},
		Contributors: []semanticPairingContributor{{
			Source: pairingPeerSource, Driver: pairingPeerDriver, Validators: []semreg.PackValidator{pv.New()},
			Budget: semanticCapacityBudget{SourceDescriptors: 16, Bindings: 64, IdentityLinks: 64},
		}},
	}
}

// pairingBatch builds one publication of a synthetic family from the staged
// transaction state: on the first batch of an incarnation its source, its
// binding and its identity link naming the unit's resource; the resource
// records it is given; and one active-power candidate about that resource.
func pairingBatch(t *testing.T, tx *semhub.Tx, family string, at, watts int64, unit semreg.ResourceID, resources ...semreg.Resource) (semreg.PublicationBatch, semreg.CandidateID) {
	t.Helper()
	inc := tx.Incarnation()
	current, exists := tx.Current()
	generation := semreg.Uint64(strconv.FormatUint(inc.Generation, 10))
	binding, err := semhub.NativeBindingID(family, inc.Asset, inc.Source, "pairtest.profile.v1", inc.Epoch, inc.Generation)
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
	evidence := pairingEvidence("pairtest.observation", family)
	candidateID := semreg.CandidateID("candidate:" + family + ":" + pairingHash("candidate", string(inc.Asset), string(binding), string(unit))[:32])
	revision := semreg.Uint64("1")
	for _, envelope := range current.Facts {
		for _, candidate := range envelope.Candidates {
			if candidate.CandidateID == candidateID {
				next, _ := strconv.ParseUint(string(candidate.Revision), 10, 64)
				revision = semreg.Uint64(strconv.FormatUint(next+1, 10))
			}
		}
	}
	subject := string(unit)
	power := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: pairingWatts(watts), Unit: "unit.watt"}}
	fact := semreg.FactCandidate{CandidateID: candidateID, Key: semreg.FactKey{PackID: pairingPVPack.ID, PackVersion: pairingPVPack.Version, FactID: pairingFactID, Dimensions: []semreg.Dimension{{ID: "pv.dimension.inverter", Value: semreg.Value{Kind: semreg.ValueText, Text: &subject}}}}, Value: &power,
		Quality:         semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:           semreg.Times{ReceivedAt: pairingWall(at), ReceiptMonotonic: pairingMono(at), EvaluatedAt: pairingWall(at), EvaluateMonotonic: pairingMono(at)},
		FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:pairtest", Version: "1.0.0", FreshForNS: "60000000000", RetainForNS: "300000000000", MaxWallUncertaintyNS: "0"},
		BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
		Origin:   semreg.OriginRef{OriginID: semreg.OriginID("origin:" + family + ":" + pairingHash("origin", string(candidateID))[:32]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}},
		Evidence: []semreg.EvidenceRef{evidence}, Revision: revision}
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:" + family + ":" + pairingHash("batch", string(inc.Asset), string(source), string(epoch), string(generation), string(tx.NextSequence()))[:32]), AssetID: inc.Asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: tx.NextSequence(), ExpectedSemanticRevision: expected, ObservedAt: pairingWall(at),
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: tx.SourceRetirements(), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, ResourceUpserts: resources, FactUpserts: []semreg.FactCandidate{fact}, FactWithdrawals: []semreg.CandidateID{},
		ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	if !hasSource {
		batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "pairtest_protocol", ProfileID: "pairtest.profile.v1", ProfileVersion: "1", RegistryEvidence: pairingEvidence("pairtest.registry", family), StartedAt: pairingWall(0), State: semreg.SourceCurrent, Revision: "1"}}
	}
	if !hasBinding {
		resource := unit
		batch.BindingUpserts = []semreg.NativeBinding{{BindingID: binding, AssetID: inc.Asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: pairingEvidence("pairtest.native", family), State: semreg.BindingCurrent, Revision: "1"}}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{{AssetID: inc.Asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: "1", Resource: &resource}}
	}
	digest, err := batch.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	batch.BatchDigest = digest
	if err := batch.Validate(); err != nil {
		t.Fatalf("%s batch is not a valid record: %v", family, err)
	}
	return batch, candidateID
}

// pairingPublish runs one transaction of a synthetic family and returns its
// candidate identifier, or the first refusal.
func pairingPublish(t *testing.T, hub *semhub.Hub, handle *semhub.Handle, family string, at, watts int64, unit semreg.ResourceID, resources ...semreg.Resource) (semreg.CandidateID, error) {
	t.Helper()
	tx, err := hub.Begin(handle, pairingAsset)
	if err != nil {
		return "", err
	}
	defer tx.Abort()
	batch, candidate := pairingBatch(t, tx, family, at, watts, unit, resources...)
	if _, _, err := tx.Apply(batch, pairingMono(at)); err != nil {
		return "", err
	}
	outcome, err := tx.Commit(nil)
	if err != nil {
		return "", err
	}
	if outcome != semhub.Accepted {
		t.Fatalf("%s commit outcome=%v", family, outcome)
	}
	return candidate, nil
}

// pairingPresented captures the asset and returns, per fact key, the watts
// of the candidate the owner-first policy presents, together with the
// captured snapshot.
func pairingPresented(t *testing.T, hub *semhub.Hub, at int64) (map[string]semreg.CandidateID, int64, semreg.Snapshot) {
	t.Helper()
	captured, err := hub.Capture(context.Background(), []semreg.AssetID{pairingAsset}, func() time.Time { return time.Unix(pairingWallBase+at, 0) })
	if err != nil {
		t.Fatal(err)
	}
	asset := captured.Assets[0]
	if !asset.PublicAdmitted || asset.Snapshot == nil || asset.Evaluation == nil {
		t.Fatalf("paired asset not admitted: %+v", asset)
	}
	selected, total := map[string]semreg.CandidateID{}, int64(0)
	for _, envelope := range asset.Snapshot.Facts {
		selection, ok, err := semselect.Present(*asset.Snapshot, *asset.Evaluation, envelope, pairingOwnerSource)
		if err != nil || !ok {
			t.Fatalf("presentation of %s: ok=%v err=%v", envelope.Key.FactID, ok, err)
		}
		selected[string(envelope.Key.FactID)] = selection.SelectedCandidate
		for _, candidate := range envelope.Candidates {
			if candidate.CandidateID != selection.SelectedCandidate {
				continue
			}
			number := candidate.Value.Quantity.Number
			value, err := strconv.ParseInt(number.Coefficient, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			for exponent := int32(0); exponent < number.Exponent10; exponent++ {
				value *= 10
			}
			total += value
		}
	}
	return selected, total, *asset.Snapshot
}

func pairingAwaitAdmitted(t *testing.T, hub *semhub.Hub) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !hub.Admitted(pairingAsset) {
		if time.Now().After(deadline) {
			t.Fatal("the paired asset did not settle")
		}
		time.Sleep(time.Millisecond)
	}
}

// One unit observed through two paths, at gateway level: the declared pair
// forms one asset, both families resolve the unit to one resource, the
// snapshot holds that resource once with one binding and one identity link per
// path, the quantity both publish is one fact key with two candidates of which
// the owner-first policy presents exactly one, a fence of the contributor
// keeps the resource and the owner's facts, and an undeclared second claimant
// stays a collision.
func TestSemanticMultiSourceAcceptanceOneUnitThroughTwoPaths(t *testing.T) {
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := composeDeclaredPairings(hub, []semanticPairing{pairingDeclaration()}); err != nil {
		t.Fatalf("declared pairing refused: %v", err)
	}
	state, err := hub.GraphState()
	if err != nil || len(state.Assets) != 1 || state.Assets[0].Asset != pairingAsset || state.Assets[0].Driver != pairingOwnerDriver || state.Assets[0].Enforcement != semhub.GraphEnforced {
		t.Fatalf("paired asset declaration=%+v (%v)", state, err)
	}
	if sources := hub.Sources(); len(sources) != 2 || sources[0].Source != pairingOwnerSource || sources[0].Driver != pairingOwnerDriver || sources[1].Source != pairingPeerSource || sources[1].Driver != pairingPeerDriver {
		t.Fatalf("paired sources=%+v", sources)
	}

	// Both paths resolve the one unit under one anchor key.
	allocator, err := semgraph.NewAllocator(semgraph.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	unit, anchor, err := allocator.Resolve(pairingResourceKind, pairingUnitAnchor(pairingOwnerDriver))
	if err != nil {
		t.Fatal(err)
	}
	ownerKey, err := pairingUnitAnchor(pairingOwnerDriver).Key(pairingResourceKind)
	if err != nil {
		t.Fatal(err)
	}
	peerKey, err := pairingUnitAnchor(pairingPeerDriver).Key(pairingResourceKind)
	if err != nil || peerKey != ownerKey {
		t.Fatalf("anchor keys differ by path: %+v %+v (%v)", ownerKey, peerKey, err)
	}
	peerUnit, _, err := allocator.Resolve(pairingResourceKind, pairingUnitAnchor(pairingPeerDriver))
	if err != nil || peerUnit != unit {
		t.Fatalf("the contributor's path resolved %q, want %q (%v)", peerUnit, unit, err)
	}
	for _, path := range []string{pairingOwnerDriver, pairingPeerDriver} {
		if home, err := allocator.Home(unit, pairingAsset); err != nil || home != pairingAsset {
			t.Fatalf("home through %s=%q (%v)", path, home, err)
		}
	}
	resource := semreg.Resource{ResourceID: unit, Kind: pairingResourceKind, Anchor: anchor, State: semreg.ResourceCurrent, Origin: semreg.OriginRef{OriginID: "origin:pairtest:operator", Kind: semreg.OriginOperator, Evidence: []semreg.EvidenceRef{pairingEvidence("pairtest.operator", "pairing")}}, Revision: "1"}

	owner, err := hub.AdmitSource(semhub.Admission{Source: pairingOwnerSource, Driver: pairingOwnerDriver})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := hub.AdmitSource(semhub.Admission{Source: pairingPeerSource, Driver: pairingPeerDriver})
	if err != nil {
		t.Fatal(err)
	}
	ownerCandidate, err := pairingPublish(t, hub, owner, pairingOwnerDriver, 1, 1500, unit, resource)
	if err != nil {
		t.Fatalf("owner publication: %v", err)
	}
	peerCandidate, err := pairingPublish(t, hub, peer, pairingPeerDriver, 2, 1490, unit)
	if err != nil {
		t.Fatalf("contributor publication: %v", err)
	}

	view, err := hub.Inspect(pairingAsset)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := view.Snapshot
	if len(snapshot.Resources) != 1 || snapshot.Resources[0].ResourceID != unit {
		t.Fatalf("resources=%+v, want the one unit", snapshot.Resources)
	}
	if len(snapshot.Bindings) != 2 {
		t.Fatalf("bindings=%+v, want one per path", snapshot.Bindings)
	}
	bySource := map[semreg.SourceID]semreg.NativeBindingID{}
	for _, binding := range snapshot.Bindings {
		bySource[binding.SourceID] = binding.BindingID
	}
	if bySource[pairingOwnerSource] == "" || bySource[pairingPeerSource] == "" {
		t.Fatalf("bindings by source=%+v", bySource)
	}
	if len(snapshot.IdentityLinks) != 2 {
		t.Fatalf("identity links=%+v, want one per binding", snapshot.IdentityLinks)
	}
	linked := map[semreg.NativeBindingID]bool{}
	for _, link := range snapshot.IdentityLinks {
		if link.Resource == nil || *link.Resource != unit {
			t.Fatalf("identity link of %s names %v, want the unit", link.BindingID, link.Resource)
		}
		linked[link.BindingID] = true
	}
	if !linked[bySource[pairingOwnerSource]] || !linked[bySource[pairingPeerSource]] {
		t.Fatalf("identity links do not cover both bindings: %+v", linked)
	}
	if len(snapshot.Facts) != 1 || snapshot.Facts[0].Key.FactID != pairingFactID || len(snapshot.Facts[0].Candidates) != 2 {
		t.Fatalf("facts=%+v, want one key with two candidates", snapshot.Facts)
	}

	// The policy presents one candidate per key: the owner's. Summing every
	// candidate would count the unit twice.
	selected, total, _ := pairingPresented(t, hub, 3)
	if selected[string(pairingFactID)] != ownerCandidate || total != 1500 {
		t.Fatalf("presented=%+v total=%d, want only the owner's 1500 W", selected, total)
	}
	if selected[string(pairingFactID)] == peerCandidate {
		t.Fatal("the contributor's candidate was presented over the owner's")
	}

	// A fence of the contributor keeps the resource and the owner's facts.
	fence, err := hub.Fence(semhub.FenceRequest{Source: pairingPeerSource, Epoch: peer.Epoch(), Generation: peer.Generation(), Reason: "lifecycle.generation_replaced", Evidence: []semreg.EvidenceRef{pairingEvidence("pairtest.fence", "peer")}, ObservedAt: pairingWall(4), Monotonic: pairingMono(4)})
	if err != nil || len(fence.Assets) != 1 || fence.Assets[0].Asset != pairingAsset {
		t.Fatalf("contributor fence=%+v (%v)", fence, err)
	}
	pairingAwaitAdmitted(t, hub)
	fenced, err := hub.Inspect(pairingAsset)
	if err != nil {
		t.Fatal(err)
	}
	if len(fenced.Snapshot.Resources) != 1 || fenced.Snapshot.Resources[0].ResourceID != unit || fenced.Snapshot.Resources[0].State != semreg.ResourceCurrent {
		t.Fatalf("resources after the contributor's fence=%+v", fenced.Snapshot.Resources)
	}
	ownerKept := false
	for _, envelope := range fenced.Snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			ownerKept = ownerKept || candidate.CandidateID == ownerCandidate
		}
	}
	if !ownerKept {
		t.Fatal("the contributor's fence removed the owner's fact")
	}
	for _, cursor := range fenced.Snapshot.Cursors {
		if cursor.SourceID == pairingOwnerSource && cursor.Fenced {
			t.Fatal("the contributor's fence fenced the owner")
		}
	}
	if _, err := pairingPublish(t, hub, owner, pairingOwnerDriver, 5, 1510, unit); err != nil {
		t.Fatalf("owner publication after the contributor's fence: %v", err)
	}
	pairingAwaitAdmitted(t, hub)
	if selected, total, _ := pairingPresented(t, hub, 6); selected[string(pairingFactID)] != ownerCandidate || total != 1510 {
		t.Fatalf("presented after the fence=%+v total=%d, want the owner's 1510 W", selected, total)
	}

	// An undeclared second claimant of the asset is still a collision, and an
	// undeclared source cannot publish on it.
	_, err = hub.Declare(semhub.AssetSpec{AssetID: pairingAsset, SourceID: "source:pairtest:third", Driver: pairingThirdDriver, Validators: []semreg.PackValidator{pv.New()}, Graph: true})
	var collision *semhub.CollisionError
	if !errors.As(err, &collision) || !strings.Contains(err.Error(), pairingThirdDriver) || !strings.Contains(err.Error(), pairingOwnerDriver) {
		t.Fatalf("undeclared second claimant=%v, want a collision naming both drivers", err)
	}
	third, err := hub.AdmitSource(semhub.Admission{Source: "source:pairtest:third", Driver: pairingThirdDriver})
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := hub.Begin(third, pairingAsset); err == nil {
		tx.Abort()
		t.Fatal("an undeclared source began a transaction on the paired asset")
	}
}

// The same undeclared claimant through the pairing composition: a second
// pairing that names the asset with another owner driver is a collision, and
// a pairing over an asset its owner declared without the contributor is a
// declaration conflict. Both refusals are fatal at startup.
func TestSemanticMultiSourceAcceptanceUndeclaredClaimantsStayFatal(t *testing.T) {
	t.Run("another owner driver", func(t *testing.T) {
		hub, err := semhub.New(semhub.Options{})
		if err != nil {
			t.Fatal(err)
		}
		other := pairingDeclaration()
		other.Owner.Driver, other.Owner.SourceID = pairingThirdDriver, "source:pairtest:third"
		other.Contributors[0].Source = "source:pairtest:fourth"
		if err := composeDeclaredPairings(hub, []semanticPairing{pairingDeclaration()}); err != nil {
			t.Fatal(err)
		}
		err = composeDeclaredPairings(hub, []semanticPairing{other})
		var collision *semhub.CollisionError
		if !errors.As(err, &collision) || !strings.Contains(err.Error(), pairingThirdDriver) || !strings.Contains(err.Error(), pairingOwnerDriver) {
			t.Fatalf("second claimant through a pairing=%v", err)
		}
	})
	t.Run("owner declared without the contributor", func(t *testing.T) {
		hub, err := semhub.New(semhub.Options{})
		if err != nil {
			t.Fatal(err)
		}
		alone := pairingDeclaration().Owner
		if _, err := hub.Declare(alone); err != nil {
			t.Fatal(err)
		}
		if err := composeDeclaredPairings(hub, []semanticPairing{pairingDeclaration()}); !errors.Is(err, semhub.ErrDeclarationConflict) {
			t.Fatalf("pairing over an owner declared alone=%v", err)
		}
		if _, known := hub.Source(pairingPeerSource); known {
			t.Fatal("the refused pairing registered its contributor")
		}
	})
}
