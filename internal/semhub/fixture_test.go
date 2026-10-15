package semhub

import (
	"errors"
	"strconv"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// The test pack is a minimal synthetic SemReg pack, so the hub tests need no
// protocol pack.
var testPackRef = semreg.PackRef{ID: "helianthus.pack.semhubtest", Version: "1.0.0"}

func testRef(id semreg.DefinitionID) semreg.DefinitionRef {
	return semreg.DefinitionRef{Pack: testPackRef, ID: id, Version: "1.0.0"}
}

type testValidator struct{ pack semreg.PackRef }

func newTestValidator() semreg.PackValidator { return testValidator{pack: testPackRef} }

func (v testValidator) Pack() semreg.PackRef { return v.pack }
func (v testValidator) Definitions() semreg.DefinitionIndex {
	return semreg.DefinitionIndex{Pack: v.pack, Fields: []semreg.DefinitionRef{testRef("semhubtest.value")}, Services: []semreg.DefinitionRef{testRef("semhubtest.service.main")}, Capabilities: []semreg.DefinitionRef{testRef("semhubtest.capability.read")}, Operations: []semreg.DefinitionRef{}, EffectRules: []semreg.DefinitionRef{}}
}
func (testValidator) ValidateFact(semreg.FactKey, *semreg.Value) error            { return nil }
func (testValidator) ValidateService(semreg.ServiceInstance) error                { return nil }
func (testValidator) ValidateCapability(semreg.CapabilityInstance) error          { return nil }
func (testValidator) ValidateField(semreg.DefinitionRef, semreg.TypedField) error { return nil }
func (testValidator) MatchConstraints(semreg.CapabilityInstance, []semreg.TypedField) error {
	return nil
}
func (testValidator) EvaluatePredicate(semreg.FactCandidate, semreg.PredicateOp, semreg.Value) (bool, error) {
	return false, errors.New("unsupported")
}

func testWall(seconds int64) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(1_800_000_000_000_000_000+seconds*1_000_000_000, 10)), ClockID: "wall.utc", UncertaintyNS: "0"}
}

func testMono(seconds int64) semreg.MonotonicPoint {
	return semreg.MonotonicPoint{ClockEpochID: "clock:semhub-test", Nanoseconds: semreg.Uint64(strconv.FormatInt(seconds*1_000_000_000, 10))}
}

func testEvidence(kind semreg.DefinitionID) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: testPackRef.ID, Kind: kind, Digest: "sha256:" + semreg.Digest(idHash("test")+idHash("test-evidence")), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

// testBatch builds one publication of the synthetic family from the staged
// transaction state: identity objects on the first batch of an incarnation,
// one refreshed fact on every batch.
func testBatch(t *testing.T, tx *Tx, family string, value int64, at int64) semreg.PublicationBatch {
	t.Helper()
	inc := tx.Incarnation()
	current, exists := tx.Current()
	generation := strconv.FormatUint(inc.Generation, 10)
	binding, err := NativeBindingID(family, inc.Asset, inc.Source, "semhubtest.profile.v1", inc.Epoch, inc.Generation)
	if err != nil {
		t.Fatal(err)
	}
	serviceID, err := NativeID(KindService, family, string(inc.Asset), "semhubtest.service.main", string(inc.Epoch), generation)
	if err != nil {
		t.Fatal(err)
	}
	capabilityID, err := NativeID(KindCapability, family, string(inc.Asset), "semhubtest.capability.read", string(inc.Epoch), generation)
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
	epoch, source, gen := inc.Epoch, inc.Source, semreg.Uint64(generation)
	evidence := testEvidence("semhubtest.observation")
	candidateID := semreg.CandidateID("candidate:" + family + ":" + idHash("candidate", string(inc.Asset), string(binding)))
	revision := semreg.Uint64("1")
	for _, envelope := range current.Facts {
		for _, candidate := range envelope.Candidates {
			if candidate.CandidateID == candidateID {
				revision = incrementUint(candidate.Revision)
			}
		}
	}
	dimension := string(inc.Asset)
	number := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: testDecimal(value), Unit: "unit.watt"}}
	fact := semreg.FactCandidate{CandidateID: candidateID, Key: semreg.FactKey{PackID: testPackRef.ID, PackVersion: testPackRef.Version, FactID: "semhubtest.value", Dimensions: []semreg.Dimension{{ID: "semhubtest.dimension.asset", Value: semreg.Value{Kind: semreg.ValueText, Text: &dimension}}}}, Value: &number,
		Quality:         semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:           semreg.Times{ReceivedAt: testWall(at), ReceiptMonotonic: testMono(at), EvaluatedAt: testWall(at), EvaluateMonotonic: testMono(at)},
		FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:semhub-test", Version: "1.0.0", FreshForNS: "60000000000", RetainForNS: "300000000000", MaxWallUncertaintyNS: "0"},
		BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &gen,
		Origin:   semreg.OriginRef{OriginID: semreg.OriginID("origin:" + family + ":" + idHash("origin", string(candidateID))), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}},
		Evidence: []semreg.EvidenceRef{evidence}, Revision: revision}
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:" + family + ":" + idHash("batch", string(inc.Asset), string(epoch), generation, string(tx.NextSequence()))), AssetID: inc.Asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: gen, Sequence: tx.NextSequence(), ExpectedSemanticRevision: expected, ObservedAt: testWall(at),
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: tx.SourceRetirements(), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, FactUpserts: []semreg.FactCandidate{fact}, FactWithdrawals: []semreg.CandidateID{},
		ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	if !hasSource {
		batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "semhubtest_protocol", ProfileID: "semhubtest.profile.v1", ProfileVersion: "1", RegistryEvidence: testEvidence("semhubtest.registry"), StartedAt: testWall(0), State: semreg.SourceCurrent, Revision: "1"}}
	}
	if !hasBinding {
		batch.BindingUpserts = []semreg.NativeBinding{{BindingID: binding, AssetID: inc.Asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: gen, NativeResource: testEvidence("semhubtest.native"), State: semreg.BindingCurrent, Revision: "1"}}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{{AssetID: inc.Asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: "1"}}
		batch.ServiceUpserts = []semreg.ServiceInstance{{InstanceID: semreg.ServiceInstanceID(serviceID), AssetID: inc.Asset, Definition: testRef("semhubtest.service.main"), BindingID: binding, SourceEpochID: epoch, DriverGeneration: gen, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1"}}
		batch.CapabilityUpserts = []semreg.CapabilityInstance{{InstanceID: semreg.CapabilityInstanceID(capabilityID), AssetID: inc.Asset, ServiceInstance: semreg.ServiceInstanceID(serviceID), Definition: testRef("semhubtest.capability.read"), BindingID: binding, SourceEpochID: epoch, DriverGeneration: gen, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{evidence}, Revision: "1"}}
	}
	digest, err := batch.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	batch.BatchDigest = digest
	return batch
}

func testDecimal(value int64) semreg.Decimal {
	if value == 0 {
		return semreg.Decimal{Coefficient: "0"}
	}
	exponent := int32(0)
	for value%10 == 0 {
		value /= 10
		exponent++
	}
	return semreg.Decimal{Coefficient: strconv.FormatInt(value, 10), Exponent10: exponent}
}

// publish runs one complete transaction of the synthetic family.
func publish(t *testing.T, hub *Hub, handle *Handle, id semreg.AssetID, family string, value, at int64) (Outcome, error) {
	t.Helper()
	tx, err := hub.Begin(handle, id)
	if err != nil {
		return "", err
	}
	defer tx.Abort()
	batch := testBatch(t, tx, family, value, at)
	if _, _, err := tx.Apply(batch, testMono(at)); err != nil {
		return "", err
	}
	return tx.Commit(nil)
}

func mustPublish(t *testing.T, hub *Hub, handle *Handle, id semreg.AssetID, family string, value, at int64) {
	t.Helper()
	outcome, err := publish(t, hub, handle, id, family, value, at)
	if err != nil || outcome != Accepted {
		t.Fatalf("publish %s=%v/%v", id, outcome, err)
	}
}

func newTestHub(t *testing.T) *Hub {
	t.Helper()
	hub, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func mustDeclare(t *testing.T, hub *Hub, id semreg.AssetID, src semreg.SourceID, driver string, onControl func(ControlEvent) error) *Declaration {
	t.Helper()
	declaration, err := hub.Declare(AssetSpec{AssetID: id, SourceID: src, Driver: driver, Validators: []semreg.PackValidator{newTestValidator()}, OnControl: onControl})
	if err != nil {
		t.Fatal(err)
	}
	return declaration
}

func errorID(err error) semreg.ErrorID {
	var semErr *semreg.Error
	if errors.As(err, &semErr) {
		return semErr.ID
	}
	return ""
}

func testFence(src semreg.SourceID, epoch semreg.SourceEpochID, generation uint64, at int64) FenceRequest {
	return FenceRequest{Source: src, Epoch: epoch, Generation: generation, Reason: "lifecycle.generation_replaced", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, ObservedAt: testWall(at), Monotonic: testMono(at)}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
