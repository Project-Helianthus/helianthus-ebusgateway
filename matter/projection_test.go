package matter

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func TestManifestAndLedgerCoverPinnedMetadataExactlyOnce(t *testing.T) {
	manifest := Manifest()
	if manifest.TargetID != TargetID || manifest.TargetVersion != TargetVersion || manifest.MappingRevision != MappingRevision || len(manifest.PackVersions) != 5 {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
	requested, dispositions := Ledger()
	if len(requested) == 0 || len(requested) != len(dispositions) {
		t.Fatalf("ledger counts requested=%d dispositions=%d", len(requested), len(dispositions))
	}
	seen := map[projection.RequestedItem]struct{}{}
	for i, item := range requested {
		if _, duplicate := seen[item]; duplicate {
			t.Fatalf("duplicate requested item: %#v", item)
		}
		seen[item] = struct{}{}
		if dispositions[i].Kind != item.Kind || dispositions[i].ItemID != item.ItemID || dispositions[i].Outcome != projection.ProjectionUnknown || dispositions[i].Reason == nil || *dispositions[i].Reason != "matter.unmapped.v1" || len(dispositions[i].SourceKeys) != 0 || len(dispositions[i].Loss) != 1 || dispositions[i].Loss[0].Kind != projection.LossPolicy || dispositions[i].Loss[0].Description != "no target lookup, inferred fallback, authority, intent, route, or operation is created" || dispositions[i].Loss[0].Reversible {
			t.Fatalf("non-positive disposition %d: %#v", i, dispositions[i])
		}
	}
	if _, ok := seen[projection.RequestedItem{Kind: projection.ItemFact, ItemID: "evse.ac.current"}]; !ok {
		t.Fatal("EVSE current absent")
	}
	if _, ok := seen[projection.RequestedItem{Kind: projection.ItemOperation, ItemID: "evse.operation.set_allocated_current"}]; !ok {
		t.Fatal("EVSE operation absent")
	}
}

func TestLedgerIsDeterministicAndDoesNotExposeOperations(t *testing.T) {
	firstRequested, firstDispositions := Ledger()
	secondRequested, secondDispositions := Ledger()
	if !reflect.DeepEqual(firstRequested, secondRequested) || !reflect.DeepEqual(firstDispositions, secondDispositions) {
		t.Fatal("ledger is nondeterministic")
	}
	for _, disposition := range firstDispositions {
		if disposition.Kind == projection.ItemOperation && (len(disposition.SourceKeys) != 0 || disposition.Outcome != projection.ProjectionUnknown) {
			t.Fatalf("operation became actionable: %#v", disposition)
		}
	}
}

func TestEVSEActiveCurrentUsesPinnedNumericTargetAndExactConversion(t *testing.T) {
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "32", Exponent10: -1}, Unit: "unit.ampere"}}
	got, err := amperesToMilliampere(value)
	if err != nil || got != "3200" {
		t.Fatalf("A to mA = %q, %v", got, err)
	}
	if evseActiveCurrent != (TargetElement{DeviceType: 0x0510, Cluster: 0x0090, Kind: ElementAttribute, ElementID: 0x0005}) {
		t.Fatalf("unexpected target: %#v", evseActiveCurrent)
	}
	for _, boundary := range []string{"-4611686018427387904", "4611686018427387904"} {
		value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: boundary, Exponent10: -3}, Unit: "unit.ampere"}}
		got, err := amperesToMilliampere(value)
		if err != nil || got != boundary {
			t.Fatalf("inclusive target boundary %s = %q, %v", boundary, got, err)
		}
	}
}

func TestEVSECurrentFailsClosedOnWrongUnitOrPrecision(t *testing.T) {
	for _, value := range []semreg.Value{
		{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "1", Exponent10: 0}, Unit: "unit.volt"}},
		{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "1", Exponent10: -4}, Unit: "unit.ampere"}},
		{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "4611686018427387905", Exponent10: -3}, Unit: "unit.ampere"}},
		{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "-4611686018427387905", Exponent10: -3}, Unit: "unit.ampere"}},
	} {
		if _, err := amperesToMilliampere(value); err == nil {
			t.Fatalf("accepted malformed or lossy value: %#v", value)
		}
	}
}

func TestEvaluationEligibilityFailsClosedForStaleAndUnavailable(t *testing.T) {
	for _, item := range []semreg.EvaluatedFact{
		{Freshness: semreg.FreshnessStale, EffectiveAvailability: semreg.AvailabilityAvailable},
		{Freshness: semreg.FreshnessFresh, EffectiveAvailability: semreg.AvailabilityUnavailable},
	} {
		if err := eligibleEvaluation(semreg.EvaluationView{Facts: []semreg.EvaluatedFact{item}}); err == nil {
			t.Fatalf("accepted ineligible evaluation: %#v", item)
		}
	}
}

func TestCanonicalJSONIsDeterministicForEquivalentResult(t *testing.T) {
	result := Result{Document: Document{Contract: Contract, TargetID: TargetID, TargetVersion: TargetVersion, MappingRevision: MappingRevision, Attributes: []TargetAttribute{}}}
	// A report is deliberately required. This is a fail-closed output boundary.
	if _, err := CanonicalJSON(result); err == nil {
		t.Fatal("accepted result without a validated report")
	}
}

type matterFixture struct {
	snapshot   semreg.Snapshot
	evaluation semreg.EvaluationView
}

func TestProjectEVSECurrentEndToEndAndContractPin(t *testing.T) {
	fixture := newMatterFixture(t, []semreg.FactCandidate{matterCandidate("candidate:matter:l1", "l1", "32", -1)})
	beforeSnapshot, _ := json.Marshal(fixture.snapshot)
	beforeEvaluation, _ := json.Marshal(fixture.evaluation)

	first, err := Project(fixture.snapshot, fixture.evaluation)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Document.Attributes) != 1 || first.Document.Attributes[0].Element != evseActiveCurrent || first.Document.Attributes[0].Value.Milliampere != "3200" {
		t.Fatalf("unexpected target document: %#v", first.Document)
	}
	if !reflect.DeepEqual(first.Report.Manifest, Manifest()) || len(first.Report.Requested) != 127 || len(first.Report.Dispositions) != 127 {
		t.Fatalf("unexpected projection report: %#v", first.Report)
	}
	var transformed int
	for _, disposition := range first.Report.Dispositions {
		if disposition.ItemID == "evse.ac.current" && disposition.Kind == projection.ItemFact {
			transformed++
			if disposition.Outcome != projection.ProjectionTransformed || disposition.Reason == nil || *disposition.Reason != "matter.phase_endpoint_identity_loss.v1" || len(disposition.SourceKeys) != 1 || len(disposition.Loss) != 1 || disposition.Loss[0].Kind != projection.LossUnit || disposition.Loss[0].Description != "amperes are represented as integral milliamperes only when exact and representable" {
				t.Fatalf("unexpected transformed disposition: %#v", disposition)
			}
			continue
		}
		if disposition.Outcome != projection.ProjectionUnknown || disposition.Reason == nil || *disposition.Reason != "matter.unmapped.v1" || len(disposition.SourceKeys) != 0 || len(disposition.Loss) != 1 || disposition.Loss[0].Kind != projection.LossPolicy || disposition.Loss[0].SourceItems[0] != disposition.ItemID {
			t.Fatalf("unexpected unknown disposition: %#v", disposition)
		}
	}
	if transformed != 1 {
		t.Fatalf("transformed rows = %d", transformed)
	}
	firstJSON, err := CanonicalJSON(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Project(fixture.snapshot, fixture.evaluation)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := CanonicalJSON(second)
	if err != nil || !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("projection is not deterministic: err=%v", err)
	}
	afterSnapshot, _ := json.Marshal(fixture.snapshot)
	afterEvaluation, _ := json.Marshal(fixture.evaluation)
	if !bytes.Equal(beforeSnapshot, afterSnapshot) || !bytes.Equal(beforeEvaluation, afterEvaluation) {
		t.Fatal("Project mutated its inputs")
	}
	if DocsSemanticCommit != "30f5e5c79ac6da3a7c7c10c990599906d1dfd0cb" || DocsSemanticContractPath != "api/v1/targets/matter-1.7-ballot-0.9-v1.json" || DocsSemanticContractSHA256 != "5ae81d5e0971d25ead08f982fdf31caf47ada4838d0ee6f2b3e719d11a6df39c" {
		t.Fatal("accepted docs-semantic contract pin drifted")
	}
}

func TestProjectRejectsEvaluationCorrespondenceDrift(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*semreg.EvaluationView)
		want   semreg.ErrorID
	}{
		{
			name: "candidate revision",
			mutate: func(view *semreg.EvaluationView) {
				view.Facts[0].CandidateRevision = "2"
			},
			want: semreg.RevisionConflict,
		},
		{
			name: "dangling candidate",
			mutate: func(view *semreg.EvaluationView) {
				view.Facts[0].CandidateID = "candidate:matter:dangling"
			},
			want: semreg.DanglingReference,
		},
		{
			name: "omitted candidate",
			mutate: func(view *semreg.EvaluationView) {
				view.Facts = []semreg.EvaluatedFact{}
			},
			want: semreg.PreconditionFailed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newMatterFixture(t, []semreg.FactCandidate{matterCandidate("candidate:matter:l1", "l1", "32", -1)})
			tc.mutate(&fixture.evaluation)
			sealEvaluation(t, &fixture.evaluation)
			got, err := Project(fixture.snapshot, fixture.evaluation)
			if !reflect.DeepEqual(got, Result{}) || semreg.ErrorIdentifier(err) != tc.want {
				t.Fatalf("result=%#v err=%v want=%s", got, err, tc.want)
			}
		})
	}
}

func TestProjectRejectsIneligibleQualityAndEvaluation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		mutateQuality func(*semreg.Quality)
		mutateView    func(*semreg.EvaluationView)
	}{
		{name: "unqualified", mutateQuality: func(q *semreg.Quality) {
			q.Qualification, q.Promotion = semreg.QualificationCandidate, semreg.PromotionUnpromoted
		}},
		{name: "unpromoted", mutateQuality: func(q *semreg.Quality) { q.Promotion = semreg.PromotionUnpromoted }},
		{name: "invalid", mutateQuality: func(q *semreg.Quality) { q.Validity, q.Promotion = semreg.ValidityBad, semreg.PromotionUnpromoted }},
		{name: "stale", mutateView: func(v *semreg.EvaluationView) { v.Facts[0].Freshness = semreg.FreshnessStale }},
		{name: "unavailable", mutateView: func(v *semreg.EvaluationView) { v.Facts[0].EffectiveAvailability = semreg.AvailabilityUnavailable }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newMatterFixture(t, []semreg.FactCandidate{matterCandidate("candidate:matter:l1", "l1", "32", -1)})
			if tc.mutateQuality != nil {
				tc.mutateQuality(&fixture.snapshot.Facts[0].Candidates[0].Quality)
				sealSnapshot(t, &fixture.snapshot)
				fixture.evaluation = evaluateMatterSnapshot(t, fixture.snapshot)
			}
			if tc.mutateView != nil {
				tc.mutateView(&fixture.evaluation)
				sealEvaluation(t, &fixture.evaluation)
			}
			got, err := Project(fixture.snapshot, fixture.evaluation)
			if !reflect.DeepEqual(got, Result{}) || err == nil {
				t.Fatalf("accepted ineligible input: result=%#v err=%v", got, err)
			}
		})
	}
}

func TestProjectRejectsWrongDimensionUnitBoundsConflictAndAmbiguity(t *testing.T) {
	t.Run("wrong phase dimension", func(t *testing.T) {
		fixture := newMatterFixture(t, []semreg.FactCandidate{matterCandidate("candidate:matter:l1", "l1", "32", -1)})
		fixture.snapshot.Facts[0].Key.Dimensions[0].ID = "evse.dimension.connector"
		fixture.snapshot.Facts[0].Candidates[0].Key = fixture.snapshot.Facts[0].Key
		sealSnapshot(t, &fixture.snapshot)
		fixture.evaluation = evaluateMatterSnapshot(t, fixture.snapshot)
		assertProjectRejected(t, fixture)
	})
	t.Run("wrong unit", func(t *testing.T) {
		fixture := newMatterFixture(t, []semreg.FactCandidate{matterCandidate("candidate:matter:l1", "l1", "32", -1)})
		fixture.snapshot.Facts[0].Candidates[0].Value.Quantity.Unit = "unit.volt"
		sealSnapshot(t, &fixture.snapshot)
		fixture.evaluation = evaluateMatterSnapshot(t, fixture.snapshot)
		assertProjectRejected(t, fixture)
	})
	t.Run("outside EVSE source bounds", func(t *testing.T) {
		fixture := newMatterFixture(t, []semreg.FactCandidate{matterCandidate("candidate:matter:l1", "l1", "32", -1)})
		fixture.snapshot.Facts[0].Candidates[0].Value.Quantity.Number = semreg.Decimal{Coefficient: "1001"}
		sealSnapshot(t, &fixture.snapshot)
		fixture.evaluation = evaluateMatterSnapshot(t, fixture.snapshot)
		assertProjectRejected(t, fixture)
	})
	t.Run("conflict", func(t *testing.T) {
		fixture := newMatterFixture(t, []semreg.FactCandidate{
			matterCandidate("candidate:matter:a", "l1", "32", -1),
			matterCandidate("candidate:matter:b", "l1", "33", -1),
		})
		if len(fixture.snapshot.Facts[0].Conflicts) == 0 {
			t.Fatal("fixture did not produce a conflict")
		}
		assertProjectRejected(t, fixture)
	})
	t.Run("multiple phase keys", func(t *testing.T) {
		fixture := newMatterFixture(t, []semreg.FactCandidate{
			matterCandidate("candidate:matter:l1", "l1", "32", -1),
			matterCandidate("candidate:matter:l2", "l2", "33", -1),
		})
		assertProjectRejected(t, fixture)
	})
}

func assertProjectRejected(t *testing.T, fixture matterFixture) {
	t.Helper()
	got, err := Project(fixture.snapshot, fixture.evaluation)
	if !reflect.DeepEqual(got, Result{}) || err == nil {
		t.Fatalf("accepted invalid projection: result=%#v err=%v", got, err)
	}
}

func newMatterFixture(t *testing.T, candidates []semreg.FactCandidate) matterFixture {
	t.Helper()
	kernel, err := semreg.NewPublicationKernel("asset:matter:1", evse.New())
	if err != nil {
		t.Fatal(err)
	}
	evidence := matterEvidence()
	batch := semreg.PublicationBatch{
		Contract:                 semreg.ContractKernelV1,
		BatchID:                  "batch:matter:1",
		AssetID:                  "asset:matter:1",
		SourceID:                 "source:matter:1",
		SourceEpochID:            "epoch:matter:1",
		DriverGeneration:         "1",
		Sequence:                 "1",
		ExpectedSemanticRevision: "0",
		ObservedAt:               matterTime("100"),
		SourceUpserts: []semreg.SourceDescriptor{{
			SourceID: "source:matter:1", SourceEpochID: "epoch:matter:1", ProtocolID: "protocol.test", ProfileID: "profile.test", ProfileVersion: "1", RegistryEvidence: evidence, StartedAt: matterTime("90"), State: semreg.SourceCurrent, Revision: "1",
		}},
		SourceRetirements: []semreg.SourceEpochID{},
		BindingUpserts: []semreg.NativeBinding{{
			BindingID: "binding:matter:1", AssetID: "asset:matter:1", SourceID: "source:matter:1", SourceEpochID: "epoch:matter:1", DriverGeneration: "1", NativeResource: evidence, State: semreg.BindingCurrent, Revision: "1",
		}},
		IdentityLinkUpserts: []semreg.IdentityLink{{
			AssetID: "asset:matter:1", BindingID: "binding:matter:1", State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: "1",
		}},
		FactUpserts:           candidates,
		FactWithdrawals:       []semreg.CandidateID{},
		ServiceUpserts:        []semreg.ServiceInstance{},
		ServiceWithdrawals:    []semreg.ServiceInstanceID{},
		CapabilityUpserts:     []semreg.CapabilityInstance{},
		CapabilityWithdrawals: []semreg.CapabilityInstanceID{},
		GenerationFences:      []semreg.GenerationFence{},
	}
	batch.BatchDigest, err = batch.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := kernel.Apply(batch, semreg.MonotonicPoint{ClockEpochID: "clock-epoch:matter", Nanoseconds: "100"})
	if err != nil {
		t.Fatal(err)
	}
	return matterFixture{snapshot: snapshot, evaluation: evaluateMatterSnapshot(t, snapshot)}
}

func matterCandidate(id semreg.CandidateID, phase, coefficient string, exponent int32) semreg.FactCandidate {
	binding := semreg.NativeBindingID("binding:matter:1")
	epoch := semreg.SourceEpochID("epoch:matter:1")
	generation := semreg.Uint64("1")
	source := semreg.SourceID("source:matter:1")
	key := semreg.FactKey{
		PackID: "helianthus.pack.evse", PackVersion: "1.0.0", FactID: "evse.ac.current",
		Dimensions: []semreg.Dimension{{ID: "evse.dimension.phase", Value: semreg.Value{Kind: semreg.ValueText, Text: &phase}}},
	}
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: coefficient, Exponent10: exponent}, Unit: "unit.ampere"}}
	return semreg.FactCandidate{
		CandidateID: id, Key: key, Value: &value,
		Quality:         semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:           semreg.Times{ReceivedAt: matterTime("100"), ReceiptMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock-epoch:matter", Nanoseconds: "100"}, EvaluatedAt: matterTime("100"), EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock-epoch:matter", Nanoseconds: "100"}},
		FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy.matter.current", Version: "1.0.0", FreshForNS: "1000", RetainForNS: "2000", MaxWallUncertaintyNS: "0"},
		BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
		Origin:   semreg.OriginRef{OriginID: "origin:matter:1", Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{matterEvidence()}},
		Evidence: []semreg.EvidenceRef{matterEvidence()}, Revision: "1",
	}
}

func matterEvidence() semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: "owner.test", Kind: "evidence.test", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Contract: "test.evidence/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
}

func matterTime(n string) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(n), ClockID: "clock.utc", UncertaintyNS: "0"}
}

func evaluateMatterSnapshot(t *testing.T, snapshot semreg.Snapshot) semreg.EvaluationView {
	t.Helper()
	view, err := semreg.EvaluateSnapshot(snapshot, semreg.EvaluationContext{EvaluatedAt: matterTime("150"), EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock-epoch:matter", Nanoseconds: "150"}})
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func sealSnapshot(t *testing.T, snapshot *semreg.Snapshot) {
	t.Helper()
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	delete(document, "snapshot_id")
	raw, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	snapshot.SnapshotID = semreg.SnapshotID(fmt.Sprintf("sha256:%x", digest))
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("invalid resealed snapshot: %v", err)
	}
}

func sealEvaluation(t *testing.T, evaluation *semreg.EvaluationView) {
	t.Helper()
	evaluation.EvaluationDigest = ""
	digest, err := evaluation.EvaluationDigestValue()
	if err != nil {
		t.Fatal(err)
	}
	evaluation.EvaluationDigest = digest
	if err := evaluation.Validate(); err != nil {
		t.Fatalf("invalid resealed evaluation: %v", err)
	}
}
