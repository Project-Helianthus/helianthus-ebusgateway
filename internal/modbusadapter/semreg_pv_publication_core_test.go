package modbusadapter

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	pvpack "github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func TestPVPublicationDraftRejectsMissingTimes(t *testing.T) {
	observation := pvCoreObservation(t, observedFroniusFloatControlsWords(), 101, 201)
	lifecycle := pvCoreLifecycle("source-epoch:pv:test-a", 1, 2_000)
	lifecycle.receivedAt = semreg.TimePoint{}
	if _, err := buildPVPublicationDraft(observation, lifecycle); err == nil {
		t.Fatalf("missing receipt time error=%v", err)
	}
}

func TestPVPublicationDraftStableIdentityIgnoresMutableObservation(t *testing.T) {
	firstWords := observedFroniusFloatControlsWords()
	secondWords := append([]uint16(nil), firstWords...)
	pvCoreSetFloat(secondWords, 20, 4_321.5)
	first := pvCoreObservation(t, firstWords, 101, 201)
	second := pvCoreObservation(t, secondWords, 102, 202)
	lifecycle := pvCoreLifecycle("source-epoch:pv:test-a", 1, 2_000)

	firstDraft, err := buildPVPublicationDraft(first, lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	secondDraft, err := buildPVPublicationDraft(second, lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	if firstDraft.assetID != secondDraft.assetID || firstDraft.source.SourceID != secondDraft.source.SourceID {
		t.Fatalf("stable identity drifted asset=%s/%s source=%s/%s", firstDraft.assetID, secondDraft.assetID, firstDraft.source.SourceID, secondDraft.source.SourceID)
	}
	if firstDraft.assetID != "pv-asset-5c57adba3ad8a529617c1f8627ae32f1" {
		t.Fatalf("accepted canonical-PV asset identity drifted: %s", firstDraft.assetID)
	}
	if firstDraft.binding.BindingID != secondDraft.binding.BindingID {
		t.Fatalf("same epoch/generation binding drifted %s/%s", firstDraft.binding.BindingID, secondDraft.binding.BindingID)
	}
	if firstDraft.observationEvidence.Digest == secondDraft.observationEvidence.Digest {
		t.Fatal("changed native observation retained the same evidence digest")
	}
}

func TestPVPublicationDraftValidatesEveryProposalAndRejectsCallerLabels(t *testing.T) {
	draft := pvCoreDraft(t, observedFroniusFloatControlsWords(), "source-epoch:pv:test-a", 1, 2_000)
	registry, err := semreg.NewRegistry(pvpack.New())
	if err != nil {
		t.Fatal(err)
	}
	if len(draft.facts) != 11 || len(draft.services) != 3 || len(draft.capabilities) != 3 || len(draft.requested) != 14 || len(draft.dispositions) != 14 {
		t.Fatalf("draft accounting facts=%d services=%d capabilities=%d requested=%d dispositions=%d", len(draft.facts), len(draft.services), len(draft.capabilities), len(draft.requested), len(draft.dispositions))
	}
	for _, fact := range draft.facts {
		if err := fact.Validate(); err != nil {
			t.Fatalf("candidate %s structural validation: %v", fact.CandidateID, err)
		}
		if err := registry.ValidateFactCandidate(fact); err != nil {
			t.Fatalf("candidate %s pack validation: %v", fact.CandidateID, err)
		}
		if fact.Quality.Qualification != semreg.QualificationQualified || fact.Quality.Promotion != semreg.PromotionPromoted {
			t.Fatalf("candidate %s labels=%s/%s", fact.CandidateID, fact.Quality.Qualification, fact.Quality.Promotion)
		}
	}
	for _, service := range draft.services {
		if err := registry.ValidateService(service); err != nil {
			t.Fatalf("service %s: %v", service.InstanceID, err)
		}
	}
	for _, capability := range draft.capabilities {
		if err := registry.ValidateCapability(capability); err != nil {
			t.Fatalf("capability %s: %v", capability.InstanceID, err)
		}
	}
	for _, disposition := range draft.dispositions {
		if err := disposition.Validate(); err != nil {
			t.Fatalf("disposition %s: %v", disposition.ItemID, err)
		}
	}

	mutated, err := draft.detached()
	if err != nil {
		t.Fatal(err)
	}
	mutated.facts[0].Quality.Qualification = semreg.QualificationCandidate
	if err := mutated.validate(); err == nil {
		t.Fatal("caller-supplied candidate qualification label was trusted")
	}
	mutated, _ = draft.detached()
	mutated.services[0].Qualification = semreg.QualificationCandidate
	if err := mutated.validate(); err == nil {
		t.Fatal("caller-supplied service qualification label was trusted")
	}
	mutated, _ = draft.detached()
	mutated.capabilities[0].Availability = semreg.AvailabilityDegraded
	if err := mutated.validate(); err == nil {
		t.Fatal("caller-supplied capability availability label was trusted")
	}
}

func TestPVPublicationDraftIsolatesOneInvalidField(t *testing.T) {
	words := observedFroniusFloatControlsWords()
	pvCoreSetFloat(words, 22, 2_000) // PV pack maximum frequency is 1000 Hz.
	draft := pvCoreDraft(t, words, "source-epoch:pv:test-a", 1, 2_000)
	if len(draft.facts) != 10 {
		t.Fatalf("valid facts=%d want=10", len(draft.facts))
	}
	disposition := pvCoreDisposition(t, draft, "inverter.ac.frequency")
	if disposition.Outcome != projection.ProjectionWithheld || disposition.Reason == nil || *disposition.Reason != pvReasonFieldInvalid || len(disposition.SourceKeys) != 0 {
		t.Fatalf("invalid frequency disposition=%+v", disposition)
	}
	if !pvCoreHasFact(draft.facts, "pv.ac.aggregate_active_power") {
		t.Fatal("invalid frequency discarded independent active power")
	}
}

func TestPVPublicationDraftWithholdsUnsupportedOperatingSymbolOnly(t *testing.T) {
	words := observedFroniusFloatControlsWords()
	const model113PayloadStart = 2 + 67 + 2
	words[model113PayloadStart+46] = 1 // OFF is admitted natively but has no accepted mapping.
	draft := pvCoreDraft(t, words, "source-epoch:pv:test-a", 1, 2_000)
	if len(draft.facts) != 10 {
		t.Fatalf("valid facts=%d want=10", len(draft.facts))
	}
	disposition := pvCoreDisposition(t, draft, "inverter.operating_state")
	if disposition.Outcome != projection.ProjectionWithheld || disposition.Reason == nil || *disposition.Reason != pvReasonSymbolUnavailable || len(disposition.SourceKeys) != 0 || len(disposition.Loss) != 1 || disposition.Loss[0].Kind != projection.LossSymbol {
		t.Fatalf("unsupported status disposition=%+v", disposition)
	}
	if !pvCoreHasFact(draft.facts, "pv.ac.aggregate_active_power") {
		t.Fatal("unsupported status discarded independent active power")
	}
}

func TestPVPublicationCoreEnforcesSequenceAndExpectedRevision(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	first := pvCoreDraft(t, observedFroniusFloatControlsWords(), "source-epoch:pv:test-a", 1, 2_000)
	if _, err := core.ingest(first); err != nil {
		t.Fatal(err)
	}
	before, err := core.currentForValidation(first.assetID)
	if err != nil {
		t.Fatal(err)
	}

	words := observedFroniusFloatControlsWords()
	pvCoreSetFloat(words, 20, 4_321.5)
	second := pvCoreDraft(t, words, "source-epoch:pv:test-a", 1, 3_000)
	_, err = core.ingestWithOrder(second, &pvPublicationOrder{sequence: "1", expectedSemanticRevision: before.snapshot.Revisions.Semantic})
	if semreg.ErrorIdentifier(err) != semreg.SequenceConflict {
		t.Fatalf("sequence collision error=%v", err)
	}
	_, err = core.ingestWithOrder(second, &pvPublicationOrder{sequence: "2", expectedSemanticRevision: "0"})
	if semreg.ErrorIdentifier(err) != semreg.RevisionConflict {
		t.Fatalf("expected revision conflict error=%v", err)
	}
	after, err := core.currentForValidation(first.assetID)
	if err != nil {
		t.Fatal(err)
	}
	if after.snapshot.SnapshotID != before.snapshot.SnapshotID {
		t.Fatalf("failed order checks advanced state %s -> %s", before.snapshot.SnapshotID, after.snapshot.SnapshotID)
	}
}

func TestPVPublicationCoreRejectsSameEpochSourceStartDrift(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	first := pvCoreDraft(t, observedFroniusFloatControlsWords(), "source-epoch:pv:test-a", 1, 2_000)
	if _, err := core.ingest(first); err != nil {
		t.Fatal(err)
	}
	before, err := core.currentForValidation(first.assetID)
	if err != nil {
		t.Fatal(err)
	}
	second := pvCoreDraft(t, observedFroniusFloatControlsWords(), "source-epoch:pv:test-a", 1, 3_000)
	second.lifecycle.sourceStartedAt = pvCoreWall(1_001)
	second.source.StartedAt = second.lifecycle.sourceStartedAt
	if _, err := core.ingest(second); semreg.ErrorIdentifier(err) != semreg.StaleSourceEpoch {
		t.Fatalf("same-epoch source start drift error=%v", err)
	}
	after, err := core.currentForValidation(first.assetID)
	if err != nil {
		t.Fatal(err)
	}
	if after.snapshot.SnapshotID != before.snapshot.SnapshotID {
		t.Fatalf("source start drift advanced state %s -> %s", before.snapshot.SnapshotID, after.snapshot.SnapshotID)
	}
}

func TestPVPublicationCoreFencesGenerationAndRetiresEpoch(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	words := observedFroniusFloatControlsWords()
	first := pvCoreDraft(t, words, "source-epoch:pv:test-a", 1, 2_000)
	if _, err := core.ingest(first); err != nil {
		t.Fatal(err)
	}
	second := pvCoreDraft(t, words, "source-epoch:pv:test-a", 2, 3_000)
	if _, err := core.ingest(second); err != nil {
		t.Fatalf("generation transition: %v", err)
	}
	view, err := core.currentForValidation(first.assetID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.snapshot.Fences) != 1 || !pvCoreHasCurrentCursor(view.snapshot, second.lifecycle.sourceEpochID, "2") {
		t.Fatalf("generation fence/cursor state=%+v/%+v", view.snapshot.Fences, view.snapshot.Cursors)
	}
	staleGeneration := pvCoreDraft(t, words, "source-epoch:pv:test-a", 1, 4_000)
	if _, err := core.ingest(staleGeneration); semreg.ErrorIdentifier(err) != semreg.StaleDriverGeneration {
		t.Fatalf("stale generation error=%v", err)
	}

	newEpoch := pvCoreDraft(t, words, "source-epoch:pv:test-b", 1, 5_000)
	newEpoch.lifecycle.sourceStartedAt = pvCoreWall(4_500)
	newEpoch.source.StartedAt = newEpoch.lifecycle.sourceStartedAt
	beforeEpoch, err := core.currentForValidation(first.assetID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.ingest(newEpoch); err != nil {
		t.Fatalf("epoch transition: %v", err)
	}
	afterEpoch, err := core.currentForValidation(first.assetID)
	if err != nil {
		t.Fatal(err)
	}
	if afterEpoch.snapshot.SnapshotID == beforeEpoch.snapshot.SnapshotID || bytes.Equal(afterEpoch.canonical, beforeEpoch.canonical) {
		t.Fatalf("accepted epoch retirement did not advance public semantic state %s", afterEpoch.snapshot.SnapshotID)
	}
	if !pvCoreHasCurrentSource(afterEpoch.snapshot, newEpoch.lifecycle.sourceEpochID) || len(afterEpoch.evaluation.Retained) == 0 {
		t.Fatalf("sequential lifecycle did not retain historic observations: %+v", afterEpoch.snapshot)
	}
	staleEpoch := pvCoreDraft(t, words, "source-epoch:pv:test-a", 3, 6_000)
	if _, err := core.ingest(staleEpoch); semreg.ErrorIdentifier(err) != semreg.StaleSourceEpoch {
		t.Fatalf("stale epoch error=%v", err)
	}
}

func TestPVPublicationCorePublishesGeneratedEnergyAndKeepsCompleteProjection(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	words := observedFroniusFloatControlsWords()
	pvCoreSetFloat(words, 30, 7_310) // native Wh, required public representation is exact kWh.
	draft := pvCoreDraft(t, words, "source-epoch:pv:test-a", 1, 2_000)
	receipt, err := core.ingest(draft)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.assetID != draft.assetID || receipt.snapshotID == "" {
		t.Fatalf("internal receipt=%+v", receipt)
	}
	if _, err := core.publicView(draft.assetID); err != nil {
		t.Fatalf("public view error=%v", err)
	}
	view, err := core.currentForValidation(draft.assetID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.snapshot.Facts) != 11 || len(view.evaluation.Facts) != 11 || len(view.selections) != 11 || len(view.projection.Requested) != 14 || len(view.projection.Dispositions) != 14 {
		t.Fatalf("view accounting facts=%d evaluation=%d selections=%d requested=%d dispositions=%d", len(view.snapshot.Facts), len(view.evaluation.Facts), len(view.selections), len(view.projection.Requested), len(view.projection.Dispositions))
	}
	energy := pvCoreDispositionReport(t, view.projection, "inverter.ac.energy_lifetime")
	if energy.Outcome != projection.ProjectionTransformed || energy.Reason == nil || *energy.Reason != pvReasonCounterContinuityUnavailable || len(energy.SourceKeys) != 1 || len(energy.Loss) != 1 || energy.Loss[0].Kind != projection.LossPolicy {
		t.Fatalf("energy continuity-loss disposition=%+v", energy)
	}
	for _, envelope := range view.snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			if candidate.Key.FactID != "pv.energy.generated" {
				continue
			}
			if candidate.Value == nil || candidate.Value.Quantity == nil || candidate.Value.Quantity.Unit != "unit.kilowatt_hour" || candidate.Value.Quantity.Number.Coefficient != "731" || candidate.Value.Quantity.Number.Exponent10 != -2 {
				t.Fatalf("generated energy transform=%+v; want exact 7.31 kWh", candidate.Value)
			}
		}
	}
	if _, err := projection.ValidateReport(view.snapshot, view.projection); err != nil {
		t.Fatalf("snapshot-bound projection: %v", err)
	}
}

func TestPVPublicationCorePublishesCanonicalZeroGeneratedEnergy(t *testing.T) {
	words := observedFroniusFloatControlsWords()
	pvCoreSetFloat(words, 30, 0)
	draft := pvCoreDraft(t, words, "source-epoch:pv:zero-energy", 1, 2_000)
	disposition := pvCoreDisposition(t, draft, "inverter.ac.energy_lifetime")
	if disposition.Outcome != projection.ProjectionTransformed || disposition.Reason == nil || *disposition.Reason != pvReasonCounterContinuityUnavailable {
		t.Fatalf("zero energy disposition=%+v", disposition)
	}
	for _, candidate := range draft.facts {
		if candidate.Key.FactID != "pv.energy.generated" {
			continue
		}
		if candidate.Value == nil || candidate.Value.Quantity == nil || candidate.Value.Quantity.Unit != "unit.kilowatt_hour" || candidate.Value.Quantity.Number.Coefficient != "0" || candidate.Value.Quantity.Number.Exponent10 != 0 {
			t.Fatalf("zero generated energy=%+v; want canonical 0e0 kWh", candidate.Value)
		}
		return
	}
	t.Fatal("canonical zero generated energy was withheld")
}

func TestPVPublicationCoreCommitsHealthyRefreshWhenRetainedFieldIsStale(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	first := pvCoreDraft(t, observedFroniusFloatControlsWords(), "source-epoch:pv:test-a", 1, 2_000)
	if _, err := core.ingest(first); err != nil {
		t.Fatal(err)
	}
	before, err := core.currentForValidation(first.assetID)
	if err != nil {
		t.Fatal(err)
	}
	words := observedFroniusFloatControlsWords()
	pvCoreSetFloat(words, 20, 4_321.5)
	pvCoreSetFloat(words, 22, 2_000) // invalid frequency; retain the prior candidate.
	second := pvCoreDraft(t, words, "source-epoch:pv:test-a", 1, 31_000_000_000)
	if _, err := core.ingest(second); err != nil {
		t.Fatalf("healthy refresh with one stale retained field: %v", err)
	}
	after, err := core.currentForValidation(first.assetID)
	if err != nil {
		t.Fatal(err)
	}
	if after.snapshot.SnapshotID == before.snapshot.SnapshotID {
		t.Fatal("healthy refresh did not advance the immutable snapshot")
	}
	frequency := pvCoreEnvelope(t, after.snapshot, "pv.ac.frequency")
	if len(frequency.Candidates) != 1 {
		t.Fatalf("retained frequency candidates=%d", len(frequency.Candidates))
	}
	if freshness, ok := pvCoreEvaluatedFreshness(after.evaluation, frequency.Candidates[0].CandidateID); !ok || freshness != semreg.FreshnessStale {
		t.Fatalf("retained frequency freshness=%s found=%t", freshness, ok)
	}
	if pvCoreHasSelection(after.selections, frequency.Key) {
		t.Fatal("stale retained frequency received a presentation selection")
	}
	frequencyProjection := pvCoreDispositionReport(t, after.projection, "inverter.ac.frequency")
	if frequencyProjection.Outcome != projection.ProjectionWithheld || frequencyProjection.Reason == nil || *frequencyProjection.Reason != pvReasonFieldInvalid {
		t.Fatalf("stale retained frequency projection=%+v", frequencyProjection)
	}
	activePower := pvCoreEnvelope(t, after.snapshot, "pv.ac.aggregate_active_power")
	if len(activePower.Candidates) != 1 || activePower.Candidates[0].Revision != "2" || !pvCoreHasSelection(after.selections, activePower.Key) {
		t.Fatalf("healthy active-power field was not refreshed: %+v", activePower)
	}
}

func TestPVPublicationCoreReturnsDeterministicImmutableReadback(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	draft := pvCoreDraft(t, observedFroniusFloatControlsWords(), "source-epoch:pv:test-a", 1, 2_000)
	if _, err := core.ingest(draft); err != nil {
		t.Fatal(err)
	}
	first, err := core.currentForValidation(draft.assetID)
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical := append([]byte(nil), first.canonical...)
	wantView, err := pvCoreViewJSON(first)
	if err != nil {
		t.Fatal(err)
	}
	first.canonical[0] ^= 0xff
	first.snapshot.Facts[0].Candidates[0].Value = nil
	first.selections = nil
	first.projection.Dispositions = nil

	second, err := core.currentForValidation(draft.assetID)
	if err != nil {
		t.Fatal(err)
	}
	gotView, err := pvCoreViewJSON(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(second.canonical, wantCanonical) || !bytes.Equal(gotView, wantView) {
		t.Fatal("caller mutation changed retained immutable readback")
	}
	reencoded, err := semreg.CanonicalJSON(second.snapshot)
	if err != nil || !bytes.Equal(reencoded, second.canonical) {
		t.Fatalf("canonical snapshot drift err=%v", err)
	}
}

func TestAdapterPublishesOneSemRegPVProjection(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().Add(-time.Second)
	adapter := &Adapter{semanticPV: core, pvSourceEpoch: "source-epoch:pv:adapter-test", startedWall: started.UTC(), startedMono: started, wallNow: time.Now, monotonicNow: time.Now}
	if err := adapter.publishSemanticPV(pvCoreObservation(t, observedFroniusFloatControlsWords(), 1, 2)); err != nil {
		t.Fatal(err)
	}
	if _, ok := adapter.SemanticPVCurrentByAsset("pv-asset-5c57adba3ad8a529617c1f8627ae32f1"); !ok {
		t.Fatal("missing public SemReg projection")
	}
}

func TestAdapterReevaluatesPVFreshnessAtEveryRead(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	started, current := time.Now(), time.Now()
	adapter := &Adapter{semanticPV: core, pvSourceEpoch: "source-epoch:pv:read-time", startedWall: started.UTC(), startedMono: started}
	adapter.wallNow = func() time.Time { return current }
	adapter.monotonicNow = func() time.Time { return current }
	if err := adapter.publishSemanticPV(pvCoreObservation(t, observedFroniusFloatControlsWords(), 1, 2)); err != nil {
		t.Fatal(err)
	}
	asset := "pv-asset-5c57adba3ad8a529617c1f8627ae32f1"
	fresh, ok := adapter.SemanticPVCurrentByAsset(asset)
	if !ok || pvCoreCurrentFreshness(t, fresh.Snapshot, fresh.Evaluation, "pv.ac.frequency") != semreg.FreshnessFresh {
		t.Fatalf("initial current view=%+v ok=%t", fresh.Evaluation, ok)
	}
	current = started.Add(31 * time.Second)
	stale, ok := adapter.SemanticPVCurrentByAsset(asset)
	if !ok || pvCoreCurrentFreshness(t, stale.Snapshot, stale.Evaluation, "pv.ac.frequency") != semreg.FreshnessStale || pvCoreHasSelection(stale.Selections, pvCoreEnvelope(t, stale.Snapshot, "pv.ac.frequency").Key) {
		t.Fatalf("stale read view=%+v selections=%+v ok=%t", stale.Evaluation, stale.Selections, ok)
	}
	current = started.Add(301 * time.Second)
	expired, ok := adapter.SemanticPVCurrentByAsset(asset)
	if !ok || pvCoreCurrentFreshness(t, expired.Snapshot, expired.Evaluation, "pv.ac.frequency") != semreg.FreshnessExpired || pvCoreHasSelection(expired.Selections, pvCoreEnvelope(t, expired.Snapshot, "pv.ac.frequency").Key) {
		t.Fatalf("expired read view=%+v selections=%+v ok=%t", expired.Evaluation, expired.Selections, ok)
	}
	if !bytes.Equal(fresh.Canonical, stale.Canonical) || !bytes.Equal(stale.Canonical, expired.Canonical) || fresh.Snapshot.SnapshotID != expired.Snapshot.SnapshotID {
		t.Fatal("read-time freshness mutated the immutable SemReg snapshot")
	}
}

func TestAdapterPVClockUsesMonotonicElapsedAcrossWallAdjustment(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	wallAdjusted := time.Unix(started.Unix()-3600, 0)
	monotonicAdvanced := started.Add(time.Minute)
	adapter := &Adapter{semanticPV: core, pvSourceEpoch: "source-epoch:pv:clock", startedWall: started.UTC(), startedMono: started,
		wallNow: func() time.Time { return wallAdjusted }, monotonicNow: func() time.Time { return monotonicAdvanced }}
	context, err := adapter.semanticPVReadContext()
	if err != nil {
		t.Fatalf("wall adjustment invalidated monotonic context: %v", err)
	}
	if context.EvaluatedAt.UnixNanoseconds != semreg.Int64(strconv.FormatInt(started.UTC().UnixNano(), 10)) || context.EvaluateMonotonic.Nanoseconds != "60000000000" {
		t.Fatalf("wall-adjusted context=%+v", context)
	}
	if err := adapter.publishSemanticPV(pvCoreObservation(t, observedFroniusFloatControlsWords(), 1, 2)); err != nil {
		t.Fatalf("wall adjustment blocked valid publication: %v", err)
	}
}

func TestAdapterPVReadClampsWallRollbackToDetachedSnapshotFloor(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	firstWall := started.Add(10 * time.Second)
	rollbackWall := started.Add(5 * time.Second)
	forwardWall := firstWall.Add(20 * time.Second)
	currentWall, currentMono := firstWall, firstWall
	adapter := &Adapter{semanticPV: core, pvSourceEpoch: "source-epoch:pv:read-wall-floor", startedWall: started.UTC(), startedMono: started,
		wallNow: func() time.Time { return currentWall }, monotonicNow: func() time.Time { return currentMono }}
	observation := pvCoreObservation(t, observedFroniusFloatControlsWords(), 1, 2)
	if err := adapter.publishSemanticPV(observation); err != nil {
		t.Fatal(err)
	}
	identity, _, err := resolvePVPublicationIdentity(observation)
	if err != nil {
		t.Fatal(err)
	}
	asset := "pv-asset-" + pvCoreRawHash(identity)[:32]
	published, ok := adapter.SemanticPVCurrentByAsset(asset)
	if !ok {
		t.Fatal("published public view unavailable")
	}
	canonical := append([]byte(nil), published.Canonical...)

	// The process wall clock rolls back after T1, while monotonic time remains
	// valid. The selected snapshot's committed T1 floor keeps evaluation valid.
	currentWall = rollbackWall
	rolledBack, ok := adapter.SemanticPVCurrentByAsset(asset)
	if !ok {
		t.Fatal("wall rollback made the detached public snapshot unavailable")
	}
	if rolledBack.Evaluation.Context.EvaluatedAt.UnixNanoseconds != semreg.Int64(strconv.FormatInt(firstWall.UTC().UnixNano(), 10)) {
		t.Fatalf("rollback read wall=%s; want committed floor %d", rolledBack.Evaluation.Context.EvaluatedAt.UnixNanoseconds, firstWall.UTC().UnixNano())
	}
	if rolledBack.Snapshot.SnapshotID != published.Snapshot.SnapshotID || !bytes.Equal(rolledBack.Canonical, canonical) {
		t.Fatal("wall-floor read changed the selected immutable snapshot")
	}

	// A later refresh while the wall is still rolled back must carry the prior
	// T1 floor forward rather than making the per-view floor regress to T0.
	updatedWords := append([]uint16(nil), observedFroniusFloatControlsWords()...)
	pvCoreSetFloat(updatedWords, 20, 4_321.5)
	currentMono = firstWall.Add(time.Second)
	if err := adapter.publishSemanticPV(pvCoreObservation(t, updatedWords, 3, 4)); err != nil {
		t.Fatalf("rollback-time refresh: %v", err)
	}
	rollbackRefresh, ok := adapter.SemanticPVCurrentByAsset(asset)
	if !ok || rollbackRefresh.Evaluation.Context.EvaluatedAt.UnixNanoseconds != semreg.Int64(strconv.FormatInt(firstWall.UTC().UnixNano(), 10)) {
		t.Fatalf("rollback refresh floor=%+v ok=%t", rollbackRefresh.Evaluation.Context, ok)
	}
	if rollbackRefresh.Snapshot.SnapshotID == published.Snapshot.SnapshotID {
		t.Fatal("rollback-time refresh did not publish its distinct immutable snapshot")
	}
	refreshCanonical := append([]byte(nil), rollbackRefresh.Canonical...)

	currentWall, currentMono = forwardWall, forwardWall
	forward, ok := adapter.SemanticPVCurrentByAsset(asset)
	if !ok || forward.Evaluation.Context.EvaluatedAt.UnixNanoseconds != semreg.Int64(strconv.FormatInt(forwardWall.UTC().UnixNano(), 10)) {
		t.Fatalf("forward wall read=%+v ok=%t", forward.Evaluation.Context, ok)
	}
	if forward.Snapshot.SnapshotID != rollbackRefresh.Snapshot.SnapshotID || !bytes.Equal(forward.Canonical, refreshCanonical) {
		t.Fatal("forward read changed the selected immutable snapshot")
	}
}

func TestAdapterPVConcurrentRefreshAndReadKeepsPublicViewAvailable(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	adapter := &Adapter{semanticPV: core, pvSourceEpoch: "source-epoch:pv:concurrent-read-floor", startedWall: started.UTC(), startedMono: started,
		profiles: make(map[string]ProfileObservationRecord), qualifications: make(map[string]sunSpecQualificationRecord), refreshEvidence: make(map[string]sunSpecQualificationRecord),
		wallNow: time.Now, monotonicNow: time.Now}
	words := observedFroniusFloatControlsWords()
	initial := pvCoreObservation(t, words, 1, 2)
	if err := adapter.RecordSunSpecQualificationObservation(initial); err != nil {
		t.Fatal(err)
	}
	identity, _, err := resolvePVPublicationIdentity(initial)
	if err != nil {
		t.Fatal(err)
	}
	asset := "pv-asset-" + pvCoreRawHash(identity)[:32]
	errCh := make(chan error, 1)
	var readers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for attempt := 0; attempt < 64; attempt++ {
				view, ok := adapter.SemanticPVCurrentByAsset(asset)
				if !ok || len(view.Canonical) == 0 {
					select {
					case errCh <- errors.New("concurrent public PV read became unavailable"):
					default:
					}
					return
				}
			}
		}()
	}
	for refresh := 0; refresh < 16; refresh++ {
		updated := append([]uint16(nil), words...)
		pvCoreSetFloat(updated, 20, float32(4_000+refresh))
		if err := adapter.RecordSunSpecCurrentObservation(pvCoreObservation(t, updated, uint64(10+refresh), uint64(20+refresh))); err != nil {
			t.Fatalf("refresh %d: %v", refresh, err)
		}
	}
	readers.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

func TestAdapterPVReadCapturesContextAfterDetachingSnapshot(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	firstReceipt := started.Add(time.Second)
	adapter := &Adapter{semanticPV: core, pvSourceEpoch: "source-epoch:pv:read-interleave", startedWall: started.UTC(), startedMono: started,
		wallNow: func() time.Time { return firstReceipt }, monotonicNow: func() time.Time { return firstReceipt }}
	words := observedFroniusFloatControlsWords()
	first := pvCoreObservation(t, words, 1, 2)
	if err := adapter.publishSemanticPV(first); err != nil {
		t.Fatal(err)
	}
	identity, _, err := resolvePVPublicationIdentity(first)
	if err != nil {
		t.Fatal(err)
	}
	asset := "pv-asset-" + pvCoreRawHash(identity)[:32]
	before, ok := adapter.SemanticPVCurrentByAsset(asset)
	if !ok {
		t.Fatal("initial public view unavailable")
	}

	contextStarted := make(chan struct{})
	releaseContext := make(chan struct{})
	adapter.wallNow = func() time.Time {
		close(contextStarted)
		<-releaseContext
		return firstReceipt
	}
	result := make(chan SemanticPVCurrent, 1)
	available := make(chan bool, 1)
	go func() {
		view, ok := adapter.SemanticPVCurrentByAsset(asset)
		result <- view
		available <- ok
	}()
	<-contextStarted

	updatedWords := append([]uint16(nil), words...)
	pvCoreSetFloat(updatedWords, 20, 4_321.5)
	updated := pvCoreObservation(t, updatedWords, 3, 4)
	secondReceipt := firstReceipt.Add(time.Second)
	draft, err := buildPVPublicationDraft(updated, pvPublicationLifecycle{
		sourceEpochID: adapter.pvSourceEpoch, driverGeneration: "1", sourceStartedAt: pvPublicationWall(adapter.startedWall),
		receivedAt: pvPublicationWall(secondReceipt), receiptMonotonic: pvPublicationMonotonic(secondReceipt.Sub(started)),
		evaluatedAt: pvPublicationWall(secondReceipt), evaluateMonotonic: pvPublicationMonotonic(secondReceipt.Sub(started)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.ingest(draft); err != nil {
		t.Fatal(err)
	}
	close(releaseContext)
	view, ok := <-result, <-available
	if !ok {
		t.Fatal("read became unavailable when a later receipt committed during context capture")
	}
	if view.Snapshot.SnapshotID != before.Snapshot.SnapshotID {
		t.Fatalf("read did not evaluate its detached snapshot: got=%q want=%q", view.Snapshot.SnapshotID, before.Snapshot.SnapshotID)
	}
}

func TestAdapterQualificationEvidenceStagesBeforePublicationAndRollsBack(t *testing.T) {
	core, err := newPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	adapter := &Adapter{semanticPV: core, profiles: make(map[string]ProfileObservationRecord), qualifications: make(map[string]sunSpecQualificationRecord), refreshEvidence: make(map[string]sunSpecQualificationRecord),
		startedWall: started.UTC(), startedMono: started, wallNow: time.Now, monotonicNow: time.Now}
	blocked := pvCoreObservation(t, observedFroniusFloatControlsWords(), 11, 12)
	if err := adapter.RecordSunSpecQualificationObservation(blocked); err == nil {
		t.Fatal("blocked qualification publication succeeded without a source epoch")
	}
	if len(adapter.qualifications) != 0 || len(core.assets) != 0 {
		t.Fatalf("blocked qualification left partial evidence/publication: qualifications=%d assets=%d", len(adapter.qualifications), len(core.assets))
	}

	adapter.pvSourceEpoch = "source-epoch:pv:qualification-stage"
	observation := pvCoreObservation(t, observedFroniusFloatControlsWords(), 21, 22)
	identity, _, err := resolvePVPublicationIdentity(observation)
	if err != nil {
		t.Fatal(err)
	}
	asset := "pv-asset-" + pvCoreRawHash(identity)[:32]
	var wait sync.WaitGroup
	errCh := make(chan error, 1)
	wait.Add(1)
	go func() {
		defer wait.Done()
		errCh <- adapter.RecordSunSpecQualificationObservation(observation)
	}()
	observedPublic := false
	for attempt := 0; attempt < 1000; attempt++ {
		if view, ok := adapter.SemanticPVCurrentByAsset(asset); ok {
			observedPublic = true
			retainedObservation, encoded, retained := adapter.SunSpecQualificationObservation(observation.Capability().ProfileID(), observation.SampleID())
			if !retained || len(encoded) == 0 {
				t.Fatal("public qualification snapshot was visible before its evidence")
			}
			if replay, err := retainedObservation.Replay(); err != nil || len(replay.SourceViews()) == 0 {
				t.Fatalf("public qualification evidence is not replayable: %v", err)
			}
			wantDigest := sunSpecObservationDigest(encoded)
			found := false
			for _, envelope := range view.Snapshot.Facts {
				for _, candidate := range envelope.Candidates {
					for _, evidence := range candidate.Evidence {
						if evidence.Kind == "sunspec.qualification_observation" && string(evidence.Digest) == wantDigest {
							found = true
						}
					}
				}
			}
			if !found {
				t.Fatal("public qualification snapshot did not cite its retained evidence")
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !observedPublic {
		t.Fatal("concurrent reader never observed the qualification publication")
	}
	wait.Wait()
	if err := <-errCh; err != nil {
		t.Fatalf("staged qualification publication: %v", err)
	}
	if _, _, ok := adapter.SunSpecQualificationObservation(observation.Capability().ProfileID(), observation.SampleID()); !ok {
		t.Fatal("successful qualification lost staged evidence")
	}
}

func pvCoreDraft(t *testing.T, words []uint16, epoch string, generation uint64, receipt uint64) pvPublicationDraft {
	t.Helper()
	observation := pvCoreObservation(t, words, receipt/10+1, receipt/10+2)
	draft, err := buildPVPublicationDraft(observation, pvCoreLifecycle(epoch, generation, receipt))
	if err != nil {
		t.Fatal(err)
	}
	return draft
}

func pvCoreLifecycle(epoch string, generation uint64, receipt uint64) pvPublicationLifecycle {
	return pvPublicationLifecycle{
		sourceEpochID:     semreg.SourceEpochID(epoch),
		driverGeneration:  semreg.Uint64(strconv.FormatUint(generation, 10)),
		sourceStartedAt:   pvCoreWall(1_000),
		receivedAt:        pvCoreWall(receipt),
		receiptMonotonic:  pvCoreMonotonic(receipt),
		evaluatedAt:       pvCoreWall(receipt + 10),
		evaluateMonotonic: pvCoreMonotonic(receipt + 10),
	}
}

func pvCoreWall(nanoseconds uint64) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatUint(nanoseconds, 10)), ClockID: "clock.utc", UncertaintyNS: "0"}
}

func pvCoreMonotonic(nanoseconds uint64) semreg.MonotonicPoint {
	return semreg.MonotonicPoint{ClockEpochID: "clock-epoch:pv-core-test", Nanoseconds: semreg.Uint64(strconv.FormatUint(nanoseconds, 10))}
}

func pvCoreObservation(t *testing.T, words []uint16, pollGeneration, deadlineIdentity uint64) modbusreg.SunSpecQualificationObservation {
	t.Helper()
	registry, err := modbusreg.NewStandardSunSpecDecoderRegistry(modbusreg.SunSpecModelsRevisionV1)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := modbusreg.NewSunSpecChainPlan(modbusreg.SunSpecChainPlanSpec{
		SchemaRevision: modbusreg.SunSpecModelsRevisionV1,
		BaseCandidates: []uint16{sunSpecBaseAddress},
		Limits:         modbusreg.SunSpecChainLimits{MaxTotalWords: sunSpecMaxTotalWords, MaxOccurrences: sunSpecMaxOccurrences},
		DecoderKeys:    registry.DecoderKeys(),
	})
	if err != nil {
		t.Fatal(err)
	}
	chain, offset := modbusreg.NewSunSpecChain(plan), 0
	var completed modbusreg.SunSpecChainSnapshot
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
			AuthorizationScope: "fixture:pv-publication-core", PollGeneration: pollGeneration, DeadlineIdentity: deadlineIdentity,
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
			break
		}
	}
	observation, err := modbusreg.NewSunSpecQualificationObservation(registry, completed)
	if err != nil {
		t.Fatal(err)
	}
	return observation
}

func pvCoreSetFloat(words []uint16, payloadOffset int, value float32) {
	const payloadStart = 2 + 67 + 2
	bits := math.Float32bits(value)
	words[payloadStart+payloadOffset] = uint16(bits >> 16)
	words[payloadStart+payloadOffset+1] = uint16(bits)
}

func pvCoreDisposition(t *testing.T, draft pvPublicationDraft, nativeID string) projection.ProjectionDisposition {
	t.Helper()
	itemID := pvProjectionItemID(nativeID)
	for _, disposition := range draft.dispositions {
		if disposition.ItemID == itemID {
			return disposition
		}
	}
	t.Fatalf("missing disposition for %s", nativeID)
	return projection.ProjectionDisposition{}
}

func pvCoreDispositionReport(t *testing.T, report projection.ProjectionReport, nativeID string) projection.ProjectionDisposition {
	t.Helper()
	itemID := pvProjectionItemID(nativeID)
	for _, disposition := range report.Dispositions {
		if disposition.ItemID == itemID {
			return disposition
		}
	}
	t.Fatalf("missing report disposition for %s", nativeID)
	return projection.ProjectionDisposition{}
}

func pvCoreHasFact(facts []semreg.FactCandidate, factID semreg.DefinitionID) bool {
	for _, fact := range facts {
		if fact.Key.FactID == factID {
			return true
		}
	}
	return false
}

func pvCoreEnvelope(t *testing.T, snapshot semreg.Snapshot, factID semreg.DefinitionID) semreg.FactEnvelope {
	t.Helper()
	for _, envelope := range snapshot.Facts {
		if envelope.Key.FactID == factID {
			return envelope
		}
	}
	t.Fatalf("missing fact envelope %s", factID)
	return semreg.FactEnvelope{}
}

func pvCoreCurrentFreshness(t *testing.T, snapshot semreg.Snapshot, view semreg.EvaluationView, factID semreg.DefinitionID) semreg.Freshness {
	t.Helper()
	envelope := pvCoreEnvelope(t, snapshot, factID)
	if len(envelope.Candidates) != 1 {
		t.Fatalf("fact %s candidates=%d", factID, len(envelope.Candidates))
	}
	freshness, ok := pvCoreEvaluatedFreshness(view, envelope.Candidates[0].CandidateID)
	if !ok {
		t.Fatalf("fact %s has no evaluated candidate", factID)
	}
	return freshness
}

func pvCoreEvaluatedFreshness(view semreg.EvaluationView, candidateID semreg.CandidateID) (semreg.Freshness, bool) {
	for _, fact := range view.Facts {
		if fact.CandidateID == candidateID {
			return fact.Freshness, true
		}
	}
	return "", false
}

func pvCoreHasSelection(selections []semreg.Selection, key semreg.FactKey) bool {
	for _, selection := range selections {
		if selection.Key.FactID == key.FactID && string(selection.Key.PackID) == string(key.PackID) && string(selection.Key.PackVersion) == string(key.PackVersion) {
			return true
		}
	}
	return false
}

func pvCoreHasCurrentCursor(snapshot semreg.Snapshot, epoch semreg.SourceEpochID, generation semreg.Uint64) bool {
	for _, cursor := range snapshot.Cursors {
		if cursor.SourceEpochID == epoch && cursor.DriverGeneration == generation && !cursor.Fenced {
			return true
		}
	}
	return false
}

func pvCoreHasCurrentSource(snapshot semreg.Snapshot, epoch semreg.SourceEpochID) bool {
	for _, source := range snapshot.Sources {
		if source.SourceEpochID == epoch && source.State == semreg.SourceCurrent {
			return true
		}
	}
	return false
}

func pvCoreViewJSON(view pvPublicationView) ([]byte, error) {
	return json.Marshal(struct {
		Snapshot   semreg.Snapshot             `json:"snapshot"`
		Canonical  []byte                      `json:"canonical"`
		Evaluation semreg.EvaluationView       `json:"evaluation"`
		Selections []semreg.Selection          `json:"selections"`
		Projection projection.ProjectionReport `json:"projection"`
	}{view.snapshot, view.canonical, view.evaluation, view.selections, view.projection})
}
