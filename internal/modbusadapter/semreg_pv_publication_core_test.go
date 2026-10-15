package modbusadapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sunspectest "github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter/testfixture"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
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

func TestPUBLIC05ValidPowerIsAvailableInPublicSemRegView(t *testing.T) {
	core, err := newTestPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	words := observedFroniusFloatControlsWords()
	pvCoreSetFloat(words, 20, 1_234.5)
	pvCoreSetFloat(words, 22, 50)
	draft := pvCoreDraft(t, words, "source-epoch:pv:public-05", 1, 2_000)
	if _, err := core.ingest(draft); err != nil {
		t.Fatal(err)
	}
	view, err := core.publicView(draft.assetID)
	if err != nil {
		t.Fatal(err)
	}
	power := pvCoreEnvelope(t, view.snapshot, "pv.ac.aggregate_active_power")
	if len(power.Candidates) != 1 {
		t.Fatalf("active-power candidates=%d; want one", len(power.Candidates))
	}
	candidate := power.Candidates[0]
	if candidate.Value == nil || candidate.Value.Quantity == nil || candidate.Value.Quantity.Unit != "unit.watt" || candidate.Value.Quantity.Number.Coefficient != "12345" || candidate.Value.Quantity.Number.Exponent10 != -1 {
		t.Fatalf("active-power value=%+v; want exact 1234.5 W", candidate.Value)
	}
	if candidate.Quality.Qualification != semreg.QualificationQualified || candidate.Quality.Promotion != semreg.PromotionPromoted || len(candidate.Evidence) == 0 || candidate.BindingID == nil || candidate.SourceEpochID == nil {
		t.Fatalf("active-power evidence boundary=%+v", candidate)
	}
	freshness, found := pvCoreEvaluatedFreshness(view.evaluation, candidate.CandidateID)
	if !found || freshness != semreg.FreshnessFresh || !pvCoreHasSelection(view.selections, power.Key) {
		t.Fatalf("active-power public availability freshness=%s found=%t selections=%+v", freshness, found, view.selections)
	}
	disposition := pvCoreDispositionReport(t, view.projection, "inverter.ac.power.active")
	if disposition.Outcome != projection.ProjectionExact || len(disposition.SourceKeys) != 1 || len(disposition.Loss) != 0 {
		t.Fatalf("active-power projection=%+v", disposition)
	}
}

func TestPVPublicationCoreEnforcesSequenceAndExpectedRevision(t *testing.T) {
	core, err := newTestPVPublicationCore()
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
	core, err := newTestPVPublicationCore()
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
	core, err := newTestPVPublicationCore()
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
	core, err := newTestPVPublicationCore()
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

func TestPUBLIC05InvalidFrequencyIsUnavailableWhilePowerRefreshes(t *testing.T) {
	core, err := newTestPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	initialWords := observedFroniusFloatControlsWords()
	pvCoreSetFloat(initialWords, 20, 1_234.5)
	pvCoreSetFloat(initialWords, 22, 50)
	first := pvCoreDraft(t, initialWords, "source-epoch:pv:test-a", 1, 2_000)
	if _, err := core.ingest(first); err != nil {
		t.Fatal(err)
	}
	before, err := core.currentForValidation(first.assetID)
	if err != nil {
		t.Fatal(err)
	}
	beforeFrequency := pvCoreEnvelope(t, before.snapshot, "pv.ac.frequency")
	if len(beforeFrequency.Candidates) != 1 {
		t.Fatalf("initial frequency candidates=%d; want one", len(beforeFrequency.Candidates))
	}
	initialFrequency := beforeFrequency.Candidates[0]
	if initialFrequency.Value == nil || initialFrequency.Value.Quantity == nil || initialFrequency.Value.Quantity.Unit != "unit.hertz" || initialFrequency.Value.Quantity.Number.Coefficient != "5" || initialFrequency.Value.Quantity.Number.Exponent10 != 1 {
		t.Fatalf("initial frequency=%+v; want exact 50 Hz", initialFrequency.Value)
	}
	words := observedFroniusFloatControlsWords()
	pvCoreSetFloat(words, 20, 4_321.5)
	pvCoreSetFloat(words, 22, 2_000) // invalid frequency; retain the prior candidate.
	second := pvCoreDraft(t, words, "source-epoch:pv:test-a", 1, 31_000_000_000)
	if pvCoreHasFact(second.facts, "pv.ac.frequency") {
		t.Fatal("invalid 2000 Hz refresh contributed a frequency candidate")
	}
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
	if len(after.snapshot.Facts) != len(before.snapshot.Facts) || len(after.projection.Requested) != 14 || len(after.projection.Dispositions) != 14 {
		t.Fatalf("partial refresh replaced the complete projection: facts=%d/%d requested=%d dispositions=%d", len(after.snapshot.Facts), len(before.snapshot.Facts), len(after.projection.Requested), len(after.projection.Dispositions))
	}
	frequency := pvCoreEnvelope(t, after.snapshot, "pv.ac.frequency")
	if len(frequency.Candidates) != 1 {
		t.Fatalf("retained frequency candidates=%d", len(frequency.Candidates))
	}
	if !reflect.DeepEqual(frequency.Candidates[0], initialFrequency) {
		t.Fatalf("invalid refresh replaced retained 50 Hz candidate:\n before=%+v\n after=%+v", initialFrequency, frequency.Candidates[0])
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
	powerValue := activePower.Candidates[0].Value
	if powerValue == nil || powerValue.Quantity == nil || powerValue.Quantity.Unit != "unit.watt" || powerValue.Quantity.Number.Coefficient != "43215" || powerValue.Quantity.Number.Exponent10 != -1 {
		t.Fatalf("healthy active-power value=%+v; want exact 4321.5 W", powerValue)
	}
	if freshness, ok := pvCoreEvaluatedFreshness(after.evaluation, activePower.Candidates[0].CandidateID); !ok || freshness != semreg.FreshnessFresh {
		t.Fatalf("healthy active-power freshness=%s found=%t", freshness, ok)
	}
	powerProjection := pvCoreDispositionReport(t, after.projection, "inverter.ac.power.active")
	if powerProjection.Outcome != projection.ProjectionExact || len(powerProjection.Loss) != 0 {
		t.Fatalf("healthy active-power projection=%+v", powerProjection)
	}
}

func TestPVPublicationCoreReturnsDeterministicImmutableReadback(t *testing.T) {
	core, err := newTestPVPublicationCore()
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
	core, err := newTestPVPublicationCore()
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

func TestAdapterPVCurrentSingleFollowsLatestIdentityRotation(t *testing.T) {
	core, err := newTestPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	current := started
	adapter := &Adapter{semanticPV: core, pvSourceEpoch: "source-epoch:pv:identity-rotation", startedWall: started.UTC(), startedMono: started,
		wallNow: func() time.Time { return current }, monotonicNow: func() time.Time { return current }}

	firstWords := observedFroniusFloatControlsWords()
	first := pvCoreObservation(t, firstWords, 1, 2)
	if err := adapter.publishSemanticPV(first); err != nil {
		t.Fatal(err)
	}
	firstIdentity, _, err := resolvePVPublicationIdentity(first)
	if err != nil {
		t.Fatal(err)
	}
	firstAsset := semreg.AssetID("pv-asset-" + pvCoreRawHash(firstIdentity)[:32])
	firstView, ok := adapter.SemanticPVCurrentSingleAt(current)
	if !ok {
		t.Fatal("initial single PV view unavailable")
	}
	if firstView.Snapshot.AssetID != firstAsset {
		t.Fatalf("initial active asset=%q want %q", firstView.Snapshot.AssetID, firstAsset)
	}

	current = current.Add(time.Second)
	rotatedWords := append([]uint16(nil), firstWords...)
	putSunSpecString(rotatedWords[52:68], "rotated-identity")
	rotated := pvCoreObservation(t, rotatedWords, 3, 4)
	if err := adapter.publishSemanticPV(rotated); err != nil {
		t.Fatal(err)
	}
	rotatedIdentity, _, err := resolvePVPublicationIdentity(rotated)
	if err != nil {
		t.Fatal(err)
	}
	rotatedAsset := semreg.AssetID("pv-asset-" + pvCoreRawHash(rotatedIdentity)[:32])
	if firstAsset == rotatedAsset || len(core.assets) != 2 {
		t.Fatalf("identity rotation was not retained: first=%q rotated=%q assets=%d", firstAsset, rotatedAsset, len(core.assets))
	}
	before, ok := adapter.SemanticPVCurrentByAsset(string(rotatedAsset))
	if !ok {
		t.Fatal("rotated public evidence unavailable by its identity")
	}
	active, ok := adapter.SemanticPVCurrentSingleAt(current)
	if !ok || active.Snapshot.AssetID != rotatedAsset || active.Snapshot.SnapshotID != before.Snapshot.SnapshotID {
		t.Fatalf("single metrics view did not select current identity: active=%q/%q want=%q/%q ok=%t", active.Snapshot.AssetID, active.Snapshot.SnapshotID, rotatedAsset, before.Snapshot.SnapshotID, ok)
	}
	if after, ok := adapter.SemanticPVCurrentByAsset(string(firstAsset)); !ok || after.Snapshot.AssetID != firstAsset {
		t.Fatal("identity rotation discarded retained native evidence")
	}
}

func TestAdapterPVCurrentSingleAtRetriesNewerSameAssetAtDetachedFloor(t *testing.T) {
	core, err := newTestPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	current := started.Add(time.Second)
	adapter := &Adapter{semanticPV: core, pvSourceEpoch: "source-epoch:pv:single-retry", startedWall: started.UTC(), startedMono: started,
		wallNow: func() time.Time { return current }, monotonicNow: func() time.Time { return current }}
	words := observedFroniusFloatControlsWords()
	if err := adapter.publishSemanticPV(pvCoreObservation(t, words, 1, 2)); err != nil {
		t.Fatal(err)
	}
	// RenderPrometheus captures this instant, then the same asset publishes a
	// newer snapshot before the scrape detaches it.
	captured := current
	current = current.Add(time.Second)
	updatedWords := append([]uint16(nil), words...)
	pvCoreSetFloat(updatedWords, 20, 4_321.5)
	if err := adapter.publishSemanticPV(pvCoreObservation(t, updatedWords, 3, 4)); err != nil {
		t.Fatal(err)
	}
	identity, _, err := resolvePVPublicationIdentity(pvCoreObservation(t, updatedWords, 5, 6))
	if err != nil {
		t.Fatal(err)
	}
	asset := "pv-asset-" + pvCoreRawHash(identity)[:32]
	newest, ok := adapter.SemanticPVCurrentByAsset(asset)
	if !ok {
		t.Fatal("newer same-asset view unavailable")
	}
	selected, selectedOK := core.singleAssetID()
	if selected != semreg.AssetID(asset) || !selectedOK {
		t.Fatalf("active asset=%q/%t want=%q", selected, selectedOK, asset)
	}
	view, ok := adapter.SemanticPVCurrentSingleAt(captured)
	if !ok || view.Snapshot.SnapshotID != newest.Snapshot.SnapshotID {
		t.Fatalf("captured scrape did not retry detached newer tuple: got=%q want=%q ok=%t", view.Snapshot.SnapshotID, newest.Snapshot.SnapshotID, ok)
	}
}

func TestAdapterReevaluatesPVFreshnessAtEveryRead(t *testing.T) {
	core, err := newTestPVPublicationCore()
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
	core, err := newTestPVPublicationCore()
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
	core, err := newTestPVPublicationCore()
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
	core, err := newTestPVPublicationCore()
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
	core, err := newTestPVPublicationCore()
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
	core, err := newTestPVPublicationCore()
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
	return pvCoreScopedObservation(t, words, pollGeneration, deadlineIdentity, "fixture:pv-publication-core")
}

// pvCoreScopedObservation replays words under one authorization scope; a
// different scope is a different native resource tuple and PV source.
func pvCoreScopedObservation(t *testing.T, words []uint16, pollGeneration, deadlineIdentity uint64, scope string) modbusreg.SunSpecQualificationObservation {
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
			AuthorizationScope: scope, PollGeneration: pollGeneration, DeadlineIdentity: deadlineIdentity,
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
	sunspectest.SetFloat(words, payloadOffset, value)
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

// Characterization goldens for issue #1024. They were captured from the
// pre-hub PV publication path and must stay byte-identical when the kernel
// moves behind the semantic publication hub. Regenerate them only on a
// deliberate, reviewed change with HELIANTHUS_UPDATE_CHARACTERIZATION=1.

type pvCharacterizationEndpoint struct {
	*fakeEndpoint
	advance func()
}

func (endpoint pvCharacterizationEndpoint) OpenConnection(connection net.Conn) (modbus.TCPConnectionHandle, error) {
	endpoint.advance()
	return endpoint.fakeEndpoint.OpenConnection(connection)
}

func TestPVCharacterizationFirstIncarnationLegacyIdentityAndCanonicalSnapshots(t *testing.T) {
	origin := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	current := origin
	advance := func() {
		clockMu.Lock()
		current = current.Add(time.Second)
		clockMu.Unlock()
	}
	set := func(at time.Time) {
		clockMu.Lock()
		current = at
		clockMu.Unlock()
	}
	config := validConfig()
	config.Clock = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return current
	}
	client, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	// The factory, the dial and the open each advance the injected clock, so
	// a PV epoch sampled anywhere but after all three changes every golden.
	adapter, err := Start(context.Background(), config,
		func(context.Context, string, string) (net.Conn, error) { advance(); return client, nil },
		func(modbus.TCPEndpointConfig) (Endpoint, error) {
			advance()
			return pvCharacterizationEndpoint{fakeEndpoint: &fakeEndpoint{}, advance: advance}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close() })

	wantEpoch := pvCharacterizationEpoch(config.Endpoint.Endpoint, origin.Add(3*time.Second))
	words := observedFroniusFloatControlsWords()
	set(origin.Add(5 * time.Second))
	if err := adapter.RecordSunSpecQualificationObservation(pvCoreObservation(t, words, 1, 2)); err != nil {
		t.Fatal(err)
	}
	first, ok := adapter.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("first PV publication unavailable")
	}
	if got := pvCharacterizationSourceEpoch(t, first.Snapshot); got != wantEpoch {
		t.Fatalf("PV epoch=%q want %q: the epoch sample point moved", got, wantEpoch)
	}
	set(origin.Add(10 * time.Second))
	refreshed := append([]uint16(nil), words...)
	pvCoreSetFloat(refreshed, 20, 4_321.5)
	if err := adapter.RecordSunSpecCurrentObservation(pvCoreObservation(t, refreshed, 3, 4)); err != nil {
		t.Fatal(err)
	}
	set(origin.Add(12 * time.Second))
	second, ok := adapter.SemanticPVCurrentByAsset(string(first.Snapshot.AssetID))
	if !ok {
		t.Fatal("second PV publication unavailable")
	}
	single, ok := adapter.SemanticPVCurrentSingleAt(origin.Add(14 * time.Second))
	if !ok {
		t.Fatal("single PV view unavailable")
	}
	dir := filepath.Join("testdata", "characterization", "pv_first_incarnation")
	pvCharacterizationGolden(t, dir, "ids.json", pvCharacterizationIdentity(t, second.Snapshot))
	pvCharacterizationGolden(t, dir, "publish1.canonical.json", first.Canonical)
	pvCharacterizationGolden(t, dir, "publish2.canonical.json", second.Canonical)
	pvCharacterizationGolden(t, dir, "read1.json", pvCharacterizationJSON(t, first))
	pvCharacterizationGolden(t, dir, "read2.json", pvCharacterizationJSON(t, second))
	pvCharacterizationGolden(t, dir, "single_at.json", pvCharacterizationJSON(t, single))
}

func pvCharacterizationEpoch(endpoint string, started time.Time) semreg.SourceEpochID {
	sum := sha256.Sum256([]byte("semantic-pv-source-epoch\x00" + endpoint + "\x00" + started.UTC().Format(time.RFC3339Nano)))
	return semreg.SourceEpochID("source-epoch:semantic-pv:" + hex.EncodeToString(sum[:])[:32])
}

func pvCharacterizationSourceEpoch(t *testing.T, snapshot semreg.Snapshot) semreg.SourceEpochID {
	t.Helper()
	for _, source := range snapshot.Sources {
		if source.State == semreg.SourceCurrent {
			return source.SourceEpochID
		}
	}
	t.Fatal("PV snapshot has no current source")
	return ""
}

func pvCharacterizationJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// pvCharacterizationIdentity summarizes every identifier family the hub
// minters must reproduce: asset, source, epoch, generation, binding, service,
// capability, candidate and cursor key.
func pvCharacterizationIdentity(t *testing.T, snapshot semreg.Snapshot) []byte {
	t.Helper()
	summary := map[string][]string{"asset": {string(snapshot.AssetID)}}
	add := func(kind string, fields ...string) {
		summary[kind] = append(summary[kind], strings.Join(fields, " "))
	}
	for _, source := range snapshot.Sources {
		add("sources", string(source.SourceID), string(source.SourceEpochID), string(source.State))
	}
	for _, binding := range snapshot.Bindings {
		add("bindings", string(binding.BindingID), string(binding.SourceID), string(binding.SourceEpochID), string(binding.DriverGeneration), string(binding.State))
	}
	for _, link := range snapshot.IdentityLinks {
		add("identity_links", string(link.BindingID), string(link.State))
	}
	for _, service := range snapshot.Services {
		add("services", string(service.InstanceID), string(service.Definition.ID), string(service.BindingID))
	}
	for _, capability := range snapshot.Capabilities {
		add("capabilities", string(capability.InstanceID), string(capability.Definition.ID), string(capability.ServiceInstance), string(capability.BindingID))
	}
	for _, envelope := range snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			add("candidates", string(candidate.CandidateID), string(envelope.Key.FactID), string(candidate.Origin.OriginID))
		}
	}
	for _, cursor := range snapshot.Cursors {
		add("cursors", string(cursor.SourceID), string(cursor.SourceEpochID), string(cursor.DriverGeneration), string(cursor.LastSequence))
	}
	for _, values := range summary {
		sort.Strings(values)
	}
	encoded, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(encoded, '\n')
}

func pvCharacterizationGolden(t *testing.T, dir, name string, got []byte) {
	t.Helper()
	path := filepath.Join(dir, name)
	if os.Getenv("HELIANTHUS_UPDATE_CHARACTERIZATION") == "1" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read characterization golden %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("characterization golden %s changed:\n got: %s\nwant: %s", path, got, want)
	}
}

func newPVTestHub(t *testing.T) (*semhub.Hub, *semhub.Class) {
	t.Helper()
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	class, err := NewSemanticPVClass(hub)
	if err != nil {
		t.Fatal(err)
	}
	return hub, class
}

func startPVOnHub(t *testing.T, class *semhub.Class, now time.Time) *Adapter {
	t.Helper()
	config := validConfig()
	config.Clock = func() time.Time { return now }
	config.SemanticPV = class
	client, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	adapter, err := Start(context.Background(), config, func(context.Context, string, string) (net.Conn, error) { return client, nil },
		func(modbus.TCPEndpointConfig) (Endpoint, error) { return &fakeEndpoint{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func TestPVPublicViewFollowsTheHubControlGate(t *testing.T) {
	hub, class := newPVTestHub(t)
	now := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	adapter := startPVOnHub(t, class, now)
	t.Cleanup(func() { _ = adapter.Close() })
	if err := adapter.RecordSunSpecQualificationObservation(pvCoreObservation(t, observedFroniusFloatControlsWords(), 1, 2)); err != nil {
		t.Fatal(err)
	}
	current, ok := adapter.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("PV view unavailable")
	}
	source := current.Snapshot.Sources[0]
	if _, err := hub.Fence(semhub.FenceRequest{Source: source.SourceID, Epoch: source.SourceEpochID, Generation: 1, Reason: "lifecycle.driver_generation_replaced",
		Evidence:   []semreg.EvidenceRef{pvEvidence("sunspec.generation_fence", semreg.Digest("sha256:"+pvCoreHash("fence", nil)))},
		ObservedAt: pvPublicationWall(now), Monotonic: pvPublicationMonotonic(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, ok := adapter.SemanticPVCurrentByAsset(string(current.Snapshot.AssetID)); ok {
		t.Fatal("withdrawn PV view stayed public")
	}
	if _, ok := adapter.SemanticPVCurrentSingleAt(now.Add(time.Second)); ok {
		t.Fatal("withdrawn single PV view stayed public")
	}
	if view, err := hub.Inspect(current.Snapshot.AssetID); err != nil || view.Snapshot.Bindings[0].State != semreg.BindingFenced {
		t.Fatalf("hub inspect=%+v err=%v", view.Snapshot.Bindings, err)
	}
}

func TestPVDynamicDeclarationCollisionIsAPerAssetRefusal(t *testing.T) {
	hub, class := newPVTestHub(t)
	if _, err := hub.Declare(semhub.AssetSpec{AssetID: "pv-asset-5c57adba3ad8a529617c1f8627ae32f1", SourceID: "source:static-owner", Driver: "growatt_storage", Validators: []semreg.PackValidator{pvpack.New()}}); err != nil {
		t.Fatal(err)
	}
	subscription := hub.Subscribe()
	defer subscription.Close()
	adapter := startPVOnHub(t, class, time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC))
	t.Cleanup(func() { _ = adapter.Close() })
	err := adapter.RecordSunSpecQualificationObservation(pvCoreObservation(t, observedFroniusFloatControlsWords(), 1, 2))
	var collision *semhub.CollisionError
	if !errors.As(err, &collision) || !strings.Contains(err.Error(), "publish SemReg PV observation") {
		t.Fatalf("colliding PV asset=%v", err)
	}
	refused := false
	for !refused {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		event, nextErr := subscription.Next(ctx)
		cancel()
		if nextErr != nil {
			t.Fatal("no DeclareRefused event")
		}
		refused = event.Kind == semhub.EventDeclareRefused && len(event.Drivers) == 2 && event.Drivers[0] == "pv_sunspec" && event.Drivers[1] == "growatt_storage"
	}
	rotated := observedFroniusFloatControlsWords()
	putSunSpecString(rotated[52:68], "rotated-identity")
	if err := adapter.RecordSunSpecQualificationObservation(pvCoreObservation(t, rotated, 3, 4)); err != nil {
		t.Fatalf("refusal was process-wide: %v", err)
	}
}

func TestPVProvisionalSourceRetiresWithTheRuntime(t *testing.T) {
	hub, class := newPVTestHub(t)
	adapter := startPVOnHub(t, class, time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC))
	if err := adapter.RecordSunSpecQualificationObservation(pvCoreObservation(t, observedFroniusFloatControlsWords(), 1, 2)); err != nil {
		t.Fatal(err)
	}
	cursor := hub.Cursor()
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	events, err := hub.Since(cursor)
	if err != nil || len(events) != 1 || events[0].Kind != semhub.EventSourceRetired || !events[0].NoPublication {
		t.Fatalf("close events=%+v err=%v", events, err)
	}
	for _, info := range hub.Sources() {
		if info.Live {
			t.Fatalf("source stayed live after close: %+v", info)
		}
	}
}

// newTestPVPublicationCore binds a PV core to a fresh hub incarnation.
func newTestPVPublicationCore() (*pvPublicationCore, error) {
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		return nil, err
	}
	class, err := NewSemanticPVClass(hub)
	if err != nil {
		return nil, err
	}
	return newPVPublicationCore(class)
}

// testSemanticPVClass is the PV class of a fresh hub incarnation.
func testSemanticPVClass() *semhub.Class {
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		panic(err)
	}
	class, err := NewSemanticPVClass(hub)
	if err != nil {
		panic(err)
	}
	return class
}

func TestPVFailedGenerationReplacementKeepsThePreviousViewAndFencesNothing(t *testing.T) {
	core, err := newTestPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	words := observedFroniusFloatControlsWords()
	first := pvCoreDraft(t, words, "source-epoch:pv:lkg", 1, 2_000)
	if _, err := core.ingest(first); err != nil {
		t.Fatal(err)
	}
	before, err := core.publicView(first.assetID)
	if err != nil {
		t.Fatal(err)
	}
	cursor := core.hub.Cursor()
	replacement := pvCoreDraft(t, words, "source-epoch:pv:lkg", 2, 3_000)
	refused := errors.New("capacity breach")
	if _, err := core.ingestWithValidation(replacement, func(semreg.AssetID, semreg.Snapshot) error { return refused }); !errors.Is(err, refused) {
		t.Fatalf("replacement validation error=%v", err)
	}
	after, err := core.publicView(first.assetID)
	if err != nil || after.snapshot.SnapshotID != before.snapshot.SnapshotID {
		t.Fatalf("previous public view was withdrawn or replaced: %v", err)
	}
	view, _ := core.hub.Inspect(first.assetID)
	if len(view.Snapshot.Fences) != 0 || core.hub.Cursor() != cursor {
		t.Fatalf("a failed replacement committed a fence: %+v", view.Snapshot.Fences)
	}
	if info, _ := core.hub.Source(first.source.SourceID); info.Generation != 1 || !info.Live {
		t.Fatalf("a failed replacement activated the new generation: %+v", info)
	}
	if _, err := core.ingest(replacement); err != nil {
		t.Fatalf("replacement retry=%v", err)
	}
}

func TestPVRejectedNewEpochDraftKeepsTheOldEpochWorking(t *testing.T) {
	core, err := newTestPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	words := observedFroniusFloatControlsWords()
	first := pvCoreDraft(t, words, "source-epoch:pv:keep-a", 1, 2_000)
	if _, err := core.ingest(first); err != nil {
		t.Fatal(err)
	}
	next := pvCoreDraft(t, words, "source-epoch:pv:keep-b", 1, 3_000)
	next.lifecycle.sourceStartedAt = pvCoreWall(2_500)
	next.source.StartedAt = next.lifecycle.sourceStartedAt
	refused := errors.New("capacity breach")
	if _, err := core.ingestWithValidation(next, func(semreg.AssetID, semreg.Snapshot) error { return refused }); !errors.Is(err, refused) {
		t.Fatalf("new-epoch validation error=%v", err)
	}
	if info, _ := core.hub.Source(first.source.SourceID); info.Epoch != "source-epoch:pv:keep-a" || !info.Live {
		t.Fatalf("a rejected new-epoch draft replaced the epoch: %+v", info)
	}
	refresh := pvCoreDraft(t, words, "source-epoch:pv:keep-a", 1, 4_000)
	if _, err := core.ingest(refresh); err != nil {
		t.Fatalf("old-epoch observation after a rejected new-epoch draft=%v", err)
	}
	retry := pvCoreDraft(t, words, "source-epoch:pv:keep-b", 1, 5_000)
	retry.lifecycle.sourceStartedAt = pvCoreWall(4_500)
	retry.source.StartedAt = retry.lifecycle.sourceStartedAt
	if _, err := core.ingest(retry); err != nil {
		t.Fatalf("new-epoch retry=%v", err)
	}
	if info, _ := core.hub.Source(first.source.SourceID); info.Epoch != "source-epoch:pv:keep-b" || !info.Live {
		t.Fatalf("committed new epoch=%+v", info)
	}
}

// onceFailingJournal fails the first retirement append for one source.
type onceFailingJournal struct {
	semhub.EpochJournal
	mu     sync.Mutex
	source semreg.SourceID
	failed bool
}

func (j *onceFailingJournal) Append(records ...semhub.EpochRecord) error {
	j.mu.Lock()
	for _, record := range records {
		if record.Kind == semhub.EventSourceRetired && record.Source == j.source && !j.failed {
			j.failed = true
			j.mu.Unlock()
			return errors.New("store unavailable")
		}
	}
	j.mu.Unlock()
	return j.EpochJournal.Append(records...)
}

func TestPVPendingRetirementIsRetriedBeforeRequalification(t *testing.T) {
	words := observedFroniusFloatControlsWords()
	rotated := append([]uint16(nil), words...)
	putSunSpecString(rotated[52:68], "second-inverter")
	scoped := func(w []uint16, scope, epoch string, receipt uint64, started uint64) pvPublicationDraft {
		t.Helper()
		lifecycle := pvCoreLifecycle(epoch, 1, receipt)
		if started != 0 {
			lifecycle.sourceStartedAt = pvCoreWall(started)
		}
		draft, err := buildPVPublicationDraft(pvCoreScopedObservation(t, w, receipt/10+1, receipt/10+2, scope), lifecycle)
		if err != nil {
			t.Fatal(err)
		}
		return draft
	}
	s1a := scoped(words, "fixture:pv-s1", "source-epoch:pv:r-a", 2_000, 0)
	s2a := scoped(rotated, "fixture:pv-s2", "source-epoch:pv:r-a", 2_100, 0)
	journal := &onceFailingJournal{EpochJournal: semhub.NewMemoryJournal(), source: s2a.source.SourceID}
	hub, err := semhub.New(semhub.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	class, err := NewSemanticPVClass(hub)
	if err != nil {
		t.Fatal(err)
	}
	core, err := newPVPublicationCore(class)
	if err != nil {
		t.Fatal(err)
	}
	if s1a.source.SourceID == s2a.source.SourceID || s1a.assetID == s2a.assetID {
		t.Fatal("fixture sources are not distinct")
	}
	for _, draft := range []pvPublicationDraft{s1a, s2a} {
		if _, err := core.ingest(draft); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := core.ingest(scoped(words, "fixture:pv-s1", "source-epoch:pv:r-b", 3_000, 2_500)); err != nil {
		t.Fatalf("S1 replacement epoch=%v", err)
	}
	if !journal.failed {
		t.Fatal("the old S2 retirement was never attempted")
	}
	if _, err := core.ingest(scoped(rotated, "fixture:pv-s2", "source-epoch:pv:r-b", 3_100, 2_500)); err != nil {
		t.Fatalf("S2 observation in the new epoch after the journal recovered=%v", err)
	}
	if info, _ := hub.Source(s2a.source.SourceID); info.Epoch != "source-epoch:pv:r-b" || !info.Live {
		t.Fatalf("S2 after recovery=%+v", info)
	}
}

// retireFailingJournal fails every retirement append while failing is set.
type retireFailingJournal struct {
	semhub.EpochJournal
	mu      sync.Mutex
	failing bool
}

func (j *retireFailingJournal) Append(records ...semhub.EpochRecord) error {
	j.mu.Lock()
	failing := j.failing
	j.mu.Unlock()
	for _, record := range records {
		if failing && record.Kind == semhub.EventSourceRetired {
			return errors.New("store unavailable")
		}
	}
	return j.EpochJournal.Append(records...)
}

func TestAdapterCloseRetriesAFailedProvisionalRetirement(t *testing.T) {
	journal := &retireFailingJournal{EpochJournal: semhub.NewMemoryJournal()}
	hub, err := semhub.New(semhub.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	class, err := NewSemanticPVClass(hub)
	if err != nil {
		t.Fatal(err)
	}
	core, err := newPVPublicationCore(class)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &fakeEndpoint{}
	started := time.Now().Add(-time.Second)
	adapter := &Adapter{endpoint: endpoint, semanticPV: core, pvSourceEpoch: "source-epoch:pv:close-retry", startedWall: started.UTC(), startedMono: started, wallNow: time.Now, monotonicNow: time.Now}
	if err := adapter.publishSemanticPV(pvCoreObservation(t, observedFroniusFloatControlsWords(), 1, 2)); err != nil {
		t.Fatal(err)
	}
	sources := hub.Sources()
	if len(sources) != 1 || !sources[0].Live {
		t.Fatalf("sources=%+v", sources)
	}
	journal.mu.Lock()
	journal.failing = true
	journal.mu.Unlock()
	if err := adapter.Close(); err == nil {
		t.Fatal("close reported success although the retirement was not journalled")
	}
	journal.mu.Lock()
	journal.failing = false
	journal.mu.Unlock()
	if err := adapter.Close(); err != nil {
		t.Fatalf("close retry=%v", err)
	}
	if info, _ := hub.Source(sources[0].Source); info.Live {
		t.Fatalf("source still live after a successful close retry: %+v", info)
	}
	endpoint.mu.Lock()
	calls := endpoint.closeCalls
	endpoint.mu.Unlock()
	if calls != 1 {
		t.Fatalf("endpoint closed %d times", calls)
	}
}

func TestPVRetryCoversDropsResolvedAndSupersededCovers(t *testing.T) {
	core, err := newTestPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	draft := pvCoreDraft(t, observedFroniusFloatControlsWords(), "source-epoch:pv:covers", 1, 2_000)
	if _, err := core.ingest(draft); err != nil {
		t.Fatal(err)
	}
	evidence := draft.source.RegistryEvidence
	result, err := core.hub.Fence(semhub.FenceRequest{Source: draft.source.SourceID, Epoch: "source-epoch:pv:covers", Generation: 1, Reason: "lifecycle.generation_replaced", Evidence: []semreg.EvidenceRef{evidence}, ObservedAt: pvCoreWall(3_000), Monotonic: draft.lifecycle.receiptMonotonic})
	if err != nil {
		t.Fatal(err)
	}
	unknown := semhub.ControlID{Incarnation: core.hub.Incarnation(), Source: draft.source.SourceID, N: 99}
	core.recordCover(result.ID)
	core.recordCover(unknown)
	done := make(chan struct{})
	go func() {
		core.retryCovers()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cover retry blocked")
	}
	core.lifecycleMu.Lock()
	defer core.lifecycleMu.Unlock()
	if len(core.covers) != 0 {
		t.Fatalf("covers kept=%+v", core.covers)
	}
}

func TestPVCoverFailureAfterCommitRetainsTheStagedEvidence(t *testing.T) {
	var (
		mu     sync.Mutex
		target semreg.AssetID
	)
	hub, err := semhub.New(semhub.Options{Faults: func(stage string, asset semreg.AssetID) {
		mu.Lock()
		hit := stage == "before-swap" && asset != "" && asset == target
		if hit {
			target = ""
		}
		mu.Unlock()
		if hit {
			panic("injected cover interruption")
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	class, err := NewSemanticPVClass(hub)
	if err != nil {
		t.Fatal(err)
	}
	core, err := newPVPublicationCore(class)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	current := started
	adapter := &Adapter{semanticPV: core, pvSourceEpoch: "source-epoch:pv:cover-a", startedWall: started.UTC(), startedMono: started,
		wallNow: func() time.Time { return current }, monotonicNow: func() time.Time { return current },
		qualifications: make(map[string]sunSpecQualificationRecord), profiles: make(map[string]ProfileObservationRecord), refreshEvidence: make(map[string]sunSpecQualificationRecord)}
	firstWords := observedFroniusFloatControlsWords()
	first := pvCoreObservation(t, firstWords, 1, 2)
	if err := adapter.RecordSunSpecQualificationObservation(first); err != nil {
		t.Fatal(err)
	}
	firstIdentity, _, err := resolvePVPublicationIdentity(first)
	if err != nil {
		t.Fatal(err)
	}
	firstAsset := semreg.AssetID("pv-asset-" + pvCoreRawHash(firstIdentity)[:32])
	current = current.Add(time.Second)
	rotatedWords := append([]uint16(nil), firstWords...)
	putSunSpecString(rotatedWords[52:68], "rotated-identity")
	if err := adapter.RecordSunSpecQualificationObservation(pvCoreObservation(t, rotatedWords, 3, 4)); err != nil {
		t.Fatal(err)
	}
	// A new epoch on the rotated asset retires the old epoch on the first
	// asset through a cover, whose submission is interrupted.
	current = current.Add(time.Second)
	adapter.startedWall = current.UTC()
	adapter.pvSourceEpoch = "source-epoch:pv:cover-b"
	mu.Lock()
	target = firstAsset
	mu.Unlock()
	observation := pvCoreObservation(t, rotatedWords, 5, 6)
	err = adapter.RecordSunSpecQualificationObservation(observation)
	var cover *semhub.CoverError
	if !errors.As(err, &cover) {
		t.Fatalf("record with an interrupted cover=%v, want a CoverError", err)
	}
	key := observation.Capability().ProfileID() + "\x00" + observation.SampleID()
	adapter.profileMu.Lock()
	_, retained := adapter.qualifications[key]
	adapter.profileMu.Unlock()
	if !retained {
		t.Fatal("the accepted publication's staged evidence was deleted")
	}
	if hub.Readable(firstAsset) {
		t.Fatal("the asset behind the interrupted cover is readable")
	}
	core.retryCovers()
	waitForPV(t, func() bool { return hub.Readable(firstAsset) })
	core.lifecycleMu.Lock()
	pending := len(core.covers)
	core.lifecycleMu.Unlock()
	if pending != 0 {
		t.Fatalf("covers still pending: %d", pending)
	}
}

func waitForPV(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
