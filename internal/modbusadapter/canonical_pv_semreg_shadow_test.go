package modbusadapter

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func TestCanonicalPVSemRegShadow_DefaultOffHasNoKernelOrShadowRecord(t *testing.T) {
	listener, _ := serveSunSpecChain(t, observedFroniusFloatControlsWords())
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	if adapter.canonicalShadow != nil {
		t.Fatal("default configuration constructed a SemReg shadow kernel")
	}
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "test:shadow-off", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 1, DeadlineIdentity: 1})
	if err != nil || result.Outcome != SunSpecQualificationGO {
		t.Fatalf("qualification=%+v err=%v", result, err)
	}
	if _, ok := adapter.CanonicalPVSemRegShadow(result.CapabilityID, result.SampleID); ok {
		t.Fatal("default-off adapter retained a SemReg shadow record")
	}
}

func TestCanonicalPVSemRegShadow_QualifiedObservationPublishesOneReadParityAndDetachment(t *testing.T) {
	listener, requests := serveSunSpecChain(t, observedFroniusFloatControlsWords())
	config := integrationConfig(t, "tcp://"+listener.Addr().String())
	config.CanonicalPVShadow.Mode = CanonicalPVShadowModeSemReg
	adapter, err := Start(context.Background(), config, realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "test:shadow", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 1, DeadlineIdentity: 1})
	if err != nil || result.Outcome != SunSpecQualificationGO {
		t.Fatalf("qualification=%+v err=%v", result, err)
	}
	// Qualification's chain is the sole native acquisition: the shadow consumes
	// its retained observation and does not create another read or scheduler.
	if got, want := len(requests()), len(result.Chain.SourceViews()); got != want {
		t.Fatalf("native reads=%d; want exactly the %d qualification reads", got, want)
	}
	legacy, _, ok := adapter.CanonicalPVSnapshot(result.CapabilityID, result.SampleID)
	if !ok {
		t.Fatal("missing retained legacy snapshot")
	}
	shadow, ok := adapter.CanonicalPVSemRegShadow(result.CapabilityID, result.SampleID)
	if !ok {
		t.Fatal("missing successful SemReg shadow record")
	}
	if len(shadow.Snapshot.Facts) != 11 || len(shadow.Evaluation.Facts) != 11 || len(shadow.Projection.Requested) != 14 || len(shadow.Projection.Dispositions) != 14 {
		t.Fatalf("shadow accounting facts=%d evaluation=%d requested=%d dispositions=%d", len(shadow.Snapshot.Facts), len(shadow.Evaluation.Facts), len(shadow.Projection.Requested), len(shadow.Projection.Dispositions))
	}
	if shadow.Snapshot.AssetID != semreg.AssetID(legacy.AssetRef) || len(shadow.Canonical) == 0 {
		t.Fatalf("shadow provenance=%+v canonical=%d", shadow.Snapshot.AssetID, len(shadow.Canonical))
	}
	assetIdentity := strings.TrimPrefix(legacy.AssetRef, "pv-asset-")
	for _, envelope := range shadow.Snapshot.Facts {
		if len(envelope.Key.Dimensions) != 1 || envelope.Key.Dimensions[0].Value.Text == nil {
			t.Fatalf("shadow fact dimension=%+v", envelope.Key)
		}
		value := *envelope.Key.Dimensions[0].Value.Text
		switch envelope.Key.Dimensions[0].ID {
		case "pv.dimension.inverter":
			if value != "inverter:"+assetIdentity {
				t.Fatalf("runtime inverter dimension=%q", value)
			}
		case "pv.dimension.system":
			if value != "system:"+assetIdentity {
				t.Fatalf("runtime system dimension=%q", value)
			}
		}
	}
	shadow.Snapshot.Facts[0].Candidates[0].Revision = "999"
	again, ok := adapter.CanonicalPVSemRegShadow(result.CapabilityID, result.SampleID)
	if !ok || again.Snapshot.Facts[0].Candidates[0].Revision == "999" {
		t.Fatal("shadow getter returned attached state")
	}
	if legacy.Facts[pv.NewFactKey(pv.FactOperatingState, pv.Dimensions{Scope: pv.ScopeTotal})].Value.Symbol != pv.OperatingStateOperating {
		t.Fatal("SemReg shadow changed the legacy public compatibility snapshot")
	}
}

func TestCanonicalPVSemRegShadow_RejectionRetainsLastGoodLegacyAndShadow(t *testing.T) {
	listener, _ := serveSunSpecChain(t, observedFroniusFloatControlsWords())
	config := integrationConfig(t, "tcp://"+listener.Addr().String())
	config.CanonicalPVShadow.Mode = CanonicalPVShadowModeSemReg
	adapter, err := Start(context.Background(), config, realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "test:shadow-reject", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 1, DeadlineIdentity: 1})
	if err != nil || initial.Outcome != SunSpecQualificationGO {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	asset := mustCanonicalPVAsset(t, adapter, initial)
	before, beforeAt, ok := adapter.CanonicalPVSnapshotByAsset(asset)
	if !ok {
		t.Fatal("missing initial legacy current slot")
	}
	shadowBefore, ok := adapter.CanonicalPVSemRegShadow(initial.CapabilityID, initial.SampleID)
	if !ok {
		t.Fatal("missing initial shadow")
	}
	adapter.canonicalShadow.compare = func(pv.Snapshot, semreg.Snapshot, semreg.EvaluationView, projection.ProjectionReport) error {
		return errors.New("forced shadow rejection")
	}
	refresh, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: 2, DeadlineIdentity: 2})
	if err != nil || refresh.Outcome != SunSpecQualificationGO {
		t.Fatalf("refresh=%+v err=%v", refresh, err)
	}
	after, afterAt, ok := adapter.CanonicalPVSnapshotByAsset(asset)
	if !ok || !afterAt.After(beforeAt) || after.Generation <= before.Generation {
		t.Fatalf("diagnostic shadow rejection failed to advance legacy current: before=%d/%s after=%d/%s", before.Generation, beforeAt, after.Generation, afterAt)
	}
	shadowAfter, ok := adapter.CanonicalPVSemRegShadow(initial.CapabilityID, initial.SampleID)
	if !ok || shadowAfter.Snapshot.SnapshotID != shadowBefore.Snapshot.SnapshotID {
		t.Fatal("rejected shadow replaced last-good retained shadow")
	}
	if adapter.canonicalShadow.lastFailure == "" {
		t.Fatal("diagnostic shadow rejection was not retained internally")
	}
}

func TestCanonicalPVSemRegShadow_CurrentRefreshAdvancesOneSourceWithoutExtraRetention(t *testing.T) {
	listener, _ := serveSunSpecChain(t, observedFroniusFloatControlsWords())
	config := integrationConfig(t, "tcp://"+listener.Addr().String())
	config.CanonicalPVShadow.Mode = CanonicalPVShadowModeSemReg
	adapter, err := Start(context.Background(), config, realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "test:shadow-refresh", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 1, DeadlineIdentity: 1})
	if err != nil || initial.Outcome != SunSpecQualificationGO {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	before, ok := adapter.CanonicalPVSemRegShadow(initial.CapabilityID, initial.SampleID)
	if !ok {
		t.Fatal("missing initial shadow")
	}
	currentBefore, ok := adapter.CanonicalPVSemRegCurrentShadow(string(before.Snapshot.AssetID))
	if !ok {
		t.Fatal("missing initial current shadow")
	}
	refresh, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: 2, DeadlineIdentity: 2})
	if err != nil || refresh.Outcome != SunSpecQualificationGO || refresh.ObservationCount != 0 {
		t.Fatalf("refresh=%+v err=%v", refresh, err)
	}
	// The retained qualification witness remains immutable; the current shadow
	// uses the same source/epoch cursor and does not consume that bounded store.
	after, ok := adapter.CanonicalPVSemRegShadow(initial.CapabilityID, initial.SampleID)
	if !ok || after.Snapshot.SnapshotID != before.Snapshot.SnapshotID || len(after.Snapshot.Sources) != 1 || len(after.Snapshot.Cursors) != 1 {
		t.Fatalf("refresh changed retained shadow=%v sources=%d cursors=%d", after.Snapshot.SnapshotID != before.Snapshot.SnapshotID, len(after.Snapshot.Sources), len(after.Snapshot.Cursors))
	}
	if len(adapter.qualifications) != 1 || len(adapter.canonicalShadow.byAsset) != 1 {
		t.Fatalf("refresh retention qualifications=%d shadow_assets=%d", len(adapter.qualifications), len(adapter.canonicalShadow.byAsset))
	}
	currentAfter, ok := adapter.CanonicalPVSemRegCurrentShadow(string(before.Snapshot.AssetID))
	if !ok || len(currentBefore.Snapshot.Cursors) != 1 || len(currentAfter.Snapshot.Cursors) != 1 || currentBefore.Snapshot.Cursors[0].LastSequence != "1" || currentAfter.Snapshot.Cursors[0].LastSequence != "2" || currentAfter.Snapshot.SnapshotID == currentBefore.Snapshot.SnapshotID || currentAfter.Evaluation.Context.EvaluateMonotonic == currentBefore.Evaluation.Context.EvaluateMonotonic {
		t.Fatalf("current SemReg shadow did not advance publication/evaluation: before_cursor=%+v after_cursor=%+v before_eval=%+v after_eval=%+v", currentBefore.Snapshot.Cursors, currentAfter.Snapshot.Cursors, currentBefore.Evaluation.Context, currentAfter.Evaluation.Context)
	}
}

func TestCanonicalPVSemRegShadow_ComparatorRejectionDoesNotAdvanceLiveState(t *testing.T) {
	listener, _ := serveSunSpecChain(t, observedFroniusFloatControlsWords())
	config := integrationConfig(t, "tcp://"+listener.Addr().String())
	config.CanonicalPVShadow.Mode = CanonicalPVShadowModeSemReg
	adapter, err := Start(context.Background(), config, realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "test:shadow-transaction", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 1, DeadlineIdentity: 1})
	if err != nil || initial.Outcome != SunSpecQualificationGO {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	asset := mustCanonicalPVAsset(t, adapter, initial)
	legacyBefore, legacyAt, ok := adapter.CanonicalPVSnapshotByAsset(asset)
	if !ok {
		t.Fatal("missing legacy before comparator rejection")
	}
	shadowBefore, ok := adapter.CanonicalPVSemRegCurrentShadow(asset)
	if !ok {
		t.Fatal("missing shadow before comparator rejection")
	}
	adapter.canonicalShadow.compare = func(pv.Snapshot, semreg.Snapshot, semreg.EvaluationView, projection.ProjectionReport) error {
		return errors.New("forced comparator divergence")
	}
	refresh, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: 2, DeadlineIdentity: 2})
	if err != nil || refresh.Outcome != SunSpecQualificationGO {
		t.Fatalf("refresh=%+v err=%v", refresh, err)
	}
	legacyAfter, legacyAfterAt, ok := adapter.CanonicalPVSnapshotByAsset(asset)
	if !ok || legacyAfter.Generation <= legacyBefore.Generation || !legacyAfterAt.After(legacyAt) {
		t.Fatal("comparator rejection failed to advance legacy current")
	}
	shadowAfter, ok := adapter.CanonicalPVSemRegCurrentShadow(asset)
	if !ok || shadowAfter.Snapshot.SnapshotID != shadowBefore.Snapshot.SnapshotID || shadowAfter.Snapshot.Cursors[0] != shadowBefore.Snapshot.Cursors[0] {
		t.Fatal("comparator rejection advanced live shadow kernel")
	}
}

func TestCanonicalPVSemRegShadow_CompactsReplayHistoryAndContinuesPastDoubleBound(t *testing.T) {
	listener, _ := serveSunSpecChain(t, observedFroniusFloatControlsWords())
	config := integrationConfig(t, "tcp://"+listener.Addr().String())
	config.CanonicalPVShadow.Mode = CanonicalPVShadowModeSemReg
	adapter, err := Start(context.Background(), config, realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "test:shadow-compaction", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 1, DeadlineIdentity: 1})
	if err != nil || initial.Outcome != SunSpecQualificationGO {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	asset := mustCanonicalPVAsset(t, adapter, initial)
	before, ok := adapter.CanonicalPVSemRegCurrentShadow(asset)
	if !ok || len(before.Snapshot.Cursors) != 1 {
		t.Fatal("missing initial cursor")
	}
	for n := 0; n < 2*maxRetainedProfileObservations+3; n++ {
		result, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: uint64(10 + n), DeadlineIdentity: uint64(100 + n)})
		if err != nil || result.Outcome != SunSpecQualificationGO {
			t.Fatalf("refresh %d=%+v err=%v", n, result, err)
		}
	}
	shadow, ok := adapter.CanonicalPVSemRegCurrentShadow(asset)
	if !ok || len(shadow.Snapshot.Facts) != 11 {
		t.Fatal("compacted shadow lost current facts")
	}
	stored := len(adapter.canonicalShadow.byAsset[asset].publications)
	if stored > maxRetainedProfileObservations {
		t.Fatalf("unbounded replay history=%d", stored)
	}
	if len(shadow.Snapshot.Cursors) != 1 || shadow.Snapshot.Cursors[0].SourceEpochID == before.Snapshot.Cursors[0].SourceEpochID || shadow.Snapshot.Cursors[0].DriverGeneration == before.Snapshot.Cursors[0].DriverGeneration || shadow.Snapshot.Cursors[0].LastSequence == "1" {
		t.Fatalf("compaction reused publication cursor: before=%+v after=%+v failure=%s", before.Snapshot.Cursors, shadow.Snapshot.Cursors, adapter.canonicalShadow.lastFailure)
	}
}

func TestCanonicalPVSemRegShadow_ComparatorRejectsEvaluationAndProjectionMutation(t *testing.T) {
	listener, _ := serveSunSpecChain(t, observedFroniusFloatControlsWords())
	config := integrationConfig(t, "tcp://"+listener.Addr().String())
	config.CanonicalPVShadow.Mode = CanonicalPVShadowModeSemReg
	adapter, err := Start(context.Background(), config, realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "test:shadow-mutation", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 1, DeadlineIdentity: 1})
	if err != nil || result.Outcome != SunSpecQualificationGO {
		t.Fatalf("qualification=%+v err=%v", result, err)
	}
	legacy, _, ok := adapter.CanonicalPVSnapshot(result.CapabilityID, result.SampleID)
	if !ok {
		t.Fatal("missing legacy")
	}
	shadow, ok := adapter.CanonicalPVSemRegShadow(result.CapabilityID, result.SampleID)
	if !ok {
		t.Fatal("missing shadow")
	}
	mutatedEvaluation := shadow.Evaluation
	mutatedEvaluation.Facts[0].Freshness = semreg.FreshnessStale
	if compareCanonicalPVSemRegShadow(legacy, shadow.Snapshot, mutatedEvaluation, shadow.Projection) == nil {
		t.Fatal("mutated evaluation was accepted")
	}
	mutatedProjection := shadow.Projection
	mutatedProjection.Dispositions[0].Loss = []projection.LossDetail{{Kind: projection.LossSymbol, SourceItems: []semreg.DefinitionID{"wrong"}, Description: "wrong"}}
	if compareCanonicalPVSemRegShadow(legacy, shadow.Snapshot, shadow.Evaluation, mutatedProjection) == nil {
		t.Fatal("mutated projection loss was accepted")
	}
	mutatedSnapshot, err := cloneSemRegSnapshot(shadow.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	mutatedSnapshot.Bindings[0].NativeResource.Digest = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if compareCanonicalPVSemRegShadow(legacy, mutatedSnapshot, shadow.Evaluation, shadow.Projection) == nil {
		t.Fatal("mutated native-resource provenance was accepted")
	}
	mutatedLegacy := cloneCanonicalPVSnapshot(legacy)
	energyKey := pv.NewFactKey(pv.FactEnergyActiveExportTotal, pv.Dimensions{Scope: pv.ScopeTotal})
	energy := mutatedLegacy.Facts[energyKey]
	energy.Unit = pv.UnitVolt
	mutatedLegacy.Facts[energyKey] = energy
	if compareCanonicalPVSemRegShadow(mutatedLegacy, shadow.Snapshot, shadow.Evaluation, shadow.Projection) == nil {
		t.Fatal("mutated legacy unit was accepted")
	}
	mutatedLegacy = cloneCanonicalPVSnapshot(legacy)
	energy = mutatedLegacy.Facts[energyKey]
	energy.Continuity.State = pv.ContinuityState("INVENTED")
	mutatedLegacy.Facts[energyKey] = energy
	if compareCanonicalPVSemRegShadow(mutatedLegacy, shadow.Snapshot, shadow.Evaluation, shadow.Projection) == nil {
		t.Fatal("mutated continuity state was accepted")
	}
	mutatedLegacy = cloneCanonicalPVSnapshot(legacy)
	mutatedLegacy.RequestedOutputs[0].RequestedOutputRef = mutatedLegacy.RequestedOutputs[1].RequestedOutputRef
	if compareCanonicalPVSemRegShadow(mutatedLegacy, shadow.Snapshot, shadow.Evaluation, shadow.Projection) == nil {
		t.Fatal("swapped legacy request association was accepted")
	}
}

func TestCanonicalPVSemRegShadow_ConcurrentReadsAndCloseAreRaceSafe(t *testing.T) {
	listener, _ := serveSunSpecChain(t, observedFroniusFloatControlsWords())
	config := integrationConfig(t, "tcp://"+listener.Addr().String())
	config.CanonicalPVShadow.Mode = CanonicalPVShadowModeSemReg
	adapter, err := Start(context.Background(), config, realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "test:shadow-race", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 1, DeadlineIdentity: 1})
	if err != nil || result.Outcome != SunSpecQualificationGO {
		t.Fatalf("qualification=%+v err=%v", result, err)
	}
	var readers sync.WaitGroup
	for range 16 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 50 {
				_, _ = adapter.CanonicalPVSemRegShadow(result.CapabilityID, result.SampleID)
				_, _, _ = adapter.CanonicalPVSnapshot(result.CapabilityID, result.SampleID)
			}
		}()
	}
	readers.Wait()
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := adapter.CanonicalPVSemRegShadow(result.CapabilityID, result.SampleID); ok {
		t.Fatal("close retained shadow state")
	}
}
