package modbusadapter

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

func TestSunSpecProducerQualifiesExactObservedFroniusChainThroughRegistry(t *testing.T) {
	words := observedFroniusFloatWords()
	listener, requests := serveSunSpecChain(t, words)
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })

	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{
		UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}
	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{
		PollGeneration: 41, DeadlineIdentity: 91,
	})
	if err != nil {
		t.Fatalf("Qualify(standard chain): %v", err)
	}
	if result.Outcome != SunSpecQualificationGO || result.ObservationCount != 1 || result.SampleID == "" {
		t.Fatalf("qualification result = %#v; want one GO observation", result)
	}
	if result.CapabilityID != modbusreg.SunSpecThreePhaseMonitoringCapabilityID ||
		result.CapabilityReason != modbusreg.SunSpecCapabilityReasonAdmitted ||
		result.FlavorID != modbusreg.SunSpecFroniusObservedFlavorID ||
		result.FlavorReason != modbusreg.SunSpecFroniusFlavorReasonMatched {
		t.Fatalf("registry decisions = %#v; want exact capability and flavor match", result)
	}
	if got := sunSpecWireKeys(result.Chain.Occurrences()); !reflect.DeepEqual(got, []modbusreg.SunSpecWireKey{
		{ModelID: 1, ModelLength: 65}, {ModelID: 113, ModelLength: 60},
		{ModelID: 120, ModelLength: 26}, {ModelID: 121, ModelLength: 30},
		{ModelID: 122, ModelLength: 44}, {ModelID: 160, ModelLength: 88},
		{ModelID: 124, ModelLength: 24},
	}) {
		t.Fatalf("published SunSpec chain = %v; want exact observed Fronius chain", got)
	}
	assertBoundedFC03Discovery(t, requests(), 1)

	views := result.Chain.SourceViews()
	if len(views) == 0 {
		t.Fatal("registry-selected chain lost its source views")
	}
	view := views[0].Record()
	if view.LogicalOffset != 40000 || view.LogicalWordCount == 0 ||
		view.PollGeneration != 41 || view.TransportGeneration == 0 ||
		view.ConnectionID == 0 || view.WireResponseID == 0 {
		t.Fatalf("chain source view lost exact sample identity: %#v", view)
	}
	for index := 0; index < reflect.TypeOf(producer).NumMethod(); index++ {
		name := reflect.TypeOf(producer).Method(index).Name
		if name == "Write" || name == "Set" || name == "Control" {
			t.Fatalf("SunSpec producer exposes forbidden write operation %q", name)
		}
	}
}

func TestSunSpecProducerQualifiesExactObservedFroniusControlsChainThroughRegistry(t *testing.T) {
	words := observedFroniusFloatControlsWords()
	listener, requests := serveSunSpecChain(t, words)
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })

	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{
		UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}
	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{
		PollGeneration: 44, DeadlineIdentity: 94,
	})
	if err != nil {
		t.Fatalf("Qualify(controls chain): %v", err)
	}
	if result.Outcome != SunSpecQualificationGO || result.ObservationCount != 1 || result.SampleID == "" {
		t.Fatalf("qualification result = %#v; want one GO observation", result)
	}
	if result.CapabilityID != modbusreg.SunSpecThreePhaseMonitoringCapabilityID ||
		result.CapabilityReason != modbusreg.SunSpecCapabilityReasonAdmitted ||
		result.FlavorID != modbusreg.SunSpecFroniusObservedFlavorV11ID ||
		result.FlavorReason != modbusreg.SunSpecFroniusFlavorReasonMatched {
		t.Fatalf("registry decisions = %#v; want exact capability and V1.1 flavor match", result)
	}
	if got := sunSpecWireKeys(result.Chain.Occurrences()); !reflect.DeepEqual(got, []modbusreg.SunSpecWireKey{
		{ModelID: 1, ModelLength: 65}, {ModelID: 113, ModelLength: 60},
		{ModelID: 120, ModelLength: 26}, {ModelID: 121, ModelLength: 30},
		{ModelID: 122, ModelLength: 44}, {ModelID: 123, ModelLength: 24},
		{ModelID: 160, ModelLength: 88}, {ModelID: 124, ModelLength: 24},
	}) {
		t.Fatalf("published SunSpec chain = %v; want exact observed Fronius controls chain", got)
	}
	controls := result.Chain.ByModelID(123)
	if len(controls) != 1 || controls[0].Disposition != modbusreg.SunSpecChainDispositionAdmitted {
		t.Fatalf("Model 123 occurrences = %#v; want one admitted standard model", controls)
	}
	decoderKey, ok := controls[0].DecoderKey()
	if !ok || decoderKey != (modbusreg.SunSpecDecoderKey{
		ModelID: 123, ModelLength: 24, SchemaRevision: modbusreg.SunSpecModelsRevisionV1,
	}) {
		t.Fatalf("Model 123 decoder key = %#v, %v; want exact standard registry key", decoderKey, ok)
	}
	assertBoundedFC03Discovery(t, requests(), 1)

	// A terminal GO is not publishable until its registry-owned capture is
	// retained under the exact capability/sample identity.
	retained, encoded, ok := adapter.SunSpecQualificationObservation(result.CapabilityID, result.SampleID)
	if !ok {
		t.Fatalf("GO sample %q is unavailable from retained profile observations", result.SampleID)
	}
	if retained.SampleID() != result.SampleID || len(encoded) == 0 {
		t.Fatalf("retained sample = %q encoded=%d; want %q with deterministic evidence", retained.SampleID(), len(encoded), result.SampleID)
	}
	replay, err := retained.Replay()
	if err != nil {
		t.Fatalf("retained Replay: %v", err)
	}
	if views := replay.SourceViews(); len(views) == 0 || views[0].Record().PollGeneration != 44 || views[0].Record().DeadlineIdentity != 94 {
		t.Fatalf("retained replay lost exact ordered poll identity: %#v", views)
	}
	occurrences := replay.Occurrences()
	if len(occurrences) != 8 || occurrences[5].WireKey != (modbusreg.SunSpecWireKey{ModelID: 123, ModelLength: 24}) {
		t.Fatalf("retained replay lost Model 123 chain evidence: %#v", occurrences)
	}
}

func TestSunSpecProducerRefreshRetainsCurrentSemRegEvidenceWithBoundedEviction(t *testing.T) {
	words := observedFroniusFloatControlsWords()
	listener, _ := serveSunSpecChain(t, words)
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{
		UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}
	initial, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 71, DeadlineIdentity: 171})
	if err != nil || initial.Outcome != SunSpecQualificationGO {
		t.Fatalf("initial qualification=%+v err=%v", initial, err)
	}

	pvCoreSetFloat(words, 20, 4_321.5)
	firstRefresh, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: 72, DeadlineIdentity: 172})
	if err != nil || firstRefresh.Outcome != SunSpecQualificationGO {
		t.Fatalf("first refresh=%+v err=%v", firstRefresh, err)
	}
	assertCurrentSunSpecEvidenceRetained(t, adapter, initial, firstRefresh)

	// Refresh evidence is insertion-ordered and bounded. Every later poll keeps
	// its current digest retrievable while eviction makes the oldest refresh
	// explicitly unavailable.
	lastRefresh := firstRefresh
	for poll := uint64(73); poll <= 73+maxRetainedSunSpecRefreshEvidence; poll++ {
		pvCoreSetFloat(words, 20, float32(poll))
		lastRefresh, err = producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: poll, DeadlineIdentity: poll + 100})
		if err != nil || lastRefresh.Outcome != SunSpecQualificationGO {
			t.Fatalf("refresh %d=%+v err=%v", poll, lastRefresh, err)
		}
	}
	if _, _, ok := adapter.SunSpecQualificationObservation(firstRefresh.CapabilityID, firstRefresh.SampleID); ok {
		t.Fatal("oldest refresh evidence remained after deterministic bounded eviction")
	}
	assertCurrentSunSpecEvidenceRetained(t, adapter, initial, lastRefresh)
}

func TestSunSpecProducerRetainsReferencedAccumulatorEvidenceAcrossPartialRefreshes(t *testing.T) {
	words := observedFroniusFloatControlsWords()
	listener, _ := serveSunSpecChain(t, words)
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	now := adapter.startedMono
	adapter.wallNow = func() time.Time { return now }
	adapter.monotonicNow = func() time.Time { return now }
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 201, DeadlineIdentity: 301})
	if err != nil || initial.Outcome != SunSpecQualificationGO {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	pvCoreSetFloat(words, 30, 100)
	now = now.Add(15 * time.Second)
	energyA, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: 202, DeadlineIdentity: 302})
	if err != nil || energyA.Outcome != SunSpecQualificationGO {
		t.Fatalf("energy A=%+v err=%v", energyA, err)
	}
	pvCoreSetFloat(words, 30, -1)
	for poll := uint64(203); poll <= 236; poll++ {
		now = now.Add(15 * time.Second)
		if result, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: poll, DeadlineIdentity: poll + 100}); err != nil || result.Outcome != SunSpecQualificationGO {
			t.Fatalf("partial refresh %d=%+v err=%v", poll, result, err)
		}
	}
	assertCurrentSunSpecEvidenceRetained(t, adapter, initial, energyA)
	if observation, _, ok := adapter.SunSpecQualificationObservation(energyA.CapabilityID, energyA.SampleID); !ok {
		t.Fatal("retained accumulator evidence was evicted")
	} else if replay, err := observation.Replay(); err != nil || len(replay.SourceViews()) == 0 {
		t.Fatalf("retained accumulator evidence is not replayable: %v", err)
	}
	pvCoreSetFloat(words, 30, 101)
	now = now.Add(15 * time.Second)
	replacement, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: 237, DeadlineIdentity: 337})
	if err != nil || replacement.Outcome != SunSpecQualificationGO {
		t.Fatalf("replacement=%+v err=%v", replacement, err)
	}
	assertCurrentSunSpecEvidenceRetained(t, adapter, initial, replacement)
	if _, _, ok := adapter.SunSpecQualificationObservation(energyA.CapabilityID, energyA.SampleID); ok {
		t.Fatal("unreferenced accumulator evidence was not pruned")
	}
}

func TestSunSpecProducerRetainsEvidenceForEveryCurrentIdentity(t *testing.T) {
	words := observedFroniusFloatControlsWords()
	listener, _ := serveSunSpecChain(t, words)
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 401, DeadlineIdentity: 501})
	if err != nil || baseline.Outcome != SunSpecQualificationGO {
		t.Fatalf("baseline=%+v err=%v", baseline, err)
	}
	b := append([]uint16(nil), words...)
	putSunSpecString(b[52:68], "synthetic-a")
	copy(words, b)
	a, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: 402, DeadlineIdentity: 502})
	if err != nil || a.Outcome != SunSpecQualificationGO {
		t.Fatalf("A=%+v err=%v", a, err)
	}
	putSunSpecString(words[52:68], "synthetic-b")
	bResult, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: 403, DeadlineIdentity: 503})
	if err != nil || bResult.Outcome != SunSpecQualificationGO {
		t.Fatalf("B=%+v err=%v", bResult, err)
	}
	assertCurrentSunSpecEvidenceRetained(t, adapter, a, a)
	assertCurrentSunSpecEvidenceRetained(t, adapter, bResult, bResult)
}

func TestSunSpecProducerProspectiveRefreshEvidenceCapacity(t *testing.T) {
	words := observedFroniusFloatControlsWords()
	listener, _ := serveSunSpecChain(t, words)
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 600, DeadlineIdentity: 700})
	if err != nil || baseline.Outcome != SunSpecQualificationGO {
		t.Fatalf("baseline=%+v err=%v", baseline, err)
	}

	protected := make([]SunSpecQualificationResult, 0, maxRetainedSunSpecRefreshEvidence)
	for index := 0; index < maxRetainedSunSpecRefreshEvidence; index++ {
		putSunSpecString(words[52:68], fmt.Sprintf("protected-%02d", index))
		result, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: uint64(601 + index), DeadlineIdentity: uint64(701 + index)})
		if err != nil || result.Outcome != SunSpecQualificationGO {
			t.Fatalf("protected refresh %d=%+v err=%v", index, result, err)
		}
		protected = append(protected, result)
	}
	if len(adapter.refreshEvidence) != maxRetainedSunSpecRefreshEvidence {
		t.Fatalf("protected refresh evidence=%d; want %d", len(adapter.refreshEvidence), maxRetainedSunSpecRefreshEvidence)
	}
	if len(adapter.semanticPV.assets) != maxRetainedSunSpecRefreshEvidence+1 {
		t.Fatalf("current assets=%d; want baseline plus %d refresh identities", len(adapter.semanticPV.assets), maxRetainedSunSpecRefreshEvidence)
	}

	old := protected[0]
	oldObservation, _, ok := adapter.SunSpecQualificationObservation(old.CapabilityID, old.SampleID)
	if !ok {
		t.Fatal("protected old evidence is unavailable before replacement")
	}
	oldIdentity, _, err := resolvePVPublicationIdentity(oldObservation)
	if err != nil {
		t.Fatal(err)
	}
	oldAssetID := "pv-asset-" + pvCoreRawHash(oldIdentity)[:32]
	before, ok := adapter.SemanticPVCurrentByAsset(oldAssetID)
	if !ok {
		t.Fatal("protected old public asset is unavailable before replacement")
	}

	// The provisional 33rd store entry replaces an existing public asset. Its
	// staged snapshot releases the old digest, so the prospective global set is
	// still exactly 32 and the refresh must commit.
	putSunSpecString(words[52:68], "protected-00")
	replacement, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: 700, DeadlineIdentity: 800})
	if err != nil || replacement.Outcome != SunSpecQualificationGO {
		t.Fatalf("replacement=%+v err=%v", replacement, err)
	}
	after, ok := adapter.SemanticPVCurrentByAsset(oldAssetID)
	if !ok || after.Snapshot.SnapshotID == before.Snapshot.SnapshotID {
		t.Fatalf("existing protected asset did not advance: before=%q after=%q", before.Snapshot.SnapshotID, after.Snapshot.SnapshotID)
	}
	assertCurrentSunSpecEvidenceRetained(t, adapter, replacement, replacement)
	if _, _, ok := adapter.SunSpecQualificationObservation(old.CapabilityID, old.SampleID); ok {
		t.Fatal("superseded protected evidence remained after prospective replacement")
	}
	if len(adapter.refreshEvidence) > maxRetainedSunSpecRefreshEvidence {
		t.Fatalf("refresh evidence exceeded structural bound: %d", len(adapter.refreshEvidence))
	}

	// A new 33rd refresh identity keeps every existing refresh observation
	// referenced, so it must fail before publishing a new semantic asset.
	assetsBefore := len(adapter.semanticPV.assets)
	currentBefore, ok := adapter.SemanticPVCurrentByAsset(oldAssetID)
	if !ok {
		t.Fatal("replacement asset unavailable before overflow attempt")
	}
	putSunSpecString(words[52:68], "protected-overflow")
	overflow, err := producer.Refresh(context.Background(), SunSpecPollIdentity{PollGeneration: 701, DeadlineIdentity: 801})
	if err != nil || overflow.Outcome != SunSpecQualificationStop {
		t.Fatalf("overflow=%+v err=%v; want terminal STOP without publication", overflow, err)
	}
	currentAfter, ok := adapter.SemanticPVCurrentByAsset(oldAssetID)
	if !ok || currentAfter.Snapshot.SnapshotID != currentBefore.Snapshot.SnapshotID || currentAfter.Snapshot.Revisions != currentBefore.Snapshot.Revisions {
		t.Fatalf("overflow attempt advanced existing semantic state: before=%#v after=%#v", currentBefore.Snapshot.Revisions, currentAfter.Snapshot.Revisions)
	}
	if len(adapter.semanticPV.assets) != assetsBefore || len(adapter.refreshEvidence) != maxRetainedSunSpecRefreshEvidence {
		t.Fatalf("overflow retained semantic/evidence state: assets=%d/%d evidence=%d/%d", len(adapter.semanticPV.assets), assetsBefore, len(adapter.refreshEvidence), maxRetainedSunSpecRefreshEvidence)
	}
}

func assertCurrentSunSpecEvidenceRetained(t *testing.T, adapter *Adapter, initial, refresh SunSpecQualificationResult) {
	t.Helper()
	current, ok := adapter.SemanticPVCurrent(initial.CapabilityID, initial.SampleID)
	if !ok {
		t.Fatal("current SemReg PV view unavailable")
	}
	observation, encoded, ok := adapter.SunSpecQualificationObservation(refresh.CapabilityID, refresh.SampleID)
	if !ok || len(encoded) == 0 {
		t.Fatalf("current refresh sample %q is unavailable through MCP evidence lookup", refresh.SampleID)
	}
	replay, err := observation.Replay()
	if err != nil || len(replay.SourceViews()) == 0 {
		t.Fatalf("current refresh sample %q has no replayable source evidence: %v", refresh.SampleID, err)
	}
	wantDigest := "sha256:" + pvCoreHash("pv-observation", encoded)
	found := false
	for _, envelope := range current.Snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			for _, evidence := range candidate.Evidence {
				if string(evidence.Kind) == "sunspec.qualification_observation" && string(evidence.Digest) == wantDigest {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("current SemReg evidence digest does not resolve refresh sample %q", refresh.SampleID)
	}
	encoded[0] ^= 0xff
	_, again, ok := adapter.SunSpecQualificationObservation(refresh.CapabilityID, refresh.SampleID)
	if !ok || len(again) == 0 || again[0] == encoded[0] {
		t.Fatal("MCP evidence lookup leaked mutable retained bytes")
	}
}

func TestSunSpecProducerStopsWhenQualificationRetentionCapacityIsExhausted(t *testing.T) {
	listener, _ := serveSunSpecChain(t, observedFroniusFloatControlsWords())
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })

	// The bounded retention owner must fail closed before GO when it cannot
	// reserve one more exact qualification record.
	for index := 0; index < maxRetainedProfileObservations; index++ {
		adapter.profiles[fmt.Sprintf("occupied-%d", index)] = ProfileObservationRecord{}
	}
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{
		UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}
	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{
		PollGeneration: 45, DeadlineIdentity: 95,
	})
	if err != nil {
		t.Fatalf("Qualify: %v", err)
	}
	if result.Outcome != SunSpecQualificationStop || result.SampleID != "" || len(result.Chain.RawWords()) != 0 {
		t.Fatalf("capacity-exhausted qualification outcome=%q sample=%q raw_words=%d; want terminal STOP without partial evidence", result.Outcome, result.SampleID, len(result.Chain.RawWords()))
	}
}

func TestSunSpecQualificationRetentionRejectsUnserializableObservationWithoutStoringIt(t *testing.T) {
	listener, _ := serveSunSpecChain(t, observedFroniusFloatControlsWords())
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })

	if err := adapter.RecordSunSpecQualificationObservation(modbusreg.SunSpecQualificationObservation{}); err == nil {
		t.Fatal("unserializable qualification observation was retained")
	}
	if _, _, ok := adapter.SunSpecQualificationObservation("sunspec.inverter.three_phase.monitoring@1.0.0", "sunspec-45-95"); ok {
		t.Fatal("failed qualification serialization left a partial retained record")
	}
}

func TestSunSpecProducerReturnsNoGoForAdmittedCapabilityWithFlavorMismatch(t *testing.T) {
	listener, _ := serveSunSpecChain(t, admittedFloatChainWords())
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{
		UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}

	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 42, DeadlineIdentity: 92})
	if err != nil {
		t.Fatalf("Qualify(live float chain): %v", err)
	}
	if result.Outcome != SunSpecQualificationNoGo ||
		result.CapabilityReason != modbusreg.SunSpecCapabilityReasonAdmitted ||
		result.FlavorReason != modbusreg.SunSpecFroniusFlavorReasonChainMismatch ||
		result.ObservationCount != 0 || result.SampleID != "" {
		t.Fatalf("flavor-mismatch result = %#v; want closed NO_GO without observation", result)
	}
	assertNoSunSpecQualificationChain(t, result)
}

func TestSunSpecProducerRejectsMixedTransportGenerationWithoutPublishing(t *testing.T) {
	listener, _ := serveSunSpecChain(t, sunSpecWords(1, 65, 101, 50, 0xffff, 0))
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{
		UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}

	result, err := producer.qualifyMixedGenerationForTest(context.Background(), SunSpecPollIdentity{
		PollGeneration: 43, DeadlineIdentity: 93,
	})
	if err != nil {
		t.Fatalf("qualifyMixedGenerationForTest: %v", err)
	}
	if result.Outcome != SunSpecQualificationStop || result.ObservationCount != 0 || result.SampleID != "" {
		t.Fatalf("mixed generation result = %#v; want STOP without publication", result)
	}
	assertNoSunSpecQualificationChain(t, result)
}

func assertNoSunSpecQualificationChain(t *testing.T, result SunSpecQualificationResult) {
	t.Helper()
	if len(result.Chain.RawWords()) != 0 || len(result.Chain.Occurrences()) != 0 || len(result.Chain.SourceViews()) != 0 {
		t.Fatalf("%s result retained qualification chain evidence", result.Outcome)
	}
}

func TestSunSpecProducerSourceDelegatesSemanticSelectionToModbusreg(t *testing.T) {
	source, err := os.ReadFile("sunspec_producer.go")
	if err != nil {
		t.Fatalf("ReadFile(sunspec_producer.go): %v", err)
	}
	text := string(source)
	for _, required := range []string{
		"NewStandardSunSpecDecoderRegistry",
		"EvaluateThreePhaseMonitoring",
		"SelectFroniusObservedFlavor",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("producer does not delegate through %s", required)
		}
	}
	for _, forbidden := range []string{
		"sunspec.phase1",
		"deferredSunSpecModelInRaw",
		"EvaluateFroniusObservedFlavor(snapshot)",
		"id >= 111",
		"id >= 120",
		"id >= 200",
		"id >= 700",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("producer retains gateway-owned SunSpec classification %q", forbidden)
		}
	}
}

type sunSpecReadRequest struct {
	UnitID    byte
	Function  modbus.FunctionCode
	Offset    uint16
	WordCount uint16
}

func serveSunSpecChain(t *testing.T, words []uint16) (net.Listener, func() []sunSpecReadRequest) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var mu sync.Mutex
	var requests []sunSpecReadRequest
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		for {
			header := make([]byte, 7)
			if _, err := io.ReadFull(connection, header); err != nil {
				return
			}
			length := int(binary.BigEndian.Uint16(header[4:6]))
			body := make([]byte, length-1)
			if _, err := io.ReadFull(connection, body); err != nil || len(body) != 5 {
				return
			}
			request := sunSpecReadRequest{
				UnitID: header[6], Function: modbus.FunctionCode(body[0]),
				Offset: binary.BigEndian.Uint16(body[1:3]), WordCount: binary.BigEndian.Uint16(body[3:5]),
			}
			mu.Lock()
			requests = append(requests, request)
			mu.Unlock()
			start := int(request.Offset) - 40000
			end := start + int(request.WordCount)
			if start < 0 || end > len(words) {
				return
			}
			response := make([]byte, 9+2*int(request.WordCount))
			copy(response[:2], header[:2])
			binary.BigEndian.PutUint16(response[4:6], uint16(3+2*int(request.WordCount)))
			response[6], response[7], response[8] = request.UnitID, byte(request.Function), byte(2*request.WordCount)
			for index, word := range words[start:end] {
				binary.BigEndian.PutUint16(response[9+2*index:], word)
			}
			if _, err := connection.Write(response); err != nil {
				return
			}
		}
	}()
	return listener, func() []sunSpecReadRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]sunSpecReadRequest(nil), requests...)
	}
}

func assertBoundedFC03Discovery(t *testing.T, requests []sunSpecReadRequest, unitID byte) {
	t.Helper()
	if len(requests) == 0 {
		t.Fatal("producer did not issue discovery reads")
	}
	if requests[0].Offset != 40000 || requests[0].WordCount != 2 {
		t.Fatalf("first discovery request = %+v; want exact SunSpec signature at 40000 x 2", requests[0])
	}
	var total uint16
	nextOffset := uint16(40000)
	for _, request := range requests {
		if request.UnitID != unitID || request.Function != modbus.FunctionReadHoldingRegisters ||
			request.Offset != nextOffset || request.WordCount == 0 || request.WordCount > 125 {
			t.Fatalf("discovery request = %+v; want bounded unit-1 FC03 from PDU 40000", request)
		}
		total += request.WordCount
		nextOffset += request.WordCount
	}
	if total > 1024 {
		t.Fatalf("discovery read budget = %d; want <= 1024 words", total)
	}
}

func sunSpecWords(headers ...uint16) []uint16 {
	words := []uint16{0x5375, 0x6e53}
	for index := 0; index < len(headers); index += 2 {
		words = append(words, headers[index], headers[index+1])
		if headers[index] != 0xffff {
			words = append(words, make([]uint16, headers[index+1])...)
		}
	}
	return words
}

func observedFroniusFloatWords() []uint16 {
	return sunSpecFixtureWords(
		sunSpecFixtureModel{1, 65, commonPayload("Fronius", "Symo GEN24 10.0", "1.41.11-1")},
		sunSpecFixtureModel{113, 60, floatInverterPayload()},
		sunSpecFixtureModel{120, 26, make([]uint16, 26)},
		sunSpecFixtureModel{121, 30, make([]uint16, 30)},
		sunSpecFixtureModel{122, 44, make([]uint16, 44)},
		sunSpecFixtureModel{160, 88, mpptPayload(4)},
		sunSpecFixtureModel{124, 24, make([]uint16, 24)},
	)
}

func observedFroniusFloatControlsWords() []uint16 {
	return sunSpecFixtureWords(
		sunSpecFixtureModel{1, 65, commonPayload("Fronius", "Symo GEN24 10.0", "1.41.11-1")},
		sunSpecFixtureModel{113, 60, floatInverterPayload()},
		sunSpecFixtureModel{120, 26, make([]uint16, 26)},
		sunSpecFixtureModel{121, 30, make([]uint16, 30)},
		sunSpecFixtureModel{122, 44, make([]uint16, 44)},
		sunSpecFixtureModel{123, 24, make([]uint16, 24)},
		sunSpecFixtureModel{160, 88, mpptPayload(4)},
		sunSpecFixtureModel{124, 24, make([]uint16, 24)},
	)
}

func admittedFloatChainWords() []uint16 {
	return sunSpecFixtureWords(
		sunSpecFixtureModel{1, 65, commonPayload("Fronius", "Symo GEN24 10.0", "1.41.11-1")},
		sunSpecFixtureModel{113, 60, floatInverterPayload()},
	)
}

type sunSpecFixtureModel struct {
	id, length uint16
	payload    []uint16
}

func sunSpecFixtureWords(models ...sunSpecFixtureModel) []uint16 {
	words := []uint16{0x5375, 0x6e53}
	for _, model := range models {
		words = append(words, model.id, model.length)
		words = append(words, model.payload...)
	}
	return append(words, 0xffff, 0)
}

func commonPayload(manufacturer, model, firmware string) []uint16 {
	payload := make([]uint16, 65)
	putSunSpecString(payload[0:16], manufacturer)
	putSunSpecString(payload[16:32], model)
	putSunSpecString(payload[40:48], firmware)
	putSunSpecString(payload[48:64], "synthetic")
	return payload
}

func floatInverterPayload() []uint16 {
	payload := make([]uint16, 60)
	// Model 113's status point is word 48 including the two-word header.
	payload[46] = 4
	return payload
}

func mpptPayload(modules uint16) []uint16 {
	payload := make([]uint16, 88)
	// Model 160's N point is word 8 including the two-word header.
	payload[6] = modules
	return payload
}

func putSunSpecString(words []uint16, value string) {
	data := []byte(value)
	for index := range words {
		var high, low byte
		if 2*index < len(data) {
			high = data[2*index]
		}
		if 2*index+1 < len(data) {
			low = data[2*index+1]
		}
		words[index] = uint16(high)<<8 | uint16(low)
	}
}

func sunSpecWireKeys(occurrences []modbusreg.SunSpecOccurrence) []modbusreg.SunSpecWireKey {
	keys := make([]modbusreg.SunSpecWireKey, len(occurrences))
	for index, occurrence := range occurrences {
		keys[index] = occurrence.WireKey
	}
	return keys
}
