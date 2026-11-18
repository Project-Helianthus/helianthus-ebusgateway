package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semgraph"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// TestM61StorageBatchUpsertsTheSourceDescriptorOnlyForAnUnseenEpoch pins the
// batch rule behind same-epoch successor publication. The source descriptor is
// keyed by source and epoch: a batch carries it only when the current snapshot
// holds no descriptor for both, and a successor generation's new binding,
// identity link and service are carried whenever the binding is new.
func TestM61StorageBatchUpsertsTheSourceDescriptorOnlyForAnUnseenEpoch(t *testing.T) {
	config := growattProductionConfig()
	runtime := startGrowattRuntimeWithFake(t, config, &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1})
	native, err := runtime.GrowattBMSRS485V202(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	evidence, ok := runtime.LastObservationEvidence()
	if !ok {
		t.Fatal("missing native evidence")
	}
	asset, source, epoch := semreg.AssetID(config.AssetID), semreg.SourceID(config.SourceID), semreg.SourceEpochID(config.SourceEpoch)
	evidence.DriverGeneration = 2
	successor := semhub.Incarnation{Asset: asset, Source: source, Epoch: epoch, Generation: 2}
	minter, err := semhub.NewLegacyMinter(semhub.LegacyGrowattStorage)
	if err != nil {
		t.Fatal(err)
	}
	binding := minter.Binding(successor, growattStorageProfile)
	pack, err := runtime.storage.pack()
	if err != nil {
		t.Fatal(err)
	}
	descriptor := func(source semreg.SourceID, epoch semreg.SourceEpochID) semreg.SourceDescriptor {
		return semreg.SourceDescriptor{SourceID: source, SourceEpochID: epoch, State: semreg.SourceCurrent, Revision: "1"}
	}
	for _, tc := range []struct {
		name     string
		exists   bool
		current  semreg.Snapshot
		source   bool // the batch carries the source descriptor
		topology bool // the batch carries binding, identity link and service
	}{
		{name: "first publication", source: true, topology: true},
		{name: "successor of the same epoch", exists: true, current: semreg.Snapshot{Sources: []semreg.SourceDescriptor{descriptor(source, epoch)}}, topology: true},
		{name: "refresh of the same generation", exists: true, current: semreg.Snapshot{Sources: []semreg.SourceDescriptor{descriptor(source, epoch)}, Bindings: []semreg.NativeBinding{{BindingID: binding}}}},
		{name: "another epoch of the source", exists: true, current: semreg.Snapshot{Sources: []semreg.SourceDescriptor{descriptor(source, "source-epoch-0")}}, source: true, topology: true},
		{name: "the epoch of another source", exists: true, current: semreg.Snapshot{Sources: []semreg.SourceDescriptor{descriptor("growatt-bms-other", epoch)}}, source: true, topology: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.current.Revisions.Semantic = "3"
			batch, _, _, _, err := growattStorageBatch(successor, pack, native.Status, evidence, "1", tc.current, tc.exists, []semreg.SourceEpochID{})
			if err != nil {
				t.Fatal(err)
			}
			if got := len(batch.SourceUpserts) == 1; got != tc.source || len(batch.SourceUpserts) > 1 {
				t.Fatalf("source upserts=%+v want carried=%t", batch.SourceUpserts, tc.source)
			}
			if tc.source && (batch.SourceUpserts[0].SourceID != source || batch.SourceUpserts[0].SourceEpochID != epoch || batch.SourceUpserts[0].Revision != "1") {
				t.Fatalf("source upsert=%+v", batch.SourceUpserts[0])
			}
			topology := len(batch.BindingUpserts) == 1 && len(batch.IdentityLinkUpserts) == 1 && len(batch.ServiceUpserts) == 1
			if topology != tc.topology || !topology && (len(batch.BindingUpserts) != 0 || len(batch.IdentityLinkUpserts) != 0 || len(batch.ServiceUpserts) != 0) {
				t.Fatalf("topology bindings=%d links=%d services=%d want carried=%t", len(batch.BindingUpserts), len(batch.IdentityLinkUpserts), len(batch.ServiceUpserts), tc.topology)
			}
			if tc.topology && (batch.BindingUpserts[0].BindingID != binding || batch.BindingUpserts[0].DriverGeneration != "2") {
				t.Fatalf("binding upsert=%+v want %s", batch.BindingUpserts[0], binding)
			}
			if len(batch.FactUpserts) == 0 || batch.DriverGeneration != "2" || batch.SourceEpochID != epoch || batch.ExpectedSemanticRevision != tc.current.Revisions.Semantic && tc.exists {
				t.Fatalf("batch header=%s/%s/%s facts=%d", batch.SourceEpochID, batch.DriverGeneration, batch.ExpectedSemanticRevision, len(batch.FactUpserts))
			}
		})
	}
}

// growattStorageGraphKind is the one resource kind the storage gate publishes.
const growattStorageGraphKind semreg.DefinitionID = "resource.battery_pack"

// growattStorageGraphSnapshot is the snapshot of the runtime's retained
// public storage view.
func growattStorageGraphSnapshot(t *testing.T, runtime *growattBMSRS485ProductionProvider) semreg.Snapshot {
	t.Helper()
	raw, ok := runtime.storage.Current(string(runtime.storage.assetID))
	if !ok {
		t.Fatal("storage current unavailable")
	}
	var public struct {
		Snapshot semreg.Snapshot `json:"snapshot"`
	}
	if err := json.Unmarshal(raw, &public); err != nil {
		t.Fatal(err)
	}
	return public.Snapshot
}

// growattStorageGraphPack returns the one battery-pack resource of snapshot
// after checking the gate's anchor: equipment class on an operator slot from
// an operator label, with an operator origin, no scheme, key or parent, and a
// slot that is none of the asset, source or binding identifiers. The gate
// states no relation and no absence.
func growattStorageGraphPack(t *testing.T, snapshot semreg.Snapshot) semreg.Resource {
	t.Helper()
	if len(snapshot.Resources) != 1 {
		t.Fatalf("resources=%+v want one battery pack", snapshot.Resources)
	}
	pack := snapshot.Resources[0]
	if pack.Kind != growattStorageGraphKind || pack.State != semreg.ResourceCurrent || pack.ResourceID.Validate() != nil {
		t.Fatalf("pack=%+v", pack)
	}
	anchor := pack.Anchor
	if anchor.Class != semreg.AnchorEquipment || anchor.Basis != semreg.AnchorOperatorSlot || anchor.Scheme != nil || anchor.Key != nil || anchor.Parent != nil || anchor.Slot == nil || len(anchor.Evidence) == 0 {
		t.Fatalf("pack anchor=%+v", anchor)
	}
	if pack.Origin.Kind != semreg.OriginOperator || pack.Origin.BindingID != nil || pack.Origin.SourceID != nil {
		t.Fatalf("pack origin=%+v", pack.Origin)
	}
	forbidden := []string{string(snapshot.AssetID)}
	for _, source := range snapshot.Sources {
		forbidden = append(forbidden, string(source.SourceID), string(source.SourceEpochID))
	}
	for _, binding := range snapshot.Bindings {
		forbidden = append(forbidden, string(binding.BindingID))
	}
	for _, value := range forbidden {
		if strings.Contains(string(pack.ResourceID), value) || strings.Contains(*anchor.Slot, value) {
			t.Fatalf("pack %s (slot %q) carries the identifier %q", pack.ResourceID, *anchor.Slot, value)
		}
	}
	if len(snapshot.Relations) != 0 || len(snapshot.RelationAbsences) != 0 {
		t.Fatalf("the gate states no relation and no absence: %+v %+v", snapshot.Relations, snapshot.RelationAbsences)
	}
	return pack
}

// growattStorageGraphDimensions requires every fact key, every projection
// source key, every identity link and every current service of the snapshot
// to name pack, and returns the facts by identifier.
func growattStorageGraphDimensions(t *testing.T, snapshot semreg.Snapshot, pack semreg.ResourceID) map[semreg.DefinitionID]bool {
	t.Helper()
	facts := map[semreg.DefinitionID]bool{}
	for _, envelope := range snapshot.Facts {
		keys := []semreg.FactKey{envelope.Key}
		for _, candidate := range envelope.Candidates {
			keys = append(keys, candidate.Key)
		}
		for _, key := range keys {
			if len(key.Dimensions) != 1 || key.Dimensions[0].ID != "storage.dimension.pack" || key.Dimensions[0].Value.Kind != semreg.ValueText || key.Dimensions[0].Value.Text == nil || *key.Dimensions[0].Value.Text != string(pack) {
				t.Fatalf("fact %s dimensions=%+v want the pack %s", key.FactID, key.Dimensions, pack)
			}
		}
		facts[envelope.Key.FactID] = true
	}
	for _, link := range snapshot.IdentityLinks {
		if link.Resource == nil || *link.Resource != pack {
			t.Fatalf("identity link %s names %v want %s", link.BindingID, link.Resource, pack)
		}
	}
	for _, service := range snapshot.Services {
		if service.Availability != semreg.AvailabilityWithdrawn && (service.Resource == nil || *service.Resource != pack) {
			t.Fatalf("service %s names %v want %s", service.InstanceID, service.Resource, pack)
		}
	}
	return facts
}

// TestGrowattStorageGraphPublishesThePackAsAnAllocatedResource: the storage
// family is graph-enforced, its asset publishes one battery-pack resource
// anchored as the gate states, and every fact key, projection source key,
// identity link and service names that resource's allocated identifier.
func TestGrowattStorageGraphPublishesThePackAsAnAllocatedResource(t *testing.T) {
	hub := newTestSemanticHub(t)
	runtime := startGrowattRuntimeWithFakeOn(t, growattProductionConfig(), &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}, hub)
	value, err := runtime.GrowattStorageSemanticCurrent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	requireFamilyGraph(t, hub, semanticFamilyStorage, growattStorageDriver)
	if state, _ := hub.GraphState(); len(state.Assets) != 1 || state.Assets[0].Enforcement != semhub.GraphEnforced {
		t.Fatalf("storage asset graph state=%+v", state)
	}
	snapshot := growattStorageGraphSnapshot(t, runtime)
	pack := growattStorageGraphPack(t, snapshot)
	facts := growattStorageGraphDimensions(t, snapshot, pack.ResourceID)
	if len(facts) != 7 {
		t.Fatalf("facts=%v want the seven mapped facts", facts)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var public struct {
		Projection struct {
			Dispositions []struct {
				SourceKeys []semreg.FactKey `json:"source_keys"`
			} `json:"dispositions"`
		} `json:"projection"`
	}
	if err := json.Unmarshal(encoded, &public); err != nil {
		t.Fatal(err)
	}
	keys := 0
	for _, disposition := range public.Projection.Dispositions {
		for _, key := range disposition.SourceKeys {
			keys++
			if len(key.Dimensions) != 1 || key.Dimensions[0].Value.Text == nil || *key.Dimensions[0].Value.Text != string(pack.ResourceID) {
				t.Fatalf("projection source key %s=%+v want the pack %s", key.FactID, key.Dimensions, pack.ResourceID)
			}
		}
	}
	if keys != 7 {
		t.Fatalf("projection source keys=%d want 7", keys)
	}
}

// TestGrowattStorageGraphPackIdentifierFollowsOnlyTheOperatorLabel varies
// every configured field of the publisher's identity on its own hub. The
// pack's anchor key is the operator slot of the configured label, so:
//   - the asset label changes the identifier (catches a constant or
//     label-independent slot);
//   - the source identifier, the source epoch, the driver generation and the
//     routed unit number keep it (catch a slot or identifier built from the
//     source, the epoch, the binding or the address).
func TestGrowattStorageGraphPackIdentifierFollowsOnlyTheOperatorLabel(t *testing.T) {
	identify := func(config ebusgateway.GrowattBMSRS485Config) semreg.ResourceID {
		t.Helper()
		runtime := startGrowattRuntimeWithFakeOn(t, config, &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}, newTestSemanticHub(t))
		if _, err := runtime.GrowattStorageSemanticCurrent(context.Background()); err != nil {
			t.Fatal(err)
		}
		snapshot := growattStorageGraphSnapshot(t, runtime)
		pack := growattStorageGraphPack(t, snapshot)
		growattStorageGraphDimensions(t, snapshot, pack.ResourceID)
		return pack.ResourceID
	}
	base := identify(growattProductionConfig())
	if again := identify(growattProductionConfig()); again != base {
		t.Fatalf("a new process allocated %s, first %s", again, base)
	}
	for _, tc := range []struct {
		field  string
		mutate func(*ebusgateway.GrowattBMSRS485Config)
		same   bool
	}{
		{"asset label", func(c *ebusgateway.GrowattBMSRS485Config) { c.AssetID = "asset:growatt-bms-b" }, false},
		{"source identifier", func(c *ebusgateway.GrowattBMSRS485Config) { c.SourceID = "growatt-bms-b" }, true},
		{"source epoch", func(c *ebusgateway.GrowattBMSRS485Config) { c.SourceEpoch = "source-epoch-9" }, true},
		{"driver generation", func(c *ebusgateway.GrowattBMSRS485Config) { c.DriverGeneration = 9 }, true},
		{"routed unit number", func(c *ebusgateway.GrowattBMSRS485Config) { c.UnitID = 9 }, true},
	} {
		t.Run(tc.field, func(t *testing.T) {
			config := growattProductionConfig()
			tc.mutate(&config)
			if got := identify(config); (got == base) != tc.same {
				t.Fatalf("%s changed=%t: %s versus %s", tc.field, got != base, got, base)
			}
		})
	}
}

// TestGrowattStorageGraphRestartKeepsThePackIdentifier drives the managed
// driver through RESTART and STOP then START: every generation publishes the
// one pack resource under the same identifier, upserted by the first
// generation only, and its facts name it.
func TestGrowattStorageGraphRestartKeepsThePackIdentifier(t *testing.T) {
	_, open := m61Endpoints()
	driver := m04Driver(t, growattProductionConfig(), open)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer func() {
		if err := driver.Stop(ctx); err != nil {
			t.Error(err)
		}
	}()
	read := func() semreg.Resource {
		t.Helper()
		provider, ok := driver.Provider()
		if !ok {
			t.Fatal("provider unavailable")
		}
		if _, err := provider.SemanticStorageCurrent(ctx); err != nil {
			t.Fatal(err)
		}
		snapshot := growattStorageGraphSnapshot(t, provider)
		pack := growattStorageGraphPack(t, snapshot)
		if len(growattStorageGraphDimensions(t, snapshot, pack.ResourceID)) != 7 {
			t.Fatal("a generation published fewer than seven facts")
		}
		return pack
	}
	if _, err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	first := read()
	if _, err := driver.Replace(ctx); err != nil {
		t.Fatal(err)
	}
	if restarted := read(); restarted.ResourceID != first.ResourceID || restarted.Revision != first.Revision {
		t.Fatalf("restart moved the pack: %+v versus %+v", restarted, first)
	}
	if err := driver.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if started := read(); started.ResourceID != first.ResourceID || started.Revision != first.Revision {
		t.Fatalf("stop then start moved the pack: %+v versus %+v", started, first)
	}
}

// TestGrowattStorageGraphSoftStartWithdrawsOnlyThePublishersOperatingState:
// soft_starting withdraws only the operating state; the pack resource stays
// current and unrevised, and the six other facts keep naming it.
func TestGrowattStorageGraphSoftStartWithdrawsOnlyThePublishersOperatingState(t *testing.T) {
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	if _, err := runtime.GrowattStorageSemanticCurrent(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := growattStorageGraphPack(t, growattStorageGraphSnapshot(t, runtime))
	fake.mu.Lock()
	fake.words[0x000d][6] = 0
	fake.mu.Unlock()
	if _, err := runtime.GrowattStorageSemanticCurrent(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := growattStorageGraphSnapshot(t, runtime)
	after := growattStorageGraphPack(t, snapshot)
	if after.ResourceID != before.ResourceID || after.Revision != before.Revision || after.State != semreg.ResourceCurrent {
		t.Fatalf("soft start touched the pack: %+v versus %+v", after, before)
	}
	facts := growattStorageGraphDimensions(t, snapshot, after.ResourceID)
	if facts["storage.status.operating"] || len(facts) != 6 {
		t.Fatalf("facts after soft start=%v", facts)
	}
}

// TestGrowattStorageGraphSoftStartBatchWithdrawsOnlyItsOwnCandidate builds the
// soft-start batch over a snapshot where another source holds a candidate of
// every fact: it withdraws only the publisher's own operating-state
// candidate, carries no resource record because the asset holds the pack
// current, and states no relation.
func TestGrowattStorageGraphSoftStartBatchWithdrawsOnlyItsOwnCandidate(t *testing.T) {
	in := foreignSourceGrowattSoftStart(t)
	current := in.current
	foreign := foreignSourceAddContributor(t, &current)
	own := map[semreg.CandidateID]bool{}
	for _, envelope := range in.current.Facts {
		if envelope.Key.FactID == "storage.status.operating" {
			for _, candidate := range envelope.Candidates {
				own[candidate.CandidateID] = true
			}
		}
	}
	batch := in.batch(t, current)
	if len(batch.FactWithdrawals) != 1 || !own[batch.FactWithdrawals[0]] {
		t.Fatalf("withdrawals=%v want only the publisher's operating state %v", batch.FactWithdrawals, own)
	}
	for _, id := range foreign {
		if batch.FactWithdrawals[0] == id {
			t.Fatalf("withdrew the foreign candidate %s", id)
		}
	}
	if len(batch.ResourceUpserts) != 0 || len(batch.RelationUpserts) != 0 || len(batch.RelationWithdrawals) != 0 {
		t.Fatalf("soft start carried graph records: resources=%+v relations=%+v/%+v", batch.ResourceUpserts, batch.RelationUpserts, batch.RelationWithdrawals)
	}
	for _, fact := range batch.FactUpserts {
		if fact.Key.Dimensions[0].Value.Text == nil || *fact.Key.Dimensions[0].Value.Text != string(in.pack.ResourceID) {
			t.Fatalf("fact %s names %v want %s", fact.Key.FactID, fact.Key.Dimensions, in.pack.ResourceID)
		}
	}
}

// TestGrowattStorageGraphRefusesADanglingPack: a batch whose facts name a
// battery pack the asset does not publish is refused as a dangling reference
// and leaves the published view unchanged.
func TestGrowattStorageGraphRefusesADanglingPack(t *testing.T) {
	hub := newTestSemanticHub(t)
	runtime := startGrowattRuntimeWithFakeOn(t, growattProductionConfig(), &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}, hub)
	if _, err := runtime.GrowattStorageSemanticCurrent(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, err := hub.Inspect(runtime.storage.assetID)
	if err != nil {
		t.Fatal(err)
	}
	native, err := runtime.GrowattBMSRS485V202(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := runtime.LastObservationEvidence()
	other := &growattStoragePublication{assetID: "asset:growatt-bms-unpublished", resources: runtime.storage.resources}
	dangling, err := other.pack()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := hub.Begin(runtime.storage.handle, runtime.storage.assetID)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	current, exists := tx.Current()
	batch, _, _, _, err := growattStorageBatch(tx.Incarnation(), dangling, native.Status, evidence, tx.NextSequence(), current, exists, tx.SourceRetirements())
	if err != nil {
		t.Fatal(err)
	}
	batch.ResourceUpserts = nil
	if batch.BatchDigest, err = batch.ComputedDigest(); err != nil {
		t.Fatal(err)
	}
	mono := semreg.MonotonicPoint{ClockEpochID: semreg.ClockEpochID(evidence.ClockEpoch), Nanoseconds: "1"}
	if _, _, err := tx.Apply(batch, mono); semreg.ErrorIdentifier(err) != semreg.DanglingReference {
		t.Fatalf("dangling pack=%v want %s", err, semreg.DanglingReference)
	}
	tx.Abort()
	if after, err := hub.Inspect(runtime.storage.assetID); err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("a refused batch changed the asset: %v", err)
	}
}

// TestGrowattStorageGraphTwoLabelsAreTwoPacksAndNeverJoin composes two
// configured storages on one hub. Each label anchors its own pack, homed in
// its own asset, and neither asset names the other's pack; a third asset
// cannot claim a pack another asset holds.
func TestGrowattStorageGraphTwoLabelsAreTwoPacksAndNeverJoin(t *testing.T) {
	hub := newTestSemanticHub(t)
	second := growattProductionConfig()
	second.AssetID, second.SourceID, second.SourceEpoch, second.UnitID = "asset:growatt-bms-b", "growatt-bms-b", "source-epoch-b", 8
	packs := []semreg.Resource{}
	for _, config := range []ebusgateway.GrowattBMSRS485Config{growattProductionConfig(), second} {
		runtime := startGrowattRuntimeWithFakeOn(t, config, &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}, hub)
		if _, err := runtime.GrowattStorageSemanticCurrent(context.Background()); err != nil {
			t.Fatal(err)
		}
		snapshot := growattStorageGraphSnapshot(t, runtime)
		pack := growattStorageGraphPack(t, snapshot)
		growattStorageGraphDimensions(t, snapshot, pack.ResourceID)
		packs = append(packs, pack)
	}
	if packs[0].ResourceID == packs[1].ResourceID || *packs[0].Anchor.Slot == *packs[1].Anchor.Slot {
		t.Fatalf("two labels joined into one pack: %+v", packs)
	}
	resources, err := semanticResourceAllocatorFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	slot := growattStoragePackSlot("asset:growatt-bms-a")
	evidence := growattStorageDigestEvidence("operator.storage.pack_label", slot)
	anchor := semgraph.Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: slot, Evidence: []semreg.EvidenceRef{evidence}}
	if _, _, err := resources.resolve(growattStoragePackKind, anchor, "asset:growatt-bms-b"); !errors.Is(err, semgraph.ErrSecondHome) {
		t.Fatalf("a second asset claimed the first pack: %v", err)
	}
}
