package main

import (
	"context"
	"testing"

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
			batch, _, _, _, err := growattStorageBatch(successor, native.Status, evidence, "1", tc.current, tc.exists, []semreg.SourceEpochID{})
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
