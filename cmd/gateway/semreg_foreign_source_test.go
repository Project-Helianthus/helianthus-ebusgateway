package main

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// The tests in this file feed the Huawei PV and Growatt storage batch builders
// a current snapshot that also holds a second source's records, as an asset
// with two contributors would. A publisher withdraws only its own records: a
// batch naming another source's candidate would be rejected whole by the
// kernel. Every identifier is synthetic.

const (
	foreignSourceID    semreg.SourceID        = "source:synthetic-foreign"
	foreignSourceEpoch semreg.SourceEpochID   = "source-epoch:synthetic-foreign"
	foreignBindingID   semreg.NativeBindingID = "binding:synthetic-foreign"
	// foreignProbeEpoch names an epoch whose descriptor the snapshot holds
	// only when a case adds one, so a probe can resolve through its binding
	// alone, through its epoch alone, or through neither.
	foreignProbeEpoch semreg.SourceEpochID = "source-epoch:synthetic-probe"
)

func foreignSourceDescriptor(source semreg.SourceID, epoch semreg.SourceEpochID, startedNS int64) semreg.SourceDescriptor {
	return semreg.SourceDescriptor{SourceID: source, SourceEpochID: epoch, StartedAt: semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(startedNS, 10)), ClockID: "clock.utc", UncertaintyNS: "0"}, State: semreg.SourceCurrent, Revision: "1"}
}

func foreignSourceBinding(asset semreg.AssetID, id semreg.NativeBindingID, source semreg.SourceID, epoch semreg.SourceEpochID, generation semreg.Uint64) semreg.NativeBinding {
	return semreg.NativeBinding{BindingID: id, AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, State: semreg.BindingCurrent, Revision: "1"}
}

// foreignSourceCandidate returns a copy of template moved to another
// identifier, binding and source epoch. A nil binding or epoch leaves that
// reference unset.
func foreignSourceCandidate(template semreg.FactCandidate, id semreg.CandidateID, binding *semreg.NativeBindingID, epoch *semreg.SourceEpochID, generation semreg.Uint64) semreg.FactCandidate {
	candidate := template
	candidate.CandidateID = id
	candidate.BindingID = binding
	candidate.SourceEpochID = epoch
	candidate.DriverGeneration = &generation
	candidate.Origin.BindingID = binding
	candidate.Origin.SourceEpochID = epoch
	candidate.Origin.SourceID = nil
	return candidate
}

func foreignSourceRetained(id semreg.CandidateID, binding *semreg.NativeBindingID, epoch semreg.SourceEpochID, generation semreg.Uint64) semreg.RetainedObservationRecord {
	return semreg.RetainedObservationRecord{Contract: semreg.ContractKernelV1, Candidate: semreg.FactCandidate{CandidateID: id, BindingID: binding, SourceEpochID: &epoch, DriverGeneration: &generation}, Removal: semreg.RetainedRemovalGenerationFence}
}

func foreignSourceBindingRef(id semreg.NativeBindingID) *semreg.NativeBindingID { return &id }
func foreignSourceEpochRef(id semreg.SourceEpochID) *semreg.SourceEpochID       { return &id }

// foreignSourceProbe is one way a candidate's source can resolve: through its
// binding, through its source epoch, through both or through neither. Each
// case varies one of the two references the publisher resolves.
type foreignSourceProbe struct {
	name string
	// ownBinding and foreignBinding place the candidate on a binding of the
	// publisher's source or of the foreign source; with neither, the binding
	// reference is unset, and with missingBinding it names an absent binding.
	ownBinding, foreignBinding, missingBinding bool
	// ownEpoch and foreignEpoch add a descriptor of the probe epoch for the
	// publisher's source or for the foreign source; with neither, the probe
	// epoch has no descriptor.
	ownEpoch, foreignEpoch bool
	// own is whether the publisher may withdraw the candidate.
	own bool
}

var foreignSourceProbes = []foreignSourceProbe{
	{name: "own source through the binding alone", ownBinding: true, own: true},
	{name: "own source through the epoch alone", ownEpoch: true, own: true},
	{name: "foreign source through the binding alone", foreignBinding: true},
	{name: "foreign source through the epoch alone", foreignEpoch: true},
	{name: "own binding with a foreign epoch", ownBinding: true, foreignEpoch: true},
	{name: "foreign binding with an own epoch", foreignBinding: true, ownEpoch: true},
	{name: "source not resolvable", missingBinding: true},
}

// apply adds the probe's descriptor and binding to snapshot and returns the
// probe's binding reference.
func (probe foreignSourceProbe) apply(snapshot *semreg.Snapshot, asset semreg.AssetID, own semreg.SourceID, ownBinding semreg.NativeBindingID) *semreg.NativeBindingID {
	switch {
	case probe.ownEpoch:
		snapshot.Sources = append(snapshot.Sources, foreignSourceDescriptor(own, foreignProbeEpoch, 500))
	case probe.foreignEpoch:
		snapshot.Sources = append(snapshot.Sources, foreignSourceDescriptor(foreignSourceID, foreignProbeEpoch, 500))
	}
	switch {
	case probe.ownBinding:
		return foreignSourceBindingRef(ownBinding)
	case probe.foreignBinding:
		snapshot.Bindings = append(snapshot.Bindings, foreignSourceBinding(asset, "binding:synthetic-probe-foreign", foreignSourceID, foreignSourceEpoch, "1"))
		return foreignSourceBindingRef("binding:synthetic-probe-foreign")
	case probe.missingBinding:
		return foreignSourceBindingRef("binding:synthetic-probe-absent")
	}
	return nil
}

func foreignSourceSortedIDs(ids []semreg.CandidateID) []semreg.CandidateID {
	out := append([]semreg.CandidateID{}, ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func foreignSourceHuaweiPublication(t *testing.T) *huaweiPVPublication {
	t.Helper()
	publication, err := newHuaweiPVPublication(newTestSemanticHub(t), "foreign-source-test", "source:synthetic", "alpha", nil, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	publication.predecessor = "source-epoch:synthetic-own-previous"
	return publication
}

// foreignSourceHuaweiSnapshot is the publisher's own single-source asset: the
// publishing generation, the predecessor epoch with three retained
// observations and an older epoch with two, each with its descriptor and
// binding, as the third START of TestH10ThirdGenerationWithdrawsOldestRetainedForCapacity
// finds it.
func foreignSourceHuaweiSnapshot(p *huaweiPVPublication, asset semreg.AssetID, withOldest bool) semreg.Snapshot {
	own, current, generation := p.handle.Source(), p.handle.Epoch(), p.handle.GenerationID()
	previous, oldest := p.predecessor, semreg.SourceEpochID("source-epoch:synthetic-own-oldest")
	snapshot := semreg.Snapshot{AssetID: asset, Revisions: semreg.RevisionVector{Semantic: "7"},
		Sources:  []semreg.SourceDescriptor{foreignSourceDescriptor(own, current, 3_000), foreignSourceDescriptor(own, previous, 2_000)},
		Bindings: []semreg.NativeBinding{foreignSourceBinding(asset, "binding:synthetic-own-current", own, current, generation), foreignSourceBinding(asset, "binding:synthetic-own-previous", own, previous, "1")},
	}
	snapshot.Facts = []semreg.FactEnvelope{{Candidates: []semreg.FactCandidate{{CandidateID: "candidate:synthetic-own-current", BindingID: foreignSourceBindingRef("binding:synthetic-own-current"), SourceEpochID: &current, DriverGeneration: &generation}}}}
	for i := 1; i <= 3; i++ {
		snapshot.Retained = append(snapshot.Retained, foreignSourceRetained(semreg.CandidateID("candidate:synthetic-own-previous-"+strconv.Itoa(i)), foreignSourceBindingRef("binding:synthetic-own-previous"), previous, "1"))
	}
	if withOldest {
		snapshot.Sources = append(snapshot.Sources, foreignSourceDescriptor(own, oldest, 1_000))
		snapshot.Bindings = append(snapshot.Bindings, foreignSourceBinding(asset, "binding:synthetic-own-oldest", own, oldest, "1"))
		for i := 1; i <= 2; i++ {
			snapshot.Retained = append(snapshot.Retained, foreignSourceRetained(semreg.CandidateID("candidate:synthetic-own-oldest-"+strconv.Itoa(i)), foreignSourceBindingRef("binding:synthetic-own-oldest"), oldest, "1"))
		}
	}
	return snapshot
}

// foreignSourceAddHuaweiContributor adds a second source's descriptor (whose
// epoch started after every own epoch), binding, current fact candidate and
// retained observations in one retained group per listed generation.
func foreignSourceAddHuaweiContributor(snapshot *semreg.Snapshot, asset semreg.AssetID, generations ...semreg.Uint64) {
	snapshot.Sources = append(snapshot.Sources, foreignSourceDescriptor(foreignSourceID, foreignSourceEpoch, 9_000))
	for _, generation := range generations {
		binding := foreignBindingID + semreg.NativeBindingID("-"+string(generation))
		snapshot.Bindings = append(snapshot.Bindings, foreignSourceBinding(asset, binding, foreignSourceID, foreignSourceEpoch, generation))
		snapshot.IdentityLinks = append(snapshot.IdentityLinks, semreg.IdentityLink{AssetID: asset, BindingID: binding, State: semreg.LinkQualified, Revision: "1"})
		for i := 1; i <= 2; i++ {
			snapshot.Retained = append(snapshot.Retained, foreignSourceRetained(semreg.CandidateID("candidate:synthetic-foreign-"+string(generation)+"-"+strconv.Itoa(i)), foreignSourceBindingRef(binding), foreignSourceEpoch, generation))
		}
		snapshot.Facts[0].Candidates = append(snapshot.Facts[0].Candidates, semreg.FactCandidate{CandidateID: semreg.CandidateID("candidate:synthetic-foreign-current-" + string(generation)), BindingID: foreignSourceBindingRef(binding), SourceEpochID: foreignSourceEpochRef(foreignSourceEpoch), DriverGeneration: &generation})
	}
}

// TestForeignSourceHuaweiCapacityWithdrawsOnlyOwnGenerations covers the
// retained-generation withdrawal. The fields a case varies, and the
// production mutation each one catches:
//   - the source of a retained group (own or foreign, one or two foreign
//     groups): grouping every retained candidate whatever its source;
//   - the foreign group's driver generation (two generations of one foreign
//     epoch): filtering one foreign group but not every foreign generation;
//   - the candidate's binding reference (own, foreign, absent, unset):
//     resolving the source through the epoch alone;
//   - the candidate's source-epoch descriptor (own, foreign, absent):
//     resolving the source through the binding alone;
//   - a binding and an epoch that name different sources: accepting either
//     resolution instead of requiring every resolvable one to be own;
//   - neither resolvable: treating an unresolvable candidate as own.
func TestForeignSourceHuaweiCapacityWithdrawsOnlyOwnGenerations(t *testing.T) {
	p := foreignSourceHuaweiPublication(t)
	asset := huaweiAssetID("alpha", "pv", 1)
	epoch, generation := p.handle.Epoch(), p.handle.GenerationID()
	oldest := semreg.SourceEpochID("source-epoch:synthetic-own-oldest")
	singleIDs := []semreg.CandidateID{"candidate:synthetic-own-oldest-1", "candidate:synthetic-own-oldest-2"}
	singleRecords := []huaweiRetainedWithdrawal{{AssetID: asset, SourceEpochID: oldest, Generation: "1", Count: 2, Reason: "capacity"}}

	check := func(t *testing.T, snapshot semreg.Snapshot, wantIDs []semreg.CandidateID, wantRecords []huaweiRetainedWithdrawal) {
		t.Helper()
		ids, records, err := p.capacityWithdrawals(asset, snapshot, epoch, generation)
		if err != nil {
			t.Fatalf("capacity withdrawals refused: %v", err)
		}
		if got, want := foreignSourceSortedIDs(ids), foreignSourceSortedIDs(wantIDs); !reflect.DeepEqual(got, want) {
			t.Fatalf("withdrawn candidates=%v want %v", got, want)
		}
		if len(records) != len(wantRecords) || len(records) != 0 && !reflect.DeepEqual(records, wantRecords) {
			t.Fatalf("withdrawal records=%+v want %+v", records, wantRecords)
		}
		for _, record := range records {
			if record.SourceEpochID == foreignSourceEpoch {
				t.Fatalf("foreign generation recorded as withdrawn: %+v", record)
			}
		}
	}
	previousIDs := []semreg.CandidateID{"candidate:synthetic-own-previous-1", "candidate:synthetic-own-previous-2", "candidate:synthetic-own-previous-3"}
	previousRecords := []huaweiRetainedWithdrawal{{AssetID: asset, SourceEpochID: p.predecessor, Generation: "1", Count: 3, Reason: "capacity"}}

	t.Run("single source", func(t *testing.T) {
		check(t, foreignSourceHuaweiSnapshot(p, asset, true), singleIDs, singleRecords)
	})
	// A foreign generation occupies the per-asset window: the own predecessor
	// is the only group that can make room, and it alone is withdrawn.
	t.Run("one foreign group beside the predecessor", func(t *testing.T) {
		snapshot := foreignSourceHuaweiSnapshot(p, asset, false)
		foreignSourceAddHuaweiContributor(&snapshot, asset, "1")
		check(t, snapshot, previousIDs, previousRecords)
	})
	// Two foreign groups and two own groups leave three to free, and only two
	// are the publisher's own: the admission is refused, nothing is withdrawn.
	t.Run("two foreign groups beside three own generations refuse", func(t *testing.T) {
		snapshot := foreignSourceHuaweiSnapshot(p, asset, true)
		foreignSourceAddHuaweiContributor(&snapshot, asset, "1", "2")
		ids, records, err := p.capacityWithdrawals(asset, snapshot, epoch, generation)
		if !errors.Is(err, errHuaweiRetainedCapacityExhausted) || len(ids) != 0 || len(records) != 0 {
			t.Fatalf("ids=%v records=%+v err=%v want refusal", ids, records, err)
		}
	})
	t.Run("one foreign group beside two own generations", func(t *testing.T) {
		snapshot := foreignSourceHuaweiSnapshot(p, asset, true)
		foreignSourceAddHuaweiContributor(&snapshot, asset, "1")
		ids, records, err := p.capacityWithdrawals(asset, snapshot, epoch, generation)
		if err != nil {
			t.Fatal(err)
		}
		// Window of two: current plus one; three own and foreign groups
		// leave two to free, both own.
		want := append(append([]semreg.CandidateID{}, singleIDs...), previousIDs...)
		if got := foreignSourceSortedIDs(ids); !reflect.DeepEqual(got, foreignSourceSortedIDs(want)) || len(records) != 2 {
			t.Fatalf("ids=%v records=%+v", got, records)
		}
		for _, record := range records {
			if record.SourceEpochID == foreignSourceEpoch {
				t.Fatalf("foreign generation recorded as withdrawn: %+v", record)
			}
		}
	})
	for _, probe := range foreignSourceProbes {
		t.Run(probe.name, func(t *testing.T) {
			snapshot := foreignSourceHuaweiSnapshot(p, asset, false)
			binding := probe.apply(&snapshot, asset, p.handle.Source(), "binding:synthetic-own-previous")
			snapshot.Retained = append(snapshot.Retained, foreignSourceRetained("candidate:synthetic-probe", binding, foreignProbeEpoch, "1"))
			wantIDs, wantRecords := previousIDs, previousRecords
			if probe.own {
				wantIDs = []semreg.CandidateID{"candidate:synthetic-probe"}
				wantRecords = []huaweiRetainedWithdrawal{{AssetID: asset, SourceEpochID: foreignProbeEpoch, Generation: "1", Count: 1, Reason: "capacity"}}
			}
			check(t, snapshot, wantIDs, wantRecords)
		})
	}
}

// foreignSourceGrowattInput observes the configured storage asset once in the
// active state, then reads (without publishing) a soft-starting observation
// and returns the inputs the next publication would pass to
// growattStorageBatch.
type foreignSourceGrowattInput struct {
	inc         semhub.Incarnation
	status      modbusreg.GrowattBMSTypedReadOnlyStatus
	evidence    GrowattBMSRS485ObservationEvidence
	sequence    semreg.Uint64
	current     semreg.Snapshot
	exists      bool
	retirements []semreg.SourceEpochID
}

func foreignSourceGrowattSoftStart(t *testing.T) foreignSourceGrowattInput {
	t.Helper()
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 4}
	fake.words[0x000d][6] = 2
	runtime := startGrowattRuntimeWithFake(t, growattProductionConfig(), fake)
	if _, err := runtime.GrowattStorageSemanticCurrent(context.Background()); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.words[0x000d][6] = 0
	fake.mu.Unlock()
	native, err := runtime.GrowattBMSRS485V202(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if native.Status.OperatingState != modbusreg.GrowattBMSStateSoftStarting {
		t.Fatalf("fixture operating state=%v, want soft starting", native.Status.OperatingState)
	}
	evidence, ok := runtime.LastObservationEvidence()
	if !ok {
		t.Fatal("missing native evidence")
	}
	tx, err := runtime.storage.hub.Begin(runtime.storage.handle, runtime.storage.assetID)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	current, exists := tx.Current()
	if !exists {
		t.Fatal("the active publication was not committed")
	}
	return foreignSourceGrowattInput{inc: tx.Incarnation(), status: native.Status, evidence: evidence, sequence: tx.NextSequence(), current: current, exists: exists, retirements: tx.SourceRetirements()}
}

func (in foreignSourceGrowattInput) batch(t *testing.T, current semreg.Snapshot) semreg.PublicationBatch {
	t.Helper()
	batch, _, _, _, err := growattStorageBatch(in.inc, in.status, in.evidence, in.sequence, current, in.exists, in.retirements)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

// foreignSourceAddContributor adds a second source's descriptor, binding,
// identity link, one current candidate per fact envelope and one retained
// observation, each copied from the publisher's own record under foreign
// identifiers. It returns every foreign candidate identifier.
func foreignSourceAddContributor(t *testing.T, snapshot *semreg.Snapshot) []semreg.CandidateID {
	t.Helper()
	snapshot.Sources = append(append([]semreg.SourceDescriptor{}, snapshot.Sources...), foreignSourceDescriptor(foreignSourceID, foreignSourceEpoch, 9_000))
	snapshot.Bindings = append(append([]semreg.NativeBinding{}, snapshot.Bindings...), foreignSourceBinding(snapshot.AssetID, foreignBindingID, foreignSourceID, foreignSourceEpoch, "1"))
	snapshot.IdentityLinks = append(append([]semreg.IdentityLink{}, snapshot.IdentityLinks...), semreg.IdentityLink{AssetID: snapshot.AssetID, BindingID: foreignBindingID, State: semreg.LinkQualified, Revision: "1"})
	facts := make([]semreg.FactEnvelope, 0, len(snapshot.Facts))
	var ids []semreg.CandidateID
	for i, envelope := range snapshot.Facts {
		if len(envelope.Candidates) == 0 {
			t.Fatal("fixture envelope without a candidate")
		}
		id := semreg.CandidateID("candidate:synthetic-foreign-" + strconv.Itoa(i))
		envelope.Candidates = append(append([]semreg.FactCandidate{}, envelope.Candidates...), foreignSourceCandidate(envelope.Candidates[0], id, foreignSourceBindingRef(foreignBindingID), foreignSourceEpochRef(foreignSourceEpoch), "1"))
		facts = append(facts, envelope)
		ids = append(ids, id)
	}
	snapshot.Facts = facts
	snapshot.Retained = append(append([]semreg.RetainedObservationRecord{}, snapshot.Retained...), foreignSourceRetained("candidate:synthetic-foreign-retained", foreignSourceBindingRef(foreignBindingID), foreignSourceEpoch, "1"))
	return append(ids, "candidate:synthetic-foreign-retained")
}

func foreignSourceOperating(snapshot semreg.Snapshot) (int, []semreg.CandidateID) {
	for i, envelope := range snapshot.Facts {
		if envelope.Key.FactID == "storage.status.operating" {
			var ids []semreg.CandidateID
			for _, candidate := range envelope.Candidates {
				ids = append(ids, candidate.CandidateID)
			}
			return i, ids
		}
	}
	return -1, nil
}

// TestForeignSourceGrowattSoftStartWithdrawsOnlyOwnOperatingCandidates covers
// the soft-start withdrawal of storage.status.operating. The fields a case
// varies, and the production mutation each one catches:
//   - a foreign current candidate of storage.status.operating and of every
//     other fact, and a foreign retained observation: withdrawing every
//     candidate of the fact whatever its source;
//   - the candidate's binding reference (own, foreign, absent, unset):
//     resolving the source through the epoch alone;
//   - the candidate's source-epoch descriptor (own, foreign, absent):
//     resolving the source through the binding alone;
//   - a binding and an epoch that name different sources: accepting either
//     resolution instead of requiring every resolvable one to be own;
//   - neither resolvable: treating an unresolvable candidate as own.
func TestForeignSourceGrowattSoftStartWithdrawsOnlyOwnOperatingCandidates(t *testing.T) {
	in := foreignSourceGrowattSoftStart(t)
	index, own := foreignSourceOperating(in.current)
	if index < 0 || len(own) == 0 {
		t.Fatal("the active publication holds no operating candidate")
	}
	single := in.batch(t, in.current)
	if got, want := foreignSourceSortedIDs(single.FactWithdrawals), foreignSourceSortedIDs(own); !reflect.DeepEqual(got, want) {
		t.Fatalf("single-source soft start withdrew %v, want the own operating candidates %v", got, want)
	}

	t.Run("second contributor", func(t *testing.T) {
		current := in.current
		foreign := foreignSourceAddContributor(t, &current)
		batch := in.batch(t, current)
		for _, id := range batch.FactWithdrawals {
			for _, foreignID := range foreign {
				if id == foreignID {
					t.Fatalf("soft start withdrew the foreign candidate %s", id)
				}
			}
		}
		if !reflect.DeepEqual(batch, single) {
			t.Fatalf("a second contributor changed the batch: withdrawals=%v want %v", batch.FactWithdrawals, single.FactWithdrawals)
		}
	})
	ownBinding := *in.current.Facts[index].Candidates[0].BindingID
	for _, probe := range foreignSourceProbes {
		t.Run(probe.name, func(t *testing.T) {
			current := in.current
			foreignSourceAddContributor(t, &current)
			binding := probe.apply(&current, current.AssetID, in.inc.Source, ownBinding)
			current.Facts[index].Candidates = append(current.Facts[index].Candidates, foreignSourceCandidate(in.current.Facts[index].Candidates[0], "candidate:synthetic-probe", binding, foreignSourceEpochRef(foreignProbeEpoch), "1"))
			batch := in.batch(t, current)
			want := append([]semreg.CandidateID{}, own...)
			if probe.own {
				want = append(want, "candidate:synthetic-probe")
			}
			if got := foreignSourceSortedIDs(batch.FactWithdrawals); !reflect.DeepEqual(got, foreignSourceSortedIDs(want)) {
				t.Fatalf("soft start withdrew %v, want %v", got, foreignSourceSortedIDs(want))
			}
		})
	}
}

// TestForeignSourceHuaweiCapacityCountsEveryContributor pins that the Huawei
// capacity checks keep counting every record of an asset: the kernel's bounds
// of 128 bindings, 128 identity links and 32 source descriptors are per asset
// and shared by every contributor, so a foreign source's records fill them as
// the publisher's own do. The fields a case varies, and the production
// mutation each one catches:
//   - the source of the bindings filling the bound: counting only own bindings;
//   - the source of the identity links filling the bound: counting only own
//     identity links;
//   - the source of the descriptors filling the bound: counting only own
//     descriptors;
//   - one record fewer than the bound: refusing below the bound;
//   - an asset that holds no descriptor of the source: counting an asset the
//     source does not publish on.
func TestForeignSourceHuaweiCapacityCountsEveryContributor(t *testing.T) {
	const own semreg.SourceID = "source:synthetic"
	asset := huaweiAssetID("alpha", "pv", 1)
	topology := func(bindings, links int) semreg.Snapshot {
		snapshot := semreg.Snapshot{AssetID: asset,
			Bindings:      []semreg.NativeBinding{foreignSourceBinding(asset, "binding:synthetic-own", own, "source-epoch:synthetic-own", "1")},
			IdentityLinks: []semreg.IdentityLink{{AssetID: asset, BindingID: "binding:synthetic-own", State: semreg.LinkQualified, Revision: "1"}},
		}
		for i := 0; i < bindings; i++ {
			snapshot.Bindings = append(snapshot.Bindings, foreignSourceBinding(asset, semreg.NativeBindingID("binding:synthetic-foreign-"+strconv.Itoa(i)), foreignSourceID, foreignSourceEpoch, "1"))
		}
		for i := 0; i < links; i++ {
			snapshot.IdentityLinks = append(snapshot.IdentityLinks, semreg.IdentityLink{AssetID: asset, BindingID: semreg.NativeBindingID("binding:synthetic-foreign-" + strconv.Itoa(i)), State: semreg.LinkQualified, Revision: "1"})
		}
		return snapshot
	}
	for _, tc := range []struct {
		name           string
		bindings, link int
		exhausted      bool
	}{
		{name: "foreign bindings fill the binding bound", bindings: huaweiMaxAssetBindings - 1, exhausted: true},
		{name: "foreign identity links fill the link bound", bindings: 1, link: huaweiMaxAssetIdentityLinks - 1, exhausted: true},
		{name: "one binding and one link below the bounds", bindings: huaweiMaxAssetBindings - 2, link: huaweiMaxAssetIdentityLinks - 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := huaweiBindingCapacity(topology(tc.bindings, tc.link))
			if tc.exhausted != errors.Is(err, errHuaweiBindingCapacityExhausted) || !tc.exhausted && err != nil {
				t.Fatalf("binding capacity=%v want exhausted=%t", err, tc.exhausted)
			}
		})
	}
	sources := func(ownDescriptor bool, foreign int) semreg.Snapshot {
		snapshot := semreg.Snapshot{AssetID: asset}
		if ownDescriptor {
			snapshot.Sources = append(snapshot.Sources, foreignSourceDescriptor(own, "source-epoch:synthetic-own", 1_000))
		}
		for i := 0; i < foreign; i++ {
			snapshot.Sources = append(snapshot.Sources, foreignSourceDescriptor(foreignSourceID, semreg.SourceEpochID("source-epoch:synthetic-foreign-"+strconv.Itoa(i)), 2_000))
		}
		return snapshot
	}
	for _, tc := range []struct {
		name      string
		own       bool
		foreign   int
		exhausted bool
	}{
		{name: "foreign descriptors fill the source bound", own: true, foreign: huaweiMaxAssetSources - 1, exhausted: true},
		{name: "one descriptor below the source bound", own: true, foreign: huaweiMaxAssetSources - 2},
		{name: "an asset the source does not publish on", foreign: huaweiMaxAssetSources},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := huaweiAssetSourceCapacity(sources(tc.own, tc.foreign), own)
			if tc.exhausted != errors.Is(err, errHuaweiSourceCapacityExhausted) || !tc.exhausted && err != nil {
				t.Fatalf("source capacity=%v want exhausted=%t", err, tc.exhausted)
			}
		})
	}
}

// A retained-capacity refusal is named in the instance status, like the
// binding-capacity refusal: the child has no asset and carries the reason.
func TestForeignSourceHuaweiRetainedCapacityRefusalIsRecorded(t *testing.T) {
	p := foreignSourceHuaweiPublication(t)
	asset := semreg.AssetID("asset:synthetic-refusal")
	epoch, generation := p.handle.Epoch(), p.handle.GenerationID()
	snapshot := foreignSourceHuaweiSnapshot(p, asset, true)
	foreignSourceAddHuaweiContributor(&snapshot, asset, "1", "2")
	const unit = byte(7)
	if _, _, err := p.admitRetained(unit, asset, snapshot, epoch, generation); !errors.Is(err, errHuaweiRetainedCapacityExhausted) {
		t.Fatalf("admitRetained error = %v, want retained capacity exhaustion", err)
	}
	if got := p.refused[unit]; got != huaweiRefusalRetainedCapacity {
		t.Fatalf("refusal = %q, want %q", got, huaweiRefusalRetainedCapacity)
	}
	single := foreignSourceHuaweiSnapshot(p, asset, true)
	if _, _, err := p.admitRetained(8, asset, single, epoch, generation); err != nil {
		t.Fatalf("single-source admission: %v", err)
	}
	if _, ok := p.refused[8]; ok {
		t.Fatal("a successful admission recorded a refusal")
	}
}
