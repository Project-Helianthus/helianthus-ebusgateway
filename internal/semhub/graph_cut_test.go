package semhub

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
)

// The cut fixture spreads one PV installation over two graph-enforced
// assets: the system asset holds the PV system resource and the relation
// that groups the inverter under it; the inverter asset holds the inverter
// resource the relation's far end names.
const (
	cutSystemAsset   semreg.AssetID    = "asset:pv-system"
	cutInverterAsset semreg.AssetID    = "asset:pv-inverter"
	cutSystem        semreg.ResourceID = "resource:semhubtest:system-1"
)

func cutSystemResource() semreg.Resource {
	slot := "system-1"
	return semreg.Resource{ResourceID: cutSystem, Kind: "resource.pv_system", Anchor: semreg.ResourceAnchor{Class: semreg.AnchorFunction, Basis: semreg.AnchorOperatorSlot, Slot: &slot, Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.anchor")}}, State: semreg.ResourceCurrent, Origin: graphTestOrigin(), Revision: "1"}
}

func cutSystemInverterRelation() semreg.RelationInstance {
	return semreg.RelationInstance{Definition: semreg.DefinitionRef{Pack: graphPVPack, ID: "pv.relation.system_inverter", Version: "1.0.0"}, From: cutSystem, To: graphInverter, State: semreg.LinkQualified, BasisClass: semreg.RelationConfiguration, Basis: []semreg.EvidenceRef{testEvidence("semhubtest.relation")}, Origin: graphTestOrigin(), Revision: "1"}
}

// cutBatch stages the next batch of a fixture asset. The system asset's
// first batch upserts the system resource and the cross-asset relation, and
// every batch refreshes one export limit fact about the system. An inverter
// asset's first batch upserts the inverter resource, and every batch
// refreshes its frequency.
func cutBatch(t *testing.T, tx *Tx, at int64) semreg.PublicationBatch {
	t.Helper()
	_, exists := tx.Current()
	if tx.Incarnation().Asset != cutSystemAsset {
		var resources []semreg.Resource
		if !exists {
			resources = []semreg.Resource{graphInverterResource()}
		}
		return graphPVBatch(t, tx, at, true, graphInverter, resources...)
	}
	batch := graphPVBatch(t, tx, at, true, graphInverter)
	subject := string(cutSystem)
	limit := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: testDecimal(1000 + at), Unit: "unit.watt"}}
	batch.FactUpserts[0].Key = semreg.FactKey{PackID: graphPVPack.ID, PackVersion: graphPVPack.Version, FactID: "pv.limit.export_power", Dimensions: []semreg.Dimension{{ID: "pv.dimension.system", Value: semreg.Value{Kind: semreg.ValueText, Text: &subject}}}}
	batch.FactUpserts[0].Value = &limit
	if len(batch.IdentityLinkUpserts) == 1 {
		resource := cutSystem
		batch.IdentityLinkUpserts[0].Resource = &resource
	}
	if !exists {
		batch.ResourceUpserts = []semreg.Resource{cutSystemResource()}
		batch.RelationUpserts = []semreg.RelationInstance{cutSystemInverterRelation()}
	}
	sealGraphTestBatch(t, &batch)
	return batch
}

// publishCut runs one complete transaction of a fixture asset.
func publishCut(t *testing.T, hub *Hub, handle *Handle, id semreg.AssetID, at int64) error {
	t.Helper()
	tx, err := hub.Begin(handle, id)
	if err != nil {
		return err
	}
	defer tx.Abort()
	if _, _, err := tx.Apply(cutBatch(t, tx, at), testMono(at)); err != nil {
		return err
	}
	outcome, err := tx.Commit(nil)
	if err == nil && outcome != Accepted {
		return errors.New("batch not accepted: " + string(outcome))
	}
	return err
}

// cutPair declares the system and inverter assets under enforcement, admits
// both sources and publishes one batch on each.
func cutPair(t *testing.T) (*Hub, *Handle, *Handle) {
	t.Helper()
	hub := newTestHub(t)
	for _, spec := range []AssetSpec{
		graphPVSpec(cutSystemAsset, "source:pv-system", "driver-pv-system", true),
		graphPVSpec(cutInverterAsset, "source:pv-inverter", "driver-pv-inverter", true),
	} {
		if _, err := hub.Declare(spec); err != nil {
			t.Fatal(err)
		}
	}
	system := admitLegacy(t, hub, "source:pv-system", "driver-pv-system", "epoch:pv-system", 1)
	inverter := admitLegacy(t, hub, "source:pv-inverter", "driver-pv-inverter", "epoch:pv-inverter", 1)
	if err := publishCut(t, hub, system, cutSystemAsset, 1); err != nil {
		t.Fatalf("system publication: %v", err)
	}
	if err := publishCut(t, hub, inverter, cutInverterAsset, 1); err != nil {
		t.Fatalf("inverter publication: %v", err)
	}
	return hub, system, inverter
}

func captureCut(t *testing.T, hub *Hub) GraphCut {
	t.Helper()
	cut, err := hub.CaptureCut(context.Background())
	if err != nil {
		t.Fatalf("capture cut: %v", err)
	}
	if cut.Cut == nil {
		t.Fatal("capture cut returned no cut")
	}
	if canonical := canonicalCutBytes(t, cut.Cut); string(canonical) != string(cut.Canonical) {
		t.Fatalf("returned bytes are not the cut's canonical bytes:\n%s\nwant %s", cut.Canonical, canonical)
	}
	return cut
}

// canonicalCutBytes encodes a cut's result with every empty collection as an
// empty list.
func canonicalCutBytes(t *testing.T, cut *graph.Cut) []byte {
	t.Helper()
	result := cut.Result()
	if result.Members == nil {
		result.Members = []graph.CutMember{}
	}
	if result.Excluded == nil {
		result.Excluded = []semreg.AssetID{}
	}
	if result.Findings == nil {
		result.Findings = []graph.CutFinding{}
	}
	if result.Relations == nil {
		result.Relations = []graph.CutRelation{}
	}
	canonical, err := semreg.CanonicalJSON(result)
	if err != nil {
		t.Fatalf("cut result does not encode: %v", err)
	}
	return canonical
}

// inspectedCut assembles the expected cut from the hub's current snapshots
// of the named assets, read one by one.
func inspectedCut(t *testing.T, hub *Hub, completeness graph.Completeness, ids ...semreg.AssetID) *graph.Cut {
	t.Helper()
	snapshots := make([]semreg.Snapshot, 0, len(ids))
	for _, id := range ids {
		view, err := hub.Inspect(id)
		if err != nil || !view.Present {
			t.Fatalf("inspect %s: present=%v err=%v", id, view.Present, err)
		}
		snapshots = append(snapshots, view.Snapshot)
	}
	catalog, err := graph.Embedded()
	if err != nil {
		t.Fatal(err)
	}
	cut, err := graph.AssembleCut(snapshots, catalog, completeness)
	if err != nil {
		t.Fatal(err)
	}
	return cut
}

func requireSameCut(t *testing.T, got GraphCut, want *graph.Cut) {
	t.Helper()
	canonical := canonicalCutBytes(t, want)
	if got.Cut.ID() != want.ID() || string(got.Canonical) != string(canonical) {
		t.Fatalf("cut=%s\nwant %s", got.Canonical, canonical)
	}
}

func cutRelation(t *testing.T, cut GraphCut) graph.CutRelation {
	t.Helper()
	relation := cutSystemInverterRelation()
	got, ok := cut.Cut.Relation(semreg.RelationKey{Definition: relation.Definition, From: relation.From, To: relation.To})
	if !ok || got.AssetID != cutSystemAsset {
		t.Fatalf("cross-asset relation missing from the cut: %+v/%v", got, ok)
	}
	return got
}

// Two enforced assets with a cross-asset relation give one complete cut whose
// members are their current snapshots and in which the relation is a
// coherent, qualified instance; nothing is reported as not included and a
// second capture of the unchanged hub gives the same bytes.
func TestGraphCutCaptureJudgesACrossAssetRelationCoherent(t *testing.T) {
	hub, _, _ := cutPair(t)
	cut := captureCut(t, hub)
	if cut.Cut.Completeness() != graph.CutComplete || cut.NotIncluded == nil || len(cut.NotIncluded) != 0 {
		t.Fatalf("completeness=%q not included=%+v", cut.Cut.Completeness(), cut.NotIncluded)
	}
	members := cut.Cut.Members()
	if len(members) != 2 || members[0].AssetID != cutInverterAsset || members[1].AssetID != cutSystemAsset {
		t.Fatalf("members=%+v", members)
	}
	if relation := cutRelation(t, cut); relation.Coherence != graph.CoherenceCoherent || relation.State != semreg.LinkQualified {
		t.Fatalf("cross-asset relation=%+v", relation)
	}
	if findings := cut.Cut.Findings(); len(findings) != 0 {
		t.Fatalf("findings=%+v", findings)
	}
	requireSameCut(t, cut, inspectedCut(t, hub, graph.CutComplete, cutSystemAsset, cutInverterAsset))
	if again := captureCut(t, hub); string(again.Canonical) != string(cut.Canonical) {
		t.Fatal("two captures of an unchanged hub differ")
	}
}

// A declared asset the hub does not admit is not a member: it is listed with
// its reason, the cut is partial and the relation into it is outside the cut.
// Each reason is reached by its own hub state, and the list is in bytewise
// asset order.
func TestGraphCutCaptureListsAnAssetThatIsNotAdmitted(t *testing.T) {
	t.Run("source unavailable", func(t *testing.T) {
		hub := newTestHub(t)
		for _, spec := range []AssetSpec{
			graphPVSpec(cutSystemAsset, "source:pv-system", "driver-pv-system", true),
			graphPVSpec(cutInverterAsset, "source:pv-inverter", "driver-pv-inverter", true),
		} {
			if _, err := hub.Declare(spec); err != nil {
				t.Fatal(err)
			}
		}
		system := admitLegacy(t, hub, "source:pv-system", "driver-pv-system", "epoch:pv-system", 1)
		if err := publishCut(t, hub, system, cutSystemAsset, 1); err != nil {
			t.Fatal(err)
		}
		cut := captureCut(t, hub)
		want := []CutNotIncluded{{Asset: cutInverterAsset, Reason: CutSourceUnavailable}}
		if cut.Cut.Completeness() != graph.CutPartial || !reflect.DeepEqual(cut.NotIncluded, want) {
			t.Fatalf("completeness=%q not included=%+v", cut.Cut.Completeness(), cut.NotIncluded)
		}
		if relation := cutRelation(t, cut); relation.Coherence != graph.CoherenceOutsideCut {
			t.Fatalf("relation into an asset that is not admitted=%+v", relation)
		}
		requireSameCut(t, cut, inspectedCut(t, hub, graph.CutPartial, cutSystemAsset))
	})

	t.Run("fenced source", func(t *testing.T) {
		hub, _, _ := cutPair(t)
		if _, err := hub.Fence(testFence("source:pv-inverter", "epoch:pv-inverter", 1, 2)); err != nil {
			t.Fatal(err)
		}
		cut := captureCut(t, hub)
		want := []CutNotIncluded{{Asset: cutInverterAsset, Reason: CutSourceUnavailable}}
		if cut.Cut.Completeness() != graph.CutPartial || !reflect.DeepEqual(cut.NotIncluded, want) {
			t.Fatalf("completeness=%q not included=%+v", cut.Cut.Completeness(), cut.NotIncluded)
		}
		if relation := cutRelation(t, cut); relation.Coherence != graph.CoherenceOutsideCut {
			t.Fatalf("relation into a fenced asset=%+v", relation)
		}
	})

	t.Run("publication withheld", func(t *testing.T) {
		hub, _, inverter := cutPair(t)
		// A successor generation that has published nothing does not expose
		// its predecessor's snapshot.
		if _, err := hub.Fence(testFence("source:pv-inverter", "epoch:pv-inverter", 1, 2)); err != nil {
			t.Fatal(err)
		}
		if _, err := hub.Activate(inverter, 2); err != nil {
			t.Fatal(err)
		}
		// An admitted asset that has published nothing has no publication to read.
		if _, err := hub.Declare(graphPVSpec("asset:pv-idle", "source:pv-idle", "driver-pv-idle", true)); err != nil {
			t.Fatal(err)
		}
		admitLegacy(t, hub, "source:pv-idle", "driver-pv-idle", "epoch:pv-idle", 1)
		cut := captureCut(t, hub)
		want := []CutNotIncluded{{Asset: "asset:pv-idle", Reason: CutPublicationWithheld}, {Asset: cutInverterAsset, Reason: CutPublicationWithheld}}
		if cut.Cut.Completeness() != graph.CutPartial || !reflect.DeepEqual(cut.NotIncluded, want) {
			t.Fatalf("completeness=%q not included=%+v", cut.Cut.Completeness(), cut.NotIncluded)
		}
		if members := cut.Cut.Members(); len(members) != 1 || members[0].AssetID != cutSystemAsset {
			t.Fatalf("members=%+v", members)
		}
		if relation := cutRelation(t, cut); relation.Coherence != graph.CoherenceOutsideCut {
			t.Fatalf("relation into a withheld asset=%+v", relation)
		}
	})

	t.Run("excluded", func(t *testing.T) {
		hub, _, _ := cutPair(t)
		// A second asset holding the same inverter current excludes both
		// holders; they were read, so they stay members, and are listed as
		// excluded.
		if _, err := hub.Declare(graphPVSpec("asset:pv-twin", "source:pv-twin", "driver-pv-twin", true)); err != nil {
			t.Fatal(err)
		}
		twin := admitLegacy(t, hub, "source:pv-twin", "driver-pv-twin", "epoch:pv-twin", 1)
		if err := publishCut(t, hub, twin, "asset:pv-twin", 1); err != nil {
			t.Fatal(err)
		}
		cut := captureCut(t, hub)
		want := []CutNotIncluded{{Asset: cutInverterAsset, Reason: CutExcluded}, {Asset: "asset:pv-twin", Reason: CutExcluded}}
		if cut.Cut.Completeness() != graph.CutPartial || !reflect.DeepEqual(cut.NotIncluded, want) {
			t.Fatalf("completeness=%q not included=%+v", cut.Cut.Completeness(), cut.NotIncluded)
		}
		if excluded := cut.Cut.Excluded(); !reflect.DeepEqual(excluded, []semreg.AssetID{cutInverterAsset, "asset:pv-twin"}) {
			t.Fatalf("excluded=%v", excluded)
		}
		if relation := cutRelation(t, cut); relation.Coherence != graph.CoherenceExcluded {
			t.Fatalf("relation into an excluded asset=%+v", relation)
		}
		// The hub asserted complete; the exclusion made the cut partial.
		requireSameCut(t, cut, inspectedCut(t, hub, graph.CutComplete, cutSystemAsset, cutInverterAsset, "asset:pv-twin"))
		// Every collection of this cut is non-empty, so SemReg's own encoder
		// accepts it and gives the same bytes.
		if canonical, err := cut.Cut.CanonicalJSON(); err != nil || string(canonical) != string(cut.Canonical) {
			t.Fatalf("SemReg encoding differs (%v)", err)
		}
	})
}

// An asset declared without enforcement that publishes facts of a pack the
// catalog covers is read into the cut and reported as ungraphed; it is
// included, so the cut stays complete.
func TestGraphCutCaptureReportsAnAssetThatIsNotEnforcedAsUngraphed(t *testing.T) {
	hub, _, _ := cutPair(t)
	if _, err := hub.Declare(graphPVSpec("asset:pv-plain", "source:pv-plain", "driver-pv-plain", false)); err != nil {
		t.Fatal(err)
	}
	plain := admitLegacy(t, hub, "source:pv-plain", "driver-pv-plain", "epoch:pv-plain", 1)
	if _, err := publishPV(t, hub, plain, "asset:pv-plain", 1, false, graphMissing); err != nil {
		t.Fatal(err)
	}
	cut := captureCut(t, hub)
	if cut.Cut.Completeness() != graph.CutComplete || len(cut.NotIncluded) != 0 || len(cut.Cut.Members()) != 3 {
		t.Fatalf("completeness=%q not included=%+v members=%+v", cut.Cut.Completeness(), cut.NotIncluded, cut.Cut.Members())
	}
	want := []graph.CutFinding{{Kind: graph.CutUngraphed, Assets: []semreg.AssetID{"asset:pv-plain"}}}
	if findings := cut.Cut.Findings(); !reflect.DeepEqual(findings, want) {
		t.Fatalf("findings=%+v", findings)
	}
	if relation := cutRelation(t, cut); relation.Coherence != graph.CoherenceCoherent {
		t.Fatalf("relation=%+v", relation)
	}
	requireSameCut(t, cut, inspectedCut(t, hub, graph.CutComplete, cutSystemAsset, cutInverterAsset, "asset:pv-plain"))
}

// A commit that is ready while the capture reads waits for the capture: the
// cut holds the snapshot each asset had when the capture began, whichever
// asset the commit targets, and the next capture holds the commit.
func TestGraphCutCaptureHoldsACommitThatLandsDuringTheCapture(t *testing.T) {
	for _, target := range []semreg.AssetID{cutInverterAsset, cutSystemAsset} {
		t.Run(string(target), func(t *testing.T) {
			hub, system, inverter := cutPair(t)
			before := inspectedCut(t, hub, graph.CutComplete, cutSystemAsset, cutInverterAsset)
			tx, err := hub.Begin(map[semreg.AssetID]*Handle{cutSystemAsset: system, cutInverterAsset: inverter}[target], target)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := tx.Apply(cutBatch(t, tx, 2), testMono(2)); err != nil {
				t.Fatal(err)
			}
			// The capture reads the assets in bytewise order; it pauses on the
			// first one while the commit becomes ready.
			entered, release, committing := make(chan struct{}), make(chan struct{}), make(chan struct{})
			hub.faults = func(stage string, id semreg.AssetID) {
				if stage == "capture-read" && id == cutInverterAsset {
					close(entered)
					<-release
				}
				if stage == "commit" {
					close(committing)
				}
			}
			type result struct {
				cut GraphCut
				err error
			}
			read := make(chan result, 1)
			go func() {
				cut, err := hub.CaptureCut(context.Background())
				read <- result{cut, err}
			}()
			<-entered
			written := make(chan error, 1)
			go func() { _, err := tx.Commit(nil); written <- err }()
			<-committing
			close(release)
			r := <-read
			if err := <-written; err != nil {
				t.Fatal(err)
			}
			hub.faults = nil
			if r.err != nil {
				t.Fatal(r.err)
			}
			requireSameCut(t, r.cut, before)
			after := captureCut(t, hub)
			if after.Cut.ID() == before.ID() {
				t.Fatal("the next capture missed the commit")
			}
			requireSameCut(t, after, inspectedCut(t, hub, graph.CutComplete, cutSystemAsset, cutInverterAsset))
		})
	}
}

// Captures that run while both assets publish never mix two snapshots of
// one asset: every cut's bytes equal the cut assembled from snapshots each
// asset actually committed, and its relation stays coherent.
func TestGraphCutCaptureNeverMixesSnapshotsOfOneAsset(t *testing.T) {
	hub, system, inverter := cutPair(t)
	var mu sync.Mutex
	committed := map[semreg.SnapshotID]semreg.Snapshot{}
	record := func(id semreg.AssetID) {
		view, err := hub.Inspect(id)
		if err != nil || !view.Present {
			t.Errorf("inspect %s: %v", id, err)
			return
		}
		mu.Lock()
		committed[view.Snapshot.SnapshotID] = view.Snapshot
		mu.Unlock()
	}
	record(cutSystemAsset)
	record(cutInverterAsset)

	const batches = 24
	var writers sync.WaitGroup
	for id, handle := range map[semreg.AssetID]*Handle{cutSystemAsset: system, cutInverterAsset: inverter} {
		writers.Add(1)
		go func(id semreg.AssetID, handle *Handle) {
			defer writers.Done()
			for at := int64(2); at < 2+batches; at++ {
				if err := publishCut(t, hub, handle, id, at); err != nil {
					t.Errorf("publish %s at %d: %v", id, at, err)
					return
				}
				// Each asset has one writer, so this read returns its commit.
				record(id)
			}
		}(id, handle)
	}
	done := make(chan struct{})
	cuts := make(chan GraphCut, 4*batches)
	go func() {
		defer close(cuts)
		for {
			select {
			case <-done:
				return
			default:
			}
			cut, err := hub.CaptureCut(context.Background())
			if err != nil {
				t.Errorf("capture cut: %v", err)
				return
			}
			select {
			case cuts <- cut:
			default:
			}
		}
	}()
	writers.Wait()
	close(done)
	var captured []GraphCut
	for cut := range cuts {
		captured = append(captured, cut)
	}
	captured = append(captured, captureCut(t, hub))

	catalog, err := graph.Embedded()
	if err != nil {
		t.Fatal(err)
	}
	distinct := map[semreg.Digest]bool{}
	for _, cut := range captured {
		distinct[cut.Cut.ID()] = true
	}
	t.Logf("%d cuts captured, %d distinct", len(captured), len(distinct))
	for index, cut := range captured {
		members := cut.Cut.Members()
		if len(members) != 2 || members[0].AssetID == members[1].AssetID || len(cut.NotIncluded) != 0 {
			t.Fatalf("cut %d members=%+v not included=%+v", index, members, cut.NotIncluded)
		}
		snapshots := make([]semreg.Snapshot, 0, len(members))
		for _, member := range members {
			snapshot, ok := committed[member.SnapshotID]
			if !ok || snapshot.AssetID != member.AssetID {
				t.Fatalf("cut %d holds a snapshot of %s that was never committed", index, member.AssetID)
			}
			snapshots = append(snapshots, snapshot)
		}
		want, err := graph.AssembleCut(snapshots, catalog, graph.CutComplete)
		if err != nil {
			t.Fatal(err)
		}
		requireSameCut(t, cut, want)
		if relation := cutRelation(t, cut); relation.Coherence != graph.CoherenceCoherent {
			t.Fatalf("cut %d relation=%+v", index, relation)
		}
	}
}

// The cut capture fails like the per-asset capture on an absent hub, a
// cancelled context and a restoring hub, refuses more declared assets than a
// cut reads, and gives an empty hub an empty complete cut.
func TestGraphCutCaptureErrorsAndBounds(t *testing.T) {
	var absent *Hub
	if _, err := absent.CaptureCut(context.Background()); !errors.Is(err, ErrCaptureUnavailable) {
		t.Fatalf("absent hub=%v", err)
	}
	hub := newTestHub(t)
	empty := captureCut(t, hub)
	if empty.Cut.Completeness() != graph.CutComplete || len(empty.Cut.Members()) != 0 || empty.NotIncluded == nil || len(empty.NotIncluded) != 0 {
		t.Fatalf("empty hub cut=%s not included=%+v", empty.Canonical, empty.NotIncluded)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := hub.CaptureCut(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context=%v", err)
	}
	hub.mu.Lock()
	hub.restoring = true
	hub.mu.Unlock()
	if _, err := hub.CaptureCut(context.Background()); !errors.Is(err, ErrCaptureUnavailable) {
		t.Fatalf("restoring hub=%v", err)
	}
	hub.mu.Lock()
	hub.restoring = false
	hub.mu.Unlock()

	for index := 0; index <= graph.MaxCutMembers; index++ {
		mustDeclare(t, hub, semreg.AssetID("asset:bound-"+strconv.Itoa(index)), "source:bound", "driver-bound", nil)
	}
	if _, err := hub.CaptureCut(context.Background()); errorID(err) != semreg.BoundsExceeded {
		t.Fatalf("more declared assets than a cut reads=%v", err)
	}
}
