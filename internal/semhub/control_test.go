package semhub

import (
	"errors"
	"sync"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// controlFixture is one source with three published assets. Each asset's
// reprojection callback records the events it saw and can be told to fail.
type controlFixture struct {
	hub    *Hub
	handle *Handle
	mu     sync.Mutex
	fail   map[semreg.AssetID]error
	seen   map[semreg.AssetID][]ControlEvent
	probe  func(ControlEvent)
}

var controlAssets = []semreg.AssetID{"asset:c1", "asset:c2", "asset:c3"}

func newControlFixture(t *testing.T) *controlFixture {
	t.Helper()
	f := &controlFixture{hub: newTestHub(t), fail: map[semreg.AssetID]error{}, seen: map[semreg.AssetID][]ControlEvent{}}
	for _, id := range controlAssets {
		mustDeclare(t, f.hub, id, "source:control", "driver-control", f.onControl)
	}
	f.handle = admitLegacy(t, f.hub, "source:control", "driver-control", "epoch:control", 1)
	for index, id := range controlAssets {
		mustPublish(t, f.hub, f.handle, id, "control", int64(index+1), 1)
	}
	return f
}

func (f *controlFixture) onControl(event ControlEvent) error {
	f.mu.Lock()
	f.seen[event.Asset] = append(f.seen[event.Asset], event)
	err, probe := f.fail[event.Asset], f.probe
	f.mu.Unlock()
	if probe != nil {
		probe(event)
	}
	return err
}

func (f *controlFixture) setFail(id semreg.AssetID, err error) {
	f.mu.Lock()
	f.fail[id] = err
	f.mu.Unlock()
}

func (f *controlFixture) calls(id semreg.AssetID) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.seen[id])
}

// intrude changes an asset's semantic revision without touching the control
// key, modelling a kernel mutation between build and submit.
func intrude(t *testing.T, hub *Hub, id semreg.AssetID) {
	t.Helper()
	hub.mu.RLock()
	target := hub.assets[id]
	hub.mu.RUnlock()
	intruder := &Hub{incarnation: hub.incarnation, journal: NewMemoryJournal(), bus: newEventBus(hub.incarnation, SubscriberQueueLimit), ids: map[string]idOwner{}, assets: map[semreg.AssetID]*asset{id: {id: id, source: "source:intruder", kernel: target.kernel}}, sources: map[semreg.SourceID]*source{}}
	handle, err := intruder.AdmitSource(Admission{Source: "source:intruder", Driver: "intruder", Legacy: LegacyGrowattStorage, Epoch: "epoch:intruder", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	intruder.assets[id].source = "source:intruder"
	tx, err := intruder.Begin(handle, id)
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "intruder", 9, 2)
	if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(nil); err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	target.kernel = intruder.assets[id].kernel
	hub.mu.Unlock()
}

func TestFenceClosesReadsUntilTheFamilyAcknowledgesTheExactSnapshot(t *testing.T) {
	f := newControlFixture(t)
	var probed []bool
	f.probe = func(event ControlEvent) {
		view, err := f.hub.Inspect(event.Asset)
		probed = append(probed, err == nil && !f.hub.ReadAdmitted(event.Asset) && !f.hub.Settled(event.Asset) && !f.hub.Readable(event.Asset) &&
			view.Snapshot.SnapshotID == event.SnapshotID && event.View.Snapshot.SnapshotID == event.SnapshotID)
	}
	result, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3))
	if err != nil {
		t.Fatal(err)
	}
	if len(probed) != 3 {
		t.Fatalf("callbacks=%d", len(probed))
	}
	for index, ok := range probed {
		if !ok {
			t.Fatalf("callback %d did not see both gates closed on the exact control snapshot", index)
		}
	}
	for index, asset := range result.Assets {
		if asset.Outcome != SemanticAccepted || asset.Asset != controlAssets[index] || !f.hub.Readable(asset.Asset) {
			t.Fatalf("asset result %d=%+v readable=%t", index, asset, f.hub.Readable(asset.Asset))
		}
		view, _ := f.hub.Inspect(asset.Asset)
		if view.Snapshot.Bindings[0].State != semreg.BindingFenced || len(view.Snapshot.Facts) != 0 || len(view.Snapshot.Retained) != 1 {
			t.Fatalf("withdrawn view=%+v", view.Snapshot)
		}
	}
	// The retained observation is kept by the kernel, not stripped by the hub.
	view, _ := f.hub.Inspect("asset:c1")
	if view.Snapshot.Retained[0].Removal != semreg.RetainedRemovalGenerationFence {
		t.Fatalf("retained removal=%q", view.Snapshot.Retained[0].Removal)
	}
}

func TestPartialFenceFailsClosedAndRetryRebuildsOnlyARevisionConflict(t *testing.T) {
	f := newControlFixture(t)
	f.hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "before-submit" && id == "asset:c2" {
			f.hub.faults = nil
			intrude(t, f.hub, id)
		}
	}
	result, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3))
	if errorID(err) != semreg.RevisionConflict {
		t.Fatalf("fence error=%v", err)
	}
	want := []SemanticState{SemanticAccepted, SemanticRejected, SemanticUnbuilt}
	for index, asset := range result.Assets {
		if asset.Outcome != want[index] {
			t.Fatalf("asset %d outcome=%s want %s", index, asset.Outcome, want[index])
		}
	}
	if !f.hub.Readable("asset:c1") || f.hub.Readable("asset:c2") || f.hub.Readable("asset:c3") {
		t.Fatal("partial failure did not fail closed")
	}
	if _, err := f.hub.Begin(f.handle, "asset:c1"); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("handle after partial fence=%v", err)
	}
	var pending *ControlPendingError
	if _, err := f.hub.Activate(f.handle, 2); !errors.As(err, &pending) || pending.ID != result.ID {
		t.Fatalf("activate during unfinished control=%v", err)
	}
	firstBytes := f.hub.attempts(t, result.ID, "asset:c2")[0]
	retried, err := f.hub.RetryControl(result.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.hub.attempts(t, result.ID, "asset:c1")) != 1 || f.calls("asset:c1") != 1 {
		t.Fatal("retry rebuilt or replayed an accepted asset")
	}
	attempts := f.hub.attempts(t, result.ID, "asset:c2")
	if len(attempts) != 2 || string(attempts[0].Batch) != string(firstBytes.Batch) || attempts[0].Sequence != attempts[1].Sequence || attempts[0].Digest == attempts[1].Digest {
		t.Fatalf("revision_conflict retry attempts=%+v", attempts)
	}
	for index, asset := range retried.Assets {
		if asset.Outcome != SemanticAccepted || retried.ID != result.ID {
			t.Fatalf("retried asset %d=%+v", index, asset)
		}
	}
	next, err := f.hub.Activate(f.handle, 2)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, f.hub, next, "asset:c1", "control", 7, 4)
}

func (h *Hub) attempts(t *testing.T, id ControlID, assetID semreg.AssetID) []Attempt {
	t.Helper()
	op, err := h.lookupControl(id)
	if err != nil {
		t.Fatal(err)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ca := range op.assets {
		if ca.asset.id == assetID {
			return append([]Attempt(nil), ca.attempts...)
		}
	}
	t.Fatalf("asset %s not in control operation", assetID)
	return nil
}

func TestUncertainSubmissionResubmitsTheExactRetainedBytes(t *testing.T) {
	for name, stage := range map[string]string{"kernel accepted after all": "after-swap", "kernel never accepted": "before-swap"} {
		t.Run(name, func(t *testing.T) {
			f := newControlFixture(t)
			f.hub.faults = func(at string, id semreg.AssetID) {
				if at == stage && id == "asset:c2" {
					f.hub.faults = nil
					panic("injected interruption")
				}
			}
			result, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3))
			if err == nil || result.Assets[1].Outcome != SemanticUncertain || result.Assets[2].Outcome != SemanticUnbuilt {
				t.Fatalf("fence=%+v err=%v", result.Assets, err)
			}
			retried, err := f.hub.RetryControl(result.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := SemanticAccepted
			if stage == "after-swap" {
				want = SemanticReplayed
			}
			if retried.Assets[1].Outcome != want || retried.Assets[2].Outcome != SemanticAccepted {
				t.Fatalf("retry=%+v", retried.Assets)
			}
			if attempts := f.hub.attempts(t, result.ID, "asset:c2"); len(attempts) != 1 {
				t.Fatalf("uncertain submission was rebuilt: %d attempts", len(attempts))
			}
			if !f.hub.Readable("asset:c2") || !f.hub.Readable("asset:c3") {
				t.Fatal("resolved assets stayed closed")
			}
		})
	}
}

func TestTerminalRejectionIsReportedAndAbandonedAssetsStayClosed(t *testing.T) {
	f := newControlFixture(t)
	f.hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "before-submit" && id == "asset:c2" {
			f.hub.faults = nil
			intrude(t, f.hub, id)
		}
	}
	result, _ := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3))
	before := f.hub.Cursor()
	var pending *ControlPendingError
	if _, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 4)); !errors.As(err, &pending) {
		t.Fatalf("second fence=%v", err)
	}
	if _, err := f.hub.RetireEpoch(RetireRequest{Source: "source:control", Epoch: "epoch:control", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(4), Monotonic: testMono(4)}); !errors.As(err, &pending) {
		t.Fatalf("retire during unfinished control=%v", err)
	}
	if _, err := f.hub.Activate(f.handle, 2); !errors.As(err, &pending) {
		t.Fatalf("activate during unfinished control=%v", err)
	}
	if f.hub.Cursor() != before {
		t.Fatal("a refused control request emitted an event")
	}
	if err := f.hub.AbandonControl(result.ID); err != nil {
		t.Fatal(err)
	}
	if f.hub.Readable("asset:c2") || f.hub.Readable("asset:c3") || !f.hub.Readable("asset:c1") {
		t.Fatal("abandon reopened an unresolved asset")
	}
	if _, err := f.hub.Activate(f.handle, 2); !errors.Is(err, ErrControlAbandoned) {
		t.Fatalf("activate after abandon=%v", err)
	}
	renewed, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 5))
	if err != nil {
		t.Fatal(err)
	}
	if len(renewed.Assets) != 2 || renewed.Assets[0].Asset != "asset:c2" || renewed.Assets[1].Asset != "asset:c3" {
		t.Fatalf("renewed fence assets=%+v", renewed.Assets)
	}
	for _, id := range controlAssets {
		if !f.hub.Readable(id) {
			t.Fatalf("%s stayed closed after a successful new control operation", id)
		}
	}
	if _, err := f.hub.Activate(f.handle, 2); err != nil {
		t.Fatal(err)
	}
}

func TestCallbackErrorKeepsReadsClosedAndResettleRetriesWithoutABatch(t *testing.T) {
	f := newControlFixture(t)
	f.setFail("asset:c1", errors.New("cache rebuild failed"))
	if _, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3)); err != nil {
		t.Fatal(err)
	}
	if f.hub.Readable("asset:c1") || f.hub.Settled("asset:c1") || !f.hub.Readable("asset:c2") {
		t.Fatal("callback error did not keep only its asset closed")
	}
	view, _ := f.hub.Inspect("asset:c1")
	if err := f.hub.Resettle("asset:c1"); err == nil {
		t.Fatal("failing callback settled")
	}
	f.setFail("asset:c1", nil)
	if err := f.hub.Resettle("asset:c1"); err != nil {
		t.Fatal(err)
	}
	after, _ := f.hub.Inspect("asset:c1")
	if !f.hub.Readable("asset:c1") || after.Snapshot.SnapshotID != view.Snapshot.SnapshotID || f.calls("asset:c1") != 3 {
		t.Fatalf("resettle readable=%t snapshot changed=%t calls=%d", f.hub.Readable("asset:c1"), after.Snapshot.SnapshotID != view.Snapshot.SnapshotID, f.calls("asset:c1"))
	}
}

func TestResettleAfterARejectedWithdrawalDoesNotReopen(t *testing.T) {
	f := newControlFixture(t)
	f.hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "before-submit" && id == "asset:c1" {
			f.hub.faults = nil
			intrude(t, f.hub, id)
		}
	}
	if _, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3)); errorID(err) != semreg.RevisionConflict {
		t.Fatalf("fence=%v", err)
	}
	if err := f.hub.Resettle("asset:c1"); !errors.Is(err, ErrSemanticOutstanding) {
		t.Fatalf("resettle=%v", err)
	}
	if f.hub.Readable("asset:c1") || f.calls("asset:c1") != 0 {
		t.Fatal("resettle reopened or acknowledged a rejected withdrawal")
	}
}

func TestSettlingControlResettlesBeforeTheNextMutation(t *testing.T) {
	f := newControlFixture(t)
	f.setFail("asset:c1", errors.New("cache rebuild failed"))
	if _, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3)); err != nil {
		t.Fatal(err)
	}
	var settle *SettlePendingError
	if _, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 4)); !errors.As(err, &settle) {
		t.Fatalf("fence while callback keeps failing=%v", err)
	}
	f.setFail("asset:c1", nil)
	// The resettle succeeds, so the request proceeds and SemReg refuses the
	// already fenced generation verbatim.
	if _, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 5)); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("fence after resettle=%v", err)
	}
	if !f.hub.Readable("asset:c1") {
		t.Fatal("resettled asset stayed closed")
	}
}

func TestActivateTimesOutWhileReprojectionKeepsFailing(t *testing.T) {
	f := newControlFixture(t)
	f.setFail("asset:c1", errors.New("cache rebuild failed"))
	result, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3))
	if err != nil {
		t.Fatal(err)
	}
	before := f.hub.Cursor()
	started := time.Now()
	var timeout *SettleTimeoutError
	if _, err := f.hub.Activate(f.handle, 2); !errors.As(err, &timeout) || timeout.ID != result.ID {
		t.Fatalf("activate=%v", err)
	}
	if elapsed := time.Since(started); elapsed < ControlSettleTimeout {
		t.Fatalf("activate returned after %s", elapsed)
	}
	events, _ := f.hub.Since(before)
	if len(events) != 0 {
		t.Fatalf("settle timeout emitted %+v", events)
	}
	if info, _ := f.hub.Source("source:control"); info.Live || info.Generation != 1 {
		t.Fatalf("settle timeout changed the source: %+v", info)
	}
}

func TestMixedFailureStaysUnfinishedUntilEverySemanticOutcomeResolves(t *testing.T) {
	f := newControlFixture(t)
	f.setFail("asset:c1", errors.New("cache rebuild failed"))
	f.hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "before-submit" && id == "asset:c2" {
			f.hub.faults = nil
			intrude(t, f.hub, id)
		}
	}
	result, _ := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3))
	var pending *ControlPendingError
	if _, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 4)); !errors.As(err, &pending) {
		t.Fatalf("fence=%v", err)
	}
	f.setFail("asset:c1", nil)
	if err := f.hub.Resettle("asset:c1"); err != nil {
		t.Fatal(err)
	}
	if !f.hub.Readable("asset:c1") || f.hub.Readable("asset:c2") || f.hub.Readable("asset:c3") {
		t.Fatal("resettle reopened the wrong assets")
	}
	if _, err := f.hub.Activate(f.handle, 2); !errors.As(err, &pending) {
		t.Fatalf("activate while unfinished=%v", err)
	}
	if _, err := f.hub.RetryControl(result.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.Activate(f.handle, 2); err != nil {
		t.Fatal(err)
	}
}

func TestFenceOfAnUnpublishedGenerationRevokesWithoutABatch(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:u1", "source:u", "driver-u", nil)
	mustDeclare(t, hub, "asset:u2", "source:u", "driver-u", nil)
	handle := admitLegacy(t, hub, "source:u", "driver-u", "epoch:u", 1)
	before := hub.Cursor()
	result, err := hub.Fence(testFence("source:u", "epoch:u", 1, 1))
	if err != nil {
		t.Fatal(err)
	}
	for _, asset := range result.Assets {
		if asset.Outcome != SemanticRevokedUnpublished || !hub.Readable(asset.Asset) {
			t.Fatalf("unpublished asset=%+v", asset)
		}
		if view, _ := hub.Inspect(asset.Asset); view.Present {
			t.Fatal("hub-level revocation submitted a batch")
		}
	}
	events, _ := hub.Since(before)
	if len(events) != 2 || events[0].Kind != EventGenerationRevoked || !events[0].NoPublication {
		t.Fatalf("events=%+v", events)
	}
	next, err := hub.Activate(handle, 2)
	if err != nil {
		t.Fatal(err)
	}
	// A historical asset on which generation 2 never published.
	mustPublish(t, hub, next, "asset:u1", "unpub", 1, 2)
	result, err = hub.Fence(testFence("source:u", "epoch:u", 2, 3))
	if err != nil {
		t.Fatal(err)
	}
	if result.Assets[0].Outcome != SemanticAccepted || result.Assets[1].Outcome != SemanticRevokedUnpublished {
		t.Fatalf("historical asset result=%+v", result.Assets)
	}
	if _, err := hub.Activate(next, 3); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidControlRequestChangesNothing(t *testing.T) {
	f := newControlFixture(t)
	before := f.hub.Cursor()
	tooMany := make([]semreg.EvidenceRef, 33)
	for index := range tooMany {
		tooMany[index] = testEvidence("semhubtest.fence")
	}
	for name, mutate := range map[string]func(*FenceRequest){
		"no evidence":       func(r *FenceRequest) { r.Evidence = nil },
		"too much evidence": func(r *FenceRequest) { r.Evidence = tooMany },
		"invalid reason":    func(r *FenceRequest) { r.Reason = "Not A Definition" },
		"invalid instant":   func(r *FenceRequest) { r.ObservedAt = semreg.TimePoint{} },
		"zero generation":   func(r *FenceRequest) { r.Generation = 0 },
		"invalid evidence":  func(r *FenceRequest) { r.Evidence = []semreg.EvidenceRef{{}} },
	} {
		request := testFence("source:control", "epoch:control", 1, 3)
		mutate(&request)
		if _, err := f.hub.Fence(request); !errors.Is(err, ErrControlRequest) {
			t.Fatalf("%s: error=%v", name, err)
		}
	}
	if f.hub.Cursor() != before {
		t.Fatal("invalid request emitted an event")
	}
	for _, id := range controlAssets {
		if !f.hub.ReadAdmitted(id) || !f.hub.Settled(id) {
			t.Fatal("invalid request touched a gate")
		}
	}
	mustPublish(t, f.hub, f.handle, "asset:c1", "control", 5, 2)
}

func TestNewControlOnAFencedGenerationOrRetiredEpochReturnsSemRegErrors(t *testing.T) {
	f := newControlFixture(t)
	if _, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 4)); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("second fence=%v", err)
	}
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:r", "source:r", "driver-r", nil)
	handle := admitLegacy(t, hub, "source:r", "driver-r", "epoch:r", 1)
	mustPublish(t, hub, handle, "asset:r", "retire", 1, 1)
	retire := RetireRequest{Source: "source:r", Epoch: "epoch:r", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(2), Monotonic: testMono(2), RetirementReason: "lifecycle.runtime_closed"}
	result, err := hub.RetireEpoch(retire)
	if err != nil || result.Assets[0].Outcome != SemanticAccepted {
		t.Fatalf("retire=%+v err=%v", result.Assets, err)
	}
	view, _ := hub.Inspect("asset:r")
	if view.Snapshot.Sources[0].State != semreg.SourceRetired {
		t.Fatalf("retired source=%+v", view.Snapshot.Sources)
	}
	if _, err := hub.RetireEpoch(retire); errorID(err) != semreg.StaleSourceEpoch {
		t.Fatalf("second retire=%v", err)
	}
	if _, err := hub.Begin(handle, "asset:r"); errorID(err) != semreg.StaleSourceEpoch {
		t.Fatalf("handle after retire=%v", err)
	}
}

func TestFutureGenerationFenceIsRefused(t *testing.T) {
	f := newControlFixture(t)
	if _, err := f.hub.Fence(testFence("source:control", "epoch:control", 2, 3)); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("future fence=%v", err)
	}
	mustPublish(t, f.hub, f.handle, "asset:c1", "control", 4, 2)
}
