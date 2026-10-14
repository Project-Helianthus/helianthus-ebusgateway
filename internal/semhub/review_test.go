package semhub

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// revokeForTest performs the handle revocation a Fence performs, at an
// arbitrary point chosen by a fault hook.
func revokeForTest(hub *Hub, src semreg.SourceID) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	source := hub.sources[src]
	source.live = false
	source.token++
}

func TestCommitValidatesTheHandleAtomicallyWithTheKernelSwap(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:a", "source:a", "driver-a", nil)
	handle := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 1)
	mustPublish(t, hub, handle, "asset:a", "fam", 1, 1)
	before, _ := hub.Inspect("asset:a")
	tx, err := hub.Begin(handle, "asset:a")
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "fam", 2, 2)
	if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
		t.Fatal(err)
	}
	fired := false
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "commit" {
			fired = true
			revokeForTest(hub, "source:a")
		}
	}
	cursor := hub.Cursor()
	called := false
	if _, err := tx.Commit(func(Committed) { called = true }); errorID(err) != semreg.StaleDriverGeneration || !fired {
		t.Fatalf("commit after revocation=%v fired=%t", err, fired)
	}
	after, _ := hub.Inspect("asset:a")
	if called || hub.Cursor() != cursor || after.Snapshot.SnapshotID != before.Snapshot.SnapshotID {
		t.Fatal("a revoked handle published")
	}
}

func TestFirstPublicationRacingAFenceIsWithdrawnNotLeftReadable(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:p1", "source:p", "driver-p", nil)
	mustDeclare(t, hub, "asset:p2", "source:p", "driver-p", nil)
	handle := admitLegacy(t, hub, "source:p", "driver-p", "epoch:p", 1)
	mustPublish(t, hub, handle, "asset:p1", "race", 1, 1)
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "control-revoke" {
			hub.faults = nil
			mustPublish(t, hub, handle, "asset:p2", "race", 2, 2)
		}
	}
	result, err := hub.Fence(testFence("source:p", "epoch:p", 1, 3))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Assets) != 2 || result.Assets[1].Asset != "asset:p2" || result.Assets[1].Outcome != SemanticAccepted {
		t.Fatalf("racing first publication=%+v", result.Assets)
	}
	view, _ := hub.Inspect("asset:p2")
	if view.Snapshot.Bindings[0].State != semreg.BindingFenced {
		t.Fatalf("racing publication stayed current: %+v", view.Snapshot.Bindings)
	}
	next, err := hub.Activate(handle, 2)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, next, "asset:p2", "race", 3, 4)
}

func TestReentrantCallbackGetsPendingInsteadOfDeadlocking(t *testing.T) {
	hub := newTestHub(t)
	var reentrant []error
	var retryErr error
	var controlID ControlID
	mustDeclare(t, hub, "asset:r", "source:r", "driver-r", func(ControlEvent) error {
		_, fenceErr := hub.Fence(testFence("source:r", "epoch:r", 1, 9))
		_, retireErr := hub.RetireEpoch(RetireRequest{Source: "source:r", Epoch: "epoch:r", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(9), Monotonic: testMono(9)})
		_, retryErr = hub.RetryControl(controlID)
		reentrant = append(reentrant, fenceErr, retireErr)
		return nil
	})
	handle := admitLegacy(t, hub, "source:r", "driver-r", "epoch:r", 1)
	mustPublish(t, hub, handle, "asset:r", "re", 1, 1)
	hub.mu.Lock()
	controlID = ControlID{Incarnation: hub.incarnation, Source: "source:r", N: 1}
	hub.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		_, err := hub.Fence(testFence("source:r", "epoch:r", 1, 2))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("re-entrant control from a reprojection callback deadlocked")
	}
	var settle *SettlePendingError
	for _, err := range append(reentrant, retryErr) {
		if !errors.As(err, &settle) {
			t.Fatalf("re-entrant result=%v", err)
		}
	}
	if !hub.Readable("asset:r") {
		t.Fatal("asset stayed closed after its callback returned")
	}
}

func TestOverlappingResettleRunsOneReprojectionAttempt(t *testing.T) {
	hub := newTestHub(t)
	var mu sync.Mutex
	running, maxRunning, calls := 0, 0, 0
	release := make(chan struct{})
	fail := true
	mustDeclare(t, hub, "asset:o", "source:o", "driver-o", func(ControlEvent) error {
		mu.Lock()
		running++
		calls++
		if running > maxRunning {
			maxRunning = running
		}
		shouldFail := fail
		mu.Unlock()
		if !shouldFail {
			<-release
		}
		mu.Lock()
		running--
		mu.Unlock()
		if shouldFail {
			return errors.New("first attempt fails")
		}
		return nil
	})
	handle := admitLegacy(t, hub, "source:o", "driver-o", "epoch:o", 1)
	mustPublish(t, hub, handle, "asset:o", "ov", 1, 1)
	if _, err := hub.Fence(testFence("source:o", "epoch:o", 1, 2)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	results := make(chan error, 2)
	go func() { results <- hub.Resettle("asset:o") }()
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return running == 1 })
	if err := hub.Resettle("asset:o"); !errors.Is(err, ErrReprojectionInFlight) {
		t.Fatalf("overlapping resettle=%v", err)
	}
	if _, err := hub.Activate(handle, 2); err == nil {
		t.Fatal("activate proceeded while a reprojection attempt was in flight")
	}
	close(release)
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if maxRunning != 1 || calls != 2 || !hub.Readable("asset:o") {
		t.Fatalf("maxRunning=%d calls=%d readable=%t", maxRunning, calls, hub.Readable("asset:o"))
	}
}

func TestActivateReturnsSettleTimeoutForABlockingCallback(t *testing.T) {
	hub := newTestHub(t)
	block := make(chan struct{})
	defer close(block)
	first := true
	mustDeclare(t, hub, "asset:b", "source:b", "driver-b", func(ControlEvent) error {
		if first {
			first = false
			return errors.New("fail once")
		}
		<-block
		return nil
	})
	handle := admitLegacy(t, hub, "source:b", "driver-b", "epoch:b", 1)
	mustPublish(t, hub, handle, "asset:b", "blk", 1, 1)
	if _, err := hub.Fence(testFence("source:b", "epoch:b", 1, 2)); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	var timeout *SettleTimeoutError
	if _, err := hub.Activate(handle, 2); !errors.As(err, &timeout) {
		t.Fatalf("activate=%v", err)
	}
	if elapsed := time.Since(started); elapsed < ControlSettleTimeout || elapsed > ControlSettleTimeout+2*time.Second {
		t.Fatalf("activate took %s", elapsed)
	}
	if hub.Readable("asset:b") {
		t.Fatal("gate opened while the reprojection is still in flight")
	}
	if err := hub.Resettle("asset:b"); !errors.Is(err, ErrReprojectionInFlight) {
		t.Fatalf("attempt not marked in flight: %v", err)
	}
}

func TestControlCommitInstallsSnapshotAndMetadataTogether(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:m", "source:m", "driver-m", nil)
	handle := admitLegacy(t, hub, "source:m", "driver-m", "epoch:m", 1)
	mustPublish(t, hub, handle, "asset:m", "meta", 1, 1)
	var probed InspectView
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "after-commit" {
			probed, _ = hub.Inspect(id)
		}
	}
	result, err := hub.Fence(testFence("source:m", "epoch:m", 1, 2))
	if err != nil {
		t.Fatal(err)
	}
	if probed.Snapshot.SnapshotID != result.Assets[0].SnapshotID || probed.HubSeq != result.Assets[0].HubSeq || probed.Control.Semantic != SemanticAccepted {
		t.Fatalf("inspect after the control commit=%+v hubSeq=%d status=%+v", probed.Snapshot.SnapshotID, probed.HubSeq, probed.Control)
	}
}

func TestReprojectionCallbacksRunInGlobalHubSeqOrder(t *testing.T) {
	hub := newTestHub(t)
	var order []uint64
	var mu sync.Mutex
	record := func(event ControlEvent) error {
		mu.Lock()
		order = append(order, event.HubSeq)
		mu.Unlock()
		return nil
	}
	triggered := false
	mustDeclare(t, hub, "asset:x1", "source:x", "driver-x", func(event ControlEvent) error {
		if !triggered {
			triggered = true
			if _, err := hub.Fence(testFence("source:y", "epoch:y", 1, 5)); err != nil {
				t.Error(err)
			}
		}
		return record(event)
	})
	mustDeclare(t, hub, "asset:x2", "source:x", "driver-x", record)
	mustDeclare(t, hub, "asset:y1", "source:y", "driver-y", record)
	hx := admitLegacy(t, hub, "source:x", "driver-x", "epoch:x", 1)
	hy := admitLegacy(t, hub, "source:y", "driver-y", "epoch:y", 1)
	mustPublish(t, hub, hx, "asset:x1", "ord", 1, 1)
	mustPublish(t, hub, hx, "asset:x2", "ord", 1, 1)
	mustPublish(t, hub, hy, "asset:y1", "ord", 1, 1)
	if _, err := hub.Fence(testFence("source:x", "epoch:x", 1, 2)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 3 })
	mu.Lock()
	defer mu.Unlock()
	if order[0] >= order[1] || order[1] >= order[2] {
		t.Fatalf("callback order=%v", order)
	}
}

func TestIdentityScopingVariesEpochAndGenerationIndependently(t *testing.T) {
	seen := map[semreg.NativeBindingID]bool{}
	for _, epoch := range []semreg.SourceEpochID{"e1", "e2"} {
		for _, generation := range []uint64{1, 2} {
			id, err := NativeBindingID("fam", "asset:a", "source:a", "p", epoch, generation)
			if err != nil || seen[id] {
				t.Fatalf("native binding (%s,%d)=%q err=%v", epoch, generation, id, err)
			}
			seen[id] = true
		}
	}
	for _, family := range []LegacyFamily{LegacyGrowattStorage, LegacyTeslaEVSE, LegacyPVSunSpec} {
		minter, _ := NewLegacyMinter(family)
		ids := map[semreg.NativeBindingID]bool{}
		for _, inc := range []Incarnation{{Asset: "a", Source: "s", Epoch: "e1", Generation: 2}, {Asset: "a", Source: "s", Epoch: "e1", Generation: 3}, {Asset: "a", Source: "s", Epoch: "e2", Generation: 2}} {
			id := minter.Binding(inc, "p")
			if ids[id] {
				t.Fatalf("%s binding not scoped by epoch and generation independently", family)
			}
			ids[id] = true
		}
	}
}

func TestEquivalentMaterialIsByteExactAndOrderIndependentOfTheCaller(t *testing.T) {
	nfc, _ := NativeID(KindAsset, "fam", "café")
	nfd, _ := NativeID(KindAsset, "fam", "café")
	if nfc == nfd {
		t.Fatal("identity material is not byte exact")
	}
	var group sync.WaitGroup
	results := make(chan string, 16)
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			id, _ := NativeID(KindService, "fam", "a", "d", "e", "1")
			results <- id
		}()
	}
	group.Wait()
	close(results)
	first := ""
	for id := range results {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("identity depends on schedule")
		}
	}
	if empty, _ := NativeID(KindAsset, "fam", ""); empty == nfc {
		t.Fatal("empty material collided")
	}
}

func TestHubStateRetainsNoPrivateMaterial(t *testing.T) {
	hub := newTestHub(t)
	asset := PVAssetID([]byte("Maker\x00Model\x00SERIAL-0042"))
	src := PVSourceID([]byte(`{"endpoint":"tcp://198.51.100.9:502","unit_id":1}`))
	mustDeclare(t, hub, asset, src, "pv_sunspec", nil)
	if _, err := hub.AdmitProvisional("pv_sunspec", LegacyPVSunSpec, PVEpochID("tcp://198.51.100.9:502", "2026-09-20T10:00:00Z"), 1); err != nil {
		t.Fatal(err)
	}
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	var state []string
	for id, owner := range hub.ids {
		state = append(state, id, owner.driver)
	}
	for id, source := range hub.sources {
		state = append(state, string(id), string(source.epoch), source.driver)
	}
	for _, value := range state {
		if strings.Contains(value, "SERIAL") || strings.Contains(value, "198.51") || strings.Contains(value, "Maker") {
			t.Fatalf("hub state retains private material: %q", value)
		}
	}
}

func TestLegacyEpochCannotReturnAndSuccessorGenerationIsContiguous(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:g", "source:g", "tesla_evse", nil)
	admit := func(epoch semreg.SourceEpochID, generation uint64) (*Handle, error) {
		return hub.AdmitSource(Admission{Source: "source:g", Driver: "tesla_evse", Legacy: LegacyTeslaEVSE, Epoch: epoch, Generation: generation})
	}
	a, err := admit("epoch:A", 7)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, a, "asset:g", "gen", 1, 1)
	if _, err := hub.Fence(testFence("source:g", "epoch:A", 7, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := admit("epoch:B", 9); !errors.Is(err, ErrAdmission) {
		t.Fatalf("successor generation gap=%v", err)
	}
	b, err := admit("epoch:B", 8)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, b, "asset:g", "gen", 2, 3)
	if _, err := hub.Fence(testFence("source:g", "epoch:B", 8, 4)); err != nil {
		t.Fatal(err)
	}
	if _, err := admit("epoch:A", 9); !errors.Is(err, ErrAdmission) {
		t.Fatalf("epoch A returned within the incarnation: %v", err)
	}
}

func TestApplyThenAbortChangesNothing(t *testing.T) {
	calls := 0
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:ab", "source:ab", "driver-ab", nil)
	handle := admitLegacy(t, hub, "source:ab", "driver-ab", "epoch:ab", 1)
	mustPublish(t, hub, handle, "asset:ab", "abort", 1, 1)
	before, _ := hub.Inspect("asset:ab")
	cursor := hub.Cursor()
	tx, err := hub.Begin(handle, "asset:ab")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx.Apply(testBatch(t, tx, "abort", 2, 2), testMono(2)); err != nil {
		t.Fatal(err)
	}
	tx.Abort()
	if _, err := tx.Commit(func(Committed) { calls++ }); !errors.Is(err, ErrTransaction) {
		t.Fatalf("commit after abort=%v", err)
	}
	after, _ := hub.Inspect("asset:ab")
	if calls != 0 || hub.Cursor() != cursor || after.Snapshot.SnapshotID != before.Snapshot.SnapshotID {
		t.Fatal("an aborted transaction changed state")
	}
	mustPublish(t, hub, handle, "asset:ab", "abort", 2, 2)
}

// intrudeOnKey publishes on the asset under the control key itself, so the
// retained control attempt hits a terminal sequence_conflict.
func intrudeOnKey(t *testing.T, hub *Hub, id semreg.AssetID, src semreg.SourceID, epoch semreg.SourceEpochID) {
	t.Helper()
	hub.mu.RLock()
	target := hub.assets[id]
	hub.mu.RUnlock()
	intruder := &Hub{incarnation: hub.incarnation, journal: NewMemoryJournal(), bus: newEventBus(hub.incarnation, SubscriberQueueLimit), ids: map[string]idOwner{}, assets: map[semreg.AssetID]*asset{id: {id: id, source: src, kernel: target.kernel}}, sources: map[semreg.SourceID]*source{}}
	handle, err := intruder.AdmitSource(Admission{Source: src, Driver: "intruder", Legacy: LegacyGrowattStorage, Epoch: epoch, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, intruder, handle, id, "intruder", 5, 2)
	hub.mu.Lock()
	target.kernel = intruder.assets[id].kernel
	hub.mu.Unlock()
}

func TestRetryControlReportsATerminalRejectionVerbatim(t *testing.T) {
	f := newControlFixture(t)
	f.hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "before-submit" && id == "asset:c2" {
			f.hub.faults = nil
			intrudeOnKey(t, f.hub, id, "source:control", "epoch:control")
		}
	}
	result, err := f.hub.Fence(testFence("source:control", "epoch:control", 1, 3))
	if errorID(err) != semreg.SequenceConflict {
		t.Fatalf("fence=%v", err)
	}
	// The gate distinction: a rejected withdrawal keeps reads closed while its
	// reprojection state stays settled.
	if f.hub.ReadAdmitted("asset:c2") || !f.hub.Settled("asset:c2") {
		t.Fatal("rejected semantic outcome and settled reprojection are not distinct")
	}
	retried, err := f.hub.RetryControl(result.ID)
	if errorID(err) != semreg.SequenceConflict || retried.Assets[1].Outcome != SemanticRejected || retried.Assets[2].Outcome != SemanticUnbuilt {
		t.Fatalf("terminal retry=%+v err=%v", retried.Assets, err)
	}
	if len(f.hub.attempts(t, result.ID, "asset:c2")) != 1 {
		t.Fatal("terminal rejection was rebuilt")
	}
}

func TestStandaloneRetirementCoversEveryGenerationAndFailsAfterAFence(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:sr", "source:sr", "driver-sr", nil)
	handle := admitLegacy(t, hub, "source:sr", "driver-sr", "epoch:sr", 1)
	mustPublish(t, hub, handle, "asset:sr", "sr", 1, 1)
	if _, err := hub.Fence(testFence("source:sr", "epoch:sr", 1, 2)); err != nil {
		t.Fatal(err)
	}
	retire := RetireRequest{Source: "source:sr", Epoch: "epoch:sr", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(3), Monotonic: testMono(3)}
	if _, err := hub.RetireEpoch(retire); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("retirement after a fence=%v", err)
	}
	next, err := hub.Activate(handle, 2)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, next, "asset:sr", "sr", 2, 4)
	retire.ObservedAt, retire.Monotonic = testWall(5), testMono(5)
	result, err := hub.RetireEpoch(retire)
	if err != nil || result.Assets[0].Outcome != SemanticAccepted {
		t.Fatalf("multi-generation retirement=%+v err=%v", result.Assets, err)
	}
	view, _ := hub.Inspect("asset:sr")
	for _, binding := range view.Snapshot.Bindings {
		if binding.State != semreg.BindingRetired && binding.State != semreg.BindingFenced {
			t.Fatalf("binding=%+v", binding)
		}
	}
	if view.Snapshot.Sources[0].State != semreg.SourceRetired {
		t.Fatalf("source=%+v", view.Snapshot.Sources)
	}
}

// Round-2 review regressions.

func TestRetryCallbackFencingItsOwnSourceDuringActivateGetsSettlePendingPromptly(t *testing.T) {
	hub := newTestHub(t)
	attempts := 0
	var inner error
	mustDeclare(t, hub, "asset:w", "source:w", "driver-w", func(ControlEvent) error {
		attempts++
		if attempts == 1 {
			return errors.New("first attempt fails")
		}
		_, inner = hub.Fence(testFence("source:w", "epoch:w", 1, 9))
		return nil
	})
	handle := admitLegacy(t, hub, "source:w", "driver-w", "epoch:w", 1)
	mustPublish(t, hub, handle, "asset:w", "w", 1, 1)
	if _, err := hub.Fence(testFence("source:w", "epoch:w", 1, 2)); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := hub.Activate(handle, 2); err != nil {
		t.Fatalf("activate=%v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("activate waited %s for a callback blocked on the source mutex", elapsed)
	}
	var settle *SettlePendingError
	if !errors.As(inner, &settle) {
		t.Fatalf("re-entrant fence from the retry callback=%v", inner)
	}
}

func TestResettleNeverOvertakesTheDrainerOrder(t *testing.T) {
	hub := newTestHub(t)
	release := make(chan struct{})
	var mu sync.Mutex
	var order []semreg.AssetID
	entered := make(chan struct{}, 1)
	mustDeclare(t, hub, "asset:q1", "source:q", "driver-q", func(event ControlEvent) error {
		entered <- struct{}{}
		<-release
		mu.Lock()
		order = append(order, event.Asset)
		mu.Unlock()
		return nil
	})
	mustDeclare(t, hub, "asset:q2", "source:q", "driver-q", func(event ControlEvent) error {
		mu.Lock()
		order = append(order, event.Asset)
		mu.Unlock()
		return nil
	})
	handle := admitLegacy(t, hub, "source:q", "driver-q", "epoch:q", 1)
	mustPublish(t, hub, handle, "asset:q1", "q", 1, 1)
	mustPublish(t, hub, handle, "asset:q2", "q", 1, 1)
	done := make(chan error, 1)
	go func() {
		_, err := hub.Fence(testFence("source:q", "epoch:q", 1, 2))
		done <- err
	}()
	<-entered
	if err := hub.Resettle("asset:q2"); !errors.Is(err, ErrReprojectionQueued) {
		t.Fatalf("resettle while the drainer runs a lower HubSeq=%v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 2 })
	mu.Lock()
	defer mu.Unlock()
	if order[0] != "asset:q1" || order[1] != "asset:q2" {
		t.Fatalf("execution order=%v", order)
	}
}

func TestDrainWaitsForAnInFlightAttemptInsteadOfDroppingTheEntry(t *testing.T) {
	hub := newTestHub(t)
	release := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	mustDeclare(t, hub, "asset:d", "source:d", "driver-d", func(ControlEvent) error {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			<-release
			return errors.New("first attempt fails")
		}
		return nil
	})
	handle := admitLegacy(t, hub, "source:d", "driver-d", "epoch:d", 1)
	mustPublish(t, hub, handle, "asset:d", "d", 1, 1)
	hub.mu.Lock()
	hub.draining = true
	hub.mu.Unlock()
	if _, err := hub.Fence(testFence("source:d", "epoch:d", 1, 2)); err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	ca := hub.assets["asset:d"].gate
	entry := hub.queue[0]
	hub.queue = nil
	inFlight, err := hub.startAttemptLocked(ca)
	hub.queue = append(hub.queue, entry)
	hub.draining = false
	hub.mu.Unlock()
	if err != nil || inFlight == nil {
		t.Fatalf("direct attempt=%v", err)
	}
	drained := make(chan struct{})
	go func() { hub.drain(); close(drained) }()
	close(release)
	<-drained
	<-entry.done
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 || !hub.Readable("asset:d") {
		t.Fatalf("calls=%d readable=%t: the queued entry was dropped", calls, hub.Readable("asset:d"))
	}
}

func TestRetiredTeslaEpochSuccessorStillContinuesTheGeneration(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:t", "source:t", "tesla_evse", nil)
	admit := func(epoch semreg.SourceEpochID, generation uint64) (*Handle, error) {
		return hub.AdmitSource(Admission{Source: "source:t", Driver: "tesla_evse", Legacy: LegacyTeslaEVSE, Epoch: epoch, Generation: generation})
	}
	first, err := admit("epoch:t1", 7)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, first, "asset:t", "t", 1, 1)
	if _, err := hub.RetireEpoch(RetireRequest{Source: "source:t", Epoch: "epoch:t1", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(2), Monotonic: testMono(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := admit("epoch:t2", 1); !errors.Is(err, ErrAdmission) {
		t.Fatalf("tesla generation reset after retirement=%v", err)
	}
	if _, err := admit("epoch:t2", 8); err != nil {
		t.Fatalf("tesla successor at current+1 after retirement=%v", err)
	}
}

func TestCommitTransitionBlocksAConcurrentRevocationUntilItCompletes(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:a", "source:a", "driver-a", nil)
	handle := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 1)
	tx, err := hub.Begin(handle, "asset:a")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx.Apply(testBatch(t, tx, "fam", 1, 1), testMono(1)); err != nil {
		t.Fatal(err)
	}
	revoked := make(chan struct{})
	blocked := false
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "commit-locked" {
			go func() { revokeForTest(hub, "source:a"); close(revoked) }()
			select {
			case <-revoked:
			case <-time.After(50 * time.Millisecond):
				blocked = true
			}
		}
	}
	if outcome, err := tx.Commit(nil); err != nil || outcome != Accepted {
		t.Fatalf("commit=%v/%v", outcome, err)
	}
	<-revoked
	if !blocked {
		t.Fatal("a revocation ran between the handle check and the kernel swap")
	}
	if _, err := hub.Begin(handle, "asset:a"); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("revocation after the transition=%v", err)
	}
}

func TestControlTransitionBlocksAConcurrentInspectUntilItCompletes(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:m", "source:m", "driver-m", nil)
	handle := admitLegacy(t, hub, "source:m", "driver-m", "epoch:m", 1)
	mustPublish(t, hub, handle, "asset:m", "meta", 1, 1)
	var probed InspectView
	inspected := make(chan struct{})
	blocked := false
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "after-swap" {
			go func() { probed, _ = hub.Inspect(id); close(inspected) }()
			select {
			case <-inspected:
			case <-time.After(50 * time.Millisecond):
				blocked = true
			}
		}
	}
	result, err := hub.Fence(testFence("source:m", "epoch:m", 1, 2))
	if err != nil {
		t.Fatal(err)
	}
	<-inspected
	if !blocked || probed.Snapshot.SnapshotID != result.Assets[0].SnapshotID || probed.HubSeq != result.Assets[0].HubSeq || probed.Control.Semantic != SemanticAccepted {
		t.Fatalf("inspect interleaved the control transition: blocked=%t view=%s hubSeq=%d status=%+v", blocked, probed.Snapshot.SnapshotID, probed.HubSeq, probed.Control)
	}
}
