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

// Final-review regressions.

func TestFailedJournalAppendLeavesTheAdmissionRetryable(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub, "asset:j", "source:j", "driver-j", nil)
	cursor := hub.Cursor()
	journal.failure = errors.New("store unavailable")
	if _, err := hub.AdmitSource(Admission{Source: "source:j", Driver: "driver-j", Legacy: LegacyGrowattStorage, Epoch: "epoch:j", Generation: 1}); err == nil {
		t.Fatal("admission succeeded without its journal record")
	}
	if info, _ := hub.Source("source:j"); info.Live || info.Epoch != "" || hub.Cursor() != cursor {
		t.Fatalf("failed admission was exposed: %+v", info)
	}
	journal.failure = nil
	handle, err := hub.AdmitSource(Admission{Source: "source:j", Driver: "driver-j", Legacy: LegacyGrowattStorage, Epoch: "epoch:j", Generation: 1})
	if err != nil {
		t.Fatalf("retry after a journal failure=%v", err)
	}
	mustPublish(t, hub, handle, "asset:j", "j", 1, 1)
	if _, err := hub.Fence(testFence("source:j", "epoch:j", 1, 2)); err != nil {
		t.Fatal(err)
	}
	journal.failure = errors.New("store unavailable")
	if _, err := hub.Activate(handle, 2); err == nil {
		t.Fatal("activation succeeded without its journal record")
	}
	journal.failure = nil
	if _, err := hub.Activate(handle, 2); err != nil {
		t.Fatalf("activation retry=%v", err)
	}
}

func TestGenerationTransitionCommitsTheFenceAndActivationAtomically(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:gt", "source:gt", "driver-gt", nil)
	handle := admitLegacy(t, hub, "source:gt", "driver-gt", "epoch:gt", 1)
	mustPublish(t, hub, handle, "asset:gt", "gt", 1, 1)
	before, _ := hub.Inspect("asset:gt")
	transition := func(fail bool) (*Tx, error) {
		tx, err := hub.BeginGeneration(handle, "asset:gt", 2)
		if err != nil {
			return nil, err
		}
		if fences := tx.GenerationFences(); len(fences) != 1 || fences[0] != "1" {
			t.Fatalf("pending fences=%v", fences)
		}
		batch := testBatch(t, tx, "gt", 2, 2)
		batch.GenerationFences = []semreg.GenerationFence{{SourceID: "source:gt", SourceEpochID: "epoch:gt", DriverGeneration: "1", Reason: "lifecycle.driver_generation_replaced", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, Revision: "1"}}
		if fail {
			batch.ExpectedSemanticRevision = "99"
		}
		digest, _ := batch.ComputedDigest()
		batch.BatchDigest = digest
		if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
			tx.Abort()
			return nil, err
		}
		_, err = tx.Commit(nil)
		return tx, err
	}
	cursor := hub.Cursor()
	if _, err := transition(true); errorID(err) != semreg.RevisionConflict {
		t.Fatalf("failing transition=%v", err)
	}
	after, _ := hub.Inspect("asset:gt")
	if info, _ := hub.Source("source:gt"); info.Generation != 1 || after.Snapshot.SnapshotID != before.Snapshot.SnapshotID || hub.Cursor() != cursor {
		t.Fatal("a rejected transition fenced or activated")
	}
	mustPublish(t, hub, handle, "asset:gt", "gt", 3, 2)
	tx, err := transition(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Begin(handle, "asset:gt"); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("old handle after transition=%v", err)
	}
	if tx.Handle().Generation() != 2 {
		t.Fatalf("new handle generation=%d", tx.Handle().Generation())
	}
	events, _ := hub.Since(cursor)
	kinds := []EventKind{}
	for _, event := range events[1:] {
		kinds = append(kinds, event.Kind)
	}
	if len(kinds) != 3 || kinds[0] != EventGenerationActivated || kinds[1] != EventGenerationFenced || kinds[2] != EventPublished {
		t.Fatalf("transition events=%v", kinds)
	}
	mustPublish(t, hub, tx.Handle(), "asset:gt", "gt", 4, 3)
}

func TestFencePausedBeforeRevocationDoesNotRevokeACommittedGenerationReplacement(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:gr", "source:gr", "driver-gr", nil)
	handle := admitLegacy(t, hub, "source:gr", "driver-gr", "epoch:gr", 1)
	mustPublish(t, hub, handle, "asset:gr", "gr", 1, 1)
	var replacement *Handle
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage != "control-revoke" {
			return
		}
		hub.faults = nil
		tx, err := hub.BeginGeneration(handle, "asset:gr", 2)
		if err != nil {
			t.Error(err)
			return
		}
		batch := testBatch(t, tx, "gr", 2, 2)
		batch.GenerationFences = []semreg.GenerationFence{{SourceID: "source:gr", SourceEpochID: "epoch:gr", DriverGeneration: "1", Reason: "lifecycle.driver_generation_replaced", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, Revision: "1"}}
		digest, _ := batch.ComputedDigest()
		batch.BatchDigest = digest
		if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
			t.Error(err)
			tx.Abort()
			return
		}
		if _, err := tx.Commit(nil); err != nil {
			t.Error(err)
			return
		}
		replacement = tx.Handle()
	}
	if _, err := hub.Fence(testFence("source:gr", "epoch:gr", 1, 3)); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("fence of a generation replaced while it was paused=%v", err)
	}
	if replacement == nil {
		t.Fatal("replacement did not commit")
	}
	if !hub.Readable("asset:gr") {
		t.Fatal("the accepted replacement was closed by a stale fence")
	}
	if info, _ := hub.Source("source:gr"); !info.Live || info.Generation != 2 {
		t.Fatalf("source after the stale fence=%+v", info)
	}
	mustPublish(t, hub, replacement, "asset:gr", "gr", 3, 4)
}

// Bot round-4 regressions.

func TestFailedControlJournalChangesNothingAndTheSameFenceSucceedsOnRetry(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub, "asset:cj", "source:cj", "driver-cj", nil)
	handle := admitLegacy(t, hub, "source:cj", "driver-cj", "epoch:cj", 1)
	mustPublish(t, hub, handle, "asset:cj", "cj", 1, 1)
	cursor := hub.Cursor()
	journal.failure = errors.New("store unavailable")
	if _, err := hub.Fence(testFence("source:cj", "epoch:cj", 1, 2)); err == nil {
		t.Fatal("fence succeeded without its journal record")
	}
	if info, _ := hub.Source("source:cj"); !info.Live || !hub.Readable("asset:cj") || hub.Cursor() != cursor {
		t.Fatalf("a fence whose journal append failed was exposed: %+v", info)
	}
	mustPublish(t, hub, handle, "asset:cj", "cj", 2, 2)
	journal.failure = nil
	if _, err := hub.Fence(testFence("source:cj", "epoch:cj", 1, 3)); err != nil {
		t.Fatalf("fence retry=%v", err)
	}
}

func TestAdmittedEvaluatesBothGatePredicatesOverOneGate(t *testing.T) {
	f := newControlFixture(t)
	installed := make(chan struct{})
	blocked := false
	f.hub.faults = func(stage string, id semreg.AssetID) {
		if stage != "admitted-read" {
			return
		}
		f.hub.faults = nil
		go func() {
			f.hub.mu.Lock()
			f.hub.assets[id].gate = &controlAsset{op: &controlOp{}, asset: f.hub.assets[id], semantic: SemanticUnbuilt, reprojection: ReprojectionSettled}
			f.hub.mu.Unlock()
			close(installed)
		}()
		select {
		case <-installed:
		case <-time.After(50 * time.Millisecond):
			blocked = true
		}
	}
	if !f.hub.Admitted("asset:c1") {
		t.Fatal("the gate read before the swap was not admitted")
	}
	<-installed
	if !blocked {
		t.Fatal("a gate swap interleaved the admission read")
	}
	if f.hub.Admitted("asset:c1") || f.hub.Readable("asset:c1") {
		t.Fatal("an unbuilt control gate admitted reads")
	}
}

func TestGenerationTransitionFencesEveryOtherAssetOfTheSource(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	reprojected := make(chan semreg.AssetID, 2)
	mustDeclare(t, hub, "asset:ma", "source:m2", "driver-m2", nil)
	mustDeclare(t, hub, "asset:mb", "source:m2", "driver-m2", func(event ControlEvent) error { reprojected <- event.Asset; return nil })
	handle := admitLegacy(t, hub, "source:m2", "driver-m2", "epoch:m2", 1)
	mustPublish(t, hub, handle, "asset:ma", "m2", 1, 1)
	mustPublish(t, hub, handle, "asset:mb", "m2", 1, 1)
	tx, err := hub.BeginGeneration(handle, "asset:ma", 2)
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "m2", 2, 2)
	batch.GenerationFences = []semreg.GenerationFence{{SourceID: "source:m2", SourceEpochID: "epoch:m2", DriverGeneration: "1", Reason: "lifecycle.driver_generation_replaced", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, Revision: "1"}}
	digest, _ := batch.ComputedDigest()
	batch.BatchDigest = digest
	if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(nil); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-reprojected:
		if id != "asset:mb" {
			t.Fatalf("reprojected %s", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the other asset of the source was not fenced")
	}
	waitFor(t, func() bool { return hub.Readable("asset:mb") })
	view, _ := hub.Inspect("asset:mb")
	if cursor, _ := cursorFor(view.Snapshot, "source:m2", "epoch:m2", "1"); !cursor.Fenced || view.Snapshot.Bindings[0].State != semreg.BindingFenced {
		t.Fatalf("other asset keeps an unfenced old generation: %+v", view.Snapshot.Cursors)
	}
	records := journal.Records()
	kinds := []EventKind{}
	for _, record := range records[1:] {
		kinds = append(kinds, record.Kind)
	}
	if len(kinds) != 2 || kinds[0] != EventGenerationFenced || kinds[1] != EventGenerationActivated || records[1].Generation != 1 || records[2].Generation != 2 {
		t.Fatalf("journal after the transition=%+v", records)
	}
	mustPublish(t, hub, tx.Handle(), "asset:mb", "m2", 3, 3)
}

// Round-5 review regressions.

// transitionTwoAssets commits a generation-2 replacement on asset:ma of a
// source that also owns asset:mb, with mb's cover processing held back by
// the returned release function.
func transitionTwoAssets(t *testing.T, hub *Hub, mbCallback func(ControlEvent) error) (*Tx, func()) {
	t.Helper()
	return transitionTwoAssetsWithHook(t, hub, mbCallback, nil)
}

// transitionTwoAssetsWithHook is transitionTwoAssets with an extra fault hook
// installed before any hub goroutine starts.
func transitionTwoAssetsWithHook(t *testing.T, hub *Hub, mbCallback func(ControlEvent) error, extra func(stage string, id semreg.AssetID)) (*Tx, func()) {
	t.Helper()
	mustDeclare(t, hub, "asset:ma", "source:m2", "driver-m2", nil)
	mustDeclare(t, hub, "asset:mb", "source:m2", "driver-m2", mbCallback)
	handle := admitLegacy(t, hub, "source:m2", "driver-m2", "epoch:m2", 1)
	mustPublish(t, hub, handle, "asset:ma", "m2", 1, 1)
	mustPublish(t, hub, handle, "asset:mb", "m2", 1, 1)
	release := make(chan struct{})
	hub.faults = func(stage string, id semreg.AssetID) {
		if extra != nil {
			extra(stage, id)
		}
		if stage == "before-submit" && id == "asset:mb" {
			<-release
		}
	}
	tx, err := hub.BeginGeneration(handle, "asset:ma", 2)
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "m2", 2, 2)
	batch.GenerationFences = []semreg.GenerationFence{{SourceID: "source:m2", SourceEpochID: "epoch:m2", DriverGeneration: "1", Reason: "lifecycle.driver_generation_replaced", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, Revision: "1"}}
	digest, _ := batch.ComputedDigest()
	batch.BatchDigest = digest
	if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
		t.Fatal(err)
	}
	committed := make(chan error, 1)
	go func() {
		_, err := tx.Commit(nil)
		committed <- err
	}()
	waitFor(t, func() bool {
		hub.mu.RLock()
		defer hub.mu.RUnlock()
		gate := hub.assets["asset:mb"].gate
		return gate != nil && gate.semantic != SemanticUnbuilt
	})
	return tx, func() {
		close(release)
		if err := <-committed; err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublicationDuringAPendingCoverIsRefusedAndTheFenceThenSucceeds(t *testing.T) {
	hub := newTestHub(t)
	tx, release := transitionTwoAssets(t, hub, nil)
	if _, err := publish(t, hub, tx.Handle(), "asset:mb", "m2", 3, 3); !errors.Is(err, ErrControlPending) {
		t.Fatalf("publication during a pending cover=%v", err)
	}
	release()
	waitFor(t, func() bool { return hub.Admitted("asset:mb") })
	view, _ := hub.Inspect("asset:mb")
	if cursor, _ := cursorFor(view.Snapshot, "source:m2", "epoch:m2", "1"); !cursor.Fenced {
		t.Fatal("the cover fence did not commit")
	}
	mustPublish(t, hub, tx.Handle(), "asset:mb", "m2", 3, 3)
}

func TestPublicationBeforeReprojectionAckIsRefusedAndSucceedsAfterIt(t *testing.T) {
	hub := newTestHub(t)
	ack := make(chan struct{})
	tx, release := transitionTwoAssets(t, hub, func(ControlEvent) error { <-ack; return nil })
	release()
	waitFor(t, func() bool {
		hub.mu.RLock()
		defer hub.mu.RUnlock()
		gate := hub.assets["asset:mb"].gate
		return semanticSuccess(gate.semantic) && gate.inFlight != nil
	})
	if _, err := publish(t, hub, tx.Handle(), "asset:mb", "m2", 3, 3); !errors.Is(err, ErrReprojectionPending) {
		t.Fatalf("publication before the reprojection ack=%v", err)
	}
	close(ack)
	waitFor(t, func() bool { return hub.Admitted("asset:mb") })
	mustPublish(t, hub, tx.Handle(), "asset:mb", "m2", 3, 3)
}

func TestFencePausedBeforeRevocationYieldsToATransitionCover(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:ma", "source:m2", "driver-m2", nil)
	mustDeclare(t, hub, "asset:mb", "source:m2", "driver-m2", nil)
	handle := admitLegacy(t, hub, "source:m2", "driver-m2", "epoch:m2", 1)
	mustPublish(t, hub, handle, "asset:ma", "m2", 1, 1)
	mustPublish(t, hub, handle, "asset:mb", "m2", 1, 1)
	var cover ControlID
	committed := make(chan error, 1)
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage != "control-revoke" {
			return
		}
		hub.faults = func(stage string, id semreg.AssetID) {
			if stage == "before-submit" && id == "asset:mb" {
				panic("injected interruption")
			}
		}
		tx, err := hub.BeginGeneration(handle, "asset:ma", 2)
		if err != nil {
			t.Error(err)
			return
		}
		batch := testBatch(t, tx, "m2", 2, 2)
		batch.GenerationFences = []semreg.GenerationFence{{SourceID: "source:m2", SourceEpochID: "epoch:m2", DriverGeneration: "1", Reason: "lifecycle.driver_generation_replaced", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, Revision: "1"}}
		digest, _ := batch.ComputedDigest()
		batch.BatchDigest = digest
		if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
			t.Error(err)
			return
		}
		// The cover's submission serializes on the source control mutex,
		// which this paused Fence holds: commit from another goroutine and
		// resume once the cover is installed.
		go func() {
			_, err := tx.Commit(nil)
			committed <- err
		}()
		waitFor(t, func() bool {
			hub.mu.RLock()
			defer hub.mu.RUnlock()
			last := hub.sources["source:m2"].last
			if last != nil {
				cover = last.id
			}
			return last != nil
		})
	}
	var pending *ControlPendingError
	if _, err := hub.Fence(testFence("source:m2", "epoch:m2", 1, 3)); !errors.As(err, &pending) || pending.ID != cover {
		t.Fatalf("fence racing a transition cover=%v (cover %+v)", err, cover)
	}
	// The interrupted cover is reported to the committing caller with its
	// ControlID; the publication itself is accepted.
	var coverErr *CoverError
	if err := <-committed; !errors.As(err, &coverErr) || coverErr.ID != cover {
		t.Fatalf("commit with an interrupted cover=%v", err)
	}
	hub.faults = nil
	if result, err := hub.RetryControl(cover); err != nil || result.Assets[0].Outcome != SemanticAccepted {
		t.Fatalf("cover retry=%+v err=%v", result.Assets, err)
	}
}

func TestProvisionalRetirementIsJournalledFirst(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	provisional, _ := hub.AdmitProvisional("pv_sunspec", LegacyPVSunSpec, "source-epoch:semantic-pv:j", 1)
	handle, err := provisional.Qualify("source:semantic-pv:j")
	if err != nil {
		t.Fatal(err)
	}
	cursor := hub.Cursor()
	journal.failure = errors.New("store unavailable")
	if err := provisional.Retire(); err == nil {
		t.Fatal("retirement succeeded without its journal record")
	}
	if info, _ := hub.Source("source:semantic-pv:j"); !info.Live || hub.Cursor() != cursor || hub.checkHandle(handle) != nil {
		t.Fatalf("a retirement whose journal append failed was exposed: %+v", info)
	}
	journal.failure = nil
	if err := provisional.Retire(); err != nil {
		t.Fatalf("retirement retry=%v", err)
	}
	if info, _ := hub.Source("source:semantic-pv:j"); info.Live {
		t.Fatal("retirement retry did not retire")
	}
}

func TestTransitionJournalNamesEveryFencedAsset(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	_, release := transitionTwoAssets(t, hub, nil)
	release()
	records := journal.Records()
	if len(records) != 3 {
		t.Fatalf("journal=%+v", records)
	}
	fence, activation := records[1], records[2]
	if fence.Kind != EventGenerationFenced || fence.Generation != 1 || len(fence.Assets) != 2 || fence.Assets[0] != "asset:ma" || fence.Assets[1] != "asset:mb" {
		t.Fatalf("fence record=%+v", fence)
	}
	if activation.Kind != EventGenerationActivated || activation.Generation != 2 {
		t.Fatalf("activation record=%+v", activation)
	}
}

// Round-6 review regressions.

func TestRetryDuringTheInitialCoverSubmissionDoesNotResubmit(t *testing.T) {
	hub := newTestHub(t)
	// The initial cover submission holds the source control mutex while it
	// is blocked; release it only once the retry has found that mutex held.
	contended := make(chan struct{})
	var once sync.Once
	tx, release := transitionTwoAssetsWithHook(t, hub, nil, func(stage string, id semreg.AssetID) {
		if stage == "source-control-contended" {
			once.Do(func() { close(contended) })
		}
	})
	hub.mu.RLock()
	cover := hub.sources["source:m2"].last.id
	hub.mu.RUnlock()
	retried := make(chan error, 1)
	go func() {
		_, err := hub.RetryControl(cover)
		retried <- err
	}()
	<-contended
	release()
	// The retry runs after the initial submission resolved: it either finds
	// the cover finished, or its reprojection still in flight (SettlePending);
	// it never submits the cover again.
	var settle *SettlePendingError
	if err := <-retried; err != nil && !errors.As(err, &settle) {
		t.Fatalf("retry interleaved with the initial cover submission=%v", err)
	}
	waitFor(t, func() bool { return hub.Admitted("asset:mb") })
	if attempts := hub.attempts(t, cover, "asset:mb"); len(attempts) != 1 {
		t.Fatalf("the cover was submitted %d times", len(attempts))
	}
	mustPublish(t, hub, tx.Handle(), "asset:mb", "m2", 3, 3)
}

func TestProvisionalRetirementNamesEveryAffectedAsset(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub, "pv-asset-b", "source:semantic-pv:r6", "pv_sunspec", nil)
	mustDeclare(t, hub, "pv-asset-a", "source:semantic-pv:r6", "pv_sunspec", nil)
	provisional, _ := hub.AdmitProvisional("pv_sunspec", LegacyPVSunSpec, "source-epoch:semantic-pv:r6", 1)
	handle, err := provisional.Qualify("source:semantic-pv:r6")
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, handle, "pv-asset-b", "pv", 1, 1)
	mustPublish(t, hub, handle, "pv-asset-a", "pv", 1, 1)
	if err := provisional.Retire(); err != nil {
		t.Fatal(err)
	}
	records := journal.Records()
	last := records[len(records)-1]
	if last.Kind != EventSourceRetired || len(last.Assets) != 2 || last.Assets[0] != "pv-asset-a" || last.Assets[1] != "pv-asset-b" {
		t.Fatalf("retirement record=%+v", last)
	}
}

func TestMemoryJournalDeepCopiesAssets(t *testing.T) {
	journal := NewMemoryJournal()
	assets := []semreg.AssetID{"asset:1", "asset:2"}
	if err := journal.Append(EpochRecord{Source: "source:j", Kind: EventGenerationFenced, Assets: assets}); err != nil {
		t.Fatal(err)
	}
	assets[0] = "mutated"
	records := journal.Records()
	records[0].Assets[1] = "mutated"
	again := journal.Records()
	if again[0].Assets[0] != "asset:1" || again[0].Assets[1] != "asset:2" {
		t.Fatalf("stored journal aliased caller slices: %+v", again[0].Assets)
	}
}

func TestHistoricalKeyControlIsJournalled(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub, "pv-asset-h", "source:semantic-pv:h", "pv_sunspec", nil)
	first, _ := hub.AdmitProvisional("pv_sunspec", LegacyPVSunSpec, "source-epoch:semantic-pv:h1", 1)
	handle, err := first.Qualify("source:semantic-pv:h")
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, handle, "pv-asset-h", "pv", 1, 1)
	if err := first.Retire(); err != nil {
		t.Fatal(err)
	}
	second, _ := hub.AdmitProvisional("pv_sunspec", LegacyPVSunSpec, "source-epoch:semantic-pv:h2", 1)
	if _, err := second.Qualify("source:semantic-pv:h"); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.RetireEpoch(RetireRequest{Source: "source:semantic-pv:h", Epoch: "source-epoch:semantic-pv:h1", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(2), Monotonic: testMono(2)}); err != nil {
		t.Fatalf("historical retirement=%v", err)
	}
	records := journal.Records()
	last := records[len(records)-1]
	if last.Kind != EventSourceRetired || last.Epoch != "source-epoch:semantic-pv:h1" || len(last.Assets) != 1 || last.Assets[0] != "pv-asset-h" {
		t.Fatalf("historical control record=%+v", last)
	}
}

// Bot threads, final round.

func TestActivateRefusesAnObsoleteGenerationHandle(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:ob", "source:ob", "driver-ob", nil)
	first := admitLegacy(t, hub, "source:ob", "driver-ob", "epoch:ob", 1)
	mustPublish(t, hub, first, "asset:ob", "ob", 1, 1)
	if _, err := hub.Fence(testFence("source:ob", "epoch:ob", 1, 2)); err != nil {
		t.Fatal(err)
	}
	second, err := hub.Activate(first, 2)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, second, "asset:ob", "ob", 2, 3)
	if _, err := hub.Fence(testFence("source:ob", "epoch:ob", 2, 4)); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Activate(first, 3); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("activation through an obsolete handle=%v", err)
	}
	if _, err := hub.Activate(second, 3); err != nil {
		t.Fatalf("activation through the replaced generation's handle=%v", err)
	}
}

func TestRetiringAnUnpublishedEpochEmitsASourceRetirement(t *testing.T) {
	for name, declare := range map[string]bool{"declared asset": true, "no asset": false} {
		t.Run(name, func(t *testing.T) {
			hub := newTestHub(t)
			if declare {
				mustDeclare(t, hub, "asset:up", "source:up", "driver-up", nil)
			}
			admitLegacy(t, hub, "source:up", "driver-up", "epoch:up", 4)
			cursor := hub.Cursor()
			if _, err := hub.RetireEpoch(RetireRequest{Source: "source:up", Epoch: "epoch:up", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(1), Monotonic: testMono(1)}); err != nil {
				t.Fatal(err)
			}
			events, _ := hub.Since(cursor)
			if len(events) != 1 || events[0].Kind != EventSourceRetired || events[0].Generation != 4 || events[0].Epoch != "epoch:up" || !events[0].NoPublication {
				t.Fatalf("events=%+v", events)
			}
		})
	}
}

// epochTransition stages the first publication of epoch:e2 on asset:ea of a
// source whose epoch:e1 published on asset:ea and asset:eb.
func epochTransition(t *testing.T, hub *Hub) (*Handle, *Tx, semreg.PublicationBatch) {
	t.Helper()
	mustDeclare(t, hub, "asset:ea", "source:e", "pv_sunspec", nil)
	mustDeclare(t, hub, "asset:eb", "source:e", "pv_sunspec", nil)
	provisional, _ := hub.AdmitProvisional("pv_sunspec", LegacyPVSunSpec, "epoch:e1", 1)
	old, err := provisional.Qualify("source:e")
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, old, "asset:ea", "e", 1, 1)
	mustPublish(t, hub, old, "asset:eb", "e", 1, 1)
	tx, err := hub.BeginEpoch(old, "asset:ea", "epoch:e2", 1)
	if err != nil {
		t.Fatal(err)
	}
	if retirements := tx.SourceRetirements(); len(retirements) != 1 || retirements[0] != "epoch:e1" {
		t.Fatalf("pending retirements=%v", retirements)
	}
	return old, tx, testBatch(t, tx, "e", 2, 2)
}

func TestEpochTransitionIsTransactional(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	old, tx, _ := epochTransition(t, hub)
	tx.Abort()
	if info, _ := hub.Source("source:e"); info.Epoch != "epoch:e1" || !info.Live || hub.checkHandle(old) != nil {
		t.Fatalf("an aborted epoch transition changed the source: %+v", info)
	}
	mustPublish(t, hub, old, "asset:ea", "e", 3, 3)
	tx, err = hub.BeginEpoch(old, "asset:ea", "epoch:e2", 1)
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "e", 4, 4)
	if _, _, err := tx.Apply(batch, testMono(4)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(nil); err != nil {
		t.Fatal(err)
	}
	if info, _ := hub.Source("source:e"); info.Epoch != "epoch:e2" || !info.Live {
		t.Fatalf("committed epoch transition=%+v", info)
	}
	if hub.checkHandle(old) == nil {
		t.Fatal("the old epoch handle stayed valid")
	}
	waitFor(t, func() bool { return hub.Admitted("asset:eb") })
	view, _ := hub.Inspect("asset:eb")
	for _, descriptor := range view.Snapshot.Sources {
		if descriptor.SourceEpochID == "epoch:e1" && descriptor.State != semreg.SourceRetired {
			t.Fatalf("other asset keeps the replaced epoch current: %+v", view.Snapshot.Sources)
		}
	}
	records := journal.Records()
	retired, admitted := records[len(records)-2], records[len(records)-1]
	if retired.Kind != EventSourceRetired || retired.Epoch != "epoch:e1" || len(retired.Assets) != 2 || admitted.Kind != EventEpochAdmitted || admitted.Epoch != "epoch:e2" {
		t.Fatalf("journal=%+v", records)
	}
	mustPublish(t, hub, tx.Handle(), "asset:eb", "e", 5, 5)
}

// Round-9 regressions.

func transitionGeneration(t *testing.T, hub *Hub, handle *Handle, id semreg.AssetID, at int64) *Handle {
	t.Helper()
	tx, err := hub.BeginGeneration(handle, id, handle.Generation()+1)
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "r9", at, at)
	for _, generation := range tx.GenerationFences() {
		batch.GenerationFences = append(batch.GenerationFences, semreg.GenerationFence{SourceID: handle.Source(), SourceEpochID: handle.Epoch(), DriverGeneration: generation, Reason: "lifecycle.driver_generation_replaced", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, Revision: "1"})
	}
	digest, _ := batch.ComputedDigest()
	batch.BatchDigest = digest
	if _, _, err := tx.Apply(batch, testMono(at)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(nil); err != nil {
		t.Fatal(err)
	}
	return tx.Handle()
}

func TestEpochCoverRetiresAnAssetWhoseOldGenerationIsAlreadyFenced(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:fa", "source:f", "pv_sunspec", nil)
	mustDeclare(t, hub, "asset:fb", "source:f", "pv_sunspec", nil)
	provisional, _ := hub.AdmitProvisional("pv_sunspec", LegacyPVSunSpec, "epoch:f1", 1)
	first, err := provisional.Qualify("source:f")
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, first, "asset:fa", "r9", 1, 1)
	mustPublish(t, hub, first, "asset:fb", "r9", 1, 1)
	second := transitionGeneration(t, hub, first, "asset:fa", 2)
	waitFor(t, func() bool { return hub.Admitted("asset:fb") })
	tx, err := hub.BeginEpoch(second, "asset:fa", "epoch:f2", 1)
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "r9", 3, 3)
	if _, _, err := tx.Apply(batch, testMono(3)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		hub.mu.RLock()
		defer hub.mu.RUnlock()
		gate := hub.assets["asset:fb"].gate
		return gate != nil && gate.semantic != SemanticUnbuilt && gate.semantic != SemanticPending
	})
	hub.mu.RLock()
	semantic, errID := hub.assets["asset:fb"].gate.semantic, hub.assets["asset:fb"].gate.errID
	hub.mu.RUnlock()
	if !semanticSuccess(semantic) {
		t.Fatalf("epoch cover on the already fenced asset=%s %s", semantic, errID)
	}
	waitFor(t, func() bool { return hub.Admitted("asset:fb") })
	mustPublish(t, hub, tx.Handle(), "asset:fb", "r9", 4, 4)
	view, _ := hub.Inspect("asset:fb")
	for _, descriptor := range view.Snapshot.Sources {
		if descriptor.SourceEpochID == "epoch:f1" && descriptor.State != semreg.SourceRetired {
			t.Fatalf("old epoch still current on asset:fb: %+v", view.Snapshot.Sources)
		}
	}
}

func TestUnpublishedRetirementEventCarriesTheGenerationAtRevocation(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:ga", "source:g9", "driver-g9", nil)
	mustDeclare(t, hub, "asset:gb", "source:g9", "driver-g9", nil)
	handle := admitLegacy(t, hub, "source:g9", "driver-g9", "epoch:g9", 1)
	mustPublish(t, hub, handle, "asset:ga", "r9", 1, 1)
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "control-revoke" {
			hub.faults = nil
			transitionGeneration(t, hub, handle, "asset:ga", 2)
		}
	}
	cursor := hub.Cursor()
	if _, err := hub.RetireEpoch(RetireRequest{Source: "source:g9", Epoch: "epoch:g9", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(3), Monotonic: testMono(3)}); err != nil {
		t.Fatal(err)
	}
	events, _ := hub.Since(cursor)
	found := false
	for _, event := range events {
		if event.Asset == "asset:gb" && event.Kind == EventSourceRetired {
			found = true
			if event.Generation != 2 {
				t.Fatalf("unpublished retirement event generation=%d want 2", event.Generation)
			}
		}
	}
	if !found {
		t.Fatalf("no retirement event for the unpublished asset: %+v", events)
	}
}

func TestAdmittedSourceKeepsItsIdentityRegime(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:r", "source:r", "driver-r", nil)
	native, err := hub.AdmitSource(Admission{Source: "source:r", Driver: "driver-r"})
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, native, "asset:r", "regime", 1, 1)
	if _, err := hub.Fence(testFence("source:r", native.Epoch(), 1, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.AdmitSource(Admission{Source: "source:r", Driver: "driver-r", Legacy: LegacyGrowattStorage, Epoch: "epoch:r2", Generation: 2}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("legacy admission of a hub-native source=%v", err)
	}
	if info, _ := hub.Source("source:r"); info.Legacy != "" || info.Epoch != native.Epoch() {
		t.Fatalf("refused admission changed the source: %+v", info)
	}

	// A failed first admission fixes no regime.
	journal := NewMemoryJournal()
	hub2, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub2, "asset:q", "source:q", "driver-q", nil)
	journal.failure = errors.New("store unavailable")
	if _, err := hub2.AdmitSource(Admission{Source: "source:q", Driver: "driver-q", Legacy: LegacyGrowattStorage, Epoch: "epoch:q", Generation: 1}); err == nil {
		t.Fatal("admission without its journal record")
	}
	journal.failure = nil
	if info, _ := hub2.Source("source:q"); info.Legacy != "" {
		t.Fatalf("failed admission fixed the regime: %+v", info)
	}
	if _, err := hub2.AdmitSource(Admission{Source: "source:q", Driver: "driver-q"}); err != nil {
		t.Fatalf("native admission after a failed legacy one=%v", err)
	}
}

func TestSuccessorCommitJournalsTheRetiredEpoch(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub, "asset:s", "source:s", "tesla_evse", nil)
	admit := func(epoch semreg.SourceEpochID, generation uint64) *Handle {
		handle, err := hub.AdmitSource(Admission{Source: "source:s", Driver: "tesla_evse", Legacy: LegacyTeslaEVSE, Epoch: epoch, Generation: generation})
		if err != nil {
			t.Fatal(err)
		}
		return handle
	}
	first := admit("epoch:s1", 7)
	mustPublish(t, hub, first, "asset:s", "successor", 1, 1)
	if _, err := hub.Fence(testFence("source:s", "epoch:s1", 7, 2)); err != nil {
		t.Fatal(err)
	}
	successor := admit("epoch:s2", 8)
	attempt := func() (Outcome, error) {
		tx, err := hub.Begin(successor, "asset:s")
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Abort()
		if _, _, err := tx.Apply(testBatch(t, tx, "successor", 2, 3), testMono(3)); err != nil {
			t.Fatal(err)
		}
		return tx.Commit(nil)
	}
	journal.failure = errors.New("store unavailable")
	before, _ := hub.Inspect("asset:s")
	if _, err := attempt(); err == nil {
		t.Fatal("retirement committed without its journal record")
	}
	if after, _ := hub.Inspect("asset:s"); after.Snapshot.SnapshotID != before.Snapshot.SnapshotID || hub.sources["source:s"].retired["epoch:s1"] {
		t.Fatal("a failed retirement journal append changed the hub")
	}
	journal.failure = nil
	if outcome, err := attempt(); err != nil || outcome != Accepted {
		t.Fatalf("successor commit=%v/%v", outcome, err)
	}
	found := false
	for _, record := range journal.Records() {
		if record.Kind == EventSourceRetired && record.Source == "source:s" && record.Epoch == "epoch:s1" && len(record.Assets) == 1 && record.Assets[0] == "asset:s" {
			found = true
		}
	}
	hub.mu.RLock()
	retired := hub.sources["source:s"].retired["epoch:s1"]
	hub.mu.RUnlock()
	if !found || !retired {
		t.Fatalf("journalled=%t retired=%t records=%+v", found, retired, journal.Records())
	}
}

// rebind replaces the binding identifier of a first publication everywhere it
// appears and recomputes the digest.
func rebind(t *testing.T, batch semreg.PublicationBatch, binding semreg.NativeBindingID) semreg.PublicationBatch {
	t.Helper()
	for index := range batch.BindingUpserts {
		batch.BindingUpserts[index].BindingID = binding
	}
	for index := range batch.IdentityLinkUpserts {
		batch.IdentityLinkUpserts[index].BindingID = binding
	}
	for index := range batch.FactUpserts {
		id := binding
		origin := binding
		batch.FactUpserts[index].BindingID = &id
		batch.FactUpserts[index].Origin.BindingID = &origin
	}
	for index := range batch.ServiceUpserts {
		batch.ServiceUpserts[index].BindingID = binding
	}
	for index := range batch.CapabilityUpserts {
		batch.CapabilityUpserts[index].BindingID = binding
	}
	digest, err := batch.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	batch.BatchDigest = digest
	return batch
}

func TestNestedPublicationIdentifiersAreClaimedAtCommit(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:a", "source:a", "driver-a", nil)
	mustDeclare(t, hub, "asset:b", "source:b", "driver-b", nil)
	handleA := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 1)
	handleB := admitLegacy(t, hub, "source:b", "driver-b", "epoch:b", 1)
	mustPublish(t, hub, handleA, "asset:a", "fam", 1, 1)
	viewA, _ := hub.Inspect("asset:a")
	claimedBinding := viewA.Snapshot.Bindings[0].BindingID
	for _, stolen := range []semreg.NativeBindingID{claimedBinding, "asset:a", "source:a"} {
		tx, err := hub.Begin(handleB, "asset:b")
		if err != nil {
			t.Fatal(err)
		}
		batch := rebind(t, testBatch(t, tx, "fam", 2, 2), stolen)
		if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
			tx.Abort()
			t.Fatalf("apply %s=%v", stolen, err)
		}
		cursor := hub.Cursor()
		var collision *CollisionError
		if _, err := tx.Commit(nil); !errors.As(err, &collision) {
			t.Fatalf("commit with another driver's identifier %s=%v", stolen, err)
		}
		if view, _ := hub.Inspect("asset:b"); view.Snapshot.SnapshotID != "" || hub.Cursor() != cursor {
			t.Fatalf("refused commit %s became visible", stolen)
		}
	}
	mustPublish(t, hub, handleB, "asset:b", "fam", 3, 3)
	viewB, _ := hub.Inspect("asset:b")
	hub.mu.RLock()
	owner := hub.ids[string(viewB.Snapshot.Bindings[0].BindingID)]
	service := hub.ids[string(viewB.Snapshot.Services[0].InstanceID)]
	capability := hub.ids[string(viewB.Snapshot.Capabilities[0].InstanceID)]
	hub.mu.RUnlock()
	if owner != (idOwner{driver: "driver-b", kind: KindBinding}) || service != (idOwner{driver: "driver-b", kind: KindService}) || capability != (idOwner{driver: "driver-b", kind: KindCapability}) {
		t.Fatalf("claims binding=%+v service=%+v capability=%+v", owner, service, capability)
	}
}

func TestLifecycleEventsAreEmittedInsideTheHubSerialization(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	outside := []EventKind{}
	seen := map[EventKind]bool{}
	// The emitting goroutine must itself hold the hub write lock: the hub
	// records the owner inside lock, so a lock held by another goroutine
	// (for example the drainer) does not count.
	hub.trackOwner = true
	hub.bus.onEmit = func(event Event) {
		if event.Kind == EventDeclareRefused {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		seen[event.Kind] = true
		if hub.owner.Load() != goroutineID() {
			outside = append(outside, event.Kind)
		}
	}
	mustDeclare(t, hub, "asset:e", "source:e", "driver-e", nil)
	handle := admitLegacy(t, hub, "source:e", "driver-e", "epoch:e", 1)
	mustPublish(t, hub, handle, "asset:e", "emit", 1, 1)
	if _, err := hub.Fence(testFence("source:e", "epoch:e", 1, 2)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return hub.Settled("asset:e") })
	next, err := hub.Activate(handle, 2)
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub, "asset:u", "source:u", "driver-e", nil)
	if _, err := hub.AdmitSource(Admission{Source: "source:u", Driver: "driver-e", Legacy: LegacyGrowattStorage, Epoch: "epoch:u", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.RetireEpoch(RetireRequest{Source: "source:u", Epoch: "epoch:u", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(3), Monotonic: testMono(3)}); err != nil {
		t.Fatal(err)
	}
	provisional, err := hub.AdmitProvisional("driver-p", LegacyPVSunSpec, "epoch:p", 1)
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub, "asset:p", "source:p", "driver-p", nil)
	if _, err := provisional.Qualify("source:p"); err != nil {
		t.Fatal(err)
	}
	if err := provisional.Retire(); err != nil {
		t.Fatal(err)
	}
	_ = next
	// The two control-failure emissions: a rejected submission and an
	// interrupted one.
	mustDeclare(t, hub, "asset:c", "source:c", "driver-c", nil)
	handleC := admitLegacy(t, hub, "source:c", "driver-c", "epoch:c", 1)
	mustPublish(t, hub, handleC, "asset:c", "c", 1, 1)
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "before-submit" && id == "asset:c" {
			hub.faults = nil
			intrudeOnKey(t, hub, id, "source:c", "epoch:c")
		}
	}
	if _, err := hub.Fence(testFence("source:c", "epoch:c", 1, 4)); errorID(err) != semreg.SequenceConflict {
		t.Fatalf("rejected fence=%v", err)
	}
	mustDeclare(t, hub, "asset:i", "source:i", "driver-i", nil)
	handleI := admitLegacy(t, hub, "source:i", "driver-i", "epoch:i", 1)
	mustPublish(t, hub, handleI, "asset:i", "i", 1, 1)
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "before-swap" && id == "asset:i" {
			hub.faults = nil
			panic("injected interruption")
		}
	}
	if _, err := hub.Fence(testFence("source:i", "epoch:i", 1, 5)); err == nil {
		t.Fatal("interrupted fence reported success")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, kind := range []EventKind{EventEpochAdmitted, EventGenerationActivated, EventSourceRetired, EventGenerationFenced, EventPublished, EventControlFailed} {
		if !seen[kind] {
			t.Fatalf("scenario did not exercise %s", kind)
		}
	}
	if len(outside) != 0 {
		t.Fatalf("events emitted outside the emitting goroutine's hub serialization: %v", outside)
	}
}

func TestCommitReadsTheRetirementsAsValidatedNotTheCallerBuffer(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub, "asset:s", "source:s", "tesla_evse", nil)
	admit := func(epoch semreg.SourceEpochID, generation uint64) *Handle {
		handle, err := hub.AdmitSource(Admission{Source: "source:s", Driver: "tesla_evse", Legacy: LegacyTeslaEVSE, Epoch: epoch, Generation: generation})
		if err != nil {
			t.Fatal(err)
		}
		return handle
	}
	first := admit("epoch:s1", 7)
	mustPublish(t, hub, first, "asset:s", "successor", 1, 1)
	if _, err := hub.Fence(testFence("source:s", "epoch:s1", 7, 2)); err != nil {
		t.Fatal(err)
	}
	successor := admit("epoch:s2", 8)
	tx, err := hub.Begin(successor, "asset:s")
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "successor", 2, 3)
	if _, _, err := tx.Apply(batch, testMono(3)); err != nil {
		tx.Abort()
		t.Fatal(err)
	}
	batch.SourceRetirements[0] = "epoch:s2"
	if outcome, err := tx.Commit(nil); err != nil || outcome != Accepted {
		t.Fatalf("commit=%v/%v", outcome, err)
	}
	for _, record := range journal.Records() {
		if record.Kind == EventSourceRetired && record.Epoch == "epoch:s2" {
			t.Fatalf("journal names the caller's mutated retirement: %+v", record)
		}
	}
	hub.mu.RLock()
	retiredS1, retiredS2 := hub.sources["source:s"].retired["epoch:s1"], hub.sources["source:s"].retired["epoch:s2"]
	hub.mu.RUnlock()
	if !retiredS1 || retiredS2 {
		t.Fatalf("retired s1=%t s2=%t", retiredS1, retiredS2)
	}
	mustPublish(t, hub, successor, "asset:s", "successor", 3, 4)
}

func TestCommitClaimsTheNestedIdentifiersAsValidatedNotTheCallerBuffer(t *testing.T) {
	for _, kind := range []Kind{KindBinding, KindService, KindCapability} {
		t.Run(string(kind), func(t *testing.T) {
			hub := newTestHub(t)
			mustDeclare(t, hub, "asset:a", "source:a", "driver-a", nil)
			mustDeclare(t, hub, "asset:b", "source:b", "driver-b", nil)
			handleA := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 1)
			handleB := admitLegacy(t, hub, "source:b", "driver-b", "epoch:b", 1)
			mustPublish(t, hub, handleA, "asset:a", "fam", 1, 1)
			viewA, _ := hub.Inspect("asset:a")
			tx, err := hub.Begin(handleB, "asset:b")
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Abort()
			batch := testBatch(t, tx, "fam", 2, 2)
			switch kind {
			case KindBinding:
				batch = rebind(t, batch, viewA.Snapshot.Bindings[0].BindingID)
			case KindService:
				stolen := viewA.Snapshot.Services[0].InstanceID
				batch.ServiceUpserts[0].InstanceID = stolen
				batch.CapabilityUpserts[0].ServiceInstance = stolen
			case KindCapability:
				batch.CapabilityUpserts[0].InstanceID = viewA.Snapshot.Capabilities[0].InstanceID
			}
			digest, err := batch.ComputedDigest()
			if err != nil {
				t.Fatal(err)
			}
			batch.BatchDigest = digest
			if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
				t.Fatalf("apply=%v", err)
			}
			switch kind {
			case KindBinding:
				batch.BindingUpserts[0].BindingID = "binding:fam:unclaimed"
			case KindService:
				batch.ServiceUpserts[0].InstanceID = "service:fam:unclaimed"
			case KindCapability:
				batch.CapabilityUpserts[0].InstanceID = "capability:fam:unclaimed"
			}
			var collision *CollisionError
			if _, err := tx.Commit(nil); !errors.As(err, &collision) {
				t.Fatalf("commit after the caller rewrote the stolen %s=%v", kind, err)
			}
			if view, _ := hub.Inspect("asset:b"); view.Snapshot.SnapshotID != "" {
				t.Fatal("the stolen identifier became visible")
			}
		})
	}
}

func TestApplyValidatesTheBatchExactlyAsTyped(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:v", "source:v", "driver-v", nil)
	handle := admitLegacy(t, hub, "source:v", "driver-v", "epoch:v", 1)
	tx, err := hub.Begin(handle, "asset:v")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	batch := testBatch(t, tx, "fam", 1, 1)
	batch.FactUpserts[0].Value.Symbols = []semreg.Symbol{}
	if _, _, err := tx.Apply(batch, testMono(1)); errorID(err) != semreg.InvalidValue {
		t.Fatalf("quantity plus an empty symbol payload=%v, want %s", err, semreg.InvalidValue)
	}
}

func TestCloneBatchPreservesNilVersusEmptyAndStringBytes(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:n", "source:n", "driver-n", nil)
	handle := admitLegacy(t, hub, "source:n", "driver-n", "epoch:n", 1)
	tx, err := hub.Begin(handle, "asset:n")
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	batch := testBatch(t, tx, "fam", 1, 1)
	batch.FactUpserts[0].Value.Symbols = []semreg.Symbol{}
	batch.FactUpserts[0].Quality.Reasons = nil
	batch.CapabilityUpserts[0].Constraints = nil
	batch.CapabilityUpserts[0].ActivationEvidence = []semreg.EvidenceRef{}
	batch.IdentityLinkUpserts[0].Basis = nil
	batch.FactWithdrawals = nil
	batch.GenerationFences = []semreg.GenerationFence{{SourceID: "source:n", Evidence: []semreg.EvidenceRef{}}}
	batch.SourceRetirements = []semreg.SourceEpochID{"bad\xffbytes"}
	out, err := cloneBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name   string
		nilIn  bool
		nilOut bool
		lenIn  int
		lenOut int
	}{
		{"symbols", batch.FactUpserts[0].Value.Symbols == nil, out.FactUpserts[0].Value.Symbols == nil, len(batch.FactUpserts[0].Value.Symbols), len(out.FactUpserts[0].Value.Symbols)},
		{"reasons", batch.FactUpserts[0].Quality.Reasons == nil, out.FactUpserts[0].Quality.Reasons == nil, 0, 0},
		{"constraints", batch.CapabilityUpserts[0].Constraints == nil, out.CapabilityUpserts[0].Constraints == nil, 0, 0},
		{"activation evidence", batch.CapabilityUpserts[0].ActivationEvidence == nil, out.CapabilityUpserts[0].ActivationEvidence == nil, 0, 0},
		{"link basis", batch.IdentityLinkUpserts[0].Basis == nil, out.IdentityLinkUpserts[0].Basis == nil, 0, 0},
		{"fact withdrawals", batch.FactWithdrawals == nil, out.FactWithdrawals == nil, 0, 0},
		{"service withdrawals", batch.ServiceWithdrawals == nil, out.ServiceWithdrawals == nil, 0, 0},
		{"fence evidence", batch.GenerationFences[0].Evidence == nil, out.GenerationFences[0].Evidence == nil, 0, 0},
		{"binding upserts", batch.BindingUpserts == nil, out.BindingUpserts == nil, len(batch.BindingUpserts), len(out.BindingUpserts)},
		{"service upserts", batch.ServiceUpserts == nil, out.ServiceUpserts == nil, len(batch.ServiceUpserts), len(out.ServiceUpserts)},
		{"source upserts", batch.SourceUpserts == nil, out.SourceUpserts == nil, len(batch.SourceUpserts), len(out.SourceUpserts)},
	}
	for _, check := range checks {
		if check.nilIn != check.nilOut || check.lenIn != check.lenOut {
			t.Errorf("%s: nil %t->%t len %d->%d", check.name, check.nilIn, check.nilOut, check.lenIn, check.lenOut)
		}
	}
	if out.SourceRetirements[0] != batch.SourceRetirements[0] {
		t.Errorf("string bytes changed: %q -> %q", batch.SourceRetirements[0], out.SourceRetirements[0])
	}
	// The copy shares nothing reachable with the original.
	out.FactUpserts[0].Value.Quantity.Unit = "unit.changed"
	*out.FactUpserts[0].BindingID = "binding:changed"
	out.GenerationFences[0].SourceID = "source:changed"
	if batch.FactUpserts[0].Value.Quantity.Unit == "unit.changed" || *batch.FactUpserts[0].BindingID == "binding:changed" || batch.GenerationFences[0].SourceID == "source:changed" {
		t.Error("the copy aliases the original")
	}
}

// commitTransitionWithFailingCover commits a generation transition on asset:fa
// whose cover over asset:fb fails as fail directs.
func commitTransitionWithFailingCover(t *testing.T, hub *Hub, fail func(stage string, id semreg.AssetID)) (*Tx, Outcome, error) {
	t.Helper()
	mustDeclare(t, hub, "asset:fa", "source:f", "driver-f", nil)
	mustDeclare(t, hub, "asset:fb", "source:f", "driver-f", nil)
	handle := admitLegacy(t, hub, "source:f", "driver-f", "epoch:f", 1)
	mustPublish(t, hub, handle, "asset:fa", "f", 1, 1)
	mustPublish(t, hub, handle, "asset:fb", "f", 1, 1)
	tx, err := hub.BeginGeneration(handle, "asset:fa", 2)
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "f", 2, 2)
	batch.GenerationFences = []semreg.GenerationFence{{SourceID: "source:f", SourceEpochID: "epoch:f", DriverGeneration: "1", Reason: "lifecycle.driver_generation_replaced", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, Revision: "1"}}
	digest, _ := batch.ComputedDigest()
	batch.BatchDigest = digest
	if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
		t.Fatal(err)
	}
	hub.faults = fail
	outcome, err := tx.Commit(nil)
	return tx, outcome, err
}

func TestCommitReportsAnInterruptedTransitionCoverAndRetryRecoversIt(t *testing.T) {
	hub := newTestHub(t)
	tx, outcome, err := commitTransitionWithFailingCover(t, hub, func(stage string, id semreg.AssetID) {
		if stage == "before-swap" && id == "asset:fb" {
			hub.faults = nil
			panic("injected interruption")
		}
	})
	var cover *CoverError
	if outcome != Accepted || !errors.As(err, &cover) {
		t.Fatalf("commit=%v/%v, want Accepted with a CoverError", outcome, err)
	}
	if view, _ := hub.Inspect("asset:fa"); view.Snapshot.SnapshotID == "" {
		t.Fatal("the accepted publication is not current")
	}
	if hub.Admitted("asset:fb") {
		t.Fatal("the asset behind the failed cover is readable")
	}
	if _, err := hub.RetryControl(cover.ID); err != nil {
		t.Fatalf("retry of the reported cover=%v", err)
	}
	waitFor(t, func() bool { return hub.Admitted("asset:fb") })
	view, _ := hub.Inspect("asset:fb")
	if cursor, _ := cursorFor(view.Snapshot, "source:f", "epoch:f", "1"); !cursor.Fenced {
		t.Fatal("the retried cover did not fence the replaced generation")
	}
	mustPublish(t, hub, tx.Handle(), "asset:fb", "f", 3, 3)
}

func TestCommitReportsARejectedTransitionCover(t *testing.T) {
	hub := newTestHub(t)
	_, outcome, err := commitTransitionWithFailingCover(t, hub, func(stage string, id semreg.AssetID) {
		if stage == "before-submit" && id == "asset:fb" {
			hub.faults = nil
			intrudeOnKey(t, hub, id, "source:f", "epoch:f")
		}
	})
	var cover *CoverError
	if outcome != Accepted || !errors.As(err, &cover) || errorID(err) != semreg.SequenceConflict {
		t.Fatalf("commit=%v/%v, want Accepted with a CoverError carrying %s", outcome, err, semreg.SequenceConflict)
	}
	if result, _ := hub.RetryControl(cover.ID); len(result.Assets) != 1 || result.Assets[0].Asset != "asset:fb" || result.Assets[0].Outcome != SemanticRejected {
		t.Fatalf("cover status=%+v", result.Assets)
	}
}

func TestSuccessorRetirementCarriesTheRetiredGeneration(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub, "asset:s", "source:s", "tesla_evse", nil)
	first, err := hub.AdmitSource(Admission{Source: "source:s", Driver: "tesla_evse", Legacy: LegacyTeslaEVSE, Epoch: "epoch:s1", Generation: 7})
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, first, "asset:s", "successor", 1, 1)
	if _, err := hub.Fence(testFence("source:s", "epoch:s1", 7, 2)); err != nil {
		t.Fatal(err)
	}
	successor, err := hub.AdmitSource(Admission{Source: "source:s", Driver: "tesla_evse", Legacy: LegacyTeslaEVSE, Epoch: "epoch:s2", Generation: 8})
	if err != nil {
		t.Fatal(err)
	}
	cursor := hub.Cursor()
	mustPublish(t, hub, successor, "asset:s", "successor", 2, 3)
	recorded := false
	for _, record := range journal.Records() {
		if record.Kind == EventSourceRetired && record.Epoch == "epoch:s1" {
			recorded = true
			if record.Generation != 7 {
				t.Errorf("journal record generation=%d, want 7", record.Generation)
			}
		}
	}
	events, err := hub.Since(cursor)
	if err != nil {
		t.Fatal(err)
	}
	emitted := false
	for _, event := range events {
		if event.Kind == EventSourceRetired && event.Epoch == "epoch:s1" {
			emitted = true
			if event.Generation != 7 {
				t.Errorf("event generation=%d, want 7", event.Generation)
			}
		}
	}
	if !recorded || !emitted {
		t.Fatalf("recorded=%t emitted=%t", recorded, emitted)
	}
}

func TestHistoricalEpochRetirementCarriesTheRetiredGeneration(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub, "asset:h", "source:h", "driver-pv", nil)
	provisional, err := hub.AdmitProvisional("driver-pv", LegacyPVSunSpec, "epoch:h", 4)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := provisional.Qualify("source:h")
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, handle, "asset:h", "h", 1, 1)
	// The hub-level retirement leaves the asset's cursor of epoch:h unfenced;
	// the later RetireEpoch retires that historical key on the asset.
	if err := provisional.Retire(); err != nil {
		t.Fatal(err)
	}
	cursor := hub.Cursor()
	if _, err := hub.RetireEpoch(RetireRequest{Source: "source:h", Epoch: "epoch:h", Reason: "lifecycle.retired", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(4), Monotonic: testMono(4)}); err != nil {
		t.Fatalf("historical retirement=%v", err)
	}
	records := journal.Records()
	last := records[len(records)-1]
	if last.Kind != EventSourceRetired || last.Epoch != "epoch:h" || last.Generation != 4 {
		t.Errorf("historical retirement record=%+v, want generation 4", last)
	}
	events, err := hub.Since(cursor)
	if err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, event := range events {
		if event.Kind == EventSourceRetired {
			seen = true
			if event.Generation != 4 {
				t.Errorf("historical retirement event=%+v, want generation 4", event)
			}
		}
	}
	if !seen {
		t.Fatal("no retirement event")
	}
}

func TestReserveClaimsDerivedIdentifiersUnderTheDeclarationRules(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:a", "source:a", "driver-a", nil)
	var collision *CollisionError
	if err := hub.Reserve("driver-b", KindBinding, "asset:a"); !errors.As(err, &collision) {
		t.Fatalf("reserving another driver's asset identifier=%v", err)
	}
	if err := hub.Reserve("driver-a", KindService, "source:a"); !errors.As(err, &collision) {
		t.Fatalf("reserving an identifier under another kind=%v", err)
	}
	if err := hub.Reserve("driver-a", KindAsset, "binding:x"); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("reserving a non-nested kind=%v", err)
	}
	for i := 0; i < 2; i++ {
		if err := hub.Reserve("driver-a", KindBinding, "binding:a"); err != nil {
			t.Fatalf("idempotent reservation=%v", err)
		}
	}
	if _, err := hub.Declare(AssetSpec{AssetID: "binding:a", SourceID: "source:b", Driver: "driver-b", Validators: []semreg.PackValidator{newTestValidator()}}); !errors.As(err, &collision) {
		t.Fatalf("declaring a reserved identifier as an asset=%v", err)
	}
	handle := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 1)
	mustPublish(t, hub, handle, "asset:a", "fam", 1, 1)
}

func TestWithdrawRollsBackAnUnpublishedAdmissionJournalFirst(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, hub, "asset:w", "source:w", "growatt_storage", nil)
	admit := func() (*Handle, error) {
		return hub.AdmitSource(Admission{Source: "source:w", Driver: "growatt_storage", Legacy: LegacyGrowattStorage, Epoch: "epoch:w", Generation: 1})
	}
	handle, err := admit()
	if err != nil {
		t.Fatal(err)
	}
	journal.failure = errors.New("store unavailable")
	if err := hub.Withdraw(handle); err == nil {
		t.Fatal("withdrawal without its journal record")
	}
	if info, _ := hub.Source("source:w"); !info.Live {
		t.Fatalf("a failed withdrawal changed the source: %+v", info)
	}
	journal.failure = nil
	if err := hub.Withdraw(handle); err != nil {
		t.Fatalf("withdrawal=%v", err)
	}
	if info, _ := hub.Source("source:w"); info.Live || info.Epoch != "" || info.Legacy != "" {
		t.Fatalf("withdrawn source=%+v", info)
	}
	if _, err := publish(t, hub, handle, "asset:w", "w", 1, 1); errorID(err) != semreg.StaleSourceEpoch {
		t.Fatalf("publication with a withdrawn handle=%v", err)
	}
	retry, err := admit()
	if err != nil {
		t.Fatalf("retry with the same admission=%v", err)
	}
	if !retry.Incarnation("asset:w").First {
		t.Fatal("the retried admission lost its first-incarnation identity")
	}
	kinds := []EventKind{}
	for _, record := range journal.Records() {
		kinds = append(kinds, record.Kind)
	}
	if len(kinds) != 3 || kinds[0] != EventEpochAdmitted || kinds[1] != EventSourceRetired || kinds[2] != EventEpochAdmitted {
		t.Fatalf("journal=%v, want admitted, retired, admitted", kinds)
	}
	mustPublish(t, hub, retry, "asset:w", "w", 1, 1)
	if err := hub.Withdraw(retry); !errors.Is(err, ErrAdmission) {
		t.Fatalf("withdrawal after a publication=%v", err)
	}
}

func TestWithdrawnHandleStaysRevokedAfterAnIdenticalReadmission(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:i", "source:i", "growatt_storage", nil)
	admit := func() *Handle {
		handle, err := hub.AdmitSource(Admission{Source: "source:i", Driver: "growatt_storage", Legacy: LegacyGrowattStorage, Epoch: "epoch:i", Generation: 1})
		if err != nil {
			t.Fatal(err)
		}
		return handle
	}
	withdrawn := admit()
	if err := hub.Withdraw(withdrawn); err != nil {
		t.Fatal(err)
	}
	current := admit()
	if _, err := publish(t, hub, withdrawn, "asset:i", "i", 1, 1); err == nil {
		t.Fatal("the withdrawn handle published after an identical readmission")
	}
	if _, err := hub.BeginGeneration(withdrawn, "asset:i", 2); err == nil {
		t.Fatal("the withdrawn handle began a generation transition")
	}
	if _, err := hub.Fence(testFence("source:i", "epoch:i", 1, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Activate(withdrawn, 2); errorID(err) != semreg.StaleSourceEpoch {
		t.Fatalf("activation through the withdrawn handle=%v, want %s", err, semreg.StaleSourceEpoch)
	}
	next, err := hub.Activate(current, 2)
	if err != nil {
		t.Fatalf("activation through the current handle=%v", err)
	}
	mustPublish(t, hub, next, "asset:i", "i", 2, 3)
}

func TestFailedFirstAdmissionLeavesNoSourceAndNoClaim(t *testing.T) {
	journal := NewMemoryJournal()
	hub, err := New(Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	journal.failure = errors.New("store unavailable")
	if _, err := hub.AdmitSource(Admission{Source: "source:phantom", Driver: "driver-a", Legacy: LegacyGrowattStorage, Epoch: "epoch:p", Generation: 1}); err == nil {
		t.Fatal("admission without its journal record")
	}
	journal.failure = nil
	if sources := hub.Sources(); len(sources) != 0 {
		t.Fatalf("phantom sources=%+v", sources)
	}
	hub.mu.RLock()
	_, claimed := hub.ids["source:phantom"]
	hub.mu.RUnlock()
	if claimed {
		t.Fatal("the failed admission kept its identifier claim")
	}
	mustDeclare(t, hub, "asset:b", "source:phantom", "driver-b", nil)
	if _, err := hub.AdmitSource(Admission{Source: "source:phantom", Driver: "driver-b", Legacy: LegacyTeslaEVSE, Epoch: "epoch:b", Generation: 1}); err != nil {
		t.Fatalf("another driver's admission after the failed one=%v", err)
	}
}
