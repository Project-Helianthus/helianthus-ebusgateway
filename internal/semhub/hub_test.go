package semhub

import (
	"errors"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func admitLegacy(t *testing.T, hub *Hub, src semreg.SourceID, driver string, epoch semreg.SourceEpochID, generation uint64) *Handle {
	t.Helper()
	handle, err := hub.AdmitSource(Admission{Source: src, Driver: driver, Legacy: LegacyGrowattStorage, Epoch: epoch, Generation: generation})
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func TestTransactionPublishesFromTheAssetCursor(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:a", "source:a", "driver-a", nil)
	handle := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 3)
	for index, want := range []semreg.Uint64{"1", "2", "3"} {
		next, err := hub.NextSequence("asset:a", "source:a", "epoch:a", 3)
		if err != nil || next != want {
			t.Fatalf("next sequence %d=%q/%v want %q", index, next, err, want)
		}
		mustPublish(t, hub, handle, "asset:a", "fam", int64(index+1), int64(index+1))
	}
	view, err := hub.Inspect("asset:a")
	if err != nil || len(view.Snapshot.Cursors) != 1 || view.Snapshot.Cursors[0].LastSequence != "3" || view.HubSeq == 0 {
		t.Fatalf("inspect=%+v err=%v", view.Snapshot.Cursors, err)
	}
}

func TestRejectedBatchChangesNothing(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:a", "source:a", "driver-a", nil)
	handle := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 1)
	mustPublish(t, hub, handle, "asset:a", "fam", 1, 1)
	before, _ := hub.Inspect("asset:a")
	cursor := hub.Cursor()
	tx, err := hub.Begin(handle, "asset:a")
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "fam", 2, 2)
	batch.ExpectedSemanticRevision = "99"
	digest, _ := batch.ComputedDigest()
	batch.BatchDigest = digest
	if _, _, err := tx.Apply(batch, testMono(2)); errorID(err) != semreg.RevisionConflict {
		t.Fatalf("apply error=%v", err)
	}
	if _, err := tx.Commit(nil); err == nil {
		t.Fatal("rejected batch committed")
	}
	after, _ := hub.Inspect("asset:a")
	if after.Snapshot.SnapshotID != before.Snapshot.SnapshotID || after.Snapshot.Cursors[0] != before.Snapshot.Cursors[0] || hub.Cursor() != cursor {
		t.Fatal("rejected batch changed snapshot, cursor or events")
	}
}

func TestReplayHasNoShortcutAndPassesSemRegErrorsThrough(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:a", "source:a", "driver-a", nil)
	handle := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 1)
	tx, err := hub.Begin(handle, "asset:a")
	if err != nil {
		t.Fatal(err)
	}
	first := testBatch(t, tx, "fam", 1, 1)
	if _, _, err := tx.Apply(first, testMono(1)); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if outcome, err := tx.Commit(func(Committed) { calls++ }); err != nil || outcome != Accepted || calls != 1 {
		t.Fatalf("first commit=%v/%v calls=%d", outcome, err, calls)
	}
	cursor := hub.Cursor()
	// (1) The same source retries its own accepted batch unchanged.
	tx, _ = hub.Begin(handle, "asset:a")
	if _, _, err := tx.Apply(first, testMono(1)); err != nil {
		t.Fatalf("replay apply=%v", err)
	}
	if outcome, err := tx.Commit(func(Committed) { calls++ }); err != nil || outcome != Replayed || calls != 1 || hub.Cursor() != cursor {
		t.Fatalf("replay commit=%v/%v calls=%d", outcome, err, calls)
	}
	// (3) Same digest with edited content keeps SemReg's own error.
	edited := first
	edited.ObservedAt = testWall(5)
	tx, _ = hub.Begin(handle, "asset:a")
	if _, _, err := tx.Apply(edited, testMono(1)); errorID(err) != semreg.SequenceConflict {
		t.Fatalf("edited replay error=%v", err)
	}
	tx.Abort()
	// (2) A later sequence is accepted, then the older one is retried.
	mustPublish(t, hub, handle, "asset:a", "fam", 2, 2)
	tx, _ = hub.Begin(handle, "asset:a")
	if _, _, err := tx.Apply(first, testMono(1)); errorID(err) != semreg.SequenceConflict {
		t.Fatalf("older replay error=%v", err)
	}
	tx.Abort()
}

func TestStaleHandleIsRefusedAtBeginAndAtCommit(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:a", "source:a", "driver-a", nil)
	handle := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 1)
	mustPublish(t, hub, handle, "asset:a", "fam", 1, 1)
	tx, err := hub.Begin(handle, "asset:a")
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "fam", 2, 2)
	if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
		t.Fatal(err)
	}
	before := hub.Cursor()
	done := make(chan error, 1)
	go func() {
		_, err := hub.Fence(testFence("source:a", "epoch:a", 1, 3))
		done <- err
	}()
	// The fence revokes handles before it waits for the asset lock.
	waitFor(t, func() bool { info, _ := hub.Source("source:a"); return !info.Live })
	if _, err := tx.Commit(nil); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("commit after fence=%v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	events, _ := hub.Since(before)
	for _, event := range events {
		if event.Kind == EventPublished {
			t.Fatalf("aborted commit emitted %+v", event)
		}
	}
	view, _ := hub.Inspect("asset:a")
	if cursor, _ := cursorFor(view.Snapshot, "source:a", "epoch:a", "1"); cursor.LastSequence != "2" || !cursor.Fenced {
		t.Fatalf("cursor=%+v: the aborted batch consumed a sequence", cursor)
	}
	if _, err := hub.Begin(handle, "asset:a"); errorID(err) != semreg.StaleDriverGeneration {
		t.Fatalf("begin with fenced handle=%v", err)
	}
}

func TestRestartSimulationFencesTheRetainedHandle(t *testing.T) {
	declare := func(hub *Hub) *Handle {
		mustDeclare(t, hub, "asset:restart", "source:restart", "driver-r", nil)
		handle, err := hub.AdmitSource(Admission{Source: "source:restart", Driver: "driver-r"})
		if err != nil {
			t.Fatal(err)
		}
		return handle
	}
	hubA, hubB := newTestHub(t), newTestHub(t)
	handleA := declare(hubA)
	for at := int64(1); at <= 3; at++ {
		mustPublish(t, hubA, handleA, "asset:restart", "restart", at, at)
	}
	handleB := declare(hubB)
	viewA, _ := hubA.Inspect("asset:restart")
	mustPublish(t, hubB, handleB, "asset:restart", "restart", 1, 1)
	viewB, _ := hubB.Inspect("asset:restart")
	if viewA.Snapshot.AssetID != viewB.Snapshot.AssetID || viewA.Snapshot.Sources[0].SourceID != viewB.Snapshot.Sources[0].SourceID {
		t.Fatal("asset or source identity changed across incarnations")
	}
	if viewA.Snapshot.Bindings[0].BindingID == viewB.Snapshot.Bindings[0].BindingID || handleA.Epoch() == handleB.Epoch() {
		t.Fatal("binding or epoch identity was reused across incarnations")
	}
	before, _ := hubB.Inspect("asset:restart")
	if _, err := hubB.Begin(handleA, "asset:restart"); errorID(err) != semreg.StaleSourceEpoch {
		t.Fatalf("retained handle error=%v", err)
	}
	after, _ := hubB.Inspect("asset:restart")
	if before.Snapshot.SnapshotID != after.Snapshot.SnapshotID {
		t.Fatal("retained handle changed the new incarnation's kernel")
	}
}

func TestBeginRefusesUnknownAndForeignAssets(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:a", "source:a", "driver-a", nil)
	mustDeclare(t, hub, "asset:b", "source:b", "driver-b", nil)
	handle := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 1)
	if _, err := hub.Begin(handle, "asset:none"); !errors.Is(err, ErrUnknownAsset) {
		t.Fatalf("unknown asset=%v", err)
	}
	if _, err := hub.Begin(handle, "asset:b"); !errors.Is(err, ErrTransaction) {
		t.Fatalf("foreign asset=%v", err)
	}
}
