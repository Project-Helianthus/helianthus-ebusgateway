package semhub

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

var recoveryAssets = []semreg.AssetID{"asset:restore-a", "asset:restore-b"}

func recoveryHub(t *testing.T, hooks *RecoveryHooks, callback func(ControlEvent) error) *Hub {
	t.Helper()
	if hooks == nil {
		hooks = &RecoveryHooks{}
	}
	hub, err := New(Options{Recovery: hooks})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range recoveryAssets {
		mustDeclare(t, hub, id, "source:restore", "synthetic-family", callback)
	}
	return hub
}

func populatedRecoveryHub(t *testing.T, hooks *RecoveryHooks) (*Hub, *Handle) {
	t.Helper()
	hub := recoveryHub(t, hooks, func(ControlEvent) error { return nil })
	handle := admitLegacy(t, hub, "source:restore", "synthetic-family", "epoch:restore", 1)
	for _, id := range recoveryAssets {
		mustPublish(t, hub, handle, id, "synthetic", 10, 1)
		mustPublish(t, hub, handle, id, "synthetic", 20, 2)
	}
	return hub, handle
}

func fenceRecovery(t *testing.T, hub *Hub, handle *Handle) error {
	t.Helper()
	_, err := hub.Fence(FenceRequest{Source: handle.Source(), Epoch: handle.Epoch(), Generation: handle.Generation(),
		Reason: "semhubtest.stop", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, ObservedAt: testWall(3), Monotonic: testMono(3)})
	return err
}

func TestRecoveryExactTwoAssetRoundTrip(t *testing.T) {
	for _, kind := range []ControlKind{ControlFence, ControlRetire} {
		t.Run(string(kind), func(t *testing.T) {
			hub, handle := populatedRecoveryHub(t, nil)
			if kind == ControlFence {
				if err := fenceRecovery(t, hub, handle); err != nil {
					t.Fatal(err)
				}
			} else {
				_, err := hub.RetireEpoch(RetireRequest{Source: handle.Source(), Epoch: handle.Epoch(), Reason: "semhubtest.stop",
					Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.retire")}, ObservedAt: testWall(3), Monotonic: testMono(3)})
				if err != nil {
					t.Fatal(err)
				}
			}
			record := hub.ExportRecovery()
			encoded, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			var persisted RecoveryRecord
			if err := json.Unmarshal(encoded, &persisted); err != nil {
				t.Fatal(err)
			}
			var fresh *Hub
			seen := 0
			fresh = recoveryHub(t, nil, func(event ControlEvent) error {
				seen++
				if fresh.Readable(event.Asset) {
					t.Error("read open inside fresh reprojection")
				}
				want, _ := hub.Inspect(event.Asset)
				if !bytes.Equal(event.View.Canonical, want.Canonical) {
					t.Error("wrong reprojection snapshot")
				}
				return nil
			})
			if err := fresh.Restore(persisted); err != nil {
				t.Fatal(err)
			}
			for _, id := range recoveryAssets {
				want, _ := hub.Inspect(id)
				got, _ := fresh.Inspect(id)
				if !reflect.DeepEqual(want.Snapshot, got.Snapshot) || !bytes.Equal(want.Canonical, got.Canonical) || want.Revisions != got.Revisions {
					t.Fatal("snapshot, history, tombstones, revisions or cursors changed")
				}
				fresh.mu.RLock()
				kernel := fresh.assets[id].kernel
				fresh.mu.RUnlock()
				fork, err := kernel.Fork()
				if err != nil {
					t.Fatal(err)
				}
				var last AcceptedBatch
				for _, asset := range persisted.Assets {
					if asset.Asset == id {
						last = asset.Accepted[len(asset.Accepted)-1]
					}
				}
				var batch semreg.PublicationBatch
				if err := json.Unmarshal(last.Batch, &batch); err != nil {
					t.Fatal(err)
				}
				replay, replayBytes, err := fork.Apply(batch, last.Monotonic)
				if err != nil || replay.SnapshotID != got.Snapshot.SnapshotID || !bytes.Equal(replayBytes, got.Canonical) {
					t.Fatalf("kernel replay record lost: %v", err)
				}
				if fresh.Readable(id) || fresh.ReadAdmitted(id) || fresh.Settled(id) {
					t.Fatal("historical settlement opened reads")
				}
				if err := fresh.Resettle(id); err != nil {
					t.Fatal(err)
				}
				if !fresh.Readable(id) {
					t.Fatal("fresh reprojection did not open reads")
				}
				next, err := fresh.NextSequence(id, handle.Source(), handle.Epoch(), handle.Generation())
				if err != nil || next != "4" {
					t.Fatalf("cursor=%s, %v", next, err)
				}
			}
			if seen != 2 {
				t.Fatal("missing reprojection")
			}
			if info, _ := fresh.Source(handle.Source()); info.Live || info.Epoch != "" || info.Generation != 0 {
				t.Fatal("historical admission restored authority")
			}
			if len(fresh.journal.Records()) != 0 || !reflect.DeepEqual(record.Epochs, fresh.ExportRecovery().Epochs) {
				t.Fatal("history replayed into active journal")
			}
			if fresh.incarnation == hub.incarnation {
				t.Fatal("incarnation reused")
			}
			if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
				t.Fatal("restored populated asset")
			}
		})
	}
}

func TestRecoveryHooksOwnCopiesAndRunOutsideHubMutex(t *testing.T) {
	var hub *Hub
	staged, accepted := 0, 0
	hooks := &RecoveryHooks{
		BeforeControl: func(attempt ControlAttempt) error {
			staged++
			hub.mu.RLock()
			src, asset := hub.sources[attempt.Control.Source], hub.assets[attempt.Asset]
			hub.mu.RUnlock()
			if !src.control.TryLock() {
				t.Error("before-submit hook holds source lock")
			} else {
				src.control.Unlock()
			}
			if !asset.commit.TryLock() {
				t.Error("before-submit hook holds asset lock")
			} else {
				asset.commit.Unlock()
			}
			view, err := hub.Inspect(attempt.Asset)
			if err != nil || !view.Present {
				t.Error("hook cannot read hub")
			}
			var batch semreg.PublicationBatch
			if json.Unmarshal(attempt.Attempt.Batch, &batch) != nil || batch.Sequence != "3" {
				t.Error("wrong staged bytes")
			}
			attempt.Attempt.Batch[0] = '!'
			return nil
		},
		Accepted: func(record AcceptedBatch) {
			accepted++
			if record.HubSeq == 0 {
				t.Error("accepted result lacks position")
			}
			var batch semreg.PublicationBatch
			if err := json.Unmarshal(record.Batch, &batch); err != nil {
				t.Error(err)
			}
			hub.mu.RLock()
			src, asset := hub.sources[batch.SourceID], hub.assets[batch.AssetID]
			hub.mu.RUnlock()
			if !src.control.TryLock() {
				t.Error("accepted hook holds source lock")
			} else {
				src.control.Unlock()
			}
			if !asset.commit.TryLock() {
				t.Error("accepted hook holds asset lock")
			} else {
				asset.commit.Unlock()
			}
			if _, err := hub.Inspect(batch.AssetID); err != nil {
				t.Error(err)
			}
			record.Batch[0] = '!'
			record.Canonical[0] = '!'
		},
	}
	// Set the hub before publishing so the callbacks can inspect it.
	hub = recoveryHub(t, hooks, func(ControlEvent) error { return nil })
	handle := admitLegacy(t, hub, "source:restore", "synthetic-family", "epoch:restore", 1)
	for _, id := range recoveryAssets {
		mustPublish(t, hub, handle, id, "synthetic", 10, 1)
		mustPublish(t, hub, handle, id, "synthetic", 20, 2)
	}
	if err := fenceRecovery(t, hub, handle); err != nil {
		t.Fatal(err)
	}
	if staged != 2 || accepted != 6 {
		t.Fatalf("hooks=%d/%d", staged, accepted)
	}
	exported := hub.ExportRecovery()
	before := hub.ExportRecovery()
	exported.Assets[0].Accepted[0].Batch[0] = '!'
	exported.Assets[0].Accepted[0].Canonical[0] = '!'
	exported.Assets[0].Packs[0].ID = "changed"
	exported.Assets[0].Control.Attempts[0].Batch[0] = '!'
	exported.Assets[0].Control.Status.ID.Source = "changed"
	exported.Epochs[0].Source = "changed"
	if !reflect.DeepEqual(before, hub.ExportRecovery()) {
		t.Fatal("export aliases hub")
	}
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(before); err != nil {
		t.Fatal(err)
	}
	before.Assets[0].Accepted[0].Batch[0] = '!'
	if fresh.ExportRecovery().Assets[0].Accepted[0].Batch[0] == '!' {
		t.Fatal("restore retained caller buffer")
	}
}

func TestRecoveryRejectedControlDoesNotAdvanceCursor(t *testing.T) {
	hub, handle := populatedRecoveryHub(t, &RecoveryHooks{BeforeControl: func(attempt ControlAttempt) error {
		if attempt.Asset == recoveryAssets[1] {
			return errors.New("synthetic persistence refusal")
		}
		return nil
	}})
	if err := fenceRecovery(t, hub, handle); err == nil {
		t.Fatal("expected rejection")
	}
	record := hub.ExportRecovery()
	if len(record.Assets[0].Accepted) != 3 || len(record.Assets[1].Accepted) != 2 {
		t.Fatal("rejected attempt entered accepted log")
	}
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(record); err != nil {
		t.Fatal(err)
	}
	for i, id := range recoveryAssets {
		got, _ := fresh.Inspect(id)
		want, _ := hub.Inspect(id)
		if !bytes.Equal(got.Canonical, want.Canonical) {
			t.Fatal("mixed outcome snapshot changed")
		}
		next, _ := fresh.NextSequence(id, handle.Source(), handle.Epoch(), 1)
		expected := []semreg.Uint64{"4", "3"}[i]
		if next != expected {
			t.Fatalf("next=%s, want %s", next, expected)
		}
	}
	if err := fresh.Resettle(recoveryAssets[1]); !errors.Is(err, ErrSemanticOutstanding) || fresh.Readable(recoveryAssets[1]) {
		t.Fatal("rejected gate opened")
	}
	if err := fresh.Resettle(recoveryAssets[0]); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryCorruptAssetFailsClosedAndKeepsIndependentAsset(t *testing.T) {
	mutations := map[string]func(*RecoveryAsset){
		"tail":   func(a *RecoveryAsset) { a.Accepted = a.Accepted[:len(a.Accepted)-1] },
		"bytes":  func(a *RecoveryAsset) { a.Accepted[0].Batch[0] = '!' },
		"result": func(a *RecoveryAsset) { a.Accepted[1].Canonical[0] = '!' },
		"cursor": func(a *RecoveryAsset) { a.Accepted[0].Cursor.LastSequence = "9" },
		"order":  func(a *RecoveryAsset) { a.Accepted[0], a.Accepted[1] = a.Accepted[1], a.Accepted[0] },
		"clock":  func(a *RecoveryAsset) { a.Accepted[0].Monotonic = semreg.MonotonicPoint{} },
		"gate":   func(a *RecoveryAsset) { a.Control.Status.Semantic = "invalid" },
	}
	hub, handle := populatedRecoveryHub(t, nil)
	if err := fenceRecovery(t, hub, handle); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			record := hub.ExportRecovery()
			mutate(&record.Assets[0])
			fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
			if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
				t.Fatal("corruption accepted")
			}
			bad, _ := fresh.Inspect(recoveryAssets[0])
			good, _ := fresh.Inspect(recoveryAssets[1])
			if bad.Present || !good.Present || fresh.Readable(recoveryAssets[0]) || fresh.Readable(recoveryAssets[1]) {
				t.Fatal("invalid state installed or read opened")
			}
			if err := fresh.Resettle(recoveryAssets[0]); err == nil && fresh.Readable(recoveryAssets[0]) {
				t.Fatal("corrupt asset reopened")
			}
			if err := fresh.Resettle(recoveryAssets[1]); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type changedRecoveryValidator struct{ testValidator }

func (changedRecoveryValidator) ValidateFact(semreg.FactKey, *semreg.Value) error {
	return errors.New("synthetic validator changed")
}

func TestRecoveryValidatorChangeAndPriorAdmissionRefused(t *testing.T) {
	hub, _ := populatedRecoveryHub(t, nil)
	record := hub.ExportRecovery()
	fresh, err := New(Options{Recovery: &RecoveryHooks{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range recoveryAssets {
		validator := newTestValidator()
		if id == recoveryAssets[0] {
			validator = changedRecoveryValidator{testValidator{pack: testPackRef}}
		}
		_, err := fresh.Declare(AssetSpec{AssetID: id, SourceID: "source:restore", Driver: "synthetic-family", Validators: []semreg.PackValidator{validator}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
		t.Fatal("changed validator accepted")
	}
	if view, _ := fresh.Inspect(recoveryAssets[0]); view.Present {
		t.Fatal("validator failure installed partial state")
	}
	admitted := recoveryHub(t, nil, nil)
	handle := admitLegacy(t, admitted, "source:restore", "synthetic-family", "epoch:unused", 1)
	if err := admitted.Withdraw(handle); err != nil {
		t.Fatal(err)
	}
	if err := admitted.Restore(record); !errors.Is(err, ErrRestore) {
		t.Fatal("restore after withdrawn admission")
	}
}

func TestRecoveryPublicationOnlyRequiresFreshCallback(t *testing.T) {
	hub, _ := populatedRecoveryHub(t, nil)
	fresh := recoveryHub(t, nil, nil)
	if err := fresh.Restore(hub.ExportRecovery()); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Resettle(recoveryAssets[0]); !errors.Is(err, ErrReprojectionPending) {
		t.Fatalf("nil reprojection=%v", err)
	}
	if fresh.Readable(recoveryAssets[0]) {
		t.Fatal("nil callback opened recovered reads")
	}
	declaration, err := fresh.Declare(AssetSpec{AssetID: recoveryAssets[0], SourceID: "source:restore", Driver: "synthetic-family", Validators: []semreg.PackValidator{newTestValidator()}})
	if err != nil {
		t.Fatal(err)
	}
	declaration.SetReprojection(func(ControlEvent) error { return nil })
	if err := fresh.Resettle(recoveryAssets[0]); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryInterruptedControlExportsResolvedReplay(t *testing.T) {
	hub, handle := populatedRecoveryHub(t, nil)
	interrupted := false
	hub.faults = func(stage string, id semreg.AssetID) {
		if stage == "after-swap" && id == recoveryAssets[0] && !interrupted {
			interrupted = true
			panic("synthetic interruption")
		}
	}
	result, err := hub.Fence(FenceRequest{Source: handle.Source(), Epoch: handle.Epoch(), Generation: 1,
		Reason: "semhubtest.stop", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, ObservedAt: testWall(3), Monotonic: testMono(3)})
	if err == nil {
		t.Fatal("expected interrupted submission")
	}
	hub.faults = nil
	if _, err := hub.RetryControl(result.ID); err != nil {
		t.Fatal(err)
	}
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(hub.ExportRecovery()); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryPublicationDraftIsOwnedAndNotAcceptance(t *testing.T) {
	hub := recoveryHub(t, nil, nil)
	handle := admitLegacy(t, hub, "source:restore", "synthetic-family", "epoch:restore", 1)
	tx, err := hub.Begin(handle, recoveryAssets[0])
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	batch := testBatch(t, tx, "synthetic", 10, 1)
	if _, _, err := tx.Apply(batch, testMono(1)); err != nil {
		t.Fatal(err)
	}
	draft, err := tx.Export()
	if err != nil || draft.HubSeq != 0 {
		t.Fatalf("draft=%v", err)
	}
	draft.Batch[0] = '!'
	draft.Canonical[0] = '!'
	if len(hub.ExportRecovery().Assets[0].Accepted) != 0 {
		t.Fatal("draft became accepted")
	}
	if _, err := tx.Commit(nil); err != nil {
		t.Fatal(err)
	}
	record := hub.ExportRecovery()
	if record.Assets[0].Accepted[0].Batch[0] == '!' || record.Assets[0].Accepted[0].Canonical[0] == '!' {
		t.Fatal("draft shares committed memory")
	}
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(record); err != nil {
		t.Fatal(err)
	}
	// A second process-equivalent checkpoint must preserve publication-only gates.
	second := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := second.Restore(fresh.ExportRecovery()); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryStagedAttemptRemainsReservedOutsideLocks(t *testing.T) {
	entered := make(chan ControlID, 1)
	release, done := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hub, handle := populatedRecoveryHub(t, &RecoveryHooks{BeforeControl: func(attempt ControlAttempt) error {
		if attempt.Asset == recoveryAssets[0] {
			entered <- attempt.Control
			<-release
		}
		return nil
	}})
	var failure error
	go func() { failure = fenceRecovery(t, hub, handle); close(done) }()
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("control did not finish")
		}
	})
	var id ControlID
	select {
	case id = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("staging callback did not start")
	}
	if _, err := hub.RetryControl(id); !errors.Is(err, ErrControlPending) {
		t.Fatalf("retry during export=%v", err)
	}
	record := hub.ExportRecovery()
	if len(record.Assets[0].Accepted) != 2 || len(record.Assets[0].Control.Attempts) != 1 {
		t.Fatal("unpersisted staged attempt submitted or rebuilt")
	}
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("control did not finish")
	}
	if failure != nil {
		t.Fatal(failure)
	}
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(hub.ExportRecovery()); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryUnsupportedRecordFailsNamedEmptyAssetsClosed(t *testing.T) {
	hub, _ := populatedRecoveryHub(t, nil)
	record := hub.ExportRecovery()
	record.Version++
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
		t.Fatal("unsupported record accepted")
	}
	for _, id := range recoveryAssets {
		if fresh.Readable(id) {
			t.Fatal("unsupported recovery left reads open")
		}
	}
}
