package semhub

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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

func TestRecoveryAcceptedReplayRetainsOriginalPosition(t *testing.T) {
	for _, interruption := range []string{"after-commit", "accepted-hook"} {
		t.Run(interruption, func(t *testing.T) {
			var deliveries []AcceptedBatch
			interrupted := false
			hub, handle := populatedRecoveryHub(t, &RecoveryHooks{Accepted: func(record AcceptedBatch) {
				deliveries = append(deliveries, record)
				if interruption == "accepted-hook" && record.Cursor.LastSequence == "3" && !interrupted {
					interrupted = true
					panic("synthetic hook interruption")
				}
			}})
			hub.faults = func(stage string, id semreg.AssetID) {
				if interruption == "after-commit" && stage == interruption && id == recoveryAssets[0] && !interrupted {
					interrupted = true
					panic("synthetic commit interruption")
				}
			}
			result, err := hub.Fence(FenceRequest{Source: handle.Source(), Epoch: handle.Epoch(), Generation: 1,
				Reason: "semhubtest.stop", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, ObservedAt: testWall(3), Monotonic: testMono(3)})
			if err == nil || !interrupted {
				t.Fatal("expected interruption after acceptance")
			}
			hub.faults = nil
			if _, err := hub.RetryControl(result.ID); err != nil {
				t.Fatal(err)
			}
			record := hub.ExportRecovery()
			var original *AcceptedBatch
			duplicates := 0
			// Model the service deduplicating callback deliveries by original position.
			record.Assets[0].Accepted = nil
			for _, delivery := range deliveries {
				var batch semreg.PublicationBatch
				if err := json.Unmarshal(delivery.Batch, &batch); err != nil {
					t.Fatal(err)
				}
				if batch.AssetID != recoveryAssets[0] {
					continue
				}
				if batch.Sequence == "3" {
					if original != nil {
						duplicates++
						if !reflect.DeepEqual(*original, delivery) {
							t.Fatal("duplicate acceptance changed its original result or position")
						}
						continue
					}
					copy := delivery
					original = &copy
				}
				record.Assets[0].Accepted = append(record.Assets[0].Accepted, delivery)
			}
			if duplicates != 1 || original == nil || original.HubSeq != record.Assets[0].HubSeq {
				t.Fatal("missing stable duplicate acceptance")
			}
			if err := recoveryHub(t, nil, func(ControlEvent) error { return nil }).Restore(record); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRecoveryOmittedAssetClosesRestoredSource(t *testing.T) {
	hub, _ := populatedRecoveryHub(t, nil)
	for _, historyOnly := range []bool{false, true} {
		record := hub.ExportRecovery()
		if historyOnly {
			record.Assets = nil
		} else {
			record.Assets = record.Assets[:1]
		}
		fresh := recoveryHub(t, nil, nil)
		if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
			t.Fatal("incomplete source restored")
		}
		for _, id := range recoveryAssets {
			if fresh.ReadAdmitted(id) || fresh.Settled(id) || fresh.Admitted(id) {
				t.Fatal("omitted source asset left open")
			}
		}
	}
}

func TestRecoveryEpochsCannotBeReadmitted(t *testing.T) {
	hub, handle := populatedRecoveryHub(t, nil)
	for _, material := range []string{"cursors", "history", "both"} {
		t.Run(material, func(t *testing.T) {
			record := hub.ExportRecovery()
			switch material {
			case "cursors":
				record.Epochs = nil
			case "history":
				record = recoveryHub(t, nil, nil).ExportRecovery()
				record.Epochs = hub.ExportRecovery().Epochs
				record.Regimes = hub.ExportRecovery().Regimes
				record.Drivers = hub.ExportRecovery().Drivers
			}
			fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
			if err := fresh.Restore(record); err != nil {
				t.Fatal(err)
			}
			for _, generation := range []uint64{1, 2} {
				if _, err := fresh.AdmitSource(Admission{Source: handle.Source(), Driver: "synthetic-family", Epoch: handle.Epoch(), Generation: generation, Legacy: LegacyGrowattStorage}); !errors.Is(err, ErrAdmission) {
					t.Fatalf("restored epoch readmitted: %v", err)
				}
			}
			newHandle := admitLegacy(t, fresh, handle.Source(), "synthetic-family", "epoch:fresh", 1)
			for _, id := range recoveryAssets {
				if err := fresh.Resettle(id); err != nil {
					t.Fatal(err)
				}
			}
			if tx, err := fresh.BeginEpoch(newHandle, recoveryAssets[0], handle.Epoch(), 2); !errors.Is(err, ErrAdmission) {
				if tx != nil {
					tx.Abort()
				}
				t.Fatalf("transition reused a restored epoch: %v", err)
			}
		})
	}
}

func TestRecoveryNegativeCheckpointPreconditions(t *testing.T) {
	hub, handle := populatedRecoveryHub(t, nil)
	publication := hub.ExportRecovery()
	if err := fenceRecovery(t, hub, handle); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*testing.T, *RecoveryAsset){
		"source":                func(t *testing.T, a *RecoveryAsset) { a.Source = "source:other" },
		"driver":                func(t *testing.T, a *RecoveryAsset) { a.Driver = "other-family" },
		"pack":                  func(t *testing.T, a *RecoveryAsset) { a.Packs[0].ID = "other.pack" },
		"hub-sequence":          func(t *testing.T, a *RecoveryAsset) { a.Accepted[1].HubSeq = a.Accepted[0].HubSeq },
		"control-identity":      func(t *testing.T, a *RecoveryAsset) { a.Control.Status.ID.Source = "source:other" },
		"control-attempt-count": func(t *testing.T, a *RecoveryAsset) { a.Control.Status.Attempts++ },
		"control-digest":        func(t *testing.T, a *RecoveryAsset) { a.Control.Attempts[0].Digest = "sha256:corrupt" },
		"fence-generation":      func(t *testing.T, a *RecoveryAsset) { a.Control.Generation++ },
		"retire-metadata":       func(t *testing.T, a *RecoveryAsset) { a.Control.Kind = ControlRetire },
		"fence-metadata": func(t *testing.T, a *RecoveryAsset) {
			a.Control.Status.Semantic = SemanticRejected
			var batch semreg.PublicationBatch
			if err := json.Unmarshal(a.Control.Attempts[0].Batch, &batch); err != nil {
				t.Fatal(err)
			}
			batch.GenerationFences = batch.GenerationFences[:0]
			digest, err := batch.ComputedDigest()
			if err != nil {
				t.Fatal(err)
			}
			batch.BatchDigest = digest
			a.Control.Attempts[0].Digest = digest
			a.Control.Attempts[0].Batch, err = json.Marshal(batch)
			if err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			record := hub.ExportRecovery()
			mutate(t, &record.Assets[0])
			fresh := recoveryHub(t, nil, nil)
			if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
				t.Fatal("corrupt checkpoint accepted")
			}
			if view, _ := fresh.Inspect(recoveryAssets[0]); view.Present {
				t.Fatal("corrupt checkpoint installed")
			}
			// Assert each public predicate independently, including a nil gate.
			if fresh.ReadAdmitted(recoveryAssets[0]) {
				t.Error("ReadAdmitted ignored restore failure")
			}
			if fresh.Settled(recoveryAssets[0]) {
				t.Error("Settled ignored restore failure")
			}
			if fresh.Admitted(recoveryAssets[0]) {
				t.Error("Admitted ignored restore failure")
			}
		})
	}
	t.Run("publication-only-truncation", func(t *testing.T) {
		publication.Assets[0].Accepted = publication.Assets[0].Accepted[:1]
		if err := recoveryHub(t, nil, nil).Restore(publication); !errors.Is(err, ErrRestore) {
			t.Fatal("truncated publication-only log accepted")
		}
	})
	t.Run("undeclared-asset", func(t *testing.T) {
		record := hub.ExportRecovery()
		record.Assets[0].Asset = "asset:undeclared"
		fresh := recoveryHub(t, nil, nil)
		if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
			t.Fatal("undeclared asset accepted")
		}
		for _, id := range recoveryAssets {
			if fresh.ReadAdmitted(id) || fresh.Settled(id) || fresh.Admitted(id) {
				t.Fatalf("undeclared checkpoint left source asset %s open", id)
			}
		}
	})
	for _, state := range []string{"hub-sequence", "records", "restored", "failed"} {
		t.Run("populated-"+state, func(t *testing.T) {
			fresh := recoveryHub(t, nil, nil)
			// Isolate asset preconditions from the separate source admission guard.
			target := fresh.assets[recoveryAssets[0]]
			switch state {
			case "hub-sequence":
				target.hubSeq = 1
			case "records":
				target.records = []AcceptedBatch{{}}
			case "restored":
				target.restored = true
			case "failed":
				target.restoreFailed = true
			}
			if err := fresh.Restore(hub.ExportRecovery()); !errors.Is(err, ErrRestore) {
				t.Fatal("populated asset accepted without prior admission")
			}
		})
	}
}

func TestRecoverySupersededDuringExportIsNotSubmitted(t *testing.T) {
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
	select {
	case id := <-entered:
		if err := hub.AbandonControl(id); err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("export did not start")
	}
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("control did not finish")
	}
	if !errors.Is(failure, ErrControlAbandoned) {
		t.Fatalf("abandoned export submitted: %v", failure)
	}
	if len(hub.ExportRecovery().Assets[0].Accepted) != 2 {
		t.Fatal("abandoned attempt committed")
	}
}

func TestRecoveryExportRefusalRequiresAbandonAndReissue(t *testing.T) {
	refusal := errors.New("synthetic storage refusal")
	refuse := true
	hub, handle := populatedRecoveryHub(t, &RecoveryHooks{BeforeControl: func(ControlAttempt) error {
		if refuse {
			return refusal
		}
		return nil
	}})
	request := FenceRequest{Source: handle.Source(), Epoch: handle.Epoch(), Generation: 1,
		Reason: "semhubtest.stop", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, ObservedAt: testWall(3), Monotonic: testMono(3)}
	result, err := hub.Fence(request)
	if !errors.Is(err, refusal) {
		t.Fatalf("refusal cause lost: %v", err)
	}
	before := hub.ExportRecovery()
	if status := before.Assets[0].Control.Status; status.ErrorID != "semhub.control_export_refused" || status.Semantic != SemanticRejected {
		t.Fatal("refusal lacks explicit terminal error")
	}
	if before.Assets[1].Control.Status.Semantic != SemanticUnbuilt {
		t.Fatal("control continued after refusal")
	}
	refuse = false
	if _, err := hub.RetryControl(result.ID); !errors.Is(err, refusal) {
		t.Fatalf("terminal refusal retried: %v", err)
	}
	if !reflect.DeepEqual(before, hub.ExportRecovery()) {
		t.Fatal("retry changed refused bytes or cursor")
	}
	if err := hub.AbandonControl(result.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Fence(request); err != nil {
		t.Fatal(err)
	}
	for _, entry := range hub.ExportRecovery().Assets {
		if len(entry.Accepted) != 3 {
			t.Fatal("reissue did not commit exactly once")
		}
	}
}

func TestRecoveryPreservesSemRegErrorCause(t *testing.T) {
	hub, _ := populatedRecoveryHub(t, nil)
	record := hub.ExportRecovery()
	record.Assets[0].Accepted[0].Monotonic = semreg.MonotonicPoint{}
	err := recoveryHub(t, nil, nil).Restore(record)
	var cause *semreg.Error
	if !errors.Is(err, ErrRestore) || !errors.As(err, &cause) || !errors.Is(err, cause) {
		t.Fatalf("restore lost SemReg error cause: %v", err)
	}
}

func TestRecoveryEpochHistoryOfAssetlessSourceStaysReserved(t *testing.T) {
	// A source admitted before it declared any asset exports epoch history only.
	origin, err := New(Options{Recovery: &RecoveryHooks{}})
	if err != nil {
		t.Fatal(err)
	}
	handle := admitLegacy(t, origin, "source:assetless", "synthetic-family", "epoch:assetless", 1)
	record := origin.ExportRecovery()
	if len(record.Assets) != 0 || len(record.Epochs) == 0 {
		t.Fatalf("fixture must carry epoch history without assets: assets=%d epochs=%d", len(record.Assets), len(record.Epochs))
	}
	// The fresh hub has no record of that source when the checkpoint is restored.
	fresh, err := New(Options{Recovery: &RecoveryHooks{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.Restore(record); err != nil {
		t.Fatal(err)
	}
	for _, generation := range []uint64{1, 2} {
		if _, err := fresh.AdmitSource(Admission{Source: handle.Source(), Driver: "synthetic-family", Epoch: handle.Epoch(), Generation: generation, Legacy: LegacyGrowattStorage}); !errors.Is(err, ErrAdmission) {
			t.Fatalf("historical epoch of an assetless source readmitted (generation %d): %v", generation, err)
		}
	}
	admitLegacy(t, fresh, handle.Source(), "synthetic-family", "epoch:assetless-new", 1)
}

func TestRecoveryLegacyRestoreAdmitsSuccessorAndPublishes(t *testing.T) {
	hub, handle := populatedRecoveryHub(t, nil)
	record := hub.ExportRecovery()
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(record); err != nil {
		t.Fatal(err)
	}
	successor := admitLegacy(t, fresh, handle.Source(), "synthetic-family", "epoch:after-restore", 1)
	for _, id := range recoveryAssets {
		if inc := successor.Incarnation(id); inc.First {
			t.Fatalf("admission after a restore must be a successor, got First for %s", id)
		}
		if err := fresh.Resettle(id); err != nil {
			t.Fatal(err)
		}
		// A successor binding is scoped by epoch and generation, so publishing
		// cannot collide with the immutable bindings of the restored history.
		mustPublish(t, fresh, successor, id, "synthetic", 30, 4)
	}
}

func TestRecoveryUnsupportedHistoryOnlyRecordClosesNamedSource(t *testing.T) {
	hub, _ := populatedRecoveryHub(t, nil)
	record := hub.ExportRecovery()
	// The source is named only through epoch history.
	record.Assets = nil
	record.Version++
	if len(record.Epochs) == 0 {
		t.Fatal("fixture must carry epoch history")
	}
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
		t.Fatal("unsupported record accepted")
	}
	for _, id := range recoveryAssets {
		if fresh.Readable(id) {
			t.Fatalf("unsupported history-only record left %s readable", id)
		}
	}
}

func TestRecoveryOmittedWholeSourceClosesAllAssets(t *testing.T) {
	origin := recoveryHub(t, nil, nil)
	record := origin.ExportRecovery()
	record.Assets = nil // No history names the omitted, unpublished source.
	fresh := recoveryHub(t, nil, nil)
	if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
		t.Fatalf("incomplete checkpoint accepted: %v", err)
	}
	for _, id := range recoveryAssets {
		if fresh.Readable(id) {
			t.Fatalf("omitted asset %s remained readable", id)
		}
	}
}

func TestRecoveryUnpublishedAssetReprojectsAbsence(t *testing.T) {
	origin := recoveryHub(t, nil, nil)
	calls := 0
	fresh := recoveryHub(t, nil, func(event ControlEvent) error {
		calls++
		if event.View.Present || event.View.Snapshot.SnapshotID != "" || len(event.View.Canonical) != 0 {
			t.Error("unpublished asset materialized by reprojection")
		}
		return nil
	})
	if err := fresh.Restore(origin.ExportRecovery()); err != nil {
		t.Fatal(err)
	}
	for _, id := range recoveryAssets {
		if err := fresh.Resettle(id); err != nil {
			t.Fatal(err)
		}
		view, err := fresh.Inspect(id)
		if err != nil || view.Present {
			t.Fatalf("unexpected restored view: %+v, %v", view, err)
		}
	}
	if calls != len(recoveryAssets) {
		t.Fatalf("callbacks=%d", calls)
	}
}

func TestRecoveryRejectsMalformedAssetlessEpochHistory(t *testing.T) {
	mutations := map[string]func(*EpochRecord){
		"incarnation":      func(e *EpochRecord) { e.Incarnation = "" },
		"source":           func(e *EpochRecord) { e.Source = "" },
		"epoch":            func(e *EpochRecord) { e.Epoch = "" },
		"generation":       func(e *EpochRecord) { e.Generation = 0 },
		"kind":             func(e *EpochRecord) { e.Kind = EventPublished },
		"asset-id":         func(e *EpochRecord) { e.Kind = EventSourceRetired; e.Assets = []semreg.AssetID{""} },
		"unknown-asset":    func(e *EpochRecord) { e.Kind = EventSourceRetired; e.Assets = []semreg.AssetID{"asset:missing"} },
		"duplicate-assets": func(e *EpochRecord) { e.Kind = EventSourceRetired; e.Assets = []semreg.AssetID{"asset:a", "asset:a"} },
		"unordered-assets": func(e *EpochRecord) { e.Kind = EventSourceRetired; e.Assets = []semreg.AssetID{"asset:b", "asset:a"} },
		"admission-assets": func(e *EpochRecord) { e.Assets = []semreg.AssetID{"asset:a"} },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			origin, err := New(Options{Recovery: &RecoveryHooks{}})
			if err != nil {
				t.Fatal(err)
			}
			admitLegacy(t, origin, "source:assetless", "synthetic-family", "epoch:old", 1)
			record := origin.ExportRecovery()
			mutate(&record.Epochs[0])
			fresh := recoveryHub(t, nil, nil)
			if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
				t.Fatalf("invalid history accepted: %v", err)
			}
			if len(fresh.ExportRecovery().Epochs) != 0 || len(fresh.restoredEpochs) != 0 {
				t.Fatal("invalid history retained or reserved")
			}
			for _, id := range recoveryAssets {
				if fresh.Readable(id) {
					t.Fatalf("invalid checkpoint left %s readable", id)
				}
			}
		})
	}
}

func TestRecoveryPreservesSourceIdentityRegime(t *testing.T) {
	for _, assetless := range []bool{false, true} {
		for _, family := range []LegacyFamily{"", LegacyGrowattStorage, LegacyPVSunSpec} {
			t.Run(fmt.Sprintf("assetless=%t/family=%s", assetless, family), func(t *testing.T) {
				origin := recoveryHub(t, nil, nil)
				if assetless {
					var err error
					origin, err = New(Options{Recovery: &RecoveryHooks{}})
					if err != nil {
						t.Fatal(err)
					}
				}
				request := Admission{Source: "source:restore", Driver: "synthetic-family", Legacy: family}
				if family != "" {
					request.Epoch, request.Generation = "epoch:old", 1
				}
				if _, err := origin.AdmitSource(request); err != nil {
					t.Fatal(err)
				}
				fresh := recoveryHub(t, nil, nil)
				if assetless {
					var err error
					fresh, err = New(Options{Recovery: &RecoveryHooks{}})
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := fresh.Restore(origin.ExportRecovery()); err != nil {
					t.Fatal(err)
				}
				// A second export/restore must retain the regime without a current handle.
				record := fresh.ExportRecovery()
				again := recoveryHub(t, nil, nil)
				if assetless {
					var err error
					again, err = New(Options{Recovery: &RecoveryHooks{}})
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := again.Restore(record); err != nil {
					t.Fatal(err)
				}
				fresh = again
				for _, other := range []LegacyFamily{"", LegacyGrowattStorage, LegacyPVSunSpec} {
					if other == family {
						continue
					}
					wrong := request
					wrong.Legacy, wrong.Epoch, wrong.Generation = other, "", 0
					if other != "" {
						wrong.Epoch, wrong.Generation = "epoch:new", 1
					}
					if _, err := fresh.AdmitSource(wrong); !errors.Is(err, ErrAdmission) {
						t.Fatalf("changed regime admitted: %v", err)
					}
				}
				if !reflect.DeepEqual(record, fresh.ExportRecovery()) {
					t.Fatal("rejected admission changed checkpoint")
				}
				request.Epoch, request.Generation = "", 0
				if family != "" {
					request.Epoch, request.Generation = "epoch:new", 1
				}
				if _, err := fresh.AdmitSource(request); err != nil {
					t.Fatalf("matching regime refused: %v", err)
				}
			})
		}
	}
}

func TestRecoveryRejectsMissingOrInvalidIdentityRegime(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		origin, _ := populatedRecoveryHub(t, nil)
		record := origin.ExportRecovery()
		if invalid {
			record.Regimes["source:restore"] = LegacyFamily("unknown-family")
		} else {
			delete(record.Regimes, "source:restore")
		}
		fresh := recoveryHub(t, nil, nil)
		if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
			t.Fatalf("invalid regime accepted: %v", err)
		}
		for _, id := range recoveryAssets {
			if fresh.Readable(id) {
				t.Fatalf("invalid regime left %s readable", id)
			}
		}
	}
}

func TestRecoveryWithdrawnFirstAdmissionRoundTrips(t *testing.T) {
	origin, err := New(Options{Recovery: &RecoveryHooks{}})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, origin, "asset:withdrawn", "source:withdrawn", "growatt_storage", nil)
	handle, err := origin.AdmitSource(Admission{Source: "source:withdrawn", Driver: "growatt_storage", Legacy: LegacyGrowattStorage, Epoch: "epoch:withdrawn", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := origin.Withdraw(handle); err != nil {
		t.Fatal(err)
	}
	record := origin.ExportRecovery()
	if len(record.Epochs) == 0 {
		t.Fatal("fixture must carry the withdrawn admission's history")
	}
	fresh, err := New(Options{Recovery: &RecoveryHooks{}})
	if err != nil {
		t.Fatal(err)
	}
	mustDeclare(t, fresh, "asset:withdrawn", "source:withdrawn", "growatt_storage", nil)
	if err := fresh.Restore(record); err != nil {
		t.Fatalf("valid checkpoint after a withdrawn first admission rejected: %v", err)
	}
	if _, err := fresh.AdmitSource(Admission{Source: "source:withdrawn", Driver: "growatt_storage", Legacy: LegacyGrowattStorage, Epoch: "epoch:withdrawn", Generation: 1}); !errors.Is(err, ErrAdmission) {
		t.Fatalf("the withdrawn admission's epoch must stay reserved: %v", err)
	}
	if _, fixed := record.Regimes["source:withdrawn"]; fixed || !reflect.DeepEqual(record.Unfixed, []semreg.SourceID{"source:withdrawn"}) {
		t.Fatalf("withdrawn first admission must export an explicit unfixed regime: regimes=%v unfixed=%v", record.Regimes, record.Unfixed)
	}
	if again := fresh.ExportRecovery(); !reflect.DeepEqual(again.Unfixed, record.Unfixed) {
		t.Fatalf("restored unfixed regime not re-exported: %v", again.Unfixed)
	}
	// An unfixed regime lets the next admission choose any regime, as in the origin hub.
	if _, err := fresh.AdmitSource(Admission{Source: "source:withdrawn", Driver: "growatt_storage", Legacy: LegacyTeslaEVSE, Epoch: "epoch:after-withdrawal", Generation: 1}); err != nil {
		t.Fatalf("unfixed regime refused a new regime: %v", err)
	}
}

func TestRecoveryRejectsOmittedRegimeOfAdmittedUnpublishedSource(t *testing.T) {
	// An admitted, never-withdrawn source fixes its regime even before it publishes.
	for _, assetless := range []bool{false, true} {
		t.Run(fmt.Sprintf("assetless=%t", assetless), func(t *testing.T) {
			newHub := func() *Hub {
				if !assetless {
					return recoveryHub(t, nil, nil)
				}
				hub, err := New(Options{Recovery: &RecoveryHooks{}})
				if err != nil {
					t.Fatal(err)
				}
				return hub
			}
			origin := newHub()
			admitLegacy(t, origin, "source:restore", "synthetic-family", "epoch:old", 1)
			record := origin.ExportRecovery()
			if _, ok := record.Regimes["source:restore"]; !ok || len(record.Unfixed) != 0 {
				t.Fatalf("admitted source must export its regime: regimes=%v unfixed=%v", record.Regimes, record.Unfixed)
			}
			delete(record.Regimes, "source:restore")
			delete(record.Drivers, "source:restore") // the whole entry is omitted, ownership included
			fresh := newHub()
			if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
				t.Fatalf("checkpoint without the admitted source's regime accepted: %v", err)
			}
			if len(fresh.restoredEpochs) != 0 || len(fresh.restoredRegimes) != 0 {
				t.Fatal("rejected checkpoint reserved history or regimes")
			}
		})
	}
}

func TestRecoveryRejectsInconsistentUnfixedRegimes(t *testing.T) {
	withdrawn := func(t *testing.T) (*Hub, RecoveryRecord) {
		origin, err := New(Options{Recovery: &RecoveryHooks{}})
		if err != nil {
			t.Fatal(err)
		}
		handle := admitLegacy(t, origin, "source:withdrawn", "synthetic-family", "epoch:withdrawn", 1)
		if err := origin.Withdraw(handle); err != nil {
			t.Fatal(err)
		}
		admitLegacy(t, origin, "source:kept", "synthetic-family", "epoch:kept", 1)
		return origin, origin.ExportRecovery()
	}
	mutations := map[string]func(*RecoveryRecord){
		"unfixed-without-driver": func(r *RecoveryRecord) { delete(r.Drivers, "source:withdrawn") },
		"omitted":                func(r *RecoveryRecord) { r.Unfixed = nil; delete(r.Drivers, "source:withdrawn") },
		"also-fixed":             func(r *RecoveryRecord) { r.Regimes["source:withdrawn"] = LegacyGrowattStorage },
		"fixed-moved": func(r *RecoveryRecord) {
			// Sorted, so only the current admission of source:kept can reject it.
			delete(r.Regimes, "source:kept")
			r.Unfixed = []semreg.SourceID{"source:kept", "source:withdrawn"}
		},
		"without-history": func(r *RecoveryRecord) {
			r.Unfixed = append(r.Unfixed, "source:zz-unknown")
			r.Drivers["source:zz-unknown"] = "synthetic-family"
		},
		"duplicate":  func(r *RecoveryRecord) { r.Unfixed = append(r.Unfixed, r.Unfixed[0]) },
		"invalid-id": func(r *RecoveryRecord) { r.Unfixed = append([]semreg.SourceID{""}, r.Unfixed...) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			_, record := withdrawn(t)
			if !reflect.DeepEqual(record.Unfixed, []semreg.SourceID{"source:withdrawn"}) {
				t.Fatalf("fixture must carry one unfixed source: %v", record.Unfixed)
			}
			mutate(&record)
			fresh, err := New(Options{Recovery: &RecoveryHooks{}})
			if err != nil {
				t.Fatal(err)
			}
			if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
				t.Fatalf("inconsistent unfixed regimes accepted: %v", err)
			}
			if len(fresh.restoredEpochs) != 0 || len(fresh.restoredRegimes) != 0 {
				t.Fatal("rejected checkpoint reserved history or regimes")
			}
		})
	}
}

func TestRecoveryRestoresAssetlessSourceDriverOwnership(t *testing.T) {
	origin, err := New(Options{Recovery: &RecoveryHooks{}})
	if err != nil {
		t.Fatal(err)
	}
	admitLegacy(t, origin, "source:assetless", "driver-a", "epoch:old", 1)
	record := origin.ExportRecovery()
	if record.Drivers["source:assetless"] != "driver-a" {
		t.Fatalf("checkpoint must name the owning driver: %v", record.Drivers)
	}
	fresh, err := New(Options{Recovery: &RecoveryHooks{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.Restore(record); err != nil {
		t.Fatal(err)
	}
	var collision *CollisionError
	if _, err := fresh.AdmitSource(Admission{Source: "source:assetless", Driver: "driver-b", Legacy: LegacyGrowattStorage, Epoch: "epoch:other", Generation: 1}); !errors.As(err, &collision) {
		t.Fatalf("another driver admitted a restored source: %v", err)
	}
	if _, err := fresh.Declare(AssetSpec{AssetID: "asset:other", SourceID: "source:assetless", Driver: "driver-b", Validators: []semreg.PackValidator{newTestValidator()}}); !errors.As(err, &collision) {
		t.Fatalf("another driver declared an asset of a restored source: %v", err)
	}
	if again := fresh.ExportRecovery(); !reflect.DeepEqual(again.Drivers, record.Drivers) {
		t.Fatalf("restored ownership not re-exported: %v", again.Drivers)
	}
	admitLegacy(t, fresh, "source:assetless", "driver-a", "epoch:new", 1)
}

func TestRecoveryRejectsInconsistentDriverOwnership(t *testing.T) {
	mutations := map[string]func(*RecoveryRecord){
		"missing":    func(r *RecoveryRecord) { delete(r.Drivers, "source:restore") },
		"empty":      func(r *RecoveryRecord) { r.Drivers["source:restore"] = "" },
		"unknown":    func(r *RecoveryRecord) { r.Drivers["source:zz-unknown"] = "synthetic-family" },
		"conflicted": func(r *RecoveryRecord) { r.Drivers["source:restore"] = "driver-other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			origin, _ := populatedRecoveryHub(t, nil)
			record := origin.ExportRecovery()
			if record.Drivers["source:restore"] != "synthetic-family" {
				t.Fatalf("fixture must name the declaring driver: %v", record.Drivers)
			}
			mutate(&record)
			fresh := recoveryHub(t, nil, nil)
			if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
				t.Fatalf("inconsistent driver ownership accepted: %v", err)
			}
			if len(fresh.restoredEpochs) != 0 || len(fresh.restoredRegimes) != 0 {
				t.Fatal("rejected checkpoint reserved history or regimes")
			}
			for _, id := range recoveryAssets {
				if fresh.Readable(id) {
					t.Fatalf("rejected checkpoint left %s readable", id)
				}
			}
		})
	}
}

func TestRecoveryUnfixedSourceRefusesPriorDestinationAdmission(t *testing.T) {
	for _, withdraw := range []bool{false, true} {
		t.Run(fmt.Sprintf("withdrawn=%t", withdraw), func(t *testing.T) {
			origin, err := New(Options{Recovery: &RecoveryHooks{}})
			if err != nil {
				t.Fatal(err)
			}
			handle := admitLegacy(t, origin, "source:withdrawn", "synthetic-family", "epoch:withdrawn", 1)
			if err := origin.Withdraw(handle); err != nil {
				t.Fatal(err)
			}
			record := origin.ExportRecovery()
			if !reflect.DeepEqual(record.Unfixed, []semreg.SourceID{"source:withdrawn"}) {
				t.Fatalf("fixture must carry one unfixed source: %v", record.Unfixed)
			}
			fresh, err := New(Options{Recovery: &RecoveryHooks{}})
			if err != nil {
				t.Fatal(err)
			}
			prior := admitLegacy(t, fresh, "source:withdrawn", "synthetic-family", "epoch:destination", 1)
			if withdraw {
				if err := fresh.Withdraw(prior); err != nil {
					t.Fatal(err)
				}
			}
			if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
				t.Fatalf("restore over a prior destination admission accepted: %v", err)
			}
			if len(fresh.restoredEpochs) != 0 {
				t.Fatal("rejected checkpoint reserved history")
			}
		})
	}
}

func TestRecoveryReplayRedeliversInterruptedAcceptance(t *testing.T) {
	var delivered []AcceptedBatch
	interrupt := true
	hooks := &RecoveryHooks{Accepted: func(record AcceptedBatch) {
		delivered = append(delivered, record)
		if interrupt {
			interrupt = false
			panic("synthetic storage interruption")
		}
	}}
	hub := recoveryHub(t, hooks, nil)
	handle := admitLegacy(t, hub, "source:restore", "synthetic-family", "epoch:restore", 1)
	id := recoveryAssets[0]
	tx, err := hub.Begin(handle, id)
	if err != nil {
		t.Fatal(err)
	}
	batch := testBatch(t, tx, "synthetic", 10, 1)
	if _, _, err := tx.Apply(batch, testMono(1)); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("fixture must interrupt the first delivery")
			}
		}()
		_, _ = tx.Commit(nil)
	}()
	retry, err := hub.Begin(handle, id)
	if err != nil {
		t.Fatal(err)
	}
	defer retry.Abort()
	if _, _, err := retry.Apply(batch, testMono(1)); err != nil {
		t.Fatal(err)
	}
	if outcome, err := retry.Commit(nil); err != nil || outcome != Replayed {
		t.Fatalf("exact retry = %v/%v, want a replay", outcome, err)
	}
	if len(delivered) != 2 || delivered[0].HubSeq == 0 || delivered[1].HubSeq != delivered[0].HubSeq || !bytes.Equal(delivered[1].Batch, delivered[0].Batch) {
		t.Fatalf("replay must redeliver the original acceptance: %+v", delivered)
	}
}

func TestRecoveryRejectsControlStatusContradictingAcceptedLog(t *testing.T) {
	for _, state := range []SemanticState{SemanticPending, SemanticRejected, SemanticUncertain} {
		t.Run(string(state), func(t *testing.T) {
			hub, handle := populatedRecoveryHub(t, nil)
			if err := fenceRecovery(t, hub, handle); err != nil {
				t.Fatal(err)
			}
			record := hub.ExportRecovery()
			tampered := false
			for i := range record.Assets {
				if control := record.Assets[i].Control; control != nil && semanticSuccess(control.Status.Semantic) {
					control.Status.Semantic = state
					tampered = true
				}
			}
			if !tampered {
				t.Fatal("fixture must carry an accepted control")
			}
			fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
			if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
				t.Fatalf("control status %s contradicting the accepted log was restored: %v", state, err)
			}
		})
	}
}

func TestRecoveryControlDeliveryInterruptionKeepsAcceptance(t *testing.T) {
	var delivered []AcceptedBatch
	interrupt := false
	hooks := &RecoveryHooks{Accepted: func(record AcceptedBatch) {
		delivered = append(delivered, record)
		if interrupt {
			interrupt = false
			panic("synthetic storage interruption")
		}
	}}
	hub, handle := populatedRecoveryHub(t, hooks)
	published := len(delivered)
	interrupt = true
	result, err := hub.Fence(FenceRequest{Source: handle.Source(), Epoch: handle.Epoch(), Generation: handle.Generation(),
		Reason: "semhubtest.stop", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, ObservedAt: testWall(3), Monotonic: testMono(3)})
	if err == nil || len(delivered) != published+1 {
		t.Fatalf("fixture must interrupt the first control delivery: err=%v deliveries=%d", err, len(delivered)-published)
	}
	interrupted := delivered[published]
	record := hub.ExportRecovery()
	// The interrupted asset stays accepted; assets not yet submitted stay
	// unbuilt until RetryControl continues the operation.
	found := false
	for _, entry := range record.Assets {
		if entry.Control != nil && entry.HubSeq == interrupted.HubSeq {
			found = true
			if !semanticSuccess(entry.Control.Status.Semantic) {
				t.Fatalf("committed control reported %s after an interrupted delivery", entry.Control.Status.Semantic)
			}
		}
	}
	if !found {
		t.Fatal("checkpoint must carry the interrupted acceptance")
	}
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(record); err != nil {
		t.Fatalf("hub-produced checkpoint after an interrupted delivery rejected: %v", err)
	}
	if _, err := hub.RetryControl(result.ID); err != nil {
		t.Fatalf("retry after an interrupted delivery: %v", err)
	}
	var redelivered *AcceptedBatch
	for i := published + 1; i < len(delivered); i++ {
		if delivered[i].HubSeq == interrupted.HubSeq {
			redelivered = &delivered[i]
		}
	}
	if redelivered == nil || interrupted.HubSeq == 0 || !bytes.Equal(redelivered.Batch, interrupted.Batch) {
		t.Fatalf("retry must redeliver the interrupted acceptance with its HubSeq: %+v", delivered[published:])
	}
	before := len(delivered)
	if _, err := hub.RetryControl(result.ID); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != before {
		t.Fatal("a delivered acceptance must not be redelivered on every retry")
	}
}

func TestRecoveryInterruptedControlAcceptanceSurvivesSupersession(t *testing.T) {
	var delivered []AcceptedBatch
	interrupt := false
	hooks := &RecoveryHooks{Accepted: func(record AcceptedBatch) {
		delivered = append(delivered, record)
		if interrupt {
			interrupt = false
			panic("synthetic storage interruption")
		}
	}}
	hub, handle := populatedRecoveryHub(t, hooks)
	published := len(delivered)
	interrupt = true
	result, err := hub.Fence(FenceRequest{Source: handle.Source(), Epoch: handle.Epoch(), Generation: handle.Generation(),
		Reason: "semhubtest.stop", Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.fence")}, ObservedAt: testWall(3), Monotonic: testMono(3)})
	if err == nil || len(delivered) != published+1 {
		t.Fatalf("fixture must interrupt the first control delivery: err=%v", err)
	}
	interrupted := delivered[published]
	op, err := hub.lookupControl(result.ID)
	if err != nil {
		t.Fatal(err)
	}
	// A newer operation supersedes the interrupted one, as when a successor
	// installs its own control while the callback is still running.
	hub.lock()
	op.src.last = &controlOp{src: op.src}
	hub.unlock()
	if _, err := hub.RetryControl(result.ID); !errors.Is(err, ErrControlRequest) {
		t.Fatalf("a superseded operation must refuse a retry: %v", err)
	}
	found := false
	for _, record := range delivered[published+1:] {
		if record.HubSeq == interrupted.HubSeq && bytes.Equal(record.Batch, interrupted.Batch) {
			found = true
		}
	}
	if !found {
		t.Fatal("a superseded operation must still redeliver its interrupted acceptance")
	}
}

func TestRecoveryAssetlessRestoreRunsOnceAndRechecksAdmission(t *testing.T) {
	origin, err := New(Options{Recovery: &RecoveryHooks{}})
	if err != nil {
		t.Fatal(err)
	}
	admitLegacy(t, origin, "source:assetless", "synthetic-family", "epoch:old", 1)
	record := origin.ExportRecovery()
	t.Run("second restore", func(t *testing.T) {
		fresh, err := New(Options{Recovery: &RecoveryHooks{}})
		if err != nil {
			t.Fatal(err)
		}
		if err := fresh.Restore(record); err != nil {
			t.Fatal(err)
		}
		if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
			t.Fatalf("a second restore of the same assetless source was accepted: %v", err)
		}
	})
	t.Run("admission during restore", func(t *testing.T) {
		fresh, err := New(Options{Recovery: &RecoveryHooks{}})
		if err != nil {
			t.Fatal(err)
		}
		fresh.faults = func(stage string, _ semreg.AssetID) {
			if stage == "restore-install" {
				fresh.faults = nil
				admitLegacy(t, fresh, "source:assetless", "synthetic-family", "epoch:racing", 1)
			}
		}
		if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
			t.Fatalf("restore installed history over a concurrent admission: %v", err)
		}
		if len(fresh.restoredEpochs) != 0 || len(fresh.restoredRegimes) != 0 {
			t.Fatal("rejected checkpoint reserved history or regimes")
		}
	})
}
