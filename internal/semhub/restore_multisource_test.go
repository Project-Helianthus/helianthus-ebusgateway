package semhub

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// secondSource is a source that a version 1 recovery record cannot name next
// to an asset's single source.
const secondSource semreg.SourceID = "source:second"

// enableRecovery switches the recovery seam on for a hub that was created
// without it, so a hub that already holds a shared asset can be exported.
func enableRecovery(hub *Hub) {
	hub.mu.Lock()
	hub.recovery = &RecoveryHooks{}
	hub.mu.Unlock()
}

// fixedRecoveryHub is populatedRecoveryHub with a fixed incarnation and a
// fenced generation, so its export carries a control and is reproducible.
func fixedRecoveryHub(t *testing.T) *Hub {
	t.Helper()
	hub, err := New(Options{Recovery: &RecoveryHooks{}})
	if err != nil {
		t.Fatal(err)
	}
	hub.incarnation = "incarnation:fixed"
	for _, id := range recoveryAssets {
		mustDeclare(t, hub, id, "source:restore", "synthetic-family", nil)
	}
	handle := admitLegacy(t, hub, "source:restore", "synthetic-family", "epoch:restore", 1)
	for _, id := range recoveryAssets {
		mustPublish(t, hub, handle, id, "synthetic", 10, 1)
		mustPublish(t, hub, handle, id, "synthetic", 20, 2)
	}
	if err := fenceRecovery(t, hub, handle); err != nil {
		t.Fatal(err)
	}
	return hub
}

// cloneRecovery detaches a record through its persisted form.
func cloneRecovery(t *testing.T, record RecoveryRecord) RecoveryRecord {
	t.Helper()
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var out RecoveryRecord
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// resealedWithSource re-encodes an accepted batch under another source with a
// valid digest, so only the source differs from the original.
func resealedWithSource(t *testing.T, raw []byte, src semreg.SourceID) []byte {
	t.Helper()
	var batch semreg.PublicationBatch
	if err := json.Unmarshal(raw, &batch); err != nil {
		t.Fatal(err)
	}
	batch.SourceID = src
	digest, err := batch.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	batch.BatchDigest = digest
	out, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// jsonKeys returns the sorted top-level keys of one encoded object.
func jsonKeys(t *testing.T, raw []byte) []string {
	t.Helper()
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// requireClosedRefusal checks that a refused restore closed every declared
// asset and installed no ownership, regime, epoch or admission.
func requireClosedRefusal(t *testing.T, fresh *Hub, before map[string]idOwner, ids []semreg.AssetID, err error) {
	t.Helper()
	if !errors.Is(err, ErrRecoveryMultiSource) || !errors.Is(err, ErrRestore) {
		t.Fatalf("restore=%v, want the multi-source refusal", err)
	}
	if !sameClaims(before, claimsSnapshot(fresh)) {
		t.Fatal("the refusal changed identifier ownership")
	}
	fresh.mu.RLock()
	installed := len(fresh.restoredEpochs) != 0 || len(fresh.restoredRegimes) != 0 || len(fresh.epochHistory) != 0
	for _, src := range fresh.sources {
		installed = installed || len(src.epochs) != 0 || src.admission != 0
	}
	closed := true
	for _, id := range ids {
		target := fresh.assets[id]
		installed = installed || target.hubSeq != 0 || len(target.records) != 0 || target.restored
		closed = closed && target.restoreFailed
	}
	fresh.mu.RUnlock()
	if installed {
		t.Fatal("the refusal installed restored state")
	}
	if !closed {
		t.Fatal("the refusal left a declared asset open")
	}
	for _, id := range ids {
		_ = fresh.Resettle(id)
		if view, _ := fresh.Inspect(id); view.Present || fresh.Readable(id) || fresh.ReadAdmitted(id) || fresh.Settled(id) {
			t.Fatalf("the refusal opened %s", id)
		}
	}
}

// TestRestoreMultiSourceExportRefusesSharedAsset exports a recovery-enabled
// hub holding an asset with a declared contributor next to a single-source
// asset. A version 1 record holds one source per asset, so the export fails
// closed with the named error and yields the zero record, which no restore
// accepts, whether or not the contributor has published.
func TestRestoreMultiSourceExportRefusesSharedAsset(t *testing.T) {
	for _, published := range []bool{false, true} {
		name := "declared"
		if published {
			name = "published"
		}
		t.Run(name, func(t *testing.T) {
			hub := newTestHub(t)
			mustDeclareShared(t, hub)
			mustDeclare(t, hub, "asset:c", sharedOther, otherDriver, nil)
			if published {
				owner := mustAdmit(t, hub, sharedOwner, sharedDriver)
				peer := mustAdmit(t, hub, sharedPeer, peerDriver)
				mustPublish(t, hub, owner, sharedAsset, "fam_a", 10, 1)
				mustPublish(t, hub, peer, sharedAsset, "fam_b", 11, 2)
			}
			enableRecovery(hub)
			record, err := hub.ExportRecoveryChecked()
			if !errors.Is(err, ErrRecoveryMultiSource) {
				t.Fatalf("export=%v, want the multi-source refusal", err)
			}
			if !reflect.DeepEqual(record, RecoveryRecord{}) {
				t.Fatalf("a refused export returned a checkpoint: version=%d assets=%d", record.Version, len(record.Assets))
			}
			if plain := hub.ExportRecovery(); !reflect.DeepEqual(plain, RecoveryRecord{}) {
				t.Fatalf("ExportRecovery returned a checkpoint: version=%d assets=%d", plain.Version, len(plain.Assets))
			}
			fresh := recoveryHub(t, nil, nil)
			if err := fresh.Restore(record); !errors.Is(err, ErrRestore) {
				t.Fatalf("the zero record restored: %v", err)
			}
		})
	}
}

// TestRestoreMultiSourceRestoreRefusesSecondSource restores a version 1
// record in which one asset names a second source next to its own. Each place
// a record can name a source for an asset is varied on its own:
//   - entry: a second, empty entry for the asset under another source;
//   - accepted: an accepted batch of the asset sealed under another source;
//   - control: the retained control's identity under another source;
//   - epoch: an epoch event of another source that covers the asset;
//   - declared: the destination hub declares a contributor on the asset.
//
// Each is refused as a whole with the named error, closes every declared
// asset and installs nothing, while the unmodified record restores.
func TestRestoreMultiSourceRestoreRefusesSecondSource(t *testing.T) {
	origin := fixedRecoveryHub(t)
	base, err := origin.ExportRecoveryChecked()
	if err != nil {
		t.Fatal(err)
	}
	target := recoveryAssets[0]
	entryOf := func(record *RecoveryRecord) *RecoveryAsset {
		for i := range record.Assets {
			if record.Assets[i].Asset == target {
				return &record.Assets[i]
			}
		}
		t.Fatalf("no entry for %s", target)
		return nil
	}
	mutations := map[string]func(*RecoveryRecord){
		"entry": func(record *RecoveryRecord) {
			// The second entry carries no log and no control, so only its
			// source names the second source.
			own := entryOf(record)
			record.Assets = append(record.Assets, RecoveryAsset{Asset: own.Asset, Source: secondSource, Driver: own.Driver, Packs: own.Packs})
		},
		"accepted": func(record *RecoveryRecord) {
			entry := entryOf(record)
			entry.Accepted[0].Batch = resealedWithSource(t, entry.Accepted[0].Batch, secondSource)
		},
		"control": func(record *RecoveryRecord) {
			entry := entryOf(record)
			if entry.Control == nil || entry.Control.Status.ID == nil {
				t.Fatal("fixture lacks a retained control")
			}
			id := *entry.Control.Status.ID
			id.Source = secondSource
			entry.Control.Status.ID = &id
		},
		"epoch": func(record *RecoveryRecord) {
			record.Epochs = append(record.Epochs, EpochRecord{Incarnation: "incarnation:fixed", Source: secondSource, Epoch: "epoch:second",
				Generation: 1, Kind: EventGenerationFenced, Assets: []semreg.AssetID{target}})
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			record := cloneRecovery(t, base)
			mutate(&record)
			fresh := recoveryHub(t, nil, nil)
			before := claimsSnapshot(fresh)
			requireClosedRefusal(t, fresh, before, recoveryAssets, fresh.Restore(record))
		})
	}
	t.Run("declared", func(t *testing.T) {
		single, err := New(Options{Recovery: &RecoveryHooks{}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := single.Declare(sharedSpec()); err != nil {
			t.Fatal(err)
		}
		owner := mustAdmit(t, single, sharedOwner, sharedDriver)
		mustPublish(t, single, owner, sharedAsset, "fam_a", 10, 1)
		record, err := single.ExportRecoveryChecked()
		if err != nil {
			t.Fatal(err)
		}
		fresh := newTestHub(t)
		mustDeclareShared(t, fresh)
		enableRecovery(fresh)
		before := claimsSnapshot(fresh)
		requireClosedRefusal(t, fresh, before, []semreg.AssetID{sharedAsset}, fresh.Restore(record))
		// The same record restores where the asset has its single source.
		plain, err := New(Options{Recovery: &RecoveryHooks{}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := plain.Declare(sharedSpec()); err != nil {
			t.Fatal(err)
		}
		if err := plain.Restore(record); err != nil {
			t.Fatalf("single-source destination refused: %v", err)
		}
	})
	t.Run("unmodified", func(t *testing.T) {
		fresh := recoveryHub(t, nil, nil)
		if err := fresh.Restore(cloneRecovery(t, base)); err != nil {
			t.Fatalf("unmodified record refused: %v", err)
		}
	})
}

// TestRestoreMultiSourceSingleSourceVersionOneUnchanged locks the version 1
// encoding of a single-source hub: both export entry points yield the same
// bytes, the record keeps exactly the version 1 fields, and a restored hub
// exports the same bytes again once its assets are reprojected.
func TestRestoreMultiSourceSingleSourceVersionOneUnchanged(t *testing.T) {
	hub := fixedRecoveryHub(t)
	plain := hub.ExportRecovery()
	checked, err := hub.ExportRecoveryChecked()
	if err != nil {
		t.Fatalf("single-source export refused: %v", err)
	}
	raw, err := json.Marshal(plain)
	if err != nil {
		t.Fatal(err)
	}
	again, err := json.Marshal(checked)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, again) {
		t.Fatal("the export entry points disagree")
	}
	if plain.Version != 1 || len(plain.Assets) != len(recoveryAssets) {
		t.Fatalf("version=%d assets=%d", plain.Version, len(plain.Assets))
	}
	want := map[string][]string{
		"record":  {"Assets", "Drivers", "Epochs", "Regimes", "Unfixed", "Version"},
		"asset":   {"Accepted", "Asset", "CommitCount", "Control", "Driver", "Failed", "HubSeq", "Packs", "Revisions", "SnapshotID", "Source"},
		"control": {"Abandoned", "Attempts", "Epoch", "Generation", "Kind", "Monotonic", "Status"},
	}
	var shape struct {
		Assets []json.RawMessage
	}
	if err := json.Unmarshal(raw, &shape); err != nil {
		t.Fatal(err)
	}
	if got := jsonKeys(t, raw); !reflect.DeepEqual(got, want["record"]) {
		t.Fatalf("record fields=%v", got)
	}
	for _, entry := range shape.Assets {
		if got := jsonKeys(t, entry); !reflect.DeepEqual(got, want["asset"]) {
			t.Fatalf("asset fields=%v", got)
		}
		var control struct{ Control json.RawMessage }
		if err := json.Unmarshal(entry, &control); err != nil {
			t.Fatal(err)
		}
		if got := jsonKeys(t, control.Control); !reflect.DeepEqual(got, want["control"]) {
			t.Fatalf("control fields=%v", got)
		}
	}
	fresh := recoveryHub(t, nil, func(ControlEvent) error { return nil })
	if err := fresh.Restore(cloneRecovery(t, plain)); err != nil {
		t.Fatal(err)
	}
	// A restored settlement is never current: each asset needs a fresh
	// reprojection before its control reports settled again.
	for _, id := range recoveryAssets {
		if err := fresh.Resettle(id); err != nil {
			t.Fatal(err)
		}
	}
	reexported, err := json.Marshal(fresh.ExportRecovery())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, reexported) {
		t.Fatal("a restored single-source hub exports other bytes")
	}
}
