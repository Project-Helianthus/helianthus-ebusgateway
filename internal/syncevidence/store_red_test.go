package syncevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func storeTestRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func negativeBundleBytes(t *testing.T, started time.Time) []byte {
	t.Helper()
	reader := eebusReaderFunc(func(context.Context, SourceRequest) (AcquiredEvidence, error) {
		return AcquiredEvidence{}, ErrBackendUnavailable
	})
	recorder := testRecorder(t, []RegisteredSource{eebusRegistration(reader, PhasePre, "store-runtime")}, func(options *RecorderOptions) {
		options.Clock = &redClock{wall: started}
	})
	bundle, err := recorder.Capture(context.Background(), ActionMarker{EvidenceRef: redEvidenceRef('1')})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func bundleIDFromBytes(t *testing.T, bundle []byte) string {
	t.Helper()
	var decoded struct {
		BundleID string `json:"bundle_id"`
	}
	if err := json.Unmarshal(bundle, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded.BundleID
}

func TestMSP065StoreReservationPublishAccountingAndIdempotentDrop(t *testing.T) {
	root := storeTestRoot(t)
	store, err := OpenFileStore(FileStoreConfig{Root: root, QuotaBytes: 1 << 20})
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	bundle := negativeBundleBytes(t, time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	reservation, err := store.ReserveCapture(int64(len(bundle) + 1024))
	if err != nil {
		t.Fatalf("ReserveCapture: %v", err)
	}
	usedBefore, reserved, err := store.Usage()
	if err != nil || usedBefore < 32 || reserved < int64(len(bundle)) {
		t.Fatalf("usage before publish=%d/%d err=%v", usedBefore, reserved, err)
	}
	id, err := reservation.Publish(bundle)
	if err != nil {
		t.Fatalf("reserved Publish: %v", err)
	}
	if id != bundleIDFromBytes(t, bundle) {
		t.Fatalf("id=%q", id)
	}
	info, err := os.Stat(filepath.Join(root, id+".json"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("bundle stat=%v err=%v", info, err)
	}
	usedAfter, reserved, err := store.Usage()
	if err != nil || usedAfter != usedBefore+int64(len(bundle)) || reserved != 0 {
		t.Fatalf("usage after publish=%d/%d err=%v", usedAfter, reserved, err)
	}
	if _, err := store.Publish(bundle); !errors.Is(err, ErrBundleExists) {
		t.Fatalf("duplicate Publish error=%v", err)
	}
	invalid := append([]byte(nil), bundle...)
	invalid[len(invalid)-2] ^= 1
	if _, err := store.Publish(invalid); err == nil {
		t.Fatal("Publish accepted a bundle without full verification")
	}
	if status, err := store.Drop(id); err != nil || status != DropStatusDropped {
		t.Fatalf("Drop #1=%q %v", status, err)
	}
	if status, err := store.Drop(id); err != nil || status != DropStatusAlreadyGone {
		t.Fatalf("Drop #2=%q %v", status, err)
	}
}

func TestMSP065StoreWalksAllComponentsNoFollowAndLocksOneWriter(t *testing.T) {
	parent := storeTestRoot(t)
	target := filepath.Join(parent, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFileStore(FileStoreConfig{Root: filepath.Join(link, "child"), QuotaBytes: 1 << 20}); !errors.Is(err, ErrUnsafeStore) {
		t.Fatalf("symlink component error=%v", err)
	}
	store, err := OpenFileStore(FileStoreConfig{Root: target, QuotaBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if second, err := OpenFileStore(FileStoreConfig{Root: target, QuotaBytes: 1 << 20}); !errors.Is(err, ErrStoreLocked) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second writer error=%v", err)
	}
}

func TestMSP065DropDeletesOnlyTheOpenedInode(t *testing.T) {
	root := storeTestRoot(t)
	store, err := OpenFileStore(FileStoreConfig{Root: root, QuotaBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	bundle := negativeBundleBytes(t, time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	id, err := store.Publish(bundle)
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(root, id+".json")
	saved := filepath.Join(root, "saved-opened-inode")
	store.beforeDropRename = func() {
		if err := os.Rename(name, saved); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte("replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Drop(id); !errors.Is(err, ErrUnsafeStore) {
		t.Fatalf("raced Drop error=%v", err)
	}
	got, err := os.ReadFile(saved)
	if err != nil || !bytes.Equal(got, bundle) {
		t.Fatalf("opened inode was removed/changed err=%v", err)
	}
}

func TestMSP065PublishJournalRecoversDurableCommit(t *testing.T) {
	root := storeTestRoot(t)
	store, err := OpenFileStore(FileStoreConfig{Root: root, QuotaBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	bundle := negativeBundleBytes(t, time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	id := bundleIDFromBytes(t, bundle)
	stage := stagingPrefix + strings.Repeat("a", 32)
	journal := journalPrefix + strings.Repeat("b", 32)
	journalBytes, err := canonicalMarshal(journalRecord{Operation: "PUBLISH", Stage: stage, Final: id + ".json"}, DefaultLimitsV1(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, stage), bundle, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, journal), journalBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenFileStore(FileStoreConfig{Root: root, QuotaBytes: 1 << 20})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	got, err := os.ReadFile(filepath.Join(root, id+".json"))
	if err != nil || !bytes.Equal(got, bundle) {
		t.Fatalf("recovered bundle err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, journal)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("journal retained err=%v", err)
	}
}

func TestMSP065DropJournalRecoveryCompletesDeletion(t *testing.T) {
	root := storeTestRoot(t)
	store, err := OpenFileStore(FileStoreConfig{Root: root, QuotaBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	bundle := negativeBundleBytes(t, time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	id, err := store.Publish(bundle)
	if err != nil {
		t.Fatal(err)
	}
	journal := journalPrefix + strings.Repeat("d", 32)
	tomb := dropPrefix + strings.Repeat("e", 32)
	journalBytes, err := canonicalMarshal(journalRecord{Operation: "DROP", Final: id + ".json", Tomb: tomb}, DefaultLimitsV1(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, journal), journalBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenFileStore(FileStoreConfig{Root: root, QuotaBytes: 1 << 20})
	if err != nil {
		t.Fatalf("recover drop: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	if _, err := os.Stat(filepath.Join(root, id+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("drop recovery retained final err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, journal)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("drop recovery retained journal err=%v", err)
	}
}

func TestMSP065RetentionRemovesOnlyExpiredCompleteBundles(t *testing.T) {
	root := storeTestRoot(t)
	store, err := OpenFileStore(FileStoreConfig{Root: root, QuotaBytes: 1 << 20, Retention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	oldBundle := negativeBundleBytes(t, time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC))
	newBundle := negativeBundleBytes(t, time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC))
	oldID, err := store.Publish(oldBundle)
	if err != nil {
		t.Fatal(err)
	}
	newID, err := store.Publish(newBundle)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := store.EnforceRetention(time.Date(2026, 7, 19, 12, 30, 0, 0, time.UTC))
	if err != nil || dropped != 1 {
		t.Fatalf("retention=%d err=%v", dropped, err)
	}
	if _, err := os.Stat(filepath.Join(root, oldID+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired bundle still present err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, newID+".json")); err != nil {
		t.Fatalf("retained bundle missing err=%v", err)
	}
}

func TestMSP065QuotaAccountsForAllFilesAndReservesBeforeAcquisition(t *testing.T) {
	root := storeTestRoot(t)
	quarantine := filepath.Join(root, quarantinePrefix+strings.Repeat("c", 32))
	if err := os.WriteFile(quarantine, bytes.Repeat([]byte("q"), 8192), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := OpenFileStore(FileStoreConfig{Root: root, QuotaBytes: 16384})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	used, _, err := store.Usage()
	if err != nil || used < 8192+32 {
		t.Fatalf("usage=%d err=%v", used, err)
	}
	if _, err := store.ReserveCapture(8192); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("reservation error=%v", err)
	}
	reservation, err := store.ReserveCapture(1024)
	if err != nil {
		t.Fatal(err)
	}
	_, reserved, _ := store.Usage()
	if reserved != 1024+journalReserveBytes {
		t.Fatalf("reserved=%d", reserved)
	}
	reservation.Release()
}
