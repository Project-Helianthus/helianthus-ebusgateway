package syncevidence

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMSP065StorePublishesImmutableFileAndDropIsIdempotent(t *testing.T) {
	root := t.TempDir()
	store, err := OpenFileStore(FileStoreConfig{Root: root, QuotaBytes: 1 << 20})
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	bundle := []byte(`{"bundle_id":"sebv1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)
	id, err := store.Publish(bundle)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if id != "sebv1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("id = %q", id)
	}
	info, err := os.Stat(filepath.Join(root, id+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("bundle mode = %o, want 600", info.Mode().Perm())
	}
	if _, err := store.Publish(bundle); !errors.Is(err, ErrBundleExists) {
		t.Fatalf("duplicate Publish error = %v, want ErrBundleExists", err)
	}
	if status, err := store.Drop(id); err != nil || status != DropStatusDropped {
		t.Fatalf("Drop #1 = %q, %v", status, err)
	}
	if status, err := store.Drop(id); err != nil || status != DropStatusAlreadyGone {
		t.Fatalf("Drop #2 = %q, %v", status, err)
	}
}

func TestMSP065StoreRejectsSymlinkAndConcurrentWriter(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFileStore(FileStoreConfig{Root: link, QuotaBytes: 1 << 20}); !errors.Is(err, ErrUnsafeStore) {
		t.Fatalf("symlink store error = %v, want ErrUnsafeStore", err)
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
		t.Fatalf("second writer error = %v, want ErrStoreLocked", err)
	}
}
