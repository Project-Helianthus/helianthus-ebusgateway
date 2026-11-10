//go:build linux || darwin

package semanticmetrics

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func slotFileFixture(t *testing.T, directory, target string) *SlotFileStore {
	t.Helper()
	s, err := NewSlotFileStore(directory, target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestSlotFileStoreLifecycle(t *testing.T) {
	dir := t.TempDir()
	p := slotFileFixture(t, dir, "target")
	if _, err := O21Open(p, "target"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, p.name+".json")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("open created document")
	}
	if _, err := O21Create(p, "target"); err != nil {
		t.Fatal(err)
	}
	raw, err := p.Load("target")
	if err != nil {
		t.Fatal(err)
	}
	raw[0] = '!'
	if _, err := O21Open(p, "target"); err != nil {
		t.Fatal("load buffer aliases persistence", err)
	}
	data := []byte(`{"Version":1,"Target":"target","Bindings":[]}`)
	if err := p.Commit("target", data); err != nil {
		t.Fatal(err)
	}
	data[0] = '!'
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Load("target"); !errors.Is(err, ErrSlotFileClosed) {
		t.Fatal(err)
	}
	if err := p.Commit("target", nil); !errors.Is(err, ErrSlotFileClosed) {
		t.Fatal(err)
	}
	q := slotFileFixture(t, dir, "target")
	s, err := O21Open(q, "target")
	if err != nil {
		t.Fatal(err)
	}
	if s.Export().Validated || len(s.Export().Bindings) != 0 {
		t.Fatal("restart trusted or changed document")
	}
	if _, err := O21Create(q, "target"); err == nil {
		t.Fatal("reprovisioned existing state")
	}
	other := slotFileFixture(t, dir, "other")
	if _, err := O21Create(other, "other"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Load("other"); err == nil {
		t.Fatal("cross-target read")
	}
	if err := q.Commit("other", nil); err == nil {
		t.Fatal("cross-target write")
	}
	if _, err := O21Open(q, "target"); err != nil {
		t.Fatal(err)
	}
}

func TestSlotFileStoreTargets(t *testing.T) {
	for _, target := range []string{"", ".", "..", "../target", "a/b", `a\b`, "a\x00b", strings.Repeat("a", 257), string([]byte{255})} {
		if p, err := NewSlotFileStore(t.TempDir(), target); err == nil {
			_ = p.Close()
			t.Fatalf("accepted target %q", target)
		}
	}
	dir := t.TempDir()
	if p, err := NewSlotFileStore(filepath.Join(dir, "missing"), "target"); err == nil {
		_ = p.Close()
		t.Fatal("created directory")
	}
	if err := os.Symlink(dir, filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if p, err := NewSlotFileStore(filepath.Join(dir, "link"), "target"); err == nil {
		_ = p.Close()
		t.Fatal("accepted symlink directory")
	}
	// Long opaque identities map to bounded filenames, without truncation.
	a := slotFileFixture(t, dir, strings.Repeat("a", 256))
	b := slotFileFixture(t, dir, strings.Repeat("a", 255)+"b")
	if a.name == b.name {
		t.Fatal("target mapping collision")
	}
}

func TestSlotFileStoreInvalidData(t *testing.T) {
	for _, kind := range []string{"corrupt", "directory", "symlink", "dangling", "fifo", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			p := slotFileFixture(t, t.TempDir(), "target")
			path := filepath.Join(p.directory, p.name+".json")
			var err error
			switch kind {
			case "corrupt":
				err = os.WriteFile(path, []byte("not json"), 0600)
			case "directory":
				err = os.Mkdir(path, 0700)
			case "symlink", "dangling":
				destination := filepath.Join(p.directory, "destination")
				if kind == "symlink" {
					if err = os.WriteFile(destination, []byte("{}"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				err = os.Symlink(destination, path)
			case "fifo":
				err = syscall.Mkfifo(path, 0600)
			case "oversized":
				err = os.WriteFile(path, make([]byte, o21MaxBytes+1), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = O21Open(p, "target"); err == nil || errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("not a definite non-absence: %v", err)
			}
			if _, err = O21Create(p, "target"); err == nil {
				t.Fatal("repaired invalid state")
			}
			if kind != "corrupt" {
				if err = p.Commit("target", []byte("replacement")); err == nil {
					t.Fatal("replaced unsafe data")
				}
			}
		})
	}
	p := slotFileFixture(t, t.TempDir(), "target")
	if err := p.Commit("target", make([]byte, o21MaxBytes+1)); err == nil {
		t.Fatal("oversize write")
	}
	if err := p.Commit("target", []byte("small")); err != nil {
		t.Fatal(err)
	}
	if raw, err := p.Load("target"); err != nil || string(raw) != "small" {
		t.Fatal(string(raw), err)
	}
}

func TestSlotFileStoreFaults(t *testing.T) {
	for _, failure := range []string{"write", "file-sync", "file-close", "rename", "directory-sync", "post-rename-fence"} {
		t.Run(failure, func(t *testing.T) {
			p := slotFileFixture(t, t.TempDir(), "target")
			s, err := O21Create(p, "target")
			if err != nil {
				t.Fatal(err)
			}
			_, registry := m36Fixture(t, "thermal.measurement.air_temperature", m36Quantity("0", "unit.celsius"))
			binding := registry.Bindings[0]
			before := s.Export()
			var calls []string
			p.fault = func(op string) error {
				calls = append(calls, op)
				if op == failure {
					return errors.New("synthetic filesystem failure")
				}
				if failure == "post-rename-fence" && op == "directory-sync" {
					return os.Remove(filepath.Join(p.directory, p.name+".lock"))
				}
				return nil
			}
			if _, err = s.Assign("target", binding); err == nil {
				t.Fatal("fault acknowledged")
			}
			if !reflect.DeepEqual(before.Bindings, s.Export().Bindings) || s.Export().Validated {
				t.Fatal("lost last accepted state")
			}
			if err = s.Revalidate("target", before.Bindings); err == nil {
				t.Fatal("uncertain store validated")
			}
			if _, err = p.Load("target"); err == nil {
				t.Fatal("uncertain adapter readable")
			}
			if err = p.Commit("target", nil); err == nil {
				t.Fatal("uncertain adapter writable")
			}
			order := []string{"write", "file-sync", "file-close", "rename", "directory-sync"}
			count := len(order)
			for i, op := range order {
				if op == failure {
					count = i + 1
				}
			}
			if !reflect.DeepEqual(calls, order[:count]) {
				t.Fatalf("order %v", calls)
			}
			if err = p.Close(); err != nil {
				t.Fatal(err)
			}
			q := slotFileFixture(t, p.directory, "target")
			restarted, err := O21Open(q, "target")
			if err != nil {
				t.Fatal(err)
			}
			expected := 0
			if failure == "directory-sync" || failure == "post-rename-fence" {
				expected = 1
			}
			if len(restarted.Export().Bindings) != expected || restarted.Export().Validated {
				t.Fatal("unexpected replacement result")
			}
			entries, err := os.ReadDir(p.directory)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.Contains(entry.Name(), ".tmp-") {
					t.Fatal("temporary file leaked")
				}
			}
		})
	}
}

func TestSlotFileStoreFencing(t *testing.T) {
	if dir := os.Getenv("SLOT_FILE_CONTENDER"); dir != "" {
		p, err := NewSlotFileStore(dir, "target")
		if err == nil {
			_ = p.Close()
			t.Fatal("contender acquired lock")
		}
		return
	}
	p := slotFileFixture(t, t.TempDir(), "target")
	if q, err := NewSlotFileStore(p.directory, "target"); err == nil {
		_ = q.Close()
		t.Fatal("second instance acquired lock")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSlotFileStoreFencing$")
	cmd.Env = append(os.Environ(), "SLOT_FILE_CONTENDER="+p.directory)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("contender %s: %v", out, err)
	}
	lockPath := filepath.Join(p.directory, p.name+".lock")
	if err := os.Rename(lockPath, lockPath+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lockPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Load("target"); !errors.Is(err, ErrSlotFileFencing) {
		t.Fatal(err)
	}
	if err := p.Commit("target", nil); err == nil {
		t.Fatal("lost fencing continued")
	}
}

func TestSlotFileStoreIdentity(t *testing.T) {
	changes := map[string]func(*Binding){
		"domain":            func(b *Binding) { b.Domain = "infrastructure" },
		"slot":              func(b *Binding) { b.Slot = 1 },
		"asset":             func(b *Binding) { b.AssetID = "asset:other" },
		"source":            func(b *Binding) { b.SourceID = "source:other" },
		"source_epoch":      func(b *Binding) { b.SourceEpochID = "epoch:other" },
		"native_binding":    func(b *Binding) { b.BindingID = "binding:other" },
		"driver_generation": func(b *Binding) { b.DriverGeneration = "2" },
		"service_instance":  func(b *Binding) { b.ServiceInstance = "service:other" },
		"dimension_id":      func(b *Binding) { b.Dimensions[0].ID = "thermal.dimension.other" },
		"dimension_value":   func(b *Binding) { *b.Dimensions[0].Value.Text = "other" },
		"selected_fields":   func(b *Binding) { b.Fields = []semreg.DefinitionID{"thermal.measurement.water_temperature"} },
		"retired":           func(b *Binding) { b.Retired = true },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			p := slotFileFixture(t, t.TempDir(), "target")
			s, err := O21Create(p, "target")
			if err != nil {
				t.Fatal(err)
			}
			_, r := m36Fixture(t, "thermal.measurement.air_temperature", m36Quantity("0", "unit.celsius"))
			b := r.Bindings[0]
			if _, err = s.Assign("target", b); err != nil {
				t.Fatal(err)
			}
			if err = p.Close(); err != nil {
				t.Fatal(err)
			}
			q := slotFileFixture(t, p.directory, "target")
			s, err = O21Open(q, "target")
			if err != nil {
				t.Fatal(err)
			}
			before, err := q.Load("target")
			if err != nil {
				t.Fatal(err)
			}
			changed := o21Clone(b)
			change(&changed)
			if err = s.Revalidate("target", []Binding{changed}); err == nil {
				t.Fatal("changed identity validated")
			}
			if _, err = s.Retire("target", changed); err == nil {
				t.Fatal("changed identity retired")
			}
			after, err := q.Load("target")
			if err != nil || string(before) != string(after) {
				t.Fatal("rejected identity wrote state", err)
			}
			if err = s.Revalidate("other", []Binding{b}); err == nil {
				t.Fatal("cross-target validation")
			}
			if _, err = s.Assign("other", b); err == nil {
				t.Fatal("cross-target assignment")
			}
			if _, err = s.Retire("other", b); err == nil {
				t.Fatal("cross-target retirement")
			}
			if err = s.Revalidate("target", []Binding{b}); err != nil {
				t.Fatal(err)
			}
			if _, err = s.Retire("target", b); err != nil {
				t.Fatal(err)
			}
			if _, err = s.Assign("target", b); err == nil {
				t.Fatal("tombstone reused")
			}
			if err = q.Close(); err != nil {
				t.Fatal(err)
			}
			restarted := slotFileFixture(t, p.directory, "target")
			store, err := O21Open(restarted, "target")
			if err != nil {
				t.Fatal(err)
			}
			if !store.Export().Bindings[0].Retired || store.Export().Validated {
				t.Fatal("lost tombstone or trusted restart")
			}
		})
	}
}

func TestSlotFileStoreAcknowledgementAndReadErrors(t *testing.T) {
	p := slotFileFixture(t, t.TempDir(), "target")
	var calls []string
	p.fault = func(op string) error { calls = append(calls, op); return nil }
	if err := p.Commit("target", []byte("accepted")); err != nil {
		t.Fatal(err)
	}
	want := []string{"write", "file-sync", "file-close", "rename", "directory-sync"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("acknowledgement order: %v", calls)
	}
	for _, readErr := range []error{syscall.EACCES, syscall.EIO, fs.ErrClosed} {
		p.fault = func(op string) error {
			if op == "read" {
				return readErr
			}
			return nil
		}
		if _, err := p.Load("target"); !errors.Is(err, readErr) || errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("read error reclassified: %v", err)
		}
		if _, err := O21Create(p, "target"); err == nil {
			t.Fatal("read error permitted reprovisioning")
		}
	}
	p.fault = nil
	if raw, err := p.Load("target"); err != nil || string(raw) != "accepted" {
		t.Fatal("read failure changed state", err)
	}
}

func TestSlotFileStoreDirectoryFencing(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "state")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	p := slotFileFixture(t, dir, "target")
	if err := os.Rename(dir, dir+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit("target", []byte("rejected")); !errors.Is(err, ErrSlotFileFencing) {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("wrote replacement directory", err)
	}
	if _, err := p.Load("target"); err == nil {
		t.Fatal("directory fencing recovered implicitly")
	}
}

func TestSlotFileStoreCapacityAndDetachedExports(t *testing.T) {
	p := slotFileFixture(t, t.TempDir(), "target")
	s, err := O21Create(p, "target")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"thermal.measurement.air_temperature", "infrastructure.ac.voltage"} {
		unit := "unit.celsius"
		if field == "infrastructure.ac.voltage" {
			unit = "unit.volt"
		}
		_, registry := m36Fixture(t, field, m36Quantity("0", unit))
		for slot := 0; slot < 16; slot++ {
			b := o21Clone(registry.Bindings[0])
			b.Slot = slot
			b.AssetID = semreg.AssetID(fmt.Sprintf("asset:%s-%d", b.Domain, slot))
			exported, err := s.Assign("target", b)
			if err != nil {
				t.Fatal(err)
			}
			if exported.Validated {
				t.Fatal("assignment inherited validation")
			}
			exported.Bindings[0].Fields[0] = "thermal.mode.system"
			if s.Export().Bindings[0].Fields[0] == "thermal.mode.system" {
				t.Fatal("export aliases store")
			}
			if _, err = s.Retire("target", b); err != nil {
				t.Fatal(err)
			}
		}
		extra := o21Clone(registry.Bindings[0])
		extra.Slot = 16
		extra.AssetID = "asset:extra"
		if _, err = s.Assign("target", extra); err == nil {
			t.Fatal("capacity exceeded")
		}
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	q := slotFileFixture(t, p.directory, "target")
	restarted, err := O21Open(q, "target")
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted.Export().Bindings) != 32 || restarted.Export().Validated {
		t.Fatal("capacity lost on restart")
	}
	for _, b := range restarted.Export().Bindings {
		if !b.Retired {
			t.Fatal("tombstone lost")
		}
	}
}
