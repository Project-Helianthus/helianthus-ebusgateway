package runtimestate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
)

func TestRenameEXDEVClassification(t *testing.T) {
	pathError := &os.PathError{Op: "rename", Path: "temporary", Err: syscall.EXDEV}
	linkError := &os.LinkError{Op: "rename", Old: "temporary", New: "state", Err: syscall.EXDEV}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil"},
		{name: "bare", err: syscall.EXDEV, want: true},
		{name: "path", err: pathError, want: true},
		{name: "link", err: linkError, want: true},
		{name: "wrapped_bare", err: fmt.Errorf("persist: %w", syscall.EXDEV), want: true},
		{name: "wrapped_path", err: fmt.Errorf("persist: %w", pathError), want: true},
		{name: "wrapped_link", err: fmt.Errorf("persist: %w", fmt.Errorf("rename: %w", linkError)), want: true},
		{name: "inner_wrap", err: &os.PathError{Op: "rename", Path: "temporary", Err: fmt.Errorf("filesystem: %w", syscall.EXDEV)}, want: true},
		{name: "other_errno", err: syscall.EACCES},
		{name: "other_path", err: &os.PathError{Op: "rename", Path: "temporary", Err: syscall.EACCES}},
		{name: "other_link", err: &os.LinkError{Op: "rename", Old: "temporary", New: "state", Err: syscall.EACCES}},
		{name: "wrapped_other", err: fmt.Errorf("persist: %w", syscall.EACCES)},
		{name: "same_message", err: errors.New(syscall.EXDEV.Error())},
		{name: "opaque_format", err: fmt.Errorf("persist: %v", syscall.EXDEV)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isExdev(tc.err); got != tc.want {
				t.Errorf("isExdev(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

type renameEXDEVHooks struct {
	DefaultFilesystemHooks
	renameErr error
	calls     []string
	oldPath   string
	newPath   string
	unlinked  string
	unlinkErr error
}

func (h *renameEXDEVHooks) Rename(oldPath, newPath string) error {
	h.calls = append(h.calls, "rename")
	h.oldPath, h.newPath = oldPath, newPath
	return h.renameErr
}

func (h *renameEXDEVHooks) Unlink(path string) error {
	h.calls = append(h.calls, "unlink")
	h.unlinked = path
	h.unlinkErr = h.DefaultFilesystemHooks.Unlink(path)
	return h.unlinkErr
}

func (h *renameEXDEVHooks) FsyncDir(path string) error {
	h.calls = append(h.calls, "fsync_dir")
	return h.DefaultFilesystemHooks.FsyncDir(path)
}

type renameEXDEVMetrics struct {
	NopMetrics
	reasons []string
}

func (m *renameEXDEVMetrics) OnWrite(reason string) {
	m.reasons = append(m.reasons, reason)
}

func TestRenameEXDEVPersist(t *testing.T) {
	for _, tc := range []struct {
		name   string
		errno  syscall.Errno
		wrap   bool
		reason string
	}{
		{name: "link", errno: syscall.EXDEV, reason: "rename_exdev"},
		{name: "wrapped_link", errno: syscall.EXDEV, wrap: true, reason: "rename_exdev"},
		{name: "non_exdev_link", errno: syscall.EACCES, reason: "rename"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "state.json")
			oldBytes := []byte(`{"schema_version":1,"meta":{"instance_guid":"synthetic-old"}}`)
			if err := os.WriteFile(path, oldBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			var injected error = &os.LinkError{Op: "rename", Old: path + ".tmp", New: path, Err: tc.errno}
			if tc.wrap {
				injected = fmt.Errorf("persist: %w", injected)
			}
			hooks := &renameEXDEVHooks{renameErr: injected}
			metrics := &renameEXDEVMetrics{}
			mgr := New(Options{Path: path, FsHooks: hooks, Metrics: metrics})
			if err := mgr.EagerPersistInstanceGUID(context.Background(), "synthetic-new", IdentitySourceGenerated); err != injected {
				t.Errorf("persist error = %v, want injected error %v", err, injected)
			}
			if !reflect.DeepEqual(metrics.reasons, []string{tc.reason}) {
				t.Errorf("write reasons = %v, want exactly [%s]", metrics.reasons, tc.reason)
			}
			if !reflect.DeepEqual(hooks.calls, []string{"rename", "unlink"}) {
				t.Errorf("hook calls = %v, want [rename unlink]", hooks.calls)
			}
			if hooks.oldPath != path+".tmp" || hooks.newPath != path || hooks.unlinked != path+".tmp" || hooks.unlinkErr != nil {
				t.Errorf("unexpected rename/cleanup: %+v", hooks)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, oldBytes) {
				t.Errorf("old bytes changed: got %q, want %q", got, oldBytes)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 || entries[0].Name() != "state.json" {
				t.Errorf("directory after cleanup = %v, want only state.json", entries)
			}
		})
	}
}
