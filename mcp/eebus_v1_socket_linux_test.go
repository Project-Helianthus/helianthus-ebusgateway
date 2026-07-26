//go:build linux

package mcp

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestIssue743LinuxOperatorParentRejectsSymlinkComponents(t *testing.T) {
	server, _ := issue743Server(t)
	root := issue743LinuxTempRoot(t)
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "linked")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(link, "nested", "operator.sock")
	closer, err := server.eebusV1ServeOperator(context.Background(), socketPath, func(net.Conn) (int, error) {
		return os.Geteuid(), nil
	})
	if err == nil {
		_ = closer.Close()
		t.Fatal("operator endpoint followed a symlink parent component")
	}
	if _, err := os.Lstat(filepath.Join(target, "nested", "operator.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink target was modified: %v", err)
	}
}

func TestIssue743LinuxOperatorCleanupStaysAnchoredToPinnedParent(t *testing.T) {
	server, _ := issue743Server(t)
	root := issue743LinuxTempRoot(t)
	parent := filepath.Join(root, "eebus")
	socketPath := filepath.Join(parent, "operator.sock")
	closer, err := server.eebusV1ServeOperator(context.Background(), socketPath, func(net.Conn) (int, error) {
		return os.Geteuid(), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	moved := filepath.Join(root, "eebus-pinned")
	if err := os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	attacker := filepath.Join(root, "attacker")
	if err := os.Mkdir(attacker, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(attacker, parent); err != nil {
		t.Fatal(err)
	}
	replacementPath := filepath.Join(attacker, "operator.sock")
	replacement := issue743LinuxListenUnix(t, replacementPath)
	defer func() {
		_ = replacement.Close()
		_ = os.Remove(replacementPath)
	}()

	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(moved, "operator.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pinned original socket was not removed: %v", err)
	}
	if info, err := os.Lstat(replacementPath); err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("cleanup escaped to replacement parent: info=%v err=%v", info, err)
	}
}

func TestIssue743LinuxOperatorCleanupPreservesReplacementSocketIdentity(t *testing.T) {
	server, _ := issue743Server(t)
	root := issue743LinuxTempRoot(t)
	socketPath := filepath.Join(root, "eebus", "operator.sock")
	closer, err := server.eebusV1ServeOperator(context.Background(), socketPath, func(net.Conn) (int, error) {
		return os.Geteuid(), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(socketPath); err != nil {
		t.Fatal(err)
	}
	replacement := issue743LinuxListenUnix(t, socketPath)
	defer func() {
		_ = replacement.Close()
		_ = os.Remove(socketPath)
	}()

	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(socketPath); err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("identity-checked cleanup removed replacement socket: info=%v err=%v", info, err)
	}
}

func TestIssue743LinuxStaleRemovalRequiresECONNREFUSED(t *testing.T) {
	root := issue743LinuxTempRoot(t)
	parent, err := eebusV1OpenPinnedParent(filepath.Join(root, "eebus", "operator.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(parent.fd) }()
	stale := issue743LinuxListenUnix(t, parent.anchoredPath())
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}

	for name, dialErr := range map[string]error{
		"timeout":   context.DeadlineExceeded,
		"backlog":   syscall.EAGAIN,
		"transient": syscall.EINTR,
	} {
		t.Run(name, func(t *testing.T) {
			before, err := parent.statChild()
			if err != nil {
				t.Fatal(err)
			}
			err = parent.removeStale(func(string, time.Duration) (net.Conn, error) {
				return nil, dialErr
			})
			if err == nil {
				t.Fatalf("%s dial failure removed socket", name)
			}
			after, statErr := parent.statChild()
			if statErr != nil || after != before {
				t.Fatalf("%s changed socket identity: before=%+v after=%+v err=%v", name, before, after, statErr)
			}
		})
	}
	if err := parent.removeStale(func(string, time.Duration) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "unix", Err: syscall.ECONNREFUSED}
	}); err != nil {
		t.Fatalf("ECONNREFUSED did not confirm stale socket: %v", err)
	}
	if _, err := parent.statChild(); !errors.Is(err, unix.ENOENT) {
		t.Fatalf("confirmed stale socket remains: %v", err)
	}
}

func TestIssue743LinuxStaleRemovalRevalidatesSocketInode(t *testing.T) {
	root := issue743LinuxTempRoot(t)
	parent, err := eebusV1OpenPinnedParent(filepath.Join(root, "eebus", "operator.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(parent.fd) }()
	stale := issue743LinuxListenUnix(t, parent.anchoredPath())
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}

	var replacement *net.UnixListener
	err = parent.removeStale(func(string, time.Duration) (net.Conn, error) {
		if removeErr := unix.Unlinkat(parent.fd, parent.child, 0); removeErr != nil {
			return nil, removeErr
		}
		replacement = issue743LinuxListenUnix(t, parent.anchoredPath())
		if closeErr := replacement.Close(); closeErr != nil {
			return nil, closeErr
		}
		return nil, syscall.ECONNREFUSED
	})
	if err == nil {
		t.Fatal("stale removal accepted a replaced socket inode")
	}
	if info, statErr := os.Lstat(filepath.Join(root, "eebus", "operator.sock")); statErr != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("replacement socket was removed: info=%v err=%v", info, statErr)
	}
}

func issue743LinuxTempRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "e743-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func issue743LinuxListenUnix(t *testing.T, path string) *net.UnixListener {
	t.Helper()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	return listener
}
