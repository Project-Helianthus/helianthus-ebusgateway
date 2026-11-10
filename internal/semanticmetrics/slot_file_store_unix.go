//go:build linux || darwin

package semanticmetrics

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

// SlotFileStore holds one advisory exclusive writer lock until Close. The
// directory must already exist and be controlled by the caller; all writers
// must use this adapter and must never remove its persistent lock file.
type SlotFileStore struct {
	mu                      sync.Mutex
	dir, lock               *os.File
	directory, target, name string
	closed, uncertain       bool
	// fault is a private test seam invoked immediately before each operation.
	fault func(string) error
}

var _ O21Persistence = (*SlotFileStore)(nil)

// NewSlotFileStore opens an existing caller-selected directory and fences one
// explicit target. It neither loads nor creates a slot document.
func NewSlotFileStore(directory, target string) (*SlotFileStore, error) {
	name, err := slotFileName(target)
	if err != nil {
		return nil, err
	}
	if directory == "" {
		return nil, fmt.Errorf("missing slot directory")
	}
	directory, err = filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Open(directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	s := &SlotFileStore{dir: os.NewFile(uintptr(fd), directory), directory: directory, target: target, name: name}
	lockfd, err := unix.Openat(fd, name+".lock", unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0600)
	if err != nil {
		_ = s.dir.Close()
		return nil, err
	}
	s.lock = os.NewFile(uintptr(lockfd), name+".lock")
	if err = unix.Flock(lockfd, unix.LOCK_EX|unix.LOCK_NB); err == nil {
		err = s.check(target)
	}
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *SlotFileStore) check(target string) (err error) {
	defer func() {
		if errors.Is(err, ErrSlotFileFencing) {
			s.uncertain = true
		}
	}()
	if s.closed {
		return ErrSlotFileClosed
	}
	if s.uncertain {
		return ErrSlotFileUncertain
	}
	if target != s.target {
		return fmt.Errorf("slot file target mismatch")
	}
	d, err := s.dir.Stat()
	if err != nil {
		return ErrSlotFileFencing
	}
	path, err := os.Lstat(s.directory)
	if err != nil || !os.SameFile(d, path) {
		return ErrSlotFileFencing
	}
	var held, named unix.Stat_t
	if unix.Fstat(int(s.lock.Fd()), &held) != nil || unix.Fstatat(int(s.dir.Fd()), s.name+".lock", &named, unix.AT_SYMLINK_NOFOLLOW) != nil || held.Mode&unix.S_IFMT != unix.S_IFREG || named.Mode&unix.S_IFMT != unix.S_IFREG || held.Nlink != 1 || held.Dev != named.Dev || held.Ino != named.Ino {
		return ErrSlotFileFencing
	}
	if unix.Flock(int(s.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB) != nil {
		return ErrSlotFileFencing
	}
	return nil
}

func (s *SlotFileStore) openData() (*os.File, error) {
	fd, err := unix.Openat(int(s.dir.Fd()), s.name+".json", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), s.name+".json")
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > o21MaxBytes {
		_ = f.Close()
		return nil, fmt.Errorf("invalid or oversized slot file")
	}
	return f, nil
}

// Load returns a bounded detached document; only a missing document yields
// fs.ErrNotExist. Closed, uncertain and unfenced adapters reject all operations.
func (s *SlotFileStore) Load(target string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(target); err != nil {
		return nil, err
	}
	if err := s.step("read", func() error { return nil }); err != nil {
		return nil, err
	}
	f, err := s.openData()
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			if fenceErr := s.check(target); fenceErr != nil {
				return nil, fenceErr
			}
			return nil, fs.ErrNotExist
		}
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(f, o21MaxBytes+1))
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(raw) > o21MaxBytes {
		return nil, fmt.Errorf("slot file too large")
	}
	if err := s.check(target); err != nil {
		return nil, err
	}
	return raw, nil
}

func (s *SlotFileStore) step(operation string, action func() error) error {
	if s.fault != nil {
		if err := s.fault(operation); err != nil {
			return err
		}
	}
	return action()
}

// Commit replaces a whole bounded document. Every commit failure latches
// uncertainty, even when the previous file may still be intact.
func (s *SlotFileStore) Commit(target string, document []byte) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.check(target); err != nil {
		return err
	}
	if len(document) > o21MaxBytes {
		return fmt.Errorf("slot file too large")
	}
	defer func() {
		if err != nil {
			s.uncertain = true
		}
	}()
	existing, err := s.openData()
	if err == nil {
		err = existing.Close()
	}
	if err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return err
	}
	tempName := fmt.Sprintf("%s.tmp-%x", s.name, nonce)
	var fd int
	err = s.step("temp-create", func() error {
		var openErr error
		fd, openErr = unix.Openat(int(s.dir.Fd()), tempName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
		return openErr
	})
	if err != nil {
		return err
	}
	temp := os.NewFile(uintptr(fd), tempName)
	defer func() { _ = temp.Close(); _ = unix.Unlinkat(int(s.dir.Fd()), tempName, 0) }()
	if err = s.step("write", func() error {
		n, e := temp.Write(document)
		if e == nil && n != len(document) {
			e = io.ErrShortWrite
		}
		return e
	}); err != nil {
		return err
	}
	if err = s.step("file-sync", temp.Sync); err != nil {
		return err
	}
	if err = s.step("file-close", temp.Close); err != nil {
		return err
	}
	if err = s.check(target); err != nil {
		return err
	}
	if err = s.step("rename", func() error { return unix.Renameat(int(s.dir.Fd()), tempName, int(s.dir.Fd()), s.name+".json") }); err != nil {
		return err
	}
	if err = s.step("directory-sync", s.dir.Sync); err != nil {
		return err
	}
	return s.check(target)
}

// Close releases fencing and descriptors. It is idempotent, waits for any
// operation in progress and makes the adapter permanently unusable.
func (s *SlotFileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.lock != nil {
		err = s.lock.Close()
	}
	if s.dir != nil {
		err = errors.Join(err, s.dir.Close())
	}
	return err
}
