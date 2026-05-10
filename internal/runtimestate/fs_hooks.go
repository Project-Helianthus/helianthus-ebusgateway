// Package runtimestate — DefaultFilesystemHooks. Plan: runtime-state-w19-26.
// AD13 production filesystem operations. Tests inject mocks via Options.FsHooks
// to fault-inject EXDEV / ENOSPC / ENOSYS / EINVAL etc.

package runtimestate

import (
	"errors"
	"os"
	"syscall"
)

// DefaultFilesystemHooks delegates to the os package. Used by Manager when
// Options.FsHooks is nil.
type DefaultFilesystemHooks struct{}

func (DefaultFilesystemHooks) WriteFile(path string, data []byte, perm uint32) error {
	return os.WriteFile(path, data, os.FileMode(perm))
}

func (DefaultFilesystemHooks) FsyncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (DefaultFilesystemHooks) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (DefaultFilesystemHooks) FsyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (DefaultFilesystemHooks) Unlink(path string) error {
	return os.Remove(path)
}

// isParentFsyncSwallowed reports whether the given error from FsyncDir should
// be silently swallowed per AD13 (ENOTSUP / EINVAL / EPERM / ENOSYS).
func isParentFsyncSwallowed(err error) bool {
	if err == nil {
		return false
	}
	var perr *os.PathError
	if errors.As(err, &perr) {
		err = perr.Err
	}
	switch err {
	case syscall.ENOTSUP, syscall.EINVAL, syscall.EPERM, syscall.ENOSYS:
		return true
	}
	return false
}

// isExdev reports whether err carries syscall.EXDEV (cross-device link).
func isExdev(err error) bool {
	if err == nil {
		return false
	}
	var perr *os.PathError
	if errors.As(err, &perr) {
		err = perr.Err
	}
	return err == syscall.EXDEV
}
