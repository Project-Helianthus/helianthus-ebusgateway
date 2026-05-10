// Unix-only helper for the runtime-state contract test suite. The Unix-only
// `syscall.Stat_t.Ino` is referenced here so the rest of the suite stays
// portable to non-Unix Go targets (Windows, Plan 9, etc.) where
// `syscall.Stat_t` is undefined.

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly || solaris

package runtimestate

import (
	"os"
	"syscall"
)

// inodeOf returns the inode number of a FileInfo on Unix-like systems.
func inodeOf(info os.FileInfo) (uint64, bool) {
	if info == nil {
		return 0, false
	}
	if sys, ok := info.Sys().(*syscall.Stat_t); ok && sys != nil {
		return uint64(sys.Ino), true
	}
	return 0, false
}
