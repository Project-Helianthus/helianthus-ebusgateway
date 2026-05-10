// Unix-only helper for the M1_TDD_RED contract suite. Production code does
// not depend on this package; only the test build under
// `runtime_state_tdd_red` does. The Unix-only `syscall.Stat_t.Ino` is
// referenced here so the rest of the suite stays portable to non-Unix Go
// targets (Windows, Plan 9, etc.) where `syscall.Stat_t` is undefined.

//go:build runtime_state_tdd_red && (linux || darwin || freebsd || netbsd || openbsd || dragonfly || solaris)

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
