// Stub for non-Unix builds of the M1_TDD_RED contract suite. inodeOf returns
// (0, false) so TestPersist_AtomicTempRename t.Skip's the inode-distinguish
// check on platforms where syscall.Stat_t is undefined.

//go:build runtime_state_tdd_red && !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly && !solaris

package runtimestate

import "os"

func inodeOf(info os.FileInfo) (uint64, bool) {
	return 0, false
}
