// Stub for non-Unix builds of the runtime-state contract test suite.
// inodeOf returns (0, false) so TestPersist_AtomicTempRename t.Skip's the
// inode-distinguish check on platforms where syscall.Stat_t is undefined.

//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly && !solaris

package runtimestate

import "os"

func inodeOf(info os.FileInfo) (uint64, bool) {
	return 0, false
}
