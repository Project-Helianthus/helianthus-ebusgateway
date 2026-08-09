//go:build linux

package syncevidence

import "golang.org/x/sys/unix"

func oneShotRequestChangeTimes(stat unix.Stat_t) (int64, int64, int64, int64) {
	return int64(stat.Mtim.Sec), int64(stat.Mtim.Nsec), int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec)
}
