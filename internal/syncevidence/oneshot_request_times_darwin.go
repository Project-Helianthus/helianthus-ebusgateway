//go:build darwin

package syncevidence

import "golang.org/x/sys/unix"

func oneShotRequestChangeTimes(stat unix.Stat_t) (int64, int64, int64, int64) {
	return stat.Mtim.Sec, stat.Mtim.Nsec, stat.Ctim.Sec, stat.Ctim.Nsec
}
