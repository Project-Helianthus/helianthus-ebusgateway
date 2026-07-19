//go:build darwin

package syncevidence

import (
	"os"

	"golang.org/x/sys/unix"
)

func syncFile(file *os.File) error {
	if _, err := unix.FcntlInt(file.Fd(), unix.F_FULLFSYNC, 0); err != nil {
		return ErrDurability
	}
	return nil
}

func syncDirectory(directory *os.File) error {
	if err := unix.Fsync(int(directory.Fd())); err != nil {
		return ErrDurability
	}
	return nil
}
