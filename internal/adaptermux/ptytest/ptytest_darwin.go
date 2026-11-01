package ptytest

import (
	"bytes"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

const getTermiosRequest = unix.TIOCGETA

func unlockSlave(fd int) (string, error) {
	if err := unix.IoctlSetInt(fd, unix.TIOCPTYGRANT, 0); err != nil {
		return "", err
	}
	if err := unix.IoctlSetInt(fd, unix.TIOCPTYUNLK, 0); err != nil {
		return "", err
	}
	name := make([]byte, 128)
	//nolint:staticcheck // SA1019: x/sys has no wrapper for this ioctl's name buffer.
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), uintptr(unix.TIOCPTYGNAME), uintptr(unsafe.Pointer(&name[0]))); errno != 0 {
		return "", errno
	}
	return string(name[:bytes.IndexByte(name, 0)]), nil
}

// Baud returns the speed configured on the slave side.
func (p *Pair) Baud(t testing.TB) uint32 {
	t.Helper()
	termios := p.Termios(t)
	if termios.Ispeed != termios.Ospeed {
		t.Fatalf("input speed %d differs from output speed %d", termios.Ispeed, termios.Ospeed)
	}
	return uint32(termios.Ispeed)
}
