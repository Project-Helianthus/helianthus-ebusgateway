package ptytest

import (
	"fmt"
	"testing"

	"golang.org/x/sys/unix"
)

const getTermiosRequest = unix.TCGETS

func unlockSlave(fd int) (string, error) {
	if err := unix.IoctlSetPointerInt(fd, unix.TIOCSPTLCK, 0); err != nil {
		return "", err
	}
	number, err := unix.IoctlGetInt(fd, unix.TIOCGPTN)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("/dev/pts/%d", number), nil
}

// Baud returns the speed configured on the slave side.
func (p *Pair) Baud(t testing.TB) uint32 {
	t.Helper()
	switch speed := p.Termios(t).Cflag & unix.CBAUD; speed {
	case unix.B115200:
		return 115200
	case unix.B9600:
		return 9600
	default:
		t.Fatalf("unexpected CBAUD value %#x", speed)
		return 0
	}
}
