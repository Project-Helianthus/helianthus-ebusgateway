package adaptermux

import "golang.org/x/sys/unix"

const (
	serialGetTermiosRequest = unix.TIOCGETA
	serialSetTermiosRequest = unix.TIOCSETA

	// serialFlushRead is the FREAD argument of TIOCFLUSH.
	serialFlushRead = 1
)

func setSerialSpeed(termios *unix.Termios, baud uint32) error {
	termios.Ispeed = uint64(baud)
	termios.Ospeed = uint64(baud)
	return nil
}

func serialSpeedIs(termios *unix.Termios, baud uint32) bool {
	return termios.Ispeed == uint64(baud) && termios.Ospeed == uint64(baud)
}

func flushSerialInput(fd int) error {
	return unix.IoctlSetPointerInt(fd, unix.TIOCFLUSH, serialFlushRead)
}
