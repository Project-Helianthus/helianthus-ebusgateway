package adaptermux

import (
	"fmt"

	"golang.org/x/sys/unix"
)

const (
	serialGetTermiosRequest = unix.TCGETS
	serialSetTermiosRequest = unix.TCSETS
)

func setSerialSpeed(termios *unix.Termios, baud uint32) error {
	speed, err := serialSpeedFlag(baud)
	if err != nil {
		return err
	}
	termios.Cflag &^= unix.CBAUD | unix.CIBAUD
	termios.Cflag |= speed
	return nil
}

func serialSpeedIs(termios *unix.Termios, baud uint32) bool {
	speed, err := serialSpeedFlag(baud)
	return err == nil && termios.Cflag&unix.CBAUD == speed
}

func serialSpeedFlag(baud uint32) (uint32, error) {
	switch baud {
	case serialBaudENS:
		return unix.B115200, nil
	case serialBaudENH:
		return unix.B9600, nil
	default:
		return 0, fmt.Errorf("unsupported serial speed %d", baud)
	}
}

func flushSerialInput(fd int) error {
	return unix.IoctlSetInt(fd, unix.TCFLSH, unix.TCIFLUSH)
}
