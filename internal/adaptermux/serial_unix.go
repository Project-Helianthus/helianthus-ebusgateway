//go:build linux || darwin

package adaptermux

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// serialSetExclusive requests exclusive access to the open device.
var serialSetExclusive = func(fd int) error {
	return unix.IoctlSetInt(fd, unix.TIOCEXCL, 0)
}

// openSerialConn opens and configures the adapter device at path. The speed
// follows protocol: 115200 Baud for ens, 9600 Baud for enh.
func openSerialConn(path, protocol string) (net.Conn, error) {
	baud, err := serialBaudForProtocol(protocol)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("serial device path %q is not absolute", path)
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err := configureSerialFD(fd, baud); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return newSerialConn(os.NewFile(uintptr(fd), path), path), nil
}

// configureSerialFD claims the device, sets raw 8N1 at baud without flow
// control and discards whatever arrived before the mux was ready to parse it.
func configureSerialFD(fd int, baud uint32) error {
	if err := serialSetExclusive(fd); err != nil {
		return fmt.Errorf("exclusive access: %w", err)
	}
	termios, err := unix.IoctlGetTermios(fd, serialGetTermiosRequest)
	if err != nil {
		return fmt.Errorf("read termios: %w", err)
	}
	setSerialRawMode(termios)
	if err := setSerialSpeed(termios, baud); err != nil {
		return err
	}
	if err := unix.IoctlSetTermios(fd, serialSetTermiosRequest, termios); err != nil {
		return fmt.Errorf("write termios: %w", err)
	}
	// The ioctl succeeds when the driver applied any part of the request, so
	// the speed is read back rather than assumed.
	applied, err := unix.IoctlGetTermios(fd, serialGetTermiosRequest)
	if err != nil {
		return fmt.Errorf("read back termios: %w", err)
	}
	if !serialSpeedIs(applied, baud) {
		return fmt.Errorf("device did not accept %d Baud", baud)
	}
	if err := flushSerialInput(fd); err != nil {
		return fmt.Errorf("discard pending input: %w", err)
	}
	return nil
}

// setSerialRawMode selects 8N1 without flow control or line processing. VMIN=1
// keeps an empty non-blocking read at EAGAIN; VMIN=0 would report it as EOF,
// which the transports treat as a disconnect. Bytes with a framing error are
// dropped instead of reaching the ENH parser as a fabricated 0x00.
func setSerialRawMode(termios *unix.Termios) {
	termios.Iflag = unix.IGNBRK | unix.IGNPAR
	termios.Oflag = 0
	termios.Lflag = 0
	termios.Cflag &^= unix.CSIZE | unix.PARENB | unix.CSTOPB | unix.CRTSCTS | unix.HUPCL
	termios.Cflag |= unix.CS8 | unix.CREAD | unix.CLOCAL
	termios.Cc[unix.VMIN] = 1
	termios.Cc[unix.VTIME] = 0
}
