//go:build linux

package main

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/teslaobserve"
)

// teslaSerialOps are the terminal operations the opener performs after
// open(2). Tests substitute recorders to prove their number and order.
type teslaSerialOps struct {
	termios func(fd int, request uintptr, termios *syscall.Termios) error
	flush   func(fd int) error
}

var teslaSerialSystem = teslaSerialOps{termios: teslaTermios, flush: teslaFlushQueues}

// openTeslaObserveSerial opens the port read-only and configures it for
// receive-only observation: raw 115200 8N1 with echo and software flow
// control disabled in a single termios update, then a flush of both queues.
//
// User space cannot set terminal attributes before open(2). Until the first
// update returns, the port still has the attributes its driver kept from the
// last user, and an inherited ECHO or IXOFF can make the driver transmit for
// bytes that arrive in that window even though the descriptor is O_RDONLY.
// The opener therefore never opens the port read-write, narrows the window to
// one ioctl pair, and discards every byte received or queued before the
// configuration (see configureTeslaObserveSerial), so such bytes never enter
// evidence. It cannot prove that nothing was transmitted inside the window. The receive-only guarantee at the bus is the operator's
// hardware boundary, and the software defence is to run the preparation mode
// (-prepare) once before the bus is connected, which leaves the port with
// echo and flow control already disabled when the observation opens it.
func openTeslaObserveSerial(path string) (teslaobserve.ByteSource, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOCTTY|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, redactTeslaSerialError(err)
	}
	if err := configureTeslaObserveSerial(fd, teslaSerialSystem); err != nil {
		_ = syscall.Close(fd)
		return nil, redactTeslaSerialError(err)
	}
	return &teslaReceiveOnlySerial{file: os.NewFile(uintptr(fd), path)}, nil
}

// configureTeslaObserveSerial applies the raw receive settings in one TCSETS,
// reads them back and then flushes both queues with TCIOFLUSH. The flush
// discards every byte that was received or queued before the configuration:
// those bytes were handled under the inherited attributes and have Unknown
// provenance, so they must not enter the evidence window. It runs after the
// read-back check on purpose: with IXOFF already off, discarding queued input
// cannot make the driver transmit a start character.
func configureTeslaObserveSerial(fd int, ops teslaSerialOps) error {
	var termios syscall.Termios
	if err := ops.termios(fd, syscall.TCGETS, &termios); err != nil {
		return err
	}
	// Build raw receive settings together: never enable reception with an
	// inherited IXOFF, echo, output processing or hardware flow control.
	termios.Iflag = 0
	termios.Oflag = 0
	termios.Lflag = 0
	termios.Cflag = syscall.B115200 | syscall.CS8 | syscall.CREAD | syscall.CLOCAL
	termios.Cc[syscall.VMIN] = 1
	termios.Cc[syscall.VTIME] = 0
	if err := ops.termios(fd, syscall.TCSETS, &termios); err != nil {
		return err
	}
	var check syscall.Termios
	if err := ops.termios(fd, syscall.TCGETS, &check); err != nil {
		return err
	}
	if check.Iflag != termios.Iflag || check.Oflag != termios.Oflag ||
		check.Lflag != termios.Lflag || check.Cflag != termios.Cflag ||
		check.Cc[syscall.VMIN] != termios.Cc[syscall.VMIN] ||
		check.Cc[syscall.VTIME] != termios.Cc[syscall.VTIME] {
		return errors.New("receive-only serial settings could not be applied")
	}
	return ops.flush(fd)
}

type teslaReceiveOnlySerial struct {
	file      *os.File
	closeOnce sync.Once
	closeErr  error
}

func (serial *teslaReceiveOnlySerial) ReceiveByte(ctx context.Context) (byte, error) {
	var buffer [1]byte
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		deadline := time.Now().Add(10 * time.Millisecond)
		if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
			deadline = end
		}
		if err := serial.file.SetReadDeadline(deadline); err != nil {
			return 0, redactTeslaSerialError(err)
		}
		n, err := serial.file.Read(buffer[:])
		if n == 1 {
			return buffer[0], nil
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			continue
		}
		if err == nil {
			err = io.EOF
		}
		return 0, redactTeslaSerialError(err)
	}
}

func (serial *teslaReceiveOnlySerial) Close() error {
	serial.closeOnce.Do(func() { serial.closeErr = serial.file.Close() })
	return serial.closeErr
}

// teslaFlushQueues discards both the received-but-unread and the untransmitted
// queue. It never transmits.
func teslaFlushQueues(fd int) error {
	return unix.IoctlSetInt(fd, unix.TCFLSH, unix.TCIOFLUSH)
}

func teslaTermios(fd int, request uintptr, termios *syscall.Termios) error {
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), request, uintptr(unsafe.Pointer(termios))); errno != 0 {
		return errno
	}
	return nil
}
