package adaptermux

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
)

// NetworkSerial is the Config.Network value of a serial adapter device;
// Config.Address then carries the absolute device path, which is reopened
// after every disconnect so that a stable /dev/serial/by-id/ link survives a
// replug.
const NetworkSerial = "serial"

// The serial speeds of the enhanced protocol. On serial ens and enh differ by
// speed only; both use the same framing.
const (
	serialBaudENS uint32 = 115200
	serialBaudENH uint32 = 9600
)

// serialRetryInterval paces retrySerialPort while the device has no data.
const serialRetryInterval = time.Millisecond

func serialBaudForProtocol(protocol string) (uint32, error) {
	switch protocol {
	case "ens":
		return serialBaudENS, nil
	case "enh", "":
		return serialBaudENH, nil
	default:
		return 0, fmt.Errorf("adaptermux: unsupported protocol %q (expected \"enh\" or \"ens\")", protocol)
	}
}

// serialPort is the part of *os.File a serial connection relies on.
type serialPort interface {
	Read(b []byte) (int, error)
	Write(b []byte) (int, error)
	Close() error
	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// serialAddr is the net.Addr of a serial device.
type serialAddr string

func (a serialAddr) Network() string { return NetworkSerial }
func (a serialAddr) String() string  { return string(a) }

// serialConn presents an open serial device as a net.Conn for the ENH and ENS
// transports. A read or write error of a vanished device (EOF, EIO, ENXIO) is
// passed through unchanged; the transports report it as a closed connection,
// which the mux handles as an ordinary disconnect.
type serialConn struct {
	serialPort
	addr serialAddr
}

func (c *serialConn) LocalAddr() net.Addr  { return c.addr }
func (c *serialConn) RemoteAddr() net.Addr { return c.addr }

// newSerialConn takes ownership of file, which must be in non-blocking mode.
func newSerialConn(file *os.File, path string) *serialConn {
	return &serialConn{serialPort: newSerialPort(file), addr: serialAddr(path)}
}

// newSerialPort prefers the runtime poller. A platform whose poller refuses
// the tty reports that through SetDeadline, and gets retrySerialPort instead.
func newSerialPort(file *os.File) serialPort {
	if err := file.SetDeadline(time.Time{}); err == nil {
		return file
	}
	return &retrySerialPort{file: file}
}

// retrySerialPort implements deadlines over a non-blocking file the runtime
// poller does not serve: EAGAIN is retried until the deadline expires. Close
// stays safe against a concurrent Read or Write because os.File serializes it,
// and a closed file ends the retry with os.ErrClosed.
type retrySerialPort struct {
	file *os.File

	mu            sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

func (p *retrySerialPort) Read(b []byte) (int, error) {
	for {
		deadline, err := p.deadline(&p.readDeadline)
		if err != nil {
			return 0, err
		}
		n, err := p.file.Read(b)
		if !errors.Is(err, syscall.EAGAIN) {
			return n, err
		}
		serialRetryWait(deadline)
	}
}

func (p *retrySerialPort) Write(b []byte) (int, error) {
	written := 0
	for {
		deadline, err := p.deadline(&p.writeDeadline)
		if err != nil {
			return written, err
		}
		n, err := p.file.Write(b[written:])
		written += n
		if !errors.Is(err, syscall.EAGAIN) {
			return written, err
		}
		serialRetryWait(deadline)
	}
}

func (p *retrySerialPort) Close() error { return p.file.Close() }

func (p *retrySerialPort) SetDeadline(t time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readDeadline, p.writeDeadline = t, t
	return nil
}

func (p *retrySerialPort) SetReadDeadline(t time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readDeadline = t
	return nil
}

func (p *retrySerialPort) SetWriteDeadline(t time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writeDeadline = t
	return nil
}

func (p *retrySerialPort) deadline(slot *time.Time) (time.Time, error) {
	p.mu.Lock()
	deadline := *slot
	p.mu.Unlock()
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return deadline, os.ErrDeadlineExceeded
	}
	return deadline, nil
}

func serialRetryWait(deadline time.Time) {
	wait := serialRetryInterval
	if !deadline.IsZero() {
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
	}
	time.Sleep(wait)
}
