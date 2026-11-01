//go:build linux || darwin

// Package ptytest provides a pseudo-terminal pair and a synthetic adapter for
// tests of the serial adapter path. It never touches a real device.
package ptytest

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"golang.org/x/sys/unix"
)

// Pair is one pseudo-terminal: the test plays the adapter on the master side,
// the code under test opens SlavePath.
type Pair struct {
	Master    *os.File
	SlavePath string

	closeOnce sync.Once
	done      chan struct{}
}

// Open creates a pair that is closed at the end of the test. A missing or
// refused /dev/ptmx is an environment failure and fails the test.
func Open(t testing.TB) *Pair {
	t.Helper()
	fd, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open /dev/ptmx: %v", err)
	}
	path, err := unlockSlave(fd)
	if err != nil {
		_ = unix.Close(fd)
		t.Fatalf("unlock pty: %v", err)
	}
	pair := &Pair{Master: os.NewFile(uintptr(fd), "/dev/ptmx"), SlavePath: path, done: make(chan struct{})}
	t.Cleanup(pair.Close)
	return pair
}

// Close closes the master side, which makes the device vanish for whoever
// holds the slave side: reads end with EOF or EIO.
func (p *Pair) Close() {
	p.closeOnce.Do(func() {
		close(p.done)
		_ = p.Master.Close()
	})
}

// Termios returns the slave's settings as seen through the master.
func (p *Pair) Termios(t testing.TB) *unix.Termios {
	t.Helper()
	raw, err := p.Master.SyscallConn()
	if err != nil {
		t.Fatalf("master syscall conn: %v", err)
	}
	var termios *unix.Termios
	var ioctlErr error
	if err := raw.Control(func(fd uintptr) {
		termios, ioctlErr = unix.IoctlGetTermios(int(fd), getTermiosRequest)
	}); err != nil {
		t.Fatalf("master control: %v", err)
	}
	if ioctlErr != nil {
		t.Fatalf("read termios: %v", ioctlErr)
	}
	return termios
}

// WriteMaster writes bytes the code under test will read from the device.
func (p *Pair) WriteMaster(t testing.TB, data []byte) {
	t.Helper()
	if err := p.writeMaster(data); err != nil {
		t.Fatalf("write master: %v", err)
	}
}

func (p *Pair) writeMaster(data []byte) error {
	for len(data) > 0 {
		n, err := p.Master.Write(data)
		data = data[n:]
		if err != nil && !errors.Is(err, syscall.EAGAIN) {
			return err
		}
		if err != nil {
			select {
			case <-p.done:
				return os.ErrClosed
			case <-time.After(time.Millisecond):
			}
		}
	}
	return nil
}

// ReadMaster reads exactly n bytes the code under test wrote to the device,
// failing the test once timeout elapses.
func (p *Pair) ReadMaster(t testing.TB, n int, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.Now().Add(timeout)
	out := make([]byte, 0, n)
	buf := make([]byte, n)
	for len(out) < n {
		if time.Now().After(deadline) {
			t.Fatalf("read master: got %x, want %d bytes", out, n)
		}
		got, err := p.readMaster(buf[:n-len(out)])
		out = append(out, buf[:got]...)
		if err != nil {
			t.Fatalf("read master: %v", err)
		}
	}
	return out
}

// readMaster returns (0, nil) when no data is pending within a short wait.
func (p *Pair) readMaster(buf []byte) (int, error) {
	_ = p.Master.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
	n, err := p.Master.Read(buf)
	switch {
	case err == nil:
		return n, nil
	case errors.Is(err, os.ErrDeadlineExceeded):
		return n, nil
	case errors.Is(err, syscall.EAGAIN):
		time.Sleep(time.Millisecond)
		return n, nil
	default:
		return n, err
	}
}

// Adapter answers the ENH handshake on the master side of a pair.
type Adapter struct {
	inits chan struct{}
}

// ServeAdapter answers INIT with RESETTED and every INFO request with an empty
// INFO response until the pair is closed.
func ServeAdapter(pair *Pair) *Adapter {
	adapter := &Adapter{inits: make(chan struct{}, 16)}
	go func() { // goroutine: synthetic adapter; ends when the pair closes
		var parser transport.ENHParser
		buf := make([]byte, 64)
		for {
			select {
			case <-pair.done:
				return
			default:
			}
			n, err := pair.readMaster(buf)
			if err != nil {
				return
			}
			for _, b := range buf[:n] {
				message, ok, err := parser.Feed(b)
				if err != nil || !ok || message.Kind != transport.ENHMessageFrame {
					continue
				}
				switch message.Command {
				case transport.ENHReqInit:
					reply := transport.EncodeENH(transport.ENHResResetted, message.Data)
					_ = pair.writeMaster(reply[:])
					select {
					case adapter.inits <- struct{}{}:
					default:
					}
				case transport.ENHReqInfo:
					reply := transport.EncodeENH(transport.ENHResInfo, 0)
					_ = pair.writeMaster(reply[:])
				}
			}
		}
	}()
	return adapter
}

// WaitInit fails the test unless an INIT arrives within timeout.
func (a *Adapter) WaitInit(t testing.TB, timeout time.Duration) {
	t.Helper()
	select {
	case <-a.inits:
	case <-time.After(timeout):
		t.Fatalf("adapter saw no INIT within %v", timeout)
	}
}
