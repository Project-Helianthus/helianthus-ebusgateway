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

// Pair is one pseudo-terminal: the test plays the adapter on the peer side,
// the code under test opens DevicePath.
type Pair struct {
	Peer       *os.File
	DevicePath string

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
	path, err := unlockDevice(fd)
	if err != nil {
		_ = unix.Close(fd)
		t.Fatalf("unlock pty: %v", err)
	}
	pair := &Pair{Peer: os.NewFile(uintptr(fd), "/dev/ptmx"), DevicePath: path, done: make(chan struct{})}
	t.Cleanup(pair.Close)
	return pair
}

// Close closes the peer side, which makes the device vanish for whoever
// holds the device side: reads end with EOF or EIO.
func (p *Pair) Close() {
	p.closeOnce.Do(func() {
		close(p.done)
		_ = p.Peer.Close()
	})
}

// Termios returns the device's settings as seen through the peer.
func (p *Pair) Termios(t testing.TB) *unix.Termios {
	t.Helper()
	raw, err := p.Peer.SyscallConn()
	if err != nil {
		t.Fatalf("peer syscall conn: %v", err)
	}
	var termios *unix.Termios
	var ioctlErr error
	if err := raw.Control(func(fd uintptr) {
		termios, ioctlErr = unix.IoctlGetTermios(int(fd), getTermiosRequest)
	}); err != nil {
		t.Fatalf("peer control: %v", err)
	}
	if ioctlErr != nil {
		t.Fatalf("read termios: %v", ioctlErr)
	}
	return termios
}

// WritePeer writes bytes the code under test will read from the device.
func (p *Pair) WritePeer(t testing.TB, data []byte) {
	t.Helper()
	if err := p.writePeer(data); err != nil {
		t.Fatalf("write peer: %v", err)
	}
}

func (p *Pair) writePeer(data []byte) error {
	for len(data) > 0 {
		n, err := p.Peer.Write(data)
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

// ReadPeer reads exactly n bytes the code under test wrote to the device,
// failing the test once timeout elapses.
func (p *Pair) ReadPeer(t testing.TB, n int, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.Now().Add(timeout)
	out := make([]byte, 0, n)
	buf := make([]byte, n)
	for len(out) < n {
		if time.Now().After(deadline) {
			t.Fatalf("read peer: got %x, want %d bytes", out, n)
		}
		got, err := p.readPeer(buf[:n-len(out)])
		out = append(out, buf[:got]...)
		if err != nil {
			t.Fatalf("read peer: %v", err)
		}
	}
	return out
}

// readPeer returns (0, nil) when no data is pending within a short wait.
func (p *Pair) readPeer(buf []byte) (int, error) {
	_ = p.Peer.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
	n, err := p.Peer.Read(buf)
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

// Adapter answers the ENH handshake on the peer side of a pair.
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
			n, err := pair.readPeer(buf)
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
					_ = pair.writePeer(reply[:])
					select {
					case adapter.inits <- struct{}{}:
					default:
					}
				case transport.ENHReqInfo:
					reply := transport.EncodeENH(transport.ENHResInfo, 0)
					_ = pair.writePeer(reply[:])
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
