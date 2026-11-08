//go:build linux

package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	canbus "github.com/Project-Helianthus/helianthus-canbus"
	"golang.org/x/sys/unix"
)

// Injection covers all operating-system calls; no operation can send a frame or
// configure controller mode, interface state or bitrate.
type growattProjectSyscalls struct {
	lookup func(string) (*net.Interface, error)
	socket func(int, int, int) (int, error)
	bind   func(int, unix.Sockaddr) error
	poll   func([]unix.PollFd, int) (int, error)
	read   func(int, []byte) (int, error)
	close  func(int) error
}

type growattProjectSocket struct {
	identity canbus.InterfaceIdentity
	fd       int
	ops      growattProjectSyscalls
	start    time.Time
	sequence uint64
	once     sync.Once
	closeErr error
}

func openGrowattProjectSocket(name string) (growattProjectReceiver, error) {
	return openGrowattProjectSocketWith(name, growattProjectSyscalls{net.InterfaceByName, unix.Socket, unix.Bind, unix.Poll, unix.Read, unix.Close})
}
func openGrowattProjectSocketWith(name string, ops growattProjectSyscalls) (growattProjectReceiver, error) {
	if _, err := canbus.NewInterfaceIdentity(name, 1); err != nil {
		return nil, err
	}
	device, err := ops.lookup(name)
	if err != nil {
		return nil, err
	}
	if device == nil || device.Name != name {
		return nil, errors.New("selected interface identity mismatch")
	}
	identity, err := canbus.NewInterfaceIdentity(device.Name, device.Index)
	if err != nil {
		return nil, err
	}
	fd, err := ops.socket(unix.AF_CAN, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.CAN_RAW)
	if err != nil {
		return nil, err
	}
	socket := &growattProjectSocket{identity: identity, fd: fd, ops: ops, start: time.Now()}
	if err := ops.bind(fd, &unix.SockaddrCAN{Ifindex: device.Index}); err != nil {
		if closeErr := socket.Close(); closeErr != nil {
			return socket, errors.Join(err, closeErr)
		}
		return nil, err
	}
	return socket, nil
}
func (s *growattProjectSocket) Identity() canbus.InterfaceIdentity { return s.identity }
func (s *growattProjectSocket) Receive(ctx context.Context) (growattProjectRecord, error) {
	for {
		if err := ctx.Err(); err != nil {
			return growattProjectRecord{}, err
		}
		poll := []unix.PollFd{{Fd: int32(s.fd), Events: unix.POLLIN}}
		ready, err := s.ops.poll(poll, 25)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return growattProjectRecord{}, err
		}
		if ready == 0 {
			continue
		}
		if poll[0].Revents&unix.POLLIN == 0 {
			return growattProjectRecord{}, errors.New("SocketCAN receive poll failure")
		}
		var raw [canbus.SocketCANFDRecordSize]byte
		n, err := s.ops.read(s.fd, raw[:])
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return growattProjectRecord{}, err
		}
		if n < 0 || n > len(raw) {
			return growattProjectRecord{}, errors.New("invalid SocketCAN read length")
		}
		s.sequence++
		return growattProjectRecord{Interface: s.identity, Raw: append([]byte(nil), raw[:n]...), Sequence: s.sequence, Monotonic: time.Since(s.start)}, nil
	}
}
func (s *growattProjectSocket) Close() error {
	s.once.Do(func() { s.closeErr = s.ops.close(s.fd) })
	return s.closeErr
}
