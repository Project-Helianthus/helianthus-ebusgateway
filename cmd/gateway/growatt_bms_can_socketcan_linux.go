//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const growattBMSCANPollInterval = 25 * time.Millisecond

// growattBMSCANSocket is a raw CAN socket bound to one interface. It only
// polls and reads: it never writes, and it sets no interface state. Bit rate
// and listen-only controller mode remain the integrator's precondition.
type growattBMSCANSocket struct {
	identity   growattBMSCANInterface
	descriptor int
	closeOnce  sync.Once
	closeErr   error
}

func openGrowattBMSCANSocket(name string) (growattBMSCANReceiver, error) {
	if !validGrowattBMSCANInterfaceName(name) {
		return nil, errGrowattBMSCANConfig
	}
	device, err := net.InterfaceByName(name)
	if err != nil {
		return nil, fmt.Errorf("lookup CAN interface: %w", err)
	}
	if device.Index <= 0 || device.Name != name {
		return nil, errGrowattBMSCANIdentity
	}
	descriptor, err := unix.Socket(unix.AF_CAN, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.CAN_RAW)
	if err != nil {
		return nil, fmt.Errorf("create SocketCAN receive socket: %w", err)
	}
	// Classic frames only; error frames stay unsubscribed (the default mask).
	if err := unix.SetsockoptInt(descriptor, unix.SOL_CAN_RAW, unix.CAN_RAW_FD_FRAMES, 0); err != nil {
		return nil, errors.Join(fmt.Errorf("restrict SocketCAN receive socket to classic CAN: %w", err), unix.Close(descriptor))
	}
	if err := unix.Bind(descriptor, &unix.SockaddrCAN{Ifindex: device.Index}); err != nil {
		return nil, errors.Join(fmt.Errorf("bind SocketCAN receive socket: %w", err), unix.Close(descriptor))
	}
	return &growattBMSCANSocket{identity: growattBMSCANInterface{Name: device.Name, Index: device.Index}, descriptor: descriptor}, nil
}

func (socket *growattBMSCANSocket) Identity() growattBMSCANInterface { return socket.identity }

func (socket *growattBMSCANSocket) Receive(ctx context.Context) (growattBMSCANRecord, error) {
	for {
		if err := ctx.Err(); err != nil {
			return growattBMSCANRecord{}, err
		}
		poll := []unix.PollFd{{Fd: int32(socket.descriptor), Events: unix.POLLIN}}
		ready, err := unix.Poll(poll, growattBMSCANPollTimeout(ctx))
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return growattBMSCANRecord{}, fmt.Errorf("poll SocketCAN receive socket: %w", err)
		}
		if ready == 0 {
			continue
		}
		events := poll[0].Revents
		if events&unix.POLLIN == 0 {
			if events&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
				return growattBMSCANRecord{}, fmt.Errorf("SocketCAN receive socket poll events %#x", events)
			}
			continue
		}
		var record [growattBMSCANFDRecordSize]byte
		count, err := unix.Read(socket.descriptor, record[:])
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EINTR) {
				continue
			}
			return growattBMSCANRecord{}, fmt.Errorf("read SocketCAN record: %w", err)
		}
		return growattBMSCANRecord{Interface: socket.identity, Raw: append([]byte(nil), record[:count]...)}, nil
	}
}

func (socket *growattBMSCANSocket) Close() error {
	socket.closeOnce.Do(func() { socket.closeErr = unix.Close(socket.descriptor) })
	return socket.closeErr
}

func growattBMSCANPollTimeout(ctx context.Context) int {
	duration := growattBMSCANPollInterval
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0
		}
		if remaining < duration {
			duration = remaining
		}
	}
	return int((duration + time.Millisecond - 1) / time.Millisecond)
}
