//go:build linux

package main

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"net"
	"testing"
)

func TestCX18LinuxInjected(t *testing.T) {
	for _, scenario := range []string{"read", "short", "bind-error", "read-error", "poll-error", "close-error", "bind-close-error", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			binds, closes, reads := 0, 0, 0
			ops := growattProjectSyscalls{
				lookup: func(name string) (*net.Interface, error) { return &net.Interface{Name: name, Index: 7}, nil },
				socket: func(domain, kind, protocol int) (int, error) {
					if domain != unix.AF_CAN || kind != unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC || protocol != unix.CAN_RAW {
						t.Fatal("socket arguments")
					}
					return 9, nil
				},
				bind: func(fd int, address unix.Sockaddr) error {
					binds++
					if fd != 9 || address.(*unix.SockaddrCAN).Ifindex != 7 {
						t.Fatal("bind arguments")
					}
					if scenario == "bind-error" || scenario == "bind-close-error" {
						return errors.New("synthetic bind error")
					}
					return nil
				},
				poll: func(fds []unix.PollFd, timeout int) (int, error) {
					if timeout != 25 || fds[0].Fd != 9 || fds[0].Events != unix.POLLIN {
						t.Fatal("poll arguments")
					}
					if scenario == "poll-error" {
						return 0, errors.New("synthetic poll error")
					}
					fds[0].Revents = unix.POLLIN
					return 1, nil
				},
				read: func(fd int, b []byte) (int, error) {
					reads++
					if fd != 9 || len(b) != 72 {
						t.Fatal("read arguments")
					}
					if scenario == "read-error" {
						return 0, errors.New("synthetic read error")
					}
					b[4] = 8
					if scenario == "short" {
						return 5, nil
					}
					return 16, nil
				},
				close: func(fd int) error {
					closes++
					if scenario == "close-error" || scenario == "bind-close-error" {
						return errors.New("synthetic close error")
					}
					return nil
				},
			}
			r, err := openGrowattProjectSocketWith("synthetic0", ops)
			if scenario == "bind-close-error" {
				if err == nil || r == nil || closes != 1 {
					t.Fatal("failed cleanup lost ownership")
				}
				if r.Close() == nil || closes != 1 {
					t.Fatal("close failure not sticky")
				}
				return
			}
			if scenario == "bind-error" {
				if err == nil || r != nil || closes != 1 {
					t.Fatal("bind cleanup")
				}
				return
			}
			if err != nil || binds != 1 {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "cancel" {
				cancel()
			}
			record, err := r.Receive(ctx)
			if scenario == "poll-error" || scenario == "read-error" || scenario == "cancel" {
				if err == nil {
					t.Fatal("missing error")
				}
			} else {
				want := 16
				if scenario == "short" {
					want = 5
				}
				if err != nil || len(record.Raw) != want || record.Sequence != 1 || record.Monotonic < 0 {
					t.Fatal(record, err)
				}
			}
			if scenario == "cancel" && reads != 0 {
				t.Fatal("read after cancel")
			}
			err = r.Close()
			if (err != nil) != (scenario == "close-error") {
				t.Fatal(err)
			}
			_ = r.Close()
			if closes != 1 {
				t.Fatal("close repeated")
			}
		})
	}
}
