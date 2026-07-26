//go:build linux

package mcp

import (
	"errors"
	"net"

	"golang.org/x/sys/unix"
)

func eebusV1PlatformPeerUIDResolver() eebusV1PeerUIDResolver {
	return func(connection net.Conn) (int, error) {
		unixConnection, ok := connection.(*net.UnixConn)
		if !ok {
			return 0, errors.New("operator connection is not AF_UNIX")
		}
		raw, err := unixConnection.SyscallConn()
		if err != nil {
			return 0, err
		}
		var (
			credential *unix.Ucred
			socketErr  error
		)
		if err := raw.Control(func(fd uintptr) {
			credential, socketErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		}); err != nil {
			return 0, err
		}
		if socketErr != nil {
			return 0, socketErr
		}
		if credential == nil {
			return 0, errors.New("operator peer credentials are unavailable")
		}
		return int(credential.Uid), nil
	}
}
