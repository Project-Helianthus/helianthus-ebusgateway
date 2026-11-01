//go:build !linux && !darwin

package adaptermux

import (
	"errors"
	"net"
)

func openSerialConn(string, string) (net.Conn, error) {
	return nil, errors.New("serial adapter devices are supported on linux and darwin")
}
