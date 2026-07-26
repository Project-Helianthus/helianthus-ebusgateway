//go:build !linux

package mcp

import (
	"errors"
	"net"
)

func eebusV1PlatformListenOperator(string) (net.Listener, error) {
	return nil, errors.New("eeBUS operator AF_UNIX filesystem proof is unsupported")
}
