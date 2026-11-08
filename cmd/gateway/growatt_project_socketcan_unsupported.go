//go:build !linux

package main

import "errors"

func openGrowattProjectSocket(string) (growattProjectReceiver, error) {
	return nil, errors.New("selected PCS/LINK SocketCAN receive is unsupported on this platform")
}
