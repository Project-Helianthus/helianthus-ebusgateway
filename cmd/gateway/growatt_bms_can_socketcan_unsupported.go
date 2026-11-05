//go:build !linux

package main

import "errors"

var errGrowattBMSCANUnsupported = errors.New("growatt BMS CAN SocketCAN receive is unsupported on this platform")

func openGrowattBMSCANSocket(name string) (growattBMSCANReceiver, error) {
	if !validGrowattBMSCANInterfaceName(name) {
		return nil, errGrowattBMSCANConfig
	}
	return nil, errGrowattBMSCANUnsupported
}
