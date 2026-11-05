//go:build !linux

package main

import (
	"errors"
	"testing"
	"time"
)

func TestGrowattBMSCANSocketUnsupportedPlatformFailsClosed(t *testing.T) {
	receiver, err := openGrowattBMSCANSocket("can7")
	if !errors.Is(err, errGrowattBMSCANUnsupported) || receiver != nil {
		t.Fatalf("receiver = %v, err = %v", receiver, err)
	}
	config := growattBMSCANConfig{Enabled: true, Interface: "can7", ListenOnlyConfirmed: true, StaleAfter: time.Second}
	driver, runtime, err := startGrowattBMSCANDriver(config)
	if !errors.Is(err, errGrowattBMSCANUnsupported) || driver != nil || runtime != nil {
		t.Fatalf("driver = %v, runtime = %v, err = %v", driver, runtime, err)
	}
}
