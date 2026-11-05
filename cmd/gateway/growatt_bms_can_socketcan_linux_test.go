//go:build linux

package main

import (
	"errors"
	"testing"
	"time"
)

func TestGrowattBMSCANSocketMissingInterfaceOpensNothing(t *testing.T) {
	receiver, err := openGrowattBMSCANSocket("hlcan404")
	if err == nil || receiver != nil {
		t.Fatalf("receiver = %v, err = %v", receiver, err)
	}
	config := growattBMSCANConfig{Enabled: true, Interface: "hlcan404", ListenOnlyConfirmed: true, StaleAfter: time.Second}
	driver, runtime, err := startGrowattBMSCANDriver(config)
	if err == nil || errors.Is(err, errGrowattBMSCANConfig) || driver != nil || runtime != nil {
		t.Fatalf("driver = %v, runtime = %v, err = %v", driver, runtime, err)
	}
}
