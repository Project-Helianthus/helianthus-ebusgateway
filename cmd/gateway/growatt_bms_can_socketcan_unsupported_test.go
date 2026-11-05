//go:build !linux

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
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

func TestCAN03ManagedUnsupportedPlatformFailsClosed(t *testing.T) {
	adapter := newGrowattBMSCANManagedFactory(growattBMSCANConfig{Enabled: true, Interface: "can7", ListenOnlyConfirmed: true, StaleAfter: time.Second}, nil)
	native, err := adapter.Construct(context.Background(), runtimecontrol.Construction{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = native.Start(context.Background()); !errors.Is(err, errGrowattBMSCANPlatform) {
		t.Fatalf("managed start = %v; want unsupported Linux SocketCAN reason", err)
	}
}
