//go:build !linux

package main

import (
	"errors"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/teslaobserve"
)

func openTeslaObserveSerial(string) (teslaobserve.ByteSource, error) {
	return nil, errors.New("receive-only serial observation requires Linux")
}
