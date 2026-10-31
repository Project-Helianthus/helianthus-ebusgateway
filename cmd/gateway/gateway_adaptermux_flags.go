package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux"
)

type adapterMuxTiming struct {
	IdleReleaseGrace     time.Duration
	MaxOwnershipDuration time.Duration
	ReadTimeout          time.Duration
}

var adapterMuxOptions = registerAdapterMuxFlags(flag.CommandLine)

func registerAdapterMuxFlags(fs *flag.FlagSet) *adapterMuxTiming {
	timing := &adapterMuxTiming{}
	fs.DurationVar(&timing.IdleReleaseGrace, "adapter-mux-idle-release-grace", 200*time.Millisecond, "adapter mux idle release grace")
	fs.DurationVar(&timing.MaxOwnershipDuration, "adapter-mux-max-ownership-duration", 10*time.Second, "adapter mux maximum continuous ownership duration")
	fs.DurationVar(&timing.ReadTimeout, "adapter-mux-read-timeout", 50*time.Millisecond, "adapter mux read timeout")
	return timing
}

func (timing adapterMuxTiming) apply(cfg *adaptermux.Config) error {
	if timing.IdleReleaseGrace <= 0 || timing.MaxOwnershipDuration <= 0 || timing.ReadTimeout <= 0 {
		return fmt.Errorf("adapter mux timing durations must be positive")
	}
	cfg.IdleReleaseGrace = timing.IdleReleaseGrace
	cfg.MaxOwnershipDuration = timing.MaxOwnershipDuration
	cfg.ReadTimeout = timing.ReadTimeout
	return nil
}
