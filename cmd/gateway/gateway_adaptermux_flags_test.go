package main

import (
	"flag"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux"
)

func TestT02_AdapterMuxTimingFlags(t *testing.T) {
	fs := flag.NewFlagSet("timing", flag.ContinueOnError)
	cfg := ebusgateway.DefaultConfig()
	bindFlags(fs, &cfg)
	timing := registerAdapterMuxFlags(fs)
	if err := fs.Parse([]string{"--adapter-mux-idle-release-grace=80ms", "--adapter-mux-max-ownership-duration=3s", "--adapter-mux-read-timeout=70ms"}); err != nil {
		t.Fatal(err)
	}
	muxCfg := adaptermux.Config{ReadTimeout: cfg.TransportConfig.ReadTimeout}
	if err := timing.apply(&muxCfg); err != nil {
		t.Fatal(err)
	}
	if muxCfg.IdleReleaseGrace != 80*time.Millisecond || muxCfg.MaxOwnershipDuration != 3*time.Second || muxCfg.ReadTimeout != 70*time.Millisecond {
		t.Fatalf("timing=%+v", muxCfg)
	}
}

func TestT02_AdapterMuxTimingDefaultsAndValidation(t *testing.T) {
	fs := flag.NewFlagSet("timing", flag.ContinueOnError)
	timing := registerAdapterMuxFlags(fs)
	cfg := adaptermux.Config{}
	if err := timing.apply(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.IdleReleaseGrace != 200*time.Millisecond || cfg.MaxOwnershipDuration != 10*time.Second || cfg.ReadTimeout != 50*time.Millisecond {
		t.Fatalf("defaults=%+v", cfg)
	}
	for _, field := range []string{"idle", "ownership", "read"} {
		t.Run(field, func(t *testing.T) {
			bad := *timing
			cfg := adaptermux.Config{}
			switch field {
			case "idle":
				bad.IdleReleaseGrace = -time.Second
			case "ownership":
				bad.MaxOwnershipDuration = -time.Second
			case "read":
				bad.ReadTimeout = -time.Second
			}
			if err := bad.apply(&cfg); err == nil {
				t.Fatal("negative duration accepted")
			}
		})
	}
}
