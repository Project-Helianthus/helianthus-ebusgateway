//go:build linux || darwin

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/ptytest"
)

func TestSerialNetworkNamesAgree(t *testing.T) {
	if ebusgateway.TransportNetworkSerial != adaptermux.NetworkSerial {
		t.Fatalf("gateway %q, mux %q", ebusgateway.TransportNetworkSerial, adaptermux.NetworkSerial)
	}
}

// Kills: a serial form that does not reach the mux unchanged. Every row runs the
// process shell's normalization and the adapter-direct wiring against a
// pseudo-terminal; only the opened device with the right speed passes.
func TestWireAdapterDirect_SerialFormsReachTheMux(t *testing.T) {
	for _, test := range []struct {
		name   string
		config func(device string) ebusgateway.TransportConfig
		baud   uint32
	}{
		{"add-on ens uri", func(device string) ebusgateway.TransportConfig {
			return ebusgateway.TransportConfig{Protocol: ebusgateway.TransportAdapterDirect, Network: "tcp", Address: "adapter-direct-ens://" + device}
		}, 115200},
		{"add-on enh uri", func(device string) ebusgateway.TransportConfig {
			return ebusgateway.TransportConfig{Protocol: ebusgateway.TransportAdapterDirect, Network: "tcp", Address: "adapter-direct://" + device}
		}, 9600},
		{"ens path-only", func(device string) ebusgateway.TransportConfig {
			return ebusgateway.TransportConfig{Address: "ens:" + device}
		}, 115200},
		{"enh path-only", func(device string) ebusgateway.TransportConfig {
			return ebusgateway.TransportConfig{Address: "enh:" + device}
		}, 9600},
		{"bare path and transport flag", func(device string) ebusgateway.TransportConfig {
			return ebusgateway.TransportConfig{Protocol: ebusgateway.TransportENS, Network: "unix", Address: device}
		}, 115200},
	} {
		t.Run(test.name, func(t *testing.T) {
			pair := ptytest.Open(t)
			adapter := ptytest.ServeAdapter(pair)
			cfg := ebusgateway.Config{TransportConfig: test.config(pair.SlavePath)}
			if got := ebusgateway.EBusDriverTransportProtocol(cfg.TransportConfig); got != ebusgateway.TransportAdapterDirect {
				t.Fatalf("driver protocol = %q, want adapter-direct", got)
			}
			applyTransportSourcePolicy(&cfg)

			closer, _, err := wireAdapterDirect(context.Background(), &cfg)
			if err != nil {
				t.Fatalf("wire: %v", err)
			}
			defer func() { _ = closer() }()

			adapter.WaitInit(t, 10*time.Second)
			if got := pair.Baud(t); got != test.baud {
				t.Fatalf("speed = %d, want %d", got, test.baud)
			}
			if cfg.TransportConfig.Network != ebusgateway.TransportNetworkSerial || cfg.TransportConfig.Address != pair.SlavePath {
				t.Fatalf("normalized tuple %q %q", cfg.TransportConfig.Network, cfg.TransportConfig.Address)
			}
		})
	}
}

// An absent device is a dependency loss the driver retries with its backoff,
// not a rejected configuration.
func TestWireAdapterDirect_AbsentSerialDeviceIsRetryable(t *testing.T) {
	cfg := ebusgateway.Config{TransportConfig: ebusgateway.TransportConfig{Address: "ens:/dev/helianthus-test-absent-device"}}
	_, _, err := wireAdapterDirect(context.Background(), &cfg)
	if err == nil {
		t.Fatal("wire succeeded without a device")
	}
	var rejection *ebusConfigRejection
	if errors.As(err, &rejection) {
		t.Fatalf("absent device rejected as configuration: %v", err)
	}
	if failure := classifyEBusDriverError(err); !failure.Reason.Retryable {
		t.Fatalf("failure %+v is not retryable", failure)
	}
}
