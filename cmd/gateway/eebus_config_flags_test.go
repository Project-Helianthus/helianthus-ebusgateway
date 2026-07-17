package main

import (
	"flag"
	"reflect"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func TestBindFlags_EEBusScaffoldNormalizesExplicitValues(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	err := fs.Parse([]string{
		"-eebus-enabled",
		"-eebus-listen-port", "4712",
		"-eebus-interfaces", " en0, en1,en0 ,,en2 ",
		"-eebus-subnets", "2001:db8::42/64,192.0.2.17/24,192.0.2.0/24",
		"-eebus-certificate-path", " /data/eebus/cert.pem ",
		"-eebus-private-key-path", " /data/eebus/key.pem ",
		"-eebus-trust-store-path", " /data/eebus/trust.json ",
		"-eebus-remote-ski-allowlist", strings.Repeat("B", 40) + "," + strings.Repeat("a", 40) + "," + strings.Repeat("b", 40),
		"-eebus-pairing-window-mode", " CLOSED ",
	})
	if err != nil {
		t.Fatalf("parse eeBUS flags: %v", err)
	}

	got := cfg.EEBusConfig
	if !got.Enabled || got.ListenPort != 4712 {
		t.Fatalf("Enabled/ListenPort = %v/%d; want true/4712", got.Enabled, got.ListenPort)
	}
	if want := []string{"en0", "en1", "en2"}; !reflect.DeepEqual(got.Interfaces, want) {
		t.Fatalf("Interfaces = %v; want %v", got.Interfaces, want)
	}
	if want := []string{"192.0.2.0/24", "2001:db8::/64"}; !reflect.DeepEqual(got.Subnets, want) {
		t.Fatalf("Subnets = %v; want %v", got.Subnets, want)
	}
	if got.CertificatePath != "/data/eebus/cert.pem" ||
		got.PrivateKeyPath != "/data/eebus/key.pem" ||
		got.TrustStorePath != "/data/eebus/trust.json" {
		t.Fatalf("paths not trimmed: %+v", got)
	}
	if want := []string{strings.Repeat("a", 40), strings.Repeat("b", 40)}; !reflect.DeepEqual(got.RemoteSKIAllowlist, want) {
		t.Fatalf("RemoteSKIAllowlist = %v; want %v", got.RemoteSKIAllowlist, want)
	}
	if got.PairingWindowMode != ebusgateway.EEBusPairingWindowClosed {
		t.Fatalf("PairingWindowMode = %q; want closed", got.PairingWindowMode)
	}
}

func TestBindFlags_EEBusListFlagsLastOccurrenceWins(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	err := fs.Parse([]string{
		"-eebus-interfaces", "en0,en1",
		"-eebus-interfaces", "eth0",
		"-eebus-subnets", "192.0.2.0/24",
		"-eebus-subnets", "2001:db8::/64",
	})
	if err != nil {
		t.Fatalf("parse repeated eeBUS list flags: %v", err)
	}
	if want := []string{"eth0"}; !reflect.DeepEqual(cfg.EEBusConfig.Interfaces, want) {
		t.Fatalf("Interfaces = %v; want %v", cfg.EEBusConfig.Interfaces, want)
	}
	if want := []string{"2001:db8::/64"}; !reflect.DeepEqual(cfg.EEBusConfig.Subnets, want) {
		t.Fatalf("Subnets = %v; want %v", cfg.EEBusConfig.Subnets, want)
	}
}

func TestBindFlags_EEBusRejectsInvalidValuesWithoutMutation(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "negative port", args: []string{"-eebus-listen-port", "-1"}},
		{name: "overflow port", args: []string{"-eebus-listen-port", "65536"}},
		{name: "non-decimal port", args: []string{"-eebus-listen-port", "0x1268"}},
		{name: "invalid subnet", args: []string{"-eebus-subnets", "not-a-prefix"}},
		{name: "short ski", args: []string{"-eebus-remote-ski-allowlist", "abcd"}},
		{name: "non-hex ski", args: []string{"-eebus-remote-ski-allowlist", strings.Repeat("z", 40)}},
		{name: "future pairing mode", args: []string{"-eebus-pairing-window-mode", "manual"}},
		{name: "certificate path NUL", args: []string{"-eebus-certificate-path", "cert\x00.pem"}},
		{name: "private key path NUL", args: []string{"-eebus-private-key-path", "key\x00.pem"}},
		{name: "trust store path NUL", args: []string{"-eebus-trust-store-path", "trust\x00.json"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := ebusgateway.DefaultConfig()
			before := cfg.EEBusConfig
			fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
			bindFlags(fs, &cfg)
			if err := fs.Parse(test.args); err == nil {
				t.Fatalf("Parse(%q) error = nil; want deterministic rejection", test.args)
			}
			if !reflect.DeepEqual(cfg.EEBusConfig, before) {
				t.Fatalf("invalid input mutated config: before=%+v after=%+v", before, cfg.EEBusConfig)
			}
		})
	}
}

func TestBindFlags_EEBusEmptyListsRemainExplicitlyEmpty(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{
		"-eebus-interfaces", " , , ",
		"-eebus-subnets", " , ",
		"-eebus-remote-ski-allowlist", " , ",
	}); err != nil {
		t.Fatalf("parse explicit empty lists: %v", err)
	}
	if len(cfg.EEBusConfig.Interfaces) != 0 || len(cfg.EEBusConfig.Subnets) != 0 || len(cfg.EEBusConfig.RemoteSKIAllowlist) != 0 {
		t.Fatalf("empty list flags widened config: %+v", cfg.EEBusConfig)
	}
}
