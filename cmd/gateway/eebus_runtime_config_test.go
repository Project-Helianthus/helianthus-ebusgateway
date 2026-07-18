package main

import (
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const testEEBusStateRoot = "/var/lib/helianthus/eebus"

type eebusResolverProbe struct {
	addresses []netip.Addr
	err       error
	calls     []string
}

func (probe *eebusResolverProbe) resolve(interfaceName string) ([]netip.Addr, error) {
	probe.calls = append(probe.calls, interfaceName)
	return probe.addresses, probe.err
}

func validEEBusRuntimeGatewayConfig() ebusgateway.EEBusConfig {
	return ebusgateway.EEBusConfig{
		Enabled:            true,
		ListenPort:         4712,
		Interfaces:         []string{"en0"},
		Subnets:            []string{"192.0.2.0/24"},
		StateRoot:          testEEBusStateRoot,
		RemoteSKIAllowlist: []string{},
		PairingWindowMode:  ebusgateway.EEBusPairingWindowClosed,
	}
}

func TestMapEEBusRuntimeConfig_DisabledProductsAreInert(t *testing.T) {
	tests := []struct {
		name string
		cfg  ebusgateway.EEBusConfig
	}{
		{name: "exact zero product", cfg: ebusgateway.EEBusConfig{}},
		{name: "gateway inert default", cfg: ebusgateway.DefaultEEBusConfig()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probe := &eebusResolverProbe{
				addresses: []netip.Addr{netip.MustParseAddr("192.0.2.42")},
			}
			resolve := eebusInterfaceAddressResolver(probe.resolve)

			got, err := mapEEBusRuntimeConfig(test.cfg, resolve)
			if err != nil {
				t.Fatalf("map disabled config: %v", err)
			}
			if len(probe.calls) != 0 {
				t.Fatalf("resolver calls = %v; want none", probe.calls)
			}
			if want := (eebusruntime.Config{}); !reflect.DeepEqual(got, want) {
				t.Fatalf("runtime config = %+v; want zero value", got)
			}
		})
	}
}

func TestMapEEBusRuntimeConfig_DisabledRejectsActiveInputBeforeResolver(t *testing.T) {
	remoteSKI := strings.Repeat("a", 40)
	tests := []struct {
		name   string
		mutate func(*ebusgateway.EEBusConfig)
	}{
		{name: "listen port", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.ListenPort = 4712 }},
		{name: "interface", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.Interfaces = []string{"en0"} }},
		{name: "subnet", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.Subnets = []string{"192.0.2.0/24"} }},
		{name: "state root", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.StateRoot = testEEBusStateRoot }},
		{name: "discovery", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.DiscoveryEnabled = true }},
		{name: "certificate path", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.CertificatePath = "/tmp/cert.pem" }},
		{name: "private key path", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.PrivateKeyPath = "/tmp/key.pem" }},
		{name: "trust store path", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.TrustStorePath = "/tmp/trust.json" }},
		{name: "remote allowlist", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.RemoteSKIAllowlist = []string{remoteSKI} }},
		{name: "non-closed pairing", mutate: func(cfg *ebusgateway.EEBusConfig) {
			cfg.PairingWindowMode = ebusgateway.EEBusPairingWindowMode("open")
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := ebusgateway.EEBusConfig{}
			test.mutate(&cfg)
			probe := &eebusResolverProbe{
				addresses: []netip.Addr{netip.MustParseAddr("192.0.2.42")},
			}

			got, err := mapEEBusRuntimeConfig(cfg, eebusInterfaceAddressResolver(probe.resolve))
			if err == nil {
				t.Fatal("map disabled active config error = nil; want rejection")
			}
			if len(probe.calls) != 0 {
				t.Fatalf("resolver calls = %v; want none", probe.calls)
			}
			if want := (eebusruntime.Config{}); !reflect.DeepEqual(got, want) {
				t.Fatalf("runtime config on error = %+v; want zero value", got)
			}
		})
	}
}

func TestMapEEBusRuntimeConfig_MapsEnabledProductLosslessly(t *testing.T) {
	firstSKI := strings.Repeat("B", 40)
	secondSKI := strings.Repeat("a", 40)
	cfg := validEEBusRuntimeGatewayConfig()
	cfg.DiscoveryEnabled = true
	cfg.RemoteSKIAllowlist = []string{firstSKI, secondSKI}
	address := netip.MustParseAddr("192.0.2.42")
	probe := &eebusResolverProbe{addresses: []netip.Addr{address}}

	got, err := mapEEBusRuntimeConfig(cfg, eebusInterfaceAddressResolver(probe.resolve))
	if err != nil {
		t.Fatalf("map enabled config: %v", err)
	}
	want := eebusruntime.Config{
		Enabled:          true,
		StateRoot:        testEEBusStateRoot,
		Interface:        "en0",
		ListenAddress:    netip.AddrPortFrom(address, 4712),
		DiscoveryEnabled: true,
		Remotes: []eebusruntime.Remote{
			{SKI: firstSKI},
			{SKI: secondSKI},
		},
		PairingPolicy: eebusruntime.PairingPolicyClosed,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime config = %+v; want %+v", got, want)
	}
	if wantCalls := []string{"en0"}; !reflect.DeepEqual(probe.calls, wantCalls) {
		t.Fatalf("resolver calls = %v; want %v", probe.calls, wantCalls)
	}
}

func TestMapEEBusRuntimeConfig_ExplicitEmptyAllowlistStaysNonNil(t *testing.T) {
	cfg := validEEBusRuntimeGatewayConfig()
	cfg.RemoteSKIAllowlist = make([]string, 0)
	probe := &eebusResolverProbe{
		addresses: []netip.Addr{netip.MustParseAddr("192.0.2.42")},
	}

	got, err := mapEEBusRuntimeConfig(cfg, eebusInterfaceAddressResolver(probe.resolve))
	if err != nil {
		t.Fatalf("map enabled config: %v", err)
	}
	if got.Remotes == nil {
		t.Fatal("runtime Remotes = nil; want explicit non-nil empty slice")
	}
	if len(got.Remotes) != 0 {
		t.Fatalf("runtime Remotes = %v; want empty", got.Remotes)
	}
}

func TestMapEEBusRuntimeConfig_RejectsIncompleteEnabledProductBeforeResolver(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ebusgateway.EEBusConfig)
	}{
		{name: "missing state root", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.StateRoot = "" }},
		{name: "relative state root", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.StateRoot = "var/lib/eebus" }},
		{name: "state root traversal", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.StateRoot = "/var/lib/helianthus/../eebus" }},
		{name: "filesystem root", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.StateRoot = "/" }},
		{name: "zero port", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.ListenPort = 0 }},
		{name: "missing interface", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.Interfaces = nil }},
		{name: "blank interface", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.Interfaces = []string{""} }},
		{name: "wildcard interface", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.Interfaces = []string{"*"} }},
		{name: "multiple interfaces", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.Interfaces = []string{"en0", "en1"} }},
		{name: "missing subnets", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.Subnets = nil }},
		{name: "invalid subnet", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.Subnets = []string{"not-a-prefix"} }},
		{name: "pairing policy missing", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.PairingWindowMode = "" }},
		{name: "pairing policy open", mutate: func(cfg *ebusgateway.EEBusConfig) {
			cfg.PairingWindowMode = ebusgateway.EEBusPairingWindowMode("open")
		}},
		{name: "legacy certificate path", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.CertificatePath = "/tmp/cert.pem" }},
		{name: "legacy private key path", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.PrivateKeyPath = "/tmp/key.pem" }},
		{name: "legacy trust store path", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.TrustStorePath = "/tmp/trust.json" }},
		{name: "invalid remote SKI", mutate: func(cfg *ebusgateway.EEBusConfig) { cfg.RemoteSKIAllowlist = []string{"abcd"} }},
		{name: "duplicate remote SKI", mutate: func(cfg *ebusgateway.EEBusConfig) {
			remoteSKI := strings.Repeat("a", 40)
			cfg.RemoteSKIAllowlist = []string{remoteSKI, remoteSKI}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validEEBusRuntimeGatewayConfig()
			test.mutate(&cfg)
			resolverErr := errors.New("resolver must not run")
			probe := &eebusResolverProbe{err: resolverErr}

			got, err := mapEEBusRuntimeConfig(cfg, eebusInterfaceAddressResolver(probe.resolve))
			if err == nil {
				t.Fatal("map incomplete enabled config error = nil; want rejection")
			}
			if errors.Is(err, resolverErr) {
				t.Fatalf("mapping error %q was masked by resolver", err)
			}
			if len(probe.calls) != 0 {
				t.Fatalf("resolver calls = %v; want none", probe.calls)
			}
			if want := (eebusruntime.Config{}); !reflect.DeepEqual(got, want) {
				t.Fatalf("runtime config on error = %+v; want zero value", got)
			}
		})
	}
}

func TestMapEEBusRuntimeConfig_ResolvesOneUniqueAddressInsideSubnetUnion(t *testing.T) {
	tests := []struct {
		name          string
		interfaceName string
		subnets       []string
		addresses     []netip.Addr
		want          netip.Addr
	}{
		{
			name:          "portable IPv4",
			interfaceName: "en0",
			subnets:       []string{"192.0.2.0/24"},
			addresses: []netip.Addr{
				netip.MustParseAddr("198.51.100.10"),
				netip.MustParseAddr("192.0.2.42"),
			},
			want: netip.MustParseAddr("192.0.2.42"),
		},
		{
			name:          "deduplicated address and overlapping prefixes",
			interfaceName: "en0",
			subnets:       []string{"192.0.2.0/24", "192.0.2.0/25", "192.0.2.0/24"},
			addresses: []netip.Addr{
				netip.MustParseAddr("192.0.2.42"),
				netip.MustParseAddr("192.0.2.42"),
			},
			want: netip.MustParseAddr("192.0.2.42"),
		},
		{
			name:          "IPv4 /31 endpoint remains a host",
			interfaceName: "en0",
			subnets:       []string{"192.0.2.0/31"},
			addresses:     []netip.Addr{netip.MustParseAddr("192.0.2.0")},
			want:          netip.MustParseAddr("192.0.2.0"),
		},
		{
			name:          "IPv4 /31 host survives an earlier overlapping network classification",
			interfaceName: "en0",
			subnets:       []string{"192.0.2.0/24", "192.0.2.0/31"},
			addresses:     []netip.Addr{netip.MustParseAddr("192.0.2.0")},
			want:          netip.MustParseAddr("192.0.2.0"),
		},
		{
			name:          "IPv4 /31 host survives a later overlapping network classification",
			interfaceName: "en0",
			subnets:       []string{"192.0.2.0/31", "192.0.2.0/24"},
			addresses:     []netip.Addr{netip.MustParseAddr("192.0.2.0")},
			want:          netip.MustParseAddr("192.0.2.0"),
		},
		{
			name:          "IPv4 /32 endpoint remains a host",
			interfaceName: "en0",
			subnets:       []string{"192.0.2.42/32"},
			addresses:     []netip.Addr{netip.MustParseAddr("192.0.2.42")},
			want:          netip.MustParseAddr("192.0.2.42"),
		},
		{
			name:          "IPv6 link-local zone is preserved",
			interfaceName: "en0",
			subnets:       []string{"fe80::/64"},
			addresses:     []netip.Addr{netip.MustParseAddr("fe80::1234%en0")},
			want:          netip.MustParseAddr("fe80::1234%en0"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validEEBusRuntimeGatewayConfig()
			cfg.Interfaces = []string{test.interfaceName}
			cfg.Subnets = test.subnets
			probe := &eebusResolverProbe{addresses: test.addresses}

			got, err := mapEEBusRuntimeConfig(cfg, eebusInterfaceAddressResolver(probe.resolve))
			if err != nil {
				t.Fatalf("map enabled config: %v", err)
			}
			if want := netip.AddrPortFrom(test.want, cfg.ListenPort); got.ListenAddress != want {
				t.Fatalf("ListenAddress = %v; want %v", got.ListenAddress, want)
			}
			if wantCalls := []string{test.interfaceName}; !reflect.DeepEqual(probe.calls, wantCalls) {
				t.Fatalf("resolver calls = %v; want %v", probe.calls, wantCalls)
			}
		})
	}
}

func TestMapEEBusRuntimeConfig_RejectsInvalidOrAmbiguousResolvedAddress(t *testing.T) {
	tests := []struct {
		name      string
		subnets   []string
		addresses []netip.Addr
	}{
		{name: "zero matches", subnets: []string{"192.0.2.0/24"}, addresses: []netip.Addr{netip.MustParseAddr("198.51.100.10")}},
		{name: "multiple unique matches", subnets: []string{"192.0.2.0/24"}, addresses: []netip.Addr{
			netip.MustParseAddr("192.0.2.42"),
			netip.MustParseAddr("192.0.2.43"),
		}},
		{name: "invalid zero address", subnets: []string{"0.0.0.0/0"}, addresses: []netip.Addr{{}}},
		{name: "unspecified IPv4", subnets: []string{"0.0.0.0/0"}, addresses: []netip.Addr{netip.MustParseAddr("0.0.0.0")}},
		{name: "unspecified IPv6", subnets: []string{"::/0"}, addresses: []netip.Addr{netip.MustParseAddr("::")}},
		{name: "multicast IPv4", subnets: []string{"0.0.0.0/0"}, addresses: []netip.Addr{netip.MustParseAddr("224.0.0.1")}},
		{name: "multicast IPv6", subnets: []string{"::/0"}, addresses: []netip.Addr{netip.MustParseAddr("ff02::1")}},
		{name: "IPv4-mapped IPv6", subnets: []string{"::/0"}, addresses: []netip.Addr{netip.MustParseAddr("::ffff:192.0.2.42")}},
		{name: "global IPv4 broadcast", subnets: []string{"0.0.0.0/0"}, addresses: []netip.Addr{netip.MustParseAddr("255.255.255.255")}},
		{name: "IPv4 subnet network address", subnets: []string{"192.0.2.0/24"}, addresses: []netip.Addr{netip.MustParseAddr("192.0.2.0")}},
		{name: "IPv4 directed broadcast address", subnets: []string{"192.0.2.0/24"}, addresses: []netip.Addr{netip.MustParseAddr("192.0.2.255")}},
		{name: "IPv6 link-local missing zone", subnets: []string{"fe80::/64"}, addresses: []netip.Addr{netip.MustParseAddr("fe80::1234")}},
		{name: "IPv6 link-local mismatched zone", subnets: []string{"fe80::/64"}, addresses: []netip.Addr{netip.MustParseAddr("fe80::1234%en1")}},
		{name: "IPv6 global address with zone", subnets: []string{"2001:db8::/64"}, addresses: []netip.Addr{netip.MustParseAddr("2001:db8::1234%en0")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validEEBusRuntimeGatewayConfig()
			cfg.Subnets = test.subnets
			probe := &eebusResolverProbe{addresses: test.addresses}

			got, err := mapEEBusRuntimeConfig(cfg, eebusInterfaceAddressResolver(probe.resolve))
			if err == nil {
				t.Fatal("map resolved address error = nil; want rejection")
			}
			if wantCalls := []string{"en0"}; !reflect.DeepEqual(probe.calls, wantCalls) {
				t.Fatalf("resolver calls = %v; want %v", probe.calls, wantCalls)
			}
			if want := (eebusruntime.Config{}); !reflect.DeepEqual(got, want) {
				t.Fatalf("runtime config on error = %+v; want zero value", got)
			}
		})
	}
}

func TestMapEEBusRuntimeConfig_PreservesResolverError(t *testing.T) {
	cfg := validEEBusRuntimeGatewayConfig()
	wantErr := errors.New("interface address lookup failed")
	probe := &eebusResolverProbe{err: wantErr}

	got, err := mapEEBusRuntimeConfig(cfg, eebusInterfaceAddressResolver(probe.resolve))
	if !errors.Is(err, wantErr) {
		t.Fatalf("mapping error = %v; want resolver error %v", err, wantErr)
	}
	if wantCalls := []string{"en0"}; !reflect.DeepEqual(probe.calls, wantCalls) {
		t.Fatalf("resolver calls = %v; want %v", probe.calls, wantCalls)
	}
	if want := (eebusruntime.Config{}); !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime config on error = %+v; want zero value", got)
	}
}

func TestMapEEBusRuntimeConfig_ValidationPrecedenceIsDeterministic(t *testing.T) {
	cfg := validEEBusRuntimeGatewayConfig()
	cfg.StateRoot = ""
	cfg.ListenPort = 0
	cfg.Interfaces = nil
	cfg.Subnets = nil
	cfg.PairingWindowMode = ""
	cfg.CertificatePath = "/tmp/unsupported.pem"
	resolverErr := errors.New("later resolver error")
	probe := &eebusResolverProbe{err: resolverErr}

	var firstError string
	for attempt := 0; attempt < 5; attempt++ {
		got, err := mapEEBusRuntimeConfig(cfg, eebusInterfaceAddressResolver(probe.resolve))
		if err == nil {
			t.Fatal("map multiply-invalid config error = nil; want rejection")
		}
		if errors.Is(err, resolverErr) {
			t.Fatalf("validation error %q was masked by resolver", err)
		}
		if attempt == 0 {
			firstError = err.Error()
		} else if err.Error() != firstError {
			t.Fatalf("attempt %d error = %q; first error was %q", attempt, err, firstError)
		}
		if want := (eebusruntime.Config{}); !reflect.DeepEqual(got, want) {
			t.Fatalf("runtime config on error = %+v; want zero value", got)
		}
	}
	if len(probe.calls) != 0 {
		t.Fatalf("resolver calls = %v; want none", probe.calls)
	}
}
