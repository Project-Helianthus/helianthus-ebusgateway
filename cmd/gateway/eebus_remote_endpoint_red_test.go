package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/netip"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const (
	testRemoteEndpointSKIA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testRemoteEndpointSKIB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testRemoteEndpointIPv4 = "192.0.2.21:12480/ship/"
	testRemoteEndpointIPv6 = "[2001:db8:1::21]:12480/ship/"
)

var (
	remoteEndpointIndexPattern  = regexp.MustCompile(`\bindex=[0-9]+\b`)
	remoteEndpointReasonPattern = regexp.MustCompile(`\breason=[a-z][a-z0-9_]*\b`)
)

type remoteEndpointRuntime struct{}

func (*remoteEndpointRuntime) Start(context.Context) error { return nil }
func (*remoteEndpointRuntime) Shutdown() error             { return nil }
func (*remoteEndpointRuntime) Snapshot() (eebusruntime.SnapshotV1, error) {
	return eebusruntime.SnapshotV1{}, nil
}
func (*remoteEndpointRuntime) PairingState() ([]eebusruntime.PairingObservationV1, error) {
	return nil, nil
}

func TestBindFlags_EEBusRemoteEndpointIsSingularRepeatableSyntax(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	bindFlags(fs, &cfg)

	want := []string{
		testRemoteEndpointSKIB + "=" + testRemoteEndpointIPv4,
		testRemoteEndpointSKIA + "=" + testRemoteEndpointIPv6,
	}
	args := []string{
		"-eebus-remote-endpoint", want[0],
		"-eebus-remote-endpoint", want[1],
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse repeatable -eebus-remote-endpoint: %v", err)
	}
	if got := remoteEndpointSpecs(t, &cfg.EEBusConfig); !reflect.DeepEqual(got, want) {
		t.Fatalf("RemoteEndpoints = %v; want declaration-ordered %v", got, want)
	}
}

func TestBindFlags_EEBusRemoteEndpointDefersRawValueErrorsToPreStartMapping(t *testing.T) {
	raw := testRemoteEndpointSKIA + "=fixture.invalid:12480/ship/"
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	bindFlags(fs, &cfg)

	if err := fs.Parse([]string{"-eebus-remote-endpoint", raw}); err != nil {
		if strings.Contains(err.Error(), raw) {
			t.Fatalf("flag parsing leaked raw remote endpoint: %v", err)
		}
		t.Fatalf("flag parsing must defer remote endpoint validation to pre-start mapping: %v", err)
	}
	if got := remoteEndpointSpecs(t, &cfg.EEBusConfig); !reflect.DeepEqual(got, []string{raw}) {
		t.Fatalf("RemoteEndpoints = %v; want deferred raw value", got)
	}
}

func TestEEBusRuntimeRemoteV1DependencyContract(t *testing.T) {
	type fieldContract struct {
		name string
		typ  reflect.Type
	}
	want := []fieldContract{
		{name: "SKI", typ: reflect.TypeOf("")},
		{name: "Endpoint", typ: reflect.TypeOf(netip.AddrPort{})},
		{name: "EndpointPath", typ: reflect.TypeOf("")},
	}

	typ := reflect.TypeOf(eebusruntime.Remote{})
	if typ.NumField() != len(want) {
		t.Fatalf("eebusruntime.Remote field count = %d; want public v1 field count %d", typ.NumField(), len(want))
	}
	for index, expected := range want {
		field := typ.Field(index)
		if field.Name != expected.name || field.Type != expected.typ {
			t.Fatalf("eebusruntime.Remote field %d = %s %s; want %s %s", index, field.Name, field.Type, expected.name, expected.typ)
		}
	}
}

func TestMapEEBusRuntimeConfig_RejectsUnsafeRemoteEndpointsBeforeResolver(t *testing.T) {
	validSpec := testRemoteEndpointSKIA + "=" + testRemoteEndpointIPv4
	tests := []struct {
		name      string
		allowlist []string
		subnets   []string
		specs     []string
		wantIndex int
	}{
		{name: "malformed binding", specs: []string{testRemoteEndpointSKIA + "@" + testRemoteEndpointIPv4}},
		{name: "short endpoint SKI", specs: []string{"abcd=" + testRemoteEndpointIPv4}},
		{name: "non-hex endpoint SKI", specs: []string{strings.Repeat("z", 40) + "=" + testRemoteEndpointIPv4}},
		{name: "unknown endpoint SKI", specs: []string{testRemoteEndpointSKIB + "=" + testRemoteEndpointIPv4}},
		{
			name:      "allowlist SKI occurs twice",
			allowlist: []string{testRemoteEndpointSKIA, strings.ToUpper(testRemoteEndpointSKIA)},
			specs:     []string{validSpec},
			wantIndex: 1,
		},
		{
			name:      "same SKI has duplicate mappings",
			specs:     []string{validSpec, testRemoteEndpointSKIA + "=192.0.2.22:12480/ship/"},
			wantIndex: 1,
		},
		{
			name:      "same endpoint is bound to two SKIs",
			allowlist: []string{testRemoteEndpointSKIA, testRemoteEndpointSKIB},
			specs:     []string{validSpec, testRemoteEndpointSKIB + "=" + testRemoteEndpointIPv4},
			wantIndex: 1,
		},
		{name: "DNS host", specs: []string{testRemoteEndpointSKIA + "=fixture.invalid:12480/ship/"}},
		{name: "unbracketed IPv6", specs: []string{testRemoteEndpointSKIA + "=2001:db8:1::21:12480/ship/"}},
		{name: "zero port", specs: []string{testRemoteEndpointSKIA + "=192.0.2.21:0/ship/"}},
		{name: "wildcard host", specs: []string{testRemoteEndpointSKIA + "=*:12480/ship/"}},
		{name: "unspecified IPv4", specs: []string{testRemoteEndpointSKIA + "=0.0.0.0:12480/ship/"}},
		{name: "unspecified IPv6", subnets: []string{"::/0"}, specs: []string{testRemoteEndpointSKIA + "=[::]:12480/ship/"}},
		{name: "multicast IPv4", subnets: []string{"0.0.0.0/0"}, specs: []string{testRemoteEndpointSKIA + "=224.0.0.1:12480/ship/"}},
		{name: "multicast IPv6", subnets: []string{"::/0"}, specs: []string{testRemoteEndpointSKIA + "=[ff02::1]:12480/ship/"}},
		{name: "IPv4-mapped IPv6", subnets: []string{"::/0"}, specs: []string{testRemoteEndpointSKIA + "=[::ffff:192.0.2.21]:12480/ship/"}},
		{name: "global broadcast", subnets: []string{"0.0.0.0/0"}, specs: []string{testRemoteEndpointSKIA + "=255.255.255.255:12480/ship/"}},
		{name: "subnet network address", specs: []string{testRemoteEndpointSKIA + "=192.0.2.0:12480/ship/"}},
		{name: "directed broadcast", specs: []string{testRemoteEndpointSKIA + "=192.0.2.255:12480/ship/"}},
		{name: "outside configured subnet", specs: []string{testRemoteEndpointSKIA + "=198.51.100.21:12480/ship/"}},
		{name: "missing path", specs: []string{testRemoteEndpointSKIA + "=192.0.2.21:12480"}},
		{name: "non-absolute path", specs: []string{testRemoteEndpointSKIA + "=192.0.2.21:12480ship/"}},
		{name: "query", specs: []string{testRemoteEndpointSKIA + "=192.0.2.21:12480/ship/?peer=x"}},
		{name: "fragment", specs: []string{testRemoteEndpointSKIA + "=192.0.2.21:12480/ship/#peer"}},
		{name: "backslash", specs: []string{testRemoteEndpointSKIA + `=192.0.2.21:12480/ship\admin`}},
		{name: "NUL", specs: []string{testRemoteEndpointSKIA + "=192.0.2.21:12480/ship/\x00admin"}},
		{name: "dot segment", specs: []string{testRemoteEndpointSKIA + "=192.0.2.21:12480/ship/./"}},
		{name: "dot-dot segment", specs: []string{testRemoteEndpointSKIA + "=192.0.2.21:12480/ship/../admin"}},
		{name: "encoded spelling", specs: []string{testRemoteEndpointSKIA + "=192.0.2.21:12480/%73hip/"}},
		{name: "encoded dot segment", specs: []string{testRemoteEndpointSKIA + "=192.0.2.21:12480/ship/%2e%2e/admin"}},
		{name: "duplicate separator", specs: []string{testRemoteEndpointSKIA + "=192.0.2.21:12480//ship/"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validEEBusRuntimeGatewayConfig()
			if test.allowlist == nil {
				cfg.RemoteSKIAllowlist = []string{testRemoteEndpointSKIA}
			} else {
				cfg.RemoteSKIAllowlist = test.allowlist
			}
			if test.subnets != nil {
				cfg.Subnets = test.subnets
			}
			setRemoteEndpointSpecs(t, &cfg, test.specs)
			probe := &eebusResolverProbe{err: fmt.Errorf("interface resolver must not run")}

			got, err := mapEEBusRuntimeConfig(cfg, eebusInterfaceAddressResolver(probe.resolve))
			if err == nil {
				t.Fatal("map unsafe remote endpoint error = nil; want fail-closed rejection")
			}
			if len(probe.calls) != 0 {
				t.Fatalf("interface resolver calls = %v; want endpoint rejection before resolver", probe.calls)
			}
			if want := (eebusruntime.Config{}); !reflect.DeepEqual(got, want) {
				t.Fatalf("runtime config on endpoint error = %+v; want zero value", got)
			}
			assertSanitizedRemoteEndpointError(t, err, test.specs[test.wantIndex], test.wantIndex)
		})
	}
}

func TestMapEEBusRuntimeConfig_DisabledRejectsRemoteEndpointAsActiveInput(t *testing.T) {
	cfg := ebusgateway.DefaultEEBusConfig()
	spec := testRemoteEndpointSKIA + "=" + testRemoteEndpointIPv4
	setRemoteEndpointSpecs(t, &cfg, []string{spec})
	probe := &eebusResolverProbe{addresses: []netip.Addr{netip.MustParseAddr("192.0.2.42")}}

	got, err := mapEEBusRuntimeConfig(cfg, eebusInterfaceAddressResolver(probe.resolve))
	if err == nil {
		t.Fatal("disabled eeBUS accepted remote endpoint input; want active-field rejection")
	}
	if strings.Contains(err.Error(), testRemoteEndpointIPv4) {
		t.Fatalf("disabled eeBUS error leaked raw endpoint: %v", err)
	}
	if len(probe.calls) != 0 {
		t.Fatalf("disabled endpoint resolver calls = %v; want none", probe.calls)
	}
	if want := (eebusruntime.Config{}); !reflect.DeepEqual(got, want) {
		t.Fatalf("disabled runtime config = %+v; want zero value", got)
	}
}

func TestMapEEBusRuntimeConfig_NoEndpointPreservesDiscoveryFallback(t *testing.T) {
	cfg := validEEBusRuntimeGatewayConfig()
	cfg.DiscoveryEnabled = true
	cfg.RemoteSKIAllowlist = []string{testRemoteEndpointSKIA}
	probe := &eebusResolverProbe{addresses: []netip.Addr{netip.MustParseAddr("192.0.2.42")}}

	got, err := mapEEBusRuntimeConfig(cfg, eebusInterfaceAddressResolver(probe.resolve))
	if err != nil {
		t.Fatalf("map discovery-only remote: %v", err)
	}
	if !got.DiscoveryEnabled || len(got.Remotes) != 1 {
		t.Fatalf("discovery fallback = enabled:%v remotes:%d; want true/1", got.DiscoveryEnabled, len(got.Remotes))
	}
	assertRemoteEndpointV1(t, got.Remotes[0], testRemoteEndpointSKIA, "", "")
}

func TestStartEEBusRuntime_MapsExactEndpointsWithoutPublicLeak(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.EEBusConfig = validEEBusRuntimeGatewayConfig()
	cfg.EEBusConfig.Subnets = []string{"192.0.2.0/24", "2001:db8:1::/64"}
	cfg.EEBusConfig.RemoteSKIAllowlist = []string{
		strings.ToUpper(testRemoteEndpointSKIB),
		testRemoteEndpointSKIA,
	}
	setRemoteEndpointSpecs(t, &cfg.EEBusConfig, []string{
		strings.ToUpper(testRemoteEndpointSKIB) + "=" + testRemoteEndpointIPv4,
		testRemoteEndpointSKIA + "=" + testRemoteEndpointIPv6,
	})
	transportBefore := cfg.TransportConfig
	busBefore := cfg.BusConfig

	oldWriter := log.Writer()
	oldFlags := log.Flags()
	var logs bytes.Buffer
	log.SetOutput(&logs)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})

	var captured eebusruntime.Config
	adapter, err := startEEBusRuntime(
		context.Background(),
		cfg.EEBusConfig,
		func(string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
		},
		func(runtimeCfg eebusruntime.Config) (eebusruntime.Runtime, error) {
			captured = runtimeCfg
			return &remoteEndpointRuntime{}, nil
		},
	)
	if err != nil {
		t.Fatalf("start valid endpoint sidecar: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Shutdown() })
	if len(captured.Remotes) != 2 {
		t.Fatalf("captured runtime Remotes length = %d; want 2", len(captured.Remotes))
	}
	assertRemoteEndpointV1(t, captured.Remotes[0], testRemoteEndpointSKIA, "[2001:db8:1::21]:12480", "/ship/")
	assertRemoteEndpointV1(t, captured.Remotes[1], testRemoteEndpointSKIB, "192.0.2.21:12480", "/ship/")

	snapshot, err := adapter.Snapshot()
	if err != nil {
		t.Fatalf("snapshot endpoint sidecar: %v", err)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal public runtime snapshot: %v", err)
	}
	for surface, content := range map[string]string{
		"logs":             logs.String(),
		"runtime snapshot": string(encoded),
	} {
		for _, secret := range []string{
			"192.0.2.21",
			"2001:db8:1::21",
			"12480",
			"/ship/",
		} {
			if strings.Contains(content, secret) {
				t.Fatalf("%s leaked remote endpoint component %q", surface, secret)
			}
		}
	}
	if !reflect.DeepEqual(cfg.TransportConfig, transportBefore) || !reflect.DeepEqual(cfg.BusConfig, busBefore) {
		t.Fatal("starting eeBUS endpoint sidecar mutated existing eBUS configuration")
	}
}

func TestEEBusRemoteEndpointMappingUsesNoDNSResolver(t *testing.T) {
	for _, path := range []string{"eebus_config_flags.go", "eebus_runtime_config.go"} {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, forbidden := range []string{"net.Resolve", "net.Lookup", "DefaultResolver", "LookupHost", "LookupIP"} {
			if bytes.Contains(content, []byte(forbidden)) {
				t.Fatalf("%s contains DNS-capable API %q; endpoint mapping must remain literal-only", path, forbidden)
			}
		}
	}
}

func remoteEndpointSpecs(t *testing.T, cfg *ebusgateway.EEBusConfig) []string {
	t.Helper()
	field := reflect.ValueOf(cfg).Elem().FieldByName("RemoteEndpoints")
	if !field.IsValid() || field.Type() != reflect.TypeOf([]string{}) {
		t.Fatal("missing gateway v1 contract: EEBusConfig.RemoteEndpoints []string")
	}
	return append([]string(nil), field.Interface().([]string)...)
}

func setRemoteEndpointSpecs(t *testing.T, cfg *ebusgateway.EEBusConfig, specs []string) {
	t.Helper()
	field := reflect.ValueOf(cfg).Elem().FieldByName("RemoteEndpoints")
	if !field.IsValid() || !field.CanSet() || field.Type() != reflect.TypeOf([]string{}) {
		t.Fatal("missing gateway v1 contract: EEBusConfig.RemoteEndpoints []string")
	}
	field.Set(reflect.ValueOf(append([]string(nil), specs...)))
}

func assertRemoteEndpointV1(t *testing.T, remote eebusruntime.Remote, wantSKI, wantEndpoint, wantPath string) {
	t.Helper()
	if remote.SKI != wantSKI {
		t.Fatalf("runtime remote SKI = %q; want %q", remote.SKI, wantSKI)
	}
	value := reflect.ValueOf(remote)
	endpointField := value.FieldByName("Endpoint")
	pathField := value.FieldByName("EndpointPath")
	if !endpointField.IsValid() || endpointField.Type() != reflect.TypeOf(netip.AddrPort{}) ||
		!pathField.IsValid() || pathField.Kind() != reflect.String {
		t.Fatal("missing public runtime v1 contract: Remote.Endpoint netip.AddrPort and Remote.EndpointPath string")
	}
	endpoint := endpointField.Interface().(netip.AddrPort)
	if endpoint.String() != wantEndpoint || pathField.String() != wantPath {
		t.Fatalf("runtime remote endpoint = %q path %q; want %q path %q", endpoint, pathField.String(), wantEndpoint, wantPath)
	}
}

func assertSanitizedRemoteEndpointError(t *testing.T, err error, spec string, wantIndex int) {
	t.Helper()
	message := err.Error()
	if !remoteEndpointIndexPattern.MatchString(message) || !remoteEndpointReasonPattern.MatchString(message) {
		t.Fatalf("remote endpoint error = %q; want index=%d and a stable reason code", message, wantIndex)
	}
	if !strings.Contains(message, fmt.Sprintf("index=%d", wantIndex)) {
		t.Fatalf("remote endpoint error = %q; want index=%d", message, wantIndex)
	}
	_, rawEndpoint, found := strings.Cut(spec, "=")
	if !found {
		rawEndpoint = spec
	}
	for _, secret := range remoteEndpointSecrets(rawEndpoint) {
		if strings.Contains(message, secret) {
			t.Fatalf("remote endpoint error leaked raw component %q: %q", secret, message)
		}
	}
}

func remoteEndpointSecrets(raw string) []string {
	secrets := []string{raw}
	if slash := strings.IndexByte(raw, '/'); slash >= 0 {
		secrets = append(secrets, raw[:slash], raw[slash:])
	}
	result := secrets[:0]
	for _, secret := range secrets {
		if len(secret) >= 4 {
			result = append(result, secret)
		}
	}
	return result
}
