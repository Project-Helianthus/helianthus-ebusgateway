package ebusgateway

import (
	"errors"
	"testing"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
)

const testSerialDevice = "/dev/ttyUSB0"

// Kills: a serial path left in the unix branch, the speed selector lost for the
// ens forms, and the path-only form skipped by the "://" shortcut.
func TestNormalizeTransportConfig_SerialForms(t *testing.T) {
	const byID = "/dev/serial/by-id/usb-EXAMPLE_DEVICE_ID-if00-port0"
	const byPath = "/dev/serial/by-path/platform-example:1.2:1.0-port0"
	for _, test := range []struct {
		name         string
		config       TransportConfig
		wantProtocol TransportProtocol
		wantAddress  string
	}{
		{"ens path-only", TransportConfig{Address: "ens:" + testSerialDevice}, transportAdapterDirectENS, testSerialDevice},
		{"enh path-only", TransportConfig{Address: "enh:" + testSerialDevice}, TransportAdapterDirect, testSerialDevice},
		{"ens triple slash", TransportConfig{Address: "ens://" + testSerialDevice}, transportAdapterDirectENS, testSerialDevice},
		{"enh triple slash", TransportConfig{Address: "enh://" + testSerialDevice}, TransportAdapterDirect, testSerialDevice},
		{"adapter-direct path-only", TransportConfig{Address: "adapter-direct:" + testSerialDevice}, TransportAdapterDirect, testSerialDevice},
		{"adapter-direct triple slash", TransportConfig{Address: "adapter-direct://" + testSerialDevice}, TransportAdapterDirect, testSerialDevice},
		{"adapter-direct-ens path-only", TransportConfig{Address: "adapter-direct-ens:" + testSerialDevice}, transportAdapterDirectENS, testSerialDevice},
		{"adapter-direct-ens triple slash", TransportConfig{Address: "adapter-direct-ens://" + testSerialDevice}, transportAdapterDirectENS, testSerialDevice},
		{"upper case scheme", TransportConfig{Address: "ENS:" + testSerialDevice}, transportAdapterDirectENS, testSerialDevice},
		{"by-id", TransportConfig{Address: "ens:" + byID}, transportAdapterDirectENS, byID},
		{"by-path with colons", TransportConfig{Address: "ens:" + byPath}, transportAdapterDirectENS, byPath},
		{"by-path URI with colons", TransportConfig{Address: "adapter-direct-ens://" + byPath}, transportAdapterDirectENS, byPath},
		{"scheme wins over configured protocol", TransportConfig{Protocol: TransportENH, Network: "tcp", Address: "ens:" + testSerialDevice}, transportAdapterDirectENS, testSerialDevice},
		{"bare path adapter-direct", TransportConfig{Protocol: TransportAdapterDirect, Network: "unix", Address: testSerialDevice}, TransportAdapterDirect, testSerialDevice},
		{"bare path with colons", TransportConfig{Protocol: TransportAdapterDirect, Network: "unix", Address: byPath}, TransportAdapterDirect, byPath},
		{"bare path enh default network", TransportConfig{Protocol: TransportENH, Network: "unix", Address: testSerialDevice}, TransportAdapterDirect, testSerialDevice},
		{"bare path ens", TransportConfig{Protocol: TransportENS, Address: testSerialDevice}, transportAdapterDirectENS, testSerialDevice},
		{"explicit serial network", TransportConfig{Protocol: transportAdapterDirectENS, Network: "serial", Address: "/tmp/not-yet-plugged"}, transportAdapterDirectENS, "/tmp/not-yet-plugged"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := normalizeTransportConfig(test.config)
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if got.Protocol != test.wantProtocol || got.Network != TransportNetworkSerial || got.Address != test.wantAddress {
				t.Fatalf("got %q %q %q, want %q serial %q", got.Protocol, got.Network, got.Address, test.wantProtocol, test.wantAddress)
			}
			if EBusDriverTransportProtocol(test.config) != TransportAdapterDirect {
				t.Fatalf("driver protocol = %q, want adapter-direct", EBusDriverTransportProtocol(test.config))
			}
			// The process shell stores the normalized tuple and every later
			// consumer normalizes it again.
			again, err := normalizeTransportConfig(got)
			if err != nil {
				t.Fatalf("second normalize: %v", err)
			}
			if again.Protocol != got.Protocol || again.Network != got.Network || again.Address != got.Address {
				t.Fatalf("second normalize changed %+v to %+v", got, again)
			}
		})
	}
}

// Kills: the serial classification widened past the enhanced adapter schemes or
// past paths that can be a device; network endpoints keep their tuples.
func TestNormalizeTransportConfig_NonSerialFormsUntouched(t *testing.T) {
	for _, test := range []struct {
		name         string
		config       TransportConfig
		wantProtocol TransportProtocol
		wantNetwork  string
		wantAddress  string
	}{
		{"enh tcp", TransportConfig{Address: "enh://127.0.0.1:19001"}, TransportENH, "tcp", "127.0.0.1:19001"},
		{"ens tcp stays equivalent to enh", TransportConfig{Address: "ens://127.0.0.1:19002"}, TransportENH, "tcp", "127.0.0.1:19002"},
		{"adapter-direct tcp", TransportConfig{Address: "adapter-direct://adapter.local:9999"}, TransportAdapterDirect, "tcp", "adapter.local:9999"},
		{"adapter-direct-ens tcp", TransportConfig{Address: "adapter-direct-ens://adapter.local:9999"}, transportAdapterDirectENS, "tcp", "adapter.local:9999"},
		{"udp-plain", TransportConfig{Address: "udp-plain://203.0.113.10:9999"}, TransportUDPPlain, "udp", "203.0.113.10:9999"},
		{"tcp-plain", TransportConfig{Address: "tcp-plain://203.0.113.10:9999"}, TransportTCPPlain, "tcp", "203.0.113.10:9999"},
		{"enh unix socket uri", TransportConfig{Address: "enh:///var/run/ebusd/ebusd.socket"}, TransportENH, "unix", "/var/run/ebusd/ebusd.socket"},
		{"ens unix socket uri", TransportConfig{Address: "ens:///var/run/ebusd/ebusd.socket"}, TransportENH, "unix", "/var/run/ebusd/ebusd.socket"},
		{"adapter-direct unix socket uri", TransportConfig{Address: "adapter-direct:///var/run/mux.sock"}, TransportAdapterDirect, "unix", "/var/run/mux.sock"},
		{"explicit unix scheme under dev", TransportConfig{Protocol: TransportENH, Address: "unix://" + testSerialDevice}, TransportENH, "unix", testSerialDevice},
		{"ebusd-tcp keeps its unix path", TransportConfig{Address: "ebusd-tcp://" + testSerialDevice}, TransportEbusdTCP, "unix", testSerialDevice},
		{"traversal out of dev", TransportConfig{Address: "enh:///dev/../var/run/ebusd.socket"}, TransportENH, "unix", "/dev/../var/run/ebusd.socket"},
		{"default socket", TransportConfig{Protocol: TransportENH, Network: "unix", Address: "/var/run/ebusd/ebusd.socket"}, TransportENH, "unix", "/var/run/ebusd/ebusd.socket"},
		{"host named like a scheme", TransportConfig{Protocol: TransportAdapterDirect, Address: "enh:9999"}, TransportAdapterDirect, "tcp", "enh:9999"},
		{"bare path under dev with explicit tcp", TransportConfig{Protocol: TransportENH, Network: "tcp", Address: testSerialDevice}, TransportENH, "tcp", testSerialDevice},
		{"plain protocol keeps a bare dev path", TransportConfig{Protocol: TransportEbusdTCP, Network: "unix", Address: testSerialDevice}, TransportEbusdTCP, "unix", testSerialDevice},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := normalizeTransportConfig(test.config)
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if got.Protocol != test.wantProtocol || got.Network != test.wantNetwork || got.Address != test.wantAddress {
				t.Fatalf("got %q %q %q, want %q %q %q", got.Protocol, got.Network, got.Address, test.wantProtocol, test.wantNetwork, test.wantAddress)
			}
		})
	}
}

func TestNormalizeTransportConfig_InvalidSerialConfigFailsClosed(t *testing.T) {
	for name, config := range map[string]TransportConfig{
		"serial network with plain protocol": {Protocol: TransportTCPPlain, Network: "serial", Address: testSerialDevice},
		"serial network with relative path":  {Protocol: TransportENS, Network: "serial", Address: "dev/ttyUSB0"},
		"serial uri with query":              {Address: "ens:" + testSerialDevice + "?baud=2400"},
		"serial uri with fragment":           {Address: "enh://" + testSerialDevice + "#x"},
		"relative path uri":                  {Address: "ens://dev/ttyUSB0"},
		"scheme nested in a uri":             {Address: "adapter-direct://ens:" + testSerialDevice},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := normalizeTransportConfig(config)
			if err == nil {
				t.Fatalf("normalize accepted %+v as %+v", config, got)
			}
			if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
				t.Fatalf("err = %v, want ErrInvalidPayload", err)
			}
		})
	}
}

func TestPassiveTransportAvailableForSerialAdapterDirect(t *testing.T) {
	cfg := Config{TransportConfig: TransportConfig{Address: "ens:" + testSerialDevice}}
	if reason := passiveTransportUnavailableReason(cfg); reason != "" {
		t.Fatalf("passive unavailable: %q", reason)
	}
}
