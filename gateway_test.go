package ebusgateway

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

func TestNewGateway_InvalidAdmissionStabilityReturnsBeforeDial(t *testing.T) {
	if os.Getenv("HELIANTHUS_TEST_INVALID_ADMISSION_STABILITY") == "1" {
		cfg := Config{
			StateMinStabilitySeconds: 120,
			TransportConfig: TransportConfig{
				Protocol: TransportENH,
				Network:  "tcp",
				Address:  "192.0.2.1:9999",
				Dial: func(context.Context, string, string, time.Duration) (net.Conn, error) {
					panic("transport dialed before admission stability validation")
				},
			},
		}
		gateway, err := New(context.Background(), cfg)
		if gateway != nil {
			t.Fatalf("New() gateway = %v; want nil", gateway)
		}
		if !errors.Is(err, ErrStartupAdmissionStabilityInvariant) {
			t.Fatalf("New() error = %v; want ErrStartupAdmissionStabilityInvariant", err)
		}
		return
	}

	command := exec.Command(os.Args[0], "-test.run=^TestNewGateway_InvalidAdmissionStabilityReturnsBeforeDial$")
	command.Env = append(os.Environ(), "HELIANTHUS_TEST_INVALID_ADMISSION_STABILITY=1")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("invalid admission stability terminated the helper process: %v\n%s", err, output)
	}
}

func TestNewGateway_UsesProvidedTransport(t *testing.T) {
	loopback := transport.NewLoopback()
	cfg := Config{
		Transport: loopback,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gateway, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if gateway.Transport != loopback {
		t.Fatalf("gateway.Transport = %T; want loopback", gateway.Transport)
	}

	gateway.Start(ctx)
	cancel()

	if err := gateway.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewGateway_DialsWithENH(t *testing.T) {
	var (
		dialed      bool
		gotNetwork  string
		gotAddress  string
		serverClose func()
	)

	dialer := func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
		dialed = true
		gotNetwork = network
		gotAddress = address
		client, server := net.Pipe()
		serverClose = func() { _ = server.Close() }

		go func() {
			buf := make([]byte, 2)
			if _, err := io.ReadFull(server, buf); err != nil {
				_ = server.Close()
				return
			}
			resp := transport.EncodeENH(transport.ENHResResetted, 0x00)
			_, _ = server.Write(resp[:])
		}()

		return client, nil
	}

	cfg := Config{
		TransportConfig: TransportConfig{
			Protocol:    TransportENH,
			Network:     "unix",
			Address:     "/tmp/ebusd.sock",
			DialTimeout: time.Second,
			Dial:        dialer,
			ReadTimeout: 200 * time.Millisecond,
		},
	}

	gateway, err := New(context.Background(), cfg)
	if serverClose != nil {
		defer serverClose()
	}
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if !dialed {
		t.Fatalf("dialer was not invoked")
	}
	if gotNetwork != "unix" || gotAddress != "/tmp/ebusd.sock" {
		t.Fatalf("dialer args = %s %s; want unix /tmp/ebusd.sock", gotNetwork, gotAddress)
	}
	if _, ok := gateway.Transport.(*transport.ENHTransport); !ok {
		t.Fatalf("gateway.Transport = %T; want *transport.ENHTransport", gateway.Transport)
	}

	if err := gateway.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewGateway_DialsWithEbusdTCP(t *testing.T) {
	var (
		dialed      bool
		gotNetwork  string
		gotAddress  string
		serverClose func()
	)

	dialer := func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
		dialed = true
		gotNetwork = network
		gotAddress = address
		client, server := net.Pipe()
		serverClose = func() { _ = server.Close() }
		return client, nil
	}

	cfg := Config{
		TransportConfig: TransportConfig{
			Protocol:    TransportEbusdTCP,
			Network:     "tcp",
			Address:     "127.0.0.1:8888",
			DialTimeout: time.Second,
			Dial:        dialer,
		},
	}

	gateway, err := New(context.Background(), cfg)
	if serverClose != nil {
		defer serverClose()
	}
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if !dialed {
		t.Fatalf("dialer was not invoked")
	}
	if gotNetwork != "tcp" || gotAddress != "127.0.0.1:8888" {
		t.Fatalf("dialer args = %s %s; want tcp 127.0.0.1:8888", gotNetwork, gotAddress)
	}
	if _, ok := gateway.Transport.(*transport.EbusdTCPTransport); !ok {
		t.Fatalf("gateway.Transport = %T; want *transport.EbusdTCPTransport", gateway.Transport)
	}

	if err := gateway.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewGateway_DialsWithENHEndpointProfile(t *testing.T) {
	var (
		dialed      bool
		gotNetwork  string
		gotAddress  string
		serverClose func()
	)

	dialer := func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
		dialed = true
		gotNetwork = network
		gotAddress = address
		client, server := net.Pipe()
		serverClose = func() { _ = server.Close() }

		go func() {
			buf := make([]byte, 2)
			if _, err := io.ReadFull(server, buf); err != nil {
				_ = server.Close()
				return
			}
			resp := transport.EncodeENH(transport.ENHResResetted, 0x00)
			_, _ = server.Write(resp[:])
		}()

		return client, nil
	}

	cfg := Config{
		TransportConfig: TransportConfig{
			Address:     "enh://127.0.0.1:9999",
			DialTimeout: time.Second,
			Dial:        dialer,
			ReadTimeout: 200 * time.Millisecond,
		},
	}

	gateway, err := New(context.Background(), cfg)
	if serverClose != nil {
		defer serverClose()
	}
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if !dialed {
		t.Fatalf("dialer was not invoked")
	}
	if gotNetwork != "tcp" || gotAddress != "127.0.0.1:9999" {
		t.Fatalf("dialer args = %s %s; want tcp 127.0.0.1:9999", gotNetwork, gotAddress)
	}
	if _, ok := gateway.Transport.(*transport.ENHTransport); !ok {
		t.Fatalf("gateway.Transport = %T; want *transport.ENHTransport", gateway.Transport)
	}

	if err := gateway.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewGateway_DialsWithENSEndpointProfile(t *testing.T) {
	var (
		dialed      bool
		gotNetwork  string
		gotAddress  string
		serverClose func()
	)

	dialer := func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
		dialed = true
		gotNetwork = network
		gotAddress = address
		client, server := net.Pipe()
		serverClose = func() { _ = server.Close() }

		go func() {
			buf := make([]byte, 2)
			if _, err := io.ReadFull(server, buf); err != nil {
				_ = server.Close()
				return
			}
			resp := transport.EncodeENH(transport.ENHResResetted, 0x00)
			_, _ = server.Write(resp[:])
		}()
		return client, nil
	}

	cfg := Config{
		TransportConfig: TransportConfig{
			Address:     "ens://127.0.0.1:10000",
			DialTimeout: time.Second,
			Dial:        dialer,
			ReadTimeout: 200 * time.Millisecond,
		},
	}

	gateway, err := New(context.Background(), cfg)
	if serverClose != nil {
		defer serverClose()
	}
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if !dialed {
		t.Fatalf("dialer was not invoked")
	}
	if gotNetwork != "tcp" || gotAddress != "127.0.0.1:10000" {
		t.Fatalf("dialer args = %s %s; want tcp 127.0.0.1:10000", gotNetwork, gotAddress)
	}
	if _, ok := gateway.Transport.(*transport.ENHTransport); !ok {
		t.Fatalf("gateway.Transport = %T; want *transport.ENHTransport", gateway.Transport)
	}

	if err := gateway.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNormalizeTransportConfigRejectsUnsupportedEndpointScheme(t *testing.T) {
	_, err := normalizeTransportConfig(TransportConfig{
		Address: "unsupported://127.0.0.1:9999",
	})
	if err == nil {
		t.Fatalf("expected error for unsupported endpoint scheme")
	}
}

func TestNormalizeTransportConfigRejectsNetworkURIWithoutHostPort(t *testing.T) {
	for _, endpoint := range []string{
		"enh://adapter.invalid",
		"ens://adapter.invalid",
		"tcp://adapter.invalid",
		"tcp-plain://adapter.invalid",
		"udp-plain://adapter.invalid",
		"ebusd://adapter.invalid",
		"ebusd-tcp://adapter.invalid",
		"adapter-direct://adapter.invalid",
		"adapter-direct-ens://adapter.invalid",
		"tcp-plain://:9999",
		"tcp-plain://adapter.invalid:",
		"tcp-plain://adapter.invalid:notaport",
	} {
		t.Run(endpoint, func(t *testing.T) {
			_, err := normalizeTransportConfig(TransportConfig{Address: endpoint})
			if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
				t.Fatalf("normalizeTransportConfig(%q) error = %v, want ErrInvalidPayload", endpoint, err)
			}
		})
	}
}

func TestNormalizeTransportConfigENHUnixEndpoint(t *testing.T) {
	cfg, err := normalizeTransportConfig(TransportConfig{
		Address: "enh:///var/run/ebusd/ebusd.socket",
	})
	if err != nil {
		t.Fatalf("normalizeTransportConfig error = %v", err)
	}
	if cfg.Protocol != TransportENH {
		t.Fatalf("protocol = %q; want %q", cfg.Protocol, TransportENH)
	}
	if cfg.Network != "unix" {
		t.Fatalf("network = %q; want unix", cfg.Network)
	}
	if cfg.Address != "/var/run/ebusd/ebusd.socket" {
		t.Fatalf("address = %q; want /var/run/ebusd/ebusd.socket", cfg.Address)
	}
}

func TestNormalizeTransportConfigUDPPlainEndpoint(t *testing.T) {
	cfg, err := normalizeTransportConfig(TransportConfig{
		Address: "udp-plain://127.0.0.1:9999",
	})
	if err != nil {
		t.Fatalf("normalizeTransportConfig error = %v", err)
	}
	if cfg.Protocol != TransportUDPPlain {
		t.Fatalf("protocol = %q; want %q", cfg.Protocol, TransportUDPPlain)
	}
	if cfg.Network != "udp" {
		t.Fatalf("network = %q; want udp", cfg.Network)
	}
	if cfg.Address != "127.0.0.1:9999" {
		t.Fatalf("address = %q; want 127.0.0.1:9999", cfg.Address)
	}
}

func TestNormalizeTransportConfigTCPPlainEndpoint(t *testing.T) {
	cfg, err := normalizeTransportConfig(TransportConfig{
		Address: "tcp-plain://127.0.0.1:9999",
	})
	if err != nil {
		t.Fatalf("normalizeTransportConfig error = %v", err)
	}
	if cfg.Protocol != TransportTCPPlain {
		t.Fatalf("protocol = %q; want %q", cfg.Protocol, TransportTCPPlain)
	}
	if cfg.Network != "tcp" {
		t.Fatalf("network = %q; want tcp", cfg.Network)
	}
	if cfg.Address != "127.0.0.1:9999" {
		t.Fatalf("address = %q; want 127.0.0.1:9999", cfg.Address)
	}
}

func TestNormalizeTransportConfigAdapterDirectEndpoints(t *testing.T) {
	tests := []struct {
		name         string
		endpoint     string
		wantProtocol TransportProtocol
	}{
		{name: "enh family", endpoint: "adapter-direct://adapter.local:9999", wantProtocol: TransportAdapterDirect},
		{name: "ens family", endpoint: "adapter-direct-ens://adapter.local:9999", wantProtocol: transportAdapterDirectENS},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := normalizeTransportConfig(TransportConfig{
				Protocol: TransportENH,
				Network:  "unix",
				Address:  test.endpoint,
			})
			if err != nil {
				t.Fatalf("normalizeTransportConfig() error = %v", err)
			}
			if cfg.Protocol != test.wantProtocol || cfg.Network != "tcp" || cfg.Address != "adapter.local:9999" {
				t.Fatalf("normalized tuple = %q/%q/%q, want %q/tcp/adapter.local:9999", cfg.Protocol, cfg.Network, cfg.Address, test.wantProtocol)
			}
			if got := EBusDriverTransportProtocol(TransportConfig{Protocol: TransportENH, Network: "unix", Address: test.endpoint}); got != TransportAdapterDirect {
				t.Fatalf("EBusDriverTransportProtocol() = %q, want stable adapter-direct shape", got)
			}
			if path, special := ResolveAdmissionPath(cfg.Protocol); path != TransportAdmissionSourceSelectionCapable || !special {
				t.Fatalf("ResolveAdmissionPath(%q) = %q/%v, want source-selection-capable/true", cfg.Protocol, path, special)
			}
		})
	}
}

func TestNormalizeTransportConfigCanonicalizesEbusdAlias(t *testing.T) {
	cfg, err := normalizeTransportConfig(TransportConfig{
		Protocol: TransportProtocol("ebusd"),
		Network:  "tcp",
		Address:  "127.0.0.1:9999",
	})
	if err != nil {
		t.Fatalf("normalizeTransportConfig error = %v", err)
	}
	if cfg.Protocol != TransportEbusdTCP {
		t.Fatalf("protocol = %q; want %q", cfg.Protocol, TransportEbusdTCP)
	}
}

func TestPassiveTransportSupported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{
			name: "protocol field ebusd-tcp",
			cfg: Config{
				TransportConfig: TransportConfig{
					Protocol: TransportEbusdTCP,
					Network:  "tcp",
					Address:  "127.0.0.1:8888",
				},
			},
			want: false,
		},
		{
			name: "endpoint scheme ebusd",
			cfg: Config{
				TransportConfig: TransportConfig{
					Address: "ebusd://127.0.0.1:8888",
				},
			},
			want: false,
		},
		{
			name: "enh transport",
			cfg: Config{
				TransportConfig: TransportConfig{
					Protocol: TransportENH,
					Network:  "tcp",
					Address:  "127.0.0.1:9999",
				},
			},
			want: true,
		},
		{
			name: "direct enh remote adapter unsupported",
			cfg: Config{
				TransportConfig: TransportConfig{
					Protocol: TransportENH,
					Network:  "tcp",
					Address:  "192.168.100.2:9999",
				},
			},
			want: false,
		},
		{
			name: "direct ens remote adapter unsupported",
			cfg: Config{
				TransportConfig: TransportConfig{
					Protocol: TransportENS,
					Network:  "tcp",
					Address:  "192.168.100.2:9999",
				},
			},
			want: false,
		},
		{
			name: "direct enh remote adapter hostname unsupported",
			cfg: Config{
				TransportConfig: TransportConfig{
					Protocol: TransportENH,
					Network:  "tcp",
					Address:  "adapter.local:9999",
				},
			},
			want: false,
		},
		{
			name: "remote ens proxy-like endpoint supported",
			cfg: Config{
				TransportConfig: TransportConfig{
					Protocol: TransportENS,
					Network:  "tcp",
					Address:  "192.168.100.4:19001",
				},
			},
			want: true,
		},
		{
			name: "remote ens proxy-like hostname endpoint supported",
			cfg: Config{
				TransportConfig: TransportConfig{
					Protocol: TransportENS,
					Network:  "tcp",
					Address:  "proxy.local:19001",
				},
			},
			want: true,
		},
		{
			name: "local ens proxy endpoint supported",
			cfg: Config{
				TransportConfig: TransportConfig{
					Protocol: TransportENS,
					Network:  "tcp",
					Address:  "127.0.0.1:19001",
				},
			},
			want: true,
		},
		{
			name: "loopback endpoint uri supported",
			cfg: Config{
				TransportConfig: TransportConfig{
					Address: "ens://127.0.0.1:19001",
				},
			},
			want: true,
		},
		{
			name: "remote endpoint uri unsupported",
			cfg: Config{
				TransportConfig: TransportConfig{
					Address: "enh://192.168.100.2:9999",
				},
			},
			want: false,
		},
		{
			name: "remote endpoint hostname uri unsupported",
			cfg: Config{
				TransportConfig: TransportConfig{
					Address: "ens://adapter.local:9999",
				},
			},
			want: false,
		},
		{
			name: "remote proxy-like endpoint uri supported",
			cfg: Config{
				TransportConfig: TransportConfig{
					Address: "ens://192.168.100.4:19001",
				},
			},
			want: true,
		},
		{
			name: "remote proxy-like hostname endpoint uri supported",
			cfg: Config{
				TransportConfig: TransportConfig{
					Address: "ens://proxy.local:19001",
				},
			},
			want: true,
		},
		{
			name: "unix endpoint unchanged",
			cfg: Config{
				TransportConfig: TransportConfig{
					Protocol: TransportENH,
					Network:  "unix",
					Address:  "/var/run/adapter.sock",
				},
			},
			want: true,
		},
		{
			name: "pre-wired passive transport overrides heuristic",
			cfg: Config{
				PassiveTransport: transport.NewLoopback(),
				TransportConfig: TransportConfig{
					Protocol: TransportENH,
					Network:  "tcp",
					Address:  "192.168.100.2:9999",
				},
			},
			want: true,
		},
		{
			name: "pre-wired passive transport overrides ebusd-tcp",
			cfg: Config{
				PassiveTransport: transport.NewLoopback(),
				TransportConfig: TransportConfig{
					Protocol: TransportEbusdTCP,
					Network:  "tcp",
					Address:  "127.0.0.1:8888",
				},
			},
			want: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := PassiveTransportSupported(test.cfg); got != test.want {
				t.Fatalf("PassiveTransportSupported() = %v; want %v", got, test.want)
			}
		})
	}
}

func TestTransportFromConn_UDPPlain(t *testing.T) {
	t.Run("requires udp conn", func(t *testing.T) {
		client, server := net.Pipe()
		defer func() { _ = client.Close() }()
		defer func() { _ = server.Close() }()

		_, err := transportFromConn(TransportUDPPlain, client, 200*time.Millisecond, 200*time.Millisecond)
		if err == nil {
			t.Fatalf("expected error for non-udp conn")
		}
	})

	t.Run("returns UDPPlainTransport", func(t *testing.T) {
		server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
		if err != nil {
			t.Fatalf("ListenUDP error = %v", err)
		}
		t.Cleanup(func() { _ = server.Close() })

		clientConn, err := net.DialUDP("udp", nil, server.LocalAddr().(*net.UDPAddr))
		if err != nil {
			t.Fatalf("DialUDP error = %v", err)
		}
		t.Cleanup(func() { _ = clientConn.Close() })

		raw, err := transportFromConn(TransportUDPPlain, clientConn, 200*time.Millisecond, 200*time.Millisecond)
		if err != nil {
			t.Fatalf("transportFromConn error = %v", err)
		}
		if _, ok := raw.(*transport.UDPPlainTransport); !ok {
			t.Fatalf("transport = %T; want *transport.UDPPlainTransport", raw)
		}
		_ = raw.Close()
	})
}

func TestTransportFromConn_TCPPlain(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	raw, err := transportFromConn(TransportTCPPlain, client, 200*time.Millisecond, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("transportFromConn error = %v", err)
	}
	if _, ok := raw.(*transport.TCPPlainTransport); !ok {
		t.Fatalf("transport = %T; want *transport.TCPPlainTransport", raw)
	}
	_ = raw.Close()
}

func TestClampEbusdTCPTimeouts_UsesMinimumForShortOrUnset(t *testing.T) {
	t.Parallel()

	cfg := TransportConfig{
		Protocol:     TransportEbusdTCP,
		ReadTimeout:  0,
		WriteTimeout: 150 * time.Millisecond,
	}

	got := clampEbusdTCPTimeouts(cfg, 2*time.Second)
	if got.ReadTimeout != 2*time.Second {
		t.Fatalf("ReadTimeout = %s; want 2s", got.ReadTimeout)
	}
	if got.WriteTimeout != 2*time.Second {
		t.Fatalf("WriteTimeout = %s; want 2s", got.WriteTimeout)
	}
}

func TestClampEbusdTCPTimeouts_LeavesLargerTimeoutsUntouched(t *testing.T) {
	t.Parallel()

	cfg := TransportConfig{
		Protocol:     TransportEbusdTCP,
		ReadTimeout:  4 * time.Second,
		WriteTimeout: 3 * time.Second,
	}

	got := clampEbusdTCPTimeouts(cfg, 2*time.Second)
	if got.ReadTimeout != 4*time.Second {
		t.Fatalf("ReadTimeout = %s; want 4s", got.ReadTimeout)
	}
	if got.WriteTimeout != 3*time.Second {
		t.Fatalf("WriteTimeout = %s; want 3s", got.WriteTimeout)
	}
}

func TestClampEbusdTCPTimeouts_NonEbusdUsesScanRequestTimeout(t *testing.T) {
	t.Parallel()

	cfg := TransportConfig{
		Protocol:     TransportENH,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 120 * time.Millisecond,
	}
	got := clampEbusdTCPTimeouts(cfg, 2*time.Second)
	if got.ReadTimeout != 2*time.Second {
		t.Fatalf("ReadTimeout = %s; want 2s", got.ReadTimeout)
	}
	if got.WriteTimeout != 2*time.Second {
		t.Fatalf("WriteTimeout = %s; want 2s", got.WriteTimeout)
	}
}

func TestClampEbusdTCPTimeouts_NonEbusdWithoutScanTimeoutUnchanged(t *testing.T) {
	t.Parallel()

	cfg := TransportConfig{
		Protocol:     TransportENS,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 120 * time.Millisecond,
	}
	got := clampEbusdTCPTimeouts(cfg, 0)
	if got.ReadTimeout != cfg.ReadTimeout || got.WriteTimeout != cfg.WriteTimeout {
		t.Fatalf("timeouts changed with zero scan timeout: got read=%s write=%s", got.ReadTimeout, got.WriteTimeout)
	}
}

type mockInitTransport struct {
	called   bool
	features []byte
}

func (m *mockInitTransport) Init(features byte) (byte, error) {
	m.called = true
	m.features = append(m.features, features)
	return features, nil
}

func (m *mockInitTransport) ReadByte() (byte, error) { return 0, nil }
func (m *mockInitTransport) Write([]byte) (int, error) {
	return 0, nil
}
func (m *mockInitTransport) Close() error { return nil }

type mockRawTransport struct{}

func (mockRawTransport) ReadByte() (byte, error) { return 0, nil }
func (mockRawTransport) Write([]byte) (int, error) {
	return 0, nil
}
func (mockRawTransport) Close() error { return nil }

func TestInitTransportIfSupported(t *testing.T) {
	t.Run("calls Init when supported", func(t *testing.T) {
		tr := &mockInitTransport{}
		if err := initTransportIfSupported(tr); err != nil {
			t.Fatalf("initTransportIfSupported error = %v", err)
		}
		if !tr.called {
			t.Fatalf("Init was not called")
		}
		if len(tr.features) != 1 || tr.features[0] != 0x01 {
			t.Fatalf("Init features = %v; want [1]", tr.features)
		}
	})

	t.Run("no-op when unsupported", func(t *testing.T) {
		tr := mockRawTransport{}
		if err := initTransportIfSupported(tr); err != nil {
			t.Fatalf("initTransportIfSupported error = %v", err)
		}
	})
}

func TestGateway_RefreshRouterPlanes(t *testing.T) {
	plane := &mockPlane{
		name: "test",
		subscriptions: []router.Subscription{
			{Primary: 0xA1, Secondary: 0xB2},
		},
	}
	provider := mockProvider{plane: plane}

	gateway, err := New(context.Background(), Config{
		Transport: transport.NewLoopback(),
		Providers: []registry.PlaneProvider{provider},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	gateway.Registry.Register(registry.DeviceInfo{
		Address:      0x10,
		Manufacturer: "test",
	})

	count := gateway.RefreshRouterPlanes()
	if count != 1 {
		t.Fatalf("RefreshRouterPlanes() = %d; want 1", count)
	}

	_ = gateway.Router.HandleBroadcast(protocol.Frame{
		Primary:   0xA1,
		Secondary: 0xB2,
	})

	if got := plane.BroadcastCount(); got != 1 {
		t.Fatalf("OnBroadcast calls = %d; want 1", got)
	}
}

func TestGateway_AddRouterPlane(t *testing.T) {
	plane := &mockPlane{
		name: "extra",
		subscriptions: []router.Subscription{
			{Primary: 0xA1, Secondary: 0xB2},
		},
	}

	gateway, err := New(context.Background(), Config{
		Transport: transport.NewLoopback(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() {
		if err := gateway.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	gateway.AddRouterPlane(plane)
	count := gateway.RefreshRouterPlanes()
	if count != 1 {
		t.Fatalf("RefreshRouterPlanes() = %d; want 1", count)
	}

	_ = gateway.Router.HandleBroadcast(protocol.Frame{
		Primary:   0xA1,
		Secondary: 0xB2,
	})

	if got := plane.BroadcastCount(); got != 1 {
		t.Fatalf("OnBroadcast calls = %d; want 1", got)
	}
}

type mockProvider struct {
	plane *mockPlane
}

func (mockProvider) Name() string {
	return "mock"
}

func (mockProvider) Match(registry.DeviceInfo) bool {
	return true
}

func (provider mockProvider) CreatePlanes(registry.DeviceInfo) []registry.Plane {
	return []registry.Plane{provider.plane}
}

type mockPlane struct {
	name          string
	subscriptions []router.Subscription
	mu            sync.Mutex
	broadcasts    int
}

func (plane *mockPlane) Name() string {
	return plane.name
}

func (plane *mockPlane) Methods() []registry.Method {
	return nil
}

func (plane *mockPlane) Subscriptions() []router.Subscription {
	return plane.subscriptions
}

func (plane *mockPlane) OnBroadcast(frame protocol.Frame) error {
	plane.mu.Lock()
	defer plane.mu.Unlock()
	plane.broadcasts++
	return nil
}

func (plane *mockPlane) BroadcastCount() int {
	plane.mu.Lock()
	defer plane.mu.Unlock()
	return plane.broadcasts
}

func (plane *mockPlane) BuildRequest(registry.Method, map[string]any) (protocol.Frame, error) {
	return protocol.Frame{}, nil
}

func (plane *mockPlane) DecodeResponse(registry.Method, protocol.Frame, map[string]any) (any, error) {
	return nil, nil
}
