package ebusgateway

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusgo/transport"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/router"
)

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

type mockInitTransport struct {
	called   bool
	features []byte
}

func (m *mockInitTransport) Init(features byte) error {
	m.called = true
	m.features = append(m.features, features)
	return nil
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
		if len(tr.features) != 1 || tr.features[0] != 0x00 {
			t.Fatalf("Init features = %v; want [0]", tr.features)
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

	if plane.broadcasts != 1 {
		t.Fatalf("OnBroadcast calls = %d; want 1", plane.broadcasts)
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

	if plane.broadcasts != 1 {
		t.Fatalf("OnBroadcast calls = %d; want 1", plane.broadcasts)
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
	plane.broadcasts++
	return nil
}

func (plane *mockPlane) BuildRequest(registry.Method, map[string]any) (protocol.Frame, error) {
	return protocol.Frame{}, nil
}

func (plane *mockPlane) DecodeResponse(registry.Method, protocol.Frame, map[string]any) (any, error) {
	return nil, nil
}
