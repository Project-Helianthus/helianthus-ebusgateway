//go:build linux || darwin

package ebusgateway

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/ptytest"
)

// Kills: a classification by path prefix alone, which would leave a character
// device outside /dev/ in the unix branch, and one that takes a socket for it.
func TestNormalizeTransportConfig_ClassifiesByFileTypeOutsideDev(t *testing.T) {
	dir, err := os.MkdirTemp("", "ens")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	pair := ptytest.Open(t)
	link := filepath.Join(dir, "adapter")
	if err := os.Symlink(pair.DevicePath, link); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer func() { _ = listener.Close() }()

	got, err := normalizeTransportConfig(TransportConfig{Address: "ens://" + link})
	if err != nil {
		t.Fatal(err)
	}
	if got.Network != TransportNetworkSerial || got.Protocol != transportAdapterDirectENS || got.Address != link {
		t.Fatalf("character device resolved to %q %q %q", got.Protocol, got.Network, got.Address)
	}
	got, err = normalizeTransportConfig(TransportConfig{Address: "ens://" + socket})
	if err != nil {
		t.Fatal(err)
	}
	if got.Network != "unix" || got.Protocol != TransportENH || got.Address != socket {
		t.Fatalf("socket resolved to %q %q %q", got.Protocol, got.Network, got.Address)
	}
}
