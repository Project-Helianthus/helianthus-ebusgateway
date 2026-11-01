//go:build linux || darwin

package adaptermux

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/ptytest"
	"golang.org/x/sys/unix"
)

// serialLogLines timestamps each mux log line for ordering assertions.
type serialLogLines chan serialLogLine

type serialLogLine struct {
	at   time.Time
	text string
}

func (l serialLogLines) Write(p []byte) (int, error) {
	select {
	case l <- serialLogLine{at: time.Now(), text: string(p)}:
	default:
	}
	return len(p), nil
}

func (l serialLogLines) wait(t *testing.T, want string) serialLogLine {
	t.Helper()
	timeout := time.After(serialTestGuard)
	for {
		select {
		case line := <-l:
			if strings.Contains(line.text, want) {
				return line
			}
		case <-timeout:
			t.Fatalf("no log line containing %q", want)
		}
	}
}

func newSerialTestMux(protocol, path string, logs serialLogLines) *Mux {
	cfg := Config{
		Protocol:              protocol,
		Network:               NetworkSerial,
		Address:               path,
		ReadTimeout:           20 * time.Millisecond,
		WriteTimeout:          time.Second,
		ReconnectInitialDelay: 40 * time.Millisecond,
		ReconnectMaxDelay:     80 * time.Millisecond,
	}
	if logs != nil {
		cfg.Logger = log.New(logs, "", 0)
	}
	return New(cfg)
}

// Kills: the serial path dialed as a Unix socket, which refuses a character
// device, and a mux that loses the protocol on the way to the device.
func TestMuxSerial_StartOpensDeviceAtTheProtocolSpeed(t *testing.T) {
	for _, test := range []struct {
		protocol string
		baud     uint32
	}{{"ens", 115200}, {"enh", 9600}} {
		t.Run(test.protocol, func(t *testing.T) {
			pair := ptytest.Open(t)
			adapter := ptytest.ServeAdapter(pair)
			mux := newSerialTestMux(test.protocol, pair.DevicePath, nil)
			if err := mux.Start(context.Background()); err != nil {
				t.Fatalf("start: %v", err)
			}
			defer func() { _ = mux.Close() }()

			adapter.WaitInit(t, serialTestGuard)
			if got := pair.Baud(t); got != test.baud {
				t.Fatalf("speed = %d, want %d", got, test.baud)
			}
			mux.connMu.Lock()
			_, isSerial := mux.conn.(*serialConn)
			mux.connMu.Unlock()
			if !isSerial {
				t.Fatal("upstream connection is not the serial connection")
			}
		})
	}
}

func TestMuxSerial_StartReportsAnAbsentDevice(t *testing.T) {
	mux := newSerialTestMux("ens", filepath.Join(t.TempDir(), "absent"), nil)
	defer func() { _ = mux.Close() }()
	if err := mux.Start(context.Background()); !errors.Is(err, unix.ENOENT) {
		t.Fatalf("start: %v, want ENOENT", err)
	}
}

// Kills: a reconnect that does not reopen the path, and a retry that spins while
// the device is absent. The stable link plays the part of a /dev/serial/by-id/
// entry: udev removes it on unplug and recreates it for the new device node.
func TestMuxSerial_ReopensThePathAfterTheDeviceWentAway(t *testing.T) {
	link := filepath.Join(t.TempDir(), "adapter")
	first := ptytest.Open(t)
	if err := os.Symlink(first.DevicePath, link); err != nil {
		t.Fatal(err)
	}
	firstAdapter := ptytest.ServeAdapter(first)
	logs := make(serialLogLines, 256)
	mux := newSerialTestMux("ens", link, logs)
	if err := mux.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = mux.Close() }()
	firstAdapter.WaitInit(t, serialTestGuard)

	// Allocated while the first pair lives, so the replacement device node
	// differs: only following the configured path can find it.
	second := ptytest.Open(t)
	if second.DevicePath == first.DevicePath {
		t.Fatalf("both pairs use %s", first.DevicePath)
	}

	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	first.Close()

	const absent = "no such file or directory"
	firstFailure := logs.wait(t, absent)
	secondFailure := logs.wait(t, absent)
	if !strings.Contains(firstFailure.text, "reconnect failed") {
		t.Fatalf("unexpected failure line: %q", firstFailure.text)
	}
	// The first retry waited ReconnectInitialDelay, the second twice that.
	if gap := secondFailure.at.Sub(firstFailure.at); gap < 80*time.Millisecond {
		t.Fatalf("retries %v apart, want the doubled backoff of 80ms", gap)
	}

	secondAdapter := ptytest.ServeAdapter(second)
	if err := os.Symlink(second.DevicePath, link); err != nil {
		t.Fatal(err)
	}
	secondAdapter.WaitInit(t, serialTestGuard)
	logs.wait(t, "reconnected to "+NetworkSerial+"/"+link)
	if got := second.Baud(t); got != 115200 {
		t.Fatalf("speed after reopen = %d, want 115200", got)
	}
}
