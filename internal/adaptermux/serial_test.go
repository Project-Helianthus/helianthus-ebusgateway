//go:build linux || darwin

package adaptermux

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/ptytest"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"golang.org/x/sys/unix"
)

const serialTestGuard = 10 * time.Second

func openSerialForTest(t *testing.T, path, protocol string) net.Conn {
	t.Helper()
	conn, err := openSerialConn(path, protocol)
	if err != nil {
		t.Fatalf("openSerialConn(%q, %q): %v", path, protocol, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// openRetrySerialForTest forces the fallback: a file created over a blocking
// descriptor stays outside the runtime poller, as on a platform that refuses
// to poll ttys.
func openRetrySerialForTest(t *testing.T, path, protocol string) net.Conn {
	t.Helper()
	baud, err := serialBaudForProtocol(protocol)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	if err := configureSerialFD(fd, baud); err != nil {
		_ = unix.Close(fd)
		t.Fatalf("configure: %v", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = file.Close()
		t.Fatalf("set nonblock: %v", err)
	}
	conn := newSerialConn(file, path)
	t.Cleanup(func() { _ = conn.Close() })
	if _, ok := conn.serialPort.(*retrySerialPort); !ok {
		t.Fatalf("port = %T, want *retrySerialPort", conn.serialPort)
	}
	return conn
}

func forEachSerialPort(t *testing.T, run func(t *testing.T, pair *ptytest.Pair, conn net.Conn)) {
	t.Helper()
	for name, open := range map[string]func(*testing.T, string, string) net.Conn{
		"poller": openSerialForTest,
		"retry":  openRetrySerialForTest,
	} {
		t.Run(name, func(t *testing.T) {
			pair := ptytest.Open(t)
			run(t, pair, open(t, pair.SlavePath, "ens"))
		})
	}
}

// Kills: a wrong speed for either scheme, including the empty protocol default.
func TestOpenSerialConn_SpeedFollowsProtocol(t *testing.T) {
	for _, test := range []struct {
		protocol string
		want     uint32
	}{{"ens", 115200}, {"enh", 9600}, {"", 9600}} {
		t.Run("protocol="+test.protocol, func(t *testing.T) {
			pair := ptytest.Open(t)
			openSerialForTest(t, pair.SlavePath, test.protocol)
			if got := pair.Baud(t); got != test.want {
				t.Fatalf("speed = %d, want %d", got, test.want)
			}
		})
	}
}

func TestOpenSerialConn_RawMode8N1WithoutFlowControl(t *testing.T) {
	pair := ptytest.Open(t)
	openSerialForTest(t, pair.SlavePath, "ens")
	termios := pair.Termios(t)

	if termios.Cflag&unix.CSIZE != unix.CS8 {
		t.Errorf("CSIZE bits = %#x, want CS8", termios.Cflag&unix.CSIZE)
	}
	if termios.Cflag&(unix.PARENB|unix.CSTOPB|unix.CRTSCTS) != 0 {
		t.Errorf("cflag %#x enables parity, a second stop bit or flow control", termios.Cflag)
	}
	if termios.Cflag&(unix.CREAD|unix.CLOCAL) != unix.CREAD|unix.CLOCAL {
		t.Errorf("cflag %#x misses CREAD or CLOCAL", termios.Cflag)
	}
	if termios.Iflag&(unix.IXON|unix.IXOFF|unix.IXANY|unix.ICRNL|unix.INLCR|unix.IGNCR|unix.ISTRIP) != 0 {
		t.Errorf("iflag %#x keeps input processing or software flow control", termios.Iflag)
	}
	if termios.Oflag&unix.OPOST != 0 {
		t.Errorf("oflag %#x keeps output processing", termios.Oflag)
	}
	if termios.Lflag&(unix.ICANON|unix.ECHO|unix.ISIG|unix.IEXTEN) != 0 {
		t.Errorf("lflag %#x keeps line processing", termios.Lflag)
	}
	// VMIN=0 would turn every idle non-blocking read into a false EOF.
	if termios.Cc[unix.VMIN] != 1 || termios.Cc[unix.VTIME] != 0 {
		t.Errorf("VMIN=%d VTIME=%d, want 1 and 0", termios.Cc[unix.VMIN], termios.Cc[unix.VTIME])
	}
}

func TestOpenSerialConn_DiscardsPendingInput(t *testing.T) {
	pair := ptytest.Open(t)
	// The slave stays open across the master write so the line discipline
	// holds the stale bytes when the code under test opens the device.
	holder, err := unix.Open(pair.SlavePath, unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	if err != nil {
		t.Fatalf("open slave: %v", err)
	}
	defer func() { _ = unix.Close(holder) }()
	pair.WriteMaster(t, []byte("stale\n"))

	conn := openSerialForTest(t, pair.SlavePath, "ens")
	pair.WriteMaster(t, []byte{0x42})

	if err := conn.SetReadDeadline(time.Now().Add(serialTestGuard)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 16)
	n, err := conn.Read(got)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got[:n], []byte{0x42}) {
		t.Fatalf("read %x, want only the byte sent after the open", got[:n])
	}
}

// Kills: an open without the exclusive request. The kernel refusal itself is
// asserted on Linux in serial_exclusive_linux_test.go; a Darwin pseudo-terminal
// does not refuse an unprivileged second open.
func TestOpenSerialConn_RequestsExclusiveAccess(t *testing.T) {
	pair := ptytest.Open(t)
	original := serialSetExclusive
	defer func() { serialSetExclusive = original }()
	calls := 0
	serialSetExclusive = func(fd int) error {
		calls++
		return original(fd)
	}
	openSerialForTest(t, pair.SlavePath, "ens")
	if calls != 1 {
		t.Fatalf("exclusive access requested %d times, want 1", calls)
	}
}

func TestOpenSerialConn_RefusedExclusiveAccessFailsAndReleasesDescriptor(t *testing.T) {
	pair := ptytest.Open(t)
	original := serialSetExclusive
	defer func() { serialSetExclusive = original }()
	refused := -1
	serialSetExclusive = func(fd int) error {
		refused = fd
		return unix.EBUSY
	}
	if conn, err := openSerialConn(pair.SlavePath, "ens"); !errors.Is(err, unix.EBUSY) {
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatalf("err = %v, want EBUSY", err)
	}
	if _, err := unix.FcntlInt(uintptr(refused), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("descriptor %d still open after the refusal: %v", refused, err)
	}
}

func TestOpenSerialConn_RejectsWhatIsNotAnAdapterDevice(t *testing.T) {
	pair := ptytest.Open(t)
	regular := filepath.Join(t.TempDir(), "regular")
	if err := os.WriteFile(regular, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		path, protocol string
		want           error
	}{
		"absent device":    {filepath.Join(t.TempDir(), "absent"), "ens", unix.ENOENT},
		"regular file":     {regular, "ens", unix.ENOTTY},
		"relative path":    {"dev/ttyUSB0", "ens", nil},
		"unknown protocol": {pair.SlavePath, "tcp-plain", nil},
	} {
		t.Run(name, func(t *testing.T) {
			conn, err := openSerialConn(test.path, test.protocol)
			if err == nil {
				_ = conn.Close()
				t.Fatal("open succeeded")
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("err = %v, want %v", err, test.want)
			}
		})
	}
}

// Kills: any line processing left on the receive path, and a transport other
// than the framing the ENS constructor expects. Every symbol value crosses the
// device, including XON/XOFF, CR, LF, INTR and EOF control characters.
func TestSerialConn_ENSFramingPassesThrough(t *testing.T) {
	forEachSerialPort(t, func(t *testing.T, pair *ptytest.Pair, conn net.Conn) {
		tr := transport.NewENSTransport(conn, serialTestGuard, serialTestGuard)

		var want, wire []byte
		for symbol := 0; symbol <= 0xff; symbol++ {
			want = append(want, byte(symbol))
			frame := transport.EncodeENH(transport.ENHResReceived, byte(symbol))
			wire = append(wire, frame[:]...)
		}
		pair.WriteMaster(t, wire)

		// The raw view, because the logical view folds the eBUS escape sequence.
		got := make([]byte, 0, len(want))
		for len(got) < len(want) {
			_, symbol, hasSymbol, err := tr.ReadEventWithRawSymbol()
			if err != nil {
				t.Fatalf("after %d symbols: %v", len(got), err)
			}
			if hasSymbol {
				got = append(got, symbol)
			}
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("symbols through the ENS transport:\n got %x\nwant %x", got, want)
		}
	})
}

// Kills: output post-processing, which would expand LF in a transmitted byte.
func TestSerialConn_WritePassesThroughUnchanged(t *testing.T) {
	forEachSerialPort(t, func(t *testing.T, pair *ptytest.Pair, conn net.Conn) {
		tr := transport.NewENSTransport(conn, serialTestGuard, serialTestGuard)
		payload := []byte{0x0a, 0x0d, 0x11, 0x13, 0x03, 0x04, 0x7f}
		var want []byte
		for _, b := range payload {
			frame := transport.EncodeENH(transport.ENHReqSend, b)
			want = append(want, frame[:]...)
		}
		if _, err := tr.Write(payload); err != nil {
			t.Fatalf("write: %v", err)
		}
		if got := pair.ReadMaster(t, len(want), serialTestGuard); !bytes.Equal(got, want) {
			t.Fatalf("master read %x, want %x", got, want)
		}
	})
}

func TestSerialConn_ReadDeadlineExpiresAndLaterDataStillArrives(t *testing.T) {
	forEachSerialPort(t, func(t *testing.T, pair *ptytest.Pair, conn net.Conn) {
		if err := conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4)
		n, err := conn.Read(buf)
		if n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("read = %d, %v; want a deadline error", n, err)
		}
		// The mux and the transport classify the idle tick through net.Error.
		if !isNetTimeout(err) {
			t.Fatalf("%v is not a net timeout", err)
		}

		pair.WriteMaster(t, []byte{0x5a})
		if err := conn.SetReadDeadline(time.Now().Add(serialTestGuard)); err != nil {
			t.Fatal(err)
		}
		n, err = conn.Read(buf)
		if err != nil || n != 1 || buf[0] != 0x5a {
			t.Fatalf("read = %x, %v; want 5a", buf[:n], err)
		}
	})
}

func TestSerialConn_QuietBusIsATimeoutThroughTheTransport(t *testing.T) {
	forEachSerialPort(t, func(t *testing.T, _ *ptytest.Pair, conn net.Conn) {
		tr := transport.NewENSTransport(conn, 20*time.Millisecond, serialTestGuard)
		if _, err := tr.ReadEvent(); !errors.Is(err, ebuserrors.ErrTimeout) {
			t.Fatalf("err = %v, want ErrTimeout", err)
		}
	})
}

func TestSerialConn_ExpiredWriteDeadlineIsATimeout(t *testing.T) {
	forEachSerialPort(t, func(t *testing.T, _ *ptytest.Pair, conn net.Conn) {
		if err := conn.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte{0x01}); !isNetTimeout(err) {
			t.Fatalf("err = %v, want a net timeout", err)
		}
	})
}

// A vanished device is an ordinary disconnect: the read ends, it is no timeout,
// and the transport reports the closed connection the mux reconnects on.
func TestSerialConn_VanishedDeviceIsADisconnect(t *testing.T) {
	forEachSerialPort(t, func(t *testing.T, pair *ptytest.Pair, conn net.Conn) {
		tr := transport.NewENSTransport(conn, serialTestGuard, serialTestGuard)
		result := make(chan error, 1)
		go func() { // goroutine: blocked read that the device loss must end
			_, err := tr.ReadEvent()
			result <- err
		}()
		pair.Close()
		select {
		case err := <-result:
			if !errors.Is(err, ebuserrors.ErrTransportClosed) {
				t.Fatalf("err = %v, want ErrTransportClosed", err)
			}
		case <-time.After(serialTestGuard):
			t.Fatal("device loss did not end the read")
		}

		if _, err := conn.Write([]byte{0x01}); err == nil || isNetTimeout(err) {
			t.Fatalf("write after device loss = %v, want a non-timeout error", err)
		}
	})
}

func TestSerialConn_CloseEndsAPendingRead(t *testing.T) {
	forEachSerialPort(t, func(t *testing.T, _ *ptytest.Pair, conn net.Conn) {
		result := make(chan error, 1)
		go func() { // goroutine: blocked read that Close must end
			_, err := conn.Read(make([]byte, 1))
			result <- err
		}()
		// Close may run before or during the read; both end it the same way.
		if err := conn.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		select {
		case err := <-result:
			if !errors.Is(err, os.ErrClosed) && !errors.Is(err, io.EOF) {
				t.Fatalf("err = %v, want os.ErrClosed", err)
			}
		case <-time.After(serialTestGuard):
			t.Fatal("close did not end the read")
		}
	})
}

func TestSerialConn_Addresses(t *testing.T) {
	pair := ptytest.Open(t)
	conn := openSerialForTest(t, pair.SlavePath, "enh")
	for _, addr := range []net.Addr{conn.LocalAddr(), conn.RemoteAddr()} {
		if addr.Network() != NetworkSerial || addr.String() != pair.SlavePath {
			t.Fatalf("addr = %s/%s", addr.Network(), addr.String())
		}
	}
}

func TestSerialBaudForProtocol_RejectsUnknown(t *testing.T) {
	_, err := serialBaudForProtocol("udp-plain")
	if err == nil || !strings.Contains(err.Error(), "udp-plain") {
		t.Fatalf("err = %v", err)
	}
}
