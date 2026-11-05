//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// These tests use a pseudo terminal. It reproduces what the opener controls:
// the attributes written by the first update, the queue flush, and echo, which
// a pseudo terminal sends back to its control side. It cannot reproduce two
// real serial-driver behaviors. A pseudo terminal has no throttle hook, so an
// inherited IXOFF never makes it transmit a stop or start character; that
// case is covered by asserting that the first update clears the flag. And a
// pseudo terminal resets its attributes when the last descriptor closes,
// while serial and USB serial drivers keep the last attributes across close
// and restore them on the next open. The persistence that the preparation run
// (-prepare) relies on therefore cannot be shown here; the command test shows
// that the preparation run opens and closes the port through the same opener.

type teslaTestPTY struct {
	// control is the side a bus peer would drive; the device side is what the
	// opener opens by path. The test keeps one device descriptor open so the
	// attributes persist between opens, as they do for a serial driver.
	control  *os.File
	path     string
	deviceFD int
}

func newTeslaTestPTY(t *testing.T) *teslaTestPTY {
	t.Helper()
	controlFD, err := syscall.Open("/dev/ptmx", syscall.O_RDWR|syscall.O_NOCTTY|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		t.Skipf("no pseudo terminal: %v", err)
	}
	control := os.NewFile(uintptr(controlFD), "synthetic-pty-control")
	t.Cleanup(func() { _ = control.Close() })
	var unlock int32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(controlFD), syscall.TIOCSPTLCK, uintptr(unsafe.Pointer(&unlock))); errno != 0 {
		t.Skipf("cannot unlock pseudo terminal: %v", errno)
	}
	var number uint32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(controlFD), syscall.TIOCGPTN, uintptr(unsafe.Pointer(&number))); errno != 0 {
		t.Skipf("no pseudo terminal number: %v", errno)
	}
	path := "/dev/pts/" + strconv.FormatUint(uint64(number), 10)
	deviceFD, err := syscall.Open(path, syscall.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		t.Skipf("cannot open the device side: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Close(deviceFD) })
	return &teslaTestPTY{control: control, path: path, deviceFD: deviceFD}
}

// inherit sets the attributes a never-prepared port can carry from an earlier
// user: software flow control and echo on. Canonical mode is off so that
// received bytes count as readable input before any line ends.
func (pty *teslaTestPTY) inherit(t *testing.T) {
	t.Helper()
	var termios syscall.Termios
	if err := teslaTermios(pty.deviceFD, syscall.TCGETS, &termios); err != nil {
		t.Fatal(err)
	}
	termios.Iflag |= syscall.IXON | syscall.IXOFF | syscall.IXANY
	termios.Lflag |= syscall.ECHO
	termios.Lflag &^= syscall.ICANON
	termios.Cc[syscall.VMIN], termios.Cc[syscall.VTIME] = 1, 0
	if err := teslaTermios(pty.deviceFD, syscall.TCSETS, &termios); err != nil {
		t.Fatal(err)
	}
}

// queuedInput reports how many received bytes wait to be read on the device side.
func (pty *teslaTestPTY) queuedInput(t *testing.T) int {
	t.Helper()
	var queued int32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(pty.deviceFD), syscall.TIOCINQ, uintptr(unsafe.Pointer(&queued))); errno != 0 {
		t.Fatalf("queued input: %v", errno)
	}
	return int(queued)
}

// waitQueuedInput waits until the device side holds want unread bytes.
func (pty *teslaTestPTY) waitQueuedInput(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for pty.queuedInput(t) != want {
		if time.Now().After(deadline) {
			t.Fatalf("device side holds %d unread bytes, want %d", pty.queuedInput(t), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// echoed collects what the device side sent back to the control side, up to
// want bytes or the timeout.
func (pty *teslaTestPTY) echoed(t *testing.T, want int, timeout time.Duration) []byte {
	t.Helper()
	var got []byte
	deadline := time.Now().Add(timeout)
	buffer := make([]byte, 256)
	for len(got) < want {
		if err := pty.control.SetReadDeadline(deadline); err != nil {
			t.Fatal(err)
		}
		n, err := pty.control.Read(buffer)
		got = append(got, buffer[:n]...)
		if err != nil {
			break
		}
	}
	return got
}

// assertSilent fails when the device side sends anything to the control side.
func (pty *teslaTestPTY) assertSilent(t *testing.T, what string) {
	t.Helper()
	if got := pty.echoed(t, 1, 50*time.Millisecond); len(got) != 0 {
		t.Fatalf("%s transmitted %x", what, got)
	}
}

func TestTeslaObserveDisablesInheritedSoftwareFlowControl(t *testing.T) {
	pty := newTeslaTestPTY(t)
	pty.inherit(t)
	updates, flushes := 0, 0
	ops := teslaSerialOps{
		termios: func(fd int, request uintptr, settings *syscall.Termios) error {
			if request == syscall.TCSETS {
				updates++
				if settings.Iflag&(syscall.IXON|syscall.IXOFF|syscall.IXANY) != 0 || settings.Lflag&syscall.ECHO != 0 {
					t.Fatal("initial setup permits automatic transmission")
				}
				if settings.Cflag != syscall.B115200|syscall.CS8|syscall.CREAD|syscall.CLOCAL {
					t.Fatalf("initial serial format = %#x", settings.Cflag)
				}
			}
			return teslaTermios(fd, request, settings)
		},
		flush: func(fd int) error {
			if updates != 1 {
				t.Fatalf("flush after %d termios updates", updates)
			}
			flushes++
			return teslaFlushQueues(fd)
		},
	}
	if err := configureTeslaObserveSerial(pty.deviceFD, ops); err != nil {
		t.Fatal(err)
	}
	if updates != 1 || flushes != 1 {
		t.Fatalf("termios updates = %d, flushes = %d", updates, flushes)
	}
	var got syscall.Termios
	if err := teslaTermios(pty.deviceFD, syscall.TCGETS, &got); err != nil {
		t.Fatal(err)
	}
	if got.Iflag&(syscall.IXON|syscall.IXOFF|syscall.IXANY) != 0 || got.Lflag&syscall.ECHO != 0 {
		t.Fatalf("automatic transmission still enabled: iflag %#x lflag %#x", got.Iflag, got.Lflag)
	}
}

// Bytes that reach the port before the configuration are handled under the
// inherited attributes. The test writes such bytes first and then runs the
// production opener: the stale bytes must be gone, new bytes must arrive, and
// nothing may be echoed after the configuration.
func TestTeslaObserveFlushesTrafficReceivedBeforeConfiguration(t *testing.T) {
	pty := newTeslaTestPTY(t)
	pty.inherit(t)
	stale := bytes.Repeat([]byte{0x42}, 64)
	if _, err := pty.control.Write(stale); err != nil {
		t.Fatal(err)
	}
	// The inherited echo answers the stale bytes before any configuration can
	// run. That window is the residual case the preparation run closes, not
	// behavior under test; draining it keeps it from being mistaken for a later
	// transmission, and it shows that the inherited attributes do transmit on
	// this terminal, so the silence asserted below is meaningful.
	if echo := pty.echoed(t, len(stale), 2*time.Second); !bytes.Equal(echo, stale) {
		t.Fatalf("inherited echo answered %d of %d stale bytes; the test cannot discriminate", len(echo), len(stale))
	}
	pty.waitQueuedInput(t, len(stale))

	stream, err := openTeslaObserveSerial(pty.path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()

	// (1) The stale bytes were flushed and cannot be read from the device side.
	if queued := pty.queuedInput(t); queued != 0 {
		t.Fatalf("%d stale bytes still queued after configuration", queued)
	}
	quiet, quietCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer quietCancel()
	if value, err := stream.ReceiveByte(quiet); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stale byte %x survived the flush (error %v)", value, err)
	}

	// (2) Traffic after the configuration is received intact, starting with
	// the first new byte, and produces no echo or flow-control character.
	fresh := bytes.Repeat([]byte{0x43}, 8192)
	if _, err := pty.control.Write(fresh); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for index := range fresh {
		value, err := stream.ReceiveByte(ctx)
		if err != nil || value != 0x43 {
			t.Fatalf("byte %d = %x, %v", index, value, err)
		}
	}
	pty.assertSilent(t, "traffic after configuration")

	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := stream.ReceiveByte(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled receive = %v", err)
	}
}

func TestTeslaObserveInitialTermiosDisablesAutomaticTransmission(t *testing.T) {
	inherited := syscall.Termios{
		Iflag: syscall.IXON | syscall.IXOFF | syscall.IXANY,
		Lflag: syscall.ECHO | syscall.ICANON,
		Cflag: syscall.CS7 | syscall.PARENB,
	}
	updates := 0
	ops := teslaSerialOps{
		termios: func(_ int, request uintptr, termios *syscall.Termios) error {
			switch request {
			case syscall.TCGETS:
				*termios = inherited
			case syscall.TCSETS:
				updates++
				if termios.Iflag != 0 || termios.Lflag != 0 || termios.Oflag != 0 ||
					termios.Cflag != syscall.B115200|syscall.CS8|syscall.CREAD|syscall.CLOCAL ||
					termios.Cc[syscall.VMIN] != 1 || termios.Cc[syscall.VTIME] != 0 {
					t.Fatalf("unsafe initial receive settings: %+v", termios)
				}
				inherited = *termios
			default:
				t.Fatalf("unexpected ioctl %d", request)
			}
			return nil
		},
		flush: func(int) error { return nil },
	}
	if err := configureTeslaObserveSerial(0, ops); err != nil || updates != 1 {
		t.Fatalf("initial setup: updates=%d error=%v", updates, err)
	}
}

// The configuration is exactly: read the attributes, one update, a read-back
// check and then the flush of both queues. The flush never runs when the
// settings were not applied, and its failure fails the open.
func TestTeslaObserveConfigurationOrderAndFlushFailures(t *testing.T) {
	flushFailure := errors.New("synthetic flush failure")
	setFailure := errors.New("synthetic update failure")
	applied := []string{"TCGETS", "TCSETS", "TCGETS", "flush"}
	notApplied := []string{"TCGETS", "TCSETS", "TCGETS"}
	cases := []struct {
		name string
		// readBack edits the attributes the second read returns.
		readBack  func(*syscall.Termios)
		setErr    error
		flushErr  error
		wantCalls []string
		// wantErr is the exact failure; failed marks a failure without one.
		wantErr error
		failed  bool
	}{
		{name: "applied", wantCalls: applied},
		{name: "update fails", setErr: setFailure, wantCalls: []string{"TCGETS", "TCSETS"}, wantErr: setFailure},
		{name: "echo remains", readBack: func(termios *syscall.Termios) { termios.Lflag |= syscall.ECHO }, wantCalls: notApplied, failed: true},
		{name: "input flow control remains", readBack: func(termios *syscall.Termios) { termios.Iflag |= syscall.IXOFF }, wantCalls: notApplied, failed: true},
		{name: "baud coerced", readBack: func(termios *syscall.Termios) { termios.Cflag = termios.Cflag&^syscall.B115200 | syscall.B9600 }, wantCalls: notApplied, failed: true},
		{name: "character size coerced", readBack: func(termios *syscall.Termios) { termios.Cflag = termios.Cflag&^syscall.CSIZE | syscall.CS7 }, wantCalls: notApplied, failed: true},
		{name: "parity enabled", readBack: func(termios *syscall.Termios) { termios.Cflag |= syscall.PARENB }, wantCalls: notApplied, failed: true},
		{name: "odd parity enabled", readBack: func(termios *syscall.Termios) { termios.Cflag |= syscall.PARODD }, wantCalls: notApplied, failed: true},
		{name: "two stop bits", readBack: func(termios *syscall.Termios) { termios.Cflag |= syscall.CSTOPB }, wantCalls: notApplied, failed: true},
		{name: "receiver disabled", readBack: func(termios *syscall.Termios) { termios.Cflag &^= syscall.CREAD }, wantCalls: notApplied, failed: true},
		{name: "local mode disabled", readBack: func(termios *syscall.Termios) { termios.Cflag &^= syscall.CLOCAL }, wantCalls: notApplied, failed: true},
		{name: "hardware flow control enabled", readBack: func(termios *syscall.Termios) { termios.Cflag |= unix.CRTSCTS }, wantCalls: notApplied, failed: true},
		{name: "canonical mode enabled", readBack: func(termios *syscall.Termios) { termios.Lflag |= syscall.ICANON }, wantCalls: notApplied, failed: true},
		{name: "signals enabled", readBack: func(termios *syscall.Termios) { termios.Lflag |= syscall.ISIG }, wantCalls: notApplied, failed: true},
		{name: "output processing enabled", readBack: func(termios *syscall.Termios) { termios.Oflag |= syscall.OPOST }, wantCalls: notApplied, failed: true},
		{name: "input translation enabled", readBack: func(termios *syscall.Termios) { termios.Iflag |= syscall.ICRNL }, wantCalls: notApplied, failed: true},
		{name: "minimum read coerced", readBack: func(termios *syscall.Termios) { termios.Cc[syscall.VMIN] = 0 }, wantCalls: notApplied, failed: true},
		{name: "read timeout coerced", readBack: func(termios *syscall.Termios) { termios.Cc[syscall.VTIME] = 1 }, wantCalls: notApplied, failed: true},
		{name: "flush fails", flushErr: flushFailure, wantCalls: applied, wantErr: flushFailure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			gets := 0
			var requested syscall.Termios
			ops := teslaSerialOps{
				termios: func(_ int, request uintptr, termios *syscall.Termios) error {
					switch request {
					case syscall.TCGETS:
						calls = append(calls, "TCGETS")
						gets++
						*termios = syscall.Termios{Iflag: syscall.IXOFF, Lflag: syscall.ECHO}
						if gets == 2 {
							*termios = requested
							if tc.readBack != nil {
								tc.readBack(termios)
							}
						}
					case syscall.TCSETS:
						calls = append(calls, "TCSETS")
						requested = *termios
						return tc.setErr
					default:
						t.Fatalf("unexpected ioctl %d", request)
					}
					return nil
				},
				flush: func(int) error {
					calls = append(calls, "flush")
					return tc.flushErr
				},
			}
			err := configureTeslaObserveSerial(0, ops)
			switch {
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			case tc.failed && err == nil:
				t.Fatal("unapplied settings were accepted")
			case tc.wantErr == nil && !tc.failed && err != nil:
				t.Fatalf("unexpected error %v", err)
			}
			if strings.Join(calls, " ") != strings.Join(tc.wantCalls, " ") {
				t.Fatalf("calls = %v, want %v", calls, tc.wantCalls)
			}
		})
	}
}
