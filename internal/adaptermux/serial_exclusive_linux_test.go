package adaptermux

import (
	"errors"
	"os"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/ptytest"
	"golang.org/x/sys/unix"
)

// Kills: a missing or wrong exclusive ioctl. The flag is read back so the check
// also holds for a privileged test user, whom the kernel lets through.
func TestOpenSerialConn_ExclusiveOpenIsRefusedByTheKernel(t *testing.T) {
	pair := ptytest.Open(t)
	conn := openSerialForTest(t, pair.SlavePath, "ens")

	raw, err := conn.(*serialConn).serialPort.(*os.File).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var exclusive int
	var ioctlErr error
	if err := raw.Control(func(fd uintptr) {
		exclusive, ioctlErr = unix.IoctlGetInt(int(fd), unix.TIOCGEXCL)
	}); err != nil {
		t.Fatal(err)
	}
	if ioctlErr != nil {
		t.Fatalf("TIOCGEXCL: %v", ioctlErr)
	}
	if exclusive != 1 {
		t.Fatalf("TIOCGEXCL = %d, want 1", exclusive)
	}

	if os.Geteuid() == 0 {
		return
	}
	second, err := openSerialConn(pair.SlavePath, "ens")
	if err == nil {
		_ = second.Close()
		t.Fatal("second open succeeded while the device is held")
	}
	if !errors.Is(err, unix.EBUSY) {
		t.Fatalf("second open: %v, want EBUSY", err)
	}
}
