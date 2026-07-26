//go:build linux

package mcp

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type eebusV1PinnedParent struct {
	fd    int
	child string
}

type eebusV1SocketIdentity struct {
	device uint64
	inode  uint64
	mode   uint32
	uid    uint32
}

type eebusV1AnchoredListener struct {
	*net.UnixListener
	once     sync.Once
	parent   *eebusV1PinnedParent
	identity eebusV1SocketIdentity
}

func (listener *eebusV1AnchoredListener) Close() error {
	var result error
	listener.once.Do(func() {
		result = listener.UnixListener.Close()
		if removeErr := listener.parent.removeIfSame(listener.identity); result == nil {
			result = removeErr
		}
		if closeErr := unix.Close(listener.parent.fd); result == nil {
			result = closeErr
		}
	})
	return result
}

func eebusV1PlatformListenOperator(socketPath string) (net.Listener, error) {
	parent, err := eebusV1OpenPinnedParent(socketPath)
	if err != nil {
		return nil, err
	}
	keepParent := false
	defer func() {
		if !keepParent {
			_ = unix.Close(parent.fd)
		}
	}()
	if err := parent.removeStale(func(address string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("unix", address, timeout)
	}); err != nil {
		return nil, err
	}

	address := &net.UnixAddr{Name: parent.anchoredPath(), Net: "unix"}
	base, err := net.ListenUnix("unix", address)
	if err != nil {
		return nil, err
	}
	base.SetUnlinkOnClose(false)
	keepListener := false
	var cleanupIdentity *eebusV1SocketIdentity
	defer func() {
		if !keepListener {
			_ = base.Close()
			if cleanupIdentity != nil {
				_ = parent.removeIfSame(*cleanupIdentity)
			}
		}
	}()

	before, err := parent.statChild()
	if err != nil {
		return nil, err
	}
	cleanupIdentity = &before
	if err := unix.Fchmodat(parent.fd, parent.child, 0o600, 0); err != nil {
		return nil, err
	}
	after, err := parent.statChild()
	if err != nil || before != after || !after.isOwnedSocket(0o600) {
		return nil, errors.New("eeBUS operator socket permission and identity proof failed")
	}
	keepParent = true
	keepListener = true
	return &eebusV1AnchoredListener{UnixListener: base, parent: parent, identity: after}, nil
}

func eebusV1OpenPinnedParent(socketPath string) (*eebusV1PinnedParent, error) {
	if socketPath == "" || !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath {
		return nil, errors.New("eeBUS operator socket path must be a clean absolute path")
	}
	parentPath, child := filepath.Split(socketPath)
	parentPath = strings.TrimSuffix(parentPath, string(filepath.Separator))
	if parentPath == "" || parentPath == "/" || child == "" || child == "." || child == ".." ||
		strings.ContainsRune(child, filepath.Separator) {
		return nil, errors.New("eeBUS operator socket path has an invalid parent or child")
	}

	current, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	components := strings.Split(strings.TrimPrefix(parentPath, "/"), "/")
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			_ = unix.Close(current)
			return nil, errors.New("eeBUS operator socket parent contains an invalid component")
		}
		next, openErr := unix.Openat(current, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if errors.Is(openErr, unix.ENOENT) {
			if mkdirErr := unix.Mkdirat(current, component, 0o700); mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
				_ = unix.Close(current)
				return nil, mkdirErr
			}
			next, openErr = unix.Openat(current, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		}
		_ = unix.Close(current)
		if openErr != nil {
			return nil, fmt.Errorf("open eeBUS operator parent component %q without following links: %w", component, openErr)
		}
		current = next
	}

	var stat unix.Stat_t
	if err := unix.Fstat(current, &stat); err != nil {
		_ = unix.Close(current)
		return nil, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != uint32(os.Geteuid()) {
		_ = unix.Close(current)
		return nil, errors.New("eeBUS operator socket parent ownership proof failed")
	}
	if err := unix.Fchmod(current, 0o700); err != nil {
		_ = unix.Close(current)
		return nil, err
	}
	if err := unix.Fstat(current, &stat); err != nil ||
		stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mode&0o777 != 0o700 || stat.Uid != uint32(os.Geteuid()) {
		_ = unix.Close(current)
		return nil, errors.New("eeBUS operator socket parent permission proof failed")
	}
	return &eebusV1PinnedParent{fd: current, child: child}, nil
}

func (parent *eebusV1PinnedParent) anchoredPath() string {
	return fmt.Sprintf("/proc/self/fd/%d/%s", parent.fd, parent.child)
}

func (parent *eebusV1PinnedParent) statChild() (eebusV1SocketIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(parent.fd, parent.child, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return eebusV1SocketIdentity{}, err
	}
	return eebusV1SocketIdentity{
		device: uint64(stat.Dev), inode: stat.Ino, mode: stat.Mode, uid: stat.Uid,
	}, nil
}

func (identity eebusV1SocketIdentity) isOwnedSocket(permissions uint32) bool {
	return identity.mode&unix.S_IFMT == unix.S_IFSOCK &&
		identity.mode&0o777 == permissions &&
		identity.uid == uint32(os.Geteuid())
}

func (parent *eebusV1PinnedParent) removeStale(dial func(string, time.Duration) (net.Conn, error)) error {
	initial, err := parent.statChild()
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	if !initial.isOwnedSocket(initial.mode & 0o777) {
		return errors.New("eeBUS operator socket path is not an owned socket")
	}
	connection, dialErr := dial(parent.anchoredPath(), 100*time.Millisecond)
	if dialErr == nil {
		_ = connection.Close()
		return errors.New("eeBUS operator socket already has an active listener")
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("eeBUS operator stale-socket proof was inconclusive: %w", dialErr)
	}
	current, err := parent.statChild()
	if err != nil {
		return err
	}
	if current != initial {
		return errors.New("eeBUS operator socket identity changed during stale proof")
	}
	return unix.Unlinkat(parent.fd, parent.child, 0)
}

func (parent *eebusV1PinnedParent) removeIfSame(expected eebusV1SocketIdentity) error {
	current, err := parent.statChild()
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	if current != expected {
		return nil
	}
	return unix.Unlinkat(parent.fd, parent.child, 0)
}
