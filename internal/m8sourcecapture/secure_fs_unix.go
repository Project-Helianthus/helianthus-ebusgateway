//go:build darwin || linux

package m8sourcecapture

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

type secureDirectory struct {
	file *os.File
}

func openSecureDirectory(path string, requirePrivate bool) (*secureDirectory, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("%w: unsafe directory", errInvalidCapture)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: open directory", errInvalidCapture)
	}
	if err := verifyDirectoryFD(fd, requirePrivate); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return &secureDirectory{file: os.NewFile(uintptr(fd), path)}, nil
}

func verifyDirectoryFD(fd int, requirePrivate bool) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("%w: directory stat", errInvalidCapture)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || requirePrivate && stat.Mode&0o777 != 0o700 {
		return fmt.Errorf("%w: directory mode %o", errInvalidCapture, stat.Mode&0o777)
	}
	return nil
}

func (directory *secureDirectory) fd() int { return int(directory.file.Fd()) }

func (directory *secureDirectory) close() error {
	if directory == nil || directory.file == nil {
		return nil
	}
	return directory.file.Close()
}

func (directory *secureDirectory) entries() ([]os.DirEntry, error) {
	if directory == nil || directory.file == nil {
		return nil, fmt.Errorf("%w: closed directory", errInvalidCapture)
	}
	entries, err := directory.file.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("%w: read directory", errInvalidCapture)
	}
	return entries, nil
}

func (directory *secureDirectory) readRegular(name string, limit int64) ([]byte, error) {
	if !safeChildName(name) {
		return nil, fmt.Errorf("%w: unsafe input name", errInvalidCapture)
	}
	fd, err := unix.Openat(directory.fd(), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: open input", errInvalidCapture)
	}
	file := os.NewFile(uintptr(fd), name)
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil || before.Mode&unix.S_IFMT != unix.S_IFREG || before.Mode&0o777 != 0o600 || before.Nlink != 1 || before.Size < 1 || before.Size > limit {
		_ = file.Close()
		return nil, fmt.Errorf("%w: unsafe input", errInvalidCapture)
	}
	payload, readErr := io.ReadAll(io.LimitReader(file, limit+1))
	var after unix.Stat_t
	statErr := unix.Fstat(fd, &after)
	closeErr := file.Close()
	if readErr != nil || statErr != nil || closeErr != nil || len(payload) < 1 || int64(len(payload)) > limit ||
		before.Dev != after.Dev || before.Ino != after.Ino || before.Size != after.Size || before.Mode != after.Mode || before.Nlink != after.Nlink {
		return nil, fmt.Errorf("%w: read input", errInvalidCapture)
	}
	return payload, nil
}

func (directory *secureDirectory) childDirectory(name string, create bool) (*secureDirectory, error) {
	if !safeChildName(name) {
		return nil, fmt.Errorf("%w: unsafe directory name", errInvalidCapture)
	}
	if create {
		if err := unix.Mkdirat(directory.fd(), name, 0o700); err != nil {
			return nil, fmt.Errorf("%w: create directory", errInvalidCapture)
		}
	}
	fd, err := unix.Openat(directory.fd(), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: open child directory", errInvalidCapture)
	}
	if err := unix.Fchmod(fd, 0o700); err != nil || verifyDirectoryFD(fd, true) != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("%w: secure child directory", errInvalidCapture)
	}
	return &secureDirectory{file: os.NewFile(uintptr(fd), name)}, nil
}

func (directory *secureDirectory) temporaryDirectory() (string, *secureDirectory, error) {
	for attempt := 0; attempt < 32; attempt++ {
		var random [16]byte
		if _, err := io.ReadFull(rand.Reader, random[:]); err != nil {
			return "", nil, fmt.Errorf("%w: temporary entropy", errInvalidCapture)
		}
		name := ".m8sourcecapture-" + hex.EncodeToString(random[:])
		if err := unix.Mkdirat(directory.fd(), name, 0o700); err != nil {
			if errors.Is(err, unix.EEXIST) {
				continue
			}
			return "", nil, fmt.Errorf("%w: create temporary directory", errInvalidCapture)
		}
		child, err := directory.childDirectory(name, false)
		if err != nil {
			_ = unix.Unlinkat(directory.fd(), name, unix.AT_REMOVEDIR)
			return "", nil, err
		}
		return name, child, nil
	}
	return "", nil, fmt.Errorf("%w: temporary directory collision", errInvalidCapture)
}

func (directory *secureDirectory) writeRegular(name string, payload []byte) error {
	if !safeChildName(name) {
		return fmt.Errorf("%w: unsafe output name", errInvalidCapture)
	}
	fd, err := unix.Openat(directory.fd(), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("%w: create source file", errInvalidCapture)
	}
	file := os.NewFile(uintptr(fd), name)
	written, writeErr := file.Write(payload)
	chmodErr := unix.Fchmod(fd, 0o600)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || written != len(payload) || syncErr != nil || chmodErr != nil || closeErr != nil {
		return fmt.Errorf("%w: write source file", errInvalidCapture)
	}
	return nil
}

func (directory *secureDirectory) sync() error {
	if directory == nil || directory.file == nil || directory.file.Sync() != nil {
		return fmt.Errorf("%w: sync directory", errInvalidCapture)
	}
	return nil
}

func (directory *secureDirectory) absent(name string) error {
	if !safeChildName(name) {
		return fmt.Errorf("%w: unsafe output name", errInvalidCapture)
	}
	var stat unix.Stat_t
	err := unix.Fstatat(directory.fd(), name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	return fmt.Errorf("%w: destination exists or cannot be inspected", errInvalidCapture)
}

func (directory *secureDirectory) renameNoReplace(source, destination string) error {
	if !safeChildName(source) || !safeChildName(destination) || renameNoReplaceAt(directory.fd(), source, destination) != nil {
		return fmt.Errorf("%w: atomically publish root", errInvalidCapture)
	}
	return nil
}

func (directory *secureDirectory) removeTree(name string) error {
	child, err := directory.childDirectory(name, false)
	if err != nil {
		return err
	}
	if err := removeDirectoryContents(child); err != nil {
		_ = child.close()
		return err
	}
	if err := child.close(); err != nil {
		return err
	}
	if err := unix.Unlinkat(directory.fd(), name, unix.AT_REMOVEDIR); err != nil {
		return fmt.Errorf("%w: remove directory", errInvalidCapture)
	}
	return nil
}

func removeDirectoryContents(directory *secureDirectory) error {
	entries, err := directory.entries()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		var stat unix.Stat_t
		if !safeChildName(name) || unix.Fstatat(directory.fd(), name, &stat, unix.AT_SYMLINK_NOFOLLOW) != nil {
			return fmt.Errorf("%w: cleanup entry", errInvalidCapture)
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			if err := directory.removeTree(name); err != nil {
				return err
			}
			continue
		}
		if err := unix.Unlinkat(directory.fd(), name, 0); err != nil {
			return fmt.Errorf("%w: cleanup file", errInvalidCapture)
		}
	}
	return nil
}

func safeChildName(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name && !strings.Contains(name, string(filepath.Separator))
}
