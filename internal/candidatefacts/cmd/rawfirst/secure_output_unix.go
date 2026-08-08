//go:build darwin || linux

package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

type privateOutputRoot struct {
	fd int
}

func writePrivateOutputs(directory string, outputs []struct {
	name    string
	content []byte
}) (result error) {
	root, err := openPrivateOutputRoot(directory)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, root.close()) }()
	for _, output := range outputs {
		if err := root.writeNewFile(output.name, output.content); err != nil {
			return err
		}
	}
	return nil
}

func openPrivateOutputRoot(path string) (*privateOutputRoot, error) {
	clean := filepath.Clean(path)
	start := "."
	if filepath.IsAbs(clean) {
		start = string(filepath.Separator)
		clean = strings.TrimPrefix(clean, string(filepath.Separator))
	}
	fd, err := unix.Open(start, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	for _, component := range strings.Split(clean, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		if component == ".." {
			_ = unix.Close(fd)
			return nil, errors.New("output-dir cannot contain parent traversal")
		}
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return nil, openErr
		}
		fd = next
	}

	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil ||
		stat.Mode&unix.S_IFMT != unix.S_IFDIR ||
		stat.Mode&0o777 != 0o700 ||
		stat.Uid != uint32(os.Geteuid()) {
		_ = unix.Close(fd)
		return nil, errors.New("output-dir must be an owner-only directory with mode 0700")
	}
	if err := requireEmptyDirectory(fd); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	return &privateOutputRoot{fd: fd}, nil
}

func requireEmptyDirectory(fd int) error {
	duplicate, err := unix.Dup(fd)
	if err != nil {
		return err
	}
	directory := os.NewFile(uintptr(duplicate), "candidate-output")
	if directory == nil {
		_ = unix.Close(duplicate)
		return errors.New("cannot inspect output-dir")
	}
	names, readErr := directory.Readdirnames(1)
	closeErr := directory.Close()
	if readErr == nil || len(names) != 0 {
		return errors.Join(errors.New("output-dir must be empty"), closeErr)
	}
	if !errors.Is(readErr, io.EOF) {
		return errors.Join(readErr, closeErr)
	}
	return closeErr
}

func (root *privateOutputRoot) writeNewFile(name string, content []byte) error {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name {
		return errors.New("invalid output filename")
	}
	fd, err := unix.Openat(root.fd, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		_ = unix.Unlinkat(root.fd, name, 0)
		return errors.New("cannot wrap private output file")
	}

	writeErr := error(nil)
	var stat unix.Stat_t
	if err := unix.Fchmod(fd, 0o600); err != nil {
		writeErr = err
	} else if err := unix.Fstat(fd, &stat); err != nil ||
		stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o777 != 0o600 ||
		stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		writeErr = errors.New("private output file metadata is invalid")
	} else if written, err := file.Write(content); err != nil {
		writeErr = err
	} else if written != len(content) {
		writeErr = io.ErrShortWrite
	} else if err := file.Sync(); err != nil {
		writeErr = err
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		unlinkErr := unix.Unlinkat(root.fd, name, 0)
		return errors.Join(writeErr, closeErr, unlinkErr)
	}
	return nil
}

func (root *privateOutputRoot) close() error {
	if root == nil || root.fd < 0 {
		return nil
	}
	err := unix.Close(root.fd)
	root.fd = -1
	return err
}
