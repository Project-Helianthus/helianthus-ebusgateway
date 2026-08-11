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

type privateOutput struct {
	name    string
	content []byte
}

func writePrivateOutputs(directory string, outputs []privateOutput) (result error) {
	root, err := openPrivateOutputRoot(directory)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, unix.Close(root)) }()
	for _, output := range outputs {
		if err := writeNewPrivateFile(root, output); err != nil {
			return err
		}
	}
	return unix.Fsync(root)
}

func openPrivateOutputRoot(path string) (int, error) {
	clean := filepath.Clean(path)
	start := "."
	if filepath.IsAbs(clean) {
		start = string(filepath.Separator)
		clean = strings.TrimPrefix(clean, string(filepath.Separator))
	}
	fd, err := unix.Open(start, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, err
	}
	for _, component := range strings.Split(clean, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		if component == ".." {
			_ = unix.Close(fd)
			return -1, errors.New("output-dir cannot contain parent traversal")
		}
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return -1, openErr
		}
		fd = next
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFDIR ||
		stat.Mode&0o777 != 0o700 || stat.Uid != uint32(os.Geteuid()) {
		_ = unix.Close(fd)
		return -1, errors.New("output-dir must be an owner-only directory with mode 0700")
	}
	if err := requireEmptyDirectory(fd); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func requireEmptyDirectory(fd int) error {
	duplicate, err := unix.Dup(fd)
	if err != nil {
		return err
	}
	directory := os.NewFile(uintptr(duplicate), "leaf-promotion-output")
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

func writeNewPrivateFile(root int, output privateOutput) error {
	if output.name == "" || output.name == "." || output.name == ".." || filepath.Base(output.name) != output.name {
		return errors.New("invalid output filename")
	}
	fd, err := unix.Openat(root, output.name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), output.name)
	if file == nil {
		_ = unix.Close(fd)
		_ = unix.Unlinkat(root, output.name, 0)
		return errors.New("cannot wrap private output file")
	}
	var writeErr error
	var stat unix.Stat_t
	if err := unix.Fchmod(fd, 0o600); err != nil {
		writeErr = err
	} else if err := unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG ||
		stat.Mode&0o777 != 0o600 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		writeErr = errors.New("private output file metadata is invalid")
	} else if written, err := file.Write(output.content); err != nil {
		writeErr = err
	} else if written != len(output.content) {
		writeErr = io.ErrShortWrite
	} else if err := file.Sync(); err != nil {
		writeErr = err
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		unlinkErr := unix.Unlinkat(root, output.name, 0)
		return errors.Join(writeErr, closeErr, unlinkErr)
	}
	return nil
}
