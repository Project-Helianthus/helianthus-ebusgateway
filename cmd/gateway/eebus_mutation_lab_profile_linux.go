//go:build linux

package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const eebusMutationLabProfileMaximumBytes = 65536

type eebusMutationLabFileIdentity struct {
	device       uint64
	inode        uint64
	mode         uint32
	uid          uint32
	linkCount    uint64
	size         int64
	changeSec    int64
	changeNsec   int64
	modifiedSec  int64
	modifiedNsec int64
}

func readEEBusMutationLabProfileFile(stateRoot string) ([]byte, bool, error) {
	rootFD, present, err := openEEBusMutationLabStateRoot(stateRoot)
	if err != nil || !present {
		return nil, false, err
	}
	defer func() { _ = unix.Close(rootFD) }()

	fd, err := unix.Openat(
		rootFD,
		eebusMutationLabProfileBasename,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if errors.Is(err, unix.ENOENT) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errEEBusMutationLabProfileLoad
	}
	file := os.NewFile(uintptr(fd), eebusMutationLabProfileBasename)
	if file == nil {
		_ = unix.Close(fd)
		return nil, false, errEEBusMutationLabProfileLoad
	}

	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil ||
		!mutationLabFileMetadataValid(
			before.Mode,
			before.Uid,
			uint64(before.Nlink),
			before.Size,
			uint32(os.Geteuid()),
		) {
		_ = file.Close()
		return nil, false, errEEBusMutationLabProfileLoad
	}
	beforeIdentity := mutationLabFileIdentity(before)

	raw, readErr := io.ReadAll(io.LimitReader(file, eebusMutationLabProfileMaximumBytes+1))
	var after unix.Stat_t
	statErr := unix.Fstat(fd, &after)
	closeErr := file.Close()
	if readErr != nil || statErr != nil || closeErr != nil ||
		len(raw) == 0 || len(raw) > eebusMutationLabProfileMaximumBytes ||
		int64(len(raw)) != before.Size ||
		beforeIdentity != mutationLabFileIdentity(after) {
		clear(raw)
		return nil, false, errEEBusMutationLabProfileLoad
	}
	return raw, true, nil
}

func openEEBusMutationLabStateRoot(stateRoot string) (int, bool, error) {
	if stateRoot == "" || !filepath.IsAbs(stateRoot) ||
		filepath.Clean(stateRoot) != stateRoot ||
		stateRoot == string(filepath.Separator) {
		return -1, false, errEEBusMutationLabProfileLoad
	}

	current, err := unix.Open(
		string(filepath.Separator),
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return -1, false, errEEBusMutationLabProfileLoad
	}
	components := strings.Split(
		strings.TrimPrefix(stateRoot, string(filepath.Separator)),
		string(filepath.Separator),
	)
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			_ = unix.Close(current)
			return -1, false, errEEBusMutationLabProfileLoad
		}
		next, openErr := unix.Openat(
			current,
			component,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
			0,
		)
		_ = unix.Close(current)
		if errors.Is(openErr, unix.ENOENT) {
			return -1, false, nil
		}
		if openErr != nil {
			return -1, false, errEEBusMutationLabProfileLoad
		}
		current = next
	}

	var stat unix.Stat_t
	if err := unix.Fstat(current, &stat); err != nil ||
		!mutationLabRootMetadataValid(stat.Mode, stat.Uid, uint32(os.Geteuid())) {
		_ = unix.Close(current)
		return -1, false, errEEBusMutationLabProfileLoad
	}
	return current, true, nil
}

func mutationLabRootMetadataValid(mode, uid, euid uint32) bool {
	return mode&unix.S_IFMT == unix.S_IFDIR &&
		mode&0o7777 == 0o700 &&
		uid == euid
}

func mutationLabFileMetadataValid(
	mode uint32,
	uid uint32,
	linkCount uint64,
	size int64,
	euid uint32,
) bool {
	return mode&unix.S_IFMT == unix.S_IFREG &&
		mode&0o7777 == 0o600 &&
		uid == euid &&
		linkCount == 1 &&
		size >= 1 &&
		size <= eebusMutationLabProfileMaximumBytes
}

func mutationLabFileIdentity(stat unix.Stat_t) eebusMutationLabFileIdentity {
	return eebusMutationLabFileIdentity{
		device:       uint64(stat.Dev),
		inode:        stat.Ino,
		mode:         stat.Mode,
		uid:          stat.Uid,
		linkCount:    uint64(stat.Nlink),
		size:         stat.Size,
		changeSec:    stat.Ctim.Sec,
		changeNsec:   stat.Ctim.Nsec,
		modifiedSec:  stat.Mtim.Sec,
		modifiedNsec: stat.Mtim.Nsec,
	}
}
