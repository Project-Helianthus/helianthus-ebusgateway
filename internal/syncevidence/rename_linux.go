//go:build linux

package syncevidence

import "golang.org/x/sys/unix"

func renameNoReplace(directoryFD int, source, destination string) error {
	return unix.Renameat2(directoryFD, source, directoryFD, destination, unix.RENAME_NOREPLACE)
}
