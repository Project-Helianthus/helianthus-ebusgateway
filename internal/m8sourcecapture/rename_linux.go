//go:build linux

package m8sourcecapture

import "golang.org/x/sys/unix"

func renameNoReplaceAt(directoryFD int, source, destination string) error {
	return unix.Renameat2(directoryFD, source, directoryFD, destination, unix.RENAME_NOREPLACE)
}
