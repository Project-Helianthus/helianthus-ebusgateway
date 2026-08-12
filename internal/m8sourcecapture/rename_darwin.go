//go:build darwin

package m8sourcecapture

import "golang.org/x/sys/unix"

func renameNoReplaceAt(directoryFD int, source, destination string) error {
	return unix.RenameatxNp(directoryFD, source, directoryFD, destination, unix.RENAME_EXCL)
}
