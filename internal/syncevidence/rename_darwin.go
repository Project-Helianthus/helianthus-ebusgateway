//go:build darwin

package syncevidence

import "golang.org/x/sys/unix"

func renameNoReplace(directoryFD int, source, destination string) error {
	return unix.RenameatxNp(directoryFD, source, directoryFD, destination, unix.RENAME_EXCL)
}
