//go:build !linux

package main

import (
	"errors"
	"os"
	"path/filepath"
)

func readEEBusMutationLabProfileFile(stateRoot string) ([]byte, bool, error) {
	root, err := os.Lstat(stateRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil || root.Mode()&os.ModeSymlink != 0 || !root.IsDir() {
		return nil, false, errEEBusMutationLabProfileLoad
	}
	_, err = os.Lstat(filepath.Join(stateRoot, eebusMutationLabProfileBasename))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	return nil, false, errEEBusMutationLabProfileLoad
}
