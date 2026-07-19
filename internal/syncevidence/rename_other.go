//go:build !linux && !darwin

package syncevidence

import (
	"errors"
)

func renameNoReplace(_ int, source, destination string) error {
	return errors.New("no-replace rename unsupported")
}
