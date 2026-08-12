//go:build !darwin && !linux

package m8sourcecapture

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Private source capture requires descriptor-relative no-follow filesystem
// operations. Unsupported platforms fail closed instead of using path-based IO.
type secureDirectory struct{}

func openSecureDirectory(string, bool) (*secureDirectory, error) {
	return nil, fmt.Errorf("%w: descriptor-anchored filesystem unsupported", errInvalidCapture)
}

func (*secureDirectory) close() error { return nil }

func (*secureDirectory) entries() ([]os.DirEntry, error) {
	return nil, fmt.Errorf("%w: descriptor-anchored filesystem unsupported", errInvalidCapture)
}

func (*secureDirectory) readRegular(string, int64) ([]byte, error) {
	return nil, fmt.Errorf("%w: descriptor-anchored filesystem unsupported", errInvalidCapture)
}

func (*secureDirectory) childDirectory(string, bool) (*secureDirectory, error) {
	return nil, fmt.Errorf("%w: descriptor-anchored filesystem unsupported", errInvalidCapture)
}

func (*secureDirectory) temporaryDirectory() (string, *secureDirectory, error) {
	return "", nil, fmt.Errorf("%w: descriptor-anchored filesystem unsupported", errInvalidCapture)
}

func (*secureDirectory) writeRegular(string, []byte) error {
	return fmt.Errorf("%w: descriptor-anchored filesystem unsupported", errInvalidCapture)
}

func (*secureDirectory) sync() error {
	return fmt.Errorf("%w: descriptor-anchored filesystem unsupported", errInvalidCapture)
}

func (*secureDirectory) absent(string) error {
	return fmt.Errorf("%w: descriptor-anchored filesystem unsupported", errInvalidCapture)
}

func (*secureDirectory) renameNoReplace(string, string) error {
	return fmt.Errorf("%w: descriptor-anchored filesystem unsupported", errInvalidCapture)
}

func (*secureDirectory) removeTree(string) error { return nil }

func safeChildName(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name && !strings.Contains(name, string(filepath.Separator))
}
