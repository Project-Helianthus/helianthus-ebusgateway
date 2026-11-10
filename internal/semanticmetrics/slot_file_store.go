package semanticmetrics

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

var (
	ErrSlotFileClosed    = errors.New("slot file store closed")
	ErrSlotFileFencing   = errors.New("slot file store fencing lost")
	ErrSlotFileUncertain = errors.New("slot file store commit uncertain; close and reopen required")
)

func slotFileName(target string) (string, error) {
	if target == "" || len(target) > 256 || !utf8.ValidString(target) || target == "." || target == ".." || strings.ContainsAny(target, "/\\") {
		return "", fmt.Errorf("invalid slot file target")
	}
	for _, c := range target {
		if unicode.IsControl(c) {
			return "", fmt.Errorf("invalid slot file target")
		}
	}
	return fmt.Sprintf("slot-%x", sha256.Sum256([]byte(target))), nil
}
