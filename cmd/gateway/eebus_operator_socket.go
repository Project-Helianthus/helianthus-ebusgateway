package main

import (
	"errors"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// eebusLegacyOperatorSocketName is the operator MCP socket that earlier
// gateway builds created inside the eeBUS state root. It is the only entry
// the startup cleanup ever considers.
const eebusLegacyOperatorSocketName = "operator-mcp.sock"

// eebusLegacyOperatorSocketDialTimeout bounds the stale-socket proof.
const eebusLegacyOperatorSocketDialTimeout = 100 * time.Millisecond

var errEEBusLegacyOperatorSocketChanged = errors.New("socket changed during the stale proof")

// eebusLegacyOperatorSocketOutcome is the result of the startup cleanup.
type eebusLegacyOperatorSocketOutcome string

const (
	eebusLegacyOperatorSocketAbsent         eebusLegacyOperatorSocketOutcome = "absent"
	eebusLegacyOperatorSocketRemoved        eebusLegacyOperatorSocketOutcome = "removed"
	eebusLegacyOperatorSocketNotOwnedSocket eebusLegacyOperatorSocketOutcome = "not_owned_socket"
	eebusLegacyOperatorSocketActive         eebusLegacyOperatorSocketOutcome = "active_listener"
	eebusLegacyOperatorSocketUnproven       eebusLegacyOperatorSocketOutcome = "unproven"
)

// clearLegacyEEBusOperatorSocket removes the operator MCP socket an earlier
// gateway build left inside the eeBUS state root. The eeBUS store accepts
// only its own entries there and rejects the whole layout otherwise, so this
// runs before any eeBUS resource opens. stateRoot is the validated root of an
// enabled eeBUS configuration (eebusEnabledStateRoot); empty does nothing.
// Only that exact name is considered, and only an owned socket without a
// listener is removed; every other entry, and anything else under that name,
// stays untouched. A non-absent outcome is logged once, naming the entry but
// no path.
func clearLegacyEEBusOperatorSocket(stateRoot string) eebusLegacyOperatorSocketOutcome {
	if stateRoot == "" {
		return eebusLegacyOperatorSocketAbsent
	}
	outcome, cause := removeStaleEEBusOperatorSocket(filepath.Join(stateRoot, eebusLegacyOperatorSocketName))
	switch outcome {
	case eebusLegacyOperatorSocketAbsent:
	case eebusLegacyOperatorSocketRemoved:
		log.Printf("eeBUS state root: removed stale %s left by an earlier gateway build", eebusLegacyOperatorSocketName)
	case eebusLegacyOperatorSocketUnproven:
		log.Printf("eeBUS state root: %s left in place reason=%s cause=%s", eebusLegacyOperatorSocketName, outcome, eebusLegacyOperatorSocketCause(cause))
	default:
		log.Printf("eeBUS state root: %s left in place reason=%s", eebusLegacyOperatorSocketName, outcome)
	}
	return outcome
}

// removeStaleEEBusOperatorSocket unlinks path only when it is a socket owned
// by this effective user that refuses connections and is unchanged after that
// proof.
func removeStaleEEBusOperatorSocket(path string) (eebusLegacyOperatorSocketOutcome, error) {
	before, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return eebusLegacyOperatorSocketAbsent, nil
	}
	if err != nil {
		return eebusLegacyOperatorSocketUnproven, err
	}
	if !eebusOwnedSocket(before) {
		return eebusLegacyOperatorSocketNotOwnedSocket, nil
	}
	connection, err := net.DialTimeout("unix", path, eebusLegacyOperatorSocketDialTimeout)
	if err == nil {
		_ = connection.Close()
		return eebusLegacyOperatorSocketActive, nil
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return eebusLegacyOperatorSocketUnproven, err
	}
	after, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return eebusLegacyOperatorSocketAbsent, nil
	}
	if err != nil {
		return eebusLegacyOperatorSocketUnproven, err
	}
	if !os.SameFile(before, after) || after.Mode() != before.Mode() || !after.ModTime().Equal(before.ModTime()) {
		return eebusLegacyOperatorSocketUnproven, errEEBusLegacyOperatorSocketChanged
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return eebusLegacyOperatorSocketUnproven, err
	}
	return eebusLegacyOperatorSocketRemoved, nil
}

func eebusOwnedSocket(info os.FileInfo) bool {
	if info.Mode().Type() != os.ModeSocket {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

// eebusLegacyOperatorSocketCause reduces a cleanup error to its operating
// system category, so no path reaches the log.
func eebusLegacyOperatorSocketCause(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno.Error()
	}
	if errors.Is(err, errEEBusLegacyOperatorSocketChanged) {
		return err.Error()
	}
	return "unavailable"
}
