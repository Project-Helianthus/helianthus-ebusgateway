package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusadmin"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// Location of the eeBUS operator MCP socket.
//
// The socket never lives inside the eeBUS state root, whose store rejects any
// entry it does not own. The rules, in order:
//
//   - -eebus-operator-socket-dir names the directory. It must be absolute,
//     outside the state root, and short enough that <dir>/operator-mcp.sock
//     fits sockaddr_un. A configured directory that is invalid or cannot be
//     used fails the gateway start.
//   - Without the flag the socket is <state-root>-operator/operator-mcp.sock
//     (the add-on: /data/eebus-operator/operator-mcp.sock). When that path is
//     too long to dial, or neither that directory nor its nearest existing
//     ancestor lets this user create it, the socket moves to a bounded
//     location instead: <temp dir>/helianthus-eebus-op-<uid>-<id>/operator-mcp.sock,
//     where <id> is the first 8 hex digits of the SHA-256 of the default path,
//     so every default location has its own directory (under /tmp when the
//     temp directory leaves no room). The fallback is logged once with its
//     reason and no path.
//   - When the default location, or its fallback, cannot be served the gateway
//     keeps running without the operator endpoint: the failure is logged with
//     its category only and the eeBUS readiness reports it as degraded.

// eebusOperatorFallbackReason is why the default socket location was replaced.
type eebusOperatorFallbackReason string

const (
	eebusOperatorFallbackPathTooLong       eebusOperatorFallbackReason = "path_too_long"
	eebusOperatorFallbackParentNotWritable eebusOperatorFallbackReason = "parent_not_writable"
)

// eebusOperatorFallbackPrefix starts the per-instance fallback directory name.
const eebusOperatorFallbackPrefix = "helianthus-eebus-op-"

// eebusOperatorSocketLocation is a resolved operator socket location.
type eebusOperatorSocketLocation struct {
	// Path is the socket path; empty when no location could be resolved.
	Path string
	// Explicit reports a directory the operator configured.
	Explicit bool
	// Fallback is why the default location was replaced; empty otherwise.
	Fallback eebusOperatorFallbackReason
}

// The operating-system probes of the location rules are seams, so tests choose
// the temp directory and the writability of the parent without a second user.
var (
	eebusOperatorTempDir            = os.TempDir
	eebusOperatorDirectoryCreatable = eebusDirectoryCreatable
)

// eebusOperatorEndpointDown is set while the operator MCP endpoint could not be
// served at its default location and the gateway runs without it.
var eebusOperatorEndpointDown atomic.Bool

// validateEEBusOperatorSocketDir validates an operator-configured socket
// directory and returns it cleaned. The socket path it implies must fit
// sockaddr_un; the error carries lengths only.
func validateEEBusOperatorSocketDir(value string) (string, error) {
	const name = "eebus-operator-socket-dir"
	input := strings.TrimSpace(value)
	if strings.ContainsRune(input, '\x00') {
		return "", fmt.Errorf("invalid %s: path contains NUL", name)
	}
	for _, segment := range strings.Split(input, string(filepath.Separator)) {
		if segment == ".." {
			return "", fmt.Errorf("invalid %s: path must not contain traversal", name)
		}
	}
	dir := filepath.Clean(input)
	if !filepath.IsAbs(dir) {
		return "", fmt.Errorf("invalid %s: path must be absolute", name)
	}
	if dir == filepath.Dir(dir) {
		return "", fmt.Errorf("invalid %s: path must not be the filesystem root", name)
	}
	if err := mcp.ValidateEEBusV1OperatorSocketPath(filepath.Join(dir, mcp.EEBusV1OperatorSocketName)); err != nil {
		return "", fmt.Errorf("invalid %s: %w", name, err)
	}
	return dir, nil
}

// resolveEEBusOperatorSocket applies the location rules. stateRoot is the
// validated eeBUS state root ("" when none is configured) and explicitDir the
// configured socket directory ("" for the default). Only a configured
// directory can be rejected; the location is returned with the error so the
// caller can tell it from a default.
func resolveEEBusOperatorSocket(stateRoot, explicitDir string) (eebusOperatorSocketLocation, error) {
	if explicitDir != "" {
		location := eebusOperatorSocketLocation{Explicit: true}
		dir, err := validateEEBusOperatorSocketDir(explicitDir)
		if err != nil {
			return location, err
		}
		if eebusPathWithin(dir, stateRoot) {
			return location, errors.New("invalid eebus-operator-socket-dir: the directory must be outside the eeBUS state root")
		}
		location.Path = filepath.Join(dir, mcp.EEBusV1OperatorSocketName)
		return location, nil
	}

	derived := mcp.EEBusV1OperatorSocketPath(stateRoot)
	var reason eebusOperatorFallbackReason
	switch {
	case mcp.ValidateEEBusV1OperatorSocketPath(derived) != nil:
		reason = eebusOperatorFallbackPathTooLong
	case eebusPathWithin(derived, stateRoot) || !eebusOperatorDirectoryCreatable(filepath.Dir(derived)):
		reason = eebusOperatorFallbackParentNotWritable
	default:
		return eebusOperatorSocketLocation{Path: derived}, nil
	}
	fallback := eebusOperatorFallbackSocket(derived, stateRoot)
	if eebusPathWithin(fallback, stateRoot) {
		// Even /tmp lies inside the state root: serving here would add an entry
		// to the store root, so the endpoint is not served at all.
		return eebusOperatorSocketLocation{Fallback: reason}, errors.New("no operator socket location outside the eeBUS state root")
	}
	return eebusOperatorSocketLocation{Path: fallback, Fallback: reason}, nil
}

// eebusOperatorFallbackSocket returns the bounded fallback socket path for a
// default location: a per-instance directory under the temp directory, or
// under /tmp when the temp directory leaves no room for it. The /tmp path is at
// most 66 bytes, so the result always fits sockaddr_un.
func eebusOperatorFallbackSocket(derived, stateRoot string) string {
	digest := sha256.Sum256([]byte(derived))
	name := fmt.Sprintf("%s%d-%s", eebusOperatorFallbackPrefix, os.Geteuid(), hex.EncodeToString(digest[:4]))
	path := filepath.Join(eebusOperatorTempDir(), name, mcp.EEBusV1OperatorSocketName)
	if !filepath.IsAbs(path) || mcp.ValidateEEBusV1OperatorSocketPath(path) != nil || eebusPathWithin(path, stateRoot) {
		path = filepath.Join("/tmp", name, mcp.EEBusV1OperatorSocketName)
	}
	return path
}

// eebusPathWithin reports whether path is root or lies inside it after resolving
// aliases. An unresolved identity fails closed rather than risking a store entry.
func eebusPathWithin(path, root string) bool {
	if root == "" {
		return false
	}
	resolvedRoot, err := eebusResolveExistingAncestor(root)
	if err != nil {
		return true
	}
	resolvedPath, err := eebusResolveExistingAncestor(path)
	if err != nil {
		return true
	}
	return resolvedPath == resolvedRoot || strings.HasPrefix(resolvedPath, resolvedRoot+string(filepath.Separator))
}

// eebusResolveExistingAncestor resolves links even when the final directories
// have not been created yet. Lstat distinguishes a missing suffix from a dangling
// link, which must fail closed instead of being treated as a new directory.
func eebusResolveExistingAncestor(path string) (string, error) {
	current := filepath.Clean(path)
	var suffix []string
	for {
		if _, err := os.Lstat(current); err == nil {
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return "", err
			}
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return resolved, nil
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fs.ErrNotExist
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}

// eebusDirectoryCreatable reports whether dir is a directory already, or can
// be created: its nearest existing ancestor is a directory (not a link, which
// the operator transport refuses to follow) this user can write and search.
// Every ancestor above that one must be a plain directory too, whether dir
// exists or not: the listener opens each component without following links, so
// an existing dir beneath a linked ancestor is rejected there and must be here.
func eebusDirectoryCreatable(dir string) bool {
	dir = filepath.Clean(dir)
	found := false
	for current := dir; ; {
		info, err := os.Lstat(current)
		switch {
		case err == nil:
			// Lstat refuses a link only in the last element, so each ancestor
			// is examined on its own.
			if !info.IsDir() {
				return false
			}
			if !found {
				found = true
				if current == dir {
					// An existing socket directory must belong to this user and be
					// usable, as the listener requires.
					st, ok := info.Sys().(*syscall.Stat_t)
					if !ok || int(st.Uid) != os.Geteuid() || unix.Access(current, unix.R_OK|unix.W_OK|unix.X_OK) != nil {
						return false
					}
				} else if unix.Access(current, unix.W_OK|unix.X_OK) != nil {
					return false
				}
			}
		case found || !errors.Is(err, fs.ErrNotExist):
			return false
		}
		parent := filepath.Dir(current)
		if parent == current {
			return found
		}
		current = parent
	}
}

// openEEBusOperatorEndpoint serves the operator MCP endpoint at its resolved
// location. It fails only for a directory the operator configured; at the
// default location an endpoint that cannot be served is logged and recorded,
// and the gateway continues without it (nil, nil).
func openEEBusOperatorEndpoint(ctx context.Context, server *mcp.Server, stateRoot, explicitDir string) (io.Closer, error) {
	location, err := resolveEEBusOperatorSocket(stateRoot, explicitDir)
	if err == nil {
		if location.Fallback != "" {
			log.Printf("eeBUS operator MCP socket: default location unusable reason=%s; using a bounded location in the temp directory", location.Fallback)
		}
		var endpoint io.Closer
		if endpoint, err = startEEBusOperatorEndpoint(server, ctx, location.Path); err == nil {
			eebusOperatorEndpointDown.Store(false)
			return endpoint, nil
		}
	}
	if location.Explicit {
		return nil, err
	}
	eebusOperatorEndpointDown.Store(true)
	log.Printf("eeBUS operator MCP endpoint unavailable cause=%s; the gateway continues without it", eebusOperatorEndpointCause(err))
	return nil, nil
}

// eebusOperatorEndpointCause reduces an endpoint error to its operating system
// category, so no path reaches the log.
func eebusOperatorEndpointCause(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno.Error()
	}
	return "unavailable"
}

// eebusReadinessWithOperatorEndpoint degrades a ready eeBUS readiness while
// the gateway runs without its operator MCP endpoint, the owner-only
// administration channel of the eeBUS runtime.
func eebusReadinessWithOperatorEndpoint(readiness eebusadmin.ReadinessV1) eebusadmin.ReadinessV1 {
	if readiness.EEBusReadiness == eebusadmin.EEBusReadinessReady && eebusOperatorEndpointDown.Load() {
		readiness.EEBusReadiness = eebusadmin.EEBusReadinessDegraded
		readiness.EEBusDegradedReason = eebusadmin.EEBusDegradedReasonAdminBoundaryUnavailable
	}
	return readiness
}
