// Package rpc_source centralises the gateway's RPC initiator source byte.
//
// Per project invariant (MEMORY.md): every gateway rpc.invoke call MUST use
// source=113 (0x71). Non-113 sources are a programming error and MUST
// surface as compile-time OR sentinel-error failures, not silent traffic
// under the wrong initiator address.
package rpc_source

import (
	"errors"
	"fmt"
)

// Gateway is the gateway's fixed initiator source byte. Compile-time
// constant — changing it is a locked-plan decision, not a code change.
const Gateway byte = 0x71

// ErrNon113Source is the sentinel returned when an RPC-invoke call site
// attempts to use a source byte other than Gateway (0x71).
var ErrNon113Source = errors.New("rpc_source: source byte is not gateway (0x71)")

// Enforce validates that the supplied source byte is the gateway source. It
// returns a non-nil error wrapping ErrNon113Source when src != Gateway.
//
// Callers SHOULD embed this check at every rpc.invoke call-site. The
// returned error carries the attempted source byte for audit purposes.
func Enforce(src byte) error {
	if src == Gateway {
		return nil
	}
	return fmt.Errorf("got 0x%02X, want 0x%02X: %w", src, Gateway, ErrNon113Source)
}

// Require panics when src != Gateway. Use in tests or startup-time
// invariant guards where a non-gateway source is unrecoverable.
func Require(src byte) {
	if err := Enforce(src); err != nil {
		panic(err)
	}
}
