// Package rpc_source contains shared validation for caller-supplied RPC source
// bytes.
//
// Gateway-owned callers must pass the admitted source selected during startup.
// This package intentionally does not define a fixed gateway initiator address.
package rpc_source

import (
	"errors"
	"fmt"
)

// ErrInvalidSource is the sentinel returned when an RPC-invoke call site
// attempts to use an invalid source byte.
var ErrInvalidSource = errors.New("rpc_source: source byte is invalid")

// Enforce validates that the supplied source byte is usable as an active
// initiator source. It returns a non-nil error wrapping ErrInvalidSource for
// source zero, which is not a valid admitted active source.
//
// Callers SHOULD embed this check at rpc.invoke call-sites that receive a
// source from startup admission or an explicit user override. The returned
// error carries the attempted source byte for audit purposes.
func Enforce(src byte) error {
	if src == 0 {
		return fmt.Errorf("got 0x%02X: %w", src, ErrInvalidSource)
	}
	return nil
}

// Require panics when src is not a valid active source. Use in tests or
// startup-time invariant guards where an invalid source is unrecoverable.
func Require(src byte) {
	if err := Enforce(src); err != nil {
		panic(err)
	}
}
