// Package execution_policy is the single shared execution-policy module for
// the ebus_standard L7 surface. It is consulted by:
//
//  1. The gateway MCP rpc.invoke path.
//  2. Direct provider invocation from runtime callers (e.g. NM runtime).
//
// Per canonical plan §10 and architecture/ebus_standard/05-execution-safety.md:
//
//   - Default-deny mutating/destructive/broadcast/memory_write.
//   - Accept read_only_safe and read_only_bus_load for any caller.
//   - Accept the compile-time 14-tuple whitelist only for
//     caller_context=system_nm_runtime.
//
// This module NEVER widens the accept set at runtime. Configuration,
// environment variables, and registry state MUST NOT affect its decisions.
package execution_policy

import (
	"errors"
	"fmt"

	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

// CallerContext identifies the call site for policy decisions.
type CallerContext string

// Known caller contexts.
const (
	CallerUserFacing      CallerContext = "user_facing"
	CallerSystemNMRuntime CallerContext = "system_nm_runtime"
)

// ErrSafetyClassDenied is the classification sentinel returned when a
// caller's combination of (safety_class, caller_context) resolves to a
// denial. Callers MUST use errors.Is(err, ErrSafetyClassDenied) and MUST
// NOT compare by pointer or string.
//
// This value wraps ebusstd.ErrSafetyClassDenied so parity tests comparing
// against the provider-level sentinel also succeed via errors.Is.
var ErrSafetyClassDenied = fmt.Errorf("execution_policy: %w", ebusstd.ErrSafetyClassDenied)

// Check evaluates the policy for (cmd, caller). It returns nil when the
// call is permitted, otherwise a non-nil error that satisfies
// errors.Is(err, ErrSafetyClassDenied) == true and carries the dynamic
// audit context (catalog method id, caller context, safety class).
func Check(cmd ebusstd.Command, caller CallerContext) error {
	switch cmd.SafetyClass {
	case ebusstd.SafetyReadOnlySafe, ebusstd.SafetyReadOnlyBusLoad:
		return nil
	}

	if caller == CallerSystemNMRuntime && nmWhitelistContains(cmd.Identity) {
		return nil
	}

	return fmt.Errorf(
		"method=%q caller=%q class=%s: %w",
		cmd.ID, caller, cmd.SafetyClass, ErrSafetyClassDenied,
	)
}

// IsDenied reports whether err is a safety-class denial.
func IsDenied(err error) bool {
	return errors.Is(err, ErrSafetyClassDenied)
}
