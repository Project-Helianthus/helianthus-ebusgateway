package mcp

import (
	"fmt"
	"math"

	rpcsource "github.com/Project-Helianthus/helianthus-ebusgateway/internal/rpc_source"
)

// enforceRPCSourceParam is the rpc.invoke call-site guard enforcing the
// MEMORY.md invariant "All rpc.invoke MUST use source: 113". If params
// carries an explicit "source", it MUST be 113 (0x71). If absent (or if
// the params map itself is nil), the canonical value is injected so
// downstream transport layers cannot silently use a different initiator.
//
// Note: a nil input map cannot be mutated, so nil is treated as "no
// params provided and no way to attach a source" — callers that accept
// absent-params must use enforceRPCSourceOnArgs which operates on the
// enclosing args envelope and can create a fresh params map.
//
// The guard wraps rpc_source.ErrNon113Source; callers classify via
// errors.Is(err, rpc_source.ErrNon113Source).
func enforceRPCSourceParam(params map[string]any) error {
	if params == nil {
		return nil
	}
	raw, ok := params["source"]
	if !ok {
		params["source"] = int(rpcsource.Gateway)
		return nil
	}
	src, err := toByteSource(raw)
	if err != nil {
		return fmt.Errorf("params.source: %w", err)
	}
	if err := rpcsource.Enforce(src); err != nil {
		return err
	}
	return nil
}

// enforceRPCSourceOnArgs enforces the source=113 invariant on the
// enclosing rpc.invoke args envelope. Unlike enforceRPCSourceParam it
// handles the case where args["params"] is absent, nil, or not a map —
// in which case a fresh params map is materialised with
// source=rpc_source.Gateway, so the invariant holds regardless of input
// shape.
//
// Returns the resolved params map (never nil on success) so callers can
// use the injected value directly instead of re-reading args["params"].
func enforceRPCSourceOnArgs(args map[string]any) (map[string]any, error) {
	if args == nil {
		// No envelope to attach params to; caller must handle.
		return nil, nil
	}
	params, _ := args["params"].(map[string]any)
	if params == nil {
		params = map[string]any{"source": int(rpcsource.Gateway)}
		args["params"] = params
		return params, nil
	}
	if err := enforceRPCSourceParam(params); err != nil {
		return params, err
	}
	return params, nil
}

// toByteSource coerces a JSON-decoded value into an 8-bit source byte.
//
// It returns a non-nil error in two distinguishable cases — both wrap
// rpc_source.ErrNon113Source so the outer classifier (classifyToolError)
// still maps them to INVALID_ARGUMENT:
//
//   - unsupported type (e.g. bool, string, map, slice, nil): the caller
//     passed a JSON type the invariant cannot be applied to.
//   - invalid value (NaN, Inf, fractional, out-of-range): the type is
//     numeric but the value is not a whole byte in [0,255].
//
// The offending value is included in the message so the "source=300"
// case reports "invalid value" rather than being misreported as
// "unsupported type".
func toByteSource(v any) (byte, error) {
	switch x := v.(type) {
	case int:
		if x < 0 || x > 255 {
			return 0, fmt.Errorf("invalid value %d (int out of byte range [0,255]): %w", x, rpcsource.ErrNon113Source)
		}
		return byte(x), nil
	case int64:
		if x < 0 || x > 255 {
			return 0, fmt.Errorf("invalid value %d (int64 out of byte range [0,255]): %w", x, rpcsource.ErrNon113Source)
		}
		return byte(x), nil
	case float64:
		// The underlying transport emits an 8-bit source byte;
		// byte(113.9) silently truncates to 113 and would bypass the
		// Enforce check for any near-match fractional value. Only
		// accept whole-number float64s in range.
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, fmt.Errorf("invalid value %v (NaN or Inf): %w", x, rpcsource.ErrNon113Source)
		}
		if math.Trunc(x) != x {
			return 0, fmt.Errorf("invalid value %v (fractional float64, expected whole number): %w", x, rpcsource.ErrNon113Source)
		}
		if x < 0 || x > 255 {
			return 0, fmt.Errorf("invalid value %v (float64 out of byte range [0,255]): %w", x, rpcsource.ErrNon113Source)
		}
		return byte(x), nil
	}
	return 0, fmt.Errorf("unsupported type %T: %w", v, rpcsource.ErrNon113Source)
}
