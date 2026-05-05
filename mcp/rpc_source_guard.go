package mcp

import (
	"errors"
	"fmt"
	"math"

	rpcsource "github.com/Project-Helianthus/helianthus-ebusgateway/internal/rpc_source"
)

var errSourceSelectionNotActive = errors.New("source selection not active")

// enforceRPCSourceParam is the rpc.invoke call-site guard enforcing the
// admitted-source invariant. If params omits "source", the startup-admitted
// source is injected. If params carries an explicit "source", it is treated as
// a user-requested override and only validated as a byte-shaped source.
//
// Note: a nil input map cannot be mutated, so nil is treated as "no
// params provided and no way to attach a source" — callers that accept
// absent-params must use enforceRPCSourceOnArgs which operates on the
// enclosing args envelope and can create a fresh params map.
//
// Invalid explicit source values wrap rpc_source.ErrInvalidSource.
func enforceRPCSourceParam(params map[string]any, admittedSource byte, admitted bool) error {
	if params == nil {
		return nil
	}
	raw, ok := params["source"]
	if !ok {
		if !admitted || admittedSource == 0 {
			return errSourceSelectionNotActive
		}
		params["source"] = int(admittedSource)
		return nil
	}
	source, err := toByteSource(raw)
	if err != nil {
		return fmt.Errorf("params.source: %w", err)
	}
	if err := rpcsource.Enforce(source); err != nil {
		return fmt.Errorf("params.source: %w", err)
	}
	params["source"] = int(source)
	return nil
}

// enforceRPCSourceOnArgs enforces the admitted-source invariant on the
// enclosing rpc.invoke args envelope. Unlike enforceRPCSourceParam it
// handles the case where args["params"] is absent, nil, or not a map —
// in which case a fresh params map is materialised with the admitted source.
//
// Returns the resolved params map (never nil on success) so callers can
// use the injected value directly instead of re-reading args["params"].
func enforceRPCSourceOnArgs(args map[string]any, admittedSource byte, admitted bool) (map[string]any, error) {
	if args == nil {
		// No envelope to attach params to; caller must handle.
		return nil, nil
	}
	params, _ := args["params"].(map[string]any)
	if params == nil {
		if !admitted || admittedSource == 0 {
			return nil, errSourceSelectionNotActive
		}
		params = map[string]any{"source": int(admittedSource)}
		args["params"] = params
		return params, nil
	}
	if err := enforceRPCSourceParam(params, admittedSource, admitted); err != nil {
		return params, err
	}
	return params, nil
}

// toByteSource coerces a JSON-decoded value into an 8-bit source byte.
//
// It returns a non-nil error in two distinguishable cases — both wrap
// rpc_source.ErrInvalidSource so the outer classifier (classifyToolError)
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
	case byte:
		// byte is an alias for uint8, always in [0,255] by type; no
		// range check needed for in-process callers that build params maps
		// programmatically.
		return x, nil
	case int:
		if x < 0 || x > 255 {
			return 0, fmt.Errorf("invalid value %d (int out of byte range [0,255]): %w", x, rpcsource.ErrInvalidSource)
		}
		return byte(x), nil
	case int64:
		if x < 0 || x > 255 {
			return 0, fmt.Errorf("invalid value %d (int64 out of byte range [0,255]): %w", x, rpcsource.ErrInvalidSource)
		}
		return byte(x), nil
	case float64:
		// The underlying transport emits an 8-bit source byte;
		// byte(127.9) silently truncates to 127. Only accept
		// whole-number float64s in range.
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, fmt.Errorf("invalid value %v (NaN or Inf): %w", x, rpcsource.ErrInvalidSource)
		}
		if math.Trunc(x) != x {
			return 0, fmt.Errorf("invalid value %v (fractional float64, expected whole number): %w", x, rpcsource.ErrInvalidSource)
		}
		if x < 0 || x > 255 {
			return 0, fmt.Errorf("invalid value %v (float64 out of byte range [0,255]): %w", x, rpcsource.ErrInvalidSource)
		}
		return byte(x), nil
	}
	return 0, fmt.Errorf("unsupported type %T: %w", v, rpcsource.ErrInvalidSource)
}
