package mcp

import (
	"fmt"

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
	src, ok := toByteSource(raw)
	if !ok {
		return fmt.Errorf("params.source has unsupported type %T: %w", raw, rpcsource.ErrNon113Source)
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

func toByteSource(v any) (byte, bool) {
	switch x := v.(type) {
	case int:
		if x < 0 || x > 255 {
			return 0, false
		}
		return byte(x), true
	case int64:
		if x < 0 || x > 255 {
			return 0, false
		}
		return byte(x), true
	case float64:
		if x < 0 || x > 255 {
			return 0, false
		}
		return byte(x), true
	}
	return 0, false
}
