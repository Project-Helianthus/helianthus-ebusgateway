package mcp

import (
	"fmt"

	rpcsource "github.com/Project-Helianthus/helianthus-ebusgateway/internal/rpc_source"
)

// enforceRPCSourceParam is the rpc.invoke call-site guard enforcing the
// MEMORY.md invariant "All rpc.invoke MUST use source: 113". If params
// carries an explicit "source", it MUST be 113 (0x71). If absent, the
// canonical value is injected so downstream transport layers cannot
// silently use a different initiator.
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
