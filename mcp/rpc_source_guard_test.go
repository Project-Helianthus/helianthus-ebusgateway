package mcp

import (
	"errors"
	"testing"

	rpcsource "github.com/Project-Helianthus/helianthus-ebusgateway/internal/rpc_source"
)

func TestEnforceRPCSourceParam_InjectsGatewayWhenMissing(t *testing.T) {
	params := map[string]any{}
	if err := enforceRPCSourceParam(params); err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	src, ok := params["source"]
	if !ok {
		t.Fatal("expected injected source key")
	}
	if src != int(rpcsource.Gateway) {
		t.Fatalf("injected source = %v, want 0x71", src)
	}
}

func TestEnforceRPCSourceParam_AcceptsExplicit113(t *testing.T) {
	for _, v := range []any{int(0x71), float64(0x71), int64(113)} {
		params := map[string]any{"source": v}
		if err := enforceRPCSourceParam(params); err != nil {
			t.Fatalf("source=%v: %v", v, err)
		}
	}
}

func TestEnforceRPCSourceParam_RejectsNon113(t *testing.T) {
	for _, v := range []any{int(0x03), int(0x08), float64(0x26), 0xFF} {
		params := map[string]any{"source": v}
		err := enforceRPCSourceParam(params)
		if err == nil {
			t.Fatalf("source=%v must be rejected", v)
		}
		if !errors.Is(err, rpcsource.ErrNon113Source) {
			t.Fatalf("source=%v: want errors.Is ErrNon113Source, got %v", v, err)
		}
	}
}

func TestEnforceRPCSourceParam_RejectsUnsupportedType(t *testing.T) {
	params := map[string]any{"source": "0x71"}
	err := enforceRPCSourceParam(params)
	if err == nil {
		t.Fatal("string source must be rejected")
	}
	if !errors.Is(err, rpcsource.ErrNon113Source) {
		t.Fatalf("want errors.Is ErrNon113Source, got %v", err)
	}
}

func TestEnforceRPCSourceParam_NilMapNoop(t *testing.T) {
	if err := enforceRPCSourceParam(nil); err != nil {
		t.Fatalf("nil map: %v", err)
	}
}

// TestEnforceRPCSourceParam_RejectsFractionalFloat64 pins the invariant
// that non-integer float64 "source" values are rejected before the
// byte() truncation would mask a near-match fractional value
// (e.g. 113.9 → 113). Regression for PR #505 comment id=3106729474.
func TestEnforceRPCSourceParam_RejectsFractionalFloat64(t *testing.T) {
	for _, v := range []float64{113.9, 113.0001, 112.5, -0.5} {
		params := map[string]any{"source": v}
		err := enforceRPCSourceParam(params)
		if err == nil {
			t.Fatalf("fractional source=%v must be rejected", v)
		}
		if !errors.Is(err, rpcsource.ErrNon113Source) {
			t.Fatalf("source=%v: want ErrNon113Source, got %v", v, err)
		}
	}
}

func TestEnforceRPCSourceParam_AcceptsWholeFloat64_113(t *testing.T) {
	params := map[string]any{"source": float64(113.0)}
	if err := enforceRPCSourceParam(params); err != nil {
		t.Fatalf("113.0 must be accepted, got %v", err)
	}
}

// TestEnforceRPCSourceOnArgs_InjectsParamsWhenAbsent pins the invariant
// that rpc.invoke calls with no "params" field still get source=113
// materialised. Regression for PR #505 comment id=3106729473.
func TestEnforceRPCSourceOnArgs_InjectsParamsWhenAbsent(t *testing.T) {
	args := map[string]any{
		"address": 0x08,
		"plane":   "regulator",
		"method":  "some.method",
		// no "params" at all
	}
	params, err := enforceRPCSourceOnArgs(args)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if params == nil {
		t.Fatal("params must be materialised, got nil")
	}
	if src := params["source"]; src != int(rpcsource.Gateway) {
		t.Fatalf("source = %v, want %d", src, int(rpcsource.Gateway))
	}
	// args must also have the params written back so downstream call
	// sites observe the same map.
	attached, ok := args["params"].(map[string]any)
	if !ok {
		t.Fatalf("args[params] not materialised: %T", args["params"])
	}
	if attached["source"] != int(rpcsource.Gateway) {
		t.Fatalf("attached.source = %v", attached["source"])
	}
}

func TestEnforceRPCSourceOnArgs_InjectsWhenParamsNilMap(t *testing.T) {
	var nilParams map[string]any
	args := map[string]any{"params": nilParams}
	params, err := enforceRPCSourceOnArgs(args)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if params["source"] != int(rpcsource.Gateway) {
		t.Fatalf("source = %v", params["source"])
	}
}

func TestEnforceRPCSourceOnArgs_PreservesExplicit113(t *testing.T) {
	args := map[string]any{
		"params": map[string]any{"source": int(0x71), "foo": "bar"},
	}
	params, err := enforceRPCSourceOnArgs(args)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if params["foo"] != "bar" {
		t.Fatalf("pre-existing params entries dropped: %v", params)
	}
}

// TestEnforceRPCSourceParam_AcceptsByteTypedGateway pins the invariant
// that in-process callers passing the canonical rpc_source.Gateway
// constant directly (type byte/uint8) are accepted without being
// rejected as "unsupported type". Regression for PR #505 comment
// id=3106887656.
func TestEnforceRPCSourceParam_AcceptsByteTypedGateway(t *testing.T) {
	params := map[string]any{"source": rpcsource.Gateway}
	if err := enforceRPCSourceParam(params); err != nil {
		t.Fatalf("byte-typed Gateway must be accepted, got %v", err)
	}
}

func TestEnforceRPCSourceParam_AcceptsByteLiteral113(t *testing.T) {
	params := map[string]any{"source": byte(0x71)}
	if err := enforceRPCSourceParam(params); err != nil {
		t.Fatalf("byte(0x71) must be accepted, got %v", err)
	}
}

func TestEnforceRPCSourceParam_RejectsNon113Byte(t *testing.T) {
	params := map[string]any{"source": byte(114)}
	err := enforceRPCSourceParam(params)
	if err == nil {
		t.Fatal("byte(114) must be rejected")
	}
	if !errors.Is(err, rpcsource.ErrNon113Source) {
		t.Fatalf("want ErrNon113Source, got %v", err)
	}
}

func TestToByteSource_ByteInRange(t *testing.T) {
	got, err := toByteSource(byte(0x71))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != 113 {
		t.Fatalf("got %d, want 113", got)
	}
}

func TestEnforceRPCSourceOnArgs_RejectsNon113(t *testing.T) {
	args := map[string]any{
		"params": map[string]any{"source": int(0x08)},
	}
	_, err := enforceRPCSourceOnArgs(args)
	if err == nil {
		t.Fatal("non-113 source must be rejected")
	}
	if !errors.Is(err, rpcsource.ErrNon113Source) {
		t.Fatalf("want ErrNon113Source, got %v", err)
	}
}
