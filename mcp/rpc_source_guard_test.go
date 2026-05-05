package mcp

import (
	"errors"
	"testing"

	rpcsource "github.com/Project-Helianthus/helianthus-ebusgateway/internal/rpc_source"
)

const admittedTestSource byte = 0x7F

func TestEnforceRPCSourceParam_InjectsGatewayWhenMissing(t *testing.T) {
	params := map[string]any{}
	if err := enforceRPCSourceParam(params, admittedTestSource, true); err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	src, ok := params["source"]
	if !ok {
		t.Fatal("expected injected source key")
	}
	if src != int(admittedTestSource) {
		t.Fatalf("injected source = %v, want 0x%02X", src, admittedTestSource)
	}
}

func TestEnforceRPCSourceParam_AcceptsExplicitByteOverride(t *testing.T) {
	for _, v := range []any{int(0x71), float64(0x7F), int64(0x08)} {
		params := map[string]any{"source": v}
		if err := enforceRPCSourceParam(params, admittedTestSource, true); err != nil {
			t.Fatalf("source=%v: %v", v, err)
		}
	}
}

func TestEnforceRPCSourceParam_AcceptsExplicitNonAdmittedOverride(t *testing.T) {
	for _, v := range []any{int(0x03), int(0x08), float64(0x26), 0xFF} {
		params := map[string]any{"source": v}
		if err := enforceRPCSourceParam(params, admittedTestSource, true); err != nil {
			t.Fatalf("source=%v explicit MCP override should be accepted: %v", v, err)
		}
	}
}

func TestEnforceRPCSourceParam_RejectsExplicitZeroOverride(t *testing.T) {
	for _, v := range []any{int(0), float64(0), int64(0), byte(0)} {
		params := map[string]any{"source": v}
		err := enforceRPCSourceParam(params, admittedTestSource, true)
		if err == nil {
			t.Fatalf("source=%v explicit MCP override must reject zero", v)
		}
		if !errors.Is(err, rpcsource.ErrInvalidSource) {
			t.Fatalf("source=%v: want ErrInvalidSource, got %v", v, err)
		}
	}
}

func TestEnforceRPCSourceParam_RejectsUnsupportedType(t *testing.T) {
	params := map[string]any{"source": "0x71"}
	err := enforceRPCSourceParam(params, admittedTestSource, true)
	if err == nil {
		t.Fatal("string source must be rejected")
	}
	if !errors.Is(err, rpcsource.ErrInvalidSource) {
		t.Fatalf("want errors.Is ErrInvalidSource, got %v", err)
	}
}

func TestEnforceRPCSourceParam_NilMapNoop(t *testing.T) {
	if err := enforceRPCSourceParam(nil, admittedTestSource, true); err != nil {
		t.Fatalf("nil map: %v", err)
	}
}

// TestEnforceRPCSourceParam_RejectsFractionalFloat64 pins the invariant
// that non-integer float64 "source" values are rejected before the byte()
// truncation would mask a near-match fractional value. Regression for PR #505
// comment id=3106729474.
func TestEnforceRPCSourceParam_RejectsFractionalFloat64(t *testing.T) {
	for _, v := range []float64{127.9, 127.0001, 126.5, -0.5} {
		params := map[string]any{"source": v}
		err := enforceRPCSourceParam(params, admittedTestSource, true)
		if err == nil {
			t.Fatalf("fractional source=%v must be rejected", v)
		}
		if !errors.Is(err, rpcsource.ErrInvalidSource) {
			t.Fatalf("source=%v: want ErrInvalidSource, got %v", v, err)
		}
	}
}

func TestEnforceRPCSourceParam_AcceptsWholeFloat64(t *testing.T) {
	params := map[string]any{"source": float64(admittedTestSource)}
	if err := enforceRPCSourceParam(params, admittedTestSource, true); err != nil {
		t.Fatalf("whole float64 source must be accepted, got %v", err)
	}
}

// TestEnforceRPCSourceOnArgs_InjectsParamsWhenAbsent pins the invariant
// that rpc.invoke calls with no "params" field still get the admitted source
// materialised. Regression for PR #505 comment id=3106729473.
func TestEnforceRPCSourceOnArgs_InjectsParamsWhenAbsent(t *testing.T) {
	args := map[string]any{
		"address": 0x08,
		"plane":   "regulator",
		"method":  "some.method",
		// no "params" at all
	}
	params, err := enforceRPCSourceOnArgs(args, admittedTestSource, true)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if params == nil {
		t.Fatal("params must be materialised, got nil")
	}
	if src := params["source"]; src != int(admittedTestSource) {
		t.Fatalf("source = %v, want %d", src, int(admittedTestSource))
	}
	// args must also have the params written back so downstream call
	// sites observe the same map.
	attached, ok := args["params"].(map[string]any)
	if !ok {
		t.Fatalf("args[params] not materialised: %T", args["params"])
	}
	if attached["source"] != int(admittedTestSource) {
		t.Fatalf("attached.source = %v", attached["source"])
	}
}

func TestEnforceRPCSourceOnArgs_InjectsWhenParamsNilMap(t *testing.T) {
	var nilParams map[string]any
	args := map[string]any{"params": nilParams}
	params, err := enforceRPCSourceOnArgs(args, admittedTestSource, true)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if params["source"] != int(admittedTestSource) {
		t.Fatalf("source = %v", params["source"])
	}
}

func TestEnforceRPCSourceOnArgs_PreservesExplicitOverride(t *testing.T) {
	args := map[string]any{
		"params": map[string]any{"source": int(0x08), "foo": "bar"},
	}
	params, err := enforceRPCSourceOnArgs(args, admittedTestSource, true)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if params["foo"] != "bar" {
		t.Fatalf("pre-existing params entries dropped: %v", params)
	}
}

// TestEnforceRPCSourceParam_AcceptsByteTypedSource pins the invariant that
// in-process callers passing a byte/uint8 source directly are accepted without
// being rejected as "unsupported type". Regression for PR #505 comment
// id=3106887656.
func TestEnforceRPCSourceParam_AcceptsByteTypedSource(t *testing.T) {
	params := map[string]any{"source": admittedTestSource}
	if err := enforceRPCSourceParam(params, admittedTestSource, true); err != nil {
		t.Fatalf("byte-typed source must be accepted, got %v", err)
	}
}

func TestEnforceRPCSourceParam_AcceptsByteLiteralOverride(t *testing.T) {
	params := map[string]any{"source": byte(0x08)}
	if err := enforceRPCSourceParam(params, admittedTestSource, true); err != nil {
		t.Fatalf("byte override source must be accepted, got %v", err)
	}
}

func TestEnforceRPCSourceParam_AcceptsNonAdmittedByteOverride(t *testing.T) {
	params := map[string]any{"source": byte(114)}
	if err := enforceRPCSourceParam(params, admittedTestSource, true); err != nil {
		t.Fatalf("byte(114) explicit MCP override should be accepted: %v", err)
	}
}

func TestToByteSource_ByteInRange(t *testing.T) {
	got, err := toByteSource(admittedTestSource)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != admittedTestSource {
		t.Fatalf("got %d, want %d", got, admittedTestSource)
	}
}

func TestEnforceRPCSourceOnArgs_AcceptsExplicitOverride(t *testing.T) {
	args := map[string]any{
		"params": map[string]any{"source": int(0x08)},
	}
	if _, err := enforceRPCSourceOnArgs(args, admittedTestSource, true); err != nil {
		t.Fatalf("explicit MCP override should be accepted: %v", err)
	}
}

func TestEnforceRPCSourceOnArgs_FailsClosedBeforeAdmission(t *testing.T) {
	args := map[string]any{"params": map[string]any{}}
	if _, err := enforceRPCSourceOnArgs(args, 0, false); err == nil {
		t.Fatal("rpc.invoke must fail closed before source admission")
	}
}
