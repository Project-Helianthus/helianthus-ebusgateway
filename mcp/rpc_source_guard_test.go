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
