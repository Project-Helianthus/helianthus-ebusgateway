package rpc_source_test

import (
	"errors"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/rpc_source"
)

func TestGateway_IsConstant113(t *testing.T) {
	if rpc_source.Gateway != 0x71 {
		t.Fatalf("Gateway = 0x%02X, want 0x71 (113)", rpc_source.Gateway)
	}
}

func TestEnforce_AcceptsGateway(t *testing.T) {
	if err := rpc_source.Enforce(0x71); err != nil {
		t.Fatalf("Enforce(0x71) = %v, want nil", err)
	}
}

func TestEnforce_RejectsNon113(t *testing.T) {
	for _, src := range []byte{0x00, 0x03, 0x08, 0x70, 0x72, 0xFF} {
		err := rpc_source.Enforce(src)
		if err == nil {
			t.Fatalf("Enforce(0x%02X) = nil, want ErrNon113Source", src)
		}
		if !errors.Is(err, rpc_source.ErrNon113Source) {
			t.Fatalf("Enforce(0x%02X) = %v, want errors.Is ErrNon113Source", src, err)
		}
	}
}

func TestRequire_PanicsOnNon113(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Require(0x08) must panic")
		}
	}()
	rpc_source.Require(0x08)
}
