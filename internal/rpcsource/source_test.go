package rpcsource_test

import (
	"errors"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/rpcsource"
)

func TestEnforce_AcceptsNonZeroSource(t *testing.T) {
	for _, src := range []byte{0x03, 0x08, 0x70, 0x72, 0x7F, 0xFF} {
		if err := rpcsource.Enforce(src); err != nil {
			t.Fatalf("Enforce(0x%02X) = %v, want nil", src, err)
		}
	}
}

func TestEnforce_RejectsZeroSource(t *testing.T) {
	err := rpcsource.Enforce(0x00)
	if err == nil {
		t.Fatal("Enforce(0x00) = nil, want ErrInvalidSource")
	}
	if !errors.Is(err, rpcsource.ErrInvalidSource) {
		t.Fatalf("Enforce(0x00) = %v, want errors.Is ErrInvalidSource", err)
	}
}

func TestRequire_PanicsOnZeroSource(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Require(0x00) must panic")
		}
	}()
	rpcsource.Require(0x00)
}
