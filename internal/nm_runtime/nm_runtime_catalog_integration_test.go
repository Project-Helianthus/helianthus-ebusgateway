//go:build !tinygo
// +build !tinygo

package nm_runtime_test

import (
	"context"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/nm_runtime"
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

// TestEmit_AgainstEmbeddedCatalog is the catalog-grounded regression gate
// for issue #505 r3106832675. It loads the REAL embedded ebusreg catalog
// and asserts that every nm_runtime.EmitEvent constant resolves to a
// catalog command (no ErrNoCatalogEntry) AND emits successfully through
// the policy gate. Drift between event constants and catalog
// service_variant strings is caught at test time.
func TestEmit_AgainstEmbeddedCatalog(t *testing.T) {
	cat := ebusstd.MustEmbeddedCatalog()

	cases := []struct {
		event  nm_runtime.EmitEvent
		wantPB byte
		wantSB byte
	}{
		{nm_runtime.EventResetStatus, 0xFF, 0x00},
		{nm_runtime.EventFailure, 0xFF, 0x02},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(string(tc.event), func(t *testing.T) {
			em := &recordingEmitter{}
			rt, err := nm_runtime.NewRuntime(cat, em, testRuntimeSource)
			if err != nil {
				t.Fatalf("NewRuntime err: %v", err)
			}
			if err := rt.Emit(context.Background(), tc.event, []byte{0x00}); err != nil {
				t.Fatalf("emit %q against embedded catalog: %v", tc.event, err)
			}
			if len(em.calls) != 1 {
				t.Fatalf("emit %q: want 1 emit call, got %d", tc.event, len(em.calls))
			}
			c := em.calls[0]
			if c.src != testRuntimeSource {
				t.Fatalf("emit %q: source = 0x%02X, want 0x%02X", tc.event, c.src, testRuntimeSource)
			}
			if c.pb != tc.wantPB || c.sb != tc.wantSB {
				t.Fatalf("emit %q: pb/sb = 0x%02X/0x%02X, want 0x%02X/0x%02X",
					tc.event, c.pb, c.sb, tc.wantPB, tc.wantSB)
			}
		})
	}
}
