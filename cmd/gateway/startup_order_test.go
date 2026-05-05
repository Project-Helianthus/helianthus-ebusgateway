package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
)

func TestShouldCloseSemanticBarrier(t *testing.T) {
	cases := []struct {
		name             string
		admissionPath    ebusgateway.TransportAdmissionPath
		overrideSet      bool
		joinResultNotNil bool
		wantClose        bool
	}{
		{"source-selection-capable + source-address selector success, override unset -> close", ebusgateway.TransportAdmissionSourceSelectionCapable, false, true, true},
		{"source-selection-capable + no source-address selector result, override unset -> keep open (DEGRADED)", ebusgateway.TransportAdmissionSourceSelectionCapable, false, false, false},
		{"source-selection-capable + override set (any source-address selector state) -> close", ebusgateway.TransportAdmissionSourceSelectionCapable, true, false, true},
		{"source-selection-capable + override set + source-address selector result -> close", ebusgateway.TransportAdmissionSourceSelectionCapable, true, true, true},
		{"static-fallback (ebusd-tcp) -> close (legacy behavior)", ebusgateway.TransportAdmissionStaticFallback, false, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldCloseSemanticBarrier(tc.admissionPath, tc.overrideSet, tc.joinResultNotNil)
			if got != tc.wantClose {
				t.Errorf("got %v, want %v", got, tc.wantClose)
			}
		})
	}
}
