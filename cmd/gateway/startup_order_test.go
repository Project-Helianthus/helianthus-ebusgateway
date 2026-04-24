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
		{"join-capable + joiner success, override unset -> close", ebusgateway.TransportAdmissionJoinCapable, false, true, true},
		{"join-capable + no joiner result, override unset -> keep open (DEGRADED)", ebusgateway.TransportAdmissionJoinCapable, false, false, false},
		{"join-capable + override set (any joiner state) -> close", ebusgateway.TransportAdmissionJoinCapable, true, false, true},
		{"join-capable + override set + joiner result -> close", ebusgateway.TransportAdmissionJoinCapable, true, true, true},
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
