package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
)

// TestResponderCapabilityProvider_CanonicalENH pins invariant I2 for the
// ENH raw alias: buildResponderCapabilityProvider MUST canonicalise to
// the "ENH" label that literally appears in transports[].
func TestResponderCapabilityProvider_CanonicalENH(t *testing.T) {
	provider := buildResponderCapabilityProvider(ebusgateway.Config{
		TransportConfig: ebusgateway.TransportConfig{
			Protocol: ebusgateway.TransportENH,
		},
	})
	if provider == nil {
		t.Fatalf("provider is nil; want non-nil for ENH")
	}
	cap := provider()
	if cap.Active.Transport != "ENH" {
		t.Fatalf("active.transport = %q; want %q", cap.Active.Transport, "ENH")
	}
	assertActiveInTransports(t, cap)
	if cap.Active.Scope != "partial" {
		t.Fatalf("ENH active.scope = %q; want %q", cap.Active.Scope, "partial")
	}
}

// TestResponderCapabilityProvider_CanonicalENS mirrors the ENH test for
// ENS.
func TestResponderCapabilityProvider_CanonicalENS(t *testing.T) {
	provider := buildResponderCapabilityProvider(ebusgateway.Config{
		TransportConfig: ebusgateway.TransportConfig{
			Protocol: ebusgateway.TransportENS,
		},
	})
	if provider == nil {
		t.Fatalf("provider is nil; want non-nil for ENS")
	}
	cap := provider()
	if cap.Active.Transport != "ENS" {
		t.Fatalf("active.transport = %q; want %q", cap.Active.Transport, "ENS")
	}
	assertActiveInTransports(t, cap)
	if cap.Active.Scope != "partial" {
		t.Fatalf("ENS active.scope = %q; want %q", cap.Active.Scope, "partial")
	}
}

// TestResponderCapabilityProvider_CanonicalEbusdTCP exercises both the
// canonical "ebusd-tcp" literal and the "ebusd" alias that the transport
// parser accepts. Both MUST resolve to active.transport = "ebusd-tcp"
// (matching the transports[] row label exactly).
func TestResponderCapabilityProvider_CanonicalEbusdTCP(t *testing.T) {
	for _, raw := range []ebusgateway.TransportProtocol{
		ebusgateway.TransportEbusdTCP,
		ebusgateway.TransportProtocol("ebusd"),
		ebusgateway.TransportProtocol("EBUSD"),
		ebusgateway.TransportProtocol("  ebusd-tcp  "),
	} {
		provider := buildResponderCapabilityProvider(ebusgateway.Config{
			TransportConfig: ebusgateway.TransportConfig{
				Protocol: raw,
			},
		})
		if provider == nil {
			t.Fatalf("raw=%q: provider is nil; want non-nil", raw)
		}
		cap := provider()
		if cap.Active.Transport != "ebusd-tcp" {
			t.Fatalf("raw=%q: active.transport = %q; want %q", raw, cap.Active.Transport, "ebusd-tcp")
		}
		assertActiveInTransports(t, cap)
		if cap.Active.Scope != "none" {
			t.Fatalf("raw=%q: active.scope = %q; want %q", raw, cap.Active.Scope, "none")
		}
		if cap.Active.Refusal == nil || cap.Active.Refusal.Code != "command_bridge_no_companion_listen" {
			t.Fatalf("raw=%q: active.refusal = %+v; want code=command_bridge_no_companion_listen", raw, cap.Active.Refusal)
		}
	}
}

// TestResponderCapabilityProvider_UnknownTransport_OmitsCapability locks
// the fail-closed absence contract (§4.3 rule 1). Any transport string
// that does not canonicalise to ENH/ENS/ebusd-tcp MUST cause
// buildResponderCapabilityProvider to return nil, so the envelope
// composer omits meta.capabilities.responder entirely. This preserves
// I1 (exactly three rows at v1.1) without violating I2 (active must be
// in transports[]).
func TestResponderCapabilityProvider_UnknownTransport_OmitsCapability(t *testing.T) {
	for _, raw := range []ebusgateway.TransportProtocol{
		ebusgateway.TransportProtocol("banana"),
		ebusgateway.TransportProtocol("udp-plain"),
		ebusgateway.TransportProtocol("tcp-plain"),
		ebusgateway.TransportProtocol("adapter-direct"),
		ebusgateway.TransportProtocol(""),
	} {
		provider := buildResponderCapabilityProvider(ebusgateway.Config{
			TransportConfig: ebusgateway.TransportConfig{
				Protocol: raw,
			},
		})
		if provider != nil {
			t.Fatalf("raw=%q: provider is non-nil (active=%+v); want nil so envelope omits capability", raw, provider().Active)
		}
	}
}

// assertActiveInTransports is the local I2 guard: active.transport MUST
// literally appear as one of the transports[] row labels.
func assertActiveInTransports(t *testing.T, cap ebus_standard.ResponderCapability) {
	t.Helper()
	if len(cap.Transports) != 3 {
		t.Fatalf("transports[] has %d rows; want 3 (I1 at v1.1)", len(cap.Transports))
	}
	for _, row := range cap.Transports {
		if row.Transport == cap.Active.Transport {
			return
		}
	}
	var labels []string
	for _, row := range cap.Transports {
		labels = append(labels, row.Transport)
	}
	t.Fatalf("I2 violated: active.transport=%q not in transports[]=%v", cap.Active.Transport, labels)
}
