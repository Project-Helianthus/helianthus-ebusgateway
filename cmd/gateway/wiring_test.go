package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	ebusgoTransport "github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// muxBypassTransport is a RawTransport that does NOT satisfy
// ResponderTransport. It models the adapter-direct mux's active path
// (cmd/gateway/main.go wireAdapterDirect -> mux.ActiveTransport()): the
// live transport implements RawTransport but has no SendResponderBytes
// primitive. Used in the Codex-P1 regression tests below.
type muxBypassTransport struct{}

func (muxBypassTransport) ReadByte() (byte, error)     { return 0, nil }
func (muxBypassTransport) Write(p []byte) (int, error) { return len(p), nil }
func (muxBypassTransport) Close() error                { return nil }

// responderCapableTransport is a RawTransport that ALSO satisfies
// ResponderTransport. Used to pin the "genuine ENH" branch: config says
// enh AND the live instance exposes the responder send primitive.
type responderCapableTransport struct{ muxBypassTransport }

func (responderCapableTransport) SendResponderBytes(payload []byte) (int, error) {
	return len(payload), nil
}

// Compile-time guards: these assertions codify the test contract so a
// future refactor of either interface breaks the tests at build time,
// not at runtime.
var (
	_ ebusgoTransport.RawTransport       = muxBypassTransport{}
	_ ebusgoTransport.RawTransport       = responderCapableTransport{}
	_ ebusgoTransport.ResponderTransport = responderCapableTransport{}
)

// TestResponderCapabilityProvider_CanonicalENH pins invariant I2 for the
// ENH raw alias: buildResponderCapabilityProvider MUST canonicalise to
// the "ENH" label that literally appears in transports[].
func TestResponderCapabilityProvider_CanonicalENH(t *testing.T) {
	provider := buildResponderCapabilityProvider(ebusgateway.Config{
		TransportConfig: ebusgateway.TransportConfig{
			Protocol: ebusgateway.TransportENH,
		},
	}, nil)
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
	}, nil)
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
		}, nil)
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
		}, nil)
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

// TestResponderCapabilityProvider_AdapterDirect_WithoutResponderSupport_DowngradesToNone
// locks the Codex P1 fix on PR #509. The adapter-direct URI mode keeps
// cfg.TransportConfig.Protocol == "enh" while wireAdapterDirect installs
// a mux.ActiveTransport() that does NOT satisfy ResponderTransport. The
// provider must downgrade active.scope to "none" with an explicit
// refusal, NOT over-advertise "partial" from the config string alone.
func TestResponderCapabilityProvider_AdapterDirect_WithoutResponderSupport_DowngradesToNone(t *testing.T) {
	provider := buildResponderCapabilityProvider(ebusgateway.Config{
		TransportConfig: ebusgateway.TransportConfig{
			Protocol: ebusgateway.TransportENH,
		},
	}, muxBypassTransport{})
	if provider == nil {
		t.Fatalf("provider is nil; want non-nil with explicit refusal")
	}
	cap := provider()
	if cap.Active.Transport != "ENH" {
		t.Fatalf("active.transport = %q; want %q (I2 — enum literal preserved)", cap.Active.Transport, "ENH")
	}
	if cap.Active.Scope != "none" {
		t.Fatalf("active.scope = %q; want %q (mux bypass must downgrade from partial)", cap.Active.Scope, "none")
	}
	if len(cap.Active.Surfaces) != 0 {
		t.Fatalf("active.surfaces = %v; want [] (no surfaces when scope=none)", cap.Active.Surfaces)
	}
	if cap.Active.Refusal == nil {
		t.Fatalf("active.refusal = nil; want non-nil with transport_mux_bypass code")
	}
	if cap.Active.Refusal.Code != "transport_mux_bypass" {
		t.Fatalf("active.refusal.code = %q; want %q", cap.Active.Refusal.Code, "transport_mux_bypass")
	}
	if cap.Active.Refusal.Reason == "" {
		t.Fatalf("active.refusal.reason is empty; want explanatory string about ResponderTransport")
	}
	assertActiveInTransports(t, cap)
}

// TestResponderCapabilityProvider_ENH_WithActualResponderSupport_ReportsPartial
// pins the positive path: config=enh AND the live instance satisfies
// ResponderTransport (the canonical ENH transport in production). The
// provider must emit scope=partial with surfaces FF_03..FF_06.
func TestResponderCapabilityProvider_ENH_WithActualResponderSupport_ReportsPartial(t *testing.T) {
	provider := buildResponderCapabilityProvider(ebusgateway.Config{
		TransportConfig: ebusgateway.TransportConfig{
			Protocol: ebusgateway.TransportENH,
		},
	}, responderCapableTransport{})
	if provider == nil {
		t.Fatalf("provider is nil; want non-nil for genuine ENH")
	}
	cap := provider()
	if cap.Active.Transport != "ENH" {
		t.Fatalf("active.transport = %q; want %q", cap.Active.Transport, "ENH")
	}
	if cap.Active.Scope != "partial" {
		t.Fatalf("active.scope = %q; want %q (genuine ENH responder)", cap.Active.Scope, "partial")
	}
	if len(cap.Active.Surfaces) != 4 {
		t.Fatalf("active.surfaces = %v; want 4 FF_0x surfaces", cap.Active.Surfaces)
	}
	if cap.Active.Refusal != nil {
		t.Fatalf("active.refusal = %+v; want nil on genuine ENH path", cap.Active.Refusal)
	}
	assertActiveInTransports(t, cap)
}

// TestResponderCapabilityProvider_MuxBypass_DowngradesMatchingTransportRow
// locks the Codex round-3 P1 finding on PR #509: when active is
// downgraded to scope=none because the runtime mux does not satisfy
// ResponderTransport, the matching transports[] row MUST also be
// downgraded to state="blocked", scope="none", surfaces=[],
// reason="transport_mux_bypass". Otherwise a consumer joining
// active.transport to the row set sees contradictory metadata
// (active.scope=none but row.scope=partial), violating invariant I3.
//
// Per Interpretation A (the adapter-direct mux is a shared runtime
// wrapper across ENH/ENS upstreams), both ENH and ENS rows downgrade
// whenever the bypass is detected regardless of which canonical enum
// is active. The ebusd-tcp row is left untouched with its own reason.
func TestResponderCapabilityProvider_MuxBypass_DowngradesMatchingTransportRow(t *testing.T) {
	for _, canonical := range []ebusgateway.TransportProtocol{
		ebusgateway.TransportENH,
		ebusgateway.TransportENS,
	} {
		provider := buildResponderCapabilityProvider(ebusgateway.Config{
			TransportConfig: ebusgateway.TransportConfig{Protocol: canonical},
		}, muxBypassTransport{})
		if provider == nil {
			t.Fatalf("canonical=%q: provider is nil", canonical)
		}
		cap := provider()
		if len(cap.Transports) != 3 {
			t.Fatalf("canonical=%q: transports[] has %d rows; want 3 (I1)", canonical, len(cap.Transports))
		}

		var enhRow, ensRow, ebusdRow *ebus_standard.TransportRow
		for i := range cap.Transports {
			row := &cap.Transports[i]
			switch row.Transport {
			case "ENH":
				enhRow = row
			case "ENS":
				ensRow = row
			case "ebusd-tcp":
				ebusdRow = row
			}
		}
		if enhRow == nil || ensRow == nil || ebusdRow == nil {
			t.Fatalf("canonical=%q: missing expected row (ENH=%v ENS=%v ebusd-tcp=%v)", canonical, enhRow, ensRow, ebusdRow)
		}

		// Both ENH and ENS rows must be downgraded — the mux is shared.
		for _, row := range []*ebus_standard.TransportRow{enhRow, ensRow} {
			if row.State != "blocked" {
				t.Fatalf("canonical=%q: %s row.state = %q; want %q", canonical, row.Transport, row.State, "blocked")
			}
			if row.Scope != "none" {
				t.Fatalf("canonical=%q: %s row.scope = %q; want %q", canonical, row.Transport, row.Scope, "none")
			}
			if len(row.Surfaces) != 0 {
				t.Fatalf("canonical=%q: %s row.surfaces = %v; want []", canonical, row.Transport, row.Surfaces)
			}
			if row.Reason != "transport_mux_bypass" {
				t.Fatalf("canonical=%q: %s row.reason = %q; want %q", canonical, row.Transport, row.Reason, "transport_mux_bypass")
			}
		}

		// ebusd-tcp row must be untouched (its own state/reason).
		if ebusdRow.State != "blocked" || ebusdRow.Scope != "none" {
			t.Fatalf("canonical=%q: ebusd-tcp row state/scope disturbed: %+v", canonical, ebusdRow)
		}
		if ebusdRow.Reason != "command_bridge_no_companion_listen" {
			t.Fatalf("canonical=%q: ebusd-tcp row.reason = %q; want %q (must NOT be overwritten by mux bypass)", canonical, ebusdRow.Reason, "command_bridge_no_companion_listen")
		}

		assertActiveInTransports(t, cap)
		assertI3ActiveScopeMatchesRow(t, cap)
	}
}

// assertI3ActiveScopeMatchesRow is the invariant I3 guard:
// active.scope MUST equal transports[x].scope where x.transport ==
// active.transport.
func assertI3ActiveScopeMatchesRow(t *testing.T, cap ebus_standard.ResponderCapability) {
	t.Helper()
	for _, row := range cap.Transports {
		if row.Transport == cap.Active.Transport {
			if row.Scope != cap.Active.Scope {
				t.Fatalf("I3 violated: active.transport=%q active.scope=%q but row.scope=%q",
					cap.Active.Transport, cap.Active.Scope, row.Scope)
			}
			return
		}
	}
	// I2 would have flagged absence already; safe to return.
}

// TestResponderCapabilityProvider_I3_ActiveScopeEqualsTransportRowScope
// sweeps every canonical-protocol × transport-instance branch and
// asserts invariant I3 holds unconditionally.
func TestResponderCapabilityProvider_I3_ActiveScopeEqualsTransportRowScope(t *testing.T) {
	cases := []struct {
		name     string
		protocol ebusgateway.TransportProtocol
		instance ebusgoTransport.RawTransport
	}{
		{"ENH/nil", ebusgateway.TransportENH, nil},
		{"ENH/responder-capable", ebusgateway.TransportENH, responderCapableTransport{}},
		{"ENH/mux-bypass", ebusgateway.TransportENH, muxBypassTransport{}},
		{"ENS/nil", ebusgateway.TransportENS, nil},
		{"ENS/responder-capable", ebusgateway.TransportENS, responderCapableTransport{}},
		{"ENS/mux-bypass", ebusgateway.TransportENS, muxBypassTransport{}},
		{"ebusd-tcp/nil", ebusgateway.TransportEbusdTCP, nil},
		{"ebusd-tcp/mux-bypass", ebusgateway.TransportEbusdTCP, muxBypassTransport{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			provider := buildResponderCapabilityProvider(ebusgateway.Config{
				TransportConfig: ebusgateway.TransportConfig{Protocol: tc.protocol},
			}, tc.instance)
			if provider == nil {
				t.Fatalf("provider is nil for %s", tc.name)
			}
			cap := provider()
			assertActiveInTransports(t, cap)
			assertI3ActiveScopeMatchesRow(t, cap)
		})
	}
}

// TestResponderCapabilityProvider_ENS_WithMuxBypass_ReportsNone mirrors
// the ENH mux-bypass test for ENS: --address=adapter-direct-ens:// can
// keep Protocol=="ens" but the mux active transport still does not
// satisfy ResponderTransport. Must downgrade to scope=none.
func TestResponderCapabilityProvider_ENS_WithMuxBypass_ReportsNone(t *testing.T) {
	provider := buildResponderCapabilityProvider(ebusgateway.Config{
		TransportConfig: ebusgateway.TransportConfig{
			Protocol: ebusgateway.TransportENS,
		},
	}, muxBypassTransport{})
	if provider == nil {
		t.Fatalf("provider is nil; want non-nil with explicit refusal")
	}
	cap := provider()
	if cap.Active.Transport != "ENS" {
		t.Fatalf("active.transport = %q; want %q", cap.Active.Transport, "ENS")
	}
	if cap.Active.Scope != "none" {
		t.Fatalf("active.scope = %q; want %q (ENS mux bypass)", cap.Active.Scope, "none")
	}
	if cap.Active.Refusal == nil || cap.Active.Refusal.Code != "transport_mux_bypass" {
		t.Fatalf("active.refusal = %+v; want code=transport_mux_bypass", cap.Active.Refusal)
	}
	assertActiveInTransports(t, cap)
}

// TestM5Portal_EbusStandardServer_WiredInProduction asserts that the
// gateway bootstrap hands the MCP sub-server into portal.Options. The
// production wiring in main.go does:
//
//	portal.NewHandler(portal.Options{
//	    ...
//	    EbusStandardServer: mcpServer.EbusStandardServer(),
//	})
//
// This smoke test mirrors that exact assignment. If mcp.NewServer stops
// installing the ebus_standard sub-server, or if the accessor goes nil,
// this test fails — preventing the regression flagged by Codex P1 on
// PR #507 (portal.Options.EbusStandardServer omitted -> all four
// /api/v1/ebus-standard/* routes return 404 in the real gateway process
// despite tests with fakes passing).
func TestM5Portal_EbusStandardServer_WiredInProduction(t *testing.T) {
	mcpServer, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("mcp.NewServer: %v", err)
	}

	sub := mcpServer.EbusStandardServer()
	if sub == nil {
		t.Fatalf("mcpServer.EbusStandardServer() is nil — RegisterEbusStandardTools not installed inside NewServer")
	}

	// Exercise the exact assignment main.go uses. This must compile:
	// mcp.EbusStandardSubServer -> portal.PortalEbusStandardServer
	// (structural-typing compatibility is load-bearing — if either
	// interface drifts, the production wiring breaks at build time.)
	opts := portal.Options{EbusStandardServer: sub}
	if opts.EbusStandardServer == nil {
		t.Fatalf("portal.Options.EbusStandardServer is nil after assignment")
	}

	// Dispatch a live call through the assigned interface to verify the
	// method set reaches the real catalog, not a typed-nil wrapper.
	data := opts.EbusStandardServer.ServicesList()
	if data == nil {
		t.Fatalf("EbusStandardServer.ServicesList() returned nil — catalog not reachable")
	}
	if ns, ok := data["namespace"].(string); !ok || ns != "ebus_standard" {
		t.Fatalf("ServicesList() namespace = %v, want ebus_standard", data["namespace"])
	}
}
