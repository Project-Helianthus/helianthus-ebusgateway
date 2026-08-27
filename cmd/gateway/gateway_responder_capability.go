package main

import (
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
	ebusgoTransport "github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

func buildResponderCapabilityProvider(cfg ebusgateway.Config, actualTransport ebusgoTransport.RawTransport) ebus_standard.ResponderCapabilityProvider {
	surfacesFF := []string{"FF_03", "FF_04", "FF_05", "FF_06"}
	// transports[] is static at v1.1 — same three rows on every gateway
	// regardless of deployment (I1 locks the count at exactly three).
	transports := []ebus_standard.TransportRow{
		{Transport: "ENH", State: "supported", Scope: "partial", Surfaces: surfacesFF, Reason: ""},
		{Transport: "ENS", State: "supported", Scope: "partial", Surfaces: surfacesFF, Reason: ""},
		{Transport: "ebusd-tcp", State: "blocked", Scope: "none", Surfaces: []string{}, Reason: "command_bridge_no_companion_listen"},
	}

	// Runtime transport authority: a non-nil actualTransport that does
	// NOT satisfy ResponderTransport means the live bus path cannot
	// actually emit responder bytes (the adapter-direct mux is the
	// concrete case today). A nil actualTransport means bootstrap is
	// still pre-wiring (e.g. unit tests that only construct Config); in
	// that case fall back to protocol-only inference so legacy callers
	// keep their behaviour.
	transportKnown := actualTransport != nil
	_, responderCapable := actualTransport.(ebusgoTransport.ResponderTransport)
	muxBypass := transportKnown && !responderCapable
	muxBypassRefusal := &ebus_standard.ActiveRefusal{
		Code:   "transport_mux_bypass",
		Reason: "runtime transport does not satisfy ResponderTransport (e.g. adapter-direct mux)",
	}

	// Invariant I3 (decision doc §4.4): active.scope MUST equal
	// transports[x].scope where x.transport == active.transport. When the
	// runtime mux bypass is in effect we therefore also rewrite the
	// matching transports[] row(s) — otherwise a consumer joining
	// active.transport → transports[] would see contradictory metadata
	// (active.scope=none but row.scope=partial).
	//
	// Interpretation A (shared-runtime downgrade): the adapter-direct mux
	// is a single wrapper that sits above whichever ENH/ENS upstream the
	// operator configured. The mux instance itself does not satisfy
	// ResponderTransport, so switching the canonical protocol from ENH to
	// ENS (or vice versa) under the same mux would not restore responder
	// emission — the bypass is shared. Accordingly, BOTH the ENH and ENS
	// rows downgrade to state=blocked, scope=none, reason="transport_mux_bypass".
	// The ebusd-tcp row is left untouched (it is always blocked with its
	// own reason "command_bridge_no_companion_listen" per §4.2/§3).
	// Invariants preserved: I1 (still exactly 3 rows), I2 (active.transport
	// still appears verbatim), I3 (active.scope == matching row.scope),
	// I5 (state=blocked ⇒ reason != ""), I7 (row order ENH,ENS,ebusd-tcp
	// unchanged).
	if muxBypass {
		for i := range transports {
			row := &transports[i]
			if row.Transport == "ENH" || row.Transport == "ENS" {
				row.State = "blocked"
				row.Scope = "none"
				row.Surfaces = []string{}
				row.Reason = "transport_mux_bypass"
			}
		}
	}

	var active ebus_standard.ActiveResponder
	// Canonicalise raw TransportProtocol (handles "ebusd" alias, case,
	// whitespace) into one of the enum constants so active.transport
	// literally equals the transports[] row label (invariant I2).
	switch ebusgateway.CanonicalTransportProtocol(cfg.TransportConfig.Protocol) {
	case ebusgateway.TransportENH:
		if muxBypass {
			active = ebus_standard.ActiveResponder{Transport: "ENH", Scope: "none", Surfaces: []string{}, Refusal: muxBypassRefusal}
		} else {
			active = ebus_standard.ActiveResponder{Transport: "ENH", Scope: "partial", Surfaces: surfacesFF}
		}
	case ebusgateway.TransportENS:
		if muxBypass {
			active = ebus_standard.ActiveResponder{Transport: "ENS", Scope: "none", Surfaces: []string{}, Refusal: muxBypassRefusal}
		} else {
			active = ebus_standard.ActiveResponder{Transport: "ENS", Scope: "partial", Surfaces: surfacesFF}
		}
	case ebusgateway.TransportEbusdTCP:
		active = ebus_standard.ActiveResponder{
			Transport: "ebusd-tcp",
			Scope:     "none",
			Surfaces:  []string{},
			Refusal:   &ebus_standard.ActiveRefusal{Code: "command_bridge_no_companion_listen", Reason: "ebusd command bridge does not expose a responder-role emission primitive"},
		}
	default:
		// Non-enumerated transports (udp-plain, tcp-plain,
		// adapter-direct, empty, or entirely unknown). Fail-closed:
		// return nil so the envelope omits meta.capabilities.responder
		// entirely (§4.3 rule 1). Emitting the raw string would
		// violate I2; adding a fourth transports[] row would violate
		// I1. Absence is the only invariant-preserving outcome.
		return nil
	}

	cap := ebus_standard.ResponderCapability{
		Version:    "v1",
		Active:     active,
		Transports: transports,
	}
	return func() ebus_standard.ResponderCapability { return cap }
}

// applyStaticSeedTable plants the productids static seed entries into
// the registry. Each seed entry contributes one DeviceInfo per address
// with full Vaillant identity (Manufacturer + DeviceID), allowing the
// registry's identity-merge contract to collapse canonical-pair faces
// into a single entry. SerialNumber and version fields are
// intentionally empty — they will be populated by subsequent active
// enrichment (P5 follow-up) or remain empty for seed-only addresses
// (e.g. NETX3 broadcast face 0x04 which does not respond to active
// probes).
//
// Phase post-C P3 (live validation 2026-05-08): NETX3's 0x04 face was
// absent from the registry entirely because broadcast-source frames
// never carry an ACKCorrelation that would feed the inserter. Static
// seed bypasses that gate at startup.
//
// P3.5 (Codex P2 follow-up, ebusreg PR #137): switches the per-address
// call from registry.Register (which stamps the AddressSlot with
// DiscoverySourceActiveConfirmed/VerificationStateIdentityConfirmed —
// wrong observability label for a pre-known seed entry) to
// registry.RegisterStaticSeed (which stamps DiscoverySourceStaticSeed
// /VerificationStateCandidate). Operators reading
// `ebus.v1.registry.devices.list` or the address-table snapshot via
// MCP/JSON now correctly see seeded addresses labelled
// `discovery_source: "static_seed"`, `verification: "candidate"`
// instead of pretending the gateway actively confirmed them at boot.
//
// Role mapping happens at this seam (gateway), not in productids or
// registry: productids.SeedAddressEntry.Role is a free-form string
// (`"initiator"` / `"target"`); registry.SlotRole is a typed enum
// (SlotRoleMaster / SlotRoleSlave / SlotRoleUnknown). Unknown role
// strings fall through to SlotRoleUnknown — registry's monotonic Role
// guard then leaves Role empty until passive observation or active
// scan fills it in.
