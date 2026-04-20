package ebus_standard

// M4c2 — meta.capabilities.responder signal (contract.minor = 1).
//
// Shape + semantics are locked by decision doc
// helianthus-execution-plans@567a6798
// (ebus-standard-l7-services-w16-26.implementing/decisions/m4b2-responder-go-no-go.md)
// §4.2 "Shape" + §4.4 "Invariants".
//
// Consumer rule per §4.3 (fail-closed):
//   - Absence of `meta.capabilities.responder` ⇒ treat as
//     `active.scope = none`. This is why the provider pattern supports a
//     nil provider: when no provider is registered (e.g. in tests or
//     during early boot before transport selection), the key is omitted
//     entirely. Emitting an empty object would falsely promise a shape
//     the consumer is entitled to interpret.
//
// DI via package-level setter (NOT an anti-pattern here): the envelope
// composer is called from ~20 sites across MCP surfaces; threading a
// capability provider through every call would bloat the signature and
// leak a bootstrap concern to every per-call site. The setter is
// idempotent + replace-only and is called exactly once at bootstrap by
// cmd/gateway/main.go based on the active transport. Tests reset to nil
// via t.Cleanup.

import "sync/atomic"

// ResponderCapability is the JSON-serialisable shape emitted under
// `meta.capabilities.responder`. Field tags use the exact literals
// enumerated in decision doc §4.2 Shape. No `omitempty` on active /
// transports — decision §4.4 I1 requires transports[] to ALWAYS have
// three rows at v1.1, and I2 requires active to reference one of them.
type ResponderCapability struct {
	Version    string          `json:"version"`
	Active     ActiveResponder `json:"active"`
	Transports []TransportRow  `json:"transports"`
}

// ActiveResponder mirrors §4.2 `active`. Refusal is a pointer so it
// serialises as JSON null when unset (per the schema sample).
type ActiveResponder struct {
	Transport string         `json:"transport"`
	Scope     string         `json:"scope"`
	Surfaces  []string       `json:"surfaces"`
	Refusal   *ActiveRefusal `json:"refusal"`
}

// ActiveRefusal mirrors §4.2 `active.refusal`. Both fields are strings
// so unknown `code` values at minor=1+ pass through verbatim (per §4.3
// rule 6 the consumer applies fail-closed semantics).
type ActiveRefusal struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

// TransportRow mirrors §4.2 `transports[]`. Reason uses `,omitempty`
// is DELIBERATELY NOT applied — decision §4.4 I6 requires reason to be
// explicit (null on supported rows, non-null on blocked). We emit the
// empty string as JSON "null" via the custom marshaller below so the
// shape matches the schema sample exactly.
type TransportRow struct {
	Transport string   `json:"transport"`
	State     string   `json:"state"`
	Scope     string   `json:"scope"`
	Surfaces  []string `json:"surfaces"`
	Reason    string   `json:"reason"`
}

// ResponderCapabilityProvider returns the current responder capability
// signal for meta.capabilities.responder emission at envelope-assembly
// time. cmd/gateway/main.go populates this once at bootstrap based on
// the active transport. Tests inject a fixed provider via
// SetResponderCapabilityProvider.
type ResponderCapabilityProvider func() ResponderCapability

// activeResponderCapabilityProvider is the package-level provider set
// via SetResponderCapabilityProvider. atomic.Pointer gives race-free
// reads from NewEnvelope concurrent with bootstrap writes (tests
// routinely race Set + composer calls).
var activeResponderCapabilityProvider atomic.Pointer[ResponderCapabilityProvider]

// SetResponderCapabilityProvider is called once at bootstrap. Idempotent;
// subsequent calls replace the provider. Nil provider means emit no
// `meta.capabilities.responder` key (forward-compat §4.3 rule 1:
// consumers treat absence as scope=none).
//
// Lifetime contract: test cleanup code MUST call
// SetResponderCapabilityProvider(nil) in t.Cleanup to restore the
// default null-provider state; otherwise subsequent tests in the same
// package see leaked capability emission.
func SetResponderCapabilityProvider(p ResponderCapabilityProvider) {
	if p == nil {
		activeResponderCapabilityProvider.Store(nil)
		return
	}
	activeResponderCapabilityProvider.Store(&p)
}

// currentResponderCapability returns the capability from the registered
// provider, or (_, false) if no provider is registered. Used by
// NewEnvelope to decide whether to attach meta.capabilities.responder.
func currentResponderCapability() (ResponderCapability, bool) {
	p := activeResponderCapabilityProvider.Load()
	if p == nil {
		return ResponderCapability{}, false
	}
	return (*p)(), true
}

// capabilityToMap converts a ResponderCapability to the plain
// map[string]any shape NewEnvelope already uses for meta.* keys. Using
// a map keeps the canonical-JSON data_hash path untouched (no new types
// need writeCanonical branches).
func capabilityToMap(c ResponderCapability) map[string]any {
	active := map[string]any{
		"transport": c.Active.Transport,
		"scope":     c.Active.Scope,
		"surfaces":  stringSliceToAny(c.Active.Surfaces),
	}
	if c.Active.Refusal != nil {
		active["refusal"] = map[string]any{
			"code":   c.Active.Refusal.Code,
			"reason": c.Active.Refusal.Reason,
		}
	} else {
		active["refusal"] = nil
	}
	rows := make([]any, 0, len(c.Transports))
	for _, r := range c.Transports {
		row := map[string]any{
			"transport": r.Transport,
			"state":     r.State,
			"scope":     r.Scope,
			"surfaces":  stringSliceToAny(r.Surfaces),
		}
		// Decision §4.4 I5/I6: reason MUST be null on supported rows,
		// non-null on blocked rows. We serialise empty string as JSON
		// null to match the schema sample.
		if r.Reason == "" {
			row["reason"] = nil
		} else {
			row["reason"] = r.Reason
		}
		rows = append(rows, row)
	}
	return map[string]any{
		"version":    c.Version,
		"active":     active,
		"transports": rows,
	}
}

// stringSliceToAny converts []string to []any so the generic envelope
// canonical-JSON writer (which branches on []any / []string / etc.) sees
// a stable type regardless of Go build-tag differences.
func stringSliceToAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
