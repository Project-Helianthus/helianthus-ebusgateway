package portalconfig

import "net/netip"

// ListenerBinding is host-owned TCP binding availability, never browser input.
// Equal owner and endpoint identify one listener with multiple HTTP routes.
type ListenerBinding struct {
	Owner     string     `json:"owner"`
	Address   netip.Addr `json:"address"`
	Port      uint16     `json:"port"`
	Available bool       `json:"-"`
}

type ListenerAvailability struct {
	Local, GraphQL, MCP, AdapterProxy ListenerBinding
}

type PlannedSurface struct {
	Surface   string           `json:"surface"`
	Desired   bool             `json:"desired"`
	Scope     string           `json:"scope"`
	Effective bool             `json:"effective"`
	Status    string           `json:"status"`
	Reason    string           `json:"reason"`
	Binding   *ListenerBinding `json:"binding,omitempty"`
}

type ListenerConflict struct {
	First  string `json:"first"`
	Second string `json:"second"`
	Reason string `json:"reason"`
}

type ListenerPlan struct {
	Status    string             `json:"status"`
	Surfaces  []PlannedSurface   `json:"surfaces"`
	Conflicts []ListenerConflict `json:"conflicts"`
}

// DeriveListenerPlan describes intent only. No plan is proof of application,
// listener readiness, native admission or output publication.
func DeriveListenerPlan(r Record, a ListenerAvailability) ListenerPlan {
	p := ListenerPlan{Status: "pending", Surfaces: []PlannedSurface{}, Conflicts: []ListenerConflict{}}
	bindings := []ListenerBinding{a.Local, a.GraphQL, a.MCP, a.AdapterProxy}
	for i, choice := range []struct {
		name    string
		desired bool
	}{{"local", true}, {"graphql", r.Sharing.GraphQL}, {"mcp", r.Sharing.MCP}, {"adapter_proxy", r.AdapterProxy}, {"matter", r.OutputBindings.Matter}} {
		s := PlannedSurface{Surface: choice.name, Desired: choice.desired, Scope: "shared", Status: "withheld", Reason: "evidence_unavailable"}
		if i == 0 || (!choice.desired && (i == 1 || i == 2)) {
			s.Scope = "local_only"
		}
		if i == 4 {
			s.Scope = "output"
		}
		if !choice.desired {
			s.Reason = "not_selected"
		} else if i < len(bindings) && bindings[i].Available {
			b := bindings[i]
			b.Address = b.Address.Unmap()
			if !validListenerBinding(b) || (i == 0 && !b.Address.IsLoopback()) {
				s.Reason = "invalid_binding"
			} else {
				s.Binding = &b
				s.Status, s.Reason = "pending", "listener_application_pending"
			}
		}
		p.Surfaces = append(p.Surfaces, s)
	}
	for i := 0; i < len(bindings); i++ {
		for j := i + 1; j < len(bindings); j++ {
			x, y := p.Surfaces[i].Binding, p.Surfaces[j].Binding
			if x == nil || y == nil {
				continue
			}
			sameEndpoint := x.Address == y.Address && x.Port == y.Port
			reason := ""
			switch {
			case x.Owner == y.Owner && !sameEndpoint:
				reason = "inconsistent_listener_owner"
			case sameEndpoint && x.Owner == y.Owner && j != 3:
				// Local, GraphQL and MCP can be routes on one host listener.
			case listenerOverlap(*x, *y):
				reason = "listener_collision"
			}
			if reason != "" {
				p.Conflicts = append(p.Conflicts, ListenerConflict{p.Surfaces[i].Surface, p.Surfaces[j].Surface, reason})
				p.Surfaces[i].Status, p.Surfaces[i].Reason = "withheld", reason
				p.Surfaces[j].Status, p.Surfaces[j].Reason = "withheld", reason
			}
		}
	}
	return p
}

func validListenerBinding(b ListenerBinding) bool {
	if len(b.Owner) == 0 || len(b.Owner) > 64 || !b.Address.IsValid() || b.Address.Zone() != "" || b.Address.IsMulticast() || b.Port == 0 {
		return false
	}
	for _, c := range b.Owner {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func listenerOverlap(x, y ListenerBinding) bool {
	// Treat wildcard IPv6 as potentially dual-stack; no socket option is inferred.
	wildcardOverlaps := func(wildcard, other netip.Addr) bool {
		return wildcard.IsUnspecified() && (wildcard.Is6() || other.Is4())
	}
	return x.Port == y.Port && (x.Address == y.Address || wildcardOverlaps(x.Address, y.Address) || wildcardOverlaps(y.Address, x.Address))
}
