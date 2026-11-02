package portalconfig_test

import (
	"net/netip"
	"reflect"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
)

func o11Fixture() (portalconfig.Record, portalconfig.ListenerAvailability, []portalconfig.O11ListenerObservation) {
	r := portalconfig.Record{Format: portalconfig.Format, Revision: "7", Sharing: portalconfig.Sharing{GraphQL: true, MCP: true}, AdapterProxy: true, OutputBindings: portalconfig.Outputs{Matter: true}}
	bindings := [4]portalconfig.ListenerBinding{}
	names := [4]string{"local", "graphql", "mcp", "adapter_proxy"}
	observations := make([]portalconfig.O11ListenerObservation, 4)
	for i, name := range names {
		bindings[i] = portalconfig.ListenerBinding{Owner: name, Address: netip.IPv6Loopback(), Port: uint16(i + 1), Available: true}
		observations[i] = portalconfig.O11ListenerObservation{Surface: name, AppliedRevision: r.Revision, Binding: bindings[i]}
	}
	return r, portalconfig.ListenerAvailability{Local: bindings[0], GraphQL: bindings[1], MCP: bindings[2], AdapterProxy: bindings[3]}, observations
}

func TestO11IdentityAndSelection(t *testing.T) {
	// Every case starts with a valid control and changes exactly one field.
	for _, field := range []string{"revision", "surface", "owner", "address", "port", "desired", "observed_availability", "planned_availability"} {
		t.Run(field, func(t *testing.T) {
			r, a, observations := o11Fixture()
			if s := portalconfig.O11DeriveListenerReadback(r, a, observations).Surfaces[1]; !s.Effective || s.Status != "effective" || s.Reason != "listener_available" {
				t.Fatalf("invalid control: %+v", s)
			}
			switch field {
			case "revision":
				observations[1].AppliedRevision = "6"
			case "surface":
				observations[1].Surface = "unknown"
			case "owner":
				observations[1].Binding.Owner = "other"
			case "address":
				observations[1].Binding.Address = netip.IPv4Unspecified()
			case "port":
				observations[1].Binding.Port++
			case "desired":
				r.Sharing.GraphQL = false
			case "observed_availability":
				observations[1].Binding.Available = false
			case "planned_availability":
				a.GraphQL.Available = false
			}
			p := portalconfig.O11DeriveListenerReadback(r, a, observations)
			if p.Surfaces[1].Effective {
				t.Fatalf("%s admitted: %+v", field, p.Surfaces[1])
			}
			for _, i := range []int{0, 2, 3} {
				if !p.Surfaces[i].Effective {
					t.Fatalf("%s damaged sibling %d: %+v", field, i, p)
				}
			}
		})
	}
}

func TestO11MissingConflictingAndRevoked(t *testing.T) {
	for _, kind := range []string{"missing", "duplicate", "conflict", "revoked", "unapplied", "invalid_revision", "invalid_binding"} {
		t.Run(kind, func(t *testing.T) {
			r, a, observations := o11Fixture()
			wantStatus, wantReason := "withheld", "evidence_unavailable"
			switch kind {
			case "missing":
				observations = nil
			case "duplicate":
				observations = append(observations, observations[1])
				wantReason = "conflicting_readback"
			case "conflict":
				a.MCP = a.GraphQL
				a.MCP.Owner = "other"
				wantReason = "listener_collision"
			case "revoked":
				observations[1].Binding.Available = false
			case "unapplied":
				observations[1].AppliedRevision = ""
				wantStatus, wantReason = "pending", "listener_application_pending"
			case "invalid_revision":
				r.Revision = ""
				observations[1].AppliedRevision = ""
				wantStatus, wantReason = "pending", "listener_application_pending"
			case "invalid_binding":
				observations[1].Binding.Address = observations[1].Binding.Address.WithZone("synthetic")
				wantStatus, wantReason = "pending", "listener_application_pending"
			}
			s := portalconfig.O11DeriveListenerReadback(r, a, observations).Surfaces[1]
			if s.Effective || s.Status != wantStatus || s.Reason != wantReason {
				t.Fatalf("%s: %+v", kind, s)
			}
		})
	}
}

func TestO11LocalIndependenceAndOutputWithholding(t *testing.T) {
	for mask := 0; mask < 4; mask++ {
		r, a, observations := o11Fixture()
		r.Sharing.GraphQL, r.Sharing.MCP = mask&1 != 0, mask&2 != 0
		// Unknown external readback must not alter the observed local surface.
		p := portalconfig.O11DeriveListenerReadback(r, a, observations[:1])
		if !p.Surfaces[0].Effective || p.Surfaces[0].Scope != "local_only" {
			t.Fatal(p)
		}
		for _, i := range []int{1, 2} {
			if p.Surfaces[i].Effective {
				t.Fatal(p)
			}
		}
		// Even a fabricated Matter listener observation cannot establish publication.
		observations = append(observations, portalconfig.O11ListenerObservation{Surface: "matter", AppliedRevision: r.Revision, Binding: a.Local})
		p = portalconfig.O11DeriveListenerReadback(r, a, observations)
		s := p.Surfaces[4]
		if s.Effective || s.Status != "withheld" || s.Reason != "evidence_unavailable" || s.Binding != nil {
			t.Fatal(s)
		}
	}
}

func TestO11WithdrawalPending(t *testing.T) {
	for _, index := range []int{1, 2, 3} {
		r, a, observations := o11Fixture()
		switch index {
		case 1:
			r.Sharing.GraphQL = false
		case 2:
			r.Sharing.MCP = false
		case 3:
			r.AdapterProxy = false
		}
		s := portalconfig.O11DeriveListenerReadback(r, a, observations).Surfaces[index]
		if s.Effective || s.Desired || s.Status != "pending" || s.Reason != "listener_withdrawal_pending" {
			t.Fatal(s)
		}
		observations[index].Binding.Available = false
		s = portalconfig.O11DeriveListenerReadback(r, a, observations).Surfaces[index]
		if s.Effective || s.Status != "withheld" || s.Reason != "not_selected" {
			t.Fatal(s)
		}
	}
}

func TestO11DetachedAndPlannerUnchanged(t *testing.T) {
	r, a, observations := o11Fixture()
	before := append([]portalconfig.O11ListenerObservation(nil), observations...)
	plan := portalconfig.DeriveListenerPlan(r, a)
	p := portalconfig.O11DeriveListenerReadback(r, a, observations)
	if !reflect.DeepEqual(before, observations) || !reflect.DeepEqual(plan, portalconfig.DeriveListenerPlan(r, a)) {
		t.Fatal("inputs or planner changed")
	}
	for i := range plan.Surfaces {
		if plan.Surfaces[i].Effective {
			t.Fatal("planner became effective")
		}
	}
	saved := *p.Surfaces[1].Binding
	observations[1].Binding.Owner = "changed"
	a.GraphQL.Port++
	if *p.Surfaces[1].Binding != saved {
		t.Fatal("result aliases inputs")
	}
	p.Surfaces[1].Binding.Owner = "result"
	p.Surfaces[0].Surface = "changed"
	if observations[1].Binding.Owner != "changed" {
		t.Fatal("result changed input")
	}
	fresh := portalconfig.O11DeriveListenerReadback(r, portalconfig.ListenerAvailability{Local: before[0].Binding, GraphQL: before[1].Binding, MCP: before[2].Binding, AdapterProxy: before[3].Binding}, before)
	if fresh.Surfaces[0].Surface != "local" || fresh.Surfaces[1].Binding.Owner != saved.Owner {
		t.Fatal("results alias each other")
	}
}
