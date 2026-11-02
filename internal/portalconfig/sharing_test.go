package portalconfig_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func m29Record() portalconfig.Record {
	return portalconfig.Record{Format: portalconfig.Format, Revision: "0", Drivers: []portalconfig.Driver{}}
}

func m29Availability() portalconfig.ListenerAvailability {
	return portalconfig.ListenerAvailability{
		Local:        portalconfig.ListenerBinding{Owner: "local", Address: netip.IPv6Loopback(), Port: 1, Available: true},
		GraphQL:      portalconfig.ListenerBinding{Owner: "graphql", Address: netip.IPv4Unspecified(), Port: 2, Available: true},
		MCP:          portalconfig.ListenerBinding{Owner: "mcp", Address: netip.IPv4Unspecified(), Port: 3, Available: true},
		AdapterProxy: portalconfig.ListenerBinding{Owner: "proxy", Address: netip.IPv4Unspecified(), Port: 4, Available: true},
	}
}

func TestM29EverySelectionDeterministicAndPending(t *testing.T) {
	for mask := 0; mask < 16; mask++ {
		r := m29Record()
		r.Sharing.GraphQL, r.Sharing.MCP = mask&1 != 0, mask&2 != 0
		r.AdapterProxy, r.OutputBindings.Matter = mask&4 != 0, mask&8 != 0
		a := m29Availability()
		before := a
		p := portalconfig.DeriveListenerPlan(r, a)
		if !reflect.DeepEqual(p, portalconfig.DeriveListenerPlan(r, a)) || a != before || len(p.Surfaces) != 5 || len(p.Conflicts) != 0 || p.Status != "pending" {
			t.Fatalf("mask %d: non-deterministic or unbounded plan: %+v", mask, p)
		}
		for i, want := range []bool{true, r.Sharing.GraphQL, r.Sharing.MCP, r.AdapterProxy, r.OutputBindings.Matter} {
			s := p.Surfaces[i]
			if s.Desired != want || s.Effective {
				t.Fatalf("mask %d surface %d: selection coupled or effective: %+v", mask, i, s)
			}
			if i < 4 && want && (s.Status != "pending" || s.Reason != "listener_application_pending" || s.Binding == nil) {
				t.Fatalf("application not pending: %+v", s)
			}
			if !want && (s.Status != "withheld" || s.Reason != "not_selected" || s.Binding != nil) {
				t.Fatalf("unselected binding planned: %+v", s)
			}
			if (i == 0 || (!want && (i == 1 || i == 2))) && s.Scope != "local_only" {
				t.Fatalf("local-only route lost: %+v", s)
			}
		}
		if s := p.Surfaces[4]; s.Scope != "output" || s.Status != "withheld" || s.Binding != nil || (s.Desired && s.Reason != "evidence_unavailable") {
			t.Fatalf("output evidence invented: %+v", s)
		}
	}
}

func TestM29BindingIdentityAndAvailability(t *testing.T) {
	r := m29Record()
	r.Sharing = portalconfig.Sharing{GraphQL: true, MCP: true}
	// Owner, address and port are the complete listener key. Each is varied
	// independently to catch omission from cohosting/collision comparisons.
	for _, field := range []string{"same", "owner", "address", "port", "available"} {
		t.Run(field, func(t *testing.T) {
			a := m29Availability()
			a.MCP = a.GraphQL
			wantConflict := false
			switch field {
			case "owner":
				a.MCP.Owner = "other"
				wantConflict = true
			case "address":
				a.MCP.Address = netip.IPv6Loopback()
				wantConflict = true
			case "port":
				a.MCP.Port++
				wantConflict = true
			case "available":
				a.MCP.Available = false
			}
			p := portalconfig.DeriveListenerPlan(r, a)
			if (len(p.Conflicts) != 0) != wantConflict {
				t.Fatalf("%s: %+v", field, p)
			}
			if wantConflict {
				if len(p.Conflicts) != 1 || p.Conflicts[0].First != "graphql" || p.Conflicts[0].Second != "mcp" || p.Surfaces[1].Status != "withheld" || p.Surfaces[2].Status != "withheld" {
					t.Fatalf("conflict not explicit: %+v", p)
				}
			}
			if field == "available" && (p.Surfaces[2].Binding != nil || p.Surfaces[2].Status != "withheld" || p.Surfaces[2].Reason != "evidence_unavailable") {
				t.Fatal("missing binding admitted", p)
			}
		})
	}
}

func TestM29MissingBindingsAndLocalSeparation(t *testing.T) {
	r := m29Record()
	r.Sharing = portalconfig.Sharing{GraphQL: true, MCP: true}
	r.AdapterProxy = true
	for i := 0; i < 4; i++ {
		a := m29Availability()
		bindings := []*portalconfig.ListenerBinding{&a.Local, &a.GraphQL, &a.MCP, &a.AdapterProxy}
		bindings[i].Available = false
		p := portalconfig.DeriveListenerPlan(r, a)
		for j := 0; j < 4; j++ {
			want := "pending"
			if j == i {
				want = "withheld"
			}
			if p.Surfaces[j].Status != want || p.Surfaces[j].Effective {
				t.Fatalf("missing binding %d damaged sibling %d: %+v", i, j, p)
			}
		}
	}
	a := m29Availability()
	a.GraphQL, a.MCP = a.Local, a.Local
	p := portalconfig.DeriveListenerPlan(r, a)
	if len(p.Conflicts) != 0 {
		t.Fatal("shared local HTTP listener collided", p)
	}
	// IPv4-mapped and native IPv4 forms name the same endpoint.
	mapped := [16]byte{10: 0xff, 11: 0xff}
	a = m29Availability()
	a.MCP = a.GraphQL
	a.MCP.Address = netip.AddrFrom16(mapped)
	p = portalconfig.DeriveListenerPlan(r, a)
	if len(p.Conflicts) != 0 || p.Surfaces[2].Binding.Address != a.GraphQL.Address {
		t.Fatal("mapped address was not normalized", p)
	}
}

func TestM29CollisionsAndInvalidBindings(t *testing.T) {
	r := m29Record()
	r.Sharing.GraphQL, r.AdapterProxy = true, true
	for _, field := range []string{"proxy_cohost", "wildcard", "distinct_address", "distinct_port", "zero_port", "invalid_address", "zone", "mapped_zone", "multicast", "empty_owner", "unsafe_owner", "long_owner", "nonlocal_portal"} {
		t.Run(field, func(t *testing.T) {
			a := m29Availability()
			wantConflicts, invalidIndex := 0, -1
			switch field {
			case "proxy_cohost":
				a.AdapterProxy = a.GraphQL
				wantConflicts = 1
			case "wildcard":
				a.AdapterProxy.Port = a.GraphQL.Port
				a.AdapterProxy.Address = netip.AddrFrom4([4]byte{127, 0, 0, 1})
				wantConflicts = 1
			case "distinct_address":
				a.AdapterProxy.Port = a.GraphQL.Port
				a.GraphQL.Address = netip.IPv6Loopback()
				a.AdapterProxy.Address = a.GraphQL.Address.Next()
			case "distinct_port":
				a.AdapterProxy.Address = a.GraphQL.Address
			case "zero_port":
				a.GraphQL.Port = 0
				invalidIndex = 1
			case "invalid_address":
				a.GraphQL.Address = netip.Addr{}
				invalidIndex = 1
			case "zone":
				a.GraphQL.Address = netip.IPv6Loopback().WithZone("synthetic")
				invalidIndex = 1
			case "mapped_zone":
				a.GraphQL.Address = netip.AddrFrom16([16]byte{10: 0xff, 11: 0xff, 12: 127, 15: 1}).WithZone("synthetic")
				invalidIndex = 1
			case "multicast":
				a.GraphQL.Address = netip.AddrFrom16([16]byte{0xff, 2})
				invalidIndex = 1
			case "empty_owner":
				a.GraphQL.Owner = ""
				invalidIndex = 1
			case "unsafe_owner":
				a.GraphQL.Owner = "private_marker/"
				invalidIndex = 1
			case "long_owner":
				a.GraphQL.Owner = string(bytes.Repeat([]byte("x"), 65))
				invalidIndex = 1
			case "nonlocal_portal":
				a.Local.Address = netip.IPv4Unspecified()
			}
			p := portalconfig.DeriveListenerPlan(r, a)
			if len(p.Conflicts) != wantConflicts {
				t.Fatalf("conflicts: %+v", p)
			}
			if field == "nonlocal_portal" {
				local := p.Surfaces[0]
				if local.Binding == nil || *local.Binding != a.Local || local.Scope != "shared" || local.Status != "pending" || local.Reason != "listener_application_pending" || local.Effective {
					t.Fatalf("host wildcard binding misreported: %+v", local)
				}
			}
			if invalidIndex >= 0 {
				s := p.Surfaces[invalidIndex]
				if s.Reason != "invalid_binding" || s.Status != "withheld" || s.Binding != nil || s.Effective {
					t.Fatal("invalid binding admitted", s)
				}
			}
		})
	}
}

func TestM29WildcardAddressFamilies(t *testing.T) {
	r := m29Record()
	r.Sharing = portalconfig.Sharing{GraphQL: true, MCP: true}
	v4Loopback := netip.AddrFrom4([4]byte{127, 0, 0, 1})
	for _, tc := range []struct {
		name          string
		first, second netip.Addr
		collision     bool
	}{
		{"ipv4_wildcard_ipv6", netip.IPv4Unspecified(), netip.IPv6Loopback(), false},
		{"ipv4_wildcard_ipv4", netip.IPv4Unspecified(), v4Loopback, true},
		{"ipv6_wildcard_ipv4", netip.IPv6Unspecified(), v4Loopback, true},
		{"ipv6_wildcard_ipv6", netip.IPv6Unspecified(), netip.IPv6Loopback(), true},
		{"both_wildcards", netip.IPv4Unspecified(), netip.IPv6Unspecified(), true},
		{"distinct_families", v4Loopback, netip.IPv6Loopback(), false},
		{"mapped_ipv4_wildcard_ipv6", netip.AddrFrom16(netip.IPv4Unspecified().As16()), netip.IPv6Loopback(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, reverse := range []bool{false, true} {
				a := m29Availability()
				a.GraphQL.Address, a.MCP.Address = tc.first, tc.second
				if reverse {
					a.GraphQL.Address, a.MCP.Address = a.MCP.Address, a.GraphQL.Address
				}
				a.GraphQL.Port, a.MCP.Port = 8080, 8080
				p := portalconfig.DeriveListenerPlan(r, a)
				want := 0
				if tc.collision {
					want = 1
				}
				if len(p.Conflicts) != want {
					t.Fatalf("reverse=%t: conflicts: %+v", reverse, p)
				}
				for _, s := range p.Surfaces[1:3] {
					status, reason := "pending", "listener_application_pending"
					if tc.collision {
						status, reason = "withheld", "listener_collision"
					}
					if s.Status != status || s.Reason != reason || s.Effective || s.Binding == nil {
						t.Fatalf("reverse=%t: unexpected surface: %+v", reverse, s)
					}
				}
			}
		})
	}
}

func TestM29ConfigAPIValidationAndPending(t *testing.T) {
	for _, mode := range []string{"ready", "missing", "collision", "invalid", "mapped_zone", "separate_families"} {
		t.Run(mode, func(t *testing.T) {
			accepted := m29Record()
			r := accepted
			r.Sharing = portalconfig.Sharing{GraphQL: true, MCP: true}
			r.AdapterProxy, r.OutputBindings.Matter = true, true
			a := m29Availability()
			if mode == "separate_families" {
				a.GraphQL.Port, a.MCP.Port = 8080, 8080
				a.MCP.Address = netip.IPv6Loopback()
			}
			if mode == "collision" {
				a.MCP = a.GraphQL
				a.MCP.Owner = "other"
			}
			if mode == "invalid" {
				a.GraphQL.Port = 0
			}
			if mode == "mapped_zone" {
				a.GraphQL.Address = netip.AddrFrom16([16]byte{10: 0xff, 11: 0xff, 12: 127, 15: 1}).WithZone("synthetic")
			}
			updates := 0
			callbacks := &portal.ConfigCallbacks{
				Read: func(context.Context) (portalconfig.Record, error) { return accepted, nil },
				Update: func(_ context.Context, next portalconfig.Record) (portalconfig.Record, error) {
					updates++
					accepted = next
					return accepted, nil
				},
			}
			if mode != "missing" {
				callbacks.Bindings = func(context.Context) portalconfig.ListenerAvailability { return a }
			}
			h := portal.NewHandler(portal.Options{Config: callbacks})
			body, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/api/v1/config", bytes.NewReader(body)))
			var view struct {
				State struct {
					Sharing struct {
						GraphQL portalconfig.PlannedSurface `json:"graphql"`
						MCP     portalconfig.PlannedSurface `json:"mcp"`
					} `json:"sharing"`
					AdapterProxy portalconfig.PlannedSurface `json:"adapter_proxy"`
					Outputs      struct {
						Matter portalconfig.PlannedSurface `json:"matter"`
					} `json:"output_bindings"`
				} `json:"state"`
				Desired     portalconfig.Record       `json:"desired"`
				Plan        portalconfig.ListenerPlan `json:"listener_plan"`
				Application string                    `json:"listener_application"`
				Error       *portalconfig.Error       `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil {
				t.Fatal(err)
			}
			if mode == "collision" || mode == "invalid" || mode == "mapped_zone" {
				code := "listener_conflict"
				if mode == "invalid" || mode == "mapped_zone" {
					code = "invalid_binding"
				}
				if w.Code != http.StatusBadRequest || updates != 0 || view.Desired.Sharing.GraphQL || view.Error == nil || view.Error.Code != code {
					t.Fatalf("invalid edit persisted: %d %s", w.Code, w.Body.String())
				}
				return
			}
			if w.Code != http.StatusOK || updates != 1 || view.Application != "pending" || view.Plan.Status != "pending" || len(view.Plan.Surfaces) != 5 {
				t.Fatalf("missing pending plan: %d %s", w.Code, w.Body.String())
			}
			for i, state := range []portalconfig.PlannedSurface{view.State.Sharing.GraphQL, view.State.Sharing.MCP, view.State.AdapterProxy, view.State.Outputs.Matter} {
				if !reflect.DeepEqual(state, view.Plan.Surfaces[i+1]) {
					t.Fatal("API state did not consume plan", w.Body.String())
				}
			}
			for _, s := range view.Plan.Surfaces {
				if s.Effective {
					t.Fatal("planner advertised activation", s)
				}
			}
			if mode == "missing" && view.Plan.Surfaces[1].Reason != "evidence_unavailable" {
				t.Fatal("absent host evidence admitted")
			}
			w = httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config", nil))
			if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil {
				t.Fatal(err)
			}
			if w.Code != http.StatusOK || updates != 1 || !view.Desired.Sharing.GraphQL || view.Application != "pending" {
				t.Fatal("GET changed selection or applied listeners", w.Body.String())
			}
			if mode == "ready" {
				a.GraphQL.Available = false
				w = httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config", nil))
				if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil {
					t.Fatal(err)
				}
				if !view.Desired.Sharing.GraphQL || view.Plan.Surfaces[1].Status != "withheld" || view.Plan.Surfaces[2].Status != "pending" || updates != 1 {
					t.Fatal("evidence loss changed desired or sibling state", w.Body.String())
				}
				a.GraphQL = a.MCP
				a.GraphQL.Owner = "other"
				w = httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config", nil))
				if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil {
					t.Fatal(err)
				}
				if len(view.Plan.Conflicts) != 1 || view.Plan.Surfaces[1].Status != "withheld" || view.Plan.Surfaces[2].Status != "withheld" {
					t.Fatal("GET did not report current conflicts", w.Body.String())
				}
			}
		})
	}
}

func TestM29HostLocalAddressPreservesValidation(t *testing.T) {
	for _, invalid := range []string{"zero_port", "empty_owner", "invalid_address", "zone", "multicast"} {
		t.Run(invalid, func(t *testing.T) {
			a := m29Availability()
			switch invalid {
			case "zero_port":
				a.Local.Port = 0
			case "empty_owner":
				a.Local.Owner = ""
			case "invalid_address":
				a.Local.Address = netip.Addr{}
			case "zone":
				a.Local.Address = netip.IPv6Unspecified().WithZone("synthetic")
			case "multicast":
				a.Local.Address = netip.AddrFrom16([16]byte{0xff, 2})
			}
			plan := portalconfig.DeriveListenerPlan(m29Record(), a)
			local := plan.Surfaces[0]
			if local.Binding != nil || local.Reason != "invalid_binding" || local.Status != "withheld" || local.Effective {
				t.Fatalf("host address bypassed validation: %+v", local)
			}
		})
	}
}
