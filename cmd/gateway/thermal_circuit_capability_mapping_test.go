package main

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

// Every admission-positive fixture is synthetic and supplies no qualification
// evidence for an installation. Expected service IDs are independent of mapping.
func thermalCircuitTestInput() thermalCircuitMappingInput {
	id := thermalCircuitIdentity{Asset: "asset:a", Service: "service:a", Capability: "capability:a", Source: "source:a", Epoch: "epoch:a", Generation: "1", Binding: "binding:a", BindingRevision: "1", AssociationRevision: "1", ControllerContext: "controller:a", Family: "VRC720-family"}
	sel := thermalCircuitSelector{PB: 0xb5, SB: 0x24, Opcode: 2, Group: 2, Register: 2, RequestDirection: "initiator_to_target", ValueDirection: "target_to_initiator"}
	receipt := semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "10"}
	role := thermalCircuitRole{Identity: id, Selector: sel, Role: "ordinary", Qualification: semreg.QualificationQualified, Presence: true, ReadBinding: true, FamilyApplicable: true, Unambiguous: true, QualificationRef: "synthetic:qualification", RoleEvidenceRef: "synthetic:role", ReceivedAt: semreg.TimePoint{UnixNanoseconds: "10"}, Receipt: receipt}
	evidenceDigest := sha256.Sum256([]byte("synthetic circuit evidence"))
	o := thermalCircuitObservation{Identity: id, Selector: sel, Raw: []byte{1, 0}, Decoded: 1, Validity: semreg.ValidityGood, Qualification: semreg.QualificationQualified, EvidenceStatus: "Unknown", Evidence: semreg.EvidenceRef{Digest: semreg.Digest(fmt.Sprintf("%x", evidenceDigest))}, ReceivedAt: semreg.TimePoint{UnixNanoseconds: "10"}, Receipt: receipt, Present: true}
	cool := o
	cool.Selector.Register, cool.Raw = 6, []byte{1}
	return thermalCircuitMappingInput{Candidates: []thermalCircuitCandidate{{Identity: id, TypeSelector: sel, Role: role}}, Observations: []thermalCircuitObservation{o, cool}, Requested: []projection.RequestedItem{{Kind: projection.ItemCapability, ItemID: "thermal.capability.read.circuit"}}, EvaluatedAt: semreg.TimePoint{UnixNanoseconds: "20"}, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "20"}, FreshForNS: "20"}
}

func thermalCircuitTestSibling(in *thermalCircuitMappingInput) {
	c := in.Candidates[0]
	c.Identity.Asset, c.Identity.Service, c.Identity.Capability = "asset:b", "service:b", "capability:b"
	c.Role.Identity = c.Identity
	in.Candidates = append(in.Candidates, c)
	for _, o := range append([]thermalCircuitObservation(nil), in.Observations...) {
		o.Identity = c.Identity
		o.Raw = append([]byte(nil), o.Raw...)
		in.Observations = append(in.Observations, o)
	}
}

func thermalCircuitTestOutcome(t *testing.T, out thermalCircuitMappingResult, service semreg.ServiceInstanceID, admitted bool, loss string) thermalCircuitSourceOutcome {
	t.Helper()
	for _, s := range out.Sources {
		if s.Candidate.Identity.Service == service {
			if (s.Service != nil) != admitted || (s.Capability != nil) != admitted || s.Loss != loss {
				t.Fatalf("service %s: %+v", service, s)
			}
			if admitted {
				if err := s.Service.Validate(); err != nil {
					t.Fatalf("admitted service %s is invalid: %v", service, err)
				}
			}
			return s
		}
	}
	t.Fatalf("missing outcome for %s", service)
	return thermalCircuitSourceOutcome{}
}

func TestThermalCircuitCapabilityMappingNativeOnly(t *testing.T) {
	for _, code := range []uint16{0, 1, 2, 3, 4, 7, 255, 65535} {
		for _, flag := range []uint16{0, 1, 2} {
			t.Run(fmt.Sprintf("type%d/cooling%d", code, flag), func(t *testing.T) {
				in := thermalCircuitTestInput()
				in.Observations[0].Decoded, in.Observations[0].Raw = code, []byte{byte(code), byte(code >> 8)}
				in.Observations[1].Decoded, in.Observations[1].Raw = flag, []byte{byte(flag)}
				in.Requested = append(in.Requested, projection.RequestedItem{Kind: projection.ItemFact, ItemID: "native:type"}, projection.RequestedItem{Kind: projection.ItemFact, ItemID: "native:cooling"})
				out := mapThermalCircuitCapabilities(in)
				loss := ""
				if code == 0 {
					loss = "inactive"
				}
				s := thermalCircuitTestOutcome(t, out, "service:a", code != 0, loss)
				coolLoss := "native_only"
				if flag == 2 {
					coolLoss = "invalid"
				}
				if s.CoolingLoss != coolLoss {
					t.Fatalf("cooling loss %q", s.CoolingLoss)
				}
				typeLoss := "native_only"
				if code > 4 {
					typeLoss = "unknown_code"
				}
				if s.TypeLoss != typeLoss {
					t.Fatalf("type loss %q", s.TypeLoss)
				}
				for _, d := range out.Dispositions {
					if d.Kind == projection.ItemFact && d.Outcome != projection.ProjectionUnsupported {
						t.Fatalf("invented field: %+v", d)
					}
				}
				if code != 0 && (s.Service.Definition.ID != "thermal.service.circuit" || s.Capability.Definition.ID != "thermal.capability.read.circuit") {
					t.Fatal("type invented a role")
				}
				if len(out.Native) != 2 {
					t.Fatal("native observations lost")
				}
				for _, original := range in.Observations {
					found := false
					for _, retained := range out.Native {
						if reflect.DeepEqual(original, retained) {
							found = true
						}
					}
					if !found {
						t.Fatal("native type/flag rewritten or dropped")
					}
				}
			})
		}
	}
}

func TestThermalCircuitCapabilityMappingIdentityFences(t *testing.T) {
	axes := []struct {
		name   string
		change func(*thermalCircuitIdentity)
	}{
		{"asset", func(i *thermalCircuitIdentity) { i.Asset = "asset:changed" }},
		{"service", func(i *thermalCircuitIdentity) { i.Service = "service:changed" }},
		{"capability", func(i *thermalCircuitIdentity) { i.Capability = "capability:changed" }},
		{"source", func(i *thermalCircuitIdentity) { i.Source = "source:changed" }},
		{"epoch", func(i *thermalCircuitIdentity) { i.Epoch = "epoch:changed" }},
		{"generation", func(i *thermalCircuitIdentity) { i.Generation = "2" }},
		{"binding", func(i *thermalCircuitIdentity) { i.Binding = "binding:changed" }},
		{"binding_revision", func(i *thermalCircuitIdentity) { i.BindingRevision = "2" }},
		{"association_revision", func(i *thermalCircuitIdentity) { i.AssociationRevision = "2" }},
		{"controller", func(i *thermalCircuitIdentity) { i.ControllerContext = "controller:changed" }},
		{"family", func(i *thermalCircuitIdentity) { i.Family = "other-family" }},
	}
	for _, axis := range axes {
		for _, where := range []string{"observation", "role", "replacement"} {
			t.Run(axis.name+"/"+where, func(t *testing.T) {
				in := thermalCircuitTestInput()
				thermalCircuitTestSibling(&in)
				loss := "missing"
				switch where {
				case "observation":
					axis.change(&in.Observations[0].Identity)
					axis.change(&in.Observations[1].Identity)
				case "role":
					axis.change(&in.Candidates[0].Role.Identity)
					loss = "role_provenance"
				case "replacement":
					axis.change(&in.Candidates[0].Identity)
					in.Candidates[0].Role.Identity = in.Candidates[0].Identity
				}
				out := mapThermalCircuitCapabilities(in)
				s := thermalCircuitTestOutcome(t, out, in.Candidates[0].Identity.Service, false, loss)
				if where != "role" && s.CoolingLoss != "missing" {
					t.Fatal("old cooling crossed fence")
				}
				if where != "role" {
					historical := 0
					for _, o := range out.Native {
						if o.Historical {
							historical++
						}
					}
					if historical != 2 {
						t.Fatal("replaced evidence not historical")
					}
				}
				thermalCircuitTestOutcome(t, out, "service:b", true, "")
				if out.Dispositions[0].Outcome != projection.ProjectionExact || len(out.Native) != 4 {
					t.Fatal("source loss erased sibling or history")
				}
			})
		}
	}
}

func TestThermalCircuitCapabilityMappingSelectorFences(t *testing.T) {
	axes := []struct {
		name   string
		change func(*thermalCircuitSelector)
	}{
		{"pb", func(s *thermalCircuitSelector) { s.PB++ }}, {"sb", func(s *thermalCircuitSelector) { s.SB++ }},
		{"opcode", func(s *thermalCircuitSelector) { s.Opcode = 6 }}, {"operation_type", func(s *thermalCircuitSelector) { s.OperationType++ }},
		{"group", func(s *thermalCircuitSelector) { s.Group++ }}, {"instance", func(s *thermalCircuitSelector) { s.Instance++ }},
		{"register", func(s *thermalCircuitSelector) { s.Register++ }}, {"initiator", func(s *thermalCircuitSelector) { s.Initiator++ }},
		{"destination", func(s *thermalCircuitSelector) { s.Destination++ }},
		{"request_direction", func(s *thermalCircuitSelector) { s.RequestDirection = "reverse" }},
		{"value_direction", func(s *thermalCircuitSelector) { s.ValueDirection = "reverse" }},
	}
	for _, axis := range axes {
		for _, where := range []string{"observation", "role"} {
			t.Run(axis.name+"/"+where, func(t *testing.T) {
				in := thermalCircuitTestInput()
				thermalCircuitTestSibling(&in)
				loss := "missing"
				if where == "observation" {
					axis.change(&in.Observations[0].Selector)
				} else {
					axis.change(&in.Candidates[0].Role.Selector)
					loss = "role_provenance"
				}
				out := mapThermalCircuitCapabilities(in)
				thermalCircuitTestOutcome(t, out, "service:a", false, loss)
				thermalCircuitTestOutcome(t, out, "service:b", true, "")
			})
		}
	}
}

func TestThermalCircuitCapabilityMappingIndependentAdmission(t *testing.T) {
	cases := []struct {
		name, loss string
		change     func(*thermalCircuitMappingInput)
	}{
		{"invalid", "invalid", func(i *thermalCircuitMappingInput) { i.Observations[0].Validity = semreg.ValidityBad }},
		{"unqualified", "unqualified", func(i *thermalCircuitMappingInput) { i.Observations[0].Qualification = semreg.QualificationCandidate }},
		{"stale", "stale", func(i *thermalCircuitMappingInput) { i.EvaluateMonotonic.Nanoseconds = "31" }},
		{"clock_epoch", "stale", func(i *thermalCircuitMappingInput) { i.Observations[0].Receipt.ClockEpochID = "clock:other" }},
		{"future_receipt", "stale", func(i *thermalCircuitMappingInput) { i.Observations[0].Receipt.Nanoseconds = "21" }},
		{"malformed_clock", "stale", func(i *thermalCircuitMappingInput) { i.Observations[0].Receipt.Nanoseconds = "invalid" }},
		{"missing", "missing", func(i *thermalCircuitMappingInput) { i.Observations[0].Present = false }},
		{"historical", "missing", func(i *thermalCircuitMappingInput) { i.Observations[0].Historical = true }},
		{"raw_width", "invalid", func(i *thermalCircuitMappingInput) { i.Observations[0].Raw = []byte{1} }},
		{"raw_decode", "invalid", func(i *thermalCircuitMappingInput) { i.Observations[0].Raw = []byte{2, 0} }},
		{"role_qualification", "unqualified_role", func(i *thermalCircuitMappingInput) { i.Candidates[0].Role.Qualification = semreg.QualificationUnknown }},
		{"presence", "unqualified_role", func(i *thermalCircuitMappingInput) { i.Candidates[0].Role.Presence = false }},
		{"read_binding", "unqualified_role", func(i *thermalCircuitMappingInput) { i.Candidates[0].Role.ReadBinding = false }},
		{"family", "unqualified_role", func(i *thermalCircuitMappingInput) { i.Candidates[0].Role.FamilyApplicable = false }},
		{"ambiguity", "unqualified_role", func(i *thermalCircuitMappingInput) { i.Candidates[0].Role.Unambiguous = false }},
		{"qualification_ref", "unqualified_role", func(i *thermalCircuitMappingInput) { i.Candidates[0].Role.QualificationRef = "" }},
		{"role_ref", "unqualified_role", func(i *thermalCircuitMappingInput) { i.Candidates[0].Role.RoleEvidenceRef = "" }},
		{"role_stale", "stale_role", func(i *thermalCircuitMappingInput) {
			i.Candidates[0].Role.Receipt.Nanoseconds = "0"
			i.FreshForNS = "15"
		}},
		{"unknown_role", "unknown_role", func(i *thermalCircuitMappingInput) { i.Candidates[0].Role.Role = "heuristic" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := thermalCircuitTestInput()
			c.change(&in)
			// Staleness is tested on the observation independently of role freshness.
			if c.name == "stale" {
				in.Candidates[0].Role.Receipt.Nanoseconds = "20"
			}
			out := mapThermalCircuitCapabilities(in)
			thermalCircuitTestOutcome(t, out, "service:a", false, c.loss)
			if out.Native[0].ReceivedAt.UnixNanoseconds != "10" {
				t.Fatal("evaluation refreshed original receipt")
			}
		})
	}
}

func TestThermalCircuitCapabilityMappingVirtualAndSparse(t *testing.T) {
	in := thermalCircuitTestInput()
	in.Candidates[0].TypeSelector.Instance = 9
	in.Candidates[0].Role.Selector.Instance = 9
	for i := range in.Observations {
		in.Observations[i].Selector.Instance = 9
	}
	thermalCircuitTestOutcome(t, mapThermalCircuitCapabilities(in), "service:a", false, "virtual_not_physical")
	in.Candidates[0].Role.Role = "heuristic"
	thermalCircuitTestOutcome(t, mapThermalCircuitCapabilities(in), "service:a", false, "unknown_role")
	in.Candidates[0].Role.Role = "virtual_water"
	in.Observations[0].Decoded, in.Observations[0].Raw = 0, []byte{0, 0}
	s := thermalCircuitTestOutcome(t, mapThermalCircuitCapabilities(in), "service:a", true, "")
	if s.Service.Definition.ID != "thermal.service.dhw" || s.Capability.Definition.ID != "thermal.capability.read.dhw" {
		t.Fatal("virtual identity became circuit")
	}
	in = thermalCircuitTestInput()
	thermalCircuitTestSibling(&in)
	in.Observations[0].Historical, in.Observations[1].Historical = true, true
	out := mapThermalCircuitCapabilities(in)
	thermalCircuitTestOutcome(t, out, "service:a", false, "missing")
	thermalCircuitTestOutcome(t, out, "service:b", true, "")
	if len(out.Native) != 4 {
		t.Fatal("sparse disappearance erased evidence")
	}
	in.Observations[0].Historical = false
	// Missing cooling preserves historical flag without withdrawing the service.
	s = thermalCircuitTestOutcome(t, mapThermalCircuitCapabilities(in), "service:a", true, "")
	if s.CoolingLoss != "missing" {
		t.Fatal("historical flag renewed")
	}
	in.Candidates[0].Role.Qualification = semreg.QualificationUnknown
	thermalCircuitTestOutcome(t, mapThermalCircuitCapabilities(in), "service:a", false, "unqualified_role")
}

func TestThermalCircuitCapabilityMappingConflictAccountingAndDetachment(t *testing.T) {
	in := thermalCircuitTestInput()
	thermalCircuitTestSibling(&in)
	in.Requested = append(in.Requested, in.Requested[0], projection.RequestedItem{Kind: projection.ItemOperation, ItemID: "thermal.operation.set_mode"})
	out := mapThermalCircuitCapabilities(in)
	if len(out.Dispositions) != 2 {
		t.Fatal("requested accounting")
	}
	if out.Dispositions[0].Outcome != projection.ProjectionExact || out.Dispositions[1].Outcome != projection.ProjectionUnsupported {
		t.Fatal("unsupported operation or lost service")
	}
	in.Observations[0].Raw[0] = 4
	in.Candidates[0].Role.RoleEvidenceRef = "changed"
	for _, o := range out.Native {
		if o.Raw[0] != 1 {
			t.Fatal("input alias")
		}
	}
	for _, s := range out.Sources {
		if s.Candidate.Role.RoleEvidenceRef != "synthetic:role" {
			t.Fatal("role provenance alias")
		}
	}
	out.Native[0].Raw[0] = 99
	out.Sources[0].Service.InstanceID = "changed"
	if in.Observations[0].Raw[0] != 4 || in.Candidates[0].Identity.Service != "service:a" {
		t.Fatal("output alias")
	}
	in = thermalCircuitTestInput()
	duplicate := in.Observations[0]
	duplicate.Decoded, duplicate.Raw = 2, []byte{2, 0}
	in.Observations = append(in.Observations, duplicate)
	thermalCircuitTestOutcome(t, mapThermalCircuitCapabilities(in), "service:a", false, "conflict")
	in = thermalCircuitTestInput()
	c := in.Candidates[0]
	c.Identity.Source = "source:b"
	c.Role.Identity = c.Identity
	in.Candidates = append(in.Candidates, c)
	for _, o := range append([]thermalCircuitObservation(nil), in.Observations...) {
		o.Identity = c.Identity
		in.Observations = append(in.Observations, o)
	}
	out = mapThermalCircuitCapabilities(in)
	for _, s := range out.Sources {
		if s.Loss != "conflict" || s.Service != nil {
			t.Fatal("arrival order winner")
		}
	}
	if out.Dispositions[0].Outcome != projection.ProjectionWithheld {
		t.Fatal("conflict projected")
	}
	in = thermalCircuitTestInput()
	thermalCircuitTestSibling(&in)
	want := mapThermalCircuitCapabilities(in)
	in.Candidates[0], in.Candidates[1] = in.Candidates[1], in.Candidates[0]
	for i, j := 0, len(in.Observations)-1; i < j; i, j = i+1, j-1 {
		in.Observations[i], in.Observations[j] = in.Observations[j], in.Observations[i]
	}
	if !reflect.DeepEqual(want, mapThermalCircuitCapabilities(in)) {
		t.Fatal("nondeterministic ordering")
	}
}

func TestThermalCircuitCapabilityMappingCoolingLossAndReceipt(t *testing.T) {
	cases := []struct {
		name, loss string
		change     func(*thermalCircuitObservation)
	}{
		{"missing", "missing", func(o *thermalCircuitObservation) { o.Present = false }},
		{"historical", "missing", func(o *thermalCircuitObservation) { o.Historical = true }},
		{"invalid", "invalid", func(o *thermalCircuitObservation) { o.Validity = semreg.ValidityBad }},
		{"unqualified", "unqualified", func(o *thermalCircuitObservation) { o.Qualification = semreg.QualificationCandidate }},
		{"stale", "stale", func(o *thermalCircuitObservation) { o.Receipt.Nanoseconds = "0" }},
		{"width", "invalid", func(o *thermalCircuitObservation) { o.Raw = []byte{1, 0} }},
		{"decode", "invalid", func(o *thermalCircuitObservation) { o.Decoded = 0 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := thermalCircuitTestInput()
			in.FreshForNS = "15"
			c.change(&in.Observations[1])
			out := mapThermalCircuitCapabilities(in)
			s := thermalCircuitTestOutcome(t, out, "service:a", true, "")
			if s.CoolingLoss != c.loss || s.TypeLoss != "native_only" {
				t.Fatal("cooling changed independent type/service")
			}
			if !reflect.DeepEqual(s.Candidate, in.Candidates[0]) {
				t.Fatal("role qualification/provenance lost")
			}
			for _, before := range in.Observations {
				found := false
				for _, after := range out.Native {
					if reflect.DeepEqual(before, after) {
						found = true
					}
				}
				if !found {
					t.Fatal("native evidence or original receipt changed")
				}
			}
			in.EvaluatedAt.UnixNanoseconds = "100"
			in.EvaluateMonotonic.Nanoseconds = "100"
			later := mapThermalCircuitCapabilities(in)
			thermalCircuitTestOutcome(t, later, "service:a", false, "stale_role")
			if !reflect.DeepEqual(out.Native, later.Native) {
				t.Fatal("evaluation renewed observation")
			}
		})
	}
}

func TestThermalCircuitCapabilityMappingInvalidIdentities(t *testing.T) {
	axes := []struct {
		name   string
		set    func(*thermalCircuitIdentity, string)
		values []string
	}{
		{"asset", func(i *thermalCircuitIdentity, v string) { i.Asset = semreg.AssetID(v) }, []string{"", "bad id"}},
		{"service", func(i *thermalCircuitIdentity, v string) { i.Service = semreg.ServiceInstanceID(v) }, []string{"", "bad id"}},
		{"capability", func(i *thermalCircuitIdentity, v string) { i.Capability = semreg.CapabilityInstanceID(v) }, []string{"", "bad id"}},
		{"source", func(i *thermalCircuitIdentity, v string) { i.Source = semreg.SourceID(v) }, []string{"", "bad id"}},
		{"epoch", func(i *thermalCircuitIdentity, v string) { i.Epoch = semreg.SourceEpochID(v) }, []string{"", "bad id"}},
		{"binding", func(i *thermalCircuitIdentity, v string) { i.Binding = semreg.NativeBindingID(v) }, []string{"", "bad id"}},
		{"generation", func(i *thermalCircuitIdentity, v string) { i.Generation = semreg.Uint64(v) }, nil},
		{"binding_revision", func(i *thermalCircuitIdentity, v string) { i.BindingRevision = semreg.Uint64(v) }, nil},
		{"association_revision", func(i *thermalCircuitIdentity, v string) { i.AssociationRevision = semreg.Uint64(v) }, nil},
	}
	for _, axis := range axes {
		values := axis.values
		if values == nil {
			values = []string{"", "not-a-number", "0", "-1", "18446744073709551616", "01"}
		}
		for _, value := range values {
			t.Run(axis.name+"/"+value, func(t *testing.T) {
				in := thermalCircuitTestInput()
				thermalCircuitTestSibling(&in)
				axis.set(&in.Candidates[0].Identity, value)
				id := in.Candidates[0].Identity
				in.Candidates[0].Role.Identity = id
				for i := 0; i < 2; i++ {
					in.Observations[i].Identity = id
				}
				out := mapThermalCircuitCapabilities(in)
				thermalCircuitTestOutcome(t, out, id.Service, false, "identity")
				thermalCircuitTestOutcome(t, out, "service:b", true, "")
				if out.Dispositions[0].Outcome != projection.ProjectionExact || len(out.Native) != 4 {
					t.Fatal("identity loss erased sibling or native evidence")
				}
			})
		}
	}
}

func TestThermalCircuitCapabilityMappingGlobalInstanceConflicts(t *testing.T) {
	for _, kind := range []string{"service_across_assets", "capability_across_assets", "capability_same_asset"} {
		t.Run(kind, func(t *testing.T) {
			in := thermalCircuitTestInput()
			thermalCircuitTestSibling(&in)
			c := in.Candidates[0]
			c.Identity.Asset, c.Identity.Service, c.Identity.Capability = "asset:c", "service:c", "capability:c"
			c.Role.Identity = c.Identity
			in.Candidates = append(in.Candidates, c)
			for _, o := range in.Observations[:2] {
				o.Identity = c.Identity
				in.Observations = append(in.Observations, o)
			}
			id := &in.Candidates[1].Identity
			if kind == "service_across_assets" {
				id.Service = in.Candidates[0].Identity.Service
			} else {
				id.Capability = in.Candidates[0].Identity.Capability
				if kind == "capability_same_asset" {
					id.Asset = in.Candidates[0].Identity.Asset
				}
			}
			in.Candidates[1].Role.Identity = *id
			for i := 2; i < 4; i++ {
				in.Observations[i].Identity = *id
			}
			out := mapThermalCircuitCapabilities(in)
			conflicts := 0
			for _, s := range out.Sources {
				if s.Candidate.Identity.Service == "service:c" {
					continue
				}
				if s.Loss != "conflict" || s.Service != nil || s.Capability != nil {
					t.Fatalf("colliding candidate admitted: %+v", s)
				}
				conflicts++
			}
			if conflicts != 2 {
				t.Fatalf("got %d conflicts, want 2", conflicts)
			}
			thermalCircuitTestOutcome(t, out, "service:c", true, "")
			if out.Dispositions[0].Outcome != projection.ProjectionExact {
				t.Fatal("conflict erased unrelated sibling")
			}
			for i, j := 0, len(in.Candidates)-1; i < j; i, j = i+1, j-1 {
				in.Candidates[i], in.Candidates[j] = in.Candidates[j], in.Candidates[i]
			}
			for i, j := 0, len(in.Observations)-1; i < j; i, j = i+1, j-1 {
				in.Observations[i], in.Observations[j] = in.Observations[j], in.Observations[i]
			}
			if !reflect.DeepEqual(out, mapThermalCircuitCapabilities(in)) {
				t.Fatal("arrival order changed conflicts")
			}
		})
	}
}
