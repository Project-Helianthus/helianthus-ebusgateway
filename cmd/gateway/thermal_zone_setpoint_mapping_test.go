package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

// All admission-positive mappings below are invented controls. They establish
// neither an owner-approved role encoding nor qualification of any native family.
func thermalZoneSetpointFixture() thermalZoneSetpointInput {
	pack := semreg.PackRef{ID: "helianthus.pack.thermal", Version: "1.0.0"}
	digest := sha256.Sum256([]byte("synthetic zone role evidence"))
	evidence := semreg.EvidenceRef{Owner: "test.zone", Kind: "test.synthetic", Digest: semreg.Digest("sha256:" + hex.EncodeToString(digest[:])), Contract: "test.synthetic/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
	in := thermalZoneSetpointInput{
		SemanticRevision: "7",
		Evaluation: semreg.EvaluationContext{
			EvaluatedAt:       semreg.TimePoint{UnixNanoseconds: "200", ClockID: "clock.synthetic", UncertaintyNS: "0"},
			EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "200"},
		},
		Requested: []projection.RequestedItem{{Kind: projection.ItemFact, ItemID: "thermal.setpoint.temperature"}},
	}
	rows := []struct {
		role thermalZoneSetpointRole
		rr   uint16
		key  string
	}{
		{thermalZoneSetpointHeatingManual, 0x0022, "synthetic:zone-a:heating:manual"},
		{thermalZoneSetpointHeatingEffective, 0x0014, "synthetic:zone-a:heating:effective"},
		{thermalZoneSetpointCoolingManual, 0x0023, "synthetic:zone-a:cooling:manual"},
		{thermalZoneSetpointCoolingEffective, 0x0015, "synthetic:zone-a:cooling:effective"},
		{thermalZoneSetpointVeto, 0x0008, "synthetic:zone-a:veto"},
		{thermalZoneSetpointHoliday, 0x0005, "synthetic:zone-a:holiday"},
	}
	for _, row := range rows {
		key := row.key
		id := thermalZoneSetpointIdentity{
			Asset: "asset:synthetic", Service: "service:zone-a", Source: "source:synthetic", Epoch: "epoch:first", Generation: "2",
			Binding: "binding:zone-a", BindingRevision: "3", AssociationRevision: "4",
			Selector: thermalZoneSetpointSelector{PB: 0xb5, SB: 0x24, Opcode: 0x02, OperationType: 0x00, Group: 0x03, Instance: 0x01, Register: row.rr, Direction: "target_to_initiator"},
		}
		in.Mappings = append(in.Mappings, thermalZoneSetpointMapping{
			Identity: id, Role: row.role, Approved: true, Qualification: semreg.QualificationQualified,
			Field:            semreg.DefinitionRef{Pack: pack, ID: "thermal.setpoint.temperature", Version: "1.0.0"},
			Key:              semreg.FactKey{PackID: pack.ID, PackVersion: pack.Version, FactID: "thermal.setpoint.temperature", Dimensions: []semreg.Dimension{{ID: "thermal.dimension.temperature", Value: semreg.Value{Kind: semreg.ValueText, Text: &key}}}},
			SemanticRevision: "7", Unit: "unit.celsius", Codec: "f32_le", BoundsKnown: true,
			Minimum: semreg.Decimal{Coefficient: "0"}, Maximum: semreg.Decimal{Coefficient: "3", Exponent10: 1}, Evidence: []semreg.EvidenceRef{evidence},
		})
		in.Observations = append(in.Observations, thermalZoneSetpointObservation{
			Identity: id, Role: row.role, Present: true,
			Value: semreg.Quantity{Number: semreg.Decimal{Coefficient: "2", Exponent10: 1}, Unit: "unit.celsius"}, Codec: "f32_le",
			Validity: semreg.ValidityGood, Qualification: semreg.QualificationQualified, Freshness: semreg.FreshnessFresh,
			ReceivedAt: semreg.TimePoint{UnixNanoseconds: "100", ClockID: "clock.synthetic", UncertaintyNS: "0"},
			Receipt:    semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "100"}, FreshForNS: "200",
			NativePayload: []byte{0, 0, 160, 65}, Evidence: []semreg.EvidenceRef{evidence},
			WindowKnown: true, Active: true, Until: semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "250"},
		})
	}
	return in
}

func thermalZoneSetpointRun(t *testing.T, in thermalZoneSetpointInput) thermalZoneSetpointResult {
	t.Helper()
	out, err := mapThermalZoneSetpoints(in)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func thermalZoneSetpointHasRole(out thermalZoneSetpointResult, role thermalZoneSetpointRole) bool {
	for _, selected := range out.Selected {
		if selected.Observation.Role == role {
			return true
		}
	}
	return false
}

func thermalZoneSetpointReason(t *testing.T, out thermalZoneSetpointResult, role thermalZoneSetpointRole, reason string) {
	t.Helper()
	for _, history := range out.History {
		if history.Observation.Role == role && !history.Current && history.Reason == reason {
			return
		}
	}
	t.Fatalf("role %s missing reason %s: %+v", role, reason, out.History)
}

func TestThermalZoneSetpointMappingRoles(t *testing.T) {
	in := thermalZoneSetpointFixture()
	out := thermalZoneSetpointRun(t, in)
	if len(out.Selected) != 6 || len(out.Dispositions) != 1 || out.Dispositions[0].Outcome != projection.ProjectionExact {
		t.Fatalf("six independently supplied roles must coexist: %+v", out)
	}
	expected := map[thermalZoneSetpointRole]uint16{
		thermalZoneSetpointHeatingManual: 0x0022, thermalZoneSetpointHeatingEffective: 0x0014,
		thermalZoneSetpointCoolingManual: 0x0023, thermalZoneSetpointCoolingEffective: 0x0015,
		thermalZoneSetpointVeto: 0x0008, thermalZoneSetpointHoliday: 0x0005,
	}
	for _, selected := range out.Selected {
		if selected.Observation.Identity.Selector.Register != expected[selected.Observation.Role] || selected.Key.FactID != "thermal.setpoint.temperature" || selected.Observation.Value.Number != (semreg.Decimal{Coefficient: "2", Exponent10: 1}) {
			t.Fatalf("role/value/selector changed: %+v", selected)
		}
		if selected.Observation.ReceivedAt.UnixNanoseconds != "100" || selected.Observation.Receipt.Nanoseconds != "100" || out.Evaluation.EvaluateMonotonic.Nanoseconds != "200" {
			t.Fatal("evaluation rewrote receipt")
		}
	}
	t.Run("missing_manual_present_effective", func(t *testing.T) {
		in := thermalZoneSetpointFixture()
		in.Observations = in.Observations[1:2]
		out := thermalZoneSetpointRun(t, in)
		if len(out.Selected) != 1 || out.Selected[0].Observation.Role != thermalZoneSetpointHeatingEffective || *out.Selected[0].Key.Dimensions[0].Value.Text != "synthetic:zone-a:heating:effective" {
			t.Fatalf("effective relabeled manual: %+v", out.Selected)
		}
		thermalZoneSetpointReason(t, out, thermalZoneSetpointHeatingManual, "missing")
		if out.Dispositions[0].Outcome != projection.ProjectionExact {
			t.Fatal("missing siblings overwrote projected field")
		}
	})
	t.Run("no_default_representability", func(t *testing.T) {
		in := thermalZoneSetpointFixture()
		for i := range in.Mappings {
			in.Mappings[i].Approved = false
		}
		out := thermalZoneSetpointRun(t, in)
		if len(out.Selected) != 0 || out.Dispositions[0].Outcome != projection.ProjectionWithheld || len(out.History) != 6 {
			t.Fatalf("unresolved role admitted: %+v", out)
		}
	})
}

func TestThermalZoneSetpointMappingIdentity(t *testing.T) {
	// Each mutation changes exactly one observation axis while the independent
	// approved mapping and all five sibling observations remain unchanged.
	cases := []struct {
		name   string
		mutate func(*thermalZoneSetpointIdentity)
	}{
		{"asset", func(x *thermalZoneSetpointIdentity) { x.Asset = "asset:replacement" }},
		{"service", func(x *thermalZoneSetpointIdentity) { x.Service = "service:replacement" }},
		{"source", func(x *thermalZoneSetpointIdentity) { x.Source = "source:replacement" }},
		{"source_epoch", func(x *thermalZoneSetpointIdentity) { x.Epoch = "epoch:replacement" }},
		{"driver_generation", func(x *thermalZoneSetpointIdentity) { x.Generation = "9" }},
		{"binding", func(x *thermalZoneSetpointIdentity) { x.Binding = "binding:replacement" }},
		{"binding_revision", func(x *thermalZoneSetpointIdentity) { x.BindingRevision = "9" }},
		{"association_revision", func(x *thermalZoneSetpointIdentity) { x.AssociationRevision = "9" }},
		{"pb", func(x *thermalZoneSetpointIdentity) { x.Selector.PB = 0xb4 }},
		{"sb", func(x *thermalZoneSetpointIdentity) { x.Selector.SB = 0x25 }},
		{"opcode", func(x *thermalZoneSetpointIdentity) { x.Selector.Opcode = 0x06 }},
		{"operation_type", func(x *thermalZoneSetpointIdentity) { x.Selector.OperationType = 0x01 }},
		{"group", func(x *thermalZoneSetpointIdentity) { x.Selector.Group = 0x00 }},
		{"instance", func(x *thermalZoneSetpointIdentity) { x.Selector.Instance = 0x02 }},
		{"register", func(x *thermalZoneSetpointIdentity) { x.Selector.Register = 0x0014 }},
		{"direction", func(x *thermalZoneSetpointIdentity) { x.Selector.Direction = "initiator_to_target" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := thermalZoneSetpointFixture()
			tc.mutate(&in.Observations[0].Identity)
			out := thermalZoneSetpointRun(t, in)
			if len(out.Selected) != 5 || thermalZoneSetpointHasRole(out, thermalZoneSetpointHeatingManual) {
				t.Fatalf("identity mismatch admitted or damaged sibling: %+v", out.Selected)
			}
			thermalZoneSetpointReason(t, out, thermalZoneSetpointHeatingManual, "unresolved_role")
			for _, selected := range out.Selected {
				if selected.Observation.Value.Number != (semreg.Decimal{Coefficient: "2", Exponent10: 1}) || selected.Observation.ReceivedAt.UnixNanoseconds != "100" {
					t.Fatal("unaffected sibling changed")
				}
			}
		})
	}
}

func TestThermalZoneSetpointMappingAdmission(t *testing.T) {
	cases := []struct {
		name, reason string
		mutate       func(*thermalZoneSetpointInput)
	}{
		{"missing", "missing", func(x *thermalZoneSetpointInput) { x.Observations[0].Present = false }},
		{"validity", "invalid", func(x *thermalZoneSetpointInput) { x.Observations[0].Validity = semreg.ValidityBad }},
		{"freshness", "not_fresh", func(x *thermalZoneSetpointInput) { x.Observations[0].Freshness = semreg.FreshnessStale }},
		{"qualification", "unqualified_role", func(x *thermalZoneSetpointInput) { x.Observations[0].Qualification = semreg.QualificationUnknown }},
		{"role_qualification", "unqualified_role", func(x *thermalZoneSetpointInput) { x.Mappings[0].Qualification = semreg.QualificationCandidate }},
		{"approval", "unqualified_role", func(x *thermalZoneSetpointInput) { x.Mappings[0].Approved = false }},
		{"semantic_revision", "semantic_revision", func(x *thermalZoneSetpointInput) { x.Mappings[0].SemanticRevision = "8" }},
		{"unknown_bounds", "invalid_native_bounds_or_value", func(x *thermalZoneSetpointInput) { x.Mappings[0].BoundsKnown = false }},
		{"reversed_bounds", "native_range", func(x *thermalZoneSetpointInput) {
			x.Mappings[0].Minimum = semreg.Decimal{Coefficient: "4", Exponent10: 1}
		}},
		{"native_range", "native_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Maximum = semreg.Decimal{Coefficient: "19"} }},
		{"pack_range", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) {
			x.Mappings[0].Maximum = semreg.Decimal{Coefficient: "2", Exponent10: 2}
			x.Observations[0].Value.Number = semreg.Decimal{Coefficient: "16", Exponent10: 1}
		}},
		{"nonoverlap", "incoherent_pack_bounds", func(x *thermalZoneSetpointInput) {
			x.Mappings[0].Minimum = semreg.Decimal{Coefficient: "-6", Exponent10: 1}
		}},
		{"unit", "unit_or_codec", func(x *thermalZoneSetpointInput) { x.Observations[0].Value.Unit = "unit.unknown" }},
		{"codec", "unit_or_codec", func(x *thermalZoneSetpointInput) { x.Observations[0].Codec = "unknown" }},
		{"nonfinite", "invalid_native_bounds_or_value", func(x *thermalZoneSetpointInput) { x.Observations[0].Value.Number.Coefficient = "NaN" }},
		{"infinity", "invalid_native_bounds_or_value", func(x *thermalZoneSetpointInput) { x.Observations[0].Value.Number.Coefficient = "Inf" }},
		{"dimension", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Key.Dimensions[0].ID = "thermal.dimension.zone" }},
		{"missing_dimension", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Key.Dimensions = nil }},
		{"dimension_value", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { *x.Mappings[0].Key.Dimensions[0].Value.Text = "" }},
		{"dimension_payload", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Key.Dimensions[0].Value.Text = nil }},
		{"dimension_kind", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Key.Dimensions[0].Value.Kind = semreg.ValueQuantity }},
		{"pack_identity", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Key.PackID = "test.other.pack" }},
		{"pack_revision", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Key.PackVersion = "2.0.0" }},
		{"invented_definition", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Key.FactID = "thermal.setpoint.manual" }},
		{"field_revision", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Field.Version = "2.0.0" }},
		{"field_identity", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Field.ID = "thermal.measurement.air_temperature" }},
		{"field_pack_identity", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Field.Pack.ID = "test.other.pack" }},
		{"field_pack_revision", "unrepresentable_key_or_pack_range", func(x *thermalZoneSetpointInput) { x.Mappings[0].Field.Pack.Version = "2.0.0" }},
		{"no_native_evidence", "unqualified_role", func(x *thermalZoneSetpointInput) { x.Observations[0].Evidence = nil }},
		{"no_mapping_evidence", "unqualified_role", func(x *thermalZoneSetpointInput) { x.Mappings[0].Evidence = nil }},
		{"invalid_evidence", "invalid_evidence", func(x *thermalZoneSetpointInput) { x.Observations[0].Evidence[0].Digest = "invalid" }},
		{"age_boundary", "not_fresh", func(x *thermalZoneSetpointInput) { x.Observations[0].FreshForNS = "100" }},
		{"clock_epoch", "not_fresh", func(x *thermalZoneSetpointInput) { x.Observations[0].Receipt.ClockEpochID = "clock:replacement" }},
		{"future_receipt", "not_fresh", func(x *thermalZoneSetpointInput) { x.Observations[0].Receipt.Nanoseconds = "201" }},
		{"role_swap", "unresolved_role", func(x *thermalZoneSetpointInput) { x.Observations[0].Role = thermalZoneSetpointHeatingEffective }},
		{"limit_fallback", "native_identity", func(x *thermalZoneSetpointInput) {
			x.Mappings[0].Identity.Selector.Register = 0x0002
			x.Observations[0].Identity.Selector.Register = 0x0002
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := thermalZoneSetpointFixture()
			tc.mutate(&in)
			out := thermalZoneSetpointRun(t, in)
			if len(out.Selected) != 5 || thermalZoneSetpointHasRole(out, thermalZoneSetpointHeatingManual) || out.Dispositions[0].Outcome != projection.ProjectionExact {
				t.Fatalf("bad manual admitted or siblings lost: %+v", out.Selected)
			}
			role := thermalZoneSetpointHeatingManual
			if tc.name == "role_swap" {
				role = thermalZoneSetpointHeatingEffective
			}
			thermalZoneSetpointReason(t, out, role, tc.reason)
		})
	}
	t.Run("valid_zero", func(t *testing.T) {
		in := thermalZoneSetpointFixture()
		in.Observations[0].Value.Number = semreg.Decimal{Coefficient: "0"}
		out := thermalZoneSetpointRun(t, in)
		if len(out.Selected) != 6 {
			t.Fatal("valid zero treated as missing")
		}
	})
}

func TestThermalZoneSetpointMappingIntervalAndConflict(t *testing.T) {
	t.Run("collision_even_if_sibling_missing", func(t *testing.T) {
		in := thermalZoneSetpointFixture()
		in.Mappings[1].Key = in.Mappings[0].Key
		in.Observations = append(in.Observations[:1], in.Observations[2:]...)
		out := thermalZoneSetpointRun(t, in)
		if len(out.Selected) != 4 || thermalZoneSetpointHasRole(out, thermalZoneSetpointHeatingManual) {
			t.Fatal("missing sibling concealed an ambiguous role encoding")
		}
		thermalZoneSetpointReason(t, out, thermalZoneSetpointHeatingManual, "role_key_collision")
	})
	for _, index := range []int{4, 5} {
		for _, condition := range []string{"expired", "unknown", "inactive", "clock"} {
			t.Run(string(thermalZoneSetpointFixture().Observations[index].Role)+"/"+condition, func(t *testing.T) {
				in := thermalZoneSetpointFixture()
				switch condition {
				case "expired":
					in.Observations[index].Until.Nanoseconds = "200"
				case "unknown":
					in.Observations[index].WindowKnown = false
				case "inactive":
					in.Observations[index].Active = false
				case "clock":
					in.Observations[index].Until.ClockEpochID = "clock:other"
				}
				out := thermalZoneSetpointRun(t, in)
				if len(out.Selected) != 5 || thermalZoneSetpointHasRole(out, in.Observations[index].Role) {
					t.Fatalf("interval admitted: %+v", out.Selected)
				}
				thermalZoneSetpointReason(t, out, in.Observations[index].Role, "inactive_or_unknown_interval")
				for _, history := range out.History {
					if history.Observation.Role == in.Observations[index].Role && history.Observation.Value.Number != (semreg.Decimal{Coefficient: "2", Exponent10: 1}) {
						t.Fatal("expired target history lost")
					}
				}
			})
		}
	}
	for _, condition := range []string{"role_key_collision", "duplicate_candidate", "equal_other_source", "disagree_other_source", "duplicate_mapping"} {
		t.Run(condition, func(t *testing.T) {
			in := thermalZoneSetpointFixture()
			want := 5
			switch condition {
			case "role_key_collision":
				in.Mappings[1].Key = in.Mappings[0].Key
				want = 4
			case "duplicate_candidate":
				in.Observations = append(in.Observations, in.Observations[0])
			case "equal_other_source", "disagree_other_source":
				m, o := in.Mappings[0], in.Observations[0]
				m.Identity.Source, o.Identity.Source = "source:other", "source:other"
				if condition == "disagree_other_source" {
					o.Value.Number = semreg.Decimal{Coefficient: "21"}
				}
				in.Mappings, in.Observations = append(in.Mappings, m), append(in.Observations, o)
			case "duplicate_mapping":
				in.Mappings = append(in.Mappings, in.Mappings[0])
			}
			out := thermalZoneSetpointRun(t, in)
			if len(out.Selected) != want || thermalZoneSetpointHasRole(out, thermalZoneSetpointHeatingManual) {
				t.Fatalf("ambiguous selection admitted: %+v", out.Selected)
			}
		})
	}
}

func TestThermalZoneSetpointMappingRetention(t *testing.T) {
	in := thermalZoneSetpointFixture()
	first := thermalZoneSetpointRun(t, in)
	in.Previous = first.History
	in.Observations = in.Observations[:1]
	in.Observations[0].Freshness = semreg.FreshnessStale
	out := thermalZoneSetpointRun(t, in)
	if len(out.Selected) != 5 || thermalZoneSetpointHasRole(out, thermalZoneSetpointHeatingManual) {
		t.Fatal("stale update erased unaffected siblings or stayed selected")
	}
	thermalZoneSetpointReason(t, out, thermalZoneSetpointHeatingManual, "retained_non_current")
	thermalZoneSetpointReason(t, out, thermalZoneSetpointHeatingManual, "not_fresh")
	in.Evaluation.EvaluateMonotonic.Nanoseconds = "300"
	out = thermalZoneSetpointRun(t, in)
	if len(out.Selected) != 0 {
		t.Fatal("re-evaluation refreshed retained receipt age")
	}
	for _, history := range out.History {
		if history.Observation.Receipt.Nanoseconds != "100" {
			t.Fatal("receipt changed")
		}
	}
	// Replacement mapping cannot revive the old source or borrow its admission.
	in = thermalZoneSetpointFixture()
	in.Previous = first.History
	in.Mappings[0].Identity.Epoch = "epoch:replacement"
	in.Observations = nil
	out = thermalZoneSetpointRun(t, in)
	if len(out.Selected) != 5 || thermalZoneSetpointHasRole(out, thermalZoneSetpointHeatingManual) {
		t.Fatal("old source admitted into replacement")
	}
	thermalZoneSetpointReason(t, out, thermalZoneSetpointHeatingManual, "unresolved_role")
}

func TestThermalZoneSetpointMappingAccountingAndDetachment(t *testing.T) {
	in := thermalZoneSetpointFixture()
	in.Requested = append(in.Requested, in.Requested[0], projection.RequestedItem{Kind: projection.ItemCapability, ItemID: "thermal.capability.set_temperature"}, projection.RequestedItem{Kind: projection.ItemOperation, ItemID: "thermal.operation.set_temperature"}, projection.RequestedItem{Kind: projection.ItemFact, ItemID: "test.requested.unknown"})
	out := thermalZoneSetpointRun(t, in)
	if len(out.Dispositions) != 4 {
		t.Fatalf("requested accounting: %+v", out.Dispositions)
	}
	for _, d := range out.Dispositions {
		if d.ItemID != "thermal.setpoint.temperature" && (d.Outcome != projection.ProjectionWithheld || len(d.SourceKeys) != 0) {
			t.Fatalf("invented write/unsupported support: %+v", d)
		}
	}
	baseline, _ := json.Marshal(out)
	// Permuting caller collections must not alter the deterministic output.
	for i, j := 0, len(in.Observations)-1; i < j; i, j = i+1, j-1 {
		in.Observations[i], in.Observations[j] = in.Observations[j], in.Observations[i]
		in.Mappings[i], in.Mappings[j] = in.Mappings[j], in.Mappings[i]
	}
	permuted, _ := json.Marshal(thermalZoneSetpointRun(t, in))
	if string(baseline) != string(permuted) {
		t.Fatal("order depends on input order")
	}
	in.Observations[0].NativePayload[0] = 99
	in.Observations[0].Evidence[0].Digest = "changed"
	in.Mappings[0].Evidence[0].Digest = "changed"
	*in.Mappings[0].Key.Dimensions[0].Value.Text = "changed"
	after, _ := json.Marshal(out)
	if string(baseline) != string(after) {
		t.Fatal("input aliases output")
	}
	in = thermalZoneSetpointFixture()
	inputBefore, _ := json.Marshal(in)
	out = thermalZoneSetpointRun(t, in)
	for i := range out.Selected {
		out.Selected[i].Observation.NativePayload[0] = 88
		out.Selected[i].Observation.Evidence[0].Digest = "changed"
		out.Selected[i].MappingEvidence[0].Digest = "changed"
		*out.Selected[i].Key.Dimensions[0].Value.Text = "changed"
	}
	for _, h := range out.History {
		if h.Observation.NativePayload[0] != 0 || h.Observation.Evidence[0].Digest == "changed" {
			t.Fatal("selected aliases history")
		}
	}
	for _, d := range out.Dispositions {
		for _, key := range d.SourceKeys {
			if *key.Dimensions[0].Value.Text == "changed" {
				t.Fatal("selected aliases dispositions")
			}
		}
	}
	inputAfter, _ := json.Marshal(in)
	if !reflect.DeepEqual(inputBefore, inputAfter) {
		t.Fatal("output aliases input")
	}
	other := projection.ProjectionDisposition{Kind: projection.ItemFact, ItemID: "thermal.setpoint.temperature", Outcome: projection.ProjectionExact, SourceKeys: []semreg.FactKey{in.Mappings[0].Key}}
	in.OtherDispositions, in.Observations, in.Mappings = []projection.ProjectionDisposition{other}, nil, nil
	out = thermalZoneSetpointRun(t, in)
	if len(out.Dispositions) != 1 || !reflect.DeepEqual(out.Dispositions[0], other) {
		t.Fatal("source loss overwrote another source disposition")
	}
}
