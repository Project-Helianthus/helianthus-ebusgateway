package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func thermalMaintenanceFixture(register uint16, value bool) thermalMaintenanceInput {
	id := thermalMaintenanceIdentity{
		Asset: "asset:synthetic", Service: "service:system",
		Path:            semreg.SourcePathRef{BindingID: "binding:synthetic", SourceID: "source:synthetic", SourceEpochID: "epoch:one", DriverGeneration: "1"},
		BindingRevision: "1", AssociationRevision: "1",
		Selector: thermalMaintenanceSelector{PB: 0xb5, SB: 0x24, Opcode: 2, Register: register, Direction: "target_to_initiator"},
	}
	raw := byte(0)
	if value {
		raw = 1
	}
	return thermalMaintenanceInput{
		Bindings: []thermalMaintenanceIdentity{id},
		Observations: []thermalMaintenanceObservation{{Identity: id, Value: &value, RawValue: []byte{raw}, Request: []byte{2, 0}, Reply: []byte{raw},
			Validity: semreg.ValidityGood, Freshness: semreg.FreshnessFresh, Qualification: semreg.QualificationQualified,
			EvidenceStatus: "Unknown", EvidenceRevision: "synthetic:one", DecoderRevision: "synthetic:decoder", MappingRevision: "synthetic:mapping",
			Evidence:         []semreg.EvidenceRef{{Owner: "synthetic.owner"}},
			ReceivedAt:       semreg.TimePoint{UnixNanoseconds: "10", ClockID: "synthetic.clock", UncertaintyNS: "0"},
			ReceiptMonotonic: semreg.MonotonicPoint{ClockEpochID: "synthetic.epoch", Nanoseconds: "10"}}},
		EvaluatedAt:       semreg.TimePoint{UnixNanoseconds: "20", ClockID: "synthetic.clock", UncertaintyNS: "0"},
		EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "synthetic.epoch", Nanoseconds: "20"},
		Requested: []projection.RequestedItem{
			{Kind: projection.ItemFact, ItemID: "thermal.mode.system"},
			{Kind: projection.ItemFact, ItemID: "thermal.status.operation"},
			{Kind: projection.ItemFact, ItemID: "thermal.status.fault"},
			{Kind: projection.ItemFact, ItemID: "thermal.action.state"},
		},
	}
}

func thermalMaintenanceMap(t *testing.T, in thermalMaintenanceInput) thermalMaintenanceResult {
	t.Helper()
	out, err := mapThermalMaintenance(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range out.Dispositions {
		if err := d.Validate(); err != nil {
			t.Fatalf("invalid disposition: %v: %+v", err, d)
		}
	}
	if len(out.Facts) != 0 {
		t.Fatal("Boolean evidence emitted canonical facts", out.Facts)
	}
	return out
}

func TestThermalMaintenanceMappingWithholding(t *testing.T) {
	for _, register := range []uint16{0x0007, 0x0096} {
		for _, value := range []bool{false, true} {
			in := thermalMaintenanceFixture(register, value)
			out := thermalMaintenanceMap(t, in)
			if !reflect.DeepEqual(out.Native[0].Observation, in.Observations[0]) || !out.Native[0].CurrentBinding || out.Native[0].Loss != "unresolved_native_meaning" {
				t.Fatal(out.Native)
			}
			for _, d := range out.Dispositions {
				if d.Outcome != projection.ProjectionWithheld {
					t.Fatal(d)
				}
			}
			if len(out.Dispositions) != 4 {
				t.Fatal(out.Dispositions)
			}
			in.Observations[0].Value = nil
			in.Observations[0].RawValue = nil
			missing := thermalMaintenanceMap(t, in)
			if missing.Native[0].Observation.Value != nil || missing.Native[0].Loss != "missing_decoded_value" {
				t.Fatal(missing)
			}
		}
	}
}

func TestThermalMaintenanceMappingIdentity(t *testing.T) {
	mutations := thermalMaintenanceIdentityMutations()
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			in := thermalMaintenanceFixture(7, false)
			// A distinct sibling must survive fencing of only the first binding.
			sibling := thermalMaintenanceFixture(0x0096, true)
			in.Bindings = append(in.Bindings, sibling.Bindings[0])
			in.Observations = append(in.Observations, sibling.Observations[0])
			// Keep replacement distinct even when the changed register equals the sibling.
			in.Bindings[1].Service = "service:sibling"
			in.Observations[1].Identity.Service = "service:sibling"
			mutation.mutate(&in.Bindings[0])
			out := thermalMaintenanceMap(t, in)
			if out.Native[0].CurrentBinding || out.Native[0].Loss != "different_binding" {
				t.Fatal("replacement inherited old evidence", out.Native[0])
			}
			if !out.Native[1].CurrentBinding || !reflect.DeepEqual(out.Native[1].Observation, in.Observations[1]) {
				t.Fatal("sibling changed", out.Native[1])
			}
		})
	}
}

func TestThermalMaintenanceMappingQualityAndRetention(t *testing.T) {
	cases := []struct {
		name, loss string
		mutate     func(*thermalMaintenanceObservation)
	}{
		{"stale", "freshness_withheld", func(o *thermalMaintenanceObservation) { o.Freshness = semreg.FreshnessStale }},
		{"unknown_freshness", "freshness_withheld", func(o *thermalMaintenanceObservation) { o.Freshness = semreg.FreshnessUnknown }},
		{"invalid", "invalid_native_value", func(o *thermalMaintenanceObservation) { o.Validity = semreg.ValidityBad }},
		{"unqualified", "qualification_withheld", func(o *thermalMaintenanceObservation) { o.Qualification = semreg.QualificationCandidate }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := thermalMaintenanceFixture(0x0096, true)
			c.mutate(&in.Observations[0])
			out := thermalMaintenanceMap(t, in)
			if out.Native[0].Loss != c.loss || !reflect.DeepEqual(out.Native[0].Observation, in.Observations[0]) {
				t.Fatal(out.Native)
			}
			if out.EvaluatedAt != in.EvaluatedAt || out.Native[0].Observation.ReceivedAt.UnixNanoseconds != "10" || out.Native[0].Observation.ReceiptMonotonic.Nanoseconds != "10" {
				t.Fatal("receipt refreshed", out)
			}
		})
	}
	in := thermalMaintenanceFixture(7, false)
	missing := in.Observations[0]
	missing.Value, missing.RawValue = nil, nil
	in.Observations = append(in.Observations, missing)
	out := thermalMaintenanceMap(t, in)
	if out.Native[0].Observation.Value == nil || *out.Native[0].Observation.Value || out.Native[1].Observation.Value != nil {
		t.Fatal("last-known native value lost", out)
	}
	conflict := thermalMaintenanceFixture(7, true)
	in.Observations = append(in.Observations, conflict.Observations[0])
	out = thermalMaintenanceMap(t, in)
	if !out.Native[0].Conflict || !out.Native[2].Conflict || out.Native[1].Conflict {
		t.Fatal(out.Native)
	}
}

func TestThermalMaintenanceMappingAccountingAndIsolation(t *testing.T) {
	for _, mode := range []string{"false", "missing", "stale", "true"} {
		t.Run(mode, func(t *testing.T) {
			in := thermalMaintenanceFixture(0x0096, mode == "true")
			if mode == "missing" {
				in.Observations[0].Value = nil
			}
			if mode == "stale" {
				in.Observations[0].Freshness = semreg.FreshnessStale
			}
			in.Requested = append(in.Requested, in.Requested[0], projection.RequestedItem{Kind: projection.ItemFact, ItemID: "unsupported.request"})
			in.Independent = []projection.ProjectionDisposition{{Kind: projection.ItemFact, ItemID: "thermal.status.fault", Outcome: projection.ProjectionTransformed,
				SourceKeys: []semreg.FactKey{{PackID: "helianthus.pack.thermal", PackVersion: "1.0.0", FactID: "thermal.status.fault", Dimensions: []semreg.Dimension{{ID: "thermal.dimension.system", Value: semreg.Value{Kind: semreg.ValueText, Text: thermalMaintenanceText("synthetic:system")}}}}},
				Loss:       []projection.LossDetail{{Kind: projection.LossProvenance, SourceItems: []semreg.DefinitionID{"thermal.status.fault"}, Description: "synthetic independent source"}}}}
			out := thermalMaintenanceMap(t, in)
			if len(out.Dispositions) != 5 {
				t.Fatal(out.Dispositions)
			}
			for _, d := range out.Dispositions {
				if d.ItemID == "thermal.status.fault" && !reflect.DeepEqual(d, in.Independent[0]) {
					t.Fatal("independent fault overwritten", d)
				}
				if d.ItemID == "unsupported.request" && d.Outcome != projection.ProjectionUnsupported {
					t.Fatal(d)
				}
			}
			again := thermalMaintenanceMap(t, in)
			if !reflect.DeepEqual(out, again) {
				t.Fatal("nondeterministic")
			}
			in.Observations[0].RawValue[0] = 9
			in.Observations[0].Request[0] = 9
			in.Observations[0].Reply[0] = 9
			in.Observations[0].Evidence[0].Owner = "changed.owner"
			expectedRaw := byte(0)
			if mode == "true" {
				expectedRaw = 1
			}
			if out.Native[0].Observation.RawValue[0] != expectedRaw || out.Native[0].Observation.Request[0] != 2 || out.Native[0].Observation.Reply[0] != expectedRaw || out.Native[0].Observation.Evidence[0].Owner != "synthetic.owner" {
				t.Fatal("input aliases output")
			}
			out.Native[0].Observation.RawValue[0] = 8
			out.Native[0].Observation.Request[0] = 8
			out.Native[0].Observation.Reply[0] = 8
			out.Native[0].Observation.Evidence[0].Owner = "output.owner"
			if out.Native[0].Observation.Value != nil {
				*out.Native[0].Observation.Value = !*out.Native[0].Observation.Value
				if *in.Observations[0].Value != (mode == "true") {
					t.Fatal("Boolean aliases input")
				}
			}
			for i := range out.Dispositions {
				if out.Dispositions[i].ItemID == "thermal.status.fault" {
					*out.Dispositions[i].SourceKeys[0].Dimensions[0].Value.Text = "changed"
					out.Dispositions[i].Loss[0].SourceItems[0] = "changed"
				}
			}
			if in.Observations[0].RawValue[0] != 9 || in.Observations[0].Evidence[0].Owner != "changed.owner" || *in.Independent[0].SourceKeys[0].Dimensions[0].Value.Text != "synthetic:system" || in.Independent[0].Loss[0].SourceItems[0] != "thermal.status.fault" {
				t.Fatal("output aliases input")
			}
		})
	}
}

func thermalMaintenanceText(s string) *string { return &s }

func TestThermalMaintenanceMappingUnknownAndAbsent(t *testing.T) {
	in := thermalMaintenanceFixture(7, true)
	in.Observations[0].RawValue = []byte{2}
	in.Observations[0].Validity = semreg.ValidityUnknown
	out := thermalMaintenanceMap(t, in)
	if out.Native[0].Loss != "invalid_native_value" || out.Native[0].Observation.RawValue[0] != 2 || !*out.Native[0].Observation.Value {
		t.Fatal("unknown code normalized", out)
	}
	in.Observations = nil
	out = thermalMaintenanceMap(t, in)
	if len(out.Native) != 0 || len(out.Dispositions) != 4 {
		t.Fatal("invented missing native value", out)
	}
	in = thermalMaintenanceFixture(7, false)
	in.Bindings[0].Selector.Opcode = 6
	in.Observations[0].Identity.Selector.Opcode = 6
	out = thermalMaintenanceMap(t, in)
	if out.Native[0].Loss != "unsupported_selector" {
		t.Fatal(out)
	}
}

func thermalMaintenanceIdentityMutations() []struct {
	name   string
	mutate func(*thermalMaintenanceIdentity)
} {
	return []struct {
		name   string
		mutate func(*thermalMaintenanceIdentity)
	}{
		{"asset", func(i *thermalMaintenanceIdentity) { i.Asset = "asset:other" }},
		{"service", func(i *thermalMaintenanceIdentity) { i.Service = "service:other" }},
		{"source", func(i *thermalMaintenanceIdentity) { i.Path.SourceID = "source:other" }},
		{"epoch", func(i *thermalMaintenanceIdentity) { i.Path.SourceEpochID = "epoch:other" }},
		{"generation", func(i *thermalMaintenanceIdentity) { i.Path.DriverGeneration = "2" }},
		{"binding", func(i *thermalMaintenanceIdentity) { i.Path.BindingID = "binding:other" }},
		{"binding_revision", func(i *thermalMaintenanceIdentity) { i.BindingRevision = "2" }},
		{"association_revision", func(i *thermalMaintenanceIdentity) { i.AssociationRevision = "2" }},
		{"pb", func(i *thermalMaintenanceIdentity) { i.Selector.PB++ }},
		{"sb", func(i *thermalMaintenanceIdentity) { i.Selector.SB++ }},
		{"opcode", func(i *thermalMaintenanceIdentity) { i.Selector.Opcode = 6 }},
		{"operation_type", func(i *thermalMaintenanceIdentity) { i.Selector.OperationType++ }},
		{"group", func(i *thermalMaintenanceIdentity) { i.Selector.Group++ }},
		{"instance", func(i *thermalMaintenanceIdentity) { i.Selector.Instance++ }},
		{"register", func(i *thermalMaintenanceIdentity) { i.Selector.Register = 0x0096 }},
		{"direction", func(i *thermalMaintenanceIdentity) { i.Selector.Direction = "initiator_to_target" }},
	}
}

func TestI39DispositionEnvelope(t *testing.T) {
	in := thermalMaintenanceFixture(7, false)
	second := thermalMaintenanceFixture(0x0096, true).Observations[0]
	second.Freshness = semreg.FreshnessStale
	in.Bindings = append(in.Bindings, second.Identity)
	in.Observations = append(in.Observations, second)
	in.Requested = append(in.Requested, projection.RequestedItem{Kind: projection.ItemFact, ItemID: "unsupported.request"})
	out := thermalMaintenanceMap(t, in)
	for _, d := range out.Dispositions {
		// Origin: internal/semanticview.project's existing default non-exact reason.
		if d.Reason == nil || *d.Reason != "semantic.read.reason.value_unknown" {
			t.Fatal("reason owner changed", d)
		}
		if len(d.SourceKeys) != 0 || len(d.Loss) != 1 || d.Loss[0].Kind != projection.LossProvenance || len(d.Loss[0].SourceItems) != 0 || d.Loss[0].Reversible {
			t.Fatal(d)
		}
		var losses []struct {
			Identity thermalMaintenanceIdentity
			Loss     string
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(d.Loss[0].Description, "No canonical equivalence; retained native source losses: ")), &losses); err != nil {
			t.Fatal(err)
		}
		if len(losses) != 2 || losses[0].Identity != in.Observations[0].Identity || losses[0].Loss != "unresolved_native_meaning" || losses[1].Identity != second.Identity || losses[1].Loss != "freshness_withheld" {
			t.Fatal(losses)
		}
		for _, member := range []string{"kind", "item_id", "outcome", "source_keys", "loss", "reason"} {
			t.Run(string(d.ItemID)+"/"+member, func(t *testing.T) {
				broken := d
				switch member {
				case "kind":
					broken.Kind = ""
				case "item_id":
					broken.ItemID = ""
				case "outcome":
					broken.Outcome = ""
				case "source_keys":
					broken.SourceKeys = nil
				case "loss":
					broken.Loss = nil
				case "reason":
					broken.Reason = nil
				}
				if broken.Validate() == nil {
					t.Fatal("required member accepted", member)
				}
			})
		}
	}
	// Invalid incoming accounting fails closed instead of returning invalid rows.
	in.Independent = []projection.ProjectionDisposition{{Kind: projection.ItemFact, ItemID: "thermal.status.fault", Outcome: projection.ProjectionWithheld}}
	if _, err := mapThermalMaintenance(in); err == nil {
		t.Fatal("invalid independent row accepted")
	}
}

func TestI39EvidenceStatusAndStaleConflict(t *testing.T) {
	in := thermalMaintenanceFixture(7, false)
	in.Observations[0].EvidenceStatus = "Provne"
	in.Observations[0].Freshness = semreg.FreshnessStale
	in.Observations[0].Qualification = semreg.QualificationCandidate
	in.Observations = append(in.Observations, thermalMaintenanceFixture(7, true).Observations[0])
	out := thermalMaintenanceMap(t, in)
	if len(out.Native) != 2 || !out.Native[0].Conflict || !out.Native[1].Conflict || out.Native[0].EvidenceInterpretation != "Unknown" || out.Native[1].EvidenceInterpretation != "Unknown" {
		t.Fatal(out.Native)
	}
	for i := range in.Observations {
		if !reflect.DeepEqual(out.Native[i].Observation, in.Observations[i]) {
			t.Fatal("candidate changed", out.Native[i])
		}
	}
	if out.Native[0].Loss != "freshness_withheld" || out.Native[1].Loss != "unresolved_native_meaning" {
		t.Fatal(out.Native)
	}
	for _, status := range []string{"Proven", "Hypothesis", "Unknown", ""} {
		in.Observations[0].EvidenceStatus = status
		got := thermalMaintenanceMap(t, in)
		want := status
		if want == "" {
			want = "Unknown"
		}
		if got.Native[0].EvidenceInterpretation != want || got.Native[0].Observation.EvidenceStatus != status {
			t.Fatal(got.Native[0])
		}
	}
}

func TestI39ConflictIdentityIsolation(t *testing.T) {
	for _, mutation := range thermalMaintenanceIdentityMutations() {
		t.Run(mutation.name, func(t *testing.T) {
			in := thermalMaintenanceFixture(7, false)
			other := thermalMaintenanceFixture(7, true).Observations[0]
			mutation.mutate(&other.Identity)
			in.Bindings = append(in.Bindings, other.Identity)
			in.Observations = append(in.Observations, other)
			out := thermalMaintenanceMap(t, in)
			if len(out.Native) != 2 {
				t.Fatal(out.Native)
			}
			for i, native := range out.Native {
				if native.Conflict || !native.CurrentBinding || !reflect.DeepEqual(native.Observation, in.Observations[i]) {
					t.Fatal("distinct identity conflated", native)
				}
			}
		})
	}
}

func TestI39IndependentExactPreserved(t *testing.T) {
	in := thermalMaintenanceFixture(0x0096, false)
	in.Independent = []projection.ProjectionDisposition{{Kind: projection.ItemFact, ItemID: "thermal.status.fault", Outcome: projection.ProjectionExact, SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{}}}
	out := thermalMaintenanceMap(t, in)
	for _, d := range out.Dispositions {
		if d.ItemID == "thermal.status.fault" && !reflect.DeepEqual(d, in.Independent[0]) {
			t.Fatal("independent exact projection changed", d)
		}
	}
	if len(out.Native) != 1 || out.Native[0].Loss != "unresolved_native_meaning" {
		t.Fatal("native loss erased", out.Native)
	}
}
