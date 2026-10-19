package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// All positive controls are synthetic assumptions, never qualification evidence.
func thermalZoneModeFixture() thermalZoneModeInput {
	id := thermalZoneModeIdentity{Asset: "asset:alpha", Service: "service:alpha", Source: "source:alpha", Epoch: "epoch:alpha", Generation: "1", Binding: "binding:alpha", BindingRevision: "2", AssociationRevision: "3", SemanticRevision: "4"}
	in := thermalZoneModeInput{Context: thermalZoneModeContext{Identity: id, Zone: "zone:alpha", ZoneInstance: 3, CircuitInstance: 0, AssociationQualified: true, EquivalenceQualified: true, FreshForNS: 20, Evaluation: semreg.EvaluationContext{EvaluatedAt: semreg.TimePoint{UnixNanoseconds: "110", ClockID: "clock.synthetic", UncertaintyNS: "0"}, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:alpha", Nanoseconds: "110"}}}}
	groups := []byte{3, 3, 3, 2, 2}
	instances := []byte{3, 3, 3, 0, 0}
	registers := []uint16{6, 14, 28, 2, 6}
	values := []uint16{1, 0, 0, 1}
	for i := range in.Observations {
		digest := sha256.Sum256([]byte(fmt.Sprintf("synthetic component %d", i)))
		o := &thermalZoneModeObservation{Identity: id, Selector: thermalZoneModeSelector{PB: 0xB5, SB: 0x24, Opcode: 0xB524, Operation: 2, Group: groups[i], Instance: instances[i], Register: registers[i], Direction: "target_to_initiator"}, Qualification: semreg.QualificationQualified, Validity: semreg.ValidityGood, Freshness: semreg.FreshnessFresh, ReceivedAt: semreg.TimePoint{UnixNanoseconds: "100", ClockID: "clock.synthetic", UncertaintyNS: "0"}, ReceiptMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:alpha", Nanoseconds: "100"}, Request: []byte{byte(i), 2}, Reply: []byte{byte(i), 0}, Evidence: []semreg.EvidenceRef{{Digest: semreg.Digest(fmt.Sprintf("%x", digest))}}}
		if i < 4 {
			v := values[i]
			o.Number = &v
		} else {
			v := false
			o.Boolean = &v
		}
		in.Observations[i] = o
	}
	return in
}

func thermalZoneModeAssert(t *testing.T, in thermalZoneModeInput, token string) thermalZoneModeResult {
	t.Helper()
	before := thermalZoneModeFixture()
	original, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	got := mapThermalZoneMode(in)
	again := mapThermalZoneMode(in)
	after, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(original) != string(after) {
		t.Fatal("mapping mutated input")
	}
	if !reflect.DeepEqual(got, again) {
		t.Fatal("non-deterministic mapping")
	}
	if token == "" {
		if got.Value != nil {
			t.Fatalf("unexpected candidate: %+v", got.Value)
		}
	} else {
		if got.Value == nil || got.Value.Kind != semreg.ValueSymbol || got.Value.Symbol.Token != token || !got.Value.Symbol.Known || got.Value.Symbol.Namespace != "thermal.mode.zone" {
			t.Fatalf("candidate = %+v; want %s", got.Value, token)
		}
		want := semreg.FactKey{PackID: "helianthus.pack.thermal", PackVersion: "1.0.0", FactID: "thermal.mode.zone", Dimensions: []semreg.Dimension{{ID: "thermal.dimension.zone", Value: semreg.Value{Kind: semreg.ValueText, Text: &before.Context.Zone}}}}
		if !reflect.DeepEqual(got.Key, want) {
			t.Fatalf("key = %+v", got.Key)
		}
	}
	if len(got.Losses) == 0 || got.Losses[0].Component != "preset" {
		t.Fatal("missing independent preset loss")
	}
	for i, o := range in.Observations {
		if !reflect.DeepEqual(o, got.Native[i]) {
			t.Fatalf("component %d changed or erased", i)
		}
	}
	return got
}

func TestThermalZoneModeMappingModes(t *testing.T) {
	for _, tc := range []struct {
		name string
		o    *uint16
		want string
	}{{"missing", nil, ""}, {"off", thermalZoneModeNumber(0), "off"}, {"auto", thermalZoneModeNumber(1), "auto"}, {"manual", thermalZoneModeNumber(2), ""}, {"unknown", thermalZoneModeNumber(9), ""}} {
		t.Run(tc.name, func(t *testing.T) {
			in := thermalZoneModeFixture()
			in.Observations[0].Number = tc.o
			thermalZoneModeAssert(t, in, tc.want)
		})
	}
	for _, s := range []uint16{0, 1, 2, 3, 4, 5, 6, 9} {
		t.Run(fmt.Sprintf("S%d", s), func(t *testing.T) {
			in := thermalZoneModeFixture()
			*in.Observations[1].Number = s
			want := ""
			if s == 0 {
				want = "auto"
			}
			thermalZoneModeAssert(t, in, want)
		})
	}
	for _, ct := range []uint16{0, 2, 9} {
		for _, o := range []uint16{0, 1, 2} {
			t.Run(fmt.Sprintf("T%dO%d", ct, o), func(t *testing.T) {
				in := thermalZoneModeFixture()
				*in.Observations[3].Number = ct
				*in.Observations[0].Number = o
				thermalZoneModeAssert(t, in, "")
			})
		}
	}
	for _, cool := range []bool{false, true} {
		t.Run(fmt.Sprintf("cooling%v", cool), func(t *testing.T) {
			in := thermalZoneModeFixture()
			*in.Observations[4].Boolean = cool
			thermalZoneModeAssert(t, in, "auto")
		})
	}
}
func thermalZoneModeNumber(n uint16) *uint16 { return &n }

// Each row changes exactly one comparison axis on one component; all sibling
// components remain byte-for-byte unchanged and the baseline still maps auto.
func TestThermalZoneModeMappingIdentity(t *testing.T) {
	for component := 0; component < 5; component++ {
		for _, group := range []string{"Identity", "Selector"} {
			fixture := thermalZoneModeFixture()
			v := reflect.ValueOf(fixture.Observations[component]).Elem().FieldByName(group)
			for field := 0; field < v.NumField(); field++ {
				name := v.Type().Field(field).Name
				t.Run(fmt.Sprintf("%d/%s/%s", component, group, name), func(t *testing.T) {
					in := thermalZoneModeFixture()
					before := mapThermalZoneMode(in).Native
					f := reflect.ValueOf(in.Observations[component]).Elem().FieldByName(group).Field(field)
					if f.Kind() == reflect.String {
						f.SetString(f.String() + ":changed")
					} else {
						f.SetUint(f.Uint() + 1)
					}
					got := thermalZoneModeAssert(t, in, "")
					for sibling := 0; sibling < 5; sibling++ {
						if sibling != component && !reflect.DeepEqual(got.Native[sibling], before[sibling]) {
							t.Fatal("unaffected sibling lost")
						}
					}
					thermalZoneModeAssert(t, thermalZoneModeFixture(), "auto")
				})
			}
		}
	}
}

func TestThermalZoneModeMappingIndependentState(t *testing.T) {
	changes := []struct {
		name  string
		apply func(*thermalZoneModeObservation)
	}{
		{"unqualified", func(o *thermalZoneModeObservation) { o.Qualification = semreg.QualificationUnknown }},
		{"invalid", func(o *thermalZoneModeObservation) { o.Validity = semreg.ValidityBad }},
		{"stale", func(o *thermalZoneModeObservation) { o.Freshness = semreg.FreshnessStale }},
		{"expired-clock", func(o *thermalZoneModeObservation) { o.ReceiptMonotonic.Nanoseconds = "1" }},
		{"clock-epoch", func(o *thermalZoneModeObservation) { o.ReceiptMonotonic.ClockEpochID = "clock:other" }},
		{"future", func(o *thermalZoneModeObservation) { o.ReceiptMonotonic.Nanoseconds = "111" }},
		{"bad-clock", func(o *thermalZoneModeObservation) { o.ReceiptMonotonic.Nanoseconds = "unknown" }},
		{"request", func(o *thermalZoneModeObservation) { o.Request = nil }},
		{"reply", func(o *thermalZoneModeObservation) { o.Reply = nil }},
		{"evidence", func(o *thermalZoneModeObservation) { o.Evidence = nil }},
		{"decoded-missing", func(o *thermalZoneModeObservation) { o.Number = nil; o.Boolean = nil }},
		{"decoded-ambiguous", func(o *thermalZoneModeObservation) { o.Number = thermalZoneModeNumber(0); v := false; o.Boolean = &v }},
	}
	for i := 0; i < 5; i++ {
		t.Run(fmt.Sprintf("missing%d", i), func(t *testing.T) {
			in := thermalZoneModeFixture()
			in.Observations[i] = nil
			thermalZoneModeAssert(t, in, "")
		})
		for _, change := range changes {
			t.Run(fmt.Sprintf("%d/%s", i, change.name), func(t *testing.T) {
				in := thermalZoneModeFixture()
				change.apply(in.Observations[i])
				thermalZoneModeAssert(t, in, "")
			})
		}
	}
	for _, tc := range []struct {
		name  string
		apply func(*thermalZoneModeInput)
	}{
		{"association", func(in *thermalZoneModeInput) { in.Context.AssociationQualified = false }},
		{"equivalence", func(in *thermalZoneModeInput) { in.Context.EquivalenceQualified = false }},
		{"reassociation", func(in *thermalZoneModeInput) { in.Context.CircuitInstance = 1 }},
		{"assignment", func(in *thermalZoneModeInput) { *in.Observations[2].Number = 1 }},
		{"no-policy", func(in *thermalZoneModeInput) { in.Context.FreshForNS = 0 }},
		{"no-zone", func(in *thermalZoneModeInput) { in.Context.Zone = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) { in := thermalZoneModeFixture(); tc.apply(&in); thermalZoneModeAssert(t, in, "") })
	}
}

func TestThermalZoneModeMappingDetached(t *testing.T) {
	in := thermalZoneModeFixture()
	a := thermalZoneModeAssert(t, in, "auto")
	b := mapThermalZoneMode(in)
	*in.Observations[0].Number = 0
	*in.Observations[4].Boolean = true
	in.Observations[0].Request[0] = 99
	in.Observations[0].Reply[0] = 99
	in.Observations[0].Evidence[0].Digest = "changed"
	if !reflect.DeepEqual(a, b) {
		t.Fatal("input mutation reached output")
	}
	*a.Native[1].Number = 9
	*a.Native[4].Boolean = false
	a.Native[1].Request[0] = 99
	a.Native[1].Reply[0] = 99
	a.Native[1].Evidence[0].Digest = "changed"
	*a.Key.Dimensions[0].Value.Text = "zone:other"
	a.Value.Symbol.Token = "changed"
	a.Losses[0].Detail = "changed"
	if *in.Observations[1].Number != 0 || in.Observations[1].Request[0] != 1 || in.Observations[1].Reply[0] != 1 || in.Observations[1].Evidence[0].Digest == "changed" || *in.Observations[4].Boolean != true {
		t.Fatal("output mutation reached input")
	}
	if b.Value.Symbol.Token != "auto" || *b.Key.Dimensions[0].Value.Text != "zone:alpha" || *b.Native[1].Number != 0 {
		t.Fatal("outputs alias")
	}
	if b.Native[0].ReceivedAt.UnixNanoseconds != "100" || b.Native[0].ReceiptMonotonic.Nanoseconds != "100" || b.Context.Evaluation.EvaluatedAt.UnixNanoseconds != "110" {
		t.Fatal("receipt replaced with evaluation")
	}
}

func TestThermalZoneModeMappingContext(t *testing.T) {
	for field := 0; field < reflect.TypeOf(thermalZoneModeIdentity{}).NumField(); field++ {
		name := reflect.TypeOf(thermalZoneModeIdentity{}).Field(field).Name
		t.Run("missing/"+name, func(t *testing.T) {
			in := thermalZoneModeFixture()
			reflect.ValueOf(&in.Context.Identity).Elem().Field(field).SetString("")
			for _, o := range in.Observations {
				reflect.ValueOf(&o.Identity).Elem().Field(field).SetString("")
			}
			thermalZoneModeAssert(t, in, "")
		})
		t.Run("replacement/"+name, func(t *testing.T) {
			in := thermalZoneModeFixture()
			f := reflect.ValueOf(&in.Context.Identity).Elem().Field(field)
			f.SetString(f.String() + ":replacement")
			thermalZoneModeAssert(t, in, "")
		})
	}
	for _, ns := range []semreg.Uint64{"", "unknown", "99", "121"} {
		t.Run("evaluation/"+string(ns), func(t *testing.T) {
			in := thermalZoneModeFixture()
			in.Context.Evaluation.EvaluateMonotonic.Nanoseconds = ns
			thermalZoneModeAssert(t, in, "")
		})
	}
	in := thermalZoneModeFixture()
	in.Context.Evaluation.EvaluateMonotonic.Nanoseconds = "120"
	thermalZoneModeAssert(t, in, "auto")
}
