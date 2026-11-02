package main

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func thermalCircuitPumpTestObservation(register uint16, value uint32, receipt string) thermalCircuitPumpObservation {
	digest := sha256.Sum256([]byte("synthetic circuit pump evidence"))
	o := thermalCircuitPumpObservation{
		Identity: thermalCircuitPumpIdentity{
			Asset: "asset:synthetic-circuit", Service: "service:synthetic-circuit", Source: "source:synthetic-controller",
			SourceEpoch: "epoch:synthetic-controller", DriverGeneration: "1", Binding: "binding:synthetic-controller",
			BindingRevision: "2", AssociationRevision: "3", SemanticRevision: "4",
			Selector: thermalCircuitPumpSelector{Source: 0x31, Target: 0x42, PB: 0xb5, SB: 0x24, Opcode: 2, Group: 2, Instance: 1, Register: register, RequestDirection: "initiator_to_target", ValueDirection: "target_to_initiator"},
		},
		Decoded: &value, Raw: []byte{0x10, 0x20}, WireType: "u32", Unit: "hours",
		Evidence:       []semreg.EvidenceRef{{Owner: "native.synthetic", Kind: "native.synthetic.observation", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", digest)), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}},
		EvidenceStatus: "Unknown", Applicability: "Unknown", Qualification: semreg.QualificationUnknown, Validity: semreg.ValidityGood,
		Acquisition: "wire", ReceivedAt: semreg.TimePoint{UnixNanoseconds: semreg.Int64(receipt), ClockID: "clock.synthetic", UncertaintyNS: "0"},
		ReceiptMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock-epoch:synthetic", Nanoseconds: semreg.Uint64(receipt)}, FreshForNS: "5",
	}
	switch register {
	case 0x001b:
		o.WireType, o.Unit = "u16", "raw"
	case 0x0025:
		o.Unit = "count"
	}
	return o
}

func thermalCircuitPumpTestEvaluation() semreg.EvaluationContext {
	return semreg.EvaluationContext{EvaluatedAt: semreg.TimePoint{UnixNanoseconds: "100", ClockID: "clock.synthetic", UncertaintyNS: "0"}, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock-epoch:synthetic", Nanoseconds: "20"}}
}

func thermalCircuitPumpTestWithheld(t *testing.T, got thermalCircuitPumpMapping, count int) {
	t.Helper()
	if len(got.Facts) != 0 || len(got.SourceLoss) != count || len(got.Members) != count {
		t.Fatalf("unexpected facts or missing native/loss members: %+v", got)
	}
	if got.ServiceDefinition.ID != "thermal.service.circuit" || got.CircuitDimension.ID != "thermal.dimension.circuit" {
		t.Fatalf("wrong owned service scope: %+v", got)
	}
	for i, loss := range got.SourceLoss {
		if loss.Identity != got.Members[i].Identity || loss.Outcome != projection.ProjectionWithheld || loss.Detail.Kind != projection.LossPolicy || loss.Detail.Description == "" || len(loss.Detail.SourceItems) != 0 || loss.Detail.Reversible {
			t.Fatalf("loss invents a definition or drops source identity: %+v", loss)
		}
		if got.Members[i].Meaning != "Unknown" || got.Members[i].Continuity != "Unknown" {
			t.Fatalf("native meaning/continuity invented: %+v", got.Members[i])
		}
	}
}

func TestThermalCircuitPumpMappingStatusAndCounters(t *testing.T) {
	for _, raw := range []uint32{0, 1, 2, 0xffff} {
		t.Run(fmt.Sprintf("status_%d", raw), func(t *testing.T) {
			o := thermalCircuitPumpTestObservation(0x001b, raw, "19")
			got := mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: o.Identity, Attempts: []thermalCircuitPumpObservation{o}}}, thermalCircuitPumpTestEvaluation())
			thermalCircuitPumpTestWithheld(t, got, 1)
			if got.Members[0].Current == nil || *got.Members[0].Current.Decoded != raw || got.Members[0].Freshness != semreg.FreshnessFresh {
				t.Fatalf("raw status collapsed or zero lost: %+v", got.Members[0])
			}
		})
	}
	for _, register := range []uint16{0x001b, 0x0024, 0x0025} {
		t.Run(fmt.Sprintf("missing_%x", register), func(t *testing.T) {
			o := thermalCircuitPumpTestObservation(register, 0, "19")
			o.Decoded = nil
			got := mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: o.Identity, Attempts: []thermalCircuitPumpObservation{o}}}, thermalCircuitPumpTestEvaluation())
			thermalCircuitPumpTestWithheld(t, got, 1)
			if got.Members[0].Current != nil || got.Members[0].Reason != "invalid_or_failed" || got.Members[0].Attempts[0].Decoded != nil {
				t.Fatal("unknown became zero/false")
			}
		})
	}
	for _, register := range []uint16{0x0024, 0x0025} {
		t.Run(fmt.Sprintf("counter_%x", register), func(t *testing.T) {
			prior := thermalCircuitPumpTestObservation(register, 10, "10")
			for _, value := range []uint32{0, 3, 12} {
				current := thermalCircuitPumpTestObservation(register, value, "19")
				got := mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: prior.Identity, Previous: &prior, Attempts: []thermalCircuitPumpObservation{current}}}, thermalCircuitPumpTestEvaluation())
				thermalCircuitPumpTestWithheld(t, got, 1)
				m := got.Members[0]
				if m.Current == nil || *m.Current.Decoded != value || len(m.Historical) != 1 || *m.Historical[0].Decoded != 10 || m.Historical[0].ReceivedAt.UnixNanoseconds != "10" || m.Current.ReceivedAt.UnixNanoseconds != "19" {
					t.Fatalf("counter or independent history lost: %+v", m)
				}
				wantReason := "observed"
				if value < 10 {
					wantReason = "counter_decreased_continuity_unknown"
				}
				if m.Reason != wantReason {
					t.Fatalf("reason = %q, want %q", m.Reason, wantReason)
				}
			}
			sentinel := thermalCircuitPumpTestObservation(register, ^uint32(0), "19")
			got := mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: prior.Identity, Previous: &prior, Attempts: []thermalCircuitPumpObservation{sentinel}}}, thermalCircuitPumpTestEvaluation())
			m := got.Members[0]
			if m.Current == nil || *m.Current.Decoded != 10 || !m.Retained || m.Reason != "invalid_or_failed" || m.Freshness != semreg.FreshnessStale || m.Current.ReceiptMonotonic.Nanoseconds != "10" || *m.Attempts[0].Decoded != ^uint32(0) {
				t.Fatalf("sentinel renewed or destroyed prior evidence: %+v", m)
			}
		})
	}
}

func TestThermalCircuitPumpMappingFreshnessDeadline(t *testing.T) {
	for _, register := range []uint16{0x001b, 0x0024, 0x0025} {
		for _, tc := range []struct {
			name, receipt string
			freshFor      semreg.Uint64
			want          semreg.Freshness
		}{
			{"before_deadline", "16", "5", semreg.FreshnessFresh},
			{"at_deadline", "15", "5", semreg.FreshnessStale},
			{"after_deadline", "14", "5", semreg.FreshnessStale},
			{"zero_window", "20", "0", semreg.FreshnessStale},
		} {
			t.Run(fmt.Sprintf("%x/%s", register, tc.name), func(t *testing.T) {
				o := thermalCircuitPumpTestObservation(register, 1, tc.receipt)
				o.FreshForNS = tc.freshFor
				got := mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: o.Identity, Attempts: []thermalCircuitPumpObservation{o}}}, thermalCircuitPumpTestEvaluation())
				thermalCircuitPumpTestWithheld(t, got, 1)
				if m := got.Members[0]; m.Current == nil || m.Freshness != tc.want || m.Current.ReceiptMonotonic != o.ReceiptMonotonic {
					t.Fatalf("freshness deadline or receipt changed: %+v, want %s", m, tc.want)
				}
			})
		}
	}
}

func TestThermalCircuitPumpMappingSourceLossDescription(t *testing.T) {
	for _, tc := range []struct {
		register uint16
		want     string
	}{
		{0x001b, "no Thermal pump status definition; native codebook and active meaning remain Unknown"},
		{0x0024, "no Thermal pump operating-hours definition; native total retained without delta or rate"},
		{0x0025, "no Thermal pump starts definition; native total retained without delta or rate"},
		{0x001e, "unsupported native register; no Thermal pump mapping"},
		{0x0026, "unsupported native register; no Thermal pump mapping"},
	} {
		t.Run(fmt.Sprintf("%x", tc.register), func(t *testing.T) {
			o := thermalCircuitPumpTestObservation(tc.register, 1, "19")
			got := mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: o.Identity, Attempts: []thermalCircuitPumpObservation{o}}}, thermalCircuitPumpTestEvaluation())
			thermalCircuitPumpTestWithheld(t, got, 1)
			if description := got.SourceLoss[0].Detail.Description; description != tc.want {
				t.Fatalf("source loss description = %q, want %q", description, tc.want)
			}
			if (tc.register == 0x001e || tc.register == 0x0026) && (got.Members[0].Current != nil || got.Members[0].Reason != "unsupported_identity") {
				t.Fatal("unsupported register admitted")
			}
		})
	}
}

func TestThermalCircuitPumpMappingIndependentAgesAndFailedSibling(t *testing.T) {
	status := thermalCircuitPumpTestObservation(0x001b, 0, "10")
	hours := thermalCircuitPumpTestObservation(0x0024, 0, "11")
	starts := thermalCircuitPumpTestObservation(0x0025, 0, "12")
	refreshed := thermalCircuitPumpTestObservation(0x001b, 2, "19")
	failed := thermalCircuitPumpTestObservation(0x0024, 7, "19")
	failed.Failure, failed.Validity, failed.Raw = "synthetic_invalid_response", semreg.ValidityBad, []byte{0xff}
	got := mapThermalCircuitPump([]thermalCircuitPumpInput{
		{Identity: status.Identity, Previous: &status, Attempts: []thermalCircuitPumpObservation{refreshed}},
		{Identity: hours.Identity, Previous: &hours, Attempts: []thermalCircuitPumpObservation{failed}},
		{Identity: starts.Identity, Previous: &starts},
	}, thermalCircuitPumpTestEvaluation())
	thermalCircuitPumpTestWithheld(t, got, 3)
	for i, want := range []struct {
		receipt  string
		value    uint32
		fresh    semreg.Freshness
		retained bool
	}{
		{"19", 2, semreg.FreshnessFresh, false}, {"11", 0, semreg.FreshnessStale, true}, {"12", 0, semreg.FreshnessStale, true},
	} {
		m := got.Members[i]
		if m.Current == nil || string(m.Current.ReceivedAt.UnixNanoseconds) != want.receipt || string(m.Current.ReceiptMonotonic.Nanoseconds) != want.receipt || *m.Current.Decoded != want.value || m.Freshness != want.fresh || m.Retained != want.retained {
			t.Fatalf("member %d renewed sibling or lost zero: %+v", i, m)
		}
	}
	if got.Members[1].Attempts[0].Failure != "synthetic_invalid_response" || !reflect.DeepEqual(got.Members[1].Attempts[0].Raw, []byte{0xff}) {
		t.Fatal("failed response evidence lost")
	}
}

func TestThermalCircuitPumpMappingEveryIdentityAxis(t *testing.T) {
	mutations := []struct {
		name   string
		change func(*thermalCircuitPumpIdentity)
	}{
		{"asset", func(i *thermalCircuitPumpIdentity) { i.Asset = "asset:replacement" }},
		{"service", func(i *thermalCircuitPumpIdentity) { i.Service = "service:replacement" }},
		{"source", func(i *thermalCircuitPumpIdentity) { i.Source = "source:replacement" }},
		{"source_epoch", func(i *thermalCircuitPumpIdentity) { i.SourceEpoch = "epoch:replacement" }},
		{"driver_generation", func(i *thermalCircuitPumpIdentity) { i.DriverGeneration = "9" }},
		{"binding", func(i *thermalCircuitPumpIdentity) { i.Binding = "binding:replacement" }},
		{"binding_revision", func(i *thermalCircuitPumpIdentity) { i.BindingRevision = "9" }},
		{"association_revision", func(i *thermalCircuitPumpIdentity) { i.AssociationRevision = "9" }},
		{"semantic_revision", func(i *thermalCircuitPumpIdentity) { i.SemanticRevision = "9" }},
		{"selector_source", func(i *thermalCircuitPumpIdentity) { i.Selector.Source++ }},
		{"selector_target", func(i *thermalCircuitPumpIdentity) { i.Selector.Target++ }},
		{"pb", func(i *thermalCircuitPumpIdentity) { i.Selector.PB++ }},
		{"sb", func(i *thermalCircuitPumpIdentity) { i.Selector.SB++ }},
		{"opcode", func(i *thermalCircuitPumpIdentity) { i.Selector.Opcode = 6 }},
		{"operation_type", func(i *thermalCircuitPumpIdentity) { i.Selector.OperationType++ }},
		{"group", func(i *thermalCircuitPumpIdentity) { i.Selector.Group++ }},
		{"instance", func(i *thermalCircuitPumpIdentity) { i.Selector.Instance++ }},
		{"register", func(i *thermalCircuitPumpIdentity) { i.Selector.Register = 0x0026 }},
		{"request_direction", func(i *thermalCircuitPumpIdentity) { i.Selector.RequestDirection = "target_to_initiator" }},
		{"value_direction", func(i *thermalCircuitPumpIdentity) { i.Selector.ValueDirection = "initiator_to_target" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			base := thermalCircuitPumpTestObservation(0x0024, 10, "10")
			sibling := thermalCircuitPumpTestObservation(0x0025, 7, "19")
			sibling.Identity.Selector.Instance = 2
			other := thermalCircuitPumpTestObservation(0x0024, 99, "19")
			mutation.change(&other.Identity)
			// Both paths matter: a mismatched incoming sample must not be selected,
			// and a mismatched prior must not survive replacement at the same index.
			for _, priorPath := range []bool{false, true} {
				input := thermalCircuitPumpInput{Identity: base.Identity, Attempts: []thermalCircuitPumpObservation{other}}
				if priorPath {
					input.Previous, input.Attempts = &other, nil
				}
				got := mapThermalCircuitPump([]thermalCircuitPumpInput{input, {Identity: sibling.Identity, Attempts: []thermalCircuitPumpObservation{sibling}}}, thermalCircuitPumpTestEvaluation())
				thermalCircuitPumpTestWithheld(t, got, 2)
				if got.Members[0].Current != nil || got.Members[0].Retained || got.Members[1].Current == nil || *got.Members[1].Current.Decoded != 7 || got.Members[1].Current.Identity != sibling.Identity || got.Members[1].Freshness != semreg.FreshnessFresh {
					t.Fatalf("axis %s leaked identity or damaged sibling (prior=%v): %+v", mutation.name, priorPath, got.Members)
				}
				if priorPath && (len(got.Members[0].Historical) != 1 || got.Members[0].Historical[0].Identity != other.Identity || *got.Members[0].Historical[0].Decoded != 99) {
					t.Fatal("replaced evidence not retained under original identity")
				}
			}
		})
	}
}

func TestThermalCircuitPumpMappingIndependentQuality(t *testing.T) {
	for _, qualification := range []semreg.Qualification{semreg.QualificationUnknown, semreg.QualificationCandidate, semreg.QualificationQualified, semreg.QualificationRejected} {
		for _, validity := range []semreg.Validity{semreg.ValidityGood, semreg.ValidityBad, semreg.ValiditySuspect, semreg.ValidityUnknown} {
			for _, receipt := range []string{"10", "19"} {
				t.Run(fmt.Sprintf("%s_%s_%s", qualification, validity, receipt), func(t *testing.T) {
					o := thermalCircuitPumpTestObservation(0x001b, 1, receipt)
					o.Qualification, o.Validity = qualification, validity
					got := mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: o.Identity, Attempts: []thermalCircuitPumpObservation{o}}}, thermalCircuitPumpTestEvaluation())
					thermalCircuitPumpTestWithheld(t, got, 1)
					m := got.Members[0]
					if validity != semreg.ValidityGood {
						if m.Current != nil || m.Attempts[0].Validity != validity || m.Attempts[0].Qualification != qualification {
							t.Fatal("validity collapsed with qualification/freshness")
						}
						return
					}
					wantFresh := semreg.FreshnessFresh
					if receipt == "10" {
						wantFresh = semreg.FreshnessStale
					}
					if m.Current == nil || m.Current.Qualification != qualification || m.Freshness != wantFresh || m.Current.ReceivedAt != o.ReceivedAt || m.Current.ReceiptMonotonic != o.ReceiptMonotonic || m.Current.EvidenceStatus != "Unknown" || got.Evaluation.EvaluatedAt.UnixNanoseconds != "100" {
						t.Fatal("qualification, evidence grade, freshness or receipt conflated")
					}
				})
			}
		}
	}
}

func TestThermalCircuitPumpMappingReplacementConflictAndReplay(t *testing.T) {
	old := thermalCircuitPumpTestObservation(0x0024, 10, "10")
	replacement := old.Identity
	replacement.SourceEpoch = "epoch:replacement"
	got := mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: replacement, Previous: &old}}, thermalCircuitPumpTestEvaluation())
	if got.Members[0].Current != nil || len(got.Members[0].Historical) != 1 || got.Members[0].Historical[0].Identity != old.Identity {
		t.Fatal("replacement inherited old counter")
	}
	a := thermalCircuitPumpTestObservation(0x0024, 11, "19")
	b := thermalCircuitPumpTestObservation(0x0024, 12, "18")
	for _, attempts := range [][]thermalCircuitPumpObservation{{a, b}, {b, a}} {
		got = mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: old.Identity, Previous: &old, Attempts: attempts}}, thermalCircuitPumpTestEvaluation())
		if m := got.Members[0]; m.Reason != "conflict" || !m.Retained || m.Current == nil || *m.Current.Decoded != 10 || len(m.Attempts) != 2 || m.Freshness != semreg.FreshnessStale {
			t.Fatalf("conflict selected arrival winner: %+v", m)
		}
	}
	for _, receipt := range []string{"9", "10"} {
		replay := thermalCircuitPumpTestObservation(0x0024, 1, receipt)
		replay.Acquisition = "cache"
		got = mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: old.Identity, Previous: &old, Attempts: []thermalCircuitPumpObservation{replay}}}, thermalCircuitPumpTestEvaluation())
		if m := got.Members[0]; m.Current == nil || *m.Current.Decoded != 10 || m.Reason != "receipt_not_newer" || m.Current.ReceivedAt.UnixNanoseconds != "10" {
			t.Fatal("cache/reordered receipt renewed sample")
		}
	}
	for _, change := range []func(*semreg.EvaluationContext){
		func(e *semreg.EvaluationContext) { e.EvaluateMonotonic.ClockEpochID = "clock-epoch:replacement" },
		func(e *semreg.EvaluationContext) { e.EvaluateMonotonic.Nanoseconds = "9" },
		func(e *semreg.EvaluationContext) { e.EvaluateMonotonic.Nanoseconds = "invalid" },
	} {
		e := thermalCircuitPumpTestEvaluation()
		change(&e)
		got = mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: old.Identity, Previous: &old}}, e)
		if got.Members[0].Freshness != semreg.FreshnessUnknown || got.Members[0].Current.ReceivedAt != old.ReceivedAt {
			t.Fatal("incomparable evaluation changed original receipt or invented freshness")
		}
	}
}

func TestThermalCircuitPumpMappingReplacementAllMembers(t *testing.T) {
	var inputs []thermalCircuitPumpInput
	for _, register := range []uint16{0x001b, 0x0024, 0x0025} {
		old := thermalCircuitPumpTestObservation(register, 10, "10")
		replacement := old.Identity
		replacement.Source, replacement.SourceEpoch = "source:replacement", "epoch:replacement"
		input := thermalCircuitPumpInput{Identity: replacement, Previous: &old}
		if register == 0x001b {
			status := thermalCircuitPumpTestObservation(register, 0, "19")
			status.Identity = replacement
			input.Attempts = []thermalCircuitPumpObservation{status}
		}
		inputs = append(inputs, input)
	}
	got := mapThermalCircuitPump(inputs, thermalCircuitPumpTestEvaluation())
	thermalCircuitPumpTestWithheld(t, got, 3)
	if got.Members[0].Current == nil || *got.Members[0].Current.Decoded != 0 || got.Members[0].Freshness != semreg.FreshnessFresh {
		t.Fatal("replacement status zero lost")
	}
	for i, member := range got.Members {
		if len(member.Historical) != 1 || member.Historical[0].Identity.Source != "source:synthetic-controller" || member.Historical[0].Identity.SourceEpoch != "epoch:synthetic-controller" || *member.Historical[0].Decoded != 10 {
			t.Fatal("replacement lost original evidence identity")
		}
		if i > 0 && (member.Current != nil || member.Retained || member.Freshness != semreg.FreshnessUnknown) {
			t.Fatal("replacement status inherited or renewed old counters")
		}
	}
}

func TestThermalCircuitPumpMappingNoSubstitution(t *testing.T) {
	for _, change := range []func(*thermalCircuitPumpIdentity){
		func(i *thermalCircuitPumpIdentity) { i.Selector.Register = 0x001e },
		func(i *thermalCircuitPumpIdentity) { i.Selector.Opcode = 6 },
		func(i *thermalCircuitPumpIdentity) { i.Selector.Group = 0 },
		func(i *thermalCircuitPumpIdentity) { i.SourceEpoch = "" },
		func(i *thermalCircuitPumpIdentity) { i.Binding = "" },
	} {
		o := thermalCircuitPumpTestObservation(0x001b, 1, "19")
		change(&o.Identity)
		got := mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: o.Identity, Attempts: []thermalCircuitPumpObservation{o}}}, thermalCircuitPumpTestEvaluation())
		thermalCircuitPumpTestWithheld(t, got, 1)
		if got.Members[0].Current != nil || got.Members[0].Reason != "unsupported_identity" || len(got.Members[0].Attempts) != 1 {
			t.Fatal("RR001E/system/other namespace or incomplete provenance admitted")
		}
	}
	// Circuit zero is still a circuit. There is no boiler/system FactKey or
	// requested-item accounting output, including for synthetic qualified input.
	o := thermalCircuitPumpTestObservation(0x001b, 1, "19")
	o.Identity.Selector.Instance, o.Qualification = 0, semreg.QualificationQualified
	got := mapThermalCircuitPump([]thermalCircuitPumpInput{{Identity: o.Identity, Attempts: []thermalCircuitPumpObservation{o}}}, thermalCircuitPumpTestEvaluation())
	thermalCircuitPumpTestWithheld(t, got, 1)
	if got.Members[0].Current == nil || got.Members[0].Current.Identity.Selector.Instance != 0 {
		t.Fatal("circuit zero incorrectly dropped")
	}
}

func TestThermalCircuitPumpMappingDetached(t *testing.T) {
	prior := thermalCircuitPumpTestObservation(0x0024, 10, "10")
	current := thermalCircuitPumpTestObservation(0x0024, 3, "19")
	input := []thermalCircuitPumpInput{{Identity: prior.Identity, Previous: &prior, Attempts: []thermalCircuitPumpObservation{current}}}
	got := mapThermalCircuitPump(input, thermalCircuitPumpTestEvaluation())
	*prior.Decoded, prior.Raw[0], prior.Evidence[0].Kind = 99, 99, "changed.input"
	*current.Decoded, current.Raw[0], current.Evidence[0].Kind = 88, 88, "changed.input"
	m := &got.Members[0]
	if *m.Current.Decoded != 3 || m.Current.Raw[0] != 0x10 || m.Current.Evidence[0].Kind != "native.synthetic.observation" || *m.Historical[0].Decoded != 10 || m.Historical[0].Raw[0] != 0x10 || m.Historical[0].Evidence[0].Kind != "native.synthetic.observation" || *m.Attempts[0].Decoded != 3 {
		t.Fatal("input mutation escaped into output")
	}
	*m.Current.Decoded, m.Current.Raw[0], m.Current.Evidence[0].Kind = 77, 77, "changed.output"
	if *current.Decoded != 88 || current.Raw[0] != 88 || current.Evidence[0].Kind != "changed.input" || *m.Attempts[0].Decoded != 3 || m.Attempts[0].Raw[0] != 0x10 || m.Attempts[0].Evidence[0].Kind != "native.synthetic.observation" {
		t.Fatal("output mutation escaped into input or sibling evidence")
	}
	*m.Historical[0].Decoded, m.Historical[0].Raw[0], m.Historical[0].Evidence[0].Kind = 66, 66, "changed.history"
	if *prior.Decoded != 99 || prior.Raw[0] != 99 || prior.Evidence[0].Kind != "changed.input" {
		t.Fatal("historical evidence aliases input")
	}
}
