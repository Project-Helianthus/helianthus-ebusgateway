package main

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
)

// All admission-positive evidence below is synthetic, never qualification of
// a family, installation, second native instance, or live runtime binding.
func thermalDhwModeTestInput() thermalDhwModeMappingInput {
	digest := sha256.Sum256([]byte("synthetic DHW read evidence"))
	evidence := semreg.EvidenceRef{Owner: "test.native", Kind: "test.fixture",
		Digest: semreg.Digest(fmt.Sprintf("sha256:%x", digest)), Contract: "test.native/v1",
		Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
	key := thermalDhwModeIdentity{
		Asset: "asset:a", Service: "service:a", Source: "source:a", SourceEpoch: "epoch:a",
		DriverGeneration: "1", Binding: "binding:a", BindingRevision: "1",
		AssociationRevision: "1", SemanticRevision: "1", NativeIdentity: "native:dhw:a",
		Initiator: "initiator:a", Target: "target:a", Family: "controller-family", Decoder: "decoder:u16-u8/v1",
		Selector: thermalDhwModeSelector{PB: 0xB5, SB: 0x24, Opcode: 2, Group: 1, Register: 1,
			RequestDirection: "initiator_to_target", ValueDirection: "target_to_initiator"},
	}
	slot := func(register, raw uint16) thermalDhwModeInput {
		identity := key
		identity.Selector.Register = register
		reply := []byte{byte(raw), byte(raw >> 8)}
		if register == 0x000D {
			reply = reply[:1]
		}
		return thermalDhwModeInput{
			Current: &thermalDhwModeObservation{
				Identity: identity, Raw: &raw, Reply: reply, Validity: semreg.ValidityGood,
				EvidenceStatus: "Unknown", Evidence: evidence,
				ReceivedAt:       semreg.TimePoint{UnixNanoseconds: "10", ClockID: "clock.synthetic", UncertaintyNS: "0"},
				ReceiptMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "10"},
				FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:synthetic", Version: "1.0.0",
					FreshForNS: "100", RetainForNS: "200", MaxWallUncertaintyNS: "0"},
			},
			Qualification: thermalDhwModeQualification{Identity: identity, Qualification: semreg.QualificationQualified,
				Admitted: true, Evidence: evidence},
		}
	}
	return thermalDhwModeMappingInput{Identity: key, Mode: slot(3, 0), SpecialMode: slot(0x000D, 6),
		State: slot(0x000F, 9), ReadService: slot(1, 1), ReadCapability: "capability:a",
		Evaluation: semreg.EvaluationContext{
			EvaluatedAt:       semreg.TimePoint{UnixNanoseconds: "20", ClockID: "clock.synthetic", UncertaintyNS: "0"},
			EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "20"}}}
}

func TestThermalDhwModeMappingNativeSeparation(t *testing.T) {
	in := thermalDhwModeTestInput()
	if err := in.Mode.Current.Evidence.Validate(); err != nil {
		t.Fatal(err)
	}
	out := mapThermalDhwMode(in)
	if len(out.Native) != 3 || len(out.Services) != 1 || len(out.Capabilities) != 1 {
		t.Fatalf("synthetic control: %+v", out)
	}
	for i, want := range []struct {
		register, raw uint16
		width         int
	}{{3, 0, 2}, {0x000D, 6, 1}, {0x000F, 9, 2}} {
		got := out.Native[i]
		if got.Selected == nil || *got.Selected.Raw != want.raw || got.Expected.Selector.Register != want.register ||
			len(got.Selected.Reply) != want.width || !got.NativeEligible || got.ConsumerLoss != "no_dedicated_thermal_definition" {
			t.Fatalf("native component %d lost distinction: %+v", i, got)
		}
	}
	if out.Services[0].Definition.ID != "thermal.service.dhw" ||
		out.Capabilities[0].Definition.ID != "thermal.capability.read.dhw" ||
		out.Capabilities[0].ServiceInstance != "service:a" {
		t.Fatal("wrong read metadata")
	}
	validator := thermal.New()
	if err := validator.ValidateService(out.Services[0]); err != nil {
		t.Fatal(err)
	}
	if err := validator.ValidateCapability(out.Capabilities[0]); err != nil {
		t.Fatal(err)
	}
	// Inventory is owned by Thermal. There is deliberately no fact/disposition
	// collection on the result that could create aliases or overwrite siblings.
	for _, forbidden := range []string{"Facts", "Dispositions", "Mode", "Preset", "Boost", "SystemMode", "Action", "Status"} {
		if _, exists := reflect.TypeOf(out).FieldByName(forbidden); exists {
			t.Fatalf("unexpected projection surface %s", forbidden)
		}
	}
	for _, raw := range []uint16{1, 2, 3, 7} {
		in.Mode.Current.Raw = &raw
		in.Mode.Current.Reply = []byte{byte(raw), 0}
		got := mapThermalDhwMode(in).Native[0]
		if got.Selected == nil || *got.Selected.Raw != raw || got.Selected.EvidenceStatus != "Unknown" ||
			got.ConsumerLoss != "no_dedicated_thermal_definition" {
			t.Fatalf("raw %d normalized or lost: %+v", raw, got)
		}
	}
	in.Mode.Current = nil
	missing := mapThermalDhwMode(in)
	if missing.Native[0].Selected != nil || missing.Native[0].AttemptLoss != "missing" ||
		!missing.Native[1].NativeEligible || !missing.Native[2].NativeEligible {
		t.Fatal("missing mode became auto/off or damaged siblings")
	}
	// Valid special-function zero is evidence, not missing or inferred false.
	zero := uint16(0)
	in.SpecialMode.Current.Raw, in.SpecialMode.Current.Reply = &zero, []byte{0}
	if got := mapThermalDhwMode(in).Native[1]; got.Selected == nil || *got.Selected.Raw != 0 {
		t.Fatal("normal code zero lost")
	}
}

func TestThermalDhwModeMappingIdentityAxes(t *testing.T) {
	// Each case changes only the named axis; full equality must fence it.
	axes := []struct {
		name   string
		mutate func(*thermalDhwModeIdentity)
	}{
		{"asset", func(k *thermalDhwModeIdentity) { k.Asset = "asset:b" }},
		{"service", func(k *thermalDhwModeIdentity) { k.Service = "service:b" }},
		{"source", func(k *thermalDhwModeIdentity) { k.Source = "source:b" }},
		{"source_epoch", func(k *thermalDhwModeIdentity) { k.SourceEpoch = "epoch:b" }},
		{"driver_generation", func(k *thermalDhwModeIdentity) { k.DriverGeneration = "2" }},
		{"binding", func(k *thermalDhwModeIdentity) { k.Binding = "binding:b" }},
		{"binding_revision", func(k *thermalDhwModeIdentity) { k.BindingRevision = "2" }},
		{"association_revision", func(k *thermalDhwModeIdentity) { k.AssociationRevision = "2" }},
		{"semantic_revision", func(k *thermalDhwModeIdentity) { k.SemanticRevision = "2" }},
		{"native_identity", func(k *thermalDhwModeIdentity) { k.NativeIdentity = "native:dhw:b" }},
		{"initiator", func(k *thermalDhwModeIdentity) { k.Initiator = "initiator:b" }},
		{"target", func(k *thermalDhwModeIdentity) { k.Target = "target:b" }},
		{"family", func(k *thermalDhwModeIdentity) { k.Family = "other-family" }},
		{"decoder", func(k *thermalDhwModeIdentity) { k.Decoder = "decoder:other/v1" }},
		{"pb", func(k *thermalDhwModeIdentity) { k.Selector.PB = 0xB4 }},
		{"sb", func(k *thermalDhwModeIdentity) { k.Selector.SB = 0x25 }},
		{"opcode", func(k *thermalDhwModeIdentity) { k.Selector.Opcode = 6 }},
		{"operation_type", func(k *thermalDhwModeIdentity) { k.Selector.OperationType = 1 }},
		{"group", func(k *thermalDhwModeIdentity) { k.Selector.Group = 3 }},
		{"instance", func(k *thermalDhwModeIdentity) { k.Selector.Instance = 1 }},
		{"register", func(k *thermalDhwModeIdentity) { k.Selector.Register = 0x000F }},
		{"request_direction", func(k *thermalDhwModeIdentity) { k.Selector.RequestDirection = "target_to_initiator" }},
		{"value_direction", func(k *thermalDhwModeIdentity) { k.Selector.ValueDirection = "initiator_to_target" }},
	}
	for _, axis := range axes {
		for _, path := range []string{"current", "history", "qualification", "service_current", "service_qualification"} {
			t.Run(axis.name+"/"+path, func(t *testing.T) {
				in := thermalDhwModeTestInput()
				before := mapThermalDhwMode(in)
				switch path {
				case "current":
					axis.mutate(&in.Mode.Current.Identity)
				case "history":
					in.Mode.LastGood, in.Mode.Current = in.Mode.Current, nil
					axis.mutate(&in.Mode.LastGood.Identity)
				case "qualification":
					axis.mutate(&in.Mode.Qualification.Identity)
				case "service_current":
					axis.mutate(&in.ReadService.Current.Identity)
				case "service_qualification":
					axis.mutate(&in.ReadService.Qualification.Identity)
				}
				out := mapThermalDhwMode(in)
				if path == "service_current" || path == "service_qualification" {
					if len(out.Services) != 0 || len(out.Capabilities) != 0 || !reflect.DeepEqual(out.Native, before.Native) {
						t.Fatal("service identity was ignored or native siblings changed")
					}
				} else if out.Native[0].NativeEligible || !reflect.DeepEqual(out.Native[1:], before.Native[1:]) ||
					!reflect.DeepEqual(out.Services, before.Services) {
					t.Fatal("native identity was ignored or siblings changed")
				}
			})
		}
	}
}

func TestThermalDhwModeMappingIndependentQualityAndHistory(t *testing.T) {
	cases := []struct {
		name      string
		mutate    func(*thermalDhwModeMappingInput)
		freshness semreg.Freshness
	}{
		{"stale", func(in *thermalDhwModeMappingInput) {
			in.Mode.Current.ReceiptMonotonic.Nanoseconds = "0"
			in.Mode.Current.FreshnessPolicy.FreshForNS = "20"
		}, semreg.FreshnessStale},
		{"expired", func(in *thermalDhwModeMappingInput) {
			in.Mode.Current.FreshnessPolicy.FreshForNS = "5"
			in.Mode.Current.FreshnessPolicy.RetainForNS = "10"
		}, semreg.FreshnessExpired},
		{"clock_epoch", func(in *thermalDhwModeMappingInput) { in.Mode.Current.ReceiptMonotonic.ClockEpochID = "clock:other" }, semreg.FreshnessUnknown},
		{"future_receipt", func(in *thermalDhwModeMappingInput) { in.Mode.Current.ReceiptMonotonic.Nanoseconds = "21" }, semreg.FreshnessUnknown},
		{"invalid", func(in *thermalDhwModeMappingInput) { in.Mode.Current.Validity = semreg.ValidityBad }, semreg.FreshnessUnknown},
		{"suspect", func(in *thermalDhwModeMappingInput) { in.Mode.Current.Validity = semreg.ValiditySuspect }, semreg.FreshnessUnknown},
		{"unknown_validity", func(in *thermalDhwModeMappingInput) { in.Mode.Current.Validity = semreg.ValidityUnknown }, semreg.FreshnessUnknown},
		{"qualification", func(in *thermalDhwModeMappingInput) {
			in.Mode.Qualification.Qualification = semreg.QualificationUnknown
		}, semreg.FreshnessFresh},
		{"admission", func(in *thermalDhwModeMappingInput) { in.Mode.Qualification.Admitted = false }, semreg.FreshnessFresh},
		{"qualification_evidence", func(in *thermalDhwModeMappingInput) { in.Mode.Qualification.Evidence = semreg.EvidenceRef{} }, semreg.FreshnessFresh},
		{"width", func(in *thermalDhwModeMappingInput) { in.Mode.Current.Reply = []byte{0} }, semreg.FreshnessUnknown},
		{"decode", func(in *thermalDhwModeMappingInput) { in.Mode.Current.Reply = []byte{1, 0} }, semreg.FreshnessUnknown},
		{"missing_value", func(in *thermalDhwModeMappingInput) { in.Mode.Current.Raw = nil }, semreg.FreshnessUnknown},
		{"provenance", func(in *thermalDhwModeMappingInput) { in.Mode.Current.Evidence = semreg.EvidenceRef{} }, semreg.FreshnessUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := thermalDhwModeTestInput()
			before := mapThermalDhwMode(in)
			tc.mutate(&in)
			out := mapThermalDhwMode(in)
			if out.Native[0].NativeEligible || out.Native[0].Freshness != tc.freshness ||
				!reflect.DeepEqual(out.Native[1:], before.Native[1:]) || !reflect.DeepEqual(out.ReadService, before.ReadService) {
				t.Fatal("independent quality/receipt axes conflated")
			}
			if in.Mode.Current.Validity != semreg.ValidityGood && out.Native[0].AttemptFreshness != semreg.FreshnessFresh {
				t.Fatal("invalidity changed the independent attempt age")
			}
			if out.Native[0].Qualification != in.Mode.Qualification || out.Native[0].Attempt.Validity != in.Mode.Current.Validity ||
				out.Native[0].Attempt.EvidenceStatus != "Unknown" {
				t.Fatal("qualification, validity or evidence status was collapsed")
			}
		})
	}
	in := thermalDhwModeTestInput()
	in.Mode.LastGood = thermalDhwModeClone(in.Mode.Current)
	in.Mode.Current.Validity = semreg.ValidityBad
	out := mapThermalDhwMode(in)
	if out.Native[0].Selected == nil || *out.Native[0].Selected.Raw != 0 || !out.Native[0].NativeEligible || out.Native[0].AttemptLoss != "invalid_or_unknown" {
		t.Fatal("failed read discarded last-known-good off")
	}
	in.Evaluation.EvaluateMonotonic.Nanoseconds = "110"
	in.SpecialMode.Current.ReceiptMonotonic.Nanoseconds = "109"
	out = mapThermalDhwMode(in)
	if out.Native[0].Freshness != semreg.FreshnessStale || out.Native[0].NativeEligible ||
		out.Native[1].Freshness != semreg.FreshnessFresh || !out.Native[1].NativeEligible ||
		out.Native[0].Selected.ReceiptMonotonic.Nanoseconds != "10" || out.Native[0].Selected.ReceivedAt.UnixNanoseconds != "10" {
		t.Fatal("fresh special function/evaluation rejuvenated mode receipt")
	}
	// Fresh mode must likewise leave the state and special-mode ages unchanged.
	in = thermalDhwModeTestInput()
	in.SpecialMode.Current.ReceiptMonotonic.Nanoseconds = "0"
	in.SpecialMode.Current.FreshnessPolicy.FreshForNS = "20"
	in.State.Current.ReceiptMonotonic.Nanoseconds = "0"
	in.State.Current.FreshnessPolicy.FreshForNS = "20"
	out = mapThermalDhwMode(in)
	if !out.Native[0].NativeEligible || out.Native[1].Freshness != semreg.FreshnessStale || out.Native[2].Freshness != semreg.FreshnessStale {
		t.Fatal("fresh mode rejuvenated special-function components")
	}
}

func TestThermalDhwModeMappingIndependentReadService(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*thermalDhwModeMappingInput)
	}{
		{"absent", func(in *thermalDhwModeMappingInput) { in.ReadService.Current = nil }},
		{"presence_zero", func(in *thermalDhwModeMappingInput) {
			raw := uint16(0)
			in.ReadService.Current.Raw = &raw
			in.ReadService.Current.Reply = []byte{0, 0}
		}},
		{"unqualified", func(in *thermalDhwModeMappingInput) {
			in.ReadService.Qualification.Qualification = semreg.QualificationCandidate
		}},
		{"not_admitted", func(in *thermalDhwModeMappingInput) { in.ReadService.Qualification.Admitted = false }},
		{"bad", func(in *thermalDhwModeMappingInput) { in.ReadService.Current.Validity = semreg.ValidityBad }},
		{"stale", func(in *thermalDhwModeMappingInput) { in.ReadService.Current.FreshnessPolicy.FreshForNS = "10" }},
		{"history_only", func(in *thermalDhwModeMappingInput) {
			in.ReadService.LastGood = in.ReadService.Current
			in.ReadService.Current = nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := thermalDhwModeTestInput()
			before := mapThermalDhwMode(in)
			tc.mutate(&in)
			out := mapThermalDhwMode(in)
			if len(out.Services) != 0 || len(out.Capabilities) != 0 || !reflect.DeepEqual(out.Native, before.Native) {
				t.Fatal("native observations supplied read-service qualification")
			}
		})
	}
	in := thermalDhwModeTestInput()
	in.Mode.Current, in.SpecialMode.Current, in.State.Current = nil, nil, nil
	if out := mapThermalDhwMode(in); len(out.Services) != 1 || len(out.Capabilities) != 1 {
		t.Fatal("separately qualified service depended on mode coverage")
	}
}

func TestThermalDhwModeMappingSelectorsAndWidths(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*thermalDhwModeMappingInput)
		index  int
	}{
		{"special_mode_is_not_state", func(in *thermalDhwModeMappingInput) { in.SpecialMode.Current.Identity.Selector.Register = 0x000F }, 1},
		{"state_is_not_special_mode", func(in *thermalDhwModeMappingInput) { in.State.Current.Identity.Selector.Register = 0x000D }, 2},
		{"special_mode_is_one_byte", func(in *thermalDhwModeMappingInput) { in.SpecialMode.Current.Reply = []byte{6, 0} }, 1},
		{"state_is_two_bytes", func(in *thermalDhwModeMappingInput) { in.State.Current.Reply = []byte{9} }, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := thermalDhwModeTestInput()
			before := mapThermalDhwMode(in)
			tc.mutate(&in)
			out := mapThermalDhwMode(in)
			for i := range out.Native {
				if i == tc.index {
					if out.Native[i].NativeEligible || out.Native[i].Selected != nil || out.Native[i].Attempt == nil {
						t.Fatal("conflated selector/width or discarded failed attempt")
					}
				} else if !reflect.DeepEqual(out.Native[i], before.Native[i]) {
					t.Fatal("unaffected sibling changed")
				}
			}
		})
	}
}

func TestThermalDhwModeMappingTwoServicesAndDetachment(t *testing.T) {
	a, b := thermalDhwModeTestInput(), thermalDhwModeTestInput()
	change := func(k *thermalDhwModeIdentity) {
		k.Asset, k.Service, k.Source, k.SourceEpoch = "asset:b", "service:b", "source:b", "epoch:b"
		k.Binding, k.NativeIdentity = "binding:b", "native:dhw:b"
	}
	change(&b.Identity)
	for _, slot := range []*thermalDhwModeInput{&b.Mode, &b.SpecialMode, &b.State, &b.ReadService} {
		change(&slot.Current.Identity)
		change(&slot.Qualification.Identity)
	}
	b.ReadCapability = "capability:b"
	raw := uint16(2)
	b.Mode.Current.Raw, b.Mode.Current.Reply = &raw, []byte{2, 0}
	oa, ob := mapThermalDhwMode(a), mapThermalDhwMode(b)
	if len(ob.Services) != 1 || ob.Services[0].InstanceID != "service:b" || oa.Services[0].InstanceID != "service:a" ||
		*oa.Native[0].Selected.Raw != 0 || *ob.Native[0].Selected.Raw != 2 ||
		oa.Native[0].Selected.Identity.Selector.Instance != 0 || ob.Native[0].Selected.Identity.Selector.Instance != 0 {
		t.Fatal("two local instance-zero identities collapsed")
	}
	a.Mode.LastGood = thermalDhwModeClone(a.Mode.Current)
	before := mapThermalDhwMode(a)
	out := mapThermalDhwMode(a)
	a.Mode.Current.Reply[0], *a.Mode.Current.Raw = 8, 8
	a.Mode.LastGood.Reply[0], *a.Mode.LastGood.Raw = 7, 7
	a.ReadService.Current.Reply[0], *a.ReadService.Current.Raw = 0, 0
	if !reflect.DeepEqual(out, before) {
		t.Fatal("input mutation changed output")
	}
	out.Native[0].Attempt.Reply[0], *out.Native[0].Attempt.Raw = 3, 3
	if *out.Native[0].Selected.Raw != 0 || out.Native[0].Selected.Reply[0] != 0 {
		t.Fatal("output attempt aliases selected")
	}
	out.Native[0].History.Reply[0], *out.Native[0].History.Raw = 4, 4
	out.Native[0].Selected.Reply[0], *out.Native[0].Selected.Raw = 5, 5
	out.ReadService.Selected.Reply[0] = 6
	out.Capabilities[0].ActivationEvidence[0].Kind = "test.changed"
	if *a.Mode.Current.Raw != 8 || a.Mode.Current.Reply[0] != 8 || *a.Mode.LastGood.Raw != 7 ||
		a.ReadService.Current.Reply[0] != 0 || a.ReadService.Qualification.Evidence.Kind != "test.fixture" ||
		!reflect.DeepEqual(before, mapThermalDhwMode(thermalDhwModeTestInputWithHistory())) {
		t.Fatal("output mutation changed input or independent evaluation")
	}
}

func thermalDhwModeTestInputWithHistory() thermalDhwModeMappingInput {
	in := thermalDhwModeTestInput()
	in.Mode.LastGood = thermalDhwModeClone(in.Mode.Current)
	return in
}
