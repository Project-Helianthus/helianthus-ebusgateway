package main

import (
	"crypto/sha256"
	"fmt"
	"math"
	"reflect"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func i32Input() dhwMeasuredRequest {
	digest := sha256.Sum256([]byte("synthetic DHW candidate evidence"))
	evidence := semreg.EvidenceRef{Owner: "test.native", Kind: "test.fixture", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", digest)), Contract: "test.native/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
	key := dhwMeasuredIdentity{Asset: "asset:a", Service: "service:a", Source: "source:a", Epoch: "epoch:a", Generation: "1", BindingRevision: "1", AssociationRevision: "1", SemanticRevision: "1", Binding: "binding:a", Native: "native:dhw", Family: "controller-family", Profile: "profile:synthetic", ProfileRevision: "1", DecoderRevision: "1", Selector: dhwMeasuredSelector{PB: 0xb5, SB: 0x24, Opcode: 2, Group: 1, Register: 5, Direction: "target_to_initiator"}}
	input := func(k dhwMeasuredIdentity, codec, unit string, value []byte) dhwMeasuredInput {
		return dhwMeasuredInput{Codec: dhwMeasuredCodec{Identity: k, Qualified: true, Codec: codec, Unit: unit, BoundsKnown: true, Minimum: -50, Maximum: 150, Evidence: evidence}, Attempt: &dhwMeasuredObservation{Identity: k, Value: value, RawRequest: []byte{2, 1, 0}, RawReply: append([]byte{2, 1, 0}, value...), Correlated: true, Outcome: "ok", EvidenceStatus: "Unknown", Evidence: evidence, ReceivedAt: semreg.TimePoint{UnixNanoseconds: "10", ClockID: "clock.synthetic", UncertaintyNS: "0"}, Receipt: semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "10"}, FreshForNS: "100"}}
	}
	p := key
	p.Selector.Register = 1
	return dhwMeasuredRequest{Identity: key, Measurement: input(key, "f32-le", "unit.celsius", []byte{0, 0, 0x34, 0x42}), Presence: input(p, "uin16-le", "native.uin", []byte{1, 0}), Evaluation: semreg.EvaluationContext{EvaluatedAt: semreg.TimePoint{UnixNanoseconds: "20", ClockID: "clock.synthetic", UncertaintyNS: "0"}, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "20"}}}
}

func i32AssertWithheld(t *testing.T, out dhwMeasuredCandidate) {
	t.Helper()
	if out.Withheld != "final fact key and decimal policy unresolved" || len(out.Facts) != 0 {
		t.Fatalf("invented publication: %+v", out)
	}
}

func TestI32NativeVectors(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
		want float64
		loss string
	}{
		{"zero", []byte{0, 0, 0, 0}, 0, ""},
		{"fraction", []byte{0, 0, 0x36, 0x42}, 45.5, ""},
		{"negative_zero", []byte{0, 0, 0, 0x80}, math.Copysign(0, -1), ""},
		{"missing", nil, 0, "temperature width is not four"},
		{"short", []byte{0, 0, 0x34}, 0, "temperature width is not four"},
		{"overlength", []byte{0, 0, 0x34, 0x42, 0}, 0, "temperature width is not four"},
		{"nan", []byte{0, 0, 0xc0, 0x7f}, 0, "nonfinite or outside supplied bounds"},
		{"positive_inf", []byte{0, 0, 0x80, 0x7f}, 0, "nonfinite or outside supplied bounds"},
		{"negative_inf", []byte{0, 0, 0x80, 0xff}, 0, "nonfinite or outside supplied bounds"},
		{"profile_low", []byte{0, 0, 0x4c, 0xc2}, 0, "nonfinite or outside supplied bounds"},  // -51
		{"profile_high", []byte{0, 0, 0x17, 0x43}, 0, "nonfinite or outside supplied bounds"}, // 151
		{"lower_boundary", []byte{0, 0, 0x48, 0xc2}, -50, ""},
		{"upper_boundary", []byte{0, 0, 0x16, 0x43}, 150, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := i32Input()
			in.Measurement.Attempt.Value = tc.raw
			out := mapDHWMeasuredTemperature(in)
			i32AssertWithheld(t, out)
			if out.Measurement.AttemptLoss != tc.loss {
				t.Fatalf("loss %q want %q", out.Measurement.AttemptLoss, tc.loss)
			}
			if tc.loss != "" {
				if out.Celsius != nil || out.Measurement.Selected != nil {
					t.Fatal("invalid value selected")
				}
				return
			}
			if out.Celsius == nil || *out.Celsius != tc.want || math.Signbit(*out.Celsius) != math.Signbit(tc.want) {
				t.Fatalf("value %v want %v", out.Celsius, tc.want)
			}
		})
	}
	for _, value := range []float32{-101, 201} {
		in := i32Input()
		in.Measurement.Codec.Minimum = -1000
		in.Measurement.Codec.Maximum = 1000
		bits := math.Float32bits(value)
		in.Measurement.Attempt.Value = []byte{byte(bits), byte(bits >> 8), byte(bits >> 16), byte(bits >> 24)}
		if mapDHWMeasuredTemperature(in).Celsius != nil {
			t.Fatal("pack interval ignored")
		}
	}
}

// Each leaf variant catches removal of that leaf from full identity equality.
// The expected result is independent: a foreign key never selects a value.
func i32Leaves(v reflect.Value, prefix string, visit func(string, func(reflect.Value))) {
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		name := prefix + field.Name
		if v.Field(i).Kind() == reflect.Struct {
			index := i
			i32Leaves(v.Field(i), name+".", func(n string, edit func(reflect.Value)) {
				visit(n, func(root reflect.Value) { edit(root.Field(index)) })
			})
		} else {
			index := i
			visit(name, func(root reflect.Value) {
				f := root.Field(index)
				if f.Kind() == reflect.String {
					f.SetString(f.String() + "2")
				} else {
					f.SetUint(f.Uint() + 1)
				}
			})
		}
	}
}

func TestI32EveryIdentityLeaf(t *testing.T) {
	base := i32Input()
	i32Leaves(reflect.ValueOf(base.Identity), "", func(name string, mutate func(reflect.Value)) {
		for _, slot := range []string{"measurement_attempt", "measurement_codec", "measurement_previous", "presence_attempt", "presence_codec", "presence_previous"} {
			t.Run(slot+"/"+name, func(t *testing.T) {
				in := i32Input()
				component := &in.Measurement
				if slot == "presence_attempt" || slot == "presence_codec" || slot == "presence_previous" {
					component = &in.Presence
				}
				switch slot {
				case "measurement_attempt", "presence_attempt":
					mutate(reflect.ValueOf(&component.Attempt.Identity).Elem())
				case "measurement_codec", "presence_codec":
					mutate(reflect.ValueOf(&component.Codec.Identity).Elem())
				default:
					component.Previous = dhwMeasuredClone(component.Attempt)
					component.Attempt = nil
					mutate(reflect.ValueOf(&component.Previous.Identity).Elem())
				}
				out := mapDHWMeasuredTemperature(in)
				i32AssertWithheld(t, out)
				c := out.Measurement
				if component == &in.Presence {
					c = out.Presence
				}
				if c.Selected != nil || out.Celsius != nil {
					t.Fatalf("foreign %s survived %s", name, slot)
				}
			})
		}
	})
}

func TestI32RawEvidenceAndStatusAdmission(t *testing.T) {
	for _, slot := range []string{"measurement", "presence"} {
		for _, mode := range []string{"attempt", "history", "retain"} {
			for _, tc := range []struct {
				name string
				loss string
				edit func(*dhwMeasuredObservation)
			}{
				{"request_nil", "missing raw request or reply", func(o *dhwMeasuredObservation) { o.RawRequest = nil }},
				{"request_empty", "missing raw request or reply", func(o *dhwMeasuredObservation) { o.RawRequest = []byte{} }},
				{"reply_nil", "missing raw request or reply", func(o *dhwMeasuredObservation) { o.RawReply = nil }},
				{"reply_empty", "missing raw request or reply", func(o *dhwMeasuredObservation) { o.RawReply = []byte{} }},
				{"status_invalid", "unknown evidence status", func(o *dhwMeasuredObservation) { o.EvidenceStatus = "invalid" }},
			} {
				t.Run(slot+"/"+mode+"/"+tc.name, func(t *testing.T) {
					in := i32Input()
					component := &in.Measurement
					if slot == "presence" {
						component = &in.Presence
					}
					o := component.Attempt
					if mode != "attempt" {
						component.Previous = dhwMeasuredClone(o)
						if mode == "history" {
							o = component.Previous
							component.Attempt = nil
						} else {
							component.Attempt.Receipt.Nanoseconds = "11"
						}
					}
					tc.edit(o)
					out := mapDHWMeasuredTemperature(in)
					i32AssertWithheld(t, out)
					c := out.Measurement
					if slot == "presence" {
						c = out.Presence
					}
					if out.Celsius != nil {
						t.Fatal("invalid observation admitted")
					}
					if mode == "retain" {
						if !reflect.DeepEqual(c.Selected, component.Previous) || c.LastGoodValue == nil {
							t.Fatal("invalid attempt replaced valid history")
						}
					} else if c.Selected != nil || c.LastGoodValue != nil {
						t.Fatal("invalid observation selected as last good")
					}
					if mode != "history" && c.AttemptLoss != tc.loss {
						t.Fatalf("loss %q want %q", c.AttemptLoss, tc.loss)
					}
					if !reflect.DeepEqual(c.Attempt, dhwMeasuredClone(component.Attempt)) || !reflect.DeepEqual(c.History, dhwMeasuredClone(component.Previous)) {
						t.Fatal("rejected evidence lost from diagnostics")
					}
				})
			}
		}
	}
}

func TestI32EveryEvidenceLeaf(t *testing.T) {
	base := i32Input()
	for i := 0; i < reflect.TypeOf(base.Measurement.Attempt.Evidence).NumField(); i++ {
		field := i
		for _, slot := range []string{"measurement_attempt", "measurement_codec", "measurement_previous", "presence_attempt", "presence_codec", "presence_previous"} {
			t.Run(slot+"/"+reflect.TypeOf(base.Measurement.Attempt.Evidence).Field(i).Name, func(t *testing.T) {
				in := i32Input()
				component := &in.Measurement
				if slot == "presence_attempt" || slot == "presence_codec" || slot == "presence_previous" {
					component = &in.Presence
				}
				e := &component.Attempt.Evidence
				if slot == "measurement_codec" || slot == "presence_codec" {
					e = &component.Codec.Evidence
				}
				if slot == "measurement_previous" || slot == "presence_previous" {
					component.Previous = dhwMeasuredClone(component.Attempt)
					component.Attempt = nil
					e = &component.Previous.Evidence
				}
				f := reflect.ValueOf(e).Elem().Field(field)
				switch f.Type().Name() {
				case "Digest":
					digest := sha256.Sum256([]byte("different synthetic evidence"))
					f.SetString(fmt.Sprintf("sha256:%x", digest))
				case "EvidenceAccess":
					f.SetString("authorized")
				case "RedactionState":
					f.SetString("redacted")
				default:
					f.SetString(f.String() + "2")
				}
				if err := e.Validate(); err != nil {
					t.Fatalf("variant must remain valid evidence: %v", err)
				}
				out := mapDHWMeasuredTemperature(in)
				c := out.Measurement
				if component == &in.Presence {
					c = out.Presence
				}
				if c.Selected != nil || out.Celsius != nil {
					t.Fatal("evidence metadata collapsed")
				}
				if !reflect.DeepEqual(c.Attempt, component.Attempt) || !reflect.DeepEqual(c.History, component.Previous) {
					t.Fatal("evidence was lost")
				}
			})
		}
	}
}

func TestI32UnknownAndPresence(t *testing.T) {
	cases := []struct {
		name string
		edit func(*dhwMeasuredRequest)
	}{
		{"qualification", func(i *dhwMeasuredRequest) { i.Measurement.Codec.Qualified = false }},
		{"codec", func(i *dhwMeasuredRequest) { i.Measurement.Codec.Codec = "unknown" }},
		{"unit", func(i *dhwMeasuredRequest) { i.Measurement.Codec.Unit = "unknown" }},
		{"bounds_unknown", func(i *dhwMeasuredRequest) { i.Measurement.Codec.BoundsKnown = false }},
		{"bounds_reversed", func(i *dhwMeasuredRequest) { i.Measurement.Codec.Minimum = 151 }},
		{"bounds_nan", func(i *dhwMeasuredRequest) { i.Measurement.Codec.Minimum = math.NaN() }},
		{"bounds_inf", func(i *dhwMeasuredRequest) { i.Measurement.Codec.Maximum = math.Inf(1) }},
		{"uncorrelated", func(i *dhwMeasuredRequest) { i.Measurement.Attempt.Correlated = false }},
		{"reply_error", func(i *dhwMeasuredRequest) { i.Measurement.Attempt.Outcome = "error" }},
		{"missing_attempt", func(i *dhwMeasuredRequest) { i.Measurement.Attempt = nil }},
		{"presence_missing", func(i *dhwMeasuredRequest) { i.Presence.Attempt = nil }},
		{"presence_zero", func(i *dhwMeasuredRequest) { i.Presence.Attempt.Value = []byte{0, 0} }},
		{"presence_short", func(i *dhwMeasuredRequest) { i.Presence.Attempt.Value = []byte{1} }},
		{"presence_long", func(i *dhwMeasuredRequest) { i.Presence.Attempt.Value = []byte{1, 0, 0} }},
		{"presence_codec", func(i *dhwMeasuredRequest) { i.Presence.Codec.Codec = "unknown" }},
		{"presence_unit", func(i *dhwMeasuredRequest) { i.Presence.Codec.Unit = "unknown" }},
		{"presence_qualification", func(i *dhwMeasuredRequest) { i.Presence.Codec.Qualified = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := i32Input()
			tc.edit(&in)
			out := mapDHWMeasuredTemperature(in)
			i32AssertWithheld(t, out)
			if out.Celsius != nil || out.Loss == "" {
				t.Fatal("unknown input admitted")
			}
		})
	}
	// No magnitude inference for presence; any exact UIN nonzero is presence only.
	in := i32Input()
	in.Presence.Attempt.Value = []byte{0, 1}
	out := mapDHWMeasuredTemperature(in)
	if out.Celsius == nil || *out.Celsius != 45 {
		t.Fatal("UIN presence interpretation changed")
	}
}

func TestI32TypedTimeAndFreshness(t *testing.T) {
	for _, slot := range []string{"measurement", "presence"} {
		for _, tc := range []struct {
			name string
			edit func(*dhwMeasuredObservation, *semreg.EvaluationContext)
		}{
			{"stale_boundary", func(o *dhwMeasuredObservation, e *semreg.EvaluationContext) { e.EvaluateMonotonic.Nanoseconds = "110" }},
			{"future", func(o *dhwMeasuredObservation, e *semreg.EvaluationContext) { o.Receipt.Nanoseconds = "21" }},
			{"foreign_clock", func(o *dhwMeasuredObservation, e *semreg.EvaluationContext) { o.Receipt.ClockEpochID = "clock:other" }},
			{"missing_clock", func(o *dhwMeasuredObservation, e *semreg.EvaluationContext) { o.Receipt.ClockEpochID = "" }},
			{"invalid_clock", func(o *dhwMeasuredObservation, e *semreg.EvaluationContext) { o.Receipt.ClockEpochID = "!" }},
			{"invalid_evaluation_clock", func(o *dhwMeasuredObservation, e *semreg.EvaluationContext) { e.EvaluateMonotonic.ClockEpochID = "!" }},
			{"invalid_receipt_wall", func(o *dhwMeasuredObservation, e *semreg.EvaluationContext) { o.ReceivedAt.UnixNanoseconds = "bad" }},
			{"invalid_evaluation_wall", func(o *dhwMeasuredObservation, e *semreg.EvaluationContext) { e.EvaluatedAt.UnixNanoseconds = "bad" }},
		} {
			t.Run(slot+"/"+tc.name, func(t *testing.T) {
				in := i32Input()
				o := in.Measurement.Attempt
				if slot == "presence" {
					o = in.Presence.Attempt
				}
				tc.edit(o, &in.Evaluation)
				out := mapDHWMeasuredTemperature(in)
				if out.Celsius != nil {
					t.Fatal("invalid time admitted")
				}
			})
		}
		for _, value := range []semreg.Uint64{"", "-1", "+1", "01", "bad", "18446744073709551616"} {
			for _, field := range []string{"receipt", "evaluation", "freshness"} {
				t.Run(slot+"/"+field+"/"+string(value), func(t *testing.T) {
					in := i32Input()
					o := in.Measurement.Attempt
					if slot == "presence" {
						o = in.Presence.Attempt
					}
					switch field {
					case "receipt":
						o.Receipt.Nanoseconds = value
					case "evaluation":
						in.Evaluation.EvaluateMonotonic.Nanoseconds = value
					case "freshness":
						o.FreshForNS = value
					}
					if mapDHWMeasuredTemperature(in).Celsius != nil {
						t.Fatal("malformed typed number admitted")
					}
				})
			}
		}
		in := i32Input()
		if slot == "measurement" {
			in.Measurement.Attempt.FreshForNS = "0"
		} else {
			in.Presence.Attempt.FreshForNS = "0"
		}
		if mapDHWMeasuredTemperature(in).Celsius != nil {
			t.Fatal("zero policy admitted")
		}
	}
}

func TestI32RetentionRecoveryAndDetachment(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*dhwMeasuredInput)
	}{
		{"missing", func(i *dhwMeasuredInput) { i.Attempt = nil }},
		{"invalid", func(i *dhwMeasuredInput) { i.Attempt.Value = []byte{1} }},
		{"duplicate", func(i *dhwMeasuredInput) {}},
		{"older", func(i *dhwMeasuredInput) { i.Attempt.Receipt.Nanoseconds = "9" }},
		{"incomparable", func(i *dhwMeasuredInput) { i.Attempt.Receipt.ClockEpochID = "clock:other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := i32Input()
			in.Measurement.Previous = dhwMeasuredClone(in.Measurement.Attempt)
			tc.edit(&in.Measurement)
			out := mapDHWMeasuredTemperature(in)
			if out.Celsius != nil || out.Measurement.LastGoodValue == nil || *out.Measurement.LastGoodValue != 45 || out.Measurement.Selected.Receipt.Nanoseconds != "10" {
				t.Fatal("last good receipt/value lost or renewed")
			}
			in.Evaluation.EvaluateMonotonic.Nanoseconds = "110"
			stale := mapDHWMeasuredTemperature(in)
			if stale.Measurement.Freshness != semreg.FreshnessStale || stale.Measurement.Selected.Receipt.Nanoseconds != "10" {
				t.Fatal("history rejuvenated")
			}
			in.Measurement.Attempt = dhwMeasuredClone(in.Measurement.Previous)
			in.Measurement.Attempt.Value = []byte{0, 0, 0x38, 0x42}
			in.Measurement.Attempt.Receipt.Nanoseconds = "105"
			in.Presence.Attempt.Receipt.Nanoseconds = "105"
			recovered := mapDHWMeasuredTemperature(in)
			if recovered.Celsius == nil || *recovered.Celsius != 46 || recovered.Measurement.Selected.Receipt.Nanoseconds != "105" || recovered.Measurement.History.Receipt.Nanoseconds != "10" {
				t.Fatal("recovery failed")
			}
			recovered.Measurement.Selected.Value[0] = 99
			recovered.Measurement.Attempt.RawRequest[0] = 99
			recovered.Measurement.History.RawReply[0] = 99
			*recovered.Celsius = 99
			if in.Measurement.Attempt.Value[0] != 0 || in.Measurement.Attempt.RawRequest[0] != 2 || in.Measurement.Previous.RawReply[0] != 2 || *recovered.Measurement.LastGoodValue != 46 {
				t.Fatal("output aliases input or sibling output")
			}
			in.Measurement.Previous.Value[0] = 99
			if recovered.Measurement.History.Value[0] != 0 {
				t.Fatal("history aliases input")
			}
		})
	}
}

func TestI32ForeignInstancesPermutationAndNoFact(t *testing.T) {
	a, b := i32Input(), i32Input()
	b.Identity.Service = "service:b"
	b.Measurement.Codec.Identity.Service = "service:b"
	b.Measurement.Attempt.Identity.Service = "service:b"
	b.Presence.Codec.Identity.Service = "service:b"
	b.Presence.Attempt.Identity.Service = "service:b"
	b.Measurement.Attempt.Value = []byte{0, 0, 0x48, 0x42} // 50; same asset and proposed definition.
	forward := []dhwMeasuredCandidate{mapDHWMeasuredTemperature(a), mapDHWMeasuredTemperature(b)}
	reverse := []dhwMeasuredCandidate{mapDHWMeasuredTemperature(b), mapDHWMeasuredTemperature(a)}
	if !reflect.DeepEqual(forward[0], reverse[1]) || !reflect.DeepEqual(forward[1], reverse[0]) || *forward[0].Celsius != 45 || *forward[1].Celsius != 50 {
		t.Fatal("request order or service collision selected a winner")
	}
	for _, out := range forward {
		i32AssertWithheld(t, out)
	}
	// Unknown binding stays withheld even if all caller records use the same empty key.
	a.Identity.Binding = ""
	a.Measurement.Attempt.Identity.Binding = ""
	a.Measurement.Codec.Identity.Binding = ""
	if mapDHWMeasuredTemperature(a).Celsius != nil {
		t.Fatal("empty binding accepted")
	}
}

func TestI32ScopeAndMalformedKeys(t *testing.T) {
	base := i32Input()
	i32Leaves(reflect.ValueOf(base.Identity.Selector), "", func(name string, edit func(reflect.Value)) {
		t.Run("selector/"+name, func(t *testing.T) {
			in := i32Input()
			edit(reflect.ValueOf(&in.Identity.Selector).Elem())
			in.Measurement.Codec.Identity = in.Identity
			in.Measurement.Attempt.Identity = in.Identity
			p := in.Identity
			p.Selector.Register = 1
			in.Presence.Codec.Identity = p
			in.Presence.Attempt.Identity = p
			if mapDHWMeasuredTemperature(in).Celsius != nil {
				t.Fatal("out-of-scope selector accepted")
			}
		})
	})
	for _, name := range []string{"Asset", "Service", "Source", "Epoch", "Binding", "Native", "Family", "Profile", "ProfileRevision", "DecoderRevision", "Generation", "BindingRevision", "AssociationRevision", "SemanticRevision"} {
		for _, bad := range []string{""} {
			t.Run("missing/"+name, func(t *testing.T) {
				in := i32Input()
				reflect.ValueOf(&in.Identity).Elem().FieldByName(name).SetString(bad)
				in.Measurement.Attempt.Identity = in.Identity
				in.Measurement.Codec.Identity = in.Identity
				if mapDHWMeasuredTemperature(in).Celsius != nil {
					t.Fatal("incomplete key admitted")
				}
			})
		}
	}
	for _, name := range []string{"Generation", "BindingRevision", "AssociationRevision", "SemanticRevision"} {
		for _, bad := range []string{"0", "01", "+1", "-1", "bad", "18446744073709551616"} {
			t.Run(name+"/"+bad, func(t *testing.T) {
				in := i32Input()
				reflect.ValueOf(&in.Identity).Elem().FieldByName(name).SetString(bad)
				in.Measurement.Attempt.Identity = in.Identity
				in.Measurement.Codec.Identity = in.Identity
				if mapDHWMeasuredTemperature(in).Celsius != nil {
					t.Fatal("malformed revision admitted")
				}
			})
		}
	}
	// Both records agreeing on malformed evidence still cannot qualify it.
	in := i32Input()
	in.Measurement.Attempt.Evidence.Digest = "bad"
	in.Measurement.Codec.Evidence.Digest = "bad"
	if mapDHWMeasuredTemperature(in).Celsius != nil {
		t.Fatal("matching invalid evidence accepted")
	}
	// Exact pack endpoints are representable native candidates with supplied bounds.
	for _, tc := range []struct {
		raw  []byte
		want float64
	}{{[]byte{0, 0, 0xc8, 0xc2}, -100}, {[]byte{0, 0, 0x48, 0x43}, 200}} {
		in := i32Input()
		in.Measurement.Codec.Minimum = -100
		in.Measurement.Codec.Maximum = 200
		in.Measurement.Attempt.Value = tc.raw
		out := mapDHWMeasuredTemperature(in)
		if out.Celsius == nil || *out.Celsius != tc.want {
			t.Fatal("pack endpoint rejected")
		}
	}
}
