package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func i33Fixture() (circuitTemperatureInput, semreg.EvaluationContext) {
	id := circuitTemperatureIdentity{Asset: "asset:synthetic", Service: "service:circuit", Source: "source:synthetic", Epoch: "epoch:synthetic", Generation: "1", Binding: "binding:circuit", BindingRevision: "1", AssociationRevision: "1", ProfileRevision: "1", DecoderRevision: "1", SemanticRevision: "1", Family: "controller-family", Profile: "synthetic-f32", Firmware: "Unknown", Selector: circuitTemperatureSelector{PB: 0xb5, SB: 0x24, Opcode: 2, Operation: 0, Group: 2, Instance: 0, Register: 8, Initiator: 1, Responder: 2, Direction: "target_to_initiator", Association: "synthetic-pair"}}
	codec := circuitTemperatureCodec{Qualified: true, ActiveCircuit: true, ByteOrder: "little"}
	digest := sha256.Sum256([]byte("public synthetic I33 observation"))
	o := circuitTemperatureObservation{Identity: id, Codec: codec, RawRequest: []byte{2, 0, 2, 0, 8, 0}, RawReply: []byte{4, 0, 0, 0x20, 0x42}, ValueBytes: []byte{0, 0, 0x20, 0x42}, Evidence: []semreg.EvidenceRef{{Owner: "fixture.native", Kind: "fixture.synthetic", Contract: "fixture/v1", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", digest)), Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}}, EvidenceStatus: "Hypothesis", Correlated: true, ReceivedAt: semreg.TimePoint{UnixNanoseconds: "0", ClockID: "clock.synthetic", UncertaintyNS: "0"}, Receipt: semreg.MonotonicPoint{ClockEpochID: "clock-epoch:synthetic", Nanoseconds: "10"}, FreshForNS: "10"}
	return circuitTemperatureInput{Identity: id, Codec: codec, Attempts: []circuitTemperatureObservation{o}}, semreg.EvaluationContext{EvaluatedAt: o.ReceivedAt, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: o.Receipt.ClockEpochID, Nanoseconds: "15"}}
}

func i33Map(t *testing.T, in circuitTemperatureInput, e semreg.EvaluationContext) circuitTemperatureCandidate {
	t.Helper()
	out := mapCircuitTemperature([]circuitTemperatureInput{in}, e)
	if len(out.Facts) != 0 || len(out.Candidates) != 1 || out.Candidates[0].Withheld == "" {
		t.Fatal("unresolved binding must always withhold facts", out)
	}
	return out.Candidates[0]
}

func TestI33Values(t *testing.T) {
	cases := []struct {
		name  string
		bytes []byte
		want  *float64
	}{
		{"zero", []byte{0, 0, 0, 0}, i33Number(0)}, {"forty", []byte{0, 0, 0x20, 0x42}, i33Number(40)},
		{"minimum", []byte{0, 0, 0xc8, 0xc2}, i33Number(-100)}, {"maximum", []byte{0, 0, 0x48, 0x43}, i33Number(200)},
		{"missing", nil, nil}, {"short", []byte{0, 0, 0x20}, nil}, {"long", []byte{0, 0, 0x20, 0x42, 0}, nil},
		{"nan", []byte{0, 0, 0xc0, 0x7f}, nil}, {"positive-infinity", []byte{0, 0, 0x80, 0x7f}, nil}, {"negative-infinity", []byte{0, 0, 0x80, 0xff}, nil},
		{"above-semantic", []byte{0, 0, 0x49, 0x43}, nil}, {"below-semantic", []byte{0, 0, 0xca, 0xc2}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in, e := i33Fixture()
			in.Attempts[0].ValueBytes = tc.bytes
			c := i33Map(t, in, e)
			if tc.want == nil {
				if c.LastGood != nil || c.NativeValue != nil {
					t.Fatal("invalid value retained", c)
				}
			} else if c.LastGood == nil || c.NativeValue == nil || *c.NativeValue != *tc.want || c.Freshness != semreg.FreshnessFresh {
				t.Fatal("wrong exact native value", c)
			}
		})
	}
}
func i33Number(n float64) *float64 { return &n }

func TestI33CodecAndBounds(t *testing.T) {
	cases := []struct {
		name   string
		change func(*circuitTemperatureInput)
		valid  bool
	}{
		{"big-endian", func(i *circuitTemperatureInput) {
			i.Codec.ByteOrder = "big"
			i.Attempts[0].ValueBytes = []byte{0x42, 0x20, 0, 0}
		}, true},
		{"unknown-codec", func(i *circuitTemperatureInput) { i.Codec.ByteOrder = "" }, false},
		{"unqualified", func(i *circuitTemperatureInput) { i.Codec.Qualified = false }, false},
		{"inactive", func(i *circuitTemperatureInput) { i.Codec.ActiveCircuit = false }, false},
		{"profile-boundary", func(i *circuitTemperatureInput) {
			i.Codec.BoundsKnown = true
			i.Codec.Minimum = 40
			i.Codec.Maximum = 40
		}, true},
		{"profile-outside", func(i *circuitTemperatureInput) { i.Codec.BoundsKnown = true; i.Codec.Maximum = 39 }, false},
		{"profile-reversed", func(i *circuitTemperatureInput) {
			i.Codec.BoundsKnown = true
			i.Codec.Minimum = 50
			i.Codec.Maximum = 30
		}, false},
		{"profile-nan", func(i *circuitTemperatureInput) { i.Codec.BoundsKnown = true; i.Codec.Minimum = math.NaN() }, false},
		{"profile-infinity", func(i *circuitTemperatureInput) { i.Codec.BoundsKnown = true; i.Codec.Maximum = math.Inf(1) }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in, e := i33Fixture()
			tc.change(&in)
			in.Attempts[0].Codec = in.Codec
			c := i33Map(t, in, e)
			if (c.LastGood != nil) != tc.valid {
				t.Fatal(c)
			}
		})
	}
	in, e := i33Fixture()
	in.Attempts[0].Codec.ByteOrder = "big"
	if i33Map(t, in, e).LastGood != nil {
		t.Fatal("codec metadata mismatch accepted")
	}
}

// Each leaf case kills removal of that leaf from the full identity equality,
// independently for incoming observations and restored previous records.
func TestI33IdentityAxes(t *testing.T) {
	id := reflect.TypeOf(circuitTemperatureIdentity{})
	for _, path := range i33Leaves(id, nil) {
		name := i33PathName(id, path)
		t.Run(name, func(t *testing.T) {
			for _, previous := range []bool{false, true} {
				in, e := i33Fixture()
				o := in.Attempts[0]
				i33Vary(reflect.ValueOf(&o.Identity).Elem(), path)
				if previous {
					in.Previous = []circuitTemperatureObservation{o}
					in.Attempts = nil
				} else {
					in.Attempts = []circuitTemperatureObservation{o}
				}
				c := i33Map(t, in, e)
				if c.LastGood != nil || c.Loss != "identity mismatch" {
					t.Fatalf("identity %s collapsed (previous=%v): %+v", name, previous, c)
				}
			}
		})
	}
}
func i33Leaves(typ reflect.Type, prefix []int) [][]int {
	var out [][]int
	for i := 0; i < typ.NumField(); i++ {
		p := append(append([]int(nil), prefix...), i)
		if typ.Field(i).Type.Kind() == reflect.Struct {
			out = append(out, i33Leaves(typ.Field(i).Type, p)...)
		} else {
			out = append(out, p)
		}
	}
	return out
}
func i33PathName(typ reflect.Type, path []int) string {
	n := ""
	for _, i := range path {
		if n != "" {
			n += "."
		}
		n += typ.Field(i).Name
		typ = typ.Field(i).Type
	}
	return n
}
func i33Vary(v reflect.Value, path []int) {
	for _, i := range path {
		v = v.Field(i)
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(v.String() + "-other")
	default:
		v.SetUint(v.Uint() + 1)
	}
}

func TestI33EvidenceAxes(t *testing.T) {
	typ := reflect.TypeOf(semreg.EvidenceRef{})
	for _, path := range i33Leaves(typ, nil) {
		name := i33PathName(typ, path)
		t.Run(name, func(t *testing.T) {
			in, e := i33Fixture()
			other := circuitTemperatureClone(in.Attempts[0])
			v := reflect.ValueOf(&other.Evidence[0]).Elem().Field(path[0])
			switch name {
			case "Digest":
				d := sha256.Sum256([]byte("different public synthetic evidence"))
				v.SetString(fmt.Sprintf("sha256:%x", d))
			case "Contract":
				v.SetString("fixture/v2")
			case "Access":
				v.SetString(string(semreg.EvidenceAccessRestricted))
				other.Evidence[0].Redaction = semreg.RedactionMetadataOnly
				in.Attempts[0].Evidence[0].Redaction = semreg.RedactionMetadataOnly
			case "Redaction":
				v.SetString(string(semreg.RedactionMetadataOnly))
			default:
				v.SetString(v.String() + "-other")
			}
			if other.Evidence[0].Validate() != nil {
				t.Fatal("invalid synthetic evidence variant")
			}
			in.Attempts = append(in.Attempts, other)
			c := i33Map(t, in, e)
			if c.LastGood != nil || c.Loss != "conflicting receipt or clock continuity" || len(c.Attempts) != 2 {
				t.Fatal("evidence identity collapsed", c)
			}
		})
	}
}

func TestI33Freshness(t *testing.T) {
	cases := []struct {
		name     string
		change   func(*circuitTemperatureInput, *semreg.EvaluationContext)
		want     semreg.Freshness
		retained bool
	}{
		{"boundary", func(i *circuitTemperatureInput, e *semreg.EvaluationContext) { e.EvaluateMonotonic.Nanoseconds = "20" }, semreg.FreshnessFresh, true},
		{"stale", func(i *circuitTemperatureInput, e *semreg.EvaluationContext) { e.EvaluateMonotonic.Nanoseconds = "21" }, semreg.FreshnessStale, true},
		{"future", func(i *circuitTemperatureInput, e *semreg.EvaluationContext) { e.EvaluateMonotonic.Nanoseconds = "9" }, semreg.FreshnessUnknown, true},
		{"epoch", func(i *circuitTemperatureInput, e *semreg.EvaluationContext) {
			e.EvaluateMonotonic.ClockEpochID = "other"
		}, semreg.FreshnessUnknown, true},
		{"missing-epoch", func(i *circuitTemperatureInput, e *semreg.EvaluationContext) { i.Attempts[0].Receipt.ClockEpochID = "" }, semreg.FreshnessUnknown, false},
	}
	for _, bad := range []semreg.Uint64{"", "0", "+10", "01", "-1", "18446744073709551616"} {
		b := bad
		cases = append(cases, struct {
			name     string
			change   func(*circuitTemperatureInput, *semreg.EvaluationContext)
			want     semreg.Freshness
			retained bool
		}{"budget-" + string(b), func(i *circuitTemperatureInput, e *semreg.EvaluationContext) { i.Attempts[0].FreshForNS = b }, semreg.FreshnessUnknown, true})
	}
	for _, bad := range []semreg.Uint64{"", "+10", "01", "-1", "18446744073709551616"} {
		b := bad
		cases = append(cases, struct {
			name     string
			change   func(*circuitTemperatureInput, *semreg.EvaluationContext)
			want     semreg.Freshness
			retained bool
		}{"receipt-" + string(b), func(i *circuitTemperatureInput, e *semreg.EvaluationContext) { i.Attempts[0].Receipt.Nanoseconds = b }, semreg.FreshnessUnknown, false}, struct {
			name     string
			change   func(*circuitTemperatureInput, *semreg.EvaluationContext)
			want     semreg.Freshness
			retained bool
		}{"evaluation-" + string(b), func(i *circuitTemperatureInput, e *semreg.EvaluationContext) { e.EvaluateMonotonic.Nanoseconds = b }, semreg.FreshnessUnknown, true})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in, e := i33Fixture()
			tc.change(&in, &e)
			c := i33Map(t, in, e)
			if c.Freshness != tc.want || (c.LastGood != nil) != tc.retained {
				t.Fatal(c)
			}
		})
	}
}

func TestI33RetentionRecoveryAndDetachment(t *testing.T) {
	in, e := i33Fixture()
	prior := circuitTemperatureClone(in.Attempts[0])
	in.Previous = []circuitTemperatureObservation{prior}
	in.Attempts[0].Receipt.Nanoseconds = "12"
	in.Attempts[0].ValueBytes = nil
	c := i33Map(t, in, e)
	if c.LastGood == nil || c.LastGood.Receipt.Nanoseconds != "10" || *c.NativeValue != 40 {
		t.Fatal("failed attempt changed prior", c)
	}
	in.Attempts[0].ValueBytes = []byte{0, 0, 0x34, 0x42}
	c = i33Map(t, in, e)
	if *c.NativeValue != 45 || c.LastGood.Receipt.Nanoseconds != "12" || c.History[0].Receipt.Nanoseconds != "10" {
		t.Fatal("recovery lost receipts", c)
	}
	in.Attempts[0].ValueBytes[0] = 255
	in.Previous[0].RawRequest[0] = 255
	c.LastGood.Evidence[0].Owner = "changed"
	if c.Attempts[0].ValueBytes[0] != 0 || c.History[0].RawRequest[0] != 2 || in.Attempts[0].Evidence[0].Owner != "fixture.native" {
		t.Fatal("aliased evidence")
	}
	in, e = i33Fixture()
	in.Previous = []circuitTemperatureObservation{in.Attempts[0]}
	in.Attempts[0] = circuitTemperatureClone(in.Attempts[0])
	in.Attempts[0].Receipt.Nanoseconds = "9"
	in.Attempts[0].ValueBytes = []byte{0, 0, 0x34, 0x42}
	c = i33Map(t, in, e)
	if *c.NativeValue != 40 {
		t.Fatal("older observation displaced prior")
	}
}

func TestI33RolesAndInstanceIsolation(t *testing.T) {
	in, e := i33Fixture()
	requested := in
	requested.Identity.Selector.Register = 7
	requested.Attempts = circuitTemperatureCloneAll(in.Attempts)
	requested.Attempts[0].Identity = requested.Identity
	requested.Attempts[0].ValueBytes = []byte{0, 0, 0x7a, 0x43} // 250: no invented requested-flow semantic bound.
	raw := in
	raw.Identity.Selector.Register = 0x20
	raw.Attempts = circuitTemperatureCloneAll(in.Attempts)
	raw.Attempts[0].Identity = raw.Identity
	foreign := in
	foreign.Identity.Selector.Instance = 1
	foreign.Attempts = circuitTemperatureCloneAll(in.Attempts)
	foreign.Attempts[0].Identity = foreign.Identity
	foreign.Attempts[0].ValueBytes = []byte{0, 0, 0x34, 0x42}
	out := mapCircuitTemperature([]circuitTemperatureInput{in, requested, raw, foreign}, e)
	if len(out.Facts) != 0 || len(out.Candidates) != 4 || *out.Candidates[0].NativeValue != 40 || *out.Candidates[1].NativeValue != 250 || out.Candidates[1].Role != "requested_flow" || out.Candidates[2].NativeValue != nil || *out.Candidates[3].NativeValue != 45 {
		t.Fatal(out)
	}
	raw.Attempts[0].ValueBytes = []byte{0}
	c := i33Map(t, raw, e)
	if c.NativeValue != nil || c.Loss != "RR0020 raw evidence only" || len(c.Attempts[0].ValueBytes) != 1 {
		t.Fatal(c)
	}
	for _, instance := range []byte{9, 10} {
		in.Identity.Selector.Instance = instance
		in.Attempts[0].Identity = in.Identity
		if i33Map(t, in, e).LastGood != nil {
			t.Fatal("virtual/out-of-scope circuit admitted")
		}
	}
}

func TestI33PermutationAndConflict(t *testing.T) {
	in, e := i33Fixture()
	old := circuitTemperatureClone(in.Attempts[0])
	newer := circuitTemperatureClone(old)
	newer.Receipt.Nanoseconds = "12"
	binary.LittleEndian.PutUint32(newer.ValueBytes, math.Float32bits(45))
	bad := circuitTemperatureClone(old)
	bad.ValueBytes = nil
	variants := [][]circuitTemperatureObservation{{old, newer, bad}, {bad, old, newer}, {newer, bad, old}, {old, bad, newer}, {bad, newer, old}, {newer, old, bad}}
	var expected circuitTemperatureCandidate
	for i, v := range variants {
		in.Attempts = v
		c := i33Map(t, in, e)
		if i == 0 {
			expected = c
		}
		if !reflect.DeepEqual(expected, c) || c.NativeValue == nil || *c.NativeValue != 45 {
			t.Fatal("arrival-order-dependent result", c)
		}
	}
	tie := circuitTemperatureClone(newer)
	tie.ValueBytes = []byte{0, 0, 0x20, 0x42}
	in.Attempts = []circuitTemperatureObservation{newer, tie}
	if i33Map(t, in, e).LastGood != nil {
		t.Fatal("ambiguous same receipt selected")
	}
	tie = circuitTemperatureClone(old)
	tie.Receipt.ClockEpochID = "other"
	in.Attempts = []circuitTemperatureObservation{old, tie, newer}
	if i33Map(t, in, e).LastGood != nil {
		t.Fatal("clock discontinuity selected")
	}
}

func TestI33MissingProvenanceAndReceipts(t *testing.T) {
	cases := []struct {
		name   string
		change func(*circuitTemperatureInput)
	}{
		{"missing", func(i *circuitTemperatureInput) { i.Attempts = nil }},
		{"uncorrelated", func(i *circuitTemperatureInput) { i.Attempts[0].Correlated = false }},
		{"failed", func(i *circuitTemperatureInput) { i.Attempts[0].Failure = "synthetic failure" }},
		{"missing-evidence", func(i *circuitTemperatureInput) { i.Attempts[0].Evidence = nil }},
		{"malformed-evidence", func(i *circuitTemperatureInput) { i.Attempts[0].Evidence[0].Digest = "invalid" }},
		{"missing-wall-receipt", func(i *circuitTemperatureInput) { i.Attempts[0].ReceivedAt = semreg.TimePoint{} }},
		{"malformed-wall-receipt", func(i *circuitTemperatureInput) { i.Attempts[0].ReceivedAt.UnixNanoseconds = "+0" }},
		{"missing-binding", func(i *circuitTemperatureInput) { i.Identity.Binding = ""; i.Attempts[0].Identity = i.Identity }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in, e := i33Fixture()
			tc.change(&in)
			c := i33Map(t, in, e)
			if c.LastGood != nil {
				t.Fatal("missing provenance retained", c)
			}
		})
	}
}

func TestI33ConflictingAttemptsRetainPrior(t *testing.T) {
	in, e := i33Fixture()
	in.Previous = circuitTemperatureCloneAll(in.Attempts)
	in.Attempts[0].Receipt.Nanoseconds = "12"
	in.Attempts[0].ValueBytes = []byte{0, 0, 0x34, 0x42}
	other := circuitTemperatureClone(in.Attempts[0])
	other.ValueBytes = []byte{0, 0, 0x48, 0x42}
	in.Attempts = append(in.Attempts, other)
	c := i33Map(t, in, e)
	if c.LastGood == nil || c.LastGood.Receipt.Nanoseconds != "10" || *c.NativeValue != 40 || c.Loss != "conflicting receipt or clock continuity" {
		t.Fatal("conflict erased last-good", c)
	}
	in.Attempts[1].Receipt.ClockEpochID = "other"
	c = i33Map(t, in, e)
	if c.LastGood == nil || c.LastGood.Receipt.Nanoseconds != "10" || *c.NativeValue != 40 {
		t.Fatal("clock conflict erased prior", c)
	}
}
