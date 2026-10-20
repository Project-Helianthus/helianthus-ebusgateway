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

func i31Fixture() zoneRoomCandidateInput {
	id := zoneRoomIdentity{Asset: "synthetic-zone", Service: "synthetic-service", Source: "synthetic-source", Epoch: "synthetic-epoch", Generation: "1", BindingRevision: "1", AssociationRevision: "1", Binding: "synthetic-binding", Family: "synthetic-family", Profile: "synthetic-profile", ProfileRevision: "r1", DecoderRevision: "r1", MappingRevision: "r1", Selector: zoneRoomSelector{PB: 0xb5, SB: 0x24, Opcode: 2, Group: 3, Instance: 4, Register: 0x0f, Direction: "target_to_initiator", Initiator: "synthetic-initiator", Target: "synthetic-target", ReplySource: "synthetic-target"}}
	codec := zoneRoomCodec{Qualified: true, Encoding: "f32_le", Unit: "unit.celsius", Width: 4}
	o := zoneRoomObservation{Identity: id, Present: true, Correlated: true, Value: i31Bytes(20), Codec: codec, ReceivedAt: semreg.TimePoint{UnixNanoseconds: "100", ClockID: "clock.synthetic", UncertaintyNS: "0"}, Receipt: semreg.MonotonicPoint{ClockEpochID: "synthetic-clock", Nanoseconds: "10"}, FreshForNS: "15", RawRequest: []byte{1}, RawReply: []byte{2}, EvidenceStatus: "Unknown", Evidence: []semreg.EvidenceRef{{Owner: "synthetic.owner", Kind: "synthetic.kind", Contract: "v1", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("synthetic evidence")))), Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}}}
	b := zoneRoomClone(o)
	b.Identity.Selector.Register = 0x13
	b.Value = []byte{2, 0}
	b.Receipt.Nanoseconds = "9"
	a := zoneRoomClone(o)
	a.Identity.Selector.Register = 0x1c
	a.Value = []byte{4}
	return zoneRoomCandidateInput{Identity: id, Codec: codec, BindingQualified: true, BindingWidth: 2, BindingValue: []byte{2, 0}, Observations: []zoneRoomObservation{o, b, a}, Evaluation: semreg.EvaluationContext{EvaluatedAt: semreg.TimePoint{UnixNanoseconds: "200", ClockID: "clock.synthetic", UncertaintyNS: "0"}, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "synthetic-clock", Nanoseconds: "20"}}}
}
func i31Bytes(v float32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, math.Float32bits(v))
	return b
}
func i31Good(t *testing.T, r zoneRoomCandidateResult, want float64) {
	t.Helper()
	if r.Temperature == nil || r.Temperature.NativeCelsius == nil || *r.Temperature.NativeCelsius != want || r.Withheld != "final FactKey binding and decimal loss policy unresolved" {
		t.Fatalf("candidate = %+v; want native %v and withholding", r, want)
	}
}

func TestI31NativeVectors(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
		good bool
		want float64
	}{
		{"zero", []byte{0, 0, 0, 0}, true, 0}, {"fraction", []byte{0, 0, 0xa2, 0x41}, true, 20.25},
		{"missing", nil, false, 0}, {"short", []byte{0, 0, 0}, false, 0}, {"long", []byte{0, 0, 0xa0, 0x41, 0}, false, 0},
		{"nan", i31Bytes(float32(math.NaN())), false, 0}, {"positive_inf", i31Bytes(float32(math.Inf(1))), false, 0}, {"negative_inf", i31Bytes(float32(math.Inf(-1))), false, 0},
		{"pack_low", i31Bytes(-101), false, 0}, {"pack_high", i31Bytes(201), false, 0}, {"minimum", i31Bytes(-100), true, -100}, {"maximum", i31Bytes(200), true, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := i31Fixture()
			in.Observations[0].Value = tc.raw
			r := mapZoneRoomTemperatureCandidate(in)
			if tc.good {
				i31Good(t, r, tc.want)
			} else if r.Temperature != nil {
				t.Fatal("invalid value selected")
			}
			if len(r.History) != 3 || r.SensorBinding == nil || r.Association == nil {
				t.Fatal("independent evidence lost")
			}
		})
	}
	for _, v := range []float32{9, 31} {
		in := i31Fixture()
		in.Codec.BoundsKnown = true
		in.Codec.Minimum = 10
		in.Codec.Maximum = 30
		in.Observations[0].Codec = in.Codec
		in.Observations[0].Value = i31Bytes(v)
		if mapZoneRoomTemperatureCandidate(in).Temperature != nil {
			t.Fatal("profile bounds ignored")
		}
	}
	in := i31Fixture()
	in.Codec.Encoding = "f32_be"
	in.Observations[0].Codec = in.Codec
	in.Observations[0].Value = []byte{0x41, 0xa0, 0, 0}
	i31Good(t, mapZoneRoomTemperatureCandidate(in), 20)
}

// Exercise foreign temperature identity fields in new and prior evidence.
// P16 additionally checks newer foreign context records and register roles.
func TestI31IdentityFence(t *testing.T) {
	base := i31Fixture().Identity
	var paths [][]int
	typ := reflect.TypeOf(base)
	for j := 0; j < typ.NumField(); j++ {
		if typ.Field(j).Name == "Selector" {
			for k := 0; k < typ.Field(j).Type.NumField(); k++ {
				paths = append(paths, []int{j, k})
			}
		} else {
			paths = append(paths, []int{j})
		}
	}
	for _, path := range paths {
		name := typ.Field(path[0]).Name
		if len(path) > 1 {
			name += "." + typ.Field(path[0]).Type.Field(path[1]).Name
		}
		t.Run(name, func(t *testing.T) {
			in := i31Fixture()
			changed := base
			f := reflect.ValueOf(&changed).Elem().FieldByIndex(path)
			if f.Kind() == reflect.String {
				if f.String() == "1" {
					f.SetString("2")
				} else {
					f.SetString("other.synthetic")
				}
			} else {
				f.SetUint(f.Uint() + 1)
			}
			in.Observations[0].Identity = changed
			r := mapZoneRoomTemperatureCandidate(in)
			if r.Temperature != nil {
				t.Fatal("foreign observation selected")
			}
			prior := zoneRoomRecord{Observation: in.Observations[0]}
			in.Observations = in.Observations[1:]
			in.Previous = []zoneRoomRecord{prior}
			if mapZoneRoomTemperatureCandidate(in).Temperature != nil {
				t.Fatal("foreign history selected")
			}
		})
	}
}

func TestI31FreshnessAndCodec(t *testing.T) {
	cases := []struct {
		name   string
		change func(*zoneRoomCandidateInput)
		want   string
	}{
		{"stale", func(i *zoneRoomCandidateInput) { i.Evaluation.EvaluateMonotonic.Nanoseconds = "25" }, "stale"},
		{"receipt_leading_zero", func(i *zoneRoomCandidateInput) { i.Observations[0].Receipt.Nanoseconds = "010" }, "unknown"},
		{"receipt_negative", func(i *zoneRoomCandidateInput) { i.Observations[0].Receipt.Nanoseconds = "-1" }, "unknown"},
		{"clock_epoch", func(i *zoneRoomCandidateInput) { i.Evaluation.EvaluateMonotonic.ClockEpochID = "other-clock" }, "unknown"},
		{"evaluation_leading_zero", func(i *zoneRoomCandidateInput) { i.Evaluation.EvaluateMonotonic.Nanoseconds = "020" }, "unknown"},
		{"future_receipt", func(i *zoneRoomCandidateInput) { i.Evaluation.EvaluateMonotonic.Nanoseconds = "5" }, "unknown"},
		{"ttl_unknown", func(i *zoneRoomCandidateInput) { i.Observations[0].FreshForNS = "" }, "unknown"},
		{"ttl_zero", func(i *zoneRoomCandidateInput) { i.Observations[0].FreshForNS = "0" }, "unknown"},
		{"ttl_leading_zero", func(i *zoneRoomCandidateInput) { i.Observations[0].FreshForNS = "015" }, "unknown"},
		{"ttl_overflow", func(i *zoneRoomCandidateInput) { i.Observations[0].FreshForNS = "18446744073709551616" }, "unknown"},
		{"ttl_plus", func(i *zoneRoomCandidateInput) { i.Observations[0].FreshForNS = "+15" }, "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := i31Fixture()
			tc.change(&in)
			r := mapZoneRoomTemperatureCandidate(in)
			if r.Freshness != tc.want || r.Withheld == "" {
				t.Fatalf("freshness=%s", r.Freshness)
			}
			if tc.name == "evaluation_leading_zero" && (r.Temperature != nil || r.SensorBinding != nil || r.Association != nil || r.LatestLoss != "malformed evaluation point; selection withheld") {
				t.Fatal("malformed evaluation selected evidence or hid loss")
			}
		})
	}
	for _, change := range []func(*zoneRoomCandidateInput){
		func(i *zoneRoomCandidateInput) { i.Codec.Qualified = false }, func(i *zoneRoomCandidateInput) { i.Codec.Width = 5 }, func(i *zoneRoomCandidateInput) { i.Codec.Encoding = "unknown" }, func(i *zoneRoomCandidateInput) { i.Codec.Unit = "unknown" },
		func(i *zoneRoomCandidateInput) { i.Observations[0].Correlated = false }, func(i *zoneRoomCandidateInput) { i.Observations[0].Present = false },
	} {
		in := i31Fixture()
		change(&in)
		if mapZoneRoomTemperatureCandidate(in).Temperature != nil {
			t.Fatal("unknown codec or invalid observation selected")
		}
	}
}

func TestI31RetentionRecoveryBindingAndDetachment(t *testing.T) {
	in := i31Fixture()
	first := mapZoneRoomTemperatureCandidate(in)
	i31Good(t, first, 20)
	in.Previous = first.History
	in.Observations = []zoneRoomObservation{zoneRoomClone(in.Observations[0])}
	in.Observations[0].Value = []byte{1}
	in.Observations[0].Receipt.Nanoseconds = "12"
	failed := mapZoneRoomTemperatureCandidate(in)
	i31Good(t, failed, 20)
	if failed.Temperature.Observation.Receipt.Nanoseconds != "10" || failed.LatestLoss != "exact width mismatch" {
		t.Fatal("failed attempt renewed receipt or hid failure")
	}
	in.Previous = failed.History
	in.Observations[0].Value = i31Bytes(21)
	in.Observations[0].Receipt.Nanoseconds = "14"
	recovered := mapZoneRoomTemperatureCandidate(in)
	i31Good(t, recovered, 21)
	if recovered.Temperature.Observation.Receipt.Nanoseconds != "14" || recovered.SensorBinding == nil || recovered.Association == nil {
		t.Fatal("partial recovery lost lineage")
	}
	in.Previous = recovered.History
	binding := zoneRoomClone(i31Fixture().Observations[1])
	binding.Value = []byte{3, 0}
	binding.Receipt.Nanoseconds = "15"
	in.Observations = []zoneRoomObservation{binding}
	changed := mapZoneRoomTemperatureCandidate(in)
	i31Good(t, changed, 21)
	if changed.LatestLoss != "sensor binding unknown or changed; requalification and new correlated temperature required" {
		t.Fatal("changed binding trusted")
	}
	newTemperature := zoneRoomClone(i31Fixture().Observations[0])
	newTemperature.Value = i31Bytes(25)
	newTemperature.Receipt.Nanoseconds = "16"
	in.Observations = append(in.Observations, newTemperature)
	i31Good(t, mapZoneRoomTemperatureCandidate(in), 21)
	in.Observations = in.Observations[:1]
	in.BindingValue = []byte{3, 0}
	if mapZoneRoomTemperatureCandidate(in).LatestLoss != "temperature predates binding evidence" {
		t.Fatal("requalification relabelled old temperature")
	}
	// Input history and selected outputs must own all nested bytes and values.
	recovered.History[0].Observation.Value[0] ^= 0xff
	recovered.Temperature.Observation.Value[0] ^= 0xff
	*recovered.Temperature.NativeCelsius = 99
	if *failed.Temperature.NativeCelsius != 20 || first.Temperature.Observation.Value[0] != 0 {
		t.Fatal("history aliases earlier result")
	}
}

func TestI31PermutationCollisionAndEvidence(t *testing.T) {
	in := i31Fixture()
	expected := mapZoneRoomTemperatureCandidate(in)
	in.Observations[0], in.Observations[2] = in.Observations[2], in.Observations[0]
	if !reflect.DeepEqual(expected, mapZoneRoomTemperatureCandidate(in)) {
		t.Fatal("permutation changes result")
	}
	// Same asset and candidate definition do not join different native instances.
	foreign := zoneRoomClone(i31Fixture().Observations[0])
	foreign.Identity.Selector.Instance++
	in.Observations = append(in.Observations, foreign)
	i31Good(t, mapZoneRoomTemperatureCandidate(in), 20)
	for _, field := range []string{"Owner", "Kind", "Contract", "Digest", "Access", "Redaction"} {
		t.Run(field, func(t *testing.T) {
			in := i31Fixture()
			other := zoneRoomClone(in.Observations[0])
			f := reflect.ValueOf(&other.Evidence[0]).Elem().FieldByName(field)
			switch field {
			case "Digest":
				f.SetString(fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("different synthetic evidence"))))
			case "Contract":
				f.SetString("v2")
			case "Access":
				f.SetString("authorized")
			case "Redaction":
				f.SetString("redacted")
			default:
				f.SetString("other.synthetic")
			}
			in.Observations = append(in.Observations, other)
			r := mapZoneRoomTemperatureCandidate(in)
			if len(r.History) != 4 || r.Temperature != nil {
				t.Fatal("different provenance collapsed or tie silently won")
			}
		})
	}
	in = i31Fixture()
	other := zoneRoomClone(in.Observations[0])
	other.Value = i31Bytes(22)
	in.Observations = append(in.Observations, other)
	if mapZoneRoomTemperatureCandidate(in).Temperature != nil {
		t.Fatal("same receipt conflicting values silently won")
	}
}

func TestI31BindingMalformedHistoryAndOldReceipts(t *testing.T) {
	for _, raw := range [][]byte{nil, {2}, {2, 0, 0}} {
		in := i31Fixture()
		previous := mapZoneRoomTemperatureCandidate(in)
		in.Previous = previous.History
		b := zoneRoomClone(in.Observations[1])
		b.Value = raw
		b.Receipt.Nanoseconds = "12"
		in.Observations = []zoneRoomObservation{b}
		r := mapZoneRoomTemperatureCandidate(in)
		i31Good(t, r, 20)
		if r.SensorBinding == nil || !reflect.DeepEqual(r.SensorBinding.Observation.Value, []byte{2, 0}) || r.SensorBinding.Observation.Receipt.Nanoseconds != "9" || r.LatestLoss != "sensor binding malformed or unqualified; retained association is history" {
			t.Fatal("malformed binding replaced retained binding")
		}
	}
	in := i31Fixture()
	in.BindingQualified = false
	r := mapZoneRoomTemperatureCandidate(in)
	if r.SensorBinding != nil || r.Withheld == "" {
		t.Fatal("unknown binding accepted")
	}
	in = i31Fixture()
	prior := mapZoneRoomTemperatureCandidate(in)
	in.Previous = prior.History
	old := zoneRoomClone(in.Observations[0])
	old.Receipt.Nanoseconds = "8"
	old.Value = i31Bytes(22)
	in.Observations = []zoneRoomObservation{old}
	r = mapZoneRoomTemperatureCandidate(in)
	i31Good(t, r, 20)
	if r.Temperature.Observation.Receipt.Nanoseconds != "10" || len(r.Attempts) != 1 {
		t.Fatal("older attempt advanced receipt or vanished")
	}
	in.Previous[0].Observation.RawRequest[0] = 9
	in.Previous[0].Observation.Evidence[0].Owner = "different.owner"

	if r.History[0].Observation.RawRequest[0] == 9 {
		t.Fatal("result aliases caller history")
	}
	// A caller's stored number and loss flags cannot bypass raw revalidation.
	in = i31Fixture()
	bad := zoneRoomClone(in.Observations[0])
	bad.Value = []byte{0}
	v := 20.0
	in.Previous = []zoneRoomRecord{{Observation: bad, NativeCelsius: &v}}
	in.Observations = in.Observations[1:]
	if mapZoneRoomTemperatureCandidate(in).Temperature != nil {
		t.Fatal("prior acceptance flags trusted without validation")
	}
}

func TestI31MalformedCodecBoundsPreserveEvidence(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		in := i31Fixture()
		in.Codec.BoundsKnown = true
		in.Codec.Minimum = v
		in.Codec.Maximum = 30
		in.Observations[0].Codec = in.Codec
		other := zoneRoomClone(in.Observations[0])
		other.Evidence[0].Owner = "other.synthetic"
		in.Observations = append(in.Observations, other)
		r := mapZoneRoomTemperatureCandidate(in)
		if r.Temperature != nil || len(r.History) != 4 || len(r.Attempts) != 4 {
			t.Fatal("malformed bounds admitted or diagnostic evidence collapsed")
		}
		in.Observations[0], in.Observations[3] = in.Observations[3], in.Observations[0]
		reordered := mapZoneRoomTemperatureCandidate(in)
		// Compare canonical records: NaN does not equal itself under DeepEqual.
		for idx := range r.History {
			if zoneRoomOrder(r.History[idx].Observation) != zoneRoomOrder(reordered.History[idx].Observation) {
				t.Fatal("malformed metadata changes permutation order")
			}
		}
	}
}

func TestP16EvaluationValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*zoneRoomCandidateInput)
	}{
		{"monotonic_leading_zero", func(i *zoneRoomCandidateInput) { i.Evaluation.EvaluateMonotonic.Nanoseconds = "020" }},
		{"monotonic_negative", func(i *zoneRoomCandidateInput) { i.Evaluation.EvaluateMonotonic.Nanoseconds = "-1" }},
		{"monotonic_overflow", func(i *zoneRoomCandidateInput) { i.Evaluation.EvaluateMonotonic.Nanoseconds = "18446744073709551616" }},
		{"monotonic_clock", func(i *zoneRoomCandidateInput) { i.Evaluation.EvaluateMonotonic.ClockEpochID = "" }},
		{"wall_nanoseconds", func(i *zoneRoomCandidateInput) { i.Evaluation.EvaluatedAt.UnixNanoseconds = "0200" }},
		{"wall_clock", func(i *zoneRoomCandidateInput) { i.Evaluation.EvaluatedAt.ClockID = "" }},
		{"wall_uncertainty", func(i *zoneRoomCandidateInput) { i.Evaluation.EvaluatedAt.UncertaintyNS = "00" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := i31Fixture()
			good := mapZoneRoomTemperatureCandidate(in)
			i31Good(t, good, 20)
			if good.SensorBinding == nil || good.Association == nil {
				t.Fatal("positive control lost context")
			}
			tc.change(&in)
			r := mapZoneRoomTemperatureCandidate(in)
			if r.Temperature != nil || r.SensorBinding != nil || r.Association != nil || r.LatestLoss != "malformed evaluation point; selection withheld" || r.Freshness != "unknown" || len(r.History) != 3 || len(r.Attempts) != 3 || r.Withheld == "" {
				t.Fatalf("malformed evaluation admitted selection or lost evidence: %+v", r)
			}
		})
	}
}

func TestP16RejectedReceiptConflict(t *testing.T) {
	for _, raw := range [][]byte{{1}, nil} {
		for _, reverse := range []bool{false, true} {
			in := i31Fixture()
			i31Good(t, mapZoneRoomTemperatureCandidate(in), 20)
			bad := zoneRoomClone(in.Observations[0])
			bad.Value = raw
			in.Observations = append(in.Observations, bad)
			if reverse {
				in.Observations[0], in.Observations[3] = in.Observations[3], in.Observations[0]
			}
			r := mapZoneRoomTemperatureCandidate(in)
			if r.Temperature != nil || r.SensorBinding == nil || r.Association == nil || r.LatestLoss != "ambiguous or incomparable receipts" || len(r.History) != 4 || len(r.Attempts) != 4 {
				t.Fatalf("rejected tie resolved or evidence lost: %+v", r)
			}
		}
	}
}

func TestP16ContextAttemptTiming(t *testing.T) {
	for _, index := range []int{1, 2} {
		for _, tc := range []struct {
			name   string
			change func(*zoneRoomObservation)
		}{
			{"receipt_nanoseconds", func(o *zoneRoomObservation) { o.Receipt.Nanoseconds = "012" }},
			{"receipt_clock", func(o *zoneRoomObservation) { o.Receipt.ClockEpochID = "" }},
			{"wall_nanoseconds", func(o *zoneRoomObservation) { o.ReceivedAt.UnixNanoseconds = "0100" }},
			{"wall_clock", func(o *zoneRoomObservation) { o.ReceivedAt.ClockID = "" }},
			{"wall_uncertainty", func(o *zoneRoomObservation) { o.ReceivedAt.UncertaintyNS = "00" }},
		} {
			t.Run(fmt.Sprintf("register_%x/%s", i31Fixture().Observations[index].Identity.Selector.Register, tc.name), func(t *testing.T) {
				in := i31Fixture()
				prior := mapZoneRoomTemperatureCandidate(in)
				i31Good(t, prior, 20)
				attempt := zoneRoomClone(in.Observations[index])
				attempt.Receipt.Nanoseconds = "12"
				tc.change(&attempt)
				in.Previous = prior.History
				in.Observations = []zoneRoomObservation{attempt}
				r := mapZoneRoomTemperatureCandidate(in)
				if !reflect.DeepEqual(r.Temperature, prior.Temperature) || !reflect.DeepEqual(r.SensorBinding, prior.SensorBinding) || !reflect.DeepEqual(r.Association, prior.Association) || r.LatestLoss != "malformed attempt receipt; ordering unknown" || len(r.Attempts) != 1 || r.Attempts[0].Loss != "malformed original receipt" || len(r.History) != 4 {
					t.Fatalf("malformed context timing hidden or history replaced: %+v", r)
				}
			})
		}
	}
}

func TestP16IdentitySelection(t *testing.T) {
	typ := reflect.TypeOf(zoneRoomIdentity{})
	for j := 0; j < typ.NumField(); j++ {
		paths := [][]int{{j}}
		if typ.Field(j).Name == "Selector" {
			paths = nil
			for k := 0; k < typ.Field(j).Type.NumField(); k++ {
				paths = append(paths, []int{j, k})
			}
		}
		for _, path := range paths {
			name := typ.Field(path[0]).Name
			if len(path) == 2 {
				name += "." + typ.Field(path[0]).Type.Field(path[1]).Name
			}
			for _, historical := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/history_%v", name, historical), func(t *testing.T) {
					in := i31Fixture()
					want := mapZoneRoomTemperatureCandidate(in)
					i31Good(t, want, 20)
					if want.SensorBinding == nil || want.Association == nil || !reflect.DeepEqual(want.SensorBinding.Observation.Value, []byte{2, 0}) || want.SensorBinding.Observation.Receipt.Nanoseconds != "9" || !reflect.DeepEqual(want.Association.Observation.Value, []byte{4}) || want.Association.Observation.Receipt.Nanoseconds != "10" {
						t.Fatal("positive control lost expected context bytes or receipts")
					}
					for _, own := range i31Fixture().Observations {
						foreign := zoneRoomClone(own)
						f := reflect.ValueOf(&foreign.Identity).Elem().FieldByIndex(path)
						if name == "Selector.Register" {
							f.SetUint(0x20) // Outside the three explicitly recognized roles.
						} else if f.Kind() == reflect.String {
							if f.String() == "1" {
								f.SetString("2")
							} else {
								f.SetString("other.synthetic")
							}
						} else {
							f.SetUint(f.Uint() + 1)
						}
						if !zoneRoomIdentityKnown(foreign.Identity) {
							t.Fatal("foreign alternative is not a valid typed identity")
						}
						foreign.Receipt.Nanoseconds = "12"
						if historical {
							in.Previous = append(in.Previous, zoneRoomRecord{Observation: foreign})
						} else {
							in.Observations = append(in.Observations, foreign)
						}
					}
					r := mapZoneRoomTemperatureCandidate(in)
					if !reflect.DeepEqual(r.Temperature, want.Temperature) || !reflect.DeepEqual(r.SensorBinding, want.SensorBinding) || !reflect.DeepEqual(r.Association, want.Association) || len(r.History) != 6 {
						t.Fatalf("foreign identity changed selected temperature, binding or association: %+v", r)
					}
				})
			}
		}
	}
}

// Direct freshness checks avoid the mapper's redundant timing and codec guards.
func TestP16FreshnessTypedValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*zoneRoomObservation, *semreg.EvaluationContext)
	}{
		{"receipt_nanoseconds", func(o *zoneRoomObservation, _ *semreg.EvaluationContext) { o.Receipt.Nanoseconds = "010" }},
		{"receipt_clock", func(o *zoneRoomObservation, _ *semreg.EvaluationContext) { o.Receipt.ClockEpochID = "" }},
		{"received_nanoseconds", func(o *zoneRoomObservation, _ *semreg.EvaluationContext) { o.ReceivedAt.UnixNanoseconds = "0100" }},
		{"received_clock", func(o *zoneRoomObservation, _ *semreg.EvaluationContext) { o.ReceivedAt.ClockID = "" }},
		{"received_uncertainty", func(o *zoneRoomObservation, _ *semreg.EvaluationContext) { o.ReceivedAt.UncertaintyNS = "00" }},
		{"evaluation_nanoseconds", func(_ *zoneRoomObservation, e *semreg.EvaluationContext) { e.EvaluateMonotonic.Nanoseconds = "020" }},
		{"evaluation_clock", func(_ *zoneRoomObservation, e *semreg.EvaluationContext) { e.EvaluateMonotonic.ClockEpochID = "" }},
		{"evaluation_wall_nanoseconds", func(_ *zoneRoomObservation, e *semreg.EvaluationContext) { e.EvaluatedAt.UnixNanoseconds = "0200" }},
		{"evaluation_wall_clock", func(_ *zoneRoomObservation, e *semreg.EvaluationContext) { e.EvaluatedAt.ClockID = "" }},
		{"evaluation_uncertainty", func(_ *zoneRoomObservation, e *semreg.EvaluationContext) { e.EvaluatedAt.UncertaintyNS = "00" }},
		{"ttl_empty", func(o *zoneRoomObservation, _ *semreg.EvaluationContext) { o.FreshForNS = "" }},
		{"ttl_zero", func(o *zoneRoomObservation, _ *semreg.EvaluationContext) { o.FreshForNS = "0" }},
		{"ttl_leading_zero", func(o *zoneRoomObservation, _ *semreg.EvaluationContext) { o.FreshForNS = "015" }},
		{"ttl_plus", func(o *zoneRoomObservation, _ *semreg.EvaluationContext) { o.FreshForNS = "+15" }},
		{"ttl_overflow", func(o *zoneRoomObservation, _ *semreg.EvaluationContext) { o.FreshForNS = "18446744073709551616" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := i31Fixture()
			o := in.Observations[0]
			if v, loss := zoneRoomDecode(o, in.Codec); v == nil || loss != "" {
				t.Fatal("positive codec control failed")
			}
			if zoneRoomFresh(o, in.Evaluation) != "fresh" {
				t.Fatal("positive freshness control failed")
			}
			tc.change(&o, &in.Evaluation)
			if got := zoneRoomFresh(o, in.Evaluation); got != "unknown" {
				t.Fatalf("malformed typed freshness = %s", got)
			}
		})
	}
}

// The mapper normalizes the register for each recognized role before identity
// equality. Check its separate input-slot guard directly to avoid that redundancy.
func TestP16SelectorRegisterRoleValidation(t *testing.T) {
	s := i31Fixture().Identity.Selector
	if !zoneRoomSelectorSupported(s) {
		t.Fatal("positive RR000F slot rejected")
	}
	s.Register = 0x20
	if zoneRoomSelectorSupported(s) {
		t.Fatal("unsupported input-slot register admitted")
	}
}
