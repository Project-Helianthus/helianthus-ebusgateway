package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"reflect"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func i34Fixture() (cylinderTemperatureRequest, semreg.MonotonicPoint) {
	digest := sha256.Sum256([]byte("public synthetic cylinder evidence"))
	id := cylinderTemperatureIdentity{
		Selector: cylinderTemperatureSelector{PB: 0xb5, SB: 0x24, Opcode: 0xb524, Operation: 2, Group: 5, Instance: 0, Register: 4, Direction: "target_to_initiator"},
		Asset:    "asset:synthetic", Service: "physical:synthetic", Source: "source:synthetic", Epoch: "epoch:synthetic", Generation: "1", Binding: "binding:synthetic",
		BindingRevision: "1", AssociationRevision: "1", MappingRevision: "1", Family: "synthetic-family", Profile: "synthetic-profile", ProfileRevision: "1", RegistryRevision: "1", CodecRevision: "1",
		RequestSource: "synthetic-initiator", RequestDestination: "synthetic-responder",
		Evidence: semreg.EvidenceRef{Owner: "test.native", Kind: "test.synthetic", Contract: "test.synthetic/v1", Digest: semreg.Digest("sha256:" + hex.EncodeToString(digest[:])), Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone},
	}
	receipt := semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "10"}
	c := cylinderTemperatureCodec{Identity: id, Qualified: true, BoundsKnown: true, Encoding: "f32-le", Unit: "unit.celsius", Minimum: -20, Maximum: 300}
	o := cylinderTemperatureObservation{Identity: id, ValueBytes: []byte{0, 0, 0, 0}, RawRequest: []byte{2, 5, 0, 4}, RawReply: []byte{0, 0, 0, 0}, Correlated: true, Successful: true, PresenceKnown: true, Present: true, EvidenceStatus: "Hypothesis", ReceivedAt: semreg.TimePoint{ClockID: "clock.synthetic", UnixNanoseconds: "10", UncertaintyNS: "0"}, Receipt: receipt, FreshForNS: "20", FM5Revision: "fm5:1"}
	return cylinderTemperatureRequest{Identity: id, Codec: c, Attempt: &o, FM5: cylinderTemperatureFM5{Interpretation: graphql.Fm5Interpretation{Mode: graphql.Fm5SemanticModeInterpreted, EvidenceRevision: "fm5:1"}, Coherent: true, Receipt: receipt, FreshForNS: "20"}}, semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "20"}
}
func i34Map(q cylinderTemperatureRequest, n semreg.MonotonicPoint) cylinderTemperatureCandidate {
	return mapCylinderTemperatureCandidates([]cylinderTemperatureRequest{q}, n)[0]
}
func i34Value(o *cylinderTemperatureObservation, v float32) {
	binary.LittleEndian.PutUint32(o.ValueBytes, math.Float32bits(v))
}
func i34Prior(q *cylinderTemperatureRequest) {
	o := *cloneCylinderObservation(q.Attempt)
	o.Receipt.Nanoseconds = "5"
	o.ReceivedAt.UnixNanoseconds = "5"
	i34Value(&o, 12.5)
	q.Previous = &cylinderTemperatureRecord{Observation: o, Codec: q.Codec}
}

func TestI34NativeVectors(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*cylinderTemperatureRequest)
		loss   string
	}{
		{"zero", func(*cylinderTemperatureRequest) {}, ""},
		{"missing", func(q *cylinderTemperatureRequest) { q.Attempt = nil }, "missing measurement"},
		{"short", func(q *cylinderTemperatureRequest) { q.Attempt.ValueBytes = []byte{0, 0, 0} }, "invalid value width"},
		{"long", func(q *cylinderTemperatureRequest) { q.Attempt.ValueBytes = []byte{0, 0, 0, 0, 0} }, "invalid value width"},
		{"nan", func(q *cylinderTemperatureRequest) { i34Value(q.Attempt, float32(math.NaN())) }, "non-finite native value"},
		{"positive_infinity", func(q *cylinderTemperatureRequest) { i34Value(q.Attempt, float32(math.Inf(1))) }, "non-finite native value"},
		{"negative_infinity", func(q *cylinderTemperatureRequest) { i34Value(q.Attempt, float32(math.Inf(-1))) }, "non-finite native value"},
		{"qualified_minimum", func(q *cylinderTemperatureRequest) { i34Value(q.Attempt, -20) }, ""},
		{"qualified_maximum", func(q *cylinderTemperatureRequest) { i34Value(q.Attempt, 300) }, ""},
		{"below_bounds", func(q *cylinderTemperatureRequest) { i34Value(q.Attempt, -21) }, "outside qualified native bounds"},
		{"above_bounds", func(q *cylinderTemperatureRequest) { i34Value(q.Attempt, 301) }, "outside qualified native bounds"},
		{"no_catalog_hint", func(q *cylinderTemperatureRequest) { i34Value(q.Attempt, 250) }, ""},
		{"unknown_bounds", func(q *cylinderTemperatureRequest) { q.Codec.BoundsKnown = false }, "native bounds unknown"},
		{"nan_bounds", func(q *cylinderTemperatureRequest) { q.Codec.Minimum = math.NaN() }, "native bounds unknown"},
		{"infinite_bounds", func(q *cylinderTemperatureRequest) { q.Codec.Maximum = math.Inf(1) }, "native bounds unknown"},
		{"reversed_bounds", func(q *cylinderTemperatureRequest) { q.Codec.Minimum = 301 }, "native bounds unknown"},
		{"unknown_unit", func(q *cylinderTemperatureRequest) { q.Codec.Unit = "" }, "native unit unknown"},
		{"unknown_codec", func(q *cylinderTemperatureRequest) { q.Codec.Encoding = "" }, "native encoding unknown"},
		{"unqualified", func(q *cylinderTemperatureRequest) { q.Codec.Qualified = false }, "unqualified codec identity"},
		{"existence_unknown", func(q *cylinderTemperatureRequest) { q.Attempt.PresenceKnown = false }, "native existence unknown or absent"},
		{"not_present", func(q *cylinderTemperatureRequest) { q.Attempt.Present = false }, "native existence unknown or absent"},
		{"failed", func(q *cylinderTemperatureRequest) { q.Attempt.Successful = false }, "uncorrelated or failed read"},
		{"uncorrelated", func(q *cylinderTemperatureRequest) { q.Attempt.Correlated = false }, "uncorrelated or failed read"},
		{"unknown_binding", func(q *cylinderTemperatureRequest) {
			q.Identity.Binding = ""
			q.Attempt.Identity = q.Identity
			q.Codec.Identity = q.Identity
		}, "unknown native binding"},
		{"bad_evidence", func(q *cylinderTemperatureRequest) {
			q.Identity.Evidence.Digest = "invalid"
			q.Attempt.Identity = q.Identity
			q.Codec.Identity = q.Identity
		}, "invalid native evidence"},
		{"bad_wall_receipt", func(q *cylinderTemperatureRequest) { q.Attempt.ReceivedAt.UnixNanoseconds = "bad" }, "invalid original receipt"},
		{"fm5_revision", func(q *cylinderTemperatureRequest) { q.Attempt.FM5Revision = "fm5:other" }, "FM5 revision mismatch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, n := i34Fixture()
			tt.mutate(&q)
			r := i34Map(q, n)
			if r.Loss != tt.loss || r.Current != (tt.loss == "") || r.Withheld == "" {
				t.Fatalf("unexpected candidate: %+v", r)
			}
			if tt.loss != "" && r.NativeValue != nil {
				t.Fatal("withheld quantity leaked")
			}
			if tt.name == "zero" && (r.NativeValue == nil || *r.NativeValue != 0) {
				t.Fatal("exact zero lost")
			}
		})
	}
	q, n := i34Fixture()
	q.Codec.Encoding = "f32-be"
	q.Attempt.ValueBytes = []byte{0x41, 0x48, 0, 0}
	if r := i34Map(q, n); r.NativeValue == nil || *r.NativeValue != 12.5 {
		t.Fatal("qualified big endian not honored")
	}
}

// These end-to-end identity cases retain separate selector and evidence fields.
// Native validation also rejects some prior codecs, so these cases alone do not
// independently discriminate every retention comparison; TestP17 adds helpers.
var i34IdentityPaths = []string{
	"Selector.PB", "Selector.SB", "Selector.Opcode", "Selector.Operation", "Selector.Group", "Selector.Instance", "Selector.Register", "Selector.Direction",
	"Asset", "Service", "Source", "Epoch", "Generation", "Binding", "BindingRevision", "AssociationRevision", "MappingRevision", "Family", "Profile", "ProfileRevision", "RegistryRevision", "CodecRevision", "RequestSource", "RequestDestination",
	"Evidence.Owner", "Evidence.Kind", "Evidence.Contract", "Evidence.Digest", "Evidence.Access", "Evidence.Redaction",
}

func i34Change(id *cylinderTemperatureIdentity, path string) {
	switch path {
	case "Evidence.Owner":
		id.Evidence.Owner = "test.other"
		return
	case "Evidence.Kind":
		id.Evidence.Kind = "test.alternate"
		return
	case "Evidence.Contract":
		id.Evidence.Contract = "test.synthetic/v2"
		return
	case "Evidence.Access":
		id.Evidence.Access = semreg.EvidenceAccessAuthorized
		return
	case "Evidence.Redaction":
		id.Evidence.Redaction = semreg.RedactionRedacted
		return
	case "Evidence.Digest":
		digest := sha256.Sum256([]byte("different public synthetic evidence"))
		id.Evidence.Digest = semreg.Digest("sha256:" + hex.EncodeToString(digest[:]))
		return
	}
	v := reflect.ValueOf(id).Elem()
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			v = v.FieldByName(path[start:i])
			start = i + 1
		}
	}
	v = v.FieldByName(path[start:])
	if v.Kind() == reflect.String {
		v.SetString(v.String() + ":different")
	} else {
		v.SetUint(v.Uint() + 1)
	}
}
func TestI34IdentityFences(t *testing.T) {
	for _, path := range i34IdentityPaths {
		t.Run(path, func(t *testing.T) {
			t.Run("attempt", func(t *testing.T) {
				q, n := i34Fixture()
				i34Change(&q.Attempt.Identity, path)
				r := i34Map(q, n)
				if r.Current || r.Loss != "foreign observation identity" {
					t.Fatalf("observation fence dropped: %+v", r)
				}
			})
			t.Run("codec", func(t *testing.T) {
				q, n := i34Fixture()
				i34Change(&q.Codec.Identity, path)
				r := i34Map(q, n)
				if r.Current || r.Loss != "unqualified codec identity" {
					t.Fatalf("codec fence dropped: %+v", r)
				}
			})
			t.Run("previous", func(t *testing.T) {
				q, n := i34Fixture()
				i34Prior(&q)
				q.Attempt = nil
				i34Change(&q.Previous.Observation.Identity, path)
				r := i34Map(q, n)
				if r.LastGood != nil || r.Structural || r.History == nil {
					t.Fatal("foreign history inherited or discarded")
				}
			})
			t.Run("previous_codec", func(t *testing.T) {
				q, n := i34Fixture()
				i34Prior(&q)
				q.Attempt = nil
				i34Change(&q.Previous.Codec.Identity, path)
				if r := i34Map(q, n); r.LastGood != nil {
					t.Fatal("foreign prior codec inherited")
				}
			})
		})
	}
}

func TestI34Freshness(t *testing.T) {
	for _, ns := range []string{"", "bad", "-1", "+1", "01", "18446744073709551616"} {
		for _, which := range []string{"receipt", "evaluation", "window", "fm5_receipt", "fm5_window"} {
			t.Run(which+"/"+ns, func(t *testing.T) {
				q, n := i34Fixture()
				switch which {
				case "receipt":
					q.Attempt.Receipt.Nanoseconds = semreg.Uint64(ns)
				case "evaluation":
					n.Nanoseconds = semreg.Uint64(ns)
				case "window":
					q.Attempt.FreshForNS = semreg.Uint64(ns)
				case "fm5_receipt":
					q.FM5.Receipt.Nanoseconds = semreg.Uint64(ns)
				case "fm5_window":
					q.FM5.FreshForNS = semreg.Uint64(ns)
				}
				if r := i34Map(q, n); r.Current || r.Loss == "" {
					t.Fatal("malformed typed time admitted")
				}
			})
		}
	}
	cases := []struct {
		name   string
		mutate func(*cylinderTemperatureRequest, *semreg.MonotonicPoint)
	}{
		{"expired", func(q *cylinderTemperatureRequest, n *semreg.MonotonicPoint) { q.Attempt.FreshForNS = "10" }},
		{"zero_policy", func(q *cylinderTemperatureRequest, n *semreg.MonotonicPoint) { q.Attempt.FreshForNS = "0" }},
		{"future", func(q *cylinderTemperatureRequest, n *semreg.MonotonicPoint) { q.Attempt.Receipt.Nanoseconds = "21" }},
		{"receipt_epoch", func(q *cylinderTemperatureRequest, n *semreg.MonotonicPoint) {
			q.Attempt.Receipt.ClockEpochID = "clock:other"
		}},
		{"missing_epoch", func(q *cylinderTemperatureRequest, n *semreg.MonotonicPoint) { q.Attempt.Receipt.ClockEpochID = "" }},
		{"evaluation_epoch", func(q *cylinderTemperatureRequest, n *semreg.MonotonicPoint) { n.ClockEpochID = "" }},
		{"fm5_expired", func(q *cylinderTemperatureRequest, n *semreg.MonotonicPoint) { q.FM5.FreshForNS = "10" }},
		{"fm5_future", func(q *cylinderTemperatureRequest, n *semreg.MonotonicPoint) { q.FM5.Receipt.Nanoseconds = "21" }},
		{"fm5_epoch", func(q *cylinderTemperatureRequest, n *semreg.MonotonicPoint) {
			q.FM5.Receipt.ClockEpochID = "clock:other"
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			q, n := i34Fixture()
			i34Prior(&q)
			tt.mutate(&q, &n)
			r := i34Map(q, n)
			if r.Current || r.NativeValue != nil || r.LastGood == nil || r.LastGood.Observation.Receipt.Nanoseconds != "5" {
				t.Fatal("stale/unknown age renewed last good")
			}
		})
	}
	q, n := i34Fixture()
	q.Attempt.FreshForNS = "11"
	if !i34Map(q, n).Current {
		t.Fatal("fresh boundary rejected")
	}
}

func TestI34RetentionRecoveryAndFM5(t *testing.T) {
	q, n := i34Fixture()
	i34Prior(&q)
	q.Attempt.ValueBytes = []byte{1}
	r := i34Map(q, n)
	if r.Current || !r.Structural || r.LastGood == nil || r.LastGood.Observation.ReceivedAt.UnixNanoseconds != "5" || r.Attempt.ReceivedAt.UnixNanoseconds != "10" {
		t.Fatal("partial loss erased or renewed evidence")
	}
	q.Attempt.ValueBytes = []byte{0, 0, 0, 0}
	r = i34Map(q, n)
	if !r.Current || r.LastGood.Observation.Receipt.Nanoseconds != "10" {
		t.Fatal("fresh recovery failed")
	}
	q.Previous = r.LastGood
	r = i34Map(q, n)
	if r.Current || r.Loss != "receipt replay or clock change" {
		t.Fatal("replay refreshed receipt")
	}
	for _, reason := range []graphql.Fm5SemanticDegradedReason{graphql.Fm5SemanticDegradedReasonControllerUnreachable, graphql.Fm5SemanticDegradedReasonConfigurationUnavailable, graphql.Fm5SemanticDegradedReasonSolarAcquisitionFailed, graphql.Fm5SemanticDegradedReasonCylinderAcquisitionFailed, graphql.Fm5SemanticDegradedReasonEvidenceStale, graphql.Fm5SemanticDegradedReasonIncoherentAcquisition} {
		t.Run(string(reason), func(t *testing.T) {
			q, n := i34Fixture()
			i34Prior(&q)
			q.FM5.Interpretation.DegradedReason = reason
			r := i34Map(q, n)
			if r.Current || r.Withdrawn || !r.Structural || r.LastGood == nil || r.FM5.Interpretation != q.FM5.Interpretation || r.Loss != "transient FM5 degradation" {
				t.Fatal("transient loss changed structural verdict")
			}
		})
	}
	for _, mode := range []graphql.Fm5SemanticMode{graphql.Fm5SemanticModeAbsent, graphql.Fm5SemanticModeGPIOOnly} {
		t.Run(string(mode), func(t *testing.T) {
			q, n := i34Fixture()
			i34Prior(&q)
			q.FM5.Interpretation.Mode = mode
			if mode == graphql.Fm5SemanticModeGPIOOnly {
				q.FM5.Interpretation.DegradedReason = graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable
			}
			r := i34Map(q, n)
			if !r.Withdrawn || r.Structural || r.LastGood != nil || r.History == nil || r.NativeValue != nil {
				t.Fatal("structural withdrawal left ghost")
			}
			q.FM5.Coherent = false
			r = i34Map(q, n)
			if r.Withdrawn || r.LastGood == nil {
				t.Fatal("incoherent evidence withdrew structure")
			}
		})
	}
	for _, change := range []func(*cylinderTemperatureRequest){func(q *cylinderTemperatureRequest) { q.FM5.Interpretation.EvidenceRevision = "" }, func(q *cylinderTemperatureRequest) { q.FM5.Interpretation.Mode = "UNKNOWN" }, func(q *cylinderTemperatureRequest) { q.FM5.Interpretation.DegradedReason = "UNKNOWN" }, func(q *cylinderTemperatureRequest) { q.FM5.Coherent = false }} {
		q, n := i34Fixture()
		change(&q)
		if i34Map(q, n).Current {
			t.Fatal("unknown FM5 admitted")
		}
	}
	q, n = i34Fixture()
	q.Attempt = nil
	r = i34Map(q, n)
	if r.Structural || r.LastGood != nil {
		t.Fatal("configuration alone created cylinder")
	}
}

func TestI34PermutationCollisionAndDetachedHistory(t *testing.T) {
	a, n := i34Fixture()
	i34Prior(&a)
	b, _ := i34Fixture()
	b.Identity.Selector.Instance = 1
	b.Attempt.Identity = b.Identity
	b.Codec.Identity = b.Identity
	i34Value(b.Attempt, 25)
	forward := mapCylinderTemperatureCandidates([]cylinderTemperatureRequest{a, b}, n)
	reverse := mapCylinderTemperatureCandidates([]cylinderTemperatureRequest{b, a}, n)
	if !reflect.DeepEqual(forward, reverse) || len(forward) != 2 {
		t.Fatal("instance collision or permutation drift")
	}
	values := map[byte]float64{}
	for _, r := range forward {
		if !r.Current || r.NativeValue == nil {
			t.Fatal("distinct instance lost")
		}
		values[r.Identity.Selector.Instance] = *r.NativeValue
	}
	if values[0] != 0 || values[1] != 25 {
		t.Fatal("same asset instance values collapsed")
	}
	duplicate := mapCylinderTemperatureCandidates([]cylinderTemperatureRequest{a, a}, n)
	for _, r := range duplicate {
		if r.Current || r.Loss != "ambiguous duplicate instance binding" {
			t.Fatal("duplicate binding silently won")
		}
	}
	// Failed sibling leaves its own last good and the healthy sibling intact.
	a.Attempt.ValueBytes = []byte{0}
	mixed := mapCylinderTemperatureCandidates([]cylinderTemperatureRequest{a, b}, n)
	for _, r := range mixed {
		if r.Identity.Selector.Instance == 0 {
			if r.Current || r.LastGood == nil {
				t.Fatal("partial history erased")
			}
		} else if !r.Current {
			t.Fatal("healthy sibling erased")
		}
	}
	r := i34Map(a, n)
	r.Attempt.RawRequest[0] = 99
	r.History.Observation.ValueBytes[0] = 99
	r.LastGood.Observation.RawReply[0] = 99
	if a.Attempt.RawRequest[0] != 2 || a.Previous.Observation.ValueBytes[0] != 0 || a.Previous.Observation.RawReply[0] != 0 {
		t.Fatal("output aliases input history")
	}
	a.Previous.Observation.ReceivedAt.UnixNanoseconds = "6"
	a.Attempt.RawReply[0] = 88
	if r.History.Observation.ReceivedAt.UnixNanoseconds != "5" || r.Attempt.RawReply[0] != 0 || r.History.Observation.RawReply[0] != 0 {
		t.Fatal("input aliases detached output")
	}
}

func TestI34NativeValuesAndSelectorIsolation(t *testing.T) {
	for _, tt := range []struct {
		input float32
		want  float64
	}{{0, 0}, {12.5, 12.5}, {-20, -20}, {300, 300}, {250, 250}, {0.1, 0.10000000149011612}} {
		q, n := i34Fixture()
		i34Value(q.Attempt, tt.input)
		r := i34Map(q, n)
		if !r.Current || r.NativeValue == nil || *r.NativeValue != tt.want || r.Withheld != "complete Thermal binding and decimal policy unresolved" {
			t.Fatalf("native value changed: %+v; want %v", r, tt.want)
		}
	}
	for _, path := range []string{"Selector.PB", "Selector.SB", "Selector.Opcode", "Selector.Operation", "Selector.Group", "Selector.Register", "Selector.Direction"} {
		t.Run(path, func(t *testing.T) {
			q, n := i34Fixture()
			i34Change(&q.Identity, path)
			q.Codec.Identity = q.Identity
			q.Attempt.Identity = q.Identity
			r := i34Map(q, n)
			if r.Current || r.Loss != "unsupported native selector" {
				t.Fatal("unsupported native plane accepted")
			}
		})
	}
	q, n := i34Fixture()
	q.Attempt.ValueBytes = nil
	if r := i34Map(q, n); r.Loss != "invalid value width" {
		t.Fatal("missing payload became zero")
	}
	q, n = i34Fixture()
	i34Prior(&q)
	q.FM5.Interpretation.Mode = graphql.Fm5SemanticModeAbsent
	q.FM5.FreshForNS = "10"
	if r := i34Map(q, n); r.Withdrawn || r.LastGood == nil {
		t.Fatal("stale structural verdict withdrew history")
	}
	// Evidence grade is carried unchanged and never supplies qualification.
	for _, grade := range []string{"Proven", "Hypothesis", "Unknown"} {
		q, n := i34Fixture()
		q.Attempt.EvidenceStatus = grade
		r := i34Map(q, n)
		if !r.Current || r.Attempt.EvidenceStatus != grade || r.Withheld == "" {
			t.Fatal("evidence grade confused with qualification")
		}
	}
}

func TestP17EvidenceGrades(t *testing.T) {
	for _, grade := range []string{"", "Bogus", "Proven", "Hypothesis", "Unknown"} {
		t.Run("grade_"+grade, func(t *testing.T) {
			valid := grade == "Proven" || grade == "Hypothesis" || grade == "Unknown"
			for _, qualified := range []bool{true, false} {
				t.Run(map[bool]string{true: "qualified", false: "unqualified"}[qualified], func(t *testing.T) {
					q, n := i34Fixture()
					q.Attempt.EvidenceStatus = grade
					q.Codec.Qualified = qualified
					r := i34Map(q, n)
					if r.Current != (valid && qualified) || (r.LastGood != nil) != (valid && qualified) || r.Attempt.EvidenceStatus != grade {
						t.Fatalf("grade supplied qualification or malformed metadata admitted: %+v", r)
					}
					if !valid && r.Loss != "invalid native evidence grade" {
						t.Fatalf("invalid grade not diagnosed independently: %q", r.Loss)
					}
					if valid && !qualified && r.Loss != "unqualified codec identity" {
						t.Fatalf("valid grade supplied qualification: %q", r.Loss)
					}
				})
			}
			q, n := i34Fixture()
			i34Prior(&q)
			q.Previous.Observation.EvidenceStatus = grade
			q.Attempt = nil
			r := i34Map(q, n)
			if (r.LastGood != nil) != valid || r.Structural != valid || r.History == nil || r.History.Observation.EvidenceStatus != grade {
				t.Fatalf("prior grade retention lost validation or diagnostic history: %+v", r)
			}
		})
	}
}

func TestP17PreviousCodecFields(t *testing.T) {
	for _, tc := range []struct {
		name        string
		change      func(*cylinderTemperatureCodec)
		nativeValid bool
	}{
		{"Qualified", func(c *cylinderTemperatureCodec) { c.Qualified = false }, false},
		{"BoundsKnown", func(c *cylinderTemperatureCodec) { c.BoundsKnown = false }, false},
		{"Encoding", func(c *cylinderTemperatureCodec) { c.Encoding = "f32-be" }, true},
		{"Unit", func(c *cylinderTemperatureCodec) { c.Unit = "unit.kelvin" }, false},
		{"Minimum", func(c *cylinderTemperatureCodec) { c.Minimum = -19 }, true},
		{"Maximum", func(c *cylinderTemperatureCodec) { c.Maximum = 299 }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, n := i34Fixture()
			i34Prior(&q)
			q.Attempt = nil
			if r := i34Map(q, n); r.LastGood == nil || !r.Structural {
				t.Fatal("equal codec positive control failed")
			}
			if !sameCylinderTemperatureCodec(q.Previous.Codec, q.Codec) {
				t.Fatal("equal helper positive control failed")
			}
			tc.change(&q.Previous.Codec)
			// Both supported encodings represent the same independent expected value.
			if tc.name == "Encoding" {
				q.Previous.Observation.ValueBytes = []byte{0x41, 0x48, 0, 0}
			}
			value, loss := cylinderTemperatureNative(q.Previous.Observation, q.Previous.Codec)
			if (loss == "") != tc.nativeValid || (tc.nativeValid && value != 12.5) {
				t.Fatalf("unexpected native masking: value=%v loss=%q", value, loss)
			}
			// Qualified, BoundsKnown and Unit are redundant end-to-end guards;
			// the helper assertion independently discriminates those comparisons.
			if sameCylinderTemperatureCodec(q.Previous.Codec, q.Codec) || sameCylinderTemperatureCodec(q.Codec, q.Previous.Codec) {
				t.Fatal("changed codec field compared equal")
			}
			r := i34Map(q, n)
			if r.LastGood != nil || r.Structural || r.History == nil || !reflect.DeepEqual(r.History, q.Previous) {
				t.Fatal("changed prior codec retained or diagnostic evidence lost")
			}
		})
	}
}

func TestP17CompleteCodecIdentity(t *testing.T) {
	for _, path := range i34IdentityPaths {
		t.Run(path, func(t *testing.T) {
			q, _ := i34Fixture()
			other := q.Codec
			if !sameCylinderTemperatureCodec(q.Codec, other) {
				t.Fatal("equal codec identity positive control failed")
			}
			i34Change(&other.Identity, path)
			if other.Identity.Evidence.Validate() != nil {
				t.Fatal("identity alternative has malformed evidence")
			}
			// Selector alternatives need not be admitted by the native decoder:
			// this directly tests equality of otherwise well-formed codec metadata.
			if sameCylinderTemperatureCodec(q.Codec, other) || sameCylinderTemperatureCodec(other, q.Codec) {
				t.Fatal("codec identity field omitted from equality")
			}
		})
	}
}

func TestP17ClockEpochOwnerBoundary(t *testing.T) {
	q, n := i34Fixture()
	i34Prior(&q)
	original := cloneCylinderRecord(q.Previous)
	if r := i34Map(q, n); !r.Current {
		t.Fatal("same epoch positive control failed")
	}
	// The sample, FM5 and evaluation agree on the new epoch, isolating the
	// comparison with the retained receipt rather than the freshness guard.
	q.Attempt.Receipt.ClockEpochID = "clock:new-synthetic"
	q.FM5.Receipt.ClockEpochID = "clock:new-synthetic"
	n.ClockEpochID = "clock:new-synthetic"
	r := i34Map(q, n)
	if r.Current || r.NativeValue != nil || r.Loss != "receipt replay or clock change" || !r.Structural || !reflect.DeepEqual(r.LastGood, original) || !reflect.DeepEqual(r.History, original) {
		t.Fatalf("old binding crossed clock epoch or lost history: %+v", r)
	}
	// Only the owner supplies this new binding; the mapper never invents it.
	q.Identity.Binding = "binding:new-synthetic"
	q.Attempt.Identity = q.Identity
	q.Codec.Identity = q.Identity
	r = i34Map(q, n)
	if !r.Current || !r.Structural || r.Loss != "" || r.NativeValue == nil || *r.NativeValue != 0 || r.LastGood == nil || r.LastGood.Observation.Identity.Binding != "binding:new-synthetic" || r.LastGood.Observation.Receipt.ClockEpochID != "clock:new-synthetic" || !reflect.DeepEqual(r.History, original) {
		t.Fatalf("owner boundary did not recover detached current evidence: %+v", r)
	}
}

func TestP17HigherInstanceCallerQualification(t *testing.T) {
	for _, instance := range []byte{2, 255} {
		for _, qualified := range []bool{true, false} {
			q, n := i34Fixture()
			q.Identity.Selector.Instance = instance
			q.Attempt.Identity = q.Identity
			q.Codec.Identity = q.Identity
			q.Codec.Qualified = qualified
			r := i34Map(q, n)
			if r.Current != qualified || (r.LastGood != nil) != qualified || r.Identity.Selector.Instance != instance || r.Withheld == "" {
				t.Fatalf("instance %d bypassed caller qualification or gained projection: %+v", instance, r)
			}
		}
	}
}
