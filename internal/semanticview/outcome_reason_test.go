package semanticview

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func TestOutcomeReasonPrecedence(t *testing.T) {
	tests := []struct {
		name    string
		kind    projection.ItemKind
		outcome projection.ProjectionOutcome
		reason  semreg.DefinitionID
	}{
		{"expired_fact", projection.ItemFact, projection.ProjectionWithheld, "semantic.read.reason.fact_unavailable"},
		{"withdrawn_fact", projection.ItemFact, projection.ProjectionWithheld, "z.native.detail"},
		{"unavailable_capability", projection.ItemCapability, projection.ProjectionWithheld, "semantic.read.reason.capability_unavailable"},
		{"unqualified_capability", projection.ItemCapability, projection.ProjectionWithheld, "semantic.read.reason.capability_unqualified"},
		{"relevant_fact", projection.ItemFact, projection.ProjectionWithheld, "z.native.detail"},
		{"relevant_availability", projection.ItemCapability, projection.ProjectionWithheld, "z.native.detail"},
		{"unpromoted_fact", projection.ItemFact, projection.ProjectionWithheld, "z.native.detail"},
		{"bad_validity_fact", projection.ItemFact, projection.ProjectionWithheld, "z.native.detail"},
		{"relevant_qualification", projection.ItemCapability, projection.ProjectionWithheld, "z.native.detail"},
		{"exact_fact", projection.ItemFact, projection.ProjectionExact, ""},
		{"exact_capability", projection.ItemCapability, projection.ProjectionExact, ""},
	}
	for _, tt := range tests {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%t", tt.name, reverse), func(t *testing.T) {
				s := outcomeReasonFixture(t)
				for i := range s.Facts {
					c := &s.Facts[i].Candidates[0]
					c.Quality.Reasons = []semreg.DefinitionID{"a.native.detail", "z.native.detail"}

					switch tt.name {
					case "unpromoted_fact":
						c.Quality.Promotion = semreg.PromotionUnpromoted
					case "bad_validity_fact":
						c.Quality.Promotion = semreg.PromotionUnpromoted
						c.Quality.Validity = semreg.ValidityBad
					case "withdrawn_fact":
						c.Quality.Availability = semreg.AvailabilityWithdrawn
						c.Quality.Promotion = semreg.PromotionUnpromoted
						c.Quality.Reasons = []semreg.DefinitionID{"z.native.detail"}
						c.Value = nil
					case "relevant_fact", "relevant_availability":
						c.Value = nil
						c.Quality.Availability = semreg.AvailabilityWithdrawn
						c.Quality.Promotion = semreg.PromotionUnpromoted
					case "relevant_qualification":
						c.Quality.Qualification = semreg.QualificationCandidate
						c.Quality.Promotion = semreg.PromotionUnpromoted
					}
				}
				for i := range s.Facts {
					c := s.Facts[i].Candidates[0]
					c.CandidateID += ":second"
					if tt.name == "relevant_fact" || tt.name == "relevant_availability" || tt.name == "relevant_qualification" || tt.name == "unpromoted_fact" || tt.name == "bad_validity_fact" {
						c.Quality.Reasons = []semreg.DefinitionID{"b.native.detail", "z.native.detail"}
					}

					s.Facts[i].Candidates = append(s.Facts[i].Candidates, c)
					if reverse {
						cs := s.Facts[i].Candidates
						cs[0].CandidateID, cs[1].CandidateID = cs[1].CandidateID, cs[0].CandidateID
						sort.Slice(cs, func(i, j int) bool { return cs[i].CandidateID < cs[j].CandidateID })
					}
				}
				for i := range s.Capabilities {
					switch tt.name {
					case "unavailable_capability", "relevant_availability":
						s.Capabilities[i].Availability = semreg.AvailabilityUnavailable
					case "unqualified_capability", "relevant_qualification":
						s.Capabilities[i].Qualification = semreg.QualificationCandidate
					}
				}
				m08Seal(t, &s)
				context := m08Evaluate(t, s).Context
				if tt.name == "expired_fact" {
					context.EvaluateMonotonic.Nanoseconds = "3"
				}
				e, err := semreg.EvaluateSnapshot(s, context)
				if err != nil {
					t.Fatal(err)
				}
				in := Input{AssetID: s.AssetID, Snapshot: &s, Evaluation: &e}
				before := m08JSON(t, in)
				v := m08Build(t, in)
				count := 0
				for _, d := range v.Assets[0].Projection.Dispositions {
					if d.Kind != tt.kind || len(d.SourceKeys) == 0 {
						continue
					}
					count++
					if d.Outcome != tt.outcome {
						t.Errorf("%s outcome = %s, want %s", d.ItemID, d.Outcome, tt.outcome)
					}
					if tt.reason == "" {
						if d.Reason != nil {
							t.Errorf("%s exact reason = %s", d.ItemID, *d.Reason)
						}
					} else if d.Reason == nil || *d.Reason != tt.reason {
						t.Errorf("%s reason = %v, want %s", d.ItemID, d.Reason, tt.reason)
					}
				}
				if count != 5 {
					t.Fatalf("selected %d dispositions, want five packs", count)
				}
				if !bytes.Equal(before, m08JSON(t, in)) {
					t.Fatal("native input evidence changed")
				}
				if !bytes.Equal(m08JSON(t, s), m08JSON(t, *v.Assets[0].Snapshot)) {
					t.Fatal("native snapshot evidence lost")
				}
			})
		}
	}
}

func TestOutcomeReasonSTD01(t *testing.T) {
	for _, state := range []string{"exact", "expired", "unavailable", "unqualified"} {
		t.Run(state, func(t *testing.T) {
			s := outcomeReasonFixture(t)
			for i := range s.Facts {
				s.Facts[i].Candidates[0].Quality.Reasons = []semreg.DefinitionID{"a.unrelated", "unknown_pending_std_01"}
			}
			for i := range s.Capabilities {
				if state == "unavailable" {
					s.Capabilities[i].Availability = semreg.AvailabilityUnavailable
				}
				if state == "unqualified" {
					s.Capabilities[i].Qualification = semreg.QualificationCandidate
				}
			}
			m08Seal(t, &s)
			ctx := m08Evaluate(t, s).Context
			if state == "expired" {
				ctx.EvaluateMonotonic.Nanoseconds = "3"
			}
			e, err := semreg.EvaluateSnapshot(s, ctx)
			if err != nil {
				t.Fatal(err)
			}
			v := m08Build(t, Input{AssetID: s.AssetID, Snapshot: &s, Evaluation: &e})
			count := 0
			for _, d := range v.Assets[0].Projection.Dispositions {
				if len(d.SourceKeys) == 0 {
					continue
				}
				count++
				if d.Outcome != projection.ProjectionUnknown || d.Reason == nil || *d.Reason != "unknown_pending_std_01" {
					t.Errorf("STD-01 changed: %+v", d)
				}
			}
			if count != 10 {
				t.Fatalf("selected %d dispositions, want ten", count)
			}
		})
	}
}

func outcomeReasonFixture(t *testing.T) semreg.Snapshot {
	t.Helper()
	s := m08Fixture(t)
	registry := m08Registry(t)
	for _, validator := range m08Validators() {
		metadata := validator.(semreg.PackMetadataProvider).Metadata()
		matched := false
		for _, field := range metadata.Fields {
			if field.CanonicalUnit == nil && field.Ref.ID != "thermal.action.state" {
				continue
			}
			for _, cap := range metadata.Capabilities {
				if !registry.FieldMatches(field.Ref, cap.Service, cap.Ref) {
					continue
				}
				for i := range s.Facts {
					f := &s.Facts[i]
					if f.Key.PackID != metadata.Pack.ID {
						continue
					}
					f.Key.FactID = field.Ref.ID
					f.Key.Dimensions[0].ID = field.Dimension.ID
					f.Candidates[0].Key = f.Key
					if field.CanonicalUnit != nil {
						f.Candidates[0].Value.Quantity.Unit = field.CanonicalUnit.ID
					} else {
						f.Candidates[0].Value = &semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: "thermal.action.state", Token: "idle", Known: true}}
					}
				}
				for i := range s.Services {
					if s.Services[i].Definition.Pack == metadata.Pack {
						s.Services[i].Definition = cap.Service
					}
				}
				for i := range s.Capabilities {
					if s.Capabilities[i].Definition.Pack == metadata.Pack {
						s.Capabilities[i].Definition = cap.Ref
					}
				}
				matched = true
				break
			}
			if matched {
				break
			}
		}
		if !matched {
			t.Fatalf("no supporting capability for %s", metadata.Pack.ID)
		}
	}
	m08Seal(t, &s)
	return s
}

func TestP12CausalKeys(t *testing.T) {
	for _, state := range []string{"unavailable", "withdrawn", "expired", "unpromoted", "bad_validity", "unqualified", "no_common", "mixed_causal_key"} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%t", state, reverse), func(t *testing.T) {
				s := outcomeReasonFixture(t)
				base := s.Facts[0]
				sibling := base
				sibling.Key.Dimensions = append([]semreg.Dimension{}, base.Key.Dimensions...)
				label := "synthetic-sibling"
				sibling.Key.Dimensions[0].Value = semreg.Value{Kind: semreg.ValueText, Text: &label}
				sibling.Candidates = append([]semreg.FactCandidate{}, base.Candidates...)
				sibling.Candidates[0].CandidateID = "candidate:p12-sibling"
				sibling.Candidates[0].Key = sibling.Key
				sibling.Candidates[0].Quality.Reasons = []semreg.DefinitionID{"a.sibling"}
				sibling.Candidates[0].FreshnessPolicy.FreshForNS = "3"
				sibling.Candidates[0].FreshnessPolicy.RetainForNS = "4"
				c := &s.Facts[0].Candidates[0]
				c.Quality.Promotion = semreg.PromotionUnpromoted
				c.Quality.Reasons = []semreg.DefinitionID{"a.causal", "z.causal"}
				want := semreg.DefinitionID("z.causal")
				switch state {
				case "unpromoted":
				case "unqualified":
					c.Quality.Qualification = semreg.QualificationCandidate
				case "bad_validity":
					c.Quality.Validity = semreg.ValidityBad
				default:
					c.Quality.Availability = semreg.AvailabilityUnavailable
					if state == "withdrawn" {
						c.Quality.Availability = semreg.AvailabilityWithdrawn
						c.Value = nil
					}
					if state == "expired" {
						c.Quality.Availability = semreg.AvailabilityAvailable
						want = reasonPrefix + "fact_unavailable"
					}
				}
				second := *c
				second.CandidateID += ":second"
				second.Quality.Reasons = []semreg.DefinitionID{"b.causal", "z.causal"}
				if state == "no_common" {
					second.Quality.Reasons = []semreg.DefinitionID{"b.causal"}
					want = reasonPrefix + "fact_unavailable"
				}
				if state == "mixed_causal_key" {
					second.Quality.Availability = semreg.AvailabilityAvailable
					want = reasonPrefix + "fact_unavailable"
				}
				s.Facts[0].Candidates = append(s.Facts[0].Candidates, second)
				if reverse {
					cs := s.Facts[0].Candidates
					cs[0].CandidateID, cs[1].CandidateID = cs[1].CandidateID, cs[0].CandidateID
					sort.Slice(cs, func(i, j int) bool { return cs[i].CandidateID < cs[j].CandidateID })
				}
				s.Facts = append(s.Facts, sibling)
				sort.Slice(s.Facts, func(i, j int) bool {
					a, _ := semreg.CanonicalJSON(s.Facts[i].Key)
					b, _ := semreg.CanonicalJSON(s.Facts[j].Key)
					return bytes.Compare(a, b) < 0
				})
				m08Seal(t, &s)
				ctx := m08Evaluate(t, s).Context
				if state == "expired" || state == "mixed_causal_key" {
					ctx.EvaluateMonotonic.Nanoseconds = "3"
				}
				e, err := semreg.EvaluateSnapshot(s, ctx)
				if err != nil {
					t.Fatal(err)
				}
				in := Input{AssetID: s.AssetID, Snapshot: &s, Evaluation: &e}
				before := m08JSON(t, in)
				v := m08Build(t, in)
				d := m08FactDisposition(t, v, base.Key.FactID)
				if d.Outcome != projection.ProjectionWithheld || d.Reason == nil || *d.Reason != want || len(d.SourceKeys) != 2 {
					t.Fatalf("disposition = %+v, want withheld/%s with both keys", d, want)
				}
				if !bytes.Equal(before, m08JSON(t, in)) || !bytes.Equal(m08JSON(t, s), m08JSON(t, *v.Assets[0].Snapshot)) {
					t.Fatal("native evidence changed")
				}
				if state == "unavailable" || state == "withdrawn" || state == "unqualified" {
					for i := range s.Capabilities {
						if state == "unqualified" {
							s.Capabilities[i].Qualification = semreg.QualificationCandidate
						} else {
							s.Capabilities[i].Availability = semreg.AvailabilityUnavailable
						}
					}
					m08Seal(t, &s)
					e = m08Evaluate(t, s)
					v = m08Build(t, Input{AssetID: s.AssetID, Snapshot: &s, Evaluation: &e})
					count := 0
					for _, cap := range v.Assets[0].Projection.Dispositions {
						if cap.Kind == projection.ItemCapability && len(cap.SourceKeys) == 2 {
							count++
							if cap.Reason == nil || *cap.Reason != want {
								t.Fatalf("capability reason = %+v, want %s", cap, want)
							}
						}
					}
					if count != 1 {
						t.Fatalf("selected %d capabilities, want one", count)
					}
				}
			})
		}
	}
}

// Direct-helper oracles isolate complete-key grouping and the evaluation join
// from upstream validation, which also guards several of these identity axes.
func TestP12CausalKeyIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*semreg.FactCandidate)
	}{
		{"pack_id", func(c *semreg.FactCandidate) { c.Key.PackID = "pack.synthetic.other" }},
		{"pack_version", func(c *semreg.FactCandidate) { c.Key.PackVersion = "9.0.0" }},
		{"fact_id", func(c *semreg.FactCandidate) { c.Key.FactID = "fact.synthetic.other" }},
		{"dimension_id", func(c *semreg.FactCandidate) { c.Key.Dimensions[0].ID = "dimension.synthetic.other" }},
		{"dimension_kind", func(c *semreg.FactCandidate) {
			v := true
			c.Key.Dimensions[0].Value = semreg.Value{Kind: semreg.ValueBoolean, Boolean: &v}
		}},
		{"dimension_value", func(c *semreg.FactCandidate) {
			v := "synthetic-other"
			c.Key.Dimensions[0].Value = semreg.Value{Kind: semreg.ValueText, Text: &v}
		}},
		{"second_dimension_id", func(c *semreg.FactCandidate) { c.Key.Dimensions[1].ID = "zz.dimension.synthetic.third" }},
		{"second_dimension_kind", func(c *semreg.FactCandidate) {
			v := true
			c.Key.Dimensions[1].Value = semreg.Value{Kind: semreg.ValueBoolean, Boolean: &v}
		}},
		{"second_dimension_value", func(c *semreg.FactCandidate) {
			v := "synthetic-other"
			c.Key.Dimensions[1].Value = semreg.Value{Kind: semreg.ValueText, Text: &v}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := outcomeReasonFixture(t).Facts[0].Candidates[0]
			label := "synthetic-second"
			c.Key.Dimensions = append(c.Key.Dimensions, semreg.Dimension{ID: "zz.dimension.synthetic.second", Value: semreg.Value{Kind: semreg.ValueText, Text: &label}})
			c.Quality.Availability = semreg.AvailabilityUnavailable
			c.Quality.Promotion = semreg.PromotionUnpromoted
			c.Quality.Reasons = []semreg.DefinitionID{"z.causal"}
			sibling, err := clone(c)
			if err != nil {
				t.Fatal(err)
			}
			sibling.CandidateID = "candidate:p12-sibling"
			sibling.Quality.Availability = semreg.AvailabilityAvailable
			tc.edit(&sibling)
			if err := sibling.Key.Validate(); err != nil {
				t.Fatal(err)
			}
			e := semreg.EvaluationView{Facts: []semreg.EvaluatedFact{{CandidateID: c.CandidateID, CandidateRevision: c.Revision, EffectiveAvailability: semreg.AvailabilityUnavailable}, {CandidateID: sibling.CandidateID, CandidateRevision: sibling.Revision, EffectiveAvailability: semreg.AvailabilityAvailable}}}
			got := causalOutcomeCandidates(reasonPrefix+"fact_unavailable", projection.ItemFact, []semreg.FactCandidate{c, sibling}, e)
			if !reflect.DeepEqual(got, []semreg.FactCandidate{c}) {
				t.Fatalf("causal candidates = %+v, want original only", got)
			}
			// Positive control: the same complete key has an available candidate.
			sibling.Key = c.Key
			if got := causalOutcomeCandidates(reasonPrefix+"fact_unavailable", projection.ItemFact, []semreg.FactCandidate{c, sibling}, e); len(got) != 0 {
				t.Fatal("available candidate on same key did not clear causality")
			}
		})
	}
}

func TestP12CandidateIdentity(t *testing.T) {
	for _, axis := range []string{"candidate_id", "revision", "source_id", "source_epoch", "binding_id", "driver_generation", "origin_id"} {
		t.Run(axis, func(t *testing.T) {
			c := outcomeReasonFixture(t).Facts[0].Candidates[0]
			c.Quality.Availability = semreg.AvailabilityUnavailable
			c.Quality.Promotion = semreg.PromotionUnpromoted
			c.Quality.Reasons = []semreg.DefinitionID{"z.causal"}
			switch axis {
			case "candidate_id":
				c.CandidateID = "candidate:p12-other"
			case "revision":
				c.Revision = "2"
			case "source_id":
				v := semreg.SourceID("source:p12-other")
				c.Origin.SourceID = &v
			case "source_epoch":
				v := semreg.SourceEpochID("epoch:p12-other")
				c.SourceEpochID = &v
				c.Origin.SourceEpochID = &v
			case "binding_id":
				v := semreg.NativeBindingID("binding:p12-other")
				c.BindingID = &v
				c.Origin.BindingID = &v
			case "driver_generation":
				v := semreg.Uint64("2")
				c.DriverGeneration = &v
			case "origin_id":
				c.Origin.OriginID = "origin:p12-other"
			}
			e := semreg.EvaluationView{Facts: []semreg.EvaluatedFact{{CandidateID: c.CandidateID, CandidateRevision: c.Revision, EffectiveAvailability: semreg.AvailabilityUnavailable}}}
			wrong := e.Facts[0]
			wrong.EffectiveAvailability = semreg.AvailabilityAvailable
			wrong.CandidateID = "candidate:p12-decoy"
			e.Facts = append(e.Facts, wrong)
			wrong.CandidateID = c.CandidateID
			wrong.CandidateRevision = "9"
			e.Facts = append(e.Facts, wrong)
			got := causalOutcomeCandidates(reasonPrefix+"fact_unavailable", projection.ItemFact, []semreg.FactCandidate{c}, e)
			if !reflect.DeepEqual(got, []semreg.FactCandidate{c}) {
				t.Fatal("causal candidate identity changed or wrong evaluation matched")
			}
			e.Facts[0].EffectiveAvailability = semreg.AvailabilityAvailable
			if got := causalOutcomeCandidates(reasonPrefix+"fact_unavailable", projection.ItemFact, []semreg.FactCandidate{c}, e); len(got) != 0 {
				t.Fatal("matching available evaluation did not clear causality")
			}
		})
	}
}

func TestP12FactValidityReason(t *testing.T) {
	// Promoted bad validity is structurally rejected upstream. Isolate the
	// validity disjunct here so promotion cannot mask a regression in it.
	c := outcomeReasonFixture(t).Facts[0].Candidates[0]
	c.Quality.Validity = semreg.ValidityBad
	c.Quality.Reasons = []semreg.DefinitionID{"z.causal"}
	e := semreg.EvaluationView{Facts: []semreg.EvaluatedFact{{CandidateID: c.CandidateID, CandidateRevision: c.Revision, EffectiveAvailability: semreg.AvailabilityAvailable}}}
	if got := causalOutcomeCandidates(reasonPrefix+"capability_unqualified", projection.ItemFact, []semreg.FactCandidate{c}, e); !reflect.DeepEqual(got, []semreg.FactCandidate{c}) {
		t.Fatal("bad validity key was not causal")
	}
	got := outcomeReasons(reasonPrefix+"capability_unqualified", projection.ItemFact, []semreg.FactCandidate{c}, nil)
	if !reflect.DeepEqual(got, []semreg.DefinitionID{"z.causal"}) {
		t.Fatalf("bad validity reason = %v", got)
	}
	for _, validity := range []semreg.Validity{semreg.ValidityGood, semreg.ValiditySuspect} {
		good := c
		good.Quality.Validity = validity
		if got := outcomeReasons(reasonPrefix+"capability_unqualified", projection.ItemFact, []semreg.FactCandidate{good}, nil); len(got) != 0 {
			t.Fatal("eligible fact explained withholding")
		}
		if got := causalOutcomeCandidates(reasonPrefix+"capability_unqualified", projection.ItemFact, []semreg.FactCandidate{good}, e); len(got) != 0 {
			t.Fatal("eligible key was causal")
		}
	}
	if got := outcomeReasons(reasonPrefix+"capability_unqualified", projection.ItemCapability, []semreg.FactCandidate{c}, nil); len(got) != 0 {
		t.Fatal("fact validity explained capability-instance qualification")
	}
}
