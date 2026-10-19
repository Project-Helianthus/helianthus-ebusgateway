package semanticview

import (
	"bytes"
	"fmt"
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
		{"withdrawn_fact", projection.ItemFact, projection.ProjectionWithheld, "semantic.read.reason.fact_unavailable"},
		{"unavailable_capability", projection.ItemCapability, projection.ProjectionWithheld, "semantic.read.reason.capability_unavailable"},
		{"unqualified_capability", projection.ItemCapability, projection.ProjectionWithheld, "semantic.read.reason.capability_unqualified"},
		{"relevant_fact", projection.ItemFact, projection.ProjectionWithheld, "z.native.detail"},
		{"relevant_availability", projection.ItemCapability, projection.ProjectionWithheld, "z.native.detail"},
		{"relevant_qualification", projection.ItemCapability, projection.ProjectionWithheld, "z.native.detail"},
		{"exact_fact", projection.ItemFact, projection.ProjectionExact, ""},
		{"exact_capability", projection.ItemCapability, projection.ProjectionExact, ""},
	}
	for _, tt := range tests {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%t", tt.name, reverse), func(t *testing.T) {
				s := outcomeReasonFixture(t)
				values := make([]*semreg.Value, len(s.Facts))
				for i := range s.Facts {
					values[i] = s.Facts[i].Candidates[0].Value
				}
				for i := range s.Facts {
					c := &s.Facts[i].Candidates[0]
					c.Quality.Reasons = []semreg.DefinitionID{"a.native.detail", "z.native.detail"}

					switch tt.name {
					case "withdrawn_fact":
						c.Quality.Availability = semreg.AvailabilityWithdrawn
						c.Quality.Promotion = semreg.PromotionUnpromoted
						c.Quality.Reasons = []semreg.DefinitionID{}
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
					if tt.name == "relevant_fact" || tt.name == "relevant_availability" || tt.name == "relevant_qualification" {
						c.Quality.Reasons = []semreg.DefinitionID{"z.native.detail"}
					}
					if tt.name == "withdrawn_fact" {
						c.Quality.Availability = semreg.AvailabilityAvailable
						c.Value = values[i]
						c.Quality.Reasons = []semreg.DefinitionID{"a.native.detail", "z.native.detail"}
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
				if tt.name == "expired_fact" || tt.name == "withdrawn_fact" {
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
